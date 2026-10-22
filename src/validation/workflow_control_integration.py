"""4.4 isolated native management/dispatch SQL; zero model/serving calls.

--run --output-dir PATH uses ADC. Owns one expiring dataset, verifies labels and
exact destination schemas, and deletes only that dataset. Fixture actors are
explicitly synthetic; this does not verify deployed HTTP/IAP/IAM permissions.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import time
from threading import Barrier, Lock
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.control_io import ControlIO, query_config
from src.application.generation_admission import ProtectedGenerationAdmission, AdmissionSettings
from src.application.identity import IapIdentityAdapter, IapSettings, VerifiedIdentity, ISSUER, principal_for_subject
from src.application.management_service import ProtectedManagementService, ManagementSettings, WorkspaceBootstrapManifest, MembershipSyncMapping, digest
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.sharing_jobs import matches_sharing_job
from src.application.sharing_publication_codec import normalize
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import TABLES
from src.application.operation_budget import worker_operation_scope
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION, _schema, _partition
from src.validation.control_fixtures import ACTOR, SUBJECT, controls, workflow
from src.validation.signing_read_integration import TTL_MS, json_rows, require

AUDIENCE='/projects/1/locations/asia-south1/services/isolated-control-probe'


class Scope:
    def __init__(self,run_id):
        require(type(run_id) is str and re.fullmatch(r'[0-9a-f]{32}',run_id),'INVALID_SCOPE')
        self.run_id=run_id;self.dataset=f'{PROJECT}.ci_workflow_control_it_{run_id}'
        self.labels=dict(purpose='workflow_control_integration',run_id=run_id)

    def table(self,name):
        require(name in TABLES,'UNRELATED_TABLE');return self.dataset+'.'+name

    def sql(self,sql):
        for name in TABLES:sql=sql.replace(f'`{PROJECT}.ci_application.{name}`',f'`{self.table(name)}`')
        refs=re.findall(r'`([^`]+)`',sql)
        require(bool(refs) and all(ref in {self.table(name) for name in TABLES} for ref in refs),'UNSAFE_SQL')
        return sql

    def own(self,dataset):
        require(dataset.full_dataset_id==f'{PROJECT}:{self.dataset.split(".")[1]}' and dataset.location.lower()==LOCATION
            and dataset.labels==self.labels and dataset.default_table_expiration_ms==TTL_MS,'OWNERSHIP_MISMATCH')


class FixtureIO(ControlIO):
    def __init__(self,probe):super().__init__(probe.client);self.probe=probe
    def read(self,sql,params):self.probe.own();return super().read(self.probe.scope.sql(sql),params)
    def preflight(self):
        self.probe.own()
        for name,spec in TABLES.items():
            actual=self.client.get_table(self.probe.scope.table(name),retry=None,timeout=10)
            expected=spec.table(PROJECT)
            require(_schema(actual.schema)==_schema(expected.schema) and _partition(actual)==_partition(expected)
                and bool(actual.require_partition_filter)==bool(expected.require_partition_filter),'SCHEMA_MISMATCH')
    def submit_once(self,plan):
        self.probe.own();return super().submit_once(replace(plan,sql=self.probe.scope.sql(plan.sql)))
    def recover(self,plan):
        self.probe.own();return super().recover(replace(plan,sql=self.probe.scope.sql(plan.sql)))
    def original_job(self,job_id):
        original=super().original_job(job_id)
        self.probe.scope.sql(original.query)
        properties=deepcopy(original._properties)
        sql=original.query
        for name in TABLES:sql=sql.replace(f'`{self.probe.scope.table(name)}`',f'`{PROJECT}.ci_application.{name}`')
        properties['configuration']['query']['query']=sql
        return bigquery.QueryJob.from_api_repr(properties,self.client)


class Probe:
    def __init__(self,client):
        require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,'INVALID_CLIENT')
        self.client=client;self.scope=Scope(uuid4().hex);self.owned=False;self.counter=0;self.lock=Lock()
        self.report=dict(step='4.4',severity='Critical',necessity='Must have',scenario='shared pilot',status='RUNNING',
            project=PROJECT,location=LOCATION,fixture_dataset=self.scope.dataset,cleanup='NOT_CREATED',
            serving_row_writes=0,model_calls=0,secret_accesses=0,scope='ISOLATED_NATIVE_CONTROL_SQL_AND_RECOVERY',cases=[],jobs=[])

    def own(self):self.scope.own(self.client.get_dataset(self.scope.dataset,retry=None,timeout=10))

    def id(self):
        with self.lock:self.counter+=1;return 'control-it-'+self.scope.run_id+'-'+str(self.counter)

    def setup(self):
        ds=bigquery.Dataset(self.scope.dataset);ds.location=LOCATION;ds.labels=self.scope.labels;ds.default_table_expiration_ms=TTL_MS
        self.client.create_dataset(ds,retry=None,timeout=10);self.owned=True;self.report['cleanup']='PENDING';self.own()
        for name,spec in TABLES.items():
            source=spec.table(PROJECT);target=bigquery.Table(self.scope.table(name),schema=source.schema)
            target.clustering_fields=source.clustering_fields;target.time_partitioning=source.time_partitioning
            target.require_partition_filter=source.require_partition_filter
            self.client.create_table(target,retry=None,timeout=10)
            actual=self.client.get_table(target,retry=None,timeout=10)
            require(_schema(actual.schema)==_schema(source.schema) and _partition(actual)==_partition(source)
                and bool(actual.require_partition_filter)==bool(source.require_partition_filter)
                and actual.clustering_fields==source.clustering_fields and actual.expires is not None,'SCHEMA_MISMATCH')

    def load(self,rows):
        self.own()
        for name,values in rows.items():
            target=self.client.get_table(self.scope.table(name),retry=None,timeout=10)
            require(_schema(target.schema)==_schema(TABLES[name].schema),'SCHEMA_MISMATCH')
            job=self.client.load_table_from_json(json_rows(values),target,job_id=self.id(),location=LOCATION,timeout=10,num_retries=0,
                job_config=bigquery.LoadJobConfig(schema=target.schema,write_disposition='WRITE_APPEND'))
            job.result(timeout=60,retry=None);require(job.error_result is None,'LOAD_FAILED')

    def auth(self,subject=SUBJECT):
        adapter=IapIdentityAdapter(IapSettings(AUDIENCE))
        def fixture_identity(headers):
            epoch=int(datetime.now(timezone.utc).timestamp())
            return VerifiedIdentity(principal_for_subject(ISSUER,subject),ISSUER,subject,AUDIENCE,epoch-10,epoch+590)
        adapter.authenticate=fixture_identity
        return adapter

    def management(self,*,subject=SUBJECT,**settings):
        service=ProtectedManagementService(client=self.client,verifier=self.auth(subject),settings=ManagementSettings(enabled=True,**settings))
        service.io=FixtureIO(self);return service

    def admission(self,rows,**kwargs):
        guard=ProtectedGenerationAdmission(client=self.client,verifier=self.auth(),worker_id='fixture-worker',
            settings=AdmissionSettings(frozenset({digest(rows['model_invocations'][0]['profile'])}),True),**kwargs)
        guard.io=FixtureIO(self);return guard

    def prepare_admission(self,guard,ws,stage='PLANNER'):
        at=datetime.now(timezone.utc)
        return guard.prepare({},workspace_id=ws,conversation_id='fixture-conversation',request_id='fixture-request',
            invocation_id='fixture-invocation',job_id='fixture-job' if stage=='PLANNER' else 'unused',
            attempt_id='fixture-attempt' if stage=='PLANNER' else 'unused',start_at=at-timedelta(hours=1),end_at=at+timedelta(hours=1))

    def execute(self,service,plan,*,fault=None,native_id=None):
        # Fault injection lives only in this scope-checked fixture adapter.
        relocated=self.scope.sql(plan.sql)
        if fault=='rollback':relocated=relocated.replace('-- PROBE_AFTER_FENCE',"ASSERT FALSE AS 'FIXTURE_ROLLBACK';")
        if fault=='postcommit':relocated=relocated.replace('-- PROBE_AFTER_COMMIT',"ASSERT FALSE AS 'FIXTURE_POSTCOMMIT';")
        actual=replace(plan,sql=relocated,job_id=native_id or plan.job_id)
        io=ControlIO(self.client)
        result=io.submit_once(actual)
        job=self.client.get_job(actual.job_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
        # query() starts asynchronously. Await once before original-job recovery.
        try:job.result(timeout=60,retry=None,job_retry=None)
        except Exception:pass
        result=io.recover(actual)
        require(matches_sharing_job(job,job_id=actual.job_id,sql=actual.sql,config=actual.config()),'JOB_MISMATCH')
        self.report['jobs'].append(dict(job_id=actual.job_id,status=result.native_status.value,
            bytes_processed=job.total_bytes_processed,error=(job.error_result or {}).get('message','')[:500]))
        return result,actual

    def wait_submit(self,service,plan):
        result=service.submit({},plan) if isinstance(service,ProtectedManagementService) else service.claim({},plan)
        actual=replace(plan,sql=self.scope.sql(plan.sql))
        try:self.client.get_job(plan.job_id,location=LOCATION,retry=None,timeout=10).result(timeout=60,retry=None,job_retry=None)
        except Exception:pass
        result=service.recover({},plan)
        job=self.client.get_job(plan.job_id,location=LOCATION,retry=None,timeout=10)
        self.report['jobs'].append(dict(job_id=plan.job_id,status=result.native_status.value,
            state=job.state,error=(job.error_result or {}).get('message','')[:1000],
            matches=matches_sharing_job(job,job_id=plan.job_id,sql=actual.sql,config=actual.config())))
        return result

    def read(self,ws,name):
        spec=TABLES[name];where='workspace_id=@workspace_id'
        if spec.partition_field:where+=f" AND {spec.partition_field} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')"
        sql=self.scope.sql(f'SELECT TO_JSON_STRING(t) AS row FROM `{PROJECT}.ci_application.{name}` t WHERE {where}')
        config=query_config([bigquery.ScalarQueryParameter('workspace_id','STRING',ws)])
        self.own();job=self.client.query(sql,job_config=config,job_id=self.id(),location=LOCATION,retry=None,job_retry=None,timeout=10)
        return [decode_row(row['row'],name) for row in job.result(timeout=30,retry=None,job_retry=None)]

    def record(self,name):
        self.report['cases'].append(dict(case=name,status='PASSED'))
        print(json.dumps(dict(event='workflow_control_case',case=name,status='PASSED')),flush=True)

    def cases(self,*,dispatch_only=False,management_only=False):
        sentinel='unrelated-'+self.scope.run_id;sentinel_rows=controls(sentinel);self.load(sentinel_rows)
        sentinel_before={name:self.read(sentinel,name) for name in sentinel_rows}
        if not dispatch_only:
            ws=self.id();self.load(controls(ws));svc=self.management()
            plan=svc.prepare({},kind='MEMBER',workspace_id=ws,command_id='add',expected_workspace_revision=1,
                target_subject='accounts.google.com:new-probe-member',roles=('VIEWER',),status='ACTIVE')
            result=self.wait_submit(svc,plan)
            require(result.native_status is Status.COMMITTED and len(self.read(ws,'memberships'))==2
                and len(self.read(ws,'audit_events'))==1,'MEMBER_NOT_ATOMIC')
            self.record('member_creation_atomic_fence_and_audit')
            again=svc.submit({},plan)
            require(again.native_status is Status.COMMITTED and len(self.read(ws,'audit_events'))==1,'DUPLICATE_MUTATED')
            self.record('duplicate_submission_and_discarded_response_read_only_recovery')
            try:svc.prepare({},kind='MEMBER',workspace_id=ws,command_id='remove-admin',expected_workspace_revision=2,
                target_subject=SUBJECT,roles=('VIEWER',),status='ACTIVE',expected_revision=1)
            except ContractError as error:require(error.code=='LAST_ADMIN_REQUIRED','WRONG_LAST_ADMIN_DENIAL')
            else:raise RuntimeError('LAST_ADMIN_ACCEPTED')
            self.record('last_admin_rejected_before_submission')

            for fault,expected in (('rollback',Status.NOT_COMMITTED),('postcommit',Status.COMMITTED)):
                target=self.id();self.load(controls(target));service=self.management()
                candidate=service.prepare({},kind='WORKSPACE_STATUS',workspace_id=target,command_id='disable',expected_workspace_revision=1,status='DISABLED')
                result,actual=self.execute(service,candidate,fault=fault)
                require(result.native_status is expected,'INCORRECT_COMMIT_CLASSIFICATION')
                require(self.read(target,'workspaces')[0]['status']==('ACTIVE' if fault=='rollback' else 'DISABLED'),'PARTIAL_STATUS')
                self.record('management_'+fault+'_positive_native_recovery')

            target=self.id();at=datetime.now(timezone.utc)-timedelta(seconds=1)
            manifest=WorkspaceBootstrapManifest(target,SUBJECT,('ADMIN','CREATOR'),'bootstrap',at,at+timedelta(minutes=4))
            service=self.management(bootstrap=manifest)
            boot=service.prepare({},kind='BOOTSTRAP',workspace_id=target,command_id='bootstrap')
            result=self.wait_submit(service,boot)
            require(result.native_status is Status.COMMITTED and len(self.read(target,'workspaces'))==1
                and self.read(target,'memberships')[0]['assignment_origin']=='BOOTSTRAP','BOOTSTRAP_FAILED')
            require(service.submit({},boot).native_status is Status.COMMITTED and len(self.read(target,'audit_events'))==1,'BOOTSTRAP_REPLAYED')
            self.record('explicit_bootstrap_global_slot_and_recovery')

            target=self.id();self.load(controls(target));service=self.management()
            archived=service.prepare({},kind='ARCHIVE',workspace_id=target,command_id='archive',expected_workspace_revision=1,
                conversation_id='fixture-conversation',expected_revision=1)
            require(self.wait_submit(service,archived).native_status is Status.COMMITTED
                and self.read(target,'conversations')[0]['status']=='ARCHIVED','ARCHIVE_FAILED')
            self.record('owner_archive_lifecycle_and_audit')

        if management_only:
            require({name:self.read(sentinel,name) for name in sentinel_rows}==sentinel_before,'UNRELATED_ROWS_CHANGED')
            self.record('unrelated_workspace_rows_preserved')
            return

        for stage in ('INTERPRETER','PLANNER'):
            target=self.id();rows=workflow(target,stage);self.load(rows);guard=self.admission(rows)
            claim=self.prepare_admission(guard,target,stage)
            require(self.wait_submit(guard,claim).native_status is Status.COMMITTED,'CLAIM_FAILED')
            observed=self.read(target,'model_invocations')[0]
            require(observed['state']=='DISPATCHED' and observed['estimated_cost'] is None,'BAD_CLAIM_ACCOUNTING')
            require(guard.claim({},claim).native_status is Status.COMMITTED and len(self.read(target,'audit_events'))==1,'CLAIM_REPLAYED')
            self.record(stage.lower()+'_atomic_claim_and_original_job_recovery')

        target=self.id();rows=workflow(target);self.load(rows);guard=self.admission(rows)
        claim=self.prepare_admission(guard,target)
        result,actual=self.execute(guard,claim,fault='rollback')
        require(result.native_status is Status.NOT_COMMITTED and self.read(target,'model_invocations')[0]['state']=='PLANNED'
            and not self.read(target,'audit_events'),'PARTIAL_DISPATCH')
        self.record('dispatch_rollback_never_claims_or_records_zero_cost')

        target=self.id();rows=workflow(target);self.load(rows);guard=self.admission(rows)
        claim=self.prepare_admission(guard,target)
        result,actual=self.execute(guard,claim,fault='postcommit')
        require(result.native_status is Status.COMMITTED and not result.rows
            and self.read(target,'model_invocations')[0]['state']=='DISPATCHED','POSTCOMMIT_CLAIM_LOST')
        self.record('dispatch_postcommit_failure_has_no_replay_permission')

        target=self.id();rows=workflow(target);self.load(rows)
        left,right=self.admission(rows),self.admission(rows)
        p1,p2=self.prepare_admission(left,target),self.prepare_admission(right,target)
        barrier=Barrier(2)
        def compete(service,plan):barrier.wait(timeout=10);return self.wait_submit(service,plan)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(compete,s,p) for s,p in ((left,p1),(right,p2))]
            outcomes=[f.result(timeout=120) for f in futures]
        require(sum(result.native_status is Status.COMMITTED for result in outcomes)==1
            and len(self.read(target,'audit_events'))==1,'TWO_DISPATCH_WINNERS')
        self.record('competing_worker_incarnations_one_global_claim')

        require({name:self.read(sentinel,name) for name in sentinel_rows}==sentinel_before,'UNRELATED_ROWS_CHANGED')
        self.record('unrelated_workspace_rows_preserved')

    def adversarial_cases(self):
        target=self.id();self.load(controls(target,second_admin=True))
        other='accounts.google.com:second-control-probe'
        left,right=self.management(),self.management(subject=other)
        plans=[service.prepare({},kind='MEMBER',workspace_id=target,command_id='demote-self',expected_workspace_revision=1,
            target_subject=subject,roles=('VIEWER',),status='ACTIVE',expected_revision=1)
            for service,subject in ((left,SUBJECT),(right,other))]
        barrier=Barrier(2)
        def race(service,plan):barrier.wait(timeout=10);return self.execute(service,plan)[0]
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes=[pool.submit(race,service,plan) for service,plan in zip((left,right),plans)]
            results=[future.result(timeout=120) for future in outcomes]
        require(sum(result.native_status is Status.COMMITTED for result in results)==1
            and all(result.native_status in {Status.COMMITTED,Status.NOT_COMMITTED} for result in results)
            and sum('ADMIN' in row['roles'] and row['status']=='ACTIVE' for row in self.read(target,'memberships'))==1,
            'LAST_ADMIN_RACE_FAILED')
        intervals=[]
        for plan in plans:
            children=list(self.client.list_jobs(project=PROJECT,parent_job=plan.job_id,max_results=64,page_size=64,retry=None,timeout=10))
            starts=[child.ended for child in children if child.statement_type=='BEGIN_TRANSACTION']
            ends=[child.started for child in children if child.statement_type in {'COMMIT_TRANSACTION','ROLLBACK_TRANSACTION'}]
            require(len(starts)==len(ends)==1 and starts[0] is not None and ends[0] is not None,'RACE_INTERVAL_MISSING')
            intervals.append((starts[0],ends[0]))
        require(max(start for start,end in intervals)<min(end for start,end in intervals),'RACE_NOT_OVERLAPPING')
        self.record('native_overlapping_admin_self_demotion_preserves_last_admin')

        target=self.id();self.load(workflow(target));service=self.management()
        plan=service.prepare({},kind='ARCHIVE',workspace_id=target,command_id='busy-archive',expected_workspace_revision=1,
            conversation_id='fixture-conversation',expected_revision=1)
        result,_=self.execute(service,plan)
        require(result.native_status is Status.NOT_COMMITTED and self.read(target,'conversations')[0]['status']=='ACTIVE'
            and not self.read(target,'audit_events'),'BUSY_ARCHIVE_ACCEPTED')
        self.record('active_generation_blocks_archive_without_partial_changes')

        target=self.id();self.load(controls(target));at=datetime.now(timezone.utc)-timedelta(seconds=1)
        mapping=MembershipSyncMapping('fixture-sync',target,'accounts.google.com:directory-mapped-probe',('VIEWER',),'ACTIVE',at,at+timedelta(minutes=10))
        service=self.management(sync_mappings=(mapping,))
        plan=service.prepare({},kind='SYNC',workspace_id=target,command_id='sync',expected_workspace_revision=1,mapping_id=mapping.mapping_id)
        require(self.wait_submit(service,plan).native_status is Status.COMMITTED
            and any(row['assignment_origin']=='SYNC' and row['roles']==['VIEWER'] for row in self.read(target,'memberships')),'SYNC_FAILED')
        self.record('explicit_sync_mapping_writes_only_pinned_roles')

        for duplicate in ('intent','stage'):
            target=self.id();rows=workflow(target)
            if duplicate=='intent':
                from src.application.workflow_contract import request_fingerprint
                row=deepcopy(rows['requests'][0]);row['request_id']='duplicate-request';row['request_sha256']=request_fingerprint(row)
                rows['requests'].append(row)
            else:rows['model_invocations'].append(rows['model_invocations'][0]|dict(invocation_id='duplicate-invocation'))
            self.load(rows);guard=self.admission(rows);plan=self.prepare_admission(guard,target)
            result,_=self.execute(guard,plan)
            require(result.native_status is Status.NOT_COMMITTED and all(row['state']=='PLANNED' for row in self.read(target,'model_invocations'))
                and not self.read(target,'audit_events'),'DUPLICATE_DISPATCH_ACCEPTED')
            self.record('duplicate_'+duplicate+'_denies_billable_claim')

        target=self.id();at=datetime.now(timezone.utc)-timedelta(seconds=1)
        manifest=WorkspaceBootstrapManifest(target,SUBJECT,('ADMIN','CREATOR'),'partial-bootstrap',at,at+timedelta(minutes=4))
        service=self.management(bootstrap=manifest)
        first=service.prepare({},kind='BOOTSTRAP',workspace_id=target,command_id=manifest.manifest_id)
        self.load(dict(memberships=controls(target)['memberships']))
        result=self.wait_submit(service,first)
        require(result.native_status is Status.NOT_COMMITTED and not self.read(target,'workspaces'),'PARTIAL_BOOTSTRAP_ACCEPTED')
        # A scoped fixture-only repair; never exposed by the serving service.
        sql=self.scope.sql(f'DELETE FROM `{PROJECT}.ci_application.memberships` WHERE workspace_id=@workspace_id')
        self.own();self.client.query(sql,job_config=query_config([bigquery.ScalarQueryParameter('workspace_id','STRING',target)]),
            job_id=self.id(),location=LOCATION,retry=None,job_retry=None,timeout=10).result(timeout=30,retry=None,job_retry=None)
        second=service.prepare({},kind='BOOTSTRAP',workspace_id=target,command_id=manifest.manifest_id,slot=2)
        require(self.wait_submit(service,second).native_status is Status.COMMITTED and len(self.read(target,'workspaces'))==1,'BOOTSTRAP_RETRY_FAILED')
        self.record('partial_bootstrap_positive_rollback_then_controlled_global_retry')

        target=self.id();self.load(controls(target));service=self.management()
        plan=service.prepare({},kind='MEMBER',workspace_id=target,command_id='cold-recovery',expected_workspace_revision=1,
            target_subject='accounts.google.com:cold-recovered-probe',roles=('VIEWER',),status='ACTIVE')
        require(self.wait_submit(service,plan).native_status is Status.COMMITTED,'COLD_SETUP_FAILED')
        restarted=self.management();original=restarted.original_plan({},workspace_id=target,command_id='cold-recovery')
        require(restarted.recover({},original).native_status is Status.COMMITTED and not restarted._issued,'COLD_RECOVERY_FAILED')
        try:restarted.submit({},original)
        except ContractError as error:require(error.code=='MANAGEMENT_PLAN_NOT_ISSUED','COLD_RESUBMIT_WRONG_DENIAL')
        else:raise RuntimeError('COLD_RECOVERY_GRANTED_WRITE')
        self.record('cold_management_original_job_recovery_cannot_resubmit')

    def callback_cases(self):
        target=self.id();rows=workflow(target);self.load(rows);calls=[]
        def fixture_callback(profile):
            calls.append(digest(profile))
            return 'FIXTURE_NON_BILLABLE_RESPONSE'
        guard=self.admission(rows,model_call=fixture_callback);plan=self.prepare_admission(guard,target)
        with worker_operation_scope(time.monotonic()+120):response=guard.dispatch({},plan)
        require(response=='FIXTURE_NON_BILLABLE_RESPONSE' and calls==[digest(rows['model_invocations'][0]['profile'])],
            'FRESH_NATIVE_CLAIM_DID_NOT_AUTHORIZE_ONCE')
        self.record('fresh_native_claim_allows_one_installed_non_billable_callback')
        require(guard.recover({},plan).native_status is Status.COMMITTED,'CALLBACK_RECOVERY_FAILED')
        try:
            with worker_operation_scope(time.monotonic()+120):guard.dispatch({},plan)
        except ContractError as error:require(error.code=='MODEL_REPLAY_DENIED','CALLBACK_REPLAY_WRONG_DENIAL')
        else:raise RuntimeError('CALLBACK_REPLAYED')
        require(len(calls)==1,'RECOVERY_CALLED_ADAPTER')
        self.record('discarded_callback_response_and_recovery_cannot_dispatch_again')

    def cleanup(self):
        if not self.owned:return
        self.own();self.client.delete_dataset(self.scope.dataset,delete_contents=True,retry=None,timeout=10)
        try:self.client.get_dataset(self.scope.dataset,retry=None,timeout=10)
        except NotFound:self.owned=False;self.report['cleanup']='DELETED_AND_ABSENCE_VERIFIED';return
        raise RuntimeError('CLEANUP_NOT_VERIFIED')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true',required=True);parser.add_argument('--output-dir',type=Path,required=True)
    group=parser.add_mutually_exclusive_group()
    group.add_argument('--adversarial-only',action='store_true')
    group.add_argument('--dispatch-only',action='store_true')
    group.add_argument('--management-only',action='store_true')
    group.add_argument('--callback-only',action='store_true')
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        probe=Probe(client)
        try:
            probe.setup()
            if args.adversarial_only:probe.adversarial_cases()
            elif args.callback_only:probe.callback_cases()
            else:
                probe.cases(dispatch_only=args.dispatch_only,management_only=args.management_only)
                if not args.dispatch_only and not args.management_only:
                    probe.adversarial_cases();probe.callback_cases()
            probe.report['status']='PASSED'
        except Exception as error:
            probe.report['status']='FAILED';probe.report['error_code']=getattr(error,'code',type(error).__name__)
            # Owned, key-free fixture diagnostics only. No JWT or serving payload.
            probe.report['fixture_error']=str(error)[:1000]
        finally:
            try:probe.cleanup()
            except Exception as error:probe.report.update(status='FAILED',cleanup='FAILED',cleanup_error=type(error).__name__)
            args.output_dir.mkdir(parents=True,exist_ok=True);path=args.output_dir/(probe.scope.run_id+'.json')
            path.write_text(json.dumps(probe.report,indent=2,default=str)+'\n')
            print(json.dumps(dict(event='workflow_control_complete',status=probe.report['status'],cases=len(probe.report['cases']),
                cleanup=probe.report['cleanup'],report=str(path))),flush=True)
        require(probe.report['status']=='PASSED','WORKFLOW_CONTROL_PROBE_FAILED')


if __name__=='__main__':main()
