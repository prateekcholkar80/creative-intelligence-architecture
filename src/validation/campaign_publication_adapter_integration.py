"""Opt-in owned native adapter/receipt verification; synthetic identity only."""
import argparse
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from src.application import campaign_publication_reader as reader,campaign_publication_receipt as receipt
from src.application.campaign_publication_executor import PublicationExecutor,PublicationIntent
from src.application.campaign_permission_lifecycle import Action
from src.application.campaign_permission_storage import PREFIX
from src.application.campaign_review_storage import encode_review,review_head,parse_row
from src.application.identity import IapIdentityAdapter,IapSettings
from src.application.workflow_contract import ContractError
from src.application.sharing_jobs import matches_sharing_job,failed_sharing_job_status
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION
from src.validation.campaign_publication_integration import (
    Scope,TABLES,TTL_MS,load_rows,snapshot,expected_mutation,scoped_plan,append_rows,replace_head,require,
)
from src.validation.campaign_publication_fixtures import seed,actor,AUDIENCE

CASES=('adapter_approve','original_recovery','duplicate_submit_no_write','current_receipt',
    'adapter_revoke','historical_receipt','expired_historical_receipt','absent_original_and_receipt',
    'lost_response_recovery','postcommit_receipt','rollback_no_receipt','revoked_grant_denies',
    'duplicate_audit_denies')
OUTPUT=Path('data/generated/campaign_publication_adapter_diagnostics')


class TrackedClient:
    """Native facade: enforce fixture SQL, count inserts, optionally lose one response."""
    def __init__(self,client,scope,report=None):
        self.client=client;self.scope=scope;self.publications=[];self.reads=[];self.lose_response=False;self.report=report
    def __getattr__(self,name):return getattr(self.client,name)
    def query(self,sql,**kwargs):
        self.scope.assert_sql(sql)
        require(kwargs.get('retry') is None and kwargs.get('job_retry') is None,'Automatic query retry')
        native=kwargs.get('job_id','')
        if native.startswith('publication_it_'):
            require(native.startswith('publication_it_'+self.scope.run_id+'_'),'Unowned native publication ID')
            self.publications.append(native)
            if self.report is not None:
                self.report['mutation_submissions']=len(self.publications)
                self.report.setdefault('submitted_job_ids',[]).append(native)
        else:
            require(native.startswith(('campaign_publication_read_','campaign_publication_receipt_')),
                'Unexpected adapter read namespace')
            self.reads.append(native)
            if self.report is not None:self.report['read_job_ids']=list(self.reads)
        job=self.client.query(sql,**kwargs)
        if self.lose_response and native.startswith('publication_it_'):
            self.lose_response=False;job.result(timeout=120,retry=None,job_retry=None)
            raise RuntimeError('Synthetic discarded submission response')
        return job


def validate_report(report):
    require(report['status'] in ('FAILED','PASSED') and len(report['cases'])==len(CASES)
        and {c['case'] for c in report['cases']}==set(CASES)
        and all(c['status']=='PASSED' for c in report['cases']) and report['cleanup_verified']
        and not report['errors'] and report['serving_writes']==report['model_calls']==0,
        'Incomplete native adapter report')
    require(report['mutation_submissions']==5 and len(report['jobs'])==5
        and len({j['native_job_id'] for j in report['jobs']})==5
        and sum(j['status']=='COMMITTED' for j in report['jobs'])==4
        and sum(j['status']=='NOT_COMMITTED' for j in report['jobs'])==1,
        'Incomplete native adapter commit evidence')
    ids={j['native_job_id'] for j in report['jobs']}
    require(set(report['submitted_job_ids'])==ids and len(report['submitted_job_ids'])==5
        and all(native.startswith('publication_it_'+report['run_id']+'_adapter_') for native in ids)
        and report['read_job_ids'] and len(set(report['read_job_ids']))==len(report['read_job_ids'])
        and all(native.startswith(('campaign_publication_read_','campaign_publication_receipt_'))
            for native in report['read_job_ids']), 'Native job ownership/correlation incomplete')
    require(all(j['initial_adapter_status'] in {'UNKNOWN','COMMITTED','NOT_COMMITTED'}
        and j['recovered_adapter_status']==j['status'] for j in report['jobs']),
        'Adapter recovery evidence differs from correlated native outcome')
    failed=[j for j in report['jobs'] if j['error']]
    require(len(failed)==2 and all(j['result_state'] is None for j in failed)
        and any(j['status']=='COMMITTED' and 'FIXTURE_PUBLICATION_POSTCOMMIT' in j['error'].get('message','') for j in failed)
        and any(j['status']=='NOT_COMMITTED' and 'FIXTURE_PUBLICATION_ROLLBACK' in j['error'].get('message','') for j in failed),
        'Native fault assertion/receipt evidence incomplete')
    by_case={c['case']:c for c in report['cases']}
    for name in ('original_recovery','duplicate_submit_no_write','current_receipt','historical_receipt',
                 'expired_historical_receipt','absent_original_and_receipt','revoked_grant_denies','duplicate_audit_denies'):
        require(by_case[name]['mutation_submissions']==0 and by_case[name]['snapshot_unchanged'],
            'Recovery or denial changed fixture rows')
    require(by_case['postcommit_receipt']['result_state'] is None
        and by_case['postcommit_receipt']['receipt_state']=='VERIFIED'
        and by_case['rollback_no_receipt']['receipt_state']=='UNKNOWN'
        and by_case['lost_response_recovery']['discarded_response'] is True,
        'Native fault receipt evidence incomplete')


def run(client,scope,report):
    ds=bigquery.Dataset(scope.dataset);ds.location=LOCATION;ds.labels=scope.labels;ds.default_table_expiration_ms=TTL_MS
    created=client.create_dataset(ds,retry=None,timeout=20);scope.created=created.created;scope.own(client)
    for name in TABLES:client.create_table(scope.expected(name),retry=None,timeout=20)
    rows,contexts=seed(datetime.now(timezone.utc))
    for name,values in rows.items():
        if values:load_rows(client,scope,name,values)
    scope.inspect(client)
    tracked=TrackedClient(client,scope,report);adapter=IapIdentityAdapter(IapSettings(AUDIENCE))
    executor=PublicationExecutor(tracked,adapter,execution_enabled=True)
    original_read=reader.query_spec;original_receipt=receipt.query_spec;original_plan=PublicationIntent.plan
    faults={};plans={}
    def plan(intent):
        value=original_plan(intent)
        value=scoped_plan(value,scope,'adapter',faults.get(value.command_sha256))
        plans[value.native_job_id]=value;return value
    def read_spec(req):
        sql,cfg=original_read(req);return scope.relocate(sql),cfg
    def receipt_spec(value):
        sql,cfg=original_receipt(value);return scope.relocate(sql),cfg
    def prepare(key):
        ctx=contexts[key]
        return executor.prepare({},ctx['request'],candidate=ctx['candidate'],reason_code='SYNTHETIC_ADAPTER_REVIEW')
    def record(name,**details):
        require(name not in {c['case'] for c in report['cases']},'Duplicate adapter case')
        report['cases'].append(dict(case=name,status='PASSED',**details))
        print(json.dumps(dict(case=name,status='PASSED')),flush=True)
    def unchanged(name,operation,check):
        before=snapshot(client,scope);count=len(tracked.publications);value=operation();check(value)
        require(len(tracked.publications)==count and snapshot(client,scope)==before,'Recovery/denial changed state')
        record(name,mutation_submissions=0,snapshot_unchanged=True)
        return value
    def published(name,intent,*,fault=None,lost=False):
        raw=original_plan(intent)
        if fault:faults[raw.command_sha256]=fault
        value=intent.plan();before=snapshot(client,scope);count=len(tracked.publications)
        tracked.lose_response=lost;outcome=executor.submit({},intent)
        require(len(tracked.publications)==count+1,'Adapter did not submit exactly once')
        job=client.get_job(value.native_job_id,location=LOCATION,retry=None,timeout=20)
        require(matches_sharing_job(job,job_id=value.native_job_id,sql=value.sql,config=value.config())
            and job.state=='DONE','Native adapter identity/config differs')
        status=failed_sharing_job_status(client,job_id=job.job_id,parent=job,timeout_seconds=20) if job.error_result else SharingCommitStatus.COMMITTED
        initial_status=outcome.native_status
        if outcome.native_status is SharingCommitStatus.UNKNOWN:
            # Result polling is bounded to 30 seconds. Positive terminal evidence
            # never rewrites UNKNOWN; use a new read-only original-job recovery.
            outcome=executor.recover({},intent)
        report['jobs'].append(dict(native_job_id=job.job_id,status=status.value,error=job.error_result,
            initial_adapter_status=initial_status.value,recovered_adapter_status=outcome.native_status.value,
            result_state=outcome.result_state))
        committed=fault!='rollback'
        require(status is (SharingCommitStatus.COMMITTED if committed else SharingCommitStatus.NOT_COMMITTED)
            and outcome.native_status is status,'Adapter/native commit classification differs')
        require(snapshot(client,scope)==(expected_mutation(before,value) if committed else before),
            'Native atomic projection differs')
        if fault:
            require(outcome.result_state is None and job.error_result is not None,'Fault invented result')
        return outcome
    with ExitStack() as stack:
        stack.enter_context(patch.object(adapter,'authenticate',side_effect=lambda headers:actor(datetime.now(timezone.utc))))
        stack.enter_context(patch.object(PublicationIntent,'plan',plan))
        stack.enter_context(patch.object(reader,'query_spec',side_effect=read_spec))
        stack.enter_context(patch.object(receipt,'query_spec',side_effect=receipt_spec))
        for module in (reader,receipt):
            for name in ('inspect_reviews','inspect_permissions'):
                stack.enter_context(patch.object(module,name,side_effect=scope.inspect))
        created=prepare('approve');outcome=published('adapter_approve',created)
        require(outcome.result_state=='APPLIED','Approve result differs');record('adapter_approve',native_job_id=outcome.native_job_id)
        def committed(value):require(value.native_status is SharingCommitStatus.COMMITTED and value.result_state=='APPLIED','Original recovery differs')
        unchanged('original_recovery',lambda:executor.recover({},created),committed)
        unchanged('duplicate_submit_no_write',lambda:executor.submit({},created),committed)
        def verified(value):require(value.state=='VERIFIED','Expected historical receipt')
        unchanged('current_receipt',lambda:executor.recover_receipt({},created),verified)
        ctx=contexts['approve'];revocation=executor.prepare({},replace(ctx['request'],action=Action.REVOKE,review=None,evidence=()),reason_code='SYNTHETIC_REVOKE')
        revoked=published('adapter_revoke',revocation);require(revoked.result_state=='APPLIED','Revoke result differs')
        record('adapter_revoke',native_job_id=revoked.native_job_id)
        unchanged('historical_receipt',lambda:executor.recover_receipt({},created),verified)
        # Supplied future time tests historical decoding, not a real future access grant.
        unchanged('expired_historical_receipt',lambda:receipt.read_receipt(tracked,created,
            clock=lambda:datetime.now(timezone.utc)+timedelta(days=2)),verified)
        absent=prepare('lost')
        def absent_both():return executor.recover({},absent),executor.recover_receipt({},absent)
        def unknown(values):require(values[0].native_status is SharingCommitStatus.UNKNOWN and values[1].state=='UNKNOWN','Absent intent became proof')
        unchanged('absent_original_and_receipt',absent_both,unknown)
        lost=published('lost_response_recovery',absent,lost=True)
        require(lost.result_state=='APPLIED','Lost-response original recovery differs')
        record('lost_response_recovery',discarded_response=True,native_job_id=lost.native_job_id)
        post=prepare('postcommit');out=published('postcommit_receipt',post,fault='postcommit')
        recovered=executor.recover_receipt({},post);verified(recovered)
        record('postcommit_receipt',result_state=out.result_state,receipt_state=recovered.state,native_job_id=out.native_job_id)
        rollback=prepare('rollback');out=published('rollback_no_receipt',rollback,fault='rollback')
        recovered=executor.recover_receipt({},rollback);require(recovered.state=='UNKNOWN','Rollback invented receipt')
        record('rollback_no_receipt',receipt_state=recovered.state,native_job_id=out.native_job_id)
        g=ctx['grant'];changed=replace(g,revision=2,parent_sha256=g.sha256,status='REVOKED',recorded_at=datetime.now(timezone.utc))
        row=encode_review(changed);append_rows(client,scope,'campaign_reviewer_grants_versions',[row])
        replace_head(client,scope,'campaign_reviewer_grants_heads',review_head(row))
        def denied(operation):
            try:operation()
            except ContractError as error:return error.code
            raise ValueError('Expected native denial')
        unchanged('revoked_grant_denies',lambda:denied(lambda:executor.recover_receipt({},created)),
            lambda code:require(code in {'CAMPAIGN_REVIEW_NOT_CURRENT','PUBLICATION_REVIEWER_DENIED','CAMPAIGN_REVIEW_INACTIVE'},'Unexpected grant denial: '+code))
        append_rows(client,scope,'campaign_permission_publication_audits',[parse_row(absent.plan().audit_json)])
        unchanged('duplicate_audit_denies',lambda:denied(lambda:executor.recover_receipt({},absent)),
            lambda code:require(code=='PUBLICATION_RECEIPT_DUPLICATE','Duplicate audit denial differs'))
    report['mutation_submissions']=len(tracked.publications);report['read_job_ids']=tracked.reads


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',required=True,action='store_true');parser.parse_args()
    scope=Scope(uuid4().hex);report=dict(status='FAILED',run_id=scope.run_id,dataset=scope.dataset,
        cases=[],jobs=[],mutation_submissions=0,read_job_ids=[],errors=[],cleanup_verified=False,
        serving_writes=0,model_calls=0,synthetic_identity=True,started_at=datetime.now(timezone.utc).isoformat())
    OUTPUT.mkdir(parents=True,exist_ok=True);path=OUTPUT/(scope.run_id+'.json')
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        try:run(client,scope,report)
        except Exception as error:report['errors'].append(type(error).__name__+': '+str(error))
        finally:
            if scope.created is not None:
                try:
                    scope.own(client);client.delete_dataset(scope.dataset,delete_contents=True,retry=None,timeout=20)
                    try:client.get_dataset(scope.dataset,retry=None,timeout=20)
                    except NotFound:report['cleanup_verified']=True
                    else:raise ValueError('Fixture deletion absence unverified')
                except Exception as error:report['errors'].append('cleanup: '+str(error))
            report['completed_at']=datetime.now(timezone.utc).isoformat()
            if not report['errors']:
                try:validate_report(report);report['status']='PASSED'
                except Exception as error:report['errors'].append(str(error))
            path.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(status=report['status'],report=str(path),errors=report['errors'])),flush=True)
    if report['status']!='PASSED':raise SystemExit(1)


if __name__=='__main__':main()
