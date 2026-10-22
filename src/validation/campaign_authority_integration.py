"""Opt-in E2 native transaction/race/recovery probe; owned expiring tables only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack,contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone
import json
from pathlib import Path
import re
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application import campaign_quality_authority_reader as reader,campaign_quality_authority_writer as writer
from src.application.access_policy import AccessDecision,Action,Reason
from src.application.campaign_quality_authority import table_metadata
from src.application.campaign_quality_request_reader import partition_predicate
from src.application.control_io import ControlIO
from src.application.prompt_runtime_contract import TABLES as BINDINGS
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_schema import TABLES as WORKFLOW
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table
from src.validation.campaign_authority_fixtures import fixtures,identity
from src.validation.prompt_evaluation_integration import Probe,Scope as BaseScope
from src.validation.prompt_evaluation_remaining import transaction_interval,validate_race
from src.validation.signing_read_integration import TTL_MS,require

AUTHORITY='campaign_request_authorities'
TABLES={n:s.table(PROJECT) for n,s in dict(WORKFLOW|BINDINGS).items()}
TABLES[AUTHORITY]=table_metadata()
TABLES['product_master']=bigquery.Table(PROJECT+'.ci_product.product_master',schema=[
    bigquery.SchemaField(n,'STRING',mode='REQUIRED') for n in ('sku','source_system','product_status')])
CASES=('atomic_publish_and_authoritative_read','same_job_duplicate_no_submission','new_job_exact_reuse',
    'discarded_response_original_recovery','postcommit_without_invented_receipt','rollback_no_partial_rows',
    'absent_original_unknown_no_submission','stale_snapshot_denies','revocation_denies_transaction',
    'changed_catalog_denies','shared_native_slot_one_commit_unknown_loser','overlapping_transactions_one_winner')


class Scope(BaseScope):
    def __init__(self,run_id):
        super().__init__(run_id)
        self.dataset=f'{PROJECT}.ci_campaign_authority_it_{run_id}'
        self.labels={'purpose':'campaign_authority_integration','run_id':run_id}

    def table(self,name):
        require(name in TABLES,'UNRELATED_TABLE');return self.dataset+'.'+name

    def expected(self,name):
        value=deepcopy(TABLES[name].to_api_repr());value['tableReference']['datasetId']=self.dataset.split('.')[1]
        return bigquery.Table.from_api_repr(value)

    def assert_sql(self,sql):
        refs=re.findall(r'`([^`]+)`',sql);bare=re.sub(r'`[^`]+`',' ',sql)
        require(refs and set(refs)<={self.table(n) for n in TABLES}
            and f'{PROJECT}.ci_application.' not in sql and f'{PROJECT}.ci_product.' not in sql
            and not re.search(r'\b(?:FROM|JOIN|UPDATE|INTO|TABLE)\s+[\w-]+\.[\w.-]+',bare,re.I)
            and not re.search(r'\b(?:DELETE|DROP|TRUNCATE|MERGE)\b',bare,re.I),'UNSAFE_SQL')

    def relocate(self,sql):
        for name,table in TABLES.items():
            sql=sql.replace('`'+str(table.reference)+'`','`'+self.table(name)+'`')
        self.assert_sql(sql);return sql


class FixtureAccess:
    """Supplied known fixture access, not deployed IAP or fresh adapter proof.

    Native publication SQL separately checks actual current fixture membership.
    """
    def authorize(self,actor,action,*,workspace_id,conversation_id=None):
        return AccessDecision(True,action,Reason.ALLOWED_OWNER if conversation_id else Reason.ALLOWED_WORKSPACE_ACTION,
            workspace_id,actor.principal.principal_id,1,conversation_id,1 if conversation_id else None)


class AuthorityProbe(Probe):
    def __init__(self,client):
        super().__init__(client);self.scope=Scope(uuid4().hex);self.report.update(priority=6,batch='E2',
            scope='ISOLATED_NATIVE_AUTHORITY',fixture_dataset=self.scope.dataset,races=[],
            mutation_submissions=[],verified_native_errors={},fixture_access='SUPPLIED_SYNTHETIC',
            product_schema='SELECTED_THREE_COLUMN_PROJECTION')
        self.access=FixtureAccess();self.lose_response=False

    def serving_rows(self):
        return {name:self.client.get_table(table.reference,retry=None,timeout=30).num_rows for name,table in TABLES.items()}

    def setup(self):
        ds=bigquery.Dataset(self.scope.dataset);ds.location=LOCATION;ds.labels=self.scope.labels
        ds.default_table_expiration_ms=TTL_MS
        self.client.create_dataset(ds,exists_ok=False,retry=None,timeout=30);self.owned=True
        self.report['cleanup']='PENDING';self.own()
        for name in TABLES:self.client.create_table(self.scope.expected(name),exists_ok=False,retry=None,timeout=30)
        self.preflight()

    def preflight(self):
        self.own()
        for name in TABLES:
            actual=self.client.get_table(self.scope.table(name),retry=None,timeout=30)
            require(actual.expires is not None and actual.created is not None
                and abs((actual.expires-actual.created).total_seconds()-TTL_MS/1000)<=120,'FIXTURE_EXPIRY_MISMATCH')
            self.validate(actual,self.scope.expected(name))

    @staticmethod
    def validate(actual,expected):
        copy=bigquery.Table.from_api_repr(deepcopy(actual.to_api_repr()));copy.expires=None
        validate_table(copy,expected)

    def load(self,name,rows):
        self.own();self.counter+=1
        cfg=bigquery.LoadJobConfig(schema=self.scope.expected(name).schema,write_disposition='WRITE_APPEND')
        def scalar(v):
            if isinstance(v,datetime):return v.isoformat()
            raise TypeError()
        job=self.client.load_table_from_json(json.loads(json.dumps(rows,default=scalar)),self.scope.table(name),
            job_config=cfg,job_id=f'authority_it_{self.scope.run_id}_load_{self.counter}',num_retries=0,
            project=PROJECT,location=LOCATION,timeout=30)
        job.result(timeout=90,retry=None);require(job.error_result is None,'FIXTURE_LOAD_FAILED')

    def snapshot(self):
        fields=[]
        for name in TABLES:
            source=f'`{self.scope.table(name)}` t WHERE TRUE{partition_predicate(name)}'
            fields.extend([f'(SELECT COUNT(*) FROM {source}) AS {name}_count',
                f'ARRAY(SELECT TO_JSON_STRING(t) FROM {source} ORDER BY TO_JSON_STRING(t) LIMIT 1025) AS {name}'])
        row=self.query('SELECT '+', '.join(fields))[0]
        require(all(row[n+'_count']==len(row[n])<=1024 for n in TABLES),'SNAPSHOT_OVERFLOW')
        return {n:row[n] for n in TABLES}

    def scoped(self,plan,slot='',fault=None):
        sql=self.scope.relocate(plan.sql)
        if fault=='rollback':
            marker='COMMIT TRANSACTION;';require(sql.count(marker)==1,'FAULT_MARKER_MISSING')
            sql=sql.replace(marker,"ASSERT FALSE AS 'FIXTURE_AUTHORITY_ROLLBACK';\n"+marker)
        elif fault=='postcommit':
            marker='SELECT receipt_state AS state,';require(sql.count(marker)==1,'FAULT_MARKER_MISSING')
            sql=sql.replace(marker,"ASSERT FALSE AS 'FIXTURE_AUTHORITY_POSTCOMMIT';\n"+marker)
        require(fault in (None,'rollback','postcommit') and re.fullmatch(r'[a-z_]*',slot),'INVALID_FIXTURE_SLOT')
        self.scope.assert_sql(sql)
        return replace(plan,sql=sql,job_id=f'authority_it_{self.scope.run_id}_{plan.job_id}_{slot}')

    @contextmanager
    def composition(self):
        probe=self;original_query=self.client.query
        class FixtureIO(ControlIO):
            def preflight(self):probe.preflight()
        def tracked(sql,**kwargs):
            probe.scope.assert_sql(sql)
            mutation=kwargs.get('job_id','').startswith('authority_it_'+probe.scope.run_id+'_quality_authority_publish_')
            if mutation:probe.report['mutation_submissions'].append(kwargs['job_id'])
            job=original_query(sql,**kwargs)
            if mutation and probe.lose_response:
                probe.lose_response=False
                job.result(timeout=120,retry=None,job_retry=None)
                raise RuntimeError('FIXTURE_DISCARDED_RESPONSE')
            return job
        with ExitStack() as stack:
            stack.enter_context(patch.object(reader,'SQL',self.scope.relocate(reader.SQL)))
            stack.enter_context(patch.object(reader,'TABLE',self.scope.table(AUTHORITY)))
            stack.enter_context(patch.object(reader,'table_metadata',lambda:self.scope.expected(AUTHORITY)))
            stack.enter_context(patch.object(reader,'validate_table',self.validate))
            stack.enter_context(patch.object(writer,'TABLE',self.scope.table(AUTHORITY)))
            stack.enter_context(patch.object(writer,'ControlIO',FixtureIO))
            stack.enter_context(patch.object(self.client,'query',side_effect=tracked))
            yield FixtureIO(self.client,result_timeout_seconds=90)

    def prepare(self,payload,slot='',fault=None):
        actor=identity()
        plan=writer.prepare_authority_write(self.client,payload,identity=actor,access_reader=self.access,
            settings=writer.AuthorityWriterSettings(True))
        return self.scoped(plan,slot,fault),actor

    def info(self,io,plan,outcome,*,status=Status.COMMITTED,state='APPLIED',error=None):
        if outcome.native_status is Status.UNKNOWN:outcome=io.recover(plan)
        job=self.client.get_job(plan.job_id,project=PROJECT,location=LOCATION,retry=None,timeout=30)
        if job.state!='DONE':
            try:job.result(timeout=120,retry=None,job_retry=None)
            except Exception:pass
            outcome=io.recover(plan)
            job=self.client.get_job(plan.job_id,project=PROJECT,location=LOCATION,retry=None,timeout=30)
        require(matches_sharing_job(job,job_id=plan.job_id,sql=plan.sql,config=plan.config()),'NATIVE_JOB_MISMATCH')
        receipt=None if not outcome.rows else outcome.rows[0]['state']
        require(outcome.native_status is status and receipt==state,'UNEXPECTED_NATIVE_OUTCOME')
        if error:
            require(job.error_result and job.error_result.get('reason')=='invalidQuery'
                and error in job.error_result.get('message',''),'NATIVE_ASSERTION_MISMATCH')
            self.report['verified_native_errors'][plan.job_id]=error
        info=dict(job_id=plan.job_id,status=outcome.native_status.value,receipt_state=receipt,error=job.error_result)
        self.report['jobs'].append(info)
        return outcome,info

    def publish(self,io,plan,actor,**expect):
        result=writer.submit_authority_write(self.client,plan,identity=actor,access_reader=self.access,
            settings=writer.AuthorityWriterSettings(True))
        return self.info(io,plan,result,**expect)[0]

    def read(self,payload,actor):
        return reader.read_execution_authority(self.client,**{k:payload[k] for k in
            ('workspace_id','conversation_id','request_id')},identity=actor)

    def assert_publication(self,before,after,payload):
        workspace=payload['workspace_id']
        for name in TABLES:
            if name not in {'workspaces',AUTHORITY}:require(before[name]==after[name],'UNRELATED_ROWS_CHANGED')
            else:
                outside=lambda values:[v for v in values if json.loads(v)['workspace_id']!=workspace]
                require(outside(before[name])==outside(after[name]),'UNRELATED_ROWS_CHANGED')
        old=next(json.loads(v) for v in before['workspaces'] if json.loads(v)['workspace_id']==workspace)
        new=next(json.loads(v) for v in after['workspaces'] if json.loads(v)['workspace_id']==workspace)
        require(new['revision']==old['revision']+1 and {k:v for k,v in old.items() if k not in {'revision','updated_at'}}
            =={k:v for k,v in new.items() if k not in {'revision','updated_at'}},'WORKSPACE_FENCE_MISMATCH')
        heads=[json.loads(v) for v in after[AUTHORITY] if json.loads(v)['workspace_id']==workspace]
        require(len(heads)==1 and json.loads(heads[0]['payload_canonical_json'])==payload,'PUBLICATION_PARTIAL')

    def record(self,case):
        require(case in CASES and case not in {c['case'] for c in self.report['cases']},'INVALID_CASE')
        self.report['cases'].append(dict(case=case,status='PASSED'))
        print(json.dumps(dict(event='authority_case',case=case,status='PASSED')),flush=True)

    def run(self):
        rows,contexts=fixtures()
        for name,values in rows.items():self.load(name,values)
        with self.composition() as io:
            payload=contexts['core'];plan,actor=self.prepare(payload);before=self.snapshot()
            first=self.publish(io,plan,actor);self.assert_publication(before,self.snapshot(),payload)
            read=self.read(payload,actor)
            require(json.loads(read.content_canonical_json)['authority']==payload,'AUTHORITATIVE_READ_MISMATCH')
            self.record(CASES[0]);count=len(self.report['mutation_submissions'])
            duplicate=writer.submit_authority_write(self.client,plan,identity=actor,access_reader=self.access,
                settings=writer.AuthorityWriterSettings(True))
            require(not duplicate.submitted and duplicate.rows==first.rows
                and len(self.report['mutation_submissions'])==count,'DUPLICATE_RESUBMITTED')
            self.record(CASES[1]);before=self.snapshot()
            self.publish(io,replace(plan,job_id=plan.job_id+'_reuse'),actor,state='REUSED')
            require(before==self.snapshot(),'REPLAY_CHANGED_ROWS');self.record(CASES[2])
            payload=contexts['lost'];plan,actor=self.prepare(payload);self.lose_response=True
            count=len(self.report['mutation_submissions']);self.publish(io,plan,actor)
            recovered=writer.submit_authority_write(self.client,plan,identity=actor,access_reader=self.access,
                settings=writer.AuthorityWriterSettings(True),recover_only=True)
            require(recovered.native_status is Status.COMMITTED and not recovered.submitted
                and len(self.report['mutation_submissions'])==count+1,'RECOVERY_RESUBMITTED')
            require(json.loads(self.read(payload,actor).content_canonical_json)['authority']==payload,'RECOVERY_READ_MISMATCH')
            self.record(CASES[3])
            for case,fault,expected,code,index in (('postcommit','postcommit',Status.COMMITTED,'FIXTURE_AUTHORITY_POSTCOMMIT',4),
                ('rollback','rollback',Status.NOT_COMMITTED,'FIXTURE_AUTHORITY_ROLLBACK',5)):
                payload=contexts[case];plan,actor=self.prepare(payload,fault=fault);before=self.snapshot()
                self.publish(io,plan,actor,status=expected,state=None,error=code)
                if expected is Status.COMMITTED:self.assert_publication(before,self.snapshot(),payload)
                else:require(before==self.snapshot(),'ROLLBACK_LEFT_ROWS')
                recovered=io.recover(plan)
                require(recovered.native_status is expected and not recovered.rows and not recovered.submitted,'FAULT_RECOVERY_MISMATCH')
                self.record(CASES[index])
            before=self.snapshot();count=len(self.report['mutation_submissions'])
            absent=io.recover(replace(plan,job_id=plan.job_id+'_absent'))
            require(absent.native_status is Status.UNKNOWN and not absent.submitted and before==self.snapshot()
                and count==len(self.report['mutation_submissions']),'ABSENCE_INFERRED_ROLLBACK');self.record(CASES[6])
            for case,mutation,code,index in (
                ('stale',"UPDATE `{requests}` SET revision=revision+1,updated_at=CURRENT_TIMESTAMP() WHERE workspace_id='{workspace}'; SELECT 1 AS n",'AUTHORITY_SOURCE_CHANGED',7),
                ('revoked',"UPDATE `{memberships}` SET status='REVOKED',revision=revision+1 WHERE workspace_id='{workspace}'; SELECT 1 AS n",'MEMBERSHIP_DENIED',8)):
                payload=contexts[case];plan,actor=self.prepare(payload)
                sql=mutation.format(requests=self.scope.table('requests'),memberships=self.scope.table('memberships'),workspace=payload['workspace_id'])
                if case=='stale':sql=sql.replace(" WHERE workspace_id=",f" WHERE TRUE{partition_predicate('requests')} AND workspace_id=")
                self.query(sql);before=self.snapshot()
                self.info(io,plan,io.submit_once(plan),status=Status.NOT_COMMITTED,state=None,error=code)
                require(before==self.snapshot(),'DENIAL_CHANGED_ROWS');self.record(CASES[index])
            self.races(io,contexts)
            payload=contexts['catalog'];plan,actor=self.prepare(payload)
            self.query(f"UPDATE `{self.scope.table('product_master')}` SET product_status='INACTIVE' WHERE sku='CI-SKU-0021'; SELECT 1 AS n")
            before=self.snapshot()
            self.info(io,plan,io.submit_once(plan),status=Status.NOT_COMMITTED,state=None,error='AUTHORITY_CATALOG_CHANGED')
            require(before==self.snapshot(),'CATALOG_DENIAL_CHANGED_ROWS');self.record(CASES[9])

    def races(self,io,contexts):
        for name,global_slot,index in (('global',True,10),('race',False,11)):
            left,actor=self.prepare(contexts[name]);right,_=self.prepare(contexts[name]|dict(country='CA'))
            require(left.job_id==right.job_id,'GLOBAL_SLOT_REBOUND')
            if not global_slot:
                left=replace(left,job_id=left.job_id+'_left');right=replace(right,job_id=right.job_id+'_right')
            before=self.snapshot();barrier=Barrier(2)
            def submit(plan):barrier.wait(timeout=30);return io.submit_once(plan)
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(submit,p) for p in (left,right)]
                outcomes=[f.result(timeout=180) for f in futures]
            if global_slot:
                outcomes=[io.recover(p) if o.native_status is Status.UNKNOWN else o for p,o in zip((left,right),outcomes)]
                winners=[i for i,o in enumerate(outcomes) if o.native_status is Status.COMMITTED]
                require(len(winners)==1 and outcomes[1-winners[0]].native_status is Status.UNKNOWN,'GLOBAL_RESERVATION_FAILED')
                winner=winners[0];self.info(io,(left,right)[winner],outcomes[winner])
            else:
                infos=[];intervals=[]
                for plan,outcome in zip((left,right),outcomes):
                    expected=outcome.native_status
                    if expected is Status.UNKNOWN:outcome=io.recover(plan);expected=outcome.native_status
                    _,info=self.info(io,plan,outcome,status=expected,state='APPLIED' if expected is Status.COMMITTED else None)
                    children=list(self.client.list_jobs(project=PROJECT,parent_job=plan.job_id,all_users=False,
                        max_results=64,page_size=64,retry=None,timeout=30))
                    interval=transaction_interval(children,plan.job_id);intervals.append(interval)
                    info['transaction_interval']=[t.isoformat() for t in interval];infos.append(info)
                winner=validate_race(infos,intervals,('AUTHORITY_SOURCE_CHANGED','AUTHORITY_CAPTURE_CONFLICT'))
                self.report['races'].append(dict(case=name,jobs=infos,winner=winner))
            payload=json.loads((left,right)[winner].candidate_json)
            self.assert_publication(before,self.snapshot(),payload);self.read(payload,actor)
            self.record(CASES[index])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--run',action='store_true');mode.add_argument('--diagnose-job')
    parser.add_argument('--output-dir',type=Path,required=True);args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        if args.diagnose_job:
            require(re.fullmatch(r'authority_it_[0-9a-f]{32}_quality_authority_publish_[0-9a-f]{64}_[a-z_]*',
                args.diagnose_job),'UNOWNED_DIAGNOSTIC_JOB')
            job=client.get_job(args.diagnose_job,project=PROJECT,location=LOCATION,retry=None,timeout=30)
            cfg=job.to_api_repr().get('configuration',{});query=cfg.get('query',{})
            print(json.dumps(dict(job_id=job.job_id,state=job.state,error=job.error_result,
                configuration_keys=sorted(cfg),query_keys=sorted(query),job_timeout=cfg.get('jobTimeoutMs'),
                script_options=query.get('scriptOptions'),parameters=[dict(name=p['name'],type=p['parameterType'],
                    value_keys=sorted(p.get('parameterValue',{})),array_length=len(p.get('parameterValue',{}).get('arrayValues',[])))
                    for p in query.get('queryParameters',[])])),flush=True)
            return
        probe=AuthorityProbe(client);before=probe.serving_rows()
        try:
            probe.setup();probe.run()
            require({c['case'] for c in probe.report['cases']}==set(CASES),'INCOMPLETE_PROBE')
            probe.report['status']='PASSED'
        except Exception as error:
            probe.report.update(status='FAILED',error_code=getattr(error,'code',type(error).__name__),fixture_error=str(error)[:1500])
        finally:
            try:probe.cleanup()
            except Exception as error:probe.report.update(status='FAILED',cleanup='FAILED',cleanup_error=type(error).__name__)
            try:
                after=probe.serving_rows();require(before==after,'SERVING_METADATA_CHANGED')
                probe.report.update(serving_rows_before=before,serving_rows_after=after,serving_row_metadata_unchanged=True)
            except Exception as error:probe.report.update(status='FAILED',preservation_error=str(error)[:200])
            args.output_dir.mkdir(parents=True,exist_ok=True);path=args.output_dir/(probe.scope.run_id+'.json')
            path.write_text(json.dumps(probe.report,indent=2,default=str)+'\n')
            print(json.dumps(dict(event='authority_complete',status=probe.report['status'],cases=len(probe.report['cases']),
                cleanup=probe.report['cleanup'],report=str(path))),flush=True)
        require(probe.report['status']=='PASSED','AUTHORITY_NATIVE_PROBE_FAILED')


if __name__=='__main__':main()
