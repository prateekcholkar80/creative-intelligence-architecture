"""Opt-in owned BigQuery question-child verification; no serving/model writes."""
import argparse,json,re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime,timezone
from pathlib import Path
from google.cloud import bigquery
from src.application.control_io import ControlIO
from src.application.workflow_schema import TABLES
from src.application.workflow_codec import decode_row
from src.application.campaign_transcript import SPEC as BODY
from src.application.campaign_response_transcript import SPEC as RESPONSE
from src.application.campaign_quality_authority import table_metadata
from src.application.prompt_runtime_contract import TABLES as BINDINGS
from src.application.campaign_transcript_history import sql as history_sql,decode_history
from src.application.conversation_contract import validate_conversation
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.validation.workflow_writer_integration import Probe
from src.validation.interpreter_outcome_integration import snapshot,require
from src.validation.prompt_evaluation_remaining import transaction_interval
from src.validation.question_child_fixtures import parent_fixture,child_plan
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,DATASET_ID

EXTRA={BODY.name,RESPONSE.name,'campaign_request_authorities','invocation_prompt_bindings','product_master'}


def relocate(probe,sql):
    allowed=set(TABLES)|EXTRA
    for name in allowed:
        source='ci_product' if name=='product_master' else 'ci_application'
        sql=sql.replace(f'`{PROJECT}.{source}.{name}`',f'`{probe.scope.dataset}.{name}`')
    refs=re.findall(r'`([^`]+)`',sql)
    require(refs and all(ref in {probe.scope.dataset+'.'+n for n in allowed} for ref in refs),'UNSAFE_QUESTION_FIXTURE_SQL')
    return sql


def run(output,*,context_only=False):
    report=dict(status='FAILED',cases=[],jobs=[],serving_writes=0,model_calls=0,
        authentication='SYNTHETIC_FIXTURE_NOT_IAP',release_enabled=False)
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        probe=Probe(client);io=ControlIO(client,result_timeout_seconds=90);report['dataset']=probe.scope.dataset
        def query(sql,parameters=()):return io.read(relocate(probe,sql),list(parameters))
        def load(name,values):
            probe.own();target=client.get_table(probe.scope.dataset+'.'+name,retry=None,timeout=20)
            job=client.load_table_from_json(values,target,job_config=bigquery.LoadJobConfig(schema=target.schema,
                write_disposition='WRITE_APPEND'),job_id=probe.job_id(),location=LOCATION,timeout=20,num_retries=0)
            job.result(timeout=60,retry=None)
        def state():
            values=snapshot(probe)
            for name in EXTRA-{'product_master'}:
                part=' WHERE recorded_at BETWEEN TIMESTAMP(\'0001-01-01\') AND TIMESTAMP(\'9999-12-31\')' if name in {BODY.name,RESPONSE.name} else ''
                values[name]=sorted(query(f'SELECT ARRAY(SELECT TO_JSON_STRING(t) FROM `{DATASET_ID}.{name}` t{part} LIMIT 101) records')['records'])
            return values
        def capture(workspace):
            values=state();rows={name:[decode_row(v,name) for v in values[name] if json.loads(v)['workspace_id']==workspace]
                for name in ('workspaces','memberships','conversations','requests','model_invocations')}
            extra={name:[json.loads(v) for v in values[name] if json.loads(v)['workspace_id']==workspace]
                for name in EXTRA-{'product_master'}}
            conv=rows['conversations'][0]
            resource=validate_conversation(conv,workspace_id=workspace,conversation_id=conv['conversation_id'])
            params=[bigquery.ScalarQueryParameter('workspace_id','STRING',workspace),bigquery.ScalarQueryParameter('conversation_id','STRING',conv['conversation_id'])]
            history=decode_history(query(history_sql(),params),resource=resource,observed_at=datetime.now(timezone.utc))
            return rows,extra,history
        def scoped(plan,slot,fault=None):
            sql=relocate(probe,plan.sql)
            if fault=='rollback':sql=sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'QUESTION_FIXTURE_ROLLBACK'; COMMIT TRANSACTION;")
            return replace(plan,sql=sql,job_id='question-child-it-'+probe.scope.run_id+'-'+slot)
        def submit(plan):
            result=io.submit_once(plan)
            if result.native_status is Status.UNKNOWN:result=io.recover(plan)
            job=client.get_job(plan.job_id,location=LOCATION,retry=None,timeout=20)
            report['jobs'].append(dict(job_id=plan.job_id,status=result.native_status.value,error=job.error_result))
            return result
        def passed(name):report['cases'].append(dict(case=name,status='PASSED'));print(name,flush=True)
        def verify(plan,workspace):
            values=state();params={p.name:p.value for p in plan.config().query_parameters}
            for name,param in (('requests','request_candidate'),('model_invocations','invocation_candidate')):
                expected=decode_row(params[param],name)
                actual=[decode_row(v,name) for v in values[name] if json.loads(v).get('request_id')==expected['request_id']]
                require(actual==[expected],'CHILD_EXACT_ROW_DIFFER')
            authority=[json.loads(v) for v in values['campaign_request_authorities'] if json.loads(v)['request_id']==json.loads(params['request_candidate'])['request_id']]
            require(len(authority)==1 and authority[0]['payload_canonical_json']==params['authority_payload']
                and authority[0]['payload_sha256']==params['authority_digest'],'CHILD_AUTHORITY_DIFFER')
            for name in values:
                before=[v for v in baseline[name] if json.loads(v)['workspace_id']!=workspace]
                after=[v for v in values[name] if json.loads(v)['workspace_id']!=workspace]
                require(before==after,'UNRELATED_ROWS_CHANGED')
            return values
        try:
            probe.setup()
            for source in (BODY.table(PROJECT),RESPONSE.table(PROJECT),table_metadata(),BINDINGS['invocation_prompt_bindings'].table(PROJECT)):
                target=bigquery.Table(probe.scope.dataset+'.'+source.table_id,schema=source.schema)
                target.time_partitioning=source.time_partitioning;target.require_partition_filter=source.require_partition_filter;target.clustering_fields=source.clustering_fields
                client.create_table(target,retry=None,timeout=20)
            product=bigquery.Table(probe.scope.dataset+'.product_master',schema=[bigquery.SchemaField(k,'STRING',mode='REQUIRED') for k in ('sku','source_system','product_status')]);client.create_table(product,retry=None,timeout=20)
            load('product_master',[dict(sku='CI-SKU-0001',source_system='BEAUTYFEEDS',product_status='ACTIVE')])
            for workspace in (('child-core','child-unrelated') if context_only else ('child-core','child-rollback','child-race','child-unrelated')):
                rows,extra=parent_fixture(workspace);probe.load(rows)
                for name,values in extra.items():load(name,values)
            baseline=state();captured=capture('child-core')
            before_context=None
            if context_only:
                from unittest.mock import patch,Mock
                from src.application.campaign_question_context_reader import read_question_context
                from src.application.campaign_question_context import verify_transition
                from src.application.identity import VerifiedIdentity,ISSUER,principal_for_subject
                from src.validation.control_fixtures import SUBJECT
                import time
                def context():
                    epoch=int(time.time())
                    identity=VerifiedIdentity(principal_for_subject(ISSUER,SUBJECT),ISSUER,SUBJECT,
                        '/projects/1042059373096/locations/asia-south1/services/fixture-question-context',epoch-1,epoch+300)
                    # Authentication/access are synthetic fixtures. The wrapper
                    # still executes exactly relocated production read SQL using
                    # ControlIO native correlation; no serving source is read.
                    scoped_io=Mock(read=lambda sql,params:query(sql,params))
                    with patch('src.application.campaign_question_context_reader.ControlIO',return_value=scoped_io), \
                        patch('src.application.campaign_question_context_reader._access',return_value=('fixture-access',)):
                        return read_question_context(client,identity=identity,access_reader=Mock(),
                            workspace_id='child-core',conversation_id=captured[0]['conversations'][0]['conversation_id'])
                before_context=context()
            plan=scoped(child_plan(*captured,'core'),'core')
            result=submit(plan);require(result.native_status is Status.COMMITTED and len(result.rows)==1,'CHILD_NOT_COMMITTED')
            if context_only:
                after_context=context()
                proof=verify_transition(before_context,after_context,plan,phase='ADMITTED',checked_at=datetime.now(timezone.utc))
                report['admission_context']=dict(before_sha256=proof.before_sha256,after_sha256=proof.after_sha256,
                    plan_sha256=proof.plan_sha256,release_allowed=proof.release_allowed)
                passed('native_complete_context_exact_admission_transition')
            after=verify(plan,'child-core');passed('atomic_child_request_invocation_authority_transcript')
            recovered=io.recover(plan);require(recovered.native_status is Status.COMMITTED and recovered.rows==result.rows
                and state()==after,'ORIGINAL_RECOVERY_DIFFER');passed('original_job_recovery_preserves_all_rows')
            again=io.submit_once(plan);require(not again.submitted and again.rows==result.rows and state()==after,'DUPLICATE_RESUBMITTED');passed('duplicate_admission_uses_original_job')
            if context_only:
                report['status']='PASSED';return report
            baseline=state();captured=capture('child-rollback');bad=scoped(child_plan(*captured,'rollback'),'rollback','rollback')
            result=submit(bad);require(result.native_status is Status.NOT_COMMITTED and state()==baseline,'CHILD_ROLLBACK_UNPROVEN');passed('atomic_rollback_exact_preservation')
            baseline=state();captured=capture('child-race')
            a=scoped(child_plan(*captured,'race-a'),'race-a');b=scoped(child_plan(*captured,'race-b'),'race-b')
            with ThreadPoolExecutor(max_workers=2) as pool:
                fa=pool.submit(submit,a);fb=pool.submit(submit,b);results=[fa.result(),fb.result()]
            require(sorted(r.native_status.value for r in results)==['COMMITTED','NOT_COMMITTED'],'RACE_NO_SINGLE_WINNER')
            winner=[p for p,r in zip((a,b),results) if r.native_status is Status.COMMITTED][0]
            after=verify(winner,'child-race')
            for name in ('requests','model_invocations','campaign_request_authorities',BODY.name):
                require(len(after[name])==len(baseline[name])+1,'RACE_PARTIAL_OR_DUPLICATE')
            intervals=[]
            for p in (a,b):
                children=list(client.list_jobs(project=PROJECT,parent_job=p.job_id,max_results=64,retry=None,timeout=20))
                intervals.append(transaction_interval(children,p.job_id))
            overlap=(min(v[1] for v in intervals)-max(v[0] for v in intervals)).total_seconds()
            require(overlap>0,'NO_NATIVE_TRANSACTION_OVERLAP');report['race_overlap_seconds']=overlap
            passed('overlapping_race_one_winner_positive_rollback')
            report['status']='PASSED'
        finally:
            try:probe.cleanup()
            finally:
                report['cleanup']=probe.report['cleanup'];output.parent.mkdir(parents=True,exist_ok=True)
                output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true',required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--context-only',action='store_true');args=parser.parse_args();run(args.output,context_only=args.context_only)

if __name__=='__main__':main()
