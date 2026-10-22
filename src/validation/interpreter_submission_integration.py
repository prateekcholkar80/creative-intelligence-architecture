"""Opt-in isolated native interpreter submission verification; never serving writes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
import json,re
from pathlib import Path
from google.cloud import bigquery
from src.application.interpreter_submission import prepare_interpreter_submission
from src.application.campaign_transcript import SPEC
from src.application.conversation_contract import validate_conversation
from src.application.control_io import ControlIO
from src.application.workflow_schema import TABLES
from src.application.workflow_codec import decode_row
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.validation.control_fixtures import controls,profile,ACTOR
from src.validation.workflow_writer_integration import Probe
from src.validation.interpreter_outcome_integration import snapshot,unrelated,require
from src.validation.prompt_evaluation_remaining import transaction_interval
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,DATASET_ID


def plan_for(rows,message='Create a campaign'):
    ws=rows['workspaces'][0];conv=rows['conversations'][0]
    return prepare_interpreter_submission(workspace=ws,membership=rows['memberships'][0],conversation=conv,
        resource=validate_conversation(conv,workspace_id=ws['workspace_id'],conversation_id=conv['conversation_id']),
        principal_id=ACTOR,message=message,retry_key='fixture-turn',profile=profile('INTERPRETER'),
        observed_at=datetime.now(timezone.utc),worker_id='fixture-submission')


def relocate(probe,plan,slot,fault=None):
    # The transcript table is additional to the existing workflow fixture catalog.
    source=f'`{DATASET_ID}.{SPEC.name}`';target=f'`{probe.scope.dataset}.{SPEC.name}`'
    sql=plan.sql.replace(source,target)
    for name in ('campaign_request_authorities','product_master'):
        dataset='ci_product' if name=='product_master' else 'ci_application'
        sql=sql.replace(f'`{PROJECT}.{dataset}.{name}`',f'`{probe.scope.dataset}.{name}`')
    for name in TABLES:
        sql=sql.replace(f'`{DATASET_ID}.{name}`',f'`{probe.scope.table(name)}`')
    refs=re.findall(r'`([^`]+)`',sql)
    allowed={probe.scope.table(n) for n in TABLES}|{target.strip('`')}|{probe.scope.dataset+'.'+n for n in ('campaign_request_authorities','product_master')}
    require(refs and all(r in allowed for r in refs) and DATASET_ID not in sql
        and not re.search(r'\b(DELETE|DROP|TRUNCATE|MERGE)\b',sql,re.I),'UNSAFE_FIXTURE_SQL')
    require(sql.count('COMMIT TRANSACTION;')==1,'FAULT_MARKER')
    if fault=='rollback':sql=sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'SUBMIT_FIXTURE_ROLLBACK';\nCOMMIT TRANSACTION;")
    elif fault=='postcommit':sql=sql.replace('COMMIT TRANSACTION;',"COMMIT TRANSACTION;\nASSERT FALSE AS 'SUBMIT_FIXTURE_POSTCOMMIT';")
    else:require(fault is None,'INVALID_FAULT')
    return replace(plan,sql=sql,job_id='interpreter-submit-it-'+probe.scope.run_id+'-'+slot)


def bodies(probe):
    probe.own()
    job=probe.client.query(f"SELECT TO_JSON_STRING(t) row FROM `{probe.scope.dataset}.{SPEC.name}` t WHERE recorded_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')",
        job_id=probe.job_id(),location=LOCATION,retry=None,job_retry=None,timeout=20,
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=64*1024**2,use_query_cache=False,job_timeout_ms=60000))
    rows=[json.loads(r['row']) for r in job.result(timeout=60,max_results=1001,retry=None,job_retry=None)]
    require(len(rows)<1000,'BODY_OVERFLOW')
    return sorted(rows,key=lambda r:(r['workspace_id'],r['request_id']))


def verify(probe,plan,workspace):
    params={p.name:p.value for p in plan.config().query_parameters};stored=snapshot(probe)
    for name,param in (('requests','request_candidate'),('model_invocations','invocation_candidate'),('workspaces','workspace_candidate')):
        actual=[r for r in stored[name] if json.loads(r)['workspace_id']==workspace]
        require(len(actual)==1 and decode_row(actual[0],name)==decode_row(params[param],name),'EXACT_ROWS_DIFFER')
    expected=json.loads(params['transcript_candidate']);actual=[r for r in bodies(probe) if r['workspace_id']==workspace]
    require(len(actual)==1,'BODY_COUNT')
    for row in (expected,actual[0]):row['recorded_at']=datetime.fromisoformat(row['recorded_at'].replace('Z','+00:00'))
    require(actual[0]==expected,'BODY_DIFFER')
    actual_audits=[decode_row(r,'audit_events') for r in stored['audit_events'] if json.loads(r)['workspace_id']==workspace]
    expected_audits=[decode_row(params[n],'audit_events') for n in ('request_audit','invocation_audit')]
    require(sorted(actual_audits,key=lambda r:r['audit_id'])==sorted(expected_audits,key=lambda r:r['audit_id']),'AUDITS_DIFFER')


def run(output,*,grounded=False,guards_only=False):
    report=dict(status='FAILED',cases=[],jobs=[],serving_writes=0,model_calls=0)
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        probe=Probe(client);io=ControlIO(client,result_timeout_seconds=90);report['dataset']=probe.scope.dataset
        def submit(plan):
            value=io.submit_once(plan)
            if value.native_status is Status.UNKNOWN:value=io.recover(plan)
            job=client.get_job(plan.job_id,location=LOCATION,retry=None,timeout=20)
            report['jobs'].append(dict(job_id=plan.job_id,status=value.native_status.value,error=job.error_result))
            return value
        def passed(name):report['cases'].append(dict(case=name,status='PASSED'));print(name,flush=True)
        def authority_rows():
            if not grounded:return []
            probe.own()
            job=client.query(f'SELECT TO_JSON_STRING(t) row FROM `{probe.scope.dataset}.campaign_request_authorities` t',
                job_id=probe.job_id(),location=LOCATION,retry=None,job_retry=None,timeout=20,
                job_config=bigquery.QueryJobConfig(maximum_bytes_billed=64*1024**2,use_query_cache=False))
            return sorted(r['row'] for r in job.result(timeout=30,max_results=1001,retry=None,job_retry=None))
        def state():return snapshot(probe),bodies(probe),authority_rows()
        def build(rows,message='Create a campaign',sku='CI-SKU-0001'):
            if not grounded:return plan_for(rows,message)
            from src.application.grounded_interpreter_submission import prepare_grounded_submission
            ws=rows['workspaces'][0];conv=rows['conversations'][0]
            return prepare_grounded_submission(selection=dict(sku=sku,country='US',channel='META',audience='Fixture shoppers'),
                workspace=ws,membership=rows['memberships'][0],conversation=conv,
                resource=validate_conversation(conv,workspace_id=ws['workspace_id'],conversation_id=conv['conversation_id']),
                principal_id=ACTOR,message=message,retry_key='fixture-turn',profile=profile('INTERPRETER'),
                observed_at=datetime.now(timezone.utc),worker_id='fixture-submission')
        def verify_capture(plan,name):
            verify(probe,plan,name)
            if grounded:
                params={p.name:p.value for p in plan.config().query_parameters}
                rows=[json.loads(r) for r in authority_rows() if json.loads(r)['workspace_id']==name]
                require(len(rows)==1 and rows[0]['payload_canonical_json']==params['authority_payload']
                    and rows[0]['payload_sha256']==params['authority_digest'],'AUTHORITY_ROWS_DIFFER')
        try:
            probe.setup()
            if grounded:
                from src.application.campaign_quality_authority import table_metadata
                source=table_metadata();target=bigquery.Table(probe.scope.dataset+'.campaign_request_authorities',schema=source.schema)
                target.clustering_fields=source.clustering_fields;client.create_table(target,retry=None,timeout=20)
                product=bigquery.Table(probe.scope.dataset+'.product_master',schema=[bigquery.SchemaField(k,'STRING',mode='REQUIRED') for k in ('sku','source_system','product_status')])
                client.create_table(product,retry=None,timeout=20)
                load=client.load_table_from_json([dict(sku='CI-SKU-0001',source_system='BEAUTYFEEDS',product_status='ACTIVE')],product,
                    job_config=bigquery.LoadJobConfig(schema=product.schema,write_disposition='WRITE_APPEND'),
                    job_id=probe.job_id(),location=LOCATION,timeout=20,num_retries=0)
                load.result(timeout=60,retry=None)
            table=SPEC.table(PROJECT);target=bigquery.Table(probe.scope.dataset+'.'+SPEC.name,schema=table.schema)
            target.time_partitioning=table.time_partitioning;target.require_partition_filter=table.require_partition_filter;target.clustering_fields=table.clustering_fields
            client.create_table(target,retry=None,timeout=20)
            probe.load(controls('unrelated'))
            scenarios=(('core',None),) if guards_only else (('core',None),('rollback','rollback'),('postcommit','postcommit'))
            for name,fault in scenarios:
                rows=controls(name);probe.load(rows);before=state();plan=relocate(probe,build(rows),name,fault)
                outcome=submit(plan);after=state();unrelated(before[0],after[0],name)
                require([r for r in before[2] if json.loads(r)['workspace_id']!=name]==[r for r in after[2] if json.loads(r)['workspace_id']!=name],'UNRELATED_AUTHORITY_CHANGED')
                require([r for r in before[1] if r['workspace_id']!=name]==[r for r in after[1] if r['workspace_id']!=name],'UNRELATED_BODY_CHANGED')
                if fault=='rollback':require(outcome.native_status is Status.NOT_COMMITTED and before==after,'ROLLBACK_UNPROVEN')
                else:
                    require(outcome.native_status is Status.COMMITTED,'NOT_COMMITTED');verify_capture(plan,name)
                    if fault:require(not outcome.rows and not io.recover(plan).rows,'INVENTED_RECEIPT')
                passed(name)
                if name=='core' and not guards_only:
                    require(io.recover(plan).native_status is Status.COMMITTED and state()==after,'RECOVERY_MUTATED');passed('original_job_recovery')
                    result=submit(replace(plan,job_id=plan.job_id+'-replay'))
                    require(result.native_status is Status.COMMITTED and result.rows[0]['state']=='REUSED' and state()==after,'REPLAY_MUTATED');passed('exact_replay')
                    conflict=relocate(probe,build(rows,'Conflicting message'),'conflict')
                    require(submit(conflict).native_status is Status.NOT_COMMITTED and state()==after,'CONFLICT_MUTATED');passed('changed_message_denied')
            if guards_only:
                require(grounded,'GUARDS_REQUIRE_GROUNDED')
                from src.validation.control_fixtures import workflow
                for name in ('priorhistory','missingproduct'):
                    rows=controls(name);probe.load(rows)
                    if name=='priorhistory':
                        history=workflow(name,'INTERPRETER')
                        probe.load({k:history[k] for k in ('requests','model_invocations')})
                    before=state();plan=relocate(probe,build(rows,sku='CI-SKU-0002' if name=='missingproduct' else 'CI-SKU-0001'),name)
                    outcome=submit(plan)
                    expected='CHAT_PRODUCT_UNAVAILABLE' if name=='missingproduct' else 'CHAT_PRIOR_TRANSCRIPT_CAPTURE_REQUIRED'
                    require(outcome.native_status is Status.NOT_COMMITTED and expected in str(report['jobs'][-1]['error']) and state()==before,'GUARD_NOT_PROVEN')
                    passed(name+'_denied_exact_preservation')
                report['status']='PASSED';return report
            rows=controls('race');probe.load(rows);before=state()
            plans=[relocate(probe,build(rows,msg),'race-'+str(i)) for i,msg in enumerate(('First campaign','Second campaign'))]
            with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(submit,plans))
            winners=[i for i,r in enumerate(results) if r.native_status is Status.COMMITTED]
            require(len(winners)==1 and results[1-winners[0]].native_status is Status.NOT_COMMITTED,'RACE_UNPROVEN')
            intervals=[transaction_interval(list(client.list_jobs(project=PROJECT,parent_job=p.job_id,max_results=64,page_size=64,retry=None,timeout=20)),p.job_id) for p in plans]
            overlap=(min(x[1] for x in intervals)-max(x[0] for x in intervals)).total_seconds()
            require(overlap>0,'RACE_NO_OVERLAP');report['race_overlap_seconds']=overlap
            verify_capture(plans[winners[0]],'race');after=state();unrelated(before[0],after[0],'race')
            require([r for r in before[2] if json.loads(r)['workspace_id']!='race']==[r for r in after[2] if json.loads(r)['workspace_id']!='race'],'RACE_UNRELATED_AUTHORITY_CHANGED')
            require([r for r in before[1] if r['workspace_id']!='race']==[r for r in after[1] if r['workspace_id']!='race'],'RACE_UNRELATED_BODY_CHANGED')
            passed('overlapping_race_complete_winner_rollback_loser');report['status']='PASSED'
        finally:
            try:probe.cleanup()
            finally:
                report['cleanup']=probe.report['cleanup'];output.parent.mkdir(parents=True,exist_ok=True)
                output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true',required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--grounded',action='store_true');parser.add_argument('--guards-only',action='store_true')
    args=parser.parse_args();run(args.output,grounded=args.grounded,guards_only=args.guards_only)

if __name__=='__main__':main()
