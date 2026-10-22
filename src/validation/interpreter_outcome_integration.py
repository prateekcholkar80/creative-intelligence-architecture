"""Owned expiring BigQuery interpreter transaction/rollback/race verification."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime,timezone
import json,re
from pathlib import Path
from google.cloud import bigquery
from src.application.interpreter_outcome import prepare_interpreter_outcome,validate_outcome
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.control_io import ControlIO
from src.application.workflow_schema import TABLES
from src.application.workflow_codec import encode_row,decode_row
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.validation.workflow_writer_integration import Probe
from src.validation.control_fixtures import workflow
from src.validation.prompt_evaluation_remaining import transaction_interval
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,DATASET_ID


def require(value,code):
    if not value:raise ValueError(code)


def fixture(ws):
    rows=workflow(ws,'INTERPRETER')
    rows['model_invocations'][0].update(state='DISPATCHED',revision=2,
        worker_id='fixture-interpreter-worker',dispatch_receipt_id='fixture-dispatch')
    return rows


def plan_for(rows,state='SUCCEEDED'):
    inv=rows['model_invocations'][0]
    observation=(ModelResultObservation('SUCCEEDED',returned_model_id=inv['profile']['model_id'],
        response_sha256='a'*64,finish_reason='STOP',safety_outcome='PASSED',input_tokens=20)
        if state=='SUCCEEDED' else ModelResultObservation('UNKNOWN',safe_error_code='CALL_OUTCOME_UNKNOWN'))
    return prepare_interpreter_outcome(workspace=rows['workspaces'][0],membership=rows['memberships'][0],
        conversation=rows['conversations'][0],request=rows['requests'][0],invocation=inv,
        observation=observation,observed_at=datetime.now(timezone.utc))


def relocate(probe,plan,slot,*,fault=None):
    sql=probe.scope.sql(plan.sql)
    if fault=='rollback':sql=sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'INTERPRETER_FIXTURE_ROLLBACK';\nCOMMIT TRANSACTION;")
    elif fault=='postcommit':sql=sql.replace('COMMIT TRANSACTION;',"COMMIT TRANSACTION;\nASSERT FALSE AS 'INTERPRETER_FIXTURE_POSTCOMMIT';")
    else:require(fault is None,'INVALID_FAULT')
    refs=re.findall(r'`([^`]+)`',sql)
    require(refs and all(r.startswith(probe.scope.dataset+'.') for r in refs)
        and DATASET_ID not in sql and not re.search(r'\b(DELETE|DROP|TRUNCATE|MERGE)\b',sql,re.I),'UNSAFE_FIXTURE_SQL')
    return replace(plan,sql=sql,job_id='interpreter-it-'+probe.scope.run_id+'-'+slot)


def snapshot(probe):
    probe.own();parts=[]
    for name,spec in TABLES.items():
        where=(' WHERE '+spec.partition_field+" BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')") if spec.partition_field else ''
        parts.append(f"SELECT '{name}' name,TO_JSON_STRING(t) row FROM `{probe.scope.table(name)}` t{where}")
    sql=probe.scope.sql(' UNION ALL '.join(parts))
    job=probe.client.query(sql,job_id=probe.job_id(),location=LOCATION,retry=None,job_retry=None,timeout=20,
        job_config=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=512*1024**2,job_timeout_ms=60000))
    result={n:[] for n in TABLES}
    for row in job.result(timeout=60,max_results=1001,retry=None,job_retry=None):
        result[row['name']].append(encode_row(decode_row(row['row'],row['name']),row['name']))
    require(sum(map(len,result.values()))<1000,'FIXTURE_SNAPSHOT_OVERFLOW')
    return {n:sorted(v) for n,v in result.items()}


def unrelated(before,after,workspace):
    for name in TABLES:
        select=lambda rows:[r for r in rows if json.loads(r)['workspace_id']!=workspace]
        require(select(before[name])==select(after[name]),'UNRELATED_FIXTURE_CHANGED')


def run(output):
    report=dict(status='FAILED',cases=[],jobs=[],serving_writes=0,model_calls=0)
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        probe=Probe(client);io=ControlIO(client,result_timeout_seconds=90)
        report['dataset']=probe.scope.dataset
        def submit(plan):
            result=io.submit_once(plan)
            if result.native_status is Status.UNKNOWN:result=io.recover(plan)
            job=client.get_job(plan.job_id,location=LOCATION,retry=None,timeout=20)
            report['jobs'].append(dict(job_id=plan.job_id,status=result.native_status.value,error=job.error_result))
            return result
        def passed(name):report['cases'].append(dict(case=name,status='PASSED'));print(name,flush=True)
        try:
            probe.setup();probe.load(fixture('unrelated'))
            rows=fixture('core');probe.load(rows);before=snapshot(probe)
            plan=relocate(probe,plan_for(rows),'core')
            outcome=submit(plan);require(outcome.native_status is Status.COMMITTED,'CORE_NOT_COMMITTED')
            validate_outcome(plan,outcome);after=snapshot(probe);unrelated(before,after,'core')
            params={p.name:p.value for p in plan.config().query_parameters}
            for table,param in (('requests','request_candidate'),('model_invocations','invocation_candidate'),('workspaces','workspace_candidate')):
                actual=[r for r in after[table] if json.loads(r)['workspace_id']=='core']
                report.setdefault('core_comparisons',{})[table]=dict(actual=actual,expected=params[param])
                require(len(actual)==1 and decode_row(actual[0],table)==decode_row(params[param],table),'CORE_ROWS_DIFFER')
            require(len([r for r in after['audit_events'] if json.loads(r)['workspace_id']=='core'])==2,'AUDIT_COUNT')
            passed('atomic_commit_exact_rows_unrelated_preservation')
            validate_outcome(plan,io.recover(plan));require(snapshot(probe)==after,'RECOVERY_MUTATED');passed('same_job_recovery')
            replay=replace(plan,job_id=plan.job_id+'-replay');value=submit(replay);validate_outcome(replay,value)
            require(value.rows[0]['state']=='REUSED' and snapshot(probe)==after,'REPLAY_MUTATED');passed('new_job_exact_sql_replay')
            for fault in ('rollback','postcommit'):
                rows=fixture(fault);probe.load(rows);before=snapshot(probe)
                faulty=relocate(probe,plan_for(rows),fault,fault=fault)
                value=submit(faulty);after=snapshot(probe);unrelated(before,after,fault)
                if fault=='rollback':
                    require(value.native_status is Status.NOT_COMMITTED and after==before,'ROLLBACK_UNPROVEN')
                else:
                    require(value.native_status is Status.COMMITTED and not value.rows,'POSTCOMMIT_RECEIPT_INVENTED')
                    require(after!=before,'POSTCOMMIT_NOT_STORED')
                    require(not io.recover(faulty).rows,'RECOVERY_INVENTED_RECEIPT')
                passed(fault)
            rows=fixture('race');probe.load(rows);before=snapshot(probe)
            plans=[relocate(probe,plan_for(rows,state),'race-'+str(i)) for i,state in enumerate(('SUCCEEDED','UNKNOWN'))]
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(submit,p) for p in plans]
                results=[f.result(timeout=180) for f in futures]
            winners=[i for i,r in enumerate(results) if r.native_status is Status.COMMITTED]
            require(len(winners)==1 and results[1-winners[0]].native_status is Status.NOT_COMMITTED,'RACE_OUTCOME_UNPROVEN')
            validate_outcome(plans[winners[0]],results[winners[0]])
            intervals=[]
            for plan in plans:
                children=list(client.list_jobs(project=PROJECT,parent_job=plan.job_id,max_results=64,page_size=64,retry=None,timeout=20))
                intervals.append(transaction_interval(children,plan.job_id))
            require(max(x[0] for x in intervals)<min(x[1] for x in intervals),'RACE_NO_OVERLAP')
            report['race_overlap_seconds']=(min(x[1] for x in intervals)-max(x[0] for x in intervals)).total_seconds()
            after=snapshot(probe);unrelated(before,after,'race')
            require([json.loads(r)['revision'] for r in after['workspaces'] if json.loads(r)['workspace_id']=='race']==[2],'RACE_FENCE_COUNT')
            winner_params={p.name:p.value for p in plans[winners[0]].config().query_parameters}
            actual=[r for r in after['model_invocations'] if json.loads(r)['workspace_id']=='race']
            require(len(actual)==1 and decode_row(actual[0],'model_invocations')==decode_row(winner_params['invocation_candidate'],'model_invocations'),'RACE_PARTIAL_WINNER')
            passed('genuine_race_one_winner_positive_loser_rollback');report['status']='PASSED'
        finally:
            try:probe.cleanup()
            finally:
                report['cleanup']=probe.report['cleanup']
                output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',action='store_true',required=True)
    p.add_argument('--output',type=Path,required=True);args=p.parse_args();run(args.output)

if __name__=='__main__':main()
