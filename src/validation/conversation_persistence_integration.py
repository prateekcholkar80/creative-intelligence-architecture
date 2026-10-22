"""Isolated native user/response/next-turn persistence; no model calls or release."""
import argparse,json,re,hashlib
from pathlib import Path
from dataclasses import replace
from datetime import datetime,timezone
from google.cloud import bigquery
from src.application.control_io import ControlIO
from src.application.workflow_codec import decode_row
from src.application.workflow_schema import TABLES
from src.application.campaign_transcript import SPEC as BODY
from src.application.campaign_response_transcript import SPEC as RESPONSE
from src.application.campaign_quality_authority import table_metadata
from src.application.campaign_transcript_history import sql as history_sql,decode_history
from src.application.grounded_interpreter_submission import prepare_grounded_submission
from src.application.conversation_contract import validate_conversation
from src.application.interpreter_outcome import prepare_interpreter_outcome,validate_outcome
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.validation.control_fixtures import controls,profile,ACTOR
from src.validation.workflow_writer_integration import Probe
from src.validation.interpreter_outcome_integration import snapshot,require
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,DATASET_ID

EXTRA={BODY.name,RESPONSE.name,'campaign_request_authorities','product_master'}

def relocate(probe,sql):
    for name in set(TABLES)|EXTRA:
        dataset='ci_product' if name=='product_master' else 'ci_application'
        sql=sql.replace(f'`{PROJECT}.{dataset}.{name}`',f'`{probe.scope.dataset}.{name}`')
    refs=re.findall(r'`([^`]+)`',sql)
    require(refs and all(ref in {probe.scope.dataset+'.'+n for n in set(TABLES)|EXTRA} for ref in refs),'UNSAFE_SQL')
    return sql


def run(output):
    report=dict(status='FAILED',cases=[],jobs=[],serving_writes=0,model_calls=0)
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        probe=Probe(client);io=ControlIO(client,result_timeout_seconds=90);report['dataset']=probe.scope.dataset
        def submit(plan,slot):
            scoped=replace(plan,sql=relocate(probe,plan.sql),job_id='conversation-it-'+probe.scope.run_id+'-'+slot)
            result=io.submit_once(scoped)
            if result.native_status is Status.UNKNOWN:result=io.recover(scoped)
            native=client.get_job(scoped.job_id,location=LOCATION,retry=None,timeout=20)
            report['jobs'].append(dict(job_id=scoped.job_id,status=result.native_status.value,error=native.error_result))
            return scoped,result
        def query(sql,params=()):return io.read(relocate(probe,sql),list(params))
        def state():
            result=snapshot(probe)
            for name,partition in ((BODY.name,'recorded_at'),(RESPONSE.name,'recorded_at'),('campaign_request_authorities',None)):
                where=f" WHERE {partition} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')" if partition else ''
                result[name]=sorted(query(f'SELECT ARRAY(SELECT TO_JSON_STRING(t) FROM `{DATASET_ID}.{name}` t{where} LIMIT 101) records')['records'])
            return result
        def passed(name):report['cases'].append(dict(case=name,status='PASSED'));print(name,flush=True)
        try:
            probe.setup()
            for spec in (BODY,RESPONSE):
                source=spec.table(PROJECT);target=bigquery.Table(probe.scope.dataset+'.'+spec.name,schema=source.schema)
                target.time_partitioning=source.time_partitioning;target.require_partition_filter=source.require_partition_filter;target.clustering_fields=source.clustering_fields
                client.create_table(target,retry=None,timeout=20)
            source=table_metadata();target=bigquery.Table(probe.scope.dataset+'.campaign_request_authorities',schema=source.schema)
            target.clustering_fields=source.clustering_fields;client.create_table(target,retry=None,timeout=20)
            product=bigquery.Table(probe.scope.dataset+'.product_master',schema=[bigquery.SchemaField(k,'STRING',mode='REQUIRED') for k in ('sku','source_system','product_status')])
            client.create_table(product,retry=None,timeout=20)
            job=client.load_table_from_json([dict(sku='CI-SKU-0001',source_system='BEAUTYFEEDS',product_status='ACTIVE')],product,
                job_config=bigquery.LoadJobConfig(schema=product.schema,write_disposition='WRITE_APPEND'),job_id=probe.job_id(),location=LOCATION,timeout=20,num_retries=0)
            job.result(timeout=60,retry=None)
            rows=controls('conversation');probe.load(rows);probe.load(controls('unrelated'))
            conv=rows['conversations'][0];resource=validate_conversation(conv,workspace_id='conversation',conversation_id=conv['conversation_id'])
            params=[bigquery.ScalarQueryParameter('workspace_id','STRING','conversation'),bigquery.ScalarQueryParameter('conversation_id','STRING',conv['conversation_id'])]
            def history():return decode_history(query(history_sql(),params),resource=resource,observed_at=datetime.now(timezone.utc))
            def build(key,captured):
                current=snapshot(probe);ws=next(decode_row(r,'workspaces') for r in current['workspaces'] if json.loads(r)['workspace_id']=='conversation')
                return prepare_grounded_submission(selection=dict(sku='CI-SKU-0001',country='US',channel='META',audience='NEW'),history=captured,
                    workspace=ws,membership=rows['memberships'][0],conversation=conv,resource=resource,principal_id=ACTOR,
                    message='Message '+key,retry_key=key,profile=profile('INTERPRETER'),observed_at=datetime.now(timezone.utc),worker_id='fixture-worker')
            first,result=submit(build('one',history()),'first');require(result.native_status is Status.COMMITTED,'FIRST_NOT_COMMITTED');passed('native_initial_history_capture')
            values={p.name:p.value for p in first.config().query_parameters}
            req=decode_row(values['request_candidate'],'requests');inv=decode_row(values['invocation_candidate'],'model_invocations')
            # Fixture-only dispatch staging. This does not authenticate a user or exercise registered dispatch.
            query(f'''BEGIN TRANSACTION; UPDATE `{DATASET_ID}.model_invocations` SET state='DISPATCHED',revision=2,
 worker_id='fixture-worker',dispatch_receipt_id='fixture-dispatch' WHERE workspace_id='conversation'
 AND invocation_id=@invocation AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31');
 ASSERT @@row_count=1 AS 'FIXTURE_DISPATCH_COUNT'; COMMIT TRANSACTION; SELECT TRUE staged;''',
                [bigquery.ScalarQueryParameter('invocation','STRING',inv['invocation_id'])])
            inv.update(state='DISPATCHED',revision=2,worker_id='fixture-worker',dispatch_receipt_id='fixture-dispatch')
            raw='{"schema_version":"fixture-v1","features":[]}'
            observation=ModelResultObservation('SUCCEEDED',response_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                returned_model_id=inv['profile']['model_id'],finish_reason='STOP',safety_outcome='PASSED',input_tokens=20)
            def outcome_plan():
                ws=next(decode_row(r,'workspaces') for r in snapshot(probe)['workspaces'] if json.loads(r)['workspace_id']=='conversation')
                return prepare_interpreter_outcome(workspace=ws,membership=rows['memberships'][0],conversation=conv,request=req,invocation=inv,
                    response_text=raw,observation=observation,observed_at=datetime.now(timezone.utc))
            before=state();bad=outcome_plan();bad=replace(bad,sql=bad.sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'RESPONSE_FIXTURE_ROLLBACK'; COMMIT TRANSACTION;"))
            _,result=submit(bad,'response-rollback');require(result.native_status is Status.NOT_COMMITTED and state()==before,'RESPONSE_ROLLBACK_UNPROVEN');passed('response_atomic_rollback')
            plan,result=submit(outcome_plan(),'response');require(result.native_status is Status.COMMITTED,'RESPONSE_NOT_COMMITTED');validate_outcome(plan,result)
            after=state();body=json.loads(after[RESPONSE.name][0]);require(body['response_text']==raw and body['content_sha256']==observation.response_sha256,'RAW_RESPONSE_DIFFER')
            passed('atomic_response_outcome_accounting')
            validate_outcome(plan,io.recover(plan));require(state()==after,'RESPONSE_RECOVERY_MUTATED');passed('original_response_recovery')
            captured=history();require(captured.texts==('Message one',raw),'NATIVE_HISTORY_DIFFER');passed('native_complete_user_response_history')
            second,result=submit(build('two',captured),'second');require(result.native_status is Status.COMMITTED,'SECOND_NOT_COMMITTED')
            values={p.name:p.value for p in second.config().query_parameters};batch=json.loads(json.loads(values['transcript_candidate'])['batch_canonical_json'])
            require([r['text'] for r in batch]==['Message two'],'SECOND_TRANSCRIPT_DIFFER')
            authority=json.loads(values['authority_payload']);require(len(authority['transcript'])==3,'SECOND_AUTHORITY_COVERAGE')
            from src.application.campaign_transcript_history import read_bound_texts
            bound=query(history_sql(),params)
            from unittest.mock import patch,Mock
            # Pure decode of the fresh native result; no source or acceptance values invented.
            with patch('src.application.campaign_transcript_history.ControlIO',return_value=Mock(read=Mock(return_value=bound))):
                texts=read_bound_texts(client,dict(authority=authority),workspace_id='conversation',conversation_id=conv['conversation_id'],clock=lambda:datetime.now(timezone.utc))
            require(texts==('Message one',raw,'Message two'),'RECONSTRUCTED_TRANSCRIPT_DIFFER');passed('next_turn_preserves_complete_transcript')
            before=state();_,result=submit(build('stale',captured),'stale')
            require(result.native_status is Status.NOT_COMMITTED and state()==before,'STALE_HISTORY_MUTATED');passed('changed_history_denied_exact_preservation')
            try:history()
            except ValueError:passed('unfinished_next_turn_denied')
            else:raise ValueError('UNFINISHED_HISTORY_ACCEPTED')
            report['status']='PASSED'
        finally:
            try:probe.cleanup()
            finally:
                report['cleanup']=probe.report['cleanup'];output.parent.mkdir(parents=True,exist_ok=True)
                output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true',required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();run(args.output)

if __name__=='__main__':main()
