"""Native omission transactions; synthetic completed history in an owned fixture."""
import argparse,hashlib,json,re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
from google.cloud import bigquery
from src.application.campaign_omission_turn import OmissionContext,bind_omission_plan
from src.application.grounded_interpreter_submission import prepare_grounded_submission
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_transcript_history import TABLES as HISTORY,decode_history
from src.application.campaign_transcript import SPEC as BODY
from src.application.campaign_response_transcript import SPEC as RESPONSE
from src.application.campaign_quality_authority import table_metadata as authority_metadata
from src.application.campaign_document_store import table_metadata as document_metadata,SUBMISSIONS
from src.application.campaign_clarification_policy import DATASET,VERSIONS,REVOCATIONS
from src.application.interpreter_outcome import prepare_interpreter_outcome
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.workflow_codec import decode_row
from src.application.conversation_contract import validate_conversation
from src.application.control_io import ControlIO,ControlPlan
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.validation.control_fixtures import controls,profile,ACTOR
from src.validation.workflow_writer_integration import Probe
from src.validation.interpreter_outcome_integration import snapshot,unrelated,require
from src.application.workflow_schema import TABLES
from src.ingestion.setup_workflow_tables import DATASET_ID
from src.validation.prompt_evaluation_remaining import transaction_interval
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

EXTRAS=('campaign_request_authorities','product_master',BODY.name,RESPONSE.name,'submissions','campaign_evidence_turn_links','policy_versions','policy_revocations')


def relocate(probe,plan,slot,fault=None):
    sql=plan.sql.replace('`'+SUBMISSIONS+'`','`'+probe.scope.dataset+'.submissions`')
    for name,target in ((VERSIONS,'policy_versions'),(REVOCATIONS,'policy_revocations')):
        sql=sql.replace('`'+DATASET+'.'+name+'`','`'+probe.scope.dataset+'.'+target+'`')
    for name in ('campaign_evidence_turn_links',RESPONSE.name):
        sql=sql.replace('`'+PROJECT+'.ci_application.'+name+'`','`'+probe.scope.dataset+'.'+name+'`')
    for name in ('campaign_request_authorities','product_master'):
        dataset='ci_product' if name=='product_master' else 'ci_application'
        sql=sql.replace(f'`{PROJECT}.{dataset}.{name}`',f'`{probe.scope.dataset}.{name}`')
    for name in set(TABLES)|{BODY.name}:
        sql=sql.replace(f'`{DATASET_ID}.{name}`',f'`{probe.scope.dataset}.{name}`')
    refs=re.findall(r'`([^`]+)`',sql)
    require(refs and all(ref.startswith(probe.scope.dataset+'.') and ref.rsplit('.',1)[-1] in set(EXTRAS)|set(TABLES) for ref in refs)
        and not re.search(r'\b(DELETE|DROP|TRUNCATE|MERGE)\b',sql,re.I),'OMISSION_UNSAFE_SQL')
    require(sql.count('COMMIT TRANSACTION;')==1,'FAULT_MARKER')
    if fault=='rollback':sql=sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'SUBMIT_FIXTURE_ROLLBACK';\nCOMMIT TRANSACTION;")
    else:require(fault is None,'INVALID_FAULT')
    return replace(plan,sql=sql,job_id='omission-it-'+probe.scope.run_id+'-'+slot)


def run(output):
    if output.exists():raise ValueError('OMISSION_REPORT_EXISTS')
    report=dict(status='RUNNING',cases=[],jobs=[],model_calls=0,serving_writes=0,release_enabled=False,
        scope='Native relocated SQL and ControlIO; synthetic completed parent, profiles and policy; no deployed IAP or OmissionTurnService runtime-creator recovery proof')
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        probe=Probe(client);report['dataset']=probe.scope.dataset;io=ControlIO(client,result_timeout_seconds=90)
        def passed(name):report['cases'].append(dict(name=name,status='PASSED'));save();print(name,flush=True)
        def query(sql,params=()):
            refs=re.findall(r'`([^`]+)`',sql);require(refs and all(r.startswith(probe.scope.dataset+'.') for r in refs),'UNSAFE_READ')
            job=client.query(sql,job_id=probe.job_id(),location=LOCATION,retry=None,job_retry=None,timeout=20,
                job_config=bigquery.QueryJobConfig(query_parameters=list(params),use_query_cache=False,maximum_bytes_billed=128*1024**2,job_timeout_ms=60000))
            return list(job.result(timeout=60,retry=None,job_retry=None))
        def extra_state():
            result={}
            for name in EXTRAS:
                partition=' WHERE recorded_at BETWEEN TIMESTAMP(\'0001-01-01\') AND TIMESTAMP(\'9999-12-31\')' if name in (BODY.name,RESPONSE.name) else ''
                result[name]=sorted(r['row'] for r in query(f'SELECT TO_JSON_STRING(t) row FROM `{probe.scope.dataset}.{name}` t'+partition))
            return result
        def state():return snapshot(probe),extra_state()
        def load(name,values):
            target=client.get_table(probe.scope.dataset+'.'+name,retry=None,timeout=20)
            from src.validation.signing_read_integration import json_rows
            client.load_table_from_json(json_rows(values),target,job_id=probe.job_id(),location=LOCATION,timeout=20,num_retries=0,
                job_config=bigquery.LoadJobConfig(schema=target.schema,write_disposition='WRITE_APPEND')).result(timeout=60,retry=None)
        def create(name,metadata):
            target=bigquery.Table(probe.scope.dataset+'.'+name,schema=metadata.schema)
            target.time_partitioning=metadata.time_partitioning;target.require_partition_filter=metadata.require_partition_filter;target.clustering_fields=metadata.clustering_fields
            client.create_table(target,exists_ok=False,retry=None,timeout=20)
        def build(rows,context,history):
            conv=rows['conversations'][0]
            plan=prepare_grounded_submission(selection=context.value['selection'],history=history,workspace=rows['workspaces'][0],membership=rows['memberships'][0],conversation=conv,
                resource=validate_conversation(conv,workspace_id=conv['workspace_id'],conversation_id=conv['conversation_id']),principal_id=ACTOR,message=context.message,
                retry_key='omission-v1-'+context.value['submission_id'],profile=profile('INTERPRETER'),observed_at=datetime.now(timezone.utc),worker_id='fixture-worker')
            return bind_omission_plan(plan,context)
        def seed(name):
            rows=controls(name);probe.load(rows);conv=rows['conversations'][0];resource=validate_conversation(conv,workspace_id=name,conversation_id=conv['conversation_id'])
            selection=dict(sku='CI-SKU-0001',country='US',channel='META',audience='Fixture shoppers')
            parent=prepare_grounded_submission(selection=selection,workspace=rows['workspaces'][0],membership=rows['memberships'][0],conversation=conv,resource=resource,
                principal_id=ACTOR,message='Synthetic parent only',retry_key='parent',profile=profile('INTERPRETER'),observed_at=datetime.now(timezone.utc),worker_id='fixture-worker')
            p={v.name:v.value for v in parent.config().query_parameters};request=decode_row(p['request_candidate'],'requests');call=decode_row(p['invocation_candidate'],'model_invocations')
            call.update(state='DISPATCHED',revision=2,worker_id='fixture-worker',dispatch_receipt_id='fixture-dispatch')
            response='{"features":[]}'
            outcome=prepare_interpreter_outcome(workspace=rows['workspaces'][0],membership=rows['memberships'][0],conversation=conv,request=request,invocation=call,response_text=response,
                observation=ModelResultObservation('SUCCEEDED',response_sha256=hashlib.sha256(response.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED',returned_model_id=call['profile']['model_id']),observed_at=datetime.now(timezone.utc))
            o={v.name:v.value for v in outcome.config().query_parameters}
            probe.load(dict(requests=[decode_row(o['request_candidate'],'requests')],model_invocations=[decode_row(o['invocation_candidate'],'model_invocations')]))
            load(BODY.name,[json.loads(p['transcript_candidate'])]);load(RESPONSE.name,[json.loads(o['response_candidate'])])
            authority=json.loads(p['authority_payload'])
            load('campaign_request_authorities',[dict(workspace_id=name,conversation_id=conv['conversation_id'],request_id=request['request_id'],principal_id=ACTOR,payload_canonical_json=p['authority_payload'],payload_sha256=p['authority_digest'],recorded_at=request['created_at'])])
            sid=hashlib.sha256(name.encode()).hexdigest();policy_sha=hashlib.sha256(b'{}').hexdigest()
            source=dict(scope=dict(workspace_id=name,conversation_id=conv['conversation_id'],request_id=request['request_id']),principal_id=ACTOR,requirement_id='fixture-gap',file=None,
                selection=dict(choice='OMIT_UNSUPPORTED_CONTENT',status='REQUEST_CHANGE_PENDING',alternative='FACTUAL_PRODUCT_COPY'),collection_request=dict(missing_fields=['study_report_reference']))
            load('submissions',[dict(submission_id=sid,record_json=canonical(source),recorded_at=datetime.now(timezone.utc))])
            row={'observed_at':datetime.now(timezone.utc)}
            for key,(table,partition) in HISTORY.items():
                where=f"workspace_id='{name}' AND conversation_id='{conv['conversation_id']}'"+(f" AND {partition} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')" if partition else '')
                row[key]=[r['row'] for r in query(f'SELECT TO_JSON_STRING(t) row FROM `{probe.scope.dataset}.{table}` t WHERE '+where)];row[key+'_count']=len(row[key])
            history=decode_history(row,resource=resource,observed_at=datetime.now(timezone.utc))
            context=OmissionContext(canonical(dict(source=source,submission_id=sid,selection=selection,parent_authority=authority,
                policy=dict(policy_id='fixture-policy',version='v1',policy_sha256=policy_sha,policy_json='{}'),generation_authorized=False)))
            return rows,context,history
        def submit(plan):
            result=io.submit_once(plan)
            if result.native_status is Status.UNKNOWN:result=io.recover(plan)
            job=client.get_job(plan.job_id,location=LOCATION,retry=None,timeout=20)
            report['jobs'].append(dict(job_id=plan.job_id,status=result.native_status.value,error=job.error_result));save()
            return result
        def verify(plan,before,after,name):
            params={v.name:v.value for v in plan.config().query_parameters}
            unrelated(before[0],after[0],name)
            for table,param in (('requests','request_candidate'),('model_invocations','invocation_candidate')):
                expected=decode_row(params[param],table);matches=[decode_row(r,table) for r in after[0][table] if json.loads(r).get('request_id')==expected['request_id'] and json.loads(r)['workspace_id']==name]
                require(matches==[expected],'OMISSION_EXACT_ROW')
                require(set(before[0][table]).issubset(after[0][table]),'PARENT_CHANGED')
            for table in EXTRAS:
                require(set(before[1][table]).issubset(after[1][table]),'EXTRA_ORIGINAL_CHANGED')
                expected_delta=1 if table in ('campaign_request_authorities',BODY.name,'campaign_evidence_turn_links') else 0
                require(len(after[1][table])==len(before[1][table])+expected_delta,'EXTRA_DELTA')
            link=[json.loads(r) for r in after[1]['campaign_evidence_turn_links'] if json.loads(r)['record_id']==params['omission_link_id']]
            require(len(link)==1 and link[0]['record_json']==params['omission_link'],'LINK_CHANGED')
        try:
            save();probe.setup();probe.load(controls('unrelated'))
            create('campaign_request_authorities',authority_metadata());create(BODY.name,BODY.table(PROJECT));create(RESPONSE.name,RESPONSE.table(PROJECT));create('campaign_evidence_turn_links',document_metadata('turn_links'))
            for name,fields in (('submissions',('submission_id','record_json')),('product_master',('sku','source_system','product_status')),('policy_versions',('policy_id','version','policy_sha256','policy_canonical_json')),('policy_revocations',('policy_id','version','policy_sha256'))):
                create(name,bigquery.Table(probe.scope.dataset+'.'+name,schema=[bigquery.SchemaField(f,'STRING',mode='REQUIRED') for f in fields]+([bigquery.SchemaField('recorded_at','TIMESTAMP',mode='REQUIRED')] if name=='submissions' else [])))
            load('product_master',[dict(sku='CI-SKU-0001',source_system='BEAUTYFEEDS',product_status='ACTIVE')]);load('policy_versions',[dict(policy_id='fixture-policy',version='v1',policy_sha256=hashlib.sha256(b'{}').hexdigest(),policy_canonical_json='{}')])
            for name,fault in (('core',None),('rollback','rollback'),('source',None),('lost',None)):
                rows,context,history=seed(name);before=state();base=build(rows,context,history)
                plan=relocate(probe,base,name,fault)
                if name=='source':
                    cfg=plan.config();cfg.query_parameters=[bigquery.ScalarQueryParameter(p.name,p.type_,'{}' if p.name=='omission_source' else p.value) for p in cfg.query_parameters];plan=replace(plan,config_json=json.dumps(cfg.to_api_repr()))
                if name=='lost':
                    real=client.query
                    def lose(*a,**kw):real(*a,**kw);raise TimeoutError('Synthetic lost response')
                    with patch.object(client,'query',side_effect=lose):require(io.submit_once(plan).native_status is Status.UNKNOWN,'FAULT_NOT_UNKNOWN')
                    result=ControlIO(client,result_timeout_seconds=90).recover(ControlPlan(**{f:getattr(plan,f) for f in plan.__dataclass_fields__}))
                else:result=submit(plan)
                after=state()
                if fault or name=='source':
                    require(result.native_status is Status.NOT_COMMITTED and before==after,'ROLLBACK_UNPROVEN')
                    require(('SUBMIT_FIXTURE_ROLLBACK' if fault else 'OMISSION_SOURCE_CHANGED') in str(report['jobs'][-1]['error']),'WRONG_DENIAL')
                else:
                    require(result.native_status is Status.COMMITTED,'OMISSION_NOT_COMMITTED');verify(plan,before,after,name)
                passed(name)
                if name=='core':
                    require(ControlIO(client).recover(plan).native_status is Status.COMMITTED and state()==after,'COLD_RECOVERY_CHANGED');passed('cold_original_job_recovery')
                    replay=submit(replace(plan,job_id=plan.job_id+'-replay'));require(replay.native_status is Status.COMMITTED and replay.rows[0]['state']=='REUSED' and state()==after,'REPLAY_CHANGED');passed('exact_replay')
            rows,context,history=seed('race');before=state();base=build(rows,context,history)
            plans=[relocate(probe,base,'race-'+str(i)) for i in range(2)]
            with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(submit,plans))
            winners=[i for i,r in enumerate(results) if r.native_status is Status.COMMITTED]
            require(len(winners)==1 and results[1-winners[0]].native_status is Status.NOT_COMMITTED,'RACE_UNPROVEN')
            intervals=[transaction_interval(list(client.list_jobs(project=PROJECT,parent_job=p.job_id,max_results=64,page_size=64,retry=None,timeout=20)),p.job_id) for p in plans]
            overlap=(min(t[1] for t in intervals)-max(t[0] for t in intervals)).total_seconds();require(overlap>0,'RACE_NO_OVERLAP');report['race_overlap_seconds']=overlap
            verify(plans[winners[0]],before,state(),'race');passed('overlapping_competing_operations_one_winner_positive_rollback')
            report['status']='PASSED'
        except BaseException as error:report.update(status='FAILED',safe_error_type=type(error).__name__,safe_error=str(error)[:1500]);raise
        finally:
            try:probe.cleanup()
            finally:report['cleanup']=probe.report['cleanup'];save()
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',action='store_true',required=True);p.add_argument('--output',type=Path,required=True);args=p.parse_args();run(args.output)


if __name__=='__main__':main()
