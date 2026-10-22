"""Explicit isolated native verification of input/transcript transaction extensions."""
import argparse,json,hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta,timezone
from pathlib import Path
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.application.campaign_dispatch_inputs import SPEC,claim_sql,VERSION
from src.application.campaign_transcript import SPEC as TRANSCRIPT,extend_publication
from src.application.campaign_quality_authority import table_metadata
from src.application.campaign_acceptance_contract import _hash
from src.application.control_io import query_config
from src.application.sharing_jobs import matches_sharing_job,failed_sharing_job_status
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,DATASET_ID


def require(value):
    if not value:raise ValueError('PLANNER_NATIVE_ASSERTION')


def main():
    p=argparse.ArgumentParser(description=__doc__);mode=p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--run',action='store_true');mode.add_argument('--verify-report',type=Path)
    p.add_argument('--output-dir',type=Path,required=True);args=p.parse_args()
    if args.verify_report:
        report=json.loads(args.verify_report.read_text());run=report['run_id']
        require(len(run)==32 and all(ch in '0123456789abcdef' for ch in run) and report['status']=='PASSED'
            and 1<=len(report['jobs'])<=50)
        with bigquery.Client(project=PROJECT,location=LOCATION) as c:
            for item in report['jobs']:
                require(item['job_id'].startswith('planner_inputs_it_'+run+'_'))
                job=c.get_job(item['job_id'],location=LOCATION,retry=None,timeout=20)
                require(job.state=='DONE' and job.error_result==item['error'])
            report['all_native_jobs_terminal_independently_verified']=True
            args.verify_report.write_text(json.dumps(report,indent=2,default=str)+'\n')
            print(json.dumps(dict(status='VERIFIED_TERMINAL',jobs=len(report['jobs']),writes=0)))
        return
    run=uuid4().hex;dataset=f'{PROJECT}.ci_planner_inputs_it_{run}';prefix=f'planner_inputs_it_{run}_'
    report=dict(run_id=run,status='FAILED',cases=[],jobs=[],cleanup='PENDING',serving_writes=0,model_calls=0)
    with bigquery.Client(project=PROJECT,location=LOCATION) as c:
        def query(sql,params=(),name=None):
            require(f'`{DATASET_ID}.' not in sql)
            config=query_config(list(params));jid=prefix+(name or uuid4().hex)
            job=c.query(sql,job_config=config,job_id=jid,location=LOCATION,retry=None,job_retry=None,timeout=20)
            try:rows=[dict(r) for r in job.result(timeout=60,retry=None,job_retry=None)]
            except Exception:rows=[]
            native=c.get_job(jid,location=LOCATION,retry=None,timeout=20)
            require(native.state=='DONE' and matches_sharing_job(native,job_id=jid,sql=sql,config=config))
            state='COMMITTED' if not native.error_result else failed_sharing_job_status(c,job_id=jid,timeout_seconds=20,parent=native).value
            report['jobs'].append(dict(job_id=jid,state=state,error=native.error_result))
            return native,rows,state
        def relocate(sql):return sql.replace('`'+DATASET_ID+'.','`'+dataset+'.')
        def parameter(name,value):return bigquery.ScalarQueryParameter(name,'STRING',value)
        def rows(name):
            job,values,state=query(f'SELECT TO_JSON_STRING(t) value FROM `{dataset}.{name}` t'+
                (" WHERE recorded_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')" if name==TRANSCRIPT.name else '')+' ORDER BY value')
            require(not job.error_result);return values
        try:
            ds=bigquery.Dataset(dataset);ds.location=LOCATION;ds.default_table_expiration_ms=3600000
            ds.labels={'purpose':'planner_inputs_integration','run_id':run};c.create_dataset(ds,retry=None,timeout=20)
            for table in (SPEC.table(PROJECT),TRANSCRIPT.table(PROJECT),table_metadata(),bigquery.Table(f'{PROJECT}.ci_application.workspaces',schema=[bigquery.SchemaField('revision','INT64')])):
                data=table.to_api_repr();data['tableReference']['datasetId']=dataset.split('.')[1]
                table=bigquery.Table.from_api_repr(data);c.create_table(table,retry=None,timeout=20)
            require(not query(f'INSERT INTO `{dataset}.workspaces` VALUES(1)')[0].error_result)
            authority=dict(transcript=[dict(request_id='request',role='USER',invocation_id=None,content_sha256=hashlib.sha256(b'hello').hexdigest())])
            authority_sha=_hash(authority)
            raw_sql=f'''BEGIN TRANSACTION;
IF TRUE THEN
INSERT INTO `{DATASET_ID}.campaign_request_authorities`
VALUES(@workspace_id,@conversation_id,@request_id,@principal_id,@payload,@digest,CURRENT_TIMESTAMP());
END IF;
COMMIT TRANSACTION;'''
            base=[parameter(k,v) for k,v in dict(workspace_id='workspace',conversation_id='conversation',request_id='request',principal_id='principal',payload=json.dumps(authority),digest=authority_sha).items()]
            raw_sql,raw_params=extend_publication(raw_sql,base,authority,('hello',))
            require(query(relocate(raw_sql),raw_params)[2]=='COMMITTED')
            require(len(rows(TRANSCRIPT.name))==1);report['cases'].append('atomic_transcript_publication')
            def manifest(invocation):
                m=dict(checked_at=datetime.now(timezone.utc).isoformat(),selection_sha256=authority_sha)
                value=dict(schema_version=VERSION,workspace_id='workspace',conversation_id='conversation',request_id='request',principal_id='principal',invocation_id=invocation,binding_sha256='a'*64,manifest_sha256=_hash(m),manifest_canonical_json=json.dumps(m))
                return [parameter(k,v) for k,v in dict(workspace_id='workspace',conversation_id='conversation',request_id='request',principal_id='principal',invocation_id=invocation,runtime_binding=json.dumps(dict(binding_sha256='a'*64)),planner_input_manifest=json.dumps(value)).items()]
            sql=relocate(claim_sql('BEGIN TRANSACTION;\nUPDATE `'+dataset+'.workspaces` SET revision=revision+1 WHERE TRUE;\n-- PROBE_AFTER_FENCE\nCOMMIT TRANSACTION;'))
            sql='BEGIN\n'+sql+'\nEXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; RAISE; END;'
            params=manifest('invocation');job,_,state=query(sql,params,'committed');require(state=='COMMITTED')
            require(len(rows(SPEC.name))==1);report['cases'].append('atomic_manifest_publication')
            original=c.get_job(job.job_id,location=LOCATION,retry=None,timeout=20)
            require(matches_sharing_job(original,job_id=job.job_id,sql=sql,config=query_config(params)) and not original.error_result)
            report['cases'].append('original_job_read_only_recovery')
            before=rows(SPEC.name);_,_,state=query(sql,manifest('invocation'))
            require(state=='NOT_COMMITTED' and before==rows(SPEC.name));report['cases'].append('duplicate_denial_preserves_rows')
            fault=sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'INPUT_PROBE_ROLLBACK';\nCOMMIT TRANSACTION;")
            # Explicit exception rollback is required for positive classification.
            # The original extension script already includes explicit rollback.
            require(query(fault,manifest('rollback'))[2]=='NOT_COMMITTED' and before==rows(SPEC.name))
            report['cases'].append('rollback_after_manifest_insert')
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(query,sql,manifest('race'),f'race_{i}') for i in range(2)]
                outcomes=[f.result(timeout=120) for f in futures]
            require(sorted(o[2] for o in outcomes)==['COMMITTED','NOT_COMMITTED'])
            require(len(rows(SPEC.name))==2);report['cases'].append('concurrent_manifest_one_winner')
            report['status']='PASSED'
        except Exception as e:report['error']=str(e)[:1000]
        finally:
            try:
                metadata=c.get_dataset(dataset,retry=None,timeout=20)
                require(metadata.labels.get('run_id')==run and metadata.labels.get('purpose')=='planner_inputs_integration')
                c.delete_dataset(dataset,delete_contents=True,retry=None,timeout=20)
                try:c.get_dataset(dataset,retry=None,timeout=20);raise ValueError('DATASET_REMAINS')
                except NotFound:report['cleanup']='DELETED_AND_ABSENCE_VERIFIED'
            except Exception as e:report.update(status='FAILED',cleanup=str(e)[:200])
            args.output_dir.mkdir(parents=True,exist_ok=True);path=args.output_dir/(run+'.json')
            path.write_text(json.dumps(report,indent=2,default=str)+'\n');print(json.dumps(dict(status=report['status'],report=str(path),cases=report['cases'],cleanup=report['cleanup'])),flush=True)
    require(report['status']=='PASSED')

if __name__=='__main__':main()
