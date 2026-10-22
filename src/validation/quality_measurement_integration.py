"""Owned expiring native measurement fixtures; synthetic, never benchmark quality."""
import argparse
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import hashlib,json,re
from pathlib import Path
from uuid import uuid4
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from src.application.campaign_quality_publication import build_publication,CONTROL,HISTORY,AUDIT
from src.application.campaign_quality_qualification import SCHEMAS,BENCHMARKS,REPORTS,REVOCATIONS,table_metadata
from src.application.campaign_quality_qualification_reader import SQL,decode_capture
from src.application.campaign_review_storage import table_metadata as control_table,PublicationControl,row_json
from src.application.prompt_runtime_contract import TABLES as BINDINGS
from src.application.workflow_schema import TABLES as WORKFLOW
from src.application.workflow_contract import _require
from src.application.control_io import ControlIO,ControlPlan,query_config
from src.application.sharing_jobs import matches_sharing_job
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.campaign_permission_publication import validate_result
from src.validation.quality_measurement_fixtures import fixture
from src.validation.policy_approval_publication_integration import settle
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

PREFIX='ci_measurement_it_'
NAMES=tuple(SCHEMAS)+(CONTROL,HISTORY,AUDIT,'model_invocations','requests','invocation_prompt_bindings')


def relocate(sql,dataset):
    _require(type(dataset) is str and re.fullmatch(PREFIX+r'[0-9a-f]{32}',dataset),'MEASUREMENT_FIXTURE_SCOPE')
    for name in NAMES:
        for source in ('ci_brand_memory','ci_application'):
            sql=sql.replace(f'`{PROJECT}.{source}.{name}`',f'`{PROJECT}.{dataset}.{name}`')
    _require('.ci_brand_memory.' not in sql and '.ci_application.' not in sql,'MEASUREMENT_FIXTURE_RELOCATION')
    return sql


def run(output):
    _require(not output.exists(),'MEASUREMENT_FIXTURE_REPORT_EXISTS')
    dataset=PREFIX+uuid4().hex;created=False
    report=dict(status='RUNNING',dataset=dataset,cases=[],native_jobs=[],cleanup_verified=False,
        authentication='SYNTHETIC_NOT_DEPLOYED_IAP',full_benchmark_execution=0,model_calls=0,production_writes=0)
    def save():output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    def passed(name,**details):report['cases'].append(dict(name=name,**details));save()
    save()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        io=ControlIO(client,result_timeout_seconds=90)
        def query(sql,params=()):
            sql=relocate(sql,dataset);config=query_config(list(params));native_id='measurement_fixture_read_'+uuid4().hex
            job=client.query(sql,job_config=config,job_id=native_id,location=LOCATION,retry=None,job_retry=None,timeout=20)
            job.result(max_results=0,retry=None,job_retry=None,timeout=45)
            native=client.get_job(native_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
            _require(matches_sharing_job(native,job_id=native_id,sql=sql,config=config)
                and native.state=='DONE' and native.error_result is None,'MEASUREMENT_FIXTURE_READ_CORRELATION')
            rows=list(native.result(max_results=2,page_size=2,retry=None,job_retry=None,timeout=20))
            _require(len(rows)==1,'MEASUREMENT_FIXTURE_READ_ROWS')
            report['native_jobs'].append(native_id);report['last_reader_job_id']=native_id;save()
            return dict(rows[0])
        def dml(sql,params=()):
            config=query_config(list(params));native='measurement_fixture_dml_'+uuid4().hex;sql=relocate(sql,dataset)
            job=client.query(sql,job_config=config,job_id=native,location=LOCATION,retry=None,job_retry=None,timeout=20)
            job.result(retry=None,job_retry=None,timeout=45)
            _require(matches_sharing_job(job,job_id=native,sql=sql,config=config) and job.error_result is None,'MEASUREMENT_FIXTURE_DML')
            report['native_jobs'].append(native);save()
        def snapshot():
            values=query('SELECT '+','.join(f'ARRAY(SELECT TO_JSON_STRING(t) FROM `{PROJECT}.{dataset}.{name}` t LIMIT 65) r{i},(SELECT COUNT(*) FROM `{PROJECT}.{dataset}.{name}`) n{i}' for i,name in enumerate(NAMES)))
            result={}
            for i,name in enumerate(NAMES):
                _require(type(values[f'n{i}']) is int and values[f'n{i}']==len(values[f'r{i}'])<65,'MEASUREMENT_FIXTURE_SNAPSHOT')
                result[name]=tuple(sorted(values[f'r{i}']))
            return result
        def native_plan(plan,label):
            return ControlPlan('synthetic-publisher','synthetic-workspace',plan.command_sha256,
                'measurement_fixture_'+hashlib.sha256((dataset+plan.native_job_id+label).encode()).hexdigest(),
                relocate(plan.sql,dataset),plan.config_json,'MEASUREMENT_FIXTURE','{}',plan.audit_json,REPORTS)
        try:
            ds=bigquery.Dataset(f'{PROJECT}.{dataset}');ds.location=LOCATION;ds.default_table_expiration_ms=3600000
            ds.labels={'purpose':'ci-measurement-fixture'};client.create_dataset(ds,retry=None,timeout=20);created=True;report['phase']='CREATING_TABLES';save()
            metadata={name:(table_metadata(name) if name in SCHEMAS else control_table(name,PROJECT) if name in (CONTROL,HISTORY,AUDIT)
                else BINDINGS[name].table(PROJECT) if name in BINDINGS else WORKFLOW[name].table(PROJECT)) for name in NAMES}
            for name,expected in metadata.items():
                table=bigquery.Table(f'{PROJECT}.{dataset}.{name}',schema=expected.schema)
                table.expires=datetime.now(timezone.utc)+timedelta(hours=1);client.create_table(table,retry=None,timeout=20)
            report['phase']='LOADING_SYNTHETIC_FIXTURES';save()
            args,seeds=fixture()
            seeds[BENCHMARKS]=[dict(document_sha256='0'*64,canonical_json='{"unrelated":"preserve"}')]
            for name,rows in seeds.items():
                data=json.loads(json.dumps(rows,default=lambda v:v.isoformat() if isinstance(v,datetime) else str(v)))
                job=client.load_table_from_json(data,f'{PROJECT}.{dataset}.{name}',
                    job_config=bigquery.LoadJobConfig(schema=metadata[name].schema,write_disposition='WRITE_APPEND'),
                    num_retries=0,timeout=20)
                job.result(retry=None,timeout=45)
            report['phase']='CAPTURING_NATIVE_PROVIDER_ROWS';save()
            # Fresh exact native provider serialization is compared again inside the transaction.
            for group,name in (('invocations','model_invocations'),('bindings','invocation_prompt_bindings'),('requests','requests')):
                row=query(f'SELECT ARRAY(SELECT TO_JSON_STRING(t) FROM `{PROJECT}.ci_application.{name}` t) source_rows')
                args['capture'][group]=tuple(row['source_rows'])
            args['checked_at']=datetime.now(timezone.utc)
            plan=build_publication(**args);before=snapshot();report['phase']='VERIFYING_PUBLICATION';save()
            rollback=native_plan(plan,'rollback')
            marker="ASSERT @@row_count=1 AS 'MEASUREMENT_CAS';"
            rollback=replace(rollback,sql=rollback.sql.replace(marker,marker+"\nASSERT FALSE AS 'MEASUREMENT_FIXTURE_ROLLBACK';"))
            outcome,seen=settle(io,rollback,io.submit_once(rollback));report['native_jobs'].append(rollback.job_id)
            _require(outcome.native_status is Status.NOT_COMMITTED and snapshot()==before,'MEASUREMENT_FIXTURE_ROLLBACK_PROOF')
            passed('ROLLBACK_AFTER_CONTROL_UPDATE',observations=seen)
            args['checked_at']=datetime.now(timezone.utc);plan=build_publication(**args)
            native=native_plan(plan,'publish');outcome,seen=settle(io,native,io.submit_once(native));report['native_jobs'].append(native.job_id)
            report['publication_observations']=seen;report['publication_status']=outcome.native_status.value;save()
            _require(outcome.native_status is Status.COMMITTED,'MEASUREMENT_FIXTURE_COMMIT');validate_result(plan,outcome.rows)
            after=snapshot();audit=json.loads(plan.audit_json)
            _require(len(after[BENCHMARKS])==2 and len(after[REPORTS])==1 and len(after[AUDIT])==1
                and len(after[HISTORY])==2 and len(after[CONTROL])==1,'MEASUREMENT_FIXTURE_ATOMIC_COUNTS')
            for name in NAMES:
                if name not in {BENCHMARKS,REPORTS,AUDIT,HISTORY,CONTROL}:
                    _require(after[name]==before[name],'MEASUREMENT_FIXTURE_UNRELATED')
            _require(set(before[BENCHMARKS])<=set(after[BENCHMARKS]),'MEASUREMENT_FIXTURE_UNRELATED_BENCHMARK')
            _require(json.loads(after[REPORTS][0])==dict(document_sha256=args['pins'].evaluation_sha256,canonical_json=args['capture']['report_json']),
                'MEASUREMENT_FIXTURE_EXACT_REPORT')
            _require(json.loads(after[AUDIT][0])['command_sha256']==plan.command_sha256
                and json.loads(after[CONTROL][0])['control_sha256']==plan.candidate_control_sha256,'MEASUREMENT_FIXTURE_EXACT_AUDIT_CONTROL')
            passed('ATOMIC_MEASUREMENT_PUBLICATION',observations=seen)
            recovered,_=settle(io,native,io.recover(native))
            _require(recovered.native_status is Status.COMMITTED and snapshot()==after,'MEASUREMENT_FIXTURE_RECOVERY')
            passed('ORIGINAL_JOB_RECOVERY_NO_MUTATION')
            conflict=io.submit_once(replace(native,sql=native.sql+'\n-- conflicting exact job configuration'))
            _require(conflict.native_status is Status.UNKNOWN and not conflict.submitted and snapshot()==after,'MEASUREMENT_FIXTURE_JOB_CONFLICT')
            passed('EXACT_JOB_CONFIG_CONFLICT_DENIED')
            def read():
                started=datetime.now(timezone.utc)
                values=query(SQL,[bigquery.ScalarQueryParameter(k,'STRING',getattr(args['pins'],k)) for k in ('workspace_id','benchmark_sha256','evaluation_sha256')])
                return decode_capture(values,pins=args['pins'],requirements=args['requirements'],
                    recorder_ids=(args['identity'].principal.principal_id,),publisher_manifest_sha256=args['manifest'].sha256,
                    native_job_id=report['last_reader_job_id'],started=started,ended=datetime.now(timezone.utc))
            # ControlIO checks native SQL/config/job metadata; capture decoder checks publication and outcomes.
            proof=read();_require(proof.pins==args['pins'],'MEASUREMENT_FIXTURE_QUALIFIED_PINS')
            passed('NATIVE_QUALIFYING_READ_EXACT_PUBLICATION_PROVIDER_BINDINGS')
            original_audit=after[AUDIT][0]
            dml(f'DELETE FROM `{PROJECT}.ci_brand_memory.{AUDIT}` WHERE record_id=@sha',
                [bigquery.ScalarQueryParameter('sha','STRING',args['pins'].evaluation_sha256)])
            damaged=snapshot()
            try:read()
            except ValueError as error:_require('MEASUREMENT_PROOF_COVERAGE' in str(error),'MEASUREMENT_FIXTURE_ORPHAN_CODE')
            else:raise ValueError('MEASUREMENT_FIXTURE_ORPHAN_ACCEPTED')
            _require(snapshot()==damaged,'MEASUREMENT_FIXTURE_DENIED_READ_MUTATED')
            passed('UNPUBLISHED_COPIED_REPORT_DENIED')
            record=json.loads(original_audit);record['recorded_at']=datetime.fromisoformat(record['recorded_at'].replace('Z','+00:00'))
            job=client.load_table_from_json(json.loads(json.dumps([record],default=str)),f'{PROJECT}.{dataset}.{AUDIT}',
                job_config=bigquery.LoadJobConfig(schema=metadata[AUDIT].schema),num_retries=0,timeout=20);job.result(retry=None,timeout=45)
            _require(snapshot()==after,'MEASUREMENT_FIXTURE_REPAIR_EXACT')
            dml(f"UPDATE `{PROJECT}.ci_application.model_invocations` SET response_sha256=REPEAT('0',64) WHERE invocation_id=@id",
                [bigquery.ScalarQueryParameter('id','STRING',json.loads(args['capture']['invocations'][0])['invocation_id'])])
            try:read()
            except ValueError as error:_require('PROVIDER_MISMATCH' in str(error),'MEASUREMENT_FIXTURE_PROVIDER_CODE')
            else:raise ValueError('MEASUREMENT_FIXTURE_CHANGED_PROVIDER_ACCEPTED')
            passed('CHANGED_DURABLE_PROVIDER_OUTCOME_DENIED')
            dml(f"UPDATE `{PROJECT}.ci_application.model_invocations` SET response_sha256=REPEAT('f',64) WHERE invocation_id=@id",
                [bigquery.ScalarQueryParameter('id','STRING',json.loads(args['capture']['invocations'][0])['invocation_id'])])
            _require(snapshot()==after,'MEASUREMENT_FIXTURE_PROVIDER_REPAIR_EXACT')
            dml(f"INSERT `{PROJECT}.ci_brand_memory.{REVOCATIONS}` VALUES('synthetic-revocation','REPORT',@sha,CURRENT_TIMESTAMP())",
                [bigquery.ScalarQueryParameter('sha','STRING',args['pins'].evaluation_sha256)])
            try:read()
            except ValueError as error:_require('REVOKED' in str(error),'MEASUREMENT_FIXTURE_REVOCATION_CODE')
            else:raise ValueError('MEASUREMENT_FIXTURE_REVOKED_ACCEPTED')
            _require(io.recover(native).native_status is Status.COMMITTED,'MEASUREMENT_FIXTURE_HISTORICAL_RECOVERY')
            passed('CURRENT_REVOCATION_DENIES_HISTORICAL_COMMIT')
            report['status']='PASSED';report['phase']='VERIFIED'
        except BaseException as error:
            report.update(status='FAILED',error_type=type(error).__name__,error=str(error)[:2000]);save();raise
        finally:
            if created:
                # Unknown metadata must never cause deletion under a live transaction.
                for native_id in report['native_jobs']:
                    if native_id.startswith('measurement_fixture_read_'):continue
                    root=client.get_job(native_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
                    if root.state!='DONE':
                        root.cancel(retry=None,timeout=10)
                        try:root.result(retry=None,job_retry=None,timeout=45)
                        except Exception:pass
                        root=client.get_job(native_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
                        _require(root.state=='DONE','MEASUREMENT_FIXTURE_ROOT_STILL_RUNNING')
                client.delete_dataset(f'{PROJECT}.{dataset}',delete_contents=True,not_found_ok=False,retry=None,timeout=30)
                try:client.get_dataset(f'{PROJECT}.{dataset}',retry=None,timeout=20)
                except NotFound:report['cleanup_verified']=True
            report['finished_at']=datetime.now(timezone.utc).isoformat();save()
    _require(report['cleanup_verified'],'MEASUREMENT_FIXTURE_CLEANUP')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true');parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args();_require(args.run,'MEASUREMENT_FIXTURE_EXPLICIT_RUN')
    report=run(args.output);print(json.dumps({k:report[k] for k in ('status','cleanup_verified')}))


if __name__=='__main__':main()
