"""Isolated native runtime-authority reads; synthetic identity, never serving writes."""
import argparse,json,re
from datetime import datetime,timedelta,timezone
from pathlib import Path
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.application.campaign_measurement_runtime import AUTHORITY_SQL,MeasurementRuntimeSettings,verify_authority
from src.application.workflow_schema import TABLES
from src.application.identity import VerifiedIdentity,principal_for_subject,ISSUER
from src.application.control_io import query_config
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require
from src.application.workflow_codec import decode_row
from src.validation.control_fixtures import controls,SUBJECT
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

PREFIX='ci_runtime_authority_it_'
AUDIENCE='/projects/1042059373096/locations/asia-south1/services/ci-prompt-registrar'


def relocate(sql,dataset):
    _require(re.fullmatch(PREFIX+'[0-9a-f]{32}',dataset) is not None,'RUNTIME_AUTHORITY_FIXTURE_SCOPE')
    for name in ('workspaces','memberships'):sql=sql.replace(f'`{PROJECT}.ci_application.{name}`',f'`{PROJECT}.{dataset}.{name}`')
    _require('.ci_application.' not in sql,'RUNTIME_AUTHORITY_FIXTURE_RELOCATION')
    return sql


def run(output):
    _require(not output.exists(),'RUNTIME_AUTHORITY_REPORT_EXISTS')
    dataset=PREFIX+uuid4().hex;created=False
    report=dict(status='RUNNING',dataset=dataset,cases=[],native_jobs=[],cleanup_verified=False,production_writes=0,identity='SYNTHETIC_NOT_DEPLOYED_IAP')
    def save():output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    save()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        def query(sql,params=()):
            config=query_config(list(params));native='runtime_authority_fixture_'+uuid4().hex;sql=relocate(sql,dataset)
            job=client.query(sql,job_id=native,job_config=config,location=LOCATION,retry=None,job_retry=None,timeout=20)
            rows=list(job.result(max_results=2,retry=None,job_retry=None,timeout=45))
            _require(matches_sharing_job(job,job_id=native,sql=sql,config=config) and job.state=='DONE' and job.error_result is None,'RUNTIME_AUTHORITY_NATIVE_CORRELATION')
            report['native_jobs'].append(native);save();return rows
        def load(name,rows):
            data=json.loads(json.dumps(rows,default=lambda v:v.isoformat()))
            job=client.load_table_from_json(data,f'{PROJECT}.{dataset}.{name}',job_config=bigquery.LoadJobConfig(schema=TABLES[name].schema),num_retries=0,timeout=20)
            job.result(retry=None,timeout=45)
        try:
            ds=bigquery.Dataset(f'{PROJECT}.{dataset}');ds.location=LOCATION;ds.default_table_expiration_ms=3600000
            client.create_dataset(ds,retry=None,timeout=20);created=True
            for name in ('workspaces','memberships'):
                table=bigquery.Table(f'{PROJECT}.{dataset}.{name}',schema=TABLES[name].schema);table.expires=datetime.now(timezone.utc)+timedelta(hours=1)
                client.create_table(table,retry=None,timeout=20)
            seed=controls('synthetic-runtime-workspace')
            for name in ('workspaces','memberships'):load(name,seed[name])
            now=datetime.now(timezone.utc);actor=VerifiedIdentity(principal_for_subject(ISSUER,SUBJECT),ISSUER,SUBJECT,AUDIENCE,int(now.timestamp())-1,int(now.timestamp())+599)
            settings=MeasurementRuntimeSettings('synthetic-runtime-workspace',actor.principal.principal_id,'a'*64,'b'*64)
            params=[bigquery.ScalarQueryParameter('workspace_id','STRING',settings.workspace_id),bigquery.ScalarQueryParameter('principal_id','STRING',settings.publisher_id)]
            def read():
                started=datetime.now(timezone.utc);rows=query(AUTHORITY_SQL,params);_require(len(rows)==1,'RUNTIME_AUTHORITY_FIXTURE_ROWS')
                row=dict(rows[0]);result=verify_authority(row,settings=settings,identity=actor,started=started,ended=datetime.now(timezone.utc))
                return result,row
            result,original=read();_require(result==(1,1),'RUNTIME_AUTHORITY_FIXTURE_POSITIVE')
            report['cases'].append('CURRENT_EXACT_MEMBERSHIP_ALLOWED');save()
            query(f"UPDATE `{PROJECT}.ci_application.memberships` SET status='SUSPENDED',revision=2 WHERE principal_id=@principal_id",params)
            try:read()
            except ValueError as error:_require('AUTHORITY_REVOKED' in str(error),'RUNTIME_AUTHORITY_REVOKED_CODE')
            else:raise ValueError('RUNTIME_AUTHORITY_REVOCATION_ACCEPTED')
            report['cases'].append('CURRENT_MEMBERSHIP_REVOCATION_DENIED');save()
            query(f"UPDATE `{PROJECT}.ci_application.memberships` SET status='ACTIVE',revision=1 WHERE principal_id=@principal_id",params)
            result,restored=read();_require(result==(1,1) and decode_row(restored['memberships'][0],'memberships')==seed['memberships'][0],
                'RUNTIME_AUTHORITY_RESTORATION_EXACT')
            report['cases'].append('EXACT_RESTORATION_ALLOWED');save()
            query(f'INSERT `{PROJECT}.ci_application.memberships` SELECT * FROM `{PROJECT}.ci_application.memberships`')
            try:read()
            except ValueError as error:_require('AUTHORITY_MISSING' in str(error),'RUNTIME_AUTHORITY_DUPLICATE_CODE')
            else:raise ValueError('RUNTIME_AUTHORITY_DUPLICATE_ACCEPTED')
            report['cases'].append('DUPLICATE_CURRENT_MEMBERSHIP_DENIED');save()
            query(f'DELETE FROM `{PROJECT}.ci_application.memberships` WHERE TRUE')
            try:read()
            except ValueError as error:_require('AUTHORITY_MISSING' in str(error),'RUNTIME_AUTHORITY_MISSING_CODE')
            else:raise ValueError('RUNTIME_AUTHORITY_MISSING_ACCEPTED')
            report['cases'].append('MISSING_CURRENT_MEMBERSHIP_DENIED');report['status']='PASSED'
        except BaseException as error:
            report.update(status='FAILED',error_type=type(error).__name__,error=str(error)[:2000]);save();raise
        finally:
            if created:
                client.delete_dataset(f'{PROJECT}.{dataset}',delete_contents=True,retry=None,timeout=30)
                try:client.get_dataset(f'{PROJECT}.{dataset}',retry=None,timeout=20)
                except NotFound:report['cleanup_verified']=True
            report['finished_at']=datetime.now(timezone.utc).isoformat();save()
    _require(report['cleanup_verified'],'RUNTIME_AUTHORITY_CLEANUP')
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',action='store_true');p.add_argument('--output',required=True,type=Path)
    args=p.parse_args();_require(args.run,'RUNTIME_AUTHORITY_EXPLICIT_RUN');r=run(args.output)
    print(json.dumps(dict(status=r['status'],cases=len(r['cases']),cleanup_verified=r['cleanup_verified'])))


if __name__=='__main__':main()
