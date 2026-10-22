"""Impersonated runtime read checks; no IAP/user authentication or model dispatch."""
import argparse,base64,json
from pathlib import Path
import google.auth
from google.auth import impersonated_credentials
from google.cloud import bigquery,secretmanager_v1
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_dataset,validate_table
from src.application.workflow_schema import TABLES
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_transcript import SPEC as BODY
from src.application.campaign_response_transcript import SPEC as RESPONSE
from src.ingestion.setup_marketing_runtime import RUNTIME


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    report=dict(status='FAILED',runtime=RUNTIME,application_rows_written=0,model_calls=0,iap_authentication='NOT_TESTED')
    try:
        source,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        credentials=impersonated_credentials.Credentials(source_credentials=source,target_principal=RUNTIME,
            target_scopes=['https://www.googleapis.com/auth/cloud-platform'],lifetime=300)
        secrets=secretmanager_v1.SecretManagerServiceClient(credentials=credentials)
        try:
            config=secrets.access_secret_version(request={'name':f'projects/{PROJECT}/secrets/ci-marketing-config/versions/1'},retry=None,timeout=20)
            manifest=json.loads(config.payload.data)
            key=secrets.access_secret_version(request={'name':f'projects/{PROJECT}/secrets/ci-marketing-history-key/versions/1'},retry=None,timeout=20)
            if len(base64.b64decode(key.payload.data,validate=True))!=32 or manifest['release_enabled'] is not False or manifest['interpreter_runtime']['enabled'] is not False:
                raise ValueError('MARKETING_SECRET_CONFIGURATION_INVALID')
            report['secret_reads_verified']=True
        finally:secrets.transport.close()
        with bigquery.Client(project=PROJECT,location=LOCATION,credentials=credentials) as client:
            validate_dataset(client.get_dataset(PROJECT+'.ci_application',retry=None,timeout=20))
            for spec in tuple(TABLES.values())+(BODY,RESPONSE):
                expected=spec.table(PROJECT);validate_table(client.get_table(expected.reference,retry=None,timeout=20),expected)
            reader=BigQueryPromptRegistryReader(client);report['profiles']=[]
            for profile in manifest['submission']['model_profiles']:
                current=reader.read_exact(profile['prompt_id'],profile['prompt_version']);snapshot=current.state.head.snapshot
                if snapshot.model_id!=profile['model_id'] or _hash(json.loads(snapshot.generation_config_json))!=profile['settings_sha256']:
                    raise ValueError('MARKETING_PROFILE_CHANGED')
                report['profiles'].append(dict(stage=profile['stage'],registry_status=current.state.head.status.value,
                    snapshot_sha256=snapshot.snapshot_sha256))
            job=client.query(f'SELECT COUNT(*) active_products FROM `{PROJECT}.ci_product.product_master` WHERE product_status="ACTIVE"',
                location=LOCATION,retry=None,job_retry=None,timeout=20,
                job_config=bigquery.QueryJobConfig(maximum_bytes_billed=64*1024**2,use_query_cache=False))
            rows=list(job.result(timeout=30,max_results=1,retry=None,job_retry=None))
            report['catalog_read_verified']=len(rows)==1
        report['status']='PASSED_READ_ONLY_RUNTIME_ACCESS'
    except Exception as error:
        report['error_type']=type(error).__name__
        raise
    finally:
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
