"""Read-only fixed registrar installation checks; browser revocation is separate."""
import argparse,json,subprocess
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlparse
import requests
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

SERVICE='ci-prompt-registrar'
IMAGE='asia-south1-docker.pkg.dev/'+PROJECT+'/ci-operator-images/prompt-registration@sha256:12f81c8df8bfa204cd2b0412df06f2f2ba6680186e8ee5b7dceca4069da8a5f6'
PILOT='iap-v1:50614b325c35a28aca7e7b737af6bd17e3a838ec94059092a1f38b7fadbe9697'
URL='https://ci-prompt-registrar-1042059373096.asia-south1.run.app/'


def command(*args):
    r=subprocess.run(['gcloud',*args],capture_output=True,text=True,timeout=60)
    if r.returncode:raise ValueError('MEASUREMENT_DEPLOYMENT_READ_FAILED')
    return json.loads(r.stdout)


def verify(service,iam,iap):
    spec=service['spec']['template']['spec'];container=spec['containers'][0]
    env={v['name']:v.get('value') for v in container.get('env',[])}
    invokers={m for b in iam.get('bindings',[]) if b['role']=='roles/run.invoker' for m in b['members']}
    members={m for b in iap.get('bindings',[]) if b['role']=='roles/iap.httpsResourceAccessor' for m in b['members']}
    return dict(ready=any(c['type']=='Ready' and c['status']=='True' for c in service['status']['conditions']),
        image_pinned=container['image']==IMAGE,
        service_account=spec['serviceAccountName']=='ci-prompt-registrar@'+PROJECT+'.iam.gserviceaccount.com',
        iap_enabled=service['metadata'].get('annotations',{}).get('run.googleapis.com/iap-enabled')=='true',
        iap_only_invoker=invokers=={'serviceAccount:service-1042059373096@gcp-sa-iap.iam.gserviceaccount.com'},
        pilot_iap=any(m.lower()=='user:prateek.cholkar@gmail.com' for m in members),
        no_public_iap=not members&{'allUsers','allAuthenticatedUsers'},
        explicit_measurement_publisher=env.get('CI_MEASUREMENT_PUBLISHER_PRINCIPAL_ID')==PILOT,
        workspace=env.get('CI_MEASUREMENT_WORKSPACE_ID')=='ci-poc-marketing',
        measurement_installed=env.get('CI_MEASUREMENT_RUNTIME_ENABLED')=='true',
        reviewed_baseline=env.get('CI_MEASUREMENT_REVIEW_SHA256')=='dc80755c1873d0e586431fe5bc551e03fdb84b6b46a335b8c296099096738862',
        registration_writes_disabled=env.get('CI_PROMPT_REGISTRATION_WRITES_ENABLED')=='false',
        no_write_or_bootstrap_enablement=not any(v=='true' for k,v in env.items() if 'WRITES' in k or 'BOOTSTRAP_ENABLED' in k))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);args=p.parse_args()
    if args.output.exists():raise ValueError('MEASUREMENT_DEPLOYMENT_REPORT_EXISTS')
    base=('--project='+PROJECT,'--region='+LOCATION,'--format=json')
    service=command('run','services','describe',SERVICE,*base)
    iam=command('run','services','get-iam-policy',SERVICE,*base)
    iap=command('iap','web','get-iam-policy','--service='+SERVICE,'--resource-type=cloud-run',*base)
    checks=verify(service,iam,iap);response=requests.get(URL,allow_redirects=False,timeout=20)
    checks['anonymous_denied']=response.status_code in {302,303,401,403}
    result=dict(status='PASSED' if all(checks.values()) else 'FAILED',checks=checks,checked_at=datetime.now(timezone.utc).isoformat(),
        revision=service['status']['latestReadyRevisionName'],image=IMAGE,anonymous_status=response.status_code,
        redirect_host=urlparse(response.headers.get('Location','')).hostname,browser_iap='PENDING',browser_reconnect='PENDING',
        deployed_revocation='PENDING',model_calls=0,release_enabled=False)
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))


if __name__=='__main__':main()
