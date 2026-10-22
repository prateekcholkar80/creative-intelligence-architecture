"""Read-only marketing deployment/IAP checks; no authenticated browser claim."""
import argparse,json,subprocess
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlparse
import requests
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

SERVICE='ci-marketing'
URL='https://ci-marketing-1042059373096.asia-south1.run.app/'
RUNTIME=f'ci-marketing@{PROJECT}.iam.gserviceaccount.com'
IMAGE='asia-south1-docker.pkg.dev/'+PROJECT+'/ci-operator-images/marketing-pilot@sha256:95b1926b7bb9607ae8780d1fa7064ec6b2a264810751ab54c346b8b1a8de8ada'


def command(*args):
    result=subprocess.run(['gcloud',*args],capture_output=True,text=True,timeout=60)
    if result.returncode:raise ValueError('MARKETING_READONLY_COMMAND_FAILED')
    return json.loads(result.stdout)


def verify_service(value):
    spec=value['spec']['template']['spec'];containers=spec['containers']
    if len(containers)!=1:raise ValueError('MARKETING_CONTAINER_SCOPE')
    container=containers[0];env={v['name']:v for v in container.get('env',[])}
    refs={name:env.get(name,{}).get('valueFrom',{}).get('secretKeyRef') for name in ('CI_CAMPAIGN_HISTORY_CURSOR_KEY_B64',)}
    return dict(ready=any(c['type']=='Ready' and c['status']=='True' for c in value['status']['conditions']),
        runtime_matches=spec.get('serviceAccountName')==RUNTIME,image_matches=container['image']==IMAGE,
        iap_enabled=value['metadata'].get('annotations',{}).get('run.googleapis.com/iap-enabled')=='true',
        durable_mode=env.get('CI_DURABLE_WORKFLOW_UI_ENABLED',{}).get('value')=='true',
        secret_pinned=refs['CI_CAMPAIGN_HISTORY_CURSOR_KEY_B64']==dict(name='ci-marketing-history-key',key='1'))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    service=command('run','services','describe',SERVICE,'--project='+PROJECT,'--region='+LOCATION,'--format=json')
    iam=command('run','services','get-iam-policy',SERVICE,'--project='+PROJECT,'--region='+LOCATION,'--format=json')
    iap=command('iap','web','get-iam-policy','--service='+SERVICE,'--resource-type=cloud-run','--project='+PROJECT,'--region='+LOCATION,'--format=json')
    checks=verify_service(service)
    expected='serviceAccount:service-1042059373096@gcp-sa-iap.iam.gserviceaccount.com'
    invokers={m for b in iam.get('bindings',[]) if b['role']=='roles/run.invoker' for m in b['members']}
    checks['only_iap_invoker']=invokers=={expected}
    members={m for b in iap.get('bindings',[]) if b['role']=='roles/iap.httpsResourceAccessor' for m in b['members']}
    checks['pilot_iap_binding']=any(m.lower()=='user:prateek.cholkar@gmail.com' for m in members)
    checks['no_public_iap']='allUsers' not in members and 'allAuthenticatedUsers' not in members
    response=requests.get(URL,allow_redirects=False,timeout=20)
    checks['anonymous_denied']=response.status_code in {302,303,401,403}
    report=dict(checked_at=datetime.now(timezone.utc).isoformat(),status='PASSED' if all(checks.values()) else 'FAILED',
        checks=checks,revision=service['status']['latestReadyRevisionName'],image=IMAGE,anonymous_status=response.status_code,
        redirect_host=urlparse(response.headers.get('Location','')).hostname,authenticated_browser='PENDING',
        live_revocation='PENDING',live_worker_reconnect='PENDING',model_calls=0,release_enabled=False)
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
