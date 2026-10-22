"""Verify an attachment-only marketing update; browser IAP proof stays separate."""
import argparse,json,re,subprocess
from copy import deepcopy
from pathlib import Path
from datetime import datetime,timezone
import requests
from src.application.campaign_attachment_service import PROJECT,LOCATION

CONFIG_PATH='/app/config/clarification/attachment_intake_deployment.json'
URL='https://ci-marketing-1042059373096.asia-south1.run.app/'


def normalized_policy(value):
    return sorted((json.dumps(b,sort_keys=True) for b in value.get('bindings',[])))


def checks(before,service,run_iam,iap_iam,image):
    if not re.fullmatch(re.escape('asia-south1-docker.pkg.dev/'+PROJECT+'/ci-operator-images/marketing-pilot@sha256:')+r'[a-f0-9]{64}',image):
        raise ValueError('ATTACHMENT_DEPLOYMENT_IMAGE')
    old=deepcopy(before['service']['spec']['template']['spec'])
    new=deepcopy(service['spec']['template']['spec'])
    if len(old['containers'])!=1 or len(new['containers'])!=1:raise ValueError('ATTACHMENT_DEPLOYMENT_CONTAINERS')
    old_container=old['containers'][0];new_container=new['containers'][0]
    old_env={r['name']:r for r in old_container.get('env',[])}
    new_env={r['name']:r for r in new_container.get('env',[])}
    env_expected=deepcopy(old_env)
    env_expected['CI_CAMPAIGN_ATTACHMENTS_CONFIG']=dict(name='CI_CAMPAIGN_ATTACHMENTS_CONFIG',value=CONFIG_PATH)
    preserved_image=new_container.pop('image')==image;old_container.pop('image')
    old_container.pop('env',None);new_container.pop('env',None)
    invokers={m for b in run_iam.get('bindings',[]) if b['role']=='roles/run.invoker' for m in b['members']}
    return dict(ready=any(c['type']=='Ready' and c['status']=='True' for c in service['status']['conditions']),
        image_pinned=preserved_image,only_attachment_env_changed=new_env==env_expected,
        runtime_secrets_resources_capacity_preserved=old==new,
        iap_enabled=service['metadata']['annotations'].get('run.googleapis.com/iap-enabled')=='true',
        run_policy_preserved=normalized_policy(run_iam)==normalized_policy(before['run_iam']),
        iap_policy_preserved=normalized_policy(iap_iam)==normalized_policy(before['iap_iam']),
        only_iap_invoker=invokers=={'serviceAccount:service-1042059373096@gcp-sa-iap.iam.gserviceaccount.com'},
        release_disabled_in_preserved_config=before['release_enabled'] is False and before['interpreter_enabled'] is False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('before','output'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--image',required=True)
    parser.add_argument('--optional-reference-collection',action='store_true')
    args=parser.parse_args()
    if args.output.exists():raise ValueError('ATTACHMENT_DEPLOYMENT_REPORT_EXISTS')
    def command(*a):
        result=subprocess.run(['gcloud',*a],capture_output=True,text=True,timeout=60)
        if result.returncode:raise ValueError('ATTACHMENT_DEPLOYMENT_READ_FAILED')
        return json.loads(result.stdout)
    base=('--project='+PROJECT,'--region='+LOCATION,'--format=json')
    service=command('run','services','describe','ci-marketing',*base)
    run_iam=command('run','services','get-iam-policy','ci-marketing',*base)
    iap=command('iap','web','get-iam-policy','--service=ci-marketing','--resource-type=cloud-run',*base)
    result=checks(json.loads(args.before.read_text()),service,run_iam,iap,args.image)
    response=requests.get(URL,allow_redirects=False,timeout=20)
    result['anonymous_denied']=response.status_code in (302,303,401,403)
    report=dict(status='PASSED' if all(result.values()) else 'FAILED',checks=result,
        checked_at=datetime.now(timezone.utc).isoformat(),revision=service['status']['latestReadyRevisionName'],
        image=args.image,anonymous_status=response.status_code,browser_iap='PENDING',browser_reconnect='PENDING',
        real_upload='PENDING_BROWSER_TEST' if args.optional_reference_collection else 'BLOCKED_NO_SUCCESSFUL_STORED_INTENT',
        optional_reference_collection=args.optional_reference_collection,release_enabled=False,model_calls=0)
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))


if __name__=='__main__':main()
