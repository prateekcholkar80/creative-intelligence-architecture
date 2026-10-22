"""Read-only exact evaluation deployment comparison; no browser or model claim."""
import argparse,json
from copy import deepcopy
from pathlib import Path
import re
import requests
from src.validation.measurement_runtime_deployment import command,PROJECT,LOCATION,URL

CONFIG='/app/config/evaluation/interpretation_pilot_deployment.json'


def normalized(policy):return sorted(json.dumps(b,sort_keys=True) for b in policy.get('bindings',[]))


def checks(before,service,run_iam,iap_iam,image,evaluator_sha256):
    _image='asia-south1-docker.pkg.dev/'+PROJECT+'/ci-operator-images/prompt-registration@sha256:'
    if not re.fullmatch(re.escape(_image)+r'[a-f0-9]{64}',image):raise ValueError('EVALUATION_DEPLOYMENT_IMAGE')
    old=deepcopy(before['service']['spec']['template']['spec']);new=deepcopy(service['spec']['template']['spec'])
    oldc=old['containers'][0];newc=new['containers'][0]
    oldenv={e['name']:e for e in oldc.pop('env',[])};newenv={e['name']:e for e in newc.pop('env',[])}
    expected=deepcopy(oldenv)
    expected['CI_INTERPRETATION_EVALUATION_CONFIG']=dict(name='CI_INTERPRETATION_EVALUATION_CONFIG',value=CONFIG)
    expected['CI_MEASUREMENT_EVALUATOR_SHA256']=dict(name='CI_MEASUREMENT_EVALUATOR_SHA256',value=evaluator_sha256)
    oldc.pop('image');newimage=newc.pop('image')
    invokers={m for b in run_iam.get('bindings',[]) if b['role']=='roles/run.invoker' for m in b['members']}
    def maxscale(v):return (v['metadata'].get('annotations',{}).get('run.googleapis.com/maxScale'),v['spec']['template']['metadata'].get('annotations',{}).get('autoscaling.knative.dev/maxScale'))
    return dict(ready=any(c['type']=='Ready' and c['status']=='True' for c in service['status']['conditions']),
        image_pinned=newimage==image,only_two_expected_environment_changes=newenv==expected,
        runtime_resources_secrets_concurrency_preserved=new==old,maximum_instances_preserved=maxscale(service)==maxscale(before['service']),
        iap_enabled=service['metadata']['annotations'].get('run.googleapis.com/iap-enabled')=='true',
        run_iam_preserved=normalized(run_iam)==normalized(before['run_iam']),iap_iam_preserved=normalized(iap_iam)==normalized(before['iap_iam']),
        iap_only_invoker=invokers=={'serviceAccount:service-1042059373096@gcp-sa-iap.iam.gserviceaccount.com'},
        registration_writes_disabled=newenv.get('CI_PROMPT_REGISTRATION_WRITES_ENABLED',{}).get('value')=='false',
        bootstrap_disabled=newenv.get('CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED',{}).get('value')=='false')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('before','output'):p.add_argument('--'+key,type=Path,required=True)
    p.add_argument('--image',required=True);p.add_argument('--evaluator-sha256',required=True);args=p.parse_args()
    if args.output.exists():raise ValueError('EVALUATION_DEPLOYMENT_REPORT_EXISTS')
    base=('--project='+PROJECT,'--region='+LOCATION,'--format=json')
    service=command('run','services','describe','ci-prompt-registrar',*base)
    run_iam=command('run','services','get-iam-policy','ci-prompt-registrar',*base)
    iap=command('iap','web','get-iam-policy','--service=ci-prompt-registrar','--resource-type=cloud-run',*base)
    result=checks(json.loads(args.before.read_text()),service,run_iam,iap,args.image,args.evaluator_sha256)
    response=requests.get(URL,allow_redirects=False,timeout=20);result['anonymous_denied']=response.status_code in (302,303,401,403)
    report=dict(status='PASSED' if all(result.values()) else 'FAILED',checks=result,revision=service['status']['latestReadyRevisionName'],image=args.image,
        anonymous_status=response.status_code,browser_iap='PENDING_CONTROL_DENIED',real_stage_and_judge='NOT_RUN',model_calls=0,release_allowed=False)
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))


if __name__=='__main__':main()
