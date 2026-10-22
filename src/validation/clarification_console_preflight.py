"""Read-only package/service/IAM inspection; never implies effective runtime access."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import shutil
from tempfile import TemporaryDirectory

PROJECT = 'project-661d501a-e709-4276-a26'
REGION = 'asia-south1'
SERVICE = 'ci-prompt-registrar'
RUNTIME = f'{SERVICE}@{PROJECT}.iam.gserviceaccount.com'
REQUIRED = (
    'src/ui/prompt_registration_console.py',
    'src/ui/clarification_registration_console.py',
    'src/application/clarification_prompt_registration.py',
    'src/application/campaign_clarification_prompts.py',
    'src/application/campaign_clarification_policy.py',
    'config/campaign_brief_generator.json',
    'deploy/prompt-registration/app.py',
    'deploy/prompt-registration/Dockerfile',
    'deploy/prompt-registration/cloudbuild.yaml',
)


def package_report(root, names):
    names = sorted(set(names))
    denied = [p for p in names if not (
        (p.startswith('src/') and '/__pycache__/' not in p and not p.endswith('.pyc'))
        or (p.startswith('config/') and p.count('/') == 1 and p.endswith('.json'))
        or (p.startswith('sql/') and p.count('/') == 1 and p.endswith('.sql'))
        or p in {'deploy/prompt-registration/'+n for n in (
            'app.py','Dockerfile','cloudbuild.yaml','.gcloudignore')})]
    missing = sorted(set(REQUIRED)-set(names))
    if denied or missing:
        raise ValueError('CONSOLE_PACKAGE_SCOPE_INVALID')
    hashes = {}
    for name in names:
        path = root / name
        if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('CONSOLE_PACKAGE_SCOPE_INVALID')
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(status='PASSED',file_count=len(names),file_sha256=hashes,
        source_sha256=hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest())


def service_report(value):
    spec = value['spec']['template']['spec']
    containers = spec['containers']
    if len(containers) != 1:
        raise ValueError('CONSOLE_SERVICE_SCOPE_INVALID')
    env = {v['name']:v.get('value') for v in containers[0].get('env',[])}
    annotations = value['metadata'].get('annotations',{})
    checks = dict(iap_enabled=annotations.get('run.googleapis.com/iap-enabled')=='true',
        writes_disabled=env.get('CI_PROMPT_REGISTRATION_WRITES_ENABLED')=='false',
        bootstrap_disabled=env.get('CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED')=='false',
        runtime_matches=spec.get('serviceAccountName')==RUNTIME,
        audience_matches=env.get('CI_IAP_EXPECTED_AUDIENCE')==
            '/projects/1042059373096/locations/asia-south1/services/'+SERVICE,
        origin_matches=env.get('CI_PROMPT_REGISTRATION_ORIGIN')==
            'https://ci-prompt-registrar-1042059373096.asia-south1.run.app',
        workspace_matches=env.get('CI_PROMPT_REGISTRATION_WORKSPACE_ID')=='ci-poc-marketing',
        capacity_matches=spec.get('containerConcurrency')==16 and
            annotations.get('run.googleapis.com/maxScale')=='1' and
            value['spec']['template']['metadata']['annotations'].get('autoscaling.knative.dev/maxScale')=='1')
    return dict(status='PASSED' if all(checks.values()) else 'FAILED',checks=checks,
        revision=value['status'].get('latestReadyRevisionName'),image=containers[0]['image'],
        clarification_selector_enabled=env.get('CI_CLARIFICATION_REGISTRATION_ENABLED')=='true')


def gcloud(*args):
    result = subprocess.run(['gcloud',*args],capture_output=True,text=True,timeout=60)
    if result.returncode:
        # Never forward SDK/provider bodies or credentials to an evidence file.
        raise RuntimeError('CONSOLE_PREFLIGHT_COMMAND_FAILED')
    return result.stdout


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    root=Path.cwd()
    report=dict(checked_at=datetime.now(timezone.utc).isoformat(),status='FAILED',
        effective_runtime_access='UNVERIFIED',registration_writes=0,iam_changes=0,deployment=False)
    try:
        # meta list-files-for-upload has no --ignore-file flag. Use an isolated
        # local mirror with the exact build ignore file; never modify root rules.
        with TemporaryDirectory(prefix='ci-console-preflight-') as directory:
            stage=Path(directory)
            candidates=list((root/'src').rglob('*'))
            candidates+=list((root/'config').glob('*.json'))+list((root/'sql').glob('*.sql'))
            candidates+=[root/'deploy/prompt-registration'/name for name in
                ('app.py','Dockerfile','cloudbuild.yaml','.gcloudignore')]
            for source in candidates:
                if source.is_symlink():
                    raise ValueError('CONSOLE_PACKAGE_SCOPE_INVALID')
                if not source.is_file() or '__pycache__' in source.parts or source.suffix=='.pyc':
                    continue
                target=stage/source.relative_to(root)
                target.parent.mkdir(parents=True,exist_ok=True)
                shutil.copy2(source,target)
            shutil.copy2(root/'deploy/prompt-registration/.gcloudignore',stage/'.gcloudignore')
            names=gcloud('meta','list-files-for-upload',str(stage)).splitlines()
        report['package']=package_report(root,names)
        report['service']=service_report(json.loads(gcloud('run','services','describe',SERVICE,
            '--project='+PROJECT,'--region='+REGION,'--format=json')))
        policy=json.loads(gcloud('projects','get-iam-policy',PROJECT,'--format=json'))
        member='serviceAccount:'+RUNTIME
        report['runtime_project_bindings']=[b for b in policy.get('bindings',[]) if member in b.get('members',[])]
        report['service_iam']=json.loads(gcloud('run','services','get-iam-policy',SERVICE,
            '--project='+PROJECT,'--region='+REGION,'--format=json'))
        from google.cloud import bigquery
        with bigquery.Client(project=PROJECT,location=REGION) as client:
            report['datasets']={}
            for dataset in ('ci_application','ci_brand_memory'):
                value=client.get_dataset(PROJECT+'.'+dataset,retry=None,timeout=20)
                report['datasets'][dataset]=dict(location=value.location,
                    direct_runtime_access=[dict(role=e.role,entity_type=e.entity_type,entity_id=e.entity_id)
                        for e in value.access_entries if e.entity_id==RUNTIME])
            report['policy_table_iam']={}
            for table in ('campaign_clarification_policy_versions','campaign_clarification_policy_revocations'):
                name=PROJECT+'.ci_brand_memory.'+table
                value=client.get_table(name,retry=None,timeout=20)
                iam=client.get_iam_policy(name,retry=None,timeout=20)
                report['policy_table_iam'][table]=dict(num_rows=value.num_rows,
                    direct_runtime_bindings=[dict(role=b['role'],members=sorted(b['members']))
                        for b in iam.bindings if member in b['members']])
        report['status']='PASSED_INSPECTION_RUNTIME_PROBE_PENDING' if report['service']['status']=='PASSED' else 'FAILED'
    except Exception as error:
        report['error_type']=type(error).__name__
        raise
    finally:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')


if __name__=='__main__':main()
