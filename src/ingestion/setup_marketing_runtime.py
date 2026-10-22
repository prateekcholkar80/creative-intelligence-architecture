"""Scoped release-disabled pilot IAM/secrets setup; no application row writes."""
import argparse,base64,json,os,subprocess
from copy import deepcopy
from pathlib import Path
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery,secretmanager_v1
from google.api_core.exceptions import NotFound
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

RUNTIME=f'ci-marketing@{PROJECT}.iam.gserviceaccount.com'
ROLE=f'projects/{PROJECT}/roles/ciMarketingWorkflowWriter'
PERMISSIONS=['bigquery.tables.get','bigquery.tables.getData','bigquery.tables.updateData']
WRITES=('workspaces','memberships','conversations','requests','model_invocations','audit_events',
        'campaign_transcript_bodies','campaign_transcript_responses','campaign_request_authorities')
SECRETS=('ci-marketing-config','ci-marketing-history-key')


def require(ok,code):
    if not ok:raise ValueError(code)


def grant_table(client,table,role):
    allowed={f'{PROJECT}.ci_application.{t}' for t in WRITES}|{f'{PROJECT}.ci_product.product_master'}
    require(table in allowed and role in {ROLE,'roles/bigquery.dataViewer'},'MARKETING_GRANT_SCOPE')
    policy=client.get_iam_policy(table,retry=None,timeout=20);require(policy.etag and policy.version in (0,1,None),'MARKETING_POLICY_ETAG')
    member='serviceAccount:'+RUNTIME
    if any(b['role']==role and member in b['members'] for b in policy.bindings):return 'EXISTING'
    proposed=deepcopy(policy);matches=[b for b in proposed.bindings if b['role']==role]
    require(len(matches)<=1,'MARKETING_POLICY_CONFLICT')
    if matches:matches[0]['members'].add(member)
    else:proposed.bindings.append(dict(role=role,members={member}))
    client.set_iam_policy(table,proposed,retry=None,timeout=20)
    actual=client.get_iam_policy(table,retry=None,timeout=20)
    normalize=lambda p:sorted((b['role'],tuple(sorted(b['members']))) for b in p.bindings)
    require(normalize(actual)==normalize(proposed),'MARKETING_POLICY_READBACK')
    return 'ADDED_VERIFIED'


def setup(manifest_path,output):
    manifest=json.loads(manifest_path.read_text())
    require(manifest['release_enabled'] is False and manifest['interpreter_runtime']['enabled'] is False
        and manifest['workspace_id']=='ci-poc-marketing'
        and manifest['iap_audience']=='/projects/1042059373096/locations/asia-south1/services/ci-marketing','MARKETING_MANIFEST_SCOPE')
    report=dict(status='FAILED',runtime=RUNTIME,application_rows_written=0,model_execution=False,release_enabled=False)
    credentials,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    try:
        with AuthorizedSession(credentials) as session:
            account_url=f'https://iam.googleapis.com/v1/projects/{PROJECT}/serviceAccounts/{RUNTIME}'
            response=session.get(account_url,timeout=20)
            if response.status_code==404:
                response=session.post(f'https://iam.googleapis.com/v1/projects/{PROJECT}/serviceAccounts',
                    json=dict(accountId='ci-marketing',serviceAccount=dict(displayName='CI release-disabled marketing pilot')),timeout=20)
            response.raise_for_status();require(response.json()['email']==RUNTIME and not response.json().get('disabled',False),'MARKETING_RUNTIME_CONFLICT')
            role_url='https://iam.googleapis.com/v1/'+ROLE
            response=session.get(role_url,timeout=20)
            if response.status_code==404:
                response=session.post(f'https://iam.googleapis.com/v1/projects/{PROJECT}/roles',json=dict(roleId='ciMarketingWorkflowWriter',role=dict(title='CI marketing scoped workflow writer',stage='GA',includedPermissions=PERMISSIONS)),timeout=20)
            response.raise_for_status();role=response.json()
            require(role['name']==ROLE and sorted(role['includedPermissions'])==sorted(PERMISSIONS) and role.get('stage')=='GA' and not role.get('deleted',False),'MARKETING_ROLE_CONFLICT')
        result=subprocess.run(['gcloud','projects','add-iam-policy-binding',PROJECT,'--member=serviceAccount:'+RUNTIME,
            '--role=roles/bigquery.jobUser','--condition=None','--quiet','--format=json'],capture_output=True,text=True,timeout=60)
        require(result.returncode==0,'MARKETING_JOB_GRANT_FAILED')
        with bigquery.Client(project=PROJECT,location=LOCATION,credentials=credentials) as client:
            dataset=client.get_dataset(PROJECT+'.ci_application',retry=None,timeout=20)
            existing=bigquery.AccessEntry('READER','userByEmail',RUNTIME)
            if existing not in dataset.access_entries:
                before=list(dataset.access_entries);dataset.access_entries=before+[existing]
                client.update_dataset(dataset,['access_entries'],retry=None,timeout=20)
                actual=client.get_dataset(dataset.reference,retry=None,timeout=20)
                normalize=lambda entries:sorted(json.dumps(e.to_api_repr(),sort_keys=True) for e in entries)
                require(normalize(actual.access_entries)==normalize(before+[existing]),'MARKETING_DATASET_READBACK')
            report['tables']={table:grant_table(client,PROJECT+'.ci_application.'+table,ROLE) for table in WRITES}
            report['product_read']=grant_table(client,PROJECT+'.ci_product.product_master','roles/bigquery.dataViewer')
        secrets=secretmanager_v1.SecretManagerServiceClient(credentials=credentials)
        try:
            report['secrets']={}
            for name in SECRETS:
                resource=f'projects/{PROJECT}/secrets/{name}'
                try:secret=secrets.get_secret(request={'name':resource},retry=None,timeout=20);created=False
                except NotFound:
                    secret=secrets.create_secret(request=dict(parent='projects/'+PROJECT,secret_id=name,
                        secret=dict(labels={'purpose':'ci-marketing'},replication=dict(user_managed=dict(replicas=[dict(location=LOCATION)])))) ,retry=None,timeout=20)
                    created=True
                require(dict(secret.labels).get('purpose')=='ci-marketing' and [r.location for r in secret.replication.user_managed.replicas]==[LOCATION],'MARKETING_SECRET_SCOPE')
                if created:
                    data=json.dumps(manifest,sort_keys=True).encode() if name==SECRETS[0] else base64.b64encode(os.urandom(32))
                    version=secrets.add_secret_version(request=dict(parent=resource,payload=dict(data=data)),retry=None,timeout=20)
                    require(version.name.endswith('/versions/1'),'MARKETING_SECRET_VERSION_CONFLICT')
                else:
                    version=secrets.get_secret_version(request={'name':resource+'/versions/1'},retry=None,timeout=20)
                    require(version.state==secretmanager_v1.SecretVersion.State.ENABLED,'MARKETING_SECRET_VERSION_DISABLED')
                    if name==SECRETS[0]:
                        stored=secrets.access_secret_version(request={'name':version.name},retry=None,timeout=20)
                        require(json.loads(stored.payload.data)==manifest,'MARKETING_MANIFEST_SECRET_CONFLICT')
                policy=secrets.get_iam_policy(request={'resource':resource},retry=None,timeout=20)
                require(policy.etag,'MARKETING_SECRET_ETAG')
                member='serviceAccount:'+RUNTIME
                bindings=[b for b in policy.bindings if b.role=='roles/secretmanager.secretAccessor']
                require(len(bindings)<=1,'MARKETING_SECRET_POLICY_CONFLICT')
                if bindings:
                    if member not in bindings[0].members:bindings[0].members.append(member)
                else:policy.bindings.add(role='roles/secretmanager.secretAccessor',members=[member])
                secrets.set_iam_policy(request=dict(resource=resource,policy=policy),retry=None,timeout=20)
                actual=secrets.get_iam_policy(request={'resource':resource},retry=None,timeout=20)
                require(any(b.role=='roles/secretmanager.secretAccessor' and member in b.members for b in actual.bindings),'MARKETING_SECRET_GRANT_READBACK')
                report['secrets'][name]=dict(version='1',created=created)
        finally:secrets.transport.close()
        report['status']='CONFIGURED_RELEASE_DISABLED'
    finally:
        output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+'\n')
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--manifest',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();print(json.dumps(setup(args.manifest,args.output)))

if __name__=='__main__':main()
