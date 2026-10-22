"""Two fixed document tables; existing runtime role only, no role broadening."""
import argparse,json
from copy import deepcopy
from pathlib import Path
from google.cloud import bigquery
from google.auth.transport.requests import AuthorizedSession
import google.auth
from src.application.campaign_document_store import TABLES,PROJECT,LOCATION
from src.ingestion.setup_campaign_document_processing import setup
from src.ingestion.setup_clarification_runtime_access import bindings

ROLE=f'projects/{PROJECT}/roles/ciMarketingWorkflowWriter'
MEMBER=f'serviceAccount:ci-marketing@{PROJECT}.iam.gserviceaccount.com'


def verify_role(value):
    if (value.get('name')!=ROLE or value.get('deleted',False) or value.get('stage')!='GA'
        or set(value.get('includedPermissions',[]))!={'bigquery.tables.get','bigquery.tables.getData','bigquery.tables.updateData'}):
        raise ValueError('DOCUMENT_WRITER_ROLE_CONFLICT')


def grant(client,table,*,apply=False):
    if table not in TABLES.values():raise ValueError('DOCUMENT_ACCESS_SCOPE')
    policy=client.get_iam_policy(table,retry=None,timeout=20)
    if policy.version not in (None,0,1):raise ValueError('DOCUMENT_ACCESS_POLICY_VERSION')
    before=bindings(policy)
    if any(role==ROLE and MEMBER in members for role,members in before):return dict(table=table,status='EXISTING_VERIFIED',bindings=before)
    if not apply:return dict(table=table,status='MISSING',bindings=before)
    if not policy.etag:raise ValueError('DOCUMENT_ACCESS_ETAG')
    proposed=deepcopy(policy);matches=[b for b in proposed.bindings if b['role']==ROLE]
    if len(matches)>1:raise ValueError('DOCUMENT_ACCESS_DUPLICATE_BINDING')
    if matches:matches[0]['members'].add(MEMBER)
    else:proposed.bindings.append(dict(role=ROLE,members={MEMBER}))
    client.set_iam_policy(table,proposed,retry=None,timeout=20)
    current=client.get_iam_policy(table,retry=None,timeout=20)
    if bindings(current)!=bindings(proposed) or current.version not in (None,0,1):raise ValueError('DOCUMENT_ACCESS_READBACK')
    return dict(table=table,status='APPLIED_VERIFIED',before=before,after=bindings(current))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true');p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    if args.output.exists():raise ValueError('DOCUMENT_ACCESS_REPORT_EXISTS')
    report=dict(status='RUNNING',apply=args.apply,tables=[],grants=[],role=ROLE,member=MEMBER,effective_runtime_access='UNVERIFIED',model_calls=0,release_enabled=False)
    try:
        credentials,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        with AuthorizedSession(credentials) as session:
            response=session.get('https://iam.googleapis.com/v1/'+ROLE,timeout=20);response.raise_for_status()
            report['role_permissions']=response.json().get('includedPermissions')
            verify_role(response.json())
        with bigquery.Client(project=PROJECT,location=LOCATION,credentials=credentials) as client:
            report['tables']=setup(client,apply=False)
            if any(t['status']!='VERIFIED' for t in report['tables']):raise ValueError('DOCUMENT_TABLES_REQUIRED')
            for table in TABLES.values():report['grants'].append(grant(client,table,apply=args.apply))
        report['status']='GRANTS_VERIFIED_RUNTIME_PROBE_PENDING' if all(g['status']!='MISSING' for g in report['grants']) else 'MISSING_GRANTS'
    except BaseException as error:report.update(status='FAILED',safe_error_type=type(error).__name__,safe_error=str(error)[:1000]);raise
    finally:
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__':main()
