"""Exact evaluator tables and registrar table grants; read-only by default."""
import argparse,json
from copy import deepcopy
from pathlib import Path
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery
from google.api_core.exceptions import NotFound,Conflict
from src.application.campaign_evaluator_store import TABLES,table_metadata,PROJECT,LOCATION
from src.ingestion.setup_workflow_tables import validate_table
from src.ingestion.setup_clarification_runtime_access import bindings
from src.application.workflow_contract import _require

ROLE=f'projects/{PROJECT}/roles/ciMarketingWorkflowWriter'
MEMBER=f'serviceAccount:ci-prompt-registrar@{PROJECT}.iam.gserviceaccount.com'
PERMISSIONS={'bigquery.tables.get','bigquery.tables.getData','bigquery.tables.updateData'}


def verify_role(value):
    _require(value.get('name')==ROLE and value.get('stage')=='GA' and not value.get('deleted',False)
        and set(value.get('includedPermissions',[]))==PERMISSIONS,'EVALUATOR_ROLE_CONFLICT')


def setup(client,*,apply=False):
    _require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,'EVALUATOR_SETUP_CLIENT')
    missing=[];result=[]
    for kind,table in TABLES.items():
        try:actual=client.get_table(table,retry=None,timeout=20)
        except NotFound:missing.append(kind)
        else:validate_table(actual,table_metadata(kind))
    for kind,table in TABLES.items():
        created=False
        if kind in missing and apply:
            try:client.create_table(table_metadata(kind),exists_ok=False,retry=None,timeout=20);created=True
            except Conflict:pass
            validate_table(client.get_table(table,retry=None,timeout=20),table_metadata(kind))
        result.append(dict(table=table,status='MISSING' if kind in missing and not apply else 'VERIFIED',created=created))
    return result


def grant(client,table,*,apply=False):
    _require(table in TABLES.values(),'EVALUATOR_GRANT_SCOPE')
    policy=client.get_iam_policy(table,retry=None,timeout=20);before=bindings(policy)
    _require(policy.version in (None,0,1),'EVALUATOR_GRANT_POLICY_VERSION')
    matching=[b for b in policy.bindings if b['role']==ROLE]
    _require(len(matching)<=1,'EVALUATOR_GRANT_DUPLICATE')
    if matching and MEMBER in matching[0]['members']:return dict(table=table,status='EXISTING_VERIFIED',bindings=before)
    if not apply:return dict(table=table,status='MISSING',bindings=before)
    _require(bool(policy.etag),'EVALUATOR_GRANT_ETAG')
    proposed=deepcopy(policy);matching=[b for b in proposed.bindings if b['role']==ROLE]
    if matching:matching[0]['members'].add(MEMBER)
    else:proposed.bindings.append(dict(role=ROLE,members={MEMBER}))
    client.set_iam_policy(table,proposed,retry=None,timeout=20)
    after=client.get_iam_policy(table,retry=None,timeout=20)
    _require(after.version in (None,0,1) and bindings(after)==bindings(proposed),'EVALUATOR_GRANT_READBACK')
    return dict(table=table,status='APPLIED_VERIFIED',before=before,after=bindings(after))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true');p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    _require(not args.output.exists(),'EVALUATOR_SETUP_REPORT_EXISTS')
    report=dict(status='RUNNING',apply=args.apply,tables=[],grants=[],role=ROLE,member=MEMBER,
        model_calls=0,release_enabled=False,effective_runtime_access='UNVERIFIED')
    try:
        credentials,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        with AuthorizedSession(credentials) as session:
            response=session.get('https://iam.googleapis.com/v1/'+ROLE,timeout=20);response.raise_for_status()
            report['role_permissions']=response.json().get('includedPermissions');verify_role(response.json())
        with bigquery.Client(project=PROJECT,location=LOCATION,credentials=credentials) as client:
            report['tables']=setup(client,apply=args.apply)
            for table in report['tables']:
                if table['status']=='VERIFIED':report['grants'].append(grant(client,table['table'],apply=args.apply))
        report['status']='TABLES_GRANTS_VERIFIED_RUNTIME_PENDING' if len(report['grants'])==2 and all(g['status']!='MISSING' for g in report['grants']) else 'MISSING_RESOURCES'
    except BaseException as error:report.update(status='FAILED',safe_error_type=type(error).__name__,safe_error=str(error)[:1000]);raise
    finally:
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
