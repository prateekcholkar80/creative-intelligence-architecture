"""Fixed marketing planner grants; no role creation, jobs.get or data writes."""
import argparse,json
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path

from src.ingestion.setup_evaluator_model_access import verified_role,normalize
from src.ingestion.setup_clarification_runtime_access import bindings
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

MEMBER=f'serviceAccount:ci-marketing@{PROJECT}.iam.gserviceaccount.com'
READER=f'projects/{PROJECT}/roles/ciClarificationPolicyReader'
WRITER=f'projects/{PROJECT}/roles/ciMarketingWorkflowWriter'
MODEL=f'projects/{PROJECT}/roles/ciEvaluationModelInvoker'
READ_TABLES=tuple(f'{PROJECT}.ci_application.{name}' for name in (
    'prompt_registry','prompt_registry_history','jobs','job_attempts','invocation_prompt_bindings'))
WRITE_TABLES=tuple(f'{PROJECT}.ci_application.campaign_evaluator_{kind}' for kind in ('intents','outcomes'))
TABLE_ROLES={**dict.fromkeys(READ_TABLES,READER),**dict.fromkeys(WRITE_TABLES,WRITER)}


def table_proposal(policy,table):
    _require(table in TABLE_ROLES,'MARKETING_PLANNER_TABLE_SCOPE')
    _require(policy.version in (None,0,1) and policy.etag,'MARKETING_PLANNER_TABLE_POLICY')
    role=TABLE_ROLES[table];proposed=deepcopy(policy)
    matches=[b for b in proposed.bindings if b['role']==role]
    _require(len(matches)<=1 and all(not b.get('condition') for b in proposed.bindings),
        'MARKETING_PLANNER_TABLE_CONFLICT')
    if matches:matches[0]['members'].add(MEMBER)
    else:proposed.bindings.append(dict(role=role,members={MEMBER}))
    return proposed,bindings(proposed)!=bindings(policy)


def project_proposal(policy):
    _require(policy.get('etag'),'MARKETING_PLANNER_PROJECT_ETAG')
    proposed=deepcopy(policy)
    matches=[b for b in proposed.get('bindings',[]) if b['role']==MODEL and 'condition' not in b]
    _require(len(matches)<=1,'MARKETING_PLANNER_PROJECT_CONFLICT')
    if matches:
        if MEMBER not in matches[0]['members']:matches[0]['members'].append(MEMBER)
    else:proposed.setdefault('bindings',[]).append(dict(role=MODEL,members=[MEMBER]))
    changed=normalize(proposed)!=normalize(policy)
    if changed:proposed['version']=3
    return proposed,changed


def setup(client,session,*,apply=False,progress=lambda value:None):
    _require(client.project==PROJECT and client.location==LOCATION and type(apply) is bool,
        'MARKETING_PLANNER_CLIENT')
    for role,permissions in ((READER,{'bigquery.tables.get','bigquery.tables.getData'}),
            (WRITER,{'bigquery.tables.get','bigquery.tables.getData','bigquery.tables.updateData'}),
            (MODEL,{'aiplatform.endpoints.predict'})):
        response=session.get('https://iam.googleapis.com/v1/'+role,timeout=20);response.raise_for_status()
        verified_role(response.json(),role,permissions)
    endpoint=f'https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT}'
    response=session.post(endpoint+':getIamPolicy',json={'options':{'requestedPolicyVersion':3}},timeout=20)
    response.raise_for_status();project=response.json();project_next,project_changed=project_proposal(project)
    pending=[]
    # Validate the complete proposal before the first mutation.
    for table in TABLE_ROLES:
        client.get_table(table,retry=None,timeout=20)
        before=client.get_iam_policy(table,retry=None,timeout=20)
        after,changed=table_proposal(before,table);pending.append((table,before,after,changed))
    result=dict(model_grant='MISSING' if project_changed else 'EXISTING_VERIFIED',tables=[
        dict(table=table,role=TABLE_ROLES[table],status='MISSING' if changed else 'EXISTING_VERIFIED',
            before_bindings=bindings(before),expected_bindings=bindings(after))
        for table,before,after,changed in pending])
    progress(result)
    if apply and project_changed:
        result['model_grant']='APPLY_ATTEMPTED_UNCERTAIN';progress(result)
        response=session.post(endpoint+':setIamPolicy',json={'policy':project_next},timeout=20);response.raise_for_status()
        response=session.post(endpoint+':getIamPolicy',json={'options':{'requestedPolicyVersion':3}},timeout=20)
        response.raise_for_status();_require(normalize(response.json())==normalize(project_next),'MARKETING_PLANNER_PROJECT_READBACK')
        result['model_grant']='APPLIED_VERIFIED'
        progress(result)
    for index,(table,before,after,changed) in enumerate(pending):
        state='MISSING' if changed else 'EXISTING_VERIFIED'
        if apply and changed:
            result['tables'][index]['status']='APPLY_ATTEMPTED_UNCERTAIN';progress(result)
            client.set_iam_policy(table,after,retry=None,timeout=20)
            actual=client.get_iam_policy(table,retry=None,timeout=20)
            _require(actual.version in (None,0,1) and bindings(actual)==bindings(after),
                'MARKETING_PLANNER_TABLE_READBACK')
            state='APPLIED_VERIFIED'
        result['tables'][index]['status']=state;progress(result)
    return result


def main():
    import google.auth
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud import bigquery
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true')
    p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    _require(not args.output.exists(),'MARKETING_PLANNER_REPORT_EXISTS')
    report=dict(status='RUNNING',checked_at=datetime.now(timezone.utc).isoformat(),apply=args.apply,
        member=MEMBER,model_permission='aiplatform.endpoints.predict',project_jobs_get_granted=False,
        model_calls=0,data_writes=0,deployment=False,release_allowed=False,effective_access='UNVERIFIED_REQUIRES_IAP')
    try:
        credentials,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        with AuthorizedSession(credentials) as session,bigquery.Client(project=PROJECT,location=LOCATION,credentials=credentials) as client:
            def progress(value):
                report.update(value);args.output.write_text(json.dumps(report,indent=2)+'\n')
            report.update(setup(client,session,apply=args.apply,progress=progress))
        report['status']='MISSING_GRANTS' if report['model_grant']=='MISSING' or any(t['status']=='MISSING' for t in report['tables']) else 'GRANTS_VERIFIED_RUNTIME_PENDING'
    except Exception as error:
        report.update(status='FAILED_OR_PARTIAL_UNCERTAIN',error_type=type(error).__name__,error=str(error)[:500]);raise
    finally:
        args.output.write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(dict(report_path=str(args.output),status=report['status'],model_grant=report.get('model_grant'),tables=[dict(table=t['table'],status=t['status']) for t in report.get('tables',[])],project_jobs_get_granted=False)))


if __name__=='__main__':main()
