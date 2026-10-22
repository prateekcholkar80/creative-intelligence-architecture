"""Fixed registrar evaluation access: one predict permission, explicit revocation."""
import argparse,json
from copy import deepcopy
from pathlib import Path
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery
from src.application.workflow_contract import _require
from src.application.campaign_evaluator_store import PROJECT,LOCATION
from src.ingestion.setup_clarification_runtime_access import bindings

RUNTIME=f'serviceAccount:ci-prompt-registrar@{PROJECT}.iam.gserviceaccount.com'
ROLE_ID='ciEvaluationModelInvoker';ROLE=f'projects/{PROJECT}/roles/{ROLE_ID}'
READER=f'projects/{PROJECT}/roles/ciClarificationPolicyReader'
PRODUCT=f'{PROJECT}.ci_product.product_master'
LEGACY_CONDITION=dict(title='ci-evaluation-pilot-20261018',description='Bounded evaluation only; serving remains disabled.',expression='request.time < timestamp("2026-10-19T00:00:00Z")')


def verified_role(value,name,permissions):
    _require(value.get('name')==name and value.get('stage')=='GA' and not value.get('deleted',False)
        and set(value.get('includedPermissions',[]))==set(permissions),'EVALUATOR_MODEL_ROLE_CONFLICT')


def normalize(policy):
    return sorted(json.dumps(dict(role=b['role'],members=sorted(b['members']),**({'condition':b['condition']} if 'condition' in b else {})),sort_keys=True) for b in policy.get('bindings',[]))


def project_proposal(policy):
    _require(policy.get('etag'),'EVALUATOR_PROJECT_ETAG')
    proposed=deepcopy(policy)
    # Migrate only our exact prior dated member; preserve every other principal.
    retained=[]
    for binding in proposed.get('bindings',[]):
        if binding['role']==ROLE and binding.get('condition')==LEGACY_CONDITION:
            binding['members']=[member for member in binding['members'] if member!=RUNTIME]
            if not binding['members']:continue
        retained.append(binding)
    proposed['bindings']=retained
    matches=[b for b in retained if b['role']==ROLE and 'condition' not in b]
    _require(len(matches)<=1,'EVALUATOR_PROJECT_DUPLICATE')
    if matches:
        if RUNTIME not in matches[0]['members']:matches[0]['members'].append(RUNTIME)
    else:retained.append(dict(role=ROLE,members=[RUNTIME]))
    changed=normalize(proposed)!=normalize(policy)
    if changed:proposed['version']=3
    return proposed,changed


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true');p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    _require(not args.output.exists(),'EVALUATOR_MODEL_REPORT_EXISTS')
    report=dict(status='RUNNING',apply=args.apply,role=ROLE,permissions=['aiplatform.endpoints.predict'],condition=None,revocation='EXPLICIT',
        product_table=PRODUCT,member=RUNTIME,release_enabled=False,model_calls=0)
    try:
        credentials,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        with AuthorizedSession(credentials) as session:
            def post(url,payload):
                response=session.post(url,json=payload,timeout=30);response.raise_for_status();return response.json()
            # Inspect both role definitions before any grant.
            response=session.get('https://iam.googleapis.com/v1/'+ROLE,timeout=20)
            missing=response.status_code==404
            if not missing:response.raise_for_status();verified_role(response.json(),ROLE,['aiplatform.endpoints.predict'])
            reader=session.get('https://iam.googleapis.com/v1/'+READER,timeout=20);reader.raise_for_status()
            verified_role(reader.json(),READER,['bigquery.tables.get','bigquery.tables.getData'])
            if missing and args.apply:
                made=post(f'https://iam.googleapis.com/v1/projects/{PROJECT}/roles',dict(roleId=ROLE_ID,role=dict(title='CI evaluation model invoker',stage='GA',includedPermissions=['aiplatform.endpoints.predict'])))
                verified_role(made,ROLE,['aiplatform.endpoints.predict']);report['role_created']=True
            elif missing:report['role_missing']=True
            endpoint=f'https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT}'
            before=post(endpoint+':getIamPolicy',dict(options=dict(requestedPolicyVersion=3)))
            proposed,changed=project_proposal(before);report['model_grant_missing']=changed
            if changed and args.apply:
                post(endpoint+':setIamPolicy',dict(policy=proposed))
                after=post(endpoint+':getIamPolicy',dict(options=dict(requestedPolicyVersion=3)))
                _require(normalize(after)==normalize(proposed),'EVALUATOR_PROJECT_READBACK')
                report['model_grant']='APPLIED_VERIFIED'
            else:report['model_grant']='MISSING' if changed else 'EXISTING_VERIFIED'
        with bigquery.Client(project=PROJECT,location=LOCATION,credentials=credentials) as client:
            policy=client.get_iam_policy(PRODUCT,retry=None,timeout=20)
            _require(policy.version in (None,0,1),'EVALUATOR_PRODUCT_IAM_VERSION')
            before=bindings(policy);matches=[b for b in policy.bindings if b['role']==READER]
            _require(len(matches)<=1,'EVALUATOR_PRODUCT_DUPLICATE')
            if matches and RUNTIME in matches[0]['members']:report['product_grant']='EXISTING_VERIFIED'
            elif args.apply:
                _require(bool(policy.etag),'EVALUATOR_PRODUCT_ETAG');proposed=deepcopy(policy)
                matching=[b for b in proposed.bindings if b['role']==READER]
                if matching:matching[0]['members'].add(RUNTIME)
                else:proposed.bindings.append(dict(role=READER,members={RUNTIME}))
                client.set_iam_policy(PRODUCT,proposed,retry=None,timeout=20)
                after=client.get_iam_policy(PRODUCT,retry=None,timeout=20)
                _require(bindings(after)==bindings(proposed),'EVALUATOR_PRODUCT_READBACK')
                report.update(product_grant='APPLIED_VERIFIED',product_before=before,product_after=bindings(after))
            else:report['product_grant']='MISSING'
        report['status']='GRANTS_VERIFIED_RUNTIME_PENDING' if report['model_grant']!='MISSING' and report['product_grant']!='MISSING' else 'MISSING_GRANTS'
    except BaseException as error:report.update(status='FAILED',safe_error_type=type(error).__name__,safe_error=str(error)[:800]);raise
    finally:
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
