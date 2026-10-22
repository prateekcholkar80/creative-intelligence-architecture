"""Exact two-table read grants for the MVP operator; no data writes/deployment."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

from src.validation.clarification_console_preflight import (
    PROJECT, REGION, SERVICE, RUNTIME, gcloud, service_report,
)

ROLE_ID = 'ciClarificationPolicyReader'
ROLE = f'projects/{PROJECT}/roles/{ROLE_ID}'
PERMISSIONS = ('bigquery.tables.get', 'bigquery.tables.getData')
TABLES = tuple(f'{PROJECT}.ci_brand_memory.campaign_clarification_policy_{s}'
               for s in ('versions', 'revocations'))
MEMBER = 'serviceAccount:' + RUNTIME
ROLE_URL = 'https://iam.googleapis.com/v1/' + ROLE


def verify_role(value):
    if (value.get('name') != ROLE or value.get('deleted', False)
            or value.get('stage') != 'GA'
            or sorted(value.get('includedPermissions', [])) != sorted(PERMISSIONS)):
        raise ValueError('CLARIFICATION_RUNTIME_ROLE_CONFLICT')


def bindings(policy):
    return sorted((b['role'], tuple(sorted(b['members']))) for b in policy.bindings)


def table_grant(client, table, *, apply):
    if table not in TABLES:
        raise ValueError('CLARIFICATION_RUNTIME_TABLE_SCOPE')
    policy = client.get_iam_policy(table, retry=None, timeout=20)
    before = bindings(policy)
    if policy.version not in (None, 0, 1):
        raise ValueError('CLARIFICATION_RUNTIME_POLICY_VERSION_UNSUPPORTED')
    if any(role == ROLE and MEMBER in members for role, members in before):
        return dict(table=table, outcome='EXISTING_VERIFIED')
    if not apply:
        return dict(table=table, outcome='MISSING')
    if not policy.etag:
        raise ValueError('CLARIFICATION_RUNTIME_ETAG_REQUIRED')
    proposed = deepcopy(policy)
    matching = [b for b in proposed.bindings if b['role'] == ROLE]
    if len(matching) > 1:
        raise ValueError('CLARIFICATION_RUNTIME_BINDING_CONFLICT')
    if matching:
        matching[0]['members'].add(MEMBER)
    else:
        proposed.bindings.append(dict(role=ROLE, members={MEMBER}))
    # BigQuery setIamPolicy retains the read etag; no blind replacement/retry.
    client.set_iam_policy(table, proposed, retry=None, timeout=20)
    current = client.get_iam_policy(table, retry=None, timeout=20)
    if (bindings(current) != bindings(proposed)
            or current.version not in (None, 0, 1)):
        raise ValueError('CLARIFICATION_RUNTIME_READBACK_CONFLICT')
    return dict(table=table, outcome='APPLIED_VERIFIED',
                prior_binding_count=len(before), current_binding_count=len(current.bindings))


def role_setup(session, *, apply):
    response = session.get(ROLE_URL, timeout=20)
    if response.status_code == 404:
        if not apply:
            return 'MISSING'
        response = session.post(f'https://iam.googleapis.com/v1/projects/{PROJECT}/roles',
            json=dict(roleId=ROLE_ID, role=dict(title='CI clarification policy reader',
                description='MVP operator: metadata and data read on two clarification policy tables only.',
                stage='GA', includedPermissions=list(PERMISSIONS))), timeout=20)
        response.raise_for_status()
        verify_role(response.json())
        response = session.get(ROLE_URL, timeout=20)
        response.raise_for_status()
        verify_role(response.json())
        return 'CREATED_VERIFIED'
    response.raise_for_status()
    verify_role(response.json())
    return 'EXISTING_VERIFIED'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--apply', action='store_true')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    report = dict(status='FAILED', checked_at=datetime.now(timezone.utc).isoformat(),
        apply_requested=args.apply, runtime=RUNTIME, role=ROLE,
        included_permissions=list(PERMISSIONS), tables=[], effective_runtime_access='UNVERIFIED',
        registration_writes=0, deployment=False)
    try:
        before = service_report(json.loads(gcloud('run','services','describe',SERVICE,
            '--project='+PROJECT,'--region='+REGION,'--format=json')))
        if before['status'] != 'PASSED':
            raise ValueError('CLARIFICATION_RUNTIME_SERVICE_UNSAFE')
        report['service_before'] = before
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        from google.cloud import bigquery
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        with AuthorizedSession(credentials) as session:
            report['role_outcome'] = role_setup(session, apply=args.apply)
        with bigquery.Client(project=PROJECT, location=REGION, credentials=credentials) as client:
            for table in TABLES:
                report['tables'].append(table_grant(client, table, apply=args.apply))
        after = service_report(json.loads(gcloud('run','services','describe',SERVICE,
            '--project='+PROJECT,'--region='+REGION,'--format=json')))
        report['service_after'] = after
        if after != before:
            raise ValueError('CLARIFICATION_RUNTIME_SERVICE_CHANGED')
        report['status'] = ('GRANTS_VERIFIED_RUNTIME_PROBE_PENDING'
            if report['role_outcome'] != 'MISSING' and all(t['outcome'] != 'MISSING' for t in report['tables'])
            else 'READ_ONLY_MISSING_GRANTS')
    except Exception as error:
        report['error_type'] = type(error).__name__
        # Retain partial evidence; failed/uncertain operations do not imply rollback.
        raise
    finally:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')


if __name__ == '__main__': main()
