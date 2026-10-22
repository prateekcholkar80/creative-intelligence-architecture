"""Explicit exact-schema setup and DRAFT-only ingestion. No approval or activation."""
import argparse,json
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import Conflict,NotFound
from src.application.campaign_clarification_policy import (
    PROJECT,LOCATION,DATASET,VERSIONS,REVOCATIONS,table_metadata,publish_draft,read_policy,ClarificationPolicyRequest,
)
from src.application.campaign_evidence_gaps import validate_policy,canonical
from src.application.campaign_acceptance_contract import _hash
from src.application.operation_budget import operation_call
from src.ingestion.setup_workflow_tables import validate_table


def setup(client,*,apply=False):
    if type(apply) is not bool or client.project!=PROJECT or client.location!=LOCATION:
        raise ValueError('Fixed project/region and explicit apply required')
    ds=operation_call(client.get_dataset,DATASET,retry=None,timeout=20)
    if ds.location.lower()!=LOCATION or ds.default_table_expiration_ms is not None or ds.default_partition_expiration_ms is not None:
        raise ValueError('Clarification dataset metadata mismatch')
    missing=[];created=[];counts={}
    for name in (VERSIONS,REVOCATIONS):
        expected=table_metadata(name)
        try:actual=operation_call(client.get_table,expected.reference,retry=None,timeout=20)
        except NotFound:
            missing.append(name)
            if not apply:continue
            try:operation_call(client.create_table,expected,retry=None,timeout=20);created.append(name)
            except Conflict:pass
            actual=operation_call(client.get_table,expected.reference,retry=None,timeout=20)
        validate_table(actual,expected);counts[name]=actual.num_rows
    return dict(status='VERIFIED' if apply or not missing else 'MISSING_RESOURCES',created_tables=created,metadata_row_counts=counts)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true')
    p.add_argument('--draft-file',type=Path);args=p.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        report=setup(client,apply=args.apply)
        if args.draft_file:
            policy_json=canonical(json.loads(args.draft_file.read_text()));policy=validate_policy(policy_json)
            request=ClarificationPolicyRequest(policy['policy_id'],policy['version'],_hash(policy))
            if args.apply:
                outcome=publish_draft(client,policy_json,apply=True)
                report['publication']=dict(native_job_id=outcome.job_id,native_status=outcome.native_status.value)
            snapshot=read_policy(client,request)
            report['policy']=dict(policy_sha256=request.policy_sha256,status=policy['status'],
                native_job_id=snapshot.native_job_id,record_sha256=snapshot.record_sha256,approval_verified=False)
        print(json.dumps(report,sort_keys=True))

if __name__=='__main__':main()
