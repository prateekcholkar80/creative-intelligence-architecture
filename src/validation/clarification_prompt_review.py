"""Read exact BigQuery policy and export prompt definitions for registry review.

This never creates a Vertex prompt/version, registers a BigQuery prompt, promotes
or dispatches a model. The output is a review artifact, not a serving fallback.
"""
import argparse,json
from dataclasses import asdict
from datetime import datetime,timezone
from pathlib import Path
from google.cloud import bigquery
from src.application.campaign_clarification_policy import ClarificationPolicyRequest,read_policy,PROJECT,LOCATION
from src.application.campaign_clarification_prompts import definitions


def review(policy_read,*,checked_at):
    prompts=definitions(policy_read,checked_at=checked_at)
    return dict(status='DRAFT_DEFINITIONS_NOT_REGISTERED',source_of_truth='BIGQUERY',
        policy_id=policy_read.request.policy_id,policy_version=policy_read.request.version,
        policy_sha256=policy_read.request.policy_sha256,policy_record_sha256=policy_read.record_sha256,
        policy_native_job_id=policy_read.native_job_id,approval_verified=False,
        prompts=[asdict(p)|dict(definition_sha256=p.sha256,task='INTERPRETER') for p in prompts],
        vertex_versions_created=0,bigquery_prompt_registrations=0,model_calls=0,serving_enabled=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy-id',required=True);parser.add_argument('--version',required=True)
    parser.add_argument('--sha256',required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        policy=read_policy(client,ClarificationPolicyRequest(args.policy_id,args.version,args.sha256))
        packet=review(policy,checked_at=datetime.now(timezone.utc))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(packet,indent=2)+'\n')
    print(json.dumps(dict(status=packet['status'],prompts=len(packet['prompts']),output=str(args.output),writes=0)))

if __name__=='__main__':main()
