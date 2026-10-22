"""Explicit synthetic workflow fixtures for isolated 4.4 probes, never serving."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from src.application.access_policy import OwnedResource
from src.application.identity import ISSUER, principal_for_subject
from src.application.management_service import digest
from src.application.workflow_contract import request_fingerprint, retry_key_hash, transition_job, JobState
from src.application.workflow_schema import SCHEMA_VERSION, TABLES

SUBJECT='accounts.google.com:isolated-control-probe'
ACTOR=principal_for_subject(ISSUER,SUBJECT).principal_id


def profile(stage):
    return dict(stage=stage,profile_id='fixture-'+stage.lower(),profile_version='fixture-v1',catalog_version='fixture-v1',
        model_id='gemini-3.5-flash',location='asia-south1',thinking_control='LEVEL',
        thinking_level='LOW' if stage=='PLANNER' else 'MINIMAL',thinking_budget=None,max_output_tokens=1024,
        timeout_ms=120000,seed=42,temperature=None,prompt_id='fixture-'+stage.lower(),prompt_version='fixture-v1',
        output_schema_version='fixture-v1',safety_policy_version='fixture-v1',settings_sha256=digest([stage,'settings']))


def controls(workspace_id,*,second_admin=False):
    at=datetime.now(timezone.utc)-timedelta(seconds=10)
    ws=dict(schema_version=SCHEMA_VERSION,workspace_id=workspace_id,status='ACTIVE',revision=1,created_at=at,updated_at=at)
    def member(subject=SUBJECT):
        return dict(schema_version=SCHEMA_VERSION,workspace_id=workspace_id,
            principal_id=principal_for_subject(ISSUER,subject).principal_id,identity_issuer=ISSUER,identity_subject=subject,
            roles=['ADMIN','CREATOR'],status='ACTIVE',revision=1,assignment_origin='ADMIN',updated_by=ACTOR,created_at=at,updated_at=at)
    conv=dict(schema_version=SCHEMA_VERSION,workspace_id=workspace_id,conversation_id='fixture-conversation',owner_id=ACTOR,
        visibility='PRIVATE',status='ACTIVE',access_revision=1,created_at=at,updated_at=at)
    return dict(workspaces=[ws],memberships=[member()]+([member('accounts.google.com:second-control-probe')] if second_admin else []),conversations=[conv])


def workflow(workspace_id,stage='PLANNER'):
    rows=controls(workspace_id);conv=rows['conversations'][0];at=conv['created_at'];resource=OwnedResource(conv['conversation_id'],workspace_id,ACTOR)
    req=dict.fromkeys(f.name for f in TABLES['requests'].schema)
    prefs=dict(canonical_sku='CI-SKU-0001',objective='CONVERSION',audience='Synthetic POC fixture',channel='PAID_SOCIAL',visual_reference='NONE')
    req.update(schema_version=SCHEMA_VERSION,workspace_id=workspace_id,conversation_id=resource.resource_id,owner_id=ACTOR,
        principal_id=ACTOR,request_id='fixture-request',session_id='fixture-session',trace_id='fixture-trace',
        operation='GENERATE' if stage=='PLANNER' else 'INTERPRET',request_status='READY' if stage=='PLANNER' else 'ACCEPTED',revision=1,
        submitted_preferences=dict(preferences=prefs if stage=='PLANNER' else dict.fromkeys(prefs),
            renderer=dict(version='fixture-v1',settings_sha256=digest(['renderer'])) if stage=='PLANNER' else None),
        unsupported_features=[],message_sha256=digest(['message']) if stage=='INTERPRETER' else None,
        generation_intent_id='fixture-intent' if stage=='PLANNER' else None,
        model_profiles=[profile('INTERPRETER'),profile('PLANNER')] if stage=='PLANNER' else [profile('INTERPRETER')],
        policy_version='marketing-access-v1',created_at=at,updated_at=at,request_sha256='0'*64,
        idempotency_key_sha256=retry_key_hash(workspace_id=workspace_id,principal_id=ACTOR,conversation_id=resource.resource_id,
            operation='GENERATE' if stage=='PLANNER' else 'INTERPRET',retry_key='fixture-key'))
    if stage=='PLANNER':req.update(prefs)
    req['request_sha256']=request_fingerprint(req);rows['requests']=[req]
    inv=dict.fromkeys(f.name for f in TABLES['model_invocations'].schema)
    for key in ('schema_version','workspace_id','conversation_id','owner_id','request_id','trace_id'):inv[key]=req[key]
    inv.update(invocation_id='fixture-invocation',stage=stage,call_number=1,state='PLANNED',revision=1,
        profile=deepcopy(next(p for p in req['model_profiles'] if p['stage']==stage)),usage_status='NOT_REPORTED',cost_status='NOT_ESTIMATED',
        started_at=at+timedelta(seconds=2))
    if stage=='PLANNER':
        job=dict.fromkeys(f.name for f in TABLES['jobs'].schema)
        for key in ('schema_version','workspace_id','conversation_id','owner_id','request_id','principal_id','trace_id','canonical_sku',
                    'idempotency_key_sha256','request_sha256','generation_intent_id','policy_version'):job[key]=req[key]
        job.update(job_id='fixture-job',state='QUEUED',revision=1,attempt_count=0,created_at=at,updated_at=at,deadline_at=at+timedelta(minutes=30))
        job=transition_job(job,request=req,conversation=resource,target=JobState.RUNNING,expected_revision=1,
            attempt_id='fixture-attempt',now=at+timedelta(seconds=1))
        attempt=dict.fromkeys(f.name for f in TABLES['job_attempts'].schema)
        for key in ('schema_version','workspace_id','conversation_id','owner_id','job_id','request_id','trace_id'):attempt[key]=job[key]
        attempt.update(attempt_id='fixture-attempt',attempt_number=1,state='RUNNING',revision=1,worker_id='fixture-worker',
            started_at=at+timedelta(seconds=1),updated_at=at+timedelta(seconds=1))
        rows.update(jobs=[job],job_attempts=[attempt]);inv.update(job_id='fixture-job',attempt_id='fixture-attempt')
    rows['model_invocations']=[inv]
    return rows
