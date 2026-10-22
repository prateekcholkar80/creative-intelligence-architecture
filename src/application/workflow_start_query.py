"""Pure query plan for atomic request/job workflow submission.

The plan consumes validated, freshly read access rows and exact audit envelopes.
It constructs bounded parameterized BigQuery SQL but never submits it. Worker
attempt claiming, stage events, model calls and result publication are separate
4.6 transactions.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime,timedelta
import hashlib
import json
from pathlib import Path

from google.cloud import bigquery

from src.application.access_policy import AccessDecision,Action,Principal,require_authorized
from src.application.audit_contract import (
    MutationKind,MutationOutcome,ResourceType,WorkflowAction,WorkflowObservation,
)
from src.application.audit_producer import AuditEnvelope
from src.application.conversation_contract import validate_conversation
from src.application.control_io import query_config
from src.application.membership_contract import validate_membership
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_codec import decode_row,encode_row,sql_row
from src.application.workflow_contract import (
    ContractError,RETRY_WINDOW,_require,canonical_json_bytes,resolve_retry,validate_job,validate_request,
)
from src.application.workspace_contract import propose_workspace_fence,validate_workspace
from src.generators.sales_generator import PROJECT,ROOT

SQL_TEMPLATE=ROOT/'sql/create_workflow_start.sql'
PLAN_TTL=timedelta(seconds=30)


@dataclass(frozen=True,slots=True)
class WorkflowStartPlan:
    job_id: str
    workspace_id: str
    request_id: str
    workflow_job_id: str
    sql: str
    config_json: str
    request_json: str
    workflow_job_json: str
    access_audit_json: str
    request_audit_json: str
    job_audit_json: str

    def config(self) -> bigquery.QueryJobConfig:
        return bigquery.QueryJobConfig.from_api_repr(json.loads(self.config_json))


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _sql() -> str:
    sql=SQL_TEMPLATE.read_text()
    replacements={
        '__REQUEST_ROW__':sql_row('candidate','requests'),
        '__JOB_ROW__':sql_row('attempt','jobs'),
        '__ACCESS_AUDIT_ROW__':sql_row('audit','audit_events'),
        '__REQUEST_AUDIT_ROW__':sql_row('owner','audit_events'),
        '__JOB_AUDIT_ROW__':sql_row('invocation','audit_events'),
        '__WORKSPACE_ROW__':sql_row('workspace','workspaces'),
    }
    for token,value in replacements.items():
        if sql.count(token)==0:
            raise ContractError('INVALID_WORKFLOW_START_SQL')
        sql=sql.replace(token,value)
    if '__' in sql:
        raise ContractError('INVALID_WORKFLOW_START_SQL')
    return sql


def _audit_scope(envelope: AuditEnvelope, *, request: dict, job: dict, kind: str) -> None:
    row=envelope.row();target=envelope.target;mutation=envelope.mutation
    if (envelope.recorded_at!=request['created_at'] or envelope.trace_id!=request['trace_id']
            or target is None):
        raise ContractError('WORKFLOW_START_AUDIT_MISMATCH')
    if kind=='ACCESS':
        if (not isinstance(envelope.observation,AccessDecision)
                or envelope.observation.action is not Action.GENERATE_DRAFT
                or target.resource_type is not ResourceType.REQUEST
                or target.resource_id!=request['request_id'] or target.request_id!=request['request_id']
                or target.job_id is not None or row['action']!='GENERATE_DRAFT' or mutation is not None):
            raise ContractError('WORKFLOW_START_AUDIT_MISMATCH')
        return
    expected_action=WorkflowAction.CREATE_REQUEST if kind=='REQUEST' else WorkflowAction.CREATE_JOB
    expected_type=ResourceType.REQUEST if kind=='REQUEST' else ResourceType.JOB
    expected_id=request['request_id'] if kind=='REQUEST' else job['job_id']
    if (not isinstance(envelope.observation,WorkflowObservation)
            or envelope.observation.action is not expected_action
            or target.resource_type is not expected_type or target.resource_id!=expected_id
            or target.request_id!=request['request_id']
            or target.job_id!=(None if kind=='REQUEST' else job['job_id'])
            or row['action']!=expected_action.value or mutation is None
            or mutation.kind is not MutationKind.CREATE
            or mutation.outcome is not MutationOutcome.APPLIED or mutation.new_revision!=1):
        raise ContractError('WORKFLOW_START_AUDIT_MISMATCH')


def prepare_workflow_start(*, workspace: dict, membership: dict, conversation: dict,
                           request: dict, job: dict, request_audit: AuditEnvelope,
                           job_audit: AuditEnvelope, access_audit: AuditEnvelope,
                           observed_at: datetime,
                           settings: SharingQuerySettings = SharingQuerySettings()) -> WorkflowStartPlan:
    """Build one deterministic single-submit transaction plan from fresh rows."""
    if not isinstance(settings,SharingQuerySettings):
        raise ContractError('INVALID_WORKFLOW_START_SETTINGS')
    workspace_id=request.get('workspace_id');principal_id=request.get('principal_id')
    validate_workspace(workspace,workspace_id=workspace_id)
    member=validate_membership(membership,workspace_id=workspace_id,principal_id=principal_id)
    resource=validate_conversation(conversation,workspace_id=workspace_id,
        conversation_id=request.get('conversation_id'))
    decision=require_authorized(Principal(principal_id),member,Action.GENERATE_DRAFT,
        workspace_id=workspace_id)
    if (workspace['status']!='ACTIVE' or conversation['status']!='ACTIVE'
            or resource.owner_id!=principal_id or decision!=access_audit.observation):
        raise ContractError('WORKFLOW_START_ACCESS_MISMATCH')
    validate_request(request,conversation=resource);validate_job(job,request=request,conversation=resource)
    if (job['state']!='QUEUED' or job['revision']!=1 or request['revision']!=1
            or observed_at!=request['created_at'] or job['created_at']!=observed_at
            or workspace['updated_at']>observed_at or membership['updated_at']>observed_at
            or conversation['updated_at']>observed_at):
        raise ContractError('INVALID_WORKFLOW_START')
    resolve_retry(request,None,conversation=resource,now=observed_at)
    _audit_scope(access_audit,request=request,job=job,kind='ACCESS')
    _audit_scope(request_audit,request=request,job=job,kind='REQUEST')
    _audit_scope(job_audit,request=request,job=job,kind='JOB')
    if len({access_audit.row()['audit_id'],request_audit.row()['audit_id'],job_audit.row()['audit_id']})!=3:
        raise ContractError('WORKFLOW_START_AUDIT_MISMATCH')
    fence=propose_workspace_fence(workspace,workspace_id=workspace_id,
        expected_revision=workspace['revision'],observed_at=observed_at)
    request_json=encode_row(request,'requests');workflow_job_json=encode_row(job,'jobs')
    access_audit_json=access_audit.audit_json;request_audit_json=request_audit.audit_json
    job_audit_json=job_audit.audit_json
    parameters=[
        bigquery.ScalarQueryParameter('workspace_id','STRING',workspace_id),
        bigquery.ScalarQueryParameter('principal_id','STRING',principal_id),
        bigquery.ScalarQueryParameter('conversation_id','STRING',request['conversation_id']),
        bigquery.ScalarQueryParameter('request_id','STRING',request['request_id']),
        bigquery.ScalarQueryParameter('job_id','STRING',job['job_id']),
        bigquery.ScalarQueryParameter('access_audit_id','STRING',access_audit.row()['audit_id']),
        bigquery.ScalarQueryParameter('request_audit_id','STRING',request_audit.row()['audit_id']),
        bigquery.ScalarQueryParameter('job_audit_id','STRING',job_audit.row()['audit_id']),
        bigquery.ScalarQueryParameter('idempotency_key_sha256','STRING',request['idempotency_key_sha256']),
        bigquery.ScalarQueryParameter('generation_intent_id','STRING',request['generation_intent_id']),
        bigquery.ScalarQueryParameter('workspace_revision','INT64',workspace['revision']),
        bigquery.ScalarQueryParameter('membership_revision','INT64',membership['revision']),
        bigquery.ScalarQueryParameter('conversation_revision','INT64',conversation['access_revision']),
        bigquery.ScalarQueryParameter('workspace_updated_at','TIMESTAMP',workspace['updated_at']),
        bigquery.ScalarQueryParameter('membership_updated_at','TIMESTAMP',membership['updated_at']),
        bigquery.ScalarQueryParameter('conversation_updated_at','TIMESTAMP',conversation['updated_at']),
        bigquery.ScalarQueryParameter('retry_start_at','TIMESTAMP',observed_at-RETRY_WINDOW),
        bigquery.ScalarQueryParameter('plan_observed_at','TIMESTAMP',observed_at),
        bigquery.ScalarQueryParameter('plan_expires_at','TIMESTAMP',observed_at+PLAN_TTL),
        bigquery.ScalarQueryParameter('candidate','STRING',request_json),
        bigquery.ScalarQueryParameter('attempt','STRING',workflow_job_json),
        bigquery.ScalarQueryParameter('audit','STRING',access_audit_json),
        bigquery.ScalarQueryParameter('owner','STRING',request_audit_json),
        bigquery.ScalarQueryParameter('invocation','STRING',job_audit_json),
        bigquery.ScalarQueryParameter('workspace','STRING',encode_row(fence,'workspaces')),
    ]
    config=query_config(parameters,settings)
    native_job_id='workflow_start_v1_'+_digest([PROJECT,workspace_id,request['request_id'],
        job['job_id'],request['request_sha256']])
    return WorkflowStartPlan(native_job_id,workspace_id,request['request_id'],job['job_id'],_sql(),
        json.dumps(config.to_api_repr(),sort_keys=True),request_json,workflow_job_json,
        access_audit_json,request_audit_json,job_audit_json)


def validate_workflow_start_result(plan: WorkflowStartPlan, row: dict) -> str:
    """Validate one native result; UNKNOWN/failed jobs are classified by the executor."""
    _require(isinstance(plan,WorkflowStartPlan) and isinstance(row,dict)
             and set(row)=={'state','request_row','job_row','access_audit','request_audit','job_audit'}
             and row['state'] in {'APPLIED','REUSED'},'INVALID_WORKFLOW_START_RESULT')
    try:
        valid=(decode_row(row['request_row'],'requests')==decode_row(plan.request_json,'requests')
            and decode_row(row['job_row'],'jobs')==decode_row(plan.workflow_job_json,'jobs')
            and decode_row(row['access_audit'],'audit_events')==decode_row(plan.access_audit_json,'audit_events')
            and decode_row(row['request_audit'],'audit_events')==decode_row(plan.request_audit_json,'audit_events')
            and decode_row(row['job_audit'],'audit_events')==decode_row(plan.job_audit_json,'audit_events'))
    except ContractError:
        raise ContractError('INVALID_WORKFLOW_START_RESULT') from None
    _require(valid,'INVALID_WORKFLOW_START_RESULT')
    return row['state']
