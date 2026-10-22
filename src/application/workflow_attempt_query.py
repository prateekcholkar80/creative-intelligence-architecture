"""Pure query plan for a fenced worker attempt and first stage event.

This module builds a bounded BigQuery transaction but never submits it. The
claim is separate from user submission and from billable model dispatch.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime,timedelta
import hashlib
import json

from google.cloud import bigquery

from src.application.access_policy import Action,Principal,require_authorized
from src.application.audit_contract import (
    AuditTarget,MutationKind,MutationObservation,MutationOutcome,ResourceType,
    WorkflowAction,WorkflowObservation,
)
from src.application.audit_producer import produce_service_audit
from src.application.conversation_contract import validate_conversation
from src.application.control_io import query_config
from src.application.membership_contract import validate_membership
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_codec import decode_row,encode_row
from src.application.workflow_contract import (
    ContractError,JobState,_require,canonical_json_bytes,transition_job,
    validate_attempt,validate_job,validate_request,validate_stage_event,
)
from src.application.workflow_schema import TABLES
from src.application.workspace_contract import propose_workspace_fence,validate_workspace
from src.generators.sales_generator import PROJECT,ROOT

SQL_TEMPLATE=ROOT/'sql/claim_workflow_attempt.sql'
PLAN_TTL=timedelta(seconds=30)


@dataclass(frozen=True,slots=True)
class WorkflowAttemptPlan:
    native_job_id: str
    workspace_id: str
    workflow_job_id: str
    attempt_id: str
    event_id: str
    sql: str
    config_json: str
    job_json: str
    attempt_json: str
    event_json: str
    job_audit_json: str
    attempt_audit_json: str
    event_audit_json: str

    def config(self) -> bigquery.QueryJobConfig:
        return bigquery.QueryJobConfig.from_api_repr(json.loads(self.config_json))


_PARAMETERS={
    'request_pin':'requests','job_pin':'jobs','job_candidate':'jobs',
    'attempt_candidate':'job_attempts','event_candidate':'stage_events',
    'job_audit':'audit_events','attempt_audit':'audit_events','event_audit':'audit_events',
    'workspace_candidate':'workspaces',
}


def _row_expr(parameter: str, table: str) -> str:
    if _PARAMETERS.get(parameter)!=table:
        raise ContractError('INVALID_WORKFLOW_CLAIM_SQL')
    def fields_expr(fields,source):
        parts=[]
        for field in fields:
            path="'$."+field.name+"'";value=f'JSON_VALUE({source}, {path})'
            if field.field_type=='RECORD':
                if field.mode=='REPEATED':
                    value=f'ARRAY(SELECT AS STRUCT {fields_expr(field.fields,"item")} FROM UNNEST(JSON_QUERY_ARRAY({source}, {path})) item)'
                else:
                    value=f'STRUCT({fields_expr(field.fields,f"JSON_QUERY({source}, {path})")})'
            elif field.mode=='REPEATED':value=f'JSON_VALUE_ARRAY({source}, {path})'
            elif field.field_type=='JSON':value=f'JSON_QUERY({source}, {path})'
            elif field.field_type!='STRING':
                kind={'INTEGER':'INT64','BOOLEAN':'BOOL','TIMESTAMP':'TIMESTAMP','NUMERIC':'NUMERIC','FLOAT':'FLOAT64'}[field.field_type]
                value=f'CAST({value} AS {kind})'
            parts.append(f'{value} AS {field.name}')
        return ', '.join(parts)
    return f'STRUCT({fields_expr(TABLES[table].schema,f"PARSE_JSON(@{parameter})")})'


def _sql() -> str:
    sql=SQL_TEMPLATE.read_text()
    for token,(parameter,table) in {
        '__REQUEST_PIN__':('request_pin','requests'),'__JOB_PIN__':('job_pin','jobs'),
        '__JOB_CANDIDATE__':('job_candidate','jobs'),
        '__ATTEMPT_CANDIDATE__':('attempt_candidate','job_attempts'),
        '__EVENT_CANDIDATE__':('event_candidate','stage_events'),
        '__JOB_AUDIT__':('job_audit','audit_events'),
        '__ATTEMPT_AUDIT__':('attempt_audit','audit_events'),
        '__EVENT_AUDIT__':('event_audit','audit_events'),
        '__WORKSPACE_CANDIDATE__':('workspace_candidate','workspaces'),
    }.items():
        if token not in sql:raise ContractError('INVALID_WORKFLOW_CLAIM_SQL')
        sql=sql.replace(token,_row_expr(parameter,table))
    if '__' in sql:raise ContractError('INVALID_WORKFLOW_CLAIM_SQL')
    return sql


def _audit(worker_id: str,workspace_id: str,action: WorkflowAction,target: AuditTarget,
           mutation: MutationObservation,trace_id: str,at: datetime):
    return produce_service_audit(WorkflowObservation(worker_id,workspace_id,action),target=target,
        mutation=mutation,trace_id=trace_id,observed_at=at,recorded_at=at)


def prepare_workflow_attempt(*, workspace: dict, membership: dict, conversation: dict,
                             request: dict, job: dict, attempt_id: str, event_id: str,
                             worker_id: str, observed_at: datetime,
                             settings: SharingQuerySettings = SharingQuerySettings()) -> WorkflowAttemptPlan:
    if not isinstance(settings,SharingQuerySettings):raise ContractError('INVALID_WORKFLOW_CLAIM_SETTINGS')
    workspace_id=request.get('workspace_id');principal_id=request.get('principal_id')
    validate_workspace(workspace,workspace_id=workspace_id)
    member=validate_membership(membership,workspace_id=workspace_id,principal_id=principal_id)
    resource=validate_conversation(conversation,workspace_id=workspace_id,
        conversation_id=request.get('conversation_id'))
    require_authorized(Principal(principal_id),member,Action.GENERATE_DRAFT,workspace_id=workspace_id)
    validate_request(request,conversation=resource);validate_job(job,request=request,conversation=resource)
    if (workspace['status']!='ACTIVE' or conversation['status']!='ACTIVE' or resource.owner_id!=principal_id
            or job['state']!='QUEUED' or job['revision']!=1 or workspace['updated_at']>observed_at
            or membership['updated_at']>observed_at or conversation['updated_at']>observed_at):
        raise ContractError('INVALID_WORKFLOW_CLAIM')
    candidate=transition_job(job,request=request,conversation=resource,target=JobState.RUNNING,
        expected_revision=1,now=observed_at,attempt_id=attempt_id)
    attempt=dict.fromkeys(field.name for field in TABLES['job_attempts'].schema)
    for key in ('schema_version','workspace_id','conversation_id','owner_id','job_id','request_id','trace_id'):
        attempt[key]=candidate[key]
    attempt.update(attempt_id=attempt_id,attempt_number=1,state='RUNNING',revision=1,
        dispatch_receipt_id=None,worker_id=worker_id,safe_error_code=None,
        started_at=observed_at,updated_at=observed_at,completed_at=None)
    validate_attempt(attempt,request=request,job=candidate,conversation=resource)
    event={key:attempt[key] for key in ('schema_version','workspace_id','conversation_id','owner_id',
        'job_id','request_id','attempt_id','trace_id')}
    event.update(event_id=event_id,sequence=1,stage='RETRIEVING_EVIDENCE',outcome='STARTED',
        duration_ms=None,safe_error_code=None,policy_version=candidate['policy_version'],occurred_at=observed_at)
    validate_stage_event(event,attempt=attempt,request=request,job=candidate,conversation=resource)
    job_target=AuditTarget(ResourceType.JOB,candidate['job_id'],workspace_id,job['revision'],
        candidate['conversation_id'],candidate['owner_id'],candidate['request_id'],candidate['job_id'])
    attempt_target=AuditTarget(ResourceType.JOB_ATTEMPT,attempt_id,workspace_id,
        conversation_id=candidate['conversation_id'],owner_id=candidate['owner_id'],
        request_id=candidate['request_id'],job_id=candidate['job_id'])
    event_target=AuditTarget(ResourceType.STAGE_EVENT,event_id,workspace_id,
        conversation_id=candidate['conversation_id'],owner_id=candidate['owner_id'],
        request_id=candidate['request_id'],job_id=candidate['job_id'])
    job_audit=_audit(worker_id,workspace_id,WorkflowAction.UPDATE_JOB,job_target,
        MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,candidate['revision']),request['trace_id'],observed_at)
    attempt_audit=_audit(worker_id,workspace_id,WorkflowAction.CLAIM_ATTEMPT,attempt_target,
        MutationObservation(MutationKind.CREATE,MutationOutcome.APPLIED,1),request['trace_id'],observed_at)
    event_audit=_audit(worker_id,workspace_id,WorkflowAction.APPEND_STAGE_EVENT,event_target,
        MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED),request['trace_id'],observed_at)
    fence=propose_workspace_fence(workspace,workspace_id=workspace_id,
        expected_revision=workspace['revision'],observed_at=observed_at)
    encoded={'request_pin':encode_row(request,'requests'),'job_pin':encode_row(job,'jobs'),
        'job_candidate':encode_row(candidate,'jobs'),'attempt_candidate':encode_row(attempt,'job_attempts'),
        'event_candidate':encode_row(event,'stage_events'),'job_audit':job_audit.audit_json,
        'attempt_audit':attempt_audit.audit_json,'event_audit':event_audit.audit_json,
        'workspace_candidate':encode_row(fence,'workspaces')}
    parameters=[bigquery.ScalarQueryParameter(name,'STRING',value) for name,value in encoded.items()]
    parameters.extend([
        bigquery.ScalarQueryParameter('workspace_id','STRING',workspace_id),
        bigquery.ScalarQueryParameter('principal_id','STRING',principal_id),
        bigquery.ScalarQueryParameter('conversation_id','STRING',request['conversation_id']),
        bigquery.ScalarQueryParameter('request_id','STRING',request['request_id']),
        bigquery.ScalarQueryParameter('job_id','STRING',job['job_id']),
        bigquery.ScalarQueryParameter('attempt_id','STRING',attempt_id),
        bigquery.ScalarQueryParameter('event_id','STRING',event_id),
        bigquery.ScalarQueryParameter('job_audit_id','STRING',job_audit.row()['audit_id']),
        bigquery.ScalarQueryParameter('attempt_audit_id','STRING',attempt_audit.row()['audit_id']),
        bigquery.ScalarQueryParameter('event_audit_id','STRING',event_audit.row()['audit_id']),
        bigquery.ScalarQueryParameter('workspace_revision','INT64',workspace['revision']),
        bigquery.ScalarQueryParameter('workspace_candidate_revision','INT64',fence['revision']),
        bigquery.ScalarQueryParameter('membership_revision','INT64',membership['revision']),
        bigquery.ScalarQueryParameter('conversation_revision','INT64',conversation['access_revision']),
        bigquery.ScalarQueryParameter('job_revision','INT64',job['revision']),
        bigquery.ScalarQueryParameter('workspace_updated_at','TIMESTAMP',workspace['updated_at']),
        bigquery.ScalarQueryParameter('membership_updated_at','TIMESTAMP',membership['updated_at']),
        bigquery.ScalarQueryParameter('conversation_updated_at','TIMESTAMP',conversation['updated_at']),
        bigquery.ScalarQueryParameter('claim_observed_at','TIMESTAMP',observed_at),
        bigquery.ScalarQueryParameter('plan_expires_at','TIMESTAMP',observed_at+PLAN_TTL),
    ])
    config=query_config(parameters,settings)
    digest=hashlib.sha256(canonical_json_bytes([PROJECT,workspace_id,job['job_id'],attempt_id,event_id])).hexdigest()
    return WorkflowAttemptPlan('workflow_attempt_v1_'+digest,workspace_id,job['job_id'],attempt_id,event_id,
        _sql(),json.dumps(config.to_api_repr(),sort_keys=True),encoded['job_candidate'],
        encoded['attempt_candidate'],encoded['event_candidate'],encoded['job_audit'],
        encoded['attempt_audit'],encoded['event_audit'])


def validate_workflow_attempt_result(plan: WorkflowAttemptPlan,row: dict) -> str:
    expected={'state','job_row','attempt_row','event_row','job_audit','attempt_audit','event_audit'}
    _require(isinstance(plan,WorkflowAttemptPlan) and isinstance(row,dict) and set(row)==expected
             and row['state'] in {'APPLIED','REUSED'},'INVALID_WORKFLOW_CLAIM_RESULT')
    try:
        valid=(decode_row(row['job_row'],'jobs')==decode_row(plan.job_json,'jobs')
            and decode_row(row['attempt_row'],'job_attempts')==decode_row(plan.attempt_json,'job_attempts')
            and decode_row(row['event_row'],'stage_events')==decode_row(plan.event_json,'stage_events')
            and decode_row(row['job_audit'],'audit_events')==decode_row(plan.job_audit_json,'audit_events')
            and decode_row(row['attempt_audit'],'audit_events')==decode_row(plan.attempt_audit_json,'audit_events')
            and decode_row(row['event_audit'],'audit_events')==decode_row(plan.event_audit_json,'audit_events'))
    except ContractError:raise ContractError('INVALID_WORKFLOW_CLAIM_RESULT') from None
    _require(valid,'INVALID_WORKFLOW_CLAIM_RESULT')
    return row['state']
