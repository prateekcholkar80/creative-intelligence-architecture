"""Pure guarded finalization plan for a failed or blocked workflow attempt."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime,timedelta
import hashlib
import json

from google.cloud import bigquery

from src.application.access_policy import Action,Principal,require_authorized
from src.application.audit_contract import (
    AuditTarget,MutationKind,MutationObservation,MutationOutcome,ResourceType,
    WorkflowAction,WorkflowObservation,validate_audit_event,
)
from src.application.audit_producer import produce_service_audit
from src.application.conversation_contract import validate_conversation
from src.application.control_io import query_config
from src.application.membership_contract import validate_membership
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_codec import decode_row,encode_row
from src.application.workflow_contract import (
    ContractError,JobState,_require,_timestamp,canonical_json_bytes,transition_job,
    validate_attempt,validate_model_invocation,validate_stage_events,
)
from src.application.workflow_schema import TABLES
from src.application.workspace_contract import propose_workspace_fence,validate_workspace
from src.generators.sales_generator import PROJECT,ROOT

SQL_TEMPLATE=ROOT/'sql/finalize_workflow_failure.sql'
PLAN_TTL=timedelta(seconds=30)


@dataclass(frozen=True,slots=True)
class WorkflowFailurePlan:
    native_job_id: str
    workspace_id: str
    workflow_job_id: str
    attempt_id: str
    invocation_id: str
    terminal_state: str
    safe_error_code: str
    job_json: str
    attempt_json: str
    event_rows_json: tuple[str,...]
    audit_rows_json: tuple[str,...]
    sql: str
    config_json: str

    def config(self) -> bigquery.QueryJobConfig:
        return bigquery.QueryJobConfig.from_api_repr(json.loads(self.config_json))


def _fields_expr(fields,source: str) -> str:
    parts=[]
    for field in fields:
        path="'$."+field.name+"'";value=f'JSON_VALUE({source}, {path})'
        if field.field_type=='RECORD':
            if field.mode=='REPEATED':
                value=f'ARRAY(SELECT AS STRUCT {_fields_expr(field.fields,"item")} FROM UNNEST(JSON_QUERY_ARRAY({source}, {path})) item)'
            else:value=f'STRUCT({_fields_expr(field.fields,f"JSON_QUERY({source}, {path})")})'
        elif field.mode=='REPEATED':value=f'JSON_VALUE_ARRAY({source}, {path})'
        elif field.field_type=='JSON':value=f'JSON_QUERY({source}, {path})'
        elif field.field_type!='STRING':
            kind={'INTEGER':'INT64','BOOLEAN':'BOOL','TIMESTAMP':'TIMESTAMP','NUMERIC':'NUMERIC','FLOAT':'FLOAT64'}[field.field_type]
            value=f'CAST({value} AS {kind})'
        parts.append(f'{value} AS {field.name}')
    return ', '.join(parts)


def _row_expr(parameter: str,table: str) -> str:
    allowed={'request_pin':'requests','job_pin':'jobs','job_candidate':'jobs',
        'attempt_pin':'job_attempts','attempt_candidate':'job_attempts',
        'invocation_pin':'model_invocations','existing_event':'stage_events',
        'workspace_candidate':'workspaces'}
    if allowed.get(parameter)!=table:raise ContractError('INVALID_WORKFLOW_FAILURE_SQL')
    return f'STRUCT({_fields_expr(TABLES[table].schema,f"PARSE_JSON(@{parameter})")})'


def _array_expr(parameter: str,table: str) -> str:
    return f'ARRAY(SELECT AS STRUCT {_fields_expr(TABLES[table].schema,"PARSE_JSON(item)")} FROM UNNEST(@{parameter}) item)'


def _sql() -> str:
    replacements={'__REQUEST_PIN__':_row_expr('request_pin','requests'),
        '__JOB_PIN__':_row_expr('job_pin','jobs'),'__JOB_CANDIDATE__':_row_expr('job_candidate','jobs'),
        '__ATTEMPT_PIN__':_row_expr('attempt_pin','job_attempts'),
        '__ATTEMPT_CANDIDATE__':_row_expr('attempt_candidate','job_attempts'),
        '__INVOCATION_PIN__':_row_expr('invocation_pin','model_invocations'),
        '__EXISTING_EVENT__':_row_expr('existing_event','stage_events'),
        '__NEW_EVENTS__':_array_expr('new_events','stage_events'),
        '__AUDIT_ROWS__':_array_expr('audit_rows','audit_events'),
        '__WORKSPACE_CANDIDATE__':_row_expr('workspace_candidate','workspaces')}
    sql=SQL_TEMPLATE.read_text()
    for token,value in replacements.items():
        if token not in sql:raise ContractError('INVALID_WORKFLOW_FAILURE_SQL')
        sql=sql.replace(token,value)
    if '__' in sql:raise ContractError('INVALID_WORKFLOW_FAILURE_SQL')
    return sql


def _audit(*,worker_id: str,workspace_id: str,action: WorkflowAction,target: AuditTarget,
           mutation: MutationObservation,trace_id: str,at: datetime,audit_id: str) -> str:
    observation=WorkflowObservation(worker_id,workspace_id,action)
    envelope=produce_service_audit(observation,target=target,mutation=mutation,
        trace_id=trace_id,observed_at=at,recorded_at=at)
    row=envelope.row();row['audit_id']=audit_id
    validate_audit_event(row,observation=observation,target=target,mutation=mutation,
        trace_id=trace_id,observed_at=at,recorded_at=at)
    return encode_row(row,'audit_events')


def prepare_workflow_failure(*,workspace: dict,membership: dict,conversation: dict,
                             request: dict,job: dict,attempt: dict,invocation: dict,
                             stage_events: tuple[dict,...],finalized_at: datetime,
                             settings: SharingQuerySettings=SharingQuerySettings()) -> WorkflowFailurePlan:
    """Build an exact terminal plan; it does not infer failure from a timeout."""
    if not isinstance(settings,SharingQuerySettings):raise ContractError('INVALID_WORKFLOW_FAILURE')
    _timestamp(finalized_at);workspace_id=request.get('workspace_id');principal_id=request.get('principal_id')
    validate_workspace(workspace,workspace_id=workspace_id)
    member=validate_membership(membership,workspace_id=workspace_id,principal_id=principal_id)
    resource=validate_conversation(conversation,workspace_id=workspace_id,
        conversation_id=request.get('conversation_id'))
    require_authorized(Principal(principal_id),member,Action.GENERATE_DRAFT,workspace_id=workspace_id)
    validate_attempt(attempt,request=request,job=job,conversation=resource)
    validate_model_invocation(invocation,request=request,conversation=resource,job=job,attempt=attempt)
    terminal=invocation.get('state');safe_error=invocation.get('safe_error_code')
    if (workspace['status']!='ACTIVE' or conversation['status']!='ACTIVE' or resource.owner_id!=principal_id
            or job['state']!='RUNNING' or attempt['state']!='RUNNING'
            or job['active_attempt_id']!=attempt['attempt_id'] or terminal not in {'FAILED','BLOCKED'}
            or not isinstance(safe_error,str) or invocation.get('stage')!='PLANNER'
            or invocation.get('attempt_id')!=attempt['attempt_id']
            or invocation.get('worker_id')!=attempt['worker_id']
            or invocation.get('dispatch_receipt_id')!=attempt['dispatch_receipt_id']
            or invocation.get('completed_at')>finalized_at
            or max(workspace['updated_at'],membership['updated_at'],conversation['updated_at'],
                job['updated_at'],attempt['updated_at'])>finalized_at):
        raise ContractError('INVALID_WORKFLOW_FAILURE')
    target=JobState(terminal)
    job_candidate=transition_job(job,request=request,conversation=resource,target=target,
        expected_revision=job['revision'],now=finalized_at,attempt_id=attempt['attempt_id'],
        safe_error_code=safe_error)
    attempt_candidate=deepcopy(attempt);attempt_candidate.update(state=terminal,revision=attempt['revision']+1,
        safe_error_code=safe_error,updated_at=finalized_at,completed_at=finalized_at)
    validate_attempt(attempt_candidate,request=request,job=job_candidate,conversation=resource)
    if (not isinstance(stage_events,tuple) or not 2<=len(stage_events)<=8
            or stage_events[0].get('sequence')!=1 or stage_events[-1].get('occurred_at')!=finalized_at
            or stage_events[-1].get('outcome')!=terminal
            or stage_events[-1].get('safe_error_code')!=safe_error):
        raise ContractError('INVALID_WORKFLOW_FAILURE')
    events=[deepcopy(row) for row in stage_events]
    validate_stage_events(events,attempt=attempt_candidate,request=request,job=job_candidate,conversation=resource)
    worker_id=attempt['worker_id'];trace_id=request['trace_id'];invocation_id=invocation['invocation_id']
    targets=[
        (WorkflowAction.UPDATE_JOB,AuditTarget(ResourceType.JOB,job['job_id'],workspace_id,job['revision'],
            request['conversation_id'],request['owner_id'],request['request_id'],job['job_id']),
            MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,job_candidate['revision']),
            f'workflow-failure-v1:JOB:{invocation_id}:{terminal}'),
        (WorkflowAction.UPDATE_ATTEMPT,AuditTarget(ResourceType.JOB_ATTEMPT,attempt['attempt_id'],workspace_id,
            attempt['revision'],request['conversation_id'],request['owner_id'],request['request_id'],job['job_id']),
            MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,attempt_candidate['revision']),
            f'workflow-failure-v1:ATTEMPT:{invocation_id}:{terminal}')]
    for event in events[1:]:
        targets.append((WorkflowAction.APPEND_STAGE_EVENT,AuditTarget(ResourceType.STAGE_EVENT,event['event_id'],workspace_id,
            conversation_id=request['conversation_id'],owner_id=request['owner_id'],request_id=request['request_id'],
            job_id=job['job_id']),MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED),
            f'workflow-failure-v1:EVENT:{event["event_id"]}'))
    audits=tuple(_audit(worker_id=worker_id,workspace_id=workspace_id,action=action,target=audit_target,
        mutation=mutation,trace_id=trace_id,at=finalized_at,audit_id=audit_id)
        for action,audit_target,mutation,audit_id in targets)
    fence=propose_workspace_fence(workspace,workspace_id=workspace_id,
        expected_revision=workspace['revision'],observed_at=finalized_at)
    event_json=tuple(encode_row(row,'stage_events') for row in events[1:])
    scalar={'request_pin':encode_row(request,'requests'),'job_pin':encode_row(job,'jobs'),
        'job_candidate':encode_row(job_candidate,'jobs'),'attempt_pin':encode_row(attempt,'job_attempts'),
        'attempt_candidate':encode_row(attempt_candidate,'job_attempts'),
        'invocation_pin':encode_row(invocation,'model_invocations'),
        'existing_event':encode_row(events[0],'stage_events'),'workspace_candidate':encode_row(fence,'workspaces')}
    parameters=[bigquery.ScalarQueryParameter(name,'STRING',value) for name,value in scalar.items()]
    parameters.extend([bigquery.ArrayQueryParameter('new_events','STRING',list(event_json)),
        bigquery.ArrayQueryParameter('audit_rows','STRING',list(audits)),
        bigquery.ArrayQueryParameter('new_event_ids','STRING',[row['event_id'] for row in events[1:]]),
        bigquery.ArrayQueryParameter('audit_ids','STRING',[decode_row(row,'audit_events')['audit_id'] for row in audits])])
    parameters.extend(bigquery.ScalarQueryParameter(name,kind,value) for name,kind,value in (
        ('workspace_id','STRING',workspace_id),('principal_id','STRING',principal_id),
        ('conversation_id','STRING',request['conversation_id']),('request_id','STRING',request['request_id']),
        ('job_id','STRING',job['job_id']),('attempt_id','STRING',attempt['attempt_id']),
        ('invocation_id','STRING',invocation_id),('worker_id','STRING',worker_id),
        ('dispatch_receipt_id','STRING',invocation['dispatch_receipt_id']),
        ('terminal_state','STRING',terminal),('safe_error_code','STRING',safe_error),
        ('existing_event_id','STRING',events[0]['event_id']),('event_row_count','INT64',len(events)),
        ('new_event_row_count','INT64',len(event_json)),('audit_row_count','INT64',len(audits)),
        ('workspace_revision','INT64',workspace['revision']),('workspace_candidate_revision','INT64',fence['revision']),
        ('membership_revision','INT64',membership['revision']),('conversation_revision','INT64',conversation['access_revision']),
        ('job_revision','INT64',job['revision']),('attempt_revision','INT64',attempt['revision']),
        ('workspace_updated_at','TIMESTAMP',workspace['updated_at']),
        ('membership_updated_at','TIMESTAMP',membership['updated_at']),
        ('conversation_updated_at','TIMESTAMP',conversation['updated_at']),
        ('finalized_at','TIMESTAMP',finalized_at),('plan_expires_at','TIMESTAMP',finalized_at+PLAN_TTL)))
    config=query_config(parameters,settings);job_json=scalar['job_candidate'];attempt_json=scalar['attempt_candidate']
    digest=hashlib.sha256(canonical_json_bytes([PROJECT,workspace_id,job['job_id'],attempt['attempt_id'],
        invocation_id,terminal,safe_error,job_json,attempt_json,list(event_json),list(audits)])).hexdigest()
    return WorkflowFailurePlan('workflow_failure_v1_'+digest,workspace_id,job['job_id'],attempt['attempt_id'],
        invocation_id,terminal,safe_error,job_json,attempt_json,event_json,audits,_sql(),
        json.dumps(config.to_api_repr(),sort_keys=True))


def validate_workflow_failure_result(plan: WorkflowFailurePlan,row: dict) -> str:
    expected={'state','job_row','attempt_row','event_rows','audit_rows'}
    _require(isinstance(plan,WorkflowFailurePlan) and isinstance(row,dict) and set(row)==expected
        and row['state'] in {'APPLIED','REUSED'} and isinstance(row['event_rows'],list)
        and isinstance(row['audit_rows'],list),'INVALID_WORKFLOW_FAILURE_RESULT')
    try:
        valid=(decode_row(row['job_row'],'jobs')==decode_row(plan.job_json,'jobs')
            and decode_row(row['attempt_row'],'job_attempts')==decode_row(plan.attempt_json,'job_attempts')
            and [decode_row(value,'stage_events') for value in row['event_rows']]
                ==[decode_row(value,'stage_events') for value in plan.event_rows_json]
            and [decode_row(value,'audit_events') for value in row['audit_rows']]
                ==[decode_row(value,'audit_events') for value in plan.audit_rows_json])
    except ContractError:raise ContractError('INVALID_WORKFLOW_FAILURE_RESULT') from None
    _require(valid,'INVALID_WORKFLOW_FAILURE_RESULT');return row['state']
