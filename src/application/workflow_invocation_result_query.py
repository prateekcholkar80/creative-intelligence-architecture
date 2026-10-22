"""Pure guarded result/accounting plan for a dispatched planner invocation."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime,timedelta
from decimal import Decimal
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
    ContractError,JobState,TOKEN_FIELDS,_require,_timestamp,canonical_json_bytes,
    transition_job,validate_attempt,validate_model_invocation_change,
)
from src.application.workflow_schema import TABLES
from src.application.workspace_contract import propose_workspace_fence,validate_workspace
from src.generators.sales_generator import PROJECT,ROOT

SQL_TEMPLATE=ROOT/'sql/record_model_invocation_result.sql'
PLAN_TTL=timedelta(seconds=30)


@dataclass(frozen=True,slots=True)
class ModelResultObservation:
    state: str
    returned_model_id: str | None = None
    response_sha256: str | None = None
    finish_reason: str | None = None
    safety_outcome: str | None = None
    safe_error_code: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_tokens: int | None = None
    thinking_tokens: int | None = None
    estimated_cost: Decimal | None = None
    currency: str | None = None
    pricing_version: str | None = None


@dataclass(frozen=True,slots=True)
class WorkflowInvocationResultPlan:
    native_job_id: str
    workspace_id: str
    invocation_id: str
    outcome: str
    invocation_json: str
    job_json: str
    attempt_json: str
    result_audit_json: str
    accounting_audit_json: str
    recovery_job_audit_json: str | None
    recovery_attempt_audit_json: str | None
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
    allowed={'request_pin':'requests','job_pin':'jobs','attempt_pin':'job_attempts',
        'invocation_pin':'model_invocations','invocation_candidate':'model_invocations',
        'job_candidate':'jobs','attempt_candidate':'job_attempts','result_audit':'audit_events',
        'accounting_audit':'audit_events','recovery_job_audit':'audit_events',
        'recovery_attempt_audit':'audit_events','workspace_candidate':'workspaces'}
    if allowed.get(parameter)!=table:raise ContractError('INVALID_WORKFLOW_RESULT_SQL')
    return f'STRUCT({_fields_expr(TABLES[table].schema,f"PARSE_JSON(@{parameter})")})'


def _recovery_sql(recovery: bool) -> dict[str,str]:
    if not recovery:
        return {'__PARENT_ASSERT__':f'''ASSERT (SELECT COUNT(*) FROM `{PROJECT}.ci_application.jobs` t
  WHERE workspace_id=@workspace_id AND job_id=@job_id AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_PIN__))=1 AS 'WORKFLOW_RESULT_JOB_CONFLICT';
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.ci_application.job_attempts` t
  WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
    AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
    AND TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_PIN__))=1 AS 'WORKFLOW_RESULT_ATTEMPT_CONFLICT';''',
            '__RECOVERY_AUDIT_COUNT__':'','__RECOVERY_NEW__':'TRUE','__RECOVERY_REPLAY__':'TRUE',
            '__RECOVERY_UPDATE__':'','__RECOVERY_AUDIT_INSERT__':'',"__RECOVERY_RESULT__":"NULL,NULL"}
    return {'__PARENT_ASSERT__':f'''ASSERT ((SELECT COUNT(*) FROM `{PROJECT}.ci_application.jobs` t
  WHERE workspace_id=@workspace_id AND job_id=@job_id
    AND (TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_PIN__) OR TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_CANDIDATE__)))=1) AS 'WORKFLOW_RESULT_JOB_CONFLICT';
ASSERT ((SELECT COUNT(*) FROM `{PROJECT}.ci_application.job_attempts` t
  WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
    AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
    AND (TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_PIN__) OR TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_CANDIDATE__)))=1) AS 'WORKFLOW_RESULT_ATTEMPT_CONFLICT';''',
        '__RECOVERY_AUDIT_COUNT__':f'''ASSERT (SELECT COUNT(*) FROM `{PROJECT}.ci_application.audit_events`
  WHERE workspace_id=@workspace_id AND audit_id IN (@recovery_job_audit_id,@recovery_attempt_audit_id)
    AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=2 AS 'WORKFLOW_RESULT_RECOVERY_AUDIT_DUPLICATE';''',
        '__RECOVERY_NEW__':f'''(SELECT COUNT(*) FROM `{PROJECT}.ci_application.audit_events`
  WHERE workspace_id=@workspace_id AND audit_id IN (@recovery_job_audit_id,@recovery_attempt_audit_id)
    AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0''',
        '__RECOVERY_REPLAY__':f'''(SELECT COUNT(*) FROM `{PROJECT}.ci_application.jobs` t WHERE workspace_id=@workspace_id
  AND job_id=@job_id AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_CANDIDATE__))=1
AND (SELECT COUNT(*) FROM `{PROJECT}.ci_application.job_attempts` t WHERE workspace_id=@workspace_id
  AND attempt_id=@attempt_id AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
  AND TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_CANDIDATE__))=1
AND (SELECT COUNT(*) FROM `{PROJECT}.ci_application.audit_events` t WHERE workspace_id=@workspace_id
  AND audit_id=@recovery_job_audit_id AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
  AND TO_JSON_STRING(t)=TO_JSON_STRING(__RECOVERY_JOB_AUDIT__))=1
AND (SELECT COUNT(*) FROM `{PROJECT}.ci_application.audit_events` t WHERE workspace_id=@workspace_id
  AND audit_id=@recovery_attempt_audit_id AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
  AND TO_JSON_STRING(t)=TO_JSON_STRING(__RECOVERY_ATTEMPT_AUDIT__))=1''',
        '__RECOVERY_UPDATE__':f'''UPDATE `{PROJECT}.ci_application.jobs` t SET state=r.state,revision=r.revision,
  safe_error_code=r.safe_error_code,updated_at=r.updated_at,completed_at=r.completed_at
  FROM UNNEST([__JOB_CANDIDATE__]) r WHERE t.workspace_id=r.workspace_id AND t.job_id=r.job_id AND t.revision=@job_revision;
ASSERT @@row_count=1 AS 'WORKFLOW_RESULT_JOB_UPDATE_CONFLICT';
UPDATE `{PROJECT}.ci_application.job_attempts` t SET state=r.state,revision=r.revision,
  safe_error_code=r.safe_error_code,updated_at=r.updated_at,completed_at=r.completed_at
  FROM UNNEST([__ATTEMPT_CANDIDATE__]) r WHERE t.workspace_id=r.workspace_id AND t.attempt_id=r.attempt_id
    AND t.revision=@attempt_revision AND t.started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31');
ASSERT @@row_count=1 AS 'WORKFLOW_RESULT_ATTEMPT_UPDATE_CONFLICT';''',
        '__RECOVERY_AUDIT_INSERT__':f'''INSERT INTO `{PROJECT}.ci_application.audit_events`
  SELECT r.* FROM UNNEST([__RECOVERY_JOB_AUDIT__,__RECOVERY_ATTEMPT_AUDIT__]) r;''',
        '__RECOVERY_RESULT__':'TO_JSON_STRING(__RECOVERY_JOB_AUDIT__),TO_JSON_STRING(__RECOVERY_ATTEMPT_AUDIT__)'}


def _sql(recovery: bool) -> str:
    sql=SQL_TEMPLATE.read_text()
    for token,value in _recovery_sql(recovery).items():
        if token not in sql:raise ContractError('INVALID_WORKFLOW_RESULT_SQL')
        sql=sql.replace(token,value)
    replacements={'__REQUEST_PIN__':_row_expr('request_pin','requests'),'__JOB_PIN__':_row_expr('job_pin','jobs'),
        '__ATTEMPT_PIN__':_row_expr('attempt_pin','job_attempts'),
        '__INVOCATION_PIN__':_row_expr('invocation_pin','model_invocations'),
        '__INVOCATION_CANDIDATE__':_row_expr('invocation_candidate','model_invocations'),
        '__JOB_CANDIDATE__':_row_expr('job_candidate','jobs'),
        '__ATTEMPT_CANDIDATE__':_row_expr('attempt_candidate','job_attempts'),
        '__RESULT_AUDIT__':_row_expr('result_audit','audit_events'),
        '__ACCOUNTING_AUDIT__':_row_expr('accounting_audit','audit_events'),
        '__WORKSPACE_CANDIDATE__':_row_expr('workspace_candidate','workspaces')}
    if recovery:
        replacements.update({'__RECOVERY_JOB_AUDIT__':_row_expr('recovery_job_audit','audit_events'),
            '__RECOVERY_ATTEMPT_AUDIT__':_row_expr('recovery_attempt_audit','audit_events')})
    for token,value in replacements.items():
        if token not in sql:raise ContractError('INVALID_WORKFLOW_RESULT_SQL')
        sql=sql.replace(token,value)
    if '__' in sql:raise ContractError('INVALID_WORKFLOW_RESULT_SQL')
    return sql


def _audit(*,worker_id: str,workspace_id: str,action: WorkflowAction,target: AuditTarget,
           mutation: MutationObservation,trace_id: str,at: datetime,audit_id: str | None=None) -> str:
    observation=WorkflowObservation(worker_id,workspace_id,action)
    envelope=produce_service_audit(observation,target=target,mutation=mutation,
        trace_id=trace_id,observed_at=at,recorded_at=at)
    row=envelope.row()
    if audit_id is not None:
        row['audit_id']=audit_id
        validate_audit_event(row,observation=observation,target=target,mutation=mutation,
            trace_id=trace_id,observed_at=at,recorded_at=at)
    return encode_row(row,'audit_events')


def _candidate(previous: dict,observation: ModelResultObservation,observed_at: datetime) -> dict:
    if not isinstance(observation,ModelResultObservation) or observation.state not in {'SUCCEEDED','FAILED','BLOCKED','UNKNOWN'}:
        raise ContractError('INVALID_WORKFLOW_RESULT')
    tokens={key:getattr(observation,key) for key in TOKEN_FIELDS};reported=sum(value is not None for value in tokens.values())
    usage='NOT_REPORTED' if reported==0 else 'REPORTED' if reported==len(tokens) else 'PARTIAL'
    if observation.estimated_cost is None:
        if observation.currency is not None or observation.pricing_version is not None:raise ContractError('INVALID_WORKFLOW_RESULT')
        cost='NOT_ESTIMATED'
    else:
        if not isinstance(observation.estimated_cost,Decimal):raise ContractError('INVALID_WORKFLOW_RESULT')
        cost='ESTIMATED' if usage=='REPORTED' else 'PARTIAL'
    result=deepcopy(previous)
    result.update(state=observation.state,revision=previous['revision']+1,
        returned_model_id=observation.returned_model_id,response_sha256=observation.response_sha256,
        finish_reason=observation.finish_reason,safety_outcome=observation.safety_outcome,
        safe_error_code=observation.safe_error_code,usage_status=usage,cost_status=cost,
        estimated_cost=observation.estimated_cost,currency=observation.currency,
        pricing_version=observation.pricing_version,
        completed_at=None if observation.state=='UNKNOWN' else observed_at,**tokens)
    return result


def prepare_workflow_invocation_result(*,workspace: dict,membership: dict,conversation: dict,
                                       request: dict,job: dict,attempt: dict,invocation: dict,
                                       observation: ModelResultObservation,observed_at: datetime,
                                       settings: SharingQuerySettings=SharingQuerySettings()) -> WorkflowInvocationResultPlan:
    if not isinstance(settings,SharingQuerySettings):raise ContractError('INVALID_WORKFLOW_RESULT')
    _timestamp(observed_at);workspace_id=request.get('workspace_id');principal_id=request.get('principal_id')
    validate_workspace(workspace,workspace_id=workspace_id)
    member=validate_membership(membership,workspace_id=workspace_id,principal_id=principal_id)
    resource=validate_conversation(conversation,workspace_id=workspace_id,
        conversation_id=request.get('conversation_id'))
    require_authorized(Principal(principal_id),member,Action.GENERATE_DRAFT,workspace_id=workspace_id)
    if (workspace['status']!='ACTIVE' or conversation['status']!='ACTIVE' or resource.owner_id!=principal_id
            or job['state']!='RUNNING' or attempt['state']!='RUNNING'
            or job['active_attempt_id']!=attempt['attempt_id'] or invocation.get('state')!='DISPATCHED'
            or invocation.get('revision')!=2 or invocation.get('dispatch_receipt_id') is None
            or invocation.get('dispatch_receipt_id')!=attempt.get('dispatch_receipt_id')
            or invocation.get('worker_id')!=attempt.get('worker_id')
            or observed_at<invocation.get('started_at')
            or max(workspace['updated_at'],membership['updated_at'],conversation['updated_at'],
                   job['updated_at'],attempt['updated_at'])>observed_at):
        raise ContractError('INVALID_WORKFLOW_RESULT')
    candidate=_candidate(invocation,observation,observed_at);recovery=candidate['state']=='UNKNOWN'
    if recovery:
        job_candidate=transition_job(job,request=request,conversation=resource,target=JobState.RECOVERY_REQUIRED,
            expected_revision=job['revision'],now=observed_at,attempt_id=attempt['attempt_id'],
            safe_error_code=candidate['safe_error_code'])
        attempt_candidate=deepcopy(attempt);attempt_candidate.update(state='RECOVERY_REQUIRED',
            revision=attempt['revision']+1,safe_error_code=candidate['safe_error_code'],updated_at=observed_at)
        validate_attempt(attempt_candidate,request=request,job=job_candidate,conversation=resource)
    else:job_candidate,attempt_candidate=deepcopy(job),deepcopy(attempt)
    validate_model_invocation_change(invocation,candidate,expected_revision=invocation['revision'],
        request=request,conversation=resource,job=job_candidate,attempt=attempt_candidate,
        previous_job=job,previous_attempt=attempt)
    target=AuditTarget(ResourceType.MODEL_INVOCATION,invocation['invocation_id'],workspace_id,invocation['revision'],
        request['conversation_id'],request['owner_id'],request['request_id'],job['job_id'])
    result_audit=_audit(worker_id=attempt['worker_id'],workspace_id=workspace_id,
        action=WorkflowAction.RECORD_MODEL_RESULT,target=target,
        mutation=MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,candidate['revision']),
        trace_id=request['trace_id'],at=observed_at,
        audit_id=f'invocation-v1:RESULT:{invocation["invocation_id"]}:{candidate["revision"]}')
    accounting_audit=_audit(worker_id=attempt['worker_id'],workspace_id=workspace_id,
        action=WorkflowAction.RECORD_MODEL_ACCOUNTING,target=target,
        mutation=MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,candidate['revision']),
        trace_id=request['trace_id'],at=observed_at,
        audit_id=f'invocation-v1:ACCOUNTING:{invocation["invocation_id"]}:{candidate["revision"]}')
    recovery_job_audit=recovery_attempt_audit=None
    if recovery:
        job_target=AuditTarget(ResourceType.JOB,job['job_id'],workspace_id,job['revision'],request['conversation_id'],
            request['owner_id'],request['request_id'],job['job_id'])
        attempt_target=AuditTarget(ResourceType.JOB_ATTEMPT,attempt['attempt_id'],workspace_id,attempt['revision'],
            request['conversation_id'],request['owner_id'],request['request_id'],job['job_id'])
        recovery_job_audit=_audit(worker_id=attempt['worker_id'],workspace_id=workspace_id,
            action=WorkflowAction.UPDATE_JOB,target=job_target,
            mutation=MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,job_candidate['revision']),
            trace_id=request['trace_id'],at=observed_at,
            audit_id=f'invocation-v1:RECOVERY_JOB:{invocation["invocation_id"]}:{candidate["revision"]}')
        recovery_attempt_audit=_audit(worker_id=attempt['worker_id'],workspace_id=workspace_id,
            action=WorkflowAction.UPDATE_ATTEMPT,target=attempt_target,
            mutation=MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,attempt_candidate['revision']),
            trace_id=request['trace_id'],at=observed_at,
            audit_id=f'invocation-v1:RECOVERY_ATTEMPT:{invocation["invocation_id"]}:{candidate["revision"]}')
    fence=propose_workspace_fence(workspace,workspace_id=workspace_id,
        expected_revision=workspace['revision'],observed_at=observed_at)
    scalar={'request_pin':encode_row(request,'requests'),'job_pin':encode_row(job,'jobs'),
        'attempt_pin':encode_row(attempt,'job_attempts'),'invocation_pin':encode_row(invocation,'model_invocations'),
        'invocation_candidate':encode_row(candidate,'model_invocations'),'job_candidate':encode_row(job_candidate,'jobs'),
        'attempt_candidate':encode_row(attempt_candidate,'job_attempts'),'result_audit':result_audit,
        'accounting_audit':accounting_audit,'workspace_candidate':encode_row(fence,'workspaces')}
    if recovery:
        scalar.update(recovery_job_audit=recovery_job_audit,recovery_attempt_audit=recovery_attempt_audit)
    parameters=[bigquery.ScalarQueryParameter(name,'STRING',value) for name,value in scalar.items()]
    parameters.extend([bigquery.ScalarQueryParameter(name,kind,value) for name,kind,value in (
        ('workspace_id','STRING',workspace_id),('principal_id','STRING',principal_id),
        ('conversation_id','STRING',request['conversation_id']),('request_id','STRING',request['request_id']),
        ('job_id','STRING',job['job_id']),('attempt_id','STRING',attempt['attempt_id']),
        ('invocation_id','STRING',invocation['invocation_id']),('worker_id','STRING',attempt['worker_id']),
        ('dispatch_receipt_id','STRING',invocation['dispatch_receipt_id']),
        ('result_audit_id','STRING',decode_row(result_audit,'audit_events')['audit_id']),
        ('accounting_audit_id','STRING',decode_row(accounting_audit,'audit_events')['audit_id']),
        ('workspace_revision','INT64',workspace['revision']),('workspace_candidate_revision','INT64',fence['revision']),
        ('membership_revision','INT64',membership['revision']),('conversation_revision','INT64',conversation['access_revision']),
        ('job_revision','INT64',job['revision']),('attempt_revision','INT64',attempt['revision']),
        ('invocation_revision','INT64',invocation['revision']),('workspace_updated_at','TIMESTAMP',workspace['updated_at']),
        ('membership_updated_at','TIMESTAMP',membership['updated_at']),
        ('conversation_updated_at','TIMESTAMP',conversation['updated_at']),
        ('result_observed_at','TIMESTAMP',observed_at),('plan_expires_at','TIMESTAMP',observed_at+PLAN_TTL))])
    if recovery:
        parameters.extend([bigquery.ScalarQueryParameter('recovery_job_audit_id','STRING',
            decode_row(recovery_job_audit,'audit_events')['audit_id']),
            bigquery.ScalarQueryParameter('recovery_attempt_audit_id','STRING',
            decode_row(recovery_attempt_audit,'audit_events')['audit_id'])])
    config=query_config(parameters,settings)
    digest=hashlib.sha256(canonical_json_bytes([PROJECT,workspace_id,invocation['invocation_id'],
        invocation['dispatch_receipt_id'],candidate['state'],candidate['revision'],encode_row(candidate,'model_invocations')])).hexdigest()
    return WorkflowInvocationResultPlan('workflow_result_v1_'+digest,workspace_id,invocation['invocation_id'],
        candidate['state'],scalar['invocation_candidate'],scalar['job_candidate'],scalar['attempt_candidate'],
        result_audit,accounting_audit,recovery_job_audit,recovery_attempt_audit,_sql(recovery),
        json.dumps(config.to_api_repr(),sort_keys=True))


def validate_workflow_invocation_result_write(plan: WorkflowInvocationResultPlan,row: dict) -> str:
    expected={'state','invocation_row','job_row','attempt_row','result_audit','accounting_audit',
        'recovery_job_audit','recovery_attempt_audit'}
    _require(isinstance(plan,WorkflowInvocationResultPlan) and isinstance(row,dict) and set(row)==expected
        and row['state'] in {'APPLIED','REUSED'},'INVALID_WORKFLOW_RESULT_RESPONSE')
    pairs=(('invocation_row',plan.invocation_json,'model_invocations'),('job_row',plan.job_json,'jobs'),
        ('attempt_row',plan.attempt_json,'job_attempts'),('result_audit',plan.result_audit_json,'audit_events'),
        ('accounting_audit',plan.accounting_audit_json,'audit_events'))
    try:valid=all(decode_row(row[key],table)==decode_row(value,table) for key,value,table in pairs)
    except ContractError:raise ContractError('INVALID_WORKFLOW_RESULT_RESPONSE') from None
    for key,value in (('recovery_job_audit',plan.recovery_job_audit_json),
                      ('recovery_attempt_audit',plan.recovery_attempt_audit_json)):
        if value is None:valid=valid and row[key] is None
        else:
            try:valid=valid and decode_row(row[key],'audit_events')==decode_row(value,'audit_events')
            except ContractError:raise ContractError('INVALID_WORKFLOW_RESULT_RESPONSE') from None
    _require(valid,'INVALID_WORKFLOW_RESULT_RESPONSE');return row['state']
