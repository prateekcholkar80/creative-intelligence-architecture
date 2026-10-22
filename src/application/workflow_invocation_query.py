"""Pure query plan for a PLANNED planner invocation after evidence publication."""
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
from src.application.evidence_contract import validate_evidence_bundle
from src.application.membership_contract import validate_membership
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_codec import decode_row,encode_row
from src.application.workflow_contract import (
    ContractError,_require,_timestamp,canonical_json_bytes,validate_model_invocation,
)
from src.application.workflow_evidence_query import WorkflowEvidencePlan
from src.application.workflow_schema import TABLES
from src.application.workspace_contract import propose_workspace_fence,validate_workspace
from src.generators.sales_generator import PROJECT,ROOT

SQL_TEMPLATE=ROOT/'sql/create_model_invocation.sql'
PLAN_TTL=timedelta(seconds=30)
MAX_EVIDENCE_AGE=timedelta(seconds=30)


@dataclass(frozen=True,slots=True)
class WorkflowInvocationPlan:
    native_job_id: str
    workspace_id: str
    workflow_job_id: str
    attempt_id: str
    invocation_id: str
    evidence_snapshot_id: str
    invocation_json: str
    audit_json: str
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
        'evidence_audit':'audit_events','invocation_row':'model_invocations',
        'invocation_audit':'audit_events','workspace_candidate':'workspaces'}
    if allowed.get(parameter)!=table:raise ContractError('INVALID_WORKFLOW_INVOCATION_SQL')
    return f'STRUCT({_fields_expr(TABLES[table].schema,f"PARSE_JSON(@{parameter})")})'


def _sql() -> str:
    array=f'ARRAY(SELECT AS STRUCT {_fields_expr(TABLES["evidence_snapshots"].schema,"PARSE_JSON(item)")} FROM UNNEST(@evidence_rows) item)'
    replacements={'__EVIDENCE_ROWS__':array,'__REQUEST_PIN__':_row_expr('request_pin','requests'),
        '__JOB_PIN__':_row_expr('job_pin','jobs'),'__ATTEMPT_PIN__':_row_expr('attempt_pin','job_attempts'),
        '__EVIDENCE_AUDIT__':_row_expr('evidence_audit','audit_events'),
        '__INVOCATION_ROW__':_row_expr('invocation_row','model_invocations'),
        '__INVOCATION_AUDIT__':_row_expr('invocation_audit','audit_events'),
        '__WORKSPACE_CANDIDATE__':_row_expr('workspace_candidate','workspaces')}
    sql=SQL_TEMPLATE.read_text()
    for token,value in replacements.items():
        if token not in sql:raise ContractError('INVALID_WORKFLOW_INVOCATION_SQL')
        sql=sql.replace(token,value)
    if '__' in sql:raise ContractError('INVALID_WORKFLOW_INVOCATION_SQL')
    return sql


def _evidence(evidence_plan: WorkflowEvidencePlan,*,request: dict,job: dict,attempt: dict,
              conversation) -> tuple[list[dict],dict]:
    if not isinstance(evidence_plan,WorkflowEvidencePlan):raise ContractError('INVALID_WORKFLOW_INVOCATION')
    rows=[decode_row(value,'evidence_snapshots') for value in evidence_plan.evidence_rows_json]
    validate_evidence_bundle(rows,request=request,job=job,attempt=attempt,conversation=conversation)
    expected=hashlib.sha256(canonical_json_bytes(list(evidence_plan.evidence_rows_json))).hexdigest()
    audit=decode_row(evidence_plan.audit_json,'audit_events')
    observation=WorkflowObservation(attempt['worker_id'],job['workspace_id'],WorkflowAction.APPEND_EVIDENCE)
    target=AuditTarget(ResourceType.EVIDENCE_SNAPSHOT,evidence_plan.evidence_snapshot_id,job['workspace_id'],
        conversation_id=request['conversation_id'],owner_id=request['owner_id'],
        request_id=request['request_id'],job_id=job['job_id'])
    validate_audit_event(audit,observation=observation,target=target,
        mutation=MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED),
        observed_at=audit['occurred_at'],recorded_at=audit['occurred_at'],trace_id=request['trace_id'])
    if (expected!=evidence_plan.bundle_sha256 or evidence_plan.workspace_id!=job['workspace_id']
            or evidence_plan.workflow_job_id!=job['job_id'] or evidence_plan.attempt_id!=attempt['attempt_id']
            or {row['evidence_snapshot_id'] for row in rows}!={evidence_plan.evidence_snapshot_id}
            or audit['action']!='APPEND_EVIDENCE'):
        raise ContractError('INVALID_WORKFLOW_INVOCATION')
    return rows,audit


def prepare_workflow_invocation(*,workspace: dict,membership: dict,conversation: dict,
                                request: dict,job: dict,attempt: dict,
                                evidence_plan: WorkflowEvidencePlan,invocation_id: str,
                                worker_id: str,observed_at: datetime,
                                settings: SharingQuerySettings=SharingQuerySettings()) -> WorkflowInvocationPlan:
    """Build an exact creation plan; this grants no dispatch permission."""
    if not isinstance(settings,SharingQuerySettings):raise ContractError('INVALID_WORKFLOW_INVOCATION')
    _timestamp(observed_at)
    workspace_id=request.get('workspace_id');principal_id=request.get('principal_id')
    validate_workspace(workspace,workspace_id=workspace_id)
    member=validate_membership(membership,workspace_id=workspace_id,principal_id=principal_id)
    resource=validate_conversation(conversation,workspace_id=workspace_id,
        conversation_id=request.get('conversation_id'))
    require_authorized(Principal(principal_id),member,Action.GENERATE_DRAFT,workspace_id=workspace_id)
    rows,evidence_audit=_evidence(evidence_plan,request=request,job=job,attempt=attempt,conversation=resource)
    created={row['created_at'] for row in rows}
    if (workspace['status']!='ACTIVE' or conversation['status']!='ACTIVE' or resource.owner_id!=principal_id
            or job['state']!='RUNNING' or job['active_attempt_id']!=attempt['attempt_id']
            or attempt['state']!='RUNNING' or attempt['worker_id']!=worker_id or len(created)!=1
            or next(iter(created))>observed_at or observed_at-next(iter(created))>MAX_EVIDENCE_AGE
            or evidence_audit['occurred_at']>observed_at
            or observed_at-evidence_audit['occurred_at']>MAX_EVIDENCE_AGE
            or observed_at>=job['deadline_at'] or workspace['updated_at']>observed_at
            or membership['updated_at']>observed_at or conversation['updated_at']>observed_at
            or attempt['updated_at']>observed_at):
        raise ContractError('INVALID_WORKFLOW_INVOCATION')
    invocation=dict.fromkeys(field.name for field in TABLES['model_invocations'].schema)
    for key in ('schema_version','workspace_id','conversation_id','owner_id','request_id','trace_id'):
        invocation[key]=request[key]
    profiles=[deepcopy(profile) for profile in request['model_profiles'] if profile['stage']=='PLANNER']
    if len(profiles)!=1:raise ContractError('INVALID_WORKFLOW_INVOCATION')
    invocation.update(invocation_id=invocation_id,job_id=job['job_id'],attempt_id=attempt['attempt_id'],
        stage='PLANNER',call_number=1,state='PLANNED',revision=1,profile=profiles[0],
        usage_status='NOT_REPORTED',cost_status='NOT_ESTIMATED',started_at=observed_at)
    validate_model_invocation(invocation,request=request,conversation=resource,job=job,attempt=attempt)
    target=AuditTarget(ResourceType.MODEL_INVOCATION,invocation_id,workspace_id,
        conversation_id=request['conversation_id'],owner_id=request['owner_id'],
        request_id=request['request_id'],job_id=job['job_id'])
    audit=produce_service_audit(WorkflowObservation(worker_id,workspace_id,WorkflowAction.CREATE_MODEL_INVOCATION),
        target=target,mutation=MutationObservation(MutationKind.CREATE,MutationOutcome.APPLIED,1),
        trace_id=request['trace_id'],observed_at=observed_at,recorded_at=observed_at)
    fence=propose_workspace_fence(workspace,workspace_id=workspace_id,
        expected_revision=workspace['revision'],observed_at=observed_at)
    evidence_json=tuple(encode_row(row,'evidence_snapshots') for row in rows)
    scalar={'request_pin':encode_row(request,'requests'),'job_pin':encode_row(job,'jobs'),
        'attempt_pin':encode_row(attempt,'job_attempts'),'evidence_audit':evidence_plan.audit_json,
        'invocation_row':encode_row(invocation,'model_invocations'),'invocation_audit':audit.audit_json,
        'workspace_candidate':encode_row(fence,'workspaces')}
    parameters=[bigquery.ScalarQueryParameter(name,'STRING',value) for name,value in scalar.items()]
    parameters.extend([
        bigquery.ArrayQueryParameter('evidence_rows','STRING',list(evidence_json)),
        bigquery.ScalarQueryParameter('workspace_id','STRING',workspace_id),
        bigquery.ScalarQueryParameter('principal_id','STRING',principal_id),
        bigquery.ScalarQueryParameter('conversation_id','STRING',request['conversation_id']),
        bigquery.ScalarQueryParameter('request_id','STRING',request['request_id']),
        bigquery.ScalarQueryParameter('job_id','STRING',job['job_id']),
        bigquery.ScalarQueryParameter('attempt_id','STRING',attempt['attempt_id']),
        bigquery.ScalarQueryParameter('worker_id','STRING',worker_id),
        bigquery.ScalarQueryParameter('invocation_id','STRING',invocation_id),
        bigquery.ScalarQueryParameter('evidence_snapshot_id','STRING',evidence_plan.evidence_snapshot_id),
        bigquery.ScalarQueryParameter('evidence_row_count','INT64',len(rows)),
        bigquery.ScalarQueryParameter('evidence_audit_id','STRING',evidence_audit['audit_id']),
        bigquery.ScalarQueryParameter('invocation_audit_id','STRING',audit.row()['audit_id']),
        bigquery.ScalarQueryParameter('workspace_revision','INT64',workspace['revision']),
        bigquery.ScalarQueryParameter('workspace_candidate_revision','INT64',fence['revision']),
        bigquery.ScalarQueryParameter('membership_revision','INT64',membership['revision']),
        bigquery.ScalarQueryParameter('conversation_revision','INT64',conversation['access_revision']),
        bigquery.ScalarQueryParameter('workspace_updated_at','TIMESTAMP',workspace['updated_at']),
        bigquery.ScalarQueryParameter('membership_updated_at','TIMESTAMP',membership['updated_at']),
        bigquery.ScalarQueryParameter('conversation_updated_at','TIMESTAMP',conversation['updated_at']),
        bigquery.ScalarQueryParameter('create_observed_at','TIMESTAMP',observed_at),
        bigquery.ScalarQueryParameter('plan_expires_at','TIMESTAMP',observed_at+PLAN_TTL),
    ])
    config=query_config(parameters,settings)
    digest=hashlib.sha256(canonical_json_bytes([PROJECT,workspace_id,job['job_id'],attempt['attempt_id'],
        invocation_id,evidence_plan.evidence_snapshot_id,evidence_plan.bundle_sha256])).hexdigest()
    return WorkflowInvocationPlan('workflow_invocation_v1_'+digest,workspace_id,job['job_id'],
        attempt['attempt_id'],invocation_id,evidence_plan.evidence_snapshot_id,
        scalar['invocation_row'],audit.audit_json,_sql(),json.dumps(config.to_api_repr(),sort_keys=True))


def validate_workflow_invocation_result(plan: WorkflowInvocationPlan,row: dict) -> str:
    expected={'state','invocation_row','audit_row'}
    _require(isinstance(plan,WorkflowInvocationPlan) and isinstance(row,dict) and set(row)==expected
        and row['state'] in {'APPLIED','REUSED'},'INVALID_WORKFLOW_INVOCATION_RESULT')
    try:valid=(decode_row(row['invocation_row'],'model_invocations')==decode_row(plan.invocation_json,'model_invocations')
        and decode_row(row['audit_row'],'audit_events')==decode_row(plan.audit_json,'audit_events'))
    except ContractError:raise ContractError('INVALID_WORKFLOW_INVOCATION_RESULT') from None
    _require(valid,'INVALID_WORKFLOW_INVOCATION_RESULT')
    return row['state']
