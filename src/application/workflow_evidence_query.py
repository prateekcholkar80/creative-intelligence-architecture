"""Pure query plan for atomic publication of one complete evidence bundle."""
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
    WorkflowAction,WorkflowObservation,
)
from src.application.audit_producer import produce_service_audit
from src.application.conversation_contract import validate_conversation
from src.application.control_io import query_config
from src.application.evidence_bundle import PlanningEvidenceBundle
from src.application.evidence_contract import validate_evidence_bundle
from src.application.membership_contract import validate_membership
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_codec import decode_row,encode_row
from src.application.workflow_contract import ContractError,_require,_timestamp,canonical_json_bytes
from src.application.workflow_schema import TABLES
from src.application.workspace_contract import propose_workspace_fence,validate_workspace
from src.generators.sales_generator import PROJECT,ROOT

SQL_TEMPLATE=ROOT/'sql/append_workflow_evidence.sql'
PLAN_TTL=timedelta(seconds=30)
MAX_CAPTURE_AGE=timedelta(seconds=30)


@dataclass(frozen=True,slots=True)
class WorkflowEvidencePlan:
    native_job_id: str
    workspace_id: str
    workflow_job_id: str
    attempt_id: str
    evidence_snapshot_id: str
    bundle_sha256: str
    evidence_rows_json: tuple[str,...]
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
            else:
                value=f'STRUCT({_fields_expr(field.fields,f"JSON_QUERY({source}, {path})")})'
        elif field.mode=='REPEATED':value=f'JSON_VALUE_ARRAY({source}, {path})'
        elif field.field_type=='JSON':value=f'JSON_QUERY({source}, {path})'
        elif field.field_type!='STRING':
            kind={'INTEGER':'INT64','BOOLEAN':'BOOL','TIMESTAMP':'TIMESTAMP','NUMERIC':'NUMERIC','FLOAT':'FLOAT64'}[field.field_type]
            value=f'CAST({value} AS {kind})'
        parts.append(f'{value} AS {field.name}')
    return ', '.join(parts)


def _row_expr(parameter: str,table: str) -> str:
    allowed={'request_pin':'requests','job_pin':'jobs','attempt_pin':'job_attempts',
        'audit_row':'audit_events','workspace_candidate':'workspaces'}
    if allowed.get(parameter)!=table:raise ContractError('INVALID_WORKFLOW_EVIDENCE_SQL')
    return f'STRUCT({_fields_expr(TABLES[table].schema,f"PARSE_JSON(@{parameter})")})'


def _sql() -> str:
    array=f'ARRAY(SELECT AS STRUCT {_fields_expr(TABLES["evidence_snapshots"].schema,"PARSE_JSON(item)")} FROM UNNEST(@evidence_rows) item)'
    replacements={'__EVIDENCE_ROWS__':array,'__REQUEST_PIN__':_row_expr('request_pin','requests'),
        '__JOB_PIN__':_row_expr('job_pin','jobs'),'__ATTEMPT_PIN__':_row_expr('attempt_pin','job_attempts'),
        '__AUDIT_ROW__':_row_expr('audit_row','audit_events'),
        '__WORKSPACE_CANDIDATE__':_row_expr('workspace_candidate','workspaces')}
    sql=SQL_TEMPLATE.read_text()
    for token,value in replacements.items():
        if token not in sql:raise ContractError('INVALID_WORKFLOW_EVIDENCE_SQL')
        sql=sql.replace(token,value)
    if '__' in sql:raise ContractError('INVALID_WORKFLOW_EVIDENCE_SQL')
    return sql


def prepare_workflow_evidence(*,workspace: dict,membership: dict,conversation: dict,
                              request: dict,job: dict,attempt: dict,
                              bundle: PlanningEvidenceBundle,worker_id: str,
                              observed_at: datetime,
                              settings: SharingQuerySettings=SharingQuerySettings()) -> WorkflowEvidencePlan:
    """Build a deterministic append plan from an already captured complete bundle."""
    if not isinstance(settings,SharingQuerySettings) or not isinstance(bundle,PlanningEvidenceBundle):
        raise ContractError('INVALID_WORKFLOW_EVIDENCE')
    _timestamp(observed_at)
    workspace_id=request.get('workspace_id');principal_id=request.get('principal_id')
    validate_workspace(workspace,workspace_id=workspace_id)
    member=validate_membership(membership,workspace_id=workspace_id,principal_id=principal_id)
    resource=validate_conversation(conversation,workspace_id=workspace_id,
        conversation_id=request.get('conversation_id'))
    require_authorized(Principal(principal_id),member,Action.GENERATE_DRAFT,workspace_id=workspace_id)
    rows=sorted((deepcopy(row) for row in bundle.rows),key=lambda row:row['evidence_ref_id'])
    validate_evidence_bundle(rows,request=request,job=job,attempt=attempt,conversation=resource)
    created={row['created_at'] for row in rows};snapshot={row['evidence_snapshot_id'] for row in rows}
    knowledge={row['knowledge_snapshot_id'] for row in rows if row['knowledge_snapshot_id'] is not None}
    if (workspace['status']!='ACTIVE' or conversation['status']!='ACTIVE'
            or resource.owner_id!=principal_id or job['state']!='RUNNING'
            or job['active_attempt_id']!=attempt['attempt_id'] or attempt['state']!='RUNNING'
            or attempt['worker_id']!=worker_id or len(created)!=1 or len(snapshot)!=1 or len(knowledge)!=1
            or bundle.evidence_snapshot_id!=next(iter(snapshot))
            or bundle.knowledge_snapshot_id!=next(iter(knowledge))
            or bundle.canonical_sku!=job['canonical_sku'] or observed_at>=job['deadline_at']
            or next(iter(created))>observed_at or observed_at-next(iter(created))>MAX_CAPTURE_AGE
            or workspace['updated_at']>observed_at or membership['updated_at']>observed_at
            or conversation['updated_at']>observed_at or attempt['updated_at']>observed_at):
        raise ContractError('INVALID_WORKFLOW_EVIDENCE')
    row_json=tuple(encode_row(row,'evidence_snapshots') for row in rows)
    bundle_sha256=hashlib.sha256(canonical_json_bytes(list(row_json))).hexdigest()
    target=AuditTarget(ResourceType.EVIDENCE_SNAPSHOT,bundle.evidence_snapshot_id,workspace_id,
        conversation_id=request['conversation_id'],owner_id=request['owner_id'],
        request_id=request['request_id'],job_id=job['job_id'])
    audit=produce_service_audit(WorkflowObservation(worker_id,workspace_id,WorkflowAction.APPEND_EVIDENCE),
        target=target,mutation=MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED),
        trace_id=request['trace_id'],observed_at=observed_at,recorded_at=observed_at)
    fence=propose_workspace_fence(workspace,workspace_id=workspace_id,
        expected_revision=workspace['revision'],observed_at=observed_at)
    scalar={'request_pin':encode_row(request,'requests'),'job_pin':encode_row(job,'jobs'),
        'attempt_pin':encode_row(attempt,'job_attempts'),'audit_row':audit.audit_json,
        'workspace_candidate':encode_row(fence,'workspaces')}
    parameters=[bigquery.ScalarQueryParameter(name,'STRING',value) for name,value in scalar.items()]
    parameters.extend([
        bigquery.ArrayQueryParameter('evidence_rows','STRING',list(row_json)),
        bigquery.ScalarQueryParameter('workspace_id','STRING',workspace_id),
        bigquery.ScalarQueryParameter('principal_id','STRING',principal_id),
        bigquery.ScalarQueryParameter('conversation_id','STRING',request['conversation_id']),
        bigquery.ScalarQueryParameter('request_id','STRING',request['request_id']),
        bigquery.ScalarQueryParameter('job_id','STRING',job['job_id']),
        bigquery.ScalarQueryParameter('attempt_id','STRING',attempt['attempt_id']),
        bigquery.ScalarQueryParameter('evidence_snapshot_id','STRING',bundle.evidence_snapshot_id),
        bigquery.ScalarQueryParameter('bundle_sha256','STRING',bundle_sha256),
        bigquery.ScalarQueryParameter('evidence_row_count','INT64',len(rows)),
        bigquery.ScalarQueryParameter('audit_id','STRING',audit.row()['audit_id']),
        bigquery.ScalarQueryParameter('workspace_revision','INT64',workspace['revision']),
        bigquery.ScalarQueryParameter('workspace_candidate_revision','INT64',fence['revision']),
        bigquery.ScalarQueryParameter('membership_revision','INT64',membership['revision']),
        bigquery.ScalarQueryParameter('conversation_revision','INT64',conversation['access_revision']),
        bigquery.ScalarQueryParameter('workspace_updated_at','TIMESTAMP',workspace['updated_at']),
        bigquery.ScalarQueryParameter('membership_updated_at','TIMESTAMP',membership['updated_at']),
        bigquery.ScalarQueryParameter('conversation_updated_at','TIMESTAMP',conversation['updated_at']),
        bigquery.ScalarQueryParameter('publish_observed_at','TIMESTAMP',observed_at),
        bigquery.ScalarQueryParameter('plan_expires_at','TIMESTAMP',observed_at+PLAN_TTL),
    ])
    config=query_config(parameters,settings)
    digest=hashlib.sha256(canonical_json_bytes([PROJECT,workspace_id,job['job_id'],
        attempt['attempt_id'],bundle.evidence_snapshot_id,bundle_sha256])).hexdigest()
    return WorkflowEvidencePlan('workflow_evidence_v1_'+digest,workspace_id,job['job_id'],
        attempt['attempt_id'],bundle.evidence_snapshot_id,bundle_sha256,row_json,audit.audit_json,
        _sql(),json.dumps(config.to_api_repr(),sort_keys=True))


def validate_workflow_evidence_result(plan: WorkflowEvidencePlan,row: dict) -> str:
    expected={'state','evidence_snapshot_id','evidence_row_count','bundle_sha256','audit_row'}
    _require(isinstance(plan,WorkflowEvidencePlan) and isinstance(row,dict) and set(row)==expected
        and row['state'] in {'APPLIED','REUSED'} and row['evidence_snapshot_id']==plan.evidence_snapshot_id
        and type(row['evidence_row_count']) is int and row['evidence_row_count']==len(plan.evidence_rows_json)
        and row['bundle_sha256']==plan.bundle_sha256,'INVALID_WORKFLOW_EVIDENCE_RESULT')
    try:valid=decode_row(row['audit_row'],'audit_events')==decode_row(plan.audit_json,'audit_events')
    except ContractError:raise ContractError('INVALID_WORKFLOW_EVIDENCE_RESULT') from None
    _require(valid,'INVALID_WORKFLOW_EVIDENCE_RESULT')
    return row['state']
