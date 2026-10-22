"""Pure guarded publication plan for one successful workflow output."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict,dataclass
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
    ContractError,JobState,_require,_timestamp,canonical_json_bytes,transition_job,
    validate_attempt,validate_model_invocation,validate_output,validate_stage_events,
)
from src.application.workflow_evidence_query import WorkflowEvidencePlan
from src.application.workflow_schema import TABLES
from src.application.workspace_contract import propose_workspace_fence,validate_workspace
from src.generators.sales_generator import PROJECT,ROOT

SQL_TEMPLATE=ROOT/'sql/publish_workflow_output.sql'
PLAN_TTL=timedelta(seconds=30)
MAX_ARTIFACT_VERIFICATION_AGE=timedelta(seconds=30)
ARTIFACT_VERIFICATION_VERSION='gcs-artifact-verification-v1'


@dataclass(frozen=True,slots=True)
class ArtifactVerification:
    artifact_id: str
    gcs_uri: str
    gcs_generation: str
    sha256: str
    content_type: str
    size_bytes: int
    verified_at: datetime
    verification_version: str=ARTIFACT_VERIFICATION_VERSION


@dataclass(frozen=True,slots=True)
class WorkflowOutputPlan:
    native_job_id: str
    workspace_id: str
    workflow_job_id: str
    attempt_id: str
    output_id: str
    artifact_verification_sha256: str
    output_json: str
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
        'invocation_pin':'model_invocations','evidence_audit':'audit_events',
        'existing_event':'stage_events','output_candidate':'outputs','workspace_candidate':'workspaces'}
    if allowed.get(parameter)!=table:raise ContractError('INVALID_WORKFLOW_OUTPUT_SQL')
    return f'STRUCT({_fields_expr(TABLES[table].schema,f"PARSE_JSON(@{parameter})")})'


def _array_expr(parameter: str,table: str) -> str:
    return f'ARRAY(SELECT AS STRUCT {_fields_expr(TABLES[table].schema,"PARSE_JSON(item)")} FROM UNNEST(@{parameter}) item)'


def _sql() -> str:
    replacements={'__REQUEST_PIN__':_row_expr('request_pin','requests'),
        '__JOB_PIN__':_row_expr('job_pin','jobs'),'__JOB_CANDIDATE__':_row_expr('job_candidate','jobs'),
        '__ATTEMPT_PIN__':_row_expr('attempt_pin','job_attempts'),
        '__ATTEMPT_CANDIDATE__':_row_expr('attempt_candidate','job_attempts'),
        '__INVOCATION_PIN__':_row_expr('invocation_pin','model_invocations'),
        '__EVIDENCE_ROWS__':_array_expr('evidence_rows','evidence_snapshots'),
        '__EVIDENCE_AUDIT__':_row_expr('evidence_audit','audit_events'),
        '__EXISTING_EVENT__':_row_expr('existing_event','stage_events'),
        '__NEW_EVENTS__':_array_expr('new_events','stage_events'),
        '__OUTPUT_CANDIDATE__':_row_expr('output_candidate','outputs'),
        '__AUDIT_ROWS__':_array_expr('audit_rows','audit_events'),
        '__WORKSPACE_CANDIDATE__':_row_expr('workspace_candidate','workspaces')}
    sql=SQL_TEMPLATE.read_text()
    for token,value in replacements.items():
        if token not in sql:raise ContractError('INVALID_WORKFLOW_OUTPUT_SQL')
        sql=sql.replace(token,value)
    if '__' in sql:raise ContractError('INVALID_WORKFLOW_OUTPUT_SQL')
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


def _evidence(plan: WorkflowEvidencePlan,*,request: dict,job: dict,attempt: dict,conversation) -> tuple[list[dict],dict]:
    if not isinstance(plan,WorkflowEvidencePlan):raise ContractError('INVALID_WORKFLOW_OUTPUT')
    rows=[decode_row(value,'evidence_snapshots') for value in plan.evidence_rows_json]
    validate_evidence_bundle(rows,request=request,job=job,attempt=attempt,conversation=conversation)
    expected=hashlib.sha256(canonical_json_bytes(list(plan.evidence_rows_json))).hexdigest()
    audit=decode_row(plan.audit_json,'audit_events')
    observation=WorkflowObservation(attempt['worker_id'],job['workspace_id'],WorkflowAction.APPEND_EVIDENCE)
    target=AuditTarget(ResourceType.EVIDENCE_SNAPSHOT,plan.evidence_snapshot_id,job['workspace_id'],
        conversation_id=request['conversation_id'],owner_id=request['owner_id'],
        request_id=request['request_id'],job_id=job['job_id'])
    validate_audit_event(audit,observation=observation,target=target,
        mutation=MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED),
        trace_id=request['trace_id'],observed_at=audit['occurred_at'],recorded_at=audit['occurred_at'])
    if (expected!=plan.bundle_sha256 or plan.workspace_id!=job['workspace_id']
            or plan.workflow_job_id!=job['job_id'] or plan.attempt_id!=attempt['attempt_id']
            or {row['evidence_snapshot_id'] for row in rows}!={plan.evidence_snapshot_id}):
        raise ContractError('INVALID_WORKFLOW_OUTPUT')
    return rows,audit


def _verification_digest(output: dict,verifications: tuple[ArtifactVerification,...],observed_at: datetime) -> str:
    if not isinstance(verifications,tuple) or len(verifications)!=len(output.get('artifacts',())):
        raise ContractError('INVALID_ARTIFACT_VERIFICATION')
    by_id={item.artifact_id:item for item in verifications if isinstance(item,ArtifactVerification)}
    if len(by_id)!=len(verifications):raise ContractError('INVALID_ARTIFACT_VERIFICATION')
    for artifact in output['artifacts']:
        item=by_id.get(artifact['artifact_id'])
        if item is None:raise ContractError('INVALID_ARTIFACT_VERIFICATION')
        _timestamp(item.verified_at)
        expected={key:artifact[key] for key in ('artifact_id','gcs_uri','gcs_generation','sha256','content_type','size_bytes')}
        actual={key:getattr(item,key) for key in expected}
        if (actual!=expected or item.verification_version!=ARTIFACT_VERIFICATION_VERSION
                or item.verified_at>observed_at or observed_at-item.verified_at>MAX_ARTIFACT_VERIFICATION_AGE):
            raise ContractError('INVALID_ARTIFACT_VERIFICATION')
    values=[]
    for key in sorted(by_id):
        value=asdict(by_id[key]);value['verified_at']=value['verified_at'].isoformat();values.append(value)
    return hashlib.sha256(canonical_json_bytes(values)).hexdigest()


def prepare_workflow_output(*,workspace: dict,membership: dict,conversation: dict,
                            request: dict,job: dict,attempt: dict,invocation: dict,
                            evidence_plan: WorkflowEvidencePlan,stage_events: tuple[dict,...],
                            output: dict,artifact_verifications: tuple[ArtifactVerification,...],
                            observed_at: datetime,
                            settings: SharingQuerySettings=SharingQuerySettings()) -> WorkflowOutputPlan:
    """Build a deterministic success-publication plan from already verified inputs."""
    if not isinstance(settings,SharingQuerySettings):raise ContractError('INVALID_WORKFLOW_OUTPUT')
    _timestamp(observed_at);workspace_id=request.get('workspace_id');principal_id=request.get('principal_id')
    validate_workspace(workspace,workspace_id=workspace_id)
    member=validate_membership(membership,workspace_id=workspace_id,principal_id=principal_id)
    resource=validate_conversation(conversation,workspace_id=workspace_id,
        conversation_id=request.get('conversation_id'))
    require_authorized(Principal(principal_id),member,Action.GENERATE_DRAFT,workspace_id=workspace_id)
    validate_attempt(attempt,request=request,job=job,conversation=resource)
    validate_model_invocation(invocation,request=request,conversation=resource,job=job,attempt=attempt)
    evidence_rows,evidence_audit=_evidence(evidence_plan,request=request,job=job,attempt=attempt,conversation=resource)
    if (workspace['status']!='ACTIVE' or conversation['status']!='ACTIVE' or resource.owner_id!=principal_id
            or job['state']!='RUNNING' or attempt['state']!='RUNNING'
            or job['active_attempt_id']!=attempt['attempt_id'] or attempt['worker_id']!=invocation.get('worker_id')
            or invocation.get('state')!='SUCCEEDED' or invocation.get('stage')!='PLANNER'
            or invocation.get('response_sha256')!=output.get('brief',{}).get('model',{}).get('response_sha256')
            or output.get('evidence_snapshot_id')!=evidence_plan.evidence_snapshot_id
            or observed_at>=job['deadline_at'] or max(workspace['updated_at'],membership['updated_at'],
                conversation['updated_at'],job['updated_at'],attempt['updated_at'],invocation['completed_at'])>observed_at):
        raise ContractError('INVALID_WORKFLOW_OUTPUT')
    job_candidate=transition_job(job,request=request,conversation=resource,target=JobState.SUCCEEDED,
        expected_revision=job['revision'],now=observed_at,attempt_id=attempt['attempt_id'],
        output_id=output.get('output_id'))
    attempt_candidate=deepcopy(attempt);attempt_candidate.update(state='SUCCEEDED',revision=attempt['revision']+1,
        safe_error_code=None,updated_at=observed_at,completed_at=observed_at)
    validate_attempt(attempt_candidate,request=request,job=job_candidate,conversation=resource)
    validate_output(output,request=request,job=job_candidate,conversation=resource)
    if (not isinstance(stage_events,tuple) or len(stage_events)!=8
            or stage_events[0].get('sequence')!=1 or stage_events[-1].get('occurred_at')!=observed_at):
        raise ContractError('INVALID_WORKFLOW_OUTPUT')
    events=[deepcopy(row) for row in stage_events]
    validate_stage_events(events,attempt=attempt_candidate,request=request,job=job_candidate,conversation=resource)
    verification_sha256=_verification_digest(output,artifact_verifications,observed_at)
    worker_id=attempt['worker_id'];trace_id=request['trace_id'];output_id=output['output_id']
    targets=[
        (WorkflowAction.APPEND_OUTPUT,AuditTarget(ResourceType.OUTPUT,output_id,workspace_id,
            conversation_id=request['conversation_id'],owner_id=request['owner_id'],request_id=request['request_id'],
            job_id=job['job_id']),MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED),
            f'workflow-output-v1:OUTPUT:{output_id}'),
        (WorkflowAction.UPDATE_JOB,AuditTarget(ResourceType.JOB,job['job_id'],workspace_id,job['revision'],
            request['conversation_id'],request['owner_id'],request['request_id'],job['job_id']),
            MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,job_candidate['revision']),
            f'workflow-output-v1:JOB:{output_id}'),
        (WorkflowAction.UPDATE_ATTEMPT,AuditTarget(ResourceType.JOB_ATTEMPT,attempt['attempt_id'],workspace_id,
            attempt['revision'],request['conversation_id'],request['owner_id'],request['request_id'],job['job_id']),
            MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,attempt_candidate['revision']),
            f'workflow-output-v1:ATTEMPT:{output_id}')]
    for event in events[1:]:
        targets.append((WorkflowAction.APPEND_STAGE_EVENT,AuditTarget(ResourceType.STAGE_EVENT,event['event_id'],workspace_id,
            conversation_id=request['conversation_id'],owner_id=request['owner_id'],request_id=request['request_id'],
            job_id=job['job_id']),MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED),
            f'workflow-output-v1:EVENT:{event["event_id"]}'))
    audits=tuple(_audit(worker_id=worker_id,workspace_id=workspace_id,action=action,target=target,
        mutation=mutation,trace_id=trace_id,at=observed_at,audit_id=audit_id)
        for action,target,mutation,audit_id in targets)
    fence=propose_workspace_fence(workspace,workspace_id=workspace_id,
        expected_revision=workspace['revision'],observed_at=observed_at)
    evidence_json=tuple(encode_row(row,'evidence_snapshots') for row in evidence_rows)
    event_json=tuple(encode_row(row,'stage_events') for row in events[1:])
    scalar={'request_pin':encode_row(request,'requests'),'job_pin':encode_row(job,'jobs'),
        'job_candidate':encode_row(job_candidate,'jobs'),'attempt_pin':encode_row(attempt,'job_attempts'),
        'attempt_candidate':encode_row(attempt_candidate,'job_attempts'),
        'invocation_pin':encode_row(invocation,'model_invocations'),'evidence_audit':evidence_plan.audit_json,
        'existing_event':encode_row(events[0],'stage_events'),'output_candidate':encode_row(output,'outputs'),
        'workspace_candidate':encode_row(fence,'workspaces')}
    parameters=[bigquery.ScalarQueryParameter(name,'STRING',value) for name,value in scalar.items()]
    parameters.extend([bigquery.ArrayQueryParameter('evidence_rows','STRING',list(evidence_json)),
        bigquery.ArrayQueryParameter('new_events','STRING',list(event_json)),
        bigquery.ArrayQueryParameter('audit_rows','STRING',list(audits)),
        bigquery.ArrayQueryParameter('new_event_ids','STRING',[row['event_id'] for row in events[1:]]),
        bigquery.ArrayQueryParameter('audit_ids','STRING',[decode_row(row,'audit_events')['audit_id'] for row in audits])])
    parameters.extend(bigquery.ScalarQueryParameter(name,kind,value) for name,kind,value in (
        ('workspace_id','STRING',workspace_id),('principal_id','STRING',principal_id),
        ('conversation_id','STRING',request['conversation_id']),('request_id','STRING',request['request_id']),
        ('job_id','STRING',job['job_id']),('attempt_id','STRING',attempt['attempt_id']),
        ('invocation_id','STRING',invocation['invocation_id']),('output_id','STRING',output_id),
        ('evidence_snapshot_id','STRING',evidence_plan.evidence_snapshot_id),
        ('evidence_audit_id','STRING',evidence_audit['audit_id']),('existing_event_id','STRING',events[0]['event_id']),
        ('artifact_verification_sha256','STRING',verification_sha256),
        ('evidence_row_count','INT64',len(evidence_rows)),('event_row_count','INT64',len(events)),
        ('new_event_row_count','INT64',len(event_json)),('audit_row_count','INT64',len(audits)),
        ('workspace_revision','INT64',workspace['revision']),('workspace_candidate_revision','INT64',fence['revision']),
        ('membership_revision','INT64',membership['revision']),('conversation_revision','INT64',conversation['access_revision']),
        ('job_revision','INT64',job['revision']),('attempt_revision','INT64',attempt['revision']),
        ('workspace_updated_at','TIMESTAMP',workspace['updated_at']),
        ('membership_updated_at','TIMESTAMP',membership['updated_at']),
        ('conversation_updated_at','TIMESTAMP',conversation['updated_at']),
        ('publish_observed_at','TIMESTAMP',observed_at),('plan_expires_at','TIMESTAMP',observed_at+PLAN_TTL)))
    config=query_config(parameters,settings)
    output_json=scalar['output_candidate'];job_json=scalar['job_candidate'];attempt_json=scalar['attempt_candidate']
    digest=hashlib.sha256(canonical_json_bytes([PROJECT,workspace_id,job['job_id'],attempt['attempt_id'],
        output_id,verification_sha256,output_json,list(event_json),list(audits)])).hexdigest()
    return WorkflowOutputPlan('workflow_output_v1_'+digest,workspace_id,job['job_id'],attempt['attempt_id'],
        output_id,verification_sha256,output_json,job_json,attempt_json,event_json,audits,_sql(),
        json.dumps(config.to_api_repr(),sort_keys=True))


def validate_workflow_output_result(plan: WorkflowOutputPlan,row: dict) -> str:
    expected={'state','output_row','job_row','attempt_row','event_rows','audit_rows','artifact_verification_sha256'}
    _require(isinstance(plan,WorkflowOutputPlan) and isinstance(row,dict) and set(row)==expected
        and row['state'] in {'APPLIED','REUSED'} and row['artifact_verification_sha256']==plan.artifact_verification_sha256
        and isinstance(row['event_rows'],list) and isinstance(row['audit_rows'],list),
        'INVALID_WORKFLOW_OUTPUT_RESULT')
    try:
        valid=(decode_row(row['output_row'],'outputs')==decode_row(plan.output_json,'outputs')
            and decode_row(row['job_row'],'jobs')==decode_row(plan.job_json,'jobs')
            and decode_row(row['attempt_row'],'job_attempts')==decode_row(plan.attempt_json,'job_attempts')
            and [decode_row(value,'stage_events') for value in row['event_rows']]
                ==[decode_row(value,'stage_events') for value in plan.event_rows_json]
            and [decode_row(value,'audit_events') for value in row['audit_rows']]
                ==[decode_row(value,'audit_events') for value in plan.audit_rows_json])
    except ContractError:raise ContractError('INVALID_WORKFLOW_OUTPUT_RESULT') from None
    _require(valid,'INVALID_WORKFLOW_OUTPUT_RESULT');return row['state']
