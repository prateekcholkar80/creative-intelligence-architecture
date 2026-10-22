"""Interpreter-only atomic outcome publication and original-job recovery.

Disabled by default. Server-issued plans only; no model calls or response release.
"""
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
import hashlib,json
from threading import RLock

from google.cloud import bigquery
from src.application.access_policy import Action,Principal,require_authorized
from src.application.audit_contract import (AuditTarget,ResourceType,WorkflowAction,
    MutationObservation,MutationKind,MutationOutcome)
from src.application.conversation_contract import validate_conversation
from src.application.membership_contract import validate_membership
from src.application.workspace_contract import validate_workspace,propose_workspace_fence
from src.application.workflow_contract import (RequestState,PREFERENCE_FIELDS,transition_request,
    validate_model_invocation_change,_require,canonical_json_bytes)
from src.application.workflow_codec import encode_row,decode_row
from src.application.workflow_schema import TABLES
from src.application.workflow_invocation_result_query import ModelResultObservation,_candidate,_audit,_fields_expr
from src.application.control_io import ControlPlan,ControlIO,query_config
from src.application.identity import IapIdentityAdapter,ensure_fresh
from src.application.operation_budget import require_owned_worker
from src.application.campaign_quality_product_reader import _access
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.ingestion.setup_workflow_tables import PROJECT,DATASET_ID

KINDS={'workspace_pin':'workspaces','workspace_candidate':'workspaces','membership_pin':'memberships',
    'conversation_pin':'conversations','request_pin':'requests','request_candidate':'requests',
    'invocation_pin':'model_invocations','invocation_candidate':'model_invocations',
    'result_audit':'audit_events','accounting_audit':'audit_events'}


def _partition(table,alias='t'):
    column={'requests':'created_at','model_invocations':'started_at','audit_events':'occurred_at'}.get(table)
    return '' if column is None else f" AND {alias}.{column} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')"


def _matches(table,variable):
    return f"(SELECT COUNT(*) FROM `{DATASET_ID}.{table}` t WHERE t.workspace_id=@workspace_id AND TO_JSON_STRING(t)=TO_JSON_STRING({variable}){_partition(table)})=1"


def _sql():
    declarations='\n'.join(f"DECLARE {name} DEFAULT STRUCT({_fields_expr(TABLES[table].schema,f'PARSE_JSON(@{name})')});" for name,table in KINDS.items())
    def update(table,variable,where):
        fields=', '.join(f'{f.name}={variable}.{f.name}' for f in TABLES[table].schema)
        return f'UPDATE `{DATASET_ID}.{table}` t SET {fields} WHERE {where}{_partition(table)};'
    def insert(variable):
        fields=', '.join(f.name for f in TABLES['audit_events'].schema)
        values=', '.join(variable+'.'+f.name for f in TABLES['audit_events'].schema)
        return f'INSERT INTO `{DATASET_ID}.audit_events` ({fields}) SELECT {values};'
    return declarations+f'''
DECLARE result_state STRING DEFAULT 'REUSED';
BEGIN TRANSACTION;
ASSERT {_matches('memberships','membership_pin')} AND membership_pin.status='ACTIVE' AS 'INTERPRETER_MEMBER_CHANGED';
ASSERT {_matches('conversations','conversation_pin')} AND conversation_pin.status='ACTIVE' AS 'INTERPRETER_CONVERSATION_CHANGED';
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.workspaces` t WHERE t.workspace_id=@workspace_id AND t.status='ACTIVE')=1 AS 'INTERPRETER_WORKSPACE_INACTIVE';
IF {_matches('model_invocations','invocation_candidate')} THEN
 ASSERT {_matches('requests','request_candidate')} AND {_matches('audit_events','result_audit')} AND {_matches('audit_events','accounting_audit')} AS 'INTERPRETER_REPLAY_CONFLICT';
ELSE
 ASSERT CURRENT_TIMESTAMP()<@expires_at AS 'INTERPRETER_PLAN_EXPIRED';
 ASSERT {_matches('workspaces','workspace_pin')} AND {_matches('requests','request_pin')} AND {_matches('model_invocations','invocation_pin')} AS 'INTERPRETER_SOURCE_CHANGED';
 ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.audit_events` t WHERE t.workspace_id=@workspace_id AND t.audit_id IN (result_audit.audit_id,accounting_audit.audit_id){_partition('audit_events')})=0 AS 'INTERPRETER_AUDIT_CONFLICT';
 {update('workspaces','workspace_candidate','t.workspace_id=@workspace_id AND t.revision=workspace_pin.revision')}
 ASSERT @@row_count=1 AS 'INTERPRETER_FENCE_CONFLICT';
 {update('requests','request_candidate','t.workspace_id=@workspace_id AND t.request_id=request_pin.request_id AND t.revision=request_pin.revision')}
 ASSERT @@row_count=1 AS 'INTERPRETER_REQUEST_CONFLICT';
 {update('model_invocations','invocation_candidate','t.workspace_id=@workspace_id AND t.invocation_id=invocation_pin.invocation_id AND t.revision=invocation_pin.revision')}
 ASSERT @@row_count=1 AS 'INTERPRETER_INVOCATION_CONFLICT';
 {insert('result_audit')}
 {insert('accounting_audit')}
 SET result_state='APPLIED';
END IF;
COMMIT TRANSACTION;
SELECT result_state AS state,TO_JSON_STRING(invocation_candidate) AS invocation_row,
 TO_JSON_STRING(request_candidate) AS request_row,TO_JSON_STRING(result_audit) AS result_audit_row,
 TO_JSON_STRING(accounting_audit) AS accounting_audit_row;
'''


def prepare_interpreter_outcome(*,workspace,membership,conversation,request,invocation,observation,observed_at,response_text=None):
    ws=request['workspace_id'];actor=request['principal_id']
    validate_workspace(workspace,workspace_id=ws)
    member=validate_membership(membership,workspace_id=ws,principal_id=actor)
    resource=validate_conversation(conversation,workspace_id=ws,conversation_id=request['conversation_id'])
    require_authorized(Principal(actor),member,Action.GENERATE_DRAFT,workspace_id=ws)
    _require(workspace['status']=='ACTIVE' and conversation['status']=='ACTIVE' and resource.owner_id==actor
        and invocation['stage']=='INTERPRETER' and invocation['state']=='DISPATCHED'
        and invocation['revision']==2 and invocation['dispatch_receipt_id'] is not None
        and invocation['worker_id'] is not None and observed_at>=invocation['started_at']
        and max(workspace['updated_at'],membership['updated_at'],conversation['updated_at'],request['updated_at'])<=observed_at,
        'INTERPRETER_OUTCOME_SCOPE_INVALID')
    candidate=_candidate(invocation,observation,observed_at)
    if observation.state=='UNKNOWN':next_request=deepcopy(request)
    else:
        # Feature extraction/question writing is not full preference resolution.
        # Do not invent READY campaign fields from an evidence-only model output.
        target=RequestState.NEEDS_CLARIFICATION if observation.state=='SUCCEEDED' else RequestState.FAILED
        next_request=transition_request(request,conversation=resource,target=target,
            expected_revision=request['revision'],now=observed_at,
            resolved_preferences=dict.fromkeys(PREFERENCE_FIELDS) if target is RequestState.NEEDS_CLARIFICATION else None)
    validate_model_invocation_change(invocation,candidate,expected_revision=2,
        request=next_request,previous_request=request,conversation=resource)
    fence=propose_workspace_fence(workspace,workspace_id=ws,expected_revision=workspace['revision'],observed_at=observed_at)
    target=AuditTarget(ResourceType.MODEL_INVOCATION,invocation['invocation_id'],ws,invocation['revision'],
        request['conversation_id'],request['owner_id'],request['request_id'],None)
    audits={}
    for name,action in (('result_audit',WorkflowAction.RECORD_MODEL_RESULT),('accounting_audit',WorkflowAction.RECORD_MODEL_ACCOUNTING)):
        audits[name]=_audit(worker_id=invocation['worker_id'],workspace_id=ws,action=action,target=target,
            mutation=MutationObservation(MutationKind.UPDATE,MutationOutcome.APPLIED,candidate['revision']),
            trace_id=request['trace_id'],at=observed_at,audit_id=f'interpreter-v1:{name}:{invocation["invocation_id"]}:3')
    rows=dict(workspace_pin=workspace,workspace_candidate=fence,membership_pin=membership,
        conversation_pin=conversation,request_pin=request,request_candidate=next_request,
        invocation_pin=invocation,invocation_candidate=candidate)
    encoded={k:encode_row(v,KINDS[k]) for k,v in rows.items()};encoded.update(audits)
    params=[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in encoded.items()]
    params += [bigquery.ScalarQueryParameter('workspace_id','STRING',ws),
        bigquery.ScalarQueryParameter('expires_at','TIMESTAMP',observed_at+timedelta(seconds=30))]
    config=query_config(params)
    # Competing observations for one durable dispatch share a single native slot.
    command=hashlib.sha256(canonical_json_bytes([PROJECT,ws,invocation['invocation_id'],invocation['dispatch_receipt_id']])).hexdigest()
    plan=ControlPlan(actor,ws,command,'interpreter_result_v1_'+command,_sql(),
        json.dumps(config.to_api_repr(),sort_keys=True),'INTERPRETER_RESULT',encoded['invocation_candidate'],
        encoded['result_audit'],'model_invocations')
    if response_text is not None:
        from src.application.campaign_response_transcript import extend_result
        plan=extend_result(plan,request=request,invocation=invocation,observation=observation,
            response_text=response_text,observed_at=observed_at)
    return plan


def validate_outcome(plan,outcome):
    if outcome.native_status is not SharingCommitStatus.COMMITTED:return outcome
    _require(len(outcome.rows)==1,'INTERPRETER_RESULT_UNVERIFIED')
    row=outcome.rows[0];params={p.name:p.value for p in plan.config().query_parameters}
    expected_keys={'state','invocation_row','request_row','result_audit_row','accounting_audit_row'}
    if 'response_candidate' in params:expected_keys.add('response_row')
    _require(set(row)==expected_keys
        and row['state'] in {'APPLIED','REUSED'},'INTERPRETER_RESULT_UNVERIFIED')
    for key,param,table in (('invocation_row','invocation_candidate','model_invocations'),
        ('request_row','request_candidate','requests'),('result_audit_row','result_audit','audit_events'),
        ('accounting_audit_row','accounting_audit','audit_events')):
        _require(decode_row(row[key],table)==decode_row(params[param],table),'INTERPRETER_RESULT_UNVERIFIED')
    if 'response_candidate' in params:
        from src.application.campaign_response_transcript import validate_response_row
        validate_response_row(row['response_row'],params['response_candidate'])
    return outcome


class ProtectedInterpreterOutcomeService:
    def __init__(self,*,client,verifier,access_reader,worker_id,enabled=False):
        from src.application.workflow_contract import _id
        _id(worker_id)
        _require(isinstance(verifier,IapIdentityAdapter) and type(enabled) is bool,'INTERPRETER_RESULT_COMPOSITION')
        self.io=ControlIO(client);self.verifier=verifier;self.access_reader=access_reader
        self.enabled=enabled;self._issued={};self._used=set();self._lock=RLock()
        self.worker_id=worker_id

    def _actor(self,headers,ws,conv):
        actor=self.verifier.authenticate(headers);ensure_fresh(actor)
        _access(self.access_reader,actor,ws,conv)
        return actor

    def prepare(self,headers,**inputs):
        _require(self.enabled,'INTERPRETER_RESULT_DISABLED');require_owned_worker()
        with self._lock:
            _require(len(self._issued)<32,'INTERPRETER_RESULT_CAPACITY')
            _require(inputs['invocation']['worker_id']==self.worker_id,'INTERPRETER_RESULT_WORKER_CHANGED')
            plan=prepare_interpreter_outcome(**inputs)
            request=inputs['request'];actor=self._actor(headers,plan.workspace_id,request['conversation_id'])
            _require(actor.principal.principal_id==plan.actor_id,'INTERPRETER_RESULT_ACTOR_CHANGED')
            self.io.preflight()
            if inputs.get('response_text') is not None:
                from src.application.campaign_response_transcript import SPEC
                from src.ingestion.setup_workflow_tables import validate_table
                from src.application.operation_budget import operation_call
                expected=SPEC.table(PROJECT)
                validate_table(operation_call(self.io.client.get_table,expected.reference,retry=None,timeout=20),expected)
            _require(plan.job_id not in self._issued,'INTERPRETER_ORIGINAL_PLAN_RETAINED')
            self._issued[plan.job_id]=(plan,request['conversation_id'])
            return plan

    def _original(self,headers,plan):
        issued=self._issued.get(plan.job_id)
        _require(issued is not None and issued[0] is plan,'INTERPRETER_ORIGINAL_PLAN_REQUIRED')
        actor=self._actor(headers,plan.workspace_id,issued[1])
        _require(actor.principal.principal_id==plan.actor_id,'INTERPRETER_RESULT_ACTOR_CHANGED')

    def publish(self,headers,plan):
        _require(self.enabled,'INTERPRETER_RESULT_DISABLED');require_owned_worker()
        with self._lock:
            self._original(headers,plan)
            _require(plan.job_id not in self._used,'INTERPRETER_RESULT_REPLAY_DENIED')
            self._used.add(plan.job_id)
            outcome=validate_outcome(plan,self.io.submit_once(plan))
            self._original(headers,plan)
            return outcome

    def recover(self,headers,plan):
        # Read-only original native job; no model call and no new write slot.
        with self._lock:
            self._original(headers,plan)
            outcome=validate_outcome(plan,self.io.recover(plan))
            self._original(headers,plan)
            return outcome
