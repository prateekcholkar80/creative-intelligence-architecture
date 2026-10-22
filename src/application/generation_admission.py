"""4.4 Critical/Must have/shared pilot: fresh admission and one-shot dispatch.

Claims existing authoritative PLANNED calls only. Never creates a request/job,
replays a model, guesses cost or turns recovery into a dispatch permission.
The installed callback is server-only; compose this service inside an owned
operation worker. Durable creation/results belong to 4.6.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from threading import RLock
from uuid import uuid4

from src.application.access_policy import Action, require_authorized
from src.application.audit_contract import (AuditTarget, WorkflowAction, WorkflowObservation,
    MutationKind, MutationObservation, MutationOutcome, ResourceType, validate_audit_event)
from src.application.control_io import ControlIO, ControlPlan, query_config
from src.application.conversation_contract import validate_conversation
from src.application.identity import IapIdentityAdapter, ensure_fresh
from src.application.management_service import digest, parameter, require, table, _insert, _update, _finish
from src.application.membership_contract import validate_membership
from src.application.operation_budget import OperationSettings, check_operation, operation_lock, operation_scope, require_owned_worker
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_codec import decode_row, encode_row, sql_row
from src.application.workflow_contract import _id, validate_model_invocation, validate_model_invocation_change
from src.application.workspace_contract import validate_workspace, propose_workspace_fence
from src.ingestion.setup_workflow_tables import PROJECT


@dataclass(frozen=True, slots=True)
class AdmissionSettings:
    # Full profile hashes, not just model IDs or untrusted browser choices.
    profile_sha256: frozenset[str] = frozenset()
    enabled: bool = False
    operation: OperationSettings = OperationSettings()
    require_registered_prompts: bool = False

    def __post_init__(self):
        import re
        require(type(self.enabled) is bool and type(self.require_registered_prompts) is bool
            and type(self.operation) is OperationSettings
            and type(self.profile_sha256) is frozenset and len(self.profile_sha256)<=32
            and all(type(pin) is str and re.fullmatch(r'[0-9a-f]{64}',pin) for pin in self.profile_sha256))


def snapshot_sql():
    projections=[]
    for name,key in (('workspaces',None),('memberships','principal_id'),('conversations','conversation_id'),
                     ('requests','request_id'),('jobs','job_id'),('job_attempts','attempt_id'),
                     ('model_invocations','invocation_id')):
        partition={'requests':'created_at','job_attempts':'started_at','model_invocations':'started_at'}.get(name)
        where='workspace_id=@workspace_id'
        if key: where+=f' AND {key}=@{key}'
        if partition:where+=f' AND {partition} BETWEEN @start_at AND @end_at'
        projections.append(f'ARRAY(SELECT TO_JSON_STRING(t) FROM {table(name)} t WHERE {where} LIMIT 2) AS {name}')
    return 'BEGIN TRANSACTION;\nCREATE TEMP TABLE snapshot AS SELECT CURRENT_TIMESTAMP() AS observed_at,\n'+',\n'.join(projections)+';\nCOMMIT TRANSACTION;\nSELECT * FROM snapshot;'


class ProtectedGenerationAdmission:
    def __init__(self, *, client, verifier, worker_id, settings=AdmissionSettings(), model_call=None,
                 prompt_runtime=None, planner_input_builder=None):
        require(isinstance(verifier,IapIdentityAdapter) and type(settings) is AdmissionSettings
            and (model_call is None or callable(model_call)))
        _id(worker_id)
        self.io=ControlIO(client,result_timeout_seconds=90 if settings.require_registered_prompts else 45)
        self.verifier=verifier;self.worker_id=worker_id
        self.settings=settings;self._call=model_call
        from src.application.prompt_runtime_service import PromptRuntimeService
        require((prompt_runtime is None or isinstance(prompt_runtime,PromptRuntimeService))
            and (prompt_runtime is None)==(not settings.require_registered_prompts),
            'PROMPT_RUNTIME_COMPOSITION_REQUIRED')
        require(planner_input_builder is None or (callable(planner_input_builder) and prompt_runtime is not None),
            'PLANNER_INPUT_COMPOSITION_REQUIRED')
        self._input_builder=planner_input_builder;self._inputs={}
        self._prompt_runtime=prompt_runtime;self._resolved={}
        # A new service incarnation cannot reproduce a predecessor's claim.
        self._incarnation=uuid4().hex;self._process_id=os.getpid()
        self._lock=RLock();self._issued={};self._used=set()

    def _actor(self,headers,write=False):
        if write:
            require(self.settings.enabled,'GENERATION_DISABLED')
            require(os.getpid()==self._process_id,'DISPATCH_PROCESS_CHANGED')
        actor=self.verifier.authenticate(headers);ensure_fresh(actor);return actor

    def _snapshot(self,actor,params,*,recovery=False):
        source=self.io.read(snapshot_sql(),params)
        require(set(source)=={'observed_at','workspaces','memberships','conversations','requests','jobs','job_attempts','model_invocations'})
        rows={}
        for name in source.keys()-{'observed_at'}:
            require(type(source[name]) is list and len(source[name])<=1,'GENERATION_SOURCE_NOT_UNIQUE')
            rows[name]=decode_row(source[name][0],name) if source[name] else None
        require(all(rows[name] is not None for name in ('workspaces','memberships','conversations','requests','model_invocations')))
        ws,member,conv,req,inv=(rows[name] for name in ('workspaces','memberships','conversations','requests','model_invocations'))
        values={p.name:p.value for p in params}
        validate_workspace(ws,workspace_id=values['workspace_id']);require(ws['status']=='ACTIVE')
        membership=validate_membership(member,workspace_id=ws['workspace_id'],principal_id=actor.principal.principal_id)
        require(member['identity_issuer']==actor.issuer and member['identity_subject']==actor.subject)
        resource=validate_conversation(conv,workspace_id=ws['workspace_id'],conversation_id=values['conversation_id'])
        require(conv['status']=='ACTIVE' and conv['owner_id']==actor.principal.principal_id)
        require_authorized(actor.principal,membership,Action.GENERATE_DRAFT,workspace_id=ws['workspace_id'])
        require_authorized(actor.principal,membership,Action.REFINE_DRAFT,workspace_id=ws['workspace_id'],resource=resource)
        require(req['request_id']==values['request_id'] and inv['invocation_id']==values['invocation_id'])
        validate_model_invocation(inv,request=req,conversation=resource,job=rows['jobs'],attempt=rows['job_attempts'])
        if not recovery:require(digest(inv['profile']) in self.settings.profile_sha256,'MODEL_PROFILE_NOT_ADMITTED')
        at=source['observed_at']
        require(isinstance(at,datetime) and at.tzinfo is not None and req['created_at']<=at
            and values['start_at']<=req['created_at']<=inv['started_at']<=at
            and (recovery or at<values['end_at'])
            and ws['updated_at']<=at and conv['updated_at']<=at and member['updated_at']<=at)
        if rows['jobs'] is not None:
            require(recovery or at<rows['jobs']['deadline_at'] and rows['job_attempts']['worker_id']==self.worker_id,
                'GENERATION_ATTEMPT_NOT_ADMITTED')
        ensure_fresh(actor);check_operation()
        return source,rows,resource

    def prepare(self,headers,*,workspace_id,conversation_id,request_id,invocation_id,
                start_at,end_at,job_id='unused',attempt_id='unused'):
        for value in (workspace_id,conversation_id,request_id,invocation_id,job_id,attempt_id):_id(value)
        require(isinstance(start_at,datetime) and start_at.tzinfo is not None and isinstance(end_at,datetime)
            and end_at.tzinfo is not None and timedelta(0)<end_at-start_at<=timedelta(hours=24),'INVALID_ADMISSION_WINDOW')
        with operation_scope(self.settings.operation),operation_lock(self._lock):
            actor=self._actor(headers,True)
            self.io.preflight()
            params=[parameter(name,value) for name,value in dict(workspace_id=workspace_id,conversation_id=conversation_id,
                request_id=request_id,invocation_id=invocation_id,job_id=job_id,attempt_id=attempt_id,
                principal_id=actor.principal.principal_id).items()]
            params += [parameter('start_at',start_at,'TIMESTAMP'),parameter('end_at',end_at,'TIMESTAMP')]
            source,rows,resource=self._snapshot(actor,params)
            inv=rows['model_invocations'];require(inv['state']=='PLANNED','INVOCATION_ALREADY_CLAIMED')
            resolved=None
            if self._prompt_runtime is not None:
                from src.ingestion.setup_prompt_runtime_tables import inspect
                require(inspect(self.io.client)['status']=='VERIFIED','PROMPT_BINDING_RESOURCES_MISSING')
                resolved=self._prompt_runtime.resolve(headers,workspace_id=workspace_id,
                    conversation_id=conversation_id,request_id=request_id,invocation_id=invocation_id,
                    profile=dict(inv['profile']))
                # Verify the exact provider-independent binding before native claims.
                from src.application.prompt_runtime_service import ResolvedPrompt
                require(type(resolved) is ResolvedPrompt and resolved.binding.workspace_id==workspace_id
                    and resolved.binding.conversation_id==conversation_id
                    and resolved.binding.request_id==request_id and resolved.binding.invocation_id==invocation_id
                    and resolved.binding.principal_id==actor.principal.principal_id,
                    'PROMPT_RUNTIME_BINDING_SCOPE_MISMATCH')
            native_id='model_dispatch_v1_'+digest([PROJECT,workspace_id,invocation_id])
            require(native_id not in self._issued,'DISPATCH_INTENT_ALREADY_PREPARED')
            at=source['observed_at'];receipt='dispatch-'+digest([self._incarnation,workspace_id,invocation_id])
            candidate=inv|dict(state='DISPATCHED',revision=inv['revision']+1,dispatch_receipt_id=receipt,worker_id=self.worker_id)
            attempt=rows['job_attempts']
            proposed_attempt=None if attempt is None else attempt|dict(dispatch_receipt_id=receipt,revision=attempt['revision']+1)
            validate_model_invocation_change(inv,candidate,expected_revision=inv['revision'],request=rows['requests'],
                conversation=resource,job=rows['jobs'],attempt=proposed_attempt,previous_attempt=attempt)
            fence=propose_workspace_fence(rows['workspaces'],workspace_id=workspace_id,
                expected_revision=rows['workspaces']['revision'],observed_at=at)
            target=AuditTarget(ResourceType.MODEL_INVOCATION,invocation_id,workspace_id,inv['revision'],
                conversation_id,inv['owner_id'],request_id,inv['job_id'])
            observation=WorkflowObservation(self.worker_id,workspace_id,WorkflowAction.CLAIM_MODEL_DISPATCH)
            audit=dict(schema_version=inv['schema_version'],workspace_id=workspace_id,audit_id='dispatch-audit-'+digest([receipt]),
                actor_id=self.worker_id,actor_kind='SERVICE',action=observation.action.value,allowed=True,
                reason_code=observation.reason.value,policy_version=observation.policy_version,
                resource_type=target.resource_type.value,resource_id=invocation_id,resource_revision=inv['revision'],
                conversation_id=conversation_id,owner_id=inv['owner_id'],request_id=request_id,job_id=inv['job_id'],
                trace_id=inv['trace_id'],membership_revision=None,mutation_outcome='APPLIED',new_revision=candidate['revision'],occurred_at=at)
            validate_audit_event(audit,observation=observation,target=target,mutation=MutationObservation(
                MutationKind.UPDATE,MutationOutcome.APPLIED,candidate['revision']),observed_at=at,recorded_at=at,trace_id=inv['trace_id'])
            for name in ('workspaces','memberships','conversations','requests','model_invocations'):
                params.append(parameter(name+'_pin',source[name][0]))
            params += [parameter('candidate',encode_row(candidate,'model_invocations')),parameter('workspace',encode_row(fence,'workspaces')),
                parameter('audit',encode_row(audit,'audit_events')),parameter('actor_subject',actor.subject),parameter('actor_issuer',actor.issuer),
                parameter('assertion_expires_at',datetime.fromtimestamp(actor.expires_at+30,timezone.utc),'TIMESTAMP')]
            if attempt is not None:
                params += [parameter('jobs_pin',source['jobs'][0]),parameter('job_attempts_pin',source['job_attempts'][0]),
                    parameter('attempt',encode_row(proposed_attempt,'job_attempts'))]
            script=dispatch_sql(attempt is not None)
            if resolved is not None:
                from src.application.prompt_runtime_query import parameters,claim_sql
                params += parameters(resolved);script=claim_sql(script)
                if self._input_builder is not None:
                    from src.application.campaign_dispatch_inputs import parameter as input_parameter,claim_sql as input_sql
                    from src.ingestion.setup_campaign_dispatch_inputs import inspect as inspect_inputs
                    require(inspect_inputs(self.io.client)['status']=='VERIFIED','INPUT_MANIFEST_RESOURCES_MISSING')
                    require(inv['stage']=='PLANNER','PLANNER_INPUT_STAGE')
                    prepared=self._input_builder(resolved,rows['requests'])
                    params.append(input_parameter(prepared,resolved.binding));script=input_sql(script)
            if resolved is None:
                config=query_config(params)
            else:
                from src.application.prompt_runtime_query import dispatch_config
                config=dispatch_config(params)
            plan=ControlPlan(actor.principal.principal_id,workspace_id,invocation_id,native_id,script,
                json.dumps(config.to_api_repr(),sort_keys=True),'DISPATCH',encode_row(candidate,'model_invocations'),
                encode_row(audit,'audit_events'),'model_invocations')
            require(len(self._issued)<1024,'GENERATION_ADMISSION_FULL');self._issued[native_id]=plan
            if resolved is not None:self._resolved[native_id]=resolved
            if self._input_builder is not None:self._inputs[native_id]=prepared
            return plan

    def _current(self,actor,plan,*,recovery=False):
        require(type(plan) is ControlPlan and plan.kind=='DISPATCH' and plan.candidate_table=='model_invocations'
            and plan.actor_id==actor.principal.principal_id)
        candidate=decode_row(plan.candidate_json,'model_invocations')
        params=plan.config().query_parameters;values={p.name:p.value for p in params}
        script=dispatch_sql(candidate['stage']=='PLANNER')
        registered='runtime_binding' in values
        require(registered==self.settings.require_registered_prompts,'PROMPT_RUNTIME_COMPOSITION_REQUIRED')
        if registered:
            from src.application.prompt_runtime_query import binding_from_parameters,claim_sql
            binding=binding_from_parameters(params);script=claim_sql(script)
            require(('planner_input_manifest' in values)==(self._input_builder is not None),
                'PLANNER_INPUT_COMPOSITION_REQUIRED')
            if self._input_builder is not None:
                from src.application.campaign_dispatch_inputs import parameter as input_parameter,claim_sql as input_sql
                prepared=self._inputs.get(plan.job_id)
                require(input_parameter(prepared,binding).value==values['planner_input_manifest'],'PLANNER_INPUT_REBOUND')
                script=input_sql(script)
            require(json.loads(binding.binding_canonical_json)['profile']==candidate['profile']
                and binding.stage==candidate['stage'],'PROMPT_RUNTIME_PROFILE_REBOUND')
        if registered:
            from src.application.prompt_runtime_query import dispatch_config
            expected_config=dispatch_config(params)
        else:
            expected_config=query_config(params)
        require(len(params)==len(values) and plan.sql==script
            and plan.job_id=='model_dispatch_v1_'+digest([PROJECT,plan.workspace_id,plan.command_id])
            and candidate['invocation_id']==plan.command_id and candidate['workspace_id']==plan.workspace_id
            and values.get('workspace_id')==plan.workspace_id and values.get('invocation_id')==plan.command_id
            and values.get('principal_id')==plan.actor_id and values.get('candidate')==plan.candidate_json
            and values.get('audit')==plan.audit_json and values.get('actor_subject')==actor.subject
            and values.get('actor_issuer')==actor.issuer
            and plan.config().to_api_repr()==expected_config.to_api_repr(),'INVALID_DISPATCH_PLAN')
        names={'workspace_id','conversation_id','request_id','invocation_id','job_id','attempt_id','principal_id','start_at','end_at'}
        return self._snapshot(actor,[p for p in params if p.name in names],recovery=recovery)

    def _validate(self,plan,outcome):
        if outcome.native_status is SharingCommitStatus.COMMITTED and outcome.rows:
            row=outcome.rows[0]
            require(set(row)=={'state','candidate','audit'} and row['state']=='APPLIED'
                and decode_row(row['candidate'],'model_invocations')==decode_row(plan.candidate_json,'model_invocations')
                and decode_row(row['audit'],'audit_events')==decode_row(plan.audit_json,'audit_events'),'DISPATCH_RESULT_INVALID')
        return outcome

    def claim(self,headers,plan):
        """Native claim result only; never a transferable model-call permission."""
        with operation_scope(self.settings.operation),operation_lock(self._lock):
            actor=self._actor(headers,True);self._current(actor,plan)
            self.io.preflight()
            require(self._issued.get(plan.job_id) is plan,'DISPATCH_PLAN_NOT_ISSUED')
            if plan.job_id in self._used:return self.recover(headers,plan)
            if self._prompt_runtime is not None:
                resolved=self._resolved[plan.job_id]
                self._prompt_runtime.revalidate(headers,resolved,
                    decode_row(plan.candidate_json,'model_invocations')['profile'])
            self._used.add(plan.job_id)  # Consume BEFORE uncertain provider I/O.
            result=self._validate(plan,self.io.submit_once(plan));check_operation()
            self._current(self._actor(headers),plan)
            return result

    def dispatch(self,headers,plan):
        """Only this first local entry may call the installed trusted adapter once."""
        with operation_scope(self.settings.operation),operation_lock(self._lock):
            require_owned_worker()
            require(self._call is not None,'MODEL_ADAPTER_NOT_INSTALLED')
            require(plan.job_id not in self._used,'MODEL_REPLAY_DENIED')
            result=self.claim(headers,plan)
            require(result.submitted and result.native_status is SharingCommitStatus.COMMITTED
                and len(result.rows)==1,'MODEL_DISPATCH_UNPROVEN')
            source,rows,_=self._current(self._actor(headers),plan)
            require(rows['model_invocations']==decode_row(plan.candidate_json,'model_invocations'),'DISPATCH_CLAIM_CHANGED')
            check_operation()
            # No retry loop and no model invocation from recover(). Ambiguous
            # provider failure leaves DISPATCHED for 4.6 reconciliation.
            profile=dict(rows['model_invocations']['profile'])
            if self._prompt_runtime is None:
                value=self._call(profile)
            else:
                resolved=self._resolved[plan.job_id]
                self._prompt_runtime.verify_committed_binding(headers,resolved,profile)
                check_operation()
                if self._input_builder is None:
                    value=self._call(profile,resolved)
                else:
                    from src.application.campaign_dispatch_inputs import verify_stored
                    prepared=self._inputs[plan.job_id]
                    verify_stored(self.io,prepared,resolved.binding)
                    value=self._call(profile,resolved,prepared)
                    verify_stored(self.io,prepared,resolved.binding)
                self._prompt_runtime.verify_committed_binding(headers,resolved,profile)
            check_operation();self._current(self._actor(headers),plan)
            return value

    def recover(self,headers,plan):
        with operation_scope(self.settings.operation),operation_lock(self._lock):
            actor=self._actor(headers);self._current(actor,plan,recovery=True)
            result=self._validate(plan,self.io.recover(plan));check_operation()
            self._current(self._actor(headers),plan,recovery=True)
            return result


def dispatch_sql(planner):
    pins=[]
    for name,key in (('workspaces',None),('memberships','principal_id'),('conversations','conversation_id'),
                     ('requests','request_id'),('model_invocations','invocation_id')):
        partition={'requests':'created_at','model_invocations':'started_at'}.get(name)
        where='workspace_id=@workspace_id'+(f' AND {key}=@{key}' if key else '')
        if partition:where+=f' AND {partition} BETWEEN @start_at AND @end_at'
        pins.append(f"ASSERT (SELECT TO_JSON_STRING(t) FROM {table(name)} t WHERE {where})=@{name}_pin AS 'DISPATCH_CONFLICT';")
    parent=''
    if planner:
        parent=f'''ASSERT (SELECT TO_JSON_STRING(t) FROM {table('jobs')} t WHERE workspace_id=@workspace_id AND job_id=@job_id)=@jobs_pin AS 'DISPATCH_PARENT_CONFLICT';
ASSERT (SELECT TO_JSON_STRING(t) FROM {table('job_attempts')} t WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
  AND started_at BETWEEN @start_at AND @end_at)=@job_attempts_pin AS 'DISPATCH_PARENT_CONFLICT';
ASSERT (SELECT COUNT(*) FROM {table('jobs')} WHERE workspace_id=@workspace_id AND job_id=@job_id
  AND state='RUNNING' AND CURRENT_TIMESTAMP()<deadline_at)=1 AS 'DISPATCH_DEADLINE';'''
    attempt='' if not planner else _update('job_attempts','attempt','attempt_id').replace(
        'AND t.attempt_id=r.attempt_id','AND t.attempt_id=r.attempt_id AND t.started_at BETWEEN @start_at AND @end_at')
    invocation=_update('model_invocations','candidate','invocation_id').replace(
        'AND t.invocation_id=r.invocation_id','AND t.invocation_id=r.invocation_id AND t.started_at BETWEEN @start_at AND @end_at')
    return f'''CREATE TEMP TABLE result(state STRING,candidate STRING,audit STRING);
BEGIN
BEGIN TRANSACTION;
ASSERT CURRENT_TIMESTAMP()<@assertion_expires_at AS 'DISPATCH_IDENTITY_EXPIRED';
{chr(10).join(pins)}
ASSERT (SELECT COUNT(*) FROM {table('workspaces')} WHERE workspace_id=@workspace_id AND status='ACTIVE')=1 AS 'DISPATCH_WORKSPACE_DENIED';
ASSERT (SELECT COUNT(*) FROM {table('memberships')} WHERE workspace_id=@workspace_id AND principal_id=@principal_id
  AND identity_issuer=@actor_issuer AND identity_subject=@actor_subject AND status='ACTIVE' AND 'CREATOR' IN UNNEST(roles))=1 AS 'DISPATCH_MEMBER_DENIED';
ASSERT (SELECT COUNT(*) FROM {table('conversations')} WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
  AND owner_id=@principal_id AND status='ACTIVE')=1 AS 'DISPATCH_OWNER_DENIED';
ASSERT (SELECT COUNT(*) FROM {table('model_invocations')} WHERE workspace_id=@workspace_id AND request_id=@request_id
  AND stage=JSON_VALUE(PARSE_JSON(@candidate),'$.stage')
  AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=1 AS 'DISPATCH_STAGE_DUPLICATE';
ASSERT (SELECT COUNT(*) FROM {table('requests')} WHERE workspace_id=@workspace_id
  AND principal_id=@principal_id AND conversation_id=@conversation_id
  AND (idempotency_key_sha256=JSON_VALUE(PARSE_JSON(@requests_pin),'$.idempotency_key_sha256')
    OR generation_intent_id=JSON_VALUE(PARSE_JSON(@requests_pin),'$.generation_intent_id'))
  AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=1 AS 'DISPATCH_INTENT_DUPLICATE';
ASSERT (SELECT COUNT(*) FROM {table('model_invocations')} WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
  AND state='PLANNED' AND dispatch_receipt_id IS NULL AND started_at BETWEEN @start_at AND @end_at)=1 AS 'DISPATCH_ALREADY_CLAIMED';
{parent}
{_update('workspaces','workspace','workspace_id')}
-- PROBE_AFTER_FENCE
{attempt}
{invocation}
{_insert('audit_events','audit')}
INSERT INTO result SELECT 'APPLIED',TO_JSON_STRING({sql_row('candidate','model_invocations')}),TO_JSON_STRING({sql_row('audit','audit_events')});
{_finish()}'''
