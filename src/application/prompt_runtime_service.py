"""Default-disabled IAP runtime resolution: BigQuery -> exact Vertex -> BigQuery."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.application.access_policy import Action, AccessDecision, POLICY_VERSION, Reason
from src.application.identity import ensure_fresh
from src.application.operation_budget import check_operation
from src.application.prompt_lifecycle_contract import LifecycleSnapshot
from src.application.prompt_runtime_contract import (
    InvocationPromptBinding, RuntimeBehavior, bind_invocation, render_prompt, validate_runtime_eligibility,
)
from src.application.prompt_runtime_reader import RuntimeReadRequest
from src.application.vertex_prompt_reader import VertexPromptReadRequest, read_exact_vertex_prompt
from src.application.workflow_contract import _require


@dataclass(frozen=True, slots=True)
class ResolvedPrompt:
    binding: InvocationPromptBinding
    capture: LifecycleSnapshot

    def __post_init__(self):
        import json
        _require(type(self.binding) is InvocationPromptBinding and type(self.capture) is LifecycleSnapshot,
                 'INVALID_RESOLVED_PROMPT')
        payload=json.loads(self.binding.binding_canonical_json)
        _require(payload['snapshot']==self.capture.target.head.snapshot.canonical_payload()
                 and payload['eligibility']==validate_runtime_eligibility(self.capture,checked_at=self.binding.bound_at)
                 and self.binding.workspace_id==self.capture.request.workspace_id
                 and self.binding.principal_id==self.capture.request.identity.principal.principal_id,
                 'RESOLVED_PROMPT_BINDING_MISMATCH')

    @property
    def snapshot(self):
        return self.capture.target.head.snapshot

    def render(self, variables):
        return render_prompt(self.snapshot,variables)


class PromptRuntimeService:
    def __init__(self, *, identity_adapter, access_reader, reader, vertex_transport,
                 behaviors: tuple[RuntimeBehavior,...], approved_profiles: frozenset[str], enabled=False):
        _require(type(enabled) is bool and type(behaviors) is tuple and 1<=len(behaviors)<=2
                 and all(type(item) is RuntimeBehavior for item in behaviors)
                 and len({item.task for item in behaviors})==len(behaviors)
                 and type(approved_profiles) is frozenset and 1<=len(approved_profiles)<=32,
                 'INVALID_PROMPT_RUNTIME_SETTINGS')
        self.identity_adapter,self.access_reader,self.reader = identity_adapter,access_reader,reader
        self.vertex_transport,self.behaviors,self.approved_profiles = vertex_transport,behaviors,approved_profiles
        self.enabled=enabled

    def _authorize(self,identity,workspace_id,conversation_id):
        ensure_fresh(identity)
        decisions=[]
        for action,reason,resource in ((Action.GENERATE_DRAFT,Reason.ALLOWED_WORKSPACE_ACTION,None),
                (Action.REFINE_DRAFT,Reason.ALLOWED_OWNER,conversation_id)):
            decision=self.access_reader.authorize(identity,action,workspace_id=workspace_id,
                **({} if resource is None else dict(conversation_id=resource)))
            _require(isinstance(decision,AccessDecision) and decision.allowed
                     and decision.action is action and decision.reason is reason
                     and decision.workspace_id==workspace_id
                     and decision.principal_id==identity.principal.principal_id
                     and decision.policy_version==POLICY_VERSION
                     and type(decision.membership_revision) is int and decision.membership_revision>=1
                     and decision.resource_id==resource
                     and (decision.resource_revision is None if resource is None else
                          type(decision.resource_revision) is int and decision.resource_revision>=1),
                     'PROMPT_RUNTIME_ACCESS_DENIED')
            decisions.append(decision)
        _require(decisions[0].membership_revision==decisions[1].membership_revision,
                 'PROMPT_RUNTIME_ACCESS_CHANGED')
        ensure_fresh(identity);check_operation()
        return decisions[0].membership_revision

    def _read(self,request):
        capture=self.reader.read(request)
        expected=capture.request if type(capture) is LifecycleSnapshot else None
        _require(expected is not None and expected.workspace_id==request.workspace_id
                 and expected.prompt_key==request.prompt_key and expected.task is request.task
                 and expected.identity==request.identity and expected.membership_revision==request.membership_revision,
                 'PROMPT_RUNTIME_READ_SCOPE_MISMATCH')
        now=datetime.now(timezone.utc)
        _require(capture.observed_at<=now<=capture.observed_at+timedelta(seconds=30),
                 'PROMPT_RUNTIME_READ_NOT_FRESH')
        return capture

    def resolve(self,headers,*,workspace_id,conversation_id,request_id,invocation_id,profile):
        _require(self.enabled,'PROMPT_RUNTIME_DISABLED')
        _require(type(profile) is dict,'INVALID_RUNTIME_PROFILE')
        identity=self.identity_adapter.authenticate(headers)
        revision=self._authorize(identity,workspace_id,conversation_id)
        behavior=next((b for b in self.behaviors if b.task.value==profile.get('stage')),None)
        _require(behavior is not None,'PROMPT_RUNTIME_STAGE_NOT_INSTALLED')
        request=RuntimeReadRequest(workspace_id,conversation_id,behavior.prompt_key,behavior.task,identity,revision)
        first=self._read(request)
        # Deny unsupported settings BEFORE calling Vertex. No model is dispatched.
        bind_invocation(first,first.target.head.snapshot,profile,behavior,self.approved_profiles,
            conversation_id=conversation_id,request_id=request_id,invocation_id=invocation_id,
            checked_at=datetime.now(timezone.utc))
        s=first.target.head.snapshot
        exact=read_exact_vertex_prompt(self.vertex_transport,VertexPromptReadRequest(s.prompt_key,s.task,
            s.vertex_prompt_id,s.vertex_version_id,s.output_schema_version,s.safety_policy_version),
            timeout_seconds=10)
        _require(self._authorize(identity,workspace_id,conversation_id)==revision,'PROMPT_RUNTIME_ACCESS_CHANGED')
        fresh=self._read(request)
        # No substitution of a newer version if lifecycle/policy changed during GET.
        _require(first.control==fresh.control and first.target==fresh.target
                 and first.evaluation.control==fresh.evaluation.control
                 and first.evaluation.record==fresh.evaluation.record
                 and first.evaluation_control_anchor==fresh.evaluation_control_anchor,
                 'PROMPT_RUNTIME_SELECTION_CHANGED')
        binding=bind_invocation(fresh,exact,profile,behavior,self.approved_profiles,
            conversation_id=conversation_id,request_id=request_id,invocation_id=invocation_id,
            checked_at=datetime.now(timezone.utc))
        _require(self._authorize(identity,workspace_id,conversation_id)==revision,'PROMPT_RUNTIME_ACCESS_CHANGED')
        return ResolvedPrompt(binding,fresh)

    def revalidate(self,headers,resolved,profile):
        _require(type(resolved) is ResolvedPrompt,'INVALID_RESOLVED_PROMPT')
        b=resolved.binding
        fresh=self.resolve(headers,workspace_id=b.workspace_id,conversation_id=b.conversation_id,
            request_id=b.request_id,invocation_id=b.invocation_id,profile=profile)
        _require(fresh.binding.principal_id==b.principal_id
                 and fresh.binding.binding_canonical_json==b.binding_canonical_json,
                 'PROMPT_RUNTIME_PIN_CHANGED')
        return fresh

    def verify_committed_binding(self,headers,resolved,profile):
        fresh=self.revalidate(headers,resolved,profile)
        b=fresh.binding
        identity=self.identity_adapter.authenticate(headers)
        revision=self._authorize(identity,b.workspace_id,b.conversation_id)
        request=RuntimeReadRequest(b.workspace_id,b.conversation_id,b.prompt_key,
            fresh.snapshot.task,identity,revision)
        row=self.reader.read_binding(request,b.invocation_id)
        now=datetime.now(timezone.utc)
        _require(row['bindings']==[resolved.binding.row()]
                 and row['observed_at']<=now<=row['observed_at']+timedelta(seconds=30),
                 'PROMPT_RUNTIME_STORED_BINDING_MISMATCH')
        _require(self._authorize(identity,b.workspace_id,b.conversation_id)==revision,
                 'PROMPT_RUNTIME_ACCESS_CHANGED')
        return fresh
