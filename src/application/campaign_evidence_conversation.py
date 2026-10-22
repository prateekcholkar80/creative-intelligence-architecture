"""Protected opt-in clarification preparation and one-shot admitted model adapter.

No request creation, approvals, public response release or automatic model retries.
Install the question adapter only as a registered generation-admission callback.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib,json
from threading import Lock
from src.application.campaign_evidence_gaps import plan_gaps,validate_questions,canonical
from src.application.campaign_evidence_intent import bind_evidence_intent
from src.application.campaign_clarification_policy import ClarificationPolicyRequest,read_policy
from src.application.campaign_evidence_coverage import read_coverage
from src.application.campaign_quality_authority_reader import read_execution_authority
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.campaign_planner_projection import parse_json
from src.application.prompt_runtime_service import ResolvedPrompt
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require
from src.common.gemini_generation import complete_response_text

QUESTION_KEY='campaign-evidence-questions'
QUESTION_SCHEMA='campaign-evidence-questions-v1'

@dataclass(frozen=True,slots=True)
class BoundGapRead:
    plan: object
    context_sha256: str
    principal_id: str
    valid_until: datetime
    policy_approval_verified: bool=False
    evidence_context_sha256: str | None=None


def read_bound_gaps(client,policy_request,*,identity,access_reader,workspace_id,conversation_id,request_id,
        observed_interpreter_response,clock=lambda:datetime.now(timezone.utc)):
    _require(type(policy_request) is ClarificationPolicyRequest,'GAP_STORED_POLICY_REQUIRED')
    args=dict(identity=identity,workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id,clock=clock)
    first=read_execution_authority(client,**args)
    initial_policy=read_policy(client,policy_request,clock=clock)
    policy_json=initial_policy.policy_canonical_json
    intent=bind_evidence_intent(first,observed_interpreter_response,policy_json,checked_at=clock())
    scope=json.loads(intent.scope_json)
    read=read_coverage(client,policy_json,scope=scope,features=intent.features,formula_version=intent.formula_version,
        audience=intent.audience,identity=identity,access_reader=access_reader,clock=clock)
    final_policy=read_policy(client,policy_request,clock=clock)
    final=read_execution_authority(client,**args)
    _require(initial_policy.record_sha256==final_policy.record_sha256
        and initial_policy.native_job_id!=final_policy.native_job_id,'GAP_POLICY_CONTEXT_CHANGED')
    _require(first.content_sha256==final.content_sha256 and first.native_job_id!=final.native_job_id
        and _utc(clock())<min(first.valid_until,final.valid_until,read.valid_until,initial_policy.valid_until,final_policy.valid_until),'GAP_AUTHORITY_CONTEXT_CHANGED')
    plan=plan_gaps(policy_json,scope=scope,features=intent.features,captures=read.captures,checked_at=clock())
    value=json.loads(plan.canonical_json)
    evidence=dict(policy_record_sha256=initial_policy.record_sha256,policy_json=policy_json,source_sha256=read.source_sha256,
        scope=scope,features=list(intent.features),outcomes=value['outcomes'],questions=value['questions'])
    context=_hash(dict(evidence,authority_sha256=first.content_sha256))
    return BoundGapRead(plan,context,json.loads(first.content_canonical_json)['authority']['principal_id'],
        min(first.valid_until,final.valid_until,read.valid_until,initial_policy.valid_until,final_policy.valid_until),
        initial_policy.approval_verified and final_policy.approval_verified,_hash(evidence))


@dataclass(frozen=True,slots=True)
class QuestionDraft:
    questions_canonical_json: str
    plan_sha256: str
    context_sha256: str
    response_sha256: str
    binding_sha256: str

    @property
    def release_allowed(self):return False


class EvidenceQuestionAdapter:
    """Server callback for ProtectedGenerationAdmission; transport is server-owned.

    A separate INTERPRETER request must carry the registered question prompt.
    The parent plan/hash is rendered into its exact bound template. Dynamic schema
    narrowing constrains this response to the pinned missing fields. Full question
    quality gates and durable outcome publication still precede public release.
    """
    def __init__(self,initial,*,refresh,transport,clock=lambda:datetime.now(timezone.utc)):
        _require(type(initial) is BoundGapRead and callable(refresh) and callable(transport),'GAP_QUESTION_COMPOSITION')
        self.initial,self.refresh,self.transport,self.clock=initial,refresh,transport,clock
        self.used=False;self._lock=Lock()

    def _fresh(self):
        fresh=self.refresh();now=_utc(self.clock())
        _require(type(fresh) is BoundGapRead and fresh.context_sha256==self.initial.context_sha256
            and fresh.principal_id==self.initial.principal_id
            and fresh.policy_approval_verified==self.initial.policy_approval_verified and now<min(fresh.valid_until,self.initial.valid_until),
            'GAP_QUESTION_CONTEXT_CHANGED')
        return fresh

    def __call__(self,profile,resolved):
        require_owned_worker();check_operation()
        with self._lock:
            _require(not self.used,'GAP_QUESTION_REPLAY_DENIED');self.used=True
        plan=self.initial.plan;data=json.loads(plan.canonical_json)
        _require(data['policy_status']=='APPROVED' and self.initial.policy_approval_verified,
            'GAP_QUESTION_POLICY_NOT_APPROVED')
        request=plan.question_request();scope=data['scope']
        _require(type(resolved) is ResolvedPrompt and resolved.binding.prompt_key==QUESTION_KEY
            and resolved.binding.stage=='INTERPRETER' and resolved.snapshot.output_schema_version==QUESTION_SCHEMA
            and resolved.snapshot.required_variables==('evidence_gap_plan_json',)
            and all(getattr(resolved.binding,k)==scope[k] for k in ('workspace_id','conversation_id'))
            and resolved.binding.principal_id==self.initial.principal_id
            and resolved.binding.request_id!=scope['request_id']
            and json.loads(resolved.binding.binding_canonical_json)['profile']==profile,
            'GAP_QUESTION_PROMPT_REQUIRED')
        self._fresh()
        system,prompt=resolved.render(dict(evidence_gap_plan_json=canonical(request)))
        check_operation()
        response=self.transport(profile,system,prompt,request['response_schema'])
        raw=complete_response_text(response)
        value=validate_questions(plan,parse_json(raw))
        self._fresh();check_operation()
        return QuestionDraft(value,plan.sha256,self.initial.context_sha256,hashlib.sha256(raw.encode()).hexdigest(),
            resolved.binding.binding_sha256)
