"""Fresh gap refresh across exactly verified child dispatch changes."""
from dataclasses import replace
import json
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_quality_authority import validate_payload
from src.application.campaign_evidence_conversation import BoundGapRead
from src.application.campaign_question_context import QuestionContextRead,QuestionContextTransition,verify_transition
from src.application.operation_budget import require_owned_worker
from src.application.workflow_contract import _require,_digest


class QuestionContextGuard:
    """Server-owned adapter refresh, not a browser override or approval grant.

    Default question adapters still compare the full context unchanged. Only this
    guard may preserve the original gap context across a separately verified exact
    child transition; evidence, policy, access and every other captured row remain
    current and unchanged. Pending child rows never substitute for raw history.
    """
    def __init__(self,*,initial,admitted,admission_proof,dispatch_plan,read_context,read_gaps,clock):
        _require(type(initial) is BoundGapRead and type(admitted) is QuestionContextRead
            and type(admission_proof) is QuestionContextTransition
            and admission_proof.phase=='ADMITTED' and admission_proof.after_sha256==admitted.sha256
            and callable(read_context) and callable(read_gaps) and callable(clock),
            'QUESTION_CONTEXT_GUARD_COMPOSITION')
        _digest(initial.evidence_context_sha256)
        parameters={p.name:p.value for p in dispatch_plan.config().query_parameters}
        call=json.loads(parameters['candidate'])
        authorities=[json.loads(v) for v in admitted.records()['campaign_request_authorities']
            if json.loads(v)['request_id']==call['request_id']]
        _require(len(authorities)==1,'QUESTION_CONTEXT_CHILD_AUTHORITY')
        authority=validate_payload(json.loads(authorities[0]['payload_canonical_json']))
        parent=authority.get('question_parent',{})
        _require(parent.get('gap_context_sha256')==initial.context_sha256
            and parent.get('gap_plan_sha256')==initial.plan.sha256
            and parent.get('question_profile_sha256')==_hash(call['profile'])
            and authority['principal_id']==initial.principal_id==admitted.principal_id,
            'QUESTION_CONTEXT_GUARD_BINDING_CHANGED')
        self.initial,self.admitted,self.proof=initial,admitted,admission_proof
        self.dispatch_plan,self.read_context,self.read_gaps,self.clock=dispatch_plan,read_context,read_gaps,clock

    def __call__(self):
        require_owned_worker()
        first=self.read_context()
        verify_transition(self.admitted,first,self.dispatch_plan,phase='DISPATCHED',checked_at=self.clock())
        fresh=self.read_gaps()
        final=self.read_context()
        verify_transition(self.admitted,final,self.dispatch_plan,phase='DISPATCHED',checked_at=self.clock())
        _require(first.read_id!=final.read_id and first.sha256==final.sha256
            and type(fresh) is BoundGapRead and fresh.evidence_context_sha256==self.initial.evidence_context_sha256
            and fresh.principal_id==self.initial.principal_id
            and fresh.policy_approval_verified==self.initial.policy_approval_verified
            and self.clock()<min(self.initial.valid_until,fresh.valid_until,first.valid_until,final.valid_until),
            'QUESTION_CONTEXT_EVIDENCE_CHANGED')
        # Preserve the originally pinned prompt plan only after exact raw-record
        # transition and unchanged independently hashed evidence are established.
        return replace(fresh,plan=self.initial.plan,context_sha256=self.initial.context_sha256,
            valid_until=min(self.initial.valid_until,fresh.valid_until,first.valid_until,final.valid_until))
