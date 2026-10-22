"""Protected chatbot connection to current registered clarification services.

Read-only preparation, no model dispatch or approval. A successful registration
is not execution eligibility; the current quality/policy readers cannot release.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock

from src.application.campaign_clarification_policy import ClarificationPolicyRequest, read_policy
from src.application.campaign_clarification_prompts import definitions
from src.application.campaign_evidence_conversation import read_bound_gaps
from src.application.campaign_quality_product_reader import _access
from src.application.identity import IapIdentityAdapter, ensure_fresh
from src.application.prompt_contract import PromptLifecycle
from src.application.workflow_contract import _id, _require


@dataclass(frozen=True, slots=True)
class EvidenceChatReadiness:
    reason_codes: tuple[str, ...]
    prompt_statuses: tuple[tuple[str, str], ...]

    @property
    def release_allowed(self):
        # Complete per-response quality release is not yet implemented. This
        # readiness result must never become a reusable authorization token.
        return False


class ProtectedEvidenceChatService:
    def __init__(self, *, client, verifier, access_reader, registry_reader,
                 policy_request, targets, clock=lambda: datetime.now(timezone.utc)):
        _require(isinstance(verifier, IapIdentityAdapter)
            and type(policy_request) is ClarificationPolicyRequest
            and type(targets) is tuple and len(targets) == 2
            and all(type(t) is tuple and len(t) == 3 for t in targets),
            'CHAT_EVIDENCE_COMPOSITION_INVALID')
        self.client, self.verifier = client, verifier
        self.access_reader, self.registry_reader = access_reader, registry_reader
        self.policy_request, self.targets, self.clock = policy_request, targets, clock
        self._questions = {}
        self._question_lock = RLock()

    def stage_questions(self, turn):
        """Trusted pipeline staging only; no browser setter or approval effect."""
        from src.application.campaign_question_view import QuestionTurn
        import json
        _require(type(turn) is QuestionTurn,'QUESTION_TURN_UNAVAILABLE')
        request=json.loads(turn.quality.context_canonical_json)['request']
        scope=(request['workspace_id'],request['conversation_id'])
        turn.validate(*scope,self.clock())
        from datetime import timedelta
        _require(turn.expires_at <= turn.quality.report.checked_at+timedelta(seconds=30),
                 'QUESTION_TURN_WINDOW_INVALID')
        with self._question_lock:
            _require(scope in self._questions or len(self._questions)<32,'QUESTION_TURN_CAPACITY')
            self._questions[scope]=turn

    def question_turn(self,headers,*,workspace_id,conversation_id):
        identity,access=self._identity(headers,workspace_id,conversation_id)
        scope=(workspace_id,conversation_id)
        with self._question_lock:
            turn=self._questions.get(scope)
            if turn is None:return None
            if self.clock()>=turn.expires_at:
                del self._questions[scope]
                return None
            turn.validate(*scope,self.clock())
        if not self.check(headers,workspace_id=workspace_id,
                          conversation_id=conversation_id).release_allowed:
            return None
        _,after=self._identity(headers,workspace_id,conversation_id)
        _require(after==access,'QUESTION_TURN_ACCESS_CHANGED')
        return turn

    def intent_adapter(self, headers, *, workspace_id, conversation_id, request_id, transport):
        """Trusted worker composition; assertions are never stored in UI state.

        The returned callback belongs to one admitted invocation. It cannot
        persist a result, approve policy or release user-visible text.
        """
        from src.application.campaign_evidence_dispatch import EvidenceIntentAdapter
        from src.application.campaign_quality_authority_reader import read_execution_authority
        from src.application.campaign_transcript import read_transcript
        import hashlib
        import json
        _id(request_id)

        def refresh():
            identity, access = self._identity(headers, workspace_id, conversation_id)
            args = dict(identity=identity, workspace_id=workspace_id,
                conversation_id=conversation_id, request_id=request_id, clock=self.clock)
            authority = read_execution_authority(self.client, **args)
            texts = read_transcript(self.client, **args)
            policy = read_policy(self.client, self.policy_request, clock=self.clock)
            content = json.loads(authority.content_canonical_json)
            matches = [t for t in texts if hashlib.sha256(t.encode()).hexdigest()
                == content['request_record']['message_sha256']]
            _require(bool(matches), 'CHAT_INTENT_PERSISTED_MESSAGE_REQUIRED')
            _, after = self._identity(headers, workspace_id, conversation_id)
            _require(after == access and self.clock() < min(authority.valid_until,policy.valid_until),
                'CHAT_INTENT_ACCESS_CHANGED')
            return authority, policy, matches[-1]

        return EvidenceIntentAdapter(refresh=refresh,transport=transport,clock=self.clock)

    def _identity(self, headers, workspace_id, conversation_id):
        _id(workspace_id); _id(conversation_id)
        identity = self.verifier.authenticate(headers)
        ensure_fresh(identity)
        access = _access(self.access_reader, identity, workspace_id, conversation_id)
        return identity, access

    def question_adapter(self, headers, *, workspace_id, conversation_id, request_id,
                         observed_interpreter_response, transport):
        """Separate admitted question invocation; parent intent must be durable."""
        from src.application.campaign_evidence_dispatch import EvidenceQuestionDispatch
        def refresh():
            return self.prepare_evidence(headers,workspace_id=workspace_id,
                conversation_id=conversation_id,request_id=request_id,
                observed_interpreter_response=observed_interpreter_response)
        initial = refresh()
        return EvidenceQuestionDispatch(initial,refresh=refresh,transport=transport,clock=self.clock)

    def prepare_evidence_from_result(self, headers, *, workspace_id, conversation_id,
                                    request_id, result):
        """Require an independently persisted successful intent outcome first.

        This does not publish or repair a missing/uncertain invocation outcome.
        """
        from src.application.campaign_evidence_dispatch import EvidenceInvocationDraft
        from src.application.campaign_quality_authority_reader import read_execution_authority
        import hashlib
        import json
        _require(type(result) is EvidenceInvocationDraft
            and hashlib.sha256(result.response_text.encode()).hexdigest()
                == result.observation.response_sha256,
            'CHAT_INTENT_RESULT_REQUIRED')
        identity, access = self._identity(headers,workspace_id,conversation_id)
        args = dict(identity=identity,workspace_id=workspace_id,
            conversation_id=conversation_id,request_id=request_id,clock=self.clock)
        initial = read_execution_authority(self.client,**args)
        content = json.loads(initial.content_canonical_json)
        bindings = [b for b in content['bindings']
            if b['binding_sha256'] == result.binding_sha256 and b['request_id'] == request_id]
        _require(len(bindings) == 1,'CHAT_INTENT_STORED_BINDING_REQUIRED')
        calls = [c for c in content['calls'] if c['invocation_id'] == bindings[0]['invocation_id']]
        _require(len(calls) == 1 and calls[0]['state'] == 'SUCCEEDED'
            and all(calls[0][k] == getattr(result.observation,k) for k in
                ('response_sha256','returned_model_id','finish_reason','safety_outcome',
                 'input_tokens','output_tokens','cached_tokens','thinking_tokens')),
            'CHAT_INTENT_DURABLE_OUTCOME_REQUIRED')
        prepared = self.prepare_evidence(headers,workspace_id=workspace_id,
            conversation_id=conversation_id,request_id=request_id,
            observed_interpreter_response=result.response_text)
        final = read_execution_authority(self.client,**args)
        _, after = self._identity(headers,workspace_id,conversation_id)
        _require(initial.content_sha256 == final.content_sha256
            and initial.native_job_id != final.native_job_id and access == after
            and self.clock() < min(initial.valid_until,final.valid_until,prepared.valid_until),
            'CHAT_INTENT_DURABLE_OUTCOME_CHANGED')
        return prepared

    def check(self, headers, *, workspace_id, conversation_id):
        started = self.clock()
        identity, access = self._identity(headers, workspace_id, conversation_id)
        initial = read_policy(self.client, self.policy_request, clock=self.clock)
        defs = definitions(initial, checked_at=self.clock())
        _require(tuple(t[0] for t in self.targets) == tuple(d.prompt_key for d in defs),
            'CHAT_EVIDENCE_TARGETS_INVALID')
        reasons, statuses, heads = [], [], []
        for definition, (_, pid, version) in zip(defs, self.targets):
            current = self.registry_reader.read_exact(pid, version)
            _require(started <= current.observed_at <= self.clock(), 'CHAT_EVIDENCE_READ_TIME_INVALID')
            definition.verify(current.state.head.snapshot)
            heads.append(current.state.head)
            status = current.state.head.status
            statuses.append((definition.prompt_key, status.value))
            if status is not PromptLifecycle.ACTIVE:
                reasons.append('CHAT_EVIDENCE_PROMPT_NOT_ACTIVE')
        final = read_policy(self.client, self.policy_request, clock=self.clock)
        _require(initial.record_sha256 == final.record_sha256
            and initial.native_job_id != final.native_job_id
            and self.clock() < min(initial.valid_until, final.valid_until),
            'CHAT_EVIDENCE_POLICY_CHANGED')
        # Detect a lifecycle/revocation change during the other reads as well.
        for head, (_, pid, version) in zip(heads, self.targets):
            _require(self.registry_reader.read_exact(pid, version).state.head == head,
                'CHAT_EVIDENCE_PROMPT_CHANGED')
        fresh_identity, after = self._identity(headers, workspace_id, conversation_id)
        _require(fresh_identity.principal == identity.principal and after == access,
            'CHAT_EVIDENCE_ACCESS_CHANGED')
        _require(self.clock() < min(initial.valid_until, final.valid_until),
            'CHAT_EVIDENCE_POLICY_EXPIRED')
        if not (initial.approval_verified and final.approval_verified):
            reasons.append('CHAT_EVIDENCE_POLICY_REVIEW_REQUIRED')
        reasons.append('CHAT_EVIDENCE_QUALITY_RELEASE_NOT_READY')
        return EvidenceChatReadiness(tuple(dict.fromkeys(reasons)), tuple(statuses))

    def prepare_evidence(self, headers, *, workspace_id, conversation_id, request_id,
                         observed_interpreter_response):
        """Server-observed persisted response only, never browser-supplied authority.

        Existing read_bound_gaps rebinds the response to the successful durable
        invocation, exact intent prompt and current scope/source captures.
        Returned plans remain private diagnostics, not user-released questions.
        """
        identity, access = self._identity(headers, workspace_id, conversation_id)
        result = read_bound_gaps(self.client, self.policy_request, identity=identity,
            access_reader=self.access_reader, workspace_id=workspace_id,
            conversation_id=conversation_id, request_id=request_id,
            observed_interpreter_response=observed_interpreter_response, clock=self.clock)
        _, after = self._identity(headers, workspace_id, conversation_id)
        _require(after == access and self.clock() < result.valid_until,
                 'CHAT_EVIDENCE_ACCESS_CHANGED')
        return result
