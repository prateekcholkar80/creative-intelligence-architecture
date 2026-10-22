"""One-shot admitted intent dispatch and server-held provider observations.

No retry, durable outcome writer, business approval or public response release.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from threading import Lock

from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_clarification_prompts import definitions, render_intent, INTENT_KEY
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_planner_projection import parse_json
from src.application.operation_budget import require_owned_worker, check_operation
from src.application.prompt_runtime_service import ResolvedPrompt
from src.application.workflow_contract import _require
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.common.gemini_generation import complete_response_text


@dataclass(frozen=True, slots=True)
class EvidenceInvocationDraft:
    response_text: str
    output_canonical_json: str
    context_sha256: str
    binding_sha256: str
    observation: ModelResultObservation

    @property
    def release_allowed(self): return False


def provider_observation(response, raw):
    usage = getattr(response, 'usage_metadata', None)
    counts = {}
    for target, source in (('input_tokens','prompt_token_count'),
            ('output_tokens','candidates_token_count'),('cached_tokens','cached_content_token_count'),
            ('thinking_tokens','thoughts_token_count')):
        value = getattr(usage, source, None) if usage is not None else None
        _require(value is None or type(value) is int and 0 <= value < 2**63,
                 'CHAT_INTENT_USAGE_INVALID')
        counts[target] = value
    returned = getattr(response, 'model_version', None)
    _require(returned is None or type(returned) is str and 0 < len(returned) <= 256,
             'CHAT_INTENT_MODEL_METADATA_INVALID')
    # Only after complete_response_text verified STOP and safety. Unknown usage
    # and returned identity remain unknown; estimates are never invented.
    return ModelResultObservation('SUCCEEDED', returned_model_id=returned,
        response_sha256=hashlib.sha256(raw.encode()).hexdigest(),
        finish_reason='STOP', safety_outcome='PASSED', **counts)


class EvidenceIntentAdapter:
    """Install only as ProtectedGenerationAdmission's registered model callback.

    refresh() captures authoritative request/policy and persisted message inside
    the owned worker after the durable claim, not browser supplied local history.
    """
    def __init__(self, *, refresh, transport, clock=lambda: datetime.now(timezone.utc)):
        _require(callable(refresh) and callable(transport),'CHAT_INTENT_COMPOSITION_INVALID')
        self.refresh, self.transport, self.clock = refresh, transport, clock
        self._lock, self._used = Lock(), False

    def __call__(self, profile, resolved):
        require_owned_worker(); check_operation()
        with self._lock:
            _require(not self._used, 'CHAT_INTENT_REPLAY_DENIED'); self._used = True
        authority, policy, text = self.refresh()
        definition = definitions(policy, checked_at=self.clock())[0]
        data = json.loads(authority.content_canonical_json)
        selection, request = data['authority'], data['request_record']
        _require(type(resolved) is ResolvedPrompt and resolved.binding.prompt_key == INTENT_KEY
            and resolved.binding.stage == 'INTERPRETER'
            and all(getattr(resolved.binding,k) == request[k] for k in
                ('workspace_id','conversation_id','request_id','principal_id'))
            and json.loads(resolved.binding.binding_canonical_json)['profile'] == profile,
            'CHAT_INTENT_BINDING_REQUIRED')
        calls = [c for c in data['calls'] if c['invocation_id'] == resolved.binding.invocation_id]
        _require(len(calls) == 1 and calls[0]['state'] == 'DISPATCHED'
            and calls[0]['profile'] == profile, 'CHAT_INTENT_DURABLE_DISPATCH_REQUIRED')
        _require(policy.approval_verified and json.loads(policy.policy_canonical_json)['status'] == 'APPROVED',
            'CHAT_INTENT_POLICY_NOT_APPROVED')
        system, prompt = render_intent(definition, resolved.snapshot, authority, text, policy,
                                      checked_at=self.clock())
        initial = _hash(dict(authority=authority.content_sha256,policy=policy.record_sha256,
                             message=hashlib.sha256(text.encode()).hexdigest()))
        check_operation()
        response = self.transport(profile, system, prompt, json.loads(definition.output_schema_json))
        raw = complete_response_text(response)
        _require(len(raw.encode()) <= 65536, 'CHAT_INTENT_RESPONSE_TOO_LARGE')
        value = parse_json(raw)
        schema = json.loads(definition.output_schema_json)['properties']
        _require(type(value) is dict and set(value) == set(schema)
            and value['schema_version'] == definition.output_schema_version
            and value['audience'] == selection['audience']
            and value['formula_version'] == selection['formula_version'], 'CHAT_INTENT_RESPONSE_INVALID')
        features = value['features']
        _require(type(features) is list and len(features) <= 32
            and all(type(f) is str for f in features) and len(set(features)) == len(features)
            and set(features) <= set(schema['features']['items']['enum']), 'CHAT_INTENT_FEATURES_INVALID')
        observation = provider_observation(response, raw)
        final, final_policy, final_text = self.refresh()
        current = _hash(dict(authority=final.content_sha256,policy=final_policy.record_sha256,
                             message=hashlib.sha256(final_text.encode()).hexdigest()))
        _require(current == initial and final.native_job_id != authority.native_job_id
            and final_policy.native_job_id != policy.native_job_id
            and final_policy.approval_verified
            and self.clock() < min(authority.valid_until,final.valid_until,
                                   policy.valid_until,final_policy.valid_until),
            'CHAT_INTENT_CONTEXT_CHANGED')
        check_operation()
        return EvidenceInvocationDraft(raw,canonical(value),initial,
            resolved.binding.binding_sha256,observation)


class EvidenceQuestionDispatch:
    """Capture provider accounting around the existing admitted question adapter."""
    def __init__(self, initial, *, refresh, transport, clock=lambda: datetime.now(timezone.utc)):
        from src.application.campaign_evidence_conversation import EvidenceQuestionAdapter
        self._response = None
        def observed_transport(*args):
            response = transport(*args)
            self._response = response
            return response
        self._adapter = EvidenceQuestionAdapter(initial,refresh=refresh,
            transport=observed_transport,clock=clock)

    def __call__(self, profile, resolved):
        draft = self._adapter(profile,resolved)
        raw = complete_response_text(self._response)
        _require(len(raw.encode()) <= 65536 and hashlib.sha256(raw.encode()).hexdigest()
            == draft.response_sha256, 'CHAT_QUESTION_PROVIDER_RESPONSE_CHANGED')
        return EvidenceInvocationDraft(raw,draft.questions_canonical_json,
            draft.context_sha256,draft.binding_sha256,provider_observation(self._response,raw))
