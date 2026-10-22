"""Versioned question-child authority pins; never execution or release permission."""
import json
from src.application.campaign_acceptance_contract import _hash, _utc
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.campaign_evidence_conversation import BoundGapRead
from src.application.workflow_contract import _require, _id, _digest

VERSION = 'campaign-question-authority-v1'
PARENT_KEYS = {'request_id', 'request_sha256', 'invocation_id', 'response_sha256',
    'authority_sha256', 'gap_plan_sha256', 'gap_context_sha256', 'question_profile_sha256'}


def validate_parent(value, *, child_request_id):
    _require(type(value) is dict and set(value) == PARENT_KEYS, 'QUESTION_PARENT_SHAPE')
    for key in ('request_id', 'invocation_id'): _id(value[key])
    for key in PARENT_KEYS - {'request_id', 'invocation_id'}: _digest(value[key])
    _require(value['request_id'] != child_request_id, 'QUESTION_PARENT_SELF_REFERENCE')


def bind_question_authority(child, *, parent, gaps, profile, checked_at):
    """Build from fresh server captures; resulting payload still needs atomic storage.

    Parent intent text is already provider-bound by read_bound_gaps. This function
    adds exact parent/candidate pins, rather than accepting a caller-supplied hash.
    """
    from src.application.campaign_quality_authority import validate_payload
    from src.application.workflow_contract import validate_profile
    validate_payload(child); validate_profile(profile)
    now = _utc(checked_at)
    _require(type(parent) is ExecutionAuthorityRead and type(gaps) is BoundGapRead
        and parent.observed_at <= now < min(parent.valid_until, gaps.valid_until),
        'QUESTION_PARENT_EXPIRED')
    content = json.loads(parent.content_canonical_json)
    _require(_hash(content) == parent.content_sha256, 'QUESTION_PARENT_CAPTURE_CHANGED')
    source = content['authority']; request = content['request_record']
    scope = json.loads(gaps.plan.canonical_json)['scope']
    _require(all(child[k] == source[k] == request[k] for k in
        ('workspace_id', 'conversation_id', 'principal_id'))
        and gaps.principal_id == child['principal_id']
        and child['request_id'] != request['request_id']
        and all(child[k] == source[k] for k in ('sku', 'country', 'channel', 'audience', 'formula_version'))
        and child['claims'] == source['claims']
        and scope['request_id'] == request['request_id']
        and scope['request_sha256'] == request['request_sha256']
        and all(scope[k] == source[k] for k in ('workspace_id', 'conversation_id', 'sku', 'country', 'channel')),
        'QUESTION_PARENT_SCOPE_CHANGED')
    calls = [c for c in content['calls'] if c['request_id'] == request['request_id']
        and c['stage'] == 'INTERPRETER']
    _require(len(calls) == 1 and calls[0]['state'] == 'SUCCEEDED'
        and calls[0]['finish_reason'] == 'STOP' and calls[0]['safety_outcome'] == 'PASSED'
        and calls[0]['profile']['output_schema_version'] == 'campaign-evidence-intent-v1'
        and calls[0]['response_sha256'] == scope['intent_sha256']
        and profile['stage'] == 'INTERPRETER'
        and profile['output_schema_version'] == 'campaign-evidence-questions-v1',
        'QUESTION_PARENT_OUTCOME_CHANGED')
    call = calls[0]
    expected = dict(request_id=request['request_id'], role='ASSISTANT',
        content_sha256=call['response_sha256'], invocation_id=call['invocation_id'])
    _require(child['transcript'].count(expected) == 1, 'QUESTION_PARENT_TRANSCRIPT_MISSING')
    pins = dict(request_id=request['request_id'], request_sha256=request['request_sha256'],
        invocation_id=call['invocation_id'], response_sha256=call['response_sha256'],
        authority_sha256=parent.content_sha256, gap_plan_sha256=gaps.plan.sha256,
        gap_context_sha256=gaps.context_sha256, question_profile_sha256=_hash(profile))
    value = dict(child, version=VERSION, stage='CLARIFICATION', question_parent=pins)
    validate_payload(value)
    return value
