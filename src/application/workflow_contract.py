"""Pure workflow contracts for future BigQuery readers/writers.

No authentication, catalog publication, cloud calls or persistence. Validated rows
are not access grants. Service code must resolve current identity, membership,
conversation, catalog and evidence, then commit under the transaction coordinator.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
import hashlib
import json
import math
import re
from types import MappingProxyType

from src.application.access_policy import OwnedResource
from src.application.workflow_schema import ARTIFACT_FIELDS, LOCATION, PROFILE_FIELDS, SCHEMA_VERSION, TABLES
from src.retrieval.request import CANONICAL_SKUS, CHANNELS, OBJECTIVES, VISUAL_REFERENCES

PREFERENCE_FIELDS = ('canonical_sku', 'objective', 'audience', 'channel', 'visual_reference')
UNSUPPORTED_FEATURES = frozenset({
    'BENEFIT_CLAIMS', 'DISCOUNTS', 'CUSTOM_COPY', 'CUSTOM_LAYOUT', 'CUSTOM_SIZE',
    'OTHER_TASK', 'OTHER_CONSTRAINT',
})
MAX_AUDIENCE_CHARACTERS = 160
MAX_JSON_BYTES = 65536
RETRY_WINDOW = timedelta(days=7)
ARTIFACT_BUCKET = 'creative-brand-memory-project-661d501a-e709-4276-a26'
BRIEF_VERSION = 'grounded-campaign-brief-v1'
OUTPUT_GUARDRAIL_VERSION = 'workflow-output-guardrails-v1'
ARTIFACT_TYPES = MappingProxyType({
    'BANNER_PNG': ('png', 'image/png'),
    'BRIEF_JSON': ('json', 'application/json'),
    'BRIEF_TEXT': ('txt', 'text/plain'),
})
CAMPAIGN_STAGES = ('RETRIEVING_EVIDENCE', 'PLANNING', 'RENDERING', 'VERIFYING')
TOKEN_FIELDS = ('input_tokens', 'output_tokens', 'cached_tokens', 'thinking_tokens')
ACCOUNTING_FIELDS = ('usage_status', *TOKEN_FIELDS, 'cost_status', 'estimated_cost', 'currency', 'pricing_version')
NUMERIC_MAX = Decimal('99999999999999999999999999999.999999999')


class ContractError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(f'Workflow validation failed: {code}')  # Never include row content.


class JobState(str, Enum):
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    RECOVERY_REQUIRED = 'RECOVERY_REQUIRED'
    SUCCEEDED = 'SUCCEEDED'
    BLOCKED = 'BLOCKED'
    FAILED = 'FAILED'


class RequestState(str, Enum):
    ACCEPTED = 'ACCEPTED'
    READY = 'READY'
    NEEDS_CLARIFICATION = 'NEEDS_CLARIFICATION'
    BLOCKED = 'BLOCKED'
    FAILED = 'FAILED'


class AttemptState(str, Enum):
    RUNNING = 'RUNNING'
    RECOVERY_REQUIRED = 'RECOVERY_REQUIRED'
    SUCCEEDED = 'SUCCEEDED'
    BLOCKED = 'BLOCKED'
    FAILED = 'FAILED'


class InvocationState(str, Enum):
    PLANNED = 'PLANNED'
    DISPATCHED = 'DISPATCHED'
    UNKNOWN = 'UNKNOWN'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    BLOCKED = 'BLOCKED'


class DispatchStatus(str, Enum):
    NOT_DISPATCHED = 'NOT_DISPATCHED'
    DISPATCHED = 'DISPATCHED'
    UNKNOWN = 'UNKNOWN'


class RetryDecision(str, Enum):
    NEW = 'NEW'
    REUSE = 'REUSE'


TRANSITIONS = MappingProxyType({
    JobState.QUEUED: frozenset({JobState.RUNNING, JobState.BLOCKED, JobState.FAILED}),
    JobState.RUNNING: frozenset({JobState.SUCCEEDED, JobState.BLOCKED, JobState.FAILED,
                               JobState.RECOVERY_REQUIRED}),
    JobState.RECOVERY_REQUIRED: frozenset({JobState.QUEUED, JobState.SUCCEEDED,
                                        JobState.BLOCKED, JobState.FAILED}),
    JobState.SUCCEEDED: frozenset(), JobState.BLOCKED: frozenset(), JobState.FAILED: frozenset(),
})


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ContractError(code)


def _id(value: str, *, maximum: int = 128) -> None:
    _require(isinstance(value, str) and 0 < len(value) <= maximum
             and all(not c.isspace() and ord(c) >= 32 and ord(c) != 127 for c in value),
             'INVALID_IDENTIFIER')


def _digest(value: str) -> None:
    _require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None,
             'INVALID_DIGEST')


def _integer(value: int, minimum: int = 1) -> None:
    _require(type(value) is int and value >= minimum, 'INVALID_INTEGER')


def _timestamp(value: datetime) -> None:
    _require(isinstance(value, datetime) and value.tzinfo is not None
             and value.utcoffset() is not None, 'INVALID_TIMESTAMP')


def _numeric(value: Decimal) -> None:
    """Exact BigQuery NUMERIC representability, independent of decimal context."""
    _require(isinstance(value, Decimal) and value.is_finite(), 'INVALID_NUMERIC')
    digits, exponent = value.as_tuple().digits, value.as_tuple().exponent
    if value.is_zero():
        exponent = 0
    else:
        # Trailing zeroes do not require rounding; Decimal.normalize() would
        # depend on the process's precision and can silently round large costs.
        for digit in reversed(digits):
            if digit != 0:
                break
            exponent += 1
    _require(exponent >= -9 and value.copy_abs() <= NUMERIC_MAX, 'INVALID_NUMERIC')


def canonical_json_bytes(value: object) -> bytes:
    """Bounded canonical UTF-8 representation shared by contracts and downloads."""
    def check(item, depth=0):
        _require(depth <= 12, 'INVALID_JSON')
        if isinstance(item, dict):
            _require(all(isinstance(k, str) for k in item), 'INVALID_JSON')
            for child in item.values():
                check(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                check(child, depth + 1)
        else:
            _require(item is None or type(item) in (str, bool, int)
                     or (type(item) is float and math.isfinite(item)), 'INVALID_JSON')
    check(value)
    try:
        encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                             separators=(',', ':')).encode('utf-8')
    except (ValueError, UnicodeError) as error:
        raise ContractError('INVALID_JSON') from error
    _require(len(encoded) <= MAX_JSON_BYTES, 'JSON_TOO_LARGE')
    return encoded


# Keep internal callers on the same bounded canonical representation.
_canonical = canonical_json_bytes


def _shape(row: dict, schema) -> None:
    _require(isinstance(row, dict) and set(row) == {f.name for f in schema}, 'INVALID_ROW_SHAPE')
    for field in schema:
        value = row[field.name]
        if field.mode == 'REPEATED':
            _require(isinstance(value, list), 'INVALID_FIELD_TYPE')
            values = value
        elif value is None:
            _require(field.mode == 'NULLABLE', 'MISSING_REQUIRED_FIELD')
            continue
        else:
            values = [value]
        for item in values:
            kind = field.field_type
            if kind == 'RECORD':
                _shape(item, field.fields)
            elif kind == 'STRING':
                _require(isinstance(item, str) and bool(item), 'INVALID_FIELD_TYPE')
            elif kind == 'INTEGER':
                _require(type(item) is int and -(2**63) <= item < 2**63, 'INVALID_FIELD_TYPE')
            elif kind == 'FLOAT':
                _require(type(item) in (int, float), 'INVALID_FIELD_TYPE')
                try:
                    finite = math.isfinite(item)
                except OverflowError:
                    finite = False
                _require(finite, 'INVALID_FIELD_TYPE')
            elif kind == 'TIMESTAMP':
                _timestamp(item)
            elif kind == 'JSON':
                _canonical(item)
            elif kind == 'BOOLEAN':
                _require(type(item) is bool, 'INVALID_FIELD_TYPE')
            elif kind == 'NUMERIC':
                _numeric(item)
            else:
                raise ContractError('UNSUPPORTED_FIELD_TYPE')


def validate_profile(profile: dict) -> None:
    """Structural consistency only; enabling a model still requires published M2 policy."""
    _shape(profile, PROFILE_FIELDS)
    for name in ('profile_id', 'profile_version', 'catalog_version', 'model_id', 'prompt_id',
                 'prompt_version', 'output_schema_version', 'safety_policy_version'):
        _id(profile[name])
    _digest(profile['settings_sha256'])
    _require(profile['stage'] in {'INTERPRETER', 'PLANNER'} and profile['location'] == LOCATION,
             'INVALID_PROFILE_SCOPE')
    _integer(profile['max_output_tokens'])
    _integer(profile['timeout_ms'])
    if profile['seed'] is not None:
        _require(-(2**31) <= profile['seed'] < 2**31, 'INVALID_SEED')
    if profile['temperature'] is not None:
        _require(profile['temperature'] >= 0, 'INVALID_TEMPERATURE')
    control, level, budget = (profile[k] for k in ('thinking_control', 'thinking_level', 'thinking_budget'))
    if control == 'LEVEL':
        _require(level in {'MINIMAL', 'LOW', 'MEDIUM', 'HIGH'} and budget is None, 'INVALID_THINKING')
    elif control == 'BUDGET':
        _require(level is None, 'INVALID_THINKING')
        _integer(budget, 0)
    elif control == 'NONE':
        _require(level is None and budget is None, 'INVALID_THINKING')
    else:
        raise ContractError('INVALID_THINKING')


def _preferences(value: dict, *, complete: bool, catalog=CANONICAL_SKUS) -> None:
    _require(type(catalog) is frozenset and 0<len(catalog)<=1000
        and all(type(sku) is str and re.fullmatch(r'CI-SKU-[0-9]{4}',sku) for sku in catalog),
        'INVALID_REQUEST_CATALOG')
    _require(isinstance(value, dict) and set(value) == set(PREFERENCE_FIELDS), 'INVALID_PREFERENCES')
    choices = dict(canonical_sku=catalog, objective=OBJECTIVES,
                   channel=CHANNELS, visual_reference=VISUAL_REFERENCES)
    for key, item in value.items():
        if item is None:
            _require(not complete, 'INCOMPLETE_PREFERENCES')
        elif key in choices:
            _require(isinstance(item, str) and item in choices[key], 'INVALID_PREFERENCES')
        else:
            _require(isinstance(item, str) and item == item.strip() and bool(item)
                     and len(item) <= MAX_AUDIENCE_CHARACTERS
                     and all(ord(c) >= 32 and ord(c) != 127 for c in item), 'INVALID_AUDIENCE')


def _request_input(row: dict, *, catalog=CANONICAL_SKUS) -> dict:
    _require(row['schema_version'] == SCHEMA_VERSION, 'SCHEMA_VERSION_MISMATCH')
    for key in ('workspace_id', 'conversation_id', 'request_id', 'trace_id', 'policy_version'):
        _id(row[key])
    for key in ('owner_id', 'principal_id'):
        _id(row[key], maximum=256)
    _require(row['principal_id'] == row['owner_id'], 'OWNER_MISMATCH')
    if row['session_id'] is not None:
        _id(row['session_id'])
    operation = row['operation']
    _require(operation in {'INTERPRET', 'GENERATE', 'REFINE'}, 'INVALID_OPERATION')
    _require(operation != 'REFINE', 'FEATURE_NOT_ENABLED')
    submission = row['submitted_preferences']
    _require(isinstance(submission, dict) and set(submission) == {'preferences', 'renderer'},
             'INVALID_SUBMISSION')
    _preferences(submission['preferences'], complete=operation == 'GENERATE', catalog=catalog)
    _require(row['parent_output_id'] is None, 'FEATURE_NOT_ENABLED')
    if operation == 'INTERPRET':
        _digest(row['message_sha256'])
        _require(row['generation_intent_id'] is None and submission['renderer'] is None,
                 'INVALID_INTERPRETATION_INPUT')
        stages = {'INTERPRETER'}
    else:
        _require(row['message_sha256'] is None, 'INVALID_GENERATION_INPUT')
        _id(row['generation_intent_id'])
        renderer = submission['renderer']
        _require(isinstance(renderer, dict) and set(renderer) == {'version', 'settings_sha256'},
                 'INVALID_RENDERER')
        _id(renderer['version'])
        _digest(renderer['settings_sha256'])
        stages = {'INTERPRETER', 'PLANNER'}
    profiles = row['model_profiles']
    for profile in profiles:
        validate_profile(profile)
    _require(len(profiles) == len(stages) and {p['stage'] for p in profiles} == stages,
             'INVALID_STAGE_PROFILES')
    return {**{key: row[key] for key in ('schema_version', 'workspace_id', 'conversation_id',
             'owner_id', 'principal_id', 'operation', 'submitted_preferences', 'message_sha256',
             'parent_output_id', 'generation_intent_id', 'policy_version')},
            'model_profiles': sorted(profiles, key=lambda p: p['stage'])}


def request_fingerprint(row: dict, *, catalog=CANONICAL_SKUS) -> str:
    """Hash accepted input, excluding outcome, revisions, timestamps and correlation IDs."""
    _shape(row, TABLES['requests'].schema)
    return hashlib.sha256(_canonical(_request_input(row, catalog=catalog))).hexdigest()


def retry_key_hash(*, workspace_id: str, principal_id: str, conversation_id: str,
                   operation: str, retry_key: str) -> str:
    for value in (workspace_id, conversation_id, retry_key):
        _id(value)
    _id(principal_id, maximum=256)
    _require(operation in {'INTERPRET', 'GENERATE'}, 'FEATURE_NOT_ENABLED')
    return hashlib.sha256(_canonical(dict(schema_version=SCHEMA_VERSION, workspace_id=workspace_id,
        principal_id=principal_id, conversation_id=conversation_id, operation=operation,
        retry_key=retry_key))).hexdigest()


def _scope(row: dict, conversation: OwnedResource) -> None:
    _require(isinstance(conversation, OwnedResource), 'INVALID_CONVERSATION')
    _require(row['workspace_id'] == conversation.workspace_id
             and row['conversation_id'] == conversation.resource_id
             and row['owner_id'] == conversation.owner_id, 'CONVERSATION_SCOPE_MISMATCH')


def validate_request(row: dict, *, conversation: OwnedResource, catalog=CANONICAL_SKUS) -> None:
    _shape(row, TABLES['requests'].schema)
    _scope(row, conversation)
    _integer(row['revision'])
    _require(row['updated_at'] >= row['created_at'], 'INVALID_TIME_ORDER')
    _digest(row['idempotency_key_sha256'])
    _digest(row['request_sha256'])
    _require(row['request_sha256'] == request_fingerprint(row, catalog=catalog), 'REQUEST_HASH_MISMATCH')
    resolved = {key: row[key] for key in PREFERENCE_FIELDS}
    _preferences(resolved, complete=row['request_status'] == 'READY', catalog=catalog)
    features = row['unsupported_features']
    _require(features == sorted(set(features)) and set(features) <= UNSUPPORTED_FEATURES,
             'INVALID_UNSUPPORTED_FEATURES')
    if row['operation'] == 'GENERATE':
        _require(row['request_status'] == 'READY' and not features
                 and resolved == row['submitted_preferences']['preferences'], 'INVALID_GENERATION_REQUEST')
    elif row['request_status'] in {'ACCEPTED', 'FAILED'}:
        _require(not features and all(v is None for v in resolved.values()), 'INVALID_INTERPRETATION_OUTCOME')
    elif row['request_status'] == 'READY':
        _require(not features, 'UNSUPPORTED_REQUEST')
    elif row['request_status'] == 'NEEDS_CLARIFICATION':
        _require(not features and any(v is None for v in resolved.values()), 'INVALID_CLARIFICATION')
    elif row['request_status'] == 'BLOCKED':
        _require(bool(features), 'INVALID_BLOCKED_REQUEST')
    else:
        raise ContractError('INVALID_REQUEST_STATE')


def resolve_retry(candidate: dict, existing: dict | None, *, conversation: OwnedResource,
                  now: datetime, retry_window: timedelta = RETRY_WINDOW) -> RetryDecision:
    """Pure decision after an authoritative, scoped lookup; not a uniqueness lock."""
    _timestamp(now)
    _require(isinstance(retry_window, timedelta) and retry_window.total_seconds() > 0,
             'INVALID_RETRY_WINDOW')
    validate_request(candidate, conversation=conversation)
    _require(candidate['updated_at'] <= now, 'INVALID_TIME_ORDER')
    _require(now - candidate['created_at'] <= retry_window, 'RETRY_WINDOW_EXPIRED')
    if existing is None:
        return RetryDecision.NEW
    validate_request(existing, conversation=conversation)
    _require(existing['operation'] == candidate['operation'], 'EXISTING_REQUEST_MISMATCH')
    same_key = existing['idempotency_key_sha256'] == candidate['idempotency_key_sha256']
    same_intent = (candidate['operation'] == 'GENERATE'
                   and existing['generation_intent_id'] == candidate['generation_intent_id'])
    _require(same_key or same_intent, 'EXISTING_REQUEST_MISMATCH')
    _require(existing['request_sha256'] == candidate['request_sha256'], 'IDEMPOTENCY_CONFLICT')
    _require(existing['updated_at'] <= now, 'INVALID_TIME_ORDER')
    _require(now - existing['created_at'] <= retry_window, 'RETRY_WINDOW_EXPIRED')
    return RetryDecision.REUSE


def transition_request(current: dict, *, conversation: OwnedResource, target: RequestState,
                       expected_revision: int, now: datetime,
                       resolved_preferences: dict | None = None,
                       unsupported_features: list[str] | None = None) -> dict:
    """Finalize interpretation without changing immutable accepted input or hash."""
    validate_request(current, conversation=conversation)
    _timestamp(now)
    _integer(expected_revision)
    _require(isinstance(target, RequestState), 'INVALID_REQUEST_STATE')
    _require(current['revision'] == expected_revision, 'REVISION_CONFLICT')
    _require(current['operation'] == 'INTERPRET' and current['request_status'] == 'ACCEPTED'
             and target != RequestState.ACCEPTED, 'INVALID_TRANSITION')
    _require(now >= current['updated_at'], 'INVALID_TIME_ORDER')
    result = deepcopy(current)
    preferences = resolved_preferences
    if target == RequestState.FAILED:
        _require(preferences is None and unsupported_features is None, 'INVALID_INTERPRETATION_OUTCOME')
        preferences = dict.fromkeys(PREFERENCE_FIELDS)
    _preferences(preferences, complete=target == RequestState.READY)
    result.update(preferences)
    result.update(request_status=target.value, revision=current['revision'] + 1, updated_at=now,
                  unsupported_features=[] if unsupported_features is None else deepcopy(unsupported_features))
    validate_request(result, conversation=conversation)
    return result


def validate_job(row: dict, *, request: dict, conversation: OwnedResource, catalog=CANONICAL_SKUS) -> None:
    _shape(row, TABLES['jobs'].schema)
    validate_request(request, conversation=conversation, catalog=catalog)
    _scope(row, conversation)
    _require(request['operation'] == 'GENERATE' and request['request_status'] == 'READY',
             'REQUEST_NOT_GENERATABLE')
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'request_id',
                'principal_id', 'trace_id', 'canonical_sku', 'idempotency_key_sha256',
                'request_sha256', 'generation_intent_id', 'policy_version'):
        _require(row[key] == request[key], 'JOB_REQUEST_MISMATCH')
    _id(row['job_id'])
    for key in ('active_attempt_id', 'output_id'):
        if row[key] is not None:
            _id(row[key])
    if row['safe_error_code'] is not None:
        _require(re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}', row['safe_error_code']) is not None,
                 'INVALID_SAFE_ERROR')
    _integer(row['revision'])
    _integer(row['attempt_count'], 0)
    _require(request['created_at'] <= row['created_at'] <= row['updated_at']
             and row['deadline_at'] > row['created_at'], 'INVALID_TIME_ORDER')
    try:
        state = JobState(row['state'])
    except ValueError as error:
        raise ContractError('INVALID_JOB_STATE') from error
    if state == JobState.QUEUED:
        _require(all(row[k] is None for k in ('active_attempt_id', 'output_id', 'safe_error_code',
                                            'completed_at')), 'INVALID_QUEUED_JOB')
    elif state in {JobState.RUNNING, JobState.RECOVERY_REQUIRED}:
        _require(row['active_attempt_id'] is not None and row['attempt_count'] > 0
                 and row['output_id'] is None and row['completed_at'] is None,
                 'INVALID_ACTIVE_JOB')
        _require((row['safe_error_code'] is None) == (state == JobState.RUNNING), 'INVALID_ACTIVE_JOB')
    else:
        _require(row['completed_at'] == row['updated_at'], 'INVALID_COMPLETED_TIME')
        if state == JobState.SUCCEEDED:
            _require(row['active_attempt_id'] is not None and row['attempt_count'] > 0
                     and row['output_id'] is not None and row['safe_error_code'] is None,
                     'INVALID_SUCCESSFUL_JOB')
        else:
            _require(row['output_id'] is None and row['safe_error_code'] is not None,
                     'INVALID_FAILED_JOB')
    _require(row['active_attempt_id'] is None or row['attempt_count'] > 0, 'INVALID_ATTEMPT_COUNT')


def transition_job(current: dict, *, request: dict, conversation: OwnedResource,
                   target: JobState, expected_revision: int, now: datetime,
                   attempt_id: str | None = None, output_id: str | None = None,
                   safe_error_code: str | None = None,
                   dispatch_status: DispatchStatus | None = None) -> dict:
    """Propose a validated copy. Reader proofs, access and atomic commit are still required."""
    validate_job(current, request=request, conversation=conversation)
    _timestamp(now)
    _integer(expected_revision)
    _require(isinstance(target, JobState), 'INVALID_JOB_STATE')
    _require(expected_revision == current['revision'], 'REVISION_CONFLICT')
    _require(target in TRANSITIONS[JobState(current['state'])], 'INVALID_TRANSITION')
    _require(now >= current['updated_at'], 'INVALID_TIME_ORDER')
    _require(dispatch_status is None or isinstance(dispatch_status, DispatchStatus), 'INVALID_DISPATCH_STATUS')
    result = deepcopy(current)
    if current['state'] == 'QUEUED' and target == JobState.RUNNING:
        _id(attempt_id)
        _require(now < current['deadline_at'], 'DEADLINE_EXCEEDED')
        result['attempt_count'] += 1
        result['active_attempt_id'] = attempt_id
    else:
        _require(attempt_id == current['active_attempt_id'], 'ATTEMPT_CONFLICT')
    if target == JobState.QUEUED:
        _require(dispatch_status == DispatchStatus.NOT_DISPATCHED, 'UNSAFE_DISPATCH_REPLAY')
        _require(now < current['deadline_at'], 'DEADLINE_EXCEEDED')
        result['active_attempt_id'] = None
    _require(target == JobState.QUEUED or dispatch_status is None, 'UNEXPECTED_DISPATCH_STATUS')
    result.update(state=target.value, revision=current['revision'] + 1, updated_at=now,
                  output_id=output_id, safe_error_code=safe_error_code,
                  completed_at=now if target in {JobState.SUCCEEDED, JobState.BLOCKED, JobState.FAILED} else None)
    validate_job(result, request=request, conversation=conversation)
    return result


def validate_attempt(row: dict, *, request: dict, job: dict, conversation: OwnedResource, catalog=CANONICAL_SKUS) -> None:
    """Check current/closed attempt consistency, not dispatch proof or ownership grants."""
    _shape(row, TABLES['job_attempts'].schema)
    validate_job(job, request=request, conversation=conversation, catalog=catalog)
    _scope(row, conversation)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id',
                'job_id', 'request_id', 'trace_id'):
        _require(row[key] == job[key], 'ATTEMPT_PARENT_MISMATCH')
    _id(row['attempt_id'])
    _id(row['worker_id'])  # Every attempt belongs to its server-assigned worker.
    if row['dispatch_receipt_id'] is not None:
        _id(row['dispatch_receipt_id'])
    if row['safe_error_code'] is not None:
        _require(re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}', row['safe_error_code']) is not None,
                 'INVALID_SAFE_ERROR')
    _integer(row['revision'])
    _integer(row['attempt_number'])
    _require(row['attempt_number'] <= job['attempt_count'], 'INVALID_ATTEMPT_COUNT')
    _require(job['created_at'] <= row['started_at'] <= row['updated_at']
             and row['started_at'] <= job['updated_at']
             and row['started_at'] < job['deadline_at'], 'INVALID_TIME_ORDER')
    try:
        state = AttemptState(row['state'])
    except ValueError:
        raise ContractError('INVALID_ATTEMPT_STATE') from None
    if state in {AttemptState.RUNNING, AttemptState.RECOVERY_REQUIRED}:
        _require(row['completed_at'] is None, 'INVALID_ACTIVE_ATTEMPT')
        _require((row['safe_error_code'] is None) == (state == AttemptState.RUNNING),
                 'INVALID_ACTIVE_ATTEMPT')
    else:
        _require(row['completed_at'] == row['updated_at'], 'INVALID_COMPLETED_TIME')
        _require((row['safe_error_code'] is None) == (state == AttemptState.SUCCEEDED),
                 'INVALID_TERMINAL_ATTEMPT')
        if state == AttemptState.SUCCEEDED:
            _require(row['dispatch_receipt_id'] is not None, 'MISSING_DISPATCH_RECEIPT')
    if row['attempt_id'] == job['active_attempt_id']:
        _require(row['attempt_number'] == job['attempt_count'], 'INVALID_ATTEMPT_COUNT')
        _require(row['state'] == job['state'] and row['safe_error_code'] == job['safe_error_code']
                 and row['completed_at'] == job['completed_at'], 'ACTIVE_ATTEMPT_MISMATCH')
    else:
        # Only a closed, declared-undispatched attempt may precede a requeue.
        # Absence of this receipt does NOT prove no model invocation was sent:
        # a future coordinator must inspect all invocation records atomically.
        _require(state == AttemptState.FAILED and row['dispatch_receipt_id'] is None,
                 'INVALID_HISTORICAL_ATTEMPT')
        _require(row['attempt_number'] < job['attempt_count'] or job['active_attempt_id'] is None,
                 'INVALID_ATTEMPT_COUNT')
        _require(row['completed_at'] <= job['updated_at'], 'INVALID_TIME_ORDER')
    if job['completed_at'] is not None:
        _require(row['updated_at'] <= job['completed_at'], 'INVALID_TIME_ORDER')


def _validate_stage_event(row: dict, *, attempt: dict, job: dict) -> None:
    _shape(row, TABLES['stage_events'].schema)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id',
                'job_id', 'request_id', 'attempt_id', 'trace_id'):
        _require(row[key] == attempt[key], 'EVENT_PARENT_MISMATCH')
    _require(row['policy_version'] == job['policy_version'], 'EVENT_POLICY_MISMATCH')
    _id(row['event_id'])
    _integer(row['sequence'])
    _require(row['stage'] in CAMPAIGN_STAGES, 'INVALID_EVENT_STAGE')
    _require(row['outcome'] in {'STARTED', 'SUCCEEDED', 'BLOCKED', 'FAILED'}, 'INVALID_EVENT_OUTCOME')
    _require(row['occurred_at'] >= attempt['started_at'], 'INVALID_TIME_ORDER')
    if attempt['completed_at'] is not None:
        _require(row['occurred_at'] <= attempt['completed_at'], 'INVALID_TIME_ORDER')
    if row['outcome'] == 'STARTED':
        _require(row['duration_ms'] is None and row['safe_error_code'] is None, 'INVALID_STARTED_EVENT')
    else:
        _integer(row['duration_ms'], 0)
        if row['outcome'] == 'SUCCEEDED':
            _require(row['safe_error_code'] is None, 'INVALID_SUCCESSFUL_EVENT')
        else:
            _require(row['safe_error_code'] is not None, 'MISSING_EVENT_ERROR')
            _require(re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}', row['safe_error_code']) is not None,
                     'INVALID_SAFE_ERROR')


def validate_stage_event(row: dict, *, attempt: dict, request: dict, job: dict,
                         conversation: OwnedResource) -> None:
    """Validate one row; ordering/pairing requires the complete attempt prefix below."""
    validate_attempt(attempt, request=request, job=job, conversation=conversation)
    _validate_stage_event(row, attempt=attempt, job=job)


def validate_stage_events(rows: list[dict], *, attempt: dict, request: dict, job: dict,
                          conversation: OwnedResource) -> None:
    """Validate a full sequence-ordered prefix; not a paginated event fragment."""
    validate_attempt(attempt, request=request, job=job, conversation=conversation)
    _require(isinstance(rows, list) and len(rows) <= 2 * len(CAMPAIGN_STAGES), 'INVALID_EVENT_HISTORY')
    seen: set[str] = set()
    stage_index, started, failed = 0, None, None
    previous_time = attempt['started_at']
    for sequence, row in enumerate(rows, 1):
        _validate_stage_event(row, attempt=attempt, job=job)
        _require(row['event_id'] not in seen, 'DUPLICATE_EVENT_ID')
        seen.add(row['event_id'])
        _require(row['sequence'] == sequence, 'EVENT_SEQUENCE_CONFLICT')
        _require(row['occurred_at'] >= previous_time, 'INVALID_TIME_ORDER')
        previous_time = row['occurred_at']
        _require(failed is None and stage_index < len(CAMPAIGN_STAGES), 'EVENT_AFTER_TERMINAL_STAGE')
        _require(row['stage'] == CAMPAIGN_STAGES[stage_index], 'EVENT_STAGE_ORDER')
        if started is None:
            _require(row['outcome'] == 'STARTED', 'MISSING_STAGE_START')
            started = row
        else:
            _require(row['outcome'] != 'STARTED', 'DUPLICATE_STAGE_START')
            elapsed = row['occurred_at'] - started['occurred_at']
            elapsed_ms = (elapsed.days * 86400000000 + elapsed.seconds * 1000000 + elapsed.microseconds) // 1000
            _require(row['duration_ms'] == elapsed_ms, 'EVENT_DURATION_MISMATCH')
            if row['outcome'] == 'SUCCEEDED':
                stage_index += 1
            else:
                failed = row
            started = None
    if attempt['state'] == 'SUCCEEDED':
        _require(stage_index == len(CAMPAIGN_STAGES) and started is None and failed is None,
                 'INCOMPLETE_SUCCESSFUL_STAGES')
    if failed is not None and attempt['state'] in {'BLOCKED', 'FAILED'}:
        _require(failed['outcome'] == attempt['state']
                 and failed['safe_error_code'] == attempt['safe_error_code'], 'EVENT_ATTEMPT_OUTCOME_MISMATCH')
    # An interrupted attempt may leave a STARTED event without a completion.
    # Never manufacture a successful event from terminal job status alone.


def append_stage_event(rows: list[dict], candidate: dict, *, attempt: dict, request: dict,
                       job: dict, conversation: OwnedResource) -> list[dict]:
    """Propose append or identical-ID replay; immutable content, no persistence/lock."""
    validate_stage_event(candidate, attempt=attempt, request=request, job=job, conversation=conversation)
    _require(isinstance(rows, list) and len(rows) <= 2 * len(CAMPAIGN_STAGES), 'INVALID_EVENT_HISTORY')
    # Validate the resulting prefix together, so the last verification event
    # can be checked against a proposed successful attempt/job in one commit.
    matches = [row for row in rows if isinstance(row, dict) and row.get('event_id') == candidate['event_id']]
    _require(len(matches) <= 1, 'DUPLICATE_EVENT_ID')
    if matches:
        _require(matches[0] == candidate, 'EVENT_IDEMPOTENCY_CONFLICT')
        result = deepcopy(rows)
    else:
        result = deepcopy(rows) + [deepcopy(candidate)]
    validate_stage_events(result, attempt=attempt, request=request, job=job, conversation=conversation)
    return result


def _invocation_accounting(row: dict) -> None:
    reported = sum(row[key] is not None for key in TOKEN_FIELDS)
    for key in TOKEN_FIELDS:
        if row[key] is not None:
            _integer(row[key], 0)
    expected_usage = 'NOT_REPORTED' if reported == 0 else 'REPORTED' if reported == len(TOKEN_FIELDS) else 'PARTIAL'
    _require(row['usage_status'] == expected_usage, 'INVALID_USAGE_STATUS')
    if row['cached_tokens'] is not None and row['input_tokens'] is not None:
        _require(row['cached_tokens'] <= row['input_tokens'], 'INVALID_CACHED_TOKENS')
    # Thinking and candidate output are separate provider categories. Actual
    # usage is retained even if it exceeds a configured generation limit.
    if row['cost_status'] == 'NOT_ESTIMATED':
        _require(all(row[key] is None for key in ('estimated_cost', 'currency', 'pricing_version')),
                 'INVALID_COST_STATUS')
    else:
        _require(row['cost_status'] in {'PARTIAL', 'ESTIMATED'} and reported > 0, 'INVALID_COST_STATUS')
        _require(row['estimated_cost'] is not None and row['estimated_cost'] >= 0, 'INVALID_COST_ESTIMATE')
        _require(isinstance(row['currency'], str) and re.fullmatch(r'[A-Z]{3}', row['currency']) is not None,
                 'INVALID_COST_CURRENCY')
        _id(row['pricing_version'])
        if row['cost_status'] == 'ESTIMATED':
            _require(row['usage_status'] == 'REPORTED', 'INCOMPLETE_ESTIMATED_USAGE')


def validate_model_invocation(row: dict, *, request: dict, conversation: OwnedResource,
                              job: dict | None = None, attempt: dict | None = None, catalog=CANONICAL_SKUS) -> None:
    """Validate one observed/proposed call; no dispatch grant, pricing proof or I/O."""
    _shape(row, TABLES['model_invocations'].schema)
    validate_request(request, conversation=conversation, catalog=catalog)
    _scope(row, conversation)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'request_id', 'trace_id'):
        _require(row[key] == request[key], 'INVOCATION_PARENT_MISMATCH')
    _id(row['invocation_id'])
    _integer(row['revision'])
    _integer(row['call_number'])
    # The current non-streaming workflow sends one call per submitted stage.
    # Explicit new generation/interpretation must use a new request/intent.
    _require(row['call_number'] == 1, 'INVOCATION_REPLAY_NOT_ENABLED')
    validate_profile(row['profile'])
    profiles = [p for p in request['model_profiles'] if p['stage'] == row['stage']]
    _require(len(profiles) == 1 and row['profile'] == profiles[0], 'INVOCATION_PROFILE_MISMATCH')
    _require(row['started_at'] >= request['created_at'], 'INVALID_TIME_ORDER')
    try:
        state = InvocationState(row['state'])
    except ValueError:
        raise ContractError('INVALID_INVOCATION_STATE') from None
    if row['stage'] == 'INTERPRETER':
        _require(request['operation'] == 'INTERPRET' and job is None and attempt is None
                 and row['job_id'] is None and row['attempt_id'] is None, 'INVALID_INTERPRETER_SCOPE')
        if state in {InvocationState.PLANNED, InvocationState.DISPATCHED}:
            _require(request['request_status'] == 'ACCEPTED', 'INTERPRETATION_ALREADY_CLOSED')
        elif request['request_status'] != 'ACCEPTED':
            _require(row['started_at'] <= request['updated_at'], 'INVALID_TIME_ORDER')
            if row['completed_at'] is not None:
                _require(row['completed_at'] <= request['updated_at'], 'INVALID_TIME_ORDER')
    else:
        _require(row['stage'] == 'PLANNER' and job is not None and attempt is not None, 'INVALID_PLANNER_SCOPE')
        validate_attempt(attempt, request=request, job=job, conversation=conversation, catalog=catalog)
        _require(row['job_id'] == job['job_id'] and row['attempt_id'] == attempt['attempt_id'],
                 'INVOCATION_PARENT_MISMATCH')
        _require(attempt['started_at'] <= row['started_at'] < job['deadline_at'], 'INVALID_TIME_ORDER')
        if state in {InvocationState.PLANNED, InvocationState.DISPATCHED}:
            _require(attempt['state'] == 'RUNNING', 'INVOCATION_ATTEMPT_CLOSED')
        elif state == InvocationState.UNKNOWN:
            _require(attempt['state'] in {'RECOVERY_REQUIRED', 'FAILED', 'BLOCKED'},
                     'UNKNOWN_INVOCATION_REQUIRES_RECOVERY')
        elif state != InvocationState.SUCCEEDED:
            _require(attempt['state'] != 'SUCCEEDED', 'FAILED_INVOCATION_IN_SUCCESSFUL_ATTEMPT')
        if attempt['completed_at'] is not None:
            _require(row['started_at'] <= attempt['completed_at'], 'INVALID_TIME_ORDER')
            if row['completed_at'] is not None:
                _require(row['completed_at'] <= attempt['completed_at'], 'INVALID_TIME_ORDER')
    for key in ('dispatch_receipt_id', 'worker_id', 'returned_model_id'):
        if row[key] is not None:
            _id(row[key])
    if row['response_sha256'] is not None:
        _digest(row['response_sha256'])
    for key in ('safe_error_code', 'finish_reason'):
        if row[key] is not None:
            _require(re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}', row[key]) is not None, 'INVALID_INVOCATION_CODE')
    _require(row['safety_outcome'] in {None, 'NOT_EVALUATED', 'PASSED', 'BLOCKED'}, 'INVALID_SAFETY_OUTCOME')
    _invocation_accounting(row)
    receipt = row['dispatch_receipt_id']
    response_fields = ('returned_model_id', 'response_sha256', 'finish_reason', 'safety_outcome')
    if state == InvocationState.PLANNED:
        _require(receipt is None and row['worker_id'] is None and row['completed_at'] is None
                 and row['safe_error_code'] is None, 'INVALID_PLANNED_INVOCATION')
    elif state == InvocationState.DISPATCHED:
        _require(receipt is not None and row['worker_id'] is not None and row['completed_at'] is None
                 and row['safe_error_code'] is None, 'INVALID_DISPATCHED_INVOCATION')
    elif state == InvocationState.UNKNOWN:
        _require(receipt is not None and row['worker_id'] is not None and row['completed_at'] is None
                 and row['safe_error_code'] is not None, 'INVALID_UNKNOWN_INVOCATION')
    else:
        _require(row['completed_at'] is not None and row['completed_at'] >= row['started_at'],
                 'INVALID_COMPLETED_TIME')
        _require((row['safe_error_code'] is None) == (state == InvocationState.SUCCEEDED),
                 'INVALID_TERMINAL_INVOCATION')
        if state == InvocationState.SUCCEEDED:
            _require(receipt is not None and row['worker_id'] is not None and row['response_sha256'] is not None
                     and row['finish_reason'] == 'STOP' and row['safety_outcome'] == 'PASSED',
                     'INVALID_SUCCESSFUL_INVOCATION')
    if receipt is not None:
        _require(row['worker_id'] is not None, 'MISSING_INVOCATION_WORKER')
        if attempt is not None:
            _require(receipt == attempt['dispatch_receipt_id'] and row['worker_id'] == attempt['worker_id'],
                     'INVOCATION_DISPATCH_MISMATCH')
    else:
        _require(attempt is None or attempt['dispatch_receipt_id'] is None, 'INVOCATION_DISPATCH_MISMATCH')
        _require(row['worker_id'] is None and all(row[key] is None for key in response_fields)
                 and row['usage_status'] == 'NOT_REPORTED' and row['cost_status'] == 'NOT_ESTIMATED',
                 'UNDISPATCHED_INVOCATION_HAS_RESULTS')
    if state in {InvocationState.PLANNED, InvocationState.DISPATCHED}:
        _require(all(row[key] is None for key in response_fields) and row['usage_status'] == 'NOT_REPORTED'
                 and row['cost_status'] == 'NOT_ESTIMATED', 'ACTIVE_INVOCATION_HAS_RESULTS')


def validate_model_invocation_change(previous: dict, candidate: dict, *, expected_revision: int,
                                     request: dict, conversation: OwnedResource,
                                     job: dict | None = None, attempt: dict | None = None,
                                     previous_request: dict | None = None, previous_job: dict | None = None,
                                     previous_attempt: dict | None = None) -> None:
    """Validate an update proposal; authoritative revision fencing remains mandatory."""
    context = dict(request=request, conversation=conversation, job=job, attempt=attempt)
    validate_model_invocation(candidate, **context)
    # When the proposed commit changes parent state/receipt too, the caller
    # must supply its separately read prior rows; never reconstruct old state.
    prior_request = request if previous_request is None else previous_request
    prior_job = job if previous_job is None else previous_job
    prior_attempt = attempt if previous_attempt is None else previous_attempt
    validate_model_invocation(previous, request=prior_request, conversation=conversation,
                              job=prior_job, attempt=prior_attempt)
    _require(prior_request['request_sha256'] == request['request_sha256']
             and prior_request['idempotency_key_sha256'] == request['idempotency_key_sha256'],
             'INVOCATION_REQUEST_CHANGED')
    _integer(expected_revision)
    _require(previous['revision'] == expected_revision and candidate['revision'] == expected_revision + 1,
             'REVISION_CONFLICT')
    immutable = set(previous) - {'revision', 'state', 'dispatch_receipt_id', 'worker_id',
        'returned_model_id', 'response_sha256', 'finish_reason', 'safety_outcome', 'safe_error_code',
        'completed_at', *ACCOUNTING_FIELDS}
    _require(all(candidate[key] == previous[key] for key in immutable), 'INVOCATION_IDENTITY_CHANGED')
    if previous['dispatch_receipt_id'] is not None:
        _require(candidate['dispatch_receipt_id'] == previous['dispatch_receipt_id']
                 and candidate['worker_id'] == previous['worker_id'], 'INVOCATION_DISPATCH_CHANGED')
    transitions = {
        'PLANNED': {'DISPATCHED', 'FAILED', 'BLOCKED'},
        'DISPATCHED': {'SUCCEEDED', 'FAILED', 'BLOCKED', 'UNKNOWN'},
        'UNKNOWN': {'SUCCEEDED', 'FAILED', 'BLOCKED', 'UNKNOWN'},
        'SUCCEEDED': {'SUCCEEDED'}, 'FAILED': {'FAILED'}, 'BLOCKED': {'BLOCKED'},
    }
    _require(candidate['state'] in transitions[previous['state']], 'UNSAFE_INVOCATION_TRANSITION')
    if previous['state'] == 'PLANNED' and candidate['state'] != 'DISPATCHED':
        _require(candidate['dispatch_receipt_id'] is None, 'DISPATCH_CLAIM_SKIPPED')
    for key in ('returned_model_id', 'response_sha256', 'finish_reason'):
        _require(previous[key] is None or candidate[key] == previous[key], 'OBSERVED_RESPONSE_CHANGED')
    if previous['safety_outcome'] in {'PASSED', 'BLOCKED'}:
        _require(candidate['safety_outcome'] == previous['safety_outcome'], 'OBSERVED_RESPONSE_CHANGED')
    if candidate['state'] == previous['state']:
        _require(all(candidate[key] == previous[key] for key in set(previous)-{'revision', *ACCOUNTING_FIELDS}),
                 'INVOCATION_OUTCOME_CHANGED')
    # Late usage/cost updates may fill missing values, never rewrite observed
    # token counts. Estimates may be revised using a recorded pricing version.
    _require(all(previous[key] is None or candidate[key] == previous[key] for key in TOKEN_FIELDS),
             'OBSERVED_USAGE_CHANGED')
    if previous['estimated_cost'] is not None:
        _require(candidate['estimated_cost'] is not None, 'COST_ESTIMATE_REMOVED')
        if (candidate['estimated_cost'] != previous['estimated_cost']
                and all(candidate[key] == previous[key] for key in TOKEN_FIELDS)):
            _require(candidate['pricing_version'] != previous['pricing_version'], 'UNVERSIONED_COST_CHANGE')


@dataclass(frozen=True, slots=True)
class ArtifactLimits:
    """Server-configured resource bounds, not values supplied by a browser."""
    maximum_artifact_bytes: int = 32 * 1024 * 1024
    maximum_total_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        _require(type(self.maximum_artifact_bytes) is int and 1 <= self.maximum_artifact_bytes <= 128 * 1024 * 1024
                 and type(self.maximum_total_bytes) is int and 1 <= self.maximum_total_bytes <= 384 * 1024 * 1024,
                 'INVALID_ARTIFACT_LIMITS')


DEFAULT_ARTIFACT_LIMITS = ArtifactLimits()


def _path_segment(value: str) -> None:
    _require(isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', value) is not None,
             'INVALID_ARTIFACT_SCOPE')


def artifact_gcs_uri(*, workspace_id: str, conversation_id: str, canonical_sku: str,
                     output_id: str, artifact_id: str, kind: str) -> str:
    """Reserved workflow namespace; never references products or registered creatives."""
    for value in (workspace_id, conversation_id, output_id, artifact_id):
        _path_segment(value)
    _require(isinstance(canonical_sku, str) and canonical_sku in CANONICAL_SKUS, 'INVALID_ARTIFACT_SCOPE')
    _require(isinstance(kind, str) and kind in ARTIFACT_TYPES, 'INVALID_ARTIFACT_KIND')
    extension, _ = ARTIFACT_TYPES[kind]
    return (f'gs://{ARTIFACT_BUCKET}/workflow_artifacts/v1/{workspace_id}/{conversation_id}/'
            f'{canonical_sku}/{output_id}/{artifact_id}.{extension}')


def _generation(value: str) -> None:
    _require(isinstance(value, str) and re.fullmatch(r'[1-9][0-9]{0,19}', value) is not None,
             'INVALID_GENERATION')


def validate_artifact(row: dict, *, workspace_id: str, conversation_id: str, canonical_sku: str,
                      output_id: str, limits: ArtifactLimits = DEFAULT_ARTIFACT_LIMITS) -> None:
    """Check metadata only. The protected reader must still verify pinned GCS bytes."""
    _require(isinstance(limits, ArtifactLimits), 'INVALID_ARTIFACT_LIMITS')
    _shape(row, ARTIFACT_FIELDS)
    expected_uri = artifact_gcs_uri(workspace_id=workspace_id, conversation_id=conversation_id,
        canonical_sku=canonical_sku, output_id=output_id, artifact_id=row['artifact_id'], kind=row['kind'])
    _require(row['gcs_uri'] == expected_uri, 'ARTIFACT_SCOPE_MISMATCH')
    _require(row['content_type'] == ARTIFACT_TYPES[row['kind']][1], 'INVALID_ARTIFACT_CONTENT_TYPE')
    _generation(row['gcs_generation'])
    _digest(row['sha256'])
    _require(1 <= row['size_bytes'] <= limits.maximum_artifact_bytes, 'ARTIFACT_SIZE_EXCEEDED')


def _json_timestamp(value) -> datetime:
    _require(isinstance(value, str), 'INVALID_TIMESTAMP')
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise ContractError('INVALID_TIMESTAMP') from None
    _timestamp(result)
    return result


def _output_brief(row: dict, request: dict, job: dict) -> None:
    """Versioned structure and pinned identity, not fresh factual/claim verification."""
    brief = row['brief']
    fields = {'version', 'canonical_sku', 'source_system', 'evidence_source_of_truth', 'status',
        'approval_scope', 'request', 'snapshot_id', 'created_at', 'concept', 'copy', 'visual_direction',
        'product_image', 'historical_reference', 'experiment', 'grounded_evidence', 'limitations',
        'model_selection', 'model', 'evidence_index', 'approval_checked_at', 'approval_recheck_passed'}
    _require(isinstance(brief, dict) and fields <= set(brief)
             and set(brief) <= fields | {'generated_visual_references'}, 'INVALID_BRIEF_SHAPE')
    expected = dict(version=BRIEF_VERSION, canonical_sku=row['canonical_sku'], source_system='SYNTHETIC_POC',
        evidence_source_of_truth='BIGQUERY', status='POC_REVIEW_DRAFT', approval_scope='SYNTHETIC_POC_ONLY',
        request={key: request[key] for key in PREFERENCE_FIELDS})
    _require(all(brief[key] == value for key, value in expected.items()), 'BRIEF_INPUT_MISMATCH')
    _require(brief['approval_recheck_passed'] is True, 'INVALID_BRIEF_APPROVAL_STATE')
    _digest(brief['snapshot_id'])
    created, checked = (_json_timestamp(brief[key]) for key in ('created_at', 'approval_checked_at'))
    _require(job['created_at'] <= checked <= created <= row['created_at'], 'INVALID_TIME_ORDER')
    for key in ('visual_direction', 'product_image', 'experiment', 'grounded_evidence', 'model_selection', 'model'):
        _require(isinstance(brief[key], dict) and bool(brief[key]), 'INVALID_BRIEF_SHAPE')
    _require(isinstance(brief['concept'], str) and bool(brief['concept'].strip())
             and isinstance(brief['limitations'], list) and bool(brief['limitations'])
             and all(isinstance(v, str) and bool(v.strip()) for v in brief['limitations'])
             and (brief['historical_reference'] is None or isinstance(brief['historical_reference'], dict)),
             'INVALID_BRIEF_SHAPE')
    if 'generated_visual_references' in brief:
        _require(isinstance(brief['generated_visual_references'], dict)
                 and bool(brief['generated_visual_references']), 'INVALID_BRIEF_SHAPE')
    index = brief['evidence_index']
    _require(isinstance(index, list) and bool(index), 'INVALID_EVIDENCE_INDEX')
    seen = set()
    for item in index:
        _require(isinstance(item, dict) and set(item) == {'evidence_id', 'reference'}, 'INVALID_EVIDENCE_INDEX')
        _id(item['evidence_id'])
        _require(item['evidence_id'] not in seen and isinstance(item['reference'], str)
                 and 0 < len(item['reference']) <= 2048
                 and all(ord(c) >= 32 and ord(c) != 127 for c in item['reference']), 'INVALID_EVIDENCE_INDEX')
        seen.add(item['evidence_id'])
    copy = brief['copy']
    _require(isinstance(copy, dict) and set(copy) == {'headline', 'body', 'cta', 'claim_ids',
             'evidence_ids', 'neutral_copy_authority'}, 'INVALID_BRIEF_SHAPE')
    _require(copy['neutral_copy_authority'] == 'PROJECT_CREATIVE_PROPOSAL'
             and all(isinstance(copy[k], str) for k in ('headline', 'body', 'cta'))
             and bool(copy['headline'].strip()) and bool(copy['cta'].strip()), 'INVALID_BRIEF_SHAPE')
    for key in ('claim_ids', 'evidence_ids'):
        values = copy[key]
        _require(isinstance(values, list), 'INVALID_EVIDENCE_INDEX')
        for value in values:
            _id(value)
        _require(len(values) == len(set(values)), 'INVALID_EVIDENCE_INDEX')
    _require(set(copy['evidence_ids']) <= seen, 'INVALID_EVIDENCE_INDEX')
    grounded = brief['grounded_evidence']
    _require(set(grounded) == {'product', 'policies', 'approved_claims', 'sales', 'campaign_performance'}
             and all(isinstance(value, dict) for value in grounded.values()), 'INVALID_BRIEF_SHAPE')
    product = grounded['product'].get('identity')
    image = brief['product_image'].get('metadata')
    _require(isinstance(product, dict) and product.get('sku') == row['canonical_sku']
             and product.get('source_system') == 'BEAUTYFEEDS'
             and isinstance(image, dict) and image.get('sku') == row['canonical_sku']
             and image.get('source_system') == 'BEAUTYFEEDS'
             and image.get('asset_id') == f'AST-{row["canonical_sku"]}-PRIMARY'
             and image.get('gcs_uri') == f'gs://{ARTIFACT_BUCKET}/products/{row["canonical_sku"]}/primary.jpg',
             'BRIEF_PRODUCT_MISMATCH')
    _require(grounded['approved_claims'].get('scope') == 'SYNTHETIC_POC_ONLY'
             and grounded['policies'].get('scope') == 'PROJECT_POC_POLICY'
             and all(grounded[key].get('source_system') == 'SYNTHETIC_POC'
                     for key in ('sales', 'campaign_performance')), 'INVALID_BRIEF_PROVENANCE')
    model = brief['model']
    required = {'requested_model', 'location', 'response_validation', 'response_sha256', 'seed', 'generation_settings'}
    _require(required <= set(model) and set(model) <= required | {'returned_model', 'seed', 'generation_settings',
        'determinism', 'usage', 'input_image_asset_ids', 'temperature'}, 'INVALID_BRIEF_MODEL')
    _digest(model['response_sha256'])
    planner = next(p for p in request['model_profiles'] if p['stage'] == 'PLANNER')
    _require(model.get('requested_model') == planner['model_id'] and model.get('location') == planner['location']
             and model.get('response_validation') == dict(finish_reason='STOP', prompt_blocked=False, safety_passed=True),
             'BRIEF_MODEL_MISMATCH')
    validation = model['response_validation']
    _require(validation['prompt_blocked'] is False and validation['safety_passed'] is True, 'BRIEF_MODEL_MISMATCH')
    settings = {'max_output_tokens': planner['max_output_tokens']}
    if planner['temperature'] is not None:
        settings['temperature'] = planner['temperature']
    if planner['thinking_control'] == 'LEVEL':
        settings['thinking_level'] = planner['thinking_level']
    elif planner['thinking_control'] == 'BUDGET':
        settings['thinking_budget'] = planner['thinking_budget']
    effective = model['generation_settings']
    _require(isinstance(effective, dict) and effective == settings
             and all((type(effective[key]) in (int, float) if key == 'temperature'
                      else type(effective[key]) is type(value)) for key, value in settings.items())
             and model['seed'] == planner['seed'] and type(model['seed']) is type(planner['seed']),
             'BRIEF_MODEL_MISMATCH')
    if 'temperature' in model:
        _require(model['temperature'] == planner['temperature']
                 and type(model['temperature']) in (int, float), 'BRIEF_MODEL_MISMATCH')


def validate_output(row: dict, *, request: dict, job: dict, conversation: OwnedResource,
                    limits: ArtifactLimits = DEFAULT_ARTIFACT_LIMITS) -> None:
    """Validate a completed/proposed-success output; no access, evidence or GCS proof."""
    _require(isinstance(limits, ArtifactLimits), 'INVALID_ARTIFACT_LIMITS')
    _shape(row, TABLES['outputs'].schema)
    validate_job(job, request=request, conversation=conversation)
    _scope(row, conversation)
    _require(job['state'] == 'SUCCEEDED', 'OUTPUT_JOB_NOT_SUCCEEDED')
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'job_id', 'request_id',
                'trace_id', 'canonical_sku', 'policy_version'):
        _require(row[key] == job[key], 'OUTPUT_PARENT_MISMATCH')
    _require(row['output_id'] == job['output_id'] and row['attempt_id'] == job['active_attempt_id']
             and row['parent_output_id'] == request['parent_output_id'], 'OUTPUT_PARENT_MISMATCH')
    for key in ('output_id', 'attempt_id', 'evidence_snapshot_id', 'brief_schema_version', 'renderer_version'):
        _id(row[key])
    _require(row['source_system'] == 'SYNTHETIC_POC' and row['approval_scope'] == 'SYNTHETIC_POC_ONLY'
             and row['initial_review_status'] == 'NOT_REVIEWED' and row['real_business_approved'] is False,
             'INVALID_OUTPUT_PROVENANCE')
    _require(job['created_at'] <= row['created_at'] <= job['completed_at'], 'INVALID_TIME_ORDER')
    _require(row['brief_schema_version'] == BRIEF_VERSION, 'BRIEF_VERSION_MISMATCH')
    _require(row['renderer_version'] == request['submitted_preferences']['renderer']['version'],
             'RENDERER_VERSION_MISMATCH')
    for profile in row['model_profiles']:
        validate_profile(profile)
    _require(sorted(row['model_profiles'], key=lambda p: p['stage'])
             == sorted(request['model_profiles'], key=lambda p: p['stage']), 'OUTPUT_PROFILE_MISMATCH')
    guardrails = row['guardrail_summary']
    _require(isinstance(guardrails, dict) and set(guardrails) == {'schema_version', 'grounding',
             'benefit_claims_authorized', 'product_image_integrity', 'human_review', 'content_screening'},
             'INVALID_OUTPUT_GUARDRAILS')
    _require(guardrails['schema_version'] == OUTPUT_GUARDRAIL_VERSION and guardrails['grounding'] == 'PASSED'
             and guardrails['product_image_integrity'] == 'PASSED' and guardrails['benefit_claims_authorized'] is False
             and guardrails['human_review'] == 'REQUIRED'
             and guardrails['content_screening'] in ('NOT_INTEGRATED', 'PASSED'), 'INVALID_OUTPUT_GUARDRAILS')
    _output_brief(row, request, job)
    artifacts = row['artifacts']
    _require(1 <= len(artifacts) <= len(ARTIFACT_TYPES), 'INVALID_ARTIFACT_SET')
    ids, kinds = set(), set()
    total_bytes = 0
    for artifact in artifacts:
        validate_artifact(artifact, workspace_id=row['workspace_id'], conversation_id=row['conversation_id'],
                          canonical_sku=row['canonical_sku'], output_id=row['output_id'], limits=limits)
        _require(artifact['artifact_id'] not in ids and artifact['kind'] not in kinds, 'DUPLICATE_ARTIFACT')
        ids.add(artifact['artifact_id'])
        kinds.add(artifact['kind'])
        total_bytes += artifact['size_bytes']
        if artifact['kind'] == 'BRIEF_JSON':
            raw = _canonical(row['brief'])
            _require(artifact['size_bytes'] == len(raw) and artifact['sha256'] == hashlib.sha256(raw).hexdigest(),
                     'BRIEF_ARTIFACT_MISMATCH')
    _require('BANNER_PNG' in kinds, 'BANNER_ARTIFACT_REQUIRED')
    _require(total_bytes <= limits.maximum_total_bytes, 'ARTIFACT_TOTAL_SIZE_EXCEEDED')
