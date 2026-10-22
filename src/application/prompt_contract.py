"""Pure Vertex prompt snapshot and BigQuery lifecycle contracts.

These objects do not call Vertex, read BigQuery, authenticate an actor, prove an
evaluation result or enforce a unique ACTIVE row. Later adapters must read an exact
Vertex version, build a snapshot from that response and fence lifecycle writes in
BigQuery. Repository or cached prompt text is never a serving fallback.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import re
import unicodedata

from src.application.workflow_contract import (
    ContractError, _digest, _id, _require, _timestamp, canonical_json_bytes,
)
from src.application.workflow_schema import LOCATION
from src.application.workspace_contract import MAX_REVISION

PROMPT_CONTRACT_VERSION = 'vertex-prompt-lifecycle-v1'
VERTEX_PROJECT = 'project-661d501a-e709-4276-a26'
MAX_PROMPT_TEXT_CHARACTERS = 32768
MAX_PROMPT_VARIABLES = 32
MAX_VERTEX_RESOURCE_ID_DIGITS = 32

_VARIABLE = re.compile(r'[A-Za-z][A-Za-z0-9_]{0,63}')
_PLACEHOLDER = re.compile(r'\{([A-Za-z][A-Za-z0-9_]{0,63})\}')
_REASON = re.compile(r'[A-Z][A-Z0-9_]{0,63}')
_PROJECT = re.compile(r'[a-z][a-z0-9-]{4,28}[a-z0-9]')
_VERTEX_RESOURCE_ID = re.compile(r'[1-9][0-9]{0,' + str(MAX_VERTEX_RESOURCE_ID_DIGITS - 1) + r'}')


class PromptTask(str, Enum):
    INTERPRETER = 'INTERPRETER'
    PLANNER = 'PLANNER'


class PromptLifecycle(str, Enum):
    DRAFT = 'DRAFT'
    REGISTERED = 'REGISTERED'
    EVALUATED = 'EVALUATED'
    ACTIVE = 'ACTIVE'
    RETIRED = 'RETIRED'
    BLOCKED = 'BLOCKED'


_TRANSITIONS = {
    PromptLifecycle.DRAFT: frozenset({PromptLifecycle.REGISTERED, PromptLifecycle.BLOCKED}),
    PromptLifecycle.REGISTERED: frozenset({PromptLifecycle.EVALUATED, PromptLifecycle.BLOCKED}),
    PromptLifecycle.EVALUATED: frozenset({PromptLifecycle.ACTIVE, PromptLifecycle.BLOCKED}),
    PromptLifecycle.ACTIVE: frozenset({PromptLifecycle.RETIRED, PromptLifecycle.BLOCKED}),
    PromptLifecycle.RETIRED: frozenset({PromptLifecycle.ACTIVE, PromptLifecycle.BLOCKED}),
    PromptLifecycle.BLOCKED: frozenset(),
}


def _safe_text(value: str, code: str) -> None:
    _require(type(value) is str and 1 <= len(value) <= MAX_PROMPT_TEXT_CHARACTERS,
             code)
    _require(value == unicodedata.normalize('NFC', value) and '\r' not in value
             and all(character in '\n\t' or ord(character) >= 32
                     and ord(character) != 127 for character in value), code)


def _json_object(value: str, code: str) -> dict:
    _safe_text(value, code)

    def object_pairs(pairs):
        _require(len({key for key, _ in pairs}) == len(pairs), code)
        return dict(pairs)

    try:
        decoded = json.loads(value, object_pairs_hook=object_pairs)
        _require(isinstance(decoded, dict), code)
        canonical = canonical_json_bytes(decoded).decode('utf-8')
        _require(value == canonical, code)
        return decoded
    except ContractError:
        raise
    except (json.JSONDecodeError, TypeError, UnicodeError):
        raise ContractError(code) from None


def _json_list(value: str, code: str) -> list:
    _safe_text(value, code)
    try:
        decoded = json.loads(value)
        _require(isinstance(decoded, list)
                 and value == canonical_json_bytes(decoded).decode('utf-8'), code)
        return decoded
    except ContractError:
        raise
    except (json.JSONDecodeError, TypeError, UnicodeError):
        raise ContractError(code) from None


def _utc(value: datetime) -> datetime:
    _timestamp(value)
    try:
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        raise ContractError('INVALID_TIMESTAMP') from None


@dataclass(frozen=True, slots=True)
class VertexPromptSnapshot:
    """Canonical exact-version response suitable for independent hash checking."""

    prompt_key: str
    task: PromptTask
    vertex_project: str
    vertex_location: str
    vertex_prompt_id: str
    vertex_version_id: str
    system_instruction: str
    prompt_template: str
    required_variables: tuple[str, ...]
    model_id: str
    generation_config_json: str
    safety_settings_json: str
    output_schema_json: str
    output_schema_version: str
    safety_policy_version: str
    contract_version: str = PROMPT_CONTRACT_VERSION
    template_sha256: str = field(init=False)
    snapshot_sha256: str = field(init=False)

    def __post_init__(self):
        _require(self.contract_version == PROMPT_CONTRACT_VERSION,
                 'UNSUPPORTED_PROMPT_CONTRACT')
        _id(self.prompt_key)
        _require(isinstance(self.task, PromptTask), 'INVALID_PROMPT_TASK')
        _require(type(self.vertex_project) is str
                 and _PROJECT.fullmatch(self.vertex_project) is not None
                 and self.vertex_project == VERTEX_PROJECT, 'INVALID_VERTEX_PROMPT_PROJECT')
        _require(self.vertex_location == LOCATION, 'INVALID_VERTEX_PROMPT_LOCATION')
        for value in (self.vertex_prompt_id, self.vertex_version_id):
            _require(type(value) is str and _VERTEX_RESOURCE_ID.fullmatch(value) is not None,
                     'INVALID_VERTEX_PROMPT_RESOURCE')
        _safe_text(self.system_instruction, 'INVALID_PROMPT_TEXT')
        _safe_text(self.prompt_template, 'INVALID_PROMPT_TEXT')
        _require(type(self.required_variables) is tuple
                 and 1 <= len(self.required_variables) <= MAX_PROMPT_VARIABLES
                 and all(type(value) is str and _VARIABLE.fullmatch(value) is not None
                         for value in self.required_variables)
                 and self.required_variables == tuple(sorted(set(self.required_variables))),
                 'INVALID_PROMPT_VARIABLES')
        placeholders = set(_PLACEHOLDER.findall(
            self.system_instruction + '\n' + self.prompt_template))
        _require(placeholders == set(self.required_variables), 'PROMPT_VARIABLE_MISMATCH')
        _id(self.model_id)
        _id(self.output_schema_version)
        _id(self.safety_policy_version)
        generation = _json_object(self.generation_config_json,
                                  'INVALID_PROMPT_GENERATION_CONFIG')
        expected = {'automatic_function_calling_disabled', 'max_output_tokens',
                    'response_mime_type', 'seed', 'temperature', 'thinking_level'}
        _require(set(generation) == expected
                 and type(generation['automatic_function_calling_disabled']) is bool
                 and generation['automatic_function_calling_disabled'] is True
                 and type(generation['max_output_tokens']) is int
                 and 1 <= generation['max_output_tokens'] <= 65536
                 and type(generation['seed']) is int
                 and -(2**31) <= generation['seed'] < 2**31
                 and (generation['temperature'] is None
                      or type(generation['temperature']) in (int, float)
                      and 0 <= generation['temperature'] <= 2)
                 and generation['thinking_level'] in {'MINIMAL', 'LOW', 'MEDIUM', 'HIGH'}
                 and generation['response_mime_type'] == 'application/json',
                 'INVALID_PROMPT_GENERATION_CONFIG')
        safety = _json_list(self.safety_settings_json, 'INVALID_PROMPT_SAFETY_SETTINGS')
        _require(1 <= len(safety) <= 16
                 and all(type(item) is dict and set(item) == {'category', 'threshold'}
                         and type(item['category']) is str and type(item['threshold']) is str
                         and _VARIABLE.fullmatch(item['category']) is not None
                         and _VARIABLE.fullmatch(item['threshold']) is not None for item in safety)
                 and [item['category'] for item in safety]
                     == sorted({item['category'] for item in safety}),
                 'INVALID_PROMPT_SAFETY_SETTINGS')
        output_schema = _json_object(self.output_schema_json,
                                     'INVALID_PROMPT_OUTPUT_SCHEMA')
        _require(output_schema.get('type') == 'OBJECT'
                 and type(output_schema.get('properties')) is dict
                 and type(output_schema.get('required')) is list
                 and all(type(name) is str and name in output_schema['properties']
                         for name in output_schema['required'])
                 and len(set(output_schema['required'])) == len(output_schema['required'])
                 and all(type(value) is dict
                         for value in output_schema['properties'].values()),
                 'INVALID_PROMPT_OUTPUT_SCHEMA')
        object.__setattr__(self, 'template_sha256', hashlib.sha256(
            canonical_json_bytes(self.template_payload())).hexdigest())
        object.__setattr__(self, 'snapshot_sha256', hashlib.sha256(
            canonical_json_bytes(self.canonical_payload())).hexdigest())

    def template_payload(self) -> dict:
        """Canonical prompt content/settings, independent of its Vertex address."""
        return dict(contract_version=self.contract_version, prompt_key=self.prompt_key,
            task=self.task.value, system_instruction=self.system_instruction,
            prompt_template=self.prompt_template,
            required_variables=list(self.required_variables), model_id=self.model_id,
            generation_config=json.loads(self.generation_config_json),
            safety_settings=json.loads(self.safety_settings_json),
            output_schema=json.loads(self.output_schema_json),
            output_schema_version=self.output_schema_version,
            safety_policy_version=self.safety_policy_version)

    def canonical_payload(self) -> dict:
        """Full immutable content and execution pins; excludes its own digest."""
        return self.template_payload() | dict(vertex_project=self.vertex_project,
            vertex_location=self.vertex_location, vertex_prompt_id=self.vertex_prompt_id,
            vertex_version_id=self.vertex_version_id)


@dataclass(frozen=True, slots=True)
class PromptRegistryRecord:
    """Revisioned supplied-state proposal; BigQuery must fence unique activation."""

    revision: int
    parent_digest: str | None
    snapshot: VertexPromptSnapshot
    status: PromptLifecycle
    changed_at: datetime
    changed_by: str
    reason_code: str
    evaluation_id: str | None = None
    contract_version: str = PROMPT_CONTRACT_VERSION
    digest: str = field(init=False)

    def __post_init__(self):
        _require(self.contract_version == PROMPT_CONTRACT_VERSION,
                 'UNSUPPORTED_PROMPT_CONTRACT')
        _require(type(self.revision) is int and 1 <= self.revision <= MAX_REVISION,
                 'INVALID_PROMPT_REGISTRY_REVISION')
        if self.revision == 1:
            _require(self.parent_digest is None, 'INVALID_PROMPT_REGISTRY_PARENT')
        else:
            _digest(self.parent_digest)
        _require(isinstance(self.snapshot, VertexPromptSnapshot)
                 and isinstance(self.status, PromptLifecycle), 'INVALID_PROMPT_REGISTRY_RECORD')
        object.__setattr__(self, 'changed_at', _utc(self.changed_at))
        _id(self.changed_by, maximum=256)
        _require(type(self.reason_code) is str
                 and _REASON.fullmatch(self.reason_code) is not None,
                 'INVALID_PROMPT_REASON_CODE')
        if self.evaluation_id is not None:
            _id(self.evaluation_id)
        _require(self.status not in {PromptLifecycle.EVALUATED, PromptLifecycle.ACTIVE,
                                    PromptLifecycle.RETIRED}
                 or self.evaluation_id is not None, 'PROMPT_EVALUATION_REQUIRED')
        payload = dict(contract_version=self.contract_version, revision=self.revision,
            parent_digest=self.parent_digest, template_sha256=self.snapshot.template_sha256,
            snapshot_sha256=self.snapshot.snapshot_sha256,
            status=self.status.value, changed_at=self.changed_at.isoformat(),
            changed_by=self.changed_by, reason_code=self.reason_code,
            evaluation_id=self.evaluation_id)
        object.__setattr__(self, 'digest', hashlib.sha256(
            canonical_json_bytes(payload)).hexdigest())


def initial_prompt_record(snapshot: VertexPromptSnapshot, *, imported_at: datetime,
                          imported_by: str) -> PromptRegistryRecord:
    """Record an exact discovered Vertex candidate; this does not verify its RPC."""
    _require(isinstance(snapshot, VertexPromptSnapshot), 'INVALID_PROMPT_REGISTRY_RECORD')
    return PromptRegistryRecord(1, None, snapshot, PromptLifecycle.DRAFT,
        imported_at, imported_by, 'IMPORTED_FROM_VERTEX')


def propose_prompt_transition(previous: PromptRegistryRecord, *, status: PromptLifecycle,
                              changed_at: datetime, changed_by: str, reason_code: str,
                              evaluation_id: str | None = None) -> PromptRegistryRecord:
    """Propose one lifecycle change against supplied state; never persists or authorizes."""
    _require(isinstance(previous, PromptRegistryRecord)
             and isinstance(status, PromptLifecycle), 'INVALID_PROMPT_REGISTRY_RECORD')
    _require(previous.revision < MAX_REVISION, 'PROMPT_REGISTRY_REVISION_EXHAUSTED')
    _require(status in _TRANSITIONS[previous.status], 'INVALID_PROMPT_LIFECYCLE_TRANSITION')
    instant = _utc(changed_at)
    _require(instant >= previous.changed_at, 'PROMPT_REGISTRY_TIME_REGRESSION')
    if status is PromptLifecycle.EVALUATED:
        _require(evaluation_id is not None, 'PROMPT_EVALUATION_REQUIRED')
    elif previous.evaluation_id is not None:
        _require(evaluation_id == previous.evaluation_id, 'PROMPT_EVALUATION_REBOUND')
    else:
        _require(evaluation_id is None, 'UNEXPECTED_PROMPT_EVALUATION')
    return PromptRegistryRecord(previous.revision + 1, previous.digest,
        previous.snapshot, status, instant, changed_by, reason_code, evaluation_id)


def validate_prompt_transition(previous: PromptRegistryRecord,
                               proposed: PromptRegistryRecord) -> None:
    """Require the exact next proposal, including immutable Vertex/content pins."""
    _require(isinstance(proposed, PromptRegistryRecord), 'INVALID_PROMPT_REGISTRY_RECORD')
    expected = propose_prompt_transition(previous, status=proposed.status,
        changed_at=proposed.changed_at, changed_by=proposed.changed_by,
        reason_code=proposed.reason_code, evaluation_id=proposed.evaluation_id)
    _require(proposed.snapshot == previous.snapshot
             and proposed.digest == expected.digest, 'PROMPT_REGISTRY_CHAIN_MISMATCH')
