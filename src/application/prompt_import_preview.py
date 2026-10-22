"""Read-only exact-Vertex import preview and canonical prompt diff.

This service composes the exact Vertex reader with authoritative BigQuery reads.
It does not authenticate or authorize the supplied operator ID, execute a writer,
persist a proposal, activate a prompt or dispatch a model. A later trusted command
must bind authorization and revalidate this preview before registration.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
from typing import Protocol

from src.application.operation_budget import OperationStopped
from src.application.prompt_contract import (
    PromptLifecycle,
    VertexPromptSnapshot,
    initial_prompt_record,
)
from src.application.prompt_registry_codec import (
    PromptRegistryState,
    initial_registration_state,
)
from src.application.prompt_registry_reader import (
    PromptRegistryRead,
    PromptRegistryReadError,
)
from src.application.vertex_prompt_reader import (
    ExactPromptVersionTransport,
    VertexPromptReadRequest,
    read_exact_vertex_prompt,
)
from src.application.workflow_contract import (
    ContractError,
    _id,
    _require,
    canonical_json_bytes,
)

MAX_DIFF_FIELDS = 9
MAX_DIFF_PREVIEW_CHARACTERS = 512

_FIELDS = (
    'system_instruction',
    'prompt_template',
    'required_variables',
    'model_id',
    'generation_config',
    'safety_settings',
    'output_schema',
    'output_schema_version',
    'safety_policy_version',
)


class PromptRegistryPreviewReader(Protocol):
    def read_exact(self, vertex_prompt_id: str,
                   vertex_version_id: str) -> PromptRegistryRead: ...

    def read_eligible(self, task,
                      prompt_key: str) -> PromptRegistryRead: ...


class PromptImportPreviewStatus(str, Enum):
    UNREGISTERED = 'UNREGISTERED'
    EXISTING = 'EXISTING'


class PromptDiffChange(str, Enum):
    ADDED = 'ADDED'
    MODIFIED = 'MODIFIED'


@dataclass(frozen=True, slots=True)
class PromptDiffValue:
    sha256: str
    utf8_bytes: int
    preview: str
    truncated: bool

    def __post_init__(self):
        _require(type(self.sha256) is str and len(self.sha256) == 64
                 and all(character in '0123456789abcdef'
                         for character in self.sha256)
                 and type(self.utf8_bytes) is int and self.utf8_bytes >= 0
                 and type(self.preview) is str
                 and len(self.preview) <= MAX_DIFF_PREVIEW_CHARACTERS
                 and type(self.truncated) is bool,
                 'INVALID_PROMPT_DIFF_VALUE')


@dataclass(frozen=True, slots=True)
class PromptDiffEntry:
    field: str
    change: PromptDiffChange
    before: PromptDiffValue | None
    after: PromptDiffValue

    def __post_init__(self):
        _require(self.field in _FIELDS
                 and isinstance(self.change, PromptDiffChange)
                 and (self.before is None
                      or isinstance(self.before, PromptDiffValue))
                 and isinstance(self.after, PromptDiffValue)
                 and ((self.change is PromptDiffChange.ADDED
                       and self.before is None)
                      or (self.change is PromptDiffChange.MODIFIED
                          and self.before is not None
                          and self.before.sha256 != self.after.sha256)),
                 'INVALID_PROMPT_DIFF_ENTRY')


@dataclass(frozen=True, slots=True)
class PromptImportPreviewRequest:
    vertex_request: VertexPromptReadRequest
    previewed_at: datetime
    proposed_by: str

    def __post_init__(self):
        _require(isinstance(self.vertex_request, VertexPromptReadRequest)
                 and isinstance(self.previewed_at, datetime)
                 and self.previewed_at.tzinfo is not None
                 and self.previewed_at.utcoffset() is not None,
                 'INVALID_PROMPT_IMPORT_PREVIEW_REQUEST')
        try:
            at = self.previewed_at.astimezone(timezone.utc)
        except (OverflowError, ValueError):
            raise ContractError(
                'INVALID_PROMPT_IMPORT_PREVIEW_REQUEST') from None
        object.__setattr__(self, 'previewed_at', at)
        _id(self.proposed_by, maximum=256)


@dataclass(frozen=True, slots=True)
class PromptImportPreview:
    status: PromptImportPreviewStatus
    request: PromptImportPreviewRequest
    candidate: VertexPromptSnapshot
    exact_state: PromptRegistryState | None
    active_state: PromptRegistryState | None
    proposal: PromptRegistryState | None
    differences: tuple[PromptDiffEntry, ...]
    preview_sha256: str

    def __post_init__(self):
        selection = (self.request.vertex_request
                     if isinstance(self.request, PromptImportPreviewRequest)
                     else None)
        _require(isinstance(self.status, PromptImportPreviewStatus)
                 and isinstance(self.request, PromptImportPreviewRequest)
                 and isinstance(self.candidate, VertexPromptSnapshot)
                 and self.candidate.prompt_key == selection.prompt_key
                 and self.candidate.task is selection.task
                 and self.candidate.vertex_prompt_id == selection.prompt_id
                 and self.candidate.vertex_version_id == selection.version_id
                 and self.candidate.output_schema_version
                 == selection.output_schema_version
                 and self.candidate.safety_policy_version
                 == selection.safety_policy_version
                 and (self.exact_state is None
                      or isinstance(self.exact_state, PromptRegistryState))
                 and (self.active_state is None
                      or isinstance(self.active_state, PromptRegistryState))
                 and (self.proposal is None
                      or isinstance(self.proposal, PromptRegistryState))
                 and type(self.differences) is tuple
                 and len(self.differences) <= MAX_DIFF_FIELDS
                 and all(isinstance(item, PromptDiffEntry)
                         for item in self.differences)
                 and tuple(item.field for item in self.differences)
                 == tuple(field for field in _FIELDS
                          if field in {item.field for item in self.differences})
                 and type(self.preview_sha256) is str
                 and len(self.preview_sha256) == 64,
                 'INVALID_PROMPT_IMPORT_PREVIEW')
        if self.exact_state is not None:
            _require(self.exact_state.head.snapshot == self.candidate,
                     'INVALID_PROMPT_IMPORT_PREVIEW')
        if self.active_state is not None:
            active = self.active_state.head
            _require(active.status is PromptLifecycle.ACTIVE
                     and active.snapshot.task is self.candidate.task
                     and active.snapshot.prompt_key == self.candidate.prompt_key,
                     'INVALID_PROMPT_IMPORT_PREVIEW')
        if self.status is PromptImportPreviewStatus.UNREGISTERED:
            expected = initial_registration_state(initial_prompt_record(
                self.candidate, imported_at=self.request.previewed_at,
                imported_by=self.request.proposed_by))
            _require(self.exact_state is None and self.proposal == expected,
                     'INVALID_PROMPT_IMPORT_PREVIEW')
        else:
            _require(self.exact_state is not None and self.proposal is None
                     and self.exact_state.head.snapshot == self.candidate,
                     'INVALID_PROMPT_IMPORT_PREVIEW')
        _require(self.differences == canonical_diff(
            None if self.active_state is None
            else self.active_state.head.snapshot, self.candidate),
            'PROMPT_IMPORT_PREVIEW_DIFF_MISMATCH')
        _require(self.preview_sha256 == _preview_digest(
            self.status, self.request, self.candidate, self.exact_state,
            self.active_state, self.proposal, self.differences),
            'PROMPT_IMPORT_PREVIEW_HASH_MISMATCH')


class PromptImportPreviewError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__('Prompt import preview is unavailable.')


def _values(snapshot: VertexPromptSnapshot) -> dict[str, str]:
    return {
        'system_instruction': snapshot.system_instruction,
        'prompt_template': snapshot.prompt_template,
        'required_variables': canonical_json_bytes(
            list(snapshot.required_variables)).decode('utf-8'),
        'model_id': snapshot.model_id,
        'generation_config': snapshot.generation_config_json,
        'safety_settings': snapshot.safety_settings_json,
        'output_schema': snapshot.output_schema_json,
        'output_schema_version': snapshot.output_schema_version,
        'safety_policy_version': snapshot.safety_policy_version,
    }


def _diff_value(value: str) -> PromptDiffValue:
    encoded = value.encode('utf-8')
    truncated = len(value) > MAX_DIFF_PREVIEW_CHARACTERS
    if truncated:
        half = MAX_DIFF_PREVIEW_CHARACTERS // 2
        preview = value[:half] + value[-half:]
    else:
        preview = value
    return PromptDiffValue(
        hashlib.sha256(encoded).hexdigest(), len(encoded), preview, truncated)


def canonical_diff(before: VertexPromptSnapshot | None,
                   after: VertexPromptSnapshot) -> tuple[PromptDiffEntry, ...]:
    _require(before is None or isinstance(before, VertexPromptSnapshot),
             'INVALID_PROMPT_DIFF_BASELINE')
    _require(isinstance(after, VertexPromptSnapshot),
             'INVALID_PROMPT_DIFF_CANDIDATE')
    old = None if before is None else _values(before)
    new = _values(after)
    entries = []
    for field in _FIELDS:
        if old is not None and old[field] == new[field]:
            continue
        entries.append(PromptDiffEntry(
            field,
            (PromptDiffChange.ADDED if old is None
             else PromptDiffChange.MODIFIED),
            None if old is None else _diff_value(old[field]),
            _diff_value(new[field])))
    return tuple(entries)


def _preview_digest(status: PromptImportPreviewStatus,
                    request: PromptImportPreviewRequest,
                    candidate: VertexPromptSnapshot,
                    exact_state: PromptRegistryState | None,
                    active_state: PromptRegistryState | None,
                    proposal: PromptRegistryState | None,
                    differences: tuple[PromptDiffEntry, ...]) -> str:
    payload = {
        'status': status.value,
        'prompt_id': request.vertex_request.prompt_id,
        'version_id': request.vertex_request.version_id,
        'previewed_at': request.previewed_at.isoformat(),
        'proposed_by': request.proposed_by,
        'candidate_snapshot_sha256': candidate.snapshot_sha256,
        'exact_head_digest': (None if exact_state is None
                              else exact_state.head.digest),
        'active_head_digest': (None if active_state is None
                               else active_state.head.digest),
        'proposal_digest': (None if proposal is None
                            else proposal.head.digest),
        'differences': [{
            'field': item.field,
            'change': item.change.value,
            'before_sha256': (None if item.before is None
                              else item.before.sha256),
            'after_sha256': item.after.sha256,
        } for item in differences],
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class PromptImportPreviewService:
    def __init__(self, transport: ExactPromptVersionTransport,
                 registry_reader: PromptRegistryPreviewReader,
                 *, vertex_timeout_seconds: int = 10):
        if (not callable(getattr(registry_reader, 'read_exact', None))
                or not callable(getattr(registry_reader, 'read_eligible', None))):
            raise ValueError('Authoritative prompt registry reader required')
        if (type(vertex_timeout_seconds) is not int
                or not 1 <= vertex_timeout_seconds <= 30):
            raise ValueError('Vertex prompt timeout must be 1..30 seconds')
        self.transport = transport
        self.registry_reader = registry_reader
        self.vertex_timeout_seconds = vertex_timeout_seconds

    @staticmethod
    def _optional_read(call):
        try:
            value = call()
            _require(isinstance(value, PromptRegistryRead),
                     'INVALID_PROMPT_REGISTRY_READ')
            return value.state
        except PromptRegistryReadError as error:
            if error.code == 'PROMPT_REGISTRY_NOT_FOUND':
                return None
            raise

    def preview(self, request: PromptImportPreviewRequest) -> PromptImportPreview:
        _require(isinstance(request, PromptImportPreviewRequest),
                 'INVALID_PROMPT_IMPORT_PREVIEW_REQUEST')
        try:
            candidate = read_exact_vertex_prompt(
                self.transport, request.vertex_request,
                timeout_seconds=self.vertex_timeout_seconds)
        except OperationStopped:
            raise
        except ContractError as error:
            raise PromptImportPreviewError(error.code) from None
        except Exception:
            raise PromptImportPreviewError(
                'PROMPT_PREVIEW_VERTEX_UNAVAILABLE') from None

        try:
            exact = self._optional_read(lambda: self.registry_reader.read_exact(
                candidate.vertex_prompt_id, candidate.vertex_version_id))
            active = self._optional_read(lambda: self.registry_reader.read_eligible(
                candidate.task, candidate.prompt_key))
        except OperationStopped:
            raise
        except (ContractError, PromptRegistryReadError) as error:
            raise PromptImportPreviewError(
                getattr(error, 'code',
                        'PROMPT_PREVIEW_REGISTRY_UNAVAILABLE')) from None
        except Exception:
            raise PromptImportPreviewError(
                'PROMPT_PREVIEW_REGISTRY_UNAVAILABLE') from None

        if exact is not None and exact.head.snapshot != candidate:
            raise PromptImportPreviewError(
                'PROMPT_PREVIEW_VERTEX_REGISTRY_MISMATCH')
        if (exact is not None and exact.head.status is PromptLifecycle.ACTIVE
                and active != exact):
            raise PromptImportPreviewError(
                'PROMPT_PREVIEW_ACTIVE_STATE_CONFLICT')
        if (active is not None
                and active.head.snapshot.snapshot_sha256
                == candidate.snapshot_sha256
                and active != exact):
            raise PromptImportPreviewError(
                'PROMPT_PREVIEW_ACTIVE_STATE_CONFLICT')

        proposal = None
        status = PromptImportPreviewStatus.EXISTING
        if exact is None:
            status = PromptImportPreviewStatus.UNREGISTERED
            proposal = initial_registration_state(initial_prompt_record(
                candidate, imported_at=request.previewed_at,
                imported_by=request.proposed_by))
        differences = canonical_diff(
            None if active is None else active.head.snapshot, candidate)
        digest = _preview_digest(status, request, candidate, exact, active,
                                 proposal, differences)
        return PromptImportPreview(status, request, candidate, exact, active,
                                   proposal, differences, digest)
