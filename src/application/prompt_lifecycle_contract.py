"""Batch 15 supplied lifecycle plans and explicit shared-control storage.

Pure proposals are not access grants or commit proof. Registry selection is global
per prompt key; workspace authorization does not narrow the ACTIVE uniqueness scope.
"""
from dataclasses import dataclass, field as derived
from datetime import datetime
from types import MappingProxyType

from src.application.identity import VerifiedIdentity
from src.application.prompt_contract import (
    PromptLifecycle, PromptTask, propose_prompt_transition,
)
from src.application.prompt_evaluation_contract import _hash, _utc, validate_promotion_evaluation
from src.application.prompt_evaluation_store import EvaluationSnapshot
from src.application.prompt_evaluation_storage import EvaluationControl, encode_evaluation
from src.application.prompt_registry_codec import PromptRegistryState, MAX_PROMPT_HISTORY, normalize
from src.application.prompt_registry_reader import PromptRegistryReadRequest
from src.application.workflow_contract import _digest, _id, _require
from src.application.workflow_schema import TableSpec, field

STORAGE_VERSION = 'prompt-lifecycle-control-v1'
MAX_CONTROL_HISTORY = 128
MAX_NATIVE_SLOTS = 3
CONTROL_FIELDS = (
    field('schema_version'), field('prompt_key'), field('task'),
    field('revision', 'INTEGER'), field('parent_digest', mode='NULLABLE'),
    field('control_digest'), field('active_snapshot_sha256', mode='NULLABLE'),
    field('command_digest'), field('transition_digests', mode='REPEATED'),
    field('workspace_id'), field('changed_at', 'TIMESTAMP'), field('changed_by'),
)
TABLES = MappingProxyType({spec.name: spec for spec in (
    TableSpec('prompt_lifecycle_controls', CONTROL_FIELDS, ('prompt_key',), ('prompt_key', 'task')),
    TableSpec('prompt_lifecycle_history', CONTROL_FIELDS, ('prompt_key', 'revision'),
        ('prompt_key', 'revision'), append_only=True),
)})


@dataclass(frozen=True, slots=True)
class LifecycleControl:
    prompt_key: str
    task: PromptTask
    revision: int
    parent_digest: str | None
    active_snapshot_sha256: str | None
    command_digest: str
    transition_digests: tuple[str, ...]
    workspace_id: str
    changed_at: datetime
    changed_by: str
    digest: str = derived(init=False)

    def __post_init__(self):
        for value in (self.prompt_key, self.workspace_id):
            _id(value)
        _id(self.changed_by, maximum=256)
        _require(isinstance(self.task, PromptTask) and type(self.revision) is int
                 and 1 <= self.revision <= MAX_CONTROL_HISTORY, 'INVALID_LIFECYCLE_CONTROL')
        if self.revision == 1:
            _require(self.parent_digest is None, 'INVALID_LIFECYCLE_CONTROL_PARENT')
        else:
            _digest(self.parent_digest)
        if self.active_snapshot_sha256 is not None:
            _digest(self.active_snapshot_sha256)
        _digest(self.command_digest)
        _require(type(self.transition_digests) is tuple and 1 <= len(self.transition_digests) <= 2
                 and self.transition_digests == tuple(sorted(set(self.transition_digests))),
                 'INVALID_LIFECYCLE_TRANSITION_PINS')
        for value in self.transition_digests:
            _digest(value)
        object.__setattr__(self, 'changed_at', _utc(self.changed_at))
        object.__setattr__(self, 'digest', _hash(self.payload()))

    def payload(self):
        return dict(schema_version=STORAGE_VERSION, prompt_key=self.prompt_key, task=self.task.value,
            revision=self.revision, parent_digest=self.parent_digest,
            active_snapshot_sha256=self.active_snapshot_sha256, command_digest=self.command_digest,
            transition_digests=list(self.transition_digests), workspace_id=self.workspace_id,
            changed_at=self.changed_at.isoformat(), changed_by=self.changed_by)


def encode_control(control):
    _require(isinstance(control, LifecycleControl), 'INVALID_LIFECYCLE_CONTROL')
    return control.payload() | dict(changed_at=control.changed_at, control_digest=control.digest)


def decode_control(row):
    row = normalize(row)
    _require(type(row) is dict and set(row) == {f.name for f in CONTROL_FIELDS},
             'INVALID_LIFECYCLE_CONTROL_SHAPE')
    _require(row['schema_version'] == STORAGE_VERSION and type(row['transition_digests']) is list,
             'INVALID_LIFECYCLE_CONTROL_STORAGE')
    control = LifecycleControl(row['prompt_key'], PromptTask(row['task']), row['revision'],
        row['parent_digest'], row['active_snapshot_sha256'], row['command_digest'],
        tuple(row['transition_digests']), row['workspace_id'], row['changed_at'], row['changed_by'])
    _require(encode_control(control) == row, 'LIFECYCLE_CONTROL_HASH_MISMATCH')
    return control


def validate_control_history(control, history):
    _require(type(history) is tuple, 'INVALID_LIFECYCLE_CONTROL_HISTORY')
    if control is None:
        _require(not history, 'ORPHAN_LIFECYCLE_HISTORY')
        return
    _require(isinstance(control, LifecycleControl) and 1 <= len(history) <= MAX_CONTROL_HISTORY
             and history[-1] == control, 'INVALID_LIFECYCLE_CONTROL_HISTORY')
    for i, item in enumerate(history, 1):
        _require(isinstance(item, LifecycleControl) and item.revision == i
                 and item.prompt_key == control.prompt_key and item.task is control.task,
                 'INVALID_LIFECYCLE_CONTROL_HISTORY')
        if i > 1:
            _require(item.parent_digest == history[i-2].digest
                     and item.changed_at >= history[i-2].changed_at,
                     'LIFECYCLE_CONTROL_CHAIN_MISMATCH')


@dataclass(frozen=True, slots=True)
class LifecycleReadRequest:
    workspace_id: str
    prompt_key: str
    task: PromptTask
    vertex_prompt_id: str
    vertex_version_id: str
    identity: VerifiedIdentity
    membership_revision: int
    evaluation_id: str | None = None

    def __post_init__(self):
        _id(self.workspace_id)
        _id(self.prompt_key)
        PromptRegistryReadRequest.exact(self.vertex_prompt_id, self.vertex_version_id)
        _require(isinstance(self.task, PromptTask) and isinstance(self.identity, VerifiedIdentity)
                 and type(self.membership_revision) is int and self.membership_revision >= 1,
                 'INVALID_LIFECYCLE_READ_REQUEST')
        if self.evaluation_id is not None:
            _id(self.evaluation_id)


@dataclass(frozen=True, slots=True)
class LifecycleSnapshot:
    request: LifecycleReadRequest
    control: LifecycleControl | None
    control_history: tuple[LifecycleControl, ...]
    target: PromptRegistryState
    active: PromptRegistryState | None
    evaluation: EvaluationSnapshot | None
    observed_at: datetime
    evaluation_control_anchor: EvaluationControl | None = None

    def __post_init__(self):
        _require(isinstance(self.request, LifecycleReadRequest)
                 and isinstance(self.target, PromptRegistryState), 'INVALID_LIFECYCLE_SNAPSHOT')
        object.__setattr__(self, 'observed_at', _utc(self.observed_at))
        validate_control_history(self.control, self.control_history)
        expected = self.request
        target = self.target.head.snapshot
        _require(target.prompt_key == expected.prompt_key and target.task is expected.task
                 and target.vertex_prompt_id == expected.vertex_prompt_id
                 and target.vertex_version_id == expected.vertex_version_id
                 and self.target.head.changed_at <= self.observed_at, 'LIFECYCLE_TARGET_SCOPE_MISMATCH')
        if self.active is not None:
            _require(isinstance(self.active, PromptRegistryState)
                     and self.active.head.status is PromptLifecycle.ACTIVE
                     and self.active.head.snapshot.prompt_key == expected.prompt_key
                     and self.active.head.snapshot.task is expected.task
                     and self.active.head.changed_at <= self.observed_at, 'INVALID_LIFECYCLE_ACTIVE')
            if self.active.head.snapshot == target:
                _require(self.active == self.target, 'LIFECYCLE_ACTIVE_TARGET_MISMATCH')
        if self.target.head.status is PromptLifecycle.ACTIVE:
            _require(self.active == self.target, 'LIFECYCLE_ACTIVE_TARGET_MISMATCH')
        active_sha = None if self.active is None else self.active.head.snapshot.snapshot_sha256
        if self.control is None:
            _require(active_sha is None, 'LIFECYCLE_GENESIS_ACTIVE_CONFLICT')
        else:
            _require(self.control.prompt_key == expected.prompt_key and self.control.task is expected.task
                     and self.control.active_snapshot_sha256 == active_sha
                     and self.control.changed_at <= self.observed_at, 'LIFECYCLE_HEAD_ACTIVE_MISMATCH')
        if expected.evaluation_id is None:
            _require(self.evaluation is None and self.evaluation_control_anchor is None,
                     'UNEXPECTED_LIFECYCLE_EVALUATION')
        else:
            _require(isinstance(self.evaluation, EvaluationSnapshot)
                     and self.evaluation.request.workspace_id == expected.workspace_id
                     and self.evaluation.request.prompt_key == expected.prompt_key
                     and self.evaluation.request.identity == expected.identity
                     and self.evaluation.request.membership_revision == expected.membership_revision
                     and self.evaluation.request.evaluation_id == expected.evaluation_id
                     and self.evaluation.registry_state == self.target
                     and self.evaluation.observed_at == self.observed_at,
                     'LIFECYCLE_EVALUATION_SCOPE_MISMATCH')
            _require(isinstance(self.evaluation_control_anchor, EvaluationControl)
                     and self.evaluation_control_anchor in self.evaluation.control_history,
                     'LIFECYCLE_EVALUATION_CONTROL_ANCHOR_MISSING')
            encode_evaluation(self.evaluation_control_anchor, self.evaluation.record)


@dataclass(frozen=True, slots=True)
class LifecycleWrite:
    capture: LifecycleSnapshot
    target_status: PromptLifecycle
    changed_at: datetime
    reason_code: str
    native_slot: int = 1
    ancestors: tuple = ()
    after_states: tuple[PromptRegistryState, ...] = derived(init=False)
    control: LifecycleControl = derived(init=False)
    command_digest: str = derived(init=False)

    def __post_init__(self):
        _require(isinstance(self.capture, LifecycleSnapshot)
                 and isinstance(self.target_status, PromptLifecycle), 'INVALID_LIFECYCLE_WRITE')
        capture = self.capture
        request = capture.request
        actor = request.identity.principal.principal_id
        at = _utc(self.changed_at)
        object.__setattr__(self, 'changed_at', at)
        _require(at >= capture.observed_at, 'LIFECYCLE_CAPTURE_TIME_REGRESSION')
        promotion = self.target_status in {PromptLifecycle.EVALUATED, PromptLifecycle.ACTIVE}
        evaluation_id = capture.target.head.evaluation_id
        if promotion:
            _require(capture.evaluation is not None and capture.evaluation.control is not None,
                     'LIFECYCLE_EVALUATION_REQUIRED')
            source = capture.evaluation
            gate = validate_promotion_evaluation(capture.target, source.record,
                policy=source.control.policy, benchmark=source.control.benchmark,
                target_status=self.target_status, checked_at=at)
            evaluation_id = gate.evaluation_id
        else:
            _require(capture.evaluation is None, 'UNEXPECTED_LIFECYCLE_EVALUATION')
        head = propose_prompt_transition(capture.target.head, status=self.target_status,
            changed_at=at, changed_by=actor, reason_code=self.reason_code, evaluation_id=evaluation_id)
        states = [PromptRegistryState(head, capture.target.history + (head,))]
        active_sha = None if capture.active is None else capture.active.head.snapshot.snapshot_sha256
        if self.target_status is PromptLifecycle.ACTIVE:
            active_sha = head.snapshot.snapshot_sha256
            if capture.active is not None:
                _require(capture.active.head.snapshot != head.snapshot, 'LIFECYCLE_ALREADY_ACTIVE')
                old = propose_prompt_transition(capture.active.head, status=PromptLifecycle.RETIRED,
                    changed_at=at, changed_by=actor, reason_code='REPLACED_BY_ACTIVE_VERSION',
                    evaluation_id=capture.active.head.evaluation_id)
                states.append(PromptRegistryState(old, capture.active.history + (old,)))
        elif capture.active == capture.target:
            active_sha = None
        after = tuple(sorted(states, key=lambda state: state.head.snapshot.snapshot_sha256))
        command = dict(version=STORAGE_VERSION, workspace_id=request.workspace_id,
            prompt_key=request.prompt_key, task=request.task.value, actor=actor,
            before_control=None if capture.control is None else capture.control.digest,
            before_registry=[state.head.digest for state in self.before_states],
            active_before=None if capture.active is None else capture.active.head.digest,
            after_registry=[state.head.digest for state in after],
            evaluation_digest=None if capture.evaluation is None else capture.evaluation.record.digest,
            evaluation_anchor_digest=None if capture.evaluation_control_anchor is None
                else capture.evaluation_control_anchor.digest,
            evaluation_control_digest=None if capture.evaluation is None else capture.evaluation.control.digest)
        digest = _hash(command)
        control = LifecycleControl(request.prompt_key, request.task,
            1 if capture.control is None else capture.control.revision + 1,
            None if capture.control is None else capture.control.digest, active_sha, digest,
            tuple(sorted(state.head.digest for state in after)), request.workspace_id, at, actor)
        object.__setattr__(self, 'after_states', after)
        object.__setattr__(self, 'command_digest', digest)
        object.__setattr__(self, 'control', control)
        _require(type(self.native_slot) is int and 1 <= self.native_slot <= MAX_NATIVE_SLOTS
                 and type(self.ancestors) is tuple and len(self.ancestors)==self.native_slot-1,
                 'INVALID_LIFECYCLE_NATIVE_SLOT')
        for i, previous in enumerate(self.ancestors, 1):
            _require(isinstance(previous, LifecycleWrite) and previous.native_slot==i
                     and previous.ancestors==self.ancestors[:i-1]
                     and previous.capture==self.capture and previous.target_status is self.target_status
                     and previous.changed_at==self.changed_at and previous.reason_code==self.reason_code
                     and previous.control==self.control, 'LIFECYCLE_RETRY_REBOUND')

    @property
    def before_states(self):
        states = [self.capture.target]
        if (self.target_status is PromptLifecycle.ACTIVE and self.capture.active is not None
                and self.capture.active != self.capture.target):
            states.append(self.capture.active)
        return tuple(sorted(states, key=lambda state: state.head.snapshot.snapshot_sha256))

    @property
    def job_id(self):
        # Global logical slot, independent of workspace, actor or candidate content.
        base = 'prompt_lifecycle_write_' + _hash([self.control.prompt_key, self.control.revision])
        return base if self.native_slot==1 else base + '_slot_' + str(self.native_slot)
