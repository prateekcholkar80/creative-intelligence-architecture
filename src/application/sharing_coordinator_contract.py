"""Offline sharing coordinator proposals; no queries, locks or commit proof.

Rows/commit observations must come from the future trusted transaction adapter.
Receipt lookup uses the full hashed retry prefix and a bounded original intent
window. Only atomically committed APPLIED audits may use that reserved prefix.
Absence of a receipt cannot resolve an uncertain submission into a retry grant.
"""
from __future__ import annotations

import base64
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
from types import MappingProxyType

from src.application.access_policy import (
    AccessDecision, Action, POLICY_VERSION, Principal, Reason, Visibility, require_authorized,
)
from src.application.audit_contract import (
    AuditTarget, MutationKind, MutationObservation, MutationOutcome, ResourceType,
    _opaque, validate_audit_event,
)
from src.application.conversation_contract import (
    ConversationChange, ConversationStatus, propose_conversation_visibility,
    validate_conversation, validate_conversation_change,
)
from src.application.membership_contract import validate_membership
from src.application.workflow_contract import ContractError, RETRY_WINDOW, _digest, _require, _shape, canonical_json_bytes
from src.application.workflow_schema import SCHEMA_VERSION, TABLES
from src.application.workspace_contract import (
    MAX_REVISION, WorkspaceChange, WorkspaceStatus, _revision as _workspace_revision, _time,
    propose_workspace_fence, validate_workspace, validate_workspace_change,
)

SHARING_CONTRACT_VERSION = 'sharing-coordinator-v1'
RECEIPT_NAMESPACE = 'share-v1:'


def _utc(value: datetime) -> datetime:
    _time(value)
    return value.astimezone(timezone.utc)


def _sha(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _encoded_digest(value: str) -> str:
    return base64.urlsafe_b64encode(bytes.fromhex(value)).decode('ascii').rstrip('=')


def _intent_revision(value: int) -> None:
    _require(type(value) is int and 1 <= value <= MAX_REVISION, 'INVALID_SHARING_REVISION')


class SharingCommitStatus(str, Enum):
    NOT_SUBMITTED = 'NOT_SUBMITTED'
    COMMITTED = 'COMMITTED'
    NOT_COMMITTED = 'NOT_COMMITTED'
    UNKNOWN = 'UNKNOWN'


class SharingPreparationState(str, Enum):
    PREPARED = 'PREPARED'
    REUSED = 'REUSED'
    UNKNOWN = 'UNKNOWN'


@dataclass(frozen=True, slots=True)
class SharingCommand:
    workspace_id: str
    conversation_id: str
    principal_id: str
    visibility: Visibility
    expected_membership_revision: int
    expected_conversation_revision: int
    retry_key_sha256: str
    accepted_at: datetime
    trace_id: str
    version: str = SHARING_CONTRACT_VERSION

    def __post_init__(self):
        for value in (self.workspace_id, self.conversation_id, self.trace_id):
            _opaque(value)
        _opaque(self.principal_id, 256)
        _digest(self.retry_key_sha256)
        _intent_revision(self.expected_membership_revision)
        _intent_revision(self.expected_conversation_revision)
        _require(self.expected_conversation_revision < MAX_REVISION, 'CONVERSATION_REVISION_EXHAUSTED')
        _require(isinstance(self.visibility, Visibility), 'INVALID_CONVERSATION_VISIBILITY')
        _require(self.version == SHARING_CONTRACT_VERSION, 'INVALID_SHARING_VERSION')
        accepted = _utc(self.accepted_at)
        try:
            accepted + RETRY_WINDOW
        except OverflowError:
            raise ContractError('INVALID_SHARING_WINDOW') from None

    @property
    def deadline_at(self) -> datetime:
        return _utc(self.accepted_at) + RETRY_WINDOW

    @property
    def command_sha256(self) -> str:
        # Server-issued intent time/trace stay fixed across transport attempts.
        # The operational workspace fence is separately refreshed per attempt.
        return _sha(dict(version=self.version, policy_version=POLICY_VERSION,
            action=Action.SET_VISIBILITY.value, workspace_id=self.workspace_id,
            conversation_id=self.conversation_id, principal_id=self.principal_id,
            visibility=self.visibility.value, membership_revision=self.expected_membership_revision,
            conversation_revision=self.expected_conversation_revision,
            accepted_at=_utc(self.accepted_at).isoformat(), trace_id=self.trace_id))

    @property
    def audit_prefix(self) -> str:
        scoped_key = _sha([self.version, Action.SET_VISIBILITY.value, self.workspace_id,
                          self.principal_id, self.conversation_id, self.retry_key_sha256])
        return RECEIPT_NAMESPACE + _encoded_digest(scoped_key) + ':'

    @property
    def audit_id(self) -> str:
        # Full two SHA-256 digests fit the audit validator's 128-character bound.
        return self.audit_prefix + _encoded_digest(self.command_sha256)


def make_sharing_command(*, workspace_id: str, conversation_id: str, principal_id: str,
                         visibility: Visibility, expected_membership_revision: int,
                         expected_conversation_revision: int, accepted_at: datetime,
                         trace_id: str, retry_key: str) -> SharingCommand:
    """Hash a bounded opaque retry key; never retain its raw value in the command."""
    _opaque(retry_key)
    return SharingCommand(workspace_id, conversation_id, principal_id, visibility,
                          expected_membership_revision, expected_conversation_revision,
                          _sha(retry_key), accepted_at, trace_id)


@dataclass(frozen=True, slots=True)
class SharingCommitObservation:
    audit_id: str
    status: SharingCommitStatus
    query_job_id: str | None = None

    def __post_init__(self):
        _opaque(self.audit_id)
        _require(isinstance(self.status, SharingCommitStatus), 'INVALID_SHARING_COMMIT_STATUS')
        if self.status is SharingCommitStatus.NOT_SUBMITTED:
            _require(self.query_job_id is None, 'INVALID_SHARING_QUERY_ID')
        else:
            # Preallocate the query ID before submission, including a lost response.
            _opaque(self.query_job_id)


def _historical_decision(command: SharingCommand) -> AccessDecision:
    """Expected original audit metadata; never use this as current authorization."""
    return AccessDecision(True, Action.SET_VISIBILITY, Reason.ALLOWED_OWNER,
        command.workspace_id, command.principal_id, command.expected_membership_revision,
        command.conversation_id, command.expected_conversation_revision)


def _audit_context(command: SharingCommand, occurred_at: datetime, recorded_at: datetime) -> dict:
    return dict(observation=_historical_decision(command),
        target=AuditTarget(ResourceType.CONVERSATION, command.conversation_id, command.workspace_id,
            revision=command.expected_conversation_revision, conversation_id=command.conversation_id,
            owner_id=command.principal_id),
        mutation=MutationObservation(MutationKind.UPDATE, MutationOutcome.APPLIED,
                                     command.expected_conversation_revision + 1),
        observed_at=occurred_at, recorded_at=recorded_at, trace_id=command.trace_id)


def _audit_candidate(command: SharingCommand, observed_at: datetime) -> dict:
    context = _audit_context(command, observed_at, observed_at)
    decision, target = context['observation'], context['target']
    row = dict.fromkeys(field.name for field in TABLES['audit_events'].schema)
    row.update(schema_version=SCHEMA_VERSION, workspace_id=command.workspace_id,
        audit_id=command.audit_id, actor_id=command.principal_id, actor_kind='USER',
        action=decision.action.value, allowed=True, reason_code=decision.reason.value,
        policy_version=decision.policy_version, membership_revision=decision.membership_revision,
        resource_type=target.resource_type.value, resource_id=target.resource_id,
        conversation_id=command.conversation_id, owner_id=command.principal_id,
        resource_revision=command.expected_conversation_revision,
        new_revision=command.expected_conversation_revision + 1,
        mutation_outcome=MutationOutcome.APPLIED.value, trace_id=command.trace_id, occurred_at=observed_at)
    validate_audit_event(row, **context)
    return row


def _frozen_row(row: Mapping, table: str) -> Mapping:
    _require(isinstance(row, Mapping), 'INVALID_ROW_SHAPE')
    copied = deepcopy(dict(row))
    _shape(copied, TABLES[table].schema)  # These three schemas contain only scalar values.
    _require(all(value is None or isinstance(value, (str, int, bool, datetime))
                 for value in copied.values()), 'INVALID_SHARING_PLAN_SCHEMA')
    return MappingProxyType(copied)


@dataclass(frozen=True, slots=True)
class SharingPlan:
    command: SharingCommand
    observed_at: datetime
    expected_workspace_revision: int
    workspace_before: Mapping
    workspace_after: Mapping
    conversation_before: Mapping
    conversation_after: Mapping
    audit_candidate: Mapping

    def __post_init__(self):
        _require(isinstance(self.command, SharingCommand), 'INVALID_SHARING_COMMAND')
        _within_window(self.command, self.observed_at)
        for name, table in (('workspace_before', 'workspaces'), ('workspace_after', 'workspaces'),
                            ('conversation_before', 'conversations'), ('conversation_after', 'conversations'),
                            ('audit_candidate', 'audit_events')):
            object.__setattr__(self, name, _frozen_row(getattr(self, name), table))
        validate_workspace_change(dict(self.workspace_before), dict(self.workspace_after),
            workspace_id=self.command.workspace_id, expected_revision=self.expected_workspace_revision,
            observed_at=self.observed_at, change=WorkspaceChange.FENCE)
        validate_conversation_change(dict(self.conversation_before), dict(self.conversation_after),
            workspace_id=self.command.workspace_id, conversation_id=self.command.conversation_id,
            expected_revision=self.command.expected_conversation_revision,
            observed_at=self.observed_at, change=ConversationChange.SET_VISIBILITY)
        _require(self.conversation_after['visibility'] == self.command.visibility.value
                 and self.conversation_before['owner_id'] == self.command.principal_id,
                 'SHARING_PLAN_SCOPE_MISMATCH')
        _require(dict(self.audit_candidate) == _audit_candidate(self.command, self.observed_at),
                 'SHARING_PLAN_AUDIT_MISMATCH')


@dataclass(frozen=True, slots=True)
class SharingPreparation:
    state: SharingPreparationState
    plan: SharingPlan | None = None
    receipt: Mapping | None = None

    def __post_init__(self):
        _require(isinstance(self.state, SharingPreparationState), 'INVALID_SHARING_PREPARATION')
        if self.state is SharingPreparationState.PREPARED:
            _require(isinstance(self.plan, SharingPlan) and self.receipt is None, 'INVALID_SHARING_PREPARATION')
        elif self.state is SharingPreparationState.REUSED:
            _require(self.plan is None and self.receipt is not None, 'INVALID_SHARING_PREPARATION')
            object.__setattr__(self, 'receipt', _frozen_row(self.receipt, 'audit_events'))
            _require(self.receipt['allowed'] is True and self.receipt['actor_kind'] == 'USER'
                     and self.receipt['action'] == Action.SET_VISIBILITY.value
                     and self.receipt['mutation_outcome'] == MutationOutcome.APPLIED.value
                     and type(self.receipt['resource_revision']) is int
                     and self.receipt['resource_revision'] > 0
                     and self.receipt['new_revision'] == self.receipt['resource_revision'] + 1,
                     'INVALID_SHARING_PREPARATION')
        else:
            _require(self.plan is None and self.receipt is None, 'INVALID_SHARING_PREPARATION')

    @property
    def resulting_revision(self) -> int | None:
        return self.receipt['new_revision'] if self.receipt is not None else None


def _within_window(command: SharingCommand, observed_at: datetime) -> None:
    instant = _utc(observed_at)
    _require(_utc(command.accepted_at) <= instant, 'SHARING_INTENT_NOT_YET_VALID')
    _require(instant < command.deadline_at, 'SHARING_RETRY_EXPIRED')


def _one(rows: list[dict], label: str, *, optional: bool = False) -> dict | None:
    _require(isinstance(rows, list) and len(rows) <= 2, 'INVALID_SHARING_LOOKUP')
    _require(len(rows) < 2, 'DUPLICATE_SHARING_' + label)
    _require(optional or bool(rows), 'MISSING_SHARING_' + label)
    if rows:
        _require(isinstance(rows[0], dict), 'INVALID_ROW_SHAPE')
    return rows[0] if rows else None


def prepare_sharing(command: SharingCommand, *, workspace_rows: list[dict], membership_rows: list[dict],
                    conversation_rows: list[dict], receipt_rows: list[dict], expected_workspace_revision: int,
                    observed_at: datetime, commit_observation: SharingCommitObservation) -> SharingPreparation:
    """Bind one atomic proposal or resolve a supplied matching committed receipt.

    All row sets are scoped, bounded fresh transaction reads in the future
    adapter. NOT_COMMITTED must mean a positively known aborted/not-executed job,
    never an exception string or a missing receipt. UNKNOWN/COMMITTED without a
    matching receipt produces UNKNOWN and no write proposal. REUSED does not
    claim the historical visibility is still current or rewrite any rows.
    """
    _require(isinstance(command, SharingCommand), 'INVALID_SHARING_COMMAND')
    _require(isinstance(commit_observation, SharingCommitObservation), 'INVALID_SHARING_COMMIT_STATUS')
    _require(commit_observation.audit_id == command.audit_id, 'SHARING_COMMIT_SCOPE_MISMATCH')
    _workspace_revision(expected_workspace_revision)
    _within_window(command, observed_at)
    control = _one(workspace_rows, 'WORKSPACE')
    member_row = _one(membership_rows, 'MEMBERSHIP')
    conversation = _one(conversation_rows, 'CONVERSATION')
    receipt = _one(receipt_rows, 'RECEIPT', optional=True)
    validate_workspace(control, workspace_id=command.workspace_id)
    _require(control['status'] == WorkspaceStatus.ACTIVE.value, 'WORKSPACE_INACTIVE')
    member = validate_membership(member_row, workspace_id=command.workspace_id, principal_id=command.principal_id)
    resource = validate_conversation(conversation, workspace_id=command.workspace_id,
                                     conversation_id=command.conversation_id)
    _require(conversation['status'] == ConversationStatus.ACTIVE.value, 'CONVERSATION_ARCHIVED')
    _require(max(row['updated_at'] for row in (control, member_row, conversation)) <= observed_at,
             'INVALID_SHARING_OBSERVATION_TIME')
    # Current access precedes historical receipt reuse; the old audit is not a grant.
    require_authorized(Principal(command.principal_id), member, Action.SET_VISIBILITY,
                       workspace_id=command.workspace_id, resource=resource)
    if receipt is not None:
        _shape(receipt, TABLES['audit_events'].schema)
        _require(receipt['workspace_id'] == command.workspace_id
                 and receipt['audit_id'].startswith(command.audit_prefix), 'SHARING_RECEIPT_SCOPE_MISMATCH')
        _require(receipt['audit_id'] == command.audit_id, 'SHARING_IDEMPOTENCY_CONFLICT')
        _time(receipt['occurred_at'])
        _require(_utc(command.accepted_at) <= receipt['occurred_at'] < command.deadline_at
                 and receipt['occurred_at'] <= observed_at, 'INVALID_SHARING_RECEIPT_TIME')
        validate_audit_event(receipt, **_audit_context(command, receipt['occurred_at'], observed_at))
        _require(conversation['access_revision'] >= receipt['new_revision']
                 and conversation['created_at'] <= receipt['occurred_at'] <= conversation['updated_at'],
                 'INCONSISTENT_SHARING_RECEIPT')
        if conversation['access_revision'] == receipt['new_revision']:
            _require(conversation['visibility'] == command.visibility.value, 'INCONSISTENT_SHARING_RECEIPT')
        _require(commit_observation.status is not SharingCommitStatus.NOT_COMMITTED,
                 'SHARING_COMMIT_OBSERVATION_CONFLICT')
        return SharingPreparation(SharingPreparationState.REUSED, receipt=receipt)
    if commit_observation.status in {SharingCommitStatus.UNKNOWN, SharingCommitStatus.COMMITTED}:
        return SharingPreparation(SharingPreparationState.UNKNOWN)
    _require(member.revision == command.expected_membership_revision, 'STALE_MEMBERSHIP_REVISION')
    proposed = propose_conversation_visibility(conversation, visibility=command.visibility,
        workspace=control, workspace_id=command.workspace_id, conversation_id=command.conversation_id,
        expected_workspace_revision=expected_workspace_revision,
        expected_revision=command.expected_conversation_revision, actor=Principal(command.principal_id),
        actor_membership=member_row, observed_at=observed_at)
    fenced = propose_workspace_fence(control, workspace_id=command.workspace_id,
                                     expected_revision=expected_workspace_revision, observed_at=observed_at)
    plan = SharingPlan(command, observed_at, expected_workspace_revision, control, fenced,
                       conversation, proposed, _audit_candidate(command, observed_at))
    return SharingPreparation(SharingPreparationState.PREPARED, plan=plan)
