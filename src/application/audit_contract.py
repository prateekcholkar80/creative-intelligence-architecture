"""Pure audit contracts; no identity verification, authorization grant or writes.

Inputs are captured server observations, never browser/prompt metadata. Domain
validators, fresh access and transaction fencing must establish the operation
separately. An APPLIED observation is not independently verified commit proof.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re
from types import MappingProxyType

from src.application.access_policy import AccessDecision, Action, POLICY_VERSION, Reason
from src.application.workflow_contract import ContractError, _integer, _require, _shape, _timestamp
from src.application.workflow_schema import SCHEMA_VERSION, TABLES

AUDIT_POLICY_VERSION = 'workflow-audit-v1'


class ActorKind(str, Enum):
    USER = 'USER'
    ANONYMOUS = 'ANONYMOUS'
    SERVICE = 'SERVICE'


class ResourceType(str, Enum):
    WORKSPACE = 'WORKSPACE'
    MEMBERSHIP = 'MEMBERSHIP'
    CONVERSATION = 'CONVERSATION'
    REQUEST = 'REQUEST'
    JOB = 'JOB'
    JOB_ATTEMPT = 'JOB_ATTEMPT'
    STAGE_EVENT = 'STAGE_EVENT'
    MODEL_INVOCATION = 'MODEL_INVOCATION'
    OUTPUT = 'OUTPUT'
    EVIDENCE_SNAPSHOT = 'EVIDENCE_SNAPSHOT'


class MutationKind(str, Enum):
    CREATE = 'CREATE'
    UPDATE = 'UPDATE'
    APPEND = 'APPEND'


class MutationOutcome(str, Enum):
    APPLIED = 'APPLIED'
    NO_CHANGE = 'NO_CHANGE'
    REJECTED = 'REJECTED'
    UNKNOWN = 'UNKNOWN'


class WorkflowAction(str, Enum):
    CREATE_REQUEST = 'CREATE_REQUEST'
    CREATE_JOB = 'CREATE_JOB'
    CLAIM_ATTEMPT = 'CLAIM_ATTEMPT'
    UPDATE_JOB = 'UPDATE_JOB'
    UPDATE_ATTEMPT = 'UPDATE_ATTEMPT'
    CREATE_MODEL_INVOCATION = 'CREATE_MODEL_INVOCATION'
    CLAIM_MODEL_DISPATCH = 'CLAIM_MODEL_DISPATCH'
    RECORD_MODEL_RESULT = 'RECORD_MODEL_RESULT'
    RECORD_MODEL_ACCOUNTING = 'RECORD_MODEL_ACCOUNTING'
    APPEND_STAGE_EVENT = 'APPEND_STAGE_EVENT'
    APPEND_EVIDENCE = 'APPEND_EVIDENCE'
    APPEND_OUTPUT = 'APPEND_OUTPUT'


class WorkflowReason(str, Enum):
    TRANSITION_VALIDATED = 'TRANSITION_VALIDATED'
    TRANSITION_REJECTED = 'TRANSITION_REJECTED'
    OUTCOME_UNKNOWN = 'OUTCOME_UNKNOWN'


_SERVICE_TARGETS = MappingProxyType({
    WorkflowAction.CREATE_REQUEST: (ResourceType.REQUEST, MutationKind.CREATE),
    WorkflowAction.CREATE_JOB: (ResourceType.JOB, MutationKind.CREATE),
    WorkflowAction.CLAIM_ATTEMPT: (ResourceType.JOB_ATTEMPT, MutationKind.CREATE),
    WorkflowAction.UPDATE_JOB: (ResourceType.JOB, MutationKind.UPDATE),
    WorkflowAction.UPDATE_ATTEMPT: (ResourceType.JOB_ATTEMPT, MutationKind.UPDATE),
    WorkflowAction.CREATE_MODEL_INVOCATION: (ResourceType.MODEL_INVOCATION, MutationKind.CREATE),
    WorkflowAction.CLAIM_MODEL_DISPATCH: (ResourceType.MODEL_INVOCATION, MutationKind.UPDATE),
    WorkflowAction.RECORD_MODEL_RESULT: (ResourceType.MODEL_INVOCATION, MutationKind.UPDATE),
    WorkflowAction.RECORD_MODEL_ACCOUNTING: (ResourceType.MODEL_INVOCATION, MutationKind.UPDATE),
    WorkflowAction.APPEND_STAGE_EVENT: (ResourceType.STAGE_EVENT, MutationKind.APPEND),
    WorkflowAction.APPEND_EVIDENCE: (ResourceType.EVIDENCE_SNAPSHOT, MutationKind.APPEND),
    WorkflowAction.APPEND_OUTPUT: (ResourceType.OUTPUT, MutationKind.APPEND),
})
_APPEND_TYPES = frozenset({ResourceType.STAGE_EVENT, ResourceType.EVIDENCE_SNAPSHOT, ResourceType.OUTPUT})
_READ_ACTIONS = frozenset({Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT})
_RESOURCE_ACTIONS = _READ_ACTIONS | {Action.REFINE_DRAFT, Action.REVIEW_FOR_POC, Action.SET_VISIBILITY,Action.ARCHIVE_CONVERSATION}
_ALLOW_REASONS = frozenset({Reason.ALLOWED_WORKSPACE_ACTION, Reason.ALLOWED_OWNER, Reason.ALLOWED_TEAM})


def _opaque(value: object, maximum: int = 128) -> None:
    """Allow normalized IAP/worker IDs; reject email, JWT, URI and free-text forms."""
    _require(isinstance(value, str) and 1 <= len(value) <= maximum
             and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9:_-]*', value) is not None, 'INVALID_AUDIT_IDENTIFIER')


def _revision(value: object) -> None:
    _integer(value)
    _require(value < 2**63, 'INVALID_INTEGER')


@dataclass(frozen=True, slots=True)
class AuditTarget:
    """Minimal server-resolved target scope, not a resource existence/access proof.

    revision is the pre-mutation resource revision (conversation access_revision
    for conversation targets), not the workspace coordinator's fence revision.
    New versioned resources and append-only targets use None. Denied lookups
    without a trustworthy target supply target=None to omit resource metadata.
    """
    resource_type: ResourceType
    resource_id: str
    workspace_id: str
    revision: int | None = None
    conversation_id: str | None = None
    owner_id: str | None = None
    request_id: str | None = None
    job_id: str | None = None

    def __post_init__(self):
        _require(isinstance(self.resource_type, ResourceType), 'INVALID_AUDIT_RESOURCE')
        _opaque(self.workspace_id)
        _opaque(self.resource_id, 256 if self.resource_type == ResourceType.MEMBERSHIP else 128)
        if self.revision is not None:
            _revision(self.revision)
        for key in ('conversation_id', 'owner_id', 'request_id', 'job_id'):
            if getattr(self, key) is not None:
                _opaque(getattr(self, key), 256 if key == 'owner_id' else 128)
        kind = self.resource_type
        if kind in {ResourceType.WORKSPACE, ResourceType.MEMBERSHIP}:
            _require(all(getattr(self, key) is None for key in ('conversation_id', 'owner_id', 'request_id', 'job_id')),
                     'INVALID_AUDIT_RESOURCE')
            if kind == ResourceType.WORKSPACE:
                _require(self.resource_id == self.workspace_id, 'INVALID_AUDIT_RESOURCE')
        else:
            _require(self.conversation_id is not None and self.owner_id is not None, 'INVALID_AUDIT_RESOURCE')
            if kind == ResourceType.CONVERSATION:
                _require(self.resource_id == self.conversation_id and (self.job_id is None or self.request_id is not None),
                         'INVALID_AUDIT_RESOURCE')
            else:
                _require(self.request_id is not None, 'INVALID_AUDIT_RESOURCE')
                if kind == ResourceType.REQUEST:
                    _require(self.resource_id == self.request_id and self.job_id is None, 'INVALID_AUDIT_RESOURCE')
                elif kind != ResourceType.MODEL_INVOCATION:
                    _require(self.job_id is not None, 'INVALID_AUDIT_RESOURCE')
                if kind == ResourceType.JOB:
                    _require(self.resource_id == self.job_id, 'INVALID_AUDIT_RESOURCE')
        if kind in _APPEND_TYPES:
            _require(self.revision is None, 'INVALID_AUDIT_RESOURCE')


@dataclass(frozen=True, slots=True)
class MutationObservation:
    """Observed result; UNKNOWN has no declared resulting revision or replay grant."""
    kind: MutationKind
    outcome: MutationOutcome
    new_revision: int | None = None

    def __post_init__(self):
        _require(isinstance(self.kind, MutationKind) and isinstance(self.outcome, MutationOutcome),
                 'INVALID_AUDIT_MUTATION')
        if self.new_revision is not None:
            _revision(self.new_revision)


@dataclass(frozen=True, slots=True)
class WorkflowObservation:
    """Captured worker observation; does not authenticate/admit the worker."""
    worker_id: str
    workspace_id: str
    action: WorkflowAction
    reason: WorkflowReason = WorkflowReason.TRANSITION_VALIDATED
    policy_version: str = AUDIT_POLICY_VERSION

    def __post_init__(self):
        _opaque(self.worker_id, 256)
        _opaque(self.workspace_id)
        _require(isinstance(self.action, WorkflowAction) and isinstance(self.reason, WorkflowReason)
                 and self.policy_version == AUDIT_POLICY_VERSION, 'INVALID_AUDIT_OBSERVATION')


@dataclass(frozen=True,slots=True)
class BootstrapObservation:
    """Captured explicit operator grant; never synthesized from ADC/membership."""
    actor_id: str
    workspace_id: str
    manifest_sha256: str
    policy_version: str = 'workspace-bootstrap-v1'

    def __post_init__(self):
        _require(type(self.actor_id) is str and re.fullmatch(r'iap-v1:[0-9a-f]{64}',self.actor_id), 'INVALID_AUDIT_OBSERVATION')
        _opaque(self.workspace_id)
        _require(type(self.manifest_sha256) is str and re.fullmatch(r'[0-9a-f]{64}',self.manifest_sha256)
            and self.policy_version=='workspace-bootstrap-v1','INVALID_AUDIT_OBSERVATION')


def _access_decision(decision: AccessDecision) -> None:
    _require(type(decision.allowed) is bool and isinstance(decision.action, Action)
             and isinstance(decision.reason, Reason) and decision.policy_version == POLICY_VERSION,
             'INVALID_AUDIT_DECISION')
    _opaque(decision.workspace_id)
    if decision.principal_id is not None:
        _opaque(decision.principal_id, 256)
    for revision in (decision.membership_revision, decision.resource_revision):
        if revision is not None:
            _revision(revision)
    if decision.resource_id is not None:
        _opaque(decision.resource_id)
    _require((decision.resource_id is None) == (decision.resource_revision is None), 'INVALID_AUDIT_DECISION')
    _require(decision.allowed == (decision.reason in _ALLOW_REASONS), 'INVALID_AUDIT_DECISION')
    if decision.allowed:
        _require((decision.reason == Reason.ALLOWED_WORKSPACE_ACTION) == (decision.action not in _RESOURCE_ACTIONS),
                 'INVALID_AUDIT_DECISION')
        if decision.action in {Action.REFINE_DRAFT, Action.SET_VISIBILITY,Action.ARCHIVE_CONVERSATION}:
            _require(decision.reason == Reason.ALLOWED_OWNER, 'INVALID_AUDIT_DECISION')
    if decision.reason == Reason.AUTHENTICATION_REQUIRED:
        _require(decision.principal_id is None and decision.membership_revision is None, 'INVALID_AUDIT_DECISION')
    else:
        _require(decision.principal_id is not None, 'INVALID_AUDIT_DECISION')
        if decision.reason == Reason.MEMBERSHIP_REQUIRED:
            _require(decision.membership_revision is None, 'INVALID_AUDIT_DECISION')
        else:
            _require(decision.membership_revision is not None, 'INVALID_AUDIT_DECISION')
    if decision.reason in {Reason.ALLOWED_WORKSPACE_ACTION, Reason.RESOURCE_REQUIRED}:
        _require(decision.resource_id is None, 'INVALID_AUDIT_DECISION')
    if decision.reason in {Reason.ALLOWED_OWNER, Reason.ALLOWED_TEAM, Reason.PRIVATE_RESOURCE,
                           Reason.OWNER_REQUIRED, Reason.UNEXPECTED_RESOURCE}:
        _require(decision.resource_id is not None, 'INVALID_AUDIT_DECISION')
    if decision.action in {Action.APPROVE_BENEFIT_CLAIMS, Action.LAUNCH_CAMPAIGN}:
        _require(not decision.allowed, 'INVALID_AUDIT_DECISION')


def _user_target(decision: AccessDecision, target: AuditTarget | None) -> None:
    if target is None:
        _require(not decision.allowed, 'AUDIT_TARGET_REQUIRED')
        return
    _require(target.workspace_id == decision.workspace_id, 'AUDIT_SCOPE_MISMATCH')
    action = decision.action
    if action in _RESOURCE_ACTIONS:
        # Access is authorized at the conversation, including child downloads.
        # Capture its access_revision, with optional known request/job links.
        _require(target.resource_type == ResourceType.CONVERSATION, 'INVALID_AUDIT_RESOURCE')
        if decision.resource_id is not None:
            _require(target.conversation_id == decision.resource_id, 'AUDIT_SCOPE_MISMATCH')
            _require(target.revision == decision.resource_revision, 'AUDIT_REVISION_MISMATCH')
        if decision.reason == Reason.ALLOWED_OWNER:
            _require(target.owner_id == decision.principal_id, 'AUDIT_SCOPE_MISMATCH')
        elif decision.reason == Reason.ALLOWED_TEAM:
            _require(target.owner_id != decision.principal_id, 'AUDIT_SCOPE_MISMATCH')
        elif decision.reason in {Reason.PRIVATE_RESOURCE, Reason.OWNER_REQUIRED}:
            _require(target.owner_id != decision.principal_id, 'AUDIT_SCOPE_MISMATCH')
    elif action == Action.GENERATE_DRAFT:
        _require(target.resource_type in {ResourceType.WORKSPACE, ResourceType.CONVERSATION, ResourceType.REQUEST, ResourceType.JOB},
                 'INVALID_AUDIT_RESOURCE')
        if decision.allowed and target.owner_id is not None:
            _require(target.owner_id == decision.principal_id, 'AUDIT_SCOPE_MISMATCH')
    elif action == Action.MANAGE_MEMBERS:
        _require(target.resource_type in {ResourceType.WORKSPACE, ResourceType.MEMBERSHIP}, 'INVALID_AUDIT_RESOURCE')
    else:
        _require(target.resource_type == ResourceType.WORKSPACE, 'INVALID_AUDIT_RESOURCE')


def _mutation(target: AuditTarget | None, mutation: MutationObservation | None, allowed: bool) -> None:
    if mutation is None:
        return
    _require(isinstance(mutation, MutationObservation) and target is not None, 'INVALID_AUDIT_MUTATION')
    if not allowed:
        _require(mutation.outcome == MutationOutcome.REJECTED, 'DENIED_AUDIT_HAS_MUTATION')
    if mutation.kind == MutationKind.APPEND:
        _require(target.resource_type in _APPEND_TYPES and target.revision is None
                 and mutation.new_revision is None, 'INVALID_AUDIT_MUTATION')
    elif mutation.kind == MutationKind.CREATE:
        _require(target.resource_type not in _APPEND_TYPES and target.revision is None, 'INVALID_AUDIT_MUTATION')
        _require(mutation.new_revision == 1 if mutation.outcome == MutationOutcome.APPLIED
                 else mutation.new_revision is None, 'AUDIT_REVISION_MISMATCH')
        _require(mutation.outcome != MutationOutcome.NO_CHANGE, 'INVALID_AUDIT_MUTATION')
    else:
        _require(target.resource_type not in _APPEND_TYPES and target.revision is not None, 'INVALID_AUDIT_MUTATION')
        expected = (target.revision + 1 if mutation.outcome == MutationOutcome.APPLIED else
                    target.revision if mutation.outcome == MutationOutcome.NO_CHANGE else None)
        _require(mutation.new_revision == expected, 'AUDIT_REVISION_MISMATCH')


def validate_audit_event(row: dict, *, observation: AccessDecision | WorkflowObservation,
                         target: AuditTarget | None, observed_at: datetime, recorded_at: datetime, trace_id: str,
                         mutation: MutationObservation | None = None) -> None:
    """Validate a complete row against captured server observations, not live truth."""
    _shape(row, TABLES['audit_events'].schema)
    _timestamp(observed_at); _timestamp(recorded_at); _opaque(trace_id)
    _require(row['schema_version'] == SCHEMA_VERSION and row['occurred_at'] == observed_at
             and observed_at <= recorded_at and row['trace_id'] == trace_id, 'AUDIT_OBSERVATION_MISMATCH')
    _opaque(row['audit_id'])
    _require(target is None or isinstance(target, AuditTarget), 'INVALID_AUDIT_RESOURCE')
    if isinstance(observation, AccessDecision):
        _access_decision(observation)
        _user_target(observation, target)
        expected = dict(actor_kind=ActorKind.USER.value if observation.principal_id is not None else ActorKind.ANONYMOUS.value,
            actor_id=observation.principal_id, workspace_id=observation.workspace_id, action=observation.action.value,
            allowed=observation.allowed, reason_code=observation.reason.value, policy_version=observation.policy_version,
            membership_revision=observation.membership_revision)
        if observation.action in _READ_ACTIONS:
            _require(mutation is None, 'READ_AUDIT_HAS_MUTATION')
        if mutation is not None:
            _require(isinstance(mutation, MutationObservation) and target is not None, 'INVALID_AUDIT_MUTATION')
            permitted = {
                Action.SET_VISIBILITY: ({ResourceType.CONVERSATION}, {MutationKind.UPDATE}),
                Action.MANAGE_MEMBERS: ({ResourceType.MEMBERSHIP}, {MutationKind.CREATE, MutationKind.UPDATE}),
                Action.MANAGE_MODELS: ({ResourceType.WORKSPACE}, {MutationKind.UPDATE}),
                Action.MANAGE_LIMITS: ({ResourceType.WORKSPACE}, {MutationKind.UPDATE}),
                Action.MANAGE_WORKSPACE: ({ResourceType.WORKSPACE}, {MutationKind.UPDATE}),
                Action.ARCHIVE_CONVERSATION: ({ResourceType.CONVERSATION}, {MutationKind.UPDATE}),
                Action.GENERATE_DRAFT: ({ResourceType.CONVERSATION, ResourceType.REQUEST, ResourceType.JOB}, {MutationKind.CREATE}),
            }
            kinds = permitted.get(observation.action)
            _require(kinds is not None and target.resource_type in kinds[0] and mutation.kind in kinds[1],
                     'INVALID_AUDIT_MUTATION')
    elif isinstance(observation,BootstrapObservation):
        _require(trace_id==observation.manifest_sha256 and row['audit_id']=='manage-v1:'+trace_id,
            'AUDIT_OBSERVATION_MISMATCH')
        _require(target is not None and target.resource_type is ResourceType.WORKSPACE
            and target.workspace_id==observation.workspace_id and target.revision is None
            and isinstance(mutation,MutationObservation) and mutation.kind is MutationKind.CREATE
            and mutation.outcome in {MutationOutcome.APPLIED,MutationOutcome.UNKNOWN,MutationOutcome.REJECTED},'INVALID_AUDIT_OBSERVATION')
        expected=dict(actor_kind='USER',actor_id=observation.actor_id,workspace_id=observation.workspace_id,
            action='BOOTSTRAP_WORKSPACE',allowed=mutation.outcome is not MutationOutcome.REJECTED,
            reason_code='EXPLICIT_OPERATOR',policy_version=observation.policy_version,membership_revision=None)
    else:
        _require(isinstance(observation, WorkflowObservation) and isinstance(mutation, MutationObservation)
                 and target is not None, 'INVALID_AUDIT_OBSERVATION')
        _require(target.workspace_id == observation.workspace_id
                 and (target.resource_type, mutation.kind) == _SERVICE_TARGETS[observation.action], 'AUDIT_SCOPE_MISMATCH')
        expected_outcomes = {
            WorkflowReason.TRANSITION_VALIDATED: {MutationOutcome.APPLIED, MutationOutcome.NO_CHANGE},
            WorkflowReason.TRANSITION_REJECTED: {MutationOutcome.REJECTED},
            WorkflowReason.OUTCOME_UNKNOWN: {MutationOutcome.UNKNOWN},
        }
        _require(mutation.outcome in expected_outcomes[observation.reason], 'INVALID_AUDIT_MUTATION')
        expected = dict(actor_kind=ActorKind.SERVICE.value, actor_id=observation.worker_id,
            workspace_id=observation.workspace_id, action=observation.action.value,
            allowed=observation.reason != WorkflowReason.TRANSITION_REJECTED,
            reason_code=observation.reason.value, policy_version=observation.policy_version, membership_revision=None)
    _mutation(target, mutation, expected['allowed'])
    expected.update(resource_type=target.resource_type.value if target else None,
        resource_id=target.resource_id if target else None, resource_revision=target.revision if target else None,
        **{key: getattr(target, key) if target else None for key in ('conversation_id', 'owner_id', 'request_id', 'job_id')},
        mutation_outcome=mutation.outcome.value if mutation else None,
        new_revision=mutation.new_revision if mutation else None)
    _require(all(row[key] == value for key, value in expected.items()), 'AUDIT_OBSERVATION_MISMATCH')


def validate_audit_replay(existing: dict, candidate: dict, **context) -> None:
    """Validate identical-key/content retry using the original captured context.

    No update, append, lock or current permission check. Writers must read the
    unique logical key and fence any publication inside the coordinator.
    """
    validate_audit_event(existing, **context)
    _shape(candidate, TABLES['audit_events'].schema)
    _opaque(candidate['audit_id'])
    _require((existing['workspace_id'], existing['audit_id']) ==
             (candidate['workspace_id'], candidate['audit_id']), 'AUDIT_REPLAY_KEY_MISMATCH')
    _require(existing == candidate, 'AUDIT_IDEMPOTENCY_CONFLICT')
