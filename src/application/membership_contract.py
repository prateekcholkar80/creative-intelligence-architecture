"""Offline membership contracts; no authentication, role bootstrap or writes.

Identity binding is deterministic, not proof that a subject was authenticated.
Admin checks evaluate supplied snapshots, not current BigQuery state. The future
service must verify identity and reread/fence access inside its transaction.
SYNC/BOOTSTRAP provenance is accepted in stored rows, not granted a write path.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from enum import Enum

from src.application.access_policy import (
    AccessDecision, Action, Membership, MembershipStatus, Principal, Role, require_authorized,
)
from src.application.identity import IdentityError, principal_for_subject
from src.application.workflow_contract import ContractError, _id, _require, _shape
from src.application.workflow_schema import SCHEMA_VERSION, TABLES
from src.application.workspace_contract import (
    MAX_REVISION, WorkspaceStatus, _revision as _workspace_revision, _time, validate_workspace,
)


class AssignmentOrigin(str, Enum):
    ADMIN = 'ADMIN'
    SYNC = 'SYNC'
    BOOTSTRAP = 'BOOTSTRAP'


def _revision(value: int) -> None:
    _require(type(value) is int and 1 <= value <= MAX_REVISION, 'INVALID_MEMBERSHIP_REVISION')


def validate_membership(row: dict, *, workspace_id: str, principal_id: str) -> Membership:
    """Return immutable access metadata after checking the entire stored row."""
    _id(workspace_id)
    _id(principal_id, maximum=256)
    _shape(row, TABLES['memberships'].schema)
    _require(row['schema_version'] == SCHEMA_VERSION, 'INVALID_SCHEMA_VERSION')
    _require(row['workspace_id'] == workspace_id and row['principal_id'] == principal_id,
             'MEMBERSHIP_SCOPE_MISMATCH')
    try:
        bound = principal_for_subject(row['identity_issuer'], row['identity_subject'])
    except IdentityError:
        raise ContractError('INVALID_MEMBERSHIP_IDENTITY') from None
    _require(bound.principal_id == principal_id, 'MEMBERSHIP_IDENTITY_MISMATCH')
    _require(row['status'] in {status.value for status in MembershipStatus}, 'INVALID_MEMBERSHIP_STATUS')
    _require(row['assignment_origin'] in {origin.value for origin in AssignmentOrigin},
             'INVALID_ASSIGNMENT_ORIGIN')
    roles = row['roles']
    _require(all(role in {r.value for r in Role} for role in roles) and len(roles) == len(set(roles)),
             'INVALID_MEMBERSHIP_ROLES')
    _id(row['updated_by'], maximum=256)
    _revision(row['revision'])
    _time(row['created_at'])
    _time(row['updated_at'])
    _require(row['created_at'] <= row['updated_at'], 'INVALID_MEMBERSHIP_TIMELINE')
    return Membership(principal_id, workspace_id, frozenset(Role(role) for role in roles),
                      MembershipStatus(row['status']), row['revision'])


def _assignment(row: dict, *, updated_by: str, origin: AssignmentOrigin) -> None:
    _id(updated_by, maximum=256)
    _require(isinstance(origin, AssignmentOrigin), 'INVALID_ASSIGNMENT_ORIGIN')
    _require(row['updated_by'] == updated_by and row['assignment_origin'] == origin.value,
             'MEMBERSHIP_ASSIGNMENT_MISMATCH')


def validate_membership_creation(row: dict, *, workspace_id: str, principal_id: str,
                                 observed_at: datetime, updated_by: str,
                                 origin: AssignmentOrigin) -> None:
    """Check an initial assignment proposal, without authorizing its origin."""
    validate_membership(row, workspace_id=workspace_id, principal_id=principal_id)
    _time(observed_at)
    _assignment(row, updated_by=updated_by, origin=origin)
    _require(row['revision'] == 1, 'INVALID_INITIAL_MEMBERSHIP_REVISION')
    _require(row['created_at'] == row['updated_at'] == observed_at, 'INVALID_MEMBERSHIP_CREATION_TIME')


def validate_membership_change(previous: dict, candidate: dict, *, workspace_id: str,
                               principal_id: str, expected_revision: int, observed_at: datetime,
                               updated_by: str, origin: AssignmentOrigin) -> None:
    """Check a real role/status update; role ordering alone is not a change.

    All six explicit status transitions are representable. Reactivating a revoked
    target requires the same authorized mutation path as assigning roles. An
    inactive target's stored roles do not grant access. Bootstrap cannot update.
    """
    validate_membership(previous, workspace_id=workspace_id, principal_id=principal_id)
    validate_membership(candidate, workspace_id=workspace_id, principal_id=principal_id)
    _revision(expected_revision)
    _time(observed_at)
    _assignment(candidate, updated_by=updated_by, origin=origin)
    _require(origin is not AssignmentOrigin.BOOTSTRAP, 'BOOTSTRAP_UPDATE_DISABLED')
    _require(previous['revision'] == expected_revision, 'STALE_MEMBERSHIP_REVISION')
    _require(expected_revision < MAX_REVISION, 'MEMBERSHIP_REVISION_EXHAUSTED')
    _require(candidate['revision'] == expected_revision + 1, 'INVALID_MEMBERSHIP_REVISION_INCREMENT')
    _require(all(previous[field] == candidate[field]
                 for field in ('identity_issuer', 'identity_subject', 'created_at')),
             'IMMUTABLE_MEMBERSHIP_IDENTITY')
    _require(previous['updated_at'] <= observed_at == candidate['updated_at'],
             'INVALID_MEMBERSHIP_CHANGE_TIME')
    _require(set(previous['roles']) != set(candidate['roles']) or previous['status'] != candidate['status'],
             'MEMBERSHIP_UNCHANGED')


def validate_admin_membership_mutation(candidate: dict, *, previous: dict | None,
                                       workspace: dict, workspace_id: str, principal_id: str,
                                       expected_workspace_revision: int,
                                       expected_revision: int | None, actor: Principal,
                                       actor_membership: dict, observed_at: datetime) -> AccessDecision:
    """Evaluate a proposed ADMIN create/update against captured pre-write inputs.

    Return the pre-mutation decision for later audit capture. Caller-supplied
    snapshots are not a live authorization grant or commit proof. Self-demotion
    is evaluated against the old row; workspace-wide last-admin safeguards need
    additional authoritative reads in the future management writer.
    """
    _require(isinstance(actor, Principal), 'INVALID_MEMBERSHIP_ACTOR')
    validate_workspace(workspace, workspace_id=workspace_id)
    _workspace_revision(expected_workspace_revision)
    _time(observed_at)
    _require(workspace['revision'] == expected_workspace_revision, 'STALE_WORKSPACE_REVISION')
    _require(expected_workspace_revision < MAX_REVISION, 'WORKSPACE_REVISION_EXHAUSTED')
    _require(workspace['status'] == WorkspaceStatus.ACTIVE.value, 'WORKSPACE_INACTIVE')
    member = validate_membership(actor_membership, workspace_id=workspace_id,
                                 principal_id=actor.principal_id)
    _require(max(workspace['updated_at'], actor_membership['updated_at']) <= observed_at,
             'INVALID_MEMBERSHIP_OBSERVATION_TIME')
    decision = require_authorized(actor, member, Action.MANAGE_MEMBERS, workspace_id=workspace_id)
    if principal_id == actor.principal_id:
        _require(previous is not None and previous == actor_membership, 'INCONSISTENT_ACTOR_MEMBERSHIP')
    if previous is None:
        _require(expected_revision is None, 'INVALID_INITIAL_MEMBERSHIP_REVISION')
        validate_membership_creation(candidate, workspace_id=workspace_id, principal_id=principal_id,
                                     observed_at=observed_at, updated_by=actor.principal_id,
                                     origin=AssignmentOrigin.ADMIN)
    else:
        validate_membership_change(previous, candidate, workspace_id=workspace_id, principal_id=principal_id,
                                   expected_revision=expected_revision, observed_at=observed_at,
                                   updated_by=actor.principal_id, origin=AssignmentOrigin.ADMIN)
    return decision


def propose_admin_membership_change(previous: dict, *, roles: frozenset[Role],
                                    status: MembershipStatus, workspace: dict, workspace_id: str,
                                    principal_id: str, expected_workspace_revision: int,
                                    expected_revision: int, actor: Principal,
                                    actor_membership: dict, observed_at: datetime) -> dict:
    """Build an independent, deterministic proposal; never write or assign users."""
    validate_membership(previous, workspace_id=workspace_id, principal_id=principal_id)
    _require(isinstance(roles, frozenset) and all(isinstance(role, Role) for role in roles),
             'INVALID_MEMBERSHIP_ROLES')
    _require(isinstance(status, MembershipStatus), 'INVALID_MEMBERSHIP_STATUS')
    _require(isinstance(actor, Principal), 'INVALID_MEMBERSHIP_ACTOR')
    _revision(expected_revision)
    _require(expected_revision < MAX_REVISION, 'MEMBERSHIP_REVISION_EXHAUSTED')
    candidate = deepcopy(previous)
    candidate.update(roles=sorted(role.value for role in roles), status=status.value,
                     revision=expected_revision + 1, updated_at=observed_at,
                     updated_by=actor.principal_id, assignment_origin=AssignmentOrigin.ADMIN.value)
    validate_admin_membership_mutation(candidate, previous=previous, workspace=workspace,
        workspace_id=workspace_id, principal_id=principal_id,
        expected_workspace_revision=expected_workspace_revision, expected_revision=expected_revision,
        actor=actor, actor_membership=actor_membership, observed_at=observed_at)
    return candidate
