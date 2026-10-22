"""Offline conversation contracts and proposals, without persistence or grants.

Access checks evaluate captured server inputs. The future service/coordinator
must verify identity, reread authoritative access, ensure uniqueness and commit
under the workspace fence. Archival has a structural contract only: its write
permission/audit action is deliberately not inferred from sharing permission.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from enum import Enum

from src.application.access_policy import (
    AccessDecision, Action, OwnedResource, Principal, Visibility, require_authorized,
)
from src.application.membership_contract import validate_membership
from src.application.workflow_contract import _id, _require, _shape
from src.application.workflow_schema import SCHEMA_VERSION, TABLES
from src.application.workspace_contract import (
    MAX_REVISION, WorkspaceStatus, _revision as _workspace_revision, _time, validate_workspace,
)


class ConversationStatus(str, Enum):
    ACTIVE = 'ACTIVE'
    ARCHIVED = 'ARCHIVED'


class ConversationChange(str, Enum):
    SET_VISIBILITY = 'SET_VISIBILITY'
    ARCHIVE = 'ARCHIVE'


def _revision(value: int) -> None:
    _require(type(value) is int and 1 <= value <= MAX_REVISION, 'INVALID_CONVERSATION_REVISION')


def validate_conversation(row: dict, *, workspace_id: str, conversation_id: str) -> OwnedResource:
    """Check the complete stored row and return immutable access metadata."""
    _id(workspace_id)
    _id(conversation_id)
    _shape(row, TABLES['conversations'].schema)
    _require(row['schema_version'] == SCHEMA_VERSION, 'INVALID_SCHEMA_VERSION')
    _require(row['workspace_id'] == workspace_id and row['conversation_id'] == conversation_id,
             'CONVERSATION_SCOPE_MISMATCH')
    _id(row['owner_id'], maximum=256)
    _require(row['status'] in {state.value for state in ConversationStatus}, 'INVALID_CONVERSATION_STATUS')
    _require(row['visibility'] in {visibility.value for visibility in Visibility}, 'INVALID_CONVERSATION_VISIBILITY')
    _revision(row['access_revision'])
    _time(row['created_at'])
    _time(row['updated_at'])
    _require(row['created_at'] <= row['updated_at'], 'INVALID_CONVERSATION_TIMELINE')
    return OwnedResource(conversation_id, workspace_id, row['owner_id'],
                         Visibility(row['visibility']), row['access_revision'])


def validate_conversation_creation(row: dict, *, workspace_id: str, conversation_id: str,
                                   owner_id: str, observed_at: datetime) -> None:
    """Check an ACTIVE/private revision-one proposal; uniqueness is external."""
    validate_conversation(row, workspace_id=workspace_id, conversation_id=conversation_id)
    _id(owner_id, maximum=256)
    _time(observed_at)
    _require(row['owner_id'] == owner_id, 'CONVERSATION_OWNER_MISMATCH')
    _require(row['access_revision'] == 1, 'INVALID_INITIAL_CONVERSATION_REVISION')
    _require(row['status'] == ConversationStatus.ACTIVE.value, 'INVALID_INITIAL_CONVERSATION_STATUS')
    _require(row['visibility'] == Visibility.PRIVATE.value, 'INVALID_INITIAL_CONVERSATION_VISIBILITY')
    _require(row['created_at'] == row['updated_at'] == observed_at, 'INVALID_CONVERSATION_CREATION_TIME')


def validate_conversation_change(previous: dict, candidate: dict, *, workspace_id: str,
                                 conversation_id: str, expected_revision: int,
                                 observed_at: datetime, change: ConversationChange) -> None:
    """Validate one sharing or archive transition; no transfer or restoration.

    A valid ARCHIVE proposal does not authorize archival. Sharing and archival
    cannot be combined into one revision. Identical/no-op sharing is rejected;
    the future writer resolves committed retry identities before proposing again.
    """
    validate_conversation(previous, workspace_id=workspace_id, conversation_id=conversation_id)
    validate_conversation(candidate, workspace_id=workspace_id, conversation_id=conversation_id)
    _revision(expected_revision)
    _time(observed_at)
    _require(isinstance(change, ConversationChange), 'INVALID_CONVERSATION_CHANGE')
    _require(previous['access_revision'] == expected_revision, 'STALE_CONVERSATION_REVISION')
    _require(expected_revision < MAX_REVISION, 'CONVERSATION_REVISION_EXHAUSTED')
    _require(candidate['access_revision'] == expected_revision + 1, 'INVALID_CONVERSATION_REVISION_INCREMENT')
    _require(previous['owner_id'] == candidate['owner_id'], 'IMMUTABLE_CONVERSATION_OWNER')
    _require(previous['created_at'] == candidate['created_at'], 'IMMUTABLE_CONVERSATION_CREATION')
    _require(previous['updated_at'] <= observed_at == candidate['updated_at'],
             'INVALID_CONVERSATION_CHANGE_TIME')
    _require(previous['status'] == ConversationStatus.ACTIVE.value, 'CONVERSATION_ARCHIVED')
    if change is ConversationChange.SET_VISIBILITY:
        _require(candidate['status'] == previous['status'], 'SHARING_CHANGED_CONVERSATION_STATUS')
        _require(candidate['visibility'] != previous['visibility'], 'CONVERSATION_VISIBILITY_UNCHANGED')
    else:
        _require(candidate['status'] == ConversationStatus.ARCHIVED.value, 'INVALID_CONVERSATION_ARCHIVE')
        _require(candidate['visibility'] == previous['visibility'], 'ARCHIVE_CHANGED_CONVERSATION_VISIBILITY')


def validate_creator_conversation_mutation(candidate: dict, *, previous: dict | None,
                                          workspace: dict, workspace_id: str, conversation_id: str,
                                          expected_workspace_revision: int,
                                          expected_revision: int | None, actor: Principal,
                                          actor_membership: dict, observed_at: datetime,
                                          change: ConversationChange | None = None) -> AccessDecision:
    """Check creator creation/sharing proposals against supplied pre-write rows.

    Return the pre-mutation decision for audit capture, not a live access grant.
    There is no archive/restore authorization path in this contract milestone.
    """
    _require(isinstance(actor, Principal), 'INVALID_CONVERSATION_ACTOR')
    validate_workspace(workspace, workspace_id=workspace_id)
    _workspace_revision(expected_workspace_revision)
    _time(observed_at)
    _require(workspace['revision'] == expected_workspace_revision, 'STALE_WORKSPACE_REVISION')
    _require(expected_workspace_revision < MAX_REVISION, 'WORKSPACE_REVISION_EXHAUSTED')
    _require(workspace['status'] == WorkspaceStatus.ACTIVE.value, 'WORKSPACE_INACTIVE')
    member = validate_membership(actor_membership, workspace_id=workspace_id,
                                 principal_id=actor.principal_id)
    _require(max(workspace['updated_at'], actor_membership['updated_at']) <= observed_at,
             'INVALID_CONVERSATION_OBSERVATION_TIME')
    if previous is None:
        _require(change is None, 'INVALID_CONVERSATION_CHANGE')
        _require(expected_revision is None, 'INVALID_INITIAL_CONVERSATION_REVISION')
        decision = require_authorized(actor, member, Action.GENERATE_DRAFT, workspace_id=workspace_id)
        validate_conversation_creation(candidate, workspace_id=workspace_id, conversation_id=conversation_id,
                                       owner_id=actor.principal_id, observed_at=observed_at)
    else:
        _require(isinstance(change, ConversationChange), 'INVALID_CONVERSATION_CHANGE')
        _require(change is ConversationChange.SET_VISIBILITY, 'CONVERSATION_LIFECYCLE_WRITE_DISABLED')
        resource = validate_conversation(previous, workspace_id=workspace_id, conversation_id=conversation_id)
        _require(previous['status'] == ConversationStatus.ACTIVE.value, 'CONVERSATION_ARCHIVED')
        decision = require_authorized(actor, member, Action.SET_VISIBILITY,
                                      workspace_id=workspace_id, resource=resource)
        validate_conversation_change(previous, candidate, workspace_id=workspace_id,
            conversation_id=conversation_id, expected_revision=expected_revision,
            observed_at=observed_at, change=change)
    return decision


def propose_conversation_creation(*, workspace: dict, workspace_id: str, conversation_id: str,
                                  expected_workspace_revision: int, actor: Principal,
                                  actor_membership: dict, observed_at: datetime) -> dict:
    """Derive owner from the actor and always propose a new private conversation."""
    _require(isinstance(actor, Principal), 'INVALID_CONVERSATION_ACTOR')
    candidate = dict(schema_version=SCHEMA_VERSION, workspace_id=workspace_id,
        conversation_id=conversation_id, owner_id=actor.principal_id, visibility=Visibility.PRIVATE.value,
        status=ConversationStatus.ACTIVE.value, access_revision=1, created_at=observed_at, updated_at=observed_at)
    validate_creator_conversation_mutation(candidate, previous=None, workspace=workspace,
        workspace_id=workspace_id, conversation_id=conversation_id,
        expected_workspace_revision=expected_workspace_revision, expected_revision=None,
        actor=actor, actor_membership=actor_membership, observed_at=observed_at)
    return candidate


def propose_conversation_visibility(previous: dict, *, visibility: Visibility,
                                    workspace: dict, workspace_id: str, conversation_id: str,
                                    expected_workspace_revision: int, expected_revision: int,
                                    actor: Principal, actor_membership: dict, observed_at: datetime) -> dict:
    """Return an independent sharing proposal; no-op proposals are rejected."""
    validate_conversation(previous, workspace_id=workspace_id, conversation_id=conversation_id)
    _require(isinstance(visibility, Visibility), 'INVALID_CONVERSATION_VISIBILITY')
    _revision(expected_revision)
    _require(expected_revision < MAX_REVISION, 'CONVERSATION_REVISION_EXHAUSTED')
    candidate = deepcopy(previous)
    candidate.update(visibility=visibility.value, access_revision=expected_revision + 1, updated_at=observed_at)
    validate_creator_conversation_mutation(candidate, previous=previous, workspace=workspace,
        workspace_id=workspace_id, conversation_id=conversation_id,
        expected_workspace_revision=expected_workspace_revision, expected_revision=expected_revision,
        actor=actor, actor_membership=actor_membership, observed_at=observed_at,
        change=ConversationChange.SET_VISIBILITY)
    return candidate
