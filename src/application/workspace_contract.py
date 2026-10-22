"""Pure workspace control contracts, without authorization, bootstrap or locks.

Proposals must be revalidated against the unique current BigQuery control row
inside the future transaction coordinator. A Python revision check cannot fence
concurrent workers. Status changes require a separately authorized control path.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum

from src.application.workflow_contract import (
    ContractError, _id, _require, _shape, _timestamp,
)
from src.application.workflow_schema import SCHEMA_VERSION, TABLES

MAX_REVISION = 2**63 - 1


class WorkspaceStatus(str, Enum):
    ACTIVE = 'ACTIVE'
    DISABLED = 'DISABLED'


class WorkspaceChange(str, Enum):
    FENCE = 'FENCE'
    SET_STATUS = 'SET_STATUS'


def _time(value: datetime) -> None:
    _timestamp(value)
    try:
        value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as error:
        raise ContractError('INVALID_TIMESTAMP') from error


def _revision(value: int) -> None:
    _require(type(value) is int and 1 <= value <= MAX_REVISION, 'INVALID_WORKSPACE_REVISION')


def validate_workspace(row: dict, *, workspace_id: str) -> None:
    """Validate a complete stored control row, including disabled workspaces."""
    _id(workspace_id)
    _shape(row, TABLES['workspaces'].schema)
    _require(row['schema_version'] == SCHEMA_VERSION, 'INVALID_SCHEMA_VERSION')
    _require(row['workspace_id'] == workspace_id, 'WORKSPACE_SCOPE_MISMATCH')
    _require(row['status'] in {status.value for status in WorkspaceStatus}, 'INVALID_WORKSPACE_STATUS')
    _revision(row['revision'])
    _time(row['created_at'])
    _time(row['updated_at'])
    _require(row['created_at'] <= row['updated_at'], 'INVALID_WORKSPACE_TIMELINE')


def validate_workspace_creation(row: dict, *, workspace_id: str, observed_at: datetime) -> None:
    """Check a controlled initial-row proposal; never insert or assign members."""
    validate_workspace(row, workspace_id=workspace_id)
    _time(observed_at)
    _require(row['revision'] == 1, 'INVALID_INITIAL_WORKSPACE_REVISION')
    _require(row['status'] == WorkspaceStatus.ACTIVE.value, 'INVALID_INITIAL_WORKSPACE_STATUS')
    _require(row['created_at'] == row['updated_at'] == observed_at, 'INVALID_WORKSPACE_CREATION_TIME')


def validate_workspace_change(previous: dict, candidate: dict, *, workspace_id: str,
                              expected_revision: int, observed_at: datetime,
                              change: WorkspaceChange) -> None:
    """Check a proposed fence increment or explicit lifecycle change.

    SET_STATUS permits disable/reactivate proposals, not ordinary workflow
    writes on disabled workspaces. Current authorization and an atomic control
    update are separate requirements. Every accepted change consumes a revision,
    even when the database clock produces the same timestamp.
    """
    validate_workspace(previous, workspace_id=workspace_id)
    validate_workspace(candidate, workspace_id=workspace_id)
    _revision(expected_revision)
    _time(observed_at)
    _require(isinstance(change, WorkspaceChange), 'INVALID_WORKSPACE_CHANGE')
    _require(previous['revision'] == expected_revision, 'STALE_WORKSPACE_REVISION')
    _require(expected_revision < MAX_REVISION, 'WORKSPACE_REVISION_EXHAUSTED')
    _require(candidate['revision'] == expected_revision + 1, 'INVALID_WORKSPACE_REVISION_INCREMENT')
    _require(candidate['created_at'] == previous['created_at'], 'IMMUTABLE_WORKSPACE_CREATION')
    _require(previous['updated_at'] <= observed_at == candidate['updated_at'],
             'INVALID_WORKSPACE_CHANGE_TIME')
    if change is WorkspaceChange.FENCE:
        _require(previous['status'] == WorkspaceStatus.ACTIVE.value, 'WORKSPACE_INACTIVE')
        _require(candidate['status'] == previous['status'], 'FENCE_CHANGED_WORKSPACE_STATUS')
    else:
        _require(candidate['status'] != previous['status'], 'WORKSPACE_STATUS_UNCHANGED')


def propose_workspace_fence(previous: dict, *, workspace_id: str,
                            expected_revision: int, observed_at: datetime) -> dict:
    """Return an independent active-row proposal; this does not acquire a lock."""
    validate_workspace(previous, workspace_id=workspace_id)
    _revision(expected_revision)
    _require(expected_revision < MAX_REVISION, 'WORKSPACE_REVISION_EXHAUSTED')
    candidate = deepcopy(previous)
    candidate.update(revision=expected_revision + 1, updated_at=observed_at)
    validate_workspace_change(previous, candidate, workspace_id=workspace_id,
                              expected_revision=expected_revision, observed_at=observed_at,
                              change=WorkspaceChange.FENCE)
    return candidate
