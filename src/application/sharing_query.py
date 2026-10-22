"""Parameterized sharing SQL and server-internal result contracts, without I/O.

No client, submission, authentication or job-outcome verification is performed.
The future executor must resolve a trusted original intent, verify identity,
check deployed schemas, use a preallocated job ID and reconcile uncertain jobs.
These validators cannot establish that a caller-supplied result was committed.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
import re

from google.cloud import bigquery

from src.application.access_policy import POLICY_VERSION, Reason
from src.application.identity import ISSUER
from src.application.sharing_coordinator_contract import (
    SharingCommand, SharingCommitObservation, SharingCommitStatus, SharingPreparationState,
    prepare_sharing,
)
from src.application.workflow_contract import _require
from src.application.workflow_schema import DATASET, LOCATION, SCHEMA_VERSION, TABLES
from src.application.workspace_contract import MAX_REVISION, _revision
from src.ingestion.setup_workflow_tables import PROJECT

SQL_PATH = Path(__file__).resolve().parents[2] / 'sql' / 'share_conversation.sql'
_TABLE_NAMES = ('workspaces', 'memberships', 'conversations', 'audit_events')


def _render_sql() -> str:
    """Render identifiers/types from server constants, never from user values."""
    sql = SQL_PATH.read_text(encoding='utf-8')
    types = dict(STRING='STRING', INTEGER='INT64', BOOLEAN='BOOL', TIMESTAMP='TIMESTAMP')
    for name in _TABLE_NAMES:
        fields = TABLES[name].schema
        declarations = []
        for field in fields:
            kind = types[field.field_type]
            if field.mode == 'REPEATED':
                kind = f'ARRAY<{kind}>'
            declarations.append(f'{field.name} {kind}')
        prefix = '__' + name.upper()
        sql = sql.replace(prefix + '_TYPE__', ', '.join(declarations))
        sql = sql.replace(prefix + '_COLUMNS__', ', '.join(f.name for f in fields))
        sql = sql.replace(prefix + '_TABLE__', f'`{PROJECT}.{DATASET}.{name}`')
    sql = sql.replace('__AUDIT_EVENTS_RECEIPT_COLUMNS__',
                      ', '.join('receipt_row.' + f.name for f in TABLES['audit_events'].schema))
    _require(not re.search(r'__[A-Z_]+__', sql), 'INVALID_SHARING_SQL_TEMPLATE')
    return sql


@dataclass(frozen=True, slots=True)
class SharingQuerySettings:
    maximum_bytes_billed: int = 64 * 1024**2
    job_timeout_ms: int = 60000
    statement_timeout_ms: int = 20000

    def __post_init__(self):
        _require(type(self.maximum_bytes_billed) is int
                 and 1 <= self.maximum_bytes_billed <= 1024**3, 'INVALID_SHARING_QUERY_SETTINGS')
        _require(type(self.job_timeout_ms) is int and type(self.statement_timeout_ms) is int
                 and 1 <= self.statement_timeout_ms <= self.job_timeout_ms <= 60000,
                 'INVALID_SHARING_QUERY_SETTINGS')


@dataclass(frozen=True, slots=True)
class SharingQuery:
    command: SharingCommand
    expected_workspace_revision: int
    commit_observation: SharingCommitObservation
    settings: SharingQuerySettings = SharingQuerySettings()

    def __post_init__(self):
        _require(isinstance(self.command, SharingCommand), 'INVALID_SHARING_COMMAND')
        _revision(self.expected_workspace_revision)
        _require(isinstance(self.commit_observation, SharingCommitObservation), 'INVALID_SHARING_COMMIT_STATUS')
        _require(self.commit_observation.audit_id == self.command.audit_id, 'SHARING_COMMIT_SCOPE_MISMATCH')
        _require(isinstance(self.settings, SharingQuerySettings), 'INVALID_SHARING_QUERY_SETTINGS')

    @property
    def sql(self) -> str:
        return _render_sql()

    @property
    def location(self) -> str:
        return LOCATION

    def job_config(self) -> bigquery.QueryJobConfig:
        """Fresh mutable SDK objects; caller mutation cannot alter this contract.

        Timeout is an operational bound, not proof that a timed-out job aborted.
        No session, destination table, inferred schema or automatic retry is set.
        The executor must separately disable SDK job retries on submit/result.
        """
        command = self.command
        values = dict(
            workspace_id=('STRING', command.workspace_id),
            conversation_id=('STRING', command.conversation_id),
            principal_id=('STRING', command.principal_id), visibility=('STRING', command.visibility.value),
            expected_workspace_revision=('INT64', self.expected_workspace_revision),
            expected_membership_revision=('INT64', command.expected_membership_revision),
            expected_conversation_revision=('INT64', command.expected_conversation_revision),
            accepted_at=('TIMESTAMP', command.accepted_at.astimezone(timezone.utc)),
            deadline_at=('TIMESTAMP', command.deadline_at), trace_id=('STRING', command.trace_id),
            audit_prefix=('STRING', command.audit_prefix), audit_id=('STRING', command.audit_id),
            commit_status=('STRING', self.commit_observation.status.value),
            schema_version=('STRING', SCHEMA_VERSION), policy_version=('STRING', POLICY_VERSION),
            reason_code=('STRING', Reason.ALLOWED_OWNER.value), identity_issuer=('STRING', ISSUER))
        sql_parameters = set(re.findall(r'(?<!@)@([a-z_]+)', self.sql))
        _require(sql_parameters == set(values), 'INVALID_SHARING_SQL_PARAMETERS')
        return bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(name, kind, value)
                              for name, (kind, value) in values.items()],
            use_legacy_sql=False, use_query_cache=False, create_session=False,
            maximum_bytes_billed=self.settings.maximum_bytes_billed,
            job_timeout_ms=self.settings.job_timeout_ms,
            script_options=bigquery.ScriptOptions(statement_timeout_ms=self.settings.statement_timeout_ms),
            labels={'workflow': 'sharing-v1'})


class SharingResultState(str, Enum):
    APPLIED = 'APPLIED'
    REUSED = 'REUSED'
    UNKNOWN = 'UNKNOWN'


@dataclass(frozen=True, slots=True)
class SharingQueryResult:
    """Minimal result projection; construct through validate_sharing_result.

    No identity-subject metadata or full records are retained. This validates
    result content, not native BigQuery job correlation or commit evidence.
    """
    state: SharingResultState
    audit_id: str
    observed_at: datetime
    resulting_revision: int | None


def validate_sharing_result(query: SharingQuery, rows: list[dict], *, query_job_id: str) -> SharingQueryResult:
    """Validate exactly one final script row against the accepted command.

    Only a trusted executor may supply rows from the completed correlated job.
    Receiving UNKNOWN must not be converted into a retry or applied response.
    """
    _require(isinstance(query, SharingQuery), 'INVALID_SHARING_QUERY')
    _require(isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict),
             'INVALID_SHARING_RESULT_ROWS')
    row = rows[0]
    _require(set(row) == {'state', 'observed_at', 'workspace', 'membership', 'conversation', 'receipt'},
             'INVALID_SHARING_RESULT_SHAPE')
    _require(isinstance(row['state'], str) and row['state'] in {s.value for s in SharingResultState},
             'INVALID_SHARING_RESULT_STATE')
    state = SharingResultState(row['state'])
    command = query.command
    current_job = SharingCommitObservation(command.audit_id, SharingCommitStatus.COMMITTED, query_job_id)
    if state is SharingResultState.UNKNOWN:
        _require(row['receipt'] is None and query.commit_observation.status in
                 {SharingCommitStatus.UNKNOWN, SharingCommitStatus.COMMITTED}, 'INVALID_SHARING_UNKNOWN_RESULT')
    else:
        _require(isinstance(row['receipt'], dict), 'INVALID_SHARING_RESULT_RECEIPT')
    # The coordinator checks exact destination shapes, all control metadata,
    # current permission, original audit fields/window and later-state causality.
    preparation = prepare_sharing(command, workspace_rows=[row['workspace']],
        membership_rows=[row['membership']], conversation_rows=[row['conversation']],
        receipt_rows=[] if row['receipt'] is None else [row['receipt']],
        expected_workspace_revision=query.expected_workspace_revision, observed_at=row['observed_at'],
        commit_observation=current_job)
    if state is SharingResultState.UNKNOWN:
        _require(preparation.state is SharingPreparationState.UNKNOWN, 'INVALID_SHARING_UNKNOWN_RESULT')
    else:
        _require(preparation.state is SharingPreparationState.REUSED, 'INVALID_SHARING_RESULT_RECEIPT')
    if state is SharingResultState.REUSED:
        _require(query.commit_observation.status is not SharingCommitStatus.NOT_COMMITTED,
                 'SHARING_COMMIT_OBSERVATION_CONFLICT')
    if state is SharingResultState.APPLIED:
        _require(query.commit_observation.status in
                 {SharingCommitStatus.NOT_SUBMITTED, SharingCommitStatus.NOT_COMMITTED},
                 'INVALID_SHARING_APPLIED_RESULT')
        _require(query.expected_workspace_revision < MAX_REVISION
                 and row['workspace']['revision'] == query.expected_workspace_revision + 1
                 and row['membership']['revision'] == command.expected_membership_revision
                 and row['conversation']['access_revision'] == command.expected_conversation_revision + 1
                 and row['conversation']['visibility'] == command.visibility.value
                 and row['workspace']['updated_at'] == row['conversation']['updated_at']
                     == row['receipt']['occurred_at'] == row['observed_at'], 'INCONSISTENT_SHARING_APPLIED_RESULT')
    return SharingQueryResult(state, command.audit_id, row['observed_at'], preparation.resulting_revision)
