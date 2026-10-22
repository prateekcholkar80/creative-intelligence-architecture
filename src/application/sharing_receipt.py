"""Read-only sharing receipt query and validation; no authentication or I/O.

One SELECT captures controls and receipts in the same BigQuery snapshot. Only
the trusted adapter may supply its results. A receipt proves the accepted intent
committed, not which native mutation job committed. Missing receipts stay UNKNOWN.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone

from google.cloud import bigquery

from src.application.sharing_coordinator_contract import (
    SharingCommand, SharingCommitObservation, SharingCommitStatus,
    SharingPreparationState, prepare_sharing,
)
from src.application.sharing_query import SharingQuery, SharingQueryResult, SharingResultState
from src.application.workflow_contract import _require
from src.application.workflow_schema import DATASET, LOCATION, TABLES
from src.ingestion.setup_workflow_tables import PROJECT


def _sql() -> str:
    predicates = dict(workspaces='workspace_id=@workspace_id',
        memberships='workspace_id=@workspace_id AND principal_id=@principal_id',
        conversations='workspace_id=@workspace_id AND conversation_id=@conversation_id',
        audit_events='workspace_id=@workspace_id AND STARTS_WITH(audit_id, @audit_prefix) '
                     'AND occurred_at >= @accepted_at AND occurred_at < @deadline_at')
    projections = ['CURRENT_TIMESTAMP() AS observed_at']
    for table, predicate in predicates.items():
        columns = ', '.join(field.name for field in TABLES[table].schema)
        projections.append(f'ARRAY(SELECT AS STRUCT {columns} FROM `{PROJECT}.{DATASET}.{table}` '
                           f'WHERE {predicate} LIMIT 2) AS {table}')
    return 'SELECT\n  ' + ',\n  '.join(projections)


@dataclass(frozen=True, slots=True)
class SharingReceiptQuery:
    intent: SharingQuery

    def __post_init__(self):
        _require(isinstance(self.intent, SharingQuery), 'INVALID_SHARING_QUERY')
        _require(self.intent.commit_observation.status is SharingCommitStatus.NOT_SUBMITTED,
                 'INVALID_SHARING_INITIAL_ATTEMPT')

    @property
    def command(self) -> SharingCommand:
        return self.intent.command

    @property
    def sql(self) -> str:
        return _sql()

    @property
    def location(self) -> str:
        return LOCATION

    def job_config(self) -> bigquery.QueryJobConfig:
        command = self.command
        values = dict(workspace_id=('STRING', command.workspace_id),
            principal_id=('STRING', command.principal_id), conversation_id=('STRING', command.conversation_id),
            audit_prefix=('STRING', command.audit_prefix),
            accepted_at=('TIMESTAMP', command.accepted_at.astimezone(timezone.utc)),
            deadline_at=('TIMESTAMP', command.deadline_at))
        return bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False, create_session=False,
            maximum_bytes_billed=self.intent.settings.maximum_bytes_billed,
            job_timeout_ms=self.intent.settings.job_timeout_ms,
            query_parameters=[bigquery.ScalarQueryParameter(name, kind, value)
                              for name, (kind, value) in values.items()],
            labels={'workflow': 'sharing-receipt-v1'})


def validate_receipt_result(query: SharingReceiptQuery, rows: list[dict], *, original_job_id: str) -> SharingQueryResult:
    """Validate fresh native SELECT rows; never construct a mutation proposal."""
    _require(isinstance(query, SharingReceiptQuery), 'INVALID_SHARING_RECEIPT_QUERY')
    _require(isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict),
             'INVALID_SHARING_RECEIPT_ROWS')
    row = rows[0]
    _require(set(row) == {'observed_at', 'workspaces', 'memberships', 'conversations', 'audit_events'},
             'INVALID_SHARING_RECEIPT_SHAPE')
    command = query.command
    preparation = prepare_sharing(command, workspace_rows=row['workspaces'], membership_rows=row['memberships'],
        conversation_rows=row['conversations'], receipt_rows=row['audit_events'],
        expected_workspace_revision=query.intent.expected_workspace_revision,
        observed_at=row['observed_at'], commit_observation=SharingCommitObservation(
            command.audit_id, SharingCommitStatus.UNKNOWN, original_job_id))
    # UNKNOWN input forbids PREPARED regardless of the original business pins.
    _require(preparation.state in {SharingPreparationState.REUSED, SharingPreparationState.UNKNOWN},
             'INVALID_SHARING_RECEIPT_STATE')
    state = (SharingResultState.REUSED if preparation.state is SharingPreparationState.REUSED
             else SharingResultState.UNKNOWN)
    return SharingQueryResult(state, command.audit_id, row['observed_at'], preparation.resulting_revision)
