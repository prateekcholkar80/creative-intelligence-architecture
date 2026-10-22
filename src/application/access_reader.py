"""Fresh, scoped BigQuery access reads for the future protected service boundary.

No caching, JSON fallback, role bootstrap, IAM changes or data writes. Public
authorize methods require a fresh server-verified identity; plain Principal is
insufficient. The current local chat is not connected to this reader yet.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import re

from google.api_core import retry
from google.cloud import bigquery

from src.application.access_policy import (
    AccessDecision, Action, AuthorizationError, Membership, MembershipStatus,
    OwnedResource, Reason, authorize, require_authorized,
)
from src.application.identity import VerifiedIdentity, ensure_fresh
from src.application.operation_budget import OperationStopped, operation_call, operation_iter, operation_retry
from src.application.conversation_contract import validate_conversation
from src.application.membership_contract import validate_membership
from src.application.protected_records import (
    ArtifactManifest, ArtifactSummary, AuthorizedArtifact, AuthorizedJob, AuthorizedOutput,
    JobMetadata, OutputMetadata, OutputSummary,
)
from src.application.workflow_contract import (
    ARTIFACT_TYPES, ArtifactLimits, ContractError, DEFAULT_ARTIFACT_LIMITS,
    canonical_json_bytes, validate_artifact, validate_output,
)
from src.application.workflow_schema import LOCATION, SCHEMA_VERSION, TABLES
from src.application.workspace_contract import validate_workspace
from src.ingestion.setup_workflow_tables import PROJECT
from src.retrieval.request import CANONICAL_SKUS

RETRY = retry.Retry(deadline=20)
MAX_OUTPUT_LOOKUP_SPAN = timedelta(days=7)
MAX_REQUEST_LOOKBACK = timedelta(days=7)
FULL_JOB_COLUMNS = ', '.join(field.name for field in TABLES['jobs'].schema)
FULL_REQUEST_COLUMNS = ', '.join(field.name for field in TABLES['requests'].schema)
FULL_OUTPUT_COLUMNS = ', '.join(field.name for field in TABLES['outputs'].schema)
JOB_COLUMNS = ('schema_version, workspace_id, conversation_id, owner_id, job_id, request_id, '
               'principal_id, canonical_sku, state, revision, attempt_count, active_attempt_id, '
               'output_id, safe_error_code, created_at, updated_at, deadline_at, completed_at')
OUTPUT_COLUMNS = ('schema_version, workspace_id, conversation_id, owner_id, output_id, job_id, '
                  'request_id, attempt_id, canonical_sku, source_system, approval_scope, '
                  'initial_review_status, real_business_approved, created_at, artifacts')


class AccessReadError(PermissionError):
    def __init__(self, code: str):
        self.code = code
        super().__init__('Current application access could not be verified.')


@dataclass(frozen=True, slots=True)
class AccessReadSettings:
    maximum_bytes_billed: int = 64 * 1024 * 1024
    timeout_seconds: int = 20

    def __post_init__(self):
        if (type(self.maximum_bytes_billed) is not int or not 1 <= self.maximum_bytes_billed <= 1024**3
                or type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 60):
            raise ValueError('Invalid bounded access-reader settings')


def _id(value: str, maximum: int = 128) -> None:
    if (not isinstance(value, str) or not 1 <= len(value) <= maximum
            or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)):
        raise AccessReadError('INVALID_ACCESS_RECORD')


def _record(row: dict, workspace_id: str) -> None:
    if row.get('schema_version') != SCHEMA_VERSION or row.get('workspace_id') != workspace_id:
        raise AccessReadError('INVALID_ACCESS_RECORD')
    for name in ('created_at', 'updated_at'):
        value = row.get(name)
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise AccessReadError('INVALID_ACCESS_RECORD')
    if row['updated_at'] < row['created_at']:
        raise AccessReadError('INVALID_ACCESS_RECORD')


def _timestamp(value) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AccessReadError('INVALID_ACCESS_RECORD')


class BigQueryAccessReader:
    def __init__(self, client: bigquery.Client, settings: AccessReadSettings | None = None):
        if client.project != PROJECT:
            raise ValueError('Access reads are scoped to the configured project')
        self.client = client
        self.settings = AccessReadSettings() if settings is None else settings
        if not isinstance(self.settings, AccessReadSettings):
            raise ValueError('Access-reader settings must be configured by the server')

    def _query(self, table: str, columns: str, predicates: str, *, dry_run: bool = False, **parameters):
        # Table/column/predicate expressions are module constants; all values are parameters.
        sql = f'SELECT {columns} FROM `{PROJECT}.ci_application.{table}` WHERE {predicates} LIMIT 2'
        config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(name, 'TIMESTAMP' if isinstance(value, datetime)
                                                           else 'STRING', value)
                              for name, value in parameters.items()],
            maximum_bytes_billed=self.settings.maximum_bytes_billed,
            job_timeout_ms=self.settings.timeout_seconds * 1000, use_query_cache=False, dry_run=dry_run)
        return operation_call(self.client.query, sql, location=LOCATION, job_config=config, retry=operation_retry(RETRY),
                                 timeout=self.settings.timeout_seconds, job_retry=None)

    def _rows(self, table: str, columns: str, predicates: str, **parameters) -> list[dict]:
        try:
            job = self._query(table, columns, predicates, **parameters)
            rows = operation_call(job.result, timeout=self.settings.timeout_seconds,
                                  retry=operation_retry(RETRY), job_retry=None)
            return [dict(row) for row in operation_iter(rows)]
        except OperationStopped:
            raise
        except Exception:
            raise AccessReadError('ACCESS_STORE_UNAVAILABLE') from None

    def _one(self, rows: list[dict]) -> dict | None:
        if len(rows) > 1:
            raise AccessReadError('DUPLICATE_ACCESS_RECORD')
        return rows[0] if rows else None

    def _workspace(self, workspace_id: str) -> None:
        _id(workspace_id)
        row = self._one(self._rows('workspaces', '*', 'workspace_id=@workspace_id', workspace_id=workspace_id))
        if row is None:
            raise AccessReadError('WORKSPACE_UNAVAILABLE')
        try:
            validate_workspace(row, workspace_id=workspace_id)
        except ContractError:
            raise AccessReadError('INVALID_ACCESS_RECORD') from None
        if row.get('status') != 'ACTIVE':
            raise AccessReadError('WORKSPACE_UNAVAILABLE')

    def _membership(self, identity: VerifiedIdentity, workspace_id: str) -> Membership | None:
        row = self._one(self._rows('memberships', '*',
            'workspace_id=@workspace_id AND principal_id=@principal_id',
            workspace_id=workspace_id, principal_id=identity.principal.principal_id))
        if row is None:
            return None
        try:
            membership = validate_membership(row, workspace_id=workspace_id,
                                             principal_id=identity.principal.principal_id)
            if row['identity_issuer'] != identity.issuer or row['identity_subject'] != identity.subject:
                raise ContractError('MEMBERSHIP_IDENTITY_MISMATCH')
            return membership
        except ContractError:
            raise AccessReadError('INVALID_ACCESS_RECORD') from None

    def _conversation(self, workspace_id: str, conversation_id: str, action: Action) -> OwnedResource:
        _id(conversation_id)
        row = self._one(self._rows('conversations', '*',
            'workspace_id=@workspace_id AND conversation_id=@conversation_id',
            workspace_id=workspace_id, conversation_id=conversation_id))
        if row is None:
            raise AccessReadError('RESOURCE_UNAVAILABLE')
        try:
            resource = validate_conversation(row, workspace_id=workspace_id, conversation_id=conversation_id)
        except ContractError:
            raise AccessReadError('INVALID_ACCESS_RECORD') from None
        if row['status'] == 'ARCHIVED' and action not in {Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT}:
            raise AccessReadError('RESOURCE_UNAVAILABLE')
        return resource

    def authorize(self, identity: VerifiedIdentity, action: Action, *, workspace_id: str,
                  conversation_id: str | None = None) -> AccessDecision:
        ensure_fresh(identity)  # No database lookup for anonymous, untyped or expired identity.
        if not isinstance(action, Action):
            raise ValueError('A typed server action is required')
        self._workspace(workspace_id)
        membership = self._membership(identity, workspace_id)
        if membership is None or membership.status != MembershipStatus.ACTIVE:
            # Deny before reading any requested private resource.
            return require_authorized(identity.principal, membership, action,
                                      workspace_id=workspace_id)
        resource = None if conversation_id is None else self._conversation(workspace_id, conversation_id, action)
        ensure_fresh(identity)  # Token may have expired during database operations.
        return require_authorized(identity.principal, membership, action,
                                  workspace_id=workspace_id, resource=resource)

    def _read_context(self, identity: VerifiedIdentity, workspace_id: str,
                      action: Action = Action.READ_DRAFT) -> Membership:
        ensure_fresh(identity)
        self._workspace(workspace_id)
        membership = self._membership(identity, workspace_id)
        decision = authorize(identity.principal, membership, action, workspace_id=workspace_id)
        # A reader with the right role still needs its current parent resource.
        # All other denials stop before any job/output/private-content lookup.
        if decision.reason != Reason.RESOURCE_REQUIRED:
            raise AuthorizationError(decision)
        return membership

    def _job_record(self, workspace_id: str, job_id: str, *, full: bool = False) -> dict:
        _id(job_id)
        row = self._one(self._rows('jobs', FULL_JOB_COLUMNS if full else JOB_COLUMNS,
            'workspace_id=@workspace_id AND job_id=@job_id', workspace_id=workspace_id, job_id=job_id))
        if row is None:
            raise AccessReadError('RESOURCE_UNAVAILABLE')
        if not {key.strip() for key in JOB_COLUMNS.split(',')} <= row.keys():
            raise AccessReadError('INVALID_ACCESS_RECORD')
        _record(row, workspace_id)
        for key in ('conversation_id', 'owner_id', 'request_id'):
            _id(row.get(key), 256 if key == 'owner_id' else 128)
        for key in ('active_attempt_id', 'output_id'):
            if row.get(key) is not None:
                _id(row[key])
        if (row.get('job_id') != job_id or row.get('principal_id') != row['owner_id']
                or not isinstance(row.get('canonical_sku'), str) or row['canonical_sku'] not in CANONICAL_SKUS
                or type(row.get('revision')) is not int or row['revision'] < 1
                or type(row.get('attempt_count')) is not int or row['attempt_count'] < 0):
            raise AccessReadError('INVALID_ACCESS_RECORD')
        _timestamp(row.get('deadline_at'))
        if row['deadline_at'] <= row['created_at']:
            raise AccessReadError('INVALID_ACCESS_RECORD')
        state = row.get('state')
        if not isinstance(state, str):
            raise AccessReadError('INVALID_ACCESS_RECORD')
        active, output, error = (row.get(key) for key in ('active_attempt_id', 'output_id', 'safe_error_code'))
        completed = row.get('completed_at')
        if error is not None and (not isinstance(error, str) or re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}', error) is None):
            raise AccessReadError('INVALID_ACCESS_RECORD')
        if state == 'QUEUED':
            valid = active is None and output is None and error is None and completed is None
        elif state in {'RUNNING', 'RECOVERY_REQUIRED'}:
            valid = (active is not None and output is None and completed is None
                     and row['attempt_count'] > 0 and (error is None) == (state == 'RUNNING'))
        elif state in {'SUCCEEDED', 'BLOCKED', 'FAILED'}:
            _timestamp(completed)
            valid = completed == row['updated_at']
            if state == 'SUCCEEDED':
                valid = valid and active is not None and row['attempt_count'] > 0 and output is not None and error is None
            else:
                valid = valid and output is None and error is not None
        else:
            valid = False
        if not valid or (active is not None and row['attempt_count'] == 0):
            raise AccessReadError('INVALID_ACCESS_RECORD')
        return row

    def _job(self, workspace_id: str, job_id: str) -> JobMetadata:
        row = self._job_record(workspace_id, job_id)
        return JobMetadata(**{key: row[key] for key in JobMetadata.__dataclass_fields__})

    def _job_parent(self, identity: VerifiedIdentity, membership: Membership, job: JobMetadata) -> AccessDecision:
        resource = self._conversation(job.workspace_id, job.conversation_id, Action.READ_DRAFT)
        if resource.owner_id != job.owner_id:
            raise AccessReadError('INVALID_ACCESS_RECORD')
        ensure_fresh(identity)
        return require_authorized(identity.principal, membership, Action.READ_DRAFT,
                                  workspace_id=job.workspace_id, resource=resource)

    def read_job(self, identity: VerifiedIdentity, *, workspace_id: str, job_id: str) -> AuthorizedJob:
        """Resolve a job's parent internally; copied ownership never grants access."""
        membership = self._read_context(identity, workspace_id)
        job = self._job(workspace_id, job_id)
        return AuthorizedJob(job, self._job_parent(identity, membership, job))

    def read_output(self, identity: VerifiedIdentity, *, workspace_id: str, job_id: str,
                    limits: ArtifactLimits = DEFAULT_ARTIFACT_LIMITS) -> AuthorizedOutput:
        """Read only the successful job's output projection, then recheck current access."""
        authorized = self.read_job(identity, workspace_id=workspace_id, job_id=job_id)
        job = authorized.metadata
        if job.state != 'SUCCEEDED':
            raise AccessReadError('RESOURCE_UNAVAILABLE')
        if job.completed_at - job.created_at > MAX_OUTPUT_LOOKUP_SPAN:
            raise AccessReadError('OUTPUT_LOOKUP_WINDOW_EXCEEDED')
        row = self._one(self._rows('outputs', OUTPUT_COLUMNS,
            'workspace_id=@workspace_id AND output_id=@output_id '
            'AND created_at>=@start_at AND created_at<=@end_at',
            workspace_id=workspace_id, output_id=job.output_id,
            start_at=job.created_at, end_at=job.completed_at))
        if row is None:
            raise AccessReadError('RESOURCE_UNAVAILABLE')
        _timestamp(row.get('created_at'))
        expected = dict(schema_version=SCHEMA_VERSION, workspace_id=workspace_id,
            conversation_id=job.conversation_id, owner_id=job.owner_id, output_id=job.output_id,
            job_id=job.job_id, request_id=job.request_id, attempt_id=job.active_attempt_id,
            canonical_sku=job.canonical_sku, source_system='SYNTHETIC_POC',
            approval_scope='SYNTHETIC_POC_ONLY', initial_review_status='NOT_REVIEWED', real_business_approved=False)
        if (any(row.get(key) != value for key, value in expected.items())
                or row.get('real_business_approved') is not False
                or not job.created_at <= row['created_at'] <= job.completed_at):
            raise AccessReadError('INVALID_ACCESS_RECORD')
        artifacts = row.get('artifacts', ())
        if not isinstance(artifacts, (list, tuple)) or len(artifacts) > len(ARTIFACT_TYPES):
            raise AccessReadError('INVALID_ACCESS_RECORD')
        summaries = []
        ids, kinds = set(), set()
        try:
            for artifact in artifacts:
                validate_artifact(artifact, workspace_id=workspace_id,
                    conversation_id=job.conversation_id, canonical_sku=job.canonical_sku,
                    output_id=job.output_id, limits=limits)
                if artifact['artifact_id'] in ids or artifact['kind'] in kinds:
                    raise ContractError('DUPLICATE_ARTIFACT')
                ids.add(artifact['artifact_id']); kinds.add(artifact['kind'])
                extension = ARTIFACT_TYPES[artifact['kind']][0]
                summaries.append(ArtifactSummary(artifact['artifact_id'], artifact['kind'],
                    f'{artifact["artifact_id"]}.{extension}', artifact['content_type'],
                    artifact['size_bytes'], artifact['sha256']))
        except (ContractError, KeyError, TypeError):
            raise AccessReadError('INVALID_ACCESS_RECORD') from None
        # Output queries can take time: reread membership and parent visibility
        # before releasing even a summary. This does not replace mutation fencing.
        membership = self._read_context(identity, workspace_id)
        access = self._job_parent(identity, membership, job)
        values = {key: row[key] for key in OutputMetadata.__dataclass_fields__}
        return AuthorizedOutput(OutputSummary(**values, downloads=tuple(summaries)), access)

    def read_artifact(self, identity: VerifiedIdentity, *, workspace_id: str, job_id: str,
                      artifact_id: str, limits: ArtifactLimits = DEFAULT_ARTIFACT_LIMITS) -> AuthorizedArtifact:
        """Resolve complete authoritative parents and manifest before any GCS read.

        Both partition windows come from the stored job, never browser dates.
        Requests older than the seven-day admission window fail closed. This
        method is also rerun after a download; its result cannot be cached.
        """
        membership = self._read_context(identity, workspace_id, Action.DOWNLOAD_ARTIFACT)
        _id(artifact_id)
        job = self._job_record(workspace_id, job_id, full=True)
        parent = self._conversation(workspace_id, job['conversation_id'], Action.DOWNLOAD_ARTIFACT)
        if parent.owner_id != job['owner_id']:
            raise AccessReadError('INVALID_ACCESS_RECORD')
        ensure_fresh(identity)
        require_authorized(identity.principal, membership, Action.DOWNLOAD_ARTIFACT,
                           workspace_id=workspace_id, resource=parent)
        if job['state'] != 'SUCCEEDED':
            raise AccessReadError('RESOURCE_UNAVAILABLE')
        if job['completed_at'] - job['created_at'] > MAX_OUTPUT_LOOKUP_SPAN:
            raise AccessReadError('OUTPUT_LOOKUP_WINDOW_EXCEEDED')
        request = self._one(self._rows('requests', FULL_REQUEST_COLUMNS,
            'workspace_id=@workspace_id AND request_id=@request_id '
            'AND created_at>=@start_at AND created_at<=@end_at',
            workspace_id=workspace_id, request_id=job['request_id'],
            start_at=job['created_at']-MAX_REQUEST_LOOKBACK, end_at=job['created_at']))
        output = self._one(self._rows('outputs', FULL_OUTPUT_COLUMNS,
            'workspace_id=@workspace_id AND output_id=@output_id '
            'AND created_at>=@start_at AND created_at<=@end_at',
            workspace_id=workspace_id, output_id=job['output_id'],
            start_at=job['created_at'], end_at=job['completed_at']))
        if request is None or output is None:
            raise AccessReadError('RESOURCE_UNAVAILABLE')
        try:
            validate_output(output, request=request, job=job, conversation=parent, limits=limits)
        except ContractError:
            raise AccessReadError('INVALID_ACCESS_RECORD') from None
        if not job['created_at']-MAX_REQUEST_LOOKBACK <= request['created_at'] <= job['created_at']:
            raise AccessReadError('REQUEST_LOOKUP_WINDOW_EXCEEDED')
        matches = [row for row in output['artifacts'] if row['artifact_id'] == artifact_id]
        if len(matches) != 1:
            raise AccessReadError('RESOURCE_UNAVAILABLE')
        # Reread current control rows after the potentially slow content queries.
        # Check sharing and ownership again after reading the content records.
        membership = self._read_context(identity, workspace_id, Action.DOWNLOAD_ARTIFACT)
        current_parent = self._conversation(workspace_id, parent.resource_id, Action.DOWNLOAD_ARTIFACT)
        if current_parent.owner_id != parent.owner_id:
            raise AccessReadError('INVALID_ACCESS_RECORD')
        ensure_fresh(identity)
        access = require_authorized(identity.principal, membership, Action.DOWNLOAD_ARTIFACT,
                                    workspace_id=workspace_id, resource=current_parent)
        return AuthorizedArtifact(
            OutputMetadata(**{key: output[key] for key in OutputMetadata.__dataclass_fields__}),
            ArtifactManifest(**matches[0]), canonical_json_bytes(output['brief']), access)


def main() -> None:
    """Read-only infrastructure/reader probe; makes no identity or role assignment."""
    parser = argparse.ArgumentParser(description='Probe empty authoritative access tables using ADC')
    parser.add_argument('--probe', action='store_true', required=True)
    parser.parse_args()
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    try:
        from src.ingestion.setup_workflow_tables import inspect
        report = inspect(client)
        if report['status'] != 'VERIFIED':
            raise AccessReadError('ACCESS_STORE_UNAVAILABLE')
        reader = BigQueryAccessReader(client)
        # Internal operator probe, not a public API. LIMIT 2 detects populated tables safely.
        counts = {name: len(reader._rows(name, '*', 'TRUE'))
                  for name in ('workspaces', 'memberships', 'conversations')}
        if any(counts.values()):
            raise AccessReadError('PROBE_REQUIRES_EMPTY_ACCESS_TABLES')
        # Validate projections and the output partition predicate without executing them.
        now = datetime.now().astimezone()
        job_query = reader._query('jobs', JOB_COLUMNS, 'workspace_id=@workspace_id AND job_id=@job_id',
            dry_run=True, workspace_id='__access_probe__', job_id='__access_probe__')
        output_query = reader._query('outputs', OUTPUT_COLUMNS,
            'workspace_id=@workspace_id AND output_id=@output_id AND created_at>=@start_at AND created_at<=@end_at',
            dry_run=True, workspace_id='__access_probe__', output_id='__access_probe__',
            start_at=now-timedelta(days=7), end_at=now)
        full_job_query = reader._query('jobs', FULL_JOB_COLUMNS,
            'workspace_id=@workspace_id AND job_id=@job_id',
            dry_run=True, workspace_id='__access_probe__', job_id='__access_probe__')
        request_query = reader._query('requests', FULL_REQUEST_COLUMNS,
            'workspace_id=@workspace_id AND request_id=@request_id AND created_at>=@start_at AND created_at<=@end_at',
            dry_run=True, workspace_id='__access_probe__', request_id='__access_probe__',
            start_at=now-MAX_REQUEST_LOOKBACK, end_at=now)
        full_output_query = reader._query('outputs', FULL_OUTPUT_COLUMNS,
            'workspace_id=@workspace_id AND output_id=@output_id AND created_at>=@start_at AND created_at<=@end_at',
            dry_run=True, workspace_id='__access_probe__', output_id='__access_probe__',
            start_at=now-MAX_OUTPUT_LOOKUP_SPAN, end_at=now)
        print(json.dumps(dict(status='VERIFIED_EMPTY_ACCESS_TABLES', dataset='ci_application',
            location=LOCATION, row_counts=counts, writes=0,
            protected_query_dry_runs=dict(jobs=job_query.total_bytes_processed,
                outputs=output_query.total_bytes_processed, download_jobs=full_job_query.total_bytes_processed,
                download_requests=request_query.total_bytes_processed,
                download_outputs=full_output_query.total_bytes_processed))))
    finally:
        client.close()


if __name__ == '__main__':
    main()
