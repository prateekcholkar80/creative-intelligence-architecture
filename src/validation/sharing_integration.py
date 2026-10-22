"""Opt-in ADC native sharing probe, isolated from every serving dataset.

Run: python -m src.validation.sharing_integration --run --output-dir PATH
Add --receipt-only for the focused read-only receipt recovery checks.
Add --retry-only for focused native sealed-attempt transaction checks.
Creates one fresh ci_sharing_it_<uuid> dataset and four expiring fixture tables.
No serving users, product rows, model calls, IAM changes or execution activation.
Report JSON is verification evidence, never a retrieval/database fallback.
"""
from __future__ import annotations

import argparse
import hashlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
from itertools import islice
import logging
from pathlib import Path
import re
import secrets
from threading import Barrier, Lock
from uuid import uuid4

from google.cloud import bigquery

from src.application.access_policy import Visibility
from src.application.identity import ISSUER, VerifiedIdentity, principal_for_subject
from src.application.sharing_coordinator_contract import (
    SharingCommitObservation, SharingCommitStatus, SharingPreparationState,
    make_sharing_command, prepare_sharing,
)
from src.application.sharing_executor import _normalize
from src.application.sharing_jobs import failed_sharing_job_status, matches_sharing_job
from src.application.sharing_intent import SharingAttempt, initial_attempt
from src.application.sharing_retry import SharingRetryAttempt, SharingRetrySealer, prepare_retry
from src.application.sharing_receipt import SharingReceiptQuery, validate_receipt_result
from src.application.sharing_query import SharingQuery, validate_sharing_result
from src.application.workflow_schema import LOCATION, SCHEMA_VERSION, TABLES
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT, _partition, _schema

LOG = logging.getLogger(__name__)
FIXTURE_TABLES = ('workspaces', 'memberships', 'conversations', 'audit_events')
CASES = ('apply', 'unknown', 'stale', 'rollback', 'postcommit', 'duplicate', 'race', 'revocation', 'sentinel')
TTL_MS = 2 * 60 * 60 * 1000


class IntegrationError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def require(condition: bool, code: str) -> None:
    if not condition:
        raise IntegrationError(code)


@dataclass(frozen=True)
class ProbeScope:
    run_id: str

    def __post_init__(self):
        require(isinstance(self.run_id, str) and re.fullmatch(r'[0-9a-f]{32}', self.run_id) is not None,
                'INVALID_PROBE_SCOPE')

    @property
    def dataset_id(self) -> str:
        return 'ci_sharing_it_' + self.run_id

    @property
    def dataset(self) -> str:
        return f'{PROJECT}.{self.dataset_id}'

    @property
    def labels(self) -> dict:
        return dict(purpose='sharing_integration', run_id=self.run_id)

    def table(self, name: str) -> str:
        require(name in FIXTURE_TABLES, 'UNRELATED_PROBE_TABLE')
        return self.dataset + '.' + name

    def assert_sql(self, sql: str) -> None:
        references = re.findall(r'`([^`]+)`', sql)
        require(bool(references) and all(ref in {self.table(n) for n in FIXTURE_TABLES} for ref in references),
                'UNSAFE_PROBE_SQL_SCOPE')

    def relocate(self, sql: str) -> str:
        for name in FIXTURE_TABLES:
            original = f'`{PROJECT}.ci_application.{name}`'
            require(original in sql, 'INVALID_SHARING_PROBE_TEMPLATE')
            sql = sql.replace(original, f'`{self.table(name)}`')
        self.assert_sql(sql)
        return sql

    def assert_owned(self, actual: bigquery.Dataset) -> None:
        require(actual.project == PROJECT and actual.dataset_id == self.dataset_id
                and actual.location.lower() == LOCATION and actual.labels == self.labels
                and actual.default_table_expiration_ms == TTL_MS, 'PROBE_OWNERSHIP_MISMATCH')


def fixture_rows(case: str, at: datetime) -> dict:
    require(case in CASES, 'INVALID_PROBE_CASE')
    principal = principal_for_subject(ISSUER, 'native-sharing-fixture:' + case).principal_id
    base = dict(schema_version=SCHEMA_VERSION, workspace_id='it_' + case)
    return dict(
        workspaces=base | dict(status='ACTIVE', revision=1, created_at=at, updated_at=at),
        memberships=base | dict(principal_id=principal, identity_issuer=ISSUER,
            identity_subject='native-sharing-fixture:' + case, roles=['CREATOR'], status='ACTIVE',
            revision=1, assignment_origin='ADMIN', updated_by=principal, created_at=at, updated_at=at),
        conversations=base | dict(conversation_id='conversation_' + case, owner_id=principal,
            visibility='PRIVATE', status='ACTIVE', access_revision=1, created_at=at, updated_at=at))


def sharing_query(case: str, *, retry_key='first', visibility=Visibility.TEAM,
                  membership_revision=1, conversation_revision=1, workspace_revision=1) -> SharingQuery:
    principal = principal_for_subject(ISSUER, 'native-sharing-fixture:' + case).principal_id
    command = make_sharing_command(workspace_id='it_' + case, conversation_id='conversation_' + case,
        principal_id=principal, visibility=visibility, expected_membership_revision=membership_revision,
        expected_conversation_revision=conversation_revision, retry_key=retry_key,
        accepted_at=datetime.now(timezone.utc) - timedelta(seconds=1), trace_id='native_' + uuid4().hex)
    return SharingQuery(command, workspace_revision,
                        SharingCommitObservation(command.audit_id, SharingCommitStatus.NOT_SUBMITTED))


def fault_variant(sql: str, fault: str) -> str:
    if fault == 'rollback':
        marker = "ASSERT @@row_count = 1 AS 'SHARING_WORKSPACE_UPDATE_CONFLICT';"
        require(sql.count(marker) == 1, 'INVALID_ROLLBACK_FAULT_MARKER')
        return sql.replace(marker, marker + "\n    ASSERT FALSE AS 'INTEGRATION_ROLLBACK';")
    require(fault == 'postcommit', 'INVALID_FAULT_VARIANT')
    marker = 'SELECT result_state AS state'
    require(sql.count(marker) == 1, 'INVALID_COMMIT_FAULT_MARKER')
    return sql.replace(marker, "ASSERT FALSE AS 'INTEGRATION_POSTCOMMIT';\nSELECT result_state AS state")


class NativeSharingProbe:
    def __init__(self, client: bigquery.Client, scope: ProbeScope):
        require(client.project == PROJECT and getattr(client, 'default_query_job_config', None) is None,
                'INVALID_PROBE_CLIENT')
        self.client, self.scope = client, scope
        self.owned = False
        self.jobs: list[str] = []
        self._lock = Lock()
        self.started_at = datetime.now(timezone.utc)
        self.report = dict(version='sharing-native-probe-v1', run_id=scope.run_id,
            project=PROJECT, location=LOCATION, source_system='SYNTHETIC_POC',
            dataset=scope.dataset, started_at=self.started_at.isoformat(), status='RUNNING',
            serving_data_writes=0, execution_activated=False, cases=[], jobs=[], cleanup='NOT_CREATED')

    def log(self, event: str, **fields) -> None:
        LOG.info(json.dumps(dict(event=event, run_id=self.scope.run_id, **fields)))

    def _new_job_id(self) -> str:
        with self._lock:
            job_id = 'share_it_' + self.scope.run_id + '_' + str(len(self.jobs)+1)
            self.jobs.append(job_id)
            return job_id

    def setup(self) -> None:
        # Read-only ADC/project/region check before any fixture resource exists.
        actual = self.client.get_dataset(f'{PROJECT}.ci_application', retry=None, timeout=10)
        require(actual.project == PROJECT and actual.location.lower() == LOCATION, 'PROJECT_PREFLIGHT_FAILED')
        resource = bigquery.Dataset(self.scope.dataset)
        resource.location = LOCATION
        resource.labels = self.scope.labels
        resource.default_table_expiration_ms = TTL_MS
        self.client.create_dataset(resource, exists_ok=False, retry=None, timeout=10)
        self.owned = True
        self.scope.assert_owned(self.client.get_dataset(self.scope.dataset, retry=None, timeout=10))
        self.report['cleanup'] = 'PENDING'
        for name in FIXTURE_TABLES:
            template = TABLES[name].table(PROJECT)
            target = bigquery.Table(self.scope.table(name), schema=template.schema)
            target.clustering_fields = template.clustering_fields
            target.time_partitioning = template.time_partitioning
            target.require_partition_filter = template.require_partition_filter
            self.client.create_table(target, exists_ok=False, retry=None, timeout=10)
            checked = self.client.get_table(target.reference, retry=None, timeout=10)
            require(_schema(checked.schema) == _schema(template.schema)
                    and checked.clustering_fields == template.clustering_fields
                    and _partition(checked) == _partition(template)
                    and bool(checked.require_partition_filter) == bool(template.require_partition_filter)
                    and checked.expires is not None, 'FIXTURE_SCHEMA_MISMATCH')
        at = self.started_at - timedelta(minutes=1)
        for name in FIXTURE_TABLES[:3]:
            rows = [fixture_rows(case, at)[name] for case in CASES]
            rows = [{key: value.isoformat() if isinstance(value, datetime) else value
                     for key, value in row.items()} for row in rows]
            destination = self.client.get_table(self.scope.table(name), retry=None, timeout=10)
            config = bigquery.LoadJobConfig(schema=destination.schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
            job_id = self._new_job_id()
            job = self.client.load_table_from_json(rows, destination, job_config=config,
                job_id=job_id, location=LOCATION, project=PROJECT, timeout=10,
                num_retries=0)
            job.result(timeout=60, retry=None)
        self.log('sharing_probe_fixtures_ready', tables=4, fixture_workspaces=len(CASES))

    def execute(self, sql: str, config: bigquery.QueryJobConfig | None = None, *,
                job_id: str | None = None, expected_failure=False, sharing: SharingQuery | None = None) -> dict:
        self.scope.assert_sql(sql)
        config = config or bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False,
                                                 maximum_bytes_billed=64*1024**2, job_timeout_ms=60000)
        job_id = self._new_job_id() if job_id is None else job_id
        try:
            job = self.client.query(sql, job_config=config, job_id=job_id, location=LOCATION,
                project=PROJECT, retry=None, job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=10)
        except Exception:
            job = self.client.get_job(job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        rows = None
        try:
            rows = [_normalize(row) for row in job.result(timeout=60, retry=None, job_retry=None)]
        except Exception:
            pass
        parent = self.client.get_job(job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        require(parent.state == 'DONE', 'NATIVE_JOB_NOT_TERMINAL')
        info = dict(job_id=job_id, state=parent.state, error_reason=(parent.error_result or {}).get('reason'),
                    bytes_processed=parent.total_bytes_processed, bytes_billed=parent.total_bytes_billed)
        with self._lock:
            self.report['jobs'].append(info)
        if sharing is not None:
            require(matches_sharing_job(parent, job_id=job_id, sql=sql, config=config), 'NATIVE_JOB_BINDING_MISMATCH')
        if parent.error_result:
            info['commit_observation'] = failed_sharing_job_status(self.client, job_id=job_id,
                timeout_seconds=10, parent=parent).value
            require(expected_failure, 'NATIVE_QUERY_FAILED')
            return dict(job_id=job_id, rows=None, info=info)
        require(not expected_failure and rows is not None, 'EXPECTED_FAILURE_OR_RESULT_MISSING')
        if sharing is not None:
            result = validate_sharing_result(sharing, rows, query_job_id=job_id)
            info['sharing_state'] = result.state.value
            info['resulting_revision'] = result.resulting_revision
        return dict(job_id=job_id, rows=rows, info=info)

    def share(self, query: SharingQuery, *, fault: str | None = None, expected_failure=False) -> dict:
        sql = self.scope.relocate(query.sql)
        if fault:
            sql = fault_variant(sql, fault)
        return self.execute(sql, query.job_config(), expected_failure=expected_failure, sharing=query)

    def snapshot(self, case: str) -> dict:
        result = {}
        config = bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False,
            maximum_bytes_billed=64*1024**2, job_timeout_ms=60000,
            query_parameters=[bigquery.ScalarQueryParameter('workspace_id', 'STRING', 'it_'+case),
                bigquery.ScalarQueryParameter('start', 'TIMESTAMP', self.started_at-timedelta(minutes=2)),
                bigquery.ScalarQueryParameter('end', 'TIMESTAMP', self.started_at+timedelta(hours=2))])
        for name in FIXTURE_TABLES:
            predicates = 'workspace_id=@workspace_id'
            if name == 'audit_events':
                predicates += ' AND occurred_at >= @start AND occurred_at < @end'
            order = ', '.join(TABLES[name].logical_key)
            result[name] = self.execute(f'SELECT * FROM `{self.scope.table(name)}` WHERE {predicates} ORDER BY {order}',
                                       config)['rows']
        return result

    def record(self, case: str, **evidence) -> None:
        self.report['cases'].append(dict(case=case, status='PASS', **evidence))
        self.log('sharing_probe_case_passed', case=case)

    def assert_applied(self, case: str, query: SharingQuery, revision=2) -> dict:
        state = self.snapshot(case)
        require(all(len(state[name]) == 1 for name in FIXTURE_TABLES), 'INVALID_APPLIED_ROW_COUNTS')
        require(state['workspaces'][0]['revision'] == revision
                and state['conversations'][0]['access_revision'] == revision
                and state['conversations'][0]['visibility'] == query.command.visibility.value
                and state['memberships'][0]['revision'] == 1
                and state['audit_events'][0]['audit_id'] == query.command.audit_id, 'INVALID_APPLIED_STATE')
        return state

    def _transaction_interval(self, job_id: str):
        children = list(self.client.list_jobs(parent_job=job_id, project=PROJECT, max_results=64,
            page_size=64, retry=None, timeout=10))
        require(len(children) < 64, 'TRUNCATED_RACE_EVIDENCE')
        require(all(isinstance(j, bigquery.QueryJob) and j.project == PROJECT and j.location == LOCATION
                    and j.parent_job_id == job_id and j.state == 'DONE' for j in children),
                'INVALID_RACE_CHILD_CORRELATION')
        begin = [j.ended for j in children if j.statement_type == 'BEGIN_TRANSACTION']
        end = [j.started for j in children if j.statement_type in ('COMMIT_TRANSACTION', 'ROLLBACK_TRANSACTION')]
        return (begin[0], end[0]) if len(begin) == len(end) == 1 and all(begin+end) else None

    def race(self, left: SharingQuery, right: SharingQuery) -> tuple[list[dict], bool]:
        barrier = Barrier(2)
        def work(query):
            sql = self.scope.relocate(query.sql)
            job_id = self._new_job_id()
            barrier.wait(timeout=10)
            try:
                return self.execute(sql, query.job_config(), job_id=job_id, sharing=query)
            except IntegrationError as error:
                require(error.code == 'NATIVE_QUERY_FAILED', error.code)
                return dict(info=next(item for item in self.report['jobs'] if item['job_id'] == job_id), failed=True)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(work, query) for query in (left, right)]
            outcomes = [future.result(timeout=90) for future in futures]
        intervals = [self._transaction_interval(item['info']['job_id']) for item in outcomes]
        for item, interval in zip(outcomes, intervals):
            item['info']['transaction_interval'] = ([at.isoformat() for at in interval] if interval else None)
        overlap = bool(all(intervals) and max(i[0] for i in intervals) < min(i[1] for i in intervals))
        return outcomes, overlap

    def cases(self) -> None:
        sentinel = self.snapshot('sentinel')
        q = sharing_query('apply')
        apply = self.share(q)
        before = self.assert_applied('apply', q)
        reuse = self.share(q)
        require(reuse['info']['sharing_state'] == 'REUSED' and self.snapshot('apply') == before, 'REUSE_MUTATED_STATE')
        self.record('apply_and_reuse', audit_rows=1, revision=2, native_job_binding=True)

        conflicting_command = replace(q.command, trace_id='changed_native_trace')
        conflicting = SharingQuery(conflicting_command, q.expected_workspace_revision,
            SharingCommitObservation(conflicting_command.audit_id, SharingCommitStatus.NOT_SUBMITTED))
        failure = self.share(conflicting, expected_failure=True)
        require(self.snapshot('apply') == before and failure['info']['commit_observation'] == 'NOT_COMMITTED',
                'CONFLICT_CHANGED_STATE')
        self.record('changed_intent_conflict', commit_observation='NOT_COMMITTED')

        later = sharing_query('apply', retry_key='later', visibility=Visibility.PRIVATE,
                              conversation_revision=2, workspace_revision=2)
        self.share(later)
        later_state = self.snapshot('apply')
        reused = self.share(q)
        require(reused['info']['resulting_revision'] == 2 and self.snapshot('apply') == later_state
                and later_state['conversations'][0]['access_revision'] == 3
                and later_state['conversations'][0]['visibility'] == 'PRIVATE', 'HISTORICAL_REUSE_RESET_STATE')
        self.record('historical_reuse_preserves_later_change', original_revision=2, current_revision=3)

        unknown = sharing_query('unknown')
        unknown = replace(unknown, commit_observation=SharingCommitObservation(
            unknown.command.audit_id, SharingCommitStatus.UNKNOWN, 'prior_uncertain_job'))
        baseline = self.snapshot('unknown')
        outcome = self.share(unknown)
        require(outcome['info']['sharing_state'] == 'UNKNOWN' and self.snapshot('unknown') == baseline,
                'UNKNOWN_PRODUCED_MUTATION')
        self.record('unknown_without_receipt', mutations=0)

        baseline = self.snapshot('stale')
        stale = self.share(sharing_query('stale', membership_revision=2), expected_failure=True)
        require(self.snapshot('stale') == baseline and stale['info']['commit_observation'] == 'NOT_COMMITTED',
                'STALE_WRITE_NOT_ROLLED_BACK')
        self.record('stale_membership', mutations=0)

        baseline = self.snapshot('rollback')
        failed = self.share(sharing_query('rollback'), fault='rollback', expected_failure=True)
        require(self.snapshot('rollback') == baseline and failed['info']['commit_observation'] == 'NOT_COMMITTED',
                'INJECTED_ROLLBACK_FAILED')
        self.record('rollback_after_fence_update', mutations=0, commit_observation='NOT_COMMITTED')

        committed = sharing_query('postcommit')
        failed = self.share(committed, fault='postcommit', expected_failure=True)
        self.assert_applied('postcommit', committed)
        require(failed['info']['commit_observation'] == 'COMMITTED', 'POSTCOMMIT_FAILURE_MISCLASSIFIED')
        self.record('failed_result_after_commit', audit_rows=1, commit_observation='COMMITTED')

        duplicate = sharing_query('duplicate')
        sql, config = self.scope.relocate(duplicate.sql), duplicate.job_config()
        job_id = self._new_job_id()
        barrier = Barrier(2)
        def submit_same_id():
            barrier.wait(timeout=10)
            try:
                self.client.query(sql, job_config=config, job_id=job_id, project=PROJECT,
                    location=LOCATION, retry=None, job_retry=None, timeout=10)
                return 'ACCEPTED'
            except Exception:
                return 'RECONCILE'
        with ThreadPoolExecutor(max_workers=2) as pool:
            submitted = [pool.submit(submit_same_id) for _ in range(2)]
            statuses = [future.result(timeout=30) for future in submitted]
        parent = self.client.get_job(job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        rows = [_normalize(row) for row in parent.result(timeout=60, retry=None, job_retry=None)]
        require(matches_sharing_job(parent, job_id=job_id, sql=sql, config=config), 'DUPLICATE_JOB_BINDING_MISMATCH')
        validate_sharing_result(duplicate, rows, query_job_id=job_id)
        self.assert_applied('duplicate', duplicate)
        # Drop/disregard the submission responses and recover only this original ID.
        recovered = self.client.get_job(job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        recovered_rows = [_normalize(row) for row in recovered.result(timeout=60, retry=None, job_retry=None)]
        require(rows == recovered_rows, 'LOST_RESPONSE_RECOVERY_CHANGED_RESULT')
        self.record('duplicate_submission_and_discarded_response_recovery', job_id=job_id,
                    submission_observations=statuses, audit_rows=1)

        left, right = sharing_query('race', retry_key='left'), sharing_query('race', retry_key='right')
        outcomes, overlap = self.race(left, right)
        winners = [item for item in outcomes if not item.get('failed')]
        require(len(winners) == 1 and overlap, 'RACE_WINNER_OR_OVERLAP_NOT_VERIFIED')
        state = self.snapshot('race')
        require(len(state['audit_events']) == 1 and state['workspaces'][0]['revision'] == 2
                and state['conversations'][0]['access_revision'] == 2, 'RACE_DOUBLE_WRITE')
        self.record('distinct_intent_transaction_race', overlapping_transactions=True, committed_mutations=1)

        revoked = sharing_query('revocation')
        # Controlled test-only management participant: revoke and fence atomically.
        sql = f'''BEGIN TRANSACTION;
BEGIN
 UPDATE `{self.scope.table('workspaces')}` SET revision=revision+1, updated_at=CURRENT_TIMESTAMP()
 WHERE workspace_id=@workspace_id AND revision=1;
 ASSERT @@row_count=1 AS 'REVOCATION_FENCE_CONFLICT';
 UPDATE `{self.scope.table('memberships')}` SET status='REVOKED', revision=revision+1,
 updated_at=CURRENT_TIMESTAMP() WHERE workspace_id=@workspace_id AND status='ACTIVE';
 ASSERT @@row_count=1 AS 'REVOCATION_MEMBER_CONFLICT';
 COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; RAISE; END;'''
        config = bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False,
            maximum_bytes_billed=64*1024**2, job_timeout_ms=60000,
            query_parameters=[bigquery.ScalarQueryParameter('workspace_id', 'STRING', 'it_revocation')])
        self.execute(sql, config)
        baseline = self.snapshot('revocation')
        self.share(revoked, expected_failure=True)
        require(self.snapshot('revocation') == baseline and not baseline['audit_events'], 'REVOKED_ACTOR_WROTE')
        self.record('fenced_revocation_blocks_sharing', mutations_after_revocation=0)

        require(self.snapshot('sentinel') == sentinel, 'UNRELATED_FIXTURE_CHANGED')
        self.record('unrelated_fixture_preserved', serving_data_writes=0)

    def read_receipt(self, intent: SharingQuery, *, original_job_id: str | None = None):
        query = SharingReceiptQuery(intent)
        sql = self.scope.relocate(query.sql)
        response = self.execute(sql, query.job_config())
        parent = self.client.get_job(response['job_id'], project=PROJECT, location=LOCATION,
                                     retry=None, timeout=10)
        require(matches_sharing_job(parent, job_id=response['job_id'], sql=sql,
                                    config=query.job_config()), 'NATIVE_RECEIPT_BINDING_MISMATCH')
        result = validate_receipt_result(query, response['rows'],
            original_job_id=original_job_id or initial_attempt(intent).job_id)
        response['info'].update(receipt_state=result.state.value, resulting_revision=result.resulting_revision)
        return result

    def receipt_cases(self) -> None:
        """Focused native verification for the new SELECT, without rerunning races."""
        sentinel = self.snapshot('sentinel')
        intent = sharing_query('apply')
        before = self.snapshot('apply')
        missing = self.read_receipt(intent)
        require(missing.state.value == 'UNKNOWN' and missing.resulting_revision is None
                and self.snapshot('apply') == before, 'MISSING_RECEIPT_CHANGED_STATE')
        self.record('receipt_absence_stays_unknown', mutations=0)

        applied = self.share(intent)
        before = self.assert_applied('apply', intent)
        recovered = self.read_receipt(intent, original_job_id=applied['job_id'])
        require(recovered.state.value == 'REUSED' and recovered.resulting_revision == 2
                and self.snapshot('apply') == before, 'RECEIPT_RECOVERY_CHANGED_STATE')
        self.record('receipt_recovers_committed_intent', original_revision=2, recovery_mutations=0)

        changed_command = replace(intent.command, trace_id='changed_receipt_trace')
        changed = SharingQuery(changed_command, intent.expected_workspace_revision,
            SharingCommitObservation(changed_command.audit_id, SharingCommitStatus.NOT_SUBMITTED))
        try:
            self.read_receipt(changed)
        except ContractError as error:
            require(error.code == 'SHARING_IDEMPOTENCY_CONFLICT', 'WRONG_RECEIPT_CONFLICT')
        else:
            raise IntegrationError('CHANGED_RECEIPT_INTENT_ACCEPTED')
        require(self.snapshot('apply') == before, 'RECEIPT_CONFLICT_CHANGED_STATE')
        self.record('receipt_prefix_detects_changed_envelope', recovery_mutations=0)

        later = sharing_query('apply', retry_key='later_receipt', visibility=Visibility.PRIVATE,
                              conversation_revision=2, workspace_revision=2)
        self.share(later)
        before = self.snapshot('apply')
        recovered = self.read_receipt(intent, original_job_id=applied['job_id'])
        require(recovered.resulting_revision == 2 and before['conversations'][0]['access_revision'] == 3
                and before['conversations'][0]['visibility'] == 'PRIVATE'
                and self.snapshot('apply') == before, 'RECEIPT_RECOVERY_RESET_LATER_STATE')
        self.record('receipt_preserves_later_visibility', original_revision=2, current_revision=3)

        postcommit = sharing_query('postcommit')
        failed = self.share(postcommit, fault='postcommit', expected_failure=True)
        require(failed['info']['commit_observation'] == 'COMMITTED', 'POSTCOMMIT_OUTCOME_NOT_VERIFIED')
        before = self.assert_applied('postcommit', postcommit)
        recovered = self.read_receipt(postcommit, original_job_id=failed['job_id'])
        require(recovered.state.value == 'REUSED' and recovered.resulting_revision == 2
                and self.snapshot('postcommit') == before, 'POSTCOMMIT_RECEIPT_NOT_RECOVERED')
        self.record('receipt_recovers_failed_postcommit_result', audit_rows=1, recovery_mutations=0)
        require(self.snapshot('sentinel') == sentinel, 'UNRELATED_FIXTURE_CHANGED')
        self.record('receipt_unrelated_fixture_preserved', serving_data_writes=0)

    def cleanup(self) -> None:
        if not self.owned:
            return
        self.scope.assert_owned(self.client.get_dataset(self.scope.dataset, retry=None, timeout=10))
        names = {table.table_id for table in self.client.list_tables(self.scope.dataset, retry=None, timeout=10)}
        require(names <= set(FIXTURE_TABLES), 'UNEXPECTED_RESOURCE_IN_PROBE_DATASET')
        for job_id in self.jobs:
            job = self.client.get_job(job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
            require(job.state == 'DONE', 'CLEANUP_BLOCKED_BY_UNRESOLVED_JOB')
        self.client.delete_dataset(self.scope.dataset, delete_contents=True, not_found_ok=False,
                                   retry=None, timeout=10)
        self.report['cleanup'] = 'DELETED_OWNED_DATASET'
        self.log('sharing_probe_cleanup_complete')


class NativeRetryChecks:
    """Native primitives with sealed IDs; NOT a deployed IAP/executor invocation.

    Only this probe accepts relocated SQL or explicit injected faults. Serving
    job correlation must reject these jobs. Keys/fixture identities are ephemeral
    contract fixtures, never actual IAP authentication or application membership.
    """
    def __init__(self, probe: NativeSharingProbe):
        self.probe = probe
        self.sealer = SharingRetrySealer(key=secrets.token_bytes(32), key_id='native_' + probe.scope.run_id)
        self.bindings: dict[str, str] = {}
        self.probe.report['sealed_attempts'] = []

    def roundtrip(self, attempt: SharingAttempt | SharingRetryAttempt):
        command = attempt.query.command
        case = command.workspace_id.removeprefix('it_')
        require(case in CASES, 'INVALID_RETRY_FIXTURE_IDENTITY')
        subject = 'native-sharing-fixture:' + case
        epoch = int(datetime.now(timezone.utc).timestamp())
        identity = VerifiedIdentity(principal_for_subject(ISSUER, subject), ISSUER, subject,
            '/projects/1/locations/asia-south1/services/sharing-fixture', epoch-1, epoch+599)
        if isinstance(attempt, SharingRetryAttempt):
            restored = self.sealer.resolve_retry(self.sealer.seal_retry(attempt), identity)
        else:
            restored = self.sealer.resolve(self.sealer.seal(attempt.query), identity)
        require(restored == attempt, 'NATIVE_RETRY_SEAL_MISMATCH')
        return restored

    def register(self, attempt: SharingAttempt | SharingRetryAttempt, *, fault: str | None = None) -> str:
        # Validate before allocating a native ID or accepting a changed SQL binding.
        self.roundtrip(attempt)
        sql = self.probe.scope.relocate(attempt.query.sql)
        if fault:
            sql = fault_variant(sql, fault)
        self.probe.scope.assert_sql(sql)
        if attempt.job_id in self.bindings:
            require(self.bindings[attempt.job_id] == sql, 'PROBE_ATTEMPT_SQL_CONFLICT')
            return sql
        self.bindings[attempt.job_id] = sql
        with self.probe._lock:
            require(attempt.job_id not in self.probe.jobs, 'PROBE_ATTEMPT_ID_CONFLICT')
            self.probe.jobs.append(attempt.job_id)
            # Digests/IDs only; never retain a signing key, token or raw subject.
            self.probe.report['sealed_attempts'].append(dict(job_id=attempt.job_id,
                contract_fingerprint=attempt.fingerprint,
                isolated_sql_sha256=hashlib.sha256(sql.encode()).hexdigest(),
                fault=fault, ordinal=len(attempt.steps) if isinstance(attempt, SharingRetryAttempt) else 0))
        return sql

    def execute(self, attempt: SharingAttempt | SharingRetryAttempt, *, fault: str | None = None,
                expected_failure=False):
        sql = self.register(attempt, fault=fault)
        return self.probe.execute(sql, attempt.query.job_config(), job_id=attempt.job_id,
            sharing=attempt.query, expected_failure=expected_failure)

    def observation(self, attempt: SharingAttempt | SharingRetryAttempt) -> SharingCommitObservation:
        status = SharingCommitStatus.UNKNOWN
        sql = self.bindings.get(attempt.job_id) or self.probe.scope.relocate(attempt.query.sql)
        try:
            parent = self.probe.client.get_job(attempt.job_id, project=PROJECT, location=LOCATION,
                                              retry=None, timeout=10)
            if matches_sharing_job(parent, job_id=attempt.job_id, sql=sql,
                                   config=attempt.query.job_config()) and parent.state == 'DONE':
                if parent.error_result is not None:
                    status = failed_sharing_job_status(self.probe.client, job_id=attempt.job_id,
                        timeout_seconds=10, parent=parent)
                else:
                    rows = [_normalize(row) for row in islice(parent.result(max_results=2, page_size=2,
                        timeout=60, retry=None, job_retry=None), 2)]
                    validate_sharing_result(attempt.query, rows, query_job_id=attempt.job_id)
                    status = SharingCommitStatus.COMMITTED
        except Exception:
            pass
        return SharingCommitObservation(attempt.query.command.audit_id, status, attempt.job_id)

    def context(self, attempt: SharingAttempt | SharingRetryAttempt):
        query = SharingReceiptQuery(attempt.query)
        sql = self.probe.scope.relocate(query.sql)
        response = self.probe.execute(sql, query.job_config())
        parent = self.probe.client.get_job(response['job_id'], project=PROJECT, location=LOCATION,
                                          retry=None, timeout=10)
        require(matches_sharing_job(parent, job_id=response['job_id'], sql=sql,
            config=query.job_config()), 'NATIVE_RETRY_CONTEXT_BINDING_MISMATCH')
        result = validate_receipt_result(query, response['rows'], original_job_id=attempt.job_id)
        response['info'].update(receipt_state=result.state.value, resulting_revision=result.resulting_revision)
        return response['rows'][0], result

    def prepare(self, predecessor: SharingAttempt | SharingRetryAttempt):
        ancestors = [predecessor]
        if isinstance(predecessor, SharingRetryAttempt):
            ancestors = [predecessor.original] + [SharingRetryAttempt(predecessor.original, predecessor.steps[:i])
                for i in range(1, len(predecessor.steps)+1)]
        observations = [self.observation(attempt) for attempt in ancestors]
        row, result = self.context(predecessor)
        if result.state.value == 'REUSED':
            return 'REUSED', None, result
        if any(item.status is not SharingCommitStatus.NOT_COMMITTED for item in observations):
            return 'RECOVERY_REQUIRED', None, result
        attempt = prepare_retry(predecessor, commit_observation=observations[-1],
            expected_workspace_revision=row['workspaces'][0]['revision'], observed_at=row['observed_at'])
        plan = prepare_sharing(attempt.query.command, workspace_rows=row['workspaces'],
            membership_rows=row['memberships'], conversation_rows=row['conversations'],
            receipt_rows=row['audit_events'], expected_workspace_revision=attempt.query.expected_workspace_revision,
            observed_at=row['observed_at'], commit_observation=attempt.query.commit_observation)
        require(plan.state is SharingPreparationState.PREPARED, 'NATIVE_RETRY_NOT_PREPARED')
        return 'READY', self.roundtrip(attempt), result

    def fence(self, case: str, revision: int, *, revoke=False):
        require(case in CASES, 'INVALID_PROBE_CASE')
        sql = f'''BEGIN TRANSACTION;
BEGIN
 UPDATE `{self.probe.scope.table('workspaces')}` SET revision=revision+1, updated_at=CURRENT_TIMESTAMP()
 WHERE workspace_id=@workspace_id AND revision=@revision AND status='ACTIVE';
 ASSERT @@row_count=1 AS 'NATIVE_RETRY_FENCE_CONFLICT';
 IF @revoke THEN
   UPDATE `{self.probe.scope.table('memberships')}` SET status='REVOKED', revision=revision+1,
     updated_at=CURRENT_TIMESTAMP() WHERE workspace_id=@workspace_id AND status='ACTIVE';
   ASSERT @@row_count=1 AS 'NATIVE_RETRY_REVOCATION_CONFLICT';
 END IF;
 COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; RAISE; END;'''
        config = bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False,
            maximum_bytes_billed=64*1024**2, job_timeout_ms=60000, query_parameters=[
                bigquery.ScalarQueryParameter('workspace_id', 'STRING', 'it_'+case),
                bigquery.ScalarQueryParameter('revision', 'INT64', revision),
                bigquery.ScalarQueryParameter('revoke', 'BOOL', revoke)])
        self.probe.execute(sql, config)

    def aborted(self, case: str) -> SharingAttempt:
        original = self.roundtrip(initial_attempt(sharing_query(case)))
        self.fence(case, 1)
        before = self.probe.snapshot(case)
        failed = self.execute(original, expected_failure=True)
        require(failed['info']['commit_observation'] == 'NOT_COMMITTED'
                and self.probe.snapshot(case) == before, 'NATIVE_RETRY_PREDECESSOR_NOT_ABORTED')
        return original

    def ready(self, predecessor: SharingAttempt | SharingRetryAttempt) -> SharingRetryAttempt:
        state, attempt, _ = self.prepare(predecessor)
        require(state == 'READY' and isinstance(attempt, SharingRetryAttempt), 'NATIVE_RETRY_NOT_READY')
        return attempt

    def assert_applied(self, case: str, attempt: SharingRetryAttempt):
        state = self.probe.snapshot(case)
        require(all(len(state[name]) == 1 for name in FIXTURE_TABLES)
                and state['workspaces'][0]['revision'] == attempt.query.expected_workspace_revision+1
                and state['conversations'][0]['access_revision'] == attempt.query.command.expected_conversation_revision+1
                and state['conversations'][0]['visibility'] == attempt.query.command.visibility.value
                and state['memberships'][0]['revision'] == attempt.query.command.expected_membership_revision
                and state['audit_events'][0]['audit_id'] == attempt.query.command.audit_id,
                'NATIVE_RETRY_APPLIED_STATE_MISMATCH')
        return state

    def race(self, left: SharingRetryAttempt, right: SharingRetryAttempt):
        barrier = Barrier(2)
        for attempt in (left, right):
            self.register(attempt)
        def submit(attempt):
            barrier.wait(timeout=10)
            try:
                return self.execute(attempt)
            except IntegrationError as error:
                require(error.code == 'NATIVE_QUERY_FAILED', error.code)
                return dict(info=next(item for item in self.probe.report['jobs']
                                     if item['job_id'] == attempt.job_id), failed=True)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(submit, attempt) for attempt in (left, right)]
            outcomes = [future.result(timeout=90) for future in futures]
        intervals = [self.probe._transaction_interval(attempt.job_id) for attempt in (left, right)]
        for item, interval in zip(outcomes, intervals):
            item['info']['transaction_interval'] = [at.isoformat() for at in interval] if interval else None
        require(all(intervals) and max(i[0] for i in intervals) < min(i[1] for i in intervals),
                'NATIVE_RETRY_OVERLAP_NOT_VERIFIED')
        require(sum(item['info'].get('sharing_state') == 'APPLIED' for item in outcomes) == 1,
                'NATIVE_RETRY_MULTIPLE_APPLIED')
        return outcomes

    def cases(self):
        probe = self.probe
        sentinel = probe.snapshot('sentinel')
        unknown = initial_attempt(sharing_query('unknown'))
        before = probe.snapshot('unknown')
        state, attempt, result = self.prepare(unknown)
        require(state == 'RECOVERY_REQUIRED' and attempt is None and result.state.value == 'UNKNOWN'
                and probe.snapshot('unknown') == before, 'NATIVE_UNKNOWN_REMINTED_RETRY')
        probe.record('retry_missing_predecessor_is_recovery_only', new_mutations=0)

        original = self.aborted('rollback')
        attempt = self.ready(original)
        require(attempt.query.command == original.query.command and attempt.query.expected_workspace_revision == 2,
                'NATIVE_RETRY_CHANGED_ENVELOPE')
        self.execute(attempt)
        self.assert_applied('rollback', attempt)
        parent = probe.client.get_job(attempt.job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        require(not matches_sharing_job(parent, job_id=attempt.job_id, sql=attempt.query.sql,
            config=attempt.query.job_config()), 'ISOLATED_JOB_ACCEPTED_AS_SERVING_JOB')
        probe.record('retry_after_positive_rollback', audit_rows=1, workspace_revision=3,
                     conversation_revision=2, serving_job_binding_rejected=True, sealed_job_id=attempt.job_id)

        duplicate = self.ready(self.aborted('duplicate'))
        sql = self.register(duplicate)
        barrier = Barrier(2)
        def duplicate_submit():
            barrier.wait(timeout=10)
            try:
                probe.client.query(sql, job_config=duplicate.query.job_config(), job_id=duplicate.job_id,
                    project=PROJECT, location=LOCATION, retry=None, job_retry=None, timeout=10,
                    api_method=bigquery.enums.QueryApiMethod.INSERT)
                return 'ACCEPTED'
            except Exception:
                return 'RECONCILE'
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(duplicate_submit) for _ in range(2)]
            statuses = [future.result(timeout=30) for future in futures]
        parent = probe.client.get_job(duplicate.job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        parent.result(timeout=60, retry=None, job_retry=None)
        require(self.observation(duplicate).status is SharingCommitStatus.COMMITTED,
                'NATIVE_DUPLICATE_RETRY_NOT_RECOVERED')
        before = self.assert_applied('duplicate', duplicate)
        probe.report['jobs'].append(dict(job_id=duplicate.job_id, state=parent.state, error_reason=None,
            bytes_processed=parent.total_bytes_processed, bytes_billed=parent.total_bytes_billed,
            commit_observation='COMMITTED'))
        require(self.observation(duplicate).status is SharingCommitStatus.COMMITTED
                and probe.snapshot('duplicate') == before, 'NATIVE_DUPLICATE_RETRY_MUTATED_AGAIN')
        probe.record('retry_duplicate_and_discarded_response_recovery', sealed_job_id=duplicate.job_id,
                     submission_observations=statuses, audit_rows=1)

        root = self.aborted('race')
        left = self.ready(root)
        self.fence('race', 2)
        right = self.ready(root)
        require(left.job_id != right.job_id and left.query.command == right.query.command,
                'NATIVE_RETRY_COMPETITORS_NOT_DISTINCT')
        self.race(left, right)
        before = self.assert_applied('race', right)
        probe.record('retry_competing_fences_overlap', overlapping_transactions=True,
                     committed_mutations=1, audit_rows=1)
        state, attempt, result = self.prepare(left)
        require(state == 'REUSED' and attempt is None and result.resulting_revision == 2
                and probe.snapshot('race') == before, 'NATIVE_COMPETING_RECEIPT_REMINTED')
        probe.record('retry_other_attempt_receipt_reused', original_revision=2, new_mutations=0)

        stale = self.ready(self.aborted('stale'))
        self.fence('stale', 2)
        before = probe.snapshot('stale')
        failed = self.execute(stale, expected_failure=True)
        require(failed['info']['commit_observation'] == 'NOT_COMMITTED'
                and probe.snapshot('stale') == before, 'NATIVE_STALE_RETRY_MUTATED')
        fresh = self.ready(stale)
        require(fresh.query.expected_workspace_revision == 3 and fresh.query.command == stale.query.command
                and fresh.job_id != stale.job_id, 'NATIVE_STALE_RETRY_NOT_EXPLICITLY_REFRESHED')
        self.execute(fresh)
        self.assert_applied('stale', fresh)
        probe.record('retry_stale_fence_then_explicit_next_attempt', workspace_revision=4,
                     conversation_revision=2, audit_rows=1)

        revoked = self.ready(self.aborted('revocation'))
        self.fence('revocation', 2, revoke=True)
        before = probe.snapshot('revocation')
        failed = self.execute(revoked, expected_failure=True)
        require(failed['info']['commit_observation'] == 'NOT_COMMITTED'
                and probe.snapshot('revocation') == before and not before['audit_events'],
                'NATIVE_REVOKED_RETRY_MUTATED')
        from src.application.access_policy import AuthorizationError
        try:
            self.prepare(revoked)
        except AuthorizationError:
            pass
        else:
            raise IntegrationError('NATIVE_REVOKED_RETRY_REISSUED')
        probe.record('retry_revocation_blocks_dispatch_and_reissue', mutations_after_revocation=0)

        postcommit = self.ready(self.aborted('postcommit'))
        failed = self.execute(postcommit, fault='postcommit', expected_failure=True)
        require(failed['info']['commit_observation'] == 'COMMITTED', 'NATIVE_RETRY_COMMIT_MISCLASSIFIED')
        before = self.assert_applied('postcommit', postcommit)
        state, attempt, result = self.prepare(postcommit)
        require(state == 'REUSED' and attempt is None and result.resulting_revision == 2
                and probe.snapshot('postcommit') == before, 'NATIVE_RETRY_POSTCOMMIT_REMINTED')
        probe.record('retry_postcommit_failure_recovers_receipt', audit_rows=1, new_mutations=0)

        historical = self.ready(self.aborted('apply'))
        self.execute(historical)
        later = initial_attempt(sharing_query('apply', retry_key='later_native_retry', visibility=Visibility.PRIVATE,
                                             conversation_revision=2, workspace_revision=3))
        self.execute(later)
        before = probe.snapshot('apply')
        state, attempt, result = self.prepare(historical)
        require(state == 'REUSED' and attempt is None and result.resulting_revision == 2
                and before['conversations'][0]['visibility'] == 'PRIVATE'
                and before['conversations'][0]['access_revision'] == 3
                and len(before['audit_events']) == 2 and probe.snapshot('apply') == before,
                'NATIVE_RETRY_HISTORICAL_REUSE_RESET_STATE')
        probe.record('retry_preserves_later_visibility', original_revision=2, current_revision=3, new_mutations=0)
        require(probe.snapshot('sentinel') == sentinel, 'UNRELATED_FIXTURE_CHANGED')
        probe.record('retry_unrelated_fixture_preserved', serving_data_writes=0)


def run_probe(client: bigquery.Client, output_dir: Path, *, receipt_only: bool = False,
              retry_only: bool = False) -> dict:
    require(type(receipt_only) is bool and type(retry_only) is bool and not (receipt_only and retry_only),
            'INVALID_PROBE_MODE')
    output_dir.mkdir(parents=True, exist_ok=True)
    probe = NativeSharingProbe(client, ProbeScope(uuid4().hex))
    probe.report['mode'] = 'RETRY_ONLY' if retry_only else 'RECEIPT_ONLY' if receipt_only else 'TRANSACTION'
    try:
        probe.setup()
        if retry_only:
            NativeRetryChecks(probe).cases()
        elif receipt_only:
            probe.receipt_cases()
        else:
            probe.cases()
        probe.report['status'] = 'PASS'
    except Exception as error:
        probe.report.update(status='FAIL', error_code=getattr(error, 'code', None)
            if isinstance(error, (IntegrationError, ContractError)) else type(error).__name__)
    finally:
        try:
            probe.cleanup()
        except Exception as error:
            probe.report.update(status='FAIL', cleanup='REFUSED_OR_FAILED',
                cleanup_error=error.code if isinstance(error, IntegrationError) else type(error).__name__)
        probe.report['finished_at'] = datetime.now(timezone.utc).isoformat()
        path = output_dir / f'{probe.scope.run_id}.json'
        path.write_text(json.dumps(probe.report, indent=2)+'\n', encoding='utf-8')
        probe.report['report_path'] = str(path)
    return probe.report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', required=True, help='Opt in to isolated native fixture writes')
    parser.add_argument('--output-dir', type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--receipt-only', action='store_true', help='Run focused receipt SELECT checks')
    mode.add_argument('--retry-only', action='store_true', help='Run focused native sealed retry checks')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    try:
        report = run_probe(client, args.output_dir, receipt_only=args.receipt_only, retry_only=args.retry_only)
    finally:
        client.close()
    print(json.dumps({key: report.get(key) for key in ('status', 'error_code', 'dataset', 'cleanup', 'report_path')}))
    if report['status'] != 'PASS':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
