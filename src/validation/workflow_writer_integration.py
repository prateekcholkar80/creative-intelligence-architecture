"""Priority 4.6 isolated native workflow-writer verification.

This opt-in probe uses ADC and owns one expiring BigQuery dataset. It relocates
the production terminal-failure transaction into that dataset, exercises the
disabled-by-default writer executor, and deletes only the owned dataset. It
never calls a model, reads GCS, or writes a serving workflow table.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_codec import decode_row
from src.application.workflow_failure_query import WorkflowFailurePlan, prepare_workflow_failure
from src.application.workflow_schema import TABLES
from src.application.workflow_writer_executor import (
    WorkflowWriterExecutor, WorkflowWriterSettings,
)
from src.ingestion.setup_workflow_tables import DATASET_ID, LOCATION, PROJECT, _partition, _schema
from src.validation.control_fixtures import workflow
from src.validation.signing_read_integration import TTL_MS, json_rows, require

NATIVE_WRITE_SETTINGS = SharingQuerySettings(maximum_bytes_billed=512 * 1024**2)


class Scope:
    def __init__(self, run_id: str):
        require(re.fullmatch(r'[0-9a-f]{32}', run_id) is not None, 'INVALID_SCOPE')
        self.run_id = run_id
        self.dataset = f'{PROJECT}.ci_workflow_writer_it_{run_id}'
        self.labels = {'purpose': 'workflow_writer_integration', 'run_id': run_id}

    def table(self, name: str) -> str:
        require(name in TABLES, 'UNRELATED_TABLE')
        return f'{self.dataset}.{name}'

    def sql(self, value: str) -> str:
        for name in TABLES:
            value = value.replace(f'`{DATASET_ID}.{name}`', f'`{self.table(name)}`')
        references = re.findall(r'`([^`]+)`', value)
        require(bool(references) and all(ref == self.table(name)
            for ref in references for name in [ref.rsplit('.', 1)[-1]]), 'UNSAFE_SQL')
        return value

    def own(self, dataset: bigquery.Dataset) -> None:
        require(dataset.full_dataset_id == f'{PROJECT}:{self.dataset.split(".")[1]}'
            and dataset.location.lower() == LOCATION and dataset.labels == self.labels
            and dataset.default_table_expiration_ms == TTL_MS, 'OWNERSHIP_MISMATCH')


def failure_fixture(workspace_id: str, *, event_suffix: str = '') -> tuple[dict, WorkflowFailurePlan]:
    """Return destination-shaped synthetic rows and a validated failure plan."""
    rows = workflow(workspace_id)
    attempt, invocation = rows['job_attempts'][0], rows['model_invocations'][0]
    receipt = 'fixture-dispatch-receipt'
    attempt.update(revision=2, dispatch_receipt_id=receipt)
    completed = datetime.now(timezone.utc) - timedelta(seconds=1)
    invocation.update(state='FAILED', revision=3, dispatch_receipt_id=receipt,
        worker_id=attempt['worker_id'], safety_outcome='NOT_EVALUATED',
        safe_error_code='RESPONSE_INVALID', completed_at=completed)
    return rows, failure_plan(rows, event_suffix=event_suffix)


def failure_plan(rows: dict, *, event_suffix: str = '') -> WorkflowFailurePlan:
    """Mint a fresh bounded plan after fixture loading has completed."""
    request, job = rows['requests'][0], rows['jobs'][0]
    attempt, invocation = rows['job_attempts'][0], rows['model_invocations'][0]
    started = attempt['started_at']
    finalized = datetime.now(timezone.utc)
    definitions = (
        ('RETRIEVING_EVIDENCE', 'STARTED', started, None),
        ('RETRIEVING_EVIDENCE', 'SUCCEEDED', started + timedelta(seconds=1), 1000),
        ('PLANNING', 'STARTED', started + timedelta(seconds=1), None),
        ('PLANNING', 'FAILED', finalized,
            int((finalized - (started + timedelta(seconds=1))).total_seconds() * 1000)),
    )
    events = []
    for sequence, (stage, outcome, occurred_at, duration_ms) in enumerate(definitions, 1):
        event = {key: attempt[key] for key in ('schema_version', 'workspace_id',
            'conversation_id', 'owner_id', 'job_id', 'request_id', 'attempt_id', 'trace_id')}
        event.update(event_id=f'fixture-failure-event-{sequence}{event_suffix}', sequence=sequence,
            stage=stage, outcome=outcome, duration_ms=duration_ms,
            safe_error_code='RESPONSE_INVALID' if outcome == 'FAILED' else None,
            policy_version=job['policy_version'], occurred_at=occurred_at)
        events.append(event)
    plan = prepare_workflow_failure(workspace=rows['workspaces'][0],
        membership=rows['memberships'][0], conversation=rows['conversations'][0],
        request=request, job=job, attempt=attempt, invocation=invocation,
        stage_events=tuple(events), finalized_at=finalized, settings=NATIVE_WRITE_SETTINGS)
    rows['stage_events'] = [events[0]]
    return plan


def scoped_plan(scope: Scope, plan: WorkflowFailurePlan, *, native_job_id: str | None = None,
                fault: str | None = None) -> WorkflowFailurePlan:
    sql = scope.sql(plan.sql)
    if fault == 'rollback':
        marker = '  COMMIT TRANSACTION;'
        require(sql.count(marker) == 1, 'FAULT_MARKER_MISSING')
        sql = sql.replace(marker, "  ASSERT FALSE AS 'FIXTURE_ROLLBACK';\n" + marker)
    elif fault == 'postcommit':
        marker = 'END;\nSELECT * FROM result;'
        require(sql.count(marker) == 1, 'FAULT_MARKER_MISSING')
        sql = sql.replace(marker, "END;\nASSERT FALSE AS 'FIXTURE_POSTCOMMIT';\nSELECT * FROM result;")
    else:
        require(fault is None, 'INVALID_FAULT')
    scope.sql(sql)
    return replace(plan, sql=sql, native_job_id=native_job_id or plan.native_job_id)


class FixtureExecutor(WorkflowWriterExecutor):
    def __init__(self, probe: 'Probe'):
        super().__init__(probe.client, WorkflowWriterSettings(execution_enabled=True,
            rpc_timeout_seconds=10, result_timeout_seconds=60))
        self.probe = probe

    def _preflight(self) -> None:
        self.probe.preflight()


class Probe:
    def __init__(self, client: bigquery.Client):
        require(client.project == PROJECT and client.location == LOCATION
            and client.default_query_job_config is None, 'INVALID_CLIENT')
        self.client = client
        self.scope = Scope(uuid4().hex)
        self.owned = False
        self.counter = 0
        self.report = {'step': '4.6', 'severity': 'Critical', 'necessity': 'Must have',
            'scenario': 'shared pilot', 'project': PROJECT, 'location': LOCATION,
            'fixture_dataset': self.scope.dataset, 'status': 'RUNNING',
            'cleanup': 'NOT_CREATED', 'serving_row_writes': 0, 'model_calls': 0,
            'gcs_reads': 0, 'scope': 'ISOLATED_NATIVE_WORKFLOW_WRITER_RECOVERY',
            'cases': [], 'jobs': []}

    def job_id(self) -> str:
        self.counter += 1
        return f'workflow-writer-it-{self.scope.run_id}-{self.counter}'

    def own(self) -> None:
        self.scope.own(self.client.get_dataset(self.scope.dataset, retry=None, timeout=10))

    def serving_rows(self) -> dict[str, int]:
        return {name: self.client.get_table(spec.table(PROJECT).reference,
            retry=None, timeout=10).num_rows for name, spec in TABLES.items()}

    def setup(self) -> None:
        dataset = bigquery.Dataset(self.scope.dataset)
        dataset.location, dataset.labels = LOCATION, self.scope.labels
        dataset.default_table_expiration_ms = TTL_MS
        self.client.create_dataset(dataset, retry=None, timeout=10)
        self.owned = True
        self.report['cleanup'] = 'PENDING'
        self.own()
        for name, spec in TABLES.items():
            source = spec.table(PROJECT)
            target = bigquery.Table(self.scope.table(name), schema=source.schema)
            target.clustering_fields = source.clustering_fields
            target.time_partitioning = source.time_partitioning
            target.require_partition_filter = source.require_partition_filter
            self.client.create_table(target, retry=None, timeout=10)
        self.preflight()

    def preflight(self) -> None:
        self.own()
        for name, spec in TABLES.items():
            actual = self.client.get_table(self.scope.table(name), retry=None, timeout=10)
            expected = spec.table(PROJECT)
            require(_schema(actual.schema) == _schema(expected.schema)
                and _partition(actual) == _partition(expected)
                and actual.clustering_fields == expected.clustering_fields
                and actual.require_partition_filter == expected.require_partition_filter
                and actual.expires is not None, 'SCHEMA_MISMATCH')

    def load(self, rows: dict) -> None:
        self.own()
        for name, values in rows.items():
            if not values:
                continue
            target = self.client.get_table(self.scope.table(name), retry=None, timeout=10)
            job = self.client.load_table_from_json(json_rows(values), target,
                job_config=bigquery.LoadJobConfig(schema=target.schema, write_disposition='WRITE_APPEND'),
                job_id=self.job_id(), project=PROJECT, location=LOCATION, timeout=10, num_retries=0)
            job.result(timeout=60, retry=None)
            require(job.error_result is None, 'LOAD_FAILED')

    def read(self, workspace_id: str, name: str) -> list[dict]:
        spec = TABLES[name]
        predicate = 'workspace_id=@workspace_id'
        if spec.partition_field:
            predicate += (f" AND {spec.partition_field} BETWEEN TIMESTAMP('0001-01-01')"
                " AND TIMESTAMP('9999-12-31')")
        sql = self.scope.sql(f'SELECT TO_JSON_STRING(t) row FROM `{DATASET_ID}.{name}` t WHERE {predicate}')
        config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter('workspace_id', 'STRING', workspace_id)],
            use_query_cache=False, maximum_bytes_billed=16 * 1024**2)
        job = self.client.query(sql, job_config=config, job_id=self.job_id(), project=PROJECT,
            location=LOCATION, retry=None, job_retry=None, timeout=10)
        return [decode_row(row['row'], name) for row in job.result(timeout=30, retry=None, job_retry=None)]

    def record_job(self, outcome, plan: WorkflowFailurePlan) -> None:
        job = self.client.get_job(plan.native_job_id, project=PROJECT, location=LOCATION,
            retry=None, timeout=10)
        children = list(self.client.list_jobs(project=PROJECT, parent_job=plan.native_job_id,
            max_results=64, page_size=64, retry=None, timeout=10))
        self.report['jobs'].append({'job_id': plan.native_job_id,
            'native_status': outcome.native_status.value, 'result_state': outcome.result_state,
            'submitted': outcome.submitted, 'error': job.error_result,
            'children': [{'statement_type': child.statement_type, 'error': child.error_result}
                for child in children]})

    def record(self, name: str) -> None:
        self.report['cases'].append({'case': name, 'status': 'PASSED'})
        print(json.dumps({'event': 'workflow_writer_case', 'case': name, 'status': 'PASSED'}), flush=True)

    @staticmethod
    def recover_unknown(executor: FixtureExecutor, plan: WorkflowFailurePlan, outcome):
        return executor.recover(plan) if outcome.native_status is Status.UNKNOWN else outcome

    def run(self) -> None:
        executor = FixtureExecutor(self)

        workspace_id = self.job_id()
        rows, _ = failure_fixture(workspace_id)
        self.load(rows)
        base = failure_plan(rows)
        applied = scoped_plan(self.scope, base)
        first = executor.submit(applied)
        self.record_job(first, applied)
        require((first.native_status, first.result_state, first.submitted)
            == (Status.COMMITTED, 'APPLIED', True), 'ATOMIC_WRITE_FAILED')
        require(self.read(workspace_id, 'jobs')[0]['state'] == 'FAILED'
            and self.read(workspace_id, 'job_attempts')[0]['state'] == 'FAILED'
            and len(self.read(workspace_id, 'stage_events')) == 4
            and len(self.read(workspace_id, 'audit_events')) == 5, 'PARTIAL_ATOMIC_WRITE')
        self.record('atomic_failure_publication')

        duplicate = executor.submit(applied)
        require((duplicate.native_status, duplicate.result_state, duplicate.submitted)
            == (Status.COMMITTED, 'APPLIED', False), 'DUPLICATE_RESUBMITTED')
        self.record('stable_duplicate_job_recovery_without_resubmission')

        conflicting = failure_plan(rows, event_suffix='-conflict')
        conflicting = scoped_plan(self.scope, conflicting, native_job_id=self.job_id())
        denied = self.recover_unknown(executor, conflicting, executor.submit(conflicting))
        self.record_job(denied, conflicting)
        event_count = len(self.read(workspace_id, 'stage_events'))
        audit_count = len(self.read(workspace_id, 'audit_events'))
        self.report['jobs'][-1].update(event_count=event_count, audit_count=audit_count)
        require(denied.native_status is Status.NOT_COMMITTED
            and event_count == 4 and audit_count == 5, 'STALE_CONFLICT_MUTATED')
        self.record('stale_revision_and_content_conflict_rolls_back')

        rollback_workspace = self.job_id()
        rollback_rows, _ = failure_fixture(rollback_workspace)
        self.load(rollback_rows)
        rollback_base = failure_plan(rollback_rows)
        rollback = scoped_plan(self.scope, rollback_base, fault='rollback')
        rolled_back = self.recover_unknown(executor, rollback, executor.submit(rollback))
        require(rolled_back.native_status is Status.NOT_COMMITTED
            and self.read(rollback_workspace, 'jobs')[0]['state'] == 'RUNNING'
            and len(self.read(rollback_workspace, 'stage_events')) == 1
            and not self.read(rollback_workspace, 'audit_events'), 'ROLLBACK_PARTIAL_WRITE')
        self.record_job(rolled_back, rollback)
        self.record('explicit_rollback_classified_not_committed')

        committed_workspace = self.job_id()
        committed_rows, _ = failure_fixture(committed_workspace)
        self.load(committed_rows)
        committed_base = failure_plan(committed_rows)
        postcommit = scoped_plan(self.scope, committed_base, fault='postcommit')
        recovered = self.recover_unknown(executor, postcommit, executor.submit(postcommit))
        require(recovered.native_status is Status.COMMITTED and recovered.result_state is None
            and self.read(committed_workspace, 'jobs')[0]['state'] == 'FAILED'
            and len(self.read(committed_workspace, 'audit_events')) == 5,
            'POSTCOMMIT_NOT_RECOVERED')
        self.record_job(recovered, postcommit)
        self.record('postcommit_failure_classified_committed')

        lost_workspace = self.job_id()
        lost_rows, _ = failure_fixture(lost_workspace)
        self.load(lost_rows)
        lost_base = failure_plan(lost_rows)
        lost = scoped_plan(self.scope, lost_base)
        native = self.client.query(lost.sql, job_config=lost.config(), job_id=lost.native_job_id,
            project=PROJECT, location=LOCATION, retry=None, job_retry=None,
            api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=10)
        native.result(timeout=60, retry=None, job_retry=None)  # Deliberately discard result rows.
        outcome = executor.recover(lost)
        require((outcome.native_status, outcome.result_state, outcome.submitted)
            == (Status.COMMITTED, 'APPLIED', False), 'DISCARDED_RESPONSE_NOT_RECOVERED')
        self.record_job(outcome, lost)
        self.record('discarded_response_recovers_original_job')

    def cleanup(self) -> None:
        if not self.owned:
            return
        self.own()
        self.client.delete_dataset(self.scope.dataset, delete_contents=True, retry=None, timeout=10)
        try:
            self.client.get_dataset(self.scope.dataset, retry=None, timeout=10)
        except NotFound:
            self.owned = False
            self.report['cleanup'] = 'DELETED_AND_ABSENCE_VERIFIED'
            return
        raise RuntimeError('CLEANUP_NOT_VERIFIED')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    with bigquery.Client(project=PROJECT, location=LOCATION) as client:
        probe = Probe(client)
        serving_before = probe.serving_rows()
        try:
            probe.setup()
            probe.run()
            probe.report['status'] = 'PASSED'
        except Exception as error:
            probe.report['status'] = 'FAILED'
            probe.report['error_code'] = getattr(error, 'code', type(error).__name__)
            probe.report['fixture_error'] = str(error)[:1000]
        finally:
            try:
                probe.cleanup()
            except Exception as error:
                probe.report.update(status='FAILED', cleanup='FAILED', cleanup_error=type(error).__name__)
            serving_after = probe.serving_rows()
            if serving_after != serving_before:
                probe.report.update(status='FAILED', serving_row_writes='UNEXPECTED_CHANGE')
            args.output_dir.mkdir(parents=True, exist_ok=True)
            path = args.output_dir / f'{probe.scope.run_id}.json'
            path.write_text(json.dumps(probe.report, indent=2, default=str) + '\n')
            print(json.dumps({'event': 'workflow_writer_complete', 'status': probe.report['status'],
                'cases': len(probe.report['cases']), 'cleanup': probe.report['cleanup'],
                'report': str(path)}), flush=True)
        require(probe.report['status'] == 'PASSED', 'WORKFLOW_WRITER_PROBE_FAILED')


if __name__ == '__main__':
    main()
