"""Opt-in Batch 14 native SQL verification in eight owned expiring tables.

Run with --run --output-dir PATH. Synthetic case observations test persistence,
not RAG quality or business approval. No Vertex, model, GCS or serving writes.
"""
from dataclasses import fields, replace
from datetime import timedelta
import argparse
import json
from pathlib import Path
import re
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.prompt_evaluation_query import PromptEvaluationWrite, wire
from src.application.prompt_evaluation_storage import TABLES as EVALUATION_TABLES
from src.application.prompt_evaluation_store import (
    BigQueryPromptEvaluationStore, EvaluationReadRequest, EvaluationStoreSettings,
    decode_snapshot, read_config, read_sql,
)
from src.application.prompt_registry_codec import encode_registry_state, normalize
from src.application.prompt_registry_schema import TABLES as REGISTRY_TABLES
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES, LOCATION
from src.ingestion.setup_workflow_tables import DATASET_ID, PROJECT, _partition, _schema, validate_dataset
from src.validation.prompt_evaluation_fixtures import UNRELATED, WORKSPACE, another_report, fixtures
from src.validation.signing_read_integration import TTL_MS, require

TABLES = dict(EVALUATION_TABLES) | dict(REGISTRY_TABLES) | {
    name: WORKFLOW_TABLES[name] for name in ('workspaces', 'memberships')}


def validate_native_errors(report):
    """A rollback caused by an unrelated SDK/SQL failure cannot pass a denial case."""
    expected = dict(report_conflict='EVALUATION_REPLAY_CONFLICT', rollback='FIXTURE_ROLLBACK',
        postcommit='FIXTURE_AFTER_COMMIT', expired_report='EVALUATION_EXPIRED',
        expired_identity='ASSERTION_EXPIRED', stale_control='EVALUATION_CONTROL_STALE',
        revoked_report_reuse='MEMBERSHIP_DENIED')
    for case, code in expected.items():
        jobs = [job for job in report['jobs'] if job['job_id'].endswith('_' + case)]
        require(len(jobs) == 1, 'EXPECTED_NATIVE_ERROR_JOB_MISSING')
        job = jobs[0]
        require(job['status'] == ('COMMITTED' if case == 'postcommit' else 'NOT_COMMITTED')
                and job['receipt_state'] is None and type(job['error']) is dict
                and job['error'].get('reason') == 'invalidQuery'
                and code in job['error'].get('message', ''), 'NATIVE_ASSERTION_MISMATCH')
    return expected


class Scope:
    def __init__(self, run_id):
        require(type(run_id) is str and re.fullmatch(r'[0-9a-f]{32}', run_id), 'INVALID_SCOPE')
        self.run_id = run_id
        self.dataset = f'{PROJECT}.ci_prompt_evaluation_it_{run_id}'
        self.labels = {'purpose': 'prompt_evaluation_integration', 'run_id': run_id}

    def table(self, name):
        require(name in TABLES, 'UNRELATED_TABLE')
        return f'{self.dataset}.{name}'

    def assert_sql(self, sql):
        refs = re.findall(r'`([^`]+)`', sql)
        require(bool(refs) and set(refs) <= {self.table(name) for name in TABLES}
                and DATASET_ID not in sql, 'UNSAFE_SQL')

    def relocate(self, sql):
        for name in TABLES:
            sql = sql.replace(f'`{DATASET_ID}.{name}`', f'`{self.table(name)}`')
        self.assert_sql(sql)
        return sql

    def own(self, actual):
        require(actual.full_dataset_id == f'{PROJECT}:{self.dataset.split(".")[1]}'
                and actual.location.lower() == LOCATION and actual.labels == self.labels
                and actual.default_table_expiration_ms == TTL_MS, 'OWNERSHIP_MISMATCH')


class ScopedWrite(PromptEvaluationWrite):
    def __init__(self, base, scope, case, fault=None):
        require(re.fullmatch(r'[a-z0-9_]+', case) is not None, 'INVALID_CASE_SLOT')
        super().__init__(**{f.name: getattr(base, f.name) for f in fields(PromptEvaluationWrite)})
        sql = scope.relocate(base.sql)
        if fault == 'rollback':
            marker = '\nCOMMIT TRANSACTION;'
            require(sql.count(marker) == 1, 'FAULT_MARKER_MISSING')
            sql = sql.replace(marker, "\nASSERT FALSE AS 'FIXTURE_ROLLBACK';" + marker)
        elif fault == 'postcommit':
            marker = '\nSELECT receipt_state AS state,'
            require(sql.count(marker) == 1, 'FAULT_MARKER_MISSING')
            sql = sql.replace(marker, "\nASSERT FALSE AS 'FIXTURE_AFTER_COMMIT';" + marker)
        else:
            require(fault is None, 'INVALID_FAULT')
        scope.assert_sql(sql)
        object.__setattr__(self, '_sql', sql)
        object.__setattr__(self, '_job_id', f'prompt_evaluation_it_{scope.run_id}_{case}')

    @property
    def sql(self):
        return self._sql

    @property
    def job_id(self):
        return self._job_id


class FixtureStore(BigQueryPromptEvaluationStore):
    def __init__(self, probe):
        super().__init__(probe.client, EvaluationStoreSettings(execution_enabled=True,
            result_timeout_seconds=60))
        self.probe = probe

    def _preflight(self):
        self.probe.preflight()


class Probe:
    def __init__(self, client):
        require(client.project == PROJECT and client.location == LOCATION
                and client.default_query_job_config is None, 'INVALID_CLIENT')
        self.client, self.scope = client, Scope(uuid4().hex)
        self.owned, self.counter = False, 0
        self.report = dict(priority='5', batch=14, severity='Critical', necessity='Must have',
            scenario='shared pilot', scope='ISOLATED_NATIVE_EVALUATION_STORAGE',
            fixture_dataset=self.scope.dataset, project=PROJECT, location=LOCATION,
            status='RUNNING', cleanup='NOT_CREATED', serving_row_writes=0,
            vertex_calls=0, model_calls=0, gcs_reads=0, measured_quality=False,
            cases=[], jobs=[])

    def own(self):
        self.scope.own(self.client.get_dataset(self.scope.dataset, retry=None, timeout=10))

    def serving_rows(self):
        return {name: self.client.get_table(spec.table(PROJECT).reference,
            retry=None, timeout=10).num_rows for name, spec in TABLES.items()}

    def setup(self):
        validate_dataset(self.client.get_dataset(DATASET_ID, retry=None, timeout=10))
        target = bigquery.Dataset(self.scope.dataset)
        target.location, target.labels, target.default_table_expiration_ms = LOCATION, self.scope.labels, TTL_MS
        self.client.create_dataset(target, exists_ok=False, retry=None, timeout=10)
        self.owned = True
        self.report['cleanup'] = 'PENDING'
        self.own()
        for name, spec in TABLES.items():
            expected = spec.table(PROJECT)
            table = bigquery.Table(self.scope.table(name), schema=expected.schema)
            table.clustering_fields, table.time_partitioning = expected.clustering_fields, expected.time_partitioning
            table.require_partition_filter = expected.require_partition_filter
            self.client.create_table(table, exists_ok=False, retry=None, timeout=10)
        self.preflight()

    def preflight(self):
        self.own()
        for name, spec in TABLES.items():
            actual = self.client.get_table(self.scope.table(name), retry=None, timeout=10)
            expected = spec.table(PROJECT)
            require(actual.project == PROJECT and actual.dataset_id == self.scope.dataset.split('.')[1]
                    and actual.table_id == name and actual.table_type == 'TABLE'
                    and actual.location.lower() == LOCATION
                    and _schema(actual.schema) == _schema(expected.schema)
                    and _partition(actual) == _partition(expected)
                    and actual.clustering_fields == expected.clustering_fields
                    and actual.require_partition_filter == expected.require_partition_filter
                    and actual.expires is not None, 'SCHEMA_MISMATCH')

    def query(self, sql, config=None, job_id=None):
        self.own()
        self.scope.assert_sql(sql)
        self.counter += 1
        job_id = job_id or f'prompt_evaluation_it_{self.scope.run_id}_read_{self.counter}'
        config = config or bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False,
            create_session=False, query_parameters=[], maximum_bytes_billed=512 * 1024**2, job_timeout_ms=60000,
            script_options=bigquery.ScriptOptions(statement_timeout_ms=20000))
        expected = bigquery.QueryJobConfig.from_api_repr(config.to_api_repr())
        job = self.client.query(sql, job_config=config, job_id=job_id, project=PROJECT,
            location=LOCATION, retry=None, job_retry=None,
            api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=10)
        require(matches_sharing_job(job, job_id=job_id, sql=sql, config=expected), 'QUERY_MISMATCH')
        rows = [normalize(row) for row in job.result(max_results=2, page_size=2,
            retry=None, job_retry=None, timeout=60)]
        require(len(rows) <= 2 and job.state == 'DONE' and job.error_result is None
                and matches_sharing_job(job, job_id=job_id, sql=sql, config=expected), 'QUERY_MISMATCH')
        return rows

    def load(self, name, rows):
        self.preflight()
        self.counter += 1
        config = bigquery.LoadJobConfig(schema=TABLES[name].schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = self.client.load_table_from_json(json.loads(wire(rows)), self.scope.table(name),
            job_config=config, job_id=f'prompt_evaluation_it_{self.scope.run_id}_load_{self.counter}',
            project=PROJECT, location=LOCATION, num_retries=0, timeout=10)
        job.result(timeout=60, retry=None)
        require(job.error_result is None, 'FIXTURE_LOAD_FAILED')

    def counts(self):
        names = tuple(EVALUATION_TABLES)
        sql = 'SELECT ' + ', '.join(f'(SELECT COUNT(*) FROM `{self.scope.table(name)}` '
            f"WHERE workspace_id='{WORKSPACE}') AS n{i}" for i, name in enumerate(names))
        row = self.query(sql)[0]
        return tuple(row[f'n{i}'] for i in range(len(names)))

    def read(self, plan, evaluation_id=None):
        request = EvaluationReadRequest(WORKSPACE, plan.control.prompt_key,
            plan.identity, plan.membership_revision, evaluation_id)
        return decode_snapshot(request, self.query(self.scope.relocate(read_sql()), read_config(request)))

    def unrelated(self):
        return self.query('SELECT ' + ', '.join(
            f"(SELECT TO_JSON_STRING(t) FROM `{self.scope.table(name)}` t WHERE workspace_id='{UNRELATED}') AS {name}"
            for name in ('workspaces', 'memberships')))

    def record(self, case):
        self.report['cases'].append(dict(case=case, status='PASSED'))
        print(json.dumps(dict(event='prompt_evaluation_case', case=case, status='PASSED')), flush=True)

    def write(self, store, base, case, *, state='APPLIED', fault=None, status=Status.COMMITTED):
        plan = ScopedWrite(base, self.scope, case, fault)
        outcome = store.submit(plan)
        if outcome.native_status is Status.UNKNOWN:
            outcome = store.recover(plan)
        job = self.client.get_job(plan.job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        self.report['jobs'].append(dict(job_id=plan.job_id, status=outcome.native_status.value,
            receipt_state=None if outcome.receipt is None else outcome.receipt['state'],
            error=job.error_result))
        require(outcome.native_status is status and
            (outcome.receipt is None if state is None else outcome.receipt is not None
             and outcome.receipt['state'] == state), 'UNEXPECTED_WRITE_OUTCOME')
        return plan, outcome

    def run(self):
        seed, registry, control, report = fixtures()
        for name, rows in seed.items():
            self.load(name, rows)
        current, history = encode_registry_state(registry)
        self.load('prompt_registry', [current])
        self.load('prompt_registry_history', history)
        sentinel = self.unrelated()
        store = FixtureStore(self)
        plan, first = self.write(store, control, 'control_apply')
        require(first.submitted and self.counts() == (1, 1, 0, 0)
                and self.read(control).control == control.control, 'CONTROL_PARTIAL')
        self.record('atomic_control_and_authoritative_read')
        duplicate = store.submit(plan)
        require(not duplicate.submitted and duplicate.receipt == first.receipt
                and self.counts() == (1, 1, 0, 0), 'DUPLICATE_RESUBMITTED')
        self.record('same_control_job_recovers_without_submission')
        self.write(store, control, 'control_reuse', state='REUSED')
        require(self.counts() == (1, 1, 0, 0), 'REUSE_ADDED_ROWS')
        self.record('control_new_job_exact_reuse')
        plan, first = self.write(store, report, 'report_apply')
        require(self.counts() == (1, 1, 1, len(report.record.results))
                and self.read(report, report.record.evaluation_id).record == report.record, 'REPORT_PARTIAL')
        self.record('atomic_report_cases_and_full_codec_read')
        duplicate = store.submit(plan)
        require(not duplicate.submitted and duplicate.receipt == first.receipt, 'REPORT_RESUBMITTED')
        self.write(store, report, 'report_reuse', state='REUSED')
        require(self.counts() == (1, 1, 1, len(report.record.results)), 'REPORT_REUSE_ADDED_ROWS')
        self.record('report_same_job_recovery_and_new_job_reuse')
        conflict = replace(report, record=replace(report.record, results=(
            replace(report.record.results[0], response_sha256='e' * 64),) + report.record.results[1:]))
        before = self.counts()
        self.write(store, conflict, 'report_conflict', state=None, status=Status.NOT_COMMITTED)
        require(self.counts() == before, 'CONFLICT_CHANGED_ROWS')
        self.record('conflicting_report_rolls_back')
        self.write(store, another_report(report, 'fixture-rollback'), 'rollback', fault='rollback',
            state=None, status=Status.NOT_COMMITTED)
        require(self.counts() == before, 'ROLLBACK_LEFT_PARTIAL_ROWS')
        self.record('rollback_leaves_no_report_or_cases')
        lost = ScopedWrite(another_report(report, 'fixture-discarded'), self.scope, 'discarded')
        self.query(lost.sql, lost.job_config(), lost.job_id)  # Discard the business receipt.
        recovered = store.recover(lost)
        require(not recovered.submitted and recovered.native_status is Status.COMMITTED
                and recovered.receipt['state'] == 'APPLIED'
                and self.read(report, lost.record.evaluation_id).record == lost.record, 'RECOVERY_FAILED')
        self.record('discarded_response_recovers_original_job')
        post = another_report(report, 'fixture-postcommit')
        plan, outcome = self.write(store, post, 'postcommit', fault='postcommit', state=None)
        recovered = store.recover(plan)
        require(recovered.native_status is Status.COMMITTED and recovered.receipt is None
                and not recovered.submitted and self.read(report, post.record.evaluation_id).record == post.record,
                'POSTCOMMIT_INFERENCE_FAILED')
        self.record('postcommit_failure_preserves_commit_without_inventing_receipt')
        before = self.counts()
        expired = another_report(report, 'fixture-expired')
        expired = replace(expired, record=replace(expired.record,
            valid_until=expired.record.completed_at + timedelta(seconds=1)))
        self.write(store, expired, 'expired_report', state=None, status=Status.NOT_COMMITTED)
        expired_identity = replace(control.identity, issued_at=control.identity.issued_at - 660,
            expires_at=control.identity.expires_at - 660)
        self.write(store, replace(control, identity=expired_identity), 'expired_identity',
            state=None, status=Status.NOT_COMMITTED)
        require(self.counts() == before, 'EXPIRY_CHANGED_ROWS')
        self.record('expired_report_and_assertion_deny')
        second = replace(control.control, revision=2, parent_digest=control.control.digest,
            policy=replace(control.control.policy, version='fixture-v2'),
            updated_at=control.control.updated_at + timedelta(seconds=1))
        replacement = replace(control, control=second, previous=control.control,
            control_history=(control.control,))
        self.write(store, replacement, 'control_replace')
        historical = self.read(report, report.record.evaluation_id)
        require(historical.control == second and historical.record == report.record, 'HISTORY_REBOUND')
        self.write(store, another_report(report, 'fixture-stale'), 'stale_control',
            state=None, status=Status.NOT_COMMITTED)
        self.write(store, control, 'historical_control_reuse', state='REUSED')
        require(self.read(control).control == second, 'REPLAY_REGRESSED_HEAD')
        self.record('control_replacement_stale_denial_and_historical_reuse')
        before = self.counts()
        self.query(f"UPDATE `{self.scope.table('memberships')}` SET status='DISABLED', revision=revision+1 "
                   f"WHERE workspace_id='{WORKSPACE}'; SELECT 1 AS updated")
        self.write(store, report, 'revoked_report_reuse', state=None, status=Status.NOT_COMMITTED)
        require(self.counts() == before and self.unrelated() == sentinel, 'REVOCATION_OR_SCOPE_FAILURE')
        self.record('revocation_blocks_replay_and_unrelated_rows_preserved')

    def cleanup(self):
        if not self.owned:
            return
        self.own()
        self.client.delete_dataset(self.scope.dataset, delete_contents=True,
            not_found_ok=False, retry=None, timeout=10)
        try:
            self.client.get_dataset(self.scope.dataset, retry=None, timeout=10)
        except NotFound:
            self.owned = False
            self.report['cleanup'] = 'DELETED_AND_ABSENCE_VERIFIED'
            return
        raise RuntimeError('CLEANUP_NOT_VERIFIED')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', required=True)
    parser.add_argument('--remaining-only', action='store_true',
        help='Run only the planned Batch 14 overlap/partial-state gates')
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    with bigquery.Client(project=PROJECT, location=LOCATION) as client:
        if args.remaining_only:
            from src.validation.prompt_evaluation_remaining import RemainingProbe
            probe = RemainingProbe(client)
        else:
            probe = Probe(client)
        before = probe.serving_rows()
        try:
            probe.setup()
            probe.run()
            if not args.remaining_only:
                probe.report['verified_native_error_codes'] = validate_native_errors(probe.report)
            probe.report['status'] = 'PASSED'
        except Exception as error:
            probe.report.update(status='FAILED', error_code=getattr(error, 'code', type(error).__name__),
                fixture_error=str(error)[:1000])
        finally:
            try:
                probe.cleanup()
            except Exception as error:
                probe.report.update(status='FAILED', cleanup='FAILED', cleanup_error=type(error).__name__)
            try:
                after = probe.serving_rows()
                require(after == before, 'SERVING_METADATA_CHANGED')
                probe.report.update(serving_rows_before=before, serving_rows_after=after,
                    serving_row_metadata_unchanged=True)
            except Exception as error:
                probe.report.update(status='FAILED', preservation_error=str(error)[:200])
            args.output_dir.mkdir(parents=True, exist_ok=True)
            path = args.output_dir / f'{probe.scope.run_id}.json'
            path.write_text(json.dumps(probe.report, indent=2, default=str) + '\n', encoding='utf-8')
            print(json.dumps(dict(event='prompt_evaluation_complete', status=probe.report['status'],
                cases=len(probe.report['cases']), cleanup=probe.report['cleanup'], report=str(path))), flush=True)
        require(probe.report['status'] == 'PASSED', 'PROMPT_EVALUATION_PROBE_FAILED')


if __name__ == '__main__':
    main()
