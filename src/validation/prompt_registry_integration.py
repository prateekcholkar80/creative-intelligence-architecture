"""Priority 5 isolated native prompt-registration verification.

Opt in with ``--run --output-dir PATH``. The probe uses ADC, creates two exact
tables in one expiring owned dataset, relocates the production registration SQL,
exercises the default-disabled executor through an explicitly enabled fixture
subclass, and deletes only that owned dataset. It never reads or writes serving
prompt rows and never accesses Vertex AI.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.prompt_contract import (
    PromptTask,
    VertexPromptSnapshot,
    initial_prompt_record,
)
from src.application.prompt_registry_codec import initial_registration_state
from src.application.prompt_registry_executor import (
    PromptRegistryExecutor,
    PromptRegistryExecutorSettings,
)
from src.application.prompt_registry_query import PromptRegistrationQuery
from src.application.prompt_registry_schema import TABLES
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_contract import canonical_json_bytes
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_workflow_tables import (
    DATASET_ID,
    PROJECT,
    _partition,
    _schema,
    validate_dataset,
)
from src.validation.signing_read_integration import TTL_MS, require


class Scope:
    def __init__(self, run_id: str):
        require(type(run_id) is str
                and re.fullmatch(r'[0-9a-f]{32}', run_id) is not None,
                'INVALID_SCOPE')
        self.run_id = run_id
        self.dataset = f'{PROJECT}.ci_prompt_registry_it_{run_id}'
        self.labels = {
            'purpose': 'prompt_registry_integration',
            'run_id': run_id,
        }

    def table(self, name: str) -> str:
        require(name in TABLES, 'UNRELATED_TABLE')
        return f'{self.dataset}.{name}'

    def relocate(self, sql: str) -> str:
        for name in TABLES:
            source = f'`{DATASET_ID}.{name}`'
            require(source in sql, 'INVALID_REGISTRATION_TEMPLATE')
            sql = sql.replace(source, f'`{self.table(name)}`')
        self.assert_sql(sql)
        return sql

    def assert_sql(self, sql: str) -> None:
        references = re.findall(r'`([^`]+)`', sql)
        require(bool(references)
                and set(references).issubset(
                    {self.table(name) for name in TABLES}),
                'UNSAFE_SQL')

    def own(self, dataset: bigquery.Dataset) -> None:
        require(dataset.full_dataset_id
                == f'{PROJECT}:{self.dataset.split(".")[1]}'
                and dataset.location.lower() == LOCATION
                and dataset.labels == self.labels
                and dataset.default_table_expiration_ms == TTL_MS,
                'OWNERSHIP_MISMATCH')


class ScopedPromptRegistrationQuery(PromptRegistrationQuery):
    """Probe-only typed plan with an allowlisted relocated SQL/job slot."""

    def __init__(self, base: PromptRegistrationQuery, *, sql: str,
                 job_id: str):
        super().__init__(base.state, base.settings)
        require(type(sql) is str and bool(sql)
                and re.fullmatch(r'prompt_registry_it_[0-9a-f]{32}_[a-z0-9_]+',
                                 job_id) is not None,
                'INVALID_SCOPED_PLAN')
        object.__setattr__(self, '_scoped_sql', sql)
        object.__setattr__(self, '_scoped_job_id', job_id)

    @property
    def sql(self) -> str:
        return self._scoped_sql

    @property
    def job_id(self) -> str:
        return self._scoped_job_id


def _canonical(value) -> str:
    return canonical_json_bytes(value).decode('utf-8')


def fixture_query(*, version: str = '41', content_suffix: str = '',
                  imported_at: datetime | None = None) -> PromptRegistrationQuery:
    """Build a contract-valid synthetic Vertex snapshot without calling Vertex."""
    snapshot = VertexPromptSnapshot(
        prompt_key='fixture-campaign-planner',
        task=PromptTask.PLANNER,
        vertex_project=PROJECT,
        vertex_location=LOCATION,
        vertex_prompt_id='901234567890',
        vertex_version_id=version,
        system_instruction=(
            'Use only approved evidence for {campaign_request}.'
            + content_suffix),
        prompt_template=(
            'Evidence:\n{evidence_bundle_json}\n'
            'Options:\n{creative_options_json}'),
        required_variables=(
            'campaign_request', 'creative_options_json',
            'evidence_bundle_json'),
        model_id='gemini-3.5-flash-001',
        generation_config_json=_canonical({
            'automatic_function_calling_disabled': True,
            'max_output_tokens': 4096,
            'response_mime_type': 'application/json',
            'seed': 42,
            'temperature': None,
            'thinking_level': 'LOW',
        }),
        safety_settings_json=_canonical([
            {'category': 'HARM_CATEGORY_DANGEROUS_CONTENT',
             'threshold': 'BLOCK_MEDIUM_AND_ABOVE'},
            {'category': 'HARM_CATEGORY_HARASSMENT',
             'threshold': 'BLOCK_MEDIUM_AND_ABOVE'},
        ]),
        output_schema_json=_canonical({
            'properties': {'concept': {'type': 'STRING'}},
            'required': ['concept'],
            'type': 'OBJECT',
        }),
        output_schema_version='campaign-draft-v1',
        safety_policy_version='poc-safety-v1')
    at = (datetime.now(timezone.utc) - timedelta(seconds=2)
          if imported_at is None else imported_at)
    record = initial_prompt_record(
        snapshot, imported_at=at,
        imported_by='iap-v1:' + 'a' * 64)
    return PromptRegistrationQuery(initial_registration_state(record))


def scoped_plan(scope: Scope, base: PromptRegistrationQuery, *, case: str,
                fault: str | None = None) -> ScopedPromptRegistrationQuery:
    sql = scope.relocate(base.sql)
    if fault == 'rollback':
        marker = '  COMMIT TRANSACTION;'
        require(sql.count(marker) == 1, 'FAULT_MARKER_MISSING')
        sql = sql.replace(
            marker, "  ASSERT FALSE AS 'FIXTURE_ROLLBACK';\n" + marker)
    else:
        require(fault is None, 'INVALID_FAULT')
    scope.assert_sql(sql)
    return ScopedPromptRegistrationQuery(
        base, sql=sql,
        job_id=f'prompt_registry_it_{scope.run_id}_{case}')


class FixtureExecutor(PromptRegistryExecutor):
    def __init__(self, probe: 'Probe'):
        super().__init__(
            probe.client,
            PromptRegistryExecutorSettings(
                execution_enabled=True, rpc_timeout_seconds=10,
                result_timeout_seconds=60))
        self.probe = probe

    def _preflight(self) -> None:
        self.probe.preflight()


class Probe:
    def __init__(self, client: bigquery.Client):
        require(client.project == PROJECT and client.location == LOCATION
                and client.default_query_job_config is None,
                'INVALID_CLIENT')
        self.client = client
        self.scope = Scope(uuid4().hex)
        self.owned = False
        self.counter = 0
        self.report = {
            'priority': '5',
            'severity': 'Critical',
            'necessity': 'Must have',
            'scenario': 'shared pilot',
            'project': PROJECT,
            'location': LOCATION,
            'fixture_dataset': self.scope.dataset,
            'status': 'RUNNING',
            'cleanup': 'NOT_CREATED',
            'serving_row_writes': 0,
            'vertex_calls': 0,
            'scope': 'ISOLATED_NATIVE_PROMPT_REGISTRATION',
            'cases': [],
            'jobs': [],
        }

    def job_id(self, prefix: str = 'read') -> str:
        self.counter += 1
        return f'prompt_registry_it_{self.scope.run_id}_{prefix}_{self.counter}'

    def own(self) -> None:
        self.scope.own(self.client.get_dataset(
            self.scope.dataset, retry=None, timeout=10))

    def serving_rows(self) -> dict[str, int]:
        return {
            name: self.client.get_table(
                spec.table(PROJECT).reference, retry=None, timeout=10).num_rows
            for name, spec in TABLES.items()
        }

    def setup(self) -> None:
        validate_dataset(self.client.get_dataset(
            DATASET_ID, retry=None, timeout=10))
        dataset = bigquery.Dataset(self.scope.dataset)
        dataset.location = LOCATION
        dataset.labels = self.scope.labels
        dataset.default_table_expiration_ms = TTL_MS
        self.client.create_dataset(
            dataset, exists_ok=False, retry=None, timeout=10)
        self.owned = True
        self.report['cleanup'] = 'PENDING'
        self.own()
        for name, spec in TABLES.items():
            expected = spec.table(PROJECT)
            target = bigquery.Table(
                self.scope.table(name), schema=expected.schema)
            target.clustering_fields = expected.clustering_fields
            target.time_partitioning = expected.time_partitioning
            target.require_partition_filter = expected.require_partition_filter
            self.client.create_table(
                target, exists_ok=False, retry=None, timeout=10)
        self.preflight()

    def preflight(self) -> None:
        self.own()
        for name, spec in TABLES.items():
            actual = self.client.get_table(
                self.scope.table(name), retry=None, timeout=10)
            expected = spec.table(PROJECT)
            require(actual.project == PROJECT
                    and actual.dataset_id == self.scope.dataset.split('.')[1]
                    and actual.table_id == name
                    and actual.table_type == 'TABLE'
                    and actual.location.lower() == LOCATION
                    and _schema(actual.schema) == _schema(expected.schema)
                    and _partition(actual) == _partition(expected)
                    and actual.clustering_fields
                    == expected.clustering_fields
                    and actual.require_partition_filter
                    == expected.require_partition_filter
                    and actual.expires is not None,
                    'SCHEMA_MISMATCH')

    def counts(self) -> tuple[int, int]:
        self.own()
        sql = self.scope.relocate(
            f"SELECT (SELECT COUNT(*) FROM `{DATASET_ID}.prompt_registry`) AS current_count, "
            f"(SELECT COUNT(*) FROM `{DATASET_ID}.prompt_registry_history` "
            "WHERE changed_at BETWEEN TIMESTAMP('0001-01-01') "
            "AND TIMESTAMP('9999-12-31')) AS history_count")
        config = bigquery.QueryJobConfig(
            use_legacy_sql=False, use_query_cache=False,
            maximum_bytes_billed=32 * 1024**2)
        job = self.client.query(
            sql, job_config=config, job_id=self.job_id(), project=PROJECT,
            location=LOCATION, retry=None, job_retry=None,
            api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=10)
        rows = list(job.result(
            max_results=2, page_size=2, timeout=30, retry=None,
            job_retry=None))
        require(len(rows) == 1, 'INVALID_COUNT_RESULT')
        return int(rows[0]['current_count']), int(rows[0]['history_count'])

    def record_job(self, outcome, plan: PromptRegistrationQuery) -> None:
        job = self.client.get_job(
            plan.job_id, project=PROJECT, location=LOCATION,
            retry=None, timeout=10)
        children = list(self.client.list_jobs(
            project=PROJECT, parent_job=plan.job_id, all_users=False,
            max_results=64, page_size=64, retry=None, timeout=10))
        self.report['jobs'].append({
            'job_id': plan.job_id,
            'native_status': outcome.native_status.value,
            'result_state': (None if outcome.result is None
                             else outcome.result.state.value),
            'submitted': outcome.submitted,
            'error': job.error_result,
            'children': [
                {'statement_type': child.statement_type,
                 'error': child.error_result}
                for child in children
            ],
        })

    def record(self, case: str) -> None:
        self.report['cases'].append({'case': case, 'status': 'PASSED'})
        print(json.dumps({
            'event': 'prompt_registry_case',
            'case': case,
            'status': 'PASSED',
        }), flush=True)

    @staticmethod
    def recover_unknown(executor: FixtureExecutor,
                        plan: PromptRegistrationQuery, outcome):
        return (executor.recover(plan)
                if outcome.native_status is Status.UNKNOWN else outcome)

    def run(self) -> None:
        executor = FixtureExecutor(self)

        base = fixture_query(version='41')
        applied_plan = scoped_plan(self.scope, base, case='apply')
        applied = executor.submit(applied_plan)
        require((applied.native_status,
                 None if applied.result is None else applied.result.state.value,
                 applied.submitted)
                == (Status.COMMITTED, 'APPLIED', True),
                'ATOMIC_REGISTRATION_FAILED')
        require(self.counts() == (1, 1), 'PARTIAL_REGISTRATION')
        self.record_job(applied, applied_plan)
        self.record('atomic_apply')

        duplicate = executor.submit(applied_plan)
        require((duplicate.native_status,
                 None if duplicate.result is None
                 else duplicate.result.state.value,
                 duplicate.submitted)
                == (Status.COMMITTED, 'APPLIED', False)
                and self.counts() == (1, 1),
                'DUPLICATE_JOB_RESUBMITTED')
        self.record('stable_duplicate_job_recovery')

        reuse_plan = scoped_plan(self.scope, base, case='reuse')
        reused = executor.submit(reuse_plan)
        require((reused.native_status,
                 None if reused.result is None else reused.result.state.value,
                 reused.submitted)
                == (Status.COMMITTED, 'REUSED', True)
                and self.counts() == (1, 1),
                'REGISTRATION_REUSE_FAILED')
        self.record_job(reused, reuse_plan)
        self.record('new_job_exact_state_reused_without_rows')

        conflict = fixture_query(
            version='41', content_suffix=' Conflicting fixture content.',
            imported_at=base.state.head.changed_at)
        conflict_plan = scoped_plan(
            self.scope, conflict, case='conflict')
        denied = self.recover_unknown(
            executor, conflict_plan, executor.submit(conflict_plan))
        require(denied.native_status is Status.NOT_COMMITTED
                and denied.result is None and self.counts() == (1, 1),
                'CONFLICT_MUTATED_REGISTRY')
        self.record_job(denied, conflict_plan)
        self.record('changed_content_same_vertex_version_rolls_back')

        rollback_plan = scoped_plan(
            self.scope, fixture_query(version='42'), case='rollback',
            fault='rollback')
        rolled_back = self.recover_unknown(
            executor, rollback_plan, executor.submit(rollback_plan))
        require(rolled_back.native_status is Status.NOT_COMMITTED
                and rolled_back.result is None and self.counts() == (1, 1),
                'EXPLICIT_ROLLBACK_LEFT_ROWS')
        self.record_job(rolled_back, rollback_plan)
        self.record('explicit_rollback_has_no_partial_rows')

        lost_plan = scoped_plan(
            self.scope, fixture_query(version='43'), case='discarded')
        native = self.client.query(
            lost_plan.sql, job_config=lost_plan.job_config(),
            job_id=lost_plan.job_id, project=PROJECT, location=LOCATION,
            retry=None, job_retry=None,
            api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=10)
        native.result(timeout=60, retry=None, job_retry=None)
        recovered = executor.recover(lost_plan)
        require((recovered.native_status,
                 None if recovered.result is None
                 else recovered.result.state.value,
                 recovered.submitted)
                == (Status.COMMITTED, 'APPLIED', False)
                and self.counts() == (2, 2),
                'DISCARDED_RESPONSE_NOT_RECOVERED')
        self.record_job(recovered, lost_plan)
        self.record('discarded_response_recovers_original_job')

    def cleanup(self) -> None:
        if not self.owned:
            return
        self.own()
        self.client.delete_dataset(
            self.scope.dataset, delete_contents=True, not_found_ok=False,
            retry=None, timeout=10)
        try:
            self.client.get_dataset(
                self.scope.dataset, retry=None, timeout=10)
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
            probe.report['error_code'] = getattr(
                error, 'code', type(error).__name__)
            probe.report['fixture_error'] = str(error)[:1000]
        finally:
            try:
                probe.cleanup()
            except Exception as error:
                probe.report.update(
                    status='FAILED', cleanup='FAILED',
                    cleanup_error=type(error).__name__)
            serving_after = probe.serving_rows()
            if serving_after != serving_before:
                probe.report.update(
                    status='FAILED',
                    serving_row_writes='UNEXPECTED_CHANGE')
            args.output_dir.mkdir(parents=True, exist_ok=True)
            path = args.output_dir / f'{probe.scope.run_id}.json'
            path.write_text(
                json.dumps(probe.report, indent=2, default=str) + '\n',
                encoding='utf-8')
            print(json.dumps({
                'event': 'prompt_registry_complete',
                'status': probe.report['status'],
                'cases': len(probe.report['cases']),
                'cleanup': probe.report['cleanup'],
                'report': str(path),
            }), flush=True)
        require(probe.report['status'] == 'PASSED',
                'PROMPT_REGISTRY_PROBE_FAILED')


if __name__ == '__main__':
    main()
