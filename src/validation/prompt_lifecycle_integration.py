"""Opt-in Batch 15 owned native verification; no serving or provider writes.

The CLI temporarily relocates only the store's SQL builders inside this probe
process. Native identity/configuration/recovery checks run unchanged against the
actual relocated SQL. Importing this module never patches or creates a client.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import argparse
import json
from pathlib import Path
import re
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from src.application import prompt_lifecycle_store as storage
from src.application.prompt_contract import PromptLifecycle as Lifecycle
from src.application.prompt_evaluation_query import PromptEvaluationWrite, EvaluationWriteKind, wire
from src.application.prompt_evaluation_storage import TABLES as EVALUATION_TABLES, encode_evaluation
from src.application.prompt_evaluation_store import EvaluationSnapshot, EvaluationReadRequest, EvaluationStoreSettings
from src.application.prompt_lifecycle_contract import TABLES as LIFECYCLE_TABLES, LifecycleReadRequest, LifecycleSnapshot, LifecycleWrite
from src.application.prompt_lifecycle_query import sql as production_sql, config
from src.application.prompt_registry_schema import TABLES as REGISTRY_TABLES
from src.application.prompt_registry_codec import normalize
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES, LOCATION
from src.ingestion.setup_workflow_tables import DATASET_ID, PROJECT, _partition, _schema, validate_dataset
from src.validation.prompt_evaluation_integration import Probe, Scope, Status
from src.validation.prompt_evaluation_remaining import transaction_interval, validate_race
from src.validation.prompt_lifecycle_fixtures import lifecycle_fixtures
from src.validation.signing_read_integration import TTL_MS, require

FIXTURE_RPC_TIMEOUT_SECONDS = 30

TABLES = dict(EVALUATION_TABLES) | dict(REGISTRY_TABLES) | dict(LIFECYCLE_TABLES) | {
    name: WORKFLOW_TABLES[name] for name in ('workspaces', 'memberships')}

EXPECTED_CASES = frozenset({
    'registered_and_evaluated_authoritative_chains', 'initial_activation',
    'atomic_active_replacement', 'rollback_to_evaluated_retired_version',
    'retirement_clears_active_pin', 'reactivation', 'blocking_clears_active_pin',
    'same_job_recovery_and_new_job_exact_replay', 'discarded_response_original_job_recovery',
    'historical_replay_preserves_later_control_head',
    'positive_rollback_only_retry_preserves_pins_and_recovers',
    'three_native_slots_exhausted_without_partial_rows',
    'postcommit_error_positive_commit_without_invented_receipt_or_retry',
    'absent_original_is_unknown_and_cannot_advance',
    'evaluation_expiry_at_native_dispatch_denies', 'assertion_expiry_denies',
    'changed_authoritative_policy_denies_stale_promotion', 'revocation_denies_historical_replay',
    'overlapping_cross_workspace_promotions_one_complete_winner',
    'competing_workspaces_share_one_native_slot_unknown_cannot_retry',
    'unrelated_fixture_rows_preserved',
})
REMAINING_CASES = frozenset({
    'absent_original_is_unknown_and_cannot_advance', 'evaluation_expiry_at_native_dispatch_denies',
    'assertion_expiry_denies', 'changed_authoritative_policy_denies_stale_promotion',
    'revocation_denies_historical_replay', 'overlapping_cross_workspace_promotions_one_complete_winner',
    'competing_workspaces_share_one_native_slot_unknown_cannot_retry', 'unrelated_fixture_rows_preserved',
})
CASE_GROUPS = {'ALL': EXPECTED_CASES, 'CORE': EXPECTED_CASES - REMAINING_CASES,
               'REMAINING': REMAINING_CASES}


def validate_native_report(report):
    """Completeness and correlated denial evidence are required for a passing run."""
    group = report.get('case_group', 'ALL')
    require(group in CASE_GROUPS, 'INVALID_LIFECYCLE_CASE_GROUP')
    expected = CASE_GROUPS[group]
    cases = report['cases']
    require(len(cases) == len(expected)
            and {case['case'] for case in cases} == expected
            and all(case['status'] == 'PASSED' for case in cases), 'INCOMPLETE_LIFECYCLE_CASES')
    errors = report['verified_native_error_codes']
    core_codes = {'FIXTURE_ROLLBACK', 'FIXTURE_AFTER_COMMIT'}
    remaining_codes = {'LIFECYCLE_EVALUATION_EXPIRED_OR_FAILED', 'ASSERTION_EXPIRED',
                       'LIFECYCLE_EVALUATION_CONTROL_STALE', 'MEMBERSHIP_DENIED'}
    require(len(errors) == {'ALL': 9, 'CORE': 5, 'REMAINING': 4}[group]
            and set(errors.values()) == (core_codes if group == 'CORE' else remaining_codes
                if group == 'REMAINING' else core_codes | remaining_codes),
        'INCOMPLETE_LIFECYCLE_DENIALS')
    for job_id, code in errors.items():
        jobs = [job for job in report['jobs'] if job['job_id'] == job_id]
        require(len(jobs) == 1, 'EXPECTED_NATIVE_ERROR_JOB_MISSING')
        job = jobs[0]
        require(job['status'] == ('COMMITTED' if code == 'FIXTURE_AFTER_COMMIT' else 'NOT_COMMITTED')
                and job['receipt_state'] is None and type(job['error']) is dict
                and job['error'].get('reason') == 'invalidQuery'
                and code in job['error'].get('message', ''), 'NATIVE_ASSERTION_MISMATCH')
    races = report['races']
    if group == 'CORE':
        require(not races, 'UNEXPECTED_LIFECYCLE_RACES')
        return
    require(len(races) == 1 and races[0]['case'] == 'cross_workspace_activation',
            'INCOMPLETE_LIFECYCLE_RACES')
    race = races[0]
    intervals = [tuple(datetime.fromisoformat(at) for at in job['transaction_interval'])
                 for job in race['jobs']]
    require(validate_race(race['jobs'], intervals,
        ('LIFECYCLE_CONTROL_STALE', 'LIFECYCLE_CONTROL_HISTORY_STALE')) == race['winner'],
        'RACE_WINNER_MISMATCH')


class LifecycleScope(Scope):
    def __init__(self, run_id):
        super().__init__(run_id)
        self.dataset = f'{PROJECT}.ci_prompt_lifecycle_it_{run_id}'
        self.labels = {'purpose': 'prompt_lifecycle_integration', 'run_id': run_id}

    def table(self, name):
        require(name in TABLES, 'UNRELATED_TABLE')
        return f'{self.dataset}.{name}'

    def assert_sql(self, script):
        refs = re.findall(r'`([^`]+)`', script)
        require(bool(refs) and set(refs) <= {self.table(name) for name in TABLES}
                and DATASET_ID not in script, 'UNSAFE_SQL')

    def relocate(self, script):
        for name in TABLES:
            script = script.replace(f'`{DATASET_ID}.{name}`', f'`{self.table(name)}`')
        self.assert_sql(script)
        return script


@dataclass(frozen=True, slots=True)
class ScopedLifecycleWrite(LifecycleWrite):
    fixture_scope: LifecycleScope | None = None
    fixture_slot: str = 'global'
    fixture_fault: str | None = None

    def __post_init__(self):
        super(ScopedLifecycleWrite, self).__post_init__()
        require(isinstance(self.fixture_scope, LifecycleScope)
                and re.fullmatch(r'[a-z0-9_]+', self.fixture_slot) is not None
                and self.fixture_fault in {None, 'rollback', 'postcommit', 'first_rollback'}, 'INVALID_FIXTURE_WRITE')

    @property
    def job_id(self):
        return f'lifecycle_it_{self.fixture_scope.run_id}_{self.fixture_slot}_{super(ScopedLifecycleWrite, self).job_id}'


def scoped_sql(plan):
    require(isinstance(plan, ScopedLifecycleWrite), 'UNSCOPED_LIFECYCLE_WRITE')
    script = plan.fixture_scope.relocate(production_sql(plan))
    fault = plan.fixture_fault
    if fault == 'rollback' or fault == 'first_rollback' and plan.native_slot == 1:
        marker = '\nCOMMIT TRANSACTION;'
        require(script.count(marker) == 1, 'FAULT_MARKER_MISSING')
        script = script.replace(marker, "\nASSERT FALSE AS 'FIXTURE_ROLLBACK';" + marker)
    elif fault == 'postcommit':
        marker = '\nSELECT receipt_state AS state,'
        require(script.count(marker) == 1, 'FAULT_MARKER_MISSING')
        script = script.replace(marker, "\nASSERT FALSE AS 'FIXTURE_AFTER_COMMIT';" + marker)
    plan.fixture_scope.assert_sql(script)
    return script


@contextmanager
def owned_sql(scope):
    require(isinstance(scope, LifecycleScope), 'INVALID_SCOPE')
    original_read = storage.read_sql
    def write(plan):
        require(isinstance(plan, ScopedLifecycleWrite) and plan.fixture_scope is scope, 'FOREIGN_FIXTURE_SCOPE')
        return scoped_sql(plan)
    with patch.object(storage, 'sql', write), patch.object(storage, 'read_sql',
            lambda: scope.relocate(original_read())):
        yield


class FixtureLifecycleStore(storage.BigQueryPromptLifecycleStore):
    def __init__(self, probe):
        super().__init__(probe.client, EvaluationStoreSettings(
            execution_enabled=True, rpc_timeout_seconds=20, result_timeout_seconds=60))
        self.probe = probe

    def _preflight(self):
        self.probe.preflight()


class LifecycleProbe(Probe):
    def __init__(self, client):
        super().__init__(client)
        self.scope = LifecycleScope(uuid4().hex)
        self.report.update(batch=15, scope='ISOLATED_NATIVE_PROMPT_LIFECYCLE',
            fixture_dataset=self.scope.dataset, races=[], verified_native_error_codes={})

    def own(self):
        self.scope.own(self.client.get_dataset(self.scope.dataset, retry=None,
            timeout=FIXTURE_RPC_TIMEOUT_SECONDS))

    def cleanup(self):
        if not self.owned:
            return
        self.own()
        self.client.delete_dataset(self.scope.dataset, delete_contents=True,
            not_found_ok=False, retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS)
        try:
            self.client.get_dataset(self.scope.dataset, retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS)
        except NotFound:
            self.owned = False
            self.report['cleanup'] = 'DELETED_AND_ABSENCE_VERIFIED'
            return
        raise RuntimeError('CLEANUP_NOT_VERIFIED')

    def query(self, script, config=None, job_id=None):
        """Bounded fixture reads/submissions; no automatic mutation retry."""
        self.own()
        self.scope.assert_sql(script)
        self.counter += 1
        job_id = job_id or f'lifecycle_it_{self.scope.run_id}_read_{self.counter}'
        cfg = config or bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False,
            create_session=False, query_parameters=[], maximum_bytes_billed=512*1024**2,
            job_timeout_ms=60000, script_options=bigquery.ScriptOptions(statement_timeout_ms=20000))
        expected = bigquery.QueryJobConfig.from_api_repr(cfg.to_api_repr())
        job = self.client.query(script, job_config=cfg, job_id=job_id, project=PROJECT,
            location=LOCATION, retry=None, job_retry=None,
            api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=FIXTURE_RPC_TIMEOUT_SECONDS)
        require(matches_sharing_job(job, job_id=job_id, sql=script, config=expected), 'QUERY_MISMATCH')
        rows = [normalize(row) for row in job.result(max_results=2, page_size=2,
            retry=None, job_retry=None, timeout=60)]
        require(len(rows) <= 2 and job.state == 'DONE' and job.error_result is None
                and matches_sharing_job(job, job_id=job_id, sql=script, config=expected), 'QUERY_MISMATCH')
        return rows

    def serving_rows(self):
        return {name: self.client.get_table(spec.table(PROJECT).reference,
            retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS).num_rows for name, spec in TABLES.items()}

    def setup(self):
        validate_dataset(self.client.get_dataset(DATASET_ID, retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS))
        dataset = bigquery.Dataset(self.scope.dataset)
        dataset.location, dataset.labels, dataset.default_table_expiration_ms = LOCATION, self.scope.labels, TTL_MS
        self.client.create_dataset(dataset, exists_ok=False, retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS)
        self.owned, self.report['cleanup'] = True, 'PENDING'
        self.own()
        for name, spec in TABLES.items():
            expected = spec.table(PROJECT)
            table = bigquery.Table(self.scope.table(name), schema=expected.schema)
            table.clustering_fields, table.time_partitioning = expected.clustering_fields, expected.time_partitioning
            table.require_partition_filter = expected.require_partition_filter
            self.client.create_table(table, exists_ok=False, retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS)
        self.preflight()

    def preflight(self):
        self.own()
        for name, spec in TABLES.items():
            actual, expected = self.client.get_table(self.scope.table(name), retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS), spec.table(PROJECT)
            require(actual.project == PROJECT and actual.dataset_id == self.scope.dataset.split('.')[1]
                and actual.table_id == name and actual.table_type == 'TABLE' and actual.location.lower() == LOCATION
                and _schema(actual.schema) == _schema(expected.schema) and _partition(actual) == _partition(expected)
                and actual.clustering_fields == expected.clustering_fields
                and actual.require_partition_filter == expected.require_partition_filter
                and actual.expires is not None, 'SCHEMA_MISMATCH')

    def load(self, name, rows):
        require(name in TABLES, 'UNRELATED_TABLE')
        self.preflight()
        self.counter += 1
        cfg = bigquery.LoadJobConfig(schema=TABLES[name].schema, write_disposition='WRITE_APPEND')
        job = self.client.load_table_from_json(json.loads(wire(rows)), self.scope.table(name),
            job_config=cfg, job_id=f'lifecycle_it_{self.scope.run_id}_load_{self.counter}',
            project=PROJECT, location=LOCATION, num_retries=0, timeout=FIXTURE_RPC_TIMEOUT_SECONDS)
        job.result(timeout=60, retry=None)
        require(job.error_result is None, 'FIXTURE_LOAD_FAILED')
        print(json.dumps(dict(event='prompt_lifecycle_fixture_loaded', table=name, rows=len(rows))), flush=True)

    def snapshot(self):
        fields = []
        for name in TABLES:
            where = (" WHERE changed_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31 23:59:59.999999+00')"
                     if name == 'prompt_registry_history' else '')
            source = f'`{self.scope.table(name)}` t{where}'
            fields.extend([f'(SELECT COUNT(*) FROM {source}) AS {name}_count',
                f'ARRAY(SELECT TO_JSON_STRING(t) FROM {source} ORDER BY TO_JSON_STRING(t) LIMIT 1025) AS {name}'])
        row = self.query('SELECT ' + ', '.join(fields))[0]
        require(all(row[name + '_count'] == len(row[name]) <= 1024 for name in TABLES), 'FIXTURE_SNAPSHOT_OVERFLOW')
        return {name: row[name] for name in TABLES}

    def request(self, context, version=0, *, evaluation=False, other=False):
        now = int(datetime.now(timezone.utc).timestamp())
        identity = replace(context['identity'], issued_at=now-1, expires_at=now+599)
        source = context['states'][version].head.snapshot
        return LifecycleReadRequest(context['other'] if other else context['workspace'], context['key'], source.task,
            source.vertex_prompt_id, source.vertex_version_id, identity, 1,
            context['reports'][version].evaluation_id if evaluation else None)

    def prepare(self, store, context, status, version=0, *, other=False, slot='global', fault=None):
        request = self.request(context, version, evaluation=status in {Lifecycle.EVALUATED, Lifecycle.ACTIVE}, other=other)
        capture = store.read(request)
        return ScopedLifecycleWrite(capture, status, datetime.now(timezone.utc), 'FIXTURE_REVIEWED',
            fixture_scope=self.scope, fixture_slot=slot, fixture_fault=fault)

    def job_info(self, plan, outcome):
        job = self.client.get_job(plan.job_id, project=PROJECT, location=LOCATION, retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS)
        require(matches_sharing_job(job, job_id=plan.job_id, sql=scoped_sql(plan), config=config(plan)), 'NATIVE_JOB_MISMATCH')
        info = dict(job_id=plan.job_id, status=outcome.native_status.value,
            receipt_state=None if outcome.receipt is None else outcome.receipt['state'], error=job.error_result)
        self.report['jobs'].append(info)
        return info

    def write(self, store, plan, *, status=Status.COMMITTED, state='APPLIED', error=None):
        outcome = store.submit(plan)
        if outcome.native_status is Status.UNKNOWN:
            outcome = store.recover(plan)
        info = self.job_info(plan, outcome)
        require(outcome.native_status is status and (outcome.receipt is None if state is None
            else outcome.receipt is not None and outcome.receipt['state'] == state), 'UNEXPECTED_WRITE_OUTCOME')
        if error is not None:
            require(type(info['error']) is dict and info['error'].get('reason') == 'invalidQuery'
                and error in info['error'].get('message', ''), 'NATIVE_ASSERTION_MISMATCH')
            self.report['verified_native_error_codes'][plan.job_id] = error
        elif state is not None:
            require(info['error'] is None, 'UNEXPECTED_NATIVE_ERROR')
        return outcome

    def after(self, store, context, plan):
        capture = store.read(self.request(context))
        require(capture.control == plan.control
                and capture.control_history == plan.capture.control_history + (plan.control,),
                'LIFECYCLE_CONTROL_PARTIAL')
        for state in plan.after_states:
            version = int(state.head.snapshot.vertex_version_id) - 41
            actual = store.read(self.request(context, version))
            require(actual.target == state, 'LIFECYCLE_REGISTRY_PARTIAL')
        return capture

    def race(self, store, context, *, reservation=False):
        for version in (0, 1):
            self.write(store, self.prepare(store, context, Lifecycle.EVALUATED, version))
        plans = [self.prepare(store, context, Lifecycle.ACTIVE, version, other=bool(version),
            slot='global' if reservation else ('race_left' if version == 0 else 'race_right')) for version in (0, 1)]
        require(plans[0].control.revision == plans[1].control.revision
                and plans[0].command_digest != plans[1].command_digest
                and ((plans[0].job_id == plans[1].job_id) == reservation), 'RACE_SLOT_MISMATCH')
        before = self.snapshot()
        barrier = Barrier(2)
        def work(plan):
            local = FixtureLifecycleStore(self)
            def preflight():
                self.preflight()
                barrier.wait(timeout=30)
            local._preflight = preflight
            outcome = local.submit(plan)
            return local.recover(plan) if outcome.native_status is Status.UNKNOWN else outcome
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(work, plan) for plan in plans]
            outcomes = [future.result(timeout=120) for future in futures]
        if reservation:
            winners = [i for i, result in enumerate(outcomes) if result.native_status is Status.COMMITTED]
            require(len(winners) == 1 and outcomes[1-winners[0]].native_status is Status.UNKNOWN,
                    'GLOBAL_RESERVATION_FAILED')
            winner = winners[0]
            self.job_info(plans[winner], outcomes[winner])
            try:
                store.issue_retry(plans[1-winner])
            except ContractError as error:
                require(error.code == 'LIFECYCLE_RETRY_NONCOMMIT_UNVERIFIED', 'WRONG_RETRY_DENIAL')
            else:
                raise RuntimeError('UNKNOWN_GRANTED_RETRY')
            self.record('competing_workspaces_share_one_native_slot_unknown_cannot_retry')
        else:
            infos, intervals = [], []
            for plan, outcome in zip(plans, outcomes):
                info = self.job_info(plan, outcome)
                children = list(self.client.list_jobs(project=PROJECT, parent_job=plan.job_id,
                    all_users=False, max_results=64, page_size=64, retry=None, timeout=FIXTURE_RPC_TIMEOUT_SECONDS))
                interval = transaction_interval(children, plan.job_id)
                intervals.append(interval)
                info['transaction_interval'] = [at.isoformat() for at in interval]
                infos.append(info)
            winner = validate_race(infos, intervals, ('LIFECYCLE_CONTROL_STALE', 'LIFECYCLE_CONTROL_HISTORY_STALE'))
            self.report['races'].append(dict(case='cross_workspace_activation', winner=winner, jobs=infos))
            self.record('overlapping_cross_workspace_promotions_one_complete_winner')
        self.after(store, context, plans[winner])
        changed = {state.head.snapshot.vertex_version_id: state for state in plans[winner].after_states}
        for version, contender in enumerate(plans):
            expected = changed.get(contender.capture.target.head.snapshot.vertex_version_id,
                                   contender.capture.target)
            require(store.read(self.request(context, version)).target == expected,
                    'RACE_NONWINNER_VERSION_CHANGED')
        after = self.snapshot()
        mutable = {'prompt_registry', 'prompt_registry_history', 'prompt_lifecycle_controls', 'prompt_lifecycle_history'}
        for name in TABLES:
            field = 'workspace_id' if name == 'workspaces' else 'prompt_key'
            allowed = plans[winner].capture.request.workspace_id if name == 'workspaces' else context['key']
            outside = lambda rows: [row for row in rows if json.loads(row).get(field) != allowed]
            require(outside(before[name]) == outside(after[name]), 'RACE_UNRELATED_ROWS_CHANGED')
            if name not in mutable | {'workspaces'}:
                require(before[name] == after[name], 'RACE_UNRELATED_ROWS_CHANGED')
        workspace = plans[winner].capture.request.workspace_id
        old = next(json.loads(row) for row in before['workspaces'] if json.loads(row)['workspace_id'] == workspace)
        new = next(json.loads(row) for row in after['workspaces'] if json.loads(row)['workspace_id'] == workspace)
        require(new['revision'] == old['revision'] + 1
            and datetime.fromisoformat(new['updated_at']) >= datetime.fromisoformat(old['updated_at'])
            and {k: v for k, v in new.items() if k not in {'revision', 'updated_at'}}
                == {k: v for k, v in old.items() if k not in {'revision', 'updated_at'}},
            'RACE_WORKSPACE_FENCE_MISMATCH')

    def run(self, case_group='ALL'):
        require(case_group in CASE_GROUPS, 'INVALID_LIFECYCLE_CASE_GROUP')
        self.report['case_group'] = case_group
        rows, contexts = lifecycle_fixtures()
        for name, values in rows.items():
            self.load(name, values)
        sentinel = self.unrelated()
        store = FixtureLifecycleStore(self)
        with owned_sql(self.scope):
            if case_group != 'REMAINING':
                self.core(store, contexts)
            if case_group != 'CORE':
                self.remaining(store, contexts)
            require(self.unrelated() == sentinel, 'UNRELATED_ROWS_CHANGED')
            if case_group != 'CORE':
                self.record('unrelated_fixture_rows_preserved')

    def core(self, store, contexts):
        flow = contexts['flow']
        for version in (0, 1):
            for status in (Lifecycle.REGISTERED, Lifecycle.EVALUATED):
                plan = self.prepare(store, flow, status, version)
                self.write(store, plan)
                self.after(store, flow, plan)
                if status is Lifecycle.REGISTERED:
                    at = datetime.now(timezone.utc)
                    record = replace(flow['reports'][version], registry_record=plan.after_states[0].head,
                        started_at=at, completed_at=at, valid_until=at+timedelta(hours=1))
                    records = list(flow['reports'])
                    records[version] = record
                    flow['reports'] = tuple(records)
                    summary, cases = encode_evaluation(flow['control'], record)
                    self.load('prompt_evaluations', [summary])
                    self.load('prompt_evaluation_cases', cases)
        self.record('registered_and_evaluated_authoritative_chains')
        for version, status, case in ((0, Lifecycle.ACTIVE, 'initial_activation'),
            (1, Lifecycle.ACTIVE, 'atomic_active_replacement'), (0, Lifecycle.ACTIVE, 'rollback_to_evaluated_retired_version'),
            (0, Lifecycle.RETIRED, 'retirement_clears_active_pin'), (1, Lifecycle.ACTIVE, 'reactivation'),
            (1, Lifecycle.BLOCKED, 'blocking_clears_active_pin')):
            plan = self.prepare(store, flow, status, version)
            self.write(store, plan)
            self.after(store, flow, plan)
            self.record(case)
        recovery = contexts['recovery']
        original = self.prepare(store, recovery, Lifecycle.BLOCKED)
        first = self.write(store, original)
        before = self.snapshot()
        duplicate = store.submit(original)
        require(not duplicate.submitted and duplicate.receipt == first.receipt, 'DUPLICATE_RESUBMITTED')
        self.write(store, replace(original, fixture_slot='replay'), state='REUSED')
        require(self.snapshot() == before, 'REPLAY_CHANGED_ROWS')
        self.record('same_job_recovery_and_new_job_exact_replay')
        lost = self.prepare(store, recovery, Lifecycle.BLOCKED, 1)
        self.query(scoped_sql(lost), config(lost), lost.job_id)  # Discard receipt, retain original job.
        recovered = store.recover(lost)
        require(not recovered.submitted and recovered.native_status is Status.COMMITTED
            and recovered.receipt['state'] == 'APPLIED', 'ORIGINAL_RECOVERY_FAILED')
        self.after(store, recovery, lost)
        self.record('discarded_response_original_job_recovery')
        before = self.snapshot()
        self.write(store, replace(original, fixture_slot='historical_replay'), state='REUSED')
        require(self.snapshot() == before, 'HISTORICAL_REPLAY_REGRESSED_STATE')
        self.record('historical_replay_preserves_later_control_head')
        for case, fault in (('retry', 'first_rollback'), ('cap', 'rollback')):
            context = contexts[case]
            plan = self.prepare(store, context, Lifecycle.BLOCKED, fault=fault)
            before = self.snapshot()
            self.write(store, plan, status=Status.NOT_COMMITTED, state=None, error='FIXTURE_ROLLBACK')
            require(self.snapshot() == before, 'ROLLBACK_CHANGED_ROWS')
            retry = store.issue_retry(plan)
            require(retry.capture == plan.capture and retry.control == plan.control
                and retry.job_id != plan.job_id and retry.native_slot == 2, 'RETRY_REBOUND')
            if case == 'retry':
                self.write(store, retry)
                self.after(store, context, retry)
                require(not store.submit(retry).submitted, 'RETRY_RESUBMITTED')
                self.record('positive_rollback_only_retry_preserves_pins_and_recovers')
            else:
                self.write(store, retry, status=Status.NOT_COMMITTED, state=None, error='FIXTURE_ROLLBACK')
                final = store.issue_retry(retry)
                self.write(store, final, status=Status.NOT_COMMITTED, state=None, error='FIXTURE_ROLLBACK')
                try:
                    store.issue_retry(final)
                except ContractError as error:
                    require(error.code == 'LIFECYCLE_NATIVE_SLOTS_EXHAUSTED', 'WRONG_RETRY_DENIAL')
                else:
                    raise RuntimeError('RETRY_CAP_NOT_ENFORCED')
                require(self.snapshot() == before, 'RETRY_ROLLBACK_CHANGED_ROWS')
                self.record('three_native_slots_exhausted_without_partial_rows')
        context = contexts['postcommit']
        plan = self.prepare(store, context, Lifecycle.BLOCKED, fault='postcommit')
        self.write(store, plan, state=None, error='FIXTURE_AFTER_COMMIT')
        outcome = store.recover(plan)
        require(outcome.native_status is Status.COMMITTED and outcome.receipt is None and not outcome.submitted,
                'POSTCOMMIT_RECOVERY_FAILED')
        self.after(store, context, plan)
        try:
            store.issue_retry(plan)
        except ContractError as error:
            require(error.code == 'LIFECYCLE_RETRY_NONCOMMIT_UNVERIFIED', 'WRONG_RETRY_DENIAL')
        else:
            raise RuntimeError('COMMIT_GRANTED_RETRY')
        self.record('postcommit_error_positive_commit_without_invented_receipt_or_retry')

    def remaining(self, store, contexts):
        plan = self.prepare(store, contexts['unknown'], Lifecycle.BLOCKED)
        before = self.snapshot()
        require(store.recover(plan).native_status is Status.UNKNOWN, 'ABSENCE_INFERRED_ROLLBACK')
        try:
            store.issue_retry(plan)
        except NotFound:
            pass
        else:
            raise RuntimeError('ABSENCE_GRANTED_RETRY')
        require(self.snapshot() == before, 'UNKNOWN_CHANGED_ROWS')
        self.record('absent_original_is_unknown_and_cannot_advance')
        context = contexts['expiry']
        request = self.request(context, evaluation=True)
        record, target, control = context['reports'][0], context['states'][0], context['control']
        at = record.completed_at
        evidence = EvaluationSnapshot(EvaluationReadRequest(request.workspace_id, request.prompt_key,
            request.identity, 1, record.evaluation_id), control, (control,), record, target, at)
        capture = LifecycleSnapshot(request, None, (), target, None, evidence, at, control)
        expired = ScopedLifecycleWrite(capture, Lifecycle.EVALUATED, at, 'FIXTURE_REVIEWED', fixture_scope=self.scope)
        before = self.snapshot()
        self.write(store, expired, status=Status.NOT_COMMITTED, state=None,
            error='LIFECYCLE_EVALUATION_EXPIRED_OR_FAILED')
        require(self.snapshot() == before, 'EXPIRY_CHANGED_ROWS')
        self.record('evaluation_expiry_at_native_dispatch_denies')
        context = contexts['assertion']
        plan = self.prepare(store, context, Lifecycle.BLOCKED)
        identity = plan.capture.request.identity
        expired_identity = replace(identity, issued_at=identity.issued_at-660, expires_at=identity.expires_at-660)
        plan = replace(plan, capture=replace(plan.capture,
            request=replace(plan.capture.request, identity=expired_identity)))
        before = self.snapshot()
        self.write(store, plan, status=Status.NOT_COMMITTED, state=None, error='ASSERTION_EXPIRED')
        require(self.snapshot() == before, 'ASSERTION_EXPIRY_CHANGED_ROWS')
        self.record('assertion_expiry_denies')
        context = contexts['policy']
        plan = self.prepare(store, context, Lifecycle.EVALUATED)
        control = context['control']
        replacement = replace(control, revision=2, parent_digest=control.digest,
            updated_at=datetime.now(timezone.utc), policy=replace(control.policy, version='fixture-v2'))
        change = PromptEvaluationWrite(EvaluationWriteKind.CONTROL, replacement,
            plan.capture.request.identity, 1, previous=control, control_history=(control,))
        self.query(self.scope.relocate(change.sql), change.job_config())
        before = self.snapshot()
        self.write(store, plan, status=Status.NOT_COMMITTED, state=None, error='LIFECYCLE_EVALUATION_CONTROL_STALE')
        require(self.snapshot() == before, 'STALE_POLICY_CHANGED_ROWS')
        self.record('changed_authoritative_policy_denies_stale_promotion')
        context = contexts['revocation']
        plan = self.prepare(store, context, Lifecycle.BLOCKED)
        self.write(store, plan)
        self.query(f"UPDATE `{self.scope.table('memberships')}` SET status='DISABLED', revision=revision+1 "
            f"WHERE workspace_id='{context['workspace']}'; SELECT 1 AS updated")
        before = self.snapshot()
        self.write(store, replace(plan, fixture_slot='revoked_replay'), status=Status.NOT_COMMITTED,
            state=None, error='MEMBERSHIP_DENIED')
        require(self.snapshot() == before, 'REVOKED_REPLAY_CHANGED_ROWS')
        self.record('revocation_denies_historical_replay')
        self.race(store, contexts['race'])
        self.race(store, contexts['reservation'], reservation=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    groups = parser.add_mutually_exclusive_group()
    groups.add_argument('--core-only', action='store_const', const='CORE', dest='case_group')
    groups.add_argument('--remaining-only', action='store_const', const='REMAINING', dest='case_group')
    parser.set_defaults(case_group='ALL')
    args = parser.parse_args()
    with bigquery.Client(project=PROJECT, location=LOCATION) as client:
        probe = LifecycleProbe(client)
        before = probe.serving_rows()
        try:
            probe.setup()
            probe.run(args.case_group)
            validate_native_report(probe.report)
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
                require(before == after, 'SERVING_METADATA_CHANGED')
                probe.report.update(serving_rows_before=before, serving_rows_after=after,
                    serving_row_metadata_unchanged=True)
            except Exception as error:
                probe.report.update(status='FAILED', preservation_error=str(error)[:200])
            args.output_dir.mkdir(parents=True, exist_ok=True)
            path = args.output_dir / f'{probe.scope.run_id}.json'
            path.write_text(json.dumps(probe.report, indent=2, default=str) + '\n', encoding='utf-8')
            print(json.dumps(dict(event='prompt_lifecycle_complete', status=probe.report['status'],
                cases=len(probe.report['cases']), cleanup=probe.report['cleanup'], report=str(path))), flush=True)
        require(probe.report['status'] == 'PASSED', 'PROMPT_LIFECYCLE_PROBE_FAILED')


if __name__ == '__main__':
    main()
