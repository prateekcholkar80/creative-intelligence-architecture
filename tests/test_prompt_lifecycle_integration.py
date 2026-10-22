"""Offline guards for Batch 15's explicit owned native verification."""
from dataclasses import replace
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import inspect
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application import prompt_lifecycle_store as storage
from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_evaluation_storage import decode_evaluation
from src.application.prompt_evaluation_store import EvaluationSnapshot, EvaluationReadRequest
from src.application.prompt_lifecycle_contract import LifecycleReadRequest, LifecycleSnapshot, LifecycleWrite
from src.application.prompt_lifecycle_query import config, sql
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION, DATASET_ID
from src.validation.prompt_lifecycle_fixtures import lifecycle_fixtures, SCENARIOS
from src.validation.prompt_lifecycle_integration import (
    LifecycleScope, LifecycleProbe, ScopedLifecycleWrite, FixtureLifecycleStore,
    TABLES, Status, owned_sql, scoped_sql,
    EXPECTED_CASES, validate_native_report,
    CASE_GROUPS,
)
from src.validation.signing_read_integration import TTL_MS


class LifecycleIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.rows, self.contexts = lifecycle_fixtures(self.now)
        self.context = self.contexts['recovery']
        self.scope = LifecycleScope('a'*32)
        self.client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)
        request = LifecycleReadRequest(self.context['workspace'], self.context['key'],
            self.context['states'][0].head.snapshot.task, self.context['states'][0].head.snapshot.vertex_prompt_id,
            '41', self.context['identity'], 1)
        self.capture = LifecycleSnapshot(request, None, (), self.context['states'][0], None, None, self.now)
        self.base = LifecycleWrite(self.capture, PromptLifecycle.BLOCKED, self.now, 'FIXTURE_REVIEWED')
        self.plan = ScopedLifecycleWrite(self.capture, PromptLifecycle.BLOCKED, self.now, 'FIXTURE_REVIEWED',
            fixture_scope=self.scope)

    def owned(self):
        return Mock(full_dataset_id=f'{PROJECT}:{self.scope.dataset.split(".")[1]}',
            location=LOCATION, labels=self.scope.labels, default_table_expiration_ms=TTL_MS)

    def test_independent_scopes_have_unique_keys_versions_and_full_synthetic_cases(self):
        self.assertEqual(len(self.contexts), len(SCENARIOS))
        self.assertEqual(len({c['key'] for c in self.contexts.values()}), len(SCENARIOS))
        self.assertEqual(len({(s.head.snapshot.vertex_prompt_id, s.head.snapshot.vertex_version_id)
            for c in self.contexts.values() for s in c['states']}), 2*len(SCENARIOS))
        for context in self.contexts.values():
            self.assertEqual(tuple(s.head.snapshot.vertex_version_id for s in context['states']), ('41', '42'))
            self.assertEqual(len(context['reports'][0].results), 10)
        self.assertEqual(len(TABLES), 10)
        self.assertFalse(any(r['prompt_key'] == self.contexts['flow']['key']
            for r in self.rows['prompt_evaluations']))

    def test_evaluation_rows_roundtrip_exact_registered_version(self):
        record = self.context['reports'][0]
        summary = next(row for row in self.rows['prompt_evaluations']
            if row['evaluation_id'] == record.evaluation_id and row['workspace_id'] == self.context['workspace'])
        cases = [row for row in self.rows['prompt_evaluation_cases']
            if row['evaluation_id'] == record.evaluation_id and row['workspace_id'] == self.context['workspace']]
        self.assertEqual(decode_evaluation(self.context['control'], summary, cases, self.context['states'][0]), record)

    def test_relocation_leaves_production_parameters_digest_and_global_slot_semantics(self):
        self.scope.assert_sql(scoped_sql(self.plan))
        self.scope.relocate(storage.read_sql())
        self.assertEqual(config(self.plan).to_api_repr(), config(self.base).to_api_repr())
        self.assertEqual(self.plan.command_digest, self.base.command_digest)
        other = replace(self.plan, capture=replace(self.capture,
            request=replace(self.capture.request, workspace_id=self.context['other'])))
        self.assertEqual(self.plan.job_id, other.job_id)
        self.assertNotEqual(self.plan.command_digest, other.command_digest)
        self.assertNotEqual(self.plan.job_id, replace(self.plan, fixture_slot='race_other').job_id)

    def test_patch_is_explicit_restored_and_rejects_unscoped_or_foreign_plans(self):
        original_sql, original_read = storage.sql, storage.read_sql
        with owned_sql(self.scope):
            self.assertEqual(storage.sql(self.plan), scoped_sql(self.plan))
            self.scope.assert_sql(storage.read_sql())
            for plan in (self.base, replace(self.plan, fixture_scope=LifecycleScope('b'*32))):
                with self.assertRaises(RuntimeError):
                    storage.sql(plan)
        self.assertIs(storage.sql, original_sql)
        self.assertIs(storage.read_sql, original_read)
        self.assertEqual(storage.sql(self.base), sql(self.base))

    def test_foreign_unqualified_and_unknown_table_references_deny(self):
        for script in (sql(self.base), sql(self.base)+'\nSELECT * FROM `other.other.foreign`;',
                       'SELECT 1', 'SELECT * FROM project.ci_application.memberships'):
            with self.assertRaises(RuntimeError):
                self.scope.assert_sql(script)
        for name in ('requests', 'outputs'):
            with self.assertRaises(RuntimeError):
                self.scope.table(name)
        with self.assertRaises(RuntimeError):
            LifecycleScope('../serving')

    def test_retry_replace_retains_scope_fault_and_pins_with_distinct_bounded_slots(self):
        first = replace(self.plan, fixture_fault='first_rollback')
        second = replace(first, native_slot=2, ancestors=(first,))
        third = replace(second, native_slot=3, ancestors=(first, second))
        self.assertIn('FIXTURE_ROLLBACK', scoped_sql(first))
        self.assertNotIn('FIXTURE_ROLLBACK', scoped_sql(second))
        self.assertEqual(first.control, third.control)
        self.assertIs(second.fixture_scope, self.scope)
        self.assertEqual(len({p.job_id for p in (first, second, third)}), 3)
        with self.assertRaises(ContractError):
            replace(third, native_slot=4, ancestors=(first, second, third))

    def test_faults_are_only_at_commit_boundaries_and_bad_slots_deny(self):
        for fault, code in (('rollback', 'FIXTURE_ROLLBACK'), ('postcommit', 'FIXTURE_AFTER_COMMIT')):
            script = scoped_sql(replace(self.plan, fixture_fault=fault))
            self.assertEqual(script.count(code), 1)
            self.scope.assert_sql(script)
        for fields in ({'fixture_fault': 'other'}, {'fixture_slot': '../bad'}, {'fixture_scope': None}):
            with self.assertRaises(RuntimeError):
                replace(self.plan, **fields)

    def test_expiry_fixture_passes_pure_old_gate_but_is_expired_at_current_time(self):
        context = self.contexts['expiry']
        record, target, control = context['reports'][0], context['states'][0], context['control']
        request = replace(self.capture.request, workspace_id=context['workspace'], prompt_key=context['key'],
            vertex_prompt_id=target.head.snapshot.vertex_prompt_id, evaluation_id=record.evaluation_id)
        at = record.completed_at
        evidence = EvaluationSnapshot(EvaluationReadRequest(request.workspace_id, request.prompt_key,
            request.identity, 1, record.evaluation_id), control, (control,), record, target, at)
        capture = LifecycleSnapshot(request, None, (), target, None, evidence, at, control)
        plan = ScopedLifecycleWrite(capture, PromptLifecycle.EVALUATED, at, 'FIXTURE_REVIEWED', fixture_scope=self.scope)
        self.assertLess(plan.changed_at, record.valid_until)
        self.assertLess(record.valid_until, self.now)
        self.assertIn('LIFECYCLE_EVALUATION_EXPIRED_OR_FAILED', scoped_sql(plan))

    def test_exact_ownership_and_expiry_required_before_cleanup(self):
        self.scope.own(self.owned())
        for key, value in (('labels', {}), ('location', 'us'), ('full_dataset_id', PROJECT+':ci_application'),
                           ('default_table_expiration_ms', None)):
            actual = self.owned()
            setattr(actual, key, value)
            with self.assertRaises(RuntimeError):
                self.scope.own(actual)
        probe = LifecycleProbe(self.client)
        probe.scope, probe.owned = self.scope, True
        self.client.get_dataset.return_value = Mock(full_dataset_id=PROJECT+':ci_application')
        with self.assertRaises(RuntimeError):
            probe.cleanup()
        self.client.delete_dataset.assert_not_called()

    def test_cleanup_verifies_absence(self):
        probe = LifecycleProbe(self.client)
        probe.scope, probe.owned = self.scope, True
        self.client.get_dataset.side_effect = [self.owned(), NotFound('absent')]
        probe.cleanup()
        self.client.delete_dataset.assert_called_once_with(self.scope.dataset, delete_contents=True,
            not_found_ok=False, retry=None, timeout=30)
        self.assertFalse(probe.owned)

    def test_known_native_mismatch_never_submits_or_grants_retry(self):
        probe = Mock(client=self.client)
        store = FixtureLifecycleStore(probe)
        self.client.get_job.return_value = Mock(query='wrong', job_id=self.plan.job_id)
        with owned_sql(self.scope):
            self.assertIs(store.submit(self.plan).native_status, Status.UNKNOWN)
            with self.assertRaises(ContractError):
                store.issue_retry(self.plan)
        self.client.query.assert_not_called()

    def test_default_production_executor_stays_disabled(self):
        with self.assertRaises(ContractError):
            storage.BigQueryPromptLifecycleStore(self.client).submit(self.base)
        self.client.query.assert_not_called()
        store = FixtureLifecycleStore(Mock(client=self.client))
        self.assertEqual(store.settings.rpc_timeout_seconds, 20)
        store._preflight()
        self.client.get_table.assert_not_called()

    def test_load_uses_exact_schema_and_installed_sdk_upload_contract(self):
        probe = LifecycleProbe(self.client)
        probe.scope, probe.preflight = self.scope, Mock()
        self.client.load_table_from_json.return_value.error_result = None
        probe.load('workspaces', self.rows['workspaces'])
        call = self.client.load_table_from_json.call_args
        inspect.signature(bigquery.Client.load_table_from_json).bind(self.client, *call.args, **call.kwargs)
        self.assertEqual(call.kwargs['num_retries'], 0)
        self.assertEqual(call.kwargs['timeout'], 30)
        self.assertEqual(call.kwargs['job_config'].schema, list(TABLES['workspaces'].schema))
        self.assertEqual(call.args[1], self.scope.table('workspaces'))

    def test_wrong_schema_preflight_prevents_load(self):
        probe = LifecycleProbe(self.client)
        probe.scope = self.scope
        self.client.get_dataset.return_value = self.owned()
        self.client.get_table.return_value = TABLES['memberships'].table(PROJECT)
        with self.assertRaises(RuntimeError):
            probe.load('memberships', [])
        self.client.load_table_from_json.assert_not_called()

    def test_postwrite_requires_complete_original_control_history_prefix(self):
        probe = LifecycleProbe(self.client)
        probe.scope = self.scope
        store = Mock()
        store.read.return_value = Mock(control=self.plan.control,
            control_history=(self.plan.control,), target=self.plan.after_states[0])
        probe.after(store, self.context, self.plan)
        store.read.return_value.control_history = (self.plan.control, self.plan.control)
        with self.assertRaisesRegex(RuntimeError, 'LIFECYCLE_CONTROL_PARTIAL'):
            probe.after(store, self.context, self.plan)

    def complete_report(self):
        codes = ['FIXTURE_ROLLBACK'] * 4 + ['FIXTURE_AFTER_COMMIT',
            'LIFECYCLE_EVALUATION_EXPIRED_OR_FAILED', 'ASSERTION_EXPIRED',
            'LIFECYCLE_EVALUATION_CONTROL_STALE', 'MEMBERSHIP_DENIED']
        errors = {str(i): code for i, code in enumerate(codes)}
        jobs = [dict(job_id=key, status='COMMITTED' if code == 'FIXTURE_AFTER_COMMIT'
            else 'NOT_COMMITTED', receipt_state=None,
            error=dict(reason='invalidQuery', message=code)) for key, code in errors.items()]
        interval = [self.now.isoformat(), (self.now + timedelta(seconds=2)).isoformat()]
        contenders = [dict(status='COMMITTED', receipt_state='APPLIED', error=None,
            transaction_interval=interval), dict(status='NOT_COMMITTED', receipt_state=None,
            error=dict(reason='concurrentUpdate', message='Transaction is aborted due to concurrent update'),
            transaction_interval=interval)]
        return dict(cases=[dict(case=case, status='PASSED') for case in EXPECTED_CASES],
            jobs=jobs, verified_native_error_codes=errors,
            races=[dict(case='cross_workspace_activation', winner=0, jobs=contenders)])

    def test_report_requires_all_unique_successful_cases(self):
        report = self.complete_report()
        validate_native_report(report)
        for change in ('missing', 'duplicate', 'failed'):
            altered = deepcopy(report)
            if change == 'missing':
                altered['cases'].pop()
            elif change == 'duplicate':
                altered['cases'][0] = altered['cases'][1]
            else:
                altered['cases'][0]['status'] = 'FAILED'
            with self.assertRaisesRegex(RuntimeError, 'INCOMPLETE_LIFECYCLE_CASES'):
                validate_native_report(altered)

    def test_report_rejects_missing_duplicate_wrong_error_or_commit_evidence(self):
        report = self.complete_report()
        for change in ('missing', 'duplicate', 'wrong_error', 'wrong_reason', 'wrong_status', 'invented_receipt'):
            altered = deepcopy(report)
            if change == 'missing':
                altered['jobs'].pop(0)
            elif change == 'duplicate':
                altered['jobs'].append(altered['jobs'][0])
            elif change == 'wrong_error':
                altered['jobs'][0]['error']['message'] = 'unrelated SQL syntax failure'
            elif change == 'wrong_reason':
                altered['jobs'][0]['error']['reason'] = 'backendError'
            elif change == 'wrong_status':
                altered['jobs'][4]['status'] = 'NOT_COMMITTED'
            else:
                altered['jobs'][4]['receipt_state'] = 'APPLIED'
            with self.assertRaises(RuntimeError):
                validate_native_report(altered)

    def test_report_rejects_incomplete_denial_manifest(self):
        report = self.complete_report()
        report['verified_native_error_codes'].pop('0')
        with self.assertRaisesRegex(RuntimeError, 'INCOMPLETE_LIFECYCLE_DENIALS'):
            validate_native_report(report)

    def test_report_requires_overlap_positive_loser_rollback_and_matching_winner(self):
        report = self.complete_report()
        for change in ('missing', 'no_overlap', 'unknown_loser', 'wrong_winner'):
            altered = deepcopy(report)
            race = altered['races'][0]
            if change == 'missing':
                altered['races'].clear()
            elif change == 'no_overlap':
                race['jobs'][1]['transaction_interval'] = [
                    (self.now + timedelta(seconds=3)).isoformat(),
                    (self.now + timedelta(seconds=4)).isoformat()]
            elif change == 'unknown_loser':
                race['jobs'][1]['status'] = 'UNKNOWN'
            else:
                race['winner'] = 1
            with self.assertRaises(RuntimeError):
                validate_native_report(altered)

    def test_group_manifests_are_disjoint_complete_and_cannot_claim_all_cases(self):
        self.assertFalse(CASE_GROUPS['CORE'] & CASE_GROUPS['REMAINING'])
        self.assertEqual(CASE_GROUPS['CORE'] | CASE_GROUPS['REMAINING'], EXPECTED_CASES)
        self.assertEqual((len(CASE_GROUPS['CORE']), len(CASE_GROUPS['REMAINING'])), (13, 8))
        for group in ('CORE', 'REMAINING'):
            report = self.complete_report()
            report['case_group'] = group
            report['cases'] = [case for case in report['cases'] if case['case'] in CASE_GROUPS[group]]
            keep = lambda code: (code in {'FIXTURE_ROLLBACK', 'FIXTURE_AFTER_COMMIT'}) == (group == 'CORE')
            report['verified_native_error_codes'] = {key: code for key, code in
                report['verified_native_error_codes'].items() if keep(code)}
            if group == 'CORE':
                report['races'] = []
            validate_native_report(report)
            report['case_group'] = 'ALL'
            with self.assertRaisesRegex(RuntimeError, 'INCOMPLETE_LIFECYCLE_CASES'):
                validate_native_report(report)

    def test_explicit_group_routes_do_not_run_other_cases_or_skip_preservation(self):
        for group in CASE_GROUPS:
            probe = LifecycleProbe(self.client)
            probe.load, probe.core, probe.remaining = Mock(), Mock(), Mock()
            probe.unrelated = Mock(return_value=['sentinel'])
            with patch('src.validation.prompt_lifecycle_integration.lifecycle_fixtures', return_value=({}, {})):
                probe.run(group)
            self.assertEqual(probe.core.call_count, int(group != 'REMAINING'))
            self.assertEqual(probe.remaining.call_count, int(group != 'CORE'))
            self.assertEqual(probe.unrelated.call_count, 2)
            self.assertEqual(probe.report['case_group'], group)
        probe = LifecycleProbe(self.client)
        probe.load = Mock()
        with self.assertRaisesRegex(RuntimeError, 'INVALID_LIFECYCLE_CASE_GROUP'):
            probe.run('other')
        probe.load.assert_not_called()

    def test_fixture_query_uses_bounded_timeout_exact_job_and_no_automatic_retry(self):
        from tests.test_prompt_lifecycle_store import native_job
        probe = LifecycleProbe(self.client)
        probe.scope, probe.own = self.scope, Mock()
        script = f'SELECT 1 AS result FROM `{self.scope.table("workspaces")}` LIMIT 1'
        def query(script, **kwargs):
            job = native_job(kwargs['job_id'], script, kwargs['job_config'])
            job.result = Mock(return_value=[dict(result=1)])
            return job
        self.client.query.side_effect = query
        self.assertEqual(probe.query(script), [dict(result=1)])
        kwargs = self.client.query.call_args.kwargs
        self.assertEqual(kwargs['timeout'], 30)
        self.assertIsNone(kwargs['retry'])
        self.assertIsNone(kwargs['job_retry'])
        self.assertEqual(kwargs['api_method'], bigquery.enums.QueryApiMethod.INSERT)
        self.assertEqual(kwargs['job_config'].maximum_bytes_billed, 512*1024**2)
        self.client.query.side_effect = None
        wrong = native_job(kwargs['job_id'], 'wrong SQL', kwargs['job_config'])
        wrong.result = Mock()
        self.client.query.return_value = wrong
        with self.assertRaisesRegex(RuntimeError, 'QUERY_MISMATCH'):
            probe.query(script)
        wrong.result.assert_not_called()

    def test_fixture_ownership_uses_bounded_metadata_timeout(self):
        probe = LifecycleProbe(self.client)
        probe.scope = self.scope
        self.client.get_dataset.return_value = self.owned()
        probe.own()
        self.client.get_dataset.assert_called_once_with(self.scope.dataset, retry=None, timeout=30)


if __name__ == '__main__':
    unittest.main()
