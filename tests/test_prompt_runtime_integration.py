"""Offline ownership, fixture and evidence guards for Batch 16 native checks."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import inspect
import json
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.auth.credentials import AnonymousCredentials

from src.application.control_io import ControlPlan
from src.application.prompt_contract import PromptLifecycle
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION, DATASET_ID
from src.validation.prompt_runtime_fixtures import runtime_fixtures, SCENARIOS
from src.validation.signing_read_integration import TTL_MS
from src.validation.prompt_runtime_integration import (
    RuntimeScope, RuntimeProbe, TABLES, CASE_GROUPS, scoped_plan, validate_native_report,
)


class RuntimeNativeGuards(unittest.TestCase):
    def setUp(self):
        self.scope = RuntimeScope('a' * 32)
        self.client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)

    def owned(self):
        return Mock(full_dataset_id=f'{PROJECT}:{self.scope.dataset.split(".")[1]}',
                    location=LOCATION, labels=self.scope.labels, default_table_expiration_ms=TTL_MS)

    def test_scopes_are_independent_and_allow_only_exact_owned_tables(self):
        other = RuntimeScope('b' * 32)
        self.assertNotEqual(self.scope.dataset, other.dataset)
        self.assertIn('invocation_prompt_bindings', TABLES)
        for name in TABLES:
            script = f'SELECT * FROM `{DATASET_ID}.{name}`'
            relocated = self.scope.relocate(script)
            self.scope.assert_sql(relocated)
            self.assertIn(self.scope.table(name), relocated)
            with self.assertRaises(RuntimeError):
                other.assert_sql(relocated)
        for token in ('../serving', '', 'A' * 32, 'a' * 31):
            with self.assertRaises(RuntimeError):
                RuntimeScope(token)
        with self.assertRaises(RuntimeError):
            self.scope.table('unowned')

    def test_serving_foreign_and_unqualified_sql_are_rejected(self):
        for script in (f'SELECT * FROM `{DATASET_ID}.workspaces`',
                       'SELECT * FROM `foreign.dataset.table`', 'SELECT 1',
                       f'SELECT * FROM `{self.scope.table("workspaces")}` JOIN foreign.dataset.table ON TRUE',
                       f'SELECT * FROM {DATASET_ID}.workspaces'):
            with self.assertRaises(RuntimeError):
                self.scope.assert_sql(script)

    def test_exact_ownership_metadata_required(self):
        self.scope.own(self.owned())
        for key, value in (('labels', {}), ('location', 'us'),
                           ('full_dataset_id', PROJECT + ':ci_application'),
                           ('default_table_expiration_ms', None)):
            actual = self.owned()
            setattr(actual, key, value)
            with self.assertRaises(RuntimeError):
                self.scope.own(actual)

    def plan(self):
        script = (f'BEGIN TRANSACTION;\nSELECT * FROM `{DATASET_ID}.invocation_prompt_bindings`;\n'
                  'COMMIT TRANSACTION;\n-- PROBE_AFTER_COMMIT\nSELECT 1;')
        return ControlPlan('actor', 'workspace', 'command', 'native_job', script,
                           '{}', 'DISPATCH', '{}', '{}', 'model_invocations')

    def test_scoped_plan_keeps_pins_and_configuration_with_stable_owned_job(self):
        original = self.plan()
        scoped = scoped_plan(original, self.scope)
        self.scope.assert_sql(scoped.sql)
        self.assertEqual(scoped.job_id, 'runtime_it_' + self.scope.run_id + '_' + original.job_id)
        self.assertEqual(scoped_plan(original, self.scope), scoped)
        for name in ('actor_id', 'workspace_id', 'command_id', 'config_json', 'kind',
                     'candidate_json', 'audit_json', 'candidate_table'):
            self.assertEqual(getattr(scoped, name), getattr(original, name))
        self.assertNotEqual(scoped_plan(original, RuntimeScope('b' * 32)).job_id, scoped.job_id)

    def test_group_manifests_are_disjoint_complete_and_explicit(self):
        groups = [CASE_GROUPS[key] for key in ('CORE', 'DENIALS', 'RACE')]
        for index, group in enumerate(groups):
            self.assertTrue(group)
            for other in groups[index + 1:]:
                self.assertFalse(group & other)
        self.assertEqual(frozenset().union(*groups), CASE_GROUPS['ALL'])

    def test_fault_injection_requires_unique_exact_commit_boundary(self):
        original = self.plan()
        for fault, code in (('rollback', 'FIXTURE_ROLLBACK'), ('postcommit', 'FIXTURE_AFTER_COMMIT')):
            scoped = scoped_plan(original, self.scope, fault)
            self.assertEqual(scoped.sql.count(code), 1)
            if fault == 'rollback':
                self.assertLess(scoped.sql.index(code), scoped.sql.index('COMMIT TRANSACTION;'))
            else:
                self.assertGreater(scoped.sql.index(code), scoped.sql.index('COMMIT TRANSACTION;'))
            self.assertEqual(scoped.config_json, original.config_json)
        for fault in ('invalid', 1):
            with self.assertRaises(RuntimeError):
                scoped_plan(original, self.scope, fault)
        for script in (original.sql.replace('COMMIT TRANSACTION;', ''),
                       original.sql + '\nCOMMIT TRANSACTION;'):
            with self.assertRaises(RuntimeError):
                scoped_plan(replace(original, sql=script), self.scope, 'rollback')

    def report(self, group='ALL'):
        now = datetime.now(timezone.utc)
        codes = {'CORE': ['FIXTURE_ROLLBACK', 'FIXTURE_AFTER_COMMIT', 'RUNTIME_BINDING_ALREADY_EXISTS'],
                 'DENIALS': ['RUNTIME_EVALUATION_EXPIRED', 'RUNTIME_POLICY_EXPIRED',
                             'RUNTIME_PROMPT_CONFLICT', 'RUNTIME_PROMPT_CONFLICT',
                             'DISPATCH_CONFLICT', 'DISPATCH_CONFLICT'], 'RACE': []}
        selected = sum(codes.values(), []) if group == 'ALL' else codes[group]
        errors = {str(i): code for i, code in enumerate(selected)}
        jobs = [dict(job_id=job_id, status='COMMITTED' if code == 'FIXTURE_AFTER_COMMIT'
                     else 'NOT_COMMITTED', receipt_state=None,
                     error=dict(reason='invalidQuery', message=code)) for job_id, code in errors.items()]
        interval = [now.isoformat(), (now + timedelta(seconds=2)).isoformat()]
        contenders = [dict(status='COMMITTED', receipt_state='APPLIED', error=None,
                           transaction_interval=interval),
                      dict(status='NOT_COMMITTED', receipt_state=None,
                           error=dict(reason='concurrentUpdate', message='Transaction is aborted due to concurrent update'),
                           transaction_interval=interval)]
        return dict(case_group=group, cases=[dict(case=case, status='PASSED') for case in CASE_GROUPS[group]],
                    jobs=jobs, verified_native_error_codes=errors,
                    races=[dict(case='promotion_versus_claim', winner=0, jobs=contenders)]
                    if group in {'RACE', 'ALL'} else [])

    def test_complete_group_reports_pass_and_cannot_claim_all(self):
        for group in CASE_GROUPS:
            report = self.report(group)
            validate_native_report(report)
            if group != 'ALL':
                report['case_group'] = 'ALL'
                with self.assertRaises(RuntimeError):
                    validate_native_report(report)

    def test_native_errors_require_correlated_job_commit_and_no_invented_receipt(self):
        for change in ('missing', 'duplicate', 'message', 'reason', 'status', 'receipt', 'codes', 'inflated'):
            report = self.report('CORE')
            if change == 'missing':
                report['jobs'].pop()
            elif change == 'duplicate':
                report['jobs'].append(deepcopy(report['jobs'][0]))
            elif change == 'message':
                report['jobs'][0]['error']['message'] = 'unrelated syntax failure'
            elif change == 'reason':
                report['jobs'][0]['error']['reason'] = 'backendError'
            elif change == 'status':
                report['jobs'][1]['status'] = 'NOT_COMMITTED'
            elif change == 'receipt':
                report['jobs'][1]['receipt_state'] = 'APPLIED'
            elif change == 'codes':
                report['verified_native_error_codes'].clear()
            else:
                duplicate = deepcopy(report['jobs'][0])
                duplicate['job_id'] = 'extra'
                report['jobs'].append(duplicate)
                report['verified_native_error_codes']['extra'] = 'FIXTURE_ROLLBACK'
            with self.assertRaises(RuntimeError):
                validate_native_report(report)

    def test_denial_code_frequency_cannot_replace_one_gate_with_another(self):
        report = self.report('DENIALS')
        job_id = next(key for key, value in report['verified_native_error_codes'].items()
                      if value == 'DISPATCH_CONFLICT')
        report['verified_native_error_codes'][job_id] = 'RUNTIME_PROMPT_CONFLICT'
        job = next(job for job in report['jobs'] if job['job_id'] == job_id)
        job['error']['message'] = 'RUNTIME_PROMPT_CONFLICT'
        # Overall count and unique code set still match; per-gate frequency must not.
        self.assertEqual(len(report['verified_native_error_codes']), 6)
        with self.assertRaisesRegex(RuntimeError, 'INCOMPLETE_RUNTIME_DENIALS'):
            validate_native_report(report)

    def test_race_requires_actual_overlap_positive_rollback_and_exact_winner(self):
        for change in ('missing', 'overlap', 'unknown', 'winner', 'name'):
            report = self.report('RACE')
            race = report['races'][0]
            if change == 'missing':
                report['races'].clear()
            elif change == 'overlap':
                end = datetime.fromisoformat(race['jobs'][0]['transaction_interval'][1])
                race['jobs'][1]['transaction_interval'] = [
                    (end + timedelta(seconds=1)).isoformat(), (end + timedelta(seconds=2)).isoformat()]
            elif change == 'unknown':
                race['jobs'][1]['status'] = 'UNKNOWN'
            elif change == 'winner':
                race['winner'] = 1
            else:
                race['case'] = 'unrelated_promotion'
            with self.assertRaises(RuntimeError):
                validate_native_report(report)

    def test_invalid_run_group_denies_before_fixture_or_cloud_calls(self):
        probe = RuntimeProbe(self.client)
        probe.load = Mock()
        with self.assertRaisesRegex(RuntimeError, 'INVALID_RUNTIME_CASE_GROUP'):
            probe.run('unknown')
        probe.load.assert_not_called()
        self.client.query.assert_not_called()

    def test_snapshot_filters_every_partitioned_table_in_count_and_row_reads(self):
        probe = RuntimeProbe(self.client)
        probe.scope = self.scope
        row = {name: [] for name in TABLES}
        row.update({name + '_count': 0 for name in TABLES})
        probe.query = Mock(return_value=[row])
        self.assertEqual(probe.snapshot(), {name: [] for name in TABLES})
        script = probe.query.call_args.args[0]
        self.scope.assert_sql(script)
        for name, spec in TABLES.items():
            table = '`' + self.scope.table(name) + '` t'
            self.assertEqual(script.count(table), 2)
            if spec.partition_field:
                filtered = table + ' WHERE ' + spec.partition_field + " BETWEEN TIMESTAMP('0001-01-01')"
                self.assertEqual(script.count(filtered), 2, name)
                self.assertIn("TIMESTAMP('9999-12-31 23:59:59.999999+00')", script)
        self.assertEqual(script.count('LIMIT 1025'), len(TABLES))

    def test_snapshot_denies_truncation_overflow_and_count_mismatch(self):
        probe = RuntimeProbe(self.client)
        row = {name: [] for name in TABLES}
        row.update({name + '_count': 0 for name in TABLES})
        name = next(iter(TABLES))
        for count, values in ((1, []), (1025, ['row'] * 1025), (1026, ['row'] * 1025)):
            altered = deepcopy(row)
            altered[name + '_count'], altered[name] = count, values
            probe.query = Mock(return_value=[altered])
            with self.assertRaisesRegex(RuntimeError, 'FIXTURE_SNAPSHOT_OVERFLOW'):
                probe.snapshot()

    def test_denial_fixture_update_keeps_required_native_timeout_configuration(self):
        probe = RuntimeProbe(self.client)
        probe.scope = self.scope
        _, contexts = runtime_fixtures(scenarios=('expiry',))
        probe.snapshot = Mock(return_value={})
        probe.prepare = Mock(return_value=(Mock(), contexts['expiry']['resolved']))
        probe.query = Mock(side_effect=RuntimeError('STOP_BEFORE_CLOUD'))
        with self.assertRaisesRegex(RuntimeError, 'STOP_BEFORE_CLOUD'):
            probe.denials(contexts)
        script, config = probe.query.call_args.args
        self.scope.assert_sql(script)
        self.assertIn('UPDATE ', script)
        api = config.to_api_repr()
        self.assertIn('jobTimeoutMs', api)
        self.assertGreater(int(api['jobTimeoutMs']), 0)
        self.assertFalse(config.use_query_cache)
        self.assertFalse(config.create_session)
        self.assertIsNotNone(config.maximum_bytes_billed)

    def test_race_scoping_uses_probe_method_across_cli_module_boundary(self):
        from src.validation.prompt_runtime_race import run_race
        # The CLI has a __main__ RuntimeScope class distinct from the imported
        # class. Calling the probe-owned method keeps validation in its module.
        probe = Mock()
        raw = object()
        probe.prepare.return_value = (raw, object())
        probe.scoped_plan.side_effect = RuntimeError('STOP_BEFORE_CLOUD')
        with self.assertRaisesRegex(RuntimeError, 'STOP_BEFORE_CLOUD'):
            run_race(probe, {})
        probe.scoped_plan.assert_called_once_with(raw)
        probe.client.assert_not_called()

    def test_stored_binding_read_adapts_real_sdk_metadata_and_native_job_identity(self):
        from tests.test_prompt_lifecycle_store import native_job
        client = bigquery.Client(project=PROJECT, location=LOCATION,
                                 credentials=AnonymousCredentials())
        probe = RuntimeProbe(client)
        probe.scope, probe.own = self.scope, Mock()
        _, contexts = runtime_fixtures(scenarios=('core',))
        context = contexts['core']
        resolved = context['resolved']
        expected_row = dict(observed_at=datetime.now(timezone.utc), binding_count=1,
                            bindings=[resolved.binding.row()])
        owned_table = TABLES['invocation_prompt_bindings'].table(PROJECT)
        owned_table.expires=datetime.now(timezone.utc)+timedelta(hours=1)
        owned_table._properties.update(type='TABLE', location=LOCATION)
        owned_table._properties['tableReference'] = bigquery.TableReference.from_string(
            self.scope.table('invocation_prompt_bindings')).to_api_repr()
        jobs = []
        def query(script, **kwargs):
            self.scope.assert_sql(script)
            job = native_job(kwargs['job_id'], script, kwargs['job_config'])
            job.result = Mock(return_value=[expected_row])
            jobs.append(job)
            return job
        with patch.object(client, 'get_table', return_value=owned_table) as get_table, \
                patch.object(client, 'query', side_effect=query) as submit:
            actual = probe.binding_read(context, resolved)
        self.assertEqual(actual, expected_row)
        get_table.assert_called_once_with(self.scope.table('invocation_prompt_bindings'),
                                         retry=None, timeout=20)
        self.assertEqual(submit.call_args.kwargs['timeout'], 20)
        self.assertIsNone(submit.call_args.kwargs['retry'])
        self.assertIsNone(submit.call_args.kwargs['job_retry'])
        self.assertEqual(jobs[0].result.call_args.kwargs['timeout'], 60)
        client.close()

    def test_cleanup_checks_exact_owned_metadata_and_verifies_absence(self):
        probe = RuntimeProbe(self.client)
        probe.scope, probe.owned = self.scope, True
        self.client.get_dataset.return_value = Mock(full_dataset_id=PROJECT + ':ci_application')
        with self.assertRaises(RuntimeError):
            probe.cleanup()
        self.client.delete_dataset.assert_not_called()
        self.client.get_dataset.side_effect = [self.owned(), NotFound('absent')]
        probe.cleanup()
        self.client.delete_dataset.assert_called_once_with(self.scope.dataset,
            delete_contents=True, not_found_ok=False, retry=None, timeout=30)
        self.assertFalse(probe.owned)

    def test_load_has_exact_schema_and_bounded_retry_disabled_sdk_call(self):
        probe = RuntimeProbe(self.client)
        probe.scope, probe.own = self.scope, Mock()
        self.client.get_table.return_value = Mock(schema=TABLES['workspaces'].schema)
        self.client.load_table_from_json.return_value.error_result = None
        rows, _ = runtime_fixtures(scenarios=('core',))
        probe.load('workspaces', rows['workspaces'])
        call = self.client.load_table_from_json.call_args
        inspect.signature(bigquery.Client.load_table_from_json).bind(self.client, *call.args, **call.kwargs)
        self.assertEqual(call.kwargs['num_retries'], 0)
        self.assertEqual(call.kwargs['timeout'], 30)
        self.assertEqual(call.kwargs['job_config'].schema, list(TABLES['workspaces'].schema))
        self.assertEqual(call.args[1], self.scope.table('workspaces'))
        self.client.get_table.assert_called_once_with(self.scope.table('workspaces'), retry=None, timeout=30)

    def test_wrong_load_schema_denies_without_submitting(self):
        probe = RuntimeProbe(self.client)
        probe.scope, probe.own = self.scope, Mock()
        self.client.get_table.return_value = Mock(schema=TABLES['memberships'].schema)
        with self.assertRaises(RuntimeError):
            probe.load('workspaces', [dict(workspace_id='fixture')])
        self.client.load_table_from_json.assert_not_called()

    def test_query_requires_exact_native_job_and_disables_automatic_retry(self):
        from tests.test_prompt_lifecycle_store import native_job
        probe = RuntimeProbe(self.client)
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
        self.client.query.side_effect = None
        wrong = native_job(kwargs['job_id'], 'wrong SQL', kwargs['job_config'])
        wrong.result = Mock()
        self.client.query.return_value = wrong
        with self.assertRaises(RuntimeError):
            probe.query(script)
        wrong.result.assert_not_called()

    def test_incomplete_duplicate_failed_and_unknown_group_reports_deny(self):
        for group in ('CORE', 'DENIALS', 'RACE', 'ALL'):
            report = dict(case_group=group, cases=[], jobs=[], verified_native_error_codes={}, races=[])
            with self.assertRaises(RuntimeError):
                validate_native_report(report)
            report['cases'] = [dict(case=case, status='FAILED') for case in CASE_GROUPS[group]]
            with self.assertRaises(RuntimeError):
                validate_native_report(report)
            report['cases'] = [dict(case=case, status='PASSED') for case in CASE_GROUPS[group]]
            report['cases'].append(deepcopy(report['cases'][0]))
            with self.assertRaises(RuntimeError):
                validate_native_report(report)
        with self.assertRaises(RuntimeError):
            validate_native_report(dict(case_group='unknown'))


class RuntimeFixtureGuards(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.rows, self.contexts = runtime_fixtures(self.now)

    def test_independent_scenarios_preserve_active_and_evaluated_candidates(self):
        self.assertEqual(set(self.contexts), set(SCENARIOS))
        self.assertEqual(len({c['key'] for c in self.contexts.values()}), len(SCENARIOS))
        for context in self.contexts.values():
            active, candidate = context['states']
            self.assertIs(active.head.status, PromptLifecycle.ACTIVE)
            self.assertIs(candidate.head.status, PromptLifecycle.EVALUATED)
            payload = json.loads(context['resolved'].binding.binding_canonical_json)
            self.assertEqual(payload['eligibility']['snapshot_sha256'], active.head.snapshot.snapshot_sha256)
            self.assertEqual(context['resolved'].binding.invocation_id, context['invocation']['invocation_id'])
            self.assertNotEqual(context['workspace'], context['other'])
            self.assertEqual(set(context['evaluation_controls']), {context['workspace'], context['other']})
        self.assertEqual(self.rows['invocation_prompt_bindings'], [])

    def test_explicit_scenario_selection_and_invalid_selection_fail_closed(self):
        rows, contexts = runtime_fixtures(self.now, scenarios=('core',))
        self.assertEqual(set(contexts), {'core'})
        self.assertEqual(len(rows['prompt_registry']), 2)
        for scenarios in ((), ['core'], ('core', 'core'), ('../serving',), (1,)):
            with self.assertRaises(ContractError):
                runtime_fixtures(self.now, scenarios=scenarios)

    def test_dynamic_denial_scenarios_have_exact_valid_binding_scope(self):
        names = ('stored', 'binding_expiry', 'stale')
        rows, contexts = runtime_fixtures(self.now, scenarios=names)
        self.assertEqual(set(contexts), set(names))
        for name, context in contexts.items():
            binding = context['resolved'].binding
            self.assertEqual(binding.workspace_id, 'fixture-runtime-' + name)
            self.assertEqual(binding.invocation_id, context['invocation']['invocation_id'])
            self.assertEqual(binding.prompt_key, context['key'])
        self.assertEqual(rows['invocation_prompt_bindings'], [])


if __name__ == '__main__':
    unittest.main()
