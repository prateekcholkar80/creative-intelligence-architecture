"""Offline containment and recovery guards for the explicit native Batch 14 probe."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import re
import inspect
import unittest
from unittest.mock import Mock

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.prompt_evaluation_storage import decode_evaluation, encode_evaluation
from src.application.prompt_evaluation_store import BigQueryPromptEvaluationStore, read_sql
from src.application.workflow_contract import ContractError
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT
from src.validation.prompt_evaluation_fixtures import fixtures
from src.validation.prompt_evaluation_integration import (
    FixtureStore, Probe, Scope, ScopedWrite, TABLES, validate_native_errors,
)
from src.validation.signing_read_integration import TTL_MS


class EvaluationIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.scope = Scope('a' * 32)
        self.seed, self.registry, self.control, self.report = fixtures()
        self.client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)

    def owned(self):
        return Mock(full_dataset_id=f'{PROJECT}:{self.scope.dataset.split(".")[1]}',
            location=LOCATION, labels=self.scope.labels, default_table_expiration_ms=TTL_MS)

    def test_synthetic_observations_roundtrip_all_required_categories(self):
        row, cases = encode_evaluation(self.control.control, self.report.record)
        self.assertEqual(len(cases), 10)
        self.assertEqual(decode_evaluation(self.control.control, row, cases, self.registry), self.report.record)
        self.assertTrue(self.report.record.evaluator_version.startswith('fixture-'))
        self.assertEqual(len(self.seed['workspaces']), 2)

    def test_relocation_covers_write_and_authoritative_reader_without_changing_config(self):
        for base in (self.control, self.report):
            plan = ScopedWrite(base, self.scope, 'apply')
            self.assertEqual(plan.job_config().to_api_repr(), base.job_config().to_api_repr())
            self.assertEqual(plan.digest, base.digest)
            self.assertTrue(all(ref.startswith(self.scope.dataset + '.') for ref in re.findall(r'`([^`]+)`', plan.sql)))
        self.scope.relocate(read_sql())
        self.assertEqual(len(TABLES), 8)

    def test_foreign_references_unknown_tables_and_unrelocated_source_denied(self):
        for sql in (self.control.sql, self.control.sql + '\nSELECT * FROM `other.other.foreign`;',
                    'SELECT 1', 'SELECT * FROM project.ci_application.workspaces'):
            with self.assertRaises(RuntimeError):
                self.scope.assert_sql(sql)
        with self.assertRaises(RuntimeError):
            self.scope.table('requests')
        with self.assertRaises(RuntimeError):
            Scope('../serving')

    def test_faults_are_bounded_and_original_job_slots_are_stable(self):
        for fault, text in (('rollback', 'FIXTURE_ROLLBACK'), ('postcommit', 'FIXTURE_AFTER_COMMIT')):
            plan = ScopedWrite(self.report, self.scope, fault, fault)
            self.assertIn(text, plan.sql)
            self.scope.assert_sql(plan.sql)
        first = ScopedWrite(self.report, self.scope, 'apply')
        self.assertEqual(first.job_id, ScopedWrite(self.report, self.scope, 'apply').job_id)
        for case, fault in (('../bad', None), ('bad', 'unsupported')):
            with self.assertRaises(RuntimeError):
                ScopedWrite(self.report, self.scope, case, fault)

    def test_ownership_rejects_changed_labels_location_name_and_expiry(self):
        self.scope.own(self.owned())
        for key, value in (('labels', {}), ('location', 'us'), ('full_dataset_id', PROJECT + ':ci_application'),
                           ('default_table_expiration_ms', None)):
            actual = self.owned()
            setattr(actual, key, value)
            with self.assertRaises(RuntimeError):
                self.scope.own(actual)

    def test_fixture_preflight_never_calls_production_preflight(self):
        probe = Mock(client=self.client)
        store = FixtureStore(probe)
        store._preflight()
        probe.preflight.assert_called_once_with()
        self.client.get_table.assert_not_called()
        with self.assertRaises(ContractError):
            BigQueryPromptEvaluationStore(self.client).submit(self.control)
        self.client.query.assert_not_called()

    def test_cleanup_uncreated_or_foreign_dataset_never_deletes(self):
        probe = Probe(self.client)
        probe.scope = self.scope
        probe.cleanup()
        probe.owned = True
        self.client.get_dataset.return_value = Mock(full_dataset_id=PROJECT + ':ci_application')
        with self.assertRaises(RuntimeError):
            probe.cleanup()
        self.client.delete_dataset.assert_not_called()

    def test_cleanup_requires_exact_ownership_and_verified_absence(self):
        probe = Probe(self.client)
        probe.scope, probe.owned = self.scope, True
        self.client.get_dataset.side_effect = [self.owned(), NotFound('absent')]
        probe.cleanup()
        self.client.delete_dataset.assert_called_once_with(self.scope.dataset, delete_contents=True,
            not_found_ok=False, retry=None, timeout=10)
        self.assertFalse(probe.owned)
        self.assertEqual(probe.report['cleanup'], 'DELETED_AND_ABSENCE_VERIFIED')

    def test_known_job_mismatch_is_unknown_without_second_submission(self):
        store = FixtureStore(Mock(client=self.client))
        plan = ScopedWrite(self.report, self.scope, 'original')
        job = Mock(job_id=plan.job_id, project=PROJECT, location=LOCATION, query='different')
        self.client.get_job.return_value = job
        self.assertIs(store.submit(plan).native_status, Status.UNKNOWN)
        self.client.query.assert_not_called()

    def test_changed_table_schema_blocks_fixture_load(self):
        probe = Probe(self.client)
        probe.scope = self.scope
        self.client.get_dataset.return_value = self.owned()
        table = TABLES['prompt_evaluation_controls'].table(PROJECT)
        self.client.get_table.return_value = table
        with self.assertRaises(RuntimeError):
            probe.load('prompt_evaluation_controls', [])
        self.client.load_table_from_json.assert_not_called()

    def test_load_matches_installed_sdk_and_uses_explicit_schema_without_upload_retry(self):
        probe = Probe(self.client)
        probe.scope = self.scope
        probe.preflight = Mock()
        self.client.load_table_from_json.return_value.error_result = None
        probe.load('workspaces', self.seed['workspaces'])
        call = self.client.load_table_from_json.call_args
        inspect.signature(bigquery.Client.load_table_from_json).bind(self.client, *call.args, **call.kwargs)
        self.assertEqual(call.kwargs['num_retries'], 0)
        self.assertEqual(call.kwargs['job_config'].schema, list(TABLES['workspaces'].schema))
        self.assertEqual(call.args[1], self.scope.table('workspaces'))

    def test_unparameterized_fixture_query_has_explicit_empty_parameter_contract(self):
        probe = Probe(self.client)
        probe.scope, probe.own = self.scope, Mock()
        sql = f'SELECT COUNT(*) AS n FROM `{self.scope.table("workspaces")}`'
        def query(text, **kwargs):
            job = bigquery.QueryJob(bigquery.job._JobReference(kwargs['job_id'], PROJECT, LOCATION),
                text, self.client, job_config=kwargs['job_config'])
            job._properties['status'] = {'state': 'DONE'}
            job.result = Mock(return_value=[{'n': 1}])
            return job
        self.client.query.side_effect = query
        self.assertEqual(probe.query(sql), [{'n': 1}])
        self.assertEqual(self.client.query.call_args.kwargs['job_config'].to_api_repr()['query']['queryParameters'], [])

    def test_expired_report_is_valid_supplied_record_but_not_current(self):
        _, _, _, plan = fixtures(datetime(2026, 10, 13, tzinfo=timezone.utc))
        record = replace(plan.record, valid_until=plan.record.completed_at + timedelta(seconds=1))
        self.assertLess(record.valid_until, datetime(2026, 10, 13, tzinfo=timezone.utc))
        expired = replace(plan, record=record)
        self.assertIn('EVALUATION_EXPIRED', ScopedWrite(expired, self.scope, 'expired').sql)

    def test_native_denial_requires_specific_assertion_not_incidental_rollback(self):
        expected = dict(report_conflict='EVALUATION_REPLAY_CONFLICT', rollback='FIXTURE_ROLLBACK',
            postcommit='FIXTURE_AFTER_COMMIT', expired_report='EVALUATION_EXPIRED',
            expired_identity='ASSERTION_EXPIRED', stale_control='EVALUATION_CONTROL_STALE',
            revoked_report_reuse='MEMBERSHIP_DENIED')
        report = {'jobs': [dict(job_id='fixture_' + case,
            status='COMMITTED' if case == 'postcommit' else 'NOT_COMMITTED', receipt_state=None,
            error={'reason': 'invalidQuery', 'message': 'Query error: ' + code})
            for case, code in expected.items()]}
        self.assertEqual(validate_native_errors(report), expected)
        report['jobs'][0]['error']['message'] = 'Syntax error'
        with self.assertRaises(RuntimeError):
            validate_native_errors(report)
        report['jobs'] = report['jobs'][1:]
        with self.assertRaises(RuntimeError):
            validate_native_errors(report)


if __name__ == '__main__':
    unittest.main()
