"""Offline containment tests for the opt-in Priority 4.6 native probe."""
import re
import unittest
from unittest.mock import Mock

from src.application.workflow_failure_query import validate_workflow_failure_result
from src.application.workflow_schema import TABLES
from src.validation.workflow_writer_integration import Scope, failure_fixture, scoped_plan


class WorkflowWriterIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.scope = Scope('a' * 32)
        self.rows, self.plan = failure_fixture('fixture-native-workspace')

    def test_fixture_uses_destination_rows_and_validated_plan(self):
        self.assertEqual(set(self.rows), {'workspaces', 'memberships', 'conversations',
            'requests', 'jobs', 'job_attempts', 'model_invocations', 'stage_events'})
        self.assertEqual(len(self.rows['stage_events']), 1)
        self.assertEqual(self.plan.config().maximum_bytes_billed, 512 * 1024**2)
        self.assertEqual(self.plan.config().script_options.statement_byte_budget, 512 * 1024**2)
        self.assertEqual(validate_workflow_failure_result(self.plan, {
            'state': 'APPLIED', 'job_row': self.plan.job_json,
            'attempt_row': self.plan.attempt_json,
            'event_rows': list(self.plan.event_rows_json),
            'audit_rows': list(self.plan.audit_rows_json)}), 'APPLIED')

    def test_relocation_contains_every_reference_in_owned_dataset(self):
        actual = scoped_plan(self.scope, self.plan)
        references = re.findall(r'`([^`]+)`', actual.sql)
        self.assertTrue(references)
        self.assertTrue(all(value.startswith(self.scope.dataset + '.') for value in references))
        self.assertNotIn('.ci_application.', actual.sql)

    def test_faults_are_scoped_and_have_distinct_exact_job_metadata(self):
        for fault, marker in (('rollback', 'FIXTURE_ROLLBACK'), ('postcommit', 'FIXTURE_POSTCOMMIT')):
            with self.subTest(fault=fault):
                actual = scoped_plan(self.scope, self.plan,
                    native_job_id='fixture-' + fault, fault=fault)
                self.assertEqual(actual.native_job_id, 'fixture-' + fault)
                self.assertIn(marker, actual.sql)
                self.assertEqual(actual.config_json, self.plan.config_json)

    def test_scope_rejects_unknown_tables_and_foreign_references(self):
        with self.assertRaises(RuntimeError):
            self.scope.table('unknown')
        with self.assertRaises(RuntimeError):
            self.scope.sql('SELECT * FROM `other-project.other_dataset.other_table`')

    def test_scope_requires_exact_owned_dataset_metadata(self):
        dataset = Mock(full_dataset_id=f'{self.scope.dataset.split(".")[0]}:'
            f'{self.scope.dataset.split(".")[1]}', location='asia-south1',
            labels=self.scope.labels, default_table_expiration_ms=2 * 60 * 60 * 1000)
        self.scope.own(dataset)
        dataset.labels = {'purpose': 'other'}
        with self.assertRaises(RuntimeError):
            self.scope.own(dataset)

    def test_all_production_tables_are_relocation_targets(self):
        for name in TABLES:
            self.assertEqual(self.scope.table(name), f'{self.scope.dataset}.{name}')


if __name__ == '__main__':
    unittest.main()
