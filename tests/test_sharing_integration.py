"""Offline probe isolation, cleanup and fault-boundary tests; not cloud races."""
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from google.cloud import bigquery

from src.application.conversation_contract import validate_conversation
from src.application.membership_contract import validate_membership
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_executor import SharingExecutorSettings
from src.application.sharing_jobs import failed_sharing_job_status
from src.application.workspace_contract import validate_workspace
from src.application.workflow_contract import ContractError
from src.validation.sharing_integration import (
    CASES, FIXTURE_TABLES, TTL_MS, IntegrationError, NativeSharingProbe, ProbeScope,
    fault_variant, fixture_rows, sharing_query, run_probe,
)
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT
from tests.test_sharing_executor import child, native_job
from tests.test_sharing_query import query
from src.application.sharing_intent import initial_attempt
from src.application.sharing_receipt import SharingReceiptQuery


class SharingIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.scope = ProbeScope('a'*32)
        self.client = Mock(project=PROJECT, default_query_job_config=None)
        self.probe = NativeSharingProbe(self.client, self.scope)
        resource = bigquery.Dataset(self.scope.dataset)
        resource.location = LOCATION
        resource.labels = self.scope.labels
        resource.default_table_expiration_ms = TTL_MS
        self.resource = resource
        self.client.get_dataset.return_value = resource

    def test_scope_is_fresh_uuid_namespace_and_only_four_fixture_tables(self):
        self.assertEqual(self.scope.dataset_id, 'ci_sharing_it_'+'a'*32)
        for value in ('', 'ci_application', 'A'*32, 'a'*31, 'a'*33, 'a'*31+'`'):
            with self.assertRaises(IntegrationError):
                ProbeScope(value)
        for name in ('product_master', 'jobs', 'workspaces`', 'other'):
            with self.assertRaises(IntegrationError):
                self.scope.table(name)

    def test_relocation_rewrites_all_four_targets_without_modifying_production_query(self):
        q = query()
        original = q.sql
        isolated = self.scope.relocate(original)
        self.assertEqual(q.sql, original)
        self.assertNotIn('.ci_application.', isolated)
        for name in FIXTURE_TABLES:
            self.assertIn(self.scope.table(name), isolated)
        self.assertFalse(SharingExecutorSettings().execution_enabled)

    def test_foreign_project_dataset_or_extra_reference_is_rejected(self):
        sql = self.scope.relocate(query().sql)
        for extra in ('project.other.table', f'{PROJECT}.ci_product.product_master',
                      f'{PROJECT}.ci_application.workspaces', 'foreign.'+self.scope.dataset_id+'.workspaces'):
            with self.assertRaises(IntegrationError):
                self.scope.assert_sql(sql + f';SELECT * FROM `{extra}`')
        with self.assertRaises(IntegrationError):
            self.scope.assert_sql('SELECT 1')

    def test_receipt_select_uses_same_isolation_guard_without_changing_original_intent(self):
        intent = query()
        receipt = SharingReceiptQuery(intent)
        fingerprint = initial_attempt(intent).fingerprint
        isolated = self.scope.relocate(receipt.sql)
        self.assertNotIn('.ci_application.', isolated)
        self.assertNotRegex(isolated, r'\b(UPDATE|INSERT|DELETE|BEGIN|COMMIT)\b')
        self.assertEqual(initial_attempt(intent).fingerprint, fingerprint)
        for name in FIXTURE_TABLES:
            self.assertIn(self.scope.table(name), isolated)

    def test_synthetic_control_rows_validate_without_business_product_ids(self):
        for case in CASES:
            rows = fixture_rows(case, datetime.now(timezone.utc))
            workspace_id = rows['workspaces']['workspace_id']
            validate_workspace(rows['workspaces'], workspace_id=workspace_id)
            validate_membership(rows['memberships'], workspace_id=workspace_id,
                                principal_id=rows['memberships']['principal_id'])
            validate_conversation(rows['conversations'], workspace_id=workspace_id,
                                  conversation_id=rows['conversations']['conversation_id'])
            self.assertNotIn('canonical_sku', rows['conversations'])

    def test_fault_injection_stays_after_fence_update_or_after_commit(self):
        sql = self.scope.relocate(query().sql)
        rollback = fault_variant(sql, 'rollback')
        self.assertTrue(rollback.index('UPDATE ') < rollback.index("ASSERT FALSE AS 'INTEGRATION_ROLLBACK'")
                        < rollback.index('INSERT INTO '))
        committed = fault_variant(sql, 'postcommit')
        self.assertTrue(committed.index('COMMIT TRANSACTION;') < committed.index("ASSERT FALSE AS 'INTEGRATION_POSTCOMMIT'"))
        self.scope.assert_sql(rollback)
        self.scope.assert_sql(committed)
        with self.assertRaises(IntegrationError):
            fault_variant(sql, 'unrecognized')

    def test_cleanup_never_deletes_without_own_creation(self):
        self.probe.cleanup()
        self.client.get_dataset.assert_not_called()
        self.client.delete_dataset.assert_not_called()

    def test_cleanup_requires_matching_scope_region_labels_and_expiry(self):
        self.probe.owned = True
        for changes in (dict(location='us'), dict(labels={'purpose':'other'}),
                        dict(default_table_expiration_ms=None)):
            resource = deepcopy(self.resource)
            for key, value in changes.items():
                setattr(resource, key, value)
            self.client.get_dataset.return_value = resource
            with self.assertRaises(IntegrationError):
                self.probe.cleanup()
        self.client.delete_dataset.assert_not_called()

    def test_unexpected_table_or_unresolved_job_prevents_dataset_deletion(self):
        self.probe.owned = True
        self.client.list_tables.return_value = [SimpleNamespace(table_id='unrelated')]
        with self.assertRaises(IntegrationError):
            self.probe.cleanup()
        self.client.list_tables.return_value = []
        self.probe.jobs = ['pending-job']
        self.client.get_job.return_value = SimpleNamespace(state='RUNNING')
        with self.assertRaises(IntegrationError):
            self.probe.cleanup()
        self.client.delete_dataset.assert_not_called()

    def test_cleanup_only_deletes_owned_fixture_dataset_after_terminal_jobs(self):
        self.probe.owned = True
        self.client.list_tables.return_value = [SimpleNamespace(table_id=n) for n in FIXTURE_TABLES]
        self.probe.jobs = ['terminal-job']
        self.client.get_job.return_value = SimpleNamespace(state='DONE')
        self.probe.cleanup()
        self.client.delete_dataset.assert_called_once_with(self.scope.dataset, delete_contents=True,
                                                          not_found_ok=False, retry=None, timeout=10)
        self.assertEqual(self.probe.report['cleanup'], 'DELETED_OWNED_DATASET')

    def test_unrelated_sql_is_rejected_before_submission(self):
        with self.assertRaises(IntegrationError):
            self.probe.execute(f'DELETE FROM `{PROJECT}.ci_application.workspaces` WHERE TRUE')
        self.client.query.assert_not_called()

    def test_failed_native_classifier_requires_failed_terminal_correlated_parent(self):
        attempt = initial_attempt(query())
        self.client.list_jobs.return_value = [child(attempt, 'ROLLBACK_TRANSACTION')]
        for parent in (native_job(attempt), native_job(attempt, state='RUNNING', error={'reason':'stopped'})):
            self.assertEqual(failed_sharing_job_status(self.client, job_id=attempt.job_id,
                timeout_seconds=5, parent=parent), SharingCommitStatus.UNKNOWN)
        self.client.list_jobs.assert_not_called()

    def test_query_construction_preserves_opaque_fixture_scope_and_revision_pins(self):
        q = sharing_query('stale', membership_revision=2)
        self.assertEqual(q.command.workspace_id, 'it_stale')
        self.assertEqual(q.command.expected_membership_revision, 2)
        self.assertEqual(q.expected_workspace_revision, 1)

    def test_failure_report_preserves_safe_contract_code_without_exception_content(self):
        for error, code in ((ContractError('SHARING_COMMIT_SCOPE_MISMATCH'), 'SHARING_COMMIT_SCOPE_MISMATCH'),
                            (RuntimeError('sensitive transport details'), 'RuntimeError')):
            with TemporaryDirectory() as directory, patch.object(NativeSharingProbe, 'setup', side_effect=error):
                report = run_probe(self.client, Path(directory))
                self.assertEqual(report['status'], 'FAIL')
                self.assertEqual(report['error_code'], code)
                self.assertEqual(report['cleanup'], 'NOT_CREATED')
                self.assertNotIn('sensitive transport details', Path(report['report_path']).read_text())
        self.client.delete_dataset.assert_not_called()

    def test_race_interval_requires_correlated_terminal_native_children(self):
        attempt = initial_attempt(query())
        valid = child(attempt, 'BEGIN_TRANSACTION')
        for other in (child(attempt, 'COMMIT_TRANSACTION', projectId='foreign-project'),
                      child(attempt, 'COMMIT_TRANSACTION', state='RUNNING')):
            self.client.list_jobs.return_value = [valid, other]
            with self.assertRaises(IntegrationError):
                self.probe._transaction_interval(attempt.job_id)

    def test_race_interval_requires_one_begin_and_one_end_with_native_timestamps(self):
        attempt = initial_attempt(query())
        begin, commit = child(attempt, 'BEGIN_TRANSACTION'), child(attempt, 'COMMIT_TRANSACTION')
        begin._properties['statistics']['endTime'] = 1000
        commit._properties['statistics']['startTime'] = 2000
        self.client.list_jobs.return_value = [begin, commit]
        self.assertEqual(self.probe._transaction_interval(attempt.job_id),
                         (datetime.fromtimestamp(1, timezone.utc), datetime.fromtimestamp(2, timezone.utc)))
        for children in ([begin], [begin, commit, commit]):
            self.client.list_jobs.return_value = children
            self.assertIsNone(self.probe._transaction_interval(attempt.job_id))


if __name__ == '__main__':
    unittest.main()
