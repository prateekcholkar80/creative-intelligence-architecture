"""Retry probe scope/mode/proof guards; actual races require the native run."""
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
import unittest

from google.api_core.exceptions import NotFound

from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_intent import initial_attempt
from src.application.sharing_query import SharingQueryResult, SharingResultState
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT
from src.validation.sharing_integration import (
    IntegrationError, NativeRetryChecks, NativeSharingProbe, ProbeScope,
    fixture_rows, main, run_probe, sharing_query,
)
from tests.test_sharing_executor import child, native_job


class NativeRetryProbeTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock(project=PROJECT, default_query_job_config=None)
        self.probe = NativeSharingProbe(self.client, ProbeScope('b'*32))
        self.checks = NativeRetryChecks(self.probe)
        self.original = initial_attempt(sharing_query('rollback'))

    def parent(self, *, children=None):
        sql = self.checks.register(self.original)
        job = native_job(self.original, error={'reason': 'invalidQuery'})
        job._properties['configuration']['query']['query'] = sql
        self.client.get_job.return_value = job
        self.client.list_jobs.return_value = (children if children is not None else
            [child(self.original, 'ROLLBACK_TRANSACTION')])
        return job

    def context(self, *, receipt=False):
        at = datetime.now(timezone.utc)
        rows = fixture_rows('rollback', at-timedelta(minutes=1))
        rows['workspaces']['revision'] = 2
        row = dict(observed_at=at, **{name: [value] for name, value in rows.items()}, audit_events=[])
        result = SharingQueryResult(SharingResultState.REUSED if receipt else SharingResultState.UNKNOWN,
            self.original.query.command.audit_id, at, 2 if receipt else None)
        return row, result

    def test_modes_are_exclusive_and_validated_before_resources(self):
        with TemporaryDirectory() as directory:
            output = Path(directory)/'not_created'
            for options in (dict(receipt_only=True, retry_only=True), dict(retry_only='yes')):
                with self.assertRaises(IntegrationError):
                    run_probe(self.client, output, **options)
            self.assertFalse(output.exists())
        self.client.create_dataset.assert_not_called()

    def test_cli_rejects_both_modes_before_constructing_client(self):
        with patch('sys.argv', ['probe', '--run', '--output-dir', '/tmp/unused',
                                '--receipt-only', '--retry-only']), \
             patch('sys.stderr', new=StringIO()), \
             patch('src.validation.sharing_integration.bigquery.Client') as client:
            with self.assertRaises(SystemExit) as caught:
                main()
            self.assertEqual(caught.exception.code, 2)
            client.assert_not_called()

    def test_retry_mode_runs_only_retry_cases_and_keeps_report_safe(self):
        with TemporaryDirectory() as directory, \
             patch.object(NativeSharingProbe, 'setup'), patch.object(NativeSharingProbe, 'cleanup'), \
             patch.object(NativeSharingProbe, 'cases') as transaction, \
             patch.object(NativeSharingProbe, 'receipt_cases') as receipt, \
             patch.object(NativeRetryChecks, 'cases') as retry:
            report = run_probe(self.client, Path(directory), retry_only=True)
            self.assertEqual((report['mode'], report['status']), ('RETRY_ONLY', 'PASS'))
            self.assertEqual(report['serving_data_writes'], 0)
            self.assertFalse(report['execution_activated'])
            retry.assert_called_once()
            transaction.assert_not_called()
            receipt.assert_not_called()
            saved = Path(report['report_path']).read_text()
            self.assertNotIn('share1.', saved)
            self.assertNotIn('signing_key', saved)

    def test_sealed_id_tracks_once_and_pins_only_owned_sql(self):
        sql = self.checks.register(self.original)
        self.assertEqual(self.checks.register(self.original), sql)
        self.assertEqual(self.probe.jobs, [self.original.job_id])
        self.assertNotIn('.ci_application.', sql)
        self.probe.scope.assert_sql(sql)
        self.assertEqual(self.probe.report['sealed_attempts'][0]['contract_fingerprint'],
                         self.original.fingerprint)
        self.client.query.assert_not_called()

    def test_fault_cannot_rebind_an_already_allocated_job(self):
        self.checks.register(self.original)
        with self.assertRaises(IntegrationError) as caught:
            self.checks.register(self.original, fault='postcommit')
        self.assertEqual(caught.exception.code, 'PROBE_ATTEMPT_SQL_CONFLICT')
        self.assertEqual(self.probe.jobs, [self.original.job_id])
        self.client.query.assert_not_called()

    def test_forged_id_and_foreign_fixture_identity_do_not_allocate_jobs(self):
        object.__setattr__(self.original, 'job_id', 'forged-id')
        with self.assertRaises(IntegrationError):
            self.checks.register(self.original)
        foreign = initial_attempt(sharing_query('outside-fixtures'))
        with self.assertRaises(IntegrationError):
            self.checks.register(foreign)
        self.assertEqual(self.probe.jobs, [])
        self.client.query.assert_not_called()

    def test_missing_native_metadata_is_unknown_without_allocating_a_job(self):
        self.client.get_job.side_effect = NotFound('missing private metadata')
        observed = self.checks.observation(self.original)
        self.assertIs(observed.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(observed.query_job_id, self.original.job_id)
        self.client.get_job.assert_called_once()
        self.assertEqual(self.probe.jobs, [])
        self.client.query.assert_not_called()

    def test_serving_sql_cannot_supply_proof_to_the_isolated_binding(self):
        self.checks.register(self.original)
        self.client.get_job.return_value = native_job(self.original, error={'reason':'invalidQuery'})
        self.client.list_jobs.return_value = [child(self.original, 'ROLLBACK_TRANSACTION')]
        self.assertIs(self.checks.observation(self.original).status, SharingCommitStatus.UNKNOWN)
        self.client.list_jobs.assert_not_called()

    def test_only_matching_isolated_parent_and_positive_children_prove_noncommit(self):
        self.parent()
        self.assertIs(self.checks.observation(self.original).status, SharingCommitStatus.NOT_COMMITTED)
        self.parent(children=[child(self.original, 'ROLLBACK_TRANSACTION', projectId='other')])
        self.assertIs(self.checks.observation(self.original).status, SharingCommitStatus.UNKNOWN)
        self.parent(children=[child(self.original, 'COMMIT_TRANSACTION'),
                              child(self.original, 'ROLLBACK_TRANSACTION')])
        self.assertIs(self.checks.observation(self.original).status, SharingCommitStatus.UNKNOWN)

    def test_positive_proof_and_current_context_preserve_original_pins(self):
        self.parent()
        with patch.object(self.checks, 'context', return_value=self.context()):
            state, attempt, _ = self.checks.prepare(self.original)
        self.assertEqual(state, 'READY')
        self.assertEqual(attempt.query.command, self.original.query.command)
        self.assertEqual(attempt.query.expected_workspace_revision, 2)
        self.assertEqual(attempt.steps[0].predecessor_observation.query_job_id, self.original.job_id)
        self.assertNotEqual(attempt.job_id, self.original.job_id)
        self.client.query.assert_not_called()

    def test_unknown_predecessor_plus_absence_never_seals_or_allocates_retry(self):
        self.client.get_job.side_effect = NotFound('unknown')
        with patch.object(self.checks, 'context', return_value=self.context()), \
             patch.object(self.checks.sealer, 'seal_retry') as seal:
            state, attempt, _ = self.checks.prepare(self.original)
        self.assertEqual(state, 'RECOVERY_REQUIRED')
        self.assertIsNone(attempt)
        seal.assert_not_called()
        self.assertEqual(self.probe.jobs, [])

    def test_receipt_wins_without_classifying_a_missing_native_job(self):
        self.client.get_job.side_effect = NotFound('unknown')
        with patch.object(self.checks, 'context', return_value=self.context(receipt=True)), \
             patch.object(self.checks.sealer, 'seal_retry') as seal:
            state, attempt, result = self.checks.prepare(self.original)
        self.assertEqual(state, 'REUSED')
        self.assertIsNone(attempt)
        self.assertEqual(result.resulting_revision, 2)
        seal.assert_not_called()

    def test_stale_business_pin_is_not_refreshed(self):
        self.parent()
        row, result = self.context()
        row['memberships'][0]['revision'] = 2
        with patch.object(self.checks, 'context', return_value=(row, result)), \
             patch.object(self.checks.sealer, 'seal_retry') as seal:
            with self.assertRaises(ContractError) as caught:
                self.checks.prepare(self.original)
        self.assertEqual(caught.exception.code, 'STALE_MEMBERSHIP_REVISION')
        seal.assert_not_called()

    def test_new_native_ids_remain_in_existing_terminal_cleanup_checks(self):
        self.checks.register(self.original)
        self.probe.owned = True
        resource = Mock(project=PROJECT, dataset_id=self.probe.scope.dataset_id,
            location='asia-south1', labels=self.probe.scope.labels, default_table_expiration_ms=7200000)
        self.client.get_dataset.return_value = resource
        self.client.list_tables.return_value = []
        self.client.get_job.return_value = Mock(state='RUNNING')
        with self.assertRaises(IntegrationError) as caught:
            self.probe.cleanup()
        self.assertEqual(caught.exception.code, 'CLEANUP_BLOCKED_BY_UNRESOLVED_JOB')
        self.client.delete_dataset.assert_not_called()


if __name__ == '__main__':
    unittest.main()
