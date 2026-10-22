"""Offline containment tests for the opt-in prompt-registry native probe."""
import re
import unittest
from unittest.mock import Mock

from google.api_core.exceptions import NotFound

from src.application.prompt_registry_query import (
    validate_prompt_registration_result,
)
from src.application.prompt_registry_schema import TABLES
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT
from src.validation.prompt_registry_integration import (
    FixtureExecutor,
    Probe,
    Scope,
    fixture_query,
    scoped_plan,
)
from src.validation.signing_read_integration import TTL_MS


class PromptRegistryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.scope = Scope('a' * 32)
        self.base = fixture_query()

    def test_fixture_is_a_valid_initial_registration_plan(self):
        current, history = self.base.rows()
        self.assertEqual(current['status'], 'DRAFT')
        self.assertEqual(history['revision'], 1)
        self.assertEqual(current['snapshot_sha256'],
                         history['snapshot_sha256'])
        row = {
            'state': 'APPLIED',
            'observed_at': self.base.state.head.changed_at,
            'prompt_key': self.base.state.head.snapshot.prompt_key,
            'snapshot_sha256': self.base.state.head.snapshot.snapshot_sha256,
            'record_digest': self.base.state.head.digest,
        }
        self.assertEqual(
            validate_prompt_registration_result(self.base, [row]).state.value,
            'APPLIED')

    def test_relocation_contains_every_reference_in_owned_dataset(self):
        plan = scoped_plan(self.scope, self.base, case='apply')
        references = re.findall(r'`([^`]+)`', plan.sql)
        self.assertTrue(references)
        self.assertTrue(all(
            value.startswith(self.scope.dataset + '.')
            for value in references))
        self.assertNotIn('.ci_application.', plan.sql)
        self.assertEqual(plan.state, self.base.state)
        self.assertEqual(plan.job_config().to_api_repr(),
                         self.base.job_config().to_api_repr())

    def test_fault_is_scoped_and_job_slots_are_explicit(self):
        first = scoped_plan(
            self.scope, self.base, case='rollback', fault='rollback')
        second = scoped_plan(self.scope, self.base, case='reuse')
        self.assertIn('FIXTURE_ROLLBACK', first.sql)
        self.assertNotEqual(first.job_id, second.job_id)
        self.assertRegex(
            first.job_id,
            r'prompt_registry_it_[0-9a-f]{32}_[a-z0-9_]+')
        with self.assertRaises(RuntimeError):
            scoped_plan(self.scope, self.base, case='bad', fault='postcommit')

    def test_scope_rejects_foreign_references_and_unknown_tables(self):
        with self.assertRaises(RuntimeError):
            self.scope.table('unknown')
        with self.assertRaises(RuntimeError):
            self.scope.relocate(
                self.base.sql + '\nSELECT * FROM `other.other.foreign`;')

    def test_scope_requires_exact_owned_dataset_metadata(self):
        dataset = Mock(
            full_dataset_id=f'{PROJECT}:{self.scope.dataset.split(".")[1]}',
            location=LOCATION, labels=self.scope.labels,
            default_table_expiration_ms=TTL_MS)
        self.scope.own(dataset)
        dataset.labels = {'purpose': 'other'}
        with self.assertRaises(RuntimeError):
            self.scope.own(dataset)

    def test_all_production_tables_are_relocation_targets(self):
        self.assertEqual(set(TABLES), {
            'prompt_registry', 'prompt_registry_history'})
        for name in TABLES:
            self.assertEqual(self.scope.table(name),
                             f'{self.scope.dataset}.{name}')

    def test_fixture_executor_only_preflights_probe_scope(self):
        client = Mock(
            project=PROJECT, location=LOCATION,
            default_query_job_config=None)
        probe = Mock(client=client)
        executor = FixtureExecutor(probe)
        executor._preflight()
        probe.preflight.assert_called_once_with()
        client.get_table.assert_not_called()

    def test_cleanup_requires_ownership_and_deletes_only_owned_dataset(self):
        client = Mock(
            project=PROJECT, location=LOCATION,
            default_query_job_config=None)
        probe = Probe(client)
        probe.scope = self.scope
        probe.cleanup()
        client.delete_dataset.assert_not_called()

        owned = Mock(
            full_dataset_id=f'{PROJECT}:{self.scope.dataset.split(".")[1]}',
            location=LOCATION, labels=self.scope.labels,
            default_table_expiration_ms=TTL_MS)
        client.get_dataset.side_effect = [owned, NotFound('deleted')]
        probe.owned = True
        probe.cleanup()
        client.delete_dataset.assert_called_once_with(
            self.scope.dataset, delete_contents=True, not_found_ok=False,
            retry=None, timeout=10)
        self.assertFalse(probe.owned)
        self.assertEqual(probe.report['cleanup'],
                         'DELETED_AND_ABSENCE_VERIFIED')


if __name__ == '__main__':
    unittest.main()
