"""Owned-scope/fixture safety tests for the opt-in native read probe; no RPCs."""
from datetime import datetime, timezone
import unittest
from unittest.mock import Mock

from google.cloud import bigquery

from src.application.sharing_publication_codec import decode_publication, decode_history
from src.application.sharing_publication_reader import read_sql
from src.application.sharing_publication_schema import TABLES
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from src.validation import signing_read_integration as probe


class ProbeGuardTests(unittest.TestCase):
    def setUp(self):
        self.scope = probe.ProbeScope('a' * 32)

    def dataset(self):
        dataset = bigquery.Dataset(self.scope.dataset)
        dataset.location = LOCATION
        dataset.labels = self.scope.labels
        dataset.default_table_expiration_ms = probe.TTL_MS
        return dataset

    def test_production_sql_can_only_relocate_to_all_four_owned_targets(self):
        sql = self.scope.relocate(read_sql())
        self.assertNotIn('.ci_application.', sql)
        for name in TABLES:
            self.assertIn(self.scope.table(name), sql)
        for bad in ('SELECT 1', f'SELECT * FROM `{PROJECT}.ci_product.product_master`',
                    sql + f'\nDELETE FROM `{PROJECT}.ci_application.signing_heads` WHERE TRUE;'):
            with self.assertRaises(RuntimeError):
                self.scope.assert_sql(bad)

    def test_unknown_names_paths_and_invalid_scope_are_rejected(self):
        for value in ('', 'a' * 31, 'g' * 32, '../path', None):
            with self.assertRaises(RuntimeError):
                probe.ProbeScope(value)
        for name in ('product_master', 'signing_heads;DELETE', '../signing_heads'):
            with self.assertRaises(RuntimeError):
                self.scope.table(name)

    def test_explicit_ownership_region_labels_and_ttl_required_before_cleanup(self):
        self.scope.assert_owned(self.dataset())
        for field, value in (('location', 'US'), ('labels', {}), ('default_table_expiration_ms', None)):
            dataset = self.dataset()
            setattr(dataset, field, value)
            with self.assertRaises(RuntimeError):
                self.scope.assert_owned(dataset)
        dataset = bigquery.Dataset(f'{PROJECT}.ci_application')
        dataset.location, dataset.labels, dataset.default_table_expiration_ms = LOCATION, self.scope.labels, probe.TTL_MS
        with self.assertRaises(RuntimeError):
            self.scope.assert_owned(dataset)

    def test_fixture_publications_are_valid_metadata_without_key_bytes(self):
        pubs = probe.publications(datetime.now(timezone.utc))
        rows = probe.fixture_rows(pubs)
        self.assertEqual(set(rows), set(TABLES))
        for pub, row in zip(pubs, rows['signing_publications']):
            history = tuple(decode_history(h) for h in rows['signing_key_history']
                            if h['publication_revision'] == pub.revision)
            self.assertEqual(decode_publication(row, history).digest, pub.digest)
            self.assertEqual(pub.config.project_number, probe.FIXTURE_PROJECT_NUMBER)
            self.assertNotIn('material', row)

    def test_fixture_times_preserve_microseconds_for_native_hash_round_trip(self):
        at = datetime(2026, 10, 11, 0, 0, 0, 123456, tzinfo=timezone.utc)
        rows = probe.json_rows(probe.fixture_rows(probe.publications(at)))
        self.assertIn('.123456+00:00', rows['signing_publications'][0]['published_at'])
        self.assertIn('.123456+00:00', rows['signing_key_history'][0]['issue_from'])

    def test_cleanup_denies_changed_owner_before_dataset_deletion(self):
        client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)
        runtime = probe.Probe(client)
        runtime.scope = self.scope
        runtime.owned = True
        wrong = self.dataset()
        wrong.labels = {'purpose': 'some-other-owner'}
        client.get_dataset.return_value = wrong
        with self.assertRaises(RuntimeError):
            runtime.cleanup()
        client.delete_dataset.assert_not_called()

    def test_no_cleanup_mutation_when_nothing_was_created(self):
        client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)
        probe.Probe(client).cleanup()
        client.get_dataset.assert_not_called()
        client.delete_dataset.assert_not_called()

    def test_setup_preflight_failure_cannot_create_fixture_dataset(self):
        client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)
        client.get_dataset.return_value = self.dataset()
        with self.assertRaises(ValueError):
            probe.Probe(client).setup()
        client.create_dataset.assert_not_called()


if __name__ == '__main__':
    unittest.main()
