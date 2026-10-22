"""4.2 Critical/Must have/shared pilot: owned mutation-probe guards; no RPCs."""
import unittest
from unittest.mock import Mock

from google.cloud import bigquery

from src.application.signing_write_query import render_write_sql
from src.application.sharing_publication_reader import read_sql
from src.application.sharing_publication_schema import TABLES
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from src.validation import signing_write_integration as probe
from src.validation.signing_read_integration import TTL_MS


class WriteProbeGuardTests(unittest.TestCase):
    def setUp(self):
        self.scope=probe.WriteProbeScope('a'*32)
        self.client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)

    def dataset(self):
        result=bigquery.Dataset(self.scope.dataset)
        result.location=LOCATION
        result.labels=self.scope.labels
        result.default_table_expiration_ms=TTL_MS
        return result

    def test_read_and_write_sql_relocate_only_to_owned_four_tables(self):
        for sql in (read_sql(),render_write_sql()):
            owned=self.scope.relocate(sql)
            self.assertNotIn('.ci_application.',owned)
            for name in TABLES:self.assertIn(self.scope.table(name),owned)
            with self.assertRaises(RuntimeError):
                self.scope.assert_sql(owned+f'\nDELETE FROM `{PROJECT}.ci_application.signing_heads` WHERE TRUE;')

    def test_cleanup_requires_write_probe_labels_scope_region_and_expiry(self):
        runtime=probe.Probe(self.client)
        runtime.scope=self.scope;runtime.owned=True
        for field,value in (('labels',dict(purpose='signing_read_integration',run_id=self.scope.run_id)),
                            ('location','US'),('default_table_expiration_ms',None)):
            actual=self.dataset();setattr(actual,field,value)
            self.client.get_dataset.return_value=actual
            with self.assertRaises(RuntimeError):runtime.cleanup()
        self.client.delete_dataset.assert_not_called()

    def test_cleanup_never_touches_an_uncreated_dataset(self):
        probe.Probe(self.client).cleanup()
        self.client.get_dataset.assert_not_called()
        self.client.delete_dataset.assert_not_called()

    def test_fixture_contains_other_policy_and_consumer_sentinels(self):
        runtime=probe.Probe(self.client)
        for name,rows in runtime.rows.items():
            self.assertTrue(any(runtime.in_scope(name,row) for row in rows))
            self.assertTrue(any(not runtime.in_scope(name,row) for row in rows))
        self.assertTrue(any(row['policy_id']==probe.POLICY_ID and row['consumer_id']=='fixture-other-consumer'
                            for row in runtime.rows['signing_checkpoints']))

    def test_ownership_failure_blocks_write_before_query(self):
        runtime=probe.Probe(self.client)
        self.client.get_dataset.return_value=self.dataset()
        with self.assertRaises(RuntimeError):runtime.execute(Mock())
        self.client.query.assert_not_called()

    def test_fixture_load_refuses_unrelated_tables(self):
        runtime=probe.Probe(self.client);runtime.scope=self.scope
        self.client.get_dataset.return_value=self.dataset()
        with self.assertRaises(RuntimeError):runtime.load('product_master',[])
        self.client.load_table_from_json.assert_not_called()


if __name__=='__main__':unittest.main()
