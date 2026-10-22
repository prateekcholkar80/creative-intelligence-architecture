"""Owned bootstrap fixture/namespace guards, no RPCs."""
import unittest
from unittest.mock import Mock
from google.cloud import bigquery
from src.application.signing_bootstrap import bootstrap_sql
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from src.validation import signing_bootstrap_integration as probe
from src.validation.signing_read_integration import TTL_MS


class BootstrapProbeGuardTests(unittest.TestCase):
    def setUp(self):
        self.scope=probe.BootstrapProbeScope('a'*32)
        self.client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)

    def dataset(self):
        data=bigquery.Dataset(self.scope.dataset)
        data.location=LOCATION;data.labels=self.scope.labels;data.default_table_expiration_ms=TTL_MS
        return data

    def test_reserved_native_slots_are_owned_and_bounded(self):
        for case in range(3):
            self.assertEqual(len({self.scope.slot(case,i) for i in range(3)}),3)
        for case,attempt in ((-1,0),(20,0),(True,0),(0,3),(0,True)):
            with self.assertRaises((RuntimeError,ValueError)):self.scope.slot(case,attempt)

    def test_all_sql_targets_are_owned_and_serving_scope_is_denied(self):
        sql=self.scope.relocate(bootstrap_sql())
        self.assertNotIn('.ci_application.',sql)
        with self.assertRaises(RuntimeError):
            self.scope.assert_sql(sql+f'\nDELETE FROM `{PROJECT}.ci_application.signing_heads` WHERE TRUE;')

    def test_ownership_labels_region_and_expiry_required_for_cleanup(self):
        runtime=probe.Probe(self.client);runtime.scope=self.scope;runtime.owned=True
        for field,value in (('labels',{}),('location','US'),('default_table_expiration_ms',None)):
            actual=self.dataset();setattr(actual,field,value)
            self.client.get_dataset.return_value=actual
            with self.assertRaises(RuntimeError):runtime.cleanup()
        self.client.delete_dataset.assert_not_called()

    def test_cleanup_never_mutates_without_ownership(self):
        probe.Probe(self.client).cleanup()
        self.client.delete_dataset.assert_not_called()

    def test_initial_fixtures_contain_only_other_policy_sentinels(self):
        runtime=probe.Probe(self.client)
        for rows in runtime.rows.values():
            self.assertTrue(rows)
            self.assertTrue(all(row['policy_id']!=probe.POLICY_ID for row in rows))


if __name__=='__main__':unittest.main()
