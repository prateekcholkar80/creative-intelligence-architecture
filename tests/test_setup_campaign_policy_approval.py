"""Metadata-only setup with no source replacement or approval seeds."""
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from src.ingestion import setup_campaign_policy_approval as module
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION


class PolicyApprovalSetupTests(TestCase):
    def client(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        client.get_dataset.return_value=SimpleNamespace(project=PROJECT,dataset_id='ci_brand_memory',location=LOCATION,default_table_expiration_ms=None)
        return client

    def test_dry_run_missing_tables_does_not_create_or_seed(self):
        client=self.client();client.get_table.side_effect=NotFound('missing')
        result=module.setup(client)
        self.assertEqual(result['status'],'MISSING');client.create_table.assert_not_called()
        client.query.assert_not_called();client.load_table_from_json.assert_not_called()

    def test_explicit_creation_and_reapply_preserve_rows_and_iam(self):
        client=self.client();actual=SimpleNamespace(num_rows=7)
        client.get_table.side_effect=[NotFound('missing'),actual,actual,actual]
        with patch.object(module,'validate_table'):
            result=module.setup(client,apply=True)
        self.assertEqual(sum(t['created'] for t in result['tables']),1)
        self.assertEqual(result['rows_written'],0);self.assertEqual(result['approvals_recorded'],0)
        client.delete_table.assert_not_called();client.update_table.assert_not_called();client.set_iam_policy.assert_not_called()
        client.get_table.side_effect=None;client.get_table.return_value=actual
        with patch.object(module,'validate_table'):again=module.setup(client,apply=True)
        self.assertFalse(any(t['created'] for t in again['tables']))

    def test_wrong_scope_or_schema_denied_without_mutation(self):
        client=self.client();client.project='foreign'
        with self.assertRaises(ValueError):module.setup(client,apply=True)
        client.create_table.assert_not_called()
        client=self.client()
        with patch.object(module,'validate_table',side_effect=ValueError('schema mismatch')):
            with self.assertRaisesRegex(ValueError,'schema mismatch'):module.setup(client,apply=True)
        client.create_table.assert_not_called()

    def test_competing_creator_is_verified_without_claiming_creation(self):
        from google.api_core.exceptions import Conflict
        client=self.client();actual=SimpleNamespace(num_rows=0)
        client.get_table.side_effect=[NotFound('missing'),actual,actual,actual]
        client.create_table.side_effect=Conflict('already created')
        with patch.object(module,'validate_table'):
            result=module.setup(client,apply=True)
        self.assertFalse(any(t['created'] for t in result['tables']))
