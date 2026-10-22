from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from google.api_core.iam import Policy
from src.ingestion.setup_campaign_evaluator import *


class EvaluatorSetupTests(TestCase):
    def client(self):return Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
    def test_read_only_missing_never_creates(self):
        client=self.client();client.get_table.side_effect=NotFound('missing')
        self.assertTrue(all(r['status']=='MISSING' for r in setup(client)))
        client.create_table.assert_not_called()
    def test_inspects_all_existing_schemas_before_creating(self):
        client=self.client();client.get_table.side_effect=[NotFound('missing'),object()]
        with patch('src.ingestion.setup_campaign_evaluator.validate_table',side_effect=ValueError('wrong schema')),self.assertRaises(ValueError):setup(client,apply=True)
        client.create_table.assert_not_called()
    def test_role_cannot_broaden_or_scope_change(self):
        verify_role(dict(name=ROLE,stage='GA',includedPermissions=list(PERMISSIONS)))
        with self.assertRaises(ValueError):verify_role(dict(name=ROLE,stage='GA',includedPermissions=list(PERMISSIONS|{'bigquery.tables.delete'})))
        with self.assertRaises(ValueError):grant(self.client(),'other.dataset.table',apply=True)
    def test_etag_and_unrelated_binding_are_preserved(self):
        client=self.client();before=Policy();before.etag=b'original';before.bindings.append(dict(role='roles/bigquery.dataViewer',members={'user:fixture@example.com'}))
        after=deepcopy(before);after.bindings.append(dict(role=ROLE,members={MEMBER}))
        client.get_iam_policy.side_effect=[before,after]
        self.assertEqual(grant(client,TABLES['intents'],apply=True)['status'],'APPLIED_VERIFIED')
        proposed=client.set_iam_policy.call_args.args[1];self.assertEqual(proposed.etag,b'original');self.assertEqual(proposed.bindings[0],before.bindings[0])
    def test_no_etag_no_mutation(self):
        client=self.client();client.get_iam_policy.return_value=Policy()
        with self.assertRaises(ValueError):grant(client,TABLES['intents'],apply=True)
        client.set_iam_policy.assert_not_called()
