from unittest import TestCase
from unittest.mock import Mock
from google.api_core.iam import Policy
from src.ingestion.setup_campaign_document_access import grant,verify_role,ROLE,MEMBER,TABLES


class DocumentAccessTests(TestCase):
    def test_role_cannot_broaden(self):
        value=dict(name=ROLE,stage='GA',includedPermissions=['bigquery.tables.get','bigquery.tables.getData','bigquery.tables.updateData'])
        verify_role(value)
        with self.assertRaises(ValueError):verify_role(dict(value,includedPermissions=value['includedPermissions']+['bigquery.tables.delete']))
    def test_scope_and_read_only(self):
        client=Mock();policy=Policy();policy.etag=b'etag';client.get_iam_policy.return_value=policy
        with self.assertRaises(ValueError):grant(client,'other.table',apply=True)
        self.assertEqual(grant(client,TABLES['extractions'])['status'],'MISSING');client.set_iam_policy.assert_not_called()
    def test_etag_and_unrelated_grants_preserved(self):
        client=Mock();policy=Policy();policy.etag=b'etag';policy.bindings.append(dict(role='roles/bigquery.dataViewer',members={'user:fixture@example.com'}))
        def apply(table,proposed,**kwargs):
            self.assertEqual(proposed.etag,b'etag');self.assertEqual(proposed.bindings[0],policy.bindings[0]);return proposed
        expected=Policy();expected.etag=b'new';expected.bindings.extend([dict(role='roles/bigquery.dataViewer',members={'user:fixture@example.com'}),dict(role=ROLE,members={MEMBER})])
        client.get_iam_policy.side_effect=[policy,expected];client.set_iam_policy.side_effect=apply
        self.assertEqual(grant(client,TABLES['turn_links'],apply=True)['status'],'APPLIED_VERIFIED')
    def test_missing_etag_denies(self):
        client=Mock();client.get_iam_policy.return_value=Policy()
        with self.assertRaises(ValueError):grant(client,TABLES['extractions'],apply=True)
        client.set_iam_policy.assert_not_called()
