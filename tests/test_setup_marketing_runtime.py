from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock
from google.api_core.iam import Policy
from src.ingestion.setup_marketing_runtime import grant_table,ROLE,RUNTIME,PROJECT,PERMISSIONS

class MarketingGrantTests(TestCase):
    def test_role_has_no_table_creation_deletion_or_model_permission(self):
        self.assertEqual(set(PERMISSIONS),{'bigquery.tables.get','bigquery.tables.getData','bigquery.tables.updateData'})
    def test_exact_table_grant_preserves_existing_bindings_and_is_idempotent(self):
        original=Policy();original.etag='etag';original.bindings.append(dict(role='roles/bigquery.dataViewer',members={'user:existing@example.com'}))
        client=Mock();store=[original]
        client.get_iam_policy.side_effect=lambda *a,**kw:deepcopy(store[0])
        client.set_iam_policy.side_effect=lambda table,policy,**kwargs:store.__setitem__(0,deepcopy(policy))
        table=PROJECT+'.ci_application.requests'
        self.assertEqual(grant_table(client,table,ROLE),'ADDED_VERIFIED')
        self.assertIn('user:existing@example.com',store[0].bindings[0]['members'])
        self.assertEqual(grant_table(client,table,ROLE),'EXISTING');client.set_iam_policy.assert_called_once()
    def test_unrelated_table_or_role_denied_before_any_api(self):
        client=Mock()
        for table,role in ((PROJECT+'.ci_product.ingredients',ROLE),(PROJECT+'.ci_application.requests','roles/owner')):
            with self.assertRaises(ValueError):grant_table(client,table,role)
        client.get_iam_policy.assert_not_called()
