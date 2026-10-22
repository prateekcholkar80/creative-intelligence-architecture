from unittest import TestCase
from src.validation.attachment_storage_integration import relocated_sql
from src.application.campaign_attachment_service import PROJECT,TABLE


class NativeAttachmentScopeTests(TestCase):
    def test_only_owned_fixture_namespace_can_receive_production_sql(self):
        target=PROJECT+'.ci_attachment_it_123456abcdef.submissions'
        sql=relocated_sql(target)
        self.assertIn('`'+target+'`',sql);self.assertNotIn('`'+TABLE+'`',sql)
        for bad in (TABLE,PROJECT+'.ci_attachment_it_invalid.submissions','other.ci_attachment_it_123456abcdef.submissions'):
            with self.assertRaises(ValueError):relocated_sql(bad)
