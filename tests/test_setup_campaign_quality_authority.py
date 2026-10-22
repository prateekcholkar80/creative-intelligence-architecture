from unittest import TestCase
from unittest.mock import Mock
from google.api_core.exceptions import NotFound
from src.ingestion.setup_campaign_quality_authority import setup,PROJECT,LOCATION
from src.application.campaign_quality_authority import table_metadata
from tests.test_setup_workflow_tables import dataset


class SetupQualityAuthorityTests(TestCase):
    def client(self):
        c=Mock(project=PROJECT,location=LOCATION);c.get_dataset.return_value=dataset()
        table=table_metadata();table._properties.update(type='TABLE',location=LOCATION)
        return c,table

    def test_check_never_creates_and_apply_is_idempotent(self):
        c,t=self.client();c.get_table.side_effect=NotFound('missing')
        self.assertEqual(setup(c)['status'],'MISSING_RESOURCE');c.create_table.assert_not_called()
        c.get_table.side_effect=[NotFound('missing'),t,t]
        self.assertTrue(setup(c,apply=True)['created'])
        self.assertFalse(setup(c,apply=True)['created'])
        self.assertEqual(c.create_table.call_count,1);c.query.assert_not_called()

    def test_schema_conflict_and_wrong_project_deny_without_replacing(self):
        c,t=self.client();t.clustering_fields=[];c.get_table.return_value=t
        with self.assertRaises(Exception):setup(c,apply=True)
        c.create_table.assert_not_called();c.project='other'
        with self.assertRaises(ValueError):setup(c)
