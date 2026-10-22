from unittest import TestCase
from unittest.mock import Mock
from google.api_core.exceptions import NotFound
from src.ingestion.setup_campaign_document_processing import setup
from src.application.campaign_document_store import table_metadata,PROJECT,LOCATION,TABLES


class DocumentSetupTests(TestCase):
    def test_check_does_not_create_and_reapply_preserves_tables(self):
        client=Mock(project=PROJECT,location=LOCATION);tables={}
        def get(name,**kw):
            if name not in tables:raise NotFound('missing')
            return tables[name]
        def create(table,**kw):
            table._properties.update(type='TABLE',location=LOCATION)
            tables[str(table.reference)]=table;return table
        client.get_table.side_effect=get;client.create_table.side_effect=create
        self.assertEqual([r['status'] for r in setup(client)],['MISSING','MISSING'])
        client.create_table.assert_not_called()
        self.assertTrue(all(r['created'] for r in setup(client,apply=True)))
        self.assertFalse(any(r['created'] for r in setup(client,apply=True)))
        self.assertEqual(client.create_table.call_count,2)

    def test_existing_schema_conflict_denies_before_any_creation(self):
        client=Mock(project=PROJECT,location=LOCATION)
        table=table_metadata('turn_links');table._properties.update(type='VIEW',location=LOCATION)
        client.get_table.side_effect=[NotFound('missing first'),table]
        with self.assertRaises(ValueError):setup(client,apply=True)
        client.create_table.assert_not_called()
