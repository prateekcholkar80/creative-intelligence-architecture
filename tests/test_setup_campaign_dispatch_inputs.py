from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from src.ingestion import setup_campaign_dispatch_inputs as setup

class SetupInputTests(TestCase):
    def test_metadata_only_setup_and_reapply(self):
        client=Mock();client.project=setup.PROJECT;client.location=setup.LOCATION
        tables={}
        def get(name,**kw):
            key=str(name)
            if key not in tables:raise NotFound('missing')
            return tables[key]
        def create(table,**kw):
            table._properties.update(type='TABLE',location=setup.LOCATION)
            tables[str(table.reference)]=table;return table
        client.get_table.side_effect=get;client.create_table.side_effect=create
        with patch.object(setup,'validate_dataset'):
            self.assertEqual(setup.setup(client)['status'],'MISSING_RESOURCES')
            self.assertEqual(setup.setup(client,apply=True)['created_tables'],['invocation_input_manifests','campaign_transcript_bodies'])
            self.assertEqual(setup.setup(client,apply=True)['created_tables'],[])
        client.query.assert_not_called()
