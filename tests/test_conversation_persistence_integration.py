from types import SimpleNamespace
from unittest import TestCase
from src.validation.workflow_writer_integration import Scope
from src.validation.conversation_persistence_integration import relocate

class ConversationProbeGuards(TestCase):
    def test_only_owned_explicit_tables_can_be_relocated(self):
        probe=SimpleNamespace(scope=Scope('a'*32))
        from src.ingestion.setup_workflow_tables import DATASET_ID
        result=relocate(probe,f'SELECT * FROM `{DATASET_ID}.campaign_transcript_responses`')
        self.assertIn(probe.scope.dataset,result)
        with self.assertRaises(ValueError):relocate(probe,'SELECT * FROM `foreign.dataset.table`')
