"""Owned native probe guards and realistic parent/child construction."""
from datetime import datetime,timezone
import json
from types import SimpleNamespace
from unittest import TestCase
from src.validation.question_child_fixtures import parent_fixture,child_plan
from src.validation.question_child_integration import relocate,EXTRA
from src.application.campaign_transcript_history import decode_history,TABLES
from src.application.conversation_contract import validate_conversation
from src.application.workflow_codec import encode_row
from src.ingestion.setup_workflow_tables import DATASET_ID


class QuestionChildProbeTests(TestCase):
    def capture(self):
        rows,extra=parent_fixture('question-offline')
        native=dict(observed_at=datetime.now(timezone.utc))
        source=dict(requests=[encode_row(rows['requests'][0],'requests')],calls=[encode_row(rows['model_invocations'][0],'model_invocations')],
            bodies=[json.dumps(extra['campaign_transcript_bodies'][0])],responses=[json.dumps(extra['campaign_transcript_responses'][0])],jobs=[],attempts=[])
        for key in TABLES:native[key]=source[key];native[key+'_count']=len(source[key])
        conv=rows['conversations'][0];resource=validate_conversation(conv,workspace_id='question-offline',conversation_id=conv['conversation_id'])
        history=decode_history(native,resource=resource,observed_at=datetime.now(timezone.utc))
        return rows,extra,history

    def test_realistic_child_construction_pins_complete_parent_history(self):
        plan=child_plan(*self.capture(),'child')
        params={p.name:p.value for p in plan.config().query_parameters}
        payload=json.loads(params['authority_payload'])
        self.assertEqual(payload['stage'],'CLARIFICATION')
        self.assertEqual(len(payload['transcript']),3)
        self.assertIn('question_parent',payload)
        self.assertIn('QUESTION_PARENT_CHANGED',plan.sql)

    def test_relocation_preserves_owned_scope_and_denies_external_table(self):
        probe=SimpleNamespace(scope=SimpleNamespace(dataset='fixture.owned'))
        for name in EXTRA-{'product_master'}:
            self.assertIn('fixture.owned',relocate(probe,f'SELECT * FROM `{DATASET_ID}.{name}`'))
        with self.assertRaises(ValueError):relocate(probe,'SELECT * FROM `unrelated.dataset.table`')
