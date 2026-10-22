"""Disabled child plan structure and scoped parent guards; no cloud writes."""
from datetime import timedelta
import json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch
from src.application.campaign_question_submission import prepare_question_submission
from src.application.campaign_transcript_history import TranscriptHistory
from src.application.campaign_question_binding import VERSION
from tests import test_interpreter_submission as fixture


class QuestionSubmissionTests(TestCase):
    def setUp(self):
        f=fixture.InterpreterSubmissionTests();f.setUp();self.inputs=f.inputs
        self.inputs['selection']=dict(sku='CI-SKU-0001',country='US',channel='META',audience='NEW')
        self.history=TranscriptHistory(json.dumps({k:[] for k in
            ('requests','calls','bodies','responses','jobs','attempts')}),(),(),self.inputs['observed_at'])
        scope=dict(workspace_id=self.inputs['workspace']['workspace_id'],request_id='parent')
        self.parent=SimpleNamespace(content_canonical_json=json.dumps(dict(request_record=scope,
            authority=dict(scope,conversation_id='conversation'),calls=[dict(request_id='parent',invocation_id='parent-call')],
            bindings=[dict(invocation_id='parent-call',prompt_key='campaign-evidence-intent')])) )
        self.parent.valid_until=self.inputs['observed_at']+timedelta(seconds=10)
        self.gaps=SimpleNamespace(valid_until=self.inputs['observed_at']+timedelta(seconds=20))

    def build(self):
        # Parent capture validation has its own adversarial contract tests. This
        # fixture isolates how a validated payload is added to the transaction.
        def bound(child,**_):
            return dict(child,version=VERSION,stage='CLARIFICATION',question_parent=dict(invocation_id='parent-call'))
        with patch('src.application.campaign_question_submission.bind_question_authority',side_effect=bound):
            return prepare_question_submission(parent=self.parent,gaps=self.gaps,history=self.history,**self.inputs)

    def test_atomic_child_uses_existing_slot_and_scoped_parent_guards(self):
        plan=self.build();params={p.name:p.value for p in plan.config().query_parameters}
        payload=json.loads(params['authority_payload'])
        self.assertEqual(payload['version'],VERSION)
        self.assertEqual(plan.kind,'QUESTION_INTERPRETER_SUBMISSION')
        self.assertTrue(plan.job_id.startswith('interpreter_submit_v1_'))
        self.assertIn('QUESTION_PARENT_AUTHORITY_CHANGED',plan.sql)
        self.assertEqual(plan.sql.count("AS 'QUESTION_PARENT_CHANGED'"),3)
        self.assertLess(plan.sql.index('QUESTION_PARENT_CHANGED'),plan.sql.index("IF (SELECT COUNT(*)"))
        self.assertIn('CHAT_TRANSCRIPT_SOURCE_CHANGED',plan.sql)
        self.assertNotIn('DELETE',plan.sql)
        self.assertIn('question_parent_invocation_id_binding',params)
        self.assertEqual(params['expires_at'],self.parent.valid_until)

    def test_missing_history_and_missing_parent_prompt_deny(self):
        with self.assertRaisesRegex(ValueError,'QUESTION_HISTORY_REQUIRED'):
            prepare_question_submission(parent=self.parent,gaps=object(),history=None,**self.inputs)
        value=json.loads(self.parent.content_canonical_json);value['bindings']=[]
        self.parent.content_canonical_json=json.dumps(value)
        with self.assertRaisesRegex(ValueError,'QUESTION_PARENT_PROMPT_REQUIRED'):self.build()
