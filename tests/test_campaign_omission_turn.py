import json,hashlib
from unittest import TestCase
from src.application.campaign_omission_turn import OmissionContext,bind_omission_plan,omission_key
from src.application.campaign_evidence_gaps import canonical
from src.application.grounded_interpreter_submission import prepare_grounded_submission
from src.application.campaign_transcript_history import TranscriptHistory,TABLES
from tests import test_interpreter_submission as base_fixtures


def fixture():
    f=base_fixtures.InterpreterSubmissionTests();f.setUp();inputs=dict(f.inputs)
    ws=inputs['workspace']['workspace_id'];conv=inputs['conversation']['conversation_id'];actor=inputs['principal_id']
    selection=dict(sku='CI-SKU-0001',country='US',channel='META',audience='NEW')
    source=dict(scope=dict(workspace_id=ws,conversation_id=conv,request_id='parent'),principal_id=actor,
        requirement_id='scientific-assertion',file=None,selection=dict(choice='OMIT_UNSUPPORTED_CONTENT',status='REQUEST_CHANGE_PENDING',alternative='FACTUAL_PRODUCT_COPY'),
        collection_request=dict(missing_fields=['approved_wording','study_report_reference']))
    context=OmissionContext(canonical(dict(source=source,submission_id='a'*64,selection=selection,parent_authority={},
        policy=dict(policy_id='policy',version='v1',policy_sha256='b'*64,policy_json='{}'),generation_authorized=False)))
    inputs.update(message=context.message,retry_key='omission-v1-'+'a'*64)
    history=TranscriptHistory(canonical({k:[] for k in TABLES}),(),(),inputs['observed_at'])
    return inputs,context,selection,history


class OmissionTurnTests(TestCase):
    def test_explicit_choice_binds_new_turn_link_and_sources_atomically(self):
        inputs,context,selection,history=fixture()
        plan=prepare_grounded_submission(selection=selection,history=history,**inputs)
        bound=bind_omission_plan(plan,context)
        params={p.name:p.value for p in bound.config().query_parameters};link=json.loads(params['omission_link'])
        self.assertNotEqual(link['new_request_id'],link['parent_request_id'])
        self.assertEqual(link['status'],'REEVALUATION_REQUIRED');self.assertFalse(link['generation_authorized'])
        self.assertEqual(link['sql_sha256'],hashlib.sha256(bound.sql.encode()).hexdigest())
        self.assertIn('OMISSION_SOURCE_CHANGED',bound.sql);self.assertIn('OMISSION_POLICY_CHANGED',bound.sql)
        self.assertLess(bound.sql.index('campaign_evidence_turn_links` (record_id'),bound.sql.index('COMMIT TRANSACTION;'))
        self.assertEqual(json.loads(params['request_candidate'])['message_sha256'],hashlib.sha256(context.message.encode()).hexdigest())

    def test_upload_or_wrong_retry_context_cannot_become_an_omission(self):
        inputs,context,selection,history=fixture()
        plan=prepare_grounded_submission(selection=selection,history=history,**inputs)
        value=context.value;value['source']['selection']['choice']='PROVIDE_EVIDENCE'
        with self.assertRaises(ValueError):bind_omission_plan(plan,OmissionContext(canonical(value)))
        inputs['retry_key']='different'
        with self.assertRaises(ValueError):bind_omission_plan(prepare_grounded_submission(selection=selection,history=history,**inputs),context)

    def test_prior_transcript_capture_is_mandatory_and_original_request_not_rewritten(self):
        inputs,context,selection,history=fixture()
        plan=prepare_grounded_submission(selection=selection,**inputs)
        with self.assertRaises(ValueError):bind_omission_plan(plan,context)
        self.assertEqual(context.value['source']['scope']['request_id'],'parent')
