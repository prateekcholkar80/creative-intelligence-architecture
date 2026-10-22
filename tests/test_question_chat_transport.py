import json
from unittest import TestCase
from unittest.mock import patch
from src.application.question_chat_transport import encode_result,decode_result
from src.application.campaign_chat_evidence_service import EvidenceChatReadiness
from src.application.workflow_submission_host import request_values
from src.application.operation_channel import ChannelError
from tests.test_campaign_question_view import QuestionViewTests
from tests.test_prompt_contract import NOW


class QuestionTransportTests(TestCase):
    def readiness(self):return EvidenceChatReadiness(('CHAT_EVIDENCE_POLICY_REVIEW_REQUIRED',),
        (('campaign-evidence-questions','DRAFT'),))

    def test_withheld_turn_does_not_cross_wire(self):
        turn=QuestionViewTests().turn()
        result=encode_result('w','c',self.readiness(),turn,now=NOW)
        self.assertIsNone(result['turn'])
        self.assertNotIn(turn.candidate.text,json.dumps(result))
        decoded=decode_result(json.loads(json.dumps(result)),now=NOW)
        self.assertIsNone(decoded.turn);self.assertFalse(decoded.readiness.release_allowed)

    def test_frame_cannot_supply_release_override(self):
        raw=encode_result('w','c',self.readiness(),None)
        raw['readiness']['release_allowed']=True
        with self.assertRaises(ValueError):decode_result(raw)

    def test_injected_provider_turn_denied(self):
        raw=encode_result('w','c',self.readiness(),None);raw['turn']={'text':'unreviewed'}
        with self.assertRaisesRegex(ValueError,'QUESTION_TRANSPORT_RELEASE_WITHHELD'):decode_result(raw)

    def test_read_only_evidence_request_cannot_submit_text_or_pins(self):
        scope=dict(workspace_id='w',conversation_id='c')
        self.assertEqual(request_values('evidence',{},scope)['parameters'],scope)
        for key in ('message','approval','request_id','release_allowed'):
            with self.assertRaises(ChannelError):request_values('evidence',{},scope|{key:'injected'})

    def test_corrupt_turn_is_rejected_before_filtering(self):
        from dataclasses import replace
        turn=QuestionViewTests().turn()
        turn=replace(turn,candidate=replace(turn.candidate,text='changed'))
        with self.assertRaises(ValueError):encode_result('w','c',self.readiness(),turn,now=NOW)
