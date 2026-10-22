from types import SimpleNamespace as NS
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.workflow_submission_worker import execute
from src.application.campaign_chat_evidence_service import EvidenceChatReadiness
from src.application.workflow_submission_host import WorkflowSubmissionHost
from src.ui.workflow_runtime import WorkflowUiRuntime,WorkflowUiSettings
from src.application.identity import ASSERTION_HEADER
from src.application.operation_channel import ChannelError


class QuestionServingTransportTests(TestCase):
    def test_worker_read_uses_scope_and_has_no_submission_side_effect(self):
        service=Mock();service.evidence_readiness.return_value=EvidenceChatReadiness(('HELD',),())
        service.question_turn.return_value=None
        scope=dict(workspace_id='w',conversation_id='c')
        result=execute(service,'evidence',{'iap':'assertion'},scope)
        self.assertIsNone(result['turn']);self.assertEqual(result['workspace_id'],'w')
        service.submit.assert_not_called()
        service.evidence_readiness.assert_called_once_with({'iap':'assertion'},**scope)

    def runtime(self):
        runtime=object.__new__(WorkflowUiRuntime)
        runtime._submission=Mock();runtime._evidence_service=None
        runtime.settings=WorkflowUiSettings('w','c')
        return runtime

    def test_ui_uses_owned_host_and_trusted_scope(self):
        runtime=self.runtime();ready=EvidenceChatReadiness(('HELD',),())
        runtime._submission.evidence.return_value=NS(readiness=ready,turn=None)
        headers={ASSERTION_HEADER:'assertion','cookie':'discard'}
        self.assertIs(runtime.evidence_readiness(headers),ready)
        self.assertIsNone(runtime.question_turn(headers))
        runtime._submission.evidence.assert_called_with({ASSERTION_HEADER:'assertion'},workspace_id='w',conversation_id='c')

    def test_denied_host_does_not_fall_back_to_local_text(self):
        runtime=self.runtime();runtime._submission.evidence.side_effect=RuntimeError('private detail')
        for method in (runtime.question_turn,runtime.evidence_readiness):
            with self.assertRaisesRegex(RuntimeError,'CHAT_') as caught:method({ASSERTION_HEADER:'assertion'})
            self.assertNotIn('private detail',str(caught.exception))

    def test_host_rejects_cross_scope_result(self):
        host=object.__new__(WorkflowSubmissionHost)
        with self.assertRaises(ChannelError):host._validate_result_scope('evidence',
            dict(workspace_id='w',conversation_id='c'),NS(workspace_id='other',conversation_id='c'))
