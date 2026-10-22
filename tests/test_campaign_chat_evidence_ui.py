from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock,patch
from streamlit.testing.v1 import AppTest
from src.application.campaign_chat_evidence_service import EvidenceChatReadiness
from src.ui.workflow_runtime import WorkflowUiRuntime,WorkflowUiSettings,WorkflowUiError
from src.ui.workflow_session import DURABLE_UI_ENV,WorkflowSessionState
from tests.test_workflow_runtime import hosts,HEADERS,WS,CONV

class EvidenceChatUiTests(TestCase):
    def test_existing_runtime_without_evidence_installation_denies(self):
        runtime=WorkflowUiRuntime(*hosts(),WorkflowUiSettings(WS,CONV))
        with self.assertRaises(WorkflowUiError) as error:runtime.evidence_readiness(HEADERS)
        self.assertEqual(error.exception.code,'CHAT_EVIDENCE_SERVICE_NOT_INSTALLED')

    def test_durable_chat_never_falls_back_to_local_model_when_gate_withholds(self):
        service=Mock();runtime=Mock()
        state=WorkflowSessionState(workspace_id=WS,conversation_id=CONV,history_loaded=True)
        runtime.load_history.return_value=state
        runtime.evidence_readiness.return_value=EvidenceChatReadiness(
            ('CHAT_EVIDENCE_PROMPT_NOT_ACTIVE',),(('campaign-evidence-intent','DRAFT'),))
        with patch.dict('os.environ',{DURABLE_UI_ENV:'true'}),patch(
                'src.ui.campaign_chat.CampaignChatService',return_value=service),patch(
                'src.ui.campaign_chat.get_workflow_runtime',return_value=runtime):
            app=AppTest.from_file(str(Path(__file__).resolve().parents[1]/'app.py')).run()
            self.assertFalse(app.exception)
            self.assertTrue(app.chat_input[0].disabled)
            self.assertIn('prompt evaluation',app.info[-1].value)
        service.catalog.assert_not_called();service.interpret.assert_not_called();service.generate.assert_not_called()
        runtime.evidence_readiness.assert_called_once()

    def test_unavailable_evidence_gate_hides_provider_details_and_disables_input(self):
        runtime=Mock();runtime.load_history.return_value=WorkflowSessionState(
            workspace_id=WS,conversation_id=CONV,history_loaded=True)
        runtime.evidence_readiness.side_effect=WorkflowUiError('PRIVATE_PROVIDER_DETAIL')
        service=Mock()
        with patch.dict('os.environ',{DURABLE_UI_ENV:'true'}),patch(
                'src.ui.campaign_chat.CampaignChatService',return_value=service),patch(
                'src.ui.campaign_chat.get_workflow_runtime',return_value=runtime):
            app=AppTest.from_file(str(Path(__file__).resolve().parents[1]/'app.py')).run()
            self.assertFalse(app.exception);self.assertTrue(app.chat_input[0].disabled)
            self.assertNotIn('PRIVATE_PROVIDER_DETAIL',str([x.value for x in app.info]))
        service.interpret.assert_not_called();service.generate.assert_not_called()

    def test_trusted_runtime_preparation_forwards_only_iap_and_fixed_scope(self):
        from src.application.campaign_chat_evidence_service import ProtectedEvidenceChatService
        runtime=WorkflowUiRuntime(*hosts(),WorkflowUiSettings(WS,CONV))
        # Trusted server composition; no request-supplied service installation.
        service=Mock(spec=ProtectedEvidenceChatService)
        runtime._evidence_service=service
        expected=object();service.prepare_evidence.return_value=expected
        result=runtime.prepare_evidence(HEADERS,request_id='request',observed_interpreter_response='observed')
        self.assertIs(result,expected)
        self.assertEqual(service.prepare_evidence.call_args.args[0],
            {'x-goog-iap-jwt-assertion':HEADERS['x-goog-iap-jwt-assertion']})
        self.assertEqual(service.prepare_evidence.call_args.kwargs,dict(workspace_id=WS,
            conversation_id=CONV,request_id='request',observed_interpreter_response='observed'))
        service.prepare_evidence.side_effect=RuntimeError('provider secret')
        with self.assertRaises(WorkflowUiError) as error:
            runtime.prepare_evidence(HEADERS,request_id='request',observed_interpreter_response='observed')
        self.assertEqual(error.exception.code,'CHAT_EVIDENCE_PREPARATION_UNAVAILABLE')
