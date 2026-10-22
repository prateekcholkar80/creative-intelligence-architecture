from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock,patch
from pathlib import Path
from streamlit.testing.v1 import AppTest
from src.ui.workflow_runtime import WorkflowUiRuntime,WorkflowUiSettings
from src.application.interpreter_submission import InterpreterSubmissionReceipt
from src.application.campaign_chat_evidence_service import EvidenceChatReadiness
from src.ui.workflow_session import DURABLE_UI_ENV,WorkflowSessionState
from tests.test_workflow_runtime import hosts,WS,CONV,HEADERS
from tests.test_campaign_chat import catalog

class ProtectedIntakeTests(TestCase):
    def test_runtime_forwards_only_iap_and_fixed_server_scope(self):
        submission,history,read=hosts();submission._config=replace(submission.config,interpretation_enabled=True)
        runtime=WorkflowUiRuntime(submission,history,read,WorkflowUiSettings(WS,CONV))
        submission.interpret=Mock(return_value=InterpreterSubmissionReceipt(WS,CONV,'request','invocation','native','COMMITTED'))
        selection=dict(sku='CI-SKU-0001',country='US',channel='META',audience='Shoppers')
        runtime.interpret(HEADERS,message='Campaign',selection=selection,retry_key='turn-1')
        args=submission.interpret.call_args
        self.assertNotIn('cookie',args.args[0]);self.assertEqual(args.kwargs['workspace_id'],WS)
        self.assertEqual(args.kwargs['selection'],selection)
    def test_intake_keeps_local_model_unused_and_preserves_retry_identity(self):
        service=Mock();service.catalog.return_value=catalog()
        runtime=Mock();runtime.interpretation_available=True
        from src.application.interpreter_submission import InterpreterIntakeOptions
        runtime.intake_options.return_value=InterpreterIntakeOptions(WS,CONV,('NEW','RETURNING'),'a'*64,'v1')
        runtime.load_history.return_value=WorkflowSessionState(workspace_id=WS,conversation_id=CONV,history_loaded=True)
        runtime.question_turn.return_value=None
        runtime.evidence_readiness.return_value=EvidenceChatReadiness(('NOT_ACTIVE',),())
        runtime.interpret.return_value=InterpreterSubmissionReceipt(WS,CONV,'request','invocation','native','COMMITTED')
        with patch.dict('os.environ',{DURABLE_UI_ENV:'true'}),patch('src.ui.campaign_chat.CampaignChatService',return_value=service),patch('src.ui.campaign_chat.get_workflow_runtime',return_value=runtime):
            app=AppTest.from_file(str(Path(__file__).resolve().parents[1]/'app.py')).run()
            self.assertFalse(app.exception);self.assertTrue(app.chat_input[0].disabled)
            app.selectbox[0].select('CI-SKU-0001');app.selectbox[1].select('US');app.selectbox[2].select('META')
            app.selectbox[3].select('NEW');app.run()
            app.chat_input[0].set_value('Create campaign').run()
            self.assertFalse(app.exception);self.assertTrue(app.success)
            original=runtime.interpret.call_args.kwargs
            next(b for b in app.button if b.label=='Check original submission').click().run()
            self.assertEqual(runtime.interpret.call_args.kwargs,original)
        service.interpret.assert_not_called();service.generate.assert_not_called()
