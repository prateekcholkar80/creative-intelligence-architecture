from dataclasses import asdict
from unittest import TestCase
from unittest.mock import Mock
from src.application.workflow_submission_host import WorkflowSubmissionConfig,WorkflowSubmissionHost,request_values,config_from_wire
from src.application.workflow_submission_worker import execute
from src.application.interpreter_submission import InterpreterSubmissionReceipt
from src.application.identity import IapSettings
from src.application.sharing_operation_host import SharingHostError
from tests.test_identity import AUDIENCE
from tests.test_grounded_interpreter_submission import SELECT

class InterpreterHostTests(TestCase):
    def params(self):return dict(workspace_id='workspace',conversation_id='conversation',message='Create campaign',retry_key='turn-1',selection=dict(SELECT))
    def test_default_disabled_and_roundtrip(self):
        cfg=WorkflowSubmissionConfig(IapSettings(AUDIENCE))
        self.assertEqual(config_from_wire(asdict(cfg)),cfg)
        with self.assertRaises(SharingHostError):WorkflowSubmissionHost(cfg).interpret({},**self.params())
    def test_wire_rejects_browser_authority_and_missing_selection(self):
        self.assertEqual(request_values('interpret',{},self.params())['parameters'],self.params())
        for extra in ({'principal_id':'untrusted'},{'selection':{}},{'message':''},{'retry_key':'x'*129}):
            with self.assertRaises(Exception):request_values('interpret',{},self.params()|extra)
    def test_worker_routes_only_to_protected_submission_and_minimal_receipt(self):
        receipt=InterpreterSubmissionReceipt('workspace','conversation','request','invocation','native','COMMITTED')
        service=Mock();service.submit.return_value=receipt
        self.assertEqual(execute(service,'interpret',{},self.params()),asdict(receipt))
        service.submit.assert_called_once_with({},**self.params())
        host=WorkflowSubmissionHost(WorkflowSubmissionConfig(IapSettings(AUDIENCE)))
        self.assertEqual(host._decode_result('interpret',asdict(receipt)),receipt)
        with self.assertRaises(Exception):host._decode_result('interpret',asdict(receipt)|{'release_allowed':True})
