from unittest import TestCase
from unittest.mock import Mock
from src.application.workflow_submission_host import request_values
from src.application.workflow_submission_worker import execute
from src.application.interpreter_submission import InterpreterSubmissionReceipt
from src.application.operation_channel import ChannelError


class OmissionTransportTests(TestCase):
    def test_only_scoped_receipt_id_crosses_transport(self):
        params=dict(workspace_id='workspace',conversation_id='conversation',submission_id='a'*64)
        self.assertEqual(request_values('omission',{},params)['parameters'],params)
        for extra in ('message','role','alternative','selection','policy'):
            with self.subTest(extra=extra),self.assertRaises(ChannelError):request_values('omission',{},dict(params,**{extra:'untrusted'}))
        service=Mock();service.omission.return_value=InterpreterSubmissionReceipt('workspace','conversation','new','call','native','COMMITTED')
        result=execute(service,'omission',{},params)
        self.assertEqual(result['request_id'],'new');service.submit.assert_not_called()
