from unittest import TestCase
from unittest.mock import Mock
from src.ui.workflow_runtime import WorkflowUiRuntime,WorkflowUiSettings,WorkflowUiError
from src.application.identity import ASSERTION_HEADER


class AttachmentRuntimeTests(TestCase):
    def test_evaluation_binding_uses_trusted_scope_and_no_raw_header_passthrough(self):
        from unittest.mock import patch
        runtime=self.runtime();runtime._document_service=Mock()
        with patch('src.application.campaign_evaluation_uploads.ProtectedEvaluationUploadReader') as reader:
            runtime.bind_evaluation_uploads({ASSERTION_HEADER:'signed','untrusted':'ADMIN'},request_id='request',submission_ids=('a'*64,))
        call=reader.return_value.publish_binding.call_args
        self.assertEqual(call.args,({ASSERTION_HEADER:'signed'},))
        self.assertEqual(call.kwargs['workspace_id'],'trusted-workspace')
        self.assertEqual(call.kwargs['conversation_id'],'trusted-conversation')

    def test_batch_operations_keep_trusted_scope_and_strip_untrusted_headers(self):
        runtime=self.runtime();runtime._attachment_batches=Mock()
        headers={ASSERTION_HEADER:'signed','untrusted-role':'ADMIN'}
        runtime.start_attachment_batch(headers,request_id='request',requirement_id='science',
            request_sha256='a'*64,category='SUPPORTING_DOCUMENT',files=(('a.txt',b'a'),))
        call=runtime._attachment_batches.start.call_args
        self.assertEqual(call.args,({ASSERTION_HEADER:'signed'},))
        self.assertEqual(call.kwargs['workspace_id'],'trusted-workspace')
        self.assertEqual(call.kwargs['conversation_id'],'trusted-conversation')
        runtime._attachment_batches.advance.side_effect=RuntimeError('private credential')
        with self.assertRaisesRegex(WorkflowUiError,'^ATTACHMENT_BATCH_UNVERIFIED$'):
            runtime.advance_attachment_batch(headers,batch_id='batch')

    def runtime(self):
        runtime=object.__new__(WorkflowUiRuntime)
        runtime.settings=WorkflowUiSettings('trusted-workspace','trusted-conversation')
        runtime._attachment_service=Mock(enabled=True)
        return runtime

    def test_scope_and_assertion_are_from_trusted_runtime_and_request(self):
        runtime=self.runtime();headers={ASSERTION_HEADER:'signed-assertion','untrusted-role':'ADMIN'}
        runtime.attachment_requests(headers,request_id='request')
        args=runtime._attachment_service.requests.call_args
        self.assertEqual(args.args,({ASSERTION_HEADER:'signed-assertion'},))
        self.assertEqual(args.kwargs,dict(workspace_id='trusted-workspace',conversation_id='trusted-conversation',request_id='request'))
        runtime.submit_attachment(headers,request_id='request',requirement_id='science',request_sha256='a'*64,
            choice='OMIT_UNSUPPORTED_CONTENT')
        self.assertEqual(runtime._attachment_service.submit.call_args.kwargs['workspace_id'],'trusted-workspace')

    def test_disabled_or_invalid_identity_never_dispatches(self):
        runtime=self.runtime();runtime._attachment_service.enabled=False
        with self.assertRaises(WorkflowUiError):runtime.attachment_requests({},request_id='request')
        runtime._attachment_service.requests.assert_not_called()
        runtime._attachment_service.enabled=True
        with self.assertRaises(WorkflowUiError):runtime.attachment_requests({},request_id='request')
        runtime._attachment_service.requests.assert_not_called()

    def test_errors_do_not_expose_provider_or_assertion_details(self):
        runtime=self.runtime();runtime._attachment_service.submit.side_effect=RuntimeError('provider credential secret')
        with self.assertRaisesRegex(WorkflowUiError,'^ATTACHMENT_SUBMISSION_UNVERIFIED$'):
            runtime.submit_attachment({ASSERTION_HEADER:'assertion'},request_id='request',requirement_id='science',
                request_sha256='a'*64,choice='PROVIDE_EVIDENCE')

    def test_contract_error_code_reaches_safe_user_message(self):
        from src.application.workflow_contract import ContractError
        runtime=self.runtime();runtime._attachment_service.submit.side_effect=ContractError('ATTACHMENT_FORMAT_UNSUPPORTED')
        with self.assertRaisesRegex(WorkflowUiError,'^ATTACHMENT_FORMAT_UNSUPPORTED$'):
            runtime.submit_attachment({ASSERTION_HEADER:'assertion'},request_id='request',requirement_id='science',
                request_sha256='a'*64,choice='PROVIDE_EVIDENCE')

    def test_readonly_intake_check_uses_trusted_scope(self):
        runtime=self.runtime();runtime._attachment_service.verify_intake.return_value=True
        self.assertTrue(runtime.verify_attachment_intake({ASSERTION_HEADER:'assertion'}))
        self.assertEqual(runtime._attachment_service.verify_intake.call_args.kwargs,
            dict(workspace_id='trusted-workspace',conversation_id='trusted-conversation'))
        runtime._attachment_service.verify_intake.return_value=False
        with self.assertRaises(WorkflowUiError):runtime.verify_attachment_intake({ASSERTION_HEADER:'assertion'})
