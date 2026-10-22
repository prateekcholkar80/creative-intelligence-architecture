from unittest import TestCase
from unittest.mock import Mock,patch
from src.ui.campaign_document_controls import show_processing_controls
from src.application.campaign_document_service import DocumentReceipt


class DocumentControlsTests(TestCase):
    def test_rerun_is_readonly_and_extract_requires_explicit_click(self):
        ui=Mock();left,right=Mock(),Mock();ui.columns.return_value=(left,right)
        left.button.return_value=right.button.return_value=False;runtime=Mock()
        row=dict(submission_id='a'*64,filename='doc.txt',status='UNVERIFIED')
        with patch('src.ui.campaign_document_controls.st',ui):show_processing_controls(runtime,row)
        runtime.process_document.assert_not_called()
        left.button.return_value=True
        runtime.process_document.return_value=DocumentReceipt('b'*64,'a'*64,'EXTRACTED_UNVERIFIED',100,1,'NEEDS_REVIEW',('SOURCE_REVIEW_REQUIRED',))
        with patch('src.ui.campaign_document_controls.st',ui):show_processing_controls(runtime,row)
        self.assertTrue(runtime.process_document.call_args.kwargs['extract'])
        self.assertIn('not used for generation',ui.info.call_args.args[0])

    def test_omission_is_an_explicit_new_turn_and_never_file_extraction(self):
        ui=Mock();ui.button.return_value=False;runtime=Mock()
        row=dict(submission_id='a'*64,filename=None,status='REQUEST_CHANGE_PENDING')
        with patch('src.ui.campaign_document_controls.st',ui):show_processing_controls(runtime,row)
        runtime.apply_omission.assert_not_called()
        ui.button.return_value=True;runtime.apply_omission.return_value=Mock(native_status='COMMITTED',request_id='new-turn')
        with patch('src.ui.campaign_document_controls.st',ui):show_processing_controls(runtime,row)
        runtime.apply_omission.assert_called_once();runtime.process_document.assert_not_called()
        self.assertIn('Reevaluation',ui.success.call_args.args[0])
