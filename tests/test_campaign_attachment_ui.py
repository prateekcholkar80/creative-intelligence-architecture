from unittest import TestCase
from unittest.mock import Mock,patch
from src.ui.campaign_attachments import show_attachment_requests,show_attachment_readiness,show_saved_attachment_requests
from src.application.campaign_attachment_service import AttachmentReceipt
from src.application.campaign_attachment_contract import attachment_requests
from tests.test_campaign_attachment_contract import plan
from src.ui.workflow_runtime import WorkflowUiError


class AttachmentUiTests(TestCase):
    def upload_file(self,name,raw):
        file=Mock();file.name=name;file.size=len(raw);file.getvalue.return_value=raw
        return file

    def test_multiple_mixed_files_are_staged_once_before_any_per_file_save(self):
        ui,runtime=self.render('PROVIDE_EVIDENCE');ui.selectbox.return_value='SUPPORTING_DOCUMENT'
        ui.file_uploader.return_value=[self.upload_file('notes.txt',b'text'),self.upload_file('data.json',b'{}')]
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_requests(runtime,'request')
        self.assertTrue(ui.file_uploader.call_args.kwargs['accept_multiple_files'])
        self.assertEqual(runtime.start_attachment_batch.call_args.kwargs['files'],(('notes.txt',b'text'),('data.json',b'{}')))
        runtime.submit_attachment.assert_not_called()
        self.assertTrue(ui.session_state['attachment_batch_running_batch_v1_test'])

    def test_oversized_file_prevents_any_partial_batch_submission(self):
        ui,runtime=self.render('PROVIDE_EVIDENCE');ui.selectbox.return_value='SUPPORTING_DOCUMENT'
        bad=self.upload_file('large.txt',b'x');bad.size=5*1024*1024+1
        ui.file_uploader.return_value=[self.upload_file('ok.txt',b'text'),bad]
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_requests(runtime,'request')
        runtime.submit_attachment.assert_not_called()

    def test_uncertain_batch_start_never_retries_or_runs_browser_file_loop(self):
        ui,runtime=self.render('PROVIDE_EVIDENCE');ui.selectbox.return_value='SUPPORTING_DOCUMENT'
        ui.file_uploader.return_value=[self.upload_file(n,b'text') for n in ('a.txt','b.txt','c.txt')]
        runtime.start_attachment_batch.side_effect=WorkflowUiError('UNKNOWN')
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_requests(runtime,'request')
        runtime.start_attachment_batch.assert_called_once();runtime.submit_attachment.assert_not_called()
        self.assertIn('inspect upload batches',ui.error.call_args.args[0])

    def test_reconnect_reads_durable_state_and_resumes_only_on_explicit_click(self):
        from src.ui.campaign_attachments import show_attachment_batches
        ui,runtime=self.render(send=False);ui.selectbox.return_value='batch_v1_test';ui.button.return_value=False
        runtime.attachment_batches.return_value=(dict(batch_id='batch_v1_test',count=2),)
        runtime.attachment_batch_state.return_value=(dict(filename='a.txt',saved=True),dict(filename='b.txt',saved=False))
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_batches(runtime,'request')
        runtime.advance_attachment_batch.assert_not_called()
        ui.button.side_effect=lambda label,**kw:label=='Resume batch'
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_batches(runtime,'request')
        runtime.advance_attachment_batch.assert_called_once_with(ui.context.headers,batch_id='batch_v1_test')
        ui.rerun.assert_called_once_with()

    def test_new_batch_focus_replaces_older_selector_on_next_render(self):
        from src.ui.campaign_attachments import show_attachment_batches
        ui,runtime=self.render(send=False);ui.selectbox.return_value='batch_v1_new';ui.button.return_value=False
        ui.session_state.update(attachment_batch_request='batch_v1_old',attachment_batch_focus_request='batch_v1_new')
        runtime.attachment_batches.return_value=(dict(batch_id='batch_v1_new',count=1),dict(batch_id='batch_v1_old',count=1))
        runtime.attachment_batch_state.return_value=(dict(filename='a.txt',saved=False),)
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_batches(runtime,'request')
        self.assertEqual(ui.session_state['attachment_batch_request'],'batch_v1_new')
        runtime.advance_attachment_batch.assert_not_called()

    def test_evaluation_binding_requires_explicit_click_on_saved_image(self):
        from src.ui.campaign_attachments import show_attachment_batches
        ui,runtime=self.render(send=False);ui.button.return_value=False
        ui.selectbox.side_effect=lambda label,*a,**kw:'batch_v1_done' if label=='Upload batch to inspect or resume' else 'a'*64
        runtime.attachment_batches.return_value=(dict(batch_id='batch_v1_done',count=2),)
        runtime.attachment_batch_state.return_value=(dict(filename='notes.txt',submission_id='b'*64,saved=True),
            dict(filename='brand.webp',submission_id='a'*64,saved=True))
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_batches(runtime,'request')
        runtime.bind_evaluation_uploads.assert_not_called()
        ui.button.side_effect=lambda label,**kw:label=='Bind selected image for pipeline evaluation'
        runtime.bind_evaluation_uploads.return_value=dict(generation_authorized=False)
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_batches(runtime,'request')
        runtime.bind_evaluation_uploads.assert_called_once_with(ui.context.headers,request_id='request',submission_ids=('a'*64,))
        runtime.advance_attachment_batch.assert_not_called();runtime.submit_attachment.assert_not_called()

    def test_real_streamlit_image_selection_never_binds_first_texture_or_document(self):
        from streamlit.testing.v1 import AppTest
        _,runtime=self.render(send=False)
        runtime.attachment_batches.return_value=(dict(batch_id='batch_v1_fixture',count=3),)
        runtime.attachment_batch_state.return_value=(
            dict(filename='texture.webp',submission_id='a'*64,saved=True),
            dict(filename='notes.txt',submission_id='b'*64,saved=True),
            dict(filename='product.webp',submission_id='c'*64,saved=True))
        runtime.bind_evaluation_uploads.return_value=dict(generation_authorized=False)
        app=AppTest.from_string("import streamlit as st\nfrom src.ui.campaign_attachments import show_attachment_batches\nshow_attachment_batches(st.session_state['r'],'request')")
        app.session_state['r']=runtime;app.run()
        self.assertFalse(app.exception)
        button=next(b for b in app.button if b.label=='Bind selected image for pipeline evaluation')
        self.assertTrue(button.disabled);runtime.bind_evaluation_uploads.assert_not_called()
        selector=next(s for s in app.selectbox if s.label=='Product image for pipeline evaluation')
        self.assertEqual(selector.options,['texture.webp','product.webp'])
        selector.set_value('c'*64).run()
        self.assertFalse(app.exception);runtime.bind_evaluation_uploads.assert_not_called()
        next(b for b in app.button if b.label=='Bind selected image for pipeline evaluation').click().run()
        self.assertFalse(app.exception)
        self.assertEqual(runtime.bind_evaluation_uploads.call_count,1)
        self.assertEqual(runtime.bind_evaluation_uploads.call_args.kwargs,
            dict(request_id='request',submission_ids=('c'*64,)))
        app.run();self.assertFalse(app.exception)
        self.assertEqual(runtime.bind_evaluation_uploads.call_count,1)
        runtime.advance_attachment_batch.assert_not_called()

    def test_invalid_image_selection_never_reaches_binding_service(self):
        from src.ui.campaign_attachments import show_attachment_batches
        ui,runtime=self.render(send=False)
        runtime.attachment_batches.return_value=(dict(batch_id='batch_v1_done',count=1),)
        runtime.attachment_batch_state.return_value=(dict(filename='product.jpg',submission_id='a'*64,saved=True),)
        ui.selectbox.side_effect=lambda label,*a,**kw:'batch_v1_done' if label=='Upload batch to inspect or resume' else 'outside-batch'
        ui.button.return_value=True
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_batches(runtime,'request')
        runtime.bind_evaluation_uploads.assert_not_called()

    def test_real_streamlit_resume_advances_once_and_survives_full_page_rerun(self):
        from streamlit.testing.v1 import AppTest
        _,runtime=self.render(send=False)
        runtime.attachment_saved_requests.return_value=(dict(request_id='request',sku='CI-SKU-0001',created_at='fixture'),)
        runtime.attachment_summaries.return_value=()
        runtime.attachment_batches.return_value=(dict(batch_id='batch_v1_fixture',count=1),)
        runtime.attachment_batch_state.return_value=(dict(filename='pending.txt',saved=False),)
        def advance(*args,**kwargs):
            runtime.attachment_batch_state.return_value=(dict(filename='pending.txt',saved=True),)
        runtime.advance_attachment_batch.side_effect=advance
        app=AppTest.from_string("import streamlit as st\nfrom src.ui.campaign_attachments import show_protected_attachments\nshow_protected_attachments(st.session_state['r'])")
        app.session_state['r']=runtime;app.run()
        app.selectbox[0].set_value('request').run()
        next(b for b in app.button if b.label=='Resume batch').click().run()
        self.assertFalse(app.exception);runtime.advance_attachment_batch.assert_called_once()
        self.assertTrue(any('All files in this batch are saved' in s.value for s in app.success))

    def test_fragment_readiness_action_rechecks_service_and_handles_revoked_access(self):
        from streamlit.testing.v1 import AppTest
        runtime=Mock(attachments_available=True)
        runtime.attachment_saved_requests.return_value=()
        runtime.verify_attachment_intake.side_effect=WorkflowUiError('ACCESS_DENIED')
        app=AppTest.from_string('''
import streamlit as st
from src.ui.campaign_attachments import show_protected_attachments
show_protected_attachments(st.session_state['attachment_runtime'])
''')
        app.session_state['attachment_runtime']=runtime
        app.run()
        self.assertFalse(app.exception)
        runtime.verify_attachment_intake.assert_not_called()
        next(b for b in app.button if b.label=='Verify protected upload intake').click().run()
        self.assertFalse(app.exception)
        runtime.verify_attachment_intake.assert_called_once()
        self.assertTrue(any('could not be verified' in e.value for e in app.error))
        runtime.submit_attachment.assert_not_called()

    def test_reconnect_can_select_persisted_request_and_read_unverified_history(self):
        ui,runtime=self.render(send=False)
        runtime.attachment_saved_requests.return_value=(dict(request_id='saved',sku='CI-SKU-0001',created_at='2026-10-17'),)
        runtime.attachment_summaries.return_value=(dict(filename='study.txt',status='UNVERIFIED',submission_id='receipt',generation_authorized=False),)
        ui.selectbox.return_value='saved'
        with patch('src.ui.campaign_attachments.st',ui):show_saved_attachment_requests(runtime)
        runtime.attachment_requests.assert_called_once_with(ui.context.headers,request_id='saved')
        self.assertIn('UNVERIFIED',ui.text.call_args.args[0]);runtime.submit_attachment.assert_not_called()
    def test_readiness_is_explicit_read_only_and_does_not_need_a_saved_intent(self):
        ui=Mock();ui.button.return_value=False;runtime=Mock(attachments_available=True)
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_readiness(runtime)
        runtime.verify_attachment_intake.assert_not_called()
        ui.button.return_value=True;runtime.verify_attachment_intake.return_value=True
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_readiness(runtime)
        runtime.verify_attachment_intake.assert_called_once()
        runtime.submit_attachment.assert_not_called()
        self.assertIn('Generation remains disabled',ui.success.call_args.args[0])

    def render(self,choice='OMIT_UNSUPPORTED_CONTENT',send=True):
        ui=Mock();ui.session_state={};ui.expander.return_value.__enter__=Mock();ui.expander.return_value.__exit__=Mock()
        ui.form.return_value.__enter__=Mock();ui.form.return_value.__exit__=Mock()
        ui.radio.return_value=choice;ui.form_submit_button.return_value=send
        runtime=Mock(attachments_available=True)
        runtime.attachment_batches.return_value=()
        runtime.start_attachment_batch.return_value='batch_v1_test'
        runtime.attachment_requests.return_value=attachment_requests(plan())
        runtime.submit_attachment.return_value=AttachmentReceipt('id','REQUEST_CHANGE_PENDING',choice)
        return ui,runtime

    def test_explicit_omission_is_scoped_and_does_not_upload_or_generate(self):
        ui,runtime=self.render()
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_requests(runtime,'request')
        call=runtime.submit_attachment.call_args.kwargs
        self.assertEqual(call['choice'],'OMIT_UNSUPPORTED_CONTENT');self.assertIsNone(call['content'])
        self.assertEqual(call['request_id'],'request');ui.file_uploader.assert_not_called()
        runtime.interpret.assert_not_called();runtime.submit.assert_not_called()

    def test_browsing_or_unavailable_requests_never_submit(self):
        ui,runtime=self.render(send=False)
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_requests(runtime,'request')
        runtime.submit_attachment.assert_not_called()
        runtime.attachment_requests.side_effect=WorkflowUiError('private diagnostic')
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_requests(runtime,'request')
        runtime.submit_attachment.assert_not_called()
        self.assertNotIn('private diagnostic',str(ui.info.call_args))

    def test_upload_content_is_forwarded_only_on_explicit_submit(self):
        ui,runtime=self.render('PROVIDE_EVIDENCE')
        ui.selectbox.return_value='SUPPORTING_DOCUMENT'
        file=Mock(name='document');file.name='study.txt';file.size=4;file.getvalue.return_value=b'text'
        ui.file_uploader.return_value=[file]
        runtime.submit_attachment.return_value=AttachmentReceipt('id','UNVERIFIED','PROVIDE_EVIDENCE')
        with patch('src.ui.campaign_attachments.st',ui):show_attachment_requests(runtime,'request')
        self.assertEqual(runtime.start_attachment_batch.call_args.kwargs['files'],(('study.txt',b'text'),))
        self.assertIn('staged privately',ui.success.call_args.args[0])

    def test_real_streamlit_form_records_omission_only_after_submit(self):
        from streamlit.testing.v1 import AppTest
        _,runtime=self.render()
        app=AppTest.from_string('''
import streamlit as st
from src.ui.campaign_attachments import show_attachment_requests
show_attachment_requests(st.session_state['attachment_runtime'],'request')
''')
        app.session_state['attachment_runtime']=runtime
        app.run()
        self.assertFalse(app.exception)
        runtime.submit_attachment.assert_not_called()
        app.radio[0].set_value('OMIT_UNSUPPORTED_CONTENT').run()
        self.assertFalse(app.exception)
        runtime.submit_attachment.assert_not_called()
        next(b for b in app.button if b.label=='Save omission request').click().run()
        self.assertFalse(app.exception)
        self.assertEqual(runtime.submit_attachment.call_count,1)
        self.assertEqual(runtime.submit_attachment.call_args.kwargs['choice'],'OMIT_UNSUPPORTED_CONTENT')
