from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import hashlib,io
from unittest import TestCase
from unittest.mock import Mock,MagicMock,patch
from PIL import Image
from streamlit.testing.v1 import AppTest

from src.application.campaign_creation import CampaignProduct,CampaignJob,CampaignImage
from src.ui.campaign_creation_chat import KEY,_jobs
from src.ui.workflow_runtime import WorkflowUiError
from src.ui.workflow_session import DURABLE_UI_ENV


class CampaignCreationChatTests(TestCase):
    def setUp(self):
        self.runtime=Mock();self.runtime.settings=SimpleNamespace(workspace_id='workspace',conversation_id='conversation')
        self.runtime.campaign_creation_available=True
        self.product=CampaignProduct('CI-SKU-0022','Rejuvenate Restorative Face Oil','US','META','NEW','READY',(),'a'*64)
        self.runtime.campaign_products.return_value=(self.product,)
        self.runtime.campaign_jobs.return_value=()
        self.message='Create an identity-only Rejuvenate campaign.'
        self.job=CampaignJob('campaign-job','CI-SKU-0022',hashlib.sha256(self.message.encode()).hexdigest(),'ACCEPTED','QUEUED')
        self.runtime.campaign_submit.return_value=self.job
        self.runtime.campaign_recover.return_value=self.job
        self.service=Mock()
        for patcher in (patch.dict('os.environ',{DURABLE_UI_ENV:'true'}),
                patch('src.ui.campaign_chat.get_workflow_runtime',return_value=self.runtime),
                patch('src.ui.campaign_chat.CampaignChatService',return_value=self.service)):
            patcher.start();self.addCleanup(patcher.stop)
        self.app=AppTest.from_file(str(Path(__file__).resolve().parents[1]/'app.py'),default_timeout=15)

    def test_single_backend_product_selected_and_no_model_dispatch_on_page_load(self):
        self.app.run();self.assertFalse(self.app.exception)
        self.assertEqual(self.app.selectbox[0].value,'CI-SKU-0022')
        self.assertFalse(self.app.chat_input[0].disabled)
        self.runtime.campaign_submit.assert_not_called();self.runtime.load_history.assert_not_called()
        self.service.catalog.assert_not_called();self.service.interpret.assert_not_called();self.service.generate.assert_not_called()

    def test_brief_submits_once_and_reruns_only_read_status(self):
        self.app.run();self.app.chat_input[0].set_value(self.message).run()
        self.assertFalse(self.app.exception);self.runtime.campaign_submit.assert_called_once()
        values=self.runtime.campaign_submit.call_args.kwargs
        self.assertEqual(values['message'],self.message);self.assertEqual(values['sku'],'CI-SKU-0022')
        self.assertEqual(set(values),{'sku','message','retry_key'})
        self.runtime.campaign_jobs.return_value=(self.job,)
        self.app.run();self.runtime.campaign_submit.assert_called_once()
        self.assertIsNone(self.app.session_state[KEY]['pending'])
        self.assertTrue(any(text.value==self.message for text in self.app.text))

    def test_missing_evidence_or_unavailable_catalog_disables_new_requests(self):
        self.runtime.campaign_products.return_value=(replace(self.product,state='NEEDS_REVIEW',reasons=('IMAGE_BINDING_REQUIRED',)),)
        self.app.run();self.assertFalse(self.app.exception);self.assertTrue(self.app.chat_input[0].disabled)
        self.assertTrue(any('protected storage and binding' in i.value for i in self.app.info))
        self.runtime.campaign_products.side_effect=WorkflowUiError('secret-provider-message')
        self.app.run();self.assertTrue(self.app.chat_input[0].disabled)
        self.assertNotIn('secret-provider-message',str([i.value for i in self.app.info]))
        self.runtime.campaign_submit.assert_not_called()

    def test_unknown_response_retains_exact_request_and_recovery_never_submits_again(self):
        self.runtime.campaign_submit.side_effect=WorkflowUiError('uncertain')
        self.app.run();self.app.chat_input[0].set_value(self.message).run()
        pending=dict(self.app.session_state[KEY]['pending'])
        self.assertTrue(self.app.chat_input[0].disabled)
        self.app.run();self.runtime.campaign_submit.assert_called_once()
        self.runtime.campaign_products.return_value=()
        next(b for b in self.app.button if b.label=='Check original campaign request').click().run()
        self.runtime.campaign_recover.assert_called_once()
        self.assertEqual(self.runtime.campaign_recover.call_args.kwargs,pending)
        self.runtime.campaign_submit.assert_called_once();self.assertIsNone(self.app.session_state[KEY]['pending'])

    def test_cold_history_shows_progress_without_new_submission(self):
        self.runtime.campaign_jobs.return_value=(replace(self.job,state='RUNNING',stage='RENDERING'),)
        self.app.run();self.assertFalse(self.app.exception)
        self.assertTrue(any(i.value=='Rendering the banner' for i in self.app.text))
        next(b for b in self.app.button if b.label=='Refresh campaign progress').click().run()
        self.runtime.campaign_submit.assert_not_called();self.runtime.campaign_artifact.assert_not_called()

    def test_succeeded_job_withheld_artifact_is_not_displayed(self):
        self.runtime.campaign_jobs.return_value=(replace(self.job,state='SUCCEEDED',stage='COMPLETE'),)
        self.runtime.campaign_artifact.side_effect=WorkflowUiError('revoked')
        self.app.run();self.assertFalse(self.app.exception)
        self.assertEqual(len(self.app.get('download_button')),0)
        self.assertTrue(any('withheld' in i.value for i in self.app.warning))
        self.assertFalse(any(i.value=='Your campaign draft is ready' for i in self.app.text))

    def test_verified_artifact_shows_draft_evidence_and_deferred_download(self):
        canvas=Image.new('RGB',(20,20),'white');buffer=io.BytesIO();canvas.save(buffer,format='PNG');raw=buffer.getvalue()
        self.runtime.campaign_jobs.return_value=(replace(self.job,state='SUCCEEDED',stage='COMPLETE'),)
        self.runtime.campaign_artifact.return_value=CampaignImage(self.job.job_id,self.job.sku,raw,
            hashlib.sha256(raw).hexdigest(),20,20,'Bound evidence and current checks')
        self.app.run();self.assertFalse(self.app.exception)
        self.assertEqual(len(self.app.get('download_button')),1)
        self.assertTrue(any(i.value=='Your campaign draft is ready' for i in self.app.text))
        self.assertTrue(any(i.value=='Bound evidence and current checks' for i in self.app.text))
        self.runtime.campaign_submit.assert_not_called()

    def test_download_uses_protected_deferred_reader_instead_of_page_image_bytes(self):
        canvas=Image.new('RGB',(20,20),'white');buffer=io.BytesIO();canvas.save(buffer,format='PNG');raw=buffer.getvalue()
        artifact=CampaignImage(self.job.job_id,self.job.sku,raw,hashlib.sha256(raw).hexdigest(),20,20,'Bound and checked evidence')
        self.runtime.campaign_jobs.return_value=(replace(self.job,state='SUCCEEDED',stage='COMPLETE'),)
        self.runtime.campaign_artifact.return_value=artifact
        fetch=Mock(side_effect=WorkflowUiError('revoked'))
        self.runtime.campaign_deferred_download.return_value=fetch
        st=MagicMock();st.context.headers={'assertion':'first'}
        _jobs(st,self.runtime)
        self.assertIs(st.download_button.call_args.args[1],fetch)
        self.runtime.campaign_deferred_download.assert_called_once_with(st.context.headers,
            job_id=self.job.job_id,sku=self.job.sku,content_sha256=artifact.content_sha256)
        with self.assertRaises(WorkflowUiError):fetch()

    def test_product_list_is_backend_driven_and_scope_changes_preserve_pending(self):
        self.runtime.campaign_products.return_value=(replace(self.product,sku='CI-SKU-0023',product_name='Another grounded product'),)
        self.app.run();self.assertEqual(self.app.selectbox[0].value,'CI-SKU-0023')
        self.runtime.settings=SimpleNamespace(workspace_id='different',conversation_id='conversation')
        self.app.run();self.assertTrue(self.app.chat_input[0].disabled)
        self.runtime.campaign_submit.assert_not_called()
