from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import io,hashlib
from PIL import Image
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_creation import ProtectedCampaignCreation,CampaignImage
from src.application.identity import IapSettings
from src.ui.workflow_runtime import WorkflowUiRuntime,WorkflowUiSettings,WorkflowUiError
from tests.test_workflow_runtime import hosts,HEADERS,WS,CONV


class CampaignRuntimeBridgeTests(TestCase):
    def runtime(self):
        owned=hosts();service=object.__new__(ProtectedCampaignCreation)
        service.verifier=SimpleNamespace(settings=owned[0].config.iap)
        for method in ('products','jobs','job','artifact','submit','recover'):setattr(service,method,Mock(return_value=object()))
        runtime=WorkflowUiRuntime(*owned,WorkflowUiSettings(WS,CONV),campaign_creation_service=service)
        return runtime,service

    def test_fixed_scope_and_only_iap_forwarded_for_all_creation_calls(self):
        runtime,service=self.runtime();self.assertTrue(runtime.campaign_creation_available)
        for public,backend,values in (
            ('campaign_products','products',{}),('campaign_jobs','jobs',{}),
            ('campaign_job','job',dict(job_id='job')),('campaign_artifact','artifact',dict(job_id='job')),
            ('campaign_submit','submit',dict(sku='CI-SKU-0022',message='brief',retry_key='key')),
            ('campaign_recover','recover',dict(sku='CI-SKU-0022',message='brief',retry_key='key'))):
            with self.subTest(method=public):
                result=getattr(runtime,public)(HEADERS,**values)
                method=getattr(service,backend)
                self.assertIs(result,method.return_value)
                self.assertEqual(method.call_args.args[0],{'x-goog-iap-jwt-assertion':HEADERS['x-goog-iap-jwt-assertion']})
                self.assertEqual(method.call_args.kwargs,dict(workspace_id=WS,conversation_id=CONV,**values))

    def test_uninstalled_service_bad_headers_or_private_exception_deny_safely(self):
        runtime=WorkflowUiRuntime(*hosts(),WorkflowUiSettings(WS,CONV))
        self.assertFalse(runtime.campaign_creation_available)
        with self.assertRaisesRegex(WorkflowUiError,'PIPELINE_NOT_INSTALLED'):runtime.campaign_products(HEADERS)
        runtime,service=self.runtime()
        with self.assertRaises(WorkflowUiError):runtime.campaign_products({})
        service.products.assert_not_called()
        service.products.side_effect=RuntimeError('secret-token')
        with self.assertRaises(WorkflowUiError) as error:runtime.campaign_products(HEADERS)
        self.assertNotIn('secret-token',str(error.exception))

    def test_mismatched_iap_cannot_install_pipeline(self):
        owned=hosts();service=object.__new__(ProtectedCampaignCreation)
        service.verifier=SimpleNamespace(settings=IapSettings('/projects/999/locations/asia-south1/services/other'))
        with self.assertRaises(ValueError):WorkflowUiRuntime(*owned,WorkflowUiSettings(WS,CONV),campaign_creation_service=service)

    def test_deferred_read_runs_without_streamlit_context_and_rechecks_access(self):
        runtime,service=self.runtime()
        image=Image.new('RGB',(8,8),'white');buffer=io.BytesIO();image.save(buffer,format='PNG');raw=buffer.getvalue()
        artifact=CampaignImage('job','CI-SKU-0022',raw,hashlib.sha256(raw).hexdigest(),8,8,'Evidence')
        service.artifact.return_value=artifact
        fetch=runtime.campaign_deferred_download(HEADERS,job_id='job',sku=artifact.sku,content_sha256=artifact.content_sha256)
        service.artifact.assert_not_called()
        with ThreadPoolExecutor(max_workers=1) as executor:self.assertEqual(executor.submit(fetch).result(),raw)
        self.assertEqual(service.artifact.call_args.args[0],{'x-goog-iap-jwt-assertion':HEADERS['x-goog-iap-jwt-assertion']})
        service.artifact.side_effect=ValueError('expired or revoked')
        with self.assertRaises(WorkflowUiError):fetch()
        service.artifact.side_effect=None;service.artifact.return_value=replace(artifact,sku='CI-SKU-0023')
        with self.assertRaises(WorkflowUiError):fetch()
