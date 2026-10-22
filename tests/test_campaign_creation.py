from dataclasses import replace
from types import SimpleNamespace
import hashlib,io
from unittest import TestCase
from unittest.mock import Mock,patch
from PIL import Image
from src.application.campaign_creation import *
from src.application.access_policy import Reason


class FixturePipeline(CampaignPipeline):
    def products(self,*a,**kw):return self.product_rows
    def admit(self,*a,**kw):return self.accept(**kw)
    def job(self,*a,**kw):return self.job_row
    def jobs(self,*a,**kw):return (self.job_row,)
    def recover_admission(self,*a,**kw):return self.original_reader(**kw)
    def artifact(self,*a,**kw):return self.image_reader(**kw)


class CampaignCreationTests(TestCase):
    def setUp(self):
        self.verifier=object.__new__(IapIdentityAdapter)
        self.identity=SimpleNamespace(principal=SimpleNamespace(principal_id='owner'))
        self.verifier.authenticate=Mock(return_value=self.identity)
        self.pipeline=FixturePipeline()
        self.product=CampaignProduct('CI-SKU-0022','Rejuvenate Restorative Face Oil','US','META','NEW','READY',(),'a'*64)
        self.pipeline.product_rows=(self.product,)
        self.job=CampaignJob('job','CI-SKU-0022',hashlib.sha256(b'Create a campaign').hexdigest(),'ACCEPTED','QUEUED')
        self.pipeline.accept=Mock(return_value=self.job);self.pipeline.job_row=self.job
        self.pipeline.original_reader=Mock(return_value=self.job)
        self.pipeline.image_reader=Mock(side_effect=ValueError('Release not allowed'))
        self.reader=Mock()
        self.generation_access=AccessDecision(True,Action.GENERATE_DRAFT,Reason.ALLOWED_WORKSPACE_ACTION,
            'workspace','owner',1,None,None)
        self.reader.authorize.return_value=self.generation_access
        self.service=ProtectedCampaignCreation(verifier=self.verifier,access_reader=self.reader,pipeline=self.pipeline)
        self.access=patch('src.application.campaign_creation._access',return_value='current-access').start()
        self.addCleanup(patch.stopall)
        self.scope=dict(workspace_id='workspace',conversation_id='conversation')

    def submit(self,**changes):
        return self.service.submit({},**(dict(self.scope,sku=self.product.sku,message='Create a campaign',retry_key='key')|changes))

    def test_catalog_and_exact_message_are_forwarded_with_server_readiness(self):
        self.assertEqual(self.service.products({},**self.scope),(self.product,))
        self.assertEqual(self.submit(),self.job)
        kw=self.pipeline.accept.call_args.kwargs
        self.assertEqual(kw['product'],self.product);self.assertEqual(kw['message'],'Create a campaign')
        self.assertEqual(kw['retry_key'],'key');self.assertEqual(kw['workspace_id'],'workspace')

    def test_cold_history_and_original_admission_recovery_never_submit(self):
        self.assertEqual(self.service.jobs({},**self.scope),(self.job,))
        self.assertEqual(self.service.recover({},**self.scope,sku=self.product.sku,
            message='Create a campaign',retry_key='key'),self.job)
        self.pipeline.accept.assert_not_called()
        self.pipeline.product_rows=()
        self.assertEqual(self.service.recover({},**self.scope,sku=self.product.sku,
            message='Create a campaign',retry_key='key'),self.job)
        self.pipeline.original_reader.return_value=None
        self.assertIsNone(self.service.recover({},**self.scope,sku=self.product.sku,message='Create a campaign',retry_key='key'))

    def test_missing_unready_and_changed_scope_cannot_admit(self):
        for state in ('PREPARING','NEEDS_EVIDENCE','NEEDS_REVIEW','UNAVAILABLE'):
            self.pipeline.product_rows=(replace(self.product,state=state,reasons=('IMAGE_BINDING_REQUIRED',)),)
            with self.assertRaises(ValueError):self.submit()
        self.pipeline.product_rows=()
        with self.assertRaises(ValueError):self.submit()
        self.pipeline.accept.assert_not_called()

    def test_false_readiness_shapes_and_duplicate_catalog_deny(self):
        with self.assertRaises(ValueError):replace(self.product,state='READY',reasons=('CLAIM_REVIEW_REQUIRED',))
        with self.assertRaises(ValueError):replace(self.product,state='NEEDS_REVIEW',reasons=())
        self.pipeline.product_rows=(self.product,self.product)
        with self.assertRaises(ValueError):self.submit()
        self.pipeline.accept.assert_not_called()

    def test_read_access_alone_does_not_allow_generation(self):
        self.reader.authorize.return_value=replace(self.generation_access,allowed=False,reason=Reason.ROLE_REQUIRED)
        with self.assertRaises(ValueError):self.submit()
        self.pipeline.accept.assert_not_called()
        self.reader.authorize.side_effect=[self.generation_access,replace(self.generation_access,membership_revision=2)]
        with self.assertRaises(ValueError):self.submit()
        self.pipeline.accept.assert_called_once()

    def test_revocation_or_invalid_prompt_denies_before_admission(self):
        self.access.side_effect=ValueError('Revoked')
        with self.assertRaises(ValueError):self.submit()
        self.access.side_effect=None
        for message in ('','   ','x'*2001,None):
            with self.assertRaises(ValueError):self.submit(message=message)
        self.pipeline.accept.assert_not_called()

    def test_wrong_receipt_or_post_admission_revocation_withholds_response(self):
        self.pipeline.accept.return_value=replace(self.job,sku='CI-SKU-0023')
        with self.assertRaises(ValueError):self.submit()
        self.pipeline.accept.return_value=self.job
        self.access.side_effect=['a','a','a','a','changed']
        with self.assertRaises(ValueError):self.submit()

    def test_completed_job_still_requires_authoritative_artifact_release(self):
        with self.assertRaises(ValueError):self.service.artifact({},**self.scope,job_id='job')
        self.pipeline.image_reader.assert_not_called()
        self.pipeline.job_row=replace(self.job,state='SUCCEEDED',stage='COMPLETE')
        with self.assertRaises(ValueError):self.service.artifact({},**self.scope,job_id='job')
        self.pipeline.image_reader.assert_called_once()

    def test_artifact_hash_dimensions_scope_and_current_access_are_checked(self):
        image=Image.new('RGB',(16,20),'white');buffer=io.BytesIO();image.save(buffer,format='PNG');raw=buffer.getvalue()
        artifact=CampaignImage('job','CI-SKU-0022',raw,hashlib.sha256(raw).hexdigest(),16,20,'Bound source evidence')
        for changed in (dict(content_sha256='0'*64),dict(width=17),dict(png=b'broken')):
            with self.assertRaises(ValueError):replace(artifact,**changed)
        self.pipeline.job_row=replace(self.job,state='SUCCEEDED',stage='COMPLETE')
        self.pipeline.image_reader.side_effect=None;self.pipeline.image_reader.return_value=artifact
        self.assertEqual(self.service.artifact({},**self.scope,job_id='job'),artifact)
        self.pipeline.image_reader.return_value=replace(artifact,sku='CI-SKU-0023')
        with self.assertRaises(ValueError):self.service.artifact({},**self.scope,job_id='job')
