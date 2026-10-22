import hashlib,json
from datetime import datetime,timezone,timedelta
from io import BytesIO
from unittest import TestCase
from unittest.mock import Mock
from PIL import Image
from src.application.campaign_evaluation_uploads import ProtectedEvaluationUploadReader
from src.application.campaign_document_service import ProtectedDocumentService
from src.application.campaign_attachment_service import inspect_file


class EvaluationUploadTests(TestCase):
    def fixture(self):
        now=datetime.now(timezone.utc);buf=BytesIO();Image.new('RGB',(8,8),'white').save(buf,format='WEBP');raw=buf.getvalue()
        from src.application.access_policy import Principal
        actor=Mock();actor.principal=Principal('pilot');context=dict(scope=dict(request_id='request'),policy_sha256='f'*64)
        source=dict(scope=dict(request_id='request'),review_status='UNVERIFIED',eligible_for_generation=False,
            file=inspect_file('brand.webp',raw,'PRODUCT_IMAGE'),selection=dict(category='PRODUCT_IMAGE'),
            original=dict(bucket='private',object_name='original',generation='123'))
        blob=Mock();blob.download_as_bytes.return_value=raw
        docs=ProtectedDocumentService(intake=Mock(),store=Mock());docs._source=Mock(return_value=(actor,'access',context,source,blob));docs._fresh=Mock()
        docs.intake._actor.return_value=(actor,'access')
        reader=ProtectedEvaluationUploadReader(docs,clock=lambda:now)
        args=dict(workspace_id='w',conversation_id='c',request_id='request',submission_ids=('a'*64,))
        return reader,docs,source,blob,args,now

    def test_original_pins_and_request_bind_without_granting_approval(self):
        r,d,s,b,args,now=self.fixture();out=r.capture({},**args);m=json.loads(out.manifest_json)
        self.assertEqual(out.images[0][1],'123');self.assertEqual(m['sources'][0]['original_native_job_id'],'evidence_submission_v1_'+'a'*64)
        self.assertEqual(m['image_pins'][0]['sha256'],hashlib.sha256(out.images[0][3]).hexdigest())
        self.assertFalse(m['claim_approval']);self.assertFalse(m['eligible_for_generation']);self.assertFalse(m['release_allowed'])
        self.assertEqual(b.download_as_bytes.call_args.kwargs['if_generation_match'],123);d._fresh.assert_called_once()
        r.clock=lambda:now+timedelta(seconds=10)
        self.assertEqual(r.capture({},**args).sha256,out.sha256)

    def test_duplicate_bytes_keep_all_sources_but_one_image_input(self):
        r,d,s,b,args,_=self.fixture();args['submission_ids']=('a'*64,'b'*64)
        out=r.capture({},**args);m=json.loads(out.manifest_json)
        self.assertEqual(len(out.images),1);self.assertEqual(len(m['sources']),2);self.assertEqual(d._fresh.call_count,2)
        self.assertEqual(m['sources'][0]['image_asset_id'],m['sources'][1]['image_asset_id'])

    def test_changed_original_revoked_access_and_wrong_request_withhold_capture(self):
        r,d,s,b,args,_=self.fixture();b.download_as_bytes.return_value=b'changed'
        with self.assertRaises(ValueError):r.capture({},**args)
        r,d,s,b,args,_=self.fixture();d._fresh.side_effect=ValueError('revoked')
        with self.assertRaises(ValueError):r.capture({},**args)
        r,d,s,b,args,_=self.fixture();args['request_id']='different'
        with self.assertRaises(ValueError):r.capture({},**args)
        b.download_as_bytes.assert_not_called()

    def test_documents_approved_flags_duplicate_ids_and_expiry_deny(self):
        r,d,s,b,args,_=self.fixture();s['file']['content_type']='text/plain'
        with self.assertRaises(ValueError):r.capture({},**args)

        r,d,s,b,args,_=self.fixture();s['eligible_for_generation']=True
        with self.assertRaises(ValueError):r.capture({},**args)
        r,d,s,b,args,_=self.fixture();args['submission_ids']=('a'*64,'a'*64)
        with self.assertRaises(ValueError):r.capture({},**args)
        r,d,s,b,args,now=self.fixture();r.clock=Mock(side_effect=[now,now+timedelta(seconds=121)])
        with self.assertRaises(ValueError):r.capture({},**args)

    def test_binding_is_durable_metadata_only_and_replay_keeps_same_original_slot(self):
        r,d,s,b,args,_=self.fixture()
        first=r.publish_binding({},**args);second=r.publish_binding({},**args)
        self.assertEqual(first,second);self.assertFalse(first['generation_authorized'])
        sid,value=d.intake.store.record.call_args.args
        self.assertEqual(sid,'eval_upload_binding_v1_'+first['input_manifest_sha256'])
        self.assertEqual(value['binding_scope']['request_id'],'request')
        self.assertNotIn('scope',value)
        self.assertNotIn('image_bytes',value);self.assertFalse(value['claim_approval'])

    def test_revocation_during_binding_withholds_receipt(self):
        r,d,s,b,args,_=self.fixture()
        d.intake._actor.side_effect=[d.intake._actor.return_value,ValueError('revoked')]
        with self.assertRaises(ValueError):r.publish_binding({},**args)
        d.intake.store.record.assert_called_once()

    def test_binding_read_correlates_original_job_without_replaying_write(self):
        from unittest.mock import patch
        r,d,s,b,args,_=self.fixture();inputs=r.capture({},**args)
        binding='eval_upload_binding_v1_'+inputs.sha256
        d.store.query.return_value=({'record_json':inputs.manifest_json},)
        job=d.store.client.get_job.return_value;job.state='DONE';job.error_result=None
        with patch('src.application.sharing_jobs.matches_sharing_job',return_value=True) as matches:
            self.assertEqual(r.verify_binding(binding,inputs),'evidence_submission_v1_'+binding)
            self.assertEqual(matches.call_args.kwargs['job_id'],'evidence_submission_v1_'+binding)
        d.intake.store.record.assert_not_called()
        d.store.query.return_value=({'record_json':'changed'},)
        with self.assertRaises(ValueError):r.verify_binding(binding,inputs)
        d.store.query.return_value=({'record_json':inputs.manifest_json},)
        with patch('src.application.sharing_jobs.matches_sharing_job',return_value=False):
            with self.assertRaises(ValueError):r.verify_binding(binding,inputs)
