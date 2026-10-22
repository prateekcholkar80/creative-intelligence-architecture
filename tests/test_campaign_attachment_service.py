from dataclasses import replace
from datetime import datetime,timezone,timedelta
from io import BytesIO
import json
from unittest import TestCase
from unittest.mock import Mock,patch
from PIL import Image

from src.application import campaign_attachment_service as module
from src.application.campaign_evidence_gaps import GapPlan,canonical
from src.application.access_policy import AccessDecision,Action,Reason,Principal
from src.application.identity import IapIdentityAdapter
from tests.test_campaign_attachment_contract import plan


class AttachmentServiceTests(TestCase):
    def setUp(self):
        self.now=datetime.now(timezone.utc)
        self.identity=Mock(principal=Principal('pilot'))
        self.verifier=Mock(spec=IapIdentityAdapter);self.verifier.authenticate.return_value=self.identity
        self.access=Mock()
        self.read=AccessDecision(True,Action.READ_DRAFT,Reason.ALLOWED_OWNER,'workspace','pilot',1,'conversation',1)
        self.access.authorize.return_value=replace(self.read,action=Action.REFINE_DRAFT)
        self.data=json.loads(plan().canonical_json);self.data.update(checked_at=self.now.isoformat(),policy_sha256='f'*64)
        self.reader=Mock(side_effect=lambda *a,**kw:GapPlan(canonical(self.data)))
        self.store=Mock();self.store.put_original.return_value=dict(bucket='evidence',object_name='private',generation='1')
        self.service=module.ProtectedAttachmentService(verifier=self.verifier,access_reader=self.access,
            plan_reader=self.reader,store=self.store,enabled=True,clock=lambda:self.now)
        self.fresh=patch.object(module,'ensure_fresh');self.fresh.start();self.addCleanup(self.fresh.stop)
        self.accesspatch=patch.object(module,'_access',return_value=self.read);self.accesspatch.start();self.addCleanup(self.accesspatch.stop)
        self.scope=dict(workspace_id='workspace',conversation_id='conversation',request_id='request')

    def args(self):
        request,=self.service.requests({},**self.scope)
        return dict(self.scope,requirement_id='science',request_sha256=request.sha256,
            choice='PROVIDE_EVIDENCE',category='SUPPORTING_DOCUMENT',filename='study.txt',content=b'Unverified supporting evidence.')

    def test_file_record_and_explicit_omission_never_grant_approval(self):
        receipt=self.service.submit({},**self.args())
        self.assertEqual(receipt.status,'UNVERIFIED');self.assertFalse(receipt.generation_authorized)
        record=self.store.record.call_args.args[1]
        self.assertEqual(record['original']['generation'],'1')
        self.assertEqual(record['extraction_status'],'NOT_PARSED')
        self.assertFalse(record['eligible_for_generation'])
        args=self.args();args.update(choice='OMIT_UNSUPPORTED_CONTENT',category=None,filename=None,content=None)
        self.assertEqual(self.service.submit({},**args).status,'REQUEST_CHANGE_PENDING')

    def test_save_timings_correlate_stages_without_logging_private_inputs(self):
        args=self.args()
        with self.assertLogs(module._LOG,level='INFO') as logs:
            self.service.submit({'private-header':'secret'},**args)
        events=[json.loads(r.getMessage()) for r in logs.records]
        self.assertEqual(len({e['trace_id'] for e in events}),1)
        self.assertEqual(len(events),7)
        self.assertEqual(events[-1]['stage'],'total')
        self.assertTrue(all(e['succeeded'] and e['duration_ms']>=0 for e in events))
        self.assertNotIn('study.txt',str(events));self.assertNotIn('secret',str(events))
        self.assertNotIn('Unverified supporting evidence',str(events))
        self.assertIsNone(module._SAVE_TRACE.get())

    def test_failed_save_is_timed_without_retry_or_invented_receipt(self):
        args=self.args();self.store.record.side_effect=ValueError('private failure')
        with self.assertLogs(module._LOG,level='INFO') as logs,self.assertRaises(ValueError):
            self.service.submit({},**args)
        events=[json.loads(r.getMessage()) for r in logs.records]
        self.assertFalse(events[-1]['succeeded'])
        self.assertFalse(events[-2]['succeeded'])
        self.assertNotIn('private failure',str(events))
        self.store.record.assert_called_once()
        self.assertIsNone(module._SAVE_TRACE.get())

    def test_fresh_read_provenance_changes_do_not_duplicate_intent(self):
        a,=self.service.requests({},**self.scope)
        self.data['checked_at']=(self.now-timedelta(seconds=1)).isoformat()
        b,=self.service.requests({},**self.scope)
        self.assertEqual(a.sha256,b.sha256)
        args=self.args();self.service.submit({},**args);self.service.submit({},**args)
        ids=[c.args[0] for c in self.store.record.call_args_list]
        self.assertEqual(ids[0],ids[1])

    def test_revoked_write_access_has_no_storage_effect(self):
        args=self.args();self.access.authorize.return_value=replace(self.read,action=Action.REFINE_DRAFT,allowed=False)
        with self.assertRaises(ValueError):self.service.submit({},**args)
        self.store.preflight.assert_not_called();self.store.record.assert_not_called()

    def test_context_change_after_upload_withholds_record(self):
        args=self.args()
        self.store.put_original.side_effect=lambda *a: self.data['scope'].update(sku='CI-SKU-0002') or {}
        with self.assertRaisesRegex(ValueError,'ATTACHMENT_AUTHORITY_CHANGED'):self.service.submit({},**args)
        self.store.record.assert_not_called()

    def test_changed_plan_scope_policy_or_stale_capture_denies(self):
        for kind in ('scope','policy','expiry'):
            original=json.loads(canonical(self.data));args=self.args()
            if kind=='scope':self.data['scope']['workspace_id']='other'
            elif kind=='policy':self.data['policy_sha256']='e'*64
            else:self.data['checked_at']=(self.now-timedelta(seconds=61)).isoformat()
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.service.submit({},**args)
            self.data=original
        self.store.record.assert_not_called()

    def test_disabled_service_denies_before_source_reads(self):
        self.service.enabled=False
        with self.assertRaises(ValueError):self.service.requests({},**self.scope)
        self.reader.assert_not_called()

    def test_readonly_storage_readiness_needs_current_creator_but_no_intent(self):
        self.assertTrue(self.service.verify_intake({},workspace_id='workspace',conversation_id='conversation'))
        self.store.preflight.assert_called_once();self.reader.assert_not_called()
        self.store.record.assert_not_called();self.store.put_original.assert_not_called()
        def revoke():self.access.authorize.return_value=replace(self.read,action=Action.REFINE_DRAFT,allowed=False)
        self.store.preflight.side_effect=revoke
        with self.assertRaises(ValueError):self.service.verify_intake({},workspace_id='workspace',conversation_id='conversation')

    def test_history_is_principal_scoped_and_rechecked_after_read(self):
        self.store.summaries.return_value=()
        self.assertEqual(self.service.summaries({},**self.scope),())
        self.assertEqual(self.store.summaries.call_args.kwargs['principal_id'],'pilot')
        def revoke(**kw):
            self.access.authorize.return_value=replace(self.read,action=Action.REFINE_DRAFT,allowed=False)
            return ()
        self.store.summaries.side_effect=revoke
        with self.assertRaises(ValueError):self.service.summaries({},**self.scope)

    def test_type_size_and_image_identity_checks(self):
        self.assertEqual(module.inspect_file('limit.txt',b'x'*module.MAX_BYTES,'SUPPORTING_DOCUMENT')['bytes'],module.MAX_BYTES)
        for filename,content,category in [('file.docx',b'zip','PLAYBOOK'),('../x.txt',b'x','REFERENCE_DATA'),
            ('x.pdf',b'not pdf','SUPPORTING_DOCUMENT'),('x.txt',b'text','PRODUCT_IMAGE'),
            ('x.txt',b'x'*(module.MAX_BYTES+1),'REFERENCE_DATA'),('x.png',b'fake','PRODUCT_IMAGE')]:
            with self.subTest(filename=filename),self.assertRaises((ValueError,OSError)):
                module.inspect_file(filename,content,category)
        buf=BytesIO();Image.new('RGB',(2,2)).save(buf,format='PNG')
        self.assertEqual(module.inspect_file('product.png',buf.getvalue(),'PRODUCT_IMAGE')['content_type'],'image/png')

    def test_original_native_job_checked_before_submit_and_mismatch_denied(self):
        client=Mock(project=module.PROJECT,location=module.LOCATION,default_query_job_config=None)
        store=module.AttachmentStore(client=client,storage=Mock(),bucket='evidence-bucket')
        with patch.object(module,'matches_sharing_job',return_value=False):
            with self.assertRaisesRegex(ValueError,'ATTACHMENT_JOB_MISMATCH'):store.record('a'*64,{'status':'UNVERIFIED'})
        client.query.assert_not_called()

    def test_webp_original_is_hashed_without_transcoding_and_mismatch_or_animation_denies(self):
        import hashlib
        buf=BytesIO();Image.new('RGB',(8,8),'white').save(buf,format='WEBP')
        raw=buf.getvalue()
        result=module.inspect_file('brand.webp',raw,'PRODUCT_IMAGE')
        self.assertEqual(result['content_type'],'image/webp')
        self.assertEqual(result['content_sha256'],hashlib.sha256(raw).hexdigest())
        with self.assertRaisesRegex(ValueError,'ATTACHMENT_IMAGE_INVALID'):
            module.inspect_file('brand.jpg',raw,'PRODUCT_IMAGE')
        animated=BytesIO()
        Image.new('RGB',(8,8),'white').save(animated,format='WEBP',save_all=True,
            append_images=[Image.new('RGB',(8,8),'black')],duration=100,loop=0)
        with self.assertRaisesRegex(ValueError,'ATTACHMENT_IMAGE_INVALID'):
            module.inspect_file('brand.webp',animated.getvalue(),'PRODUCT_IMAGE')

    def test_single_frame_jpeg_without_n_frames_attribute_is_accepted(self):
        import hashlib
        buf=BytesIO();Image.new('RGB',(8,8),'white').save(buf,format='JPEG');raw=buf.getvalue()
        result=module.inspect_file('pack.jpg',raw,'PRODUCT_IMAGE')
        self.assertEqual(result['content_type'],'image/jpeg')
        self.assertEqual(result['content_sha256'],hashlib.sha256(raw).hexdigest())

    def test_public_or_nonregional_bucket_denies_before_upload(self):
        client=Mock(project=module.PROJECT,location=module.LOCATION,default_query_job_config=None)
        storage=Mock();bucket=storage.get_bucket.return_value
        store=module.AttachmentStore(client=client,storage=storage,bucket='evidence-bucket')
        with patch.object(module,'validate_table'):
            bucket.location=module.LOCATION
            bucket.iam_configuration.uniform_bucket_level_access_enabled=True
            bucket.iam_configuration.public_access_prevention='inherited'
            with self.assertRaisesRegex(ValueError,'ATTACHMENT_PRIVATE_BUCKET_REQUIRED'):store.preflight()
            bucket.iam_configuration.public_access_prevention='enforced';store.preflight()
            bucket.location='US'
            with self.assertRaisesRegex(ValueError,'ATTACHMENT_BUCKET_LOCATION'):store.preflight()
        storage.bucket.assert_not_called()
