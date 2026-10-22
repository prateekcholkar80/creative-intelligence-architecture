from copy import deepcopy
import hashlib,json,time
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_corpus_preparation import ProtectedCorpusPreparation,CorpusPreparationSettings,canonical,digest,PREFIX
from src.application.campaign_evaluation_uploads import EvaluationUploadInputs
from src.application.operation_budget import worker_operation_scope
from tests import test_upload_corpus as fixtures


class SavedCorpusReceiptTests(TestCase):
    def fixture(self):
        reader,docs,extracted,source,blob,args=fixtures.UploadCorpusTests().fixture()
        image=deepcopy(json.loads(reader.images.capture.return_value.manifest_json));image_id='a'*64
        image_source=dict(scope=source['scope'],principal_id='owner',original=dict(bucket='private',object_name='campaign-evidence/'+image_id,generation='12'),
            file=dict(content_sha256=hashlib.sha256(b'image').hexdigest(),content_type='image/webp'),review_status='UNVERIFIED',eligible_for_generation=False)
        image['sources']=[dict(submission_id=image_id,source_record_sha256=digest(image_source))]
        old=reader.images.capture.return_value
        reader.images.capture.return_value=EvaluationUploadInputs(canonical(image),old.images,old.observed_at,old.checked_at,old.valid_until)
        args['image_binding_id']='eval_upload_binding_v1_'+digest(image)
        full=reader.capture({},**args);manifest=json.loads(full.manifest_json)
        scope={k:args[k] for k in ('workspace_id','conversation_id','request_id','sku','country','channel')}
        sources={image_id:image_source,extracted['submission_id']:source}
        selected=dict(version='private-evaluation-source-selection-v1',scope=scope,image_binding_id=args['image_binding_id'],sources=[dict(
            submission_id=sid,content_sha256=v['file']['content_sha256'],content_type=v['file']['content_type'],generation=v['original']['generation'],
            role='BOUND_IMAGE' if sid==image_id else 'DOCUMENT') for sid,v in sources.items()])
        settings=CorpusPreparationSettings('saved-corpus',canonical(selected),digest(selected),True)
        metadata=dict(manifest,text_chunks=[{k:v for k,v in c.items() if k!='text'} for c in manifest['text_chunks']],
            text_materialization='RECONSTRUCT_FROM_EXACT_STORED_EXTRACTION')
        record=dict(version='private-upload-corpus-binding-v1',run_id=settings.run_id,selection_sha256=settings.selection_sha256,
            corpus_sha256=full.sha256,corpus=metadata,binding_scope={k:scope[k] for k in ('workspace_id','conversation_id','request_id')},
            principal_id='owner',claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)
        bid=PREFIX+digest(record);records={bid:record,args['image_binding_id']:image}
        actor,access,context=docs._source.return_value[:3];docs._capture=Mock(return_value=(actor,access,context))
        docs.intake.store.bucket='private';docs.store.submission.side_effect=lambda sid,**_:sources[sid]
        docs.store.query.side_effect=lambda sql,params:((dict(source_count=4,source_matches=4,extraction_count=1,extraction_matches=1),)
            if sql.startswith('WITH source_pins') else (dict(record_json=canonical(records[params[0].value])),))
        def generation(name,**kw):return Mock(generation=str(kw['generation']))
        docs.intake.store.storage.bucket.return_value.blob.side_effect=generation
        service=ProtectedCorpusPreparation(docs,settings,clock=reader.clock);service.reader=reader;service._verify_binding=Mock(return_value=True)
        blob.download_as_bytes.reset_mock();docs._source.reset_mock();reader.capture=Mock(side_effect=AssertionError('Full capture repeated'))
        return service,dict(binding_id=bid,corpus_sha256=full.sha256),docs,blob,full,extracted,records

    def work(self,fn):
        with worker_operation_scope(time.monotonic()+60):fn()

    def test_exact_cold_reconstruction_has_no_original_byte_download_or_full_capture(self):
        def run():
            service,args,docs,blob,full,_,_=self.fixture();bound=service.capture_saved_bound_for_receipt({'iap':'fresh'},**args)
            self.assertEqual(bound.capture.manifest_json,full.manifest_json);self.assertEqual(bound.capture.images,())
            blob.download_as_bytes.assert_not_called();docs._source.assert_not_called();service.reader.capture.assert_not_called()
            self.assertTrue(bound.refresh());self.assertGreaterEqual(docs._capture.call_count,3)
            self.assertEqual(json.loads(bound.receipt_json)['corpus_sha256'],full.sha256)
        self.work(run)

    def test_current_policy_source_generation_and_extraction_changes_deny(self):
        for fault in ('policy','source','generation','extraction','image_binding'):
            def run():
                s,args,d,_,_,e,records=self.fixture()
                if fault=='policy':
                    actor,access,context=d._capture.return_value;d._capture.return_value=(actor,access,dict(context,policy_sha256='f'*64))
                elif fault=='source':d.store.submission.side_effect=lambda *a,**k:dict(review_status='APPROVED',eligible_for_generation=True)
                elif fault=='generation':d.intake.store.storage.bucket.return_value.blob.side_effect=lambda *a,**k:Mock(generation='999')
                elif fault=='extraction':e['extraction']['segments'][0]['text']='changed'
                else:next(v for k,v in records.items() if k.startswith('eval_upload_binding'))['image_pins']=[]
                with self.assertRaises(ValueError):s.capture_saved_bound_for_receipt({},**args)
            with self.subTest(fault=fault):self.work(run)

    def test_refresh_denies_changed_current_bound_rows(self):
        def run():
            s,args,d,*_=self.fixture();bound=s.capture_saved_bound_for_receipt({},**args)
            d.store.query.return_value=();d.store.query.side_effect=lambda *a: (dict(source_count=4,source_matches=3,extraction_count=1,extraction_matches=1),)
            with self.assertRaisesRegex(ValueError,'BOUND_RECORDS_CHANGED'):bound.refresh()
        self.work(run)
