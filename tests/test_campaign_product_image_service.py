from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock
import hashlib,io,json,time
import PIL
from PIL import Image,ImageDraw
from google.cloud import bigquery

from src.application import campaign_attachment_service as attachments
from src.application.campaign_document_service import ProtectedDocumentService
from src.application.campaign_evaluation_uploads import EvaluationUploadInputs,VERSION as INPUT_VERSION
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_product_image_derivation import ImageDerivationSettings,prepare_derivation,canonical_rgb_png,PNG_ENCODING
from src.application.campaign_product_image_service import ProtectedProductImage
from src.application.operation_budget import worker_operation_scope


def fixture():
    now=datetime.now(timezone.utc);sid='a'*64
    canvas=Image.new('RGB',(100,120),'white');draw=ImageDraw.Draw(canvas)
    draw.rectangle((25,15,65,105),fill='orange');draw.rectangle((80,0,99,20),fill='red')
    buffer=io.BytesIO();canvas.save(buffer,format='PNG');raw=buffer.getvalue();digest=hashlib.sha256(raw).hexdigest()
    crop=canvas.crop((20,10,75,110));png=io.BytesIO(canonical_rgb_png(crop))
    scope=dict(workspace_id='workspace',conversation_id='conversation',request_id='request',sku='CI-SKU-0022',
        country='US',channel='META',audience='NEW')
    original=dict(bucket='private-fixture',object_name='campaign-evidence/'+sid,generation='123')
    source=dict(scope=scope,review_status='UNVERIFIED',eligible_for_generation=False,
        original=original,file=dict(content_sha256=digest))
    pin=dict(asset_id='asset',generation='123',mime_type='image/png',byte_count=len(raw),sha256=digest)
    parent=dict(submission_id=sid,source_record_sha256=sha(source),image_asset_id='asset',original=original)
    context=dict(scope=scope,policy_sha256='b'*64)
    manifest=dict(version=INPUT_VERSION,use='UNVERIFIED_VISUAL_REFERENCE_FOR_EVALUATION_ONLY',principal_id='owner',
        context=context,sources=[parent],image_pins=[pin],claim_approval=False,eligible_for_generation=False,
        qualifying=False,release_allowed=False)
    inputs=EvaluationUploadInputs(canonical(manifest),(('asset','123','image/png',raw),),now,now,now+timedelta(seconds=120))
    decision=dict(version='private-product-image-transform-decision-v1',decision='EXPLICIT_USER_PRODUCT_ONLY_CROP',
        source_binding_id='eval_upload_binding_v1_'+inputs.sha256,source_submission_id=sid,source_sha256=digest,
        source_dimensions=[100,120],crop_box=[20,10,75,110],output_sha256=hashlib.sha256(png.getvalue()).hexdigest(),
        crop_decoded_rgb_sha256=hashlib.sha256(crop.tobytes()).hexdigest(),png_encoding=PNG_ENCODING,
        pillow_version=PIL.__version__,scope={k:scope[k] for k in ('sku','country','channel','audience')},
        claim_approval=False,release_allowed=False)
    settings=ImageDerivationSettings(canonical(decision),sha(decision),True)
    actor=SimpleNamespace(principal=SimpleNamespace(principal_id='owner'))
    docs=ProtectedDocumentService(intake=Mock(),store=Mock())
    docs.intake._actor.return_value=(actor,'access')
    docs._source=Mock(return_value=(actor,'access',context,source,None))
    docs.intake.store.bucket='private-fixture'
    records={};objects={}
    docs.store.query.side_effect=lambda sql,params: (() if params[0].value not in records else ({'record_json':canonical(records[params[0].value])},))
    def put(key,data,file):
        objects.setdefault(key,data);assert objects[key]==data
        return dict(bucket='private-fixture',object_name='campaign-evidence/'+key,generation='456')
    docs.intake.store.put_original.side_effect=put
    docs.intake.store.record.side_effect=lambda key,value:records.setdefault(key,deepcopy(value))
    blob=docs.intake.store.storage.bucket.return_value.blob.return_value
    blob.generation='456';blob.content_type='image/png';blob.size=len(png.getvalue())
    blob.download_as_bytes.side_effect=lambda **kw:next(iter(objects.values()))
    docs.store.client.project=attachments.PROJECT
    def native(jid,**kw):
        key=jid.removeprefix('evidence_submission_v1_')
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,query_parameters=[
                bigquery.ScalarQueryParameter('id','STRING',key),
                bigquery.ScalarQueryParameter('record','STRING',canonical(records[key]))])
        config=cfg.to_api_repr();config['query']['query']=attachments.SQL
        return bigquery.QueryJob.from_api_repr(dict(jobReference=dict(projectId=attachments.PROJECT,
            location=attachments.LOCATION,jobId=jid),configuration=config,status=dict(state='DONE')),docs.store.client)
    docs.store.client.get_job.side_effect=native
    service=ProtectedProductImage(docs,settings,clock=lambda:now)
    service.reader=Mock();service.reader.capture.return_value=inputs
    return service,docs,records,objects,inputs


class ProductImageServiceTests(TestCase):
    def test_canonical_png_preserves_pixels_across_multiple_stored_blocks(self):
        from unittest.mock import patch
        image=Image.frombytes('RGB',(460,760),bytes(range(256))*4096+bytes(range(224)))
        with patch.object(Image.Image,'save',side_effect=AssertionError('Platform encoder must not run')):
            encoded=canonical_rgb_png(image)
        self.assertEqual(hashlib.sha256(encoded).hexdigest(),'5ca04552bcdb12b809a60970c2474282e8696a9ad80bf13229972b5d1a183ccf')
        with Image.open(io.BytesIO(encoded)) as decoded:
            self.assertEqual(decoded.tobytes(),image.tobytes());self.assertEqual(decoded.size,image.size)
        self.assertEqual(encoded,canonical_rgb_png(image))
    def work(self,fn):
        with worker_operation_scope(time.monotonic()+60):fn()

    def call(self,service,**kw):
        return service.prepare({},workspace_id='workspace',conversation_id='conversation',request_id='request',**kw)

    def test_exact_crop_saved_separately_and_cold_recovery_does_not_write(self):
        def run():
            service,docs,records,objects,inputs=fixture()
            first=self.call(service)
            cold=ProtectedProductImage(docs,service.settings,clock=service.clock);cold.reader=service.reader
            self.assertEqual(self.call(cold,recover_only=True),first)
            docs.intake.store.put_original.assert_called_once();docs.intake.store.record.assert_called_once()
            value=next(iter(records.values()))
            self.assertEqual(value['parent_manifest_sha256'],inputs.sha256)
            self.assertNotIn('scope',value);self.assertNotIn('file',value)
            self.assertTrue(all(value[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')))
            self.assertFalse(first['generation_authorized']);self.assertEqual((first['width'],first['height']),(55,100))
            capture=cold.capture_bound({},workspace_id='workspace',conversation_id='conversation',request_id='request')
            self.assertEqual(capture.raw,objects[first['binding_id']])
        self.work(run)

    def test_missing_recovery_never_uploads_or_publishes(self):
        def run():
            service,docs,*_=fixture()
            with self.assertRaisesRegex(ValueError,'ORIGINAL_MISSING'):self.call(service,recover_only=True)
            docs.intake.store.put_original.assert_not_called();docs.intake.store.record.assert_not_called()
        self.work(run)

    def test_lost_committed_reply_recovers_same_record_and_object(self):
        def run():
            service,docs,records,objects,_=fixture();publish=docs.intake.store.record.side_effect
            def lose(*args):publish(*args);raise TimeoutError('lost reply')
            docs.intake.store.record.side_effect=lose
            with self.assertRaises(TimeoutError):self.call(service)
            result=self.call(service,recover_only=True)
            self.assertEqual(len(records),1);self.assertEqual(len(objects),1)
            docs.intake.store.record.assert_called_once();self.assertIn(result['binding_id'],records)
        self.work(run)

    def test_source_policy_owner_or_access_drift_denies_before_write(self):
        def run():
            for fault in ('policy','owner','access','source','expiry'):
                service,docs,*_=fixture();actor,access,context,source,_=docs._source.return_value
                if fault=='expiry':service.clock=lambda:service.reader.capture.return_value.valid_until
                elif fault=='owner':actor=SimpleNamespace(principal=SimpleNamespace(principal_id='other'))
                elif fault=='access':access='changed'
                elif fault=='policy':context=dict(context,policy_sha256='0'*64)
                else:source=dict(source,review_status='APPROVED')
                docs._source.return_value=(actor,access,context,source,None)
                with self.subTest(fault=fault),self.assertRaises(ValueError):self.call(service)
                docs.intake.store.put_original.assert_not_called();docs.intake.store.record.assert_not_called()
        self.work(run)

    def test_modified_output_native_job_or_generation_is_not_repaired_on_recovery(self):
        def run():
            for fault in ('record','bytes','generation','job'):
                service,docs,records,objects,_=fixture();first=self.call(service)
                if fault=='record':records[first['binding_id']]['width']=99
                elif fault=='bytes':objects[first['binding_id']]=b'changed'
                elif fault=='generation':docs.intake.store.storage.bucket.return_value.blob.return_value.generation='999'
                else:docs.store.client.get_job.side_effect=ValueError('original job unavailable')
                with self.subTest(fault=fault),self.assertRaises(ValueError):self.call(service,recover_only=True)
                docs.intake.store.put_original.assert_called_once();docs.intake.store.record.assert_called_once()
        self.work(run)

    def test_changed_transform_output_profile_or_approval_marker_deny(self):
        def run():
            service,_,_,_,inputs=fixture();decision=json.loads(service.settings.decision_json)
            for field,value in (('crop_box',[20,10,76,110]),('output_sha256','0'*64),('crop_decoded_rgb_sha256','0'*64),('png_encoding','unknown'),('pillow_version','unknown'),('claim_approval',True)):
                changed=dict(decision);changed[field]=value
                with self.subTest(field=field),self.assertRaises(ValueError):
                    settings=ImageDerivationSettings(canonical(changed),sha(changed),True)
                    prepare_derivation(inputs,settings)
        self.work(run)
