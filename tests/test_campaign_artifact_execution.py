"""Synthetic authority tests; real local PNG rendering and pixel measurements."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone
import hashlib,json,time
from unittest import TestCase
from unittest.mock import Mock,patch

from src.application.campaign_artifact_execution import *
from src.application.campaign_evaluator_store import Reservation
from src.application.campaign_product_image_service import DerivedImageCapture
from src.application.operation_budget import worker_operation_scope
from tests.test_campaign_product_image_service import fixture as crop_fixture
from tests.test_campaign_planner_evaluation import PlannerEvaluationTests
from tests.test_source_bound_campaign_renderer import SourceBoundRendererTests


class ArtifactExecutionTests(TestCase):
    def fixture(self):
        with worker_operation_scope(time.monotonic()+60):
            crop_service,docs,records,objects,inputs=crop_fixture()
            receipt=crop_service.prepare({},workspace_id='workspace',conversation_id='conversation',request_id='request')
        crop=DerivedImageCapture(receipt['binding_id'],canonical(records[receipt['binding_id']]),objects[receipt['binding_id']],inputs.valid_until)
        renderer=SourceBoundRendererTests().fixture();planner=PlannerEvaluationTests().prepared()
        p=json.loads(planner.intent_json);source=json.loads(inputs.manifest_json)
        evidence=dict(corpus_sha256='c'*64,rerank_record_id='d'*64)
        p.update(source=source,input_binding_id='eval_upload_binding_v1_'+inputs.sha256,input_manifest_sha256=inputs.sha256,
            retrieval_evidence=evidence,retrieval_evidence_sha256=sha(evidence),options_json=renderer['options_json'])
        planner=replace(planner,intent_json=canonical(p),options_json=renderer['options_json'])
        prepared=prepare_campaign_artifact(run_id='artifact-pilot',planner=planner,crop=crop,
            product=renderer['product'],config=renderer['config'],checked_at=renderer['checked_at'])
        intents={};outcomes={};store=Mock(spec=EvaluatorStore)
        def recover(value):
            slot=value['record_id']
            if slot not in intents:return None
            base=json.loads(intents[slot]);base.pop('reservation_nonce')
            if base!=value:raise ValueError('EVALUATOR_RECOVERY_INTENT_CHANGED')
            return deepcopy(outcomes.get(slot,dict(state='UNKNOWN')))
        def reserve(value):
            slot=value['record_id'];fresh=slot not in intents
            intents.setdefault(slot,canonical(dict(value,reservation_nonce='nonce')))
            return Reservation(slot,intents[slot],fresh)
        def publish(r,value):
            self.assertEqual(value['intent_sha256'],hashlib.sha256(r.intent_json.encode()).hexdigest())
            if r.record_id in outcomes:self.assertEqual(outcomes[r.record_id],value)
            outcomes.setdefault(r.record_id,deepcopy(value))
        store.recover.side_effect=recover;store.reserve.side_effect=reserve;store.publish.side_effect=publish
        reserve(p);outcomes[p['record_id']]=dict(state='SUCCEEDED',fresh_source_check='PASS',finish_reason='STOP',
            safety_outcome='PASSED',returned_model_id=p['profile']['model_id'],response_text=renderer['planner_response'],
            response_sha256=hashlib.sha256(renderer['planner_response'].encode()).hexdigest())
        store.reserve.reset_mock()
        objects=Mock(spec=AttachmentStore);objects.bucket='private-artifacts';objects.storage=Mock();saved={}
        def put(key,raw,file):
            saved.setdefault(key,raw);self.assertEqual(saved[key],raw)
            return dict(bucket=objects.bucket,object_name='campaign-evidence/'+key,generation='123')
        objects.put_original.side_effect=put
        blob=objects.storage.bucket.return_value.blob.return_value
        blob.generation='123';blob.content_type='image/png'
        def reload(**kw):blob.size=len(next(iter(saved.values())))
        blob.reload.side_effect=reload;blob.download_as_bytes.side_effect=lambda **kw:next(iter(saved.values()))
        guard=Mock(return_value=True)
        engine=DurableCampaignArtifact(store=store,objects=objects,guard=guard,clock=lambda:renderer['checked_at'],enabled=True)
        return prepared,engine,store,objects,guard,intents,outcomes,saved

    def execute(self,engine,p,**kw):
        with worker_operation_scope(time.monotonic()+60):return engine.execute(p,**kw)

    def test_actual_banner_and_measured_pixels_saved_then_cold_recovery_is_read_only(self):
        p,e,store,objects,guard,intents,outcomes,saved=self.fixture()
        receipt=self.execute(e,p)
        self.assertEqual(receipt['measurements']['status'],'PASS')
        self.assertEqual(receipt['measurements']['measurements']['product_region_mse'],0)
        self.assertEqual(len(intents),3);self.assertEqual(len(outcomes),3)
        self.assertFalse(receipt['qualifying']);self.assertFalse(receipt['release_allowed'])
        cold=DurableCampaignArtifact(store=store,objects=objects,guard=guard,clock=e.clock,enabled=True)
        store.reserve.reset_mock();store.publish.reset_mock();objects.put_original.reset_mock()
        with patch('src.application.campaign_artifact_execution.render_identity_campaign',side_effect=AssertionError('No rerender')):
            self.assertEqual(self.execute(cold,p,recover_only=True),receipt)
        store.reserve.assert_not_called();store.publish.assert_not_called();objects.put_original.assert_not_called()

    def test_no_original_or_uncertain_render_never_uploads(self):
        p,e,s,o,g,intents,outcomes,_=self.fixture()
        with self.assertRaisesRegex(ValueError,'ARTIFACT_ORIGINAL_MISSING'):self.execute(e,p,recover_only=True)
        o.put_original.assert_not_called();s.reserve.assert_not_called()
        o.put_original.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):self.execute(e,p)
        with self.assertRaisesRegex(ValueError,'ARTIFACT_ORIGINAL_UNCERTAIN'):self.execute(e,p)
        o.put_original.assert_called_once()

    def test_lost_committed_render_and_score_replies_recover_original_records(self):
        for lost_stage in ('render','score'):
            p,e,s,o,g,intents,outcomes,_=self.fixture();publish=s.publish.side_effect;lost=[False]
            def interrupted(r,value):
                publish(r,value)
                target=value['state']==('STORED_PRIVATE_CANDIDATE' if lost_stage=='render' else 'MEASURED')
                if target and not lost[0]:lost[0]=True;raise TimeoutError()
            s.publish.side_effect=interrupted
            with self.assertRaises(TimeoutError):self.execute(e,p)
            if lost_stage=='render':
                with self.assertRaisesRegex(ValueError,'ARTIFACT_SCORE_ORIGINAL_MISSING'):self.execute(e,p,recover_only=True)
            result=self.execute(e,p)
            self.assertEqual(result['measurements']['status'],'PASS');o.put_original.assert_called_once()
            self.assertEqual(self.execute(e,p,recover_only=True),result)

    def test_access_or_inputs_changed_before_storage_are_held(self):
        for fault in ('access','options','image','profile'):
            p,e,s,o,g,*_=self.fixture()
            if fault=='access':g.return_value=False
            if fault=='options':p=replace(p,planner=replace(p.planner,options_json='{}'))
            if fault=='image':p=replace(p,image=replace(p.image,binding_id='different',
                lineage_json=canonical(dict(json.loads(p.image.lineage_json),binding_id='different'))))
            if fault=='profile':p=replace(p,config_json='{}')
            with self.subTest(fault=fault),self.assertRaises((ValueError,KeyError)):self.execute(e,p)
            o.put_original.assert_not_called()

    def test_generation_bytes_or_score_tampering_denies_recovery(self):
        for fault in ('generation','bytes','score','planner'):
            p,e,s,o,g,intents,outcomes,saved=self.fixture();receipt=self.execute(e,p)
            if fault=='generation':o.storage.bucket.return_value.blob.return_value.generation='124'
            if fault=='bytes':saved[next(iter(saved))]=b'changed'
            if fault=='score':outcomes[receipt['scoring_record_id']]['measurement']['status']='FAIL'
            if fault=='planner':outcomes[json.loads(p.planner.intent_json)['record_id']]['response_sha256']='0'*64
            with self.subTest(fault=fault),self.assertRaises(Exception):self.execute(e,p,recover_only=True)
            o.put_original.assert_called_once()

    def test_source_change_after_upload_preserves_uncertain_slot_without_reupload(self):
        p,e,s,o,g,intents,outcomes,saved=self.fixture()
        g.side_effect=[True,True,True,False]
        with self.assertRaisesRegex(ValueError,'ARTIFACT_SOURCE_OR_ACCESS_CHANGED'):self.execute(e,p)
        self.assertEqual(len(saved),1);self.assertEqual(len(outcomes),1)
        g.side_effect=None;g.return_value=True
        with self.assertRaisesRegex(ValueError,'ARTIFACT_ORIGINAL_UNCERTAIN'):self.execute(e,p)
        o.put_original.assert_called_once()

    def test_pixel_corruption_is_measured_as_failure(self):
        from PIL import Image
        import io
        p,e,s,o,g,*_=self.fixture();self.execute(e,p)
        result=render_identity_campaign(product=p.product,image=p.image,planner_response=SourceBoundRendererTests().fixture()['planner_response'],
            options_json=p.planner.options_json,checked_at=e.clock(),config=json.loads(p.config_json))
        meta=json.loads(result.metadata_json)
        with Image.open(io.BytesIO(result.png)) as banner:
            x,y,_,_=meta['geometry']['product_bounds'];banner.putpixel((x,y),(1,2,3));buffer=io.BytesIO();banner.save(buffer,format='PNG')
        png=buffer.getvalue();meta['content_sha256']=hashlib.sha256(png).hexdigest()
        score=measure_banner(png,meta,image=p.image,planner_response=SourceBoundRendererTests().fixture()['planner_response'],options_json=p.planner.options_json,config=json.loads(p.config_json))
        self.assertEqual(score['status'],'FAIL');self.assertGreater(score['measurements']['product_region_mse'],0)
