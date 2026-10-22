from dataclasses import replace
import json,time
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_visual_execution import DurableVertexVisual
from src.application.campaign_visual_request import VisualRequest
from src.application.campaign_evaluator_store import canonical
from src.application.operation_budget import worker_operation_scope,OperationStopped
from src.generators.vertex_campaign_visual import extract_visual
from tests.test_campaign_artifact_execution import ArtifactExecutionTests
from tests.test_vertex_campaign_visual import VertexVisualTests


class VisualExecutionTests(TestCase):
    def fixture(self):
        _,_,store,objects,_,intents,outcomes,saved=ArtifactExecutionTests().fixture()
        request=VertexVisualTests().request(True)
        transport=Mock(return_value=extract_visual(VertexVisualTests().response(),'gemini-3.1-flash-image'))
        guard=Mock(return_value=True)
        engine=DurableVertexVisual(store=store,objects=objects,transport=transport,guard=guard,enabled=True)
        return request,engine,store,objects,transport,guard,intents,outcomes,saved

    def call(self,e,r,**kw):
        with worker_operation_scope(time.monotonic()+60):return e.execute(r,**kw)

    def test_generated_image_and_exact_provider_manifest_are_durable_with_read_only_cold_recovery(self):
        r,e,s,o,t,g,intents,outcomes,saved=self.fixture();result=self.call(e,r)
        receipt=json.loads(result.receipt_json);self.assertFalse(receipt['qualifying'])
        self.assertEqual(result.scene.source_image_binding_id,r.intent['image']['binding_id'])
        self.assertEqual(outcomes[r.intent['record_id']]['response_manifest_json'],t.return_value.response_manifest_json)
        cold=DurableVertexVisual(store=s,objects=o,transport=t,guard=g,enabled=True)
        s.reserve.reset_mock();s.publish.reset_mock();o.put_original.reset_mock()
        self.assertEqual(self.call(cold,r,recover_only=True),result)
        t.assert_called_once();s.reserve.assert_not_called();s.publish.assert_not_called();o.put_original.assert_not_called()

    def test_missing_original_and_uncertain_provider_never_dispatch_twice(self):
        r,e,s,o,t,g,*_=self.fixture()
        with self.assertRaisesRegex(ValueError,'VISUAL_ORIGINAL_MISSING'):self.call(e,r,recover_only=True)
        s.reserve.assert_not_called();t.assert_not_called()
        t.side_effect=TimeoutError()
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'VISUAL_ORIGINAL_UNAVAILABLE_OR_UNKNOWN'):self.call(e,r)
        t.assert_called_once();o.put_original.assert_not_called()

    def test_lost_committed_reply_cold_recovers_without_second_provider_or_upload(self):
        r,e,s,o,t,g,*_=self.fixture();publish=s.publish.side_effect;lost=[False]
        def lose(reservation,outcome):
            publish(reservation,outcome)
            if not lost[0]:lost[0]=True;raise TimeoutError()
        s.publish.side_effect=lose
        with self.assertRaises(TimeoutError):self.call(e,r)
        cold=DurableVertexVisual(store=s,objects=o,transport=t,guard=g,enabled=True)
        self.call(cold,r,recover_only=True);t.assert_called_once();o.put_original.assert_called_once()

    def test_changed_authority_after_upload_retains_unknown_slot_and_original_object(self):
        r,e,s,o,t,g,intents,outcomes,saved=self.fixture();g.side_effect=[True,True,True,False]
        with self.assertRaisesRegex(ValueError,'VISUAL_ORIGINAL_UNAVAILABLE_OR_UNKNOWN'):self.call(e,r)
        self.assertEqual(len(saved),1);g.side_effect=None;g.return_value=True
        with self.assertRaisesRegex(ValueError,'VISUAL_ORIGINAL_UNAVAILABLE_OR_UNKNOWN'):self.call(e,r)
        t.assert_called_once();o.put_original.assert_called_once()

    def test_unauthorized_region_revoked_source_and_changed_request_cannot_dispatch(self):
        for fault in ('region','source'):
            r,e,s,o,t,g,*_=self.fixture()
            if fault=='region':
                value=r.intent;value['profile']['endpoint_authorized']=False;r=VisualRequest(canonical(value),r.crop_png)
            else:g.return_value=False
            with self.subTest(fault=fault),self.assertRaises(ValueError):self.call(e,r)
            s.reserve.assert_not_called();t.assert_not_called()
        r,e,s,o,t,g,*_=self.fixture();self.call(e,r)
        value=r.intent;value['planner_outcome_sha256']='0'*64
        with self.assertRaises(ValueError):self.call(e,VisualRequest(canonical(value),r.crop_png))
        t.assert_called_once()

    def test_changed_generation_denies_recovery_without_repair(self):
        r,e,s,o,t,g,*_=self.fixture();self.call(e,r)
        o.storage.bucket.return_value.blob.return_value.generation='124'
        with self.assertRaisesRegex(ValueError,'VISUAL_OBJECT_GENERATION_CHANGED'):self.call(e,r,recover_only=True)
        t.assert_called_once();o.put_original.assert_called_once()

    def test_deadline_after_provider_response_keeps_uncertain_slot_without_redispatch(self):
        r,e,s,o,t,g,intents,outcomes,_=self.fixture();clock=[0.];response=t.return_value
        def late(request):clock[0]=101.;return response
        t.side_effect=late
        with self.assertRaises(OperationStopped),patch('src.application.operation_budget.time.monotonic',side_effect=lambda:clock[0]),worker_operation_scope(100):
            e.execute(r)
        with self.assertRaisesRegex(ValueError,'VISUAL_ORIGINAL_UNAVAILABLE_OR_UNKNOWN'):self.call(e,r)
        t.assert_called_once();o.put_original.assert_not_called()
