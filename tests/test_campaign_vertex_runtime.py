from datetime import datetime,timezone,timedelta
from dataclasses import replace
from types import SimpleNamespace
import json,time,hashlib
from unittest import TestCase
from unittest.mock import Mock,patch

from src.application.campaign_vertex_runtime import *
from src.application.campaign_product_image_service import DerivedImageCapture
from src.application.operation_budget import worker_operation_scope
from src.retrieval.upload_corpus import UploadCorpusCapture
from tests.test_campaign_artifact_execution import ArtifactExecutionTests
from tests.test_campaign_product_image_service import fixture as crop_fixture
from tests.test_vertex_campaign_visual import VertexVisualTests
from src.generators.vertex_campaign_visual import extract_visual


class VertexRuntimeTests(TestCase):
    def fixture(self):
        artifact,old,store,objects,_,intents,outcomes,_=ArtifactExecutionTests().fixture()
        with worker_operation_scope(time.monotonic()+60):
            crop,docs,records,data,inputs=crop_fixture()
            receipt=crop.prepare({},workspace_id='workspace',conversation_id='conversation',request_id='request')
        captured=DerivedImageCapture(receipt['binding_id'],canonical(records[receipt['binding_id']]),data[receipt['binding_id']],inputs.valid_until)
        crop.capture_bound=Mock(return_value=captured)
        parent=json.loads(artifact.planner.intent_json);parent.pop('retrieval_evidence');parent.pop('retrieval_evidence_sha256')
        prepared=replace(artifact.planner,intent_json=canonical(parent))
        intents[parent['record_id']]=canonical(dict(parent,reservation_nonce='nonce'))
        objects.bucket='private-fixture';memory={'campaign-evidence/'+captured.binding_id:dict(raw=captured.raw,generation='456',mime='image/png')}
        def blob_for(name):
            blob=SimpleNamespace()
            def reload(**kw):
                value=memory[name];blob.generation=value['generation'];blob.size=len(value['raw']);blob.content_type=value['mime']
            def download(**kw):
                self.assertEqual(str(kw['if_generation_match']),memory[name]['generation']);return memory[name]['raw']
            blob.reload=reload;blob.download_as_bytes=download;return blob
        objects.storage.bucket.return_value.blob.side_effect=blob_for
        def put(key,raw,file):
            name='campaign-evidence/'+key
            memory.setdefault(name,dict(raw=raw,generation=str(1000+len(memory)),mime=file['content_type']))
            self.assertEqual(memory[name]['raw'],raw)
            return dict(bucket=objects.bucket,object_name=name,generation=memory[name]['generation'])
        objects.put_original.side_effect=put;docs.intake.store=objects
        client=Mock();store.client=client;clock=old.clock
        fields=json.loads(artifact.product.fact.fields_json);pins=dict(registry_digest='d'*64,grounding=dict(product_fields=fields))
        planner=object.__new__(ProtectedPlannerEvaluation);planner.documents=docs
        planner.measurement=SimpleNamespace(client=client,clock=clock,settings=SimpleNamespace(workspace_id='workspace'),_actor=Mock(
            return_value=SimpleNamespace(principal=SimpleNamespace(principal_id='owner'),expires_at=int((clock()+timedelta(seconds=600)).timestamp()))))
        planner.settings=SimpleNamespace(conversation_id='conversation',request_id='request',binding_id=parent['input_binding_id'],
            prompt_id='999',prompt_version='1',prompt_snapshot_sha256='c'*64)
        planner._capture=Mock(return_value=(inputs,SimpleNamespace(model_id=parent['profile']['model_id']),pins,{}))
        planner._prepare=Mock(return_value=prepared);planner.registry=Mock()
        planner.registry.read_exact.return_value=SimpleNamespace(state=SimpleNamespace(head=SimpleNamespace(digest='d'*64,
            snapshot=SimpleNamespace(snapshot_sha256='c'*64))))
        manifest=dict(scope=parent['source']['context']['scope'],image_manifest_sha256=inputs.sha256,image_binding_id=parent['input_binding_id'])
        capture=UploadCorpusCapture(canonical(manifest),(),clock()+timedelta(seconds=60));refresh=Mock(return_value=True)
        bound=BoundCorpusCapture(capture,'{}',refresh)
        retrieval=object.__new__(ProtectedCorpusRetrieval);retrieval.store=store
        retrieval.embedding=SimpleNamespace(corpus=SimpleNamespace(documents=docs),settings=SimpleNamespace(corpus_sha256=capture.sha256))
        receipt=dict(measurement_record_id='a'*64,outcome_sha256='b'*64,corpus_sha256=capture.sha256,index_sha256='e'*64,
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
        retrieval.execute_bound=Mock(side_effect=lambda headers,consume,**kw:consume(bound,receipt))
        settings=VertexCampaignSettings('vertex-private-pilot','rejuvenate-visual',artifact.config_json,
            sha(json.loads(artifact.config_json)),VisualProfile(endpoint_authorized=True),captured.binding_id,True)
        transport=Mock(return_value=extract_visual(VertexVisualTests().response(),'gemini-3.1-flash-image'))
        with worker_operation_scope(time.monotonic()+60):runtime=ProtectedVertexCampaign(planner,retrieval,crop,settings,transport=transport)
        return runtime,artifact.product,transport,store,objects,refresh,memory

    def call(self,r,product,**kw):
        with patch('src.application.campaign_vertex_runtime.read_product_fact',return_value=product),worker_operation_scope(time.monotonic()+180):
            return r.execute({'iap':'fixture'},**kw)

    def test_visual_then_banner_uses_one_provider_call_and_keeps_actual_retrieval_provenance(self):
        r,product,t,store,objects,refresh,memory=self.fixture()
        visual=self.call(r,product,phase='VISUAL');self.assertIsNone(visual['artifact'])
        banner=self.call(r,product,phase='BANNER')
        self.assertEqual(banner['artifact']['measurements']['status'],'PASS')
        self.assertEqual(banner['artifact']['measurements']['measurements']['product_region_mse'],0)
        self.assertEqual(banner['artifact']['measurements']['measurements']['generated_scene_region_mse'],0)
        self.assertEqual(banner['semantic_reranking'],'NOT_EXECUTED')
        self.assertEqual(banner['retrieval_use'],'POST_PLANNER_PRIVATE_CALIBRATION_NOT_IN_PLANNER_INPUT')
        t.assert_called_once();self.assertEqual(objects.put_original.call_count,2)
        self.assertIsNone(r._headers);self.assertIsNone(r._visual_request)
        self.assertEqual(self.call(r,product,phase='BANNER',recover_only=True),banner)
        t.assert_called_once();self.assertEqual(objects.put_original.call_count,2)
        for call in r.retrieval.execute_bound.call_args_list:self.assertIs(call.kwargs['recover_only'],True)

    def test_banner_and_recovery_cannot_dispatch_a_missing_image(self):
        for phase in ('VISUAL','BANNER'):
            r,p,t,s,o,*_=self.fixture()
            with self.assertRaisesRegex(ValueError,'VISUAL_ORIGINAL_MISSING'):self.call(r,p,phase=phase,recover_only=True)
            t.assert_not_called();o.put_original.assert_not_called();s.reserve.assert_not_called()
            self.assertIsNone(r._headers)

    def test_source_change_or_expired_identity_denies_before_model(self):
        for fault in ('source','identity'):
            r,p,t,s,o,refresh,_=self.fixture()
            if fault=='source':refresh.return_value=False
            else:r.planner.measurement._actor.return_value.principal.principal_id='other'
            with self.subTest(fault=fault),self.assertRaises(ValueError):self.call(r,p,phase='VISUAL')
            t.assert_not_called();s.reserve.assert_not_called();self.assertIsNone(r._headers)
