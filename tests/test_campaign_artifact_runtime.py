from dataclasses import replace
from types import SimpleNamespace
import json,time
from unittest import TestCase
from unittest.mock import Mock,patch

from src.application.campaign_artifact_runtime import *
from src.application.campaign_product_image_service import DerivedImageCapture
from src.application.operation_budget import worker_operation_scope
from tests.test_campaign_artifact_execution import ArtifactExecutionTests
from tests.test_campaign_product_image_service import fixture as crop_fixture


class CampaignArtifactRuntimeTests(TestCase):
    def test_preflight_is_read_only_and_cannot_enable_execution(self):
        from pathlib import Path
        from src.application.campaign_evaluation_uploads import EvaluationUploadInputs
        from datetime import timedelta
        r,p,crop,retrieved,store,objects=self.fixture()
        render=json.loads(Path('config/evaluation/rejuvenate_campaign_render.json').read_text())
        r.settings=CampaignArtifactSettings('preflight',canonical(render),sha(render),False)
        retrieved.settings=SimpleNamespace(enabled=False)
        parent=json.loads(p.planner.intent_json);now=r.engine.clock()
        inputs=EvaluationUploadInputs(canonical(parent['source']),(),now,now,now+timedelta(seconds=120))
        retrieved.planner._capture=Mock(return_value=(inputs,None,None,None))
        retrieved.planner.measurement._actor=Mock()
        retrieved.planner.settings.binding_id=parent['input_binding_id']
        retrieved.retrieval.settings=SimpleNamespace(index_sha256='f'*64)
        retrieved.retrieval.execute=Mock(return_value=dict(index_sha256='f'*64,measurement_record_id='a'*64,outcome_sha256='b'*64))
        r.engine.execute=Mock(side_effect=AssertionError('No campaign execution'))
        with worker_operation_scope(time.monotonic()+60):receipt=r.preflight({'iap':'fixture'})
        self.assertEqual(receipt['model_calls'],0);self.assertFalse(receipt['generation_authorized'])
        self.assertTrue(retrieved.retrieval.execute.call_args.kwargs['recover_only'])
        store.reserve.assert_not_called();objects.put_original.assert_not_called();r.engine.execute.assert_not_called()

    def fixture(self):
        prepared,engine,store,objects,_,_,_,_=ArtifactExecutionTests().fixture()
        with worker_operation_scope(time.monotonic()+60):
            crop,docs,records,data,inputs=crop_fixture()
            receipt=crop.prepare({},workspace_id='workspace',conversation_id='conversation',request_id='request')
        capture=DerivedImageCapture(receipt['binding_id'],canonical(records[receipt['binding_id']]),data[receipt['binding_id']],inputs.valid_until)
        crop.capture_bound=Mock(return_value=capture);docs.intake.store=objects
        retrieved=object.__new__(ProtectedRetrievedPlanner)
        retrieved.planner=SimpleNamespace(documents=docs,measurement=SimpleNamespace(client=Mock(),clock=engine.clock,
            settings=SimpleNamespace(workspace_id='workspace')),settings=SimpleNamespace(conversation_id='conversation',request_id='request'))
        retrieved.retrieval=SimpleNamespace(store=store);retrieved._guard_planner=Mock()
        receipt=dict(run_id='retrieved-pilot',artifact_status='NOT_EXECUTED',qualifying=False,release_allowed=False)
        retrieved.execute_bound=Mock(side_effect=lambda headers,consume,**kw:consume(prepared.planner,receipt))
        settings=CampaignArtifactSettings('artifact-pilot',prepared.config_json,sha(json.loads(prepared.config_json)),True)
        with worker_operation_scope(time.monotonic()+60):runtime=ProtectedCampaignArtifact(retrieved,crop,settings)
        return runtime,prepared,crop,retrieved,store,objects

    def call(self,r,product,**kw):
        with patch('src.application.campaign_artifact_runtime.read_product_fact',return_value=product),worker_operation_scope(time.monotonic()+60):
            return r.execute({'iap':'synthetic'},**kw)

    def test_single_protected_operation_connects_saved_crop_planner_artifact_and_scores(self):
        r,p,crop,retrieved,store,objects=self.fixture();receipt=self.call(r,p.product)
        self.assertEqual(receipt['artifact_status'],'STORED_PRIVATE_CANDIDATE')
        self.assertEqual(receipt['artifact']['measurements']['status'],'PASS')
        self.assertFalse(receipt['generation_authorized']);self.assertFalse(receipt['qualifying'])
        self.assertIsNone(r._headers);self.assertIsNone(r._prepared);self.assertIsNone(r._crop)
        recovered=self.call(r,p.product,recover_only=True);self.assertEqual(receipt,recovered)
        self.assertTrue(retrieved.execute_bound.call_args.kwargs['recover_only'])
        self.assertGreater(crop.capture_bound.call_count,2)
        for call in crop.capture_bound.call_args_list:
            self.assertEqual(call.kwargs,dict(workspace_id='workspace',conversation_id='conversation',request_id='request'))
        objects.put_original.assert_called_once()

    def test_changed_crop_product_or_source_never_uploads_and_clears_context(self):
        for fault in ('crop','product','source'):
            r,p,crop,retrieved,store,objects=self.fixture()
            if fault=='crop':
                good=crop.capture_bound.return_value
                crop.capture_bound.side_effect=[good,replace(good,binding_id='different')]
            if fault=='product':
                product=replace(p.product,fact=replace(p.product.fact,fields_json=canonical(dict(product_name='Different'))))
            else:product=p.product
            if fault=='source':retrieved._guard_planner.side_effect=ValueError('Revoked')
            with self.subTest(fault=fault),self.assertRaises(ValueError):self.call(r,product)
            objects.put_original.assert_not_called();self.assertIsNone(r._headers)

    def test_disabled_and_missing_original_do_not_run_model_or_publish(self):
        r,p,crop,retrieved,store,objects=self.fixture()
        with self.assertRaisesRegex(ValueError,'ARTIFACT_ORIGINAL_MISSING'):self.call(r,p.product,recover_only=True)
        store.reserve.assert_not_called();objects.put_original.assert_not_called()
        r.settings=replace(r.settings,enabled=False)
        retrieved.execute_bound.reset_mock()
        with self.assertRaisesRegex(ValueError,'ARTIFACT_RUNTIME_DISABLED'):self.call(r,p.product)
        retrieved.execute_bound.assert_not_called()
