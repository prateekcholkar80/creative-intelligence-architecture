from dataclasses import replace
from datetime import datetime,timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock
import json
from src.application.campaign_operator_host import ProtectedOperatorHost,config_from_wire,request_values
from src.application.campaign_operator_worker import execute
from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
from src.application.campaign_planner_runtime import PlannerEvaluationSettings
from src.application.campaign_product_image_derivation import ImageDerivationSettings
from src.application.campaign_evaluator_store import canonical,sha
from src.application.operation_channel import ChannelError
from src.ui.campaign_operator_console import read_only_config
from tests.test_campaign_permission_lifecycle import AUDIENCE
from src.application.identity import IapSettings


def config():
    image=json.loads(Path('config/evaluation/rejuvenate_product_image_derivation.json').read_text())
    planner=json.loads(Path('config/evaluation/rejuvenate_planner_pilot_deployment.json').read_text())
    planner['submission_ids']=tuple(planner['submission_ids'])
    principal='iap-v1:'+'1'*64
    measurement=MeasurementRuntimeSettings('ci-poc-marketing',principal,'a'*64,'b'*64)
    return read_only_config(IapSettings(AUDIENCE),principal,now=datetime.now(timezone.utc),measurement=measurement,
        planner=PlannerEvaluationSettings(**planner),
        product_image=ImageDerivationSettings(canonical(image['decision']),image['decision_sha256'],True))


class ProductImageHostTests(TestCase):
    def receipt(self):
        value=json.loads(config().product_image.decision_json)
        return dict(binding_id='eval_image_derivation_v1_'+'a'*64,record_sha256='b'*64,
            parent_binding_id=value['source_binding_id'],content_sha256=value['output_sha256'],generation='123',
            width=460,height=760,claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)

    def test_roundtrip_and_parent_scope_are_server_pinned(self):
        cfg=config();host=ProtectedOperatorHost(cfg)
        self.assertEqual(config_from_wire(host._config_values()),cfg)
        decision=json.loads(cfg.product_image.decision_json);decision['scope']['sku']='CI-SKU-0023'
        with self.assertRaises(ChannelError):replace(cfg,product_image=ImageDerivationSettings(canonical(decision),sha(decision),True))
        with self.assertRaises(ChannelError):replace(cfg,writes_enabled=True)

    def test_browser_cannot_supply_crop_bytes_source_identity_or_approval(self):
        for key in ('crop_box','image','submission_id','identity','claim_approval','native_job_id'):
            with self.assertRaises(ChannelError):request_values('product_image_prepare',{}, {key:'injected'})
        self.assertEqual(request_values('product_image_recover',{}, {})['parameters'],{})

    def test_disabled_prepare_and_no_configuration_deny_but_configured_recovery_allowed(self):
        cfg=config();host=ProtectedOperatorHost(replace(cfg,product_image=replace(cfg.product_image,enabled=False)))
        with self.assertRaises(ChannelError):host._request_values('product_image_prepare',{}, {})
        host._request_values('product_image_recover',{}, {})
        empty=ProtectedOperatorHost(replace(cfg,product_image=None))
        with self.assertRaises(ChannelError):empty._request_values('product_image_recover',{}, {})

    def test_receipt_rejects_claims_changed_bytes_dimensions_and_parent(self):
        host=ProtectedOperatorHost(config());receipt=self.receipt()
        self.assertEqual(host._decode_result('product_image_prepare',receipt),receipt)
        host._validate_result_scope('product_image_prepare',{},receipt)
        for change in (dict(claim_approval=True),dict(generation_authorized=True),dict(width=True),dict(extra=True)):
            with self.assertRaises(ChannelError):host._decode_result('product_image_prepare',dict(receipt,**change))
        for change in (dict(content_sha256='0'*64),dict(width=461),dict(parent_binding_id='eval_upload_binding_v1_'+'0'*64)):
            with self.assertRaises(ChannelError):host._validate_result_scope('product_image_prepare',{},dict(receipt,**change))

    def test_worker_uses_configured_scope_and_recovery_cannot_switch_to_prepare(self):
        cfg=config();product=Mock();product.prepare.return_value=self.receipt()
        result=execute(Mock(),cfg,'product_image_recover',{'iap':'current'},{},product_image=product)
        self.assertEqual(result,self.receipt())
        product.prepare.assert_called_once_with({'iap':'current'},workspace_id=cfg.measurement.workspace_id,
            conversation_id=cfg.planner.conversation_id,request_id=cfg.planner.request_id,recover_only=True)
        with self.assertRaises(ChannelError):execute(Mock(),cfg,'product_image_prepare',{}, {})
