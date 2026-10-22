from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import json
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_operator_host import ProtectedOperatorHost,config_from_wire,request_values
from src.application.campaign_operator_worker import execute
from src.application.campaign_vertex_runtime import VertexCampaignSettings
from src.application.campaign_visual_request import VisualProfile
from src.application.operation_channel import ChannelError
from tests.test_campaign_artifact_preflight_host import config as base_config
from tests.test_campaign_vertex_runtime import VertexRuntimeTests


def config():
    data=json.loads(Path('config/evaluation/rejuvenate_vertex_campaign_test.json').read_text())['settings']
    data['profile']=VisualProfile(**data['profile'])
    return replace(base_config(),vertex=VertexCampaignSettings(**data))


class VertexHostTests(TestCase):
    def receipt(self):
        cfg=config();f=VertexRuntimeTests();runtime,product,*_=f.fixture()
        f.call(runtime,product,phase='VISUAL');result=f.call(runtime,product,phase='BANNER')
        result.update(run_id=cfg.vertex.run_id,case_id=cfg.vertex.case_id,crop_binding_id=cfg.vertex.crop_binding_id)
        result['visual']['original']['bucket']='ci-campaign-evidence-1042059373096'
        result['artifact']['original']['bucket']='ci-campaign-evidence-1042059373096'
        return result

    def test_exact_server_settings_roundtrip_and_no_browser_configuration(self):
        cfg=config();host=ProtectedOperatorHost(cfg)
        self.assertEqual(config_from_wire(host._config_values()),cfg)
        for field in ('model','location','endpoint_authorized','crop_binding_id','run_id'):
            with self.assertRaises(ChannelError):request_values('vertex_visual_run',{}, {field:True})
        with self.assertRaises(ValueError):replace(cfg.vertex,profile=VisualProfile(endpoint_authorized=False))
        with self.assertRaises(ChannelError):replace(cfg,writes_enabled=True)
        with self.assertRaises(ChannelError):ProtectedOperatorHost(replace(cfg,vertex=None))._request_values('vertex_visual_run',{}, {})

    def test_metadata_receipt_binding_and_authority_flags(self):
        host=ProtectedOperatorHost(config());value=self.receipt()
        self.assertEqual(host._decode_result('vertex_banner_run',value),value)
        host._validate_result_scope('vertex_banner_run',{},value)
        for field in ('generation_authorized','qualifying','release_allowed'):
            with self.assertRaises(ChannelError):host._decode_result('vertex_banner_run',dict(value,**{field:True}))
        changed=deepcopy(value);changed['visual']['returned_model_id']='other'
        with self.assertRaises(ChannelError):host._decode_result('vertex_banner_run',changed)
        with self.assertRaises(ChannelError):host._validate_result_scope('vertex_banner_run',{},dict(value,crop_binding_id='different'))
        visual=dict(value,phase='VISUAL',artifact=None)
        self.assertEqual(host._decode_result('vertex_visual_recover',visual),visual)

    def test_worker_recovery_cannot_switch_to_run_or_another_phase(self):
        cfg=config();vertex=Mock()
        execute(Mock(),cfg,'vertex_visual_recover',{'iap':'current'},{},vertex=vertex)
        vertex.execute.assert_called_once_with({'iap':'current'},phase='VISUAL',recover_only=True)
        vertex.reset_mock();execute(Mock(),cfg,'vertex_banner_run',{'iap':'current'},{},vertex=vertex)
        vertex.execute.assert_called_once_with({'iap':'current'},phase='BANNER',recover_only=False)
