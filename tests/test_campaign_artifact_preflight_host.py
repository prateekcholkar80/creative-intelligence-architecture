from dataclasses import replace
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock
import json
from src.application.campaign_operator_host import *
from src.application.campaign_operator_worker import execute
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_corpus_preparation import CorpusPreparationSettings
from src.application.campaign_corpus_embedding import CorpusEmbeddingSettings
from src.application.campaign_corpus_retrieval import CorpusRetrievalSettings
from src.application.operation_channel import ChannelError
from tests.test_product_image_host import config as image_config


def config():
    def load(name):return json.loads((Path('config/evaluation')/name).read_text())
    selected=load('rejuvenate_upload_corpus.json')
    corpus=CorpusPreparationSettings('corpus',canonical(selected),sha(selected),True)
    value=load('rejuvenate_text_embeddings.json');value.pop('version')
    embedding=CorpusEmbeddingSettings(**value,enabled=True)
    value=load('rejuvenate_retrieval_measurement.json');value.pop('version');case=value.pop('case')
    retrieval=CorpusRetrievalSettings(**value,case_json=canonical(case),case_sha256=sha(case),enabled=True)
    value=load('rejuvenate_campaign_artifact_preflight.json')
    return replace(image_config(),corpus=corpus,embedding=embedding,corpus_retrieval=retrieval,
        retrieved_planner=RetrievedPlannerSettings(**value['retrieved_planner']),artifact=CampaignArtifactSettings(**value['artifact']))


class ArtifactPreflightHostTests(TestCase):
    def receipt(self,cfg):
        return dict(status='PREFLIGHT_PASSED_NOT_GENERATION_AUTHORITY',run_id=cfg.artifact.run_id,
            render_config_sha256=cfg.artifact.render_config_sha256,font_profile_sha256='a'*64,
            crop_binding_id='eval_image_derivation_v1_'+'b'*64,crop_record_sha256='c'*64,
            source_manifest_sha256=cfg.planner.binding_id.removeprefix('eval_upload_binding_v1_'),
            retrieval_measurement_record_id='d'*64,retrieval_outcome_sha256='e'*64,
            phase_seconds=dict(planner_sources=1.,saved_crop=2.,saved_retrieval=3.),model_calls=0,
            generation_authorized=False,qualifying=False,release_allowed=False)

    def test_server_only_config_roundtrips_and_cannot_enable_execution(self):
        cfg=config();host=ProtectedOperatorHost(cfg)
        self.assertEqual(config_from_wire(host._config_values()),cfg)
        for change in (dict(artifact=replace(cfg.artifact,enabled=True)),dict(retrieved_planner=replace(cfg.retrieved_planner,enabled=True)),dict(writes_enabled=True)):
            with self.assertRaises(ChannelError):replace(cfg,**change)

    def test_browser_cannot_choose_source_profile_or_request_generation(self):
        for key in ('run_id','enabled','model','crop_binding_id','render_config_json'):
            with self.assertRaises(ChannelError):request_values('artifact_preflight',{}, {key:True})
        for method in ('artifact_run','artifact_recover'):
            with self.assertRaises(ChannelError):request_values(method,{}, {})

    def test_safe_receipt_rejects_authority_nonfinite_timings_and_wrong_scope(self):
        cfg=config();host=ProtectedOperatorHost(cfg);value=self.receipt(cfg)
        self.assertEqual(host._decode_result('artifact_preflight',value),value)
        host._validate_result_scope('artifact_preflight',{},value)
        for change in (dict(generation_authorized=True),dict(model_calls=True),dict(phase_seconds=dict(planner_sources=float('nan'),saved_crop=0,saved_retrieval=0))):
            with self.assertRaises(ChannelError):host._decode_result('artifact_preflight',dict(value,**change))
        with self.assertRaises(ChannelError):host._validate_result_scope('artifact_preflight',{},dict(value,run_id='other'))

    def test_worker_only_calls_read_only_preflight(self):
        cfg=config();artifact=Mock();artifact.preflight.return_value=self.receipt(cfg)
        self.assertEqual(execute(Mock(),cfg,'artifact_preflight',{'iap':'current'},{},artifact=artifact),self.receipt(cfg))
        artifact.preflight.assert_called_once_with({'iap':'current'});artifact.execute.assert_not_called()
