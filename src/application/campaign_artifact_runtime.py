"""Protected private retrieval/planner/crop/banner operation; no chat activation.

Reuses original retrieval and embedding records. Does not approve any source,
clinical claim or prompt. No headers survive the bounded worker action.
"""
from dataclasses import dataclass
from threading import RLock
import json,time

from src.application.campaign_artifact_execution import DurableCampaignArtifact,prepare_campaign_artifact
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_product_image_service import ProtectedProductImage
from src.application.campaign_quality_product_reader import read_product_fact
from src.application.campaign_retrieved_planner_runtime import ProtectedRetrievedPlanner
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id,_digest


@dataclass(frozen=True,slots=True)
class CampaignArtifactSettings:
    run_id:str
    render_config_json:str
    render_config_sha256:str
    enabled:bool=False

    def __post_init__(self):
        _id(self.run_id);_digest(self.render_config_sha256)
        _require(type(self.enabled) is bool and type(self.render_config_json) is str
            and len(self.render_config_json.encode())<=8192
            and canonical(json.loads(self.render_config_json))==self.render_config_json
            and sha(json.loads(self.render_config_json))==self.render_config_sha256,'ARTIFACT_RUNTIME_SETTINGS')


class ProtectedCampaignArtifact:
    def __init__(self,retrieved,product_image,settings):
        require_owned_worker()
        _require(type(retrieved) is ProtectedRetrievedPlanner and type(product_image) is ProtectedProductImage
            and type(settings) is CampaignArtifactSettings
            and retrieved.planner.documents is product_image.documents,'ARTIFACT_RUNTIME_COMPOSITION')
        self.retrieved,self.product_image,self.settings=retrieved,product_image,settings
        self._headers=None;self._prepared=None;self._crop=None;self._lock=RLock()
        self.engine=DurableCampaignArtifact(store=retrieved.retrieval.store,objects=product_image.documents.intake.store,
            guard=self._guard,clock=retrieved.planner.measurement.clock,enabled=settings.enabled)

    def _scope(self):
        p=self.retrieved.planner
        return dict(workspace_id=p.measurement.settings.workspace_id,conversation_id=p.settings.conversation_id,
            request_id=p.settings.request_id)

    def preflight(self,headers):
        """Read-only dependency/deadline probe; never authorizes a later run."""
        from src.validation.campaign_render_preflight import verify
        from src.retrieval.upload_corpus import parallel_reads
        require_owned_worker();check_operation()
        _require(not self.settings.enabled and not self.retrieved.settings.enabled,'ARTIFACT_PREFLIGHT_REQUIRES_DISABLED_EXECUTION')
        with self._lock:
            # The shared verifier takes a file; runtime verifies the exact
            # configured object rather than substituting a hardcoded profile.
            from pathlib import Path
            path=Path('config/evaluation/rejuvenate_campaign_render.json')
            _require(canonical(json.loads(path.read_text()))==self.settings.render_config_json,'ARTIFACT_PREFLIGHT_CONFIG_CHANGED')
            font_result=verify(path);timings={}
            def measured(name,fn):
                started=time.monotonic()
                try:return fn()
                finally:timings[name]=round(time.monotonic()-started,3)
            p=self.retrieved.planner
            planner,crop,retrieval=parallel_reads((
                lambda:measured('planner_sources',lambda:p._capture(headers)),
                lambda:measured('saved_crop',lambda:self.product_image.capture_bound(headers,**self._scope())),
                lambda:measured('saved_retrieval',lambda:self.retrieved.retrieval.execute(headers,recover_only=True))))
            inputs,snapshot,pins,evidence=planner;lineage=json.loads(crop.record_json)
            _require(lineage['parent_manifest_sha256']==inputs.sha256
                and lineage['parent_binding_id']==p.settings.binding_id
                and lineage['derivation_scope']==json.loads(inputs.manifest_json)['context']['scope']
                and retrieval['index_sha256']==self.retrieved.retrieval.settings.index_sha256
                and p.measurement.clock()<min(inputs.valid_until,crop.valid_until),
                'ARTIFACT_PREFLIGHT_SOURCE_CHANGED')
            p.measurement._actor(headers);check_operation()
            return dict(status='PREFLIGHT_PASSED_NOT_GENERATION_AUTHORITY',run_id=self.settings.run_id,
                render_config_sha256=self.settings.render_config_sha256,font_profile_sha256=sha(font_result['fonts']),
                crop_binding_id=crop.binding_id,crop_record_sha256=sha(lineage),source_manifest_sha256=inputs.sha256,
                retrieval_measurement_record_id=retrieval['measurement_record_id'],
                retrieval_outcome_sha256=retrieval['outcome_sha256'],phase_seconds=timings,
                model_calls=0,generation_authorized=False,qualifying=False,release_allowed=False)

    def _guard(self,intent):
        require_owned_worker();check_operation()
        _require(self._headers is not None and self._prepared is not None and self._crop is not None,
            'ARTIFACT_RUNTIME_CONTEXT')
        # Existing protected planner guard reconstructs current IAP/access,
        # registry, product, policy, source/corpus and recovered rerank bindings.
        self.retrieved._guard_planner(json.loads(self._prepared.planner.intent_json))
        current=self.product_image.capture_bound(self._headers,**self._scope())
        _require(current.binding_id==self._crop.binding_id and current.record_json==self._crop.record_json
            and current.raw==self._crop.raw,'ARTIFACT_RUNTIME_CROP_CHANGED')
        p=self.retrieved.planner
        product=read_product_fact(p.measurement.client,intent['scope']['sku'],clock=p.measurement.clock)
        _require(product.fact.fields_json==intent['product_fields_json'],'ARTIFACT_RUNTIME_PRODUCT_CHANGED')
        return product

    def execute(self,headers,*,recover_only=False):
        require_owned_worker();check_operation()
        _require(self.settings.enabled and type(recover_only) is bool,'ARTIFACT_RUNTIME_DISABLED')
        with self._lock:
            try:
                self._headers=headers
                def consume(planner,receipt):
                    p=self.retrieved.planner
                    self._crop=self.product_image.capture_bound(headers,**self._scope())
                    product=read_product_fact(p.measurement.client,json.loads(planner.intent_json)['case_input']['sku'],
                        clock=p.measurement.clock)
                    self._prepared=prepare_campaign_artifact(run_id=self.settings.run_id,planner=planner,crop=self._crop,
                        product=product,config=json.loads(self.settings.render_config_json),checked_at=p.measurement.clock())
                    artifact=self.engine.execute(self._prepared,recover_only=recover_only)
                    return dict(receipt,artifact=artifact,artifact_status='STORED_PRIVATE_CANDIDATE',
                        full_pipeline_qualification='NOT_EXECUTED',claim_approval=False,generation_authorized=False,
                        qualifying=False,release_allowed=False)
                return self.retrieved.execute_bound(headers,consume,recover_only=recover_only)
            finally:self._headers=None;self._prepared=None;self._crop=None
