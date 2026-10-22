"""Protected, phase-bounded Vertex visual pilot using saved Rejuvenate evidence.

Uses the original identity planner without pretending retrieval was in its prompt.
Visual generation and banner storage are separate bounded worker actions; banner
execution can only recover the image-model result, never dispatch that model.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
from threading import RLock
import hashlib,json
from src.application.campaign_artifact_execution import DurableCampaignArtifact,prepare_campaign_artifact
from src.application.campaign_corpus_preparation import BoundCorpusCapture
from src.application.campaign_corpus_retrieval import ProtectedCorpusRetrieval
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_planner_runtime import ProtectedPlannerEvaluation
from src.application.campaign_planner_projection import project_planner_response
from src.application.campaign_product_image_service import ProtectedProductImage
from src.application.campaign_product_image_service import DerivedImageCapture
from src.application.campaign_quality_product_reader import read_product_fact
from src.application.campaign_visual_execution import DurableVertexVisual
from src.application.campaign_visual_request import VisualProfile,prepare_visual_request
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_digest,_id
from src.generators.vertex_campaign_visual import VertexCampaignVisualTransport
from src.retrieval.upload_corpus import parallel_reads


@dataclass(frozen=True,slots=True)
class VertexCampaignSettings:
    run_id:str
    case_id:str
    render_config_json:str
    render_config_sha256:str
    profile:VisualProfile
    crop_binding_id:str
    enabled:bool=False

    def __post_init__(self):
        _id(self.run_id);_id(self.case_id);_digest(self.render_config_sha256)
        _require(type(self.crop_binding_id) is str and self.crop_binding_id.startswith('eval_image_derivation_v1_'),
            'VERTEX_CAMPAIGN_CROP_SETTING');_digest(self.crop_binding_id.removeprefix('eval_image_derivation_v1_'))
        _require(type(self.enabled) is bool and type(self.profile) is VisualProfile
            and (not self.enabled or self.profile.endpoint_authorized)
            and type(self.render_config_json) is str and len(self.render_config_json.encode())<=8192
            and canonical(json.loads(self.render_config_json))==self.render_config_json
            and sha(json.loads(self.render_config_json))==self.render_config_sha256,'VERTEX_CAMPAIGN_SETTINGS')


class ProtectedVertexCampaign:
    def __init__(self,planner,retrieval,product_image,settings,*,transport=None):
        require_owned_worker()
        _require(type(planner) is ProtectedPlannerEvaluation and type(retrieval) is ProtectedCorpusRetrieval
            and type(product_image) is ProtectedProductImage and type(settings) is VertexCampaignSettings
            and planner.documents is product_image.documents is retrieval.embedding.corpus.documents
            and planner.measurement.client is retrieval.store.client,'VERTEX_CAMPAIGN_COMPOSITION')
        self.planner,self.retrieval,self.product_image,self.settings=planner,retrieval,product_image,settings
        self._lock=RLock();self._clear()
        self.visual=DurableVertexVisual(store=retrieval.store,objects=product_image.documents.intake.store,
            transport=transport or VertexCampaignVisualTransport(),guard=self._guard_visual,enabled=settings.enabled)
        self.artifacts=DurableCampaignArtifact(store=retrieval.store,objects=product_image.documents.intake.store,
            guard=self._guard_artifact,clock=planner.measurement.clock,enabled=settings.enabled)

    def _clear(self):
        self._headers=self._bound=self._crop=self._planner=self._pins=self._visual_request=self._artifact_input=None

    def _scope(self):
        p=self.planner
        return dict(workspace_id=p.measurement.settings.workspace_id,conversation_id=p.settings.conversation_id,
            request_id=p.settings.request_id)

    def _capture_planner(self,headers):
        inputs,snapshot,pins,evidence=self.planner._capture(headers)
        prepared=self.planner._prepare(inputs,snapshot,pins)
        original=self.retrieval.store.recover(json.loads(prepared.intent_json))
        _require(type(original) is dict and original.get('state')=='SUCCEEDED'
            and original.get('fresh_source_check')=='PASS' and original.get('returned_model_id')==snapshot.model_id
            and original.get('finish_reason')=='STOP' and original.get('safety_outcome')=='PASSED'
            and type(original.get('response_text')) is str
            and hashlib.sha256(original['response_text'].encode()).hexdigest()==original.get('response_sha256'),
            'VERTEX_CAMPAIGN_PLANNER_ORIGINAL_UNAVAILABLE')
        project_planner_response(original['response_text'],prepared.options_json)
        return inputs,prepared,pins,original

    def _fresh(self):
        require_owned_worker();check_operation()
        _require(self._headers is not None and type(self._bound) is BoundCorpusCapture
            and self._crop is not None and self._planner is not None,'VERTEX_CAMPAIGN_CONTEXT')
        p=self.planner;docs=p.documents;crop_record=json.loads(self._crop.record_json)
        def crop_current():
            _require(self.product_image._read(self._crop.binding_id)==crop_record,'VERTEX_CAMPAIGN_CROP_CHANGED')
            original=crop_record['derived_original'];file=crop_record['derived_file']
            _require(original['bucket']==docs.intake.store.bucket
                and original['object_name']=='campaign-evidence/'+self._crop.binding_id,'VERTEX_CAMPAIGN_CROP_CHANGED')
            blob=docs.intake.store.storage.bucket(original['bucket']).blob(original['object_name']);blob.reload(retry=None,timeout=20)
            _require(str(blob.generation)==original['generation'] and blob.size==file['bytes']
                and blob.content_type==file['content_type'],'VERTEX_CAMPAIGN_CROP_CHANGED')
            return True
        def registry_current():
            state=p.registry.read_exact(p.settings.prompt_id,p.settings.prompt_version).state.head
            _require(state.digest==self._pins['registry_digest']
                and state.snapshot.snapshot_sha256==p.settings.prompt_snapshot_sha256,'VERTEX_CAMPAIGN_PROMPT_CHANGED')
            return True
        current,product,_,_=parallel_reads((self._bound.refresh,
            lambda:read_product_fact(p.measurement.client,crop_record['derivation_scope']['sku'],clock=p.measurement.clock),
            crop_current,registry_current))
        _require(current is True and json.loads(product.fact.fields_json)==self._pins['grounding']['product_fields'],
            'VERTEX_CAMPAIGN_SOURCE_CHANGED')
        actor=p.measurement._actor(self._headers)
        _require(actor.principal.principal_id==crop_record['principal_id'],'VERTEX_CAMPAIGN_OWNER_CHANGED')
        check_operation();return product

    def _guard_visual(self,intent):
        _require(self._visual_request is not None and self._visual_request.intent==intent,'VERTEX_CAMPAIGN_VISUAL_CHANGED')
        self._fresh();return True

    def _rechecked_crop(self,product):
        """New action-local capture after _fresh verified rows/access/generations.

        Immutable generation-pinned bytes need no second download. This grants no
        source approval and never extends the parent operation or IAP lifetime.
        """
        actor=self.planner.measurement._actor(self._headers)
        until=min(product.fact.valid_until,datetime.fromtimestamp(actor.expires_at,tz=timezone.utc))
        _require(self.planner.measurement.clock()<until,'VERTEX_CAMPAIGN_SOURCE_EXPIRED')
        return DerivedImageCapture(self._crop.binding_id,self._crop.record_json,self._crop.raw,until)

    def _guard_artifact(self,intent):
        _require(self._artifact_input is not None and json.loads(self._artifact_input.intent_json)==intent,
            'VERTEX_CAMPAIGN_ARTIFACT_CHANGED')
        return self._fresh()

    def execute(self,headers,*,phase,recover_only=False):
        require_owned_worker();check_operation()
        _require(self.settings.enabled and phase in {'VISUAL','BANNER'} and type(recover_only) is bool,'VERTEX_CAMPAIGN_DISABLED')
        with self._lock:
            try:
                self._headers=headers
                captured,crop,bound_and_receipt=parallel_reads((lambda:self._capture_planner(headers),
                    lambda:self.product_image.capture_bound(headers,**self._scope()),
                    lambda:self.retrieval.execute_bound(headers,lambda bound,receipt:(bound,receipt),recover_only=True)))
                inputs,prepared,self._pins,planner_outcome=captured
                _require(crop.binding_id==self.settings.crop_binding_id,'VERTEX_CAMPAIGN_CROP_CHANGED')
                self._planner=prepared;self._crop=crop;self._bound,receipt=bound_and_receipt
                manifest=json.loads(self._bound.capture.manifest_json)
                _require(type(self._bound) is BoundCorpusCapture and manifest['scope']==json.loads(inputs.manifest_json)['context']['scope']
                    and manifest['image_manifest_sha256']==inputs.sha256
                    and manifest['image_binding_id']==self.planner.settings.binding_id
                    and receipt['corpus_sha256']==self.retrieval.embedding.settings.corpus_sha256,
                    'VERTEX_CAMPAIGN_SOURCE_CHANGED')
                product=self._fresh();config=json.loads(self.settings.render_config_json);now=self.planner.measurement.clock()
                current_crop=self._rechecked_crop(product)
                base=prepare_campaign_artifact(run_id=self.settings.run_id,planner=prepared,crop=current_crop,product=product,
                    config=config,checked_at=now,retrieval_receipt=receipt)
                self._visual_request=prepare_visual_request(run_id=self.settings.run_id,case_id=self.settings.case_id,
                    artifact_input=json.loads(base.intent_json),crop=current_crop,planner_outcome_sha256=sha(planner_outcome),
                    checked_at=now,profile=self.settings.profile)
                visual=self.visual.execute(self._visual_request,recover_only=(recover_only or phase=='BANNER'))
                result=dict(run_id=self.settings.run_id,case_id=self.settings.case_id,phase=phase,
                    crop_binding_id=crop.binding_id,
                    visual=json.loads(visual.receipt_json),semantic_reranking='NOT_EXECUTED',
                    retrieval_use='POST_PLANNER_PRIVATE_CALIBRATION_NOT_IN_PLANNER_INPUT',
                    artifact=None,claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
                if phase=='BANNER':
                    product=self._fresh()
                    self._artifact_input=prepare_campaign_artifact(run_id=self.settings.run_id,planner=prepared,crop=self._rechecked_crop(product),
                        product=product,config=config,checked_at=self.planner.measurement.clock(),retrieval_receipt=receipt,scene=visual.scene)
                    result['artifact']=self.artifacts.execute(self._artifact_input,recover_only=recover_only)
                return result
            finally:self._clear()
