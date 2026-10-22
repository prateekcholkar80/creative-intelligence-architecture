"""Private durable composition/artifact measurement from recovered planner inputs.

No provider dispatch, chat admission, source approval or serving activation. Cold
recovery never renders, uploads, reserves or publishes. Uncertain slots are held.
"""
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
import hashlib,json
import PIL

from src.application.campaign_artifact_measurement import measure_banner,VERSION as SCORE_VERSION
from src.application.campaign_attachment_service import AttachmentStore,inspect_file
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha
from src.application.campaign_planner_evaluation import PreparedPlannerEvaluation
from src.application.campaign_product_image_service import DerivedImageCapture
from src.application.campaign_quality_product_reader import ProductFactRead
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id
from src.generators.source_bound_campaign_renderer import ProductImageInput,GeneratedSceneInput,render_identity_campaign,VERSION as RENDER_VERSION

VERSION='private-durable-banner-composition-v1'


@dataclass(frozen=True,slots=True)
class PreparedCampaignArtifact:
    intent_json:str
    planner:PreparedPlannerEvaluation
    image:ProductImageInput
    product:ProductFactRead
    config_json:str
    scene:GeneratedSceneInput|None=None


def prepare_campaign_artifact(*,run_id,planner,crop,product,config,checked_at,retrieval_receipt=None,scene=None):
    _id(run_id)
    _require(type(planner) is PreparedPlannerEvaluation and type(crop) is DerivedImageCapture
        and type(product) is ProductFactRead and checked_at<crop.valid_until
        and product.fact.observed_at<=checked_at<product.fact.valid_until,'ARTIFACT_INPUT_EXPIRED')
    parent=json.loads(planner.intent_json);lineage=json.loads(crop.record_json)
    source=parent['source'];scope=source['context']['scope']
    if 'retrieval_evidence' in parent:
        _require(sha(parent['retrieval_evidence'])==parent['retrieval_evidence_sha256'],'ARTIFACT_RETRIEVAL_CHANGED')
        retrieval_sha=parent['retrieval_evidence_sha256'];retrieval_mode='IN_PLANNER_INPUT'
    else:
        _require(type(retrieval_receipt) is dict and all(retrieval_receipt[k] is False for k in
            ('claim_approval','generation_authorized','qualifying','release_allowed')),'ARTIFACT_RETRIEVAL_REQUIRED')
        for k in ('measurement_record_id','outcome_sha256','corpus_sha256','index_sha256'):
            from src.application.workflow_contract import _digest
            _digest(retrieval_receipt[k])
        retrieval_sha=sha(retrieval_receipt);retrieval_mode='POST_PLANNER_PRIVATE_CALIBRATION_NOT_IN_PLANNER_INPUT'
    _require(canonical(parent)==planner.intent_json and canonical(lineage)==crop.record_json
        and lineage['derivation_scope']==scope and lineage['principal_id']==source['principal_id']
        and lineage['policy_sha256']==source['context']['policy_sha256']
        and lineage['parent_binding_id']==parent['input_binding_id']
        and lineage['parent_manifest_sha256']==parent['input_manifest_sha256']
        and lineage['parent_source'] in source['sources']
        and crop.binding_id=='eval_image_derivation_v1_'+sha({k:v for k,v in lineage.items() if k!='derived_original'})
        and all(lineage[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed'))
        and product.fact.sku==scope['sku'] and product.fact.access_allowed is True,
        'ARTIFACT_SOURCE_BINDING')
    raw_lineage=dict(sku=scope['sku'],binding_id=crop.binding_id,content_sha256=lineage['derived_file']['content_sha256'],
        source_binding_id=lineage['parent_binding_id'],derivation_record_sha256=sha(lineage),
        generation=lineage['derived_original']['generation'],decision_sha256=lineage['decision_sha256'])
    image=ProductImageInput(scope['sku'],crop.binding_id,raw_lineage['content_sha256'],canonical(raw_lineage),crop.raw)
    # Native read IDs/times are evidence, not logical slot identity. Fresh readers
    # must match these stable product fields before every execution/recovery.
    render_profile=dict(version=RENDER_VERSION,pillow_version=PIL.__version__,config=config,
        font_sha256={k:hashlib.sha256(Path(config[k]).read_bytes()).hexdigest()
            for k in ('heading_font_path','body_font_path')})
    slot=sha([VERSION,run_id,parent['case_input']['case_id'],source['principal_id'],'ARTIFACT'])
    intent=dict(version=VERSION,record_id=slot,run_id=run_id,scope=scope,principal_id=source['principal_id'],
        planner_intent_sha256=sha(parent),planner_record_id=parent['record_id'],
        retrieval_evidence_sha256=retrieval_sha,retrieval_input_mode=retrieval_mode,
        image=raw_lineage,product_fields_json=product.fact.fields_json,render_profile=render_profile,
        qualifying=False,release_allowed=False)
    if retrieval_mode=='POST_PLANNER_PRIVATE_CALIBRATION_NOT_IN_PLANNER_INPUT':
        intent['retrieval_reference']=retrieval_receipt
    if scene is not None:
        _require(type(scene) is GeneratedSceneInput and scene.sku==scope['sku']
            and scene.source_image_binding_id==crop.binding_id,'ARTIFACT_SCENE_SCOPE')
        intent['generated_scene']=scene.pins()
    return PreparedCampaignArtifact(canonical(intent),planner,image,product,canonical(config),scene)


class DurableCampaignArtifact:
    def __init__(self,*,store,objects,guard,clock,enabled=False):
        _require(isinstance(store,EvaluatorStore) and isinstance(objects,AttachmentStore)
            and callable(guard) and callable(clock) and type(enabled) is bool,'ARTIFACT_COMPOSITION')
        self.store,self.objects,self.guard,self.clock,self.enabled=store,objects,guard,clock,enabled
        self._lock=RLock()

    def _guard(self,prepared,intent):
        check_operation()
        _require(prepared.product.fact.fields_json==intent['product_fields_json']
            and prepared.product.fact.access_allowed is True
            and prepared.product.fact.sku==intent['scope']['sku']
            and prepared.image.binding_id==intent['image']['binding_id']
            and json.loads(prepared.image.lineage_json)==intent['image']
            and sha(json.loads(prepared.planner.intent_json))==intent['planner_intent_sha256']
            and prepared.planner.options_json==json.loads(prepared.planner.intent_json)['options_json']
            and ((prepared.scene is None and 'generated_scene' not in intent)
                or (type(prepared.scene) is GeneratedSceneInput and prepared.scene.pins()==intent.get('generated_scene')))
            and canonical(json.loads(prepared.config_json))==prepared.config_json
            and json.loads(prepared.config_json)==intent['render_profile']['config']
            and PIL.__version__==intent['render_profile']['pillow_version']
            and all(hashlib.sha256(Path(json.loads(prepared.config_json)[k]).read_bytes()).hexdigest()==v
                for k,v in intent['render_profile']['font_sha256'].items()),'ARTIFACT_PREPARED_CHANGED')
        current=self.guard(intent)
        check_operation()
        _require(current is True or type(current) is ProductFactRead,'ARTIFACT_SOURCE_OR_ACCESS_CHANGED')
        product=prepared.product if current is True else current
        _require(product.fact.fields_json==intent['product_fields_json'] and product.fact.sku==intent['scope']['sku']
            and product.fact.access_allowed is True and product.fact.source_id==product.native_job_id
            and product.fact.observed_at<=self.clock()<product.fact.valid_until,'ARTIFACT_PRODUCT_CAPTURE_EXPIRED')
        return product

    def _bytes(self,intent,outcome):
        artifact=outcome['artifact'];original=artifact['original'];slot=intent['record_id']
        _require(original['bucket']==self.objects.bucket
            and original['object_name']=='campaign-evidence/campaign_artifact_v1_'+slot,
            'ARTIFACT_OBJECT_SCOPE')
        blob=self.objects.storage.bucket(original['bucket']).blob(original['object_name']);blob.reload(retry=None,timeout=20)
        _require(str(blob.generation)==original['generation'] and blob.size==artifact['file']['bytes']
            and blob.content_type=='image/png','ARTIFACT_GENERATION_CHANGED')
        raw=blob.download_as_bytes(if_generation_match=int(original['generation']),retry=None,timeout=30)
        _require(inspect_file('campaign.png',raw,'PRODUCT_IMAGE')==artifact['file'],'ARTIFACT_BYTES_CHANGED')
        return raw

    def execute(self,prepared,*,recover_only=False):
        require_owned_worker();check_operation()
        _require(self.enabled and type(prepared) is PreparedCampaignArtifact and type(recover_only) is bool,
            'ARTIFACT_EXECUTION_DISABLED')
        with self._lock:
            intent=json.loads(prepared.intent_json)
            _require(canonical(intent)==prepared.intent_json,'ARTIFACT_INTENT_CHANGED')
            self._guard(prepared,intent)
            planner_intent=json.loads(prepared.planner.intent_json)
            planner=self.store.recover(planner_intent)
            _require(type(planner) is dict and planner.get('state')=='SUCCEEDED'
                and planner.get('fresh_source_check')=='PASS' and planner.get('finish_reason')=='STOP'
                and planner.get('safety_outcome')=='PASSED'
                and planner.get('returned_model_id')==planner_intent['profile']['model_id']
                and type(planner.get('response_text')) is str
                and hashlib.sha256(planner['response_text'].encode()).hexdigest()==planner.get('response_sha256'),
                'ARTIFACT_PLANNER_ORIGINAL_UNAVAILABLE')
            # The stable slot cannot be reminted when a provider outcome changes.
            bound=dict(intent,planner_outcome_sha256=sha(planner))
            outcome=self.store.recover(bound)
            if outcome is None:
                _require(not recover_only,'ARTIFACT_ORIGINAL_MISSING')
                check_operation()
                reservation=self.store.reserve(bound)
                _require(reservation.newly_reserved,'ARTIFACT_ORIGINAL_UNCERTAIN')
                product=self._guard(prepared,intent)
                rendered=render_identity_campaign(product=product,image=prepared.image,
                    planner_response=planner['response_text'],options_json=prepared.planner.options_json,
                    checked_at=self.clock(),config=json.loads(prepared.config_json),scene=prepared.scene)
                metadata=json.loads(rendered.metadata_json);file=inspect_file('campaign.png',rendered.png,'PRODUCT_IMAGE')
                self._guard(prepared,intent)
                self.objects.preflight()
                original=self.objects.put_original('campaign_artifact_v1_'+intent['record_id'],rendered.png,file)
                self._guard(prepared,intent)
                outcome=dict(version=VERSION,record_id=intent['record_id'],intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),
                    state='STORED_PRIVATE_CANDIDATE',artifact=dict(original=original,file=file,metadata=metadata),
                    product_native_job_id=product.native_job_id,product_capture_sha256=product.fact.sha256,
                    qualifying=False,release_allowed=False)
                self.store.publish(reservation,outcome)
            _require(outcome.get('state')=='STORED_PRIVATE_CANDIDATE' and outcome.get('qualifying') is False
                and outcome.get('release_allowed') is False,'ARTIFACT_ORIGINAL_UNCERTAIN')
            raw=self._bytes(intent,outcome);self._guard(prepared,intent)
            score_intent=dict(version=SCORE_VERSION,record_id=sha([intent['record_id'],'SCORING']),
                artifact_record_id=intent['record_id'],artifact_outcome_sha256=sha(outcome),
                planner_outcome_sha256=sha(planner),qualifying=False,release_allowed=False)
            score=self.store.recover(score_intent)
            measured=measure_banner(raw,outcome['artifact']['metadata'],image=prepared.image,
                planner_response=planner['response_text'],options_json=prepared.planner.options_json,
                config=json.loads(prepared.config_json),scene=prepared.scene)
            if score is None:
                _require(not recover_only,'ARTIFACT_SCORE_ORIGINAL_MISSING')
                check_operation()
                reservation=self.store.reserve(score_intent)
                _require(reservation.newly_reserved,'ARTIFACT_SCORE_ORIGINAL_UNCERTAIN')
                self._guard(prepared,intent)
                score=dict(record_id=score_intent['record_id'],intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),
                    state='MEASURED',measurement=measured,qualifying=False,release_allowed=False)
                self.store.publish(reservation,score)
            _require(score.get('state')=='MEASURED' and score.get('measurement')==measured
                and score.get('qualifying') is False and score.get('release_allowed') is False,'ARTIFACT_MEASUREMENT_CHANGED')
            self._guard(prepared,intent)
            return dict(artifact_record_id=intent['record_id'],artifact_outcome_sha256=sha(outcome),
                scoring_record_id=score_intent['record_id'],scoring_outcome_sha256=sha(score),
                original=outcome['artifact']['original'],content_sha256=hashlib.sha256(raw).hexdigest(),
                measurements=measured,qualifying=False,release_allowed=False)
