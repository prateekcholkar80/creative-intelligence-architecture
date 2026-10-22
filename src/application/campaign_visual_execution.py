"""Once-reserved private Vertex scene with immutable native records/GCS bytes.

Not installed by this module. Recovery is read-only. Missing/uncertain outcomes
never permit a second model call, alternate object or repaired original outcome.
"""
from dataclasses import dataclass
from threading import RLock
import hashlib,json
from src.application.campaign_attachment_service import AttachmentStore,inspect_file
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha
from src.application.campaign_evaluator_observer import USAGE
from src.application.campaign_visual_request import validate_visual_request
from src.application.operation_budget import require_owned_worker,check_operation,OperationStopped
from src.application.workflow_contract import _require
from src.generators.vertex_campaign_visual import GeneratedVisual
from src.generators.source_bound_campaign_renderer import GeneratedSceneInput


@dataclass(frozen=True,slots=True)
class VisualExecution:
    scene:GeneratedSceneInput
    receipt_json:str


class DurableVertexVisual:
    def __init__(self,*,store,objects,transport,guard,enabled=False):
        _require(isinstance(store,EvaluatorStore) and isinstance(objects,AttachmentStore)
            and callable(transport) and callable(guard) and type(enabled) is bool,'VISUAL_EXECUTION_COMPOSITION')
        self.store,self.objects,self.transport,self.guard,self.enabled=store,objects,transport,guard,enabled
        self._lock=RLock()

    def _guard(self,intent):
        check_operation();_require(self.guard(intent) is True,'VISUAL_SOURCE_OR_ACCESS_CHANGED');check_operation()

    def _bytes(self,intent,outcome):
        artifact=outcome['artifact'];original=artifact['original'];file=artifact['file']
        _require(original['bucket']==self.objects.bucket
            and original['object_name']=='campaign-evidence/vertex_visual_v1_'+intent['record_id'],'VISUAL_OBJECT_SCOPE')
        blob=self.objects.storage.bucket(original['bucket']).blob(original['object_name']);blob.reload(retry=None,timeout=20)
        _require(str(blob.generation)==original['generation'] and blob.size==file['bytes']
            and blob.content_type==file['content_type'],'VISUAL_OBJECT_GENERATION_CHANGED')
        raw=blob.download_as_bytes(if_generation_match=int(original['generation']),retry=None,timeout=30)
        _require(inspect_file(file['filename'],raw,'REFERENCE_CREATIVE')==file,'VISUAL_OBJECT_BYTES_CHANGED')
        return raw

    def execute(self,request,*,recover_only=False):
        require_owned_worker();check_operation();profile=validate_visual_request(request)
        _require(self.enabled and type(recover_only) is bool,'VISUAL_EXECUTION_DISABLED')
        # The processing-location decision is required even before reservation.
        _require(profile.endpoint_authorized,'VISUAL_ENDPOINT_AUTHORIZATION_REQUIRED')
        with self._lock:
            intent=request.intent;self._guard(intent);outcome=self.store.recover(intent)
            if outcome is None:
                _require(not recover_only,'VISUAL_ORIGINAL_MISSING');check_operation()
                reservation=self.store.reserve(intent);_require(reservation.newly_reserved,'VISUAL_ORIGINAL_UNCERTAIN')
                self._guard(intent)
                outcome=dict(version='private-vertex-visual-outcome-v1',record_id=intent['record_id'],
                    intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),state='UNKNOWN',artifact=None,
                    returned_model_id=None,response_sha256=None,finish_reason=None,safety_outcome=None,
                    **dict.fromkeys(USAGE),claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
                try:
                    result=self.transport(request)
                    _require(type(result) is GeneratedVisual,'VISUAL_TRANSPORT_RESULT')
                    obs=result.observation
                    outcome.update(returned_model_id=obs.returned_model_id,response_sha256=obs.response_sha256,
                        finish_reason=obs.finish_reason,safety_outcome=obs.safety_outcome,**{k:getattr(obs,k) for k in USAGE})
                    _require(obs.state=='SUCCEEDED' and obs.returned_model_id==profile.model_id
                        and obs.finish_reason=='STOP' and obs.safety_outcome=='PASSED'
                        and obs.response_sha256==hashlib.sha256(result.response_manifest_json.encode()).hexdigest(),
                        'VISUAL_TRANSPORT_OUTCOME_UNVERIFIED')
                    manifest=json.loads(result.response_manifest_json)
                    pins=[p for p in manifest['parts'] if p['kind']=='image' and p['thought'] is False]
                    _require(len(pins)==1 and pins[0]['sha256']==hashlib.sha256(result.raw).hexdigest()
                        and pins[0]['mime_type']==result.mime_type and pins[0]['bytes']==len(result.raw),'VISUAL_TRANSPORT_BYTES_CHANGED')
                    outcome['response_manifest_json']=result.response_manifest_json
                    self._guard(intent);self.objects.preflight()
                    filename='vertex-scene.png' if result.mime_type=='image/png' else 'vertex-scene.jpg'
                    file=inspect_file(filename,result.raw,'REFERENCE_CREATIVE')
                    original=self.objects.put_original('vertex_visual_v1_'+intent['record_id'],result.raw,file)
                    outcome['artifact']=dict(original=original,file=file)
                    self._guard(intent);outcome['state']='GENERATED_PRIVATE_SCENE'
                except OperationStopped:raise
                except Exception:
                    outcome['safe_error_code']='VISUAL_PROVIDER_STORAGE_OR_SOURCE_UNVERIFIED'
                check_operation();self.store.publish(reservation,outcome)
            _require(outcome.get('state')=='GENERATED_PRIVATE_SCENE' and outcome.get('returned_model_id')==profile.model_id
                and outcome.get('finish_reason')=='STOP' and outcome.get('safety_outcome')=='PASSED'
                and all(outcome.get(k) is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed')),
                'VISUAL_ORIGINAL_UNAVAILABLE_OR_UNKNOWN')
            raw=self._bytes(intent,outcome);self._guard(intent)
            scene=GeneratedSceneInput(intent['scope']['sku'],intent['image']['binding_id'],intent['record_id'],sha(outcome),
                hashlib.sha256(raw).hexdigest(),raw,profile.model_id)
            receipt=dict(record_id=intent['record_id'],outcome_sha256=sha(outcome),response_sha256=outcome['response_sha256'],
                returned_model_id=outcome['returned_model_id'],usage={k:outcome[k] for k in USAGE},
                original=outcome['artifact']['original'],content_sha256=scene.content_sha256,
                claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
            return VisualExecution(scene,canonical(receipt))
