"""Disabled-by-default owned-worker planner measurement kernel, not a serving path.

A protected host must verify exact registry/source authority in guard(). This
kernel cannot install itself or approve inputs. Full 47-check/artifact scoring
remains unavailable until its separate measured evaluator is connected.
"""
from dataclasses import dataclass,asdict
from datetime import datetime,timezone
from threading import RLock
import hashlib,json
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha
from src.application.campaign_evaluator_observer import EvaluatorProfile,USAGE
from src.application.campaign_evaluator_transport import PinnedVertexEvaluatorTransport,APPLICABILITY_VERSION
from src.application.campaign_evaluation_uploads import EvaluationUploadInputs
from src.application.campaign_grounded_planner_definition import KEY,SYSTEM,TEMPLATE
from src.application.campaign_planner_projection import validate_options,project_planner_response
from src.application.prompt_contract import VertexPromptSnapshot,PromptTask
from src.application.prompt_runtime_contract import render_prompt
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id,_digest
from src.generators.campaign_brief_generator import response_schema


@dataclass(frozen=True,slots=True)
class PreparedPlannerEvaluation:
    intent_json:str
    images:tuple
    options_json:str


def prepare_planner_evaluation(*,run_id,case_input,snapshot,registry_record_digest,uploads,binding_id,options_json,grounding=None):
    for value in (run_id,case_input['case_id']):_id(value)
    _digest(registry_record_digest);_digest(case_input['case_sha256'])
    _require(type(snapshot) is VertexPromptSnapshot and snapshot.task is PromptTask.PLANNER
        and snapshot.prompt_key==KEY and snapshot.system_instruction==SYSTEM and snapshot.prompt_template==TEMPLATE,
        'PLANNER_EVALUATION_REGISTERED_DEFINITION_REQUIRED')
    _require(type(uploads) is EvaluationUploadInputs and binding_id=='eval_upload_binding_v1_'+uploads.sha256,
        'PLANNER_EVALUATION_UPLOAD_BINDING')
    _require(set(case_input)=={'case_id','case_sha256','stage','sku','country','channel','turns'}
        and case_input['stage']=='GENERATION' and type(case_input['turns']) is list and bool(case_input['turns']),
        'PLANNER_EVALUATION_CASE_INPUT')
    for turn in case_input['turns']:
        _require(type(turn) is dict and set(turn)=={'role','text'} and turn['role'] in ('USER','ASSISTANT')
            and type(turn['text']) is str,'PLANNER_EVALUATION_CASE_TURN')
    source=json.loads(uploads.manifest_json);scope=source['context']['scope']
    _require(all(case_input[k]==scope[k] for k in ('sku','country','channel')),'PLANNER_EVALUATION_CASE_SOURCE_SCOPE')
    options=validate_options(options_json)
    system,content=render_prompt(snapshot,dict(campaign_request=canonical(case_input),
        evidence_bundle_json=canonical(dict(uploaded_visual_reference=source,approved_claims=[],retrieval_status='NOT_EXECUTED',product_grounding=grounding)),
        creative_options_json=options_json))
    profile=EvaluatorProfile(snapshot.model_id,canonical(dict(version=APPLICABILITY_VERSION,
        source_snapshot_sha256=snapshot.snapshot_sha256,generation_config=json.loads(snapshot.generation_config_json),
        safety_settings=json.loads(snapshot.safety_settings_json))),hashlib.sha256(system.encode()).hexdigest())
    slot=sha([run_id,case_input['case_id'],source['principal_id'],'GENERATION_STAGE'])
    intent=dict(version='benchmark-grounded-planner-stage-v1',record_id=slot,run_id=run_id,case_input=case_input,
        prompt_snapshot_sha256=snapshot.snapshot_sha256,registry_record_digest=registry_record_digest,
        input_binding_id=binding_id,input_manifest_sha256=uploads.sha256,source=source,
        profile=asdict(profile),system=system,content=content,response_schema=response_schema(options,snapshot.model_id),
        options_json=options_json,grounding=grounding,execution_lane='EVALUATION_ONLY_NOT_SERVING',release_allowed=False,qualifying=False)
    return PreparedPlannerEvaluation(canonical(intent),uploads.images,options_json)


class DurablePlannerEvaluation:
    def __init__(self,*,store,guard,transport=None,enabled=False):
        _require(isinstance(store,EvaluatorStore) and callable(guard) and type(enabled) is bool,'PLANNER_EVALUATION_CONFIG')
        self.store,self.guard,self.transport,self.enabled=store,guard,transport or PinnedVertexEvaluatorTransport(),enabled
        self._lock=RLock();self._pending={}

    @staticmethod
    def _pins(prepared,intent):
        pins=[]
        for asset,generation,mime,raw in prepared.images:
            pins.append(dict(asset_id=asset,generation=generation,mime_type=mime,byte_count=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
        _require(pins==intent['source']['image_pins'] and prepared.options_json==intent['options_json'],
            'PLANNER_EVALUATION_PREPARED_INPUT_CHANGED')

    def _recover(self,intent,recover_only):
        slot=intent['record_id']
        if recover_only and slot in self._pending:
            reservation,outcome=self._pending[slot];self.store.publish(reservation,outcome);self._pending.pop(slot)
        return self.store.recover(intent)

    def _publish(self,reservation,outcome):
        _require(len(self._pending)<16,'PLANNER_EVALUATION_PENDING_CAPACITY')
        self._pending[reservation.record_id]=(reservation,outcome)
        self.store.publish(reservation,outcome);self._pending.pop(reservation.record_id)

    def execute(self,prepared,*,recover_only=False):
        require_owned_worker();check_operation()
        _require(self.enabled and type(prepared) is PreparedPlannerEvaluation and type(recover_only) is bool,'PLANNER_EVALUATION_DISABLED')
        with self._lock:
            intent=json.loads(prepared.intent_json);_require(canonical(intent)==prepared.intent_json,'PLANNER_EVALUATION_INPUT')
            self._pins(prepared,intent);self.guard(intent)
            outcome=self._recover(intent,recover_only)
            if outcome is None:
                _require(not recover_only,'PLANNER_EVALUATION_NO_ORIGINAL')
                reservation=self.store.reserve(intent)
                _require(reservation.newly_reserved,'PLANNER_EVALUATION_ORIGINAL_UNCERTAIN')
                dispatch_evidence=self.guard(intent);check_operation()
                profile=EvaluatorProfile(**intent['profile'])
                outcome=dict(version='benchmark-grounded-planner-outcome-v1',record_id=intent['record_id'],
                    intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),state='UNKNOWN',
                    response_text=None,response_sha256=None,returned_model_id=None,finish_reason=None,safety_outcome=None,
                    **dict.fromkeys(USAGE),qualifying=False,release_allowed=False,
                    dispatch_source_evidence=dispatch_evidence if type(dispatch_evidence) is dict else None)
                try:
                    result=self.transport.planner(profile,intent['system'],intent['content'],intent['response_schema'],prepared.images)
                    obs=result.observation
                    outcome.update(state='SUCCEEDED' if obs.state=='SUCCEEDED' and obs.returned_model_id==profile.model_id
                        and obs.finish_reason=='STOP' and obs.safety_outcome=='PASSED'
                        and obs.response_sha256==hashlib.sha256(result.text.encode()).hexdigest() else 'REJECTED',
                        response_text=result.text,response_sha256=obs.response_sha256,returned_model_id=obs.returned_model_id,
                        finish_reason=obs.finish_reason,safety_outcome=obs.safety_outcome,**{k:getattr(obs,k) for k in USAGE})
                except Exception:outcome['safe_error_code']='PLANNER_PROVIDER_ERROR_OR_UNKNOWN'
                try:
                    final_evidence=self.guard(intent);outcome['fresh_source_check']='PASS'
                    outcome['final_source_evidence']=final_evidence if type(final_evidence) is dict else None
                except Exception:outcome['fresh_source_check']='UNAVAILABLE'
                outcome['completed_at']=datetime.now(timezone.utc).isoformat();self._publish(reservation,outcome)
            _require(outcome.get('state')=='SUCCEEDED' and outcome.get('returned_model_id')==intent['profile']['model_id']
                and outcome.get('finish_reason')=='STOP' and outcome.get('safety_outcome')=='PASSED'
                and type(outcome.get('response_text')) is str
                and hashlib.sha256(outcome['response_text'].encode()).hexdigest()==outcome.get('response_sha256'),
                'PLANNER_EVALUATION_STAGE_UNAVAILABLE')
            self.guard(intent)
            score_intent=dict(version='planner-projection-measurement-intent-v1',record_id=sha([intent['record_id'],'SCORING']),
                stage_record_id=intent['record_id'],stage_outcome_sha256=sha(outcome),options_sha256=sha(json.loads(prepared.options_json)),
                qualifying=False,release_allowed=False)
            score=self._recover(score_intent,recover_only)
            if score is None:
                _require(not recover_only,'PLANNER_EVALUATION_SCORING_ORIGINAL_MISSING')
                reservation=self.store.reserve(score_intent);_require(reservation.newly_reserved,'PLANNER_EVALUATION_ORIGINAL_UNCERTAIN')
                try:
                    projection=project_planner_response(outcome['response_text'],prepared.options_json)
                    measured=dict(status='PASS',projection_sha256=projection.sha256)
                except ValueError:measured=dict(status='FAIL',projection_sha256=None)
                score=dict(version='planner-projection-measurement-outcome-v1',record_id=score_intent['record_id'],
                    intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),state='MEASURED',
                    checks=dict(schema_and_enum_projection=measured,fresh_source_check=outcome.get('fresh_source_check','UNAVAILABLE'),
                        full_47_check_evaluation='UNAVAILABLE',retrieval_measurement='UNAVAILABLE',rendered_artifact_evaluation='UNAVAILABLE'),
                    qualifying=False,release_allowed=False)
                self._publish(reservation,score)
            self.guard(intent)
            return dict(stage_record_id=intent['record_id'],response_sha256=outcome['response_sha256'],
                returned_model_id=outcome['returned_model_id'],usage={k:outcome[k] for k in USAGE},
                scoring_record_id=score_intent['record_id'],checks=score['checks'],qualifying=False,release_allowed=False)
