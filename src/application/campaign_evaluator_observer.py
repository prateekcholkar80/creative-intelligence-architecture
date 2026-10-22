"""Evaluation-only, once-reserved observer. Never grants serving eligibility."""
from dataclasses import dataclass,asdict,replace
from datetime import datetime,timezone
import hashlib,json
from threading import RLock
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.workflow_contract import _require,_digest,_id
from src.application.operation_budget import require_owned_worker,check_operation

VERSION='campaign-durable-evaluator-v1'
USAGE=('input_tokens','output_tokens','cached_tokens','thinking_tokens')


@dataclass(frozen=True,slots=True)
class StageEvaluationBinding:
    workspace_id:str
    conversation_id:str
    request_id:str
    principal_id:str
    stage:str
    invocation_id:str
    response_sha256:str
    prompt_binding_sha256:str
    authority_sha256:str
    candidate_sha256:str

    def __post_init__(self):
        for name in ('workspace_id','conversation_id','request_id','principal_id','invocation_id'):_id(getattr(self,name))
        for name in ('response_sha256','prompt_binding_sha256','authority_sha256','candidate_sha256'):_digest(getattr(self,name))
        _require(self.stage=='INTERPRETATION','EVALUATOR_FIRST_STAGE_ONLY')


@dataclass(frozen=True,slots=True)
class EvaluatorProfile:
    model_id:str
    settings_json:str
    template_sha256:str

    def __post_init__(self):
        _id(self.model_id);_digest(self.template_sha256)
        _require(type(self.settings_json) is str and len(self.settings_json.encode())<=16384
            and canonical(json.loads(self.settings_json))==self.settings_json,'EVALUATOR_PROFILE_CONFIG')


@dataclass(frozen=True,slots=True)
class EvaluatorProviderResponse:
    text:str
    observation:ModelResultObservation


class DurableEvaluatorObserver:
    def __init__(self,*,store,run_id,evaluator_sha256,profile,transport,guard,enabled=False,binding=None,recover_only=False):
        _require(isinstance(store,EvaluatorStore) and type(profile) is EvaluatorProfile
            and callable(transport) and callable(guard) and type(enabled) is bool and type(recover_only) is bool,'EVALUATOR_OBSERVER_CONFIG')
        _id(run_id);_digest(evaluator_sha256)
        _require(binding is None or type(binding) is StageEvaluationBinding,'EVALUATOR_STAGE_BINDING')
        self.store,self.run_id,self.evaluator_sha256,self.profile=store,run_id,evaluator_sha256,profile
        self.transport,self.guard,self.enabled,self.binding=transport,guard,enabled,binding
        self.recover_only=recover_only
        self._lock=RLock();self._used=set();self._pending={};self._bound={};self._receipts={}

    def for_stage(self,binding):
        _require(type(binding) is StageEvaluationBinding,'EVALUATOR_STAGE_BINDING')
        with self._lock:
            key=sha(asdict(binding))
            if key not in self._bound:
                _require(len(self._bound)<16,'EVALUATOR_BOUND_CAPACITY')
                self._bound[key]=DurableEvaluatorObserver(store=self.store,run_id=self.run_id,evaluator_sha256=self.evaluator_sha256,
                    profile=self.profile,transport=self.transport,guard=self.guard,enabled=self.enabled,binding=binding,recover_only=self.recover_only)
            return self._bound[key]

    @property
    def receipts(self):return tuple(self._receipts[k] for k in sorted(self._receipts))

    def _record_receipt(self,outcome):
        slot=outcome['record_id']
        self._receipts[slot]=dict(record_id=slot,intent_native_job_id='evaluator_intents_v1_'+slot,
            outcome_native_job_id='evaluator_outcomes_v1_'+slot,outcome_sha256=sha(outcome),
            response_sha256=outcome['response_sha256'],returned_model_id=outcome['returned_model_id'],
            usage={k:outcome[k] for k in USAGE},qualifying=False)

    def _intent(self,system,content,rules):
        _require(self.enabled and type(self.binding) is StageEvaluationBinding,'EVALUATOR_DISABLED_OR_UNBOUND')
        _require(type(system) is str and 0<len(system.encode())<=16384
            and type(content) is str and 0<len(content.encode())<=49152
            and type(rules) is tuple and rules==tuple(sorted(set(rules))) and bool(rules),'EVALUATOR_INPUT')
        _require(hashlib.sha256(system.encode()).hexdigest()==self.profile.template_sha256,'EVALUATOR_TEMPLATE_CHANGED')
        # Slot excludes changing candidate/config/observation bytes. Conflicting
        # attempts cannot mint another slot for the same run/stage/rule batch.
        slot=sha([self.run_id,self.binding.workspace_id,self.binding.invocation_id,list(rules)])
        return dict(version=VERSION,record_id=slot,run_id=self.run_id,binding=asdict(self.binding),
            evaluator_sha256=self.evaluator_sha256,profile=asdict(self.profile),
            system=system,content=content,rule_ids=list(rules),
            input_sha256=sha(dict(system=system,content=content,rule_ids=list(rules))),
            prompt_registration='VERSIONED_EVALUATION_TEMPLATE_NOT_SERVING_APPROVAL',release_allowed=False)

    def _result(self,outcome):
        _require(outcome['state']=='SUCCEEDED' and outcome['returned_model_id']==self.profile.model_id
            and outcome['finish_reason']=='STOP' and outcome['safety_outcome']=='PASSED'
            and type(outcome['response_text']) is str
            and hashlib.sha256(outcome['response_text'].encode()).hexdigest()==outcome['response_sha256'],
            'EVALUATOR_OUTCOME_NOT_USABLE')
        for key in USAGE:_require(outcome[key] is None or type(outcome[key]) is int and 0<=outcome[key]<2**63,'EVALUATOR_USAGE')
        from src.application.campaign_planner_projection import parse_json
        return parse_json(outcome['response_text'])

    def __call__(self,system,content,rules):
        require_owned_worker();check_operation()
        with self._lock:
            intent=self._intent(system,content,rules);slot=intent['record_id']
            self.guard(self.binding)
            recovered=self.store.recover(intent)
            if recovered is not None:
                self.guard(self.binding);result=self._result(recovered);self._record_receipt(recovered);return result
            _require(not self.recover_only,'EVALUATOR_RECOVERY_NO_DISPATCH')
            _require(slot not in self._used,'EVALUATOR_DISPATCH_ALREADY_CONSUMED')
            self._used.add(slot)
            reservation=self.store.reserve(intent)
            _require(reservation.newly_reserved,'EVALUATOR_RESERVATION_RECOVERY_NO_DISPATCH')
            # Guard failure after reservation permanently holds this original slot.
            self.guard(self.binding);check_operation()
            outcome=dict(version=VERSION,record_id=slot,intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),
                state='UNKNOWN',response_text=None,response_sha256=None,returned_model_id=None,finish_reason=None,safety_outcome=None,
                **dict.fromkeys(USAGE),safe_error_code=None,recorded_at=datetime.now(timezone.utc).isoformat(),
                qualifying=False,release_allowed=False)
            try:
                received=self.transport(self.profile,system,content,rules)
                _require(type(received) is EvaluatorProviderResponse and type(received.observation) is ModelResultObservation
                    and type(received.text) is str and 0<len(received.text.encode())<=65536,'EVALUATOR_PROVIDER_RESPONSE')
                observed=received.observation
                outcome.update(response_text=received.text,response_sha256=hashlib.sha256(received.text.encode()).hexdigest(),
                    returned_model_id=observed.returned_model_id,finish_reason=observed.finish_reason,safety_outcome=observed.safety_outcome,
                    **{k:getattr(observed,k) for k in USAGE})
                valid=observed.state=='SUCCEEDED' and observed.response_sha256==outcome['response_sha256'] and observed.returned_model_id==self.profile.model_id and observed.finish_reason=='STOP' and observed.safety_outcome=='PASSED'
                valid=valid and all(outcome[k] is None or type(outcome[k]) is int and 0<=outcome[k]<2**63 for k in USAGE)
                outcome.update(state='SUCCEEDED' if valid else 'REJECTED',safe_error_code=None if valid else 'EVALUATOR_PROVIDER_METADATA')
            except Exception:
                outcome.update(state='UNKNOWN',safe_error_code='EVALUATOR_PROVIDER_ERROR_OR_UNKNOWN')
            outcome['completed_at']=datetime.now(timezone.utc).isoformat()
            # Retain exact publication bytes before any uncertain database RPC.
            self._pending[slot]=(reservation,outcome)
            self.store.publish(reservation,outcome)
            self._pending.pop(slot,None)
            self.guard(self.binding);check_operation()
            result=self._result(outcome);self._record_receipt(outcome);return result

    def recover_pending(self,record_id):
        require_owned_worker()
        with self._lock:
            self.guard(self.binding);reservation,outcome=self._pending[record_id]
            self.store.publish(reservation,outcome) # exact original outcome job only
            self.guard(self.binding);self._pending.pop(record_id,None)
            result=self._result(outcome);self._record_receipt(outcome);return result
