"""One protected benchmark interpretation, isolated from serving admission/release."""
from dataclasses import dataclass,asdict,replace
from datetime import datetime,timezone,timedelta
import hashlib,json
from threading import RLock
from google.cloud import bigquery
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha
from src.application.campaign_evaluator_observer import DurableEvaluatorObserver,EvaluatorProfile,StageEvaluationBinding,USAGE
from src.application.campaign_evaluator_transport import PinnedVertexEvaluatorTransport,VERSION as TRANSPORT_VERSION,STRICT_VERSION as STRICT_TRANSPORT_VERSION,APPLICABILITY_VERSION as APPLICABILITY_TRANSPORT_VERSION
from src.application.campaign_full_quality_checks import FullQualityEvaluator
from src.application.campaign_quality_checks import QualityCandidate,check_candidate,SourceFact
from src.application.campaign_acceptance_contract import Stage
from src.application.campaign_measurement_runtime import AUTHORITY_SQL,verify_authority,ProtectedMeasurementRuntime
from src.application.campaign_clarification_policy import ClarificationPolicyRequest,read_policy
from src.application.campaign_clarification_prompts import definitions
from src.application.campaign_quality_product_reader import read_product_fact
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.prompt_runtime_contract import render_prompt
from src.application.prompt_contract import PromptLifecycle
from src.application.campaign_planner_projection import parse_json
from src.application.campaign_evaluation_source_applicability import interpretation_source_applicability
from src.application.campaign_stage_applicability import INTENT_CONTRACT
from src.application.campaign_semantic_checks import SYSTEM,VERSION as LEGACY_CONTRACT,STRICT_VERSION as STRICT_CONTRACT,APPLICABILITY_VERSION as APPLICABILITY_CONTRACT
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id,_digest
from src.validation.campaign_semantic_benchmark import load_cases


@dataclass(frozen=True,slots=True)
class InterpretationEvaluationSettings:
    run_id:str
    case_id:str
    case_sha256:str
    prompt_id:str
    prompt_version:str
    prompt_snapshot_sha256:str
    policy_id:str
    policy_version:str
    policy_sha256:str
    evaluator_sha256:str
    enabled:bool=False
    judge_contract_version:str=LEGACY_CONTRACT
    recovery_only:bool=False

    def __post_init__(self):
        for name in ('run_id','case_id','prompt_id','prompt_version','policy_id','policy_version'):_id(getattr(self,name))
        for name in ('case_sha256','prompt_snapshot_sha256','policy_sha256','evaluator_sha256'):_digest(getattr(self,name))
        _require(type(self.enabled) is bool and type(self.recovery_only) is bool
            and self.judge_contract_version in (LEGACY_CONTRACT,STRICT_CONTRACT,APPLICABILITY_CONTRACT),'INTERPRETATION_EVALUATION_CONFIG')


def render_evaluation_input(snapshot,input_value):
    system,content=render_prompt(snapshot,dict(evidence_intent_context_json=canonical(input_value)))
    _require(system==snapshot.system_instruction,'EVALUATION_SYSTEM_TEMPLATE_CHANGED')
    return content


class ProtectedInterpretationEvaluation:
    def __init__(self,measurement,settings):
        require_owned_worker()
        _require(type(measurement) is ProtectedMeasurementRuntime and type(settings) is InterpretationEvaluationSettings and settings.evaluator_sha256==measurement.settings.evaluator_sha256,
            'INTERPRETATION_EVALUATION_PINS')
        self.measurement,self.settings=measurement,settings;self.client=measurement.client
        self.store=EvaluatorStore(self.client);self.transport=PinnedVertexEvaluatorTransport()
        self._lock=RLock();self._pending={}

    def _capture(self,headers):
        m=self.measurement;s=self.settings;actor=m._actor(headers)
        started=m.clock();row=m.io.read(AUTHORITY_SQL,[bigquery.ScalarQueryParameter('workspace_id','STRING',m.settings.workspace_id),bigquery.ScalarQueryParameter('principal_id','STRING',actor.principal.principal_id)])
        revisions=verify_authority(row,settings=m.settings,identity=actor,started=started,ended=m.clock())
        catalog,cases=load_cases();catalog=replace(catalog,contract_version=s.judge_contract_version);selected=[c for c in cases if c['case_id']==s.case_id]
        _require(len(selected)==1 and selected[0]['case_sha256']==s.case_sha256 and catalog.review_sha256==m.settings.review_sha256,'INTERPRETATION_EVALUATION_CASE_CHANGED')
        case=selected[0]['case'];selection=json.loads(case.selection_json)
        _require(case.stage=='INTERPRETATION' and case.candidate is None and selection.get('canonical_sku') in ('CI-SKU-0021','CI-SKU-0022','CI-SKU-0023')
            and case.turns_json!='[]','INTERPRETATION_EVALUATION_CASE_SCOPE')
        registered=BigQueryPromptRegistryReader(self.client).read_exact(s.prompt_id,s.prompt_version)
        _require(registered.state.head.status in (PromptLifecycle.DRAFT,PromptLifecycle.EVALUATED,PromptLifecycle.ACTIVE)
            and registered.state.head.snapshot.snapshot_sha256==s.prompt_snapshot_sha256,'INTERPRETATION_EVALUATION_PROMPT_CHANGED')
        snapshot=registered.state.head.snapshot
        policy=read_policy(self.client,ClarificationPolicyRequest(s.policy_id,s.policy_version,s.policy_sha256),clock=m.clock)
        definition=definitions(policy,checked_at=m.clock())[0];definition.verify(snapshot)
        product=read_product_fact(self.client,selection['canonical_sku'],clock=m.clock)
        # Fixture-only benchmark inputs never become a production campaign request.
        scope=dict(workspace_id=m.settings.workspace_id,principal_id=actor.principal.principal_id,
            sku=selection['canonical_sku'],country=case.country,channel=case.channel,audience='NEW',formula_version=None)
        input_value=dict(user_text='\n'.join(t['text'] for t in json.loads(case.turns_json) if t['role']=='USER'),
            selection=scope,requirements=json.loads(policy.policy_canonical_json)['requirements'],policy_sha256=s.policy_sha256)
        rendered=render_evaluation_input(snapshot,input_value)
        self._source_jobs=dict(product_native_job_id=product.native_job_id,policy_native_job_id=policy.native_job_id,authority_row_sha256=sha(dict(row,observed_at=row['observed_at'].isoformat())))
        source=dict(scope=scope,case_sha256=s.case_sha256,review_sha256=catalog.review_sha256,
            prompt_snapshot_sha256=snapshot.snapshot_sha256,prompt_record_digest=registered.state.head.digest,
            policy_record_sha256=policy.record_sha256,product_fields_json=product.fact.fields_json,
            authority_revisions=list(revisions),rendered_sha256=hashlib.sha256(rendered.encode()).hexdigest())
        if s.judge_contract_version==APPLICABILITY_CONTRACT:
            self._source_product,self._source_policy=product,policy
            source['evaluation_applicability_contract']='campaign-evaluation-source-applicability-v1'
        _require(m._actor(headers).principal==actor.principal,'INTERPRETATION_EVALUATION_ACTOR_CHANGED')
        return source,case,catalog,snapshot,rendered

    def _profile(self,snapshot,system):
        return EvaluatorProfile(snapshot.model_id,canonical(dict(version=APPLICABILITY_TRANSPORT_VERSION if self.settings.judge_contract_version==APPLICABILITY_CONTRACT else STRICT_TRANSPORT_VERSION if self.settings.judge_contract_version==STRICT_CONTRACT else TRANSPORT_VERSION,source_snapshot_sha256=snapshot.snapshot_sha256,
            generation_config=json.loads(snapshot.generation_config_json),safety_settings=json.loads(snapshot.safety_settings_json))),hashlib.sha256(system.encode()).hexdigest())

    def _guard(self,headers,source):
        _require(self._capture(headers)[0]==source,'INTERPRETATION_EVALUATION_SOURCE_CHANGED')

    def _stage(self,headers,source,snapshot,rendered,*,recover_only):
        s=self.settings;slot=sha([s.run_id,s.case_id,source['scope']['principal_id'],'INTERPRETATION_STAGE'])
        profile=self._profile(snapshot,snapshot.system_instruction)
        intent=dict(version='benchmark-interpretation-stage-v1',record_id=slot,run_id=s.run_id,case_id=s.case_id,
            source=source,profile=asdict(profile),system=snapshot.system_instruction,content=rendered,
            output_schema_json=snapshot.output_schema_json,execution_lane='EVALUATION_ONLY_NOT_SERVING',release_allowed=False)
        if recover_only and slot in self._pending:
            self._guard(headers,source)
            reservation,retained=self._pending[slot]
            self.store.publish(reservation,retained)
            self._pending.pop(slot,None)
        existing=self.store.recover(intent)
        if existing is not None:return slot,existing
        _require(not recover_only,'INTERPRETATION_EVALUATION_NO_ORIGINAL')
        reservation=self.store.reserve(intent)
        _require(reservation.newly_reserved,'INTERPRETATION_EVALUATION_ORIGINAL_UNCERTAIN')
        self._guard(headers,source);check_operation()
        outcome=dict(version='benchmark-interpretation-outcome-v1',record_id=slot,
            intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),state='UNKNOWN',
            response_text=None,response_sha256=None,returned_model_id=None,finish_reason=None,safety_outcome=None,
            **dict.fromkeys(USAGE),source_native_evidence=dict(self._source_jobs),release_allowed=False,qualifying=False)
        try:
            response=self.transport.interpretation(profile,snapshot.system_instruction,rendered,json.loads(snapshot.output_schema_json))
            observation=response.observation
            outcome.update(state='SUCCEEDED' if observation.state=='SUCCEEDED' and observation.response_sha256==hashlib.sha256(response.text.encode()).hexdigest() and observation.returned_model_id==snapshot.model_id and observation.finish_reason=='STOP' and observation.safety_outcome=='PASSED' else 'REJECTED',response_text=response.text,response_sha256=observation.response_sha256,
                returned_model_id=observation.returned_model_id,finish_reason=observation.finish_reason,safety_outcome=observation.safety_outcome,
                **{k:getattr(observation,k) for k in USAGE})
        except Exception:outcome['safe_error_code']='STAGE_PROVIDER_ERROR_OR_UNKNOWN'
        if self.settings.judge_contract_version==APPLICABILITY_CONTRACT and outcome['state']=='SUCCEEDED':
            try:
                self._guard(headers,source)
                outcome['source_applicability']=interpretation_source_applicability(response_text=outcome['response_text'],
                    scope=source['scope'],product=self._source_product,policy=self._source_policy,checked_at=self.measurement.clock())
                outcome['source_applicability_state']='CAPTURED'
                outcome['after_response_source_native_evidence']=dict(self._source_jobs)
            except Exception:
                # Preserve the actual provider result, but do not judge/qualify
                # it when fresh applicability/access capture was not obtained.
                outcome['source_applicability_state']='FAILED_OR_UNAVAILABLE'
        outcome['completed_at']=datetime.now(timezone.utc).isoformat();self._pending[slot]=(reservation,outcome)
        self.store.publish(reservation,outcome);self._pending.pop(slot,None)
        self._guard(headers,source)
        return slot,outcome

    def execute(self,headers,*,recover_only=False):
        require_owned_worker();_require(self.settings.enabled,'INTERPRETATION_EVALUATION_DISABLED')
        _require(recover_only or not self.settings.recovery_only,'INTERPRETATION_EVALUATION_RECOVERY_ONLY')
        with self._lock:
            source,case,catalog,snapshot,rendered=self._capture(headers);self.store.preflight()
            slot,outcome=self._stage(headers,source,snapshot,rendered,recover_only=recover_only)
            _require(outcome['state']=='SUCCEEDED' and outcome['returned_model_id']==snapshot.model_id
                and outcome['finish_reason']=='STOP' and outcome['safety_outcome']=='PASSED'
                and type(outcome['response_text']) is str and hashlib.sha256(outcome['response_text'].encode()).hexdigest()==outcome['response_sha256'],
                'INTERPRETATION_EVALUATION_STAGE_UNAVAILABLE')
            value=parse_json(outcome['response_text']);schema=json.loads(snapshot.output_schema_json)
            _require(set(value)==set(schema['required']) and value['schema_version']==snapshot.output_schema_version
                and value['audience']=='NEW' and value['formula_version'] is None
                and type(value['features']) is list and len(value['features'])==len(set(value['features']))
                and set(value['features'])<=set(schema['properties']['features']['items']['enum']),'INTERPRETATION_EVALUATION_SCHEMA')
            scope=source['scope'];candidate=QualityCandidate('eval-request-'+slot,Stage.INTERPRETATION,scope['sku'],scope['country'],scope['channel'],outcome['response_text'],())
            probe=FullQualityEvaluator(catalog,catalog.sha256,lambda *a:None)
            _require(probe.evaluator_sha256==self.settings.evaluator_sha256,'INTERPRETATION_EVALUATOR_CHANGED')
            def guard(binding):self._guard(headers,source)
            observer=DurableEvaluatorObserver(store=self.store,run_id=self.settings.run_id,evaluator_sha256=probe.evaluator_sha256,
                profile=self._profile(snapshot,catalog.system),transport=self.transport,guard=guard,enabled=True,recover_only=recover_only)
            binding=StageEvaluationBinding(scope['workspace_id'],'eval-conversation-'+sha(self.settings.run_id),candidate.request_id,scope['principal_id'],
                'INTERPRETATION','eval-stage-'+slot,outcome['response_sha256'],sha(dict(source=source,profile=asdict(self._profile(snapshot,snapshot.system_instruction)))),sha(source),candidate.sha256)
            evaluator=FullQualityEvaluator(catalog,catalog.sha256,observer).for_stage(binding)
            now=self.measurement.clock();fact=SourceFact('evaluation-product-'+scope['sku'],scope['sku'],source['product_fields_json'],now,now+timedelta(seconds=60),True)
            base=check_candidate(candidate,(),checked_at=now,not_before=now)
            scoped={}
            if self.settings.judge_contract_version==APPLICABILITY_CONTRACT:
                _require(outcome.get('source_applicability_state')=='CAPTURED' and type(outcome.get('source_applicability')) is dict,
                    'INTERPRETATION_EVALUATION_SOURCE_APPLICABILITY_UNAVAILABLE')
                scoped=dict(output_contract=INTENT_CONTRACT,source_applicability=outcome['source_applicability'])
            result=evaluator.assess(candidate,base,**scoped,turns=json.loads(case.turns_json),
                evidence=[dict(evidence_id=fact.source_id,kinds=['PRODUCT_FACT'],statement=fact.fields_json,provenance='CURRENT_PRODUCT_IDENTITY_NOT_CLAIM_APPROVAL')],selection=scope)
            self._guard(headers,source)
            assessment_id=sha([self.settings.run_id,self.settings.case_id,scope['principal_id'],'ASSESSMENT'])
            assessment_intent=dict(version='evaluation-assessment-intent-v1',record_id=assessment_id,
                source=source,stage_record_id=slot,stage_response_sha256=outcome['response_sha256'],
                evaluator_sha256=self.settings.evaluator_sha256,evaluator_receipts=result['durable_evaluator_receipts'],release_allowed=False)
            if recover_only and assessment_id in self._pending:
                retained_reservation,retained_outcome=self._pending[assessment_id]
                self.store.publish(retained_reservation,retained_outcome);self._pending.pop(assessment_id,None)
            stored=self.store.recover(assessment_intent)
            if stored is None:
                _require(not recover_only,'INTERPRETATION_ASSESSMENT_ORIGINAL_MISSING')
                reservation=self.store.reserve(assessment_intent)
                _require(reservation.newly_reserved,'INTERPRETATION_ASSESSMENT_ORIGINAL_UNCERTAIN')
                stored=dict(version='evaluation-assessment-outcome-v1',record_id=assessment_id,
                    intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),state='ASSESSMENT_RECORDED',
                    assessment=result,source_native_evidence=outcome['source_native_evidence'],qualifying=False,release_allowed=False)
                self._pending[assessment_id]=(reservation,stored);self.store.publish(reservation,stored);self._pending.pop(assessment_id,None)
            _require(stored['state']=='ASSESSMENT_RECORDED' and stored['assessment']['candidate_sha256']==candidate.sha256
                and stored['assessment']['evaluator_sha256']==self.settings.evaluator_sha256,'INTERPRETATION_ASSESSMENT_UNAVAILABLE')
            result=stored['assessment'];self._guard(headers,source)
            return dict(assessment_record_id=assessment_id,case_id=self.settings.case_id,run_id=self.settings.run_id,stage_record_id=slot,
                stage_response_sha256=outcome['response_sha256'],stage_returned_model_id=outcome['returned_model_id'],
                stage_usage={k:outcome[k] for k in USAGE},evaluator_receipts=result['durable_evaluator_receipts'],
                check_count=len(result['checks']),observation_sha256=result['observation_sha256'],
                semantic_decisions=result['semantic_requested_content_decisions'],qualifying=False,release_allowed=False)
