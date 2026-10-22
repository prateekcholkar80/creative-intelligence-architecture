"""Protected single-case planner measurement; no serving mutation or qualification.

Deployment-owned case text describes a separate evaluation task. It never closes
or changes the uploaded campaign request or invents its interpreter outcome.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib,json
from threading import RLock
from google.cloud import bigquery
from src.application.campaign_measurement_runtime import ProtectedMeasurementRuntime,AUTHORITY_SQL,verify_authority
from src.application.campaign_document_service import ProtectedDocumentService
from src.application.campaign_evaluation_uploads import ProtectedEvaluationUploadReader
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha
from src.application.campaign_planner_evaluation import DurablePlannerEvaluation,prepare_planner_evaluation
from src.application.campaign_grounded_planner_definition import KEY,SYSTEM,TEMPLATE
from src.application.campaign_quality_product_reader import read_product_fact
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.prompt_contract import PromptLifecycle
from src.application.operation_budget import require_owned_worker,operation_timeout
from src.application.workflow_contract import _require,_id,_digest
from src.generators.campaign_brief_generator import CONCEPTS,LAYOUTS,HYPOTHESES
from src.retrieval.campaign_retrieval_coverage import capture_retrieval_coverage,verify_retrieval_coverage,VERSION as COVERAGE_VERSION


@dataclass(frozen=True,slots=True)
class PlannerEvaluationSettings:
    run_id:str
    case_input_json:str
    case_sha256:str
    binding_id:str
    conversation_id:str
    request_id:str
    submission_ids:tuple
    prompt_id:str
    prompt_version:str
    prompt_snapshot_sha256:str
    enabled:bool=False

    def __post_init__(self):
        for k in ('run_id','conversation_id','request_id','prompt_id','prompt_version'):_id(getattr(self,k))
        for h in (self.case_sha256,self.prompt_snapshot_sha256):_digest(h)
        _require(type(self.binding_id) is str and self.binding_id.startswith('eval_upload_binding_v1_'),
            'PLANNER_RUNTIME_BINDING')
        _digest(self.binding_id.removeprefix('eval_upload_binding_v1_'))
        _require(type(self.submission_ids) is tuple and 0<len(self.submission_ids)<=16
            and len(set(self.submission_ids))==len(self.submission_ids) and type(self.enabled) is bool,'PLANNER_RUNTIME_CONFIG')
        for sid in self.submission_ids:_digest(sid)
        _require(type(self.case_input_json) is str and len(self.case_input_json.encode())<=16384,'PLANNER_RUNTIME_CASE')
        case=json.loads(self.case_input_json)
        _require(canonical(case)==self.case_input_json and sha(case)==self.case_sha256
            and set(case)=={'case_id','stage','sku','country','channel','turns'} and case['stage']=='GENERATION',
            'PLANNER_RUNTIME_CASE')
        _id(case['case_id'])
        _require(type(case['turns']) is list and 0<len(case['turns'])<=16,'PLANNER_RUNTIME_CASE')
        for turn in case['turns']:
            _require(type(turn) is dict and set(turn)=={'role','text'} and turn['role']=='USER'
                and type(turn['text']) is str and 0<len(turn['text'])<=8192,'PLANNER_RUNTIME_CASE')


def identity_options(product_fields):
    """Exact catalog identity plus neutral design choices, never claim approval."""
    return canonical(dict(concept=list(CONCEPTS),layout=list(LAYOUTS),
        headline=[product_fields['product_name']],body=[''],cta=['View product'],
        reference_campaign_id=['NONE_NO_APPLICABLE_REFERENCE'],test_variable=list(HYPOTHESES),
        image_treatment=['PRESERVE_ORIGINAL_REFERENCE_UNVERIFIED_BADGE_REQUIRES_REVIEW'],
        background=['NEUTRAL_OFF_WHITE'],text_color=['DARK_CHARCOAL'],typography=['CLEAR_SANS_SERIF']))


class ProtectedPlannerEvaluation:
    def __init__(self,*,measurement,documents,settings,transport=None):
        require_owned_worker()
        _require(type(measurement) is ProtectedMeasurementRuntime and type(documents) is ProtectedDocumentService
            and type(settings) is PlannerEvaluationSettings
            and documents.store.client is measurement.client
            and documents.intake.verifier is measurement.verifier,'PLANNER_RUNTIME_COMPOSITION')
        self.measurement,self.documents,self.settings=measurement,documents,settings
        self.uploads=ProtectedEvaluationUploadReader(documents,clock=measurement.clock)
        self.registry=BigQueryPromptRegistryReader(measurement.client)
        self._headers=None;self._lock=RLock()
        self._retrieval_pending={}
        self.engine=DurablePlannerEvaluation(store=EvaluatorStore(measurement.client),guard=self._guard,
            transport=transport,enabled=settings.enabled)

    def _capture(self,headers,*,require_session_lifetime=False):
        m,s=self.measurement,self.settings;actor=m._actor(headers)
        started=m.clock()
        if require_session_lifetime:
            _require((datetime.fromtimestamp(actor.expires_at,tz=timezone.utc)-started).total_seconds()
                >=operation_timeout(300),'PLANNER_SESSION_REFRESH_REQUIRED')
        row=m.io.read(AUTHORITY_SQL,[bigquery.ScalarQueryParameter('workspace_id','STRING',m.settings.workspace_id),
            bigquery.ScalarQueryParameter('principal_id','STRING',actor.principal.principal_id)])
        revisions=verify_authority(row,settings=m.settings,identity=actor,started=started,ended=m.clock())
        inputs=self.uploads.capture(headers,workspace_id=m.settings.workspace_id,conversation_id=s.conversation_id,
            request_id=s.request_id,submission_ids=s.submission_ids)
        case=json.loads(s.case_input_json)
        scope=json.loads(inputs.manifest_json)['context']['scope']
        _require(all(scope[k]==case[k] for k in ('sku','country','channel')),
            'PLANNER_RUNTIME_CASE_SOURCE_SCOPE')
        original_job=self.uploads.verify_binding(s.binding_id,inputs)
        _require(json.loads(inputs.manifest_json)['principal_id']==actor.principal.principal_id,'PLANNER_RUNTIME_OWNER')
        registered=self.registry.read_exact(s.prompt_id,s.prompt_version)
        head=registered.state.head;snapshot=head.snapshot
        _require(head.status in (PromptLifecycle.DRAFT,PromptLifecycle.EVALUATED,PromptLifecycle.ACTIVE)
            and snapshot.snapshot_sha256==s.prompt_snapshot_sha256 and snapshot.prompt_key==KEY
            and snapshot.system_instruction==SYSTEM and snapshot.prompt_template==TEMPLATE,'PLANNER_RUNTIME_PROMPT_CHANGED')
        product=read_product_fact(m.client,case['sku'],clock=m.clock)
        fields=json.loads(product.fact.fields_json)
        _require(m.clock()<min(inputs.valid_until,product.fact.valid_until)
            and m._actor(headers).principal==actor.principal,'PLANNER_RUNTIME_SOURCE_EXPIRED')
        grounding=dict(product_fields=fields,sku=case['sku'],source='CURRENT_BIGQUERY_PRODUCT_IDENTITY',
            claim_approval=False,brand_approval=False,market_approval=False,
            retrieval_status='NOT_EXECUTED',artifact_status='NOT_EXECUTED')
        pins=dict(grounding=grounding,registry_digest=head.digest,authority_revisions=list(revisions))
        evidence=dict(product_native_job_id=product.native_job_id,binding_native_job_id=original_job)
        return inputs,snapshot,pins,evidence

    def _guard(self,intent):
        _require(self._headers is not None,'PLANNER_RUNTIME_ACTION_REQUIRED')
        inputs,snapshot,pins,evidence=self._capture(self._headers)
        expected=self._prepare(inputs,snapshot,pins)
        _require(json.loads(expected.intent_json)==intent,'PLANNER_RUNTIME_SOURCE_CHANGED')
        return evidence

    def _prepare(self,inputs,snapshot,pins):
        s=self.settings;case=dict(json.loads(s.case_input_json),case_sha256=s.case_sha256)
        grounding=dict(pins['grounding'],authority_revisions=pins['authority_revisions'])
        return prepare_planner_evaluation(run_id=s.run_id,case_input=case,snapshot=snapshot,
            registry_record_digest=pins['registry_digest'],uploads=inputs,binding_id=s.binding_id,
            options_json=identity_options(pins['grounding']['product_fields']),grounding=grounding)

    def execute(self,headers,*,recover_only=False):
        require_owned_worker()
        _require(self.settings.enabled and type(recover_only) is bool,'PLANNER_EVALUATION_DISABLED')
        with self._lock:
            self._headers=dict(headers)
            try:
                inputs,snapshot,pins,_=self._capture(self._headers,require_session_lifetime=not recover_only)
                result=self.engine.execute(self._prepare(inputs,snapshot,pins),recover_only=recover_only)
                return dict(result,run_id=self.settings.run_id,case_id=json.loads(self.settings.case_input_json)['case_id'])
            finally:self._headers=None

    def measure_retrieval(self,headers,*,recover_only=False):
        """Separate coverage record linked to the original stage; no model call.

        Unknown original reservations never authorize a new slot or a new read.
        Coverage does not change historical stage inputs or qualify the pilot.
        """
        require_owned_worker()
        _require(self.settings.enabled and type(recover_only) is bool,'PLANNER_EVALUATION_DISABLED')
        with self._lock:
            self._headers=dict(headers)
            try:
                inputs,snapshot,pins,_=self._capture(self._headers)
                original=json.loads(self._prepare(inputs,snapshot,pins).intent_json)
                store=self.engine.store;stage=store.recover(original)
                _require(type(stage) is dict and stage.get('state')=='SUCCEEDED'
                    and stage.get('returned_model_id')==original['profile']['model_id']
                    and stage.get('finish_reason')=='STOP' and stage.get('safety_outcome')=='PASSED'
                    and type(stage.get('response_text')) is str
                    and hashlib.sha256(stage['response_text'].encode()).hexdigest()==stage.get('response_sha256'),
                    'RETRIEVAL_ORIGINAL_STAGE_UNAVAILABLE')
                case=json.loads(self.settings.case_input_json)
                intent=dict(version=COVERAGE_VERSION,record_id=sha([original['record_id'],COVERAGE_VERSION]),
                    stage_record_id=original['record_id'],stage_outcome_sha256=sha(stage),
                    scope=dict(sku=case['sku'],country=case['country'],channel=case['channel']),
                    input_manifest_sha256=original['input_manifest_sha256'],qualifying=False,release_allowed=False)
                slot=intent['record_id'];pending=self._retrieval_pending
                if recover_only and slot in pending:
                    self._guard(original)
                    reservation,outcome=pending[slot];store.publish(reservation,outcome);pending.pop(slot)
                outcome=store.recover(intent)
                if outcome is None:
                    _require(not recover_only,'RETRIEVAL_NO_ORIGINAL_MEASUREMENT')
                    reservation=store.reserve(intent)
                    _require(reservation.newly_reserved,'RETRIEVAL_ORIGINAL_UNCERTAIN')
                    self._guard(original)
                    coverage=capture_retrieval_coverage(self.measurement.client,case['sku'],clock=self.measurement.clock)
                    self._guard(original)
                    outcome=dict(version=COVERAGE_VERSION,record_id=slot,
                        intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),
                        state='MEASURED',coverage=coverage,qualifying=False,release_allowed=False)
                    _require(len(pending)<16,'RETRIEVAL_PENDING_CAPACITY')
                    pending[slot]=(reservation,outcome);store.publish(reservation,outcome);pending.pop(slot)
                _require(outcome.get('state')=='MEASURED' and outcome.get('version')==COVERAGE_VERSION,
                    'RETRIEVAL_MEASUREMENT_UNAVAILABLE')
                self._guard(original)
                coverage=outcome['coverage']
                _require(coverage['sku']==case['sku'] and coverage['sha256']==sha(
                    {k:v for k,v in coverage.items() if k!='sha256'}),'RETRIEVAL_MEASUREMENT_BINDING')
                verify_retrieval_coverage(self.measurement.client,coverage)
                self._guard(original)
                return dict(record_id=slot,stage_record_id=original['record_id'],coverage_sha256=coverage['sha256'],
                    corpus_status=coverage['corpus_status'],counts=coverage['counts'],native_job_id=coverage['native_job_id'],
                    qualifying=False,release_allowed=False,run_id=self.settings.run_id,case_id=case['case_id'])
            finally:self._headers=None
