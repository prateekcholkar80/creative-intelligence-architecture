"""One protected private operation joining saved retrieval, reranking and planning.

No extra marketing stage controls and no serving activation. Host installation,
native execution and artifact rendering remain separate gates.
"""
from dataclasses import dataclass
from threading import RLock
import json

from src.application.campaign_corpus_preparation import BoundCorpusCapture
from src.application.campaign_corpus_retrieval import ProtectedCorpusRetrieval
from src.application.campaign_planner_runtime import ProtectedPlannerEvaluation,identity_options
from src.application.campaign_planner_evaluation import DurablePlannerEvaluation
from src.application.campaign_retrieved_planner import prepare_retrieved_planner_evaluation
from src.application.campaign_evaluator_store import canonical
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id
from src.retrieval.private_semantic_rerank import DurableSemanticRerank,prepare_rerank
from src.retrieval.private_rerank_transport import PinnedRerankTransport


@dataclass(frozen=True,slots=True)
class RetrievedPlannerSettings:
    run_id:str
    rerank_run_id:str
    enabled:bool=False

    def __post_init__(self):
        _id(self.run_id);_id(self.rerank_run_id)
        _require(type(self.enabled) is bool,'RETRIEVED_PLANNER_SETTINGS')


class ProtectedRetrievedPlanner:
    def __init__(self,planner,retrieval,settings,*,rerank_transport=None,planner_transport=None):
        require_owned_worker()
        _require(type(planner) is ProtectedPlannerEvaluation and type(retrieval) is ProtectedCorpusRetrieval
            and type(settings) is RetrievedPlannerSettings
            and planner.documents is retrieval.embedding.corpus.documents
            and planner.measurement.client is retrieval.store.client
            and settings.run_id!=planner.settings.run_id,'RETRIEVED_PLANNER_COMPOSITION')
        self.planner,self.retrieval,self.settings=planner,retrieval,settings
        self._lock=RLock();self._headers=None;self._bound=None;self._parent=None
        self.reranker=DurableSemanticRerank(retrieval.store,rerank_transport or PinnedRerankTransport(),
            self._guard_rerank,enabled=settings.enabled)
        self.engine=DurablePlannerEvaluation(store=retrieval.store,guard=self._guard_planner,
            transport=planner_transport,enabled=settings.enabled)

    def _request(self):
        _require(type(self._bound) is BoundCorpusCapture and self._parent is not None,'RETRIEVED_PLANNER_CONTEXT')
        r=self.retrieval
        return prepare_rerank(self._bound.capture,r.embedding.settings.binding_id,self._parent,
            r.settings.query_text,json.loads(r.settings.case_json),self.settings.rerank_run_id,r.settings.query_id)

    def _guard_rerank(self,request):
        require_owned_worker();check_operation()
        _require(self._headers is not None and request==self._request(),'RETRIEVED_PLANNER_INPUT_CHANGED')
        return self._bound.refresh() is True

    def _prepare(self):
        _require(self._headers is not None,'RETRIEVED_PLANNER_ACTION_REQUIRED')
        p=self.planner;inputs,snapshot,pins,evidence=p._capture(self._headers)
        case=dict(json.loads(p.settings.case_input_json),case_sha256=p.settings.case_sha256)
        grounding=dict(pins['grounding'],authority_revisions=pins['authority_revisions'])
        # The parent helper reports its legacy retrieval status; this new input
        # uses only the separately verified private measurement and rerank.
        grounding['retrieval_status']='MEASURED_PRIVATE_CALIBRATION'
        prepared=prepare_retrieved_planner_evaluation(corpus=self._bound.capture,
            rerank_request=self._request(),rerank_case=json.loads(self.retrieval.settings.case_json),
            reranker=self.reranker,checked_at=p.measurement.clock(),run_id=self.settings.run_id,
            case_input=case,snapshot=snapshot,registry_record_digest=pins['registry_digest'],uploads=inputs,
            binding_id=p.settings.binding_id,options_json=identity_options(pins['grounding']['product_fields']),grounding=grounding)
        return prepared,evidence

    def _guard_planner(self,intent):
        require_owned_worker();check_operation()
        prepared,evidence=self._prepare()
        _require(json.loads(prepared.intent_json)==intent,'RETRIEVED_PLANNER_SOURCE_CHANGED')
        return dict(evidence,retrieval_measurement_record_id=self._parent['measurement_record_id'],
            retrieval_outcome_sha256=self._parent['outcome_sha256'],
            rerank_record_id=intent['retrieval_evidence']['rerank_record_id'],
            rerank_outcome_sha256=intent['retrieval_evidence']['rerank_outcome_sha256'])

    def execute(self,headers,*,recover_only=False):
        return self._execute(headers,recover_only=recover_only)

    def execute_bound(self,headers,consume,*,recover_only=False):
        """Trusted worker continuation while exact source context is still held."""
        _require(callable(consume),'RETRIEVED_PLANNER_CONTINUATION')
        return self._execute(headers,recover_only=recover_only,consume=consume)

    def _execute(self,headers,*,recover_only,consume=None):
        require_owned_worker();check_operation()
        _require(type(recover_only) is bool and self.settings.enabled,'RETRIEVED_PLANNER_DISABLED')
        with self._lock:
            try:
                self._headers=headers
                # Always reuse the original query/vector/ranking/score records.
                self._parent=self.retrieval.execute(headers,recover_only=True)
                self._bound=self.retrieval.embedding.corpus.capture_bound(headers)
                _require(type(self._bound) is BoundCorpusCapture,'RETRIEVED_PLANNER_CONTEXT')
                binding=json.loads(self._bound.receipt_json);cfg=self.retrieval.embedding.settings
                _require(binding['binding_id']==cfg.binding_id and binding['corpus_sha256']==cfg.corpus_sha256
                    and binding['selection_sha256']==cfg.selection_sha256,'RETRIEVED_PLANNER_CORPUS_CHANGED')
                # Resolve current registry and image scope before any new model call.
                inputs,_,_,_=self.planner._capture(headers)
                manifest=json.loads(self._bound.capture.manifest_json)
                _require(manifest['image_manifest_sha256']==inputs.sha256
                    and manifest['image_binding_id']==self.planner.settings.binding_id,
                    'RETRIEVED_PLANNER_IMAGE_CHANGED')
                rerank=self.reranker.execute(self._request(),json.loads(self.retrieval.settings.case_json),recover_only=recover_only)
                prepared,_=self._prepare()
                result=self.engine.execute(prepared,recover_only=recover_only)
                receipt=dict(run_id=self.settings.run_id,planner=result,rerank=rerank,
                    retrieval_measurement_record_id=self._parent['measurement_record_id'],
                    artifact_status='NOT_EXECUTED',claim_approval=False,generation_authorized=False,
                    qualifying=False,release_allowed=False)
                return receipt if consume is None else consume(prepared,receipt)
            finally:
                self._headers=None;self._bound=None;self._parent=None
