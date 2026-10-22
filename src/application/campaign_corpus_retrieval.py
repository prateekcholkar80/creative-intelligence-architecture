"""Protected fixed-case upload-corpus retrieval calibration; release stays off."""
from dataclasses import dataclass
import hashlib,json,logging,time
from contextlib import contextmanager
from contextvars import ContextVar
from threading import RLock
from src.application.campaign_corpus_embedding import ProtectedCorpusEmbedding
from src.application.campaign_corpus_preparation import BoundCorpusCapture
from src.application.campaign_evaluator_store import canonical,sha
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id,_digest
from src.retrieval.private_query_embedding import query_request
from src.retrieval.private_text_embeddings import DurableTextEmbedding,document_request
from src.retrieval.private_retrieval_rank import BigQueryPrivateRanker
from src.retrieval.private_retrieval_measurement import DurableRetrievalMeasurement

LOGGER=logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
PHASES=frozenset({'source_capture','saved_source_verification','document_index_verification','query_embedding','measurement','measurement_recovery'})
_PROGRESS=ContextVar('private_retrieval_progress',default=None)


@contextmanager
def retrieval_progress(callback):
    _require(callable(callback),'CORPUS_RETRIEVAL_PROGRESS')
    token=_PROGRESS.set(callback)
    try:yield
    finally:_PROGRESS.reset(token)


@contextmanager
def measured_phase(phase):
    _require(phase in PHASES,'CORPUS_RETRIEVAL_PHASE')
    started=time.monotonic();completed=False
    callback=_PROGRESS.get()
    if callback is not None:callback(dict(phase=phase,state='STARTED',elapsed_seconds=0.))
    try:
        yield
        completed=True
    finally:
        LOGGER.info(json.dumps(dict(event='private_retrieval_phase_completed',
            phase=phase,completed=completed,elapsed_seconds=round(time.monotonic()-started,3))))
        if callback is not None:callback(dict(phase=phase,state='SUCCEEDED' if completed else 'STOPPED',elapsed_seconds=round(time.monotonic()-started,3)))


@dataclass(frozen=True,slots=True)
class CorpusRetrievalSettings:
    run_id:str
    query_id:str
    query_text:str
    case_json:str
    case_sha256:str
    index_record_id:str
    index_sha256:str
    enabled:bool=False

    def __post_init__(self):
        _id(self.run_id);_id(self.query_id)
        for value in (self.case_sha256,self.index_record_id,self.index_sha256):_digest(value)
        _require(type(self.enabled) is bool and type(self.query_text) is str and 0<len(self.query_text.encode())<=4096
            and type(self.case_json) is str and len(self.case_json.encode())<=16384,'CORPUS_RETRIEVAL_SETTINGS')
        case=json.loads(self.case_json)
        _require(canonical(case)==self.case_json and sha(case)==self.case_sha256
            and case['version']=='private-retrieval-relevance-case-v1' and case['case_id']==self.query_id
            and case['query_text_sha256']==hashlib.sha256(self.query_text.encode()).hexdigest()
            and case['index_sha256']==self.index_sha256
            and case['label_authority']=='SOURCE_REVIEW_CALIBRATION_NOT_INDEPENDENT_QUALIFICATION','CORPUS_RETRIEVAL_CASE')


class ProtectedCorpusRetrieval:
    def __init__(self,embedding,settings):
        require_owned_worker()
        _require(type(embedding) is ProtectedCorpusEmbedding and type(settings) is CorpusRetrievalSettings
            and json.loads(settings.case_json)['corpus_sha256']==embedding.settings.corpus_sha256,'CORPUS_RETRIEVAL_COMPOSITION')
        self.embedding,self.settings=embedding,settings;self.store=embedding.store
        self._lock=RLock();self._bound=None
        self.engine=DurableTextEmbedding(self.store,embedding.transport,self._guard,enabled=settings.enabled)
        self.ranker=BigQueryPrivateRanker(self.store,self._guard)
        self.measurement=DurableRetrievalMeasurement(self.store,self.ranker,self._guard,enabled=settings.enabled)

    def _index_receipt(self):
        manifest=json.loads(self._bound.capture.manifest_json)
        return dict(index_record_id=self.settings.index_record_id,index_sha256=self.settings.index_sha256,
            corpus_sha256=self.embedding.settings.corpus_sha256,chunk_count=len(manifest['text_chunks']),dimension=768,
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)

    def _guard(self,request):
        check_operation();_require(type(self._bound) is BoundCorpusCapture,'CORPUS_RETRIEVAL_CAPTURE_REQUIRED')
        expected=query_request(self._bound.capture,self.embedding.settings.binding_id,self._index_receipt(),
            self.settings.run_id,self.settings.query_id,self.settings.query_text)
        _require(request==expected,'CORPUS_RETRIEVAL_INPUT_CHANGED')
        return self._bound.refresh() is True

    def execute(self,headers,*,recover_only=False):
        return self._execute(headers,recover_only=recover_only)

    def execute_bound(self,headers,consume,*,recover_only=False):
        """Trusted action-local continuation; not a browser-returned authority."""
        _require(callable(consume),'CORPUS_RETRIEVAL_CONTINUATION')
        return self._execute(headers,recover_only=recover_only,consume=consume)

    def _execute(self,headers,*,recover_only,consume=None):
        require_owned_worker();check_operation()
        _require(type(recover_only) is bool and (self.settings.enabled or recover_only),'CORPUS_RETRIEVAL_DISABLED')
        with self._lock,self.embedding._lock:
            with measured_phase('saved_source_verification' if recover_only else 'source_capture'):
                bound=(self.embedding.corpus.capture_saved_bound_for_receipt(headers,
                    binding_id=self.embedding.settings.binding_id,corpus_sha256=self.embedding.settings.corpus_sha256)
                    if recover_only else self.embedding.corpus.capture_bound(headers))
            _require(type(bound) is BoundCorpusCapture,'CORPUS_RETRIEVAL_CAPTURE_REQUIRED')
            receipt=json.loads(bound.receipt_json);cfg=self.embedding.settings
            _require(receipt['binding_id']==cfg.binding_id and receipt['corpus_sha256']==cfg.corpus_sha256
                and receipt['selection_sha256']==cfg.selection_sha256,'CORPUS_RETRIEVAL_BINDING_CHANGED')
            self._bound=bound;self.embedding._bound=bound
            try:
                manifest=json.loads(bound.capture.manifest_json);chunks=manifest['text_chunks']
                documents=tuple(document_request(bound.capture,cfg.binding_id,cfg.run_id,i) for i in range(len(chunks)))
                # This always recovers the original document index. It cannot
                # reserve or dispatch document embeddings, even on Run.
                index_intent=self.embedding.index._intent(documents,json.loads(documents[0].input_json))
                query=query_request(bound.capture,cfg.binding_id,self._index_receipt(),self.settings.run_id,self.settings.query_id,self.settings.query_text)
                case=json.loads(self.settings.case_json)
                if recover_only:
                    # Measurement recovery independently verifies the complete
                    # query/document/index ledger. Do not repeat that work here.
                    if query.intent['record_id'] in self.engine._pending:self.engine.recover_pending(query)
                else:
                    with measured_phase('document_index_verification'):
                        _require(self.ranker.verify_documents(query,documents,index_intent) is True,'CORPUS_RETRIEVAL_INDEX_CHANGED')
                    self.measurement._check(query,case,chunks)
                    with measured_phase('query_embedding'):self.engine.execute(query)
                with measured_phase('measurement_recovery' if recover_only else 'measurement'):
                    result=self.measurement.execute(query,documents,index_intent,case,chunks,recover_only=recover_only)
                return result if consume is None else consume(bound,result)
            finally:self._bound=None;self.embedding._bound=None
