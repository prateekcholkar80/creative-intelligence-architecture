"""Protected fixed-corpus text embedding/index operation; disabled by default.

Does not enable generation, image embedding, retrieval scoring or serving release.
"""
from dataclasses import dataclass
from threading import RLock
import json
from src.application.campaign_corpus_preparation import ProtectedCorpusPreparation,BoundCorpusCapture
from src.application.campaign_evaluator_store import EvaluatorStore
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id,_digest
from src.retrieval.private_text_embeddings import DurableTextEmbedding,document_request,TextEmbeddingProfile
from src.retrieval.private_text_index import DurableTextIndex
from src.retrieval.private_embedding_transport import PinnedTextEmbeddingTransport


@dataclass(frozen=True,slots=True)
class CorpusEmbeddingSettings:
    run_id:str
    binding_id:str
    corpus_sha256:str
    selection_sha256:str
    enabled:bool=False

    def __post_init__(self):
        _id(self.run_id);_digest(self.corpus_sha256);_digest(self.selection_sha256)
        prefix='eval_corpus_binding_v1_'
        _require(type(self.binding_id) is str and self.binding_id.startswith(prefix)
            and type(self.enabled) is bool,'CORPUS_EMBEDDING_SETTINGS')
        _digest(self.binding_id[len(prefix):])


class ProtectedCorpusEmbedding:
    def __init__(self,corpus,store,settings,*,transport=None):
        require_owned_worker()
        _require(type(corpus) is ProtectedCorpusPreparation and type(store) is EvaluatorStore
            and type(settings) is CorpusEmbeddingSettings
            and corpus.documents.store.client is store.client
            and settings.selection_sha256==corpus.settings.selection_sha256,
            'CORPUS_EMBEDDING_COMPOSITION')
        self.corpus,self.store,self.settings=corpus,store,settings
        self.transport=transport if transport is not None else PinnedTextEmbeddingTransport()
        self._lock=RLock();self._bound=None
        self.engine=DurableTextEmbedding(store,self.transport,self._guard,enabled=settings.enabled,guard_all=self._guard_all)
        self.index=DurableTextIndex(store,self._guard,enabled=settings.enabled,guard_all=self._guard_all)

    def _guard(self,request):
        return self._guard_all((request,))

    def _guard_all(self,requests):
        _require(type(self._bound) is BoundCorpusCapture,'CORPUS_EMBEDDING_CAPTURE_REQUIRED')
        _require(type(requests) is tuple and bool(requests),'CORPUS_EMBEDDING_INPUT_CHANGED')
        for request in requests:
            value=json.loads(request.input_json);receipt=json.loads(self._bound.receipt_json)
            _require(value['corpus_binding_id']==self.settings.binding_id==receipt['binding_id']
                and value['corpus_sha256']==self.settings.corpus_sha256==self._bound.capture.sha256
                and request.run_id==self.settings.run_id,'CORPUS_EMBEDDING_SOURCE_CHANGED')
            expected=document_request(self._bound.capture,self.settings.binding_id,self.settings.run_id,
                request.ordinal,TextEmbeddingProfile())
            _require(expected==request,'CORPUS_EMBEDDING_INPUT_CHANGED')
        return self._bound.refresh() is True

    def execute(self,headers,*,recover_only=False):
        require_owned_worker();check_operation()
        _require(type(recover_only) is bool and (self.settings.enabled or recover_only),
            'CORPUS_EMBEDDING_DISABLED')
        with self._lock:
            bound=self.corpus.capture_bound(headers)
            _require(type(bound) is BoundCorpusCapture,'CORPUS_EMBEDDING_CAPTURE_REQUIRED')
            receipt=json.loads(bound.receipt_json);settings=self.settings
            _require(receipt['binding_id']==settings.binding_id and receipt['corpus_sha256']==settings.corpus_sha256
                and receipt['selection_sha256']==settings.selection_sha256,'CORPUS_EMBEDDING_BINDING_CHANGED')
            self._bound=bound
            try:
                count=len(json.loads(bound.capture.manifest_json)['text_chunks'])
                requests=tuple(document_request(bound.capture,settings.binding_id,settings.run_id,i) for i in range(count))
                if recover_only:
                    for request in requests:
                        if request.intent['record_id'] in self.engine._pending:self.engine.recover_pending(request)
                existing=self.engine.recover_many(requests)
                receipts=[]
                for request,previous in zip(requests,existing):
                    # Resume only unreserved chunks; uncertainty in any original
                    # slot stops execution and can never grant another dispatch.
                    receipts.append(previous if previous is not None else self.engine.execute(request,recover_only=recover_only))
                index_intent=self.index._intent(requests,json.loads(requests[0].input_json))
                if recover_only and index_intent['record_id'] in self.index._pending:
                    index=self.index.recover_pending(requests)
                else:index=self.index.build(requests,recover_only=recover_only)
                return dict(run_id=settings.run_id,corpus_binding_id=settings.binding_id,
                    corpus_sha256=settings.corpus_sha256,text_embeddings=receipts,index=index,
                    image_embedding_status='NOT_EXECUTED',retrieval_quality='NOT_MEASURED',
                    claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
            finally:self._bound=None
