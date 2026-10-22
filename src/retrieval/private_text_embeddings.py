"""One-attempt private text embeddings in the existing 30-day BigQuery ledger.

Not installed in serving. The trusted caller must freshly reconstruct the bound
corpus and check IAP/source authority at every guard. No local cache or retry.
"""
from dataclasses import dataclass,asdict
from datetime import datetime,timezone
import hashlib,json,math
from threading import RLock
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id,_digest
from src.retrieval.upload_corpus import UploadCorpusCapture,VERSION as CORPUS_VERSION
from src.retrieval.private_query_embedding import QueryEmbeddingRequest

VERSION='private-text-embedding-v1'


@dataclass(frozen=True,slots=True)
class TextEmbeddingProfile:
    model_id:str='gemini-embedding-001'
    dimension:int=768
    location:str='asia-south1'
    task_type:str='RETRIEVAL_DOCUMENT'
    auto_truncate:bool=False

    def __post_init__(self):
        _require(self.model_id=='gemini-embedding-001' and type(self.dimension) is int
            and self.dimension==768 and self.location=='asia-south1'
            and self.task_type in ('RETRIEVAL_DOCUMENT','RETRIEVAL_QUERY')
            and self.auto_truncate is False,'PRIVATE_EMBEDDING_PROFILE')


@dataclass(frozen=True,slots=True)
class TextEmbeddingRequest:
    run_id:str
    ordinal:int
    input_json:str
    profile:TextEmbeddingProfile

    def __post_init__(self):
        _id(self.run_id)
        _require(type(self.ordinal) is int and 0<=self.ordinal<24
            and type(self.profile) is TextEmbeddingProfile,'PRIVATE_EMBEDDING_REQUEST')
        value=json.loads(self.input_json)
        _require(canonical(value)==self.input_json and set(value)=={'version','corpus_sha256',
            'corpus_binding_id','corpus_chunk_count','scope','principal_id','chunk_id','provenance','text'}
            and value['version']==VERSION,'PRIVATE_EMBEDDING_INPUT')
        _digest(value['corpus_sha256']);_digest(value['chunk_id'])
        _require(type(value['corpus_chunk_count']) is int and 1<=value['corpus_chunk_count']<=24
            and self.ordinal<value['corpus_chunk_count'],'PRIVATE_EMBEDDING_CHUNK_COUNT')
        bid=value['corpus_binding_id'];prefix='eval_corpus_binding_v1_'
        _require(type(bid) is str and bid.startswith(prefix),'PRIVATE_EMBEDDING_BINDING');_digest(bid[len(prefix):])
        _require(type(value['scope']) is dict and type(value['provenance']) is dict
            and type(value['text']) is str and 0<len(value['text'].encode())<=4096
            and self.profile.task_type=='RETRIEVAL_DOCUMENT','PRIVATE_EMBEDDING_TEXT')
        for key in ('workspace_id','conversation_id','request_id','sku','country','channel'):_id(value['scope'][key])
        _id(value['principal_id'])
        _require(value['chunk_id']==sha(value['provenance'])
            and hashlib.sha256(value['text'].encode()).hexdigest()==value['provenance']['text_sha256'],
            'PRIVATE_EMBEDDING_PROVENANCE')

    @property
    def intent(self):
        value=json.loads(self.input_json)
        # Slot deliberately excludes changing source/config bytes. A competing
        # corpus or model profile for the same ordinal conflicts, never remints.
        slot=sha([VERSION,self.run_id,value['scope']['workspace_id'],
            value['scope']['conversation_id'],value['scope']['request_id'],self.ordinal])
        return dict(version=VERSION,record_id=slot,run_id=self.run_id,ordinal=self.ordinal,
            input=value,input_sha256=sha(value),profile=asdict(self.profile),
            claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)


def document_request(capture,binding_id,run_id,ordinal,profile=None):
    _require(type(capture) is UploadCorpusCapture,'PRIVATE_EMBEDDING_CAPTURE')
    value=json.loads(capture.manifest_json)
    _require(canonical(value)==capture.manifest_json and value['version']==CORPUS_VERSION
        and value['use']=='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY'
        and all(value[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed'))
        and type(ordinal) is int and 0<=ordinal<len(value['text_chunks'])<=24,'PRIVATE_EMBEDDING_CAPTURE')
    chunk=value['text_chunks'][ordinal]
    _require(chunk['claim_approval'] is False and chunk['eligible_for_generation'] is False
        and chunk['trust']=='UNTRUSTED_USER_EVIDENCE','PRIVATE_EMBEDDING_CHUNK')
    payload=dict(version=VERSION,corpus_sha256=capture.sha256,corpus_binding_id=binding_id,
        corpus_chunk_count=len(value['text_chunks']),
        scope=value['scope'],principal_id=value['principal_id'],
        **{k:chunk[k] for k in ('chunk_id','provenance','text')})
    return TextEmbeddingRequest(run_id,ordinal,canonical(payload),profile or TextEmbeddingProfile())


@dataclass(frozen=True,slots=True)
class TextEmbeddingResponse:
    vector:tuple
    truncated:bool|None
    input_tokens:int|None=None
    returned_model_id:str|None=None


def validate_response(value,profile):
    _require(type(value) is TextEmbeddingResponse and type(value.vector) is tuple
        and len(value.vector)==profile.dimension and all(type(x) in (int,float)
        and math.isfinite(x) for x in value.vector) and any(x!=0 for x in value.vector)
        and value.truncated is False,'PRIVATE_EMBEDDING_RESPONSE')
    _require(value.input_tokens is None or type(value.input_tokens) is int
        and 0<=value.input_tokens<2**63,'PRIVATE_EMBEDDING_USAGE')
    # The embedding API need not return a model version. Unknown stays unknown.
    _require(value.returned_model_id is None or value.returned_model_id==profile.model_id,
        'PRIVATE_EMBEDDING_MODEL')
    return dict(vector=list(value.vector),truncated=value.truncated,input_tokens=value.input_tokens,
        returned_model_id=value.returned_model_id)


class DurableTextEmbedding:
    def __init__(self,store,transport,guard,*,enabled=False,guard_all=None):
        require_owned_worker()
        _require(type(store) is EvaluatorStore and callable(transport) and callable(guard)
            and type(enabled) is bool and (guard_all is None or callable(guard_all)),
            'PRIVATE_EMBEDDING_COMPOSITION')
        self.store,self.transport,self.guard,self.enabled=store,transport,guard,enabled
        self.guard_all=guard_all
        self._lock=RLock();self._used=set();self._pending={}

    def _guard(self,request):
        check_operation();_require(self.guard(request) is True,'PRIVATE_EMBEDDING_SOURCE_UNVERIFIED')

    def recover_many(self,requests):
        """Read-only recovery as one interaction; missing is never dispatch permission."""
        require_owned_worker();check_operation()
        _require(type(requests) is tuple and 0<len(requests)<=24
            and all(type(r) in (TextEmbeddingRequest,QueryEmbeddingRequest) for r in requests)
            and len({r.intent['record_id'] for r in requests})==len(requests),'PRIVATE_EMBEDDING_REQUEST')
        def guard():
            if self.guard_all is None:
                for r in requests:self._guard(r)
            else:_require(self.guard_all(requests) is True,'PRIVATE_EMBEDDING_SOURCE_UNVERIFIED')
        with self._lock:
            guard();results=[]
            for request in requests:
                check_operation();outcome=self.store.recover(request.intent)
                results.append(None if outcome is None else self._result(request,outcome))
            guard();return tuple(results)

    def _result(self,request,outcome):
        _require(outcome.get('state')=='SUCCEEDED' and outcome.get('version')==request.intent['version']
            and outcome.get('record_id')==request.intent['record_id']
            and outcome.get('input_sha256')==request.intent['input_sha256']
            and outcome.get('profile')==asdict(request.profile)
            and all(outcome.get(k) is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')),
            'PRIVATE_EMBEDDING_OUTCOME_UNUSABLE')
        response=outcome['response']
        valid=validate_response(TextEmbeddingResponse(tuple(response['vector']),response['truncated'],
            response['input_tokens'],response['returned_model_id']),request.profile)
        _require(response==valid and outcome['response_sha256']==sha(valid),'PRIVATE_EMBEDDING_OUTCOME_CHANGED')
        return dict(record_id=outcome['record_id'],outcome_sha256=sha(outcome),
            response_sha256=outcome['response_sha256'],dimension=request.profile.dimension,
            input_tokens=valid['input_tokens'],returned_model_id=valid['returned_model_id'],
            qualifying=False,release_allowed=False)

    def execute(self,request,*,recover_only=False):
        require_owned_worker();check_operation()
        _require(type(request) in (TextEmbeddingRequest,QueryEmbeddingRequest) and type(recover_only) is bool,'PRIVATE_EMBEDDING_REQUEST')
        with self._lock:
            self._guard(request);intent=request.intent;slot=intent['record_id']
            old=self.store.recover(intent)
            if old is not None:
                self._guard(request);return self._result(request,old)
            _require(self.enabled and not recover_only,'PRIVATE_EMBEDDING_RECOVERY_NO_DISPATCH')
            _require(slot not in self._used,'PRIVATE_EMBEDDING_DISPATCH_CONSUMED');self._used.add(slot)
            reservation=self.store.reserve(intent)
            _require(reservation.newly_reserved,'PRIVATE_EMBEDDING_RESERVATION_NO_DISPATCH')
            self._guard(request)
            outcome=dict(version=intent['version'],record_id=slot,input_sha256=intent['input_sha256'],
                intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),
                profile=asdict(request.profile),state='UNKNOWN',response=None,response_sha256=None,
                safe_error_code=None,completed_at=None,claim_approval=False,
                eligible_for_generation=False,qualifying=False,release_allowed=False)
            try:
                received=self.transport(request.profile,json.loads(request.input_json)['text'])
            except Exception:
                outcome['safe_error_code']='PRIVATE_EMBEDDING_PROVIDER_UNKNOWN'
            else:
                try:
                    value=validate_response(received,request.profile)
                    outcome.update(state='SUCCEEDED',response=value,response_sha256=sha(value))
                except (ValueError,TypeError,OverflowError):
                    outcome.update(state='REJECTED',safe_error_code='PRIVATE_EMBEDDING_PROVIDER_REJECTED')
            outcome['completed_at']=datetime.now(timezone.utc).isoformat()
            self._pending[slot]=(reservation,outcome)
            self.store.publish(reservation,outcome)
            self._pending.pop(slot,None);self._guard(request)
            return self._result(request,outcome)

    def recover_pending(self,request):
        require_owned_worker()
        _require(type(request) in (TextEmbeddingRequest,QueryEmbeddingRequest),'PRIVATE_EMBEDDING_REQUEST')
        with self._lock:
            self._guard(request);slot=request.intent['record_id'];reservation,outcome=self._pending[slot]
            expected=json.loads(reservation.intent_json);expected.pop('reservation_nonce')
            _require(expected==request.intent,'PRIVATE_EMBEDDING_PENDING_CHANGED')
            self.store.publish(reservation,outcome)
            self._pending.pop(slot,None);self._guard(request)
            return self._result(request,outcome)
