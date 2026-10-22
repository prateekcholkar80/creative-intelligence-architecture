"""Complete, version-pinned private index manifest over BigQuery vector outcomes.

Vectors remain in their original evaluator outcome rows with 30-day retention.
This index is not retrieval measurement, source approval or serving eligibility.
"""
from dataclasses import asdict
import hashlib,json
from threading import RLock
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require
from src.retrieval.private_text_embeddings import TextEmbeddingRequest,DurableTextEmbedding

VERSION='private-text-index-v1'


class DurableTextIndex:
    def __init__(self,store,guard,*,enabled=False,guard_all=None):
        require_owned_worker()
        _require(type(store) is EvaluatorStore and callable(guard) and type(enabled) is bool
            and (guard_all is None or callable(guard_all)),
            'PRIVATE_INDEX_COMPOSITION')
        self.store,self.guard,self.enabled=store,guard,enabled
        self.guard_all=guard_all
        self._lock=RLock();self._pending={}

    def _check(self,requests):
        check_operation()
        _require(type(requests) is tuple and 0<len(requests)<=24
            and all(type(r) is TextEmbeddingRequest for r in requests)
            and tuple(r.ordinal for r in requests)==tuple(range(len(requests))),
            'PRIVATE_INDEX_REQUESTS')
        first=requests[0];base=json.loads(first.input_json)
        _require(len(requests)==base['corpus_chunk_count'],'PRIVATE_INDEX_INCOMPLETE')
        for r in requests:
            value=json.loads(r.input_json)
            _require(r.run_id==first.run_id and r.profile==first.profile
                and all(value[k]==base[k] for k in ('corpus_sha256','corpus_binding_id','corpus_chunk_count','scope','principal_id')),
                'PRIVATE_INDEX_MIXED_CORPUS')
            if self.guard_all is None:_require(self.guard(r) is True,'PRIVATE_INDEX_SOURCE_UNVERIFIED')
        _require(len({json.loads(r.input_json)['chunk_id'] for r in requests})==len(requests),
            'PRIVATE_INDEX_DUPLICATE_CHUNK')
        if self.guard_all is not None:_require(self.guard_all(requests) is True,'PRIVATE_INDEX_SOURCE_UNVERIFIED')
        return base

    def _intent(self,requests,base):
        first=requests[0]
        slot=sha([VERSION,first.run_id,base['scope']['workspace_id'],
            base['scope']['conversation_id'],base['scope']['request_id']])
        return dict(version=VERSION,record_id=slot,run_id=first.run_id,
            corpus_sha256=base['corpus_sha256'],corpus_binding_id=base['corpus_binding_id'],
            scope=base['scope'],principal_id=base['principal_id'],profile=asdict(first.profile),
            # All expected embedding intent bytes are pinned, not just chunk IDs.
            embedding_intents=[dict(record_id=r.intent['record_id'],intent_sha256=sha(r.intent)) for r in requests],
            claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)

    def _manifest(self,requests,intent):
        references=[]
        validator=DurableTextEmbedding(self.store,lambda *_:None,self.guard,enabled=False)
        for r in requests:
            check_operation()
            outcome=self.store.recover(r.intent)
            _require(outcome is not None,'PRIVATE_INDEX_EMBEDDING_MISSING')
            receipt=validator._result(r,outcome)
            data=json.loads(r.input_json)
            references.append(dict(ordinal=r.ordinal,chunk_id=data['chunk_id'],
                text_sha256=data['provenance']['text_sha256'],embedding_record_id=receipt['record_id'],
                embedding_outcome_sha256=receipt['outcome_sha256'],vector_response_sha256=receipt['response_sha256'],
                intent_native_job_id='evaluator_intents_v1_'+receipt['record_id'],
                outcome_native_job_id='evaluator_outcomes_v1_'+receipt['record_id']))
        return dict(version=VERSION,corpus_sha256=intent['corpus_sha256'],
            corpus_binding_id=intent['corpus_binding_id'],scope=intent['scope'],principal_id=intent['principal_id'],
            profile=intent['profile'],chunks=references,chunk_count=len(references),
            vector_authority='BIGQUERY_ORIGINAL_EVALUATOR_OUTCOMES',retention_days=30,
            use='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY',retrieval_quality='NOT_MEASURED',
            claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)

    def _receipt(self,intent,outcome,manifest):
        _require(outcome.get('version')==VERSION and outcome.get('record_id')==intent['record_id']
            and outcome.get('state')=='SUCCEEDED' and outcome.get('manifest')==manifest
            and outcome.get('manifest_sha256')==sha(manifest)
            and all(outcome.get(k) is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')),
            'PRIVATE_INDEX_OUTCOME_UNVERIFIED')
        return dict(index_record_id=intent['record_id'],index_sha256=sha(manifest),
            corpus_sha256=intent['corpus_sha256'],chunk_count=manifest['chunk_count'],
            dimension=intent['profile']['dimension'],claim_approval=False,
            generation_authorized=False,qualifying=False,release_allowed=False)

    def build(self,requests,*,recover_only=False):
        require_owned_worker();_require(type(recover_only) is bool,'PRIVATE_INDEX_RECOVERY_MODE')
        with self._lock:
            base=self._check(requests);intent=self._intent(requests,base)
            # Verify every vector's original intent/outcome jobs on both build
            # and cold recovery. An index row alone never supplies vectors.
            manifest=self._manifest(requests,intent)
            old=self.store.recover(intent)
            if old is not None:
                self._check(requests);return self._receipt(intent,old,manifest)
            _require(self.enabled and not recover_only,'PRIVATE_INDEX_RECOVERY_NO_WRITE')
            reservation=self.store.reserve(intent)
            _require(reservation.newly_reserved,'PRIVATE_INDEX_RESERVATION_UNVERIFIED')
            self._check(requests)
            outcome=dict(version=VERSION,record_id=intent['record_id'],state='SUCCEEDED',
                intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),
                manifest=manifest,manifest_sha256=sha(manifest),claim_approval=False,
                eligible_for_generation=False,qualifying=False,release_allowed=False)
            self._pending[intent['record_id']]=(reservation,outcome)
            self.store.publish(reservation,outcome);self._pending.pop(intent['record_id'],None)
            self._check(requests)
            return self._receipt(intent,outcome,manifest)

    def recover_pending(self,requests):
        require_owned_worker()
        with self._lock:
            base=self._check(requests);intent=self._intent(requests,base)
            reservation,outcome=self._pending[intent['record_id']]
            stored=json.loads(reservation.intent_json);stored.pop('reservation_nonce')
            _require(stored==intent and outcome['manifest']==self._manifest(requests,intent),
                'PRIVATE_INDEX_PENDING_CHANGED')
            self._check(requests);self.store.publish(reservation,outcome)
            self._pending.pop(intent['record_id'],None);self._check(requests)
            return self._receipt(intent,outcome,outcome['manifest'])
