"""Immutable retrieval calibration records with original-job cold recovery.

Only a newly reserved measurement may execute ranking. Recovery validates the
saved observation and original native job without fetching anonymous results.
"""
import hashlib,json
from threading import RLock
from src.application.campaign_evaluator_store import EvaluatorStore,Reservation,canonical,sha,LOCATION,write_sql
from src.application.sharing_jobs import matches_sharing_job
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require
from src.retrieval.private_retrieval_rank import BigQueryPrivateRanker,VERSION as RANK_VERSION,ranking_sql
from src.retrieval.private_retrieval_metrics import measure,VERSION as METRICS_VERSION

VERSION='private-retrieval-measurement-v1'


class DurableRetrievalMeasurement:
    def __init__(self,store,ranker,guard,*,enabled=False):
        require_owned_worker()
        _require(type(store) is EvaluatorStore and type(ranker) is BigQueryPrivateRanker and ranker.store is store
            and callable(guard) and type(enabled) is bool,'PRIVATE_MEASUREMENT_COMPOSITION')
        self.store,self.ranker,self.guard,self.enabled=store,ranker,guard,enabled
        self._lock=RLock();self._pending={}

    def intent(self,query,documents,index_intent,case):
        q=json.loads(query.input_json)
        slot=sha([VERSION,query.run_id,q['scope']['workspace_id'],q['scope']['conversation_id'],q['scope']['request_id'],query.query_id])
        return dict(version=VERSION,record_id=slot,run_id=query.run_id,query_id=query.query_id,
            scope=q['scope'],principal_id=q['principal_id'],case=case,case_sha256=sha(case),
            query_intent=query.intent,document_intents=[dict(record_id=d.intent['record_id'],intent_sha256=sha(d.intent)) for d in documents],
            index_intent=index_intent,ranking_version=RANK_VERSION,ranking_sql_sha256=hashlib.sha256(ranking_sql().encode()).hexdigest(),
            metrics_version=METRICS_VERSION,claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)

    def _check(self,query,case,chunks):
        check_operation();_require(self.guard(query) is True,'PRIVATE_MEASUREMENT_SOURCE_UNVERIFIED')
        q=json.loads(query.input_json)
        # Validate exact labels before reserving or running a read. A temporary
        # empty lexical list and ordered vector list validate coverage only.
        rows=[dict(chunk_id=c['chunk_id'],lexical_rank=None,vector_rank=i) for i,c in enumerate(chunks,1)]
        measure(dict(corpus_sha256=q['corpus_sha256'],index_sha256=q['index_sha256'],scope=q['scope'],
            query_text_sha256=hashlib.sha256(q['text'].encode()).hexdigest(),rows=rows),case,chunks,q['scope'])

    def _original_reservation(self,intent):
        """Recover the exact committed reservation, never reserve another slot."""
        jid='evaluator_intents_v1_'+intent['record_id']
        job=self.store.client.get_job(jid,location=LOCATION,retry=None,timeout=20)
        raw={p.name:p.value for p in job.query_parameters}['record']
        stored=json.loads(raw);base=dict(stored);base.pop('reservation_nonce')
        _require(base==intent and canonical(stored)==raw and job.state=='DONE' and job.error_result is None
            and matches_sharing_job(job,job_id=jid,sql=write_sql('intents'),
                config=self.store._config(intent['record_id'],raw,'intents')),'PRIVATE_MEASUREMENT_ORIGINAL_RESERVATION_CHANGED')
        return Reservation(intent['record_id'],raw,False)

    def _receipt(self,intent,outcome,query,documents,index_intent,case,chunks,*,verify_native=True):
        _require(outcome.get('state')=='SUCCEEDED' and outcome.get('version')==VERSION
            and outcome.get('record_id')==intent['record_id']
            and all(outcome.get(k) is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')),
            'PRIVATE_MEASUREMENT_OUTCOME_UNUSABLE')
        observation=outcome['observation'];metrics=outcome['measurement']
        _require(outcome['observation_sha256']==sha(observation) and outcome['measurement_sha256']==sha(metrics)
            and metrics==measure(observation,case,chunks,intent['scope']),'PRIVATE_MEASUREMENT_CHANGED')
        if verify_native:self.ranker.verify_saved(query,documents,index_intent,observation)
        self._check(query,case,chunks)
        return dict(run_id=query.run_id,case_id=case['case_id'],measurement_record_id=intent['record_id'],
            outcome_sha256=sha(outcome),query_embedding_record_id=query.intent['record_id'],
            index_record_id=observation['index_record_id'],index_sha256=observation['index_sha256'],
            corpus_sha256=observation['corpus_sha256'],native_ranking_job_id=observation['native_job_id'],
            observation_sha256=sha(observation),measurement_sha256=sha(metrics),
            rankings=metrics['rankings'],metrics=metrics['metrics'],label_authority=metrics['label_authority'],
            semantic_reranking='NOT_EXECUTED',image_embedding_status='NOT_EXECUTED',
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)

    def execute(self,query,documents,index_intent,case,chunks,*,recover_only=False):
        require_owned_worker();_require(type(recover_only) is bool,'PRIVATE_MEASUREMENT_MODE')
        with self._lock:
            self._check(query,case,chunks);intent=self.intent(query,documents,index_intent,case)
            slot=intent['record_id'];pending=self._pending.get(slot)
            if pending is not None:
                reservation,outcome=pending;stored=json.loads(reservation.intent_json);stored.pop('reservation_nonce')
                _require(stored==intent,'PRIVATE_MEASUREMENT_PENDING_CHANGED')
                self.ranker.verify_saved(query,documents,index_intent,outcome['observation'])
                self.store.publish(reservation,outcome);self._pending.pop(slot,None)
            old=self.store.recover(intent)
            if old is not None and old.get('state')!='UNKNOWN':
                return self._receipt(intent,old,query,documents,index_intent,case,chunks)
            if old is not None:
                _require(recover_only,'PRIVATE_MEASUREMENT_OUTCOME_UNUSABLE')
                reservation=self._original_reservation(intent)
            else:
                _require(self.enabled and not recover_only,'PRIVATE_MEASUREMENT_RECOVERY_NO_WRITE')
                reservation=self.store.reserve(intent)
                _require(reservation.newly_reserved,'PRIVATE_MEASUREMENT_RESERVATION_UNVERIFIED')
            self._check(query,case,chunks)
            observation=self.ranker.capture(query,documents,index_intent,recover_only=True) if recover_only else self.ranker.capture(query,documents,index_intent)
            metrics=measure(observation,case,chunks,intent['scope']);self._check(query,case,chunks)
            outcome=dict(version=VERSION,record_id=slot,state='SUCCEEDED',
                intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),
                observation=observation,observation_sha256=sha(observation),measurement=metrics,measurement_sha256=sha(metrics),
                claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)
            self._pending[slot]=(reservation,outcome)
            self.store.publish(reservation,outcome);self._pending.pop(slot,None)
            # capture already verified this exact native result and dependencies;
            # recovery independently re-verifies them without cached results.
            return self._receipt(intent,outcome,query,documents,index_intent,case,chunks,verify_native=False)
