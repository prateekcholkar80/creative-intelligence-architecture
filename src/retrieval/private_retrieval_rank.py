"""Exact bounded BigQuery lexical/vector ranks over original private ledger rows.

No vectors or source text are accepted as SQL parameters. Expected record hashes
pin the query, every document, and the complete index in one read snapshot.
"""
import hashlib,json,math
from dataclasses import asdict
from google.cloud import bigquery
from google.api_core.exceptions import NotFound,Conflict
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,sha,TABLES,recent,LOCATION
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require
from src.application.operation_budget import require_owned_worker,check_operation
from src.retrieval.private_query_embedding import QueryEmbeddingRequest
from src.retrieval.private_text_embeddings import TextEmbeddingRequest,TextEmbeddingResponse,validate_response

VERSION='private-bigquery-exact-retrieval-v1'


def ranking_sql():
    return f'''WITH pins AS (
 SELECT JSON_VALUE(p,'$.kind') kind,JSON_VALUE(p,'$.id') id,JSON_VALUE(p,'$.sha256') sha256
 FROM UNNEST(JSON_QUERY_ARRAY(@pins)) p),
records AS (
 SELECT 'intents' kind,record_id id,record_json FROM `{TABLES['intents']}` WHERE {recent()} AND record_id IN UNNEST(@ids)
 UNION ALL
 SELECT 'outcomes' kind,record_id id,record_json FROM `{TABLES['outcomes']}` WHERE {recent()} AND record_id IN UNNEST(@ids)),
guards AS (SELECT (SELECT COUNT(*) FROM pins) expected_count,(SELECT COUNT(*) FROM records) observed_count,
 (SELECT COUNT(*) FROM records r JOIN pins p ON r.kind=p.kind AND r.id=p.id
 AND LOWER(TO_HEX(SHA256(r.record_json)))=p.sha256) matched_count),
valid AS (SELECT * FROM guards WHERE expected_count=observed_count AND expected_count=matched_count),
query_data AS (SELECT JSON_VALUE(i.record_json,'$.input.text') text,
 ARRAY(SELECT CAST(x AS FLOAT64) FROM UNNEST(JSON_VALUE_ARRAY(o.record_json,'$.response.vector')) x WITH OFFSET pos ORDER BY pos) vector
 FROM records i JOIN records o ON i.id=o.id AND o.kind='outcomes' CROSS JOIN valid
 WHERE i.kind='intents' AND i.id=@query_id),
documents AS (SELECT JSON_VALUE(i.record_json,'$.input.chunk_id') chunk_id,JSON_VALUE(i.record_json,'$.input.text') text,
 ARRAY(SELECT CAST(x AS FLOAT64) FROM UNNEST(JSON_VALUE_ARRAY(o.record_json,'$.response.vector')) x WITH OFFSET pos ORDER BY pos) vector
 FROM records i JOIN records o ON i.id=o.id AND o.kind='outcomes' CROSS JOIN valid
 WHERE i.kind='intents' AND i.id IN UNNEST(@documents)),
scores AS (SELECT d.chunk_id,
 (SELECT COUNT(DISTINCT token) FROM UNNEST(TEXT_ANALYZE(q.text)) token
 WHERE token IN UNNEST(TEXT_ANALYZE(d.text))) lexical_overlap,
 ML.DISTANCE(d.vector,q.vector,'COSINE') cosine_distance FROM documents d CROSS JOIN query_data q),
lexical AS (SELECT chunk_id,ROW_NUMBER() OVER(ORDER BY lexical_overlap DESC,chunk_id) lexical_rank FROM scores WHERE lexical_overlap>0),
ranked AS (SELECT s.*,l.lexical_rank,ROW_NUMBER() OVER(ORDER BY cosine_distance,chunk_id) vector_rank
 FROM scores s LEFT JOIN lexical l USING(chunk_id))
SELECT guards.*,ARRAY(SELECT AS STRUCT * FROM ranked ORDER BY chunk_id) AS `rows` FROM guards'''


def rank_config(pins,query_id,document_ids):
    return bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
        maximum_bytes_billed=256*1024**2,job_timeout_ms=60000,query_parameters=[
            bigquery.ScalarQueryParameter('pins','STRING',canonical(pins)),
            bigquery.ArrayQueryParameter('ids','STRING',sorted({p['id'] for p in pins})),
            bigquery.ScalarQueryParameter('query_id','STRING',query_id),
            bigquery.ArrayQueryParameter('documents','STRING',list(document_ids))])


def validate_rows(row,pins,chunk_ids):
    _require(type(row) is dict and set(row)=={'expected_count','observed_count','matched_count','rows'}
        and all(type(row[k]) is int and row[k]==len(pins) for k in ('expected_count','observed_count','matched_count')),
        'PRIVATE_RETRIEVAL_PINS_CHANGED')
    rows=row['rows'];_require(type(rows) is list and len(rows)==len(chunk_ids),'PRIVATE_RETRIEVAL_ROWS')
    _require(all(type(r) is dict and set(r)=={'chunk_id','lexical_overlap','cosine_distance','lexical_rank','vector_rank'} for r in rows)
        and len({r['chunk_id'] for r in rows})==len(rows) and {r['chunk_id'] for r in rows}==set(chunk_ids),'PRIVATE_RETRIEVAL_MEMBERSHIP')
    for r in rows:
        _require(set(r)=={'chunk_id','lexical_overlap','cosine_distance','lexical_rank','vector_rank'}
            and type(r['lexical_overlap']) is int and r['lexical_overlap']>=0
            and type(r['cosine_distance']) in (int,float) and math.isfinite(r['cosine_distance'])
            and -1e-9<=r['cosine_distance']<=2+1e-9 and type(r['vector_rank']) is int
            and (r['lexical_rank'] is None if r['lexical_overlap']==0 else type(r['lexical_rank']) is int),
            'PRIVATE_RETRIEVAL_SCORES')
    for i,r in enumerate(sorted(rows,key=lambda r:(r['cosine_distance'],r['chunk_id'])),1):
        _require(r['vector_rank']==i,'PRIVATE_RETRIEVAL_VECTOR_ORDER')
    for i,r in enumerate(sorted((r for r in rows if r['lexical_overlap']>0),key=lambda r:(-r['lexical_overlap'],r['chunk_id'])),1):
        _require(r['lexical_rank']==i,'PRIVATE_RETRIEVAL_LEXICAL_ORDER')
    return rows


class BigQueryPrivateRanker:
    def __init__(self,store,guard):
        require_owned_worker();_require(type(store) is EvaluatorStore and callable(guard),'PRIVATE_RETRIEVAL_COMPOSITION')
        self.store,self.guard=store,guard

    def _pairs(self,intents):
        pairs=self.store.recover_batch(intents);result=[]
        for intent,(stored,outcome) in zip(intents,pairs):
            _require(outcome is not None and outcome.get('state')=='SUCCEEDED','PRIVATE_RETRIEVAL_OUTCOME_MISSING')
            _require(outcome['intent_sha256']==sha(stored),'PRIVATE_RETRIEVAL_INTENT_CHANGED')
            result.append((outcome,[dict(kind='intents',id=intent['record_id'],sha256=sha(stored)),
                dict(kind='outcomes',id=intent['record_id'],sha256=sha(outcome))]))
        return tuple(result)

    def _prepare(self,query,documents,index_intent,*,include_query=True):
        require_owned_worker();check_operation()
        _require(type(query) is QueryEmbeddingRequest and type(documents) is tuple and 0<len(documents)<=24
            and all(type(d) is TextEmbeddingRequest for d in documents)
            and tuple(d.ordinal for d in documents)==tuple(range(len(documents))),'PRIVATE_RETRIEVAL_INPUT')
        _require(self.guard(query) is True,'PRIVATE_RETRIEVAL_SOURCE_UNVERIFIED')
        q=json.loads(query.input_json)
        pairs=self._pairs((index_intent,)+((query.intent,) if include_query else ())+tuple(d.intent for d in documents))
        index,index_pins=pairs[0];manifest=index['manifest']
        _require(index_intent['record_id']==q['index_record_id'] and index['manifest_sha256']==q['index_sha256']==sha(manifest)
            and manifest['corpus_sha256']==q['corpus_sha256'] and manifest['chunk_count']==len(documents)
            and len(manifest['chunks'])==len(documents) and index_intent['version']==manifest['version']=='private-text-index-v1'
            and all(manifest[k]==q[k] for k in ('scope','principal_id','corpus_binding_id'))
            and manifest['profile']==asdict(documents[0].profile)
            and all(index[k] is False and manifest[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')),
            'PRIVATE_RETRIEVAL_INDEX_CHANGED')
        pins=list(index_pins)
        def vector(record,request):
            profile=request.profile
            _require(record['version']==request.intent['version'] and record['input_sha256']==request.intent['input_sha256']
                and record['profile']==asdict(profile),'PRIVATE_RETRIEVAL_VECTOR_CHANGED')
            response=record['response'];normalized=validate_response(TextEmbeddingResponse(tuple(response['vector']),response['truncated'],
                response['input_tokens'],response['returned_model_id']),profile)
            _require(response==normalized and record['response_sha256']==sha(response)
                and all(record[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')),
                'PRIVATE_RETRIEVAL_VECTOR_CHANGED')
        if include_query:
            outcome,query_pins=pairs[1];pins.extend(query_pins);vector(outcome,query)
        chunk_ids=[]
        for d,ref,pair in zip(documents,manifest['chunks'],pairs[2 if include_query else 1:]):
            data=json.loads(d.input_json)
            _require(all(data[k]==q[k] for k in ('corpus_sha256','corpus_binding_id','scope','principal_id'))
                and d.profile.model_id==query.profile.model_id and d.profile.dimension==query.profile.dimension
                and d.profile.location==query.profile.location,'PRIVATE_RETRIEVAL_SCOPE')
            v,dpins=pair;vector(v,d);pins.extend(dpins)
            _require(ref['ordinal']==d.ordinal and ref['chunk_id']==data['chunk_id']
                and ref['text_sha256']==data['provenance']['text_sha256']
                and ref['embedding_record_id']==d.intent['record_id'] and ref['embedding_outcome_sha256']==sha(v)
                and ref['vector_response_sha256']==v['response_sha256']
                and ref['intent_native_job_id']=='evaluator_intents_v1_'+d.intent['record_id']
                and ref['outcome_native_job_id']=='evaluator_outcomes_v1_'+d.intent['record_id'],'PRIVATE_RETRIEVAL_INDEX_CHANGED')
            chunk_ids.append(data['chunk_id'])
        pins.sort(key=lambda p:(p['kind'],p['id']))
        _require(len({(p['kind'],p['id']) for p in pins})==len(pins),'PRIVATE_RETRIEVAL_DUPLICATE_PIN')
        config=rank_config(pins,query.intent['record_id'],tuple(d.intent['record_id'] for d in documents));sql=ranking_sql()
        slot=sha([VERSION,query.run_id,q['scope']['workspace_id'],q['scope']['conversation_id'],q['scope']['request_id'],query.query_id])
        jid='private_retrieval_rank_v1_'+slot
        return q,pins,chunk_ids,config,sql,jid

    def verify_documents(self,query,documents,index_intent):
        """Verify complete original document/index proof before a query call."""
        self._prepare(query,documents,index_intent,include_query=False)
        self.guard_before_read(query)
        return True

    def verify_saved(self,query,documents,index_intent,observation):
        q,pins,ids,config,sql,jid=self._prepare(query,documents,index_intent)
        _require(type(observation) is dict and observation['version']==VERSION
            and observation['native_job_id']==jid and observation['sql_sha256']==hashlib.sha256(sql.encode()).hexdigest()
            and observation['config_sha256']==sha(config.to_api_repr()) and observation['pins_sha256']==sha(pins)
            and observation['query_record_id']==query.intent['record_id']
            and observation['index_record_id']==q['index_record_id'] and observation['index_sha256']==q['index_sha256']
            and observation['corpus_sha256']==q['corpus_sha256'] and observation['scope']==q['scope']
            and observation['query_input_sha256']==query.intent['input_sha256']
            and observation['query_text_sha256']==hashlib.sha256(q['text'].encode()).hexdigest()
            and all(observation[k] is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed')),
            'PRIVATE_RETRIEVAL_OBSERVATION_CHANGED')
        validate_rows(dict(expected_count=len(pins),observed_count=len(pins),matched_count=len(pins),rows=observation['rows']),pins,ids)
        job=self.store.client.get_job(jid,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=jid,sql=sql,config=config) and job.state=='DONE' and job.error_result is None,
            'PRIVATE_RETRIEVAL_NATIVE_UNVERIFIED')
        for key,actual in [('native_created_at',job.created),('native_started_at',job.started),('native_ended_at',job.ended)]:
            _require(observation[key]==(actual.isoformat() if actual else None),'PRIVATE_RETRIEVAL_NATIVE_CHANGED')
        for key in ('total_bytes_processed','total_bytes_billed'):
            _require(observation[key]==getattr(job,key),'PRIVATE_RETRIEVAL_NATIVE_CHANGED')
        self.guard_before_read(query)
        return True

    def capture(self,query,documents,index_intent,*,recover_only=False):
        _require(type(recover_only) is bool,'PRIVATE_RETRIEVAL_RECOVERY_MODE')
        q,pins,chunk_ids,config,sql,jid=self._prepare(query,documents,index_intent)
        self.guard_before_read(query);client=self.store.client
        try:job=client.get_job(jid,location=LOCATION,retry=None,timeout=20)
        except NotFound:
            _require(not recover_only,'PRIVATE_RETRIEVAL_ORIGINAL_JOB_MISSING')
            try:job=client.query(sql,job_id=jid,location=LOCATION,job_config=config,retry=None,job_retry=None,timeout=20)
            except Conflict:job=client.get_job(jid,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=jid,sql=sql,config=config),'PRIVATE_RETRIEVAL_JOB_CONFLICT')
        rows=[dict(r) for r in job.result(timeout=60,max_results=2,retry=None,job_retry=None)]
        job=client.get_job(jid,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=jid,sql=sql,config=config) and job.state=='DONE'
            and job.error_result is None and len(rows)==1,'PRIVATE_RETRIEVAL_NATIVE_UNVERIFIED')
        value=rows[0];value['rows']=[dict(r) for r in value['rows']];ranked=validate_rows(value,pins,chunk_ids)
        self.guard_before_read(query)
        return dict(version=VERSION,native_job_id=jid,sql_sha256=hashlib.sha256(sql.encode()).hexdigest(),
            config_sha256=sha(config.to_api_repr()),pins_sha256=sha(pins),rows=ranked,
            query_record_id=query.intent['record_id'],index_record_id=q['index_record_id'],index_sha256=q['index_sha256'],
            corpus_sha256=q['corpus_sha256'],native_created_at=job.created.isoformat() if job.created else None,
            scope=q['scope'],query_input_sha256=query.intent['input_sha256'],
            query_text_sha256=hashlib.sha256(q['text'].encode()).hexdigest(),
            native_started_at=job.started.isoformat() if job.started else None,native_ended_at=job.ended.isoformat() if job.ended else None,
            total_bytes_processed=job.total_bytes_processed,total_bytes_billed=job.total_bytes_billed,
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)

    def guard_before_read(self,query):
        check_operation();_require(self.guard(query) is True,'PRIVATE_RETRIEVAL_SOURCE_UNVERIFIED')
