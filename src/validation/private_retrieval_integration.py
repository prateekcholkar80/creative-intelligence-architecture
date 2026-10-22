"""Opt-in exact ranking SQL proof in a disposable, synthetic BigQuery dataset."""
import argparse,json,math,re,time,hashlib
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.application import campaign_evaluator_store as source
from src.retrieval import private_retrieval_rank as ranking
from src.application.operation_budget import worker_operation_scope


def run(output):
    assert not output.exists()
    token=uuid4().hex[:12];dataset=source.PROJECT+'.ci_private_retrieval_it_'+token
    assert re.fullmatch(re.escape(source.PROJECT)+r'\.ci_private_retrieval_it_[a-f0-9]{12}',dataset)
    client=bigquery.Client(project=source.PROJECT,location=source.LOCATION)
    report=dict(status='RUNNING',dataset=dataset,cases=[],model_calls=0,serving_writes=0,
        authority='SYNTHETIC_FIXTURE_NOT_IAP_OR_RETRIEVAL_QUALITY',qualifying=False,release_allowed=False,cleanup={})
    report['implementation_sha256']={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in (
        Path(ranking.__file__),Path('src/retrieval/private_retrieval_measurement.py'),Path(__file__))}
    output.parent.mkdir(parents=True,exist_ok=True);made=False
    def save():output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    def passed(name):report['cases'].append(dict(name=name,status='PASS'));save();print(name,flush=True)
    def query(sql,config=None):
        return list(client.query(sql,location=source.LOCATION,job_config=config or bigquery.QueryJobConfig(
            use_query_cache=False,maximum_bytes_billed=256*1024**2,job_timeout_ms=60000),
            retry=None,job_retry=None,timeout=20).result(timeout=60,retry=None,job_retry=None))
    records=[]
    def pair(id,text,vector,chunk=None):
        records.extend([dict(kind='intents',id=id,value=dict(input=dict(text=text,**(dict(chunk_id=chunk) if chunk else {})))),
            dict(kind='outcomes',id=id,value=dict(response=dict(vector=vector)))])
    pair('q','How many drops?', [1.,0.])
    pair('d1','Use four drops', [1.,0.],'a')
    pair('d2','Other product', [0.,1.],'b')
    pair('d3','Drops and many drops', [-1.,0.],'c')
    records.extend([dict(kind='intents',id='index',value={'manifest':'fixture'}),
        dict(kind='outcomes',id='index',value={'manifest':'fixture'})])
    pins=[dict(kind=r['kind'],id=r['id'],sha256=source.sha(r['value'])) for r in records]
    tables={k:dataset+'.'+k for k in ('intents','outcomes')}
    def insert(rows):
        for kind in tables:
            values=[dict(id=r['id'],raw=source.canonical(r['value'])) for r in rows if r['kind']==kind]
            if not values:continue
            cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=256*1024**2,job_timeout_ms=60000,
                query_parameters=[bigquery.ScalarQueryParameter('rows','STRING',source.canonical(values))])
            query(f"INSERT `{tables[kind]}` SELECT JSON_VALUE(r,'$.id'),JSON_VALUE(r,'$.raw'),CURRENT_TIMESTAMP() FROM UNNEST(JSON_QUERY_ARRAY(@rows)) r",cfg)
    def read(expected=pins):
        rows=query(ranking.ranking_sql(),ranking.rank_config(expected,'q',('d1','d2','d3')))
        assert len(rows)==1
        value=dict(rows[0]);value['rows']=[dict(r) for r in value['rows']];return value
    def snapshots():
        return {(r['kind'],r['id'],r['record_json']) for r in query(' UNION ALL '.join(
            f"SELECT '{k}' kind,record_id id,record_json FROM `{t}` WHERE {source.recent()}" for k,t in tables.items()))}
    save()
    try:
        ds=bigquery.Dataset(dataset);ds.location=source.LOCATION
        ds.labels={'purpose':'private_retrieval_it','run_id':token};ds.default_table_expiration_ms=3600000
        client.create_dataset(ds,exists_ok=False,retry=None,timeout=20);made=True
        with patch.object(source,'TABLES',tables),patch.object(ranking,'TABLES',tables):
            for kind in tables:client.create_table(source.table_metadata(kind),exists_ok=False,retry=None,timeout=20)
            insert(records);before=snapshots();value=read();rows=ranking.validate_rows(value,pins,('a','b','c'))
            by={r['chunk_id']:r for r in rows}
            assert [by[i]['vector_rank'] for i in ('a','b','c')]==[1,2,3]
            assert by['c']['lexical_rank']==1 and by['a']['lexical_rank']==2 and by['b']['lexical_rank'] is None
            assert all(math.isclose(by[i]['cosine_distance'],v,abs_tol=1e-9) for i,v in zip(('a','b','c'),(0,1,2)))
            passed('native_exact_cosine_lexical_and_deterministic_ties')
            assert read()==value and snapshots()==before
            passed('repeat_read_exact_unrelated_preservation')
            changed=[dict(p) for p in pins];changed[0]['sha256']='f'*64
            rejected=read(changed);assert rejected['rows']==[] and rejected['matched_count']==len(pins)-1
            try:ranking.validate_rows(rejected,changed,('a','b','c'))
            except ValueError:pass
            else:raise AssertionError('Changed pin accepted')
            assert snapshots()==before;passed('changed_record_hash_denied_without_mutation')
            insert([records[0]]);duplicate=read();assert duplicate['rows']==[] and duplicate['observed_count']==len(pins)+1
            try:ranking.validate_rows(duplicate,pins,('a','b','c'))
            except ValueError:pass
            else:raise AssertionError('Duplicate accepted')
            passed('duplicate_native_row_denied')
            from tests.test_private_text_embeddings import request
            from src.retrieval.upload_corpus import UploadCorpusCapture,VERSION as CORPUS_VERSION
            from src.retrieval.private_text_embeddings import DurableTextEmbedding,TextEmbeddingResponse
            from src.retrieval.private_text_index import DurableTextIndex
            from src.retrieval.private_query_embedding import query_request
            from src.retrieval.private_retrieval_measurement import DurableRetrievalMeasurement
            from datetime import datetime,timezone,timedelta
            def store():
                value=source.EvaluatorStore(client)
                def preflight():
                    for kind in tables:
                        actual=client.get_table(tables[kind],retry=None,timeout=20)
                        assert actual.expires is not None;actual.expires=None
                        source.validate_table(actual,source.table_metadata(kind))
                value.preflight=preflight;return value
            def work(fn):
                with worker_operation_scope(time.monotonic()+300):return fn()
            docs=tuple(replace(request('Use four drops',ordinal=i,count=3),run_id='native-doc-'+token) for i in range(3))
            data=json.loads(docs[0].input_json)
            corpus=dict(version=CORPUS_VERSION,use='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY',
                scope=data['scope'],principal_id=data['principal_id'],text_chunks=[dict(chunk_id=json.loads(d.input_json)['chunk_id'],
                    provenance=json.loads(d.input_json)['provenance'],text=json.loads(d.input_json)['text'],
                    claim_approval=False,eligible_for_generation=False,trust='UNTRUSTED_USER_EVIDENCE') for d in docs],
                claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)
            capture=UploadCorpusCapture(source.canonical(corpus),(),datetime.now(timezone.utc)+timedelta(minutes=20))
            assert capture.sha256==data['corpus_sha256']
            s=store();guard=lambda _:True
            def make_vectors():
                engine=DurableTextEmbedding(s,lambda *_:TextEmbeddingResponse((.25,)*768,False),guard,enabled=True)
                for d in docs:engine.execute(d)
                index=DurableTextIndex(s,guard,enabled=True)
                return index.build(docs),index._intent(docs,data)
            index,index_intent=work(make_vectors)
            q=query_request(capture,data['corpus_binding_id'],index,'native-query-'+token,'fixture','How many drops?')
            work(lambda:DurableTextEmbedding(s,lambda *_:TextEmbeddingResponse((.25,)*768,False),guard,enabled=True).execute(q))
            case=dict(version='private-retrieval-relevance-case-v1',case_id='synthetic-fixture',
                corpus_sha256=capture.sha256,index_sha256=index['index_sha256'],
                query_text_sha256=hashlib.sha256(json.loads(q.input_json)['text'].encode()).hexdigest(),
                label_authority='SOURCE_REVIEW_CALIBRATION_NOT_INDEPENDENT_QUALIFICATION',
                scope={k:data['scope'][k] for k in ('sku','country','channel')},
                labels=[dict(chunk_id=c['chunk_id'],text_sha256=c['provenance']['text_sha256'],grade=3 if i==0 else 0)
                    for i,c in enumerate(corpus['text_chunks'])])
            def measurement(recover=False):
                ranker=ranking.BigQueryPrivateRanker(s,guard)
                runner=DurableRetrievalMeasurement(s,ranker,guard,enabled=not recover)
                return runner.execute(q,docs,index_intent,case,corpus['text_chunks'],recover_only=recover)
            receipt=work(measurement);before=snapshots()
            with patch.object(ranking.BigQueryPrivateRanker,'capture',side_effect=AssertionError('Recovery ran ranking')):
                assert work(lambda:measurement(True))==receipt and snapshots()==before
            passed('native_ledger_to_rank_to_measurement_commit_and_cold_recovery')
            original=s.publish
            def lost(reservation,outcome):
                value=original(reservation,outcome)
                if outcome['version']=='private-retrieval-measurement-v1':raise TimeoutError('Injected lost committed measurement response')
                return value
            q=replace(q,run_id='native-query-lost-'+token)
            work(lambda:DurableTextEmbedding(s,lambda *_:TextEmbeddingResponse((.25,)*768,False),guard,enabled=True).execute(q))
            s.publish=lost
            try:work(measurement)
            except TimeoutError:pass
            else:raise AssertionError('Lost measurement response not injected')
            s.publish=original;before=snapshots()
            with patch.object(ranking.BigQueryPrivateRanker,'capture',side_effect=AssertionError('Recovery ran ranking')):
                recovered=work(lambda:measurement(True));assert snapshots()==before
            assert recovered['run_id']==q.run_id
            passed('native_lost_committed_measurement_response_cold_recovery_no_ranking_repeat')
            q=replace(q,run_id='native-query-rankonly-'+token)
            work(lambda:DurableTextEmbedding(s,lambda *_:TextEmbeddingResponse((.25,)*768,False),guard,enabled=True).execute(q))
            original_capture=ranking.BigQueryPrivateRanker.capture
            def rank_response_lost(self,*args,**kwargs):
                original_capture(self,*args,**kwargs)
                raise TimeoutError('Injected loss after original native ranking completed')
            with patch.object(ranking.BigQueryPrivateRanker,'capture',new=rank_response_lost):
                try:work(measurement)
                except TimeoutError:pass
                else:raise AssertionError('Rank response loss not injected')
            before=snapshots();recovered=work(lambda:measurement(True));after=snapshots()
            assert all(value in after for value in before)
            assert len(after-before)==1 and recovered['run_id']==q.run_id
            assert work(lambda:measurement(True))==recovered and snapshots()==after
            passed('cold_reserved_measurement_finishes_original_rank_job_without_reservation_or_rank_replay')
            q=replace(q,run_id='native-query-norank-'+token)
            work(lambda:DurableTextEmbedding(s,lambda *_:TextEmbeddingResponse((.25,)*768,False),guard,enabled=True).execute(q))
            def reserve_only():
                runner=DurableRetrievalMeasurement(s,ranking.BigQueryPrivateRanker(s,guard),guard)
                s.reserve(runner.intent(q,docs,index_intent,case))
            work(reserve_only);before=snapshots()
            try:work(lambda:measurement(True))
            except ValueError as error:assert 'PRIVATE_RETRIEVAL_ORIGINAL_JOB_MISSING' in str(error)
            else:raise AssertionError('Missing original ranking was invented')
            assert snapshots()==before
            passed('cold_reserved_measurement_without_original_rank_denies_without_mutation')
            report['synthetic_measurement_receipt']=receipt
            report.update(status='PASS',rows=rows,sql_sha256=source.sha(ranking.ranking_sql()))
    except Exception as error:
        report.update(status='FAILED',error_type=type(error).__name__,error=str(error)[:1500]);raise
    finally:
        try:
            if made:
                actual=client.get_dataset(dataset,retry=None,timeout=20)
                assert actual.labels.get('run_id')==token and actual.labels.get('purpose')=='private_retrieval_it'
                client.delete_dataset(dataset,delete_contents=True,not_found_ok=False,retry=None,timeout=20)
                try:client.get_dataset(dataset,retry=None,timeout=20)
                except NotFound:report['cleanup']['dataset_absent']=True
                else:raise AssertionError('Fixture still exists')
        finally:save();client.close()
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true')
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    if not args.run:parser.error('Explicit --run required; creates/deletes an owned isolated fixture dataset')
    run(args.output)
