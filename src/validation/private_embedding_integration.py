"""Opt-in isolated native storage proof; synthetic sources/vectors, no model/IAP."""
import argparse,json,re,time,hashlib
from dataclasses import replace
from datetime import datetime,timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier,Lock
from unittest.mock import Mock,patch
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.application import campaign_evaluator_store as source
from src.application.operation_budget import worker_operation_scope
from src.retrieval.private_text_embeddings import DurableTextEmbedding,TextEmbeddingResponse
from src.retrieval.private_text_index import DurableTextIndex


def run(output):
    from tests.test_private_text_embeddings import request
    assert not output.exists()
    token=uuid4().hex[:12];dataset=source.PROJECT+'.ci_private_embedding_it_'+token
    assert re.fullmatch(re.escape(source.PROJECT)+r'\.ci_private_embedding_it_[a-f0-9]{12}',dataset)
    client=bigquery.Client(project=source.PROJECT,location=source.LOCATION)
    report=dict(status='RUNNING',dataset=dataset,cases=[],model_calls=0,serving_writes=0,
        authority='SYNTHETIC_FIXTURE_NOT_IAP_OR_MODEL_QUALITY',qualifying=False,release_allowed=False,cleanup={})
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    def passed(name):report['cases'].append(dict(name=name,status='PASS'));save();print(name,flush=True)
    def store():
        value=source.EvaluatorStore(client)
        def preflight():
            for kind in source.TABLES:
                actual=client.get_table(source.TABLES[kind],retry=None,timeout=20)
                assert actual.expires is not None;actual.expires=None
                source.validate_table(actual,source.table_metadata(kind))
        value.preflight=preflight;return value
    def snapshots():
        cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=128*1024**2,job_timeout_ms=60000)
        sql=' UNION ALL '.join(f"SELECT '{k}' kind,record_id,record_json FROM `{dataset}.{k}` WHERE {source.recent()}" for k in ('intents','outcomes'))
        return {(r['kind'],r['record_id']):r['record_json'] for r in client.query(sql,location=source.LOCATION,
            job_config=cfg,retry=None,job_retry=None,timeout=20).result(timeout=60,retry=None,job_retry=None)}
    def work(fn):
        with worker_operation_scope(time.monotonic()+240):return fn()
    def req(label):return replace(request(),run_id='fixture-'+token+'-'+label)
    made=False;save()
    try:
        ds=bigquery.Dataset(dataset);ds.location=source.LOCATION;ds.labels={'purpose':'private_embedding_it','run_id':token}
        ds.default_table_expiration_ms=3600000
        client.create_dataset(ds,exists_ok=False,retry=None,timeout=20);made=True
        with patch.object(source,'TABLES',{k:dataset+'.'+k for k in ('intents','outcomes')}):
            for kind in source.TABLES:client.create_table(source.table_metadata(kind),exists_ok=False,retry=None,timeout=20)
            guard=Mock(return_value=True);transport=Mock(return_value=TextEmbeddingResponse((.25,)*768,False))
            r=req('complete');s=store()
            first=work(lambda:DurableTextEmbedding(s,transport,guard,enabled=True).execute(r))
            frozen=snapshots()
            second=work(lambda:DurableTextEmbedding(store(),transport,guard).execute(r,recover_only=True))
            assert first==second and snapshots()==frozen and transport.call_count==1
            passed('native_embedding_commit_exact_binding_cold_recovery_no_redispatch')
            index=work(lambda:DurableTextIndex(store(),guard,enabled=True).build((r,)))
            frozen=snapshots()
            assert work(lambda:DurableTextIndex(store(),guard).build((r,),recover_only=True))==index
            assert snapshots()==frozen and transport.call_count==1
            passed('native_index_commit_dependency_jobs_and_cold_recovery')
            changed=json.loads(r.input_json);changed['corpus_sha256']='f'*64
            changed=replace(r,input_json=source.canonical(changed))
            try:work(lambda:DurableTextEmbedding(store(),transport,guard,enabled=True).execute(changed))
            except ValueError:pass
            else:raise AssertionError('Changed source accepted')
            assert snapshots()==frozen and transport.call_count==1
            passed('exact_source_conflict_denied_unrelated_rows_preserved')
            lost=req('lost');s=store();original=s.publish
            def lose(*args):original(*args);raise TimeoutError('Fixture lost committed response')
            s.publish=lose
            try:work(lambda:DurableTextEmbedding(s,transport,guard,enabled=True).execute(lost))
            except TimeoutError:pass
            else:raise AssertionError('Lost response not injected')
            calls=transport.call_count;before=snapshots()
            work(lambda:DurableTextEmbedding(store(),transport,guard).execute(lost,recover_only=True))
            assert transport.call_count==calls and snapshots()==before
            passed('lost_committed_response_cold_original_job_recovery')
            roll=req('rollback');before=snapshots();original_sql=source.write_sql
            def rollback(kind):
                sql=original_sql(kind)
                return sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'PRIVATE_EMBEDDING_FIXTURE_ROLLBACK'; COMMIT TRANSACTION;") if kind=='outcomes' else sql
            with patch.object(source,'write_sql',side_effect=rollback):
                try:work(lambda:DurableTextEmbedding(store(),transport,guard,enabled=True).execute(roll))
                except Exception:pass
                else:raise AssertionError('No rollback fault')
            job=client.get_job('evaluator_outcomes_v1_'+roll.intent['record_id'],location=source.LOCATION,retry=None,timeout=20)
            assert job.state=='DONE' and 'PRIVATE_EMBEDDING_FIXTURE_ROLLBACK' in str(job.error_result)
            after=snapshots();assert all(after.get(k)==v for k,v in before.items())
            assert set(after)-set(before)=={('intents',roll.intent['record_id'])}
            calls=transport.call_count
            try:work(lambda:DurableTextEmbedding(store(),transport,guard,enabled=True).execute(roll))
            except ValueError:pass
            else:raise AssertionError('Rolled back slot dispatched')
            assert transport.call_count==calls and snapshots()==after
            passed('native_rollback_unknown_slot_no_replay_unrelated_preservation')
            race=req('race');barrier=Barrier(2);counts=[];count_lock=Lock()
            def compete():
                s=store();recover=s.recover
                def together(intent):
                    value=recover(intent);barrier.wait(timeout=60);return value
                s.recover=together
                def go():
                    try:
                        value=DurableTextEmbedding(s,transport,guard,enabled=True).execute(race)
                        with count_lock:counts.append('WIN')
                        return value
                    except ValueError:
                        with count_lock:counts.append('HELD')
                return work(go)
            calls=transport.call_count
            with ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(lambda _:compete(),range(2)))
            assert sorted(counts)==['HELD','WIN'] and transport.call_count==calls+1
            assert work(lambda:DurableTextEmbedding(store(),transport,guard).execute(race,recover_only=True))
            passed('competing_native_reservations_one_provider_callback')
            report.update(status='PASS',synthetic_provider_callbacks=transport.call_count,
                index_receipt=index,embedding_receipt=first,fixture_rows=len(snapshots()),
                race_results=counts,overlapping_native_transaction_intervals='NOT_MEASURED')
    except Exception as error:
        report.update(status='FAILED',error_type=type(error).__name__);raise
    finally:
        try:
            if made:
                actual=client.get_dataset(dataset,retry=None,timeout=20)
                assert actual.labels.get('run_id')==token and actual.labels.get('purpose')=='private_embedding_it'
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
