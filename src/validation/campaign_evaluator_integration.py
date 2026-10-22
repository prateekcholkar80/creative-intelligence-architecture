"""Owned native evaluator storage fixtures; no stage/model execution or release."""
import argparse,hashlib,json,re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.application import campaign_evaluator_store as source


def fixture_sql(sql,dataset):
    if not re.fullmatch(re.escape(source.PROJECT)+r'\.ci_evaluator_it_[a-f0-9]{12}',dataset):raise ValueError('EVALUATOR_FIXTURE_SCOPE')
    refs=re.findall(r'`([^`]+)`',sql)
    if not refs or not all(r in {dataset+'.intents',dataset+'.outcomes'} for r in refs):raise ValueError('EVALUATOR_FIXTURE_SQL')
    if re.search(r'\b(DELETE|DROP|TRUNCATE|MERGE)\b',sql,re.I):raise ValueError('EVALUATOR_FIXTURE_MUTATION')
    return sql


def run(output):
    if output.exists():raise ValueError('EVALUATOR_REPORT_EXISTS')
    token=uuid4().hex[:12];dataset=source.PROJECT+'.ci_evaluator_it_'+token
    client=bigquery.Client(project=source.PROJECT,location=source.LOCATION)
    report=dict(status='RUNNING',dataset=dataset,cases=[],cleanup={},model_calls=0,serving_writes=0,qualifying=False)
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    def passed(name):report['cases'].append(dict(name=name,status='PASSED'));save();print(name,flush=True)
    def snapshot():
        sql=' UNION ALL '.join(f"SELECT '{kind}' kind,TO_JSON_STRING(t) row FROM `{dataset}.{kind}` t WHERE {source.recent()}" for kind in ('intents','outcomes'))
        fixture_sql(sql,dataset)
        job=client.query(sql,location=source.LOCATION,retry=None,job_retry=None,timeout=20,
            job_config=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=128*1024**2,job_timeout_ms=60000))
        return sorted((r['kind'],r['row']) for r in job.result(timeout=60,retry=None,job_retry=None))
    class FixtureStore(source.EvaluatorStore):
        def preflight(self):
            for kind in source.TABLES:
                actual=client.get_table(source.TABLES[kind],retry=None,timeout=20)
                assert actual.expires is not None;actual.expires=None
                source.validate_table(actual,source.table_metadata(kind))
    created=False;save()
    try:
        ds=bigquery.Dataset(dataset);ds.location=source.LOCATION;ds.labels={'purpose':'evaluator_integration','run_id':token};ds.default_table_expiration_ms=3600000
        client.create_dataset(ds,exists_ok=False,retry=None,timeout=20);created=True
        with patch.object(source,'TABLES',dict(intents=dataset+'.intents',outcomes=dataset+'.outcomes')):
            for kind in source.TABLES:client.create_table(source.table_metadata(kind),exists_ok=False,retry=None,timeout=20)
            for kind in source.TABLES:fixture_sql(source.write_sql(kind),dataset)
            store=FixtureStore(client)
            def intent(label):return dict(record_id=hashlib.sha256((token+label).encode()).hexdigest(),fixture=token,label=label)
            def outcome(reservation):return dict(record_id=reservation.record_id,intent_sha256=hashlib.sha256(reservation.intent_json.encode()).hexdigest(),state='SYNTHETIC_STORAGE_ONLY',release_allowed=False)
            unrelated=store.reserve(intent('unrelated'),nonce='unrelated');store.publish(unrelated,outcome(unrelated));before=snapshot()
            first=intent('core');reservation=store.reserve(first,nonce='owner');assert reservation.newly_reserved
            value=outcome(reservation);store.publish(reservation,value);after=snapshot()
            assert set(before).issubset(after) and len(after)==len(before)+2
            assert FixtureStore(client).recover(first)==value and snapshot()==after
            assert not store.reserve(first,nonce='owner').newly_reserved
            passed('commit_exact_original_job_replay_and_cold_recovery')
            try:store.reserve(dict(first,label='changed'),nonce='owner')
            except ValueError as error:assert 'EVALUATOR_ORIGINAL_JOB_CONFLICT' in str(error)
            else:raise AssertionError('Changed original accepted')
            assert snapshot()==after;passed('conflicting_intent_denied_exact_snapshot')
            real_query=client.query
            def lose(*a,**kw):real_query(*a,**kw);raise TimeoutError('Synthetic lost response')
            lost=intent('lost-reservation')
            with patch.object(client,'query',side_effect=lose):
                try:store.reserve(lost,nonce='lost-owner')
                except TimeoutError:pass
                else:raise AssertionError('No lost reservation fault')
            native=client.get_job('evaluator_intents_v1_'+lost['record_id'],location=source.LOCATION,retry=None,timeout=20)
            native.result(timeout=60,retry=None,job_retry=None)
            assert FixtureStore(client).recover(lost)['state']=='UNKNOWN'
            assert not store.reserve(lost,nonce='lost-owner').newly_reserved
            passed('lost_reservation_cold_unknown_no_dispatch_permission')
            pending=intent('lost-outcome');reservation=store.reserve(pending,nonce='pending-owner');value=outcome(reservation)
            with patch.object(client,'query',side_effect=lose):
                try:store.publish(reservation,value)
                except TimeoutError:pass
                else:raise AssertionError('No lost outcome fault')
            client.get_job('evaluator_outcomes_v1_'+pending['record_id'],location=source.LOCATION,retry=None,timeout=20).result(timeout=60,retry=None,job_retry=None)
            assert FixtureStore(client).recover(pending)==value
            frozen=snapshot();store.publish(reservation,value);assert snapshot()==frozen
            passed('lost_outcome_cold_recovery_and_exact_publication_replay')
            roll=intent('rollback');reservation=store.reserve(roll,nonce='rollback-owner');before=snapshot();original_sql=source.write_sql
            def rollback(kind):return original_sql(kind).replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'EVALUATOR_FIXTURE_ROLLBACK'; COMMIT TRANSACTION;")
            with patch.object(source,'write_sql',side_effect=rollback):
                try:store.publish(reservation,outcome(reservation))
                except Exception:pass
                else:raise AssertionError('No rollback fault')
            native=client.get_job('evaluator_outcomes_v1_'+roll['record_id'],location=source.LOCATION,retry=None,timeout=20)
            assert native.state=='DONE' and 'EVALUATOR_FIXTURE_ROLLBACK' in str(native.error_result)
            assert snapshot()==before;passed('outcome_rollback_exact_unrelated_snapshot')
            race=intent('race');before=snapshot();barrier=Barrier(2)
            def contender(nonce):
                barrier.wait(timeout=10)
                try:return FixtureStore(client).reserve(race,nonce=nonce).newly_reserved
                except ValueError as error:
                    assert 'EVALUATOR_ORIGINAL_JOB_CONFLICT' in str(error);return False
            with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(contender,('first','second')))
            after=snapshot();assert sorted(results)==[False,True] and len(after)==len(before)+1 and set(before).issubset(after)
            passed('competing_same_native_slot_only_one_dispatch_permission')
            report['competition_scope']='Same native job with different reservation nonces; not a distinct-job transaction race'
            report['status']='PASSED'
    except BaseException as error:report.update(status='FAILED',safe_error_type=type(error).__name__,safe_error=str(error)[:1000]);raise
    finally:
        try:
            if created:
                ds=client.get_dataset(dataset,retry=None,timeout=20)
                assert ds.labels=={'purpose':'evaluator_integration','run_id':token} and ds.location.lower()==source.LOCATION
                client.delete_dataset(dataset,delete_contents=True,not_found_ok=False,retry=None,timeout=20)
            try:client.get_dataset(dataset,retry=None,timeout=20)
            except NotFound:report['cleanup']['dataset_absent']=True
            assert report['cleanup'].get('dataset_absent')
        except BaseException:
            report['status']='FAILED_CLEANUP';raise
        finally:save();client.close()
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',required=True,action='store_true');p.add_argument('--output',type=Path,required=True);args=p.parse_args();run(args.output)


if __name__=='__main__':main()
