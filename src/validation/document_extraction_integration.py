"""Opt-in native extraction transactions in an owned expiring dataset only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib,json,re
from pathlib import Path
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.application import campaign_document_store as source
from src.application.campaign_document_extraction import parse_document
from src.application.campaign_evidence_gaps import canonical


def fixture_sql(sql,dataset):
    if not re.fullmatch(re.escape(source.PROJECT)+r'\.ci_document_it_[a-f0-9]{12}',dataset):
        raise ValueError('DOCUMENT_FIXTURE_SCOPE')
    for name in re.findall(r'`([^`]+)`',sql):
        if not name.startswith(dataset+'.') or name.rsplit('.',1)[-1] not in ('submissions','extractions'):
            raise ValueError('DOCUMENT_FIXTURE_SQL')
    if re.search(r'\b(DELETE|DROP|TRUNCATE|MERGE)\b',sql,re.I):raise ValueError('DOCUMENT_FIXTURE_MUTATION')
    return sql


def run(output):
    if output.exists():raise ValueError('DOCUMENT_REPORT_EXISTS')
    token=uuid4().hex[:12];dataset=source.PROJECT+'.ci_document_it_'+token
    client=bigquery.Client(project=source.PROJECT,location=source.LOCATION)
    report=dict(status='RUNNING',dataset=dataset,cases=[],jobs=[],cleanup={},serving_writes=0,model_calls=0,release_enabled=False)
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    def passed(name):report['cases'].append(dict(name=name,status='PASSED'));save();print(name,flush=True)
    def query(sql,params=(),job_id=None):
        fixture_sql(sql,dataset)
        job=client.query(sql,job_id=job_id,location=source.LOCATION,retry=None,job_retry=None,timeout=20,
            job_config=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=256*1024**2,job_timeout_ms=60000,query_parameters=list(params)))
        return list(job.result(timeout=60,retry=None,job_retry=None))
    def snapshot():
        return {k:sorted(r['row'] for r in query(f'SELECT TO_JSON_STRING(t) row FROM `{dataset}.{k}` t')) for k in ('submissions','extractions')}
    created=False;save()
    try:
        ds=bigquery.Dataset(dataset);ds.location=source.LOCATION;ds.labels={'purpose':'document_integration','run_id':token};ds.default_table_expiration_ms=3600000
        client.create_dataset(ds,exists_ok=False,retry=None,timeout=20);created=True
        for kind in ('submissions','extractions'):
            fields=[bigquery.SchemaField('submission_id' if kind=='submissions' else 'record_id','STRING',mode='REQUIRED'),bigquery.SchemaField('record_json','STRING',mode='REQUIRED'),bigquery.SchemaField('recorded_at','TIMESTAMP',mode='REQUIRED')]
            table=bigquery.Table(dataset+'.'+kind,schema=fields)
            if kind=='extractions':table.clustering_fields=['record_id']
            client.create_table(table,exists_ok=False,retry=None,timeout=20)
        sid=hashlib.sha256(token.encode()).hexdigest();submission=dict(fixture=token,original=dict(generation='123',sha256=hashlib.sha256(b'Unverified fixture').hexdigest()))
        record=dict(submission_id=sid,source=submission,extraction=parse_document(b'Unverified fixture','text/plain'),eligible_for_generation=False)
        for table,key,value,raw in (('submissions','submission_id',sid,canonical(submission)),('extractions','record_id','f'*64,'{"unrelated":true}')):
            query(f'INSERT INTO `{dataset}.{table}` ({key},record_json,recorded_at) VALUES(@id,@raw,CURRENT_TIMESTAMP())',[bigquery.ScalarQueryParameter('id','STRING',value),bigquery.ScalarQueryParameter('raw','STRING',raw)])
        class FixtureStore(source.DocumentStore):
            def preflight(self,kind):
                actual=client.get_table(source.TABLES[kind],retry=None,timeout=20)
                assert actual.expires is not None
                actual.expires=None
                source.validate_table(actual,source.table_metadata(kind))
        original_query=source.DocumentStore.query
        jobs=[]
        def scoped(self,sql,parameters,**kw):
            fixture_sql(sql,dataset)
            if kw.get('job_id'):jobs.append(kw['job_id'])
            return original_query(self,sql,parameters,**kw)
        with patch.object(source,'TABLES',dict(extractions=dataset+'.extractions',turn_links=dataset+'.turn_links')),patch.object(source,'SUBMISSIONS',dataset+'.submissions'),patch.object(source.DocumentStore,'query',scoped):
            store=FixtureStore(client);before=snapshot()
            rid=store.save_extraction(record,submission=submission);committed=snapshot()
            assert committed['submissions']==before['submissions'] and len(committed['extractions'])==2
            assert store.read('extractions',rid)==record
            assert set(before['extractions']).issubset(committed['extractions'])
            passed('commit_exact_source_and_provenance_unrelated_preserved')
            FixtureStore(client).save_extraction(record,submission=submission)
            assert snapshot()==committed;passed('cold_original_job_recovery_exact_snapshot')
            changed=dict(record,case='source-mismatch')
            try:store.save_extraction(changed,submission=dict(submission,original=dict(generation='124')))
            except Exception:pass
            else:raise AssertionError('Changed source accepted')
            failed=client.get_job(jobs[-1],location=source.LOCATION,retry=None,timeout=20)
            assert failed.state=='DONE' and 'DOCUMENT_SOURCE_CHANGED' in str(failed.error_result) and snapshot()==committed
            passed('changed_source_denied_exact_snapshot')
            def rollback(self,sql,parameters,**kw):
                return scoped(self,sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'DOCUMENT_FIXTURE_ROLLBACK'; COMMIT TRANSACTION;"),parameters,**kw)
            with patch.object(source.DocumentStore,'query',rollback):
                try:store.save_extraction(dict(record,case='rollback'),submission=submission)
                except Exception:pass
                else:raise AssertionError('Rollback did not fail')
            failed=client.get_job(jobs[-1],location=source.LOCATION,retry=None,timeout=20)
            assert failed.state=='DONE' and 'DOCUMENT_FIXTURE_ROLLBACK' in str(failed.error_result) and snapshot()==committed
            passed('native_rollback_exact_snapshot')
            real_query=client.query
            def lose(*args,**kwargs):
                real_query(*args,**kwargs)
                raise TimeoutError('Synthetic lost submit response')
            lost=dict(record,case='lost')
            with patch.object(client,'query',side_effect=lose):
                try:store.save_extraction(lost,submission=submission)
                except TimeoutError:pass
                else:raise AssertionError('Fault did not run')
            lost_id=FixtureStore(client).save_extraction(lost,submission=submission)
            assert store.read('extractions',lost_id)==lost
            passed('lost_response_cold_original_job_recovery')
            race=dict(record,case='duplicate');barrier=Barrier(2)
            def contender():
                barrier.wait(timeout=10)
                return FixtureStore(client).save_extraction(race,submission=submission)
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(contender) for _ in range(2)]
                ids=[f.result(timeout=100) for f in futures]
            assert ids[0]==ids[1] and len(snapshot()['extractions'])==4
            passed('same_original_job_competing_admission_one_row')
            report['competition_scope']='Same deterministic job; distinct-job transaction race not established'
        for job_id in sorted(set(jobs)):
            job=client.get_job(job_id,location=source.LOCATION,retry=None,timeout=20)
            report['jobs'].append(dict(job_id=job_id,state=job.state,error=job.error_result))
            assert job.state=='DONE'
        report['status']='PASSED'
    except BaseException as error:
        report.update(status='FAILED',safe_error_type=type(error).__name__,safe_error=str(error)[:1500]);raise
    finally:
        try:
            if created:
                ds=client.get_dataset(dataset,retry=None,timeout=20)
                assert ds.labels=={'purpose':'document_integration','run_id':token} and ds.location.lower()==source.LOCATION and ds.default_table_expiration_ms==3600000
                client.delete_dataset(dataset,delete_contents=True,not_found_ok=False,retry=None,timeout=20)
            try:client.get_dataset(dataset,retry=None,timeout=20)
            except NotFound:report['cleanup']['dataset_absent']=True
            assert report['cleanup'].get('dataset_absent') is True
        finally:save();client.close()
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',action='store_true',required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();run(args.output)


if __name__=='__main__':main()
