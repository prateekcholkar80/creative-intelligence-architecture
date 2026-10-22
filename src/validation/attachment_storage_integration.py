"""Owned native attachment storage fixtures; no serving rows or model calls."""
import argparse,json,hashlib,re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta,timezone
from pathlib import Path
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4

from google.cloud import bigquery,storage
from google.api_core.exceptions import NotFound
from src.application import campaign_attachment_service as source


def relocated_sql(table):
    if not re.fullmatch(re.escape(source.PROJECT)+r'\.ci_attachment_it_[a-f0-9]{12}\.submissions',table):
        raise ValueError('ATTACHMENT_FIXTURE_SCOPE')
    sql=source.SQL.replace('`'+source.TABLE+'`','`'+table+'`')
    if set(re.findall(r'`([^`]+)`',sql))!={table}:raise ValueError('ATTACHMENT_FIXTURE_SQL')
    return sql


def run(output):
    if output.exists():raise ValueError('ATTACHMENT_FIXTURE_REPORT_EXISTS')
    token=uuid4().hex[:12];dataset=source.PROJECT+'.ci_attachment_it_'+token
    table=dataset+'.submissions';bucket_name='ci-attachment-it-1042059373096-'+token
    sql=relocated_sql(table)
    client=bigquery.Client(project=source.PROJECT,location=source.LOCATION)
    gcs=storage.Client(project=source.PROJECT)
    report=dict(status='RUNNING',run_id=token,dataset=dataset,bucket=bucket_name,cases=[],
        cleanup={},model_calls=0,serving_writes=0,release_enabled=False,
        production_sql_sha256=hashlib.sha256(source.SQL.encode()).hexdigest())
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():output.write_text(json.dumps(report,indent=2)+'\n')
    def passed(name,**evidence):report['cases'].append(dict(name=name,status='PASSED',**evidence));save()
    bucket_created=dataset_created=False
    def rows():
        return sorted((dict(r) for r in client.query(f'SELECT * FROM `{table}` ORDER BY submission_id',
            job_config=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=64*1024*1024,job_timeout_ms=60000),
            location=source.LOCATION,retry=None,job_retry=None,timeout=20).result(timeout=60,retry=None,job_retry=None)),key=lambda r:r['submission_id'])
    save()
    try:
        ds=bigquery.Dataset(dataset);ds.location=source.LOCATION
        ds.labels={'purpose':'attachment_integration','run_id':token};ds.default_table_expiration_ms=3600000
        client.create_dataset(ds,exists_ok=False,retry=None,timeout=20);dataset_created=True
        metadata=source.table_metadata().to_api_repr();metadata['tableReference']['datasetId']='ci_attachment_it_'+token
        metadata['tableReference']['tableId']='submissions'
        client.create_table(bigquery.Table.from_api_repr(metadata),exists_ok=False,retry=None,timeout=20)
        bucket=gcs.bucket(bucket_name);bucket.iam_configuration.uniform_bucket_level_access_enabled=True
        bucket.iam_configuration.public_access_prevention='enforced'
        bucket.labels={'purpose':'attachment_integration','run_id':token}
        bucket=gcs.create_bucket(bucket,location=source.LOCATION,retry=None,timeout=30);bucket_created=True
        store=source.AttachmentStore(client=client,storage=gcs,bucket=bucket_name)
        data=b'Synthetic attachment fixture. Not campaign evidence.'
        file=source.inspect_file('fixture.txt',data,'SUPPORTING_DOCUMENT')
        sid=hashlib.sha256((token+'original').encode()).hexdigest()
        original=store.put_original(sid,data,file)
        assert original==store.put_original(sid,data,file)
        passed('gcs_create_and_exact_generation_recovery',generation=original['generation'])
        blob=bucket.blob('campaign-evidence/'+hashlib.sha256((token+'conflict').encode()).hexdigest())
        blob.upload_from_string(b'X'*len(data),content_type='text/plain',if_generation_match=0,retry=None,timeout=30)
        try:store.put_original(blob.name.split('/')[-1],data,file)
        except ValueError as e:assert getattr(e,'code',None)=='ATTACHMENT_OBJECT_CHANGED'
        else:raise AssertionError('Conflicting object accepted')
        passed('gcs_conflicting_original_denied')
        record=dict(status='UNVERIFIED',eligible_for_generation=False,original=original,fixture=token)
        with patch.object(source,'TABLE',table),patch.object(source,'SQL',sql):
            store.record(sid,record);before=rows();assert len(before)==1
            cold=source.AttachmentStore(client=client,storage=gcs,bucket=bucket_name)
            cold.record(sid,record);assert rows()==before
            passed('native_commit_and_cold_original_job_recovery',job_id='evidence_submission_v1_'+sid)
            try:store.record(sid,dict(record,status='APPROVED'))
            except ValueError as e:assert getattr(e,'code',None)=='ATTACHMENT_JOB_MISMATCH'
            else:raise AssertionError('Changed original accepted')
            assert rows()==before;passed('native_changed_original_denied_preserving_rows')
            lost=hashlib.sha256((token+'lost').encode()).hexdigest()
            actual_query=client.query
            def lose(*a,**kw):
                actual_query(*a,**kw)
                raise TimeoutError('Synthetic lost submit response')
            with patch.object(client,'query',side_effect=lose):
                try:store.record(lost,record)
                except TimeoutError:pass
                else:raise AssertionError('Fault did not run')
            store.record(lost,record)
            assert len(rows())==2;passed('native_lost_response_original_recovery',job_id='evidence_submission_v1_'+lost)
            race=hashlib.sha256((token+'race').encode()).hexdigest();barrier=Barrier(2)
            def contender():
                barrier.wait(timeout=10);store.record(race,record)
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(contender) for _ in range(2)]
                for f in futures:f.result(timeout=100)
            snapshot=rows();assert len(snapshot)==3
            passed('concurrent_same_native_job_one_row',job_id='evidence_submission_v1_'+race,
                scope='same deterministic job admission, not competing distinct transaction CAS')
            rollback=hashlib.sha256((token+'rollback').encode()).hexdigest()
            fault=sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'ATTACHMENT_FIXTURE_ROLLBACK';\nCOMMIT TRANSACTION;")
            with patch.object(source,'SQL',fault):
                try:store.record(rollback,record)
                except Exception:pass
                else:raise AssertionError('Rollback fault did not run')
            assert rows()==snapshot
            job=client.get_job('evidence_submission_v1_'+rollback,location=source.LOCATION,retry=None,timeout=20)
            assert job.state=='DONE' and job.error_result is not None and 'ATTACHMENT_FIXTURE_ROLLBACK' in str(job.error_result)
            passed('native_assertion_rollback_exact_snapshot_preserved',job_id=job.job_id)
            history=dict(scope=dict(workspace_id='fixture',conversation_id=token,request_id='history'),
                principal_id='fixture-pilot',review_status='UNVERIFIED',eligible_for_generation=False,
                selection=dict(status='UNVERIFIED'),file=file)
            from src.application.campaign_evidence_gaps import canonical
            history_id=hashlib.sha256(canonical(history).encode()).hexdigest()
            history['original']=original
            store.record(history_id,history)
            summaries=store.summaries(workspace_id='fixture',conversation_id=token,request_id='history',principal_id='fixture-pilot')
            assert len(summaries)==1 and summaries[0]['submission_id']==history_id and summaries[0]['generation_authorized'] is False
            assert store.summaries(workspace_id='fixture',conversation_id=token,request_id='history',principal_id='other')==()
            passed('native_principal_scoped_history_and_reconnect_read')
        report['status']='PASSED'
    except BaseException as error:
        report.update(status='FAILED',safe_error_type=type(error).__name__,safe_error=str(error)[:1500]);raise
    finally:
        try:
            if bucket_created:
                check=gcs.get_bucket(bucket_name,retry=None,timeout=20)
                assert check.labels.get('run_id')==token
                for blob in gcs.list_blobs(check,retry=None,timeout=20):
                    blob.delete(if_generation_match=int(blob.generation),retry=None,timeout=20)
                check.delete(retry=None,timeout=20)
            try:gcs.get_bucket(bucket_name,retry=None,timeout=20)
            except NotFound:report['cleanup']['bucket_absent']=True
            if dataset_created:
                check=client.get_dataset(dataset,retry=None,timeout=20);assert check.labels.get('run_id')==token
                client.delete_dataset(dataset,delete_contents=True,not_found_ok=False,retry=None,timeout=20)
            try:client.get_dataset(dataset,retry=None,timeout=20)
            except NotFound:report['cleanup']['dataset_absent']=True
        finally:save()
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',action='store_true',required=True)
    p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    report=run(args.output);print(json.dumps(dict(status=report['status'],cases=len(report['cases']),cleanup=report['cleanup'])))


if __name__=='__main__':main()
