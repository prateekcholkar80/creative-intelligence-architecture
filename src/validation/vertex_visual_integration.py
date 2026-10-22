"""Synthetic provider/authority, actual native visual ledger/GCS; owned cleanup."""
import argparse,json,time
from pathlib import Path
from unittest.mock import Mock,patch
from uuid import uuid4
from google.cloud import bigquery,storage
from google.api_core.exceptions import NotFound
from src.application import campaign_evaluator_store as ledger
from src.application import campaign_attachment_service as attachments
from src.application.campaign_visual_request import VisualRequest,VERSION
from src.application.campaign_visual_execution import DurableVertexVisual
from src.application.operation_budget import worker_operation_scope
from tests.test_campaign_visual_execution import VisualExecutionTests


def run(output):
    if output.exists():raise ValueError('VISUAL_FIXTURE_REPORT_EXISTS')
    token=uuid4().hex[:12];dataset=ledger.PROJECT+'.ci_evaluator_it_'+token
    bucket_name='ci-vertex-visual-it-1042059373096-'+token
    client=bigquery.Client(project=ledger.PROJECT,location=ledger.LOCATION);gcs=storage.Client(project=ledger.PROJECT)
    report=dict(status='RUNNING',dataset=dataset,bucket=bucket_name,cases=[],cleanup={},model_calls=0,serving_writes=0,
        scope='SYNTHETIC_PROVIDER_AND_AUTHORITY_REAL_NATIVE_VISUAL_LEDGER_AND_GCS',qualifying=False,release_allowed=False)
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():output.write_text(json.dumps(report,indent=2)+'\n')
    def passed(name):report['cases'].append(dict(name=name,status='PASS'));save();print(name,flush=True)
    def snapshot():
        sql=' UNION ALL '.join(f"SELECT '{k}' kind,record_id,record_json FROM `{dataset}.{k}` WHERE {ledger.recent()}" for k in ('intents','outcomes'))
        cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=128*1024**2,job_timeout_ms=60000)
        return sorted((dict(r) for r in client.query(sql,location=ledger.LOCATION,job_config=cfg,retry=None,job_retry=None,
            timeout=20).result(timeout=60,retry=None,job_retry=None)),key=lambda r:(r['kind'],r['record_id']))
    save()
    try:
        ds=bigquery.Dataset(dataset);ds.location=ledger.LOCATION;ds.default_table_expiration_ms=3600000
        ds.labels={'purpose':'vertex_visual_it','run_id':token};client.create_dataset(ds,retry=None,timeout=20)
        bucket=gcs.bucket(bucket_name);bucket.iam_configuration.uniform_bucket_level_access_enabled=True
        bucket.iam_configuration.public_access_prevention='enforced';bucket.labels={'purpose':'vertex_visual_it','run_id':token}
        gcs.create_bucket(bucket,location=ledger.LOCATION,retry=None,timeout=30)
        with patch.object(ledger,'TABLES',{k:dataset+'.'+k for k in ('intents','outcomes')}),patch.object(attachments,'TABLE',dataset+'.submissions'):
            for metadata in [ledger.table_metadata(k) for k in ('intents','outcomes')]+[attachments.table_metadata()]:
                table=client.create_table(metadata,retry=None,timeout=20);table.expires=None;client.update_table(table,['expires'],retry=None,timeout=20)
            store=ledger.EvaluatorStore(client);objects=attachments.AttachmentStore(client=client,storage=gcs,bucket=bucket_name)
            request,old,_,_,transport,guard,*_=VisualExecutionTests().fixture()
            def prepared(label):
                value=request.intent;value['run_id']='visual-fixture-'+token+'-'+label
                value['record_id']=ledger.sha([VERSION,value['run_id'],value['case_id'],value['principal_id'],'VERTEX_VISUAL'])
                return VisualRequest(ledger.canonical(value),request.crop_png)
            def engine(t=transport):return DurableVertexVisual(store=store,objects=objects,transport=t,guard=guard,enabled=True)
            unrelated=dict(record_id=ledger.sha([token,'unrelated']),fixture=token)
            r=store.reserve(unrelated,nonce='unrelated');store.publish(r,dict(record_id=r.record_id,
                intent_sha256=ledger.sha(json.loads(r.intent_json)),state='UNRELATED'))
            before=snapshot()
            with worker_operation_scope(time.monotonic()+900):
                first=prepared('commit');e=engine();result=e.execute(first);after=snapshot()
                assert all(row in after for row in before) and len(after)==len(before)+2
                passed('commit_provider_manifest_and_generation_bound_bytes_unrelated_preserved')
                cold=engine(Mock(side_effect=AssertionError('No second provider')))
                with patch.object(objects,'put_original',side_effect=AssertionError('No recovery upload')):
                    assert cold.execute(first,recover_only=True)==result
                    assert cold.execute(first)==result
                assert snapshot()==after;transport.assert_called_once()
                passed('cold_original_job_recovery_and_duplicate_without_provider_or_upload')
                uncertain=Mock(side_effect=TimeoutError());unknown=prepared('unknown');u=engine(uncertain)
                for _ in range(2):
                    try:u.execute(unknown)
                    except ValueError as error:assert 'VISUAL_ORIGINAL_UNAVAILABLE_OR_UNKNOWN' in str(error)
                    else:raise AssertionError('Unknown provider accepted')
                uncertain.assert_called_once();passed('unknown_provider_outcome_never_redispatched')
                lost=prepared('lost-reply');publish=store.publish
                def lose(r,value):publish(r,value);raise TimeoutError('Synthetic committed reply loss')
                with patch.object(store,'publish',side_effect=lose):
                    try:engine().execute(lost)
                    except TimeoutError:pass
                    else:raise AssertionError('Missing reply loss')
                engine(Mock(side_effect=AssertionError('No repeat model'))).execute(lost,recover_only=True)
                passed('lost_committed_outcome_cold_recovery')
                stable=snapshot();original=json.loads(result.receipt_json)['original']
                blob=gcs.bucket(bucket_name).blob(original['object_name'])
                raw=blob.download_as_bytes(if_generation_match=int(original['generation']),retry=None,timeout=30)
                blob.upload_from_string(raw,content_type='image/png',if_generation_match=int(original['generation']),retry=None,timeout=30)
                try:cold.execute(first,recover_only=True)
                except ValueError as error:assert 'VISUAL_OBJECT_GENERATION_CHANGED' in str(error)
                else:raise AssertionError('Changed generation accepted')
                assert snapshot()==stable;passed('changed_generation_denied_without_record_repair')
            report['status']='PASSED'
    except BaseException as error:
        report.update(status='FAILED',error_type=type(error).__name__,safe_code=getattr(error,'code',None));save();raise
    finally:
        try:
            try:bucket=gcs.get_bucket(bucket_name,retry=None,timeout=20)
            except NotFound:report['cleanup']['bucket_absent']=True
            else:
                assert bucket.labels.get('run_id')==token and bucket.labels.get('purpose')=='vertex_visual_it'
                for blob in gcs.list_blobs(bucket,versions=True):blob.delete(if_generation_match=blob.generation,retry=None,timeout=20)
                bucket.delete(retry=None,timeout=20)
                try:gcs.get_bucket(bucket_name,retry=None,timeout=20)
                except NotFound:report['cleanup']['bucket_absent']=True
                else:raise AssertionError('Fixture bucket remains')
        finally:
            try:
                try:ds=client.get_dataset(dataset,retry=None,timeout=20)
                except NotFound:report['cleanup']['dataset_absent']=True
                else:
                    assert ds.labels.get('run_id')==token and ds.labels.get('purpose')=='vertex_visual_it'
                    client.delete_dataset(dataset,delete_contents=True,retry=None,timeout=30)
                    try:client.get_dataset(dataset,retry=None,timeout=20)
                    except NotFound:report['cleanup']['dataset_absent']=True
                    else:raise AssertionError('Fixture dataset remains')
            finally:save();client.close();gcs.close()
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--run',action='store_true');parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not args.run:raise SystemExit('Explicit --run required')
    print(json.dumps(run(args.output)))
