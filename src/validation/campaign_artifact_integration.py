"""Owned native artifact fixtures: synthetic planner/access, actual PNG/GCS/BQ."""
import argparse,json,time
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from google.cloud import bigquery,storage
from google.api_core.exceptions import NotFound

from src.application import campaign_evaluator_store as evaluators
from src.application import campaign_attachment_service as attachments
from src.application.campaign_artifact_execution import DurableCampaignArtifact
from src.application.operation_budget import worker_operation_scope
from tests.test_campaign_artifact_execution import ArtifactExecutionTests


def run(output):
    if output.exists():raise ValueError('ARTIFACT_FIXTURE_REPORT_EXISTS')
    token=uuid4().hex[:12];dataset=evaluators.PROJECT+'.ci_evaluator_it_'+token
    bucket_name='ci-campaign-artifact-it-1042059373096-'+token
    client=bigquery.Client(project=evaluators.PROJECT,location=evaluators.LOCATION)
    gcs=storage.Client(project=evaluators.PROJECT)
    report=dict(status='RUNNING',dataset=dataset,bucket=bucket_name,cases=[],cleanup={},model_calls=0,
        serving_writes=0,scope='SYNTHETIC_AUTHORITY_AND_PLANNER_REAL_RENDERING_AND_NATIVE_STORAGE',qualifying=False,release_allowed=False)
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():output.write_text(json.dumps(report,indent=2)+'\n')
    def passed(name):report['cases'].append(dict(name=name,status='PASS'));save();print(name,flush=True)
    def snapshot():
        sql=' UNION ALL '.join(f"SELECT '{k}' kind,record_id,record_json FROM `{dataset}.{k}` WHERE {evaluators.recent()}" for k in ('intents','outcomes'))
        cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=128*1024**2,job_timeout_ms=60000)
        return sorted((dict(r) for r in client.query(sql,location=evaluators.LOCATION,job_config=cfg,retry=None,job_retry=None,
            timeout=20).result(timeout=60,retry=None,job_retry=None)),key=lambda r:(r['kind'],r['record_id']))
    created_dataset=created_bucket=False;save()
    try:
        ds=bigquery.Dataset(dataset);ds.location=evaluators.LOCATION;ds.default_table_expiration_ms=3600000
        ds.labels={'purpose':'campaign_artifact_it','run_id':token}
        client.create_dataset(ds,retry=None,timeout=20);created_dataset=True
        b=gcs.bucket(bucket_name);b.iam_configuration.uniform_bucket_level_access_enabled=True
        b.iam_configuration.public_access_prevention='enforced';b.labels={'purpose':'campaign_artifact_it','run_id':token}
        gcs.create_bucket(b,location=evaluators.LOCATION,retry=None,timeout=30);created_bucket=True
        with patch.object(evaluators,'TABLES',{k:dataset+'.'+k for k in ('intents','outcomes')}),patch.object(attachments,'TABLE',dataset+'.submissions'):
            for metadata in [evaluators.table_metadata(k) for k in ('intents','outcomes')]+[attachments.table_metadata()]:
                table=client.create_table(metadata,retry=None,timeout=20);table.expires=None
                client.update_table(table,['expires'],retry=None,timeout=20)
            store=evaluators.EvaluatorStore(client)
            objects=attachments.AttachmentStore(client=client,storage=gcs,bucket=bucket_name)
            p,old,_,_,guard,intents,outcomes,_=ArtifactExecutionTests().fixture()
            # Seed a synthetic planner outcome through the actual reservation/job contract.
            planner=json.loads(p.planner.intent_json);o=deepcopy(outcomes[planner['record_id']])
            planner['record_id']=evaluators.sha([token,'planner'])
            plan=replace(p.planner,intent_json=evaluators.canonical(planner))
            intent=json.loads(p.intent_json);intent.update(record_id=evaluators.sha([token,'artifact']),run_id='artifact-fixture-'+token,
                planner_record_id=planner['record_id'],planner_intent_sha256=evaluators.sha(planner))
            p=replace(p,planner=plan,intent_json=evaluators.canonical(intent))
            r=store.reserve(planner,nonce='fixture-planner')
            o.update(record_id=r.record_id,intent_sha256=evaluators.sha(json.loads(r.intent_json)))
            store.publish(r,o)
            unrelated=dict(record_id=evaluators.sha([token,'unrelated']),fixture=token)
            r=store.reserve(unrelated,nonce='unrelated');store.publish(r,dict(record_id=r.record_id,
                intent_sha256=evaluators.sha(json.loads(r.intent_json)),state='UNRELATED'))
            before=snapshot()
            engine=DurableCampaignArtifact(store=store,objects=objects,guard=guard,clock=old.clock,enabled=True)
            with worker_operation_scope(time.monotonic()+900):
                receipt=engine.execute(p)
                assert receipt['measurements']['status']=='PASS'
                assert receipt['measurements']['measurements']['product_region_mse']==0
                after=snapshot();assert all(row in after for row in before) and len(after)==len(before)+4
                passed('actual_render_commit_lineage_pixel_measurement_unrelated_preservation')
                cold=DurableCampaignArtifact(store=evaluators.EvaluatorStore(client),objects=objects,guard=guard,clock=old.clock,enabled=True)
                with patch.object(cold.store,'reserve',side_effect=AssertionError('No recovery writes')),patch.object(cold.store,'publish',side_effect=AssertionError('No recovery writes')),patch.object(objects,'put_original',side_effect=AssertionError('No recovery uploads')):
                    assert cold.execute(p,recover_only=True)==receipt
                assert snapshot()==after;passed('cold_native_original_job_recovery_and_exact_snapshot')
                with patch.object(objects,'put_original',side_effect=AssertionError('No duplicate uploads')):
                    assert engine.execute(p)==receipt
                assert snapshot()==after;passed('duplicate_completed_operation_no_second_artifact')
                original=receipt['original'];blob=gcs.bucket(bucket_name).blob(original['object_name'])
                raw=blob.download_as_bytes(if_generation_match=int(original['generation']),retry=None,timeout=30)
                blob.upload_from_string(raw,content_type='image/png',if_generation_match=int(original['generation']),retry=None,timeout=30)
                try:cold.execute(p,recover_only=True)
                except ValueError as error:assert 'ARTIFACT_GENERATION_CHANGED' in str(error)
                else:raise AssertionError('Changed generation accepted')
                assert snapshot()==after;passed('changed_generation_denied_without_record_repair')
            report['receipt']=receipt;report['status']='PASSED'
    except BaseException as error:
        report['status']='FAILED';report['error_type']=type(error).__name__;report['safe_code']=getattr(error,'code',None);save();raise
    finally:
        try:
            if created_bucket:
                bucket=gcs.bucket(bucket_name)
                for blob in gcs.list_blobs(bucket,versions=True):blob.delete(if_generation_match=blob.generation,retry=None,timeout=20)
                bucket.delete(retry=None,timeout=20)
                try:gcs.get_bucket(bucket_name,retry=None,timeout=20)
                except NotFound:report['cleanup']['bucket_absent']=True
                else:raise AssertionError('Fixture bucket remains')
        finally:
            try:
                if created_dataset:
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
