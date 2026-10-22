"""Explicit owned BigQuery/GCS derivative fixtures; synthetic access, no models."""
import argparse,hashlib,json,time
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone,timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from threading import Barrier
from unittest.mock import Mock,patch
from uuid import uuid4
from google.cloud import bigquery,storage
from google.api_core.exceptions import NotFound

from src.application import campaign_attachment_service as attachments
from src.application.campaign_document_store import DocumentStore
from src.application.campaign_evaluation_uploads import ProtectedEvaluationUploadReader
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_product_image_derivation import ImageDerivationSettings
from src.application.campaign_product_image_service import ProtectedProductImage
from src.application.operation_budget import worker_operation_scope
from src.validation.attachment_storage_integration import relocated_sql
from tests.test_campaign_product_image_service import fixture


def run(output):
    if output.exists():raise ValueError('DERIVATIVE_FIXTURE_REPORT_EXISTS')
    token=uuid4().hex[:12];dataset=attachments.PROJECT+'.ci_attachment_it_'+token
    table=dataset+'.submissions';bucket_name='ci-image-derivation-it-1042059373096-'+token
    sql=relocated_sql(table)
    client=bigquery.Client(project=attachments.PROJECT,location=attachments.LOCATION)
    gcs=storage.Client(project=attachments.PROJECT)
    report=dict(status='RUNNING',run_id=token,dataset=dataset,bucket=bucket_name,cases=[],cleanup={},
        model_calls=0,serving_writes=0,scope='SYNTHETIC_ACCESS_REAL_DERIVATIVE_STORAGE_AND_NATIVE_JOBS',
        production_sql_sha256=hashlib.sha256(attachments.SQL.encode()).hexdigest(),release_allowed=False)
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():output.write_text(json.dumps(report,indent=2)+'\n')
    def passed(name,**evidence):report['cases'].append(dict(name=name,status='PASS',**evidence));save()
    created_dataset=created_bucket=False
    def query(sql,params=()):
        cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,query_parameters=params)
        return tuple(dict(r) for r in client.query(sql,job_config=cfg,location=attachments.LOCATION,
            retry=None,job_retry=None,timeout=20).result(timeout=60,retry=None,job_retry=None))
    def snapshot():
        return sorted(query(f'SELECT submission_id,record_json FROM `{table}`'),key=lambda r:r['submission_id'])
    def call(service,recover_only=False):
        return service.prepare({},workspace_id='workspace-'+token,conversation_id='conversation',
            request_id=service.fixture_request,recover_only=recover_only)
    save()
    try:
        ds=bigquery.Dataset(dataset);ds.location=attachments.LOCATION;ds.default_table_expiration_ms=3600000
        ds.labels={'purpose':'image_derivation_it','run_id':token}
        client.create_dataset(ds,retry=None,timeout=20);created_dataset=True
        metadata=attachments.table_metadata().to_api_repr()
        metadata['tableReference'].update(datasetId='ci_attachment_it_'+token,tableId='submissions')
        native_table=client.create_table(bigquery.Table.from_api_repr(metadata),retry=None,timeout=20)
        # Dataset fixture TTL is inherited at creation. This owned table must
        # match the production no-expiration contract for real store preflight.
        native_table.expires=None
        client.update_table(native_table,['expires'],retry=None,timeout=20)
        bucket=gcs.bucket(bucket_name);bucket.iam_configuration.uniform_bucket_level_access_enabled=True
        bucket.iam_configuration.public_access_prevention='enforced';bucket.labels={'purpose':'image_derivation_it','run_id':token}
        bucket=gcs.create_bucket(bucket,location=attachments.LOCATION,retry=None,timeout=30);created_bucket=True
        store=attachments.AttachmentStore(client=client,storage=gcs,bucket=bucket_name)
        with patch.object(attachments,'TABLE',table),patch.object(attachments,'SQL',sql),worker_operation_scope(time.monotonic()+900):
            store.preflight()
            unrelated='unrelated-'+token;store.record(unrelated,dict(fixture=token,preserve=True))
            def prepare(label):
                base,docs,_,_,inputs=fixture()
                decision=json.loads(base.settings.decision_json);manifest=json.loads(inputs.manifest_json)
                actor,access,context,source,_=docs._source.return_value
                source=deepcopy(source);scope=source['scope'];scope.update(workspace_id='workspace-'+token,request_id='request-'+label)
                sid=sha([token,label,'source']);raw=inputs.images[0][3]
                file=attachments.inspect_file('synthetic-product.png',raw,'PRODUCT_IMAGE')
                original=store.put_original(sid,raw,file);source.update(file=file,original=original)
                store.record(sid,source)
                context=dict(scope=scope,policy_sha256=manifest['context']['policy_sha256'])
                manifest.update(context=context)
                manifest['image_pins'][0].update(generation=original['generation'])
                manifest['sources']=[dict(submission_id=sid,source_record_sha256=sha(source),image_asset_id='asset',original=original)]
                inputs=replace(inputs,manifest_json=canonical(manifest),images=(('asset',original['generation'],'image/png',raw),))
                parent='eval_upload_binding_v1_'+inputs.sha256;store.record(parent,manifest)
                decision.update(source_submission_id=sid,source_binding_id=parent)
                settings=ImageDerivationSettings(canonical(decision),sha(decision),True)
                docs.intake.store=store;docs.store=DocumentStore(client)
                docs._source.return_value=(actor,access,context,source,None)
                service=ProtectedProductImage(docs,settings)
                service.fixture_request='request-'+label
                service.reader=ProtectedEvaluationUploadReader(docs)
                def capture(*a,**kw):
                    now=datetime.now(timezone.utc)
                    return replace(inputs,observed_at=now,checked_at=now,valid_until=now+timedelta(seconds=120))
                service.reader.capture=Mock(side_effect=capture)
                return service
            service=prepare('commit');before=snapshot();receipt=call(service);committed=snapshot()
            assert len(committed)==len(before)+1 and all(r in committed for r in before)
            passed('commit_exact_parent_binding_and_unrelated_preservation',binding_id=receipt['binding_id'],generation=receipt['generation'])
            cold=ProtectedProductImage(service.documents,service.settings);cold.reader=service.reader;cold.fixture_request=service.fixture_request
            with patch.object(store,'put_original',side_effect=AssertionError('Recovery upload')),patch.object(store,'record',side_effect=AssertionError('Recovery write')):
                assert call(cold,True)==receipt
            assert snapshot()==committed;passed('cold_original_job_recovery_without_writes',binding_id=receipt['binding_id'])
            missing=prepare('missing');before=snapshot()
            with patch.object(store,'put_original',side_effect=AssertionError('Recovery upload')),patch.object(store,'record',side_effect=AssertionError('Recovery write')):
                try:call(missing,True)
                except ValueError as error:assert 'ORIGINAL_MISSING' in str(error)
                else:raise AssertionError('Missing original accepted')
            assert snapshot()==before;passed('missing_original_denied_without_mutation')
            lost=prepare('lost');before=snapshot();actual=store.record
            def lose(*a):actual(*a);raise TimeoutError('Synthetic lost committed reply')
            with patch.object(store,'record',side_effect=lose):
                try:call(lost)
                except TimeoutError:pass
                else:raise AssertionError('Fault not reached')
            after=snapshot();assert len(after)==len(before)+1 and all(r in after for r in before)
            recovered=call(lost,True);assert snapshot()==after
            passed('lost_reply_original_outcome_recovered',binding_id=recovered['binding_id'])
            race=prepare('duplicate');before=snapshot();barrier=Barrier(2)
            def contender():
                other=ProtectedProductImage(race.documents,race.settings);other.reader=race.reader;other.fixture_request=race.fixture_request
                barrier.wait(timeout=10);return call(other)
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(copy_context().run,contender) for _ in range(2)]
                receipts=[f.result(timeout=180) for f in futures]
            assert receipts[0]==receipts[1];after=snapshot();assert len(after)==len(before)+1 and all(r in after for r in before)
            passed('duplicate_operation_one_row_same_generation',binding_id=receipts[0]['binding_id'],
                limitation='Same stable native job, not a distinct-job transaction CAS race')
            prior=snapshot();blob=bucket.blob('campaign-evidence/'+receipt['binding_id'])
            blob.upload_from_string(b'changed fixture bytes',content_type='image/png',
                if_generation_match=int(receipt['generation']),retry=None,timeout=30)
            try:call(service,True)
            except ValueError as error:assert 'OBJECT_CHANGED' in str(error)
            else:raise AssertionError('Changed output accepted')
            assert snapshot()==prior;passed('changed_object_generation_denied_without_record_repair')
            assert any(r['submission_id']==unrelated and json.loads(r['record_json'])==dict(fixture=token,preserve=True) for r in snapshot())
        report['status']='PASSED'
    except BaseException as error:
        report.update(status='FAILED',safe_error_type=type(error).__name__);save();raise
    finally:
        try:
            if created_bucket:
                for blob in gcs.list_blobs(bucket_name,versions=True):blob.delete(if_generation_match=int(blob.generation),retry=None,timeout=20)
                gcs.bucket(bucket_name).delete(retry=None,timeout=30)
                try:gcs.get_bucket(bucket_name,retry=None,timeout=20)
                except NotFound:report['cleanup']['bucket_absent']=True
                else:raise AssertionError('Owned bucket remains')
            if created_dataset:
                client.delete_dataset(dataset,delete_contents=True,retry=None,timeout=30)
                try:client.get_dataset(dataset,retry=None,timeout=20)
                except NotFound:report['cleanup']['dataset_absent']=True
                else:raise AssertionError('Owned dataset remains')
        except BaseException as error:
            report.update(status='FAILED',cleanup_error_type=type(error).__name__);raise
        finally:
            save();client.close();gcs.close()
    print(json.dumps(dict(status=report['status'],cases=len(report['cases']),cleanup=report['cleanup'])))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--run',action='store_true');parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not args.run:parser.error('--run explicitly authorizes owned fixture creation and cleanup')
    run(args.output)
