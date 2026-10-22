"""Read-only fixed-source infrastructure probe; never IAP/user authentication.

Run under the marketing service identity (or explicitly impersonate that same
identity). Native query creator is checked before any source content is read.
No original bytes, extracted text, tokens or credentials are emitted.
"""
import argparse,hashlib,json,os
from pathlib import Path
from uuid import uuid4

from google.cloud import bigquery,storage
from src.application.campaign_document_store import DocumentStore
from src.application.campaign_attachment_service import TABLE,SQL as BINDING_SQL
from src.application.campaign_evidence_gaps import canonical
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require,_digest,_id
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

RUNTIME=f'ci-marketing@{PROJECT}.iam.gserviceaccount.com'
BUCKET='ci-campaign-evidence-1042059373096'
VERSION='upload-corpus-runtime-access-v1'


from src.retrieval.upload_corpus_selection import validate_selection as selection


def verify_access(client,gcs,selected):
    selected=selection(selected);scope=selected['scope']
    _require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,
        'UPLOAD_ACCESS_CLIENT')
    sql='SELECT @probe AS probe'
    cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
        maximum_bytes_billed=32*1024**2,job_timeout_ms=60000,
        query_parameters=[bigquery.ScalarQueryParameter('probe','STRING',VERSION)])
    jid='upload_corpus_access_'+uuid4().hex
    job=client.query(sql,job_id=jid,location=LOCATION,job_config=cfg,retry=None,job_retry=None,timeout=20)
    rows=tuple(dict(r) for r in job.result(timeout=60,max_results=2,retry=None,job_retry=None))
    native=client.get_job(jid,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(native,job_id=jid,sql=sql,config=cfg) and native.state=='DONE'
        and native.error_result is None and native.user_email==RUNTIME and rows==({'probe':VERSION},),
        'UPLOAD_ACCESS_RUNTIME_IDENTITY_UNVERIFIED')
    store=DocumentStore(client);store.preflight('extractions')
    client.get_table(TABLE,retry=None,timeout=20)
    binding=selected['image_binding_id']
    rows=store.query(f'SELECT record_json FROM `{TABLE}` WHERE submission_id=@id LIMIT 2',
        [bigquery.ScalarQueryParameter('id','STRING',binding)])
    _require(len(rows)==1,'UPLOAD_ACCESS_BINDING_MISSING')
    raw=rows[0]['record_json'];manifest=json.loads(raw)
    _require(raw==canonical(manifest) and hashlib.sha256(raw.encode()).hexdigest()==binding.removeprefix('eval_upload_binding_v1_')
        and manifest['claim_approval'] is False and manifest['eligible_for_generation'] is False
        and all(manifest['context']['scope'].get(k)==v for k,v in scope.items())
        and {s['submission_id'] for s in manifest['sources']}==
            {s['submission_id'] for s in selected['sources'] if s['role']=='BOUND_IMAGE'},'UPLOAD_ACCESS_BINDING_SCOPE')
    binding_cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
        maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,query_parameters=[
            bigquery.ScalarQueryParameter('id','STRING',binding),bigquery.ScalarQueryParameter('record','STRING',raw)])
    binding_job_id='evidence_submission_v1_'+binding
    binding_job=client.get_job(binding_job_id,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(binding_job,job_id=binding_job_id,sql=BINDING_SQL,config=binding_cfg)
        and binding_job.state=='DONE' and binding_job.error_result is None,'UPLOAD_ACCESS_BINDING_NATIVE')
    results=[]
    for pinned in selected['sources']:
        sid=pinned['submission_id']
        source=store.submission(sid,workspace_id=scope['workspace_id'],conversation_id=scope['conversation_id'],
            principal_id=manifest['principal_id'])
        f=source['file'];original=source['original']
        _require(all(source['scope'].get(k)==v for k,v in scope.items())
            and original==dict(bucket=BUCKET,object_name='campaign-evidence/'+sid,generation=pinned['generation'])
            and source['review_status']=='UNVERIFIED' and f['content_sha256']==pinned['content_sha256']
            and f['content_type']==pinned['content_type'] and type(f['bytes']) is int and 0<f['bytes']<=5*1024**2,
            'UPLOAD_ACCESS_SOURCE_CHANGED')
        blob=gcs.bucket(BUCKET).blob(original['object_name']);blob.reload(retry=None,timeout=20)
        _require(str(blob.generation)==pinned['generation'] and blob.content_type==f['content_type']
            and blob.size==f['bytes'],'UPLOAD_ACCESS_OBJECT_METADATA')
        content=blob.download_as_bytes(if_generation_match=int(pinned['generation']),retry=None,timeout=30)
        _require(len(content)==f['bytes'] and hashlib.sha256(content).hexdigest()==pinned['content_sha256'],
            'UPLOAD_ACCESS_OBJECT_BYTES')
        blob.reload(retry=None,timeout=20)
        _require(str(blob.generation)==pinned['generation'],'UPLOAD_ACCESS_OBJECT_CHANGED')
        item=dict(submission_id=sid,source_read='PASS',original_job_read='PASS',gcs_generation_bytes='PASS')
        if pinned['role']=='DOCUMENT':
            eid=store.find_extraction(sid,workspace_id=scope['workspace_id'],conversation_id=scope['conversation_id'],
                principal_id=manifest['principal_id'])
            item['extraction_lookup']='PASS';item['extraction_state']='NOT_STORED' if eid is None else 'STORED'
            if eid is not None:
                record=store.read('extractions',eid)
                _require(record['source_record_sha256']==hashlib.sha256(canonical(source).encode()).hexdigest()
                    and record['original']==source['original'] and record['scope']==source['scope']
                    and record['principal_id']==source['principal_id'],'UPLOAD_ACCESS_EXTRACTION_CHANGED')
                store.verify_extraction(eid,record,submission=source)
                item['extraction_original_job']='PASS'
        results.append(item)
    return dict(status='RUNTIME_INFRASTRUCTURE_READS_VERIFIED',version=VERSION,runtime=RUNTIME,
        identity_native_job_id=jid,selection_sha256=hashlib.sha256(canonical(selected).encode()).hexdigest(),
        sources=results,iap_user_authentication='NOT_TESTED',application_authorization='NOT_TESTED',
        corpus_capture='NOT_PERFORMED',raw_content_emitted=False,model_calls=0,data_writes=0,
        iam_changes=0,qualifying=False,release_allowed=False)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--selection',type=Path,required=True);p.add_argument('--output',type=Path)
    p.add_argument('--impersonate',action='store_true');args=p.parse_args()
    if args.output:_require(not args.output.exists(),'UPLOAD_ACCESS_REPORT_EXISTS')
    report=dict(status='FAILED',version=VERSION,runtime=RUNTIME,model_calls=0,data_writes=0,
        iap_user_authentication='NOT_TESTED',application_authorization='NOT_TESTED',release_allowed=False)
    try:
        _require(args.impersonate or os.environ.get('CI_UPLOAD_CORPUS_ACCESS_PROBE')=='true','UPLOAD_ACCESS_EXPLICIT_PROBE_REQUIRED')
        import google.auth
        credentials,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        if args.impersonate:
            from google.auth import impersonated_credentials
            credentials=impersonated_credentials.Credentials(source_credentials=credentials,
                target_principal=RUNTIME,target_scopes=['https://www.googleapis.com/auth/cloud-platform'],lifetime=300)
        selected=selection(json.loads(args.selection.read_text()))
        with bigquery.Client(project=PROJECT,location=LOCATION,credentials=credentials) as client,storage.Client(
                project=PROJECT,credentials=credentials) as gcs:
            report=verify_access(client,gcs,selected)
    except Exception as error:
        report.update(error_type=type(error).__name__,safe_code=getattr(error,'code',None)
            if type(getattr(error,'code',None)) is str else 'UPLOAD_ACCESS_PROBE_FAILED')
    finally:
        if args.output:args.output.write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report))
    if report['status']=='FAILED':raise SystemExit(1)


if __name__=='__main__':main()
