"""Protected unverified evidence intake. Never supplies approved model context.

Original bytes live in GCS; canonical submission records live in BigQuery. No
automatic parsing, document instruction execution, claim approval or activation.
Server composition supplies a fresh authoritative gap reader and cloud clients.
"""
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import hashlib
from io import BytesIO
import json
import logging
from time import perf_counter
from uuid import uuid4
from functools import wraps
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import PurePath

from google.api_core.exceptions import NotFound, Conflict, PreconditionFailed
from google.cloud import bigquery
from PIL import Image

from src.application.access_policy import Action, AccessDecision
from src.application.campaign_attachment_contract import attachment_requests, collection_choice
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_quality_product_reader import _access
from src.application.identity import IapIdentityAdapter, ensure_fresh
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require, _id
from src.generators.sales_generator import PROJECT, LOCATION
from src.ingestion.setup_workflow_tables import validate_table

MAX_BYTES = 5 * 1024 * 1024
_SAVE_TRACE=ContextVar('attachment_save_trace',default=None)
_LOG=logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)
if not _LOG.handlers:_LOG.addHandler(logging.StreamHandler())
_LOG.propagate=False


@contextmanager
def _save_stage(stage):
    started=perf_counter();succeeded=False
    try:
        yield
        succeeded=True
    finally:
        _LOG.info(json.dumps(dict(event='attachment_save_timing',trace_id=_SAVE_TRACE.get(),
            stage=stage,duration_ms=round((perf_counter()-started)*1000,3),succeeded=succeeded)))


def _measure_save(method):
    @wraps(method)
    def measured(*args,**kwargs):
        token=_SAVE_TRACE.set(uuid4().hex)
        try:
            with _save_stage('total'):return method(*args,**kwargs)
        finally:_SAVE_TRACE.reset(token)
    return measured


TABLE = f'{PROJECT}.ci_application.campaign_evidence_submissions'
EXTENSIONS = {'png':'image/png', 'jpg':'image/jpeg', 'jpeg':'image/jpeg', 'webp':'image/webp',
              'pdf':'application/pdf', 'txt':'text/plain', 'json':'application/json', 'csv':'text/csv'}
SQL = f'''BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM `{TABLE}` WHERE submission_id=@id) <= 1 AS 'ATTACHMENT_DUPLICATE';
ASSERT NOT EXISTS(SELECT 1 FROM `{TABLE}` WHERE submission_id=@id AND record_json!=@record)
 AS 'ATTACHMENT_CONFLICT';
INSERT INTO `{TABLE}` (submission_id,record_json,recorded_at)
 SELECT @id,@record,CURRENT_TIMESTAMP() FROM UNNEST([1]) WHERE NOT EXISTS
 (SELECT 1 FROM `{TABLE}` WHERE submission_id=@id);
COMMIT TRANSACTION;
SELECT record_json FROM `{TABLE}` WHERE submission_id=@id;'''


def table_metadata():
    table=bigquery.Table(TABLE,schema=[bigquery.SchemaField('submission_id','STRING',mode='REQUIRED'),
        bigquery.SchemaField('record_json','STRING',mode='REQUIRED'),
        bigquery.SchemaField('recorded_at','TIMESTAMP',mode='REQUIRED')])
    table.clustering_fields=['submission_id']
    return table


def inspect_file(filename, content, category):
    _require(type(filename) is str and 0<len(filename)<=200 and '/' not in filename
        and '\\' not in filename and all(ord(c)>=32 and ord(c)!=127 for c in filename),
        'ATTACHMENT_FILENAME_INVALID')
    _require(type(content) is bytes and 0<len(content)<=MAX_BYTES,'ATTACHMENT_FILE_SIZE')
    ext=PurePath(filename).suffix.lower().lstrip('.')
    _require(ext in EXTENSIONS,'ATTACHMENT_FORMAT_UNSUPPORTED')
    mime=EXTENSIONS[ext]
    if mime.startswith('image/'):
        with Image.open(BytesIO(content)) as image:
            _require(image.format=={'image/png':'PNG','image/jpeg':'JPEG','image/webp':'WEBP'}[mime]
                and image.width*image.height<=20_000_000 and getattr(image,'n_frames',1)==1,
                'ATTACHMENT_IMAGE_INVALID')
            image.verify()
    elif mime=='application/pdf':
        _require(content.startswith(b'%PDF-') and b'%%EOF' in content[-2048:], 'ATTACHMENT_PDF_INVALID')
    else:
        text=content.decode('utf-8',errors='strict')
        _require('\x00' not in text,'ATTACHMENT_TEXT_INVALID')
        if mime=='application/json': json.loads(text)
    _require(category!='PRODUCT_IMAGE' or mime.startswith('image/'),'ATTACHMENT_IMAGE_REQUIRED')
    return dict(filename=filename,content_type=mime,bytes=len(content),
        content_sha256=hashlib.sha256(content).hexdigest())


@dataclass(frozen=True,slots=True)
class AttachmentReceipt:
    submission_id: str
    status: str
    choice: str
    generation_authorized: bool = False


def prepare_submission(request,principal_id,*,choice,category,filename,content):
    """Same canonical per-file record for single saves and durable batches."""
    selected=collection_choice(request,choice,category=category)
    file=inspect_file(filename,content,category) if choice=='PROVIDE_EVIDENCE' else None
    if file is None:_require(filename is None and content is None,'ATTACHMENT_OMISSION_HAS_FILE')
    collection=json.loads(request.canonical_json);collection.pop('plan_sha256')
    record=dict(version='campaign-evidence-submission-v1',scope=collection['scope'],collection_request=collection,
        principal_id=principal_id,requirement_id=collection['requirement_id'],selection=selected,file=file,
        review_status='UNVERIFIED',eligible_for_generation=False,
        extraction_status='NOT_PARSED' if file else 'NOT_APPLICABLE')
    return hashlib.sha256(canonical(record).encode()).hexdigest(),record


class AttachmentStore:
    def __init__(self, *, client, storage, bucket):
        _require(client.project==PROJECT and client.location==LOCATION
            and client.default_query_job_config is None,'ATTACHMENT_CLIENT_SCOPE')
        _require(type(bucket) is str and 3<=len(bucket)<=63
            and all(c.islower() or c.isdigit() or c in '-.' for c in bucket),'ATTACHMENT_BUCKET_INVALID')
        self.client,self.storage,self.bucket=client,storage,bucket

    def preflight(self):
        validate_table(self.client.get_table(TABLE,retry=None,timeout=20),table_metadata())
        bucket=self.storage.get_bucket(self.bucket,retry=None,timeout=20)
        _require(bucket.location.lower()==LOCATION,'ATTACHMENT_BUCKET_LOCATION')
        _require(bucket.iam_configuration.uniform_bucket_level_access_enabled is True
            and bucket.iam_configuration.public_access_prevention=='enforced',
            'ATTACHMENT_PRIVATE_BUCKET_REQUIRED')

    def put_original(self, submission_id, content, metadata):
        blob=self.storage.bucket(self.bucket).blob('campaign-evidence/'+submission_id)
        try:
            blob.upload_from_string(content,content_type=metadata['content_type'],
                if_generation_match=0,retry=None,timeout=30,checksum='crc32c')
        except (Conflict,PreconditionFailed):
            pass # Recover only this exact content-addressed object.
        blob.reload(retry=None,timeout=20)
        _require(blob.size==len(content) and blob.content_type==metadata['content_type']
            and blob.generation is not None,'ATTACHMENT_OBJECT_CHANGED')
        actual=blob.download_as_bytes(if_generation_match=int(blob.generation),retry=None,timeout=30)
        _require(hashlib.sha256(actual).hexdigest()==metadata['content_sha256'], 'ATTACHMENT_OBJECT_CHANGED')
        return dict(bucket=self.bucket,object_name=blob.name,generation=str(blob.generation))

    def record(self, submission_id, record):
        raw=canonical(record)
        config=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=128*1024*1024,job_timeout_ms=60000,
            query_parameters=[bigquery.ScalarQueryParameter('id','STRING',submission_id),
                bigquery.ScalarQueryParameter('record','STRING',raw)])
        native_id='evidence_submission_v1_'+submission_id
        try:
            job=self.client.get_job(native_id,location=LOCATION,retry=None,timeout=20)
        except NotFound:
            try:
                job=self.client.query(SQL,job_id=native_id,job_config=config,location=LOCATION,
                    retry=None,job_retry=None,timeout=20)
            except Conflict:
                job=self.client.get_job(native_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=native_id,sql=SQL,config=config),'ATTACHMENT_JOB_MISMATCH')
        rows=list(job.result(timeout=60,max_results=2,retry=None,job_retry=None))
        native=self.client.get_job(native_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(native,job_id=native_id,sql=SQL,config=config)
            and native.state=='DONE' and native.error_result is None
            and len(rows)==1 and dict(rows[0])==dict(record_json=raw),'ATTACHMENT_RECEIPT_UNAVAILABLE')

    def summaries(self,*,workspace_id,conversation_id,request_id,principal_id):
        sql=f'''SELECT submission_id,record_json,recorded_at FROM `{TABLE}`
WHERE JSON_VALUE(record_json,'$.scope.workspace_id')=@workspace_id
AND JSON_VALUE(record_json,'$.scope.conversation_id')=@conversation_id
AND JSON_VALUE(record_json,'$.scope.request_id')=@request_id
AND JSON_VALUE(record_json,'$.principal_id')=@principal_id
ORDER BY recorded_at DESC,submission_id DESC LIMIT 20'''
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=64*1024*1024,job_timeout_ms=60000,query_parameters=[
                bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in dict(workspace_id=workspace_id,
                    conversation_id=conversation_id,request_id=request_id,principal_id=principal_id).items()])
        from uuid import uuid4
        job_id='evidence_history_read_'+uuid4().hex
        job=self.client.query(sql,job_id=job_id,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
        rows=list(job.result(timeout=60,max_results=20,retry=None,job_retry=None))
        native=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(native,job_id=job_id,sql=sql,config=cfg)
            and native.state=='DONE' and native.error_result is None,'ATTACHMENT_HISTORY_UNVERIFIED')
        output=[]
        for raw in rows:
            text=raw['record_json'];_require(type(text) is str and len(text.encode())<=65536,'ATTACHMENT_HISTORY_SIZE')
            record=json.loads(text);base=dict(record);base.pop('original',None)
            _require(hashlib.sha256(canonical(base).encode()).hexdigest()==raw['submission_id']
                and record['eligible_for_generation'] is False and record['review_status']=='UNVERIFIED'
                and record['principal_id']==principal_id
                and all(record['scope'][k]==v for k,v in [('workspace_id',workspace_id),('conversation_id',conversation_id),('request_id',request_id)]),
                'ATTACHMENT_HISTORY_SCOPE')
            output.append(dict(submission_id=raw['submission_id'],filename=record['file']['filename'] if record['file'] else None,
                status=record['selection']['status'],created_at=raw['recorded_at'].isoformat(),
                generation_authorized=False))
        return tuple(output)


class ProtectedAttachmentService:
    def __init__(self, *, verifier, access_reader, plan_reader, store, enabled=False,
                 clock=lambda:datetime.now(timezone.utc)):
        _require(isinstance(verifier,IapIdentityAdapter) and callable(plan_reader)
            and type(enabled) is bool,'ATTACHMENT_COMPOSITION_INVALID')
        self.verifier,self.access_reader,self.plan_reader,self.store=verifier,access_reader,plan_reader,store
        self.enabled,self.clock=enabled,clock

    def _actor(self,headers,workspace_id,conversation_id):
        _require(self.enabled,'ATTACHMENT_INTAKE_DISABLED')
        for key in (workspace_id,conversation_id): _id(key)
        identity=self.verifier.authenticate(headers);ensure_fresh(identity)
        read=_access(self.access_reader,identity,workspace_id,conversation_id)
        write=self.access_reader.authorize(identity,Action.REFINE_DRAFT,
            workspace_id=workspace_id,conversation_id=conversation_id)
        _require(type(write) is AccessDecision and write.allowed
            and write.action is Action.REFINE_DRAFT and write.resource_id==conversation_id
            and write.workspace_id==workspace_id and write.principal_id==identity.principal.principal_id
            and (write.membership_revision,write.resource_revision)==(read.membership_revision,read.resource_revision),
            'ATTACHMENT_WRITE_DENIED')
        return identity,read

    def verify_intake(self,headers,*,workspace_id,conversation_id):
        """Read-only deployed check; no intent, object write or approval needed."""
        identity,access=self._actor(headers,workspace_id,conversation_id)
        self.store.preflight()
        after,current=self._actor(headers,workspace_id,conversation_id)
        _require(after.principal==identity.principal and current==access,'ATTACHMENT_AUTHORITY_CHANGED')
        return True

    def _current(self,headers,workspace_id,conversation_id,request_id):
        _id(request_id)
        identity,read=self._actor(headers,workspace_id,conversation_id)
        plan=self.plan_reader(headers,workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id)
        data=json.loads(plan.canonical_json);scope=data['scope']
        _require(all(scope[k]==v for k,v in [('workspace_id',workspace_id),('conversation_id',conversation_id),('request_id',request_id)]),
            'ATTACHMENT_REQUEST_SCOPE')
        now=self.clock();checked=datetime.fromisoformat(data['checked_at'])
        _require(checked<=now<checked+timedelta(seconds=60),'ATTACHMENT_PLAN_EXPIRED')
        return identity,read,attachment_requests(plan)

    def requests(self,headers,**scope):
        _,_,requests=self._current(headers,**scope)
        return requests

    def saved_requests(self,headers,*,workspace_id,conversation_id):
        identity,access=self._actor(headers,workspace_id,conversation_id)
        result=self.plan_reader.saved_requests(headers,workspace_id=workspace_id,conversation_id=conversation_id)
        after,current=self._actor(headers,workspace_id,conversation_id)
        _require(after.principal==identity.principal and current==access,'ATTACHMENT_AUTHORITY_CHANGED')
        return result

    def summaries(self,headers,*,workspace_id,conversation_id,request_id):
        _id(request_id)
        identity,access=self._actor(headers,workspace_id,conversation_id)
        result=self.store.summaries(workspace_id=workspace_id,conversation_id=conversation_id,
            request_id=request_id,principal_id=identity.principal.principal_id)
        after,current=self._actor(headers,workspace_id,conversation_id)
        _require(after.principal==identity.principal and current==access,'ATTACHMENT_AUTHORITY_CHANGED')
        return result

    @_measure_save
    def submit(self,headers,*,workspace_id,conversation_id,request_id,requirement_id,
               request_sha256,choice,category=None,filename=None,content=None):
        args=dict(workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id)
        with _save_stage('initial_authority_and_plan'):
            identity,access,requests=self._current(headers,**args)
        matches=[r for r in requests if json.loads(r.canonical_json)['requirement_id']==requirement_id]
        _require(len(matches)==1 and matches[0].sha256==request_sha256,'ATTACHMENT_REQUEST_CHANGED')
        request=matches[0]
        submission_id,record=prepare_submission(request,identity.principal.principal_id,
            choice=choice,category=category,filename=filename,content=content)
        selected=record['selection'];file=record['file']
        with _save_stage('storage_preflight'):self.store.preflight()
        if file:
            with _save_stage('original_upload_and_readback'):
                record['original']=self.store.put_original(submission_id,content,file)
        # Never publish metadata after a changed scope or revoked access.
        with _save_stage('precommit_authority_and_plan'):
            after,after_access,current=self._current(headers,**args)
        _require(after.principal==identity.principal and after_access==access
            and any(r.sha256==request.sha256 for r in current),'ATTACHMENT_AUTHORITY_CHANGED')
        with _save_stage('durable_record_and_job_verification'):
            self.store.record(submission_id,record)
        with _save_stage('postcommit_authority_and_plan'):
            final,final_access,current=self._current(headers,**args)
        _require(final.principal==identity.principal and final_access==access
            and any(r.sha256==request.sha256 for r in current),'ATTACHMENT_AUTHORITY_CHANGED')
        return AttachmentReceipt(submission_id,selected['status'],choice)
