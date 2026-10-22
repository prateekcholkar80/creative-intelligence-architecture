"""Protected original-image references for evaluation, never claim/serving authority."""
from dataclasses import dataclass
from datetime import datetime,timezone,timedelta
import hashlib,json
from src.application.campaign_document_service import ProtectedDocumentService
from src.application.campaign_attachment_service import inspect_file
from src.application.campaign_evidence_gaps import canonical
from src.application.workflow_contract import _require,_digest

VERSION='campaign-evaluation-upload-inputs-v1'
MAX_TOTAL_BYTES=20*1024**2


@dataclass(frozen=True,slots=True)
class EvaluationUploadInputs:
    manifest_json:str
    images:tuple
    observed_at:datetime
    checked_at:datetime
    valid_until:datetime

    @property
    def sha256(self):return hashlib.sha256(self.manifest_json.encode()).hexdigest()


class ProtectedEvaluationUploadReader:
    def __init__(self,documents,*,clock=lambda:datetime.now(timezone.utc)):
        _require(type(documents) is ProtectedDocumentService,'EVALUATION_UPLOAD_SERVICE_REQUIRED')
        self.documents,self.clock=documents,clock

    def verify_binding(self,binding_id,inputs):
        """Read-only original-job correlation; never replay a binding write."""
        from google.cloud import bigquery
        from src.application.campaign_attachment_service import TABLE,SQL,LOCATION
        from src.application.sharing_jobs import matches_sharing_job
        _require(type(inputs) is EvaluationUploadInputs
            and binding_id=='eval_upload_binding_v1_'+inputs.sha256,'EVALUATION_UPLOAD_BINDING_CHANGED')
        self.documents.intake.store.preflight()
        rows=self.documents.store.query(f'SELECT record_json FROM `{TABLE}` WHERE submission_id=@id LIMIT 2',
            [bigquery.ScalarQueryParameter('id','STRING',binding_id)])
        _require(len(rows)==1 and rows[0]==dict(record_json=inputs.manifest_json),
            'EVALUATION_UPLOAD_BINDING_CHANGED')
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=128*1024*1024,job_timeout_ms=60000,query_parameters=[
                bigquery.ScalarQueryParameter('id','STRING',binding_id),
                bigquery.ScalarQueryParameter('record','STRING',inputs.manifest_json)])
        job_id='evidence_submission_v1_'+binding_id
        job=self.documents.store.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=job_id,sql=SQL,config=cfg)
            and job.state=='DONE' and job.error_result is None,'EVALUATION_UPLOAD_BINDING_JOB_UNVERIFIED')
        return job_id

    def capture(self,headers,*,workspace_id,conversation_id,request_id,submission_ids):
        _require(type(submission_ids) is tuple and 0<len(submission_ids)<=16
            and len(set(submission_ids))==len(submission_ids),'EVALUATION_UPLOAD_SELECTION')
        for sid in submission_ids:_digest(sid)
        started=self.clock();initial=[];sources=[];images=[];seen={};total=0
        for sid in submission_ids:
            _require(self.clock()<started+timedelta(seconds=120),'EVALUATION_UPLOAD_CAPTURE_EXPIRED')
            capture=self.documents._source(headers,workspace_id,conversation_id,sid)
            actor,access,context,source,blob=capture
            _require(source['scope']['request_id']==request_id and source['review_status']=='UNVERIFIED'
                and source['eligible_for_generation'] is False,'EVALUATION_UPLOAD_REQUEST_SCOPE')
            if initial:
                prior=initial[0][1]
                _require(actor.principal==prior[0].principal and access==prior[1] and context==prior[2],
                    'EVALUATION_UPLOAD_CONTEXT_CHANGED')
            f=source['file'];o=source['original']
            _require(f['content_type'] in ('image/png','image/jpeg','image/webp'),'EVALUATION_UPLOAD_IMAGE_REQUIRED')
            raw=blob.download_as_bytes(if_generation_match=int(o['generation']),retry=None,timeout=30)
            _require(type(raw) is bytes and len(raw)==f['bytes']
                and hashlib.sha256(raw).hexdigest()==f['content_sha256'],'EVALUATION_UPLOAD_BYTES_CHANGED')
            _require(inspect_file(f['filename'],raw,source['selection']['category'])==f,'EVALUATION_UPLOAD_BYTES_CHANGED')
            key=(f['content_sha256'],f['content_type'])
            if key not in seen:
                asset='upload-'+sid;seen[key]=asset;total+=len(raw)
                _require(total<=MAX_TOTAL_BYTES,'EVALUATION_UPLOAD_INPUT_TOO_LARGE')
                images.append((asset,o['generation'],f['content_type'],raw))
            sources.append(dict(submission_id=sid,source_record_sha256=hashlib.sha256(canonical(source).encode()).hexdigest(),
                original_native_job_id='evidence_submission_v1_'+sid,original=o,file=f,image_asset_id=seen[key]))
            initial.append((sid,capture))
        for sid,capture in initial:
            _require(self.clock()<started+timedelta(seconds=120),'EVALUATION_UPLOAD_CAPTURE_EXPIRED')
            self.documents._fresh(headers,workspace_id,conversation_id,sid,capture)
        ended=self.clock();_require(started<=ended<started+timedelta(seconds=120),'EVALUATION_UPLOAD_CAPTURE_EXPIRED')
        context=initial[0][1][2]
        # Stable input identity excludes transient read times, so fresh guards can
        # compare content without reminting an uncertain original evaluation job.
        value=dict(version=VERSION,request_id=request_id,context=context,sources=sources,
            binding_scope=dict(workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id),
            principal_id=initial[0][1][0].principal.principal_id,
            image_pins=[dict(asset_id=a,generation=g,mime_type=m,byte_count=len(b),sha256=hashlib.sha256(b).hexdigest()) for a,g,m,b in images],
            use='UNVERIFIED_VISUAL_REFERENCE_FOR_EVALUATION_ONLY',claim_approval=False,
            eligible_for_generation=False,qualifying=False,release_allowed=False)
        return EvaluationUploadInputs(canonical(value),tuple(images),started,ended,started+timedelta(seconds=120))

    def publish_binding(self,headers,**scope):
        """Persist a checked snapshot only; dispatch must freshly recapture/match it."""
        inputs=self.capture(headers,**scope);manifest=json.loads(inputs.manifest_json)
        args={k:scope[k] for k in ('workspace_id','conversation_id')}
        actor,access=self.documents.intake._actor(headers,**args)
        _require(actor.principal.principal_id==manifest['principal_id'] and self.clock()<inputs.valid_until,
            'EVALUATION_UPLOAD_AUTHORITY_CHANGED')
        binding_id='eval_upload_binding_v1_'+inputs.sha256
        self.documents.intake.store.record(binding_id,manifest)
        after,current=self.documents.intake._actor(headers,**args)
        _require(after.principal==actor.principal and current==access and self.clock()<inputs.valid_until,
            'EVALUATION_UPLOAD_AUTHORITY_CHANGED')
        return dict(binding_id=binding_id,input_manifest_sha256=inputs.sha256,
            source_count=len(manifest['sources']),unique_image_count=len(inputs.images),
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
