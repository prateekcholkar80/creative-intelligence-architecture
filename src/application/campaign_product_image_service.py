"""Protected immutable derivative storage and original-record recovery; no approval."""
from dataclasses import dataclass
from datetime import datetime,timezone
from threading import RLock
import hashlib,json
from google.cloud import bigquery

from src.application import campaign_attachment_service as attachments
from src.application.campaign_document_service import ProtectedDocumentService
from src.application.campaign_evaluation_uploads import ProtectedEvaluationUploadReader
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_product_image_derivation import ImageDerivationSettings,prepare_derivation
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require


@dataclass(frozen=True,slots=True)
class DerivedImageCapture:
    binding_id:str
    record_json:str
    raw:bytes
    valid_until:datetime


class ProtectedProductImage:
    def __init__(self,documents,settings,*,clock=lambda:datetime.now(timezone.utc)):
        require_owned_worker()
        _require(type(documents) is ProtectedDocumentService and type(settings) is ImageDerivationSettings,
            'PRODUCT_IMAGE_COMPOSITION')
        self.documents,self.settings,self.clock=documents,settings,clock
        self.reader=ProtectedEvaluationUploadReader(documents,clock=clock);self._lock=RLock()

    def _read(self,binding_id):
        rows=self.documents.store.query(f'SELECT record_json FROM `{attachments.TABLE}` WHERE submission_id=@id LIMIT 2',
            [bigquery.ScalarQueryParameter('id','STRING',binding_id)])
        _require(len(rows)<=1,'PRODUCT_IMAGE_BINDING_DUPLICATE')
        if not rows:return None
        raw=rows[0]['record_json'];_require(type(raw) is str and len(raw.encode())<=65536,'PRODUCT_IMAGE_RECORD_SIZE')
        value=json.loads(raw);_require(canonical(value)==raw,'PRODUCT_IMAGE_RECORD_CHANGED');return value

    def _native(self,binding_id,record):
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,query_parameters=[
                bigquery.ScalarQueryParameter('id','STRING',binding_id),
                bigquery.ScalarQueryParameter('record','STRING',canonical(record))])
        jid='evidence_submission_v1_'+binding_id
        job=self.documents.store.client.get_job(jid,location=attachments.LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=jid,sql=attachments.SQL,config=cfg)
            and job.state=='DONE' and job.error_result is None,'PRODUCT_IMAGE_ORIGINAL_JOB_UNVERIFIED')

    def _fresh(self,headers,scope,inputs,actor,access):
        check_operation();_require(self.clock()<inputs.valid_until,'PRODUCT_IMAGE_SOURCE_EXPIRED')
        manifest=json.loads(inputs.manifest_json);parent=manifest['sources'][0]
        current=self.documents._source(headers,scope['workspace_id'],scope['conversation_id'],parent['submission_id'])
        _require(current[0].principal==actor.principal and current[1]==access
            and current[2]==manifest['context'] and sha(current[3])==parent['source_record_sha256'],
            'PRODUCT_IMAGE_SOURCE_OR_ACCESS_CHANGED')
        self.reader.verify_binding(json.loads(self.settings.decision_json)['source_binding_id'],inputs)

    def _saved_bytes(self,binding_id,record,expected):
        original=record['derived_original'];store=self.documents.intake.store
        _require(original['bucket']==store.bucket and original['object_name']=='campaign-evidence/'+binding_id,
            'PRODUCT_IMAGE_OBJECT_SCOPE')
        blob=store.storage.bucket(original['bucket']).blob(original['object_name'])
        blob.reload(retry=None,timeout=20)
        _require(str(blob.generation)==original['generation'] and blob.size==len(expected)
            and blob.content_type=='image/png','PRODUCT_IMAGE_OBJECT_CHANGED')
        raw=blob.download_as_bytes(if_generation_match=int(original['generation']),retry=None,timeout=30)
        _require(raw==expected and hashlib.sha256(raw).hexdigest()==record['derived_file']['content_sha256'],
            'PRODUCT_IMAGE_BYTES_CHANGED')
        return raw

    def _execute(self,headers,scope,*,recover_only):
        require_owned_worker();check_operation()
        _require(type(recover_only) is bool and (self.settings.enabled or recover_only),'PRODUCT_IMAGE_DERIVATION_DISABLED')
        with self._lock:
            decision=json.loads(self.settings.decision_json);docs=self.documents
            actor,access=docs.intake._actor(headers,scope['workspace_id'],scope['conversation_id'])
            inputs=self.reader.capture(headers,**scope,submission_ids=(decision['source_submission_id'],))
            self.reader.verify_binding(decision['source_binding_id'],inputs)
            _require(json.loads(inputs.manifest_json)['principal_id']==actor.principal.principal_id,'PRODUCT_IMAGE_OWNER_CHANGED')
            binding_id,draft,png=prepare_derivation(inputs,self.settings)
            self._fresh(headers,scope,inputs,actor,access)
            existing=self._read(binding_id)
            if existing is None:
                _require(not recover_only,'PRODUCT_IMAGE_ORIGINAL_MISSING')
                original=docs.intake.store.put_original(binding_id,png,draft['derived_file'])
                record=dict(draft,derived_original=original)
                self._fresh(headers,scope,inputs,actor,access)
                docs.intake.store.record(binding_id,record)
                existing=self._read(binding_id)
                _require(existing==record,'PRODUCT_IMAGE_BINDING_NOT_VERIFIED')
            base=dict(existing);base.pop('derived_original',None)
            _require(base==draft,'PRODUCT_IMAGE_BINDING_CHANGED')
            self._native(binding_id,existing)
            raw=self._saved_bytes(binding_id,existing,png)
            self._fresh(headers,scope,inputs,actor,access)
            return DerivedImageCapture(binding_id,canonical(existing),raw,inputs.valid_until)

    def prepare(self,headers,*,workspace_id,conversation_id,request_id,recover_only=False):
        capture=self._execute(headers,dict(workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id),
            recover_only=recover_only)
        record=json.loads(capture.record_json)
        return dict(binding_id=capture.binding_id,record_sha256=sha(record),parent_binding_id=record['parent_binding_id'],
            content_sha256=record['derived_file']['content_sha256'],generation=record['derived_original']['generation'],
            width=record['width'],height=record['height'],claim_approval=False,generation_authorized=False,
            qualifying=False,release_allowed=False)

    def capture_bound(self,headers,*,workspace_id,conversation_id,request_id):
        return self._execute(headers,dict(workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id),
            recover_only=True)
