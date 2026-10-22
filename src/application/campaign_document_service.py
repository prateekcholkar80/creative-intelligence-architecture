"""Protected extraction and fresh source-eligibility checks; no text release to RAG."""
import hashlib,json
from dataclasses import dataclass
from src.application.campaign_document_extraction import extract_document,VERSION
from src.application.campaign_attachment_service import inspect_file,MAX_BYTES
from src.application.campaign_evidence_gaps import canonical
from src.application.workflow_contract import _require,_digest


@dataclass(frozen=True,slots=True)
class DocumentReceipt:
    extraction_id:str
    submission_id:str
    status:str
    text_bytes:int
    segment_count:int
    eligibility_decision:str
    reasons:tuple[str,...]
    generation_authorized:bool=False


class ProtectedDocumentService:
    def __init__(self,*,intake,store,extractor=extract_document):
        self.intake,self.store,self.extractor=intake,store,extractor

    def _capture(self,headers,workspace_id,conversation_id,request_id):
        actor,access=self.intake._actor(headers,workspace_id,conversation_id)
        read=self.intake.plan_reader._optional_reference(actor,workspace_id,conversation_id,request_id)
        value=json.loads(read.canonical_json)
        return actor,access,dict(scope=value['scope'],policy_sha256=value['policy_sha256'])

    def _source(self,headers,workspace_id,conversation_id,submission_id):
        _digest(submission_id)
        actor,access=self.intake._actor(headers,workspace_id,conversation_id)
        source=self.store.submission(submission_id,workspace_id=workspace_id,
            conversation_id=conversation_id,principal_id=actor.principal.principal_id)
        _require(source['selection']['choice']=='PROVIDE_EVIDENCE' and source['file'] is not None
            and source['review_status']=='UNVERIFIED','DOCUMENT_ORIGINAL_REQUIRED')
        actor,access,context=self._capture(headers,workspace_id,conversation_id,source['scope']['request_id'])
        for k in ('workspace_id','conversation_id','request_id','sku','country','channel','request_sha256'):
            _require(source['scope'][k]==context['scope'][k],'DOCUMENT_REQUEST_CHANGED')
        self.intake.store.preflight()
        original=source['original'];file=source['file']
        _require(original['bucket']==self.intake.store.bucket
            and original['object_name']=='campaign-evidence/'+submission_id
            and 0<file['bytes']<=MAX_BYTES,'DOCUMENT_ORIGINAL_SCOPE')
        blob=self.intake.store.storage.bucket(original['bucket']).blob(original['object_name'])
        blob.reload(retry=None,timeout=20)
        _require(str(blob.generation)==original['generation'] and blob.size==file['bytes']
            and blob.content_type==file['content_type'],'DOCUMENT_ORIGINAL_CHANGED')
        return actor,access,context,source,blob

    def _fresh(self,headers,workspace_id,conversation_id,submission_id,initial):
        actor,access,context,source,blob=initial
        after=self._source(headers,workspace_id,conversation_id,submission_id)
        _require(after[0].principal==actor.principal and after[1]==access and after[2]==context
            and canonical(after[3])==canonical(source),'DOCUMENT_CONTEXT_CHANGED')

    def extract(self,headers,*,workspace_id,conversation_id,submission_id):
        initial=self._source(headers,workspace_id,conversation_id,submission_id)
        actor,access,context,source,blob=initial
        content=blob.download_as_bytes(if_generation_match=int(source['original']['generation']),retry=None,timeout=30)
        file=inspect_file(source['file']['filename'],content,source['selection']['category'])
        _require(file==source['file'],'DOCUMENT_ORIGINAL_HASH')
        result=self.extractor(content,file['content_type'])
        _require(result['source_sha256']==file['content_sha256'] and result['eligible_for_generation'] is False
            and result['claim_approval'] is False,'DOCUMENT_EXTRACTOR_UNVERIFIED')
        record=dict(version='campaign-evidence-extraction-v1',submission_id=submission_id,
            source_record_sha256=hashlib.sha256(canonical(source).encode()).hexdigest(),
            scope=source['scope'],principal_id=actor.principal.principal_id,original=source['original'],
            extraction=result,review_status='UNVERIFIED',eligible_for_generation=False)
        self._fresh(headers,workspace_id,conversation_id,submission_id,initial)
        extraction_id=self.store.save_extraction(record,submission=source)
        self._fresh(headers,workspace_id,conversation_id,submission_id,initial)
        return self._receipt(extraction_id,record)

    @staticmethod
    def _receipt(extraction_id,record):
        extraction=record['extraction']
        reasons=['SOURCE_REVIEW_REQUIRED','NO_CLAIM_APPROVAL','PER_RESPONSE_EVALUATION_REQUIRED']
        if extraction['status']!='EXTRACTED_UNVERIFIED':reasons.insert(0,extraction['status'])
        return DocumentReceipt(extraction_id,record['submission_id'],extraction['status'],
            extraction['text_bytes'],len(extraction['segments']),'NEEDS_REVIEW',tuple(reasons))

    def reevaluate(self,headers,*,workspace_id,conversation_id,extraction_id):
        actor,_=self.intake._actor(headers,workspace_id,conversation_id)
        record=self.store.read('extractions',extraction_id)
        _require(record is not None and record['scope']['workspace_id']==workspace_id
            and record['scope']['conversation_id']==conversation_id
            and record['principal_id']==actor.principal.principal_id,'DOCUMENT_EXTRACTION_SCOPE')
        initial=self._source(headers,workspace_id,conversation_id,record['submission_id'])
        _require(hashlib.sha256(canonical(initial[3]).encode()).hexdigest()==record['source_record_sha256']
            and record['version']=='campaign-evidence-extraction-v1'
            and record['review_status']=='UNVERIFIED'
            and initial[3]['original']==record['original'] and record['extraction']['version']==VERSION
            and record['extraction']['source_sha256']==initial[3]['file']['content_sha256']
            and record['extraction']['claim_approval'] is False
            and record['extraction']['eligible_for_generation'] is False
            and record['eligible_for_generation'] is False,'DOCUMENT_EXTRACTION_CHANGED')
        self._fresh(headers,workspace_id,conversation_id,record['submission_id'],initial)
        # Extraction success is not source/claim approval or a campaign quality
        # measurement. No text is returned to model context through this API.
        return self._receipt(extraction_id,record)

    def status(self,headers,*,workspace_id,conversation_id,submission_id):
        actor,_=self.intake._actor(headers,workspace_id,conversation_id)
        record_id=self.store.find_extraction(submission_id,workspace_id=workspace_id,
            conversation_id=conversation_id,principal_id=actor.principal.principal_id)
        if record_id is None:return None
        return self.reevaluate(headers,workspace_id=workspace_id,conversation_id=conversation_id,extraction_id=record_id)
