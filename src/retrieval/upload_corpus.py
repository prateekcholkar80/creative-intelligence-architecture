"""Private retrieval corpus from protected BigQuery records and original GCS bytes.

No legacy POC view, local fallback, embedding/model invocation or authority
promotion. Text remains untrusted private evaluation material, never approved
claim context. This read-only capture is not a persistent vector index.
"""
from dataclasses import dataclass
from datetime import datetime,timezone,timedelta
import hashlib,json
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context

from src.application.campaign_document_service import ProtectedDocumentService
from src.application.campaign_document_extraction import VERSION as PARSER_VERSION
from src.application.campaign_evaluation_uploads import ProtectedEvaluationUploadReader
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_attachment_service import inspect_file
from src.application.workflow_contract import _require,_digest,_id

VERSION='private-upload-retrieval-corpus-v1'
MAX_TEXT_BYTES=24000


def parallel_reads(callbacks):
    """Bounded read-only overlap; every child inherits its owned operation budget."""
    with ThreadPoolExecutor(max_workers=3,thread_name_prefix='private-corpus-read') as pool:
        futures=[pool.submit(copy_context().run,callback) for callback in callbacks]
        return tuple(f.result() for f in futures)


@dataclass(frozen=True,slots=True)
class UploadCorpusCapture:
    manifest_json:str
    images:tuple
    valid_until:datetime

    @property
    def sha256(self):return hashlib.sha256(self.manifest_json.encode()).hexdigest()


class ProtectedUploadCorpusReader:
    def __init__(self,documents,*,clock=lambda:datetime.now(timezone.utc)):
        _require(type(documents) is ProtectedDocumentService,'UPLOAD_CORPUS_SERVICE')
        self.documents,self.clock=documents,clock
        self.images=ProtectedEvaluationUploadReader(documents,clock=clock)

    def capture(self,headers,*,workspace_id,conversation_id,request_id,sku,country,channel,
                image_binding_id,image_submission_ids,extraction_ids):
        for value in (workspace_id,conversation_id,request_id,sku,country,channel):_id(value)
        _require(type(extraction_ids) is tuple and len(extraction_ids)<=8
            and len(set(extraction_ids))==len(extraction_ids),'UPLOAD_CORPUS_SELECTION')
        for eid in extraction_ids:_digest(eid)
        started=self.clock();fresh_checks=[]
        first_actor,first_access=self.documents.intake._actor(headers,workspace_id,conversation_id)
        args=dict(workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id,
            submission_ids=image_submission_ids)
        def read_text(eid):
            _require(self.clock()<started+timedelta(seconds=300),'UPLOAD_CORPUS_EXPIRED')
            record=self.documents.store.read('extractions',eid)
            _require(type(record) is dict and record.get('version')=='campaign-evidence-extraction-v1'
                and record.get('review_status')=='UNVERIFIED' and record.get('eligible_for_generation') is False
                and record.get('principal_id')==first_actor.principal.principal_id,'UPLOAD_CORPUS_EXTRACTION_SCOPE')
            sid=record['submission_id'];_digest(sid)
            local_chunks=[]
            initial=self.documents._source(headers,workspace_id,conversation_id,sid)
            actor,access,context,source,blob=initial
            _require(actor.principal==first_actor.principal and access==first_access
                and context['scope']==record['scope'] and source['scope']==record['scope']
                and source['original']==record['original']
                and sha(source)==record['source_record_sha256'],'UPLOAD_CORPUS_SOURCE_CHANGED')
            raw=blob.download_as_bytes(if_generation_match=int(source['original']['generation']),retry=None,timeout=30)
            _require(inspect_file(source['file']['filename'],raw,source['selection']['category'])==source['file'],
                'UPLOAD_CORPUS_BYTES_CHANGED')
            parsed=self.documents.extractor(raw,source['file']['content_type'])
            _require(parsed==record['extraction'] and parsed['version']==PARSER_VERSION
                and parsed['source_sha256']==source['file']['content_sha256']
                and parsed['status']=='EXTRACTED_UNVERIFIED' and parsed['claim_approval'] is False
                and parsed['eligible_for_generation'] is False,'UPLOAD_CORPUS_EXTRACTION_CHANGED')
            native=self.documents.store.verify_extraction(eid,record,submission=source)
            for segment in parsed['segments']:
                provenance=dict(submission_id=sid,extraction_id=eid,source_record_sha256=record['source_record_sha256'],
                    source_sha256=parsed['source_sha256'],original=record['original'],parser_version=parsed['version'],
                    parser=parsed['parser'],offset_unit=parsed['offset_unit'],segment=segment['segment'],page=segment['page'],
                    start_char=segment['start_char'],end_char=segment['end_char'],text_sha256=segment['text_sha256'])
                local_chunks.append(dict(chunk_id=sha(provenance),provenance=provenance,text=segment['text'],
                    trust='UNTRUSTED_USER_EVIDENCE',eligible_for_generation=False,claim_approval=False))
            text_source=dict(submission_id=sid,extraction_id=eid,original_native_job_id='evidence_submission_v1_'+sid,
                extraction_native_job_id=native,source_record_sha256=record['source_record_sha256'])
            return text_source,local_chunks,parsed['text_bytes'],(sid,initial,eid,record)
        reads=parallel_reads([lambda:self.images.capture(headers,**args)]
            +[lambda eid=eid:read_text(eid) for eid in sorted(extraction_ids)])
        images=reads[0];binding_job=self.images.verify_binding(image_binding_id,images)
        image_manifest=json.loads(images.manifest_json);scope=image_manifest['context']['scope']
        _require(first_actor.principal.principal_id==image_manifest['principal_id']
            and all(scope.get(k)==v for k,v in dict(workspace_id=workspace_id,conversation_id=conversation_id,
            request_id=request_id,sku=sku,country=country,channel=channel).items()),'UPLOAD_CORPUS_SCOPE')
        chunks=[];sources=[];initials=[];submissions=set(image_submission_ids);total=0
        for text_source,local_chunks,size,initial in reads[1:]:
            sid,capture,eid,record=initial
            _require(record['scope']==scope,'UPLOAD_CORPUS_EXTRACTION_SCOPE')
            _require(capture[2]==image_manifest['context'],'UPLOAD_CORPUS_SOURCE_CHANGED')
            _require(sid not in submissions,'UPLOAD_CORPUS_DUPLICATE_SOURCE');submissions.add(sid)
            total+=size;_require(total<=MAX_TEXT_BYTES,'UPLOAD_CORPUS_TEXT_LIMIT')
            chunks.extend(local_chunks);sources.append(text_source);initials.append(initial)
        def recheck(item):
            sid,initial,eid,record=item;observed=self.clock()
            self.documents._fresh(headers,workspace_id,conversation_id,sid,initial)
            _require(self.documents.store.read('extractions',eid)==record,'UPLOAD_CORPUS_EXTRACTION_CHANGED')
            return observed
        checked=parallel_reads([lambda:self.images.capture(headers,**args)]
            +[lambda item=item:recheck(item) for item in initials])
        latest=checked[0];fresh_checks.extend(checked[1:])
        _require(latest.manifest_json==images.manifest_json,'UPLOAD_CORPUS_IMAGE_CHANGED')
        self.images.verify_binding(image_binding_id,latest)
        last_actor,last_access=self.documents.intake._actor(headers,workspace_id,conversation_id)
        _require(last_actor.principal==first_actor.principal and last_access==first_access,
            'UPLOAD_CORPUS_ACCESS_CHANGED')
        ended=self.clock();valid_until=min([latest.valid_until,started+timedelta(seconds=300)]
            +[t+timedelta(seconds=120) for t in fresh_checks]
            +[datetime.fromtimestamp(last_actor.expires_at,tz=timezone.utc)])
        _require(started<=ended<valid_until,'UPLOAD_CORPUS_EXPIRED')
        manifest=dict(version=VERSION,scope=scope,policy_sha256=image_manifest['context']['policy_sha256'],principal_id=image_manifest['principal_id'],
            image_binding_id=image_binding_id,image_binding_native_job_id=binding_job,
            image_manifest_sha256=images.sha256,image_pins=image_manifest['image_pins'],
            text_sources=sources,text_chunks=chunks,text_bytes=total,
            source_of_truth='BIGQUERY_RECORDS_AND_GCS_ORIGINAL_BYTES',
            use='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY',claim_approval=False,
            eligible_for_generation=False,qualifying=False,release_allowed=False,
            vector_status='NOT_EMBEDDED',retrieval_quality='NOT_MEASURED')
        return UploadCorpusCapture(canonical(manifest),latest.images,valid_until)
