"""Explicit protected source preparation and immutable corpus binding; no model.

Reuses extraction transactions and the existing content-addressed binding store.
Recovery reads the original binding only. This does not complete a campaign
request, approve claims, create vectors or qualify benchmark cases.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib,json
from threading import RLock
from google.cloud import bigquery

from src.retrieval.upload_corpus_selection import validate_selection
from src.retrieval.upload_corpus import ProtectedUploadCorpusReader
from src.application.campaign_document_service import ProtectedDocumentService
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_attachment_service import TABLE,SQL,LOCATION
from src.application.sharing_jobs import matches_sharing_job
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_id,_digest

VERSION='private-upload-corpus-binding-v1'
PREFIX='eval_corpus_binding_v1_'


def bound_record_guard_sql():
    from src.application.campaign_document_store import TABLES
    return f'''WITH source_pins AS (
SELECT JSON_VALUE(p,'$.id') id,JSON_VALUE(p,'$.record') record FROM UNNEST(JSON_QUERY_ARRAY(@sources)) p),
extraction_pins AS (SELECT JSON_VALUE(p,'$.id') id,JSON_VALUE(p,'$.record') record FROM UNNEST(JSON_QUERY_ARRAY(@extractions)) p)
SELECT (SELECT COUNT(*) FROM `{TABLE}` WHERE submission_id IN UNNEST(@source_ids)) source_count,
(SELECT COUNT(*) FROM `{TABLE}` t JOIN source_pins p ON t.submission_id=p.id AND t.record_json=p.record) source_matches,
(SELECT COUNT(*) FROM `{TABLES['extractions']}` WHERE record_id IN UNNEST(@extraction_ids)) extraction_count,
(SELECT COUNT(*) FROM `{TABLES['extractions']}` t JOIN extraction_pins p ON t.record_id=p.id AND t.record_json=p.record) extraction_matches'''


def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True,slots=True)
class CorpusPreparationSettings:
    run_id:str
    selection_json:str
    selection_sha256:str
    enabled:bool=False

    def __post_init__(self):
        _id(self.run_id);_digest(self.selection_sha256)
        _require(type(self.enabled) is bool and type(self.selection_json) is str
            and len(self.selection_json.encode())<=16384,'CORPUS_SETTINGS')
        value=validate_selection(json.loads(self.selection_json))
        _require(canonical(value)==self.selection_json and digest(value)==self.selection_sha256,'CORPUS_SELECTION_CHANGED')


@dataclass(frozen=True,slots=True)
class BoundCorpusCapture:
    capture:object
    receipt_json:str
    refresh:object


class ProtectedCorpusPreparation:
    def __init__(self,documents,settings,*,clock=lambda:datetime.now(timezone.utc)):
        require_owned_worker()
        _require(type(documents) is ProtectedDocumentService and type(settings) is CorpusPreparationSettings,
            'CORPUS_PREPARATION_COMPOSITION')
        self.documents,self.settings,self.clock=documents,settings,clock
        self.reader=ProtectedUploadCorpusReader(documents,clock=clock);self._lock=RLock()

    def _verify_binding(self,binding_id,record,*,allow_missing=False):
        rows=self.documents.store.query(f'SELECT record_json FROM `{TABLE}` WHERE submission_id=@id LIMIT 2',
            [bigquery.ScalarQueryParameter('id','STRING',binding_id)])
        if not rows and allow_missing:return False
        _require(rows==({'record_json':canonical(record)},),'CORPUS_BINDING_NOT_VERIFIED')
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,query_parameters=[
                bigquery.ScalarQueryParameter('id','STRING',binding_id),
                bigquery.ScalarQueryParameter('record','STRING',canonical(record))])
        job_id='evidence_submission_v1_'+binding_id
        job=self.documents.store.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=job_id,sql=SQL,config=cfg)
            and job.state=='DONE' and job.error_result is None,'CORPUS_BINDING_NATIVE_UNVERIFIED')
        return True

    def prepare(self,headers,*,recover_only=False):
        return self._prepare(headers,recover_only=recover_only)

    def capture_bound(self,headers):
        """Trusted in-process capture; never a browser response or write path."""
        return self._prepare(headers,recover_only=True,return_bound=True)

    def capture_saved_bound_for_receipt(self,headers,*,binding_id,corpus_sha256):
        from src.application.campaign_saved_corpus_receipt import capture_for_receipt
        _require(self.settings.enabled,'CORPUS_PREPARATION_DISABLED')
        with self._lock:return capture_for_receipt(self,headers,binding_id,corpus_sha256)

    def _prepare(self,headers,*,recover_only=False,return_bound=False):
        require_owned_worker();check_operation()
        _require(self.settings.enabled and type(recover_only) is bool and type(return_bound) is bool
            and (not return_bound or recover_only),'CORPUS_PREPARATION_DISABLED')
        with self._lock:
            selected=json.loads(self.settings.selection_json);scope=selected['scope'];docs=self.documents
            args={k:scope[k] for k in ('workspace_id','conversation_id')}
            actor,access=docs.intake._actor(headers,**args);sources={}
            # Validate every fixed source before a missing extraction may write.
            for pin in selected['sources']:
                source=docs.store.submission(pin['submission_id'],**args,principal_id=actor.principal.principal_id)
                _require(source['review_status']=='UNVERIFIED' and source['eligible_for_generation'] is False
                    and all(source['scope'].get(k)==v for k,v in scope.items())
                    and source['file']['content_sha256']==pin['content_sha256']
                    and source['file']['content_type']==pin['content_type']
                    and source['original']['generation']==pin['generation'],'CORPUS_SELECTED_SOURCE_CHANGED')
                sources[pin['submission_id']]=source
            eids=[]
            for pin in selected['sources']:
                if pin['role']!='DOCUMENT':continue
                sid=pin['submission_id']
                eid=docs.store.find_extraction(sid,**args,principal_id=actor.principal.principal_id)
                if eid is None:
                    _require(not recover_only,'CORPUS_EXTRACTION_REQUIRED')
                    receipt=docs.extract(headers,**args,submission_id=sid)
                    _require(receipt.submission_id==sid and receipt.status=='EXTRACTED_UNVERIFIED'
                        and receipt.generation_authorized is False,'CORPUS_EXTRACTION_UNAVAILABLE')
                    eid=receipt.extraction_id
                _digest(eid);eids.append(eid)
            captured=self.reader.capture(headers,**scope,image_binding_id=selected['image_binding_id'],
                image_submission_ids=tuple(s['submission_id'] for s in selected['sources'] if s['role']=='BOUND_IMAGE'),
                extraction_ids=tuple(eids))
            manifest=json.loads(captured.manifest_json)
            for s in manifest['text_sources']:
                _require(s['source_record_sha256']==digest(sources[s['submission_id']]),'CORPUS_SELECTED_SOURCE_CHANGED')
            metadata=dict(manifest,text_chunks=[{k:v for k,v in c.items() if k!='text'} for c in manifest['text_chunks']],
                text_materialization='RECONSTRUCT_FROM_EXACT_STORED_EXTRACTION')
            record=dict(version=VERSION,run_id=self.settings.run_id,selection_sha256=self.settings.selection_sha256,
                corpus_sha256=captured.sha256,corpus=metadata,
                binding_scope={k:scope[k] for k in ('workspace_id','conversation_id','request_id')},
                principal_id=actor.principal.principal_id,
                claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)
            binding_id=PREFIX+digest(record)
            # Read-only bound capture has just completed the reader's before/
            # after checks; verify its immutable binding, then recheck once.
            # Publication paths retain the additional pre-write guard.
            if not return_bound:self._fresh(headers,actor,access,scope,sources,captured)
            exists=self._verify_binding(binding_id,record,allow_missing=not recover_only)
            if not exists:
                docs.intake.store.preflight();docs.intake.store.record(binding_id,record)
                self._verify_binding(binding_id,record)
            self._fresh(headers,actor,access,scope,sources,captured)
            receipt=dict(binding_id=binding_id,corpus_sha256=captured.sha256,selection_sha256=self.settings.selection_sha256,
                run_id=self.settings.run_id,source_count=len(selected['sources']),chunk_count=len(manifest['text_chunks']),
                image_count=len(manifest['image_pins']),text_bytes=manifest['text_bytes'],claim_approval=False,
                generation_authorized=False,qualifying=False,release_allowed=False)
            if return_bound:
                extraction_records={}
                for item in manifest['text_sources']:
                    eid=item['extraction_id'];extracted=docs.store.read('extractions',eid)
                    _require(extracted is not None and digest(extracted)==eid,'CORPUS_EXTRACTION_CHANGED')
                    extraction_records[eid]=extracted
                def refresh():
                    require_owned_worker();check_operation()
                    return self._refresh_bound(headers,actor,access,scope,sources,captured,
                        binding_id,record_binding,extraction_records)
                record_binding=record
                return BoundCorpusCapture(captured,canonical(receipt),refresh)
            return receipt

    def _refresh_bound(self,headers,actor,access,scope,sources,captured,binding_id,binding,extractions,*,additional_records=None):
        """New current proof over immutable byte identities; no authority cache.

        Initial capture verified the actual bytes and original native jobs. Every
        refresh rechecks exact destination rows, current access/context and GCS
        generations. It does not extend or reuse the old capture deadline.
        """
        docs=self.documents;started=self.clock();args={k:scope[k] for k in ('workspace_id','conversation_id')}
        _require(all(digest(record)==eid for eid,record in extractions.items()),'CORPUS_EXTRACTION_CHANGED')
        after,current,context=docs._capture(headers,**args,request_id=scope['request_id'])
        manifest=json.loads(captured.manifest_json)
        _require(after.principal==actor.principal and current==access and context['scope']==manifest['scope']
            and context['policy_sha256']==manifest['policy_sha256'],'CORPUS_PREPARATION_SOURCE_OR_ACCESS_CHANGED')
        wanted=dict(sources);wanted[binding_id]=binding
        if additional_records is not None:wanted.update(additional_records)
        params=[bigquery.ArrayQueryParameter('source_ids','STRING',list(wanted)),
            bigquery.ArrayQueryParameter('extraction_ids','STRING',list(extractions)),
            bigquery.ScalarQueryParameter('sources','STRING',canonical([dict(id=k,record=canonical(v)) for k,v in wanted.items()])),
            bigquery.ScalarQueryParameter('extractions','STRING',canonical([dict(id=k,record=canonical(v)) for k,v in extractions.items()]))]
        rows=docs.store.query(bound_record_guard_sql(),params)
        _require(rows==({'source_count':len(wanted),'source_matches':len(wanted),
            'extraction_count':len(extractions),'extraction_matches':len(extractions)},),'CORPUS_BOUND_RECORDS_CHANGED')
        for source in sources.values():
            original=source['original'];generation=int(original['generation'])
            blob=docs.intake.store.storage.bucket(original['bucket']).blob(original['object_name'],generation=generation)
            blob.reload(if_generation_match=generation,retry=None,timeout=20)
            _require(str(blob.generation)==original['generation'],'CORPUS_SELECTED_SOURCE_CHANGED')
        final,decision=docs.intake._actor(headers,**args)
        _require(final.principal==actor.principal and decision==access
            and 0<=(self.clock()-started).total_seconds()<=60,'CORPUS_BOUND_REFRESH_UNVERIFIED')
        return True

    def _fresh(self,headers,actor,access,scope,sources,captured):
        _require(self.clock()<captured.valid_until,'CORPUS_PREPARATION_CAPTURE_EXPIRED')
        docs=self.documents;args={k:scope[k] for k in ('workspace_id','conversation_id')}
        after,current,context=docs._capture(headers,**args,request_id=scope['request_id'])
        manifest=json.loads(captured.manifest_json)
        _require(after.principal==actor.principal and current==access and context['scope']==manifest['scope']
            and context['policy_sha256']==manifest['policy_sha256'],
            'CORPUS_PREPARATION_SOURCE_OR_ACCESS_CHANGED')
        for sid,source in sources.items():
            _require(docs.store.submission(sid,**args,principal_id=actor.principal.principal_id)==source,
                'CORPUS_SELECTED_SOURCE_CHANGED')
        final,decision=docs.intake._actor(headers,**args)
        _require(final.principal==actor.principal and decision==access,
            'CORPUS_PREPARATION_SOURCE_OR_ACCESS_CHANGED')
        _require(self.clock()<captured.valid_until,'CORPUS_PREPARATION_CAPTURE_EXPIRED')
