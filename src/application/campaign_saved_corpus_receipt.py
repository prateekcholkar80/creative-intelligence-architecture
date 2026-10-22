"""Metadata-only saved-score recovery; no image bytes or dispatch authority.

The original native binding proves the earlier full byte capture. Every call
checks current access/policy, exact current records/native jobs and GCS generation
metadata. This reader is used only by explicit no-model receipt recovery.
"""
import hashlib,json
from datetime import timedelta
from google.cloud import bigquery
from src.application.campaign_evaluator_store import sha
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_attachment_service import TABLE
from src.application.campaign_corpus_preparation import BoundCorpusCapture,PREFIX,digest
from src.application.campaign_evaluation_uploads import EvaluationUploadInputs
from src.application.operation_budget import require_owned_worker,check_operation
from src.application.workflow_contract import _require,_digest
from src.retrieval.upload_corpus import UploadCorpusCapture,parallel_reads,VERSION


def materialize(record,extractions,expected_sha256):
    """Reconstruct text only from the exact stored segments, never local input."""
    manifest=json.loads(canonical(record['corpus']))
    _require(manifest.pop('text_materialization')=='RECONSTRUCT_FROM_EXACT_STORED_EXTRACTION'
        and manifest['version']==VERSION and manifest['use']=='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY'
        and all(manifest[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')),
        'SAVED_CORPUS_RECORD_CHANGED')
    for chunk in manifest['text_chunks']:
        provenance=chunk['provenance'];eid=provenance['extraction_id'];extracted=extractions[eid]
        _require(digest(extracted)==eid and sha(provenance)==chunk['chunk_id'],'SAVED_CORPUS_EXTRACTION_CHANGED')
        segments=[s for s in extracted['extraction']['segments'] if s['segment']==provenance['segment']]
        _require(len(segments)==1,'SAVED_CORPUS_SEGMENT_CHANGED');segment=segments[0]
        text=segment['text']
        _require(all(segment[k]==provenance[k] for k in ('segment','page','start_char','end_char','text_sha256'))
            and hashlib.sha256(text.encode()).hexdigest()==provenance['text_sha256'],'SAVED_CORPUS_SEGMENT_CHANGED')
        chunk['text']=text
    _require(digest(manifest)==expected_sha256==record['corpus_sha256'],'SAVED_CORPUS_HASH_CHANGED')
    return manifest


def capture_for_receipt(service,headers,binding_id,corpus_sha256):
    require_owned_worker();check_operation();_digest(corpus_sha256)
    _require(type(binding_id) is str and binding_id.startswith(PREFIX),'SAVED_CORPUS_BINDING');_digest(binding_id[len(PREFIX):])
    selected=json.loads(service.settings.selection_json);scope=selected['scope'];docs=service.documents
    args={k:scope[k] for k in ('workspace_id','conversation_id')}
    actor,access,context=docs._capture(headers,**args,request_id=scope['request_id'])
    def read_binding(id):
        rows=docs.store.query(f'SELECT record_json FROM `{TABLE}` WHERE submission_id=@id LIMIT 2',
            [bigquery.ScalarQueryParameter('id','STRING',id)])
        _require(len(rows)==1,'SAVED_CORPUS_BINDING_MISSING');raw=rows[0]['record_json'];value=json.loads(raw)
        _require(canonical(value)==raw,'SAVED_CORPUS_BINDING_CHANGED');return value
    record=read_binding(binding_id)
    _require(binding_id==PREFIX+digest(record) and record['version']=='private-upload-corpus-binding-v1'
        and record['run_id']==service.settings.run_id and record['selection_sha256']==service.settings.selection_sha256
        and record['corpus_sha256']==corpus_sha256 and record['principal_id']==actor.principal.principal_id
        and record['binding_scope']==dict(args,request_id=scope['request_id'])
        and all(record[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed'))
        and record['corpus']['scope']==context['scope'] and record['corpus']['policy_sha256']==context['policy_sha256']
        and all(context['scope'].get(k)==v for k,v in scope.items()),'SAVED_CORPUS_SCOPE_CHANGED')
    service._verify_binding(binding_id,record)
    def read_source(pin):
        source=docs.store.submission(pin['submission_id'],**args,principal_id=actor.principal.principal_id)
        _require(source['review_status']=='UNVERIFIED' and source['eligible_for_generation'] is False
            and source['scope']==context['scope'] and source['file']['content_sha256']==pin['content_sha256']
            and source['file']['content_type']==pin['content_type'] and source['original']['generation']==pin['generation']
            and source['original']['bucket']==docs.intake.store.bucket
            and source['original']['object_name']=='campaign-evidence/'+pin['submission_id'],'SAVED_CORPUS_SOURCE_CHANGED')
        return source
    observed=parallel_reads(tuple(lambda pin=pin:read_source(pin) for pin in selected['sources']))
    sources={pin['submission_id']:source for pin,source in zip(selected['sources'],observed)}
    def read_extraction(item):
        eid=item['extraction_id'];extracted=docs.store.read('extractions',eid);source=sources[item['submission_id']]
        _require(extracted is not None and digest(extracted)==eid and extracted['scope']==context['scope']
            and extracted['principal_id']==actor.principal.principal_id and extracted['review_status']=='UNVERIFIED'
            and extracted['eligible_for_generation'] is False and extracted['original']==source['original']
            and extracted['source_record_sha256']==digest(source)==item['source_record_sha256'],
            'SAVED_CORPUS_EXTRACTION_CHANGED')
        _require(docs.store.verify_extraction(eid,extracted,submission=source)==item['extraction_native_job_id'],
            'SAVED_CORPUS_EXTRACTION_CHANGED')
        return eid,extracted
    extractions=dict(parallel_reads(tuple(lambda item=item:read_extraction(item) for item in record['corpus']['text_sources'])))
    _require({v['submission_id'] for v in record['corpus']['text_sources']}==
        {pin['submission_id'] for pin in selected['sources'] if pin['role']=='DOCUMENT'},'SAVED_CORPUS_SELECTION_CHANGED')
    image_id=selected['image_binding_id'];image_record=read_binding(image_id);now=service.clock()
    _require(image_record['context']==context and image_record['principal_id']==actor.principal.principal_id
        and image_record['image_pins']==record['corpus']['image_pins']
        and digest(image_record)==record['corpus']['image_manifest_sha256']
        and image_id==record['corpus']['image_binding_id'],'SAVED_CORPUS_IMAGE_BINDING_CHANGED')
    for item in image_record['sources']:
        _require(item['source_record_sha256']==digest(sources[item['submission_id']]),'SAVED_CORPUS_IMAGE_BINDING_CHANGED')
    inputs=EvaluationUploadInputs(canonical(image_record),(),now,now,now+timedelta(seconds=60))
    _require(service.reader.images.verify_binding(image_id,inputs)==record['corpus']['image_binding_native_job_id'],
        'SAVED_CORPUS_IMAGE_BINDING_CHANGED')
    manifest=materialize(record,extractions,corpus_sha256)
    # Deliberately no image bytes. This object is never used by Run/dispatch.
    capture=UploadCorpusCapture(canonical(manifest),(),now+timedelta(seconds=60))
    def refresh():
        return service._refresh_bound(headers,actor,access,scope,sources,capture,binding_id,record,extractions,
            additional_records={image_id:image_record})
    _require(refresh() is True,'SAVED_CORPUS_SOURCE_CHANGED')
    receipt=dict(binding_id=binding_id,corpus_sha256=corpus_sha256,selection_sha256=service.settings.selection_sha256)
    return BoundCorpusCapture(capture,canonical(receipt),refresh)
