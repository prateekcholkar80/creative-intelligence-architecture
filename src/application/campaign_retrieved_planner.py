"""Connect recovered private reranking to a registered evaluation planner.

The caller's protected planner guard must reconstruct this input before dispatch
and publication. This builder grants no source, claim, artifact or release authority.
"""
from dataclasses import replace
from datetime import datetime
import hashlib,json

from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_planner_evaluation import prepare_planner_evaluation
from src.application.prompt_runtime_contract import render_prompt
from src.application.workflow_contract import _require
from src.retrieval.private_semantic_rerank import DurableSemanticRerank,RerankRequest
from src.retrieval.upload_corpus import UploadCorpusCapture,VERSION as CORPUS_VERSION

VERSION='retrieved-private-planner-input-v1'


def prepare_retrieved_planner_evaluation(*,corpus,rerank_request,rerank_case,reranker,checked_at,**planner_arguments):
    _require(type(corpus) is UploadCorpusCapture and type(rerank_request) is RerankRequest
        and type(reranker) is DurableSemanticRerank and type(checked_at) is datetime
        and checked_at.tzinfo is not None and checked_at<corpus.valid_until,
        'RETRIEVED_PLANNER_CAPTURE')
    prepared=prepare_planner_evaluation(**planner_arguments)
    intent=json.loads(prepared.intent_json);source=intent['source'];manifest=json.loads(corpus.manifest_json)
    request=json.loads(rerank_request.input_json)
    uploads=planner_arguments['uploads']
    _require(checked_at<uploads.valid_until and canonical(manifest)==corpus.manifest_json
        and manifest['version']==CORPUS_VERSION
        and manifest['use']=='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY'
        and all(manifest[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed'))
        and manifest['scope']==source['context']['scope']==request['scope']
        and manifest['principal_id']==source['principal_id']==request['principal_id']
        and manifest['policy_sha256']==source['context']['policy_sha256']
        and manifest['image_manifest_sha256']==uploads.sha256
        and manifest['image_binding_id']==intent['input_binding_id']
        and manifest['image_pins']==source['image_pins']
        and corpus.images==uploads.images
        and corpus.sha256==request['corpus_sha256'],'RETRIEVED_PLANNER_SOURCE_CHANGED')
    chunks=manifest['text_chunks']
    expected=[dict(chunk_id=c['chunk_id'],text=c['text'],text_sha256=c['provenance']['text_sha256']) for c in chunks]
    _require(expected==request['chunks'],'RETRIEVED_PLANNER_CHUNKS_CHANGED')
    for c in chunks:
        _require(hashlib.sha256(c['text'].encode()).hexdigest()==c['provenance']['text_sha256'],
            'RETRIEVED_PLANNER_CHUNKS_CHANGED')
    # Recovery verifies the original intent/outcome and fresh upstream authority.
    # It cannot dispatch a missing or uncertain semantic evaluation.
    receipt=reranker.execute(rerank_request,rerank_case,recover_only=True)
    _require(receipt['corpus_sha256']==corpus.sha256
        and all(receipt[k] is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed')),
        'RETRIEVED_PLANNER_RERANK_UNVERIFIED')
    by_id={c['chunk_id']:c for c in chunks}
    selected=[dict(rank=i+1,chunk_id=cid,text=by_id[cid]['text'],provenance=by_id[cid]['provenance'],
        approved_copy_source=False) for i,cid in enumerate(receipt['ranked_ids'][:3])]
    evidence=dict(version=VERSION,status='MEASURED_PRIVATE_CALIBRATION',
        corpus_binding_id=request['corpus_binding_id'],corpus_sha256=corpus.sha256,
        rerank_record_id=receipt['rerank_record_id'],rerank_outcome_sha256=receipt['outcome_sha256'],
        rerank_response_sha256=receipt['response_sha256'],parent=receipt['parent'],
        query=request['query'],selected_chunks=selected,
        use='UNVERIFIED_REFERENCE_NOT_APPROVED_COPY',claim_approval=False,qualifying=False,release_allowed=False)
    bundle=dict(uploaded_visual_reference=source,approved_claims=[],
        retrieval_status=evidence['status'],retrieval_evidence=evidence,product_grounding=intent['grounding'])
    system,content=render_prompt(planner_arguments['snapshot'],dict(campaign_request=canonical(intent['case_input']),
        evidence_bundle_json=canonical(bundle),creative_options_json=prepared.options_json))
    _require(system==intent['system'],'RETRIEVED_PLANNER_PROMPT_CHANGED')
    intent.update(content=content,retrieval_evidence=evidence,retrieval_evidence_sha256=sha(evidence))
    return replace(prepared,intent_json=canonical(intent))
