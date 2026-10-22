"""Pinned private retrieval query; separate slots from immutable document calls.

This contract grants no source eligibility, dispatch permission or qualification.
The protected caller must verify the exact index and fresh source/access state.
"""
from dataclasses import dataclass,asdict
import json
from src.application.campaign_evaluator_store import canonical,sha
from src.application.workflow_contract import _require,_id,_digest
from src.retrieval.upload_corpus import UploadCorpusCapture,VERSION as CORPUS_VERSION

VERSION='private-query-embedding-v1'


@dataclass(frozen=True,slots=True)
class QueryEmbeddingRequest:
    run_id:str
    query_id:str
    input_json:str
    profile:object

    def __post_init__(self):
        from src.retrieval.private_text_embeddings import TextEmbeddingProfile
        _id(self.run_id);_id(self.query_id)
        _require(type(self.profile) is TextEmbeddingProfile and self.profile.task_type=='RETRIEVAL_QUERY',
            'PRIVATE_QUERY_PROFILE')
        _require(type(self.input_json) is str and len(self.input_json.encode())<=16384,'PRIVATE_QUERY_INPUT')
        value=json.loads(self.input_json)
        _require(canonical(value)==self.input_json and set(value)=={'version','corpus_sha256',
            'corpus_binding_id','index_record_id','index_sha256','scope','principal_id','text'}
            and value['version']==VERSION,'PRIVATE_QUERY_INPUT')
        for key in ('corpus_sha256','index_record_id','index_sha256'):_digest(value[key])
        prefix='eval_corpus_binding_v1_';bid=value['corpus_binding_id']
        _require(type(bid) is str and bid.startswith(prefix),'PRIVATE_QUERY_BINDING');_digest(bid[len(prefix):])
        _require(type(value['scope']) is dict and type(value['text']) is str
            and 0<len(value['text'].encode())<=4096,'PRIVATE_QUERY_TEXT')
        for key in ('workspace_id','conversation_id','request_id','sku','country','channel'):_id(value['scope'][key])
        _id(value['principal_id'])

    @property
    def intent(self):
        value=json.loads(self.input_json)
        slot=sha([VERSION,self.run_id,value['scope']['workspace_id'],value['scope']['conversation_id'],
            value['scope']['request_id'],self.query_id])
        return dict(version=VERSION,record_id=slot,run_id=self.run_id,query_id=self.query_id,
            input=value,input_sha256=sha(value),profile=asdict(self.profile),
            claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)


def query_request(capture,binding_id,index,run_id,query_id,text):
    from src.retrieval.private_text_embeddings import TextEmbeddingProfile
    _require(type(capture) is UploadCorpusCapture and type(index) is dict,'PRIVATE_QUERY_CAPTURE')
    value=json.loads(capture.manifest_json)
    _require(canonical(value)==capture.manifest_json and value['version']==CORPUS_VERSION
        and value['use']=='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY'
        and all(value[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')),
        'PRIVATE_QUERY_CAPTURE')
    _require(set(index)=={'index_record_id','index_sha256','corpus_sha256','chunk_count','dimension',
        'claim_approval','generation_authorized','qualifying','release_allowed'}
        and index['corpus_sha256']==capture.sha256 and type(index['chunk_count']) is int
        and index['chunk_count']==len(value['text_chunks'])>0
        and type(index['dimension']) is int and index['dimension']==768
        and all(index[k] is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed')),
        'PRIVATE_QUERY_INDEX_CHANGED')
    payload=dict(version=VERSION,corpus_sha256=capture.sha256,corpus_binding_id=binding_id,
        index_record_id=index['index_record_id'],index_sha256=index['index_sha256'],
        scope=value['scope'],principal_id=value['principal_id'],text=text)
    return QueryEmbeddingRequest(run_id,query_id,canonical(payload),TextEmbeddingProfile(task_type='RETRIEVAL_QUERY'))
