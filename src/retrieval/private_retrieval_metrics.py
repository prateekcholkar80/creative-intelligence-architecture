"""Source-bound single-case calibration metrics; never pipeline qualification."""
import math
from src.application.campaign_evaluator_store import sha
from src.application.workflow_contract import _require
from src.retrieval.campaign_hybrid import EligibleCandidate,rank_hybrid

VERSION='private-retrieval-calibration-metrics-v1'


def ranking_metrics(ids,grades,k):
    _require(type(grades) is dict and bool(grades) and type(ids) is tuple
        and len(set(ids))==len(ids) and all(i in grades for i in ids)
        and all(type(g) is int and 0<=g<=3 for g in grades.values())
        and any(g>=2 for g in grades.values()) and type(k) is int and 1<=k<=20,'PRIVATE_RETRIEVAL_LABELS')
    selected=ids[:k];relevant={i for i,g in grades.items() if g>=2}
    gains=lambda values:sum((2**g-1)/math.log2(i+2) for i,g in enumerate(values))
    ideal=gains(sorted(grades.values(),reverse=True)[:k])
    return dict(k=k,precision=sum(i in relevant for i in selected)/k,
        recall=sum(i in relevant for i in selected)/len(relevant),
        reciprocal_rank=next((1/(r+1) for r,i in enumerate(selected) if i in relevant),0.),
        ndcg=gains([grades[i] for i in selected])/ideal)


def measure(observation,case,chunks,scope):
    _require(type(case) is dict and case['version']=='private-retrieval-relevance-case-v1'
        and case['label_authority']=='SOURCE_REVIEW_CALIBRATION_NOT_INDEPENDENT_QUALIFICATION'
        and case['corpus_sha256']==observation['corpus_sha256']
        and case['index_sha256']==observation['index_sha256']
        and case['query_text_sha256']==observation['query_text_sha256']
        and scope==observation['scope'] and case['scope']=={k:scope[k] for k in ('sku','country','channel')},'PRIVATE_RETRIEVAL_CASE')
    source={c['chunk_id']:c for c in chunks};labels=case['labels']
    _require(type(labels) is list and len(labels)==len(source)
        and len({x['chunk_id'] for x in labels})==len(labels) and {x['chunk_id'] for x in labels}==set(source),
        'PRIVATE_RETRIEVAL_LABEL_COVERAGE')
    for x in labels:_require(x['text_sha256']==source[x['chunk_id']]['provenance']['text_sha256'],'PRIVATE_RETRIEVAL_LABEL_SOURCE')
    grades={x['chunk_id']:x['grade'] for x in labels};rows=observation['rows']
    _require(len(rows)==len(source) and {r['chunk_id'] for r in rows}==set(source),'PRIVATE_RETRIEVAL_METRIC_MEMBERSHIP')
    lexical=tuple(r['chunk_id'] for r in sorted((r for r in rows if r['lexical_rank'] is not None),key=lambda r:r['lexical_rank']))
    vector=tuple(r['chunk_id'] for r in sorted(rows,key=lambda r:r['vector_rank']))
    candidates=tuple(EligibleCandidate(i,source[i]['provenance']['text_sha256'],scope['sku'],scope['country'],scope['channel'],
        'UNSPECIFIED','UNSPECIFIED',observation['corpus_sha256']) for i in sorted(source))
    fusion=rank_hybrid(candidates,lexical,vector,sku=scope['sku'],country=scope['country'],channel=scope['channel'],
        objective='PRIVATE_SOURCE_LOOKUP',audience='UNSPECIFIED',top_k=min(len(source),20))
    fused=tuple(r['chunk_id'] for r in fusion['results'])
    return dict(version=VERSION,case_sha256=sha(case),observation_sha256=sha(observation),
        corpus_sha256=observation['corpus_sha256'],index_sha256=observation['index_sha256'],
        rankings=dict(lexical=list(lexical),vector=list(vector),rrf=list(fused)),
        metrics={name:[ranking_metrics(ids,grades,k) for k in sorted({1,min(3,len(source))})]
            for name,ids in [('lexical',lexical),('vector',vector),('rrf',fused)]},
        fusion_sha256=fusion['sha256'],label_authority=case['label_authority'],
        semantic_reranking='NOT_EXECUTED',candidate_metadata='UNSPECIFIED_NO_PREFERENCE_ADVANTAGE',
        claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
