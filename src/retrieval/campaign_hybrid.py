"""Bounded deterministic ranking for caller-verified eligible candidates.

This pure algorithm is not an eligibility reader or approval authority. Server
composition must recheck source/access before using any resulting reference.
Lexical/vector lists come from the same eligible corpus; RRF does not invent a
vector score for lexical-only matches. Relevance requires labeled evaluation.
"""
from dataclasses import dataclass
from fractions import Fraction

from src.application.campaign_evaluator_store import sha
from src.application.workflow_contract import _require, _id, _digest

VERSION = 'campaign-hybrid-rrf-scope-rerank-v1'
MAX_CANDIDATES = 100


@dataclass(frozen=True, slots=True)
class EligibleCandidate:
    chunk_id: str
    content_sha256: str
    sku: str
    country: str
    channel: str
    objective: str
    audience: str
    source_capture_sha256: str

    def __post_init__(self):
        for name in ('chunk_id', 'sku', 'country', 'channel', 'objective', 'audience'):
            _id(getattr(self, name))
        for name in ('content_sha256', 'source_capture_sha256'):
            _digest(getattr(self, name))


def rank_hybrid(candidates, lexical_ids, vector_ids, *, sku, country, channel,
                objective, audience, top_k=5):
    """RRF baseline, then exact objective/audience preference within fixed scope.

    Match preference is deliberately explicit, not measured semantic relevance.
    Stable chunk IDs resolve ties. Empty input remains empty without fallback.
    """
    for value in (sku, country, channel, objective, audience): _id(value)
    _require(type(candidates) is tuple and len(candidates) <= MAX_CANDIDATES
        and all(type(c) is EligibleCandidate for c in candidates)
        and type(top_k) is int and 1 <= top_k <= 20, 'HYBRID_CANDIDATES')
    by_id = {c.chunk_id: c for c in candidates}
    _require(len(by_id) == len(candidates) and all((c.sku, c.country, c.channel) ==
        (sku, country, channel) for c in candidates)
        and len({c.source_capture_sha256 for c in candidates}) <= 1, 'HYBRID_SCOPE_OR_CAPTURE')
    for ids in (lexical_ids, vector_ids):
        _require(type(ids) is tuple and len(ids) <= MAX_CANDIDATES and len(set(ids)) == len(ids)
            and all(type(i) is str and i in by_id for i in ids), 'HYBRID_RANK_LIST')
    ranks = [{i: rank for rank, i in enumerate(ids, 1)} for ids in (lexical_ids, vector_ids)]
    union = set(lexical_ids) | set(vector_ids)
    _require(union == set(by_id), 'HYBRID_UNRANKED_CANDIDATE')
    scores = {i: sum((Fraction(1, 60+r[i]) for r in ranks if i in r), Fraction()) for i in union}
    baseline = sorted(union, key=lambda i: (-scores[i], i))
    ordered = sorted(baseline, key=lambda i: (
        -(by_id[i].objective == objective), -(by_id[i].audience == audience), -scores[i], i))
    rows = [dict(chunk_id=i, content_sha256=by_id[i].content_sha256,
        source_capture_sha256=by_id[i].source_capture_sha256, rank=rank,
        fusion_rank=baseline.index(i)+1, lexical_rank=ranks[0].get(i), vector_rank=ranks[1].get(i),
        rrf_score=dict(numerator=scores[i].numerator, denominator=scores[i].denominator),
        objective_match=by_id[i].objective == objective, audience_match=by_id[i].audience == audience)
        for rank, i in enumerate(ordered[:top_k], 1)]
    result = dict(version=VERSION, scope=dict(sku=sku, country=country, channel=channel),
        objective=objective, audience=audience, top_k=top_k, candidates=len(candidates),
        results=rows, empty=not rows, relevance='NOT_MEASURED', qualifying=False, release_allowed=False)
    return dict(result, sha256=sha(result))
