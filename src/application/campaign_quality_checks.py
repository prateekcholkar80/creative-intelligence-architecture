"""E2 deterministic fact/citation checks and fail-closed release primitive.

SourceFact is a supplied server-side observation, not authentication or a fresh
BigQuery read. Exact text equality proves bounded support, not semantic safety.
Unimplemented E1 checks stay UNAVAILABLE, preventing global PASS/activation.
"""
from dataclasses import dataclass
from datetime import datetime
import json,re,unicodedata
from src.application.campaign_acceptance_contract import RULE_IDS,Stage,Decision,CheckState,_utc,_hash
from src.application.workflow_contract import _require,_id,_digest,canonical_json_bytes

VERSION='campaign-quality-deterministic-v1'
IMPLEMENTED=('A01','G01','G03','G07','G10','U06')
CHANNEL_FIELDS={
    'META':('primary_text','headline','cta','placement_preview','applicable_disclosures'),
    'KLAVIYO_EMAIL':('subject','preheader','body','cta','applicable_terms_and_consent_review'),
    'TIKTOK':('script','on_screen_text','voiceover','cta','rights_and_disclosure_plan'),
    'PINTEREST':('title','description','destination','visual','applicable_disclosures')}


@dataclass(frozen=True,slots=True)
class SourceFact:
    source_id: str
    sku: str
    fields_json: str
    observed_at: datetime
    valid_until: datetime
    access_allowed: bool

    def __post_init__(self):
        _id(self.source_id);_id(self.sku)
        _require(type(self.fields_json) is str and len(self.fields_json.encode())<=65536,'INVALID_QUALITY_SOURCE')
        try:fields=json.loads(self.fields_json)
        except (ValueError,RecursionError):raise ValueError('Invalid source JSON') from None
        _require(type(fields) is dict and fields and len(fields)<=64
            and all(type(k) is str and type(v) is str and k and v for k,v in fields.items())
            and canonical_json_bytes(fields).decode()==self.fields_json,'INVALID_QUALITY_SOURCE')
        object.__setattr__(self,'observed_at',_utc(self.observed_at));object.__setattr__(self,'valid_until',_utc(self.valid_until))
        _require(self.observed_at<self.valid_until and type(self.access_allowed) is bool,'INVALID_QUALITY_SOURCE')

    @property
    def sha256(self):return _hash(dict(source_id=self.source_id,sku=self.sku,fields_json=self.fields_json,
        observed_at=self.observed_at.isoformat(),valid_until=self.valid_until.isoformat(),access_allowed=self.access_allowed))


@dataclass(frozen=True,slots=True)
class FactSpan:
    start: int
    end: int
    source_id: str
    field: str
    source_sha256: str

    def __post_init__(self):
        _require(type(self.start) is int and type(self.end) is int and 0<=self.start<self.end,'INVALID_QUALITY_SPAN')
        _id(self.source_id);_id(self.field);_digest(self.source_sha256)


@dataclass(frozen=True,slots=True)
class QualityCandidate:
    request_id: str
    stage: Stage
    sku: str
    country: str
    channel: str
    text: str
    spans: tuple[FactSpan,...]
    sections_json: str='{}'

    def __post_init__(self):
        for v in (self.request_id,self.sku,self.country,self.channel):_id(v)
        _require(isinstance(self.stage,Stage) and re.fullmatch(r'CI-SKU-\d{4}',self.sku) is not None
            and self.country in {'US','CA','GB','AU'} and self.channel in CHANNEL_FIELDS,'INVALID_QUALITY_SCOPE')
        _require(type(self.text) is str and 0<len(self.text)<=8192 and unicodedata.normalize('NFC',self.text)==self.text
            and type(self.spans) is tuple and len(self.spans)<=64 and all(type(s) is FactSpan for s in self.spans),
            'INVALID_QUALITY_CANDIDATE')
        previous=0
        for span in self.spans:
            _require(previous<=span.start<span.end<=len(self.text),'QUALITY_SPAN_OVERLAP_OR_BOUNDS');previous=span.end
        _require(type(self.sections_json) is str and len(self.sections_json.encode())<=65536,'INVALID_QUALITY_SECTIONS')
        sections=json.loads(self.sections_json)
        _require(type(sections) is dict and len(sections)<=16 and all(type(k) is str and type(v) is str for k,v in sections.items())
            and canonical_json_bytes(sections).decode()==self.sections_json,'INVALID_QUALITY_SECTIONS')
        self.sha256

    @property
    def sha256(self):return _hash(dict(version=VERSION,request_id=self.request_id,stage=self.stage.value,sku=self.sku,
        country=self.country,channel=self.channel,text=self.text,sections_json=self.sections_json,
        spans=[dict(start=s.start,end=s.end,source_id=s.source_id,field=s.field,source_sha256=s.source_sha256) for s in self.spans]))


@dataclass(frozen=True,slots=True)
class RuleCheck:
    rule_id: str
    state: CheckState
    reason_code: str


@dataclass(frozen=True,slots=True)
class QualityReport:
    candidate_sha256: str
    evidence_sha256: str
    checked_at: datetime
    not_before: datetime
    checks: tuple[RuleCheck,...]
    decision: Decision
    sha256: str


def check_candidate(candidate,sources,*,checked_at,not_before):
    _require(type(candidate) is QualityCandidate and type(sources) is tuple and len(sources)<=64
        and all(type(s) is SourceFact for s in sources),'INVALID_QUALITY_INPUT')
    now=_utc(checked_at);floor=_utc(not_before);_require(floor<=now,'INVALID_QUALITY_WINDOW')
    by_id={s.source_id:s for s in sources};_require(len(by_id)==len(sources),'DUPLICATE_QUALITY_SOURCE')
    results={rid:RuleCheck(rid,CheckState.UNAVAILABLE,'CHECKER_NOT_IMPLEMENTED') for rid in RULE_IDS}
    def set_result(rid,state,reason):results[rid]=RuleCheck(rid,state,reason)
    for rid in IMPLEMENTED:set_result(rid,CheckState.INCONCLUSIVE,'APPLICABILITY_OR_COVERAGE_UNPROVEN')
    used=[];pin_error=False;scope_error=False;value_error=False;fresh_error=False;denied=False
    for span in candidate.spans:
        source=by_id.get(span.source_id)
        if source is None:pin_error=True;continue
        used.append(source);fields=json.loads(source.fields_json)
        pin_error|=span.source_sha256!=source.sha256 or span.field not in fields
        scope_error|=source.sku!=candidate.sku
        value_error|=fields.get(span.field)!=candidate.text[span.start:span.end]
        fresh_error|=not (floor<=source.observed_at<=now<source.valid_until)
        denied|=not source.access_allowed
    if candidate.spans:
        set_result('G10',CheckState.FAIL if pin_error else CheckState.PASS,'CITATION_PIN_CONFLICT' if pin_error else 'EXACT_CITATION_PINS')
        set_result('G03',CheckState.FAIL if scope_error else CheckState.INCONCLUSIVE if pin_error else CheckState.PASS,
            'CROSS_SKU_SOURCE' if scope_error else 'SOURCE_UNRESOLVED' if pin_error else 'EXACT_SKU_SCOPE')
        set_result('G07',CheckState.FAIL if fresh_error or denied else CheckState.INCONCLUSIVE if pin_error else CheckState.PASS,
            'SOURCE_INELIGIBLE_OR_STALE' if fresh_error or denied else 'SOURCE_UNRESOLVED' if pin_error else 'SUPPLIED_SOURCE_CURRENT')
        covered=[False]*len(candidate.text)
        for span in candidate.spans:
            for i in range(span.start,span.end):covered[i]=True
        full=all(covered[i] or char.isspace() for i,char in enumerate(candidate.text)) and not json.loads(candidate.sections_json)
        bad=pin_error or scope_error or value_error or fresh_error or denied
        set_result('G01',CheckState.FAIL if bad else CheckState.PASS if full else CheckState.INCONCLUSIVE,
            'FACT_SUPPORT_CONFLICT' if bad else 'EXACT_FULL_TEXT_SUPPORT' if full else 'UNCHECKED_TEXT_OR_SECTIONS')
        identity=[s for s in candidate.spans if s.field in {'product_name','full_size_ml','size_value'}]
        if identity:
            identity_bad=False
            for span in identity:
                source=by_id.get(span.source_id)
                identity_bad|=source is None or source.sku!=candidate.sku or source.sha256!=span.source_sha256
                if source:identity_bad|=json.loads(source.fields_json).get(span.field)!=candidate.text[span.start:span.end]
            set_result('A01',CheckState.FAIL if identity_bad else CheckState.PASS,
                'PRODUCT_FACT_MISMATCH' if identity_bad else 'EXACT_PRODUCT_FACT')
    else:
        for rid in ('A01','G01','G03','G07','G10'):set_result(rid,CheckState.INCONCLUSIVE,'FACT_OR_CITATION_COVERAGE_UNPROVEN')
    if candidate.stage in {Stage.GENERATION,Stage.REFINEMENT}:
        sections=json.loads(candidate.sections_json);required=set(CHANNEL_FIELDS[candidate.channel])
        complete=required==set(sections) and all(value.strip() for value in sections.values())
        set_result('U06',CheckState.PASS if complete else CheckState.FAIL,
            'CHANNEL_STRUCTURE_COMPLETE_CONTENT_UNVERIFIED' if complete else 'CHANNEL_FIELDS_MISSING_OR_EXTRA')
    checks=tuple(results[rid] for rid in RULE_IDS)
    blocked=any(c.state is CheckState.FAIL and c.rule_id!='U06' for c in checks)
    decision=Decision.BLOCK if blocked else Decision.NEEDS_REVIEW if any(c.state is not CheckState.PASS for c in checks) else Decision.PASS
    evidence_hash=_hash([dict(source_id=s.source_id,sha256=s.sha256) for s in sorted(sources,key=lambda s:s.source_id)])
    digest=_hash(dict(version=VERSION,candidate=candidate.sha256,evidence=evidence_hash,checked_at=now.isoformat(),
        not_before=floor.isoformat(),checks=[dict(rule_id=c.rule_id,state=c.state.value,reason=c.reason_code) for c in checks],decision=decision.value))
    return QualityReport(candidate.sha256,evidence_hash,now,floor,checks,decision,digest)


def require_release(candidate,sources,report,*,checked_at,not_before):
    """Recompute at release; reports/benchmark scores are never bearer permission."""
    _require(type(report) is QualityReport,'QUALITY_REPORT_REQUIRED')
    original=check_candidate(candidate,sources,checked_at=report.checked_at,not_before=report.not_before)
    _require(report==original,'QUALITY_REPORT_BINDING_CONFLICT')
    _require(_utc(checked_at)>=report.checked_at and _utc(not_before)>=report.not_before,'QUALITY_RELEASE_WINDOW_REGRESSED')
    fresh=check_candidate(candidate,sources,checked_at=checked_at,not_before=not_before)
    _require(fresh.candidate_sha256==report.candidate_sha256 and fresh.evidence_sha256==report.evidence_sha256
        and fresh.decision is Decision.PASS,'QUALITY_RELEASE_WITHHELD')
    return fresh
