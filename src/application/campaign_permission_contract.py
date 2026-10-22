"""P6 pure supplied claim/offer permissions, not approval or scientific review.

No cloud reads/writes, semantic entailment checks, authentication or publication.
Authoritative adapters must verify records, evidence and current access separately.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
import re

from src.application.campaign_acceptance_contract import PolicyStatus, _utc, _hash, _ordered
from src.application.workflow_contract import _id, _digest, _require, _numeric

CHANNELS = {'META', 'KLAVIYO_EMAIL', 'TIKTOK', 'PINTEREST'}
AUDIENCES = {'NEW', 'RETURNING'}


class EvidenceKind(str, Enum):
    PRODUCT_FACT = 'PRODUCT_FACT'
    WEBSITE_SUMMARY = 'WEBSITE_SUMMARY'
    FINISHED_PRODUCT_CLINICAL = 'FINISHED_PRODUCT_CLINICAL'
    CONSUMER_STUDY = 'CONSUMER_STUDY'
    INGREDIENT_RESEARCH = 'INGREDIENT_RESEARCH'
    DERMATOLOGIST_RECOMMENDATION = 'DERMATOLOGIST_RECOMMENDATION'


class ClaimKind(str, Enum):
    PRODUCT_FACT = 'PRODUCT_FACT'
    COSMETIC_BENEFIT = 'COSMETIC_BENEFIT'
    CLINICAL_RESULT = 'CLINICAL_RESULT'
    CONSUMER_PERCEPTION = 'CONSUMER_PERCEPTION'
    DERMATOLOGIST_RECOMMENDATION = 'DERMATOLOGIST_RECOMMENDATION'


def _text(value):
    _require(isinstance(value, str) and 0 < len(value) <= 4096
             and value == value.strip() and not any(ord(c) < 32 for c in value),
             'INVALID_PERMISSION_TEXT')


def _texts(values, *, empty=False):
    _require(type(values) is tuple and len(values) <= 32 and (values or empty),
             'INVALID_PERMISSION_TEXTS')
    for value in values: _text(value)
    _require(values == tuple(sorted(set(values))), 'INVALID_PERMISSION_TEXTS')


def _sku(value):
    _require(isinstance(value, str) and re.fullmatch(r'CI-SKU-\d{4}',value) is not None
             and 1 <= int(value[-4:]) <= 20, 'INVALID_PERMISSION_SKU')


def _decimal(value):
    # Fixed NUMERIC scale is context-independent and canonical across 97/97.0.
    return '0' if value == 0 else format(value, '.9f').rstrip('0').rstrip('.')


def _scope(countries, channels, audiences):
    for values in (countries, channels, audiences): _ordered(values)
    _require(all(len(c)==2 and c.isascii() and c.isalpha() and c.isupper()
                 for c in countries), 'INVALID_PERMISSION_COUNTRY')
    _require(set(channels) <= CHANNELS and set(audiences) <= AUDIENCES,
             'INVALID_PERMISSION_SCOPE')


@dataclass(frozen=True, slots=True)
class Approval:
    version: str
    revision: int
    parent_sha256: str | None
    status: PolicyStatus
    approved_by: str | None
    approved_at: datetime | None
    effective_at: datetime
    valid_until: datetime

    def __post_init__(self):
        _id(self.version)
        _require(type(self.revision) is int and self.revision >= 1
                 and isinstance(self.status,PolicyStatus), 'INVALID_PERMISSION_REVISION')
        _require((self.revision == 1) == (self.parent_sha256 is None), 'INVALID_PERMISSION_PARENT')
        if self.parent_sha256 is not None: _digest(self.parent_sha256)
        object.__setattr__(self,'effective_at',_utc(self.effective_at))
        object.__setattr__(self,'valid_until',_utc(self.valid_until))
        _require(self.effective_at < self.valid_until, 'INVALID_PERMISSION_WINDOW')
        if self.approved_by is None or self.approved_at is None:
            _require(self.approved_by is None and self.approved_at is None
                     and self.status is PolicyStatus.DRAFT, 'PERMISSION_APPROVAL_REQUIRED')
        else:
            _id(self.approved_by)
            object.__setattr__(self,'approved_at',_utc(self.approved_at))
            _require(self.approved_at <= self.effective_at, 'INVALID_PERMISSION_APPROVAL_TIME')

    def require_current(self, now):
        _require(self.status is PolicyStatus.APPROVED
                 and self.effective_at <= _utc(now) < self.valid_until,
                 'PERMISSION_NOT_CURRENT')

    def payload(self):
        return dict(version=self.version, revision=self.revision,parent_sha256=self.parent_sha256,
            status=self.status.value, approved_by=self.approved_by,
            approved_at=self.approved_at.isoformat() if self.approved_at else None,
            effective_at=self.effective_at.isoformat(),valid_until=self.valid_until.isoformat())


@dataclass(frozen=True, slots=True)
class EvidencePin:
    evidence_id: str
    sku: str
    formula_version: str
    kind: EvidenceKind
    content_sha256: str
    review: Approval
    endpoint: str | None = None
    sample_size: int | None = None
    duration_days: int | None = None
    usage_conditions: str | None = None
    population: str | None = None
    statistic: Decimal | None = None
    statistic_meaning: str | None = None
    permission_reference_sha256: str | None = None

    def __post_init__(self):
        _id(self.evidence_id); _sku(self.sku); _id(self.formula_version); _digest(self.content_sha256)
        _require(isinstance(self.kind,EvidenceKind) and isinstance(self.review,Approval),
                 'INVALID_PERMISSION_EVIDENCE')
        study = (self.endpoint,self.sample_size,self.duration_days,self.usage_conditions,self.population,
                 self.statistic,self.statistic_meaning)
        if self.kind in {EvidenceKind.FINISHED_PRODUCT_CLINICAL,EvidenceKind.CONSUMER_STUDY}:
            _text(self.endpoint); _text(self.usage_conditions); _text(self.population)
            _require(type(self.sample_size) is int and self.sample_size > 0
                     and type(self.duration_days) is int and self.duration_days > 0,
                     'INVALID_PERMISSION_STUDY')
            _numeric(self.statistic)
            _require(0 <= self.statistic <= 100 and self.statistic_meaning in
                     {'PARTICIPANTS_PERCENT','ENDPOINT_CHANGE_PERCENT'}, 'INVALID_PERMISSION_STATISTIC')
        else:
            _require(all(v is None for v in study), 'UNEXPECTED_PERMISSION_STUDY')
        if self.kind is EvidenceKind.DERMATOLOGIST_RECOMMENDATION:
            _require(self.permission_reference_sha256 is not None, 'ENDORSEMENT_PERMISSION_REQUIRED')
        if self.permission_reference_sha256 is not None: _digest(self.permission_reference_sha256)

    @property
    def sha256(self):
        return _hash(dict(evidence_id=self.evidence_id,sku=self.sku,formula_version=self.formula_version,
            kind=self.kind.value,content_sha256=self.content_sha256,review=self.review.payload(),
            endpoint=self.endpoint,sample_size=self.sample_size,duration_days=self.duration_days,
            usage_conditions=self.usage_conditions,population=self.population,
            statistic=_decimal(self.statistic) if self.statistic is not None else None,
            statistic_meaning=self.statistic_meaning,permission_reference_sha256=self.permission_reference_sha256))


@dataclass(frozen=True, slots=True)
class ClaimPermission:
    claim_id: str
    sku: str
    formula_version: str
    kind: ClaimKind
    approval: Approval
    evidence: tuple[EvidencePin, ...]
    wording: tuple[str, ...]
    qualifications: tuple[str, ...]
    countries: tuple[str, ...]
    channels: tuple[str, ...]
    audiences: tuple[str, ...]

    def __post_init__(self):
        _id(self.claim_id); _sku(self.sku); _id(self.formula_version)
        _require(isinstance(self.kind,ClaimKind) and isinstance(self.approval,Approval),
                 'INVALID_CLAIM_PERMISSION')
        _texts(self.wording); _texts(self.qualifications,empty=True)
        if self.kind in {ClaimKind.CLINICAL_RESULT, ClaimKind.CONSUMER_PERCEPTION}:
            _require(bool(self.qualifications), 'CLAIM_QUALIFICATIONS_REQUIRED')
        _scope(self.countries,self.channels,self.audiences)
        _require(type(self.evidence) is tuple and 1 <= len(self.evidence) <= 32
                 and all(isinstance(e,EvidencePin) for e in self.evidence)
                 and tuple(e.evidence_id for e in self.evidence)
                 == tuple(sorted({e.evidence_id for e in self.evidence})), 'INVALID_CLAIM_EVIDENCE')
        _require(all(e.sku==self.sku and e.formula_version==self.formula_version
                     for e in self.evidence), 'CLAIM_EVIDENCE_SCOPE_MISMATCH')
        allowed = {
            ClaimKind.PRODUCT_FACT:{EvidenceKind.PRODUCT_FACT},
            ClaimKind.COSMETIC_BENEFIT:{EvidenceKind.FINISHED_PRODUCT_CLINICAL,EvidenceKind.CONSUMER_STUDY},
            ClaimKind.CLINICAL_RESULT:{EvidenceKind.FINISHED_PRODUCT_CLINICAL},
            ClaimKind.CONSUMER_PERCEPTION:{EvidenceKind.CONSUMER_STUDY},
            ClaimKind.DERMATOLOGIST_RECOMMENDATION:{EvidenceKind.DERMATOLOGIST_RECOMMENDATION},
        }
        _require(any(e.kind in allowed[self.kind] for e in self.evidence),
                 'CLAIM_SUBSTANTIATION_REQUIRED')

    @property
    def sha256(self):
        return _hash(dict(contract_version='campaign-permission-v1',claim_id=self.claim_id,
            sku=self.sku,formula_version=self.formula_version,kind=self.kind.value,
            approval=self.approval.payload(),evidence=[e.sha256 for e in self.evidence],
            wording=list(self.wording),qualifications=list(self.qualifications),
            countries=list(self.countries),channels=list(self.channels),audiences=list(self.audiences)))

    def validate_use(self, *, sku, formula_version, wording, qualifications,
                     country, channel, audience, checked_at):
        self.approval.require_current(checked_at)
        for e in self.evidence: e.review.require_current(checked_at)
        _require(sku==self.sku and formula_version==self.formula_version
                 and country in self.countries and channel in self.channels
                 and audience in self.audiences, 'CLAIM_USE_SCOPE_DENIED')
        _require(wording in self.wording and type(qualifications) is tuple
                 and qualifications==self.qualifications, 'CLAIM_WORDING_DENIED')
        return self.sha256


@dataclass(frozen=True, slots=True)
class OfferPermission:
    offer_id: str
    approval: Approval
    countries: tuple[str, ...]
    channels: tuple[str, ...]
    audiences: tuple[str, ...]
    skus: tuple[str, ...]
    currency: str
    threshold: Decimal
    discount_percent: Decimal
    gift_skus: tuple[str, ...]
    code: str | None
    automatic: bool
    stacking_allowed: bool
    subscription_eligible: bool
    shipping_service: str | None
    qualifications: tuple[str, ...]

    def __post_init__(self):
        _id(self.offer_id)
        _require(isinstance(self.approval,Approval), 'INVALID_OFFER_APPROVAL')
        _scope(self.countries,self.channels,self.audiences)
        _ordered(self.skus,maximum=20)
        for sku in self.skus: _sku(sku)
        _require(type(self.gift_skus) is tuple and self.gift_skus==tuple(sorted(set(self.gift_skus)))
                 and len(self.gift_skus)<=20, 'INVALID_OFFER_GIFTS')
        for sku in self.gift_skus: _sku(sku)
        _require(isinstance(self.currency,str)
                 and self.currency in {'USD','CAD','GBP','EUR','AUD'}, 'INVALID_OFFER_CURRENCY')
        for amount in (self.threshold,self.discount_percent): _numeric(amount)
        _require(self.threshold >= 0 and 0 <= self.discount_percent <= 100, 'INVALID_OFFER_AMOUNT')
        _require(all(type(v) is bool for v in (self.automatic,self.stacking_allowed,self.subscription_eligible)),
                 'INVALID_OFFER_FLAGS')
        _require(self.automatic == (self.code is None), 'OFFER_CODE_CONFLICT')
        if self.code is not None: _id(self.code)
        if self.shipping_service is not None: _id(self.shipping_service)
        _texts(self.qualifications)

    @property
    def sha256(self):
        return _hash(dict(contract_version='campaign-offer-v1',offer_id=self.offer_id,
            approval=self.approval.payload(),countries=list(self.countries),channels=list(self.channels),
            audiences=list(self.audiences),skus=list(self.skus),currency=self.currency,
            threshold=_decimal(self.threshold),discount_percent=_decimal(self.discount_percent),
            gift_skus=list(self.gift_skus),code=self.code,automatic=self.automatic,
            stacking_allowed=self.stacking_allowed,subscription_eligible=self.subscription_eligible,
            shipping_service=self.shipping_service,qualifications=list(self.qualifications)))

    def validate_use(self, *, candidate_offer_sha256, country, channel, audience,
                     sku, currency, is_subscription, checked_at):
        self.approval.require_current(checked_at)
        _digest(candidate_offer_sha256)
        _require(type(is_subscription) is bool, 'INVALID_OFFER_FLAGS')
        _require(candidate_offer_sha256==self.sha256 and country in self.countries
                 and channel in self.channels and audience in self.audiences and sku in self.skus
                 and currency==self.currency and (not is_subscription or self.subscription_eligible),
                 'OFFER_USE_DENIED')
        return self.sha256
