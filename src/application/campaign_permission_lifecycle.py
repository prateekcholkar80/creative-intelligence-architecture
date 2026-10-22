"""Pure P6 controlled lifecycle proposals; no authentication or cloud writes.

Typed identity/grants/reviews/states are supplied observations, not proof. A
trusted writer must independently authenticate IAP, read grants/head/history,
verify review provenance and atomically fence/audit these proposals before use.
"""
from __future__ import annotations
from dataclasses import dataclass,replace
from datetime import datetime
from enum import Enum

from src.application.identity import VerifiedIdentity,IapSettings
from src.application.campaign_acceptance_contract import PolicyStatus,_utc,_hash,_ordered
from src.application.campaign_permission_storage import (
    PermissionState,MAX_HISTORY,encode_record,head_row,decode_state,_kind,_identity,_review,_encode,
)
from src.application.workflow_contract import _id,_digest,_require


class Action(str,Enum):
    CREATE_DRAFT='CREATE_DRAFT'
    REPLACE_DRAFT='REPLACE_DRAFT'
    APPROVE='APPROVE'
    REVOKE='REVOKE'


@dataclass(frozen=True,slots=True)
class ReviewerGrant:
    grant_id: str
    version: str
    expected_iap_audience: str
    principal_ids: tuple[str,...]
    kinds: tuple[str,...]
    effective_at: datetime
    valid_until: datetime

    def __post_init__(self):
        _id(self.grant_id);_id(self.version);IapSettings(self.expected_iap_audience)
        _ordered(self.principal_ids);_ordered(self.kinds)
        _require(set(self.kinds)<={'CLAIM','OFFER','EVIDENCE'},'INVALID_PERMISSION_REVIEW_GRANT')
        object.__setattr__(self,'effective_at',_utc(self.effective_at))
        object.__setattr__(self,'valid_until',_utc(self.valid_until))
        _require(self.effective_at<self.valid_until,'INVALID_PERMISSION_REVIEW_GRANT')

    @property
    def sha256(self):
        return _hash(dict(grant_id=self.grant_id,version=self.version,
            expected_iap_audience=self.expected_iap_audience,principal_ids=list(self.principal_ids),
            kinds=list(self.kinds),effective_at=self.effective_at.isoformat(),valid_until=self.valid_until.isoformat()))


def reviewed_content_sha256(record):
    """Bind full content AND proposed eligibility dates/version, excluding status/signoff.

    Embedded claim evidence keeps its full independent review metadata. Parent,
    revision and actor metadata belong to the lifecycle proposal, not copy review.
    """
    kind=_kind(record);review=_review(record,kind)
    data=_encode(record);data.pop('review' if kind=='EVIDENCE' else 'approval')
    return _hash(dict(contract_version='campaign-permission-review-v1',kind=kind,
        content=data,permission_window=dict(version=review.version,
            effective_at=review.effective_at.isoformat(),valid_until=review.valid_until.isoformat())))


@dataclass(frozen=True,slots=True)
class ReviewCapture:
    review_id: str
    grant_sha256: str
    principal_id: str
    kind: str
    record_id: str
    content_sha256: str
    review_evidence_sha256: str
    reviewed_at: datetime
    valid_until: datetime

    def __post_init__(self):
        for value in (self.review_id,self.principal_id,self.record_id):_id(value)
        for value in (self.grant_sha256,self.content_sha256,self.review_evidence_sha256):_digest(value)
        _require(type(self.kind) is str and self.kind in {'CLAIM','OFFER','EVIDENCE'},'INVALID_PERMISSION_REVIEW')
        for name in ('reviewed_at','valid_until'):object.__setattr__(self,name,_utc(getattr(self,name)))
        _require(self.reviewed_at<self.valid_until,'INVALID_PERMISSION_REVIEW')

    @property
    def sha256(self):
        return _hash(dict(review_id=self.review_id,grant_sha256=self.grant_sha256,
            principal_id=self.principal_id,kind=self.kind,record_id=self.record_id,
            content_sha256=self.content_sha256,review_evidence_sha256=self.review_evidence_sha256,
            reviewed_at=self.reviewed_at.isoformat(),valid_until=self.valid_until.isoformat()))


@dataclass(frozen=True,slots=True)
class PermissionProposal:
    action: Action
    kind: str
    record_id: str
    expected_revision: int
    expected_record_sha256: str | None
    proposed_record: object
    changed_by: str
    changed_at: datetime
    grant_sha256: str
    review_sha256: str | None
    reason_code: str
    sha256: str


def _validated_current(state):
    _require(isinstance(state,PermissionState),'INVALID_PERMISSION_CURRENT_STATE')
    # Revalidate complete typed supplied state so manually assembled objects
    # cannot evade revision/chain/identity checks. This does not prove freshness.
    rows=tuple(encode_record(r,recorded_at=state.recorded_at) for r in state.history)
    _require(bool(rows),'INVALID_PERMISSION_CURRENT_STATE')
    rebuilt=decode_state(head_row(rows[-1]),rows,kind=state.kind,record_id=state.record_id)
    _require(rebuilt.current==state.current,'INVALID_PERMISSION_CURRENT_STATE')
    return state


def propose_permission(action,*,identity,grant,candidate=None,current=None,review=None,
                       checked_at,reason_code):
    _require(isinstance(action,Action) and isinstance(identity,VerifiedIdentity)
             and isinstance(grant,ReviewerGrant),'INVALID_PERMISSION_LIFECYCLE_INPUT')
    _id(reason_code);now=_utc(checked_at)
    _require(identity.audience==grant.expected_iap_audience
        and identity.issued_at<=now.timestamp()<identity.expires_at
        and identity.principal.principal_id in grant.principal_ids
        and grant.effective_at<=now<grant.valid_until,'PERMISSION_REVIEWER_DENIED')
    if current is not None:
        current=_validated_current(current)
        _require(current.recorded_at<=now,'PERMISSION_RECORD_FROM_FUTURE')
    if action is Action.CREATE_DRAFT:
        _require(current is None and candidate is not None,'PERMISSION_CREATE_CONFLICT')
    else:_require(current is not None,'PERMISSION_CURRENT_REQUIRED')
    if action is Action.REVOKE:
        _require(candidate is None and review is None,'INVALID_PERMISSION_REVOCATION_INPUT')
        kind=current.kind;record=current.current;old=_review(record,kind)
        _require(old.status is PolicyStatus.APPROVED,'PERMISSION_REVOCATION_DENIED')
    else:
        kind=_kind(candidate);record=candidate;old=_review(record,kind)
        _require(old.status is PolicyStatus.DRAFT and old.approved_by is None
            and old.approved_at is None and old.revision==1 and old.parent_sha256 is None,
            'PERMISSION_UNREVIEWED_CANDIDATE_REQUIRED')
    record_id=_identity(record,kind)
    _require(kind in grant.kinds,'PERMISSION_REVIEWER_KIND_DENIED')
    if current is not None:
        _require(current.kind==kind and current.record_id==record_id,'PERMISSION_LIFECYCLE_SCOPE_MISMATCH')
        prior=_review(current.current,kind)
        _require(prior.status is not PolicyStatus.REVOKED,'PERMISSION_REVOCATION_TERMINAL')
        if kind in {'CLAIM','EVIDENCE'}:
            _require(current.current.sku==record.sku and current.current.formula_version==record.formula_version,
                     'PERMISSION_IDENTITY_CHANGED')
        if action is Action.REPLACE_DRAFT:
            _require(prior.status is PolicyStatus.DRAFT,'PERMISSION_DRAFT_REPLACEMENT_DENIED')
    revision=1 if current is None else _review(current.current,kind).revision+1
    _require(revision<=MAX_HISTORY,'PERMISSION_HISTORY_LIMIT')
    parent=None if current is None else current.current.sha256
    if action is Action.APPROVE:
        _require(isinstance(review,ReviewCapture) and review.kind==kind and review.record_id==record_id
            and review.principal_id==identity.principal.principal_id and review.grant_sha256==grant.sha256
            and review.content_sha256==reviewed_content_sha256(record)
            and grant.effective_at<=review.reviewed_at<=now<review.valid_until<=grant.valid_until,
            'PERMISSION_EXACT_REVIEW_REQUIRED')
        _require(now<=old.effective_at<old.valid_until,'PERMISSION_APPROVAL_BACKDATE_OR_EXPIRY')
        if kind=='CLAIM':
            for pin in record.evidence:pin.review.require_current(now)
        updated=replace(old,revision=revision,parent_sha256=parent,status=PolicyStatus.APPROVED,
                        approved_by=identity.principal.principal_id,approved_at=now)
    elif action is Action.REVOKE:
        updated=replace(old,revision=revision,parent_sha256=parent,status=PolicyStatus.REVOKED)
    else:
        _require(review is None,'UNEXPECTED_PERMISSION_REVIEW')
        updated=replace(old,revision=revision,parent_sha256=parent)
    proposed=replace(record,**{'review' if kind=='EVIDENCE' else 'approval':updated})
    if current is not None:
        rows=tuple(encode_record(r,recorded_at=now) for r in current.history)+(encode_record(proposed,recorded_at=now),)
        decode_state(head_row(rows[-1]),rows,kind=kind,record_id=record_id)
    payload=dict(contract_version='campaign-permission-lifecycle-v1',action=action.value,kind=kind,
        record_id=record_id,expected_revision=revision-1,expected_record_sha256=parent,
        proposed_record_sha256=proposed.sha256,changed_by=identity.principal.principal_id,
        changed_at=now.isoformat(),grant_sha256=grant.sha256,
        review_sha256=review.sha256 if review else None,reason_code=reason_code)
    return PermissionProposal(action,kind,record_id,revision-1,parent,proposed,
        identity.principal.principal_id,now,grant.sha256,review.sha256 if review else None,reason_code,_hash(payload))
