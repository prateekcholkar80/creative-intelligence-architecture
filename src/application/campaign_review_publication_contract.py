"""Pure explicit operator policy and reviewer-record transitions; no live authority.

Policy and Rows are supplied captures, not authentication or authoritative reads.
No bootstrap owner/ADC membership is inferred. No grant or review is published.
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re
from src.application.identity import VerifiedIdentity,IapSettings
from src.application.campaign_acceptance_contract import PolicyStatus,_utc,_hash
from src.application.campaign_permission_lifecycle import ReviewerGrant,ReviewCapture,reviewed_content_sha256
from src.application.campaign_permission_publication import Rows
from src.application.campaign_permission_storage import decode_state,_review
from src.application.campaign_review_storage import ReviewVersion,decode_review,review_head
from src.application.workflow_contract import _require,_id


class ReviewAction(str,Enum):
    CREATE_GRANT='CREATE_GRANT'
    REVISE_GRANT='REVISE_GRANT'
    REVOKE_GRANT='REVOKE_GRANT'
    CREATE_REVIEW='CREATE_REVIEW'
    REVOKE_REVIEW='REVOKE_REVIEW'


def _principals(values):
    _require(type(values) is tuple and len(values)<=256
        and all(type(v) is str and re.fullmatch(r'iap-v1:[0-9a-f]{64}',v) for v in values)
        and values==tuple(sorted(set(values))),
        'INVALID_REVIEW_OPERATOR_PRINCIPALS')


@dataclass(frozen=True,slots=True)
class ReviewOperatorPolicy:
    policy_id: str
    revision: int
    expected_audience: str
    grant_operator_ids: tuple[str,...]
    reviewer_principal_ids: tuple[str,...]
    review_revoker_ids: tuple[str,...]
    kinds: tuple[str,...]
    max_grant_seconds: int
    max_review_seconds: int
    effective_at: datetime
    valid_until: datetime

    def __post_init__(self):
        _id(self.policy_id);IapSettings(self.expected_audience)
        _require(type(self.revision) is int and 1<=self.revision<=1000000,'INVALID_REVIEW_OPERATOR_POLICY')
        for values in (self.grant_operator_ids,self.reviewer_principal_ids,self.review_revoker_ids):_principals(values)
        _require(bool(self.grant_operator_ids) and bool(self.reviewer_principal_ids),'EMPTY_REVIEW_OPERATOR_POLICY')
        _require(type(self.kinds) is tuple and all(type(k) is str for k in self.kinds)
            and self.kinds==tuple(sorted(set(self.kinds))) and self.kinds and set(self.kinds)<={'CLAIM','OFFER','EVIDENCE'},'INVALID_REVIEW_OPERATOR_KINDS')
        _require(all(type(v) is int and 1<=v<=31536000 for v in (self.max_grant_seconds,self.max_review_seconds)),
            'INVALID_REVIEW_OPERATOR_LIFETIMES')
        object.__setattr__(self,'effective_at',_utc(self.effective_at));object.__setattr__(self,'valid_until',_utc(self.valid_until))
        _require(self.effective_at<self.valid_until,'INVALID_REVIEW_OPERATOR_WINDOW')

    @property
    def sha256(self):
        return _hash(dict(version='campaign-review-operator-policy-v1',policy_id=self.policy_id,revision=self.revision,
            audience=self.expected_audience,grant_operators=list(self.grant_operator_ids),reviewers=list(self.reviewer_principal_ids),
            revokers=list(self.review_revoker_ids),kinds=list(self.kinds),max_grant_seconds=self.max_grant_seconds,
            max_review_seconds=self.max_review_seconds,effective_at=self.effective_at.isoformat(),valid_until=self.valid_until.isoformat()))


def structural_records(source,now):
    """Allow expired ACTIVE records to be renewed/revoked; revocation is terminal."""
    _require(isinstance(source,Rows) and source.kind in {'GRANT','REVIEW'},'INVALID_REVIEW_PUBLICATION_ROWS')
    head,history=source.decoded()
    if head is None:return ()
    records=tuple(decode_review(row,source.kind) for row in history)
    _require(head==review_head(history[-1]) and tuple(r.revision for r in records)==tuple(range(1,len(records)+1))
        and all(r.record_id==source.record_id and r.recorded_at<=now for r in records),'REVIEW_PUBLICATION_PARTIAL_STATE')
    for before,after in zip(records,records[1:]):
        _require(after.parent_sha256==before.sha256 and after.recorded_at>=before.recorded_at
            and before.status!='REVOKED','REVIEW_PUBLICATION_CHAIN_CONFLICT')
        if source.kind=='REVIEW':_require(after.value==before.value,'CAMPAIGN_REVIEW_CAPTURE_IMMUTABLE')
        else:_require(after.value.expected_iap_audience==before.value.expected_iap_audience,'REVIEW_GRANT_AUDIENCE_IMMUTABLE')
    return records


@dataclass(frozen=True,slots=True)
class ReviewProposal:
    action: ReviewAction
    record: ReviewVersion
    previous: ReviewVersion | None
    grant_record: ReviewVersion | None
    policy_sha256: str
    sha256: str


def propose_review_publication(action,*,identity,policy,target,value=None,grant=None,permission=None,checked_at,reason_code):
    now=_utc(checked_at);_id(reason_code)
    _require(isinstance(action,ReviewAction) and isinstance(policy,ReviewOperatorPolicy)
        and isinstance(identity,VerifiedIdentity),'INVALID_REVIEW_PUBLICATION_REQUEST')
    _require(identity.audience==policy.expected_audience and identity.issued_at<=now.timestamp()<identity.expires_at
        and policy.effective_at<=now<policy.valid_until,'REVIEW_PUBLICATION_AUTHORITY_DENIED')
    actor=identity.principal.principal_id
    expected_kind='GRANT' if action in {ReviewAction.CREATE_GRANT,ReviewAction.REVISE_GRANT,ReviewAction.REVOKE_GRANT} else 'REVIEW'
    _require(isinstance(target,Rows) and target.kind==expected_kind,'REVIEW_PUBLICATION_TARGET_KIND')
    records=structural_records(target,now);old=records[-1] if records else None
    create=action in {ReviewAction.CREATE_GRANT,ReviewAction.CREATE_REVIEW}
    _require((old is None)==create,'REVIEW_PUBLICATION_CREATE_OR_CURRENT_CONFLICT')
    if old:_require(old.status=='ACTIVE' and old.revision<64,'REVIEW_PUBLICATION_TERMINAL_OR_LIMIT')
    source_grant=None
    if expected_kind=='GRANT':
        _require(actor in policy.grant_operator_ids and grant is None and permission is None,
            'REVIEW_GRANT_OPERATOR_DENIED')
        if action is ReviewAction.REVOKE_GRANT:
            _require(value is None,'REVIEW_REVOCATION_VALUE_DENIED');candidate=old.value
        else:
            _require(type(value) is ReviewerGrant and value.grant_id==target.record_id
                and value.expected_iap_audience==policy.expected_audience
                and set(value.principal_ids)<=set(policy.reviewer_principal_ids)
                and set(value.kinds)<=set(policy.kinds),'REVIEW_GRANT_SCOPE_DENIED')
            _require(now<value.valid_until and (value.valid_until-value.effective_at).total_seconds()<=policy.max_grant_seconds
                and value.valid_until<=policy.valid_until,'REVIEW_GRANT_WINDOW_DENIED')
            if old:_require(value.expected_iap_audience==old.value.expected_iap_audience
                and value.version not in {r.value.version for r in records}
                and value.sha256 not in {r.value.sha256 for r in records},'REVIEW_GRANT_REVISION_DENIED')
            candidate=value
        _require(candidate.expected_iap_audience==policy.expected_audience,'REVIEW_GRANT_SCOPE_DENIED')
    else:
        _require(isinstance(grant,Rows) and grant.kind=='GRANT','REVIEW_SOURCE_GRANT_REQUIRED')
        grants=structural_records(grant,now)
        _require(bool(grants),'REVIEW_SOURCE_GRANT_REQUIRED');source_grant=grants[-1]
        _require(source_grant.value.expected_iap_audience==policy.expected_audience,'REVIEW_SOURCE_GRANT_SCOPE')
        if action is ReviewAction.REVOKE_REVIEW:
            _require(value is None and permission is None,'REVIEW_REVOCATION_VALUE_DENIED')
            _require(actor in policy.review_revoker_ids or (actor==old.value.principal_id and actor in policy.reviewer_principal_ids),
                'REVIEW_REVOCATION_ACTOR_DENIED')
            _require(any(g.value.sha256==old.value.grant_sha256 for g in grants),'REVIEW_SOURCE_GRANT_SCOPE')
            candidate=old.value
        else:
            _require(type(value) is ReviewCapture and value.review_id==target.record_id
                and actor in policy.reviewer_principal_ids and value.principal_id==actor
                and value.grant_sha256==source_grant.value.sha256,'REVIEW_CAPTURE_ACTOR_OR_GRANT_DENIED')
            g=source_grant.value
            _require(source_grant.status=='ACTIVE' and g.effective_at<=now<g.valid_until
                and actor in g.principal_ids and value.kind in g.kinds and value.kind in policy.kinds,
                'REVIEW_CAPTURE_GRANT_NOT_CURRENT')
            _require(isinstance(permission,Rows) and permission.kind==value.kind
                and permission.record_id==value.record_id,'REVIEW_CAPTURE_PERMISSION_REQUIRED')
            head,history=permission.decoded()
            state=decode_state(head,history,kind=permission.kind,record_id=permission.record_id)
            approval=_review(state.current,permission.kind)
            _require(state.recorded_at<=now and approval.status is PolicyStatus.DRAFT
                and reviewed_content_sha256(state.current)==value.content_sha256,'REVIEW_CAPTURE_CONTENT_CONFLICT')
            _require(value.reviewed_at==now and now<value.valid_until<=min(g.valid_until,approval.valid_until,policy.valid_until)
                and (value.valid_until-now).total_seconds()<=policy.max_review_seconds,'REVIEW_CAPTURE_WINDOW_DENIED')
            candidate=value
    revoked=action in {ReviewAction.REVOKE_GRANT,ReviewAction.REVOKE_REVIEW}
    record=ReviewVersion(candidate,1 if old is None else old.revision+1,None if old is None else old.sha256,
        'REVOKED' if revoked else 'ACTIVE',actor,now)
    digest=_hash(dict(version='campaign-review-transition-v1',action=action.value,record_sha256=record.sha256,
        policy_sha256=policy.sha256,grant_record_sha256=source_grant.sha256 if source_grant else None,reason_code=reason_code))
    return ReviewProposal(action,record,old,source_grant,policy.sha256,digest)
