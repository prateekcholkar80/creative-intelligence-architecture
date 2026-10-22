"""Supplied reviewer/identity lifecycle fixtures, not real approval or IAP auth."""
from dataclasses import replace,FrozenInstanceError
from datetime import timedelta
import unittest
from src.application.identity import VerifiedIdentity,principal_for_subject,ISSUER
from src.application.campaign_permission_lifecycle import (
    Action,ReviewerGrant,ReviewCapture,reviewed_content_sha256,propose_permission,
)
from src.application.campaign_acceptance_contract import PolicyStatus
from src.application.campaign_permission_storage import encode_record,head_row,decode_state
from src.application.workflow_contract import ContractError
from tests.test_campaign_permission_contract import claim,offer,evidence,NOW

AUDIENCE='/projects/1042059373096/locations/asia-south1/services/ci-review-fixture'


def identity(subject='synthetic-manager'):
    return VerifiedIdentity(principal_for_subject(ISSUER,subject),ISSUER,subject,AUDIENCE,
                            int(NOW.timestamp())-30,int(NOW.timestamp())+300)


def grant():
    return ReviewerGrant('fixture-grant','v1',AUDIENCE,(identity().principal.principal_id,),
        ('CLAIM','EVIDENCE','OFFER'),NOW-timedelta(hours=1),NOW+timedelta(hours=1))


def draft(kind='CLAIM'):
    record={'CLAIM':claim(),'OFFER':offer(),'EVIDENCE':evidence()}[kind]
    key='review' if kind=='EVIDENCE' else 'approval'
    return replace(record,**{key:replace(getattr(record,key),status=PolicyStatus.DRAFT,
        approved_by=None,approved_at=None,effective_at=NOW,valid_until=NOW+timedelta(minutes=30))})


def current(record,kind):
    row=encode_record(record,recorded_at=NOW)
    return decode_state(head_row(row),(row,),kind=kind,record_id=row['record_id'])


def capture(record,kind='CLAIM',selected_grant=None):
    g=selected_grant or grant();key={'CLAIM':'claim_id','OFFER':'offer_id','EVIDENCE':'evidence_id'}[kind]
    return ReviewCapture('fixture-review',g.sha256,identity().principal.principal_id,kind,
        getattr(record,key),reviewed_content_sha256(record),'f'*64,NOW,NOW+timedelta(minutes=10))


def propose(action,**changes):
    return propose_permission(action,**(dict(identity=identity(),grant=grant(),checked_at=NOW,
                                            reason_code='FIXTURE_REVIEW')|changes))


class PermissionLifecycleTests(unittest.TestCase):
    def test_all_kinds_genesis_and_exact_review_approval_chain(self):
        for kind in ('CLAIM','OFFER','EVIDENCE'):
            candidate=draft(kind);genesis=propose(Action.CREATE_DRAFT,candidate=candidate)
            self.assertEqual(genesis.expected_revision,0)
            self.assertIsNone(genesis.expected_record_sha256)
            state=current(genesis.proposed_record,kind)
            approved=propose(Action.APPROVE,current=state,candidate=candidate,review=capture(candidate,kind))
            key='review' if kind=='EVIDENCE' else 'approval'
            metadata=getattr(approved.proposed_record,key)
            self.assertEqual(metadata.revision,2)
            self.assertEqual(metadata.parent_sha256,candidate.sha256)
            self.assertEqual(metadata.approved_by,identity().principal.principal_id)
            self.assertEqual(metadata.status,PolicyStatus.APPROVED)
            with self.assertRaises(FrozenInstanceError):approved.reason_code='other'

    def test_grant_identity_kind_audience_and_window_denied(self):
        c=draft()
        variants=[dict(identity=identity('ungranted')),
            dict(identity=replace(identity(),expires_at=int(NOW.timestamp()))),
            dict(grant=replace(grant(),valid_until=NOW)),
            dict(grant=replace(grant(),kinds=('OFFER',))),
            dict(grant=replace(grant(),expected_iap_audience=AUDIENCE+'-other'))]
        for values in variants:
            with self.assertRaises(ContractError):propose(Action.CREATE_DRAFT,candidate=c,**values)

    def test_create_requires_absent_state_and_unapproved_revision_one_candidate(self):
        c=draft()
        for values in (dict(candidate=claim()),dict(candidate=c,current=current(c,'CLAIM')),
                       dict(candidate=replace(c,approval=replace(c.approval,revision=2,parent_sha256='a'*64)))):
            with self.assertRaises(ContractError):propose(Action.CREATE_DRAFT,**values)
        with self.assertRaises(ContractError):propose(Action.APPROVE,candidate=c,review=capture(c))

    def test_content_scope_wording_dates_and_version_changes_require_new_review(self):
        c=draft();s=current(c,'CLAIM');review=capture(c)
        for values in ({'wording':('Different wording.',)},{'countries':('GB',)},
                       {'qualifications':('Different qualification.',)},
                       {'approval':replace(c.approval,valid_until=NOW+timedelta(minutes=20))},
                       {'approval':replace(c.approval,version='v2')}):
            candidate=replace(c,**values)
            with self.assertRaises(ContractError):propose(Action.APPROVE,current=s,candidate=candidate,review=review)

    def test_review_must_match_grant_actor_kind_identity_hash_and_freshness(self):
        c=draft();s=current(c,'CLAIM');r=capture(c)
        for values in ({'grant_sha256':'0'*64},{'principal_id':'other'}, {'kind':'OFFER'},
                       {'record_id':'other'},{'content_sha256':'0'*64},
                       {'valid_until':NOW},{'reviewed_at':NOW+timedelta(seconds=1)}):
            # Some malformed temporal captures fail during construction, also safe.
            with self.assertRaises(ContractError):
                candidate=replace(r,**values)
                propose(Action.APPROVE,current=s,candidate=c,review=candidate)
        with self.assertRaises(ContractError):propose(Action.APPROVE,current=s,candidate=c)

    def test_approval_cannot_backdate_or_use_expired_embedded_evidence(self):
        c=draft()
        backdated=replace(c,approval=replace(c.approval,effective_at=NOW-timedelta(seconds=1)))
        with self.assertRaises(ContractError):propose(Action.APPROVE,current=current(c,'CLAIM'),
            candidate=backdated,review=capture(backdated))
        expired=replace(evidence(),review=replace(evidence().review,valid_until=NOW))
        candidate=replace(c,evidence=(expired,))
        with self.assertRaises(ContractError):propose(Action.APPROVE,current=current(c,'CLAIM'),
            candidate=candidate,review=capture(candidate))

    def test_draft_replacement_keeps_chain_without_approval(self):
        c=draft();changed=replace(c,wording=('New draft wording.',))
        proposal=propose(Action.REPLACE_DRAFT,current=current(c,'CLAIM'),candidate=changed)
        self.assertEqual(proposal.proposed_record.approval.status,PolicyStatus.DRAFT)
        self.assertIsNone(proposal.proposed_record.approval.approved_by)
        self.assertEqual(proposal.expected_record_sha256,c.sha256)
        with self.assertRaises(ContractError):propose(Action.REPLACE_DRAFT,current=current(claim(),'CLAIM'),candidate=changed)

    def test_revoke_preserves_original_approval_and_records_distinct_actor(self):
        c=claim();proposal=propose(Action.REVOKE,current=current(c,'CLAIM'))
        self.assertEqual(proposal.proposed_record.approval.approved_by,c.approval.approved_by)
        self.assertEqual(proposal.proposed_record.approval.approved_at,c.approval.approved_at)
        self.assertEqual(proposal.proposed_record.approval.status,PolicyStatus.REVOKED)
        self.assertEqual(proposal.changed_by,identity().principal.principal_id)
        with self.assertRaises(ContractError):propose(Action.REVOKE,current=current(draft(),'CLAIM'))
        with self.assertRaises(ContractError):propose(Action.REVOKE,current=current(c,'CLAIM'),candidate=draft())

    def test_revoked_history_cannot_be_restored_or_replaced(self):
        c=claim();revoked=propose(Action.REVOKE,current=current(c,'CLAIM')).proposed_record
        rows=(encode_record(c,recorded_at=NOW),encode_record(revoked,recorded_at=NOW))
        s=decode_state(head_row(rows[-1]),rows,kind='CLAIM',record_id=c.claim_id)
        for action in (Action.APPROVE,Action.REPLACE_DRAFT):
            with self.assertRaises(ContractError):propose(action,current=s,candidate=draft(),
                **({'review':capture(draft())} if action is Action.APPROVE else {}))

    def test_cross_record_formula_and_manually_corrupt_state_denied(self):
        c=draft();s=current(c,'CLAIM')
        other=replace(c,claim_id='other')
        with self.assertRaises(ContractError):propose(Action.REPLACE_DRAFT,current=s,candidate=other)
        changed=replace(c,formula_version='different',evidence=(replace(evidence(),formula_version='different'),))
        with self.assertRaises(ContractError):propose(Action.REPLACE_DRAFT,current=s,candidate=changed)
        for bad in (replace(s,current=claim()),replace(s,record_id='other'),replace(s,history=()),
                    replace(s,recorded_at=NOW+timedelta(seconds=1))):
            with self.assertRaises(ContractError):propose(Action.REPLACE_DRAFT,current=bad,candidate=c)

    def test_proposal_hash_pins_reason_grant_review_and_parent(self):
        c=draft();s=current(c,'CLAIM');r=capture(c)
        a=propose(Action.APPROVE,current=s,candidate=c,review=r)
        b=propose(Action.APPROVE,current=s,candidate=c,review=r,reason_code='OTHER_REASON')
        self.assertNotEqual(a.sha256,b.sha256)
        other=propose(Action.APPROVE,current=s,candidate=c,review=replace(r,review_evidence_sha256='0'*64))
        self.assertNotEqual(a.sha256,other.sha256)
        self.assertEqual(a,propose(Action.APPROVE,current=s,candidate=c,review=r))

    def test_no_review_on_draft_and_typed_action_identity_required(self):
        c=draft()
        with self.assertRaises(ContractError):propose(Action.CREATE_DRAFT,candidate=c,review=capture(c))
        with self.assertRaises(ContractError):propose('APPROVE',candidate=c)
        with self.assertRaises(ContractError):propose(Action.CREATE_DRAFT,candidate=c,identity='manager')

    def test_revision_cap_and_changed_grant_cannot_reuse_review(self):
        c=draft();past=[]
        for revision in range(1,65):
            c=replace(c,approval=replace(c.approval,revision=revision,
                parent_sha256=past[-1]['record_sha256'] if past else None))
            past.append(encode_record(c,recorded_at=NOW))
        s=decode_state(head_row(past[-1]),tuple(past),kind='CLAIM',record_id=c.claim_id)
        with self.assertRaises(ContractError):propose(Action.REPLACE_DRAFT,current=s,candidate=draft())
        candidate=draft();g=grant();review=capture(candidate)
        with self.assertRaises(ContractError):propose(Action.APPROVE,current=current(candidate,'CLAIM'),
            candidate=candidate,grant=replace(g,version='v2'),review=review)

    def test_future_identity_and_expired_review_cannot_authorize(self):
        c=draft();s=current(c,'CLAIM')
        with self.assertRaises(ContractError):propose(Action.CREATE_DRAFT,candidate=c,
            identity=replace(identity(),issued_at=int(NOW.timestamp())+1))
        expired=replace(capture(c),reviewed_at=NOW-timedelta(minutes=2),valid_until=NOW-timedelta(minutes=1))
        with self.assertRaises(ContractError):propose(Action.APPROVE,current=s,candidate=c,review=expired)


if __name__=='__main__':unittest.main()
