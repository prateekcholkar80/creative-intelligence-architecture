"""Stored publication-chain checks with synthetic audits, not real approvals."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from src.application import campaign_policy_approval_proof as proof
from src.application.campaign_policy_approval_receipt import decode_current
from src.application.campaign_review_storage import PublicationControl, STORAGE_VERSION, row_json, parse_row
from tests.test_campaign_policy_approval_receipt import fixture
from tests.test_campaign_permission_lifecycle import NOW, identity


def proven_fixture():
    row,args,authority,receipt=fixture()
    current=decode_current(row,**args);root=identity('explicit-policy-authority-publisher').principal.principal_id
    genesis=PublicationControl(1,None,'a'*64,root,NOW-timedelta(minutes=3))
    grant=PublicationControl(2,genesis.sha256,'b'*64,root,NOW-timedelta(minutes=2))
    review=PublicationControl(3,grant.sha256,'c'*64,receipt.reviewer_id,receipt.reviewed_at)
    def audit(control,kind,record_id,digest):
        return dict(schema_version=STORAGE_VERSION,command_sha256=control.command_sha256,
            action='POLICY_APPROVAL_CREATE_'+kind,permission_kind='POLICY_'+kind,record_id=record_id,
            previous_revision=0,revision=1,previous_record_sha256=None,record_sha256=digest,
            control_revision=control.revision,control_sha256=control.sha256,
            grant_id=authority.authority_id,grant_sha256=authority.sha256,
            review_id=receipt.receipt_id if kind=='RECEIPT' else None,review_sha256=receipt.sha256 if kind=='RECEIPT' else None,
            changed_by=control.changed_by,reason_code='EXPLICIT_SYNTHETIC_REVIEW',recorded_at=control.recorded_at,outcome='APPLIED')
    publication=dict(publication_controls=[row_json(review.row())],publication_history=[row_json(v.row()) for v in (genesis,grant,review)],
        authority_publications=[row_json(audit(grant,'AUTHORITY',authority.authority_id,authority.sha256))],
        receipt_publications=[row_json(audit(review,'RECEIPT',receipt.receipt_id,receipt.sha256))])
    for key in proof.GROUP_LIMITS:publication[key+'_count']=len(publication[key])
    return row,args,authority,receipt,current,publication


class ApprovalProofTests(TestCase):
    def test_exact_control_and_audits_prove_publication_not_release(self):
        row,args,authority,receipt,current,capture=proven_fixture()
        result=proof.verify_publication(capture,current=current,authority=authority)
        self.assertEqual(result.receipt_control_revision,3);self.assertEqual(len(result.sha256),64)
        self.assertFalse(current.release_allowed)

    def test_orphan_duplicate_boolean_and_overflow_captures_deny(self):
        *_,current,capture=proven_fixture();authority=proven_fixture()[2]
        for key in proof.GROUP_LIMITS:
            for count in (0,2 if key!='publication_history' else 65,True):
                changed=dict(capture,**{key+'_count':count})
                with self.subTest(key=key,count=count),self.assertRaises(ValueError):
                    proof.verify_publication(changed,current=current,authority=authority)

    def test_changed_reviewer_record_hash_namespace_or_grant_denies(self):
        row,args,authority,receipt,current,capture=proven_fixture()
        for key,value in (('changed_by',identity('other').principal.principal_id),('grant_sha256','f'*64),
                ('record_sha256','f'*64),('permission_kind','CLAIM'),('revision',True),('control_revision',2)):
            changed=deepcopy(capture);audit=parse_row(changed['receipt_publications'][0]);audit[key]=value
            changed['receipt_publications']=[row_json(audit)]
            with self.subTest(key=key),self.assertRaises(ValueError):proof.verify_publication(changed,current=current,authority=authority)

    def test_missing_history_and_changed_control_command_deny(self):
        row,args,authority,receipt,current,capture=proven_fixture()
        changed=deepcopy(capture);changed['publication_history'].pop(1);changed['publication_history_count']=2
        with self.assertRaises(ValueError):proof.verify_publication(changed,current=current,authority=authority)
        changed=deepcopy(capture);audit=parse_row(changed['authority_publications'][0]);audit['command_sha256']='0'*64
        changed['authority_publications']=[row_json(audit)]
        with self.assertRaisesRegex(ValueError,'CONTROL_CONFLICT'):proof.verify_publication(changed,current=current,authority=authority)

    def test_future_control_denies_and_unrelated_append_preserves_prefix(self):
        row,args,authority,receipt,current,capture=proven_fixture()
        original=proof.verify_publication(capture,current=current,authority=authority)
        previous=parse_row(capture['publication_controls'][0])
        appended=PublicationControl(4,previous['control_sha256'],'d'*64,receipt.reviewer_id,NOW).row()
        capture['publication_controls']=[row_json(appended)];capture['publication_history'].append(row_json(appended));capture['publication_history_count']=4
        self.assertEqual(proof.verify_publication(capture,current=current,authority=authority),original)
        future=replace(current,observed_at=NOW-timedelta(seconds=1))
        with self.assertRaises(ValueError):proof.verify_publication(capture,current=future,authority=authority)
