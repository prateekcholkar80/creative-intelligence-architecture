"""Supplied synthetic captures; no real appointments, approvals or cloud writes."""
from dataclasses import replace
from datetime import timedelta
import json
from unittest import TestCase
from src.application.campaign_policy_approval_publication import (
    PolicyAuthorityPublisherManifest, ApprovalPublicationSources, build_approval_publication,
)
from src.application.campaign_policy_approval_receipt import _text
from tests.test_campaign_policy_approval_receipt import fixture
from tests.test_campaign_review_publication_writer import control_rows
from tests.test_campaign_permission_lifecycle import NOW, AUDIENCE, identity
from src.application.identity import IapSettings


def manifest(actor):
    return PolicyAuthorityPublisherManifest('explicit-policy-authority-publisher', AUDIENCE,
        (actor.principal.principal_id,), 'ci-poc-marketing', NOW-timedelta(seconds=1), NOW+timedelta(seconds=120))


def inputs(action='CREATE_RECEIPT'):
    row, args, authority, receipt = fixture(); actor=args['identity']
    receipt=replace(receipt,reviewed_at=NOW)
    sources=ApprovalPublicationSources(control_rows(),tuple(row['authorities']),(),())
    values=dict(identity=actor,expected_audience=IapSettings(AUDIENCE),sources=sources,
        authority_id=authority.authority_id,receipt_id=receipt.receipt_id,candidate=receipt,
        confirmed_pins_sha256=receipt.pins.sha256,checked_at=NOW,reason_code='EXPLICIT_SYNTHETIC_REVIEW')
    if action=='CREATE_AUTHORITY':
        values.update(sources=replace(sources,authorities=()),receipt_id=None,candidate=authority,
            confirmed_pins_sha256=None,publisher_manifest=manifest(actor))
    elif action.startswith('REVOKE'):
        values.update(sources=replace(sources,receipts=tuple(row['receipts']) if action=='REVOKE_RECEIPT' else ()),
            receipt_id=receipt.receipt_id if action=='REVOKE_RECEIPT' else None,candidate=None,
            confirmed_pins_sha256=None,publisher_manifest=manifest(actor))
    return values


class ApprovalPublicationTests(TestCase):
    def test_each_action_is_disabled_fenced_atomic_and_append_only(self):
        for action in ('CREATE_AUTHORITY','CREATE_RECEIPT','REVOKE_AUTHORITY','REVOKE_RECEIPT'):
            with self.subTest(action=action):
                plan=build_approval_publication(action,**inputs(action))
                self.assertFalse(plan.execution_enabled)
                self.assertIn('BEGIN TRANSACTION',plan.sql);self.assertIn('ROLLBACK TRANSACTION',plan.sql)
                self.assertIn('POLICY_PUBLICATION_CAS',plan.sql)
                self.assertNotIn('DELETE ',plan.sql);self.assertNotIn('TRUNCATE ',plan.sql)
                self.assertLessEqual(plan.sql.count(';'),64)
                self.assertEqual(plan.config().maximum_bytes_billed,768*1024*1024)
                self.assertEqual(int(plan.config().job_timeout_ms),120000)
                self.assertIn('POLICY_APPROVAL_'+action,plan.audit_json)

    def test_operator_assignment_does_not_imply_policy_authority_publisher(self):
        from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
        values=inputs('CREATE_AUTHORITY');values['publisher_manifest']=OperatorPublisherManifest(
            'operator-policy-only',AUDIENCE,(values['identity'].principal.principal_id,),NOW-timedelta(seconds=1),NOW+timedelta(seconds=60))
        with self.assertRaisesRegex(ValueError,'PUBLISHER_REQUIRED'):
            build_approval_publication('CREATE_AUTHORITY',**values)

    def test_root_publisher_does_not_imply_reviewer_and_hash_confirmation_is_required(self):
        values=inputs();values['identity']=identity('foreign-reviewer')
        with self.assertRaisesRegex(ValueError,'REVIEWER_DENIED'):build_approval_publication('CREATE_RECEIPT',**values)
        values=inputs();values['confirmed_pins_sha256']='f'*64
        with self.assertRaisesRegex(ValueError,'NOT_CONFIRMED'):build_approval_publication('CREATE_RECEIPT',**values)
        values=inputs();values['publisher_manifest']=manifest(values['identity'])
        with self.assertRaisesRegex(ValueError,'CANNOT_USE_PUBLISHER_ROLE'):build_approval_publication('CREATE_RECEIPT',**values)

    def test_exact_authority_and_scope_are_required(self):
        for changes in ({'authority_sha256':'f'*64},{'reviewer_id':identity('other').principal.principal_id},
                        {'pins':replace(inputs()['candidate'].pins,workspace_id='other')},
                        {'reviewed_at':NOW-timedelta(seconds=1)}):
            values=inputs();values['candidate']=replace(values['candidate'],**changes)
            with self.subTest(changes=changes),self.assertRaises(ValueError):build_approval_publication('CREATE_RECEIPT',**values)

    def test_revocation_is_terminal_and_blocks_new_receipts(self):
        values=inputs()
        revoked=_text(dict(revocation_id='revoked-one',target_kind='AUTHORITY',target_id=values['authority_id'],revoked_at=NOW.isoformat()))
        values['sources']=replace(values['sources'],revocations=(revoked,))
        with self.assertRaisesRegex(ValueError,'CREATE_DENIED'):build_approval_publication('CREATE_RECEIPT',**values)
        values=inputs('REVOKE_AUTHORITY');values['sources']=replace(values['sources'],revocations=(revoked,))
        with self.assertRaisesRegex(ValueError,'TERMINAL'):build_approval_publication('REVOKE_AUTHORITY',**values)

    def test_global_slot_does_not_change_with_payload_or_actor_time(self):
        values=inputs();first=build_approval_publication('CREATE_RECEIPT',**values)
        values['candidate']=replace(values['candidate'],valid_until=NOW+timedelta(minutes=5))
        other=build_approval_publication('CREATE_RECEIPT',**values)
        self.assertEqual(first.native_job_id,other.native_job_id)
        self.assertNotEqual(first.command_sha256,other.command_sha256)

    def test_missing_control_and_oversized_capture_deny(self):
        values=inputs()
        with self.assertRaises(ValueError):replace(values['sources'],control=None)
        with self.assertRaises(ValueError):replace(values['sources'],revocations=('x'*65537,))

    def test_revoke_does_not_need_unexpired_source_approval(self):
        values=inputs('REVOKE_AUTHORITY')
        row,args,authority,receipt=fixture()
        old=replace(authority,effective_at=NOW-timedelta(hours=2),valid_until=NOW-timedelta(hours=1))
        encoded=_text(dict(authority_id=old.authority_id,authority_sha256=old.sha256,canonical_json=_text(old.payload())))
        values['sources']=replace(values['sources'],authorities=(encoded,))
        self.assertFalse(build_approval_publication('REVOKE_AUTHORITY',**values).execution_enabled)
