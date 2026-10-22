"""Exact stored review bindings, not qualifying evaluation or real approval."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from unittest.mock import Mock, patch
from src.application import campaign_policy_approval_receipt as approval
from src.application import campaign_policy_approval_reader as reader
from src.application.identity import IapIdentityAdapter, IapSettings
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from tests.test_campaign_permission_lifecycle import identity, NOW, AUDIENCE


def pins():
    return approval.PolicyApprovalPins('ci-poc-marketing', 'CLARIFICATION', 'CLARIFICATION',
        'campaign-evidence-clarification', 'draft-v1', *[c*64 for c in 'abcde'])


def fixture():
    actor = identity()
    authority = approval.PolicyApprovalAuthority('explicit-policy-reviewers', AUDIENCE, 'ci-poc-marketing',
        (actor.principal.principal_id,), ('ACCEPTANCE', 'CLARIFICATION'), NOW-timedelta(hours=1), NOW+timedelta(hours=1))
    receipt = approval.PolicyApprovalReceipt('review-one', pins(), authority.authority_id, authority.sha256,
        actor.principal.principal_id, NOW-timedelta(minutes=1), NOW+timedelta(minutes=30))
    a = dict(authority_id=authority.authority_id, authority_sha256=authority.sha256, canonical_json=approval._text(authority.payload()))
    r = dict(receipt_id=receipt.receipt_id, authority_id=authority.authority_id,
        receipt_sha256=receipt.sha256, canonical_json=approval._text(receipt.payload()))
    row = dict(observed_at=NOW, authorities=[approval._text(a)], authority_count=1,
        receipts=[approval._text(r)], receipt_count=1, revocation_count=0)
    args = dict(pins=pins(), authority_id=authority.authority_id, authority_sha256=authority.sha256,
        receipt_id=receipt.receipt_id, identity=actor, native_job_id='exact-native-read', started=NOW, ended=NOW)
    return row, args, authority, receipt


def reader_fixture():
    from tests.test_campaign_policy_approval_proof import proven_fixture
    row,args,authority,receipt,current,publication=proven_fixture()
    return dict(row,**publication),args,authority,receipt


class PolicyApprovalReceiptTests(TestCase):
    def test_exact_receipt_never_alone_grants_release(self):
        row, args, _, receipt = fixture()
        current = approval.decode_current(row, **args)
        self.assertEqual(current.receipt, receipt); self.assertFalse(current.release_allowed)

    def test_every_version_context_pin_is_bound(self):
        row, args, _, _ = fixture()
        for name in ('policy_sha256', 'prompt_snapshot_sha256', 'model_profile_sha256', 'benchmark_sha256', 'evaluation_sha256'):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'BINDING_CHANGED'):
                approval.decode_current(row, **dict(args, pins=replace(pins(), **{name: 'f'*64})))
        for name, value in (('policy_version', 'draft-v2'), ('stage', 'INTERPRETATION'), ('kind', 'ACCEPTANCE'), ('workspace_id', 'other')):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'BINDING_CHANGED'):
                approval.decode_current(row, **dict(args, pins=replace(pins(), **{name: value})))

    def test_revoked_missing_duplicate_and_incomplete_capture_deny(self):
        row, args, _, _ = fixture()
        for changes in ({'revocation_count': 1}, {'authority_count': 0}, {'receipt_count': 2}, {'receipts': []}, {'authority_count': True}):
            with self.subTest(changes=changes), self.assertRaises(ValueError): approval.decode_current(dict(row, **changes), **args)

    def test_expiry_future_window_and_changed_authority_deny(self):
        row, args, _, _ = fixture()
        for changes in ({'authority_sha256': 'f'*64}, {'ended': NOW+timedelta(seconds=60)}, {'started': NOW+timedelta(seconds=1)}):
            with self.assertRaises(ValueError): approval.decode_current(row, **dict(args, **changes))

    def test_claim_operator_kinds_do_not_authorize_policy_reviews(self):
        _, _, authority, _ = fixture()
        with self.assertRaisesRegex(ValueError, 'AUTHORITY_KINDS'): replace(authority, kinds=('CLAIM',))

    def test_reader_configuration_and_sql_are_fixed_and_read_only(self):
        row, args, authority, receipt = reader_fixture()
        verifier = IapIdentityAdapter(IapSettings(AUDIENCE)); verifier.authenticate = Mock(return_value=args['identity'])
        client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)
        current = reader.PolicyApprovalReader(client=client, identity_adapter=verifier, pins=pins(),
            authority_id=authority.authority_id, authority_sha256=authority.sha256,
            receipt_id=receipt.receipt_id, reader_ids=(args['identity'].principal.principal_id,), clock=lambda: NOW)
        job = Mock(state='DONE', error_result=None); job.result.return_value = [row]
        client.query.return_value = client.get_job.return_value = job
        with patch.object(reader, 'require_owned_worker'), patch.object(reader, 'validate_table'), patch.object(reader, 'matches_sharing_job', return_value=True):
            self.assertFalse(current.read({}).release_allowed)
        self.assertEqual(verifier.authenticate.call_count, 2)
        self.assertNotIn('INSERT', reader.SQL); self.assertNotIn('UPDATE', reader.SQL)
        self.assertIn("target_kind='AUTHORITY'", reader.SQL); self.assertIn("target_kind='RECEIPT'", reader.SQL)

    def test_revocation_after_first_read_is_not_cached(self):
        row, args, authority, receipt = reader_fixture()
        verifier = IapIdentityAdapter(IapSettings(AUDIENCE)); verifier.authenticate = Mock(return_value=args['identity'])
        client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)
        current = reader.PolicyApprovalReader(client=client, identity_adapter=verifier, pins=pins(),
            authority_id=authority.authority_id, authority_sha256=authority.sha256,
            receipt_id=receipt.receipt_id, reader_ids=(args['identity'].principal.principal_id,), clock=lambda: NOW)
        job = Mock(state='DONE', error_result=None)
        job.result.side_effect = [[], [row], [], [dict(row, revocation_count=1)]]
        client.query.return_value = client.get_job.return_value = job
        with patch.object(reader, 'require_owned_worker'), patch.object(reader, 'validate_table'), patch.object(reader, 'matches_sharing_job', return_value=True):
            current.read({})
            with self.assertRaisesRegex(ValueError, 'REVOKED'): current.read({})
        self.assertEqual(client.query.call_count, 2)

    def test_proven_reader_rejects_orphan_and_binds_stable_publication_hash(self):
        row,args,authority,receipt=reader_fixture()
        current=reader.decode_proven_snapshot(row,**args)
        self.assertEqual(len(current.publication_sha256),64);self.assertFalse(current.release_allowed)
        with self.assertRaisesRegex(ValueError,'PROVEN_CAPTURE'):
            reader.decode_proven_snapshot({k:row[k] for k in reader.BASE_KEYS},**args)
        with self.assertRaisesRegex(ValueError,'MISSING_DUPLICATE'):
            reader.decode_proven_snapshot(dict(row,receipt_publications=[],receipt_publications_count=0),**args)
