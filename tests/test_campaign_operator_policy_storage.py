"""Pure operator-policy records; no publisher authentication or live approvals."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from src.application.campaign_operator_policy_storage import (
    POLICY_ID,SCHEMAS,HEAD_TABLE,HISTORY_TABLE,OperatorPolicyVersion,encode_record,head_row,decode_record,decode_state,table_metadata,
)
from src.application.workflow_contract import ContractError
from src.application.campaign_review_storage import row_json
from tests.test_campaign_review_publication_writer import policy,OPERATOR
from tests.test_campaign_permission_lifecycle import NOW,AUDIENCE
from src.ingestion.configure_campaign_acceptance import PROJECT


def record(revision=1,previous=None,**changes):
    return OperatorPolicyVersion(replace(policy(),policy_id=POLICY_ID,revision=revision),
        previous.sha256 if previous else None,changes.pop('status','ACTIVE'),OPERATOR.principal.principal_id,
        changes.pop('reason_code','EXPLICIT_FIXTURE_POLICY'),changes.pop('recorded_at',NOW),**changes)


def state(*records,**kwargs):
    rows=tuple(encode_record(r) for r in records)
    return decode_state(head_row(rows[-1]),rows,checked_at=NOW,**kwargs)


class OperatorPolicyStorageTests(TestCase):
    def test_two_separate_tables_exact_layout_and_no_seeded_values(self):
        self.assertEqual(set(SCHEMAS),{HEAD_TABLE,HISTORY_TABLE})
        for name in SCHEMAS:
            table=table_metadata(name,PROJECT)
            self.assertEqual(table.dataset_id,'ci_brand_memory');self.assertIsNone(table.expires)
            self.assertEqual(table.clustering_fields,['policy_id','revision'])
        with self.assertRaises(ContractError):table_metadata('other',PROJECT)

    def test_complete_canonical_roundtrip_and_current_policy(self):
        first=record();row=encode_record(first)
        self.assertEqual(decode_record(row),first)
        self.assertEqual(state(first).require_current(NOW),first.policy)
        self.assertEqual(row['policy_sha256'],first.policy.sha256)
        self.assertNotEqual(row['record_sha256'],row['policy_sha256'])

    def test_hash_scope_projection_type_and_payload_corruption_deny(self):
        row=encode_record(record())
        for changes in ({'revision':True},{'revision':2},{'policy_id':'other'},{'policy_sha256':'a'*64},
            {'record_sha256':'b'*64},{'schema_version':'unknown'},{'reason_code':'OTHER'},
            {'payload_canonical_json':row['payload_canonical_json']+' '},{'extra':True},
            {'payload_canonical_json':'{}'},{'payload_canonical_json':'x'*65537}):
            with self.assertRaises(ContractError):decode_record(row|changes)

    def test_fixed_scope_explicit_publisher_parent_and_no_revoked_genesis(self):
        first=record()
        for changes in ({'changed_by':'admin@example.com'},{'parent_sha256':'a'*64},{'status':'REVOKED'},
            {'policy':replace(first.policy,policy_id='other')},{'policy':replace(first.policy,revision=65)}):
            with self.assertRaises(ContractError):replace(first,**changes)

    def test_missing_duplicate_gap_wrong_head_and_head_boolean_revision_deny(self):
        first=record();second=record(2,first);a,b=encode_record(first),encode_record(second)
        for head,history in ((head_row(a),()),(head_row(a),(a,a)),(head_row(b),(b,)),
            (head_row(a),(a,b)),(head_row(a)|{'revision':True},(a,)),(head_row(a)|{'extra':1},(a,))):
            with self.assertRaises(ContractError):decode_state(head,history,checked_at=NOW)

    def test_exact_historical_checkpoint_and_higher_revision(self):
        first=record();second=record(2,first)
        self.assertEqual(state(first,second,minimum_revision=1,checkpoint_sha256=first.sha256).current,second)
        self.assertEqual(state(first,second,minimum_revision=2,checkpoint_sha256=second.sha256).current,second)
        for kwargs in ({'minimum_revision':3},{'minimum_revision':True},{'checkpoint_sha256':'a'*64},
            {'minimum_revision':1,'checkpoint_sha256':second.sha256}):
            with self.assertRaises(ContractError):state(first,second,**kwargs)

    def test_parent_audience_and_timestamp_order_conflicts_deny(self):
        first=record();second=record(2,first)
        for changed in (replace(second,parent_sha256='b'*64),
            replace(second,policy=replace(second.policy,expected_audience=AUDIENCE+'-other')),
            replace(second,recorded_at=NOW-timedelta(seconds=1))):
            with self.assertRaises(ContractError):state(first,changed)
        with self.assertRaises(ContractError):state(replace(first,recorded_at=NOW+timedelta(seconds=1)))

    def test_expired_future_and_revoked_heads_never_release_policy(self):
        first=record();revoked=record(2,first,status='REVOKED')
        for value,when in ((state(first),first.policy.valid_until),(state(first),first.policy.effective_at-timedelta(seconds=1)),
            (state(first,revoked),NOW)):
            with self.assertRaises(ContractError):value.require_current(when)
        self.assertEqual(state(first,revoked).current.status,'REVOKED')

    def test_revocation_is_terminal_and_cannot_sneak_in_role_changes(self):
        first=record();revoked=record(2,first,status='REVOKED');resurrect=record(3,revoked)
        with self.assertRaises(ContractError):state(first,revoked,resurrect)
        changed=replace(revoked,policy=replace(revoked.policy,max_review_seconds=601))
        with self.assertRaises(ContractError):state(first,changed)

    def test_full_history_bound_rejects_overflow_without_truncation(self):
        rows=tuple(encode_record(record()) for _ in range(65))
        with self.assertRaises(ContractError):decode_state(head_row(rows[0]),rows,checked_at=NOW)

    def test_aggregate_history_size_and_nonrow_entries_deny_without_truncation(self):
        from tests.test_campaign_permission_lifecycle import identity
        principals=tuple(sorted(identity('bounded-'+str(i)).principal.principal_id for i in range(160)))
        first=replace(record(),policy=replace(record().policy,grant_operator_ids=principals,
            reviewer_principal_ids=principals,review_revoker_ids=principals))
        second=replace(record(2,first),policy=replace(first.policy,revision=2))
        a,b=encode_record(first),encode_record(second)
        self.assertLess(len(row_json(a).encode()),65536)
        with self.assertRaises(ContractError):decode_state(head_row(b),(a,b),checked_at=NOW)
        with self.assertRaises(ContractError):decode_state(head_row(a),('not-a-row',),checked_at=NOW)
