"""Synthetic publisher authority and plans only, never actual appointments."""
from dataclasses import replace
from datetime import timedelta
import json
from unittest import TestCase
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest,build_operator_publication
from src.application.campaign_operator_policy_storage import encode_record,head_row,decode_state
from src.application.campaign_review_storage import row_json
from src.application.campaign_permission_publication import validate_result
from tests.test_campaign_operator_policy_storage import record
from tests.test_campaign_review_publication_writer import OPERATOR,control_rows,policy
from tests.test_campaign_permission_lifecycle import NOW,AUDIENCE,identity


def manifest(**changes):
    return OperatorPublisherManifest(**(dict(manifest_id='explicit-fixture-publisher',expected_audience=AUDIENCE,
        principal_ids=(OPERATOR.principal.principal_id,),effective_at=NOW-timedelta(seconds=1),
        valid_until=NOW+timedelta(seconds=120))|changes))


def plan(previous=(),candidate=None,**changes):
    rows=tuple(encode_record(v) for v in previous)
    values=dict(identity=OPERATOR,manifest=manifest(),control=control_rows(),
        head_json=row_json(head_row(rows[-1])) if rows else None,
        history_json=tuple(row_json(r) for r in rows),candidate=candidate or record(),checked_at=NOW)
    return build_operator_publication(**(values|changes))


class OperatorPublicationTests(TestCase):
    def test_genesis_disabled_exact_fenced_plan_and_distinct_audit_domain(self):
        value=plan()
        self.assertFalse(value.execution_enabled)
        self.assertIn('campaign_permission_controls',value.sql)
        self.assertIn('OPERATOR_PUBLICATION_CONTROLS',value.sql)
        self.assertIn('OPERATOR_PUBLICATION_HEAD',value.sql)
        self.assertIn('ROLLBACK TRANSACTION',value.sql)
        self.assertIn('ASSERT @@row_count=1',value.sql)
        self.assertNotIn('DELETE ',value.sql)
        audit=json.loads(value.audit_json)
        self.assertEqual(audit['permission_kind'],'OPERATOR_POLICY')
        self.assertEqual(audit['action'],'OPERATOR_POLICY_PUBLISH')
        self.assertEqual(audit['grant_id'],'explicit-fixture-publisher')
        self.assertLessEqual(value.sql.count(';'),64)
        self.assertEqual(value.config().maximum_bytes_billed,768*1024*1024)
        self.assertEqual(int(value.config().job_timeout_ms),120000)

    def test_undeclared_actor_wrong_audience_expiry_and_unbounded_manifest_deny(self):
        for changes in (dict(identity=identity('not-a-publisher')),
                dict(manifest=manifest(effective_at=NOW+timedelta(seconds=1))),
                dict(manifest=manifest(valid_until=NOW)),dict(candidate=replace(record(),changed_by=identity().principal.principal_id))):
            with self.subTest(changes=changes),self.assertRaises(ValueError):plan(**changes)
        with self.assertRaises(ValueError):manifest(valid_until=NOW+timedelta(seconds=301))
        with self.assertRaises(ValueError):manifest(principal_ids=())

    def test_source_partial_state_missing_fence_and_changed_parent_deny(self):
        for changes in (dict(control=None),dict(head_json='{}'),dict(history_json=('{}',)),
                dict(candidate=replace(record(),parent_sha256='a'*64,policy=replace(policy(),revision=2)))):
            with self.subTest(changes=changes),self.assertRaises(ValueError):plan(**changes)

    def test_replacement_and_terminal_revocation_validate_complete_chain(self):
        first=record();second=record(2,first)
        replaced=plan((first,),second)
        self.assertIn('OPERATOR_PUBLICATION_CAS',replaced.sql)
        revoked=record(2,first,status='REVOKED')
        revoked_plan=plan((first,),revoked)
        self.assertEqual(json.loads(revoked_plan.audit_json)['action'],'OPERATOR_POLICY_REVOKE')
        state=decode_state(head_row(encode_record(revoked)),(encode_record(first),encode_record(revoked)),checked_at=NOW)
        with self.assertRaisesRegex(ValueError,'OPERATOR_POLICY_NOT_CURRENT'):state.require_current(NOW)
        with self.assertRaises(ValueError):plan((first,revoked),record(3,revoked))
        changed=replace(revoked,policy=replace(revoked.policy,max_review_seconds=1))
        with self.assertRaises(ValueError):plan((first,),changed)

    def test_expired_policy_can_be_revoked_by_current_explicit_publisher(self):
        first=replace(record(),policy=replace(policy(),effective_at=NOW-timedelta(hours=3),valid_until=NOW-timedelta(hours=2)),recorded_at=NOW-timedelta(hours=3))
        revoked=replace(first,policy=replace(first.policy,revision=2),parent_sha256=first.sha256,status='REVOKED',recorded_at=NOW)
        self.assertEqual(json.loads(plan((first,),revoked).audit_json)['action'],'OPERATOR_POLICY_REVOKE')

    def test_command_binds_manifest_sources_and_deadline(self):
        first=plan();other=plan(manifest=manifest(manifest_id='other-explicit-manifest'))
        self.assertEqual(first.native_job_id,other.native_job_id)
        self.assertNotEqual(first.command_sha256,other.command_sha256)
        value=plan(manifest=manifest(valid_until=NOW+timedelta(seconds=10)))
        parameters={p.name:p.value for p in value.config().query_parameters if hasattr(p,'value')}
        self.assertEqual(parameters['expires_at'],NOW+timedelta(seconds=10))
        for marker in ('REPLAY_AUDIT','REPLAY_HEAD','REPLAY_HISTORY','REPLAY_CONTROL','REPLAY_CONTROLS'):
            self.assertIn('OPERATOR_PUBLICATION_'+marker,first.sql)

    def test_receipt_requires_exact_original_hashes_and_single_row(self):
        value=plan();row=dict(state='APPLIED',command_sha256=value.command_sha256,
            record_sha256=value.proposed_record_sha256,control_sha256=value.candidate_control_sha256)
        self.assertEqual(validate_result(value,(row,)),'APPLIED')
        for rows in ((),(row,row),(row|{'control_sha256':'a'*64},)):
            with self.assertRaises(ValueError):validate_result(value,rows)
