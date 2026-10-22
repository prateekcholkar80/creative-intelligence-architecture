"""Fixture guardrails before running billable isolated SQL verification."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from src.validation import policy_approval_publication_integration as probe
from tests.test_campaign_policy_approval_publication import inputs
from src.application.campaign_policy_approval_publication import build_approval_publication
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status


class PolicyApprovalProbeTests(TestCase):
    def test_complete_exact_relocation_and_fixture_native_slot(self):
        plan=build_approval_publication('CREATE_RECEIPT',**inputs())
        fixture=probe.relocate(plan,probe.PREFIX+'owned123')
        self.assertNotIn('.ci_brand_memory.',fixture.sql)
        self.assertNotEqual(plan.native_job_id,fixture.job_id)
        self.assertEqual(fixture.config_json,plan.config_json)
        with self.assertRaises(ValueError):probe.relocate(plan,'ci_brand_memory')
        with self.assertRaises(ValueError):probe.relocate_sql('SELECT * FROM `'+probe.PROJECT+'.ci_brand_memory.unlisted`',probe.PREFIX+'owned')

    def test_unknown_recovery_is_bounded_and_never_submits(self):
        from unittest.mock import Mock
        io=Mock();plan=Mock();unknown=ControlOutcome('original-job',Status.UNKNOWN)
        io.recover.return_value=unknown
        outcome,observations=probe.settle(io,plan,unknown)
        self.assertIs(outcome,unknown);self.assertEqual(len(observations),3)
        self.assertEqual(io.recover.call_count,2);io.submit_once.assert_not_called()

    def test_existing_report_never_replays_cloud_work(self):
        with TemporaryDirectory() as directory:
            output=Path(directory)/'existing.json';output.write_text('{}')
            with patch.object(probe.bigquery,'Client') as client:
                with self.assertRaisesRegex(ValueError,'REPORT_EXISTS'):probe.run(output)
                client.assert_not_called()

    def test_receipt_revocation_group_is_explicit_and_typed(self):
        with TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError,'MODE'):
                probe.run(Path(directory)/'report.json',receipt_revocation_only='yes')

    def test_race_metadata_requires_actual_overlap_and_known_loser_error(self):
        from unittest.mock import Mock
        from datetime import timedelta
        from tests.test_campaign_permission_lifecycle import NOW
        plan=probe.relocate(build_approval_publication('CREATE_RECEIPT',**inputs()),probe.PREFIX+'owned')
        client=Mock();client.list_jobs.return_value=[];client.get_job.side_effect=[Mock(state='DONE',error_result=None),Mock(state='DONE',error_result=dict(reason='invalidQuery',message='POLICY_PUBLICATION_CONTROL'))]
        outcomes=[ControlOutcome(plan.job_id,Status.COMMITTED,({'state':'APPLIED'},)),ControlOutcome(plan.job_id,Status.NOT_COMMITTED)]
        with patch.object(probe,'matches_sharing_job',return_value=True),patch.object(probe,'transaction_interval',
            side_effect=[(NOW,NOW+timedelta(seconds=4)),(NOW+timedelta(seconds=1),NOW+timedelta(seconds=3))]):
            result=probe.verify_race_metadata(client,[plan,plan],outcomes)
        self.assertEqual(result['overlap_seconds'],2)
        client.get_job.side_effect=[Mock(state='DONE',error_result=None),Mock(state='DONE',error_result=dict(reason='invalidQuery',message='POLICY_PUBLICATION_CONTROL'))]
        with patch.object(probe,'matches_sharing_job',return_value=True),patch.object(probe,'transaction_interval',
            side_effect=[(NOW,NOW+timedelta(seconds=1)),(NOW+timedelta(seconds=2),NOW+timedelta(seconds=3))]):
            with self.assertRaisesRegex(Exception,'RACE_DID_NOT_OVERLAP'):probe.verify_race_metadata(client,[plan,plan],outcomes)

    def test_proof_group_uses_authoritative_audits_without_new_publication_slots(self):
        self.assertIn('campaign_permission_publication_audits',probe.READ_SQL)
        self.assertIn('POLICY_APPROVAL_CREATE_RECEIPT',probe.READ_SQL)
        with TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError,'MODE'):
                probe.run(Path(directory)/'report.json',receipt_revocation_only=True,proof_only=True)
