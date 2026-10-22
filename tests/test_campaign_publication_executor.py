"""Offline trusted adapter guards; no cloud authentication or writes."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from src.application.campaign_publication_executor import PublicationExecutor,PublicationIntent
from src.application.identity import IapIdentityAdapter,IapSettings
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_contract import ContractError
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION
from tests.test_campaign_publication_reader import decode,wire_rows,request
from src.application.campaign_permission_lifecycle import Action
from tests.test_campaign_permission_lifecycle import NOW,AUDIENCE,draft,identity


class ExecutorTests(TestCase):
    def setUp(self):
        self.client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        self.adapter=IapIdentityAdapter(IapSettings(AUDIENCE))
        self.adapter.authenticate=Mock(return_value=identity())
        self.executor=PublicationExecutor(self.client,self.adapter,execution_enabled=True,clock=lambda:NOW)
        self.intent=PublicationIntent(decode(),draft(),'FIXTURE_REVIEW');self.plan=self.intent.plan()
        self.read=patch('src.application.campaign_publication_executor.read_capture',return_value=decode()).start()
        self.addCleanup(patch.stopall)
        self.match=patch('src.application.campaign_publication_executor.matches_sharing_job',return_value=True).start()
        self.job=Mock(state='DONE',error_result=None)
        self.job.result.return_value=[dict(state='APPLIED',command_sha256=self.plan.command_sha256,
            record_sha256=self.plan.proposed_record_sha256,control_sha256=self.plan.candidate_control_sha256)]
        self.client.get_job.return_value=self.job

    def test_disabled_denies_before_authentication_or_cloud(self):
        executor=PublicationExecutor(self.client,self.adapter,clock=lambda:NOW)
        with self.assertRaises(ContractError):executor.recover({},self.intent)
        self.adapter.authenticate.assert_not_called();self.client.get_job.assert_not_called()

    def test_prepare_and_original_recovery_never_submit(self):
        prepared=self.executor.prepare({},decode().request,candidate=draft(),reason_code='FIXTURE_REVIEW')
        self.assertEqual(prepared.plan(),self.plan)
        outcome=self.executor.recover({},prepared)
        self.assertEqual(outcome.native_status,Status.COMMITTED)
        self.assertEqual(outcome.result_state,'APPLIED');self.client.query.assert_not_called()
        self.assertEqual(self.read.call_count,3)

    def test_absent_recovery_and_mismatch_remain_unknown(self):
        self.client.get_job.side_effect=NotFound('absent')
        self.assertEqual(self.executor.recover({},self.intent).native_status,Status.UNKNOWN)
        self.client.query.assert_not_called()
        self.client.get_job.side_effect=None;self.match.return_value=False
        self.assertEqual(self.executor.submit({},self.intent).native_status,Status.UNKNOWN)
        self.client.query.assert_not_called()

    def test_authoritative_absence_allows_one_insert_without_retries(self):
        self.client.get_job.side_effect=NotFound('absent');self.client.query.return_value=self.job
        outcome=self.executor.submit({},self.intent)
        self.assertTrue(outcome.submitted);self.assertEqual(outcome.native_status,Status.COMMITTED)
        self.client.query.assert_called_once()
        kwargs=self.client.query.call_args.kwargs
        self.assertIsNone(kwargs['retry']);self.assertIsNone(kwargs['job_retry'])
        self.assertEqual(kwargs['job_id'],self.plan.native_job_id)

    def test_lost_response_reconciles_exact_original_once(self):
        self.client.get_job.side_effect=[NotFound('absent'),self.job]
        self.client.query.side_effect=RuntimeError('lost')
        outcome=self.executor.submit({},self.intent)
        self.assertEqual(outcome.native_status,Status.COMMITTED);self.assertTrue(outcome.submitted)
        self.client.query.assert_called_once()
        for call in self.client.get_job.call_args_list:self.assertEqual(call.args,(self.plan.native_job_id,))

    def test_unknown_lookup_never_submits(self):
        self.client.get_job.side_effect=RuntimeError('unavailable')
        self.assertEqual(self.executor.submit({},self.intent).native_status,Status.UNKNOWN)
        self.client.query.assert_not_called()

    def test_wrong_actor_revoked_authority_and_stale_capture_deny(self):
        self.adapter.authenticate.return_value=identity('other')
        with self.assertRaises(ContractError):self.executor.submit({},self.intent)
        self.adapter.authenticate.return_value=identity()
        self.read.side_effect=ContractError('PUBLICATION_REVIEWER_DENIED')
        with self.assertRaises(ContractError):self.executor.recover({},self.intent)
        self.read.side_effect=None;self.read.return_value=replace(decode(),evidence=())
        self.client.get_job.side_effect=NotFound('absent')
        with self.assertRaises(ContractError):self.executor.submit({},self.intent)
        self.client.query.assert_not_called()

    def test_postcommit_release_revocation_withholds_result(self):
        self.read.side_effect=[decode(),ContractError('PUBLICATION_REVIEWER_DENIED')]
        with self.assertRaises(ContractError):self.executor.recover({},self.intent)
        self.client.query.assert_not_called()

    def test_malformed_result_and_failed_script_positive_status(self):
        self.job.result.return_value=[{}]
        self.assertEqual(self.executor.recover({},self.intent).native_status,Status.UNKNOWN)
        self.job.error_result={'reason':'invalidQuery'}
        with patch('src.application.campaign_publication_executor.failed_sharing_job_status',return_value=Status.COMMITTED):
            outcome=self.executor.recover({},self.intent)
        self.assertEqual(outcome.native_status,Status.COMMITTED);self.assertIsNone(outcome.result_state)

    def test_expired_plan_cannot_dispatch_but_known_job_can_recover(self):
        self.executor.clock=lambda:NOW+timedelta(seconds=121)
        self.assertEqual(self.executor.recover({},self.intent).native_status,Status.COMMITTED)
        self.client.get_job.side_effect=NotFound('absent')
        with self.assertRaises(ContractError):self.executor.submit({},self.intent)
        self.client.query.assert_not_called()

    def test_creation_authority_handles_absent_then_present_target(self):
        original=decode(wire_rows(Action.CREATE_DRAFT),request(Action.CREATE_DRAFT))
        intent=PublicationIntent(original,draft(),'FIXTURE_REVIEW')
        self.read.side_effect=[ContractError('PUBLICATION_READ_MISSING'),original]
        capture=self.executor._authority({},intent)
        self.assertEqual(capture,original)
        self.assertEqual(self.read.call_args_list[0].args[1].action,Action.REPLACE_DRAFT)
        self.assertEqual(self.read.call_args_list[1].args[1].action,Action.CREATE_DRAFT)
        self.read.side_effect=None;self.read.return_value=decode()
        self.assertEqual(self.executor._authority({},intent),decode())

    def test_creation_does_not_fallback_after_revoked_grant(self):
        original=decode(wire_rows(Action.CREATE_DRAFT),request(Action.CREATE_DRAFT))
        self.read.side_effect=ContractError('PUBLICATION_REVIEWER_DENIED')
        with self.assertRaises(ContractError):
            self.executor._authority({},PublicationIntent(original,draft(),'FIXTURE_REVIEW'))
        self.read.assert_called_once()

    def test_pending_result_timeout_remains_unknown_then_original_recovers(self):
        self.job.state='RUNNING';self.job.result.side_effect=TimeoutError('bounded wait')
        self.assertEqual(self.executor.recover({},self.intent).native_status,Status.UNKNOWN)
        self.job.state='DONE';self.job.result.side_effect=None
        self.assertEqual(self.executor.recover({},self.intent).native_status,Status.COMMITTED)
        self.client.query.assert_not_called()
