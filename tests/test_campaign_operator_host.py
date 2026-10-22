"""Operator transport boundaries; no cloud authentication or business approval."""
from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_operator_host import OperatorHostConfig, ProtectedOperatorHost, config_from_wire, request_values
from src.application.campaign_operator_worker import execute
from src.application.sharing_operation_host import SharingHostSettings
from src.application.identity import IapSettings
from src.application.operation_channel import ChannelError
from tests.test_campaign_operator_policy_publication import manifest
from tests.test_campaign_review_publication_writer import policy
from tests.test_campaign_permission_lifecycle import AUDIENCE


def config(): return OperatorHostConfig(IapSettings(AUDIENCE), manifest(), policy(), 'EXPLICIT_REVIEW', enabled=True)


class OperatorHostTests(TestCase):
    def test_retrieval_progress_is_correlated_bounded_and_separate_from_receipt(self):
        host=ProtectedOperatorHost(config());host._channel=Mock()
        progress=dict(phase='saved_source_verification',state='SUCCEEDED',elapsed_seconds=12.3)
        final=dict(id='original',result={'receipt':'unchanged'})
        with patch('src.application.campaign_operator_host.receive',side_effect=[dict(id='original',progress=progress),final]),self.assertLogs('src.application.campaign_operator_host',level='WARNING') as logs:
            self.assertEqual(host._receive_reply('corpus_retrieval_recover','original',lambda:30),final)
        self.assertIn('private_retrieval_phase',logs.output[0]);self.assertNotIn('receipt',logs.output[0])

    def test_retrieval_progress_rejects_unrecognized_or_private_fields(self):
        host=ProtectedOperatorHost(config());host._channel=Mock()
        event=dict(phase='saved_source_verification',state='STARTED',elapsed_seconds=0.)
        frames=[dict(id='other',progress=event),dict(id='original',progress=dict(event,token='private')),
            dict(id='original',progress=dict(event,phase='identity@example.com')),
            dict(id='original',progress=dict(event,elapsed_seconds=float('nan')))]
        for frame in frames:
            with patch('src.application.campaign_operator_host.receive',return_value=frame),self.assertRaises(ChannelError):
                host._receive_reply('corpus_retrieval_recover','original',lambda:30)
    def test_config_roundtrip_and_explicit_audience(self):
        host = ProtectedOperatorHost(config())
        self.assertEqual(config_from_wire(host._config_values()), config())
        with self.assertRaises(ChannelError): replace(config(), writes_enabled='true')
        with self.assertRaises(ChannelError): replace(config(), iap=IapSettings(AUDIENCE+'-other'))

    def test_browser_cannot_supply_policy_or_native_job_or_identity(self):
        for key in ('policy', 'native_job_id', 'identity', 'approval_verified'):
            with self.assertRaises(ChannelError): request_values('prepare', {}, {key: 'value'})
        with self.assertRaises(ChannelError): request_values('approve', {}, {})
        with self.assertRaises(ChannelError): request_values('publish', {}, {'confirmed_sha256': 'bad'})
        self.assertEqual(request_values('current', {}, {})['parameters'], {})

    def test_writes_disabled_in_both_host_and_worker(self):
        host = ProtectedOperatorHost(config(), SharingHostSettings(enabled=True))
        with self.assertRaisesRegex(RuntimeError, 'WRITES_DISABLED'):
            host._request_values('publish', {}, {'confirmed_sha256': 'a'*64})
        service = Mock()
        with self.assertRaises(ChannelError): execute(service, config(), 'publish', {}, {'confirmed_sha256': 'a'*64})
        service.execute.assert_not_called()

    def test_read_and_recovery_do_not_enable_publication(self):
        service = Mock(); service.current.return_value = {'approval_verified': False}
        self.assertEqual(execute(service, config(), 'current', {}, {}), {'approval_verified': False})
        execute(service, config(), 'recover', {}, {'confirmed_sha256': 'a'*64})
        service.execute.assert_called_once_with({}, confirmed_sha256='a'*64, recover=True)

    def test_receipt_codec_rejects_approval_and_unknown_current_authority(self):
        host = ProtectedOperatorHost(config())
        row = dict(native_job_id='original-job', native_status='UNKNOWN', approval_verified=False)
        self.assertEqual(host._decode_result('recover', row), row)
        for changed in (dict(row, approval_verified=True), dict(row, current_record_sha256='a'*64), dict(row, extra=True)):
            with self.assertRaises(ChannelError): host._decode_result('recover', changed)

    def test_real_owned_worker_handshake_and_terminal_close(self):
        host = ProtectedOperatorHost(config(), SharingHostSettings(enabled=True))
        try:
            host.start(); pid = host.worker_pid
            self.assertIsInstance(pid, int)
            host.start(); self.assertEqual(host.worker_pid, pid)
        finally: self.assertTrue(host.close())
        with self.assertRaisesRegex(RuntimeError, 'RETIRED'): host.start()

    def test_parent_binds_receipt_to_original_review_and_reconnect_withholds(self):
        host = ProtectedOperatorHost(config())
        review = dict(command_sha256='a'*64, policy_sha256=config().policy.sha256,
            proposed_record_sha256='b'*64, native_job_id='original-job')
        with self.assertRaises(ChannelError):
            host._request_values('recover', {}, {'confirmed_sha256': 'a'*64})
        host._validate_result_scope('prepare', {}, review)
        host._request_values('recover', {}, {'confirmed_sha256': 'a'*64})
        with self.assertRaises(ChannelError):
            host._validate_result_scope('recover', {'confirmed_sha256': 'a'*64}, {'native_job_id': 'foreign-job'})
        replacement = ProtectedOperatorHost(config())
        with self.assertRaises(ChannelError):
            replacement._request_values('recover', {}, {'confirmed_sha256': 'a'*64})

    def test_read_only_config_roundtrips_without_role_assignments(self):
        current = replace(config(), policy=None)
        host = ProtectedOperatorHost(current)
        self.assertEqual(config_from_wire(host._config_values()), current)
        host._request_values('current', {}, {})
        with self.assertRaisesRegex(RuntimeError, 'PROPOSAL_UNCONFIGURED'):
            host._request_values('prepare', {}, {})

    def test_measurement_configuration_and_transport_do_not_accept_browser_pins(self):
        from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
        settings=MeasurementRuntimeSettings('ci-poc-marketing',config().manifest.principal_ids[0],'a'*64,'b'*64)
        host=ProtectedOperatorHost(replace(config(),measurement=settings))
        self.assertEqual(config_from_wire(host._config_values()),host.config)
        with self.assertRaises(ChannelError):request_values('measurement_current',{}, {'pins':{}})
        measurement=Mock();measurement.current.return_value={'release_allowed':False}
        self.assertEqual(execute(Mock(),host.config,'measurement_current',{}, {},measurement),{'release_allowed':False})
        with self.assertRaises(ChannelError):execute(Mock(),host.config,'measurement_current',{}, {})

    def test_measurement_receipt_must_keep_all_writes_and_release_disabled(self):
        host=ProtectedOperatorHost(config())
        value=dict(runtime_installed=True,workspace_revision=1,membership_revision=1,publisher_verified=True,
            measurement_writes_enabled=False,approval_writes_enabled=False,qualifying_evaluation_configured=False,release_allowed=False)
        self.assertEqual(host._decode_result('measurement_current',value),value)
        for changes in (dict(release_allowed=True),dict(measurement_writes_enabled=True),dict(publisher_verified=False),dict(workspace_revision=True)):
            with self.assertRaises(ChannelError):host._decode_result('measurement_current',value|changes)

    def test_planner_route_is_separate_pinned_and_browser_cannot_supply_inputs(self):
        from tests.test_campaign_planner_runtime import PlannerRuntimeTests
        from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
        planner=PlannerRuntimeTests().settings()
        measurement=MeasurementRuntimeSettings('ci-poc-marketing',config().manifest.principal_ids[0],'a'*64,'b'*64)
        host=ProtectedOperatorHost(replace(config(),policy=None,measurement=measurement,planner=planner))
        self.assertEqual(config_from_wire(host._config_values()),host.config)
        for method in ('planner_run','planner_recover'):
            with self.assertRaises(ChannelError):request_values(method,{},dict(binding_id='browser'))
            host._request_values(method,{}, {})
        service=Mock();execute(Mock(),host.config,'planner_recover',{}, {},planner=service)
        service.execute.assert_called_once_with({},recover_only=True)
        with self.assertRaises(ChannelError):ProtectedOperatorHost(config())._request_values('planner_run',{}, {})

    def test_old_wire_configuration_still_reconstructs_without_planner_field(self):
        host=ProtectedOperatorHost(config());wire=host._config_values();wire.pop('planner')
        self.assertEqual(config_from_wire(wire),config())

    def test_planner_receipt_cannot_claim_full_qualification_or_cross_case(self):
        host=ProtectedOperatorHost(config())
        value=dict(stage_record_id='a'*64,response_sha256='b'*64,returned_model_id='gemini-3.5-flash',usage=dict.fromkeys(('input_tokens','output_tokens','cached_tokens','thinking_tokens')),
            scoring_record_id='c'*64,checks=dict(schema_and_enum_projection=dict(status='PASS',projection_sha256='d'*64),fresh_source_check='PASS',full_47_check_evaluation='UNAVAILABLE',retrieval_measurement='UNAVAILABLE',
                rendered_artifact_evaluation='UNAVAILABLE'),qualifying=False,release_allowed=False,run_id='pilot',case_id='case')
        self.assertEqual(host._decode_result('planner_run',value),value)
        for changes in (dict(qualifying=True),dict(release_allowed=True),dict(checks={})):
            with self.assertRaises(ChannelError):host._decode_result('planner_run',value|changes)
        with self.assertRaises(ChannelError):host._validate_result_scope('planner_run',{},value)
