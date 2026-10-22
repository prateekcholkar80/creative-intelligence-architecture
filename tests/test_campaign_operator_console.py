"""Protected read-only console configuration; no cloud writes or approvals."""
from unittest import TestCase
from unittest.mock import Mock, patch
from src.ui import campaign_operator_console as console
from src.application.identity import IapSettings
from tests.test_campaign_permission_lifecycle import identity, NOW, AUDIENCE


class OperatorConsoleTests(TestCase):
    def test_image_failure_exposes_only_fixed_code_in_ui_and_logs(self):
        from src.application.sharing_operation_host import SharingHostError
        for error,expected in ((SharingHostError('PRODUCT_IMAGE_ORIGINAL_MISSING'),'PRODUCT_IMAGE_ORIGINAL_MISSING'),
                (SharingHostError('OPERATION_DEADLINE_EXCEEDED'),'OPERATION_DEADLINE_EXCEEDED'),
                (SharingHostError('OPERATOR_FAILURE_ATTRIBUTE_L201'),'OPERATOR_FAILURE_ATTRIBUTE_L201'),
                (SharingHostError('PRODUCT_IMAGE_private@example.com'),'RUNTIME_UNAVAILABLE'),
                (RuntimeError('secret token'),'RUNTIME_UNAVAILABLE')):
            st=Mock()
            with self.assertLogs(console.__name__,level='WARNING') as logs:console.show_product_image_failure(st,error)
            st.caption.assert_called_once_with('Diagnostic reference: '+expected)
            self.assertNotIn('secret',str(logs.output));self.assertNotIn('private@example.com',str(logs.output))

    def test_assignment_is_explicit_and_no_reviewer_policy_is_invented(self):
        config = console.read_only_config(IapSettings(AUDIENCE), identity().principal.principal_id, now=NOW)
        self.assertFalse(config.writes_enabled); self.assertIsNone(config.policy)
        self.assertEqual((config.manifest.valid_until-NOW).total_seconds(), 300)
        self.assertEqual(config.manifest.principal_ids, (identity().principal.principal_id,))
        with self.assertRaises(ValueError): console.read_only_config(IapSettings(AUDIENCE), '', now=NOW)

    def test_foreign_identity_denied_before_host_start(self):
        with patch.dict('os.environ', {'CI_OPERATOR_PUBLISHER_PRINCIPAL_ID': 'iap-v1:'+'f'*64}), patch.object(console, 'ProtectedOperatorHost') as host:
            with self.assertRaisesRegex(ValueError, 'ACCESS_DENIED'):
                console.render(Mock(), {}, identity(), Mock(iap=IapSettings(AUDIENCE)))
            host.assert_not_called()

    def test_measurement_assignment_is_separate_and_roundtrips_disabled(self):
        from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
        from src.application.campaign_operator_host import ProtectedOperatorHost,config_from_wire
        principal=identity().principal.principal_id
        settings=MeasurementRuntimeSettings('ci-poc-marketing',principal,'a'*64,'b'*64)
        config=console.read_only_config(IapSettings(AUDIENCE),principal,now=NOW,measurement=settings)
        self.assertEqual(config_from_wire(ProtectedOperatorHost(config)._config_values()).measurement,settings)
        self.assertFalse(config.writes_enabled)

    def test_fixed_attempt_choices_preserve_original_recovery_and_current_pin(self):
        from pathlib import Path
        from dataclasses import replace
        from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
        from src.application.campaign_operator_host import ProtectedOperatorHost,config_from_wire
        principal=identity().principal.principal_id
        measurement=MeasurementRuntimeSettings('ci-poc-marketing',principal,'a'*64,'b49cebb51b4d4af87f6285a2f5f75f4da19526f1c50d60ace3d16d5dc7740d29')
        choices=console.evaluation_choices(Path('config/evaluation/interpretation_pilot_deployment.json').resolve(),measurement)
        current=choices['Stage/source v5 attempt'];original=choices['Original failed attempt (recovery only)']
        self.assertTrue(choices['Previous v4 attempt (recovery only)'].recovery_only)
        self.assertFalse(current.recovery_only);self.assertTrue(original.recovery_only)
        self.assertEqual(original.run_id,'interpretation-pilot-20261018-v1')
        self.assertEqual(original.evaluator_sha256,'7cff215eaf3d69ff8fe7291c249f9a6d945e236e4679969beb20d58972d0c5ec')
        self.assertNotEqual(current.run_id,original.run_id)
        for evaluation in (current,original):
            config=console.read_only_config(IapSettings(AUDIENCE),principal,now=NOW,measurement=replace(measurement,evaluator_sha256=evaluation.evaluator_sha256),evaluation=evaluation)
            self.assertEqual(config_from_wire(ProtectedOperatorHost(config)._config_values()).evaluation,evaluation)
            self.assertFalse(config.writes_enabled)
        with self.assertRaisesRegex(ValueError,'MEASUREMENT_BINDING'):
            console.evaluation_choices(Path('config/evaluation/interpretation_pilot_deployment.json').resolve(),replace(measurement,evaluator_sha256='b'*64))

    def test_bundle_cannot_make_original_dispatchable(self):
        import json,tempfile
        from pathlib import Path
        from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
        value=json.loads(Path('config/evaluation/interpretation_pilot_deployment.json').read_text())
        value['original_recovery']['recovery_only']=False
        measurement=MeasurementRuntimeSettings('ci-poc-marketing',identity().principal.principal_id,'a'*64,value['current']['evaluator_sha256'])
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'config.json';path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'LANE_CONFIGURATION'):console.evaluation_choices(path,measurement)

    def test_lane_switch_reconnects_and_original_never_offers_run(self):
        import json
        from pathlib import Path
        from types import SimpleNamespace
        value=json.loads(Path('config/evaluation/interpretation_pilot_deployment.json').read_text())
        principal=identity().principal.principal_id
        environment=dict(CI_OPERATOR_PUBLISHER_PRINCIPAL_ID=principal,CI_MEASUREMENT_RUNTIME_ENABLED='true',
            CI_MEASUREMENT_WORKSPACE_ID='ci-poc-marketing',CI_MEASUREMENT_PUBLISHER_PRINCIPAL_ID=principal,
            CI_MEASUREMENT_REVIEW_SHA256='a'*64,CI_MEASUREMENT_EVALUATOR_SHA256=value['current']['evaluator_sha256'],
            CI_INTERPRETATION_EVALUATION_CONFIG=str(Path('config/evaluation/interpretation_pilot_deployment.json').resolve()))
        class Host:
            def __init__(self,config,*args):
                self.config=config;self.start=Mock();self.close=Mock(return_value=True)
                self.evaluation_run=Mock(return_value={});self.evaluation_recover=Mock(return_value={})
        st=Mock();st.session_state={};st.selectbox.return_value='Stage/source v5 attempt';st.button.return_value=False
        with patch.dict('os.environ',environment),patch.object(console,'ProtectedOperatorHost',Host):
            console.render(st,{},identity(),SimpleNamespace(iap=IapSettings(AUDIENCE)))
            first=st.session_state[console.SESSION_KEY]
            st.selectbox.return_value='Original failed attempt (recovery only)'
            st.button.reset_mock();st.button.side_effect=lambda label:label=='Recover the original evaluation records'
            console.render(st,{},identity(),SimpleNamespace(iap=IapSettings(AUDIENCE)))
            first.close.assert_called_once()
            second=st.session_state[console.SESSION_KEY]
            self.assertIsNot(first,second);self.assertTrue(second.config.evaluation.recovery_only)
            second.evaluation_recover.assert_called_once();second.evaluation_run.assert_not_called()
            self.assertNotIn('Run the configured interpretation evaluation',[c.args[0] for c in st.button.call_args_list])

    def test_v4_recovery_lane_has_exact_prior_pin_and_cannot_become_current(self):
        import json,tempfile
        from pathlib import Path
        from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
        value=json.loads(Path('config/evaluation/interpretation_pilot_deployment.json').read_text())
        measurement=MeasurementRuntimeSettings('ci-poc-marketing',identity().principal.principal_id,'a'*64,value['current']['evaluator_sha256'])
        choices=console.evaluation_choices(Path('config/evaluation/interpretation_pilot_deployment.json').resolve(),measurement)
        previous=choices['Previous v4 attempt (recovery only)']
        self.assertEqual(previous.evaluator_sha256,'f8325305e75637668f509e0979dfb07099aa8a938f01e084b7f8845f7ab1cb54')
        self.assertEqual(previous.run_id,'interpretation-pilot-20261018-v2')
        value['previous_recovery']['recovery_only']=False
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'config.json';path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'LANE_CONFIGURATION'):console.evaluation_choices(path,measurement)
