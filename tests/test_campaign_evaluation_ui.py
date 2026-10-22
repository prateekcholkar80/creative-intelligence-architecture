from datetime import datetime,timezone
from unittest import TestCase
from src.ui.campaign_operator_console import read_only_config
from src.application.identity import IapSettings
from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
from tests.test_campaign_interpretation_evaluation import settings
from src.application.campaign_operator_host import config_from_wire,ProtectedOperatorHost
from src.application.sharing_operation_host import SharingHostSettings


class EvaluationUiConfigTests(TestCase):
    def test_evaluation_configuration_is_separate_from_policy_writes(self):
        audience='/projects/1042059373096/locations/asia-south1/services/ci-prompt-registrar'
        actor='iap-v1:'+'a'*64;measurement=MeasurementRuntimeSettings('workspace',actor,'b'*64,'d'*64)
        config=read_only_config(IapSettings(audience),actor,now=datetime.now(timezone.utc),measurement=measurement,evaluation=settings(enabled=True))
        self.assertFalse(config.writes_enabled);self.assertIsNone(config.policy);self.assertEqual(config.operation.timeout_seconds,300)
        host=ProtectedOperatorHost(config,SharingHostSettings(enabled=True))
        self.assertEqual(config_from_wire(host._config_values()),config)
    def test_legacy_read_only_mode_keeps_original_deadline(self):
        config=read_only_config(IapSettings('/projects/1042059373096/locations/asia-south1/services/ci-prompt-registrar'),'iap-v1:'+'a'*64,now=datetime.now(timezone.utc))
        self.assertIsNone(config.evaluation);self.assertFalse(config.writes_enabled);self.assertEqual(config.operation.timeout_seconds,120)
