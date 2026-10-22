from copy import deepcopy
from dataclasses import asdict
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_serving_startup as startup
from tests.test_workflow_submission_service import settings
from tests.test_identity import AUDIENCE

class StartupTests(TestCase):
    def config(self):
        value=asdict(settings());value['model_profiles']=list(value['model_profiles'])
        return dict(iap_audience=AUDIENCE,workspace_id='workspace',conversation_id='conversation',submission=value)
    def test_unconfigured_startup_leaves_local_and_existing_runtime_untouched(self):
        self.assertIsNone(startup.startup({}));self.assertIsNone(startup.startup({'CI_DURABLE_WORKFLOW_UI_ENABLED':'true'}))
    def test_prestarts_hosts_before_install_and_never_installs_model_worker(self):
        hosts=[Mock(retired=False,worker_pid=i+1) for i in range(3)]
        with patch.object(startup,'_INSTALLED',None),patch.object(startup,'_CONFIG',None),patch.object(startup,'get_workflow_runtime',return_value=None),patch.object(startup,'WorkflowSubmissionHost',return_value=hosts[0]),patch.object(startup,'WorkflowHistoryHost',return_value=hosts[1]),patch.object(startup,'WorkflowReadHost',return_value=hosts[2]),patch.object(startup,'WorkflowUiRuntime',return_value='runtime'),patch.object(startup,'install_workflow_runtime') as install,patch.object(startup.atexit,'register'):
            config=self.config();self.assertEqual(startup.install_from_config(config,b'x'*32),'runtime')
            for host in hosts:host.start.assert_called_once()
            install.assert_called_once_with('runtime')
            self.assertEqual(startup.install_from_config(config,b'x'*32),'runtime')
            changed=deepcopy(config);changed['conversation_id']='other'
            with self.assertRaises(ValueError):startup.install_from_config(changed,b'x'*32)
    def test_failed_start_closes_every_host_and_does_not_install(self):
        hosts=[Mock() for _ in range(3)];hosts[1].start.side_effect=ValueError('not ready')
        with patch.object(startup,'_INSTALLED',None),patch.object(startup,'get_workflow_runtime',return_value=None),patch.object(startup,'WorkflowSubmissionHost',return_value=hosts[0]),patch.object(startup,'WorkflowHistoryHost',return_value=hosts[1]),patch.object(startup,'WorkflowReadHost',return_value=hosts[2]),patch.object(startup,'install_workflow_runtime') as install:
            with self.assertRaises(ValueError):startup.install_from_config(self.config(),b'x'*32)
            for host in hosts:host.close.assert_called_once()
            install.assert_not_called()
    def test_missing_cursor_material_denies_before_spawning(self):
        with self.assertRaises(ValueError):startup.install_from_config(self.config(),b'')
