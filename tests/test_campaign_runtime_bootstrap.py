from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch,Mock
from src.application.campaign_runtime_bootstrap import install_campaign_runtime
from src.application import interpreter_worker as workers
from src.ui import workflow_runtime as workflows
from src.application.campaign_chat_evidence_service import ProtectedEvidenceChatService
from tests import test_interpreter_worker as fixtures
from tests.test_workflow_runtime import hosts,WS,CONV

class CampaignRuntimeInstallTests(TestCase):
    def setup_pair(self):
        fixture=fixtures.InterpreterWorkerTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        worker=fixture.worker(False);evidence=object.__new__(ProtectedEvidenceChatService)
        objects=hosts();settings=objects[0].config.iap
        evidence.client=object();evidence.verifier=SimpleNamespace(settings=settings)
        worker.admission.io=SimpleNamespace(client=evidence.client)
        from src.application.campaign_evidence_dispatch import EvidenceIntentAdapter
        worker.admission.verifier=evidence.verifier;worker.admission._call=object.__new__(EvidenceIntentAdapter)
        worker.publisher.io=SimpleNamespace(client=evidence.client);worker.publisher.verifier=evidence.verifier
        runtime=workflows.WorkflowUiRuntime(*objects,workflows.WorkflowUiSettings(WS,CONV),evidence_service=evidence)
        return runtime,worker
    def test_atomic_install_reuse_and_no_enablement(self):
        runtime,worker=self.setup_pair()
        with patch.object(workers,'_WORKER',None),patch.object(workflows,'_RUNTIME',None):
            install_campaign_runtime(runtime,worker);install_campaign_runtime(runtime,worker)
            self.assertIs(workers.get_interpreter_worker(),worker)
            self.assertIs(workflows.get_workflow_runtime(),runtime)
            self.assertFalse(worker.enabled)
    def test_conflicting_slot_does_not_partially_install(self):
        runtime,worker=self.setup_pair()
        with patch.object(workers,'_WORKER',None),patch.object(workflows,'_RUNTIME',object()):
            with self.assertRaises(ValueError):install_campaign_runtime(runtime,worker)
            self.assertIsNone(workers.get_interpreter_worker())
    def test_mismatched_clients_deny_before_install(self):
        runtime,worker=self.setup_pair();worker.publisher.io.client=object()
        with patch.object(workers,'_WORKER',None),patch.object(workflows,'_RUNTIME',None):
            with self.assertRaises(ValueError):install_campaign_runtime(runtime,worker)
            self.assertIsNone(workers.get_interpreter_worker());self.assertIsNone(workflows.get_workflow_runtime())

    def test_legacy_model_callback_cannot_be_installed_as_evidence_worker(self):
        runtime,worker=self.setup_pair();worker.admission._call=Mock()
        with patch.object(workers,'_WORKER',None),patch.object(workflows,'_RUNTIME',None):
            with self.assertRaises(ValueError):install_campaign_runtime(runtime,worker)
