from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.interpreter_worker import InterpreterWorker,install_interpreter_worker,get_interpreter_worker
from src.application.generation_admission import ProtectedGenerationAdmission
from src.application.interpreter_outcome import ProtectedInterpreterOutcomeService
from src.application.campaign_evidence_dispatch import EvidenceInvocationDraft
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.operation_budget import OperationStopped

class InterpreterWorkerTests(TestCase):
    def setUp(self):
        self.admission=object.__new__(ProtectedGenerationAdmission)
        self.admission.worker_id='worker';self.admission.settings=SimpleNamespace(require_registered_prompts=True)
        self.draft=EvidenceInvocationDraft('{}','{}','a'*64,'b'*64,ModelResultObservation('SUCCEEDED'))
        self.admission.dispatch=Mock(return_value=self.draft);self.admission._actor=Mock()
        self.rows=dict(workspaces={},memberships={},conversations={},requests={},
            model_invocations=dict(stage='INTERPRETER',worker_id='worker'))
        self.admission._current=Mock(return_value=(None,self.rows,None))
        self.publisher=object.__new__(ProtectedInterpreterOutcomeService);self.publisher.worker_id='worker'
        self.result_plan=object();self.publisher.prepare=Mock(return_value=self.result_plan)
        self.publisher.publish=Mock(return_value='committed');self.publisher.recover=Mock(return_value='recovered')
        self.plan=SimpleNamespace(job_id='original-admission')
        patch('src.application.interpreter_worker.require_owned_worker').start();self.addCleanup(patch.stopall)
    def worker(self,enabled=True):return InterpreterWorker(admission=self.admission,publisher=self.publisher,enabled=enabled)

    def test_success_dispatches_and_publishes_once_then_recovers_without_model(self):
        worker=self.worker();result=worker.run({},self.plan)
        self.assertFalse(result.release_allowed);self.assertIs(result.draft,self.draft)
        self.assertIs(self.publisher.prepare.call_args.kwargs['observation'],self.draft.observation)
        self.assertEqual(self.publisher.prepare.call_args.kwargs['response_text'],self.draft.response_text)
        with self.assertRaises(ValueError):worker.run({},self.plan)
        self.assertEqual(worker.recover({},self.plan.job_id).outcome,'recovered')
        self.admission.dispatch.assert_called_once();self.publisher.publish.assert_called_once()
        self.publisher.recover.assert_called_once_with({},self.result_plan)

    def test_uncertain_provider_is_recorded_unknown_and_never_retried(self):
        self.admission.dispatch.side_effect=RuntimeError('provider body')
        worker=self.worker();result=worker.run({},self.plan)
        self.assertIsNone(result.draft)
        observation=self.publisher.prepare.call_args.kwargs['observation']
        self.assertEqual(observation.state,'UNKNOWN');self.assertIsNone(observation.input_tokens)
        worker.recover({},self.plan.job_id);self.admission.dispatch.assert_called_once()

    def test_lost_publish_response_retains_original_recovery_plan(self):
        worker=self.worker();self.publisher.publish.side_effect=RuntimeError('lost response')
        with self.assertRaises(RuntimeError):worker.run({},self.plan)
        worker.recover({},self.plan.job_id)
        self.publisher.recover.assert_called_once_with({},self.result_plan)
        self.publisher.prepare.assert_called_once();self.admission.dispatch.assert_called_once()

    def test_disabled_unregistered_and_missing_recovery_deny(self):
        with self.assertRaises(ValueError):self.worker(False).run({},self.plan)
        self.admission.dispatch.assert_not_called()
        with self.assertRaises(ValueError):self.worker().recover({},'missing')
        self.admission.settings.require_registered_prompts=False
        with self.assertRaises(ValueError):self.worker()

    def test_host_stop_prevents_publication_or_further_io(self):
        self.admission.dispatch.side_effect=OperationStopped('OPERATION_DEADLINE_EXCEEDED')
        with self.assertRaises(OperationStopped):self.worker().run({},self.plan)
        self.admission._current.assert_not_called();self.publisher.prepare.assert_not_called()

    def test_installation_is_trusted_once_only_and_does_not_enable(self):
        with patch('src.application.interpreter_worker._WORKER',None):
            worker=self.worker(False);install_interpreter_worker(worker)
            self.assertIs(get_interpreter_worker(),worker);self.assertFalse(worker.enabled)
            install_interpreter_worker(worker)
            with self.assertRaises(ValueError):install_interpreter_worker(self.worker(False))
