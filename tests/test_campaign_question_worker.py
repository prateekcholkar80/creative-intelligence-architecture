from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS
from unittest import TestCase
from unittest.mock import Mock, patch
from contextlib import ExitStack

from src.application.campaign_question_worker import RegisteredQuestionWorker
from src.application.campaign_question_writer import ProtectedQuestionWriter
from src.application.prompt_runtime_service import PromptRuntimeService

MODULE = 'src.application.campaign_question_worker.'


class QuestionWorkerTests(TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.writer = object.__new__(ProtectedQuestionWriter)
        for name in ('client', 'verifier', 'access_reader', 'submit', 'admission_context'):
            setattr(self.writer, name, Mock())
        self.writer.worker_id = 'worker'
        self.writer.profile = {'profile': 'test-pin'}
        self.runtime = object.__new__(PromptRuntimeService)
        self.provider, self.gaps = Mock(), Mock()
        self.parent = NS(workspace_id='ws', conversation_id='conv', request_id='parent',
                         invocation_id='parent-call', native_status='COMMITTED')
        self.child = NS(workspace_id='ws', conversation_id='conv', request_id='child',
                        invocation_id='child-call', native_status='COMMITTED')
        self.writer.submit.return_value = self.child
        self.context = NS(initial_gaps=NS(policy_approval_verified=True,
            valid_until=self.now + timedelta(seconds=30)), admitted=NS(
            valid_until=self.now + timedelta(seconds=30)), proof=object(),
            plan=NS(config=lambda: NS(query_parameters=[NS(name='request_candidate', value='encoded')])) )
        self.writer.admission_context.return_value = self.context

    def make(self, enabled=True):
        return RegisteredQuestionWorker(writer=self.writer, runtime=self.runtime,
            provider_transport=self.provider, refresh_gaps=self.gaps,
            enabled=enabled, clock=lambda: self.now)

    def test_disabled_does_not_read_write_or_call_model(self):
        with patch(MODULE+'require_owned_worker'):
            result = self.make(False).process({}, self.parent)
        self.assertFalse(result.release_allowed)
        self.writer.submit.assert_not_called()
        self.provider.assert_not_called()

    def test_unknown_child_is_held_without_composing_dispatch(self):
        self.child.native_status = 'UNKNOWN'
        with patch(MODULE+'require_owned_worker'), patch(MODULE+'ProtectedGenerationAdmission') as admission:
            self.assertEqual(self.make().process({}, self.parent).reason, 'QUESTION_CHILD_NOT_COMMITTED')
        admission.assert_not_called()
        self.writer.admission_context.assert_not_called()

    def test_unapproved_capture_cannot_dispatch(self):
        self.context.initial_gaps.policy_approval_verified = False
        with patch(MODULE+'require_owned_worker'), patch(MODULE+'ProtectedGenerationAdmission') as admission:
            self.assertEqual(self.make().process({}, self.parent).reason, 'QUESTION_POLICY_REVIEW_REQUIRED')
        admission.assert_not_called()
        self.provider.assert_not_called()

    def composed(self, stack):
        stack.enter_context(patch(MODULE+'require_owned_worker'))
        stack.enter_context(patch(MODULE+'decode_row', return_value={'created_at': self.now}))
        adapter = stack.enter_context(patch(MODULE+'EvidenceQuestionDispatch'))
        admission = stack.enter_context(patch(MODULE+'ProtectedGenerationAdmission')).return_value
        stack.enter_context(patch(MODULE+'ProtectedInterpreterOutcomeService'))
        worker = stack.enter_context(patch(MODULE+'InterpreterWorker')).return_value
        guard = stack.enter_context(patch(MODULE+'QuestionContextGuard'))
        admission.prepare.return_value = NS(job_id='original-dispatch')
        worker.run.return_value = worker.recover.return_value = NS(outcome=NS(native_status=NS(value='COMMITTED')))
        return adapter, admission, worker, guard

    def test_guard_installed_before_run_and_reconnect_only_recovers(self):
        current = self.make()
        with ExitStack() as stack:
            adapter, admission, worker, guard = self.composed(stack)
            guard.return_value.return_value = 'fresh-gaps'
            def run(*args):
                self.assertEqual(adapter.call_args.kwargs['refresh'](), 'fresh-gaps')
                self.assertEqual(len(current._operations), 1)
                return NS(outcome=NS(native_status=NS(value='COMMITTED')))
            worker.run.side_effect = run
            self.assertFalse(current.process({}, self.parent).release_allowed)
            current.process({}, self.parent)
        worker.run.assert_called_once()
        worker.recover.assert_called_once_with({}, 'original-dispatch')
        self.writer.submit.assert_called_once()
        self.assertTrue(admission is not None)
        guard.call_args.kwargs['read_gaps']()
        self.gaps.assert_called_once_with({}, self.parent)

    def test_dispatch_exception_retains_original_recovery_operation(self):
        current = self.make()
        with ExitStack() as stack:
            _, _, worker, _ = self.composed(stack)
            worker.run.side_effect = RuntimeError('lost response')
            with self.assertRaises(RuntimeError): current.process({}, self.parent)
            current.process({}, self.parent)
        self.writer.submit.assert_called_once()
        worker.run.assert_called_once()
        worker.recover.assert_called_once()

    def test_context_guard_failure_prevents_run(self):
        current = self.make()
        with ExitStack() as stack:
            _, _, worker, guard = self.composed(stack)
            guard.side_effect = ValueError('changed context')
            with self.assertRaises(ValueError): current.process({}, self.parent)
        worker.run.assert_not_called()
        self.assertEqual(current._operations, {})

    def test_quality_callback_only_after_committed_success_and_on_original_recovery(self):
        current=self.make();callback=Mock();current.on_committed=callback
        result=NS(outcome=NS(native_status=NS(value='COMMITTED')),
            draft=NS(observation=NS(state='SUCCEEDED')))
        with ExitStack() as stack:
            _,_,worker,_=self.composed(stack)
            worker.run.return_value=worker.recover.return_value=result
            current.process({},self.parent);current.process({},self.parent)
        self.assertEqual(callback.call_count,2)
        callback.assert_called_with({},self.child,self.context,result)
        worker.run.assert_called_once()

    def test_unknown_publication_does_not_stage_quality(self):
        current=self.make();callback=Mock();current.on_committed=callback
        with ExitStack() as stack:
            _,_,worker,_=self.composed(stack)
            worker.run.return_value=NS(outcome=NS(native_status=NS(value='UNKNOWN')),
                draft=NS(observation=NS(state='SUCCEEDED')))
            current.process({},self.parent)
        callback.assert_not_called()
