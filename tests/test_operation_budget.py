"""4.4 Critical/Must have/shared pilot: monotonic/cooperative budget behavior."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import FrozenInstanceError
from threading import Event, RLock
import unittest
from unittest.mock import Mock, patch

from src.application.operation_budget import (
    CancellationSignal, OperationBudget, OperationSettings, OperationStopped,
    check_operation, operation_lock, operation_scope, operation_timeout, operation_retry,
    operation_call, operation_iter,
)


class OperationBudgetTests(unittest.TestCase):
    def setUp(self):
        self.tick = 100.0
        p = patch('src.application.operation_budget.time.monotonic', side_effect=lambda:self.tick)
        p.start();self.addCleanup(p.stop)

    def test_settings_are_bounded_and_immutable(self):
        for value in (0, 601, True, 1.5, '10', None):
            with self.subTest(value=value),self.assertRaises(ValueError):OperationSettings(value)
        with self.assertRaises(FrozenInstanceError):OperationSettings().timeout_seconds=600

    def test_invalid_budget_shapes_rejected(self):
        signal=CancellationSignal()
        for deadline,signals in ((float('nan'),(signal,)),(float('inf'),(signal,)),
                                (True,(signal,)),(110,()),(110,[signal]),(110,(Mock(),))):
            with self.subTest(deadline=deadline),self.assertRaises(ValueError):
                OperationBudget(deadline,signals)

    def test_cumulative_elapsed_time_is_not_reset_at_checkpoints(self):
        with operation_scope(OperationSettings(10)) as budget:
            self.tick+=3;check_operation()
            self.tick+=4;check_operation()
            self.assertEqual(budget.remaining(),3)
            self.tick+=2
            self.assertEqual(budget.remaining(),1)

    def test_exact_deadline_denies_late_success_and_context_is_restored(self):
        with self.assertRaises(OperationStopped) as caught:
            with operation_scope(OperationSettings(10)):
                self.tick+=10
        self.assertEqual(caught.exception.code,'OPERATION_DEADLINE_EXCEEDED')
        check_operation()  # No stopped budget leaks into the next independent request.
        with operation_scope(OperationSettings(10)):check_operation()

    def test_nested_longer_scope_cannot_renew_parent(self):
        with operation_scope(OperationSettings(10)) as parent:
            self.tick+=8
            with operation_scope(OperationSettings(600)) as child:
                self.assertEqual(child.deadline,parent.deadline)
                self.assertEqual(child.remaining(),2)

    def test_nested_shorter_scope_restores_unchanged_parent(self):
        with operation_scope(OperationSettings(20)) as parent:
            with operation_scope(OperationSettings(5)) as child:
                self.assertEqual(child.deadline,105)
            self.tick+=7
            self.assertEqual(parent.remaining(),13)

    def test_nested_expiry_cannot_be_replaced_by_new_scope(self):
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(5)):
                self.tick+=5
                with operation_scope(OperationSettings(600)):self.fail('expired scope renewed')

    def test_one_way_signal_stops_nested_scopes_without_private_reason(self):
        signal=CancellationSignal()
        with self.assertRaises(OperationStopped) as caught:
            with operation_scope(OperationSettings(10),signal):
                with operation_scope(OperationSettings(600)):
                    signal.cancel();signal.cancel();check_operation()
        self.assertTrue(signal.cancelled)
        self.assertEqual(str(caught.exception),'OPERATION_CANCELLED')
        self.assertFalse(hasattr(signal,'reset'))

    def test_parent_signal_cannot_be_replaced_by_child_signal(self):
        parent,child=CancellationSignal(),CancellationSignal()
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10),parent):
                with operation_scope(OperationSettings(10),child):
                    parent.cancel();check_operation()

    def test_child_signal_can_also_stop_nested_work(self):
        signal=CancellationSignal()
        with operation_scope(OperationSettings(20)):
            with self.assertRaises(OperationStopped):
                with operation_scope(OperationSettings(10),signal):
                    signal.cancel();check_operation()
            check_operation()  # Cancellation stays scoped to its supplied child.

    def test_cancelled_request_never_enters_body(self):
        signal=CancellationSignal();signal.cancel()
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10),signal):self.fail('entered cancelled body')

    def test_invalid_composition_and_stop_codes_rejected(self):
        for settings,signal in (({},None),(OperationSettings(),Mock())):
            with self.assertRaises(ValueError):
                with operation_scope(settings,signal):pass
        with self.assertRaises(ValueError):OperationStopped('private provider detail')

    def test_exception_and_baseexception_restore_parent_context(self):
        for kind in (ValueError,KeyboardInterrupt):
            with operation_scope(OperationSettings(20)) as parent:
                with self.assertRaises(kind):
                    with operation_scope(OperationSettings(10)):raise kind()
                self.assertEqual(parent.remaining(),20)
            check_operation()

    def test_lock_wait_is_cancellable_without_waiting_for_owner(self):
        lock=RLock();signal=CancellationSignal();queued=Event()
        def wait():
            with operation_scope(OperationSettings(10),signal):
                queued.set()
                with operation_lock(lock):self.fail('waiter acquired owner lock')
        with lock,ThreadPoolExecutor(max_workers=1) as pool:
            waiting=pool.submit(wait)
            self.assertTrue(queued.wait(1));signal.cancel()
            with self.assertRaises(OperationStopped):waiting.result(timeout=1)

    def test_lock_poll_uses_only_remaining_time_and_never_releases_unowned_lock(self):
        lock=Mock()
        def acquire(**kwargs):
            self.assertLessEqual(kwargs['timeout'],0.05)
            self.tick+=1
            return False
        lock.acquire.side_effect=acquire
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(2)):
                with operation_lock(lock):self.fail('unowned lock')
        self.assertEqual(lock.acquire.call_count,2)
        lock.release.assert_not_called()

    def test_stop_after_lock_acquisition_releases_it_before_body(self):
        lock=Mock()
        def acquire(**kwargs):self.tick+=5;return True
        lock.acquire.side_effect=acquire
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(5)):
                with operation_lock(lock):self.fail('late lock body')
        lock.release.assert_called_once()

    def test_acquired_lock_released_on_cancelled_body(self):
        lock=Mock();lock.acquire.return_value=True;signal=CancellationSignal()
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10),signal):
                with operation_lock(lock):signal.cancel();check_operation()
        lock.release.assert_called_once()

    def test_lock_requires_explicit_scope(self):
        with self.assertRaises(ValueError):
            with operation_lock(Mock()):pass

    def test_explicit_context_propagation_preserves_parent_deadline(self):
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10)):
                ctx=copy_context()
                self.tick+=10
                with ThreadPoolExecutor(max_workers=1) as pool:
                    with self.assertRaises(OperationStopped):pool.submit(ctx.run,check_operation).result(timeout=1)

    def test_normal_lock_body_released_once(self):
        lock=Mock();lock.acquire.return_value=True
        with operation_scope(OperationSettings(10)):
            with operation_lock(lock):check_operation()
        lock.release.assert_called_once()

    def test_provider_cap_is_recomputed_and_does_not_extend_parent(self):
        with operation_scope(OperationSettings(10)):
            self.assertEqual(operation_timeout(5),5)
            self.tick+=8
            self.assertEqual(operation_timeout(5),2)
        self.assertEqual(operation_timeout(20),20)

    def test_nonfinite_unbounded_or_invalid_caps_rejected(self):
        for cap in (None,True,0,-1,float('inf'),float('nan'),'10'):
            with self.subTest(cap=cap),self.assertRaises(ValueError):operation_timeout(cap)

    def test_retries_disabled_only_in_operation_context(self):
        retry=object()
        self.assertIs(operation_retry(retry),retry)
        with operation_scope(OperationSettings(10)):self.assertIsNone(operation_retry(retry))
        self.assertIs(operation_retry(retry),retry)

    def test_rpc_uses_remaining_time_and_preserves_arguments(self):
        rpc=Mock(return_value='result')
        with operation_scope(OperationSettings(10)):
            self.tick+=7
            self.assertEqual(operation_call(rpc,'stable-id',timeout=20,retry=None),'result')
        rpc.assert_called_once_with('stable-id',timeout=3,retry=None)

    def test_stopped_operation_never_calls_provider(self):
        rpc=Mock()
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10)):
                self.tick+=10;operation_call(rpc,timeout=20)
        rpc.assert_not_called()

    def test_late_provider_success_and_failure_both_report_safe_stop(self):
        for fail in (False,True):
            def rpc(**kwargs):
                self.tick+=10
                if fail:raise ValueError('private provider response')
                return 'late success'
            with self.subTest(fail=fail),self.assertRaises(OperationStopped) as caught:
                with operation_scope(OperationSettings(10)):operation_call(rpc,timeout=20)
            self.assertEqual(str(caught.exception),'OPERATION_DEADLINE_EXCEEDED')

    def test_timely_provider_failure_is_preserved(self):
        with operation_scope(OperationSettings(10)),self.assertRaises(ValueError):
            operation_call(Mock(side_effect=ValueError()),timeout=20)

    def test_lazy_iterator_checks_before_next_page(self):
        calls=[]
        def pages():
            calls.append('first');yield 1
            calls.append('second');yield 2
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10)):
                values=operation_iter(pages());self.assertEqual(next(values),1)
                self.tick+=10;next(values)
        self.assertEqual(calls,['first'])

    def test_stopped_scope_never_constructs_provider_iterator(self):
        values=Mock();values.__iter__=Mock(return_value=iter(()))
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10)):
                self.tick+=10;list(operation_iter(values))
        values.__iter__.assert_not_called()

    def test_late_empty_iterator_is_rejected(self):
        def empty():
            self.tick+=10
            return
            yield
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10)):list(operation_iter(empty()))

    def test_lazy_provider_exception_after_cancel_does_not_mask_stop(self):
        signal=CancellationSignal()
        def fail():
            signal.cancel();raise ValueError('private page response')
            yield
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10),signal):list(operation_iter(fail()))


if __name__=='__main__':unittest.main()
