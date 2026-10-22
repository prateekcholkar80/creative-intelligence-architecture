"""4.4 owned subprocess enforcement and IPC tests; all cloud RPCs remain offline."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import signal
import socket
import struct
import sys
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import patch

from src.application.identity import IapSettings
from src.application.operation_budget import (
    CancellationSignal, OperationSettings, OperationStopped, operation_scope, worker_operation_scope,
)
from src.application.operation_channel import ChannelError, MAX_FRAME_BYTES, encode, receive
from src.application.sharing_host_contract import (
    SharingWorkerConfig, config_from_wire, pack_result, request_values, unpack_result,
)
from src.application.sharing_executor import (
    SharingExecutorSettings, SharingExecutionOutcome, SharingRetryIssuance, SharingRetryIssuanceState,
)
from src.application.sharing_coordinator_contract import SharingCommitObservation, SharingCommitStatus
from src.application.sharing_query import SharingQueryResult, SharingResultState
from src.application.sharing_runtime import SigningRuntimeSettings
from src.application.sharing_operation_host import SharingHostError, SharingHostSettings, SharingOperationHost
from tests.test_identity import AUDIENCE
from tests.test_sharing_query import query
from tests.test_sharing_signing import SharingRetrySealer, KEY


def config(**changes):
    return SharingWorkerConfig(IapSettings(AUDIENCE),
        SigningRuntimeSettings('pilot-runtime','1042059373096',enabled=True,operation=OperationSettings(2)),
        SharingExecutorSettings(execution_enabled=True)) if not changes else replace(config(),**changes)


@unittest.skipUnless(os.name=='posix','Owned POSIX worker supervision')
class HostTests(unittest.TestCase):
    def setUp(self):
        temp=TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.path=str(Path(temp.name)/'fixture.jsonl')
        self.hosts=[]
        self.addCleanup(self.cleanup)
        self.token=SharingRetrySealer(key=KEY,key_id='old').seal(query())
        self.headers={'x-goog-iap-jwt-assertion':'offline'}

    def cleanup(self):
        for host in self.hosts:
            host.close()
            if host._process is not None:
                host._process.wait(timeout=3)

    def host(self, mode='composed', *, timeout=2):
        cfg=config(runtime=replace(config().runtime,operation=OperationSettings(timeout)))
        host=SharingOperationHost(cfg,SharingHostSettings(enabled=True,cleanup_seconds=.3))
        self.hosts.append(host)
        def command(fd):
            return [sys.executable,'-m','tests.sharing_host_worker_fixture',str(fd),mode,self.path]
        with patch('src.application.sharing_operation_host._worker_command',side_effect=command):host.start()
        return host

    def rows(self):
        return [json.loads(line) for line in Path(self.path).read_text().splitlines()] if Path(self.path).exists() else []

    def wait_record(self, key):
        end=time.monotonic()+4
        while time.monotonic()<end:
            rows=self.rows()
            if any(key in row for row in rows):return rows
            time.sleep(.01)
        self.fail('Fixture synchronization timed out')

    def test_disabled_controls_never_start_worker(self):
        for cfg,settings in ((config(),SharingHostSettings()),
                (config(runtime=replace(config().runtime,enabled=False)),SharingHostSettings(enabled=True)),
                (config(executor=SharingExecutorSettings()),SharingHostSettings(enabled=True))):
            host=SharingOperationHost(cfg,settings)
            with patch('src.application.sharing_operation_host.subprocess.Popen') as popen:
                with self.assertRaises(SharingHostError):host.start()
                with self.assertRaises(SharingHostError):host.submit(self.headers,self.token)
                popen.assert_not_called()

    def test_prestart_required_and_calls_never_launch_implicitly(self):
        host=SharingOperationHost(config(),SharingHostSettings(enabled=True))
        with patch('src.application.sharing_operation_host.subprocess.Popen') as popen:
            with self.assertRaises(SharingHostError) as caught:host.recover(self.headers,self.token)
        self.assertEqual(caught.exception.code,'SHARING_HOST_NOT_STARTED');popen.assert_not_called()

    def test_production_worker_prestart_does_not_discover_adc_or_load_secrets(self):
        host=SharingOperationHost(config(),SharingHostSettings(enabled=True,cleanup_seconds=.3))
        self.hosts.append(host)
        with patch.dict(os.environ,{'GOOGLE_APPLICATION_CREDENTIALS':'/nonexistent/host-prestart-fixture'}):
            host.start()
        self.assertIsNotNone(host.worker_pid);self.assertFalse(host.retired)

    def test_server_host_configuration_cannot_be_reassigned(self):
        host=SharingOperationHost(config())
        for field in ('config','settings'):
            with self.assertRaises(AttributeError):setattr(host,field,None)

    def test_ignored_provider_timeout_is_killed_within_outer_budget(self):
        host=self.host('hang',timeout=1);start=time.monotonic()
        with self.assertRaises(OperationStopped) as caught:host.recover(self.headers,self.token)
        self.assertEqual(caught.exception.code,'OPERATION_DEADLINE_EXCEEDED')
        self.assertLess(time.monotonic()-start,1.5)
        self.assertTrue(host.retired);self.assertTrue(host.cleanup_complete)
        self.assertEqual(len(self.rows()),1)

    def test_partial_frame_cannot_block_receiver_past_deadline(self):
        host=self.host('partial',timeout=1)
        with self.assertRaises(OperationStopped):host.recover(self.headers,self.token)
        self.assertTrue(host.cleanup_complete)

    def test_cancellation_stops_active_worker_without_dispatch_replacement(self):
        host=self.host('hang',timeout=10);cancel=CancellationSignal()
        with ThreadPoolExecutor(max_workers=1) as pool:
            call=pool.submit(host.recover,self.headers,self.token,cancellation=cancel)
            self.wait_record('method');start=time.monotonic();cancel.cancel()
            with self.assertRaises(OperationStopped) as caught:call.result(timeout=2)
        self.assertEqual(caught.exception.code,'OPERATION_CANCELLED')
        self.assertLess(time.monotonic()-start,1)
        with self.assertRaises(SharingHostError):host.recover(self.headers,self.token)
        self.assertEqual(len(self.rows()),1)

    def test_cancelled_waiter_does_not_retire_active_owner(self):
        host=self.host('hang',timeout=10);owner_signal=CancellationSignal();waiter_signal=CancellationSignal()
        with ThreadPoolExecutor(max_workers=2) as pool:
            owner=pool.submit(host.recover,self.headers,self.token,cancellation=owner_signal)
            self.wait_record('method')
            waiter=pool.submit(host.recover,self.headers,self.token,cancellation=waiter_signal)
            waiter_signal.cancel()
            with self.assertRaises(OperationStopped):waiter.result(timeout=1)
            self.assertFalse(host.retired);self.assertIsNone(host._process.poll())
            owner_signal.cancel()
            with self.assertRaises(OperationStopped):owner.result(timeout=2)
        self.assertEqual(len(self.rows()),1)

    def test_close_cancels_active_operation_and_is_bounded(self):
        host=self.host('hang',timeout=10)
        with ThreadPoolExecutor(max_workers=1) as pool:
            active=pool.submit(host.recover,self.headers,self.token)
            self.wait_record('method');start=time.monotonic();host.close()
            self.assertLess(time.monotonic()-start,.8)
            with self.assertRaises(OperationStopped):active.result(timeout=2)
        self.assertTrue(host.cleanup_complete)

    def test_blocked_worker_cleanup_escalates_to_kill(self):
        host=self.host('cleanup');self.wait_record('cleanup');start=time.monotonic()
        self.assertTrue(host.close())
        self.assertLess(time.monotonic()-start,.8)
        self.assertEqual(host._process.returncode,-signal.SIGKILL)

    def test_startup_handshake_timeout_retires_owned_process(self):
        host=SharingOperationHost(config(),SharingHostSettings(enabled=True,startup_seconds=1,cleanup_seconds=.3))
        self.hosts.append(host)
        command=lambda fd:[sys.executable,'-m','tests.sharing_host_worker_fixture',str(fd),'startup-hang',self.path]
        with patch('src.application.sharing_operation_host._worker_command',side_effect=command):
            with self.assertRaises(OperationStopped):host.start()
        self.assertTrue(host.retired);self.assertTrue(host.cleanup_complete)

    def test_owned_credential_helper_group_is_signalled_on_worker_stop(self):
        host=self.host('helper',timeout=10);cancel=CancellationSignal()
        with ThreadPoolExecutor(max_workers=1) as pool:
            active=pool.submit(host.recover,self.headers,self.token,cancellation=cancel)
            rows=self.wait_record('helper_ready');helper=next(row['helper_ready'] for row in rows if 'helper_ready' in row)
            cancel.cancel()
            with self.assertRaises(OperationStopped):active.result(timeout=2)
        self.assertIn({'helper_stopped':helper},self.wait_record('helper_stopped'))

    def test_repeated_close_never_resignals_reaped_worker_group(self):
        host=self.host();host.close()
        with patch('src.application.sharing_operation_host.os.killpg') as kill:
            self.assertTrue(host.close())
        kill.assert_not_called()

    def test_malformed_and_uncorrelated_replies_retire_without_exposing_payload(self):
        for mode in ('mismatch','private-error','death'):
            with self.subTest(mode=mode):
                host=self.host(mode)
                with self.assertRaises(SharingHostError) as caught:host.recover(self.headers,self.token)
                self.assertEqual(str(caught.exception),'SHARING_HOST_PROTOCOL_FAILED')
                self.assertTrue(host.retired);self.assertTrue(host.cleanup_complete)

    def test_late_success_is_never_released(self):
        host=self.host('late',timeout=1)
        with self.assertRaises(OperationStopped):host.issue_intent(self.headers,**self.intent_parameters())
        self.assertTrue(host.retired)

    def intent_parameters(self):
        q=query()
        return dict(workspace_id=q.command.workspace_id,conversation_id=q.command.conversation_id,
            visibility=q.command.visibility,expected_membership_revision=q.command.expected_membership_revision,
            expected_conversation_revision=q.command.expected_conversation_revision,
            expected_workspace_revision=q.expected_workspace_revision,retry_key='host-intent')

    def test_composed_sdk_submission_and_recovery_use_one_same_worker_and_job(self):
        host=self.host();pid=host.worker_pid
        result=host.submit(self.headers,self.token)
        recovered=host.recover(self.headers,self.token)
        self.assertEqual(result.observation.status.value,'COMMITTED')
        self.assertEqual(result,recovered)
        self.assertEqual(host.worker_pid,pid)
        self.assertEqual([r['insert'] for r in self.rows() if 'insert' in r],[result.observation.query_job_id])

    def test_committed_hung_submission_requires_explicit_original_job_recovery(self):
        host=self.host('committed-hang',timeout=1)
        with self.assertRaises(OperationStopped):host.submit(self.headers,self.token)
        inserts=[r['insert'] for r in self.rows() if 'insert' in r]
        self.assertEqual(len(inserts),1)
        with self.assertRaises(SharingHostError):host.submit(self.headers,self.token)
        recovery=self.host('recover')
        result=recovery.recover(self.headers,self.token)
        self.assertEqual(result.observation.status.value,'COMMITTED')
        self.assertEqual(result.observation.query_job_id,inserts[0])
        self.assertEqual(len([r for r in self.rows() if 'insert' in r]),1)

    def test_parent_budget_cannot_be_renewed_in_worker(self):
        host=self.host('hang',timeout=10)
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(1)):host.recover(self.headers,self.token)
        self.assertTrue(host.cleanup_complete)

    def test_cancelled_call_sends_no_message_and_preserves_idle_worker(self):
        host=self.host();cancel=CancellationSignal();cancel.cancel()
        with self.assertRaises(OperationStopped):host.submit(self.headers,self.token,cancellation=cancel)
        self.assertFalse(host.retired);self.assertEqual(self.rows(),[])

    def test_scope_exit_cancellation_withholds_result_and_retires_idle_worker(self):
        host=self.host('recover');cancel=CancellationSignal()
        @contextmanager
        def late_stop(settings, cancellation=None):
            with operation_scope(settings,cancellation) as budget:
                yield budget
                if cancellation is cancel:cancel.cancel()
        with patch('src.application.sharing_operation_host.operation_scope',side_effect=late_stop):
            with self.assertRaises(OperationStopped):host.recover(self.headers,self.token,cancellation=cancel)
        self.assertTrue(host.retired);self.assertTrue(host.cleanup_complete)

    def test_invalid_request_is_rejected_before_worker_or_provider_io(self):
        host=self.host()
        for token in ('x'*8193,None,True):
            with self.subTest(token_type=type(token)),self.assertRaises(ChannelError):
                host.submit(self.headers,token)
        self.assertFalse(host.retired);self.assertEqual(self.rows(),[])

    def test_timely_access_denial_does_not_reset_worker_floor_or_restart(self):
        host=self.host()
        with self.assertRaises(SharingHostError):host.submit(self.headers,'invalid-seal')
        self.assertFalse(host.retired)
        self.assertEqual(host.submit(self.headers,self.token).observation.status.value,'COMMITTED')

    def test_same_worker_preserves_higher_floor_and_denies_later_store_rollback(self):
        host=self.host('floor-rollback')
        result=host.submit(self.headers,self.token)
        pid=host.worker_pid
        self.assertEqual(result.observation.status.value,'COMMITTED')
        self.assertEqual([row['floor'] for row in self.rows() if 'floor' in row],[2])
        with self.assertRaises(SharingHostError):host.recover(self.headers,self.token)
        self.assertEqual(host.worker_pid,pid);self.assertFalse(host.retired)
        self.assertEqual(len([row for row in self.rows() if 'insert' in row]),1)

    def test_successful_concurrent_callers_share_serial_worker(self):
        host=self.host('recover')
        with ThreadPoolExecutor(max_workers=2) as pool:
            calls=[pool.submit(host.recover,self.headers,self.token) for _ in range(2)]
            values=[call.result(timeout=3) for call in calls]
        self.assertEqual(values[0],values[1]);self.assertEqual(self.rows(),[])


class HostContractTests(unittest.TestCase):
    def test_config_roundtrip_and_default_activation_stays_disabled(self):
        self.assertEqual(config_from_wire(asdict(config())),config())
        self.assertFalse(SharingHostSettings().enabled)
        for settings in (dict(enabled=1),dict(startup_seconds=0),dict(cleanup_seconds=float('nan')),
                         dict(cleanup_seconds=True),dict(cleanup_seconds=3)):
            with self.assertRaises(ValueError):SharingHostSettings(**settings)

    def test_channel_rejects_oversized_nonfinite_and_duplicate_json(self):
        for value in ({'data':'x'*MAX_FRAME_BYTES},{'x':float('nan')},[]):
            with self.assertRaises(ChannelError):encode(value)
        for body in (b'{"id":1,"id":2}',b'{"value":NaN}',b'[]',b'{invalid'):
            left,right=socket.socketpair()
            with left,right:
                left.setblocking(False);right.send(struct.pack('!I',len(body))+body)
                with self.assertRaises(ChannelError):receive(left,lambda:1)

    def test_channel_rejects_oversize_prefix_before_reading_body(self):
        left,right=socket.socketpair()
        with left,right:
            left.setblocking(False);right.send(struct.pack('!I',MAX_FRAME_BYTES+1))
            with self.assertRaises(ChannelError):receive(left,lambda:1)

    def test_fixed_method_and_strict_parameters_cannot_invoke_arbitrary_worker_code(self):
        for method,params in (('close',{'token':'x'}),('submit',{'token':'x','deadline':999999}),
                ('issue_intent',{'visibility':'TEAM'})):
            with self.assertRaises(ChannelError):request_values(method,{},params)

    def test_worker_context_uses_same_monotonic_deadline_and_restores_parent(self):
        with patch('src.application.operation_budget.time.monotonic',return_value=100):
            with worker_operation_scope(105) as budget:
                with operation_scope(OperationSettings(600)) as child:
                    self.assertEqual(child.deadline,105)
                self.assertEqual(budget.remaining(),5)
            with self.assertRaises(ValueError):
                with worker_operation_scope(float('nan')):pass

    def test_all_result_types_roundtrip_through_bounded_json(self):
        q=query()
        result=SharingQueryResult(SharingResultState.APPLIED,q.command.audit_id,q.command.accepted_at,5)
        outcome=SharingExecutionOutcome(SharingCommitObservation(q.command.audit_id,
            SharingCommitStatus.COMMITTED,'offline-native-job'),result)
        values={'issue_intent':'opaque-token','recover_receipt':result,
                'issue_retry':SharingRetryIssuance(SharingRetryIssuanceState.RECOVERY_REQUIRED)}
        values.update({method:outcome for method in ('submit','recover','submit_retry','recover_retry')})
        for method,value in values.items():
            with self.subTest(method=method):
                payload=pack_result(method,value)
                plain=json.loads(encode(payload)[4:])
                self.assertEqual(unpack_result(method,plain),value)

    def test_malformed_result_projections_cannot_add_private_records_or_permission(self):
        for value in ({'token':'x','roles':['ADMIN']},{'token':'x'*8193},{'token':True}):
            with self.assertRaises(ChannelError):unpack_result('issue_intent',value)
        q=query()
        for changes in (dict(observed_at='2026-10-11T12:00:00'),dict(resulting_revision=True),
                        dict(resulting_revision=None),dict(audit_id='private\nrecord')):
            payload=dict(state='APPLIED',audit_id=q.command.audit_id,
                         observed_at=q.command.accepted_at.isoformat(),resulting_revision=5)|changes
            with self.assertRaises(ChannelError):unpack_result('recover_receipt',payload)

    def test_worker_deadline_cannot_extend_existing_parent_or_remove_signal(self):
        cancel=CancellationSignal()
        with patch('src.application.operation_budget.time.monotonic',return_value=100):
            with self.assertRaises(OperationStopped):
                with operation_scope(OperationSettings(5),cancel):
                    with worker_operation_scope(999) as child:
                        self.assertEqual(child.deadline,105);cancel.cancel();child.remaining()


if __name__=='__main__':unittest.main()
