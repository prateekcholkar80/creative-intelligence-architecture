"""4.4 Critical/Must have/shared pilot: opt-in owned worker supervision.

Prestart outside request handling. A timed-out/disconnected worker is terminal;
there is no automatic restart, native cancellation, retry or commit inference.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
from threading import RLock
import time
from uuid import uuid4

from src.application.operation_budget import (
    CancellationSignal, OperationSettings, OperationStopped, operation_lock, operation_scope,
)
from src.application.operation_channel import ChannelError, receive, send
from src.application.sharing_host_contract import SharingWorkerConfig, request_values, unpack_result
from src.application.access_capture import current_delivery_trace


class SharingHostError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class SharingHostSettings:
    enabled: bool = False
    startup_seconds: int = 10
    cleanup_seconds: float = .5

    def __post_init__(self):
        if (type(self.enabled) is not bool or type(self.startup_seconds) is not int
                or not 1 <= self.startup_seconds <= 30
                or type(self.cleanup_seconds) not in (float,int)
                or not .1 <= self.cleanup_seconds <= 2):
            raise ValueError('Invalid server host settings')


def _worker_command(fd):
    return [sys.executable, '-m', 'src.application.sharing_operation_worker', str(fd)]


class SharingOperationHost:
    """Trusted local supervisor; call results still require the existing executor.

    One admitted call at a time. Signals and deadlines remain parent-owned. The
    only wire deadline is computed here, never accepted from marketing input.
    Worker state/floors survive successful calls; any uncertain channel/stop
    retires this host permanently. Recovery requires explicit new composition
    and the original sealed token, with fresh authoritative checkpoint checks.
    """
    def __init__(self, config: SharingWorkerConfig, settings: SharingHostSettings = SharingHostSettings()):
        if not self._valid_config(config) or type(settings) is not SharingHostSettings:
            raise ValueError('Typed trusted sharing host composition is required')
        self._config, self._settings = config, settings
        self._lock = RLock()
        self._shutdown = CancellationSignal()
        self._process = self._channel = None
        self._ready = False
        self._termination_started = False

    @property
    def config(self):
        return self._config

    @property
    def settings(self):
        return self._settings

    def _valid_config(self, config):
        return type(config) is SharingWorkerConfig

    @property
    def operation_settings(self):
        return self.config.runtime.operation

    def _command(self, fd):
        return _worker_command(fd)

    def _config_values(self):
        return asdict(self.config)

    def _request_values(self, method, headers, parameters):
        return request_values(method, headers, parameters)

    def _receive_reply(self, method, command_id, remaining):
        return receive(self._channel, remaining)

    def _decode_result(self, method, value):
        return unpack_result(method, value)

    def _validate_result_scope(self, method, parameters, result):
        pass  # Sharing projections carry sealed receipt/job identity, not resource rows.

    @property
    def worker_pid(self):
        return None if self._process is None else self._process.pid

    @property
    def retired(self):
        return self._shutdown.cancelled

    @property
    def cleanup_complete(self):
        return self._process is None or self.retired and self._process.poll() is not None

    def _enabled(self):
        if (not self.settings.enabled or not self.config.runtime.enabled
                or not self.config.executor.execution_enabled):
            raise SharingHostError('SHARING_HOST_DISABLED')
        if self.retired:
            raise SharingHostError('SHARING_HOST_RETIRED')

    def start(self):
        """Explicit server prewarm; process creation is outside request guarantees."""
        self._enabled()
        if os.name != 'posix':
            raise SharingHostError('SHARING_HOST_PLATFORM_UNSUPPORTED')
        with operation_scope(OperationSettings(self.settings.startup_seconds),self._shutdown) as budget, operation_lock(self._lock):
            self._enabled()
            if self._ready:
                return
            parent, child = socket.socketpair()
            parent.setblocking(False)
            self._channel = parent
            try:
                self._process = subprocess.Popen(self._command(child.fileno()),
                    cwd=Path(__file__).resolve().parents[2], pass_fds=(child.fileno(),),
                    start_new_session=True, stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
                child.close()
                nonce = uuid4().hex
                send(parent, {'config':self._config_values(),'nonce':nonce}, budget.remaining)
                if receive(parent,budget.remaining) != {'ready':nonce}:
                    raise ChannelError()
                budget.remaining()
                self._ready = True
            except BaseException as error:
                child.close()
                self._retire()
                if isinstance(error,(OperationStopped,KeyboardInterrupt,SystemExit)):
                    raise
                raise SharingHostError('SHARING_HOST_START_FAILED') from None

    def _retire(self, *, deadline=None):
        """Owned lock required; bounded escalation, no provider/client close here."""
        self._shutdown.cancel()
        self._ready = False
        if self._channel is not None:
            self._channel.close()
            self._channel = None
        process = self._process
        if process is None:
            return
        if self._termination_started and process.poll() is not None:
            return  # Never signal a reaped PID/group again on repeated close.
        self._termination_started = True
        end = time.monotonic() + self.settings.cleanup_seconds
        if deadline is not None:
            end = min(end,deadline)
        # Own process group also contains ordinary inherited credential helpers.
        # Signal even if the leader exited, so such helpers cannot outlive it.
        for sig in (signal.SIGTERM,signal.SIGKILL):
            try:
                os.killpg(process.pid,sig)
            except ProcessLookupError:
                pass
            except OSError:
                # Fail closed and keep the handle for explicit cleanup inspection.
                pass
            if sig == signal.SIGTERM:
                # Do not reap the leader before the last group signal: its PID
                # must remain reserved so an unrelated new group cannot reuse it.
                time.sleep(max(0,end-time.monotonic())/2)
        try:
            process.wait(timeout=max(0,end-time.monotonic()))
        except subprocess.TimeoutExpired:
            pass  # Retired handle remains available; no replacement is allowed.

    def close(self) -> bool:
        """Signal first; do not wait indefinitely for a call, startup or cleanup."""
        self._shutdown.cancel()
        end = time.monotonic() + self.settings.cleanup_seconds
        if not self._lock.acquire(timeout=self.settings.cleanup_seconds):
            return False
        try:
            self._retire(deadline=end)
            return self.cleanup_complete
        finally:
            self._lock.release()

    def _invoke(self, method, headers, parameters, cancellation=None):
        sent = []
        try:
            return self._invoke_scoped(method,headers,parameters,cancellation,sent)
        except OperationStopped:
            # Covers a stop on scope exit after the lock has been released.
            # An untouched/cancelled waiter never retires another owner's host.
            if sent and not self.retired:
                self._shutdown.cancel()
                if self._lock.acquire(blocking=False):
                    try:
                        self._retire(deadline=sent[-1])
                    finally:
                        self._lock.release()
                # Otherwise the current owner sees shutdown and owns cleanup.
            raise

    def _invoke_scoped(self, method, headers, parameters, cancellation, sent):
        self._enabled()
        values = self._request_values(method,headers,parameters)
        # Check lock waits under the outer budget. A stopped waiter never owns or
        # retires the other caller's active worker.
        with operation_scope(self.operation_settings,cancellation):
            with operation_scope(self.operation_settings,self._shutdown) as budget, operation_lock(self._lock):
                self._enabled()
                if not self._ready:
                    raise SharingHostError('SHARING_HOST_NOT_STARTED')
                work_deadline = budget.deadline - self.settings.cleanup_seconds
                def remaining():
                    budget.remaining()
                    left = work_deadline-time.monotonic()
                    if left <= 0:
                        raise OperationStopped('OPERATION_DEADLINE_EXCEEDED')
                    return left
                command_id = current_delivery_trace() or uuid4().hex
                try:
                    remaining()
                    sent.append(budget.deadline)
                    send(self._channel, values | {'id':command_id,'deadline':work_deadline}, remaining)
                    reply = self._receive_reply(method,command_id,remaining)
                    remaining()
                    if reply.get('id') != command_id:
                        raise ChannelError()
                    if set(reply) == {'id','error'}:
                        code = reply['error']
                        if type(code) is not str or not re.fullmatch(r'[A-Z][A-Z0-9_]{0,95}',code):
                            raise ChannelError()
                        # Returned application denials are safe, correlated and
                        # timely; preserve the serial worker's validated floors.
                        if code in ('OPERATION_CANCELLED','OPERATION_DEADLINE_EXCEEDED'):
                            raise OperationStopped(code)
                        raise SharingHostError(code)
                    if set(reply) != {'id','result'}:
                        raise ChannelError()
                    result = self._decode_result(method,reply['result'])
                    self._validate_result_scope(method,parameters,result)
                    remaining()
                    return result
                except SharingHostError:
                    raise
                except BaseException as error:
                    self._retire(deadline=budget.deadline)
                    if isinstance(error,(OperationStopped,KeyboardInterrupt,SystemExit)):
                        raise
                    raise SharingHostError('SHARING_HOST_PROTOCOL_FAILED') from None

    def issue_intent(self, headers, *, cancellation=None, **parameters):
        return self._invoke('issue_intent',headers,parameters,cancellation)

    def submit(self, headers, token, *, cancellation=None):
        return self._invoke('submit',headers,{'token':token},cancellation)

    def recover(self, headers, token, *, cancellation=None):
        return self._invoke('recover',headers,{'token':token},cancellation)

    def issue_retry(self, headers, token, *, cancellation=None):
        return self._invoke('issue_retry',headers,{'token':token},cancellation)

    def submit_retry(self, headers, token, *, cancellation=None):
        return self._invoke('submit_retry',headers,{'token':token},cancellation)

    def recover_retry(self, headers, token, *, cancellation=None):
        return self._invoke('recover_retry',headers,{'token':token},cancellation)

    def recover_receipt(self, headers, token, *, cancellation=None):
        return self._invoke('recover_receipt',headers,{'token':token},cancellation)
