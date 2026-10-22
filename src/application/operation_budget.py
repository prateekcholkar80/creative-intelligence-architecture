"""4.4 Critical/Must have/shared pilot: cooperative operation budget.

Server-only cancellation/monotonic deadlines. No auth, dispatch, commit proof,
transport interruption or durable cancellation. Context does not cross threads
automatically; trusted composition owns the request boundary and signal.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import math
from threading import Event
import time


class OperationStopped(RuntimeError):
    """Safe stop only; conveys no knowledge of any native commit outcome."""
    def __init__(self, code: str):
        if code not in {'OPERATION_CANCELLED', 'OPERATION_DEADLINE_EXCEEDED'}:
            raise ValueError('Invalid operation stop code')
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class OperationSettings:
    timeout_seconds: int = 120

    def __post_init__(self):
        if type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 600:
            raise ValueError('Configure an operation timeout between 1 and 600 seconds')


class CancellationSignal:
    """One-way server-owned signal; no reset, caller reason or identity."""
    def __init__(self):
        self.__event = Event()

    def cancel(self) -> None:
        self.__event.set()

    @property
    def cancelled(self) -> bool:
        return self.__event.is_set()


@dataclass(frozen=True, slots=True)
class OperationBudget:
    deadline: float
    signals: tuple[CancellationSignal, ...]

    def __post_init__(self):
        if (type(self.deadline) not in (float, int) or not math.isfinite(self.deadline)
                or type(self.signals) is not tuple or not self.signals
                or any(type(s) is not CancellationSignal for s in self.signals)):
            raise ValueError('Invalid server operation budget')

    def remaining(self) -> float:
        if any(s.cancelled for s in self.signals):
            raise OperationStopped('OPERATION_CANCELLED')
        remaining = self.deadline - time.monotonic()
        if remaining <= 0 or not math.isfinite(remaining):
            raise OperationStopped('OPERATION_DEADLINE_EXCEEDED')
        return remaining


_CURRENT: ContextVar[OperationBudget | None] = ContextVar('creative_operation_budget', default=None)
_OWNED_WORKER: ContextVar[bool] = ContextVar('creative_owned_operation_worker', default=False)


def require_owned_worker() -> None:
    """Trusted composition check, not authentication or a dispatch permission."""
    if not _OWNED_WORKER.get() or _CURRENT.get() is None:
        raise ValueError('Billable dispatch requires owned worker supervision')
    check_operation()


def check_operation() -> None:
    budget = _CURRENT.get()
    if budget is not None:
        budget.remaining()


def operation_timeout(cap: float) -> float:
    """Cap a provider timeout without changing native job configuration/identity."""
    if type(cap) not in (float, int) or not math.isfinite(cap) or cap <= 0:
        raise ValueError('A positive finite provider timeout is required')
    budget = _CURRENT.get()
    return cap if budget is None else min(cap, budget.remaining())


def operation_retry(default):
    """Keep legacy read retries outside a scope; no SDK replay inside a budget."""
    check_operation()
    return default if _CURRENT.get() is None else None


@contextmanager
def worker_operation_scope(deadline: float):
    """Install a trusted same-host monotonic deadline in an owned worker.

    This is server IPC composition, never a browser timeout or permission. The
    supervisor handles external signals by retiring the whole worker process.
    """
    parent = _CURRENT.get()
    if parent is not None:
        parent.remaining()
    budget = OperationBudget(deadline if parent is None else min(deadline, parent.deadline),
                             (CancellationSignal(),) if parent is None else parent.signals)
    budget.remaining()
    token = _CURRENT.set(budget)
    worker_token = _OWNED_WORKER.set(True)
    try:
        yield budget
        check_operation()
    finally:
        _OWNED_WORKER.reset(worker_token)
        _CURRENT.reset(token)


def operation_call(call, *args, timeout: float, **kwargs):
    """Recalculate at each RPC boundary and reject late success or failure.

    This cannot interrupt a provider that ignores its timeout. A stopped write
    may have committed; this helper does not cancel jobs or classify outcomes.
    """
    try:
        return call(*args, timeout=operation_timeout(timeout), **kwargs)
    finally:
        check_operation()


def operation_iter(values):
    """Check around lazy iteration, including each page fetch and empty results."""
    check_operation()
    try:
        iterator = iter(values)
    finally:
        check_operation()
    while True:
        check_operation()
        try:
            value = next(iterator)
        except StopIteration:
            check_operation()
            return
        except Exception:
            check_operation()
            raise
        check_operation()
        yield value


@contextmanager
def operation_scope(settings: OperationSettings, cancellation: CancellationSignal | None = None):
    """Nested scopes can shorten, never extend, the supplied parent's budget.

    All ancestor signals stay effective. A fresh top-level recovery receives a
    new host budget without changing a sealed intent, job ID or retry window.
    """
    if type(settings) is not OperationSettings or (cancellation is not None
            and type(cancellation) is not CancellationSignal):
        raise ValueError('Operation settings/signals must come from trusted server composition')
    parent = _CURRENT.get()
    if parent is not None:
        parent.remaining()
    deadline = time.monotonic() + settings.timeout_seconds
    signals = () if parent is None else parent.signals
    signal = CancellationSignal() if cancellation is None and parent is None else cancellation
    if signal is not None and all(signal is not s for s in signals):
        signals += (signal,)
    budget = OperationBudget(deadline if parent is None else min(parent.deadline, deadline), signals)
    token = _CURRENT.set(budget)
    try:
        check_operation()
        yield budget
        check_operation()  # Late success cannot leave a stopped scope.
    finally:
        _CURRENT.reset(token)


@contextmanager
def operation_lock(lock):
    """Poll a server lock without detaching a worker or clearing its state.

    A stopped waiter never owns the lock and must not invalidate another active
    operation. After acquisition, check again and release even on cancellation.
    """
    budget = _CURRENT.get()
    if budget is None:
        raise ValueError('A server operation scope is required for bounded locking')
    acquired = False
    try:
        while not acquired:
            acquired = lock.acquire(timeout=min(0.05, budget.remaining()))
        check_operation()
        yield
    finally:
        if acquired:
            lock.release()
