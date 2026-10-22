"""4.3 Critical/Must have/shared pilot: serialized, opt-in signing runtime.

Fresh native metadata and pinned secret loads precede every public sealer call.
No raw sealer, key bytes, caller-supplied state or local fallback is exposed.
This is not an executor dispatch fence or a deployed identity/IAM integration.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from threading import RLock
import time

from src.application.identity import VerifiedIdentity
from src.application.identity import IdentityError
from src.application.operation_budget import (
    OperationSettings, OperationStopped, check_operation, operation_lock, operation_scope,
)
from src.application.access_policy import AuthorizationError
from src.application.access_reader import AccessReadError
from src.application.sharing_intent import SharingAttempt
from src.application.sharing_publication_read_contract import SigningReadRequest
from src.application.sharing_publication_reader import BigQuerySigningReader
from src.application.sharing_query import SharingQuery, SharingQuerySettings
from src.application.sharing_refresh_contract import (
    SigningRefreshSettings, SigningReplicaPhase, SigningReplicaState,
    begin_refresh, complete_refresh, observe_publications, require_signing_ready,
)
from src.application.sharing_retained_sealer import RetainedSharingSealer
from src.application.sharing_retry import SharingRetryAttempt, SharingRetrySealer
from src.application.sharing_secret_loader import SharingSecretLoader
from src.application.sharing_signing_contract import _identifier
from src.application.workflow_contract import ContractError, _require


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class SigningRuntimeSettings:
    consumer_id: str
    project_number: str
    enabled: bool = False
    refresh: SigningRefreshSettings = SigningRefreshSettings()
    operation: OperationSettings = OperationSettings()

    def __post_init__(self):
        _identifier(self.consumer_id)
        # Reuse the reader's exact trusted-scope validation without doing I/O.
        _, ticket = begin_refresh(SigningReplicaState('scope-validation'))
        SigningReadRequest(self.consumer_id, self.project_number, ticket)
        _require(type(self.enabled) is bool and isinstance(self.refresh, SigningRefreshSettings)
                 and type(self.operation) is OperationSettings,
                 'INVALID_SIGNING_RUNTIME_SETTINGS')


class SigningRuntimeError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(f'Signing runtime unavailable: {code}')


class RuntimeSharingSealer(SharingRetrySealer):
    """Own live state/material under one lock, including operation/result gates.

    A trusted deployment supplies the consumer/project-number binding and a new
    process instance ID. Before loading, its authoritative checkpoint must equal
    the current head: an authorized writer advances that floor separately. A
    typed/local checkpoint cannot seed this runtime. Every call reloads all
    nonrevoked keys; there is no success cache after a failed refresh.

    Clients remain caller-owned. Cooperative budgets bound lock waits and operation
    boundaries; provider/ADC interruption is not an overall host deadline. A returned attempt
    does not authorize later dispatch; the executor still needs operation gates.
    """

    def __init__(self, *, reader: BigQuerySigningReader, loader: SharingSecretLoader,
                 instance_id: str, settings: SigningRuntimeSettings):
        _require(isinstance(reader, BigQuerySigningReader) and isinstance(loader, SharingSecretLoader)
                 and isinstance(settings, SigningRuntimeSettings), 'INVALID_SIGNING_RUNTIME')
        self._reader, self._loader, self._settings = reader, loader, settings
        self._state = SigningReplicaState(instance_id)
        self._lock = RLock()
        self._sealer: RetainedSharingSealer | None = None
        self._monotonic_deadline: float | None = None
        self._busy = False
        self._closed = False

    @property
    def state(self) -> SigningReplicaState:
        """Immutable diagnostic metadata only; never a usable signing grant."""
        with self._lock:
            return self._state

    def _invalidate(self) -> None:
        self._sealer = None
        self._monotonic_deadline = None
        if self._state.phase is not SigningReplicaPhase.STOPPED:
            self._state = replace(self._state, generation=max(1, self._state.generation),
                phase=SigningReplicaPhase.FAILED, observed_at=None, valid_until=None)

    def _discard_idle_material(self) -> None:
        """Stopped executor cleanup never waits or changes another active owner."""
        if self._lock.acquire(blocking=False):
            try:
                if not self._busy:
                    self._invalidate()
            finally:
                self._lock.release()

    def close(self) -> None:
        """Permanently deny future operations and release owned key references.

        Python reference removal is not guaranteed memory zeroization. Closing
        waits for locally serialized work; it cannot cancel remote I/O.
        """
        with self._lock:
            _require(not self._busy, 'SIGNING_RUNTIME_REENTRANT')
            self._closed = True
            self._invalidate()

    def _refresh(self) -> None:
        check_operation()
        started = time.monotonic()
        self._sealer = None
        self._monotonic_deadline = None
        self._state, ticket = begin_refresh(self._state)
        request = SigningReadRequest(self._settings.consumer_id, self._settings.project_number,
                                     ticket, self._state.head)
        observation = self._reader.read(request)
        # Only a validated authoritative reader result can introduce a cold
        # checkpoint anchor. Preserve this floor even if subsequent work fails.
        if self._state.head is None and observation.checkpoint_anchor is not None:
            self._state = replace(self._state, head=observation.checkpoint_anchor)
            ticket = replace(ticket, base_revision=self._state.head.revision,
                             base_digest=self._state.head.digest)
        self._state, load_ticket = observe_publications(self._state, ticket,
            publications=observation.publications, observed_at=observation.observed_at,
            now=_now(), settings=self._settings.refresh)
        # Retain a validated higher floor even when the read exceeded the host budget.
        check_operation()
        if load_ticket is None:
            raise SigningRuntimeError('SIGNING_REPLICA_NOT_READY')
        _require(observation.checkpoint_anchor is not None
                 and observation.checkpoint_anchor.digest == self._state.head.digest,
                 'SIGNING_RUNTIME_CHECKPOINT_REQUIRED')
        loaded = self._loader.load(self._state.head.config)
        check_operation()
        _require(isinstance(loaded, RetainedSharingSealer)
                 and loaded.policy == self._state.head.config.policy, 'SIGNING_LOADED_CONFIG_MISMATCH')
        self._state = complete_refresh(self._state, load_ticket,
            loaded_config=self._state.head.config, now=_now())
        # Independent monotonic bound prevents a backward wall-clock adjustment
        # from extending readiness after the captured authority read.
        self._monotonic_deadline = started + self._settings.refresh.max_age_seconds
        self._sealer = loaded
        self._ready()

    def _ready(self) -> RetainedSharingSealer:
        check_operation()
        require_signing_ready(self._state, now=_now())
        _require(self._sealer is not None and self._sealer.policy == self._state.head.config.policy
                 and self._monotonic_deadline is not None
                 and time.monotonic() < self._monotonic_deadline, 'SIGNING_REFRESH_EXPIRED')
        return self._sealer

    @contextmanager
    def _operation(self):
        try:
            with operation_scope(self._settings.operation), operation_lock(self._lock):
                if self._closed or not self._settings.enabled:
                    raise SigningRuntimeError('SIGNING_RUNTIME_DISABLED')
                if self._busy:
                    raise SigningRuntimeError('SIGNING_RUNTIME_REENTRANT')
                self._busy = True
                try:
                    check_operation()
                    yield
                    check_operation()
                except ContractError as error:
                    self._invalidate()
                    check_operation()
                    raise SigningRuntimeError(error.code) from None
                except (SigningRuntimeError, OperationStopped):
                    self._invalidate()
                    raise
                except (IdentityError, AuthorizationError, AccessReadError):
                    self._invalidate()
                    check_operation()
                    raise
                except Exception:
                    self._invalidate()
                    check_operation()
                    # Provider details, key bytes and tokens never enter safe errors.
                    raise SigningRuntimeError('SIGNING_RUNTIME_REFRESH_OR_OPERATION_FAILED') from None
                except BaseException:
                    self._invalidate()
                    raise
                finally:
                    self._busy = False
        except OperationStopped:
            # Covers expiry at lock acquisition or scope exit, without waiting
            # for or modifying a different active operation.
            self._discard_idle_material()
            raise

    def _run(self, method: str | None, *args):
        with self._operation():
            self._refresh()
            sealer = self._ready()
            if method is None:
                result = self._state
            elif method == 'settings':
                result = sealer.settings
            else:
                result = getattr(sealer, method)(*args)
            self._ready()  # No result release after the original lease.
            return result

    def check_attempt(self, attempt: SharingAttempt | SharingRetryAttempt,
                      identity: VerifiedIdentity, *, authorize: Callable[[], None] | None = None) -> None:
        """Fresh policy/optional trusted access check, not a signature proof.

        Authorization callbacks belong to the server executor, never request
        input. Check access after loading and readiness after callback I/O.
        """
        _require(authorize is None or callable(authorize), 'INVALID_SIGNING_RUNTIME_AUTHORIZER')
        with self._operation():
            self._refresh()
            self._ready().check_attempt(attempt, identity)
            if authorize is not None:
                authorize()
            self._ready()

    @contextmanager
    def attempt_gate(self, attempt: SharingAttempt | SharingRetryAttempt,
                     identity: VerifiedIdentity, *, authorize: Callable[[], None] | None = None):
        """Serialize a bounded executor operation without exporting keys/sealers.

        Validate fresh authority before entering, preserve the initial lease over
        the caller's I/O, then refresh and check current authority before exit.
        A post-dispatch denial withholds results; it cannot undo/classify a job.
        """
        _require(authorize is None or callable(authorize), 'INVALID_SIGNING_RUNTIME_AUTHORIZER')
        with self._operation():
            self._refresh()
            self._ready().check_attempt(attempt, identity)
            if authorize is not None:
                authorize()
            self._ready()
            yield
            self._ready()  # Expiry during I/O cannot be erased by a new lease.
            self._refresh()
            self._ready().check_attempt(attempt, identity)
            if authorize is not None:
                authorize()
            self._ready()

    def refresh(self) -> SigningReplicaState:
        return self._run(None)

    @property
    def settings(self) -> SharingQuerySettings:
        return self._run('settings')

    def seal(self, query: SharingQuery) -> str:
        return self._run('seal', query)

    def resolve(self, token: str, identity: VerifiedIdentity) -> SharingAttempt:
        return self._run('resolve', token, identity)

    def seal_retry(self, attempt: SharingRetryAttempt) -> str:
        return self._run('seal_retry', attempt)

    def resolve_retry(self, token: str, identity: VerifiedIdentity) -> SharingRetryAttempt:
        return self._run('resolve_retry', token, identity)
