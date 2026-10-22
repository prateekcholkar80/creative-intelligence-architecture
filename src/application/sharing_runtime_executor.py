"""4.3 Critical/Must have/shared pilot: opt-in runtime sharing operation gates.

No serving route, deployment, identity fallback, model dispatch or activation.
Inherited stable native IDs, rollback proof and UNKNOWN recovery stay unchanged.
"""
from __future__ import annotations

from functools import wraps
from google.cloud import bigquery

from src.application.access_reader import BigQueryAccessReader
from src.application.identity import IapIdentityAdapter, VerifiedIdentity
from src.application.operation_budget import (
    CancellationSignal, check_operation, operation_scope,
)
from src.application.sharing_executor import (
    ProtectedSharingExecutor, SharingExecutorSettings, SharingNativeAttempt,
)
from src.application.sharing_runtime import RuntimeSharingSealer
from src.application.workflow_contract import _require


def _bounded(method):
    @wraps(method)
    def call(self, *args, cancellation: CancellationSignal | None = None, **kwargs):
        try:
            with operation_scope(self.sealer._settings.operation, cancellation):
                try:
                    return method(self, *args, **kwargs)
                except Exception:
                    check_operation()  # A late provider error cannot hide cancellation/expiry.
                    raise
        except BaseException:
            self.sealer._discard_idle_material()
            raise
    return call


class RuntimeProtectedSharingExecutor(ProtectedSharingExecutor):
    """Require a live runtime and check access/authority at dispatch and release.

    The legacy executor remains available for existing isolated injected-key
    probes. Shared runtime composition uses this explicit adapter; both execution
    settings and runtime settings default disabled. No current deployment exists.
    """

    def __init__(self, *, client: bigquery.Client, verifier: IapIdentityAdapter,
                 reader: BigQueryAccessReader, sealer: RuntimeSharingSealer,
                 settings: SharingExecutorSettings | None = None):
        _require(isinstance(sealer, RuntimeSharingSealer) and sealer._reader.client is client,
                 'INVALID_SHARING_RUNTIME_COMPOSITION')
        super().__init__(client=client, verifier=verifier, reader=reader, sealer=sealer, settings=settings)

    def _checkpoint(self):
        check_operation()

    @_bounded
    def issue_intent(self, *args, **kwargs):
        return super().issue_intent(*args, **kwargs)

    @_bounded
    def submit(self, *args, **kwargs):
        return super().submit(*args, **kwargs)

    @_bounded
    def recover(self, *args, **kwargs):
        return super().recover(*args, **kwargs)

    @_bounded
    def issue_retry(self, *args, **kwargs):
        return super().issue_retry(*args, **kwargs)

    @_bounded
    def submit_retry(self, *args, **kwargs):
        return super().submit_retry(*args, **kwargs)

    @_bounded
    def recover_retry(self, *args, **kwargs):
        return super().recover_retry(*args, **kwargs)

    @_bounded
    def recover_receipt(self, *args, **kwargs):
        return super().recover_receipt(*args, **kwargs)

    def _dispatch_guard(self, identity: VerifiedIdentity, attempt: SharingNativeAttempt):
        # Access is checked after loading at entry and exit. Readiness is then
        # checked after access I/O, before entering jobs.insert/result release.
        return self.sealer.attempt_gate(attempt, identity,
            authorize=lambda: self._authorize(identity, attempt))

    def _release(self, identity: VerifiedIdentity, attempt: SharingNativeAttempt, result):
        # This includes every early known-job/REUSED/UNKNOWN/issuance return.
        self.sealer.check_attempt(attempt, identity,
            authorize=lambda: self._authorize(identity, attempt))
        check_operation()
        return result
