"""4.4 Critical/Must have/shared pilot: opt-in budget-aware ADC HTTP clients.

Recheck at the wire after credential refresh, including lazy SDK page requests.
Socket timeouts remain cooperative, not a hard host wall-clock deadline. Never
cancel/replay a mutation or interpret transport failure as noncommit proof.
"""
from __future__ import annotations

from dataclasses import dataclass

import google.auth
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import bigquery, secretmanager_v1, storage
from google.cloud.secretmanager_v1.services.secret_manager_service.transports.rest import (
    DEFAULT_CLIENT_INFO, SecretManagerServiceRestTransport,
)
import requests

from src.application.operation_budget import (
    OperationSettings, OperationStopped, check_operation, operation_scope, operation_timeout,
)
from src.application.sharing_secret_loader import REGIONAL_ENDPOINT
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT

SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)


def _timeout(value, cap: float):
    """Accept requests' scalar/connect-read tuple; never allow an infinite socket."""
    cap = operation_timeout(cap)
    if value is None:
        return cap
    if type(value) is tuple and len(value) == 2:
        return tuple(cap if item is None else min(cap, operation_timeout(item)) for item in value)
    return min(cap, operation_timeout(value))


class OperationSession(requests.Session):
    """Owned unauthenticated session for ADC and public IAP certificates."""
    def __init__(self, *, timeout_seconds: float = 20):
        operation_timeout(timeout_seconds)
        super().__init__()  # requests' default adapters have zero transport retries.
        self.timeout_seconds = timeout_seconds

    def request(self, method, url, **kwargs):
        check_operation()
        kwargs['allow_redirects'] = False
        try:
            return super().request(method, url, **kwargs)
        finally:
            check_operation()

    def send(self, request, **kwargs):
        kwargs['timeout'] = _timeout(kwargs.get('timeout'), self.timeout_seconds)
        try:
            return super().send(request, **kwargs)
        finally:
            check_operation()


class OperationRequest(Request):
    """Google Auth request preserving cancellation instead of masking it."""
    def __call__(self, url, method='GET', body=None, headers=None, timeout=20, **kwargs):
        timeout = _timeout(timeout, 20)
        try:
            return super().__call__(url, method=method, body=body, headers=headers,
                                    timeout=timeout, **kwargs)
        finally:
            check_operation()


class OperationAuthorizedSession(AuthorizedSession):
    """ADC refresh shares the request budget; no automatic 401/redirect replay."""
    def __init__(self, credentials):
        self._operation_auth_session = OperationSession()
        try:
            super().__init__(credentials, auth_request=OperationRequest(self._operation_auth_session),
                             max_refresh_attempts=0)
        except BaseException:
            self._operation_auth_session.close()
            raise

    def request(self, method, url, **kwargs):
        kwargs['timeout'] = _timeout(kwargs.get('timeout'), 20)
        kwargs['allow_redirects'] = False
        try:
            return super().request(method, url, **kwargs)
        finally:
            check_operation()

    def send(self, request, **kwargs):
        # AuthorizedSession calls before_request before Session.send. Recompute
        # here so ADC cannot spend time then reuse the pre-refresh timeout.
        kwargs['timeout'] = _timeout(kwargs.get('timeout'), 20)
        try:
            return super().send(request, **kwargs)
        finally:
            check_operation()

    def close(self):
        try:
            super().close()
        finally:
            self._operation_auth_session.close()


class _OperationSecretRestTransport(SecretManagerServiceRestTransport):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # The generated REST transport exposes no session injection parameter.
        # This opt-in subclass owns that SDK session slot; compatibility is
        # checked with actual installed SDK calls in the offline test suite.
        original = self._session
        original.close()
        original._auth_request.session.close()
        self._session = OperationAuthorizedSession(self._credentials)
        try:
            self._prep_wrapped_messages(kwargs.get('client_info', DEFAULT_CLIENT_INFO))
        except BaseException:
            self._session.close()
            raise


@dataclass(frozen=True, slots=True)
class RuntimeCloudClients:
    """Server-owned clients, not browser input or a process-wide cache."""
    bigquery: bigquery.Client
    secrets: secretmanager_v1.SecretManagerServiceClient

    def close(self):
        try:
            self.bigquery.close()
        finally:
            self.secrets.transport.close()


class RuntimeClientError(RuntimeError):
    def __init__(self):
        super().__init__('RUNTIME_CLOUD_CLIENTS_UNAVAILABLE')


@dataclass(frozen=True, slots=True)
class WorkflowReadClients:
    bigquery: bigquery.Client
    storage: storage.Client

    def close(self):
        try:
            self.bigquery.close()
        finally:
            self.storage.close()


def create_workflow_read_clients(*, settings: OperationSettings = OperationSettings()) -> WorkflowReadClients:
    """Budget-aware, keyless BQ/GCS composition; no Secret Manager dependency."""
    bq = gcs = bq_session = gcs_session = None
    try:
        with operation_scope(settings):
            with OperationSession() as discovery:
                credentials, _ = google.auth.default(scopes=SCOPES, request=OperationRequest(discovery))
                check_operation()
            bq_session = OperationAuthorizedSession(credentials)
            bq = bigquery.Client(project=PROJECT, location=LOCATION, credentials=credentials, _http=bq_session)
            gcs_session = OperationAuthorizedSession(credentials)
            gcs = storage.Client(project=PROJECT, credentials=credentials, _http=gcs_session)
            check_operation()
            return WorkflowReadClients(bq, gcs)
    except BaseException as error:
        if bq is not None:
            bq.close()
        elif bq_session is not None:
            bq_session.close()
        if gcs is not None:
            gcs.close()
        elif gcs_session is not None:
            gcs_session.close()
        if isinstance(error, (OperationStopped, KeyboardInterrupt, SystemExit)):
            raise
        raise RuntimeClientError() from None


def create_runtime_cloud_clients(*, settings: OperationSettings = OperationSettings()) -> RuntimeCloudClients:
    """Explicit ADC composition in the fixed project/region, disabled by omission.

    No credential files supplied, secret reads, IAM, discovery cache or fallback.
    REST keeps ADC refresh in the operation context; the legacy gRPC factory is
    unchanged and does not acquire this HTTP-level guarantee automatically.
    """
    bq = transport = session = None
    try:
        with operation_scope(settings):
            with OperationSession() as discovery:
                credentials, _ = google.auth.default(scopes=SCOPES, request=OperationRequest(discovery))
                check_operation()
            session = OperationAuthorizedSession(credentials)
            bq = bigquery.Client(project=PROJECT, location=LOCATION, credentials=credentials, _http=session)
            transport = _OperationSecretRestTransport(host=REGIONAL_ENDPOINT, credentials=credentials)
            sm = secretmanager_v1.SecretManagerServiceClient(transport=transport,
                client_options={'api_endpoint': REGIONAL_ENDPOINT})
            check_operation()
            return RuntimeCloudClients(bq, sm)
    except BaseException as error:
        if bq is not None:
            bq.close()
        elif session is not None:
            session.close()
        if transport is not None:
            transport.close()
        if isinstance(error, (OperationStopped, KeyboardInterrupt, SystemExit)):
            raise
        raise RuntimeClientError() from None
