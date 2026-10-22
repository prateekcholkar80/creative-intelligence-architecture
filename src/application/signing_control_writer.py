"""4.2 Critical/Must have/shared pilot: opt-in IAP global-control writer.

Global allowlists are trusted deployment configuration, independent of marketing
roles. ADC supplies storage IAM only. No browser route, genesis or default enable.
Prepared plans remain in this process; callers must retain the exact envelope for
read-only original-job recovery. Process-local admission is not a durable queue.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import islice
import json
import re
from threading import Lock
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.identity import IapIdentityAdapter, ensure_fresh
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_jobs import failed_sharing_job_status, matches_sharing_job
from src.application.sharing_publication_codec import normalize
from src.application.sharing_publication_contract import SigningPublication
from src.application.sharing_publication_read_contract import SigningReadRequest
from src.application.sharing_publication_reader import BigQuerySigningReader
from src.application.sharing_refresh_contract import SigningReadTicket
from src.application.sharing_signing_contract import _identifier
from src.application.signing_write_query import (
    SigningWriteKind, SigningWritePlan, SigningWriteResult, prepare_write, validate_write_result, write_job_id,
)
from src.application.workflow_contract import _require
from src.ingestion.setup_signing_tables import inspect
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT


def _now():
    return datetime.now(timezone.utc)


def _principal(value):
    _require(type(value) is str and re.fullmatch(r'iap-v1:[0-9a-f]{64}', value), 'INVALID_SIGNING_WRITER_GRANT')


@dataclass(frozen=True, slots=True)
class SigningCheckpointGrant:
    principal_id: str
    consumer_id: str

    def __post_init__(self):
        _principal(self.principal_id)
        _identifier(self.consumer_id)


@dataclass(frozen=True, slots=True)
class SigningWriterSettings:
    project_number: str  # Verified project association is a deployment prerequisite.
    operator_principals: frozenset[str] = frozenset()
    checkpoint_grants: tuple[SigningCheckpointGrant, ...] = ()
    publisher_consumer_id: str = 'signing-publisher'
    enabled: bool = False
    rpc_timeout_seconds: int = 10
    result_timeout_seconds: int = 20

    def __post_init__(self):
        _require(type(self.project_number) is str and re.fullmatch(r'[1-9][0-9]{0,19}', self.project_number),
                 'INVALID_SIGNING_WRITER_SCOPE')
        _identifier(self.publisher_consumer_id)
        _require(type(self.enabled) is bool and type(self.operator_principals) is frozenset
                 and type(self.checkpoint_grants) is tuple, 'INVALID_SIGNING_WRITER_SETTINGS')
        for principal in self.operator_principals:
            _principal(principal)
        _require(all(isinstance(grant, SigningCheckpointGrant) for grant in self.checkpoint_grants)
                 and len(set(self.checkpoint_grants)) == len(self.checkpoint_grants), 'INVALID_SIGNING_WRITER_GRANT')
        for value in (self.rpc_timeout_seconds, self.result_timeout_seconds):
            _require(type(value) is int and 1 <= value <= 60, 'INVALID_SIGNING_WRITER_SETTINGS')


class SigningWriteError(PermissionError):
    def __init__(self, code):
        self.code = code
        super().__init__('Signing control operation denied: ' + code)


@dataclass(frozen=True, slots=True)
class SigningWriteOutcome:
    job_id: str
    native_status: SharingCommitStatus
    result: SigningWriteResult | None = None


class ProtectedSigningWriter:
    def __init__(self, *, client: bigquery.Client, identity: IapIdentityAdapter, settings: SigningWriterSettings):
        _require(isinstance(client, bigquery.Client) and client.project == PROJECT and client.location == LOCATION
                 and client.default_query_job_config is None, 'INVALID_SIGNING_WRITER_CLIENT')
        _require(isinstance(identity, IapIdentityAdapter) and isinstance(settings, SigningWriterSettings),
                 'INVALID_SIGNING_WRITER_SETTINGS')
        self.client, self.identity, self.settings = client, identity, settings
        self.reader = BigQuerySigningReader(client=client)
        self._issued: dict[str, SigningWritePlan] = {}
        self._attempted: set[str] = set()
        self._lock = Lock()

    def _authorize(self, headers: Mapping[str, str], kind: SigningWriteKind, consumer_id: str, *, write: bool):
        if write and not self.settings.enabled:
            raise SigningWriteError('SIGNING_WRITES_DISABLED')
        actor = self.identity.authenticate(headers)
        ensure_fresh(actor)
        principal = actor.principal.principal_id
        allowed = (principal in self.settings.operator_principals if kind is SigningWriteKind.PUBLICATION else
                   SigningCheckpointGrant(principal, consumer_id) in self.settings.checkpoint_grants)
        if not allowed:
            raise SigningWriteError('SIGNING_WRITER_NOT_AUTHORIZED')
        return actor

    def _prepare(self, headers, *, kind, consumer_id, command_id, instance_id, publication=None):
        actor = self._authorize(headers, kind, consumer_id, write=True)
        request = SigningReadRequest(consumer_id, self.settings.project_number,
                                     SigningReadTicket(instance_id, 1, 0, None))
        job_id = write_job_id(kind, actor.principal.principal_id, command_id, consumer_id)

        def reuse(previous):
            # A second fresh read would change recorded_at and possibly the head.
            # Compare the business input, then retain the original envelope. This
            # return is historical admission, never proof of current state/commit.
            same_intent = (isinstance(publication, SigningPublication)
                and json.loads(previous.publication_json)[0]['publication_digest'] == publication.digest
                if kind is SigningWriteKind.PUBLICATION else
                previous.capture.request.ticket.instance_id == instance_id)
            if not same_intent:
                raise SigningWriteError('SIGNING_COMMAND_CONFLICT')
            return previous

        with self._lock:
            previous = self._issued.get(job_id)
            if previous is not None:
                return reuse(previous)
        capture = self.reader.capture(request)
        _require(capture.request == request, 'SIGNING_WRITE_CAPTURE_SCOPE_MISMATCH')
        plan = prepare_write(capture, kind=kind, actor_id=actor.principal.principal_id,
                             command_id=command_id, publication=publication)
        with self._lock:
            previous = self._issued.get(plan.job_id)
            if previous is not None:
                return reuse(previous)
            self._issued[plan.job_id] = plan
        return plan

    def prepare_publication(self, headers, *, command_id: str, publication: SigningPublication):
        return self._prepare(headers, kind=SigningWriteKind.PUBLICATION,
            consumer_id=self.settings.publisher_consumer_id, command_id=command_id,
            instance_id='operator-' + uuid4().hex, publication=publication)

    def prepare_checkpoint(self, headers, *, consumer_id: str, command_id: str, instance_id: str):
        # Target is the fresh authoritative head, never a caller's revision/hash.
        return self._prepare(headers, kind=SigningWriteKind.CHECKPOINT, consumer_id=consumer_id,
                             command_id=command_id, instance_id=instance_id)

    def _preflight(self):
        try:
            _require(self.client.project == PROJECT and self.client.location == LOCATION
                     and self.client.default_query_job_config is None, 'INVALID_SIGNING_WRITER_CLIENT')
            _require(inspect(self.client)['status'] == 'VERIFIED', 'SIGNING_WRITE_RESOURCES_MISSING')
        except Exception:
            raise SigningWriteError('SIGNING_WRITE_STORE_UNAVAILABLE') from None

    def _unknown(self, plan):
        return SigningWriteOutcome(plan.job_id, SharingCommitStatus.UNKNOWN)

    def _read_job(self, plan, job, sql, config):
        if not matches_sharing_job(job, job_id=plan.job_id, sql=sql, config=config) or job.state != 'DONE':
            return self._unknown(plan)
        if job.error_result is not None:
            status = failed_sharing_job_status(self.client, job_id=plan.job_id,
                timeout_seconds=self.settings.rpc_timeout_seconds, parent=job)
            return SigningWriteOutcome(plan.job_id, status)
        try:
            rows = list(islice(job.result(max_results=2, page_size=2, retry=None, job_retry=None,
                timeout=self.settings.result_timeout_seconds), 2))
            if not matches_sharing_job(job, job_id=plan.job_id, sql=sql, config=config):
                return self._unknown(plan)
            result = validate_write_result(plan, [normalize(row) for row in rows])
            return SigningWriteOutcome(plan.job_id, SharingCommitStatus.COMMITTED, result)
        except Exception:
            return self._unknown(plan)

    def recover(self, headers, plan: SigningWritePlan) -> SigningWriteOutcome:
        _require(isinstance(plan, SigningWritePlan), 'INVALID_SIGNING_WRITE_PLAN')
        actor = self._authorize(headers, plan.kind, plan.capture.request.consumer_id, write=False)
        if (actor.principal.principal_id != plan.actor_id
                or plan.capture.request.project_number != self.settings.project_number
                or (plan.kind is SigningWriteKind.PUBLICATION
                    and plan.capture.request.consumer_id != self.settings.publisher_consumer_id)):
            raise SigningWriteError('SIGNING_WRITE_SCOPE_MISMATCH')
        self._preflight()
        config = bigquery.QueryJobConfig.from_api_repr(deepcopy(plan.job_config().to_api_repr()))
        try:
            job = self.client.get_job(plan.job_id, project=PROJECT, location=LOCATION,
                                      retry=None, timeout=self.settings.rpc_timeout_seconds)
            return self._read_job(plan, job, plan.sql, config)
        except Exception:
            return self._unknown(plan)

    def submit(self, headers, plan: SigningWritePlan) -> SigningWriteOutcome:
        _require(isinstance(plan, SigningWritePlan), 'INVALID_SIGNING_WRITE_PLAN')
        actor = self._authorize(headers, plan.kind, plan.capture.request.consumer_id, write=True)
        if actor.principal.principal_id != plan.actor_id:
            raise SigningWriteError('SIGNING_WRITE_SCOPE_MISMATCH')
        with self._lock:
            if self._issued.get(plan.job_id) is not plan:
                raise SigningWriteError('SIGNING_PLAN_NOT_ISSUED')
            attempted = plan.job_id in self._attempted
            self._attempted.add(plan.job_id)  # Consume admission before any uncertain I/O.
        if attempted:
            return self.recover(headers, plan)
        self._preflight()
        ensure_fresh(actor)
        if _now() >= plan.deadline_at:
            raise SigningWriteError('SIGNING_WRITE_WINDOW_CLOSED')
        config = plan.job_config()
        expected = bigquery.QueryJobConfig.from_api_repr(deepcopy(config.to_api_repr()))
        sql = plan.sql
        try:
            # A preexisting job is recovered, never submitted again. A changed
            # envelope under the same business ID stays UNKNOWN on correlation.
            try:
                job = self.client.get_job(plan.job_id, project=PROJECT, location=LOCATION,
                                          retry=None, timeout=self.settings.rpc_timeout_seconds)
            except NotFound:
                job = self.client.query(sql, job_config=config, job_id=plan.job_id, project=PROJECT,
                    location=LOCATION, retry=None, job_retry=None, timeout=self.settings.rpc_timeout_seconds)
                if not matches_sharing_job(job, job_id=plan.job_id, sql=sql, config=expected):
                    return self._unknown(plan)
                try:
                    job.result(max_results=2, page_size=2, retry=None, job_retry=None,
                               timeout=self.settings.result_timeout_seconds)
                except Exception:
                    pass
                # Positive classification uses a fresh correlated parent.
                job = self.client.get_job(plan.job_id, project=PROJECT, location=LOCATION,
                                          retry=None, timeout=self.settings.rpc_timeout_seconds)
            return self._read_job(plan, job, sql, expected)
        except Exception:
            return self._unknown(plan)
