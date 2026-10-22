"""4.2 Critical/Must have/shared pilot: bounded exclusive genesis bootstrap.

BigQuery's reserved native job slots serialize initial execution across callers.
No configurable IDs, operator election by row INSERT, secret bytes or activation.
Settings are a trusted one-time deployment manifest, never browser input.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
from itertools import islice
from pathlib import Path
from threading import Lock

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from src.application.identity import IapIdentityAdapter, ensure_fresh
from src.application.sharing_coordinator_contract import SharingCommitStatus, _utc
from src.application.sharing_jobs import failed_sharing_job_status, matches_sharing_job
from src.application.sharing_publication_codec import decode_history, decode_publication, encode_publication, normalize
from src.application.sharing_publication_contract import POLICY_ID, SigningPublication
from src.application.sharing_publication_schema import STORAGE_VERSION, TABLES
from src.application.signing_control_writer import SigningWriteError, _principal
from src.application.signing_write_query import SIGNING_WRITE_SETTINGS, _json, _projection
from src.application.workflow_contract import _require
from src.ingestion.setup_signing_tables import inspect
from src.ingestion.setup_workflow_tables import DATASET_ID, LOCATION, PROJECT

MAX_BOOTSTRAP_ATTEMPTS = 3
BOOTSTRAP_COMMAND = 'signing-bootstrap-v1'
SQL_PATH = Path(__file__).resolve().parents[2] / 'sql' / 'signing_bootstrap.sql'


def _now() -> datetime:
    return datetime.now(timezone.utc)


def bootstrap_job_id(attempt: int) -> str:
    _require(type(attempt) is int and 0 <= attempt < MAX_BOOTSTRAP_ATTEMPTS, 'INVALID_BOOTSTRAP_ATTEMPT')
    # Never include actor, command, config digest, time or a deployment epoch.
    namespace = hashlib.sha256(f'{PROJECT}/{LOCATION}/{POLICY_ID}'.encode()).hexdigest()
    return f'signing_bootstrap_v1_{namespace}_{attempt}'


def bootstrap_sql() -> str:
    sql = SQL_PATH.read_text(encoding='utf-8')
    inputs = []
    for table, name, parameter in (
        ('signing_publications', 'proposed_publication', 'publication_json'),
        ('signing_key_history', 'proposed_history', 'history_json'),
        ('signing_heads', 'proposed_head', 'head_json'),
    ):
        inputs.append(f'CREATE TEMP TABLE {name} AS SELECT {_projection(TABLES[table].schema)} '
                      f'FROM UNNEST(JSON_QUERY_ARRAY(@{parameter})) AS record;')
    sql = sql.replace('__INPUTS__', '\n  '.join(inputs))
    for name, spec in TABLES.items():
        sql = sql.replace('__' + name.upper() + '__', f'`{DATASET_ID}.{name}`')
        sql = sql.replace('__' + name.upper() + '_COLUMNS__', ', '.join(f.name for f in spec.schema))
    _require('__' not in sql, 'INVALID_BOOTSTRAP_TEMPLATE')
    return sql


@dataclass(frozen=True, slots=True)
class SigningBootstrapManifest:
    owner_principal_id: str
    publication: SigningPublication = field(repr=False)
    accepted_at: datetime
    deadline_at: datetime
    publication_json: str = field(init=False, repr=False)
    history_json: str = field(init=False, repr=False)
    head_json: str = field(init=False, repr=False)

    def __post_init__(self):
        _principal(self.owner_principal_id)
        pub = self.publication
        _require(isinstance(pub, SigningPublication) and pub.revision == 1 and pub.parent_digest is None,
                 'INVALID_BOOTSTRAP_GENESIS')
        accepted, deadline = _utc(self.accepted_at), _utc(self.deadline_at)
        _require(pub.published_at <= accepted and timedelta(seconds=1) <= deadline-accepted <= timedelta(seconds=300),
                 'INVALID_BOOTSTRAP_WINDOW')
        object.__setattr__(self, 'accepted_at', accepted)
        object.__setattr__(self, 'deadline_at', deadline)
        body, history = encode_publication(pub, publisher_id=self.owner_principal_id,
            command_id=BOOTSTRAP_COMMAND, recorded_at=accepted)
        head = dict(schema_version=STORAGE_VERSION, policy_id=POLICY_ID, revision=1, control_revision=1,
            publication_digest=pub.digest, published_at=pub.published_at,
            publisher_id=self.owner_principal_id, command_id=BOOTSTRAP_COMMAND)
        for name, rows in (('publication_json', [body]), ('history_json', history), ('head_json', [head])):
            object.__setattr__(self, name, _json(rows))

    def job_config(self, attempt: int) -> bigquery.QueryJobConfig:
        bootstrap_job_id(attempt)
        values = dict(policy_id=('STRING', POLICY_ID), attempt=('INT64', attempt),
            accepted_at=('TIMESTAMP', self.accepted_at), deadline_at=('TIMESTAMP', self.deadline_at),
            publication_json=('STRING', self.publication_json), history_json=('STRING', self.history_json),
            head_json=('STRING', self.head_json))
        return bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False, create_session=False,
            maximum_bytes_billed=SIGNING_WRITE_SETTINGS.maximum_bytes_billed,
            job_timeout_ms=SIGNING_WRITE_SETTINGS.job_timeout_ms,
            script_options=bigquery.ScriptOptions(statement_timeout_ms=SIGNING_WRITE_SETTINGS.statement_timeout_ms),
            query_parameters=[bigquery.ScalarQueryParameter(name, kind, value) for name, (kind, value) in values.items()],
            labels={'workflow': 'signing-bootstrap-v1'})


@dataclass(frozen=True, slots=True)
class BootstrapReceipt:
    observed_at: datetime
    publication_digest: str


def validate_bootstrap_result(manifest: SigningBootstrapManifest, rows: list[dict]) -> BootstrapReceipt:
    _require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict, 'INVALID_BOOTSTRAP_RESULT')
    row = rows[0]
    _require(set(row) == {'observed_at', 'head', 'publication', 'history', 'checkpoint_count'},
             'INVALID_BOOTSTRAP_RESULT')
    observed = _utc(row['observed_at'])
    _require(manifest.accepted_at <= observed < manifest.deadline_at
             and type(row['checkpoint_count']) is int and row['checkpoint_count'] == 0,
             'INVALID_BOOTSTRAP_RESULT')
    _require(_json([row['head']]) == manifest.head_json
             and _json([row['publication']]) == manifest.publication_json
             and _json(sorted(row['history'], key=lambda h: h['key_id'])) == manifest.history_json,
             'INVALID_BOOTSTRAP_RECEIPT')
    publication = decode_publication(row['publication'], tuple(decode_history(h) for h in row['history']))
    _require(publication.digest == manifest.publication.digest, 'INVALID_BOOTSTRAP_RECEIPT')
    return BootstrapReceipt(observed, publication.digest)


@dataclass(frozen=True, slots=True)
class BootstrapOutcome:
    job_id: str
    native_status: SharingCommitStatus
    receipt: BootstrapReceipt | None = None


class ProtectedSigningBootstrap:
    def __init__(self, *, client: bigquery.Client, identity: IapIdentityAdapter,
                 manifest: SigningBootstrapManifest, enabled: bool = False):
        _require(isinstance(client, bigquery.Client) and client.project == PROJECT and client.location == LOCATION
                 and client.default_query_job_config is None, 'INVALID_BOOTSTRAP_CLIENT')
        _require(isinstance(identity, IapIdentityAdapter) and isinstance(manifest, SigningBootstrapManifest)
                 and type(enabled) is bool, 'INVALID_BOOTSTRAP_SETTINGS')
        self.client, self.identity, self.manifest, self.enabled = client, identity, manifest, enabled
        self._attempted: set[int] = set()
        self._lock = Lock()

    def _authorize(self, headers, *, write: bool):
        if write and not self.enabled:
            raise SigningWriteError('SIGNING_BOOTSTRAP_DISABLED')
        actor = self.identity.authenticate(headers)
        ensure_fresh(actor)
        if actor.principal.principal_id != self.manifest.owner_principal_id:
            raise SigningWriteError('SIGNING_BOOTSTRAP_NOT_OWNER')
        return actor

    def _preflight(self):
        try:
            _require(self.client.project == PROJECT and self.client.location == LOCATION
                     and self.client.default_query_job_config is None
                     and inspect(self.client)['status'] == 'VERIFIED', 'BOOTSTRAP_RESOURCES_UNAVAILABLE')
        except Exception:
            raise SigningWriteError('BOOTSTRAP_RESOURCES_UNAVAILABLE') from None

    def _get(self, attempt: int):
        return self.client.get_job(bootstrap_job_id(attempt), project=PROJECT, location=LOCATION,
                                   retry=None, timeout=10)

    def _read_job(self, attempt: int, job) -> BootstrapOutcome:
        job_id = bootstrap_job_id(attempt)
        unknown = BootstrapOutcome(job_id, SharingCommitStatus.UNKNOWN)
        config = bigquery.QueryJobConfig.from_api_repr(deepcopy(self.manifest.job_config(attempt).to_api_repr()))
        sql = bootstrap_sql()
        if not matches_sharing_job(job, job_id=job_id, sql=sql, config=config) or job.state != 'DONE':
            return unknown
        if job.error_result is not None:
            status = failed_sharing_job_status(self.client, job_id=job_id, timeout_seconds=10, parent=job)
            return BootstrapOutcome(job_id, status)
        try:
            rows = [normalize(row) for row in islice(job.result(max_results=2, page_size=2,
                retry=None, job_retry=None, timeout=20), 2)]
            if not matches_sharing_job(job, job_id=job_id, sql=sql, config=config):
                return unknown
            receipt = validate_bootstrap_result(self.manifest, rows)
            return BootstrapOutcome(job_id, SharingCommitStatus.COMMITTED, receipt)
        except Exception:
            return unknown

    def recover(self, headers, *, attempt: int = 0) -> BootstrapOutcome:
        self._authorize(headers, write=False)
        job_id = bootstrap_job_id(attempt)
        self._preflight()
        try:
            return self._read_job(attempt, self._get(attempt))
        except Exception:
            return BootstrapOutcome(job_id, SharingCommitStatus.UNKNOWN)

    def submit(self, headers, *, attempt: int = 0) -> BootstrapOutcome:
        actor = self._authorize(headers, write=True)
        job_id = bootstrap_job_id(attempt)
        self._preflight()
        # Recover a known slot even after window expiry; never submit it again.
        try:
            existing = self._get(attempt)
        except NotFound:
            existing = None
        except Exception:
            return BootstrapOutcome(job_id, SharingCommitStatus.UNKNOWN)
        if existing is not None:
            return self._read_job(attempt, existing)
        # A higher slot requires fresh positive noncommit of EVERY ancestor.
        for predecessor in range(attempt):
            try:
                outcome = self._read_job(predecessor, self._get(predecessor))
            except Exception:
                outcome = BootstrapOutcome(bootstrap_job_id(predecessor), SharingCommitStatus.UNKNOWN)
            if outcome.native_status is not SharingCommitStatus.NOT_COMMITTED:
                raise SigningWriteError('BOOTSTRAP_PREDECESSOR_NOT_ROLLED_BACK')
        ensure_fresh(actor)
        if not self.manifest.accepted_at <= _now() < self.manifest.deadline_at:
            raise SigningWriteError('BOOTSTRAP_WINDOW_CLOSED')
        with self._lock:
            if attempt in self._attempted:
                return BootstrapOutcome(job_id, SharingCommitStatus.UNKNOWN)
            self._attempted.add(attempt)
        try:
            try:
                job = self.client.query(bootstrap_sql(), job_config=self.manifest.job_config(attempt),
                    job_id=job_id, project=PROJECT, location=LOCATION, retry=None, job_retry=None, timeout=10)
            except Conflict:
                # Another replica reserved exactly this slot. Read its job only.
                return self._read_job(attempt, self._get(attempt))
            expected = self.manifest.job_config(attempt)
            if not matches_sharing_job(job, job_id=job_id, sql=bootstrap_sql(), config=expected):
                return BootstrapOutcome(job_id, SharingCommitStatus.UNKNOWN)
            try:
                job.result(max_results=2, page_size=2, retry=None, job_retry=None, timeout=20)
            except Exception:
                pass
            return self._read_job(attempt, self._get(attempt))
        except Exception:
            return BootstrapOutcome(job_id, SharingCommitStatus.UNKNOWN)
