"""4.2 Critical/Must have/shared pilot: pinned write plans and explicit-schema SQL.

Pure proposals only. The IAP adapter must issue them from actual fresh captures.
Neither this module nor a typed plan authenticates, submits or bootstraps a head.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import json
from pathlib import Path
import re

from google.cloud import bigquery

from src.application.audit_contract import _opaque
from src.application.sharing_coordinator_contract import _utc
from src.application.sharing_publication_codec import (
    _row, decode_history, decode_publication, decode_read_result, encode_publication,
)
from src.application.sharing_publication_contract import (
    POLICY_ID, SigningPublication, _counter, validate_publication_transition,
)
from src.application.sharing_publication_reader import SigningCapturedRead
from src.application.sharing_publication_schema import STORAGE_VERSION, TABLES
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_contract import _digest, _require
from src.ingestion.setup_workflow_tables import DATASET_ID

SQL_PATH = Path(__file__).resolve().parents[2] / 'sql' / 'signing_control_write.sql'
MAX_WRITE_BYTES = 2 * 1024**2
# Multi-statement billing minima accumulate across the scoped validation/DML
# statements. The native probe exhausted the sharing script's 64 MiB cap before
# dispatch reached its fence. Keep this separate, explicit and bounded.
SIGNING_WRITE_SETTINGS = SharingQuerySettings(maximum_bytes_billed=512 * 1024**2)


class SigningWriteKind(str, Enum):
    PUBLICATION = 'PUBLICATION'
    CHECKPOINT = 'CHECKPOINT'


class SigningWriteState(str, Enum):
    APPLIED = 'APPLIED'
    REUSED = 'REUSED'
    SATISFIED = 'SATISFIED'


def write_job_id(kind: SigningWriteKind, actor_id: str, command_id: str, consumer_id: str) -> str:
    """Business intent ID, independent of read timestamps and publication pins."""
    _require(isinstance(kind, SigningWriteKind), 'INVALID_SIGNING_WRITE_PLAN')
    _opaque(command_id)
    scope = dict(kind=kind.value, actor=actor_id, command=command_id, consumer=consumer_id, policy=POLICY_ID)
    return 'signing_write_' + hashlib.sha256(_json(scope).encode()).hexdigest()


def _json(value) -> str:
    def convert(item):
        if isinstance(item, datetime):
            return _utc(item).isoformat()
        if type(item) is dict:
            return {key: convert(v) for key, v in item.items()}
        if type(item) in (tuple, list):
            return [convert(v) for v in item]
        _require(item is None or type(item) in (str, int, bool), 'INVALID_SIGNING_WRITE_JSON')
        return item
    encoded = json.dumps(convert(value), sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    _require(len(encoded.encode('utf-8')) <= MAX_WRITE_BYTES, 'SIGNING_WRITE_TOO_LARGE')
    return encoded


def _projection(fields, path='$'):
    parts = []
    for column in fields:
        target = path + '.' + column.name
        if column.field_type == 'RECORD':
            expression = 'STRUCT(' + _projection(column.fields, target) + ')'
        else:
            kind = {'INTEGER': 'INT64', 'BOOLEAN': 'BOOL', 'TIMESTAMP': 'TIMESTAMP', 'STRING': 'STRING'}[column.field_type]
            expression = f"CAST(JSON_VALUE(record, '{target}') AS {kind})"
        parts.append(expression + ' AS ' + column.name)
    return ', '.join(parts)


def render_write_sql() -> str:
    sql = SQL_PATH.read_text(encoding='utf-8')
    temp = []
    groups = [('expected_' + name, name, f"JSON_QUERY_ARRAY(@snapshot_json, '$.{group}')")
              for name, group in (('signing_heads', 'heads'), ('signing_checkpoints', 'checkpoints'),
                                  ('signing_publications', 'publications'), ('signing_key_history', 'history'))]
    groups += [('proposed_publication', 'signing_publications', 'JSON_QUERY_ARRAY(@publication_json)'),
               ('proposed_history', 'signing_key_history', 'JSON_QUERY_ARRAY(@history_json)'),
               ('proposed_checkpoint', 'signing_checkpoints', 'JSON_QUERY_ARRAY(@checkpoint_json)')]
    for name, schema, source in groups:
        temp.append(f'CREATE TEMP TABLE {name} AS SELECT {_projection(TABLES[schema].schema)} '
                    f'FROM UNNEST({source}) AS record;')
    sql = sql.replace('__TEMP_INPUTS__', '\n  '.join(temp))
    for name, spec in TABLES.items():
        sql = sql.replace('__' + name.upper() + '__', f'`{DATASET_ID}.{name}`')
        sql = sql.replace('__' + name.upper() + '_COLUMNS__', ', '.join(f.name for f in spec.schema))
    _require('__' not in sql, 'INVALID_SIGNING_WRITE_TEMPLATE')
    return sql


@dataclass(frozen=True, slots=True)
class SigningWritePlan:
    kind: SigningWriteKind
    actor_id: str
    command_id: str
    capture: SigningCapturedRead = field(repr=False)
    publication_json: str = field(repr=False)
    history_json: str = field(repr=False)
    checkpoint_json: str = field(repr=False)
    deadline_at: datetime
    settings: SharingQuerySettings = SIGNING_WRITE_SETTINGS
    snapshot_json: str = field(init=False, repr=False)

    def __post_init__(self):
        _require(isinstance(self.kind, SigningWriteKind) and isinstance(self.capture, SigningCapturedRead)
                 and isinstance(self.settings, SharingQuerySettings), 'INVALID_SIGNING_WRITE_PLAN')
        _require(type(self.actor_id) is str and re.fullmatch(r'iap-v1:[0-9a-f]{64}', self.actor_id),
                 'INVALID_SIGNING_WRITER')
        _opaque(self.command_id)
        deadline = _utc(self.deadline_at)
        _require(timedelta(seconds=1) <= deadline - self.capture.observation.observed_at <= timedelta(seconds=300),
                 'INVALID_SIGNING_WRITE_WINDOW')
        object.__setattr__(self, 'deadline_at', deadline)
        object.__setattr__(self, 'snapshot_json', _json(self.capture.rows))
        for raw in (self.publication_json, self.history_json, self.checkpoint_json):
            _require(type(raw) is str and _json(json.loads(raw)) == raw, 'INVALID_SIGNING_WRITE_JSON')

    @property
    def job_id(self):
        # One actor/kind/consumer/command has one original job, even if pins change.
        return write_job_id(self.kind, self.actor_id, self.command_id, self.capture.request.consumer_id)

    @property
    def sql(self):
        return render_write_sql()

    def job_config(self):
        values = dict(kind=('STRING', self.kind.value), actor_id=('STRING', self.actor_id),
            command_id=('STRING', self.command_id), policy_id=('STRING', POLICY_ID),
            schema_version=('STRING', STORAGE_VERSION), consumer_id=('STRING', self.capture.request.consumer_id),
            accepted_at=('TIMESTAMP', self.capture.observation.observed_at), deadline_at=('TIMESTAMP', self.deadline_at),
            snapshot_json=('STRING', self.snapshot_json), publication_json=('STRING', self.publication_json),
            history_json=('STRING', self.history_json), checkpoint_json=('STRING', self.checkpoint_json))
        return bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False, create_session=False,
            maximum_bytes_billed=self.settings.maximum_bytes_billed, job_timeout_ms=self.settings.job_timeout_ms,
            script_options=bigquery.ScriptOptions(statement_timeout_ms=self.settings.statement_timeout_ms),
            query_parameters=[bigquery.ScalarQueryParameter(n, kind, v) for n, (kind, v) in values.items()],
            labels={'workflow': 'signing-write-v1'})


def prepare_write(capture: SigningCapturedRead, *, kind: SigningWriteKind, actor_id: str, command_id: str,
                  publication: SigningPublication | None = None, settings=SIGNING_WRITE_SETTINGS) -> SigningWritePlan:
    """Validate supplied capture and immutable proposals, without proving I/O/access."""
    _require(isinstance(capture, SigningCapturedRead) and isinstance(kind, SigningWriteKind),
             'INVALID_SIGNING_WRITE_CAPTURE')
    rows = capture.rows
    observation = decode_read_result(capture.request, [rows], now=capture.observation.observed_at)
    _require(observation == capture.observation, 'INVALID_SIGNING_WRITE_CAPTURE')
    head = observation.publications[-1]
    checkpoint = []
    if kind is SigningWriteKind.PUBLICATION:
        _require(isinstance(publication, SigningPublication)
                 and publication.config.project_number == capture.request.project_number
                 and publication.published_at <= observation.observed_at, 'INVALID_SIGNING_WRITE_PUBLICATION')
        existing = [p for p in rows['publications'] if p['revision'] == publication.revision]
        if existing:
            stored = existing[0]
            _require(stored['publication_digest'] == publication.digest and stored['publisher_id'] == actor_id
                     and stored['command_id'] == command_id, 'SIGNING_PUBLICATION_REPLAY_CONFLICT')
            body = stored
            history = [h for h in rows['history'] if h['publication_revision'] == publication.revision]
        else:
            validate_publication_transition(head, publication)
            body, history = encode_publication(publication, publisher_id=actor_id, command_id=command_id,
                                               recorded_at=observation.observed_at)
    else:
        _require(publication is None, 'INVALID_SIGNING_CHECKPOINT_TARGET')
        body = next(p for p in rows['publications'] if p['revision'] == head.revision)
        history = [h for h in rows['history'] if h['publication_revision'] == head.revision]
        previous = rows['checkpoints'][0] if rows['checkpoints'] else None
        if previous is not None and previous['publication_revision'] == head.revision:
            checkpoint = [previous]  # Preserve the existing floor and its stamps.
        else:
            revision = 1 if previous is None else previous['checkpoint_revision'] + 1
            _counter(revision)
            checkpoint = [dict(schema_version=STORAGE_VERSION, policy_id=POLICY_ID,
                consumer_id=capture.request.consumer_id, checkpoint_revision=revision,
                publication_revision=head.revision, publication_digest=head.digest,
                instance_id=capture.request.ticket.instance_id, updated_at=observation.observed_at,
                writer_id=actor_id, command_id=command_id)]
    history = sorted(history, key=lambda h: h['key_id'])
    return SigningWritePlan(kind, actor_id, command_id, capture, _json([body]), _json(history), _json(checkpoint),
                            observation.observed_at + timedelta(seconds=300), settings)


@dataclass(frozen=True, slots=True)
class SigningWriteResult:
    state: SigningWriteState
    observed_at: datetime
    publication_revision: int
    control_revision: int
    checkpoint_revision: int | None


def validate_write_result(plan: SigningWritePlan, rows: list[dict]) -> SigningWriteResult:
    _require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict, 'INVALID_SIGNING_WRITE_RESULT')
    row = rows[0]
    _require(set(row) == {'state', 'observed_at', 'head', 'publication', 'history', 'checkpoints'},
             'INVALID_SIGNING_WRITE_RESULT')
    state = SigningWriteState(row['state'])
    observed = _utc(row['observed_at'])
    _require(plan.capture.observation.observed_at <= observed < plan.deadline_at, 'INVALID_SIGNING_WRITE_RESULT_TIME')
    _row(row['head'], 'signing_heads')
    head = row['head']
    _counter(head['revision'])
    _counter(head['control_revision'])
    _digest(head['publication_digest'])
    _require(head['published_at'] <= observed, 'INVALID_SIGNING_WRITE_RESULT')
    history = tuple(decode_history(h) for h in row['history'])
    publication = decode_publication(row['publication'], history)
    _require(_json([row['publication']]) == plan.publication_json
             and _json(sorted(row['history'], key=lambda h: h['key_id'])) == plan.history_json
             and publication.revision <= head['revision'], 'INVALID_SIGNING_WRITE_RECEIPT')
    if publication.revision == head['revision']:
        _require(publication.digest == head['publication_digest'] and publication.published_at == head['published_at'],
                 'INVALID_SIGNING_WRITE_RECEIPT')
    original = json.loads(plan.snapshot_json)['heads'][0]
    _require(head['control_revision'] >= original['control_revision'], 'INVALID_SIGNING_WRITE_RESULT')
    if state is SigningWriteState.APPLIED:
        expected_head = dict(original, control_revision=original['control_revision'] + 1)
        if plan.kind is SigningWriteKind.PUBLICATION:
            expected_head.update(revision=publication.revision, publication_digest=publication.digest,
                published_at=publication.published_at, publisher_id=plan.actor_id, command_id=plan.command_id)
        _require(_json(head) == _json(expected_head), 'INVALID_SIGNING_WRITE_RESULT')
    _require(type(row['checkpoints']) is list and len(row['checkpoints']) <= 1, 'INVALID_SIGNING_WRITE_RESULT')
    checkpoint = row['checkpoints'][0] if row['checkpoints'] else None
    if checkpoint is not None:
        _row(checkpoint, 'signing_checkpoints')
        _counter(checkpoint['checkpoint_revision'])
        _counter(checkpoint['publication_revision'])
        _digest(checkpoint['publication_digest'])
        _require(checkpoint['consumer_id'] == plan.capture.request.consumer_id
                 and checkpoint['updated_at'] <= observed
                 and checkpoint['publication_revision'] <= head['revision'], 'INVALID_SIGNING_WRITE_RESULT')
    if plan.kind is SigningWriteKind.CHECKPOINT:
        _require(checkpoint is not None and checkpoint['publication_revision'] >= publication.revision,
                 'INVALID_SIGNING_WRITE_RECEIPT')
        if checkpoint['publication_revision'] == publication.revision:
            _require(checkpoint['publication_digest'] == publication.digest
                     and checkpoint['updated_at'] >= publication.published_at, 'INVALID_SIGNING_WRITE_RECEIPT')
        if state in (SigningWriteState.APPLIED, SigningWriteState.REUSED):
            _require(_json([checkpoint]) == plan.checkpoint_json, 'INVALID_SIGNING_WRITE_RECEIPT')
    else:
        _require(state is not SigningWriteState.SATISFIED, 'INVALID_SIGNING_WRITE_RESULT')
    return SigningWriteResult(state, observed, publication.revision, head['control_revision'],
                              None if checkpoint is None else checkpoint['checkpoint_revision'])
