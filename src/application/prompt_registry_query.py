"""Parameterized initial prompt-registration transaction contract; no I/O.

The query plan accepts only the codec's revision-one DRAFT state. It renders
fixed table/schema metadata, bounded BigQuery configuration and a stable job ID
derived from the exact Vertex version identity. A future executor must use that
job ID, verify the native job configuration/result and recover uncertain outcomes
without blind resubmission.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from pathlib import Path
import re

from google.cloud import bigquery

from src.application.prompt_contract import PROMPT_CONTRACT_VERSION, PromptLifecycle
from src.application.prompt_registry_codec import (
    PromptRegistryState, encode_registry_state,
)
from src.application.prompt_registry_schema import STORAGE_VERSION, TABLES
from src.application.workflow_contract import ContractError, _require, canonical_json_bytes
from src.application.workflow_schema import DATASET, LOCATION
from src.ingestion.setup_workflow_tables import PROJECT

SQL_PATH = Path(__file__).resolve().parents[2] / 'sql' / 'register_prompt.sql'
_TABLE_NAMES = ('prompt_registry', 'prompt_registry_history')
_LIFECYCLE_COLUMNS = {'revision', 'parent_digest', 'status', 'evaluation_id',
                      'changed_at', 'changed_by', 'reason_code', 'record_digest'}


def _type(field: bigquery.SchemaField) -> str:
    kind = {'STRING': 'STRING', 'INTEGER': 'INT64', 'TIMESTAMP': 'TIMESTAMP'}[
        field.field_type]
    return f'ARRAY<{kind}>' if field.mode == 'REPEATED' else kind


def _json_value(field: bigquery.SchemaField, parameter: str) -> str:
    path = "'$." + field.name + "'"
    source = f'PARSE_JSON(@{parameter})'
    if field.mode == 'REPEATED':
        value = f'JSON_VALUE_ARRAY({source}, {path})'
    else:
        value = f'JSON_VALUE({source}, {path})'
        if field.field_type == 'INTEGER':
            value = f'CAST({value} AS INT64)'
        elif field.field_type == 'TIMESTAMP':
            value = f'CAST({value} AS TIMESTAMP)'
    return f'{value} AS {field.name}'


def _render_sql() -> str:
    """Render fixed identifiers, types and typed JSON row expressions."""
    sql = SQL_PATH.read_text(encoding='utf-8')
    for name in _TABLE_NAMES:
        spec = TABLES[name]
        token = '__' + name.upper()
        fields = spec.schema
        sql = sql.replace(token + '_TYPE__', ', '.join(
            f'{field.name} {_type(field)}' for field in fields))
        sql = sql.replace(token + '_COLUMNS__', ', '.join(
            field.name for field in fields))
        sql = sql.replace(token + '_TABLE__',
            f'`{PROJECT}.{DATASET}.{name}`')
        parameter = 'registry_json' if name == 'prompt_registry' else 'history_json'
        sql = sql.replace(token + '_JSON_ROW__', ', '.join(
            _json_value(field, parameter) for field in fields))
        alias = 'candidate' if name == 'prompt_registry' else 'history_candidate'
        sql = sql.replace(token + '_CANDIDATE_COLUMNS__', ', '.join(
            f'{alias}.{field.name}' for field in fields))

    history_fields = TABLES['prompt_registry_history'].schema
    common = [field.name for field in history_fields if field.name != 'schema_version']
    sql = sql.replace('__HISTORY_MATCHES_CANDIDATE__', ' AND '.join(
        f'history_candidate.{name} IS NOT DISTINCT FROM candidate.{name}'
        for name in common))

    immutable = [field.name for field in TABLES['prompt_registry'].schema
                 if field.name not in _LIFECYCLE_COLUMNS]
    left = ', '.join(f'current_row.{name} AS {name}' for name in immutable)
    right = ', '.join(f'candidate.{name} AS {name}' for name in immutable)
    sql = sql.replace('__REGISTRY_IMMUTABLE_MATCH__',
        f'TO_JSON_STRING(STRUCT({left})) = TO_JSON_STRING(STRUCT({right}))')
    _require(not re.search(r'__[A-Z_]+__', sql),
             'INVALID_PROMPT_REGISTRATION_SQL_TEMPLATE')
    return sql


def _wire(row: dict) -> str:
    def scalar(value):
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        raise TypeError()
    return json.dumps(row, default=scalar, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':'))


@dataclass(frozen=True, slots=True)
class PromptRegistrationQuerySettings:
    maximum_bytes_billed: int = 64 * 1024**2
    job_timeout_ms: int = 60_000
    statement_timeout_ms: int = 20_000

    def __post_init__(self):
        _require(type(self.maximum_bytes_billed) is int
                 and 1 <= self.maximum_bytes_billed <= 1024**3,
                 'INVALID_PROMPT_REGISTRATION_QUERY_SETTINGS')
        _require(type(self.job_timeout_ms) is int
                 and type(self.statement_timeout_ms) is int
                 and 1 <= self.statement_timeout_ms <= self.job_timeout_ms <= 60_000,
                 'INVALID_PROMPT_REGISTRATION_QUERY_SETTINGS')


@dataclass(frozen=True, slots=True)
class PromptRegistrationQuery:
    state: PromptRegistryState
    settings: PromptRegistrationQuerySettings = PromptRegistrationQuerySettings()

    def __post_init__(self):
        _require(isinstance(self.state, PromptRegistryState)
                 and len(self.state.history) == 1
                 and self.state.head.revision == 1
                 and self.state.head.status is PromptLifecycle.DRAFT,
                 'INVALID_INITIAL_PROMPT_REGISTRATION')
        _require(isinstance(self.settings, PromptRegistrationQuerySettings),
                 'INVALID_PROMPT_REGISTRATION_QUERY_SETTINGS')

    @property
    def sql(self) -> str:
        return _render_sql()

    @property
    def location(self) -> str:
        return LOCATION

    @property
    def job_id(self) -> str:
        snapshot = self.state.head.snapshot
        # Exclude content/import metadata so competing registrations for one
        # immutable Vertex version share the same native idempotency boundary.
        identity = [STORAGE_VERSION, snapshot.vertex_project,
                    snapshot.vertex_location, snapshot.vertex_prompt_id,
                    snapshot.vertex_version_id]
        digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        return 'prompt_register_' + digest

    def rows(self) -> tuple[dict, dict]:
        current, history = encode_registry_state(self.state)
        _require(len(history) == 1, 'INVALID_INITIAL_PROMPT_REGISTRATION')
        return current, history[0]

    def job_config(self) -> bigquery.QueryJobConfig:
        """Build fresh bounded configuration without a client or submission."""
        current, history = self.rows()
        values = dict(registry_json=('STRING', _wire(current)),
            history_json=('STRING', _wire(history)),
            storage_version=('STRING', STORAGE_VERSION),
            contract_version=('STRING', PROMPT_CONTRACT_VERSION),
            vertex_project=('STRING', current['vertex_project']),
            vertex_location=('STRING', current['vertex_location']))
        parameters = set(re.findall(r'(?<!@)@([a-z_]+)', self.sql))
        _require(parameters == set(values),
                 'INVALID_PROMPT_REGISTRATION_SQL_PARAMETERS')
        return bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(name, kind, value)
                              for name, (kind, value) in values.items()],
            use_legacy_sql=False, use_query_cache=False, create_session=False,
            maximum_bytes_billed=self.settings.maximum_bytes_billed,
            job_timeout_ms=self.settings.job_timeout_ms,
            script_options=bigquery.ScriptOptions(
                statement_timeout_ms=self.settings.statement_timeout_ms),
            labels={'workflow': 'prompt-registry-v1'})


class PromptRegistrationResultState(str, Enum):
    APPLIED = 'APPLIED'
    REUSED = 'REUSED'


@dataclass(frozen=True, slots=True)
class PromptRegistrationResult:
    state: PromptRegistrationResultState
    observed_at: datetime
    prompt_key: str
    snapshot_sha256: str
    record_digest: str


def validate_prompt_registration_result(query: PromptRegistrationQuery,
                                        rows: list[dict]) -> PromptRegistrationResult:
    """Validate caller-supplied final rows; this is not native commit proof."""
    _require(isinstance(query, PromptRegistrationQuery)
             and type(rows) is list and len(rows) == 1 and type(rows[0]) is dict,
             'INVALID_PROMPT_REGISTRATION_RESULT')
    row = rows[0]
    _require(set(row) == {'state', 'observed_at', 'prompt_key',
                          'snapshot_sha256', 'record_digest'},
             'INVALID_PROMPT_REGISTRATION_RESULT')
    _require(all(type(row[name]) is str for name in (
        'state', 'prompt_key', 'snapshot_sha256', 'record_digest')),
        'INVALID_PROMPT_REGISTRATION_RESULT')
    try:
        state = PromptRegistrationResultState(row['state'])
    except (TypeError, ValueError):
        raise ContractError('INVALID_PROMPT_REGISTRATION_RESULT_STATE') from None
    record = query.state.head
    _require(isinstance(row['observed_at'], datetime)
             and row['observed_at'].tzinfo is not None
             and row['observed_at'].utcoffset() is not None
             and row['observed_at'].astimezone(timezone.utc) >= record.changed_at
             and row['prompt_key'] == record.snapshot.prompt_key
             and row['snapshot_sha256'] == record.snapshot.snapshot_sha256
             and row['record_digest'] == record.digest,
             'PROMPT_REGISTRATION_RESULT_MISMATCH')
    return PromptRegistrationResult(state, row['observed_at'].astimezone(timezone.utc),
        row['prompt_key'], row['snapshot_sha256'], row['record_digest'])
