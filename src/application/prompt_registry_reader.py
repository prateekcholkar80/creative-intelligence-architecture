"""Fresh authoritative BigQuery reads for complete prompt-registry state.

Each request performs schema preflight and one bounded transaction in the fixed
project and region. BigQuery is the only serving source: there is no local,
cached, latest-version or repository fallback. This module never writes rows,
calls Vertex, activates prompts or dispatches models.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from itertools import islice
import re
from uuid import uuid4

from google.cloud import bigquery

from src.application.operation_budget import (
    OperationStopped,
    operation_call,
    operation_iter,
)
from src.application.prompt_contract import PromptLifecycle, PromptTask
from src.application.prompt_registry_codec import (
    MAX_PROMPT_HISTORY,
    PromptRegistryState,
    decode_registry_state,
    normalize,
)
from src.application.prompt_registry_schema import TABLES
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import ContractError, _require
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_prompt_registry_tables import inspect
from src.ingestion.setup_workflow_tables import DATASET_ID, PROJECT

_VERTEX_ID = re.compile(r'[1-9][0-9]{0,31}')


def _valid_prompt_key(value) -> bool:
    """Match the canonical prompt contract's 128-character identifier rule."""
    return (type(value) is str and 1 <= len(value) <= 128
            and all(not character.isspace() and ord(character) >= 32
                    and ord(character) != 127 for character in value))


class PromptRegistryLookupMode(str, Enum):
    EXACT_VERTEX_VERSION = 'EXACT_VERTEX_VERSION'
    ELIGIBLE_HEAD = 'ELIGIBLE_HEAD'


@dataclass(frozen=True, slots=True)
class PromptRegistryReadRequest:
    mode: PromptRegistryLookupMode
    vertex_prompt_id: str | None = None
    vertex_version_id: str | None = None
    task: PromptTask | None = None
    prompt_key: str | None = None

    def __post_init__(self):
        _require(isinstance(self.mode, PromptRegistryLookupMode),
                 'INVALID_PROMPT_REGISTRY_READ_REQUEST')
        if self.mode is PromptRegistryLookupMode.EXACT_VERTEX_VERSION:
            _require(self.task is None and self.prompt_key is None
                     and type(self.vertex_prompt_id) is str
                     and type(self.vertex_version_id) is str
                     and _VERTEX_ID.fullmatch(self.vertex_prompt_id) is not None
                     and _VERTEX_ID.fullmatch(self.vertex_version_id) is not None,
                     'INVALID_PROMPT_REGISTRY_READ_REQUEST')
        else:
            _require(self.vertex_prompt_id is None
                     and self.vertex_version_id is None
                     and isinstance(self.task, PromptTask)
                     and _valid_prompt_key(self.prompt_key),
                     'INVALID_PROMPT_REGISTRY_READ_REQUEST')

    @classmethod
    def exact(cls, vertex_prompt_id: str,
              vertex_version_id: str) -> 'PromptRegistryReadRequest':
        return cls(PromptRegistryLookupMode.EXACT_VERTEX_VERSION,
                   vertex_prompt_id=vertex_prompt_id,
                   vertex_version_id=vertex_version_id)

    @classmethod
    def eligible(cls, task: PromptTask,
                 prompt_key: str) -> 'PromptRegistryReadRequest':
        return cls(PromptRegistryLookupMode.ELIGIBLE_HEAD,
                   task=task, prompt_key=prompt_key)


@dataclass(frozen=True, slots=True)
class PromptRegistryReadSettings:
    maximum_bytes_billed: int = 64 * 1024**2
    job_timeout_ms: int = 30_000
    result_timeout_seconds: int = 30

    def __post_init__(self):
        _require(type(self.maximum_bytes_billed) is int
                 and 1 <= self.maximum_bytes_billed <= 1024**3
                 and type(self.job_timeout_ms) is int
                 and 1 <= self.job_timeout_ms <= 60_000
                 and type(self.result_timeout_seconds) is int
                 and 1 <= self.result_timeout_seconds <= 60,
                 'INVALID_PROMPT_REGISTRY_READ_SETTINGS')


@dataclass(frozen=True, slots=True)
class PromptRegistryRead:
    request: PromptRegistryReadRequest
    state: PromptRegistryState
    observed_at: datetime

    def __post_init__(self):
        _require(isinstance(self.request, PromptRegistryReadRequest)
                 and isinstance(self.state, PromptRegistryState)
                 and isinstance(self.observed_at, datetime)
                 and self.observed_at.tzinfo is not None
                 and self.observed_at.utcoffset() is not None
                 and self.observed_at.astimezone(timezone.utc)
                 >= self.state.head.changed_at,
                 'INVALID_PROMPT_REGISTRY_READ')
        object.__setattr__(self, 'observed_at',
                           self.observed_at.astimezone(timezone.utc))


class PromptRegistryReadError(RuntimeError):
    """Safe error code without SQL, prompt text or provider messages."""

    def __init__(self, code: str):
        self.code = code
        super().__init__('Prompt registry could not be read.')


def read_sql() -> str:
    current = TABLES['prompt_registry']
    history = TABLES['prompt_registry_history']
    current_columns = ', '.join(field.name for field in current.schema)
    history_columns = ', '.join(field.name for field in history.schema)
    current_table = f'`{DATASET_ID}.prompt_registry`'
    history_table = f'`{DATASET_ID}.prompt_registry_history`'
    return f'''BEGIN TRANSACTION;
CREATE TEMP TABLE prompt_registry_read_snapshot AS
WITH heads AS (
  SELECT {current_columns}
  FROM {current_table}
  WHERE (@lookup_mode = 'EXACT_VERTEX_VERSION'
         AND vertex_project = @vertex_project
         AND vertex_location = @vertex_location
         AND vertex_prompt_id = @vertex_prompt_id
         AND vertex_version_id = @vertex_version_id)
     OR (@lookup_mode = 'ELIGIBLE_HEAD'
         AND task = @task AND prompt_key = @prompt_key AND status = 'ACTIVE')
),
head_scope AS (
  SELECT prompt_key, snapshot_sha256 FROM heads LIMIT 2
),
history AS (
  SELECT {', '.join('h.' + field.name for field in history.schema)}
  FROM {history_table} h
  WHERE h.changed_at BETWEEN TIMESTAMP('0001-01-01 00:00:00+00')
                         AND TIMESTAMP('9999-12-31 23:59:59.999999+00')
    AND EXISTS (SELECT 1 FROM head_scope s
                WHERE s.prompt_key = h.prompt_key
                  AND s.snapshot_sha256 = h.snapshot_sha256)
)
SELECT CURRENT_TIMESTAMP() AS observed_at,
       (SELECT COUNT(*) FROM heads) AS head_count,
       (SELECT COUNT(*) FROM history) AS history_count,
       ARRAY(SELECT AS STRUCT {current_columns}
             FROM heads LIMIT 2) AS heads,
       ARRAY(SELECT AS STRUCT {history_columns}
             FROM history ORDER BY revision LIMIT {MAX_PROMPT_HISTORY + 1}) AS history;
COMMIT TRANSACTION;
SELECT * FROM prompt_registry_read_snapshot;'''


def read_config(request: PromptRegistryReadRequest,
                settings: PromptRegistryReadSettings) -> bigquery.QueryJobConfig:
    _require(isinstance(request, PromptRegistryReadRequest)
             and isinstance(settings, PromptRegistryReadSettings),
             'INVALID_PROMPT_REGISTRY_READ_REQUEST')
    values = {
        'lookup_mode': request.mode.value,
        'vertex_project': PROJECT,
        'vertex_location': LOCATION,
        'vertex_prompt_id': request.vertex_prompt_id or '',
        'vertex_version_id': request.vertex_version_id or '',
        'task': '' if request.task is None else request.task.value,
        'prompt_key': request.prompt_key or '',
    }
    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(name, 'STRING', value)
            for name, value in values.items()
        ],
        use_legacy_sql=False,
        use_query_cache=False,
        create_session=False,
        maximum_bytes_billed=settings.maximum_bytes_billed,
        job_timeout_ms=settings.job_timeout_ms,
        script_options=bigquery.ScriptOptions(
            statement_timeout_ms=settings.job_timeout_ms),
        labels={'workflow': 'prompt-registry-read-v1'})


def _decode(request: PromptRegistryReadRequest, rows: list[dict]) -> PromptRegistryRead:
    _require(type(rows) is list and len(rows) == 1
             and type(rows[0]) is dict,
             'INVALID_PROMPT_REGISTRY_READ_RESULT')
    row = rows[0]
    _require(set(row) == {
        'observed_at', 'head_count', 'history_count', 'heads', 'history'},
        'INVALID_PROMPT_REGISTRY_READ_RESULT')
    head_count, history_count = row['head_count'], row['history_count']
    heads, history = row['heads'], row['history']
    _require(type(head_count) is int and type(history_count) is int
             and type(heads) is list and type(history) is list,
             'INVALID_PROMPT_REGISTRY_READ_RESULT')
    _require(head_count != 0, 'PROMPT_REGISTRY_NOT_FOUND')
    _require(head_count == 1 and len(heads) == 1,
             'PROMPT_REGISTRY_DUPLICATE_HEAD')
    _require(history_count == len(history)
             and 1 <= history_count <= MAX_PROMPT_HISTORY,
             'PROMPT_REGISTRY_INCOMPLETE_HISTORY')
    state = decode_registry_state(heads[0], history)
    snapshot = state.head.snapshot
    if request.mode is PromptRegistryLookupMode.EXACT_VERTEX_VERSION:
        _require(snapshot.vertex_project == PROJECT
                 and snapshot.vertex_location == LOCATION
                 and snapshot.vertex_prompt_id == request.vertex_prompt_id
                 and snapshot.vertex_version_id == request.vertex_version_id,
                 'PROMPT_REGISTRY_SCOPE_MISMATCH')
    else:
        _require(snapshot.task is request.task
                 and snapshot.prompt_key == request.prompt_key
                 and state.head.status is PromptLifecycle.ACTIVE,
                 'PROMPT_REGISTRY_NOT_ELIGIBLE')
    return PromptRegistryRead(request, state, row['observed_at'])


class BigQueryPromptRegistryReader:
    def __init__(self, client: bigquery.Client,
                 settings: PromptRegistryReadSettings | None = None):
        if (getattr(client, 'project', None) != PROJECT
                or getattr(client, 'location', None) != LOCATION
                or getattr(client, 'default_query_job_config', None) is not None):
            raise ValueError(
                'Fixed-project/region ADC prompt-registry client required')
        self.client = client
        self.settings = (PromptRegistryReadSettings()
                         if settings is None else settings)
        _require(isinstance(self.settings, PromptRegistryReadSettings),
                 'INVALID_PROMPT_REGISTRY_READ_SETTINGS')

    def read_exact(self, vertex_prompt_id: str,
                   vertex_version_id: str) -> PromptRegistryRead:
        return self.read(PromptRegistryReadRequest.exact(
            vertex_prompt_id, vertex_version_id))

    def read_eligible(self, task: PromptTask,
                      prompt_key: str) -> PromptRegistryRead:
        return self.read(PromptRegistryReadRequest.eligible(task, prompt_key))

    def read(self, request: PromptRegistryReadRequest) -> PromptRegistryRead:
        _require(isinstance(request, PromptRegistryReadRequest),
                 'INVALID_PROMPT_REGISTRY_READ_REQUEST')
        try:
            _require(inspect(self.client)['status'] == 'VERIFIED',
                     'PROMPT_REGISTRY_RESOURCES_MISSING')
            sql = read_sql()
            config = read_config(request, self.settings)
            expected = bigquery.QueryJobConfig.from_api_repr(
                deepcopy(config.to_api_repr()))
            job_id = 'prompt_registry_read_' + uuid4().hex
            job = operation_call(
                self.client.query, sql, job_config=config, job_id=job_id,
                project=PROJECT, location=LOCATION, retry=None, job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT,
                timeout=self.settings.result_timeout_seconds)
            _require(matches_sharing_job(
                job, job_id=job_id, sql=sql, config=expected),
                'PROMPT_REGISTRY_READ_JOB_MISMATCH')
            result = operation_call(
                job.result, max_results=2, page_size=2, retry=None,
                job_retry=None, timeout=self.settings.result_timeout_seconds)
            rows = list(islice(operation_iter(result), 2))
            _require(job.state == 'DONE' and job.error_result is None
                     and matches_sharing_job(
                         job, job_id=job_id, sql=sql, config=expected),
                     'PROMPT_REGISTRY_READ_JOB_MISMATCH')
            return _decode(request, [normalize(row) for row in rows])
        except OperationStopped:
            raise
        except ContractError as error:
            raise PromptRegistryReadError(error.code) from None
        except Exception:
            raise PromptRegistryReadError(
                'PROMPT_REGISTRY_STORE_UNAVAILABLE') from None
