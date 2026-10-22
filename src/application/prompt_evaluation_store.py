"""Default-disabled native writes and bounded single-snapshot evaluation reads."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.identity import VerifiedIdentity
from src.application.operation_budget import OperationStopped, operation_call, operation_iter
from src.application.prompt_evaluation_contract import MAX_CASES, _utc
from src.application.prompt_evaluation_query import (
    PromptEvaluationWrite, access_sql, columns, validate_write_receipt,
)
from src.application.prompt_evaluation_storage import (
    MAX_CONTROL_HISTORY, TABLES, decode_control, decode_control_history, decode_evaluation,
)
from src.application.prompt_registry_codec import MAX_PROMPT_HISTORY, decode_registry_state, normalize
from src.application.prompt_registry_schema import TABLES as REGISTRY_TABLES
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_jobs import failed_sharing_job_status, matches_sharing_job
from src.application.workflow_contract import ContractError, _id, _require
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES, LOCATION
from src.ingestion.setup_prompt_evaluation_tables import inspect
from src.ingestion.setup_workflow_tables import DATASET_ID, PROJECT, validate_table


class EvaluationStoreError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__('Prompt evaluation storage unavailable.')


@dataclass(frozen=True, slots=True)
class EvaluationStoreSettings:
    execution_enabled: bool = False
    rpc_timeout_seconds: int = 10
    result_timeout_seconds: int = 30

    def __post_init__(self):
        _require(type(self.execution_enabled) is bool
                 and type(self.rpc_timeout_seconds) is int and 1 <= self.rpc_timeout_seconds <= 20
                 and type(self.result_timeout_seconds) is int and 1 <= self.result_timeout_seconds <= 60,
                 'INVALID_EVALUATION_STORE_SETTINGS')


@dataclass(frozen=True, slots=True)
class EvaluationWriteOutcome:
    native_job_id: str
    native_status: SharingCommitStatus
    receipt: dict | None = None
    submitted: bool = False

    def __post_init__(self):
        _require(type(self.native_job_id) is str and bool(self.native_job_id)
                 and isinstance(self.native_status, SharingCommitStatus)
                 and type(self.submitted) is bool
                 and (self.receipt is None or type(self.receipt) is dict
                      and self.native_status is SharingCommitStatus.COMMITTED),
                 'INVALID_EVALUATION_WRITE_OUTCOME')


@dataclass(frozen=True, slots=True)
class EvaluationReadRequest:
    workspace_id: str
    prompt_key: str
    identity: VerifiedIdentity
    membership_revision: int
    evaluation_id: str | None = None

    def __post_init__(self):
        _id(self.workspace_id)
        _id(self.prompt_key)
        if self.evaluation_id is not None:
            _id(self.evaluation_id)
        _require(isinstance(self.identity, VerifiedIdentity)
                 and type(self.membership_revision) is int and self.membership_revision >= 1,
                 'INVALID_EVALUATION_READ_REQUEST')


@dataclass(frozen=True, slots=True)
class EvaluationSnapshot:
    request: EvaluationReadRequest
    control: object
    control_history: tuple
    record: object
    registry_state: object
    observed_at: datetime

    def __post_init__(self):
        _require(isinstance(self.request, EvaluationReadRequest), 'INVALID_EVALUATION_SNAPSHOT')
        object.__setattr__(self, 'observed_at', _utc(self.observed_at))
        if self.control is None:
            _require(not self.control_history and self.record is None and self.registry_state is None,
                     'INVALID_EVALUATION_SNAPSHOT')
        else:
            from src.application.prompt_evaluation_storage import encode_control
            decode_control_history(self.control, [encode_control(item) for item in self.control_history])
            _require(self.control.workspace_id == self.request.workspace_id
                     and self.control.prompt_key == self.request.prompt_key
                     and self.observed_at >= self.control.updated_at, 'EVALUATION_SNAPSHOT_SCOPE_MISMATCH')
        if self.request.evaluation_id is None:
            _require(self.record is None and self.registry_state is None, 'INVALID_EVALUATION_SNAPSHOT')
        else:
            _require(self.control is not None and self.record is not None and self.registry_state is not None
                     and self.record.evaluation_id == self.request.evaluation_id
                     and self.observed_at >= self.record.completed_at
                     and self.observed_at >= self.registry_state.head.changed_at,
                     'INVALID_EVALUATION_SNAPSHOT')


def read_sql():
    specs = dict(TABLES) | dict(REGISTRY_TABLES)
    selects = []
    for name in ('prompt_evaluation_controls', 'prompt_evaluation_control_history', 'prompt_evaluations',
                 'prompt_evaluation_cases', 'prompt_registry', 'prompt_registry_history'):
        spec = specs[name]
        if name in {'prompt_evaluation_controls', 'prompt_evaluation_control_history'}:
            predicate = 'workspace_id=@workspace_id AND prompt_key=@prompt_key'
        elif name in {'prompt_evaluations', 'prompt_evaluation_cases'}:
            predicate = "@evaluation_id != '' AND workspace_id=@workspace_id AND evaluation_id=@evaluation_id"
        else:
            predicate = '''prompt_key=@prompt_key AND snapshot_sha256 IN
                (SELECT snapshot_sha256 FROM prompt_evaluations_snapshot LIMIT 2)'''
            if name == 'prompt_registry_history':
                predicate += " AND changed_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31 23:59:59.999999+00')"
        selects.append(f'''CREATE TEMP TABLE {name}_snapshot AS SELECT {columns(spec)}
FROM `{DATASET_ID}.{name}` WHERE {predicate};''')
    result = []
    limits = {'prompt_evaluation_controls': 2, 'prompt_evaluation_control_history': MAX_CONTROL_HISTORY + 1,
              'prompt_evaluations': 2, 'prompt_evaluation_cases': MAX_CASES + 1,
              'prompt_registry': 2, 'prompt_registry_history': MAX_PROMPT_HISTORY + 1}
    for name, limit in limits.items():
        order = ' ORDER BY revision' if name.endswith('history') else ' ORDER BY case_id' \
            if name == 'prompt_evaluation_cases' else ''
        result.extend([f'(SELECT COUNT(*) FROM {name}_snapshot) AS {name}_count',
            f'ARRAY(SELECT AS STRUCT * FROM {name}_snapshot{order} LIMIT {limit}) AS {name}'])
    return f'''BEGIN TRANSACTION;
{access_sql()}
ASSERT CURRENT_TIMESTAMP() < @assertion_expires_at AS 'ASSERTION_EXPIRED';
{chr(10).join(selects)}
CREATE TEMP TABLE evaluation_read_receipt AS SELECT CURRENT_TIMESTAMP() AS observed_at,
{', '.join(result)};
COMMIT TRANSACTION;
SELECT * FROM evaluation_read_receipt;'''


def read_config(request):
    from datetime import timezone
    values = dict(workspace_id=('STRING', request.workspace_id), prompt_key=('STRING', request.prompt_key),
        principal_id=('STRING', request.identity.principal.principal_id), issuer=('STRING', request.identity.issuer),
        subject=('STRING', request.identity.subject), membership_revision=('INT64', request.membership_revision),
        evaluation_id=('STRING', request.evaluation_id or ''), assertion_expires_at=('TIMESTAMP',
            datetime.fromtimestamp(request.identity.expires_at, timezone.utc)))
    return bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(k, kind, v)
        for k, (kind, v) in values.items()], use_legacy_sql=False, use_query_cache=False,
        create_session=False, maximum_bytes_billed=512 * 1024**2, job_timeout_ms=60000,
        script_options=bigquery.ScriptOptions(statement_timeout_ms=20000),
        labels={'workflow': 'prompt-evaluation-read-v1'})


def decode_snapshot(request, rows):
    _require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict,
             'INVALID_EVALUATION_READ_RESULT')
    row = rows[0]
    names = tuple(TABLES) + tuple(REGISTRY_TABLES)
    _require(set(row) == {'observed_at'} | set(names) | {name + '_count' for name in names},
             'INVALID_EVALUATION_READ_SHAPE')
    for name in names:
        _require(type(row[name]) is list and type(row[name + '_count']) is int
                 and row[name + '_count'] == len(row[name]), 'INCOMPLETE_EVALUATION_READ')
    heads, history = row['prompt_evaluation_controls'], row['prompt_evaluation_control_history']
    _require(len(heads) <= 1, 'DUPLICATE_EVALUATION_CONTROL')
    if not heads:
        _require(not history and all(not row[name] for name in names if name not in {
            'prompt_evaluation_controls', 'prompt_evaluation_control_history'}), 'ORPHAN_EVALUATION_STORAGE')
        _require(request.evaluation_id is None, 'EVALUATION_NOT_FOUND')
        return EvaluationSnapshot(request, None, (), None, None, row['observed_at'])
    head = decode_control(heads[0])
    chain = decode_control_history(head, history)
    if request.evaluation_id is None:
        _require(all(not row[name] for name in ('prompt_evaluations', 'prompt_evaluation_cases',
            'prompt_registry', 'prompt_registry_history')), 'INVALID_EVALUATION_CONTROL_READ')
        return EvaluationSnapshot(request, head, chain, None, None, row['observed_at'])
    evaluations = row['prompt_evaluations']
    if not evaluations:
        _require(not row['prompt_evaluation_cases'], 'ORPHAN_EVALUATION_CASES')
        raise EvaluationStoreError('EVALUATION_NOT_FOUND')
    _require(len(evaluations) == 1 and len(row['prompt_registry']) == 1
             and len(row['prompt_evaluation_cases']) <= MAX_CASES
             and len(row['prompt_registry_history']) <= MAX_PROMPT_HISTORY,
             'DUPLICATE_OR_INCOMPLETE_EVALUATION')
    selected = [item for item in chain if item.digest == evaluations[0]['control_digest']]
    _require(len(selected) == 1, 'EVALUATION_CONTROL_ANCHOR_MISSING')
    state = decode_registry_state(row['prompt_registry'][0], row['prompt_registry_history'])
    record = decode_evaluation(selected[0], evaluations[0], row['prompt_evaluation_cases'], state)
    return EvaluationSnapshot(request, head, chain, record, state, row['observed_at'])


class BigQueryPromptEvaluationStore:
    def __init__(self, client, settings=None):
        if (client.project != PROJECT or client.location != LOCATION
                or client.default_query_job_config is not None):
            raise ValueError('Fixed-project/region client required')
        self.client = client
        self.settings = EvaluationStoreSettings() if settings is None else settings
        _require(isinstance(self.settings, EvaluationStoreSettings), 'INVALID_EVALUATION_STORE_SETTINGS')

    def _preflight(self):
        _require(inspect(self.client)['status'] == 'VERIFIED', 'EVALUATION_RESOURCES_MISSING')
        for spec in tuple(REGISTRY_TABLES.values()) + tuple(WORKFLOW_TABLES[name] for name in ('workspaces', 'memberships')):
            expected = spec.table(PROJECT)
            validate_table(operation_call(self.client.get_table, expected.reference,
                retry=None, timeout=self.settings.rpc_timeout_seconds), expected)

    def _enabled(self, plan):
        _require(self.settings.execution_enabled, 'EVALUATION_WRITES_DISABLED')
        _require(isinstance(plan, PromptEvaluationWrite), 'INVALID_EVALUATION_WRITE')

    def _outcome(self, plan, job, *, submitted=False):
        unknown = EvaluationWriteOutcome(plan.job_id, SharingCommitStatus.UNKNOWN, submitted=submitted)
        config = plan.job_config()
        if not matches_sharing_job(job, job_id=plan.job_id, sql=plan.sql, config=config):
            return unknown
        try:
            rows = None
            if job.state != 'DONE':
                rows = list(islice(operation_iter(operation_call(job.result, max_results=2, page_size=2,
                    retry=None, job_retry=None, timeout=self.settings.result_timeout_seconds)), 2))
            if job.state != 'DONE' or not matches_sharing_job(job, job_id=plan.job_id, sql=plan.sql, config=config):
                return unknown
            if job.error_result is not None:
                status = failed_sharing_job_status(self.client, job_id=plan.job_id,
                    timeout_seconds=self.settings.rpc_timeout_seconds, parent=job)
                # A correlated COMMIT can establish commit despite post-commit failure;
                # it cannot invent the missing validated business receipt.
                return EvaluationWriteOutcome(plan.job_id, status, submitted=submitted)
            if rows is None:
                rows = list(islice(operation_iter(operation_call(job.result, max_results=2, page_size=2,
                    retry=None, job_retry=None, timeout=self.settings.result_timeout_seconds)), 2))
            receipt = validate_write_receipt(plan, [normalize(row) for row in rows])
            if not matches_sharing_job(job, job_id=plan.job_id, sql=plan.sql, config=config):
                return unknown
            return EvaluationWriteOutcome(plan.job_id, SharingCommitStatus.COMMITTED, receipt, submitted)
        except OperationStopped:
            raise
        except Exception:
            return unknown

    def recover(self, plan):
        self._enabled(plan)
        try:
            job = operation_call(self.client.get_job, plan.job_id, project=PROJECT,
                location=LOCATION, retry=None, timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, job)
        except OperationStopped:
            raise
        except Exception:
            return EvaluationWriteOutcome(plan.job_id, SharingCommitStatus.UNKNOWN)

    def submit(self, plan):
        self._enabled(plan)
        try:
            self._preflight()
        except OperationStopped:
            raise
        except Exception:
            raise EvaluationStoreError('EVALUATION_SCHEMA_PREFLIGHT_FAILED') from None
        try:
            existing = operation_call(self.client.get_job, plan.job_id, project=PROJECT,
                location=LOCATION, retry=None, timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, existing)
        except NotFound:
            pass
        except OperationStopped:
            raise
        except Exception:
            return EvaluationWriteOutcome(plan.job_id, SharingCommitStatus.UNKNOWN)
        try:
            job = operation_call(self.client.query, plan.sql, job_config=plan.job_config(), job_id=plan.job_id,
                project=PROJECT, location=LOCATION, retry=None, job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, job, submitted=True)
        except OperationStopped:
            raise
        except Exception:
            outcome = self.recover(plan)
            return EvaluationWriteOutcome(outcome.native_job_id, outcome.native_status, outcome.receipt, True)

    def read(self, request):
        _require(isinstance(request, EvaluationReadRequest), 'INVALID_EVALUATION_READ_REQUEST')
        try:
            self._preflight()
            sql, config = read_sql(), read_config(request)
            expected = bigquery.QueryJobConfig.from_api_repr(deepcopy(config.to_api_repr()))
            job_id = 'prompt_evaluation_read_' + uuid4().hex
            job = operation_call(self.client.query, sql, job_config=config, job_id=job_id,
                project=PROJECT, location=LOCATION, retry=None, job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=self.settings.rpc_timeout_seconds)
            _require(matches_sharing_job(job, job_id=job_id, sql=sql, config=expected),
                     'EVALUATION_READ_JOB_MISMATCH')
            rows = list(islice(operation_iter(operation_call(job.result, max_results=2, page_size=2,
                retry=None, job_retry=None, timeout=self.settings.result_timeout_seconds)), 2))
            _require(job.state == 'DONE' and job.error_result is None
                     and matches_sharing_job(job, job_id=job_id, sql=sql, config=expected),
                     'EVALUATION_READ_JOB_MISMATCH')
            return decode_snapshot(request, [normalize(row) for row in rows])
        except OperationStopped:
            raise
        except EvaluationStoreError:
            raise
        except ContractError as error:
            raise EvaluationStoreError(error.code) from None
        except Exception:
            raise EvaluationStoreError('EVALUATION_READ_UNAVAILABLE') from None
