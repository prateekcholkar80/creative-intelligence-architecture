"""Bounded authoritative lifecycle captures and default-disabled native writes."""
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from itertools import islice
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.operation_budget import OperationStopped, operation_call, operation_iter
from src.application.prompt_evaluation_query import access_sql, columns
from src.application.prompt_evaluation_storage import TABLES as EVALUATION_TABLES
from src.application.prompt_evaluation_store import (
    EvaluationReadRequest, EvaluationStoreSettings, decode_snapshot as decode_evaluation_snapshot,
)
from src.application.prompt_lifecycle_contract import (
    TABLES, MAX_CONTROL_HISTORY, MAX_NATIVE_SLOTS, LifecycleReadRequest, LifecycleSnapshot, LifecycleWrite, decode_control,
)
from src.application.prompt_lifecycle_query import HISTORY_WINDOW, config, sql, validate_receipt
from src.application.prompt_registry_codec import MAX_PROMPT_HISTORY, decode_registry_state, normalize
from src.application.prompt_registry_schema import TABLES as REGISTRY_TABLES
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_jobs import failed_sharing_job_status, matches_sharing_job
from src.application.workflow_contract import ContractError, _require
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES, LOCATION
from src.ingestion.setup_prompt_lifecycle_tables import inspect
from src.ingestion.setup_workflow_tables import DATASET_ID, PROJECT, validate_table


class LifecycleStoreError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__('Prompt lifecycle storage unavailable.')


@dataclass(frozen=True, slots=True)
class LifecycleWriteOutcome:
    native_job_id: str
    native_status: SharingCommitStatus
    receipt: dict | None = None
    submitted: bool = False

    def __post_init__(self):
        _require(type(self.native_job_id) is str and bool(self.native_job_id)
                 and isinstance(self.native_status, SharingCommitStatus) and type(self.submitted) is bool
                 and (self.receipt is None or type(self.receipt) is dict
                     and self.native_status is SharingCommitStatus.COMMITTED), 'INVALID_LIFECYCLE_OUTCOME')


def read_sources():
    control, history = TABLES['prompt_lifecycle_controls'], TABLES['prompt_lifecycle_history']
    registry, registry_history = REGISTRY_TABLES['prompt_registry'], REGISTRY_TABLES['prompt_registry_history']
    sources = [
        ('controls', control, 'prompt_key=@prompt_key', 2, ''),
        ('control_history', history, 'prompt_key=@prompt_key', MAX_CONTROL_HISTORY+1, 'revision'),
        ('target_heads', registry, f"vertex_project='{PROJECT}' AND vertex_location='{LOCATION}' "
            'AND vertex_prompt_id=@vertex_prompt_id AND vertex_version_id=@vertex_version_id', 2, ''),
        ('target_history', registry_history, HISTORY_WINDOW + ' AND EXISTS (SELECT 1 FROM target_heads_snapshot t '
            'WHERE t.prompt_key=prompt_key AND t.snapshot_sha256=snapshot_sha256)', MAX_PROMPT_HISTORY+1, 'revision'),
        ('active_heads', registry, "prompt_key=@prompt_key AND status='ACTIVE'", 2, ''),
        ('active_history', registry_history, HISTORY_WINDOW + ' AND EXISTS (SELECT 1 FROM active_heads_snapshot t '
            'WHERE t.prompt_key=prompt_key AND t.snapshot_sha256=snapshot_sha256)', MAX_PROMPT_HISTORY+1, 'revision'),
    ]
    for name, spec in EVALUATION_TABLES.items():
        predicate = "@evaluation_id!='' AND workspace_id=@workspace_id AND " + (
            'prompt_key=@prompt_key' if 'control' in name else 'evaluation_id=@evaluation_id')
        bound = 65 if name.endswith('history') else 129 if name.endswith('cases') else 2
        order = 'revision' if name.endswith('history') else 'case_id' if name.endswith('cases') else ''
        sources.append((name, spec, predicate, bound, order))
    return sources


def read_sql():
    queries, result = [], []
    for name, spec, predicate, bound, order in read_sources():
        # Qualify history references: unqualified names inside the EXISTS would
        # otherwise bind to the inner target alias rather than the outer row.
        predicate = predicate.replace('t.prompt_key=prompt_key', 't.prompt_key=r.prompt_key') \
            .replace('t.snapshot_sha256=snapshot_sha256', 't.snapshot_sha256=r.snapshot_sha256')
        queries.append(f'''CREATE TEMP TABLE {name}_snapshot AS SELECT {columns(spec, 'r')}
FROM `{DATASET_ID}.{spec.name}` r WHERE {predicate};''')
        result.extend([f'(SELECT COUNT(*) FROM {name}_snapshot) AS {name}_count',
            f'ARRAY(SELECT AS STRUCT * FROM {name}_snapshot'
            + (f' ORDER BY {order}' if order else '') + f' LIMIT {bound}) AS {name}'])
    return f'''BEGIN TRANSACTION;
{access_sql()}
ASSERT CURRENT_TIMESTAMP() < @assertion_expires_at AS 'ASSERTION_EXPIRED';
{chr(10).join(queries)}
CREATE TEMP TABLE lifecycle_read_receipt AS SELECT CURRENT_TIMESTAMP() AS observed_at,
{', '.join(result)};
COMMIT TRANSACTION;
SELECT * FROM lifecycle_read_receipt;'''


def read_config(request):
    identity = request.identity
    values = dict(workspace_id=('STRING', request.workspace_id), prompt_key=('STRING', request.prompt_key),
        vertex_prompt_id=('STRING', request.vertex_prompt_id), vertex_version_id=('STRING', request.vertex_version_id),
        evaluation_id=('STRING', request.evaluation_id or ''), principal_id=('STRING', identity.principal.principal_id),
        issuer=('STRING', identity.issuer), subject=('STRING', identity.subject),
        membership_revision=('INT64', request.membership_revision), assertion_expires_at=('TIMESTAMP',
            datetime.fromtimestamp(identity.expires_at, timezone.utc)))
    return bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(k, kind, value)
        for k, (kind, value) in values.items()], use_legacy_sql=False, use_query_cache=False,
        create_session=False, maximum_bytes_billed=512*1024**2, job_timeout_ms=60000,
        script_options=bigquery.ScriptOptions(statement_timeout_ms=20000),
        labels={'workflow': 'prompt-lifecycle-read-v1'})


def decode_snapshot(request, rows):
    _require(type(rows) is list and len(rows)==1 and type(rows[0]) is dict, 'INVALID_LIFECYCLE_READ_RESULT')
    row = rows[0]
    sources = read_sources()
    names = [item[0] for item in sources]
    _require(set(row)=={'observed_at'} | set(names) | {name+'_count' for name in names},
             'INVALID_LIFECYCLE_READ_SHAPE')
    for name, _, _, bound, _ in sources:
        _require(type(row[name]) is list and type(row[name+'_count']) is int
                 and row[name+'_count']==len(row[name]) and len(row[name])<bound,
                 'INCOMPLETE_LIFECYCLE_READ')
    _require(len(row['controls'])<=1 and len(row['target_heads'])==1
             and len(row['active_heads'])<=1, 'LIFECYCLE_MISSING_OR_DUPLICATE_HEAD')
    current = None if not row['controls'] else decode_control(row['controls'][0])
    history = tuple(decode_control(item) for item in row['control_history'])
    target = decode_registry_state(row['target_heads'][0], row['target_history'])
    active = None
    if row['active_heads']:
        active = decode_registry_state(row['active_heads'][0], row['active_history'])
    else:
        _require(not row['active_history'], 'ORPHAN_LIFECYCLE_ACTIVE_HISTORY')
    evaluation, anchor = None, None
    if request.evaluation_id is None:
        _require(all(not row[name] for name in EVALUATION_TABLES), 'UNEXPECTED_LIFECYCLE_EVALUATION')
    else:
        ev_row = {name:row[name] for name in EVALUATION_TABLES}
        ev_row.update(prompt_registry=row['target_heads'], prompt_registry_history=row['target_history'])
        ev_row.update({name+'_count':len(values) for name, values in list(ev_row.items())})
        ev_row['observed_at'] = row['observed_at']
        evaluation = decode_evaluation_snapshot(EvaluationReadRequest(request.workspace_id,
            request.prompt_key, request.identity, request.membership_revision, request.evaluation_id), [ev_row])
        stored = row['prompt_evaluations'][0]
        anchor = next(item for item in evaluation.control_history if item.digest==stored['control_digest'])
    return LifecycleSnapshot(request, current, history, target, active, evaluation, row['observed_at'], anchor)


class BigQueryPromptLifecycleStore:
    def __init__(self, client, settings=None):
        if (client.project!=PROJECT or client.location!=LOCATION or client.default_query_job_config is not None):
            raise ValueError('Fixed-project/region client required')
        self.client = client
        self.settings = EvaluationStoreSettings() if settings is None else settings
        _require(isinstance(self.settings, EvaluationStoreSettings), 'INVALID_LIFECYCLE_STORE_SETTINGS')

    def _preflight(self):
        _require(inspect(self.client)['status']=='VERIFIED', 'LIFECYCLE_RESOURCES_MISSING')
        specs = tuple(EVALUATION_TABLES.values()) + tuple(REGISTRY_TABLES.values()) + tuple(
            WORKFLOW_TABLES[name] for name in ('workspaces', 'memberships'))
        for spec in specs:
            expected = spec.table(PROJECT)
            validate_table(operation_call(self.client.get_table, expected.reference, retry=None,
                timeout=self.settings.rpc_timeout_seconds), expected)

    def _enabled(self, plan):
        _require(self.settings.execution_enabled and isinstance(plan, LifecycleWrite), 'LIFECYCLE_WRITES_DISABLED')

    def _outcome(self, plan, job, *, submitted=False):
        unknown = LifecycleWriteOutcome(plan.job_id, SharingCommitStatus.UNKNOWN, submitted=submitted)
        script, expected = sql(plan), config(plan)
        if not matches_sharing_job(job, job_id=plan.job_id, sql=script, config=expected):
            return unknown
        try:
            rows = None
            if job.state!='DONE':
                rows = list(islice(operation_iter(operation_call(job.result, max_results=2, page_size=2,
                    retry=None, job_retry=None, timeout=self.settings.result_timeout_seconds)), 2))
            if job.state!='DONE' or not matches_sharing_job(job, job_id=plan.job_id, sql=script, config=expected):
                return unknown
            if job.error_result is not None:
                return LifecycleWriteOutcome(plan.job_id, failed_sharing_job_status(self.client,
                    job_id=plan.job_id, timeout_seconds=self.settings.rpc_timeout_seconds, parent=job), submitted=submitted)
            if rows is None:
                rows = list(islice(operation_iter(operation_call(job.result, max_results=2, page_size=2,
                    retry=None, job_retry=None, timeout=self.settings.result_timeout_seconds)), 2))
            receipt = validate_receipt(plan, [normalize(r) for r in rows])
            if not matches_sharing_job(job, job_id=plan.job_id, sql=script, config=expected):
                return unknown
            return LifecycleWriteOutcome(plan.job_id, SharingCommitStatus.COMMITTED, receipt, submitted)
        except OperationStopped:
            raise
        except Exception:
            return unknown

    def recover(self, plan):
        self._enabled(plan)
        try:
            job = operation_call(self.client.get_job, plan.job_id, project=PROJECT, location=LOCATION,
                retry=None, timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, job)
        except OperationStopped:
            raise
        except Exception:
            return LifecycleWriteOutcome(plan.job_id, SharingCommitStatus.UNKNOWN)

    def _rolled_back_ancestors(self, ancestors):
        for ancestor in ancestors:
            job = operation_call(self.client.get_job, ancestor.job_id, project=PROJECT, location=LOCATION,
                retry=None, timeout=self.settings.rpc_timeout_seconds)
            _require(matches_sharing_job(job, job_id=ancestor.job_id, sql=sql(ancestor), config=config(ancestor))
                     and job.state=='DONE' and job.error_result is not None,
                     'LIFECYCLE_RETRY_NONCOMMIT_UNVERIFIED')
            status = failed_sharing_job_status(self.client, job_id=ancestor.job_id, parent=job,
                timeout_seconds=self.settings.rpc_timeout_seconds)
            _require(status is SharingCommitStatus.NOT_COMMITTED
                     and matches_sharing_job(job, job_id=ancestor.job_id, sql=sql(ancestor), config=config(ancestor)),
                     'LIFECYCLE_RETRY_NONCOMMIT_UNVERIFIED')

    def issue_retry(self, original):
        self._enabled(original)
        _require(original.native_slot<MAX_NATIVE_SLOTS, 'LIFECYCLE_NATIVE_SLOTS_EXHAUSTED')
        ancestors = original.ancestors+(original,)
        self._rolled_back_ancestors(ancestors)
        return replace(original,native_slot=original.native_slot+1,ancestors=ancestors)

    def submit(self, plan):
        self._enabled(plan)
        try:
            self._preflight()
        except OperationStopped:
            raise
        except Exception:
            raise LifecycleStoreError('LIFECYCLE_SCHEMA_PREFLIGHT_FAILED') from None
        try:
            job = operation_call(self.client.get_job, plan.job_id, project=PROJECT, location=LOCATION,
                retry=None, timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, job)
        except NotFound:
            pass
        except OperationStopped:
            raise
        except Exception:
            return LifecycleWriteOutcome(plan.job_id, SharingCommitStatus.UNKNOWN)
        if plan.ancestors:
            # A supplied retry object never proves rollback. Reverify every exact
            # predecessor immediately before a previously absent native insert.
            try:
                self._rolled_back_ancestors(plan.ancestors)
            except OperationStopped:
                raise
            except Exception:
                return LifecycleWriteOutcome(plan.job_id, SharingCommitStatus.UNKNOWN)
        try:
            job = operation_call(self.client.query, sql(plan), job_config=config(plan), job_id=plan.job_id,
                project=PROJECT, location=LOCATION, retry=None, job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, job, submitted=True)
        except OperationStopped:
            raise
        except Exception:
            outcome = self.recover(plan)
            return LifecycleWriteOutcome(outcome.native_job_id, outcome.native_status, outcome.receipt, True)

    def read(self, request):
        _require(isinstance(request, LifecycleReadRequest), 'INVALID_LIFECYCLE_READ_REQUEST')
        try:
            self._preflight()
            script, cfg = read_sql(), read_config(request)
            expected = bigquery.QueryJobConfig.from_api_repr(deepcopy(cfg.to_api_repr()))
            job_id = 'prompt_lifecycle_read_' + uuid4().hex
            job = operation_call(self.client.query, script, job_config=cfg, job_id=job_id, project=PROJECT,
                location=LOCATION, retry=None, job_retry=None, api_method=bigquery.enums.QueryApiMethod.INSERT,
                timeout=self.settings.rpc_timeout_seconds)
            _require(matches_sharing_job(job, job_id=job_id, sql=script, config=expected), 'LIFECYCLE_READ_JOB_MISMATCH')
            rows = list(islice(operation_iter(operation_call(job.result, max_results=2, page_size=2,
                retry=None, job_retry=None, timeout=self.settings.result_timeout_seconds)), 2))
            _require(job.state=='DONE' and job.error_result is None and matches_sharing_job(
                job, job_id=job_id, sql=script, config=expected), 'LIFECYCLE_READ_JOB_MISMATCH')
            return decode_snapshot(request, [normalize(r) for r in rows])
        except OperationStopped:
            raise
        except ContractError as error:
            raise LifecycleStoreError(error.code) from None
        except Exception:
            raise LifecycleStoreError('LIFECYCLE_READ_UNAVAILABLE') from None
