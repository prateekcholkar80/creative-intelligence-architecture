"""One fresh CREATOR/owner BigQuery snapshot for runtime prompt eligibility."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import islice
from uuid import uuid4

from google.cloud import bigquery

from src.application.identity import VerifiedIdentity
from src.application.operation_budget import OperationStopped, operation_call, operation_iter
from src.application.prompt_contract import PromptTask
from src.application.prompt_evaluation_query import access_sql, columns
from src.application.prompt_evaluation_storage import TABLES as EVALUATION_TABLES
from src.application.prompt_evaluation_store import EvaluationStoreSettings
from src.application.prompt_lifecycle_contract import LifecycleReadRequest
from src.application.prompt_lifecycle_store import (
    BigQueryPromptLifecycleStore, read_sources as lifecycle_sources,
    decode_snapshot as decode_lifecycle_snapshot,
)
from src.application.prompt_registry_codec import decode_registry_state, normalize
from src.application.prompt_runtime_contract import TABLES as BINDING_TABLES, validate_runtime_eligibility
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import ContractError, _id, _require
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES, LOCATION, SCHEMA_VERSION
from src.ingestion.setup_workflow_tables import DATASET_ID, PROJECT, validate_table


@dataclass(frozen=True, slots=True)
class RuntimeReadRequest:
    workspace_id: str
    conversation_id: str
    prompt_key: str
    task: PromptTask
    identity: VerifiedIdentity
    membership_revision: int

    def __post_init__(self):
        _id(self.conversation_id)
        LifecycleReadRequest(self.workspace_id,self.prompt_key,self.task,'1','1',
                             self.identity,self.membership_revision)


def read_sources():
    sources = []
    for name,spec,predicate,bound,order in lifecycle_sources():
        if name in {'target_heads','active_heads'}:
            predicate = "prompt_key=@prompt_key AND status='ACTIVE'"
        elif name in EVALUATION_TABLES:
            predicate = 'workspace_id=@workspace_id AND ' + (
                'prompt_key=@prompt_key' if 'control' in name else
                'evaluation_id=(SELECT evaluation_id FROM target_heads_snapshot)')
        predicate = predicate.replace('t.prompt_key=prompt_key','t.prompt_key=r.prompt_key') \
            .replace('t.snapshot_sha256=snapshot_sha256','t.snapshot_sha256=r.snapshot_sha256')
        sources.append((name,spec,predicate,bound,order))
    return sources


def runtime_access_sql():
    # MANAGE_MODELS remains ADMIN-only; generating with an approved template uses
    # the existing CREATOR role and current parent-conversation ownership.
    workspace_member = access_sql().replace("'ADMIN' IN UNNEST(roles)","'CREATOR' IN UNNEST(roles)")
    return workspace_member + f'''
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.conversations`
WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id)=1 AS 'CONVERSATION_CONFLICT';
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.conversations`
WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
AND schema_version='{SCHEMA_VERSION}' AND status='ACTIVE' AND owner_id=@principal_id)=1 AS 'CONVERSATION_DENIED';
ASSERT CURRENT_TIMESTAMP()<@assertion_expires_at AS 'ASSERTION_EXPIRED';'''


def read_sql():
    queries,fields = [],[]
    for name,spec,predicate,bound,order in read_sources():
        queries.append(f'CREATE TEMP TABLE {name}_snapshot AS SELECT {columns(spec,"r")} '
            f'FROM `{DATASET_ID}.{spec.name}` r WHERE {predicate};')
        fields.extend([f'(SELECT COUNT(*) FROM {name}_snapshot) AS {name}_count',
            f'ARRAY(SELECT AS STRUCT * FROM {name}_snapshot'
            + (f' ORDER BY {order}' if order else '') + f' LIMIT {bound}) AS {name}'])
    return f'''BEGIN TRANSACTION;
{runtime_access_sql()}
{chr(10).join(queries)}
CREATE TEMP TABLE prompt_runtime_receipt AS SELECT CURRENT_TIMESTAMP() AS observed_at,
{', '.join(fields)};
COMMIT TRANSACTION;
SELECT * FROM prompt_runtime_receipt;'''


def read_config(request):
    identity = request.identity
    values = dict(workspace_id=('STRING',request.workspace_id),conversation_id=('STRING',request.conversation_id),
        prompt_key=('STRING',request.prompt_key),principal_id=('STRING',identity.principal.principal_id),
        issuer=('STRING',identity.issuer),subject=('STRING',identity.subject),
        membership_revision=('INT64',request.membership_revision), assertion_expires_at=('TIMESTAMP',
            datetime.fromtimestamp(identity.expires_at,timezone.utc)))
    return bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(k,kind,value)
        for k,(kind,value) in values.items()],use_legacy_sql=False,use_query_cache=False,
        create_session=False,maximum_bytes_billed=512*1024**2,job_timeout_ms=60000,
        script_options=bigquery.ScriptOptions(statement_timeout_ms=20000),
        labels={'workflow':'prompt-runtime-read-v1'})


def decode_snapshot(request, rows):
    _require(type(request) is RuntimeReadRequest and type(rows) is list and len(rows)==1,
             'INVALID_RUNTIME_READ_RESULT')
    row = rows[0]
    _require(type(row) is dict and type(row.get('target_heads')) is list
             and len(row['target_heads'])==1, 'RUNTIME_ACTIVE_HEAD_NOT_UNIQUE')
    state = decode_registry_state(row['target_heads'][0],row['target_history'])
    head = state.head
    _require(head.snapshot.prompt_key==request.prompt_key and head.snapshot.task is request.task
             and head.evaluation_id is not None, 'RUNTIME_ACTIVE_SCOPE_MISMATCH')
    capture = decode_lifecycle_snapshot(LifecycleReadRequest(request.workspace_id,request.prompt_key,
        request.task,head.snapshot.vertex_prompt_id,head.snapshot.vertex_version_id,
        request.identity,request.membership_revision,head.evaluation_id),rows)
    validate_runtime_eligibility(capture,checked_at=capture.observed_at)
    return capture


class RuntimeReadError(RuntimeError):
    def __init__(self,code):
        self.code = code
        super().__init__('Approved prompt is unavailable.')


class BigQueryPromptRuntimeReader:
    def __init__(self,client,settings=EvaluationStoreSettings()):
        self._base = BigQueryPromptLifecycleStore(client,settings)
        self.client,self.settings = client,settings

    def preflight(self):
        self._base._preflight()
        expected = WORKFLOW_TABLES['conversations'].table(PROJECT)
        validate_table(operation_call(self.client.get_table,expected.reference,retry=None,
            timeout=self.settings.rpc_timeout_seconds),expected)

    def read_binding(self,request,invocation_id):
        """Current access plus exact append-only invocation metadata; no local copy."""
        try:
            return self._read_binding(request,invocation_id)
        except OperationStopped:
            raise
        except ContractError as error:
            raise RuntimeReadError(error.code) from None
        except Exception:
            raise RuntimeReadError('RUNTIME_BINDING_READ_UNAVAILABLE') from None

    def _read_binding(self,request,invocation_id):
        _require(type(request) is RuntimeReadRequest,'INVALID_RUNTIME_READ_REQUEST')
        _id(invocation_id)
        self.preflight()
        spec=BINDING_TABLES['invocation_prompt_bindings']
        expected=spec.table(PROJECT)
        validate_table(operation_call(self.client.get_table,expected.reference,retry=None,
            timeout=self.settings.rpc_timeout_seconds),expected)
        script=f'''BEGIN TRANSACTION;
{runtime_access_sql()}
CREATE TEMP TABLE binding_read AS SELECT CURRENT_TIMESTAMP() AS observed_at,
 (SELECT COUNT(*) FROM `{DATASET_ID}.{spec.name}` WHERE workspace_id=@workspace_id
  AND invocation_id=@runtime_invocation_id) AS binding_count,
 ARRAY(SELECT AS STRUCT {columns(spec)} FROM `{DATASET_ID}.{spec.name}`
  WHERE workspace_id=@workspace_id AND invocation_id=@runtime_invocation_id LIMIT 2) AS bindings;
COMMIT TRANSACTION;
SELECT * FROM binding_read;'''
        cfg=read_config(request)
        # prompt_key is not needed for this access-only query.
        cfg.query_parameters=[p for p in cfg.query_parameters if p.name!='prompt_key']+[
            bigquery.ScalarQueryParameter('runtime_invocation_id','STRING',invocation_id)]
        cfg.labels={'workflow':'prompt-binding-read-v1'}
        job_id='prompt_binding_read_'+uuid4().hex
        config_pin=bigquery.QueryJobConfig.from_api_repr(deepcopy(cfg.to_api_repr()))
        job=operation_call(self.client.query,script,job_config=cfg,job_id=job_id,project=PROJECT,
            location=LOCATION,retry=None,job_retry=None,api_method=bigquery.enums.QueryApiMethod.INSERT,
            timeout=self.settings.rpc_timeout_seconds)
        _require(matches_sharing_job(job,job_id=job_id,sql=script,config=config_pin),'RUNTIME_BINDING_JOB_MISMATCH')
        rows=[normalize(r) for r in islice(operation_iter(operation_call(job.result,max_results=2,
            page_size=2,retry=None,job_retry=None,timeout=self.settings.result_timeout_seconds)),2)]
        _require(job.state=='DONE' and job.error_result is None and matches_sharing_job(job,
            job_id=job_id,sql=script,config=config_pin) and len(rows)==1
            and set(rows[0])=={'observed_at','binding_count','bindings'}
            and type(rows[0]['binding_count']) is int and rows[0]['binding_count']==1
            and type(rows[0]['bindings']) is list and len(rows[0]['bindings'])==1,
            'RUNTIME_BINDING_READ_INVALID')
        return rows[0]

    def read(self,request):
        _require(type(request) is RuntimeReadRequest,'INVALID_RUNTIME_READ_REQUEST')
        try:
            self.preflight()
            script,cfg = read_sql(),read_config(request)
            expected = bigquery.QueryJobConfig.from_api_repr(deepcopy(cfg.to_api_repr()))
            job_id = 'prompt_runtime_read_'+uuid4().hex
            job = operation_call(self.client.query,script,job_config=cfg,job_id=job_id,
                project=PROJECT,location=LOCATION,retry=None,job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT,timeout=self.settings.rpc_timeout_seconds)
            _require(matches_sharing_job(job,job_id=job_id,sql=script,config=expected),'RUNTIME_READ_JOB_MISMATCH')
            rows = list(islice(operation_iter(operation_call(job.result,max_results=2,page_size=2,
                retry=None,job_retry=None,timeout=self.settings.result_timeout_seconds)),2))
            _require(job.state=='DONE' and job.error_result is None and matches_sharing_job(
                job,job_id=job_id,sql=script,config=expected),'RUNTIME_READ_JOB_MISMATCH')
            return decode_snapshot(request,[normalize(r) for r in rows])
        except OperationStopped:
            raise
        except ContractError as error:
            raise RuntimeReadError(error.code) from None
        except Exception:
            raise RuntimeReadError('RUNTIME_READ_UNAVAILABLE') from None
