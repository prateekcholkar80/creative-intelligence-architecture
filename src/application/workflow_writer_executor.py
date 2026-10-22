"""Disabled-by-default single-submit executor for typed workflow write plans."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from itertools import islice

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.operation_budget import OperationStopped,operation_call,operation_iter
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_jobs import failed_sharing_job_status,matches_sharing_job
from src.application.workflow_attempt_query import WorkflowAttemptPlan,validate_workflow_attempt_result
from src.application.workflow_contract import ContractError,_require
from src.application.workflow_evidence_query import WorkflowEvidencePlan,validate_workflow_evidence_result
from src.application.workflow_failure_query import WorkflowFailurePlan,validate_workflow_failure_result
from src.application.workflow_invocation_query import WorkflowInvocationPlan,validate_workflow_invocation_result
from src.application.workflow_invocation_result_query import (
    WorkflowInvocationResultPlan,validate_workflow_invocation_result_write,
)
from src.application.workflow_output_query import WorkflowOutputPlan,validate_workflow_output_result
from src.application.workflow_schema import LOCATION,TABLES
from src.application.workflow_start_query import WorkflowStartPlan,validate_workflow_start_result
from src.ingestion.setup_workflow_tables import DATASET_ID,PROJECT,validate_dataset,validate_table

WorkflowWritePlan=(WorkflowStartPlan|WorkflowAttemptPlan|WorkflowEvidencePlan|WorkflowInvocationPlan
    |WorkflowInvocationResultPlan|WorkflowOutputPlan|WorkflowFailurePlan)
_PLAN_TYPES=(WorkflowStartPlan,WorkflowAttemptPlan,WorkflowEvidencePlan,WorkflowInvocationPlan,
    WorkflowInvocationResultPlan,WorkflowOutputPlan,WorkflowFailurePlan)


class WorkflowWriterError(RuntimeError):
    def __init__(self,code: str):
        self.code=code
        super().__init__('Workflow persistence is unavailable.')


@dataclass(frozen=True,slots=True)
class WorkflowWriterSettings:
    execution_enabled: bool=False
    rpc_timeout_seconds: int=10
    result_timeout_seconds: int=30

    def __post_init__(self):
        _require(type(self.execution_enabled) is bool and type(self.rpc_timeout_seconds) is int
            and type(self.result_timeout_seconds) is int and 1<=self.rpc_timeout_seconds<=20
            and 1<=self.result_timeout_seconds<=60,'INVALID_WORKFLOW_WRITER_SETTINGS')


@dataclass(frozen=True,slots=True)
class WorkflowWriteOutcome:
    native_job_id: str
    native_status: SharingCommitStatus
    result_state: str|None=None
    submitted: bool=False

    def __post_init__(self):
        _require(isinstance(self.native_status,SharingCommitStatus)
            and type(self.submitted) is bool and (self.result_state is None
            or self.result_state in {'APPLIED','REUSED'})
            and (self.native_status is SharingCommitStatus.COMMITTED or self.result_state is None),
            'INVALID_WORKFLOW_WRITE_OUTCOME')


def _normalize(value,depth: int=0):
    _require(depth<=4,'INVALID_WORKFLOW_WRITE_RESULT')
    if isinstance(value,(Mapping,bigquery.Row)):
        _require(len(value)<=32,'INVALID_WORKFLOW_WRITE_RESULT')
        return {key:_normalize(value[key],depth+1) for key in value.keys()}
    if isinstance(value,(list,tuple)):
        _require(len(value)<=16,'INVALID_WORKFLOW_WRITE_RESULT')
        return [_normalize(item,depth+1) for item in value]
    return value


def _job_id(plan: WorkflowWritePlan) -> str:
    return plan.job_id if isinstance(plan,WorkflowStartPlan) else plan.native_job_id


def _validate_result(plan: WorkflowWritePlan,row: dict) -> str:
    if isinstance(plan,WorkflowStartPlan):return validate_workflow_start_result(plan,row)
    if isinstance(plan,WorkflowAttemptPlan):return validate_workflow_attempt_result(plan,row)
    if isinstance(plan,WorkflowEvidencePlan):return validate_workflow_evidence_result(plan,row)
    if isinstance(plan,WorkflowInvocationPlan):return validate_workflow_invocation_result(plan,row)
    if isinstance(plan,WorkflowInvocationResultPlan):return validate_workflow_invocation_result_write(plan,row)
    if isinstance(plan,WorkflowOutputPlan):return validate_workflow_output_result(plan,row)
    if isinstance(plan,WorkflowFailurePlan):return validate_workflow_failure_result(plan,row)
    raise ContractError('INVALID_WORKFLOW_WRITE_PLAN')


class WorkflowWriterExecutor:
    """Runs only prebuilt plans; it never creates plans, calls models or accesses GCS."""
    def __init__(self,client: bigquery.Client,settings: WorkflowWriterSettings|None=None):
        if (getattr(client,'project',None)!=PROJECT or getattr(client,'location',None)!=LOCATION
                or getattr(client,'default_query_job_config',None) is not None):
            raise ValueError('Fixed-project/region ADC workflow client required')
        self.client=client
        self.settings=WorkflowWriterSettings() if settings is None else settings
        _require(isinstance(self.settings,WorkflowWriterSettings),'INVALID_WORKFLOW_WRITER_SETTINGS')

    def _enabled(self,plan: WorkflowWritePlan) -> None:
        if not self.settings.execution_enabled:raise WorkflowWriterError('WORKFLOW_WRITES_DISABLED')
        if not isinstance(plan,_PLAN_TYPES):raise WorkflowWriterError('INVALID_WORKFLOW_WRITE_PLAN')

    def _preflight(self) -> None:
        try:
            validate_dataset(operation_call(self.client.get_dataset,DATASET_ID,retry=None,
                timeout=self.settings.rpc_timeout_seconds))
            for spec in TABLES.values():
                expected=spec.table(PROJECT)
                actual=operation_call(self.client.get_table,expected.reference,retry=None,
                    timeout=self.settings.rpc_timeout_seconds)
                validate_table(actual,expected)
        except OperationStopped:raise
        except Exception:raise WorkflowWriterError('WORKFLOW_STORE_UNAVAILABLE') from None

    def _outcome(self,plan: WorkflowWritePlan,job,*,submitted: bool=False) -> WorkflowWriteOutcome:
        job_id=_job_id(plan);config=plan.config()
        if not matches_sharing_job(job,job_id=job_id,sql=plan.sql,config=config):
            return WorkflowWriteOutcome(job_id,SharingCommitStatus.UNKNOWN,submitted=submitted)
        rows=None
        if job.state!='DONE':
            try:
                result=operation_call(job.result,max_results=2,page_size=2,retry=None,job_retry=None,
                    timeout=self.settings.result_timeout_seconds)
                rows=list(islice(operation_iter(result),2))
            except OperationStopped:raise
            except Exception:return WorkflowWriteOutcome(job_id,SharingCommitStatus.UNKNOWN,submitted=submitted)
        if (job.state!='DONE'
                or not matches_sharing_job(job,job_id=job_id,sql=plan.sql,config=config)):
            return WorkflowWriteOutcome(job_id,SharingCommitStatus.UNKNOWN,submitted=submitted)
        if job.error_result is not None:
            status=failed_sharing_job_status(self.client,job_id=job_id,
                timeout_seconds=self.settings.rpc_timeout_seconds,parent=job)
            return WorkflowWriteOutcome(job_id,status,submitted=submitted)
        try:
            if rows is None:
                result=operation_call(job.result,max_results=2,page_size=2,retry=None,job_retry=None,
                    timeout=self.settings.result_timeout_seconds)
                rows=list(islice(operation_iter(result),2))
            if len(rows)!=1 or not matches_sharing_job(job,job_id=job_id,sql=plan.sql,config=config):
                return WorkflowWriteOutcome(job_id,SharingCommitStatus.UNKNOWN,submitted=submitted)
            state=_validate_result(plan,_normalize(rows[0]))
            return WorkflowWriteOutcome(job_id,SharingCommitStatus.COMMITTED,state,submitted)
        except OperationStopped:raise
        except Exception:return WorkflowWriteOutcome(job_id,SharingCommitStatus.UNKNOWN,submitted=submitted)

    def _recover(self,plan: WorkflowWritePlan,*,submitted: bool=False) -> WorkflowWriteOutcome:
        job_id=_job_id(plan)
        try:
            job=operation_call(self.client.get_job,job_id,project=PROJECT,location=LOCATION,
                retry=None,timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan,job,submitted=submitted)
        except OperationStopped:raise
        except Exception:return WorkflowWriteOutcome(job_id,SharingCommitStatus.UNKNOWN,submitted=submitted)

    def recover(self,plan: WorkflowWritePlan) -> WorkflowWriteOutcome:
        self._enabled(plan)
        return self._recover(plan)

    def submit(self,plan: WorkflowWritePlan) -> WorkflowWriteOutcome:
        """At most one jobs.insert after an authoritative NotFound observation."""
        self._enabled(plan);self._preflight();job_id=_job_id(plan)
        try:
            existing=operation_call(self.client.get_job,job_id,project=PROJECT,location=LOCATION,
                retry=None,timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan,existing)
        except NotFound:
            pass
        except OperationStopped:raise
        except Exception:return WorkflowWriteOutcome(job_id,SharingCommitStatus.UNKNOWN)
        try:
            job=operation_call(self.client.query,plan.sql,job_config=plan.config(),job_id=job_id,
                project=PROJECT,location=LOCATION,retry=None,job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT,timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan,job,submitted=True)
        except OperationStopped:raise
        except Exception:
            return self._recover(plan,submitted=True)
