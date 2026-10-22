"""Disabled-by-default single-submit executor for initial prompt registration."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from itertools import islice

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.operation_budget import OperationStopped, operation_call, operation_iter
from src.application.prompt_registry_query import (
    PromptRegistrationQuery,
    PromptRegistrationResult,
    validate_prompt_registration_result,
)
from src.application.prompt_registry_schema import TABLES
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_jobs import failed_sharing_job_status, matches_sharing_job
from src.application.workflow_contract import _require
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_workflow_tables import (
    DATASET_ID,
    PROJECT,
    validate_dataset,
    validate_table,
)


class PromptRegistryExecutorError(RuntimeError):
    """Safe failure raised before a prompt-registration job can run."""

    def __init__(self, code: str):
        self.code = code
        super().__init__('Prompt registration is unavailable.')


@dataclass(frozen=True, slots=True)
class PromptRegistryExecutorSettings:
    execution_enabled: bool = False
    rpc_timeout_seconds: int = 10
    result_timeout_seconds: int = 30

    def __post_init__(self):
        _require(type(self.execution_enabled) is bool
                 and type(self.rpc_timeout_seconds) is int
                 and type(self.result_timeout_seconds) is int
                 and 1 <= self.rpc_timeout_seconds <= 20
                 and 1 <= self.result_timeout_seconds <= 60,
                 'INVALID_PROMPT_REGISTRY_EXECUTOR_SETTINGS')


@dataclass(frozen=True, slots=True)
class PromptRegistrationOutcome:
    native_job_id: str
    native_status: SharingCommitStatus
    result: PromptRegistrationResult | None = None
    submitted: bool = False

    def __post_init__(self):
        _require(type(self.native_job_id) is str and bool(self.native_job_id)
                 and isinstance(self.native_status, SharingCommitStatus)
                 and (self.result is None
                      or isinstance(self.result, PromptRegistrationResult))
                 and type(self.submitted) is bool
                 and (self.native_status is SharingCommitStatus.COMMITTED
                      or self.result is None),
                 'INVALID_PROMPT_REGISTRATION_OUTCOME')


def _normalize(value, depth: int = 0):
    """Bound native row material before passing it to the pure validator."""
    _require(depth <= 3, 'INVALID_PROMPT_REGISTRATION_NATIVE_RESULT')
    if isinstance(value, (Mapping, bigquery.Row)):
        _require(len(value) <= 8,
                 'INVALID_PROMPT_REGISTRATION_NATIVE_RESULT')
        return {key: _normalize(value[key], depth + 1) for key in value.keys()}
    if isinstance(value, (list, tuple)):
        _require(len(value) <= 2,
                 'INVALID_PROMPT_REGISTRATION_NATIVE_RESULT')
        return [_normalize(item, depth + 1) for item in value]
    return value


class PromptRegistryExecutor:
    """Executes only prebuilt DRAFT registration plans; never reads Vertex."""

    def __init__(self, client: bigquery.Client,
                 settings: PromptRegistryExecutorSettings | None = None):
        if (getattr(client, 'project', None) != PROJECT
                or getattr(client, 'location', None) != LOCATION
                or getattr(client, 'default_query_job_config', None) is not None):
            raise ValueError('Fixed-project/region ADC prompt-registry client required')
        self.client = client
        self.settings = (PromptRegistryExecutorSettings()
                         if settings is None else settings)
        _require(isinstance(self.settings, PromptRegistryExecutorSettings),
                 'INVALID_PROMPT_REGISTRY_EXECUTOR_SETTINGS')

    def _enabled(self, plan: PromptRegistrationQuery) -> None:
        if not self.settings.execution_enabled:
            raise PromptRegistryExecutorError('PROMPT_REGISTRY_WRITES_DISABLED')
        if not isinstance(plan, PromptRegistrationQuery):
            raise PromptRegistryExecutorError('INVALID_PROMPT_REGISTRATION_PLAN')

    def _preflight(self) -> None:
        try:
            dataset = operation_call(
                self.client.get_dataset, DATASET_ID, retry=None,
                timeout=self.settings.rpc_timeout_seconds)
            validate_dataset(dataset)
            for spec in TABLES.values():
                expected = spec.table(PROJECT)
                actual = operation_call(
                    self.client.get_table, expected.reference, retry=None,
                    timeout=self.settings.rpc_timeout_seconds)
                validate_table(actual, expected)
        except OperationStopped:
            raise
        except Exception:
            raise PromptRegistryExecutorError(
                'PROMPT_REGISTRY_STORE_UNAVAILABLE') from None

    def _outcome(self, plan: PromptRegistrationQuery, job, *,
                 submitted: bool = False) -> PromptRegistrationOutcome:
        job_id = plan.job_id
        config = plan.job_config()
        if not matches_sharing_job(job, job_id=job_id, sql=plan.sql,
                                   config=config):
            return PromptRegistrationOutcome(
                job_id, SharingCommitStatus.UNKNOWN, submitted=submitted)

        rows = None
        if job.state != 'DONE':
            try:
                result = operation_call(
                    job.result, max_results=2, page_size=2, retry=None,
                    job_retry=None, timeout=self.settings.result_timeout_seconds)
                rows = list(islice(operation_iter(result), 2))
            except OperationStopped:
                raise
            except Exception:
                return PromptRegistrationOutcome(
                    job_id, SharingCommitStatus.UNKNOWN, submitted=submitted)

        if (job.state != 'DONE'
                or not matches_sharing_job(job, job_id=job_id, sql=plan.sql,
                                           config=config)):
            return PromptRegistrationOutcome(
                job_id, SharingCommitStatus.UNKNOWN, submitted=submitted)

        if job.error_result is not None:
            status = failed_sharing_job_status(
                self.client, job_id=job_id,
                timeout_seconds=self.settings.rpc_timeout_seconds, parent=job)
            return PromptRegistrationOutcome(job_id, status,
                                             submitted=submitted)

        try:
            if rows is None:
                result = operation_call(
                    job.result, max_results=2, page_size=2, retry=None,
                    job_retry=None, timeout=self.settings.result_timeout_seconds)
                rows = list(islice(operation_iter(result), 2))
            if (len(rows) != 1
                    or not matches_sharing_job(
                        job, job_id=job_id, sql=plan.sql, config=config)):
                return PromptRegistrationOutcome(
                    job_id, SharingCommitStatus.UNKNOWN, submitted=submitted)
            receipt = validate_prompt_registration_result(
                plan, [_normalize(rows[0])])
            return PromptRegistrationOutcome(
                job_id, SharingCommitStatus.COMMITTED, receipt, submitted)
        except OperationStopped:
            raise
        except Exception:
            return PromptRegistrationOutcome(
                job_id, SharingCommitStatus.UNKNOWN, submitted=submitted)

    def _recover(self, plan: PromptRegistrationQuery, *,
                 submitted: bool = False) -> PromptRegistrationOutcome:
        try:
            job = operation_call(
                self.client.get_job, plan.job_id, project=PROJECT,
                location=LOCATION, retry=None,
                timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, job, submitted=submitted)
        except OperationStopped:
            raise
        except Exception:
            return PromptRegistrationOutcome(
                plan.job_id, SharingCommitStatus.UNKNOWN, submitted=submitted)

    def recover(self, plan: PromptRegistrationQuery) -> PromptRegistrationOutcome:
        """Read one exact native job; never submit a replacement."""
        self._enabled(plan)
        return self._recover(plan)

    def submit(self, plan: PromptRegistrationQuery) -> PromptRegistrationOutcome:
        """Insert at most one job after an authoritative exact-job NotFound."""
        self._enabled(plan)
        self._preflight()
        try:
            existing = operation_call(
                self.client.get_job, plan.job_id, project=PROJECT,
                location=LOCATION, retry=None,
                timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, existing)
        except NotFound:
            pass
        except OperationStopped:
            raise
        except Exception:
            return PromptRegistrationOutcome(
                plan.job_id, SharingCommitStatus.UNKNOWN)

        try:
            job = operation_call(
                self.client.query, plan.sql, job_config=plan.job_config(),
                job_id=plan.job_id, project=PROJECT, location=LOCATION,
                retry=None, job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT,
                timeout=self.settings.rpc_timeout_seconds)
            return self._outcome(plan, job, submitted=True)
        except OperationStopped:
            raise
        except Exception:
            return self._recover(plan, submitted=True)
