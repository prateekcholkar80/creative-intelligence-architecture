"""Server-only sharing submission, controlled retries and job/receipt recovery.

Execution is disabled by default; deployed recovery and endpoint gates remain.
No HTTP/UI wiring, automatic retry, bootstrap, model call or local DB fallback.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from enum import Enum
from itertools import islice
from uuid import uuid4

from google.cloud import bigquery

from src.application.access_policy import Action, Visibility
from src.application.access_reader import BigQueryAccessReader
from src.application.identity import IapIdentityAdapter, VerifiedIdentity, ensure_fresh
from src.application.operation_budget import OperationStopped, operation_call, operation_iter
from src.application.sharing_coordinator_contract import (
    SharingCommitObservation, SharingCommitStatus, SharingPreparationState,
    _within_window, make_sharing_command, prepare_sharing,
)
from src.application.sharing_intent import SharingAttempt, SharingIntentSealer, _now, initial_attempt
from src.application.sharing_query import SharingQuery, SharingQueryResult, SharingResultState, validate_sharing_result
from src.application.sharing_jobs import matches_sharing_job, failed_sharing_job_status
from src.application.sharing_receipt import SharingReceiptQuery, validate_receipt_result
from src.application.sharing_retry import SharingRetryAttempt, SharingRetrySealer, prepare_retry
from src.application.sharing_runtime import RuntimeSharingSealer
from src.application.workflow_contract import _require
from src.application.workflow_schema import LOCATION, TABLES
from src.ingestion.setup_workflow_tables import (
    DATASET_ID, PROJECT, validate_dataset, validate_table,
)

_CONTROL_TABLES = ('workspaces', 'memberships', 'conversations', 'audit_events')
SharingNativeAttempt = SharingAttempt | SharingRetryAttempt


class SharingExecutorError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__('Sharing execution is unavailable.')


@dataclass(frozen=True, slots=True)
class SharingExecutorSettings:
    execution_enabled: bool = False
    rpc_timeout_seconds: int = 5
    result_timeout_seconds: int = 20

    def __post_init__(self):
        _require(type(self.execution_enabled) is bool and type(self.rpc_timeout_seconds) is int
                 and type(self.result_timeout_seconds) is int
                 and 1 <= self.rpc_timeout_seconds <= 20 and 1 <= self.result_timeout_seconds <= 60,
                 'INVALID_SHARING_EXECUTOR_SETTINGS')


@dataclass(frozen=True, slots=True)
class SharingExecutionOutcome:
    observation: SharingCommitObservation
    result: SharingQueryResult | None = None


class SharingRetryIssuanceState(str, Enum):
    READY = 'READY'
    REUSED = 'REUSED'
    RECOVERY_REQUIRED = 'RECOVERY_REQUIRED'


@dataclass(frozen=True, slots=True)
class SharingRetryIssuance:
    """Minimal issuance result; receipt reuse does not classify a native job."""
    state: SharingRetryIssuanceState
    token: str | None = None
    result: SharingQueryResult | None = None

    def __post_init__(self):
        _require(isinstance(self.state, SharingRetryIssuanceState), 'INVALID_SHARING_RETRY_ISSUANCE')
        if self.state is SharingRetryIssuanceState.READY:
            _require(isinstance(self.token, str) and self.token.startswith('share-retry1.')
                     and len(self.token) <= 8192 and self.result is None, 'INVALID_SHARING_RETRY_ISSUANCE')
        elif self.state is SharingRetryIssuanceState.REUSED:
            _require(self.token is None and isinstance(self.result, SharingQueryResult)
                     and self.result.state is SharingResultState.REUSED, 'INVALID_SHARING_RETRY_ISSUANCE')
        else:
            _require(self.token is None and self.result is None, 'INVALID_SHARING_RETRY_ISSUANCE')


def _normalize(value, depth: int = 0):
    """Bounded conversion of nested SDK Row/STRUCT values before row validation."""
    _require(depth <= 4, 'INVALID_SHARING_RESULT_SHAPE')
    if isinstance(value, (Mapping, bigquery.Row)):
        _require(len(value) <= 32, 'INVALID_SHARING_RESULT_SHAPE')
        # Row.items() deep-copies nested values in this SDK; indexing avoids its
        # recursion problem with nested Row instances while preserving bounds.
        return {key: _normalize(value[key], depth + 1) for key in value.keys()}
    if isinstance(value, list):
        _require(len(value) <= 8, 'INVALID_SHARING_RESULT_SHAPE')
        return [_normalize(item, depth + 1) for item in value]
    return value


class ProtectedSharingExecutor:
    """Every call verifies actual request headers, intent scope and current access.

    Runtime adapters/key/settings must be provided by trusted server composition.
    This callable write adapter is not connected to the existing read gateway.
    """
    def __init__(self, *, client: bigquery.Client, verifier: IapIdentityAdapter,
                 reader: BigQueryAccessReader, sealer: SharingIntentSealer,
                 settings: SharingExecutorSettings | None = None):
        if (client.project != PROJECT or not isinstance(verifier, IapIdentityAdapter)
                or not isinstance(reader, BigQueryAccessReader) or reader.client is not client
                or not isinstance(sealer, SharingIntentSealer)
                or getattr(client, 'default_query_job_config', None) is not None):
            raise ValueError('Sharing requires scoped server adapters without default query overrides')
        self.client, self.verifier, self.reader, self.sealer = client, verifier, reader, sealer
        self.settings = SharingExecutorSettings() if settings is None else settings
        _require(isinstance(self.settings, SharingExecutorSettings), 'INVALID_SHARING_EXECUTOR_SETTINGS')
        if isinstance(sealer, RuntimeSharingSealer):
            _require(type(self)._dispatch_guard is not ProtectedSharingExecutor._dispatch_guard
                     and type(self)._release is not ProtectedSharingExecutor._release,
                     'SHARING_RUNTIME_GATES_REQUIRED')

    def _identity(self, headers: Mapping[str, str]) -> VerifiedIdentity:
        self._checkpoint()
        if not self.settings.execution_enabled:
            raise SharingExecutorError('SHARING_EXECUTION_DISABLED')
        identity = self.verifier.authenticate(headers)
        self._checkpoint()
        return identity

    def _checkpoint(self) -> None:
        """Legacy injected-key path has no request budget; runtime overrides it."""

    def _authorize(self, identity: VerifiedIdentity, attempt: SharingNativeAttempt) -> None:
        self._checkpoint()
        ensure_fresh(identity)
        if isinstance(attempt, SharingRetryAttempt):
            _require(SharingRetryAttempt(attempt.original, attempt.steps) == attempt, 'SHARING_ATTEMPT_MISMATCH')
        else:
            _require(isinstance(attempt, SharingAttempt) and initial_attempt(attempt.query) == attempt,
                     'SHARING_ATTEMPT_MISMATCH')
        command = attempt.query.command
        _within_window(command, _now())
        self.reader.authorize(identity, Action.SET_VISIBILITY, workspace_id=command.workspace_id,
                              conversation_id=command.conversation_id)
        self._checkpoint()

    def _dispatch_guard(self, identity: VerifiedIdentity, attempt: SharingNativeAttempt):
        """Legacy injected-key boundary; the opt-in runtime executor overrides it."""
        return nullcontext()

    def _release(self, identity: VerifiedIdentity, attempt: SharingNativeAttempt, result):
        """One exit hook for the opt-in fresh runtime/access release gate."""
        return result

    def issue_intent(self, headers: Mapping[str, str], *, workspace_id: str, conversation_id: str,
                     visibility: Visibility, expected_membership_revision: int,
                     expected_conversation_revision: int, expected_workspace_revision: int,
                     retry_key: str) -> str:
        """Accept once on the server; transport retries must retain this token.

        Pins are optimistic expectations, not permission claims. Reissuing an
        intent is not the recovery protocol and does not preserve an old envelope.
        """
        identity = self._identity(headers)
        command = make_sharing_command(workspace_id=workspace_id, conversation_id=conversation_id,
            principal_id=identity.principal.principal_id, visibility=visibility,
            expected_membership_revision=expected_membership_revision,
            expected_conversation_revision=expected_conversation_revision, retry_key=retry_key,
            accepted_at=_now(), trace_id='share_' + uuid4().hex)
        query = SharingQuery(command, expected_workspace_revision,
            SharingCommitObservation(command.audit_id, SharingCommitStatus.NOT_SUBMITTED), self.sealer.settings)
        self._authorize(identity, initial_attempt(query))
        return self._release(identity, initial_attempt(query), self.sealer.seal(query))

    def _schema_preflight(self) -> None:
        try:
            self._checkpoint()
            validate_dataset(operation_call(self.client.get_dataset, DATASET_ID, retry=None,
                                                     timeout=self.settings.rpc_timeout_seconds))
            self._checkpoint()
            for name in _CONTROL_TABLES:
                expected = TABLES[name].table(PROJECT)
                actual = operation_call(self.client.get_table, expected.reference, retry=None,
                                               timeout=self.settings.rpc_timeout_seconds)
                validate_table(actual, expected)
                self._checkpoint()
        except OperationStopped:
            raise
        except Exception:
            raise SharingExecutorError('SHARING_STORE_UNAVAILABLE') from None

    def _job_matches(self, job, attempt: SharingNativeAttempt) -> bool:
        return matches_sharing_job(job, job_id=attempt.job_id, sql=attempt.query.sql,
                                   config=attempt.query.job_config())

    def _outcome(self, attempt: SharingNativeAttempt, status=SharingCommitStatus.UNKNOWN,
                 result: SharingQueryResult | None = None) -> SharingExecutionOutcome:
        return SharingExecutionOutcome(SharingCommitObservation(attempt.query.command.audit_id, status,
                                                               attempt.job_id), result)

    def _failed_job_outcome(self, attempt: SharingNativeAttempt, parent: bigquery.QueryJob) -> SharingExecutionOutcome:
        self._checkpoint()
        status = failed_sharing_job_status(self.client, job_id=attempt.job_id,
            timeout_seconds=self.settings.rpc_timeout_seconds, parent=parent)
        self._checkpoint()
        return self._outcome(attempt, status)

    def _reconcile(self, attempt: SharingNativeAttempt) -> SharingExecutionOutcome:
        try:
            self._checkpoint()
            job = operation_call(self.client.get_job, attempt.job_id, project=PROJECT, location=LOCATION,
                                      retry=None, timeout=self.settings.rpc_timeout_seconds)
            self._checkpoint()
            if not self._job_matches(job, attempt):
                return self._outcome(attempt)
            if job.state != 'DONE':
                return self._outcome(attempt)
            if job.error_result is not None:
                return self._failed_job_outcome(attempt, job)
            result_rows = operation_call(job.result, max_results=2, page_size=2, retry=None, job_retry=None,
                                         timeout=self.settings.result_timeout_seconds)
            rows = list(islice(operation_iter(result_rows), 2))
            self._checkpoint()
            result = validate_sharing_result(attempt.query, [_normalize(row) for row in rows],
                                              query_job_id=attempt.job_id)
            status = (SharingCommitStatus.UNKNOWN if result.state is SharingResultState.UNKNOWN
                      else SharingCommitStatus.COMMITTED)
            return self._outcome(attempt, status, result)
        except OperationStopped:
            raise
        except Exception:
            # Missing metadata, lost results, timeout and malformed rows cannot
            # authorize another write. Recover again using this same job ID.
            return self._outcome(attempt)

    def _dispatch(self, identity: VerifiedIdentity, attempt: SharingNativeAttempt) -> SharingExecutionOutcome:
        with self._dispatch_guard(identity, attempt):
            ensure_fresh(identity)
            _within_window(attempt.query.command, _now())
            try:
                # One jobs.insert call only. Stable ID survives transport loss and
                # duplicate submits; a collision is reconciled, never replaced.
                self._checkpoint()
                operation_call(self.client.query, attempt.query.sql, job_config=attempt.query.job_config(),
                    job_id=attempt.job_id, project=PROJECT, location=LOCATION,
                    retry=None, job_retry=None, api_method=bigquery.enums.QueryApiMethod.INSERT,
                    timeout=self.settings.rpc_timeout_seconds)
            except OperationStopped:
                raise
            except Exception:
                pass
            self._checkpoint()  # Stop before reconciliation, even after a lost response.
            outcome = self._reconcile(attempt)
            self._authorize(identity, attempt)  # Recheck before exposing historical results.
            return outcome

    def submit(self, headers: Mapping[str, str], token: str) -> SharingExecutionOutcome:
        identity = self._identity(headers)
        attempt = self.sealer.resolve(token, identity)
        self._authorize(identity, attempt)
        self._schema_preflight()
        return self._release(identity, attempt, self._dispatch(identity, attempt))

    def recover(self, headers: Mapping[str, str], token: str) -> SharingExecutionOutcome:
        identity = self._identity(headers)
        attempt = self.sealer.resolve(token, identity)
        self._authorize(identity, attempt)
        outcome = self._reconcile(attempt)
        self._authorize(identity, attempt)
        return self._release(identity, attempt, outcome)

    def _receipt_snapshot(self, identity: VerifiedIdentity, attempt: SharingNativeAttempt
                          ) -> tuple[list[dict], SharingQueryResult]:
        """Keep complete records server-internal for current fence/pin validation."""
        ensure_fresh(identity)
        _within_window(attempt.query.command, _now())
        query = SharingReceiptQuery(attempt.query)
        config = query.job_config()
        read_job_id = 'share_receipt_v1_' + uuid4().hex
        try:
            self._checkpoint()
            job = operation_call(self.client.query, query.sql, job_config=config, job_id=read_job_id,
                project=PROJECT, location=LOCATION, retry=None, job_retry=None,
                api_method=bigquery.enums.QueryApiMethod.INSERT, timeout=self.settings.rpc_timeout_seconds)
            self._checkpoint()
            result_rows = operation_call(job.result, max_results=2, page_size=2, retry=None, job_retry=None,
                                         timeout=self.settings.result_timeout_seconds)
            rows = list(islice(operation_iter(result_rows), 2))
            self._checkpoint()
            if (job.state != 'DONE' or job.error_result is not None
                    or not matches_sharing_job(job, job_id=read_job_id, sql=query.sql, config=query.job_config())):
                raise SharingExecutorError('SHARING_RECEIPT_UNAVAILABLE')
        except OperationStopped:
            raise
        except Exception:
            raise SharingExecutorError('SHARING_RECEIPT_UNAVAILABLE') from None
        rows = [_normalize(row) for row in rows]
        original = attempt.original if isinstance(attempt, SharingRetryAttempt) else attempt
        result = validate_receipt_result(query, rows, original_job_id=original.job_id)
        return rows, result

    def _retry_sealer(self) -> SharingRetrySealer:
        if not isinstance(self.sealer, SharingRetrySealer):
            raise SharingExecutorError('SHARING_RETRY_NOT_CONFIGURED')
        return self.sealer

    def _resolve_attempt(self, token: str, identity: VerifiedIdentity) -> SharingNativeAttempt:
        if isinstance(token, str) and token.startswith('share-retry1.'):
            return self._retry_sealer().resolve_retry(token, identity)
        return self.sealer.resolve(token, identity)

    def _rollback_proof(self, identity: VerifiedIdentity,
                        attempt: SharingNativeAttempt) -> SharingCommitObservation | None:
        """Freshly verify ALL ancestors; signed supplied observations are insufficient.

        At most four native attempts for issuance, three predecessors for submit.
        Missing metadata, successful jobs, ambiguous children and timeouts stop
        verification. A receipt is checked separately, never used as job proof.
        """
        ancestors = [attempt]
        if isinstance(attempt, SharingRetryAttempt):
            ancestors = [attempt.original] + [SharingRetryAttempt(attempt.original, attempt.steps[:i])
                for i in range(1, len(attempt.steps) + 1)]
        observation = None
        for ancestor in ancestors:
            self._checkpoint()
            ensure_fresh(identity)
            _within_window(ancestor.query.command, _now())
            outcome = self._reconcile(ancestor)
            if outcome.observation.status is not SharingCommitStatus.NOT_COMMITTED:
                return None
            observation = outcome.observation
        return observation

    def _prepare_retry_snapshot(self, attempt: SharingRetryAttempt, rows: list[dict]) -> None:
        row = rows[0]  # Already bounded, shaped and authorized by receipt validation.
        preparation = prepare_sharing(attempt.query.command, workspace_rows=row['workspaces'],
            membership_rows=row['memberships'], conversation_rows=row['conversations'],
            receipt_rows=row['audit_events'], expected_workspace_revision=attempt.query.expected_workspace_revision,
            observed_at=row['observed_at'], commit_observation=attempt.query.commit_observation)
        _require(preparation.state is SharingPreparationState.PREPARED, 'INVALID_SHARING_RETRY_PREPARATION')

    def issue_retry(self, headers: Mapping[str, str], predecessor_token: str) -> SharingRetryIssuance:
        """Server-derived retry only after current native rollback and BQ checks.

        Accept no caller status/fence. Receipt reuse wins over job uncertainty;
        absent receipt plus uncertainty stays recovery-only. No DML is submitted.
        """
        identity = self._identity(headers)
        sealer = self._retry_sealer()
        predecessor = self._resolve_attempt(predecessor_token, identity)
        self._authorize(identity, predecessor)
        self._schema_preflight()
        observation = self._rollback_proof(identity, predecessor)
        rows, result = self._receipt_snapshot(identity, predecessor)
        self._authorize(identity, predecessor)
        if result.state is SharingResultState.REUSED:
            return self._release(identity, predecessor,
                SharingRetryIssuance(SharingRetryIssuanceState.REUSED, result=result))
        if observation is None:
            return self._release(identity, predecessor,
                SharingRetryIssuance(SharingRetryIssuanceState.RECOVERY_REQUIRED))
        row = rows[0]
        attempt = prepare_retry(predecessor, commit_observation=observation,
            expected_workspace_revision=row['workspaces'][0]['revision'], observed_at=row['observed_at'])
        self._prepare_retry_snapshot(attempt, rows)
        ensure_fresh(identity)
        return self._release(identity, attempt,
            SharingRetryIssuance(SharingRetryIssuanceState.READY, token=sealer.seal_retry(attempt)))

    def submit_retry(self, headers: Mapping[str, str], token: str) -> SharingExecutionOutcome:
        """Reverify rollback and controls; dispatch only the sealed ID/fence once."""
        identity = self._identity(headers)
        attempt = self._retry_sealer().resolve_retry(token, identity)
        self._authorize(identity, attempt)
        self._schema_preflight()
        ensure_fresh(identity)
        _within_window(attempt.query.command, _now())
        outcome = self._reconcile(attempt)
        self._authorize(identity, attempt)
        if outcome.observation.status is not SharingCommitStatus.UNKNOWN:
            return self._release(identity, attempt, outcome)  # No resubmit/remint.
        predecessor = (attempt.original if len(attempt.steps) == 1 else
                       SharingRetryAttempt(attempt.original, attempt.steps[:-1]))
        observation = self._rollback_proof(identity, predecessor)
        rows, result = self._receipt_snapshot(identity, attempt)
        self._authorize(identity, attempt)
        if result.state is SharingResultState.REUSED:
            # Intent committed, but this SELECT cannot attribute the mutation to
            # this native job. Keep its observation UNKNOWN alongside REUSED.
            return self._release(identity, attempt, self._outcome(attempt, result=result))
        if observation is None:
            return self._release(identity, attempt, self._outcome(attempt))
        self._prepare_retry_snapshot(attempt, rows)  # Pins AND sealed fence must still match.
        return self._release(identity, attempt, self._dispatch(identity, attempt))

    def recover_retry(self, headers: Mapping[str, str], token: str) -> SharingExecutionOutcome:
        """Recover ONLY the sealed retry job; missing ancestors never prevent reads."""
        identity = self._identity(headers)
        attempt = self._retry_sealer().resolve_retry(token, identity)
        self._authorize(identity, attempt)
        outcome = self._reconcile(attempt)
        self._authorize(identity, attempt)
        return self._release(identity, attempt, outcome)

    def recover_receipt(self, headers: Mapping[str, str], token: str) -> SharingQueryResult:
        """SELECT receipt for initial/retry tokens; no job classification or remint.

        No original/predecessor job metadata is required. Receipt absence stays
        UNKNOWN; current access is checked before and after the read.
        """
        identity = self._identity(headers)
        attempt = self._resolve_attempt(token, identity)
        self._authorize(identity, attempt)
        self._schema_preflight()
        _, result = self._receipt_snapshot(identity, attempt)
        self._authorize(identity, attempt)
        return self._release(identity, attempt, result)
