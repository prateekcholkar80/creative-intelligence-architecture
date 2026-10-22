"""Authenticated command composition for initial prompt registration.

The caller supplies a server-held, hash-bound UNREGISTERED preview. This service
requires current MANAGE_MODELS access before and after the one executor call and
freshly verifies the exact authoritative BigQuery state before returning success.
It never enables the executor, bootstraps access, activates a prompt or dispatches
a model.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from src.application.access_policy import (
    AccessDecision,
    Action,
    POLICY_VERSION,
    Reason,
)
from src.application.identity import VerifiedIdentity, ensure_fresh
from src.application.operation_budget import OperationStopped
from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_import_preview import (
    PromptImportPreview,
    PromptImportPreviewError,
    PromptImportPreviewService,
    PromptImportPreviewStatus,
)
from src.application.prompt_registry_executor import (
    PromptRegistrationOutcome,
    PromptRegistryExecutor,
)
from src.application.prompt_registry_query import (
    PromptRegistrationQuery,
    PromptRegistrationResultState,
)
from src.application.prompt_registry_reader import (
    PromptRegistryRead,
    PromptRegistryReadError,
)
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import ContractError, _id, _require


class PromptRegistrationAccessReader(Protocol):
    def authorize(self, identity: VerifiedIdentity, action: Action, *,
                  workspace_id: str,
                  conversation_id: str | None = None) -> AccessDecision: ...


class PromptRegistrationStateReader(Protocol):
    def read_exact(self, vertex_prompt_id: str,
                   vertex_version_id: str) -> PromptRegistryRead: ...


class PromptRegistrationWriter(Protocol):
    def submit(self, plan: PromptRegistrationQuery
               ) -> PromptRegistrationOutcome: ...


@dataclass(frozen=True, slots=True)
class PromptRegistrationCommand:
    workspace_id: str
    approved_preview: PromptImportPreview
    approved_preview_sha256: str

    def __post_init__(self):
        _id(self.workspace_id)
        _require(isinstance(self.approved_preview, PromptImportPreview)
                 and self.approved_preview.status
                 is PromptImportPreviewStatus.UNREGISTERED
                 and self.approved_preview.exact_state is None
                 and self.approved_preview.proposal is not None
                 and self.approved_preview.proposal.head.revision == 1
                 and self.approved_preview.proposal.head.status
                 is PromptLifecycle.DRAFT
                 and type(self.approved_preview_sha256) is str
                 and self.approved_preview_sha256
                 == self.approved_preview.preview_sha256,
                 'INVALID_PROMPT_REGISTRATION_COMMAND')


@dataclass(frozen=True, slots=True)
class PromptRegistrationCommandResult:
    workspace_id: str
    principal_id: str
    approved_preview_sha256: str
    native_job_id: str
    registration_state: PromptRegistrationResultState
    registered: PromptRegistryRead
    access_revision_before: int
    access_revision_after: int
    recovered_existing_state: bool

    def __post_init__(self):
        _id(self.workspace_id)
        _id(self.principal_id, maximum=256)
        _require(type(self.approved_preview_sha256) is str
                 and len(self.approved_preview_sha256) == 64
                 and type(self.native_job_id) is str
                 and bool(self.native_job_id)
                 and isinstance(self.registration_state,
                                PromptRegistrationResultState)
                 and isinstance(self.registered, PromptRegistryRead)
                 and type(self.access_revision_before) is int
                 and self.access_revision_before >= 1
                 and type(self.access_revision_after) is int
                 and self.access_revision_after >= 1
                 and type(self.recovered_existing_state) is bool,
                 'INVALID_PROMPT_REGISTRATION_COMMAND_RESULT')


class PromptRegistrationCommandError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__('Prompt registration could not be completed.')


class PromptRegistrationCommandService:
    def __init__(self, *, access_reader: PromptRegistrationAccessReader,
                 preview_service: PromptImportPreviewService,
                 executor: PromptRegistrationWriter,
                 registry_reader: PromptRegistrationStateReader):
        if (not callable(getattr(access_reader, 'authorize', None))
                or not isinstance(preview_service, PromptImportPreviewService)
                or not callable(getattr(executor, 'submit', None))
                or not callable(getattr(registry_reader, 'read_exact', None))):
            raise ValueError('Trusted prompt-registration dependencies required')
        if preview_service.registry_reader is not registry_reader:
            raise ValueError('Preview and post-write reads must share one registry reader')
        self.access_reader = access_reader
        self.preview_service = preview_service
        self.executor = executor
        self.registry_reader = registry_reader

    def _authorize(self, identity: VerifiedIdentity,
                   workspace_id: str) -> AccessDecision:
        decision = self.access_reader.authorize(
            identity, Action.MANAGE_MODELS, workspace_id=workspace_id)
        if (not isinstance(decision, AccessDecision) or not decision.allowed
                or decision.action is not Action.MANAGE_MODELS
                or decision.reason is not Reason.ALLOWED_WORKSPACE_ACTION
                or decision.workspace_id != workspace_id
                or decision.principal_id != identity.principal.principal_id
                or type(decision.membership_revision) is not int
                or decision.membership_revision < 1
                or decision.resource_id is not None
                or decision.resource_revision is not None
                or decision.policy_version != POLICY_VERSION):
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_ACCESS_INVALID')
        return decision

    @staticmethod
    def _validate_outcome(plan: PromptRegistrationQuery,
                          outcome) -> PromptRegistrationOutcome:
        if (not isinstance(outcome, PromptRegistrationOutcome)
                or outcome.native_job_id != plan.job_id
                or outcome.native_status is not SharingCommitStatus.COMMITTED
                or outcome.result is None
                or outcome.result.state not in {
                    PromptRegistrationResultState.APPLIED,
                    PromptRegistrationResultState.REUSED}
                or outcome.result.prompt_key
                != plan.state.head.snapshot.prompt_key
                or outcome.result.snapshot_sha256
                != plan.state.head.snapshot.snapshot_sha256
                or outcome.result.record_digest != plan.state.head.digest):
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_OUTCOME_UNVERIFIED')
        return outcome

    def execute(self, identity: VerifiedIdentity,
                command: PromptRegistrationCommand
                ) -> PromptRegistrationCommandResult:
        if not isinstance(command, PromptRegistrationCommand):
            raise PromptRegistrationCommandError(
                'INVALID_PROMPT_REGISTRATION_COMMAND')
        ensure_fresh(identity)
        principal_id = identity.principal.principal_id
        if command.approved_preview.request.proposed_by != principal_id:
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_ACTOR_MISMATCH')
        before = self._authorize(identity, command.workspace_id)

        try:
            fresh = self.preview_service.preview(
                command.approved_preview.request)
        except OperationStopped:
            raise
        except PromptImportPreviewError as error:
            raise PromptRegistrationCommandError(error.code) from None
        except Exception:
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_PREVIEW_UNAVAILABLE') from None

        approved = command.approved_preview
        recovery = False
        if fresh.preview_sha256 == command.approved_preview_sha256:
            if (fresh.status is not PromptImportPreviewStatus.UNREGISTERED
                    or fresh.proposal != approved.proposal):
                raise PromptRegistrationCommandError(
                    'PROMPT_REGISTRATION_PREVIEW_MISMATCH')
        elif (fresh.status is PromptImportPreviewStatus.EXISTING
              and fresh.candidate == approved.candidate
              and fresh.exact_state == approved.proposal):
            # A prior invocation may have committed and lost its response. The
            # same deterministic plan/job is recovered; no new proposal is made.
            recovery = True
        else:
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_PREVIEW_MISMATCH')

        plan = PromptRegistrationQuery(approved.proposal)
        try:
            outcome = self.executor.submit(plan)
        except OperationStopped:
            raise
        except Exception as error:
            code = getattr(error, 'code',
                           'PROMPT_REGISTRATION_EXECUTOR_UNAVAILABLE')
            raise PromptRegistrationCommandError(code) from None

        # Access can change while the native job runs. Never release success
        # until current identity/membership is checked again.
        ensure_fresh(identity)
        after = self._authorize(identity, command.workspace_id)
        if after.membership_revision < before.membership_revision:
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_ACCESS_REGRESSION')
        outcome = self._validate_outcome(plan, outcome)

        try:
            registered = self.registry_reader.read_exact(
                approved.candidate.vertex_prompt_id,
                approved.candidate.vertex_version_id)
        except OperationStopped:
            raise
        except (ContractError, PromptRegistryReadError) as error:
            raise PromptRegistrationCommandError(
                getattr(error, 'code',
                        'PROMPT_REGISTRATION_POSTREAD_UNAVAILABLE')) from None
        except Exception:
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_POSTREAD_UNAVAILABLE') from None
        if (not isinstance(registered, PromptRegistryRead)
                or registered.state != approved.proposal):
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_POSTREAD_MISMATCH')

        return PromptRegistrationCommandResult(
            command.workspace_id, principal_id,
            command.approved_preview_sha256, outcome.native_job_id,
            outcome.result.state, registered,
            before.membership_revision, after.membership_revision, recovery)
