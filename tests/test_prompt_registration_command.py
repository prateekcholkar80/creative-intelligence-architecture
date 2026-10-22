"""Offline authenticated prompt-registration command composition tests."""
from dataclasses import replace
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.access_policy import (
    AccessDecision,
    Action,
    AuthorizationError,
    Reason,
)
from src.application.identity import IdentityError
from src.application.prompt_import_preview import (
    PromptImportPreviewService,
)
from src.application.prompt_registration_command import (
    PromptRegistrationCommand,
    PromptRegistrationCommandError,
    PromptRegistrationCommandService,
)
from src.application.prompt_registry_executor import (
    PromptRegistrationOutcome,
)
from src.application.prompt_registry_query import (
    PromptRegistrationResult,
    PromptRegistrationResultState,
)
from src.application.prompt_registry_reader import (
    PromptRegistryRead,
    PromptRegistryReadError,
    PromptRegistryReadRequest,
)
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import ContractError
from tests.test_access_reader import EPOCH, IDENTITY, WORKSPACE
from tests.test_prompt_contract import ACTOR, NOW
from tests.test_prompt_import_preview import (
    FakeRegistryReader,
    preview_request,
)
from tests.test_vertex_prompt_reader import FakeTransport


def decision(*, revision=3):
    return AccessDecision(
        True, Action.MANAGE_MODELS, Reason.ALLOWED_WORKSPACE_ACTION,
        WORKSPACE, IDENTITY.principal.principal_id, revision,
        None, None)


class AccessReader:
    def __init__(self, results=None):
        self.results = list(results or [decision(), decision(revision=4)])
        self.calls = []

    def authorize(self, identity, action, *, workspace_id,
                  conversation_id=None):
        self.calls.append((identity, action, workspace_id, conversation_id))
        value = self.results.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class MutableRegistryReader(FakeRegistryReader):
    def __init__(self):
        super().__init__()
        self.post_state = None
        self.post_calls = 0

    def read_exact(self, prompt_id, version_id):
        if self.post_state is None:
            return super().read_exact(prompt_id, version_id)
        self.post_calls += 1
        request = PromptRegistryReadRequest.exact(prompt_id, version_id)
        return PromptRegistryRead(
            request, self.post_state, NOW + timedelta(seconds=3))


class Writer:
    def __init__(self, registry):
        self.registry = registry
        self.calls = []
        self.status = SharingCommitStatus.COMMITTED
        self.result_state = PromptRegistrationResultState.APPLIED
        self.publish = True
        self.publish_state = None
        self.error = None

    def submit(self, plan):
        self.calls.append(plan)
        if self.error is not None:
            raise self.error
        if self.publish:
            self.registry.post_state = (
                plan.state if self.publish_state is None
                else self.publish_state)
        result = None
        if self.result_state is not None:
            result = PromptRegistrationResult(
                self.result_state, NOW + timedelta(seconds=3),
                plan.state.head.snapshot.prompt_key,
                plan.state.head.snapshot.snapshot_sha256,
                plan.state.head.digest)
        return PromptRegistrationOutcome(
            plan.job_id, self.status, result, submitted=True)


def approved(registry=None, *, proposed_by=None, previewed_at=None):
    registry = FakeRegistryReader() if registry is None else registry
    service = PromptImportPreviewService(FakeTransport(), registry)
    request = preview_request(
        proposed_by=(IDENTITY.principal.principal_id
                     if proposed_by is None else proposed_by),
        **({} if previewed_at is None
           else {'previewed_at': previewed_at}))
    return service.preview(request)


class PromptRegistrationCommandTests(unittest.TestCase):
    def setUp(self):
        self.registry = MutableRegistryReader()
        self.preview = approved(self.registry)
        self.preview_service = PromptImportPreviewService(
            FakeTransport(), self.registry)
        self.access = AccessReader()
        self.writer = Writer(self.registry)
        self.service = PromptRegistrationCommandService(
            access_reader=self.access,
            preview_service=self.preview_service,
            executor=self.writer,
            registry_reader=self.registry)
        self.command = PromptRegistrationCommand(
            WORKSPACE, self.preview, self.preview.preview_sha256)
        self.time = patch('src.application.identity.time.time',
                          return_value=EPOCH).start()
        self.addCleanup(patch.stopall)

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(
                (ContractError, PromptRegistrationCommandError)) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_applied_registration_rechecks_access_and_exact_state(self):
        result = self.service.execute(IDENTITY, self.command)
        self.assertEqual(result.registration_state,
                         PromptRegistrationResultState.APPLIED)
        self.assertEqual(result.registered.state, self.preview.proposal)
        self.assertEqual((result.access_revision_before,
                          result.access_revision_after), (3, 4))
        self.assertFalse(result.recovered_existing_state)
        self.assertEqual(len(self.writer.calls), 1)
        self.assertEqual(self.registry.post_calls, 1)
        self.assertEqual([item[1] for item in self.access.calls],
                         [Action.MANAGE_MODELS, Action.MANAGE_MODELS])
        self.assertTrue(all(item[2] == WORKSPACE
                            and item[3] is None
                            for item in self.access.calls))

    def test_same_approved_command_recovers_already_registered_state(self):
        self.registry.post_state = self.preview.proposal
        self.writer.result_state = PromptRegistrationResultState.REUSED
        result = self.service.execute(IDENTITY, self.command)
        self.assertTrue(result.recovered_existing_state)
        self.assertEqual(result.registration_state,
                         PromptRegistrationResultState.REUSED)
        self.assertEqual(len(self.writer.calls), 1)

    def test_actor_mismatch_and_expired_identity_stop_before_access(self):
        other = approved(proposed_by=ACTOR)
        command = PromptRegistrationCommand(
            WORKSPACE, other, other.preview_sha256)
        self.assert_code('PROMPT_REGISTRATION_ACTOR_MISMATCH',
                         self.service.execute, IDENTITY, command)
        self.assertEqual(self.access.calls, [])
        with patch('src.application.identity.time.time',
                   return_value=EPOCH + 1000), self.assertRaises(IdentityError):
            self.service.execute(IDENTITY, self.command)
        self.assertEqual(self.access.calls, [])

    def test_initial_or_post_execution_access_denial_never_releases_success(self):
        denial = AuthorizationError(AccessDecision(
            False, Action.MANAGE_MODELS, Reason.MEMBERSHIP_INACTIVE,
            WORKSPACE, IDENTITY.principal.principal_id, 5, None, None))
        service = PromptRegistrationCommandService(
            access_reader=AccessReader([denial]),
            preview_service=self.preview_service, executor=self.writer,
            registry_reader=self.registry)
        with self.assertRaises(AuthorizationError):
            service.execute(IDENTITY, self.command)
        self.assertEqual(self.writer.calls, [])

        self.access.results = [decision(), denial]
        with self.assertRaises(AuthorizationError):
            self.service.execute(IDENTITY, self.command)
        self.assertEqual(len(self.writer.calls), 1)
        self.assertEqual(self.registry.post_calls, 0)

    def test_preview_hash_or_state_drift_stops_before_executor(self):
        changed = replace(
            self.preview.request,
            previewed_at=self.preview.request.previewed_at
            + timedelta(seconds=1))
        self.preview_service.preview = lambda _request: (
            PromptImportPreviewService(
                FakeTransport(), FakeRegistryReader()).preview(changed))
        self.assert_code('PROMPT_REGISTRATION_PREVIEW_MISMATCH',
                         self.service.execute, IDENTITY, self.command)
        self.assertEqual(self.writer.calls, [])

    def test_command_accepts_only_exact_approved_unregistered_preview(self):
        for workspace, digest in (
                ('bad workspace', self.preview.preview_sha256),
                (WORKSPACE, 'f' * 64)):
            with self.subTest(workspace=workspace, digest=digest), \
                    self.assertRaises(ContractError):
                PromptRegistrationCommand(workspace, self.preview, digest)
        self.registry.post_state = self.preview.proposal
        existing = self.preview_service.preview(self.preview.request)
        with self.assertRaises(ContractError):
            PromptRegistrationCommand(
                WORKSPACE, existing, existing.preview_sha256)

    def test_unknown_rollback_or_missing_receipt_fail_without_resubmit(self):
        cases = (
            (SharingCommitStatus.UNKNOWN, None),
            (SharingCommitStatus.NOT_COMMITTED, None),
            (SharingCommitStatus.COMMITTED, None),
        )
        for status, result_state in cases:
            with self.subTest(status=status):
                self.writer.status = status
                self.writer.result_state = result_state
                self.writer.publish = False
                before = len(self.writer.calls)
                self.assert_code(
                    'PROMPT_REGISTRATION_OUTCOME_UNVERIFIED',
                    self.service.execute, IDENTITY, self.command)
                self.assertEqual(len(self.writer.calls), before + 1)
                self.access.results.extend(
                    [decision(), decision(revision=4)])

    def test_postread_missing_or_different_state_fails_closed(self):
        self.writer.publish = False
        self.registry.post_state = None
        self.assert_code('PROMPT_REGISTRY_NOT_FOUND',
                         self.service.execute, IDENTITY, self.command)
        self.assertEqual(len(self.writer.calls), 1)

        self.registry.post_state = None
        self.access.results = [decision(), decision(revision=4)]
        other = approved(
            previewed_at=self.preview.request.previewed_at
            + timedelta(seconds=1)).proposal
        self.writer.publish = True
        self.writer.publish_state = other
        self.assert_code('PROMPT_REGISTRATION_POSTREAD_MISMATCH',
                         self.service.execute, IDENTITY, self.command)

    def test_invalid_access_or_writer_result_is_rejected(self):
        invalid = AccessDecision(
            True, Action.MANAGE_MODELS, Reason.ALLOWED_WORKSPACE_ACTION,
            WORKSPACE, 'other-principal', 3, None, None)
        for value in (invalid, replace(decision(), policy_version='old-policy')):
            self.access.results = [value]
            self.assert_code('PROMPT_REGISTRATION_ACCESS_INVALID',
                             self.service.execute, IDENTITY, self.command)
        self.writer.calls.clear()

        self.access.results = [decision(), decision()]
        self.writer.submit = lambda _plan: {}
        self.assert_code('PROMPT_REGISTRATION_OUTCOME_UNVERIFIED',
                         self.service.execute, IDENTITY, self.command)

    def test_post_execution_access_revision_cannot_regress(self):
        self.access.results = [decision(revision=5), decision(revision=4)]
        self.assert_code('PROMPT_REGISTRATION_ACCESS_REGRESSION',
                         self.service.execute, IDENTITY, self.command)
        self.assertEqual(len(self.writer.calls), 1)
        self.assertEqual(self.registry.post_calls, 0)

    def test_dependency_validation_and_disabled_executor_error_are_safe(self):
        for changes in (
                {'access_reader': object()},
                {'preview_service': object()},
                {'executor': object()},
                {'registry_reader': object()}):
            values = {
                'access_reader': self.access,
                'preview_service': self.preview_service,
                'executor': self.writer,
                'registry_reader': self.registry,
            } | changes
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                PromptRegistrationCommandService(**values)
        self.writer.error = RuntimeError('private executor failure')
        self.assert_code('PROMPT_REGISTRATION_EXECUTOR_UNAVAILABLE',
                         self.service.execute, IDENTITY, self.command)


if __name__ == '__main__':
    unittest.main()
