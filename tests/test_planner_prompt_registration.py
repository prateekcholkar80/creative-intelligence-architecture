"""Batch 13 operator boundaries; actual command contracts, no cloud/model calls."""
from contextlib import contextmanager
from dataclasses import replace
import io
import json
import unittest
from unittest.mock import Mock, patch

from src.application.identity import ASSERTION_HEADER, IdentityError
from src.application.planner_prompt_registration import (
    PlannerPromptRegistrationOperator, main, planner_request, review_projection,
)
from src.application.prompt_import_preview import PromptImportPreviewRequest, PromptImportPreviewService
from src.application.prompt_registration_command import (
    PromptRegistrationCommandError, PromptRegistrationCommandService,
)
from src.application.sharing_coordinator_contract import SharingCommitStatus
from tests.test_access_reader import EPOCH, IDENTITY, WORKSPACE
from tests.test_planner_prompt_parity import CONFIG, candidate
from tests.test_prompt_contract import NOW
from tests.test_prompt_registration_command import (
    AccessReader, MutableRegistryReader, Writer, decision,
)
from tests.test_vertex_prompt_reader import FakeTransport


class PlannerPromptRegistrationTests(unittest.TestCase):
    def setUp(self):
        self.registry = MutableRegistryReader()
        self.preview = PromptImportPreviewService(FakeTransport(), self.registry)
        self.access = AccessReader([decision() for _ in range(8)])
        self.writer = Writer(self.registry)
        self.service = PromptRegistrationCommandService(
            access_reader=self.access, preview_service=self.preview,
            executor=self.writer, registry_reader=self.registry)
        self.identity = Mock()
        self.identity.authenticate.return_value = IDENTITY
        self.operator = PlannerPromptRegistrationOperator(
            preview_service=self.preview, command_service=self.service,
            identity_adapter=self.identity, config=CONFIG)
        self.request = planner_request('1234567890', '1')
        self.headers = {ASSERTION_HEADER: 'test-assertion-never-exported'}
        self.snapshot = candidate()
        self.vertex_read = patch(
            'src.application.prompt_import_preview.read_exact_vertex_prompt',
            return_value=self.snapshot).start()
        patch('src.application.identity.time.time', return_value=EPOCH).start()
        clock = patch('src.application.planner_prompt_registration.datetime').start()
        clock.now.return_value = NOW
        self.addCleanup(patch.stopall)

    def prepare(self):
        return self.operator.prepare(self.headers, workspace_id=WORKSPACE,
            request=self.request)

    def register(self, command):
        return self.operator.register(self.headers, command,
            confirmed_sha256=command.approved_preview_sha256)

    def test_inspection_exports_full_canonical_review_without_authorization(self):
        preview = self.operator.inspect(self.request)
        review = review_projection(preview, authorized=False)
        self.assertFalse(review['registration_authorized'])
        self.assertFalse(review['mutation_performed'])
        self.assertIsNone(review['proposed_status'])
        self.assertEqual(review['system_instruction'], self.snapshot.system_instruction)
        self.assertEqual(review['prompt_template'], self.snapshot.prompt_template)
        self.assertEqual(review['output_schema'], json.loads(self.snapshot.output_schema_json))
        self.assertEqual(len(review['differences']), 9)
        self.assertEqual(self.writer.calls, [])
        self.assertEqual(self.access.calls, [])
        self.identity.authenticate.assert_not_called()
        self.assertNotIn(self.headers[ASSERTION_HEADER], json.dumps(review))

    def test_prepare_binds_verified_actor_and_rechecks_access(self):
        command = self.prepare()
        self.assertEqual(command.approved_preview.request.proposed_by,
            IDENTITY.principal.principal_id)
        self.assertEqual(len(self.access.calls), 2)
        self.assertEqual(self.writer.calls, [])
        self.assertEqual(review_projection(command.approved_preview,
            authorized=True)['proposed_status'], 'DRAFT')

    def test_no_identity_or_initial_denial_stops_before_vertex_or_writer(self):
        self.identity.authenticate.side_effect = IdentityError('AUTHENTICATION_REQUIRED')
        with self.assertRaises(IdentityError):
            self.prepare()
        self.vertex_read.assert_not_called()
        self.identity.authenticate.side_effect = None
        self.access.results = [replace(decision(), allowed=False)]
        with self.assertRaises(PromptRegistrationCommandError):
            self.prepare()
        self.vertex_read.assert_not_called()
        self.assertEqual(self.writer.calls, [])

    def test_wrong_scope_actor_action_and_partial_decisions_deny(self):
        for changes in ({'workspace_id': 'other'}, {'principal_id': 'other'},
                        {'membership_revision': None}, {'resource_id': 'other'},
                        {'policy_version': 'wrong'}):
            with self.subTest(changes=changes):
                self.access.results = [replace(decision(), **changes)]
                with self.assertRaises(PromptRegistrationCommandError):
                    self.prepare()
        self.assertEqual(self.writer.calls, [])

    def test_revocation_or_regression_during_preview_denies_release(self):
        for after in (replace(decision(), allowed=False), decision(revision=2)):
            self.access.results = [decision(revision=3), after]
            with self.assertRaises(PromptRegistrationCommandError):
                self.prepare()
        self.assertEqual(self.writer.calls, [])

    def test_parity_change_blocks_preparation(self):
        self.vertex_read.return_value = replace(self.snapshot, model_id='changed-model')
        with self.assertRaises(PromptRegistrationCommandError) as caught:
            self.prepare()
        self.assertEqual(caught.exception.code, 'PLANNER_PROMPT_PARITY_FAILED')
        self.assertEqual(self.writer.calls, [])

    def test_hash_confirmation_required_before_authentication_and_dispatch(self):
        command = self.prepare()
        self.identity.authenticate.reset_mock()
        for digest in ('', '0' * 64, command.approved_preview_sha256.upper(), None):
            with self.assertRaises(PromptRegistrationCommandError):
                self.operator.register(self.headers, command, confirmed_sha256=digest)
        self.identity.authenticate.assert_not_called()
        self.assertEqual(self.writer.calls, [])

    def test_authenticated_apply_verifies_current_and_history(self):
        command = self.prepare()
        result = self.register(command)
        self.assertEqual(result.registered.state, command.approved_preview.proposal)
        self.assertEqual(len(result.registered.state.history), 1)
        self.assertEqual(result.registered.state.head.status.value, 'DRAFT')
        self.assertEqual(len(self.writer.calls), 1)
        self.assertEqual(len(self.access.calls), 4)
        self.assertEqual(self.identity.authenticate.call_count, 2)

    def test_same_server_held_command_recovers_exact_committed_state(self):
        command = self.prepare()
        self.registry.post_state = command.approved_preview.proposal
        result = self.register(command)
        self.assertTrue(result.recovered_existing_state)
        self.assertEqual(self.writer.calls[0].state, command.approved_preview.proposal)

    def test_read_only_review_cannot_register_as_authenticated_actor(self):
        from src.application.prompt_registration_command import PromptRegistrationCommand
        preview = self.operator.inspect(self.request)
        command = PromptRegistrationCommand(WORKSPACE, preview, preview.preview_sha256)
        with self.assertRaises(PromptRegistrationCommandError) as caught:
            self.register(command)
        self.assertEqual(caught.exception.code, 'PROMPT_REGISTRATION_ACTOR_MISMATCH')
        self.assertEqual(self.writer.calls, [])

    def test_changed_vertex_after_review_never_dispatches(self):
        command = self.prepare()
        self.vertex_read.return_value = replace(self.snapshot,
            system_instruction=self.snapshot.system_instruction + '\nChanged.')
        with self.assertRaises(PromptRegistrationCommandError):
            self.register(command)
        self.assertEqual(self.writer.calls, [])

    def test_unknown_result_cannot_release_success_or_retry(self):
        command = self.prepare()
        self.writer.status = SharingCommitStatus.UNKNOWN
        self.writer.result_state = None
        with self.assertRaises(PromptRegistrationCommandError):
            self.register(command)
        self.assertEqual(len(self.writer.calls), 1)

    def test_cli_defaults_to_read_only_and_never_prompts_for_identity(self):
        @contextmanager
        def factory(config, **kwargs):
            self.assertFalse(kwargs['execution_enabled'])
            self.assertIsNone(kwargs['iap_settings'])
            yield self.operator
        with patch('src.application.planner_prompt_registration.live_operator', factory), \
                patch('sys.argv', ['registration', '--prompt-id', '1234567890', '--version-id', '1']), \
                patch('sys.stdout', new_callable=io.StringIO) as output, \
                patch('getpass.getpass') as secret:
            main()
        self.assertFalse(json.loads(output.getvalue())['registration_authorized'])
        secret.assert_not_called()
        self.assertEqual(self.writer.calls, [])

    def test_apply_missing_iap_fails_before_adc_and_secret_prompt(self):
        with patch.dict('os.environ', {}, clear=True), \
                patch('sys.argv', ['registration', '--apply', '--workspace-id', WORKSPACE]), \
                patch('sys.stdout', new_callable=io.StringIO) as output, \
                patch('src.application.planner_prompt_registration.live_operator') as factory, \
                patch('getpass.getpass') as secret, self.assertRaises(SystemExit):
            main()
        self.assertEqual(json.loads(output.getvalue())['error_code'],
            'PROMPT_REGISTRATION_IAP_NOT_CONFIGURED')
        factory.assert_not_called()
        secret.assert_not_called()

    def test_cli_apply_holds_exact_review_across_fresh_client_composition(self):
        @contextmanager
        def factory(config, **kwargs):
            self.assertTrue(kwargs['execution_enabled'])
            self.assertIsNotNone(kwargs['iap_settings'])
            yield self.operator
        preview = self.preview.preview(PromptImportPreviewRequest(
            self.request, NOW, IDENTITY.principal.principal_id))
        with patch.dict('os.environ', {'CI_IAP_EXPECTED_AUDIENCE': IDENTITY.audience}), \
                patch('src.application.planner_prompt_registration.live_operator', factory), \
                patch('sys.argv', ['registration', '--apply', '--workspace-id', WORKSPACE,
                    '--prompt-id', '1234567890', '--version-id', '1']), \
                patch('sys.stdout', new_callable=io.StringIO) as output, \
                patch('getpass.getpass', return_value=self.headers[ASSERTION_HEADER]), \
                patch('builtins.input', return_value=preview.preview_sha256):
            main()
        self.assertIn('planner_prompt_draft_verified', output.getvalue())
        self.assertNotIn(self.headers[ASSERTION_HEADER], output.getvalue())
        self.assertEqual(len(self.writer.calls), 1)
        self.assertEqual(self.writer.calls[0].state, preview.proposal)

    def test_cli_provider_error_never_echoes_exception_or_credentials(self):
        @contextmanager
        def factory(*args, **kwargs):
            raise RuntimeError('secret assertion and unsafe provider body')
            yield
        with patch('src.application.planner_prompt_registration.live_operator', factory), \
                patch('sys.argv', ['registration']), \
                patch('sys.stdout', new_callable=io.StringIO) as output, self.assertRaises(SystemExit):
            main()
        self.assertNotIn('secret assertion', output.getvalue())
        self.assertEqual(json.loads(output.getvalue())['error_code'],
            'PROMPT_REGISTRATION_OPERATOR_UNAVAILABLE')

    def test_cli_rejected_review_never_requests_second_assertion_or_writes(self):
        @contextmanager
        def factory(*args, **kwargs):
            yield self.operator
        with patch.dict('os.environ', {'CI_IAP_EXPECTED_AUDIENCE': IDENTITY.audience}), \
                patch('src.application.planner_prompt_registration.live_operator', factory), \
                patch('sys.argv', ['registration', '--apply', '--workspace-id', WORKSPACE,
                    '--prompt-id', '1234567890', '--version-id', '1']), \
                patch('sys.stdout', new_callable=io.StringIO) as output, \
                patch('getpass.getpass', return_value=self.headers[ASSERTION_HEADER]) as secret, \
                patch('builtins.input', return_value='wrong hash'), self.assertRaises(SystemExit):
            main()
        self.assertIn('PROMPT_REGISTRATION_REVIEW_NOT_CONFIRMED', output.getvalue())
        self.assertEqual(secret.call_count, 1)
        self.assertEqual(self.writer.calls, [])


if __name__ == '__main__':
    unittest.main()
