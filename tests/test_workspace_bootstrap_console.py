"""Explicit bootstrap UI authorization, uncertainty and recovery; offline only."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from src.application.control_io import ControlOutcome
from src.application.identity import ISSUER, IapSettings, principal_for_subject
from src.application.management_service import ManagementSettings, WorkspaceBootstrapManifest
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_codec import decode_row, encode_row
from src.ui.prompt_registration_console import ConsoleSettings
from src.ui.workspace_bootstrap_console import (
    BootstrapConsoleError, BootstrapConsoleSettings, BootstrapSession, review_projection,
)
from tests import test_management_service as fixtures


class BootstrapConsoleTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.identity = replace(fixtures.identity(),
            audience='/projects/1042059373096/locations/asia-south1/services/ci-prompt-registrar')
        self.console = ConsoleSettings(IapSettings(self.identity.audience), 'https://console.run.app')
        self.settings = BootstrapConsoleSettings(WorkspaceBootstrapManifest(fixtures.WS,
            fixtures.SUBJECT, ('ADMIN',), 'explicit-bootstrap', self.now-timedelta(seconds=5),
            self.now+timedelta(minutes=4)))
        self.session = BootstrapSession(self.settings)
        self.service = fixtures.service(ManagementSettings(enabled=True, bootstrap=self.settings.manifest))
        self.addCleanup(self.service.io.client.close)
        self.service.verifier.authenticate = Mock(return_value=self.identity)
        self.service.io.submit_once = Mock(side_effect=fixtures.applied)
        self.factory = patch('src.ui.workspace_bootstrap_console.live_management').start()
        self.factory.return_value.__enter__.return_value = self.service
        self.addCleanup(patch.stopall)
        self.headers = {'x-goog-iap-jwt-assertion': 'not-retained-in-session'}

    def raw_manifest(self):
        return {key: value for key, value in review_projection(self.settings).items()
                if key in {'workspace_id', 'owner_subject', 'roles', 'manifest_id', 'created_at', 'expires_at'}}

    def submit(self, **changes):
        args = dict(headers=self.headers, identity=self.identity, console_settings=self.console,
            settings=self.settings, confirmation=fixtures.WS, now=self.now)
        return self.session.submit(**(args | changes))

    def test_default_disabled_does_not_require_manifest(self):
        with patch.dict('os.environ', {}, clear=True):
            self.assertIsNone(BootstrapConsoleSettings.from_environment())
        self.factory.assert_not_called()

    def test_enabled_configuration_requires_complete_exact_manifest(self):
        env = {'CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED': 'true'}
        with patch.dict('os.environ', env, clear=True):
            with self.assertRaises(BootstrapConsoleError):
                BootstrapConsoleSettings.from_environment()
        for changes in ({'roles': []}, {'roles': ['ADMIN', 'CREATOR']},
                        {'roles': ['ADMIN', 'ADMIN']}, {'extra': True},
                        {'owner_subject': 'bad subject'},
                        {'expires_at': (self.now+timedelta(minutes=10)).isoformat()},
                        {'created_at': self.now.replace(tzinfo=None).isoformat()}):
            with self.subTest(changes=changes), patch.dict('os.environ', env | {
                    'CI_WORKSPACE_BOOTSTRAP_MANIFEST_JSON': json.dumps(self.raw_manifest() | changes)}, clear=True):
                with self.assertRaises(BootstrapConsoleError):
                    BootstrapConsoleSettings.from_environment()

    def test_valid_server_configuration_and_duplicate_keys(self):
        raw = json.dumps(self.raw_manifest())
        env = dict(CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED='true',
            CI_WORKSPACE_BOOTSTRAP_MANIFEST_JSON=raw)
        with patch.dict('os.environ', env, clear=True):
            self.assertEqual(BootstrapConsoleSettings.from_environment(), self.settings)
        with patch.dict('os.environ', env | {
                'CI_WORKSPACE_BOOTSTRAP_MANIFEST_JSON': raw[:-1] + ',"roles":["ADMIN"]}'}, clear=True):
            with self.assertRaises(BootstrapConsoleError):
                BootstrapConsoleSettings.from_environment()

    def test_confirmation_expiry_scope_and_actor_deny_before_cloud(self):
        other = replace(self.identity, subject='accounts.google.com:other',
            principal=principal_for_subject(ISSUER, 'accounts.google.com:other'))
        for changes in ({'confirmation': 'different'}, {'now': self.settings.manifest.expires_at},
                        {'now': self.settings.manifest.created_at-timedelta(seconds=1)},
                        {'identity': other},
                        {'identity': replace(self.identity, audience=self.identity.audience+'-other')},
                        {'console_settings': replace(self.console, workspace_id=fixtures.WS)},
                        {'console_settings': replace(self.console, workspace_id=fixtures.WS, writes_enabled=True)},
                        {'settings': replace(self.settings, manifest=replace(self.settings.manifest, manifest_id='changed'))}):
            with self.subTest(changes=changes), self.assertRaises(BootstrapConsoleError):
                self.submit(**changes)
        self.assertFalse(self.session.attempted)
        self.factory.assert_not_called()

    def test_committed_bootstrap_is_existing_service_and_minimal_receipt(self):
        result = self.submit()
        self.assertEqual(result['native_status'], 'COMMITTED')
        self.assertEqual(result['workspace_id'], fixtures.WS)
        self.assertEqual(result['state'], 'APPLIED')
        plan = self.service.io.submit_once.call_args.args[0]
        values = {p.name: p.value for p in plan.config().query_parameters}
        self.assertEqual(decode_row(values['owner'], 'memberships')['roles'], ['ADMIN'])
        self.assertEqual(plan.kind, 'BOOTSTRAP')
        self.assertNotIn(self.headers['x-goog-iap-jwt-assertion'], str(vars(self.session)))
        self.assertNotIn('owner_subject', result)

    def test_second_entry_never_submits_even_after_failure_before_prepare(self):
        self.service.prepare = Mock(side_effect=RuntimeError('private provider body'))
        with self.assertRaises(RuntimeError):
            self.submit()
        self.assertTrue(self.session.attempted)
        with self.assertRaises(BootstrapConsoleError):
            self.submit()
        self.service.prepare.assert_called_once()
        self.service.io.submit_once.assert_not_called()

    def test_unknown_never_becomes_committed_or_new_attempt(self):
        self.service.io.submit_once.side_effect = lambda plan: ControlOutcome(plan.job_id, Status.UNKNOWN)
        result = self.submit()
        self.assertIsNone(result['state'])
        self.assertEqual(result['native_status'], 'UNKNOWN')
        with self.assertRaises(BootstrapConsoleError):
            self.submit()
        self.service.io.submit_once.assert_called_once()

    def test_original_recovery_uses_no_prepare_or_submit_and_accepts_expired_manifest(self):
        plan = self.service.prepare(self.headers, kind='BOOTSTRAP', workspace_id=fixtures.WS,
            command_id=self.settings.manifest.manifest_id)
        self.service.original_plan = Mock(return_value=plan)
        self.service.io.recover = Mock(return_value=fixtures.applied(plan, submitted=False))
        self.service.prepare = Mock(side_effect=AssertionError('no write preparation'))
        with patch('src.application.management_service.now', return_value=self.settings.manifest.expires_at):
            result = self.session.recover(self.headers, self.identity, self.console, self.settings)
        self.assertEqual(result['native_status'], 'COMMITTED')
        self.service.prepare.assert_not_called()
        self.service.io.submit_once.assert_not_called()
        with self.assertRaises(BootstrapConsoleError):
            self.submit()

    def test_malformed_committed_or_foreign_results_are_not_released(self):
        plan = self.service.prepare(self.headers, kind='BOOTSTRAP', workspace_id=fixtures.WS,
            command_id=self.settings.manifest.manifest_id)
        row = decode_row(plan.candidate_json, 'workspaces')
        wrong = encode_row(row | {'workspace_id': 'another'}, 'workspaces')
        for outcome in (ControlOutcome('foreign', Status.UNKNOWN),
                        ControlOutcome(plan.job_id, Status.COMMITTED),
                        replace(fixtures.applied(plan), rows=({'state': 'APPLIED',
                            'candidate': wrong, 'audit': plan.audit_json},)),
                        replace(fixtures.applied(plan), native_status=Status.UNKNOWN)):
            with self.subTest(outcome=outcome), self.assertRaises(Exception):
                self.session._result(outcome)

    def test_streamlit_manifest_review_requires_explicit_confirmation_and_retains_uncertainty(self):
        from streamlit.testing.v1 import AppTest
        env = dict(CI_PROMPT_REGISTRATION_CONSOLE_ENABLED='true',
            CI_IAP_EXPECTED_AUDIENCE=self.identity.audience,
            CI_PROMPT_REGISTRATION_ORIGIN=self.console.origin,
            CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED='true',
            CI_WORKSPACE_BOOTSTRAP_MANIFEST_JSON=json.dumps(self.raw_manifest()))
        self.service.io.submit_once.side_effect = lambda plan: ControlOutcome(plan.job_id, Status.UNKNOWN)
        with patch.dict('os.environ', env, clear=True), \
                patch('src.ui.prompt_registration_console.request_identity',
                      return_value=(self.identity, self.headers)):
            app = AppTest.from_file(str(Path(__file__).resolve().parents[1]
                / 'deploy/prompt-registration/app.py')).run(timeout=30)
            self.assertEqual(len(app.error), 0)
            self.factory.assert_not_called()
            app.text_input[0].set_value(fixtures.WS)
            app.button[0].click().run()
            self.assertEqual(len(app.error), 0)
            self.assertEqual(len(app.warning), 1)
            app.run()
            self.assertTrue(app.button[0].disabled)
            self.assertEqual(len(app.warning), 1)
            self.assertTrue(any('already attempted' in item.value for item in app.info))
            self.assertTrue(any('UNKNOWN' in item.value for item in app.json))
            self.service.io.submit_once.assert_called_once()

    def bootstrap_app(self):
        from streamlit.testing.v1 import AppTest
        return AppTest.from_file(str(Path(__file__).resolve().parents[1]
            / 'deploy/prompt-registration/app.py')).run(timeout=30)

    def ui_environment(self):
        return dict(CI_PROMPT_REGISTRATION_CONSOLE_ENABLED='true',
            CI_IAP_EXPECTED_AUDIENCE=self.identity.audience,
            CI_PROMPT_REGISTRATION_ORIGIN=self.console.origin,
            CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED='true',
            CI_WORKSPACE_BOOTSTRAP_MANIFEST_JSON=json.dumps(self.raw_manifest()))

    def test_expired_manifest_explains_disabled_create_without_cloud_calls(self):
        self.settings = replace(self.settings, manifest=replace(self.settings.manifest,
            created_at=self.now-timedelta(minutes=6), expires_at=self.now-timedelta(minutes=1)))
        with patch.dict('os.environ', self.ui_environment(), clear=True), \
                patch('src.ui.prompt_registration_console.request_identity',
                      return_value=(self.identity, self.headers)):
            app = self.bootstrap_app()
            self.assertTrue(app.button[0].disabled)
            self.assertFalse(app.button[1].disabled)
            self.assertIn('approval window expired', app.warning[0].value)
            self.assertIn(self.settings.manifest.expires_at.isoformat(), app.warning[0].value)
            self.factory.assert_not_called()

    def test_failed_submission_retains_safe_error_and_logs_without_private_body(self):
        self.service.prepare = Mock(side_effect=RuntimeError('private provider body credential-secret'))
        with patch.dict('os.environ', self.ui_environment(), clear=True), \
                patch('src.ui.prompt_registration_console.request_identity',
                      return_value=(self.identity, self.headers)), \
                self.assertLogs('src.ui.workspace_bootstrap_console', level='INFO') as logs:
            app = self.bootstrap_app()
            app.text_input[0].set_value(fixtures.WS)
            app.button[0].click().run()
            self.assertEqual(app.error[0].value, 'CONSOLE_OPERATION_UNAVAILABLE')
            app.run()
            self.assertEqual(app.error[0].value, 'CONSOLE_OPERATION_UNAVAILABLE')
            self.assertTrue(app.button[0].disabled)
            self.assertNotIn('credential-secret', str(app.session_state))
            self.assertNotIn('credential-secret', str(logs.output))
            self.assertNotIn(self.headers['x-goog-iap-jwt-assertion'], str(logs.output))
            self.assertTrue(any('phase=failed' in line for line in logs.output))
            self.service.prepare.assert_called_once()
            self.service.io.submit_once.assert_not_called()

    def test_committed_receipt_survives_rerun_but_owner_change_withholds_it(self):
        with patch.dict('os.environ', self.ui_environment(), clear=True), \
                patch('src.ui.prompt_registration_console.request_identity',
                      return_value=(self.identity, self.headers)) as authenticate:
            app = self.bootstrap_app()
            app.text_input[0].set_value(fixtures.WS)
            app.button[0].click().run()
            app.run()
            self.assertTrue(any('transaction verified' in item.value for item in app.success))
            self.assertTrue(any('COMMITTED' in item.value for item in app.json))
            self.service.io.submit_once.assert_called_once()
            other = replace(self.identity, subject='accounts.google.com:other',
                principal=principal_for_subject(ISSUER, 'accounts.google.com:other'))
            authenticate.return_value = (other, self.headers)
            app.run()
            self.assertEqual(app.error[0].value, 'BOOTSTRAP_OWNER_REQUIRED')
            self.assertFalse(any('COMMITTED' in item.value for item in app.json))


if __name__ == '__main__':
    unittest.main()
