"""Console identity/origin/review gates; no cloud or model calls."""
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock, patch
import unittest

from src.application.identity import ASSERTION_HEADER, IapSettings
from src.ui.prompt_registration_console import (
    ConsoleError, ConsoleSettings, RegistrationSession, identity_projection,
    request_identity, safe_error,
)
from tests.test_access_reader import EPOCH, IDENTITY as FIXTURE_IDENTITY, WORKSPACE
from tests import test_planner_prompt_registration as operator_fixtures
from tests.test_prompt_registration_command import decision
from src.application.identity import principal_for_subject
from tests.test_identity import signer_and_public_key

IDENTITY = replace(FIXTURE_IDENTITY,
    audience=FIXTURE_IDENTITY.audience.replace('123456789', '1042059373096'))

class ConsoleRequestTests(unittest.TestCase):
    def setUp(self):
        self.settings = ConsoleSettings(IapSettings(IDENTITY.audience), 'https://console.run.app')
        patch('src.application.identity.time.time', return_value=EPOCH).start()
        self.addCleanup(patch.stopall)

    def test_disabled_configuration_denies_before_identity(self):
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaisesRegex(ConsoleError, 'denied'):
                ConsoleSettings.from_environment()

    def test_invalid_origins_project_and_write_scope_deny(self):
        for origin in ('http://console.run.app', 'https://console.run.app/',
                       'https://console.run.app?x=1', 'https://user@console.run.app'):
            with self.subTest(origin=origin), self.assertRaises(ConsoleError):
                ConsoleSettings(self.settings.iap, origin)
        with self.assertRaises(ConsoleError):
            ConsoleSettings(self.settings.iap, self.settings.origin, writes_enabled=True)
        with self.assertRaises(ConsoleError):
            ConsoleSettings(IapSettings(IDENTITY.audience.replace('1042059373096', '123')),
                            self.settings.origin)

    def test_missing_duplicate_and_wrong_origin_deny_before_auth(self):
        with patch('src.ui.prompt_registration_console.IapIdentityAdapter') as verifier:
            for headers in ({}, {ASSERTION_HEADER: 'secret', 'origin': 'https://wrong.run.app'},
                    {ASSERTION_HEADER: 'secret', ASSERTION_HEADER.upper(): 'second',
                     'origin': self.settings.origin}):
                with self.assertRaises(ConsoleError):
                    request_identity(headers, self.settings)
            verifier.assert_not_called()

    def test_server_headers_verified_and_no_token_in_identity_projection(self):
        with patch('src.ui.prompt_registration_console.IapIdentityAdapter') as verifier:
            verifier.return_value.authenticate.return_value = IDENTITY
            identity, headers = request_identity({ASSERTION_HEADER: 'secret',
                'origin': self.settings.origin, 'x-user-role': 'ADMIN'}, self.settings)
            self.assertEqual(headers, {ASSERTION_HEADER: 'secret'})
            self.assertNotIn('secret', str(identity_projection(identity)))

    def test_repeated_streamlit_headers_deny(self):
        headers = Mock()
        headers.items.return_value = [(ASSERTION_HEADER, 'a'), ('origin', self.settings.origin)]
        headers.get_all.side_effect = [['a', 'b'], [self.settings.origin]]
        with self.assertRaises(ConsoleError):
            request_identity(headers, self.settings)

    def test_safe_errors_omit_provider_bodies(self):
        self.assertEqual(safe_error(RuntimeError('secret')), 'CONSOLE_OPERATION_UNAVAILABLE')
        self.assertEqual(safe_error(ConsoleError('TOKEN secret')), 'CONSOLE_OPERATION_UNAVAILABLE')
        self.assertEqual(safe_error(ConsoleError('CONSOLE_DISABLED')), 'CONSOLE_DISABLED')

    def test_real_es256_assertion_and_tampering_at_console_boundary(self):
        import base64
        import json
        from types import SimpleNamespace
        from google.auth import jwt
        signer, public = signer_and_public_key()
        claims = dict(iss=IDENTITY.issuer, sub=IDENTITY.subject, aud=IDENTITY.audience,
                      iat=IDENTITY.issued_at, exp=IDENTITY.expires_at)
        assertion = jwt.encode(signer, claims).decode('ascii')
        request = Mock(return_value=SimpleNamespace(status=200,
            data=json.dumps({'fixture-key': public}).encode()))
        with patch('src.application.identity._BoundedKeyRequest', return_value=request), \
                patch('google.auth._helpers.utcnow',
                      return_value=self.fixture_clock()):
            identity, _ = request_identity({ASSERTION_HEADER: assertion,
                'origin': self.settings.origin}, self.settings)
            self.assertEqual(identity, IDENTITY)
            parts = assertion.split('.')
            claims['sub'] = 'attacker'
            parts[1] = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip('=')
            from src.application.identity import IdentityError
            with self.assertRaises(IdentityError):
                request_identity({ASSERTION_HEADER: '.'.join(parts),
                    'origin': self.settings.origin}, self.settings)

    @staticmethod
    def fixture_clock():
        from datetime import datetime, timezone
        return datetime.fromtimestamp(EPOCH, timezone.utc).replace(tzinfo=None)

    def test_streamlit_disabled_startup_has_no_cloud_composition(self):
        from streamlit.testing.v1 import AppTest
        with patch.dict('os.environ', {}, clear=True), \
                patch('src.ui.prompt_registration_console.live_operator') as cloud:
            app = AppTest.from_file(str(Path(__file__).resolve().parents[1]
                / 'deploy/prompt-registration/app.py')).run(timeout=30)
        self.assertEqual(len(app.exception), 0)
        self.assertEqual([error.value for error in app.error], ['CONSOLE_DISABLED'])
        cloud.assert_not_called()


class ConsoleSessionTests(unittest.TestCase):
    def setUp(self):
        # Reuse actual command/service fixtures without inheriting unrelated tests.
        self.fixture = operator_fixtures.PlannerPromptRegistrationTests(
            methodName='test_prepare_binds_verified_actor_and_rechecks_access')
        self.fixture.setUp()
        self.fixture.identity.authenticate.return_value = IDENTITY
        self.addCleanup(self.fixture.doCleanups)
        self.settings = ConsoleSettings(IapSettings(IDENTITY.audience), 'https://console.run.app',
                                        WORKSPACE, True)
        self.session = RegistrationSession()
        from tests.test_prompt_contract import NOW
        self.now = NOW
        # Console fixes the real live prompt identity; this fixture uses test pins.
        patch('src.ui.prompt_registration_console.planner_request',
              return_value=self.fixture.request).start()

    def prepare(self):
        return self.session.prepare(self.fixture.operator, self.fixture.headers,
            IDENTITY, self.settings, now=self.now)

    def submit(self, **changes):
        args = dict(operator=self.fixture.operator, headers=self.fixture.headers,
            identity=IDENTITY, settings=self.settings,
            confirmed_sha256=self.session.command.approved_preview_sha256, now=self.now)
        args.update(changes)
        return self.session.submit(**args)

    def test_prepare_requires_workspace_and_current_actor(self):
        with self.assertRaises(ConsoleError):
            self.session.prepare(self.fixture.operator, self.fixture.headers,
                IDENTITY, replace(self.settings, workspace_id=None, writes_enabled=False), now=self.now)
        self.assertEqual(self.fixture.writer.calls, [])
        review = self.prepare()
        self.assertEqual(review['proposed_status'], 'DRAFT')
        self.assertEqual(self.session.principal_id, IDENTITY.principal.principal_id)
        self.assertNotIn(self.fixture.headers[ASSERTION_HEADER], str(vars(self.session)))

    def test_cannot_replace_original_review(self):
        self.prepare()
        command = self.session.command
        with self.assertRaises(ConsoleError):
            self.prepare()
        self.assertIs(self.session.command, command)

    def test_write_opt_in_hash_expiry_and_scope_gate_writer(self):
        self.prepare()
        for changes in ({'settings': replace(self.settings, writes_enabled=False)},
                        {'confirmed_sha256': '0'*64},
                        {'now': self.now+timedelta(minutes=5)},
                        {'settings': replace(self.settings, workspace_id='other')}):
            with self.subTest(changes=changes), self.assertRaises(ConsoleError):
                self.submit(**changes)
        self.assertFalse(self.session.attempted)
        self.assertEqual(self.fixture.writer.calls, [])

    def test_identity_switch_and_wrong_audience_deny(self):
        self.prepare()
        other = replace(IDENTITY, subject='other', principal=principal_for_subject(IDENTITY.issuer, 'other'))
        with self.assertRaises(ConsoleError):
            self.submit(identity=other)
        with self.assertRaises(ConsoleError):
            self.submit(identity=replace(IDENTITY, audience=IDENTITY.audience+'-other'))
        self.assertEqual(self.fixture.writer.calls, [])

    def test_success_is_existing_verified_draft_current_and_history(self):
        self.prepare()
        result = self.submit()
        self.assertEqual(result['status'], 'DRAFT')
        self.assertEqual(result['history_rows'], 1)
        self.assertTrue(self.session.attempted)
        self.assertEqual(len(self.fixture.writer.calls), 1)

    def test_uncertain_exception_retains_original_command_without_retry(self):
        self.prepare()
        command = self.session.command
        with patch.object(self.fixture.operator, 'register', side_effect=RuntimeError('secret')) as call:
            with self.assertRaises(RuntimeError):
                self.submit()
            call.assert_called_once()
        self.assertTrue(self.session.attempted)
        self.assertIs(self.session.command, command)
        with self.assertRaises(ConsoleError):
            self.prepare()

    def test_same_session_recovers_original_verified_state(self):
        self.prepare()
        command = self.session.command
        self.submit()
        self.fixture.access.results.extend([decision() for _ in range(2)])
        result = self.submit()
        self.assertIs(self.session.command, command)
        self.assertEqual(result['status'], 'DRAFT')
        self.assertEqual(self.fixture.writer.calls[0], self.fixture.writer.calls[1])

    def test_streamlit_rerun_retains_review_and_disabled_writes(self):
        from streamlit.testing.v1 import AppTest
        environment = dict(CI_PROMPT_REGISTRATION_CONSOLE_ENABLED='true',
            CI_IAP_EXPECTED_AUDIENCE=IDENTITY.audience,
            CI_PROMPT_REGISTRATION_ORIGIN=self.settings.origin,
            CI_PROMPT_REGISTRATION_WORKSPACE_ID=WORKSPACE)
        with patch.dict('os.environ', environment, clear=True), \
                patch('src.ui.prompt_registration_console.request_identity',
                    return_value=(IDENTITY, self.fixture.headers)), \
                patch('src.ui.prompt_registration_console.live_operator') as cloud:
            cloud.return_value.__enter__.return_value = self.fixture.operator
            app = AppTest.from_file(str(Path(__file__).resolve().parents[1]
                / 'deploy/prompt-registration/app.py')).run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            app.button[0].click().run()
            self.assertEqual(len(app.error), 0)
            session = app.session_state['ci_prompt_registration_console_v1']
            command = session.command
            app.text_input[0].set_value(command.approved_preview_sha256).run()
            self.assertIs(app.session_state['ci_prompt_registration_console_v1'].command, command)
            self.assertEqual(len(app.error), 0)
            self.assertTrue(app.button[1].disabled)
            self.assertEqual(self.fixture.writer.calls, [])

    def test_streamlit_without_workspace_never_composes_cloud_clients(self):
        from streamlit.testing.v1 import AppTest
        environment = dict(CI_PROMPT_REGISTRATION_CONSOLE_ENABLED='true',
            CI_IAP_EXPECTED_AUDIENCE=IDENTITY.audience,
            CI_PROMPT_REGISTRATION_ORIGIN=self.settings.origin)
        with patch.dict('os.environ', environment, clear=True), \
                patch('src.ui.prompt_registration_console.request_identity',
                    return_value=(IDENTITY, self.fixture.headers)), \
                patch('src.ui.prompt_registration_console.live_operator') as cloud:
            app = AppTest.from_file(str(Path(__file__).resolve().parents[1]
                / 'deploy/prompt-registration/app.py')).run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.button), 0)
            self.assertEqual(len(app.error), 0)
            cloud.assert_not_called()


if __name__ == '__main__':
    unittest.main()
