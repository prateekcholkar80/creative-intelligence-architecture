"""Protected clarification review reruns, fresh authority and exact target gates."""
from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from src.application.clarification_prompt_registration import ClarificationPromptRegistrationOperator
from src.application.identity import IapSettings
from src.ui.clarification_registration_console import (
    ClarificationConsoleConfig, ClarificationRegistrationSession, KEYS, SESSION_KEY,
)
from src.ui.prompt_registration_console import ConsoleError, ConsoleSettings
from tests.test_clarification_prompt_registration import ClarificationRegistrationTests
from tests.test_prompt_registration_console import IDENTITY
from tests.test_prompt_contract import NOW
from tests.test_access_reader import WORKSPACE
from tests.test_prompt_registration_command import decision


class ClarificationConsoleTests(TestCase):
    def setUp(self):
        self.fixture = ClarificationRegistrationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.f = self.fixture
        self.f.f.identity.authenticate.return_value = IDENTITY
        self.config = ClarificationConsoleConfig(self.f.policy.request,
            self.f.base.vertex_prompt_id, self.f.base.vertex_version_id,
            ((KEYS[0], '1234567890', '1'), (KEYS[1], '9876543210', '1')))
        self.settings = ConsoleSettings(IapSettings(IDENTITY.audience),
            'https://console.run.app', WORKSPACE, True)
        self.session = ClarificationRegistrationSession(self.config, KEYS[0])

    def operator(self, **changes):
        args = dict(policy_request=self.f.policy.request, policy_reader=self.f.reader,
            settings_snapshot=self.f.base, prompt_key=KEYS[0], clock=lambda: NOW,
            review_store=self.session.review_store, preview_service=self.f.f.preview,
            command_service=self.f.f.service, identity_adapter=self.f.f.identity)
        args.update(changes)
        return ClarificationPromptRegistrationOperator(**args)

    def prepare(self):
        self.session.prepare_exact(self.operator(), self.f.request, self.f.f.headers,
            IDENTITY, self.settings, now=NOW)

    def submit(self, **changes):
        args = dict(operator=self.operator(), headers=self.f.f.headers, identity=IDENTITY,
            settings=self.settings, confirmed_sha256=self.session.command.approved_preview_sha256,
            now=NOW)
        args.update(changes)
        return self.session.submit(**args)

    def env_config(self):
        return json.dumps(dict(policy_id=self.config.policy.policy_id,
            policy_version=self.config.policy.version, policy_sha256=self.config.policy.policy_sha256,
            settings_prompt_id=self.config.settings_prompt_id,
            settings_version_id=self.config.settings_version_id,
            targets={k:dict(prompt_id=p,version_id=v) for k,p,v in self.config.targets}))

    def test_configuration_exact_allowlist_and_resource_validation(self):
        with patch.dict('os.environ', {'CI_CLARIFICATION_REGISTRATION_CONFIG':self.env_config()}):
            self.assertEqual(ClarificationConsoleConfig.from_environment(), self.config)
        for raw in ('{}', '[]', 'x'*8193, self.env_config().replace('1234567890','latest'),
                    self.env_config().replace(KEYS[0],'untrusted-key')):
            with self.subTest(raw=raw[:30]), patch.dict('os.environ',
                    {'CI_CLARIFICATION_REGISTRATION_CONFIG':raw}), self.assertRaises(ConsoleError):
                ClarificationConsoleConfig.from_environment()

    def test_fresh_operator_recovers_issued_review_without_open_clients_or_tokens(self):
        self.prepare()
        command = self.session.command
        result = self.submit()
        self.assertEqual(result['status'], 'DRAFT')
        self.assertIs(self.session.command, command)
        self.assertNotIn(self.f.f.headers['x-goog-iap-jwt-assertion'], str(vars(self.session)))
        self.f.f.access.results.extend([decision() for _ in range(2)])
        self.assertEqual(self.submit()['native_job_id'], result['native_job_id'])
        self.assertEqual(self.f.f.writer.calls[0], self.f.f.writer.calls[1])

    def test_separate_session_cannot_replay_another_issued_review(self):
        self.prepare()
        other = ClarificationRegistrationSession(self.config, KEYS[0])
        with self.assertRaises(ValueError):
            self.submit(operator=self.operator(review_store=other.review_store))
        self.assertEqual(self.f.f.writer.calls, [])

    def test_wrong_target_and_replacement_review_deny(self):
        with self.assertRaises(ConsoleError):
            self.session.prepare_exact(self.operator(), replace(self.f.request,prompt_id='42'),
                self.f.f.headers, IDENTITY, self.settings, now=NOW)
        self.prepare()
        with self.assertRaises(ConsoleError):
            self.prepare()
        self.assertEqual(self.f.f.writer.calls, [])

    def test_current_settings_policy_identity_and_write_switch_deny(self):
        self.prepare()
        for kwargs in ({'settings':replace(self.settings,writes_enabled=False)},
                       {'confirmed_sha256':'0'*64},
                       {'identity':replace(IDENTITY,audience=IDENTITY.audience+'-other')}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ConsoleError):
                self.submit(**kwargs)
        with self.assertRaises(ValueError):
            self.submit(operator=self.operator(settings_snapshot=replace(self.f.base,model_id='changed-model')))
        self.f.reader.side_effect = ValueError('revoked')
        with self.assertRaises(ValueError):
            self.submit()
        self.assertEqual(self.f.f.writer.calls, [])

    def test_uncertain_submit_retains_original_command_and_policy_binding(self):
        self.prepare()
        command = self.session.command
        with patch.object(ClarificationPromptRegistrationOperator,'register',side_effect=RuntimeError('lost')):
            with self.assertRaises(RuntimeError): self.submit()
        self.assertTrue(self.session.attempted)
        self.assertIs(self.session.command, command)
        self.assertIs(self.session.review_store.issued[command.approved_preview_sha256][0],command)
        self.assertEqual(self.f.f.writer.calls, [])

    def test_streamlit_selection_rerun_retains_review_and_disables_writes(self):
        from streamlit.testing.v1 import AppTest
        env = dict(CI_PROMPT_REGISTRATION_CONSOLE_ENABLED='true',
            CI_IAP_EXPECTED_AUDIENCE=IDENTITY.audience,
            CI_PROMPT_REGISTRATION_ORIGIN=self.settings.origin,
            CI_PROMPT_REGISTRATION_WORKSPACE_ID=WORKSPACE,
            CI_CLARIFICATION_REGISTRATION_ENABLED='true',
            CI_CLARIFICATION_REGISTRATION_CONFIG=self.env_config())
        @contextmanager
        def composed(session,settings,*,execution_enabled):
            self.assertFalse(execution_enabled)
            yield self.operator(review_store=session.review_store), self.f.request
        with patch.dict('os.environ',env,clear=True), patch(
                'src.ui.prompt_registration_console.request_identity',return_value=(IDENTITY,self.f.f.headers)), patch(
                'src.ui.clarification_registration_console.console_operator',side_effect=composed):
            app=AppTest.from_file(str(Path(__file__).resolve().parents[1]/'deploy/prompt-registration/app.py')).run()
            app.selectbox[0].select(KEYS[0]).run()
            app.button[0].click().run()
            self.assertEqual(len(app.error),0)
            session=app.session_state[SESSION_KEY][KEYS[0]]
            command=session.command
            app.text_input[0].set_value(command.approved_preview_sha256).run()
            self.assertIs(app.session_state[SESSION_KEY][KEYS[0]].command,command)
            self.assertTrue(app.button[1].disabled)
            app.selectbox[0].select(KEYS[1]).run()
            self.assertIsNone(app.session_state[SESSION_KEY][KEYS[1]].command)
            app.selectbox[0].select(KEYS[0]).run()
            self.assertIs(app.session_state[SESSION_KEY][KEYS[0]].command,command)
            changed=json.loads(self.env_config());changed['targets'][KEYS[0]]['version_id']='2'
            with patch.dict('os.environ',{'CI_CLARIFICATION_REGISTRATION_CONFIG':json.dumps(changed)}):
                app.run()
                self.assertEqual(app.error[0].value,'CONSOLE_SESSION_CONFIGURATION_CHANGED')
        self.assertEqual(self.f.f.writer.calls,[])

    def test_streamlit_enabled_submit_uses_fresh_operator_and_exact_issued_command(self):
        from streamlit.testing.v1 import AppTest
        env = dict(CI_PROMPT_REGISTRATION_CONSOLE_ENABLED='true',
            CI_IAP_EXPECTED_AUDIENCE=IDENTITY.audience,
            CI_PROMPT_REGISTRATION_ORIGIN=self.settings.origin,
            CI_PROMPT_REGISTRATION_WORKSPACE_ID=WORKSPACE,
            CI_PROMPT_REGISTRATION_WRITES_ENABLED='true',
            CI_CLARIFICATION_REGISTRATION_ENABLED='true',
            CI_CLARIFICATION_REGISTRATION_CONFIG=self.env_config())
        modes=[]
        @contextmanager
        def composed(session,settings,*,execution_enabled):
            modes.append(execution_enabled)
            yield self.operator(review_store=session.review_store), self.f.request
        with patch.dict('os.environ',env,clear=True), patch(
                'src.ui.prompt_registration_console.request_identity',return_value=(IDENTITY,self.f.f.headers)), patch(
                'src.ui.clarification_registration_console.console_operator',side_effect=composed):
            app=AppTest.from_file(str(Path(__file__).resolve().parents[1]/'deploy/prompt-registration/app.py')).run()
            app.selectbox[0].select(KEYS[0]).run()
            app.button[0].click().run()
            command=app.session_state[SESSION_KEY][KEYS[0]].command
            app.text_input[0].set_value(command.approved_preview_sha256)
            app.button[1].click().run()
            self.assertEqual(len(app.error),0)
            self.assertEqual(app.success[-1].value,'DRAFT registration verified in BigQuery')
            self.assertIs(app.session_state[SESSION_KEY][KEYS[0]].command,command)
        self.assertEqual(modes,[False,True])
        self.assertEqual(len(self.f.f.writer.calls),1)
