from dataclasses import replace
from unittest import TestCase
from unittest.mock import patch
from src.ui.grounded_planner_registration_console import GroundedRegistrationSession
from src.ui.prompt_registration_console import ConsoleSettings,ConsoleError
from src.application.identity import IapSettings
from tests import test_grounded_planner_registration as fixture
from tests.test_prompt_registration_console import IDENTITY
from tests.test_access_reader import WORKSPACE
from tests.test_prompt_contract import NOW


class GroundedConsoleTests(TestCase):
    def setUp(self):
        self.f=fixture.GroundedRegistrationTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.f.f.identity.authenticate.return_value=IDENTITY
        self.session=GroundedRegistrationSession(self.f.target)
        self.session.review_store=self.f.operator.reviews
        self.settings=ConsoleSettings(IapSettings(IDENTITY.audience),'https://console.run.app',WORKSPACE,False)
        clock=patch('src.ui.grounded_planner_registration_console.datetime').start();clock.now.return_value=NOW
    def test_review_reruns_keep_original_command_and_disabled_submit_never_writes(self):
        self.session.prepare_exact(self.f.operator,self.f.request,self.f.f.headers,IDENTITY,self.settings)
        with self.assertRaises(ConsoleError):self.session.prepare_exact(self.f.operator,self.f.request,self.f.f.headers,IDENTITY,self.settings)
        with self.assertRaises(ConsoleError):self.session.submit(self.f.operator,self.f.f.headers,IDENTITY,self.settings,
            confirmed_sha256=self.session.command.approved_preview_sha256,now=NOW)
        self.assertEqual(self.f.f.writer.calls,[])
    def test_explicit_target_window_and_exact_hash_import_draft_only(self):
        self.session.prepare_exact(self.f.operator,self.f.request,self.f.f.headers,IDENTITY,self.settings)
        result=self.session.submit(self.f.operator,self.f.f.headers,IDENTITY,replace(self.settings,writes_enabled=True),
            confirmed_sha256=self.session.command.approved_preview_sha256,now=NOW)
        self.assertEqual(result['status'],'DRAFT');self.assertEqual(len(self.f.f.writer.calls),1)
        self.assertFalse(self.settings.writes_enabled)

    def test_real_streamlit_scoped_window_prepares_then_requires_hash_before_import(self):
        from contextlib import contextmanager
        from streamlit.testing.v1 import AppTest
        from src.ui.grounded_planner_registration_console import SESSION_KEY,WRITES_ENV
        @contextmanager
        def operator(session,settings,*,enabled):
            self.f.operator.reviews=session.review_store
            yield self.f.operator,self.f.request
        app=AppTest.from_string('''
import streamlit as st
from src.ui.grounded_planner_registration_console import render
from tests.test_prompt_registration_console import IDENTITY
render(st,{},IDENTITY,st.session_state['settings'])
''')
        app.session_state['settings']=self.settings
        with patch('src.ui.grounded_planner_registration_console.target_from_environment',return_value=self.f.target), \
             patch('src.ui.grounded_planner_registration_console.console_operator',operator),patch.dict('os.environ',{WRITES_ENV:'true'}):
            app.run();self.assertFalse(app.exception);self.assertEqual(self.f.f.writer.calls,[])
            next(b for b in app.button if b.label=='Prepare exact-version DRAFT review').click().run()
            self.assertFalse(app.exception);self.assertEqual(self.f.f.writer.calls,[])
            sha=app.session_state[SESSION_KEY].command.approved_preview_sha256
            app.text_input[0].set_value(sha)
            next(b for b in app.button if b.label=='Register as DRAFT').click().run()
            self.assertFalse(app.exception);self.assertEqual(len(self.f.f.writer.calls),1)
        self.assertFalse(self.settings.writes_enabled)
