import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

from src.application.campaign_chat import CampaignResult, Interpretation
from tests import test_campaign_chat as chat_fixtures
from tests.test_campaign_chat import catalog, preferences


class ChatUITests(unittest.TestCase):
    def setUp(self):
        self.service = Mock()
        self.service.catalog.return_value = catalog()
        self.service.interpret.return_value = Interpretation(preferences())
        self.patch = patch('src.ui.campaign_chat.CampaignChatService', return_value=self.service)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.app = AppTest.from_file(str(Path(__file__).resolve().parents[1]/'app.py'), default_timeout=15)

    def run_prompt(self, text='Create a conversion campaign'):
        self.app.run()
        self.app.chat_input[0].set_value(text).run()
        self.assertFalse(self.app.exception)

    def test_connected_catalog_renders_chat_and_does_not_generate_on_page_load(self):
        self.app.run()
        self.assertFalse(self.app.exception)
        self.assertEqual(self.app.title[0].value, 'From product to campaign')
        self.assertFalse(self.app.chat_input[0].disabled)
        self.service.generate.assert_not_called()

    def test_catalog_failure_disables_chat_without_factual_fallback(self):
        self.service.catalog.side_effect = RuntimeError('raw secret')
        self.app.run()
        self.assertFalse(self.app.exception)
        self.assertTrue(self.app.chat_input[0].disabled)
        self.assertNotIn('raw secret', self.app.error[0].value)
        self.service.interpret.assert_not_called()

    def test_missing_preferences_are_requested_without_generation(self):
        self.service.interpret.return_value = Interpretation(dict(preferences(), audience='', channel=''))
        self.run_prompt()
        self.assertIn('audience', self.app.info[0].value)
        submit = next(b for b in self.app.button if b.label=='Generate preview')
        submit.click().run()
        self.assertFalse(self.app.exception)
        self.assertTrue(self.app.warning)
        self.service.generate.assert_not_called()

    def test_unsupported_claim_request_has_no_generate_button(self):
        self.service.interpret.return_value = Interpretation(preferences(), ('BENEFIT_CLAIMS',))
        self.run_prompt('Say this serum cures acne')
        self.assertIn('benefit claims', self.app.warning[0].value)
        self.assertFalse(any(b.label=='Generate preview' for b in self.app.button))
        self.service.generate.assert_not_called()

    def test_followup_receives_previous_preferences_and_unsupported_features(self):
        self.service.interpret.return_value = Interpretation(preferences(), ('BENEFIT_CLAIMS',))
        self.run_prompt()
        self.app.chat_input[0].set_value('Use email').run()
        self.service.interpret.assert_called_with('Use email', preferences(), ('BENEFIT_CLAIMS',))

    def test_generate_and_rerun_do_not_repeat_expensive_operation(self):
        self.service.generate.return_value = CampaignResult(dict(status='FAILED',
            request_id='request', error=dict(code='CLOUD_UNAVAILABLE', message='Cloud unavailable')))
        self.run_prompt()
        next(b for b in self.app.button if b.label=='Generate preview').click().run()
        self.assertFalse(self.app.exception)
        self.assertEqual(self.app.error[0].value, 'Cloud unavailable')
        self.app.run()
        self.service.generate.assert_called_once()

    def test_success_displays_banner_brief_downloads_and_review_status(self):
        fixture = chat_fixtures.ChatWorkflowTests()
        fixture.setUp()
        try:
            result = fixture.service.generate(fixture.request, 'session', 'request')
        finally:
            fixture.doCleanups()
        self.service.generate.return_value = result
        self.run_prompt()
        next(b for b in self.app.button if b.label=='Generate preview').click().run()
        self.assertFalse(self.app.exception)
        self.assertTrue(any('awaiting human review' in element.value for element in self.app.caption))
        self.assertEqual(len(self.app.get('download_button')), 3)
        self.app.run()
        self.service.generate.assert_called_once()

    def test_new_conversation_clears_session_history(self):
        self.run_prompt()
        old_session = self.app.session_state['session_id']
        next(b for b in self.app.button if b.label=='New conversation').click().run()
        self.assertFalse(self.app.exception)
        self.assertEqual(self.app.session_state['messages'], [])
        self.assertIsNone(self.app.session_state['pending'])
        self.assertNotEqual(self.app.session_state['session_id'], old_session)


if __name__ == '__main__':
    unittest.main()
