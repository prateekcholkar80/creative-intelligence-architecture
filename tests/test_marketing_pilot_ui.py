from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock,patch
from streamlit.testing.v1 import AppTest

class MarketingUiTests(TestCase):
    def app(self):return AppTest.from_file(str(Path(__file__).resolve().parents[1]/'deploy/marketing/app.py'))
    def test_unauthenticated_page_never_exposes_identity_or_campaign_input(self):
        pilot=Mock();pilot.inspect.side_effect=ValueError('private detail')
        with patch('src.ui.marketing_pilot.pilot',return_value=pilot),patch('src.ui.marketing_pilot.iap_headers',return_value={}):
            app=self.app().run()
        self.assertFalse(app.exception);self.assertTrue(app.chat_input[0].disabled)
        pilot.setup.assert_not_called();pilot.runtime.assert_not_called()
    def test_admin_creator_assignment_requires_explicit_button_click(self):
        pilot=Mock();pilot.inspect.return_value=(None,None,dict(roles=['ADMIN']),None,None)
        with patch('src.ui.marketing_pilot.pilot',return_value=pilot),patch('src.ui.marketing_pilot.iap_headers',return_value={}):
            app=self.app().run();pilot.setup.assert_not_called()
            next(b for b in app.button if b.label=='Enable my campaign creator access').click().run()
        self.assertFalse(app.exception);pilot.setup.assert_called_once_with({},'CREATOR',context='default')
    def test_viewer_cannot_assign_role_or_start_conversation(self):
        pilot=Mock();pilot.inspect.return_value=(None,None,dict(roles=['VIEWER']),None,None)
        with patch('src.ui.marketing_pilot.pilot',return_value=pilot),patch('src.ui.marketing_pilot.iap_headers',return_value={}):
            app=self.app().run()
        self.assertFalse(app.button);pilot.setup.assert_not_called();pilot.runtime.assert_not_called()

    def test_evaluation_conversation_requires_explicit_create_and_fixed_context(self):
        pilot=Mock();pilot.inspect.return_value=(None,None,dict(roles=['CREATOR']),None,None)
        with patch('src.ui.marketing_pilot.pilot',return_value=pilot),patch('src.ui.marketing_pilot.iap_headers',return_value={}):
            app=self.app();app.query_params['campaign']='rejuvenate-evaluation-v1';app.run()
            pilot.setup.assert_not_called()
            next(b for b in app.button if b.label=='Create my private Rejuvenate evaluation conversation').click().run()
        self.assertFalse(app.exception)
        pilot.setup.assert_called_once_with({},'CONVERSATION',context='rejuvenate-evaluation-v1')

    def test_arbitrary_conversation_parameter_denies_before_runtime_or_setup(self):
        pilot=Mock()
        with patch('src.ui.marketing_pilot.pilot',return_value=pilot),patch('src.ui.marketing_pilot.iap_headers',return_value={}):
            app=self.app();app.query_params['campaign']='someone-elses-conversation';app.run()
        self.assertFalse(app.exception);pilot.inspect.assert_not_called();pilot.setup.assert_not_called();pilot.runtime.assert_not_called()

    def test_retained_original_submission_cannot_be_rebound_by_changing_url(self):
        pilot=Mock()
        pending=dict(message='original unfinished text',retry_key='original-key')
        with patch('src.ui.marketing_pilot.pilot',return_value=pilot),patch('src.ui.marketing_pilot.iap_headers',return_value={}):
            app=self.app();app.session_state['protected_pending_turn']=pending
            app.query_params['campaign']='rejuvenate-evaluation-v1';app.run()
        self.assertFalse(app.exception);self.assertTrue(any('new tab' in x.value for x in app.info))
        self.assertEqual(app.session_state['protected_pending_turn'],pending)
        pilot.inspect.assert_not_called();pilot.runtime.assert_not_called();pilot.setup.assert_not_called()
