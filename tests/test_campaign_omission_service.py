from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from src.application import campaign_omission_service as module


class OmissionServiceTests(TestCase):
    def instance(self):
        submission=Mock(capture_history=True);intake=Mock()
        actor=Mock();actor.principal.principal_id='pilot'
        intake._actor.return_value=(actor,'access')
        with patch.object(module,'DocumentStore') as store:
            service=module.OmissionTurnService(submission=submission,intake=intake)
        service.store.submission.return_value=dict(selection=dict(choice='OMIT_UNSUPPORTED_CONTENT',status='REQUEST_CHANGE_PENDING'),file=None)
        service.store.read.return_value=None
        return service

    def test_existing_unknown_native_slot_never_resubmits_or_invents_commit(self):
        service=self.instance()
        with patch.object(module,'read_policy'):
            receipt=service.apply({},workspace_id='w',conversation_id='c',submission_id='a'*64)
        self.assertEqual(receipt.native_status,'UNKNOWN')
        service.submission.submit.assert_not_called()

    def test_only_explicit_stored_omission_can_create_new_turn(self):
        service=self.instance();service.store.submission.return_value['selection']['choice']='PROVIDE_EVIDENCE'
        with self.assertRaises(ValueError):service.apply({},workspace_id='w',conversation_id='c',submission_id='a'*64)
        service.submission.submit.assert_not_called()

    def test_fresh_action_uses_server_context_and_stable_retry_key(self):
        service=self.instance();service.submission.io.original_job.side_effect=NotFound('missing')
        context=Mock(message='explicit omission',value={'selection':{'sku':'CI-SKU-0001'}})
        with patch.object(module,'read_omission',return_value=context),patch.object(module,'read_policy'):
            service.apply({},workspace_id='w',conversation_id='c',submission_id='a'*64)
        call=service.submission.submit.call_args.kwargs
        self.assertEqual(call['retry_key'],'omission-v1-'+'a'*64)
        self.assertEqual(call['message'],'explicit omission');self.assertIs(call['omission_context'],context)
