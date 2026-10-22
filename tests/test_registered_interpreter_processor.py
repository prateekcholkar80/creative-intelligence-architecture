from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
import json
from src.application.registered_interpreter_processor import RegisteredInterpreterProcessor

class RegisteredProcessorTests(TestCase):
    def processor(self,enabled=True):return RegisteredInterpreterProcessor(client=Mock(),verifier=Mock(),access_reader=Mock(),
        policy_request=Mock(),targets=(),vertex_transport=Mock(),provider_transport=Mock(),profile={'profile':'pin'},worker_id='worker',enabled=enabled)
    def receipt(self):return SimpleNamespace(workspace_id='ws',conversation_id='conv',request_id='req',invocation_id='inv',native_status='COMMITTED')
    def test_disabled_never_reads_or_calls_provider(self):
        current=self.processor(False)
        with patch('src.application.registered_interpreter_processor.require_owned_worker'):
            self.assertEqual(current.process({},self.receipt()).state,'HELD')
        current.verifier.authenticate.assert_not_called();current.provider_transport.assert_not_called()
    def test_unverified_policy_holds_before_admission_or_model(self):
        current=self.processor();data=dict(calls=[dict(invocation_id='inv',state='PLANNED',profile=current.profile)])
        with patch('src.application.registered_interpreter_processor.require_owned_worker'),patch('src.application.registered_interpreter_processor.read_execution_authority',return_value=SimpleNamespace(content_canonical_json=json.dumps(data))),patch('src.application.registered_interpreter_processor.read_policy',return_value=SimpleNamespace(approval_verified=False)):
            self.assertEqual(current.process({},self.receipt()).reason,'INTERPRETER_POLICY_REVIEW_REQUIRED')
        current.provider_transport.assert_not_called();self.assertEqual(current._workers,{})
    def test_reconnect_recovers_retained_operation_without_dispatch(self):
        current=self.processor();worker=Mock();plan=SimpleNamespace(job_id='original')
        current._workers[('ws','conv','req','inv')]=(worker,plan)
        with patch('src.application.registered_interpreter_processor.require_owned_worker'):
            self.assertEqual(current.process({},self.receipt()).state,'RECOVERED')
        worker.recover.assert_called_once_with({},'original');worker.run.assert_not_called();current.provider_transport.assert_not_called()

    def test_cold_stored_success_is_read_only_and_never_replayed(self):
        from datetime import datetime,timezone,timedelta
        current=self.processor();data=dict(calls=[dict(invocation_id='inv',state='SUCCEEDED',profile=current.profile)])
        now=datetime.now(timezone.utc)
        def capture(job):return SimpleNamespace(content_canonical_json=json.dumps(data),content_sha256='a'*64,native_job_id=job,valid_until=now+timedelta(seconds=30))
        with patch('src.application.registered_interpreter_processor.require_owned_worker'),patch('src.application.registered_interpreter_processor.read_execution_authority',side_effect=[capture('one'),capture('two')]),patch('src.application.campaign_response_transcript.verify_stored_response') as body:
            result=current.process({},self.receipt())
        self.assertEqual(result.state,'SUCCEEDED_STORED');self.assertFalse(result.release_allowed)
        body.assert_called_once();current.provider_transport.assert_not_called();self.assertEqual(current._workers,{})
