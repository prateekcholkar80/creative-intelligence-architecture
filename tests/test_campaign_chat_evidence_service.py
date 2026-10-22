from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from src.application.campaign_chat_evidence_service import ProtectedEvidenceChatService
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_clarification_prompts import definitions
from src.application.identity import IapIdentityAdapter
from src.application.prompt_contract import initial_prompt_record
from src.application.prompt_registry_codec import initial_registration_state
from tests.test_campaign_clarification_prompts import ClarificationPromptTests, snapshot
from tests.test_prompt_contract import NOW


class EvidenceChatServiceTests(TestCase):
    def setUp(self):
        fixture=ClarificationPromptTests();fixture.setUp()
        self.policy=fixture.policy
        self.verifier=Mock(spec=IapIdentityAdapter)
        self.identity=Mock();self.verifier.authenticate.return_value=self.identity
        self.reader=Mock();self.targets=[];self.states={}
        for n,d in enumerate(definitions(self.policy,checked_at=NOW),1):
            pid=str(n);self.targets.append((d.prompt_key,pid,'1'))
            value=d.snapshot(snapshot(),vertex_prompt_id=pid,vertex_version_id='1')
            state=initial_registration_state(initial_prompt_record(value,imported_at=NOW,imported_by='operator'))
            self.states[pid]=SimpleNamespace(state=state,observed_at=NOW)
        self.reader.read_exact.side_effect=lambda pid,v:self.states[pid]
        self.service=ProtectedEvidenceChatService(client=Mock(),verifier=self.verifier,
            access_reader=Mock(),registry_reader=self.reader,policy_request=self.policy.request,
            targets=tuple(self.targets),clock=lambda:NOW)
        self.count=0
        def policy(*a,**kw):
            self.count+=1;return replace(self.policy,native_job_id='policy-'+str(self.count))
        self.policy_mock=patch('src.application.campaign_chat_evidence_service.read_policy',side_effect=policy).start()
        self.access=patch('src.application.campaign_chat_evidence_service._access',return_value='current-access').start()
        patch('src.application.campaign_chat_evidence_service.ensure_fresh').start()
        self.addCleanup(patch.stopall)

    def check(self):return self.service.check({'assertion':'not-retained'},workspace_id='w',conversation_id='c')

    def test_registered_drafts_require_policy_evaluation_and_never_release(self):
        result=self.check()
        self.assertFalse(result.release_allowed)
        self.assertEqual([s for k,s in result.prompt_statuses],['DRAFT','DRAFT'])
        self.assertEqual(set(result.reason_codes),{'CHAT_EVIDENCE_PROMPT_NOT_ACTIVE',
            'CHAT_EVIDENCE_POLICY_REVIEW_REQUIRED','CHAT_EVIDENCE_QUALITY_RELEASE_NOT_READY'})
        self.assertEqual(self.reader.read_exact.call_count,4)
        self.assertEqual(self.access.call_count,2)

    def test_unauthenticated_or_revoked_access_prevents_source_reads(self):
        self.verifier.authenticate.side_effect=ValueError('denied')
        with self.assertRaises(ValueError):self.check()
        self.policy_mock.assert_not_called();self.reader.read_exact.assert_not_called()
        self.verifier.authenticate.side_effect=None;self.access.side_effect=ValueError('revoked')
        with self.assertRaises(ValueError):self.check()
        self.policy_mock.assert_not_called()

    def test_policy_or_prompt_change_withholds_result(self):
        self.policy_mock.side_effect=[self.policy,replace(self.policy,native_job_id='other',record_sha256='b'*64)]
        with self.assertRaises(ValueError):self.check()
        self.policy_mock.side_effect=[self.policy,replace(self.policy,native_job_id='other')]
        first=self.states['1'];second=self.states['2']
        self.reader.read_exact.side_effect=[first,second,SimpleNamespace(state=SimpleNamespace(head='changed'))]
        with self.assertRaises(ValueError):self.check()

    def test_final_revocation_or_wrong_target_denies(self):
        self.access.side_effect=['before','revoked']
        with self.assertRaises(ValueError):self.check()
        self.access.side_effect=None;self.access.return_value='current-access'
        self.service.targets=tuple(reversed(self.service.targets))
        with self.assertRaises(ValueError):self.check()

    def test_preparation_calls_existing_authoritative_gap_service_and_rechecks_access(self):
        result=SimpleNamespace(valid_until=NOW+timedelta(seconds=60))
        with patch('src.application.campaign_chat_evidence_service.read_bound_gaps',return_value=result) as gaps:
            actual=self.service.prepare_evidence({},workspace_id='w',conversation_id='c',request_id='r',
                observed_interpreter_response='server-observed-response')
        self.assertIs(actual,result)
        self.assertEqual(gaps.call_args.kwargs['observed_interpreter_response'],'server-observed-response')
        self.assertEqual(self.access.call_count,2)

    def test_preparation_rejects_revocation_after_gap_read(self):
        self.access.side_effect=['before','changed']
        with patch('src.application.campaign_chat_evidence_service.read_bound_gaps',
                return_value=SimpleNamespace(valid_until=NOW+timedelta(seconds=60))):
            with self.assertRaises(ValueError):
                self.service.prepare_evidence({},workspace_id='w',conversation_id='c',request_id='r',
                    observed_interpreter_response='response')

    def test_intent_factory_reads_persisted_text_and_fixed_scope(self):
        import hashlib
        text='persisted user message'
        authority=SimpleNamespace(content_canonical_json=canonical(dict(request_record=dict(
            message_sha256=hashlib.sha256(text.encode()).hexdigest()))),valid_until=NOW+timedelta(seconds=60))
        with patch('src.application.campaign_quality_authority_reader.read_execution_authority',return_value=authority) as reader,\
                patch('src.application.campaign_transcript.read_transcript',return_value=(text,)) as transcript:
            adapter=self.service.intent_adapter({},workspace_id='w',conversation_id='c',request_id='r',transport=Mock())
            actual,policy,body=adapter.refresh()
            self.assertIs(actual,authority);self.assertEqual(body,text)
            self.assertEqual(reader.call_args.kwargs['request_id'],'r')
            self.assertEqual(transcript.call_args.kwargs['conversation_id'],'c')
        self.assertEqual(self.access.call_count,2)

    def test_provider_result_requires_matching_durable_outcome_before_gaps(self):
        import hashlib
        from src.application.campaign_evidence_dispatch import EvidenceInvocationDraft
        from src.application.workflow_invocation_result_query import ModelResultObservation
        raw='{"features":[]}'
        obs=ModelResultObservation('SUCCEEDED',returned_model_id='model',
            response_sha256=hashlib.sha256(raw.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED')
        result=EvidenceInvocationDraft(raw,raw,'a'*64,'b'*64,obs)
        call=dict(invocation_id='i',state='SUCCEEDED',**{k:getattr(obs,k) for k in
            ('response_sha256','returned_model_id','finish_reason','safety_outcome',
             'input_tokens','output_tokens','cached_tokens','thinking_tokens')})
        body=dict(bindings=[dict(binding_sha256='b'*64,request_id='r',invocation_id='i')],calls=[call])
        def captured(job):return SimpleNamespace(content_canonical_json=canonical(body),
            content_sha256=_hash(body),native_job_id=job,valid_until=NOW+timedelta(seconds=60))
        initial,final=captured('first'),captured('second')
        prepared=SimpleNamespace(valid_until=NOW+timedelta(seconds=60))
        with patch('src.application.campaign_quality_authority_reader.read_execution_authority',
                side_effect=[initial,final]),patch.object(self.service,'prepare_evidence',return_value=prepared) as gaps:
            self.assertIs(self.service.prepare_evidence_from_result({},workspace_id='w',
                conversation_id='c',request_id='r',result=result),prepared)
            gaps.assert_called_once()
        body['calls'][0]['state']='DISPATCHED'
        with patch('src.application.campaign_quality_authority_reader.read_execution_authority',
                return_value=captured('third')),patch.object(self.service,'prepare_evidence') as gaps:
            with self.assertRaises(ValueError):self.service.prepare_evidence_from_result({},
                workspace_id='w',conversation_id='c',request_id='r',result=result)
            gaps.assert_not_called()

    def test_question_factory_uses_protected_parent_plan_and_fresh_reader(self):
        prepared=object();transport=Mock()
        with patch.object(self.service,'prepare_evidence',return_value=prepared) as gaps,patch(
                'src.application.campaign_evidence_dispatch.EvidenceQuestionDispatch') as adapter:
            returned=self.service.question_adapter({},workspace_id='w',conversation_id='c',request_id='r',
                observed_interpreter_response='persisted response',transport=transport)
            self.assertIs(returned,adapter.return_value)
            self.assertIs(adapter.call_args.args[0],prepared)
            adapter.call_args.kwargs['refresh']()
            self.assertEqual(gaps.call_count,2)
