from dataclasses import replace
from datetime import timedelta
import hashlib,json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch,PropertyMock

from src.application import campaign_evidence_dispatch as dispatch
from src.application.campaign_clarification_policy import ClarificationPolicyRead,ClarificationPolicyRequest
from src.application.campaign_clarification_prompts import definitions
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_evidence_gaps import canonical
from tests.test_campaign_clarification_prompts import ClarificationPromptTests,snapshot,NOW

class SyntheticResolved:
    def __init__(self,binding,snapshot):self.binding,self.snapshot=binding,snapshot

class IntentDispatchTests(TestCase):
    def setUp(self):
        f=ClarificationPromptTests();f.setUp();self.policy=f.policy
        self.profile=dict(stage='INTERPRETER',model_id=snapshot().model_id)
        self.text='Create a shipping campaign'
        request=dict(workspace_id='w',conversation_id='c',request_id='r',principal_id='p',
            operation='INTERPRET',message_sha256=hashlib.sha256(self.text.encode()).hexdigest())
        self.content=dict(authority=dict(audience='NEW',formula_version=None),request_record=request,
            calls=[dict(invocation_id='i',state='DISPATCHED',profile=self.profile)])
        self.authority=ExecutionAuthorityRead(canonical(self.content),_hash(self.content),'authority-1',NOW,NOW+timedelta(seconds=60))
        d=definitions(self.policy,checked_at=NOW)[0]
        self.resolved=SyntheticResolved(SimpleNamespace(**{k:request[k] for k in
            ('workspace_id','conversation_id','request_id','principal_id')},invocation_id='i',
            prompt_key=d.prompt_key,stage='INTERPRETER',binding_sha256='b'*64,
            binding_canonical_json=canonical(dict(profile=self.profile))),
            d.snapshot(snapshot(),vertex_prompt_id='123',vertex_version_id='1'))
        self.raw=canonical(dict(schema_version=d.output_schema_version,features=['SHIPPING'],audience='NEW',formula_version=None))
        self.response=SimpleNamespace(text=self.raw,prompt_feedback=None,candidates=[SimpleNamespace(
            finish_reason='STOP',safety_ratings=[])],model_version=snapshot().model_id,
            usage_metadata=SimpleNamespace(prompt_token_count=123,candidates_token_count=25,
                cached_content_token_count=None,thoughts_token_count=10))
        self.transport=Mock(return_value=self.response)
        self.refresh=Mock(side_effect=lambda:(replace(self.authority,native_job_id='authority-'+str(self.refresh.call_count)),
            replace(self.policy,native_job_id='policy-'+str(self.refresh.call_count)),self.text))
        patch.object(dispatch,'ResolvedPrompt',SyntheticResolved).start()
        patch.object(dispatch,'require_owned_worker').start();patch.object(dispatch,'check_operation').start()
        self.addCleanup(patch.stopall)

    def approved_fixture(self):
        policy=json.loads(self.policy.policy_canonical_json);policy['status']='APPROVED'
        self.policy=replace(self.policy,policy_canonical_json=canonical(policy),
            request=ClarificationPolicyRequest(policy['policy_id'],policy['version'],_hash(policy)))
        self.resolved.snapshot=definitions(self.policy,checked_at=NOW)[0].snapshot(snapshot(),vertex_prompt_id='123',vertex_version_id='1')
        patch.object(ClarificationPolicyRead,'approval_verified',new_callable=PropertyMock,return_value=True).start()

    def adapter(self):return dispatch.EvidenceIntentAdapter(refresh=self.refresh,transport=self.transport,clock=lambda:NOW)

    def test_actual_draft_policy_never_calls_model(self):
        with self.assertRaises(ValueError):self.adapter()(self.profile,self.resolved)
        self.transport.assert_not_called()

    def test_synthetic_approved_dispatch_binds_provider_usage_and_denies_replay(self):
        self.approved_fixture();a=self.adapter();result=a(self.profile,self.resolved)
        self.assertFalse(result.release_allowed);self.assertEqual(result.response_text,self.raw)
        self.assertEqual(result.observation.input_tokens,123)
        self.assertEqual(result.observation.thinking_tokens,10)
        self.assertIsNone(result.observation.cached_tokens)
        self.assertEqual(result.observation.response_sha256,hashlib.sha256(self.raw.encode()).hexdigest())
        with self.assertRaises(ValueError):a(self.profile,self.resolved)
        self.transport.assert_called_once()

    def test_changed_context_after_provider_withholds_result_without_retry(self):
        self.approved_fixture()
        second=replace(self.authority,content_sha256='d'*64,native_job_id='changed')
        self.refresh.side_effect=[(self.authority,self.policy,self.text),
            (second,replace(self.policy,native_job_id='other'),self.text)]
        a=self.adapter()
        with self.assertRaises(ValueError):a(self.profile,self.resolved)
        with self.assertRaises(ValueError):a(self.profile,self.resolved)
        self.transport.assert_called_once()

    def test_unclaimed_invocation_or_wrong_principal_denies_provider(self):
        self.approved_fixture();self.content['calls'][0]['state']='PLANNED'
        self.authority=replace(self.authority,content_canonical_json=canonical(self.content),content_sha256=_hash(self.content))
        with self.assertRaises(ValueError):self.adapter()(self.profile,self.resolved)
        self.transport.assert_not_called()

    def test_invalid_feature_or_safety_block_never_returns_draft(self):
        self.approved_fixture();v=json.loads(self.raw);v['features']=['INVENTED'];self.response.text=canonical(v)
        with self.assertRaises(ValueError):self.adapter()(self.profile,self.resolved)
        self.response.candidates[0].finish_reason='MAX_TOKENS'
        with self.assertRaises(ValueError):self.adapter()(self.profile,self.resolved)

    def test_uncertain_transport_never_retries_and_unknown_usage_stays_unknown(self):
        self.approved_fixture();self.transport.side_effect=RuntimeError('uncertain');a=self.adapter()
        with self.assertRaises(RuntimeError):a(self.profile,self.resolved)
        with self.assertRaises(ValueError):a(self.profile,self.resolved)
        self.transport.assert_called_once()
        self.response.usage_metadata=None;self.response.model_version=None
        observation=dispatch.provider_observation(self.response,self.raw)
        self.assertIsNone(observation.input_tokens);self.assertIsNone(observation.returned_model_id)
        self.assertIsNone(observation.estimated_cost)

    def test_negative_and_boolean_usage_denied(self):
        for value in (-1,True,2**63):
            self.response.usage_metadata.prompt_token_count=value
            with self.assertRaises(ValueError):dispatch.provider_observation(self.response,self.raw)

class QuestionDispatchTests(TestCase):
    def test_question_wrapper_preserves_exact_provider_response_and_usage(self):
        from tests import test_campaign_evidence_conversation as f
        from src.application import campaign_evidence_conversation as conversation
        fixture=f.EvidenceConversationTests();fixture.setUp()
        response=SimpleNamespace(text=fixture.raw,prompt_feedback=None,
            candidates=[SimpleNamespace(finish_reason='STOP',safety_ratings=[])],
            model_version='provider-model',usage_metadata=None)
        transport=Mock(return_value=response)
        adapter=dispatch.EvidenceQuestionDispatch(fixture.initial,refresh=fixture.refresh,
            transport=transport,clock=lambda:NOW)
        with patch.object(conversation,'ResolvedPrompt',f.FakeResolved),patch.object(conversation,'require_owned_worker'),\
                patch.object(conversation,'check_operation'):
            result=adapter({'stage':'INTERPRETER'},f.FakeResolved())
            with self.assertRaises(ValueError):adapter({'stage':'INTERPRETER'},f.FakeResolved())
        self.assertFalse(result.release_allowed)
        self.assertEqual(result.observation.response_sha256,hashlib.sha256(fixture.raw.encode()).hexdigest())
        self.assertEqual(json.loads(result.output_canonical_json),json.loads(fixture.raw))
        transport.assert_called_once()

    def test_question_context_failure_does_not_return_provider_output(self):
        from tests import test_campaign_evidence_conversation as f
        from src.application import campaign_evidence_conversation as conversation
        fixture=f.EvidenceConversationTests();fixture.setUp()
        fixture.refresh.side_effect=ValueError('revoked')
        transport=Mock()
        adapter=dispatch.EvidenceQuestionDispatch(fixture.initial,refresh=fixture.refresh,
            transport=transport,clock=lambda:NOW)
        with patch.object(conversation,'ResolvedPrompt',f.FakeResolved),patch.object(conversation,'require_owned_worker'),\
                patch.object(conversation,'check_operation'):
            with self.assertRaises(ValueError):adapter({'stage':'INTERPRETER'},f.FakeResolved())
        transport.assert_not_called()
