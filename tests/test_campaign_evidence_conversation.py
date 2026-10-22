from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_evidence_conversation as conversation
from src.application.campaign_evidence_gaps import plan_gaps,EvidenceCoverage,canonical
from src.application.campaign_acceptance_contract import _hash
from tests.test_campaign_permission_contract import NOW

class FakeResolved:
    """Synthetic fixture only; actual ResolvedPrompt admission is tested separately."""
    def __init__(self):
        self.binding=SimpleNamespace(prompt_key=conversation.QUESTION_KEY,stage='INTERPRETER',workspace_id='w',
            conversation_id='c',principal_id='p',request_id='question-request',binding_canonical_json=canonical({'profile':{'stage':'INTERPRETER'}}),binding_sha256='a'*64)
        self.snapshot=SimpleNamespace(output_schema_version=conversation.QUESTION_SCHEMA,required_variables=('evidence_gap_plan_json',))
    def render(self,variables):return 'fixture instructions',variables['evidence_gap_plan_json']

class EvidenceConversationTests(TestCase):
    def setUp(self):
        policy=json.loads(Path('config/clarification/evidence_requirements_v1.json').read_text());policy['status']='APPROVED'
        scope=dict(workspace_id='w',conversation_id='c',request_id='r',sku='CI-SKU-0001',country='US',channel='META',
            request_sha256='a'*64,intent_sha256='b'*64)
        capture=EvidenceCoverage('commercial-offer',_hash(scope),'ABSENT',(),NOW,NOW+timedelta(seconds=60),'native-read')
        self.plan=plan_gaps(canonical(policy),scope=scope,features=('OFFER',),captures=(capture,),checked_at=NOW)
        self.initial=conversation.BoundGapRead(self.plan,'c'*64,'p',NOW+timedelta(seconds=60),True)
        gap=json.loads(self.plan.canonical_json)['questions'][0]
        self.raw=canonical(dict(plan_sha256=self.plan.sha256,questions=[dict(requirement_id=gap['requirement_id'],
            fields=gap['fields'],question='Which offer terms, eligibility, exclusions and dates apply?')]))
        self.transport=Mock(return_value=Mock());self.refresh=Mock(return_value=self.initial)
    def run_adapter(self,initial=None,resolved=None,raw=None):
        adapter=conversation.EvidenceQuestionAdapter(initial or self.initial,refresh=self.refresh,transport=self.transport,clock=lambda:NOW)
        with patch.object(conversation,'ResolvedPrompt',FakeResolved),patch.object(conversation,'require_owned_worker'),\
            patch.object(conversation,'check_operation'),patch.object(conversation,'complete_response_text',return_value=raw or self.raw):
            result=adapter({'stage':'INTERPRETER'},resolved or FakeResolved())
            with self.assertRaises(ValueError):adapter({'stage':'INTERPRETER'},resolved or FakeResolved())
        return result
    def test_admitted_callback_renders_bound_request_and_never_grants_release(self):
        draft=self.run_adapter();self.assertFalse(draft.release_allowed)
        self.assertEqual(draft.plan_sha256,self.plan.sha256)
        self.assertEqual(self.refresh.call_count,2);self.transport.assert_called_once()
        self.assertEqual(json.loads(self.transport.call_args.args[2])['plan_sha256'],self.plan.sha256)
    def test_draft_policy_wrong_prompt_and_pre_call_context_change_deny(self):
        data=json.loads(self.plan.canonical_json);data['policy_status']='DRAFT'
        with self.assertRaises(ValueError):self.run_adapter(initial=replace(self.initial,plan=replace(self.plan,canonical_json=canonical(data))))
        self.transport.assert_not_called()
        resolved=FakeResolved();resolved.binding.prompt_key='wrong'
        with self.assertRaises(ValueError):self.run_adapter(resolved=resolved)
        self.transport.assert_not_called()
        self.refresh.return_value=replace(self.initial,context_sha256='d'*64)
        with self.assertRaises(ValueError):self.run_adapter()
        self.transport.assert_not_called()
    def test_post_call_revocation_or_response_rewrite_withholds_draft(self):
        self.refresh.side_effect=[self.initial,ValueError('revoked')]
        with self.assertRaises(ValueError):self.run_adapter()
        self.transport.assert_called_once()
        self.refresh.side_effect=None;self.refresh.return_value=self.initial
        changed=json.loads(self.raw);changed['plan_sha256']='d'*64
        with self.assertRaises(ValueError):self.run_adapter(raw=canonical(changed))
    def test_unowned_worker_cannot_dispatch(self):
        adapter=conversation.EvidenceQuestionAdapter(self.initial,refresh=self.refresh,transport=self.transport,clock=lambda:NOW)
        with patch.object(conversation,'require_owned_worker',side_effect=ValueError('unowned')):
            with self.assertRaises(ValueError):adapter({},FakeResolved())
        self.transport.assert_not_called()

    def test_authoritative_binding_compares_final_capture_and_pins_source_state(self):
        from tests import test_campaign_evidence_intent as fixture
        from src.application.campaign_evidence_coverage import CoverageRead
        from src.application.campaign_evidence_intent import bind_evidence_intent
        f=fixture.EvidenceIntentTests();f.setUp();first=f.capture();second=replace(first,native_job_id='second')
        from src.application.campaign_clarification_policy import ClarificationPolicyRequest,ClarificationPolicyRead
        policy=json.loads(f.policy);request=ClarificationPolicyRequest(policy['policy_id'],policy['version'],_hash(policy))
        policy_read=ClarificationPolicyRead(request,f.policy,'a'*64,'policy-1',NOW,NOW+timedelta(seconds=60))
        intent=bind_evidence_intent(first,f.raw,f.policy,checked_at=NOW);scope=json.loads(intent.scope_json)
        coverage=CoverageRead((EvidenceCoverage('commercial-offer',_hash(scope),'ABSENT',(),NOW,
            NOW+timedelta(seconds=60),'catalog'),),'f'*64,'catalog',NOW,NOW+timedelta(seconds=60))
        def read():
            return conversation.read_bound_gaps(Mock(),request,identity=Mock(),access_reader=Mock(),workspace_id='w',
                conversation_id='c',request_id='r',observed_interpreter_response=f.raw,clock=lambda:NOW)
        with patch.object(conversation,'read_execution_authority',side_effect=[first,second]),\
            patch.object(conversation,'read_coverage',return_value=coverage),\
            patch.object(conversation,'read_policy',side_effect=[policy_read,replace(policy_read,native_job_id='policy-2')]):
            result=read();self.assertEqual(json.loads(result.plan.canonical_json)['decision'],'CLARIFY')
        with patch.object(conversation,'read_execution_authority',side_effect=[first,replace(second,content_sha256='d'*64)]),\
            patch.object(conversation,'read_coverage',return_value=coverage),\
            patch.object(conversation,'read_policy',side_effect=[policy_read,replace(policy_read,native_job_id='policy-2')]):
            with self.assertRaises(ValueError):read()

    def test_provider_safety_failure_and_expired_context_withhold_draft(self):
        adapter=conversation.EvidenceQuestionAdapter(self.initial,refresh=self.refresh,transport=self.transport,clock=lambda:NOW)
        with patch.object(conversation,'ResolvedPrompt',FakeResolved),patch.object(conversation,'require_owned_worker'),\
            patch.object(conversation,'check_operation'),patch.object(conversation,'complete_response_text',side_effect=ValueError('blocked')):
            with self.assertRaises(ValueError):adapter({'stage':'INTERPRETER'},FakeResolved())
        self.refresh.return_value=replace(self.initial,valid_until=NOW)
        self.transport.reset_mock()
        with self.assertRaises(ValueError):self.run_adapter()
        self.transport.assert_not_called()

    def test_unverified_policy_marker_prevents_model_transport(self):
        with self.assertRaises(ValueError):self.run_adapter(initial=replace(self.initial,policy_approval_verified=False))
        self.transport.assert_not_called()

    def test_browser_supplied_policy_json_is_rejected_before_reads(self):
        with patch.object(conversation,'read_execution_authority') as authority:
            with self.assertRaises(ValueError):conversation.read_bound_gaps(Mock(),'{}',identity=Mock(),access_reader=Mock(),
                workspace_id='w',conversation_id='c',request_id='r',observed_interpreter_response='{}')
            authority.assert_not_called()
