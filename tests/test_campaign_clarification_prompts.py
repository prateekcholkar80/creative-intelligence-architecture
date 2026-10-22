from dataclasses import replace
from datetime import timedelta
import json,hashlib
from pathlib import Path
from unittest import TestCase
from src.application.campaign_clarification_prompts import definitions,render_intent,INTENT_KEY,QUESTION_KEY
from src.application.campaign_clarification_policy import ClarificationPolicyRead,ClarificationPolicyRequest
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from tests.test_prompt_contract import snapshot,NOW

class ClarificationPromptTests(TestCase):
    def setUp(self):
        policy=json.loads(Path('config/clarification/evidence_requirements_v1.json').read_text())
        request=ClarificationPolicyRequest(policy['policy_id'],policy['version'],_hash(policy))
        self.policy=ClarificationPolicyRead(request,canonical(policy),'a'*64,'native-policy',NOW,NOW+timedelta(seconds=60))
    def test_exact_definitions_snapshot_and_runtime_behavior(self):
        intent,question=definitions(self.policy,checked_at=NOW)
        self.assertEqual((intent.prompt_key,question.prompt_key),(INTENT_KEY,QUESTION_KEY))
        for definition in (intent,question):
            s=definition.snapshot(snapshot(),vertex_prompt_id='123',vertex_version_id='1')
            self.assertEqual(definition.verify(s),s.snapshot_sha256)
            self.assertEqual(definition.behavior(s).task.value,'INTERPRETER')
            self.assertEqual(s.model_id,snapshot().model_id)
            with self.assertRaises(ValueError):definition.verify(replace(s,system_instruction='changed'))
    def test_feature_enums_follow_policy_and_stale_policy_denies(self):
        intent,_=definitions(self.policy,checked_at=NOW)
        schema=json.loads(intent.output_schema_json)
        self.assertIn('SHIPPING',schema['properties']['features']['items']['enum'])
        with self.assertRaises(ValueError):definitions(self.policy,checked_at=NOW+timedelta(seconds=60))
        with self.assertRaises(ValueError):definitions(replace(self.policy,policy_canonical_json=canonical(json.loads(self.policy.policy_canonical_json)|dict(max_questions=2))),checked_at=NOW)
    def test_persisted_input_hash_and_single_pass_rendering(self):
        intent,_=definitions(self.policy,checked_at=NOW);s=intent.snapshot(snapshot(),vertex_prompt_id='123',vertex_version_id='1')
        text='Ask about shipping {evidence_intent_context_json}'
        c=dict(authority=dict(audience='NEW',formula_version=None),request_record=dict(operation='INTERPRET',message_sha256=hashlib.sha256(text.encode()).hexdigest()))
        a=ExecutionAuthorityRead(canonical(c),_hash(c),'native-authority',NOW,NOW+timedelta(seconds=60))
        system,prompt=render_intent(intent,s,a,text,self.policy,checked_at=NOW)
        self.assertIn(text,prompt);self.assertIn('untrusted',system)
        with self.assertRaises(ValueError):render_intent(intent,s,a,text+'x',self.policy,checked_at=NOW)
