from copy import deepcopy
from dataclasses import replace
import json
from unittest import TestCase
from src.application.campaign_question_binding import bind_question_authority
from src.application.campaign_quality_authority import VERSION, validate_payload
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_evidence_gaps import canonical
from tests import test_campaign_evidence_intent as intent_fixture
from tests import test_campaign_evidence_conversation as gap_fixture
from tests.test_generation_admission import records
from tests.test_campaign_permission_contract import NOW


class QuestionBindingTests(TestCase):
    def setUp(self):
        f=intent_fixture.EvidenceIntentTests();f.setUp();f.content['authority']['claims']=[]
        self.parent=f.capture()
        # The gap plan must refer to the same successfully stored intent response.
        q=gap_fixture.EvidenceConversationTests();q.setUp()
        plan=json.loads(q.plan.canonical_json)
        plan['scope']['intent_sha256']=f.call['response_sha256']
        gap_plan=replace(q.plan,canonical_json=canonical(plan))
        self.gaps=replace(q.initial,plan=gap_plan)
        a=deepcopy(f.content['authority'])
        self.child=dict(a,version=VERSION,request_id='question-child',request_sha256='d'*64,
            stage='INTERPRETATION',transcript=[dict(request_id='r',role='ASSISTANT',
                content_sha256=f.call['response_sha256'],invocation_id='inv')])
        self.profile=deepcopy(records('INTERPRETER')['model_invocations']['profile'])
        self.profile['output_schema_version']='campaign-evidence-questions-v1'

    def bind(self):
        return bind_question_authority(self.child,parent=self.parent,gaps=self.gaps,
            profile=self.profile,checked_at=NOW)

    def test_child_pins_parent_gap_profile_and_keeps_v1_compatible(self):
        validate_payload(self.child)
        value=self.bind();validate_payload(value)
        self.assertEqual(value['stage'],'CLARIFICATION')
        self.assertEqual(value['question_parent']['authority_sha256'],self.parent.content_sha256)
        self.assertEqual(value['question_parent']['question_profile_sha256'],_hash(self.profile))
        self.assertNotIn('release_allowed',value)

    def test_scope_transcript_profile_and_expiry_changes_deny(self):
        for key,value in [('country','GB'),('request_id','r'),('transcript',[])]:
            before=deepcopy(self.child);self.child[key]=value
            with self.assertRaises(ValueError):self.bind()
            self.child=before
        self.profile['output_schema_version']='campaign-evidence-intent-v1'
        with self.assertRaises(ValueError):self.bind()
        self.setUp();self.parent=replace(self.parent,valid_until=NOW)
        with self.assertRaises(ValueError):self.bind()

    def test_supplied_hash_changed_parent_outcome_and_gap_pins_deny(self):
        self.parent=replace(self.parent,content_sha256='e'*64)
        with self.assertRaises(ValueError):self.bind()
        self.setUp();content=json.loads(self.parent.content_canonical_json)
        content['calls'][0]['state']='FAILED'
        self.parent=replace(self.parent,content_canonical_json=canonical(content),content_sha256=_hash(content))
        with self.assertRaises(ValueError):self.bind()
        self.setUp();plan=json.loads(self.gaps.plan.canonical_json);plan['scope']['request_sha256']='e'*64
        self.gaps=replace(self.gaps,plan=replace(self.gaps.plan,canonical_json=canonical(plan)))
        with self.assertRaises(ValueError):self.bind()

    def test_question_authority_rejects_missing_extra_and_self_parent(self):
        value=self.bind()
        for change in ({'question_parent':{}},{'unrecognized':'value'},
                {'question_parent':value['question_parent']|{'request_id':value['request_id']}}):
            with self.assertRaises(ValueError):validate_payload(value|change)
