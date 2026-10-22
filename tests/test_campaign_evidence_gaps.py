"""Synthetic coverage captures; no claim approval, BigQuery proof or model calls."""
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
from unittest import TestCase
from src.application.campaign_evidence_gaps import EvidenceCoverage,plan_gaps,validate_questions,canonical
from src.application.campaign_acceptance_contract import _hash

class EvidenceGapTests(TestCase):
    def setUp(self):
        self.policy=json.loads(Path('config/clarification/evidence_requirements_v1.json').read_text())
        self.scope=dict(workspace_id='workspace',conversation_id='conversation',request_id='request',
            sku='CI-SKU-0021',country='US',channel='META',request_sha256='a'*64,intent_sha256='b'*64)
        self.now=datetime(2026,10,16,tzinfo=timezone.utc)
    def capture(self,rule,state='ABSENT',fields=()):
        return EvidenceCoverage(rule,_hash(self.scope),state,fields,self.now,self.now+timedelta(seconds=60),'native-read')
    def plan(self,features=(),captures=(),policy=None):
        return plan_gaps(canonical(policy or self.policy),scope=self.scope,features=features,captures=captures,checked_at=self.now)
    def data(self,p):return json.loads(p.canonical_json)
    def test_factual_campaign_has_no_scientific_or_offer_questions(self):
        p=self.plan();self.assertEqual(self.data(p)['questions'],[])
        self.assertEqual(self.data(p)['decision'],'NO_EVIDENCE_GAPS');self.assertFalse(p.generation_authorized)
        with self.assertRaises(ValueError):p.question_request()
    def test_absence_asks_only_relevant_missing_fields(self):
        p=self.plan(('PRICE',),(self.capture('current-price'),));g=self.data(p)['questions'][0]
        self.assertEqual(g['fields'],['price','currency','effective_at'])
        self.assertNotIn('study_report_reference',g['fields']);self.assertEqual(g['submission_status'],'UNVERIFIED')
        partial=self.plan(('PRICE',),(self.capture('current-price','CURRENT_APPROVED',('price','currency')),))
        self.assertEqual(self.data(partial)['questions'][0]['fields'],['effective_at'])
    def test_approved_complete_record_suppresses_question(self):
        p=self.plan(('PRICE',),(self.capture('current-price','CURRENT_APPROVED',('price','currency','effective_at')),))
        self.assertEqual(self.data(p)['questions'],[]);self.assertFalse(p.generation_authorized)
    def test_failure_is_not_record_absence_or_user_workaround(self):
        for state,action in [('UNAVAILABLE','RETRY_RETRIEVAL'),('PENDING_REVIEW','NEEDS_REVIEW'),
                ('REJECTED','NEEDS_REVIEW'),('STALE','NEEDS_REVIEW'),('CONFLICT','NEEDS_REVIEW')]:
            p=self.plan(('PRICE',),(self.capture('current-price',state),))
            self.assertEqual(self.data(p)['decision'],action);self.assertEqual(self.data(p)['questions'],[])
    def test_scope_freshness_and_missing_or_duplicate_captures_deny(self):
        c=self.capture('current-price')
        for captures in ((),(c,c),(replace(c,scope_sha256='b'*64),),
                (replace(c,valid_until=self.now),),(replace(c,available_fields=('invented',)),)):
            with self.assertRaises(ValueError):self.plan(('PRICE',),captures)
        with self.assertRaises(ValueError):self.plan(('UNRECOGNIZED_FEATURE',),())
    def test_request_changes_invalidate_prior_coverage(self):
        c=self.capture('current-price');self.scope['country']='CA'
        with self.assertRaises(ValueError):self.plan(('PRICE',),(c,))
    def test_requirements_are_configuration_driven_without_code_change(self):
        policy=self.policy|dict(requirements=[dict(requirement_id='custom',when_any=['CUSTOM_FEATURE'],
            evidence_kind='CUSTOM_RECORD',owner='CUSTOM_OWNER',required_fields=['custom_field'],alternative='OMIT_CUSTOM')])
        p=self.plan(('CUSTOM_FEATURE',),(self.capture('custom'),),policy)
        self.assertEqual(self.data(p)['questions'][0]['fields'],['custom_field'])
        self.assertEqual(p.question_request()['gaps'][0]['evidence_kind'],'CUSTOM_RECORD')
    def test_question_wording_is_generated_and_plan_bound(self):
        p=self.plan(('PRICE',),(self.capture('current-price'),));gap=self.data(p)['questions'][0]
        q=dict(plan_sha256=p.sha256,questions=[dict(requirement_id=gap['requirement_id'],fields=gap['fields'],
            question='What current price, currency and effective date should apply?')])
        self.assertEqual(json.loads(validate_questions(p,q)),q)
        for changed in (q|dict(plan_sha256='b'*64),q|dict(questions=[]),
            q|dict(questions=[q['questions'][0]|dict(fields=['invented'])])):
            with self.assertRaises(ValueError):validate_questions(p,changed)
    def test_three_question_limit_and_pending_review_does_not_repeat_submission(self):
        features=('PRICE','OFFER','SCIENTIFIC_ASSERTION','DIRECTIONS')
        captures=tuple(self.capture(k) for k in ('current-price','commercial-offer','scientific-assertion','label-instructions'))
        p=self.plan(features,captures);self.assertEqual(len(self.data(p)['questions']),3)
        self.assertEqual(self.data(p)['deferred_questions'],1)
        p=self.plan(('PRICE',),(self.capture('current-price','PENDING_REVIEW',('price','currency','effective_at')),))
        self.assertEqual(self.data(p)['questions'],[]);self.assertFalse(p.generation_authorized)
    def test_shipping_does_not_ask_stock_or_price(self):
        p=self.plan(('SHIPPING',),(self.capture('shipping'),))
        self.assertEqual(self.data(p)['questions'][0]['fields'],['shipping_terms','effective_at'])
