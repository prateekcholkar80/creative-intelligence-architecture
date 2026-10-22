"""Gap refresh may preserve old pins only behind verified exact transitions."""
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
import json
from google.cloud import bigquery
from src.application.control_io import query_config
from src.application.campaign_evidence_conversation import BoundGapRead
from src.application.campaign_question_context_guard import QuestionContextGuard
from tests import test_campaign_question_context as fixture


class QuestionContextGuardTests(TestCase):
    def setUp(self):
        f=fixture.QuestionContextTests();f.setUp();self.f=f
        self.admitted=f.capture(f.after,'admitted');self.proof=f.verify()
        pins=json.loads(f.p['authority_payload'])['question_parent']
        self.initial=BoundGapRead(SimpleNamespace(sha256=pins['gap_plan_sha256']),pins['gap_context_sha256'],
            self.admitted.principal_id,f.now+timedelta(seconds=60),False,'d'*64)
        call=json.loads(f.p['invocation_candidate']);call.update(state='DISPATCHED',revision=2)
        config=query_config([bigquery.ScalarQueryParameter('candidate','STRING',json.dumps(call))])
        self.dispatch=replace(f.plan,kind='DISPATCH',config_json=json.dumps(config.to_api_repr()))
        self.current=[replace(self.admitted,read_id='current-first'),replace(self.admitted,read_id='current-final')]
        self.fresh=replace(self.initial,context_sha256='e'*64,plan=SimpleNamespace(sha256='fresh-time-dependent-plan'))
    def guard(self,initial=None,fresh=None,current=None):
        return QuestionContextGuard(initial=initial or self.initial,admitted=self.admitted,
            admission_proof=self.proof,dispatch_plan=self.dispatch,read_context=Mock(side_effect=current or self.current),
            read_gaps=Mock(return_value=fresh or self.fresh),clock=lambda:self.f.now)
    def test_preserves_original_plan_only_after_both_transition_checks(self):
        guard=self.guard()
        # Actual transition semantics are tested with complete plans in the
        # context tests. Here mock only that boundary to isolate gap composition.
        with patch('src.application.campaign_question_context_guard.require_owned_worker'), \
                patch('src.application.campaign_question_context_guard.verify_transition') as verify:
            result=guard()
        self.assertEqual(verify.call_count,2)
        self.assertEqual(result.context_sha256,self.initial.context_sha256)
        self.assertIs(result.plan,self.initial.plan)
        self.assertEqual(result.policy_approval_verified,False)
    def test_changed_evidence_policy_principal_or_expiry_denies(self):
        for fresh in (replace(self.fresh,evidence_context_sha256='f'*64),
                replace(self.fresh,policy_approval_verified=True),replace(self.fresh,principal_id='different'),
                replace(self.fresh,valid_until=self.f.now)):
            guard=self.guard(fresh=fresh)
            with patch('src.application.campaign_question_context_guard.require_owned_worker'), \
                    patch('src.application.campaign_question_context_guard.verify_transition'):
                with self.assertRaises(ValueError):guard()
    def test_wrong_initial_hash_missing_evidence_or_wrong_admission_proof_denies(self):
        for initial in (replace(self.initial,context_sha256='f'*64),
                replace(self.initial,evidence_context_sha256=None)):
            with self.assertRaises(ValueError):self.guard(initial=initial)
        self.proof=replace(self.proof,after_sha256='f'*64)
        with self.assertRaises(ValueError):self.guard()
    def test_transition_failure_stops_before_evidence_read(self):
        guard=self.guard()
        with patch('src.application.campaign_question_context_guard.require_owned_worker'), \
                patch('src.application.campaign_question_context_guard.verify_transition',side_effect=ValueError('unexpected')):
            with self.assertRaises(ValueError):guard()
        guard.read_gaps.assert_not_called()
    def test_unowned_worker_never_reads_context(self):
        guard=self.guard()
        with patch('src.application.campaign_question_context_guard.require_owned_worker',side_effect=ValueError('unowned')):
            with self.assertRaises(ValueError):guard()
        guard.read_context.assert_not_called()
