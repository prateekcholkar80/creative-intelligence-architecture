from copy import deepcopy
import json
from unittest import TestCase
from src.validation.campaign_e1_closeout import DEFAULT,REVIEW,validate,validate_review
from src.application.workflow_contract import ContractError


class E1CloseoutTests(TestCase):
    def setUp(self):self.package=json.loads(DEFAULT.read_text())

    def deny(self,change):
        package=deepcopy(self.package);change(package)
        with self.assertRaises(ContractError):validate(package)

    def test_complete_packet_is_ready_for_review_without_invented_approval(self):
        result=validate(self.package)
        self.assertEqual((result['criteria_count'],result['proposed_label_count']),(47,159))
        self.assertEqual(result['market_channel_dispositions'],16);self.assertEqual(result['visual_rubric_items'],8)
        for key in ('human_review_recorded','e1_complete','serving_enabled','model_quality_measured'):self.assertIs(result[key],False)

    def test_human_review_and_completion_cannot_be_forged_in_draft(self):
        for changes in ({'approval':{'reviewer':'invented'}},{'e1_complete':True},{'serving_enabled':True},{'status':'APPROVED'}):
            self.deny(lambda p,v=changes:p.update(v))

    def test_latest_policy_and_all_stage_rule_coverage_are_required(self):
        self.deny(lambda p:p.update(policy_specification_sha256='a'*64))
        self.deny(lambda p:p['criteria'].pop())
        self.deny(lambda p:p['criteria'][0]['stage_applicability'].pop('REFINEMENT'))
        index=next(i for i,c in enumerate(self.package['criteria']) if c['rule_id']=='C07')
        self.deny(lambda p:p['criteria'][index]['stage_applicability']['GENERATION'].update(trigger='Only when efficacy is claimed'))

    def test_unreviewed_market_cannot_become_compliance_pass(self):
        self.deny(lambda p:p['market_dispositions'][0].update(campaign_compliance_pass_allowed=True))
        self.deny(lambda p:p['market_dispositions'][0].update(overlay_status='APPROVED'))
        self.deny(lambda p:p['market_dispositions'].__setitem__(0,deepcopy(p['market_dispositions'][1])))

    def test_case_hash_and_exact_label_scope_are_pinned(self):
        self.deny(lambda p:p['case_artifacts'][0].update(sha256='b'*64))
        other='BLOCK' if self.package['case_index'][0]['expected_decision']=='PASS' else 'PASS'
        self.deny(lambda p:p['case_index'][0].update(expected_decision=other))
        self.deny(lambda p:p['case_index'][0].update(label_scope='GLOBAL_CAMPAIGN_PASS'))

    def test_source_and_visual_specimens_have_complete_pinned_coverage(self):
        self.deny(lambda p:p['source_precedence'].pop())
        self.deny(lambda p:p['visual_rubric'].pop())
        self.deny(lambda p:p['visual_specimens']['CI-SKU-0021'].update(generation=1))

    def test_supplemental_cases_require_multi_turn_and_all_market_channel_pairs(self):
        self.deny(lambda p:p['supplemental_cases'].pop())
        self.deny(lambda p:[r.update(turns=[{'role':'USER','text':'single turn'}]) for r in p['supplemental_cases']])
        self.deny(lambda p:p['supplemental_cases'][0].update(country='EU'))

    def test_identity_only_pass_cannot_become_whole_campaign_pass(self):
        index=next(i for i,c in enumerate(self.package['supplemental_cases']) if c['expected_decision']=='PASS')
        self.deny(lambda p:p['supplemental_cases'][index].update(decision_target='REQUESTED_CONTENT_OR_ARTIFACT'))

    def test_explicit_recorded_user_review_closes_only_e1_and_preserves_all_other_holds(self):
        review=json.loads(REVIEW.read_text());result=validate_review(self.package,review)
        self.assertTrue(result['e1_complete']);self.assertTrue(result['human_review_recorded'])
        self.assertFalse(result['runtime_authority_granted']);self.assertFalse(result['serving_enabled'])
        self.assertFalse(result['model_quality_measured'])

    def test_review_hash_scope_counts_or_claim_authorization_cannot_be_changed(self):
        review=json.loads(REVIEW.read_text())
        for changes in ({'review_sha256':'a'*64},{'criteria_count':46},{'label_count':143},
            {'scope':'CAMPAIGN_PUBLICATION'},{'claim_use_authorized':True},{'serving_enabled':True},
            {'legal_clearance':0},{'scientific_claim_approval':'APPROVED'}):
            with self.assertRaises(ContractError):validate_review(self.package,review|changes)
