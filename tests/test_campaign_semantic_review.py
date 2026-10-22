from unittest import TestCase
from copy import deepcopy
from src.validation.campaign_semantic_benchmark import load_cases
from src.validation.campaign_semantic_review import review
from src.application.campaign_semantic_checks import verify_result,VERSION


class SemanticReviewTests(TestCase):
    def fixture(self):
        catalog,cases=load_cases();entry=cases[0];case=entry['case']
        result=verify_result(catalog,case,dict(requested_content_decision='BLOCK',decision_reason='Synthetic review boundary fixture.',
            checks=[dict(rule_id=rid,handling_state='INCONCLUSIVE',reason='Not a real observation.',candidate_quote='',evidence_ids=[]) for rid in case.rule_ids]))
        return dict(version=VERSION,evaluator_sha256=catalog.sha256,review_sha256=catalog.review_sha256,
            qualifying=False,release_allowed=False,full_e1_end_to_end_cases_executed=0,status='INCOMPLETE',cases=[dict(
                case_id=entry['case_id'],case_sha256=entry['case_sha256'],input_sha256=case.sha256,state='OBSERVED',result=result)])

    def test_incomplete_and_mismatched_calibration_cannot_become_approval(self):
        observed=review(self.fixture())
        self.assertEqual(observed['valid_assessments'],1)
        self.assertEqual(len(observed['unattempted_cases']),158)
        self.assertFalse(observed['approval_request_ready']);self.assertFalse(observed['release_allowed'])
        self.assertEqual(observed['full_end_to_end_execution'],'0/159')

    def test_changed_hash_duplicate_and_fabricated_observation_deny(self):
        for mode in ('hash','duplicate','quote'):
            value=deepcopy(self.fixture())
            if mode=='hash':value['cases'][0]['input_sha256']='0'*64
            elif mode=='duplicate':value['cases']*=2
            else:value['cases'][0]['result']['checks'][0]['candidate_quote']='Never actually observed'
            with self.subTest(mode=mode),self.assertRaises(ValueError):review(value)

    def test_calibration_summary_cannot_substitute_for_qualifying_policy_report(self):
        from dataclasses import replace
        from src.application.campaign_quality_qualification import qualify_documents
        from src.application.campaign_acceptance_contract import _hash
        from src.application.workflow_contract import canonical_json_bytes
        from tests.test_campaign_quality_qualification import qualification_fixture
        benchmark,_,args=qualification_fixture();diagnostic=review(self.fixture())
        args['pins']=replace(args['pins'],evaluation_sha256=_hash(diagnostic))
        with self.assertRaises(ValueError):
            qualify_documents(canonical_json_bytes(benchmark).decode(),canonical_json_bytes(diagnostic).decode(),**args)

    def test_aggregate_report_uses_separate_bound_without_relaxing_workflow_limit(self):
        from src.application.workflow_contract import canonical_json_bytes
        value=self.fixture()|dict(diagnostic_notes='x'*70000)
        with self.assertRaises(ValueError):canonical_json_bytes(value)
        self.assertEqual(len(review(value)['input_report_sha256']),64)
        with self.assertRaisesRegex(ValueError,'REPORT_SIZE'):
            review(self.fixture()|dict(diagnostic_notes='x'*(8*1024*1024)))
