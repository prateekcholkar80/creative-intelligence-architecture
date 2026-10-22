import json
from copy import deepcopy
from unittest import TestCase
from src.validation.campaign_pipeline_benchmark import compile_inventory,readiness,digest


class PipelineInventoryTests(TestCase):
    @classmethod
    def setUpClass(cls):cls.inventory=compile_inventory()

    def test_exact_159_with_honest_scopes_and_no_execution_claim(self):
        result=readiness(self.inventory)
        self.assertEqual(result['case_count'],159)
        self.assertEqual(result['frozen_candidate_cases'],143)
        self.assertEqual(result['request_only_cases'],16)
        self.assertEqual(result['full_qualifying_pipeline_cases_executed'],0)
        self.assertFalse(result['qualifying']);self.assertFalse(result['release_allowed'])
        self.assertEqual(result['reviewed_label_scopes'],dict(ISOLATED_RULE_HANDLING=94,SCENARIO_HANDLING=49,REQUEST_ONLY_SCENARIO=16))

    def test_expected_answers_never_enter_execution_payload(self):
        for entry in self.inventory['execution_cases']:
            value=entry['execution_input']
            self.assertFalse({'expected_decision','expected_handling','expected_evidence_ids','decision_target','expected_reason','evidence_state'}&set(value))
            self.assertNotIn('expected_handling',json.dumps(value))
            self.assertFalse(value['fixture_evidence_is_current_authority'])
        changed=deepcopy(self.inventory)
        changed['scoring_labels'][0]['expected_decision']='NEEDS_REVIEW'
        self.assertEqual(changed['execution_cases'],self.inventory['execution_cases'])
        self.assertNotEqual(digest(changed),digest(self.inventory))

    def test_request_only_has_no_manufactured_response(self):
        entries=[e for e in self.inventory['execution_cases'] if e['execution_input']['reviewed_label_scope']=='REQUEST_ONLY_SCENARIO']
        self.assertEqual(len(entries),16)
        for entry in entries:
            value=entry['execution_input']
            self.assertIsNone(value['supplied_candidate'])
            self.assertEqual(value['supplied_candidate_origin'],'ABSENT')
            self.assertFalse(any('authored-scenario-state'==e['evidence_id'] for e in value['fixture_evidence']))

    def test_modality_is_observed_and_refinement_requires_parent(self):
        for entry in self.inventory['execution_cases']:
            stage=entry['execution_input']['stage'];requirements=entry['required_evidence']
            self.assertIn('OUTPUT_MODALITY_AND_APPLICABILITY_CAPTURE',requirements)
            self.assertNotIn('GROUNDED_RENDERED_ARTIFACT_BYTES',requirements)
            if stage in ('GENERATION','REFINEMENT'):
                self.assertIn('ACTUAL_RETRIEVAL_RANKS_AND_SOURCE_PINS',requirements)
            if stage=='REFINEMENT':self.assertIn('BOUND_PRIOR_OUTPUT_AND_ACTUAL_REFINEMENT',requirements)

    def test_tampered_inputs_and_false_execution_claims_are_rejected(self):
        for kind in ('input','execution','coverage'):
            value=deepcopy(self.inventory)
            if kind=='input':value['execution_cases'][0]['execution_input']['country']='GB'
            elif kind=='execution':value['execution_cases'][0]['qualifying_pipeline_execution']=True
            else:value['execution_cases'].pop()
            with self.subTest(kind=kind),self.assertRaises(ValueError):readiness(value)
