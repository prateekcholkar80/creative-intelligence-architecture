from dataclasses import replace
import json
from unittest import TestCase
from src.application.campaign_semantic_checks import APPLICABILITY_VERSION,SemanticInput,prompt
from src.application.campaign_full_quality_checks import FullQualityEvaluator
from src.application.campaign_quality_checks import QualityCandidate,check_candidate
from src.application.campaign_acceptance_contract import Stage
from src.application.campaign_stage_applicability import INTENT_CONTRACT
from src.validation.campaign_semantic_benchmark import load_cases
from tests import test_campaign_evaluation_source_applicability as fixtures

class StageSourceQualityTests(TestCase):
    def setUp(self):
        f=fixtures.EvaluationSourceApplicabilityTests();f.setUp();self.f=f
        self.text=json.dumps(dict(schema_version=INTENT_CONTRACT,features=[],formula_version=None,audience='NEW'))
        self.source=f.check(response_text=self.text)
        self.candidate=QualityCandidate('request',Stage.INTERPRETATION,'CI-SKU-0022','CA','META',self.text,())
        self.base=check_candidate(self.candidate,(),checked_at=f.now,not_before=f.now)
        catalog,_=load_cases();self.catalog=replace(catalog,contract_version=APPLICABILITY_VERSION)
        self.seen=[]
    def observe(self,system,content,rules):
        data=json.loads(content);self.seen.append(data)
        self.assertEqual(data['label_scope'],'STAGE_OUTPUT_OBSERVATION')
        self.assertEqual(data['stage_context']['output_kind'],'EVIDENCE_INTENT_METADATA')
        self.assertFalse(data['source_applicability']['qualification_ready'])
        return dict(requested_content_decision='NEEDS_REVIEW',decision_reason='Downstream qualification not established',checks=[
            dict(rule_id=r,handling_state='NOT_APPLICABLE' if r=='U02' else 'INCONCLUSIVE',reason='No campaign concept in typed intent' if r=='U02' else 'Not established',candidate_quote='',evidence_ids=[]) for r in rules])
    def run_check(self,observer=None,**changes):
        evaluator=FullQualityEvaluator(self.catalog,self.catalog.sha256,observer or self.observe)
        args=dict(turns=[dict(role='USER',text='Prepare an internal draft')],evidence=[],selection=self.f.scope,output_contract=INTENT_CONTRACT,source_applicability=self.source)
        return evaluator.assess(self.candidate,self.base,**(args|changes))
    def test_all_checks_remain_with_bound_stage_and_downstream_holds(self):
        result=self.run_check();self.assertEqual(len(result['checks']),47)
        self.assertEqual(len(self.seen),3)
        self.assertEqual(next(c for c in result['checks'] if c['rule_id']=='U02')['state'],'NOT_APPLICABLE')
        self.assertFalse(result['qualifying']);self.assertFalse(result['release_allowed'])
        self.assertEqual(result['source_applicability'],self.source)
    def test_misclassification_of_intent_as_incomplete_concept_is_rejected_not_rewritten(self):
        def wrong(system,content,rules):
            value=self.observe(system,content,rules)
            for check in value['checks']:
                if check['rule_id']=='U02':check.update(handling_state='FAIL',candidate_quote='NEW',reason='Missing concept and CTA')
            return value
        with self.assertRaisesRegex(ValueError,'SEMANTIC_STAGE_APPLICABILITY'):self.run_check(wrong)
    def test_missing_changed_or_cross_scope_source_report_denies_before_judge(self):
        from unittest.mock import Mock
        for source in (None,self.source|dict(response_sha256='f'*64),self.source|dict(scope_sha256='f'*64),self.source|dict(qualification_ready=True)):
            observer=Mock()
            with self.subTest(source=source),self.assertRaises(ValueError):self.run_check(observer,source_applicability=source)
            observer.assert_not_called()
    def test_unknown_contract_does_not_reuse_intent_exemption(self):
        with self.assertRaises(ValueError):self.run_check(output_contract=None)
    def test_v3_and_v4_evaluator_pins_are_preserved(self):
        from src.application.campaign_semantic_checks import STRICT_VERSION
        catalog,_=load_cases()
        self.assertEqual(FullQualityEvaluator(catalog,catalog.sha256,lambda *a:None).evaluator_sha256,'7cff215eaf3d69ff8fe7291c249f9a6d945e236e4679969beb20d58972d0c5ec')
        v4=replace(catalog,contract_version=STRICT_VERSION)
        self.assertEqual(FullQualityEvaluator(v4,v4.sha256,lambda *a:None).evaluator_sha256,'f8325305e75637668f509e0979dfb07099aa8a938f01e084b7f8845f7ab1cb54')
