from dataclasses import replace
from datetime import datetime,timezone
import json
from unittest import TestCase
from src.application.campaign_full_quality_checks import FullQualityEvaluator
from src.application.campaign_quality_checks import QualityCandidate,check_candidate,IMPLEMENTED
from src.application.campaign_acceptance_contract import Stage,RULE_IDS
from src.application.campaign_semantic_checks import SEMANTIC_RULE_IDS
from src.validation.campaign_semantic_benchmark import load_cases


class FullQualityChecksTests(TestCase):
    @classmethod
    def setUpClass(cls):cls.catalog,_=load_cases()
    def fixture(self,observe):
        candidate=QualityCandidate('request',Stage.INTERPRETATION,'CI-SKU-0001','US','META','Please clarify.',())
        now=datetime.now(timezone.utc);base=check_candidate(candidate,(),checked_at=now,not_before=now)
        return FullQualityEvaluator(self.catalog,self.catalog.sha256,observe),candidate,base
    def assess(self,evaluator,candidate,base):return evaluator.assess(candidate,base,turns=[dict(role='USER',text='Create an internal draft')],evidence=[],selection=dict(sku=candidate.sku))
    def response(self,system,content,rules):
        payload=json.loads(content)
        self.assertNotIn('expected_decision',payload);self.assertEqual(payload['candidate'],'Please clarify.')
        return dict(requested_content_decision='NEEDS_REVIEW',decision_reason='Missing source support',checks=[dict(rule_id=r,handling_state='INCONCLUSIVE',reason='Missing support',candidate_quote='',evidence_ids=[]) for r in rules])
    def test_all_47_observed_without_overwriting_literal_holds(self):
        evaluator,candidate,base=self.fixture(self.response);result=self.assess(evaluator,candidate,base)
        self.assertEqual([r['rule_id'] for r in result['checks']],list(RULE_IDS))
        self.assertEqual(sum(r['origin']=='SEMANTIC' for r in result['checks']),41)
        self.assertFalse(result['qualifying']);self.assertFalse(result['release_allowed'])
        for row in result['checks']:
            if row['rule_id'] in IMPLEMENTED:self.assertEqual(row['state'],next(c.state.value for c in base.checks if c.rule_id==row['rule_id']))
    def test_wrong_evaluator_or_changed_candidate_denies(self):
        with self.assertRaises(ValueError):FullQualityEvaluator(self.catalog,'f'*64,self.response)
        evaluator,candidate,base=self.fixture(self.response)
        with self.assertRaises(ValueError):self.assess(evaluator,replace(candidate,text='Changed'),base)
    def test_missing_rule_or_unobserved_quote_is_rejected(self):
        for fault in ('rule','quote'):
            def observe(system,content,rules):
                value=self.response(system,content,rules)
                if fault=='rule':value['checks'].pop()
                else:value['checks'][0].update(handling_state='PASS',candidate_quote='invented text')
                return value
            evaluator,candidate,base=self.fixture(observe)
            with self.subTest(fault=fault),self.assertRaises(ValueError):self.assess(evaluator,candidate,base)

    def test_integrated_observation_is_followed_by_fresh_authority_checks(self):
        from unittest.mock import patch
        from tests.test_campaign_quality_bound_execution import BoundExecutionTests
        from tests import test_campaign_quality_execution as fixtures
        from src.application import campaign_quality_bound_execution as service
        f=BoundExecutionTests();f.setUp();self.addCleanup(f.doCleanups)
        def observe(system,content,rules):
            return dict(requested_content_decision='NEEDS_REVIEW',decision_reason='Unverified evidence',checks=[dict(rule_id=r,handling_state='INCONCLUSIVE',reason='Missing support',candidate_quote='',evidence_ids=[]) for r in rules])
        evaluator=FullQualityEvaluator(self.catalog,self.catalog.sha256,observe)
        with patch('src.application.campaign_transcript_history.read_bound_texts',return_value=('Actual stored fixture user text',)):
            result=service.run_authoritative_quality(f.r,lambda fact:(fixtures.builder(fact),fixtures.builder(fact).text),
                settings=f.s,client=f.fixture.client,runtime_reader=f.fixture.runtime,access_reader=f.fixture.access,
                identity=f.fixture.current.request.identity,clock=lambda:f.now,full_evaluator=evaluator)
        context=json.loads(result.context_canonical_json)
        self.assertEqual(len(context['full_quality_observation']['checks']),47)
        self.assertEqual(context['authority_native_job_ids'],['read-1','read-2'])
        self.assertFalse(result.release_allowed)

    def test_integrated_context_change_during_semantic_callback_withholds(self):
        from unittest.mock import patch
        from tests.test_campaign_quality_bound_execution import BoundExecutionTests
        from tests import test_campaign_quality_execution as fixtures
        from src.application import campaign_quality_bound_execution as service
        f=BoundExecutionTests();f.setUp();self.addCleanup(f.doCleanups)
        def observe(system,content,rules):
            f.content['catalog_sha256']='c'*64
            return dict(requested_content_decision='NEEDS_REVIEW',decision_reason='Missing support',checks=[dict(rule_id=r,handling_state='INCONCLUSIVE',reason='Missing support',candidate_quote='',evidence_ids=[]) for r in rules])
        evaluator=FullQualityEvaluator(self.catalog,self.catalog.sha256,observe)
        with patch('src.application.campaign_transcript_history.read_bound_texts',return_value=('Actual stored fixture user text',)),self.assertRaisesRegex(ValueError,'QUALITY_AUTHORITY_CONTEXT_CHANGED'):
            service.run_authoritative_quality(f.r,lambda fact:(fixtures.builder(fact),fixtures.builder(fact).text),
                settings=f.s,client=f.fixture.client,runtime_reader=f.fixture.runtime,access_reader=f.fixture.access,
                identity=f.fixture.current.request.identity,clock=lambda:f.now,full_evaluator=evaluator)
