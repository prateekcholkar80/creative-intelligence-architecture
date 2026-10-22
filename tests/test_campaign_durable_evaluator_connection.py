import hashlib,json,time
from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_full_quality_checks import FullQualityEvaluator
from src.application.campaign_evaluator_observer import DurableEvaluatorObserver,EvaluatorProfile,EvaluatorProviderResponse,StageEvaluationBinding
from src.application.campaign_evaluator_store import EvaluatorStore,Reservation,canonical
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.campaign_quality_checks import QualityCandidate,check_candidate
from src.application.campaign_acceptance_contract import Stage
from src.application.operation_budget import worker_operation_scope
from src.application.campaign_semantic_checks import SYSTEM
from src.validation.campaign_semantic_benchmark import load_cases


class DurableEvaluatorConnectionTests(TestCase):
    def fixture(self):
        catalog,_=load_cases();store=Mock(spec=EvaluatorStore);store.recover.return_value=None
        store.reserve.side_effect=lambda intent:Reservation(intent['record_id'],canonical(dict(intent,reservation_nonce='nonce')),True)
        def transport(profile,system,content,rules):
            raw=json.dumps(dict(requested_content_decision='NEEDS_REVIEW',decision_reason='Missing support',checks=[dict(rule_id=r,handling_state='INCONCLUSIVE',reason='Missing support',candidate_quote='',evidence_ids=[]) for r in rules]))
            return EvaluatorProviderResponse(raw,ModelResultObservation('SUCCEEDED',returned_model_id=profile.model_id,response_sha256=hashlib.sha256(raw.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED'))
        probe=FullQualityEvaluator(catalog,catalog.sha256,lambda *a:None)
        observer=DurableEvaluatorObserver(store=store,run_id='benchmark-run',evaluator_sha256=probe.evaluator_sha256,
            profile=EvaluatorProfile('model','{}',hashlib.sha256(SYSTEM.encode()).hexdigest()),transport=transport,guard=Mock(),enabled=True)
        binding=StageEvaluationBinding('workspace','conversation','request','principal','INTERPRETATION','invocation',*(['a'*64]*4))
        return probe,observer,binding,store
    def test_interpretation_binding_persists_three_chunks_and_native_receipts(self):
        from datetime import datetime,timezone
        probe,observer,binding,store=self.fixture()
        candidate=QualityCandidate('request',Stage.INTERPRETATION,'CI-SKU-0001','US','META','Please clarify.',())
        binding=replace(binding,candidate_sha256=candidate.sha256)
        evaluator=replace(probe,observe=observer).for_stage(binding)
        now=datetime.now(timezone.utc);base=check_candidate(candidate,(),checked_at=now,not_before=now)
        with worker_operation_scope(time.monotonic()+30):
            result=evaluator.assess(candidate,base,turns=[],evidence=[],selection={})
        self.assertEqual(store.publish.call_count,3);self.assertEqual(len(result['durable_evaluator_receipts']),3)
        self.assertEqual(len(result['checks']),47);self.assertFalse(result['qualifying']);self.assertFalse(result['release_allowed'])
        self.assertIs(observer.for_stage(binding),observer.for_stage(binding))
        for call in store.reserve.call_args_list:self.assertEqual(call.args[0]['binding']['invocation_id'],'invocation')
    def test_durable_evaluator_pin_and_unsupported_stage_deny(self):
        probe,observer,binding,_=self.fixture();observer.evaluator_sha256='f'*64
        with self.assertRaises(ValueError):replace(probe,observe=observer).for_stage(binding)
        with self.assertRaises(ValueError):replace(binding,stage='GENERATION')
