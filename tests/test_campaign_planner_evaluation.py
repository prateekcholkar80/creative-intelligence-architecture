import hashlib,json,time
from dataclasses import replace
from datetime import datetime,timezone,timedelta
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_planner_evaluation import *
from src.application.campaign_grounded_planner_definition import GroundedPlannerDefinition
from src.application.campaign_evaluator_store import Reservation
from src.application.campaign_evaluator_observer import EvaluatorProviderResponse
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.operation_budget import worker_operation_scope
from tests.test_prompt_contract import snapshot
from tests.test_campaign_planner_projection import options,response


class PlannerEvaluationTests(TestCase):
    def prepared(self,case_changes=None,legacy=False):
        now=datetime.now(timezone.utc);raw=b'fixture image';pins=[dict(asset_id='asset',generation='123',mime_type='image/webp',byte_count=len(raw),sha256=hashlib.sha256(raw).hexdigest())]
        m=dict(principal_id='pilot',context=dict(scope=dict(sku='CI-SKU-0022',country='US',channel='META')),image_pins=pins)
        uploads=EvaluationUploadInputs(canonical(m),(('asset','123','image/webp',raw),),now,now,now+timedelta(seconds=120))
        base=snapshot(model_id='gemini-3.5-flash');definition=GroundedPlannerDefinition(base.output_schema_json)
        s=base if legacy else definition.snapshot(base,vertex_prompt_id='999',vertex_version_id='1')
        case=dict(case_id='generation-pilot',case_sha256='c'*64,stage='GENERATION',sku='CI-SKU-0022',country='US',channel='META',turns=[dict(role='USER',text='Prepare an identity-only private proposal.')])
        case.update(case_changes or {})
        return prepare_planner_evaluation(run_id='pilot',case_input=case,snapshot=s,registry_record_digest='d'*64,
            uploads=uploads,binding_id='eval_upload_binding_v1_'+uploads.sha256,options_json=options())
    def fixture(self):
        prepared=self.prepared();intents={};outcomes={};store=Mock(spec=EvaluatorStore)
        store.recover.side_effect=lambda i:outcomes.get(i['record_id']) if i['record_id'] in intents else None
        def reserve(i):
            fresh=i['record_id'] not in intents;intents.setdefault(i['record_id'],canonical(dict(i,reservation_nonce='nonce')))
            return Reservation(i['record_id'],intents[i['record_id']],fresh)
        store.reserve.side_effect=reserve
        store.publish.side_effect=lambda r,o:outcomes.setdefault(r.record_id,o)
        transport=Mock();raw=response()
        transport.planner.return_value=EvaluatorProviderResponse(raw,ModelResultObservation('SUCCEEDED',returned_model_id='gemini-3.5-flash',
            response_sha256=hashlib.sha256(raw.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED'))
        guard=Mock();engine=DurablePlannerEvaluation(store=store,transport=transport,guard=guard,enabled=True)
        return prepared,engine,store,transport,guard,intents,outcomes
    def execute(self,engine,p,**kw):
        with worker_operation_scope(time.monotonic()+60):return engine.execute(p,**kw)
    def test_model_stage_and_measured_projection_are_durable_and_cold_replay_is_read_only(self):
        p,e,s,t,g,intents,outcomes=self.fixture();result=self.execute(e,p)
        self.assertEqual(result['checks']['schema_and_enum_projection']['status'],'PASS')
        self.assertEqual(result['checks']['full_47_check_evaluation'],'UNAVAILABLE');self.assertFalse(result['qualifying'])
        self.assertIsNone(result['usage']['input_tokens']);self.assertEqual(len(intents),2);self.assertEqual(len(outcomes),2)
        cold=DurablePlannerEvaluation(store=s,transport=t,guard=g,enabled=True)
        self.assertEqual(self.execute(cold,p,recover_only=True),result);t.planner.assert_called_once()
    def test_unknown_provider_and_existing_reservation_never_dispatch_again(self):
        p,e,s,t,g,intents,outcomes=self.fixture();t.planner.side_effect=TimeoutError()
        with self.assertRaises(ValueError):self.execute(e,p)
        with self.assertRaises(ValueError):self.execute(e,p)
        t.planner.assert_called_once()
        p,e,s,t,g,intents,outcomes=self.fixture();s.reserve.side_effect=lambda i:Reservation(i['record_id'],canonical(i),False)
        with self.assertRaises(ValueError):self.execute(e,p)
        t.planner.assert_not_called()
    def test_lost_outcome_reply_republishes_original_pending_without_another_model_call(self):
        p,e,s,t,g,intents,outcomes=self.fixture();original=s.publish.side_effect;lost=[False]
        def publish(r,o):
            original(r,o)
            if not lost[0]:lost[0]=True;raise TimeoutError()
        s.publish.side_effect=publish
        with self.assertRaises(TimeoutError):self.execute(e,p)
        # Recovery-only cannot invent a missing score; it can finish retained publication.
        with self.assertRaises(ValueError):self.execute(e,p,recover_only=True)
        self.execute(e,p);t.planner.assert_called_once()
    def test_changed_bytes_source_guard_or_disabled_engine_prevents_provider(self):
        p,e,s,t,g,*_=self.fixture()
        with self.assertRaises(ValueError):self.execute(e,replace(p,images=(('asset','123','image/webp',b'changed'),)))
        g.side_effect=ValueError('revoked')
        with self.assertRaises(ValueError):self.execute(e,p)
        t.planner.assert_not_called();s.reserve.assert_not_called()
        e.enabled=False
        with self.assertRaises(ValueError):self.execute(e,p)
    def test_case_gold_or_legacy_prompt_cannot_be_injected_into_prepared_execution(self):
        with self.assertRaises(ValueError):self.prepared(dict(expected_decision='PASS'))
        with self.assertRaises(ValueError):self.prepared(legacy=True)
