from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_interpretation_evaluation import *


def settings(**changes):
    return InterpretationEvaluationSettings(**(dict(run_id='pilot-evaluation-1',case_id='e1-closure-missing-report',case_sha256='a'*64,
        prompt_id='5933813566455939072',prompt_version='1',prompt_snapshot_sha256='b'*64,policy_id='policy',policy_version='v1',policy_sha256='c'*64,evaluator_sha256='d'*64,enabled=False)|changes))


class InterpretationEvaluationTests(TestCase):
    def service(self,enabled=True):
        measurement=object.__new__(ProtectedMeasurementRuntime);measurement.client=Mock();measurement.settings=Mock(evaluator_sha256='d'*64)
        with patch('src.application.campaign_interpretation_evaluation.require_owned_worker'),patch('src.application.campaign_interpretation_evaluation.EvaluatorStore'):
            return ProtectedInterpretationEvaluation(measurement,settings(enabled=enabled))
    def test_disabled_before_any_capture_or_model(self):
        service=self.service(False);service._capture=Mock()
        with patch('src.application.campaign_interpretation_evaluation.require_owned_worker'),self.assertRaises(ValueError):service.execute({})
        service._capture.assert_not_called()
    def test_untrusted_measurement_or_evaluator_pin_rejected(self):
        with patch('src.application.campaign_interpretation_evaluation.require_owned_worker'),self.assertRaises(ValueError):ProtectedInterpretationEvaluation(Mock(),settings())
        real=object.__new__(ProtectedMeasurementRuntime);real.settings=Mock(evaluator_sha256='e'*64)
        with patch('src.application.campaign_interpretation_evaluation.require_owned_worker'),self.assertRaises(ValueError):ProtectedInterpretationEvaluation(real,settings())
    def test_source_change_denies(self):
        service=self.service();service._capture=Mock(return_value=({'changed':True},))
        with self.assertRaises(ValueError):service._guard({},dict(original=True))
    def test_stage_cold_recovery_never_calls_transport_or_reserve(self):
        service=self.service();service.store.recover.return_value=dict(state='UNKNOWN');service.transport=Mock();service._profile=Mock(return_value=EvaluatorProfile('model','{}','f'*64))
        snapshot=Mock(system_instruction='system',output_schema_json='{}')
        slot,result=service._stage({},dict(scope=dict(principal_id='actor')),snapshot,'content',recover_only=True)
        self.assertEqual(result['state'],'UNKNOWN');service.store.reserve.assert_not_called();service.transport.interpretation.assert_not_called()
    def test_missing_original_recovery_does_not_create_stage(self):
        service=self.service();service.store.recover.return_value=None;service.transport=Mock();service._profile=Mock(return_value=EvaluatorProfile('model','{}','f'*64))
        snapshot=Mock(system_instruction='system',output_schema_json='{}')
        with self.assertRaises(ValueError):service._stage({},dict(scope=dict(principal_id='actor')),snapshot,'content',recover_only=True)
        service.store.reserve.assert_not_called();service.transport.interpretation.assert_not_called()

    def test_real_controller_flow_and_cold_recovery_use_original_records(self):
        self._exercise_controller_flow(LEGACY_CONTRACT)

    def test_strict_controller_flow_and_cold_recovery_use_original_records(self):
        self._exercise_controller_flow(STRICT_CONTRACT)

    def test_applicability_controller_flow_and_frozen_source_recovery(self):
        self._exercise_controller_flow(APPLICABILITY_CONTRACT)

    def _exercise_controller_flow(self,contract_version):
        # Synthetic native/provider boundaries isolate controller behavior. This
        # is not browser IAP or a measured benchmark observation.
        import time,hashlib,json
        from datetime import datetime,timezone
        from src.application.campaign_evaluator_store import Reservation
        from src.application.campaign_evaluator_observer import EvaluatorProviderResponse
        from src.application.workflow_invocation_result_query import ModelResultObservation
        from src.application.operation_budget import worker_operation_scope
        class MemoryStore(EvaluatorStore):
            def __init__(self):self.intents={};self.outcomes={}
            def preflight(self):pass
            def reserve(self,intent):
                self.intents[intent['record_id']]=intent
                return Reservation(intent['record_id'],canonical(dict(intent,reservation_nonce='fixture')),True)
            def publish(self,reservation,outcome):self.outcomes[reservation.record_id]=dict(outcome)
            def recover(self,intent):
                old=self.intents.get(intent['record_id'])
                if old is None:return None
                if old!=intent:raise ValueError('changed intent')
                return self.outcomes.get(intent['record_id'],dict(state='UNKNOWN'))
        catalog,cases=load_cases();catalog=replace(catalog,contract_version=contract_version);entry=next(c for c in cases if c['case_id']=='e1-closure-missing-report')
        full=FullQualityEvaluator(catalog,catalog.sha256,lambda *a:None)
        service=self.service();service.settings=replace(service.settings,evaluator_sha256=full.evaluator_sha256,judge_contract_version=contract_version)
        service.measurement.clock=lambda:datetime.now(timezone.utc);service.store=MemoryStore();service.transport=Mock()
        schema=dict(required=['schema_version','features','formula_version','audience'],properties=dict(features=dict(items=dict(enum=['SCIENTIFIC_ASSERTION']))))
        snapshot=Mock(model_id='model',system_instruction='stage system',snapshot_sha256='e'*64,generation_config_json='{}',safety_settings_json='[]',output_schema_json=canonical(schema),output_schema_version='campaign-evidence-intent-v1')
        source=dict(scope=dict(workspace_id='workspace',principal_id='actor',sku='CI-SKU-0022',country='US',channel='META',audience='NEW',formula_version=None),product_fields_json='{"product_name":"Product"}')
        if contract_version==APPLICABILITY_CONTRACT:
            from tests import test_campaign_evaluation_source_applicability as source_fixture
            f=source_fixture.EvaluationSourceApplicabilityTests();f.setUp()
            service._source_product,service._source_policy=f.product,f.policy
            service.measurement.clock=lambda:f.now
            source['scope']=f.scope
        service._capture=Mock(return_value=(source,entry['case'],catalog,snapshot,'rendered stage context'));service._source_jobs={}
        stage_text=canonical(dict(schema_version='campaign-evidence-intent-v1',features=[],formula_version=None,audience='NEW'))
        def received(raw):return EvaluatorProviderResponse(raw,ModelResultObservation('SUCCEEDED',returned_model_id='model',response_sha256=hashlib.sha256(raw.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED',input_tokens=5,output_tokens=3))
        service.transport.interpretation.return_value=received(stage_text)
        service.transport.side_effect=lambda profile,system,content,rules:received(canonical(dict(requested_content_decision='NEEDS_REVIEW',decision_reason='Missing support',checks=[dict(rule_id=r,handling_state='NOT_APPLICABLE' if r=='U02' and contract_version==APPLICABILITY_CONTRACT else 'INCONCLUSIVE',reason='Missing support',candidate_quote='',evidence_ids=[]) for r in rules])))
        with worker_operation_scope(time.monotonic()+300):first=service.execute({})
        self.assertEqual(len(first['evaluator_receipts']),3);self.assertFalse(first['release_allowed']);self.assertFalse(first['qualifying'])
        self.assertEqual(service.transport.interpretation.call_count,1);self.assertEqual(service.transport.call_count,3)
        # Cold observer instance is created by execute; store recovery supplies
        # all four original outcomes without another provider invocation.
        with worker_operation_scope(time.monotonic()+300):second=service.execute({},recover_only=True)
        self.assertEqual(second['stage_response_sha256'],first['stage_response_sha256'])
        self.assertEqual(second['assessment_record_id'],first['assessment_record_id'])
        self.assertEqual(second['observation_sha256'],first['observation_sha256'])
        self.assertEqual(second['evaluator_receipts'],first['evaluator_receipts'])
        self.assertEqual(service.transport.interpretation.call_count,1);self.assertEqual(service.transport.call_count,3)

    def test_actual_renderer_tuple_is_bound_as_system_and_content(self):
        from tests.test_prompt_contract import snapshot
        from src.application.prompt_contract import PromptTask
        snap=snapshot(task=PromptTask.INTERPRETER,system_instruction='Fixed system',
            required_variables=('evidence_intent_context_json',),prompt_template='Input: {evidence_intent_context_json}')
        result=render_evaluation_input(snap,dict(user_text='Literal {untrusted}'))
        self.assertIsInstance(result,str)
        self.assertIn('Literal {untrusted}',result)
        self.assertNotIn('Fixed system',result)

    def test_capture_uses_native_timestamp_and_real_template_contract(self):
        from datetime import timedelta
        from tests.test_campaign_measurement_runtime import authority_fixture
        from tests.test_campaign_clarification_prompts import ClarificationPromptTests
        from tests.test_prompt_contract import snapshot
        from src.application.campaign_clarification_prompts import definitions
        from src.application.campaign_quality_product_reader import ProductFactRead
        row,args,_=authority_fixture();now=args['ended'];catalog,cases=load_cases()
        case=next(c for c in cases if c['case_id']=='e1-closure-missing-report')
        fixture=ClarificationPromptTests();fixture.setUp()
        policy=replace(fixture.policy,observed_at=now,valid_until=now+timedelta(seconds=60))
        snap=definitions(policy,checked_at=now)[0].snapshot(snapshot(),vertex_prompt_id='5933813566455939072',vertex_version_id='1')
        service=self.service();service.settings=replace(service.settings,case_sha256=case['case_sha256'],prompt_snapshot_sha256=snap.snapshot_sha256)
        service.measurement.settings=replace(args['settings'],review_sha256=catalog.review_sha256)
        service.measurement._actor=Mock(return_value=args['identity']);service.measurement.clock=lambda:now
        service.measurement.io=Mock();service.measurement.io.read.return_value=row
        head=Mock(snapshot=snap,status=PromptLifecycle.DRAFT,digest='e'*64)
        product=ProductFactRead(SourceFact('native-product','CI-SKU-0022','{"product_name":"Product"}',now,now+timedelta(seconds=60),True),'native-product')
        with patch('src.application.campaign_interpretation_evaluation.BigQueryPromptRegistryReader') as reader,patch('src.application.campaign_interpretation_evaluation.read_policy',return_value=policy),patch('src.application.campaign_interpretation_evaluation.read_product_fact',return_value=product):
            reader.return_value.read_exact.return_value=Mock(state=Mock(head=head))
            source,_,_,_,rendered=service._capture({})
        self.assertIsInstance(rendered,str);self.assertEqual(source['scope']['sku'],'CI-SKU-0022')
        self.assertEqual(len(service._source_jobs['authority_row_sha256']),64)
        self.assertNotIn('expected_decision',rendered);self.assertNotIn('expected_handling',rendered)

    def test_recovery_only_lane_denies_run_before_capture_and_transport(self):
        service=self.service();service.settings=replace(service.settings,recovery_only=True)
        service._capture=Mock();service.transport=Mock()
        with patch('src.application.campaign_interpretation_evaluation.require_owned_worker'),self.assertRaisesRegex(ValueError,'RECOVERY_ONLY'):
            service.execute({})
        service._capture.assert_not_called();service.transport.assert_not_called()

    def test_contract_version_pins_transport_without_changing_legacy_bytes(self):
        from src.application.campaign_semantic_checks import STRICT_VERSION,STRICT_SYSTEM
        service=self.service();snapshot=Mock(model_id='model',snapshot_sha256='b'*64,generation_config_json='{}',safety_settings_json='[]')
        legacy=service._profile(snapshot,SYSTEM)
        self.assertEqual(json.loads(legacy.settings_json)['version'],TRANSPORT_VERSION)
        service.settings=replace(service.settings,judge_contract_version=STRICT_VERSION)
        strict=service._profile(snapshot,STRICT_SYSTEM)
        self.assertEqual(json.loads(strict.settings_json)['version'],STRICT_TRANSPORT_VERSION)
        self.assertNotEqual(legacy.template_sha256,strict.template_sha256)
        with self.assertRaises(ValueError):replace(service.settings,judge_contract_version='unregistered')

    def test_applicability_transport_version_is_distinct(self):
        service=self.service();service.settings=replace(service.settings,judge_contract_version=APPLICABILITY_CONTRACT)
        snapshot=Mock(model_id='model',snapshot_sha256='b'*64,generation_config_json='{}',safety_settings_json='[]')
        self.assertEqual(json.loads(service._profile(snapshot,'fixed').settings_json)['version'],APPLICABILITY_TRANSPORT_VERSION)

    def test_failed_source_capture_preserves_actual_provider_outcome(self):
        import hashlib
        from src.application.campaign_evaluator_store import Reservation
        from src.application.campaign_evaluator_observer import EvaluatorProviderResponse
        from src.application.workflow_invocation_result_query import ModelResultObservation
        service=self.service();service.settings=replace(service.settings,judge_contract_version=APPLICABILITY_CONTRACT)
        service.store.recover.return_value=None
        service.store.reserve.return_value=Reservation('a'*64,'{}',True)
        service._source_jobs={};service._guard=Mock()
        service._profile=Mock(return_value=EvaluatorProfile('model','{}','f'*64))
        raw=canonical(dict(schema_version=INTENT_CONTRACT,features=[],formula_version=None,audience='NEW'))
        response=EvaluatorProviderResponse(raw,ModelResultObservation('SUCCEEDED',returned_model_id='model',response_sha256=hashlib.sha256(raw.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED'))
        service.transport.interpretation=Mock(return_value=response)
        snapshot=Mock(model_id='model',system_instruction='fixed',output_schema_json='{}')
        # Missing native captures cannot be converted into approved source checks.
        with patch('src.application.campaign_interpretation_evaluation.check_operation'):
            _,outcome=service._stage({},dict(scope=dict(principal_id='actor')),snapshot,'input',recover_only=False)
        self.assertEqual(outcome['state'],'SUCCEEDED')
        self.assertEqual(outcome['source_applicability_state'],'FAILED_OR_UNAVAILABLE')
        self.assertEqual(outcome['response_text'],raw)
        service.store.publish.assert_called_once();service.transport.interpretation.assert_called_once()
