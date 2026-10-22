import json,time
from threading import RLock
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch

from src.application.campaign_planner_runtime import ProtectedPlannerEvaluation
from src.application.campaign_evaluator_store import canonical,sha
from src.application.operation_budget import worker_operation_scope
from tests.test_campaign_planner_evaluation import PlannerEvaluationTests


class RetrievalMeasurementTests(TestCase):
    def setUp(self):
        verifier=patch('src.application.campaign_planner_runtime.verify_retrieval_coverage')
        self.verify=verifier.start();self.addCleanup(verifier.stop)
    def fixture(self):
        helper=PlannerEvaluationTests();p,e,s,t,g,intents,outcomes=helper.fixture()
        helper.execute(e,p)
        s.recover.side_effect=lambda i: (outcomes.get(i['record_id'],dict(state='UNKNOWN'))
            if i['record_id'] in intents else None)
        service=object.__new__(ProtectedPlannerEvaluation)
        service._lock=RLock();service._headers=None;service._retrieval_pending={}
        original=json.loads(p.intent_json)
        service.settings=SimpleNamespace(enabled=True,run_id=original['run_id'],case_input_json=canonical(original['case_input']))
        service.engine=e;service.measurement=Mock()
        service._capture=Mock(return_value=('inputs','snapshot','pins','evidence'))
        service._prepare=Mock(return_value=p);service._guard=Mock()
        value=dict(sku='CI-SKU-0022',corpus_status='CORPUS_EMPTY',counts=dict(knowledge_chunks=0,text_vectors=0,image_vectors=0),
            native_job_id='retrieval_coverage_'+'a'*32,qualifying=False,release_allowed=False)
        coverage=dict(value,sha256=sha(value))
        return service,s,t,coverage,intents,outcomes

    def run_action(self,service,**kw):
        with worker_operation_scope(time.monotonic()+60):return service.measure_retrieval({},**kw)

    def test_separate_measurement_and_cold_recovery_never_repeat_planner_or_modify_stage(self):
        service,s,t,coverage,intents,outcomes=self.fixture();before=canonical(outcomes)
        with patch('src.application.campaign_planner_runtime.capture_retrieval_coverage',return_value=coverage) as capture:
            result=self.run_action(service)
            self.assertEqual(result['corpus_status'],'CORPUS_EMPTY');self.assertFalse(result['qualifying'])
            self.assertEqual(len(outcomes),3);self.assertEqual(len(intents),3)
            for k,v in json.loads(before).items():self.assertEqual(outcomes[k],v)
            service._retrieval_pending={}
            self.assertEqual(self.run_action(service,recover_only=True),result)
            capture.assert_called_once();t.planner.assert_called_once()
            self.assertEqual(self.verify.call_count,2)
            self.assertIsNone(service._headers)

    def test_missing_original_recovery_does_not_reserve_or_capture(self):
        service,s,t,coverage,*_=self.fixture();s.reserve.reset_mock()
        with patch('src.application.campaign_planner_runtime.capture_retrieval_coverage') as capture:
            with self.assertRaisesRegex(ValueError,'RETRIEVAL_NO_ORIGINAL_MEASUREMENT'):
                self.run_action(service,recover_only=True)
        capture.assert_not_called();s.reserve.assert_not_called();t.planner.assert_called_once()

    def test_unknown_reservation_never_reads_again_or_creates_another_slot(self):
        service,s,t,coverage,intents,outcomes=self.fixture()
        with patch('src.application.campaign_planner_runtime.capture_retrieval_coverage',side_effect=TimeoutError()) as capture:
            with self.assertRaises(TimeoutError):self.run_action(service)
            for recover in (False,True):
                with self.assertRaisesRegex(ValueError,'RETRIEVAL_MEASUREMENT_UNAVAILABLE'):
                    self.run_action(service,recover_only=recover)
            capture.assert_called_once();t.planner.assert_called_once();self.assertEqual(len(intents),3)

    def test_lost_publish_reply_recovers_original_pending_without_requery(self):
        service,s,t,coverage,*_=self.fixture();publish=s.publish.side_effect;lost=[False]
        def failing(r,o):
            publish(r,o)
            if not lost[0]:lost[0]=True;raise TimeoutError()
        s.publish.side_effect=failing
        with patch('src.application.campaign_planner_runtime.capture_retrieval_coverage',return_value=coverage) as capture:
            with self.assertRaises(TimeoutError):self.run_action(service)
            result=self.run_action(service,recover_only=True)
            self.assertFalse(result['release_allowed']);capture.assert_called_once();t.planner.assert_called_once()

    def test_revocation_withholds_publication_and_no_model_call(self):
        service,s,t,coverage,*_=self.fixture();s.publish.reset_mock()
        service._guard.side_effect=[{},ValueError('revoked')]
        with patch('src.application.campaign_planner_runtime.capture_retrieval_coverage',return_value=coverage):
            with self.assertRaises(ValueError):self.run_action(service)
        s.publish.assert_not_called();t.planner.assert_called_once();self.assertIsNone(service._headers)

    def test_missing_or_corrupted_stage_denies_before_measurement(self):
        for stage in (None,dict(state='UNKNOWN'),dict(state='SUCCEEDED',response_text='forged')):
            service,s,t,coverage,*_=self.fixture();s.recover.side_effect=None;s.recover.return_value=stage;s.reserve.reset_mock()
            with patch('src.application.campaign_planner_runtime.capture_retrieval_coverage') as capture:
                with self.assertRaisesRegex(ValueError,'RETRIEVAL_ORIGINAL_STAGE_UNAVAILABLE'):self.run_action(service)
            capture.assert_not_called();s.reserve.assert_not_called()

    def test_native_recovery_failure_withholds_receipt(self):
        service,s,t,coverage,*_=self.fixture();self.verify.side_effect=ValueError('native changed')
        with patch('src.application.campaign_planner_runtime.capture_retrieval_coverage',return_value=coverage):
            with self.assertRaises(ValueError):self.run_action(service)
        self.assertIsNone(service._headers);t.planner.assert_called_once()

    def test_protocol_is_bounded_and_result_bound_to_configured_case(self):
        from src.application.campaign_operator_host import ProtectedOperatorHost,request_values
        from src.application.campaign_operator_worker import execute
        from src.application.operation_channel import ChannelError
        from tests.test_campaign_planner_runtime import PlannerRuntimeTests
        cfg=SimpleNamespace(planner=PlannerRuntimeTests().settings())
        host=object.__new__(ProtectedOperatorHost);host._config=cfg
        service,s,t,coverage,*_=self.fixture()
        receipt=dict(record_id='a'*64,stage_record_id='b'*64,coverage_sha256='c'*64,
            corpus_status='CORPUS_EMPTY',counts=coverage['counts'],native_job_id=coverage['native_job_id'],
            qualifying=False,release_allowed=False,run_id=cfg.planner.run_id,
            case_id=json.loads(cfg.planner.case_input_json)['case_id'])
        for method in ('retrieval_measure','retrieval_recover'):
            self.assertEqual(request_values(method,{},{} )['method'],method)
            with self.assertRaises(ChannelError):request_values(method,{},dict(sku='other'))
            self.assertEqual(host._decode_result(method,receipt),receipt)
            host._validate_result_scope(method,{},receipt)
            with self.assertRaises(ChannelError):host._validate_result_scope(method,{},dict(receipt,run_id='other'))
            with self.assertRaises(ChannelError):host._decode_result(method,dict(receipt,qualifying=True))
            planner=Mock();execute(None,cfg,method,{}, {},planner=planner)
            planner.measure_retrieval.assert_called_once_with({},recover_only=method=='retrieval_recover')
