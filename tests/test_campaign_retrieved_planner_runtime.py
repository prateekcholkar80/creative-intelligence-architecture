from dataclasses import replace
from types import SimpleNamespace
import hashlib,json,time
from unittest import TestCase
from unittest.mock import Mock

from src.application.campaign_retrieved_planner_runtime import *
from src.application.campaign_evaluator_observer import EvaluatorProviderResponse,ModelResultObservation
from src.application.campaign_evaluator_store import sha
from src.application.operation_budget import worker_operation_scope
from tests import test_campaign_retrieved_planner as planner_fixtures
from tests.test_private_semantic_rerank import response


class RetrievedPlannerRuntimeTests(TestCase):
    def fixture(self):
        args,rerank_transport,_,store=planner_fixtures.RetrievedPlannerTests().fixture()
        request=args['rerank_request'];value=json.loads(request.input_json)
        documents=object();capture=args['corpus'];binding=dict(binding_id=value['corpus_binding_id'],
            corpus_sha256=capture.sha256,selection_sha256='e'*64)
        refresh=Mock(return_value=True)
        bound=BoundCorpusCapture(capture,canonical(binding),refresh)
        corpus=SimpleNamespace(documents=documents,capture_bound=Mock(return_value=bound))
        retrieval=object.__new__(ProtectedCorpusRetrieval)
        retrieval.store=store
        retrieval.embedding=SimpleNamespace(corpus=corpus,settings=SimpleNamespace(**binding))
        retrieval.settings=SimpleNamespace(query_text=value['query'],query_id=request.query_id,
            case_json=canonical(args['rerank_case']))
        parent=dict(value['parent'],corpus_sha256=capture.sha256,rankings=dict(rrf=value['baseline_ids']),
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
        retrieval.execute=Mock(return_value=parent)
        planner=object.__new__(ProtectedPlannerEvaluation);planner.documents=documents
        planner.measurement=SimpleNamespace(client=store.client,clock=lambda:args['checked_at'])
        case=dict(args['case_input']);case.pop('case_sha256')
        planner.settings=SimpleNamespace(run_id='old-planner-pilot',case_input_json=canonical(case),
            case_sha256=args['case_input']['case_sha256'],binding_id=args['binding_id'])
        fields=dict(product_name='Rejuvenate Restorative Face Oil')
        pins=dict(grounding=dict(product_fields=fields,sku='CI-SKU-0022',retrieval_status='NOT_EXECUTED'),
            authority_revisions=[4,2],registry_digest='d'*64)
        planner._capture=Mock(return_value=(args['uploads'],args['snapshot'],pins,dict(product_native_job_id='native-product')))
        text=canonical({k:values[0] for k,values in json.loads(identity_options(fields)).items()})
        transport=Mock();transport.planner.return_value=EvaluatorProviderResponse(text,ModelResultObservation('SUCCEEDED',
            returned_model_id='gemini-3.5-flash',response_sha256=hashlib.sha256(text.encode()).hexdigest(),
            finish_reason='STOP',safety_outcome='PASSED'))
        settings=RetrievedPlannerSettings('new-campaign-v1',request.run_id,True)
        runtime=ProtectedRetrievedPlanner(planner,retrieval,settings,rerank_transport=rerank_transport,planner_transport=transport)
        return runtime,rerank_transport,transport,refresh,store

    def test_one_operation_and_cold_recovery_reuse_saved_stages(self):
        with worker_operation_scope(time.monotonic()+60):
            runtime,rerank,planner,_,store=self.fixture()
            result=runtime.execute({'iap':'fixture'})
            self.assertEqual(result['planner']['checks']['schema_and_enum_projection']['status'],'PASS')
            self.assertEqual(result['artifact_status'],'NOT_EXECUTED');self.assertFalse(result['release_allowed'])
            cold=ProtectedRetrievedPlanner(runtime.planner,runtime.retrieval,runtime.settings,
                rerank_transport=rerank,planner_transport=planner)
            self.assertEqual(cold.execute({'iap':'fixture'},recover_only=True),result)
            rerank.assert_called_once();planner.planner.assert_called_once()
            for call in runtime.retrieval.execute.call_args_list:self.assertIs(call.kwargs['recover_only'],True)
            self.assertIsNone(runtime._headers);self.assertIsNone(runtime._bound);self.assertIsNone(runtime._parent)

    def test_recovery_missing_original_has_no_new_model_calls(self):
        with worker_operation_scope(time.monotonic()+60):
            runtime,rerank,planner,_,store=self.fixture()
            with self.assertRaises(ValueError):runtime.execute({},recover_only=True)
            rerank.assert_not_called();planner.planner.assert_not_called();store.reserve.assert_not_called()
            self.assertIsNone(runtime._headers)

    def test_trusted_continuation_uses_bound_context_and_failure_clears_it(self):
        with worker_operation_scope(time.monotonic()+60):
            runtime,_,_,_,_=self.fixture()
            def consume(prepared,receipt):
                self.assertEqual(runtime._headers,{'iap':'fixture'})
                self.assertIsNotNone(runtime._bound);self.assertIsNotNone(runtime._parent)
                self.assertEqual(receipt['artifact_status'],'NOT_EXECUTED')
                raise ValueError('Continuation stopped')
            with self.assertRaisesRegex(ValueError,'Continuation stopped'):
                runtime.execute_bound({'iap':'fixture'},consume)
            self.assertIsNone(runtime._headers);self.assertIsNone(runtime._bound);self.assertIsNone(runtime._parent)

    def test_artifact_continuation_cannot_reset_the_original_worker_deadline(self):
        from unittest.mock import patch
        from src.application.operation_budget import check_operation,OperationStopped
        clock=[0.]
        with self.assertRaises(OperationStopped),patch('src.application.operation_budget.time.monotonic',side_effect=lambda:clock[0]),worker_operation_scope(100):
            runtime,rerank,planner,_,_=self.fixture()
            def consume(prepared,receipt):
                clock[0]=101.
                with worker_operation_scope(1000):check_operation()
            try:runtime.execute_bound({'iap':'fixture'},consume)
            finally:
                self.assertIsNone(runtime._headers);self.assertIsNone(runtime._bound)
                rerank.assert_called_once();planner.planner.assert_called_once()

    def test_source_change_or_parent_failure_stops_before_model(self):
        with worker_operation_scope(time.monotonic()+60):
            for fault in ('source','parent','image'):
                runtime,rerank,planner,refresh,store=self.fixture()
                if fault=='source':refresh.return_value=False
                elif fault=='parent':runtime.retrieval.execute.side_effect=ValueError('Original unavailable')
                else:runtime.planner.settings.binding_id='eval_upload_binding_v1_'+'0'*64
                with self.subTest(fault=fault),self.assertRaises(ValueError):runtime.execute({})
                rerank.assert_not_called();planner.planner.assert_not_called();store.reserve.assert_not_called()
                self.assertIsNone(runtime._headers)

    def test_legacy_run_slot_and_disabled_configuration_deny(self):
        with worker_operation_scope(time.monotonic()+60):
            runtime,rerank,planner,_,store=self.fixture()
            with self.assertRaises(ValueError):ProtectedRetrievedPlanner(runtime.planner,runtime.retrieval,
                replace(runtime.settings,run_id=runtime.planner.settings.run_id))
            runtime.settings=replace(runtime.settings,enabled=False)
            with self.assertRaises(ValueError):runtime.execute({})
            rerank.assert_not_called();planner.planner.assert_not_called();store.reserve.assert_not_called()
