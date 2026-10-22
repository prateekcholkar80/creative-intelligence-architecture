import hashlib,time
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_evaluator_transport import PinnedVertexEvaluatorTransport,VERSION
from src.application.campaign_evaluator_observer import EvaluatorProfile
from src.application.campaign_evaluator_store import canonical
from src.application.operation_budget import worker_operation_scope


class EvaluatorTransportTests(TestCase):
    def test_planner_transmits_exact_inline_originals_in_order_without_uri_fetching(self):
        from src.generators.campaign_brief_generator import response_schema
        from tests.test_campaign_planner_projection import options
        import json
        factory=Mock();client=Mock();factory.return_value.__enter__=Mock(return_value=client);factory.return_value.__exit__=Mock(return_value=False)
        client.models.generate_content.return_value=Mock(usage_metadata=None,model_version='exact-model')
        images=(('asset','123','image/webp',b'exact original'),)
        with patch('src.application.campaign_evaluator_transport.genai.Client',factory),patch('src.application.campaign_evaluator_transport.complete_response_text',return_value='{}'):
            with worker_operation_scope(time.monotonic()+30):PinnedVertexEvaluatorTransport().planner(self.profile(),'system','context',response_schema(json.loads(options())),images)
        parts=client.models.generate_content.call_args.kwargs['contents'][0].parts
        self.assertEqual(parts[0].text,'context');self.assertEqual(parts[1].inline_data.data,b'exact original')
        self.assertEqual(parts[1].inline_data.mime_type,'image/webp')
        self.assertEqual(factory.call_args.kwargs['http_options'].retry_options.attempts,1)

    def test_planner_rejects_changed_template_or_duplicate_assets_before_provider(self):
        from src.generators.campaign_brief_generator import response_schema
        from tests.test_campaign_planner_projection import options
        import json
        schema=response_schema(json.loads(options()))
        with patch('src.application.campaign_evaluator_transport.genai.Client') as factory:
            with worker_operation_scope(time.monotonic()+30),self.assertRaises(ValueError):
                PinnedVertexEvaluatorTransport().planner(self.profile(),'changed','context',schema,(('asset','123','image/webp',b'original'),))
            with worker_operation_scope(time.monotonic()+30),self.assertRaises(ValueError):
                PinnedVertexEvaluatorTransport().planner(self.profile(),'system','context',schema,(('asset','123','image/webp',b'original'),)*2)
        factory.assert_not_called()

    def profile(self,change=None):
        value=dict(version=VERSION,source_snapshot_sha256='a'*64,generation_config=dict(automatic_function_calling_disabled=True,thinking_level=None,max_output_tokens=2048),safety_settings=[])
        if change:value.update(change)
        return EvaluatorProfile('exact-model',canonical(value),hashlib.sha256(b'system').hexdigest())
    def test_exact_model_budget_single_attempt_and_unknown_usage(self):
        factory=Mock();client=Mock()
        factory.return_value.__enter__=Mock(return_value=client);factory.return_value.__exit__=Mock(return_value=False)
        response=Mock(usage_metadata=None,model_version='exact-model');client.models.generate_content.return_value=response
        with patch('src.application.campaign_evaluator_transport.genai.Client',factory),patch('src.application.campaign_evaluator_transport.complete_response_text',return_value='{}'):
            with worker_operation_scope(time.monotonic()+30):result=PinnedVertexEvaluatorTransport()(self.profile(),'system','{}',('A02',))
        self.assertEqual(client.models.generate_content.call_args.kwargs['model'],'exact-model')
        options=factory.call_args.kwargs['http_options'];self.assertEqual(options.retry_options.attempts,1)
        self.assertLessEqual(options.timeout,30000);self.assertIsNone(result.observation.input_tokens)
        self.assertEqual(result.observation.response_sha256,hashlib.sha256(b'{}').hexdigest())
    def test_no_worker_or_config_override_never_calls_provider(self):
        with patch('src.application.campaign_evaluator_transport.genai.Client') as factory:
            with self.assertRaises(ValueError):PinnedVertexEvaluatorTransport()(self.profile(),'system','{}',('A02',))
            bad=self.profile(dict(generation_config=dict(automatic_function_calling_disabled=True,thinking_level=None,tools=[])))
            with worker_operation_scope(time.monotonic()+30),self.assertRaises(ValueError):PinnedVertexEvaluatorTransport()(bad,'system','{}',('A02',))
            factory.assert_not_called()
