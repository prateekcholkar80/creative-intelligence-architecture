from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.interpreter_serving_composition import validate_config,InterpreterServingSubmission,provider_call

class ServingCompositionTests(TestCase):
    def config(self):return dict(enabled=False,policy=dict(policy_id='policy',version='v1',policy_sha256='a'*64),
        targets=[['campaign-evidence-intent','123','1'],['campaign-evidence-questions','456','1']])
    def test_configuration_never_accepts_browser_pins_or_arbitrary_targets(self):
        self.assertEqual(validate_config(self.config()),self.config())
        for value in (self.config()|{'approval':True},self.config()|{'enabled':'true'},self.config()|{'targets':[['other','123','1'],['campaign-evidence-questions','456','1']]}):
            with self.assertRaises(ValueError):validate_config(value)
    def test_unknown_submission_does_not_dispatch(self):
        service=Mock();processor=Mock();service.submit.return_value=SimpleNamespace(native_status='UNKNOWN')
        result=InterpreterServingSubmission(service,processor).submit({},message='raw')
        self.assertEqual(result.native_status,'UNKNOWN');processor.process.assert_not_called()
    def test_confirmed_submission_enters_gated_processor_without_releasing_text(self):
        service=Mock();processor=Mock();receipt=SimpleNamespace(native_status='COMMITTED')
        service.submit.return_value=receipt
        result=InterpreterServingSubmission(service,processor).submit({},message='raw')
        self.assertIs(result,receipt);processor.process.assert_called_once_with({},receipt)
    def test_provider_uses_exact_settings_and_no_automatic_tools_or_retry(self):
        import json
        snapshot=SimpleNamespace(generation_config_json=json.dumps(dict(automatic_function_calling_disabled=True,
            max_output_tokens=1024,response_mime_type='application/json',seed=42,temperature=None,thinking_level='MINIMAL')),
            safety_settings_json='[{"category":"HARM_CATEGORY_HARASSMENT","threshold":"BLOCK_LOW_AND_ABOVE"}]')
        client=Mock();client.__enter__=Mock(return_value=client);client.__exit__=Mock()
        with patch('src.application.interpreter_serving_composition.genai.Client',return_value=client) as factory:
            provider_call(dict(timeout_ms=10000,model_id='model'),'system','prompt',{'type':'OBJECT'},snapshot)
        config=client.models.generate_content.call_args.kwargs['config']
        self.assertTrue(config.automatic_function_calling.disable)
        self.assertEqual(config.safety_settings[0].threshold.value,'BLOCK_LOW_AND_ABOVE')
        self.assertEqual(factory.call_args.kwargs['http_options'].retry_options.attempts,1)
