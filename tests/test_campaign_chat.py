import copy
import json
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import Forbidden, ServiceUnavailable
from google.auth.exceptions import DefaultCredentialsError

from src.application.campaign_chat import (
    FIELDS, STAGES, CampaignChatService, ChatError, Interpretation, interpret_message,
    load_settings, read_catalog, safe_error, validate_catalog, validate_preferences,
)
from src.generators.banner_preview_generator import render_preview
from tests import test_banner_preview_generator as banner_fixtures

MODULE = 'src.application.campaign_chat.'


def catalog():
    # Test fixture only: no ingestion or serving fallback.
    return [dict(sku=f'CI-SKU-{i:04d}', brand='Test brand', product_name=f'Product {i}',
                 source_system='BEAUTYFEEDS') for i in range(1, 21)]


def preferences():
    return dict(canonical_sku='CI-SKU-0001', objective='CONVERSION', audience='Skincare shoppers',
                channel='PAID_SOCIAL', visual_reference='NONE')


class InterpretationTests(unittest.TestCase):
    def setUp(self):
        self.model = Mock()
        self.settings = load_settings()
        self.previous = dict.fromkeys(FIELDS, '')
        self.payload = dict(preferences=preferences(), unsupported_features=[])

    def run_interpretation(self, payload=None, **response_fields):
        response = Mock(text=json.dumps(self.payload if payload is None else payload),
                        candidates=[Mock(finish_reason='STOP',safety_ratings=[])], model_version='gemini-2.5-flash',
                        usage_metadata=None,prompt_feedback=None)
        for key, value in response_fields.items():
            setattr(response, key, value)
        self.model.models.generate_content.return_value = response
        return interpret_message(self.model, 'Create a campaign', catalog(), self.previous,
                                 self.settings, 'gemini-2.5-flash')

    def test_complete_request_is_validated_and_has_no_freeform_fact_output(self):
        result = self.run_interpretation()
        self.assertEqual(result.request().to_dict(), preferences())
        self.assertEqual(result.missing, ())
        call = self.model.models.generate_content.call_args.kwargs
        self.assertEqual(call['config'].temperature, 0)
        self.assertEqual(call['config'].thinking_config.thinking_budget, 0)
        self.assertTrue(call['config'].automatic_function_calling.disable)
        self.assertEqual(len(call['config'].safety_settings), 4)
        self.assertIn('untrusted', call['config'].system_instruction)
        self.assertEqual(json.loads(call['contents'])['catalog'], catalog())

    def test_missing_details_require_clarification_instead_of_inventing_defaults(self):
        self.payload['preferences'].update(audience='', channel='')
        result = self.run_interpretation()
        self.assertEqual(result.missing, ('audience', 'channel'))
        with self.assertRaises(ChatError):
            result.request()

    def test_unsupported_claims_and_discounts_cannot_generate(self):
        self.payload['unsupported_features'] = ['BENEFIT_CLAIMS', 'DISCOUNTS']
        result = self.run_interpretation()
        with self.assertRaises(ChatError):
            result.request()

    def test_unknown_sku_enum_extra_field_or_long_audience_is_rejected(self):
        for key, value in [('canonical_sku', 'OTHER-0001'), ('objective', 'CURE_ACNE'),
                           ('channel', 'BILLBOARD'), ('audience', 'x'*201), ('claim', 'Guaranteed cure')]:
            payload = copy.deepcopy(self.payload)
            payload['preferences'][key] = value
            with self.subTest(key=key), self.assertRaises(ChatError):
                self.run_interpretation(payload)

    def test_invalid_schema_or_classifier_is_rejected(self):
        for payload in ([1], dict(self.payload, facts='new facts'),
                        dict(self.payload, unsupported_features='DISCOUNTS'),
                        dict(self.payload, unsupported_features=['UNKNOWN'])):
            with self.assertRaises(ChatError):
                self.run_interpretation(payload)

    def test_safety_block_token_cutoff_and_missing_candidates_stop_interpretation(self):
        for fields in (dict(candidates=[Mock(finish_reason='SAFETY')], text=None),
                       dict(candidates=[Mock(finish_reason='MAX_TOKENS')]), dict(candidates=[])):
            with self.assertRaisesRegex(ChatError, 'safely and completely'):
                self.run_interpretation(**fields)

    def test_malformed_json_is_a_safe_error(self):
        with self.assertRaisesRegex(ChatError, 'interpretation was invalid'):
            self.run_interpretation(text='not JSON')

    def test_empty_and_overlong_messages_make_no_model_call(self):
        for message in (' ', 'x'*(self.settings['max_message_characters']+1)):
            with self.assertRaises(ChatError):
                interpret_message(self.model, message, catalog(), self.previous, self.settings, 'gemini-2.5-flash')
        self.model.models.generate_content.assert_not_called()

    def test_followup_passes_previous_preferences_and_unsupported_requirements(self):
        self.run_interpretation()
        interpret_message(self.model, 'Use email instead', catalog(), preferences(), self.settings,
                          'gemini-2.5-flash', ('BENEFIT_CLAIMS',))
        contents = json.loads(self.model.models.generate_content.call_args.kwargs['contents'])
        self.assertEqual(contents['previous_unsupported_features'], ['BENEFIT_CLAIMS'])
        self.assertEqual(contents['previous_preferences'], preferences())

    def test_catalog_requires_unique_real_canonical_products(self):
        for rows in (catalog()[:-1], catalog()+[catalog()[0]],
                     [dict(r, source_system='SYNTHETIC_POC') for r in catalog()],
                     [dict(r, sku='REMAPPED') for r in catalog()]):
            with self.assertRaises(ChatError):
                validate_catalog(rows)

    def test_catalog_read_uses_live_parameterized_bounded_query(self):
        client = Mock()
        client.query.return_value.result.return_value = catalog()
        with patch('pathlib.Path.read_text', side_effect=AssertionError('No factual file fallback')):
            self.assertEqual(read_catalog(client, 64*1024*1024), catalog())
        call = client.query.call_args
        self.assertIn('product_master', call.args[0])
        self.assertNotIn('INSERT', call.args[0])
        self.assertEqual(call.kwargs['job_config'].maximum_bytes_billed, 64*1024*1024)

    def test_safe_errors_do_not_expose_raw_cloud_messages(self):
        for error, code in [(DefaultCredentialsError('secret'), 'AUTHENTICATION_REQUIRED'),
                            (Forbidden('secret'), 'ACCESS_DENIED'),
                            (ServiceUnavailable('secret'), 'CLOUD_UNAVAILABLE'),
                            (ValueError('secret'), 'GROUNDING_CHECK_FAILED'),
                            (RuntimeError('secret'), 'GENERATION_FAILED')]:
            result = safe_error(error)
            self.assertEqual(result.code, code)
            self.assertNotIn('secret', result.message)

    def test_gemini3_interpretation_preserves_claim_blocking_with_compatible_settings(self):
        self.payload['unsupported_features'] = ['BENEFIT_CLAIMS']
        self.run_interpretation()
        result = interpret_message(self.model,'Add an acne cure claim',catalog(),self.previous,
                                   self.settings,'gemini-3.5-flash')
        with self.assertRaises(ChatError):result.request()
        config = self.model.models.generate_content.call_args.kwargs['config']
        self.assertEqual(config.thinking_config.thinking_level,'MINIMAL')
        self.assertIsNone(config.thinking_config.thinking_budget)
        self.assertIsNone(config.temperature)
        self.assertEqual(result.model['generation_settings']['thinking_level'],'MINIMAL')

    def test_prompt_block_overrides_an_apparently_valid_response(self):
        with self.assertRaisesRegex(ChatError,'safely and completely'):
            self.run_interpretation(prompt_feedback=Mock(block_reason='SAFETY'))

    def test_gemini3_nullable_channel_preserves_clarification_without_defaulting(self):
        self.run_interpretation()
        self.model.models.generate_content.return_value.text = json.dumps(
            dict(self.payload,preferences={**preferences(),'channel':None}))
        result=interpret_message(self.model,'Channel is undecided',catalog(),self.previous,
                                 self.settings,'gemini-3.5-flash')
        self.assertEqual(result.missing,('channel',))
        with self.assertRaises(ChatError):result.request()
        schema=self.model.models.generate_content.call_args.kwargs['config'].response_schema
        channel=schema['properties']['preferences']['properties']['channel']
        self.assertTrue(channel['nullable'])
        self.assertNotIn('',channel['enum'])

    def test_gemini3_null_does_not_permit_unknown_fields_or_null_audience(self):
        self.run_interpretation()
        for preferences_payload in ({**preferences(),'audience':None},{**preferences(),'facts':None}):
            self.model.models.generate_content.return_value.text=json.dumps(
                dict(self.payload,preferences=preferences_payload))
            with self.assertRaises(ChatError):
                interpret_message(self.model,'Campaign',catalog(),self.previous,self.settings,'gemini-3.5-flash')


class ChatWorkflowTests(unittest.TestCase):
    def setUp(self):
        fixture = banner_fixtures.BannerPreviewTests()
        fixture.setUp()
        self.request, self.brief, self.bundle = fixture.request, fixture.brief, fixture.bundle
        self.preview = render_preview(fixture.brief, fixture.bundle, fixture.config)
        self.service = CampaignChatService()
        self.patches = [patch.object(self.service, 'catalog', return_value=catalog()),
                        patch.object(self.service, 'model_client'), patch(MODULE+'bigquery.Client'),
                        patch(MODULE+'storage.Client'), patch(MODULE+'generate_preview')]
        self.catalog_mock, self.model, self.bq, self.gcs, self.generate = [p.start() for p in self.patches]
        for p in self.patches:
            self.addCleanup(p.stop)
        def generate(*args, **kwargs):
            for stage in STAGES:
                kwargs['on_stage'](stage)
            return self.preview, self.brief, self.bundle
        self.generate.side_effect = generate

    def test_existing_grounded_generator_supplies_result_and_stage_events(self):
        events = []
        result = self.service.generate(self.request, 'session', 'request', events.append)
        self.assertEqual(result.envelope['status'], 'SUCCEEDED')
        self.assertEqual(result.png, self.preview.png)
        self.assertEqual([e['stage'] for e in events], list(STAGES))
        self.assertTrue(all(e['request_id']=='request' and e['session_id']=='session' for e in events))
        self.assertFalse(result.envelope['guardrails']['benefit_claims_authorized'])
        self.assertIsNone(result.envelope['cost']['estimated_amount'])
        self.assertEqual(result.envelope['review_status'], 'NOT_REVIEWED')
        self.assertEqual(result.envelope['conversation_persistence'], 'SESSION_ONLY')
        self.bq.return_value.close.assert_called_once()
        self.gcs.return_value.close.assert_called_once()

    def test_grounding_failure_returns_no_preview_or_partial_evidence(self):
        self.generate.side_effect = ValueError('revoked approval')
        result = self.service.generate(self.request, 'session', 'request')
        self.assertEqual(result.envelope['status'], 'BLOCKED')
        self.assertIsNone(result.png)
        self.assertIsNone(result.envelope['brief'])
        self.assertEqual(result.envelope['evidence'], [])

    def test_cloud_failure_is_safe_and_resources_close(self):
        self.generate.side_effect = ServiceUnavailable('sensitive raw response')
        result = self.service.generate(self.request, 'session', 'request')
        self.assertEqual(result.envelope['status'], 'FAILED')
        self.assertEqual(result.envelope['error']['code'], 'CLOUD_UNAVAILABLE')
        self.assertNotIn('sensitive', result.envelope['error']['message'])
        self.bq.return_value.close.assert_called_once()

    def test_catalog_failure_stops_before_any_generation(self):
        self.catalog_mock.side_effect = Forbidden('denied')
        result = self.service.generate(self.request, 'session', 'request')
        self.assertEqual(result.envelope['error']['code'], 'ACCESS_DENIED')
        self.generate.assert_not_called()

    def test_http_client_has_bounded_timeout_and_no_automatic_retry(self):
        with patch(MODULE+'genai.Client') as factory:
            CampaignChatService.model_client(dict(location='asia-south1'), load_settings())
        options = factory.call_args.kwargs['http_options']
        self.assertEqual(options.timeout, 120000)
        self.assertEqual(options.retry_options.attempts, 1)


if __name__ == '__main__':
    unittest.main()
