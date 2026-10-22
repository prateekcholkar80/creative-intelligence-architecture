import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.generators.campaign_brief_generator import (
    assemble_brief, creative_options, export_brief, generate_grounded_brief,
    render_brief, select_creative, validate_config, validate_draft,
)
from tests import test_evidence_packet

MODULE = 'src.generators.campaign_brief_generator.'


class CampaignBriefTests(unittest.TestCase):
    def setUp(self):
        fixture = test_evidence_packet.EvidencePacketTests();fixture.setUp()
        self.request = fixture.request
        self.config = dict(model_id='gemini-2.5-flash',location='asia-south1',temperature=0.2,
                           seed=42,max_output_tokens=4096)
        fixture.visual['visual_chunks'] = [
            dict(source_reference='https://example.com/#colors',chunk_text='#FFFFFF #F5F4F0 #E3C499 #B2BCB8 #000000 #373737',
                 evidence_references=['https://example.com/#colors']),
            dict(source_reference='https://example.com/#typography',chunk_text='Canela-Thin-Web BrownStd-Regular',
                 evidence_references=['https://example.com/#typography'])]
        fixture.sales.update(start_date='2025-01-01',end_date='2025-12-31',
            windows=dict(RECENT=dict(first_date='2025-12-02',last_date='2025-12-31',units_sold=50),
                         PREVIOUS=dict(first_date='2025-11-02',last_date='2025-12-01',units_sold=60)),
            trends=dict(units_change=-10),evidence_references=['sales-table#scope'])
        fixture.sales['totals'].update(units_sold=800,orders=700)
        fixture.performance['summaries'][0].update(ctr=None,conversion_rate=None,roas=None,
                                                   evidence_references=['performance-table#campaign'])
        self.bundle = fixture.assemble()
        self.bundle.context['approval_recheck_passed'] = True
        self.bundle.context['brand_context']['approved_claims'][0]['evidence_references'] = ['approval#claim']
        options = creative_options(self.bundle.context)
        self.draft = {key:values[0] for key,values in options.items()}
        self.model = Mock()
        self.model.models.generate_content.return_value = Mock(
            text=json.dumps(self.draft),model_version='gemini-2.5-flash',usage_metadata=None,
            prompt_feedback=None,candidates=[Mock(finish_reason='STOP',safety_ratings=[])])

    def test_original_images_are_sent_inline_with_cloud_evidence_and_fixed_settings(self):
        with patch('pathlib.Path.read_text',side_effect=AssertionError('No local evidence fallback')):
            draft,metadata = select_creative(self.model,self.bundle,self.config)
        call = self.model.models.generate_content.call_args.kwargs
        parts = call['contents'].parts
        actual = [part.inline_data.data for part in parts if part.inline_data is not None]
        self.assertEqual(actual,list(self.bundle.image_bytes.values()))
        self.assertIn('BIGQUERY',parts[0].text)
        self.assertEqual(call['config'].seed,42)
        self.assertEqual(call['config'].response_mime_type,'application/json')
        self.assertEqual(draft,self.draft)
        self.assertEqual(metadata['input_image_asset_ids'],list(self.bundle.image_bytes))

    def test_claim_injection_extra_statistics_and_unknown_reference_are_rejected(self):
        options = creative_options(self.bundle.context)
        for key,value in [('headline','Reduces wrinkles by 40%'),('body','10% niacinamide'),
                          ('cta','Save 20%'),('reference_campaign_id','another-sku'),('background','#FF0000')]:
            with self.subTest(key=key),self.assertRaises(ValueError):
                validate_draft({**self.draft,key:value},options)
        for draft in ({**self.draft,'roas':'12'},['Not JSON object'],{k:v for k,v in self.draft.items() if k != 'headline'}):
            with self.assertRaises(ValueError):validate_draft(draft,options)

    def test_palette_and_fonts_require_actual_style_evidence(self):
        context = copy.deepcopy(self.bundle.context)
        context['visual_reference']['visual_chunks'][0]['chunk_text'] = '#FFFFFF #000000'
        options = creative_options(context)
        self.assertEqual(options['background'],['#FFFFFF'])
        self.assertEqual(options['text_color'],['#000000'])
        context['visual_reference']['visual_chunks'][1]['chunk_text'] = 'Other font'
        with self.assertRaises(ValueError):creative_options(context)
        context['visual_reference']['approval_scope'] = 'MANUFACTURER_POLICY'
        with self.assertRaises(ValueError):creative_options(context)

    def test_missing_identity_approval_or_stale_bundle_cannot_generate(self):
        for edit in ('no-approval','stale','benefit'):
            context = copy.deepcopy(self.bundle.context)
            if edit == 'no-approval':context['generation_constraints']['allowed_identity_wording'] = []
            elif edit == 'stale':context['approval_recheck_passed'] = False
            else:context['generation_constraints']['benefit_claims_authorized'] = True
            with self.assertRaises(ValueError):creative_options(context)

    def test_none_styling_and_empty_history_do_not_invent_citations_or_campaigns(self):
        context = copy.deepcopy(self.bundle.context)
        context['visual_reference'] = dict(visual_reference='NONE',approval_scope=None,visual_chunks=[])
        context['campaign_creatives']['creatives'] = []
        context['campaign_performance']['summaries'] = []
        options = creative_options(context);draft = {k:v[0] for k,v in options.items()}
        brief = assemble_brief(context,draft,{})
        self.assertIsNone(brief['historical_reference'])
        self.assertEqual(brief['visual_direction']['scope'],'PROJECT_DESIGN_PROPOSAL')
        self.assertEqual(brief['visual_direction']['evidence_ids'],[])

    def test_requested_channel_precedes_unmatched_cross_channel_performance(self):
        context = copy.deepcopy(self.bundle.context)
        extra = copy.deepcopy(context['campaign_creatives']['creatives'][0])
        extra.update(campaign_id='synthetic-email',channel='EMAIL',objective='CONVERSION')
        context['campaign_creatives']['creatives'].append(extra)
        self.assertEqual(creative_options(context)['reference_campaign_id'],[self.draft['reference_campaign_id']])

    def test_facts_and_ratios_come_from_bigquery_and_citation_roles_are_separate(self):
        brief = assemble_brief(self.bundle.context,self.draft,{})
        self.assertEqual(brief['grounded_evidence']['sales']['totals']['net_sales'],self.bundle.context['sales_analytics']['totals']['net_sales'])
        self.assertIsNone(brief['grounded_evidence']['campaign_performance']['summaries'][0]['roas'])
        self.assertEqual(brief['copy']['headline'],'Source product')
        self.assertEqual(brief['copy']['claim_ids'],['claim-identity'])
        refs = {e['evidence_id']:e['reference'] for e in brief['evidence_index']}
        self.assertEqual([refs[e] for e in brief['copy']['evidence_ids']],['approval#claim'])
        self.assertTrue(all(refs[e].startswith('https://example.com/') for e in brief['visual_direction']['evidence_ids']))
        self.assertFalse(brief['historical_reference']['objective_match'])
        self.assertIsNone(brief['experiment']['expected_lift'])
        text = render_brief(brief)
        self.assertIn('Exact objective/channel history available: False',text)
        self.assertIn('not observed business performance',text)
        self.assertIn('12345.67 EUR',text)

    def test_corrupt_missing_or_unlisted_images_prevent_model_call(self):
        asset = next(iter(self.bundle.image_bytes))
        for images in ({**self.bundle.image_bytes,asset:b'altered'},
                       {k:v for k,v in self.bundle.image_bytes.items() if k != asset},
                       {**self.bundle.image_bytes,'unlisted':b'extra'}):
            bundle = copy.deepcopy(self.bundle);bundle.image_bytes.clear();bundle.image_bytes.update(images)
            with self.assertRaises(ValueError):select_creative(self.model,bundle,self.config)
        self.model.models.generate_content.assert_not_called()

    def test_empty_malformed_or_forbidden_model_responses_fail_without_fallback(self):
        for response in ('','not JSON',json.dumps({**self.draft,'headline':'New benefit'})):
            self.model.models.generate_content.return_value.text = response
            with self.assertRaises((ValueError,json.JSONDecodeError)):
                select_creative(self.model,self.bundle,self.config)

    def cloud_mocks(self):
        self.retrieve = patch(MODULE+'retrieve_evidence_packet',return_value=self.bundle).start()
        self.addCleanup(patch.stopall)
        self.brand = patch(MODULE+'retrieve_brand_context',return_value=copy.deepcopy(self.bundle.context['brand_context'])).start()
        self.product = patch(MODULE+'retrieve_product_knowledge',return_value=copy.deepcopy(self.bundle.context['product_knowledge'])).start()

    def generate(self):
        return generate_grounded_brief(Mock(),Mock(),self.model,self.request,{}, {},self.config)

    def test_fresh_cloud_retrieval_and_post_generation_approval_recheck(self):
        self.cloud_mocks()
        with patch('pathlib.Path.read_text',side_effect=AssertionError('No serving file reads')):
            brief,bundle = self.generate()
        self.retrieve.assert_called_once();self.brand.assert_called_once();self.product.assert_called_once()
        self.assertTrue(brief['approval_recheck_passed']);self.assertIs(bundle,self.bundle)

    def test_approval_revocation_policy_and_product_changes_abort_after_model_call(self):
        self.cloud_mocks()
        for target,key,value in ((self.brand,'approved_claims',[]),(self.brand,'poc_rules',[]),
                                 (self.product,'snapshot_id','changed')):
            original = target.return_value
            target.return_value = {**original,key:value}
            with self.assertRaisesRegex(ValueError,'changed during generation'):self.generate()
            target.return_value = original

    def test_bigquery_or_model_errors_propagate_and_do_not_export(self):
        self.cloud_mocks()
        self.retrieve.side_effect = RuntimeError('BigQuery unavailable')
        with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):self.generate()
        self.model.models.generate_content.assert_not_called()
        self.retrieve.side_effect = None
        self.model.models.generate_content.side_effect = RuntimeError('Model unavailable')
        with self.assertRaisesRegex(RuntimeError,'Model unavailable'):self.generate()
        self.brand.assert_not_called()

    def test_review_export_is_unique_retains_exact_images_and_has_no_serving_read(self):
        self.cloud_mocks();brief,bundle = self.generate()
        with tempfile.TemporaryDirectory() as tmp:
            output = export_brief(brief,bundle,Path(tmp));second = export_brief(brief,bundle,Path(tmp))
            self.assertNotEqual(output,second)
            self.assertEqual(json.loads(output.read_text())['source_system'],'SYNTHETIC_POC')
            self.assertTrue((output.parent/'campaign_brief.txt').is_file())
            evidence_file = next((output.parent/'evidence').rglob('evidence_packet.json'))
            evidence = json.loads(evidence_file.read_text())
            for image in evidence['image_manifest']:
                self.assertEqual(Path(image['local_review_path']).read_bytes(),bundle.image_bytes[image['asset_id']])
            brief['approval_recheck_passed'] = False
            with self.assertRaises(ValueError):export_brief(brief,bundle,Path(tmp))

    def test_config_rejects_wrong_region_invalid_bounds_and_secrets(self):
        validate_config(self.config)
        for field,value in [('location','global'),('temperature',float('nan')),('seed',True),('max_output_tokens',0)]:
            with self.assertRaises(ValueError):validate_config({**self.config,field:value})
        with self.assertRaises(ValueError):validate_config({**self.config,'api_key':'secret'})

    def test_gemini3_uses_thinking_levels_and_safety_without_legacy_sampling(self):
        config = dict(model_id='gemini-3.5-flash',location='asia-south1',thinking_level='LOW',
                      seed=42,max_output_tokens=4096)
        _, metadata = select_creative(self.model,self.bundle,config)
        call = self.model.models.generate_content.call_args.kwargs
        self.assertEqual(call['model'],'gemini-3.5-flash')
        self.assertEqual(call['config'].thinking_config.thinking_level,'LOW')
        self.assertIsNone(call['config'].thinking_config.thinking_budget)
        self.assertIsNone(call['config'].temperature)
        self.assertTrue(call['config'].automatic_function_calling.disable)
        self.assertEqual(len(call['config'].safety_settings),4)
        self.assertEqual(metadata['generation_settings']['thinking_level'],'LOW')
        self.assertEqual(metadata['response_validation']['finish_reason'],'STOP')

    def test_valid_json_with_token_cutoff_or_safety_verdict_cannot_create_brief(self):
        self.cloud_mocks()
        for reason in ('MAX_TOKENS','SAFETY','RECITATION','OTHER',None):
            self.model.models.generate_content.return_value.candidates[0].finish_reason = reason
            with self.subTest(reason=reason),self.assertRaises(ValueError):self.generate()
        self.brand.assert_not_called()

    def test_new_model_config_rejects_legacy_settings_unknown_models_and_invalid_levels(self):
        config = dict(model_id='gemini-3.5-flash',location='asia-south1',thinking_level='LOW',
                      seed=42,max_output_tokens=4096)
        for invalid in ({**config,'temperature':0.2},{**config,'thinking_budget':0},
                        {**config,'thinking_level':'UNLIMITED'},{**config,'thinking_level':[]},
                        {**config,'model_id':'gemini-unvalidated'},{**config,'location':'global'}):
            with self.subTest(config=invalid),self.assertRaises(ValueError):validate_config(invalid)

    def test_gemini3_null_body_means_no_copy_and_cannot_remove_required_identity(self):
        config=dict(model_id='gemini-3.5-flash',location='asia-south1',thinking_level='LOW',
                    seed=42,max_output_tokens=4096)
        response=self.model.models.generate_content.return_value
        response.text=json.dumps({**self.draft,'body':None})
        draft,_=select_creative(self.model,self.bundle,config)
        self.assertEqual(draft['body'],'')
        schema=self.model.models.generate_content.call_args.kwargs['config'].response_schema
        self.assertTrue(schema['properties']['body']['nullable'])
        self.assertNotIn('',schema['properties']['body']['enum'])
        response.text=json.dumps({**self.draft,'headline':None})
        with self.assertRaises(ValueError):select_creative(self.model,self.bundle,config)


if __name__ == '__main__':
    unittest.main()
