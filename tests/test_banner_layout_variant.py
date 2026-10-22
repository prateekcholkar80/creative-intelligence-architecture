import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.generators.banner_layout_variant import generate_variant
from src.generators.banner_preview_generator import export_preview, render_preview
from src.ingestion.register_generated_banner import revalidate_candidate
from src.retrieval.campaign_performance import json_value
from tests import test_banner_preview_generator

MODULE = 'src.generators.banner_layout_variant.'


class LayoutVariantTests(unittest.TestCase):
    def setUp(self):
        fixture = test_banner_preview_generator.BannerPreviewTests();fixture.setUp()
        self.bundle = fixture.bundle
        self.brief = fixture.brief
        self.config = fixture.config
        self.request = fixture.request
        original = render_preview(self.brief,self.bundle,self.config)
        self.asset_id = 'SYN-GEN-CI-SKU-0001-'+original.metadata['sha256']
        self.control = dict(asset_id=self.asset_id,content_sha256=original.metadata['sha256'],
            gcs_generation='123',snapshot_id=self.bundle.context['snapshot_id'],
            review=dict(review_id='review-approved',revision=2,status='APPROVED_FOR_POC',
                        approval_scope='POC_VISUAL_REUSE_ONLY'))
        self.asset = dict(asset_id=self.asset_id,sha256=original.metadata['sha256'],gcs_generation='123',
            brief_json=json.dumps(self.brief,default=json_value))
        self.control_png = original.png
        self.reader = patch(MODULE+'retrieve_generated_banner',return_value=(self.control,self.control_png)).start()
        self.asset_reader = patch(MODULE+'read_asset',return_value=self.asset).start()
        self.evidence = patch(MODULE+'retrieve_evidence_packet',return_value=self.bundle).start()
        self.recheck = patch(MODULE+'recheck_sources').start()
        self.addCleanup(patch.stopall)
        self.model = Mock()

    def generate(self, layout='PRODUCT_LEFT_COPY_RIGHT'):
        return generate_variant(Mock(),Mock(),self.model,self.request,self.asset_id,{}, {},
                                self.config,{}, {},layout)

    def test_only_layout_changes_copy_source_fonts_palette_and_pixels_are_preserved(self):
        preview,brief,bundle,report,png = self.generate()
        self.assertEqual(brief['copy'],self.brief['copy'])
        expected = {**self.brief['model_selection'],'layout':'PRODUCT_LEFT_COPY_RIGHT'}
        self.assertEqual(brief['model_selection'],expected)
        self.assertEqual(preview.metadata['original_image'],render_preview(self.brief,bundle,self.config).metadata['original_image'])
        self.assertNotEqual(preview.png,png)
        self.assertEqual(report['changed_creative_fields'],['layout'])
        self.assertFalse(report['cloud_writes'])
        self.assertEqual(report['variant_review_status'],'NOT_REGISTERED')
        self.assertEqual(brief['model']['selection_method'],'DETERMINISTIC_LAYOUT_VARIANT')
        self.assertEqual(report['control_review']['revision'],2)
        self.assertTrue(all(preview.metadata['validation'].values()))
        self.model.models.generate_content.assert_not_called()
        self.recheck.assert_called_once()
        self.assertEqual(self.reader.call_count,2)

    def test_repeat_is_deterministic_and_does_not_mutate_control_proposal(self):
        original = copy.deepcopy(self.asset)
        self.assertEqual(self.generate()[0].png,self.generate()[0].png)
        self.assertEqual(self.asset,original)

    def test_unsupported_same_layout_and_nonlayout_experiment_are_rejected(self):
        for layout in ('UNSUPPORTED','PRODUCT_RIGHT_COPY_LEFT'):
            with self.assertRaises(ValueError):self.generate(layout)
        proposal = copy.deepcopy(self.brief);proposal['model_selection']['test_variable']='CTA'
        self.asset['brief_json']=json.dumps(proposal,default=json_value)
        with self.assertRaisesRegex(ValueError,'layout experiment'):self.generate()

    def test_cross_request_control_is_rejected_before_evidence_retrieval(self):
        proposal = copy.deepcopy(self.brief);proposal['request']['canonical_sku']='CI-SKU-0002'
        self.asset['brief_json']=json.dumps(proposal,default=json_value)
        with self.assertRaisesRegex(ValueError,'requested campaign'):self.generate()
        self.evidence.assert_not_called()

    def test_local_files_cannot_supply_serving_evidence_and_cloud_errors_propagate(self):
        with patch('pathlib.Path.read_text',side_effect=AssertionError('No local serving fallback')):
            self.generate()
        self.evidence.side_effect = RuntimeError('BigQuery unavailable')
        with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):self.generate()

    def test_revoked_control_approval_propagates_and_no_variant_is_rendered(self):
        self.reader.side_effect = ValueError('No longer eligible')
        with patch(MODULE+'render_preview') as renderer:
            with self.assertRaisesRegex(ValueError,'No longer eligible'):self.generate()
        renderer.assert_not_called()

    def test_control_hash_generation_snapshot_or_reconstruction_mismatch_aborts(self):
        for key,value in (('content_sha256','changed'),('gcs_generation','456'),('snapshot_id','old')):
            with self.subTest(key=key):
                self.reader.return_value = ({**self.control,key:value},self.control_png)
                with self.assertRaisesRegex(ValueError,'differs from current'):self.generate()
        self.reader.return_value = (self.control,b'altered PNG')
        with self.assertRaisesRegex(ValueError,'differs from current'):self.generate()

    def test_control_revision_or_source_change_during_render_aborts(self):
        changed = copy.deepcopy(self.control);changed['review']['revision']=3
        self.reader.side_effect = [(self.control,self.control_png),(changed,self.control_png)]
        with self.assertRaisesRegex(ValueError,'changed during rendering'):self.generate()
        self.reader.side_effect = None
        self.recheck.side_effect = ValueError('Source changed')
        with self.assertRaisesRegex(ValueError,'Source changed'):self.generate()

    def test_export_is_compatible_with_existing_registration_reconstruction(self):
        preview,brief,bundle,report,png = self.generate()
        with tempfile.TemporaryDirectory() as tmp:
            path = export_preview(preview,brief,bundle,Path(tmp))
            with patch('src.ingestion.register_generated_banner.retrieve_evidence_packet',return_value=bundle):
                reconstructed,rebuilt,_ = revalidate_candidate(
                    Mock(),Mock(),self.model,self.request,path.parent,{}, {})
            self.assertEqual(reconstructed.png,preview.png)
            self.assertEqual(rebuilt['model_selection'],brief['model_selection'])


if __name__ == '__main__':
    unittest.main()
