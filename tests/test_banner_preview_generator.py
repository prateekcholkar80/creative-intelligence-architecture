import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from PIL import Image, ImageDraw, ImageOps

from src.generators.banner_preview_generator import (
    CONFIG, export_preview, generate_preview, render_preview, validate_config,
)
from src.generators.campaign_brief_generator import assemble_brief
from tests import test_campaign_brief_generator

MODULE = 'src.generators.banner_preview_generator.'


class BannerPreviewTests(unittest.TestCase):
    def setUp(self):
        fixture = test_campaign_brief_generator.CampaignBriefTests();fixture.setUp()
        self.bundle = fixture.bundle;self.draft = fixture.draft;self.request = fixture.request
        # Nonuniform corners make cropping, repainting and overlays observable.
        image = Image.new('RGB',(120,180),'#C4B79A');draw = ImageDraw.Draw(image)
        for bounds,color in (((0,0,35,35),'red'),((84,0,119,35),'green'),
                             ((0,144,35,179),'blue'),((84,144,119,179),'black')):
            draw.rectangle(bounds,fill=color)
        buffer = io.BytesIO();image.save(buffer,format='JPEG');raw = buffer.getvalue()
        metadata = self.bundle.context['image_manifest'][0]
        metadata.update(width=120,height=180,byte_count=len(raw),content_sha256=hashlib.sha256(raw).hexdigest())
        self.bundle.image_bytes[metadata['asset_id']] = raw
        self.config = json.loads(CONFIG.read_text())
        self.brief = assemble_brief(self.bundle.context,self.draft,{})
        self.brief['approval_recheck_passed'] = True

    def preview(self):
        return render_preview(self.brief,self.bundle,self.config)

    def test_all_layouts_are_deterministic_and_keep_complete_original_pixel_region(self):
        for layout in ('PRODUCT_RIGHT_COPY_LEFT','PRODUCT_LEFT_COPY_RIGHT','PRODUCT_CENTER_COPY_ABOVE'):
            with self.subTest(layout=layout):
                draft = {**self.draft,'layout':layout}
                brief = assemble_brief(self.bundle.context,draft,{})
                brief['approval_recheck_passed'] = True
                result = render_preview(brief,self.bundle,self.config)
                self.assertEqual(result.png,render_preview(brief,self.bundle,self.config).png)
                image = Image.open(io.BytesIO(result.png))
                self.assertEqual(image.size,(1200,628));self.assertEqual(image.format,'PNG')
                x,y,w,h = result.metadata['geometry']['original_image_bounds']
                original = result.metadata['original_image']
                with Image.open(io.BytesIO(self.bundle.image_bytes[original['asset_id']])) as source:
                    pw,ph = result.metadata['geometry']['regions']['product'][2:]
                    expected = ImageOps.contain(ImageOps.exif_transpose(source).convert('RGB'),
                        (pw-24,ph-24),method=Image.Resampling.LANCZOS)
                self.assertEqual(image.crop((x,y,x+w,y+h)).tobytes(),expected.tobytes())
                self.assertTrue(all(result.metadata['validation'].values()))

    def test_full_headline_is_wrapped_without_truncation_and_boxes_fit(self):
        name = 'Honest Beauty Beauty Sleep Resurfacing Serum 30ml'
        bundle = copy.deepcopy(self.bundle)
        # A live reader would supply the same canonical name consistently in all fields.
        bundle.context['product_knowledge']['product']['product_name'] = name
        bundle.context['generation_constraints']['allowed_identity_wording'] = [name]
        bundle.context['brand_context']['approved_claims'][0]['claim_text'] = name
        brief = assemble_brief(bundle.context,{**self.draft,'headline':name},{})
        brief['approval_recheck_passed'] = True
        result = render_preview(brief,bundle,self.config)
        text = result.metadata['text_layout']['headline']
        self.assertEqual(' '.join(text['lines']),name)
        self.assertGreaterEqual(text['font_size'],self.config['heading_min_size'])
        self.assertEqual(result.metadata['text_layout']['cta']['text'],brief['copy']['cta'])

    def test_unapproved_copy_and_modified_style_or_image_cannot_render(self):
        for section,key,value in (('copy','headline','Removes wrinkles'),('copy','body','10% niacinamide'),
                                  ('copy','cta','Save 20%'),('visual_direction','background','#FF0000'),
                                  ('product_image','treatment','Redraw packaging')):
            brief = copy.deepcopy(self.brief);brief[section][key] = value
            with self.subTest(key=key),self.assertRaises(ValueError):render_preview(brief,self.bundle,self.config)
        bundle = copy.deepcopy(self.bundle)
        asset = self.brief['product_image']['metadata']['asset_id']
        bundle.image_bytes[asset] = b'altered'
        with self.assertRaises(ValueError):render_preview(self.brief,bundle,self.config)

    def test_stale_cross_sku_or_non_poc_brief_is_rejected(self):
        for key,value in (('canonical_sku','CI-SKU-0002'),('snapshot_id','old'),('source_system','REAL'),
                          ('approval_recheck_passed',False),('approval_scope','MANUFACTURER_APPROVED')):
            with self.assertRaises(ValueError):render_preview({**self.brief,key:value},self.bundle,self.config)

    def test_overlong_product_copy_fails_instead_of_truncating(self):
        bundle = copy.deepcopy(self.bundle);name = 'Long source identity '*150
        bundle.context['product_knowledge']['product']['product_name'] = name
        bundle.context['generation_constraints']['allowed_identity_wording'] = [name]
        bundle.context['brand_context']['approved_claims'][0]['claim_text'] = name
        brief = assemble_brief(bundle.context,{**self.draft,'headline':name},{})
        brief['approval_recheck_passed'] = True
        with self.assertRaisesRegex(ValueError,'refusing to truncate'):render_preview(brief,bundle,self.config)

    def test_invalid_dimensions_and_missing_fonts_are_rejected(self):
        for key,value in (('width',600),('height',True),('heading_min_size',8),
                          ('heading_max_size',70),('body_font_path','missing.ttf')):
            with self.assertRaises(ValueError):validate_config({**self.config,key:value})

    def mocks(self):
        self.ground = patch(MODULE+'generate_grounded_brief',return_value=(self.brief,self.bundle)).start()
        self.addCleanup(patch.stopall)
        self.brand = patch(MODULE+'retrieve_brand_context',return_value=copy.deepcopy(self.bundle.context['brand_context'])).start()
        self.product = patch(MODULE+'retrieve_product_knowledge',return_value=copy.deepcopy(self.bundle.context['product_knowledge'])).start()

    def generate(self):
        return generate_preview(Mock(),Mock(),Mock(),self.request,{}, {},{},self.config)

    def test_pipeline_reads_fresh_evidence_rechecks_cloud_approvals_and_has_no_json_fallback(self):
        self.mocks()
        with patch('pathlib.Path.read_text',side_effect=AssertionError('No local grounding fallback')):
            result,brief,bundle = self.generate()
        self.ground.assert_called_once();self.brand.assert_called_once();self.product.assert_called_once()
        self.assertTrue(result.metadata['approval_recheck_passed'])
        self.assertEqual(result.metadata['source_system'],'SYNTHETIC_POC')
        self.assertIs(bundle,self.bundle);self.assertIs(brief,self.brief)

    def test_revoked_approval_or_changed_product_during_render_aborts(self):
        self.mocks()
        self.brand.return_value['approved_claims'] = []
        with self.assertRaisesRegex(ValueError,'changed during rendering'):self.generate()
        self.brand.return_value = self.bundle.context['brand_context']
        self.product.return_value['snapshot_id'] = 'changed'
        with self.assertRaisesRegex(ValueError,'changed during rendering'):self.generate()

    def test_cloud_failure_propagates_and_cannot_create_preview(self):
        self.mocks();self.ground.side_effect = RuntimeError('BigQuery unavailable')
        with patch(MODULE+'render_preview') as render:
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):self.generate()
        render.assert_not_called()

    def test_unique_review_export_retains_provenance_evidence_and_image_hash(self):
        self.mocks();result,brief,bundle = self.generate()
        with tempfile.TemporaryDirectory() as tmp:
            path = export_preview(result,brief,bundle,Path(tmp))
            second = export_preview(result,brief,bundle,Path(tmp))
            self.assertNotEqual(path,second)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),result.metadata['sha256'])
            metadata = json.loads((path.parent/'banner_preview.json').read_text())
            self.assertEqual(metadata['source_system'],'SYNTHETIC_POC')
            self.assertEqual(metadata['evidence_index'],brief['evidence_index'])
            self.assertTrue((path.parent/'campaign_brief.json').exists())
            self.assertEqual(len(list((path.parent/'evidence').rglob('evidence_packet.json'))),1)

    def test_export_rejects_tampered_png_metadata_and_missing_recheck_before_writing(self):
        self.mocks();result,brief,bundle = self.generate()
        with tempfile.TemporaryDirectory() as tmp:
            for field,value in (('sha256','bad'),('approval_recheck_passed',False),('source_system','REAL')):
                changed = copy.deepcopy(result);changed.metadata[field] = value
                with self.assertRaises(ValueError):export_preview(changed,brief,bundle,Path(tmp))
            self.assertEqual(list(Path(tmp).iterdir()),[])


if __name__ == '__main__':
    unittest.main()
