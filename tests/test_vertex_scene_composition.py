from dataclasses import replace
import hashlib,io,json
from unittest import TestCase
from PIL import Image
from src.application.campaign_artifact_measurement import measure_banner
from src.generators.source_bound_campaign_renderer import GeneratedSceneInput,render_identity_campaign
from tests.test_source_bound_campaign_renderer import SourceBoundRendererTests


class VertexSceneCompositionTests(TestCase):
    def fixture(self):
        args=SourceBoundRendererTests().fixture();buffer=io.BytesIO()
        image=Image.new('RGB',(768,432),'#dcc9ad');image.putpixel((0,0),(1,2,3));image.save(buffer,format='PNG')
        raw=buffer.getvalue();scene=GeneratedSceneInput(args['image'].sku,args['image'].binding_id,'a'*64,'b'*64,
            hashlib.sha256(raw).hexdigest(),raw,'gemini-3.1-flash-image')
        return args,scene

    def test_scene_is_used_and_exact_source_product_pixels_survive_all_layouts(self):
        args,scene=self.fixture()
        for layout in json.loads(args['options_json'])['layout']:
            response=json.loads(args['planner_response']);response['layout']=layout
            from src.application.campaign_evaluator_store import canonical
            args['planner_response']=canonical(response)
            result=render_identity_campaign(**args,scene=scene);meta=json.loads(result.metadata_json)
            score=measure_banner(result.png,meta,image=args['image'],planner_response=args['planner_response'],
                options_json=args['options_json'],config=args['config'],scene=scene)
            self.assertEqual(score['status'],'PASS');self.assertEqual(score['measurements']['product_region_mse'],0)
            self.assertEqual(score['measurements']['generated_scene_region_mse'],0)
            self.assertEqual(meta['method'],'VERTEX_GENERATED_SCENE_WITH_PRESERVED_SOURCE_PRODUCT')
            self.assertFalse(meta['release_allowed'])

    def test_wrong_sku_binding_or_bytes_are_denied(self):
        args,scene=self.fixture()
        for change in (dict(sku='CI-SKU-0023'),dict(source_image_binding_id='different'),dict(raw=b'changed')):
            with self.subTest(change=change),self.assertRaises(ValueError):render_identity_campaign(**args,scene=replace(scene,**change))

    def test_background_tamper_is_measured_as_failure_and_missing_lineage_denies(self):
        args,scene=self.fixture();result=render_identity_campaign(**args,scene=scene);meta=json.loads(result.metadata_json)
        with Image.open(io.BytesIO(result.png)) as banner:
            banner.putpixel((0,0),(255,0,0));buffer=io.BytesIO();banner.save(buffer,format='PNG')
        raw=buffer.getvalue();meta['content_sha256']=hashlib.sha256(raw).hexdigest()
        score=measure_banner(raw,meta,image=args['image'],planner_response=args['planner_response'],
            options_json=args['options_json'],config=args['config'],scene=scene)
        self.assertEqual(score['status'],'FAIL');self.assertGreater(score['measurements']['generated_scene_region_mse'],0)
        with self.assertRaisesRegex(ValueError,'ARTIFACT_MEASUREMENT_SCENE_REQUIRED'):
            measure_banner(raw,meta,image=args['image'],planner_response=args['planner_response'],options_json=args['options_json'],config=args['config'])
