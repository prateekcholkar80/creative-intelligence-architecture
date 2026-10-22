from dataclasses import replace
from datetime import datetime,timezone,timedelta
import hashlib,io,json
from pathlib import Path
from unittest import TestCase
from PIL import Image,ImageDraw,ImageOps
from src.application.campaign_evaluator_store import canonical
from src.application.campaign_quality_checks import SourceFact
from src.application.campaign_quality_product_reader import ProductFactRead
from src.application.campaign_planner_runtime import identity_options
from src.generators.source_bound_campaign_renderer import ProductImageInput,render_identity_campaign


class SourceBoundRendererTests(TestCase):
    def fixture(self,layout='PRODUCT_RIGHT_COPY_LEFT',sku='CI-SKU-0022'):
        now=datetime.now(timezone.utc);fields=dict(product_name='Rejuvenate Restorative Face Oil')
        fact=SourceFact('native-product',sku,canonical(fields),now,now+timedelta(seconds=60),True)
        image=Image.new('RGB',(120,180),'white');draw=ImageDraw.Draw(image)
        for bounds,color in (((0,0,25,25),'red'),((95,0,119,25),'green'),((0,155,25,179),'blue'),((95,155,119,179),'black')):
            draw.rectangle(bounds,fill=color)
        buffer=io.BytesIO();image.save(buffer,format='PNG');raw=buffer.getvalue();digest=hashlib.sha256(raw).hexdigest()
        lineage=canonical(dict(sku=sku,binding_id='derived-source',content_sha256=digest,
            source_binding_id='original-source',original_content_sha256='a'*64,transform='AUTHORIZED_CROP'))
        bound=ProductImageInput(sku,'derived-source',digest,lineage,raw)
        options=identity_options(fields);draft={k:v[0] for k,v in json.loads(options).items()};draft['layout']=layout
        config=json.loads(Path('config/banner_preview_generator.json').read_text())
        return dict(product=ProductFactRead(fact,'native-product'),image=bound,planner_response=canonical(draft),
            options_json=options,checked_at=now,config=config)

    def test_all_layouts_keep_full_image_and_do_not_assume_legacy_sku(self):
        for layout in ('PRODUCT_RIGHT_COPY_LEFT','PRODUCT_LEFT_COPY_RIGHT','PRODUCT_CENTER_COPY_ABOVE'):
            args=self.fixture(layout);result=render_identity_campaign(**args);meta=json.loads(result.metadata_json)
            self.assertEqual(meta['sku'],'CI-SKU-0022');self.assertTrue(all(meta['structural_checks'].values()))
            self.assertEqual(result,render_identity_campaign(**args));self.assertFalse(meta['release_allowed'])
            with Image.open(io.BytesIO(result.png)) as banner,Image.open(io.BytesIO(args['image'].raw)) as source:
                x,y,w,h=meta['geometry']['product_bounds'];pw,ph=meta['geometry']['regions']['product'][2:]
                expected=ImageOps.contain(source,(pw-24,ph-24),method=Image.Resampling.LANCZOS)
                self.assertEqual(banner.crop((x,y,x+w,y+h)).tobytes(),expected.tobytes())
        self.assertEqual(json.loads(render_identity_campaign(**self.fixture(sku='CI-SKU-0023')).metadata_json)['sku'],'CI-SKU-0023')

    def test_changed_bytes_lineage_and_cross_sku_are_rejected(self):
        args=self.fixture();image=args['image']
        with self.assertRaises(ValueError):replace(image,raw=b'changed')
        with self.assertRaises(ValueError):replace(image,lineage_json=canonical(dict(sku=image.sku)))
        args['product']=replace(args['product'],fact=replace(args['product'].fact,sku='CI-SKU-0023'))
        with self.assertRaises(ValueError):render_identity_campaign(**args)

    def test_stale_product_claim_copy_or_changed_options_are_rejected(self):
        for fault in ('stale','copy','options','access'):
            args=self.fixture()
            if fault=='stale':args['checked_at']=args['product'].fact.valid_until
            elif fault=='access':args['product']=replace(args['product'],fact=replace(args['product'].fact,access_allowed=False))
            elif fault=='copy':
                draft=json.loads(args['planner_response']);draft['headline']='Clinically proven wrinkle reduction';args['planner_response']=canonical(draft)
            else:
                options=json.loads(args['options_json']);options['headline']=['Unapproved benefit'];args['options_json']=canonical(options)
            with self.subTest(fault=fault),self.assertRaises(ValueError):render_identity_campaign(**args)
