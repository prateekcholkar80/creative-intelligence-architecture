import copy
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import ExitStack
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

from PIL import Image

from src.retrieval.evidence_packet import assemble_packet, export_evidence_packet, retrieve_evidence_packet
from src.retrieval.request import RetrievalRequest

MODULE = 'src.retrieval.evidence_packet.'


class EvidencePacketTests(unittest.TestCase):
    def setUp(self):
        self.sku = 'CI-SKU-0001';self.snapshot = 'current-snapshot'
        self.request = RetrievalRequest(self.sku,'CONVERSION','Skincare shoppers','PAID_SOCIAL','NAKED_AND_THRIVING')
        self.campaign_config = dict(model_id='gemini-embedding-001',dimension=768,location='asia-south1',
                                   top_k=3,maximum_bytes_billed=128*1024*1024)
        self.sales_config = dict(start_date='2025-01-01',end_date='2025-12-31',channel='ECOMMERCE',
                                trend_window_days=30,maximum_bytes_billed=96*1024*1024)
        uri = f'gs://creative-brand-memory-project-661d501a-e709-4276-a26/products/{self.sku}/primary.jpg'
        identity = dict(sku=self.sku,brand='Honest Beauty',product_name='Source product',currency='eur',
                        primary_image_uri=uri,source_system='BEAUTYFEEDS')
        claim = dict(claim_id='claim-identity',sku=self.sku,claim_type='PRODUCT_NAME',claim_text='Source product',
                     approval_status='APPROVED',approval_scope='SYNTHETIC_POC_ONLY')
        self.product = dict(canonical_sku=self.sku,product=identity,snapshot_id=self.snapshot,
                            poc_identity_claims=[claim],product_evidence_reference='product-table#sku='+self.sku)
        self.brand = dict(canonical_sku=self.sku,product=identity,snapshot_id=self.snapshot,
            checked_at=datetime(2026,10,8,tzinfo=timezone.utc),approved_claims=[dict(claim,evidence_references=['approval#claim'])],
            policy_authority='PROJECT_POC_POLICY',claim_approval_scope='SYNTHETIC_POC_ONLY',
            poc_rules=[dict(rule_id='POC-EVIDENCE',evidence_references=['policy#rule'])])
        self.visual = dict(visual_reference='NAKED_AND_THRIVING',approval_scope='VISUAL_INSPIRATION_ONLY',
                           snapshot_id=self.snapshot,visual_chunks=[dict(evidence_references=['https://example.com/style'])])
        primary = self.image('JPEG',f'AST-{self.sku}-PRIMARY','BEAUTYFEEDS')
        primary[0].update(identity,gcs_uri=uri)
        self.primary = primary
        campaign_id = f'SYN-CMP-20250101-{self.sku}-01'
        asset = f'SYN-AST-{campaign_id}-template-v1'
        image = self.image('PNG',asset,'SYNTHETIC_POC')
        banner_uri = f'gs://bucket/campaigns/{campaign_id}/{image[0]["content_sha256"]}.png'
        image[0].update(campaign_id=campaign_id,rank=1,snapshot_id=self.snapshot,gcs_uri=banner_uri)
        self.images = [image]
        creative = dict(campaign_id=campaign_id,asset_id=asset,sku=self.sku,brand=identity['brand'],
            product_name=identity['product_name'],campaign_name='Synthetic reference',objective='AWARENESS',
            channel='PAID_SOCIAL',audience='Source audience',start_date=date(2025,2,5),end_date=date(2025,2,25),
            gcs_uri=banner_uri,banner_sha256=image[0]['content_sha256'],rank=1,evidence_references=['campaign-table#'+campaign_id])
        self.creatives = dict(request=self.request.to_dict(),snapshot_id=self.snapshot,
                              source_system='SYNTHETIC_POC',creatives=[creative])
        self.performance = dict(canonical_sku=self.sku,snapshot_id=self.snapshot,source_system='SYNTHETIC_POC',
            summaries=[dict(creative,currency='EUR',conversions=10,attributed_units=11,attributed_revenue=Decimal('220'))])
        self.sales = dict(canonical_sku=self.sku,product=identity,currency='EUR',source_system='SYNTHETIC_POC',
            totals=dict(day_count=365,net_sales=Decimal('12345.67')),monthly=[{} for _ in range(12)],
            campaign_periods=[dict(campaign_id=campaign_id,effective_start=creative['start_date'],
                effective_end=creative['end_date'],orders=50,units_sold=80,net_sales=Decimal('1000'))])

    def image(self, image_format, asset, source):
        buffer = io.BytesIO();Image.new('RGB',(30,40),'white').save(buffer,format=image_format)
        raw = buffer.getvalue()
        metadata = dict(asset_id=asset,sku=self.sku,mime_type='image/jpeg' if image_format=='JPEG' else 'image/png',
            width=30,height=40,content_sha256=hashlib.sha256(raw).hexdigest(),source_system=source,
            byte_count=len(raw),evidence_references=['image-table#'+asset],
            verification=dict(generation_match=True,sha256_match=True,format_match=True,dimensions_match=True))
        return metadata,raw

    def assemble(self, **changes):
        values = dict(request=self.request,product=self.product,brand=self.brand,visual=self.visual,
                      creatives=self.creatives,performance=self.performance,sales=self.sales,
                      product_image=self.primary,campaign_images=self.images)
        return assemble_packet(**{**values,**changes})

    def readers(self, final_brand=None, final_product=None):
        stack = ExitStack();self.addCleanup(stack.close)
        returns = dict(retrieve_visual_reference=self.visual,retrieve_campaign_creatives=self.creatives,
            retrieve_campaign_performance=self.performance,retrieve_sales_analytics=self.sales,
            retrieve_product_image=self.primary,retrieve_campaign_images=self.images)
        mocks = {name:stack.enter_context(patch(MODULE+name,return_value=value)) for name,value in returns.items()}
        mocks['retrieve_brand_context'] = stack.enter_context(patch(MODULE+'retrieve_brand_context',
            side_effect=[self.brand,final_brand or self.brand]))
        mocks['retrieve_product_knowledge'] = stack.enter_context(patch(MODULE+'retrieve_product_knowledge',
            side_effect=[self.product,final_product or self.product]))
        return mocks

    def test_live_readers_are_composed_without_reading_exports_and_selected_ids_are_bound(self):
        mocks = self.readers();client,gcs,embedder = Mock(),Mock(),Mock()
        with patch('pathlib.Path.read_text',side_effect=AssertionError('No serving file reads')):
            bundle = retrieve_evidence_packet(client,gcs,embedder,self.request,self.campaign_config,self.sales_config)
        mocks['retrieve_campaign_performance'].assert_called_once_with(client,self.sku,
            [self.creatives['creatives'][0]['campaign_id']])
        mocks['retrieve_campaign_images'].assert_called_once_with(client,gcs,self.creatives)
        self.assertEqual(mocks['retrieve_brand_context'].call_count,2)
        self.assertEqual(mocks['retrieve_product_knowledge'].call_count,2)
        self.assertTrue(bundle.context['approval_recheck_passed'])
        self.assertEqual(len(bundle.image_bytes),2)

    def test_authority_scopes_original_bytes_decimal_and_evidence_survive_assembly(self):
        bundle = self.assemble();context = bundle.context
        self.assertEqual(context['source_of_truth'],'BIGQUERY')
        self.assertEqual(context['sales_analytics']['totals']['net_sales'],Decimal('12345.67'))
        self.assertEqual(bundle.image_bytes[self.primary[0]['asset_id']],self.primary[1])
        self.assertFalse(context['generation_constraints']['benefit_claims_authorized'])
        self.assertEqual(context['generation_constraints']['visual_reference_scope'],'VISUAL_INSPIRATION_ONLY')
        self.assertEqual(context['generation_constraints']['allowed_identity_wording'],['Source product'])
        refs = {e['reference'] for e in context['evidence_index']}
        self.assertIn('approval#claim',refs);self.assertIn('https://example.com/style',refs)
        context['brand_context']['approved_claims'][0]['claim_text'] = 'changed copy'
        self.assertEqual(self.brand['approved_claims'][0]['claim_text'],'Source product')

    def test_cross_sku_identity_currency_or_mixed_snapshot_is_rejected(self):
        for key,value in (('canonical_sku','CI-SKU-0002'),('snapshot_id','old')):
            with self.assertRaises(ValueError):self.assemble(brand={**self.brand,key:value})
        brand = copy.deepcopy(self.brand);brand['product']['brand'] = 'Different brand'
        with self.assertRaises(ValueError):self.assemble(brand=brand)
        with self.assertRaises(ValueError):self.assemble(sales={**self.sales,'currency':'USD'})

    def test_approval_disagreement_and_pending_or_benefit_claims_are_rejected(self):
        with self.assertRaisesRegex(ValueError,'claims disagree'):
            self.assemble(brand={**self.brand,'approved_claims':[]})
        for field,value in (('claim_type','BENEFIT'),('approval_status','PENDING_REVIEW'),('claim_text','New benefit')):
            product,brand = copy.deepcopy(self.product),copy.deepcopy(self.brand)
            product['poc_identity_claims'][0][field] = value;brand['approved_claims'][0][field] = value
            with self.assertRaises(ValueError):self.assemble(product=product,brand=brand)

    def test_mid_retrieval_revocation_or_policy_change_aborts_entire_bundle(self):
        changed = copy.deepcopy(self.brand);changed['approved_claims'] = []
        self.readers(final_brand=changed)
        with self.assertRaisesRegex(ValueError,'changed during retrieval'):
            retrieve_evidence_packet(Mock(),Mock(),Mock(),self.request,self.campaign_config,self.sales_config)

    def test_mid_retrieval_product_change_aborts_entire_bundle(self):
        changed = copy.deepcopy(self.product);changed['product']['product_name'] = 'Changed name'
        self.readers(final_product=changed)
        with self.assertRaisesRegex(ValueError,'changed during retrieval'):
            retrieve_evidence_packet(Mock(),Mock(),Mock(),self.request,self.campaign_config,self.sales_config)

    def test_campaign_set_metadata_dates_and_sales_caps_are_checked(self):
        with self.assertRaises(ValueError):self.assemble(performance={**self.performance,'summaries':[]})
        with self.assertRaises(ValueError):self.assemble(campaign_images=[])
        for field,value in (('asset_id','other'),('channel','EMAIL'),('attributed_revenue',Decimal('1001'))):
            performance = copy.deepcopy(self.performance);performance['summaries'][0][field] = value
            with self.assertRaises(ValueError):self.assemble(performance=performance)
        sales = copy.deepcopy(self.sales);sales['campaign_periods'][0]['effective_start'] = date(2025,2,6)
        with self.assertRaises(ValueError):self.assemble(sales=sales)

    def test_missing_or_corrupt_image_bytes_and_failed_verification_cannot_be_exported(self):
        with self.assertRaises(ValueError):self.assemble(product_image=(self.primary[0],b'corrupt'))
        bad = copy.deepcopy(self.primary[0]);bad['verification']['generation_match'] = False
        with self.assertRaises(ValueError):self.assemble(product_image=(bad,self.primary[1]))
        images = copy.deepcopy(self.images);images[0][0]['rank'] = 2
        with self.assertRaises(ValueError):self.assemble(campaign_images=images)

    def test_none_styling_and_empty_campaign_selection_do_not_expand_or_invent_history(self):
        request = RetrievalRequest(self.sku,'CONVERSION','Skincare shoppers','PAID_SOCIAL','NONE')
        creatives = dict(self.creatives,request=request.to_dict(),creatives=[],snapshot_id=None)
        bundle = self.assemble(request=request,visual=dict(visual_reference='NONE',approval_scope=None,visual_chunks=[]),
            creatives=creatives,performance=dict(self.performance,summaries=[],snapshot_id=None),campaign_images=[])
        self.assertEqual(len(bundle.image_bytes),1)
        self.assertIsNone(bundle.context['generation_constraints']['visual_reference_scope'])
        with self.assertRaises(ValueError):self.assemble(visual={**self.visual,'approval_scope':'MANUFACTURER_POLICY'})

    def test_cloud_failure_propagates_and_never_assembles_or_exports_partial_context(self):
        mocks = self.readers();mocks['retrieve_sales_analytics'].side_effect = RuntimeError('BigQuery unavailable')
        with patch(MODULE+'assemble_packet') as assemble,patch(MODULE+'export_evidence_packet') as export:
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_evidence_packet(Mock(),Mock(),Mock(),self.request,self.campaign_config,self.sales_config)
        assemble.assert_not_called();export.assert_not_called()
        mocks['retrieve_product_image'].assert_not_called()

    def test_unique_review_export_keeps_exact_bytes_and_serializes_decimal_without_serving_reads(self):
        bundle = self.assemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = export_evidence_packet(bundle,Path(tmp));second = export_evidence_packet(bundle,Path(tmp))
            self.assertNotEqual(path,second)
            exported = json.loads(path.read_text())
            self.assertEqual(exported['sales_analytics']['totals']['net_sales'],'12345.67')
            for image in exported['image_manifest']:
                self.assertEqual(Path(image['local_review_path']).read_bytes(),bundle.image_bytes[image['asset_id']])
            self.assertNotIn('local_review_path',bundle.context['image_manifest'][0])


if __name__ == '__main__':
    unittest.main()
