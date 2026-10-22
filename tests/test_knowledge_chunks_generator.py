import hashlib
import json
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from src.generators.knowledge_chunks_generator import ROOT, SCHEMA, build_chunks, split_text
from src.generators.brand_memory_generator import build_memory


class KnowledgeTests(unittest.TestCase):
    def setUp(self):
        self.config=json.loads((ROOT/'config/knowledge_chunks.json').read_text())
        self.profile=json.loads((ROOT/self.config['visual_reference']).read_text())
        self.products=[dict(sku=f'CI-SKU-{i:04d}',brand='Source brand',product_name=f'Product {i}',
            product_type='Serum',price=Decimal('35'),currency='EUR',source_system='BEAUTYFEEDS',
            primary_image_uri=f'gs://test/products/CI-SKU-{i:04d}/primary.jpg') for i in range(1,21)]
        self.ingredients=[dict(sku=p['sku'],ingredient_sequence=1,ingredient_id='ING-1',
                               inci_name='Aqua',source_system='BEAUTYFEEDS') for p in self.products]
        brand_config=json.loads((ROOT/'config/brand_memory.json').read_text())
        self.rules,self.claims=build_memory(self.products,self.ingredients,brand_config)
        self.approved={c['claim_id'] for c in self.claims if c['approval_status']=='APPROVED'}
        self.campaigns=[]
        for p in self.products:
            for i in range(3):
                self.campaigns.append(dict(campaign_id=f"CMP-{p['sku']}-{i}",sku=p['sku'],
                    source_system='SYNTHETIC_POC',performance_days=21,campaign_name='Synthetic scenario',
                    start_date=date(2025,1,1),end_date=date(2025,1,21),channel='PAID_SOCIAL',
                    audience='Test audience',objective='AWARENESS',headline=p['product_name'],body_copy='View details',
                    call_to_action='View product',visual_description='Synthetic template',asset_id=f"AST-{p['sku']}-{i}",
                    gcs_uri='gs://test/campaign.png',product_image_uri=p['primary_image_uri'],impressions=1000,
                    clicks=20,conversions=2,spend=Decimal('12'),revenue=Decimal('70'),currency='EUR',
                    ctr=Decimal('0.02'),conversion_rate=Decimal('0.1'),roas=Decimal('5.833333333')))

    def build(self):
        return build_chunks(self.products,self.ingredients,self.rules,self.claims,self.approved,
                            self.campaigns,self.profile,self.config)

    def test_determinism_order_and_hashes(self):
        rows=self.build()
        reversed_rows=build_chunks(self.products[::-1],self.ingredients[::-1],self.rules[::-1],
            self.claims[::-1],self.approved,self.campaigns[::-1],self.profile,self.config)
        self.assertEqual(rows,reversed_rows)
        self.assertEqual(len({r['snapshot_id'] for r in rows}),1)
        self.assertEqual(len({r['chunk_id'] for r in rows}),len(rows))
        for r in rows:
            self.assertEqual(r['content_sha256'],hashlib.sha256(r['chunk_text'].encode()).hexdigest())
            self.assertEqual(set(r),{f.name for f in SCHEMA})
            self.assertLessEqual(len(r['chunk_text']),self.config['max_chars'])
            self.assertTrue(r['evidence_references'])

    def test_review_boundaries_and_real_provenance(self):
        rows=self.build()
        for r in rows:
            if r['chunk_type']=='CLAIM_REVIEW_NOTE':self.assertEqual(r['retrieval_status'],'REVIEW_ONLY')
            if r['chunk_type']=='POC_APPROVED_CLAIM':self.assertIn(r['claim_id'],self.approved)
            if r['chunk_type'] in ('PRODUCT_KNOWLEDGE','INGREDIENT_KNOWLEDGE'):
                self.assertEqual(r['source_system'],'BEAUTYFEEDS')
            if r['chunk_type'].startswith('WEBSITE_'):
                self.assertEqual(r['brand'],'Naked & Thriving')
                self.assertIsNone(r['sku'])
                self.assertEqual(r['retrieval_status'],'VISUAL_REFERENCE')
                self.assertEqual(r['approval_scope'],'VISUAL_INSPIRATION_ONLY')

    def test_campaign_aggregation_and_images(self):
        rows=self.build()
        learns=[r for r in rows if r['chunk_type']=='CAMPAIGN_LEARNING']
        self.assertEqual(len(learns),60)
        self.assertFalse(any(r['chunk_type']=='DAILY_SALES' for r in rows))
        for r in learns:
            self.assertEqual(r['source_system'],'SYNTHETIC_POC')
            self.assertIn('not causal evidence',r['chunk_text'])
            self.assertTrue(r['image_uris'])
        for r in rows:
            if r['chunk_type']=='CAMPAIGN_CREATIVE':self.assertEqual(len(r['image_uris']),2)

    def test_chunk_splitting_preserves_words(self):
        text=' '.join(f'ingredient{i}' for i in range(100))
        parts=split_text(text,200)
        self.assertEqual(' '.join(parts),text)
        self.assertTrue(all(len(p)<=200 for p in parts))
        with self.assertRaises(ValueError):split_text(text,20)

    def test_invalid_scope_and_false_approval(self):
        self.profile['canonical_sku_mapping']=['CI-SKU-0001']
        with self.assertRaises(ValueError):self.build()
        self.profile['canonical_sku_mapping']=[]
        pending=next(c for c in self.claims if c['approval_status']=='PENDING_REVIEW')
        self.approved.add(pending['claim_id'])
        with self.assertRaises(ValueError):self.build()

    def test_incomplete_campaign_and_missing_product(self):
        self.campaigns[0]['performance_days']=20
        with self.assertRaises(ValueError):self.build()
        self.campaigns[0]['performance_days']=21
        self.products.pop()
        with self.assertRaises(ValueError):self.build()

    def test_changed_scope_evidence_updates_snapshot(self):
        original=self.build()[0]['snapshot_id']
        self.profile['creative_patterns'][0]['evidence_urls'].append('https://example.com/evidence')
        self.assertNotEqual(original,self.build()[0]['snapshot_id'])

    def test_visual_reference_excludes_site_claims_and_tone(self):
        visual=[r for r in self.build() if r['chunk_type']=='WEBSITE_VISUAL_REFERENCE']
        self.assertEqual(len(visual),4)
        sections={r['source_reference'].rsplit('#',1)[1] for r in visual}
        self.assertEqual(sections,{'typography','colors','layout','creative_patterns'})
        for row in visual:
            self.assertIsNone(row['claim_id'])
            self.assertIsNone(row['sku'])
            self.assertNotIn('100% showed an increase in hydration',row['chunk_text'])
            self.assertNotIn('Renew Resurfacing Night Serum',row['chunk_text'])
            self.assertIn('User-selected styling inspiration only',row['chunk_text'])

    def test_full_research_and_nonvisual_fields_rejected(self):
        self.profile['published_claim_candidates']=[]
        with self.assertRaises(ValueError):self.build()
        del self.profile['published_claim_candidates']
        self.profile['typography'][0]['claim_text']='An unsupported product benefit'
        with self.assertRaises(ValueError):self.build()


if __name__=='__main__':unittest.main()
