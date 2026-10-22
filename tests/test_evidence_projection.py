"""Fresh product-reader projection checks; no cloud calls or persistence."""
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import io
import unittest

from PIL import Image

from src.application.evidence_contract import validate_evidence_snapshot
from src.application.evidence_projection import (
    project_brand_context, project_campaign_creatives, project_campaign_performance, project_product_knowledge,
    project_campaign_images, project_generated_images, project_product_image, project_sales_analytics,
    project_visual_reference,
)
from src.application.workflow_contract import ContractError
from src.generators.sales_generator import LOCATION, PROJECT
from src.retrieval.brand_context import BrandContextRead, build_packet as build_brand_packet
from src.retrieval.campaign_creatives import CampaignCreativeRead, build_packet as build_creative_packet
from src.retrieval.campaign_performance import (
    ATTRIBUTION_MODEL, CampaignPerformanceRead, build_packet as build_performance_packet,
)
from src.retrieval.campaign_images import CampaignImagesRead
from src.retrieval.generated_banner import GeneratedBannerRead
from src.retrieval.generated_banner_search import GeneratedBannerSearchRead
from src.retrieval.product_image import ProductImageRead
from src.retrieval.product_knowledge import ProductKnowledgeRead, build_packet
from src.retrieval.request import RetrievalRequest
from src.retrieval.sales_analytics import SalesAnalyticsRead, build_packet as build_sales_packet
from src.retrieval.visual_reference import VisualReferenceRead, build_packet as build_visual_packet
from tests.test_execution_contract import execution
from tests.test_workflow_contract import CONVERSATION, TIME, digest


def captured_read() -> ProductKnowledgeRead:
    sku = 'CI-SKU-0001'
    image = f'gs://creative-brand-memory-{PROJECT}/products/{sku}/primary.jpg'
    product = dict(sku=sku,brand='Fixture brand',product_name='Fixture product',product_type='SERUM',
        price=Decimal('25.000000000'),currency='USD',primary_image_uri=image,source_system='BEAUTYFEEDS')
    claim = dict(claim_id='CLM-CI-SKU-0001',sku=sku,claim_type='PRODUCT_NAME',claim_text='Fixture product',
        evidence_reference=f'{PROJECT}.ci_product.product_master#sku={sku}&field=product_name',
        evidence_excerpt='Fixture product',evidence_source_system='BEAUTYFEEDS',approval_status='APPROVED',
        approval_scope='SYNTHETIC_POC_ONLY',approved_by='SYNTHETIC_POC_REVIEWER',
        approval_reference='config/brand_memory.json#review_fixture',approved_at=TIME,
        review_notes='POC identity only',source_system='SYNTHETIC_POC')
    chunks = []
    for kind, table, text in (
            ('PRODUCT_KNOWLEDGE','product_master','Fixture product identity.'),
            ('INGREDIENT_KNOWLEDGE','product_ingredients','Recorded ingredient wording.')):
        reference = f'{PROJECT}.ci_product.{table}#sku={sku}'
        chunks.append(dict(snapshot_id=digest('snapshot'),chunk_id=f'KCH-{kind}',chunk_type=kind,
            brand='Fixture brand',sku=sku,chunk_text=text,source_system='BEAUTYFEEDS',
            source_reference=reference,evidence_references=[reference],image_uris=[image],
            retrieval_status='FACTUAL_CONTEXT',approval_scope='SOURCE_PRODUCT_FACTS',claim_id=None,
            content_sha256=hashlib.sha256(text.encode()).hexdigest()))
    text = 'Approved identity wording: Fixture product'
    chunks.append(dict(snapshot_id=digest('snapshot'),chunk_id='KCH-IDENTITY',chunk_type='POC_APPROVED_CLAIM',
        brand='Fixture brand',sku=sku,chunk_text=text,source_system='SYNTHETIC_POC',
        source_reference=f'{PROJECT}.ci_brand_memory.product_claims#claim_id={claim["claim_id"]}',
        evidence_references=[claim['evidence_reference']],image_uris=[],retrieval_status='POC_APPROVED',
        approval_scope='SYNTHETIC_POC_ONLY',claim_id=claim['claim_id'],
        content_sha256=hashlib.sha256(text.encode()).hexdigest()))
    packet = build_packet(sku,dict(products=[product],knowledge_chunks=chunks,poc_identity_claims=[claim]))
    return ProductKnowledgeRead(packet=packet,query_job_id='bq-product-read-1',
        observed_at=TIME+timedelta(seconds=2),project=PROJECT,location=LOCATION)


def captured_brand_read() -> BrandContextRead:
    sku = 'CI-SKU-0001'; checked = TIME+timedelta(seconds=2)
    product = dict(sku=sku,brand='Fixture brand',product_name='Fixture product',source_system='BEAUTYFEEDS')
    rule = dict(rule_id='POC-RULE-GROUNDING',brand='*',scope='PROJECT_POC',category='GROUNDING',
        rule_text='Use approved identity wording only.',rule_status='ACTIVE_FOR_POC',
        authority='PROJECT_POC_POLICY',source_system='SYNTHETIC_POC',
        source_reference='config/brand_memory.json#POC-RULE-GROUNDING')
    text = ('Type: POC_RULE. Brand: *. Status: POC_POLICY. Scope: SYNTHETIC_POC_ONLY.\n'
            'SYNTHETIC_POC: not actual historical business evidence.\nUse approved identity wording only.')
    chunk = dict(snapshot_id=digest('snapshot'),chunk_id='KCH-POLICY',chunk_type='POC_RULE',brand='*',
        sku=None,campaign_id=None,asset_id=None,claim_id=None,chunk_text=text,source_system='SYNTHETIC_POC',
        source_reference=f'{PROJECT}.ci_brand_memory.brand_guidelines#rule_id={rule["rule_id"]}',
        evidence_references=[rule['source_reference']],image_uris=[],retrieval_status='POC_POLICY',
        approval_scope='SYNTHETIC_POC_ONLY',content_sha256=hashlib.sha256(text.encode()).hexdigest())
    claim = dict(claim_id='CLM-CI-SKU-0001',sku=sku,claim_type='PRODUCT_NAME',claim_text='Fixture product',
        evidence_reference=f'{PROJECT}.ci_product.product_master#sku={sku}&field=product_name',
        evidence_excerpt='Fixture product',evidence_source_system='BEAUTYFEEDS',approval_status='APPROVED',
        approval_scope='SYNTHETIC_POC_ONLY',approved_by='SYNTHETIC_POC_REVIEWER',
        approval_reference='config/brand_memory.json#review_fixture',approved_at=TIME,
        review_notes='POC identity only',source_system='SYNTHETIC_POC')
    packet = build_brand_packet(sku,dict(checked_at=checked,products=[product],poc_rules=[rule],
        policy_chunks=[chunk],approved_claims=[claim]))
    return BrandContextRead(packet=packet,query_job_id='bq-brand-read-1',observed_at=checked,
        project=PROJECT,location=LOCATION)


def captured_sales_read() -> SalesAnalyticsRead:
    sku='CI-SKU-0001';start=date(2026,1,1);end=date(2026,1,4)
    def totals(first,last,units):
        days=(last-first).days+1
        return dict(row_count=days,day_count=days,first_date=first,last_date=last,
            units_sold=units,orders=units,gross_sales=Decimal(units),discount_amount=Decimal(0),
            refund_amount=Decimal(0),net_sales=Decimal(units))
    previous=totals(start,date(2026,1,2),2)|{'window_name':'PREVIOUS'}
    recent=totals(date(2026,1,3),end,4)|{'window_name':'RECENT'}
    full=totals(start,end,6)|{'currency_count':1,'invalid_rows':0}
    config=dict(start_date=start.isoformat(),end_date=end.isoformat(),trend_window_days=2,
        channel='ECOMMERCE',maximum_bytes_billed=1)
    result=dict(products=[dict(sku=sku,brand='Fixture brand',product_name='Fixture product',
        currency='USD',source_system='BEAUTYFEEDS')],totals=full,
        monthly=[totals(start,end,6)|{'month_start':start}],windows=[previous,recent],
        trends=dict(units_change=2,orders_change=2,net_sales_change=Decimal(2),
            units_change_fraction=Decimal(1),orders_change_fraction=Decimal(1),
            net_sales_change_fraction=Decimal(1)),campaign_periods=[],campaign_count=0,
        overlapping_campaign_days=0)
    packet=build_sales_packet(sku,result,config)
    return SalesAnalyticsRead(packet=packet,query_job_id='bq-sales-read-1',
        observed_at=TIME+timedelta(seconds=2),project=PROJECT,location=LOCATION)


def captured_performance_read() -> CampaignPerformanceRead:
    sku='CI-SKU-0001';campaign=f'SYN-CMP-20260101-{sku}-01'
    summary=dict(campaign_id=campaign,asset_id=f'SYN-AST-{campaign}-template-v1',sku=sku,
        campaign_name='Synthetic fixture',objective='AWARENESS',channel='PAID_SOCIAL',audience='Audience',
        start_date=date(2026,1,1),end_date=date(2026,1,2),budget=Decimal(20),currency='USD',
        snapshot_id=digest('snapshot'),expected_days=2,performance_rows=2,performance_days=2,
        first_performance_date=date(2026,1,1),last_performance_date=date(2026,1,2),
        impressions=100,clicks=10,conversions=2,attributed_units=2,spend=Decimal(20),
        attributed_revenue=Decimal(40),ctr=Decimal('0.100000000'),
        conversion_rate=Decimal('0.200000000'),roas=Decimal('2.000000000'),invalid_rows=0,
        source_system='SYNTHETIC_POC',attribution_model=ATTRIBUTION_MODEL)
    packet=build_performance_packet(sku,[summary],[campaign])
    return CampaignPerformanceRead(packet=packet,query_job_id='bq-performance-read-1',
        observed_at=TIME+timedelta(seconds=2),project=PROJECT,location=LOCATION)


def captured_creative_read() -> CampaignCreativeRead:
    sku='CI-SKU-0001';campaign=f'SYN-CMP-20260101-{sku}-01'
    asset=f'SYN-AST-{campaign}-template-v1';snapshot=digest('snapshot')
    campaign_ref=f'{PROJECT}.ci_creative.campaign_master#campaign_id={campaign}'
    asset_ref=f'{PROJECT}.ci_creative.campaign_assets#asset_id={asset}'
    product_image=f'gs://creative-brand-memory-{PROJECT}/products/{sku}/primary.jpg'
    banner_hash=hashlib.sha256(b'campaign-banner').hexdigest()
    banner=f'gs://creative-brand-memory-{PROJECT}/campaigns/{campaign}/template-v1/{banner_hash}.png'
    text='SYNTHETIC_POC fixture campaign creative.'
    creative=dict(sku=sku,brand='Fixture brand',product_name='Fixture product',snapshot_id=snapshot,
        chunk_id='KCH-CAMPAIGN-1',asset_id=asset,campaign_id=campaign,chunk_type='CAMPAIGN_CREATIVE',
        chunk_text=text,content_sha256=hashlib.sha256(text.encode()).hexdigest(),
        source_system='SYNTHETIC_POC',retrieval_status='POC_CONTEXT',
        approval_scope='SYNTHETIC_POC_ONLY',claim_id=None,source_reference=campaign_ref,
        evidence_references=[campaign_ref,asset_ref],source_image_uri=product_image,gcs_uri=banner,
        banner_sha256=banner_hash,image_uris=[product_image,banner],model_id='gemini-embedding-001',
        dimension=768,model_location=LOCATION,task_type='RETRIEVAL_DOCUMENT',objective='CONVERSION',
        channel='PAID_SOCIAL',audience='Skincare shoppers',distance=0.2)
    request=RetrievalRequest(sku,'CONVERSION','Skincare shoppers','PAID_SOCIAL','NONE')
    config=dict(model_id='gemini-embedding-001',dimension=768,location=LOCATION,top_k=3,
        maximum_bytes_billed=1)
    return CampaignCreativeRead(build_creative_packet(request,[creative],config),'bq-creative-read-1',
        TIME+timedelta(seconds=2),PROJECT,LOCATION)


def captured_visual_read() -> VisualReferenceRead:
    chunks=[]
    for section in ('typography','colors','layout','creative_patterns'):
        text=f'Observed Naked & Thriving {section}; styling inspiration only.'
        reference=f'data/research/naked_and_thriving_visual_reference.json#{section}'
        chunks.append(dict(snapshot_id=digest('snapshot'),chunk_id=f'KCH-VISUAL-{section}',
            chunk_type='WEBSITE_VISUAL_REFERENCE',brand='Naked & Thriving',sku=None,campaign_id=None,
            asset_id=None,claim_id=None,chunk_text=text,source_system='OFFICIAL_BRAND_WEBSITE',
            source_reference=reference,evidence_references=[reference],image_uris=[],
            retrieval_status='VISUAL_REFERENCE',approval_scope='VISUAL_INSPIRATION_ONLY',
            content_sha256=hashlib.sha256(text.encode()).hexdigest()))
    return VisualReferenceRead(build_visual_packet('NAKED_AND_THRIVING',chunks),'bq-visual-read-1',
        TIME+timedelta(seconds=2),PROJECT,LOCATION)


def image_bytes(kind: str) -> bytes:
    output=io.BytesIO();Image.new('RGB',(1200,628),'white').save(output,format=kind)
    return output.getvalue()


def captured_image_reads():
    sku='CI-SKU-0001';product_raw=image_bytes('JPEG');product_sha=hashlib.sha256(product_raw).hexdigest()
    product_meta=dict(canonical_sku=sku,asset_id=f'AST-{sku}-PRIMARY',gcs_uri=
        f'gs://creative-brand-memory-{PROJECT}/products/{sku}/primary.jpg',gcs_generation='101',
        content_sha256=product_sha,mime_type='image/jpeg',width=1200,height=628,
        asset_description='Verified product image.',embedding_id='EMB-product',model_id='multimodalembedding@001',
        model_location=LOCATION,dimension=512,verification=dict(generation_match=True,sha256_match=True,
            format_match=True,dimensions_match=True))
    product=ProductImageRead(product_meta,product_raw,'bq-product-image',TIME+timedelta(seconds=2),PROJECT,LOCATION)

    campaign=f'SYN-CMP-20260101-{sku}-01';campaign_raw=image_bytes('PNG')
    campaign_sha=hashlib.sha256(campaign_raw).hexdigest();campaign_asset=f'SYN-AST-{campaign}-template-v1'
    campaign_meta=dict(canonical_sku=sku,asset_id=campaign_asset,campaign_id=campaign,
        gcs_uri=f'gs://creative-brand-memory-{PROJECT}/campaigns/{campaign}/template-v1/{campaign_sha}.png',
        gcs_generation='102',content_sha256=campaign_sha,mime_type='image/png',width=1200,height=628,
        visual_description='Verified historical campaign.',embedding_id='EMB-campaign',
        model_id='multimodalembedding@001',model_location=LOCATION,dimension=512,
        snapshot_id=digest('snapshot'),rank=1,distance=0.2,
        verification=dict(generation_match=True,mime_match=True,sha256_match=True,format_match=True,
            dimensions_match=True))
    campaigns=CampaignImagesRead([(campaign_meta,campaign_raw)],'bq-campaign-images',
        TIME+timedelta(seconds=2),PROJECT,LOCATION)

    generated_raw=image_bytes('PNG');generated_sha=hashlib.sha256(generated_raw).hexdigest()
    generated_asset=f'SYN-GEN-{sku}-{generated_sha}'
    generated_meta=dict(canonical_sku=sku,asset_id=generated_asset,
        gcs_uri=f'gs://creative-brand-memory-{PROJECT}/generated_campaigns/{sku}/grounded-banner-preview-v1/{generated_sha}.png',
        gcs_generation='103',content_sha256=generated_sha,mime_type='image/png',width=1200,height=628,
        visual_description='Approved generated visual.',snapshot_id=digest('snapshot'),rank=1,distance=0.1,
        review=dict(review_id='REV-approved',revision=2,status='APPROVED_FOR_POC',
            approval_scope='POC_VISUAL_REUSE_ONLY'),usage_scope='NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE',
        original_product_image=dict(asset_id=f'AST-{sku}-PRIMARY',gcs_uri=product_meta['gcs_uri'],
            gcs_generation='101',content_sha256=product_sha),embedding=dict(embedding_id='EMB-generated',
            model_id='multimodalembedding@001',model_location=LOCATION,dimension=512,task_type='IMAGE'),
        verification=dict(current_eligibility=True,generation_match=True,sha256_match=True,format_match=True,
            dimensions_match=True,vector_identity_match=True),evidence_references=[])
    exact_packet={key:value for key,value in generated_meta.items() if key not in ('rank','distance')}
    exact=GeneratedBannerRead(exact_packet,generated_raw,('bq-gen-before','bq-gen-after'),
        TIME+timedelta(seconds=2),PROJECT,LOCATION)
    search_packet=dict(canonical_sku=sku,source_of_truth='BIGQUERY',results=[generated_meta])
    generated=GeneratedBannerSearchRead(search_packet,[(generated_meta,generated_raw)],
        ('bq-rank-before','bq-rank-after'),(exact,),TIME+timedelta(seconds=2),PROJECT,LOCATION)
    return product,campaigns,generated


class EvidenceProjectionTests(unittest.TestCase):
    def setUp(self):
        self.attempt, self.request, self.job = execution()
        self.read = captured_read()
        self.rows = project_product_knowledge(self.read,evidence_snapshot_id='evidence-snapshot-1',
            request=self.request,job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=TIME+timedelta(seconds=3))

    def test_projects_exact_product_claim_and_chunk_rows(self):
        self.assertEqual([row['source_kind'] for row in self.rows],
            ['PRODUCT','POC_CLAIM','KNOWLEDGE_CHUNK','KNOWLEDGE_CHUNK','KNOWLEDGE_CHUNK'])
        self.assertTrue(all(row['source_revision']=='bq-product-read-1' for row in self.rows))
        self.assertTrue(all(row['evidence_snapshot_id']=='evidence-snapshot-1' for row in self.rows))
        self.assertEqual(self.rows[0]['evidence_payload']['record']['price'],'25.000000000')
        self.assertIsInstance(self.rows[1]['evidence_payload']['record']['approved_at'],str)
        for row in self.rows:
            validate_evidence_snapshot(row,request=self.request,job=self.job,attempt=self.attempt,
                conversation=CONVERSATION)

    def test_source_packet_is_not_mutated(self):
        before = deepcopy(self.read.packet)
        project_product_knowledge(self.read,evidence_snapshot_id='evidence-snapshot-1',
            request=self.request,job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=TIME+timedelta(seconds=3))
        self.assertEqual(self.read.packet,before)

    def test_wrong_scope_project_or_packet_shape_fails_closed(self):
        for read in (
            ProductKnowledgeRead(self.read.packet,self.read.query_job_id,self.read.observed_at,'other',LOCATION),
            ProductKnowledgeRead(self.read.packet|{'local_fallback':'file.json'},self.read.query_job_id,
                self.read.observed_at,PROJECT,LOCATION),
        ):
            with self.subTest(read=read), self.assertRaises(ContractError):
                project_product_knowledge(read,evidence_snapshot_id='evidence-snapshot-1',request=self.request,
                    job=self.job,attempt=self.attempt,conversation=CONVERSATION,
                    created_at=TIME+timedelta(seconds=3))

    def test_tampered_hash_stale_claim_and_cross_sku_fail(self):
        mutations = (
            lambda packet: packet['factual_chunks'][0].update(content_sha256='0'*64),
            lambda packet: packet['poc_identity_claims'][0].update(approval_status='REJECTED'),
            lambda packet: packet['product'].update(sku='CI-SKU-0002'),
        )
        for mutate in mutations:
            packet = deepcopy(self.read.packet); mutate(packet)
            read = ProductKnowledgeRead(packet,self.read.query_job_id,self.read.observed_at,PROJECT,LOCATION)
            with self.subTest(mutate=mutate), self.assertRaises(ContractError):
                project_product_knowledge(read,evidence_snapshot_id='evidence-snapshot-1',request=self.request,
                    job=self.job,attempt=self.attempt,conversation=CONVERSATION,
                    created_at=TIME+timedelta(seconds=3))

    def test_read_observation_must_fit_attempt_window(self):
        read = ProductKnowledgeRead(self.read.packet,self.read.query_job_id,TIME,PROJECT,LOCATION)
        with self.assertRaises(ContractError):
            project_product_knowledge(read,evidence_snapshot_id='evidence-snapshot-1',request=self.request,
                job=self.job,attempt=self.attempt,conversation=CONVERSATION,
                created_at=TIME+timedelta(seconds=3))


class BrandEvidenceProjectionTests(unittest.TestCase):
    def setUp(self):
        self.attempt,self.request,self.job = execution()
        self.read = captured_brand_read()
        self.rows = project_brand_context(self.read,evidence_snapshot_id='evidence-snapshot-1',
            request=self.request,job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=TIME+timedelta(seconds=3))

    def project(self,read):
        return project_brand_context(read,evidence_snapshot_id='evidence-snapshot-1',request=self.request,
            job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=TIME+timedelta(seconds=3))

    def test_projects_active_rules_current_claims_and_policy_chunks(self):
        self.assertEqual([row['source_kind'] for row in self.rows],
            ['POC_RULE','POC_CLAIM','KNOWLEDGE_CHUNK'])
        self.assertTrue(all(row['source_revision']=='bq-brand-read-1' for row in self.rows))
        self.assertEqual(self.rows[0]['evidence_payload']['record']['rule_status'],'ACTIVE_FOR_POC')
        self.assertEqual(self.rows[1]['evidence_payload']['record']['approval_status'],'APPROVED')
        self.assertEqual(self.rows[2]['knowledge_snapshot_id'],digest('snapshot'))
        for row in self.rows:
            validate_evidence_snapshot(row,request=self.request,job=self.job,attempt=self.attempt,
                conversation=CONVERSATION)

    def test_policy_source_packet_is_not_mutated(self):
        before = deepcopy(self.read.packet);self.project(self.read)
        self.assertEqual(self.read.packet,before)

    def test_wrong_project_extra_fields_or_changed_observation_fail(self):
        candidates = (
            BrandContextRead(self.read.packet,self.read.query_job_id,self.read.observed_at,'other',LOCATION),
            BrandContextRead(self.read.packet|{'local_fallback':'file.json'},self.read.query_job_id,
                self.read.observed_at,PROJECT,LOCATION),
            BrandContextRead(self.read.packet,self.read.query_job_id,self.read.observed_at+timedelta(seconds=1),
                PROJECT,LOCATION),
        )
        for candidate in candidates:
            with self.subTest(candidate=candidate),self.assertRaises(ContractError):self.project(candidate)

    def test_inactive_rule_stale_chunk_and_rejected_claim_fail_closed(self):
        mutations = (
            lambda packet: packet['poc_rules'][0].update(rule_status='INACTIVE'),
            lambda packet: packet['policy_chunks'][0].update(content_sha256='0'*64),
            lambda packet: packet['approved_claims'][0].update(approval_status='REJECTED'),
        )
        for mutate in mutations:
            packet=deepcopy(self.read.packet);mutate(packet)
            candidate=BrandContextRead(packet,self.read.query_job_id,self.read.observed_at,PROJECT,LOCATION)
            with self.subTest(mutate=mutate),self.assertRaises(ContractError):self.project(candidate)


class AnalyticsEvidenceProjectionTests(unittest.TestCase):
    def setUp(self):
        self.attempt,self.request,self.job=execution()

    def test_sales_projects_unique_aggregate_periods_with_exact_numeric_strings(self):
        read=captured_sales_read()
        rows=project_sales_analytics(read,evidence_snapshot_id='evidence-snapshot-1',request=self.request,
            job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=TIME+timedelta(seconds=3))
        self.assertEqual(len(rows),3)  # Full/month duplicate plus the two trend windows.
        self.assertTrue(all(row['source_revision']=='bq-sales-read-1' for row in rows))
        self.assertTrue(all(row['source_kind']=='SALES_SUMMARY' for row in rows))
        full=next(row for row in rows if row['evidence_payload']['record']['start_date']=='2026-01-01'
                  and row['evidence_payload']['record']['end_date']=='2026-01-04')
        self.assertEqual(full['evidence_payload']['record']['net_sales'],'6')
        for row in rows:
            validate_evidence_snapshot(row,request=self.request,job=self.job,attempt=self.attempt,
                conversation=CONVERSATION)

    def test_sales_tampering_wrong_scope_and_local_fallback_fail(self):
        read=captured_sales_read()
        candidates=[]
        packet=deepcopy(read.packet);packet['totals']['net_sales']=Decimal(7)
        candidates.append(SalesAnalyticsRead(packet,read.query_job_id,read.observed_at,PROJECT,LOCATION))
        candidates.append(SalesAnalyticsRead(read.packet|{'local_fallback':'file.json'},read.query_job_id,
            read.observed_at,PROJECT,LOCATION))
        candidates.append(SalesAnalyticsRead(read.packet,read.query_job_id,read.observed_at,'other',LOCATION))
        for candidate in candidates:
            with self.subTest(candidate=candidate),self.assertRaises(ContractError):
                project_sales_analytics(candidate,evidence_snapshot_id='evidence-snapshot-1',request=self.request,
                    job=self.job,attempt=self.attempt,conversation=CONVERSATION,
                    created_at=TIME+timedelta(seconds=3))

    def test_campaign_performance_projects_exact_aggregate_and_snapshot(self):
        read=captured_performance_read()
        rows=project_campaign_performance(read,evidence_snapshot_id='evidence-snapshot-1',request=self.request,
            job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=TIME+timedelta(seconds=3))
        self.assertEqual(len(rows),1);row=rows[0]
        self.assertEqual(row['source_revision'],'bq-performance-read-1')
        self.assertEqual(row['knowledge_snapshot_id'],digest('snapshot'))
        self.assertEqual(row['evidence_payload']['record']['ctr'],'0.100000000')
        validate_evidence_snapshot(row,request=self.request,job=self.job,attempt=self.attempt,
            conversation=CONVERSATION)

    def test_campaign_creative_projects_ranked_text_vector_and_snapshot(self):
        read=captured_creative_read()
        rows=project_campaign_creatives(read,evidence_snapshot_id='evidence-snapshot-1',
            request=self.request,job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=TIME+timedelta(seconds=3))
        self.assertEqual(len(rows),1);row=rows[0]
        self.assertEqual(row['source_revision'],'bq-creative-read-1')
        self.assertEqual(row['knowledge_snapshot_id'],digest('snapshot'))
        self.assertEqual((row['rank'],row['cosine_distance']),(1,0.2))
        self.assertEqual((row['embedding_model'],row['embedding_dimensions']),
            ('gemini-embedding-001',768))
        validate_evidence_snapshot(row,request=self.request,job=self.job,attempt=self.attempt,
            conversation=CONVERSATION)

    def test_campaign_creative_tampering_or_local_fallback_fails_closed(self):
        read=captured_creative_read();packet=deepcopy(read.packet)
        packet['creatives'][0]['content_sha256']='0'*64
        candidates=(CampaignCreativeRead(packet,read.query_job_id,read.observed_at,PROJECT,LOCATION),
            CampaignCreativeRead(read.packet|{'local_fallback':'file.json'},read.query_job_id,
                read.observed_at,PROJECT,LOCATION),
            CampaignCreativeRead(read.packet,read.query_job_id,read.observed_at,'other',LOCATION))
        for candidate in candidates:
            with self.subTest(candidate=candidate),self.assertRaises(ContractError):
                project_campaign_creatives(candidate,evidence_snapshot_id='evidence-snapshot-1',
                    request=self.request,job=self.job,attempt=self.attempt,conversation=CONVERSATION,
                    created_at=TIME+timedelta(seconds=3))

    def test_campaign_ratio_snapshot_scope_and_packet_shape_fail(self):
        read=captured_performance_read();candidates=[]
        packet=deepcopy(read.packet);packet['summaries'][0]['ctr']=Decimal('0.2')
        candidates.append(CampaignPerformanceRead(packet,read.query_job_id,read.observed_at,PROJECT,LOCATION))
        packet=deepcopy(read.packet);packet['snapshot_id']='other'
        candidates.append(CampaignPerformanceRead(packet,read.query_job_id,read.observed_at,PROJECT,LOCATION))
        candidates.append(CampaignPerformanceRead(read.packet|{'raw_daily_rows':[]},read.query_job_id,
            read.observed_at,PROJECT,LOCATION))
        for candidate in candidates:
            with self.subTest(candidate=candidate),self.assertRaises(ContractError):
                project_campaign_performance(candidate,evidence_snapshot_id='evidence-snapshot-1',
                    request=self.request,job=self.job,attempt=self.attempt,conversation=CONVERSATION,
                    created_at=TIME+timedelta(seconds=3))


class ImageEvidenceProjectionTests(unittest.TestCase):
    def setUp(self):
        self.attempt,self.request,self.job=execution();self.product,self.campaigns,self.generated=captured_image_reads()

    def kwargs(self):
        return dict(evidence_snapshot_id='evidence-snapshot-1',request=self.request,job=self.job,
            attempt=self.attempt,conversation=CONVERSATION,created_at=TIME+timedelta(seconds=3))

    def test_projects_product_campaign_and_reviewed_generated_images(self):
        rows=[*project_product_image(self.product,**self.kwargs()),
            *project_campaign_images(self.campaigns,**self.kwargs()),
            *project_generated_images(self.generated,**self.kwargs())]
        self.assertEqual([row['source_kind'] for row in rows],
            ['PRODUCT_IMAGE','CAMPAIGN_IMAGE','GENERATED_IMAGE'])
        self.assertIsNone(rows[0]['rank']);self.assertEqual(rows[1]['rank'],1)
        self.assertEqual(rows[2]['review_revision'],2)
        self.assertTrue(rows[2]['source_revision'].startswith('bqset-'))
        for row in rows:
            validate_evidence_snapshot(row,request=self.request,job=self.job,attempt=self.attempt,
                conversation=CONVERSATION)

    def test_changed_bytes_invalid_generation_review_and_cross_sku_fail_closed(self):
        product=ProductImageRead(self.product.metadata,b'changed',self.product.query_job_id,
            self.product.observed_at,PROJECT,LOCATION)
        with self.assertRaises(ContractError):project_product_image(product,**self.kwargs())
        campaign_meta=deepcopy(self.campaigns.images[0][0]);campaign_meta['gcs_generation']='0'
        campaigns=CampaignImagesRead([(campaign_meta,self.campaigns.images[0][1])],self.campaigns.query_job_id,
            self.campaigns.observed_at,PROJECT,LOCATION)
        with self.assertRaises(ContractError):project_campaign_images(campaigns,**self.kwargs())
        generated_meta=deepcopy(self.generated.images[0][0]);generated_meta['review']['status']='REJECTED'
        generated=GeneratedBannerSearchRead(
            self.generated.packet|{'results':[generated_meta]},[(generated_meta,self.generated.images[0][1])],
            self.generated.ranking_job_ids,self.generated.banner_reads,self.generated.observed_at,PROJECT,LOCATION)
        with self.assertRaises(ContractError):project_generated_images(generated,**self.kwargs())
        generated_meta=deepcopy(self.generated.images[0][0]);generated_meta['canonical_sku']='CI-SKU-0002'
        generated=GeneratedBannerSearchRead(
            self.generated.packet|{'results':[generated_meta]},[(generated_meta,self.generated.images[0][1])],
            self.generated.ranking_job_ids,self.generated.banner_reads,self.generated.observed_at,PROJECT,LOCATION)
        with self.assertRaises(ContractError):project_generated_images(generated,**self.kwargs())


class VisualEvidenceProjectionTests(unittest.TestCase):
    def setUp(self):
        self.attempt,self.request,self.job=execution()
        from src.application.workflow_contract import request_fingerprint
        self.request['visual_reference']='NAKED_AND_THRIVING'
        self.request['submitted_preferences']['preferences']['visual_reference']='NAKED_AND_THRIVING'
        self.request['request_sha256']=request_fingerprint(self.request)
        self.job['request_sha256']=self.request['request_sha256']

    def project(self,read):
        return project_visual_reference(read,evidence_snapshot_id='evidence-snapshot-1',
            request=self.request,job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=TIME+timedelta(seconds=3))

    def test_projects_all_four_styling_sections_from_native_read(self):
        rows=self.project(captured_visual_read())
        self.assertEqual(len(rows),4)
        self.assertEqual({row['source_revision'] for row in rows},{'bq-visual-read-1'})
        self.assertEqual({row['source_table'] for row in rows},
            {f'{PROJECT}.ci_brand_memory.poc_visual_reference_chunks'})
        for row in rows:
            validate_evidence_snapshot(row,request=self.request,job=self.job,attempt=self.attempt,
                conversation=CONVERSATION)

    def test_missing_section_none_request_and_local_fallback_fail_closed(self):
        read=captured_visual_read();packet=deepcopy(read.packet);packet['visual_chunks'].pop()
        candidates=(VisualReferenceRead(packet,read.query_job_id,read.observed_at,PROJECT,LOCATION),
            VisualReferenceRead(read.packet|{'local_fallback':'file.json'},read.query_job_id,
                read.observed_at,PROJECT,LOCATION))
        for candidate in candidates:
            with self.subTest(candidate=candidate),self.assertRaises(ContractError):self.project(candidate)
        self.request['visual_reference']='NONE'
        with self.assertRaises(ContractError):self.project(read)


if __name__ == '__main__':
    unittest.main()
