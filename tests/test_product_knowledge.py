import copy
import hashlib
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

from src.retrieval.product_knowledge import (
    PROJECT, SQL, build_packet, capture_product_knowledge, retrieve_product_knowledge,
)


class ProductKnowledgeTests(unittest.TestCase):
    def setUp(self):
        self.sku = 'CI-SKU-0001'
        self.image = f'gs://creative-brand-memory-{PROJECT}/products/{self.sku}/primary.jpg'
        self.product = dict(sku=self.sku,brand='Source brand',product_name='Exact source product',
            product_type='Serum',price=Decimal('39.5'),currency='eur',primary_image_uri=self.image,
            source_system='BEAUTYFEEDS')
        self.claim = dict(claim_id='POC-IDENTITY-1',sku=self.sku,claim_type='PRODUCT_NAME',
            claim_text=self.product['product_name'],evidence_excerpt=self.product['product_name'],
            evidence_reference=f'{PROJECT}.ci_product.product_master#sku={self.sku}&field=product_name',
            evidence_source_system='BEAUTYFEEDS',approval_status='APPROVED',approval_scope='SYNTHETIC_POC_ONLY',
            approved_by='SYNTHETIC_POC_REVIEWER',approval_reference='config/brand_memory.json#review_fixture',
            approved_at=datetime(2025,1,1,tzinfo=timezone.utc),review_notes='Mock identity review only',
            source_system='SYNTHETIC_POC')
        chunks = []
        for kind,table,text in (('PRODUCT_KNOWLEDGE','product_master','Exact source product; identity only'),
                                ('INGREDIENT_KNOWLEDGE','product_ingredients','Stored source ingredient wording')):
            ref = f'{PROJECT}.ci_product.{table}#sku={self.sku}'
            chunks.append(dict(snapshot_id='snapshot-1',chunk_id=kind,chunk_type=kind,brand=self.product['brand'],
                sku=self.sku,chunk_text=text,source_system='BEAUTYFEEDS',source_reference=ref,
                evidence_references=[ref],image_uris=[self.image],retrieval_status='FACTUAL_CONTEXT',
                approval_scope='SOURCE_PRODUCT_FACTS',claim_id=None,content_sha256=hashlib.sha256(text.encode()).hexdigest()))
        identity=copy.deepcopy(chunks[0]);text='Mock reviewed identity: Exact source product'
        identity.update(chunk_id='identity-chunk',chunk_type='POC_APPROVED_CLAIM',chunk_text=text,
            source_system='SYNTHETIC_POC',retrieval_status='POC_APPROVED',approval_scope='SYNTHETIC_POC_ONLY',
            claim_id=self.claim['claim_id'],evidence_references=[self.claim['evidence_reference']],
            image_uris=[],content_sha256=hashlib.sha256(text.encode()).hexdigest())
        chunks.append(identity)
        self.result = dict(products=[self.product],knowledge_chunks=chunks,poc_identity_claims=[self.claim])

    def client(self):
        client=Mock()
        job=client.query.return_value
        job.result.return_value=[self.result]
        job.job_id='bq-product-read-1'
        job.project=PROJECT
        job.location='asia-south1'
        return client

    def test_grounded_packet_preserves_sources_and_recorded_text(self):
        original=copy.deepcopy(self.result)
        packet=build_packet(self.sku,self.result)
        self.assertEqual(packet['product'],self.product)
        self.assertIsInstance(packet['product']['price'],Decimal)
        self.assertEqual(len(packet['factual_chunks']),2)
        self.assertEqual(len(packet['poc_identity_chunks']),1)
        self.assertEqual(packet['factual_chunks'][1]['chunk_text'],'Stored source ingredient wording')
        self.assertEqual(packet['poc_identity_claims'][0]['approval_scope'],'SYNTHETIC_POC_ONLY')
        self.assertEqual(self.result,original)

    def test_invalid_sku_rejected_before_query(self):
        client=Mock()
        for sku in ('OTHER', 'CI-SKU-0021', [], None):
            with self.assertRaises(ValueError):retrieve_product_knowledge(client,sku)
        client.query.assert_not_called()

    def test_missing_duplicate_and_cross_sku_product_rejected(self):
        for products in ([],[self.product,self.product],[{**self.product,'sku':'CI-SKU-0002'}],
                         [{**self.product,'source_system':'SYNTHETIC_POC'}]):
            with self.assertRaises(ValueError):build_packet(self.sku,{**self.result,'products':products})

    def test_hash_brand_image_and_snapshot_rejected(self):
        for key,value in (('content_sha256','tampered'),('brand','Other brand'),('sku','CI-SKU-0002'),
                          ('image_uris',['gs://other/image.jpg']),('snapshot_id','other-snapshot'),
                          ('evidence_references',[])):
            result=copy.deepcopy(self.result);result['knowledge_chunks'][0][key]=value
            with self.subTest(field=key),self.assertRaises(ValueError):build_packet(self.sku,result)

    def test_revoked_review_and_benefit_claim_rejected(self):
        for key,value in (('approval_status','REJECTED'),('approval_status','PENDING_REVIEW'),
                          ('claim_type','BENEFIT'),('approval_scope','MANUFACTURER_APPROVED'),
                          ('approved_by','UNKNOWN'),('claim_text','Invented benefit'),('approved_at',None)):
            result=copy.deepcopy(self.result);result['poc_identity_claims'][0][key]=value
            with self.subTest(field=key),self.assertRaises(ValueError):build_packet(self.sku,result)
        with self.assertRaises(ValueError):build_packet(self.sku,{**self.result,'poc_identity_claims':[]})

    def test_no_approved_identity_is_valid_when_absent_from_current_chunks(self):
        packet=build_packet(self.sku,{**self.result,'knowledge_chunks':self.result['knowledge_chunks'][:2],
            'poc_identity_claims':[]})
        self.assertEqual(packet['poc_identity_claims'],[])
        self.assertEqual(packet['poc_identity_chunks'],[])

    def test_unrelated_or_incomplete_chunks_rejected(self):
        for kind in ('WEBSITE_VISUAL_REFERENCE','CAMPAIGN_LEARNING','CLAIM_REVIEW_NOTE','DAILY_SALES'):
            result=copy.deepcopy(self.result);result['knowledge_chunks'][0]['chunk_type']=kind
            with self.subTest(kind=kind),self.assertRaises(ValueError):build_packet(self.sku,result)
        for chunks in ([],self.result['knowledge_chunks'][:1],self.result['knowledge_chunks']*2):
            with self.assertRaises(ValueError):build_packet(self.sku,{**self.result,'knowledge_chunks':chunks})

    def test_parameterized_read_only_query_and_current_eligibility(self):
        client=self.client()
        packet=retrieve_product_knowledge(client,self.sku)
        self.assertEqual(packet['canonical_sku'],self.sku)
        arguments=client.query.call_args
        self.assertEqual(arguments.kwargs['location'],'asia-south1')
        self.assertEqual(arguments.kwargs['job_config'].maximum_bytes_billed,64*1024*1024)
        self.assertFalse(arguments.kwargs['job_config'].use_query_cache)
        parameter=arguments.kwargs['job_config'].query_parameters[0]
        self.assertEqual((parameter.name,parameter.type_,parameter.value),('sku','STRING',self.sku))
        sql=arguments.args[0]
        self.assertEqual(sql,SQL.read_text())
        self.assertIn('WHERE sku=@sku',sql)
        self.assertIn('poc_generation_knowledge_chunks',sql)
        self.assertIn('poc_approved_product_claims',sql)
        for mutation in ('INSERT','UPDATE','DELETE','MERGE','CREATE','DROP'):
            self.assertNotIn(mutation,sql)
        client.create_table.assert_not_called()
        client.load_table_from_dataframe.assert_not_called()

    def test_billing_cap_is_configurable_and_validated_before_query(self):
        client=self.client()
        retrieve_product_knowledge(client,self.sku,maximum_bytes_billed=80_000_000)
        self.assertEqual(client.query.call_args.kwargs['job_config'].maximum_bytes_billed,80_000_000)
        client.reset_mock()
        for cap in (0,-1,True,'unlimited'):
            with self.assertRaises(ValueError):retrieve_product_knowledge(client,self.sku,cap)
        client.query.assert_not_called()

    def test_capture_binds_native_job_identity_and_observation(self):
        captured=capture_product_knowledge(self.client(),self.sku)
        self.assertEqual(captured.query_job_id,'bq-product-read-1')
        self.assertEqual((captured.project,captured.location),(PROJECT,'asia-south1'))
        self.assertEqual(captured.packet['canonical_sku'],self.sku)
        self.assertIsNotNone(captured.observed_at.utcoffset())

    def test_capture_rejects_wrong_or_missing_native_job_identity(self):
        for field,value in (('job_id','bad job'),('project','other'),('location','us-central1')):
            client=self.client();setattr(client.query.return_value,field,value)
            with self.subTest(field=field),self.assertRaises(ValueError):
                capture_product_knowledge(client,self.sku)

    def test_bigquery_failure_stops_retrieval_without_local_json_fallback(self):
        client=Mock();client.query.side_effect=RuntimeError('BigQuery unavailable')
        sql=SQL.read_text()

        def read_sql_only(path, *args, **kwargs):
            self.assertEqual(path,SQL,'Retrieval must not read local JSON evidence')
            return sql

        with patch('pathlib.Path.read_text',new=read_sql_only):
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_product_knowledge(client,self.sku)


if __name__=='__main__':
    unittest.main()
