import copy
import hashlib
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from src.generators.sales_generator import PROJECT
from src.retrieval.brand_context import SQL, build_packet, capture_brand_context, retrieve_brand_context


class BrandContextTests(unittest.TestCase):
    def setUp(self):
        self.sku = 'CI-SKU-0001'
        self.now = datetime(2026,10,8,tzinfo=timezone.utc)
        self.product = dict(sku=self.sku,brand='Honest Beauty',product_name='Source product name',source_system='BEAUTYFEEDS')
        rules = [];chunks = []
        for index,brand in enumerate(('*',self.product['brand'])):
            rule_id = f'POC-RULE-{index}'
            rule = dict(rule_id=rule_id,brand=brand,scope='PROJECT_POC',category='GROUNDING',
                rule_text=f'Retain evidence {index}.',rule_status='ACTIVE_FOR_POC',authority='PROJECT_POC_POLICY',
                source_system='SYNTHETIC_POC',source_reference=f'config/brand_memory.json#{rule_id}')
            text = (f'Type: POC_RULE. Brand: {brand}. Status: POC_POLICY. Scope: SYNTHETIC_POC_ONLY.\n'
                    f'SYNTHETIC_POC: not actual historical business evidence.\n{rule["rule_text"]}')
            chunk = dict(snapshot_id='current-snapshot',chunk_id=f'KCH-{index}',chunk_type='POC_RULE',
                brand=brand,sku=None,campaign_id=None,asset_id=None,claim_id=None,chunk_text=text,
                source_system='SYNTHETIC_POC',source_reference=f'{PROJECT}.ci_brand_memory.brand_guidelines#rule_id={rule_id}',
                evidence_references=[rule['source_reference']],image_uris=[],retrieval_status='POC_POLICY',
                approval_scope='SYNTHETIC_POC_ONLY',content_sha256=hashlib.sha256(text.encode()).hexdigest())
            rules.append(rule);chunks.append(chunk)
        self.claim = dict(claim_id=f'POC-CLM-{self.sku}-PRODUCT_NAME-example',sku=self.sku,claim_type='PRODUCT_NAME',
            claim_text=self.product['product_name'],evidence_excerpt=self.product['product_name'],
            evidence_reference=f'{PROJECT}.ci_product.product_master#sku={self.sku}&field=product_name',
            evidence_source_system='BEAUTYFEEDS',approval_status='APPROVED',approval_scope='SYNTHETIC_POC_ONLY',
            approved_by='SYNTHETIC_POC_REVIEWER',approval_reference='config/brand_memory.json#review_fixture',
            approved_at=self.now-timedelta(days=1),review_notes='Mock POC review only',source_system='SYNTHETIC_POC')
        self.result = dict(checked_at=self.now,products=[self.product],poc_rules=rules,
                           policy_chunks=chunks,approved_claims=[self.claim])
        self.client = Mock();self.client.query.return_value.result.return_value = [self.result]
        self.client.query.return_value.job_id = 'bq-brand-read-1'
        self.client.query.return_value.project = PROJECT
        self.client.query.return_value.location = 'asia-south1'

    def test_global_and_matching_brand_policy_with_current_identity_approval(self):
        packet = retrieve_brand_context(self.client,self.sku)
        self.assertEqual(packet['source_of_truth'],'BIGQUERY')
        self.assertEqual({r['brand'] for r in packet['poc_rules']},{'*','Honest Beauty'})
        self.assertEqual(packet['allowed_claim_types'],['PRODUCT_NAME'])
        self.assertEqual(packet['approved_claims'][0]['claim_text'],self.product['product_name'])
        self.assertEqual(packet['policy_authority'],'PROJECT_POC_POLICY')
        self.assertEqual(packet['claim_approval_scope'],'SYNTHETIC_POC_ONLY')
        self.assertEqual(packet['checked_at'],self.now)
        self.assertEqual(len(packet['approved_claims'][0]['evidence_references']),3)
        self.assertNotIn('evidence_references',self.claim)

    def test_currently_revoked_approval_stays_empty_without_snapshot_fallback(self):
        packet = build_packet(self.sku,{**self.result,'approved_claims':[]})
        self.assertEqual(packet['approved_claims'],[])
        self.assertEqual(packet['allowed_claim_types'],[])
        self.assertEqual(len(packet['poc_rules']),2)

    def test_pending_rejected_benefit_wrong_sku_or_manufacturer_claim_cannot_pass(self):
        for key,value in (('approval_status','PENDING_REVIEW'),('approval_status','REJECTED'),
                          ('claim_type','BENEFIT'),('claim_type','INGREDIENT_PRESENCE'),('sku','CI-SKU-0002'),
                          ('approval_scope','MANUFACTURER_APPROVED'),('claim_text','Invented benefit'),
                          ('evidence_excerpt','Altered name'),('evidence_reference','unrelated'),
                          ('source_system','OFFICIAL_BRAND_WEBSITE'),('evidence_source_system','SYNTHETIC_POC'),
                          ('approved_by','MANUFACTURER'),('approval_reference','unrelated'),
                          ('approved_at',None),('approved_at',self.now+timedelta(seconds=1)),
                          ('approved_at',self.now.replace(tzinfo=None))):
            result = dict(self.result,approved_claims=[{**self.claim,key:value}])
            with self.subTest(field=key,value=value),self.assertRaises(ValueError):build_packet(self.sku,result)

    def test_inactive_wrong_brand_or_website_policy_cannot_pass(self):
        for key,value in (('brand','Naked & Thriving'),('rule_status','INACTIVE'),('scope','VISUAL_INSPIRATION_ONLY'),
                          ('authority','MANUFACTURER_POLICY'),('source_system','OFFICIAL_BRAND_WEBSITE'),('rule_text','')):
            result = copy.deepcopy(self.result);result['poc_rules'][0][key] = value
            with self.subTest(field=key),self.assertRaises(ValueError):build_packet(self.sku,result)

    def test_updated_rule_text_requires_current_chunk_instead_of_silent_stale_serving(self):
        result = copy.deepcopy(self.result);result['poc_rules'][0]['rule_text'] = 'Updated active policy.'
        with self.assertRaisesRegex(ValueError,'Stale policy'):build_packet(self.sku,result)
        result = copy.deepcopy(self.result);result['policy_chunks'][0]['content_sha256'] = 'bad'
        with self.assertRaises(ValueError):build_packet(self.sku,result)

    def test_mis_scoped_chunks_and_inspiration_are_rejected(self):
        for key,value in (('sku',self.sku),('campaign_id','campaign'),('asset_id','image'),('claim_id','claim'),
                          ('chunk_type','WEBSITE_VISUAL_REFERENCE'),('brand','Naked & Thriving'),
                          ('approval_scope','VISUAL_INSPIRATION_ONLY'),('retrieval_status','REVIEW_ONLY'),
                          ('image_uris',['gs://image']),('evidence_references',[]),('source_reference','unknown')):
            result = copy.deepcopy(self.result);result['policy_chunks'][0][key] = value
            with self.subTest(field=key),self.assertRaises(ValueError):build_packet(self.sku,result)

    def test_missing_duplicate_or_mixed_snapshot_evidence_fails(self):
        for key,values in (('poc_rules',[]),('poc_rules',self.result['poc_rules']*2),
                            ('policy_chunks',self.result['policy_chunks'][:1]),
                            ('policy_chunks',self.result['policy_chunks']*2),('approved_claims',[self.claim]*2)):
            with self.assertRaises(ValueError):build_packet(self.sku,{**self.result,key:values})
        result = copy.deepcopy(self.result);result['policy_chunks'][0]['snapshot_id'] = 'old'
        with self.assertRaises(ValueError):build_packet(self.sku,result)

    def test_wrong_product_or_approval_check_timestamp_fails(self):
        for key,value in (('sku','CI-SKU-0002'),('source_system','SYNTHETIC_POC')):
            with self.assertRaises(ValueError):build_packet(self.sku,{**self.result,'products':[{**self.product,key:value}]})
        for checked_at in (None,self.now.replace(tzinfo=None)):
            with self.assertRaises(ValueError):build_packet(self.sku,{**self.result,'checked_at':checked_at})
        with self.assertRaises(ValueError):build_packet(self.sku,{**self.result,'products':[]})

    def test_read_only_query_and_invalid_inputs(self):
        retrieve_brand_context(self.client,self.sku,110_000_000)
        call = self.client.query.call_args;job = call.kwargs['job_config']
        self.assertEqual(job.query_parameters[0].value,self.sku)
        self.assertEqual(job.maximum_bytes_billed,110_000_000)
        self.assertFalse(job.use_query_cache)
        self.assertEqual(call.kwargs['location'],'asia-south1')
        self.assertIn('poc_approved_product_claims',call.args[0])
        self.assertIn('poc_generation_knowledge_chunks',call.args[0])
        for mutation in ('INSERT','UPDATE','DELETE','MERGE','CREATE','DROP'):
            self.assertNotIn(mutation,call.args[0])
        self.client.create_table.assert_not_called();self.client.load_table_from_json.assert_not_called()
        self.client.query.reset_mock()
        for sku,cap in (('OTHER',1),(self.sku,0),(self.sku,True)):
            with self.assertRaises(ValueError):retrieve_brand_context(self.client,sku,cap)
        self.client.query.assert_not_called()

    def test_capture_binds_native_job_and_server_observation(self):
        captured = capture_brand_context(self.client,self.sku)
        self.assertEqual(captured.query_job_id,'bq-brand-read-1')
        self.assertEqual((captured.project,captured.location),(PROJECT,'asia-south1'))
        self.assertEqual(captured.observed_at,self.now)
        self.assertIs(captured.packet['checked_at'],self.now)

    def test_capture_rejects_wrong_or_missing_native_job_identity(self):
        for field,value in (('job_id','bad job'),('project','other'),('location','us-central1')):
            setattr(self.client.query.return_value,field,value)
            with self.subTest(field=field),self.assertRaises(ValueError):
                capture_brand_context(self.client,self.sku)
            setattr(self.client.query.return_value,field,
                'bq-brand-read-1' if field=='job_id' else PROJECT if field=='project' else 'asia-south1')

    def test_bigquery_failure_does_not_read_local_rule_config_or_exports(self):
        sql = SQL.read_text()
        def read_sql_only(path,*args,**kwargs):
            self.assertEqual(path,SQL)
            return sql
        with patch('pathlib.Path.read_text',new=read_sql_only):
            self.client.query.side_effect = RuntimeError('BigQuery unavailable')
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_brand_context(self.client,self.sku)


if __name__ == '__main__':
    unittest.main()
