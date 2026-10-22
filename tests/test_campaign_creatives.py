import hashlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.generators.sales_generator import PROJECT
from src.retrieval.campaign_creatives import (
    BUCKET, CONFIG, SQL, CampaignCreativeRead, build_packet, capture_campaign_creatives, embed_query, query_text,
    retrieve_campaign_creatives, validate_config,
)
from src.retrieval.request import RetrievalRequest


class CampaignCreativeTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads(CONFIG.read_text())
        self.request = RetrievalRequest('CI-SKU-0001','CONVERSION','Skincare shoppers',
                                        'PAID_SOCIAL','NAKED_AND_THRIVING')
        self.bq = Mock()
        self.embedder = Mock()
        self.vector = [0.1]*768
        self.embedding = SimpleNamespace(values=self.vector,statistics=SimpleNamespace(truncated=False))
        self.embedder.models.embed_content.return_value = SimpleNamespace(embeddings=[self.embedding])
        self.rows = [self.creative(1,'AWARENESS','PAID_SOCIAL',0.3),
                     self.creative(2,'CONVERSION','EMAIL',0.2)]
        query_job=self.bq.query.return_value
        query_job.result.return_value=self.rows
        query_job.job_id='bq-campaign-creative-read-1'
        query_job.project=PROJECT
        query_job.location='asia-south1'

    def creative(self, index, objective, channel, distance):
        campaign_id = f'SYN-CMP-20250101-CI-SKU-0001-{index:02d}'
        asset_id = f'SYN-AST-{campaign_id}-template-v1'
        campaign_ref = f'{PROJECT}.ci_creative.campaign_master#campaign_id={campaign_id}'
        asset_ref = f'{PROJECT}.ci_creative.campaign_assets#asset_id={asset_id}'
        product_image = f'gs://{BUCKET}/products/CI-SKU-0001/primary.jpg'
        banner_hash = 'a'*64
        banner = f'gs://{BUCKET}/campaigns/{campaign_id}/template-v1/{banner_hash}.png'
        text = f'SYNTHETIC_POC creative reference {index}'
        return dict(sku='CI-SKU-0001',brand='Source brand',product_name='Source product',
            snapshot_id='current-snapshot',chunk_id=f'KCH-{index}',asset_id=asset_id,
            campaign_id=campaign_id,chunk_type='CAMPAIGN_CREATIVE',chunk_text=text,
            content_sha256=hashlib.sha256(text.encode()).hexdigest(),source_system='SYNTHETIC_POC',
            retrieval_status='POC_CONTEXT',approval_scope='SYNTHETIC_POC_ONLY',claim_id=None,
            source_reference=campaign_ref,evidence_references=[campaign_ref,asset_ref],
            source_image_uri=product_image,gcs_uri=banner,banner_sha256=banner_hash,
            image_uris=[product_image,banner],model_id='gemini-embedding-001',dimension=768,
            model_location='asia-south1',task_type='RETRIEVAL_DOCUMENT',objective=objective,
            channel=channel,audience='Source audience',distance=distance)

    def test_rank_and_expose_nonmatching_preferences_without_remapping_sku(self):
        packet = retrieve_campaign_creatives(self.bq,self.embedder,self.request,self.config)
        self.assertEqual([c['rank'] for c in packet['creatives']],[1,2])
        conversion,awareness = packet['creatives']
        self.assertTrue(conversion['objective_match'])
        self.assertFalse(conversion['channel_match'])
        self.assertFalse(awareness['objective_match'])
        self.assertTrue(awareness['channel_match'])
        self.assertEqual({c['sku'] for c in packet['creatives']},{self.request.canonical_sku})
        self.assertEqual(packet['source_of_truth'],'BIGQUERY')
        self.assertEqual(packet['source_system'],'SYNTHETIC_POC')
        self.assertFalse(packet['query_embedding']['stored'])
        # The function must not mutate source rows or invent effectiveness/claims.
        self.assertNotIn('rank',self.rows[0])
        self.assertNotIn('approved_claims',packet)
        self.assertNotIn('performance',packet)

    def test_query_uses_matching_model_with_query_task_and_no_styling_scope(self):
        vector = embed_query(self.embedder,query_text(self.request),self.config)
        self.assertEqual(vector,self.vector)
        call = self.embedder.models.embed_content.call_args.kwargs
        self.assertEqual(call['model'],'gemini-embedding-001')
        self.assertEqual(call['config'].task_type,'RETRIEVAL_QUERY')
        self.assertEqual(call['config'].output_dimensionality,768)
        self.assertFalse(call['config'].auto_truncate)
        self.assertNotIn('NAKED_AND_THRIVING',call['contents'])

    def test_bound_parameters_and_read_only_current_eligibility_query(self):
        retrieve_campaign_creatives(self.bq,self.embedder,self.request,self.config)
        call = self.bq.query.call_args
        job = call.kwargs['job_config']
        params = {p.name:p for p in job.query_parameters}
        self.assertEqual(params['sku'].value,'CI-SKU-0001')
        self.assertEqual(params['query_vector'].values,self.vector)
        self.assertEqual(params['top_k'].value,3)
        self.assertEqual(job.maximum_bytes_billed,self.config['maximum_bytes_billed'])
        self.assertFalse(job.use_query_cache)
        self.assertEqual(call.kwargs['location'],'asia-south1')
        self.assertIn('poc_generation_text_embeddings',call.args[0])
        self.assertIn('COSINE_DISTANCE(k.vector,@query_vector)',call.args[0])
        for mutation in ('INSERT','UPDATE','DELETE','MERGE','CREATE','DROP'):
            self.assertNotIn(mutation,call.args[0])
        self.bq.create_table.assert_not_called()
        self.bq.load_table_from_json.assert_not_called()

    def test_capture_binds_validated_packet_to_native_bigquery_job(self):
        captured=capture_campaign_creatives(self.bq,self.embedder,self.request,self.config)
        self.assertIsInstance(captured,CampaignCreativeRead)
        self.assertEqual(captured.packet['creatives'][0]['chunk_id'],'KCH-2')
        self.assertEqual(captured.query_job_id,'bq-campaign-creative-read-1')
        self.assertEqual(captured.project,PROJECT)
        self.assertEqual(captured.location,'asia-south1')
        self.assertIsNotNone(captured.observed_at.tzinfo)

    def test_capture_rejects_missing_or_cross_project_native_job_identity(self):
        query_job=self.bq.query.return_value
        for field,value in (('job_id',None),('project','other'),('location','US')):
            original=getattr(query_job,field);setattr(query_job,field,value)
            with self.subTest(field=field),self.assertRaises(ValueError):
                capture_campaign_creatives(self.bq,self.embedder,self.request,self.config)
            setattr(query_job,field,original)

    def test_ties_have_stable_order_and_result_limit_is_configured(self):
        tied = [{**r,'distance':0.2} for r in reversed(self.rows)]
        packet = build_packet(self.request,tied,self.config)
        self.assertEqual([c['chunk_id'] for c in packet['creatives']],['KCH-1','KCH-2'])
        with self.assertRaises(ValueError):
            build_packet(self.request,self.rows,{**self.config,'top_k':1})

    def test_missing_candidates_are_explicit_without_invented_references(self):
        packet = build_packet(self.request,[],self.config)
        self.assertEqual(packet['creatives'],[])
        self.assertIsNone(packet['snapshot_id'])

    def test_cross_sku_review_only_stale_hash_and_unrelated_assets_rejected(self):
        row = self.rows[0]
        for key,value in (
            ('sku','CI-SKU-0002'),('source_system','BEAUTYFEEDS'),('claim_id','claim'),
            ('chunk_type','CAMPAIGN_LEARNING'),('retrieval_status','REVIEW_ONLY'),
            ('approval_scope','VISUAL_INSPIRATION_ONLY'),('content_sha256','stale'),
            ('source_reference','unrelated'),('evidence_references',[]),('image_uris',[]),
            ('gcs_uri','gs://other/banner.png'),('source_image_uri','gs://other/product.jpg'),
            ('dimension',512),('model_id','multimodalembedding@001'),('model_location','global'),
            ('task_type','IMAGE'),('distance',float('nan')),('distance',float('inf')),
            ('distance',-1),('distance',True),('distance',3)):
            with self.subTest(field=key,value=value),self.assertRaises(ValueError):
                build_packet(self.request,[{**row,key:value}],self.config)

    def test_duplicate_and_mixed_snapshot_evidence_rejected(self):
        with self.assertRaises(ValueError):build_packet(self.request,[self.rows[0]]*2,self.config)
        with self.assertRaises(ValueError):
            build_packet(self.request,[self.rows[0],{**self.rows[1],'snapshot_id':'old'}],self.config)

    def test_bad_config_stops_before_any_cloud_calls(self):
        for key,value in (('dimension',512),('dimension',True),('model_id','other'),
                          ('location','global'),('top_k',0),('top_k',True),
                          ('maximum_bytes_billed',0),('maximum_bytes_billed',True)):
            with self.subTest(field=key),self.assertRaises(ValueError):
                retrieve_campaign_creatives(self.bq,self.embedder,self.request,{**self.config,key:value})
        self.bq.query.assert_not_called();self.embedder.models.embed_content.assert_not_called()

    def test_invalid_truncated_or_missing_query_vector_stops_before_bigquery(self):
        for values,truncated in (([],False),([0.0]*768,False),([float('nan')]*768,False),
                                 ([True]*768,False),([0.1]*768,True)):
            self.embedding.values = values;self.embedding.statistics.truncated = truncated
            with self.subTest(truncated=truncated),self.assertRaises(ValueError):
                retrieve_campaign_creatives(self.bq,self.embedder,self.request,self.config)
        self.embedder.models.embed_content.return_value.embeddings = []
        with self.assertRaises(ValueError):embed_query(self.embedder,'query',self.config)
        self.bq.query.assert_not_called()

    def test_cloud_failures_propagate_without_local_embedding_or_export_fallback(self):
        sql = SQL.read_text()
        def read_sql_only(path,*args,**kwargs):
            self.assertEqual(path,SQL)
            return sql
        with patch('pathlib.Path.read_text',new=read_sql_only):
            self.bq.query.side_effect = RuntimeError('BigQuery unavailable')
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_campaign_creatives(self.bq,self.embedder,self.request,self.config)
            self.bq.query.side_effect = None
            self.embedder.models.embed_content.side_effect = RuntimeError('Vertex unavailable')
            self.bq.query.reset_mock()
            with self.assertRaisesRegex(RuntimeError,'Vertex unavailable'):
                retrieve_campaign_creatives(self.bq,self.embedder,self.request,self.config)
            self.bq.query.assert_not_called()


if __name__ == '__main__':
    unittest.main()
