import copy
import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from google.cloud import bigquery

from src.generators.embeddings_generator import SCHEMA, record_identity
from src.generators.generated_banner_embeddings import (
    ASSET_SQL, CONFIG, IMAGE_TARGET, download_banner, embed_banner, embedding_view_sql,
    image_source, load_embedding, read_cloud_embedding, read_eligible_asset,
    validate_config, validate_selection,
)
from src.retrieval.generated_banner import capture_generated_banner, retrieve_generated_banner
from tests import test_register_generated_banner

MODULE = 'src.generators.generated_banner_embeddings.'


class GeneratedBannerEmbeddingTests(unittest.TestCase):
    def setUp(self):
        fixture = test_register_generated_banner.RegisterGeneratedBannerTests();fixture.setUp()
        self.addCleanup(fixture.temp.cleanup)
        self.asset = fixture.row;self.raw = fixture.preview.png
        self.config = json.loads(CONFIG.read_text());self.sku = self.asset['sku'];self.asset_id = self.asset['asset_id']
        self.source = image_source(self.asset,self.raw)
        self.identity = record_identity(self.source,self.config)
        self.vector = [0.1]*512
        self.embedding = dict(**self.identity,vector=self.vector,created_at=datetime.now(timezone.utc))
        self.stats = dict(eligible_vectors=1,stored_vectors=1,invalid_vectors=0,
                          product_vectors=20,historical_campaign_vectors=60,leaked_historical_vectors=0)

    def gcs(self):
        gcs = Mock();blob = gcs.bucket.return_value.blob.return_value
        blob.generation=int(self.asset['gcs_generation']);blob.content_type='image/png'
        blob.size=len(self.raw);blob.download_as_bytes.return_value=self.raw
        return gcs,blob

    def test_source_preserves_generation_hash_canonical_sku_and_generated_only_provenance(self):
        self.assertEqual(self.source['sku'],self.sku);self.assertIsNone(self.source['campaign_id'])
        self.assertEqual(self.source['source_system'],'SYNTHETIC_POC')
        self.assertIn('generated_campaign_assets',self.source['source_reference'])
        gcs,blob = self.gcs();self.assertEqual(download_banner(gcs,self.asset),self.raw)
        blob.download_as_bytes.assert_called_once_with(if_generation_match=int(self.asset['gcs_generation']))
        blob.upload_from_string.assert_not_called()

    def test_pending_rejected_real_approval_or_mismatched_source_is_rejected(self):
        for key,value in (('status','REJECTED'),('status','PENDING_REVIEW'),('source_system','ACTUAL'),
                          ('real_business_approved',True),('approval_scope','MANUFACTURER_APPROVED'),
                          ('gcs_generation','0'),('sha256','0'*64),('sku','CI-SKU-0002'),('width',12)):
            with self.subTest(key=key),self.assertRaises(ValueError):image_source({**self.asset,key:value},self.raw)
        with self.assertRaises(ValueError):image_source(self.asset,b'changed')

    def test_current_gcs_generation_mime_and_content_must_match(self):
        gcs,blob = self.gcs();blob.generation += 1
        with self.assertRaises(ValueError):download_banner(gcs,self.asset)
        blob.download_as_bytes.assert_not_called()
        blob.generation=int(self.asset['gcs_generation']);blob.content_type='image/jpeg'
        with self.assertRaises(ValueError):download_banner(gcs,self.asset)
        blob.content_type='image/png';blob.download_as_bytes.return_value=b'changed'
        with self.assertRaises(ValueError):download_banner(gcs,self.asset)

    def test_config_and_cross_sku_id_stop_before_cloud_access(self):
        validate_config(self.config);validate_selection(self.sku,self.asset_id)
        for key,value in (('location','global'),('image_dimension',768),('max_attempts',True),
                          ('image_model','another-model'),('maximum_bytes_billed',0)):
            with self.assertRaises(ValueError):validate_config({**self.config,key:value})
        with self.assertRaises(ValueError):validate_selection('CI-SKU-0002',self.asset_id)

    def test_missing_duplicate_or_ineligible_banner_fails_closed(self):
        client = Mock()
        for rows in ([],[self.asset,self.asset]):
            client.query.return_value.result.return_value=rows
            with self.assertRaises(ValueError):read_eligible_asset(client,self.sku,self.asset_id,self.config)
        args = client.query.call_args
        self.assertIn('poc_generated_banner_assets',args.args[0])
        self.assertEqual(args.kwargs['job_config'].query_parameters[0].value,self.sku)

    def test_vector_reuse_reads_only_bigquery_and_rejects_malformed_authoritative_vector(self):
        client=Mock();client.get_table.return_value=bigquery.Table(IMAGE_TARGET,schema=SCHEMA)
        client.query.return_value.result.return_value=[self.embedding]
        with patch('pathlib.Path.read_text',side_effect=AssertionError('No local vector cache')):
            self.assertEqual(read_cloud_embedding(client,self.identity,self.config),self.embedding)
        client.query.return_value.result.return_value=[{**self.embedding,'vector':[0.0]*512}]
        with self.assertRaises(ValueError):read_cloud_embedding(client,self.identity,self.config)
        client.query.return_value.result.return_value=[self.embedding,self.embedding]
        with self.assertRaises(ValueError):read_cloud_embedding(client,self.identity,self.config)

    def test_schema_explicit_insert_only_merge_does_not_replace_other_vectors_and_cleans_stage(self):
        client=Mock();client.get_table.return_value=bigquery.Table(IMAGE_TARGET,schema=SCHEMA)
        with patch(MODULE+'read_cloud_embedding',return_value=self.embedding):
            result=load_embedding(client,self.embedding,self.config)
        self.assertEqual(result,self.embedding)
        settings=client.load_table_from_json.call_args.kwargs['job_config']
        self.assertEqual(settings.schema,SCHEMA);self.assertFalse(settings.autodetect)
        sql=client.query.call_args.args[0]
        self.assertIn('BEGIN TRANSACTION',sql);self.assertIn('A.gcs_generation=S.gcs_generation',sql)
        self.assertIn('T.source_system=S.source_system',sql)
        self.assertNotIn('WHEN MATCHED',sql);self.assertNotIn('DELETE',sql)
        client.delete_table.assert_called_once()

    def test_failed_load_cleans_only_its_stage(self):
        client=Mock();client.get_table.return_value=bigquery.Table(IMAGE_TARGET,schema=SCHEMA)
        client.load_table_from_json.side_effect=RuntimeError('load failed')
        with self.assertRaises(RuntimeError):load_embedding(client,self.embedding,self.config)
        client.delete_table.assert_called_once()
        self.assertNotEqual(client.delete_table.call_args.args[0],IMAGE_TARGET)

    def pipeline(self, reused):
        stack = __import__('contextlib').ExitStack();self.addCleanup(stack.close)
        stack.enter_context(patch(MODULE+'publish_asset_view'))
        self.reader=stack.enter_context(patch(MODULE+'read_eligible_asset',return_value=self.asset))
        stack.enter_context(patch(MODULE+'download_banner',return_value=self.raw))
        stack.enter_context(patch(MODULE+'read_cloud_embedding',return_value=self.embedding if reused else None))
        self.load=stack.enter_context(patch(MODULE+'load_embedding',return_value=self.embedding))
        stack.enter_context(patch(MODULE+'create_view'))
        client=Mock();client.query.return_value.result.return_value=[self.stats]
        return client

    def test_first_run_calls_model_once_and_cloud_rerun_calls_it_zero_times(self):
        client=self.pipeline(False);embedder=Mock(return_value=self.vector)
        report=embed_banner(client,Mock(),embedder,self.sku,self.asset_id,self.config)
        self.assertEqual(report['model_calls'],1);embedder.assert_called_once()
        self.assertEqual(report['retrieval_status'],'GENERATED_POC_VISUAL_REFERENCE')
        patch.stopall()
        client=self.pipeline(True);embedder=Mock(side_effect=AssertionError('No repeated model call'))
        report=embed_banner(client,Mock(),embedder,self.sku,self.asset_id,self.config)
        self.assertEqual(report['model_calls'],0);self.assertTrue(report['cloud_reused'])
        self.load.assert_not_called()

    def test_mid_call_eligibility_revocation_aborts_before_write(self):
        client=self.pipeline(False);self.reader.side_effect=[self.asset,ValueError('approval revoked')]
        with self.assertRaises(ValueError):embed_banner(client,Mock(),Mock(return_value=self.vector),self.sku,self.asset_id,self.config)
        self.load.assert_not_called()

    def retrieval_row(self):
        return dict(self.asset,embedding_id=self.embedding['embedding_id'],vector=self.vector,
            model_id=self.config['image_model'],model_location=self.config['location'],dimension=512,task_type='IMAGE',
            embedding_created_at=self.embedding['created_at'],retrieval_status='GENERATED_POC_VISUAL_REFERENCE',
            review_id='REV-test-approved',review_revision=2,review_status='APPROVED_FOR_POC',
            review_approval_scope='POC_VISUAL_REUSE_ONLY',
            usage_scope='NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE')

    def test_retrieval_returns_verified_png_and_scoped_evidence_without_vector_exports(self):
        client=Mock();client.query.return_value.result.return_value=[self.retrieval_row()]
        gcs,blob=self.gcs()
        packet,raw=retrieve_generated_banner(client,gcs,self.sku,self.asset_id,self.config)
        self.assertEqual(raw,self.raw);self.assertTrue(all(packet['verification'].values()))
        self.assertEqual(packet['source_of_truth'],'BIGQUERY');self.assertNotIn('vector',packet)
        self.assertEqual(packet['embedding']['dimension'],512);self.assertEqual(client.query.call_count,2)
        self.assertFalse(client.query.call_args.kwargs['job_config'].use_query_cache)

    def test_capture_binds_both_native_eligibility_reads(self):
        first=Mock(job_id='bq-generated-before',project='project-661d501a-e709-4276-a26',
            location='asia-south1');first.result.return_value=[self.retrieval_row()]
        second=Mock(job_id='bq-generated-after',project='project-661d501a-e709-4276-a26',
            location='asia-south1');second.result.return_value=[self.retrieval_row()]
        client=Mock();client.query.side_effect=[first,second];gcs,_=self.gcs()
        captured=capture_generated_banner(client,gcs,self.sku,self.asset_id,self.config)
        self.assertEqual(captured.query_job_ids,('bq-generated-before','bq-generated-after'))
        self.assertTrue(captured.source_revision.startswith('bq2-'))
        self.assertEqual(captured.raw,self.raw)
        self.assertIsNotNone(captured.observed_at.utcoffset())

    def test_capture_rejects_untrusted_native_job_identity(self):
        for field,value in (('job_id','bad job'),('project','other'),('location','us-central1')):
            jobs=[]
            for name in ('before','after'):
                job=Mock(job_id=f'bq-{name}',project='project-661d501a-e709-4276-a26',location='asia-south1')
                job.result.return_value=[self.retrieval_row()];jobs.append(job)
            setattr(jobs[1],field,value)
            client=Mock();client.query.side_effect=jobs;gcs,_=self.gcs()
            with self.subTest(field=field),self.assertRaises(ValueError):
                capture_generated_banner(client,gcs,self.sku,self.asset_id,self.config)

    def test_retrieval_cloud_errors_and_post_download_revocation_do_not_fall_back(self):
        client=Mock();client.query.side_effect=RuntimeError('BigQuery unavailable')
        with (patch('pathlib.Path.read_text',side_effect=AssertionError('No local JSON')),
              patch('pathlib.Path.read_bytes',side_effect=AssertionError('No local images'))):
            with self.assertRaises(RuntimeError):retrieve_generated_banner(client,Mock(),self.sku,self.asset_id,self.config)
        client.query.side_effect=[Mock(result=Mock(return_value=[self.retrieval_row()])),Mock(result=Mock(return_value=[]))]
        gcs,_=self.gcs()
        with self.assertRaises(ValueError):retrieve_generated_banner(client,gcs,self.sku,self.asset_id,self.config)

    def test_generated_views_have_current_authority_gates_and_leave_historical_views_separate(self):
        sql=ASSET_SQL.read_text()
        for gate in ('poc_approved_product_claims','brand_guidelines','active_knowledge_chunks',
                     'original.gcs_generation=A.source_image_generation','policies.policy_count=rules.rule_count',
                     "A.status='REGISTERED_POC_DRAFT'","A.real_business_approved=FALSE"):
            self.assertIn(gate,sql)
        sql=embedding_view_sql(self.config)
        self.assertIn('E.gcs_generation=A.gcs_generation',sql)
        self.assertIn('E.content_sha256=A.sha256',sql);self.assertIn('E.campaign_id IS NULL',sql)
        self.assertIn('ARRAY_LENGTH(E.vector)=E.dimension',sql)


if __name__ == '__main__':
    unittest.main()
