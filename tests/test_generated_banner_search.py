import json
import math
import unittest
from unittest.mock import Mock, patch

from src.retrieval.generated_banner_search import (
    CONFIG, SQL, MultimodalTextQueryEmbedder, capture_generated_banner_search, embedding_config,
    read_ranked_banners, search_generated_banners, validate_config, validate_query, verify_search_queries,
)
from tests import test_generated_banner_embeddings

MODULE = 'src.retrieval.generated_banner_search.'


class GeneratedBannerSearchTests(unittest.TestCase):
    def setUp(self):
        fixture = test_generated_banner_embeddings.GeneratedBannerEmbeddingTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.config = json.loads(CONFIG.read_text())
        self.sku = fixture.sku
        self.text = 'Minimal skincare banner, off-white background, product on right'
        self.row = fixture.retrieval_row()
        self.selection = dict(asset_id=self.row['asset_id'],sku=self.sku,
            review_id=self.row['review_id'],review_revision=self.row['review_revision'],
            embedding_id=self.row['embedding_id'],gcs_uri=self.row['gcs_uri'],
            gcs_generation=self.row['gcs_generation'],sha256=self.row['sha256'],
            knowledge_snapshot_id=self.row['knowledge_snapshot_id'],distance=0.3)

    def reader(self, rows):
        client = Mock();client.query.return_value.result.return_value=rows
        return client

    def embedder(self):
        embedder = MultimodalTextQueryEmbedder.__new__(MultimodalTextQueryEmbedder)
        embedder.config=self.config;embedder.last_request=0;embedder.model_calls=0;embedder.session=Mock()
        embedder.session.post.return_value.json.return_value={'predictions':[{'textEmbedding':[0.1]*512}]}
        return embedder

    def packet(self):
        client = self.reader([self.row]);gcs,_ = self.fixture.gcs()
        return __import__('src.retrieval.generated_banner',fromlist=['retrieve_generated_banner']).retrieve_generated_banner(
            client,gcs,self.sku,self.row['asset_id'],embedding_config(self.config))[0]

    def test_config_profile_query_limits_and_canonical_identity(self):
        validate_config(self.config)
        self.assertEqual(embedding_config(self.config),self.fixture.config)
        self.assertEqual(validate_query(self.sku,'  '+self.text+'  ',self.config),self.text)
        for key,value in (('image_dimension',768),('image_model','gemini-embedding-001'),('top_k',True),
                          ('top_k',0),('top_k',21),('max_query_characters',129),('max_query_words',21),
                          ('version','other'),('embedding_version','other'),('extra',1)):
            with self.subTest(key=key),self.assertRaises(ValueError):validate_config({**self.config,key:value})
        for text in ('','   ','x'*129,' '.join(['x']*21),'query\nextra','query\x7f','crème'):
            with self.subTest(text=text),self.assertRaises(ValueError):validate_query(self.sku,text,self.config)
        with self.assertRaises(ValueError):validate_query('CI-SKU-0099',self.text,self.config)

    def test_query_is_text_in_the_image_model_space_not_the_document_space(self):
        embedder=self.embedder()
        vector=embedder(self.text)
        self.assertEqual(len(vector),512);self.assertEqual(embedder.model_calls,1)
        args=embedder.session.post.call_args
        self.assertIn('asia-south1',args.args[0]);self.assertIn('multimodalembedding@001:predict',args.args[0])
        self.assertEqual(args.kwargs['json'],{'instances':[{'text':self.text}],'parameters':{'dimension':512}})
        self.assertNotIn('gemini-embedding-001',args.args[0])
        embedder.close();embedder.session.close.assert_called_once()

    def test_invalid_query_and_invalid_model_response_are_rejected(self):
        embedder=self.embedder()
        with self.assertRaises(ValueError):embedder('x'*129)
        embedder.session.post.assert_not_called()
        for predictions in ([],[{},{}],[{'textEmbedding':[0]*512}],
                            [{'textEmbedding':[float('nan')]*512}],[{'textEmbedding':[1]*768}]):
            embedder.session.post.return_value.json.return_value={'predictions':predictions}
            with self.assertRaises(ValueError):embedder(self.text)

    def test_transient_requests_retry_and_permissions_fail_immediately(self):
        embedder=self.embedder();response=embedder.session.post.return_value
        error=RuntimeError('rate limit');error.response=Mock(status_code=429)
        response.raise_for_status.side_effect=[error,None]
        with patch(MODULE+'time.sleep'):embedder(self.text)
        self.assertEqual(embedder.model_calls,2)
        embedder=self.embedder();error=RuntimeError('permission denied');error.response=Mock(status_code=403)
        embedder.session.post.return_value.raise_for_status.side_effect=error
        with self.assertRaises(RuntimeError):embedder(self.text)
        self.assertEqual(embedder.model_calls,1)

    def test_ranking_sorts_distance_then_asset_identity_and_parameters_are_bound(self):
        second={**self.selection,'asset_id':f'SYN-GEN-{self.sku}-'+('1'*64),'distance':0.1}
        third={**self.selection,'asset_id':f'SYN-GEN-{self.sku}-'+('2'*64),'distance':0.1}
        client=self.reader([self.selection,third,second])
        ranked=read_ranked_banners(client,self.sku,[0.1]*512,self.config)
        self.assertEqual([row['asset_id'] for row in ranked],[second['asset_id'],third['asset_id'],self.selection['asset_id']])
        args=client.query.call_args;params={p.name:p for p in args.kwargs['job_config'].query_parameters}
        self.assertEqual(params['sku'].value,self.sku);self.assertEqual(params['top_k'].value,3)
        self.assertEqual(len(params['query_vector'].values),512)
        self.assertEqual(args.kwargs['location'],'asia-south1')
        self.assertEqual(args.kwargs['job_config'].maximum_bytes_billed,134217728)
        self.assertFalse(args.kwargs['job_config'].use_query_cache)

    def test_duplicate_cross_sku_stale_snapshot_invalid_distance_and_limit_fail_closed(self):
        invalid = [[self.selection,self.selection],
            [{**self.selection,'sku':'CI-SKU-0002'}],
            [{**self.selection,'knowledge_snapshot_id':''}],
            [self.selection,{**self.selection,'asset_id':f'SYN-GEN-{self.sku}-'+('1'*64),'knowledge_snapshot_id':'different'}],
            [{**self.selection,'asset_id':f'SYN-GEN-CI-SKU-0002-'+('1'*64)}]]
        invalid += [[{**self.selection,'distance':value}] for value in (True,float('nan'),float('inf'),-1,3,'0.2')]
        for rows in invalid:
            with self.subTest(rows=rows),self.assertRaises(ValueError):
                read_ranked_banners(self.reader(rows),self.sku,[0.1]*512,self.config)
        with self.assertRaises(ValueError):
            read_ranked_banners(self.reader([self.selection]*4),self.sku,[0.1]*512,self.config)
        client=Mock()
        with self.assertRaises(ValueError):read_ranked_banners(client,self.sku,[1]*768,self.config)
        client.query.assert_not_called()

    def test_search_returns_verified_original_bytes_evidence_and_no_vectors_or_cloud_writes(self):
        client=Mock()
        client.query.side_effect=[Mock(result=Mock(return_value=[self.selection])),
            Mock(result=Mock(return_value=[self.row])),Mock(result=Mock(return_value=[self.row])),
            Mock(result=Mock(return_value=[self.selection]))]
        gcs,blob=self.fixture.gcs();embed=Mock(return_value=[0.1]*512)
        packet,images=search_generated_banners(client,gcs,embed,self.sku,self.text,self.config)
        embed.assert_called_once_with(self.text)
        self.assertEqual(len(images),1);self.assertEqual(images[0][1],self.fixture.raw)
        self.assertEqual(packet['results'][0]['rank'],1);self.assertEqual(packet['results'][0]['distance'],0.3)
        self.assertTrue(all(packet['results'][0]['verification'].values()))
        self.assertEqual(packet['source_of_truth'],'BIGQUERY');self.assertEqual(packet['query_embedding']['dimension'],512)
        self.assertTrue(packet['query_embedding']['ephemeral'])
        self.assertNotIn('vector',packet['query_embedding']);self.assertNotIn('vector',packet['results'][0])
        ref='project-661d501a-e709-4276-a26.ci_brand_memory.image_embeddings#embedding_id='+self.selection['embedding_id']
        self.assertIn(ref,packet['results'][0]['evidence_references'])
        blob.download_as_bytes.assert_called_once_with(if_generation_match=int(self.row['gcs_generation']))
        client.load_table_from_json.assert_not_called();client.create_table.assert_not_called()
        blob.upload_from_string.assert_not_called()

    def test_capture_binds_rank_recheck_exact_reads_and_original_bytes(self):
        def job(identifier,rows):
            value=Mock(job_id=identifier,project='project-661d501a-e709-4276-a26',location='asia-south1')
            value.result.return_value=rows
            return value
        client=Mock();client.query.side_effect=[job('rank-before',[self.selection]),
            job('asset-before',[self.row]),job('asset-after',[self.row]),job('rank-after',[self.selection])]
        gcs,_=self.fixture.gcs();embed=Mock(return_value=[0.1]*512)
        captured=capture_generated_banner_search(client,gcs,embed,self.sku,self.text,self.config)
        self.assertEqual(captured.ranking_job_ids,('rank-before','rank-after'))
        self.assertEqual(captured.banner_reads[0].query_job_ids,('asset-before','asset-after'))
        self.assertEqual(captured.images[0][1],self.fixture.raw)
        self.assertTrue(captured.source_revision.startswith('bqset-'))
        self.assertEqual(captured.packet['results'][0]['rank'],1)

    def test_capture_rejects_untrusted_ranking_job_identity(self):
        def job(identifier,rows,project='project-661d501a-e709-4276-a26'):
            value=Mock(job_id=identifier,project=project,location='asia-south1');value.result.return_value=rows
            return value
        client=Mock();client.query.side_effect=[job('rank-before',[self.selection]),
            job('asset-before',[self.row]),job('asset-after',[self.row]),
            job('rank-after',[self.selection],project='other')]
        gcs,_=self.fixture.gcs()
        with self.assertRaises(ValueError):
            capture_generated_banner_search(client,gcs,Mock(return_value=[0.1]*512),
                self.sku,self.text,self.config)

    def test_empty_search_has_no_image_download_or_invented_evidence(self):
        client=self.reader([]);gcs=Mock();embed=Mock(return_value=[0.1]*512)
        packet,images=search_generated_banners(client,gcs,embed,self.sku,self.text,self.config)
        self.assertEqual(packet['results'],[]);self.assertEqual(images,[]);self.assertIsNone(packet['snapshot_id'])
        gcs.bucket.assert_not_called();embed.assert_called_once()

    def test_sql_verifier_reuses_production_filters_and_top_k_without_writes(self):
        rows=[dict(asset_id=name,distance=distance) for name,distance in (
            ('00-same-a',0.0),('01-same-b',0.0),('02-near',1-1/math.sqrt(2)),
            ('03-orthogonal',1.0),('04-opposite',2.0))]
        client=Mock();client.query.side_effect=[Mock(result=Mock(return_value=rows)),
                                               Mock(result=Mock(return_value=rows[:2]))]
        result=verify_search_queries(client,self.sku,self.config)
        self.assertTrue(result['cosine_distances_and_ties_verified']);self.assertEqual(result['excluded_vectors'],12)
        self.assertTrue(result['top_k_verified']);client.create_table.assert_not_called()
        for call in client.query.call_args_list:
            self.assertIn('FROM fixture',call.args[0]);self.assertIn('COSINE_DISTANCE',call.args[0])
            self.assertNotIn('INSERT',call.args[0]);self.assertNotIn('CREATE',call.args[0])

    def test_sql_verifier_rejects_incorrect_distances_filter_leaks_and_result_limits(self):
        rows=[dict(asset_id=name,distance=distance) for name,distance in (
            ('00-same-a',0.0),('01-same-b',0.0),('02-near',1-1/math.sqrt(2)),
            ('03-orthogonal',1.0),('04-opposite',2.0))]
        for bad in (rows[::-1],rows+[dict(asset_id='rejected',distance=0)],
                    [{**rows[0],'distance':1.0}]+rows[1:]):
            with self.assertRaises(ValueError):verify_search_queries(self.reader(bad),self.sku,self.config)
        client=Mock();client.query.side_effect=[Mock(result=Mock(return_value=rows)),
                                               Mock(result=Mock(return_value=rows))]
        with self.assertRaisesRegex(ValueError,'top_k'):verify_search_queries(client,self.sku,self.config)

    def test_revoked_eligibility_and_ranking_identity_change_abort_whole_search(self):
        # Same reader as exact retrieval; revocation must propagate before any export.
        client=Mock();client.query.side_effect=[Mock(result=Mock(return_value=[self.selection])),
                                               Mock(result=Mock(return_value=[]))]
        with self.assertRaises(ValueError):
            search_generated_banners(client,Mock(),Mock(return_value=[0.1]*512),self.sku,self.text,self.config)
        packet=self.packet()
        with (patch(MODULE+'read_ranked_banners',return_value=[self.selection]),
              patch(MODULE+'retrieve_generated_banner',return_value=({**packet,'gcs_generation':'99'},self.fixture.raw))):
            with self.assertRaisesRegex(ValueError,'identity changed'):
                search_generated_banners(Mock(),Mock(),Mock(return_value=[0.1]*512),self.sku,self.text,self.config)
        with (patch(MODULE+'read_ranked_banners',side_effect=[[self.selection],[]]),
              patch(MODULE+'retrieve_generated_banner',return_value=(packet,self.fixture.raw))):
            with self.assertRaisesRegex(ValueError,'changed during search'):
                search_generated_banners(Mock(),Mock(),Mock(return_value=[0.1]*512),self.sku,self.text,self.config)

    def test_model_bigquery_and_gcs_errors_propagate_without_local_fallback(self):
        client=Mock();embed=Mock(side_effect=RuntimeError('model unavailable'))
        with self.assertRaises(RuntimeError):search_generated_banners(client,Mock(),embed,self.sku,self.text,self.config)
        client.query.assert_not_called()
        client.query.side_effect=RuntimeError('BigQuery unavailable')
        # Reading SQL/config is allowed; banner and vector review artifacts are not.
        with patch('pathlib.Path.read_bytes',side_effect=AssertionError('No local image fallback')):
            with self.assertRaises(RuntimeError):
                search_generated_banners(client,Mock(),Mock(return_value=[0.1]*512),self.sku,self.text,self.config)
            with (patch(MODULE+'read_ranked_banners',return_value=[self.selection]),
                  patch(MODULE+'retrieve_generated_banner',side_effect=RuntimeError('GCS unavailable'))):
                with self.assertRaises(RuntimeError):
                    search_generated_banners(Mock(),Mock(),Mock(return_value=[0.1]*512),self.sku,self.text,self.config)


if __name__=='__main__':
    unittest.main()
