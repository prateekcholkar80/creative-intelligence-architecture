import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from google.genai import types

from src.common.bq_merge import merge_statement
from src.generators.embeddings_generator import (
    ROOT, BUCKET, SCHEMA, embed_or_reuse, image_source, record_identity,
    text_source, validate_config, validate_vector, VertexEmbedder)


class EmbeddingTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT/'config/embeddings.json').read_text())
        self.chunk = dict(chunk_id='KCH-test',chunk_type='PRODUCT_KNOWLEDGE',brand='Actual brand',
            retrieval_status='FACTUAL_CONTEXT',sku='CI-SKU-0001',campaign_id=None,claim_id=None,
            approval_scope='SOURCE_PRODUCT_FACTS',source_system='BEAUTYFEEDS',
            source_reference='product_master#sku=CI-SKU-0001',chunk_text='Product identity only.',
            content_sha256=hashlib.sha256(b'Product identity only.').hexdigest())

    def test_vector_validation(self):
        self.assertEqual(validate_vector([1,0],2),[1.0,0.0])
        for vector in ([1],[0,0],[float('nan'),1],[float('inf'),1],[True,1],['1',1]):
            with self.assertRaises(ValueError): validate_vector(vector,2)

    def test_excludes_review_notes_and_daily_rows(self):
        self.assertEqual(text_source(self.chunk)['source_system'],'BEAUTYFEEDS')
        for kind in ('CLAIM_REVIEW_NOTE','DAILY_SALES','UNKNOWN'):
            self.chunk['chunk_type']=kind
            with self.assertRaises(ValueError):text_source(self.chunk)
        self.chunk['chunk_type']='POC_APPROVED_CLAIM'
        self.chunk['retrieval_status']='REVIEW_ONLY'
        with self.assertRaises(ValueError):text_source(self.chunk)

    def test_hash_and_sku_rejected(self):
        self.chunk['chunk_text']='Changed input'
        with self.assertRaises(ValueError):text_source(self.chunk)
        self.chunk['content_sha256']=hashlib.sha256(b'Changed input').hexdigest()
        self.chunk['sku']='OTHER-SKU'
        with self.assertRaises(ValueError):text_source(self.chunk)

    def test_visual_reference_is_unmapped_and_styling_only(self):
        self.chunk.update(chunk_type='WEBSITE_VISUAL_REFERENCE',brand='Naked & Thriving',
            retrieval_status='VISUAL_REFERENCE',sku=None,approval_scope='VISUAL_INSPIRATION_ONLY',
            source_system='OFFICIAL_BRAND_WEBSITE')
        self.assertIsNone(text_source(self.chunk)['sku'])
        self.chunk['sku']='CI-SKU-0001'
        with self.assertRaises(ValueError):text_source(self.chunk)

    def test_cache_identity_tracks_model_input_and_generation(self):
        source=text_source(self.chunk)
        original=record_identity(source,self.config)['embedding_id']
        for key,value in (('content_sha256','changed'),('source_system','SYNTHETIC_POC')):
            self.assertNotEqual(original,record_identity({**source,key:value},self.config)['embedding_id'])
        for key,value in (('text_dimension',1536),('text_model','other'),('text_task','RETRIEVAL_QUERY'),
                          ('location','other'),('version','v2')):
            self.assertNotEqual(original,record_identity(source,{**self.config,key:value})['embedding_id'])
        self.assertEqual(record_identity({**source,'snapshot_id':'irrelevant'},self.config)['embedding_id'],original)

    def test_local_and_cloud_reuse_without_model_calls(self):
        source=text_source(self.chunk); embed=Mock(return_value=[1.0]*768)
        with tempfile.TemporaryDirectory() as directory:
            row,called=embed_or_reuse(source,self.config,{},Path(directory),embed)
            self.assertTrue(called)
            self.assertEqual(set(row),{f.name for f in SCHEMA})
            self.assertEqual(embed_or_reuse(source,self.config,{},Path(directory),embed),(row,False))
            self.assertEqual(embed_or_reuse(source,self.config,{row['embedding_id']:row},Path(directory),embed),(row,False))
            self.assertEqual(embed.call_count,1)
            # Detect malformed checkpoints rather than silently paying for replacement vectors.
            path=Path(directory)/(row['embedding_id']+'.json')
            damaged=json.loads(path.read_text());damaged['vector']=[1]
            path.write_text(json.dumps(damaged))
            with self.assertRaises(ValueError):embed_or_reuse(source,self.config,{},Path(directory),embed)

    def test_resume_after_model_failure(self):
        source=text_source(self.chunk)
        with tempfile.TemporaryDirectory() as directory:
            embed=Mock(side_effect=RuntimeError('Transient interruption'))
            with self.assertRaises(RuntimeError):embed_or_reuse(source,self.config,{},Path(directory),embed)
            self.assertEqual(list(Path(directory).iterdir()),[])
            embed.side_effect=None;embed.return_value=[1.0]*768
            row,called=embed_or_reuse(source,self.config,{},Path(directory),embed)
            self.assertTrue(called)
            self.assertEqual(embed_or_reuse(source,self.config,{},Path(directory),embed),(row,False))
            self.assertEqual(embed.call_count,2)

    def test_bigquery_vector_takes_precedence_over_conflicting_local_checkpoint(self):
        source=text_source(self.chunk);embed=Mock(return_value=[1.0]*768)
        with tempfile.TemporaryDirectory() as directory:
            row,_=embed_or_reuse(source,self.config,{},Path(directory),embed)
            path=Path(directory)/(row['embedding_id']+'.json')
            local=json.loads(path.read_text());local['vector']=[2.0]*768
            path.write_text(json.dumps(local))
            embed.reset_mock()
            with patch.object(Path,'read_text',side_effect=AssertionError('Do not read local vectors')):
                reused,called=embed_or_reuse(source,self.config,{row['embedding_id']:row},Path(directory),embed)
            self.assertEqual(reused,row)
            self.assertFalse(called)
            embed.assert_not_called()
            # Invalid authoritative data must not silently fall back to the local copy.
            invalid={**row,'vector':[]}
            with self.assertRaises(ValueError):
                embed_or_reuse(source,self.config,{row['embedding_id']:invalid},Path(directory),embed)
            embed.assert_not_called()

    def test_image_hash_generation_and_real_provenance(self):
        asset=dict(asset_id='product-1',sku='CI-SKU-0001',source_system='BEAUTYFEEDS',campaign_id=None,
            gcs_uri=f'gs://{BUCKET}/products/CI-SKU-0001/primary.jpg',source_reference='product_assets#1',sha256=None)
        source=image_source(asset,123,b'original image')
        self.assertEqual(source['gcs_generation'],'123')
        self.assertEqual(source['source_system'],'BEAUTYFEEDS')
        first=record_identity(source,self.config)['embedding_id']
        self.assertNotEqual(first,record_identity(image_source(asset,124,b'original image'),self.config)['embedding_id'])
        asset['sha256']='wrong'
        with self.assertRaises(ValueError):image_source(asset,123,b'original image')
        asset['sha256']=None;asset['gcs_uri']='gs://other/image.jpg'
        with self.assertRaises(ValueError):image_source(asset,123,b'original image')

    def test_synthetic_campaign_hash_checked(self):
        asset=dict(asset_id='campaign-1',sku='CI-SKU-0001',source_system='SYNTHETIC_POC',campaign_id='CMP1',
            gcs_uri=f'gs://{BUCKET}/campaigns/CMP1/template-v1/hash.png',source_reference='campaign_assets#1',
            sha256=hashlib.sha256(b'banner').hexdigest())
        self.assertEqual(image_source(asset,1,b'banner')['source_system'],'SYNTHETIC_POC')
        with self.assertRaises(ValueError):image_source(asset,1,b'changed banner')

    def test_transient_retry_and_no_retry_for_permission_failure(self):
        embedder=VertexEmbedder.__new__(VertexEmbedder)
        embedder.config={**self.config,'request_interval_seconds':0};embedder.last_request=0
        embedder.text_client=Mock()
        error=RuntimeError('Permission failure');error.code=403
        embedder.text_client.models.embed_content.side_effect=error
        with self.assertRaises(RuntimeError):embedder(text_source(self.chunk))
        self.assertEqual(embedder.text_client.models.embed_content.call_count,1)
        transient=RuntimeError('Rate limit');transient.code=429
        response=types.EmbedContentResponse(embeddings=[types.ContentEmbedding(values=[1.0]*768,
            statistics=types.ContentEmbeddingStatistics(truncated=False,token_count=10))])
        embedder.text_client.models.embed_content.reset_mock()
        embedder.text_client.models.embed_content.side_effect=[transient,response]
        with patch('src.generators.embeddings_generator.time.sleep'):
            self.assertEqual(len(embedder(text_source(self.chunk))),768)
        self.assertEqual(embedder.text_client.models.embed_content.call_count,2)
        options=embedder.text_client.models.embed_content.call_args.kwargs['config']
        self.assertEqual(options.task_type,'RETRIEVAL_DOCUMENT')
        self.assertFalse(options.auto_truncate)

    def test_truncated_response_rejected(self):
        embedder=VertexEmbedder.__new__(VertexEmbedder)
        embedder.config={**self.config,'request_interval_seconds':0};embedder.last_request=0
        embedder.text_client=Mock()
        embedder.text_client.models.embed_content.return_value=types.EmbedContentResponse(
            embeddings=[types.ContentEmbedding(values=[1.0]*768,
                statistics=types.ContentEmbeddingStatistics(truncated=True,token_count=10))])
        with self.assertRaises(ValueError):embedder(text_source(self.chunk))
        self.assertEqual(embedder.text_client.models.embed_content.call_count,1)

    def test_configuration_and_upsert_boundaries(self):
        validate_config(self.config)
        for key,value in (('location','global'),('text_task','RETRIEVAL_QUERY'),('cache_directory','../outside')):
            with self.assertRaises(ValueError):validate_config({**self.config,key:value})
        sql=merge_statement('p.d.embeddings','p.d.stage',[f.name for f in SCHEMA],['embedding_id','source_system'])
        self.assertIn('T.source_system=S.source_system',sql)
        self.assertNotIn('DELETE',sql)
        self.assertNotIn('T.created_at=S.created_at',sql)


if __name__=='__main__':unittest.main()
