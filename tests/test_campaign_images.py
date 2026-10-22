import copy
import hashlib
import io
import unittest
from datetime import date
from unittest.mock import Mock, patch

from PIL import Image

from src.generators.sales_generator import PROJECT
from src.retrieval.campaign_images import (
    BUCKET, SQL, capture_campaign_images, retrieve_campaign_images, validate_metadata, verify_image_bytes,
)
from src.retrieval.request import RetrievalRequest


class CampaignImageTests(unittest.TestCase):
    def setUp(self):
        self.sku = 'CI-SKU-0001'
        self.request = RetrievalRequest(self.sku,'CONVERSION','Skincare shoppers','PAID_SOCIAL','NONE')
        self.rows = [];self.creatives = [];self.raw = {};self.blobs = {}
        for index,color in enumerate(('white','tan'),1):
            image = io.BytesIO();Image.new('RGB',(120,63),color).save(image,format='PNG')
            raw = image.getvalue();digest = hashlib.sha256(raw).hexdigest()
            campaign = f'SYN-CMP-20250101-{self.sku}-{index:02d}'
            asset = f'SYN-AST-{campaign}-template-v1'
            campaign_ref = f'{PROJECT}.ci_creative.campaign_master#campaign_id={campaign}'
            asset_ref = f'{PROJECT}.ci_creative.campaign_assets#asset_id={asset}'
            uri = f'gs://{BUCKET}/campaigns/{campaign}/template-v1/{digest}.png'
            product_uri = f'gs://{BUCKET}/products/{self.sku}/primary.jpg'
            row = dict(sku=self.sku,brand='Source brand',product_name='Source product',
                snapshot_id='current-snapshot',chunk_id=f'KCH-{index}',asset_id=asset,campaign_id=campaign,
                asset_type='IMAGE',asset_role='CAMPAIGN_BANNER',gcs_uri=uri,source_image_uri=product_uri,
                mime_type='image/png',banner_sha256=digest,width=120,height=63,
                headline='Source product',body_copy='Neutral copy',call_to_action='View product',
                visual_description='Synthetic template',generator_version='template-v1',
                source_system='SYNTHETIC_POC',objective='AWARENESS',channel='PAID_SOCIAL',audience='Source audience',
                start_date=date(2025,2,1),end_date=date(2025,2,21),
                creative_content_sha256=f'creative-hash-{index}',evidence_references=[campaign_ref,asset_ref],
                image_uris=[uri,product_uri],retrieval_status='POC_CONTEXT',approval_scope='SYNTHETIC_POC_ONLY',
                claim_id=None,embedding_id=f'EMB-{index}',content_sha256=digest,gcs_generation=str(100+index),
                model_id='multimodalembedding@001',model_location='asia-south1',dimension=512,task_type='IMAGE')
            creative = dict(row,rank=index,distance=0.2*index,objective_match=False,channel_match=True,
                            content_sha256=row['creative_content_sha256'])
            self.rows.append(row);self.creatives.append(creative);self.raw[asset] = raw
            blob = Mock(generation=100+index,content_type='image/png')
            blob.download_as_bytes.return_value = raw
            self.blobs[uri.removeprefix(f'gs://{BUCKET}/')] = blob
        self.packet = dict(request=self.request.to_dict(),creatives=self.creatives,snapshot_id='current-snapshot',
            source_of_truth='BIGQUERY',source_system='SYNTHETIC_POC',approval_scope='SYNTHETIC_POC_ONLY')
        self.bq = Mock();self.bq.query.return_value.result.return_value = list(reversed(self.rows))
        self.bq.query.return_value.job_id='bq-campaign-images-read-1'
        self.bq.query.return_value.project=PROJECT
        self.bq.query.return_value.location='asia-south1'
        self.gcs = Mock();self.gcs.bucket.return_value.blob.side_effect = self.blobs.__getitem__

    def test_original_bytes_follow_ranking_with_pinned_generations_and_evidence(self):
        images = retrieve_campaign_images(self.bq,self.gcs,self.packet)
        self.assertEqual([metadata['rank'] for metadata,_ in images],[1,2])
        for index,(metadata,raw) in enumerate(images,1):
            self.assertEqual(raw,self.raw[metadata['asset_id']])
            self.assertEqual(metadata['byte_count'],len(raw))
            self.assertEqual(metadata['source_system'],'SYNTHETIC_POC')
            self.assertEqual(metadata['source_of_truth'],'BIGQUERY')
            self.assertTrue(all(metadata['verification'].values()))
            self.assertIn(f'{PROJECT}.ci_brand_memory.image_embeddings#embedding_id=EMB-{index}',
                          metadata['evidence_references'])
            blob = self.blobs[metadata['gcs_uri'].removeprefix(f'gs://{BUCKET}/')]
            blob.download_as_bytes.assert_called_once_with(if_generation_match=100+index)
            blob.upload_from_string.assert_not_called()
        self.assertNotIn('verification',self.rows[0])

    def test_parameterized_current_views_and_read_only_query(self):
        retrieve_campaign_images(self.bq,self.gcs,self.packet,150_000_000)
        call = self.bq.query.call_args;job = call.kwargs['job_config']
        params = {p.name:p for p in job.query_parameters}
        self.assertEqual(params['sku'].value,self.sku)
        self.assertEqual(params['snapshot'].value,'current-snapshot')
        self.assertEqual(params['assets'].values,[c['asset_id'] for c in self.creatives])
        self.assertEqual(job.maximum_bytes_billed,150_000_000)
        self.assertFalse(job.use_query_cache)
        self.assertEqual(call.kwargs['location'],'asia-south1')
        self.assertIn('poc_image_embeddings',call.args[0])
        self.assertIn('poc_generation_text_embeddings',call.args[0])
        for mutation in ('INSERT','UPDATE','DELETE','MERGE','CREATE','DROP'):
            self.assertNotIn(mutation,call.args[0])
        self.bq.create_table.assert_not_called();self.bq.load_table_from_json.assert_not_called()

    def test_capture_binds_one_native_job_to_complete_verified_batch(self):
        captured=capture_campaign_images(self.bq,self.gcs,self.packet)
        self.assertEqual(captured.query_job_id,'bq-campaign-images-read-1')
        self.assertEqual((captured.project,captured.location),(PROJECT,'asia-south1'))
        self.assertEqual(len(captured.images),2)
        self.assertIsNotNone(captured.observed_at.utcoffset())

    def test_capture_requires_nonempty_selection_and_exact_native_identity(self):
        with self.assertRaises(ValueError):
            capture_campaign_images(self.bq,self.gcs,{**self.packet,'creatives':[],'snapshot_id':None})
        self.bq.query.assert_not_called()
        for field,value in (('job_id','bad job'),('project','other'),('location','us-central1')):
            setattr(self.bq.query.return_value,field,value)
            with self.subTest(field=field),self.assertRaises(ValueError):
                capture_campaign_images(self.bq,self.gcs,self.packet)
            setattr(self.bq.query.return_value,field,
                'bq-campaign-images-read-1' if field=='job_id' else PROJECT if field=='project' else 'asia-south1')

    def test_changed_creative_snapshot_copy_dates_hash_or_image_rejected_before_any_download(self):
        for key,value in (('snapshot_id','old'),('chunk_id','other'),('creative_content_sha256','changed'),
                          ('headline','Changed name'),('gcs_uri','gs://other/image.png'),
                          ('source_image_uri','gs://other/product.jpg'),('start_date',date(2025,1,1))):
            with self.subTest(field=key):
                self.bq.query.return_value.result.return_value = [self.rows[0],{**self.rows[1],key:value}]
                with self.assertRaises(ValueError):retrieve_campaign_images(self.bq,self.gcs,self.packet)
        self.gcs.bucket.assert_not_called()

    def test_missing_duplicate_extra_or_ineligible_rows_rejected(self):
        for rows in ([],[self.rows[0]],[self.rows[0]]*2,
                     [*self.rows,{**self.rows[0],'asset_id':'unrelated'}]):
            self.bq.query.return_value.result.return_value = rows
            with self.assertRaises(ValueError):retrieve_campaign_images(self.bq,self.gcs,self.packet)
        self.gcs.bucket.assert_not_called()

    def test_invalid_identity_scope_model_generation_or_hash_rejected(self):
        for key,value in (('source_system','BEAUTYFEEDS'),('asset_role','PRIMARY_PRODUCT_IMAGE'),
                          ('asset_type','TEXT'),('mime_type','image/jpeg'),('dimension',768),
                          ('task_type','RETRIEVAL_DOCUMENT'),('model_id','other'),('model_location','global'),
                          ('gcs_generation','0'),('gcs_generation','bad'),('content_sha256','bad'),
                          ('generator_version','other'),('width',True),('height',0),
                          ('retrieval_status','REVIEW_ONLY'),('approval_scope','APPROVED'),
                          ('claim_id','claim'),('evidence_references',[]),('image_uris',[])):
            with self.subTest(field=key),self.assertRaises(ValueError):
                validate_metadata(self.sku,'current-snapshot',self.creatives[0],{**self.rows[0],key:value})

    def test_generation_and_mime_changes_stop_before_object_download(self):
        first = self.blobs[self.rows[0]['gcs_uri'].removeprefix(f'gs://{BUCKET}/')]
        first.generation = 999
        with self.assertRaisesRegex(ValueError,'generation or MIME'):
            retrieve_campaign_images(self.bq,self.gcs,self.packet)
        first.generation = 101;first.content_type = 'image/jpeg'
        with self.assertRaisesRegex(ValueError,'generation or MIME'):
            retrieve_campaign_images(self.bq,self.gcs,self.packet)
        first.download_as_bytes.assert_not_called()

    def test_hash_format_and_dimensions_are_verified_on_original_bytes(self):
        row = self.rows[0];raw = self.raw[row['asset_id']]
        with self.assertRaises(ValueError):verify_image_bytes(row,b'changed')
        with self.assertRaises(ValueError):verify_image_bytes({**row,'width':121},raw)
        jpeg = io.BytesIO();Image.new('RGB',(120,63)).save(jpeg,format='JPEG');raw_jpeg = jpeg.getvalue()
        with self.assertRaises(ValueError):
            verify_image_bytes({**row,'content_sha256':hashlib.sha256(raw_jpeg).hexdigest()},raw_jpeg)

    def test_gcs_precondition_failure_propagates_without_returning_partial_batch(self):
        second = self.blobs[self.rows[1]['gcs_uri'].removeprefix(f'gs://{BUCKET}/')]
        second.download_as_bytes.side_effect = RuntimeError('generation precondition failed')
        with self.assertRaisesRegex(RuntimeError,'precondition failed'):
            retrieve_campaign_images(self.bq,self.gcs,self.packet)
        second.download_as_bytes.assert_called_once_with(if_generation_match=102)

    def test_invalid_selection_or_billing_cap_stops_before_cloud_access(self):
        for cap in (0,-1,True,'unlimited'):
            with self.assertRaises(ValueError):retrieve_campaign_images(self.bq,self.gcs,self.packet,cap)
        for key,value in (('source_of_truth','LOCAL_JSON'),('source_system','BEAUTYFEEDS'),
                          ('snapshot_id','old'),('creatives',[self.creatives[0]]*2)):
            with self.assertRaises(ValueError):
                retrieve_campaign_images(self.bq,self.gcs,{**self.packet,key:value})
        bad = copy.deepcopy(self.packet);bad['creatives'][0]['sku'] = 'CI-SKU-0002'
        with self.assertRaises(ValueError):retrieve_campaign_images(self.bq,self.gcs,bad)
        bad = copy.deepcopy(self.packet);bad['creatives'][0]['rank'] = True
        with self.assertRaises(ValueError):retrieve_campaign_images(self.bq,self.gcs,bad)
        self.bq.query.assert_not_called();self.gcs.bucket.assert_not_called()

    def test_empty_ranking_has_no_cloud_downloads(self):
        images = retrieve_campaign_images(self.bq,self.gcs,{**self.packet,'creatives':[],'snapshot_id':None})
        self.assertEqual(images,[])
        self.bq.query.assert_not_called();self.gcs.bucket.assert_not_called()

    def test_cloud_failures_have_no_local_json_or_image_fallback(self):
        sql = SQL.read_text()
        def read_sql_only(path,*args,**kwargs):
            self.assertEqual(path,SQL)
            return sql
        with patch('pathlib.Path.read_text',new=read_sql_only),patch('pathlib.Path.read_bytes',
                side_effect=AssertionError('No local image serving fallback')):
            self.bq.query.side_effect = RuntimeError('BigQuery unavailable')
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_campaign_images(self.bq,self.gcs,self.packet)
            self.gcs.bucket.assert_not_called()
            self.bq.query.side_effect = None
            first = self.blobs[self.rows[0]['gcs_uri'].removeprefix(f'gs://{BUCKET}/')]
            first.reload.side_effect = RuntimeError('GCS unavailable')
            with self.assertRaisesRegex(RuntimeError,'GCS unavailable'):
                retrieve_campaign_images(self.bq,self.gcs,self.packet)


if __name__ == '__main__':
    unittest.main()
