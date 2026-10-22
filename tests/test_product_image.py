import hashlib
import io
import unittest
from unittest.mock import Mock, patch

from PIL import Image

from src.generators.sales_generator import PROJECT
from src.retrieval.product_image import (
    BUCKET, SQL, capture_product_image, retrieve_product_image, validate_metadata, verify_image_bytes,
)


class ProductImageTests(unittest.TestCase):
    def setUp(self):
        self.sku='CI-SKU-0001'
        output=io.BytesIO();Image.new('RGB',(30,40),'white').save(output,format='JPEG')
        self.raw=output.getvalue()
        self.row=dict(sku=self.sku,brand='Source brand',product_name='Source product',
            asset_id=f'AST-{self.sku}-PRIMARY',asset_type='IMAGE',asset_role='PRIMARY_PRODUCT_IMAGE',
            is_primary=True,gcs_uri=f'gs://{BUCKET}/products/{self.sku}/primary.jpg',mime_type='image/jpeg',
            width=30,height=40,source_system='BEAUTYFEEDS',source_url='https://example.com/product.jpg',
            asset_description='Imported primary image',embedding_id='EMB-test',gcs_generation='123',
            content_sha256=hashlib.sha256(self.raw).hexdigest(),model_id='multimodalembedding@001',
            model_location='asia-south1',dimension=512,task_type='IMAGE')
        self.client=Mock();self.client.query.return_value.result.return_value=[self.row]
        self.client.query.return_value.job_id='bq-product-image-read-1'
        self.client.query.return_value.project=PROJECT
        self.client.query.return_value.location='asia-south1'
        self.gcs=Mock();self.blob=self.gcs.bucket.return_value.blob.return_value
        self.blob.generation=123;self.blob.content_type='image/jpeg'
        self.blob.download_as_bytes.return_value=self.raw

    def test_original_bytes_and_bigquery_provenance(self):
        metadata,raw=retrieve_product_image(self.client,self.gcs,self.sku)
        self.assertEqual(raw,self.raw)
        self.assertEqual(metadata['source_system'],'BEAUTYFEEDS')
        self.assertEqual(metadata['embedding_id'],'EMB-test')
        self.assertTrue(all(metadata['verification'].values()))
        self.assertEqual(metadata['byte_count'],len(self.raw))
        self.assertEqual(self.client.query.call_args.kwargs['job_config'].maximum_bytes_billed,96*1024*1024)
        self.blob.download_as_bytes.assert_called_once_with(if_generation_match=123)
        self.gcs.bucket.assert_called_once_with(BUCKET)
        self.gcs.bucket.return_value.blob.assert_called_once_with(f'products/{self.sku}/primary.jpg')
        self.blob.upload_from_string.assert_not_called()

    def test_parameterized_read_only_metadata_query(self):
        retrieve_product_image(self.client,self.gcs,self.sku,80_000_000)
        arguments=self.client.query.call_args
        config=arguments.kwargs['job_config']
        self.assertEqual(config.maximum_bytes_billed,80_000_000)
        self.assertFalse(config.use_query_cache)
        self.assertEqual(config.query_parameters[0].value,self.sku)
        self.assertEqual(arguments.kwargs['location'],'asia-south1')
        self.assertIn('poc_image_embeddings',arguments.args[0])
        for mutation in ('INSERT','UPDATE','DELETE','MERGE','CREATE','DROP'):
            self.assertNotIn(mutation,arguments.args[0])
        self.client.create_table.assert_not_called()

    def test_capture_binds_native_job_and_verified_bytes(self):
        captured=capture_product_image(self.client,self.gcs,self.sku)
        self.assertEqual(captured.query_job_id,'bq-product-image-read-1')
        self.assertEqual((captured.project,captured.location),(PROJECT,'asia-south1'))
        self.assertEqual(captured.raw,self.raw)
        self.assertIsNotNone(captured.observed_at.utcoffset())

    def test_capture_rejects_wrong_native_job_identity_after_integrity_checks(self):
        for field,value in (('job_id','bad job'),('project','other'),('location','us-central1')):
            setattr(self.client.query.return_value,field,value)
            with self.subTest(field=field),self.assertRaises(ValueError):
                capture_product_image(self.client,self.gcs,self.sku)
            setattr(self.client.query.return_value,field,
                'bq-product-image-read-1' if field=='job_id' else PROJECT if field=='project' else 'asia-south1')

    def test_invalid_input_stops_before_cloud_access(self):
        for sku in ('OTHER','CI-SKU-0021',None,[]):
            with self.assertRaises(ValueError):retrieve_product_image(self.client,self.gcs,sku)
        for cap in (0,-1,True,'unlimited'):
            with self.assertRaises(ValueError):retrieve_product_image(self.client,self.gcs,self.sku,cap)
        self.client.query.assert_not_called();self.gcs.bucket.assert_not_called()

    def test_invalid_scope_identity_generation_and_provenance(self):
        for key,value in (('sku','CI-SKU-0002'),('asset_id','other'),('source_system','SYNTHETIC_POC'),
                          ('asset_role','CAMPAIGN_BANNER'),('is_primary',False),('gcs_uri','gs://other/image.jpg'),
                          ('gcs_generation','0'),('gcs_generation','bad'),('content_sha256','bad'),
                          ('model_location','global'),('model_id','other'),('dimension',768),('width',0)):
            with self.subTest(field=key),self.assertRaises(ValueError):
                validate_metadata(self.sku,{**self.row,key:value})

    def test_missing_or_duplicate_metadata_rejected(self):
        for rows in ([],[self.row,self.row]):
            self.client.query.return_value.result.return_value=rows
            with self.assertRaises(ValueError):retrieve_product_image(self.client,self.gcs,self.sku)
        self.gcs.bucket.assert_not_called()

    def test_changed_generation_and_mime_fail_before_download(self):
        self.blob.generation=124
        with self.assertRaises(ValueError):retrieve_product_image(self.client,self.gcs,self.sku)
        self.blob.generation=123;self.blob.content_type='image/png'
        with self.assertRaises(ValueError):retrieve_product_image(self.client,self.gcs,self.sku)
        self.blob.download_as_bytes.assert_not_called()

    def test_hash_format_and_dimensions_checked(self):
        with self.assertRaises(ValueError):verify_image_bytes(self.row,b'changed')
        with self.assertRaises(ValueError):verify_image_bytes({**self.row,'width':31},self.raw)
        output=io.BytesIO();Image.new('RGB',(30,40)).save(output,format='PNG');png=output.getvalue()
        with self.assertRaises(ValueError):
            verify_image_bytes({**self.row,'content_sha256':hashlib.sha256(png).hexdigest()},png)

    def test_cloud_failures_do_not_fall_back_to_local_image_or_json(self):
        sql=SQL.read_text()
        def read_sql_only(path,*args,**kwargs):
            self.assertEqual(path,SQL)
            return sql
        with patch('pathlib.Path.read_text',new=read_sql_only),patch('pathlib.Path.read_bytes',
                side_effect=AssertionError('Do not read local image copies')):
            self.client.query.side_effect=RuntimeError('BigQuery unavailable')
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_product_image(self.client,self.gcs,self.sku)
            self.gcs.bucket.assert_not_called()
            self.client.query.side_effect=None;self.blob.download_as_bytes.side_effect=RuntimeError('GCS unavailable')
            with self.assertRaisesRegex(RuntimeError,'GCS unavailable'):
                retrieve_product_image(self.client,self.gcs,self.sku)


if __name__=='__main__':unittest.main()
