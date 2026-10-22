import io,json
from copy import deepcopy
from datetime import datetime,timezone
from unittest import TestCase
from unittest.mock import Mock,patch
from PIL import Image
from src.retrieval import evaluation_product_sources as sources

class EvaluationProductSourcesTests(TestCase):
    def fixture(self):
        now=datetime(2026,10,18,tzinfo=timezone.utc);sku='CI-SKU-0022';uri=f'gs://{sources.BUCKET}/products/{sku}/primary.jpg'
        product=dict(sku=sku,source_system='NAKED_AND_THRIVING_OFFICIAL',brand='Naked & Thriving',product_name='Rejuvenate Restorative Face Oil',product_status='ACTIVE',primary_image_uri=uri,size_value=35,size_unit='mL')
        asset=dict(sku=sku,source_system='NAKED_AND_THRIVING_OFFICIAL',asset_id=f'AST-{sku}-PRIMARY',asset_type='IMAGE',asset_role='PRIMARY_PRODUCT_IMAGE',is_primary=True,gcs_uri=uri,mime_type='image/jpeg',width=8,height=8,source_url='https://cdn.shopify.com/fixture.jpg',asset_description='Original image; visible claims unreviewed')
        row=dict(observed_at=now,products=[json.dumps(product)],product_count=1,assets=[json.dumps(asset)],asset_count=1)
        client=Mock(project=sources.PROJECT,location=sources.LOCATION,default_query_job_config=None)
        job=client.query.return_value;job.result.side_effect=lambda **kwargs:iter([deepcopy(row)]);job.state='DONE';job.error_result=None
        data=io.BytesIO();Image.new('RGB',(8,8),'white').save(data,format='JPEG');raw=data.getvalue()
        gcs=Mock();blob=gcs.bucket.return_value.blob.return_value;blob.generation=123;blob.size=len(raw);blob.content_type='image/jpeg';blob.download_as_bytes.return_value=raw
        return now,sku,client,gcs,blob,row
    def capture(self,fixture):
        now,sku,client,gcs,*_=fixture
        with patch.object(sources,'matches_sharing_job',return_value=True):return sources.capture_official_product_sources(client,gcs,sku,clock=lambda:now)
    def test_native_identity_and_conditionally_downloaded_bytes_are_pinned_not_approved(self):
        f=self.fixture();result=self.capture(f);blob=f[4]
        self.assertEqual(result.manifest['identity']['full_size_ml'],'35')
        self.assertEqual(result.manifest['image']['generation'],'123')
        self.assertEqual(len(result.manifest['native_job_ids']),2)
        self.assertEqual(result.manifest['image']['byte_count'],len(result.image_bytes))
        self.assertFalse(result.manifest['claim_use_authorized']);self.assertFalse(result.manifest['release_allowed'])
        self.assertEqual(result.manifest['visual_claim_review'],'REQUIRED_ACTUAL_IMAGE_REVIEW')
        self.assertEqual(blob.download_as_bytes.call_args.kwargs['if_generation_match'],123)
        self.assertEqual(blob.download_as_bytes.call_args.kwargs['checksum'],'auto')
    def test_duplicate_partial_or_boolean_count_denies_before_storage(self):
        for change in (dict(product_count=2),dict(asset_count=0,assets=[]),dict(product_count=True)):
            f=self.fixture();f[5].update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):self.capture(f)
            f[3].bucket.assert_not_called()
    def test_changed_source_or_object_generation_withholds_capture(self):
        f=self.fixture();count=[0]
        def changed_rows(**kwargs):
            row=deepcopy(f[5]);count[0]+=1
            if count[0]==2:
                p=json.loads(row['products'][0]);p['product_name']='Changed';row['products']=[json.dumps(p)]
            return iter([row])
        f[2].query.return_value.result.side_effect=changed_rows
        with self.assertRaisesRegex(ValueError,'SOURCE_CHANGED'):self.capture(f)
        f=self.fixture();count=[0]
        def changed_blob(**kwargs):
            count[0]+=1
            if count[0]==2:f[4].generation=124
        f[4].reload.side_effect=changed_blob
        with self.assertRaisesRegex(ValueError,'OBJECT_CHANGED'):self.capture(f)
    def test_corrupt_oversized_or_cross_sku_image_is_not_accepted(self):
        f=self.fixture();f[4].download_as_bytes.return_value=b'bad'
        with self.assertRaises(ValueError):self.capture(f)
        f=self.fixture();f[4].size=sources.MAX_IMAGE_BYTES+1
        with self.assertRaises(ValueError):self.capture(f)
        f[4].download_as_bytes.assert_not_called()
        f=self.fixture();a=json.loads(f[5]['assets'][0]);a['sku']='CI-SKU-0021';f[5]['assets']=[json.dumps(a)]
        with self.assertRaises(ValueError):self.capture(f)
    def test_unverified_native_read_or_legacy_scope_is_not_a_fallback(self):
        f=self.fixture()
        with patch.object(sources,'matches_sharing_job',return_value=False),self.assertRaises(ValueError):
            sources.capture_official_product_sources(f[2],f[3],f[1],clock=lambda:f[0])
        with self.assertRaises(ValueError):sources.capture_official_product_sources(f[2],f[3],'CI-SKU-0001')
