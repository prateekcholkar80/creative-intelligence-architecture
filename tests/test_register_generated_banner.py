import copy
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import bigquery

from src.generators.banner_preview_generator import render_preview
from src.generators.campaign_brief_generator import assemble_brief
from src.ingestion.register_generated_banner import (
    IMMUTABLE, SCHEMA, SQL, TARGET, encode, ensure_destination, inspect_inventory, load_registration,
    recheck_sources, register_banner, registration_row, revalidate_candidate, upload_banner,
)
from tests import test_banner_preview_generator

MODULE = 'src.ingestion.register_generated_banner.'


class RegisterGeneratedBannerTests(unittest.TestCase):
    def setUp(self):
        fixture = test_banner_preview_generator.BannerPreviewTests();fixture.setUp()
        self.bundle = fixture.bundle;self.request = fixture.request;self.config = fixture.config
        self.bundle.context['image_manifest'][0]['gcs_generation'] = '12345'
        self.brief = assemble_brief(self.bundle.context,fixture.draft,{})
        self.brief['approval_recheck_passed'] = True
        self.preview = render_preview(self.brief,self.bundle,self.config)
        self.preview.metadata['approval_recheck_passed'] = True
        self.row = registration_row(self.preview,self.brief,self.bundle)
        self.row['gcs_generation'] = '67890'
        self.destination = bigquery.Table(TARGET,schema=SCHEMA)
        self.temp = tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.write_candidate()

    def write_candidate(self, proposal=None, metadata=None, png=None):
        (self.directory/'banner_preview.png').write_bytes(self.preview.png if png is None else png)
        (self.directory/'banner_preview.json').write_text(encode(self.preview.metadata if metadata is None else metadata))
        (self.directory/'campaign_brief.json').write_text(encode(self.brief if proposal is None else proposal))

    def revalidate(self):
        return revalidate_candidate(Mock(),Mock(),Mock(),self.request,self.directory,{}, {})

    def test_registration_has_complete_schema_provenance_and_content_addressed_identity(self):
        self.assertEqual(set(self.row),{f.name for f in SCHEMA})
        repeated = {f.name for f in SCHEMA if f.mode == 'REPEATED'}
        self.assertEqual(repeated,{'approved_claim_ids','evidence_references'})
        self.assertIn(self.row['sha256'],self.row['asset_id'])
        self.assertTrue(self.row['gcs_uri'].endswith(self.row['sha256']+'.png'))
        self.assertFalse(self.row['real_business_approved'])
        self.assertEqual(json.loads(self.row['evidence_packet_json'])['source_of_truth'],'BIGQUERY')
        self.assertEqual(self.row['source_image_generation'],'12345')
        self.assertEqual(registration_row(self.preview,self.brief,self.bundle)['asset_id'],self.row['asset_id'])

    def test_fresh_cloud_evidence_reconstructs_same_preview_without_cached_facts(self):
        proposal = copy.deepcopy(self.brief)
        proposal['grounded_evidence']['sales']['totals']['net_sales'] = 'FAKE CACHED METRIC'
        proposal['grounded_evidence']['approved_claims']['claims'] = ['FAKE APPROVAL']
        self.write_candidate(proposal=proposal)
        with patch(MODULE+'retrieve_evidence_packet',return_value=self.bundle) as reader:
            preview,brief,bundle = self.revalidate()
        reader.assert_called_once()
        self.assertEqual(preview.png,self.preview.png)
        self.assertEqual(brief['grounded_evidence']['sales']['totals'],self.brief['grounded_evidence']['sales']['totals'])
        self.assertEqual(brief['grounded_evidence']['approved_claims'],self.brief['grounded_evidence']['approved_claims'])
        self.assertIs(bundle,self.bundle)

    def test_stale_snapshot_altered_preview_and_claim_or_font_changes_are_rejected(self):
        with patch(MODULE+'retrieve_evidence_packet',return_value=self.bundle):
            for key,value in (('snapshot_id','old'),('sha256','bad'),('canonical_sku','CI-SKU-0002')):
                metadata = copy.deepcopy(self.preview.metadata);metadata[key] = value
                self.write_candidate(metadata=metadata)
                with self.assertRaises(ValueError):self.revalidate()
            proposal = copy.deepcopy(self.brief);proposal['model_selection']['headline'] = 'Benefit invented'
            self.write_candidate(proposal=proposal)
            with self.assertRaises(ValueError):self.revalidate()
            metadata = copy.deepcopy(self.preview.metadata)
            metadata['font_files']['heading_font_path']['sha256'] = 'changed-font'
            self.write_candidate(metadata=metadata)
            with self.assertRaises(ValueError):self.revalidate()
            self.write_candidate(png=b'corrupt')
            with self.assertRaises(ValueError):self.revalidate()

    def test_bigquery_unavailable_has_no_export_fallback(self):
        with patch(MODULE+'retrieve_evidence_packet',side_effect=RuntimeError('BigQuery unavailable')):
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):self.revalidate()

    def test_inventory_verifies_existing_datasets_products_and_twenty_original_objects(self):
        client = Mock();gcs = Mock();client.get_dataset.return_value.location = 'asia-south1'
        client.get_table.side_effect = NotFound('new generated table absent')
        counts = dict(products=20,product_assets=20,sales_rows=7300,historical_campaigns=60,historical_assets=60)
        client.query.return_value.result.return_value = [counts]
        gcs.list_blobs.return_value = [Mock(name=f'products/CI-SKU-{n:04}/primary.jpg') for n in range(1,21)]
        # Mock's name parameter labels the mock instead of exposing a .name field.
        for n,blob in enumerate(gcs.list_blobs.return_value,1):blob.name = f'products/CI-SKU-{n:04}/primary.jpg'
        result = inspect_inventory(client,gcs)
        self.assertEqual(result['original_gcs_images'],20);self.assertEqual(result['generated_assets'],0)
        self.assertEqual(client.get_dataset.call_count,4)
        client.create_table.assert_not_called()

    def test_inventory_missing_original_or_wrong_region_is_not_silently_accepted(self):
        client = Mock();gcs = Mock();client.get_dataset.return_value.location = 'asia-south1'
        client.query.return_value.result.return_value = [dict(products=20,product_assets=20)]
        gcs.list_blobs.return_value = []
        with self.assertRaises(ValueError):inspect_inventory(client,gcs)
        client.get_dataset.return_value.location = 'US'
        with self.assertRaisesRegex(ValueError,'region'):inspect_inventory(client,gcs)

    def blob(self, metadata=None, stored=None):
        gcs = Mock();blob = gcs.bucket.return_value.blob.return_value
        blob.content_type = 'image/png';blob.size = len(self.preview.png);blob.generation = 67890
        blob.download_as_bytes.return_value = self.preview.png if stored is None else stored
        if metadata is not None:
            blob.reload.side_effect = lambda: setattr(blob,'metadata',metadata)
        return gcs,blob

    def test_upload_is_create_only_and_pins_readback_generation(self):
        gcs,blob = self.blob()
        self.assertEqual(upload_banner(gcs,self.row,self.preview.png),'67890')
        self.assertEqual(blob.upload_from_string.call_args.kwargs['if_generation_match'],0)
        blob.download_as_bytes.assert_called_once_with(if_generation_match=67890)

    def test_existing_object_is_reused_but_not_replaced_on_retry(self):
        gcs,blob = self.blob();blob.upload_from_string.side_effect = PreconditionFailed('exists')
        self.assertEqual(upload_banner(gcs,self.row,self.preview.png),'67890')
        blob.delete.assert_not_called();blob.patch.assert_not_called()
        blob.download_as_bytes.return_value = b'foreign-content'
        with self.assertRaisesRegex(ValueError,'refusing to overwrite'):upload_banner(gcs,self.row,self.preview.png)

    def test_foreign_object_metadata_or_path_is_not_overwritten(self):
        gcs,blob = self.blob(metadata={'source_system':'ACTUAL_HISTORY'})
        blob.upload_from_string.side_effect = PreconditionFailed('exists')
        with self.assertRaisesRegex(ValueError,'metadata differs'):upload_banner(gcs,self.row,self.preview.png)
        with self.assertRaises(ValueError):upload_banner(gcs,{**self.row,'gcs_uri':'gs://foreign/banner.png'},self.preview.png)

    def test_destination_schema_is_verified_without_alteration(self):
        client = Mock();client.get_table.return_value = self.destination
        self.assertEqual(ensure_destination(client).schema,SCHEMA)
        client.get_table.return_value = bigquery.Table(TARGET,schema=[bigquery.SchemaField('other','STRING')])
        with self.assertRaisesRegex(ValueError,'schema differs'):ensure_destination(client)
        client.update_table.assert_not_called()

    def load_client(self):
        client = Mock();stored = {**self.row,'created_at':datetime(2025,1,1,tzinfo=timezone.utc)}
        validation = dict(asset_count=1,invalid_metadata=0,missing_product_evidence=0,missing_current_approvals=0)
        client.query.side_effect = [Mock(result=Mock(return_value=[])),Mock(result=Mock(return_value=[validation])),
                                    Mock(result=Mock(return_value=[stored]))]
        return client,stored

    def test_schema_explicit_json_staging_transaction_and_timestamp_preservation(self):
        client,stored = self.load_client()
        result = load_registration(client,self.row,self.destination)
        self.assertEqual(result['created_at'],stored['created_at'])
        settings = client.load_table_from_json.call_args.kwargs['job_config']
        self.assertEqual(settings.schema,self.destination.schema);self.assertFalse(settings.autodetect)
        self.assertEqual(settings.write_disposition,'WRITE_EMPTY')
        sql = client.query.call_args_list[0].args[0]
        self.assertEqual(client.query.call_args_list[0].kwargs['job_config'].maximum_bytes_billed,512*1024*1024)
        self.assertIn('BEGIN TRANSACTION',sql);self.assertIn('COMMIT TRANSACTION',sql)
        self.assertIn('T.source_system=S.source_system',sql)
        self.assertNotIn('DELETE',sql);self.assertNotIn('created_at=S.created_at',sql)
        self.assertIn('poc_approved_product_claims',sql)
        self.assertIn('C.claim_id=required_claim_id',sql)
        for key in IMMUTABLE:self.assertIn(f'T.{key} IS DISTINCT FROM S.{key}',sql)
        client.delete_table.assert_called_once()

    def test_stage_cleanup_on_load_or_transaction_failure(self):
        for phase in ('load','merge'):
            client = Mock()
            if phase == 'load':client.load_table_from_json.side_effect = RuntimeError('failure')
            else:client.query.side_effect = RuntimeError('failure')
            with self.assertRaises(RuntimeError):load_registration(client,self.row,self.destination)
            client.delete_table.assert_called_once()
            self.assertNotEqual(client.delete_table.call_args.args[0],TARGET)

    def test_duplicate_validation_or_stored_metadata_mismatch_is_detected(self):
        client,stored = self.load_client();stored['sha256'] = 'foreign'
        with self.assertRaisesRegex(ValueError,'payload differs'):load_registration(client,self.row,self.destination)

    def test_changed_current_approval_product_or_original_image_aborts_recheck(self):
        brand = copy.deepcopy(self.bundle.context['brand_context'])
        product = copy.deepcopy(self.bundle.context['product_knowledge'])
        original = copy.deepcopy(self.bundle.context['image_manifest'][0])
        with patch(MODULE+'retrieve_brand_context',return_value=brand),patch(MODULE+'retrieve_product_knowledge',return_value=product),patch(MODULE+'retrieve_product_image',return_value=(original,b'original')):
            recheck_sources(Mock(),Mock(),self.bundle)
            brand['approved_claims'] = []
            with self.assertRaises(ValueError):recheck_sources(Mock(),Mock(),self.bundle)
            brand['approved_claims'] = self.bundle.context['brand_context']['approved_claims']
            original['gcs_generation'] = 'changed'
            with self.assertRaises(ValueError):recheck_sources(Mock(),Mock(),self.bundle)

    def test_pipeline_rechecks_before_and_after_upload_and_never_uploads_on_grounding_failure(self):
        with (patch(MODULE+'revalidate_candidate',return_value=(self.preview,self.brief,self.bundle)) as candidate,
             patch(MODULE+'ensure_destination',return_value=self.destination),
             patch(MODULE+'recheck_sources') as recheck,
             patch(MODULE+'upload_banner',return_value='67890') as upload,
             patch(MODULE+'initialize_review') as review,
             patch(MODULE+'load_registration',return_value=self.row) as load):
            result = register_banner(Mock(),Mock(),Mock(),self.request,self.directory,{}, {})
            self.assertEqual(result,self.row);self.assertEqual(recheck.call_count,2)
            self.assertEqual(load.call_args.args[1]['gcs_generation'],'67890')
            review.assert_called_once();self.assertEqual(review.call_args.args[1],self.row)
            candidate.side_effect = RuntimeError('No cloud evidence');upload.reset_mock();load.reset_mock()
            with self.assertRaises(RuntimeError):register_banner(Mock(),Mock(),Mock(),self.request,self.directory,{}, {})
            upload.assert_not_called();load.assert_not_called()


if __name__ == '__main__':
    unittest.main()
