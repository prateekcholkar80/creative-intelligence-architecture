"""Focused authorized catalog-extension guards; no cloud calls."""
from copy import deepcopy
from decimal import Decimal
import json,hashlib
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import PreconditionFailed,NotFound
from src.ingestion import add_naked_thriving_products as ingest


def fixture():
    manifest=json.loads(ingest.MANIFEST.read_text());images={}
    for p in manifest['products']:
        images[p['canonical_sku']]=dict(source_variant_id=p['source_variant_id'],source_product_id=p['source_product_id'],
            mime_type='image/jpeg',sha256='a'*64,width=1200,height=1200,source_url='https://cdn.shopify.com/official.jpg')
    return manifest,images


def baseline():
    values={name:[] for name in ingest.SCHEMAS}
    for i in range(1,21):
        row={name:[] if mode=='REPEATED' else None for name,_,mode in ingest.SCHEMAS['product_master']}
        row.update(sku=f'CI-SKU-{i:04}',source_system='BEAUTYFEEDS')
        values['product_master'].append(ingest.encoded(row))
    return values


class CatalogAdditionTests(TestCase):
    def setUp(self):self.manifest,self.images=fixture()

    def rows(self):return ingest.prepare_rows(self.manifest,self.images)

    def test_exact_three_full_size_products_no_claims_or_invented_formula(self):
        rows=self.rows();products=rows['product_master']
        self.assertEqual([r['sku'] for r in products],['CI-SKU-0021','CI-SKU-0022','CI-SKU-0023'])
        self.assertEqual([r['size_value'] for r in products],['30','35','35'])
        for r in products:
            self.assertEqual(r['benefits'],[]);self.assertEqual(r['source_system'],'NAKED_AND_THRIVING_OFFICIAL')
            self.assertIn('Formula version unverified',r['description']);self.assertIn(':variant:',r['source_product_id'])
        self.assertEqual(len(rows['ingredients']),41);self.assertEqual(len(rows['product_ingredients']),54)

    def test_source_variant_brand_size_price_and_formula_mismatches_deny(self):
        for values in ({'source_variant_id':'wrong'},{'brand':'Other Brand'},{'size_ml':50},
            {'formula_version':'invented-v1'},{'observed_price_minor':62.1},{'currency':'CAD'},
            {'source_system':'BEAUTYFEEDS'},{'source_product_url':'https://other.test/product'}):
            m=deepcopy(self.manifest);m['products'][0].update(values)
            with self.assertRaises(ValueError):ingest.prepare_rows(m,self.images)
        with self.assertRaises(ValueError):ingest.prepare_rows(self.manifest|{'claim_permissions_created':True},self.images)

    def test_cross_variant_image_pin_and_wrong_format_deny(self):
        for values in ({'source_variant_id':'travel-size'},{'source_product_id':'other'},{'mime_type':'image/webp'},{'sha256':'invalid'}):
            images=deepcopy(self.images);images['CI-SKU-0021'].update(values)
            with self.assertRaises(ValueError):ingest.prepare_rows(self.manifest,images)

    def test_decimal_prices_have_no_binary_float_conversion(self):
        products=self.rows()['product_master']
        self.assertEqual([Decimal(r['price']) for r in products],[Decimal('62.10'),Decimal('66.60'),Decimal('62.10')])

    def test_ingredient_parser_preserves_parentheses_order_and_source_duplicates(self):
        self.assertEqual(ingest.split_ingredients('A (x, y), B*, C'),['A (x, y)','B*','C'])
        for value in ('A,,B','A (B','A),B',''):
            with self.assertRaises(ValueError):ingest.split_ingredients(value)
        rows=self.rows();links=[r for r in rows['product_ingredients'] if r['sku']=='CI-SKU-0022']
        self.assertEqual([r['ingredient_sequence'] for r in links],list(range(1,len(links)+1)))
        self.assertLess(len({r['ingredient_id'] for r in links}),len(links))
        self.assertTrue(all(r['concentration_percent'] is None and r['is_active_ingredient'] is None for r in links))
        self.assertTrue(all(r['ingredient_id'].startswith('ING-NT-') for r in rows['ingredients']))

    def test_identity_usage_warnings_and_asset_badge_limitations_are_retained(self):
        rows=self.rows();renew=rows['product_master'][0]
        for term in ('AHA','sunscreen','seven days','retinol','Patch test'):
            self.assertIn(term,renew['usage_instructions'])
        for asset in rows['product_assets']:
            self.assertIn('not a claim-use approval',asset['asset_description'])
            self.assertTrue(asset['gcs_uri'].endswith('/primary.jpg'))

    def test_write_is_one_atomic_insert_only_transaction_with_full_before_image_checks(self):
        sql,cfg=ingest.write_spec(baseline(),self.rows())
        self.assertEqual(sql.count('INSERT `'),4);self.assertEqual(sql.count('COMMIT TRANSACTION;'),1)
        self.assertIn('ROLLBACK TRANSACTION;',sql);self.assertNotIn('UPDATE',sql);self.assertNotIn('DELETE',sql)
        for name in ingest.SCHEMAS:self.assertIn('NT_CATALOG_STALE_'+name,sql)
        self.assertIn('JSON_VALUE_ARRAY',sql);self.assertIn('AS NUMERIC)',sql)
        self.assertIn('TO_JSON_STRING(t) row_json,COUNT(*) copies',sql)
        self.assertNotIn('EXCEPT DISTINCT SELECT *',sql)
        self.assertFalse(cfg.use_query_cache);self.assertEqual(int(cfg.job_timeout_ms),120000)

    def test_existing_new_or_partial_catalog_cannot_be_overwritten_or_replayed(self):
        old=baseline();rows=self.rows()
        for change in ('missing','duplicate','target','wrong-source'):
            altered=deepcopy(old)
            if change=='missing':altered['product_master'].pop()
            elif change=='duplicate':altered['product_master'][0]=altered['product_master'][1]
            elif change=='target':altered['product_master'].append(ingest.encoded(rows['product_master'][0]))
            else:
                r=json.loads(altered['product_master'][0]);r['source_system']='OTHER';altered['product_master'][0]=ingest.encoded(r)
            with self.assertRaises(ValueError):ingest.write_spec(altered,rows)

    def test_orphan_related_rows_and_existing_ingredient_id_conflicts_deny(self):
        old=baseline();rows=self.rows()
        for table in ('product_ingredients','product_assets','ingredients'):
            altered=deepcopy(old);altered[table].append(ingest.encoded(rows[table][0]))
            with self.assertRaises(ValueError):ingest.write_spec(altered,rows)

    def test_bounded_serialization_rejects_unknown_types_and_oversized_inputs(self):
        with self.assertRaises(TypeError):ingest.encoded(object())
        with self.assertRaises(ValueError):ingest.encoded('x'*(ingest.MAX_BYTES+1))

    def test_corrected_slot_requires_correlated_positive_rollback_and_identical_full_snapshot(self):
        from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
        old=baseline();plan=dict(sql='original failed script',config=ingest.settings([]).to_api_repr(),baseline=old)
        client=Mock()
        with patch.object(ingest,'matches_sharing_job',return_value=True),patch.object(ingest,'snapshot',return_value=(old,'fixture-read')):
            for status in (Status.UNKNOWN,Status.COMMITTED):
                with patch.object(ingest,'failed_sharing_job_status',return_value=status):
                    with self.assertRaises(ValueError):ingest.authorize_corrected_submission(client,plan)
            with patch.object(ingest,'failed_sharing_job_status',return_value=Status.NOT_COMMITTED):
                self.assertEqual(ingest.authorize_corrected_submission(client,plan)['status'],'NOT_COMMITTED')
                with patch.object(ingest,'snapshot',return_value=({},'changed-read')):
                    with self.assertRaises(ValueError):ingest.authorize_corrected_submission(client,plan)
        client.query.assert_not_called()
        with patch.object(ingest,'matches_sharing_job',return_value=False):
            with self.assertRaises(ValueError):ingest.authorize_corrected_submission(client,plan)

    def image_client(self):
        raw=b'original-image-fixture';image=self.images['CI-SKU-0021']|dict(local_path='/missing/file.jpg',
            sha256=hashlib.sha256(raw).hexdigest(),size_bytes=len(raw))
        client=Mock();blob=Mock(generation='7',content_type='image/jpeg',name='products/CI-SKU-0021/primary.jpg')
        client.bucket.return_value.blob.return_value=blob;blob.download_as_bytes.return_value=raw
        opened=Mock();opened.__enter__=Mock(return_value=Mock(format='JPEG',size=(1200,1200)));opened.__exit__=Mock(return_value=False)
        return client,blob,image,raw,opened

    def test_existing_exact_gcs_object_recovers_without_original_local_file_or_upload(self):
        client,blob,image,raw,opened=self.image_client()
        with patch.object(ingest.Path,'read_bytes',side_effect=FileNotFoundError),patch.object(ingest.Image,'open',return_value=opened):
            pins=ingest.upload_images(client,{'CI-SKU-0021':image})
        self.assertEqual(pins['CI-SKU-0021']['generation'],7);blob.upload_from_string.assert_not_called()
        self.assertEqual(blob.download_as_bytes.call_args.kwargs['if_generation_match'],7)

    def test_conflicting_existing_object_is_never_overwritten(self):
        client,blob,image,raw,opened=self.image_client();blob.download_as_bytes.return_value=b'changed'
        with self.assertRaises(ValueError):ingest.upload_images(client,{'CI-SKU-0021':image})
        blob.upload_from_string.assert_not_called()

    def test_create_race_uses_generation_zero_then_verifies_winning_bytes(self):
        client,blob,image,raw,opened=self.image_client();blob.reload.side_effect=[NotFound('missing'),None]
        blob.upload_from_string.side_effect=PreconditionFailed('another creator')
        with patch.object(ingest.Path,'read_bytes',return_value=raw),patch.object(ingest.Image,'open',return_value=opened):
            self.assertEqual(ingest.upload_images(client,{'CI-SKU-0021':image})['CI-SKU-0021']['generation'],7)
        self.assertEqual(blob.upload_from_string.call_args.kwargs['if_generation_match'],0)
