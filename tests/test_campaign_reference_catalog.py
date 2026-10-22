from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_chat import read_reference_catalog,ChatError,CANONICAL_SKUS
from src.application.campaign_quality_authority import CATALOG


class ReferenceCatalogTests(TestCase):
    def fixture(self):
        client=Mock();rows=[dict(sku=sku,brand='Brand' if sku in CANONICAL_SKUS else 'Naked & Thriving',
            product_name='Product '+sku,source_system='BEAUTYFEEDS' if sku in CANONICAL_SKUS else 'NAKED_AND_THRIVING_OFFICIAL',product_status='ACTIVE') for sku in sorted(CATALOG)]
        client.query.return_value.result.return_value=rows
        native=client.get_job.return_value;native.state='DONE';native.error_result=None
        return client,rows
    def test_official_products_visible_only_in_separate_protected_catalog(self):
        client,rows=self.fixture()
        with patch('src.application.sharing_jobs.matches_sharing_job',return_value=True):
            result=read_reference_catalog(client,67108864)
        self.assertEqual(len(result),23);self.assertEqual(len(CANONICAL_SKUS),20)
        self.assertIn('CI-SKU-0022',{r['sku'] for r in result})
        self.assertFalse(client.query.call_args.kwargs['job_config'].use_query_cache)
    def test_inactive_duplicate_wrong_provenance_and_uncorrelated_jobs_deny(self):
        for change in ('inactive','duplicate','source','job'):
            client,rows=self.fixture()
            if change=='inactive':rows[-1]['product_status']='INACTIVE'
            if change=='duplicate':rows[-1]=rows[0]
            if change=='source':rows[-1]['source_system']='BEAUTYFEEDS'
            with patch('src.application.sharing_jobs.matches_sharing_job',return_value=change!='job'):
                with self.assertRaises(ChatError):read_reference_catalog(client,67108864)
