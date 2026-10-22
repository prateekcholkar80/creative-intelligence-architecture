import json
import unittest
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

from src.generators.campaign_generator import (
    PROJECT, ROOT, SCHEMA, campaign_merge_sql, generate_campaigns, load_campaigns,
)


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'config/campaign_generator.json').read_text())
        self.start = date(2025, 1, 1)
        self.products = [dict(
            sku=f'CI-SKU-{i:04d}', brand='Source brand', product_name=f'Source product {i}',
            currency='eur', source_system='BEAUTYFEEDS',
            primary_image_uri=f'gs://creative-brand-memory-{PROJECT}/products/CI-SKU-{i:04d}/primary.jpg',
        ) for i in range(1, 21)]

    def generate(self):
        return generate_campaigns(self.products, self.config, self.start)

    def client(self):
        client = MagicMock()
        client.get_table.return_value.schema = SCHEMA
        client.query.return_value.result.return_value = []
        return client

    def test_determinism_and_input_order(self):
        rows = self.generate()
        self.assertEqual(rows, generate_campaigns(self.products[::-1], self.config, self.start))
        changed = generate_campaigns(self.products, dict(self.config, seed=43), self.start)
        self.assertNotEqual(rows, changed)
        self.assertEqual([r['campaign_id'] for r in rows], [r['campaign_id'] for r in changed])

    def test_coverage_dates_and_nonoverlap(self):
        rows = self.generate()
        self.assertEqual(len(rows), 60)
        self.assertEqual(len({r['campaign_id'] for r in rows}), 60)
        for product in self.products:
            campaigns = sorted((r for r in rows if r['sku'] == product['sku']), key=lambda r: r['start_date'])
            self.assertEqual(len(campaigns), 3)
            for r in campaigns:
                self.assertGreaterEqual(r['start_date'], self.start)
                self.assertLessEqual(r['end_date'], self.start + timedelta(days=364))
                self.assertEqual((r['end_date'] - r['start_date']).days, 20)
            self.assertTrue(all(a['end_date'] < b['start_date'] for a, b in zip(campaigns, campaigns[1:])))

    def test_grounded_copy_provenance_and_budget(self):
        products = {p['sku']: p for p in self.products}
        for row in self.generate():
            p = products[row['sku']]
            self.assertEqual(row['headline'], p['product_name'])
            self.assertEqual(row['body_copy'], f"Explore {p['product_name']}. View product details before choosing.")
            self.assertEqual(row['product_image_uri'], p['primary_image_uri'])
            self.assertEqual(row['source_system'], 'SYNTHETIC_POC')
            self.assertEqual(row['campaign_status'], 'SYNTHETIC_SCENARIO')
            self.assertIn(p['sku'], row['source_reference'])
            self.assertIsInstance(row['budget'], Decimal)
            self.assertTrue(500 <= row['budget'] <= 2500)
            self.assertEqual(set(row), {f.name for f in SCHEMA})

    def test_invalid_identity_and_evidence(self):
        with self.assertRaises(ValueError):
            generate_campaigns(self.products[:-1], self.config, self.start)
        self.products[0]['source_system'] = 'SYNTHETIC_POC'
        with self.assertRaises(ValueError):
            self.generate()
        self.products[0]['source_system'] = 'BEAUTYFEEDS'
        self.products[0]['primary_image_uri'] = 'gs://other/product.jpg'
        with self.assertRaises(ValueError):
            self.generate()

    def test_invalid_periods(self):
        for config in (dict(self.config, duration_days=0),
                       dict(self.config, start_jitter_days=-1),
                       dict(self.config, duration_days=100),
                       dict(self.config, waves=[self.config['waves'][0]] * 3)):
            with self.assertRaises(ValueError):
                generate_campaigns(self.products, config, self.start)

    def test_schema_and_merge_scope(self):
        client = self.client()
        rows = self.generate()
        load_campaigns(client, rows)
        self.assertEqual(client.load_table_from_dataframe.call_args.kwargs['job_config'].schema, SCHEMA)
        sql = campaign_merge_sql('project.dataset.stage', list(rows[0]))
        self.assertIn('T.source_system=S.source_system', sql)
        self.assertIn('T.campaign_id=S.campaign_id', sql)
        self.assertNotIn('DELETE', sql)
        self.assertNotIn('T.created_at=', sql)
        client.delete_table.assert_called_once()

    def test_failure_cleanup_and_duplicates(self):
        client = self.client()
        client.load_table_from_dataframe.side_effect = RuntimeError('load failed')
        with self.assertRaises(RuntimeError):
            load_campaigns(client, self.generate())
        client.delete_table.assert_called_once()
        client = self.client()
        client.query.return_value.result.return_value = [{'campaign_id': 'duplicate'}]
        with self.assertRaises(ValueError):
            load_campaigns(client, self.generate())
        self.assertEqual(client.query.call_count, 1)
        client.delete_table.assert_called_once()

    def test_schema_mismatch_and_duplicate_payload(self):
        client = self.client()
        client.get_table.return_value.schema = SCHEMA[:-1]
        with self.assertRaises(ValueError):
            load_campaigns(client, self.generate())
        client.load_table_from_dataframe.assert_not_called()
        rows = self.generate()
        with self.assertRaises(ValueError):
            load_campaigns(self.client(), rows + [rows[0]])


if __name__ == '__main__':
    unittest.main()
