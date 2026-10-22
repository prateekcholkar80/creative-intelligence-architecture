import json
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from google.cloud import bigquery
from src.generators.sales_generator import ROOT, generate_sales, load_sales, merge_sql


class SalesTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'config/sales_generator.json').read_text())
        self.products = [dict(sku=f'CI-SKU-{i:04d}', price=Decimal('35'),
                              currency='eur', product_type='Serum') for i in range(1, 21)]

    def test_determinism_order_and_seed(self):
        rows = generate_sales(self.products, self.config)
        self.assertEqual(rows, generate_sales(self.products[::-1], self.config))
        changed = generate_sales(self.products, dict(self.config, seed=43))
        self.assertNotEqual([r['units_sold'] for r in rows], [r['units_sold'] for r in changed])

    def test_coverage_and_financials(self):
        rows = generate_sales(self.products, self.config)
        self.assertEqual(len(rows), 7300)
        self.assertEqual(len({(r['sku'], r['sales_date']) for r in rows}), 7300)
        for p in self.products:
            dates = [r['sales_date'] for r in rows if r['sku'] == p['sku']]
            self.assertEqual(len(dates), 365)
            self.assertEqual((min(dates), max(dates)), (date(2025, 1, 1), date(2025, 12, 31)))
        for r in rows:
            self.assertEqual(r['source_system'], 'SYNTHETIC_POC')
            self.assertIsInstance(r['net_sales'], Decimal)
            self.assertEqual(r['net_sales'], r['gross_sales'] - r['discount_amount'] - r['refund_amount'])
            self.assertGreaterEqual(r['net_sales'], 0)
            self.assertLessEqual(r['orders'], r['units_sold'])

    def test_invalid_identity_and_price(self):
        with self.assertRaises(ValueError):
            generate_sales(self.products[:-1], self.config)
        self.products[0]['price'] = Decimal('-1')
        with self.assertRaises(ValueError):
            generate_sales(self.products, self.config)

    def test_demand_effects(self):
        self.config.update(noise_bound=0, trend_bound=0, seasonality_amplitude=0)
        rows = generate_sales(self.products, self.config)
        first = rows[:365]
        saturday = [r['units_sold'] for r in first if r['sales_date'].weekday() == 5]
        monday = [r['units_sold'] for r in first if r['sales_date'].weekday() == 0]
        self.assertGreater(sum(saturday) / len(saturday), sum(monday) / len(monday))
        self.products[0]['price'] = Decimal('100')
        cheaper = rows[0]['units_sold']
        self.assertLess(generate_sales(self.products, self.config)[0]['units_sold'], cheaper)

    def test_merge_scope_and_destination_schema(self):
        rows = generate_sales(self.products, self.config)
        client = MagicMock()
        schema = [bigquery.SchemaField(k, 'STRING') for k in rows[0]]
        client.get_table.return_value.schema = schema
        client.query.return_value.result.return_value = []
        load_sales(client, rows)
        kwargs = client.load_table_from_dataframe.call_args.kwargs
        self.assertEqual(kwargs['job_config'].schema, schema)
        sql = merge_sql('project.dataset.stage', list(rows[0]))
        self.assertIn('T.source_system = S.source_system', sql)
        self.assertIn('T.channel = S.channel', sql)
        self.assertNotIn('DELETE', sql)
        self.assertNotIn('T.created_at =', sql)
        client.delete_table.assert_called_once()

    def test_stage_cleanup_on_failure(self):
        rows = generate_sales(self.products, self.config)
        client = MagicMock()
        client.get_table.return_value.schema = [bigquery.SchemaField(k, 'STRING') for k in rows[0]]
        client.load_table_from_dataframe.side_effect = RuntimeError('load failed')
        with self.assertRaises(RuntimeError):
            load_sales(client, rows)
        client.delete_table.assert_called_once()


if __name__ == '__main__':
    unittest.main()
