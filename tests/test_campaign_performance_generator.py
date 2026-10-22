import json
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from src.generators.campaign_performance_generator import (
    ROOT, SCHEMA, allocate_budget, generate_performance, load_performance,
    performance_merge_sql, ratio,
)


class PerformanceTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'config/campaign_performance_generator.json').read_text())
        self.campaigns, self.sales = [], []
        for i in range(1, 21):
            sku = f'CI-SKU-{i:04d}'
            for j, channel in enumerate(('PAID_SOCIAL', 'PAID_SEARCH', 'EMAIL')):
                start = date(2025, 1, 1) + timedelta(days=j * 100)
                self.campaigns.append(dict(campaign_id=f'CMP-{sku}-{j}', asset_id=f'AST-{sku}-{j}',
                    sku=sku, channel=channel, currency='EUR', budget=Decimal('1000.01'),
                    start_date=start, end_date=start + timedelta(days=20), source_system='SYNTHETIC_POC',
                    created_at=datetime(2025, 1, 1, tzinfo=timezone.utc)))
                for d in range(21):
                    self.sales.append(dict(sku=sku, sales_date=start + timedelta(days=d), units_sold=40,
                        orders=35, net_sales=Decimal('1200.00'), currency='EUR', source_system='SYNTHETIC_POC'))

    def generate(self):
        return generate_performance(self.campaigns, self.sales, self.config)

    def test_determinism_and_order(self):
        rows = self.generate()
        self.assertEqual(rows, generate_performance(self.campaigns[::-1], self.sales[::-1], self.config))
        self.assertNotEqual(rows, generate_performance(self.campaigns, self.sales, dict(self.config, seed=43)))

    def test_funnel_budget_and_attribution(self):
        rows = self.generate()
        self.assertEqual(len(rows), 1260)
        self.assertEqual(len({(r['campaign_id'], r['performance_date']) for r in rows}), 1260)
        for campaign in self.campaigns:
            daily = [r for r in rows if r['campaign_id'] == campaign['campaign_id']]
            self.assertEqual(len(daily), 21)
            self.assertEqual(sum(r['spend'] for r in daily), campaign['budget'])
        for r in rows:
            self.assertTrue(0 <= r['conversions'] <= r['clicks'] <= r['impressions'])
            self.assertTrue(r['conversions'] <= r['attributed_units'] <= 26)
            self.assertLessEqual(r['attributed_revenue'], Decimal('780'))
            self.assertEqual(r['ctr'], ratio(r['clicks'], r['impressions']))
            self.assertEqual(r['conversion_rate'], ratio(r['conversions'], r['clicks']))
            self.assertEqual(r['roas'], ratio(r['attributed_revenue'], r['spend']))
            self.assertEqual(set(r), {f.name for f in SCHEMA})

    def test_zero_sales_and_tiny_budget(self):
        for s in self.sales:
            s.update(units_sold=0, orders=0, net_sales=Decimal(0))
        for c in self.campaigns:
            c['budget'] = Decimal('0.01')
        for r in self.generate():
            self.assertEqual(r['conversions'], 0)
            self.assertEqual(r['attributed_units'], 0)
            self.assertEqual(r['attributed_revenue'], 0)
            self.assertGreaterEqual(r['roas'], 0)
        self.assertEqual(sum(allocate_budget(Decimal('0.01'),21)), Decimal('0.01'))

    def test_missing_duplicate_sales_and_currency(self):
        with self.assertRaises(ValueError):
            generate_performance(self.campaigns, self.sales[1:], self.config)
        with self.assertRaises(ValueError):
            generate_performance(self.campaigns, self.sales + [self.sales[0]], self.config)
        self.sales[0]['currency'] = 'USD'
        with self.assertRaises(ValueError):
            self.generate()

    def test_invalid_campaigns_and_overlap(self):
        with self.assertRaises(ValueError):
            generate_performance(self.campaigns[:-1], self.sales, self.config)
        self.campaigns[1]['start_date'] = self.campaigns[0]['start_date']
        self.campaigns[1]['end_date'] = self.campaigns[0]['end_date']
        with self.assertRaises(ValueError):
            self.generate()

    def test_invalid_config_budget_and_zero_ratio(self):
        with self.assertRaises(ValueError):
            generate_performance(self.campaigns, self.sales, dict(self.config, max_attribution_share='1.1'))
        with self.assertRaises(ValueError):
            allocate_budget(Decimal('1.001'), 21)
        self.assertEqual(ratio(1,0), 0)
        self.assertEqual(ratio(1,3), Decimal('0.333333333'))

    def test_schema_explicit_load_scoped_merge_and_cleanup(self):
        rows = self.generate()
        client = MagicMock()
        client.get_table.return_value.schema = SCHEMA
        client.query.return_value.result.return_value = []
        load_performance(client, rows)
        self.assertEqual(client.load_table_from_dataframe.call_args.kwargs['job_config'].schema, SCHEMA)
        sql = performance_merge_sql('project.dataset.stage', list(rows[0]))
        self.assertIn('T.source_system=S.source_system', sql)
        self.assertIn('T.performance_date=S.performance_date', sql)
        self.assertNotIn('DELETE', sql)
        self.assertNotIn('T.created_at=', sql)
        client.delete_table.assert_called_once()

    def test_failure_cleanup(self):
        client = MagicMock()
        client.get_table.return_value.schema = SCHEMA
        client.load_table_from_dataframe.side_effect = RuntimeError('failed')
        with self.assertRaises(RuntimeError):
            load_performance(client, self.generate())
        client.delete_table.assert_called_once()


if __name__ == '__main__':
    unittest.main()
