import json
import unittest
from datetime import date
from decimal import Decimal

from src.generators.sales_generator import ROOT, campaign_effects, generate_sales


class CampaignUpliftTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'config/sales_generator.json').read_text())
        self.products = [dict(sku=f'CI-SKU-{i:04d}', price=Decimal('35'),
                              currency='eur', product_type='Serum') for i in range(1, 21)]
        self.campaigns = [dict(campaign_id='SYN-CMP-20250101-CI-SKU-0001-01',
            sku='CI-SKU-0001', start_date=date(2025, 2, 1), end_date=date(2025, 2, 21),
            objective='CONVERSION', source_system='SYNTHETIC_POC')]

    def test_inclusive_period_and_unchanged_baseline(self):
        baseline = generate_sales(self.products, self.config)
        modeled = generate_sales(self.products, self.config, self.campaigns)
        effects = campaign_effects(self.campaigns, self.config)
        self.assertEqual(len(effects), 21)
        self.assertIn(('CI-SKU-0001', date(2025, 2, 1)), effects)
        self.assertIn(('CI-SKU-0001', date(2025, 2, 21)), effects)
        self.assertEqual(len(modeled), 7300)
        changed = 0
        for before, after in zip(baseline, modeled):
            key = (after['sku'], after['sales_date'])
            if key in effects:
                self.assertGreater(after['units_sold'], before['units_sold'])
                self.assertEqual(after['net_sales'], after['gross_sales'] - after['discount_amount'] - after['refund_amount'])
                changed += 1
            else:
                self.assertEqual(before, after)
        self.assertEqual(changed, 21)

    def test_determinism_and_no_compounding(self):
        campaigns = self.campaigns + [dict(self.campaigns[0], campaign_id='other',
            sku='CI-SKU-0002')]
        rows = generate_sales(self.products, self.config, campaigns)
        self.assertEqual(rows, generate_sales(self.products[::-1], self.config, campaigns[::-1]))
        self.assertEqual(rows, generate_sales(self.products, self.config, campaigns))
        self.assertEqual(generate_sales(self.products, self.config, []),
                         generate_sales(self.products, self.config))

    def test_curve_zero_uplift_and_single_day(self):
        effects = campaign_effects(self.campaigns, self.config)
        self.assertGreater(effects[('CI-SKU-0001',date(2025,2,11))]['multiplier'],
                           effects[('CI-SKU-0001',date(2025,2,1))]['multiplier'])
        single = dict(self.campaigns[0], end_date=date(2025,2,1))
        self.assertEqual(len(campaign_effects([single], self.config)), 1)
        self.config['campaign_effects']['uplift_by_objective']['CONVERSION'] = 0
        self.assertEqual(generate_sales(self.products, self.config, self.campaigns),
                         generate_sales(self.products, self.config))

    def test_reject_overlap_invalid_identity_and_dates(self):
        with self.assertRaises(ValueError):
            campaign_effects(self.campaigns * 2, self.config)
        with self.assertRaises(ValueError):
            campaign_effects(self.campaigns + [dict(self.campaigns[0], campaign_id='other')], self.config)
        for changes in (dict(source_system='REAL'), dict(sku='OTHER'),
                        dict(start_date=date(2024,12,31)), dict(end_date=date(2026,1,1)),
                        dict(end_date=date(2025,1,1))):
            with self.assertRaises(ValueError):
                campaign_effects([dict(self.campaigns[0], **changes)], self.config)


if __name__ == '__main__':
    unittest.main()
