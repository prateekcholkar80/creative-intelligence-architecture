import copy
import json
import unittest
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

from src.generators.sales_generator import PROJECT
from src.retrieval.sales_analytics import (
    CONFIG, SQL, build_packet, capture_sales_analytics, query_scope, retrieve_sales_analytics,
)


class SalesAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.sku = 'CI-SKU-0001'
        self.config = dict(json.loads(CONFIG.read_text()),start_date='2025-01-30',end_date='2025-02-04',trend_window_days=2)
        start = date(2025,1,30)
        self.days = {start+timedelta(days=i):u for i,u in enumerate((10,20,0,40,50,60))}
        def totals(first,last):
            units = sum(u for d,u in self.days.items() if first<=d<=last)
            days = (last-first).days+1
            return dict(row_count=days,day_count=days,first_date=first,last_date=last,
                units_sold=units,orders=units//2,gross_sales=Decimal(units)*2,
                discount_amount=Decimal('0'),refund_amount=Decimal('0'),net_sales=Decimal(units)*2)
        recent = dict(totals(date(2025,2,3),date(2025,2,4)),window_name='RECENT')
        previous = dict(totals(date(2025,2,1),date(2025,2,2)),window_name='PREVIOUS')
        campaign = dict(totals(date(2025,1,31),date(2025,2,1)),
            campaign_id=f'SYN-CMP-20250101-{self.sku}-01',sku=self.sku,objective='AWARENESS',
            start_date=date(2025,1,31),end_date=date(2025,2,1),currency='EUR',source_system='SYNTHETIC_POC',
            effective_start=date(2025,1,31),effective_end=date(2025,2,1))
        self.result = dict(products=[dict(sku=self.sku,brand='Source brand',product_name='Source product',
            currency='EUR',source_system='BEAUTYFEEDS')],
            totals=dict(totals(start,date(2025,2,4)),currency_count=1,invalid_rows=0),
            monthly=[dict(totals(start,date(2025,1,31)),month_start=date(2025,1,1)),
                     dict(totals(date(2025,2,1),date(2025,2,4)),month_start=date(2025,2,1))],
            windows=[recent,previous],trends=dict(units_change=70,orders_change=35,net_sales_change=Decimal('140'),
                units_change_fraction=Decimal('1.75'),orders_change_fraction=Decimal('1.75'),
                net_sales_change_fraction=Decimal('1.75')),
            campaign_periods=[campaign],campaign_count=1,overlapping_campaign_days=0)
        self.client = Mock();self.client.query.return_value.result.return_value = [self.result]
        self.client.query.return_value.job_id='bq-sales-read-1'
        self.client.query.return_value.project=PROJECT
        self.client.query.return_value.location='asia-south1'

    def test_partial_months_equal_windows_and_exact_financial_reconciliation(self):
        packet = retrieve_sales_analytics(self.client,self.sku,self.config)
        self.assertEqual(packet['source_of_truth'],'BIGQUERY')
        self.assertEqual(packet['source_system'],'SYNTHETIC_POC')
        self.assertEqual(packet['totals']['units_sold'],180)
        self.assertEqual(packet['totals']['net_sales'],Decimal('360'))
        self.assertEqual([m['day_count'] for m in packet['monthly']],[2,4])
        self.assertEqual(packet['windows']['RECENT']['first_date'],date(2025,2,3))
        self.assertEqual(packet['windows']['PREVIOUS']['last_date'],date(2025,2,2))
        self.assertEqual(packet['trends']['units_change_fraction'],Decimal('1.75'))
        self.assertEqual(len(packet['evidence_references']),2)

    def test_campaign_period_sales_do_not_invent_counterfactual_uplift(self):
        packet = build_packet(self.sku,self.result,self.config)
        campaign = packet['campaign_periods'][0]
        self.assertEqual(campaign['units_sold'],20)
        self.assertIsNone(campaign['modeled_incremental_units'])
        self.assertEqual(campaign['uplift_status'],'BASELINE_NOT_PRESENT_IN_QUERIED_BIGQUERY_SALES')
        self.assertEqual(len(campaign['evidence_references']),2)
        self.assertNotIn('modeled_incremental_units',self.result['campaign_periods'][0])
        self.assertNotIn('raw_daily_rows',packet)

    def test_invalid_daily_coverage_currency_or_financials_stop_retrieval(self):
        for key,value in (('row_count',7),('day_count',5),('invalid_rows',1),('currency_count',2),
                          ('first_date',date(2025,1,29)),('net_sales',Decimal('359')),
                          ('gross_sales',360.0),('orders',181),('units_sold',True),
                          ('refund_amount',Decimal('NaN'))):
            result = copy.deepcopy(self.result);result['totals'][key] = value
            with self.subTest(field=key),self.assertRaises(ValueError):build_packet(self.sku,result,self.config)

    def test_missing_duplicate_or_nonreconciling_months_fail(self):
        for monthly in (self.result['monthly'][:1],[self.result['monthly'][0]]*2,
                        [self.result['monthly'][0],dict(self.result['monthly'][1],units_sold=149)]):
            with self.assertRaises(ValueError):
                build_packet(self.sku,{**self.result,'monthly':monthly},self.config)

    def test_bad_windows_and_averaged_or_incorrect_trends_fail(self):
        result = copy.deepcopy(self.result);result['windows'][0]['row_count'] = 1
        with self.assertRaises(ValueError):build_packet(self.sku,result,self.config)
        result = copy.deepcopy(self.result);result['windows'] = result['windows'][:1]*2
        with self.assertRaises(ValueError):build_packet(self.sku,result,self.config)
        for key,value in (('units_change',0),('units_change_fraction',Decimal('175')),
                          ('net_sales_change_fraction',float('nan'))):
            result = copy.deepcopy(self.result);result['trends'][key] = value
            with self.assertRaises(ValueError):build_packet(self.sku,result,self.config)

    def test_zero_previous_window_produces_null_growth_not_infinity(self):
        result = copy.deepcopy(self.result)
        previous = result['windows'][1]
        for key in ('units_sold','orders'):previous[key] = 0
        for key in ('gross_sales','discount_amount','refund_amount','net_sales'):previous[key] = Decimal(0)
        result['trends'] = dict(units_change=110,orders_change=55,net_sales_change=Decimal('220'),
            units_change_fraction=None,orders_change_fraction=None,net_sales_change_fraction=None)
        packet = build_packet(self.sku,result,self.config)
        self.assertIsNone(packet['trends']['units_change_fraction'])
        result['trends']['units_change_fraction'] = Decimal('0')
        with self.assertRaises(ValueError):build_packet(self.sku,result,self.config)

    def test_product_and_campaign_identity_currency_or_overlap_errors_fail(self):
        for key,value in (('sku','CI-SKU-0002'),('source_system','ACTUAL'),('currency','USD')):
            result = copy.deepcopy(self.result);result['campaign_periods'][0][key] = value
            with self.assertRaises(ValueError):build_packet(self.sku,result,self.config)
        for key,value in (('overlapping_campaign_days',1),('campaign_count',2),('products',[])):
            with self.assertRaises(ValueError):build_packet(self.sku,{**self.result,key:value},self.config)
        result = copy.deepcopy(self.result);result['products'][0]['sku'] = 'CI-SKU-0002'
        with self.assertRaises(ValueError):build_packet(self.sku,result,self.config)

    def test_scope_validation_and_leap_year_calendar(self):
        leap = dict(self.config,start_date='2024-02-01',end_date='2024-02-29',trend_window_days=7)
        start,end,recent,previous = query_scope(leap)
        self.assertEqual((start,end,recent,previous),
                         (date(2024,2,1),date(2024,2,29),date(2024,2,23),date(2024,2,16)))
        for key,value in (('trend_window_days',0),('trend_window_days',True),('trend_window_days',4),
                          ('channel','OTHER'),('maximum_bytes_billed',0),('maximum_bytes_billed',True)):
            with self.assertRaises(ValueError):
                retrieve_sales_analytics(self.client,self.sku,{**self.config,key:value})
        with self.assertRaises(ValueError):retrieve_sales_analytics(self.client,'OTHER',self.config)
        self.client.query.assert_not_called()

    def test_read_only_query_binds_canonical_scope_and_preserves_structured_analytics(self):
        retrieve_sales_analytics(self.client,self.sku,self.config)
        call = self.client.query.call_args;job = call.kwargs['job_config']
        params = {p.name:p for p in job.query_parameters}
        self.assertEqual(params['sku'].value,self.sku)
        self.assertEqual(params['channel'].value,'ECOMMERCE')
        self.assertEqual(str(params['recent_start'].value),'2025-02-03')
        self.assertEqual(job.maximum_bytes_billed,self.config['maximum_bytes_billed'])
        self.assertFalse(job.use_query_cache)
        self.assertEqual(call.kwargs['location'],'asia-south1')
        self.assertIn('sku_daily_sales',call.args[0]);self.assertNotIn('embeddings',call.args[0])
        for mutation in ('INSERT','UPDATE','DELETE','MERGE','CREATE','DROP'):
            self.assertNotIn(mutation,call.args[0])
        self.client.create_table.assert_not_called();self.client.load_table_from_json.assert_not_called()

    def test_capture_binds_native_job_and_fresh_observation(self):
        captured=capture_sales_analytics(self.client,self.sku,self.config)
        self.assertEqual(captured.query_job_id,'bq-sales-read-1')
        self.assertEqual((captured.project,captured.location),(PROJECT,'asia-south1'))
        self.assertEqual(captured.packet['canonical_sku'],self.sku)
        self.assertIsNotNone(captured.observed_at.utcoffset())

    def test_capture_rejects_wrong_native_job_identity(self):
        for field,value in (('job_id','bad job'),('project','other'),('location','us-central1')):
            setattr(self.client.query.return_value,field,value)
            with self.subTest(field=field),self.assertRaises(ValueError):
                capture_sales_analytics(self.client,self.sku,self.config)
            setattr(self.client.query.return_value,field,
                'bq-sales-read-1' if field=='job_id' else PROJECT if field=='project' else 'asia-south1')

    def test_bigquery_failure_has_no_local_audit_or_export_fallback(self):
        sql = SQL.read_text()
        def read_sql_only(path,*args,**kwargs):
            self.assertEqual(path,SQL)
            return sql
        with patch('pathlib.Path.read_text',new=read_sql_only):
            self.client.query.side_effect = RuntimeError('BigQuery unavailable')
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_sales_analytics(self.client,self.sku,self.config)


if __name__ == '__main__':
    unittest.main()
