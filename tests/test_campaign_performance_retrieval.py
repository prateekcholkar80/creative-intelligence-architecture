import json
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import Mock, patch

from src.generators.sales_generator import PROJECT
from src.retrieval.campaign_performance import (
    ATTRIBUTION_MODEL, SQL, build_packet, capture_campaign_performance, json_value,
    retrieve_campaign_performance,
)


class CampaignPerformanceRetrievalTests(unittest.TestCase):
    def setUp(self):
        self.sku = 'CI-SKU-0001'
        self.id = f'SYN-CMP-20250101-{self.sku}-01'
        # Two unequal days: 20/100 and 10/1000 clicks/impressions.
        # Aggregate CTR is 30/1100, not the average daily CTR of 0.105.
        self.row = dict(campaign_id=self.id,asset_id=f'SYN-AST-{self.id}-template-v1',sku=self.sku,
            campaign_name='Synthetic reference',objective='AWARENESS',channel='PAID_SOCIAL',audience='Source audience',
            start_date=date(2025,2,1),end_date=date(2025,2,2),budget=Decimal('40.00'),currency='EUR',
            snapshot_id='current-snapshot',expected_days=2,performance_rows=2,performance_days=2,
            first_performance_date=date(2025,2,1),last_performance_date=date(2025,2,2),
            impressions=1100,clicks=30,conversions=3,attributed_units=4,spend=Decimal('40.00'),
            attributed_revenue=Decimal('80.00'),ctr=Decimal('0.027272727'),
            conversion_rate=Decimal('0.100000000'),roas=Decimal('2.000000000'),invalid_rows=0,
            source_system='SYNTHETIC_POC',attribution_model=ATTRIBUTION_MODEL)
        self.client = Mock();self.client.query.return_value.result.return_value = [self.row]
        self.client.query.return_value.job_id='bq-performance-read-1'
        self.client.query.return_value.project=PROJECT
        self.client.query.return_value.location='asia-south1'

    def test_grounded_totals_ratios_provenance_and_evidence(self):
        packet = retrieve_campaign_performance(self.client,self.sku)
        self.assertEqual(packet['source_of_truth'],'BIGQUERY')
        self.assertEqual(packet['source_system'],'SYNTHETIC_POC')
        summary = packet['summaries'][0]
        self.assertEqual(summary['spend'],Decimal('40.00'))
        self.assertEqual(summary['ctr'],Decimal('0.027272727'))
        self.assertEqual(summary['conversion_rate'],Decimal('0.1'))
        self.assertEqual(summary['roas'],Decimal('2'))
        self.assertEqual(len(summary['evidence_references']),3)
        self.assertIn('source_system=SYNTHETIC_POC',summary['evidence_references'][-1])
        self.assertNotIn('evidence_references',self.row)
        self.assertNotIn('raw_daily_rows',packet)

    def test_ratios_averaged_from_daily_rows_are_rejected(self):
        for name,value in (('ctr',Decimal('0.105')),('conversion_rate',Decimal('0.2')),('roas',Decimal('1.5'))):
            with self.subTest(metric=name),self.assertRaisesRegex(ValueError,'aggregate totals'):
                build_packet(self.sku,[{**self.row,name:value}])

    def test_zero_denominators_are_null_and_money_stays_exact(self):
        row = dict(self.row,impressions=0,clicks=0,conversions=0,attributed_units=0,
                   budget=Decimal('0'),spend=Decimal('0'),attributed_revenue=Decimal('0'),
                   ctr=None,conversion_rate=None,roas=None)
        packet = build_packet(self.sku,[row])
        self.assertIsNone(packet['summaries'][0]['ctr'])
        self.assertIsNone(packet['summaries'][0]['roas'])
        with self.assertRaises(ValueError):build_packet(self.sku,[{**row,'ctr':Decimal('0')}])
        value = Decimal('1234567890123456789.123456789')
        self.assertEqual(json.loads(json.dumps({'money':value},default=json_value))['money'],str(value))
        self.assertEqual(json_value(date(2025,2,1)),'2025-02-01')
        with self.assertRaises(TypeError):json_value(object())

    def test_missing_duplicate_and_out_of_range_daily_coverage_fail(self):
        for key,value in (('performance_rows',0),('performance_rows',3),('performance_days',1),
                          ('first_performance_date',date(2025,1,31)),('last_performance_date',date(2025,2,3)),
                          ('invalid_rows',1),('expected_days',3),('spend',Decimal('39.99'))):
            with self.subTest(field=key),self.assertRaises(ValueError):
                build_packet(self.sku,[{**self.row,key:value}])

    def test_invalid_identity_scope_counts_money_or_model_fail(self):
        for key,value in (('sku','CI-SKU-0002'),('asset_id','other'),('source_system','ACTUAL'),
                          ('attribution_model','UNKNOWN'),('snapshot_id',''),('clicks',1200),
                          ('conversions',31),('attributed_units',2),('impressions',True),
                          ('attributed_revenue',float('nan')),('spend',40.0),
                          ('attributed_revenue',Decimal('NaN')),('budget',Decimal('-1')),
                          ('ctr',float('nan')),('roas',Decimal('Infinity'))):
            with self.subTest(field=key),self.assertRaises(ValueError):
                build_packet(self.sku,[{**self.row,key:value}])

    def test_explicit_ids_must_all_be_present_and_unique_in_same_snapshot(self):
        with self.assertRaises(ValueError):build_packet(self.sku,[self.row,self.row])
        with self.assertRaisesRegex(ValueError,'missing'):
            build_packet(self.sku,[],[self.id])
        second_id = f'SYN-CMP-20250101-{self.sku}-02'
        second = dict(self.row,campaign_id=second_id,asset_id=f'SYN-AST-{second_id}-template-v1',snapshot_id='old')
        with self.assertRaises(ValueError):build_packet(self.sku,[self.row,second])
        self.assertEqual(build_packet(self.sku,[])['summaries'],[])

    def test_explicit_empty_selection_never_expands_to_all_campaigns(self):
        self.assertEqual(retrieve_campaign_performance(self.client,self.sku,[])['summaries'],[])
        self.client.query.assert_not_called()

    def test_bad_ids_sku_or_billing_caps_stop_before_cloud_access(self):
        for sku,ids in (('OTHER',None),(self.sku,[self.id,self.id]),
                        (self.sku,['SYN-CMP-20250101-CI-SKU-0002-01']),
                        (self.sku,'not a list'),(self.sku,['bad']), (self.sku,[None])):
            with self.assertRaises(ValueError):retrieve_campaign_performance(self.client,sku,ids)
        for cap in (0,-1,True,'unlimited'):
            with self.assertRaises(ValueError):retrieve_campaign_performance(self.client,self.sku,None,cap)
        self.client.query.assert_not_called()

    def test_parameterized_read_only_structured_query_no_daily_ratio_averaging(self):
        retrieve_campaign_performance(self.client,self.sku,[self.id],150_000_000)
        call = self.client.query.call_args;job = call.kwargs['job_config']
        params = {p.name:p for p in job.query_parameters}
        self.assertEqual(params['sku'].value,self.sku)
        self.assertFalse(params['all_campaigns'].value)
        self.assertEqual(params['campaign_ids'].values,[self.id])
        self.assertEqual(job.maximum_bytes_billed,150_000_000)
        self.assertFalse(job.use_query_cache)
        self.assertEqual(call.kwargs['location'],'asia-south1')
        self.assertIn('campaign_daily_performance',call.args[0])
        self.assertNotIn('AVG(',call.args[0])
        self.assertNotIn('embeddings',call.args[0])
        for mutation in ('INSERT','UPDATE','DELETE','MERGE','CREATE','DROP'):
            self.assertNotIn(mutation,call.args[0])
        self.client.create_table.assert_not_called();self.client.load_table_from_json.assert_not_called()

    def test_capture_binds_native_job_and_fresh_observation(self):
        captured=capture_campaign_performance(self.client,self.sku,[self.id])
        self.assertEqual(captured.query_job_id,'bq-performance-read-1')
        self.assertEqual((captured.project,captured.location),(PROJECT,'asia-south1'))
        self.assertEqual(captured.packet['summaries'][0]['campaign_id'],self.id)
        self.assertIsNotNone(captured.observed_at.utcoffset())

    def test_capture_requires_query_and_exact_native_identity(self):
        with self.assertRaises(ValueError):capture_campaign_performance(self.client,self.sku,[])
        self.client.query.assert_not_called()
        for field,value in (('job_id','bad job'),('project','other'),('location','us-central1')):
            setattr(self.client.query.return_value,field,value)
            with self.subTest(field=field),self.assertRaises(ValueError):
                capture_campaign_performance(self.client,self.sku,[self.id])
            setattr(self.client.query.return_value,field,
                'bq-performance-read-1' if field=='job_id' else PROJECT if field=='project' else 'asia-south1')

    def test_bigquery_failure_propagates_without_export_fallback(self):
        sql = SQL.read_text()
        def read_sql_only(path,*args,**kwargs):
            self.assertEqual(path,SQL)
            return sql
        with patch('pathlib.Path.read_text',new=read_sql_only):
            self.client.query.side_effect = RuntimeError('BigQuery unavailable')
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_campaign_performance(self.client,self.sku)


if __name__ == '__main__':
    unittest.main()
