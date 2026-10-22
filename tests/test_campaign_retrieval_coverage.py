from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest import TestCase
from unittest.mock import Mock

from google.cloud import bigquery
from src.retrieval import campaign_retrieval_coverage as r

NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)


def row():
    return dict(observed_at=NOW, products=[dict(sku='CI-SKU-0022', product_status='ACTIVE',
        source_system='NAKED_AND_THRIVING_OFFICIAL')], product_count=1,
        knowledge_chunks=0, text_vectors=0, image_vectors=0)


def client(rows=None, native_change=None):
    c = Mock(project=r.PROJECT, location=r.LOCATION, default_query_job_config=None)
    def query(sql, **kw):
        config = deepcopy(kw['job_config'].to_api_repr()); config['query']['query'] = sql
        resource = dict(jobReference=dict(projectId=r.PROJECT, location=r.LOCATION, jobId=kw['job_id']),
            configuration=config, status=dict(state='DONE'))
        if native_change: native_change(resource)
        c.get_job.return_value = bigquery.QueryJob.from_api_repr(resource, c)
        c.get_job.return_value.result = Mock(return_value=[row()] if rows is None else rows)
        return Mock(result=Mock(return_value=[row()] if rows is None else rows))
    c.query.side_effect = query
    return c


class RetrievalCoverageTests(TestCase):
    def read(self, c=None, clock=lambda: NOW):
        return r.capture_retrieval_coverage(c or client(), 'CI-SKU-0022', clock=clock)

    def test_verified_empty_is_not_a_quality_pass(self):
        c = client(); result = self.read(c)
        self.assertEqual(result['corpus_status'], 'CORPUS_EMPTY')
        self.assertEqual(result['retrieval_quality'], 'NOT_MEASURED')
        self.assertFalse(result['qualifying']); self.assertFalse(result['ranking_executed'])
        self.assertEqual(result['sha256'], r.sha({k:v for k,v in result.items() if k != 'sha256'}))
        kw = c.query.call_args.kwargs
        self.assertIsNone(kw['retry']); self.assertIsNone(kw['job_retry'])
        self.assertFalse(kw['job_config'].use_query_cache)
        self.assertEqual(kw['job_config'].query_parameters[0].value, 'CI-SKU-0022')

    def test_read_failures_missing_rows_and_duplicate_product_do_not_imply_empty(self):
        c = client(); c.query.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError): self.read(c)
        for rows in ([], [row(), row()], [dict(row(), product_count=2)],
                     [dict(row(), products=[])], [dict(row(), text_vectors=-1)],
                     [dict(row(), image_vectors=False)]):
            with self.subTest(rows=rows), self.assertRaises(ValueError): self.read(client(rows))

    def test_wrong_sku_status_and_source_deny(self):
        for change in (dict(sku='CI-SKU-0001'), dict(product_status='INACTIVE'), dict(source_system='BEAUTYFEEDS')):
            x=row(); x['products'][0].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError): self.read(client([x]))
        c=client()
        with self.assertRaises(ValueError): r.capture_retrieval_coverage(c, "x' OR TRUE")
        c.query.assert_not_called()

    def test_native_sql_scope_config_and_terminal_status_are_independent(self):
        changes = [lambda x:x['configuration']['query'].update(query='SELECT 1'),
            lambda x:x['configuration']['query'].update(useQueryCache=True),
            lambda x:x['jobReference'].update(location='us'),
            lambda x:x['status'].update(state='RUNNING'),
            lambda x:x['status'].update(errorResult=dict(reason='accessDenied'))]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError): self.read(client(native_change=change))

    def test_expired_or_regressed_capture_denies(self):
        for end in (NOW-timedelta(seconds=1), NOW+timedelta(seconds=60)):
            times=iter((NOW,end))
            with self.assertRaises(ValueError): self.read(clock=lambda:next(times))

    def test_counts_cannot_establish_eligibility(self):
        for counts,expected in ((dict(knowledge_chunks=1),'TEXT_VECTORS_MISSING'),
            (dict(text_vectors=1),'CORPUS_PRESENT_REQUIRES_ELIGIBILITY_AND_RANKING')):
            result=r.coverage_result('CI-SKU-0022',dict(row(),**counts))
            self.assertEqual(result['corpus_status'],expected)
            self.assertEqual(result['eligibility'],'NOT_ESTABLISHED_BY_COUNTS')

    def test_sdk_mutation_cannot_redefine_expected_native_configuration(self):
        c=client(); original=c.query.side_effect
        def mutate(sql,**kw):
            kw['job_config'].maximum_bytes_billed=1
            return original(sql,**kw)
        c.query.side_effect=mutate
        with self.assertRaises(ValueError): self.read(c)

    def test_historical_recovery_verifies_original_query_without_resubmission(self):
        c=client();value=self.read(c)
        self.assertEqual(r.verify_retrieval_coverage(c,value),value)
        c.query.assert_called_once()

    def test_changed_record_native_query_or_result_denies_recovery(self):
        for fault in ('record','query','rows'):
            c=client();value=self.read(c)
            if fault=='record':
                value['counts']['text_vectors']=1
                value['sha256']=r.sha({k:v for k,v in value.items() if k!='sha256'})
            elif fault=='query':c.get_job.return_value._properties['configuration']['query']['query']='SELECT 1'
            else:c.get_job.return_value.result.return_value=[]
            with self.subTest(fault=fault),self.assertRaises(ValueError):r.verify_retrieval_coverage(c,value)
