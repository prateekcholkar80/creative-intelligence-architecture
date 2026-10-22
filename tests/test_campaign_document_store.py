from unittest import TestCase
from unittest.mock import Mock
from google.cloud import bigquery
from src.application.campaign_document_store import DocumentStore,PROJECT,LOCATION
from src.application.campaign_evidence_gaps import canonical


class DocumentStoreTests(TestCase):
    def store(self):return DocumentStore(Mock(project=PROJECT,location=LOCATION,default_query_job_config=None))

    def test_extraction_is_canonical_idempotent_and_source_bound(self):
        store=self.store();store.preflight=Mock()
        store.query=Mock(side_effect=lambda sql,params,**kw:({'record_json':next(p.value for p in params if p.name=='record')},))
        record=dict(submission_id='a'*64,eligible_for_generation=False)
        first=store.save_extraction(record,submission={'source':'original'})
        self.assertEqual(first,store.save_extraction(record,submission={'source':'original'}))
        sql=store.query.call_args.args[0]
        self.assertIn('DOCUMENT_SOURCE_CHANGED',sql)
        self.assertNotIn('DELETE',sql)
        self.assertEqual(store.query.call_args.kwargs['job_id'],'document_extract_v1_'+first)

    def test_corrupt_or_duplicate_extraction_never_releases(self):
        store=self.store();store.preflight=Mock();store.query=Mock(return_value=({'record_json':'{}'},))
        with self.assertRaises(ValueError):store.read('extractions','a'*64)
        store.query.return_value=({'record_json':'{}'},{'record_json':'{}'})
        with self.assertRaises(ValueError):store.read('extractions','a'*64)

    def test_unknown_job_mismatch_does_not_submit_another_job(self):
        store=self.store()
        with self.assertRaises(ValueError):store.query('SELECT 1',[],job_id='fixed',write=True)
        store.client.query.assert_not_called()

    def test_extraction_statement_preserves_existing_native_job_identity(self):
        import hashlib
        from src.application.campaign_document_store import extraction_sql
        self.assertEqual(hashlib.sha256(extraction_sql().encode()).hexdigest(),
            'e5d4e66575c0ce288916ccf9bf783f80ece86e20a726feb314098322fcdae76f')

    def test_original_extraction_verification_uses_read_only_native_job(self):
        import hashlib
        from src.application.campaign_document_store import extraction_sql
        store=self.store();record=dict(submission_id='a'*64);raw=canonical(record)
        eid=hashlib.sha256(raw.encode()).hexdigest();source=dict(source='original')
        def native(job_id,**kw):
            cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
                maximum_bytes_billed=256*1024**2,job_timeout_ms=60000,query_parameters=[
                    bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in dict(id=eid,record=raw,
                        submission_id=record['submission_id'],source=canonical(source)).items()])
            config=cfg.to_api_repr();config['query']['query']=extraction_sql()
            job=bigquery.QueryJob.from_api_repr(dict(jobReference=dict(projectId=PROJECT,location=LOCATION,jobId=job_id),
                configuration=config,status=dict(state='DONE')),store.client)
            job.result=Mock(return_value=({'record_json':raw},));return job
        store.client.get_job.side_effect=native
        self.assertEqual(store.verify_extraction(eid,record,submission=source),'document_extract_v1_'+eid)
        store.client.query.assert_not_called()
        store.client.get_job.side_effect=None
        original=native('document_extract_v1_'+eid)
        store.client.get_job.return_value=original
        original.result.side_effect=PermissionError('anonymous result unavailable')
        self.assertEqual(store.verify_extraction(eid,record,submission=source),'document_extract_v1_'+eid)
        original.result.assert_not_called()
        store.client.get_job.side_effect=None;store.client.get_job.return_value=Mock()
        with self.assertRaises(ValueError):store.verify_extraction(eid,record,submission=source)
