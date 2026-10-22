from unittest import TestCase
from unittest.mock import Mock
from google.api_core.exceptions import NotFound,PreconditionFailed
from google.cloud import bigquery
from src.application import campaign_attachment_service as module
from src.application.campaign_evidence_gaps import canonical
from src.ingestion.setup_campaign_attachments import setup


class AttachmentStorageTests(TestCase):
    def client(self):return Mock(project=module.PROJECT,location=module.LOCATION,default_query_job_config=None)

    def job(self,client,job_id,config,raw):
        job=bigquery.QueryJob(job_id,module.SQL,client=client,job_config=config)
        job._properties['jobReference'].update(projectId=module.PROJECT,location=module.LOCATION)
        job._properties['status']=dict(state='DONE')
        job.result=Mock(return_value=[dict(record_json=raw)])
        return job

    def test_single_original_job_and_cold_recovery_do_not_resubmit(self):
        client=self.client();jobs={};submissions=[]
        def get(job_id,**kw):
            if job_id not in jobs:raise NotFound('absent')
            return jobs[job_id]
        def query(sql,**kw):
            submissions.append(kw['job_id']);raw=kw['job_config'].query_parameters[1].value
            jobs[kw['job_id']]=self.job(client,kw['job_id'],kw['job_config'],raw)
            return jobs[kw['job_id']]
        client.get_job.side_effect=get;client.query.side_effect=query
        store=module.AttachmentStore(client=client,storage=Mock(),bucket='evidence-bucket')
        store.record('a'*64,dict(review_status='UNVERIFIED'))
        module.AttachmentStore(client=client,storage=Mock(),bucket='evidence-bucket').record('a'*64,dict(review_status='UNVERIFIED'))
        self.assertEqual(len(submissions),1)
        with self.assertRaisesRegex(ValueError,'ATTACHMENT_JOB_MISMATCH'):
            store.record('a'*64,dict(review_status='APPROVED'))
        self.assertEqual(len(submissions),1)

    def test_lost_query_response_recovers_only_original_job(self):
        client=self.client();jobs={}
        client.get_job.side_effect=lambda job_id,**kw: jobs[job_id] if job_id in jobs else (_ for _ in ()).throw(NotFound('absent'))
        def query(sql,**kw):
            jobs[kw['job_id']]=self.job(client,kw['job_id'],kw['job_config'],canonical({'status':'UNVERIFIED'}))
            raise TimeoutError('lost acknowledgement')
        client.query.side_effect=query
        store=module.AttachmentStore(client=client,storage=Mock(),bucket='evidence-bucket')
        with self.assertRaises(TimeoutError):store.record('a'*64,{'status':'UNVERIFIED'})
        store.record('a'*64,{'status':'UNVERIFIED'})
        self.assertEqual(client.query.call_count,1)

    def test_existing_gcs_generation_must_match_original_bytes(self):
        storage=Mock();blob=storage.bucket.return_value.blob.return_value
        blob.upload_from_string.side_effect=PreconditionFailed('exists')
        blob.size=4;blob.content_type='text/plain';blob.generation=7;blob.name='private'
        blob.download_as_bytes.return_value=b'text'
        store=module.AttachmentStore(client=self.client(),storage=storage,bucket='evidence-bucket')
        meta=module.inspect_file('data.txt',b'text','REFERENCE_DATA')
        self.assertEqual(store.put_original('a'*64,b'text',meta)['generation'],'7')
        self.assertEqual(blob.download_as_bytes.call_args.kwargs['if_generation_match'],7)
        blob.download_as_bytes.return_value=b'fake'
        with self.assertRaisesRegex(ValueError,'ATTACHMENT_OBJECT_CHANGED'):store.put_original('a'*64,b'text',meta)

    def test_setup_check_never_creates_and_apply_preserves_existing_table(self):
        client=self.client();client.get_table.side_effect=NotFound('missing')
        self.assertEqual(setup(client)['status'],'MISSING');client.create_table.assert_not_called()
        table=module.table_metadata();table._properties.update(location=module.LOCATION,type='TABLE')
        client.get_table.side_effect=[NotFound('missing'),table]
        self.assertTrue(setup(client,apply=True)['created'])
        client.get_table.side_effect=None;client.get_table.return_value=table
        self.assertFalse(setup(client,apply=True)['created'])
        self.assertEqual(client.create_table.call_count,1)

    def test_history_verifies_scope_and_original_metadata_hash(self):
        import hashlib
        from datetime import datetime,timezone
        client=self.client();base=dict(scope=dict(workspace_id='w',conversation_id='c',request_id='r'),
            principal_id='pilot',eligible_for_generation=False,review_status='UNVERIFIED',
            selection=dict(status='UNVERIFIED'),file=dict(filename='document.txt'))
        sid=hashlib.sha256(canonical(base).encode()).hexdigest()
        raw=dict(submission_id=sid,record_json=canonical(base),recorded_at=datetime.now(timezone.utc))
        def query(sql,**kw):
            job=bigquery.QueryJob(kw['job_id'],sql,client=client,job_config=kw['job_config'])
            job._properties['jobReference'].update(projectId=module.PROJECT,location=module.LOCATION)
            job._properties['status']=dict(state='DONE');job.result=Mock(return_value=[raw])
            client.get_job.return_value=job;return job
        client.query.side_effect=query
        store=module.AttachmentStore(client=client,storage=Mock(),bucket='evidence-bucket')
        args=dict(workspace_id='w',conversation_id='c',request_id='r',principal_id='pilot')
        self.assertEqual(store.summaries(**args)[0]['filename'],'document.txt')
        base['principal_id']='other';raw['record_json']=canonical(base)
        with self.assertRaises(ValueError):store.summaries(**args)
