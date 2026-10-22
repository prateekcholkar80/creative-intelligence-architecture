from copy import deepcopy
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock,patch
from google.cloud import bigquery
import hashlib,json

from src.validation import upload_corpus_runtime_access as p


class UploadCorpusRuntimeAccessTests(TestCase):
    def selected(self):return json.loads(Path('config/evaluation/rejuvenate_upload_corpus.json').read_text())

    def test_selection_is_pinned_and_rejects_duplicates_or_scope_extensions(self):
        value=self.selected();self.assertEqual(p.selection(value),value)
        for change in ('duplicate','extra_scope','mime','generation'):
            v=deepcopy(value)
            if change=='duplicate':v['sources'].append(v['sources'][0])
            elif change=='extra_scope':v['scope']['principal_id']='invented'
            elif change=='mime':v['sources'][1]['content_type']='image/png'
            else:v['sources'][0]['generation']='0'
            with self.subTest(change=change),self.assertRaises(ValueError):p.selection(v)

    def test_wrong_runtime_creator_denies_before_any_source_or_gcs_read(self):
        client=Mock(project=p.PROJECT,location=p.LOCATION,default_query_job_config=None);gcs=Mock()
        def query(sql,**kw):
            config=kw['job_config'].to_api_repr();config['query']['query']=sql
            job=bigquery.QueryJob.from_api_repr(dict(jobReference=dict(projectId=p.PROJECT,location=p.LOCATION,
                jobId=kw['job_id']),configuration=config,status=dict(state='DONE'),user_email='admin@example.com'),client)
            job.result=Mock(return_value=[dict(probe=p.VERSION)]);client.get_job.return_value=job;return job
        client.query.side_effect=query
        with patch.object(p,'DocumentStore') as store,self.assertRaisesRegex(ValueError,'UPLOAD_ACCESS_RUNTIME_IDENTITY'):
            p.verify_access(client,gcs,self.selected())
        store.assert_not_called();gcs.bucket.assert_not_called()

    def test_probe_handles_verified_empty_extraction_lookup_without_writing(self):
        # Native SQL correlation is tested through the real sharing-job helper;
        # the source store is synthetic here, not effective cloud access proof.
        selected=self.selected();content={s['submission_id']:(b'image' if s['role']=='BOUND_IMAGE' else b'text') for s in selected['sources']}
        for s in selected['sources']:s['content_sha256']=hashlib.sha256(content[s['submission_id']]).hexdigest()
        scope=selected['scope'];sources=[]
        for s in selected['sources']:
            sources.append(dict(scope=scope,principal_id='owner',file=dict(bytes=len(content[s['submission_id']]),
                content_sha256=s['content_sha256'],content_type=s['content_type']),original=dict(bucket=p.BUCKET,
                object_name='campaign-evidence/'+s['submission_id'],generation=s['generation']),review_status='UNVERIFIED'))
        manifest=dict(context=dict(scope=scope),sources=[dict(submission_id=selected['sources'][0]['submission_id'])],
            principal_id='owner',claim_approval=False,eligible_for_generation=False)
        raw=p.canonical(manifest);selected['image_binding_id']='eval_upload_binding_v1_'+hashlib.sha256(raw.encode()).hexdigest()
        client=Mock(project=p.PROJECT,location=p.LOCATION,default_query_job_config=None);gcs=Mock()
        def query(sql,**kw):
            config=kw['job_config'].to_api_repr();config['query']['query']=sql
            job=bigquery.QueryJob.from_api_repr(dict(jobReference=dict(projectId=p.PROJECT,location=p.LOCATION,
                jobId=kw['job_id']),configuration=config,status=dict(state='DONE'),user_email=p.RUNTIME),client)
            job.result=Mock(return_value=[dict(probe=p.VERSION)]);client.get_job.return_value=job;return job
        client.query.side_effect=query
        blobs={}
        for source in sources:
            sid=source['original']['object_name'].split('/')[-1];f=source['file']
            blobs[source['original']['object_name']]=Mock(generation=int(source['original']['generation']),
                content_type=f['content_type'],size=f['bytes'],download_as_bytes=Mock(return_value=content[sid]))
        gcs.bucket.return_value.blob.side_effect=lambda name:blobs[name]
        with patch.object(p,'DocumentStore') as cls,patch.object(p,'matches_sharing_job',return_value=True):
            store=cls.return_value;store.query.return_value=({'record_json':raw},)
            store.submission.side_effect=sources;store.find_extraction.return_value=None
            result=p.verify_access(client,gcs,selected)
            self.assertEqual(result['status'],'RUNTIME_INFRASTRUCTURE_READS_VERIFIED')
            self.assertTrue(all(s['extraction_state']=='NOT_STORED' for s in result['sources'][1:]))
            self.assertEqual(result['iap_user_authentication'],'NOT_TESTED')
            self.assertEqual(result['data_writes'],0);store.save_extraction.assert_not_called()
            self.assertNotIn('owner',json.dumps(result));self.assertNotIn('response_text',json.dumps(result))
