import json
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_attachment_batch import ProtectedAttachmentBatches
from src.application.campaign_evidence_gaps import canonical
from tests import test_campaign_attachment_service as fixture


class AttachmentBatchTests(TestCase):
    def setUp(self):
        self.f=fixture.AttachmentServiceTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.rows={};self.f.store.bucket='private-bucket';self.f.store.client=Mock()
        self.f.store.put_original.side_effect=lambda sid,raw,meta:dict(bucket='private-bucket',object_name='campaign-evidence/'+sid,generation='123')
        self.f.store.record.side_effect=lambda sid,record:self.rows.setdefault(sid,canonical(record))
        self.batch=ProtectedAttachmentBatches(self.f.service)
        self.batch._rows=self.read_rows
        self.args=self.f.args();self.args.pop('choice');self.args.pop('filename');self.args.pop('content')
        self.args['files']=(('first.txt',b'one'),('second.json',b'{}'),('third.csv',b'a,b\n1,2'))
    def read_rows(self,sql,values):
        if 'id' in values:return tuple(dict(record_json=self.rows[values['id']]) for _ in [0] if values['id'] in self.rows)
        if 'ids' in values:
            return tuple(dict(submission_id=sid,record_json=self.rows[sid]) for sid in json.loads(values['ids']) if sid in self.rows)
        return tuple(dict(submission_id=sid,record_json=raw) for sid,raw in self.rows.items() if sid.startswith('batch_v1_'))
    def start(self):return self.batch.start({},**self.args)
    def scope(self,bid):return dict(workspace_id='workspace',conversation_id='conversation',batch_id=bid)
    def supply_original(self):
        raws={name:raw for name,raw in self.args['files']}
        blobs={}
        for text in self.rows.values():
            m=json.loads(text)
            if m.get('version')=='campaign-evidence-batch-v1':
                for e in m['files']:
                    blob=Mock();blob.download_as_bytes.return_value=raws[e['file']['filename']];blobs[e['original']['object_name']]=blob
        self.f.store.storage.bucket.return_value.blob.side_effect=lambda name,**kw:blobs[name]
    def test_all_originals_and_manifest_precede_first_receipt_and_cold_resume(self):
        bid=self.start();self.assertEqual(self.f.store.put_original.call_count,3)
        self.assertEqual(len(self.rows),1);self.assertTrue(bid.startswith('batch_v1_'))
        self.supply_original();state=self.batch.advance({},**self.scope(bid))
        self.assertEqual([r['saved'] for r in state],[True,False,False])
        cold=ProtectedAttachmentBatches(self.f.service);cold._rows=self.read_rows
        state=cold.advance({},**self.scope(bid))
        self.assertEqual([r['saved'] for r in state],[True,True,False])
        state=cold.advance({},**self.scope(bid));self.assertTrue(all(r['saved'] for r in state))
        count=self.f.store.put_original.call_count
        cold.advance({},**self.scope(bid));self.assertEqual(count,self.f.store.put_original.call_count)
    def test_lost_response_recovers_committed_original_without_resubmitting_file(self):
        bid=self.start();self.supply_original();original=self.f.store.record.side_effect;lost=[False]
        def record(sid,value):
            original(sid,value)
            if not sid.startswith('batch_v1_') and not lost[0]:lost[0]=True;raise TimeoutError()
        self.f.store.record.side_effect=record
        with self.assertRaises(TimeoutError):self.batch.advance({},**self.scope(bid))
        state=self.batch.state({},**self.scope(bid));self.assertTrue(state[0]['saved'])
        self.batch.advance({},**self.scope(bid))
        self.assertEqual(self.f.store.put_original.call_count,5) # 3 staged, then first and second.
    def test_invalid_file_has_no_staging_or_manifest_and_tampered_manifest_denies(self):
        self.args['files']+=(('invalid.webp',b'fake'),)
        with self.assertRaises((ValueError,OSError)):self.start()
        self.f.store.put_original.assert_not_called();self.f.store.record.assert_not_called()
        self.args['files']=self.args['files'][:3];bid=self.start()
        m=json.loads(self.rows[bid]);m['files'][0]['file']['filename']='changed.txt';self.rows[bid]=canonical(m)
        with self.assertRaises(ValueError):self.batch.state({},**self.scope(bid))
    def test_source_drift_and_missing_original_job_deny(self):
        bid=self.start();self.supply_original()
        self.f.data['scope']['sku']='CHANGED'
        with self.assertRaises(ValueError):self.batch.advance({},**self.scope(bid))
        self.assertEqual(len(self.rows),1)
        self.f.store.client.get_job.side_effect=RuntimeError('missing original job')
        with self.assertRaises(RuntimeError):self.batch.state({},**self.scope(bid))
    def test_batch_rows_are_outside_per_file_history_scope(self):
        bid=self.start();m=json.loads(self.rows[bid])
        self.assertNotIn('scope',m);self.assertIn('batch_scope',m)
        self.assertFalse(m['generation_authorized'])

    def test_changed_staged_bytes_or_revoked_access_never_publish_file_receipt(self):
        bid=self.start();self.supply_original()
        self.f.store.storage.bucket.return_value.blob.side_effect=None
        self.f.store.storage.bucket.return_value.blob.return_value.download_as_bytes.return_value=b'changed'
        with self.assertRaises(ValueError):self.batch.advance({},**self.scope(bid))
        self.assertEqual(len(self.rows),1)
        from dataclasses import replace
        from src.application.access_policy import Action
        self.f.access.authorize.return_value=replace(self.f.read,action=Action.REFINE_DRAFT,allowed=False)
        with self.assertRaises(ValueError):self.batch.advance({},**self.scope(bid))
        self.assertEqual(len(self.rows),1)

    def test_seven_file_batch_recovers_after_every_browser_step(self):
        self.args['files']=tuple((f'image-reference-{i}.txt',str(i).encode()) for i in range(7))
        bid=self.start();self.supply_original()
        for completed in range(1,8):
            cold=ProtectedAttachmentBatches(self.f.service);cold._rows=self.read_rows
            state=cold.advance({},**self.scope(bid))
            self.assertEqual(sum(r['saved'] for r in state),completed)
        self.assertEqual(len(self.rows),8) # One durable intent and seven original receipts.
