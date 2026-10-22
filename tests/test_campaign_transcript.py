from datetime import datetime,timedelta,timezone
import json
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_transcript as transcript
from src.application.campaign_planner_inputs import digest
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import canonical_json_bytes

class TranscriptTests(TestCase):
    def setUp(self):
        self.now=datetime.now(timezone.utc)
        self.authority=dict(transcript=[dict(request_id='r',role='USER',content_sha256=digest('hello'),invocation_id=None)])
        self.content=dict(authority=self.authority)
    def test_exact_hash_and_order_required(self):
        batch=transcript.encode_turns(self.authority,('hello',))
        self.assertEqual(json.loads(batch)[0]['text'],'hello')
        for values in ((),('changed',),('hello','extra')):
            with self.assertRaises(ValueError):transcript.encode_turns(self.authority,values)
    def test_transaction_contains_bodies_and_replay_check(self):
        sql,params=transcript.extend_publication('BEGIN TRANSACTION;\nINSERT INTO `'+transcript.DATASET_ID+
            '.campaign_request_authorities` SELECT 1;\nEND IF;\nCOMMIT TRANSACTION;',[],self.authority,('hello',))
        self.assertLess(sql.index('campaign_transcript_bodies'),sql.index('campaign_request_authorities'))
        self.assertIn('TRANSCRIPT_BODY_CONFLICT',sql)
        self.assertEqual(params[0].value,transcript.encode_turns(self.authority,('hello',)))
        spec=transcript.SPEC.table('project-661d501a-e709-4276-a26')
        self.assertEqual(spec.time_partitioning.expiration_ms,30*86400000)
        self.assertTrue(spec.require_partition_filter)
    def test_native_read_coverage_expiry_and_context_drift(self):
        def capture(job,content=None):
            c=content or self.content
            return ExecutionAuthorityRead(canonical_json_bytes(c).decode(),_hash(c),job,self.now,self.now+timedelta(seconds=60))
        io=Mock();row=dict(schema_version=transcript.VERSION,batch_canonical_json=transcript.encode_turns(self.authority,('hello',)),recorded_at=self.now)
        io.read.return_value={'bodies':[row]}
        with patch.object(transcript,'ControlIO',return_value=io),patch.object(transcript,'validate_table'),\
            patch.object(transcript,'read_execution_authority',side_effect=lambda *a,**kw:capture(str(io.read.call_count))):
            self.assertEqual(transcript.read_transcript(Mock(project='project-661d501a-e709-4276-a26'),identity=Mock(),workspace_id='w',conversation_id='c',request_id='r',clock=lambda:self.now),('hello',))
            for changed in (dict(row,recorded_at=self.now-timedelta(days=30)),dict(row,batch_canonical_json='[]')):
                io.read.return_value={'bodies':[changed]}
                with self.assertRaises(ValueError):transcript.read_transcript(Mock(project='project-661d501a-e709-4276-a26'),identity=Mock(),workspace_id='w',conversation_id='c',request_id='r',clock=lambda:self.now)

    def test_authority_change_after_body_read_withholds_text(self):
        def capture(job,sha):
            return ExecutionAuthorityRead(canonical_json_bytes(self.content).decode(),sha,job,self.now,self.now+timedelta(seconds=60))
        io=Mock();io.read.return_value={'bodies':[dict(schema_version=transcript.VERSION,
            batch_canonical_json=transcript.encode_turns(self.authority,('hello',)),recorded_at=self.now)]}
        with patch.object(transcript,'ControlIO',return_value=io),patch.object(transcript,'validate_table'),\
            patch.object(transcript,'read_execution_authority',side_effect=[capture('a',_hash(self.content)),capture('b','f'*64)]):
            with self.assertRaises(ValueError):transcript.read_transcript(Mock(project='project-661d501a-e709-4276-a26'),
                identity=Mock(),workspace_id='w',conversation_id='c',request_id='r',clock=lambda:self.now)
