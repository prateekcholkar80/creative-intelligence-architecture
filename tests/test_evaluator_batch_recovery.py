import hashlib
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_evaluator_store import EvaluatorStore,canonical,PROJECT,LOCATION


class EvaluatorBatchTests(TestCase):
    def fixture(self):
        c=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None);s=EvaluatorStore(c);s.preflight=Mock()
        intent=dict(record_id='a'*64,version='test');stored=dict(intent,reservation_nonce='original')
        outcome=dict(record_id=intent['record_id'],intent_sha256=hashlib.sha256(canonical(stored).encode()).hexdigest(),state='SUCCEEDED')
        rows=[dict(kind=k,record_id=intent['record_id'],record_json=canonical(v)) for k,v in (('intents',stored),('outcomes',outcome))]
        c.query.return_value.result.return_value=rows;c.query.return_value.state='DONE';c.query.return_value.error_result=None
        c.get_job.return_value.state='DONE';c.get_job.return_value.error_result=None
        return s,intent,stored,outcome,rows

    def test_one_current_snapshot_preserves_original_nonce_and_checks_both_jobs(self):
        s,i,stored,outcome,_=self.fixture()
        with patch('src.application.campaign_evaluator_store.matches_sharing_job',return_value=True):
            self.assertEqual(s.recover_batch((i,)),((stored,outcome),))
        self.assertEqual(s.client.query.call_count,1);self.assertEqual(s.client.get_job.call_count,2);s.preflight.assert_called_once()
        self.assertIn('UNION ALL',s.client.query.call_args.args[0]);self.assertFalse(s.client.query.call_args.kwargs['job_config'].use_query_cache)

    def test_missing_unknown_or_orphan_are_distinct(self):
        for kinds,wanted in [((),None),(('intents',),'UNKNOWN'),(('outcomes',),'DENIED')]:
            s,i,stored,_,rows=self.fixture();s.client.query.return_value.result.return_value=[r for r in rows if r['kind'] in kinds]
            with patch('src.application.campaign_evaluator_store.matches_sharing_job',return_value=True):
                if wanted=='DENIED':
                    with self.assertRaisesRegex(ValueError,'ORPHAN'):s.recover_batch((i,))
                else:
                    actual=s.recover_batch((i,))[0]
                    self.assertEqual(actual,(None,None) if wanted is None else (stored,dict(state='UNKNOWN',record_id=i['record_id'],release_allowed=False)))

    def test_duplicate_corrupt_conflicting_or_mismatched_original_jobs_deny(self):
        for fault in ('duplicate','corrupt','intent','native','outcome'):
            s,i,stored,o,rows=self.fixture()
            if fault=='duplicate':rows.append(rows[0])
            elif fault=='corrupt':rows[0]['record_json']=' '+rows[0]['record_json']
            elif fault=='intent':i=dict(i,version='changed')
            elif fault=='outcome':o['intent_sha256']='f'*64;rows[1]['record_json']=canonical(o)
            with patch('src.application.campaign_evaluator_store.matches_sharing_job',side_effect=lambda job,**kw: not (fault=='native' and kw['job_id'].startswith('evaluator_outcomes'))):
                with self.assertRaises(ValueError):s.recover_batch((i,))
        s,i,*_=self.fixture()
        with self.assertRaisesRegex(ValueError,'DUPLICATE_INPUT'):s.recover_batch((i,i))
        s.client.query.assert_not_called()
