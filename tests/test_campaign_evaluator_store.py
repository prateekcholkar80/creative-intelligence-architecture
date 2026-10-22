import hashlib
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from src.application.campaign_evaluator_store import EvaluatorStore,Reservation,canonical,write_sql,table_metadata,PROJECT,LOCATION


class EvaluatorStoreTests(TestCase):
    def store(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        return EvaluatorStore(client)
    def test_raw_tables_have_30_day_partition_expiry_and_required_filters(self):
        for kind in ('intents','outcomes'):
            table=table_metadata(kind)
            self.assertEqual(table.time_partitioning.expiration_ms,30*86400000)
            self.assertTrue(table.require_partition_filter)
            self.assertIn('INTERVAL 30 DAY',write_sql(kind))
        self.assertIn('EVALUATOR_INTENT_CHANGED',write_sql('outcomes'))
    def test_lost_reservation_response_does_not_return_dispatch_permission(self):
        store=self.store();store.preflight=Mock();store.client.get_job.side_effect=NotFound('missing')
        store.client.query.side_effect=TimeoutError('lost')
        with self.assertRaises(TimeoutError):store.reserve(dict(record_id='a'*64),nonce='nonce')
        self.assertEqual(store.client.query.call_count,1)
    def test_stable_slot_conflict_does_not_submit_new_job(self):
        store=self.store();store.preflight=Mock()
        with patch('src.application.campaign_evaluator_store.matches_sharing_job',return_value=False),self.assertRaisesRegex(ValueError,'EVALUATOR_ORIGINAL_JOB_CONFLICT'):
            store.reserve(dict(record_id='a'*64),nonce='other')
        store.client.query.assert_not_called()
    def test_recovery_is_read_only_and_missing_outcome_is_unknown(self):
        store=self.store();store.preflight=Mock();intent=dict(record_id='a'*64)
        store._read=Mock(side_effect=[dict(intent,reservation_nonce='original'),None])
        with patch('src.application.campaign_evaluator_store.matches_sharing_job',return_value=True):
            store.client.get_job.return_value.state='DONE';store.client.get_job.return_value.error_result=None
            self.assertEqual(store.recover(intent)['state'],'UNKNOWN')
        store.client.query.assert_not_called()
    def test_outcome_cannot_be_rebound_to_another_intent(self):
        store=self.store();raw=canonical(dict(record_id='a'*64));reservation=Reservation('a'*64,raw,True)
        with self.assertRaises(ValueError):store.publish(reservation,dict(record_id='a'*64,intent_sha256='b'*64))
