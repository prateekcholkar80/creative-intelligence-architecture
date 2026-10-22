import copy,json,time
from unittest import TestCase
from unittest.mock import Mock
from src.application.operation_budget import worker_operation_scope
from src.retrieval.private_retrieval_measurement import DurableRetrievalMeasurement
from src.retrieval.private_retrieval_rank import BigQueryPrivateRanker
from tests.test_private_query_embedding import query_fixture
from tests.test_private_text_embeddings import ledger,request
from tests import test_private_retrieval_metrics as metrics_fixtures


class DurableMeasurementTests(TestCase):
    def fixture(self):
        store,intents,outcomes=ledger();q=query_fixture();obs,case,chunks,_=metrics_fixtures.RetrievalMetricsTests().fixture()
        data=json.loads(q.input_json)
        import hashlib
        obs.update(scope=data['scope'],corpus_sha256=data['corpus_sha256'],index_sha256=data['index_sha256'],
            query_text_sha256=hashlib.sha256(data['text'].encode()).hexdigest(),index_record_id=data['index_record_id'],native_job_id='native-fixture')
        case.update(case_id='fixture',corpus_sha256=obs['corpus_sha256'],index_sha256=obs['index_sha256'],
            query_text_sha256=obs['query_text_sha256'],scope={k:data['scope'][k] for k in ('sku','country','channel')})
        ranker=BigQueryPrivateRanker(store,Mock(return_value=True))
        ranker.capture=Mock(return_value=obs);ranker.verify_saved=Mock(return_value=True)
        runner=DurableRetrievalMeasurement(store,ranker,Mock(return_value=True),enabled=True)
        args=(q,(request(),),dict(record_id=data['index_record_id']),case,chunks)
        return runner,args,store,intents,outcomes

    def work(self,fn):
        with worker_operation_scope(time.monotonic()+30):fn()

    def test_commit_cold_recovery_never_repeats_ranking(self):
        def run():
            runner,args,store,_,_=self.fixture();first=runner.execute(*args)
            cold=DurableRetrievalMeasurement(store,runner.ranker,runner.guard)
            self.assertEqual(cold.execute(*args,recover_only=True),first)
            runner.ranker.capture.assert_called_once();self.assertFalse(first['qualifying'])
            self.assertEqual(store.reserve.call_count,1);self.assertEqual(store.publish.call_count,1)
        self.work(run)

    def test_lost_committed_response_cold_recovery(self):
        def run():
            runner,args,store,_,_=self.fixture();publish=store.publish.side_effect
            def lost(*a):publish(*a);raise TimeoutError('lost')
            store.publish.side_effect=lost
            with self.assertRaises(TimeoutError):runner.execute(*args)
            cold=DurableRetrievalMeasurement(store,runner.ranker,runner.guard)
            self.assertFalse(cold.execute(*args,recover_only=True)['release_allowed'])
            runner.ranker.capture.assert_called_once()
        self.work(run)

    def test_pending_uncommitted_publication_replays_exact_outcome(self):
        def run():
            runner,args,store,_,_=self.fixture();publish=store.publish.side_effect
            store.publish.side_effect=TimeoutError('before commit')
            with self.assertRaises(TimeoutError):runner.execute(*args)
            original=copy.deepcopy(store.publish.call_args.args[1]);store.publish.side_effect=publish
            runner.execute(*args,recover_only=True)
            self.assertEqual(store.publish.call_args.args[1],original);runner.ranker.capture.assert_called_once()
        self.work(run)

    def test_uncertain_native_rank_slot_is_not_run_again(self):
        def run():
            runner,args,store,_,_=self.fixture();runner.ranker.capture.side_effect=TimeoutError('uncertain read')
            with self.assertRaises(TimeoutError):runner.execute(*args)
            cold=DurableRetrievalMeasurement(store,runner.ranker,runner.guard,enabled=True)
            with self.assertRaisesRegex(ValueError,'OUTCOME_UNUSABLE'):cold.execute(*args)
            runner.ranker.capture.assert_called_once();self.assertEqual(store.reserve.call_count,1)
        self.work(run)

    def test_cold_recover_finishes_original_reserved_measurement_from_existing_ranking_only(self):
        def run():
            runner,args,store,intents,outcomes=self.fixture();saved=runner.ranker.capture.return_value
            runner.ranker.capture.side_effect=TimeoutError('lost original ranking response')
            with self.assertRaises(TimeoutError):runner.execute(*args)
            intent=runner.intent(*args[:3],args[3]);raw=intents[intent['record_id']]
            from src.application.campaign_evaluator_store import Reservation
            cold=DurableRetrievalMeasurement(store,runner.ranker,runner.guard)
            cold._original_reservation=Mock(return_value=Reservation(intent['record_id'],raw,False))
            runner.ranker.capture.side_effect=None;runner.ranker.capture.return_value=saved
            receipt=cold.execute(*args,recover_only=True)
            self.assertFalse(receipt['qualifying']);self.assertEqual(store.reserve.call_count,1)
            self.assertEqual(runner.ranker.capture.call_args.kwargs,dict(recover_only=True))
            self.assertEqual(store.publish.call_args.args[0].intent_json,raw)
            self.assertEqual(len(outcomes),1)
        self.work(run)

    def test_cold_recover_cannot_create_missing_ranking_or_reservation(self):
        def run():
            runner,args,store,intents,_=self.fixture();intent=runner.intent(*args[:3],args[3]);r=store.reserve(intent)
            cold=DurableRetrievalMeasurement(store,runner.ranker,runner.guard)
            cold._original_reservation=Mock(return_value=r)
            runner.ranker.capture.side_effect=ValueError('PRIVATE_RETRIEVAL_ORIGINAL_JOB_MISSING')
            with self.assertRaisesRegex(ValueError,'ORIGINAL_JOB_MISSING'):cold.execute(*args,recover_only=True)
            store.publish.assert_not_called();self.assertEqual(store.reserve.call_count,1)
        self.work(run)

    def test_missing_recovery_is_read_only_and_stale_access_denies(self):
        def run():
            runner,args,store,_,_=self.fixture()
            with self.assertRaisesRegex(ValueError,'RECOVERY_NO_WRITE'):runner.execute(*args,recover_only=True)
            runner.guard.return_value=False
            with self.assertRaisesRegex(ValueError,'SOURCE_UNVERIFIED'):runner.execute(*args)
            store.reserve.assert_not_called();runner.ranker.capture.assert_not_called()
        self.work(run)

    def test_changed_labels_conflict_in_original_slot(self):
        def run():
            runner,args,store,_,_=self.fixture();runner.execute(*args)
            changed=list(args);changed[3]=copy.deepcopy(args[3]);changed[3]['labels'][1]['grade']=2
            self.assertEqual(runner.intent(*args[:3],args[3])['record_id'],runner.intent(*changed[:3],changed[3])['record_id'])
            with self.assertRaises(AssertionError):runner.execute(*changed)
            runner.ranker.capture.assert_called_once()
        self.work(run)
