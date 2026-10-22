import hashlib,json,time
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_evaluator_observer import *
from src.application.campaign_evaluator_store import Reservation
from src.application.operation_budget import worker_operation_scope


class DurableObserverTests(TestCase):
    def fixture(self,transport=None):
        store=Mock(spec=EvaluatorStore);store.recover.return_value=None
        store.reserve.side_effect=lambda intent:Reservation(intent['record_id'],canonical(dict(intent,reservation_nonce='nonce')),True)
        binding=StageEvaluationBinding('workspace','conversation','request','principal','INTERPRETATION','invocation',*(['a'*64]*4))
        profile=EvaluatorProfile('model','{}',hashlib.sha256(b'system').hexdigest())
        raw='{"checks":[]}'
        transport=transport or Mock(return_value=EvaluatorProviderResponse(raw,ModelResultObservation('SUCCEEDED',returned_model_id='model',response_sha256=hashlib.sha256(raw.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED')))
        guard=Mock()
        observer=DurableEvaluatorObserver(store=store,run_id='evaluation',evaluator_sha256='c'*64,profile=profile,transport=transport,guard=guard,enabled=True,binding=binding)
        return observer,store,transport,guard
    def call(self,observer,content='{}'):
        with worker_operation_scope(time.monotonic()+30):return observer('system',content,('A02',))
    def test_reservation_precedes_call_and_usage_stays_unknown(self):
        observer,store,transport,guard=self.fixture()
        transport.side_effect=lambda *a:(self.assertEqual(store.reserve.call_count,1) or EvaluatorProviderResponse('{"checks":[]}',ModelResultObservation('SUCCEEDED',returned_model_id='model',response_sha256=hashlib.sha256(b'{"checks":[]}').hexdigest(),finish_reason='STOP',safety_outcome='PASSED')))
        self.assertEqual(self.call(observer),dict(checks=[]))
        saved=store.publish.call_args.args[1];self.assertIsNone(saved['input_tokens']);self.assertFalse(saved['release_allowed'])
        self.assertEqual(guard.call_count,3)
    def test_transport_unknown_is_durable_and_never_called_again(self):
        observer,store,transport,_=self.fixture(Mock(side_effect=TimeoutError('lost')))
        with self.assertRaises(ValueError):self.call(observer)
        saved=store.publish.call_args.args[1];self.assertEqual(saved['state'],'UNKNOWN')
        store.recover.return_value=saved
        with self.assertRaises(ValueError):self.call(observer)
        self.assertEqual(transport.call_count,1)
    def test_lost_reservation_does_not_dispatch(self):
        observer,store,transport,_=self.fixture();store.reserve.side_effect=TimeoutError('lost')
        with self.assertRaises(TimeoutError):self.call(observer)
        transport.assert_not_called()
    def test_changed_input_keeps_same_slot_but_changes_intent(self):
        observer,_,_,_=self.fixture()
        first=observer._intent('system','{}',('A02',));second=observer._intent('system','{"changed":true}',('A02',))
        self.assertEqual(first['record_id'],second['record_id']);self.assertNotEqual(first,second)
    def test_publication_loss_recovers_exact_outcome_without_transport(self):
        observer,store,transport,_=self.fixture();store.publish.side_effect=[TimeoutError('lost'),None]
        with self.assertRaises(TimeoutError):self.call(observer)
        first=store.publish.call_args.args
        with worker_operation_scope(time.monotonic()+30):self.assertEqual(observer.recover_pending(first[0].record_id),dict(checks=[]))
        self.assertEqual(store.publish.call_args.args,first);self.assertEqual(transport.call_count,1)
    def test_no_owned_worker_or_disabled_observer_cannot_dispatch(self):
        observer,_,transport,_=self.fixture()
        with self.assertRaises(ValueError):observer('system','{}',('A02',))
        observer.enabled=False
        with self.assertRaises(ValueError):self.call(observer)
        transport.assert_not_called()

    def test_model_mismatch_is_recorded_but_never_returned_as_assessment(self):
        observer,store,transport,_=self.fixture()
        raw='{"checks":[]}'
        transport.return_value=EvaluatorProviderResponse(raw,ModelResultObservation('SUCCEEDED',returned_model_id='other',response_sha256=hashlib.sha256(raw.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED',input_tokens=12,output_tokens=3))
        with self.assertRaises(ValueError):self.call(observer)
        outcome=store.publish.call_args.args[1]
        self.assertEqual(outcome['state'],'REJECTED');self.assertEqual(outcome['input_tokens'],12)
        self.assertEqual(outcome['returned_model_id'],'other')

    def test_access_change_after_reservation_prevents_model_call(self):
        observer,store,transport,guard=self.fixture();guard.side_effect=[None,ValueError('revoked')]
        with self.assertRaises(ValueError):self.call(observer)
        self.assertEqual(store.reserve.call_count,1);transport.assert_not_called();store.publish.assert_not_called()

    def test_recovery_only_missing_chunk_never_reserves_or_dispatches(self):
        observer,store,transport,_=self.fixture();observer.recover_only=True
        with self.assertRaisesRegex(ValueError,'EVALUATOR_RECOVERY_NO_DISPATCH'):self.call(observer)
        store.reserve.assert_not_called();transport.assert_not_called();store.publish.assert_not_called()
