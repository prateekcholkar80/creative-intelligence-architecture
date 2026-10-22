from dataclasses import replace
import json,time
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_evaluator_store import canonical
from src.application.operation_budget import worker_operation_scope
from src.retrieval.private_query_embedding import QueryEmbeddingRequest,query_request,VERSION
from src.retrieval.private_text_embeddings import TextEmbeddingProfile,DurableTextEmbedding,TextEmbeddingResponse
from tests import test_upload_corpus as fixtures
from tests.test_private_text_embeddings import ledger,request as document_fixture


def query_fixture():
    reader,_,_,_,_,args=fixtures.UploadCorpusTests().fixture();capture=reader.capture({},**args)
    index=dict(index_record_id='a'*64,index_sha256='b'*64,corpus_sha256=capture.sha256,
        chunk_count=1,dimension=768,claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
    return query_request(capture,'eval_corpus_binding_v1_'+'c'*64,index,'retrieval-v1','application-instructions',
        'How should Rejuvenate face oil be applied?')


class PrivateQueryTests(TestCase):
    def work(self,fn):
        with worker_operation_scope(time.monotonic()+30):return fn()

    def test_query_slot_conflicts_on_text_corpus_index_or_scope_profile_drift(self):
        r=query_fixture();slot=r.intent['record_id']
        for key,value in (('text','Changed query'),('corpus_sha256','d'*64),('index_sha256','e'*64)):
            data=json.loads(r.input_json);data[key]=value;changed=replace(r,input_json=canonical(data))
            self.assertEqual(changed.intent['record_id'],slot)
            self.assertNotEqual(changed.intent['input_sha256'],r.intent['input_sha256'])
        self.assertNotEqual(slot,document_fixture().intent['record_id'])
        with self.assertRaises(ValueError):replace(r,profile=TextEmbeddingProfile())
        data=json.loads(r.input_json);data['scope']['sku']='OTHER'
        changed=replace(r,input_json=canonical(data));self.assertEqual(changed.intent['record_id'],slot)

    def test_one_query_call_and_cold_recovery_bind_query_task_and_index(self):
        def run():
            store,_,outcomes=ledger();guard=Mock(return_value=True)
            transport=Mock(return_value=TextEmbeddingResponse((.25,)*768,False,17))
            r=query_fixture();first=DurableTextEmbedding(store,transport,guard,enabled=True).execute(r)
            second=DurableTextEmbedding(store,transport,guard).execute(r,recover_only=True)
            self.assertEqual(first,second);transport.assert_called_once_with(r.profile,json.loads(r.input_json)['text'])
            self.assertEqual(r.profile.task_type,'RETRIEVAL_QUERY')
            self.assertEqual(outcomes[r.intent['record_id']]['version'],VERSION)
            self.assertFalse(first['qualifying']);self.assertIsNone(first['returned_model_id'])
        self.work(run)

    def test_unknown_query_never_dispatches_again(self):
        def run():
            store,_,_=ledger();transport=Mock(side_effect=TimeoutError('unknown'));r=query_fixture()
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'OUTCOME_UNUSABLE'):
                    DurableTextEmbedding(store,transport,Mock(return_value=True),enabled=True).execute(r)
            transport.assert_called_once()
        self.work(run)

    def test_lost_committed_query_response_recovers_without_new_call(self):
        def run():
            store,_,_=ledger();original=store.publish.side_effect
            def lost(*a):original(*a);raise TimeoutError('lost response')
            store.publish.side_effect=lost;r=query_fixture();transport=Mock(return_value=TextEmbeddingResponse((.25,)*768,False))
            with self.assertRaises(TimeoutError):DurableTextEmbedding(store,transport,Mock(return_value=True),enabled=True).execute(r)
            self.assertEqual(DurableTextEmbedding(store,transport,Mock(return_value=True)).execute(r,recover_only=True)['record_id'],r.intent['record_id'])
            transport.assert_called_once()
        self.work(run)

    def test_changed_query_intent_cannot_recover_or_dispatch_new_slot(self):
        def run():
            store,_,_=ledger();transport=Mock(return_value=TextEmbeddingResponse((.25,)*768,False));r=query_fixture()
            engine=DurableTextEmbedding(store,transport,Mock(return_value=True),enabled=True);engine.execute(r)
            data=json.loads(r.input_json);data['text']='altered'
            with self.assertRaises(AssertionError):engine.execute(replace(r,input_json=canonical(data)))
            transport.assert_called_once()
        self.work(run)

    def test_query_source_denied_before_reservation(self):
        def run():
            store,_,_=ledger();transport=Mock()
            with self.assertRaisesRegex(ValueError,'SOURCE_UNVERIFIED'):
                DurableTextEmbedding(store,transport,Mock(return_value=False),enabled=True).execute(query_fixture())
            transport.assert_not_called();store.reserve.assert_not_called()
        self.work(run)
