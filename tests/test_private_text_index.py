from copy import deepcopy
from dataclasses import replace
import json,time
from unittest import TestCase
from unittest.mock import Mock
from src.application.operation_budget import worker_operation_scope
from src.application.campaign_evaluator_store import canonical
from src.retrieval.private_text_embeddings import DurableTextEmbedding,TextEmbeddingResponse
from src.retrieval.private_text_index import DurableTextIndex
from tests.test_private_text_embeddings import request,ledger


class PrivateTextIndexTests(TestCase):
    def test_complete_index_uses_one_batch_source_guard_per_boundary(self):
        def run():
            store,_,_=ledger();transport=Mock(return_value=TextEmbeddingResponse((.25,)*768,False))
            requests=tuple(request(count=3,ordinal=i) for i in range(3))
            for r in requests:DurableTextEmbedding(store,transport,Mock(return_value=True),enabled=True).execute(r)
            individual=Mock(return_value=True);batch=Mock(return_value=True)
            receipt=DurableTextIndex(store,individual,enabled=True,guard_all=batch).build(requests)
            self.assertEqual(receipt['chunk_count'],3);self.assertEqual(batch.call_count,3)
            self.assertTrue(all(c.args==(requests,) for c in batch.call_args_list));individual.assert_not_called()
            with self.assertRaisesRegex(ValueError,'SOURCE_UNVERIFIED'):
                DurableTextIndex(store,individual,guard_all=Mock(return_value=False)).build(requests,recover_only=True)
        self.work(run)

    def work(self,fn):
        with worker_operation_scope(time.monotonic()+30):fn()

    def fixture(self):
        store,_,outcomes=ledger();r=request();guard=Mock(return_value=True)
        transport=Mock(return_value=TextEmbeddingResponse((.25,)*768,False))
        DurableTextEmbedding(store,transport,guard,enabled=True).execute(r)
        return store,r,guard,transport,outcomes

    def test_build_and_cold_recovery_bind_vector_outcomes_without_copying_vectors(self):
        def run():
            store,r,guard,transport,outcomes=self.fixture()
            first=DurableTextIndex(store,guard,enabled=True).build((r,))
            second=DurableTextIndex(store,guard).build((r,),recover_only=True)
            self.assertEqual(first,second);transport.assert_called_once()
            index=outcomes[first['index_record_id']]
            self.assertNotIn('vector',index['manifest']['chunks'][0]);self.assertIn('vector_response_sha256',index['manifest']['chunks'][0])
            self.assertEqual(index['manifest']['chunks'][0]['embedding_record_id'],r.intent['record_id'])
            self.assertFalse(index['eligible_for_generation']);self.assertFalse(first['release_allowed'])
        self.work(run)

    def test_missing_vector_or_unknown_vector_never_creates_index(self):
        def run():
            for unknown in (False,True):
                store,r,guard,transport,outcomes=self.fixture();outcomes.pop(r.intent['record_id'])
                if not unknown:store.recover.side_effect=lambda _:None
                writes=store.reserve.call_count
                with self.assertRaises(ValueError):DurableTextIndex(store,guard,enabled=True).build((r,))
                self.assertEqual(store.reserve.call_count,writes);transport.assert_called_once()
        self.work(run)

    def test_recovery_of_missing_index_does_not_publish(self):
        def run():
            store,r,guard,_,_=self.fixture();writes=store.publish.call_count
            with self.assertRaisesRegex(ValueError,'RECOVERY_NO_WRITE'):DurableTextIndex(store,guard).build((r,),recover_only=True)
            self.assertEqual(store.publish.call_count,writes)
        self.work(run)

    def test_duplicate_partial_or_mixed_corpus_sequence_denied(self):
        def run():
            store,r,guard,_,_=self.fixture();index=DurableTextIndex(store,guard,enabled=True)
            for requests in ((),(r,r),(request(count=2),),(request(count=2,ordinal=1),)):
                with self.assertRaises(ValueError):index.build(requests)
            first=request(count=2);second=request(count=2,ordinal=1)
            value=json.loads(second.input_json);value['corpus_sha256']='f'*64
            changed=replace(second,input_json=canonical(value))
            with self.assertRaisesRegex(ValueError,'MIXED_CORPUS'):index.build((first,changed))
        self.work(run)

    def test_source_change_denies_before_index_publication(self):
        def run():
            store,r,guard,_,_=self.fixture();writes=store.publish.call_count;guard.return_value=False
            with self.assertRaisesRegex(ValueError,'SOURCE_UNVERIFIED'):DurableTextIndex(store,guard,enabled=True).build((r,))
            self.assertEqual(store.publish.call_count,writes)
        self.work(run)

    def test_committed_lost_response_recovers_original_index(self):
        def run():
            store,r,guard,transport,_=self.fixture();original=store.publish.side_effect
            def lost(*args):original(*args);raise TimeoutError('lost response')
            store.publish.side_effect=lost
            with self.assertRaises(TimeoutError):DurableTextIndex(store,guard,enabled=True).build((r,))
            got=DurableTextIndex(store,guard).build((r,),recover_only=True)
            self.assertEqual(got['chunk_count'],1);transport.assert_called_once()
        self.work(run)

    def test_pending_publication_preserves_exact_original_bytes(self):
        def run():
            store,r,guard,transport,_=self.fixture();original=store.publish.side_effect
            store.publish.side_effect=TimeoutError('precommit uncertainty');index=DurableTextIndex(store,guard,enabled=True)
            with self.assertRaises(TimeoutError):index.build((r,))
            exact=deepcopy(store.publish.call_args.args);store.publish.side_effect=original
            index.recover_pending((r,));self.assertEqual(store.publish.call_args.args,exact);transport.assert_called_once()
        self.work(run)

    def test_modified_dependency_denies_even_with_saved_index(self):
        def run():
            store,r,guard,transport,outcomes=self.fixture()
            DurableTextIndex(store,guard,enabled=True).build((r,))
            outcomes[r.intent['record_id']]['response']['vector'][0]=.9
            with self.assertRaises(ValueError):DurableTextIndex(store,guard).build((r,),recover_only=True)
            transport.assert_called_once()
        self.work(run)

    def test_all_three_chunks_required_and_bound_in_order(self):
        def run():
            store,_,outcomes=ledger();guard=Mock(return_value=True)
            transport=Mock(return_value=TextEmbeddingResponse((.25,)*768,False))
            requests=tuple(request(count=3,ordinal=i) for i in range(3))
            embedding=DurableTextEmbedding(store,transport,guard,enabled=True)
            for r in requests:embedding.execute(r)
            index=DurableTextIndex(store,guard,enabled=True)
            with self.assertRaisesRegex(ValueError,'INCOMPLETE'):index.build(requests[:2])
            receipt=index.build(requests)
            self.assertEqual(receipt['chunk_count'],3)
            self.assertEqual([r['ordinal'] for r in outcomes[receipt['index_record_id']]['manifest']['chunks']],[0,1,2])
            self.assertEqual(index.build(requests,recover_only=True),receipt);self.assertEqual(transport.call_count,3)
        self.work(run)

    def test_authority_change_after_commit_suppresses_index_receipt(self):
        def run():
            store,r,guard,_,_=self.fixture();original=store.publish.side_effect
            def change(*args):
                value=original(*args);guard.return_value=False;return value
            store.publish.side_effect=change
            with self.assertRaisesRegex(ValueError,'SOURCE_UNVERIFIED'):
                DurableTextIndex(store,guard,enabled=True).build((r,))
        self.work(run)
