from dataclasses import replace
from datetime import datetime,timezone,timedelta
import json,time
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_corpus_embedding import ProtectedCorpusEmbedding,CorpusEmbeddingSettings
from src.application.campaign_corpus_preparation import BoundCorpusCapture,canonical,digest
from src.application.operation_budget import worker_operation_scope
from src.retrieval.upload_corpus import UploadCorpusCapture
from src.retrieval.private_text_embeddings import TextEmbeddingResponse
from tests import test_campaign_corpus_preparation as preparation_fixtures
from tests.test_private_text_embeddings import ledger
from tests import test_upload_corpus as upload_fixtures


class ProtectedEmbeddingTests(TestCase):
    def fixture(self):
        reader,_,_,_,_,args=upload_fixtures.UploadCorpusTests().fixture();capture=reader.capture({},**args)
        corpus,_,_,_,_=preparation_fixtures.CorpusPreparationTests().fixture();store,_,_=ledger()
        corpus.documents.store.client=store.client
        receipt=dict(binding_id='eval_corpus_binding_v1_'+'d'*64,corpus_sha256=capture.sha256,
            selection_sha256=corpus.settings.selection_sha256)
        refresh=Mock(return_value=True)
        bound=BoundCorpusCapture(capture,canonical(receipt),refresh)
        corpus.capture_bound=Mock(return_value=bound)
        settings=CorpusEmbeddingSettings('protected-embedding',receipt['binding_id'],capture.sha256,
            receipt['selection_sha256'],True)
        transport=Mock(return_value=TextEmbeddingResponse((.25,)*768,False))
        return corpus,store,settings,transport,refresh

    def work(self,fn):
        with worker_operation_scope(time.monotonic()+60):return fn()

    def test_signed_headers_reach_capture_and_recovery_has_no_second_model_call(self):
        def run():
            corpus,store,settings,transport,refresh=self.fixture()
            service=ProtectedCorpusEmbedding(corpus,store,settings,transport=transport)
            headers={'signed':'opaque'};first=service.execute(headers)
            corpus.capture_bound.assert_called_once_with(headers)
            second=ProtectedCorpusEmbedding(corpus,store,replace(settings,enabled=False),transport=transport).execute(headers,recover_only=True)
            self.assertEqual(first,second);transport.assert_called_once();self.assertTrue(refresh.called)
            self.assertEqual(first['image_embedding_status'],'NOT_EXECUTED');self.assertFalse(first['qualifying'])
            self.assertIsNone(service._bound)
        self.work(run)

    def test_changed_binding_denies_before_reservation(self):
        def run():
            corpus,store,settings,transport,_=self.fixture()
            service=ProtectedCorpusEmbedding(corpus,store,replace(settings,corpus_sha256='e'*64),transport=transport)
            with self.assertRaisesRegex(ValueError,'BINDING_CHANGED'):service.execute({})
            store.reserve.assert_not_called();transport.assert_not_called()
        self.work(run)

    def test_fresh_source_check_failure_never_dispatches(self):
        def run():
            corpus,store,settings,transport,refresh=self.fixture();refresh.return_value=False
            service=ProtectedCorpusEmbedding(corpus,store,settings,transport=transport)
            with self.assertRaisesRegex(ValueError,'SOURCE_UNVERIFIED'):service.execute({})
            transport.assert_not_called();store.reserve.assert_not_called();self.assertIsNone(service._bound)
        self.work(run)

    def test_disabled_run_and_empty_recovery_never_dispatch(self):
        def run():
            corpus,store,settings,transport,_=self.fixture()
            service=ProtectedCorpusEmbedding(corpus,store,replace(settings,enabled=False),transport=transport)
            for recover in (False,True):
                with self.assertRaises(ValueError):service.execute({},recover_only=recover)
            transport.assert_not_called();store.reserve.assert_not_called()
        self.work(run)

    def test_recover_finishes_retained_exact_publication_without_provider_replay(self):
        def run():
            corpus,store,settings,transport,_=self.fixture();original=store.publish.side_effect
            store.publish.side_effect=TimeoutError('uncertain publication')
            service=ProtectedCorpusEmbedding(corpus,store,settings,transport=transport)
            with self.assertRaises(TimeoutError):service.execute({})
            exact=store.publish.call_args.args
            store.publish.side_effect=original
            # Recovery cannot create a missing index. Its absence is reported
            # after recovering the original provider outcome, with no redispatch.
            with self.assertRaisesRegex(ValueError,'PRIVATE_INDEX_RECOVERY_NO_WRITE'):
                service.execute({},recover_only=True)
            self.assertEqual(store.publish.call_args.args,exact);transport.assert_called_once()
        self.work(run)

    def test_recover_retained_index_publication_without_provider_replay(self):
        def run():
            corpus,store,settings,transport,_=self.fixture();original=store.publish.side_effect
            def index_lost(reservation,outcome):
                if outcome['version']=='private-text-index-v1':raise TimeoutError('index response')
                return original(reservation,outcome)
            store.publish.side_effect=index_lost
            service=ProtectedCorpusEmbedding(corpus,store,settings,transport=transport)
            with self.assertRaises(TimeoutError):service.execute({})
            exact=store.publish.call_args.args;store.publish.side_effect=original
            receipt=service.execute({},recover_only=True)
            self.assertEqual(receipt['index']['chunk_count'],1)
            self.assertEqual(store.publish.call_args.args,exact);transport.assert_called_once()
        self.work(run)

    def test_bound_capture_refresh_checks_extraction_and_original_binding(self):
        def run():
            corpus,docs,_,_,_=preparation_fixtures.CorpusPreparationTests().fixture()
            manifest=json.loads(corpus.reader.capture.return_value.manifest_json);records={};by_sid={}
            for pin in json.loads(corpus.settings.selection_json)['sources']:
                source=docs.store.submission(pin['submission_id']);source['original'].update(bucket='private',object_name='campaign-evidence/'+pin['submission_id'])
            for item in manifest['text_sources']:
                item['source_record_sha256']=digest(docs.store.submission(item['submission_id']))
                record=dict(submission_id=item['submission_id'],extraction=dict(version='synthetic'))
                eid=digest(record);item['extraction_id']=eid;records[eid]=record;by_sid[item['submission_id']]=eid
            docs.store.find_extraction.side_effect=lambda sid,**kw:by_sid.get(sid)
            docs.store.read.side_effect=lambda kind,eid:records.get(eid)
            corpus.reader.capture.return_value=UploadCorpusCapture(canonical(manifest),(),datetime.now(timezone.utc)+timedelta(seconds=120))
            corpus.prepare({});writes=docs.intake.store.record.call_count
            original_query=docs.store.query.side_effect
            def query(sql,params):
                if sql.startswith('WITH source_pins'):
                    return ({'source_count':4,'source_matches':4,'extraction_count':2,'extraction_matches':2},)
                return original_query(sql,params)
            docs.store.query.side_effect=query
            def blob(name,*,generation):return Mock(generation=generation)
            docs.intake.store.storage.bucket.return_value.blob.side_effect=blob
            bound=corpus.capture_bound({});self.assertIs(type(bound),BoundCorpusCapture)
            self.assertTrue(bound.refresh());self.assertEqual(docs.intake.store.record.call_count,writes)
            self.assertEqual(sum(c.args[0].startswith('WITH source_pins') for c in docs.store.query.call_args_list),1)
            self.assertEqual(docs.intake.store.storage.bucket.return_value.blob.call_count,3)
            # A new guard is proved from current records/context/generations,
            # rather than extending an expired snapshot's deadline.
            original_clock=corpus.clock;corpus.clock=lambda:original_clock()+timedelta(minutes=10)
            self.assertTrue(bound.refresh())
            def mismatch(sql,params):
                if sql.startswith('WITH source_pins'):
                    return ({'source_count':4,'source_matches':3,'extraction_count':2,'extraction_matches':2},)
                return query(sql,params)
            docs.store.query.side_effect=mismatch
            with self.assertRaisesRegex(ValueError,'BOUND_RECORDS_CHANGED'):bound.refresh()
            docs.store.query.side_effect=query
            docs.intake.store.storage.bucket.return_value.blob.side_effect=FileNotFoundError('deleted generation')
            with self.assertRaises(FileNotFoundError):bound.refresh()
            docs.intake.store.storage.bucket.return_value.blob.side_effect=blob
            next(iter(records.values()))['extraction']['version']='tampered'
            with self.assertRaisesRegex(ValueError,'EXTRACTION_CHANGED'):bound.refresh()
        self.work(run)
