from dataclasses import replace
import hashlib,json,time
from unittest import TestCase
from unittest.mock import Mock
from src.application.campaign_corpus_embedding import ProtectedCorpusEmbedding
from src.application.campaign_corpus_retrieval import CorpusRetrievalSettings,ProtectedCorpusRetrieval
from src.application.campaign_evaluator_store import canonical,sha
from src.application.operation_budget import worker_operation_scope
from tests import test_campaign_corpus_embedding as fixtures


class ProtectedRetrievalTests(TestCase):
    def test_phase_diagnostic_never_logs_input_or_exception_text(self):
        from src.application.campaign_corpus_retrieval import measured_phase,retrieval_progress
        callback=Mock()
        with self.assertLogs('src.application.campaign_corpus_retrieval',level='INFO') as logs:
            with self.assertRaises(ValueError):
                with retrieval_progress(callback),measured_phase('source_capture'):
                    raise ValueError('private source content and identity@example.com')
        row=json.loads(logs.records[0].getMessage())
        self.assertEqual(set(row),{'event','phase','completed','elapsed_seconds'})
        self.assertFalse(row['completed']);self.assertNotIn('identity@example.com',str(logs.output))
        self.assertEqual([c.args[0]['state'] for c in callback.call_args_list],['STARTED','STOPPED'])
        self.assertNotIn('identity@example.com',str(callback.call_args_list))

    def work(self,fn):
        with worker_operation_scope(time.monotonic()+60):fn()

    def fixture(self):
        corpus,store,cfg,transport,refresh=fixtures.ProtectedEmbeddingTests().fixture()
        embedding=ProtectedCorpusEmbedding(corpus,store,cfg,transport=transport)
        index=embedding.execute({})['index'];manifest=json.loads(corpus.capture_bound.return_value.capture.manifest_json)
        corpus.capture_saved_bound_for_receipt=Mock(return_value=corpus.capture_bound.return_value)
        text='How should this product be applied?';querysha=hashlib.sha256(text.encode()).hexdigest()
        case=dict(version='private-retrieval-relevance-case-v1',case_id='application',
            label_authority='SOURCE_REVIEW_CALIBRATION_NOT_INDEPENDENT_QUALIFICATION',
            corpus_sha256=manifest and cfg.corpus_sha256,index_sha256=index['index_sha256'],query_text_sha256=querysha,
            scope={k:manifest['scope'][k] for k in ('sku','country','channel')},labels=[dict(
                chunk_id=c['chunk_id'],text_sha256=c['provenance']['text_sha256'],grade=3) for c in manifest['text_chunks']])
        settings=CorpusRetrievalSettings('retrieval-run','application',text,canonical(case),sha(case),
            index['index_record_id'],index['index_sha256'],True)
        service=ProtectedCorpusRetrieval(embedding,settings)
        obs=dict(corpus_sha256=cfg.corpus_sha256,index_sha256=index['index_sha256'],index_record_id=index['index_record_id'],
            scope=manifest['scope'],query_text_sha256=querysha,native_job_id='fixture-rank',rows=[dict(
                chunk_id=c['chunk_id'],lexical_overlap=2,lexical_rank=i,vector_rank=i,cosine_distance=.1)
                    for i,c in enumerate(manifest['text_chunks'],1)])
        service.ranker.capture=Mock(return_value=obs);service.ranker.verify_saved=Mock(return_value=True)
        return service,transport,refresh,corpus

    def test_explicit_run_only_queries_and_recover_never_repeats_document_calls(self):
        def run():
            service,transport,_,corpus=self.fixture();before=transport.call_count
            first=service.execute({'iap':'opaque'})
            service.engine.execute=Mock(side_effect=AssertionError('Recovery repeated query engine'))
            service.ranker.verify_documents=Mock(side_effect=AssertionError('Recovery repeated index preflight'))
            second=service.execute({'iap':'fresh'},recover_only=True)
            self.assertEqual(first,second);self.assertEqual(transport.call_count,before+1)
            self.assertEqual(transport.call_args.args[0].task_type,'RETRIEVAL_QUERY')
            service.ranker.capture.assert_called_once();self.assertIsNone(service._bound);self.assertIsNone(service.embedding._bound)
            self.assertEqual(corpus.capture_saved_bound_for_receipt.call_args.args,({'iap':'fresh'},))
            self.assertEqual(corpus.capture_bound.call_args.args,({'iap':'opaque'},))
        self.work(run)

    def test_recover_missing_query_denies_without_provider(self):
        def run():
            service,transport,_,_=self.fixture();before=transport.call_count
            with self.assertRaisesRegex(ValueError,'RECOVERY_NO_WRITE'):service.execute({},recover_only=True)
            self.assertEqual(transport.call_count,before);service.ranker.capture.assert_not_called()
        self.work(run)

    def test_fresh_access_or_pinned_index_drift_denies_before_query(self):
        def run():
            service,transport,refresh,_=self.fixture();before=transport.call_count;refresh.return_value=False
            with self.assertRaises(ValueError):service.execute({})
            self.assertEqual(transport.call_count,before);refresh.return_value=True
            service.settings=replace(service.settings,index_record_id='f'*64)
            with self.assertRaisesRegex(ValueError,'INDEX_CHANGED'):service.execute({})
            self.assertEqual(transport.call_count,before)
        self.work(run)

    def test_label_source_drift_denies_before_query(self):
        def run():
            service,transport,_,_=self.fixture();before=transport.call_count
            case=json.loads(service.settings.case_json);case['labels'][0]['text_sha256']='f'*64
            service.settings=replace(service.settings,case_json=canonical(case),case_sha256=sha(case))
            with self.assertRaisesRegex(ValueError,'LABEL_SOURCE'):service.execute({})
            self.assertEqual(transport.call_count,before)
        self.work(run)
