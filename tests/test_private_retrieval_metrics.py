import copy,math,unittest
from src.application.campaign_evaluator_store import sha
from src.retrieval.private_retrieval_metrics import measure,ranking_metrics
from src.retrieval.private_retrieval_rank import validate_rows,ranking_sql,rank_config


class RetrievalMetricsTests(unittest.TestCase):
    def fixture(self):
        scope=dict(sku='SKU',country='US',channel='META')
        chunks=[dict(chunk_id=i,provenance=dict(text_sha256=sha(i))) for i in ('a','b','c')]
        rows=[dict(chunk_id='a',lexical_overlap=2,cosine_distance=.1,lexical_rank=1,vector_rank=1),
            dict(chunk_id='b',lexical_overlap=1,cosine_distance=.2,lexical_rank=2,vector_rank=2),
            dict(chunk_id='c',lexical_overlap=0,cosine_distance=.9,lexical_rank=None,vector_rank=3)]
        obs=dict(corpus_sha256=sha('corpus'),index_sha256=sha('index'),query_text_sha256=sha('query'),scope=scope,rows=rows)
        case=dict(version='private-retrieval-relevance-case-v1',label_authority='SOURCE_REVIEW_CALIBRATION_NOT_INDEPENDENT_QUALIFICATION',
            corpus_sha256=obs['corpus_sha256'],index_sha256=obs['index_sha256'],query_text_sha256=obs['query_text_sha256'],scope=scope,
            labels=[dict(chunk_id=c['chunk_id'],text_sha256=c['provenance']['text_sha256'],grade=g) for c,g in zip(chunks,(3,1,0))])
        return obs,case,chunks,scope

    def test_known_precision_recall_rr_and_graded_ndcg(self):
        grades=dict(a=3,b=1,c=0);m=ranking_metrics(('b','a','c'),grades,3)
        self.assertEqual((m['precision'],m['recall'],m['reciprocal_rank']),(1/3,1,.5))
        self.assertAlmostEqual(m['ndcg'],(1+7/math.log2(3))/(7+1/math.log2(3)))
        self.assertEqual(ranking_metrics((),grades,1)['ndcg'],0)
        self.assertEqual(ranking_metrics(('b',),grades,1)['recall'],0)

    def test_invalid_or_duplicate_labels_denied(self):
        for ids,grades,k in [(('a','a'),dict(a=3),1),(('x',),dict(a=3),1),(('a',),dict(a=True),1),
            (('a',),dict(a=1),1),(('a',),dict(a=3),True),((),None,1)]:
            with self.assertRaises(ValueError):ranking_metrics(ids,grades,k)

    def test_source_bound_measurement_is_calibration_only(self):
        obs,case,chunks,scope=self.fixture();m=measure(obs,case,chunks,scope)
        self.assertEqual(m['rankings']['rrf'],['a','b','c'])
        self.assertEqual(m['metrics']['rrf'][0]['ndcg'],1)
        self.assertFalse(m['qualifying']);self.assertFalse(m['release_allowed'])
        self.assertEqual(m['semantic_reranking'],'NOT_EXECUTED')

    def test_empty_lexical_still_has_vector_candidates_without_invented_matches(self):
        obs,case,chunks,scope=self.fixture()
        for row in obs['rows']:row.update(lexical_rank=None,lexical_overlap=0)
        m=measure(obs,case,chunks,scope)
        self.assertEqual(m['rankings']['lexical'],[]);self.assertEqual(m['metrics']['lexical'][0]['precision'],0)
        self.assertEqual(m['rankings']['rrf'],['a','b','c'])

    def test_case_query_scope_source_and_label_coverage_drift_denied(self):
        for mutation in ('query','scope','source','coverage','duplicate'):
            obs,case,chunks,scope=self.fixture()
            if mutation=='query':case['query_text_sha256']=sha('changed')
            elif mutation=='scope':case['scope']=dict(scope,country='CA')
            elif mutation=='source':case['labels'][0]['text_sha256']=sha('changed')
            elif mutation=='coverage':case['labels'].pop()
            else:case['labels'][1]=copy.deepcopy(case['labels'][0])
            with self.assertRaises(ValueError):measure(obs,case,chunks,scope)

    def test_native_rank_record_pins_and_deterministic_order_validated(self):
        obs,*_=self.fixture();pins=[1]*10
        row=dict(expected_count=10,observed_count=10,matched_count=10,rows=obs['rows'])
        self.assertEqual(validate_rows(row,pins,('a','b','c')),obs['rows'])
        mutations=[lambda r:r.update(matched_count=9),lambda r:r['rows'].pop(),
            lambda r:r['rows'][0].update(cosine_distance=float('nan')),
            lambda r:r['rows'][0].update(vector_rank=2),lambda r:r['rows'][1].update(lexical_rank=1),
            lambda r:r['rows'][0].update(chunk_id='b'),lambda r:r['rows'][2].update(lexical_rank=3)]
        for change in mutations:
            r=copy.deepcopy(row);change(r)
            with self.assertRaises(ValueError):validate_rows(r,pins,('a','b','c'))

    def test_sql_has_exact_native_hash_and_partition_guards_not_client_vector_parameters(self):
        sql=ranking_sql();self.assertIn('SHA256(r.record_json)',sql);self.assertIn('ML.DISTANCE',sql)
        self.assertIn('TEXT_ANALYZE',sql);self.assertIn('expected_count=matched_count',sql)
        cfg=rank_config([dict(kind='intents',id='id',sha256=sha('x'))],'query',('doc',))
        self.assertEqual({p.name for p in cfg.query_parameters},{'pins','ids','query_id','documents'})
        self.assertFalse(cfg.use_query_cache)
