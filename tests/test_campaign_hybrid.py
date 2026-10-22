from dataclasses import replace
from fractions import Fraction
from unittest import TestCase
from src.retrieval.campaign_hybrid import EligibleCandidate, rank_hybrid


def candidate(i, **kw):
    return EligibleCandidate(**(dict(chunk_id=i,content_sha256='a'*64,sku='CI-SKU-0022',country='US',
        channel='META',objective='AWARENESS',audience='NEW',source_capture_sha256='b'*64)|kw))


class HybridTests(TestCase):
    def rank(self, candidates, lexical=(), vector=(), **kw):
        return rank_hybrid(candidates,lexical,vector,**(dict(sku='CI-SKU-0022',country='US',channel='META',
            objective='AWARENESS',audience='NEW')|kw))

    def test_exact_rrf_both_lanes_and_missing_lane(self):
        result=self.rank((candidate('a'),candidate('b')),('a','b'),('b',))
        a,b=result['results'][1],result['results'][0]
        self.assertEqual(b['chunk_id'],'b')
        self.assertEqual(Fraction(**b['rrf_score']),Fraction(1,62)+Fraction(1,61))
        self.assertEqual(Fraction(**a['rrf_score']),Fraction(1,61)); self.assertIsNone(a['vector_rank'])

    def test_ties_stable_and_scope_preferences_explicit(self):
        values=(candidate('a',objective='CONVERSION'),candidate('b'))
        result=self.rank(values,('a',),('b',))
        self.assertEqual(result['results'][0]['chunk_id'],'b')
        self.assertEqual(result['results'][0]['fusion_rank'],2)
        self.assertEqual(result,self.rank(tuple(reversed(values)),('a',),('b',)))

    def test_true_empty_has_no_fallback_or_qualification(self):
        result=self.rank(())
        self.assertEqual(result['results'],[]);self.assertTrue(result['empty'])
        self.assertEqual(result['relevance'],'NOT_MEASURED');self.assertFalse(result['release_allowed'])

    def test_cross_scope_mixed_capture_and_duplicates_deny(self):
        for change in (dict(sku='CI-SKU-0001'),dict(country='GB'),dict(channel='EMAIL'),
                       dict(source_capture_sha256='c'*64)):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.rank((candidate('a'),candidate('b',**change)),('a','b'))
        with self.assertRaises(ValueError):self.rank((candidate('a'),candidate('a')),('a',))

    def test_unknown_duplicate_and_unranked_candidates_deny(self):
        for lexical in (('other',),('a','a'),()):
            with self.assertRaises(ValueError):self.rank((candidate('a'),),lexical)
        with self.assertRaises(ValueError):self.rank((candidate('a'),),('a',),top_k=True)
        with self.assertRaises(ValueError):self.rank(tuple(candidate('c'+str(i)) for i in range(101)))

    def test_hash_and_top_k_bind_exact_selection(self):
        from src.application.campaign_evaluator_store import sha
        result=self.rank((candidate('a'),candidate('b')),('a','b'),top_k=1)
        self.assertEqual(len(result['results']),1)
        self.assertEqual(result['sha256'],sha({k:v for k,v in result.items() if k!='sha256'}))
        self.assertFalse(result['qualifying'])
