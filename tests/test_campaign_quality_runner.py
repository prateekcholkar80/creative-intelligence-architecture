from copy import deepcopy
import json
from unittest import TestCase
from src.validation.campaign_quality_runner import DEFAULT,run,retrieval_metrics
from src.application.workflow_contract import ContractError


class QualityRunnerTests(TestCase):
    def setUp(self):self.package=json.loads(DEFAULT.read_text())

    def test_actual_rule_replay_is_deterministic_and_explicitly_incomplete(self):
        report=run(self.package);self.assertEqual(report,run(self.package))
        self.assertEqual(report['rule_projection_matches'],6);self.assertEqual(report['rule_projection_cases'],6)
        self.assertEqual(report['full_e1_cases_executed'],0);self.assertEqual(report['global_e1_matches'],3)
        self.assertFalse(report['release_allowed']);self.assertFalse(report['e2_complete'])
        self.assertIsNone(report['retrieval_quality_metrics']);self.assertFalse(report['model_quality_measured'])
        self.assertEqual(len(report['unimplemented_rule_ids']),41)

    def test_gold_label_changes_do_not_change_checker_predictions(self):
        changed=deepcopy(self.package);changed['cases'][0]['projected_expected_state']='FAIL'
        before=run(self.package);after=run(changed)
        self.assertEqual([r['observed_state'] for r in before['results']],[r['observed_state'] for r in after['results']])
        self.assertEqual(after['rule_projection_matches'],5)

    def test_actual_source_text_context_and_review_pins_cannot_change_silently(self):
        for change in (lambda p:p.update(e1_review_sha256='a'*64),lambda p:p['cases'][0]['candidate'].update(text='changed'),
            lambda p:p['cases'][0].update(source_projection_sha256='b'*64),lambda p:p['cases'].pop()):
            data=deepcopy(self.package);change(data)
            with self.assertRaises(ContractError):run(data)

    def test_retrieval_metrics_cut_at_top_k_and_distinguish_exact_from_semantic(self):
        result=retrieval_metrics(('a','b'),('x','a','b'),top_k=2,mode='SEMANTIC')
        self.assertEqual(result['hits'],1);self.assertEqual(result['precision_at_k'],.5);self.assertEqual(result['recall_at_k'],.5)
        exact=retrieval_metrics(('a',),('a',),top_k=1,mode='EXACT_SKU')
        self.assertEqual(exact['precision_at_k'],1);self.assertNotEqual(result['mode'],exact['mode'])

    def test_empty_relevance_and_no_results_preserve_undefined_rates_and_false_matches(self):
        missing=retrieval_metrics(('a',),(),top_k=3,mode='SEMANTIC')
        self.assertIsNone(missing['precision_at_k']);self.assertEqual(missing['recall_at_k'],0)
        none=retrieval_metrics((),('x',),top_k=3,mode='SEMANTIC')
        self.assertIsNone(none['recall_at_k']);self.assertEqual(none['irrelevant_at_k'],1)
        self.assertFalse(none['no_match_observed'])

    def test_duplicate_ids_boolean_top_k_and_mixed_modes_are_rejected(self):
        for expected,returned,k,mode in ((('a','a'),('a',),3,'SEMANTIC'),(('a',),('a','a'),3,'EXACT_SKU'),
            (('a',),('a',),True,'SEMANTIC'),(('a',),('a',),1,'COMBINED')):
            with self.assertRaises(ContractError):retrieval_metrics(expected,returned,top_k=k,mode=mode)
