"""Benchmark isolation and measurement accounting; no model-quality assertion."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
import json
from src.validation import campaign_semantic_benchmark as benchmark
from src.application.campaign_semantic_checks import prompt,SEMANTIC_RULE_IDS


class SemanticBenchmarkTests(TestCase):
    def test_all_reviewed_cases_are_pinned_and_gold_fields_are_not_prompted(self):
        catalog,cases=benchmark.load_cases()
        self.assertEqual(len(cases),159);self.assertEqual(len({v['case_id'] for v in cases}),159)
        self.assertEqual(sum(v['case'].candidate is not None for v in cases),143)
        covered=set()
        for entry in cases:
            _,text=prompt(catalog,entry['case']);value=json.loads(text)
            self.assertFalse({'case_id','expected_decision','expected_handling','expected_reason'}&set(value))
            self.assertNotIn('case_id',value)
            covered.update(entry['case'].rule_ids)
            if entry['case'].label_scope=='REQUEST_ONLY_SCENARIO':
                self.assertIsNone(entry['case'].candidate)
                self.assertNotIn('authored-scenario-state',entry['case'].evidence_json)
        self.assertTrue(set(SEMANTIC_RULE_IDS)<=covered)

    def test_existing_reports_and_invalid_limits_cannot_dispatch(self):
        with TemporaryDirectory() as directory:
            output=Path(directory)/'existing.json';output.write_text('{}')
            with patch.object(benchmark.bigquery,'Client') as client:
                with self.assertRaisesRegex(ValueError,'REPORT_EXISTS'):benchmark.run(output)
                with self.assertRaisesRegex(ValueError,'RUN_LIMIT'):benchmark.run(Path(directory)/'new.json',limit=160)
                with self.assertRaisesRegex(ValueError,'RUN_LIMIT'):benchmark.run(Path(directory)/'new.json',workers=3)
            client.assert_not_called()

    def test_selected_rule_payloads_are_bounded_and_no_claim_permission_is_implied(self):
        catalog,cases=benchmark.load_cases()
        for entry in cases:
            _,text=prompt(catalog,entry['case'])
            self.assertLessEqual(len(text.encode()),49152)
            self.assertFalse(json.loads(text)['current_production_authority_verified'])
            self.assertFalse(json.loads(text)['rendered_pixels_available'])

    def test_repeated_capacity_errors_stop_without_retry_or_unknown_usage_zeroes(self):
        from types import SimpleNamespace as NS
        from unittest.mock import Mock
        catalog,_=benchmark.load_cases()
        policy=NS(snapshot_sha256='a'*64,native_job_id='native-read',rule_canonical_json=tuple(
            json.dumps(c) for c in json.loads(catalog.criteria_json)))
        snapshot=NS(model_id='gemini-3.5-flash',snapshot_sha256='b'*64,generation_config_json='{}')
        registry=Mock();registry.read_exact.return_value=NS(state=NS(head=NS(snapshot=snapshot)))
        class CapacityError(Exception):code=429
        counter=iter(range(0,100000,100))
        with TemporaryDirectory() as directory,patch.object(benchmark.bigquery,'Client'),\
             patch.object(benchmark,'read_policy',return_value=policy),\
             patch.object(benchmark,'BigQueryPromptRegistryReader',return_value=registry),\
             patch.object(benchmark,'observe',side_effect=CapacityError('capacity')) as observe,\
             patch.object(benchmark.time,'monotonic',side_effect=lambda:next(counter)):
            result=benchmark.run(Path(directory)/'report.json')
        self.assertEqual(observe.call_count,3)
        self.assertEqual(result['status'],'INCOMPLETE_CAPACITY')
        self.assertEqual(result['attempted_cases'],3);self.assertEqual(result['unattempted_cases'],156)
        self.assertEqual(result['usage']['input_tokens']['unknown_cases'],3)
        self.assertEqual(result['observed_cases'],0);self.assertFalse(result['qualifying'])
