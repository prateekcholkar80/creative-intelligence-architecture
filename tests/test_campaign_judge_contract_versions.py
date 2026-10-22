from dataclasses import replace
import hashlib,json
from unittest import TestCase
from unittest.mock import patch
from src.application import campaign_semantic_checks as checks
from src.application import campaign_evaluator_transport as transport
from src.application.campaign_evaluator_observer import EvaluatorProfile
from src.application.campaign_evaluator_store import canonical
from src.application.campaign_full_quality_checks import FullQualityEvaluator
from src.application.workflow_contract import canonical_json_bytes
from src.validation.campaign_semantic_benchmark import load_cases

class JudgeContractVersionsTests(TestCase):
    def setUp(self):
        self.legacy,_=load_cases();self.strict=replace(self.legacy,contract_version=checks.STRICT_VERSION)
        self.rules=tuple(sorted(checks.SEMANTIC_RULE_IDS))[:16]
        self.case=checks.SemanticInput('INTERPRETATION','US','META','Observed candidate','[]','[]',self.rules,'GLOBAL_CAMPAIGN_RELEASE')
    def test_legacy_hash_and_request_remain_exact_and_strict_is_distinct(self):
        self.assertEqual(FullQualityEvaluator(self.legacy,self.legacy.sha256,lambda *a:None).evaluator_sha256,'7cff215eaf3d69ff8fe7291c249f9a6d945e236e4679969beb20d58972d0c5ec')
        system,content=checks.prompt(self.legacy,self.case)
        self.assertEqual(system,checks.SYSTEM);self.assertNotIn('required_rule_ids',json.loads(content))
        strict_system,strict_content=checks.prompt(self.strict,self.case)
        self.assertNotEqual(strict_system,system);self.assertNotEqual(self.strict.sha256,self.legacy.sha256)
        self.assertEqual(json.loads(strict_content)['required_rule_ids'],list(self.rules))
        self.assertEqual(json.loads(strict_content)['version'],checks.STRICT_VERSION)
    def test_schema_cardinality_and_transport_version_are_pinned(self):
        for version,strict in ((transport.VERSION,False),(transport.STRICT_VERSION,True),(transport.APPLICABILITY_VERSION,True)):
            profile=EvaluatorProfile('model',canonical(dict(version=version)),hashlib.sha256(b'system').hexdigest())
            with patch.object(transport.PinnedVertexEvaluatorTransport,'_generate') as generate:
                transport.PinnedVertexEvaluatorTransport()(profile,'system','{}',self.rules)
            schema=generate.call_args.args[-1]['properties']['checks']
            if strict:self.assertEqual((schema['minItems'],schema['maxItems']),(16,16))
            else:self.assertNotIn('minItems',schema);self.assertNotIn('maxItems',schema)
    def test_short_duplicate_reordered_or_extra_check_cannot_validate(self):
        good=[dict(rule_id=r,handling_state='INCONCLUSIVE',reason='Support unavailable',candidate_quote='',evidence_ids=[]) for r in self.rules]
        value=dict(requested_content_decision='NEEDS_REVIEW',decision_reason='Support unavailable',checks=good)
        checks.verify_result(self.strict,self.case,value)
        for bad in (good[:2],good[:-1]+[good[0]],list(reversed(good)),good+[good[0]]):
            with self.subTest(count=len(bad)),self.assertRaisesRegex(ValueError,'SEMANTIC_RESULT_COVERAGE'):
                checks.verify_result(self.strict,self.case,value|dict(checks=bad))
    def test_unknown_contract_denied(self):
        with self.assertRaises(ValueError):replace(self.legacy,contract_version='unknown')
        with self.assertRaises(ValueError):checks.output_schema(self.rules,'unknown')

    def test_strict_bounds_survive_real_sdk_configuration(self):
        from google.genai import types
        schema=checks.output_schema(self.rules,checks.STRICT_VERSION)
        config=types.GenerateContentConfig(response_schema=schema,response_mime_type='application/json')
        bounds=types.Schema.model_validate(config.response_schema).properties['checks']
        self.assertEqual((bounds.min_items,bounds.max_items),(16,16))
        self.assertEqual(bounds.items.properties['rule_id'].enum,list(self.rules))
