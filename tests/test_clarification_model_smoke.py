from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace as NS
from unittest import TestCase
import json
from src.validation.clarification_model_smoke import inputs,grade,run
from src.application.campaign_evidence_gaps import canonical
from tests import test_campaign_clarification_prompts as fixtures


class ClarificationSmokeTests(TestCase):
    def policy(self):
        f=fixtures.ClarificationPromptTests();f.setUp();return f.policy

    def test_evaluation_inputs_never_change_draft_policy(self):
        policy=self.policy();before=policy.policy_canonical_json
        definition=NS(policy_sha256='a'*64)
        value,plan=inputs(dict(features=['OFFER']),definition,policy)
        self.assertIsNotNone(plan)
        self.assertIn('SIMULATED_ABSENT',plan.canonical_json)
        self.assertEqual(policy.policy_canonical_json,before)
        self.assertEqual(json.loads(before)['status'],'DRAFT')
        self.assertFalse(policy.approval_verified)
        self.assertIn('evidence_gap_plan_json',value)

    def test_intent_grade_checks_exact_keys_audience_and_proposed_features(self):
        raw=dict(schema_version='campaign-evidence-intent-v1',features=['PRICE'],formula_version=None,audience='NEW')
        result=grade(dict(features=['PRICE']),json.dumps(raw),None,allowed_features=['PRICE'])
        self.assertTrue(result['structural_pass']);self.assertTrue(result['feature_match'])
        for changes in (dict(audience='RETURNING'),dict(features=['UNKNOWN']),dict(extra='unreviewed')):
            self.assertFalse(grade(dict(features=['PRICE']),json.dumps(raw|changes),None,allowed_features=['PRICE'])['structural_pass'])

    def test_question_grade_rejects_coverage_drift_and_does_not_measure_semantics(self):
        _,plan=inputs(dict(features=['SHIPPING']),NS(policy_sha256='a'*64),self.policy())
        gaps=json.loads(plan.canonical_json)['questions']
        raw=dict(plan_sha256=plan.sha256,questions=[dict(requirement_id=g['requirement_id'],fields=g['fields'],question='What terms apply?') for g in gaps])
        self.assertEqual(grade({},canonical(raw),plan)['semantic_review'],'REQUIRED')
        raw['questions'][0]['fields']=[]
        with self.assertRaises(ValueError):grade({},canonical(raw),plan)

    def test_existing_report_prevents_call_replay(self):
        with TemporaryDirectory() as directory:
            path=Path(directory)/'existing.json';path.write_text('{"state":"IN_FLIGHT"}')
            with self.assertRaisesRegex(ValueError,'EVAL_ORIGINAL_REPORT_ALREADY_EXISTS'):run(path)
            self.assertEqual(path.read_text(),'{"state":"IN_FLIGHT"}')
