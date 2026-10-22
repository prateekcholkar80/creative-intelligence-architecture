"""Offline planner migration parity tests; no cloud clients or model calls."""
from dataclasses import FrozenInstanceError, replace
import json
import unittest

from src.application.planner_prompt_parity import (
    PLANNER_OUTPUT_SCHEMA_VERSION,
    PLANNER_PROMPT_KEY,
    PLANNER_REQUIRED_VARIABLES,
    PLANNER_SAFETY_POLICY_VERSION,
    PlannerParityImpact,
    PlannerParityStatus,
    compare_planner_prompt,
    derive_planner_runtime_contract,
)
from src.application.prompt_contract import (
    VERTEX_PROJECT,
    PromptTask,
    VertexPromptSnapshot,
)
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION
from src.generators.campaign_brief_generator import planner_system_instruction


CONFIG = dict(model_id='gemini-3.5-flash', location=LOCATION,
              thinking_level='LOW', seed=42, max_output_tokens=4096)


def candidate(**changes):
    contract = derive_planner_runtime_contract(CONFIG)
    values = dict(
        prompt_key=PLANNER_PROMPT_KEY,
        task=PromptTask.PLANNER,
        vertex_project=VERTEX_PROJECT,
        vertex_location=LOCATION,
        vertex_prompt_id='1234567890',
        vertex_version_id='1',
        system_instruction=contract.system_instruction,
        prompt_template=contract.prompt_template,
        required_variables=PLANNER_REQUIRED_VARIABLES,
        model_id=contract.model_id,
        generation_config_json=contract.generation_config_json,
        safety_settings_json=contract.safety_settings_json,
        output_schema_json=contract.output_schema_shape_json,
        output_schema_version=PLANNER_OUTPUT_SCHEMA_VERSION,
        safety_policy_version=PLANNER_SAFETY_POLICY_VERSION,
    )
    return VertexPromptSnapshot(**(values | changes))


class PlannerPromptParityTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_exact_candidate_passes_but_report_never_authorizes_registration(self):
        result = compare_planner_prompt(candidate(), CONFIG)
        self.assertIs(result.status, PlannerParityStatus.PASS)
        self.assertEqual(result.differences, ())
        self.assertFalse(result.registration_authorized)
        self.assertEqual(len(result.report_sha256), 64)

    def test_runtime_contract_derives_request_domains_and_one_instruction_body(self):
        contract = derive_planner_runtime_contract(CONFIG)
        request = json.loads(contract.allowed_request_options_json)
        self.assertEqual(request['canonical_sku'][0], 'CI-SKU-0001')
        self.assertEqual(request['canonical_sku'][-1], 'CI-SKU-0020')
        self.assertEqual(request['channel'], ['EMAIL', 'PAID_SEARCH', 'PAID_SOCIAL'])
        self.assertEqual(request['audience'], {'type': 'NONEMPTY_TRIMMED_STRING'})
        self.assertEqual(contract.system_instruction,
                         planner_system_instruction(CONFIG['model_id']))
        self.assertEqual(len(contract.contract_sha256), 64)

    def test_prompt_language_change_is_review_visible_and_cannot_authorize(self):
        changed = replace(candidate(), system_instruction=(
            candidate().system_instruction + ' Prefer concise compositions.'))
        result = compare_planner_prompt(changed, CONFIG)
        self.assertIs(result.status, PlannerParityStatus.REVIEW_REQUIRED)
        self.assertEqual([(item.field, item.impact) for item in result.differences],
                         [('system_instruction', PlannerParityImpact.LANGUAGE_REVIEW)])
        self.assertFalse(result.registration_authorized)

    def test_each_behavior_change_blocks_parity(self):
        base = candidate()
        schema = json.loads(base.output_schema_json)
        schema['properties'].pop('cta')
        schema['required'].remove('cta')
        cases = [
            replace(base, model_id='gemini-2.5-flash'),
            replace(base, generation_config_json=json.dumps(
                json.loads(base.generation_config_json) | {'seed': 43},
                sort_keys=True, separators=(',', ':'))),
            replace(base, output_schema_json=json.dumps(
                schema, sort_keys=True, separators=(',', ':'))),
            replace(base, output_schema_version='campaign-draft-v2'),
            replace(base, safety_policy_version='poc-safety-v2'),
        ]
        for value in cases:
            with self.subTest(snapshot=value):
                result = compare_planner_prompt(value, CONFIG)
                self.assertIs(result.status, PlannerParityStatus.BLOCKED)
                self.assertTrue(any(item.impact is PlannerParityImpact.BEHAVIOR_CHANGE
                                    for item in result.differences))

    def test_language_and_behavior_changes_remain_ordered_and_behavior_blocks(self):
        changed = replace(candidate(),
            system_instruction=candidate().system_instruction + ' Review this.',
            model_id='gemini-2.5-flash')
        result = compare_planner_prompt(changed, CONFIG)
        self.assertIs(result.status, PlannerParityStatus.BLOCKED)
        self.assertEqual([item.field for item in result.differences],
                         ['system_instruction', 'model_id'])

    def test_full_values_are_hashed_and_previews_are_bounded(self):
        changed = replace(candidate(), system_instruction=(
            candidate().system_instruction + ' ' + ('A' * 600)))
        difference = compare_planner_prompt(changed, CONFIG).differences[0]
        self.assertTrue(difference.actual.truncated)
        self.assertEqual(len(difference.actual.preview), 256)
        self.assertGreater(difference.actual.utf8_bytes,
                           len(difference.actual.preview.encode('utf-8')))
        self.assertEqual(len(difference.actual.sha256), 64)

    def test_dynamic_enum_values_do_not_change_the_registered_schema_shape(self):
        base = candidate()
        schema = json.loads(base.output_schema_json)
        schema['properties']['headline']['enum'] = ['A different approved identity']
        changed = replace(base, output_schema_json=json.dumps(
            schema, sort_keys=True, separators=(',', ':')))
        result = compare_planner_prompt(changed, CONFIG)
        self.assertIs(result.status, PlannerParityStatus.PASS)
        self.assertEqual(result.differences, ())

    def test_wrong_task_or_key_is_rejected_before_comparison(self):
        for value in (replace(candidate(), task=PromptTask.INTERPRETER),
                      replace(candidate(), prompt_key='other-planner')):
            with self.subTest(value=value):
                self.assert_code('INVALID_PLANNER_PARITY_CANDIDATE',
                                 compare_planner_prompt, value, CONFIG)

    def test_invalid_runtime_config_fails_before_report(self):
        with self.assertRaises(ValueError):
            compare_planner_prompt(candidate(), CONFIG | {'api_key': 'secret'})

    def test_report_and_runtime_contract_are_immutable_and_hash_bound(self):
        result = compare_planner_prompt(candidate(), CONFIG)
        with self.assertRaises(FrozenInstanceError):
            result.registration_authorized = True
        self.assert_code('PLANNER_PARITY_REPORT_HASH_MISMATCH',
                         replace, result, report_sha256='f' * 64)
        self.assert_code('PLANNER_RUNTIME_CONTRACT_HASH_MISMATCH',
                         replace, result.runtime_contract,
                         model_id='gemini-2.5-flash')


if __name__ == '__main__':
    unittest.main()
