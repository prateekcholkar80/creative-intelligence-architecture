"""Pure Vertex prompt snapshot and lifecycle tests; no cloud calls."""
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import json
import unittest

from src.application.prompt_contract import (
    PROMPT_CONTRACT_VERSION, VERTEX_PROJECT, PromptLifecycle, PromptRegistryRecord,
    PromptTask, VertexPromptSnapshot, initial_prompt_record, propose_prompt_transition,
    validate_prompt_transition,
)
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION
from src.application.workspace_contract import MAX_REVISION

NOW = datetime(2026, 10, 11, 12, tzinfo=timezone.utc)
ACTOR = 'iap-v1:' + 'a' * 64


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def snapshot(**changes):
    values = dict(prompt_key='campaign-planner', task=PromptTask.PLANNER,
        vertex_project=VERTEX_PROJECT, vertex_location=LOCATION,
        vertex_prompt_id='1234567890', vertex_version_id='4',
        system_instruction='Use only approved evidence for {campaign_request}.',
        prompt_template='Evidence:\n{evidence_bundle_json}\nOptions:\n{creative_options_json}',
        required_variables=('campaign_request', 'creative_options_json',
                            'evidence_bundle_json'),
        model_id='gemini-3.5-flash-001',
        generation_config_json=canonical(dict(
            automatic_function_calling_disabled=True, max_output_tokens=4096,
            response_mime_type='application/json', seed=42, temperature=None,
            thinking_level='LOW')),
        safety_settings_json=canonical([
            dict(category='HARM_CATEGORY_DANGEROUS_CONTENT', threshold='BLOCK_MEDIUM_AND_ABOVE'),
            dict(category='HARM_CATEGORY_HARASSMENT', threshold='BLOCK_MEDIUM_AND_ABOVE'),
        ]),
        output_schema_json=canonical(dict(type='OBJECT', properties={
            'concept': {'type': 'STRING'}}, required=['concept'])),
        output_schema_version='campaign-draft-v1',
        safety_policy_version='poc-safety-v1')
    return VertexPromptSnapshot(**(values | changes))


def draft(**changes):
    values = dict(imported_at=NOW, imported_by=ACTOR)
    return initial_prompt_record(snapshot(), **(values | changes))


def advance(previous, status, seconds, **changes):
    values = dict(changed_at=NOW + timedelta(seconds=seconds), changed_by=ACTOR,
        reason_code='OPERATOR_REVIEWED', evaluation_id=previous.evaluation_id)
    return propose_prompt_transition(previous, status=status, **(values | changes))


class PromptContractTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_snapshot_is_deterministic_immutable_and_binds_exact_vertex_version(self):
        first = snapshot()
        self.assertEqual(first, snapshot())
        self.assertEqual(len(first.template_sha256), 64)
        self.assertEqual(len(first.snapshot_sha256), 64)
        self.assertEqual(first.canonical_payload()['vertex_version_id'], '4')
        self.assertNotIn('template_sha256', first.canonical_payload())
        with self.assertRaises(FrozenInstanceError):
            first.vertex_version_id = '5'
        moved = replace(first, vertex_version_id='5')
        self.assertEqual(first.template_sha256, moved.template_sha256)
        self.assertNotEqual(first.snapshot_sha256, moved.snapshot_sha256)
        self.assertNotEqual(first.template_sha256,
            replace(first, system_instruction=first.system_instruction + ' Review required.').template_sha256)

    def test_exact_project_location_and_numeric_vertex_resources_are_required(self):
        cases = [dict(vertex_project='other-project'), dict(vertex_location='us-central1'),
            dict(vertex_prompt_id='latest'), dict(vertex_prompt_id='0'),
            dict(vertex_version_id='04'), dict(vertex_version_id=True)]
        for changes in cases:
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                snapshot(**changes)

    def test_variables_are_sorted_unique_declared_and_used_exactly(self):
        for variables in (('campaign_request',),
                ('evidence_bundle_json', 'campaign_request', 'creative_options_json'),
                ('campaign_request', 'campaign_request', 'creative_options_json',
                 'evidence_bundle_json'), ('bad-name',)):
            with self.subTest(variables=variables), self.assertRaises(ContractError):
                snapshot(required_variables=variables)
        self.assert_code('PROMPT_VARIABLE_MISMATCH', replace, snapshot(),
            prompt_template=snapshot().prompt_template + '\n{unregistered}')

    def test_generation_configuration_is_canonical_bounded_and_complete(self):
        base = json.loads(snapshot().generation_config_json)
        cases = [base | {'max_output_tokens': 0}, base | {'seed': True},
            base | {'temperature': 3}, base | {'thinking_level': 'UNSUPPORTED'},
            base | {'response_mime_type': 'text/plain'},
            {key: value for key, value in base.items() if key != 'seed'}]
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ContractError):
                snapshot(generation_config_json=canonical(value))
        self.assert_code('INVALID_PROMPT_GENERATION_CONFIG', snapshot,
            generation_config_json=json.dumps(base, indent=2))

    def test_safety_settings_and_output_schema_are_canonical_and_structured(self):
        safety = json.loads(snapshot().safety_settings_json)
        for value in ([], list(reversed(safety)), safety + [safety[0]],
                      [dict(category='BAD-NAME', threshold='BLOCK')]):
            with self.subTest(value=value), self.assertRaises(ContractError):
                snapshot(safety_settings_json=canonical(value))
        for value in ({}, {'type': 'ARRAY', 'properties': {}, 'required': []},
                      {'type': 'OBJECT', 'properties': [] , 'required': []},
                      {'type': 'OBJECT', 'properties': {'concept': {'type': 'STRING'}},
                       'required': ['missing']}):
            with self.subTest(value=value), self.assertRaises(ContractError):
                snapshot(output_schema_json=canonical(value))

    def test_noncanonical_or_unsafe_prompt_content_is_rejected(self):
        for changes in (dict(system_instruction='bad\r\ntext'),
                        dict(prompt_template='bad\x00text'),
                        dict(system_instruction='e\u0301 {campaign_request}')):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                snapshot(**changes)
        self.assert_code('UNSUPPORTED_PROMPT_CONTRACT', replace, snapshot(),
            contract_version='future-v2')

    def test_initial_import_is_draft_with_no_claim_of_vertex_or_bq_verification(self):
        record = draft()
        self.assertEqual((record.revision, record.parent_digest, record.status),
            (1, None, PromptLifecycle.DRAFT))
        self.assertEqual(record.reason_code, 'IMPORTED_FROM_VERTEX')
        self.assertIsNone(record.evaluation_id)
        self.assertEqual(len(record.digest), 64)

    def test_complete_lifecycle_and_controlled_rollback_preserve_exact_snapshot(self):
        imported = draft()
        registered = advance(imported, PromptLifecycle.REGISTERED, 1)
        evaluated = advance(registered, PromptLifecycle.EVALUATED, 2,
            evaluation_id='evaluation-1')
        active = advance(evaluated, PromptLifecycle.ACTIVE, 3)
        retired = advance(active, PromptLifecycle.RETIRED, 4)
        rollback = advance(retired, PromptLifecycle.ACTIVE, 5,
            reason_code='AUTHORIZED_ROLLBACK')
        for previous, proposed in zip(
                (imported, registered, evaluated, active, retired),
                (registered, evaluated, active, retired, rollback)):
            validate_prompt_transition(previous, proposed)
            self.assertEqual(proposed.snapshot, imported.snapshot)
            self.assertEqual(proposed.parent_digest, previous.digest)
        self.assertEqual(rollback.evaluation_id, 'evaluation-1')

    def test_activation_requires_evaluation_and_identity_cannot_be_rebound(self):
        registered = advance(draft(), PromptLifecycle.REGISTERED, 1)
        self.assert_code('INVALID_PROMPT_LIFECYCLE_TRANSITION', advance,
            registered, PromptLifecycle.ACTIVE, 2)
        self.assert_code('PROMPT_EVALUATION_REQUIRED', advance,
            registered, PromptLifecycle.EVALUATED, 2)
        evaluated = advance(registered, PromptLifecycle.EVALUATED, 2,
            evaluation_id='evaluation-1')
        self.assert_code('PROMPT_EVALUATION_REBOUND', advance,
            evaluated, PromptLifecycle.ACTIVE, 3, evaluation_id='evaluation-2')

    def test_blocked_is_terminal_and_keeps_prior_evaluation_identity(self):
        registered = advance(draft(), PromptLifecycle.REGISTERED, 1)
        blocked = advance(registered, PromptLifecycle.BLOCKED, 2,
            reason_code='HASH_MISMATCH')
        self.assert_code('INVALID_PROMPT_LIFECYCLE_TRANSITION', advance,
            blocked, PromptLifecycle.REGISTERED, 3)
        evaluated = advance(registered, PromptLifecycle.EVALUATED, 2,
            evaluation_id='evaluation-1')
        self.assert_code('PROMPT_EVALUATION_REBOUND', advance,
            evaluated, PromptLifecycle.BLOCKED, 3, evaluation_id=None)

    def test_revision_parent_time_and_snapshot_chain_fail_closed(self):
        registered = advance(draft(), PromptLifecycle.REGISTERED, 1)
        self.assert_code('PROMPT_REGISTRY_TIME_REGRESSION', propose_prompt_transition,
            registered, status=PromptLifecycle.EVALUATED,
            changed_at=NOW, changed_by=ACTOR, reason_code='EVALUATED',
            evaluation_id='evaluation-1')
        evaluated = advance(registered, PromptLifecycle.EVALUATED, 2,
            evaluation_id='evaluation-1')
        for proposed in (replace(evaluated, revision=4),
                         replace(evaluated, parent_digest='f' * 64),
                         replace(evaluated, snapshot=replace(evaluated.snapshot,
                             vertex_version_id='5'))):
            self.assert_code('PROMPT_REGISTRY_CHAIN_MISMATCH',
                validate_prompt_transition, registered, proposed)
        exhausted = replace(registered, revision=MAX_REVISION)
        self.assert_code('PROMPT_REGISTRY_REVISION_EXHAUSTED', advance,
            exhausted, PromptLifecycle.EVALUATED, 3, evaluation_id='evaluation-1')

    def test_invalid_record_fields_and_naive_time_are_rejected(self):
        first = draft()
        for changes in (dict(changed_by='bad actor'), dict(reason_code='lowercase'),
                        dict(contract_version='future-v2'), dict(revision=True)):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                replace(first, **changes)
        with self.assertRaises(ContractError):
            draft(imported_at=NOW.replace(tzinfo=None))
        self.assert_code('INVALID_PROMPT_REGISTRY_PARENT', replace, first,
            parent_digest='f' * 64)


if __name__ == '__main__':
    unittest.main()
