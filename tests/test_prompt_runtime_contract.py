"""Offline ACTIVE eligibility, immutable provenance and untrusted variable tests."""
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
import json
import unittest

from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_evaluation_contract import _hash
from src.application.prompt_runtime_contract import (
    RuntimeBehavior, bind_invocation, canonical, render_prompt, validate_runtime_eligibility,
    validate_runtime_profile,
)
from src.application.workflow_contract import ContractError
from tests.test_prompt_contract import NOW
from tests.test_prompt_evaluation_contract import evaluation, state
from tests.test_prompt_lifecycle_contract import snapshot, plan, progressed
from tests.test_workflow_contract import profile as base_profile


def capture():
    active = progressed(progressed(state(), PromptLifecycle.EVALUATED), PromptLifecycle.ACTIVE, 5)
    control = replace(plan().control, active_snapshot_sha256=active.head.snapshot.snapshot_sha256,
                      transition_digests=(active.head.digest,))
    return snapshot(active, active=active, control=control, history=(control,), record=evaluation())


def profile():
    s = capture().target.head.snapshot
    return base_profile() | dict(model_id=s.model_id, prompt_id=s.vertex_prompt_id,
        prompt_version=s.vertex_version_id, output_schema_version=s.output_schema_version,
        safety_policy_version=s.safety_policy_version)


def behavior():
    s = capture().target.head.snapshot
    return RuntimeBehavior(s.prompt_key, s.task, s.required_variables,
        s.output_schema_json, s.safety_settings_json, s.output_schema_version, s.safety_policy_version)


def binding(current=None, p=None, **changes):
    current = capture() if current is None else current
    p = profile() if p is None else p
    return bind_invocation(current, current.target.head.snapshot, p, behavior(), frozenset({_hash(p)}),
        conversation_id='conversation-1', request_id='request-1', invocation_id='invocation-1',
        checked_at=current.observed_at, **changes)


class RuntimeContractTests(unittest.TestCase):
    def test_active_exact_snapshot_has_complete_immutable_invocation_pins(self):
        result = binding()
        data = json.loads(result.binding_canonical_json)
        self.assertEqual(data['snapshot']['vertex_version_id'], '4')
        self.assertEqual(data['eligibility']['registry_digest'], capture().target.head.digest)
        self.assertEqual(data['eligibility']['evaluation_anchor_digest'], capture().evaluation_control_anchor.digest)
        self.assertEqual(data['profile'], profile())
        self.assertEqual(result, binding())
        with self.assertRaises(FrozenInstanceError):
            result.invocation_id = 'other'
        p = profile(); result2 = binding(p=p); p['model_id'] = 'edited'
        self.assertEqual(json.loads(result2.binding_canonical_json)['profile']['model_id'], profile()['model_id'])

    def test_non_active_missing_evaluation_and_expiry_deny(self):
        for current in (snapshot(), snapshot(record=evaluation())):
            with self.assertRaises(ContractError):
                validate_runtime_eligibility(current, checked_at=current.observed_at)
        with self.assertRaises(ContractError):
            validate_runtime_eligibility(capture(), checked_at=evaluation().valid_until)
        with self.assertRaises(ContractError):
            validate_runtime_eligibility(capture(), checked_at=NOW)

    def test_changed_current_policy_and_incomplete_or_failed_results_deny(self):
        c = capture(); ec = c.evaluation.control
        newer = replace(ec, revision=2, parent_digest=ec.digest,
            updated_at=ec.updated_at + timedelta(seconds=1), policy=replace(ec.policy, version='v2'))
        candidates = [replace(c, evaluation=replace(c.evaluation, control=newer, control_history=(ec,newer)))]
        for record in (evaluation(results=()), replace(evaluation(),
                results=(replace(evaluation().results[0], returned_model_id='wrong'),)+evaluation().results[1:])):
            candidates.append(replace(c, evaluation=replace(c.evaluation, record=record)))
        for current in candidates:
            with self.assertRaises(ContractError):
                validate_runtime_eligibility(current, checked_at=current.observed_at)

    def test_equal_content_new_control_revision_retains_original_anchor(self):
        c = capture(); ec = c.evaluation.control
        new = replace(ec, revision=2, parent_digest=ec.digest, updated_at=ec.updated_at+timedelta(seconds=1))
        c = replace(c, evaluation=replace(c.evaluation, control=new, control_history=(ec,new)))
        data = json.loads(binding(c).binding_canonical_json)['eligibility']
        self.assertEqual(data['evaluation_control_revision'], 2)
        self.assertEqual(data['evaluation_anchor_revision'], 1)

    def test_model_profile_pins_and_generation_controls_cannot_drift(self):
        s = capture().target.head.snapshot
        for key,value in (('model_id','other'),('prompt_version','5'),('prompt_id','99'),
                          ('max_output_tokens',2048),('thinking_level','HIGH'),('seed',1),
                          ('temperature',0.5),('output_schema_version','new')):
            p = profile() | {key:value}
            with self.subTest(key=key), self.assertRaises(ContractError):
                validate_runtime_profile(s,p,behavior(),frozenset({_hash(p)}))
        with self.assertRaises(ContractError):
            validate_runtime_profile(s,profile(),behavior(),frozenset({'a'*64}))

    def test_reviewed_language_changes_allowed_behavior_changes_denied(self):
        s = capture().target.head.snapshot
        validate_runtime_profile(replace(s, system_instruction=s.system_instruction+' Reviewed copy.'),
            profile(),behavior(),frozenset({_hash(profile())}))
        with self.assertRaises(ContractError):
            validate_runtime_profile(replace(s, safety_policy_version='new'), profile(), behavior(),
                                     frozenset({_hash(profile())}))
        with self.assertRaises(ContractError):
            validate_runtime_profile(replace(s, output_schema_json=canonical(dict(type='OBJECT',
                properties={'other':{'type':'STRING'}},required=['other']))),profile(),behavior(),
                frozenset({_hash(profile())}))

    def test_vertex_edit_with_same_resource_identity_does_not_match_bigquery(self):
        c = capture()
        with self.assertRaises(ContractError):
            bind_invocation(c, replace(c.target.head.snapshot,system_instruction='Edited {campaign_request}'),
                profile(),behavior(),frozenset({_hash(profile())}),conversation_id='conversation-1',
                request_id='request-1',invocation_id='invocation-1',checked_at=c.observed_at)

    def test_variables_are_exact_bounded_single_pass_data(self):
        s = capture().target.head.snapshot
        variables = dict(campaign_request='{evidence_bundle_json}', evidence_bundle_json='{"x":"{campaign_request}"}',
                         creative_options_json='{}')
        instruction,text = render_prompt(s,variables)
        self.assertIn('{evidence_bundle_json}',instruction)
        self.assertIn('{"x":"{campaign_request}"}',text)
        for values in ({}, variables|{'extra':'x'}, variables|{'campaign_request':1},
                       variables|{'campaign_request':'x'*262145}):
            with self.assertRaises(ContractError):
                render_prompt(s,values)


if __name__ == '__main__':
    unittest.main()
