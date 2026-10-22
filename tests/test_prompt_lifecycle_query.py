"""Lifecycle query boundaries and global logical slots, without native SQL."""
from dataclasses import replace
from datetime import timedelta
import re
import json
import unittest

from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_lifecycle_query import config, parameters, sql, validate_receipt
from src.application.workflow_contract import ContractError
from tests.test_prompt_lifecycle_contract import plan, snapshot, progressed
from tests.test_prompt_evaluation_contract import evaluation, state


def receipt(p, state='APPLIED'):
    return dict(state=state, prompt_key=p.control.prompt_key, revision=p.control.revision,
        control_digest=p.control.digest, command_digest=p.command_digest,
        observed_at=p.changed_at + timedelta(seconds=1))


class LifecycleQueryTests(unittest.TestCase):
    def test_all_parameters_explicit_schema_and_bounded_native_settings(self):
        for p in (plan(), plan(snapshot(), PromptLifecycle.BLOCKED)):
            cfg = config(p).to_api_repr()
            self.assertEqual(set(parameters(p)), set(re.findall(r'(?<!@)@([a-z_]+)', sql(p))))
            self.assertFalse(cfg['query']['useQueryCache'])
            self.assertEqual(cfg['query']['maximumBytesBilled'], str(768*1024**2))
            self.assertFalse(cfg['query']['createSession'])
            self.assertIn('BEGIN TRANSACTION', sql(p))
            self.assertIn('ROLLBACK TRANSACTION', sql(p))
            self.assertNotIn('DELETE ', sql(p))
            self.assertNotIn('MERGE ', sql(p))
            self.assertIn('changed_at BETWEEN', sql(p))
            self.assertIn('identity_subject=@subject', sql(p))
            self.assertIn('revision=@membership_revision', sql(p))

    def test_gate_pins_report_cases_current_control_full_history_and_native_expiry(self):
        script = sql(plan())
        for term in ('LIFECYCLE_EVALUATION_CONTROL_STALE', 'LIFECYCLE_EVALUATION_CONTROL_HISTORY_STALE',
                     'LIFECYCLE_EVALUATION_CASES_STALE', 'LIFECYCLE_EVALUATION_STALE',
                     'LIFECYCLE_EVALUATION_EXPIRED_OR_FAILED', 'ASSERTION_EXPIRED',
                     'LIFECYCLE_REGISTRY_HISTORY_STALE', 'LIFECYCLE_CONTROL_HISTORY_STALE'):
            self.assertIn(term, script)
        self.assertIn("status FROM current_evaluation)='PASSED'", script)
        self.assertNotIn('evaluation_id', parameters(plan(snapshot(), PromptLifecycle.BLOCKED)))

    def test_activation_fences_global_control_and_checks_entire_active_set(self):
        candidate = progressed(state(), PromptLifecycle.EVALUATED)
        p = plan(snapshot(candidate, record=evaluation()), PromptLifecycle.ACTIVE)
        script = sql(p)
        self.assertIn('LIFECYCLE_GLOBAL_FENCE_FAILED', script)
        self.assertIn('LIFECYCLE_ACTIVE_STALE', script)
        self.assertIn('LIFECYCLE_ACTIVE_DUPLICATE', script)
        self.assertIn('LIFECYCLE_ACTIVE_PIN_MISMATCH', script)
        self.assertIn("WHERE prompt_key=@prompt_key AND status='ACTIVE'", script)
        self.assertIn('t.record_digest=e.record_digest', script)
        self.assertIn('INSERT INTO', script)
        self.assertNotIn('system_instruction=c.system_instruction', script)

    def test_exact_replay_precedes_current_policy_gate_and_preserves_later_states(self):
        script = sql(plan())
        self.assertLess(script.index('LIFECYCLE_PARTIAL_REPLAY'), script.index('LIFECYCLE_EVALUATION_CONTROL_STALE'))
        self.assertIn('h.revision<=c.revision', script)
        self.assertIn('r.revision<c.revision', script)
        self.assertIn("SET receipt_state='REUSED'", script)

    def test_competing_workspaces_share_slot_but_not_native_config(self):
        one, two = plan(), plan(snapshot(record=evaluation(), workspace='other-workspace'))
        self.assertEqual(one.job_id, two.job_id)
        self.assertNotEqual(config(one).to_api_repr(), config(two).to_api_repr())
        changed = replace(one, reason_code='CHANGED_REVIEW')
        self.assertEqual(one.job_id, changed.job_id)
        self.assertNotEqual(one.control.digest, changed.control.digest)

    def test_receipt_requires_exact_control_and_command_without_inferred_commit(self):
        p = plan()
        self.assertEqual(validate_receipt(p, [receipt(p)])['state'], 'APPLIED')
        self.assertEqual(validate_receipt(p, [receipt(p, 'REUSED')])['state'], 'REUSED')
        for rows in ([], [receipt(p)]*2, [receipt(p)|{'state':'UNKNOWN'}],
                [receipt(p)|{'control_digest':'a'*64}], [receipt(p)|{'revision':True}],
                [receipt(p)|{'prompt_key':'wrong'}], [receipt(p)|{'extra':'x'}],
                [receipt(p)|{'observed_at':p.changed_at-timedelta(seconds=1)}]):
            with self.assertRaises(ContractError):
                validate_receipt(p, rows)

    def test_original_evaluation_control_pin_is_not_inferred_from_equal_policy_content(self):
        capture = snapshot(record=evaluation())
        first = capture.evaluation.control
        second = replace(first, revision=2, parent_digest=first.digest,
            updated_at=first.updated_at+timedelta(seconds=1))
        ev = replace(capture.evaluation, control=second, control_history=(first,second))
        p = plan(replace(capture, evaluation=ev, evaluation_control_anchor=second))
        report = json.loads(parameters(p)['evaluation_json'][1])
        self.assertEqual(report['control_digest'], second.digest)
        self.assertEqual(report['control_revision'], 2)
        with self.assertRaises(ContractError):
            replace(capture,evaluation_control_anchor=second)


if __name__ == '__main__':
    unittest.main()
