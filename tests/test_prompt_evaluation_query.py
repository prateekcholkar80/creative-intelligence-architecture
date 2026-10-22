"""Offline exact-schema SQL plans and receipt integrity."""
from dataclasses import replace
from datetime import timedelta
import re
import unittest

from src.application.prompt_evaluation_query import (
    EvaluationWriteKind, PromptEvaluationWrite, validate_write_receipt,
)
from src.application.prompt_evaluation_storage import encode_control
from src.application.workflow_contract import ContractError
from tests.test_access_reader import IDENTITY
from tests.test_prompt_evaluation_contract import evaluation, state
from tests.test_prompt_evaluation_storage import control


def control_plan():
    current = replace(control(), updated_by=IDENTITY.principal.principal_id)
    return PromptEvaluationWrite(EvaluationWriteKind.CONTROL, current, IDENTITY, 3)


def result_plan():
    current = control()
    record = replace(evaluation(), recorded_by=IDENTITY.principal.principal_id)
    return PromptEvaluationWrite(EvaluationWriteKind.EVALUATION, current, IDENTITY, 3,
        record=record, registry_state=state(), control_history=(current,))


def receipt(plan):
    return dict(state='APPLIED', observed_at=evaluation().completed_at + timedelta(seconds=1),
        workspace_id=plan.control.workspace_id, logical_id=plan.logical_id, record_digest=plan.digest)


class EvaluationQueryTests(unittest.TestCase):
    def test_fixed_scoped_tables_explicit_schemas_and_parameters(self):
        for plan in (control_plan(), result_plan()):
            sql = plan.sql
            config = plan.job_config().to_api_repr()
            self.assertEqual(set(re.findall(r'(?<!@)@([a-z_]+)', sql)),
                {p['name'] for p in config['query']['queryParameters']})
            self.assertFalse(config['query']['useQueryCache'])
            self.assertEqual(config['query']['maximumBytesBilled'], str(512 * 1024**2))
            self.assertIn('BEGIN TRANSACTION', sql)
            self.assertIn('ROLLBACK TRANSACTION', sql)
            self.assertIn('identity_subject=@subject', sql)
            self.assertIn('revision=@membership_revision', sql)
            self.assertIn('ASSERTION_EXPIRED', sql)
            self.assertIn('CONTROL_HISTORY_STALE', sql)
            self.assertNotIn('DELETE ', sql)
            self.assertNotIn('MERGE ', sql)
            self.assertNotIn('latest', sql)
            self.assertIn('INSERT INTO', sql)

    def test_result_write_is_append_only_and_fences_full_registry_capture(self):
        plan = result_plan()
        self.assertIn('REGISTRY_HISTORY_STALE', plan.sql)
        self.assertIn('ORPHAN_EVALUATION_CASES', plan.sql)
        self.assertIn('EVALUATION_CASE_REPLAY_CONFLICT', plan.sql)
        self.assertIn('UPDATE `project-661d501a-e709-4276-a26.ci_application.workspaces`', plan.sql)
        self.assertNotIn('UPDATE `project-661d501a-e709-4276-a26.ci_application.prompt_evaluations`', plan.sql)
        self.assertIn('changed_at BETWEEN', plan.sql)

    def test_logical_job_ids_ignore_competing_payload_but_configs_differ(self):
        plan = control_plan()
        competing = replace(plan, control=replace(plan.control,
            updated_at=plan.control.updated_at + timedelta(seconds=1)))
        self.assertEqual(plan.job_id, competing.job_id)
        self.assertNotEqual(plan.job_config().to_api_repr(), competing.job_config().to_api_repr())
        plan = result_plan()
        changed = replace(plan, record=replace(plan.record, evaluator_version=plan.record.evaluator_version,
            completed_at=plan.record.completed_at + timedelta(seconds=1)))
        self.assertEqual(plan.job_id, changed.job_id)
        self.assertNotEqual(plan.digest, changed.digest)
        self.assertNotEqual(plan.job_id, replace(plan, record=replace(plan.record,
            evaluation_id='different')).job_id)

    def test_control_revision_requires_complete_immutable_parent_chain(self):
        plan = control_plan()
        new = replace(plan.control, revision=2, parent_digest=plan.control.digest,
            updated_at=plan.control.updated_at + timedelta(seconds=1))
        changed = PromptEvaluationWrite(EvaluationWriteKind.CONTROL, new, IDENTITY, 3,
            previous=plan.control, control_history=(plan.control,))
        changed.job_config()
        for kwargs in ({'control_history': ()}, {'previous': None},
                       {'control': replace(new, parent_digest='0' * 64)}):
            with self.assertRaises(ContractError):
                replace(changed, **kwargs)
        with self.assertRaises(ContractError):
            replace(changed, control=replace(new,
                policy=replace(new.policy, allowed_returned_models=('other',))))

    def test_actor_registry_or_control_rebinding_cannot_form_plan(self):
        for plan, changes in ((control_plan(), {'control': control()}),
                (result_plan(), {'record': evaluation()}),
                (result_plan(), {'control_history': ()}),
                (result_plan(), {'membership_revision': True})):
            with self.assertRaises(ContractError):
                replace(plan, **changes)

    def test_receipt_rejects_wrong_scope_digest_time_partial_or_unknown(self):
        plan = result_plan()
        self.assertEqual(validate_write_receipt(plan, [receipt(plan)])['state'], 'APPLIED')
        self.assertEqual(validate_write_receipt(plan, [receipt(plan) | {'state': 'REUSED'}])['state'], 'REUSED')
        for rows in ([], [receipt(plan)] * 2, [receipt(plan) | {'record_digest': '0' * 64}],
                     [receipt(plan) | {'state': 'UNKNOWN'}], [receipt(plan) | {'extra': 'x'}],
                     [receipt(plan) | {'observed_at': evaluation().started_at}]):
            with self.assertRaises(ContractError):
                validate_write_receipt(plan, rows)


if __name__ == '__main__':
    unittest.main()
