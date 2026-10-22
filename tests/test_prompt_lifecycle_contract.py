"""Supplied lifecycle/control cases; no live SQL or application identities."""
from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock
import unittest

from google.api_core.exceptions import NotFound

from src.application.prompt_contract import PromptLifecycle, initial_prompt_record
from src.application.prompt_evaluation_store import EvaluationSnapshot, EvaluationReadRequest
from src.application.prompt_lifecycle_contract import (
    TABLES, LifecycleReadRequest, LifecycleSnapshot, LifecycleWrite, LifecycleControl,
    encode_control, decode_control, validate_control_history,
)
from src.application.prompt_registry_codec import PromptRegistryState
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_prompt_lifecycle_tables import setup
from src.ingestion.setup_workflow_tables import PROJECT, SchemaMismatch
from tests.test_access_reader import IDENTITY
from tests.test_prompt_contract import NOW, advance, draft
from tests.test_prompt_evaluation_contract import evaluation, state
from tests.test_prompt_evaluation_storage import control as evaluation_control
from tests.test_setup_workflow_tables import dataset


def snapshot(target=None, *, active=None, control=None, history=(), record=None, workspace='workspace-1'):
    target = state() if target is None else target
    s = target.head.snapshot
    request = LifecycleReadRequest(workspace, s.prompt_key, s.task, s.vertex_prompt_id,
        s.vertex_version_id, IDENTITY, 3, None if record is None else record.evaluation_id)
    evidence = None
    at = max(NOW + timedelta(seconds=20), target.head.changed_at,
        NOW if control is None else control.changed_at,
        NOW if active is None else active.head.changed_at)
    if record is not None:
        ec = replace(evaluation_control(), workspace_id=workspace)
        evidence = EvaluationSnapshot(EvaluationReadRequest(workspace, s.prompt_key, IDENTITY, 3,
            record.evaluation_id), ec, (ec,), record, target, at)
    return LifecycleSnapshot(request, control, history, target, active, evidence, at,
        None if evidence is None else evidence.control)


def plan(capture=None, status=PromptLifecycle.EVALUATED):
    capture = snapshot(record=evaluation()) if capture is None else capture
    return LifecycleWrite(capture, status, capture.observed_at + timedelta(seconds=1), 'OPERATOR_REVIEWED')


def progressed(current, status, seconds=4, evaluation_id='evaluation-1'):
    head = advance(current.head, status, seconds, evaluation_id=evaluation_id)
    return PromptRegistryState(head, current.history + (head,))


class LifecycleContractTests(unittest.TestCase):
    def test_evaluated_proposal_binds_actor_evaluation_and_shared_control(self):
        p = plan()
        head = p.after_states[0].head
        self.assertIs(head.status, PromptLifecycle.EVALUATED)
        self.assertEqual(head.evaluation_id, evaluation().evaluation_id)
        self.assertEqual(head.changed_by, IDENTITY.principal.principal_id)
        self.assertEqual(p.control.transition_digests, (head.digest,))
        self.assertEqual(p.control.revision, 1)
        self.assertIsNone(p.control.active_snapshot_sha256)
        self.assertEqual(decode_control(encode_control(p.control)), p.control)

    def test_registration_and_block_require_no_evaluation_or_activation(self):
        target = PromptRegistryState(draft(), (draft(),))
        for status in (PromptLifecycle.REGISTERED, PromptLifecycle.BLOCKED):
            p = plan(snapshot(target), status)
            self.assertIs(p.after_states[0].head.status, status)
            self.assertIsNone(p.control.active_snapshot_sha256)

    def test_missing_failed_incomplete_or_changed_policy_cannot_promote(self):
        original = snapshot(record=evaluation())
        old = original.evaluation.control
        new = replace(old, revision=2, parent_digest=old.digest,
            updated_at=old.updated_at + timedelta(seconds=1), policy=replace(old.policy, version='v2'))
        changed = replace(original, evaluation=replace(original.evaluation,
            control=new, control_history=(old, new)))
        failed = replace(evaluation(), results=(replace(evaluation().results[0],
            returned_model_id='unapproved'),) + evaluation().results[1:])
        for capture in (snapshot(), snapshot(record=evaluation(results=())), snapshot(record=failed), changed):
            with self.assertRaises(ContractError):
                plan(capture)

    def test_activation_retires_other_version_atomically_with_preserved_evaluation(self):
        candidate = progressed(state(), PromptLifecycle.EVALUATED)
        other_draft = initial_prompt_record(replace(draft().snapshot, vertex_version_id='2'),
            imported_at=NOW, imported_by=draft().changed_by)
        old = PromptRegistryState(other_draft, (other_draft,))
        old = progressed(old, PromptLifecycle.REGISTERED, 1, None)
        old = progressed(old, PromptLifecycle.EVALUATED)
        old = progressed(old, PromptLifecycle.ACTIVE, 5)
        base = plan()
        selected = replace(base.control, active_snapshot_sha256=old.head.snapshot.snapshot_sha256)
        p = plan(snapshot(candidate, active=old, control=selected, history=(selected,),
            record=evaluation()), PromptLifecycle.ACTIVE)
        by_version = {s.head.snapshot.vertex_version_id: s for s in p.after_states}
        self.assertIs(by_version[candidate.head.snapshot.vertex_version_id].head.status, PromptLifecycle.ACTIVE)
        self.assertIs(by_version['2'].head.status, PromptLifecycle.RETIRED)
        self.assertEqual(by_version['2'].head.evaluation_id, old.head.evaluation_id)
        self.assertEqual(p.control.active_snapshot_sha256, candidate.head.snapshot.snapshot_sha256)
        self.assertEqual(p.control.parent_digest, selected.digest)

    def test_retirement_block_and_rollback_preserve_evaluation_and_active_pin(self):
        current = progressed(progressed(state(), PromptLifecycle.EVALUATED), PromptLifecycle.ACTIVE, 5)
        selected = replace(plan().control, active_snapshot_sha256=current.head.snapshot.snapshot_sha256)
        for status in (PromptLifecycle.RETIRED, PromptLifecycle.BLOCKED):
            p = plan(snapshot(current, active=current, control=selected, history=(selected,)), status)
            self.assertIsNone(p.control.active_snapshot_sha256)
            self.assertEqual(p.after_states[0].head.evaluation_id, current.head.evaluation_id)
        retired = progressed(current, PromptLifecycle.RETIRED, 6)
        rollback = plan(snapshot(retired, record=evaluation()), PromptLifecycle.ACTIVE)
        self.assertEqual(rollback.after_states[0].head.evaluation_id, current.head.evaluation_id)
        with self.assertRaises(ContractError):
            replace(rollback, changed_at=evaluation().valid_until)

    def test_global_logical_slot_ignores_workspace_actor_or_target_content(self):
        p = plan()
        other = plan(snapshot(record=evaluation(), workspace='other-workspace'))
        self.assertEqual(p.job_id, other.job_id)
        self.assertNotEqual(p.command_digest, other.command_digest)
        same = plan(snapshot(), PromptLifecycle.BLOCKED)
        self.assertEqual(p.job_id, same.job_id)

    def test_corrupt_duplicate_missing_or_rebound_control_chain_denied(self):
        first = plan().control
        next_control = replace(first, revision=2, parent_digest=first.digest,
            changed_at=first.changed_at + timedelta(seconds=1))
        validate_control_history(next_control, (first, next_control))
        for history in ((), (next_control,), (first, first),
                        (first, replace(next_control, parent_digest='a'*64))):
            with self.assertRaises(ContractError):
                validate_control_history(next_control, history)
        for row in (encode_control(first) | {'revision': True},
                    encode_control(first) | {'control_digest': 'b'*64},
                    encode_control(first) | {'extra': 'x'}):
            with self.assertRaises(ContractError):
                decode_control(row)

    def test_missing_control_cannot_adopt_existing_active_or_wrong_scope(self):
        active = progressed(progressed(state(), PromptLifecycle.EVALUATED), PromptLifecycle.ACTIVE, 5)
        with self.assertRaises(ContractError):
            snapshot(active, active=active)
        current = snapshot(record=evaluation())
        with self.assertRaises(ContractError):
            replace(current, request=replace(current.request, vertex_version_id='8'))
        with self.assertRaises(ContractError):
            replace(plan(), changed_at=NOW)

    def test_history_cap_refuses_transition_without_truncation(self):
        current = state()
        head = current.head
        records = list(current.history)
        head = advance(head, PromptLifecycle.EVALUATED, 4, evaluation_id='evaluation-1')
        records.append(head)
        for i in range(5, 66):
            head = advance(head, PromptLifecycle.ACTIVE if head.status is not PromptLifecycle.ACTIVE
                else PromptLifecycle.RETIRED, i)
            records.append(head)
            if len(records) == 64:
                break
        full = PromptRegistryState(head, tuple(records))
        selected = replace(plan().control, active_snapshot_sha256=head.snapshot.snapshot_sha256)
        with self.assertRaises(ContractError):
            plan(snapshot(full, active=full, control=selected, history=(selected,)), PromptLifecycle.RETIRED)

    def test_setup_is_metadata_only_repeatable_and_preflights_before_create(self):
        client = Mock(project=PROJECT, location=LOCATION)
        client.get_dataset.return_value = dataset()
        tables = {}
        def get_table(ref, **kwargs):
            if ref.table_id not in tables:
                raise NotFound('missing')
            return tables[ref.table_id]
        def create(table, **kwargs):
            table._properties.update(type='TABLE', location=LOCATION)
            tables[table.table_id] = table
            return table
        client.get_table.side_effect, client.create_table.side_effect = get_table, create
        self.assertEqual(setup(client)['status'], 'MISSING_RESOURCES')
        self.assertEqual(len(setup(client, apply=True)['created_tables']), 2)
        self.assertEqual(setup(client, apply=True)['created_tables'], [])
        client.query.assert_not_called()
        broken = TABLES['prompt_lifecycle_history'].table(PROJECT)
        broken._properties.update(type='TABLE', location=LOCATION)
        broken.clustering_fields = ['task']
        tables.clear()
        tables[broken.table_id] = broken
        calls = client.create_table.call_count
        with self.assertRaises(SchemaMismatch):
            setup(client, apply=True)
        self.assertEqual(client.create_table.call_count, calls)


if __name__ == '__main__':
    unittest.main()
