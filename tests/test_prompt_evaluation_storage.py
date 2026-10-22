"""Explicit evaluation schemas/codecs/setup; offline SDK fixtures."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock
import unittest

from google.api_core.exceptions import Conflict, NotFound

from src.application.prompt_evaluation_storage import (
    TABLES, EvaluationControl, canonical, decode_control, decode_control_history,
    decode_evaluation, encode_control, encode_evaluation, parse_canonical,
)
from src.application.workflow_contract import ContractError
from src.ingestion.setup_prompt_evaluation_tables import inspect, setup
from src.ingestion.setup_workflow_tables import PROJECT
from tests.test_prompt_evaluation_contract import evaluation, state
from tests.test_setup_workflow_tables import dataset
from src.application.workflow_schema import LOCATION


def control():
    record = evaluation()
    return EvaluationControl('workspace-1', record.registry_record.snapshot.prompt_key,
        1, None, record.benchmark, record.policy, record.registry_record.changed_at,
        record.recorded_by)


class EvaluationStorageTests(unittest.TestCase):
    def test_explicit_schemas_and_roundtrip_records(self):
        current, record = control(), evaluation()
        self.assertEqual(len(TABLES), 4)
        self.assertEqual(decode_control(encode_control(current)), current)
        row, cases = encode_evaluation(current, record)
        self.assertEqual(decode_evaluation(current, row, cases, state()), record)
        self.assertEqual({f.name for f in TABLES['prompt_evaluations'].schema}, set(row))
        self.assertEqual(decode_control_history(current, [encode_control(current)]), (current,))

    def test_failed_and_incomplete_observations_roundtrip(self):
        for results in ((), evaluation().results[:-1]):
            record = replace(evaluation(), results=results)
            row, cases = encode_evaluation(control(), record)
            self.assertEqual(decode_evaluation(control(), row, cases, state()), record)
        record = evaluation()
        record = replace(record, results=(replace(record.results[0], returned_model_id='wrong'),)
            + record.results[1:])
        row, cases = encode_evaluation(control(), record)
        self.assertEqual(decode_evaluation(control(), row, cases, state()), record)

    def test_corrupt_control_shape_types_hash_and_json_fail(self):
        base = encode_control(control())
        for changes in ({'control_digest': 'a' * 64}, {'revision': True},
                        {'extra': 'x'}, {'task': 'INTERPRETER'},
                        {'binding_canonical_json': '{}'}, {'schema_version': 'bad'}):
            with self.subTest(changes=changes), self.assertRaises((ContractError, ValueError)):
                decode_control(base | changes)

    def test_noncanonical_duplicate_and_oversized_json_denied(self):
        for value in ('{ "x": 1}', '{"x":1,"x":1}', 'x' * 65537, 1):
            with self.assertRaises(ContractError):
                parse_canonical(value)

    def test_corrupt_summary_or_cases_never_decode(self):
        row, cases = encode_evaluation(control(), evaluation())
        for changes in ({'status': 'FAILED'}, {'registry_revision': 99},
                        {'evaluation_digest': 'a' * 64}, {'result_count': 0},
                        {'workspace_id': 'other'}, {'control_digest': 'a' * 64},
                        {'payload_canonical_json': '{}'}, {'registry_digest': 'f' * 64}):
            with self.subTest(changes=changes), self.assertRaises((ContractError, ValueError)):
                decode_evaluation(control(), row | changes, cases, state())
        for changed in (cases[::-1], cases[:-1], cases + [cases[0]],
                        [cases[0] | {'invocation_id': 'other'}] + cases[1:],
                        [cases[0] | {'observation_sha256': '0' * 64}] + cases[1:]):
            with self.assertRaises(ContractError):
                decode_evaluation(control(), row, changed, state())

    def test_control_chain_and_immutable_policy_versions(self):
        first = control()
        second = replace(first, revision=2, parent_digest=first.digest,
            updated_at=first.updated_at + timedelta(seconds=1))
        self.assertEqual(len(decode_control_history(second,
            [encode_control(first), encode_control(second)])), 2)
        for rows in ([encode_control(second)], [encode_control(second), encode_control(first)]):
            with self.assertRaises(ContractError):
                decode_control_history(second, rows)
        rebound = replace(second, policy=replace(second.policy,
            allowed_returned_models=('other-model',)))
        with self.assertRaises(ContractError):
            decode_control_history(rebound, [encode_control(first), encode_control(rebound)])

    def test_control_mismatch_and_future_selection_cannot_encode(self):
        record = evaluation()
        for current in (replace(control(), prompt_key='other'),
                        replace(control(), updated_at=record.completed_at),
                        replace(control(), policy=replace(control().policy, version='v2'))):
            with self.assertRaises(ContractError):
                encode_evaluation(current, record)

    def test_missing_setup_is_read_only_apply_is_idempotent(self):
        client = Mock(project=PROJECT, location=LOCATION)
        client.get_dataset.return_value = dataset()
        present = {}
        def get(reference, **kwargs):
            if reference.table_id not in present:
                raise NotFound('absent')
            return deepcopy(present[reference.table_id])
        def create(table, **kwargs):
            table._properties.update(type='TABLE', location=LOCATION)
            present[table.table_id] = deepcopy(table)
            return table
        client.get_table.side_effect = get
        client.create_table.side_effect = create
        self.assertEqual(len(inspect(client)['missing_tables']), 4)
        client.create_table.assert_not_called()
        self.assertEqual(len(setup(client, apply=True)['created_tables']), 4)
        self.assertEqual(setup(client, apply=True)['created_tables'], [])
        client.query.assert_not_called()

    def test_incompatible_schema_denies_before_any_creation(self):
        client = Mock(project=PROJECT, location=LOCATION)
        client.get_dataset.return_value = dataset()
        wrong = TABLES['prompt_evaluation_controls'].table(PROJECT)
        wrong.schema = []
        client.get_table.return_value = wrong
        with self.assertRaises(ValueError):
            setup(client, apply=True)
        client.create_table.assert_not_called()


if __name__ == '__main__':
    unittest.main()
