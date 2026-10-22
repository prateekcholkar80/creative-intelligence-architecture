"""Pure prompt registry codec/registration-plan tests; no clients or I/O."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

from google.cloud import bigquery

from src.application.prompt_contract import (
    PromptLifecycle, PromptRegistryRecord,
)
from src.application.prompt_registry_codec import (
    MAX_PROMPT_HISTORY, PromptRegistryState, decode_registry_state,
    encode_registry_state, initial_registration_state, normalize,
)
from src.application.prompt_registry_schema import STORAGE_VERSION, TABLES
from src.application.workflow_contract import ContractError
from tests.test_prompt_contract import ACTOR, NOW, advance, draft


def lifecycle():
    records = [draft()]
    records.append(advance(records[-1], PromptLifecycle.REGISTERED, 1))
    records.append(advance(records[-1], PromptLifecycle.EVALUATED, 2,
                           evaluation_id='evaluation-1'))
    records.append(advance(records[-1], PromptLifecycle.ACTIVE, 3))
    records.append(advance(records[-1], PromptLifecycle.RETIRED, 4))
    records.append(advance(records[-1], PromptLifecycle.ACTIVE, 5,
                           reason_code='AUTHORIZED_ROLLBACK'))
    return tuple(records)


def rows(records=None):
    records = lifecycle() if records is None else records
    return encode_registry_state(PromptRegistryState(records[-1], records))


class PromptRegistryCodecTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_initial_registration_plan_is_exact_draft_and_immutable(self):
        record = draft()
        state = initial_registration_state(record)
        self.assertEqual((state.head, state.history), (record, (record,)))
        current, history = encode_registry_state(state)
        self.assertEqual(current['status'], 'DRAFT')
        self.assertEqual(current['revision'], 1)
        self.assertEqual(current['imported_at'], NOW)
        self.assertEqual(current['imported_by'], ACTOR)
        self.assertEqual(len(history), 1)
        with self.assertRaises(FrozenInstanceError): state.head = record

    def test_complete_lifecycle_round_trip_recalculates_all_hashes(self):
        records = lifecycle(); state = PromptRegistryState(records[-1], records)
        current, history = encode_registry_state(state)
        decoded = decode_registry_state(current, list(history))
        self.assertEqual(decoded, state)
        self.assertEqual(current['snapshot_sha256'], records[0].snapshot.snapshot_sha256)
        self.assertEqual(current['template_sha256'], records[0].snapshot.template_sha256)
        self.assertEqual(current['record_digest'], records[-1].digest)
        self.assertEqual([item['revision'] for item in history], list(range(1, 7)))

    def test_rows_match_every_physical_destination_field_exactly(self):
        current, history = rows()
        self.assertEqual(set(current), {field.name for field in TABLES['prompt_registry'].schema})
        self.assertEqual(set(history[0]), {
            field.name for field in TABLES['prompt_registry_history'].schema})
        self.assertEqual(current['schema_version'], STORAGE_VERSION)
        for name in ('generation_config_canonical_json',
                     'safety_settings_canonical_json', 'output_schema_canonical_json'):
            self.assertEqual(json.dumps(json.loads(current[name]), sort_keys=True,
                                        separators=(',', ':')), current[name])
        self.assertEqual(current['required_variables'], sorted(current['required_variables']))

    def test_current_missing_extra_storage_version_and_types_fail_closed(self):
        current, history = rows()
        cases = []
        item = deepcopy(current); item.pop('model_id'); cases.append(item)
        item = deepcopy(current); item['extra'] = 'x'; cases.append(item)
        item = deepcopy(current); item['schema_version'] = 'future'; cases.append(item)
        item = deepcopy(current); item['revision'] = True; cases.append(item)
        item = deepcopy(current); item['required_variables'] = tuple(item['required_variables']); cases.append(item)
        item = deepcopy(current); item['changed_at'] = item['changed_at'].replace(tzinfo=None); cases.append(item)
        for item in cases:
            with self.subTest(keys=set(item)):
                with self.assertRaises(ContractError):
                    decode_registry_state(item, list(history))

    def test_canonical_config_text_and_content_hashes_cannot_change(self):
        current, history = rows()
        mutations = {
            'generation_config_canonical_json': lambda value: json.dumps(json.loads(value), indent=2),
            'safety_settings_canonical_json': lambda value: value.replace('BLOCK_', 'OFF_'),
            'output_schema_canonical_json': lambda value: value.replace('STRING', 'NUMBER'),
            'system_instruction': lambda value: value + ' Changed.',
            'snapshot_sha256': lambda value: 'f' * 64,
            'template_sha256': lambda value: 'e' * 64,
        }
        for name, mutate in mutations.items():
            item = deepcopy(current); item[name] = mutate(item[name])
            with self.subTest(name=name), self.assertRaises(ContractError):
                decode_registry_state(item, list(history))

    def test_current_and_history_identity_or_record_digest_cannot_diverge(self):
        current, history = rows()
        cases = []
        item = deepcopy(history); item[2]['vertex_version_id'] = '5'; cases.append(item)
        item = deepcopy(history); item[-1]['record_digest'] = 'f' * 64; cases.append(item)
        item = deepcopy(history); item[-1]['status'] = 'UNKNOWN'; cases.append(item)
        item = deepcopy(history); item[-1]['task'] = 'INTERPRETER'; cases.append(item)
        for item in cases:
            with self.subTest(change=item[-1]):
                with self.assertRaises(ContractError):
                    decode_registry_state(current, item)

    def test_history_must_be_complete_ordered_unique_and_end_at_head(self):
        current, history_tuple = rows(); history = list(history_tuple)
        cases = [list(reversed(history)), history[:-1], history + [deepcopy(history[-1])],
                 history[:2] + history[3:], []]
        for item in cases:
            with self.subTest(revisions=[row.get('revision') for row in item]), \
                    self.assertRaises(ContractError):
                decode_registry_state(current, item)
        self.assert_code('INVALID_PROMPT_REGISTRY_HISTORY',
            decode_registry_state, current, history * (MAX_PROMPT_HISTORY // len(history) + 1))

    def test_import_identity_is_bound_to_first_history_record(self):
        current, history = rows()
        for name, value in (('imported_by', 'other-actor'),
                            ('imported_at', NOW + timedelta(seconds=1))):
            item = deepcopy(current); item[name] = value
            self.assert_code('PROMPT_STORAGE_IMPORT_IDENTITY_MISMATCH',
                             decode_registry_state, item, list(history))

    def test_initial_and_transition_semantics_are_replayed_not_inferred(self):
        first = draft()
        altered_initial = replace(first, reason_code='OTHER_IMPORT')
        self.assert_code('INVALID_PROMPT_REGISTRY_INITIAL_STATE',
                         initial_registration_state, altered_initial)
        registered = advance(first, PromptLifecycle.REGISTERED, 1)
        invalid = PromptRegistryRecord(3, registered.digest, registered.snapshot,
            PromptLifecycle.RETIRED, NOW + timedelta(seconds=2), ACTOR,
            'INVALID_SKIP', 'evaluation-1')
        with self.assertRaises(ContractError):
            PromptRegistryState(invalid, (first, registered, invalid))

    def test_snapshot_and_head_objects_must_remain_identical_across_history(self):
        records = list(lifecycle())
        changed = replace(records[-1].snapshot, vertex_version_id='5')
        records[-1] = replace(records[-1], snapshot=changed)
        with self.assertRaises(ContractError):
            PromptRegistryState(records[-1], tuple(records))
        state = PromptRegistryState(lifecycle()[-1], lifecycle())
        with self.assertRaises(ContractError):
            PromptRegistryState(state.history[-2], state.history)

    def test_native_bigquery_rows_are_normalized_without_generic_serializers(self):
        current, history = rows()

        def native(value):
            if type(value) is dict:
                return bigquery.Row(tuple(native(item) for item in value.values()),
                    {key: index for index, key in enumerate(value)})
            if type(value) is list:
                return [native(item) for item in value]
            return value

        self.assertEqual(decode_registry_state(native(current),
            native(list(history))).head, lifecycle()[-1])
        class Arbitrary:
            def items(self): raise AssertionError('must not inspect arbitrary objects')
        self.assertIs(normalize(Arbitrary()).__class__, Arbitrary)

    def test_codec_has_no_cloud_client_or_query_dependency(self):
        with patch('google.cloud.bigquery.Client',
                   side_effect=AssertionError('no client expected')):
            current, history = rows()
            self.assertEqual(decode_registry_state(current, list(history)).history,
                             lifecycle())


if __name__ == '__main__':
    unittest.main()
