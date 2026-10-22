"""Offline initial prompt-registration SQL/query contract tests; no submission."""
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
import json
import re
import unittest
from unittest.mock import patch

from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_registry_codec import initial_registration_state
from src.application.prompt_registry_query import (
    PromptRegistrationQuery, PromptRegistrationQuerySettings,
    PromptRegistrationResultState, validate_prompt_registration_result,
)
from src.application.prompt_registry_schema import TABLES
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_workflow_tables import DATASET_ID
from tests.test_prompt_contract import NOW, advance, draft


def query(record=None):
    return PromptRegistrationQuery(initial_registration_state(draft() if record is None else record))


def parameters(config):
    return {item.name: item.value for item in config.query_parameters}


def result(plan=None, **changes):
    plan = query() if plan is None else plan
    record = plan.state.head
    values = dict(state='APPLIED', observed_at=record.changed_at,
        prompt_key=record.snapshot.prompt_key,
        snapshot_sha256=record.snapshot.snapshot_sha256,
        record_digest=record.digest)
    return [values | changes]


class PromptRegistryQueryTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_sql_is_fixed_atomic_insert_only_and_returns_minimal_receipt(self):
        sql = query().sql
        self.assertTrue(sql.startswith('-- Server-only initial prompt registration'))
        self.assertIn('BEGIN TRANSACTION;', sql)
        self.assertIn('COMMIT TRANSACTION;', sql)
        self.assertIn('ROLLBACK TRANSACTION;', sql)
        self.assertEqual(sql.count('INSERT INTO'), 2)
        self.assertNotIn('UPDATE ', sql)
        self.assertNotIn('DELETE ', sql)
        self.assertNotIn('MERGE ', sql)
        self.assertNotRegex(sql, r'__[A-Z_]+__')
        for name in TABLES:
            self.assertIn(f'`{DATASET_ID}.{name}`', sql)
        self.assertIn("candidate.status = 'DRAFT'", sql)
        self.assertIn("SET result_state = 'REUSED'", sql)
        self.assertIn("SET result_state = 'APPLIED'", sql)
        self.assertIn('LIMIT 2', sql)
        self.assertIn("TIMESTAMP('0001-01-01 00:00:00+00')", sql)
        self.assertNotIn('system_instruction AS system_instruction;', sql.split('SELECT result_state')[-1])

    def test_rendered_structs_use_every_destination_column_and_explicit_casts(self):
        sql = query().sql
        for name, spec in TABLES.items():
            declaration = ', '.join(f'{field.name} ' for field in spec.schema)
            for field in spec.schema:
                self.assertIn(field.name, sql)
            columns = ', '.join(field.name for field in spec.schema)
            self.assertIn(f'({columns})', sql)
        self.assertIn('JSON_VALUE_ARRAY(PARSE_JSON(@registry_json)', sql)
        self.assertIn('CAST(JSON_VALUE(PARSE_JSON(@registry_json)', sql)
        self.assertIn('AS INT64)', sql)
        self.assertIn('AS TIMESTAMP)', sql)

    def test_job_config_is_bounded_parameterized_and_fresh(self):
        plan = query(); first = plan.job_config(); second = plan.job_config()
        self.assertIsNot(first, second)
        self.assertEqual(plan.location, LOCATION)
        self.assertFalse(first.use_legacy_sql)
        self.assertFalse(first.use_query_cache)
        self.assertFalse(first.create_session)
        self.assertIsNone(first.destination)
        self.assertEqual(first.maximum_bytes_billed, 64 * 1024**2)
        self.assertEqual(int(first.job_timeout_ms), 60_000)
        self.assertEqual(first.script_options.statement_timeout_ms, 20_000)
        self.assertEqual(first.labels, {'workflow': 'prompt-registry-v1'})
        values = parameters(first)
        self.assertEqual(set(values), {'registry_json', 'history_json',
            'storage_version', 'contract_version', 'vertex_project', 'vertex_location'})
        self.assertEqual(set(re.findall(r'(?<!@)@([a-z_]+)', plan.sql)), set(values))
        current = json.loads(values['registry_json'])
        history = json.loads(values['history_json'])
        self.assertEqual(current['status'], 'DRAFT')
        self.assertEqual(history['revision'], 1)
        self.assertEqual(current['snapshot_sha256'], history['snapshot_sha256'])

    def test_stable_job_id_uses_vertex_identity_not_mutable_import_metadata(self):
        first = query()
        later = query(draft(imported_at=NOW + timedelta(seconds=1),
                            imported_by='iap-v1:' + 'b' * 64))
        self.assertEqual(first.job_id, later.job_id)
        changed_snapshot = replace(first.state.head.snapshot,
            system_instruction=first.state.head.snapshot.system_instruction + ' Review.')
        changed_record = first.state.head.__class__(1, None, changed_snapshot,
            PromptLifecycle.DRAFT, NOW, first.state.head.changed_by,
            'IMPORTED_FROM_VERTEX')
        self.assertEqual(first.job_id, query(changed_record).job_id)
        self.assertRegex(first.job_id, r'prompt_register_[0-9a-f]{64}')
        self.assertNotIn(first.state.head.snapshot.vertex_prompt_id, first.job_id)
        other = replace(first.state.head.snapshot, vertex_version_id='5')
        other_record = draft().__class__(1, None, other, PromptLifecycle.DRAFT,
            NOW, draft().changed_by, 'IMPORTED_FROM_VERTEX')
        self.assertNotEqual(first.job_id, query(other_record).job_id)

    def test_only_exact_initial_draft_state_can_build_a_query(self):
        registered = advance(draft(), PromptLifecycle.REGISTERED, 1)
        from src.application.prompt_registry_codec import PromptRegistryState
        state = PromptRegistryState(registered, (draft(), registered))
        self.assert_code('INVALID_INITIAL_PROMPT_REGISTRATION',
                         PromptRegistrationQuery, state)
        for settings in (None, {}, 'settings'):
            with self.subTest(settings=settings), self.assertRaises(ContractError):
                PromptRegistrationQuery(initial_registration_state(draft()), settings)

    def test_setting_bounds_are_strict_and_immutable(self):
        for changes in (dict(maximum_bytes_billed=0),
                dict(maximum_bytes_billed=1024**3 + 1),
                dict(maximum_bytes_billed=True), dict(job_timeout_ms=60_001),
                dict(statement_timeout_ms=0),
                dict(statement_timeout_ms=20_001, job_timeout_ms=20_000)):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                PromptRegistrationQuerySettings(**changes)
        settings = PromptRegistrationQuerySettings()
        with self.assertRaises(FrozenInstanceError): settings.job_timeout_ms = 1

    def test_result_accepts_applied_or_reused_exact_receipt(self):
        plan = query()
        applied = validate_prompt_registration_result(plan, result(plan))
        reused = validate_prompt_registration_result(plan,
            result(plan, state='REUSED', observed_at=NOW + timedelta(days=1)))
        self.assertEqual(applied.state, PromptRegistrationResultState.APPLIED)
        self.assertEqual(reused.state, PromptRegistrationResultState.REUSED)
        self.assertEqual(applied.record_digest, plan.state.head.digest)

    def test_result_shape_state_scope_digest_and_time_fail_closed(self):
        plan = query()
        cases = [[], result(plan) * 2, [{}], result(plan, state='UNKNOWN'),
            result(plan, state=PromptRegistrationResultState.APPLIED),
            result(plan, prompt_key='other'), result(plan, snapshot_sha256='f' * 64),
            result(plan, record_digest='e' * 64),
            result(plan, observed_at=NOW - timedelta(microseconds=1)),
            result(plan, observed_at=NOW.replace(tzinfo=None)),
            [result(plan)[0] | {'extra': 'x'}]]
        for rows in cases:
            with self.subTest(rows=rows), self.assertRaises(ContractError):
                validate_prompt_registration_result(plan, rows)

    def test_plan_building_has_no_client_query_or_cloud_dependency(self):
        with patch('google.cloud.bigquery.Client',
                   side_effect=AssertionError('no client expected')):
            plan = query()
            config = plan.job_config()
            self.assertEqual(parameters(config)['vertex_project'],
                             plan.state.head.snapshot.vertex_project)


if __name__ == '__main__':
    unittest.main()
