"""Offline authoritative prompt-registry reader tests; no cloud calls."""
from copy import deepcopy
from datetime import timedelta
import re
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import Forbidden
from google.cloud import bigquery

from src.application.operation_budget import (
    OperationSettings,
    OperationStopped,
    operation_scope,
)
from src.application.prompt_contract import PromptLifecycle, PromptTask
from src.application.prompt_registry_codec import (
    MAX_PROMPT_HISTORY,
    PromptRegistryState,
    encode_registry_state,
)
from src.application.prompt_registry_reader import (
    BigQueryPromptRegistryReader,
    PromptRegistryLookupMode,
    PromptRegistryReadError,
    PromptRegistryReadRequest,
    PromptRegistryReadSettings,
    _decode,
    read_config,
    read_sql,
)
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_workflow_tables import PROJECT
from tests.test_prompt_contract import NOW
from tests.test_prompt_registry_codec import lifecycle, rows


def output(*, current=None, history=None, **changes):
    stored, events = rows()
    stored = stored if current is None else current
    events = list(events) if history is None else history
    value = {
        'observed_at': NOW + timedelta(days=1),
        'head_count': 1,
        'history_count': len(events),
        'heads': [stored],
        'history': events,
    }
    return value | changes


def native_job(job_id, sql, config, result, *, job_sql=None,
               state='DONE', error=None):
    job = bigquery.QueryJob(
        job_id, sql if job_sql is None else job_sql,
        client=Mock(project=PROJECT), job_config=config)
    job._properties['jobReference'].update(
        projectId=PROJECT, location=LOCATION)
    job._properties['status'] = {'state': state}
    if error is not None:
        job._properties['status']['errorResult'] = error
    job._properties['statistics'] = {'query': {'statementType': 'SCRIPT'}}
    job.result = Mock(return_value=result)
    return job


class ClientFixture:
    project = PROJECT
    location = LOCATION
    default_query_job_config = None

    def __init__(self, result):
        self.result = result
        self.calls = []
        self.job_sql = None
        self.job_error = None

    def query(self, sql, **kwargs):
        self.calls.append((sql, kwargs))
        return native_job(
            kwargs['job_id'], sql, kwargs['job_config'],
            deepcopy(self.result), job_sql=self.job_sql,
            error=self.job_error)


class PromptRegistryReaderTests(unittest.TestCase):
    def setUp(self):
        self.client = ClientFixture([output()])
        self.inspect = patch(
            'src.application.prompt_registry_reader.inspect',
            return_value={'status': 'VERIFIED'}).start()
        self.addCleanup(patch.stopall)
        self.reader = BigQueryPromptRegistryReader(self.client)

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(
                (ContractError, PromptRegistryReadError)) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_sql_is_fixed_single_snapshot_complete_and_partition_filtered(self):
        sql = read_sql()
        self.assertTrue(sql.startswith('BEGIN TRANSACTION;'))
        self.assertIn('CREATE TEMP TABLE prompt_registry_read_snapshot', sql)
        self.assertIn('COMMIT TRANSACTION;', sql)
        self.assertIn('EXACT_VERTEX_VERSION', sql)
        self.assertIn("status = 'ACTIVE'", sql)
        self.assertIn("TIMESTAMP('0001-01-01 00:00:00+00')", sql)
        self.assertIn("TIMESTAMP('9999-12-31 23:59:59.999999+00')", sql)
        self.assertIn(f'LIMIT {MAX_PROMPT_HISTORY + 1}', sql)
        self.assertIn('LIMIT 2', sql)
        self.assertNotIn('SELECT * FROM `', sql)
        references = re.findall(r'`([^`]+)`', sql)
        self.assertEqual(set(references), {
            f'{PROJECT}.ci_application.prompt_registry',
            f'{PROJECT}.ci_application.prompt_registry_history'})

    def test_requests_and_configs_are_typed_bounded_and_parameterized(self):
        exact = PromptRegistryReadRequest.exact('1234567890', '4')
        eligible = PromptRegistryReadRequest.eligible(
            PromptTask.PLANNER, 'campaign-planner')
        self.assertEqual(exact.mode,
                         PromptRegistryLookupMode.EXACT_VERTEX_VERSION)
        self.assertEqual(eligible.mode,
                         PromptRegistryLookupMode.ELIGIBLE_HEAD)
        for request in (exact, eligible):
            config = read_config(request, PromptRegistryReadSettings())
            values = {item.name: item.value
                      for item in config.query_parameters}
            self.assertEqual(set(values), {
                'lookup_mode', 'vertex_project', 'vertex_location',
                'vertex_prompt_id', 'vertex_version_id', 'task',
                'prompt_key'})
            self.assertEqual(values['vertex_project'], PROJECT)
            self.assertEqual(values['vertex_location'], LOCATION)
            self.assertFalse(config.use_query_cache)
            self.assertFalse(config.use_legacy_sql)
            self.assertFalse(config.create_session)
            self.assertEqual(config.maximum_bytes_billed, 64 * 1024**2)
            self.assertEqual(int(config.job_timeout_ms), 30_000)
            self.assertEqual(config.labels,
                             {'workflow': 'prompt-registry-read-v1'})

    def test_exact_read_returns_complete_validated_state_with_fresh_job(self):
        first = self.reader.read_exact('1234567890', '4')
        second = self.reader.read_exact('1234567890', '4')
        self.assertEqual(first.state.history, lifecycle())
        self.assertEqual(first.state.head.status, PromptLifecycle.ACTIVE)
        self.assertEqual(first.observed_at, NOW + timedelta(days=1))
        self.assertEqual(len(self.client.calls), 2)
        self.assertNotEqual(self.client.calls[0][1]['job_id'],
                            self.client.calls[1][1]['job_id'])
        for sql, kwargs in self.client.calls:
            self.assertEqual(sql, read_sql())
            self.assertEqual((kwargs['project'], kwargs['location']),
                             (PROJECT, LOCATION))
            self.assertIsNone(kwargs['retry'])
            self.assertIsNone(kwargs['job_retry'])
            self.assertEqual(kwargs['api_method'],
                             bigquery.enums.QueryApiMethod.INSERT)
        self.assertEqual(self.inspect.call_count, 2)

    def test_eligible_read_requires_exact_active_task_and_prompt_key(self):
        result = self.reader.read_eligible(
            PromptTask.PLANNER, 'campaign-planner')
        self.assertEqual(result.state.head.status, PromptLifecycle.ACTIVE)
        request = self.client.calls[0][1]['job_config'].query_parameters
        values = {item.name: item.value for item in request}
        self.assertEqual((values['task'], values['prompt_key']),
                         ('PLANNER', 'campaign-planner'))
        retired = lifecycle()[:-1]
        current, history = encode_registry_state(
            PromptRegistryState(retired[-1], retired))
        self.assert_code(
            'PROMPT_REGISTRY_NOT_ELIGIBLE', _decode,
            PromptRegistryReadRequest.eligible(
                PromptTask.PLANNER, 'campaign-planner'),
            [output(current=current, history=list(history))])

    def test_missing_duplicate_partial_and_overlong_results_fail_closed(self):
        request = PromptRegistryReadRequest.exact('1234567890', '4')
        cases = (
            ('PROMPT_REGISTRY_NOT_FOUND', output(
                head_count=0, history_count=0, heads=[], history=[])),
            ('PROMPT_REGISTRY_DUPLICATE_HEAD', output(
                head_count=2, heads=output()['heads'] * 2)),
            ('PROMPT_REGISTRY_INCOMPLETE_HISTORY', output(
                history_count=7)),
            ('PROMPT_REGISTRY_INCOMPLETE_HISTORY', output(
                history=output()['history'] * 11,
                history_count=len(output()['history'] * 11))),
        )
        for code, row in cases:
            with self.subTest(code=code):
                self.assert_code(code, _decode, request, [row])

    def test_codec_rejects_conflicting_or_malformed_complete_chains(self):
        request = PromptRegistryReadRequest.exact('1234567890', '4')
        current, history_tuple = rows()
        history = list(history_tuple)
        cases = []
        missing = deepcopy(history); missing.pop(2); cases.append(missing)
        changed = deepcopy(history); changed[-1]['record_digest'] = 'f' * 64; cases.append(changed)
        reordered = list(reversed(deepcopy(history))); cases.append(reordered)
        for events in cases:
            row = output(current=deepcopy(current), history=events)
            with self.subTest(events=events), self.assertRaises(
                    PromptRegistryReadError):
                self.client.result = [row]
                self.reader.read(request)

    def test_request_scope_and_result_shape_are_strict(self):
        invalid = (
            lambda: PromptRegistryReadRequest.exact('latest', '4'),
            lambda: PromptRegistryReadRequest.exact('01', '4'),
            lambda: PromptRegistryReadRequest.eligible(
                'PLANNER', 'campaign-planner'),
            lambda: PromptRegistryReadRequest.eligible(
                PromptTask.PLANNER, 'bad key'),
            lambda: PromptRegistryReadRequest(
                PromptRegistryLookupMode.EXACT_VERTEX_VERSION,
                vertex_prompt_id='1', vertex_version_id='2',
                task=PromptTask.PLANNER),
        )
        for build in invalid:
            with self.subTest(build=build):
                self.assert_code(
                    'INVALID_PROMPT_REGISTRY_READ_REQUEST', build)
        request = PromptRegistryReadRequest.exact('1234567890', '4')
        for result in ([], [output(), output()], [{}],
                       [output(extra='x')],
                       [output(observed_at=NOW.replace(tzinfo=None))]):
            with self.subTest(result=result), self.assertRaises(
                    (ContractError, PromptRegistryReadError)):
                _decode(request, result)

    def test_exact_result_identity_and_eligible_result_scope_must_match(self):
        exact = PromptRegistryReadRequest.exact('1234567890', '5')
        self.assert_code('PROMPT_REGISTRY_SCOPE_MISMATCH',
                         _decode, exact, [output()])
        eligible = PromptRegistryReadRequest.eligible(
            PromptTask.INTERPRETER, 'campaign-planner')
        self.assert_code('PROMPT_REGISTRY_NOT_ELIGIBLE',
                         _decode, eligible, [output()])

    def test_schema_job_and_provider_failures_have_no_fallback(self):
        self.inspect.return_value = {'status': 'MISSING_RESOURCES'}
        self.assert_code('PROMPT_REGISTRY_RESOURCES_MISSING',
                         self.reader.read_exact, '1234567890', '4')
        self.assertEqual(self.client.calls, [])
        self.inspect.return_value = {'status': 'VERIFIED'}
        self.client.job_sql = 'SELECT 1'
        self.assert_code('PROMPT_REGISTRY_READ_JOB_MISMATCH',
                         self.reader.read_exact, '1234567890', '4')
        self.client.job_sql = None
        self.client.query = Mock(side_effect=Forbidden(
            'private prompt provider details'))
        with self.assertRaises(PromptRegistryReadError) as caught:
            self.reader.read_exact('1234567890', '4')
        self.assertEqual(caught.exception.code,
                         'PROMPT_REGISTRY_STORE_UNAVAILABLE')
        self.assertNotIn('private', str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)

    def test_settings_client_and_operation_budget_fail_closed(self):
        for values in (
                {'maximum_bytes_billed': True},
                {'maximum_bytes_billed': 1024**3 + 1},
                {'job_timeout_ms': 0},
                {'result_timeout_seconds': 61}):
            with self.subTest(values=values):
                self.assert_code(
                    'INVALID_PROMPT_REGISTRY_READ_SETTINGS',
                    PromptRegistryReadSettings, **values)
        for client in (
                Mock(project='other', location=LOCATION,
                     default_query_job_config=None),
                Mock(project=PROJECT, location='US',
                     default_query_job_config=None),
                Mock(project=PROJECT, location=LOCATION,
                     default_query_job_config=bigquery.QueryJobConfig())):
            with self.assertRaises(ValueError):
                BigQueryPromptRegistryReader(client)

        tick = [100.0]
        original_query = self.client.query

        def slow_query(*args, **kwargs):
            tick[0] += 10
            return original_query(*args, **kwargs)

        self.client.query = slow_query
        with patch('src.application.operation_budget.time.monotonic',
                   side_effect=lambda: tick[0]), self.assertRaises(
                       OperationStopped):
            with operation_scope(OperationSettings(10)):
                self.reader.read_exact('1234567890', '4')


if __name__ == '__main__':
    unittest.main()
