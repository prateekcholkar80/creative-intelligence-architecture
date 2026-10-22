"""Native SDK job correlation and single-snapshot decoding with offline RPCs."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock, patch
import re
import unittest

from google.api_core.exceptions import Forbidden, NotFound, ServiceUnavailable
from google.cloud import bigquery

from src.application.prompt_evaluation_storage import TABLES, encode_control, encode_evaluation
from src.application.prompt_evaluation_store import (
    BigQueryPromptEvaluationStore, EvaluationReadRequest, EvaluationStoreSettings,
    EvaluationStoreError, decode_snapshot, read_config, read_sql,
)
from src.application.prompt_registry_codec import encode_registry_state
from src.application.prompt_registry_schema import TABLES as REGISTRY_TABLES
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES, LOCATION
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT
from tests.test_access_reader import IDENTITY
from tests.test_prompt_evaluation_contract import evaluation, state
from tests.test_prompt_evaluation_query import control_plan, result_plan, receipt
from tests.test_prompt_evaluation_storage import control
from tests.test_prompt_registry_executor import native_job, child
from tests.test_setup_workflow_tables import dataset


def request(evaluation_id=None):
    return EvaluationReadRequest(control().workspace_id, control().prompt_key, IDENTITY, 3, evaluation_id)


def snapshot_rows(*, include_evaluation=False, current=None, history=None):
    current = control() if current is None else current
    history = (current,) if history is None else history
    records = {name: [] for name in tuple(TABLES) + tuple(REGISTRY_TABLES)}
    records['prompt_evaluation_controls'] = [encode_control(current)]
    records['prompt_evaluation_control_history'] = [encode_control(item) for item in history]
    if include_evaluation:
        records['prompt_evaluations'], records['prompt_evaluation_cases'] = (
            lambda pair: ([pair[0]], pair[1]))(encode_evaluation(control(), evaluation()))
        registry, chain = encode_registry_state(state())
        records['prompt_registry'], records['prompt_registry_history'] = [registry], list(chain)
    return [dict(records, observed_at=evaluation().completed_at + timedelta(seconds=1),
        **{name + '_count': len(rows) for name, rows in records.items()})]


class Client:
    project = PROJECT
    location = LOCATION
    default_query_job_config = None

    def __init__(self, plan):
        self.plan = plan
        self.jobs = {}
        self.lookup_error = self.query_error = None
        self.calls = []
        self.children = []
        self.read_rows = snapshot_rows()
        self.read_wrong_job = False

    def get_dataset(self, *args, **kwargs):
        return dataset()

    def get_table(self, reference, **kwargs):
        spec = (dict(TABLES) | dict(REGISTRY_TABLES) | dict(WORKFLOW_TABLES))[reference.table_id]
        table = spec.table(PROJECT)
        table._properties.update(type='TABLE', location=LOCATION)
        return table

    def get_job(self, job_id, **kwargs):
        if self.lookup_error:
            raise self.lookup_error
        if job_id not in self.jobs:
            raise NotFound('missing')
        return self.jobs[job_id]

    def query(self, sql, **kwargs):
        self.calls.append((sql, kwargs))
        if kwargs['job_id'].startswith('prompt_evaluation_read_'):
            job = bigquery.QueryJob(kwargs['job_id'], sql, client=Mock(project=PROJECT),
                job_config=kwargs['job_config'])
            job._properties['jobReference'].update(projectId=PROJECT, location=LOCATION)
            job._properties['status'] = {'state': 'DONE'}
            job.result = Mock(return_value=deepcopy(self.read_rows))
            if self.read_wrong_job:
                job._properties['jobReference']['location'] = 'us-central1'
            return job
        job = native_job(self.plan, [receipt(self.plan)])
        self.jobs[kwargs['job_id']] = job
        if self.query_error:
            raise self.query_error
        return job

    def list_jobs(self, **kwargs):
        return iter(self.children)


class EvaluationStoreTests(unittest.TestCase):
    def setUp(self):
        self.plan = result_plan()
        self.client = Client(self.plan)
        self.store = BigQueryPromptEvaluationStore(self.client, EvaluationStoreSettings(True))

    def test_disabled_writes_and_schema_mismatch_cannot_submit(self):
        with self.assertRaises(ContractError):
            BigQueryPromptEvaluationStore(self.client).submit(self.plan)
        self.assertEqual(self.client.calls, [])
        with patch.object(self.client, 'get_table', side_effect=Forbidden('denied')):
            with self.assertRaises(EvaluationStoreError):
                self.store.submit(self.plan)
        self.assertEqual(self.client.calls, [])

    def test_native_write_receipt_and_duplicate_recovery_submit_once(self):
        first = self.store.submit(self.plan)
        second = self.store.submit(self.plan)
        self.assertEqual(first.native_status, SharingCommitStatus.COMMITTED)
        self.assertIsNotNone(first.receipt)
        self.assertFalse(second.submitted)
        self.assertEqual(len(self.client.calls), 1)
        self.assertIsNone(self.client.calls[0][1]['job_retry'])
        self.assertIsNone(self.client.calls[0][1]['retry'])

    def test_discarded_response_recovers_original_without_second_insert(self):
        self.client.query_error = ServiceUnavailable('lost')
        outcome = self.store.submit(self.plan)
        self.assertEqual(outcome.native_status, SharingCommitStatus.COMMITTED)
        self.assertTrue(outcome.submitted)
        self.assertEqual(len(self.client.calls), 1)

    def test_unavailable_lookup_is_unknown_and_never_submits(self):
        self.client.lookup_error = ServiceUnavailable('unknown')
        self.assertEqual(self.store.submit(self.plan).native_status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.store.recover(self.plan).native_status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.calls, [])

    def test_read_only_recovery_of_absent_job_cannot_create(self):
        self.assertEqual(self.store.recover(self.plan).native_status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.calls, [])

    def test_wrong_sql_config_or_receipt_remains_unknown(self):
        for job in (native_job(self.plan, [receipt(self.plan)], sql='SELECT 1'),
                    native_job(self.plan, [receipt(self.plan) | {'record_digest': '0' * 64}]),
                    native_job(self.plan, [receipt(self.plan)] * 2),
                    native_job(self.plan, [], config=bigquery.QueryJobConfig())):
            self.client.jobs[self.plan.job_id] = job
            self.assertEqual(self.store.submit(self.plan).native_status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.calls, [])

    def test_failed_scripts_require_positive_correlated_children(self):
        error = {'reason': 'invalidQuery', 'message': 'error'}
        self.client.jobs[self.plan.job_id] = native_job(self.plan, [], error=error)
        self.assertEqual(self.store.recover(self.plan).native_status, SharingCommitStatus.UNKNOWN)
        self.client.children = [child(self.plan, 'ROLLBACK_TRANSACTION')]
        self.assertEqual(self.store.recover(self.plan).native_status, SharingCommitStatus.NOT_COMMITTED)
        self.client.children = [child(self.plan, 'COMMIT_TRANSACTION')]
        outcome = self.store.recover(self.plan)
        self.assertEqual(outcome.native_status, SharingCommitStatus.COMMITTED)
        self.assertIsNone(outcome.receipt)

    def test_actual_sdk_read_correlates_and_decodes_one_snapshot(self):
        self.client.read_rows = snapshot_rows(include_evaluation=True)
        captured = self.store.read(request('evaluation-1'))
        self.assertEqual(captured.record, evaluation())
        self.assertEqual(captured.registry_state, state())
        self.assertEqual(len(self.client.calls), 1)
        self.assertFalse(self.client.calls[0][1]['job_config'].use_query_cache)

    def test_read_job_mismatch_does_not_release_data(self):
        self.client.read_wrong_job = True
        with self.assertRaises(EvaluationStoreError):
            self.store.read(request())

    def test_duplicate_partial_or_orphan_read_records_are_denied(self):
        row = snapshot_rows(include_evaluation=True)[0]
        for changed in (row | {'prompt_evaluations_count': 2},
                row | {'prompt_evaluation_controls': [], 'prompt_evaluation_controls_count': 0},
                row | {'prompt_evaluation_control_history': [], 'prompt_evaluation_control_history_count': 0},
                row | {'prompt_evaluation_cases_count': 129}):
            with self.assertRaises(ContractError):
                decode_snapshot(request('evaluation-1'), [changed])

    def test_empty_control_is_expected_only_without_orphans(self):
        row = snapshot_rows()[0]
        row['prompt_evaluation_controls'] = []
        row['prompt_evaluation_controls_count'] = 0
        row['prompt_evaluation_control_history'] = []
        row['prompt_evaluation_control_history_count'] = 0
        self.assertIsNone(decode_snapshot(request(), [row]).control)
        with self.assertRaises(ContractError):
            decode_snapshot(request('evaluation-1'), [row])

    def test_policy_changes_preserve_old_record_but_current_head_is_new(self):
        first = control()
        second = replace(first, revision=2, parent_digest=first.digest,
            policy=replace(first.policy, version='v2'),
            updated_at=first.updated_at + timedelta(seconds=1))
        captured = decode_snapshot(request('evaluation-1'), snapshot_rows(
            include_evaluation=True, current=second, history=(first, second)))
        self.assertEqual(captured.record.policy, first.policy)
        self.assertEqual(captured.control.policy, second.policy)

    def test_read_sql_is_single_transaction_parameterized_bounded_and_read_only(self):
        sql = read_sql()
        cfg = read_config(request()).to_api_repr()['query']
        self.assertEqual(set(re.findall(r'(?<!@)@([a-z_]+)', sql)), {p['name'] for p in cfg['queryParameters']})
        self.assertEqual(sql.count('BEGIN TRANSACTION'), 1)
        self.assertEqual(sql.count('COMMIT TRANSACTION'), 1)
        self.assertIn('changed_at BETWEEN', sql)
        for mutation in ('INSERT INTO', 'UPDATE ', 'DELETE ', 'MERGE '):
            self.assertNotIn(mutation, sql)
        self.assertIn('LIMIT 129', sql)
        self.assertIn('LIMIT 65', sql)


if __name__ == '__main__':
    unittest.main()
