"""Offline exact-job prompt-registry executor tests; no cloud calls."""
from copy import deepcopy
from dataclasses import replace
from unittest.mock import Mock
import unittest

from google.api_core.exceptions import Forbidden, NotFound, ServiceUnavailable
from google.cloud import bigquery

from src.application.prompt_registry_executor import (
    PromptRegistrationOutcome,
    PromptRegistryExecutor,
    PromptRegistryExecutorError,
    PromptRegistryExecutorSettings,
    _normalize,
)
from src.application.prompt_registry_query import PromptRegistrationResultState
from src.application.prompt_registry_schema import TABLES
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_workflow_tables import PROJECT
from tests.test_prompt_registry_query import query, result
from tests.test_prompt_registry_storage import table
from tests.test_setup_workflow_tables import dataset


def native_job(plan, rows, *, state='DONE', error=None, sql=None,
               config=None):
    job = bigquery.QueryJob(
        plan.job_id, plan.sql if sql is None else sql,
        client=Mock(project=PROJECT),
        job_config=plan.job_config() if config is None else config)
    job._properties['jobReference'].update(
        projectId=PROJECT, location=LOCATION)
    job._properties['status'] = {'state': state}
    if error is not None:
        job._properties['status']['errorResult'] = error
    job._properties['statistics'] = {'query': {'statementType': 'SCRIPT'}}
    job.result = Mock(return_value=rows)
    return job


def child(plan, statement, *, error=None, state='DONE', location=LOCATION):
    job = bigquery.QueryJob(
        'child_' + statement, statement, client=Mock(project=PROJECT))
    job._properties['jobReference'].update(
        projectId=PROJECT, location=location)
    job._properties['statistics'] = {
        'parentJobId': plan.job_id,
        'query': {'statementType': statement},
    }
    job._properties['status'] = {'state': state}
    if error is not None:
        job._properties['status']['errorResult'] = error
    return job


class ClientFixture:
    project = PROJECT
    location = LOCATION
    default_query_job_config = None

    def __init__(self, plan, row):
        self.plan = plan
        self.row = row
        self.jobs = {}
        self.children = []
        self.lookups = []
        self.submissions = []
        self.schemas = []
        self.child_calls = []
        self.lookup_error = None
        self.query_error = None

    def get_dataset(self, name, **kwargs):
        self.schemas.append(('dataset', name, kwargs))
        return dataset()

    def get_table(self, reference, **kwargs):
        self.schemas.append(('table', reference.table_id, kwargs))
        return table(reference.table_id)

    def get_job(self, job_id, **kwargs):
        self.lookups.append((job_id, kwargs))
        if self.lookup_error is not None:
            raise self.lookup_error
        if job_id not in self.jobs:
            raise NotFound('missing')
        return self.jobs[job_id]

    def query(self, sql, **kwargs):
        self.submissions.append((sql, kwargs))
        job = native_job(self.plan, [deepcopy(self.row)])
        self.jobs[kwargs['job_id']] = job
        if self.query_error is not None:
            raise self.query_error
        return job

    def list_jobs(self, **kwargs):
        self.child_calls.append(kwargs)
        return iter(self.children)


class PromptRegistryExecutorTests(unittest.TestCase):
    def setUp(self):
        self.plan = query()
        self.row = result(self.plan)[0]
        self.client = ClientFixture(self.plan, self.row)
        self.executor = PromptRegistryExecutor(
            self.client,
            PromptRegistryExecutorSettings(execution_enabled=True))

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(
                (ContractError, PromptRegistryExecutorError)) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_default_disabled_before_schema_lookup_or_submission(self):
        executor = PromptRegistryExecutor(self.client)
        self.assert_code('PROMPT_REGISTRY_WRITES_DISABLED',
                         executor.submit, self.plan)
        self.assert_code('PROMPT_REGISTRY_WRITES_DISABLED',
                         executor.recover, self.plan)
        self.assertEqual(
            self.client.schemas + self.client.lookups
            + self.client.submissions, [])

    def test_submit_preflights_two_tables_and_inserts_once_exactly(self):
        outcome = self.executor.submit(self.plan)
        self.assertEqual(
            (outcome.native_status, outcome.result.state, outcome.submitted),
            (SharingCommitStatus.COMMITTED,
             PromptRegistrationResultState.APPLIED, True))
        self.assertEqual(
            [item[1] for item in self.client.schemas if item[0] == 'table'],
            list(TABLES))
        self.assertEqual(len(self.client.submissions), 1)
        sql, kwargs = self.client.submissions[0]
        self.assertEqual(
            (sql, kwargs['job_id'], kwargs['project'], kwargs['location']),
            (self.plan.sql, self.plan.job_id, PROJECT, LOCATION))
        self.assertIsNone(kwargs['retry'])
        self.assertIsNone(kwargs['job_retry'])
        self.assertEqual(kwargs['api_method'],
                         bigquery.enums.QueryApiMethod.INSERT)

    def test_repeated_submit_recovers_without_second_insert(self):
        first = self.executor.submit(self.plan)
        second = self.executor.submit(self.plan)
        self.assertTrue(first.submitted)
        self.assertFalse(second.submitted)
        self.assertEqual(second.native_status, SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.client.submissions), 1)

    def test_lost_response_recovers_only_same_original_job(self):
        self.client.query_error = ServiceUnavailable(
            'lost response containing private provider content')
        outcome = self.executor.submit(self.plan)
        self.assertEqual(
            (outcome.native_status, outcome.result.state, outcome.submitted),
            (SharingCommitStatus.COMMITTED,
             PromptRegistrationResultState.APPLIED, True))
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual({item[0] for item in self.client.lookups},
                         {self.plan.job_id})

    def test_unavailable_lookup_and_missing_recovery_never_submit(self):
        self.client.lookup_error = Forbidden('unavailable')
        outcome = self.executor.submit(self.plan)
        self.assertEqual(outcome.native_status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.submissions, [])
        self.client.lookup_error = NotFound('missing')
        recovered = self.executor.recover(self.plan)
        self.assertEqual(recovered.native_status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.submissions, [])

    def test_pending_and_invalid_native_results_fail_closed(self):
        pending = native_job(self.plan, [deepcopy(self.row)], state='RUNNING')

        def finish(**_kwargs):
            pending._properties['status'] = {'state': 'DONE'}
            return [deepcopy(self.row)]

        pending.result.side_effect = finish
        self.client.jobs[self.plan.job_id] = pending
        self.assertEqual(self.executor.recover(self.plan).native_status,
                         SharingCommitStatus.COMMITTED)
        cases = (
            native_job(self.plan, []),
            native_job(self.plan, [self.row, self.row]),
            native_job(self.plan, [{'state': 'APPLIED'}]),
            native_job(self.plan, [self.row], sql='SELECT 1'),
        )
        for job in cases:
            with self.subTest(job=job):
                self.client.jobs[self.plan.job_id] = job
                self.assertEqual(
                    self.executor.recover(self.plan).native_status,
                    SharingCommitStatus.UNKNOWN)

    def test_failed_parent_requires_correlated_transaction_child(self):
        failed = native_job(
            self.plan, [], error={'reason': 'backendError'})
        self.client.jobs[self.plan.job_id] = failed
        self.client.children = [child(self.plan, 'ROLLBACK_TRANSACTION')]
        self.assertEqual(self.executor.recover(self.plan).native_status,
                         SharingCommitStatus.NOT_COMMITTED)
        self.client.children = [child(self.plan, 'COMMIT_TRANSACTION')]
        committed = self.executor.recover(self.plan)
        self.assertEqual(committed.native_status,
                         SharingCommitStatus.COMMITTED)
        self.assertIsNone(committed.result)
        self.client.children = []
        self.assertEqual(self.executor.recover(self.plan).native_status,
                         SharingCommitStatus.UNKNOWN)

    def test_same_vertex_version_changed_payload_never_reuses_or_resubmits(self):
        self.client.jobs[self.plan.job_id] = native_job(
            self.plan, [deepcopy(self.row)])
        changed_snapshot = replace(
            self.plan.state.head.snapshot,
            system_instruction=(
                self.plan.state.head.snapshot.system_instruction
                + ' Changed candidate.'))
        record_type = self.plan.state.head.__class__
        changed_record = record_type(
            1, None, changed_snapshot, self.plan.state.head.status,
            self.plan.state.head.changed_at, self.plan.state.head.changed_by,
            self.plan.state.head.reason_code)
        changed = query(changed_record)
        self.assertEqual(changed.job_id, self.plan.job_id)
        outcome = self.executor.submit(changed)
        self.assertEqual(outcome.native_status, SharingCommitStatus.UNKNOWN)
        self.assertIsNone(outcome.result)
        self.assertEqual(self.client.submissions, [])

    def test_reused_receipt_and_settings_client_plan_fail_closed(self):
        self.row['state'] = 'REUSED'
        self.assertEqual(self.executor.submit(self.plan).result.state,
                         PromptRegistrationResultState.REUSED)
        self.assert_code('INVALID_PROMPT_REGISTRATION_PLAN',
                         self.executor.submit, {})
        for values in (
                {'execution_enabled': 1}, {'rpc_timeout_seconds': 0},
                {'result_timeout_seconds': 61},
                {'result_timeout_seconds': True}):
            with self.subTest(values=values):
                self.assert_code(
                    'INVALID_PROMPT_REGISTRY_EXECUTOR_SETTINGS',
                    PromptRegistryExecutorSettings, **values)
        self.client.default_query_job_config = bigquery.QueryJobConfig()
        with self.assertRaises(ValueError):
            PromptRegistryExecutor(self.client)

    def test_schema_failure_stops_before_job_lookup_or_submission(self):
        self.client.get_table = Mock(side_effect=Forbidden('schema denied'))
        self.assert_code('PROMPT_REGISTRY_STORE_UNAVAILABLE',
                         self.executor.submit, self.plan)
        self.assertEqual(self.client.lookups, [])
        self.assertEqual(self.client.submissions, [])

    def test_normalization_and_outcome_shape_are_bounded(self):
        for value in ({str(index): index for index in range(9)},
                      [1, 2, 3], [[[[1]]]]):
            with self.subTest(value=value):
                self.assert_code(
                    'INVALID_PROMPT_REGISTRATION_NATIVE_RESULT',
                    _normalize, value)
        self.assert_code(
            'INVALID_PROMPT_REGISTRATION_OUTCOME',
            PromptRegistrationOutcome, self.plan.job_id,
            SharingCommitStatus.NOT_COMMITTED,
            validate_result := self.executor.submit(self.plan).result)
        self.client.query_error = ServiceUnavailable(
            'private provider detail')
        self.client.jobs.clear()
        self.client.get_job = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ServiceUnavailable('private lookup detail'))
        outcome = self.executor.submit(self.plan)
        self.assertEqual(outcome.native_status, SharingCommitStatus.UNKNOWN)
        self.assertNotIn('private', repr(outcome))
        self.assertIsNotNone(validate_result)


if __name__ == '__main__':
    unittest.main()
