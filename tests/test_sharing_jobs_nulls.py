"""Native scalar NULL encodings retain exact job-correlation boundaries."""
import unittest
from copy import deepcopy
from types import SimpleNamespace
from google.cloud import bigquery
from src.application.control_io import query_config
from src.application.sharing_jobs import matches_sharing_job
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION


class NativeNullParameterTests(unittest.TestCase):
    def fixture(self):
        config = query_config([bigquery.ScalarQueryParameter('cursor', 'TIMESTAMP', None)])
        native = deepcopy(config.to_api_repr())
        native['query']['query'] = 'SELECT @cursor'
        native['query']['queryParameters'][0].pop('parameterValue')
        job = bigquery.QueryJob.from_api_repr(dict(jobReference=dict(
            projectId=PROJECT, location=LOCATION, jobId='null-fixture'), configuration=native),
            client=SimpleNamespace(project=PROJECT))
        return job, config

    def matches(self, job, config):
        return matches_sharing_job(job, job_id='null-fixture', sql='SELECT @cursor', config=config)

    def test_native_omitted_null_matches_explicit_null(self):
        job, config = self.fixture()
        self.assertTrue(self.matches(job, config))

    def test_non_null_type_sql_and_budget_changes_still_deny(self):
        for change in ('value', 'type', 'sql', 'budget'):
            job, config = self.fixture()
            q = job._properties['configuration']['query']
            if change == 'value': q['queryParameters'][0]['parameterValue'] = {'value': '2026-01-01'}
            if change == 'type': q['queryParameters'][0]['parameterType'] = {'type': 'STRING'}
            if change == 'sql': q['query'] = 'SELECT 1'
            if change == 'budget': q['maximumBytesBilled'] = '1'
            self.assertFalse(self.matches(job, config), change)
