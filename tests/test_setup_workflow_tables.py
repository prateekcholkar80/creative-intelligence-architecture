"""Resource setup tests: failures must never alter existing state or business data."""
from copy import deepcopy
from datetime import datetime, timezone
import unittest
from unittest.mock import Mock

from google.api_core.exceptions import Conflict, Forbidden, NotFound, ServiceUnavailable
from google.cloud import bigquery

from src.application.workflow_schema import TABLES
from src.ingestion.setup_workflow_tables import (
    DATASET_ID, LOCATION, PROJECT, SchemaMismatch, setup, validate_dataset, validate_table,
)


def dataset():
    value = bigquery.Dataset(DATASET_ID)
    value.location = LOCATION
    return value


def table(name):
    value = TABLES[name].table(PROJECT)
    value._properties.update(type='TABLE', location=LOCATION, numRows='0')
    return value


class FakeClient:
    project = PROJECT

    def __init__(self, existing_dataset=None, existing_tables=None):
        self.dataset = existing_dataset
        self.tables = existing_tables or {}
        self.created_dataset = 0
        self.created_tables = []
        self.fail_table = None
        self.table_conflict = None
        self.dataset_conflict = False

    def get_dataset(self, reference, **kwargs):
        if reference != DATASET_ID:
            raise AssertionError('Unrelated dataset access')
        if self.dataset is None:
            raise NotFound('missing')
        return self.dataset

    def get_table(self, reference, **kwargs):
        if reference.project != PROJECT or reference.dataset_id != 'ci_application':
            raise AssertionError('Unrelated table access')
        if reference.table_id not in self.tables:
            raise NotFound('missing')
        return self.tables[reference.table_id]

    def create_dataset(self, resource, **kwargs):
        self.dataset = deepcopy(resource)
        if self.dataset_conflict:
            raise Conflict('concurrent compatible creator')
        self.created_dataset += 1

    def create_table(self, resource, **kwargs):
        name = resource.table_id
        if name == self.fail_table:
            raise ServiceUnavailable('interrupted setup')
        self.tables[name] = table(name)
        if name == self.table_conflict:
            raise Conflict('concurrent compatible creator')
        self.created_tables.append(name)


class WorkflowTableSetupTests(unittest.TestCase):
    def test_check_missing_resources_has_no_mutation(self):
        client = FakeClient()
        result = setup(client)
        self.assertEqual(result['status'], 'MISSING_RESOURCES')
        self.assertEqual(result['missing_tables'], list(TABLES))
        self.assertFalse(result['dataset_present'])
        self.assertEqual(client.created_dataset, 0)
        self.assertEqual(client.created_tables, [])

    def test_apply_creates_all_then_rerun_is_idempotent(self):
        client = FakeClient()
        first = setup(client, apply=True)
        self.assertTrue(first['created_dataset'])
        self.assertEqual(first['created_tables'], list(TABLES))
        self.assertEqual(first['status'], 'VERIFIED')
        second = setup(client, apply=True)
        self.assertFalse(second['created_dataset'])
        self.assertEqual(second['created_tables'], [])
        self.assertEqual(client.created_dataset, 1)
        self.assertEqual(client.created_tables, list(TABLES))

    def test_only_missing_tables_are_created_and_existing_metadata_is_preserved(self):
        existing = table('jobs')
        existing.description = 'Existing operator documentation'
        existing._properties['numRows'] = '7'
        client = FakeClient(dataset(), {'jobs': existing})
        result = setup(client, apply=True)
        self.assertNotIn('jobs', result['created_tables'])
        self.assertIs(client.tables['jobs'], existing)
        self.assertEqual(existing.description, 'Existing operator documentation')
        self.assertEqual(result['table_row_counts']['jobs'], 7)

    def test_preflight_checks_later_incompatible_table_before_any_creation(self):
        existing = table('audit_events')
        existing.clustering_fields = ['actor_id']
        client = FakeClient(dataset(), {'audit_events': existing})
        with self.assertRaises(SchemaMismatch):
            setup(client, apply=True)
        self.assertEqual(client.created_tables, [])

    def test_wrong_project_and_region_fail_before_writes(self):
        for client in (FakeClient(), FakeClient(dataset())):
            if client.dataset is None:
                client.project = 'another-project'
            else:
                client.dataset.location = 'US'
            with self.assertRaises((SchemaMismatch, ValueError)):
                setup(client, apply=True)
            self.assertEqual(client.created_dataset, 0)
            self.assertEqual(client.created_tables, [])

    def test_dataset_default_expiration_is_rejected(self):
        for attribute in ('default_table_expiration_ms', 'default_partition_expiration_ms'):
            value = dataset()
            setattr(value, attribute, 86400000)
            with self.assertRaises(SchemaMismatch):
                validate_dataset(value)

    def test_dataset_identity_must_be_exact(self):
        value = bigquery.Dataset(f'{PROJECT}.ci_product')
        value.location = LOCATION
        with self.assertRaises(SchemaMismatch):
            validate_dataset(value)

    def test_nested_schema_type_mode_order_or_extra_fields_fail_closed(self):
        expected = TABLES['outputs'].table(PROJECT)
        for mutation in ('type', 'mode', 'order', 'extra', 'default'):
            actual = table('outputs')
            raw = [deepcopy(f.to_api_repr()) for f in actual.schema]
            artifact = next(f for f in raw if f['name'] == 'artifacts')
            if mutation == 'type':
                artifact['fields'][-1]['type'] = 'STRING'
            elif mutation == 'mode':
                artifact['fields'][0]['mode'] = 'NULLABLE'
            elif mutation == 'order':
                artifact['fields'].reverse()
            elif mutation == 'extra':
                artifact['fields'].append({'name': 'extra', 'type': 'STRING'})
            else:
                raw[0]['defaultValueExpression'] = "'other-version'"
            actual.schema = [bigquery.SchemaField.from_api_repr(f) for f in raw]
            with self.subTest(mutation=mutation), self.assertRaises(SchemaMismatch):
                validate_table(actual, expected)

    def test_schema_aliases_and_descriptions_do_not_cause_false_migration(self):
        actual = table('model_invocations')
        raw = [deepcopy(f.to_api_repr()) for f in actual.schema]
        for f in raw:
            f['description'] = 'Operator field documentation'
            if f['type'] == 'RECORD':
                f['type'] = 'STRUCT'
            if f['type'] == 'INTEGER':
                f['type'] = 'INT64'
        actual.schema = [bigquery.SchemaField.from_api_repr(f) for f in raw]
        validate_table(actual, TABLES['model_invocations'].table(PROJECT))

    def test_server_partition_filter_mirror_is_accepted_only_if_consistent(self):
        actual = table('requests')
        actual._properties['timePartitioning']['requirePartitionFilter'] = True
        validate_table(actual, TABLES['requests'].table(PROJECT))
        actual._properties['timePartitioning']['requirePartitionFilter'] = False
        with self.assertRaises(SchemaMismatch):
            validate_table(actual, TABLES['requests'].table(PROJECT))

    def test_incompatible_resource_or_storage_layout_is_rejected(self):
        for mutation in ('view', 'region', 'expiry', 'clustering', 'partition_field',
                         'partition_expiry', 'partition_filter', 'constraints'):
            actual = table('outputs')
            if mutation == 'view':
                actual._properties['type'] = 'VIEW'
            elif mutation == 'region':
                actual._properties['location'] = 'us-central1'
            elif mutation == 'expiry':
                actual.expires = datetime(2030, 1, 1, tzinfo=timezone.utc)
            elif mutation == 'clustering':
                actual.clustering_fields = ['owner_id', 'workspace_id']
            elif mutation == 'partition_field':
                actual.time_partitioning = bigquery.TimePartitioning(field='other_time')
            elif mutation == 'partition_expiry':
                actual.time_partitioning = bigquery.TimePartitioning(field='created_at', expiration_ms=1000)
            elif mutation == 'partition_filter':
                actual.require_partition_filter = False
            else:
                actual._properties['tableConstraints'] = {'primaryKey': {'columns': ['output_id']}}
            with self.subTest(mutation=mutation), self.assertRaises(SchemaMismatch):
                validate_table(actual, TABLES['outputs'].table(PROJECT))

    def test_compatible_concurrent_creation_is_rechecked(self):
        client = FakeClient()
        client.dataset_conflict = True
        client.table_conflict = 'jobs'
        result = setup(client, apply=True)
        self.assertEqual(result['status'], 'VERIFIED')
        self.assertFalse(result['created_dataset'])
        self.assertNotIn('jobs', result['created_tables'])

    def test_incompatible_concurrent_creation_is_rejected(self):
        client = FakeClient(dataset())
        original = client.create_table
        def create(resource, **kwargs):
            original(resource, **kwargs)
            client.tables[resource.table_id].schema = [bigquery.SchemaField('wrong', 'STRING')]
            raise Conflict('race')
        client.create_table = create
        with self.assertRaises(SchemaMismatch):
            setup(client, apply=True)
        self.assertEqual(len(client.tables), 1)

    def test_interrupted_creation_is_resumable_without_replacing_tables(self):
        client = FakeClient()
        client.fail_table = 'jobs'
        with self.assertRaises(ServiceUnavailable):
            setup(client, apply=True)
        existing = dict(client.tables)
        client.fail_table = None
        result = setup(client, apply=True)
        self.assertEqual(result['status'], 'VERIFIED')
        for name, original in existing.items():
            self.assertIs(client.tables[name], original)
            self.assertNotIn(name, result['created_tables'])

    def test_forbidden_is_not_treated_as_a_missing_resource(self):
        client = Mock(project=PROJECT)
        client.get_dataset.side_effect = Forbidden('access denied')
        with self.assertRaises(Forbidden):
            setup(client, apply=True)
        client.create_dataset.assert_not_called()
        client.create_table.assert_not_called()

    def test_setup_never_executes_data_queries_or_row_writes(self):
        client = Mock(wraps=FakeClient())
        client.project = PROJECT
        forbidden = ('query', 'insert_rows_json', 'load_table_from_json', 'load_table_from_dataframe',
                     'update_table', 'update_dataset', 'delete_table', 'delete_dataset')
        for name in forbidden:
            setattr(client, name, Mock())
        setup(client, apply=True)
        for name in forbidden:
            getattr(client, name).assert_not_called()
