"""Offline prompt-registry schema and resource setup tests; no cloud calls."""
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import Conflict, Forbidden, NotFound, ServiceUnavailable

from src.application import prompt_registry_schema as schema
from src.application import sharing_publication_schema, workflow_schema
from src.ingestion import setup_prompt_registry_tables as setup
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT, SchemaMismatch
from tests import test_setup_workflow_tables as workflow_fixtures


def table(name):
    value = schema.TABLES[name].table(PROJECT)
    value._properties.update(type='TABLE', location=LOCATION, numRows='0')
    return value


class PromptRegistrySchemaTests(unittest.TestCase):
    def columns(self, name):
        return {field.name: field for field in schema.TABLES[name].schema}

    def test_two_prompt_tables_are_separate_from_existing_controls(self):
        self.assertEqual(set(schema.TABLES), {
            'prompt_registry', 'prompt_registry_history'})
        self.assertFalse(set(schema.TABLES) & set(workflow_schema.TABLES))
        self.assertFalse(set(schema.TABLES) & set(sharing_publication_schema.TABLES))
        self.assertEqual(schema.TABLES['prompt_registry'].logical_key,
                         ('prompt_key', 'snapshot_sha256'))
        self.assertEqual(schema.TABLES['prompt_registry_history'].logical_key,
                         ('prompt_key', 'snapshot_sha256', 'revision'))

    def test_registry_contains_full_canonical_snapshot_and_current_lifecycle(self):
        columns = self.columns('prompt_registry')
        expected = {'schema_version', 'prompt_key', 'task', 'snapshot_sha256',
            'template_sha256', 'vertex_project', 'vertex_location',
            'vertex_prompt_id', 'vertex_version_id', 'system_instruction',
            'prompt_template', 'required_variables', 'model_id',
            'generation_config_canonical_json',
            'safety_settings_canonical_json', 'output_schema_canonical_json',
            'output_schema_version', 'safety_policy_version',
            'prompt_contract_version', 'imported_at', 'imported_by', 'revision',
            'parent_digest', 'status', 'evaluation_id', 'changed_at',
            'changed_by', 'reason_code', 'record_digest'}
        self.assertEqual(set(columns), expected)
        self.assertTrue(all(columns[name].field_type == 'STRING' for name in (
            'system_instruction', 'prompt_template',
            'generation_config_canonical_json',
            'safety_settings_canonical_json', 'output_schema_canonical_json')))
        self.assertEqual(columns['required_variables'].mode, 'REPEATED')
        self.assertEqual(columns['revision'].field_type, 'INTEGER')
        self.assertEqual(columns['imported_at'].field_type, 'TIMESTAMP')
        self.assertEqual(columns['changed_at'].field_type, 'TIMESTAMP')
        self.assertEqual(columns['parent_digest'].mode, 'NULLABLE')
        self.assertEqual(columns['evaluation_id'].mode, 'NULLABLE')
        for name, column in columns.items():
            if name not in {'required_variables', 'parent_digest', 'evaluation_id'}:
                self.assertEqual(column.mode, 'REQUIRED', name)

    def test_history_has_exact_identity_and_lifecycle_without_mutable_content_copy(self):
        columns = self.columns('prompt_registry_history')
        self.assertEqual(set(columns), {'schema_version', 'prompt_key', 'task',
            'snapshot_sha256', 'template_sha256', 'vertex_project',
            'vertex_location', 'vertex_prompt_id', 'vertex_version_id',
            'revision', 'parent_digest', 'status', 'evaluation_id', 'changed_at',
            'changed_by', 'reason_code', 'record_digest'})
        self.assertNotIn('prompt_template', columns)
        self.assertNotIn('system_instruction', columns)
        self.assertTrue(schema.TABLES['prompt_registry_history'].append_only)
        self.assertFalse(schema.TABLES['prompt_registry'].append_only)

    def test_storage_layout_preserves_active_lookup_and_bounded_history_reads(self):
        current = schema.TABLES['prompt_registry'].table(PROJECT)
        history = schema.TABLES['prompt_registry_history'].table(PROJECT)
        self.assertIsNone(current.time_partitioning)
        self.assertFalse(current.require_partition_filter)
        self.assertEqual(current.clustering_fields,
                         ['task', 'status', 'prompt_key', 'snapshot_sha256'])
        self.assertEqual(history.time_partitioning.field, 'changed_at')
        self.assertEqual(history.time_partitioning.type_, 'DAY')
        self.assertTrue(history.require_partition_filter)
        self.assertEqual(history.clustering_fields,
                         ['task', 'prompt_key', 'snapshot_sha256', 'status'])
        for value in (current, history):
            self.assertIsNone(value.expires)
            self.assertIsNone(value.range_partitioning)
            self.assertIsNone(value.table_constraints)
            self.assertIn(schema.STORAGE_VERSION, value.description)

    def test_manifest_and_table_metadata_are_fresh_and_need_no_client(self):
        with patch('google.cloud.bigquery.Client',
                   side_effect=AssertionError('no client expected')):
            first = schema.TABLES['prompt_registry'].table(PROJECT)
            second = schema.TABLES['prompt_registry'].table(PROJECT)
            first.clustering_fields = ['changed_at']
            self.assertNotEqual(first.clustering_fields, second.clustering_fields)
            manifest = schema.manifest()
        self.assertEqual(manifest['schema_version'], schema.STORAGE_VERSION)
        self.assertEqual(manifest['retention'], 'NO_AUTOMATIC_EXPIRATION')
        self.assertEqual(len(manifest['tables']), 2)


class PromptRegistrySetupTests(unittest.TestCase):
    def client(self, resources=None):
        client = Mock(project=PROJECT)
        client.get_dataset.return_value = workflow_fixtures.dataset()
        tables = {} if resources is None else resources

        def fetch(reference, **kwargs):
            self.assertEqual(reference.project, PROJECT)
            self.assertEqual(reference.dataset_id, 'ci_application')
            self.assertIn(reference.table_id, schema.TABLES)
            if reference.table_id not in tables:
                raise NotFound('missing')
            return tables[reference.table_id]

        def create(resource, **kwargs):
            tables[resource.table_id] = table(resource.table_id)

        client.get_table.side_effect = fetch
        client.create_table.side_effect = create
        return client

    def test_check_is_read_only_and_reports_both_missing_tables(self):
        client = self.client()
        result = setup.setup(client)
        self.assertEqual(result['mode'], 'CHECK')
        self.assertEqual(result['missing_tables'], list(schema.TABLES))
        self.assertEqual(result['created_tables'], [])
        client.create_table.assert_not_called()
        client.create_dataset.assert_not_called()
        client.query.assert_not_called()

    def test_apply_then_rerun_is_idempotent_and_never_writes_rows(self):
        client = self.client()
        first = setup.setup(client, apply=True)
        second = setup.setup(client, apply=True)
        self.assertEqual(first['created_tables'], list(schema.TABLES))
        self.assertEqual(second['created_tables'], [])
        self.assertEqual(second['status'], 'VERIFIED')
        self.assertEqual(client.create_table.call_count, 2)
        client.create_dataset.assert_not_called()
        client.query.assert_not_called()
        client.insert_rows.assert_not_called()
        client.update_table.assert_not_called()
        client.delete_table.assert_not_called()
        for call in client.create_table.call_args_list:
            self.assertIsNone(call.kwargs['retry'])
            self.assertEqual(call.kwargs['timeout'], 20)

    def test_complete_preflight_rejects_mismatch_before_any_creation(self):
        wrong = table('prompt_registry_history')
        wrong.clustering_fields = ['prompt_key']
        client = self.client({'prompt_registry_history': wrong})
        with self.assertRaises(SchemaMismatch):
            setup.setup(client, apply=True)
        client.create_table.assert_not_called()

    def test_existing_table_and_rows_are_preserved(self):
        existing = table('prompt_registry')
        existing.description = 'operator note'
        existing._properties['numRows'] = '7'
        client = self.client({'prompt_registry': existing})
        result = setup.setup(client, apply=True)
        self.assertNotIn('prompt_registry', result['created_tables'])
        self.assertEqual(result['table_row_counts']['prompt_registry'], 7)
        self.assertEqual(existing.description, 'operator note')

    def test_concurrent_creation_is_revalidated(self):
        client = self.client()
        create = client.create_table.side_effect

        def concurrent(resource, **kwargs):
            create(resource, **kwargs)
            raise Conflict('already exists')

        client.create_table.side_effect = concurrent
        result = setup.setup(client, apply=True)
        self.assertEqual(result['status'], 'VERIFIED')
        self.assertEqual(result['created_tables'], [])

    def test_interrupted_setup_resumes_without_recreating_verified_table(self):
        client = self.client()
        create = client.create_table.side_effect

        def interrupted(resource, **kwargs):
            if resource.table_id == 'prompt_registry_history':
                raise ServiceUnavailable('fixture outage')
            create(resource, **kwargs)

        client.create_table.side_effect = interrupted
        with self.assertRaises(ServiceUnavailable):
            setup.setup(client, apply=True)
        client.create_table.side_effect = create
        result = setup.setup(client, apply=True)
        self.assertEqual(result['created_tables'], ['prompt_registry_history'])

    def test_existing_dataset_is_required_and_never_created(self):
        client = self.client()
        client.get_dataset.side_effect = NotFound('missing dataset')
        with self.assertRaises(NotFound):
            setup.setup(client, apply=True)
        client.create_dataset.assert_not_called()
        client.create_table.assert_not_called()

    def test_scope_dataset_failures_and_nonboolean_apply_fail_before_creation(self):
        for mode in ('project', 'region', 'expiry', 'denied', 'apply'):
            client = self.client(); apply = True
            if mode == 'project': client.project = 'foreign-project'
            if mode == 'region': client.get_dataset.return_value.location = 'US'
            if mode == 'expiry':
                client.get_dataset.return_value.default_table_expiration_ms = 1000
            if mode == 'denied': client.get_dataset.side_effect = Forbidden('denied')
            if mode == 'apply': apply = 'yes'
            with self.subTest(mode=mode), self.assertRaises((ValueError, Forbidden)):
                setup.setup(client, apply=apply)
            client.create_table.assert_not_called()


if __name__ == '__main__':
    unittest.main()
