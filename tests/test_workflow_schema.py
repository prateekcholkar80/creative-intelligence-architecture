"""Checks for the proposed persistence contract; no cloud setup or client calls."""
import json
import unittest
from unittest.mock import patch

from src.application.workflow_schema import DATASET, LOCATION, SCHEMA_VERSION, TABLES, manifest


def fields(name):
    return {f.name: f for f in TABLES[name].schema}


class WorkflowSchemaTests(unittest.TestCase):
    def test_expected_record_boundaries_and_logical_keys(self):
        expected = {
            'workspaces': ('workspace_id',),
            'memberships': ('workspace_id', 'principal_id'),
            'conversations': ('workspace_id', 'conversation_id'),
            'requests': ('workspace_id', 'request_id'),
            'jobs': ('workspace_id', 'job_id'),
            'job_attempts': ('workspace_id', 'attempt_id'),
            'stage_events': ('workspace_id', 'event_id'),
            'outputs': ('workspace_id', 'output_id'),
            'evidence_snapshots': ('workspace_id', 'evidence_snapshot_id', 'evidence_ref_id'),
            'model_invocations': ('workspace_id', 'invocation_id'),
            'audit_events': ('workspace_id', 'audit_id'),
        }
        self.assertEqual(set(TABLES), set(expected))
        for name, key in expected.items():
            with self.subTest(table=name):
                self.assertEqual(TABLES[name].logical_key, key)
                for column in key:
                    self.assertEqual(fields(name)[column].mode, 'REQUIRED')

    def test_all_workflow_records_are_versioned_and_workspace_scoped(self):
        for name in TABLES:
            for column in ('workspace_id', 'schema_version'):
                self.assertEqual(fields(name)[column].mode, 'REQUIRED')
            self.assertEqual(TABLES[name].clustering[0], 'workspace_id')

    def test_owned_records_link_to_current_conversation_access(self):
        for name in ('requests', 'jobs', 'job_attempts', 'stage_events', 'outputs',
                     'evidence_snapshots', 'model_invocations'):
            self.assertEqual(fields(name)['conversation_id'].mode, 'REQUIRED')
            self.assertEqual(fields(name)['owner_id'].mode, 'REQUIRED')
            self.assertNotIn('visibility', fields(name))
        self.assertEqual(fields('conversations')['access_revision'].mode, 'REQUIRED')
        self.assertEqual(fields('conversations')['visibility'].mode, 'REQUIRED')

    def test_control_records_do_not_expire_or_need_date_for_identity_lookup(self):
        for name in ('workspaces', 'memberships', 'conversations', 'jobs'):
            table = TABLES[name].table('test-project')
            self.assertIsNone(table.time_partitioning)
            self.assertIsNone(table.expires)

    def test_partitioned_history_requires_bounded_time_queries_without_automatic_deletion(self):
        for spec in TABLES.values():
            if spec.partition_field:
                table = spec.table('test-project')
                self.assertEqual(fields(spec.name)[spec.partition_field].field_type, 'TIMESTAMP')
                self.assertEqual(fields(spec.name)[spec.partition_field].mode, 'REQUIRED')
                self.assertEqual(table.time_partitioning.field, spec.partition_field)
                self.assertTrue(table.require_partition_filter)
                self.assertIsNone(table.time_partitioning.expiration_ms)

    def test_profile_snapshots_preserve_independent_thinking_and_limits(self):
        for name, column in (('requests', 'model_profiles'), ('outputs', 'model_profiles'),
                             ('model_invocations', 'profile')):
            profile = {f.name: f for f in fields(name)[column].fields}
            for required in ('stage', 'profile_id', 'profile_version', 'catalog_version',
                             'model_id', 'location', 'thinking_control', 'max_output_tokens',
                             'timeout_ms', 'prompt_version', 'settings_sha256', 'safety_policy_version'):
                self.assertEqual(profile[required].mode, 'REQUIRED')
            self.assertEqual(profile['thinking_level'].mode, 'NULLABLE')
            self.assertEqual(profile['thinking_budget'].mode, 'NULLABLE')
            self.assertEqual(profile['thinking_budget'].field_type, 'INTEGER')

    def test_unknown_usage_and_money_are_nullable_with_explicit_status(self):
        invocation = fields('model_invocations')
        self.assertEqual(invocation['usage_status'].mode, 'REQUIRED')
        self.assertEqual(invocation['cost_status'].mode, 'REQUIRED')
        for name in ('input_tokens', 'output_tokens', 'cached_tokens', 'thinking_tokens'):
            self.assertEqual(invocation[name].mode, 'NULLABLE')
            self.assertEqual(invocation[name].field_type, 'INTEGER')
        self.assertEqual(invocation['estimated_cost'].field_type, 'NUMERIC')
        self.assertEqual(invocation['estimated_cost'].mode, 'NULLABLE')
        self.assertEqual(invocation['job_id'].mode, 'NULLABLE')  # Clarification calls have no job.

    def test_artifacts_store_integrity_references_instead_of_image_bytes(self):
        artifacts = fields('outputs')['artifacts']
        self.assertEqual(artifacts.mode, 'REPEATED')
        self.assertEqual({f.name for f in artifacts.fields},
                         {'artifact_id', 'kind', 'gcs_uri', 'gcs_generation', 'sha256',
                          'content_type', 'size_bytes'})
        self.assertTrue(all(f.mode == 'REQUIRED' for f in artifacts.fields))
        def check_no_bytes(schema):
            for f in schema:
                self.assertNotEqual(f.field_type, 'BYTES')
                check_no_bytes(f.fields)
        for spec in TABLES.values():
            check_no_bytes(spec.schema)

    def test_evidence_can_pin_approval_image_and_vector_identity(self):
        evidence = fields('evidence_snapshots')
        for name in ('source_table', 'source_record_id', 'source_system', 'content_sha256', 'validated_at',
                     'evidence_schema_version', 'evidence_payload'):
            self.assertEqual(evidence[name].mode, 'REQUIRED')
        for name in ('approval_reference', 'review_revision', 'gcs_generation', 'image_sha256',
                     'knowledge_snapshot_id', 'embedding_model', 'embedding_dimensions',
                     'ranking_version', 'cosine_distance'):
            self.assertIn(name, evidence)

    def test_synthetic_content_provenance_does_not_label_real_memberships(self):
        for name in ('source_system', 'approval_scope', 'initial_review_status', 'real_business_approved'):
            self.assertEqual(fields('outputs')[name].mode, 'REQUIRED')
        self.assertNotIn('source_system', fields('memberships'))
        self.assertEqual(fields('memberships')['assignment_origin'].mode, 'REQUIRED')

    def test_job_identity_recovery_and_revision_fields_are_explicit(self):
        for name in ('idempotency_key_sha256', 'request_sha256', 'state', 'revision',
                     'attempt_count', 'deadline_at'):
            self.assertEqual(fields('jobs')[name].mode, 'REQUIRED')
        self.assertIn('dispatch_receipt_id', fields('job_attempts'))
        self.assertIn('dispatch_receipt_id', fields('model_invocations'))
        self.assertFalse(TABLES['requests'].append_only)  # Interpretation updates a revisioned outcome.
        self.assertEqual(fields('requests')['revision'].mode, 'REQUIRED')
        self.assertEqual(fields('requests')['submitted_preferences'].mode, 'REQUIRED')
        self.assertFalse(TABLES['jobs'].append_only)

    def test_audit_can_record_anonymous_denial_without_capturing_prompt_content(self):
        audit = fields('audit_events')
        self.assertEqual(audit['actor_id'].mode, 'NULLABLE')
        for name in ('action', 'allowed', 'reason_code', 'policy_version', 'trace_id'):
            self.assertEqual(audit[name].mode, 'REQUIRED')
        for forbidden in ('prompt', 'response', 'email', 'jwt', 'notes'):
            self.assertNotIn(forbidden, audit)

    def test_metadata_inspection_needs_no_cloud_client_and_manifest_is_design_only(self):
        with patch('google.cloud.bigquery.Client', side_effect=AssertionError('No cloud clients')):
            data = json.loads(json.dumps(manifest()))
            for spec in TABLES.values():
                table = spec.table('test-project')
                self.assertEqual(table.dataset_id, DATASET)
                self.assertEqual(table.table_id, spec.name)
                self.assertIsNone(table.table_constraints)
        self.assertEqual(data['schema_version'], SCHEMA_VERSION)
        self.assertEqual(data['location'], LOCATION)
        self.assertEqual(data['deployment_status'], 'DESIGN_ONLY')

    def test_manifest_and_metadata_cannot_modify_shared_nested_definitions(self):
        original = fields('outputs')['artifacts'].fields[0].name
        data = manifest()
        output = next(t for t in data['tables'] if t['name'] == 'outputs')
        next(f for f in output['schema'] if f['name'] == 'artifacts')['fields'][0]['name'] = 'changed'
        table = TABLES['outputs'].table('test-project')
        nested = next(f for f in table.schema if f.name == 'artifacts')
        nested.fields[0]._properties['name'] = 'changed'  # SDK mutable representation.
        self.assertEqual(fields('outputs')['artifacts'].fields[0].name, original)

    def test_invalid_project_identifier_is_rejected(self):
        for value in ('', 'x.y', 'test-project`', 'test-project;DROP', None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                TABLES['jobs'].table(value)
