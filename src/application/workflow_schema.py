"""Proposed ci_application schemas; builds metadata only, never creates cloud resources.

Inspect locally: python -m src.application.workflow_schema
Business state and vectors remain authoritative in BigQuery. The manifest is an
authoring artifact, not a serving fallback. Loaders and service wiring are pending.
"""
from __future__ import annotations

from dataclasses import dataclass
from copy import deepcopy
import json
import re
from types import MappingProxyType

from google.cloud import bigquery

SCHEMA_VERSION = 'campaign-workflow-v1'
DATASET = 'ci_application'
LOCATION = 'asia-south1'


def field(name: str, kind: str = 'STRING', mode: str = 'REQUIRED', *, children=()):
    return bigquery.SchemaField(name, kind, mode=mode, fields=children)


BASE = (field('schema_version'), field('workspace_id'))
OWNED = BASE + (field('conversation_id'), field('owner_id'))

# Effective settings snapshots, not a published model catalog (M2 is separate).
PROFILE_FIELDS = (
    field('stage'), field('profile_id'), field('profile_version'), field('catalog_version'),
    field('model_id'), field('location'), field('thinking_control'),
    field('thinking_level', mode='NULLABLE'), field('thinking_budget', 'INTEGER', 'NULLABLE'),
    field('max_output_tokens', 'INTEGER'), field('timeout_ms', 'INTEGER'),
    field('seed', 'INTEGER', 'NULLABLE'), field('temperature', 'FLOAT', 'NULLABLE'),
    field('prompt_id'), field('prompt_version'), field('output_schema_version'),
    field('safety_policy_version'), field('settings_sha256'),
)
ARTIFACT_FIELDS = (
    field('artifact_id'), field('kind'), field('gcs_uri'), field('gcs_generation'),
    field('sha256'), field('content_type'), field('size_bytes', 'INTEGER'),
)


@dataclass(frozen=True)
class TableSpec:
    name: str
    schema: tuple[bigquery.SchemaField, ...]
    logical_key: tuple[str, ...]
    clustering: tuple[str, ...]
    partition_field: str | None = None
    append_only: bool = False

    def table(self, project_id: str) -> bigquery.Table:
        """Return fresh SDK metadata without a client, credentials or network calls."""
        if not isinstance(project_id, str) or not re.fullmatch(r'[a-z][a-z0-9-]{4,61}[a-z0-9]', project_id):
            raise ValueError('Expected a Google Cloud project ID')
        schema = [bigquery.SchemaField.from_api_repr(deepcopy(f.to_api_repr())) for f in self.schema]
        table = bigquery.Table(f'{project_id}.{DATASET}.{self.name}', schema=schema)
        table.description = f'{SCHEMA_VERSION}: proposed workflow record; see WORKFLOW_DATA_DESIGN.md'
        table.clustering_fields = list(self.clustering)
        if self.partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=self.partition_field)
            table.require_partition_filter = True
        # No expiry or unenforced key constraints are activated by this definition.
        return table

    def to_dict(self) -> dict:
        return dict(name=self.name, schema=[deepcopy(f.to_api_repr()) for f in self.schema],
                    logical_key=list(self.logical_key), clustering=list(self.clustering),
                    partition_field=self.partition_field, append_only=self.append_only)


_SPECS = (
    TableSpec('workspaces', BASE + (
        field('status'), field('revision', 'INTEGER'), field('created_at', 'TIMESTAMP'),
        field('updated_at', 'TIMESTAMP'),
    ), ('workspace_id',), ('workspace_id',)),
    TableSpec('memberships', BASE + (
        field('principal_id'), field('identity_issuer'), field('identity_subject'),
        field('roles', mode='REPEATED'), field('status'), field('revision', 'INTEGER'),
        field('assignment_origin'), field('updated_by'),
        field('created_at', 'TIMESTAMP'), field('updated_at', 'TIMESTAMP'),
    ), ('workspace_id', 'principal_id'), ('workspace_id', 'principal_id')),
    TableSpec('conversations', OWNED + (
        field('visibility'), field('status'), field('access_revision', 'INTEGER'),
        field('created_at', 'TIMESTAMP'), field('updated_at', 'TIMESTAMP'),
    ), ('workspace_id', 'conversation_id'), ('workspace_id', 'owner_id', 'conversation_id')),
    TableSpec('requests', OWNED + (
        field('request_id'), field('principal_id'), field('session_id', mode='NULLABLE'),
        field('trace_id'), field('operation'), field('request_status'),
        field('revision', 'INTEGER'), field('submitted_preferences', 'JSON'),
        field('message_sha256', mode='NULLABLE'),
        field('canonical_sku', mode='NULLABLE'), field('objective', mode='NULLABLE'),
        field('audience', mode='NULLABLE'), field('channel', mode='NULLABLE'),
        field('visual_reference', mode='NULLABLE'), field('unsupported_features', mode='REPEATED'),
        field('idempotency_key_sha256'), field('request_sha256'),
        field('parent_output_id', mode='NULLABLE'), field('generation_intent_id', mode='NULLABLE'),
        field('model_profiles', 'RECORD', 'REPEATED', children=PROFILE_FIELDS),
        field('policy_version'), field('created_at', 'TIMESTAMP'), field('updated_at', 'TIMESTAMP'),
    ), ('workspace_id', 'request_id'), ('workspace_id', 'owner_id', 'conversation_id', 'request_id'),
        'created_at'),
    TableSpec('jobs', OWNED + (
        field('job_id'), field('request_id'), field('principal_id'), field('trace_id'),
        field('canonical_sku'), field('idempotency_key_sha256'), field('request_sha256'),
        field('generation_intent_id'),
        field('state'), field('revision', 'INTEGER'), field('attempt_count', 'INTEGER'),
        field('active_attempt_id', mode='NULLABLE'), field('output_id', mode='NULLABLE'),
        field('safe_error_code', mode='NULLABLE'), field('policy_version'),
        field('created_at', 'TIMESTAMP'), field('updated_at', 'TIMESTAMP'),
        field('deadline_at', 'TIMESTAMP'), field('completed_at', 'TIMESTAMP', 'NULLABLE'),
    ), ('workspace_id', 'job_id'), ('workspace_id', 'owner_id', 'conversation_id', 'job_id')),
    TableSpec('job_attempts', OWNED + (
        field('job_id'), field('request_id'), field('attempt_id'), field('trace_id'),
        field('attempt_number', 'INTEGER'), field('state'), field('revision', 'INTEGER'),
        field('dispatch_receipt_id', mode='NULLABLE'), field('worker_id', mode='NULLABLE'),
        field('safe_error_code', mode='NULLABLE'),
        field('started_at', 'TIMESTAMP'), field('updated_at', 'TIMESTAMP'),
        field('completed_at', 'TIMESTAMP', 'NULLABLE'),
    ), ('workspace_id', 'attempt_id'), ('workspace_id', 'job_id', 'attempt_id'), 'started_at'),
    TableSpec('stage_events', OWNED + (
        field('job_id'), field('request_id'), field('attempt_id'), field('trace_id'),
        field('event_id'), field('sequence', 'INTEGER'), field('stage'), field('outcome'),
        field('duration_ms', 'INTEGER', 'NULLABLE'), field('safe_error_code', mode='NULLABLE'),
        field('policy_version'), field('occurred_at', 'TIMESTAMP'),
    ), ('workspace_id', 'event_id'), ('workspace_id', 'job_id', 'attempt_id'), 'occurred_at', True),
    TableSpec('outputs', OWNED + (
        field('output_id'), field('job_id'), field('request_id'), field('attempt_id'), field('trace_id'),
        field('canonical_sku'), field('parent_output_id', mode='NULLABLE'),
        field('source_system'), field('approval_scope'), field('initial_review_status'),
        field('real_business_approved', 'BOOLEAN'), field('evidence_snapshot_id'),
        field('brief_schema_version'), field('brief', 'JSON'),
        field('guardrail_summary', 'JSON'), field('renderer_version'), field('policy_version'),
        field('model_profiles', 'RECORD', 'REPEATED', children=PROFILE_FIELDS),
        field('artifacts', 'RECORD', 'REPEATED', children=ARTIFACT_FIELDS),
        field('created_at', 'TIMESTAMP'),
    ), ('workspace_id', 'output_id'), ('workspace_id', 'owner_id', 'conversation_id', 'output_id'),
        'created_at', True),
    TableSpec('evidence_snapshots', OWNED + (
        field('evidence_snapshot_id'), field('evidence_ref_id'), field('job_id'), field('request_id'),
        field('attempt_id'), field('canonical_sku'), field('source_kind'), field('source_table'),
        field('source_record_id'), field('source_system'), field('source_revision', mode='NULLABLE'),
        field('knowledge_snapshot_id', mode='NULLABLE'), field('content_sha256'),
        field('evidence_schema_version'), field('evidence_payload', 'JSON'),
        field('approval_reference', mode='NULLABLE'), field('review_revision', 'INTEGER', 'NULLABLE'),
        field('gcs_uri', mode='NULLABLE'), field('gcs_generation', mode='NULLABLE'),
        field('image_sha256', mode='NULLABLE'), field('embedding_model', mode='NULLABLE'),
        field('embedding_dimensions', 'INTEGER', 'NULLABLE'), field('rank', 'INTEGER', 'NULLABLE'),
        field('cosine_distance', 'FLOAT', 'NULLABLE'), field('ranking_version', mode='NULLABLE'),
        field('validated_at', 'TIMESTAMP'), field('created_at', 'TIMESTAMP'),
    ), ('workspace_id', 'evidence_snapshot_id', 'evidence_ref_id'),
        ('workspace_id', 'job_id', 'evidence_snapshot_id'), 'created_at', True),
    TableSpec('model_invocations', OWNED + (
        field('invocation_id'), field('request_id'), field('job_id', mode='NULLABLE'),
        field('attempt_id', mode='NULLABLE'), field('trace_id'), field('stage'),
        field('call_number', 'INTEGER'), field('state'), field('revision', 'INTEGER'),
        field('dispatch_receipt_id', mode='NULLABLE'),
        field('worker_id', mode='NULLABLE'),
        field('profile', 'RECORD', children=PROFILE_FIELDS),
        field('returned_model_id', mode='NULLABLE'), field('response_sha256', mode='NULLABLE'),
        field('finish_reason', mode='NULLABLE'), field('safety_outcome', mode='NULLABLE'),
        field('safe_error_code', mode='NULLABLE'), field('usage_status'),
        field('input_tokens', 'INTEGER', 'NULLABLE'), field('output_tokens', 'INTEGER', 'NULLABLE'),
        field('cached_tokens', 'INTEGER', 'NULLABLE'), field('thinking_tokens', 'INTEGER', 'NULLABLE'),
        field('cost_status'), field('estimated_cost', 'NUMERIC', 'NULLABLE'),
        field('currency', mode='NULLABLE'), field('pricing_version', mode='NULLABLE'),
        field('started_at', 'TIMESTAMP'), field('completed_at', 'TIMESTAMP', 'NULLABLE'),
    ), ('workspace_id', 'invocation_id'), ('workspace_id', 'request_id', 'job_id', 'invocation_id'),
        'started_at'),
    TableSpec('audit_events', BASE + (
        field('audit_id'), field('actor_id', mode='NULLABLE'), field('actor_kind'),
        field('action'), field('allowed', 'BOOLEAN'), field('reason_code'), field('policy_version'),
        field('resource_type', mode='NULLABLE'), field('resource_id', mode='NULLABLE'),
        field('conversation_id', mode='NULLABLE'), field('owner_id', mode='NULLABLE'),
        field('request_id', mode='NULLABLE'), field('job_id', mode='NULLABLE'), field('trace_id'),
        field('membership_revision', 'INTEGER', 'NULLABLE'),
        field('resource_revision', 'INTEGER', 'NULLABLE'), field('new_revision', 'INTEGER', 'NULLABLE'),
        field('mutation_outcome', mode='NULLABLE'), field('occurred_at', 'TIMESTAMP'),
    ), ('workspace_id', 'audit_id'), ('workspace_id', 'actor_id', 'resource_id'), 'occurred_at', True),
)
TABLES = MappingProxyType({spec.name: spec for spec in _SPECS})


def manifest() -> dict:
    return dict(schema_version=SCHEMA_VERSION, dataset=DATASET, location=LOCATION,
                deployment_status='DESIGN_ONLY', tables=[spec.to_dict() for spec in TABLES.values()])


if __name__ == '__main__':
    print(json.dumps(manifest(), indent=2))
