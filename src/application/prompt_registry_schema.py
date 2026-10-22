"""Priority 5 Critical/Must have: authoritative BigQuery prompt registry schemas.

This module builds metadata only.  ``prompt_registry`` keeps the full immutable
Vertex snapshot plus its current lifecycle head. ``prompt_registry_history`` is
the append-only transition log.  BigQuery is authoritative for eligibility and
exact runtime selection; these definitions do not create rows or activate a
prompt.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from types import MappingProxyType

from google.cloud import bigquery

from src.application.workflow_schema import DATASET, LOCATION, TableSpec, field

STORAGE_VERSION = 'prompt-registry-storage-v1'

IDENTITY_FIELDS = (
    field('prompt_key'), field('task'), field('snapshot_sha256'),
    field('template_sha256'), field('vertex_project'), field('vertex_location'),
    field('vertex_prompt_id'), field('vertex_version_id'),
)

LIFECYCLE_FIELDS = (
    field('revision', 'INTEGER'), field('parent_digest', mode='NULLABLE'),
    field('status'), field('evaluation_id', mode='NULLABLE'),
    field('changed_at', 'TIMESTAMP'), field('changed_by'), field('reason_code'),
    field('record_digest'),
)


@dataclass(frozen=True)
class PromptTableSpec(TableSpec):
    def table(self, project_id: str) -> bigquery.Table:
        """Return fresh exact table metadata without credentials or cloud I/O."""
        table = super().table(project_id)
        table.description = (
            f'{STORAGE_VERSION}: authoritative prompt lifecycle control; '
            'see IMPLEMENTATION_PLAN.md')
        return table


_SPECS = (
    PromptTableSpec('prompt_registry', (field('schema_version'),) + IDENTITY_FIELDS + (
        field('system_instruction'), field('prompt_template'),
        field('required_variables', mode='REPEATED'), field('model_id'),
        # Preserve canonical bytes used by the contract hashes. BigQuery JSON
        # may reserialize object members and is unsuitable for byte identity.
        field('generation_config_canonical_json'),
        field('safety_settings_canonical_json'),
        field('output_schema_canonical_json'),
        field('output_schema_version'), field('safety_policy_version'),
        field('prompt_contract_version'), field('imported_at', 'TIMESTAMP'),
        field('imported_by'),
    ) + LIFECYCLE_FIELDS,
        ('prompt_key', 'snapshot_sha256'),
        ('task', 'status', 'prompt_key', 'snapshot_sha256')),
    PromptTableSpec('prompt_registry_history',
        (field('schema_version'),) + IDENTITY_FIELDS + LIFECYCLE_FIELDS,
        ('prompt_key', 'snapshot_sha256', 'revision'),
        ('task', 'prompt_key', 'snapshot_sha256', 'status'),
        partition_field='changed_at', append_only=True),
)

TABLES = MappingProxyType({spec.name: spec for spec in _SPECS})


def manifest() -> dict:
    return dict(schema_version=STORAGE_VERSION, dataset=DATASET, location=LOCATION,
                deployment_status='RESOURCE_DEFINITION',
                retention='NO_AUTOMATIC_EXPIRATION',
                tables=[spec.to_dict() for spec in TABLES.values()])


if __name__ == '__main__':
    print(json.dumps(manifest(), indent=2))
