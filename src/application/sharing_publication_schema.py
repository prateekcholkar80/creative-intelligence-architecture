"""4.1 High/Must have: explicit signing-control metadata, with no cloud I/O.

These four proposed tables are separate from the eleven workflow definitions.
Inspect: python -m src.application.sharing_publication_schema
The manifest is an authoring artifact, never a serving source or deployment.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from types import MappingProxyType

from google.cloud import bigquery

from src.application.workflow_schema import DATASET, LOCATION, TableSpec, field

STORAGE_VERSION = 'sharing-publication-storage-v1'
BASE = (field('schema_version'), field('policy_id'))
PROFILE = (
    field('profile_id'),
    field('settings', 'RECORD', children=(
        field('maximum_bytes_billed', 'INTEGER'), field('job_timeout_ms', 'INTEGER'),
        field('statement_timeout_ms', 'INTEGER'),
    )),
    field('fingerprint'), field('handler_version'),
)


@dataclass(frozen=True)
class SigningTableSpec(TableSpec):
    def table(self, project_id: str) -> bigquery.Table:
        """Fresh SDK metadata only; no client, credentials, expiration or keys."""
        table = super().table(project_id)
        table.description = (
            f'{STORAGE_VERSION}: proposed signing control; see SHARING_PUBLICATION_STORAGE.md')
        return table


_SPECS = (
    SigningTableSpec('signing_publications', BASE + (
        field('publication_version'), field('revision', 'INTEGER'),
        field('parent_digest', mode='NULLABLE'), field('publication_digest'),
        field('config_digest'), field('status'), field('published_at', 'TIMESTAMP'),
        # Canonical metadata bytes are preserved as UTF-8 text, not reserialized JSON.
        field('config_canonical_json'), field('history_count', 'INTEGER'),
        field('publisher_id'), field('command_id'), field('recorded_at', 'TIMESTAMP'),
    ), ('policy_id', 'revision'), ('policy_id', 'revision'), append_only=True),
    SigningTableSpec('signing_heads', BASE + (
        field('revision', 'INTEGER'), field('control_revision', 'INTEGER'), field('publication_digest'),
        field('published_at', 'TIMESTAMP'), field('publisher_id'), field('command_id'),
    ), ('policy_id',), ('policy_id',)),
    SigningTableSpec('signing_key_history', BASE + (
        field('publication_revision', 'INTEGER'), field('publication_digest'),
        field('key_id'), field('profile_id'), field('issue_from', 'TIMESTAMP'),
        field('issue_until', 'TIMESTAMP'), field('retain_until', 'TIMESTAMP'),
        field('revoked', 'BOOLEAN'), field('secret_id'), field('secret_version', 'INTEGER'),
        field('profile', 'RECORD', children=PROFILE),
    ), ('policy_id', 'publication_revision', 'key_id'),
        ('policy_id', 'publication_revision', 'key_id'), append_only=True),
    SigningTableSpec('signing_checkpoints', BASE + (
        field('consumer_id'), field('checkpoint_revision', 'INTEGER'),
        field('publication_revision', 'INTEGER'), field('publication_digest'),
        field('instance_id'), field('updated_at', 'TIMESTAMP'),
        field('writer_id'), field('command_id'),
    ), ('policy_id', 'consumer_id'), ('policy_id', 'consumer_id')),
)
TABLES = MappingProxyType({spec.name: spec for spec in _SPECS})


def manifest() -> dict:
    return dict(schema_version=STORAGE_VERSION, dataset=DATASET, location=LOCATION,
                deployment_status='DESIGN_ONLY',
                retention='NO_AUTOMATIC_EXPIRATION',
                tables=[spec.to_dict() for spec in TABLES.values()])


if __name__ == '__main__':
    print(json.dumps(manifest(), indent=2))
