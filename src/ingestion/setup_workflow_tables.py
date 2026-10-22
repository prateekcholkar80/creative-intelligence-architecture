"""Create/verify workflow infrastructure only; never insert, alter or delete rows.

ADC read-only preflight: python -m src.ingestion.setup_workflow_tables --check
Create missing resources: python -m src.ingestion.setup_workflow_tables --apply
Every existing resource is checked before creation. Conflicts fail closed; a
partially completed setup is resumable. No IAM, workspace or membership bootstrap.
"""
from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from google.api_core import retry
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from src.application.workflow_schema import DATASET, LOCATION, SCHEMA_VERSION, TABLES

PROJECT = 'project-661d501a-e709-4276-a26'
DATASET_ID = f'{PROJECT}.{DATASET}'
TIMEOUT_SECONDS = 20
RETRY = retry.Retry(deadline=30)
LOG = logging.getLogger(__name__)
ALIASES = {'INTEGER': 'INT64', 'BOOLEAN': 'BOOL', 'FLOAT': 'FLOAT64',
           'RECORD': 'STRUCT', 'DECIMAL': 'NUMERIC', 'BIGDECIMAL': 'BIGNUMERIC'}


class SchemaMismatch(ValueError):
    """Existing resource needs an explicit migration; setup will not modify it."""


def _schema(fields: list[bigquery.SchemaField] | tuple) -> list[dict[str, Any]]:
    normalized = []
    for field in fields:
        data = dict(field.to_api_repr())
        data.pop('description', None)
        data['type'] = ALIASES.get(field.field_type, field.field_type)
        data['mode'] = field.mode or 'NULLABLE'
        data['fields'] = _schema(field.fields)
        normalized.append(data)
    return normalized


def validate_dataset(dataset: bigquery.Dataset) -> None:
    if (dataset.project != PROJECT or dataset.dataset_id != DATASET
            or (dataset.location or '').lower() != LOCATION
            or dataset.default_table_expiration_ms is not None
            or dataset.default_partition_expiration_ms is not None):
        raise SchemaMismatch('Workflow dataset identity, region or default expiration differs; refusing to alter it')


def _partition(table: bigquery.Table) -> dict | None:
    if table.time_partitioning is None:
        return None
    data = dict(table.time_partitioning.to_api_repr())
    # The service mirrors the table-level flag into this deprecated API field.
    # Ignore only a consistent duplicate; retain expiry and every other setting.
    mirror = data.pop('requirePartitionFilter', None)
    if mirror is not None and mirror != bool(table.require_partition_filter):
        raise SchemaMismatch(f'{table.table_id}: inconsistent partition-filter metadata')
    return data


def validate_table(actual: bigquery.Table, expected: bigquery.Table) -> None:
    resource = f'{actual.project}.{actual.dataset_id}.{actual.table_id}'
    target = f'{expected.project}.{expected.dataset_id}.{expected.table_id}'
    if (resource != target or actual.table_type != 'TABLE'
            or (actual.location or '').lower() != LOCATION
            or actual.external_data_configuration is not None
            or actual.range_partitioning is not None
            or actual.expires is not None
            or actual.table_constraints is not None):
        raise SchemaMismatch(f'{target}: resource identity/type, region, expiration or constraints differ')
    if _schema(actual.schema) != _schema(expected.schema):
        raise SchemaMismatch(f'{target}: destination schema differs (including nested fields/modes)')
    actual_partition = _partition(actual)
    expected_partition = _partition(expected)
    if (actual_partition != expected_partition
            or bool(actual.require_partition_filter) != bool(expected.require_partition_filter)
            or list(actual.clustering_fields or []) != list(expected.clustering_fields or [])):
        raise SchemaMismatch(f'{target}: storage layout differs: '
            f'partition={actual_partition!r} expected={expected_partition!r}; '
            f'cluster={actual.clustering_fields!r} expected={expected.clustering_fields!r}; '
            f'filter={actual.require_partition_filter!r} expected={expected.require_partition_filter!r}')


def _get_dataset(client: bigquery.Client) -> bigquery.Dataset | None:
    try:
        dataset = client.get_dataset(DATASET_ID, retry=RETRY, timeout=TIMEOUT_SECONDS)
    except NotFound:
        return None
    validate_dataset(dataset)
    return dataset


def _get_table(client: bigquery.Client, expected: bigquery.Table) -> bigquery.Table | None:
    try:
        actual = client.get_table(expected.reference, retry=RETRY, timeout=TIMEOUT_SECONDS)
    except NotFound:
        return None
    validate_table(actual, expected)
    return actual


def inspect(client: bigquery.Client) -> dict:
    """Read every known target before any mutation; unrelated resources are untouched."""
    if client.project != PROJECT:
        raise ValueError('Workflow setup is scoped to the configured project')
    dataset = _get_dataset(client)
    present, missing, row_counts = [], [], {}
    if dataset is None:
        missing = list(TABLES)
    else:
        for spec in TABLES.values():
            actual = _get_table(client, spec.table(PROJECT))
            (present if actual is not None else missing).append(spec.name)
            if actual is not None:
                row_counts[spec.name] = actual.num_rows
    return dict(schema_version=SCHEMA_VERSION, project=PROJECT, dataset=DATASET,
                location=LOCATION, dataset_present=dataset is not None,
                verified_tables=present, missing_tables=missing, table_row_counts=row_counts,
                status='VERIFIED' if dataset is not None and not missing else 'MISSING_RESOURCES')


def setup(client: bigquery.Client, *, apply: bool = False) -> dict:
    before = inspect(client)
    if not apply:
        return dict(before, mode='CHECK', created_dataset=False, created_tables=[])
    created_dataset = False
    created_tables = []
    if not before['dataset_present']:
        dataset = bigquery.Dataset(DATASET_ID)
        dataset.location = LOCATION
        dataset.description = f'{SCHEMA_VERSION}: campaign application ownership and workflow records'
        try:
            client.create_dataset(dataset, retry=RETRY, timeout=TIMEOUT_SECONDS)
            created_dataset = True
        except Conflict:
            pass  # Concurrent creator: verify its actual settings, never assume compatibility.
        if _get_dataset(client) is None:
            raise SchemaMismatch('Workflow dataset was not available after creation')
    for name in before['missing_tables']:
        expected = TABLES[name].table(PROJECT)
        try:
            client.create_table(expected, retry=RETRY, timeout=TIMEOUT_SECONDS)
            created_tables.append(name)
        except Conflict:
            pass
        if _get_table(client, expected) is None:
            raise SchemaMismatch(f'{name}: table was not available after creation')
        LOG.info(json.dumps(dict(event='workflow_table_verified', table=name,
                                 created=name in created_tables)))
    after = inspect(client)
    if after['status'] != 'VERIFIED':
        raise SchemaMismatch('Workflow setup did not verify all expected resources')
    return dict(after, mode='APPLY', created_dataset=created_dataset, created_tables=created_tables)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument('--check', action='store_true', help='Read-only ADC/schema/layout verification')
    modes.add_argument('--apply', action='store_true', help='Create missing dataset/tables only')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    try:
        result = setup(client, apply=args.apply)
    finally:
        client.close()
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
