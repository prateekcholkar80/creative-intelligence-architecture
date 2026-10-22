"""4.2 Critical/Must have/shared pilot: provision only four signing-control tables.

--check is read-only; --apply creates missing tables after complete preflight.
Requires the existing ci_application dataset. No row writes, IAM or bootstrap.
"""
from __future__ import annotations

import argparse
import json

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from src.application.sharing_publication_schema import STORAGE_VERSION, TABLES
from src.application.operation_budget import operation_call
from src.ingestion.setup_workflow_tables import (
    DATASET_ID, LOCATION, PROJECT, SchemaMismatch, validate_dataset, validate_table,
)

TIMEOUT_SECONDS = 20


def inspect(client: bigquery.Client) -> dict:
    """Preflight every target; reject incompatible resources before any creation."""
    if client.project != PROJECT:
        raise ValueError('Signing setup requires the configured project')
    validate_dataset(operation_call(client.get_dataset, DATASET_ID, retry=None, timeout=TIMEOUT_SECONDS))
    present, missing, counts = [], [], {}
    for name, spec in TABLES.items():
        expected = spec.table(PROJECT)
        try:
            actual = operation_call(client.get_table, expected.reference, retry=None, timeout=TIMEOUT_SECONDS)
        except NotFound:
            missing.append(name)
            continue
        validate_table(actual, expected)
        present.append(name)
        counts[name] = actual.num_rows  # Resource metadata, not a consistency/uniqueness proof.
    return dict(schema_version=STORAGE_VERSION, project=PROJECT, dataset='ci_application',
                location=LOCATION, verified_tables=present, missing_tables=missing,
                table_row_counts=counts, status='MISSING_RESOURCES' if missing else 'VERIFIED')


def setup(client: bigquery.Client, *, apply: bool = False) -> dict:
    if type(apply) is not bool:
        raise ValueError('Apply must be an explicit boolean')
    before = inspect(client)
    created = []
    if apply:
        for name in before['missing_tables']:
            expected = TABLES[name].table(PROJECT)
            try:
                client.create_table(expected, retry=None, timeout=TIMEOUT_SECONDS)
                created.append(name)
            except Conflict:
                pass  # A concurrent creator must still pass exact resource validation.
            validate_table(client.get_table(expected.reference, retry=None,
                                             timeout=TIMEOUT_SECONDS), expected)
        before = inspect(client)
        if before['status'] != 'VERIFIED':
            raise SchemaMismatch('Signing setup did not verify all four tables')
    return dict(before, mode='APPLY' if apply else 'CHECK', created_tables=created)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument('--check', action='store_true')
    modes.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    with bigquery.Client(project=PROJECT, location=LOCATION) as client:
        print(json.dumps(setup(client, apply=args.apply), indent=2))


if __name__ == '__main__':
    main()
