"""Create or verify only the two authoritative prompt-registry tables.

Read-only preflight: python -m src.ingestion.setup_prompt_registry_tables --check
Create missing tables: python -m src.ingestion.setup_prompt_registry_tables --apply

The existing ci_application dataset is required. Existing resources are checked
before any creation. This command never issues a query, writes a row, changes a
schema, activates a prompt, changes IAM, or deletes a resource.
"""
from __future__ import annotations

import argparse
import json

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

from src.application.operation_budget import operation_call
from src.application.prompt_registry_schema import STORAGE_VERSION, TABLES
from src.ingestion.setup_workflow_tables import (
    DATASET_ID, LOCATION, PROJECT, SchemaMismatch, validate_dataset, validate_table,
)

TIMEOUT_SECONDS = 20


def inspect(client: bigquery.Client) -> dict:
    """Read every target and refuse incompatible metadata before mutation."""
    if client.project != PROJECT:
        raise ValueError('Prompt registry setup requires the configured project')
    dataset = operation_call(client.get_dataset, DATASET_ID, retry=None,
                             timeout=TIMEOUT_SECONDS)
    validate_dataset(dataset)
    present, missing, counts = [], [], {}
    for name, spec in TABLES.items():
        expected = spec.table(PROJECT)
        try:
            actual = operation_call(client.get_table, expected.reference,
                                    retry=None, timeout=TIMEOUT_SECONDS)
        except NotFound:
            missing.append(name)
            continue
        validate_table(actual, expected)
        present.append(name)
        counts[name] = actual.num_rows
    return dict(schema_version=STORAGE_VERSION, project=PROJECT,
                dataset='ci_application', location=LOCATION,
                verified_tables=present, missing_tables=missing,
                table_row_counts=counts,
                status='MISSING_RESOURCES' if missing else 'VERIFIED')


def setup(client: bigquery.Client, *, apply: bool = False) -> dict:
    """Create missing tables only after a complete compatible preflight."""
    if type(apply) is not bool:
        raise ValueError('Apply must be an explicit boolean')
    state = inspect(client)
    created = []
    if apply:
        for name in state['missing_tables']:
            expected = TABLES[name].table(PROJECT)
            try:
                operation_call(client.create_table, expected, retry=None,
                               timeout=TIMEOUT_SECONDS)
                created.append(name)
            except Conflict:
                pass
            actual = operation_call(client.get_table, expected.reference,
                                    retry=None, timeout=TIMEOUT_SECONDS)
            validate_table(actual, expected)
        state = inspect(client)
        if state['status'] != 'VERIFIED':
            raise SchemaMismatch('Prompt registry setup did not verify both tables')
    return dict(state, mode='APPLY' if apply else 'CHECK', created_tables=created)


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
