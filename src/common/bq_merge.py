"""Schema-explicit scoped upserts for newly introduced brand-memory records."""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from google.cloud import bigquery

from src.generators.sales_generator import LOCATION


def merge_statement(target: str, stage: str, columns: list[str], keys: list[str],
                    pending_only: bool = False) -> str:
    for identifier in [target, stage, *columns, *keys]:
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', identifier):
            raise ValueError('Invalid SQL identifier')
    join = ' AND '.join(f'T.{k}=S.{k}' for k in keys)
    protected = {'created_at', 'approved_at', 'approved_by', 'approval_reference'}
    update = ', '.join(f'T.{c}=S.{c}' for c in columns if c not in set(keys) | protected)
    condition = " AND T.approval_status='PENDING_REVIEW'" if pending_only else ''
    return f"""MERGE `{target}` T USING `{stage}` S ON {join}
        WHEN MATCHED{condition} THEN UPDATE SET {update}
        WHEN NOT MATCHED THEN INSERT ({', '.join(columns)})
        VALUES ({', '.join('S.' + c for c in columns)})"""


def upsert(client: bigquery.Client, target: str, schema: list[bigquery.SchemaField],
           rows: list[dict], keys: list[str], pending_only: bool = False) -> None:
    if not rows or len({tuple(r[k] for k in keys) for r in rows}) != len(rows):
        raise ValueError('Unique nonempty payload required')
    client.create_table(bigquery.Table(target, schema=schema), exists_ok=True)
    destination = client.get_table(target)
    if {(f.name, f.field_type, f.mode) for f in destination.schema} != {
            (f.name, f.field_type, f.mode) for f in schema}:
        raise ValueError('Destination schema differs; refusing to alter it')
    columns = [f.name for f in destination.schema]
    if any(set(r) != set(columns) for r in rows):
        raise ValueError('Payload differs from destination schema')
    stage = target.rsplit('.',1)[0] + '._stage_brand_memory_' + uuid.uuid4().hex
    table = bigquery.Table(stage, schema=destination.schema)
    table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    client.create_table(table)
    try:
        client.load_table_from_dataframe(pd.DataFrame(rows)[columns], stage,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY), location=LOCATION).result()
        group = ', '.join(f'T.{k}' for k in keys)
        duplicates = list(client.query(f"""SELECT {group} FROM `{target}` T
            JOIN `{stage}` S USING({', '.join(keys)}) GROUP BY {group}
            HAVING COUNT(*) > 1 LIMIT 1""", location=LOCATION).result())
        if duplicates:
            raise ValueError('Duplicate destination keys')
        client.query(merge_statement(target, stage, columns, keys, pending_only), location=LOCATION).result()
    finally:
        client.delete_table(stage, not_found_ok=True)
