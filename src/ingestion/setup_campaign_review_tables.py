"""Provision only seven P6 control/review resources; never write permission rows.

--check is read-only; --apply creates missing exact tables after full preflight.
--verify-empty requires a fresh authoritative zero-row snapshot in all seven tables.
No datasets, IAM, migrations, approvals or serving configuration are changed.
"""
from __future__ import annotations
import argparse
import json
from uuid import uuid4

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from src.application.campaign_review_storage import SCHEMAS as TABLE_SCHEMAS, STORAGE_VERSION, table_metadata
from src.ingestion.configure_campaign_acceptance import PROJECT, LOCATION, DATASET
from src.ingestion.setup_workflow_tables import SchemaMismatch, validate_table

TIMEOUT_SECONDS=20


def inspect(client):
    if client.project!=PROJECT:raise ValueError('Review setup requires configured project')
    ds=client.get_dataset(f'{PROJECT}.{DATASET}',retry=None,timeout=TIMEOUT_SECONDS)
    if (ds.project!=PROJECT or ds.dataset_id!=DATASET or (ds.location or '').lower()!=LOCATION
            or ds.default_table_expiration_ms is not None or ds.default_partition_expiration_ms is not None):
        raise SchemaMismatch('Review dataset identity/location/expiration differs')
    present,missing,counts=[],[],{}
    for name in TABLE_SCHEMAS:
        expected=table_metadata(name,PROJECT)
        try:actual=client.get_table(expected.reference,retry=None,timeout=TIMEOUT_SECONDS)
        except NotFound:
            missing.append(name);continue
        validate_table(actual,expected)
        present.append(name);counts[name]=actual.num_rows
    return dict(schema_version=STORAGE_VERSION,project=PROJECT,dataset=DATASET,location=LOCATION,
        verified_tables=present,missing_tables=missing,metadata_row_counts=counts,
        status='MISSING_RESOURCES' if missing else 'VERIFIED')


def verify_empty(client):
    sql='\nUNION ALL\n'.join(f"SELECT '{name}' table_name,COUNT(*) row_count FROM `{PROJECT}.{DATASET}.{name}`"
                           for name in TABLE_SCHEMAS)
    # No parameters are used. BigQuery omits an empty queryParameters array in
    # native job metadata, so do not request one or weaken exact correlation.
    cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=256*1024*1024,
                               job_timeout_ms=60000)
    native_id='campaign_reviews_empty_'+uuid4().hex
    job=client.query(sql,job_config=cfg,job_id=native_id,location=LOCATION,
                     retry=None,job_retry=None,timeout=TIMEOUT_SECONDS)
    values=list(job.result(timeout=60,max_results=8,page_size=8))
    original=client.get_job(native_id,location=LOCATION,retry=None,timeout=TIMEOUT_SECONDS)
    config=original.to_api_repr().get('configuration',{})
    if (original.project!=PROJECT or (original.location or '').lower()!=LOCATION
            or original.job_id!=native_id or original.query!=sql
            or original.state!='DONE' or original.error_result is not None
            or config.get('jobTimeoutMs')!='60000'
            or any(config.get('query',{}).get(k)!=v for k,v in cfg.to_api_repr()['query'].items())):
        raise ValueError(f'Review empty-check original native job mismatch: {native_id}')
    counts={}
    for supplied in values:
        row=dict(supplied)
        if (set(row)!={'table_name','row_count'} or type(row['table_name']) is not str
                or row['table_name'] not in TABLE_SCHEMAS or row['table_name'] in counts
                or type(row['row_count']) is not int or row['row_count']!=0):
            raise ValueError('Review empty snapshot is malformed, duplicate or nonempty')
        counts[row['table_name']]=row['row_count']
    if set(counts)!=set(TABLE_SCHEMAS):raise ValueError('Review empty snapshot is incomplete')
    return dict(authoritative_row_counts=counts,empty_check_native_job_id=native_id)


def setup(client, *, apply=False, require_empty=False):
    if type(apply) is not bool or type(require_empty) is not bool:
        raise ValueError('Explicit boolean setup flags required')
    before=inspect(client);created=[]
    if apply:
        for name in before['missing_tables']:
            expected=table_metadata(name,PROJECT)
            try:
                client.create_table(expected,retry=None,timeout=TIMEOUT_SECONDS);created.append(name)
            except Conflict:pass
            validate_table(client.get_table(expected.reference,retry=None,timeout=TIMEOUT_SECONDS),expected)
        before=inspect(client)
        if before['status']!='VERIFIED':raise SchemaMismatch('Review setup did not verify all resources')
    if require_empty and before['missing_tables']:
        raise ValueError('Cannot verify empty snapshot with missing review tables')
    empty=verify_empty(client) if require_empty else {}
    return dict(before,mode='APPLY' if apply else 'CHECK',created_tables=created,**empty,
                review_rows_written=0,serving_enabled=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--check',action='store_true');mode.add_argument('--apply',action='store_true')
    parser.add_argument('--verify-empty',action='store_true')
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        print(json.dumps(setup(client,apply=args.apply,require_empty=args.verify_empty),indent=2))


if __name__=='__main__':main()
