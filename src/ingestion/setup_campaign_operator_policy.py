"""Exact two-table resource setup; never seeds operator policies or roles."""
import argparse,json
from uuid import uuid4
from google.api_core.exceptions import NotFound,Conflict
from google.cloud import bigquery
from src.application.campaign_operator_policy_storage import SCHEMAS,STORAGE_VERSION,table_metadata
from src.application.operation_budget import operation_call,operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION,settings
from src.ingestion.setup_workflow_tables import validate_table,SchemaMismatch
from itertools import islice


def inspect(client):
    if client.project!=PROJECT or client.location!=LOCATION or client.default_query_job_config is not None:
        raise ValueError('Fixed operator-policy project/region/client required')
    dataset=operation_call(client.get_dataset,f'{PROJECT}.{DATASET}',retry=None,timeout=20)
    if (dataset.project!=PROJECT or dataset.dataset_id!=DATASET or (dataset.location or '').lower()!=LOCATION
        or dataset.default_table_expiration_ms is not None or dataset.default_partition_expiration_ms is not None):
        raise SchemaMismatch('Operator policy dataset identity/location/expiration differs')
    present=[];missing=[];counts={}
    for name in SCHEMAS:
        expected=table_metadata(name,PROJECT)
        try:actual=operation_call(client.get_table,expected.reference,retry=None,timeout=20)
        except NotFound:missing.append(name);continue
        validate_table(actual,expected);present.append(name);counts[name]=actual.num_rows
    return dict(status='MISSING_RESOURCES' if missing else 'VERIFIED',schema_version=STORAGE_VERSION,
        verified_tables=present,missing_tables=missing,metadata_row_counts=counts)


def verify_empty(client):
    sql=' UNION ALL '.join(f"SELECT '{name}' table_name,COUNT(*) row_count FROM `{PROJECT}.{DATASET}.{name}`" for name in SCHEMAS)
    cfg=settings([]);native='campaign_operator_policy_empty_'+uuid4().hex
    job=operation_call(client.query,sql,job_config=cfg,job_id=native,project=PROJECT,location=LOCATION,retry=None,job_retry=None,timeout=20)
    values=tuple(dict(row) for row in islice(operation_iter(operation_call(job.result,timeout=60,retry=None,
        job_retry=None,max_results=3,page_size=3)),3))
    original=operation_call(client.get_job,native,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    if (not matches_sharing_job(original,job_id=native,sql=sql,config=cfg) or original.state!='DONE' or original.error_result is not None):
        raise ValueError('Operator policy empty-check native job mismatch')
    counts={}
    for row in values:
        if (set(row)!={'table_name','row_count'} or row['table_name'] not in SCHEMAS or row['table_name'] in counts
            or type(row['row_count']) is not int or row['row_count']!=0):
            raise ValueError('Operator policy empty snapshot is malformed or nonempty')
        counts[row['table_name']]=row['row_count']
    if set(counts)!=set(SCHEMAS):raise ValueError('Operator policy empty snapshot incomplete')
    return dict(authoritative_row_counts=counts,empty_check_native_job_id=native)


def setup(client,*,apply=False,require_empty=False):
    if type(apply) is not bool or type(require_empty) is not bool:raise ValueError('Explicit setup flags required')
    before=inspect(client);created=[]
    if apply:
        for name in before['missing_tables']:
            expected=table_metadata(name,PROJECT)
            try:operation_call(client.create_table,expected,retry=None,timeout=20);created.append(name)
            except Conflict:pass
            validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
        before=inspect(client)
        if before['status']!='VERIFIED':raise SchemaMismatch('Operator policy resources unavailable')
    if require_empty and before['missing_tables']:raise ValueError('Operator policy resources missing')
    return dict(before,mode='APPLY' if apply else 'CHECK',created_tables=created,
        **(verify_empty(client) if require_empty else {}),policy_rows_written=0,serving_enabled=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__);modes=parser.add_mutually_exclusive_group(required=True)
    modes.add_argument('--check',action='store_true');modes.add_argument('--apply',action='store_true')
    parser.add_argument('--verify-empty',action='store_true');args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        print(json.dumps(setup(client,apply=args.apply,require_empty=args.verify_empty),indent=2))


if __name__=='__main__':main()
