"""Create empty structured evidence stores and optionally verify an empty native read."""
import argparse,json
from datetime import datetime,timezone
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound,Conflict
from src.application.campaign_structured_evidence import (
    PROJECT,LOCATION,DATASET,RECORDS,REVOCATIONS,FIELDS,SQL,table_metadata,decode_rows,
)
from src.application.operation_budget import operation_call
from src.application.sharing_jobs import matches_sharing_job
from src.ingestion.setup_workflow_tables import validate_table


def setup(client,*,apply=False):
    if type(apply) is not bool or client.project!=PROJECT or client.location!=LOCATION:
        raise ValueError('Fixed project/region and explicit apply required')
    ds=operation_call(client.get_dataset,DATASET,retry=None,timeout=20)
    if ds.location.lower()!=LOCATION or ds.default_table_expiration_ms is not None or ds.default_partition_expiration_ms is not None:
        raise ValueError('Structured evidence dataset mismatch')
    missing=[];created=[];counts={}
    for name in (RECORDS,REVOCATIONS):
        expected=table_metadata(name)
        try:actual=operation_call(client.get_table,expected.reference,retry=None,timeout=20)
        except NotFound:
            missing.append(name)
            if not apply:continue
            try:operation_call(client.create_table,expected,retry=None,timeout=20);created.append(name)
            except Conflict:pass
            actual=operation_call(client.get_table,expected.reference,retry=None,timeout=20)
        validate_table(actual,expected);counts[name]=actual.num_rows
    return dict(status='VERIFIED' if apply or not missing else 'MISSING_RESOURCES',created_tables=created,metadata_row_counts=counts)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true')
    p.add_argument('--verify-empty-sku');args=p.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        report=setup(client,apply=args.apply)
        if args.verify_empty_sku:
            if args.verify_empty_sku not in {f'CI-SKU-{i:04d}' for i in range(1,24)}:raise ValueError('Unknown canonical SKU')
            reads=[]
            for kind in FIELDS:
                cfg=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('sku','STRING',args.verify_empty_sku),
                    bigquery.ScalarQueryParameter('kind','STRING',kind)],use_legacy_sql=False,use_query_cache=False,
                    create_session=False,maximum_bytes_billed=128*1024**2,job_timeout_ms=60000)
                nid='structured_empty_verification_'+uuid4().hex
                job=client.query(SQL,job_config=cfg,job_id=nid,location=LOCATION,retry=None,job_retry=None,timeout=20)
                job.result(max_results=0,retry=None,job_retry=None,timeout=45)
                original=client.get_job(nid,project=PROJECT,location=LOCATION,retry=None,timeout=20)
                if not matches_sharing_job(original,job_id=nid,sql=SQL,config=cfg) or original.state!='DONE' or original.error_result:
                    raise ValueError('Native structured read mismatch')
                rows=list(original.result(max_results=2,retry=None,job_retry=None,timeout=20))
                if len(rows)!=1:raise ValueError('Native structured row count')
                records,revocations,sha=decode_rows(dict(rows[0]),sku=args.verify_empty_sku,kind=kind,now=datetime.now(timezone.utc))
                if records or revocations:raise ValueError('Store is not empty; no data changed')
                reads.append(dict(kind=kind,native_job_id=nid,source_sha256=sha))
            report['native_empty_reads']=reads;report['authentication_scope']='ADC_OPERATOR_NOT_DEPLOYED_IAP'
        print(json.dumps(report,sort_keys=True))

if __name__=='__main__':main()
