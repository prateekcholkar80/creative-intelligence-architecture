"""Opt-in metadata-only planner input manifest setup; never creates data rows."""
import argparse
import json

from google.api_core.exceptions import Conflict,NotFound
from google.cloud import bigquery

from src.application.operation_budget import operation_call
from src.application.campaign_dispatch_inputs import SPEC,VERSION as STORAGE_VERSION
from src.application.campaign_transcript import SPEC as TRANSCRIPT_SPEC
TABLES={SPEC.name:SPEC,TRANSCRIPT_SPEC.name:TRANSCRIPT_SPEC}
from src.ingestion.setup_workflow_tables import DATASET_ID,LOCATION,PROJECT,validate_dataset,validate_table


def inspect(client):
    if client.project!=PROJECT or client.location!=LOCATION:
        raise ValueError('Fixed-project/region client required')
    validate_dataset(operation_call(client.get_dataset,DATASET_ID,retry=None,timeout=20))
    missing=[];counts={}
    for name,spec in TABLES.items():
        try:table=operation_call(client.get_table,spec.table(PROJECT).reference,retry=None,timeout=20)
        except NotFound:missing.append(name);continue
        validate_table(table,spec.table(PROJECT));counts[name]=table.num_rows
    return dict(schema_version=STORAGE_VERSION,missing_tables=missing,metadata_row_counts=counts,status='MISSING_RESOURCES' if missing else 'VERIFIED')


def setup(client,*,apply=False):
    if type(apply) is not bool:raise ValueError('Explicit boolean apply required')
    report=inspect(client);created=[]
    if apply:
        for name in report['missing_tables']:
            expected=TABLES[name].table(PROJECT)
            try:operation_call(client.create_table,expected,retry=None,timeout=20);created.append(name)
            except Conflict:pass
            validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
        report=inspect(client)
        if report['status']!='VERIFIED':raise ValueError('Runtime binding resources did not verify')
    return dict(report,created_tables=created,mode='APPLY' if apply else 'CHECK')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--check',action='store_true');group.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        print(json.dumps(setup(client,apply=args.apply),sort_keys=True))


if __name__=='__main__':main()
