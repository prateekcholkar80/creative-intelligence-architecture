"""Create/verify only P6 measurement tables; never seed measurements or approvals."""
import argparse
import json
from google.api_core.exceptions import NotFound,Conflict
from google.cloud import bigquery
from src.application.campaign_quality_qualification import SCHEMAS,table_metadata
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table


def setup(client, *, apply=False):
    _require(type(apply) is bool and client.project==PROJECT and client.location==LOCATION
        and client.default_query_job_config is None,'QUALITY_MEASUREMENT_SETUP_CONFIGURATION')
    dataset=client.get_dataset(PROJECT+'.ci_brand_memory',retry=None,timeout=20)
    _require(dataset.project==PROJECT and dataset.dataset_id=='ci_brand_memory'
        and dataset.location.lower()==LOCATION and dataset.default_table_expiration_ms is None,
        'QUALITY_MEASUREMENT_SETUP_DATASET')
    tables=[]
    for name in SCHEMAS:
        expected=table_metadata(name);created=False
        try:actual=client.get_table(expected.reference,retry=None,timeout=20)
        except NotFound:
            if not apply:
                tables.append(dict(table=name,state='MISSING',created=False));continue
            try:
                client.create_table(expected,exists_ok=False,retry=None,timeout=20);created=True
            except Conflict:
                pass  # A competing creator is verified; never claimed as our creation.
            actual=client.get_table(expected.reference,retry=None,timeout=20)
        validate_table(actual,expected)
        tables.append(dict(table=name,state='VERIFIED',created=created,rows_metadata=actual.num_rows))
    return dict(status='VERIFIED' if all(t['state']=='VERIFIED' for t in tables) else 'MISSING',
        dataset=PROJECT+'.ci_brand_memory',tables=tables,rows_written=0,measured_reports_written=0,grants_created=0)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        print(json.dumps(setup(client,apply=args.apply),indent=2))


if __name__=='__main__':main()
