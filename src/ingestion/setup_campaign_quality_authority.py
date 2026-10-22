"""Create/check only the E2 request-authority table; never seed selection rows."""
import argparse
import json
from google.api_core.exceptions import NotFound,Conflict
from google.cloud import bigquery
from src.application.campaign_quality_authority import TABLE,table_metadata
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_dataset,validate_table


def setup(client,*,apply=False):
    if type(apply) is not bool or client.project!=PROJECT or client.location!=LOCATION:
        raise ValueError('Fixed-project/region setup requires explicit boolean apply')
    validate_dataset(client.get_dataset(PROJECT+'.ci_application',retry=None,timeout=20))
    expected=table_metadata();created=False
    try:actual=client.get_table(TABLE,retry=None,timeout=20)
    except NotFound:
        if not apply:return dict(status='MISSING_RESOURCE',table=TABLE,created=False,rows_written=0)
        try:client.create_table(expected,exists_ok=False,retry=None,timeout=20);created=True
        except Conflict:pass
        actual=client.get_table(TABLE,retry=None,timeout=20)
    validate_table(actual,expected)
    return dict(status='VERIFIED',table=TABLE,created=created,metadata_row_count=actual.num_rows,rows_written=0)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--check',action='store_true');mode.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        print(json.dumps(setup(client,apply=args.apply)),flush=True)


if __name__=='__main__':main()
