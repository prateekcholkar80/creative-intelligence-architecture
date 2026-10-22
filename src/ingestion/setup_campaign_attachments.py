"""Explicit attachment-table setup. No rows, buckets, IAM or activation."""
import argparse,json
from google.cloud import bigquery
from google.api_core.exceptions import NotFound,Conflict
from src.application.campaign_attachment_service import table_metadata,PROJECT,LOCATION,TABLE
from src.ingestion.setup_workflow_tables import validate_table
from src.application.workflow_contract import _require


def setup(client,*,apply=False):
    _require(client.project==PROJECT and client.location==LOCATION,'ATTACHMENT_SETUP_SCOPE')
    expected=table_metadata();created=False
    try:actual=client.get_table(TABLE,retry=None,timeout=20)
    except NotFound:
        if not apply:return dict(table=TABLE,status='MISSING',created=False)
        try:client.create_table(expected,exists_ok=False,retry=None,timeout=20);created=True
        except Conflict:pass
        actual=client.get_table(TABLE,retry=None,timeout=20)
    validate_table(actual,expected)
    return dict(table=TABLE,status='VERIFIED',created=created)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group();mode.add_argument('--check',action='store_true');mode.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    print(json.dumps(setup(bigquery.Client(project=PROJECT,location=LOCATION),apply=args.apply)))


if __name__=='__main__':main()
