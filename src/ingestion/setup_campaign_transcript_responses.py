"""Create/verify only private 30-day response storage; never seed response rows."""
import argparse,json
from google.api_core.exceptions import NotFound,Conflict
from google.cloud import bigquery
from src.application.campaign_response_transcript import SPEC
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table


def setup(client,*,apply=False):
    expected=SPEC.table(PROJECT);created=False
    try:actual=client.get_table(expected.reference,retry=None,timeout=20)
    except NotFound:
        if not apply:return dict(status='MISSING_RESOURCE',table=str(expected.reference),created=False,rows_written=0)
        try:client.create_table(expected,exists_ok=False,retry=None,timeout=20);created=True
        except Conflict:pass
        actual=client.get_table(expected.reference,retry=None,timeout=20)
    validate_table(actual,expected)
    return dict(status='VERIFIED',table=str(expected.reference),created=created,rows_written=0,metadata_row_count=actual.num_rows)


def main():
    parser=argparse.ArgumentParser(description=__doc__);mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--apply',action='store_true');mode.add_argument('--check',action='store_true');args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:print(json.dumps(setup(client,apply=args.apply)))

if __name__=='__main__':main()
