"""Create/verify only extraction and omission-link tables; no rows/IAM changes."""
import argparse,json
from google.cloud import bigquery
from google.api_core.exceptions import NotFound,Conflict
from src.application.campaign_document_store import TABLES,table_metadata,PROJECT,LOCATION
from src.ingestion.setup_workflow_tables import validate_table
from src.application.workflow_contract import _require


def setup(client,*,apply=False):
    _require(client.project==PROJECT and client.location==LOCATION,'DOCUMENT_SETUP_CLIENT')
    missing=[];results=[]
    # Inspect every existing destination before creating any missing destination.
    for kind,name in TABLES.items():
        try:actual=client.get_table(name,retry=None,timeout=20)
        except NotFound:missing.append(kind)
        else:validate_table(actual,table_metadata(kind))
    for kind,name in TABLES.items():
        created=False
        if kind in missing and apply:
            try:client.create_table(table_metadata(kind),exists_ok=False,retry=None,timeout=20);created=True
            except Conflict:pass
            validate_table(client.get_table(name,retry=None,timeout=20),table_metadata(kind))
        results.append(dict(table=name,status='MISSING' if kind in missing and not apply else 'VERIFIED',created=created))
    return results


def main():
    p=argparse.ArgumentParser(description=__doc__);mode=p.add_mutually_exclusive_group()
    mode.add_argument('--check',action='store_true');mode.add_argument('--apply',action='store_true');args=p.parse_args()
    print(json.dumps(setup(bigquery.Client(project=PROJECT,location=LOCATION),apply=args.apply)))


if __name__=='__main__':main()
