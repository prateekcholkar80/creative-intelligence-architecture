"""Fresh all-table genesis captures; counts prevent omitted/truncated emptiness.

Server-only trusted manifest and verified identity inputs, never browser values.
This reader authenticates no JWT, submits no mutation and infers no reviewer role.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
from itertools import islice
from uuid import uuid4
from src.application.campaign_bootstrap_contract import BootstrapManifest,BootstrapSnapshot,TABLES
from src.application.campaign_publication_reader import _identity,_native_row
from src.application.campaign_review_storage import row_json
from src.application.campaign_acceptance_contract import _utc
from src.application.workflow_contract import _require,_id
from src.application.operation_budget import operation_call,operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.ingestion.setup_campaign_review_tables import inspect as inspect_reviews
from src.ingestion.setup_campaign_permission_tables import inspect as inspect_permissions
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION,settings

MAX_ROWS=len(TABLES)+1
MAX_BYTES=65536


@dataclass(frozen=True,slots=True)
class BootstrapCapture:
    snapshot: BootstrapSnapshot
    native_job_id: str
    checked_at: datetime


def authorize_owner(manifest,identity,expected_audience,checked_at):
    _require(isinstance(manifest,BootstrapManifest),'INVALID_CAMPAIGN_BOOTSTRAP_MANIFEST')
    _identity(identity,expected_audience,_utc(checked_at))
    _require(manifest.expected_audience==expected_audience.expected_audience
        and identity.principal.principal_id==manifest.owner_principal_id,'CAMPAIGN_BOOTSTRAP_OWNER_DENIED')


def query_spec():
    # Each aggregate returns a marker even for an empty table. Three rows/counts
    # detect overflow; oversized rows cannot silently disappear as empty tables.
    parts=[f"SELECT '{name}' table_name,COUNT(*) row_count,ARRAY_AGG(IF(BYTE_LENGTH(TO_JSON_STRING(t))<={MAX_BYTES},TO_JSON_STRING(t),NULL) IGNORE NULLS LIMIT 3) records FROM `{PROJECT}.{DATASET}.{name}` t" for name in TABLES]
    return ' UNION ALL '.join(parts)+f' LIMIT {MAX_ROWS}',settings([])


def decode_capture(values,*,native_job_id,checked_at):
    _id(native_job_id);now=_utc(checked_at)
    _require(type(values) is tuple and len(values)==len(TABLES),'CAMPAIGN_BOOTSTRAP_READ_INCOMPLETE')
    captured={};size=0
    for supplied in values:
        row=dict(supplied)
        _require(set(row)=={'table_name','row_count','records'} and row['table_name'] in TABLES
            and row['table_name'] not in captured,'CAMPAIGN_BOOTSTRAP_READ_SCOPE')
        count=row['row_count'];records=row['records']
        _require(type(count) is int and 0<=count<=2,'CAMPAIGN_BOOTSTRAP_READ_OVERFLOW')
        if records is None:records=()
        _require(type(records) in (list,tuple) and len(records)==count,'CAMPAIGN_BOOTSTRAP_READ_TRUNCATED')
        normalized=[]
        for record in records:
            data=_native_row(record);text=row_json(data);size+=len(text.encode())
            _require(size<=MAX_BYTES,'CAMPAIGN_BOOTSTRAP_READ_TOO_LARGE')
            _require(data['recorded_at']<=now,'CAMPAIGN_BOOTSTRAP_READ_FROM_FUTURE')
            normalized.append(text)
        captured[row['table_name']]=tuple(sorted(normalized))
    return BootstrapCapture(BootstrapSnapshot(tuple((name,captured[name]) for name in TABLES)),native_job_id,now)


def read_capture(client,manifest,identity,*,expected_audience,clock=lambda:datetime.now(timezone.utc)):
    authorize_owner(manifest,identity,expected_audience,clock())
    _require(client.project==PROJECT and client.location==LOCATION
        and client.default_query_job_config is None,'CAMPAIGN_BOOTSTRAP_READ_CLIENT')
    _require(inspect_reviews(client)['status']=='VERIFIED' and inspect_permissions(client)['status']=='VERIFIED',
        'CAMPAIGN_BOOTSTRAP_READ_RESOURCES')
    # Authentication may expire during metadata lookups. Do not issue a query then.
    authorize_owner(manifest,identity,expected_audience,clock())
    sql,cfg=query_spec();native='campaign_bootstrap_read_'+uuid4().hex
    job=operation_call(client.query,sql,job_config=cfg,job_id=native,project=PROJECT,location=LOCATION,
        retry=None,job_retry=None,timeout=20)
    values=tuple(dict(row) for row in islice(operation_iter(operation_call(job.result,
        timeout=60,retry=None,job_retry=None,max_results=MAX_ROWS,page_size=MAX_ROWS)),MAX_ROWS))
    original=operation_call(client.get_job,native,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(original,job_id=native,sql=sql,config=cfg)
        and original.state=='DONE' and original.error_result is None,'CAMPAIGN_BOOTSTRAP_READ_NATIVE_MISMATCH')
    result=decode_capture(values,native_job_id=native,checked_at=clock())
    authorize_owner(manifest,identity,expected_audience,clock())
    return result
