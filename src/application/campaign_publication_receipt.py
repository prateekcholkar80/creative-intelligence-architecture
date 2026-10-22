"""Read-only historical business receipts, independent of native result ownership.

Absence is UNKNOWN, never permission to submit. A receipt does not establish
current claim eligibility or classify any particular native mutation job.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
from itertools import islice
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_publication_reader import _native_row
from src.application.campaign_permission_storage import PREFIX,decode_state
from src.application.campaign_review_storage import parse_row,row_json,decode_control_state
from src.application.campaign_permission_publication import MAX_CAPTURE_BYTES
from src.application.workflow_contract import _require,_id
from src.application.campaign_acceptance_contract import _utc
from src.application.operation_budget import operation_call,operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION,settings
from src.ingestion.setup_campaign_review_tables import inspect as inspect_reviews
from src.ingestion.setup_campaign_permission_tables import inspect as inspect_permissions

LIMIT=132  # one audit, two heads, at most 64 revisions for each chain, overflow sentinel


@dataclass(frozen=True,slots=True)
class PublicationReceipt:
    command_sha256: str
    state: str  # VERIFIED or UNKNOWN; not a native commit classification
    record_sha256: str | None
    control_sha256: str | None
    read_job_id: str
    checked_at: datetime


def query_spec(plan):
    audit=parse_row(plan.audit_json);prefix=PREFIX[audit['permission_kind']]
    selections=[]
    for kind,table,condition in (
        ('AUDIT','campaign_permission_publication_audits','command_sha256=@command'),
        ('CONTROL_HEAD','campaign_permission_controls','control_id=@control'),
        ('CONTROL_HISTORY','campaign_permission_control_history','control_id=@control'),
        ('PERMISSION_HEAD',prefix+'_heads','record_id=@record'),
        ('PERMISSION_HISTORY',prefix+'_versions','record_id=@record')):
        selections.append(f"SELECT '{kind}' capture_kind,IF(BYTE_LENGTH(TO_JSON_STRING(t))<={MAX_CAPTURE_BYTES},TO_JSON_STRING(t),NULL) record_json FROM `{PROJECT}.{DATASET}.{table}` t WHERE {condition}")
    sql=f"SELECT * FROM ({' UNION ALL '.join(selections)}) LIMIT {LIMIT}"
    cfg=settings([bigquery.ScalarQueryParameter('command','STRING',plan.command_sha256),
        bigquery.ScalarQueryParameter('control','STRING','campaign-permissions'),
        bigquery.ScalarQueryParameter('record','STRING',audit['record_id'])])
    return sql,cfg


def decode_receipt(values,intent,*,read_job_id,checked_at):
    _id(read_job_id);checked_at=_utc(checked_at)
    plan=intent.plan();expected=parse_row(plan.audit_json)
    _require(type(values) is tuple and len(values)<LIMIT,'PUBLICATION_RECEIPT_OVERFLOW')
    groups={kind:[] for kind in ('AUDIT','CONTROL_HEAD','CONTROL_HISTORY','PERMISSION_HEAD','PERMISSION_HISTORY')}
    size=0
    for value in values:
        row=dict(value)
        _require(set(row)=={'capture_kind','record_json'} and row['capture_kind'] in groups,
            'PUBLICATION_RECEIPT_SCOPE_MISMATCH')
        item=_native_row(row['record_json']);size+=len(row_json(item).encode())
        _require(size<=MAX_CAPTURE_BYTES,'PUBLICATION_RECEIPT_TOO_LARGE')
        groups[row['capture_kind']].append(item)
    _require(len(groups['AUDIT'])<=1,'PUBLICATION_RECEIPT_DUPLICATE')
    # Validate present state even when no receipt exists; partial chains fail closed.
    ch=groups['CONTROL_HEAD'];ph=groups['PERMISSION_HEAD']
    controls=tuple(sorted(groups['CONTROL_HISTORY'],key=lambda r:r['revision']))
    history=tuple(sorted(groups['PERMISSION_HISTORY'],key=lambda r:r['revision']))
    _require(len(ch)==1,'PUBLICATION_RECEIPT_CONTROL_MISSING')
    decode_control_state(ch[0],controls,checked_at)
    if ph or history:
        _require(len(ph)==1,'PUBLICATION_RECEIPT_PARTIAL_PERMISSION')
        state=decode_state(ph[0],history,kind=expected['permission_kind'],record_id=expected['record_id'])
        _require(state.recorded_at<=checked_at,'PUBLICATION_RECEIPT_FROM_FUTURE')
    if not groups['AUDIT']:
        return PublicationReceipt(plan.command_sha256,'UNKNOWN',None,None,read_job_id,checked_at)
    _require(groups['AUDIT'][0]==expected,'PUBLICATION_RECEIPT_AUDIT_CONFLICT')
    revision=expected['revision'];control_revision=expected['control_revision']
    _require(len(history)>=revision and len(controls)>=control_revision,
        'PUBLICATION_RECEIPT_MISSING_REVISION')
    original_permission=intent.capture.permission.decoded()[1]
    original_control=intent.capture.control.decoded()[1]
    _require(history[:revision-1]==original_permission and controls[:control_revision-1]==original_control,
        'PUBLICATION_RECEIPT_PREFIX_CONFLICT')
    committed=history[revision-1];control=controls[control_revision-1]
    _require(committed['record_sha256']==plan.proposed_record_sha256
        and committed['recorded_at']==expected['recorded_at']
        and control['control_sha256']==plan.candidate_control_sha256
        and control['command_sha256']==plan.command_sha256
        and control['changed_by']==expected['changed_by']
        and control['recorded_at']==expected['recorded_at'], 'PUBLICATION_RECEIPT_BINDING_CONFLICT')
    return PublicationReceipt(plan.command_sha256,'VERIFIED',plan.proposed_record_sha256,
        plan.candidate_control_sha256,read_job_id,checked_at)


def read_receipt(client,intent,*,clock=lambda:datetime.now(timezone.utc)):
    _require(client.project==PROJECT and client.location==LOCATION
        and client.default_query_job_config is None,'PUBLICATION_RECEIPT_CLIENT_MISMATCH')
    _require(inspect_reviews(client)['status']=='VERIFIED'
        and inspect_permissions(client)['status']=='VERIFIED','PUBLICATION_RECEIPT_TABLES_MISSING')
    plan=intent.plan();sql,cfg=query_spec(plan);native='campaign_publication_receipt_'+uuid4().hex
    job=operation_call(client.query,sql,job_config=cfg,job_id=native,project=PROJECT,
        location=LOCATION,retry=None,job_retry=None,timeout=20)
    values=tuple(dict(r) for r in islice(operation_iter(operation_call(job.result,
        timeout=60,retry=None,job_retry=None,max_results=LIMIT,page_size=LIMIT)),LIMIT))
    original=operation_call(client.get_job,native,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(original,job_id=native,sql=sql,config=cfg)
        and original.state=='DONE' and original.error_result is None,'PUBLICATION_RECEIPT_NATIVE_MISMATCH')
    return decode_receipt(values,intent,read_job_id=native,checked_at=clock())
