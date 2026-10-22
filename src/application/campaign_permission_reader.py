"""Fresh read-only P6 permission snapshots; no local/cache fallback or approval.

Claims and all independently current evidence heads/histories share one SQL
snapshot. Returned permissions still require user access, scoped use checks,
actual source/rights verification, quality gates and final campaign review.
"""
from __future__ import annotations
from copy import deepcopy
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import islice
import json
from uuid import uuid4

from google.cloud import bigquery
from src.application.campaign_permission_storage import (
    MAX_HISTORY, MAX_PAYLOAD_BYTES, PREFIX, KINDS, decode_state, reconcile_claim_evidence, PermissionState,
)
from src.application.campaign_acceptance_contract import _utc
from src.application.workflow_contract import ContractError, _require, _id, _digest
from src.ingestion.setup_campaign_permission_tables import inspect
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION,settings

MAX_ROWS=2146  # 33 records * (one head + 64 revisions), plus overflow sentinel.


@dataclass(frozen=True,slots=True)
class Checkpoint:
    record_id: str
    minimum_revision: int=1
    sha256: str | None=None

    def __post_init__(self):
        _id(self.record_id)
        _require(type(self.minimum_revision) is int and 1<=self.minimum_revision<=MAX_HISTORY,
                 'INVALID_PERMISSION_CHECKPOINT')
        if self.sha256 is not None:_digest(self.sha256)


@dataclass(frozen=True,slots=True)
class PermissionRequest:
    kind: str
    checkpoint: Checkpoint
    evidence_checkpoints: tuple[Checkpoint,...]=()

    def __post_init__(self):
        _require(type(self.kind) is str and self.kind in KINDS
                 and isinstance(self.checkpoint,Checkpoint),'INVALID_PERMISSION_READ_REQUEST')
        _require(type(self.evidence_checkpoints) is tuple and len(self.evidence_checkpoints)<=32
            and all(isinstance(c,Checkpoint) for c in self.evidence_checkpoints)
            and tuple(c.record_id for c in self.evidence_checkpoints)
            ==tuple(sorted({c.record_id for c in self.evidence_checkpoints}))
            and (self.kind=='CLAIM' or not self.evidence_checkpoints),
            'INVALID_PERMISSION_EVIDENCE_CHECKPOINTS')


@dataclass(frozen=True,slots=True)
class PermissionSnapshot:
    state: PermissionState
    evidence_states: tuple[PermissionState,...]
    native_job_id: str
    checked_at: datetime


def query_spec(request):
    _require(isinstance(request,PermissionRequest),'INVALID_PERMISSION_READ_REQUEST')
    prefix=PREFIX[request.kind]
    hist=f'`{PROJECT}.{DATASET}.{prefix}_versions`'
    heads=f'`{PROJECT}.{DATASET}.{prefix}_heads`'
    evidence_hist=f'`{PROJECT}.{DATASET}.{PREFIX["EVIDENCE"]}_versions`'
    evidence_heads=f'`{PROJECT}.{DATASET}.{PREFIX["EVIDENCE"]}_heads`'
    # Independent evidence IDs are derived inside this same statement from every
    # target current-head match; conflicting/duplicate heads still fail decoding.
    evidence_cte=f'''SELECT DISTINCT JSON_VALUE(e,'$.evidence_id') record_id
 FROM target_heads h JOIN target_history v
 ON h.record_id=v.record_id AND h.revision=v.revision
 CROSS JOIN UNNEST(JSON_QUERY_ARRAY(v.payload_canonical_json,'$.record.evidence')) e'''
    if request.kind!='CLAIM':evidence_cte="SELECT record_id FROM UNNEST(ARRAY<STRING>[]) AS record_id"
    columns='schema_version,record_kind,record_id,revision,version,status,parent_sha256,record_sha256,payload_sha256,recorded_at'
    sql=f'''WITH target_heads AS (SELECT * FROM {heads} WHERE record_id=@record_id),
 target_history AS (SELECT * FROM {hist} WHERE record_id=@record_id),
 evidence_ids AS ({evidence_cte}),
 scoped AS (
 SELECT 'TARGET' scope_role,'HEAD' row_role,{columns},CAST(NULL AS STRING) payload_canonical_json FROM target_heads
 UNION ALL SELECT 'TARGET','HISTORY',{columns},payload_canonical_json FROM target_history
 UNION ALL SELECT 'EVIDENCE','HEAD',{columns},CAST(NULL AS STRING) payload_canonical_json
 FROM {evidence_heads} WHERE record_id IN (SELECT record_id FROM evidence_ids)
 UNION ALL SELECT 'EVIDENCE','HISTORY',{columns},payload_canonical_json
 FROM {evidence_hist} WHERE record_id IN (SELECT record_id FROM evidence_ids)
 )
 SELECT * REPLACE(IF(BYTE_LENGTH(payload_canonical_json)<={MAX_PAYLOAD_BYTES},
 payload_canonical_json,NULL) AS payload_canonical_json),
 COUNT(*) OVER(PARTITION BY scope_role,record_id,row_role) scoped_row_count
 FROM scoped ORDER BY scope_role,record_id,row_role,revision LIMIT {MAX_ROWS}'''
    cfg=settings([bigquery.ScalarQueryParameter('record_id','STRING',request.checkpoint.record_id)])
    return sql,cfg


def decode_snapshot(values,request,*,native_job_id,checked_at):
    _require(isinstance(request,PermissionRequest) and type(values) is tuple
             and len(values)<MAX_ROWS,'PERMISSION_READ_OVERFLOW')
    _id(native_job_id);now=_utc(checked_at)
    groups={}
    for supplied in values:
        row=dict(supplied)
        _require({'scope_role','row_role','scoped_row_count'}<=set(row),
                 'INVALID_PERMISSION_READ_ROW')
        role=row.pop('scope_role');part=row.pop('row_role');count=row.pop('scoped_row_count')
        _require(role in {'TARGET','EVIDENCE'} and part in {'HEAD','HISTORY'}
            and type(count) is int and 1<=count<=(1 if part=='HEAD' else MAX_HISTORY),
            'PERMISSION_READ_PARTIAL_OR_DUPLICATE')
        _id(row.get('record_id'))
        if role=='TARGET':
            _require(row['record_id']==request.checkpoint.record_id
                and row.get('record_kind')==request.kind,'PERMISSION_READ_SCOPE_MISMATCH')
        else:
            _require(request.kind=='CLAIM' and row.get('record_kind')=='EVIDENCE',
                     'PERMISSION_READ_SCOPE_MISMATCH')
        if part=='HEAD':
            _require(row.pop('payload_canonical_json',False) is None,'INVALID_PERMISSION_READ_HEAD')
        group=groups.setdefault((role,row['record_id']),{'HEAD':[],'HISTORY':[],'COUNTS':{}})
        if part in group['COUNTS']:_require(group['COUNTS'][part]==count,'PERMISSION_READ_COUNT_CONFLICT')
        group['COUNTS'][part]=count;group[part].append(row)
    for group in groups.values():
        _require(len(group['HEAD'])==1 and 1<=len(group['HISTORY'])<=MAX_HISTORY
            and all(len(group[p])==group['COUNTS'].get(p) for p in ('HEAD','HISTORY')),
            'PERMISSION_READ_INCOMPLETE')
    target=groups.pop(('TARGET',request.checkpoint.record_id),None)
    _require(target is not None,'PERMISSION_READ_MISSING')
    def state(group,kind,checkpoint):
        return decode_state(group['HEAD'][0],tuple(sorted(group['HISTORY'],key=lambda r:r['revision'])),
            kind=kind,record_id=checkpoint.record_id,minimum_revision=checkpoint.minimum_revision,
            checkpoint_sha256=checkpoint.sha256)
    current=state(target,request.kind,request.checkpoint)
    current.require_current(now)
    evidence=[]
    pins={c.record_id:c for c in request.evidence_checkpoints}
    if request.kind=='CLAIM':
        _require(set(pins)<=set(e.evidence_id for e in current.current.evidence),
                 'PERMISSION_EVIDENCE_CHECKPOINT_SCOPE_MISMATCH')
        for pin in current.current.evidence:
            group=groups.pop(('EVIDENCE',pin.evidence_id),None)
            _require(group is not None,'PERMISSION_EVIDENCE_HEAD_MISSING')
            evidence.append(state(group,'EVIDENCE',pins.get(pin.evidence_id,Checkpoint(pin.evidence_id))))
        reconcile_claim_evidence(current,tuple(evidence),checked_at=now)
    _require(not groups,'PERMISSION_READ_EXTRA_SCOPE')
    return PermissionSnapshot(current,tuple(evidence),native_job_id,now)


def read_permission(client,request,*,clock=lambda:datetime.now(timezone.utc)):
    _require(isinstance(request,PermissionRequest),'INVALID_PERMISSION_READ_REQUEST')
    _require(client.project==PROJECT,'PERMISSION_READ_PROJECT_MISMATCH')
    _require(inspect(client)['status']=='VERIFIED','PERMISSION_READ_TABLES_MISSING')
    sql,cfg=query_spec(request);native_id='campaign_permission_read_'+uuid4().hex
    expected=deepcopy(cfg.to_api_repr())
    job=client.query(sql,job_config=cfg,job_id=native_id,location=LOCATION,retry=None,job_retry=None,timeout=20)
    values=tuple(dict(r) for r in islice(job.result(timeout=60,max_results=MAX_ROWS,page_size=100,
        retry=None,job_retry=None),MAX_ROWS))
    original=client.get_job(native_id,location=LOCATION,retry=None,timeout=20)
    actual=original.to_api_repr().get('configuration',{})
    _require(original.project==PROJECT and (original.location or '').lower()==LOCATION
        and original.job_id==native_id and original.query==sql and original.state=='DONE'
        and original.error_result is None and actual.get('jobTimeoutMs')=='60000'
        and all(actual.get('query',{}).get(k)==v for k,v in expected['query'].items()),
        'PERMISSION_READ_NATIVE_JOB_MISMATCH')
    try:
        return decode_snapshot(values,request,native_job_id=native_id,checked_at=clock())
    except ContractError as error:
        # Bounded server-side diagnostic for a validated read job, never a
        # permission result, actor proof or mutation receipt.
        error.native_job_id=native_id
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind',choices=tuple(KINDS),required=True)
    parser.add_argument('--record-id',required=True)
    args=parser.parse_args()
    request=PermissionRequest(args.kind,Checkpoint(args.record_id))
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        try:
            snapshot=read_permission(client,request)
            print(json.dumps(dict(status='CURRENT_PERMISSION_READ',native_job_id=snapshot.native_job_id,
                record_sha256=snapshot.state.current.sha256,evidence_count=len(snapshot.evidence_states))))
        except ContractError as error:
            print(json.dumps(dict(status='DENIED',reason_code=error.code,kind=args.kind,record_id=args.record_id,
                                  native_job_id=getattr(error,'native_job_id',None))))
            raise SystemExit(2) from None


if __name__=='__main__':main()
