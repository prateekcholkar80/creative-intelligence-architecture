"""P6 permission metadata/codecs and supplied-history checks. No cloud I/O.

Schemas are proposals, not provisioned resources. Hashes do not authenticate
approval or prove GCS bytes. Claim evidence must reconcile with fresh independent
review heads before use; embedded historical evidence is not current permission.
"""
from __future__ import annotations
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
import json
from types import MappingProxyType

from google.cloud import bigquery
from src.application.campaign_permission_contract import (
    Approval, EvidencePin, ClaimPermission, OfferPermission, EvidenceKind, ClaimKind, _decimal,
)
from src.application.campaign_acceptance_contract import PolicyStatus, _utc
from src.application.workflow_contract import ContractError, _require, _id, _digest, canonical_json_bytes
from src.ingestion.configure_campaign_acceptance import digest

STORAGE_VERSION='campaign-permission-storage-v1'
MAX_HISTORY=64
MAX_PAYLOAD_BYTES=262144
KINDS={'CLAIM':ClaimPermission,'OFFER':OfferPermission,'EVIDENCE':EvidencePin}
PREFIX={'CLAIM':'campaign_claim_permissions','OFFER':'campaign_offer_permissions',
        'EVIDENCE':'campaign_evidence_reviews'}
BASE=(('schema_version','STRING','REQUIRED'),('record_kind','STRING','REQUIRED'),
      ('record_id','STRING','REQUIRED'),('revision','INTEGER','REQUIRED'),
      ('version','STRING','REQUIRED'),('status','STRING','REQUIRED'),
      ('parent_sha256','STRING','NULLABLE'),('record_sha256','STRING','REQUIRED'),
      ('payload_sha256','STRING','REQUIRED'),('recorded_at','TIMESTAMP','REQUIRED'))
TABLE_SCHEMAS=MappingProxyType({name:BASE+(('payload_canonical_json','STRING','REQUIRED'),)
    if name.endswith('_versions') else BASE for prefix in PREFIX.values()
    for name in (prefix+'_versions',prefix+'_heads')})


def table_metadata(name,project):
    _require(name in TABLE_SCHEMAS,'INVALID_PERMISSION_TABLE')
    result=bigquery.Table(f'{project}.ci_brand_memory.{name}',schema=[bigquery.SchemaField(n,t,mode=m)
                         for n,t,m in TABLE_SCHEMAS[name]])
    result.clustering_fields=['record_id','revision']
    result.description=f'{STORAGE_VERSION}: proposed scoped permission history/head; not approval'
    return result


def _encode(value):
    if is_dataclass(value): return {f.name:_encode(getattr(value,f.name)) for f in fields(value)}
    if isinstance(value,datetime): return _utc(value).isoformat()
    if isinstance(value,Decimal): return _decimal(value)
    if isinstance(value,Enum): return value.value
    if isinstance(value,tuple): return [_encode(v) for v in value]
    return value


def _shape(data,cls):
    _require(type(data) is dict and set(data)=={f.name for f in fields(cls)},
             'INVALID_PERMISSION_PAYLOAD_SHAPE')
    return dict(data)


def _timestamp(value):
    _require(type(value) is str,'INVALID_PERMISSION_STORED_TIMESTAMP')
    try: return _utc(datetime.fromisoformat(value))
    except (ValueError,TypeError,OverflowError): raise ContractError('INVALID_PERMISSION_STORED_TIMESTAMP') from None


def _number(value):
    _require(type(value) is str,'INVALID_PERMISSION_STORED_DECIMAL')
    try: return Decimal(value)
    except InvalidOperation: raise ContractError('INVALID_PERMISSION_STORED_DECIMAL') from None


def _approval(data):
    values=_shape(data,Approval)
    values['status']=PolicyStatus(values['status'])
    for key in ('effective_at','valid_until'):values[key]=_timestamp(values[key])
    if values['approved_at'] is not None:values['approved_at']=_timestamp(values['approved_at'])
    return Approval(**values)


def _evidence(data):
    values=_shape(data,EvidencePin)
    values['kind']=EvidenceKind(values['kind']); values['review']=_approval(values['review'])
    if values['statistic'] is not None:values['statistic']=_number(values['statistic'])
    return EvidencePin(**values)


def _permission(data,kind):
    if kind=='EVIDENCE':return _evidence(data)
    values=_shape(data,KINDS[kind]);values['approval']=_approval(values['approval'])
    array_keys=['countries','channels','audiences','qualifications']
    if kind=='CLAIM':
        values['kind']=ClaimKind(values['kind'])
        _require(type(values['evidence']) is list,'INVALID_PERMISSION_STORED_ARRAY')
        values['evidence']=tuple(_evidence(e) for e in values['evidence'])
        array_keys.append('wording')
    else:
        array_keys.extend(['skus','gift_skus'])
        for key in ('threshold','discount_percent'):values[key]=_number(values[key])
    for key in array_keys:
        _require(type(values[key]) is list,'INVALID_PERMISSION_STORED_ARRAY')
        values[key]=tuple(values[key])
    return KINDS[kind](**values)


def _kind(record):
    for kind,cls in KINDS.items():
        if type(record) is cls:return kind
    raise ContractError('INVALID_PERMISSION_RECORD')


def _identity(record,kind):
    return getattr(record,{'CLAIM':'claim_id','OFFER':'offer_id','EVIDENCE':'evidence_id'}[kind])


def _review(record,kind):return record.review if kind=='EVIDENCE' else record.approval


def encode_record(record, *, recorded_at):
    kind=_kind(record); approval=_review(record,kind)
    payload=canonical_json_bytes(dict(storage_version=STORAGE_VERSION,record_kind=kind,record=_encode(record)))
    _require(len(payload)<=MAX_PAYLOAD_BYTES,'PERMISSION_PAYLOAD_TOO_LARGE')
    _require(approval.revision<=MAX_HISTORY,'PERMISSION_HISTORY_LIMIT')
    return dict(schema_version=STORAGE_VERSION,record_kind=kind,record_id=_identity(record,kind),
        revision=approval.revision,version=approval.version,status=approval.status.value,
        parent_sha256=approval.parent_sha256,record_sha256=record.sha256,payload_sha256=digest(payload),
        recorded_at=_utc(recorded_at),payload_canonical_json=payload.decode())


def head_row(history_row):
    _require(type(history_row) is dict and 'payload_canonical_json' in history_row,
             'INVALID_PERMISSION_HISTORY_ROW')
    return {k:v for k,v in history_row.items() if k!='payload_canonical_json'}


def _row_types(row,schema):
    for name,kind,mode in schema:
        value=row[name]
        if value is None and mode=='NULLABLE':continue
        if kind=='STRING':_require(type(value) is str,'INVALID_PERMISSION_ROW_TYPE')
        elif kind=='INTEGER':_require(type(value) is int and 1<=value<=MAX_HISTORY,
                                      'INVALID_PERMISSION_ROW_TYPE')
        else:_utc(value)


def decode_record(row, *, kind, record_id):
    _require(kind in KINDS,'INVALID_PERMISSION_KIND');_id(record_id)
    _require(type(row) is dict and set(row)=={n for n,_,_ in TABLE_SCHEMAS[PREFIX[kind]+'_versions']},
             'INVALID_PERMISSION_ROW_SHAPE')
    _row_types(row,TABLE_SCHEMAS[PREFIX[kind]+'_versions'])
    _require(row['record_kind']==kind and row['record_id']==record_id
             and row['schema_version']==STORAGE_VERSION,'PERMISSION_ROW_SCOPE_MISMATCH')
    value=row['payload_canonical_json']
    _require(type(value) is str,'INVALID_PERMISSION_STORAGE_JSON')
    try:
        raw=value.encode('utf-8')
        _require(len(raw)<=MAX_PAYLOAD_BYTES,'PERMISSION_PAYLOAD_TOO_LARGE')
        data=json.loads(value)
        _require(type(data) is dict and set(data)=={'storage_version','record_kind','record'}
            and data['storage_version']==STORAGE_VERSION and data['record_kind']==kind,
            'INVALID_PERMISSION_STORAGE_JSON')
        _require(canonical_json_bytes(data)==raw,'NONCANONICAL_PERMISSION_STORAGE_JSON')
        record=_permission(data['record'],kind)
        _require(encode_record(record,recorded_at=row['recorded_at'])==row,
                 'PERMISSION_ROW_HASH_OR_PROJECTION_MISMATCH')
        return record
    except (ValueError,TypeError,UnicodeError,RecursionError,KeyError,OverflowError) as error:
        if isinstance(error,ContractError):raise
        raise ContractError('INVALID_PERMISSION_STORAGE_JSON') from None


@dataclass(frozen=True, slots=True)
class PermissionState:
    kind: str
    record_id: str
    current: ClaimPermission | OfferPermission | EvidencePin
    history: tuple[ClaimPermission | OfferPermission | EvidencePin, ...]
    recorded_at: datetime

    def require_current(self,checked_at):
        _require(_utc(checked_at)>=self.recorded_at,'PERMISSION_RECORD_FROM_FUTURE')
        _review(self.current,self.kind).require_current(checked_at)
        return self.current


def decode_state(head, history, *, kind, record_id, minimum_revision=1, checkpoint_sha256=None):
    _require(kind in KINDS,'INVALID_PERMISSION_KIND');_id(record_id)
    _require(type(minimum_revision) is int and 1<=minimum_revision<=MAX_HISTORY,
             'INVALID_PERMISSION_CHECKPOINT')
    if checkpoint_sha256 is not None:_digest(checkpoint_sha256)
    _require(type(head) is dict and set(head)=={n for n,_,_ in TABLE_SCHEMAS[PREFIX[kind]+'_heads']},
             'INVALID_PERMISSION_HEAD')
    _row_types(head,TABLE_SCHEMAS[PREFIX[kind]+'_heads'])
    _require(type(history) is tuple and 1<=len(history)<=MAX_HISTORY,
             'INVALID_PERMISSION_HISTORY')
    records=tuple(decode_record(row,kind=kind,record_id=record_id) for row in history)
    _require(tuple(_review(r,kind).revision for r in records)==tuple(range(1,len(records)+1)),
             'PERMISSION_HISTORY_GAP_OR_DUPLICATE')
    _require(head==head_row(history[-1]) and len(records)>=minimum_revision,
             'PERMISSION_HEAD_OR_CHECKPOINT_MISMATCH')
    if checkpoint_sha256 is not None:
        _require(records[minimum_revision-1].sha256==checkpoint_sha256,'PERMISSION_CHECKPOINT_CONFLICT')
    for previous,current in zip(records,records[1:]):
        before,after=_review(previous,kind),_review(current,kind)
        _require(after.parent_sha256==previous.sha256,'PERMISSION_PARENT_CONFLICT')
        _require(_utc(history[after.revision-1]['recorded_at'])>=_utc(history[before.revision-1]['recorded_at']),
                 'PERMISSION_HISTORY_TIME_CONFLICT')
        _require(before.status is not PolicyStatus.REVOKED,'PERMISSION_REVOCATION_TERMINAL')
        # Formula identity is immutable within one claim/evidence chain; create
        # a separately mapped record for a changed formulation.
        if kind in {'CLAIM','EVIDENCE'}:
            _require(previous.sku==current.sku and previous.formula_version==current.formula_version,
                     'PERMISSION_IDENTITY_CHANGED')
    return PermissionState(kind,record_id,records[-1],records,_utc(history[-1]['recorded_at']))


def reconcile_claim_evidence(claim_state, evidence_states, *, checked_at):
    _require(isinstance(claim_state,PermissionState) and claim_state.kind=='CLAIM'
             and type(evidence_states) is tuple and all(isinstance(s,PermissionState) and s.kind=='EVIDENCE'
                                                       for s in evidence_states), 'INVALID_PERMISSION_BUNDLE')
    claim=claim_state.require_current(checked_at)
    _require(tuple(s.record_id for s in evidence_states)==tuple(e.evidence_id for e in claim.evidence),
             'PERMISSION_EVIDENCE_COVERAGE_REQUIRED')
    for pin,state in zip(claim.evidence,evidence_states):
        actual=state.require_current(checked_at)
        _require(actual.sha256==pin.sha256,'PERMISSION_EVIDENCE_HEAD_CHANGED')
    return claim
