"""Authoritative operator-policy schemas and canonical full-chain decoding.

Pure codecs do not publish policy, authenticate publishers or confer authority.
These two tables are separate from acceptance rules and the 13 workflow controls.
"""
from dataclasses import dataclass,fields
from datetime import datetime
from types import MappingProxyType
import json,re
from google.cloud import bigquery
from src.application.campaign_review_publication_contract import ReviewOperatorPolicy
from src.application.campaign_permission_storage import _encode,_timestamp
from src.application.campaign_acceptance_contract import _utc,_hash
from src.application.campaign_review_storage import row_json
from src.application.workflow_contract import _require,_id,_digest,canonical_json_bytes,ContractError

POLICY_ID='campaign-review-operators'
STORAGE_VERSION='campaign-operator-policy-storage-v1'
MAX_HISTORY=64
MAX_BYTES=65536
HEAD_TABLE='campaign_review_operator_policy_heads'
HISTORY_TABLE='campaign_review_operator_policy_versions'
BASE=(('schema_version','STRING','REQUIRED'),('policy_id','STRING','REQUIRED'),
    ('revision','INTEGER','REQUIRED'),('status','STRING','REQUIRED'),('parent_sha256','STRING','NULLABLE'),
    ('policy_sha256','STRING','REQUIRED'),('record_sha256','STRING','REQUIRED'),
    ('changed_by','STRING','REQUIRED'),('reason_code','STRING','REQUIRED'),('recorded_at','TIMESTAMP','REQUIRED'))
SCHEMAS=MappingProxyType({HEAD_TABLE:BASE,HISTORY_TABLE:BASE+(('payload_canonical_json','STRING','REQUIRED'),)})


def table_metadata(name,project):
    _require(name in SCHEMAS,'INVALID_OPERATOR_POLICY_TABLE')
    table=bigquery.Table(f'{project}.ci_brand_memory.{name}',schema=[bigquery.SchemaField(n,k,mode=m) for n,k,m in SCHEMAS[name]])
    table.clustering_fields=['policy_id','revision'];table.description=STORAGE_VERSION+': explicit operator authority, no policy seeded'
    return table


@dataclass(frozen=True,slots=True)
class OperatorPolicyVersion:
    policy: ReviewOperatorPolicy
    parent_sha256: str | None
    status: str
    changed_by: str
    reason_code: str
    recorded_at: datetime

    def __post_init__(self):
        _require(type(self.policy) is ReviewOperatorPolicy and self.policy.policy_id==POLICY_ID
            and self.policy.revision<=MAX_HISTORY,'INVALID_OPERATOR_POLICY_SCOPE')
        _require(type(self.status) is str and self.status in {'ACTIVE','REVOKED'}
            and (self.policy.revision==1)==(self.parent_sha256 is None)
            and (self.policy.revision!=1 or self.status=='ACTIVE'),'INVALID_OPERATOR_POLICY_VERSION')
        if self.parent_sha256 is not None:_digest(self.parent_sha256)
        _require(type(self.changed_by) is str and re.fullmatch(r'iap-v1:[0-9a-f]{64}',self.changed_by) is not None,
            'INVALID_OPERATOR_POLICY_PUBLISHER')
        _id(self.reason_code);object.__setattr__(self,'recorded_at',_utc(self.recorded_at))

    @property
    def sha256(self):
        return _hash(dict(storage_version=STORAGE_VERSION,policy_sha256=self.policy.sha256,
            parent_sha256=self.parent_sha256,status=self.status,changed_by=self.changed_by,
            reason_code=self.reason_code,recorded_at=self.recorded_at.isoformat()))


def encode_record(record):
    _require(type(record) is OperatorPolicyVersion,'INVALID_OPERATOR_POLICY_RECORD')
    payload=canonical_json_bytes(_encode(record.policy)).decode()
    row=dict(schema_version=STORAGE_VERSION,policy_id=POLICY_ID,revision=record.policy.revision,
        status=record.status,parent_sha256=record.parent_sha256,policy_sha256=record.policy.sha256,
        record_sha256=record.sha256,changed_by=record.changed_by,reason_code=record.reason_code,
        recorded_at=record.recorded_at,payload_canonical_json=payload)
    _require(len(row_json(row).encode())<=MAX_BYTES,'OPERATOR_POLICY_RECORD_TOO_LARGE')
    return row


def head_row(row):return {k:v for k,v in row.items() if k!='payload_canonical_json'}


def decode_record(row):
    _require(type(row) is dict and set(row)=={n for n,_,_ in SCHEMAS[HISTORY_TABLE]},'INVALID_OPERATOR_POLICY_ROW')
    try:
        for name,kind,mode in SCHEMAS[HISTORY_TABLE]:
            value=row[name]
            if value is None and mode=='NULLABLE':continue
            if kind=='STRING':_require(type(value) is str,'INVALID_OPERATOR_POLICY_ROW_TYPE')
            elif kind=='INTEGER':_require(type(value) is int and 1<=value<=MAX_HISTORY,'INVALID_OPERATOR_POLICY_ROW_TYPE')
            else:_utc(value)
        text=row['payload_canonical_json']
        _require(len(text.encode())<=MAX_BYTES,'OPERATOR_POLICY_RECORD_TOO_LARGE')
        data=json.loads(text)
        _require(type(data) is dict and set(data)=={f.name for f in fields(ReviewOperatorPolicy)},'INVALID_OPERATOR_POLICY_PAYLOAD')
        _require(canonical_json_bytes(data).decode()==text,'NONCANONICAL_OPERATOR_POLICY_PAYLOAD')
        values=dict(data)
        for name in ('grant_operator_ids','reviewer_principal_ids','review_revoker_ids','kinds'):
            _require(type(values[name]) is list,'INVALID_OPERATOR_POLICY_PAYLOAD');values[name]=tuple(values[name])
        for name in ('effective_at','valid_until'):values[name]=_timestamp(values[name])
        record=OperatorPolicyVersion(ReviewOperatorPolicy(**values),row['parent_sha256'],row['status'],
            row['changed_by'],row['reason_code'],row['recorded_at'])
        _require(encode_record(record)==row,'OPERATOR_POLICY_HASH_OR_PROJECTION_CONFLICT')
        return record
    except (ValueError,TypeError,KeyError,RecursionError,OverflowError,UnicodeError) as error:
        if isinstance(error,ContractError):raise
        raise ContractError('INVALID_OPERATOR_POLICY_PAYLOAD') from None


@dataclass(frozen=True,slots=True)
class OperatorPolicyState:
    current: OperatorPolicyVersion
    history: tuple[OperatorPolicyVersion,...]

    def require_current(self,checked_at):
        now=_utc(checked_at);current=self.current
        _require(current.status=='ACTIVE' and current.recorded_at<=now
            and current.policy.effective_at<=now<current.policy.valid_until,'OPERATOR_POLICY_NOT_CURRENT')
        return current.policy


def decode_state(head,history,*,checked_at,minimum_revision=1,checkpoint_sha256=None):
    now=_utc(checked_at)
    _require(type(minimum_revision) is int and 1<=minimum_revision<=MAX_HISTORY,'INVALID_OPERATOR_POLICY_CHECKPOINT')
    if checkpoint_sha256 is not None:_digest(checkpoint_sha256)
    _require(type(history) is tuple and 1<=len(history)<=MAX_HISTORY and all(type(row) is dict for row in history),
        'INVALID_OPERATOR_POLICY_HISTORY')
    _require(type(head) is dict and set(head)=={n for n,_,_ in SCHEMAS[HEAD_TABLE]}
        and type(head.get('revision')) is int,'INVALID_OPERATOR_POLICY_HEAD')
    _require(len(row_json(head).encode())+sum(len(row_json(row).encode()) for row in history)<=MAX_BYTES,
        'OPERATOR_POLICY_CAPTURE_TOO_LARGE')
    records=tuple(decode_record(row) for row in history)
    _require(tuple(r.policy.revision for r in records)==tuple(range(1,len(records)+1))
        and head==head_row(history[-1]) and records[-1].policy.revision>=minimum_revision,
        'OPERATOR_POLICY_HEAD_OR_HISTORY_CONFLICT')
    for before,after in zip(records,records[1:]):
        _require(before.status=='ACTIVE' and after.parent_sha256==before.sha256
            and after.recorded_at>=before.recorded_at
            and after.policy.expected_audience==before.policy.expected_audience,
            'OPERATOR_POLICY_CHAIN_CONFLICT')
        if after.status=='REVOKED':
            a=_encode(after.policy);b=_encode(before.policy);a.pop('revision');b.pop('revision')
            _require(a==b,'OPERATOR_POLICY_REVOCATION_CONTENT_CHANGED')
    _require(all(r.recorded_at<=now for r in records),'OPERATOR_POLICY_FROM_FUTURE')
    if checkpoint_sha256 is not None:
        _require(records[minimum_revision-1].sha256==checkpoint_sha256,'OPERATOR_POLICY_CHECKPOINT_CONFLICT')
    return OperatorPolicyState(records[-1],records)
