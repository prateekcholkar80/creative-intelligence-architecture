"""P6 proposed reviewer/review/control/audit schemas and pure codecs; no I/O."""
from dataclasses import dataclass,fields
from datetime import datetime
from types import MappingProxyType
import json

from google.cloud import bigquery
from src.application.campaign_permission_lifecycle import ReviewerGrant,ReviewCapture
from src.application.campaign_permission_storage import _encode,_timestamp,MAX_HISTORY
from src.application.campaign_acceptance_contract import _utc,_hash
from src.application.workflow_contract import _require,_id,_digest,canonical_json_bytes,ContractError

STORAGE_VERSION='campaign-review-storage-v1'
BASE=(('schema_version','STRING','REQUIRED'),('record_kind','STRING','REQUIRED'),
 ('record_id','STRING','REQUIRED'),('revision','INTEGER','REQUIRED'),('status','STRING','REQUIRED'),
 ('parent_sha256','STRING','NULLABLE'),('record_sha256','STRING','REQUIRED'),
 ('payload_sha256','STRING','REQUIRED'),('changed_by','STRING','REQUIRED'),('recorded_at','TIMESTAMP','REQUIRED'))
CONTROL=(('schema_version','STRING','REQUIRED'),('control_id','STRING','REQUIRED'),
 ('revision','INTEGER','REQUIRED'),('parent_sha256','STRING','NULLABLE'),('control_sha256','STRING','REQUIRED'),
 ('command_sha256','STRING','REQUIRED'),('changed_by','STRING','REQUIRED'),('recorded_at','TIMESTAMP','REQUIRED'))
AUDIT=(('schema_version','STRING','REQUIRED'),('command_sha256','STRING','REQUIRED'),
 ('action','STRING','REQUIRED'),('permission_kind','STRING','REQUIRED'),('record_id','STRING','REQUIRED'),
 ('previous_revision','INTEGER','REQUIRED'),('revision','INTEGER','REQUIRED'),
 ('previous_record_sha256','STRING','NULLABLE'),('record_sha256','STRING','REQUIRED'),
 ('control_revision','INTEGER','REQUIRED'),('control_sha256','STRING','REQUIRED'),
 ('grant_id','STRING','REQUIRED'),('grant_sha256','STRING','REQUIRED'),
 ('review_id','STRING','NULLABLE'),('review_sha256','STRING','NULLABLE'),
 ('changed_by','STRING','REQUIRED'),('reason_code','STRING','REQUIRED'),
 ('recorded_at','TIMESTAMP','REQUIRED'),('outcome','STRING','REQUIRED'))
SCHEMAS=MappingProxyType({**{f'campaign_{kind}_'+suffix:BASE+((('payload_canonical_json','STRING','REQUIRED'),) if suffix=='versions' else ())
    for kind in ('reviewer_grants','review_captures') for suffix in ('versions','heads')},
    'campaign_permission_controls':CONTROL,'campaign_permission_control_history':CONTROL,
    'campaign_permission_publication_audits':AUDIT})
PREFIX={'GRANT':'campaign_reviewer_grants','REVIEW':'campaign_review_captures'}


def table_metadata(name,project):
    _require(name in SCHEMAS,'INVALID_CAMPAIGN_REVIEW_TABLE')
    t=bigquery.Table(f'{project}.ci_brand_memory.{name}',schema=[bigquery.SchemaField(n,k,mode=m) for n,k,m in SCHEMAS[name]])
    t.clustering_fields=['record_id','revision'] if 'record_id' in {f.name for f in t.schema} else ['control_id','revision']
    t.description=f'{STORAGE_VERSION}: proposed controlled review/publication; no bootstrap or approval'
    return t


@dataclass(frozen=True,slots=True)
class ReviewVersion:
    value: ReviewerGrant | ReviewCapture
    revision: int
    parent_sha256: str | None
    status: str
    changed_by: str
    recorded_at: datetime

    def __post_init__(self):
        _require(type(self.value) in (ReviewerGrant,ReviewCapture) and type(self.revision) is int
            and 1<=self.revision<=MAX_HISTORY and type(self.status) is str
            and self.status in {'ACTIVE','REVOKED'},'INVALID_CAMPAIGN_REVIEW_VERSION')
        _require((self.revision==1)==(self.parent_sha256 is None),'INVALID_CAMPAIGN_REVIEW_PARENT')
        if self.parent_sha256 is not None:_digest(self.parent_sha256)
        _id(self.changed_by);object.__setattr__(self,'recorded_at',_utc(self.recorded_at))

    @property
    def kind(self):return 'GRANT' if type(self.value) is ReviewerGrant else 'REVIEW'
    @property
    def record_id(self):return self.value.grant_id if self.kind=='GRANT' else self.value.review_id
    @property
    def sha256(self):
        return _hash(dict(storage_version=STORAGE_VERSION,kind=self.kind,record_id=self.record_id,
            revision=self.revision,parent_sha256=self.parent_sha256,status=self.status,
            value_sha256=self.value.sha256,changed_by=self.changed_by,recorded_at=self.recorded_at.isoformat()))


def encode_review(record):
    _require(isinstance(record,ReviewVersion),'INVALID_CAMPAIGN_REVIEW_VERSION')
    payload=canonical_json_bytes(_encode(record.value))
    return dict(schema_version=STORAGE_VERSION,record_kind=record.kind,record_id=record.record_id,
        revision=record.revision,status=record.status,parent_sha256=record.parent_sha256,
        record_sha256=record.sha256,payload_sha256=_hash(_encode(record.value)),
        changed_by=record.changed_by,recorded_at=record.recorded_at,payload_canonical_json=payload.decode())


def review_head(row):return {k:v for k,v in row.items() if k!='payload_canonical_json'}


def row_json(row):
    return canonical_json_bytes({k:(_utc(v).isoformat() if isinstance(v,datetime) else v) for k,v in row.items()}).decode()


def parse_row(text):
    _require(type(text) is str,'INVALID_CAMPAIGN_REVIEW_ROW_JSON')
    try:
        d=json.loads(text)
        _require(type(d) is dict and canonical_json_bytes(d).decode()==text,'INVALID_CAMPAIGN_REVIEW_ROW_JSON')
        d['recorded_at']=_timestamp(d['recorded_at']);return d
    except (ValueError,TypeError,KeyError,RecursionError) as e:
        if isinstance(e,ContractError):raise
        raise ContractError('INVALID_CAMPAIGN_REVIEW_ROW_JSON') from None


def decode_review(row,kind):
    _require(kind in PREFIX and type(row) is dict
        and set(row)=={n for n,_,_ in SCHEMAS[PREFIX[kind]+'_versions']},'INVALID_CAMPAIGN_REVIEW_ROW')
    try:
        payload=json.loads(row['payload_canonical_json']);cls=ReviewerGrant if kind=='GRANT' else ReviewCapture
        _require(type(payload) is dict and set(payload)=={f.name for f in fields(cls)},'INVALID_CAMPAIGN_REVIEW_PAYLOAD')
        data=dict(payload)
        if kind=='GRANT':
            for name in ('principal_ids','kinds'):
                _require(type(data[name]) is list,'INVALID_CAMPAIGN_REVIEW_PAYLOAD');data[name]=tuple(data[name])
            data['effective_at']=_timestamp(data['effective_at'])
        else:data['reviewed_at']=_timestamp(data['reviewed_at'])
        data['valid_until']=_timestamp(data['valid_until'])
        record=ReviewVersion(cls(**data),row['revision'],row['parent_sha256'],row['status'],row['changed_by'],row['recorded_at'])
        _require(record.kind==kind and encode_review(record)==row,'CAMPAIGN_REVIEW_ROW_MISMATCH')
        return record
    except (ValueError,TypeError,KeyError,RecursionError) as e:
        if isinstance(e,ContractError):raise
        raise ContractError('INVALID_CAMPAIGN_REVIEW_PAYLOAD') from None


def decode_review_state(head,history,*,kind,record_id,checked_at):
    _require(kind in PREFIX and type(history) is tuple and 1<=len(history)<=MAX_HISTORY,
             'INVALID_CAMPAIGN_REVIEW_HISTORY')
    _require(type(head) is dict and type(head.get('revision')) is int
        and head==review_head(history[-1]),'CAMPAIGN_REVIEW_HEAD_MISMATCH')
    records=tuple(decode_review(row,kind) for row in history)
    _require(tuple(r.revision for r in records)==tuple(range(1,len(records)+1))
        and all(r.record_id==record_id for r in records),'CAMPAIGN_REVIEW_HISTORY_GAP')
    for before,after in zip(records,records[1:]):
        _require(after.parent_sha256==before.sha256 and after.recorded_at>=before.recorded_at
            and before.status!='REVOKED','CAMPAIGN_REVIEW_CHAIN_CONFLICT')
        if kind=='REVIEW':_require(after.value==before.value,'CAMPAIGN_REVIEW_CAPTURE_IMMUTABLE')
    current=records[-1];now=_utc(checked_at)
    start=current.value.effective_at if kind=='GRANT' else current.value.reviewed_at
    _require(current.status=='ACTIVE' and current.recorded_at<=now
        and start<=now<current.value.valid_until,'CAMPAIGN_REVIEW_NOT_CURRENT')
    return current


@dataclass(frozen=True,slots=True)
class PublicationControl:
    revision: int
    parent_sha256: str | None
    command_sha256: str
    changed_by: str
    recorded_at: datetime

    def __post_init__(self):
        _require(type(self.revision) is int and 1<=self.revision<=1000000,'INVALID_PERMISSION_PUBLICATION_CONTROL')
        _require((self.revision==1)==(self.parent_sha256 is None),'INVALID_PERMISSION_PUBLICATION_CONTROL')
        if self.parent_sha256 is not None:_digest(self.parent_sha256)
        _digest(self.command_sha256);_id(self.changed_by)
        object.__setattr__(self,'recorded_at',_utc(self.recorded_at))

    @property
    def sha256(self):
        return _hash(dict(storage_version=STORAGE_VERSION,control_id='campaign-permissions',revision=self.revision,
            parent_sha256=self.parent_sha256,command_sha256=self.command_sha256,
            changed_by=self.changed_by,recorded_at=self.recorded_at.isoformat()))

    def row(self):
        return dict(schema_version=STORAGE_VERSION,control_id='campaign-permissions',revision=self.revision,
            parent_sha256=self.parent_sha256,control_sha256=self.sha256,command_sha256=self.command_sha256,
            changed_by=self.changed_by,recorded_at=self.recorded_at)


def decode_control_state(head,history,checked_at):
    _require(type(history) is tuple and 1<=len(history)<=64,'INVALID_PERMISSION_CONTROL_HISTORY')
    previous=None
    for i,row in enumerate(history,1):
        _require(type(row) is dict and type(row.get('revision')) is int,'INVALID_PERMISSION_CONTROL_ROW')
        item=PublicationControl(row['revision'],row['parent_sha256'],row['command_sha256'],row['changed_by'],row['recorded_at'])
        _require(item.row()==row and item.revision==i and (previous is None or
            (item.parent_sha256==previous.sha256 and item.recorded_at>=previous.recorded_at)),
            'PERMISSION_CONTROL_CHAIN_CONFLICT')
        previous=item
    _require(type(head) is dict and type(head.get('revision')) is int
        and head==history[-1] and previous.recorded_at<=_utc(checked_at),
             'PERMISSION_CONTROL_HEAD_MISMATCH')
    return previous
