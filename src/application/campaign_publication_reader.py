"""Fresh atomic server-side publication captures; no local fallback or writes.

Typed VerifiedIdentity is a trusted verifier result, not browser input. This
module does not authenticate JWTs, bootstrap controls or grant reviewer access.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
from itertools import islice
import json
from uuid import uuid4

from google.cloud import bigquery
from src.application.identity import VerifiedIdentity,IapSettings
from src.application.campaign_permission_lifecycle import Action
from src.application.campaign_permission_reader import Checkpoint
from src.application.campaign_permission_publication import Rows,build_publication,MAX_CAPTURE_BYTES
from src.application.campaign_review_storage import (
    PREFIX as REVIEW_PREFIX,decode_review,decode_review_state,decode_control_state,row_json,
)
from src.application.campaign_permission_storage import PREFIX,decode_state,_timestamp
from src.application.campaign_acceptance_contract import _utc
from src.application.workflow_contract import ContractError,_require,_id,canonical_json_bytes
from src.ingestion.setup_campaign_review_tables import inspect as inspect_reviews
from src.ingestion.setup_campaign_permission_tables import inspect as inspect_permissions
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION,settings

MAX_GROUPS=36
MAX_ROWS=MAX_GROUPS*65+1


@dataclass(frozen=True,slots=True)
class PublicationReadRequest:
    action: Action
    kind: str
    record_id: str
    grant: Checkpoint
    review: Checkpoint | None=None
    permission: Checkpoint | None=None
    evidence: tuple[Checkpoint,...]=()
    control: Checkpoint=Checkpoint('campaign-permissions')

    def __post_init__(self):
        _require(isinstance(self.action,Action) and type(self.kind) is str and self.kind in PREFIX,
                 'INVALID_PUBLICATION_READ_REQUEST')
        _id(self.record_id)
        _require(isinstance(self.grant,Checkpoint) and isinstance(self.control,Checkpoint)
            and self.control.record_id=='campaign-permissions'
            and (self.review is None or isinstance(self.review,Checkpoint))
            and (self.action is Action.APPROVE)==(self.review is not None)
            and (self.permission is None or isinstance(self.permission,Checkpoint)
                 and self.permission.record_id==self.record_id), 'INVALID_PUBLICATION_READ_CHECKPOINT')
        _require(type(self.evidence) is tuple and len(self.evidence)<=32
            and all(isinstance(c,Checkpoint) for c in self.evidence)
            and tuple(c.record_id for c in self.evidence)==tuple(sorted({c.record_id for c in self.evidence}))
            and (self.action is Action.APPROVE and self.kind=='CLAIM' or not self.evidence),
            'INVALID_PUBLICATION_READ_EVIDENCE')


@dataclass(frozen=True,slots=True)
class PublicationCapture:
    request: PublicationReadRequest
    identity: VerifiedIdentity
    control: Rows
    grant: Rows
    permission: Rows
    review: Rows | None
    evidence: tuple[Rows,...]
    native_job_id: str
    checked_at: datetime

    def plan(self,*,candidate=None,reason_code):
        return build_publication(self.request.action,identity=self.identity,control=self.control,
            grant=self.grant,permission=self.permission,candidate=candidate,review=self.review,
            evidence=self.evidence,checked_at=self.checked_at,reason_code=reason_code)


def _identity(identity,expected_audience,now):
    _require(isinstance(identity,VerifiedIdentity) and isinstance(expected_audience,IapSettings),
             'PUBLICATION_READER_IDENTITY_REQUIRED')
    _require(identity.audience==expected_audience.expected_audience
        and identity.issued_at<=now.timestamp()<identity.expires_at,'PUBLICATION_READER_IDENTITY_DENIED')


def query_spec(request):
    _require(isinstance(request,PublicationReadRequest),'INVALID_PUBLICATION_READ_REQUEST')
    selects=[]
    def append(kind,prefix,key,condition):
        for part,suffix in [('HEAD','heads'),('HISTORY','versions')]:
            table=f'{prefix}_{suffix}'
            if kind=='CONTROL':table='campaign_permission_controls' if part=='HEAD' else 'campaign_permission_control_history'
            selects.append(f"SELECT '{kind}' capture_kind,'{part}' row_role,{key} record_id,revision,TO_JSON_STRING(t) record_json FROM `{PROJECT}.{DATASET}.{table}` t WHERE {condition}")
    append('CONTROL','', 'control_id','control_id=@control_id')
    append('GRANT',REVIEW_PREFIX['GRANT'],'record_id','record_id=@grant_id')
    append('REVIEW',REVIEW_PREFIX['REVIEW'],'record_id','record_id=@review_id' if request.review else 'FALSE')
    append(request.kind,PREFIX[request.kind],'record_id','record_id=@record_id')
    # TARGET evidence and independent evidence use different capture labels to
    # avoid ambiguity when approving an EVIDENCE record itself.
    if request.action is Action.APPROVE and request.kind=='CLAIM':
        append('CLAIM_EVIDENCE',PREFIX['EVIDENCE'],'record_id','record_id IN UNNEST(JSON_VALUE_ARRAY(@evidence_ids_json))')
    sql=f'''WITH scoped AS ({' UNION ALL '.join(selects)})
SELECT capture_kind,row_role,record_id,revision,
 IF(BYTE_LENGTH(record_json)<={MAX_CAPTURE_BYTES},record_json,NULL) record_json,
 COUNT(*) OVER(PARTITION BY capture_kind,record_id,row_role) scoped_row_count
FROM scoped ORDER BY capture_kind,record_id,row_role,revision LIMIT {MAX_ROWS}'''
    parameters=[bigquery.ScalarQueryParameter('control_id','STRING','campaign-permissions'),
        bigquery.ScalarQueryParameter('grant_id','STRING',request.grant.record_id),
        bigquery.ScalarQueryParameter('record_id','STRING',request.record_id)]
    if request.review:parameters.append(bigquery.ScalarQueryParameter('review_id','STRING',request.review.record_id))
    if request.action is Action.APPROVE and request.kind=='CLAIM':
        parameters.append(bigquery.ScalarQueryParameter('evidence_ids_json','STRING',
            canonical_json_bytes([c.record_id for c in request.evidence]).decode()))
    return sql,settings(parameters)


def _native_row(value):
    _require(type(value) is str and len(value.encode('utf-8'))<=MAX_CAPTURE_BYTES,
             'PUBLICATION_READ_ROW_TOO_LARGE')
    try:
        data=json.loads(value)
        _require(type(data) is dict,'INVALID_PUBLICATION_READ_JSON')
        data['recorded_at']=_timestamp(data['recorded_at'])
        return data
    except (ValueError,TypeError,KeyError,RecursionError) as error:
        if isinstance(error,ContractError):raise
        raise ContractError('INVALID_PUBLICATION_READ_JSON') from None


def decode_capture(values,request,identity,*,expected_audience,native_job_id,checked_at):
    _require(isinstance(request,PublicationReadRequest) and type(values) is tuple and len(values)<MAX_ROWS,
             'PUBLICATION_READ_OVERFLOW')
    _id(native_job_id);now=_utc(checked_at);_identity(identity,expected_audience,now)
    groups={};total_bytes=0
    allowed={('CONTROL','campaign-permissions'),('GRANT',request.grant.record_id),(request.kind,request.record_id)}
    if request.review:allowed.add(('REVIEW',request.review.record_id))
    allowed.update(('CLAIM_EVIDENCE',c.record_id) for c in request.evidence)
    for supplied in values:
        row=dict(supplied)
        _require(set(row)=={'capture_kind','row_role','record_id','revision','record_json','scoped_row_count'},
                 'INVALID_PUBLICATION_READ_SHAPE')
        key=(row['capture_kind'],row['record_id']);part=row['row_role'];count=row['scoped_row_count']
        _require(key in allowed and part in {'HEAD','HISTORY'} and type(count) is int
            and 1<=count<=(1 if part=='HEAD' else 64) and type(row['revision']) is int,
            'PUBLICATION_READ_SCOPE_OR_DUPLICATE')
        data=_native_row(row['record_json'])
        key_field='control_id' if key[0]=='CONTROL' else 'record_id'
        _require(data.get(key_field)==key[1] and type(data.get('revision')) is int
            and data.get('revision')==row['revision'],
                 'PUBLICATION_READ_PROJECTION_MISMATCH')
        wire=row_json(data);total_bytes+=len(wire.encode())
        _require(total_bytes<=MAX_CAPTURE_BYTES,'PERMISSION_PUBLICATION_CAPTURE_TOO_LARGE')
        group=groups.setdefault(key,{'HEAD':[],'HISTORY':[],'COUNTS':{}})
        _require(part not in group['COUNTS'] or group['COUNTS'][part]==count,'PUBLICATION_READ_COUNT_CONFLICT')
        group['COUNTS'][part]=count;group[part].append(data)
    for group in groups.values():
        _require(len(group['HEAD'])==1 and 1<=len(group['HISTORY'])<=64
            and all(len(group[p])==group['COUNTS'].get(p) for p in ('HEAD','HISTORY')),
            'PUBLICATION_READ_PARTIAL_STATE')
    def get(kind,checkpoint,*,absent=False,stored_kind=None):
        group=groups.pop((kind,checkpoint.record_id),None)
        if group is None:
            _require(absent and checkpoint.minimum_revision==1 and checkpoint.sha256 is None,'PUBLICATION_READ_MISSING')
            return Rows(kind,checkpoint.record_id,None,()),None
        history=tuple(sorted(group['HISTORY'],key=lambda r:r['revision']));head=group['HEAD'][0]
        if kind=='CONTROL':
            current=decode_control_state(head,history,now);sha=current.sha256;revision=current.revision
        elif kind in REVIEW_PREFIX:
            current=decode_review_state(head,history,kind=kind,record_id=checkpoint.record_id,checked_at=now)
            sha=current.sha256;revision=current.revision
        else:
            current=decode_state(head,history,kind=stored_kind or kind,record_id=checkpoint.record_id,
                minimum_revision=checkpoint.minimum_revision,checkpoint_sha256=checkpoint.sha256)
            # DRAFT/expired current targets may be replaced/revoked, but evidence
            # support must independently be currently APPROVED.
            if kind=='CLAIM_EVIDENCE':current.require_current(now)
            sha=current.current.sha256
            review=current.current.review if (stored_kind or kind)=='EVIDENCE' else current.current.approval
            revision=review.revision
            _require(current.recorded_at<=now,'PERMISSION_RECORD_FROM_FUTURE')
        _require(revision>=checkpoint.minimum_revision,'PUBLICATION_READ_CHECKPOINT_STALE')
        if checkpoint.sha256 is not None:
            # Review/control floors bind that exact historical revision, not an
            # arbitrarily newer head. Permission decode already enforces this.
            if kind in REVIEW_PREFIX:
                pinned=decode_review(history[checkpoint.minimum_revision-1],kind)
                _require(pinned.sha256==checkpoint.sha256,'PUBLICATION_READ_CHECKPOINT_CONFLICT')
            elif kind=='CONTROL':
                prefix_history=history[:checkpoint.minimum_revision]
                pinned=decode_control_state(prefix_history[-1],prefix_history,now)
                _require(pinned.sha256==checkpoint.sha256,'PUBLICATION_READ_CHECKPOINT_CONFLICT')
        return Rows(stored_kind or kind,checkpoint.record_id,row_json(head),tuple(row_json(r) for r in history)),current
    # Explicit reviewer authorization before returning any permission captures.
    g_rows,g=get('GRANT',request.grant)
    _require(g.value.expected_iap_audience==expected_audience.expected_audience
        and identity.principal.principal_id in g.value.principal_ids and request.kind in g.value.kinds,
        'PUBLICATION_REVIEWER_DENIED')
    control,_=get('CONTROL',request.control)
    review=None
    if request.review:
        review,r=get('REVIEW',request.review)
        _require(r.value.grant_sha256==g.value.sha256 and r.value.principal_id==identity.principal.principal_id
            and r.value.kind==request.kind and r.value.record_id==request.record_id,'PUBLICATION_REVIEW_SCOPE_DENIED')
    p_rows,p=get(request.kind,request.permission or Checkpoint(request.record_id),absent=request.action is Action.CREATE_DRAFT)
    _require((p is None)==(request.action is Action.CREATE_DRAFT),'PUBLICATION_CREATE_OR_CURRENT_CONFLICT')
    evidence=tuple(get('CLAIM_EVIDENCE',c,stored_kind='EVIDENCE')[0] for c in request.evidence)
    _require(not groups,'PUBLICATION_READ_EXTRA_SCOPE')
    return PublicationCapture(request,identity,control,g_rows,p_rows,review,evidence,native_job_id,now)


def read_capture(client,request,identity,*,expected_audience,clock=lambda:datetime.now(timezone.utc)):
    _require(isinstance(request,PublicationReadRequest),'INVALID_PUBLICATION_READ_REQUEST')
    _identity(identity,expected_audience,_utc(clock()))
    _require(client.project==PROJECT,'PUBLICATION_READ_PROJECT_MISMATCH')
    _require(inspect_reviews(client)['status']=='VERIFIED' and inspect_permissions(client)['status']=='VERIFIED',
             'PUBLICATION_READ_TABLES_MISSING')
    sql,cfg=query_spec(request);native='campaign_publication_read_'+uuid4().hex
    job=client.query(sql,job_config=cfg,job_id=native,location=LOCATION,retry=None,job_retry=None,timeout=20)
    values=tuple(dict(r) for r in islice(job.result(timeout=60,max_results=MAX_ROWS,page_size=100),MAX_ROWS))
    original=client.get_job(native,location=LOCATION,retry=None,timeout=20);config=original.to_api_repr().get('configuration',{})
    _require(original.project==PROJECT and (original.location or '').lower()==LOCATION and original.job_id==native
        and original.query==sql and original.state=='DONE' and original.error_result is None
        and config.get('jobTimeoutMs')=='60000'
        and all(config.get('query',{}).get(k)==v for k,v in cfg.to_api_repr()['query'].items()),
        'PUBLICATION_READ_NATIVE_JOB_MISMATCH')
    try:return decode_capture(values,request,identity,expected_audience=expected_audience,native_job_id=native,checked_at=clock())
    except ContractError as error:
        error.native_job_id=native;raise
