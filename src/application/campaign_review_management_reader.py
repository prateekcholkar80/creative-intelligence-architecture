"""Atomic reviewer-management captures and disabled plans; no publication route.

Identity/settings/checkpoints are trusted server inputs, not browser assertions.
One native statement captures operator policy, global fence and all action inputs.
JWT verification, retained checkpoint floors and controlled policy publication
remain responsibilities of future trusted composition.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
from itertools import islice
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_review_publication_contract import ReviewAction,structural_records,propose_review_publication
from src.application.campaign_review_publication_query import build_review_publication
from src.application.campaign_permission_publication import Rows,MAX_CAPTURE_BYTES
from src.application.campaign_permission_reader import Checkpoint
from src.application.campaign_operator_policy_storage import POLICY_ID,HEAD_TABLE,HISTORY_TABLE
from src.application.campaign_operator_policy_reader import OperatorPolicySnapshot,decode_snapshot as decode_policy
from src.application.campaign_publication_reader import _identity,_native_row
from src.application.campaign_review_storage import PREFIX,decode_control_state,row_json
from src.application.campaign_permission_storage import PREFIX as PERMISSION_PREFIX,decode_state as decode_permission,_review
from src.application.campaign_acceptance_contract import _utc,PolicyStatus
from src.application.identity import VerifiedIdentity
from src.application.workflow_contract import _require,_id,ContractError
from src.application.operation_budget import operation_call,operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.ingestion.setup_campaign_operator_policy import inspect as inspect_policy
from src.ingestion.setup_campaign_review_tables import inspect as inspect_reviews
from src.ingestion.setup_campaign_permission_tables import inspect as inspect_permissions
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION,settings

MAX_ROWS=5*65+1
GRANT_ACTIONS=frozenset((ReviewAction.CREATE_GRANT,ReviewAction.REVISE_GRANT,ReviewAction.REVOKE_GRANT))
CREATE_ACTIONS=frozenset((ReviewAction.CREATE_GRANT,ReviewAction.CREATE_REVIEW))


@dataclass(frozen=True,slots=True)
class ManagementReadRequest:
    action: ReviewAction
    target: Checkpoint
    grant: Checkpoint | None=None
    permission_kind: str | None=None
    permission: Checkpoint | None=None
    policy: Checkpoint=Checkpoint(POLICY_ID)
    control: Checkpoint=Checkpoint('campaign-permissions')

    def __post_init__(self):
        _require(isinstance(self.action,ReviewAction) and isinstance(self.target,Checkpoint)
            and isinstance(self.policy,Checkpoint) and self.policy.record_id==POLICY_ID
            and isinstance(self.control,Checkpoint) and self.control.record_id=='campaign-permissions',
            'INVALID_REVIEW_MANAGEMENT_REQUEST')
        if self.action in GRANT_ACTIONS:
            _require(self.grant is None and self.permission is None and self.permission_kind is None,
                'INVALID_REVIEW_MANAGEMENT_SOURCES')
        else:
            _require(isinstance(self.grant,Checkpoint),'INVALID_REVIEW_MANAGEMENT_SOURCES')
            if self.action is ReviewAction.CREATE_REVIEW:
                _require(type(self.permission_kind) is str and self.permission_kind in PERMISSION_PREFIX
                    and isinstance(self.permission,Checkpoint),'INVALID_REVIEW_MANAGEMENT_SOURCES')
            else:
                _require(self.permission_kind is None and self.permission is None,'INVALID_REVIEW_MANAGEMENT_SOURCES')

    @property
    def target_kind(self):return 'GRANT' if self.action in GRANT_ACTIONS else 'REVIEW'


@dataclass(frozen=True,slots=True)
class ManagementCapture:
    request: ManagementReadRequest
    identity: VerifiedIdentity
    policy_snapshot: OperatorPolicySnapshot
    control: Rows
    target: Rows
    grant: Rows | None
    permission: Rows | None
    native_job_id: str
    checked_at: datetime

    def plan(self,*,value=None,reason_code):
        return build_review_publication(self.request.action,identity=self.identity,
            policy=self.policy_snapshot.state.require_current(self.checked_at),policy_snapshot=self.policy_snapshot,
            control=self.control,target=self.target,grant=self.grant,permission=self.permission,value=value,
            checked_at=self.checked_at,reason_code=reason_code)


def query_spec(request):
    _require(isinstance(request,ManagementReadRequest),'INVALID_REVIEW_MANAGEMENT_REQUEST')
    selects=[];params=[]
    def append(label,key,value,head,history):
        param=label.lower()+'_id';params.append(bigquery.ScalarQueryParameter(param,'STRING',value))
        for role,table in (('HEAD',head),('HISTORY',history)):
            selects.append(f"SELECT '{label}' capture_kind,'{role}' row_role,{key} record_id,revision,TO_JSON_STRING(t) record_json FROM `{PROJECT}.{DATASET}.{table}` t WHERE {key}=@{param}")
    append('POLICY','policy_id',POLICY_ID,HEAD_TABLE,HISTORY_TABLE)
    append('CONTROL','control_id',request.control.record_id,'campaign_permission_controls','campaign_permission_control_history')
    prefix=PREFIX[request.target_kind]
    append('TARGET','record_id',request.target.record_id,prefix+'_heads',prefix+'_versions')
    if request.grant:
        prefix=PREFIX['GRANT'];append('GRANT','record_id',request.grant.record_id,prefix+'_heads',prefix+'_versions')
    if request.permission:
        prefix=PERMISSION_PREFIX[request.permission_kind]
        append('PERMISSION','record_id',request.permission.record_id,prefix+'_heads',prefix+'_versions')
    sql=f'''WITH scoped AS ({' UNION ALL '.join(selects)})
SELECT capture_kind,row_role,record_id,revision,
 IF(BYTE_LENGTH(record_json)<={MAX_CAPTURE_BYTES},record_json,NULL) record_json,
 COUNT(*) OVER(PARTITION BY capture_kind,record_id,row_role) scoped_row_count
FROM scoped ORDER BY capture_kind,record_id,row_role,revision LIMIT {MAX_ROWS}'''
    return sql,settings(params)


def decode_capture(values,request,identity,*,expected_audience,native_job_id,checked_at):
    _require(isinstance(request,ManagementReadRequest) and type(values) is tuple and len(values)<MAX_ROWS,
        'REVIEW_MANAGEMENT_READ_OVERFLOW')
    _id(native_job_id);now=_utc(checked_at);_identity(identity,expected_audience,now)
    allowed={'POLICY':request.policy,'CONTROL':request.control,'TARGET':request.target}
    if request.grant:allowed['GRANT']=request.grant
    if request.permission:allowed['PERMISSION']=request.permission
    groups={};policy_values=[];size=0
    for supplied in values:
        row=dict(supplied)
        _require(set(row)=={'capture_kind','row_role','record_id','revision','record_json','scoped_row_count'},
            'REVIEW_MANAGEMENT_READ_SHAPE')
        label=row['capture_kind'];part=row['row_role'];count=row['scoped_row_count']
        _require(type(label) is str and label in allowed and row['record_id']==allowed[label].record_id
            and type(part) is str and part in {'HEAD','HISTORY'} and type(row['revision']) is int
            and type(count) is int and 1<=count<=(1 if part=='HEAD' else 64),'REVIEW_MANAGEMENT_READ_SCOPE')
        data=_native_row(row['record_json']);key='policy_id' if label=='POLICY' else 'control_id' if label=='CONTROL' else 'record_id'
        _require(data.get(key)==row['record_id'] and type(data.get('revision')) is int and data['revision']==row['revision'],
            'REVIEW_MANAGEMENT_READ_PROJECTION_CONFLICT')
        size+=len(row_json(data).encode());_require(size<=MAX_CAPTURE_BYTES,'PERMISSION_PUBLICATION_CAPTURE_TOO_LARGE')
        group=groups.setdefault(label,{'HEAD':[],'HISTORY':[],'COUNTS':{}})
        _require(part not in group['COUNTS'] or group['COUNTS'][part]==count,'REVIEW_MANAGEMENT_READ_COUNT_CONFLICT')
        group['COUNTS'][part]=count;group[part].append(data)
        if label=='POLICY':
            policy_values.append(dict(row_role=part,policy_id=row['record_id'],revision=row['revision'],
                record_json=row['record_json'],scoped_row_count=count))
    for group in groups.values():
        _require(len(group['HEAD'])==1 and 1<=len(group['HISTORY'])<=64
            and all(len(group[p])==group['COUNTS'].get(p) for p in ('HEAD','HISTORY')),'REVIEW_MANAGEMENT_READ_PARTIAL')
    policy_snapshot=decode_policy(tuple(policy_values),request.policy,identity,expected_audience=expected_audience,
        native_job_id=native_job_id,checked_at=now)
    policy=policy_snapshot.state.require_current(now);actor=identity.principal.principal_id
    groups.pop('POLICY')
    def get(label,kind,checkpoint,*,absent=False):
        group=groups.pop(label,None)
        if group is None:
            _require(absent and checkpoint.minimum_revision==1 and checkpoint.sha256 is None,'REVIEW_MANAGEMENT_READ_MISSING')
            return Rows(kind,checkpoint.record_id,None,()),None
        history=tuple(sorted(group['HISTORY'],key=lambda r:r['revision']));head=group['HEAD'][0]
        source=Rows(kind,checkpoint.record_id,row_json(head),tuple(row_json(row) for row in history))
        if kind=='CONTROL':
            current=decode_control_state(head,history,now);revision=current.revision
            pin=lambda n:decode_control_state(history[n-1],history[:n],now).sha256
        elif kind in PREFIX:
            records=structural_records(source,now);current=records[-1];revision=current.revision
            pin=lambda n:records[n-1].sha256
        else:
            current=decode_permission(head,history,kind=kind,record_id=checkpoint.record_id,
                minimum_revision=checkpoint.minimum_revision,checkpoint_sha256=checkpoint.sha256)
            _require(current.recorded_at<=now,'PERMISSION_RECORD_FROM_FUTURE')
            revision=_review(current.current,kind).revision;pin=None
        _require(revision>=checkpoint.minimum_revision,'REVIEW_MANAGEMENT_CHECKPOINT_STALE')
        if pin and checkpoint.sha256 is not None:
            _require(pin(checkpoint.minimum_revision)==checkpoint.sha256,'REVIEW_MANAGEMENT_CHECKPOINT_CONFLICT')
        return source,current
    control,_=get('CONTROL','CONTROL',request.control)
    target,current=get('TARGET',request.target_kind,request.target,absent=request.action in CREATE_ACTIONS)
    _require((current is None)==(request.action in CREATE_ACTIONS)
        and (current is None or current.status=='ACTIVE' and current.revision<64),'REVIEW_PUBLICATION_TERMINAL_OR_LIMIT')
    grant=None;permission=None
    if request.action in GRANT_ACTIONS:
        _require(actor in policy.grant_operator_ids,'REVIEW_GRANT_OPERATOR_DENIED')
        if current:_require(current.value.expected_iap_audience==policy.expected_audience,'REVIEW_GRANT_SCOPE_DENIED')
    else:
        grant,g=get('GRANT','GRANT',request.grant)
        _require(g.value.expected_iap_audience==policy.expected_audience,'REVIEW_SOURCE_GRANT_SCOPE')
        if request.action is ReviewAction.CREATE_REVIEW:
            _require(actor in policy.reviewer_principal_ids and actor in g.value.principal_ids and g.status=='ACTIVE'
                and g.value.effective_at<=now<g.value.valid_until and request.permission_kind in g.value.kinds
                and request.permission_kind in policy.kinds,'REVIEW_CAPTURE_GRANT_NOT_CURRENT')
            permission,p=get('PERMISSION',request.permission_kind,request.permission)
            approval=_review(p.current,request.permission_kind)
            _require(approval.status is PolicyStatus.DRAFT and now<approval.valid_until,'REVIEW_CAPTURE_CONTENT_CONFLICT')
        else:
            # Reuse the transition checks for owner/revoker rights and historical
            # source-grant binding; expired/revoked source grants can be cleaned up.
            propose_review_publication(request.action,identity=identity,policy=policy,target=target,grant=grant,
                checked_at=now,reason_code='VALIDATE_MANAGEMENT_CAPTURE')
    _require(not groups,'REVIEW_MANAGEMENT_READ_EXTRA_SCOPE')
    return ManagementCapture(request,identity,policy_snapshot,control,target,grant,permission,native_job_id,now)


def read_capture(client,request,identity,*,expected_audience,clock=lambda:datetime.now(timezone.utc)):
    _require(isinstance(request,ManagementReadRequest),'INVALID_REVIEW_MANAGEMENT_REQUEST')
    _identity(identity,expected_audience,_utc(clock()))
    _require(all(check(client)['status']=='VERIFIED'
        for check in (inspect_policy,inspect_reviews,inspect_permissions)),'REVIEW_MANAGEMENT_RESOURCES_MISSING')
    _identity(identity,expected_audience,_utc(clock()))
    sql,cfg=query_spec(request);native='campaign_review_management_read_'+uuid4().hex
    job=operation_call(client.query,sql,job_config=cfg,job_id=native,project=PROJECT,location=LOCATION,
        retry=None,job_retry=None,timeout=20)
    values=tuple(dict(row) for row in islice(operation_iter(operation_call(job.result,timeout=60,retry=None,
        job_retry=None,max_results=MAX_ROWS,page_size=MAX_ROWS)),MAX_ROWS))
    original=operation_call(client.get_job,native,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(original,job_id=native,sql=sql,config=cfg)
        and original.state=='DONE' and original.error_result is None,'REVIEW_MANAGEMENT_NATIVE_MISMATCH')
    try:return decode_capture(values,request,identity,expected_audience=expected_audience,native_job_id=native,checked_at=clock())
    except ContractError as error:
        error.native_job_id=native;raise
