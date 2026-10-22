"""Fresh fixed-ID operator-policy reads. No fallback, policy publication or activation.

VerifiedIdentity and checkpoint are server-only inputs. IAP JWT authentication,
retained revision floors and publisher provenance require trusted composition.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
from itertools import islice
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_operator_policy_storage import (
    POLICY_ID,HEAD_TABLE,HISTORY_TABLE,MAX_BYTES,MAX_HISTORY,decode_state,OperatorPolicyState,
)
from src.application.campaign_permission_reader import Checkpoint
from src.application.campaign_publication_reader import _identity,_native_row
from src.application.campaign_acceptance_contract import _utc
from src.application.campaign_review_storage import row_json
from src.application.workflow_contract import _require,_id,ContractError
from src.application.operation_budget import operation_call,operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.ingestion.setup_campaign_operator_policy import inspect
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION,settings

MAX_ROWS=MAX_HISTORY+2


@dataclass(frozen=True,slots=True)
class OperatorPolicySnapshot:
    state: OperatorPolicyState
    head_json: str
    history_json: tuple[str,...]
    native_job_id: str
    checked_at: datetime


def _checkpoint(checkpoint):
    _require(isinstance(checkpoint,Checkpoint) and checkpoint.record_id==POLICY_ID,'INVALID_OPERATOR_POLICY_READ_CHECKPOINT')


def query_spec():
    parts=[]
    for role,table in (('HEAD',HEAD_TABLE),('HISTORY',HISTORY_TABLE)):
        parts.append(f"SELECT '{role}' row_role,policy_id,revision,IF(BYTE_LENGTH(TO_JSON_STRING(t))<={MAX_BYTES},TO_JSON_STRING(t),NULL) record_json,COUNT(*) OVER() scoped_row_count FROM `{PROJECT}.{DATASET}.{table}` t WHERE policy_id=@policy_id")
    return ' UNION ALL '.join(parts)+f' ORDER BY row_role,revision LIMIT {MAX_ROWS}',settings([bigquery.ScalarQueryParameter('policy_id','STRING',POLICY_ID)])


def decode_snapshot(values,checkpoint,identity,*,expected_audience,native_job_id,checked_at):
    _checkpoint(checkpoint);now=_utc(checked_at);_identity(identity,expected_audience,now);_id(native_job_id)
    _require(type(values) is tuple and len(values)<MAX_ROWS,'OPERATOR_POLICY_READ_OVERFLOW')
    groups={'HEAD':[],'HISTORY':[]};counts={};size=0
    for supplied in values:
        row=dict(supplied)
        _require(set(row)=={'row_role','policy_id','revision','record_json','scoped_row_count'}
            and row['row_role'] in groups and row['policy_id']==POLICY_ID and type(row['revision']) is int,
            'OPERATOR_POLICY_READ_SCOPE')
        part=row['row_role'];count=row['scoped_row_count']
        _require(type(count) is int and 1<=count<=(1 if part=='HEAD' else MAX_HISTORY)
            and (part not in counts or counts[part]==count),'OPERATOR_POLICY_READ_COUNT_CONFLICT')
        counts[part]=count;data=_native_row(row['record_json'])
        _require(data.get('policy_id')==POLICY_ID and type(data.get('revision')) is int
            and data['revision']==row['revision'],'OPERATOR_POLICY_READ_PROJECTION_CONFLICT')
        size+=len(row_json(data).encode());_require(size<=MAX_BYTES,'OPERATOR_POLICY_CAPTURE_TOO_LARGE')
        groups[part].append(data)
    _require(len(groups['HEAD'])==1 and 1<=len(groups['HISTORY'])<=MAX_HISTORY
        and all(len(groups[part])==counts.get(part) for part in groups),'OPERATOR_POLICY_READ_MISSING_OR_PARTIAL')
    history=tuple(sorted(groups['HISTORY'],key=lambda row:row['revision']));head=groups['HEAD'][0]
    state=decode_state(head,history,checked_at=now,minimum_revision=checkpoint.minimum_revision,checkpoint_sha256=checkpoint.sha256)
    policy=state.require_current(now)
    _require(policy.expected_audience==expected_audience.expected_audience and identity.principal.principal_id in
        set(policy.grant_operator_ids+policy.reviewer_principal_ids+policy.review_revoker_ids),'OPERATOR_POLICY_READ_ACTOR_DENIED')
    # Returning a policy to one of its actors is not permission for every action;
    # transition validation still enforces the separate operator/reviewer roles.
    return OperatorPolicySnapshot(state,row_json(head),tuple(row_json(row) for row in history),native_job_id,now)


def read_policy(client,checkpoint,identity,*,expected_audience,clock=lambda:datetime.now(timezone.utc)):
    _checkpoint(checkpoint);_identity(identity,expected_audience,_utc(clock()))
    _require(inspect(client)['status']=='VERIFIED','OPERATOR_POLICY_RESOURCES_MISSING')
    _identity(identity,expected_audience,_utc(clock()))
    sql,cfg=query_spec();native='campaign_operator_policy_read_'+uuid4().hex
    job=operation_call(client.query,sql,job_config=cfg,job_id=native,project=PROJECT,location=LOCATION,retry=None,job_retry=None,timeout=20)
    values=tuple(dict(row) for row in islice(operation_iter(operation_call(job.result,timeout=60,retry=None,
        job_retry=None,max_results=MAX_ROWS,page_size=MAX_ROWS)),MAX_ROWS))
    original=operation_call(client.get_job,native,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(original,job_id=native,sql=sql,config=cfg)
        and original.state=='DONE' and original.error_result is None,'OPERATOR_POLICY_READ_NATIVE_MISMATCH')
    try:return decode_snapshot(values,checkpoint,identity,expected_audience=expected_audience,native_job_id=native,checked_at=clock())
    except ContractError as error:
        error.native_job_id=native;raise
