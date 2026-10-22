"""Bounded native-correlated question context, including pending child rows."""
from datetime import datetime,timedelta,timezone
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_question_context import NAMES,SPECS,QuestionContextRead,canonical_records
from src.application.campaign_quality_product_reader import _access
from src.application.identity import VerifiedIdentity,ensure_fresh
from src.application.control_io import ControlIO
from src.application.workflow_contract import _require,_id
from src.ingestion.setup_workflow_tables import DATASET_ID


def sql():
    fields=['CURRENT_TIMESTAMP() observed_at','@read_id read_id']
    for name in sorted(NAMES):
        where='workspace_id=@workspace_id'
        if name=='memberships':where+=' AND principal_id=@principal_id'
        elif name!='workspaces':where+=' AND conversation_id=@conversation_id'
        partition=None if name=='campaign_request_authorities' else SPECS[name].partition_field
        if partition:where+=f" AND {partition} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')"
        if name in {'campaign_transcript_bodies','campaign_transcript_responses'}:
            where+=' AND recorded_at>=TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 30 DAY)'
        fields.extend([f"ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=131072,TO_JSON_STRING(t),NULL) FROM `{DATASET_ID}.{name}` t WHERE {where} LIMIT 257) {name}",
            f"(SELECT COUNT(*) FROM `{DATASET_ID}.{name}` WHERE {where}) {name}_count"])
    return 'SELECT '+',\n'.join(fields)


def read_question_context(client,*,identity,access_reader,workspace_id,conversation_id,
        clock=lambda:datetime.now(timezone.utc)):
    _require(type(identity) is VerifiedIdentity,'QUESTION_CONTEXT_IDENTITY_REQUIRED')
    for key in (workspace_id,conversation_id):_id(key)
    ensure_fresh(identity);started=clock()
    before=_access(access_reader,identity,workspace_id,conversation_id)
    read_id='question-context-'+uuid4().hex
    params=[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in dict(workspace_id=workspace_id,
        conversation_id=conversation_id,principal_id=identity.principal.principal_id,read_id=read_id).items()]
    row=ControlIO(client).read(sql(),params)
    ended=clock();ensure_fresh(identity)
    _require(row.get('read_id')==read_id and started<=row['observed_at']<=ended
        and ended<started+timedelta(seconds=60),'QUESTION_CONTEXT_CAPTURE_EXPIRED')
    expected={'observed_at','read_id'}|set(NAMES)|{n+'_count' for n in NAMES}
    _require(set(row)==expected,'QUESTION_CONTEXT_CAPTURE_SHAPE')
    records={}
    for name in NAMES:
        _require(type(row[name]) is list and type(row[name+'_count']) is int
            and 0<=row[name+'_count']<=256 and len(row[name])==row[name+'_count']
            and all(type(value) is str and len(value.encode())<=131072 for value in row[name]),
            'QUESTION_CONTEXT_CAPTURE_INCOMPLETE')
        records[name]=row[name]
    _require(all(len(records[n])==1 for n in ('workspaces','memberships','conversations')),
        'QUESTION_CONTEXT_ACCESS_MISSING')
    _require(_access(access_reader,identity,workspace_id,conversation_id)==before,'QUESTION_CONTEXT_ACCESS_CHANGED')
    result=QuestionContextRead(workspace_id,conversation_id,identity.principal.principal_id,
        canonical_records(records),read_id,row['observed_at'],started+timedelta(seconds=60))
    result.records();return result
