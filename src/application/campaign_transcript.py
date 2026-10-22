"""Opt-in raw transcript bodies, exact hash coverage and 30-day retention."""
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
import hashlib,json
from google.cloud import bigquery
from src.application.workflow_schema import TableSpec,field
from src.application.workflow_contract import _require,canonical_json_bytes
from src.application.campaign_quality_authority_reader import read_execution_authority
from src.application.control_io import ControlIO
from src.application.prompt_evaluation_query import wire
from src.ingestion.setup_workflow_tables import DATASET_ID,validate_table
from src.application.operation_budget import operation_call

VERSION='campaign-transcript-bodies-v1'
RETENTION_DAYS=30

@dataclass(frozen=True)
class TranscriptSpec(TableSpec):
    def table(self,project_id):
        table=super().table(project_id)
        table.time_partitioning.expiration_ms=RETENTION_DAYS*86400000
        return table

SPEC=TranscriptSpec('campaign_transcript_bodies',tuple(field(k) for k in
    ('schema_version','workspace_id','conversation_id','request_id','batch_canonical_json'))+
    (field('recorded_at','TIMESTAMP'),),('workspace_id','request_id'),
    ('workspace_id','conversation_id','request_id'),'recorded_at',True)


def encode_turns(authority,texts):
    _require(type(texts) is tuple and len(texts)==len(authority['transcript']),'TRANSCRIPT_TEXT_COVERAGE')
    result=[];total=0
    for turn,text in zip(authority['transcript'],texts):
        _require(type(text) is str and 0<len(text.encode())<=65536,'TRANSCRIPT_TEXT_SIZE')
        total+=len(text.encode())
        _require(hashlib.sha256(text.encode()).hexdigest()==turn['content_sha256'],'TRANSCRIPT_TEXT_HASH_CHANGED')
        result.append(dict(turn=turn,text=text))
    _require(total<=262144,'TRANSCRIPT_TEXT_SIZE')
    return canonical_json_bytes(result).decode()


def extend_publication(sql,params,authority,texts):
    batch=encode_turns(authority,texts)
    marker='INSERT INTO `'+DATASET_ID+'.campaign_request_authorities`'
    _require(sql.count(marker)==1 and sql.count('END IF;')==1,'TRANSCRIPT_PUBLICATION_SHAPE')
    insert=f'''INSERT INTO `{DATASET_ID}.{SPEC.name}`
 (schema_version,workspace_id,conversation_id,request_id,batch_canonical_json,recorded_at)
 VALUES('{VERSION}',@workspace_id,@conversation_id,@request_id,@transcript_batch,CURRENT_TIMESTAMP());
'''
    # Exact replay checks bodies too; existing hash-only jobs cannot be upgraded
    # by reminting the original global job ID or silently filling absent bodies.
    check=f'''ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.{SPEC.name}`
 WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id AND request_id=@request_id
 AND recorded_at BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 30 DAY) AND CURRENT_TIMESTAMP())=1
 AND (SELECT COUNT(*) FROM `{DATASET_ID}.{SPEC.name}` WHERE workspace_id=@workspace_id
 AND conversation_id=@conversation_id AND request_id=@request_id
 AND recorded_at BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()
 AND schema_version='{VERSION}' AND batch_canonical_json=@transcript_batch)=1 AS 'TRANSCRIPT_BODY_CONFLICT';'''
    sql=sql.replace(marker,insert+marker).replace('END IF;','END IF;\n'+check)
    return sql,params+[bigquery.ScalarQueryParameter('transcript_batch','STRING',batch)]


def read_transcript(client,*,identity,workspace_id,conversation_id,request_id,clock=lambda:datetime.now(timezone.utc)):
    args=dict(identity=identity,workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id,clock=clock)
    before=read_execution_authority(client,**args);content=json.loads(before.content_canonical_json)
    if len(content['authority']['transcript'])>1:
        from src.application.campaign_transcript_history import read_bound_texts
        from src.application.campaign_response_transcript import SPEC as RESPONSE_SPEC
        for spec in (SPEC,RESPONSE_SPEC):
            expected=spec.table(client.project)
            validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
        texts=read_bound_texts(client,content,workspace_id=workspace_id,conversation_id=conversation_id,clock=clock)
        after=read_execution_authority(client,**args)
        _require(before.content_sha256==after.content_sha256 and before.native_job_id!=after.native_job_id
            and clock()<min(before.valid_until,after.valid_until),'TRANSCRIPT_CONTEXT_CHANGED')
        return texts
    expected=SPEC.table(client.project)
    validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
    io=ControlIO(client)
    params=[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in
        dict(workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id).items()]
    rows=io.read(f'''SELECT ARRAY(SELECT AS STRUCT schema_version,batch_canonical_json,recorded_at
 FROM `{DATASET_ID}.{SPEC.name}` WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
 AND request_id=@request_id AND recorded_at BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 30 DAY)
 AND CURRENT_TIMESTAMP() LIMIT 2) AS bodies''',params)
    _require(set(rows)=={'bodies'} and len(rows['bodies'])==1,'TRANSCRIPT_BODY_UNAVAILABLE')
    row=dict(rows['bodies'][0]);batch=json.loads(row['batch_canonical_json'])
    _require(row['schema_version']==VERSION and type(batch) is list and len(batch)<=100
        and all(type(item) is dict and set(item)=={'turn','text'} for item in batch),'TRANSCRIPT_BODY_SHAPE')
    texts=tuple(item['text'] for item in batch)
    _require([item['turn'] for item in batch]==content['authority']['transcript']
        and encode_turns(content['authority'],texts)==row['batch_canonical_json'],'TRANSCRIPT_BODY_CHANGED')
    at=clock();_require(at-timedelta(days=30)<row['recorded_at']<=at,'TRANSCRIPT_BODY_EXPIRED')
    after=read_execution_authority(client,**args)
    _require(before.content_sha256==after.content_sha256 and before.native_job_id!=after.native_job_id
        and clock()<min(before.valid_until,after.valid_until),'TRANSCRIPT_CONTEXT_CHANGED')
    return texts
