"""Bounded authoritative prior-turn capture; missing/expired text never falls back."""
from dataclasses import dataclass
from datetime import datetime,timedelta
import hashlib,json
from google.cloud import bigquery
from src.application.campaign_transcript import SPEC as BODY_SPEC,VERSION as BODY_VERSION
from src.application.campaign_response_transcript import SPEC as RESPONSE_SPEC,VERSION as RESPONSE_VERSION
from src.application.campaign_quality_authority import CATALOG,validate_transcript
from src.application.control_io import ControlIO
from src.application.conversation_contract import validate_conversation
from src.application.identity import ensure_fresh
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import _require,validate_request,validate_model_invocation
from src.ingestion.setup_workflow_tables import DATASET_ID,PROJECT,validate_table
from src.application.operation_budget import operation_call

TABLES={'requests':('requests','created_at'),'calls':('model_invocations','started_at'),
        'bodies':(BODY_SPEC.name,'recorded_at'),'responses':(RESPONSE_SPEC.name,'recorded_at'),
        'jobs':('jobs',None),'attempts':('job_attempts','started_at')}

@dataclass(frozen=True,slots=True)
class TranscriptHistory:
    source_json:str
    turns:tuple
    texts:tuple
    observed_at:datetime


def sql():
    fields=['CURRENT_TIMESTAMP() observed_at']
    for key,(name,partition) in TABLES.items():
        where="workspace_id=@workspace_id AND conversation_id=@conversation_id"
        if partition:where+=f" AND {partition} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')"
        fields.extend([f"ARRAY(SELECT TO_JSON_STRING(t) FROM `{DATASET_ID}.{name}` t WHERE {where} LIMIT 101) {key}",
            f"(SELECT COUNT(*) FROM `{DATASET_ID}.{name}` t WHERE {where}) {key}_count"])
    return 'BEGIN TRANSACTION;\nCREATE TEMP TABLE history AS SELECT '+',\n'.join(fields)+';\nCOMMIT TRANSACTION;\nSELECT * FROM history;'


def decode_history(row,*,resource,observed_at):
    _require(set(row)=={'observed_at'}|set(TABLES)|{k+'_count' for k in TABLES},'TRANSCRIPT_HISTORY_SHAPE')
    at=row['observed_at'];_require(isinstance(at,datetime) and at.tzinfo is not None
        and at<=observed_at<at+timedelta(seconds=60),'TRANSCRIPT_HISTORY_EXPIRED')
    for name in TABLES:
        _require(type(row[name]) is list and type(row[name+'_count']) is int and 0<=len(row[name])==row[name+'_count']<=100
            and all(type(t) is str and len(t.encode())<=300000 for t in row[name]),'TRANSCRIPT_HISTORY_COVERAGE')
    _require(sum(len(t.encode()) for name in TABLES for t in row[name])<=4*1024**2,'TRANSCRIPT_HISTORY_SIZE')
    _require(not row['jobs'] and not row['attempts'],'TRANSCRIPT_HISTORY_MIXED_WORKFLOW_UNSUPPORTED')
    requests=[decode_row(t,'requests') for t in row['requests']];calls=[decode_row(t,'model_invocations') for t in row['calls']]
    by_id={r['request_id']:r for r in requests};_require(len(by_id)==len(requests),'TRANSCRIPT_HISTORY_DUPLICATE')
    _require(len({c['invocation_id'] for c in calls})==len(calls),'TRANSCRIPT_HISTORY_DUPLICATE')
    for request in requests:
        validate_request(request,conversation=resource,catalog=CATALOG)
        _require(request['operation']=='INTERPRET' and request['message_sha256'] is not None
            and request['request_status'] in {'NEEDS_CLARIFICATION','FAILED','BLOCKED','READY'}
            and request['updated_at']<=at,'TRANSCRIPT_HISTORY_UNFINISHED')
    for call in calls:
        _require(call['request_id'] in by_id and call['job_id'] is None and call['attempt_id'] is None,'TRANSCRIPT_HISTORY_ORPHAN')
        validate_model_invocation(call,request=by_id[call['request_id']],conversation=resource,catalog=CATALOG)
        _require(call['state'] in {'SUCCEEDED','FAILED','BLOCKED'} and call['completed_at'] is not None
            and call['completed_at']<=at,'TRANSCRIPT_HISTORY_UNFINISHED')
    _require(all(any(c['request_id']==r['request_id'] for c in calls) for r in requests),'TRANSCRIPT_HISTORY_OUTCOME_MISSING')
    def rows(name,spec,version,key):
        values={}
        for text in row[name]:
            value=json.loads(text)
            _require(set(value)=={f.name for f in spec.schema} and value['schema_version']==version
                and value['workspace_id']==resource.workspace_id and value['conversation_id']==resource.resource_id
                and value['request_id'] in by_id and value[key] not in values,'TRANSCRIPT_HISTORY_BODY_SCOPE')
            recorded=datetime.fromisoformat(value['recorded_at'].replace('Z','+00:00'))
            _require(at-timedelta(days=30)<=recorded<=at and by_id[value['request_id']]['created_at']<=recorded,'TRANSCRIPT_HISTORY_BODY_EXPIRED')
            values[value[key]]=value
        return values
    bodies=rows('bodies',BODY_SPEC,BODY_VERSION,'request_id');responses=rows('responses',RESPONSE_SPEC,RESPONSE_VERSION,'invocation_id')
    _require(set(bodies)==set(by_id),'TRANSCRIPT_HISTORY_BODY_MISSING')
    successful={c['invocation_id']:c for c in calls if c['state']=='SUCCEEDED'}
    _require(set(responses)==set(successful),'TRANSCRIPT_HISTORY_RESPONSE_MISSING')
    turns=[];texts=[]
    for request in sorted(requests,key=lambda r:(r['created_at'],r['request_id'])):
        batch=json.loads(bodies[request['request_id']]['batch_canonical_json'])
        _require(type(batch) is list and all(type(i) is dict and set(i)=={'turn','text'} for i in batch),'TRANSCRIPT_HISTORY_BODY_SHAPE')
        own=dict(request_id=request['request_id'],role='USER',content_sha256=request['message_sha256'],invocation_id=None)
        _require([i['turn'] for i in batch]==[own],'TRANSCRIPT_HISTORY_ORDER')
        validate_transcript(dict(request_id=request['request_id'],transcript=turns+[own]),requests,calls)
        for item in batch:
            _require(type(item['text']) is str and 0<len(item['text'].encode())<=65536
                and hashlib.sha256(item['text'].encode()).hexdigest()==item['turn']['content_sha256'],'TRANSCRIPT_HISTORY_HASH')
        turns.append(own);texts.append(batch[-1]['text'])
        matching=[c for c in successful.values() if c['request_id']==request['request_id']]
        _require(len(matching)<=1,'TRANSCRIPT_HISTORY_AMBIGUOUS_RESPONSE')
        for call in matching:
            response=responses[call['invocation_id']];text=response['response_text']
            _require(response['request_id']==request['request_id'] and call['finish_reason']=='STOP'
                and call['safety_outcome']=='PASSED' and type(text) is str and 0<len(text.encode())<=65536
                and response['content_sha256']==call['response_sha256']==hashlib.sha256(text.encode()).hexdigest(),'TRANSCRIPT_HISTORY_RESPONSE_HASH')
            turns.append(dict(request_id=request['request_id'],role='ASSISTANT',content_sha256=call['response_sha256'],invocation_id=call['invocation_id']))
            texts.append(text)
    _require(len(turns)<100 and sum(len(t.encode()) for t in texts)<=262144,'TRANSCRIPT_HISTORY_SIZE')
    source=json.dumps({key:row[key] for key in TABLES},sort_keys=True,separators=(',',':'))
    return TranscriptHistory(source,tuple(turns),tuple(texts),at)


def read_history(client,*,identity,workspace_id,conversation_id,conversation,clock):
    ensure_fresh(identity);resource=validate_conversation(conversation,workspace_id=workspace_id,conversation_id=conversation_id)
    _require(resource.owner_id==identity.principal.principal_id,'TRANSCRIPT_HISTORY_OWNER')
    for spec in (BODY_SPEC,RESPONSE_SPEC):
        expected=spec.table(PROJECT)
        validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
    row=ControlIO(client).read(sql(),[bigquery.ScalarQueryParameter('workspace_id','STRING',workspace_id),bigquery.ScalarQueryParameter('conversation_id','STRING',conversation_id)])
    result=decode_history(row,resource=resource,observed_at=clock());ensure_fresh(identity)
    return result


def read_bound_texts(client,content,*,workspace_id,conversation_id,clock):
    """Resolve hash-pinned input turns from individual original-age body rows."""
    row=ControlIO(client).read(sql(),[bigquery.ScalarQueryParameter('workspace_id','STRING',workspace_id),
        bigquery.ScalarQueryParameter('conversation_id','STRING',conversation_id)])
    at=clock();turns=content['authority']['transcript'];texts=[]
    for name in ('bodies','responses'):
        _require(type(row[name]) is list and len(row[name])==row[name+'_count']<=100,'TRANSCRIPT_BODY_UNAVAILABLE')
    bodies=[json.loads(t) for t in row['bodies']];responses=[json.loads(t) for t in row['responses']]
    for turn in turns:
        if turn['role']=='USER':
            matching=[r for r in bodies if r['request_id']==turn['request_id']]
            spec,version=BODY_SPEC,BODY_VERSION
        else:
            matching=[r for r in responses if r['invocation_id']==turn['invocation_id']]
            spec,version=RESPONSE_SPEC,RESPONSE_VERSION
        _require(len(matching)==1,'TRANSCRIPT_BODY_UNAVAILABLE');body=matching[0]
        _require(set(body)=={f.name for f in spec.schema} and body['schema_version']==version
            and body['workspace_id']==workspace_id and body['conversation_id']==conversation_id
            and body['request_id']==turn['request_id'],'TRANSCRIPT_BODY_SCOPE')
        recorded=datetime.fromisoformat(body['recorded_at'].replace('Z','+00:00'))
        _require(at-timedelta(days=30)<recorded<=at,'TRANSCRIPT_BODY_EXPIRED')
        if turn['role']=='USER':
            batch=json.loads(body['batch_canonical_json'])
            _require(type(batch) is list and len(batch)==1 and set(batch[0])=={'turn','text'}
                and batch[0]['turn']==turn,'TRANSCRIPT_BODY_CHANGED')
            text=batch[0]['text']
        else:
            _require(body['content_sha256']==turn['content_sha256'],'TRANSCRIPT_BODY_CHANGED')
            text=body['response_text']
        _require(type(text) is str and 0<len(text.encode())<=65536
            and hashlib.sha256(text.encode()).hexdigest()==turn['content_sha256'],'TRANSCRIPT_BODY_CHANGED')
        texts.append(text)
    _require(sum(len(t.encode()) for t in texts)<=262144,'TRANSCRIPT_HISTORY_SIZE')
    return tuple(texts)
