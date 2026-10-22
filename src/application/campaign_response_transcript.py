"""Private provider response bodies; 30-day retention, never release permission."""
from dataclasses import replace
from datetime import datetime
import hashlib,json
from google.cloud import bigquery
from src.application.campaign_transcript import TranscriptSpec
from src.application.workflow_schema import field
from src.application.workflow_invocation_result_query import _fields_expr
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import DATASET_ID

VERSION='campaign-response-transcript-v1'
SPEC=TranscriptSpec('campaign_transcript_responses',tuple(field(k) for k in
    ('schema_version','workspace_id','conversation_id','request_id','invocation_id','content_sha256','response_text'))+
    (field('recorded_at','TIMESTAMP'),),('workspace_id','invocation_id'),
    ('workspace_id','conversation_id','request_id'),'recorded_at',True)


def extend_result(plan,*,request,invocation,observation,response_text,observed_at):
    _require(observation.state=='SUCCEEDED' and observation.finish_reason=='STOP' and observation.safety_outcome=='PASSED'
        and type(response_text) is str and 0<len(response_text.encode())<=65536
        and hashlib.sha256(response_text.encode()).hexdigest()==observation.response_sha256,'RESPONSE_TRANSCRIPT_HASH_MISMATCH')
    for key in ('workspace_id','conversation_id','request_id'):
        _require(invocation[key]==request[key],'RESPONSE_TRANSCRIPT_SCOPE')
    row=dict(schema_version=VERSION,**{k:request[k] for k in ('workspace_id','conversation_id','request_id')},
        invocation_id=invocation['invocation_id'],content_sha256=observation.response_sha256,
        response_text=response_text,recorded_at=observed_at.isoformat())
    encoded=json.dumps(row,sort_keys=True,separators=(',',':'))
    declaration=f"DECLARE response_candidate DEFAULT STRUCT({_fields_expr(SPEC.schema,'PARSE_JSON(@response_candidate)')});\n"
    scope="workspace_id=@workspace_id AND recorded_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31') AND invocation_id=response_candidate.invocation_id"
    exact=f"(SELECT COUNT(*) FROM `{DATASET_ID}.{SPEC.name}` t WHERE {scope} AND TO_JSON_STRING(t)=TO_JSON_STRING(response_candidate))=1"
    sql=declaration+plan.sql
    sql=sql.replace(" AS 'INTERPRETER_REPLAY_CONFLICT';",f" AND {exact} AND (SELECT COUNT(*) FROM `{DATASET_ID}.{SPEC.name}` WHERE {scope})=1 AS 'INTERPRETER_REPLAY_CONFLICT';")
    marker=" ASSERT CURRENT_TIMESTAMP()<@expires_at AS 'INTERPRETER_PLAN_EXPIRED';"
    sql=sql.replace(marker,f" ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.{SPEC.name}` WHERE {scope})=0 AS 'RESPONSE_TRANSCRIPT_CONFLICT';\n"+marker)
    columns=', '.join(f.name for f in SPEC.schema)
    values=', '.join('response_candidate.'+f.name for f in SPEC.schema)
    sql=sql.replace(" SET result_state='APPLIED';",f" INSERT INTO `{DATASET_ID}.{SPEC.name}` ({columns}) SELECT {values};\n SET result_state='APPLIED';")
    sql=sql.replace(' AS accounting_audit_row;',' AS accounting_audit_row,TO_JSON_STRING(response_candidate) AS response_row;')
    _require(sql.count('RESPONSE_TRANSCRIPT_CONFLICT')==1 and 'AS response_row;' in sql,'RESPONSE_TRANSCRIPT_SQL_SHAPE')
    config=plan.config();config.query_parameters=list(config.query_parameters)+[bigquery.ScalarQueryParameter('response_candidate','STRING',encoded)]
    return replace(plan,sql=sql,config_json=json.dumps(config.to_api_repr(),sort_keys=True))


def validate_response_row(actual,expected):
    def decode(text):
        row=json.loads(text)
        _require(set(row)=={f.name for f in SPEC.schema},'RESPONSE_TRANSCRIPT_ROW_SHAPE')
        row['recorded_at']=datetime.fromisoformat(row['recorded_at'].replace('Z','+00:00'))
        return row
    _require(decode(actual)==decode(expected),'RESPONSE_TRANSCRIPT_RECEIPT_MISMATCH')


def verify_stored_response(client,invocation,*,clock):
    """Read-only cold recovery evidence; no reconstructed native commit receipt."""
    from datetime import timedelta
    from src.application.control_io import ControlIO
    from src.ingestion.setup_workflow_tables import PROJECT,validate_table
    from src.application.operation_budget import operation_call
    expected=SPEC.table(PROJECT)
    validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
    params=[bigquery.ScalarQueryParameter(k,'STRING',invocation[k]) for k in
        ('workspace_id','invocation_id')]
    row=ControlIO(client).read(f'''SELECT ARRAY(SELECT TO_JSON_STRING(t) FROM `{DATASET_ID}.{SPEC.name}` t
 WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id AND recorded_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31') LIMIT 2) records''',params)
    _require(set(row)=={'records'} and len(row['records'])==1,'RESPONSE_TRANSCRIPT_UNAVAILABLE')
    body=json.loads(row['records'][0]);_require(set(body)=={f.name for f in SPEC.schema} and body['schema_version']==VERSION,'RESPONSE_TRANSCRIPT_ROW_SHAPE')
    for key in ('workspace_id','conversation_id','request_id','invocation_id'):
        _require(body[key]==invocation[key],'RESPONSE_TRANSCRIPT_SCOPE')
    at=clock();recorded=datetime.fromisoformat(body['recorded_at'].replace('Z','+00:00'))
    completed=invocation['completed_at']
    if type(completed) is str:completed=datetime.fromisoformat(completed.replace('Z','+00:00'))
    _require(at-timedelta(days=30)<recorded<=at and recorded==completed,'RESPONSE_TRANSCRIPT_EXPIRED')
    text=body['response_text']
    _require(invocation['state']=='SUCCEEDED' and invocation['finish_reason']=='STOP' and invocation['safety_outcome']=='PASSED'
        and type(text) is str and 0<len(text.encode())<=65536 and body['content_sha256']==invocation['response_sha256']
        ==hashlib.sha256(text.encode()).hexdigest(),'RESPONSE_TRANSCRIPT_HASH_MISMATCH')
    return body['content_sha256']
