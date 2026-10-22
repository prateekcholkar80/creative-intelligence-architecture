"""Bounded E2 authority capture from current BigQuery records; no local fallback."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
from itertools import islice
import json
from uuid import uuid4
from google.cloud import bigquery

from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.campaign_quality_authority import CATALOG,TABLE,decode_authority,validate_transcript,table_metadata
from src.application.campaign_quality_request_reader import SQL as REQUEST_SQL,_decode_capture,MAX_ROW_BYTES,partition_predicate
from src.application.conversation_contract import validate_conversation
from src.application.identity import ensure_fresh,VerifiedIdentity
from src.application.operation_budget import operation_call,operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_codec import decode_row,encode_row
from src.application.workflow_contract import _id,_require,canonical_json_bytes,validate_model_invocation
from src.application.prompt_runtime_contract import InvocationPromptBinding,STORAGE_VERSION as BINDING_VERSION
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table

EXTRA_TABLES={'calls':'model_invocations','jobs':'jobs','attempts':'job_attempts'}
extras=[f'''ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=@row_bytes,TO_JSON_STRING(t),NULL)
 FROM `{PROJECT}.ci_application.{table}` t WHERE workspace_id=@workspace_id
 AND conversation_id=@conversation_id{partition_predicate(table)} LIMIT 257) AS {name},
 (SELECT COUNT(*) FROM `{PROJECT}.ci_application.{table}` WHERE workspace_id=@workspace_id
 AND conversation_id=@conversation_id{partition_predicate(table)}) AS {name}_count''' for name,table in EXTRA_TABLES.items()]
extras.extend([f'''ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=@row_bytes,TO_JSON_STRING(t),NULL)
 FROM `{TABLE}` t WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
 AND request_id=@request_id LIMIT 2) AS authorities''',
 f'''ARRAY(SELECT TO_JSON_STRING(STRUCT(sku,source_system,product_status))
 FROM `{PROJECT}.ci_product.product_master` WHERE sku IN UNNEST(@catalog_skus) ORDER BY sku LIMIT 24) AS products''',
 f'''ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=@row_bytes,TO_JSON_STRING(t),NULL)
 FROM `{PROJECT}.ci_application.invocation_prompt_bindings` t WHERE workspace_id=@workspace_id
 AND conversation_id=@conversation_id LIMIT 257) AS bindings,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_application.invocation_prompt_bindings`
 WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id) AS bindings_count'''])
SQL=REQUEST_SQL.replace('AS request_count;','AS request_count,\n'+',\n'.join(extras)+';')


@dataclass(frozen=True,slots=True)
class ExecutionAuthorityRead:
    content_canonical_json: str
    content_sha256: str
    native_job_id: str
    observed_at: datetime
    valid_until: datetime


def decode_capture(row,*,workspace_id,conversation_id,request_id,identity,started,ended):
    extra={'authorities','products','bindings','bindings_count'}|set(EXTRA_TABLES)|{n+'_count' for n in EXTRA_TABLES}
    _require(type(row) is dict and extra<=set(row),'QUALITY_AUTHORITY_CAPTURE_SHAPE')
    _require(type(row['products']) is list and len(row['products'])<=23,'QUALITY_CATALOG_OVERFLOW')
    products=[];catalog=set()
    for text in row['products']:
        _require(type(text) is str and len(text.encode())<=4096,'QUALITY_CATALOG_ROW')
        product=json.loads(text)
        _require(type(product) is dict and set(product)=={'sku','source_system','product_status'}
            and product['sku'] in CATALOG and product['sku'] not in catalog
            and product['product_status']=='ACTIVE','QUALITY_CATALOG_ROW')
        expected='NAKED_AND_THRIVING_OFFICIAL' if int(product['sku'][-4:])>20 else 'BEAUTYFEEDS'
        _require(product['source_system']==expected,'QUALITY_CATALOG_SOURCE')
        catalog.add(product['sku']);products.append(product)
    base={k:v for k,v in row.items() if k not in extra}
    pins=_decode_capture(base,workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id,
        identity=identity,started=started,ended=ended,catalog=frozenset(catalog))
    _require(type(row['authorities']) is list and len(row['authorities'])==1
        and type(row['authorities'][0]) is str and len(row['authorities'][0].encode())<=MAX_ROW_BYTES,
        'QUALITY_AUTHORITY_MISSING_OR_DUPLICATE')
    authority_row=json.loads(row['authorities'][0])
    authority_row['recorded_at']=datetime.fromisoformat(authority_row['recorded_at'].replace('Z','+00:00'))
    requests=[decode_row(t,'requests') for t in row['requests']]
    request=next(r for r in requests if r['request_id']==request_id)
    authority=decode_authority(authority_row,request=request,identity=identity,observed_at=row['observed_at'])
    records={}
    total_bytes=sum(len(t.encode()) for t in row['authorities'])
    for name,table in EXTRA_TABLES.items():
        values=row[name]
        _require(type(values) is list and type(row[name+'_count']) is int
            and 0<=row[name+'_count']<=256 and len(values)==row[name+'_count']
            and all(type(t) is str and len(t.encode())<=MAX_ROW_BYTES for t in values),'QUALITY_LINEAGE_INCOMPLETE')
        total_bytes+=sum(len(t.encode()) for t in values)
        records[name]=[decode_row(t,table) for t in values]
    _require(total_bytes<=4*1024**2,'QUALITY_LINEAGE_SIZE')
    conversation=decode_row(row['conversations'][0],'conversations')
    resource=validate_conversation(conversation,workspace_id=workspace_id,conversation_id=conversation_id)
    for name,key in (('calls','invocation_id'),('jobs','job_id'),('attempts','attempt_id')):
        _require(len({r[key] for r in records[name]})==len(records[name]),'QUALITY_LINEAGE_DUPLICATE')
        for record in records[name]:
            _require(record['workspace_id']==workspace_id and record['conversation_id']==conversation_id
                and record['owner_id']==resource.owner_id,'QUALITY_LINEAGE_SCOPE')
    by_request={r['request_id']:r for r in requests}
    for call in records['calls']:
        _require(call['request_id'] in by_request,'QUALITY_LINEAGE_SCOPE')
        jobs=[j for j in records['jobs'] if j['job_id']==call['job_id']]
        attempts=[a for a in records['attempts'] if a['attempt_id']==call['attempt_id']]
        _require(len(jobs)==(0 if call['job_id'] is None else 1)
            and len(attempts)==(0 if call['attempt_id'] is None else 1),'QUALITY_LINEAGE_PARENT_MISSING')
        validate_model_invocation(call,request=by_request[call['request_id']],conversation=resource,
            job=jobs[0] if jobs else None,attempt=attempts[0] if attempts else None,catalog=frozenset(catalog))
        _require(call['started_at']<=ended and (call['completed_at'] is None or call['completed_at']<=ended),
            'QUALITY_LINEAGE_FUTURE')
    transcript_sha=validate_transcript(authority,requests,records['calls'])
    _require(type(row['bindings']) is list and type(row['bindings_count']) is int
        and len(row['bindings'])==row['bindings_count']<=256,'QUALITY_BINDINGS_INCOMPLETE')
    bindings=[]
    for text in row['bindings']:
        _require(type(text) is str and len(text.encode())<=MAX_ROW_BYTES,'QUALITY_BINDING_SIZE')
        item=json.loads(text)
        _require(set(item)=={'schema_version','binding_sha256','bound_at','workspace_id','conversation_id',
            'request_id','invocation_id','principal_id','prompt_key','stage','binding_canonical_json'}
            and item['schema_version']==BINDING_VERSION,'QUALITY_BINDING_SHAPE')
        binding=InvocationPromptBinding(**{k:(datetime.fromisoformat(v.replace('Z','+00:00')) if k=='bound_at' else v)
            for k,v in item.items() if k not in {'schema_version','binding_sha256'}})
        _require(binding.binding_sha256==item['binding_sha256'] and binding.workspace_id==workspace_id
            and binding.conversation_id==conversation_id and binding.request_id in by_request
            and binding.principal_id==identity.principal.principal_id and binding.bound_at<=ended,
            'QUALITY_BINDING_SCOPE')
        bindings.append(item)
    _require(len({b['invocation_id'] for b in bindings})==len(bindings),'QUALITY_BINDING_DUPLICATE')
    def projection(values,table):
        return [json.loads(encode_row(v,table)) for v in sorted(values,key=lambda r:canonical_json_bytes(
            json.loads(encode_row(r,table))))]
    return dict(version='campaign-execution-authority-capture-v1',request_bindings=pins,authority=authority,
        authority_recorded_at=authority_row['recorded_at'].isoformat(),
        authority_sha256=_hash(authority),catalog_sha256=_hash(sorted(products,key=lambda p:p['sku'])),
        transcript_sha256=transcript_sha,request_record=json.loads(encode_row(request,'requests')),
        bindings=sorted(bindings,key=lambda b:b['invocation_id']),
        calls=projection(records['calls'],'model_invocations'),jobs=projection(records['jobs'],'jobs'),
        attempts=projection(records['attempts'],'job_attempts'))


def _read_native(client,*,workspace_id,conversation_id,request_id,identity,
                             clock=lambda:datetime.now(timezone.utc)):
    for key in (workspace_id,conversation_id,request_id):_id(key)
    _require(type(identity) is VerifiedIdentity,'QUALITY_REQUEST_IDENTITY_REQUIRED');ensure_fresh(identity)
    _require(client.project==PROJECT and client.location==LOCATION
        and client.default_query_job_config is None,'QUALITY_REQUEST_CLIENT_SCOPE')
    validate_table(operation_call(client.get_table,TABLE,retry=None,timeout=20),table_metadata())
    started=_utc(clock())
    cfg=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('workspace_id','STRING',workspace_id),
        bigquery.ScalarQueryParameter('conversation_id','STRING',conversation_id),
        bigquery.ScalarQueryParameter('request_id','STRING',request_id),
        bigquery.ScalarQueryParameter('principal_id','STRING',identity.principal.principal_id),
        bigquery.ScalarQueryParameter('row_bytes','INT64',MAX_ROW_BYTES),
        bigquery.ArrayQueryParameter('catalog_skus','STRING',sorted(CATALOG))],
        use_legacy_sql=False,use_query_cache=False,create_session=False,maximum_bytes_billed=512*1024**2,
        job_timeout_ms=60000,script_options=bigquery.ScriptOptions(statement_timeout_ms=20000))
    expected=bigquery.QueryJobConfig.from_api_repr(deepcopy(cfg.to_api_repr()));native_id='quality_authority_read_'+uuid4().hex
    job=operation_call(client.query,SQL,job_config=cfg,job_id=native_id,project=PROJECT,location=LOCATION,
        retry=None,job_retry=None,timeout=20)
    _require(matches_sharing_job(job,job_id=native_id,sql=SQL,config=expected),'QUALITY_AUTHORITY_NATIVE_MISMATCH')
    operation_call(job.result,max_results=0,retry=None,job_retry=None,timeout=60)
    native=operation_call(client.get_job,native_id,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(native,job_id=native_id,sql=SQL,config=expected)
        and native.state=='DONE' and native.error_result is None,'QUALITY_AUTHORITY_NATIVE_MISMATCH')
    rows=list(islice(operation_iter(operation_call(native.result,max_results=2,page_size=2,
        retry=None,job_retry=None,timeout=20)),2))
    ended=_utc(clock())
    _require(len(rows)==1 and matches_sharing_job(native,job_id=native_id,sql=SQL,config=expected)
        and native.state=='DONE' and native.error_result is None,'QUALITY_AUTHORITY_NATIVE_MISMATCH')
    ensure_fresh(identity)
    return dict(rows[0]),native_id,started,ended


def read_execution_authority(client,*,workspace_id,conversation_id,request_id,identity,
                             clock=lambda:datetime.now(timezone.utc)):
    row,native_id,started,ended=_read_native(client,workspace_id=workspace_id,conversation_id=conversation_id,
        request_id=request_id,identity=identity,clock=clock)
    content=decode_capture(row,workspace_id=workspace_id,conversation_id=conversation_id,
        request_id=request_id,identity=identity,started=started,ended=ended)
    ensure_fresh(identity)
    return ExecutionAuthorityRead(canonical_json_bytes(content).decode(),_hash(content),native_id,
        _utc(row['observed_at']),started+timedelta(seconds=60))
