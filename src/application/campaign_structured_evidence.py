"""Scoped structured evidence discovery; facts are not claim/offer permission."""
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
from decimal import Decimal,InvalidOperation
from itertools import islice
from uuid import uuid4
import json,unicodedata
from google.cloud import bigquery
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_permission_reader import read_permission,PermissionRequest,Checkpoint
from src.application.campaign_permission_contract import EvidenceKind
from src.application.campaign_quality_product_reader import _access
from src.application.sharing_jobs import matches_sharing_job
from src.application.operation_budget import operation_call,operation_iter
from src.application.workflow_contract import _require,_id,_digest
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table

DATASET=PROJECT+'.ci_brand_memory'
RECORDS='campaign_structured_evidence_records'
REVOCATIONS='campaign_structured_evidence_revocations'
VERSION='campaign-structured-evidence-v1'
FIELDS={
    'CURRENT_LABEL':{'formula_version','label_reference','directions','warnings'},
    'CURRENT_PRICE':{'price','currency','effective_at'},
    'CURRENT_FULFILMENT':{'availability','effective_at'},
    'CURRENT_SHIPPING':{'shipping_terms','effective_at'},
    'OFFER_EXCLUSIONS':{'exclusions'},
}


def table_metadata(name):
    _require(name in {RECORDS,REVOCATIONS},'STRUCTURED_TABLE')
    fields=[bigquery.SchemaField(k,'STRING',mode='REQUIRED') for k in ('schema_version','record_id','record_sha256')]
    if name==RECORDS:
        fields += [bigquery.SchemaField(k,'STRING',mode='REQUIRED') for k in ('sku','evidence_kind','record_canonical_json')]
        clustering=['sku','evidence_kind','record_id']
    else:clustering=['record_id']
    fields += [bigquery.SchemaField('recorded_at','TIMESTAMP',mode='REQUIRED')]
    table=bigquery.Table(DATASET+'.'+name,schema=fields);table.clustering_fields=clustering
    table.description=VERSION+': immutable source records; no implicit use permission'
    return table


def validate_record(value):
    _require(type(value) is dict and set(value)=={'schema_version','record_id','sku','evidence_kind','formula_version',
        'countries','channels','audiences','permission_record_id','fields','source_reference','source_sha256','status','review_id','effective_at','valid_until'},
        'STRUCTURED_RECORD_SHAPE')
    _require(value['schema_version']==VERSION and value['evidence_kind'] in FIELDS and value['sku'] in
        {f'CI-SKU-{i:04d}' for i in range(1,24)},'STRUCTURED_RECORD_SCOPE')
    if value['evidence_kind']=='OFFER_EXCLUSIONS':_id(value['permission_record_id'])
    else:_require(value['permission_record_id'] is None,'STRUCTURED_UNEXPECTED_PERMISSION')
    for k in ('record_id','source_reference'):_id(value[k],maximum=4096)
    _digest(value['source_sha256'])
    if value['formula_version'] is not None:_id(value['formula_version'])
    for key,allowed in (('countries',{'US','CA','GB','AU'}),('channels',{'META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'}),
            ('audiences',{'NEW','RETURNING'})):
        values=value[key]
        _require(type(values) is list and bool(values) and all(type(v) is str for v in values)
            and len(set(values))==len(values) and set(values)<=allowed,'STRUCTURED_RECORD_SCOPE')
    fields=value['fields']
    _require(type(fields) is dict and bool(fields) and set(fields)<=FIELDS[value['evidence_kind']]
        and all(type(v) is str and 0<len(v.encode())<=8192 and unicodedata.normalize('NFC',v)==v for v in fields.values()),
        'STRUCTURED_RECORD_FIELDS')
    if 'formula_version' in fields:_require(fields['formula_version']==value['formula_version'],'STRUCTURED_FORMULA_CONFLICT')
    if 'price' in fields:
        try:price=Decimal(fields['price'])
        except InvalidOperation:raise ValueError('STRUCTURED_PRICE') from None
        _require(price.is_finite() and 0<=price<10**12 and price.as_tuple().exponent>=-9,'STRUCTURED_PRICE')
    if 'currency' in fields:_require(fields['currency'] in {'USD','CAD','GBP','AUD'},'STRUCTURED_CURRENCY')
    if 'availability' in fields:_require(fields['availability'] in {'IN_STOCK','OUT_OF_STOCK','PREORDER','BACKORDER','UNKNOWN'},'STRUCTURED_AVAILABILITY')
    start=_utc(datetime.fromisoformat(value['effective_at']));end=_utc(datetime.fromisoformat(value['valid_until']))
    if 'effective_at' in fields:
        _require(_utc(datetime.fromisoformat(fields['effective_at']))==start,'STRUCTURED_EFFECTIVE_AT_CONFLICT')
    _require(start<end and value['status'] in {'UNVERIFIED','PENDING_REVIEW','APPROVED','REJECTED'},'STRUCTURED_RECORD_STATUS')
    if value['review_id'] is not None:_id(value['review_id'])
    if value['status']=='APPROVED':_require(value['review_id'] is not None,'STRUCTURED_REVIEW_REQUIRED')
    _require(len(canonical(value).encode())<=65536,'STRUCTURED_RECORD_SIZE')
    return value


@dataclass(frozen=True,slots=True)
class StructuredRead:
    state: str
    available_fields: tuple[str,...]
    source_sha256: str
    native_job_id: str
    observed_at: datetime
    valid_until: datetime


SQL=f'''WITH scoped AS (SELECT * FROM `{DATASET}.{RECORDS}` WHERE sku=@sku AND evidence_kind=@kind)
SELECT CURRENT_TIMESTAMP() observed_at,
 ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=131072,TO_JSON_STRING(t),NULL) FROM scoped t LIMIT 33) records,
 (SELECT COUNT(*) FROM scoped) record_count,
 ARRAY(SELECT TO_JSON_STRING(t) FROM `{DATASET}.{REVOCATIONS}` t
 WHERE record_id IN (SELECT record_id FROM scoped) LIMIT 33) revocations,
 (SELECT COUNT(*) FROM `{DATASET}.{REVOCATIONS}` WHERE record_id IN (SELECT record_id FROM scoped)) revocation_count'''


def decode_rows(row,*,sku,kind,now):
    _require(type(row) is dict and set(row)=={'observed_at','records','record_count','revocations','revocation_count'},'STRUCTURED_READ_SHAPE')
    for key,count in (('records','record_count'),('revocations','revocation_count')):
        _require(type(row[key]) is list and type(row[count]) is int and 0<=row[count]<=32 and len(row[key])==row[count]
            and all(type(text) is str and len(text.encode())<=131072 for text in row[key]),'STRUCTURED_READ_PARTIAL')
    records={};revoked=set()
    for text in row['records']:
        item=json.loads(text)
        _require(set(item)=={f.name for f in table_metadata(RECORDS).schema} and item['schema_version']==VERSION
            and item['sku']==sku and item['evidence_kind']==kind,'STRUCTURED_ROW_SCOPE')
        value=validate_record(json.loads(item['record_canonical_json']))
        _require(canonical(value)==item['record_canonical_json'] and _hash(value)==item['record_sha256']
            and value['record_id']==item['record_id'] and value['sku']==sku and value['evidence_kind']==kind
            and value['record_id'] not in records,'STRUCTURED_ROW_HASH_OR_DUPLICATE')
        _require(_utc(datetime.fromisoformat(item['recorded_at'].replace('Z','+00:00')))<=now,'STRUCTURED_FUTURE_ROW')
        records[value['record_id']]=(value,item['record_sha256'])
    for text in row['revocations']:
        item=json.loads(text)
        _require(set(item)=={f.name for f in table_metadata(REVOCATIONS).schema} and item['schema_version']==VERSION
            and item['record_id'] in records and item['record_id'] not in revoked
            and records[item['record_id']][1]==item['record_sha256']
            and _utc(datetime.fromisoformat(item['recorded_at'].replace('Z','+00:00')))<=now,'STRUCTURED_REVOCATION_CONFLICT')
        revoked.add(item['record_id'])
    return records,revoked,_hash(dict(records=sorted(row['records']),revocations=sorted(row['revocations'])))


def project_records(records,revoked,*,scope,formula_version,audience,required_fields,now,review_reader,permission_record_id=None,expected_exclusions=None):
    matches=[(value,sha) for value,sha in records.values() if scope['country'] in value['countries']
        and scope['channel'] in value['channels'] and audience in value['audiences']
        and (value['evidence_kind']!='OFFER_EXCLUSIONS' or value['permission_record_id']==permission_record_id)]
    if not matches:return 'ABSENT',()
    if len(matches)!=1:return 'CONFLICT',()
    value,sha=matches[0]
    if value['record_id'] in revoked or value['status']=='REJECTED':return 'REJECTED',()
    if value['status'] in {'UNVERIFIED','PENDING_REVIEW'}:return 'PENDING_REVIEW',()
    if not _utc(datetime.fromisoformat(value['effective_at']))<=now<_utc(datetime.fromisoformat(value['valid_until'])):return 'STALE',()
    if value['evidence_kind']=='CURRENT_LABEL' and (formula_version is None or formula_version!=value['formula_version']):return 'CONFLICT',()
    try:
        review=review_reader(value['review_id']);pin=review.state.current
        _require(review.state.kind=='EVIDENCE' and pin.kind is EvidenceKind.PRODUCT_FACT and pin.content_sha256==sha
            and pin.sku==value['sku'] and pin.formula_version==value['formula_version']
            and now-timedelta(seconds=60)<=review.checked_at<=now,'STRUCTURED_REVIEW_CONFLICT')
        review.state.require_current(now)
    except ValueError:return 'CONFLICT',()
    if value['evidence_kind']=='OFFER_EXCLUSIONS' and value['fields'].get('exclusions')!=expected_exclusions:
        return 'CONFLICT',()
    fields=tuple(f for f in required_fields if f in value['fields'] and value['fields'][f]!='UNKNOWN')
    return 'CURRENT_APPROVED',fields


def read_structured(client,*,scope,kind,formula_version,audience,required_fields,identity,access_reader,
        clock=lambda:datetime.now(timezone.utc),permission_record_id=None,expected_exclusions=None):
    _require(kind in FIELDS and set(required_fields)<=FIELDS[kind] and scope['sku'] in
        {f'CI-SKU-{i:04d}' for i in range(1,24)} and audience in {'NEW','RETURNING'},'STRUCTURED_READ_SCOPE')
    _require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,'STRUCTURED_CLIENT')
    before=_access(access_reader,identity,scope['workspace_id'],scope['conversation_id']);started=_utc(clock())
    for name in (RECORDS,REVOCATIONS):
        expected=table_metadata(name);validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
    params=[bigquery.ScalarQueryParameter('sku','STRING',scope['sku']),bigquery.ScalarQueryParameter('kind','STRING',kind)]
    cfg=bigquery.QueryJobConfig(query_parameters=params,use_legacy_sql=False,use_query_cache=False,create_session=False,
        maximum_bytes_billed=128*1024**2,job_timeout_ms=60000)
    nid='structured_evidence_read_'+uuid4().hex
    job=operation_call(client.query,SQL,job_config=cfg,job_id=nid,location=LOCATION,retry=None,job_retry=None,timeout=20)
    operation_call(job.result,max_results=0,retry=None,job_retry=None,timeout=45)
    native=operation_call(client.get_job,nid,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(native,job_id=nid,sql=SQL,config=cfg) and native.state=='DONE'
        and native.error_result is None,'STRUCTURED_NATIVE_MISMATCH')
    rows=list(islice(operation_iter(operation_call(native.result,max_results=2,page_size=2,retry=None,job_retry=None,timeout=20)),2))
    now=_utc(clock());_require(len(rows)==1 and started<=rows[0]['observed_at']<=now<started+timedelta(seconds=60),'STRUCTURED_READ_EXPIRED')
    records,revoked,source=decode_rows(dict(rows[0]),sku=scope['sku'],kind=kind,now=now)
    reviews=[]
    def reader(rid):
        snapshot=read_permission(client,PermissionRequest('EVIDENCE',Checkpoint(rid)),clock=clock)
        reviews.append(dict(id=rid,sha256=snapshot.state.current.sha256,valid_until=snapshot.state.current.review.valid_until.isoformat()));return snapshot
    state,fields=project_records(records,revoked,scope=scope,formula_version=formula_version,audience=audience,
        required_fields=required_fields,now=now,review_reader=reader,permission_record_id=permission_record_id,expected_exclusions=expected_exclusions)
    _require(before==_access(access_reader,identity,scope['workspace_id'],scope['conversation_id'])
        and _utc(clock())<started+timedelta(seconds=60),'STRUCTURED_ACCESS_OR_EXPIRY_CHANGED')
    deadline=started+timedelta(seconds=60)
    if state=='CURRENT_APPROVED':
        applicable=[value for value,sha in records.values() if scope['country'] in value['countries']
            and scope['channel'] in value['channels'] and audience in value['audiences']
            and (kind!='OFFER_EXCLUSIONS' or value['permission_record_id']==permission_record_id)]
        deadline=min([deadline]+[_utc(datetime.fromisoformat(v['valid_until'])) for v in applicable]
            +[_utc(datetime.fromisoformat(r['valid_until'])) for r in reviews])
        _require(_utc(clock())<deadline,'STRUCTURED_SOURCE_EXPIRED')
    return StructuredRead(state,fields,_hash(dict(source=source,reviews=reviews)),nid,rows[0]['observed_at'],deadline)
