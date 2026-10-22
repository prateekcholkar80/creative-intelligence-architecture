"""Bounded native-correlated discovery of existing claim/offer permission records.

No approvals, policy loading, intent inference, model calls or public response.
Unsupported stores/scopes stay UNAVAILABLE, never invented ABSENT records.
"""
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
from itertools import islice
from uuid import uuid4
import json
from google.cloud import bigquery
from src.application.campaign_acceptance_contract import _hash,_utc,PolicyStatus
from src.application.campaign_evidence_gaps import EvidenceCoverage,validate_policy
from src.application.campaign_permission_storage import PREFIX,decode_state,reconcile_claim_evidence,table_metadata
from src.application.campaign_permission_contract import ClaimKind,EvidenceKind
from src.application.campaign_quality_product_reader import _access
from src.application.campaign_structured_evidence import read_structured,FIELDS as STRUCTURED_FIELDS
from src.application.operation_budget import operation_call,operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table

MAX_ROWS=2146
MAX_BYTES=4*1024**2
NAMES=tuple(prefix+suffix for prefix in PREFIX.values() for suffix in ('_heads','_versions'))
SQL='SELECT CURRENT_TIMESTAMP() observed_at,@scope_sha256 scope_sha256,\n'+',\n'.join(
    f'''ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=262144,TO_JSON_STRING(t),NULL)
 FROM `{PROJECT}.ci_brand_memory.{name}` t LIMIT {MAX_ROWS}) {name},
 (SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{name}`) {name}_count''' for name in NAMES)


@dataclass(frozen=True,slots=True)
class CoverageRead:
    captures: tuple[EvidenceCoverage,...]
    source_sha256: str
    native_job_id: str
    observed_at: datetime
    valid_until: datetime


def decode_catalog(row):
    _require(type(row) is dict and set(row)=={'observed_at','scope_sha256'}|set(NAMES)|{n+'_count' for n in NAMES},
        'GAP_CATALOG_SHAPE')
    decoded={};total=0;fingerprint={}
    for name in NAMES:
        values=row[name];count=row[name+'_count']
        _require(type(count) is int and 0<=count<MAX_ROWS and type(values) is list and len(values)==count,
            'GAP_CATALOG_OVERFLOW_OR_PARTIAL')
        _require(all(type(v) is str and len(v.encode())<=262144 for v in values),'GAP_CATALOG_ROW_SIZE')
        total+=sum(len(v.encode()) for v in values);_require(total<=MAX_BYTES,'GAP_CATALOG_SIZE')
        fingerprint[name]=sorted(values);records=[]
        for text in values:
            item=json.loads(text);item['recorded_at']=datetime.fromisoformat(item['recorded_at'].replace('Z','+00:00'))
            _require(_utc(item['recorded_at'])<=_utc(row['observed_at']),'GAP_CATALOG_FUTURE_ROW')
            records.append(item)
        decoded[name]=records
    states={}
    for kind,prefix in PREFIX.items():
        heads=decoded[prefix+'_heads'];versions=decoded[prefix+'_versions']
        ids={r['record_id'] for r in heads}|{r['record_id'] for r in versions}
        _require(len(ids)<=32,'GAP_CATALOG_RECORD_OVERFLOW')
        for rid in sorted(ids):
            h=[r for r in heads if r['record_id']==rid];history=tuple(sorted(
                (r for r in versions if r['record_id']==rid),key=lambda r:r['revision']))
            _require(len(h)==1 and bool(history),'GAP_CATALOG_ORPHAN_OR_DUPLICATE')
            states[kind,rid]=decode_state(h[0],history,kind=kind,record_id=rid)
    return states,_hash(fingerprint)


def _disposition(record,kind,now):
    approval=record.approval if kind!='EVIDENCE' else record.review
    if approval.status is PolicyStatus.REVOKED:return 'REJECTED'
    if approval.status is PolicyStatus.DRAFT:return 'PENDING_REVIEW'
    if not approval.effective_at<=now<approval.valid_until:return 'STALE'
    return 'CURRENT_APPROVED'


def project_coverage(states,rule,scope,*,formula_version,audience,now):
    # Existing permission codecs deliberately remain scoped to the original 20.
    if scope['sku'] not in {f'CI-SKU-{i:04d}' for i in range(1,21)}:return 'UNAVAILABLE',()
    kind={'SCIENTIFIC_CLAIM_PERMISSION':'CLAIM','CURRENT_OFFER':'OFFER'}.get(rule['evidence_kind'])
    if kind is None:return 'UNAVAILABLE',()
    candidates=[]
    for (k,_),state in states.items():
        if k!=kind:continue
        record=state.current
        if kind=='CLAIM' and record.sku!=scope['sku']:continue
        if kind=='OFFER' and scope['sku'] not in record.skus:continue
        if scope['country'] not in record.countries or scope['channel'] not in record.channels:continue
        if audience not in record.audiences:continue
        if kind=='CLAIM' and record.kind not in {ClaimKind.CLINICAL_RESULT,ClaimKind.CONSUMER_PERCEPTION}:continue
        if kind=='CLAIM' and (formula_version is None or formula_version!=record.formula_version):
            candidates.append(('CONFLICT',()));continue
        status=_disposition(record,kind,now)
        if status!='CURRENT_APPROVED':candidates.append((status,()));continue
        if kind=='OFFER':
            # Structured terms and eligibility exist; free-text qualifications do not
            # establish complete structured exclusions, price or stock records.
            fields=('offer_terms','eligibility','starts_at','ends_at')
        else:
            evidence=tuple(states['EVIDENCE',pin.evidence_id] for pin in record.evidence
                if ('EVIDENCE',pin.evidence_id) in states)
            try:reconcile_claim_evidence(state,evidence,checked_at=now)
            except ValueError:candidates.append(('CONFLICT',()));continue
            studies=[e.current for e in evidence if e.current.kind in
                {EvidenceKind.FINISHED_PRODUCT_CLINICAL,EvidenceKind.CONSUMER_STUDY}]
            if len(studies)!=1:candidates.append(('CONFLICT',()));continue
            # A reviewed content hash does not establish a retrievable full study report.
            fields=('formula_version','study_endpoint','study_population','study_duration','approved_wording','qualifications')
        candidates.append(('CURRENT_APPROVED',tuple(f for f in rule['required_fields'] if f in fields)))
    if not candidates:return 'ABSENT',()
    # No latest/best-match selection over conflicting permissions or different offers.
    if len(candidates)!=1:return 'CONFLICT',()
    return candidates[0]


def read_coverage(client,policy_json,*,scope,features,formula_version,audience,identity,access_reader,
        clock=lambda:datetime.now(timezone.utc)):
    policy=validate_policy(policy_json)
    allowed={f for r in policy['requirements'] for f in r['when_any']}
    _require(type(features) is tuple and len(features)==len(set(features)) and set(features)<=allowed,
        'GAP_FEATURES_UNRESOLVED')
    _require(type(audience) is str and audience in {'NEW','RETURNING'},'GAP_AUDIENCE_UNRESOLVED')
    _require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,
        'GAP_CLIENT_SCOPE')
    before=_access(access_reader,identity,scope['workspace_id'],scope['conversation_id']);started=_utc(clock())
    for name in NAMES:
        expected=table_metadata(name,PROJECT)
        validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
    cfg=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('scope_sha256','STRING',_hash(scope))],
        use_legacy_sql=False,use_query_cache=False,create_session=False,maximum_bytes_billed=512*1024**2,
        job_timeout_ms=60000)
    native_id='campaign_gap_coverage_'+uuid4().hex
    job=operation_call(client.query,SQL,job_config=cfg,job_id=native_id,location=LOCATION,retry=None,job_retry=None,timeout=20)
    operation_call(job.result,max_results=0,retry=None,job_retry=None,timeout=60)
    native=operation_call(client.get_job,native_id,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(native,job_id=native_id,sql=SQL,config=cfg) and native.state=='DONE'
        and native.error_result is None,'GAP_NATIVE_MISMATCH')
    rows=list(islice(operation_iter(operation_call(native.result,max_results=2,page_size=2,retry=None,job_retry=None,timeout=20)),2))
    _require(len(rows)==1,'GAP_CATALOG_RESULT');row=dict(rows[0]);now=_utc(clock())
    _require(row['scope_sha256']==_hash(scope) and started<=row['observed_at']<=now<started+timedelta(seconds=60),
        'GAP_CAPTURE_EXPIRED')
    states,source_sha=decode_catalog(row)
    active=[r for r in policy['requirements'] if set(features)&set(r['when_any'])]
    captures=[];structured_cache={};structured_pins=[]
    for rule in active:
        kind=rule['evidence_kind'];observed=row['observed_at'];capture_job=native_id;capture_until=started+timedelta(seconds=60)
        if kind in STRUCTURED_FIELDS:
            if kind not in structured_cache:
                structured_cache[kind]=read_structured(client,scope=scope,kind=kind,formula_version=formula_version,
                    audience=audience,required_fields=tuple(sorted(STRUCTURED_FIELDS[kind])),identity=identity,
                    access_reader=access_reader,clock=clock)
                structured_pins.append(dict(kind=kind,sha256=structured_cache[kind].source_sha256))
            source=structured_cache[kind];status=source.state
            fields=tuple(f for f in rule['required_fields'] if f in source.available_fields)
            observed=source.observed_at;capture_job=source.native_job_id;capture_until=min(capture_until,source.valid_until)
        else:
            status,fields=project_coverage(states,rule,scope,formula_version=formula_version,audience=audience,now=now)
            if kind=='CURRENT_OFFER' and status=='CURRENT_APPROVED' and 'exclusions' in rule['required_fields']:
                offers=[s.current for (k,_),s in states.items() if k=='OFFER' and scope['sku'] in s.current.skus
                    and scope['country'] in s.current.countries and scope['channel'] in s.current.channels
                    and audience in s.current.audiences]
                _require(len(offers)==1,'GAP_OFFER_SOURCE_CONFLICT');offer=offers[0]
                source=read_structured(client,scope=scope,kind='OFFER_EXCLUSIONS',formula_version=formula_version,
                    audience=audience,required_fields=('exclusions',),identity=identity,access_reader=access_reader,clock=clock,
                    permission_record_id=offer.offer_id,expected_exclusions='\n'.join(offer.qualifications))
                structured_pins.append(dict(kind='OFFER_EXCLUSIONS',sha256=source.source_sha256))
                if source.state=='CURRENT_APPROVED':
                    fields+=source.available_fields;capture_until=min(capture_until,source.valid_until,offer.approval.valid_until)
                elif source.state!='ABSENT':status=source.state;fields=()
        captures.append(EvidenceCoverage(rule['requirement_id'],_hash(scope),status,fields,observed,
            capture_until,capture_job))
    _require(before==_access(access_reader,identity,scope['workspace_id'],scope['conversation_id'])
        and _utc(clock())<started+timedelta(seconds=60),'GAP_ACCESS_CHANGED_OR_EXPIRED')
    return CoverageRead(tuple(captures),_hash(dict(permissions=source_sha,structured=structured_pins)),native_id,row['observed_at'],min([started+timedelta(seconds=60)]+[c.valid_until for c in captures]))
