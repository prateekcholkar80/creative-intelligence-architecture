"""Native-correlated corpus coverage, never retrieval relevance or approval.

Counts inspect current SKU-scoped POC views, not generation eligibility. Callers
must check application access around this read before exposing its result.
"""
from datetime import datetime, timedelta, timezone
from copy import deepcopy
import hashlib
import re
from itertools import islice
from time import monotonic
from uuid import uuid4

from google.cloud import bigquery

from src.application.campaign_evaluator_store import sha
from src.application.operation_budget import operation_call, operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION

VERSION = 'campaign-retrieval-corpus-coverage-v1'
SKUS = tuple(f'CI-SKU-{i:04}' for i in range(1, 24))
COUNTS = ('knowledge_chunks', 'text_vectors', 'image_vectors')
SQL = f'''SELECT CURRENT_TIMESTAMP() observed_at,
 ARRAY(SELECT AS STRUCT sku, product_status, source_system
   FROM `{PROJECT}.ci_product.product_master` WHERE sku=@sku LIMIT 2) products,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_product.product_master` WHERE sku=@sku) product_count,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.poc_generation_knowledge_chunks` WHERE sku=@sku) knowledge_chunks,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.poc_generation_text_embeddings` WHERE sku=@sku) text_vectors,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.poc_image_embeddings` WHERE sku=@sku) image_vectors'''


def _config(sku):
    return bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False, create_session=False,
        maximum_bytes_billed=128*1024**2, job_timeout_ms=60000,
        query_parameters=[bigquery.ScalarQueryParameter('sku', 'STRING', sku)])


def coverage_result(sku, row):
    """Empty is a measured source state; missing/inconsistent rows are errors."""
    _require(sku in SKUS and type(row) is dict and set(row) ==
        {'observed_at', 'products', 'product_count', *COUNTS}, 'RETRIEVAL_COVERAGE_SCHEMA')
    expected = 'NAKED_AND_THRIVING_OFFICIAL' if sku in SKUS[-3:] else 'BEAUTYFEEDS'
    _require(type(row['product_count']) is int and row['product_count'] == 1
        and row['products'] == [dict(sku=sku, product_status='ACTIVE', source_system=expected)],
        'RETRIEVAL_COVERAGE_PRODUCT')
    _require(all(type(row[k]) is int and row[k] >= 0 for k in COUNTS), 'RETRIEVAL_COVERAGE_COUNTS')
    counts = {k: row[k] for k in COUNTS}
    status = ('CORPUS_EMPTY' if not any(counts.values()) else
        'TEXT_VECTORS_MISSING' if counts['knowledge_chunks'] and not counts['text_vectors'] else
        'CORPUS_PRESENT_REQUIRES_ELIGIBILITY_AND_RANKING')
    return dict(version=VERSION, sku=sku, counts=counts, corpus_status=status,
        retrieval_quality='NOT_MEASURED', eligibility='NOT_ESTABLISHED_BY_COUNTS',
        ranking_executed=False, model_calls=0, qualifying=False, release_allowed=False)


def capture_retrieval_coverage(client, sku, *, clock=lambda: datetime.now(timezone.utc)):
    _require(type(sku) is str and sku in SKUS and client.project == PROJECT
        and client.location == LOCATION and client.default_query_job_config is None,
        'RETRIEVAL_COVERAGE_CLIENT_SCOPE')
    started = clock(); timer = monotonic()
    cfg = _config(sku)
    expected = deepcopy(cfg)
    job_id = 'retrieval_coverage_' + uuid4().hex
    job = operation_call(client.query, SQL, job_id=job_id, job_config=cfg, location=LOCATION,
        retry=None, job_retry=None, timeout=20)
    rows = tuple(dict(r) for r in islice(operation_iter(operation_call(job.result, timeout=60, max_results=2,
        retry=None, job_retry=None)), 2))
    native = operation_call(client.get_job, job_id, location=LOCATION, retry=None, timeout=20)
    _require(matches_sharing_job(native, job_id=job_id, sql=SQL, config=expected)
        and native.state == 'DONE' and native.error_result is None and len(rows) == 1,
        'RETRIEVAL_COVERAGE_NATIVE_UNVERIFIED')
    ended = clock(); row = rows[0]
    _require(type(row.get('observed_at')) is datetime and started <= row['observed_at'] <= ended
        < started + timedelta(seconds=60), 'RETRIEVAL_COVERAGE_CAPTURE_EXPIRED')
    value = coverage_result(sku, row)
    telemetry = {name: getattr(native, name, None) for name in ('total_bytes_processed', 'total_bytes_billed', 'slot_millis')}
    _require(all(v is None or type(v) is int and v >= 0 for v in telemetry.values()),
        'RETRIEVAL_COVERAGE_TELEMETRY')
    value.update(native_job_id=job_id, sql_sha256=hashlib.sha256(SQL.encode()).hexdigest(),
        observed_at=row['observed_at'].isoformat(), checked_at=ended.isoformat(),
        latency_ms=round((monotonic()-timer)*1000), native_usage=telemetry)
    return dict(value, sha256=sha(value))


def verify_retrieval_coverage(client, value):
    """Recover the historical original query, not a fresh corpus measurement."""
    _require(type(value) is dict and value.get('sku') in SKUS
        and value.get('sha256') == sha({k:v for k,v in value.items() if k!='sha256'})
        and type(value.get('native_job_id')) is str
        and re.fullmatch(r'retrieval_coverage_[a-f0-9]{32}',value['native_job_id'])
        and client.project == PROJECT and client.location == LOCATION
        and client.default_query_job_config is None,'RETRIEVAL_COVERAGE_RECOVERY_INPUT')
    native = operation_call(client.get_job, value['native_job_id'], location=LOCATION, retry=None, timeout=20)
    _require(matches_sharing_job(native,job_id=value['native_job_id'],sql=SQL,config=_config(value['sku']))
        and native.state=='DONE' and native.error_result is None,'RETRIEVAL_COVERAGE_RECOVERY_NATIVE')
    rows=tuple(dict(r) for r in islice(operation_iter(operation_call(native.result,timeout=60,
        max_results=2,retry=None,job_retry=None)),2))
    _require(len(rows)==1,'RETRIEVAL_COVERAGE_RECOVERY_ROWS')
    original=coverage_result(value['sku'],rows[0])
    _require(all(value.get(k)==v for k,v in original.items())
        and rows[0]['observed_at'].isoformat()==value.get('observed_at')
        and value.get('sql_sha256')==hashlib.sha256(SQL.encode()).hexdigest(),
        'RETRIEVAL_COVERAGE_RECOVERY_CHANGED')
    return value
