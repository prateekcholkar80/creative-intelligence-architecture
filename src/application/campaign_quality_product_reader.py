"""Read current catalog identity facts for E2; no claims, fallback or release.

This separate adapter does not widen the legacy BEAUTYFEEDS/permission readers.
Only server composition may supply clients, identity and the access reader.
"""
from dataclasses import dataclass
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import islice
from uuid import uuid4

from google.cloud import bigquery
from src.application.access_policy import AccessDecision, Action
from src.application.identity import ensure_fresh
from src.application.campaign_quality_checks import SourceFact, QualityCandidate, check_candidate
from src.application.campaign_acceptance_contract import _utc, _hash
from src.application.workflow_contract import _require, _id, canonical_json_bytes
from src.generators.sales_generator import PROJECT, LOCATION

ORIGINAL_SKUS = tuple(f'CI-SKU-{number:04}' for number in range(1,21))
OFFICIAL_SKUS = ('CI-SKU-0021', 'CI-SKU-0022', 'CI-SKU-0023')
SKUS = ORIGINAL_SKUS + OFFICIAL_SKUS
COLUMNS = ('sku', 'brand', 'product_name', 'size_value', 'size_unit', 'product_status', 'source_system')
SQL = f"SELECT {', '.join(COLUMNS)} FROM `{PROJECT}.ci_product.product_master` WHERE sku=@sku LIMIT 2"


@dataclass(frozen=True, slots=True)
class ProductFactRead:
    fact: SourceFact
    native_job_id: str


def read_product_fact(client, sku, *, clock=lambda: datetime.now(timezone.utc)):
    """Fresh exact-SKU read, independently correlate original native SQL/config."""
    _require(type(sku) is str and sku in SKUS, 'QUALITY_PRODUCT_SCOPE_UNSUPPORTED')
    _require(client.project == PROJECT, 'QUALITY_PRODUCT_PROJECT_MISMATCH')
    started = _utc(clock())
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('sku', 'STRING', sku)],
        use_legacy_sql=False, create_session=False, use_query_cache=False,
        maximum_bytes_billed=64*1024*1024, job_timeout_ms=60000)
    expected = deepcopy(cfg.to_api_repr())
    native_id = 'quality_product_read_' + uuid4().hex
    job = client.query(SQL, job_config=cfg, job_id=native_id, location=LOCATION,
        retry=None, job_retry=None, timeout=20)
    rows = tuple(dict(row) for row in islice(job.result(timeout=60, max_results=2, page_size=2,
        retry=None, job_retry=None), 2))
    native = client.get_job(native_id, location=LOCATION, retry=None, timeout=20)
    actual = native.to_api_repr().get('configuration', {})
    query_config = dict(actual.get('query', {}))
    native_sql = query_config.pop('query', None)
    _require(native.project == PROJECT and native.location.lower() == LOCATION
        and native.job_id == native_id and native.query == SQL and native_sql == SQL and native.state == 'DONE'
        and getattr(native,'parent_job_id',None) is None
        and native.error_result is None and actual.get('jobTimeoutMs') == '60000'
        and actual.get('dryRun',False) is False and not query_config.get('connectionProperties')
        and all(query_config.get(key,False if key in {'createSession','useLegacySql'} else None)==value
            for key,value in expected['query'].items()),
        'QUALITY_PRODUCT_NATIVE_MISMATCH')
    ended = _utc(clock())
    _require(started <= ended < started + timedelta(seconds=60), 'QUALITY_PRODUCT_READ_EXPIRED')
    _require(len(rows) == 1 and set(rows[0]) == set(COLUMNS), 'QUALITY_PRODUCT_ROW_COVERAGE')
    row = rows[0]
    official = sku in OFFICIAL_SKUS
    _require(row['sku'] == sku and row['product_status'] == 'ACTIVE'
        and type(row['brand']) is str and 0 < len(row['brand']) <= 512 and row['brand'].strip() == row['brand']
        and (row['brand'] == 'Naked & Thriving' and row['source_system'] == 'NAKED_AND_THRIVING_OFFICIAL'
             if official else row['source_system'] == 'BEAUTYFEEDS')
        and type(row['product_name']) is str and 0 < len(row['product_name']) <= 512
        and row['product_name'].strip() == row['product_name'], 'QUALITY_PRODUCT_ROW_INVALID')
    fields = dict(product_name=row['product_name'])
    if not official and row['size_value'] is None and row['size_unit'] is None:
        # Legacy imported names may contain size wording; never parse it into
        # an independently authoritative numeric size or fill a missing field.
        pass
    else:
        _require(type(row['size_value']) is Decimal and row['size_value'].is_finite()
            and 0 < row['size_value'] <= 10000 and row['size_unit'] == 'mL', 'QUALITY_PRODUCT_ROW_INVALID')
        fields['full_size_ml'] = format(row['size_value'].normalize(), 'f')
    fact = SourceFact(native_id, sku, canonical_json_bytes(fields).decode(),
        started, started + timedelta(seconds=60), True)
    return ProductFactRead(fact, native_id)


def _access(reader, identity, workspace_id, conversation_id):
    ensure_fresh(identity)
    decision = reader.authorize(identity, Action.READ_DRAFT, workspace_id=workspace_id,
        conversation_id=conversation_id)
    _require(type(decision) is AccessDecision and decision.allowed is True
        and decision.action is Action.READ_DRAFT and decision.workspace_id == workspace_id
        and decision.resource_id == conversation_id and decision.principal_id == identity.principal.principal_id
        and type(decision.membership_revision) is int and decision.membership_revision > 0
        and type(decision.resource_revision) is int and decision.resource_revision > 0,
        'QUALITY_PRODUCT_ACCESS_DENIED')
    return decision


def check_current_product(sku, build_candidate, *, client, access_reader, identity, workspace_id,
                          conversation_id, clock=lambda: datetime.now(timezone.utc)):
    """Protected fact check only. Never returns candidate text or a release grant.

    Fresh access on both sides of the source read catches revocation and changed
    conversation scope. This is not an atomic access/source snapshot, nor a
    substitute for another authoritative access check at eventual release.
    """
    _require(type(sku) is str and sku in SKUS and callable(build_candidate), 'INVALID_QUALITY_CANDIDATE')
    _id(workspace_id); _id(conversation_id)
    before = _access(access_reader, identity, workspace_id, conversation_id)
    read = read_product_fact(client, sku, clock=clock)
    # Installed server adapter annotates exact original text using current pins.
    # No untrusted model callback or automatic repair of existing citations.
    candidate = build_candidate(read.fact)
    _require(type(candidate) is QualityCandidate and candidate.sku == sku, 'QUALITY_PRODUCT_CANDIDATE_SCOPE')
    after = _access(access_reader, identity, workspace_id, conversation_id)
    _require(before == after, 'QUALITY_PRODUCT_ACCESS_CHANGED')
    report = check_candidate(candidate, (read.fact,), checked_at=_utc(clock()), not_before=read.fact.observed_at)
    return dict(report=report, source=read.fact, native_job_id=read.native_job_id,
        context_sha256=_hash(dict(candidate=candidate.sha256, report=report.sha256,
            access=after.to_dict(), native_job_id=read.native_job_id)), release_allowed=False)
