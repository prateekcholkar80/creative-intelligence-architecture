"""Pure, versioned evidence projections for future workflow persistence.

Callers must project freshly validated BigQuery reader results, explicitly encode
dates/timestamps and Decimal values as strings, and supply verified image pins.
No source reads, local fallback, approval grants, vectors, image bytes or writes.
Historical evidence never replaces current eligibility or actual GCS checks.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import re
from types import MappingProxyType

from src.application.access_policy import OwnedResource
from src.application.workflow_contract import (
    ARTIFACT_BUCKET, ContractError, _digest, _id, _integer, _json_timestamp,
    _numeric, _require, _scope, _shape, canonical_json_bytes, validate_attempt,
    validate_output,
)
from src.application.workflow_schema import TABLES
from src.generators.sales_generator import PROJECT

EVIDENCE_VERSION = 'workflow-evidence-v1'
MAX_EVIDENCE_ROWS = 256
MAX_SNAPSHOT_BYTES = 2 * 1024 * 1024
SOURCE_TABLES = MappingProxyType({
    'PRODUCT': 'ci_product.product_master',
    'KNOWLEDGE_CHUNK': 'ci_brand_memory.poc_generation_knowledge_chunks',
    'POC_RULE': 'ci_brand_memory.brand_guidelines',
    'POC_CLAIM': 'ci_brand_memory.poc_approved_product_claims',
    'SALES_SUMMARY': 'ci_sales.sku_daily_sales',
    'CAMPAIGN_PERFORMANCE': 'ci_creative.campaign_daily_performance',
    'PRODUCT_IMAGE': 'ci_product.product_assets',
    'CAMPAIGN_IMAGE': 'ci_creative.campaign_assets',
    'GENERATED_IMAGE': 'ci_creative.generated_campaign_assets',
})
CHUNK_TYPES = MappingProxyType({
    'PRODUCT_KNOWLEDGE': ('BEAUTYFEEDS', 'FACTUAL_CONTEXT', 'SOURCE_PRODUCT_FACTS'),
    'INGREDIENT_KNOWLEDGE': ('BEAUTYFEEDS', 'FACTUAL_CONTEXT', 'SOURCE_PRODUCT_FACTS'),
    'POC_APPROVED_CLAIM': ('SYNTHETIC_POC', 'POC_APPROVED', 'SYNTHETIC_POC_ONLY'),
    'POC_RULE': ('SYNTHETIC_POC', 'POC_POLICY', 'SYNTHETIC_POC_ONLY'),
    'WEBSITE_VISUAL_REFERENCE': ('OFFICIAL_BRAND_WEBSITE', 'VISUAL_REFERENCE', 'VISUAL_INSPIRATION_ONLY'),
    'CAMPAIGN_CREATIVE': ('SYNTHETIC_POC', 'POC_CONTEXT', 'SYNTHETIC_POC_ONLY'),
})
SALES_COUNTS = ('units_sold', 'orders', 'row_count', 'day_count')
SALES_MONEY = ('gross_sales', 'discount_amount', 'refund_amount', 'net_sales')
PERFORMANCE_COUNTS = ('impressions', 'clicks', 'conversions', 'attributed_units', 'performance_rows', 'performance_days')
PERFORMANCE_MONEY = ('budget', 'spend', 'attributed_revenue')
RANK_FIELDS = ('rank', 'cosine_distance', 'ranking_version')
VECTOR_FIELDS = ('embedding_model', 'embedding_dimensions')
IMAGE_FIELDS = ('gcs_uri', 'gcs_generation', 'image_sha256')


def evidence_reference_id(reference: str) -> str:
    """Same deterministic citation IDs as the live retrieval packet."""
    _text(reference, 2048)
    return 'EVD-' + hashlib.sha256(reference.encode('utf-8')).hexdigest()[:16]


def evidence_row_id(source_table: str, source_record_id: str, reference: str) -> str:
    """A citation may be supported by multiple records; pin each separately."""
    for value in (source_table, source_record_id, reference):
        _text(value, 2048)
    return 'EVR-' + hashlib.sha256(canonical_json_bytes([source_table, source_record_id, reference])).hexdigest()[:32]


def _text(value: object, maximum: int = 4096) -> None:
    _require(isinstance(value, str) and bool(value.strip()) and len(value) <= maximum
             and all(ord(c) >= 32 and ord(c) != 127 for c in value), 'INVALID_EVIDENCE_TEXT')
    try:
        value.encode('utf-8')
    except UnicodeError:
        raise ContractError('INVALID_EVIDENCE_TEXT') from None


def _fields(record: dict, names: tuple[str, ...]) -> None:
    _require(isinstance(record, dict) and set(record) == set(names), 'INVALID_EVIDENCE_PAYLOAD')


def _number(value: object) -> Decimal:
    # NUMERIC JSON values retain exact strings; floats are never inferred.
    _require(isinstance(value, str) and re.fullmatch(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?', value) is not None,
             'INVALID_EVIDENCE_NUMERIC')
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ContractError('INVALID_EVIDENCE_NUMERIC') from None
    _numeric(result)
    return result


def _date(value: object) -> date:
    _require(isinstance(value, str) and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value) is not None,
             'INVALID_EVIDENCE_DATE')
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ContractError('INVALID_EVIDENCE_DATE') from None


def _campaign(identifier: str, sku: str) -> None:
    _require(isinstance(identifier, str) and re.fullmatch(rf'SYN-CMP-[0-9]{{8}}-{sku}-[0-9]{{2}}', identifier) is not None,
             'INVALID_EVIDENCE_CAMPAIGN')


def _reference(table: str, key: str) -> str:
    return f'{PROJECT}.{table}#{key}'


def _chunk(row: dict, record: dict) -> tuple[str, set[str]]:
    _fields(record, ('chunk_id', 'snapshot_id', 'chunk_type', 'brand', 'sku', 'campaign_id', 'asset_id',
        'claim_id', 'chunk_text', 'source_system', 'source_reference', 'evidence_references', 'image_uris',
        'retrieval_status', 'approval_scope', 'content_sha256'))
    _id(record['chunk_id']); _digest(record['snapshot_id']); _digest(record['content_sha256'])
    _text(record['brand']); _text(record['source_reference'], 2048)
    kind = record['chunk_type']
    _require(isinstance(kind, str) and kind in CHUNK_TYPES, 'INVALID_EVIDENCE_CHUNK_TYPE')
    _require(tuple(record[k] for k in ('source_system', 'retrieval_status', 'approval_scope')) == CHUNK_TYPES[kind],
             'INVALID_EVIDENCE_PROVENANCE')
    text = record['chunk_text']
    _require(isinstance(text, str) and bool(text.strip()) and len(text) <= 8192
             and all(ord(c) >= 32 or c in '\n\t' for c in text) and '\x7f' not in text,
             'INVALID_EVIDENCE_TEXT')
    _require(record['content_sha256'] == hashlib.sha256(text.encode('utf-8')).hexdigest()
             and row['content_sha256'] == record['content_sha256'], 'EVIDENCE_HASH_MISMATCH')
    _require(row['knowledge_snapshot_id'] == record['snapshot_id'], 'EVIDENCE_SNAPSHOT_MISMATCH')
    refs, images = record['evidence_references'], record['image_uris']
    for values in (refs, images):
        _require(isinstance(values, list) and len(values) <= 32, 'INVALID_EVIDENCE_REFERENCES')
        for value in values:
            _text(value, 2048)
        _require(len(set(values)) == len(values), 'INVALID_EVIDENCE_REFERENCES')
    _require(bool(refs), 'INVALID_EVIDENCE_REFERENCES')
    global_kind = kind in {'POC_RULE', 'WEBSITE_VISUAL_REFERENCE'}
    _require(record['sku'] is None if global_kind else record['sku'] == row['canonical_sku'], 'EVIDENCE_SKU_MISMATCH')
    for key in ('campaign_id', 'asset_id', 'claim_id'):
        if record[key] is not None:
            _id(record[key])
    _require((record['claim_id'] is not None) == (kind == 'POC_APPROVED_CLAIM'), 'INVALID_EVIDENCE_CLAIM')
    _require((record['campaign_id'] is not None and record['asset_id'] is not None) if kind == 'CAMPAIGN_CREATIVE'
             else record['campaign_id'] is None and record['asset_id'] is None, 'INVALID_EVIDENCE_CAMPAIGN')
    if kind == 'CAMPAIGN_CREATIVE':
        _campaign(record['campaign_id'], row['canonical_sku'])
        _require(record['asset_id'] == f'SYN-AST-{record["campaign_id"]}-template-v1', 'INVALID_EVIDENCE_CAMPAIGN')
        expected = _reference('ci_creative.campaign_master', f'campaign_id={record["campaign_id"]}')
    elif kind in {'PRODUCT_KNOWLEDGE', 'INGREDIENT_KNOWLEDGE'}:
        table = 'product_master' if kind == 'PRODUCT_KNOWLEDGE' else 'product_ingredients'
        expected = _reference(f'ci_product.{table}', f'sku={row["canonical_sku"]}')
    elif kind == 'POC_APPROVED_CLAIM':
        expected = _reference('ci_brand_memory.product_claims', f'claim_id={record["claim_id"]}')
    elif kind == 'POC_RULE':
        _require(record['source_reference'].startswith(_reference('ci_brand_memory.brand_guidelines', 'rule_id=POC-')),
                 'INVALID_EVIDENCE_SOURCE')
        expected = record['source_reference']
    else:
        _require(record['brand'] == 'Naked & Thriving' and record['source_reference'] in {
            f'data/research/naked_and_thriving_visual_reference.json#{s}'
            for s in ('typography', 'colors', 'layout', 'creative_patterns')}, 'INVALID_EVIDENCE_SOURCE')
        expected = record['source_reference']
    _require(record['source_reference'] == expected, 'INVALID_EVIDENCE_SOURCE')
    if global_kind or kind == 'POC_APPROVED_CLAIM':
        _require(not images, 'INVALID_EVIDENCE_IMAGE')
    else:
        _require(f'gs://{ARTIFACT_BUCKET}/products/{row["canonical_sku"]}/primary.jpg' in images,
                 'INVALID_EVIDENCE_IMAGE')
    return record['chunk_id'], set(refs) | {record['source_reference']}


def _analytics(row: dict, record: dict) -> tuple[str, set[str]]:
    sales = row['source_kind'] == 'SALES_SUMMARY'
    counts, money = (SALES_COUNTS, SALES_MONEY) if sales else (PERFORMANCE_COUNTS, PERFORMANCE_MONEY)
    common = ('sku', 'source_system', 'currency', 'start_date', 'end_date')
    extra = ('channel',) if sales else ('campaign_id', 'asset_id', 'attribution_model', 'ctr', 'conversion_rate', 'roas')
    _fields(record, common + extra + counts + money)
    _require(record['sku'] == row['canonical_sku'] and record['source_system'] == 'SYNTHETIC_POC',
             'INVALID_EVIDENCE_PROVENANCE')
    _require(isinstance(record['currency'], str) and re.fullmatch(r'[A-Z]{3}', record['currency']) is not None,
             'INVALID_EVIDENCE_CURRENCY')
    start, end = _date(record['start_date']), _date(record['end_date'])
    _require(end >= start, 'INVALID_EVIDENCE_DATE')
    days = (end-start).days+1
    for name in counts:
        _integer(record[name], 0)
        _require(record[name] < 2**63, 'INVALID_INTEGER')
    values = {name: _number(record[name]) for name in money}
    _require(all(value >= 0 for value in values.values()), 'INVALID_EVIDENCE_NUMERIC')
    with localcontext() as context:
        context.prec = 80
        if sales:
            _require(record['channel'] == 'ECOMMERCE' and record['row_count'] == record['day_count'] == days
                     and record['orders'] <= record['units_sold']
                     and values['net_sales'] == values['gross_sales']-values['discount_amount']-values['refund_amount'],
                     'INVALID_SALES_SUMMARY')
            key = (f'sku={row["canonical_sku"]}&channel=ECOMMERCE&source_system=SYNTHETIC_POC'
                   f'&start_date={start.isoformat()}&end_date={end.isoformat()}')
        else:
            _campaign(record['campaign_id'], row['canonical_sku'])
            _require(record['asset_id'] == f'SYN-AST-{record["campaign_id"]}-template-v1'
                     and record['attribution_model'] == 'SYNTHETIC_SALES_CAPPED_ATTRIBUTION'
                     and record['performance_rows'] == record['performance_days'] == days
                     and record['conversions'] <= record['clicks'] <= record['impressions']
                     and record['attributed_units'] >= record['conversions'] and values['spend'] == values['budget'],
                     'INVALID_PERFORMANCE_SUMMARY')
            for name, numerator, denominator in (('ctr', record['clicks'], record['impressions']),
                    ('conversion_rate', record['conversions'], record['clicks']),
                    ('roas', values['attributed_revenue'], values['spend'])):
                actual = None if record[name] is None else _number(record[name])
                expected = (Decimal(numerator)/Decimal(denominator)).quantize(Decimal('.000000001'),
                    rounding='ROUND_HALF_UP') if denominator else None
                _require(actual == expected, 'INVALID_PERFORMANCE_RATIO')
            key = (f'campaign_id={record["campaign_id"]}&asset_id={record["asset_id"]}&source_system=SYNTHETIC_POC'
                   f'&start_date={start.isoformat()}&end_date={end.isoformat()}')
    return key, {_reference(SOURCE_TABLES[row['source_kind']], key)}


def _image(row: dict, record: dict) -> tuple[str, set[str]]:
    _fields(record, ('sku', 'asset_id', 'campaign_id', 'source_system', 'gcs_uri', 'gcs_generation',
        'content_sha256', 'mime_type', 'width', 'height', 'description', 'embedding_id',
        'model_id', 'model_location', 'dimension', 'task_type', 'snapshot_id', 'review',
        'original_product_image', 'usage_scope'))
    _require(record['sku'] == row['canonical_sku'], 'EVIDENCE_SKU_MISMATCH')
    _id(record['asset_id']); _id(record['embedding_id']); _text(record['description'], 8192)
    _digest(record['content_sha256'])
    for name in ('width', 'height'):
        _integer(record[name])
        _require(record[name] <= 20000, 'INVALID_EVIDENCE_IMAGE')
    generation = record['gcs_generation']
    _require(isinstance(generation, str) and re.fullmatch(r'[1-9][0-9]{0,19}', generation) is not None,
             'INVALID_EVIDENCE_GENERATION')
    _require(tuple(row[k] for k in IMAGE_FIELDS) == (record['gcs_uri'], generation, record['content_sha256'])
             and row['content_sha256'] == record['content_sha256'], 'EVIDENCE_IMAGE_MISMATCH')
    _require(record['model_id'] == row['embedding_model'] == 'multimodalembedding@001'
             and type(record['dimension']) is int and record['dimension'] == row['embedding_dimensions']
             and record['dimension'] in (128, 256, 512, 1408)
             and record['task_type'] == 'IMAGE' and record['model_location'] == 'asia-south1',
             'INVALID_EVIDENCE_VECTOR')
    kind, sku = row['source_kind'], row['canonical_sku']
    if kind == 'PRODUCT_IMAGE':
        _require(record['asset_id'] == f'AST-{sku}-PRIMARY' and record['campaign_id'] is None
                 and record['mime_type'] == 'image/jpeg' and record['review'] is None
                 and record['snapshot_id'] is None and record['original_product_image'] is None
                 and record['usage_scope'] == 'SOURCE_PRODUCT_IMAGE', 'INVALID_EVIDENCE_IMAGE')
        expected_uri = f'gs://{ARTIFACT_BUCKET}/products/{sku}/primary.jpg'
    elif kind == 'CAMPAIGN_IMAGE':
        _campaign(record['campaign_id'], sku)
        _require(record['asset_id'] == f'SYN-AST-{record["campaign_id"]}-template-v1' and record['review'] is None
                 and record['original_product_image'] is None and record['usage_scope'] == 'SYNTHETIC_POC_ONLY',
                 'INVALID_EVIDENCE_IMAGE')
        expected_uri = f'gs://{ARTIFACT_BUCKET}/campaigns/{record["campaign_id"]}/template-v1/{record["content_sha256"]}.png'
    else:
        _require(record['asset_id'] == f'SYN-GEN-{sku}-{record["content_sha256"]}' and record['campaign_id'] is None,
                 'INVALID_EVIDENCE_IMAGE')
        review = record['review']
        _fields(review, ('review_id', 'revision', 'status', 'approval_scope'))
        _id(review['review_id']); _integer(review['revision'])
        _require(review['revision'] == row['review_revision'] and review['status'] == 'APPROVED_FOR_POC'
                 and review['approval_scope'] == 'POC_VISUAL_REUSE_ONLY'
                 and row['approval_reference'] == _reference('ci_creative.generated_asset_reviews', f'review_id={review["review_id"]}'),
                 'INVALID_EVIDENCE_REVIEW')
        original = record['original_product_image']
        _fields(original, ('asset_id', 'gcs_uri', 'gcs_generation', 'content_sha256'))
        _digest(original['content_sha256'])
        _require(original['asset_id'] == f'AST-{sku}-PRIMARY'
                 and original['gcs_uri'] == f'gs://{ARTIFACT_BUCKET}/products/{sku}/primary.jpg'
                 and isinstance(original['gcs_generation'], str)
                 and re.fullmatch(r'[1-9][0-9]{0,19}', original['gcs_generation']) is not None
                 and record['usage_scope'] == 'NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE',
                 'INVALID_EVIDENCE_IMAGE')
        expected_uri = f'gs://{ARTIFACT_BUCKET}/generated_campaigns/{sku}/grounded-banner-preview-v1/{record["content_sha256"]}.png'
    if kind != 'PRODUCT_IMAGE':
        _digest(record['snapshot_id'])
        _require(record['snapshot_id'] == row['knowledge_snapshot_id'] and record['mime_type'] == 'image/png'
                 and (record['width'], record['height']) == (1200, 628), 'INVALID_EVIDENCE_IMAGE')
    _require(record['gcs_uri'] == expected_uri, 'INVALID_EVIDENCE_IMAGE')
    key = f'asset_id={record["asset_id"]}'
    refs = {_reference(SOURCE_TABLES[kind], key),
            _reference('ci_brand_memory.image_embeddings', f'embedding_id={record["embedding_id"]}')}
    if row['approval_reference']:
        refs.add(row['approval_reference'])
    return key, refs


def _source(row: dict, record: dict) -> tuple[str, set[str]]:
    kind, sku = row['source_kind'], row['canonical_sku']
    if kind == 'KNOWLEDGE_CHUNK':
        return _chunk(row, record)
    if kind in {'SALES_SUMMARY', 'CAMPAIGN_PERFORMANCE'}:
        return _analytics(row, record)
    if kind.endswith('_IMAGE'):
        return _image(row, record)
    if kind == 'PRODUCT':
        _fields(record, ('sku', 'brand', 'product_name', 'product_type', 'price', 'currency', 'primary_image_uri', 'source_system'))
        for key in ('brand', 'product_name', 'product_type', 'currency'):
            _text(record[key])
        _require(record['sku'] == sku and _number(record['price']) >= 0
                 and re.fullmatch(r'[A-Z]{3}', record['currency']) is not None
                 and record['primary_image_uri'] == f'gs://{ARTIFACT_BUCKET}/products/{sku}/primary.jpg',
                 'INVALID_EVIDENCE_PRODUCT')
        key = f'sku={sku}'
        return key, {_reference('ci_product.product_master', key),
                     _reference('ci_product.product_master', key+'&field=product_name')}
    if kind == 'POC_RULE':
        _fields(record, ('rule_id', 'brand', 'scope', 'authority', 'rule_status', 'rule_text', 'source_reference', 'source_system'))
        _id(record['rule_id']); _text(record['brand']); _text(record['rule_text'], 8192)
        _text(record['source_reference'], 2048)
        _require(record['rule_id'].startswith('POC-') and record['scope'] == 'PROJECT_POC'
                 and record['authority'] == 'PROJECT_POC_POLICY' and record['rule_status'] == 'ACTIVE_FOR_POC',
                 'INVALID_EVIDENCE_POLICY')
        key = f'rule_id={record["rule_id"]}'
        return key, {_reference('ci_brand_memory.brand_guidelines', key), record['source_reference']}
    _fields(record, ('claim_id', 'sku', 'claim_type', 'claim_text', 'evidence_reference', 'evidence_excerpt',
        'evidence_source_system', 'approval_status', 'approval_scope', 'approved_by', 'approval_reference',
        'approved_at', 'source_system'))
    _id(record['claim_id']); _text(record['claim_text'])
    _require(record['sku'] == sku and record['claim_type'] == 'PRODUCT_NAME'
             and record['evidence_excerpt'] == record['claim_text'] and record['evidence_source_system'] == 'BEAUTYFEEDS'
             and record['evidence_reference'] == _reference('ci_product.product_master', f'sku={sku}&field=product_name')
             and record['approval_status'] == 'APPROVED' and record['approval_scope'] == 'SYNTHETIC_POC_ONLY'
             and record['approved_by'] == 'SYNTHETIC_POC_REVIEWER'
             and record['approval_reference'] == row['approval_reference'] == 'config/brand_memory.json#review_fixture'
             and _json_timestamp(record['approved_at']) <= row['validated_at'], 'INVALID_EVIDENCE_CLAIM')
    key = f'claim_id={record["claim_id"]}'
    return key, {_reference('ci_brand_memory.product_claims', key), record['evidence_reference'], record['approval_reference']}


def validate_evidence_snapshot(row: dict, *, request: dict, job: dict, attempt: dict,
                               conversation: OwnedResource) -> None:
    """Validate one append-only projection; supplied rows are not eligibility proof."""
    _shape(row, TABLES['evidence_snapshots'].schema)
    validate_attempt(attempt, request=request, job=job, conversation=conversation)
    _scope(row, conversation)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'job_id', 'request_id'):
        _require(row[key] == attempt[key], 'EVIDENCE_PARENT_MISMATCH')
    _require(row['attempt_id'] == attempt['attempt_id'] and row['canonical_sku'] == job['canonical_sku'],
             'EVIDENCE_PARENT_MISMATCH')
    for key in ('evidence_snapshot_id', 'evidence_ref_id', 'source_record_id'):
        _id(row[key], maximum=512 if key == 'source_record_id' else 128)
    _require(attempt['started_at'] <= row['validated_at'] <= row['created_at'] < job['deadline_at'],
             'INVALID_TIME_ORDER')
    if attempt['completed_at'] is not None:
        _require(row['created_at'] <= attempt['completed_at'], 'INVALID_TIME_ORDER')
    _require(row['evidence_schema_version'] == EVIDENCE_VERSION, 'INVALID_EVIDENCE_VERSION')
    kind = row['source_kind']
    _require(kind in SOURCE_TABLES, 'INVALID_EVIDENCE_SOURCE')
    _digest(row['content_sha256'])
    for key in ('source_revision', 'knowledge_snapshot_id'):
        if row[key] is not None:
            (_digest if key == 'knowledge_snapshot_id' else _id)(row[key])
    payload = row['evidence_payload']
    _fields(payload, ('reference', 'record'))
    _require(row['evidence_ref_id'] == evidence_row_id(row['source_table'], row['source_record_id'],
             payload['reference']), 'EVIDENCE_REFERENCE_MISMATCH')
    record = payload['record']
    _require(isinstance(record, dict), 'INVALID_EVIDENCE_PAYLOAD')
    table = ('ci_brand_memory.poc_visual_reference_chunks'
             if kind == 'KNOWLEDGE_CHUNK' and record.get('chunk_type') == 'WEBSITE_VISUAL_REFERENCE'
             else SOURCE_TABLES[kind])
    _require(row['source_table'] == f'{PROJECT}.{table}', 'INVALID_EVIDENCE_SOURCE')
    source = ('BEAUTYFEEDS' if kind in {'PRODUCT', 'PRODUCT_IMAGE'} else
              record.get('source_system') if kind == 'KNOWLEDGE_CHUNK' else 'SYNTHETIC_POC')
    _require(row['source_system'] == record.get('source_system') == source, 'INVALID_EVIDENCE_PROVENANCE')
    review_kind = kind in {'POC_CLAIM', 'GENERATED_IMAGE'}
    if not review_kind:
        _require(row['approval_reference'] is None and row['review_revision'] is None, 'INVALID_EVIDENCE_REVIEW')
    elif kind == 'POC_CLAIM':
        _require(row['review_revision'] is None, 'INVALID_EVIDENCE_REVIEW')  # Existing mock claims have no revision field.
    if not kind.endswith('_IMAGE'):
        _require(all(row[key] is None for key in IMAGE_FIELDS), 'INVALID_EVIDENCE_IMAGE')
    ranked = kind in {'CAMPAIGN_IMAGE', 'GENERATED_IMAGE'} or (kind == 'KNOWLEDGE_CHUNK' and record.get('chunk_type') == 'CAMPAIGN_CREATIVE')
    if ranked:
        _integer(row['rank']); _id(row['ranking_version'])
        _require(row['cosine_distance'] is not None and -1e-12 <= row['cosine_distance'] <= 2+1e-12,
                 'INVALID_EVIDENCE_RANK')
    else:
        _require(all(row[key] is None for key in RANK_FIELDS), 'INVALID_EVIDENCE_RANK')
    vector_required = ranked or kind == 'PRODUCT_IMAGE'
    if vector_required:
        expected_model = 'gemini-embedding-001' if kind == 'KNOWLEDGE_CHUNK' else 'multimodalembedding@001'
        _require(row['embedding_model'] == expected_model and type(row['embedding_dimensions']) is int
                 and row['embedding_dimensions'] in ((768,) if kind == 'KNOWLEDGE_CHUNK' else (128, 256, 512, 1408)),
                 'INVALID_EVIDENCE_VECTOR')
    else:
        _require(all(row[key] is None for key in VECTOR_FIELDS), 'INVALID_EVIDENCE_VECTOR')
    key, references = _source(row, record)
    _require(row['source_record_id'] == key and payload['reference'] in references, 'INVALID_EVIDENCE_SOURCE')
    if kind not in {'KNOWLEDGE_CHUNK', 'PRODUCT_IMAGE', 'CAMPAIGN_IMAGE', 'GENERATED_IMAGE'}:
        _require(row['content_sha256'] == hashlib.sha256(canonical_json_bytes(record)).hexdigest(), 'EVIDENCE_HASH_MISMATCH')
    if kind not in {'KNOWLEDGE_CHUNK', 'CAMPAIGN_IMAGE', 'GENERATED_IMAGE', 'CAMPAIGN_PERFORMANCE'}:
        _require(row['knowledge_snapshot_id'] is None, 'EVIDENCE_SNAPSHOT_MISMATCH')
    if kind == 'CAMPAIGN_PERFORMANCE':
        _digest(row['knowledge_snapshot_id'])


def validate_evidence_bundle(rows: list[dict], *, request: dict, job: dict, attempt: dict,
                             conversation: OwnedResource, output: dict | None = None) -> None:
    """Check a complete projection set and optional output citations; no I/O/lock."""
    _require(isinstance(rows, list) and 0 < len(rows) <= MAX_EVIDENCE_ROWS, 'INVALID_EVIDENCE_BUNDLE')
    seen, sources, snapshots, snapshot_ids, size = set(), {}, set(), set(), 0
    products, claims, chunks, rules, images, performance = {}, {}, [], [], {}, {}
    ranked_chunks = {}
    for row in rows:
        validate_evidence_snapshot(row, request=request, job=job, attempt=attempt, conversation=conversation)
        _require(row['evidence_ref_id'] not in seen, 'DUPLICATE_EVIDENCE_REFERENCE')
        seen.add(row['evidence_ref_id']); snapshot_ids.add(row['evidence_snapshot_id'])
        if row['knowledge_snapshot_id'] is not None:
            snapshots.add(row['knowledge_snapshot_id'])
        record = row['evidence_payload']['record']
        encoded = canonical_json_bytes(row['evidence_payload']); size += len(encoded)
        source_key = row['source_table'], row['source_record_id']
        # Different citation aliases may retain the same authoritative record.
        identity = canonical_json_bytes(record), tuple(row[k] for k in ('source_revision', 'knowledge_snapshot_id',
            'content_sha256', 'approval_reference', 'review_revision', *IMAGE_FIELDS, *VECTOR_FIELDS, *RANK_FIELDS))
        _require(source_key not in sources or sources[source_key] == identity, 'EVIDENCE_SOURCE_CONFLICT')
        sources[source_key] = identity
        kind = row['source_kind']
        if kind == 'PRODUCT':
            products[record['sku']] = record
        elif kind == 'POC_CLAIM':
            claims[record['claim_id']] = record
        elif kind == 'KNOWLEDGE_CHUNK':
            chunks.append(record)
            if record['chunk_type'] == 'CAMPAIGN_CREATIVE':
                previous = ranked_chunks.get(record['campaign_id'])
                _require(previous is None or previous['source_record_id'] == row['source_record_id'],
                         'EVIDENCE_CAMPAIGN_MISMATCH')
                ranked_chunks[record['campaign_id']] = row
        elif kind == 'POC_RULE':
            rules.append(record)
        elif kind.endswith('_IMAGE'):
            images[record['asset_id']] = row
        elif kind == 'CAMPAIGN_PERFORMANCE':
            _require(record['campaign_id'] not in performance or performance[record['campaign_id']] == record,
                     'EVIDENCE_CAMPAIGN_MISMATCH')
            performance[record['campaign_id']] = record
    _require(size <= MAX_SNAPSHOT_BYTES and len(snapshot_ids) == 1 and len(snapshots) == 1,
             'INVALID_EVIDENCE_BUNDLE')
    product = products.get(job['canonical_sku'])
    chunk_types = {c['chunk_type'] for c in chunks}
    _require(product is not None and bool(rules) and any(r['source_kind'] == 'PRODUCT_IMAGE' for r in rows)
             and any(r['source_kind'] == 'SALES_SUMMARY' for r in rows)
             and {'PRODUCT_KNOWLEDGE', 'INGREDIENT_KNOWLEDGE', 'POC_RULE'} <= chunk_types,
             'INCOMPLETE_EVIDENCE_BUNDLE')
    for rule in rules:
        _require(rule['brand'] in ('*', product['brand']), 'INAPPLICABLE_EVIDENCE_POLICY')
    primary = images[f'AST-{job["canonical_sku"]}-PRIMARY']['evidence_payload']['record']
    original = {k: primary[k] for k in ('asset_id', 'gcs_uri', 'gcs_generation', 'content_sha256')}
    for image_row in images.values():
        if image_row['source_kind'] == 'GENERATED_IMAGE':
            _require(image_row['evidence_payload']['record']['original_product_image'] == original,
                     'EVIDENCE_IMAGE_MISMATCH')
    campaign_images = {r['evidence_payload']['record']['campaign_id']: r for r in images.values()
                       if r['source_kind'] == 'CAMPAIGN_IMAGE'}
    _require(set(ranked_chunks) == set(campaign_images) == set(performance), 'EVIDENCE_CAMPAIGN_MISMATCH')
    for campaign_id, chunk_row in ranked_chunks.items():
        image_row = campaign_images[campaign_id]
        chunk_record, image_record = chunk_row['evidence_payload']['record'], image_row['evidence_payload']['record']
        _require(chunk_record['asset_id'] == image_record['asset_id'] == performance[campaign_id]['asset_id']
                 and tuple(chunk_row[k] for k in RANK_FIELDS) == tuple(image_row[k] for k in RANK_FIELDS)
                 and image_record['gcs_uri'] in chunk_record['image_uris'], 'EVIDENCE_CAMPAIGN_MISMATCH')
    for claim in claims.values():
        _require(claim['claim_text'] == product['product_name'], 'EVIDENCE_CLAIM_PRODUCT_MISMATCH')
    for chunk in chunks:
        if chunk['sku'] is not None:
            _require(chunk['brand'] == product['brand'], 'EVIDENCE_PRODUCT_MISMATCH')
        if chunk['chunk_type'] == 'POC_APPROVED_CLAIM':
            claim = claims.get(chunk['claim_id'])
            _require(claim is not None and claim['claim_text'] in chunk['chunk_text']
                     and claim['evidence_reference'] in chunk['evidence_references'], 'INVALID_EVIDENCE_CLAIM')
        if chunk['chunk_type'] == 'WEBSITE_VISUAL_REFERENCE':
            _require(request['visual_reference'] == 'NAKED_AND_THRIVING', 'INAPPLICABLE_EVIDENCE_STYLE')
        if chunk['chunk_type'] == 'POC_RULE':
            matching = [r for r in rules if chunk['source_reference'] == _reference(
                'ci_brand_memory.brand_guidelines', f'rule_id={r["rule_id"]}')]
            _require(bool(matching) and any(r['rule_text'] in chunk['chunk_text'] for r in matching),
                     'EVIDENCE_POLICY_MISMATCH')
    for rule in rules:
        _require(any(c['chunk_type'] == 'POC_RULE' and c['source_reference'] == _reference(
            'ci_brand_memory.brand_guidelines', f'rule_id={rule["rule_id"]}') for c in chunks), 'EVIDENCE_POLICY_MISMATCH')
    for kind in ('CAMPAIGN_IMAGE', 'GENERATED_IMAGE', 'KNOWLEDGE_CHUNK'):
        selections = {r['source_record_id']: r for r in rows if r['source_kind'] == kind and r['rank'] is not None}
        _require(sorted(r['rank'] for r in selections.values()) == list(range(1, len(selections)+1)),
                 'INVALID_EVIDENCE_RANK')
        _require(len({(r['embedding_model'], r['embedding_dimensions'], r['ranking_version'])
                     for r in selections.values()}) <= 1, 'INVALID_EVIDENCE_VECTOR')
    if request['visual_reference'] == 'NAKED_AND_THRIVING':
        _require({c['source_reference'].rsplit('#', 1)[-1] for c in chunks
                  if c['chunk_type'] == 'WEBSITE_VISUAL_REFERENCE'} ==
                 {'typography', 'colors', 'layout', 'creative_patterns'}, 'INCOMPLETE_EVIDENCE_STYLE')
    if output is not None:
        validate_output(output, request=request, job=job, conversation=conversation)
        _require(output['attempt_id'] == attempt['attempt_id'] and output['evidence_snapshot_id'] in snapshot_ids
                 and output['brief']['snapshot_id'] in snapshots, 'OUTPUT_EVIDENCE_MISMATCH')
        expected_index = {evidence_reference_id(r['evidence_payload']['reference']):
                          r['evidence_payload']['reference'] for r in rows}
        _require(len(expected_index) == len({r['evidence_payload']['reference'] for r in rows}),
                 'EVIDENCE_REFERENCE_MISMATCH')
        actual_index = {e['evidence_id']: e['reference'] for e in output['brief']['evidence_index']}
        _require(actual_index == expected_index, 'OUTPUT_EVIDENCE_MISMATCH')
        _require(all(row['created_at'] <= _json_timestamp(output['brief']['created_at']) for row in rows),
                 'INVALID_TIME_ORDER')
        _require(set(output['brief']['copy']['claim_ids']) <= set(claims), 'INVALID_EVIDENCE_CLAIM')
        grounded = output['brief']['grounded_evidence']
        identity = grounded['product']['identity']
        _require(all(identity.get(k) == v for k, v in product.items()), 'OUTPUT_EVIDENCE_MISMATCH')
        image = output['brief']['product_image']['metadata']
        _require(all(image.get(k) == primary[k] for k in ('sku', 'asset_id', 'source_system',
            'gcs_uri', 'gcs_generation', 'content_sha256', 'mime_type', 'width', 'height')), 'OUTPUT_EVIDENCE_MISMATCH')
        for actual, expected, key in (
            (grounded['approved_claims'].get('claims'), claims, 'claim_id'),
            (grounded['policies'].get('rules'), {r['rule_id']: r for r in rules}, 'rule_id'),
            (grounded['campaign_performance'].get('summaries'), performance, 'campaign_id'),
        ):
            _require(isinstance(actual, list) and all(isinstance(r, dict) and isinstance(r.get(key), str) for r in actual),
                     'OUTPUT_EVIDENCE_MISMATCH')
            by_id = {r[key]: r for r in actual}
            _require(len(by_id) == len(actual) and set(by_id) == set(expected)
                     and all(all(by_id[identifier].get(k) == v for k, v in record.items())
                             for identifier, record in expected.items()), 'OUTPUT_EVIDENCE_MISMATCH')
        sales = grounded['sales']
        candidates = [r['evidence_payload']['record'] for r in rows if r['source_kind'] == 'SALES_SUMMARY'
                      and all(r['evidence_payload']['record'][k] == sales.get(k)
                              for k in ('start_date', 'end_date', 'currency'))]
        totals = sales.get('totals')
        _require(isinstance(totals, dict) and bool(candidates)
                 and any(all(totals.get(k) == r[k] for k in (*SALES_COUNTS, *SALES_MONEY)) for r in candidates),
                 'OUTPUT_EVIDENCE_MISMATCH')
