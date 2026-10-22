"""Fresh authoritative reader projections for workflow evidence.

The implemented 4.5 slices project canonical product knowledge and current POC
policy/identity approvals from captured BigQuery reads. They do not write rows,
read local exports, load images or create a complete planning bundle. Later 4.5
slices add analytics, image integrity, complete assembly and audits.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import io

from PIL import Image

from src.application.access_policy import OwnedResource
from src.application.evidence_contract import (
    EVIDENCE_VERSION, SOURCE_TABLES, evidence_row_id, validate_evidence_snapshot,
)
from src.application.workflow_contract import ContractError, canonical_json_bytes
from src.application.workflow_schema import TABLES
from src.generators.sales_generator import LOCATION, PROJECT
from src.retrieval.brand_context import BrandContextRead, build_packet as build_brand_packet
from src.retrieval.campaign_creatives import (
    CampaignCreativeRead, build_packet as build_creative_packet,
)
from src.retrieval.campaign_performance import (
    CampaignPerformanceRead, build_packet as build_performance_packet,
)
from src.retrieval.campaign_images import CampaignImagesRead
from src.retrieval.generated_banner import GeneratedBannerRead
from src.retrieval.generated_banner_search import GeneratedBannerSearchRead
from src.retrieval.product_image import ProductImageRead
from src.retrieval.product_knowledge import ProductKnowledgeRead, build_packet
from src.retrieval.request import RetrievalRequest
from src.retrieval.sales_analytics import SalesAnalyticsRead, build_packet as build_sales_packet
from src.retrieval.visual_reference import VisualReferenceRead, build_packet as build_visual_packet

_PACKET_FIELDS = {
    'canonical_sku', 'retrieval_method', 'snapshot_id', 'product',
    'product_evidence_reference', 'factual_chunks', 'poc_identity_claims',
    'poc_identity_chunks', 'usage_notes',
}
_BRAND_PACKET_FIELDS = {
    'canonical_sku', 'product', 'source_of_truth', 'checked_at', 'snapshot_id',
    'retrieval_method', 'policy_authority', 'claim_approval_scope', 'poc_rules',
    'policy_chunks', 'approved_claims', 'allowed_claim_types', 'usage_notes',
}
_SALES_PACKET_FIELDS = {
    'canonical_sku', 'product', 'source_of_truth', 'source_system', 'channel',
    'currency', 'start_date', 'end_date', 'trend_window_days', 'totals', 'monthly',
    'windows', 'trends', 'campaign_periods', 'evidence_references', 'usage_notes',
}
_PERFORMANCE_PACKET_FIELDS = {
    'canonical_sku', 'source_of_truth', 'source_system', 'snapshot_id',
    'retrieval_method', 'summaries', 'usage_notes',
}
_CREATIVE_PACKET_FIELDS = {
    'request', 'source_of_truth', 'source_system', 'approval_scope', 'snapshot_id',
    'query_embedding', 'search', 'creatives', 'usage_notes',
}
_VISUAL_PACKET_FIELDS = {
    'visual_reference', 'reference_brand', 'approval_scope', 'snapshot_id',
    'visual_chunks', 'usage_notes',
}


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date,datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_json_value(child) for child in value]
    return value


def _source_record(kind: str, record: dict) -> tuple[str, str]:
    if kind == 'PRODUCT':
        key = f'sku={record["sku"]}'
        return key, f'{PROJECT}.ci_product.product_master#{key}'
    if kind == 'POC_CLAIM':
        key = f'claim_id={record["claim_id"]}'
        return key, f'{PROJECT}.ci_brand_memory.product_claims#{key}'
    if kind == 'POC_RULE':
        key = f'rule_id={record["rule_id"]}'
        return key, f'{PROJECT}.ci_brand_memory.brand_guidelines#{key}'
    if kind == 'SALES_SUMMARY':
        key = (f'sku={record["sku"]}&channel=ECOMMERCE&source_system=SYNTHETIC_POC'
               f'&start_date={record["start_date"]}&end_date={record["end_date"]}')
        return key, f'{PROJECT}.ci_sales.sku_daily_sales#{key}'
    if kind == 'CAMPAIGN_PERFORMANCE':
        key = (f'campaign_id={record["campaign_id"]}&asset_id={record["asset_id"]}'
               f'&source_system=SYNTHETIC_POC&start_date={record["start_date"]}'
               f'&end_date={record["end_date"]}')
        return key, f'{PROJECT}.ci_creative.campaign_daily_performance#{key}'
    if kind.endswith('_IMAGE'):
        key=f'asset_id={record["asset_id"]}'
        return key,f'{PROJECT}.{SOURCE_TABLES[kind]}#{key}'
    return record['chunk_id'], record['source_reference']


def _row(*, kind: str, record: dict,
         read: ProductKnowledgeRead | BrandContextRead | SalesAnalyticsRead | CampaignPerformanceRead
               | CampaignCreativeRead | VisualReferenceRead | ProductImageRead | CampaignImagesRead
               | GeneratedBannerSearchRead,
         evidence_snapshot_id: str, request: dict, job: dict, attempt: dict,
         conversation: OwnedResource, created_at: datetime,
         knowledge_snapshot_id: str | None = None, rank: int | None = None,
         cosine_distance: float | None = None, ranking_version: str | None = None,
         approval_reference: str | None = None, review_revision: int | None = None,
         embedding_model: str | None = None, embedding_dimensions: int | None = None) -> dict:
    source_record_id, reference = _source_record(kind, record)
    table=('ci_brand_memory.poc_visual_reference_chunks'
           if kind=='KNOWLEDGE_CHUNK' and record.get('chunk_type')=='WEBSITE_VISUAL_REFERENCE'
           else SOURCE_TABLES[kind])
    source_table = f'{PROJECT}.{table}'
    row = dict.fromkeys(field.name for field in TABLES['evidence_snapshots'].schema)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id',
                'job_id', 'request_id', 'attempt_id'):
        row[key] = attempt[key]
    content_sha256 = (record['content_sha256'] if kind == 'KNOWLEDGE_CHUNK' or kind.endswith('_IMAGE')
                      else hashlib.sha256(canonical_json_bytes(record)).hexdigest())
    row.update(evidence_snapshot_id=evidence_snapshot_id,
        evidence_ref_id=evidence_row_id(source_table,source_record_id,reference),
        canonical_sku=job['canonical_sku'], source_kind=kind, source_table=source_table,
        source_record_id=source_record_id, source_system=record['source_system'],
        source_revision=(read.source_revision if isinstance(read,GeneratedBannerSearchRead)
                         else read.query_job_id),
        knowledge_snapshot_id=(record['snapshot_id'] if kind == 'KNOWLEDGE_CHUNK'
                               else knowledge_snapshot_id),
        content_sha256=content_sha256, evidence_schema_version=EVIDENCE_VERSION,
        evidence_payload={'reference':reference,'record':record},
        approval_reference=(record['approval_reference'] if kind == 'POC_CLAIM' else approval_reference),
        review_revision=review_revision,
        gcs_uri=record['gcs_uri'] if kind.endswith('_IMAGE') else None,
        gcs_generation=record['gcs_generation'] if kind.endswith('_IMAGE') else None,
        image_sha256=record['content_sha256'] if kind.endswith('_IMAGE') else None,
        embedding_model=record['model_id'] if kind.endswith('_IMAGE') else embedding_model,
        embedding_dimensions=record['dimension'] if kind.endswith('_IMAGE') else embedding_dimensions,
        rank=rank,cosine_distance=cosine_distance,ranking_version=ranking_version,
        validated_at=read.observed_at, created_at=created_at)
    validate_evidence_snapshot(row,request=request,job=job,attempt=attempt,conversation=conversation)
    return row


def project_product_knowledge(read: ProductKnowledgeRead, *, evidence_snapshot_id: str,
                              request: dict, job: dict, attempt: dict,
                              conversation: OwnedResource, created_at: datetime) -> list[dict]:
    """Bind a captured BigQuery product read to one workflow evidence snapshot."""
    if not isinstance(read, ProductKnowledgeRead) or read.project != PROJECT or read.location != LOCATION:
        raise ContractError('INVALID_EVIDENCE_READ')
    if set(read.packet) != _PACKET_FIELDS or read.packet.get('canonical_sku') != job.get('canonical_sku'):
        raise ContractError('INVALID_EVIDENCE_READ')
    try:
        validated = build_packet(read.packet['canonical_sku'], {
            'products':[read.packet['product']],
            'knowledge_chunks':read.packet['factual_chunks'] + read.packet['poc_identity_chunks'],
            'poc_identity_claims':read.packet['poc_identity_claims'],
        })
    except (KeyError, TypeError, ValueError):
        raise ContractError('INVALID_EVIDENCE_READ') from None
    if validated != read.packet:
        raise ContractError('INVALID_EVIDENCE_READ')

    product = _json_value(validated['product'])
    claims = [_json_value(claim) for claim in validated['poc_identity_claims']]
    for claim in claims:
        claim.pop('review_notes', None)
    chunks = []
    for original in validated['factual_chunks'] + validated['poc_identity_chunks']:
        chunk = _json_value(original)
        chunk.update(campaign_id=None,asset_id=None)
        chunks.append(chunk)
    records = [('PRODUCT',product)]
    records.extend(('POC_CLAIM',record) for record in sorted(claims,key=lambda item:item['claim_id']))
    records.extend(('KNOWLEDGE_CHUNK',record) for record in sorted(chunks,key=lambda item:item['chunk_id']))
    return [_row(kind=kind,record=record,read=read,evidence_snapshot_id=evidence_snapshot_id,
        request=request,job=job,attempt=attempt,conversation=conversation,created_at=created_at)
        for kind,record in records]


def _sales_record(packet: dict, summary: dict, start: date, end: date) -> dict:
    return _json_value(dict(sku=packet['canonical_sku'],source_system='SYNTHETIC_POC',
        currency=packet['currency'],channel='ECOMMERCE',start_date=start,end_date=end,
        **{name:summary[name] for name in ('units_sold','orders','row_count','day_count',
            'gross_sales','discount_amount','refund_amount','net_sales')}))


def project_sales_analytics(read: SalesAnalyticsRead, *, evidence_snapshot_id: str,
                            request: dict, job: dict, attempt: dict,
                            conversation: OwnedResource, created_at: datetime) -> list[dict]:
    """Project validated aggregate sales periods without exposing daily rows."""
    if (not isinstance(read,SalesAnalyticsRead) or read.project!=PROJECT or read.location!=LOCATION
            or set(read.packet)!=_SALES_PACKET_FIELDS
            or read.packet.get('canonical_sku')!=job.get('canonical_sku')
            or read.packet.get('source_of_truth')!='BIGQUERY'):
        raise ContractError('INVALID_EVIDENCE_READ')
    packet=read.packet
    config=dict(start_date=packet['start_date'].isoformat(),end_date=packet['end_date'].isoformat(),
        trend_window_days=packet['trend_window_days'],channel=packet['channel'],maximum_bytes_billed=1)
    try:
        validated=build_sales_packet(packet['canonical_sku'],dict(products=[packet['product']],
            totals=packet['totals'],monthly=packet['monthly'],windows=list(packet['windows'].values()),
            trends=packet['trends'],campaign_periods=packet['campaign_periods'],
            campaign_count=len(packet['campaign_periods']),overlapping_campaign_days=0),config)
    except (AttributeError,KeyError,TypeError,ValueError):
        raise ContractError('INVALID_EVIDENCE_READ') from None
    if validated!=packet:
        raise ContractError('INVALID_EVIDENCE_READ')

    periods=[(packet['start_date'],packet['end_date'],packet['totals'])]
    periods.extend((item['first_date'],item['last_date'],item) for item in packet['monthly'])
    periods.extend((item['first_date'],item['last_date'],item) for item in packet['windows'].values())
    periods.extend((item['effective_start'],item['effective_end'],item) for item in packet['campaign_periods'])
    unique={}
    for start,end,summary in periods:
        record=_sales_record(packet,summary,start,end)
        key=(record['start_date'],record['end_date'])
        if key in unique and unique[key]!=record:
            raise ContractError('EVIDENCE_SOURCE_CONFLICT')
        unique[key]=record
    records=[unique[key] for key in sorted(unique)]
    return [_row(kind='SALES_SUMMARY',record=record,read=read,
        evidence_snapshot_id=evidence_snapshot_id,request=request,job=job,attempt=attempt,
        conversation=conversation,created_at=created_at) for record in records]


def project_campaign_performance(read: CampaignPerformanceRead, *, evidence_snapshot_id: str,
                                 request: dict, job: dict, attempt: dict,
                                 conversation: OwnedResource, created_at: datetime) -> list[dict]:
    """Project current structured campaign aggregates and eligibility snapshot pins."""
    if (not isinstance(read,CampaignPerformanceRead) or read.project!=PROJECT or read.location!=LOCATION
            or set(read.packet)!=_PERFORMANCE_PACKET_FIELDS
            or read.packet.get('canonical_sku')!=job.get('canonical_sku')
            or read.packet.get('source_of_truth')!='BIGQUERY'):
        raise ContractError('INVALID_EVIDENCE_READ')
    packet=read.packet
    try:
        identifiers=[summary['campaign_id'] for summary in packet['summaries']]
        validated=build_performance_packet(packet['canonical_sku'],packet['summaries'],identifiers)
    except (KeyError,TypeError,ValueError):
        raise ContractError('INVALID_EVIDENCE_READ') from None
    if validated!=packet:
        raise ContractError('INVALID_EVIDENCE_READ')

    rows=[]
    fields=('sku','source_system','currency','campaign_id','asset_id','start_date','end_date',
        'attribution_model','impressions','clicks','conversions','attributed_units','performance_rows',
        'performance_days','budget','spend','attributed_revenue','ctr','conversion_rate','roas')
    for summary in validated['summaries']:
        record=_json_value({name:summary[name] for name in fields})
        rows.append(_row(kind='CAMPAIGN_PERFORMANCE',record=record,read=read,
            evidence_snapshot_id=evidence_snapshot_id,request=request,job=job,attempt=attempt,
            conversation=conversation,created_at=created_at,
            knowledge_snapshot_id=summary['snapshot_id']))
    return rows


def project_campaign_creatives(read: CampaignCreativeRead, *, evidence_snapshot_id: str,
                               request: dict, job: dict, attempt: dict,
                               conversation: OwnedResource, created_at: datetime) -> list[dict]:
    """Project ranked campaign text with its native query and vector identity."""
    if (not isinstance(read,CampaignCreativeRead) or read.project!=PROJECT or read.location!=LOCATION
            or set(read.packet)!=_CREATIVE_PACKET_FIELDS
            or read.packet.get('source_of_truth')!='BIGQUERY'):
        raise ContractError('INVALID_EVIDENCE_READ')
    packet=read.packet
    try:
        retrieval_request=RetrievalRequest.from_dict(packet['request'])
        config=dict(model_id=packet['query_embedding']['model_id'],
            dimension=packet['query_embedding']['dimension'],
            location=packet['query_embedding']['model_location'],top_k=packet['search']['top_k'],
            maximum_bytes_billed=1)
        validated=build_creative_packet(retrieval_request,packet['creatives'],config)
    except (KeyError,TypeError,ValueError):
        raise ContractError('INVALID_EVIDENCE_READ') from None
    if (validated!=packet or retrieval_request.canonical_sku!=job.get('canonical_sku')
            or packet.get('snapshot_id') is None):
        raise ContractError('INVALID_EVIDENCE_READ')

    rows=[]
    fields=('chunk_id','snapshot_id','chunk_type','brand','sku','campaign_id','asset_id','claim_id',
        'chunk_text','source_system','source_reference','evidence_references','image_uris',
        'retrieval_status','approval_scope','content_sha256')
    for creative in validated['creatives']:
        record={name:_json_value(creative[name]) for name in fields}
        rows.append(_row(kind='KNOWLEDGE_CHUNK',record=record,read=read,
            evidence_snapshot_id=evidence_snapshot_id,request=request,job=job,attempt=attempt,
            conversation=conversation,created_at=created_at,rank=creative['rank'],
            cosine_distance=creative['distance'],ranking_version='CAMPAIGN-CREATIVE-RANKING-v1',
            embedding_model=creative['model_id'],embedding_dimensions=creative['dimension']))
    return rows


def project_visual_reference(read: VisualReferenceRead, *, evidence_snapshot_id: str,
                             request: dict, job: dict, attempt: dict,
                             conversation: OwnedResource, created_at: datetime) -> list[dict]:
    """Project all four current styling-only sections for an opted-in request."""
    if (not isinstance(read,VisualReferenceRead) or read.project!=PROJECT or read.location!=LOCATION
            or set(read.packet)!=_VISUAL_PACKET_FIELDS
            or read.packet.get('visual_reference')!='NAKED_AND_THRIVING'
            or request.get('visual_reference')!='NAKED_AND_THRIVING'):
        raise ContractError('INVALID_EVIDENCE_READ')
    try:
        validated=build_visual_packet('NAKED_AND_THRIVING',read.packet['visual_chunks'])
    except (KeyError,TypeError,ValueError):
        raise ContractError('INVALID_EVIDENCE_READ') from None
    if validated!=read.packet:
        raise ContractError('INVALID_EVIDENCE_READ')
    records=[_json_value(record) for record in validated['visual_chunks']]
    return [_row(kind='KNOWLEDGE_CHUNK',record=record,read=read,
        evidence_snapshot_id=evidence_snapshot_id,request=request,job=job,attempt=attempt,
        conversation=conversation,created_at=created_at) for record in records]


def _verify_captured_image(record: dict, raw: bytes, expected_format: str) -> None:
    if not isinstance(raw,bytes) or not raw or hashlib.sha256(raw).hexdigest()!=record['content_sha256']:
        raise ContractError('EVIDENCE_IMAGE_MISMATCH')
    try:
        with Image.open(io.BytesIO(raw)) as image:
            if image.format!=expected_format or image.size!=(record['width'],record['height']):
                raise ContractError('EVIDENCE_IMAGE_MISMATCH')
            image.verify()
    except ContractError:
        raise
    except Exception:
        raise ContractError('EVIDENCE_IMAGE_MISMATCH') from None


def _image_record(kind: str, metadata: dict) -> dict:
    if kind=='PRODUCT_IMAGE':
        return dict(sku=metadata['canonical_sku'],asset_id=metadata['asset_id'],campaign_id=None,
            source_system='BEAUTYFEEDS',gcs_uri=metadata['gcs_uri'],gcs_generation=metadata['gcs_generation'],
            content_sha256=metadata['content_sha256'],mime_type=metadata['mime_type'],width=metadata['width'],
            height=metadata['height'],description=metadata['asset_description'],embedding_id=metadata['embedding_id'],
            model_id=metadata['model_id'],model_location=metadata['model_location'],dimension=metadata['dimension'],
            task_type='IMAGE',snapshot_id=None,review=None,original_product_image=None,
            usage_scope='SOURCE_PRODUCT_IMAGE')
    if kind=='CAMPAIGN_IMAGE':
        return dict(sku=metadata['canonical_sku'],asset_id=metadata['asset_id'],campaign_id=metadata['campaign_id'],
            source_system='SYNTHETIC_POC',gcs_uri=metadata['gcs_uri'],gcs_generation=metadata['gcs_generation'],
            content_sha256=metadata['content_sha256'],mime_type=metadata['mime_type'],width=metadata['width'],
            height=metadata['height'],description=metadata['visual_description'],embedding_id=metadata['embedding_id'],
            model_id=metadata['model_id'],model_location=metadata['model_location'],dimension=metadata['dimension'],
            task_type='IMAGE',snapshot_id=metadata['snapshot_id'],review=None,original_product_image=None,
            usage_scope='SYNTHETIC_POC_ONLY')
    embedding=metadata['embedding']
    return dict(sku=metadata['canonical_sku'],asset_id=metadata['asset_id'],campaign_id=None,
        source_system='SYNTHETIC_POC',gcs_uri=metadata['gcs_uri'],gcs_generation=metadata['gcs_generation'],
        content_sha256=metadata['content_sha256'],mime_type=metadata['mime_type'],width=metadata['width'],
        height=metadata['height'],description=metadata['visual_description'],embedding_id=embedding['embedding_id'],
        model_id=embedding['model_id'],model_location=embedding['model_location'],dimension=embedding['dimension'],
        task_type='IMAGE',snapshot_id=metadata['snapshot_id'],review=metadata['review'],
        original_product_image=metadata['original_product_image'],usage_scope=metadata['usage_scope'])


def project_product_image(read: ProductImageRead, *, evidence_snapshot_id: str, request: dict,
                          job: dict, attempt: dict, conversation: OwnedResource,
                          created_at: datetime) -> list[dict]:
    if (not isinstance(read,ProductImageRead) or read.project!=PROJECT or read.location!=LOCATION
            or read.metadata.get('canonical_sku')!=job.get('canonical_sku')
            or read.metadata.get('verification')!={'generation_match':True,'sha256_match':True,
                'format_match':True,'dimensions_match':True}):
        raise ContractError('INVALID_EVIDENCE_READ')
    record=_image_record('PRODUCT_IMAGE',read.metadata);_verify_captured_image(record,read.raw,'JPEG')
    return [_row(kind='PRODUCT_IMAGE',record=record,read=read,evidence_snapshot_id=evidence_snapshot_id,
        request=request,job=job,attempt=attempt,conversation=conversation,created_at=created_at)]


def project_campaign_images(read: CampaignImagesRead, *, evidence_snapshot_id: str, request: dict,
                            job: dict, attempt: dict, conversation: OwnedResource,
                            created_at: datetime) -> list[dict]:
    if not isinstance(read,CampaignImagesRead) or read.project!=PROJECT or read.location!=LOCATION or not read.images:
        raise ContractError('INVALID_EVIDENCE_READ')
    rows=[]
    for expected_rank,(metadata,raw) in enumerate(read.images,1):
        if (metadata.get('canonical_sku')!=job.get('canonical_sku') or metadata.get('rank')!=expected_rank
                or not all(metadata.get('verification',{}).values())):
            raise ContractError('INVALID_EVIDENCE_READ')
        record=_image_record('CAMPAIGN_IMAGE',metadata);_verify_captured_image(record,raw,'PNG')
        rows.append(_row(kind='CAMPAIGN_IMAGE',record=record,read=read,
            evidence_snapshot_id=evidence_snapshot_id,request=request,job=job,attempt=attempt,
            conversation=conversation,created_at=created_at,knowledge_snapshot_id=record['snapshot_id'],
            rank=expected_rank,cosine_distance=metadata['distance'],
            ranking_version='CAMPAIGN-CREATIVE-RANKING-v1'))
    return rows


def project_generated_images(read: GeneratedBannerSearchRead, *, evidence_snapshot_id: str,
                             request: dict, job: dict, attempt: dict,
                             conversation: OwnedResource, created_at: datetime) -> list[dict]:
    if (not isinstance(read,GeneratedBannerSearchRead) or read.project!=PROJECT or read.location!=LOCATION
            or read.packet.get('canonical_sku')!=job.get('canonical_sku')
            or read.packet.get('source_of_truth')!='BIGQUERY'
            or len(read.images)!=len(read.banner_reads)
            or read.packet.get('results')!=[metadata for metadata,_ in read.images]):
        raise ContractError('INVALID_EVIDENCE_READ')
    rows=[]
    for expected_rank,((metadata,raw),exact) in enumerate(zip(read.images,read.banner_reads),1):
        if (metadata.get('rank')!=expected_rank or metadata.get('canonical_sku')!=job.get('canonical_sku')
                or exact.raw!=raw or exact.packet['asset_id']!=metadata['asset_id']
                or not all(metadata.get('verification',{}).values())):
            raise ContractError('INVALID_EVIDENCE_READ')
        record=_image_record('GENERATED_IMAGE',metadata);_verify_captured_image(record,raw,'PNG')
        review=record['review'];approval=f'{PROJECT}.ci_creative.generated_asset_reviews#review_id={review["review_id"]}'
        rows.append(_row(kind='GENERATED_IMAGE',record=record,read=read,
            evidence_snapshot_id=evidence_snapshot_id,request=request,job=job,attempt=attempt,
            conversation=conversation,created_at=created_at,knowledge_snapshot_id=record['snapshot_id'],
            rank=expected_rank,cosine_distance=metadata['distance'],
            ranking_version='GENERATED-BANNER-COSINE-v1',approval_reference=approval,
            review_revision=review['revision']))
    return rows


def project_brand_context(read: BrandContextRead, *, evidence_snapshot_id: str,
                          request: dict, job: dict, attempt: dict,
                          conversation: OwnedResource, created_at: datetime) -> list[dict]:
    """Bind current applicable POC rules and identity approvals to one snapshot."""
    if (not isinstance(read,BrandContextRead) or read.project != PROJECT or read.location != LOCATION
            or set(read.packet) != _BRAND_PACKET_FIELDS
            or read.packet.get('canonical_sku') != job.get('canonical_sku')
            or read.packet.get('source_of_truth') != 'BIGQUERY'
            or read.observed_at != read.packet.get('checked_at')):
        raise ContractError('INVALID_EVIDENCE_READ')
    try:
        validated = build_brand_packet(read.packet['canonical_sku'], {
            'checked_at':read.packet['checked_at'],
            'products':[read.packet['product']],
            'poc_rules':read.packet['poc_rules'],
            'policy_chunks':read.packet['policy_chunks'],
            'approved_claims':read.packet['approved_claims'],
        })
    except (KeyError,TypeError,ValueError):
        raise ContractError('INVALID_EVIDENCE_READ') from None
    if validated != read.packet:
        raise ContractError('INVALID_EVIDENCE_READ')

    rules = []
    for original in validated['poc_rules']:
        rule = _json_value(original)
        rule.pop('category',None)
        rule.pop('evidence_references',None)
        rules.append(rule)
    claims = []
    for original in validated['approved_claims']:
        claim = _json_value(original)
        claim.pop('review_notes',None)
        claim.pop('evidence_references',None)
        claims.append(claim)
    chunks = [_json_value(chunk) for chunk in validated['policy_chunks']]
    records = [('POC_RULE',record) for record in sorted(rules,key=lambda item:item['rule_id'])]
    records.extend(('POC_CLAIM',record) for record in sorted(claims,key=lambda item:item['claim_id']))
    records.extend(('KNOWLEDGE_CHUNK',record) for record in sorted(chunks,key=lambda item:item['chunk_id']))
    return [_row(kind=kind,record=record,read=read,evidence_snapshot_id=evidence_snapshot_id,
        request=request,job=job,attempt=attempt,conversation=conversation,created_at=created_at)
        for kind,record in records]
