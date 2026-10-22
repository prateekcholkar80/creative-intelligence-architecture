"""Attach current generated banner originals as visual references only.

Request-derived query text is a design preference, never product evidence.
Parent banner copy, claim IDs and analytics are excluded from model context.
Original product facts/approvals and historical metrics keep their own readers.
"""
from __future__ import annotations

import copy
import hashlib
import math

from google.cloud import bigquery, storage

from src.generators.sales_generator import PROJECT
from src.retrieval.generated_banner import retrieve_generated_banner
from src.retrieval.generated_banner_search import (
    MultimodalTextQueryEmbedder, embedding_config, search_generated_banners, validate_query,
)
from src.retrieval.campaign_images import verify_image_bytes
from src.retrieval.request import RetrievalRequest
from src.generators.generated_banner_embeddings import BUCKET, validate_selection

ROLE = 'GENERATED_POC_VISUAL_REFERENCE'
USAGE = 'NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE'


def visual_query(request: RetrievalRequest) -> str:
    """Bounded channel/objective/style preferences; no product/benefit assertions."""
    channel = {'PAID_SOCIAL':'Skincare social banner','PAID_SEARCH':'Skincare product ad',
               'EMAIL':'Skincare email banner'}[request.channel]
    intent = {'AWARENESS':'product focus','CONSIDERATION':'clear product details',
              'CONVERSION':'clear shopping CTA'}[request.objective]
    style = ('spacious off-white layout, serif heading, sans-serif text'
             if request.visual_reference=='NAKED_AND_THRIVING' else 'spacious white layout, clear typography')
    return f'{channel}, {intent}, {style}'


def retrieve_generated_references(client: bigquery.Client, gcs: storage.Client,
                                  request: RetrievalRequest, config: dict) -> tuple[dict,list[tuple[dict,bytes]]]:
    text = validate_query(request.canonical_sku,visual_query(request),config)
    embedder = MultimodalTextQueryEmbedder(config)
    try:
        packet,images = search_generated_banners(client,gcs,embedder,request.canonical_sku,text,config)
        packet['model_calls'] = embedder.model_calls
        return packet,images
    finally:
        embedder.close()


def visual_metadata(context: dict, metadata: dict, raw: bytes, config: dict) -> dict:
    """Validate and retain visual identity/provenance; omit inherited claim/copy data."""
    sku = context['canonical_sku'];asset = metadata['asset_id']
    validate_selection(sku,asset)
    primary = next(m for m in context['image_manifest'] if m['source_system']=='BEAUTYFEEDS')
    expected_original = dict(asset_id=primary['asset_id'],gcs_uri=primary['gcs_uri'],
        gcs_generation=primary['gcs_generation'],content_sha256=primary['content_sha256'])
    vector = metadata['embedding'];distance = metadata['distance']
    review = metadata['review']
    if (metadata['canonical_sku']!=sku or metadata['snapshot_id']!=context['snapshot_id']
            or metadata['source_of_truth']!='BIGQUERY' or metadata['original_image_store']!='GCS'
            or metadata['source_system']!='SYNTHETIC_POC' or metadata['status']!='REGISTERED_POC_DRAFT'
            or metadata['approval_scope']!='SYNTHETIC_POC_ONLY' or metadata['retrieval_status']!=ROLE
            or metadata['usage_scope']!=USAGE or metadata['original_product_image']!=expected_original
            or review['status']!='APPROVED_FOR_POC' or review['approval_scope']!='POC_VISUAL_REUSE_ONLY'
            or not review['review_id'] or type(review['revision']) is not int or review['revision']<1
            or asset!=f'SYN-GEN-{sku}-{metadata["content_sha256"]}'
            or metadata['gcs_uri']!=f'gs://{BUCKET}/generated_campaigns/{sku}/grounded-banner-preview-v1/{metadata["content_sha256"]}.png'
            or metadata['mime_type']!='image/png' or (metadata['width'],metadata['height'])!=(1200,628)
            or type(metadata['byte_count']) is not int or metadata['byte_count']!=len(raw)
            or vector['model_id']!=config['image_model'] or vector['model_location']!=config['location']
            or vector['dimension']!=config['image_dimension'] or vector['task_type']!='IMAGE'
            or not vector['embedding_id'] or type(distance) not in (int,float)
            or not math.isfinite(distance) or not -1e-12<=distance<=2+1e-12
            or not metadata['verification'] or any(value is not True for value in metadata['verification'].values())
            or not {'current_eligibility','generation_match','sha256_match','format_match','dimensions_match',
                    'vector_identity_match'}.issubset(metadata['verification'])
            or metadata['description_method']!='DETERMINISTIC_COMPOSITION'):
        raise ValueError('Generated visual reference has stale identity, image, snapshot, profile or authority scope')
    verify_image_bytes(metadata,raw)
    fields = ('asset_id','canonical_sku','source_of_truth','original_image_store','source_system','status',
        'approval_scope','retrieval_status','usage_scope','gcs_uri','gcs_generation','content_sha256',
        'mime_type','width','height','byte_count','snapshot_id','original_product_image','embedding',
        'verification','rank','distance','visual_description','description_method','review')
    result = copy.deepcopy({key:metadata[key] for key in fields})
    # Direct authoritative pointers preserve provenance without importing a parent
    # banner's claim/analytics evidence into this generation's factual sections.
    result.update(sku=sku,image_role=ROLE,evidence_references=[
        f'{PROJECT}.ci_creative.generated_campaign_assets#asset_id={asset}',
        f'{PROJECT}.ci_brand_memory.image_embeddings#embedding_id={vector["embedding_id"]}',
        f'{PROJECT}.ci_creative.generated_asset_reviews#review_id={review["review_id"]}',
        f'{PROJECT}.ci_product.product_assets#asset_id={primary["asset_id"]}'])
    return result


def build_visual_context(context: dict, packet: dict, images: list[tuple[dict,bytes]], config: dict) -> dict:
    request = RetrievalRequest.from_dict(context['request'])
    text = validate_query(request.canonical_sku,visual_query(request),config)
    profile = packet['query_embedding']
    if (packet['canonical_sku']!=request.canonical_sku or packet['query']!=text
            or packet['source_of_truth']!='BIGQUERY' or packet['original_image_store']!='GCS'
            or packet['source_system']!='SYNTHETIC_POC' or packet['approval_scope']!='SYNTHETIC_POC_ONLY'
            or packet['retrieval_status']!=ROLE or packet['usage_scope']!=USAGE
            or packet['retrieval_method']!='EXACT_COSINE_IMAGE_SEARCH' or packet['distance_type']!='COSINE'
            or profile['model_id']!=config['image_model'] or profile['model_location']!=config['location']
            or profile['dimension']!=config['image_dimension'] or profile['input_modality']!='TEXT'
            or profile['query_sha256']!=hashlib.sha256(text.encode()).hexdigest()
            or profile['ephemeral'] is not True or packet['top_k']!=config['top_k']
            or len(images)>config['top_k'] or packet['results']!=[metadata for metadata,_ in images]
            or any(value is not True for value in packet['verification'].values())
            or not {'current_ranked_eligibility','original_images_verified'}.issubset(packet['verification'])):
        raise ValueError('Invalid generated-only visual search provenance, query, profile or results')
    if packet['snapshot_id']!=(context['snapshot_id'] if images else None):
        raise ValueError('Generated visual references do not share the current evidence snapshot')
    if len({metadata['asset_id'] for metadata,_ in images})!=len(images):
        raise ValueError('Duplicate generated visual references')
    results = []
    for rank,(metadata,raw) in enumerate(images,1):
        if type(metadata['rank']) is not int or metadata['rank']!=rank:
            raise ValueError('Generated visual references must retain their ranked order')
        results.append(visual_metadata(context,metadata,raw,config))
    if results!=sorted(results,key=lambda row:(row['distance'],row['asset_id'])):
        raise ValueError('Generated visual references are not ordered by cosine distance')
    return dict(canonical_sku=request.canonical_sku,source_of_truth='BIGQUERY',original_image_store='GCS',
        source_system='SYNTHETIC_POC',approval_scope='SYNTHETIC_POC_ONLY',retrieval_status=ROLE,usage_scope=USAGE,
        query=text,query_authority='REQUEST_VISUAL_PREFERENCE_ONLY',query_embedding=copy.deepcopy(profile),
        snapshot_id=packet['snapshot_id'],retrieval_method='EXACT_COSINE_IMAGE_SEARCH',distance_type='COSINE',
        top_k=packet['top_k'],model_calls=packet.get('model_calls'),results=results,
        usage_notes=['Generated drafts are visual inspiration only, not brand rules, product facts or approved copy.',
            'No generated-banner benefit claims, parent copy, claim approvals or performance are imported.',
            'Cosine distance ranks visual references; it is not observed creative effectiveness.'])


def recheck_generated_references(client: bigquery.Client, gcs: storage.Client,
                                 context: dict, image_bytes: dict[str,bytes], config: dict) -> None:
    """Re-read current BQ eligibility and pinned GCS bytes after remote generation."""
    for expected in context.get('generated_banner_references',{}).get('results',[]):
        metadata,raw = retrieve_generated_banner(client,gcs,context['canonical_sku'],expected['asset_id'],embedding_config(config))
        metadata.update(rank=expected['rank'],distance=expected['distance'])
        if visual_metadata(context,metadata,raw,config)!=expected or raw!=image_bytes[expected['asset_id']]:
            raise ValueError('Generated visual reference changed during generation; retrieve again')
