"""Assemble fresh BigQuery evidence and verified original GCS image bytes.

Run: python -m src.retrieval.evidence_packet --request config/retrieval_request.example.json
Returns a context dictionary plus in-memory image bytes for subsequent model
use. Exports are review artifacts only. This step generates no brief or banner.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.cloud import bigquery, storage
from google.genai import types

from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.brand_context import retrieve_brand_context
from src.retrieval.campaign_creatives import CONFIG as CAMPAIGN_CONFIG, retrieve_campaign_creatives, validate_config
from src.retrieval.campaign_images import retrieve_campaign_images, verify_image_bytes as verify_campaign_bytes
from src.retrieval.campaign_performance import json_value, retrieve_campaign_performance
from src.retrieval.product_image import retrieve_product_image, verify_image_bytes as verify_product_bytes
from src.retrieval.product_knowledge import retrieve_product_knowledge
from src.retrieval.request import RetrievalRequest
from src.retrieval.sales_analytics import CONFIG as SALES_CONFIG, query_scope, retrieve_sales_analytics
from src.retrieval.visual_reference import retrieve_visual_reference
from src.retrieval.generated_banner_context import build_visual_context, retrieve_generated_references
from src.retrieval.generated_banner_search import CONFIG as GENERATED_CONFIG, validate_config as validate_generated_config

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvidenceBundle:
    """Live validated context and original bytes; no serving dependence on files."""
    context: dict
    image_bytes: dict[str,bytes]


def claim_signature(claims: list[dict]) -> dict:
    if len({c['claim_id'] for c in claims}) != len(claims):
        raise ValueError('Duplicate approved claim IDs')
    return {c['claim_id']:{k:v for k,v in c.items() if k != 'evidence_references'} for c in claims}


def evidence_index(context: dict) -> list[dict[str,str]]:
    references = set()
    def visit(value: object) -> None:
        if isinstance(value,dict):
            for key,item in value.items():
                if key == 'evidence_references':
                    references.update(item)
                elif key in ('source_reference','evidence_reference','product_evidence_reference','approval_reference'):
                    if item:references.add(item)
                else:visit(item)
        elif isinstance(value,list):
            for item in value:visit(item)
    visit(context)
    return [dict(evidence_id='EVD-'+hashlib.sha256(ref.encode()).hexdigest()[:16],reference=ref)
            for ref in sorted(references)]


def assemble_packet(request: RetrievalRequest, product: dict, brand: dict, visual: dict,
                    creatives: dict, performance: dict, sales: dict,
                    product_image: tuple[dict,bytes], campaign_images: list[tuple[dict,bytes]]) -> EvidenceBundle:
    """Combine validated reader outputs and verify consistency across domains."""
    sku = request.canonical_sku;identity = product['product']
    for packet in (product,brand,performance,sales):
        if packet['canonical_sku'] != sku:
            raise ValueError('Cross-SKU evidence cannot be assembled')
    for other in (brand['product'],sales['product'],product_image[0]):
        if any(other[field] != identity[field] for field in ('sku','brand','product_name')):
            raise ValueError('Product identity changed across retrieval reads')
    if sales['currency'] != identity['currency'].upper():
        raise ValueError('Sales and product currencies disagree')
    if (creatives['request'] != request.to_dict() or visual['visual_reference'] != request.visual_reference
            or visual['approval_scope'] != request.visual_reference_scope):
        raise ValueError('Retrieved campaign preferences or styling scope disagree with the request')
    if (brand['policy_authority'] != 'PROJECT_POC_POLICY' or brand['claim_approval_scope'] != 'SYNTHETIC_POC_ONLY'
            or any(p['source_system'] != 'SYNTHETIC_POC' for p in (creatives,performance,sales))):
        raise ValueError('Invalid policy or analytics provenance')
    snapshots = {p['snapshot_id'] for p in (product,brand,visual,creatives,performance)
                 if p.get('snapshot_id') is not None}
    if len(snapshots) != 1 or not next(iter(snapshots)):
        raise ValueError('Evidence contains inconsistent knowledge snapshots')
    if claim_signature(product['poc_identity_claims']) != claim_signature(brand['approved_claims']):
        raise ValueError('Current approved claims disagree across retrieval reads')
    for claim in brand['approved_claims']:
        if (claim['sku'] != sku or claim['claim_type'] != 'PRODUCT_NAME'
                or claim['claim_text'] != identity['product_name'] or claim['approval_status'] != 'APPROVED'
                or claim['approval_scope'] != 'SYNTHETIC_POC_ONLY'):
            raise ValueError('Only currently approved exact POC identity wording is allowed')
    selected = {c['campaign_id']:c for c in creatives['creatives']}
    summaries = {p['campaign_id']:p for p in performance['summaries']}
    images_by_campaign = {m['campaign_id']:m for m,_ in campaign_images}
    if (len(selected) != len(creatives['creatives']) or len(summaries) != len(performance['summaries'])
            or len(images_by_campaign) != len(campaign_images)
            or set(selected) != set(summaries) or set(selected) != set(images_by_campaign)):
        raise ValueError('Campaign selections, performance summaries and images must match exactly')
    sales_periods = {c['campaign_id']:c for c in sales['campaign_periods']}
    for campaign_id,creative in selected.items():
        summary = summaries[campaign_id];image = images_by_campaign[campaign_id]
        if any(summary[field] != creative[field] for field in (
                'sku','campaign_id','asset_id','campaign_name','objective','channel','audience','start_date','end_date')):
            raise ValueError('Campaign metadata changed between creative and performance reads')
        if (creative['sku'] != sku or creative['brand'] != identity['brand']
                or creative['product_name'] != identity['product_name']
                or image['asset_id'] != creative['asset_id'] or image['gcs_uri'] != creative['gcs_uri']
                or image['content_sha256'] != creative['banner_sha256'] or image['rank'] != creative['rank']
                or image['snapshot_id'] != next(iter(snapshots)) or summary['currency'] != sales['currency']):
            raise ValueError('Campaign/image identity, ranking, currency or snapshot disagree')
        period = sales_periods.get(campaign_id)
        if (period is None or period['effective_start'] != summary['start_date']
                or period['effective_end'] != summary['end_date']):
            raise ValueError('Sales scope must cover every selected campaign period completely')
        if (summary['conversions']>period['orders'] or summary['attributed_units']>period['units_sold']
                or summary['attributed_revenue']>period['net_sales']):
            raise ValueError('Synthetic attribution exceeds the retrieved campaign-period sales')
    primary,primary_bytes = product_image
    if (primary['asset_id'] != f'AST-{sku}-PRIMARY' or primary['source_system'] != 'BEAUTYFEEDS'
            or primary['gcs_uri'] != identity['primary_image_uri']):
        raise ValueError('Product image must match the authoritative primary image')
    image_manifest = [];raw_by_asset = {}
    ordered_images = [(primary,primary_bytes),*sorted(campaign_images,key=lambda pair:pair[0]['rank'])]
    for metadata,raw in ordered_images:
        asset_id = metadata['asset_id']
        if (metadata['sku'] != sku or asset_id in raw_by_asset
                or not metadata['verification'] or any(v is not True for v in metadata['verification'].values())
                or not {'generation_match','sha256_match','format_match','dimensions_match'}.issubset(metadata['verification'])
                or metadata['byte_count'] != len(raw)):
            raise ValueError('Incomplete image verification or duplicate/cross-SKU bytes')
        if asset_id == primary['asset_id']:
            verify_product_bytes(metadata,raw)
        else:
            if metadata['source_system'] != 'SYNTHETIC_POC':
                raise ValueError('Campaign reference images must retain synthetic provenance')
            verify_campaign_bytes(metadata,raw)
        image_manifest.append(copy.deepcopy(metadata));raw_by_asset[asset_id] = raw
    context = copy.deepcopy(dict(version='evidence-packet-v1',request=request.to_dict(),
        canonical_sku=sku,source_of_truth='BIGQUERY',original_image_store='GCS',
        snapshot_id=next(iter(snapshots)),product_knowledge=product,brand_context=brand,
        visual_reference=visual,campaign_creatives=creatives,campaign_performance=performance,
        sales_analytics=sales,image_manifest=image_manifest,
        generation_constraints=dict(policy_scope='PROJECT_POC',claim_scope='SYNTHETIC_POC_ONLY',
            allowed_identity_wording=[c['claim_text'] for c in brand['approved_claims']],
            benefit_claims_authorized=False,visual_reference_scope=request.visual_reference_scope,
            analytics_source_system='SYNTHETIC_POC'),
        usage_notes=['This packet combines retrieved evidence; it is not a generated campaign brief.',
            'Pass the in-memory original image bytes with grounded text to the next model step.',
            'Styling inspiration supplies visual details only; POC approvals supply identity wording only.',
            'Campaign/sales metrics are simulations and do not prove causal lift or creative effectiveness.',
            'Re-retrieve before generation. Sequential cloud reads are checked for consistency, not an atomic cross-service snapshot.',
            'Exported JSON/images are review artifacts and never serve as runtime fallback.']))
    context['evidence_index'] = evidence_index(context)
    return EvidenceBundle(context,raw_by_asset)


def retrieve_evidence_packet(client: bigquery.Client, gcs: storage.Client, embedding_client: genai.Client,
                             request: RetrievalRequest, campaign_config: dict, sales_config: dict,
                             generated_config: dict | None = None) -> EvidenceBundle:
    validate_config(campaign_config);query_scope(sales_config)
    if generated_config is not None:
        validate_generated_config(generated_config)
    sku = request.canonical_sku;started_at = datetime.now(timezone.utc)
    def stage(name: str, value):
        LOG.info(json.dumps({'event':'evidence_stage_completed','stage':name,'sku':sku}))
        return value
    product = stage('product_knowledge',retrieve_product_knowledge(client,sku))
    brand = stage('brand_context',retrieve_brand_context(client,sku))
    visual = stage('visual_reference',retrieve_visual_reference(client,request.visual_reference))
    creatives = stage('campaign_creatives',retrieve_campaign_creatives(client,embedding_client,request,campaign_config))
    ids = [c['campaign_id'] for c in creatives['creatives']]
    performance = stage('campaign_performance',retrieve_campaign_performance(client,sku,ids))
    sales = stage('sales_analytics',retrieve_sales_analytics(client,sku,sales_config))
    primary = stage('product_image',retrieve_product_image(client,gcs,sku))
    images = stage('campaign_images',retrieve_campaign_images(client,gcs,creatives))
    generated = (stage('generated_banner_references',retrieve_generated_references(client,gcs,request,generated_config))
                 if generated_config is not None else None)
    # Detect changes/revocations while other cloud reads and downloads were running.
    final_brand = retrieve_brand_context(client,sku)
    final_product = retrieve_product_knowledge(client,sku)
    if (final_product != product or {k:v for k,v in final_brand.items() if k != 'checked_at'}
            != {k:v for k,v in brand.items() if k != 'checked_at'}):
        raise ValueError('Product facts, knowledge, policies or approvals changed during retrieval; retrieve again')
    bundle = assemble_packet(request,final_product,final_brand,visual,creatives,performance,sales,primary,images)
    if generated is not None:
        section = build_visual_context(bundle.context,*generated,generated_config)
        for metadata,(_,raw) in zip(section['results'],generated[1]):
            if metadata['asset_id'] in bundle.image_bytes:
                raise ValueError('Generated reference duplicates another image asset')
            bundle.context['image_manifest'].append(copy.deepcopy(metadata))
            bundle.image_bytes[metadata['asset_id']] = raw
        bundle.context['generated_banner_references'] = section
        bundle.context['generation_constraints']['generated_banner_scope'] = section['usage_scope']
        bundle.context['evidence_index'] = evidence_index(bundle.context)
    bundle.context.update(retrieval_started_at=started_at,retrieval_completed_at=datetime.now(timezone.utc),
        approval_recheck_passed=True)
    return bundle


def export_evidence_packet(bundle: EvidenceBundle, output_directory: Path) -> Path:
    """Write a unique review bundle only after all live retrieval checks pass."""
    context = copy.deepcopy(bundle.context)
    # Serialize/validate paths and bytes before creating a review directory.
    encoded = []
    for metadata in context['image_manifest']:
        asset = metadata['asset_id']
        if not re.fullmatch(r'[A-Za-z0-9_-]+',asset) or metadata['mime_type'] not in ('image/jpeg','image/png'):
            raise ValueError('Invalid review image filename or MIME type')
        raw = bundle.image_bytes[asset]
        if hashlib.sha256(raw).hexdigest() != metadata['content_sha256']:
            raise ValueError('Image bytes changed before export')
        suffix = '.jpg' if metadata['mime_type'] == 'image/jpeg' else '.png'
        encoded.append((asset+suffix,raw,metadata))
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:8]
    directory = output_directory/run
    for name,_,metadata in encoded:
        metadata['local_review_path'] = str((directory/'images'/name).resolve())
    payload = json.dumps(context,indent=2,default=json_value)
    (directory/'images').mkdir(parents=True,exist_ok=False)
    for name,raw,_ in encoded:
        (directory/'images'/name).write_bytes(raw)
    destination = directory/'evidence_packet.json'
    destination.write_text(payload)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request',type=Path,required=True)
    parser.add_argument('--campaign-config',type=Path,default=CAMPAIGN_CONFIG)
    parser.add_argument('--sales-config',type=Path,default=SALES_CONFIG)
    parser.add_argument('--generated-banner-config',type=Path,default=GENERATED_CONFIG)
    parser.add_argument('--output-dir',type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    request = RetrievalRequest.from_dict(json.loads(args.request.read_text()))
    campaign_config = json.loads(args.campaign_config.read_text());sales_config = json.loads(args.sales_config.read_text())
    generated_config = json.loads(args.generated_banner_config.read_text());validate_generated_config(generated_config)
    validate_config(campaign_config);query_scope(sales_config)
    client = bigquery.Client(project=PROJECT,location=LOCATION)
    with genai.Client(vertexai=True,project=PROJECT,location=LOCATION,
                      http_options=types.HttpOptions(api_version='v1')) as embedding_client:
        bundle = retrieve_evidence_packet(client,storage.Client(project=PROJECT),embedding_client,
                                         request,campaign_config,sales_config,generated_config)
    output = export_evidence_packet(bundle,args.output_dir or ROOT/f'data/generated/retrieval/{request.canonical_sku}_evidence')
    LOG.info(json.dumps({'event':'evidence_packet_verified','sku':request.canonical_sku,
        'images':len(bundle.image_bytes),'campaigns':len(bundle.context['campaign_creatives']['creatives']),
        'evidence_references':len(bundle.context['evidence_index']),'output':str(output)}))


if __name__ == '__main__':
    main()
