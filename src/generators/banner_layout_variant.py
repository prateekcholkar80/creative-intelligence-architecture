"""Create a local layout variant of an approved BigQuery banner.

Only the control's creative selection is reused. Product facts, approvals,
styling, analytics and originals are retrieved afresh from BigQuery/GCS.
No local brief is a serving source; this command makes no cloud writes.
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.cloud import bigquery, storage
from google.genai import types

from src.generators.banner_preview_generator import (
    CONFIG as PREVIEW_CONFIG, BannerPreview, export_preview, render_preview, validate_config,
)
from src.generators.campaign_brief_generator import LAYOUTS, assemble_brief
from src.generators.generated_banner_embeddings import CONFIG as EMBEDDING_CONFIG
from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.ingestion.generated_banner_reviews import CONFIG as REVIEW_CONFIG, read_asset
from src.ingestion.register_generated_banner import recheck_sources
from src.retrieval.campaign_creatives import CONFIG as CAMPAIGN_CONFIG
from src.retrieval.campaign_performance import json_value
from src.retrieval.evidence_packet import EvidenceBundle, retrieve_evidence_packet
from src.retrieval.generated_banner import retrieve_generated_banner
from src.retrieval.request import RetrievalRequest
from src.retrieval.sales_analytics import CONFIG as SALES_CONFIG

CONFIG = ROOT/'config/banner_layout_variant.json'
LOG = logging.getLogger(__name__)


def generate_variant(client: bigquery.Client, gcs: storage.Client, model_client: genai.Client,
                     request: RetrievalRequest, asset_id: str,
                     campaign_config: dict, sales_config: dict, preview_config: dict,
                     embedding_config: dict, review_config: dict, layout: str
                     ) -> tuple[BannerPreview,dict,EvidenceBundle,dict,bytes]:
    """Fail closed when the control cannot be reproduced from current evidence."""
    validate_config(preview_config)
    if layout not in LAYOUTS:
        raise ValueError('Unsupported layout variant')
    control, control_png = retrieve_generated_banner(
        client,gcs,request.canonical_sku,asset_id,embedding_config)
    asset = read_asset(client,request.canonical_sku,asset_id,review_config)
    proposal = json.loads(asset['brief_json'])
    if proposal['request'] != request.to_dict():
        raise ValueError('The approved control must match the requested campaign')
    draft = proposal['model_selection']
    if layout == draft['layout']:
        raise ValueError('Variant layout must differ from the approved control')
    if draft['test_variable'] != 'LAYOUT':
        raise ValueError('The control must describe a layout experiment')
    bundle = retrieve_evidence_packet(client,gcs,model_client,request,campaign_config,sales_config)
    baseline = assemble_brief(bundle.context,draft,proposal['model'])
    baseline.update(approval_recheck_passed=True,
                    approval_checked_at=bundle.context['brand_context']['checked_at'])
    reconstructed = render_preview(baseline,bundle,preview_config)
    if (reconstructed.png != control_png or control['asset_id'] != asset['asset_id']
            or control['content_sha256'] != asset['sha256']
            or control['gcs_generation'] != asset['gcs_generation']
            or control['snapshot_id'] != bundle.context['snapshot_id']):
        raise ValueError('Approved control differs from current evidence, fonts or render settings')
    # This is a deterministic intervention, not a fresh Gemini creative selection.
    selection = {**draft,'layout':layout}
    provenance = dict(selection_method='DETERMINISTIC_LAYOUT_VARIANT',
        parent_asset_id=asset_id,parent_review=copy.deepcopy(control['review']),
        parent_model=copy.deepcopy(proposal['model']),new_creative_model_calls=0)
    brief = assemble_brief(bundle.context,selection,provenance)
    brief.update(approval_recheck_passed=True,
                 approval_checked_at=bundle.context['brand_context']['checked_at'])
    preview = render_preview(brief,bundle,preview_config)
    if (brief['copy'] != baseline['copy'] or preview.png == control_png
            or any(brief['visual_direction'][key] != baseline['visual_direction'][key]
                   for key in ('background','text_color','typography'))
            or preview.metadata['original_image'] != reconstructed.metadata['original_image']
            or preview.metadata['font_files'] != reconstructed.metadata['font_files']):
        raise ValueError('Variant must change layout while preserving copy, source and fonts')
    recheck_sources(client,gcs,bundle)
    latest, latest_png = retrieve_generated_banner(client,gcs,request.canonical_sku,asset_id,embedding_config)
    if (latest != control or latest_png != control_png
            or read_asset(client,request.canonical_sku,asset_id,review_config) != asset):
        raise ValueError('Control approval or registered asset changed during rendering')
    preview.metadata.update(approval_recheck_passed=True,
        approval_checked_at=bundle.context['brand_context']['checked_at'],created_at=datetime.now(timezone.utc))
    report = dict(source_of_truth='BIGQUERY',source_system='SYNTHETIC_POC',
        control_asset_id=asset_id,control_review=control['review'],
        control_sha256=control['content_sha256'],variant_sha256=preview.metadata['sha256'],
        variant_review_status='NOT_REGISTERED',cloud_writes=False,
        changed_creative_fields=['layout'],control_layout=draft['layout'],variant_layout=layout,
        copy_preserved=True,original_product_image_preserved=True,fonts_preserved=True,
        palette_preserved=True,control_reproduced_from_current_evidence=True,
        post_render_sources_and_control_rechecked=True,new_creative_model_calls=0,
        performance_comparison='UNMEASURED')
    return preview,brief,bundle,report,control_png


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request',type=Path,required=True)
    parser.add_argument('--control-asset-id',required=True)
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--preview-config',type=Path,default=PREVIEW_CONFIG)
    parser.add_argument('--output-dir',type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    settings = json.loads(args.config.read_text())
    if set(settings) != {'layout'} or settings['layout'] not in LAYOUTS:
        raise ValueError('Configure exactly one supported variant layout')
    request = RetrievalRequest.from_dict(json.loads(args.request.read_text()))
    with genai.Client(vertexai=True,project=PROJECT,location=LOCATION,
                      http_options=types.HttpOptions(api_version='v1')) as model_client:
        preview,brief,bundle,report,control_png = generate_variant(
            bigquery.Client(project=PROJECT,location=LOCATION),storage.Client(project=PROJECT),
            model_client,request,args.control_asset_id,
            json.loads(CAMPAIGN_CONFIG.read_text()),json.loads(SALES_CONFIG.read_text()),
            json.loads(args.preview_config.read_text()),json.loads(EMBEDDING_CONFIG.read_text()),
            json.loads(REVIEW_CONFIG.read_text()),settings['layout'])
    destination = export_preview(preview,brief,bundle,
        args.output_dir or ROOT/f'data/generated/banner_variants/{request.canonical_sku}')
    (destination.parent/'control_banner.png').write_bytes(control_png)
    (destination.parent/'layout_comparison.json').write_text(json.dumps(report,indent=2,default=json_value))
    LOG.info(json.dumps(dict(event='layout_variant_verified',output=str(destination),**report)))


if __name__ == '__main__':
    main()
