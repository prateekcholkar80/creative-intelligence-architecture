"""Revalidate a local banner candidate, upload immutable PNG, register POC metadata.

Run: python -m src.ingestion.register_generated_banner --request config/retrieval_request.example.json --preview-dir <review-directory>
Local JSON supplies candidate creative/settings/provenance for ingestion only.
All product facts, images, rules, approvals and analytics are freshly retrieved
from BigQuery/GCS; missing cloud evidence never falls back to the candidate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google import genai
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import bigquery, storage
from google.genai import types

from src.generators.banner_preview_generator import BannerPreview, render_preview
from src.generators.campaign_brief_generator import assemble_brief
from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT
from src.retrieval.brand_context import retrieve_brand_context
from src.retrieval.campaign_creatives import CONFIG as CAMPAIGN_CONFIG
from src.retrieval.campaign_performance import json_value
from src.retrieval.evidence_packet import EvidenceBundle, retrieve_evidence_packet
from src.retrieval.product_image import retrieve_product_image
from src.retrieval.product_knowledge import retrieve_product_knowledge
from src.retrieval.request import RetrievalRequest
from src.retrieval.sales_analytics import CONFIG as SALES_CONFIG
from src.ingestion.generated_banner_reviews import CONFIG as REVIEW_CONFIG, initialize_review

BUCKET = f'creative-brand-memory-{PROJECT}'
TARGET = f'{PROJECT}.ci_creative.generated_campaign_assets'
SQL = ROOT/'sql/register_generated_banner.sql'
VALIDATION_SQL = ROOT/'sql/validate_generated_banner.sql'
CONFIG = ROOT/'config/generated_banner_registration.json'
LOG = logging.getLogger(__name__)
SCHEMA = [bigquery.SchemaField(name,'STRING',mode='REQUIRED') for name in (
    'asset_id','sku','source_system','status','approval_scope','asset_role','mime_type',
    'gcs_uri','gcs_generation','sha256','source_image_asset_id','source_image_uri',
    'source_image_generation','source_image_sha256','generator_version','description_method',
    'visual_description','objective','channel','audience','headline','body_copy','call_to_action',
    'knowledge_snapshot_id','brief_json','evidence_packet_json','policy_json',
)] + [bigquery.SchemaField(name,'INTEGER',mode='REQUIRED') for name in ('width','height','byte_count')
] + [bigquery.SchemaField('real_business_approved','BOOLEAN',mode='REQUIRED'),
     bigquery.SchemaField('approved_claim_ids','STRING',mode='REPEATED'),
     bigquery.SchemaField('evidence_references','STRING',mode='REPEATED')
] + [bigquery.SchemaField(name,'TIMESTAMP',mode='REQUIRED') for name in ('created_at','last_verified_at')]
IMMUTABLE = tuple(field.name for field in SCHEMA if field.name not in {
    'created_at','last_verified_at','brief_json','evidence_packet_json','policy_json',
    'approved_claim_ids','evidence_references'})


def encode(value: object) -> str:
    return json.dumps(value,sort_keys=True,separators=(',',':'),default=json_value,allow_nan=False)


def inspect_inventory(client: bigquery.Client, gcs: storage.Client) -> dict:
    """Read existing ecosystem counts and original object names, without writes."""
    for name in ('ci_product','ci_sales','ci_creative','ci_brand_memory'):
        if client.get_dataset(f'{PROJECT}.{name}').location.lower() != LOCATION:
            raise ValueError('Existing dataset location differs from the project region')
    params = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter('skus','STRING',sorted(EXPECTED_SKUS))],maximum_bytes_billed=128*1024*1024)
    sql = f"""SELECT
      (SELECT COUNT(*) FROM `{PROJECT}.ci_product.product_master`
        WHERE sku IN UNNEST(@skus) AND source_system='BEAUTYFEEDS') products,
      (SELECT COUNT(*) FROM `{PROJECT}.ci_product.product_assets`
        WHERE sku IN UNNEST(@skus) AND source_system='BEAUTYFEEDS') product_assets,
      (SELECT COUNT(*) FROM `{PROJECT}.ci_sales.sku_daily_sales`
        WHERE sku IN UNNEST(@skus) AND source_system='SYNTHETIC_POC') sales_rows,
      (SELECT COUNT(*) FROM `{PROJECT}.ci_creative.campaign_master`
        WHERE sku IN UNNEST(@skus) AND source_system='SYNTHETIC_POC') historical_campaigns,
      (SELECT COUNT(*) FROM `{PROJECT}.ci_creative.campaign_assets`
        WHERE sku IN UNNEST(@skus) AND source_system='SYNTHETIC_POC' AND generator_version='template-v1') historical_assets"""
    result = dict(next(iter(client.query(sql,location=LOCATION,job_config=params).result())))
    expected_names = {f'products/{sku}/primary.jpg' for sku in EXPECTED_SKUS}
    found = {blob.name for blob in gcs.list_blobs(BUCKET,prefix='products/') if blob.name in expected_names}
    if (found != expected_names or result['products'] != 20 or result['product_assets'] != 20):
        raise ValueError('The existing 20 canonical products and original assets must be present')
    result['original_gcs_images'] = len(found)
    try:
        client.get_table(TARGET)
    except NotFound:
        result['generated_assets'] = 0
    else:
        result['generated_assets'] = next(iter(client.query(
            f"SELECT COUNT(*) n FROM `{TARGET}` WHERE source_system='SYNTHETIC_POC'",
            location=LOCATION,job_config=bigquery.QueryJobConfig(maximum_bytes_billed=128*1024*1024)).result()))['n']
    LOG.info(json.dumps(dict(event='existing_ecosystem_verified',**result)))
    return result


def revalidate_candidate(client: bigquery.Client, gcs: storage.Client, embedding_client: genai.Client,
                         request: RetrievalRequest, preview_directory: Path,
                         campaign_config: dict, sales_config: dict) -> tuple[BannerPreview,dict,EvidenceBundle]:
    candidate = json.loads((preview_directory/'banner_preview.json').read_text())
    proposal = json.loads((preview_directory/'campaign_brief.json').read_text())
    png = (preview_directory/'banner_preview.png').read_bytes()
    if (candidate['canonical_sku'] != request.canonical_sku or proposal['request'] != request.to_dict()
            or candidate['source_system'] != 'SYNTHETIC_POC'
            or proposal['source_system'] != 'SYNTHETIC_POC'
            or candidate['status'] != 'LOCAL_REVIEW_PREVIEW'
            or candidate['sha256'] != hashlib.sha256(png).hexdigest()):
        raise ValueError('Invalid same-request POC candidate or PNG hash')
    # The only candidate inputs used to rebuild are creative/settings and model
    # provenance. Its cached factual/approval/analytics blocks supply no evidence.
    bundle = retrieve_evidence_packet(client,gcs,embedding_client,request,campaign_config,sales_config)
    brief = assemble_brief(bundle.context,proposal['model_selection'],proposal['model'])
    brief.update(approval_recheck_passed=True,approval_checked_at=bundle.context['brand_context']['checked_at'])
    preview = render_preview(brief,bundle,candidate['render_config'])
    if (preview.png != png
            or any(encode(candidate.get(key)) != encode(value) for key,value in preview.metadata.items())):
        raise ValueError('Candidate differs from a reconstruction grounded on current BigQuery/GCS evidence')
    preview.metadata.update(approval_recheck_passed=True,
                            approval_checked_at=bundle.context['brand_context']['checked_at'])
    return preview,brief,bundle


def registration_row(preview: BannerPreview, brief: dict, bundle: EvidenceBundle) -> dict:
    if (not preview.metadata.get('approval_recheck_passed')
            or hashlib.sha256(preview.png).hexdigest() != preview.metadata['sha256']):
        raise ValueError('A revalidated preview is required for registration')
    source = preview.metadata['original_image'];sku = brief['canonical_sku'];digest = preview.metadata['sha256']
    asset_id = f'SYN-GEN-{sku}-{digest}'
    name = f'generated_campaigns/{sku}/{preview.metadata["generator_version"]}/{digest}.png'
    timestamp = datetime.now(timezone.utc)
    return dict(asset_id=asset_id,sku=sku,source_system='SYNTHETIC_POC',status='REGISTERED_POC_DRAFT',
        approval_scope='SYNTHETIC_POC_ONLY',real_business_approved=False,asset_role='GENERATED_CAMPAIGN_BANNER',
        mime_type='image/png',gcs_uri=f'gs://{BUCKET}/{name}',gcs_generation='PENDING_UPLOAD',sha256=digest,
        source_image_asset_id=source['asset_id'],source_image_uri=source['gcs_uri'],
        source_image_generation=source['gcs_generation'],source_image_sha256=source['content_sha256'],
        width=preview.metadata['width'],height=preview.metadata['height'],byte_count=len(preview.png),
        generator_version=preview.metadata['generator_version'],description_method='DETERMINISTIC_COMPOSITION',
        visual_description=(f'POC banner for {sku}. Layout: {brief["visual_direction"]["layout"]} '
            f'Complete original product image contained without cropping or packaging regeneration. '
            f'Background {brief["visual_direction"]["background"]}; text {brief["visual_direction"]["text_color"]}. '
            f'Exact product identity headline, neutral copy, {brief["copy"]["cta"]} CTA, SYNTHETIC POC PREVIEW footer.'),
        objective=brief['request']['objective'],channel=brief['request']['channel'],audience=brief['request']['audience'],
        headline=brief['copy']['headline'],body_copy=brief['copy']['body'],call_to_action=brief['copy']['cta'],
        knowledge_snapshot_id=brief['snapshot_id'],approved_claim_ids=brief['copy']['claim_ids'],
        evidence_references=[e['reference'] for e in brief['evidence_index']],
        brief_json=encode(brief),evidence_packet_json=encode(bundle.context),
        policy_json=encode(bundle.context['brand_context']['poc_rules']),created_at=timestamp,last_verified_at=timestamp)


def ensure_destination(client: bigquery.Client) -> bigquery.Table:
    table = bigquery.Table(TARGET,schema=SCHEMA)
    table.description = 'Generated POC draft banners; not historical campaigns or real-business approvals.'
    table.clustering_fields = ['sku','channel']
    client.create_table(table,exists_ok=True)
    destination = client.get_table(TARGET)
    if {(f.name,f.field_type,f.mode) for f in destination.schema} != {(f.name,f.field_type,f.mode) for f in SCHEMA}:
        raise ValueError('Existing generated asset schema differs; refusing to alter it')
    return destination


def upload_banner(gcs: storage.Client, row: dict, png: bytes) -> str:
    expected_prefix = f'gs://{BUCKET}/generated_campaigns/{row["sku"]}/{row["generator_version"]}/'
    if (row['gcs_uri'] != expected_prefix+row['sha256']+'.png'
            or row['source_system'] != 'SYNTHETIC_POC'
            or hashlib.sha256(png).hexdigest() != row['sha256']):
        raise ValueError('Invalid immutable upload path, provenance or content hash')
    blob = gcs.bucket(BUCKET).blob(row['gcs_uri'].removeprefix(f'gs://{BUCKET}/'))
    metadata = dict(source_system='SYNTHETIC_POC',asset_id=row['asset_id'],sku=row['sku'],sha256=row['sha256'])
    blob.metadata = metadata
    try:
        blob.upload_from_string(png,content_type='image/png',if_generation_match=0)
    except PreconditionFailed:
        LOG.info(json.dumps(dict(event='existing_banner_object_reused',asset_id=row['asset_id'])))
    # Never overwrite. Pin the read-back to the actual object generation.
    blob.reload()
    if (blob.content_type != 'image/png' or blob.size != len(png)
            or any((blob.metadata or {}).get(k) != v for k,v in metadata.items()) or not blob.generation):
        raise ValueError('Stored GCS banner metadata differs; refusing to reuse')
    stored = blob.download_as_bytes(if_generation_match=int(blob.generation))
    if stored != png:
        raise ValueError('Stored GCS banner differs from verified PNG; refusing to overwrite')
    return str(blob.generation)


def recheck_sources(client: bigquery.Client, gcs: storage.Client, bundle: EvidenceBundle) -> None:
    sku = bundle.context['canonical_sku']
    brand = retrieve_brand_context(client,sku);product = retrieve_product_knowledge(client,sku)
    original,_ = retrieve_product_image(client,gcs,sku)
    without_time = lambda packet: {k:v for k,v in packet.items() if k != 'checked_at'}
    primary = next(m for m in bundle.context['image_manifest'] if m['source_system'] == 'BEAUTYFEEDS')
    if (product != bundle.context['product_knowledge'] or original != primary
            or without_time(brand) != without_time(bundle.context['brand_context'])):
        raise ValueError('Product, original image, policy or approvals changed before registration; retry from fresh evidence')


def load_registration(client: bigquery.Client, row: dict, destination: bigquery.Table,
                      maximum_bytes_billed: int = 512*1024*1024) -> dict:
    columns = [field.name for field in destination.schema]
    if (set(row) != set(columns) or not row['gcs_generation'].isdigit()
            or not row['approved_claim_ids'] or row['real_business_approved'] is not False):
        raise ValueError('Complete uploaded POC draft payload required')
    stage = f'{PROJECT}.ci_creative._stage_generated_banner_{uuid.uuid4().hex}'
    table = bigquery.Table(stage,schema=destination.schema)
    table.expires = datetime.now(timezone.utc)+timedelta(hours=1)
    client.create_table(table)
    try:
        # JSON load with the actual destination schema: no pandas inference.
        client.load_table_from_json([json.loads(encode(row))],stage,location=LOCATION,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,autodetect=False,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY)).result()
        script = SQL.read_text().replace('{stage}',stage)
        script = script.replace('{immutable_mismatch}',' OR '.join(f'T.{c} IS DISTINCT FROM S.{c}' for c in IMMUTABLE))
        script = script.replace('{columns}',', '.join(columns)).replace('{values}',', '.join('S.'+c for c in columns))
        client.query(script,location=LOCATION,job_config=bigquery.QueryJobConfig(
            maximum_bytes_billed=maximum_bytes_billed)).result()
    finally:
        client.delete_table(stage,not_found_ok=True)
    params = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('asset_id','STRING',row['asset_id'])],
                                   maximum_bytes_billed=128*1024*1024)
    validation = dict(next(iter(client.query(VALIDATION_SQL.read_text(),location=LOCATION,job_config=params).result())))
    if validation != dict(asset_count=1,invalid_metadata=0,missing_product_evidence=0,missing_current_approvals=0):
        raise ValueError(f'Generated banner validation failed: {validation}')
    stored = [dict(r) for r in client.query(f"SELECT * FROM `{TARGET}` WHERE asset_id=@asset_id AND source_system='SYNTHETIC_POC'",
                                          location=LOCATION,job_config=params).result()]
    if (len(stored) != 1 or any(stored[0][key] != value for key,value in row.items() if key != 'created_at')):
        raise ValueError('BigQuery registered payload differs from validated banner metadata')
    return stored[0]


def register_banner(client: bigquery.Client, gcs: storage.Client, embedding_client: genai.Client,
                    request: RetrievalRequest, preview_directory: Path,
                    campaign_config: dict, sales_config: dict,
                    maximum_bytes_billed: int = 512*1024*1024) -> dict:
    if type(maximum_bytes_billed) is not int or not 128*1024*1024 <= maximum_bytes_billed <= 1024*1024*1024:
        raise ValueError('Registration transaction cap must be between 128 MiB and 1 GiB')
    preview,brief,bundle = revalidate_candidate(client,gcs,embedding_client,request,preview_directory,campaign_config,sales_config)
    row = registration_row(preview,brief,bundle)
    destination = ensure_destination(client)
    recheck_sources(client,gcs,bundle)
    row['gcs_generation'] = upload_banner(gcs,row,preview.png)
    recheck_sources(client,gcs,bundle)
    stored = load_registration(client,row,destination,maximum_bytes_billed)
    initialize_review(client,stored,json.loads(REVIEW_CONFIG.read_text()))
    LOG.info(json.dumps(dict(event='generated_banner_registered',asset_id=stored['asset_id'],
        sku=request.canonical_sku,gcs_uri=stored['gcs_uri'],gcs_generation=stored['gcs_generation'],
        source_system=stored['source_system'],status=stored['status'])))
    return stored


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request',type=Path,required=True)
    parser.add_argument('--preview-dir',type=Path,required=True)
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--campaign-config',type=Path,default=CAMPAIGN_CONFIG)
    parser.add_argument('--sales-config',type=Path,default=SALES_CONFIG)
    parser.add_argument('--dry-run',action='store_true',help='Revalidate candidate without cloud writes')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    request = RetrievalRequest.from_dict(json.loads(args.request.read_text()))
    config = json.loads(args.config.read_text())
    if set(config) != {'maximum_bytes_billed'}:
        raise ValueError('Registration config must contain only maximum_bytes_billed')
    cap = config['maximum_bytes_billed']
    if type(cap) is not int or not 128*1024*1024 <= cap <= 1024*1024*1024:
        raise ValueError('Registration transaction cap must be between 128 MiB and 1 GiB')
    campaign_config = json.loads(args.campaign_config.read_text());sales_config = json.loads(args.sales_config.read_text())
    client = bigquery.Client(project=PROJECT,location=LOCATION);gcs = storage.Client(project=PROJECT)
    before = inspect_inventory(client,gcs)
    with genai.Client(vertexai=True,project=PROJECT,location=LOCATION,
                      http_options=types.HttpOptions(api_version='v1')) as embedding_client:
        if args.dry_run:
            preview,brief,bundle = revalidate_candidate(client,gcs,embedding_client,request,args.preview_dir,campaign_config,sales_config)
            LOG.info(json.dumps(dict(event='banner_registration_dry_run_verified',
                asset_id=registration_row(preview,brief,bundle)['asset_id'],cloud_writes=False)))
            return
        stored = register_banner(client,gcs,embedding_client,request,args.preview_dir,campaign_config,sales_config,cap)
    after = inspect_inventory(client,gcs)
    if any(after[key] != value for key,value in before.items() if key != 'generated_assets'):
        raise ValueError('Existing ecosystem inventory changed during registration; inspect concurrent activity')
    # Receipt is an audit export; authoritative registration is the BigQuery row.
    (args.preview_dir/'registration_receipt.json').write_text(json.dumps(stored,indent=2,default=json_value))
    (args.preview_dir/'registration_verification.json').write_text(json.dumps(dict(before=before,after=after,
        asset_id=stored['asset_id'],gcs_generation=stored['gcs_generation'],existing_ecosystem_counts_unchanged=True),indent=2))


if __name__ == '__main__':
    main()
