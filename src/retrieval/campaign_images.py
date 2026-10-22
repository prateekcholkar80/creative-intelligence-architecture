"""Download original ranked campaign banners with BQ/GCS provenance verification.

Run: python -m src.retrieval.campaign_images --request config/retrieval_request.example.json
The CLI searches BigQuery afresh; it never reads exported retrieval JSON. One
ephemeral query embedding ranks creatives. Original PNG bytes are returned for
later model use, with no image generation, re-encoding, or cloud writes.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.cloud import bigquery, storage
from google.genai import types
from PIL import Image

from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.campaign_creatives import CONFIG, retrieve_campaign_creatives, validate_config
from src.retrieval.product_image import BUCKET
from src.retrieval.request import RetrievalRequest

SQL = ROOT/'sql/retrieve_campaign_images.sql'
LOG = logging.getLogger(__name__)
DEFAULT_MAXIMUM_BYTES_BILLED = 128*1024*1024
# These values must still match the live ranked selection, not a local export.
SELECTION_FIELDS = ('asset_id','campaign_id','sku','brand','product_name','gcs_uri',
    'source_image_uri','banner_sha256','headline','body_copy','call_to_action',
    'visual_description','objective','channel','audience')


@dataclass(frozen=True)
class CampaignImagesRead:
    """Verified ranked campaign images plus their native BigQuery eligibility read."""

    images: list[tuple[dict,bytes]]
    query_job_id: str
    observed_at: datetime
    project: str
    location: str


def _validate_read_job(job: object) -> tuple[str,str,str]:
    job_id=getattr(job,'job_id',None);project=getattr(job,'project',None);location=getattr(job,'location',None)
    if (not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',job_id)
            or project!=PROJECT or location!=LOCATION):
        raise ValueError('BigQuery campaign-image read does not match the fixed project and region')
    return job_id,project,location


def validate_metadata(sku: str, snapshot: str, creative: dict, row: dict) -> int:
    generation = row['gcs_generation']
    campaign_id = row['campaign_id']
    if (any(row[name] != creative[name] for name in SELECTION_FIELDS)
            or any(str(row[name]) != str(creative[name]) for name in ('start_date','end_date'))
            or row['sku'] != sku or row['snapshot_id'] != snapshot
            or row['chunk_id'] != creative['chunk_id']
            or row['creative_content_sha256'] != creative['content_sha256']
            or not re.fullmatch(rf'SYN-CMP-[0-9]{{8}}-{re.escape(sku)}-[0-9]{{2}}',campaign_id)
            or row['asset_id'] != f'SYN-AST-{campaign_id}-template-v1'
            or row['source_system'] != 'SYNTHETIC_POC' or row['generator_version'] != 'template-v1'
            or row['asset_type'] != 'IMAGE' or row['asset_role'] != 'CAMPAIGN_BANNER'
            or row['retrieval_status'] != 'POC_CONTEXT' or row['claim_id'] is not None
            or row['approval_scope'] != 'SYNTHETIC_POC_ONLY'
            or row['source_image_uri'] != f'gs://{BUCKET}/products/{sku}/primary.jpg'
            or not re.fullmatch(r'[a-f0-9]{64}',row['content_sha256'])
            or row['banner_sha256'] != row['content_sha256']
            or row['gcs_uri'] != f'gs://{BUCKET}/campaigns/{campaign_id}/template-v1/{row["content_sha256"]}.png'
            or row['mime_type'] != 'image/png' or not row['embedding_id']
            or not isinstance(generation,str) or not re.fullmatch(r'[1-9][0-9]*',generation)
            or row['model_id'] != 'multimodalembedding@001' or row['model_location'] != LOCATION
            or row['dimension'] != 512 or row['task_type'] != 'IMAGE'
            or any(type(row[name]) is not int or row[name] <= 0 for name in ('width','height'))
            or not {row['gcs_uri'],row['source_image_uri']}.issubset(row['image_uris'])
            or not {f'{PROJECT}.ci_creative.campaign_master#campaign_id={campaign_id}',
                    f'{PROJECT}.ci_creative.campaign_assets#asset_id={row["asset_id"]}'
                    }.issubset(row['evidence_references'])):
        raise ValueError('Campaign selection changed or banner identity/embedding provenance is invalid')
    return int(generation)


def verify_image_bytes(row: dict, raw: bytes) -> None:
    if not raw or hashlib.sha256(raw).hexdigest() != row['content_sha256']:
        raise ValueError('Downloaded campaign image differs from BigQuery content hash')
    with Image.open(io.BytesIO(raw)) as image:
        if image.format != 'PNG' or image.size != (row['width'],row['height']):
            raise ValueError('Campaign image format or dimensions differ from BigQuery metadata')
        image.verify()


def _retrieve_campaign_images(client: bigquery.Client, gcs: storage.Client, packet: dict,
                              maximum_bytes_billed: int) -> tuple[list[tuple[dict,bytes]],object | None]:
    request = RetrievalRequest.from_dict(packet['request'])
    if type(maximum_bytes_billed) is not int or maximum_bytes_billed <= 0:
        raise ValueError('maximum_bytes_billed must be a positive integer')
    creatives = packet['creatives'];snapshot = packet['snapshot_id']
    if (packet['source_of_truth'] != 'BIGQUERY' or packet['source_system'] != 'SYNTHETIC_POC'
            or packet['approval_scope'] != 'SYNTHETIC_POC_ONLY'
            or not isinstance(creatives,list) or len(creatives) > 20
            or len({c['asset_id'] for c in creatives}) != len(creatives)
            or len({c['chunk_id'] for c in creatives}) != len(creatives)
            or any(c['sku'] != request.canonical_sku or c['snapshot_id'] != snapshot
                   or type(c['rank']) is not int or c['rank'] != rank
                   for rank,c in enumerate(creatives,1))
            or (creatives and (not isinstance(snapshot,str) or not snapshot))):
        raise ValueError('Expected unique, ranked, same-SKU BigQuery selections from one snapshot')
    if not creatives:
        return [],None
    params = [bigquery.ScalarQueryParameter('sku','STRING',request.canonical_sku),
        bigquery.ScalarQueryParameter('snapshot','STRING',snapshot),
        bigquery.ArrayQueryParameter('assets','STRING',[c['asset_id'] for c in creatives]),
        bigquery.ArrayQueryParameter('chunks','STRING',[c['chunk_id'] for c in creatives])]
    query_job=client.query(SQL.read_text(),location=LOCATION,
        job_config=bigquery.QueryJobConfig(query_parameters=params,
            maximum_bytes_billed=maximum_bytes_billed,use_query_cache=False))
    rows=[dict(row) for row in query_job.result()]
    by_asset = {row['asset_id']:row for row in rows}
    if len(rows) != len(by_asset) or set(by_asset) != {c['asset_id'] for c in creatives}:
        raise ValueError('Selected banners are missing, duplicated, or no longer eligible in BigQuery')
    # Validate the whole batch before downloading any objects; return only a complete batch.
    generations = {c['asset_id']:validate_metadata(request.canonical_sku,snapshot,c,by_asset[c['asset_id']])
                   for c in creatives}
    images = []
    for creative in creatives:
        row = by_asset[creative['asset_id']];generation = generations[row['asset_id']]
        blob = gcs.bucket(BUCKET).blob(row['gcs_uri'].removeprefix(f'gs://{BUCKET}/'))
        blob.reload()
        if int(blob.generation) != generation or blob.content_type != row['mime_type']:
            raise ValueError('Current GCS banner differs from BigQuery generation or MIME metadata')
        raw = blob.download_as_bytes(if_generation_match=generation)
        verify_image_bytes(row,raw)
        evidence = sorted(set(row['evidence_references']) | {
            f'{PROJECT}.ci_brand_memory.image_embeddings#embedding_id={row["embedding_id"]}'})
        metadata = dict(row,canonical_sku=request.canonical_sku,source_of_truth='BIGQUERY',
            rank=creative['rank'],distance=creative['distance'],objective_match=creative['objective_match'],
            channel_match=creative['channel_match'],evidence_references=evidence,
            retrieval_method='RANKED_CAMPAIGN_IMAGE_LOOKUP',byte_count=len(raw),
            verification=dict(generation_match=True,mime_match=True,sha256_match=True,
                              format_match=True,dimensions_match=True))
        images.append((metadata,raw))
    return images,query_job


def capture_campaign_images(client: bigquery.Client, gcs: storage.Client, packet: dict,
                            maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED) -> CampaignImagesRead:
    """Capture a nonempty ranked batch with current BQ eligibility and pinned bytes."""
    images,query_job=_retrieve_campaign_images(client,gcs,packet,maximum_bytes_billed)
    if query_job is None:
        raise ValueError('An empty campaign selection has no native BigQuery evidence read')
    job_id,project,location=_validate_read_job(query_job)
    return CampaignImagesRead(images=images,query_job_id=job_id,
        observed_at=datetime.now(timezone.utc),project=project,location=location)


def retrieve_campaign_images(client: bigquery.Client, gcs: storage.Client, packet: dict,
                             maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED) -> list[tuple[dict,bytes]]:
    """Compatibility facade; an explicit empty ranking performs no query."""
    return _retrieve_campaign_images(client,gcs,packet,maximum_bytes_billed)[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request',type=Path,required=True)
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--output-dir',type=Path)
    parser.add_argument('--maximum-bytes-billed',type=int,default=DEFAULT_MAXIMUM_BYTES_BILLED)
    args = parser.parse_args()
    if args.maximum_bytes_billed <= 0:
        parser.error('--maximum-bytes-billed must be positive')
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    request = RetrievalRequest.from_dict(json.loads(args.request.read_text()))
    config = json.loads(args.config.read_text());validate_config(config)
    client = bigquery.Client(project=PROJECT,location=LOCATION)
    with genai.Client(vertexai=True,project=PROJECT,location=config['location'],
                      http_options=types.HttpOptions(api_version='v1')) as embedding_client:
        packet = retrieve_campaign_creatives(client,embedding_client,request,config)
    images = retrieve_campaign_images(client,storage.Client(project=PROJECT),packet,args.maximum_bytes_billed)
    output = args.output_dir or ROOT/f'data/generated/retrieval/images/{request.canonical_sku}/campaigns'
    output.mkdir(parents=True,exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()
    metadata_rows = []
    for metadata,raw in images:
        path = output/f'{metadata["asset_id"]}.png'
        path.write_bytes(raw)
        metadata_rows.append(dict(metadata,retrieved_at=timestamp,local_review_path=str(path)))
    manifest = dict(request=request.to_dict(),source_of_truth='BIGQUERY',source_system='SYNTHETIC_POC',
        snapshot_id=packet['snapshot_id'],retrieved_at=timestamp,images=metadata_rows,
        usage_notes=['Original GCS bytes; local files are review copies, never retrieval fallback.',
                     'Synthetic POC creative references do not establish approved claims or real performance.'])
    (output/'metadata.json').write_text(json.dumps(manifest,indent=2,default=lambda value:value.isoformat()))
    LOG.info(json.dumps({'event':'campaign_images_verified','sku':request.canonical_sku,
        'images':len(images),'bytes':sum(len(raw) for _,raw in images),'output':str(output)}))


if __name__ == '__main__':
    main()
