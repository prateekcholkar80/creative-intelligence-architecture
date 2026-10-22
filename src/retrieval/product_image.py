"""Retrieve original product bytes through BigQuery metadata and pinned GCS reads.

Run: python -m src.retrieval.product_image --sku CI-SKU-0001
No model calls, cloud writes, or local evidence fallback. Returned bytes can later
be passed directly to Gemini; local files are review copies only.
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

from google.cloud import bigquery, storage
from PIL import Image

from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.product_knowledge import validate_sku
from src.retrieval.request import CANONICAL_SKUS

BUCKET = f'creative-brand-memory-{PROJECT}'
SQL = ROOT/'sql/retrieve_product_image.sql'
LOG = logging.getLogger(__name__)
# The image eligibility view references more tables than the product-facts lookup.
DEFAULT_IMAGE_MAXIMUM_BYTES_BILLED = 96 * 1024 * 1024


@dataclass(frozen=True)
class ProductImageRead:
    """Verified original bytes plus the native BigQuery eligibility read."""

    metadata: dict
    raw: bytes
    query_job_id: str
    observed_at: datetime
    project: str
    location: str


def _validate_read_job(job: object) -> tuple[str,str,str]:
    job_id=getattr(job,'job_id',None);project=getattr(job,'project',None);location=getattr(job,'location',None)
    if (not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',job_id)
            or project!=PROJECT or location!=LOCATION):
        raise ValueError('BigQuery product-image read does not match the fixed project and region')
    return job_id,project,location


def validate_metadata(sku: str, row: dict) -> int:
    validate_sku(sku)
    generation = row['gcs_generation']
    if (row['sku'] != sku or row['asset_id'] != f'AST-{sku}-PRIMARY'
            or row['source_system'] != 'BEAUTYFEEDS' or not row['brand'] or not row['product_name']
            or row['asset_type'] != 'IMAGE' or row['asset_role'] != 'PRIMARY_PRODUCT_IMAGE'
            or row['is_primary'] is not True
            or row['gcs_uri'] != f'gs://{BUCKET}/products/{sku}/primary.jpg'
            or row['mime_type'] != 'image/jpeg' or not row['embedding_id']
            or not isinstance(generation,str) or not re.fullmatch(r'[1-9][0-9]*',generation)
            or not re.fullmatch(r'[a-f0-9]{64}',row['content_sha256'])
            or row['model_id'] != 'multimodalembedding@001' or row['task_type'] != 'IMAGE'
            or row['model_location'] != LOCATION or row['dimension'] not in (128,256,512,1408)
            or any(type(row[n]) is not int or row[n] <= 0 for n in ('width','height'))):
        raise ValueError('Invalid canonical product image or embedding provenance')
    return int(generation)


def verify_image_bytes(row: dict, raw: bytes) -> None:
    if not raw or hashlib.sha256(raw).hexdigest() != row['content_sha256']:
        raise ValueError('Downloaded image differs from BigQuery content hash')
    with Image.open(io.BytesIO(raw)) as image:
        if image.format != 'JPEG' or image.size != (row['width'],row['height']):
            raise ValueError('Image format or dimensions differ from BigQuery asset metadata')
        image.verify()


def capture_product_image(client: bigquery.Client, gcs: storage.Client, sku: str,
                          maximum_bytes_billed: int = DEFAULT_IMAGE_MAXIMUM_BYTES_BILLED) -> ProductImageRead:
    """Capture one BigQuery-authorized, generation-pinned product image."""
    validate_sku(sku)
    if type(maximum_bytes_billed) is not int or maximum_bytes_billed <= 0:
        raise ValueError('maximum_bytes_billed must be a positive integer')
    query_job=client.query(SQL.read_text(),location=LOCATION,job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter('sku','STRING',sku)],
        maximum_bytes_billed=maximum_bytes_billed,use_query_cache=False))
    rows=list(query_job.result())
    if len(rows) != 1:
        raise ValueError('Expected exactly one current primary product image with embedding provenance')
    row = dict(rows[0]); generation = validate_metadata(sku,row)
    blob = gcs.bucket(BUCKET).blob(f'products/{sku}/primary.jpg')
    blob.reload()
    if int(blob.generation) != generation or blob.content_type != row['mime_type']:
        raise ValueError('Current GCS image differs from BigQuery generation or MIME metadata')
    raw = blob.download_as_bytes(if_generation_match=generation)
    verify_image_bytes(row,raw)
    metadata = dict(row,canonical_sku=sku,retrieval_method='EXACT_PRIMARY_IMAGE_LOOKUP',
        evidence_references=[f'{PROJECT}.ci_product.product_master#sku={sku}',
            f'{PROJECT}.ci_product.product_assets#asset_id={row["asset_id"]}',
            f'{PROJECT}.ci_brand_memory.image_embeddings#embedding_id={row["embedding_id"]}'],
        verification=dict(generation_match=True,sha256_match=True,format_match=True,dimensions_match=True),
        byte_count=len(raw))
    job_id,project,location=_validate_read_job(query_job)
    return ProductImageRead(metadata=metadata,raw=raw,query_job_id=job_id,
        observed_at=datetime.now(timezone.utc),project=project,location=location)


def retrieve_product_image(client: bigquery.Client, gcs: storage.Client, sku: str,
                           maximum_bytes_billed: int = DEFAULT_IMAGE_MAXIMUM_BYTES_BILLED) -> tuple[dict,bytes]:
    """Compatibility facade returning verified metadata and original bytes."""
    captured=capture_product_image(client,gcs,sku,maximum_bytes_billed)
    return captured.metadata,captured.raw


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sku',required=True,choices=sorted(CANONICAL_SKUS))
    parser.add_argument('--output-dir',type=Path)
    parser.add_argument('--maximum-bytes-billed',type=int,default=DEFAULT_IMAGE_MAXIMUM_BYTES_BILLED)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    metadata,raw = retrieve_product_image(bigquery.Client(project=PROJECT,location=LOCATION),
        storage.Client(project=PROJECT),args.sku,args.maximum_bytes_billed)
    output = args.output_dir or ROOT/f'data/generated/retrieval/images/{args.sku}'
    output.mkdir(parents=True,exist_ok=True)
    image_path = output/'primary.jpg'
    image_path.write_bytes(raw)  # Original bytes, without re-encoding or edits.
    metadata.update(retrieved_at=datetime.now(timezone.utc).isoformat(),local_review_path=str(image_path))
    (output/'metadata.json').write_text(json.dumps(metadata,indent=2))
    LOG.info(json.dumps({'event':'product_image_retrieved','sku':args.sku,'bytes':len(raw),
        'width':metadata['width'],'height':metadata['height'],'verification':metadata['verification'],
        'image_path':str(image_path)}))


if __name__=='__main__':main()
