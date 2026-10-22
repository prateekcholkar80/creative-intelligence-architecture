"""Read existing product knowledge by canonical SKU, without model calls or cloud writes.

Run: python -m src.retrieval.product_knowledge --sku CI-SKU-0001
Writes a local evidence packet under data/generated/retrieval. Exact SKU lookup
returns all product/ingredient chunks; semantic ranking is a separate next step.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.request import CANONICAL_SKUS

LOG = logging.getLogger(__name__)
SQL = ROOT/'sql/retrieve_product_knowledge.sql'
DEFAULT_MAXIMUM_BYTES_BILLED = 64 * 1024 * 1024


@dataclass(frozen=True)
class ProductKnowledgeRead:
    """Validated packet plus the native BigQuery read that produced it."""

    packet: dict
    query_job_id: str
    observed_at: datetime
    project: str
    location: str


def _validate_read_job(job: object) -> tuple[str, str, str]:
    job_id = getattr(job, 'job_id', None)
    project = getattr(job, 'project', None)
    location = getattr(job, 'location', None)
    if (not isinstance(job_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', job_id)
            or project != PROJECT or location != LOCATION):
        raise ValueError('BigQuery read job identity does not match the fixed project and region')
    return job_id, project, location


def validate_sku(sku: str) -> None:
    if not isinstance(sku, str) or sku not in CANONICAL_SKUS:
        raise ValueError('Expected CI-SKU-0001 through CI-SKU-0020, exactly as stored')


def build_packet(sku: str, result: dict) -> dict:
    """Fail closed on cross-SKU evidence, invalid hashes, or ineligible claim content."""
    validate_sku(sku)
    products = [dict(p) for p in result['products']]
    chunks = [dict(c) for c in result['knowledge_chunks']]
    claims = [dict(c) for c in result['poc_identity_claims']]
    if len(products) != 1:
        raise ValueError('Expected exactly one existing product for this SKU')
    product = products[0]
    image = f'gs://creative-brand-memory-{PROJECT}/products/{sku}/primary.jpg'
    if (product['sku'] != sku or product['source_system'] != 'BEAUTYFEEDS'
            or not product['brand'] or not product['product_name'] or product['primary_image_uri'] != image):
        raise ValueError('Invalid canonical product evidence')
    by_claim = {c['claim_id']: c for c in claims}
    if len(by_claim) != len(claims):
        raise ValueError('Duplicate eligible claim IDs')
    claim_evidence = f'{PROJECT}.ci_product.product_master#sku={sku}&field=product_name'
    for claim in claims:
        if (claim['sku'] != sku or claim['claim_type'] != 'PRODUCT_NAME'
                or claim['claim_text'] != product['product_name']
                or claim['evidence_excerpt'] != product['product_name']
                or claim['evidence_reference'] != claim_evidence
                or claim['evidence_source_system'] != 'BEAUTYFEEDS'
                or claim['source_system'] != 'SYNTHETIC_POC'
                or claim['approval_status'] != 'APPROVED' or claim['approval_scope'] != 'SYNTHETIC_POC_ONLY'
                or claim['approved_by'] != 'SYNTHETIC_POC_REVIEWER'
                or claim['approval_reference'] != 'config/brand_memory.json#review_fixture'
                or not isinstance(claim['approved_at'], datetime) or claim['approved_at'].tzinfo is None
                or claim['approved_at'] > datetime.now(timezone.utc)):
            raise ValueError('Claim is not currently eligible POC identity evidence')
    if not chunks or len({c['chunk_id'] for c in chunks}) != len(chunks):
        raise ValueError('Unique nonempty product knowledge required')
    snapshots = {c['snapshot_id'] for c in chunks}
    if len(snapshots) != 1 or not next(iter(snapshots)):
        raise ValueError('Knowledge must belong to one current snapshot')
    facts, identity_chunks = [], []
    for chunk in chunks:
        if (chunk['sku'] != sku or chunk['brand'] != product['brand']
                or not chunk['chunk_text'].strip() or not chunk['evidence_references']
                or hashlib.sha256(chunk['chunk_text'].encode()).hexdigest() != chunk['content_sha256']):
            raise ValueError('Invalid chunk scope, evidence, or content hash')
        kind = chunk['chunk_type']
        if kind in ('PRODUCT_KNOWLEDGE', 'INGREDIENT_KNOWLEDGE'):
            table = 'product_master' if kind == 'PRODUCT_KNOWLEDGE' else 'product_ingredients'
            reference = f'{PROJECT}.ci_product.{table}#sku={sku}'
            if (chunk['source_system'] != 'BEAUTYFEEDS' or chunk['retrieval_status'] != 'FACTUAL_CONTEXT'
                    or chunk['approval_scope'] != 'SOURCE_PRODUCT_FACTS' or chunk['claim_id'] is not None
                    or chunk['source_reference'] != reference or reference not in chunk['evidence_references']
                    or chunk['image_uris'] != [image]
                    or (kind == 'PRODUCT_KNOWLEDGE' and product['product_name'] not in chunk['chunk_text'])):
                raise ValueError('Invalid factual product chunk')
            facts.append(chunk)
        elif kind == 'POC_APPROVED_CLAIM':
            claim = by_claim.get(chunk['claim_id'])
            if (claim is None or chunk['source_system'] != 'SYNTHETIC_POC'
                    or chunk['retrieval_status'] != 'POC_APPROVED'
                    or chunk['approval_scope'] != 'SYNTHETIC_POC_ONLY'
                    or claim['evidence_reference'] not in chunk['evidence_references']
                    or claim['claim_text'] not in chunk['chunk_text']):
                raise ValueError('Stale or ineligible identity claim chunk')
            identity_chunks.append(chunk)
        else:
            raise ValueError('Only product facts, ingredients, and eligible POC identity allowed')
    if not {'PRODUCT_KNOWLEDGE', 'INGREDIENT_KNOWLEDGE'} <= {c['chunk_type'] for c in facts}:
        raise ValueError('Product and ingredient knowledge both required')
    return dict(canonical_sku=sku,retrieval_method='EXACT_SKU_LOOKUP',snapshot_id=next(iter(snapshots)),
        product=product,product_evidence_reference=f'{PROJECT}.ci_product.product_master#sku={sku}',
        factual_chunks=facts,poc_identity_claims=claims,poc_identity_chunks=identity_chunks,
        usage_notes=[
            'Product names and classifications are identity metadata, not independently approved benefit claims.',
            'Ingredient wording is retained as recorded; presence alone establishes neither concentration nor efficacy.',
            'SYNTHETIC_POC_ONLY identity approval is a mock review, not manufacturer approval.',
        ])


def capture_product_knowledge(client: bigquery.Client, sku: str,
                              maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED) -> ProductKnowledgeRead:
    """Execute and capture one fresh authoritative product-knowledge read."""
    validate_sku(sku)
    if type(maximum_bytes_billed) is not int or maximum_bytes_billed <= 0:
        raise ValueError('maximum_bytes_billed must be a positive integer')
    query_job = client.query(SQL.read_text(),location=LOCATION,job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter('sku','STRING',sku)],
        maximum_bytes_billed=maximum_bytes_billed,use_query_cache=False))
    rows = list(query_job.result())
    if len(rows) != 1:
        raise ValueError('Expected one product lookup result')
    job_id, project, location = _validate_read_job(query_job)
    return ProductKnowledgeRead(packet=build_packet(sku,dict(rows[0])), query_job_id=job_id,
        observed_at=datetime.now(timezone.utc), project=project, location=location)


def retrieve_product_knowledge(client: bigquery.Client, sku: str,
                               maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED) -> dict:
    """Compatibility facade returning only the validated authoritative packet."""
    return capture_product_knowledge(client,sku,maximum_bytes_billed).packet


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sku',required=True,choices=sorted(CANONICAL_SKUS))
    parser.add_argument('--output',type=Path,help='Optional local JSON output path')
    parser.add_argument('--maximum-bytes-billed',type=int,default=DEFAULT_MAXIMUM_BYTES_BILLED,
        help='Read-only query billing cap in bytes (default: 64 MiB)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    client = bigquery.Client(project=PROJECT,location=LOCATION)  # Existing ADC; no keys.
    packet = retrieve_product_knowledge(client,args.sku,args.maximum_bytes_billed)
    packet['retrieved_at'] = datetime.now(timezone.utc).isoformat()
    output = args.output or ROOT/f'data/generated/retrieval/{args.sku}_product_knowledge.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(packet,default=str,indent=2))
    LOG.info(json.dumps({'event':'product_knowledge_retrieved','sku':args.sku,
        'product_name':packet['product']['product_name'],'factual_chunks':len(packet['factual_chunks']),
        'poc_identity_chunks':len(packet['poc_identity_chunks']),
        'eligible_poc_identity_claims':len(packet['poc_identity_claims']),'output':str(output)}))


if __name__ == '__main__':
    main()
