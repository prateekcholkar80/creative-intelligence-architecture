"""Retrieve current applicable POC policy and eligible identity wording from BigQuery.

Run: python -m src.retrieval.brand_context --sku CI-SKU-0001
No model calls, cloud writes or local evidence fallback. The POC approval view
authorizes identity wording only; it supplies no manufacturer benefit approvals.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.campaign_performance import json_value
from src.retrieval.product_knowledge import validate_sku
from src.retrieval.request import CANONICAL_SKUS

SQL = ROOT/'sql/retrieve_brand_context.sql'
LOG = logging.getLogger(__name__)
DEFAULT_MAXIMUM_BYTES_BILLED = 96*1024*1024


@dataclass(frozen=True)
class BrandContextRead:
    """Validated policy packet and the native BigQuery read that produced it."""

    packet: dict
    query_job_id: str
    observed_at: datetime
    project: str
    location: str


def _validate_read_job(job: object) -> tuple[str, str, str]:
    job_id = getattr(job,'job_id',None)
    project = getattr(job,'project',None)
    location = getattr(job,'location',None)
    if (not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',job_id)
            or project != PROJECT or location != LOCATION):
        raise ValueError('BigQuery policy read job identity does not match the fixed project and region')
    return job_id,project,location


def build_packet(sku: str, result: dict) -> dict:
    validate_sku(sku)
    checked_at = result['checked_at']
    if not isinstance(checked_at,datetime) or checked_at.tzinfo is None:
        raise ValueError('A timezone-aware BigQuery approval-check timestamp is required')
    products = [dict(p) for p in result['products']]
    if len(products) != 1:
        raise ValueError('Expected exactly one current canonical product')
    product = products[0]
    if (product['sku'] != sku or product['source_system'] != 'BEAUTYFEEDS'
            or not product['brand'] or not product['product_name']):
        raise ValueError('Invalid product identity or provenance')
    rules = [dict(r) for r in result['poc_rules']]
    chunks = [dict(c) for c in result['policy_chunks']]
    claims = [dict(c) for c in result['approved_claims']]
    if (not rules or len({r['rule_id'] for r in rules}) != len(rules)
            or len({c['chunk_id'] for c in chunks}) != len(chunks)
            or len({c['claim_id'] for c in claims}) != len(claims)):
        raise ValueError('Unique nonempty applicable POC rules and unique evidence IDs required')
    by_reference = {}
    for rule in rules:
        if (rule['brand'] not in ('*',product['brand']) or rule['scope'] != 'PROJECT_POC'
                or rule['authority'] != 'PROJECT_POC_POLICY' or rule['rule_status'] != 'ACTIVE_FOR_POC'
                or rule['source_system'] != 'SYNTHETIC_POC' or not rule['rule_text'].strip()
                or not rule['rule_id'].startswith('POC-') or not rule['source_reference']):
            raise ValueError('Inapplicable, inactive or incorrectly scoped POC rule')
        reference = f'{PROJECT}.ci_brand_memory.brand_guidelines#rule_id={rule["rule_id"]}'
        by_reference[reference] = rule
        rule['evidence_references'] = [reference,rule['source_reference']]
    snapshots = {c['snapshot_id'] for c in chunks}
    if len(snapshots) != 1 or not next(iter(snapshots)):
        raise ValueError('Policy chunks must come from one current BigQuery snapshot')
    matched = set()
    for chunk in chunks:
        reference = chunk['source_reference'];rule = by_reference.get(reference)
        if rule is None or reference in matched:
            raise ValueError('Unknown or duplicate policy evidence')
        expected_text = (f'Type: POC_RULE. Brand: {rule["brand"]}. Status: POC_POLICY. '
            f'Scope: SYNTHETIC_POC_ONLY.\nSYNTHETIC_POC: not actual historical business evidence.\n'
            f'{rule["rule_text"]}')
        if (chunk['chunk_type'] != 'POC_RULE' or chunk['brand'] != rule['brand']
                or any(chunk[name] is not None for name in ('sku','campaign_id','asset_id','claim_id'))
                or chunk['source_system'] != 'SYNTHETIC_POC' or chunk['retrieval_status'] != 'POC_POLICY'
                or chunk['approval_scope'] != 'SYNTHETIC_POC_ONLY' or chunk['image_uris']
                or rule['source_reference'] not in chunk['evidence_references']
                or chunk['chunk_text'] != expected_text
                or hashlib.sha256(chunk['chunk_text'].encode()).hexdigest() != chunk['content_sha256']):
            raise ValueError('Stale policy wording, scope, provenance or content hash')
        matched.add(reference)
    if matched != set(by_reference):
        raise ValueError('An active applicable rule is missing current knowledge evidence')
    evidence = f'{PROJECT}.ci_product.product_master#sku={sku}&field=product_name'
    for claim in claims:
        approved_at = claim['approved_at']
        if (claim['sku'] != sku or claim['claim_type'] != 'PRODUCT_NAME'
                or claim['claim_text'] != product['product_name'] or claim['evidence_excerpt'] != product['product_name']
                or claim['evidence_reference'] != evidence or claim['evidence_source_system'] != 'BEAUTYFEEDS'
                or claim['source_system'] != 'SYNTHETIC_POC' or claim['approval_status'] != 'APPROVED'
                or claim['approval_scope'] != 'SYNTHETIC_POC_ONLY'
                or claim['approved_by'] != 'SYNTHETIC_POC_REVIEWER'
                or claim['approval_reference'] != 'config/brand_memory.json#review_fixture'
                or not isinstance(approved_at,datetime) or approved_at.tzinfo is None or approved_at>checked_at):
            raise ValueError('Claim is not currently eligible exact POC identity wording')
        claim['evidence_references'] = [
            f'{PROJECT}.ci_brand_memory.product_claims#claim_id={claim["claim_id"]}',
            evidence,claim['approval_reference']]
    return dict(canonical_sku=sku,product=product,source_of_truth='BIGQUERY',checked_at=checked_at,
        snapshot_id=next(iter(snapshots)),retrieval_method='CURRENT_POC_POLICY_AND_APPROVAL_LOOKUP',
        policy_authority='PROJECT_POC_POLICY',claim_approval_scope='SYNTHETIC_POC_ONLY',
        poc_rules=sorted(rules,key=lambda r:r['rule_id']),policy_chunks=sorted(chunks,key=lambda c:c['chunk_id']),
        approved_claims=sorted(claims,key=lambda c:c['claim_id']),
        allowed_claim_types=sorted({c['claim_type'] for c in claims}),
        usage_notes=['Rules are project POC policies, not manufacturer brand guidelines.',
            'Approved wording is exact product identity for synthetic POC creatives only; approval is a mock review.',
            'This approval view supplies no authorized benefit claims. Pending/rejected claims are excluded.',
            'Ingredient presence does not establish concentrations, clinical efficacy or benefits.',
            'Naked & Thriving styling inspiration has a separate retrieval path and does not authorize policy or claims.',
            'Re-read BigQuery eligibility before generating a brief or banner; exports are review artifacts only.'])


def capture_brand_context(client: bigquery.Client, sku: str,
                          maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED) -> BrandContextRead:
    """Execute and capture one fresh authoritative policy/approval read."""
    validate_sku(sku)
    if type(maximum_bytes_billed) is not int or maximum_bytes_billed<=0:
        raise ValueError('maximum_bytes_billed must be a positive integer')
    query_job = client.query(SQL.read_text(),location=LOCATION,job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter('sku','STRING',sku)],
        maximum_bytes_billed=maximum_bytes_billed,use_query_cache=False))
    result = list(query_job.result())
    if len(result) != 1:
        raise ValueError('Expected one current rule/approval lookup result')
    packet = build_packet(sku,dict(result[0]))
    job_id,project,location = _validate_read_job(query_job)
    return BrandContextRead(packet=packet,query_job_id=job_id,observed_at=packet['checked_at'],
        project=project,location=location)


def retrieve_brand_context(client: bigquery.Client, sku: str,
                           maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED) -> dict:
    """Compatibility facade returning only the validated authoritative packet."""
    return capture_brand_context(client,sku,maximum_bytes_billed).packet


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sku',required=True,choices=sorted(CANONICAL_SKUS))
    parser.add_argument('--maximum-bytes-billed',type=int,default=DEFAULT_MAXIMUM_BYTES_BILLED)
    parser.add_argument('--output',type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    packet = retrieve_brand_context(bigquery.Client(project=PROJECT,location=LOCATION),args.sku,args.maximum_bytes_billed)
    output = args.output or ROOT/f'data/generated/retrieval/{args.sku}_brand_context.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(packet,indent=2,default=json_value))
    LOG.info(json.dumps({'event':'brand_context_retrieved','sku':args.sku,
        'rules':len(packet['poc_rules']),'approved_claims':len(packet['approved_claims']),'output':str(output)}))


if __name__ == '__main__':
    main()
