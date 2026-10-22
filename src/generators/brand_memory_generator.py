"""Seed POC rules and evidence-backed claims with explicitly simulated review outcomes.

Run: python -m src.generators.brand_memory_generator [--dry-run]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

from src.common.bq_merge import upsert
from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT, SOURCE

RULE_TABLE = f'{PROJECT}.ci_brand_memory.brand_guidelines'
CLAIM_TABLE = f'{PROJECT}.ci_brand_memory.product_claims'
VIEW = f'{PROJECT}.ci_brand_memory.poc_approved_product_claims'
LOG = logging.getLogger(__name__)
RULE_SCHEMA = [bigquery.SchemaField(n,'STRING',mode='REQUIRED') for n in (
    'rule_id','brand','scope','category','rule_text','rule_status','authority',
    'source_system','source_reference','generator_version')] + [
    bigquery.SchemaField(n,'TIMESTAMP',mode='REQUIRED') for n in ('created_at','updated_at')]
CLAIM_SCHEMA = [bigquery.SchemaField(n,'STRING',mode='REQUIRED') for n in (
    'claim_id','sku','claim_type','claim_text','evidence_reference','evidence_excerpt',
    'evidence_source_system','approval_status','approval_scope','review_notes','source_system','generator_version')] + [
    bigquery.SchemaField(n,'STRING') for n in ('approved_by','approval_reference')] + [
    bigquery.SchemaField('approved_at','TIMESTAMP')] + [
    bigquery.SchemaField(n,'TIMESTAMP',mode='REQUIRED') for n in ('created_at','updated_at')]


def build_memory(products: list[dict], ingredients: list[dict], config: dict) -> tuple[list[dict], list[dict]]:
    if len(products) != 20 or {p['sku'] for p in products} != EXPECTED_SKUS:
        raise ValueError('Expected exactly the 20 canonical products')
    timestamp = datetime.fromisoformat(config['generated_at'])
    if timestamp.tzinfo is None:
        raise ValueError('Timezone required for generated_at')
    rules = []
    for rule in config['rules']:
        if not rule['rule_id'].startswith('POC-') or not rule['rule_text'].strip():
            raise ValueError('Rules must explicitly belong to the POC')
        rules.append(dict(**rule, brand='*', scope='PROJECT_POC', rule_status='ACTIVE_FOR_POC',
            authority='PROJECT_POC_POLICY', source_system=SOURCE,
            source_reference='config/brand_memory.json#' + rule['rule_id'],
            generator_version=config['version'], created_at=timestamp, updated_at=timestamp))
    if len({r['rule_id'] for r in rules}) != len(rules):
        raise ValueError('Duplicate rule IDs')
    first_ingredients = {}
    for item in sorted(ingredients, key=lambda x: (x['sku'], x['ingredient_sequence'], x['ingredient_id'])):
        if item['sku'] not in EXPECTED_SKUS or item['source_system'] != 'BEAUTYFEEDS' or not item['inci_name']:
            raise ValueError('Invalid ingredient evidence')
        first_ingredients.setdefault(item['sku'], item)
    fixture = config['review_fixture']
    if fixture['scope'] != 'SYNTHETIC_POC_ONLY' or fixture['reviewer'] != 'SYNTHETIC_POC_REVIEWER':
        raise ValueError('Review fixtures must explicitly remain synthetic')
    claims = []
    for product in sorted(products, key=lambda p:p['sku']):
        sku = product['sku']
        if product['source_system'] != 'BEAUTYFEEDS' or not product['product_name'] or not product['brand']:
            raise ValueError('Missing real product identity evidence')
        if sku not in first_ingredients:
            raise ValueError(f'Missing ingredient evidence for {sku}')
        ingredient = first_ingredients[sku]
        facts = [
            ('PRODUCT_NAME', product['product_name'], product['product_name'],
             f'{PROJECT}.ci_product.product_master#sku={sku}&field=product_name'),
            ('BRAND', product['brand'], product['brand'],
             f'{PROJECT}.ci_product.product_master#sku={sku}&field=brand'),
            ('INGREDIENT_PRESENCE', f"Source ingredient list includes {ingredient['inci_name']}.",
             ingredient['inci_name'], f"{PROJECT}.ci_product.product_ingredients#sku={sku}&ingredient_id={ingredient['ingredient_id']}"),
        ]
        for kind, text, excerpt, evidence in facts:
            status = fixture['statuses_by_type'][kind]
            if status not in {'APPROVED','REJECTED','PENDING_REVIEW'}:
                raise ValueError('Invalid simulated review status')
            if status == 'APPROVED' and kind != 'PRODUCT_NAME':
                raise ValueError('Only exact product identity may receive mock approval')
            digest = hashlib.sha256((evidence+'\n'+text).encode()).hexdigest()[:16]
            claims.append(dict(claim_id=f'POC-CLM-{sku}-{kind}-{digest}', sku=sku,
                claim_type=kind, claim_text=text, evidence_reference=evidence, evidence_excerpt=excerpt,
                evidence_source_system='BEAUTYFEEDS', approval_status=status,
                approval_scope=fixture['scope'], review_notes=fixture['notes_by_type'][kind],
                approved_by=fixture['reviewer'] if status == 'APPROVED' else None,
                approval_reference='config/brand_memory.json#review_fixture' if status == 'APPROVED' else None,
                approved_at=timestamp if status == 'APPROVED' else None,
                source_system=SOURCE, generator_version=config['version'],
                created_at=timestamp, updated_at=timestamp))
    return rules, claims


def approved_view_sql() -> str:
    """Expose mock-approved identity facts only; not manufacturer benefit approvals."""
    return f"""SELECT c.* FROM `{CLAIM_TABLE}` c
        JOIN `{PROJECT}.ci_product.product_master` p USING(sku)
        WHERE c.approval_status='APPROVED'
          AND c.approval_scope='SYNTHETIC_POC_ONLY' AND c.source_system='SYNTHETIC_POC'
          AND c.claim_type='PRODUCT_NAME' AND c.approved_by='SYNTHETIC_POC_REVIEWER'
          AND c.approval_reference='config/brand_memory.json#review_fixture'
          AND c.approved_at IS NOT NULL AND c.approved_at <= CURRENT_TIMESTAMP()
          AND c.evidence_source_system='BEAUTYFEEDS' AND p.source_system='BEAUTYFEEDS'
          AND c.claim_text=p.product_name AND c.evidence_excerpt=p.product_name
          AND c.evidence_reference=CONCAT('{PROJECT}.ci_product.product_master#sku=',
              p.sku,'&field=product_name')"""


def load_memory(client: bigquery.Client, rules: list[dict], claims: list[dict]) -> dict:
    upsert(client, RULE_TABLE, RULE_SCHEMA, rules, ['rule_id','source_system'])
    # Reviewed records are never downgraded or overwritten by the seed generator.
    upsert(client, CLAIM_TABLE, CLAIM_SCHEMA, claims, ['claim_id','source_system'], pending_only=True)
    table = bigquery.Table(VIEW)
    table.view_query = approved_view_sql()
    client.create_table(table, exists_ok=True)
    if client.get_table(VIEW).view_query != table.view_query:
        raise ValueError('Existing approved view differs; refusing to replace it')
    params = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter('rule_ids','STRING',[r['rule_id'] for r in rules]),
        bigquery.ArrayQueryParameter('claim_ids','STRING',[c['claim_id'] for c in claims])])
    result = dict(next(iter(client.query((ROOT/'sql/validate_brand_memory.sql').read_text(),
        job_config=params, location=LOCATION).result())))
    if result['rule_count'] != len(rules) or result['claim_count'] != 60 or result['sku_count'] != 20 or any(
            result[k] != 0 for k in ('duplicate_claims','invalid_poc_rules','missing_evidence','invalid_approvals')):
        raise ValueError(f'Brand memory validation failed: {result}')
    stored_rules = [dict(r) for r in client.query(f"""SELECT * FROM `{RULE_TABLE}`
        WHERE rule_id IN UNNEST(@rule_ids) AND source_system='SYNTHETIC_POC'
        ORDER BY rule_id""",job_config=params,location=LOCATION).result()]
    if stored_rules != sorted(rules,key=lambda r:r['rule_id']):
        raise ValueError('Stored POC rules differ')
    stored_claims = [dict(r) for r in client.query(f"""SELECT * FROM `{CLAIM_TABLE}`
        WHERE claim_id IN UNNEST(@claim_ids) AND source_system='SYNTHETIC_POC'
        ORDER BY claim_id""",job_config=params,location=LOCATION).result()]
    wanted = {c['claim_id']:c for c in claims}
    for row in stored_claims:
        if row['approval_status']=='PENDING_REVIEW' and row != wanted[row['claim_id']]:
            raise ValueError('Stored pending claim differs')
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'config/brand_memory.json')
    parser.add_argument('--dry-run',action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    config = json.loads(args.config.read_text())
    client = bigquery.Client(project=PROJECT,location=LOCATION)
    params = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter('skus','STRING',sorted(EXPECTED_SKUS))])
    products = [dict(r) for r in client.query(f"""SELECT sku, brand, product_name, source_system
        FROM `{PROJECT}.ci_product.product_master` WHERE sku IN UNNEST(@skus)""",
        job_config=params,location=LOCATION).result()]
    ingredients = [dict(r) for r in client.query(f"""SELECT pi.sku, pi.ingredient_id,
        pi.ingredient_sequence, i.inci_name, i.source_system
        FROM `{PROJECT}.ci_product.product_ingredients` pi
        JOIN `{PROJECT}.ci_product.ingredients` i USING(ingredient_id)
        WHERE pi.sku IN UNNEST(@skus)""",job_config=params,location=LOCATION).result()]
    rules, claims = build_memory(products,ingredients,config)
    output = ROOT/'data/generated/brand_memory_review.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(dict(poc_rules=rules,synthetic_review_claims=claims),default=str,indent=2))
    if not args.dry_run:
        LOG.info(json.dumps({'event':'brand_memory_validated',**load_memory(client,rules,claims)}))
    else:
        LOG.info(json.dumps({'event':'brand_memory_preview','rules':len(rules),'claims':len(claims)}))


if __name__=='__main__':
    main()
