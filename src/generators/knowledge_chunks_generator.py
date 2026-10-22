"""Create evidence-backed RAG chunks, retaining review and synthetic boundaries.

Run: python -m src.generators.knowledge_chunks_generator [--dry-run]
Raw daily sales/performance rows are never chunked. Embeddings are a later step.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

from src.common.bq_merge import upsert
from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT, SOURCE

TARGET = f'{PROJECT}.ci_brand_memory.knowledge_chunks'
POINTER = f'{PROJECT}.ci_brand_memory.knowledge_snapshots'
ACTIVE_VIEW = f'{PROJECT}.ci_brand_memory.active_knowledge_chunks'
VISUAL_VIEW = f'{PROJECT}.ci_brand_memory.poc_visual_reference_chunks'
ELIGIBLE_VIEW = f'{PROJECT}.ci_brand_memory.poc_generation_knowledge_chunks'
LOG = logging.getLogger(__name__)
SCHEMA = [bigquery.SchemaField(n,'STRING',mode='REQUIRED') for n in (
    'snapshot_id','chunk_id','chunk_type','brand','chunk_text','source_system',
    'source_reference','retrieval_status','approval_scope','generator_version','content_sha256')
] + [bigquery.SchemaField(n,'STRING') for n in ('sku','campaign_id','asset_id','claim_id')] + [
    bigquery.SchemaField(n,'STRING',mode='REPEATED') for n in ('evidence_references','image_uris')
] + [bigquery.SchemaField(n,'TIMESTAMP',mode='REQUIRED') for n in ('created_at','updated_at')]
POINTER_SCHEMA = [bigquery.SchemaField(n,'STRING',mode='REQUIRED') for n in (
    'pipeline_id','snapshot_id','source_system')] + [bigquery.SchemaField('row_count','INTEGER',mode='REQUIRED')] + [
    bigquery.SchemaField(n,'TIMESTAMP',mode='REQUIRED') for n in ('created_at','updated_at')]


def split_text(text: str, limit: int) -> list[str]:
    if not isinstance(limit,int) or limit < 200:
        raise ValueError('max_chars must be an integer of at least 200')
    pieces = []
    while len(text) > limit:
        boundary = text.rfind(' ',0,limit+1)
        if boundary <= 0:
            boundary = limit
        pieces.append(text[:boundary].strip())
        text = text[boundary:].strip()
    if text.strip():
        pieces.append(text.strip())
    return pieces


def validate_visual_reference(profile: dict) -> None:
    """Reject full brand research or claim records in a styling-only reference."""
    if (profile.get('canonical_sku_mapping') or profile.get('scope')!='VISUAL_INSPIRATION_ONLY'
            or profile.get('reference_status')!='USER_SELECTED_VISUAL_INSPIRATION'
            or profile.get('brand')!='Naked & Thriving'):
        raise ValueError('Expected an unmapped user-selected Naked & Thriving visual reference')
    if any(key in profile for key in ('tone','published_claim_candidates','performance_preferences')):
        raise ValueError('Claims, tone and performance do not belong in visual-reference retrieval')
    allowed_keys={'role','family','value','classification','evidence_url','evidence_urls',
                  'selector','selectors','hex','color_name'}
    for section in ('typography','colors','layout','creative_patterns'):
        if not profile.get(section):
            raise ValueError(f'Missing visual section: {section}')
        for item in profile[section]:
            if set(item)-allowed_keys:
                raise ValueError('Unexpected nonvisual fields in styling reference')


def build_chunks(products: list[dict], ingredients: list[dict], rules: list[dict],
                 claims: list[dict], approved_ids: set[str], campaigns: list[dict],
                 profile: dict, config: dict) -> list[dict]:
    if len(products)!=20 or {p['sku'] for p in products}!=EXPECTED_SKUS:
        raise ValueError('Expected 20 canonical source products')
    by_sku = {p['sku']:p for p in products}
    if any(p['source_system']!='BEAUTYFEEDS' for p in products):
        raise ValueError('Real product evidence required')
    timestamp = datetime.fromisoformat(config['generated_at'])
    if timestamp.tzinfo is None:
        raise ValueError('Timezone required')
    chunks = []

    def add(kind: str, identity: str, text: str, source: str, brand: str,
            evidence: list[str], status: str='POC_CONTEXT', scope: str='SYNTHETIC_POC_ONLY',
            sku: str|None=None, campaign_id: str|None=None, asset_id: str|None=None,
            claim_id: str|None=None, images: list[str]|None=None) -> None:
        if not evidence or not text.strip():
            raise ValueError('Nonempty evidence and text required')
        if sku is not None and (sku not in by_sku or by_sku[sku]['brand']!=brand):
            raise ValueError('Brand and SKU scope mismatch')
        header = f'Type: {kind}. Brand: {brand}. Status: {status}. Scope: {scope}.\n'
        if source==SOURCE:
            header += 'SYNTHETIC_POC: not actual historical business evidence.\n'
        if status=='REVIEW_ONLY':
            header += 'For review only. Do not use this content as an approved creative claim or brand rule.\n'
        limit = config['max_chars']-len(header)
        for index, part in enumerate(split_text(text,limit)):
            body = header+part
            digest = hashlib.sha256(body.encode()).hexdigest()
            identifier = hashlib.sha256((identity+':'+str(index)+':'+digest).encode()).hexdigest()[:32]
            chunks.append(dict(snapshot_id='',chunk_id='KCH-'+identifier,chunk_type=kind,brand=brand,
                sku=sku,campaign_id=campaign_id,asset_id=asset_id,claim_id=claim_id,chunk_text=body,
                source_system=source,source_reference=identity,evidence_references=sorted(set(evidence)),
                image_uris=sorted(set(images or [])),retrieval_status=status,approval_scope=scope,
                generator_version=config['version'],content_sha256=digest,created_at=timestamp,updated_at=timestamp))

    ingredient_lists = {sku:[] for sku in by_sku}
    for item in sorted(ingredients,key=lambda i:(i['sku'],i['ingredient_sequence'],i['ingredient_id'])):
        if item['sku'] not in by_sku or item['source_system']!='BEAUTYFEEDS':
            raise ValueError('Invalid ingredient evidence')
        ingredient_lists[item['sku']].append(item['inci_name'])
    for p in sorted(products,key=lambda p:p['sku']):
        sku=p['sku']; ref=f'{PROJECT}.ci_product.product_master#sku={sku}'
        if not p['primary_image_uri'] or not ingredient_lists[sku]:
            raise ValueError('Product image and ingredients required')
        add('PRODUCT_KNOWLEDGE',ref,
            f"Canonical SKU: {sku}. Source product name: {p['product_name']}. Brand: {p['brand']}. "
            f"Source classification: {p['product_type']}. Listed price: {p['price']} {p['currency']}. "
            'Product names and classifications are identity metadata, not independent approved benefit claims.',
            'BEAUTYFEEDS',p['brand'],[ref],status='FACTUAL_CONTEXT',scope='SOURCE_PRODUCT_FACTS',
            sku=sku,images=[p['primary_image_uri']])
        ref=f'{PROJECT}.ci_product.product_ingredients#sku={sku}'
        add('INGREDIENT_KNOWLEDGE',ref,
            'Source-listed ingredients in recorded sequence: '+ '; '.join(ingredient_lists[sku])+'. '
            'Presence alone establishes neither concentration nor efficacy. Do not derive benefit claims from this list.',
            'BEAUTYFEEDS',p['brand'],[ref,f'{PROJECT}.ci_product.ingredients'],
            status='FACTUAL_CONTEXT',scope='SOURCE_PRODUCT_FACTS',sku=sku,images=[p['primary_image_uri']])
    for rule in sorted(rules,key=lambda r:r['rule_id']):
        if rule['authority']!='PROJECT_POC_POLICY' or rule['scope']!='PROJECT_POC' or rule['source_system']!=SOURCE:
            raise ValueError('Only explicitly scoped POC policies supported')
        add('POC_RULE',f'{PROJECT}.ci_brand_memory.brand_guidelines#rule_id={rule["rule_id"]}',
            rule['rule_text'],SOURCE,'*',[rule['source_reference']],status='POC_POLICY')
    for claim in sorted(claims,key=lambda c:c['claim_id']):
        sku=claim['sku']
        if sku not in by_sku or claim['source_system']!=SOURCE:
            raise ValueError('Invalid claim scope')
        approved = claim['claim_id'] in approved_ids
        if approved and (claim['approval_status']!='APPROVED' or claim['claim_type']!='PRODUCT_NAME'
                         or claim['approval_scope']!='SYNTHETIC_POC_ONLY'
                         or claim['claim_text']!=by_sku[sku]['product_name']):
            raise ValueError('Invalid POC approved identity claim')
        status='POC_APPROVED' if approved else 'REVIEW_ONLY'
        add('POC_APPROVED_CLAIM' if approved else 'CLAIM_REVIEW_NOTE',
            f'{PROJECT}.ci_brand_memory.product_claims#claim_id={claim["claim_id"]}',
            f"Review status: {claim['approval_status']}. Type: {claim['claim_type']}. "
            f"Source wording: {claim['claim_text']}. Review note: {claim['review_notes']}",
            SOURCE,by_sku[sku]['brand'],[claim['evidence_reference']],status=status,
            sku=sku,claim_id=claim['claim_id'])
    if len(campaigns)!=60 or len({c['campaign_id'] for c in campaigns})!=60:
        raise ValueError('Expected 60 campaigns with one asset and performance summary each')
    for c in sorted(campaigns,key=lambda c:c['campaign_id']):
        sku=c['sku']
        if sku not in by_sku or c['source_system']!=SOURCE or c['performance_days']!=21:
            raise ValueError('Invalid campaign or incomplete aggregate performance')
        ref=f'{PROJECT}.ci_creative.campaign_master#campaign_id={c["campaign_id"]}'
        asset_ref=f'{PROJECT}.ci_creative.campaign_assets#asset_id={c["asset_id"]}'
        add('CAMPAIGN_CREATIVE',ref,
            f"Campaign: {c['campaign_name']}. Dates: {c['start_date']} through {c['end_date']}. "
            f"Channel: {c['channel']}. Audience: {c['audience']}. Objective: {c['objective']}. "
            f"Headline: {c['headline']}. Copy: {c['body_copy']}. CTA: {c['call_to_action']}. "
            f"Template description: {c['visual_description']}. This is a synthetic scenario, not actual campaign history.",
            SOURCE,by_sku[sku]['brand'],[ref,asset_ref],sku=sku,campaign_id=c['campaign_id'],
            asset_id=c['asset_id'],images=[c['gcs_uri'],c['product_image_uri']])
        summary_ref=f'{PROJECT}.ci_creative.campaign_daily_performance#campaign_id={c["campaign_id"]}'
        add('CAMPAIGN_LEARNING',summary_ref,
            f"Synthetic campaign summary for {c['campaign_id']}: {c['impressions']} impressions, "
            f"{c['clicks']} clicks, {c['conversions']} conversions. Modeled spend: {c['spend']} {c['currency']}. "
            f"Modeled attributed revenue: {c['revenue']} {c['currency']}. Aggregate CTR: {c['ctr']}. "
            f"Aggregate conversion rate: {c['conversion_rate']}. Aggregate ROAS: {c['roas']}. "
            'Ratios use aggregate totals, not averages of daily ratios. These metrics use simulated attribution '
            'capped to synthetic SKU sales. They are not causal evidence or proof of creative effectiveness.',
            SOURCE,by_sku[sku]['brand'],[summary_ref,ref,asset_ref],sku=sku,
            campaign_id=c['campaign_id'],asset_id=c['asset_id'],images=[c['gcs_uri']])
    validate_visual_reference(profile)
    for section in ('typography','colors','layout','creative_patterns'):
        evidence = set()
        lines=[]
        for item in profile[section]:
            evidence.update(item.get('evidence_urls',[]))
            if item.get('evidence_url'):evidence.add(item['evidence_url'])
            lines.append(json.dumps({k:v for k,v in item.items() if k not in ('evidence_url','evidence_urls')},sort_keys=True))
        add('WEBSITE_VISUAL_REFERENCE',f"{config['visual_reference']}#{section}",
            '\n'.join(lines)+'\nUser-selected styling inspiration only: CSS, colors, typography, layout and imagery. '
            'Keep SKU identity, product facts and approved claim wording grounded in their own sources. '
            'This reference does not authorize product claims, brand tone or performance conclusions.',
            profile['source_system'],profile['brand'],list(evidence),status='VISUAL_REFERENCE',scope='VISUAL_INSPIRATION_ONLY')
    chunks.sort(key=lambda r:r['chunk_id'])
    if len({r['chunk_id'] for r in chunks})!=len(chunks):
        raise ValueError('Duplicate chunks')
    # Include metadata in the snapshot fingerprint so scope/evidence changes invalidate it.
    fingerprint=json.dumps([{k:v for k,v in r.items() if k not in ('created_at','updated_at','snapshot_id')}
                            for r in chunks],sort_keys=True,separators=(',',':'))
    snapshot=hashlib.sha256(fingerprint.encode()).hexdigest()
    for row in chunks:row['snapshot_id']=snapshot
    return chunks


def create_view(client: bigquery.Client, identifier: str, sql: str) -> None:
    table=bigquery.Table(identifier);table.view_query=sql
    client.create_table(table,exists_ok=True)
    if client.get_table(identifier).view_query!=sql:
        raise ValueError('Existing retrieval view differs; refusing to replace it')


def load_chunks(client: bigquery.Client, rows: list[dict], config: dict) -> dict:
    upsert(client,TARGET,SCHEMA,rows,['snapshot_id','chunk_id','source_system'])
    params=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter('snapshot','STRING',rows[0]['snapshot_id'])])
    stored=[dict(r) for r in client.query(f'SELECT * FROM `{TARGET}` WHERE snapshot_id=@snapshot ORDER BY chunk_id',
        job_config=params,location=LOCATION).result()]
    if stored!=rows:raise ValueError('Stored chunks differ from generated snapshot')
    stats=dict(next(iter(client.query((ROOT/'sql/validate_knowledge_chunks.sql').read_text(),
        job_config=params,location=LOCATION).result())))
    if stats['chunk_count']!=len(rows) or any(stats[k]!=0 for k in (
            'invalid_hashes','invalid_skus','invalid_evidence','invalid_claim_eligibility','invalid_brand_scope')):
        raise ValueError(f'Chunk validation failed: {stats}')
    pointer=dict(pipeline_id=config['pipeline_id'],snapshot_id=rows[0]['snapshot_id'],row_count=len(rows),
        source_system=SOURCE,created_at=rows[0]['created_at'],updated_at=rows[0]['updated_at'])
    upsert(client,POINTER,POINTER_SCHEMA,[pointer],['pipeline_id','source_system'])
    create_view(client,ACTIVE_VIEW,f"""SELECT k.* FROM `{TARGET}` k
        JOIN `{POINTER}` s USING(snapshot_id)
        WHERE s.pipeline_id='{config['pipeline_id']}' AND s.source_system='SYNTHETIC_POC'""")
    create_view(client,ELIGIBLE_VIEW,f"""SELECT k.* FROM `{ACTIVE_VIEW}` k
        WHERE k.retrieval_status IN ('FACTUAL_CONTEXT','POC_POLICY','POC_CONTEXT','POC_APPROVED')
          AND (k.claim_id IS NULL OR EXISTS (SELECT 1
            FROM `{PROJECT}.ci_brand_memory.poc_approved_product_claims` c
            WHERE c.claim_id=k.claim_id AND c.sku=k.sku))""")
    create_view(client,VISUAL_VIEW,f"""SELECT k.* FROM `{ACTIVE_VIEW}` k
        WHERE k.chunk_type='WEBSITE_VISUAL_REFERENCE' AND k.retrieval_status='VISUAL_REFERENCE'
          AND k.approval_scope='VISUAL_INSPIRATION_ONLY' AND k.sku IS NULL
          AND k.brand='Naked & Thriving' AND k.claim_id IS NULL
          AND k.source_system='OFFICIAL_BRAND_WEBSITE'""")
    return stats


def read_sources(client: bigquery.Client, config: dict) -> tuple:
    def query(sql: str,params: bigquery.QueryJobConfig|None=None) -> list[dict]:
        return [dict(r) for r in client.query(sql,job_config=params,location=LOCATION).result()]
    params=bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter('skus','STRING',sorted(EXPECTED_SKUS))])
    products=query(f"""SELECT sku,brand,product_name,product_type,price,currency,primary_image_uri,source_system
        FROM `{PROJECT}.ci_product.product_master` WHERE sku IN UNNEST(@skus)""",params)
    ingredients=query(f"""SELECT pi.sku,pi.ingredient_id,pi.ingredient_sequence,i.inci_name,i.source_system
        FROM `{PROJECT}.ci_product.product_ingredients` pi JOIN `{PROJECT}.ci_product.ingredients` i USING(ingredient_id)
        WHERE pi.sku IN UNNEST(@skus)""",params)
    rules=query(f"SELECT * FROM `{PROJECT}.ci_brand_memory.brand_guidelines` WHERE source_system='SYNTHETIC_POC' AND rule_status='ACTIVE_FOR_POC'")
    claims=query(f"SELECT * FROM `{PROJECT}.ci_brand_memory.product_claims` WHERE sku IN UNNEST(@skus) AND source_system='SYNTHETIC_POC'",params)
    approved={r['claim_id'] for r in query(f'SELECT claim_id FROM `{PROJECT}.ci_brand_memory.poc_approved_product_claims`')}
    sales_config=json.loads((ROOT/'config/sales_generator.json').read_text())
    prefix='SYN-CMP-'+sales_config['start_date'].replace('-','')+'-'
    campaign_params=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('prefix','STRING',prefix)])
    campaigns=query(f"""WITH perf AS (
        SELECT campaign_id,asset_id,COUNT(*) performance_days,SUM(impressions) impressions,
          SUM(clicks) clicks,SUM(conversions) conversions,SUM(spend) spend,SUM(attributed_revenue) revenue,
          COALESCE(SAFE_DIVIDE(CAST(SUM(clicks) AS NUMERIC),SUM(impressions)),0) ctr,
          COALESCE(SAFE_DIVIDE(CAST(SUM(conversions) AS NUMERIC),SUM(clicks)),0) conversion_rate,
          COALESCE(SAFE_DIVIDE(SUM(attributed_revenue),SUM(spend)),0) roas
        FROM `{PROJECT}.ci_creative.campaign_daily_performance`
        WHERE source_system='SYNTHETIC_POC' GROUP BY campaign_id,asset_id
      ) SELECT c.*,a.asset_id,a.gcs_uri,a.visual_description,
          p.performance_days,p.impressions,p.clicks,p.conversions,p.spend,p.revenue,p.ctr,p.conversion_rate,p.roas
        FROM `{PROJECT}.ci_creative.campaign_master` c
        JOIN `{PROJECT}.ci_creative.campaign_assets` a USING(campaign_id,sku)
        JOIN perf p ON p.campaign_id=c.campaign_id AND p.asset_id=a.asset_id
        WHERE c.source_system='SYNTHETIC_POC' AND a.source_system='SYNTHETIC_POC'
          AND STARTS_WITH(c.campaign_id,@prefix) AND a.generator_version='template-v1'""",campaign_params)
    profile=json.loads((ROOT/config['visual_reference']).read_text())
    return products,ingredients,rules,claims,approved,campaigns,profile


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'config/knowledge_chunks.json')
    parser.add_argument('--dry-run',action='store_true')
    args=parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    config=json.loads(args.config.read_text())
    # pipeline_id is interpolated only after a strict identifier check.
    import re
    if not re.fullmatch(r'[A-Za-z0-9_-]+',config['pipeline_id']):raise ValueError('Invalid pipeline_id')
    client=bigquery.Client(project=PROJECT,location=LOCATION)
    rows=build_chunks(*read_sources(client,config),config)
    output=ROOT/'data/generated/knowledge_chunks.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(rows,default=str,indent=2))
    LOG.info(json.dumps({'event':'chunks_generated','count':len(rows),
        'types':dict(Counter(r['chunk_type'] for r in rows)),
        'statuses':dict(Counter(r['retrieval_status'] for r in rows))}))
    if not args.dry_run:LOG.info(json.dumps({'event':'chunks_validated',**load_chunks(client,rows,config)}))


if __name__=='__main__':main()
