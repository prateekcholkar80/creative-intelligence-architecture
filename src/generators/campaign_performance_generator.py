"""Synthetic daily campaign metrics constrained by existing synthetic SKU sales.

Run: python -m src.generators.campaign_performance_generator [--dry-run]
Assumptions are configurable, not observed benchmarks. EMAIL spend represents
modeled delivery/operating cost. Attribution is simulated, not incremental lift.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT, SOURCE, money

TARGET = f"{PROJECT}.ci_creative.campaign_daily_performance"
VERSION = "performance-v1"
MODEL = "SYNTHETIC_SALES_CAPPED_ATTRIBUTION"
LOG = logging.getLogger(__name__)
SCHEMA = [bigquery.SchemaField(n, "STRING", mode="REQUIRED") for n in (
    "campaign_id", "asset_id", "sku", "channel", "currency", "source_system",
    "generator_version", "attribution_model",
)] + [bigquery.SchemaField("performance_date", "DATE", mode="REQUIRED")] + [
    bigquery.SchemaField(n, "INTEGER", mode="REQUIRED") for n in (
        "impressions", "clicks", "conversions", "attributed_units")
] + [bigquery.SchemaField(n, "NUMERIC", mode="REQUIRED") for n in (
    "spend", "attributed_revenue", "ctr", "conversion_rate", "roas")
] + [bigquery.SchemaField(n, "TIMESTAMP", mode="REQUIRED") for n in ("created_at", "updated_at")]


def ratio(numerator: int | Decimal, denominator: int | Decimal) -> Decimal:
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal("0.000000001"), rounding=ROUND_HALF_UP) if denominator else Decimal(0)


def allocate_budget(budget: Decimal, days: int) -> list[Decimal]:
    if days < 1 or not budget.is_finite() or budget <= 0 or money(budget) != budget:
        raise ValueError("Positive budget with at most two decimal places required")
    cents = int(budget * 100)
    base, remainder = divmod(cents, days)
    return [Decimal(base + (i < remainder)) / 100 for i in range(days)]


def validate_config(config: dict) -> None:
    bound = Decimal(str(config['variation_bound']))
    share = Decimal(config['max_attribution_share'])
    if not bound.is_finite() or not share.is_finite() or not 0 <= bound < 1 or not 0 < share <= 1:
        raise ValueError("Invalid variation or attribution share")
    for params in config['channels'].values():
        cpm = Decimal(params['cost_per_thousand'])
        if not cpm.is_finite() or cpm <= 0:
            raise ValueError("Invalid cost per thousand")
        for key in ('ctr', 'conversion_rate'):
            value = Decimal(params[key])
            if not value.is_finite() or not 0 <= value <= 1:
                raise ValueError(f"Invalid {key}")


def generate_performance(campaigns: list[dict], sales: list[dict], config: dict) -> list[dict]:
    validate_config(config)
    if len(campaigns) != 60 or len({c['campaign_id'] for c in campaigns}) != 60:
        raise ValueError("Expected 60 unique campaigns")
    if {c['sku'] for c in campaigns} != EXPECTED_SKUS or any(
            sum(c['sku'] == sku for c in campaigns) != 3 for sku in EXPECTED_SKUS):
        raise ValueError("Expected three campaigns for every canonical SKU")
    if len({c['asset_id'] for c in campaigns}) != 60:
        raise ValueError("Expected a unique asset per campaign")
    ledger = {}
    for sale in sales:
        key = (sale['sku'], sale['sales_date'])
        if key in ledger:
            raise ValueError("Duplicate SKU sales dates")
        if sale['source_system'] != SOURCE or sale['units_sold'] < 0 or not (
                0 <= sale['orders'] <= sale['units_sold']) or sale['net_sales'] < 0:
            raise ValueError("Invalid synthetic sales ledger")
        ledger[key] = sale
    rows, active_keys = [], set()
    share = Decimal(config['max_attribution_share'])
    for campaign in sorted(campaigns, key=lambda c: c['campaign_id']):
        if campaign['source_system'] != SOURCE:
            raise ValueError("Only synthetic campaigns supported")
        days = (campaign['end_date'] - campaign['start_date']).days + 1
        if days != 21:
            raise ValueError("Expected 21-day campaign periods")
        spend_by_day = allocate_budget(Decimal(campaign['budget']), days)
        params = config['channels'][campaign['channel']]
        seed = int.from_bytes(hashlib.sha256(
            f"{VERSION}:{config['seed']}:{campaign['campaign_id']}".encode()).digest()[:8], 'big')
        rng = random.Random(seed)
        quality = Decimal(str(round(rng.uniform(0.85, 1.15), 6)))
        bound = config['variation_bound']
        for index, spend in enumerate(spend_by_day):
            day = campaign['start_date'] + timedelta(days=index)
            key = (campaign['sku'], day)
            if key in active_keys:
                raise ValueError("Overlapping campaigns would double-count attribution")
            active_keys.add(key)
            if key not in ledger:
                raise ValueError(f"Missing SKU sales for {key}")
            sale = ledger[key]
            if sale['currency'] != campaign['currency']:
                raise ValueError("Sales and campaign currencies differ")
            noise = Decimal(str(round(rng.uniform(1 - bound, 1 + bound), 6)))
            fatigue = Decimal(1) - Decimal(index) / Decimal(days) * Decimal('0.12')
            impressions = max(0, round(spend * 1000 / Decimal(params['cost_per_thousand']) * noise))
            clicks = min(impressions, max(0, round(impressions * Decimal(params['ctr']) * quality * fatigue)))
            conversions = min(clicks, int(sale['orders'] * share), max(0,
                round(clicks * Decimal(params['conversion_rate']) * quality)))
            units = min(int(sale['units_sold'] * share), round(Decimal(conversions) * Decimal('1.12')))
            # Units cannot be below conversions because source orders <= source units.
            revenue = money(sale['net_sales'] * units / sale['units_sold']) if sale['units_sold'] else Decimal('0.00')
            timestamp = campaign['created_at']
            rows.append(dict(campaign_id=campaign['campaign_id'], asset_id=campaign['asset_id'],
                sku=campaign['sku'], channel=campaign['channel'], currency=campaign['currency'],
                performance_date=day, impressions=impressions, clicks=clicks,
                conversions=conversions, attributed_units=units, spend=spend,
                attributed_revenue=revenue, ctr=ratio(clicks, impressions),
                conversion_rate=ratio(conversions, clicks), roas=ratio(revenue, spend),
                source_system=SOURCE, generator_version=VERSION, attribution_model=MODEL,
                created_at=timestamp, updated_at=timestamp))
    return rows


def performance_merge_sql(stage: str, columns: list[str]) -> str:
    keys = {'campaign_id', 'asset_id', 'performance_date', 'source_system'}
    join = ' AND '.join(f'T.{k}=S.{k}' for k in sorted(keys))
    updates = ', '.join(f'T.{c}=S.{c}' for c in columns if c not in keys | {'created_at'})
    return f"""MERGE `{TARGET}` T USING `{stage}` S ON {join}
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ({', '.join(columns)})
        VALUES ({', '.join('S.' + c for c in columns)})"""


def load_performance(client: bigquery.Client, rows: list[dict]) -> None:
    keys = [(r['campaign_id'], r['asset_id'], r['performance_date'], r['source_system']) for r in rows]
    if not rows or len(set(keys)) != len(keys):
        raise ValueError("Unique nonempty performance keys required")
    table = bigquery.Table(TARGET, schema=SCHEMA)
    table.description = 'Synthetic daily campaign performance; assumptions, not actual historical metrics.'
    table.time_partitioning = bigquery.TimePartitioning(field='performance_date')
    table.clustering_fields = ['sku', 'campaign_id']
    client.create_table(table, exists_ok=True)
    destination = client.get_table(TARGET)
    if {(f.name, f.field_type, f.mode) for f in destination.schema} != {
            (f.name, f.field_type, f.mode) for f in SCHEMA}:
        raise ValueError("Existing performance schema differs; refusing to alter it")
    columns = [f.name for f in destination.schema]
    if any(set(r) != set(columns) for r in rows):
        raise ValueError("Payload differs from destination schema")
    stage = f'{PROJECT}.ci_creative._stage_performance_{uuid.uuid4().hex}'
    temporary = bigquery.Table(stage, schema=destination.schema)
    temporary.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    client.create_table(temporary)
    try:
        client.load_table_from_dataframe(pd.DataFrame(rows)[columns], stage,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY), location=LOCATION).result()
        duplicates = list(client.query(f"""SELECT T.campaign_id FROM `{TARGET}` T
            JOIN `{stage}` S USING(campaign_id, asset_id, performance_date, source_system)
            GROUP BY T.campaign_id, T.asset_id, T.performance_date, T.source_system
            HAVING COUNT(*) > 1 LIMIT 1""", location=LOCATION).result())
        if duplicates:
            raise ValueError('Duplicate destination keys')
        client.query(performance_merge_sql(stage, columns), location=LOCATION).result()
    finally:
        client.delete_table(stage, not_found_ok=True)


def validate_performance(client: bigquery.Client, rows: list[dict], sales_channel: str) -> dict:
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter('campaign_ids', 'STRING', sorted({r['campaign_id'] for r in rows})),
        bigquery.ScalarQueryParameter('sales_channel', 'STRING', sales_channel)])
    result = dict(next(iter(client.query((ROOT / 'sql/validate_campaign_performance.sql').read_text(),
        job_config=config, location=LOCATION).result())))
    expected = dict(row_count=1260, campaign_count=60, sku_count=20, duplicate_keys=0,
                    bad_coverage=0, bad_budget=0, missing_evidence=0, invalid_metrics=0)
    if result != expected:
        raise ValueError(f'Performance validation failed: {result}')
    stored = [dict(r) for r in client.query(f"""SELECT * FROM `{TARGET}`
        WHERE source_system='SYNTHETIC_POC' AND campaign_id IN UNNEST(@campaign_ids)
        ORDER BY campaign_id, performance_date""", job_config=config, location=LOCATION).result()]
    if stored != rows:
        raise ValueError('Stored values differ from deterministic generation')
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=ROOT / 'config/campaign_performance_generator.json')
    parser.add_argument('--sales-config', type=Path, default=ROOT / 'config/sales_generator.json')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    config = json.loads(args.config.read_text())
    sales_config = json.loads(args.sales_config.read_text())
    validate_config(config)
    start = date.fromisoformat(sales_config['start_date'])
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    parameters = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter('prefix', 'STRING', f'SYN-CMP-{start:%Y%m%d}-'),
        bigquery.ScalarQueryParameter('start', 'DATE', start),
        bigquery.ScalarQueryParameter('end', 'DATE', start + timedelta(days=364)),
        bigquery.ScalarQueryParameter('channel', 'STRING', sales_config['channel'])])
    campaigns = [dict(r) for r in client.query(f"""SELECT c.*, a.asset_id
        FROM `{PROJECT}.ci_creative.campaign_master` c
        JOIN `{PROJECT}.ci_creative.campaign_assets` a USING(campaign_id, sku)
        WHERE c.source_system='SYNTHETIC_POC' AND a.source_system='SYNTHETIC_POC'
          AND c.generator_version='campaign-v1' AND a.generator_version='template-v1'
          AND STARTS_WITH(c.campaign_id, @prefix) ORDER BY c.campaign_id""",
        job_config=parameters, location=LOCATION).result()]
    sales = [dict(r) for r in client.query(f"""SELECT sku, sales_date, units_sold, orders,
        net_sales, currency, source_system FROM `{PROJECT}.ci_sales.sku_daily_sales`
        WHERE source_system='SYNTHETIC_POC' AND channel=@channel
          AND sales_date BETWEEN @start AND @end ORDER BY sku, sales_date""",
        job_config=parameters, location=LOCATION).result()]
    if len(sales) != 7300 or {s['sku'] for s in sales} != EXPECTED_SKUS:
        raise ValueError('Expected 7300 existing SKU sales rows')
    if any(c['start_date'] < start or c['end_date'] > start + timedelta(days=364) for c in campaigns):
        raise ValueError('Campaigns fall outside sales period')
    rows = generate_performance(campaigns, sales, config)
    output = ROOT / 'data/generated/campaign_daily_performance.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, default=str, indent=2))
    LOG.info(json.dumps({'event': 'performance_generated', 'count': len(rows), 'dry_run': args.dry_run}))
    if not args.dry_run:
        load_performance(client, rows)
        LOG.info(json.dumps({'event': 'performance_validated', 'source_system': SOURCE,
                            **validate_performance(client, rows, sales_config['channel'])}))


if __name__ == '__main__':
    main()
