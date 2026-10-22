"""Deterministic synthetic sales; no historical business performance is implied.

Run: python -m src.generators.sales_generator [--config path]
Campaign uplift is regenerated from the baseline, never compounded on stored sales.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

PROJECT = "project-661d501a-e709-4276-a26"
LOCATION = "asia-south1"
TARGET = f"{PROJECT}.ci_sales.sku_daily_sales"
ROOT = Path(__file__).resolve().parents[2]
SOURCE = "SYNTHETIC_POC"
EXPECTED_SKUS = {f"CI-SKU-{i:04d}" for i in range(1, 21)}
LOG = logging.getLogger(__name__)


def money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def campaign_effects(campaigns: list[dict], config: dict) -> dict[tuple[str, date], dict]:
    """Build deterministic inclusive campaign periods; reject double counting."""
    settings = config['campaign_effects']
    bound = settings['variation_bound']
    edge = settings['edge_strength']
    if not 0 <= bound < 1 or not 0 < edge <= 1:
        raise ValueError('Invalid campaign variation or edge strength')
    start = date.fromisoformat(config['start_date'])
    end = start + timedelta(days=364)
    if len({c['campaign_id'] for c in campaigns}) != len(campaigns):
        raise ValueError('Duplicate campaign IDs')
    effects = {}
    for campaign in sorted(campaigns, key=lambda c: c['campaign_id']):
        if campaign['sku'] not in EXPECTED_SKUS or campaign['source_system'] != SOURCE:
            raise ValueError('Only canonical synthetic campaigns supported')
        first, last = campaign['start_date'], campaign['end_date']
        if first < start or last > end or last < first:
            raise ValueError('Campaign outside the sales period')
        uplift = settings['uplift_by_objective'][campaign['objective']]
        if not math.isfinite(uplift) or not 0 <= uplift <= 1:
            raise ValueError('Invalid synthetic uplift assumption')
        seed = int.from_bytes(hashlib.sha256(
            f"sales-uplift-v1:{config['seed']}:{campaign['campaign_id']}".encode()).digest()[:8], 'big')
        peak = uplift * random.Random(seed).uniform(1 - bound, 1 + bound)
        days = (last - first).days + 1
        for offset in range(days):
            key = (campaign['sku'], first + timedelta(days=offset))
            if key in effects:
                raise ValueError('Overlapping campaigns would double-count sales uplift')
            strength = edge + (1 - edge) * math.sin(math.pi * offset / (days - 1)) if days > 1 else 1
            effects[key] = dict(campaign_id=campaign['campaign_id'],
                                multiplier=1 + peak * strength)
    return effects


def generate_sales(products: list[dict], config: dict,
                   campaigns: list[dict] | None = None) -> list[dict]:
    """Return 365 dates per canonical SKU, independent of input ordering."""
    if len(products) != 20 or {p["sku"] for p in products} != EXPECTED_SKUS:
        raise ValueError("Expected exactly CI-SKU-0001 through CI-SKU-0020")
    start = date.fromisoformat(config["start_date"])
    effects = campaign_effects(campaigns, config) if campaigns is not None else {}
    weekdays = config["weekday_effects"]
    if len(weekdays) != 7 or any(x <= 0 for x in weekdays):
        raise ValueError("Seven positive weekday effects required")
    for name in ("noise_bound", "seasonality_amplitude", "trend_bound"):
        if not 0 <= config[name] < 1:
            raise ValueError(f"Invalid {name}")
    discount_rate = Decimal(config["discount_rate"])
    refund_rate = Decimal(config["refund_rate"])
    if not 0 <= discount_rate < 1 or not 0 <= refund_rate < 1:
        raise ValueError("Invalid financial rates")
    rows = []
    # Fixed metadata makes generated payloads reproducible too.
    timestamp = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
    for product in sorted(products, key=lambda p: p["sku"]):
        price = Decimal(str(product["price"]))
        if not price.is_finite() or price <= 0 or not product["currency"]:
            raise ValueError(f"Invalid price/currency for {product['sku']}")
        seed = int.from_bytes(hashlib.sha256(
            f"{config['seed']}:{product['sku']}".encode()).digest()[:8], "big")
        rng = random.Random(seed)
        baseline = config["baseline_by_type"][product["product_type"]]
        baseline *= max(0.4, min(1.8, (35 / float(price)) ** 0.45))
        baseline *= rng.uniform(0.85, 1.15)
        trend = rng.uniform(-config["trend_bound"], config["trend_bound"])
        phase = 172 if product["product_type"] == "Sunscreen" else 355
        for day in range(365):
            sales_date = start + timedelta(days=day)
            seasonal = 1 + config["seasonality_amplitude"] * math.cos(
                2 * math.pi * (sales_date.timetuple().tm_yday - phase) / 365.25)
            noise = rng.uniform(1 - config["noise_bound"], 1 + config["noise_bound"])
            units = max(0, round(baseline * weekdays[sales_date.weekday()]
                                 * seasonal * (1 + trend * day / 364) * noise
                                 * effects.get((product["sku"], sales_date), {}).get("multiplier", 1)))
            orders = min(units, max(1, round(units / 1.12))) if units else 0
            gross = money(price * units)
            discount = money(gross * discount_rate)
            refund = money((gross - discount) * refund_rate)
            net = gross - discount - refund
            rows.append(dict(sales_date=sales_date, sku=product["sku"],
                             channel=config["channel"], units_sold=units, orders=orders,
                             gross_sales=gross, discount_amount=discount,
                             refund_amount=refund, net_sales=net,
                             average_selling_price=money((gross - discount) / units)
                             if units else Decimal("0.00"),
                             currency=product["currency"].upper(), source_system=SOURCE,
                             created_at=timestamp, updated_at=timestamp))
    return rows


def merge_sql(stage: str, columns: list[str]) -> str:
    keys = {"sales_date", "sku", "channel", "source_system"}
    join = " AND ".join(f"T.{k} = S.{k}" for k in sorted(keys))
    updates = ", ".join(f"T.{c} = S.{c}" for c in columns
                        if c not in keys | {"created_at"})
    return f"""MERGE `{TARGET}` T USING `{stage}` S ON {join}
    WHEN MATCHED THEN UPDATE SET {updates}
    WHEN NOT MATCHED THEN INSERT ({', '.join(columns)})
    VALUES ({', '.join('S.' + c for c in columns)})"""


def load_sales(client: bigquery.Client, rows: list[dict]) -> None:
    destination = client.get_table(TARGET)
    columns = [f.name for f in destination.schema]
    if set(columns) != set(rows[0]):
        raise ValueError("Destination schema columns differ from generated payload")
    stage = f"{PROJECT}.ci_sales._stage_sku_daily_sales_{uuid.uuid4().hex}"
    table = bigquery.Table(stage, schema=destination.schema)
    table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    client.create_table(table)
    try:
        client.load_table_from_dataframe(
            pd.DataFrame(rows)[columns], stage,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY),
            location=LOCATION).result()
        # Reject ambiguous existing business keys before MERGE.
        duplicates = list(client.query(f"""SELECT T.sku FROM `{TARGET}` T
            JOIN `{stage}` S USING(sales_date, sku, channel, source_system)
            GROUP BY T.sales_date, T.sku, T.channel, T.source_system
            HAVING COUNT(*) > 1 LIMIT 1""", location=LOCATION).result())
        if duplicates:
            raise ValueError("Duplicate destination keys; refusing ambiguous merge")
        client.query(merge_sql(stage, columns), location=LOCATION).result()
    finally:
        client.delete_table(stage, not_found_ok=True)


def validate(client: bigquery.Client, config: dict, rows: list[dict]) -> dict:
    sql = (ROOT / "sql/validate_sku_daily_sales.sql").read_text()
    start = date.fromisoformat(config["start_date"])
    result = dict(next(iter(client.query(sql, location=LOCATION,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", start + timedelta(days=364)),
            bigquery.ScalarQueryParameter("channel", "STRING", config["channel"]),
        ])).result())))
    if result != dict(row_count=7300, sku_count=20, bad_skus=0, duplicate_keys=0,
                      invalid_financials=0, bad_coverage=0):
        raise ValueError(f"Validation failed: {result}")
    # Compare every stored value, beyond aggregate row counts.
    stored = list(client.query(f"""SELECT * FROM `{TARGET}`
        WHERE source_system='SYNTHETIC_POC' AND channel=@channel
        AND sales_date BETWEEN @start_date AND @end_date ORDER BY sku,sales_date""",
        location=LOCATION, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", start + timedelta(days=364)),
            bigquery.ScalarQueryParameter("channel", "STRING", config["channel"]),
        ])).result())
    if [dict(r) for r in stored] != rows:
        raise ValueError("Stored payload differs from deterministic generated rows")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/sales_generator.json")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    config = json.loads(args.config.read_text())
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    products = [dict(r) for r in client.query(f"""SELECT sku, product_type, price, currency
        FROM `{PROJECT}.ci_product.product_master` WHERE source_system='BEAUTYFEEDS'
        AND sku IN UNNEST(@skus) ORDER BY sku""", job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("skus", "STRING", sorted(EXPECTED_SKUS))]
        )).result()]
    campaigns = None
    if config.get('campaign_effects', {}).get('enabled', False):
        prefix = 'SYN-CMP-' + config['start_date'].replace('-', '') + '-'
        campaigns = [dict(r) for r in client.query(f"""SELECT campaign_id, sku, objective,
            start_date, end_date, source_system FROM `{PROJECT}.ci_creative.campaign_master`
            WHERE source_system='SYNTHETIC_POC' AND generator_version='campaign-v1'
              AND STARTS_WITH(campaign_id, @prefix) ORDER BY campaign_id""",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter('prefix', 'STRING', prefix)]), location=LOCATION).result()]
        if len(campaigns) != 60 or any(sum(c['sku'] == sku for c in campaigns) != 3
                                      for sku in EXPECTED_SKUS):
            raise ValueError('Expected 60 campaigns, three per canonical SKU')
    baseline = generate_sales(products, config)
    rows = generate_sales(products, config, campaigns)
    effects = campaign_effects(campaigns, config) if campaigns is not None else {}
    audit = []
    for before, after in zip(baseline, rows):
        effect = effects.get((after['sku'], after['sales_date']))
        if effect is None:
            if before != after:
                raise ValueError('Off-campaign sales changed')
        else:
            if after['units_sold'] < before['units_sold']:
                raise ValueError('Positive uplift reduced sales')
            audit.append(dict(sku=after['sku'], sales_date=after['sales_date'],
                campaign_id=effect['campaign_id'], multiplier=effect['multiplier'],
                baseline_units=before['units_sold'], modeled_units=after['units_sold'],
                incremental_units=after['units_sold']-before['units_sold'],
                source_system=SOURCE, model_version='sales-uplift-v1'))
    output = ROOT / 'data/generated/campaign_sales_effects.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(audit, default=str, indent=2))
    LOG.info(json.dumps({'event': 'sales_uplift_generated', 'campaign_days': len(audit),
        'off_campaign_days': len(rows)-len(audit),
        'incremental_units': sum(a['incremental_units'] for a in audit),
        'audit_path': str(output)}))
    load_sales(client, rows)
    LOG.info(json.dumps({"event": "sales_validated", "source_system": SOURCE,
                         "seed": config["seed"], **validate(client, config, rows)}))


if __name__ == "__main__":
    main()
