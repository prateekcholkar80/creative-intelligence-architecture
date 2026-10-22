"""Generate synthetic campaign scenarios for existing products, never actual history.

Run: python -m src.generators.campaign_generator
Dates derive from the sales configuration. Campaign assets and measured performance
are separate milestones. Product names are source text, not approved benefit claims.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT, SOURCE

TARGET = f"{PROJECT}.ci_creative.campaign_master"
LOG = logging.getLogger(__name__)
STRING_FIELDS = (
    "campaign_id", "sku", "campaign_name", "objective", "channel", "audience",
    "headline", "body_copy", "call_to_action", "product_image_uri", "currency",
    "campaign_status", "source_system", "source_reference", "generator_version",
)
SCHEMA = [bigquery.SchemaField(n, "STRING", mode="REQUIRED") for n in STRING_FIELDS] + [
    bigquery.SchemaField("start_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("end_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("budget", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]


def generate_campaigns(products: list[dict], config: dict, sales_start: date) -> list[dict]:
    if len(products) != 20 or {p["sku"] for p in products} != EXPECTED_SKUS:
        raise ValueError("Expected exactly the 20 canonical CI-SKU products")
    if len(config["waves"]) != 3:
        raise ValueError("Exactly three campaign waves required")
    duration, jitter = config["duration_days"], config["start_jitter_days"]
    if not isinstance(duration, int) or duration < 1 or not isinstance(jitter, int) or jitter < 0:
        raise ValueError("Invalid campaign duration or jitter")
    minimum, maximum = config["budget_min"], config["budget_max"]
    if not isinstance(minimum, int) or not isinstance(maximum, int) or not 0 < minimum <= maximum:
        raise ValueError("Invalid synthetic budget bounds")
    for wave in config["waves"]:
        if not isinstance(wave["offset_days"], int) or wave["offset_days"] < 0:
            raise ValueError("Invalid wave offset")
        if wave["offset_days"] + jitter + duration > 365:
            raise ValueError("Campaign could fall outside sales period")
        if any(not isinstance(wave[k], str) or not wave[k].strip()
               for k in ("theme", "objective", "channel", "audience")):
            raise ValueError("Campaign metadata must be nonempty")
    timestamp = datetime.combine(sales_start, datetime.min.time(), tzinfo=timezone.utc)
    rows = []
    for product in sorted(products, key=lambda p: p["sku"]):
        if product["source_system"] != "BEAUTYFEEDS":
            raise ValueError("Campaigns must reference real source products")
        if any(not product.get(k) for k in ("brand", "product_name", "primary_image_uri", "currency")):
            raise ValueError(f"Missing product evidence: {product['sku']}")
        expected_image = (f"gs://creative-brand-memory-{PROJECT}/products/"
                          f"{product['sku']}/primary.jpg")
        if product["primary_image_uri"] != expected_image:
            raise ValueError(f"Unexpected image reference: {product['sku']}")
        seed = int.from_bytes(hashlib.sha256(
            f"campaign-v1:{config['seed']}:{product['sku']}".encode()).digest()[:8], "big")
        rng = random.Random(seed)
        periods = []
        for index, wave in enumerate(config["waves"], 1):
            start = sales_start + timedelta(days=wave["offset_days"] + rng.randint(0, jitter))
            end = start + timedelta(days=duration - 1)
            if any(start <= old_end and end >= old_start for old_start, old_end in periods):
                raise ValueError(f"Overlapping campaigns for {product['sku']}")
            periods.append((start, end))
            rows.append(dict(
                campaign_id=f"SYN-CMP-{sales_start:%Y%m%d}-{product['sku']}-{index:02d}",
                sku=product["sku"],
                campaign_name=f"{wave['theme']} | {product['product_name']}",
                objective=wave["objective"], channel=wave["channel"], audience=wave["audience"],
                headline=product["product_name"],
                body_copy=f"Explore {product['product_name']}. View product details before choosing.",
                call_to_action="View product", product_image_uri=product["primary_image_uri"],
                currency=product["currency"].upper(), budget=Decimal(rng.randint(minimum, maximum)),
                start_date=start, end_date=end, campaign_status="SYNTHETIC_SCENARIO",
                source_system=SOURCE,
                source_reference=f"{PROJECT}.ci_product.product_master#sku={product['sku']}",
                generator_version="campaign-v1", created_at=timestamp, updated_at=timestamp,
            ))
    return rows


def campaign_merge_sql(stage: str, columns: list[str]) -> str:
    updates = ", ".join(f"T.{c}=S.{c}" for c in columns
                        if c not in {"campaign_id", "source_system", "created_at"})
    return f"""MERGE `{TARGET}` T USING `{stage}` S
        ON T.campaign_id=S.campaign_id AND T.source_system=S.source_system
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ({', '.join(columns)})
        VALUES ({', '.join('S.' + c for c in columns)})"""


def load_campaigns(client: bigquery.Client, rows: list[dict]) -> None:
    if not rows or len({r['campaign_id'] for r in rows}) != len(rows):
        raise ValueError("Campaign payload must be nonempty with unique IDs")
    table = bigquery.Table(TARGET, schema=SCHEMA)
    table.description = "Synthetic POC campaign scenarios; not actual historical business data."
    table.clustering_fields = ["sku", "source_system"]
    client.create_table(table, exists_ok=True)
    destination = client.get_table(TARGET)
    actual = {(f.name, f.field_type, f.mode) for f in destination.schema}
    expected = {(f.name, f.field_type, f.mode) for f in SCHEMA}
    if actual != expected:
        raise ValueError("Existing campaign schema differs; refusing to alter it")
    columns = [f.name for f in destination.schema]
    if any(set(r) != set(columns) for r in rows):
        raise ValueError("Payload differs from destination schema")
    stage = f"{PROJECT}.ci_creative._stage_campaign_master_{uuid.uuid4().hex}"
    temporary = bigquery.Table(stage, schema=destination.schema)
    temporary.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    client.create_table(temporary)
    try:
        client.load_table_from_dataframe(pd.DataFrame(rows)[columns], stage,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY),
            location=LOCATION).result()
        duplicates = list(client.query(f"""SELECT T.campaign_id FROM `{TARGET}` T
            JOIN `{stage}` S USING(campaign_id, source_system)
            GROUP BY T.campaign_id HAVING COUNT(*) > 1 LIMIT 1""", location=LOCATION).result())
        if duplicates:
            raise ValueError("Duplicate destination keys; refusing ambiguous merge")
        client.query(campaign_merge_sql(stage, columns), location=LOCATION).result()
    finally:
        client.delete_table(stage, not_found_ok=True)


def query_config(rows: list[dict], start: date) -> bigquery.QueryJobConfig:
    return bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("campaign_ids", "STRING", [r["campaign_id"] for r in rows]),
        bigquery.ScalarQueryParameter("sales_start", "DATE", start),
        bigquery.ScalarQueryParameter("sales_end", "DATE", start + timedelta(days=364)),
    ])


def validate_campaigns(client: bigquery.Client, rows: list[dict], start: date) -> dict:
    config = query_config(rows, start)
    result = dict(next(iter(client.query((ROOT / "sql/validate_campaigns.sql").read_text(),
        job_config=config, location=LOCATION).result())))
    expected = dict(campaign_count=60, sku_count=20, bad_counts=0, duplicate_ids=0,
                    invalid_periods=0, overlaps=0, missing_evidence=0, invalid_metadata=0)
    if result != expected:
        raise ValueError(f"Campaign validation failed: {result}")
    stored = [dict(r) for r in client.query(f"""SELECT * FROM `{TARGET}`
        WHERE campaign_id IN UNNEST(@campaign_ids) AND source_system='SYNTHETIC_POC'
        ORDER BY campaign_id""", job_config=config, location=LOCATION).result()]
    if stored != sorted(rows, key=lambda r: r['campaign_id']):
        raise ValueError("Stored campaign payload differs from deterministic generation")
    return result


def verify_sales_period(client: bigquery.Client, start: date, channel: str) -> None:
    counts = list(client.query(f"""SELECT sku, COUNT(*) n, COUNT(DISTINCT sales_date) days
        FROM `{PROJECT}.ci_sales.sku_daily_sales`
        WHERE source_system='SYNTHETIC_POC' AND channel=@channel
          AND sales_date BETWEEN @sales_start AND @sales_end
          AND sku IN UNNEST(@skus) GROUP BY sku""", location=LOCATION,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("channel", "STRING", channel),
            bigquery.ScalarQueryParameter("sales_start", "DATE", start),
            bigquery.ScalarQueryParameter("sales_end", "DATE", start + timedelta(days=364)),
            bigquery.ArrayQueryParameter("skus", "STRING", sorted(EXPECTED_SKUS)),
        ])).result())
    if len(counts) != 20 or any(r.n != 365 or r.days != 365 for r in counts):
        raise ValueError("Expected 365 existing sales dates for each canonical SKU")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/campaign_generator.json")
    parser.add_argument("--sales-config", type=Path, default=ROOT / "config/sales_generator.json")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = json.loads(args.config.read_text())
    sales_config = json.loads(args.sales_config.read_text())
    start = date.fromisoformat(sales_config["start_date"])
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    verify_sales_period(client, start, sales_config['channel'])
    products = [dict(r) for r in client.query(f"""SELECT sku, brand, product_name,
        primary_image_uri, currency, source_system FROM `{PROJECT}.ci_product.product_master`
        WHERE sku IN UNNEST(@skus) ORDER BY sku""", job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("skus", "STRING", sorted(EXPECTED_SKUS))]
        )).result()]
    rows = generate_campaigns(products, config, start)
    LOG.info(json.dumps({"event": "campaigns_generated", "count": len(rows), "seed": config['seed']}))
    load_campaigns(client, rows)
    LOG.info(json.dumps({"event": "campaigns_validated", "source_system": SOURCE,
                         **validate_campaigns(client, rows, start)}))


if __name__ == "__main__":
    main()
