"""Render template-based synthetic banners using original product images.

Preview: python -m src.generators.campaign_assets_generator --dry-run --limit 3
Upload:  python -m src.generators.campaign_assets_generator
No benefit claims or approved brand guidelines are inferred. Image embeddings
will be generated at the later embedding milestone, not by this renderer.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery, storage
from PIL import Image, ImageDraw, ImageFont, ImageOps

from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT, SOURCE

BUCKET = f"creative-brand-memory-{PROJECT}"
TARGET = f"{PROJECT}.ci_creative.campaign_assets"
VERSION = "template-v1"
LOG = logging.getLogger(__name__)
SCHEMA = [bigquery.SchemaField(n, "STRING", mode="REQUIRED") for n in (
    "asset_id", "campaign_id", "sku", "asset_type", "asset_role", "gcs_uri",
    "source_image_uri", "mime_type", "sha256", "headline", "body_copy",
    "call_to_action", "visual_description", "description_method", "generator_version",
    "source_system", "source_reference",
)] + [bigquery.SchemaField(n, "INTEGER", mode="REQUIRED") for n in ("width", "height")] + [
    bigquery.SchemaField(n, "TIMESTAMP", mode="REQUIRED") for n in ("created_at", "updated_at")]


def validate_config(config: dict) -> None:
    if config["width"] != 1200 or config["height"] != 628:
        raise ValueError("Template v1 requires a 1200 x 628 canvas")
    if not Path(config["font_path"]).is_file():
        raise ValueError("Set font_path to an available TrueType font in the asset config")


def wrap_text(text: str, draw: ImageDraw.ImageDraw, font: ImageFont.FreeTypeFont,
              width: int) -> list[str]:
    """Wrap without truncating source names, including individual long words."""
    lines, line = [], ""
    for word in text.split():
        if draw.textlength(word, font=font) > width:
            if line:
                lines.append(line)
                line = ""
            for char in word:
                if line and draw.textlength(line + char, font=font) > width:
                    lines.append(line)
                    line = ""
                line += char
            continue
        candidate = f"{line} {word}".strip()
        if line and draw.textlength(candidate, font=font) > width:
            lines.append(line)
            line = word
        else:
            line = candidate
    if line:
        lines.append(line)
    return lines


def fitted_text(draw: ImageDraw.ImageDraw, text: str, font_path: str, box: tuple,
                max_size: int, fill: str) -> None:
    x, y, width, height = box
    for size in range(max_size, 15, -1):
        font = ImageFont.truetype(font_path, size)
        lines = wrap_text(text, draw, font, width)
        spacing = size + 8
        if len(lines) * spacing <= height:
            for index, line in enumerate(lines):
                draw.text((x, y + index * spacing), line, font=font, fill=fill, anchor="lt")
            return
    raise ValueError("Source copy is too long for template; refusing to truncate")


def validate_campaign(campaign: dict) -> None:
    sku = campaign["sku"]
    if sku not in EXPECTED_SKUS or campaign["source_system"] != SOURCE:
        raise ValueError("Only canonical synthetic campaigns may be rendered")
    if not re.fullmatch(r"SYN-CMP-\d{8}-CI-SKU-\d{4}-0[1-3]", campaign["campaign_id"]):
        raise ValueError("Invalid campaign ID")
    expected = f"gs://{BUCKET}/products/{sku}/primary.jpg"
    if campaign["product_image_uri"] != expected or campaign["headline"] != campaign["product_name"]:
        raise ValueError("Campaign image or headline differs from product evidence")
    body = f"Explore {campaign['product_name']}. View product details before choosing."
    if campaign["body_copy"] != body or campaign["call_to_action"] != "View product":
        raise ValueError("Only neutral copy is supported until approved claims are available")


def render_banner(campaign: dict, original: bytes, config: dict) -> bytes:
    validate_config(config)
    validate_campaign(campaign)
    width, height = config["width"], config["height"]
    canvas = Image.new("RGB", (width, height), config["background"])
    draw = ImageDraw.Draw(canvas)
    accent = config["accent_by_objective"][campaign["objective"]]
    draw.rectangle((0, 0, 14, height), fill=accent)
    font_path = config["font_path"]
    fitted_text(draw, "SYNTHETIC POC • PRODUCT SPOTLIGHT", font_path,
                (55, 42, 600, 40), 18, accent)
    fitted_text(draw, campaign["headline"], font_path,
                (55, 120, 605, 230), 42, config["text_color"])
    fitted_text(draw, campaign["body_copy"], font_path,
                (55, 365, 605, 115), 23, config["text_color"])
    draw.rounded_rectangle((55, 515, 270, 570), radius=10, fill=accent)
    fitted_text(draw, campaign["call_to_action"], font_path,
                (77, 531, 173, 30), 22, "#FFFFFF")
    # Contain the complete source image, preserving its aspect ratio and contents.
    with Image.open(io.BytesIO(original)) as image:
        image = ImageOps.exif_transpose(image).convert("RGBA")
        image = ImageOps.contain(image, (440, 480), method=Image.Resampling.LANCZOS)
        background = Image.new("RGBA", image.size, "white")
        background.alpha_composite(image)
        draw.rounded_rectangle((695, 70, 1160, 558), radius=16, fill="white")
        canvas.paste(background.convert("RGB"), (927 - image.width // 2, 314 - image.height // 2))
    output = io.BytesIO()
    canvas.save(output, format="PNG", optimize=False)
    return output.getvalue()


def asset_row(campaign: dict, png: bytes, config: dict) -> dict:
    digest = hashlib.sha256(png).hexdigest()
    asset_id = f"SYN-AST-{campaign['campaign_id']}-{VERSION}"
    name = f"campaigns/{campaign['campaign_id']}/{VERSION}/{digest}.png"
    timestamp = campaign["created_at"]
    return dict(asset_id=asset_id, campaign_id=campaign['campaign_id'], sku=campaign['sku'],
        asset_type="IMAGE", asset_role="CAMPAIGN_BANNER", gcs_uri=f"gs://{BUCKET}/{name}",
        source_image_uri=campaign['product_image_uri'], mime_type="image/png", sha256=digest,
        width=config['width'], height=config['height'], headline=campaign['headline'],
        body_copy=campaign['body_copy'], call_to_action=campaign['call_to_action'],
        visual_description=(f"Synthetic template banner for {campaign['sku']}: neutral copy on "
            "the left, complete source product image on a white panel on the right, "
            "a View product button, and a visible SYNTHETIC POC label."),
        description_method="TEMPLATE_METADATA", generator_version=VERSION, source_system=SOURCE,
        source_reference=f"{PROJECT}.ci_creative.campaign_master#campaign_id={campaign['campaign_id']}",
        created_at=timestamp, updated_at=timestamp)


def upload_asset(bucket: storage.Bucket, row: dict, png: bytes) -> None:
    object_name = row['gcs_uri'].removeprefix(f"gs://{BUCKET}/")
    blob = bucket.blob(object_name)
    blob.metadata = {"source_system": SOURCE, "sha256": row['sha256'],
                     "campaign_id": row['campaign_id'], "sku": row['sku']}
    try:
        blob.upload_from_string(png, content_type="image/png", if_generation_match=0)
    except PreconditionFailed:
        existing = blob.download_as_bytes()
        if hashlib.sha256(existing).hexdigest() != row['sha256']:
            raise ValueError("Existing GCS object differs; refusing to overwrite")


def asset_merge_sql(stage: str, columns: list[str]) -> str:
    updates = ", ".join(f"T.{c}=S.{c}" for c in columns
                        if c not in {"asset_id", "source_system", "created_at"})
    return f"""MERGE `{TARGET}` T USING `{stage}` S
        ON T.asset_id=S.asset_id AND T.source_system=S.source_system
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ({', '.join(columns)})
        VALUES ({', '.join('S.' + c for c in columns)})"""


def load_metadata(client: bigquery.Client, rows: list[dict]) -> None:
    if not rows or len({r['asset_id'] for r in rows}) != len(rows):
        raise ValueError("Unique nonempty asset payload required")
    table = bigquery.Table(TARGET, schema=SCHEMA)
    table.description = "Synthetic template campaign banners; original product image references retained."
    table.clustering_fields = ["sku", "campaign_id"]
    client.create_table(table, exists_ok=True)
    destination = client.get_table(TARGET)
    if {(f.name, f.field_type, f.mode) for f in destination.schema} != {
            (f.name, f.field_type, f.mode) for f in SCHEMA}:
        raise ValueError("Existing asset schema differs; refusing to alter it")
    columns = [f.name for f in destination.schema]
    if any(set(r) != set(columns) for r in rows):
        raise ValueError("Asset payload differs from destination schema")
    stage = f"{PROJECT}.ci_creative._stage_campaign_assets_{uuid.uuid4().hex}"
    temporary = bigquery.Table(stage, schema=destination.schema)
    temporary.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    client.create_table(temporary)
    try:
        client.load_table_from_dataframe(pd.DataFrame(rows)[columns], stage,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY), location=LOCATION).result()
        duplicates = list(client.query(f"""SELECT T.asset_id FROM `{TARGET}` T
            JOIN `{stage}` S USING(asset_id, source_system)
            GROUP BY T.asset_id HAVING COUNT(*) > 1 LIMIT 1""", location=LOCATION).result())
        if duplicates:
            raise ValueError("Duplicate destination asset keys")
        client.query(asset_merge_sql(stage, columns), location=LOCATION).result()
    finally:
        client.delete_table(stage, not_found_ok=True)
    params = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("asset_ids", "STRING", [r['asset_id'] for r in rows])])
    stored = [dict(r) for r in client.query(f"""SELECT * FROM `{TARGET}`
        WHERE source_system='SYNTHETIC_POC' AND asset_id IN UNNEST(@asset_ids)
        ORDER BY asset_id""", location=LOCATION, job_config=params).result()]
    if stored != sorted(rows, key=lambda r: r['asset_id']):
        raise ValueError("Stored asset metadata differs from generated payload")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/campaign_assets_generator.json")
    parser.add_argument("--sales-config", type=Path, default=ROOT / "config/sales_generator.json")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data/generated/campaign_assets")
    parser.add_argument("--dry-run", action="store_true", help="Only read cloud data and write local previews")
    parser.add_argument("--limit", type=int, help="Preview a subset; requires --dry-run")
    args = parser.parse_args()
    if args.limit is not None and (not args.dry_run or args.limit < 1):
        parser.error("--limit requires --dry-run and a positive value")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = json.loads(args.config.read_text())
    validate_config(config)
    sales_config = json.loads(args.sales_config.read_text())
    prefix = "SYN-CMP-" + sales_config['start_date'].replace('-', '') + "-"
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    gcs = storage.Client(project=PROJECT)
    campaigns = [dict(r) for r in client.query(f"""SELECT c.*, p.product_name
        FROM `{PROJECT}.ci_creative.campaign_master` c
        JOIN `{PROJECT}.ci_product.product_master` p USING(sku)
        WHERE c.source_system='SYNTHETIC_POC' AND c.generator_version='campaign-v1'
          AND STARTS_WITH(c.campaign_id, @prefix) AND p.source_system='BEAUTYFEEDS'
          AND c.product_image_uri=p.primary_image_uri
        ORDER BY c.campaign_id""", job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("prefix", "STRING", prefix)]), location=LOCATION).result()]
    if len(campaigns) != 60 or len({r['campaign_id'] for r in campaigns}) != 60:
        raise ValueError("Expected 60 existing campaigns for the configured sales period")
    if {r['sku'] for r in campaigns} != EXPECTED_SKUS or any(
            sum(r['sku'] == sku for r in campaigns) != 3 for sku in EXPECTED_SKUS):
        raise ValueError("Expected three campaigns per canonical SKU")
    for campaign in campaigns:
        validate_campaign(campaign)
    if args.limit:
        campaigns = campaigns[:args.limit]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    bucket = gcs.bucket(BUCKET)
    images, rows = {}, []
    for campaign in campaigns:
        uri = campaign['product_image_uri']
        if uri not in images:
            images[uri] = bucket.blob(uri.removeprefix(f"gs://{BUCKET}/")).download_as_bytes()
        png = render_banner(campaign, images[uri], config)
        row = asset_row(campaign, png, config)
        (args.output_dir / f"{row['asset_id']}.png").write_bytes(png)
        if not args.dry_run:
            upload_asset(bucket, row, png)
        rows.append(row)
        LOG.info(json.dumps({"event": "asset_rendered", "campaign_id": campaign['campaign_id']}))
    (args.output_dir / "manifest.json").write_text(json.dumps(rows, default=str, indent=2))
    if not args.dry_run:
        load_metadata(client, rows)
    LOG.info(json.dumps({"event": "assets_complete", "count": len(rows),
        "dry_run": args.dry_run, "source_system": SOURCE, "output_dir": str(args.output_dir)}))


if __name__ == '__main__':
    main()
