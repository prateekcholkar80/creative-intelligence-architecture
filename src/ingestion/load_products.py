from __future__ import annotations

import hashlib
import io
import mimetypes
import re
from pathlib import Path

import pandas as pd
import requests
from PIL import Image
from google.cloud import bigquery, storage
from decimal import Decimal, InvalidOperation

PROJECT_ID = "project-661d501a-e709-4276-a26"
DATASET = "ci_product"
BUCKET_NAME = "creative-brand-memory-project-661d501a-e709-4276-a26"

ROOT = Path(__file__).resolve().parents[2]
INPUT_FILE = ROOT / "data/curated/selected_20_products.csv"

bq = bigquery.Client(project=PROJECT_ID)
gcs = storage.Client(project=PROJECT_ID)
bucket = gcs.bucket(BUCKET_NAME)


def clean(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    return value or None


def parse_price(value):
    if pd.isna(value):
        return None

    match = re.search(r"[\d,.]+", str(value))

    if not match:
        return None

    try:
        cleaned = match.group(0).replace(",", "")
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def normalize_ingredient(name):
    name = re.sub(r"\s+", " ", name).strip(" .;,")
    return name


def ingredient_id(name):
    key = name.lower().encode("utf-8")
    digest = hashlib.sha1(key).hexdigest()[:12].upper()
    return f"ING-{digest}"


def split_ingredients(raw):
    if not raw:
        return []

    text = str(raw).replace("\n", " ")

    # POC parser. Keeps original text separately for provenance.
    parts = [
        normalize_ingredient(x)
        for x in text.split(",")
    ]

    return [x for x in parts if len(x) > 1]


def download_and_upload_image(sku, url):
    headers = {
        "User-Agent": "Mozilla/5.0 CreativeIntelligencePOC/1.0"
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()

    content_type = response.headers.get(
        "content-type",
        "image/jpeg",
    ).split(";")[0]

    extension = mimetypes.guess_extension(content_type) or ".jpg"

    if extension == ".jpe":
        extension = ".jpg"

    object_name = f"products/{sku}/primary{extension}"

    blob = bucket.blob(object_name)
    blob.upload_from_string(
        response.content,
        content_type=content_type,
    )

    width = None
    height = None

    try:
        image = Image.open(io.BytesIO(response.content))
        width, height = image.size
    except Exception:
        pass

    return {
        "gcs_uri": f"gs://{BUCKET_NAME}/{object_name}",
        "file_name": f"primary{extension}",
        "mime_type": content_type,
        "width": width,
        "height": height,
    }

def coerce_dataframe_to_bq_schema(df, schema):
    """
    Coerce pandas values to Python types expected by the
    corresponding BigQuery schema before PyArrow serialization.
    """

    df = df.copy()

    for field in schema:
        name = field.name

        if name not in df.columns:
            continue

        # Repeated fields such as ARRAY<STRING>
        if field.mode == "REPEATED":
            df[name] = df[name].apply(
                lambda value: (
                    value
                    if isinstance(value, list)
                    else []
                )
            )
            continue

        field_type = field.field_type.upper()

        if field_type in ("NUMERIC", "BIGNUMERIC"):
            def to_decimal(value):
                if value is None or pd.isna(value):
                    return None

                if isinstance(value, Decimal):
                    return value

                try:
                    return Decimal(str(value))
                except (InvalidOperation, ValueError):
                    return None

            df[name] = df[name].apply(to_decimal)

        elif field_type == "STRING":
            df[name] = df[name].apply(
                lambda value: (
                    None
                    if value is None or pd.isna(value)
                    else str(value)
                )
            )

        elif field_type == "INTEGER":
            df[name] = pd.to_numeric(
                df[name],
                errors="coerce",
            ).astype("Int64")

        elif field_type == "FLOAT":
            df[name] = pd.to_numeric(
                df[name],
                errors="coerce",
            )

        elif field_type == "BOOLEAN":
            df[name] = df[name].astype("boolean")

        elif field_type == "DATE":
            df[name] = df[name].apply(
                lambda value: (
                    None
                    if value is None or pd.isna(value)
                    else pd.to_datetime(value).date()
                )
            )

        elif field_type in ("TIMESTAMP", "DATETIME"):
            df[name] = pd.to_datetime(
                df[name],
                errors="coerce",
                utc=(field_type == "TIMESTAMP"),
            )

    return df

def load_stage(df, table_name):
    """
    Load a DataFrame into a temporary staging table using the
    exact schema of the target BigQuery table.

    This prevents pandas/pyarrow from incorrectly inferring
    all-null columns such as STRING, DATE, NUMERIC and ARRAY fields.
    """

    target = f"{PROJECT_ID}.{DATASET}.{table_name}"
    stage = f"{PROJECT_ID}.{DATASET}._stage_{table_name}"

    # Get authoritative schema from the real destination table
    target_table = bq.get_table(target)
    df = coerce_dataframe_to_bq_schema(df,target_table.schema,)

    # Remove any incorrectly inferred stage table from a previous run
    bq.delete_table(stage, not_found_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=target_table.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = bq.load_table_from_dataframe(
        df,
        stage,
        job_config=job_config,
    )

    job.result()

    print(
        f"  Staged {len(df)} rows → {stage}"
    )

    return stage


def merge_table(stage, target, key_columns, columns):
    target_ref = f"{PROJECT_ID}.{DATASET}.{target}"

    join_condition = " AND ".join(
        f"T.{key} = S.{key}"
        for key in key_columns
    )

    update_columns = [
    col for col in columns
    if col not in key_columns
    and col != "created_at"
    ]

    update_clause = ",\n".join(
        f"T.{col} = S.{col}"
        for col in update_columns
    )

    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"S.{col}" for col in columns)

    sql = f"""
    MERGE `{target_ref}` T
    USING `{stage}` S
    ON {join_condition}

    WHEN MATCHED THEN
      UPDATE SET
      {update_clause}

    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """

    bq.query(sql).result()

    bq.delete_table(stage, not_found_ok=True)


def main():
    df = pd.read_csv(INPUT_FILE)

    now = pd.Timestamp.now(tz="UTC")

    product_rows = []
    ingredient_rows = {}
    product_ingredient_rows = []
    asset_rows = []

    image_failures = []

    for _, row in df.iterrows():
        sku = row["canonical_sku"]

        print(f"\nProcessing {sku}: {row['product_name']}")

        image_uri = None

        image_url = clean(row.get("main_image_url"))

        if image_url:
            try:
                img = download_and_upload_image(
                    sku,
                    image_url,
                )

                image_uri = img["gcs_uri"]

                asset_rows.append({
                    "asset_id": f"AST-{sku}-PRIMARY",
                    "sku": sku,
                    "asset_type": "IMAGE",
                    "asset_role": "PRIMARY_PRODUCT_IMAGE",
                    "gcs_uri": img["gcs_uri"],
                    "file_name": img["file_name"],
                    "mime_type": img["mime_type"],
                    "width": img["width"],
                    "height": img["height"],
                    "alt_text": (
                        f"{row['brand']} {row['product_name']}"
                    ),
                    "asset_description": (
                        "Primary product image imported "
                        "from source dataset"
                    ),
                    "is_primary": True,
                    "display_order": 1,
                    "source_system": "BEAUTYFEEDS",
                    "source_url": image_url,
                    "created_at": now,
                    "updated_at": now,
                })

                print(f"  Image uploaded: {image_uri}")

            except Exception as exc:
                image_failures.append(
                    (sku, image_url, str(exc))
                )
                print(f"  IMAGE FAILED: {exc}")

        product_rows.append({
            "sku": sku,
            "brand": clean(row.get("brand")),
            "product_name": clean(row.get("product_name")),
            "category": clean(row.get("category_1")),
            "subcategory": (
                clean(row.get("category_2"))
                or clean(row.get("category_3"))
            ),
            "product_type": clean(row.get("product_type")),
            "description": clean(row.get("description")),
            "size_value": None,
            "size_unit": None,
            "price": parse_price(row.get("price")),
            "currency": clean(row.get("currency")) or "USD",
            "skin_types": [],
            "skin_concerns": [],
            "benefits": [],
            "usage_instructions": clean(
                row.get("usage_instructions")
            ),
            "country_of_origin": None,
            "launch_date": None,
            "product_status": "ACTIVE",
            "primary_image_uri": image_uri,
            "source_system": "BEAUTYFEEDS",
            "source_product_id": clean(
                row.get("source_sku")
            ),
            "created_at": now,
            "updated_at": now,
        })

        raw_ingredients = clean(row.get("ingredients"))

        parsed = split_ingredients(raw_ingredients)

        for sequence, ingredient in enumerate(
            parsed,
            start=1,
        ):
            ing_id = ingredient_id(ingredient)

            ingredient_rows[ing_id] = {
                "ingredient_id": ing_id,
                "inci_name": ingredient,
                "common_name": ingredient,
                "ingredient_category": None,
                "primary_function": None,
                "description": None,
                "typical_benefits": [],
                "related_skin_concerns": [],
                "source_system": "BEAUTYFEEDS",
                "source_reference": clean(
                    row.get("source_product_url")
                ),
                "created_at": now,
                "updated_at": now,
            }

            product_ingredient_rows.append({
                "sku": sku,
                "ingredient_id": ing_id,
                "ingredient_sequence": sequence,
                "concentration_percent": None,
                "concentration_text": None,
                "is_active_ingredient": False,
                "is_hero_ingredient": False,
                "ingredient_role": None,
                "source_ingredient_text": raw_ingredients,
                "created_at": now,
                "updated_at": now,
            })

    product_df = pd.DataFrame(product_rows)
    ingredient_df = pd.DataFrame(
        ingredient_rows.values()
    )
    product_ingredient_df = pd.DataFrame(
        product_ingredient_rows
    )
    asset_df = pd.DataFrame(asset_rows)

    print("\nLoading BigQuery staging tables...")

    stage = load_stage(
        product_df,
        "product_master",
    )

    merge_table(
        stage,
        "product_master",
        ["sku"],
        list(product_df.columns),
    )

    stage = load_stage(
        ingredient_df,
        "ingredients",
    )

    merge_table(
        stage,
        "ingredients",
        ["ingredient_id"],
        list(ingredient_df.columns),
    )

    stage = load_stage(
        product_ingredient_df,
        "product_ingredients",
    )

    merge_table(
        stage,
        "product_ingredients",
        ["sku", "ingredient_id"],
        list(product_ingredient_df.columns),
    )

    if not asset_df.empty:
        stage = load_stage(
            asset_df,
            "product_assets",
        )

        merge_table(
            stage,
            "product_assets",
            ["asset_id"],
            list(asset_df.columns),
        )

    print("\n======================================")
    print("LOAD COMPLETE")
    print("======================================")
    print(f"Products:             {len(product_df)}")
    print(f"Ingredients:          {len(ingredient_df)}")
    print(
        "Product ingredients: "
        f"{len(product_ingredient_df)}"
    )
    print(f"Product assets:       {len(asset_df)}")
    print(f"Image failures:       {len(image_failures)}")

    if image_failures:
        print("\nIMAGE FAILURES")
        for sku, url, error in image_failures:
            print(f"\n{sku}")
            print(url)
            print(error)


if __name__ == "__main__":
    main()
