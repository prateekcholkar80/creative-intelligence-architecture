from pathlib import Path
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
FILE = ROOT / "data/curated/selected_20_products.csv"

df = pd.read_csv(FILE)

print("\nPRODUCT DATASET VALIDATION")
print("=" * 70)

print(f"Rows:                {len(df)}")
print(f"Unique SKUs:         {df['canonical_sku'].nunique()}")
print(f"Unique brands:       {df['brand'].nunique()}")
print(f"Missing ingredients: {df['ingredients'].isna().sum()}")
print(f"Missing image URLs:  {df['main_image_url'].isna().sum()}")

print("\nPRODUCT MIX")
print("-" * 70)
print(df["product_type"].value_counts().to_string())

print("\nCHECKING IMAGE URLS")
print("-" * 70)

working = 0
failed = []

headers = {
    "User-Agent": "Mozilla/5.0 CreativeIntelligencePOC/1.0"
}

for _, row in df.iterrows():
    sku = row["canonical_sku"]
    url = str(row["main_image_url"]).strip()

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=15,
            stream=True
        )

        content_type = response.headers.get("content-type", "")

        if response.ok and "image" in content_type:
            working += 1
            status = "OK"
        else:
            status = f"FAILED {response.status_code}"
            failed.append((sku, url))

    except Exception as exc:
        status = f"ERROR {exc}"
        failed.append((sku, url))

    print(f"{sku:15} {status}")

print("\nSUMMARY")
print("-" * 70)
print(f"Working images: {working}/{len(df)}")
print(f"Failed images:  {len(failed)}")

if failed:
    print("\nFailed URLs:")
    for sku, url in failed:
        print(f"{sku}: {url}")
