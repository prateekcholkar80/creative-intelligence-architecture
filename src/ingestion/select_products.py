from pathlib import Path
import re
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "data/raw/beautyfeeds_products.csv"
OUTPUT_FILE = PROJECT_ROOT / "data/curated/selected_20_products.csv"


TARGETS = {
    "Serum": 5,
    "Moisturizer": 4,
    "Cleanser": 3,
    "Sunscreen": 2,
    "Toner": 2,
    "Mask": 1,
    "Eye Care": 1,
    "Face Oil": 1,
    "Lip Care": 1,
}


def clean(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def find_column(df, candidates):
    lookup = {
        str(column).strip().lower(): column
        for column in df.columns
    }

    # Exact match first
    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]

    # Partial match fallback
    for candidate in candidates:
        for normalized, original in lookup.items():
            if candidate.lower() in normalized:
                return original

    return None


def classify_product(text):
    rules = [
        ("Serum", r"\bserum\b|booster|ampoule"),
        (
            "Moisturizer",
            r"moisturi[sz]|face cream|facial cream|day cream|night cream",
        ),
        (
            "Cleanser",
            r"cleanser|cleansing|face wash|facial wash",
        ),
        (
            "Sunscreen",
            r"sunscreen|sun screen|\bspf\b|sun protection",
        ),
        ("Toner", r"\btoner\b|tonic"),
        ("Mask", r"\bmask\b|masque"),
        (
            "Eye Care",
            r"eye cream|eye serum|under eye|eye treatment",
        ),
        (
            "Face Oil",
            r"face oil|facial oil",
        ),
        (
            "Lip Care",
            r"lip balm|lip treatment|lip mask",
        ),
        (
            "Exfoliator",
            r"exfoliat|peel|scrub",
        ),
    ]

    for product_type, pattern in rules:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return product_type

    return None


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Source dataset not found: {INPUT_FILE}"
        )

    df = pd.read_csv(INPUT_FILE)

    print(f"Source rows: {len(df):,}")
    print(f"Source columns: {len(df.columns)}")

    sku_col = find_column(
        df,
        ["N_sku", "sku", "product_id"]
    )

    product_col = find_column(
        df,
        ["E_product_name", "product_name", "name"]
    )

    brand_col = find_column(
        df,
        ["F_brand_name", "brand_name", "brand"]
    )

    category1_col = find_column(
        df,
        ["K_category_1", "category_1", "primary_category"]
    )

    category2_col = find_column(
        df,
        ["L_category_2", "category_2", "secondary_category"]
    )

    category3_col = find_column(
        df,
        ["M_category_3", "category_3", "tertiary_category"]
    )

    description_col = find_column(
        df,
        ["description", "product_description"]
    )

    ingredients_col = find_column(
        df,
        ["ingredients", "ingredient"]
    )

    price_col = find_column(
        df,
        ["price"]
    )

    currency_col = find_column(
        df,
        ["currency"]
    )

    usage_col = find_column(
        df,
        ["how to use", "how_to_use", "usage"]
    )

    image_col = find_column(
        df,
        ["main image", "main_image", "image_url", "image"]
    )

    additional_images_col = find_column(
        df,
        ["additional images", "additional_images"]
    )

    url_col = find_column(
        df,
        ["S_product_url", "product_url"]
    )

    required = {
        "product_name": product_col,
        "brand": brand_col,
        "ingredients": ingredients_col,
        "main_image": image_col,
    }

    missing = [
        name
        for name, column in required.items()
        if column is None
    ]

    if missing:
        raise RuntimeError(
            "Missing required columns: "
            + ", ".join(missing)
        )

    print("\nDetected important columns:")

    detected = {
        "sku": sku_col,
        "product_name": product_col,
        "brand": brand_col,
        "category_1": category1_col,
        "category_2": category2_col,
        "category_3": category3_col,
        "description": description_col,
        "ingredients": ingredients_col,
        "price": price_col,
        "currency": currency_col,
        "usage": usage_col,
        "main_image": image_col,
        "additional_images": additional_images_col,
        "product_url": url_col,
    }

    for key, value in detected.items():
        print(f"{key:20} -> {value}")

    category_columns = [
        column
        for column in [
            category1_col,
            category2_col,
            category3_col,
        ]
        if column is not None
    ]

    def searchable_text(row):
        values = [
            clean(row[product_col]),
            clean(row[brand_col]),
        ]

        values.extend(
            clean(row[column])
            for column in category_columns
        )

        return " ".join(values).lower()

    df["_search_text"] = df.apply(
        searchable_text,
        axis=1,
    )

    df["_product_type"] = df["_search_text"].apply(
        classify_product
    )

    # Minimum quality criteria for our multimodal RAG.
    filtered = df[
        df[product_col].notna()
        & df[brand_col].notna()
        & df[ingredients_col].notna()
        & df[image_col].notna()
        & df["_product_type"].notna()
    ].copy()

    filtered = filtered[
        filtered[ingredients_col]
        .astype(str)
        .str.len()
        .gt(10)
    ]

    filtered = filtered[
        filtered[image_col]
        .astype(str)
        .str.len()
        .gt(8)
    ]

    # Favor richer product records.
    if description_col:
        filtered["_description_length"] = (
            filtered[description_col]
            .fillna("")
            .astype(str)
            .str.len()
        )
    else:
        filtered["_description_length"] = 0

    selected_parts = []

    for product_type, target_count in TARGETS.items():
        subset = filtered[
            filtered["_product_type"] == product_type
        ].copy()

        subset = subset.sort_values(
            "_description_length",
            ascending=False,
        )

        selected_parts.append(
            subset.head(target_count)
        )

    selected = pd.concat(
        selected_parts,
        ignore_index=True,
    )

    selected = selected.drop_duplicates(
        subset=[brand_col, product_col]
    )

    # Fill shortages from remaining valid skincare products.
    if len(selected) < 20:
        selected_keys = set(
            zip(
                selected[brand_col].astype(str),
                selected[product_col].astype(str),
            )
        )

        remaining = filtered[
            ~filtered.apply(
                lambda row: (
                    str(row[brand_col]),
                    str(row[product_col]),
                )
                in selected_keys,
                axis=1,
            )
        ].copy()

        remaining = remaining.sort_values(
            "_description_length",
            ascending=False,
        )

        required_count = 20 - len(selected)

        selected = pd.concat(
            [
                selected,
                remaining.head(required_count),
            ],
            ignore_index=True,
        )

    selected = selected.head(20).copy()

    # This becomes our canonical enterprise key.
    selected["canonical_sku"] = [
        f"CI-SKU-{number:04d}"
        for number in range(1, len(selected) + 1)
    ]

    def values(column):
        if column is None:
            return [""] * len(selected)

        return selected[column].apply(clean)

    output = pd.DataFrame(
        {
            "canonical_sku": selected["canonical_sku"],
            "source_sku": values(sku_col),
            "brand": values(brand_col),
            "product_name": values(product_col),
            "product_type": selected["_product_type"],
            "category_1": values(category1_col),
            "category_2": values(category2_col),
            "category_3": values(category3_col),
            "description": values(description_col),
            "ingredients": values(ingredients_col),
            "price": values(price_col),
            "currency": values(currency_col),
            "usage_instructions": values(usage_col),
            "main_image_url": values(image_col),
            "additional_images": values(
                additional_images_col
            ),
            "source_product_url": values(url_col),
            "source_system": "BEAUTYFEEDS",
        }
    )

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output.to_csv(
        OUTPUT_FILE,
        index=False,
    )

    print("\n========================================")
    print("CURATED PRODUCT DATASET")
    print("========================================")

    print(f"Selected products: {len(output)}")

    print("\nProduct mix:")

    print(
        output["product_type"]
        .value_counts()
        .to_string()
    )

    print("\nProducts:\n")

    print(
        output[
            [
                "canonical_sku",
                "brand",
                "product_name",
                "product_type",
            ]
        ].to_string(index=False)
    )

    print(
        f"\nOutput saved to:\n{OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
