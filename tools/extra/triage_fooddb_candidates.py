import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd


DEFAULT_INPUT = Path("data/staging/fooddb_rebuild/foods_enriched_snapshot.parquet")
DEFAULT_TRIAGED = Path("data/staging/fooddb_rebuild/foods_triaged.csv")
DEFAULT_CANDIDATES = Path("data/staging/fooddb_rebuild/foods_fooddb_candidates.csv")
DEFAULT_REJECTED = Path("data/staging/fooddb_rebuild/foods_recipe_like_or_rejected.csv")

NAME_COLUMN_CANDIDATES = [
    "name_core",
    "name",
    "food_name",
    "item_name",
    "label",
    "description",
    "title",
]

RECIPE_LIKE_MAIN_GROUPS = {
    "starters and dishes",
}

RECIPE_LIKE_SUB_GROUPS = {
    "mixed salads",
    "soups",
    "sandwiches",
    "dishes",
    "pizzas crepe and pies",
    "savoury pastries and other starters",
    "baby dishes",
    "frozen desserts",
}

RECIPE_LIKE_NAME_KEYWORDS = [
    "salad",
    "soup",
    "pizza",
    "lasagna",
    "cannelloni",
    "sandwich",
    "canape",
    "toast",
    "omelette",
    "cordon bleu",
    "quenelle",
    "in sauce",
    "quiche",
    "tart",
    "vol au vent",
    "profiteroles",
    "peach melba",
    "baked alaska",
    "soft drink",
    "ready-to-drink",
    "ice cream",
    "sorbet",
]

MIX_OR_AGGREGATE_KEYWORDS = [
    "average",
    "all types",
    "mix of species",
    "mix",
    "mixed",
    "assortment",
    "selection",
]

RECIPE_LIKE_KEYWORD_EXCEPTIONS = {
    "sandwich": [
        "sandwich loaf",
        "sandwich bread",
    ],
    "pizza": [
        "pizza base",
        "pizza shell",
        "pizza sauce",
        "sauce for pizza",
    ],
    "toast": [
        "swedish toast",
    ],
    "ice cream": [
        "wafer for ice cream",
    ],
}

PROCESSING_KEYWORDS = [
    "canned",
    "prepacked",
    "prepared",
    "cooked",
    "boiled",
    "flavoured",
    "flavored",
    "sweetened",
    "reduced sugar",
    "reduced fat",
    "smoked",
    "marinated",
    "dried",
    "dehydrated",
    "reconstituted",
    "powder",
    "powdered",
    "fried",
    "roasted",
    "grilled",
    "pasteurised",
    "pasteurized",
    "uht",
    "condensed",
    "fermented",
]

ATOMIC_MAIN_GROUPS = {
    "fruits vegetables legumes and nuts",
    "meat egg and fish",
}

ATOMIC_SUB_GROUPS = {
    "vegetables",
    "fruits",
    "raw meat",
    "fish raw",
    "nuts and seeds",
    "potatoes and other tubers",
    "legumes",
    "water",
}

CANONICAL_STOP_CHUNKS = {
    "canned",
    "prepacked",
    "prepared",
    "home made",
    "homemade",
    "cooked",
    "raw",
    "unsalted",
    "salted",
    "refrigerated",
    "frozen",
    "dehydrated",
    "reconstituted",
    "drained",
    "plain",
    "flavoured",
    "flavored",
    "sweetened",
    "uht",
    "pasteurised",
    "pasteurized",
    "powder",
    "powdered",
    "boiled",
    "grilled",
    "roasted",
    "fried",
    "smoked",
    "to be reheated",
    "reheated",
    "reduced sugar",
    "reduced fat",
}

CANONICAL_PREFIX_PATTERNS = [
    re.compile(r"^prepared\s+"),
    re.compile(r"^pre cooked\s+"),
    re.compile(r"^precooked\s+"),
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Triere simpla a datasetului de alimente pentru candidati Food_DB."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--out-triaged", default=str(DEFAULT_TRIAGED))
    parser.add_argument("--out-candidates", default=str(DEFAULT_CANDIDATES))
    parser.add_argument("--out-rejected", default=str(DEFAULT_REJECTED))
    return parser.parse_args()


def strip_accents(text):
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_text(value):
    if pd.isna(value):
        return ""

    text = str(value).strip().lower()
    text = strip_accents(text)
    text = text.replace("&", " and ")
    text = re.sub(r"[()\[\]{}]", " ", text)
    text = re.sub(r"\s*/\s*", " / ", text)
    text = re.sub(r"[^a-z0-9,/' +.-]+", " ", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ,;-")


def flatten_for_match(text):
    flat = text.replace(",", " ")
    flat = flat.replace("/", " ")
    flat = re.sub(r"\s+", " ", flat)
    return f" {flat.strip()} "


def contains_keyword(text, keywords):
    haystack = flatten_for_match(text)
    for keyword in keywords:
        needle = f" {normalize_text(keyword)} "
        if needle in haystack:
            return keyword
    return None


def find_recipe_like_keyword(name_clean, sub_group, sub_sub_group):
    # Pastram cateva exceptii sigure pentru keyword-uri prea late.
    for keyword in RECIPE_LIKE_NAME_KEYWORDS:
        if not contains_keyword(name_clean, [keyword]):
            continue

        exception_keywords = RECIPE_LIKE_KEYWORD_EXCEPTIONS.get(keyword, [])
        if contains_keyword(name_clean, exception_keywords):
            continue

        if keyword == "toast" and sub_sub_group == "rusks":
            continue

        return keyword

    return None


def is_truthy(value):
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    try:
        return float(value) != 0.0
    except Exception:
        return bool(value)


def detect_name_column(columns):
    for column in NAME_COLUMN_CANDIDATES:
        if column in columns:
            return column
    raise ValueError(
        f"Nu am gasit o coloana de nume. Verificate: {', '.join(NAME_COLUMN_CANDIDATES)}"
    )


def is_generic_canonical_chunk(chunk):
    if chunk in CANONICAL_STOP_CHUNKS:
        return True

    for stop_chunk in CANONICAL_STOP_CHUNKS:
        if chunk.startswith(f"{stop_chunk} "):
            return True

    return False


def build_canonical_name_guess(name_clean):
    if not name_clean:
        return ""

    chunks = [chunk.strip() for chunk in name_clean.split(",") if chunk.strip()]
    kept_chunks = []

    for chunk in chunks:
        if is_generic_canonical_chunk(chunk):
            break
        kept_chunks.append(chunk)

    canonical = ", ".join(kept_chunks) if kept_chunks else name_clean

    for pattern in CANONICAL_PREFIX_PATTERNS:
        canonical = pattern.sub("", canonical)

    canonical = re.sub(r"\s+", " ", canonical)
    return canonical.strip(" ,")


def classify_row(row, name_clean):
    main_group = normalize_text(row.get("main_group"))
    sub_group = normalize_text(row.get("sub_group"))
    sub_sub_group = normalize_text(row.get("sub_sub_group"))
    name_tags = normalize_text(row.get("name_tags"))

    if not name_clean:
        return "unclear", False, True, "missing_name"

    if is_truthy(row.get("tag_composite_dish_hint")):
        return "recipe_like", False, False, "tag_composite_dish_hint"

    if is_truthy(row.get("tag_salad_like")):
        return "recipe_like", False, False, "tag_salad_like"

    if main_group in RECIPE_LIKE_MAIN_GROUPS:
        return "recipe_like", False, False, f"main_group:{main_group}"

    if sub_group in RECIPE_LIKE_SUB_GROUPS:
        return "recipe_like", False, False, f"sub_group:{sub_group}"

    if sub_sub_group in RECIPE_LIKE_SUB_GROUPS:
        return "recipe_like", False, False, f"sub_sub_group:{sub_sub_group}"

    recipe_keyword = find_recipe_like_keyword(name_clean, sub_group, sub_sub_group)
    if recipe_keyword:
        return "recipe_like", False, False, f"name_keyword:{recipe_keyword}"

    if is_truthy(row.get("exclude")):
        return "unclear", False, True, "source_exclude_flag"

    if is_truthy(row.get("is_baby_food")) or main_group == "baby food":
        return "unclear", False, True, "baby_food_scope"

    mix_or_aggregate_keyword = contains_keyword(name_clean, MIX_OR_AGGREGATE_KEYWORDS)

    # Agregatele evidente merg direct la drop, nu la review.
    if "note average" in flatten_for_match(name_tags) or mix_or_aggregate_keyword == "average":
        return "unclear", False, False, "aggregate_average_item"

    if mix_or_aggregate_keyword == "all types":
        return "unclear", False, False, "aggregate_all_types_item"

    if mix_or_aggregate_keyword in {"mix of species", "mix", "mixed", "assortment", "selection"}:
        return "unclear", False, False, "mixed_multi_item"

    atomic_group_hit = (
        main_group in ATOMIC_MAIN_GROUPS
        or sub_group in ATOMIC_SUB_GROUPS
        or " raw " in flatten_for_match(sub_sub_group)
        or " raw " in flatten_for_match(name_clean)
    )
    processing_hit = contains_keyword(name_clean, PROCESSING_KEYWORDS)

    if atomic_group_hit and not processing_hit:
        return "atomic", True, False, ""

    return "semi_atomic", True, False, ""


def print_columns(columns):
    print(f"Available columns ({len(columns)}):")
    for column in columns:
        print(f" - {column}")


def ensure_parent(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def main():
    args = parse_args()

    input_path = Path(args.input)
    out_triaged = Path(args.out_triaged)
    out_candidates = Path(args.out_candidates)
    out_rejected = Path(args.out_rejected)

    df = pd.read_parquet(input_path)
    print_columns(df.columns)

    name_column = detect_name_column(df.columns)
    print(f"Chosen name column: {name_column}")

    triaged = df.copy()
    triaged["name_clean"] = triaged[name_column].map(normalize_text)
    triaged["canonical_name_guess"] = triaged["name_clean"].map(build_canonical_name_guess)

    classifications = triaged.apply(
        lambda row: classify_row(row, row["name_clean"]),
        axis=1,
        result_type="expand",
    )
    classifications.columns = [
        "entity_level_guess",
        "keep_for_food_db",
        "review_flag",
        "drop_reason",
    ]

    triaged = pd.concat([triaged, classifications], axis=1)

    candidates = triaged[triaged["keep_for_food_db"]].copy()
    rejected = triaged[~triaged["keep_for_food_db"]].copy()

    ensure_parent(out_triaged)
    ensure_parent(out_candidates)
    ensure_parent(out_rejected)

    triaged.to_csv(out_triaged, index=False, encoding="utf-8")
    candidates.to_csv(out_candidates, index=False, encoding="utf-8")
    rejected.to_csv(out_rejected, index=False, encoding="utf-8")

    total_rows = len(triaged)
    kept_rows = int(triaged["keep_for_food_db"].sum())
    review_rows = int(triaged["review_flag"].sum())
    dropped_rows = int((~triaged["keep_for_food_db"] & ~triaged["review_flag"]).sum())

    print(f"Triaged CSV: {out_triaged}")
    print(f"Candidates CSV: {out_candidates}")
    print(f"Recipe-like or rejected CSV: {out_rejected}")
    print(f"Total rows: {total_rows}")
    print(f"Kept rows: {kept_rows}")
    print(f"Review rows: {review_rows}")
    print(f"Dropped rows: {dropped_rows}")


if __name__ == "__main__":
    main()
