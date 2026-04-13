import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


DEFAULT_INPUT = Path("data/recipesdb/source/1_Recipe_csv.csv")
DEFAULT_SUBSET_RAW_OUT = Path("data/recipesdb/draft/recipes_pilot_subset_raw.csv")
DEFAULT_CLEAN_OUT = Path("data/recipesdb/draft/recipes_pilot_recipes_clean.csv")
DEFAULT_SUMMARY_OUT = Path("data/recipesdb/audit/recipes_source_profile_summary.txt")
DEFAULT_DUP_AUDIT_OUT = Path("data/recipesdb/audit/recipes_duplicate_signature_audit.csv")
DEFAULT_EXCLUSION_LOG_OUT = Path("data/recipesdb/audit/recipes_pilot_exclusion_log.csv")

TARGET_PILOT_SIZE = 132
MIN_INGREDIENTS = 5
MAX_INGREDIENTS = 14
MIN_STEPS = 2
MAX_STEPS = 8
MAX_PER_CATEGORY = 18
MAX_PER_SUBCATEGORY = 8

ALLOWED_CATEGORIES = {
    "main dishes",
    "dinner",
    "healthy recipes",
    "vegetarian",
    "salad",
    "soups, stews and chili",
    "beef recipes",
    "chicken",
    "pork",
    "seafood",
    "mexican",
    "italian",
}

PRIMARY_MEAL_CATEGORIES = {
    "main dishes",
    "dinner",
    "vegetarian",
    "salad",
    "soups, stews and chili",
}

OUT_OF_SCOPE_CATEGORY_KEYWORDS = {
    "cake",
    "cookie",
    "dessert",
    "cocktail",
    "candy",
    "frosting",
    "icing",
    "brownie",
    "cupcake",
    "muffin",
    "pie",
    "bread",
    "drink",
    "smoothie",
    "shake",
    "liqueur",
    "martini",
}

OUT_OF_SCOPE_TITLE_KEYWORDS = {
    "cookie",
    "cake",
    "dessert",
    "brownie",
    "cupcake",
    "muffin",
    "smoothie",
    "cocktail",
    "martini",
    "milkshake",
    "frosting",
    "icing",
    "candy",
    "parfait",
    "granola",
}

MOJIBAKE_MARKERS = (
    "\u00e2\u20ac\u201d",
    "\u00e2\u20ac\u201c",
    "\u00e2\u20ac\u2122",
    "\u00e2\u20ac\u0153",
    "\u00e2\u20ac",
    "\u00c2",
    "\u00c3",
)

PACKAGING_PATTERNS = (
    "package",
    "packages",
    "can ",
    "cans ",
    "jar",
    "bottle",
    "box ",
    "boxes ",
    "packet",
    "packets",
    "envelope",
)

ALTERNATIVE_PATTERNS = (
    " or ",
    " to taste",
    " as needed",
    " as desired",
    " if desired",
)

OPTIONAL_PATTERNS = ("optional",)
GARNISH_PATTERNS = ("for garnish", "for serving")
BRAND_HINT_PATTERNS = ("such as",)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Selectie pilot simpla pentru Recipes_DB, fara parsing de ingrediente."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--out-subset-raw", default=str(DEFAULT_SUBSET_RAW_OUT))
    parser.add_argument("--out-clean", default=str(DEFAULT_CLEAN_OUT))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY_OUT))
    parser.add_argument("--out-dup-audit", default=str(DEFAULT_DUP_AUDIT_OUT))
    parser.add_argument("--out-exclusion-log", default=str(DEFAULT_EXCLUSION_LOG_OUT))
    parser.add_argument("--target-size", type=int, default=TARGET_PILOT_SIZE)
    return parser.parse_args()


def normalize_space(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_label(text):
    return normalize_space(text).lower()


def build_signature(title, ingredients_list, directions_list):
    title_norm = normalize_label(title)
    ingredients_norm = " || ".join(normalize_space(item).lower() for item in ingredients_list)
    directions_norm = " || ".join(normalize_space(item).lower() for item in directions_list)
    payload = "||".join([title_norm, ingredients_norm, directions_norm])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def safe_load_list(raw_value):
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError:
        return None

    if not isinstance(value, list):
        return None

    return [str(item) for item in value]


def has_mojibake(text):
    return any(marker in text for marker in MOJIBAKE_MARKERS)


def contains_any_keyword(text, keywords):
    lowered = normalize_label(text)
    return any(keyword in lowered for keyword in keywords)


def count_line_matches(lines, patterns):
    total = 0
    for line in lines:
        lowered = normalize_label(line)
        if any(pattern in lowered for pattern in patterns):
            total += 1
    return total


def score_representative(row):
    score = 0

    if row["category_norm"] in PRIMARY_MEAL_CATEGORIES:
        score += 5
    elif row["category_norm"] in ALLOWED_CATEGORIES:
        score += 3

    if row["subcategory_norm"] in PRIMARY_MEAL_CATEGORIES:
        score += 1

    if row["has_mojibake"]:
        score -= 6

    if row["is_out_of_scope_category"]:
        score -= 6

    if (
        row["num_ingredients_int"] is not None
        and MIN_INGREDIENTS <= row["num_ingredients_int"] <= MAX_INGREDIENTS
    ):
        score += 2

    if row["num_steps_int"] is not None and MIN_STEPS <= row["num_steps_int"] <= MAX_STEPS:
        score += 2

    score -= row["noise_score"]
    score -= row["source_row_number"] / 1_000_000
    return score


def build_row_record(source_row_number, raw_row):
    ingredients_list = safe_load_list(raw_row["ingredients"])
    directions_list = safe_load_list(raw_row["directions"])
    if ingredients_list is None:
        ingredients_list = []
    if directions_list is None:
        directions_list = []

    recipe_title = normalize_space(raw_row["recipe_title"])
    category = normalize_space(raw_row["category"])
    subcategory = normalize_space(raw_row["subcategory"])
    description = normalize_space(raw_row["description"])

    category_norm = normalize_label(category)
    subcategory_norm = normalize_label(subcategory)
    title_norm = normalize_label(recipe_title)

    ingredients_count = int(str(raw_row["num_ingredients"]).strip())
    steps_count = int(str(raw_row["num_steps"]).strip())

    packaging_count = count_line_matches(ingredients_list, PACKAGING_PATTERNS)
    alternative_count = count_line_matches(ingredients_list, ALTERNATIVE_PATTERNS)
    optional_count = count_line_matches(ingredients_list, OPTIONAL_PATTERNS)
    garnish_count = count_line_matches(ingredients_list, GARNISH_PATTERNS)
    brand_hint_count = count_line_matches(ingredients_list, BRAND_HINT_PATTERNS)
    section_header_count = sum(line.strip().endswith(":") for line in ingredients_list)

    combined_text = " ".join([recipe_title, description] + ingredients_list + directions_list)
    out_of_scope_text = " ".join([category_norm, subcategory_norm])

    return {
        "source_row_number": source_row_number,
        "recipe_signature": build_signature(recipe_title, ingredients_list, directions_list),
        "recipe_title": recipe_title,
        "category": category,
        "subcategory": subcategory,
        "description": description,
        "ingredients": raw_row["ingredients"],
        "directions": raw_row["directions"],
        "num_ingredients": raw_row["num_ingredients"],
        "num_steps": raw_row["num_steps"],
        "category_norm": category_norm,
        "subcategory_norm": subcategory_norm,
        "title_norm": title_norm,
        "num_ingredients_int": ingredients_count,
        "num_steps_int": steps_count,
        "packaging_count": packaging_count,
        "alternative_count": alternative_count,
        "optional_count": optional_count,
        "garnish_count": garnish_count,
        "brand_hint_count": brand_hint_count,
        "section_header_count": section_header_count,
        "noise_score": (
            packaging_count
            + alternative_count
            + optional_count
            + brand_hint_count
            + section_header_count * 2
        ),
        "has_mojibake": has_mojibake(combined_text),
        "is_out_of_scope_category": contains_any_keyword(
            out_of_scope_text, OUT_OF_SCOPE_CATEGORY_KEYWORDS
        ),
        "is_out_of_scope_title": contains_any_keyword(
            title_norm, OUT_OF_SCOPE_TITLE_KEYWORDS
        ),
        "is_allowed_category": category_norm in ALLOWED_CATEGORIES,
    }


def choose_representative(rows):
    ranked = sorted(
        rows,
        key=lambda row: (-score_representative(row), row["source_row_number"]),
    )
    kept = dict(ranked[0])
    kept["duplicate_group_size"] = len(rows)
    kept["duplicate_variant_count"] = len(rows) - 1
    return kept


def get_primary_exclusion_reason(row):
    if row["is_out_of_scope_category"]:
        return "out_of_scope_category"

    if row["is_out_of_scope_title"]:
        return "out_of_scope_title"

    if row["has_mojibake"]:
        return "mojibake_detected"

    if not row["is_allowed_category"]:
        return "non_preferred_category"

    if row["num_ingredients_int"] < MIN_INGREDIENTS or row["num_ingredients_int"] > MAX_INGREDIENTS:
        return "ingredient_count_out_of_range"

    if row["num_steps_int"] < MIN_STEPS or row["num_steps_int"] > MAX_STEPS:
        return "step_count_out_of_range"

    if row["packaging_count"] >= 4:
        return "too_many_packaging_patterns"

    if row["alternative_count"] >= 3:
        return "too_many_alternative_patterns"

    if row["optional_count"] >= 2:
        return "too_many_optional_patterns"

    if row["brand_hint_count"] >= 2:
        return "too_many_brand_hints"

    if row["noise_score"] >= 6:
        return "too_noisy_total"

    return ""


def compute_selection_score(row):
    score = 0

    if row["category_norm"] in PRIMARY_MEAL_CATEGORIES:
        score += 6
    else:
        score += 4

    score += 2 - abs(row["num_ingredients_int"] - 9) * 0.25
    score += 2 - abs(row["num_steps_int"] - 5) * 0.25
    score -= row["noise_score"] * 0.75
    score -= row["duplicate_group_size"] * 0.02

    return round(score, 4)


def build_selection_bucket(row):
    return row["category_norm"].replace(" ", "_").replace(",", "")


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_duplicate_audit(groups, representatives):
    audit_rows = []

    for signature, group_rows in groups.items():
        if len(group_rows) <= 1:
            continue

        kept = representatives[signature]
        category_pairs = sorted(
            {f"{row['category']} -> {row['subcategory']}" for row in group_rows}
        )
        source_rows = sorted(row["source_row_number"] for row in group_rows)

        audit_rows.append(
            {
                "recipe_signature": signature,
                "group_size": len(group_rows),
                "kept_source_row_number": kept["source_row_number"],
                "kept_recipe_title": kept["recipe_title"],
                "kept_category": kept["category"],
                "kept_subcategory": kept["subcategory"],
                "all_source_rows": "|".join(str(value) for value in source_rows),
                "category_pairs": " | ".join(category_pairs),
            }
        )

    audit_rows.sort(key=lambda row: (-row["group_size"], row["kept_source_row_number"]))
    return audit_rows


def build_selected_outputs(selected_rows):
    raw_rows = []
    clean_rows = []

    for idx, row in enumerate(selected_rows, start=1):
        pilot_recipe_id = f"pilot_recipe_{idx:03d}"

        raw_rows.append(
            {
                "pilot_recipe_id": pilot_recipe_id,
                "source_row_number": row["source_row_number"],
                "recipe_signature": row["recipe_signature"],
                "duplicate_group_size": row["duplicate_group_size"],
                "selection_bucket": row["selection_bucket"],
                "selection_score": row["selection_score"],
                "noise_score": row["noise_score"],
                "packaging_count": row["packaging_count"],
                "alternative_count": row["alternative_count"],
                "optional_count": row["optional_count"],
                "brand_hint_count": row["brand_hint_count"],
                "recipe_title": row["recipe_title"],
                "category": row["category"],
                "subcategory": row["subcategory"],
                "description": row["description"],
                "ingredients": row["ingredients"],
                "directions": row["directions"],
                "num_ingredients": row["num_ingredients_int"],
                "num_steps": row["num_steps_int"],
            }
        )

        clean_rows.append(
            {
                "pilot_recipe_id": pilot_recipe_id,
                "source_row_number": row["source_row_number"],
                "recipe_signature": row["recipe_signature"],
                "recipe_title": row["recipe_title"],
                "category": row["category"],
                "subcategory": row["subcategory"],
                "description": row["description"],
                "ingredients_json": row["ingredients"],
                "directions_json": row["directions"],
                "num_ingredients": row["num_ingredients_int"],
                "num_steps": row["num_steps_int"],
                "duplicate_group_size": row["duplicate_group_size"],
                "selection_bucket": row["selection_bucket"],
                "selection_score": row["selection_score"],
                "noise_score": row["noise_score"],
                "packaging_count": row["packaging_count"],
                "alternative_count": row["alternative_count"],
                "optional_count": row["optional_count"],
                "brand_hint_count": row["brand_hint_count"],
            }
        )

    return raw_rows, clean_rows


def build_summary_text(
    original_count,
    deduped_count,
    candidate_count,
    selected_count,
    duplicate_audit_rows,
    selected_rows,
    exclusion_reason_counts,
):
    category_counts = Counter(row["category"] for row in selected_rows)
    subcategory_counts = Counter(row["subcategory"] for row in selected_rows)

    lines = [
        "Recipes pilot selection summary",
        "",
        f"Original rows: {original_count}",
        f"Deduped rows: {deduped_count}",
        f"After exclusions: {candidate_count}",
        f"Final pilot subset: {selected_count}",
        f"Duplicate signature groups: {len(duplicate_audit_rows)}",
        "",
        "Top exclusion reasons:",
    ]

    for reason, count in exclusion_reason_counts.most_common(12):
        lines.append(f"- {reason}: {count}")

    lines.extend(
        [
            "",
            "Final pilot categories:",
        ]
    )

    for category, count in category_counts.most_common():
        lines.append(f"- {category}: {count}")

    lines.extend(
        [
            "",
            "Top final pilot subcategories:",
        ]
    )

    for subcategory, count in subcategory_counts.most_common(10):
        lines.append(f"- {subcategory}: {count}")

    return "\n".join(lines) + "\n"


def main():
    args = parse_args()

    input_path = Path(args.input)
    subset_raw_out = Path(args.out_subset_raw)
    clean_out = Path(args.out_clean)
    summary_out = Path(args.out_summary)
    dup_audit_out = Path(args.out_dup_audit)
    exclusion_log_out = Path(args.out_exclusion_log)

    grouped_rows = defaultdict(list)
    original_rows = []

    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for source_row_number, raw_row in enumerate(reader, start=1):
            row = build_row_record(source_row_number, raw_row)
            grouped_rows[row["recipe_signature"]].append(row)
            original_rows.append(row)

    representatives = {}
    deduped_rows = []

    for signature, group_rows in grouped_rows.items():
        kept = choose_representative(group_rows)
        representatives[signature] = kept
        deduped_rows.append(kept)

    duplicate_audit_rows = build_duplicate_audit(grouped_rows, representatives)

    filtered_candidates = []
    exclusion_rows = []
    exclusion_reason_counts = Counter()

    for row in sorted(deduped_rows, key=lambda item: item["source_row_number"]):
        reason = get_primary_exclusion_reason(row)
        if reason:
            exclusion_rows.append(
                {
                    "source_row_number": row["source_row_number"],
                    "recipe_signature": row["recipe_signature"],
                    "recipe_title": row["recipe_title"],
                    "category": row["category"],
                    "subcategory": row["subcategory"],
                    "exclude_stage": "filter",
                    "exclude_reason": reason,
                    "duplicate_group_size": row["duplicate_group_size"],
                    "num_ingredients": row["num_ingredients_int"],
                    "num_steps": row["num_steps_int"],
                    "packaging_count": row["packaging_count"],
                    "alternative_count": row["alternative_count"],
                    "optional_count": row["optional_count"],
                    "brand_hint_count": row["brand_hint_count"],
                    "noise_score": row["noise_score"],
                }
            )
            exclusion_reason_counts[reason] += 1
            continue

        row["selection_score"] = compute_selection_score(row)
        row["selection_bucket"] = build_selection_bucket(row)
        filtered_candidates.append(row)

    selected_rows = []
    category_counts = Counter()
    subcategory_counts = Counter()

    ranked_candidates = sorted(
        filtered_candidates,
        key=lambda row: (
            -row["selection_score"],
            row["noise_score"],
            abs(row["num_ingredients_int"] - 9),
            abs(row["num_steps_int"] - 5),
            row["recipe_title"],
            row["source_row_number"],
        ),
    )

    for row in ranked_candidates:
        reason = ""

        if len(selected_rows) >= args.target_size:
            reason = "target_size_reached"
        elif category_counts[row["category"]] >= MAX_PER_CATEGORY:
            reason = "pilot_category_cap"
        elif subcategory_counts[row["subcategory"]] >= MAX_PER_SUBCATEGORY:
            reason = "pilot_subcategory_cap"

        if reason:
            exclusion_rows.append(
                {
                    "source_row_number": row["source_row_number"],
                    "recipe_signature": row["recipe_signature"],
                    "recipe_title": row["recipe_title"],
                    "category": row["category"],
                    "subcategory": row["subcategory"],
                    "exclude_stage": "selection",
                    "exclude_reason": reason,
                    "duplicate_group_size": row["duplicate_group_size"],
                    "num_ingredients": row["num_ingredients_int"],
                    "num_steps": row["num_steps_int"],
                    "packaging_count": row["packaging_count"],
                    "alternative_count": row["alternative_count"],
                    "optional_count": row["optional_count"],
                    "brand_hint_count": row["brand_hint_count"],
                    "noise_score": row["noise_score"],
                }
            )
            exclusion_reason_counts[reason] += 1
            continue

        selected_rows.append(row)
        category_counts[row["category"]] += 1
        subcategory_counts[row["subcategory"]] += 1

    subset_raw_rows, clean_rows = build_selected_outputs(selected_rows)

    write_csv(
        subset_raw_out,
        subset_raw_rows,
        [
            "pilot_recipe_id",
            "source_row_number",
            "recipe_signature",
            "duplicate_group_size",
            "selection_bucket",
            "selection_score",
            "noise_score",
            "packaging_count",
            "alternative_count",
            "optional_count",
            "brand_hint_count",
            "recipe_title",
            "category",
            "subcategory",
            "description",
            "ingredients",
            "directions",
            "num_ingredients",
            "num_steps",
        ],
    )
    write_csv(
        clean_out,
        clean_rows,
        [
            "pilot_recipe_id",
            "source_row_number",
            "recipe_signature",
            "recipe_title",
            "category",
            "subcategory",
            "description",
            "ingredients_json",
            "directions_json",
            "num_ingredients",
            "num_steps",
            "duplicate_group_size",
            "selection_bucket",
            "selection_score",
            "noise_score",
            "packaging_count",
            "alternative_count",
            "optional_count",
            "brand_hint_count",
        ],
    )
    write_csv(
        dup_audit_out,
        duplicate_audit_rows,
        [
            "recipe_signature",
            "group_size",
            "kept_source_row_number",
            "kept_recipe_title",
            "kept_category",
            "kept_subcategory",
            "all_source_rows",
            "category_pairs",
        ],
    )
    write_csv(
        exclusion_log_out,
        exclusion_rows,
        [
            "source_row_number",
            "recipe_signature",
            "recipe_title",
            "category",
            "subcategory",
            "exclude_stage",
            "exclude_reason",
            "duplicate_group_size",
            "num_ingredients",
            "num_steps",
            "packaging_count",
            "alternative_count",
            "optional_count",
            "brand_hint_count",
            "noise_score",
        ],
    )

    summary_text = build_summary_text(
        original_count=len(original_rows),
        deduped_count=len(deduped_rows),
        candidate_count=len(filtered_candidates),
        selected_count=len(selected_rows),
        duplicate_audit_rows=duplicate_audit_rows,
        selected_rows=selected_rows,
        exclusion_reason_counts=exclusion_reason_counts,
    )

    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text(summary_text, encoding="utf-8")

    print(f"Original rows: {len(original_rows)}")
    print(f"Deduped rows: {len(deduped_rows)}")
    print(f"After exclusions: {len(filtered_candidates)}")
    print(f"Final pilot subset: {len(selected_rows)}")
    print("")
    print("Top exclusion reasons:")
    for reason, count in exclusion_reason_counts.most_common(10):
        print(f" - {reason}: {count}")
    print("")
    print(f"Written: {subset_raw_out}")
    print(f"Written: {clean_out}")
    print(f"Written: {summary_out}")
    print(f"Written: {dup_audit_out}")
    print(f"Written: {exclusion_log_out}")


if __name__ == "__main__":
    main()
