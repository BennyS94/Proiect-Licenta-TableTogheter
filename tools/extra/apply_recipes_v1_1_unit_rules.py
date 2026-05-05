from __future__ import annotations

import argparse
import csv
import math
import re
import unicodedata
from collections import Counter
from pathlib import Path


DEFAULT_PARSED_INGREDIENTS = Path("data/recipesdb/draft/recipes_v1_1_ingredients_parsed.csv")
DEFAULT_MAPPING = Path("data/recipesdb/draft/recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1.csv")
DEFAULT_FOODDB = Path("data/fooddb/draft/fooddb_v1_1_core_master_draft.csv")
DEFAULT_OUT_PARSED = Path("data/recipesdb/draft/recipes_v1_1_ingredients_parsed_unit_rules.csv")
DEFAULT_OUT_SUMMARY = Path("data/recipesdb/audit/recipes_v1_1_unit_rules_summary.txt")
DEFAULT_OUT_APPLIED = Path("data/recipesdb/audit/recipes_v1_1_unit_rules_applied.csv")
DEFAULT_OUT_DEFERRED = Path("data/recipesdb/audit/recipes_v1_1_unit_rules_deferred.csv")

APPLIED_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "ingredient_position",
    "ingredient_raw_text",
    "ingredient_name_normalized",
    "quantity_value",
    "quantity_unit",
    "old_quantity_grams_estimated",
    "new_quantity_grams_estimated",
    "unit_rule_key",
    "unit_rule_reason",
    "mapping_status",
    "mapped_food_id",
    "safety",
]

DEFERRED_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "ingredient_position",
    "ingredient_raw_text",
    "ingredient_name_normalized",
    "quantity_value",
    "quantity_unit",
    "defer_reason",
    "suggested_future_rule",
]

UNIT_ALIASES = {
    "teaspoon": "teaspoon",
    "teaspoons": "teaspoon",
    "tsp": "teaspoon",
    "tablespoon": "tablespoon",
    "tablespoons": "tablespoon",
    "tbsp": "tablespoon",
    "cup": "cup",
    "cups": "cup",
    "clove": "clove",
    "cloves": "clove",
    "count": "count",
    "piece": "count",
    "pieces": "count",
    "whole": "count",
}

EXPLICIT_GRAMS_PER_UNIT = {
    ("olive oil", "teaspoon"): (4.5, "oil_teaspoon_density"),
    ("olive oil", "tablespoon"): (13.5, "oil_tablespoon_density"),
    ("vegetable oil", "teaspoon"): (4.5, "oil_teaspoon_density"),
    ("vegetable oil", "tablespoon"): (13.5, "oil_tablespoon_density"),
    ("soy sauce", "teaspoon"): (5.3, "soy_sauce_teaspoon_density"),
    ("soy sauce", "tablespoon"): (16.0, "soy_sauce_tablespoon_density"),
    ("white sugar", "teaspoon"): (4.2, "white_sugar_teaspoon_density"),
    ("white sugar", "tablespoon"): (12.5, "white_sugar_tablespoon_density"),
    ("white sugar", "cup"): (200.0, "white_sugar_cup_density"),
    ("brown sugar", "teaspoon"): (4.5, "brown_sugar_teaspoon_density"),
    ("brown sugar", "tablespoon"): (13.0, "brown_sugar_tablespoon_density"),
    ("brown sugar", "cup"): (220.0, "brown_sugar_cup_density"),
    ("cornstarch", "teaspoon"): (2.7, "cornstarch_teaspoon_density"),
    ("cornstarch", "tablespoon"): (8.0, "cornstarch_tablespoon_density"),
    ("black pepper", "teaspoon"): (2.3, "black_pepper_teaspoon_density"),
    ("black pepper", "tablespoon"): (6.9, "black_pepper_tablespoon_density"),
    ("milk", "cup"): (244.0, "milk_cup_density"),
    ("chicken broth", "cup"): (240.0, "chicken_broth_cup_density"),
    ("water", "cup"): (240.0, "water_cup_weight_diagnostics_only"),
}

DRIED_HERB_RULES = {
    ("dried oregano", "teaspoon"): (1.0, "dried_oregano_teaspoon_density"),
    ("dried oregano", "tablespoon"): (3.0, "dried_oregano_tablespoon_density"),
    ("dried basil", "teaspoon"): (0.7, "dried_basil_teaspoon_density"),
    ("dried basil", "tablespoon"): (2.1, "dried_basil_tablespoon_density"),
    ("dried thyme", "teaspoon"): (1.0, "dried_thyme_teaspoon_density"),
    ("dried thyme", "tablespoon"): (3.0, "dried_thyme_tablespoon_density"),
    ("dried parsley", "teaspoon"): (0.5, "dried_parsley_teaspoon_density"),
    ("dried parsley", "tablespoon"): (1.5, "dried_parsley_tablespoon_density"),
}

PACKAGE_UNITS = {"can", "jar", "package", "packet", "bag", "bottle", "container", "box"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aplica reguli conservative unit-to-grams pentru Recipes_DB v1.1.")
    parser.add_argument("--ingredients", "--ingredients_path", dest="ingredients", default=str(DEFAULT_PARSED_INGREDIENTS))
    parser.add_argument("--mapping", "--mapping_path", dest="mapping", default=str(DEFAULT_MAPPING))
    parser.add_argument("--fooddb", "--fooddb_path", dest="fooddb", default=str(DEFAULT_FOODDB))
    parser.add_argument("--out_ingredients", default=str(DEFAULT_OUT_PARSED))
    parser.add_argument("--out_summary", default=str(DEFAULT_OUT_SUMMARY))
    parser.add_argument("--out_applied", default=str(DEFAULT_OUT_APPLIED))
    parser.add_argument("--out_deferred", default=str(DEFAULT_OUT_DEFERRED))
    return parser.parse_args()


def read_csv_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    return str(value or "").strip()


def normalize_text(value: object) -> str:
    text = clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = text.replace("_", " ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_float(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def format_number(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".rstrip("0").rstrip(".")


def has_valid_grams(row: dict[str, str]) -> bool:
    grams = parse_float(row.get("quantity_grams_estimated"))
    return grams is not None and grams > 0


def row_key(row: dict[str, str]) -> tuple[str, str]:
    return clean_text(row.get("recipe_id_candidate")), clean_text(row.get("ingredient_position"))


def build_mapping_index(mapping_rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    return {row_key(row): row for row in mapping_rows}


def normalize_unit(value: object) -> str:
    unit = normalize_text(value)
    return UNIT_ALIASES.get(unit, unit)


def append_parse_note(existing: str, note: str) -> str:
    notes = [part.strip() for part in clean_text(existing).split(";") if part.strip()]
    notes.append(note)
    return "; ".join(dict.fromkeys(notes))


def is_fresh_garlic_candidate(row: dict[str, str]) -> bool:
    name = normalize_text(row.get("ingredient_name_normalized"))
    parsed = normalize_text(row.get("ingredient_name_parsed"))
    raw = normalize_text(row.get("ingredient_raw_text"))
    if name != "garlic":
        return False
    if "powder" in parsed or "powder" in raw:
        return False
    return parsed in {"garlic", "minced garlic", "fresh garlic", "fresh minced garlic"} or "garlic" in parsed


def find_unit_rule(row: dict[str, str]) -> tuple[float | None, str, str, str]:
    quantity_value = parse_float(row.get("quantity_value"))
    if quantity_value is None or quantity_value <= 0:
        return None, "", "missing_or_invalid_quantity_value", ""

    unit = normalize_unit(row.get("quantity_unit"))
    if not unit:
        return None, "", "missing_quantity_unit", ""

    name = normalize_text(row.get("ingredient_name_normalized"))
    parsed = normalize_text(row.get("ingredient_name_parsed"))
    raw = normalize_text(row.get("ingredient_raw_text"))

    explicit_rule = EXPLICIT_GRAMS_PER_UNIT.get((name, unit))
    if explicit_rule:
        grams_per_unit, reason = explicit_rule
        if name == "water":
            reason = f"{reason}; no_macro_impact_but_weight_diagnostic"
        return quantity_value * grams_per_unit, f"{name}_{unit}", reason, ""

    if is_fresh_garlic_candidate(row) and unit in {"clove", "teaspoon", "tablespoon"}:
        grams_per_unit = {"clove": 3.0, "teaspoon": 2.8, "tablespoon": 8.4}[unit]
        return quantity_value * grams_per_unit, f"fresh_garlic_{unit}", "fresh_garlic_explicit_density", ""

    if name in {"onion", "yellow onion", "red onion"} and unit in {"count", "cup"}:
        grams_per_unit = 110.0 if unit == "count" else 160.0
        return quantity_value * grams_per_unit, f"{name}_{unit}", "onion_explicit_household_weight", ""

    if name in {"carrot", "carrots"} and unit in {"count", "cup"}:
        grams_per_unit = 60.0 if unit == "count" else 125.0
        return quantity_value * grams_per_unit, f"carrot_{unit}", "carrot_explicit_household_weight", ""

    dried_herb_rule = DRIED_HERB_RULES.get((parsed, unit))
    if dried_herb_rule:
        grams_per_unit, reason = dried_herb_rule
        return quantity_value * grams_per_unit, f"{parsed}_{unit}", reason, ""

    return None, "", build_defer_reason(name, parsed, raw, unit), suggest_future_rule(name, unit, raw)


def build_defer_reason(name: str, parsed: str, raw: str, unit: str) -> str:
    if unit in PACKAGE_UNITS:
        return "package_or_container_unit_deferred"
    if "rice" in name and unit == "cup":
        return "rice_cup_deferred_without_cooked_signal"
    if "pasta" in name and unit == "cup":
        return "pasta_cup_deferred_without_cooked_signal"
    if "potato" in name and unit == "count":
        return "potato_count_deferred_size_ambiguous"
    if name == "tomato sauce" and unit == "can":
        return "tomato_sauce_can_deferred"
    if "tomato" in name and unit == "can":
        return "canned_tomato_deferred"
    if any(token in name for token in ("beef", "chicken", "pork", "turkey", "shrimp")) and unit in {"count", "piece"}:
        return "meat_or_seafood_count_deferred"
    if "cheese" in name and unit == "cup":
        return "cheese_cup_deferred_without_specific_safe_rule"
    if "butter" in name:
        return "butter_deferred_until_mapping_decision"
    if name in {"garlic powder", "onion powder"}:
        return "powder_density_deferred_not_fresh_rule"
    if unit in {"teaspoon", "tablespoon", "cup", "count", "clove"}:
        return "no_explicit_safe_rule_for_ingredient_unit"
    return "unit_not_supported_in_this_pass"


def suggest_future_rule(name: str, unit: str, raw: str) -> str:
    if unit in PACKAGE_UNITS:
        return "review_package_or_can_weight_from_label_or_parser"
    if "rice" in name and unit == "cup":
        return "add_cooked_rice_cup_rule_only_when_text_confirms_cooked"
    if "pasta" in name and unit == "cup":
        return "add_cooked_pasta_cup_rule_only_when_text_confirms_cooked"
    if "potato" in name and unit == "count":
        return "add_potato_count_rule_only_with_size_signal"
    if name == "tomato sauce" and unit == "can":
        return "add_tomato_sauce_can_rule_after_fooddb_decision"
    if "cheese" in name and unit == "cup":
        return "add_specific_cheese_cup_rules_after_review"
    if "butter" in name:
        return "decide_butter_mapping_then_add_tablespoon_cup_rules"
    if name in {"garlic powder", "onion powder"}:
        return "add_powder_specific_density_rule_after_review"
    return ""


def build_applied_row(
    source_row: dict[str, str],
    mapping_row: dict[str, str],
    old_grams: str,
    new_grams: float,
    rule_key: str,
    rule_reason: str,
) -> dict[str, object]:
    return {
        "recipe_id_candidate": clean_text(source_row.get("recipe_id_candidate")),
        "display_name": clean_text(source_row.get("display_name")),
        "ingredient_position": clean_text(source_row.get("ingredient_position")),
        "ingredient_raw_text": clean_text(source_row.get("ingredient_raw_text")),
        "ingredient_name_normalized": clean_text(source_row.get("ingredient_name_normalized")),
        "quantity_value": clean_text(source_row.get("quantity_value")),
        "quantity_unit": clean_text(source_row.get("quantity_unit")),
        "old_quantity_grams_estimated": clean_text(old_grams),
        "new_quantity_grams_estimated": format_number(new_grams),
        "unit_rule_key": rule_key,
        "unit_rule_reason": rule_reason,
        "mapping_status": clean_text(mapping_row.get("mapping_status")),
        "mapped_food_id": clean_text(mapping_row.get("mapped_food_id")),
        "safety": "safe_auto",
    }


def build_deferred_row(
    source_row: dict[str, str],
    defer_reason: str,
    suggested_future_rule: str,
) -> dict[str, object]:
    return {
        "recipe_id_candidate": clean_text(source_row.get("recipe_id_candidate")),
        "display_name": clean_text(source_row.get("display_name")),
        "ingredient_position": clean_text(source_row.get("ingredient_position")),
        "ingredient_raw_text": clean_text(source_row.get("ingredient_raw_text")),
        "ingredient_name_normalized": clean_text(source_row.get("ingredient_name_normalized")),
        "quantity_value": clean_text(source_row.get("quantity_value")),
        "quantity_unit": clean_text(source_row.get("quantity_unit")),
        "defer_reason": defer_reason,
        "suggested_future_rule": suggested_future_rule,
    }


def write_summary(
    path: Path,
    original_rows: list[dict[str, str]],
    updated_rows: list[dict[str, str]],
    applied_rows: list[dict[str, object]],
    deferred_rows: list[dict[str, object]],
    fooddb_row_count: int,
) -> None:
    before_with_grams = sum(1 for row in original_rows if has_valid_grams(row))
    after_with_grams = sum(1 for row in updated_rows if has_valid_grams(row))
    total_rows = len(original_rows)
    rule_counts = Counter(clean_text(row.get("unit_rule_key")) for row in applied_rows)
    deferred_unit_counts = Counter(clean_text(row.get("quantity_unit")) or "<missing>" for row in deferred_rows)
    deferred_ingredient_counts = Counter(clean_text(row.get("ingredient_name_normalized")) for row in deferred_rows)
    status_gain_counts = Counter(clean_text(row.get("mapping_status")) or "missing_mapping" for row in applied_rows)
    mapped_rows_gained = sum(
        count for status, count in status_gain_counts.items()
        if status in {"accepted_auto", "review_needed"}
    )

    lines = [
        "Recipes_DB v1.1 unit-to-grams pass summary",
        "",
        f"Food_DB v1.1 draft rows read: {fooddb_row_count}",
        f"Total ingredient rows: {total_rows}",
        f"Rows with grams before: {before_with_grams}",
        f"Rows with grams after: {after_with_grams}",
        f"Grams coverage before: {before_with_grams / total_rows:.2%}",
        f"Grams coverage after: {after_with_grams / total_rows:.2%}",
        f"Rules applied count: {len(applied_rows)}",
        "",
        "Rows that gained grams by mapping status:",
        f"- mapped rows gained grams (accepted_auto + review_needed): {mapped_rows_gained}",
        f"- accepted_auto rows gained grams: {status_gain_counts['accepted_auto']}",
        f"- review_needed rows gained grams: {status_gain_counts['review_needed']}",
        f"- unmapped rows gained grams for future use: {status_gain_counts['unmapped']}",
        "",
        "Top rules by usage:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in rule_counts.most_common(30))
    lines.extend(["", "Top deferred units:"])
    lines.extend(f"- {name}: {count}" for name, count in deferred_unit_counts.most_common(30))
    lines.extend(["", "Top deferred ingredients:"])
    lines.extend(f"- {name}: {count}" for name, count in deferred_ingredient_counts.most_common(30))
    lines.extend(
        [
            "",
            "Recommended next patch:",
            "- Review high-impact remaining gaps before nutrition cache rebuild: beef/turkey/butter/lemon/green onions/chicken thighs.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def apply_unit_rules(
    ingredient_rows: list[dict[str, str]],
    ingredient_fieldnames: list[str],
    mapping_rows: list[dict[str, str]],
    fooddb_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, object]], list[dict[str, object]], int]:
    mapping_index = build_mapping_index(mapping_rows)
    updated_rows: list[dict[str, str]] = []
    applied_rows: list[dict[str, object]] = []
    deferred_rows: list[dict[str, object]] = []

    for row in ingredient_rows:
        updated = dict(row)
        if has_valid_grams(row):
            updated_rows.append(updated)
            continue

        new_grams, rule_key, rule_reason, suggested_future_rule = find_unit_rule(row)
        mapping_row = mapping_index.get(row_key(row), {})
        if new_grams is not None and new_grams > 0:
            old_grams = clean_text(row.get("quantity_grams_estimated"))
            updated["quantity_grams_estimated"] = format_number(new_grams)
            updated["grams_estimation_method"] = f"unit_rule_v1_1:{rule_key}"
            updated["parse_notes"] = append_parse_note(
                updated.get("parse_notes", ""),
                f"unit_rule_v1_1_applied:{rule_key}",
            )
            applied_rows.append(build_applied_row(row, mapping_row, old_grams, new_grams, rule_key, rule_reason))
        else:
            deferred_rows.append(build_deferred_row(row, rule_reason, suggested_future_rule))
        updated_rows.append(updated)

    # Pastreaza forma initiala a tabelului parsed.
    updated_rows = [{field: row.get(field, "") for field in ingredient_fieldnames} for row in updated_rows]
    return updated_rows, applied_rows, deferred_rows, len(fooddb_rows)


def main() -> None:
    args = parse_args()
    ingredient_rows, ingredient_fieldnames = read_csv_rows(Path(args.ingredients))
    mapping_rows, _ = read_csv_rows(Path(args.mapping))
    fooddb_rows, _ = read_csv_rows(Path(args.fooddb))
    updated_rows, applied_rows, deferred_rows, fooddb_row_count = apply_unit_rules(
        ingredient_rows,
        ingredient_fieldnames,
        mapping_rows,
        fooddb_rows,
    )

    write_csv(Path(args.out_ingredients), updated_rows, ingredient_fieldnames)
    write_csv(Path(args.out_applied), applied_rows, APPLIED_COLUMNS)
    write_csv(Path(args.out_deferred), deferred_rows, DEFERRED_COLUMNS)
    write_summary(
        Path(args.out_summary),
        ingredient_rows,
        updated_rows,
        applied_rows,
        deferred_rows,
        fooddb_row_count,
    )

    print(f"Total ingredient rows: {len(ingredient_rows)}")
    print(f"Rows with grams before: {sum(1 for row in ingredient_rows if has_valid_grams(row))}")
    print(f"Rows with grams after: {sum(1 for row in updated_rows if has_valid_grams(row))}")
    print(f"Rules applied count: {len(applied_rows)}")
    print(f"Deferred count: {len(deferred_rows)}")
    print(f"Written: {args.out_ingredients}")
    print(f"Written: {args.out_summary}")
    print(f"Written: {args.out_applied}")
    print(f"Written: {args.out_deferred}")


if __name__ == "__main__":
    main()
