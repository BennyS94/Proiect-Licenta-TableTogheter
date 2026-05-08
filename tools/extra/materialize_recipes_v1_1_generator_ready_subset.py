from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

CURATED_RECIPES = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_curated_200.csv"
ROUND9_PARSED_INGREDIENTS = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_ingredients_parsed_unit_rules_round9.csv"
)
ROUND9_MAPPING = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round9_final_mapping.csv"
)
ROUND10_CACHE = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_nutrition_cache_draft_round10.csv"
ROUND10_READINESS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_generator_readiness.csv"
)
ROUND10_READINESS_SUMMARY = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_generator_readiness_summary.txt"
)

OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready"
OUT_RECIPES = OUT_DIR / "recipes.csv"
OUT_INGREDIENTS = OUT_DIR / "recipe_ingredients.csv"
OUT_NUTRITION = OUT_DIR / "recipe_nutrition_cache.csv"
OUT_README = OUT_DIR / "README_v1_1_generator_ready.txt"
OUT_SUMMARY = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_generator_ready_materialization_summary.txt"
)
OUT_AUDIT = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_generator_ready_materialization_audit.csv"
)
SLOT_CONTENT_AUDIT = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_generator_ready_slot_content_audit.csv"
)
SLOT_CHECKED_OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready_slot_checked"
SLOT_CHECKED_OUT_RECIPES = SLOT_CHECKED_OUT_DIR / "recipes.csv"
SLOT_CHECKED_OUT_INGREDIENTS = SLOT_CHECKED_OUT_DIR / "recipe_ingredients.csv"
SLOT_CHECKED_OUT_NUTRITION = SLOT_CHECKED_OUT_DIR / "recipe_nutrition_cache.csv"
SLOT_CHECKED_OUT_README = SLOT_CHECKED_OUT_DIR / "README_v1_1_generator_ready_slot_checked.txt"
SLOT_CHECKED_OUT_SUMMARY = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "audit"
    / "recipes_v1_1_generator_ready_slot_checked_materialization_summary.txt"
)
SLOT_CHECKED_OUT_AUDIT = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "audit"
    / "recipes_v1_1_generator_ready_slot_checked_materialization_audit.csv"
)

SOURCE_DATASET = "recipes_dataset_64k_dishes_v1_1_generator_ready_draft"
SCOPE_STATUS = "v1_1_generator_ready_draft"
NUTRITION_BASIS = "recipes_v1_1_generator_ready_round10_draft"
CACHE_VERSION = "recipes_v1_1_generator_ready_round10"
QC_NOTE = "recipes_v1_1_round10_generator_ready_subset"
SLOT_CHECKED_SOURCE_DATASET = "recipes_dataset_64k_dishes_v1_1_generator_ready_slot_checked_draft"
SLOT_CHECKED_SCOPE_STATUS = "v1_1_generator_ready_slot_checked_draft"
SLOT_CHECKED_NUTRITION_BASIS = "recipes_v1_1_generator_ready_slot_checked_round10_draft"
SLOT_CHECKED_CACHE_VERSION = "recipes_v1_1_generator_ready_slot_checked_round10"
SLOT_CHECKED_QC_NOTE = "recipes_v1_1_round10_generator_ready_slot_checked_subset"

RECIPES_COLUMNS = [
    "recipe_id",
    "source_recipe_id",
    "source_dataset",
    "recipe_name",
    "display_name",
    "recipe_family_name",
    "recipe_kind",
    "recipe_category",
    "recipe_subcategory",
    "recipe_cuisine",
    "directions_json",
    "directions_step_count",
    "servings_declared",
    "servings_normalized",
    "prep_time_min",
    "cook_time_min",
    "total_time_min",
    "difficulty_level",
    "scope_status",
    "has_ingredients_parsed",
    "has_nutrition_cache",
    "is_pilot_recipe",
    "is_active",
    "qc_recipe_status",
    "qc_notes",
]
SLOT_CHECKED_RECIPE_COLUMNS = RECIPES_COLUMNS + [
    "allowed_slots_json",
    "slot_policy_reason",
    "content_quality_status",
    "content_exclusion_reason",
]

RECIPE_INGREDIENT_COLUMNS = [
    "recipe_ingredient_id",
    "recipe_id",
    "ingredient_position",
    "ingredient_raw_text",
    "ingredient_name_parsed",
    "ingredient_name_normalized",
    "quantity_value",
    "quantity_unit",
    "quantity_text",
    "quantity_grams_estimated",
    "ingredient_role",
    "ingredient_slot_key",
    "is_optional",
    "is_substitutable",
    "substitution_group_id",
    "mapped_food_id",
    "mapped_food_canonical_name",
    "mapping_status",
    "mapping_confidence",
    "mapping_method",
    "mapping_notes",
    "qc_ingredient_status",
    "qc_notes",
]

NUTRITION_COLUMNS = [
    "recipe_id",
    "nutrition_basis",
    "servings_basis",
    "total_weight_grams_estimated",
    "energy_kcal_total",
    "protein_g_total",
    "carbs_g_total",
    "fat_g_total",
    "fibre_g_total",
    "sugars_g_total",
    "salt_g_total",
    "water_g_total",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "fibre_g_per_serving",
    "sugars_g_per_serving",
    "salt_g_per_serving",
    "water_g_per_serving",
    "mapped_ingredient_count",
    "unmapped_ingredient_count",
    "mapped_weight_ratio",
    "cache_status",
    "cache_version",
    "qc_notes",
    "macro_relevant_mapped_weight_ratio",
    "uses_pilot_servings_fallback",
    "servings_estimation_method",
    "servings_adjustment_applied",
    "original_servings_basis",
    "adjusted_servings_basis",
    "quality_flags",
]

AUDIT_COLUMNS = [
    "recipe_id",
    "display_name",
    "recipe_kind",
    "primary_protein",
    "generator_readiness",
    "materialization_decision",
    "decision_reason",
    "cache_status",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "servings_basis",
    "servings_adjustment_applied",
]
SLOT_CHECKED_AUDIT_COLUMNS = AUDIT_COLUMNS + [
    "content_quality_status",
    "content_exclusion_reason",
    "allowed_slots_json",
    "slot_policy_reason",
    "slot_policy_confidence",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materializeaza subsetul draft Recipes_DB v1.1 generator-ready.")
    parser.add_argument(
        "--slot_checked",
        action="store_true",
        help="Scrie varianta slot/content checked intr-un folder separat.",
    )
    return parser.parse_args()


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Lipseste fisierul asteptat: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
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


def format_number(value: float | None, digits: int = 4) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def median(values: list[float]) -> float | None:
    clean_values = [value for value in values if not math.isnan(value) and not math.isinf(value)]
    if not clean_values:
        return None
    return statistics.median(clean_values)


def index_by_recipe_id(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {clean_text(row.get("recipe_id_candidate") or row.get("recipe_id")): row for row in rows}


def key_for_ingredient(row: dict[str, str]) -> tuple[str, str]:
    return (
        clean_text(row.get("recipe_id_candidate") or row.get("recipe_id")),
        clean_text(row.get("ingredient_position")),
    )


def ingredient_sort_key(row: dict[str, str]) -> tuple[str, int, str]:
    position = parse_float(row.get("ingredient_position"))
    position_sort = int(position) if position is not None else 9999
    return (
        clean_text(row.get("recipe_id_candidate") or row.get("recipe_id")),
        position_sort,
        clean_text(row.get("ingredient_raw_text")),
    )


def directions_step_count(row: dict[str, str]) -> str:
    explicit_count = parse_float(row.get("num_steps"))
    if explicit_count is not None:
        return str(int(explicit_count))
    directions_text = clean_text(row.get("directions_json"))
    if not directions_text:
        return ""
    try:
        parsed = json.loads(directions_text)
    except json.JSONDecodeError:
        return ""
    if isinstance(parsed, list):
        return str(len(parsed))
    return ""


def included_recipe_ids(
    readiness_rows: list[dict[str, str]],
    slot_audit_rows: list[dict[str, str]] | None = None,
) -> set[str]:
    base_ids = {
        clean_text(row.get("recipe_id_candidate"))
        for row in readiness_rows
        if clean_text(row.get("generator_readiness")) == "generator_ready"
    }
    if slot_audit_rows is None:
        return base_ids
    keep_ids = {
        clean_text(row.get("recipe_id"))
        for row in slot_audit_rows
        if clean_text(row.get("content_quality_status")) == "keep"
    }
    return base_ids & keep_ids


def build_recipes_rows(
    recipe_rows: list[dict[str, str]],
    cache_by_id: dict[str, dict[str, str]],
    readiness_by_id: dict[str, dict[str, str]],
    included_ids: set[str],
    slot_audit_by_id: dict[str, dict[str, str]] | None = None,
    source_dataset: str = SOURCE_DATASET,
    scope_status: str = SCOPE_STATUS,
    qc_note: str = QC_NOTE,
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for recipe in recipe_rows:
        recipe_id = clean_text(recipe.get("recipe_id_candidate"))
        if recipe_id not in included_ids:
            continue
        cache = cache_by_id.get(recipe_id, {})
        readiness = readiness_by_id.get(recipe_id, {})
        slot_audit = (slot_audit_by_id or {}).get(recipe_id, {})
        display_name = clean_text(recipe.get("display_name"))
        kind = clean_text(recipe.get("recipe_kind_guess"))
        component_note = ""
        if kind in {"protein_component", "carb_side", "veg_side", "component"}:
            component_note = "; component_or_side_flagged"
        output.append(
            {
                "recipe_id": recipe_id,
                "source_recipe_id": clean_text(recipe.get("source_index")),
                "source_dataset": source_dataset,
                "recipe_name": display_name,
                "display_name": display_name,
                "recipe_family_name": display_name,
                "recipe_kind": kind,
                "recipe_category": clean_text(recipe.get("source_category")),
                "recipe_subcategory": clean_text(recipe.get("source_subcategory")),
                "recipe_cuisine": "",
                "directions_json": clean_text(recipe.get("directions_json")),
                "directions_step_count": directions_step_count(recipe),
                "servings_declared": "",
                "servings_normalized": clean_text(cache.get("servings_basis")),
                "prep_time_min": "",
                "cook_time_min": "",
                "total_time_min": "",
                "difficulty_level": "",
                "scope_status": scope_status,
                "has_ingredients_parsed": "1",
                "has_nutrition_cache": "1",
                "is_pilot_recipe": "0",
                "is_active": "1",
                "qc_recipe_status": "generator_ready_draft",
                "qc_notes": (
                    f"{qc_note}; readiness={clean_text(readiness.get('generator_readiness'))}"
                    f"{component_note}; time_fields_missing_in_round10_source"
                ),
                "allowed_slots_json": clean_text(slot_audit.get("allowed_slots_json")),
                "slot_policy_reason": clean_text(slot_audit.get("slot_policy_reason")),
                "content_quality_status": clean_text(slot_audit.get("content_quality_status")),
                "content_exclusion_reason": clean_text(slot_audit.get("content_exclusion_reason")),
            }
        )
    return output


def build_ingredient_rows(
    parsed_rows: list[dict[str, str]],
    mapping_rows: list[dict[str, str]],
    included_ids: set[str],
) -> list[dict[str, object]]:
    mapping_by_key = {key_for_ingredient(row): row for row in mapping_rows}
    output: list[dict[str, object]] = []
    for parsed in sorted(parsed_rows, key=ingredient_sort_key):
        recipe_id = clean_text(parsed.get("recipe_id_candidate"))
        if recipe_id not in included_ids:
            continue
        position = clean_text(parsed.get("ingredient_position"))
        mapping = mapping_by_key.get((recipe_id, position), {})
        mapping_status = clean_text(mapping.get("mapping_status"))
        qc_status = "mapped_or_deferred_from_round9"
        if mapping_status == "accepted_auto":
            qc_status = "accepted_auto_round9"
        elif mapping_status == "review_needed":
            qc_status = "review_needed_round9"
        elif mapping_status == "unmapped":
            qc_status = "unmapped_round9"
        output.append(
            {
                "recipe_ingredient_id": f"{recipe_id}_ingredient_{position.zfill(3)}",
                "recipe_id": recipe_id,
                "ingredient_position": position,
                "ingredient_raw_text": clean_text(parsed.get("ingredient_raw_text")),
                "ingredient_name_parsed": clean_text(parsed.get("ingredient_name_parsed")),
                "ingredient_name_normalized": clean_text(parsed.get("ingredient_name_normalized")),
                "quantity_value": clean_text(parsed.get("quantity_value")),
                "quantity_unit": clean_text(parsed.get("quantity_unit")),
                "quantity_text": clean_text(parsed.get("quantity_text")),
                "quantity_grams_estimated": clean_text(parsed.get("quantity_grams_estimated")),
                "ingredient_role": clean_text(parsed.get("ingredient_role")),
                "ingredient_slot_key": clean_text(parsed.get("ingredient_slot_key")),
                "is_optional": clean_text(parsed.get("is_optional")),
                "is_substitutable": clean_text(parsed.get("is_substitutable")),
                "substitution_group_id": "",
                "mapped_food_id": clean_text(mapping.get("mapped_food_id")),
                "mapped_food_canonical_name": clean_text(mapping.get("mapped_food_canonical_name")),
                "mapping_status": mapping_status,
                "mapping_confidence": clean_text(mapping.get("mapping_confidence")),
                "mapping_method": clean_text(mapping.get("mapping_method")),
                "mapping_notes": clean_text(mapping.get("mapping_notes") or mapping.get("manual_decision_notes")),
                "qc_ingredient_status": qc_status,
                "qc_notes": "recipes_v1_1_round9_final_mapping_used_for_generator_ready_subset",
            }
        )
    return output


def build_nutrition_rows(
    cache_rows: list[dict[str, str]],
    included_ids: set[str],
    nutrition_basis: str = NUTRITION_BASIS,
    cache_version: str = CACHE_VERSION,
    qc_note: str = "round10_cache_values; v1_1_generator_ready_draft_only",
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for cache in cache_rows:
        recipe_id = clean_text(cache.get("recipe_id_candidate"))
        if recipe_id not in included_ids:
            continue
        review_count = int(parse_float(cache.get("review_needed_count")) or 0)
        unmapped_count = int(parse_float(cache.get("unmapped_count")) or 0)
        output.append(
            {
                "recipe_id": recipe_id,
                "nutrition_basis": nutrition_basis,
                "servings_basis": clean_text(cache.get("servings_basis")),
                "total_weight_grams_estimated": clean_text(cache.get("total_weight_grams_estimated")),
                "energy_kcal_total": clean_text(cache.get("energy_kcal_total")),
                "protein_g_total": clean_text(cache.get("protein_g_total")),
                "carbs_g_total": clean_text(cache.get("carbs_g_total")),
                "fat_g_total": clean_text(cache.get("fat_g_total")),
                "fibre_g_total": "",
                "sugars_g_total": "",
                "salt_g_total": "",
                "water_g_total": "",
                "energy_kcal_per_serving": clean_text(cache.get("energy_kcal_per_serving")),
                "protein_g_per_serving": clean_text(cache.get("protein_g_per_serving")),
                "carbs_g_per_serving": clean_text(cache.get("carbs_g_per_serving")),
                "fat_g_per_serving": clean_text(cache.get("fat_g_per_serving")),
                "fibre_g_per_serving": "",
                "sugars_g_per_serving": "",
                "salt_g_per_serving": "",
                "water_g_per_serving": "",
                "mapped_ingredient_count": clean_text(cache.get("accepted_auto_count")),
                "unmapped_ingredient_count": str(review_count + unmapped_count),
                "mapped_weight_ratio": clean_text(cache.get("mapped_weight_ratio")),
                "cache_status": clean_text(cache.get("cache_status")),
                "cache_version": cache_version,
                "qc_notes": qc_note,
                "macro_relevant_mapped_weight_ratio": clean_text(
                    cache.get("macro_relevant_mapped_weight_ratio")
                ),
                "uses_pilot_servings_fallback": clean_text(cache.get("uses_pilot_servings_fallback")),
                "servings_estimation_method": clean_text(cache.get("servings_estimation_method")),
                "servings_adjustment_applied": clean_text(cache.get("servings_adjustment_applied")),
                "original_servings_basis": clean_text(cache.get("original_servings_basis")),
                "adjusted_servings_basis": clean_text(cache.get("adjusted_servings_basis")),
                "quality_flags": clean_text(cache.get("quality_flags")),
            }
        )
    return output


def build_audit_rows(
    readiness_rows: list[dict[str, str]],
    cache_by_id: dict[str, dict[str, str]],
    included_ids: set[str],
    slot_audit_by_id: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for readiness in readiness_rows:
        recipe_id = clean_text(readiness.get("recipe_id_candidate"))
        cache = cache_by_id.get(recipe_id, {})
        slot_audit = (slot_audit_by_id or {}).get(recipe_id, {})
        included = recipe_id in included_ids
        readiness_value = clean_text(readiness.get("generator_readiness"))
        content_status = clean_text(slot_audit.get("content_quality_status"))
        if included:
            decision_reason = "generator_ready_in_round10"
        elif content_status and content_status != "keep":
            decision_reason = f"slot_content_not_included:{content_status}:{clean_text(slot_audit.get('content_exclusion_reason'))}"
        else:
            decision_reason = f"readiness_not_included:{readiness_value}"
        output.append(
            {
                "recipe_id": recipe_id,
                "display_name": clean_text(readiness.get("display_name")),
                "recipe_kind": clean_text(readiness.get("recipe_kind_guess")),
                "primary_protein": clean_text(readiness.get("primary_protein")),
                "generator_readiness": readiness_value,
                "materialization_decision": "included" if included else "excluded",
                "decision_reason": decision_reason,
                "cache_status": clean_text(cache.get("cache_status") or readiness.get("cache_status")),
                "energy_kcal_per_serving": clean_text(
                    cache.get("energy_kcal_per_serving") or readiness.get("energy_kcal_per_serving")
                ),
                "protein_g_per_serving": clean_text(
                    cache.get("protein_g_per_serving") or readiness.get("protein_g_per_serving")
                ),
                "carbs_g_per_serving": clean_text(
                    cache.get("carbs_g_per_serving") or readiness.get("carbs_g_per_serving")
                ),
                "fat_g_per_serving": clean_text(
                    cache.get("fat_g_per_serving") or readiness.get("fat_g_per_serving")
                ),
                "mapped_weight_ratio": clean_text(
                    cache.get("mapped_weight_ratio") or readiness.get("mapped_weight_ratio")
                ),
                "macro_relevant_mapped_weight_ratio": clean_text(
                    cache.get("macro_relevant_mapped_weight_ratio")
                    or readiness.get("macro_relevant_mapped_weight_ratio")
                ),
                "servings_basis": clean_text(cache.get("servings_basis") or readiness.get("servings_basis")),
                "servings_adjustment_applied": clean_text(
                    cache.get("servings_adjustment_applied") or readiness.get("servings_adjustment_applied")
                ),
                "content_quality_status": content_status,
                "content_exclusion_reason": clean_text(slot_audit.get("content_exclusion_reason")),
                "allowed_slots_json": clean_text(slot_audit.get("allowed_slots_json")),
                "slot_policy_reason": clean_text(slot_audit.get("slot_policy_reason")),
                "slot_policy_confidence": clean_text(slot_audit.get("slot_policy_confidence")),
            }
        )
    return output


def numeric_column(rows: list[dict[str, object]], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        parsed = parse_float(row.get(column))
        if parsed is not None:
            values.append(parsed)
    return values


def counter_text(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in counter.most_common()]


def write_readme(path: Path, included_count: int, slot_checked: bool) -> None:
    title = "Recipes_DB v1.1 generator-ready slot-checked draft subset" if slot_checked else "Recipes_DB v1.1 generator-ready draft subset"
    heading = "=" * len(title)
    slot_notes = ""
    if slot_checked:
        slot_notes = """
Slot/content policy:
- only content_quality_status=keep rows are included
- review and exclude rows from the slot/content audit are not included
- recipes.csv includes allowed_slots_json for Generator v1 hard slot gating
- pet/animal food and ambiguous appetizer/component rows are excluded from this test subset
"""
    else:
        slot_notes = """
Slot/content policy:
- this original subset does not include hard allowed slot tags
- use the slot_checked subset for semantic Generator v1 smoke tests
"""
    text = f"""{title}
{heading}

This folder is a draft/test materialization only. It is not production current and it does not replace data/recipesdb/current.

Contents:
- recipes.csv
- recipe_ingredients.csv
- recipe_nutrition_cache.csv

Included scope:
- only recipes classified as generator_ready in round10
- included recipes: {included_count}
- usable_but_review, partial_keep_for_future, replace_candidate, component_only, and excluded_for_now rows are not included unless they were marked generator_ready
{slot_notes}

Important caveats:
- Full Recipes_DB v1.1 is not materialized yet.
- Mapping/Food_DB/unit-rule passes are stopped for now.
- Time fields are left blank because the round10 v1.1 source does not provide reliable prep/cook/total time.
- servings_declared is left blank; servings_normalized comes from round10 nutrition cache servings_basis.
- recipe_cuisine and difficulty_level are left blank because they are not available safely in the round10 source.

Intended use:
- Generator v1 CLI/Streamlit testing with explicit path arguments.
- Generator v1 can load this draft through an explicit dataset_profile.
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_summary(
    recipes_rows: list[dict[str, object]],
    ingredient_rows: list[dict[str, object]],
    nutrition_rows: list[dict[str, object]],
    readiness_rows: list[dict[str, str]],
    slot_audit_rows: list[dict[str, str]] | None = None,
    slot_checked: bool = False,
) -> str:
    included_ids = {clean_text(row.get("recipe_id")) for row in recipes_rows}
    excluded_readiness = Counter(
        clean_text(row.get("generator_readiness"))
        for row in readiness_rows
        if clean_text(row.get("recipe_id_candidate")) not in included_ids
    )
    recipe_kind_counts = Counter(clean_text(row.get("recipe_kind")) for row in recipes_rows)
    cache_status_counts = Counter(clean_text(row.get("cache_status")) for row in nutrition_rows)
    protein_counts = Counter()
    readiness_by_id = {clean_text(row.get("recipe_id_candidate")): row for row in readiness_rows}
    for row in recipes_rows:
        recipe_id = clean_text(row.get("recipe_id"))
        protein_counts[clean_text(readiness_by_id.get(recipe_id, {}).get("primary_protein"))] += 1

    ingredient_status_counts = Counter(clean_text(row.get("mapping_status")) for row in ingredient_rows)
    slot_counts: Counter[str] = Counter()
    content_counts: Counter[str] = Counter()
    if slot_audit_rows is not None:
        content_counts = Counter(clean_text(row.get("content_quality_status")) for row in slot_audit_rows)
    for row in recipes_rows:
        allowed_text = clean_text(row.get("allowed_slots_json"))
        if not allowed_text:
            continue
        try:
            allowed_slots = json.loads(allowed_text)
        except json.JSONDecodeError:
            allowed_slots = []
        if isinstance(allowed_slots, list):
            for slot in allowed_slots:
                slot_counts[str(slot)] += 1

    title = (
        "Recipes_DB v1.1 generator-ready slot-checked draft materialization summary"
        if slot_checked
        else "Recipes_DB v1.1 generator-ready draft materialization summary"
    )
    lines = [
        title,
        "=" * len(title),
        "",
        f"included_recipe_count: {len(recipes_rows)}",
        f"recipe_ingredients_count: {len(ingredient_rows)}",
        f"recipe_nutrition_cache_count: {len(nutrition_rows)}",
        "",
        "Included readiness classes:",
        "- generator_ready: " + str(len(recipes_rows)),
        "",
        "Excluded counts by readiness class:",
    ]
    lines.extend(counter_text(excluded_readiness))
    lines.extend(["", "Counts by recipe_kind:"])
    lines.extend(counter_text(recipe_kind_counts))
    lines.extend(["", "Counts by primary_protein:"])
    lines.extend(counter_text(protein_counts))
    lines.extend(["", "Ingredient mapping status counts:"])
    lines.extend(counter_text(ingredient_status_counts))
    lines.extend(["", "Nutrition cache status counts:"])
    lines.extend(counter_text(cache_status_counts))
    if slot_checked:
        lines.extend(["", "Content quality counts from slot audit:"])
        lines.extend(counter_text(content_counts))
        lines.extend(["", "Allowed slot counts in included subset:"])
        lines.extend(counter_text(slot_counts))
    lines.extend(
        [
            "",
            "Median per-serving macros:",
            f"- energy_kcal_per_serving: {format_number(median(numeric_column(nutrition_rows, 'energy_kcal_per_serving')))}",
            f"- protein_g_per_serving: {format_number(median(numeric_column(nutrition_rows, 'protein_g_per_serving')))}",
            f"- carbs_g_per_serving: {format_number(median(numeric_column(nutrition_rows, 'carbs_g_per_serving')))}",
            f"- fat_g_per_serving: {format_number(median(numeric_column(nutrition_rows, 'fat_g_per_serving')))}",
            "",
            "Generator v1 path compatibility:",
            "- CLI path arguments are supported: yes",
            "- direct eligibility compatibility: yes, with the matching explicit dataset_profile",
            "- allowed_slots_json is included and should be enforced as a hard slot gate",
            "",
            "Next recommended test command:",
            (
                "python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_1_generator_ready_slot_checked"
                if slot_checked
                else "python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_1_generator_ready"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    recipe_rows, _ = read_csv(CURATED_RECIPES)
    parsed_rows, _ = read_csv(ROUND9_PARSED_INGREDIENTS)
    mapping_rows, _ = read_csv(ROUND9_MAPPING)
    cache_rows, _ = read_csv(ROUND10_CACHE)
    readiness_rows, _ = read_csv(ROUND10_READINESS)
    if not ROUND10_READINESS_SUMMARY.exists():
        raise FileNotFoundError(f"Lipseste fisierul asteptat: {ROUND10_READINESS_SUMMARY}")
    slot_audit_rows: list[dict[str, str]] | None = None
    if args.slot_checked:
        slot_audit_rows, _ = read_csv(SLOT_CONTENT_AUDIT)

    included_ids = included_recipe_ids(readiness_rows, slot_audit_rows)
    cache_by_id = index_by_recipe_id(cache_rows)
    readiness_by_id = index_by_recipe_id(readiness_rows)
    slot_audit_by_id = index_by_recipe_id(slot_audit_rows or [])

    recipes_out = build_recipes_rows(
        recipe_rows,
        cache_by_id,
        readiness_by_id,
        included_ids,
        slot_audit_by_id=slot_audit_by_id if args.slot_checked else None,
        source_dataset=SLOT_CHECKED_SOURCE_DATASET if args.slot_checked else SOURCE_DATASET,
        scope_status=SLOT_CHECKED_SCOPE_STATUS if args.slot_checked else SCOPE_STATUS,
        qc_note=SLOT_CHECKED_QC_NOTE if args.slot_checked else QC_NOTE,
    )
    ingredients_out = build_ingredient_rows(parsed_rows, mapping_rows, included_ids)
    nutrition_out = build_nutrition_rows(
        cache_rows,
        included_ids,
        nutrition_basis=SLOT_CHECKED_NUTRITION_BASIS if args.slot_checked else NUTRITION_BASIS,
        cache_version=SLOT_CHECKED_CACHE_VERSION if args.slot_checked else CACHE_VERSION,
        qc_note=(
            "round10_cache_values; v1_1_generator_ready_slot_checked_draft_only"
            if args.slot_checked
            else "round10_cache_values; v1_1_generator_ready_draft_only"
        ),
    )
    audit_rows = build_audit_rows(
        readiness_rows,
        cache_by_id,
        included_ids,
        slot_audit_by_id=slot_audit_by_id if args.slot_checked else None,
    )

    recipes_path = SLOT_CHECKED_OUT_RECIPES if args.slot_checked else OUT_RECIPES
    ingredients_path = SLOT_CHECKED_OUT_INGREDIENTS if args.slot_checked else OUT_INGREDIENTS
    nutrition_path = SLOT_CHECKED_OUT_NUTRITION if args.slot_checked else OUT_NUTRITION
    readme_path = SLOT_CHECKED_OUT_README if args.slot_checked else OUT_README
    summary_path = SLOT_CHECKED_OUT_SUMMARY if args.slot_checked else OUT_SUMMARY
    audit_path = SLOT_CHECKED_OUT_AUDIT if args.slot_checked else OUT_AUDIT
    recipes_columns = SLOT_CHECKED_RECIPE_COLUMNS if args.slot_checked else RECIPES_COLUMNS
    audit_columns = SLOT_CHECKED_AUDIT_COLUMNS if args.slot_checked else AUDIT_COLUMNS

    write_csv(recipes_path, recipes_out, recipes_columns)
    write_csv(ingredients_path, ingredients_out, RECIPE_INGREDIENT_COLUMNS)
    write_csv(nutrition_path, nutrition_out, NUTRITION_COLUMNS)
    write_csv(audit_path, audit_rows, audit_columns)
    write_readme(readme_path, len(recipes_out), slot_checked=args.slot_checked)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        build_summary(
            recipes_out,
            ingredients_out,
            nutrition_out,
            readiness_rows,
            slot_audit_rows=slot_audit_rows,
            slot_checked=args.slot_checked,
        ),
        encoding="utf-8",
    )

    label = "slot-checked " if args.slot_checked else ""
    print(f"Recipes_DB v1.1 generator-ready {label}draft subset materialized")
    print(f"included_recipe_count={len(recipes_out)}")
    print(f"recipe_ingredients_count={len(ingredients_out)}")
    print(f"recipe_nutrition_cache_count={len(nutrition_out)}")
    print(f"written_recipes={recipes_path}")
    print(f"written_ingredients={ingredients_path}")
    print(f"written_nutrition={nutrition_path}")
    print(f"written_summary={summary_path}")


if __name__ == "__main__":
    main()
