from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra import build_recipes_v1_2_round28_plus30_nutrition_cache as base


RECIPES = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round37_targeted_plus100.csv"
PARSED = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round37_plus100_ingredients_parsed.csv"
FOODDB = REPO_ROOT / "data/fooddb/draft/fooddb_v1_1_core_master_draft_round9.csv"
BASE_DATASET_DIR = REPO_ROOT / "data/recipesdb/draft/v1_2_generator_ready_plus30_plus15_repaired"
EXPANDED_DATASET_DIR = REPO_ROOT / "data/recipesdb/draft/v1_2_generator_ready_round37_expanded"
MANUAL_REPAIR_QUEUE = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_manual_repair_queue.csv"

OUT_UNIT_RULES = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round37_plus100_ingredients_unit_rules.csv"
OUT_MATCHES = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round37_plus100_food_matches.csv"
OUT_UNMAPPED = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round37_plus100_unmapped.csv"
OUT_MAPPING_SUMMARY = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_plus100_mapping_summary.txt"
OUT_MAPPING_REVIEW = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_plus100_mapping_review.csv"
OUT_MAPPING_QUALITY_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_plus100_mapping_quality_audit.csv"
OUT_CACHE = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round37_plus100_nutrition_cache.csv"
OUT_NUTRITION_SUMMARY = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_plus100_nutrition_summary.txt"
OUT_NUTRITION_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_plus100_nutrition_audit.csv"
OUT_GENERATOR_READY_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_plus100_generator_ready_audit.csv"
OUT_QUEUE_ADDITIONS = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_manual_repair_queue_additions.csv"
OUT_QUEUE_ADDITIONS_SUMMARY = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_manual_repair_queue_additions_summary.txt"
OUT_MATERIALIZATION_SUMMARY = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_materialization_summary.txt"
OUT_MATERIALIZATION_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round37_materialization_audit.csv"

ROUND37_TAG = "round37_targeted_expansion"
ROUND37_SCOPE_STATUS = "v1_2_generator_ready_draft"
ROUND37_NUTRITION_BASIS = "recipes_v1_2_round37_plus100_mapped_ingredients_draft"
ROUND37_CACHE_VERSION = "recipes_v1_2_round37_plus100_001"

MAPPING_COLUMNS = base.MAPPING_COLUMNS
CACHE_COLUMNS = base.ROUND28_CACHE_COLUMNS + [
    "strong_generator_ready",
    "strong_generator_ready_reason",
]
NUTRITION_AUDIT_COLUMNS = CACHE_COLUMNS + [
    "selected_for_round37_dataset",
]
MAPPING_QUALITY_COLUMNS = base.MAPPING_QUALITY_COLUMNS
MATERIALIZATION_AUDIT_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "target_bucket",
    "generator_ready_candidate",
    "strong_generator_ready",
    "materialized",
    "generator_ready_failure_reason",
]

ROUND37_ALIAS_FOOD_IDS = {
    key: value
    for key, value in base.ROUND28_ALIAS_FOOD_IDS.items()
    if key not in {"beef", "pork", "turkey"}
}
ROUND37_ALIAS_FOOD_IDS.update(
    {
        "ham": "food_cooked_ham_choice",
        "cooked ham": "food_cooked_ham_choice",
        "skim milk": "food_milk_skimmed_pasteurised",
        "smoked salmon": "food_salmon_smoked",
        "coconut oil": "food_coconut_fat_or_oil",
        "white pepper": "food_white_pepper_powder",
        "butternut squash": "food_squash_butternut_pulp_raw",
        "soy sauce": "food_soy_sauce_prepacked",
        "reduced sodium soy sauce": "food_soy_sauce_prepacked",
        "hard boiled eggs": "food_egg_hard_boiled",
        "hard-boiled eggs": "food_egg_hard_boiled",
        "whole wheat flour": "food_wheat_flour_wholemeal",
    }
)

ROUND37_CONTAINS_ALIAS_FOOD_IDS = [
    item
    for item in base.CONTAINS_ALIAS_FOOD_IDS
    if item[0] not in {"beef", "pork", "turkey"}
]
ROUND37_CONTAINS_ALIAS_FOOD_IDS.extend(
    [
        ("smoked salmon", "food_salmon_smoked"),
        ("skim milk", "food_milk_skimmed_pasteurised"),
        ("coconut oil", "food_coconut_fat_or_oil"),
        ("whole wheat flour", "food_wheat_flour_wholemeal"),
    ]
)


def main() -> None:
    run_full_pipeline()


def run_unit_rules() -> list[dict[str, object]]:
    parsed_rows = read_csv(PARSED)
    unit_rows = base.apply_round28_unit_rules(parsed_rows)
    write_csv(OUT_UNIT_RULES, unit_rows, list(unit_rows[0].keys()) if unit_rows else [])
    print(f"Round37 unit rules written: {OUT_UNIT_RULES}")
    return unit_rows


def run_mapping() -> tuple[list[dict[str, str]], list[dict[str, object]], list[dict[str, object]], dict[str, dict[str, Any]]]:
    recipes = read_csv(RECIPES)
    unit_rows = run_unit_rules()
    food_lookup = base.build_food_lookup(read_csv(FOODDB))
    mapping_rows, unmapped_rows = build_mapping_rows(unit_rows, food_lookup)
    write_csv(OUT_MATCHES, mapping_rows, MAPPING_COLUMNS)
    write_csv(OUT_UNMAPPED, unmapped_rows, MAPPING_COLUMNS)
    write_csv(OUT_MAPPING_REVIEW, [row for row in mapping_rows if row["mapping_status"] != "accepted_auto"], MAPPING_COLUMNS)
    write_csv(OUT_MAPPING_QUALITY_AUDIT, build_mapping_quality_rows(recipes, mapping_rows), MAPPING_QUALITY_COLUMNS)
    OUT_MAPPING_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_MAPPING_SUMMARY.write_text(build_mapping_summary(mapping_rows), encoding="utf-8")
    print(f"Round37 mapping written: {OUT_MATCHES}")
    return recipes, mapping_rows, unmapped_rows, food_lookup


def run_full_pipeline() -> None:
    recipes, mapping_rows, _unmapped_rows, food_lookup = run_mapping()
    cache_rows, audit_rows = build_cache_rows(recipes, mapping_rows, food_lookup)
    ready_ids = {
        row["recipe_id_candidate"]
        for row in cache_rows
        if row["generator_ready_candidate"] == "true"
    }
    strong_ids = {
        row["recipe_id_candidate"]
        for row in cache_rows
        if row["strong_generator_ready"] == "true"
    }
    materialize_expanded_dataset(recipes, mapping_rows, cache_rows, ready_ids)
    queue_additions = update_manual_repair_queue(recipes, mapping_rows, cache_rows, ready_ids)

    write_csv(OUT_CACHE, cache_rows, CACHE_COLUMNS)
    write_csv(OUT_NUTRITION_AUDIT, audit_rows, NUTRITION_AUDIT_COLUMNS)
    write_csv(OUT_GENERATOR_READY_AUDIT, audit_rows, NUTRITION_AUDIT_COLUMNS)
    write_csv(OUT_MATERIALIZATION_AUDIT, build_materialization_audit_rows(recipes, cache_rows, ready_ids), MATERIALIZATION_AUDIT_COLUMNS)
    write_csv(OUT_QUEUE_ADDITIONS, queue_additions, manual_repair_columns())
    OUT_NUTRITION_SUMMARY.write_text(build_nutrition_summary(cache_rows), encoding="utf-8")
    OUT_QUEUE_ADDITIONS_SUMMARY.write_text(build_queue_additions_summary(queue_additions), encoding="utf-8")
    OUT_MATERIALIZATION_SUMMARY.write_text(
        build_materialization_summary(recipes, ready_ids, strong_ids),
        encoding="utf-8",
    )

    print("Round37 +100 unit rules, mapping, nutrition cache and materialization written")
    print(f"selected_recipes={len(recipes)}")
    print(f"generator_ready_count={len(ready_ids)}")
    print(f"strong_generator_ready_count={len(strong_ids)}")
    print(f"manual_repair_queue_additions={len(queue_additions)}")
    print(f"expanded_dataset={EXPANDED_DATASET_DIR}")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_mapping_rows(
    rows: list[dict[str, object]],
    food_lookup: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    mapped = []
    unmapped = []
    for row in rows:
        mapping = map_ingredient(row, food_lookup)
        output = {
            "recipe_id_candidate": row.get("recipe_id_candidate"),
            "source_index": row.get("source_index"),
            "display_name": row.get("display_name"),
            "ingredient_position": row.get("ingredient_position"),
            "ingredient_raw_text": row.get("ingredient_raw_text"),
            "ingredient_name_parsed": row.get("ingredient_name_parsed"),
            "ingredient_name_normalized": row.get("ingredient_name_normalized"),
            "quantity_value": row.get("quantity_value"),
            "quantity_unit": row.get("quantity_unit"),
            "quantity_grams_estimated": row.get("quantity_grams_estimated"),
            "parse_status": row.get("parse_status"),
            "fooddb_version_used": "fooddb_v1_1_draft_round9",
            "mapped_food_id": mapping["food_id"],
            "mapped_food_canonical_name": mapping["canonical_name"],
            "mapping_status": mapping["status"],
            "mapping_confidence": mapping["confidence"],
            "mapping_method": mapping["method"],
            "mapping_notes": mapping["notes"],
            "edible_yield_factor": "1",
            "uses_pilot_edible_yield": "False",
            "edible_yield_reason": "",
            "manual_decision_notes": mapping["manual_notes"],
        }
        mapped.append(output)
        if output["mapping_status"] != "accepted_auto":
            unmapped.append(output)
    return mapped, unmapped


def map_ingredient(row: dict[str, object], food_lookup: dict[str, dict[str, Any]]) -> dict[str, str]:
    parse_status = clean_text(row.get("parse_status"))
    name = normalize_text(row.get("ingredient_name_normalized"))
    raw = normalize_text(row.get("ingredient_raw_text"))
    if parse_status == "failed_parse":
        return mapping_result("", "", "unmapped", "low", "parse_failed", "round37_parse_failed")
    if name in {"beef", "pork", "turkey"}:
        return mapping_result("", "", "review_needed", "low", "round37_generic_meat_blocked", "generic_meat_not_auto_mapped")
    food_id = food_id_for_name(name, raw)
    if food_id and food_id in food_lookup:
        return mapping_result(
            food_id,
            food_lookup[food_id]["canonical_name"],
            "accepted_auto",
            "high",
            "round37_existing_fooddb_alias",
            "round37_targeted_existing_fooddb_item",
        )
    if food_id and food_id not in food_lookup:
        return mapping_result(
            food_id,
            "",
            "review_needed",
            "low",
            "round37_alias_missing_fooddb_item",
            "target_food_id_not_found",
        )
    if parse_status == "review_needed":
        return mapping_result("", "", "review_needed", "low", "parse_review_needed", "")
    return mapping_result("", "", "unmapped", "low", "no_round37_safe_alias", "")


def food_id_for_name(name: str, raw: str) -> str:
    if "cooked rice" in raw:
        return "food_rice_cooked_unsalted"
    if "cooked pasta" in raw:
        return "food_dried_pasta_cooked_unsalted"
    if "tomato sauce" in name or "tomato sauce" in raw or "spaghetti sauce" in raw:
        return "food_basque_style_sauce_or_tomato_sauce_with_sweet_peppers_prepacked"
    for term, food_id in ROUND37_CONTAINS_ALIAS_FOOD_IDS:
        if term in name or term in raw:
            return food_id
    return ROUND37_ALIAS_FOOD_IDS.get(name, "")


def mapping_result(
    food_id: str,
    canonical_name: str,
    status: str,
    confidence: str,
    method: str,
    notes: str,
) -> dict[str, str]:
    return {
        "food_id": food_id,
        "canonical_name": canonical_name,
        "status": status,
        "confidence": confidence,
        "method": method,
        "notes": notes,
        "manual_notes": "round37_no_fooddb_rows_added",
    }


def build_cache_rows(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, object]],
    food_lookup: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    by_recipe: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in mapping_rows:
        by_recipe[clean_text(row.get("recipe_id_candidate"))].append(row)

    cache_rows = []
    audit_rows = []
    for recipe in recipes:
        recipe_id = clean_text(recipe.get("recipe_id_candidate"))
        rows = by_recipe.get(recipe_id, [])
        totals = base.nutrition_totals(rows, food_lookup)
        coverage = base.coverage_totals(rows)
        servings = base.estimate_servings(recipe, coverage["known_weight_grams_sum"])
        energy_per = totals["energy"] / servings if servings else 0.0
        protein_per = totals["protein"] / servings if servings else 0.0
        carbs_per = totals["carbs"] / servings if servings else 0.0
        fat_per = totals["fat"] / servings if servings else 0.0
        mapped_ratio = base.safe_ratio(totals["mapped_weight"], coverage["known_weight_grams_sum"])
        macro_ratio = base.safe_ratio(totals["macro_relevant_mapped_weight"], coverage["known_weight_grams_sum"])
        cache_status = cache_status_for_recipe(coverage, macro_ratio, energy_per)
        ready, failure_reasons = generator_ready_status(
            target_bucket=recipe.get("target_bucket", ""),
            coverage=coverage,
            mapped_ratio=mapped_ratio,
            macro_ratio=macro_ratio,
            energy_per=energy_per,
            protein_per=protein_per,
            carbs_per=carbs_per,
            cache_status=cache_status,
        )
        strong_ready, strong_reason = strong_generator_ready_status(
            target_bucket=recipe.get("target_bucket", ""),
            macro_ratio=macro_ratio,
            energy_per=energy_per,
            protein_per=protein_per,
            carbs_per=carbs_per,
            ready=ready,
        )
        quality_flags = quality_flags_for_recipe(
            target_bucket=recipe.get("target_bucket", ""),
            mapped_ratio=mapped_ratio,
            macro_ratio=macro_ratio,
            energy_per=energy_per,
            protein_per=protein_per,
            carbs_per=carbs_per,
            coverage=coverage,
        )
        row = {
            "recipe_id_candidate": recipe_id,
            "display_name": recipe.get("display_name"),
            "recipe_kind_guess": recipe.get("recipe_kind_guess"),
            "target_bucket": recipe.get("target_bucket"),
            "primary_protein": recipe.get("primary_protein"),
            "nutrition_basis": ROUND37_NUTRITION_BASIS,
            "servings_basis": format_number(servings),
            "uses_pilot_servings_fallback": "True",
            "servings_estimation_method": "round37_bucket_default",
            "servings_estimation_reasons": f"{recipe.get('target_bucket', '')}_default_servings",
            "total_weight_grams_estimated": format_number(coverage["known_weight_grams_sum"]),
            "mapped_weight_grams": format_number(totals["mapped_weight"]),
            "macro_relevant_mapped_weight_grams": format_number(totals["macro_relevant_mapped_weight"]),
            "low_or_no_macro_mapped_weight_grams": format_number(totals["low_or_no_macro_mapped_weight"]),
            "known_weight_grams_sum": format_number(coverage["known_weight_grams_sum"]),
            "mapped_weight_ratio": format_number(mapped_ratio),
            "macro_relevant_mapped_weight_ratio": format_number(macro_ratio),
            "energy_kcal_total": format_number(totals["energy"]),
            "protein_g_total": format_number(totals["protein"]),
            "carbs_g_total": format_number(totals["carbs"]),
            "fat_g_total": format_number(totals["fat"]),
            "energy_kcal_per_serving": format_number(energy_per),
            "protein_g_per_serving": format_number(protein_per),
            "carbs_g_per_serving": format_number(carbs_per),
            "fat_g_per_serving": format_number(fat_per),
            "ingredient_count": str(int(coverage["ingredient_count"])),
            "accepted_auto_count": str(int(coverage["accepted_auto_count"])),
            "accepted_auto_with_grams_count": str(int(coverage["accepted_auto_with_grams_count"])),
            "review_needed_count": str(int(coverage["review_needed_count"])),
            "review_needed_with_grams_count": str(int(coverage["review_needed_with_grams_count"])),
            "unmapped_count": str(int(coverage["unmapped_count"])),
            "unmapped_with_grams_count": str(int(coverage["unmapped_with_grams_count"])),
            "cache_status": cache_status,
            "quality_flags": ";".join(quality_flags),
            "generator_ready_candidate": str(ready).lower(),
            "generator_ready_failure_reason": ";".join(failure_reasons),
            "cache_version": ROUND37_CACHE_VERSION,
            "strong_generator_ready": str(strong_ready).lower(),
            "strong_generator_ready_reason": strong_reason,
        }
        cache_rows.append(row)
        audit_row = dict(row)
        audit_row["selected_for_round37_dataset"] = str(ready).lower()
        audit_rows.append(audit_row)
    return cache_rows, audit_rows


def cache_status_for_recipe(coverage: dict[str, float], macro_ratio: float, energy_per: float) -> str:
    if coverage["accepted_auto_count"] <= 0:
        return "no_accepted_mapped_ingredients"
    if coverage["accepted_auto_with_grams_count"] <= 0:
        return "mapped_without_weight_estimates"
    if coverage["accepted_auto_with_grams_count"] >= 3 and macro_ratio >= 0.45 and energy_per >= 150:
        return "usable_from_mapped_ingredients"
    return "partial_from_mapped_ingredients"


def generator_ready_status(
    *,
    target_bucket: object,
    coverage: dict[str, float],
    mapped_ratio: float,
    macro_ratio: float,
    energy_per: float,
    protein_per: float,
    carbs_per: float,
    cache_status: str,
) -> tuple[bool, list[str]]:
    reasons = []
    bucket = clean_text(target_bucket)
    if cache_status not in {"usable_from_mapped_ingredients", "partial_from_mapped_ingredients"}:
        reasons.append(f"cache_status={cache_status}")
    if coverage["accepted_auto_with_grams_count"] < 3:
        reasons.append("accepted_auto_with_grams_lt_3")
    if mapped_ratio < 0.45:
        reasons.append("mapped_weight_ratio_lt_0.45")
    macro_min = 0.45 if bucket == "breakfast_competitor" else 0.50
    if macro_ratio < macro_min:
        reasons.append(f"macro_relevant_mapped_weight_ratio_lt_{macro_min:.2f}")
    if bucket == "breakfast_competitor":
        if energy_per < 200:
            reasons.append("breakfast_kcal_per_serving_lt_200")
        if energy_per > 800:
            reasons.append("breakfast_kcal_per_serving_gt_800")
        if protein_per < 8:
            reasons.append("breakfast_protein_per_serving_lt_8")
        if carbs_per < 25:
            reasons.append("breakfast_carbs_per_serving_lt_25")
    else:
        if energy_per < 350:
            reasons.append("main_kcal_per_serving_lt_350")
        if protein_per < 14:
            reasons.append("main_protein_per_serving_lt_14")
        if carbs_per < 25:
            reasons.append("main_carbs_per_serving_lt_25")
    return not reasons, reasons


def strong_generator_ready_status(
    *,
    target_bucket: object,
    macro_ratio: float,
    energy_per: float,
    protein_per: float,
    carbs_per: float,
    ready: bool,
) -> tuple[bool, str]:
    if not ready:
        return False, "not_generator_ready"
    bucket = clean_text(target_bucket)
    reasons = []
    if macro_ratio < 0.60:
        reasons.append("macro_relevant_mapped_weight_ratio_lt_0.60")
    if bucket == "breakfast_competitor":
        if energy_per < 250:
            reasons.append("breakfast_kcal_lt_250")
        if protein_per < 10:
            reasons.append("breakfast_protein_lt_10")
        if carbs_per < 35:
            reasons.append("breakfast_carbs_lt_35")
    else:
        if energy_per < 450:
            reasons.append("main_kcal_lt_450")
        if protein_per < 18:
            reasons.append("main_protein_lt_18")
        if carbs_per < 35:
            reasons.append("main_carbs_lt_35")
    return not reasons, ";".join(reasons) if reasons else "strong"


def quality_flags_for_recipe(
    *,
    target_bucket: object,
    mapped_ratio: float,
    macro_ratio: float,
    energy_per: float,
    protein_per: float,
    carbs_per: float,
    coverage: dict[str, float],
) -> list[str]:
    flags = []
    bucket = clean_text(target_bucket)
    if mapped_ratio < 0.45:
        flags.append("low_mapped_weight_ratio")
    if macro_ratio < (0.45 if bucket == "breakfast_competitor" else 0.50):
        flags.append("low_macro_relevant_mapped_weight_ratio")
    if bucket == "breakfast_competitor":
        if energy_per < 200:
            flags.append("low_breakfast_kcal")
        if energy_per > 800:
            flags.append("high_breakfast_kcal")
        if protein_per < 8:
            flags.append("low_breakfast_protein")
        if carbs_per < 25:
            flags.append("low_breakfast_carbs")
    else:
        if energy_per < 350:
            flags.append("low_main_kcal")
        if protein_per < 14:
            flags.append("low_main_protein")
        if carbs_per < 25:
            flags.append("low_main_carbs")
    if coverage["review_needed_with_grams_count"] + coverage["unmapped_with_grams_count"] >= 3:
        flags.append("mapping_review_blockers")
    return flags


def build_mapping_quality_rows(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows = base.build_mapping_quality_rows(recipes, mapping_rows)
    for row in rows:
        row["mapping_quality_flags"] = str(row.get("mapping_quality_flags", "")).replace(
            "round28", "round37"
        )
    return rows


def materialize_expanded_dataset(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, object]],
    cache_rows: list[dict[str, object]],
    ready_ids: set[str],
) -> None:
    EXPANDED_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    base_recipes = read_csv(BASE_DATASET_DIR / "recipes.csv")
    base_ingredients = read_csv(BASE_DATASET_DIR / "recipe_ingredients.csv")
    base_cache = read_csv(BASE_DATASET_DIR / "recipe_nutrition_cache.csv")
    recipe_by_id = {row["recipe_id_candidate"]: row for row in recipes}
    cache_by_id = {row["recipe_id_candidate"]: row for row in cache_rows}

    new_recipe_rows = [
        materialized_recipe_row(recipe_by_id[recipe_id], cache_by_id[recipe_id])
        for recipe_id in sorted(ready_ids)
    ]
    new_ingredient_rows = [
        materialized_ingredient_row(row)
        for row in mapping_rows
        if clean_text(row.get("recipe_id_candidate")) in ready_ids
    ]
    new_cache_rows = [
        materialized_cache_row(cache_by_id[recipe_id])
        for recipe_id in sorted(ready_ids)
    ]

    write_csv(EXPANDED_DATASET_DIR / "recipes.csv", base_recipes + new_recipe_rows, list(base_recipes[0].keys()))
    write_csv(
        EXPANDED_DATASET_DIR / "recipe_ingredients.csv",
        base_ingredients + new_ingredient_rows,
        list(base_ingredients[0].keys()),
    )
    write_csv(
        EXPANDED_DATASET_DIR / "recipe_nutrition_cache.csv",
        base_cache + new_cache_rows,
        list(base_cache[0].keys()),
    )
    readme_text = "\n".join(
        [
            "Recipes_DB v1.2 generator-ready Round37 expanded draft",
            "",
            "Draft/test only. Do not treat as current production data.",
            f"Base dataset: {BASE_DATASET_DIR}",
            f"Round37 ready additions: {len(ready_ids)}",
            f"QC tag: {ROUND37_TAG}",
            "",
        ]
    )
    (EXPANDED_DATASET_DIR / "README_v1_2_generator_ready_round37_expanded.txt").write_text(
        readme_text,
        encoding="utf-8",
    )


def materialized_recipe_row(recipe: dict[str, str], cache: dict[str, object]) -> dict[str, object]:
    source_index = clean_text(recipe.get("source_index"))
    directions = json.loads(recipe.get("directions_json") or "[]")
    active_time = base.estimate_active_time(recipe)
    target_bucket = clean_text(recipe.get("target_bucket"))
    allowed_slots = ["breakfast"] if target_bucket == "breakfast_competitor" else ["lunch", "dinner"]
    return {
        "recipe_id": recipe["recipe_id_candidate"],
        "source_recipe_id": source_index,
        "source_dataset": "recipes_dataset_64k_dishes_round37_plus100",
        "recipe_name": recipe["display_name"],
        "display_name": recipe["display_name"],
        "recipe_family_name": recipe["display_name"],
        "recipe_kind": recipe["recipe_kind_guess"],
        "recipe_category": recipe["source_category"],
        "recipe_subcategory": recipe["source_subcategory"],
        "recipe_cuisine": "",
        "directions_json": recipe["directions_json"],
        "directions_step_count": len(directions),
        "servings_declared": "",
        "servings_normalized": cache["servings_basis"],
        "prep_time_min": 15,
        "cook_time_min": max(20, active_time - 15),
        "total_time_min": active_time,
        "difficulty_level": "",
        "scope_status": ROUND37_SCOPE_STATUS,
        "has_ingredients_parsed": 1,
        "has_nutrition_cache": 1,
        "is_pilot_recipe": 0,
        "is_active": 1,
        "qc_recipe_status": "generator_ready_draft",
        "qc_notes": f"{ROUND37_TAG}; draft_only; no_fooddb_rows_added; strong={cache.get('strong_generator_ready')}",
        "allowed_slots_json": json.dumps(allowed_slots),
        "slot_policy_reason": f"round37_targeted_{target_bucket}",
        "content_quality_status": "keep",
        "content_exclusion_reason": "",
        "active_time_estimated_min": active_time,
        "passive_time_estimated_min": 0,
        "effective_time_min_for_scoring": active_time,
        "has_long_passive_time": "False",
        "time_estimation_confidence": "medium",
        "time_estimation_method": "round37_direction_step_estimate",
        "time_estimation_reasons": "draft_plus100_direction_count_estimate",
    }


def materialized_ingredient_row(row: dict[str, object]) -> dict[str, object]:
    output = base.materialized_ingredient_row(row)
    output["qc_ingredient_status"] = (
        "accepted_auto_round37"
        if row.get("mapping_status") == "accepted_auto"
        else "needs_review_round37"
    )
    output["qc_notes"] = ROUND37_TAG
    return output


def materialized_cache_row(row: dict[str, object]) -> dict[str, object]:
    output = base.materialized_cache_row(row)
    output["nutrition_basis"] = ROUND37_NUTRITION_BASIS
    output["cache_version"] = ROUND37_CACHE_VERSION
    output["qc_notes"] = f"{ROUND37_TAG}; {row['generator_ready_failure_reason']}; strong={row.get('strong_generator_ready')}"
    return output


def update_manual_repair_queue(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, object]],
    cache_rows: list[dict[str, object]],
    ready_ids: set[str],
) -> list[dict[str, object]]:
    existing_rows = read_csv(MANUAL_REPAIR_QUEUE) if MANUAL_REPAIR_QUEUE.exists() else []
    columns = manual_repair_columns(existing_rows)
    existing_recipe_ids = {row.get("recipe_id_candidate", "") for row in existing_rows}
    existing_round37_rows = [
        row for row in existing_rows
        if row.get("created_from_round") == "round37"
        or str(row.get("recipe_id_candidate", "")).startswith("recipes_v1_2_round37_plus100_")
    ]
    cache_by_id = {row["recipe_id_candidate"]: row for row in cache_rows}
    mapping_by_recipe: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in mapping_rows:
        mapping_by_recipe[clean_text(row.get("recipe_id_candidate"))].append(row)

    additions = []
    for recipe in recipes:
        recipe_id = recipe["recipe_id_candidate"]
        if recipe_id in ready_ids or recipe_id in existing_recipe_ids:
            continue
        cache = cache_by_id.get(recipe_id, {})
        if not is_valuable_repair_candidate(recipe, cache):
            continue
        blocker = choose_blocking_ingredient(mapping_by_recipe.get(recipe_id, []), cache)
        additions.append(manual_repair_row(len(existing_rows) + len(additions) + 1, recipe, cache, blocker))

    if additions:
        write_csv(MANUAL_REPAIR_QUEUE, existing_rows + additions, columns)
    return existing_round37_rows + additions


def manual_repair_columns(existing_rows: list[dict[str, str]] | None = None) -> list[str]:
    if existing_rows:
        return list(existing_rows[0].keys())
    return [
        "repair_id",
        "recipe_id_candidate",
        "source_index",
        "display_name",
        "recipe_source",
        "recipe_kind_guess",
        "target_slot",
        "target_bucket",
        "priority",
        "problem_type",
        "blocking_ingredient",
        "ingredient_raw_text",
        "ingredient_name_normalized",
        "current_mapping_status",
        "current_quantity_grams_estimated",
        "proposed_fix_type",
        "proposed_fix_detail",
        "needs_fooddb_addition",
        "needs_alias",
        "needs_unit_rule",
        "needs_servings_fix",
        "needs_web_source",
        "source_needed",
        "source_name",
        "source_url",
        "decision_status",
        "decision_notes",
        "expected_generator_value",
        "created_from_round",
        "qc_notes",
    ]


def is_valuable_repair_candidate(recipe: dict[str, str], cache: dict[str, object]) -> bool:
    if recipe.get("quality_status") == "reject":
        return False
    if recipe.get("target_bucket") not in {
        "breakfast_competitor",
        "carb_protein_main",
        "fish_turkey_pork_main",
        "vegetarian_legume_balanced",
    }:
        return False
    failure = clean_text(cache.get("generator_ready_failure_reason"))
    if not failure:
        return False
    return any(
        term in failure
        for term in [
            "mapped_weight_ratio",
            "macro_relevant",
            "accepted_auto_with_grams",
            "kcal_per_serving",
            "protein_per_serving",
            "carbs_per_serving",
        ]
    )


def choose_blocking_ingredient(
    rows: list[dict[str, object]],
    cache: dict[str, object],
) -> dict[str, object]:
    with_grams = [
        row for row in rows
        if clean_text(row.get("mapping_status")) != "accepted_auto"
        and base.to_optional_float(row.get("quantity_grams_estimated")) is not None
    ]
    if with_grams:
        return sorted(with_grams, key=lambda row: -base.to_float(row.get("quantity_grams_estimated")))[0]
    without_grams = [
        row for row in rows
        if clean_text(row.get("mapping_status")) != "accepted_auto"
    ]
    if without_grams:
        return without_grams[0]
    return {}


def manual_repair_row(
    sequence: int,
    recipe: dict[str, str],
    cache: dict[str, object],
    blocker: dict[str, object],
) -> dict[str, object]:
    mapping_status = clean_text(blocker.get("mapping_status"))
    grams = clean_text(blocker.get("quantity_grams_estimated"))
    ingredient = clean_text(blocker.get("ingredient_name_normalized") or blocker.get("ingredient_raw_text"))
    proposed_fix_type = "servings_fix"
    problem_type = "nutrition_threshold_or_servings"
    needs_alias = "false"
    needs_unit_rule = "false"
    needs_fooddb_addition = "false"
    needs_servings_fix = "true"
    if blocker:
        if not grams:
            proposed_fix_type = "unit_rule"
            problem_type = "unit_or_mapping_gap"
            needs_unit_rule = "true"
            needs_servings_fix = "false"
        elif mapping_status in {"unmapped", "review_needed"}:
            proposed_fix_type = "alias_mapping"
            problem_type = "mapping_gap"
            needs_alias = "true"
            needs_servings_fix = "false"
            if clean_text(blocker.get("mapping_method")) == "round37_alias_missing_fooddb_item":
                proposed_fix_type = "fooddb_addition"
                needs_fooddb_addition = "true"
    needs_web = "true" if needs_fooddb_addition == "true" else "false"
    return {
        "repair_id": f"repair_v1_2_round37_{sequence:04d}",
        "recipe_id_candidate": recipe.get("recipe_id_candidate", ""),
        "source_index": recipe.get("source_index", ""),
        "display_name": recipe.get("display_name", ""),
        "recipe_source": "recipes_dataset_64k_dishes_round37_plus100",
        "recipe_kind_guess": recipe.get("recipe_kind_guess", ""),
        "target_slot": "breakfast" if recipe.get("target_bucket") == "breakfast_competitor" else "lunch_dinner",
        "target_bucket": recipe.get("target_bucket", ""),
        "priority": priority_for_recipe(recipe),
        "problem_type": problem_type,
        "blocking_ingredient": ingredient,
        "ingredient_raw_text": blocker.get("ingredient_raw_text", ""),
        "ingredient_name_normalized": blocker.get("ingredient_name_normalized", ""),
        "current_mapping_status": mapping_status,
        "current_quantity_grams_estimated": grams,
        "proposed_fix_type": proposed_fix_type,
        "proposed_fix_detail": cache.get("generator_ready_failure_reason", ""),
        "needs_fooddb_addition": needs_fooddb_addition,
        "needs_alias": needs_alias,
        "needs_unit_rule": needs_unit_rule,
        "needs_servings_fix": needs_servings_fix,
        "needs_web_source": needs_web,
        "source_needed": needs_web,
        "source_name": "",
        "source_url": "",
        "decision_status": "pending",
        "decision_notes": "round37_failed_but_potentially_useful",
        "expected_generator_value": recipe.get("expected_generator_value", ""),
        "created_from_round": "round37",
        "qc_notes": "round37_manual_repair_queue_addition",
    }


def priority_for_recipe(recipe: dict[str, str]) -> str:
    bucket = recipe.get("target_bucket")
    if bucket in {"carb_protein_main", "breakfast_competitor"}:
        return "high"
    return "medium"


def build_mapping_summary(rows: list[dict[str, object]]) -> str:
    status_counts = Counter(clean_text(row.get("mapping_status")) for row in rows)
    method_counts = Counter(clean_text(row.get("mapping_method")) for row in rows)
    with_grams = Counter()
    for row in rows:
        if base.to_optional_float(row.get("quantity_grams_estimated")) is not None:
            with_grams[clean_text(row.get("mapping_status"))] += 1
    lines = [
        "Recipes_DB v1.2 Round37 +100 mapping summary",
        "",
        f"input={OUT_UNIT_RULES}",
        f"fooddb={FOODDB}",
        f"rows={len(rows)}",
        "",
        "Mapping status counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in status_counts.most_common())
    lines.extend(["", "Mapping status counts with grams:"])
    lines.extend(f"- {name}: {count}" for name, count in with_grams.most_common())
    lines.extend(["", "Mapping method counts:"])
    lines.extend(f"- {name}: {count}" for name, count in method_counts.most_common())
    lines.extend(
        [
            "",
            "Strict note:",
            "Round37 only maps to existing Food_DB v1.1 round9 draft rows.",
            "Generic beef/pork/turkey are not globally auto-mapped.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_nutrition_summary(rows: list[dict[str, object]]) -> str:
    ready = [row for row in rows if row["generator_ready_candidate"] == "true"]
    strong = [row for row in rows if row["strong_generator_ready"] == "true"]
    failure_counts = Counter()
    for row in rows:
        if row["generator_ready_candidate"] != "true":
            failure_counts.update(
                reason for reason in str(row["generator_ready_failure_reason"]).split(";") if reason
            )
    lines = [
        "Recipes_DB v1.2 Round37 +100 nutrition summary",
        "",
        f"recipes={len(rows)}",
        f"generator_ready_count={len(ready)}",
        f"strong_generator_ready_count={len(strong)}",
        "",
        "Top failure reasons:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in failure_counts.most_common(20))
    lines.extend(["", "Per recipe:"])
    for row in rows:
        lines.append(
            (
                f"- {row['display_name']}: ready={row['generator_ready_candidate']} "
                f"strong={row['strong_generator_ready']} "
                f"kcal={row['energy_kcal_per_serving']} "
                f"P/C/F={row['protein_g_per_serving']}/"
                f"{row['carbs_g_per_serving']}/{row['fat_g_per_serving']} "
                f"macro_ratio={row['macro_relevant_mapped_weight_ratio']} "
                f"failure={row['generator_ready_failure_reason'] or 'none'}"
            )
        )
    return "\n".join(lines) + "\n"


def build_queue_additions_summary(rows: list[dict[str, object]]) -> str:
    problem_counts = Counter(str(row.get("problem_type")) for row in rows)
    fix_counts = Counter(str(row.get("proposed_fix_type")) for row in rows)
    lines = [
        "Recipes_DB v1.2 Round37 manual repair queue additions summary",
        "",
        f"additions={len(rows)}",
        "",
        "Problem types:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in problem_counts.most_common())
    lines.extend(["", "Proposed fix types:"])
    lines.extend(f"- {name}: {count}" for name, count in fix_counts.most_common())
    lines.extend(["", "Added rows:"])
    for row in rows:
        lines.append(
            f"- {row['repair_id']} | {row['display_name']} | {row['problem_type']} | {row['blocking_ingredient']}"
        )
    return "\n".join(lines) + "\n"


def build_materialization_summary(
    recipes: list[dict[str, str]],
    ready_ids: set[str],
    strong_ids: set[str],
) -> str:
    base_count = len(read_csv(BASE_DATASET_DIR / "recipes.csv"))
    lines = [
        "Recipes_DB v1.2 Round37 materialization summary",
        "",
        f"base_dataset={BASE_DATASET_DIR}",
        f"new_dataset={EXPANDED_DATASET_DIR}",
        f"base_recipe_count={base_count}",
        f"selected_plus100_count={len(recipes)}",
        f"generator_ready_round37_count={len(ready_ids)}",
        f"strong_generator_ready_round37_count={len(strong_ids)}",
        f"new_recipe_count={base_count + len(ready_ids)}",
        "",
        "Ready recipes included:",
    ]
    for row in recipes:
        if row["recipe_id_candidate"] in ready_ids:
            strong = "strong" if row["recipe_id_candidate"] in strong_ids else "ready"
            lines.append(f"- {row['recipe_id_candidate']} | {strong} | {row['display_name']}")
    blocked = [row for row in recipes if row["recipe_id_candidate"] not in ready_ids]
    if blocked:
        lines.extend(["", "Selected but not materialized:"])
        for row in blocked:
            lines.append(f"- {row['recipe_id_candidate']} | {row['display_name']}")
    return "\n".join(lines) + "\n"


def build_materialization_audit_rows(
    recipes: list[dict[str, str]],
    cache_rows: list[dict[str, object]],
    ready_ids: set[str],
) -> list[dict[str, object]]:
    cache_by_id = {clean_text(row.get("recipe_id_candidate")): row for row in cache_rows}
    rows = []
    for recipe in recipes:
        recipe_id = clean_text(recipe.get("recipe_id_candidate"))
        cache = cache_by_id.get(recipe_id, {})
        rows.append(
            {
                "recipe_id_candidate": recipe_id,
                "display_name": recipe.get("display_name", ""),
                "target_bucket": recipe.get("target_bucket", ""),
                "generator_ready_candidate": cache.get("generator_ready_candidate", "false"),
                "strong_generator_ready": cache.get("strong_generator_ready", "false"),
                "materialized": str(recipe_id in ready_ids).lower(),
                "generator_ready_failure_reason": cache.get("generator_ready_failure_reason", ""),
            }
        )
    return rows


def clean_text(value: object) -> str:
    return str(value or "").strip()


def normalize_text(value: object) -> str:
    return base.normalize_text(value)


def format_number(value: object) -> str:
    return base.format_number(value)


if __name__ == "__main__":
    main()
