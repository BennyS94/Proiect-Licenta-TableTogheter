from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra import build_recipes_v1_2_round37_plus100_nutrition_cache as round37


ORIGINAL_MANUAL_REPAIR_ROW = round37.manual_repair_row
ORIGINAL_MANUAL_REPAIR_COLUMNS = round37.manual_repair_columns
ORIGINAL_BUILD_MAPPING_SUMMARY = round37.build_mapping_summary
ORIGINAL_BUILD_NUTRITION_SUMMARY = round37.build_nutrition_summary

RECIPES = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round42_dataset_curated_selected.csv"
PARSED = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round42_dataset_ingredients_parsed.csv"
FOODDB = REPO_ROOT / "data/fooddb/draft/fooddb_v1_2_core_master_manual_batch1.csv"
BASE_DATASET_DIR = REPO_ROOT / "data/recipesdb/draft/v1_2_generator_ready_round41_manual_curated"
EXPANDED_DATASET_DIR = REPO_ROOT / "data/recipesdb/draft/v1_2_generator_ready_round42_dataset_expanded"
MANUAL_REPAIR_QUEUE = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_manual_repair_queue.csv"

OUT_UNIT_RULES = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round42_dataset_ingredients_unit_rules.csv"
OUT_MATCHES = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round42_dataset_food_matches.csv"
OUT_UNMAPPED = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round42_dataset_unmapped.csv"
OUT_MAPPING_SUMMARY = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_mapping_summary.txt"
OUT_MAPPING_REVIEW = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_mapping_review.csv"
OUT_MAPPING_QUALITY_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_mapping_quality_audit.csv"
OUT_CACHE = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round42_dataset_nutrition_cache.csv"
OUT_NUTRITION_SUMMARY = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_nutrition_summary.txt"
OUT_NUTRITION_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_nutrition_audit.csv"
OUT_GENERATOR_READY_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_generator_ready_audit.csv"
OUT_QUEUE_ADDITIONS = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_manual_repair_queue_additions.csv"
OUT_QUEUE_ADDITIONS_SUMMARY = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_manual_repair_queue_additions_summary.txt"
OUT_MATERIALIZATION_SUMMARY = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_materialization_summary.txt"
OUT_MATERIALIZATION_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_materialization_audit.csv"

ROUND42_TAG = "round42_dataset_curated_expansion"
ROUND42_SCOPE_STATUS = "v1_2_generator_ready_draft"
ROUND42_NUTRITION_BASIS = "recipes_v1_2_round42_dataset_mapped_ingredients_draft"
ROUND42_CACHE_VERSION = "recipes_v1_2_round42_dataset_001"


def main() -> None:
    configure_round37_module()
    round37.run_full_pipeline()


def run_unit_rules() -> list[dict[str, object]]:
    configure_round37_module()
    return round37.run_unit_rules()


def run_mapping() -> tuple[list[dict[str, str]], list[dict[str, object]], list[dict[str, object]], dict[str, dict[str, Any]]]:
    configure_round37_module()
    return round37.run_mapping()


def configure_round37_module() -> None:
    round37.RECIPES = RECIPES
    round37.PARSED = PARSED
    round37.FOODDB = FOODDB
    round37.BASE_DATASET_DIR = BASE_DATASET_DIR
    round37.EXPANDED_DATASET_DIR = EXPANDED_DATASET_DIR
    round37.MANUAL_REPAIR_QUEUE = MANUAL_REPAIR_QUEUE
    round37.OUT_UNIT_RULES = OUT_UNIT_RULES
    round37.OUT_MATCHES = OUT_MATCHES
    round37.OUT_UNMAPPED = OUT_UNMAPPED
    round37.OUT_MAPPING_SUMMARY = OUT_MAPPING_SUMMARY
    round37.OUT_MAPPING_REVIEW = OUT_MAPPING_REVIEW
    round37.OUT_MAPPING_QUALITY_AUDIT = OUT_MAPPING_QUALITY_AUDIT
    round37.OUT_CACHE = OUT_CACHE
    round37.OUT_NUTRITION_SUMMARY = OUT_NUTRITION_SUMMARY
    round37.OUT_NUTRITION_AUDIT = OUT_NUTRITION_AUDIT
    round37.OUT_GENERATOR_READY_AUDIT = OUT_GENERATOR_READY_AUDIT
    round37.OUT_QUEUE_ADDITIONS = OUT_QUEUE_ADDITIONS
    round37.OUT_QUEUE_ADDITIONS_SUMMARY = OUT_QUEUE_ADDITIONS_SUMMARY
    round37.OUT_MATERIALIZATION_SUMMARY = OUT_MATERIALIZATION_SUMMARY
    round37.OUT_MATERIALIZATION_AUDIT = OUT_MATERIALIZATION_AUDIT
    round37.ROUND37_TAG = ROUND42_TAG
    round37.ROUND37_SCOPE_STATUS = ROUND42_SCOPE_STATUS
    round37.ROUND37_NUTRITION_BASIS = ROUND42_NUTRITION_BASIS
    round37.ROUND37_CACHE_VERSION = ROUND42_CACHE_VERSION
    round37.ROUND37_ALIAS_FOOD_IDS.update(round42_alias_food_ids())
    round37.ROUND37_CONTAINS_ALIAS_FOOD_IDS.extend(round42_contains_alias_food_ids())
    round37.materialize_expanded_dataset = materialize_expanded_dataset
    round37.materialized_recipe_row = materialized_recipe_row
    round37.materialized_ingredient_row = materialized_ingredient_row
    round37.materialized_cache_row = materialized_cache_row
    round37.update_manual_repair_queue = update_manual_repair_queue
    round37.manual_repair_columns = manual_repair_columns
    round37.manual_repair_row = manual_repair_row
    round37.is_valuable_repair_candidate = is_valuable_repair_candidate
    round37.priority_for_recipe = priority_for_recipe
    round37.build_mapping_summary = build_mapping_summary
    round37.build_nutrition_summary = build_nutrition_summary
    round37.build_queue_additions_summary = build_queue_additions_summary
    round37.build_materialization_summary = build_materialization_summary


def round42_alias_food_ids() -> dict[str, str]:
    return {
        "beef round steak": "food_v1_2_candidate_beef_round_steak_raw",
        "round steak": "food_v1_2_candidate_beef_round_steak_raw",
        "chicken thighs": "food_v1_2_candidate_chicken_thigh_meat_skin_raw",
        "chicken thigh": "food_v1_2_candidate_chicken_thigh_meat_skin_raw",
        "lamb stew meat": "food_v1_2_candidate_lamb_cubed_stew_raw",
        "lamb": "food_v1_2_candidate_lamb_cubed_stew_raw",
        "pinto beans": "food_v1_2_candidate_pinto_beans_dry",
        "great northern beans": "food_v1_2_candidate_great_northern_beans_dry",
        "sour cream": "food_v1_2_candidate_sour_cream_cultured",
        "light sour cream": "food_v1_2_candidate_sour_cream_light",
        "bok choy": "food_v1_2_candidate_bok_choy_raw",
        "refried beans": "food_v1_2_candidate_refried_beans_canned_traditional",
        "cheddar cheese": "food_v1_2_candidate_cheddar_cheese",
        "swiss cheese": "food_v1_2_candidate_swiss_cheese",
        "soy milk": "food_v1_2_candidate_unsweetened_soy_milk",
        "oat milk": "food_v1_2_candidate_oat_milk_unsweetened_plain",
    }


def round42_contains_alias_food_ids() -> list[tuple[str, str]]:
    return [
        ("beef round steak", "food_v1_2_candidate_beef_round_steak_raw"),
        ("round steak", "food_v1_2_candidate_beef_round_steak_raw"),
        ("chicken thigh", "food_v1_2_candidate_chicken_thigh_meat_skin_raw"),
        ("lamb stew", "food_v1_2_candidate_lamb_cubed_stew_raw"),
        ("pinto bean", "food_v1_2_candidate_pinto_beans_dry"),
        ("great northern bean", "food_v1_2_candidate_great_northern_beans_dry"),
        ("sour cream", "food_v1_2_candidate_sour_cream_cultured"),
        ("bok choy", "food_v1_2_candidate_bok_choy_raw"),
        ("refried bean", "food_v1_2_candidate_refried_beans_canned_traditional"),
        ("cheddar cheese", "food_v1_2_candidate_cheddar_cheese"),
        ("swiss cheese", "food_v1_2_candidate_swiss_cheese"),
        ("soy milk", "food_v1_2_candidate_unsweetened_soy_milk"),
        ("oat milk", "food_v1_2_candidate_oat_milk_unsweetened_plain"),
    ]


def materialize_expanded_dataset(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, object]],
    cache_rows: list[dict[str, object]],
    ready_ids: set[str],
) -> None:
    EXPANDED_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    base_recipes = round37.read_csv(BASE_DATASET_DIR / "recipes.csv")
    base_ingredients = round37.read_csv(BASE_DATASET_DIR / "recipe_ingredients.csv")
    base_cache = round37.read_csv(BASE_DATASET_DIR / "recipe_nutrition_cache.csv")
    recipe_by_id = {row["recipe_id_candidate"]: row for row in recipes}
    cache_by_id = {row["recipe_id_candidate"]: row for row in cache_rows}
    new_recipe_rows = [
        materialized_recipe_row(recipe_by_id[recipe_id], cache_by_id[recipe_id])
        for recipe_id in sorted(ready_ids)
    ]
    new_ingredient_rows = [
        materialized_ingredient_row(row)
        for row in mapping_rows
        if round37.clean_text(row.get("recipe_id_candidate")) in ready_ids
    ]
    new_cache_rows = [
        materialized_cache_row(cache_by_id[recipe_id])
        for recipe_id in sorted(ready_ids)
    ]
    round37.write_csv(EXPANDED_DATASET_DIR / "recipes.csv", base_recipes + new_recipe_rows, list(base_recipes[0].keys()))
    round37.write_csv(
        EXPANDED_DATASET_DIR / "recipe_ingredients.csv",
        base_ingredients + new_ingredient_rows,
        list(base_ingredients[0].keys()),
    )
    round37.write_csv(
        EXPANDED_DATASET_DIR / "recipe_nutrition_cache.csv",
        base_cache + new_cache_rows,
        list(base_cache[0].keys()),
    )
    readme_text = "\n".join(
        [
            "Recipes_DB v1.2 generator-ready Round42 dataset-expanded draft",
            "",
            "Draft/test only. Do not treat as current production data.",
            f"Base dataset: {BASE_DATASET_DIR}",
            f"Round42 ready additions: {len(ready_ids)}",
            f"QC tag: {ROUND42_TAG}",
            "",
        ]
    )
    (EXPANDED_DATASET_DIR / "README_v1_2_generator_ready_round42_dataset_expanded.txt").write_text(
        readme_text,
        encoding="utf-8",
    )


def materialized_recipe_row(recipe: dict[str, str], cache: dict[str, object]) -> dict[str, object]:
    source_index = round37.clean_text(recipe.get("source_index"))
    directions = json.loads(recipe.get("directions_json") or "[]")
    active_time = round37.base.estimate_active_time(recipe)
    target_bucket = round37.clean_text(recipe.get("target_bucket"))
    allowed_slots = ["breakfast"] if target_bucket == "breakfast_competitor" else ["lunch", "dinner"]
    return {
        "recipe_id": recipe["recipe_id_candidate"],
        "source_recipe_id": source_index,
        "source_dataset": "recipes_dataset_64k_dishes_round42_dataset_curated",
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
        "scope_status": ROUND42_SCOPE_STATUS,
        "has_ingredients_parsed": 1,
        "has_nutrition_cache": 1,
        "is_pilot_recipe": 0,
        "is_active": 1,
        "qc_recipe_status": "generator_ready_draft",
        "qc_notes": f"{ROUND42_TAG}; draft_only; no_fooddb_rows_added; strong={cache.get('strong_generator_ready')}",
        "allowed_slots_json": json.dumps(allowed_slots),
        "slot_policy_reason": f"round42_dataset_{target_bucket}",
        "content_quality_status": "keep",
        "content_exclusion_reason": "",
        "active_time_estimated_min": active_time,
        "passive_time_estimated_min": 0,
        "effective_time_min_for_scoring": active_time,
        "has_long_passive_time": "False",
        "time_estimation_confidence": "medium",
        "time_estimation_method": "round42_direction_step_estimate",
        "time_estimation_reasons": "dataset_curated_direction_count_estimate",
    }


def materialized_ingredient_row(row: dict[str, object]) -> dict[str, object]:
    output = round37.base.materialized_ingredient_row(row)
    output["qc_ingredient_status"] = (
        "accepted_auto_round42"
        if row.get("mapping_status") == "accepted_auto"
        else "needs_review_round42"
    )
    output["qc_notes"] = ROUND42_TAG
    return output


def materialized_cache_row(row: dict[str, object]) -> dict[str, object]:
    output = round37.base.materialized_cache_row(row)
    output["nutrition_basis"] = ROUND42_NUTRITION_BASIS
    output["cache_version"] = ROUND42_CACHE_VERSION
    output["qc_notes"] = f"{ROUND42_TAG}; {row['generator_ready_failure_reason']}; strong={row.get('strong_generator_ready')}"
    return output


def update_manual_repair_queue(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, object]],
    cache_rows: list[dict[str, object]],
    ready_ids: set[str],
) -> list[dict[str, object]]:
    existing_rows_all = round37.read_csv(MANUAL_REPAIR_QUEUE) if MANUAL_REPAIR_QUEUE.exists() else []
    columns = manual_repair_columns(existing_rows_all)
    retained_rows = [
        row for row in existing_rows_all
        if row.get("created_from_round") != "round42"
        and not str(row.get("recipe_id_candidate", "")).startswith("recipes_v1_2_round42_dataset_")
    ]
    existing_recipe_ids = {row.get("recipe_id_candidate", "") for row in retained_rows}
    cache_by_id = {row["recipe_id_candidate"]: row for row in cache_rows}
    mapping_by_recipe: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in mapping_rows:
        mapping_by_recipe[round37.clean_text(row.get("recipe_id_candidate"))].append(row)

    additions = []
    for recipe in recipes:
        recipe_id = recipe["recipe_id_candidate"]
        if recipe_id in ready_ids or recipe_id in existing_recipe_ids:
            continue
        cache = cache_by_id.get(recipe_id, {})
        if not is_valuable_repair_candidate(recipe, cache):
            continue
        blocker = round37.choose_blocking_ingredient(mapping_by_recipe.get(recipe_id, []), cache)
        additions.append(manual_repair_row(len(retained_rows) + len(additions) + 1, recipe, cache, blocker))

    normalized_existing = [{column: row.get(column, "") for column in columns} for row in retained_rows]
    normalized_additions = [{column: row.get(column, "") for column in columns} for row in additions]
    round37.write_csv(MANUAL_REPAIR_QUEUE, normalized_existing + normalized_additions, columns)
    return additions


def manual_repair_columns(existing_rows: list[dict[str, str]] | None = None) -> list[str]:
    base_columns = list(existing_rows[0].keys()) if existing_rows else ORIGINAL_MANUAL_REPAIR_COLUMNS([])
    extra_columns = [
        "why_failed",
        "blocking_ingredients",
        "needed_fooddb_items",
        "needed_aliases",
        "needed_unit_rules",
        "needed_servings_decision",
        "source_needed",
        "priority",
        "expected_generator_value",
    ]
    columns = list(base_columns)
    for column in extra_columns:
        if column not in columns:
            columns.append(column)
    return columns


def is_valuable_repair_candidate(recipe: dict[str, str], cache: dict[str, object]) -> bool:
    if recipe.get("quality_status") == "reject":
        return False
    if recipe.get("target_bucket") not in {
        "breakfast_competitor",
        "lunch_dinner_carb_protein",
        "fish_turkey_pork_main",
        "vegetarian_legume_balanced",
        "flexible_high_value",
    }:
        return False
    failure = round37.clean_text(cache.get("generator_ready_failure_reason"))
    if not failure:
        return False
    if "cache_status=no_accepted_mapped_ingredients" in failure:
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


def manual_repair_row(
    sequence: int,
    recipe: dict[str, str],
    cache: dict[str, object],
    blocker: dict[str, object],
) -> dict[str, object]:
    row = ORIGINAL_MANUAL_REPAIR_ROW(sequence, recipe, cache, blocker)
    row.update(
        {
            "repair_id": f"repair_v1_2_round42_{sequence:04d}",
            "recipe_source": "recipes_dataset_64k_dishes_round42_dataset_curated",
            "decision_notes": "round42_failed_but_potentially_useful",
            "created_from_round": "round42",
            "qc_notes": "round42_manual_repair_queue_addition",
            "why_failed": cache.get("generator_ready_failure_reason", ""),
            "blocking_ingredients": blocker.get("ingredient_name_normalized", "") or blocker.get("ingredient_raw_text", ""),
            "needed_fooddb_items": blocker.get("ingredient_name_normalized", ""),
            "needed_aliases": blocker.get("ingredient_name_normalized", "") if blocker.get("mapping_status") in {"unmapped", "review_needed"} else "",
            "needed_unit_rules": blocker.get("ingredient_name_normalized", "") if not blocker.get("quantity_grams_estimated") else "",
            "needed_servings_decision": "true" if not blocker else "false",
            "source_needed": "true" if row.get("needs_fooddb_addition") == "true" else "false",
        }
    )
    return row


def priority_for_recipe(recipe: dict[str, str]) -> str:
    bucket = recipe.get("target_bucket")
    if bucket in {"lunch_dinner_carb_protein", "breakfast_competitor"}:
        return "high"
    if bucket in {"fish_turkey_pork_main", "vegetarian_legume_balanced"}:
        return "medium"
    return "low"


def build_mapping_summary(rows: list[dict[str, object]]) -> str:
    text = ORIGINAL_BUILD_MAPPING_SUMMARY(rows)
    return text.replace("Round37 +100", "Round42 dataset-curated").replace("Round37", "Round42").replace("round37", "round42")


def build_nutrition_summary(rows: list[dict[str, object]]) -> str:
    text = ORIGINAL_BUILD_NUTRITION_SUMMARY(rows)
    return text.replace("Round37 +100", "Round42 dataset-curated").replace("Round37", "Round42").replace("round37", "round42")


def build_queue_additions_summary(rows: list[dict[str, object]]) -> str:
    problem_counts = Counter(str(row.get("problem_type")) for row in rows)
    fix_counts = Counter(str(row.get("proposed_fix_type")) for row in rows)
    lines = [
        "Recipes_DB v1.2 Round42 manual repair queue additions summary",
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
    base_count = len(round37.read_csv(BASE_DATASET_DIR / "recipes.csv"))
    lines = [
        "Recipes_DB v1.2 Round42 dataset materialization summary",
        "",
        f"base_dataset={BASE_DATASET_DIR}",
        f"new_dataset={EXPANDED_DATASET_DIR}",
        f"base_recipe_count={base_count}",
        f"selected_dataset_candidate_count={len(recipes)}",
        f"generator_ready_round42_count={len(ready_ids)}",
        f"strong_generator_ready_round42_count={len(strong_ids)}",
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


if __name__ == "__main__":
    main()
