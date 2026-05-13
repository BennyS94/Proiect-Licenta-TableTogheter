from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.round41_manual_curated_common import (
    BASELINE_DIR,
    MANUAL_REPAIR_QUEUE,
    RECIPES_AUDIT_DIR,
    RECIPES_DRAFT_DIR,
    ROUND41_DATASET_DIR,
    dumps_json,
    fooddb_path,
    read_csv,
    recipe_catalog,
    to_float,
    write_csv,
    write_text,
)


OUT_RECIPES = RECIPES_DRAFT_DIR / "recipes_v1_2_round41_manual_curated_50.csv"
OUT_INGREDIENTS = RECIPES_DRAFT_DIR / "recipes_v1_2_round41_manual_curated_50_ingredients.csv"
OUT_NUTRITION = RECIPES_DRAFT_DIR / "recipes_v1_2_round41_manual_curated_50_nutrition_cache.csv"
OUT_SUMMARY = RECIPES_AUDIT_DIR / "recipes_v1_2_round41_manual_curated_50_build_summary.txt"
OUT_AUDIT = RECIPES_AUDIT_DIR / "recipes_v1_2_round41_manual_curated_50_build_audit.csv"
OUT_FAILURES = RECIPES_AUDIT_DIR / "recipes_v1_2_round41_manual_curated_50_failures.csv"
OUT_MATERIALIZATION_SUMMARY = RECIPES_AUDIT_DIR / "recipes_v1_2_round41_manual_curated_materialization_summary.txt"
OUT_MATERIALIZATION_AUDIT = RECIPES_AUDIT_DIR / "recipes_v1_2_round41_manual_curated_materialization_audit.csv"

READY_SCOPE_STATUS = "v1_2_generator_ready_draft"
QC_TAG = "manual_curated_round41_v1_2"


def main() -> None:
    fooddb = read_csv(fooddb_path())
    food_lookup = {row.get("canonical_name", ""): row for row in fooddb}
    baseline_recipes = read_csv(BASELINE_DIR / "recipes.csv")
    baseline_ingredients = read_csv(BASELINE_DIR / "recipe_ingredients.csv")
    baseline_nutrition = read_csv(BASELINE_DIR / "recipe_nutrition_cache.csv")

    recipe_fieldnames = list(baseline_recipes[0].keys())
    ingredient_fieldnames = list(baseline_ingredients[0].keys())
    nutrition_fieldnames = list(baseline_nutrition[0].keys())

    ready_recipes: list[dict[str, Any]] = []
    ready_ingredients: list[dict[str, Any]] = []
    ready_nutrition: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []

    for item in recipe_catalog():
        build = build_recipe(item, food_lookup, recipe_fieldnames, ingredient_fieldnames, nutrition_fieldnames)
        audit_rows.append(build["audit"])
        if build["ready"]:
            ready_recipes.append(build["recipe"])
            ready_ingredients.extend(build["ingredients"])
            ready_nutrition.append(build["nutrition"])
        else:
            failure_rows.append(build["failure"])

    write_csv(OUT_RECIPES, ready_recipes, recipe_fieldnames)
    write_csv(OUT_INGREDIENTS, ready_ingredients, ingredient_fieldnames)
    write_csv(OUT_NUTRITION, ready_nutrition, nutrition_fieldnames)
    write_csv(
        OUT_AUDIT,
        audit_rows,
        [
            "manual_recipe_id",
            "display_name",
            "target_bucket",
            "allowed_slots_json",
            "ready",
            "failure_reason",
            "kcal_per_serving",
            "protein_g_per_serving",
            "carbs_g_per_serving",
            "fat_g_per_serving",
            "total_weight_grams",
            "servings",
            "missing_canonicals",
        ],
    )
    write_csv(
        OUT_FAILURES,
        failure_rows,
        [
            "manual_recipe_id",
            "display_name",
            "why_failed",
            "blocking_ingredients",
            "needed_fooddb_items",
            "needed_aliases",
            "needed_unit_rules",
            "needed_servings_decision",
            "source_needed",
            "priority",
            "expected_generator_value",
        ],
    )
    if failure_rows:
        append_failures_to_repair_queue(failure_rows)

    materialization_rows = materialize_dataset(
        baseline_recipes,
        baseline_ingredients,
        baseline_nutrition,
        ready_recipes,
        ready_ingredients,
        ready_nutrition,
        recipe_fieldnames,
        ingredient_fieldnames,
        nutrition_fieldnames,
    )
    summary = build_summary(ready_recipes, failure_rows, audit_rows, baseline_recipes, materialization_rows)
    write_text(OUT_SUMMARY, summary)
    print(summary)


def build_recipe(
    item: dict[str, Any],
    food_lookup: dict[str, dict[str, str]],
    recipe_fieldnames: list[str],
    ingredient_fieldnames: list[str],
    nutrition_fieldnames: list[str],
) -> dict[str, Any]:
    missing = [
        ingredient["canonical"]
        for ingredient in item["ingredients"]
        if ingredient["canonical"] not in food_lookup
    ]
    totals = {"kcal": 0.0, "protein": 0.0, "carbs": 0.0, "fat": 0.0}
    total_weight = 0.0
    ingredient_rows: list[dict[str, Any]] = []
    if not missing:
        for position, ingredient in enumerate(item["ingredients"], start=1):
            food = food_lookup[ingredient["canonical"]]
            grams = float(ingredient["grams"])
            total_weight += grams
            totals["kcal"] += to_float(food.get("energy_kcal_100g")) * grams / 100.0
            totals["protein"] += to_float(food.get("protein_g_100g")) * grams / 100.0
            totals["carbs"] += to_float(food.get("carbs_g_100g")) * grams / 100.0
            totals["fat"] += to_float(food.get("fat_g_100g")) * grams / 100.0
            ingredient_rows.append(
                row_for_fields(
                    ingredient_fieldnames,
                    {
                        "recipe_ingredient_id": f"{item['manual_recipe_id']}_ingredient_{position:03d}",
                        "recipe_id": item["manual_recipe_id"],
                        "ingredient_position": position,
                        "ingredient_raw_text": f"{format_number(grams)} g {ingredient['name']}",
                        "ingredient_name_parsed": ingredient["name"],
                        "ingredient_name_normalized": ingredient["name"].lower().replace(" ", "_"),
                        "quantity_value": format_number(grams),
                        "quantity_unit": "g",
                        "quantity_text": format_number(grams),
                        "quantity_grams_estimated": round(grams, 4),
                        "ingredient_role": ingredient["role"],
                        "ingredient_slot_key": ingredient["canonical"],
                        "is_optional": 0,
                        "is_substitutable": 1,
                        "substitution_group_id": "",
                        "mapped_food_id": food.get("food_id", ""),
                        "mapped_food_canonical_name": food.get("canonical_name", ""),
                        "mapping_status": "accepted_auto",
                        "mapping_confidence": "high",
                        "mapping_method": "manual_curated_round41",
                        "mapping_notes": f"manual_curated_round41:{ingredient['canonical']}",
                        "qc_ingredient_status": "accepted_auto_round41_manual_curated",
                        "qc_notes": QC_TAG,
                    },
                )
            )

    servings = int(item["servings"])
    per_serving = {
        "kcal": totals["kcal"] / servings if servings else 0.0,
        "protein": totals["protein"] / servings if servings else 0.0,
        "carbs": totals["carbs"] / servings if servings else 0.0,
        "fat": totals["fat"] / servings if servings else 0.0,
        "weight": total_weight / servings if servings else 0.0,
    }
    failure_reason = readiness_failure(item, per_serving, missing)
    ready = not failure_reason
    recipe_row = row_for_fields(recipe_fieldnames, recipe_row_values(item, total_weight))
    nutrition_row = row_for_fields(nutrition_fieldnames, nutrition_row_values(item, totals, per_serving, total_weight))
    audit = {
        "manual_recipe_id": item["manual_recipe_id"],
        "display_name": item["display_name"],
        "target_bucket": item["target_bucket"],
        "allowed_slots_json": item["allowed_slots_json"],
        "ready": str(ready).lower(),
        "failure_reason": failure_reason,
        "kcal_per_serving": round(per_serving["kcal"], 4),
        "protein_g_per_serving": round(per_serving["protein"], 4),
        "carbs_g_per_serving": round(per_serving["carbs"], 4),
        "fat_g_per_serving": round(per_serving["fat"], 4),
        "total_weight_grams": round(total_weight, 4),
        "servings": servings,
        "missing_canonicals": ";".join(missing),
    }
    failure = {
        "manual_recipe_id": item["manual_recipe_id"],
        "display_name": item["display_name"],
        "why_failed": failure_reason,
        "blocking_ingredients": ";".join(missing),
        "needed_fooddb_items": ";".join(missing),
        "needed_aliases": "",
        "needed_unit_rules": "",
        "needed_servings_decision": "false",
        "source_needed": "true" if missing else "false",
        "priority": "high" if item["target_bucket"] != "snack" else "medium",
        "expected_generator_value": expected_generator_value(item),
    }
    return {
        "ready": ready,
        "recipe": recipe_row,
        "ingredients": ingredient_rows,
        "nutrition": nutrition_row,
        "audit": audit,
        "failure": failure,
    }


def row_for_fields(fieldnames: list[str], values: dict[str, Any]) -> dict[str, Any]:
    return {field: values.get(field, "") for field in fieldnames}


def recipe_row_values(item: dict[str, Any], total_weight: float) -> dict[str, Any]:
    prep = int(item["prep_time_min"])
    cook = int(item["cook_time_min"])
    slots = json.loads(item["allowed_slots_json"])
    return {
        "recipe_id": item["manual_recipe_id"],
        "source_recipe_id": item["manual_recipe_id"],
        "source_dataset": "manual_curated_round41_v1_2",
        "recipe_name": item["display_name"],
        "display_name": item["display_name"],
        "recipe_family_name": item["display_name"],
        "recipe_kind": item["recipe_kind"],
        "recipe_category": "manual_curated",
        "recipe_subcategory": item["target_bucket"],
        "recipe_cuisine": "European practical",
        "directions_json": dumps_json(item["directions"]),
        "directions_step_count": len(item["directions"]),
        "servings_declared": item["servings"],
        "servings_normalized": item["servings"],
        "prep_time_min": prep,
        "cook_time_min": cook,
        "total_time_min": prep + cook,
        "difficulty_level": "easy",
        "scope_status": READY_SCOPE_STATUS,
        "has_ingredients_parsed": 1,
        "has_nutrition_cache": 1,
        "is_pilot_recipe": 0,
        "is_active": 1,
        "qc_recipe_status": "generator_ready_draft",
        "qc_notes": f"{QC_TAG}; total_weight_g={round(total_weight, 2)}",
        "allowed_slots_json": item["allowed_slots_json"],
        "slot_policy_reason": slot_policy_reason(slots),
        "content_quality_status": "keep",
        "content_exclusion_reason": "",
        "active_time_estimated_min": prep + cook,
        "passive_time_estimated_min": 0,
        "effective_time_min_for_scoring": prep + cook,
        "has_long_passive_time": "False",
        "time_estimation_confidence": "high",
        "time_estimation_method": "manual_curated_round41",
        "time_estimation_reasons": "explicit_manual_curated_time",
    }


def nutrition_row_values(
    item: dict[str, Any],
    totals: dict[str, float],
    per_serving: dict[str, float],
    total_weight: float,
) -> dict[str, Any]:
    servings = int(item["servings"])
    return {
        "recipe_id": item["manual_recipe_id"],
        "nutrition_basis": "recipes_v1_2_manual_curated_round41",
        "servings_basis": servings,
        "total_weight_grams_estimated": round(total_weight, 4),
        "energy_kcal_total": round(totals["kcal"], 4),
        "protein_g_total": round(totals["protein"], 4),
        "carbs_g_total": round(totals["carbs"], 4),
        "fat_g_total": round(totals["fat"], 4),
        "fibre_g_total": "",
        "sugars_g_total": "",
        "salt_g_total": "",
        "water_g_total": "",
        "energy_kcal_per_serving": round(per_serving["kcal"], 4),
        "protein_g_per_serving": round(per_serving["protein"], 4),
        "carbs_g_per_serving": round(per_serving["carbs"], 4),
        "fat_g_per_serving": round(per_serving["fat"], 4),
        "fibre_g_per_serving": "",
        "sugars_g_per_serving": "",
        "salt_g_per_serving": "",
        "water_g_per_serving": "",
        "mapped_ingredient_count": len(item["ingredients"]),
        "unmapped_ingredient_count": 0,
        "mapped_weight_ratio": 1.0,
        "cache_status": "usable_from_mapped_ingredients",
        "cache_version": "recipes_v1_2_manual_curated_round41",
        "qc_notes": QC_TAG,
        "macro_relevant_mapped_weight_ratio": 1.0,
        "uses_pilot_servings_fallback": "False",
        "servings_estimation_method": "explicit_manual_curated",
        "servings_adjustment_applied": "false",
        "original_servings_basis": servings,
        "adjusted_servings_basis": servings,
        "quality_flags": "",
    }


def readiness_failure(item: dict[str, Any], per_serving: dict[str, float], missing: list[str]) -> str:
    if missing:
        return "missing_fooddb_mapping"
    slots = json.loads(item["allowed_slots_json"])
    kcal = per_serving["kcal"]
    protein = per_serving["protein"]
    carbs = per_serving["carbs"]
    if slots == ["breakfast"]:
        failures = []
        if kcal < 200 or kcal > 800:
            failures.append("breakfast_kcal_out_of_range")
        if protein < 8:
            failures.append("breakfast_protein_low")
        if carbs < 20:
            failures.append("breakfast_carbs_low")
        return ";".join(failures)
    if slots == ["snack"]:
        failures = []
        if kcal < 80 or kcal > 350:
            failures.append("snack_kcal_out_of_range")
        return ";".join(failures)
    failures = []
    if kcal < 350:
        failures.append("main_kcal_low")
    if protein < 14:
        failures.append("main_protein_low")
    if carbs < 25:
        failures.append("main_carbs_low")
    return ";".join(failures)


def slot_policy_reason(slots: list[str]) -> str:
    if slots == ["breakfast"]:
        return "manual_curated_breakfast_round41"
    if slots == ["snack"]:
        return "manual_curated_snack_round41"
    return "manual_curated_complete_main_lunch_dinner_round41"


def expected_generator_value(item: dict[str, Any]) -> str:
    bucket = str(item["target_bucket"])
    protein = str(item["primary_protein"])
    if bucket == "breakfast_competitor":
        return "breakfast_variety"
    if bucket == "snack":
        return "snack"
    if "fish" in protein:
        return "fish_turkey_pork_main"
    if "turkey" in protein:
        return "fish_turkey_pork_main"
    if "pork" in protein:
        return "fish_turkey_pork_main"
    if "legume" in protein or bucket == "vegetarian_legume_balanced":
        return "vegetarian_balanced"
    return "carb_protein_main"


def append_failures_to_repair_queue(failure_rows: list[dict[str, Any]]) -> None:
    existing_rows = read_csv(MANUAL_REPAIR_QUEUE)
    if not existing_rows:
        return
    fieldnames = list(existing_rows[0].keys())
    existing_ids = {row.get("repair_id", "") for row in existing_rows}
    start_index = len(existing_rows) + 1
    additions = []
    for offset, failure in enumerate(failure_rows, start=start_index):
        repair_id = f"repair_v1_2_round41_{offset:04d}"
        while repair_id in existing_ids:
            offset += 1
            repair_id = f"repair_v1_2_round41_{offset:04d}"
        additions.append(
            row_for_fields(
                fieldnames,
                {
                    "repair_id": repair_id,
                    "recipe_id_candidate": failure["manual_recipe_id"],
                    "source_index": "",
                    "display_name": failure["display_name"],
                    "recipe_source": "manual_curated_round41_v1_2",
                    "recipe_kind_guess": "manual_curated",
                    "target_slot": "",
                    "target_bucket": "",
                    "priority": failure["priority"],
                    "problem_type": failure["why_failed"],
                    "blocking_ingredient": failure["blocking_ingredients"],
                    "ingredient_raw_text": failure["blocking_ingredients"],
                    "ingredient_name_normalized": failure["blocking_ingredients"],
                    "current_mapping_status": "missing_or_failed",
                    "current_quantity_grams_estimated": "",
                    "proposed_fix_type": "fooddb_addition" if failure["needed_fooddb_items"] else "recipe_metadata_fix",
                    "proposed_fix_detail": failure["why_failed"],
                    "needs_fooddb_addition": str(bool(failure["needed_fooddb_items"])).lower(),
                    "needs_alias": str(bool(failure["needed_aliases"])).lower(),
                    "needs_unit_rule": str(bool(failure["needed_unit_rules"])).lower(),
                    "needs_servings_fix": failure["needed_servings_decision"],
                    "needs_web_source": failure["source_needed"],
                    "source_needed": "verified_food_nutrition_source" if failure["source_needed"] == "true" else "",
                    "decision_status": "pending",
                    "decision_notes": "",
                    "expected_generator_value": failure["expected_generator_value"],
                    "created_from_round": "round41_manual_curated",
                    "qc_notes": failure["why_failed"],
                },
            )
        )
    write_csv(MANUAL_REPAIR_QUEUE, existing_rows + additions, fieldnames)


def materialize_dataset(
    baseline_recipes: list[dict[str, str]],
    baseline_ingredients: list[dict[str, str]],
    baseline_nutrition: list[dict[str, str]],
    ready_recipes: list[dict[str, Any]],
    ready_ingredients: list[dict[str, Any]],
    ready_nutrition: list[dict[str, Any]],
    recipe_fieldnames: list[str],
    ingredient_fieldnames: list[str],
    nutrition_fieldnames: list[str],
) -> list[dict[str, Any]]:
    ROUND41_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(ROUND41_DATASET_DIR / "recipes.csv", baseline_recipes + ready_recipes, recipe_fieldnames)
    write_csv(
        ROUND41_DATASET_DIR / "recipe_ingredients.csv",
        baseline_ingredients + ready_ingredients,
        ingredient_fieldnames,
    )
    write_csv(
        ROUND41_DATASET_DIR / "recipe_nutrition_cache.csv",
        baseline_nutrition + ready_nutrition,
        nutrition_fieldnames,
    )
    readme = [
        "Recipes_DB v1.2 Round41 manual-curated draft dataset",
        "",
        f"base_dataset={BASELINE_DIR}",
        f"manual_ready_count={len(ready_recipes)}",
        f"new_recipe_count={len(baseline_recipes) + len(ready_recipes)}",
        "scope=draft/audit/test only",
        "qc_notes=manual_curated_round41_v1_2",
    ]
    write_text(
        ROUND41_DATASET_DIR / "README_v1_2_generator_ready_round41_manual_curated.txt",
        "\n".join(readme),
    )
    rows = [
        {
            "recipe_id": row["recipe_id"],
            "display_name": row["display_name"],
            "source": "round41_manual_curated",
            "included": "true",
        }
        for row in ready_recipes
    ]
    write_csv(
        OUT_MATERIALIZATION_AUDIT,
        rows,
        ["recipe_id", "display_name", "source", "included"],
    )
    summary = [
        "Recipes_DB v1.2 Round41 manual-curated materialization summary",
        "",
        f"base_dataset={BASELINE_DIR}",
        f"new_dataset={ROUND41_DATASET_DIR}",
        f"base_recipe_count={len(baseline_recipes)}",
        f"manual_ready_recipe_count={len(ready_recipes)}",
        f"new_recipe_count={len(baseline_recipes) + len(ready_recipes)}",
    ]
    write_text(OUT_MATERIALIZATION_SUMMARY, "\n".join(summary))
    return rows


def build_summary(
    ready_recipes: list[dict[str, Any]],
    failure_rows: list[dict[str, Any]],
    audit_rows: list[dict[str, Any]],
    baseline_recipes: list[dict[str, str]],
    materialization_rows: list[dict[str, Any]],
) -> str:
    slot_counts = Counter()
    for row in ready_recipes:
        slots = json.loads(row.get("allowed_slots_json", "[]"))
        if slots == ["breakfast"]:
            slot_counts["breakfast"] += 1
        elif slots == ["snack"]:
            slot_counts["snack"] += 1
        else:
            slot_counts["lunch_dinner"] += 1
    failure_counts = Counter(row["why_failed"] for row in failure_rows)
    return "\n".join(
        [
            "Recipes_DB v1.2 Round41 manual-curated build summary",
            "",
            f"planned_recipes={len(audit_rows)}",
            f"ready_recipes={len(ready_recipes)}",
            f"failed_recipes={len(failure_rows)}",
            f"ready_breakfast={slot_counts.get('breakfast', 0)}",
            f"ready_lunch_dinner={slot_counts.get('lunch_dinner', 0)}",
            f"ready_snack={slot_counts.get('snack', 0)}",
            f"failure_reasons={dict(failure_counts)}",
            f"base_recipe_count={len(baseline_recipes)}",
            f"new_dataset_recipe_count={len(baseline_recipes) + len(ready_recipes)}",
            f"materialized_rows={len(materialization_rows)}",
            "",
            "Conclusion:",
            "- Manual-curated batch uses explicit grams and Food_DB mapped ingredients.",
            "- Nutrition values are computed from Food_DB per-100g values only.",
            "- Failed valuable recipes, if any, are written to failures and repair queue.",
        ]
    )


def format_number(value: float) -> str:
    if abs(value - round(value)) < 0.0001:
        return str(int(round(value)))
    return str(round(value, 2))


if __name__ == "__main__":
    main()
