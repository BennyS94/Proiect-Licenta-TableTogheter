from __future__ import annotations

import csv
import json
import math
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.generator_v1.candidate_filter import (  # noqa: E402
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (  # noqa: E402
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.multi_day_audit import multi_day_meal_rows  # noqa: E402
from src.generator_v1.multi_day_selector import MULTI_DAY_MODE_GLOBAL, generate_multi_day_plan  # noqa: E402
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import build_nutrition_target  # noqa: E402
from tools.extra import build_recipes_v1_2_round37_plus100_nutrition_cache as round37  # noqa: E402


FOODDB_BASE = REPO_ROOT / "data/fooddb/draft/fooddb_v1_1_core_master_draft_round9.csv"
VERIFIED_BATCH = REPO_ROOT / "data/fooddb/draft/fooddb_v1_2_manual_additions_verified_batch1.csv"
FOODDB_BATCH1 = REPO_ROOT / "data/fooddb/draft/fooddb_v1_2_core_master_manual_batch1.csv"
VALIDATION_OUT = REPO_ROOT / "data/fooddb/audit/fooddb_v1_2_manual_batch1_validation.csv"
SUMMARY_OUT = REPO_ROOT / "data/fooddb/audit/fooddb_v1_2_manual_batch1_summary.txt"

RECIPES_DRAFT_DIR = REPO_ROOT / "data/recipesdb/draft"
RECIPES_AUDIT_DIR = REPO_ROOT / "data/recipesdb/audit"
BASE_DATASET_DIR = RECIPES_DRAFT_DIR / "v1_2_generator_ready_round37_expanded_repaired"
BATCH1_DATASET_DIR = RECIPES_DRAFT_DIR / "v1_2_generator_ready_round37_expanded_repaired_manual_batch1"

OUT_AFFECTED_MATCHES = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_batch1_affected_food_matches.csv"
OUT_AFFECTED_NUTRITION = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_batch1_affected_nutrition_cache.csv"
OUT_MAPPING_AUDIT = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_batch1_mapping_audit.csv"
OUT_RECOVERED = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_batch1_recovered_recipes.csv"
OUT_MATERIALIZATION_SUMMARY = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_batch1_materialization_summary.txt"
OUT_MATERIALIZATION_AUDIT = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_batch1_materialization_audit.csv"

OUT_IMPACT_SUMMARY = RECIPES_AUDIT_DIR / "generator_v1_manual_batch1_impact_summary.txt"
OUT_IMPACT_DAYS = RECIPES_AUDIT_DIR / "generator_v1_manual_batch1_days.csv"
OUT_IMPACT_MEALS = RECIPES_AUDIT_DIR / "generator_v1_manual_batch1_meals.csv"
OUT_IMPACT_REPETITION = RECIPES_AUDIT_DIR / "generator_v1_manual_batch1_repetition.csv"

PROFILE_PATH = REPO_ROOT / "profiles/member_profile_demo_v1.json"

BATCH1_NUTRITION_BASIS = "recipes_v1_2_manual_batch1_verified_fooddb_draft"
BATCH1_CACHE_VERSION = "recipes_v1_2_manual_batch1_001"
BATCH1_TAG = "manual_batch1_verified_fooddb"

ROUND_INPUTS = [
    {
        "round": "round28",
        "recipes": RECIPES_DRAFT_DIR / "recipes_v1_2_round28_targeted_plus30.csv",
        "matches": RECIPES_DRAFT_DIR / "recipes_v1_2_round28_plus30_food_matches.csv",
    },
    {
        "round": "round30",
        "recipes": RECIPES_DRAFT_DIR / "recipes_v1_2_round30_targeted_plus15.csv",
        "matches": RECIPES_DRAFT_DIR / "recipes_v1_2_round30_plus15_food_matches.csv",
    },
    {
        "round": "round37_round38_repaired",
        "recipes": RECIPES_DRAFT_DIR / "recipes_v1_2_round37_targeted_plus100.csv",
        "matches": RECIPES_DRAFT_DIR / "recipes_v1_2_round38_repaired_food_matches.csv",
    },
]

SAFE_MAPPING_RULES = {
    "baby bok choy": "bok_choy_raw",
    "beef round steak": "beef_round_steak_raw",
    "canned refried beans": "refried_beans_canned_traditional",
    "cheddar cheese": "cheddar_cheese",
    "chicken thighs with skin": "chicken_thigh_meat_and_skin_raw",
    "cubed lamb meat": "lamb_cubed_stew_meat_raw",
    "dry pinto beans": "pinto_beans_dry",
    "great northern beans": "great_northern_beans_dry",
    "low fat sour cream": "sour_cream_light",
    "refried beans": "refried_beans_canned_traditional",
    "sour cream": "sour_cream_cultured",
    "swiss cheese": "swiss_cheese",
    "unsweetened oat milk": "oat_milk_unsweetened_plain",
    "plain unsweetened oat milk": "oat_milk_unsweetened_plain",
    "unsweetened soy milk": "soy_milk_unsweetened_plain",
    "plain unsweetened soy milk": "soy_milk_unsweetened_plain",
}

VALIDATION_COLUMNS = [
    "candidate_food_id",
    "canonical_name",
    "decision",
    "validation_status",
    "apply_status",
    "failure_reasons",
    "macro_kcal_estimate",
    "kcal_macro_delta",
    "kcal_macro_delta_ratio",
    "qc_notes",
]

MAPPING_AUDIT_COLUMNS = [
    "round",
    "recipe_id_candidate",
    "display_name",
    "ingredient_position",
    "ingredient_name_normalized",
    "ingredient_raw_text",
    "previous_mapping_status",
    "previous_food_id",
    "new_mapping_status",
    "new_food_id",
    "applied",
    "application_notes",
]

RECOVERED_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "source_round",
    "target_bucket",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "strong_generator_ready",
    "selected_for_dataset",
]


def main() -> None:
    RECIPES_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    (REPO_ROOT / "data/fooddb/audit").mkdir(parents=True, exist_ok=True)

    base_fooddb = read_csv(FOODDB_BASE)
    verified_rows = read_csv(VERIFIED_BATCH)
    validation_rows, safe_rows = validate_verified_rows(verified_rows, base_fooddb)
    batch_fooddb = base_fooddb + [fooddb_row_from_verified(row, base_fooddb) for row in safe_rows]
    write_csv(FOODDB_BATCH1, batch_fooddb, list(base_fooddb[0].keys()))
    write_csv(VALIDATION_OUT, validation_rows, VALIDATION_COLUMNS)

    food_lookup = round37.base.build_food_lookup(batch_fooddb)
    safe_by_canonical = {row["canonical_name"]: row for row in safe_rows}
    affected = rebuild_affected_recipes(food_lookup, safe_by_canonical)
    materialization = materialize_batch1_dataset(affected)
    impact_rows = evaluate_multiday_impact()

    SUMMARY_OUT.write_text(
        build_summary(validation_rows, safe_rows, affected, materialization, impact_rows),
        encoding="utf-8",
    )
    print("Food_DB v1.2 manual batch1 applied to draft/audit outputs")
    print(f"fooddb_draft={FOODDB_BATCH1}")
    print(f"safe_additions={len(safe_rows)}")
    print(f"recovered_recipes={len(materialization['recovered_ids'])}")
    print(f"impact_summary={OUT_IMPACT_SUMMARY}")


def validate_verified_rows(
    verified_rows: list[dict[str, str]],
    base_fooddb: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    existing_ids = {row.get("food_id", "") for row in base_fooddb}
    existing_canonicals = {row.get("canonical_name", "") for row in base_fooddb}
    validation_rows: list[dict[str, str]] = []
    safe_rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for row in verified_rows:
        decision = clean(row.get("decision"))
        reasons: list[str] = []
        macro_estimate = macro_kcal(row)
        kcal = parse_float(row.get("energy_kcal_100"))
        delta = abs(kcal - macro_estimate) if kcal is not None and macro_estimate is not None else None
        ratio = delta / max(kcal or 0.0, macro_estimate or 0.0, 1.0) if delta is not None else None

        if decision == "safe_to_add":
            reasons.extend(safe_row_failures(row, existing_ids, existing_canonicals, seen_ids, macro_estimate, kcal, ratio))
            if not reasons:
                safe_rows.append(row)
                seen_ids.add(clean(row.get("candidate_food_id")))
                status = "passed"
                apply_status = "applied_to_fooddb_draft"
            else:
                status = "failed"
                apply_status = "rejected_sanity"
        elif decision == "needs_review":
            status = "not_applied"
            apply_status = "kept_needs_review"
        elif decision == "keep_deferred":
            status = "not_applied"
            apply_status = "kept_deferred"
        else:
            status = "failed"
            apply_status = "rejected_unknown_decision"
            reasons.append("unknown_decision")

        validation_rows.append(
            {
                "candidate_food_id": row.get("candidate_food_id", ""),
                "canonical_name": row.get("canonical_name", ""),
                "decision": decision,
                "validation_status": status,
                "apply_status": apply_status,
                "failure_reasons": ";".join(reasons),
                "macro_kcal_estimate": format_float(macro_estimate),
                "kcal_macro_delta": format_float(delta),
                "kcal_macro_delta_ratio": format_float(ratio),
                "qc_notes": row.get("qc_notes", ""),
            }
        )

    return validation_rows, safe_rows


def safe_row_failures(
    row: dict[str, str],
    existing_ids: set[str],
    existing_canonicals: set[str],
    seen_ids: set[str],
    macro_estimate: float | None,
    kcal: float | None,
    ratio: float | None,
) -> list[str]:
    reasons: list[str] = []
    food_id = clean(row.get("candidate_food_id"))
    canonical = clean(row.get("canonical_name"))
    if not food_id:
        reasons.append("missing_candidate_food_id")
    if food_id in existing_ids or food_id in seen_ids:
        reasons.append("duplicate_food_id")
    if not canonical:
        reasons.append("missing_canonical_name")
    if canonical in existing_canonicals:
        reasons.append("canonical_already_exists_in_base_fooddb")
    if not clean(row.get("source_name")):
        reasons.append("missing_source_name")
    if not clean(row.get("source_url")):
        reasons.append("missing_source_url")
    if not clean(row.get("raw_or_cooked_state")):
        reasons.append("missing_raw_cooked_processed_state")
    for column in ["energy_kcal_100", "protein_g_100", "carbs_g_100", "fat_g_100"]:
        value = parse_float(row.get(column))
        if value is None:
            reasons.append(f"missing_{column}")
        elif value < 0:
            reasons.append(f"negative_{column}")
    if kcal is not None and macro_estimate is not None:
        if abs(kcal - macro_estimate) > 80 and (ratio or 0) > 0.25:
            reasons.append("kcal_not_plausible_vs_macro_estimate")
    return reasons


def fooddb_row_from_verified(row: dict[str, str], base_fooddb: list[dict[str, str]]) -> dict[str, str]:
    output = {column: "" for column in base_fooddb[0].keys()}
    role = clean(row.get("role"))
    group = clean(row.get("food_group"))
    state = clean(row.get("raw_or_cooked_state"))
    kcal = parse_float(row.get("energy_kcal_100")) or 0.0
    protein = parse_float(row.get("protein_g_100")) or 0.0
    carbs = parse_float(row.get("carbs_g_100")) or 0.0
    fat = parse_float(row.get("fat_g_100")) or 0.0
    output.update(
        {
            "food_id": clean(row.get("candidate_food_id")),
            "canonical_name": clean(row.get("canonical_name")),
            "display_name": clean(row.get("display_name")),
            "food_family_name": clean(row.get("display_name")),
            "entity_level": entity_level_for_state(state),
            "food_group": food_group_for_verified(group),
            "food_subgroup": food_subgroup_for_verified(group, state),
            "energy_kcal_100g": format_float(kcal),
            "protein_g_100g": format_float(protein),
            "carbs_g_100g": format_float(carbs),
            "fat_g_100g": format_float(fat),
            "processing_state": state,
            "preservation_state": "",
            "helper_macro_profile": helper_macro_profile(protein, carbs, fat),
            "helper_use_as_protein": str(protein >= 8 or role in {"protein", "protein_fat", "fat_protein", "carb_protein"}),
            "helper_use_as_carb_side": str(carbs >= 15 or role in {"carb", "carb_protein"}),
            "helper_use_as_veg_side": str(group == "vegetables"),
            "helper_is_sweet": "False",
            "helper_is_salty": str(group in {"processed_meat", "processed_grain", "sauce"}),
            "helper_is_drink": str(group == "plant_milk"),
            "helper_is_vegetarian": str(group not in {"meat_beef", "meat_lamb", "poultry", "processed_meat"}),
            "helper_is_vegan": str(group in {"legumes", "vegetables", "plant_milk", "processed_grain", "sauce"}),
            "helper_protein_bucket": protein_bucket(group, protein, fat),
            "helper_carb_bucket": carb_bucket(group, carbs),
            "helper_veg_bucket": "cruciferous" if "bok_choy" in clean(row.get("canonical_name")) else "",
            "primary_source_uid": clean(row.get("candidate_food_id")),
            "primary_source_name": clean(row.get("source_name")),
            "primary_source_ciqual_code": "",
            "primary_source_name_tags": clean(row.get("source_type")),
            "qc_macro_complete": "True",
            "qc_taxonomy_complete": "True",
            "qc_canonicalization_status": "manual_batch1_verified_source",
            "qc_scope_status": "accepted_core",
            "qc_source_merge_count": "1",
            "qc_notes": build_fooddb_qc_notes(row),
        }
    )
    return output


def rebuild_affected_recipes(
    food_lookup: dict[str, dict[str, Any]],
    safe_by_canonical: dict[str, dict[str, str]],
) -> dict[str, Any]:
    affected_match_rows: list[dict[str, Any]] = []
    mapping_audit_rows: list[dict[str, str]] = []
    affected_cache_rows: list[dict[str, Any]] = []
    affected_recipe_by_id: dict[str, dict[str, str]] = {}
    affected_mapping_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_round_by_id: dict[str, str] = {}

    for round_input in ROUND_INPUTS:
        recipes = read_csv(round_input["recipes"])
        recipes_by_id = {row["recipe_id_candidate"]: row for row in recipes}
        mapping_rows = read_csv(round_input["matches"])
        updated_rows, changed_ids, audit_rows = apply_batch1_mapping_rules(
            rows=mapping_rows,
            source_round=round_input["round"],
            safe_by_canonical=safe_by_canonical,
        )
        if not changed_ids:
            continue
        affected_recipes = [recipes_by_id[recipe_id] for recipe_id in sorted(changed_ids) if recipe_id in recipes_by_id]
        affected_mapping = [row for row in updated_rows if clean(row.get("recipe_id_candidate")) in changed_ids]
        cache_rows, _audit_rows = round37.build_cache_rows(affected_recipes, affected_mapping, food_lookup)
        cache_rows = [mark_batch1_cache(row) for row in cache_rows]

        affected_match_rows.extend(affected_mapping)
        mapping_audit_rows.extend(audit_rows)
        affected_cache_rows.extend(cache_rows)
        for recipe in affected_recipes:
            recipe_id = recipe["recipe_id_candidate"]
            affected_recipe_by_id[recipe_id] = recipe
            source_round_by_id[recipe_id] = round_input["round"]
        for row in affected_mapping:
            affected_mapping_by_id[clean(row.get("recipe_id_candidate"))].append(row)

    write_csv(OUT_AFFECTED_MATCHES, affected_match_rows, round37.MAPPING_COLUMNS)
    write_csv(OUT_AFFECTED_NUTRITION, affected_cache_rows, round37.CACHE_COLUMNS)
    write_csv(OUT_MAPPING_AUDIT, mapping_audit_rows, MAPPING_AUDIT_COLUMNS)

    return {
        "recipes_by_id": affected_recipe_by_id,
        "mapping_by_id": affected_mapping_by_id,
        "cache_rows": affected_cache_rows,
        "source_round_by_id": source_round_by_id,
        "mapping_audit_rows": mapping_audit_rows,
    }


def apply_batch1_mapping_rules(
    *,
    rows: list[dict[str, str]],
    source_round: str,
    safe_by_canonical: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], set[str], list[dict[str, str]]]:
    updated_rows: list[dict[str, Any]] = []
    affected_ids: set[str] = set()
    audit_rows: list[dict[str, str]] = []
    for row in rows:
        updated = dict(row)
        ingredient = normalize_ingredient(row.get("ingredient_name_normalized"))
        canonical = SAFE_MAPPING_RULES.get(ingredient, "")
        safe_row = safe_by_canonical.get(canonical)
        applied = False
        notes = ""
        if safe_row and clean(row.get("mapping_status")) != "accepted_auto":
            updated["mapped_food_id"] = safe_row["candidate_food_id"]
            updated["mapped_food_canonical_name"] = safe_row["canonical_name"]
            updated["mapping_status"] = "accepted_auto"
            updated["mapping_confidence"] = "high" if clean(safe_row.get("confidence")) == "high" else "medium"
            updated["mapping_method"] = "manual_batch1_verified_source_alias"
            updated["mapping_notes"] = "manual_batch1_verified_fooddb_item"
            updated["manual_decision_notes"] = append_note(
                clean(row.get("manual_decision_notes")),
                "manual_batch1_source_verified",
            )
            applied = True
            affected_ids.add(clean(row.get("recipe_id_candidate")))
            notes = "mapped_to_verified_batch1_fooddb_item"
        elif canonical and not safe_row:
            notes = "target_canonical_not_safe_to_add"
        if applied or canonical:
            audit_rows.append(
                {
                    "round": source_round,
                    "recipe_id_candidate": clean(row.get("recipe_id_candidate")),
                    "display_name": clean(row.get("display_name")),
                    "ingredient_position": clean(row.get("ingredient_position")),
                    "ingredient_name_normalized": ingredient,
                    "ingredient_raw_text": clean(row.get("ingredient_raw_text")),
                    "previous_mapping_status": clean(row.get("mapping_status")),
                    "previous_food_id": clean(row.get("mapped_food_id")),
                    "new_mapping_status": clean(updated.get("mapping_status")),
                    "new_food_id": clean(updated.get("mapped_food_id")),
                    "applied": str(applied).lower(),
                    "application_notes": notes,
                }
            )
        updated_rows.append(updated)
    return updated_rows, affected_ids, audit_rows


def materialize_batch1_dataset(affected: dict[str, Any]) -> dict[str, Any]:
    BATCH1_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    base_recipes = read_csv(BASE_DATASET_DIR / "recipes.csv")
    base_ingredients = read_csv(BASE_DATASET_DIR / "recipe_ingredients.csv")
    base_cache = read_csv(BASE_DATASET_DIR / "recipe_nutrition_cache.csv")
    existing_ids = {row.get("recipe_id", "") for row in base_recipes}
    cache_by_id = {row["recipe_id_candidate"]: row for row in affected["cache_rows"]}
    recovered_ids = {
        recipe_id
        for recipe_id, cache in cache_by_id.items()
        if cache.get("generator_ready_candidate") == "true" and recipe_id not in existing_ids
    }

    new_recipes = [
        materialized_recipe_row(affected["recipes_by_id"][recipe_id], cache_by_id[recipe_id], affected["source_round_by_id"].get(recipe_id, ""))
        for recipe_id in sorted(recovered_ids)
    ]
    new_ingredients = [
        materialized_ingredient_row(row)
        for recipe_id in sorted(recovered_ids)
        for row in affected["mapping_by_id"].get(recipe_id, [])
    ]
    new_cache = [
        materialized_cache_row(cache_by_id[recipe_id])
        for recipe_id in sorted(recovered_ids)
    ]

    write_csv(BATCH1_DATASET_DIR / "recipes.csv", base_recipes + new_recipes, list(base_recipes[0].keys()))
    write_csv(BATCH1_DATASET_DIR / "recipe_ingredients.csv", base_ingredients + new_ingredients, list(base_ingredients[0].keys()))
    write_csv(BATCH1_DATASET_DIR / "recipe_nutrition_cache.csv", base_cache + new_cache, list(base_cache[0].keys()))
    (BATCH1_DATASET_DIR / "README_v1_2_generator_ready_round37_expanded_repaired_manual_batch1.txt").write_text(
        "\n".join(
            [
                "Recipes_DB v1.2 generator-ready Round37 expanded repaired manual batch1 draft",
                "",
                "Draft/test only. Do not treat as current production data.",
                f"Base dataset: {BASE_DATASET_DIR}",
                f"Food_DB draft: {FOODDB_BATCH1}",
                f"Recovered ready additions: {len(recovered_ids)}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    recovered_rows = [
        recovered_audit_row(
            recipe_id=recipe_id,
            recipe=affected["recipes_by_id"][recipe_id],
            cache=cache_by_id[recipe_id],
            source_round=affected["source_round_by_id"].get(recipe_id, ""),
            selected=True,
        )
        for recipe_id in sorted(recovered_ids)
    ]
    ready_but_existing = [
        recovered_audit_row(
            recipe_id=recipe_id,
            recipe=affected["recipes_by_id"].get(recipe_id, {}),
            cache=cache,
            source_round=affected["source_round_by_id"].get(recipe_id, ""),
            selected=False,
        )
        for recipe_id, cache in sorted(cache_by_id.items())
        if cache.get("generator_ready_candidate") == "true" and recipe_id in existing_ids
    ]
    write_csv(OUT_RECOVERED, recovered_rows + ready_but_existing, RECOVERED_COLUMNS)
    write_csv(
        OUT_MATERIALIZATION_AUDIT,
        build_materialization_audit_rows(affected, recovered_ids, existing_ids),
        [
            "recipe_id_candidate",
            "display_name",
            "source_round",
            "generator_ready_candidate",
            "strong_generator_ready",
            "already_in_base_dataset",
            "materialized",
            "failure_reason",
        ],
    )
    OUT_MATERIALIZATION_SUMMARY.write_text(
        build_materialization_summary(base_recipes, recovered_ids, cache_by_id),
        encoding="utf-8",
    )
    return {
        "recovered_ids": recovered_ids,
        "base_count": len(base_recipes),
        "new_count": len(base_recipes) + len(new_recipes),
        "cache_by_id": cache_by_id,
    }


def evaluate_multiday_impact() -> list[dict[str, Any]]:
    scenarios = [
        {
            "scenario": "round38_repaired",
            "recipes": BASE_DATASET_DIR / "recipes.csv",
            "ingredients": BASE_DATASET_DIR / "recipe_ingredients.csv",
            "nutrition": BASE_DATASET_DIR / "recipe_nutrition_cache.csv",
            "fooddb": FOODDB_BASE,
        },
        {
            "scenario": "manual_batch1",
            "recipes": BATCH1_DATASET_DIR / "recipes.csv",
            "ingredients": BATCH1_DATASET_DIR / "recipe_ingredients.csv",
            "nutrition": BATCH1_DATASET_DIR / "recipe_nutrition_cache.csv",
            "fooddb": FOODDB_BATCH1,
        },
    ]
    rows: list[dict[str, Any]] = []
    day_rows: list[dict[str, Any]] = []
    meal_rows: list[dict[str, Any]] = []
    repetition_rows: list[dict[str, Any]] = []
    for scenario in scenarios:
        plan, diagnostics = run_multiday_scenario(scenario)
        row = summary_row(scenario, plan, diagnostics)
        rows.append(row)
        day_rows.extend(day_rows_for_scenario(scenario, plan))
        meal_rows.extend(meal_rows_for_scenario(scenario, plan))
        repetition_rows.extend(repetition_rows_for_scenario(scenario, plan))
    write_csv(OUT_IMPACT_DAYS, day_rows, list(day_rows[0].keys()) if day_rows else [])
    write_csv(OUT_IMPACT_MEALS, meal_rows, list(meal_rows[0].keys()) if meal_rows else [])
    write_csv(OUT_IMPACT_REPETITION, repetition_rows, list(repetition_rows[0].keys()) if repetition_rows else [])
    OUT_IMPACT_SUMMARY.write_text(build_impact_summary(rows), encoding="utf-8")
    return rows


def run_multiday_scenario(scenario: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    started = time.perf_counter()
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=scenario["recipes"],
        ingredients_path=scenario["ingredients"],
        nutrition_path=scenario["nutrition"],
        dataset_profile=V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE,
    )
    fooddb = load_fooddb_current(scenario["fooddb"])
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode="target_aware",
    )
    plan = generate_multi_day_plan(
        profile=profile,
        target=target,
        slot_candidates=slot_candidates,
        days=3,
        config=multi_day_config(),
    )
    diagnostics = {
        "runtime_seconds": round(time.perf_counter() - started, 3),
        "recipe_count": len(pool.recipes),
        "eligible_candidate_count": len(pool.eligible_candidates),
        "slot_candidate_count": len(slot_candidates),
    }
    return plan, diagnostics


def multi_day_config() -> dict[str, Any]:
    return {
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": 3,
        "return_alternatives": True,
        "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
        "no_repeat_policy": "hard",
        "multi_day_speed_mode": "fast",
        "day_candidate_builder": "direct_from_slots",
        "direct_slot_shortlist_size": 12,
        "day_candidate_pool_size_target": 50,
        "day_candidate_pool_max": 150,
        "include_slot_forced_variants": True,
    }


def materialized_recipe_row(recipe: dict[str, str], cache: dict[str, Any], source_round: str) -> dict[str, Any]:
    row = round37.materialized_recipe_row(recipe, cache)
    row["source_dataset"] = source_dataset_for_round(source_round)
    row["qc_notes"] = append_note(clean(row.get("qc_notes")), BATCH1_TAG)
    row["slot_policy_reason"] = append_note(clean(row.get("slot_policy_reason")), "manual_batch1_recovered")
    return row


def materialized_ingredient_row(row: dict[str, Any]) -> dict[str, Any]:
    output = round37.materialized_ingredient_row(row)
    output["qc_notes"] = append_note(clean(output.get("qc_notes")), BATCH1_TAG)
    output["qc_ingredient_status"] = (
        "accepted_auto_manual_batch1"
        if clean(row.get("mapping_method")) == "manual_batch1_verified_source_alias"
        else output.get("qc_ingredient_status", "")
    )
    return output


def materialized_cache_row(row: dict[str, Any]) -> dict[str, Any]:
    output = round37.materialized_cache_row(row)
    output["nutrition_basis"] = BATCH1_NUTRITION_BASIS
    output["cache_version"] = BATCH1_CACHE_VERSION
    output["qc_notes"] = append_note(clean(output.get("qc_notes")), BATCH1_TAG)
    return output


def mark_batch1_cache(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated["nutrition_basis"] = BATCH1_NUTRITION_BASIS
    updated["cache_version"] = BATCH1_CACHE_VERSION
    return updated


def source_dataset_for_round(source_round: str) -> str:
    if source_round == "round28":
        return "recipes_dataset_64k_dishes_round28_plus30_manual_batch1"
    if source_round == "round30":
        return "recipes_dataset_64k_dishes_round30_plus15_manual_batch1"
    return "recipes_dataset_64k_dishes_round37_plus100_manual_batch1"


def recovered_audit_row(
    *,
    recipe_id: str,
    recipe: dict[str, str],
    cache: dict[str, Any],
    source_round: str,
    selected: bool,
) -> dict[str, Any]:
    return {
        "recipe_id_candidate": recipe_id,
        "display_name": recipe.get("display_name", cache.get("display_name", "")),
        "source_round": source_round,
        "target_bucket": recipe.get("target_bucket", cache.get("target_bucket", "")),
        "energy_kcal_per_serving": cache.get("energy_kcal_per_serving", ""),
        "protein_g_per_serving": cache.get("protein_g_per_serving", ""),
        "carbs_g_per_serving": cache.get("carbs_g_per_serving", ""),
        "fat_g_per_serving": cache.get("fat_g_per_serving", ""),
        "mapped_weight_ratio": cache.get("mapped_weight_ratio", ""),
        "macro_relevant_mapped_weight_ratio": cache.get("macro_relevant_mapped_weight_ratio", ""),
        "strong_generator_ready": cache.get("strong_generator_ready", ""),
        "selected_for_dataset": str(selected).lower(),
    }


def build_materialization_audit_rows(
    affected: dict[str, Any],
    recovered_ids: set[str],
    existing_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for cache in affected["cache_rows"]:
        recipe_id = cache["recipe_id_candidate"]
        recipe = affected["recipes_by_id"].get(recipe_id, {})
        rows.append(
            {
                "recipe_id_candidate": recipe_id,
                "display_name": recipe.get("display_name", cache.get("display_name", "")),
                "source_round": affected["source_round_by_id"].get(recipe_id, ""),
                "generator_ready_candidate": cache.get("generator_ready_candidate", ""),
                "strong_generator_ready": cache.get("strong_generator_ready", ""),
                "already_in_base_dataset": str(recipe_id in existing_ids).lower(),
                "materialized": str(recipe_id in recovered_ids).lower(),
                "failure_reason": cache.get("generator_ready_failure_reason", ""),
            }
        )
    return rows


def build_materialization_summary(
    base_recipes: list[dict[str, str]],
    recovered_ids: set[str],
    cache_by_id: dict[str, dict[str, Any]],
) -> str:
    ready_count = sum(1 for row in cache_by_id.values() if row.get("generator_ready_candidate") == "true")
    strong_count = sum(1 for row in cache_by_id.values() if row.get("strong_generator_ready") == "true")
    return "\n".join(
        [
            "Recipes_DB v1.2 manual batch1 materialization summary",
            "",
            f"base_dataset={BASE_DATASET_DIR}",
            f"new_dataset={BATCH1_DATASET_DIR}",
            f"base_recipe_count={len(base_recipes)}",
            f"affected_recipe_count={len(cache_by_id)}",
            f"affected_ready_count={ready_count}",
            f"affected_strong_ready_count={strong_count}",
            f"recovered_ready_recipe_count={len(recovered_ids)}",
            f"new_recipe_count={len(base_recipes) + len(recovered_ids)}",
            f"recovered_recipe_ids={', '.join(sorted(recovered_ids))}",
            "",
        ]
    )


def summary_row(scenario: dict[str, Any], plan: dict[str, Any], diagnostics: dict[str, Any]) -> dict[str, Any]:
    summary = plan.get("multi_day_summary", {})
    return {
        "scenario": scenario["scenario"],
        "recipe_count": diagnostics["recipe_count"],
        "eligible_candidate_count": diagnostics["eligible_candidate_count"],
        "runtime_seconds": diagnostics["runtime_seconds"],
        "valid_day_count": summary.get("valid_day_count"),
        "accept_day_count": summary.get("accept_day_count"),
        "review_day_count": summary.get("review_day_count"),
        "unique_recipe_count": summary.get("unique_recipe_count"),
        "repeated_recipe_count": summary.get("repeated_recipe_count"),
        "repeated_recipe_ids": ";".join(summary.get("repeated_recipe_ids", [])),
        "multi_day_loss": plan.get("multi_day_loss"),
        "average_day_loss": summary.get("average_day_loss"),
        "strict_verdict": summary.get("multi_day_classification"),
    }


def day_rows_for_scenario(scenario: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        totals = day.get("day_totals", {})
        diagnostics = day.get("selector_diagnostics", {})
        rows.append(
            {
                "scenario": scenario["scenario"],
                "day_index": day.get("day_index"),
                "validation_status": day.get("validation_status"),
                "quality_gate_status": day.get("quality_gate_status"),
                "fallback_used": day.get("fallback_used"),
                "total_kcal": totals.get("total_kcal"),
                "total_protein_g": totals.get("total_protein_g"),
                "total_carbs_g": totals.get("total_carbs_g"),
                "total_fat_g": totals.get("total_fat_g"),
                "base_day_loss": diagnostics.get("base_day_loss"),
                "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
                "selected_meals": " | ".join(clean(meal.get("display_name")) for meal in day.get("selected_meals", [])),
            }
        )
    return rows


def meal_rows_for_scenario(scenario: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"scenario": scenario["scenario"], **row} for row in multi_day_meal_rows(plan)]


def repetition_rows_for_scenario(scenario: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
    repeated = plan.get("multi_day_summary", {}).get("repeated_recipe_ids", [])
    if not repeated:
        return [{"scenario": scenario["scenario"], "recipe_id": "", "repeat_count": 0}]
    rows = []
    for recipe_id in repeated:
        rows.append({"scenario": scenario["scenario"], "recipe_id": recipe_id, "repeat_count": 2})
    return rows


def build_impact_summary(rows: list[dict[str, Any]]) -> str:
    lines = ["Generator v1 manual batch1 impact summary", ""]
    for row in rows:
        lines.append(
            "- {scenario}: recipes={recipe_count}, valid={valid_day_count}, accept={accept_day_count}, "
            "review={review_day_count}, unique={unique_recipe_count}, repeated={repeated_recipe_count}, "
            "loss={multi_day_loss}, runtime_s={runtime_seconds}".format(**row)
        )
    return "\n".join(lines) + "\n"


def build_summary(
    validation_rows: list[dict[str, str]],
    safe_rows: list[dict[str, str]],
    affected: dict[str, Any],
    materialization: dict[str, Any],
    impact_rows: list[dict[str, Any]],
) -> str:
    validation_counts = Counter(row["apply_status"] for row in validation_rows)
    changed_mappings = [row for row in affected["mapping_audit_rows"] if row.get("applied") == "true"]
    return "\n".join(
        [
            "Food_DB v1.2 manual additions batch1 summary",
            "",
            f"- verified rows: {len(validation_rows)}",
            f"- safe additions applied to draft Food_DB: {len(safe_rows)}",
            f"- kept needs_review: {validation_counts.get('kept_needs_review', 0)}",
            f"- kept deferred: {validation_counts.get('kept_deferred', 0)}",
            f"- rejected by sanity: {validation_counts.get('rejected_sanity', 0)}",
            f"- affected mapping rows changed: {len(changed_mappings)}",
            f"- affected recipe count: {len(affected['cache_rows'])}",
            f"- recovered generator-ready recipes: {len(materialization['recovered_ids'])}",
            f"- new dataset recipe count: {materialization['new_count']}",
            "",
            "Recovered recipe ids",
            f"- {', '.join(sorted(materialization['recovered_ids'])) or 'none'}",
            "",
            "Multi-day impact",
            *[
                (
                    f"- {row['scenario']}: recipes={row['recipe_count']}, valid={row['valid_day_count']}, "
                    f"accept={row['accept_day_count']}, review={row['review_day_count']}, "
                    f"unique={row['unique_recipe_count']}, repeated={row['repeated_recipe_count']}, "
                    f"loss={row['multi_day_loss']}, runtime_s={row['runtime_seconds']}"
                )
                for row in impact_rows
            ],
            "",
            "Strict notes",
            "- data/fooddb/current and data/recipesdb/current were not modified.",
            "- needs_review and keep_deferred rows were not applied.",
            "- Nutrition values came only from the verified batch CSV.",
            "",
        ]
    )


def macro_kcal(row: dict[str, str]) -> float | None:
    protein = parse_float(row.get("protein_g_100"))
    carbs = parse_float(row.get("carbs_g_100"))
    fat = parse_float(row.get("fat_g_100"))
    if protein is None or carbs is None or fat is None:
        return None
    return protein * 4 + carbs * 4 + fat * 9


def entity_level_for_state(state: str) -> str:
    if any(token in state for token in ["processed", "canned", "cheese", "mix", "cultured", "shelf stable", "refrigerated"]):
        return "semi_atomic"
    return "atomic"


def food_group_for_verified(group: str) -> str:
    if group in {"meat_beef", "meat_lamb", "poultry", "processed_meat"}:
        return "meat, egg and fish"
    if group in {"legumes", "vegetables"}:
        return "fruits, vegetables, legumes and nuts"
    if group in {"dairy", "plant_milk"}:
        return "milk and milk products"
    if group == "processed_grain":
        return "cereal products"
    if group == "sauce":
        return "fats and sauces"
    return group or "other"


def food_subgroup_for_verified(group: str, state: str) -> str:
    if group in {"meat_beef", "meat_lamb", "poultry"}:
        return "raw meat"
    if group == "processed_meat":
        return "processed meat"
    if group == "legumes":
        return "legumes"
    if group == "vegetables":
        return "vegetables, raw" if "raw" in state else "vegetables"
    if group == "dairy":
        return "cheese or cream"
    if group == "plant_milk":
        return "plant-based dairy alternative"
    if group == "processed_grain":
        return "processed grain product"
    if group == "sauce":
        return "prepared sauce"
    return "review"


def helper_macro_profile(protein: float, carbs: float, fat: float) -> str:
    parts = []
    parts.append("protein_high" if protein >= 15 else "protein_moderate" if protein >= 5 else "protein_low")
    parts.append("carb_high" if carbs >= 40 else "carb_moderate" if carbs >= 10 else "carb_low")
    parts.append("fat_high" if fat >= 15 else "fat_moderate" if fat >= 5 else "fat_low")
    return ";".join(parts)


def protein_bucket(group: str, protein: float, fat: float) -> str:
    if group in {"meat_beef", "meat_lamb", "processed_meat"}:
        return "lean_red" if fat < 10 else "fatty_red"
    if group == "poultry":
        return "poultry"
    if group in {"dairy", "plant_milk"}:
        return "dairy_light" if fat < 10 else "dairy_fat"
    if protein >= 10:
        return "plant_protein"
    return ""


def carb_bucket(group: str, carbs: float) -> str:
    if group == "legumes":
        return "legume"
    if group == "processed_grain":
        return "bakery"
    if carbs >= 20:
        return "carb_source"
    return ""


def build_fooddb_qc_notes(row: dict[str, str]) -> str:
    parts = [
        BATCH1_TAG,
        f"source_url={clean(row.get('source_url'))}",
        f"confidence={clean(row.get('confidence'))}",
        clean(row.get("qc_notes")),
    ]
    return "; ".join(part for part in parts if part)


def source_blocked_next_batch(validation_rows: list[dict[str, str]]) -> str:
    needs_review = [row["canonical_name"] for row in validation_rows if row["apply_status"] == "kept_needs_review"]
    deferred = [row["canonical_name"] for row in validation_rows if row["apply_status"] == "kept_deferred"]
    return ", ".join(needs_review[:5] + deferred[:3])


def append_note(existing: str, note: str) -> str:
    if not existing:
        return note
    if note in existing:
        return existing
    return f"{existing}; {note}"


def normalize_ingredient(value: Any) -> str:
    return " ".join(clean(value).lower().replace("Â", "").split())


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def parse_float(value: Any) -> float | None:
    text = clean(value)
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def format_float(value: Any) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if abs(number - round(number)) < 0.000001:
        return str(int(round(number)))
    return f"{number:.6f}".rstrip("0").rstrip(".")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
