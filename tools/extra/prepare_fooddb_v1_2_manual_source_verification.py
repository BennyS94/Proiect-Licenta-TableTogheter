from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
RECIPES_AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
FOODDB_AUDIT_DIR = REPO_ROOT / "data" / "fooddb" / "audit"
FOODDB_DRAFT_DIR = REPO_ROOT / "data" / "fooddb" / "draft"

ROUND36_DEFERRED_PATH = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_repair_deferred.csv"
ROUND38_SOURCE_NEEDED_PATH = RECIPES_AUDIT_DIR / "recipes_v1_2_round38_manual_web_source_needed.csv"
MANUAL_REPAIR_QUEUE_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_queue.csv"
ROUND38_NUTRITION_AUDIT_PATH = RECIPES_AUDIT_DIR / "recipes_v1_2_round38_repaired_nutrition_audit.csv"
ROUND38_DATASET_RECIPES_PATH = (
    RECIPES_DRAFT_DIR / "v1_2_generator_ready_round37_expanded_repaired" / "recipes.csv"
)
FOODDB_PATH = FOODDB_DRAFT_DIR / "fooddb_v1_1_core_master_draft_round9.csv"

OUT_VERIFICATION_QUEUE = FOODDB_AUDIT_DIR / "fooddb_v1_2_manual_source_verification_queue.csv"
OUT_SUMMARY = FOODDB_AUDIT_DIR / "fooddb_v1_2_manual_source_verification_summary.txt"
OUT_SOURCE_BLOCKED_RECIPES = RECIPES_AUDIT_DIR / "recipes_v1_2_source_blocked_recipes.csv"
OUT_PREFILL = FOODDB_DRAFT_DIR / "fooddb_v1_2_manual_additions_candidates_prefill.csv"
OUT_CHECKLIST = FOODDB_AUDIT_DIR / "fooddb_v1_2_manual_source_checklist.txt"

QUEUE_COLUMNS = [
    "blocker_id",
    "ingredient_name_normalized",
    "ingredient_raw_examples",
    "affected_recipe_ids",
    "affected_recipe_names",
    "affected_recipe_count",
    "affected_rows",
    "total_grams_affected",
    "expected_role",
    "blocker_type",
    "recommended_action",
    "priority",
    "source_needed",
    "suggested_source_type",
    "suggested_search_query",
    "risk_notes",
    "decision_status",
]

PREFILL_COLUMNS = [
    "candidate_food_id",
    "canonical_name",
    "display_name",
    "food_group",
    "food_subgroup",
    "role",
    "energy_kcal_100",
    "protein_g_100",
    "carbs_g_100",
    "fat_g_100",
    "source_needed",
    "source_name",
    "source_url",
    "source_type",
    "added_reason",
    "linked_recipe_ids",
    "linked_ingredients",
    "qc_status",
    "qc_notes",
]

SOURCE_BLOCKED_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "blocked_by_ingredients",
    "blocker_priority",
    "likely_recoverable_after_source_verification",
    "expected_generator_value",
    "recommended_action",
]

HIGH_PRIORITY_OVERRIDES: dict[str, dict[str, str]] = {
    "beef round steak": {
        "role": "protein",
        "blocker_type": "missing_fooddb_item",
        "recommended_action": "add_fooddb_item",
        "source_type": "USDA",
        "canonical_name": "beef_round_steak_raw",
        "display_name": "Beef round steak, raw",
        "food_group": "meat, egg and fish",
        "food_subgroup": "raw meat",
        "query": "USDA FoodData Central beef round steak raw nutrition 100g",
        "risk": "Specific beef cut is missing locally; do not map generic beef automatically.",
    },
    "sour cream": {
        "role": "dairy",
        "blocker_type": "missing_fooddb_item",
        "recommended_action": "add_fooddb_item",
        "source_type": "CIQUAL",
        "canonical_name": "sour_cream",
        "display_name": "Sour cream",
        "food_group": "milk and milk products",
        "food_subgroup": "cream",
        "query": "CIQUAL sour cream nutrition 100g",
        "risk": "Dairy fat level must be source-backed; do not substitute yogurt or cream blindly.",
    },
    "pinto beans": {
        "role": "carb",
        "blocker_type": "missing_fooddb_item",
        "recommended_action": "add_fooddb_item",
        "source_type": "USDA",
        "canonical_name": "pinto_beans_dry",
        "display_name": "Pinto beans, dry",
        "food_group": "fruits, vegetables, legumes and nuts",
        "food_subgroup": "legumes",
        "query": "USDA pinto beans dry nutrition 100g",
        "risk": "Recipe wording suggests dry beans; cooked/canned state must be confirmed manually.",
    },
    "dry pinto beans": {
        "role": "carb",
        "blocker_type": "missing_fooddb_item",
        "recommended_action": "add_fooddb_item",
        "source_type": "USDA",
        "canonical_name": "pinto_beans_dry",
        "display_name": "Pinto beans, dry",
        "food_group": "fruits, vegetables, legumes and nuts",
        "food_subgroup": "legumes",
        "query": "USDA pinto beans dry nutrition 100g",
        "risk": "Dry bean macros are sourceable, but cooked yield should stay recipe-side.",
    },
    "great northern beans": {
        "role": "carb",
        "blocker_type": "missing_fooddb_item",
        "recommended_action": "add_fooddb_item",
        "source_type": "USDA",
        "canonical_name": "great_northern_beans_dry",
        "display_name": "Great northern beans, dry",
        "food_group": "fruits, vegetables, legumes and nuts",
        "food_subgroup": "legumes",
        "query": "USDA great northern beans dry nutrition 100g",
        "risk": "Dry/cooked state must be verified before applying to recipes.",
    },
    "chicken thighs with skin": {
        "role": "protein",
        "blocker_type": "missing_fooddb_item",
        "recommended_action": "add_fooddb_item",
        "source_type": "CIQUAL",
        "canonical_name": "chicken_thigh_meat_and_skin_raw",
        "display_name": "Chicken thigh, meat and skin, raw",
        "food_group": "meat, egg and fish",
        "food_subgroup": "raw meat",
        "query": "CIQUAL chicken thigh meat and skin raw nutrition 100g",
        "risk": "Needs raw/cooked and edible portion state; do not reuse boneless skinless thigh.",
    },
    "cubed lamb meat": {
        "role": "protein",
        "blocker_type": "ambiguous_generic_mapping",
        "recommended_action": "manual_decision",
        "source_type": "official_nutrition_database",
        "canonical_name": "lamb_meat_raw_review",
        "display_name": "Lamb meat, raw, review",
        "food_group": "meat, egg and fish",
        "food_subgroup": "raw meat",
        "query": "CIQUAL USDA lamb stew meat raw nutrition 100g",
        "risk": "Ingredient is useful but cut is vague; source verification must choose a defensible lamb cut.",
    },
}

MEDIUM_PRIORITY_OVERRIDES: dict[str, dict[str, str]] = {
    "oat milk": {
        "role": "dairy",
        "blocker_type": "processed_food_review",
        "recommended_action": "add_fooddb_item",
        "source_type": "official_nutrition_database",
        "canonical_name": "oat_milk_plain",
        "display_name": "Oat milk, plain",
        "food_group": "milk and milk products",
        "food_subgroup": "plant-based dairy alternative",
        "query": "oat milk plain nutrition 100g official database",
        "risk": "Plant milk varies by brand and fortification; source must state plain/unsweetened if used.",
    },
    "plain or vanilla soy milk": {
        "role": "dairy",
        "blocker_type": "processed_food_review",
        "recommended_action": "manual_decision",
        "source_type": "manufacturer_if_processed",
        "canonical_name": "soy_milk_plain_or_vanilla_review",
        "display_name": "Soy milk, plain or vanilla, review",
        "food_group": "milk and milk products",
        "food_subgroup": "plant-based dairy alternative",
        "query": "soy milk plain vanilla nutrition 100g official database",
        "risk": "Plain and vanilla sweetened variants differ; recipe wording needs manual decision.",
    },
    "milk or unsweetened plant milk of your choice": {
        "role": "dairy",
        "blocker_type": "ambiguous_generic_mapping",
        "recommended_action": "manual_decision",
        "source_type": "official_nutrition_database",
        "canonical_name": "plant_milk_unsweetened_review",
        "display_name": "Unsweetened plant milk, review",
        "food_group": "milk and milk products",
        "food_subgroup": "plant-based dairy alternative",
        "query": "unsweetened plant milk nutrition 100g official database",
        "risk": "Ingredient allows multiple foods; choose a recipe-side default before adding.",
    },
    "refrigerated pie pastry": {
        "role": "carb",
        "blocker_type": "processed_food_review",
        "recommended_action": "add_fooddb_item",
        "source_type": "manufacturer_if_processed",
        "canonical_name": "refrigerated_pie_pastry",
        "display_name": "Refrigerated pie pastry",
        "food_group": "cereal products",
        "food_subgroup": "pastry dough",
        "query": "refrigerated pie pastry nutrition 100g manufacturer",
        "risk": "Processed dough varies by brand and fat; use manufacturer or official processed-food source.",
    },
    "baked beans": {
        "role": "carb",
        "blocker_type": "processed_food_review",
        "recommended_action": "add_fooddb_item",
        "source_type": "official_nutrition_database",
        "canonical_name": "baked_beans_canned",
        "display_name": "Baked beans, canned",
        "food_group": "fruits, vegetables, legumes and nuts",
        "food_subgroup": "legumes, prepared",
        "query": "baked beans canned nutrition 100g official database",
        "risk": "Sugar/sauce levels vary; source should match canned baked beans.",
    },
    "biscuit baking mix": {
        "role": "carb",
        "blocker_type": "processed_food_review",
        "recommended_action": "add_fooddb_item",
        "source_type": "manufacturer_if_processed",
        "canonical_name": "biscuit_baking_mix",
        "display_name": "Biscuit baking mix",
        "food_group": "cereal products",
        "food_subgroup": "baking mix",
        "query": "biscuit baking mix nutrition 100g manufacturer",
        "risk": "Processed mix is brand-sensitive; keep as review until source is explicit.",
    },
    "guanciale": {
        "role": "protein",
        "blocker_type": "processed_food_review",
        "recommended_action": "add_fooddb_item",
        "source_type": "official_nutrition_database",
        "canonical_name": "guanciale",
        "display_name": "Guanciale",
        "food_group": "meat, egg and fish",
        "food_subgroup": "processed meat",
        "query": "guanciale nutrition 100g official database",
        "risk": "Cured pork product; salt/fat may vary and should stay source-backed.",
    },
}

KEEP_DEFERRED_INGREDIENTS = {
    "beef",
    "pork",
    "turkey",
    "green onion",
    "green onions",
    "meaty ham bone",
    "ham bone",
    "vegetable oil for frying",
    "cooking spray",
    "olive oil cooking spray",
    "piece ginger",
    "ginger root",
    "hot water",
    "onion salt",
    "avocado",
    "sottocenere",
    "rosemary",
    "low fat vegetable broth",
}

ROLE_KEYWORDS = [
    ("beef", "protein"),
    ("lamb", "protein"),
    ("chicken", "protein"),
    ("ham", "protein"),
    ("guanciale", "protein"),
    ("bean", "carb"),
    ("pastry", "carb"),
    ("biscuit", "carb"),
    ("milk", "dairy"),
    ("cream", "dairy"),
    ("cheese", "dairy"),
    ("oil", "fat"),
    ("spray", "fat"),
    ("avocado", "fat"),
    ("ginger", "seasoning"),
    ("rosemary", "seasoning"),
    ("salt", "seasoning"),
    ("broth", "sauce"),
]


def main() -> None:
    FOODDB_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    FOODDB_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    RECIPES_AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    repair_queue = read_csv(MANUAL_REPAIR_QUEUE_PATH)
    deferred_rows = read_csv(ROUND36_DEFERRED_PATH)
    source_needed_rows = read_csv(ROUND38_SOURCE_NEEDED_PATH)
    nutrition_rows = read_csv(ROUND38_NUTRITION_AUDIT_PATH)
    ready_recipe_rows = read_csv(ROUND38_DATASET_RECIPES_PATH)
    fooddb_rows = read_csv(FOODDB_PATH)

    queue_lookup = build_repair_lookup(repair_queue)
    nutrition_lookup = {row.get("recipe_id_candidate", ""): row for row in nutrition_rows}
    ready_recipe_ids = {row.get("recipe_id", "") for row in ready_recipe_rows}
    existing_food_ids = {row.get("food_id", "") for row in fooddb_rows}

    blocker_sources = collect_blocker_sources(
        deferred_rows=deferred_rows,
        source_needed_rows=source_needed_rows,
        queue_lookup=queue_lookup,
    )
    blocker_rows = build_blocker_rows(blocker_sources)
    source_blocked_rows = build_source_blocked_rows(blocker_rows, queue_lookup, nutrition_lookup, ready_recipe_ids)
    prefill_rows = build_prefill_rows(blocker_rows, existing_food_ids)

    write_csv(OUT_VERIFICATION_QUEUE, blocker_rows, QUEUE_COLUMNS)
    write_csv(OUT_SOURCE_BLOCKED_RECIPES, source_blocked_rows, SOURCE_BLOCKED_COLUMNS)
    write_csv(OUT_PREFILL, prefill_rows, PREFILL_COLUMNS)
    OUT_SUMMARY.write_text(
        build_summary(blocker_rows, source_blocked_rows, prefill_rows),
        encoding="utf-8",
    )
    OUT_CHECKLIST.write_text(
        build_checklist(blocker_rows),
        encoding="utf-8",
    )
    print("Food_DB v1.2 manual source verification package written")
    print(f"queue={OUT_VERIFICATION_QUEUE}")
    print(f"summary={OUT_SUMMARY}")
    print(f"checklist={OUT_CHECKLIST}")


def collect_blocker_sources(
    deferred_rows: list[dict[str, str]],
    source_needed_rows: list[dict[str, str]],
    queue_lookup: dict[tuple[str, str], dict[str, str]],
) -> list[dict[str, str]]:
    collected: dict[tuple[str, str], dict[str, str]] = {}

    for row in deferred_rows:
        decision = row.get("repair_decision", "")
        if decision == "reject":
            continue
        if not row.get("recipe_id_candidate") or not row.get("ingredient_name_normalized"):
            continue
        if decision and not decision.startswith("defer"):
            continue
        ingredient = normalize_ingredient(row.get("ingredient_name_normalized") or row.get("blocking_ingredient"))
        key = (row.get("recipe_id_candidate", ""), ingredient)
        collected[key] = merge_source_row(
            base=collected.get(key),
            row=row,
            ingredient=ingredient,
            source="deferred_repair_queue",
            queue_lookup=queue_lookup,
        )

    for row in source_needed_rows:
        recipe_id = row.get("recipe_id_candidate", "")
        ingredient = normalize_ingredient(row.get("blocking_ingredient"))
        if not recipe_id or not ingredient:
            continue
        key = (recipe_id, ingredient)
        collected[key] = merge_source_row(
            base=collected.get(key),
            row=row,
            ingredient=ingredient,
            source="round38_source_needed",
            queue_lookup=queue_lookup,
        )

    return sorted(collected.values(), key=lambda row: (row.get("ingredient_name_normalized", ""), row.get("recipe_id_candidate", "")))


def merge_source_row(
    base: dict[str, str] | None,
    row: dict[str, str],
    ingredient: str,
    source: str,
    queue_lookup: dict[tuple[str, str], dict[str, str]],
) -> dict[str, str]:
    recipe_id = row.get("recipe_id_candidate", "")
    queue_row = queue_lookup.get((recipe_id, ingredient), {})
    merged = dict(base or {})
    merged["recipe_id_candidate"] = recipe_id
    merged["display_name"] = first_nonempty(merged.get("display_name"), row.get("display_name"), queue_row.get("display_name"))
    merged["ingredient_name_normalized"] = ingredient
    merged["blocking_ingredient"] = first_nonempty(row.get("blocking_ingredient"), queue_row.get("blocking_ingredient"), ingredient)
    merged["ingredient_raw_text"] = first_nonempty(
        merged.get("ingredient_raw_text"),
        row.get("ingredient_raw_text"),
        queue_row.get("ingredient_raw_text"),
        row.get("blocking_ingredient"),
    )
    merged["current_quantity_grams_estimated"] = first_nonempty(
        merged.get("current_quantity_grams_estimated"),
        row.get("current_quantity_grams_estimated"),
        queue_row.get("current_quantity_grams_estimated"),
    )
    merged["expected_generator_value"] = first_nonempty(
        merged.get("expected_generator_value"),
        row.get("expected_generator_value"),
        queue_row.get("expected_generator_value"),
    )
    merged["target_bucket"] = first_nonempty(merged.get("target_bucket"), row.get("target_bucket"), queue_row.get("target_bucket"))
    merged["problem_type"] = first_nonempty(merged.get("problem_type"), row.get("problem_type"), queue_row.get("problem_type"))
    merged["proposed_fix_type"] = first_nonempty(
        merged.get("proposed_fix_type"),
        row.get("proposed_fix_type"),
        queue_row.get("proposed_fix_type"),
    )
    merged["reason"] = first_nonempty(merged.get("reason"), row.get("reason"), row.get("repair_reason"), queue_row.get("qc_notes"))
    merged["source_rows"] = join_unique([merged.get("source_rows", ""), source])
    return merged


def build_blocker_rows(blocker_sources: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in blocker_sources:
        grouped[row["ingredient_name_normalized"]].append(row)

    blocker_rows: list[dict[str, str]] = []
    for index, ingredient in enumerate(sorted(grouped), start=1):
        rows = grouped[ingredient]
        classification = classify_blocker(ingredient, rows)
        recipe_ids = sorted_unique(row.get("recipe_id_candidate", "") for row in rows)
        recipe_names = sorted_unique(row.get("display_name", "") for row in rows)
        raw_examples = sorted_unique(row.get("ingredient_raw_text", "") for row in rows)
        total_grams = sum(parse_float(row.get("current_quantity_grams_estimated")) for row in rows)
        blocker_rows.append(
            {
                "blocker_id": f"fooddb_v1_2_source_blocker_{index:03d}",
                "ingredient_name_normalized": ingredient,
                "ingredient_raw_examples": " | ".join(raw_examples[:5]),
                "affected_recipe_ids": " | ".join(recipe_ids),
                "affected_recipe_names": " | ".join(recipe_names),
                "affected_recipe_count": str(len(recipe_ids)),
                "affected_rows": str(len(rows)),
                "total_grams_affected": format_float(total_grams),
                "expected_role": classification["role"],
                "blocker_type": classification["blocker_type"],
                "recommended_action": classification["recommended_action"],
                "priority": classification["priority"],
                "source_needed": "true" if classification["source_needed"] else "false",
                "suggested_source_type": classification["source_type"],
                "suggested_search_query": classification["query"],
                "risk_notes": classification["risk"],
                "decision_status": "pending",
            }
        )
    return blocker_rows


def classify_blocker(ingredient: str, rows: list[dict[str, str]]) -> dict[str, Any]:
    if ingredient in HIGH_PRIORITY_OVERRIDES:
        return with_priority(HIGH_PRIORITY_OVERRIDES[ingredient], "high", source_needed=True)
    if ingredient in MEDIUM_PRIORITY_OVERRIDES:
        return with_priority(MEDIUM_PRIORITY_OVERRIDES[ingredient], "medium", source_needed=True)
    if ingredient in KEEP_DEFERRED_INGREDIENTS:
        return {
            "role": infer_role(ingredient),
            "blocker_type": infer_keep_deferred_type(ingredient),
            "recommended_action": "keep_deferred",
            "priority": "low",
            "source_needed": False,
            "source_type": "keep_deferred",
            "query": "",
            "risk": "Generic, yield-sensitive, brand-sensitive, or low-impact blocker; not safe for source prefill yet.",
        }

    problem_types = {row.get("problem_type", "") for row in rows}
    fix_types = {row.get("proposed_fix_type", "") for row in rows}
    role = infer_role(ingredient)
    if "fooddb_addition" in fix_types:
        return {
            "role": role,
            "blocker_type": "missing_fooddb_item",
            "recommended_action": "add_fooddb_item",
            "priority": "medium",
            "source_needed": True,
            "source_type": suggested_source_type_for_role(role, ingredient),
            "query": default_query(ingredient, role),
            "risk": "Food_DB item appears missing, but exact state/source still needs manual verification.",
        }
    if "unit_rule" in fix_types or "unit_or_mapping_gap" in problem_types:
        return {
            "role": role,
            "blocker_type": "needs_unit_rule",
            "recommended_action": "manual_decision",
            "priority": "low",
            "source_needed": False,
            "source_type": "keep_deferred",
            "query": "",
            "risk": "Unit or edible-yield rule is not safe to apply automatically.",
        }
    return {
        "role": role,
        "blocker_type": "missing_fooddb_item",
        "recommended_action": "manual_decision",
        "priority": "medium",
        "source_needed": True,
        "source_type": suggested_source_type_for_role(role, ingredient),
        "query": default_query(ingredient, role),
        "risk": "Potentially useful blocker, but not in the high-confidence list yet.",
    }


def with_priority(base: dict[str, str], priority: str, source_needed: bool) -> dict[str, Any]:
    return {
        "role": base["role"],
        "blocker_type": base["blocker_type"],
        "recommended_action": base["recommended_action"],
        "priority": priority,
        "source_needed": source_needed,
        "source_type": base["source_type"],
        "query": base["query"],
        "risk": base["risk"],
    }


def build_source_blocked_rows(
    blocker_rows: list[dict[str, str]],
    queue_lookup: dict[tuple[str, str], dict[str, str]],
    nutrition_lookup: dict[str, dict[str, str]],
    ready_recipe_ids: set[str],
) -> list[dict[str, str]]:
    recipe_blockers: dict[str, list[dict[str, str]]] = defaultdict(list)
    for blocker in blocker_rows:
        for recipe_id in split_joined(blocker["affected_recipe_ids"]):
            recipe_blockers[recipe_id].append(blocker)

    rows: list[dict[str, str]] = []
    for recipe_id in sorted(recipe_blockers):
        blockers = recipe_blockers[recipe_id]
        queue_rows = [row for (candidate_id, _), row in queue_lookup.items() if candidate_id == recipe_id]
        display_name = first_nonempty(
            *(row.get("display_name") for row in queue_rows),
            *(blocker.get("affected_recipe_names", "").split(" | ")[0] for blocker in blockers),
            nutrition_lookup.get(recipe_id, {}).get("display_name"),
        )
        priority = highest_priority(blocker.get("priority", "low") for blocker in blockers)
        expected_values = sorted_unique(row.get("expected_generator_value", "") for row in queue_rows)
        failure_reason = nutrition_lookup.get(recipe_id, {}).get("generator_ready_failure_reason", "")
        sourceable = any(
            blocker.get("source_needed") == "true"
            and blocker.get("priority") in {"high", "medium"}
            and blocker.get("recommended_action") in {"add_fooddb_item", "manual_decision", "alias_to_existing"}
            for blocker in blockers
        )
        likely_recoverable = (
            recipe_id not in ready_recipe_ids
            and sourceable
            and any(token in failure_reason for token in ["mapped_weight_ratio", "accepted_auto", "protein", "kcal", "carbs"])
        )
        recommended_action = "manual_source_verification" if likely_recoverable else "keep_deferred_or_low_priority"
        rows.append(
            {
                "recipe_id_candidate": recipe_id,
                "display_name": display_name,
                "blocked_by_ingredients": " | ".join(blocker["ingredient_name_normalized"] for blocker in blockers),
                "blocker_priority": priority,
                "likely_recoverable_after_source_verification": "true" if likely_recoverable else "false",
                "expected_generator_value": " | ".join(expected_values),
                "recommended_action": recommended_action,
            }
        )
    return rows


def build_prefill_rows(blocker_rows: list[dict[str, str]], existing_food_ids: set[str]) -> list[dict[str, str]]:
    source_blockers: dict[str, list[dict[str, str]]] = defaultdict(list)
    for blocker in blocker_rows:
        if blocker["source_needed"] != "true":
            continue
        if blocker["recommended_action"] != "add_fooddb_item":
            continue
        if blocker["priority"] not in {"high", "medium"}:
            continue
        details = prefill_details(blocker["ingredient_name_normalized"], blocker)
        source_blockers[details["canonical_name"]].append(blocker)

    rows: list[dict[str, str]] = []
    used_ids: set[str] = set()
    for canonical_name in sorted(source_blockers):
        blockers = source_blockers[canonical_name]
        details = prefill_details(blockers[0]["ingredient_name_normalized"], blockers[0])
        candidate_id = make_candidate_food_id(details["canonical_name"])
        candidate_id = avoid_id_conflict(candidate_id, existing_food_ids, used_ids)
        used_ids.add(candidate_id)
        linked_recipe_ids = join_unique([blocker["affected_recipe_ids"] for blocker in blockers])
        linked_ingredients = join_unique([blocker["ingredient_name_normalized"] for blocker in blockers])
        rows.append(
            {
                "candidate_food_id": candidate_id,
                "canonical_name": details["canonical_name"],
                "display_name": details["display_name"],
                "food_group": details["food_group"],
                "food_subgroup": details["food_subgroup"],
                "role": blockers[0]["expected_role"],
                "energy_kcal_100": "",
                "protein_g_100": "",
                "carbs_g_100": "",
                "fat_g_100": "",
                "source_needed": "true",
                "source_name": "",
                "source_url": "",
                "source_type": blockers[0]["suggested_source_type"],
                "added_reason": f"Round39 source verification for {linked_ingredients}",
                "linked_recipe_ids": linked_recipe_ids,
                "linked_ingredients": linked_ingredients,
                "qc_status": "pending",
                "qc_notes": "Macros intentionally blank; fill only from verified source.",
            }
        )
    return rows


def prefill_details(ingredient: str, blocker: dict[str, str]) -> dict[str, str]:
    details = HIGH_PRIORITY_OVERRIDES.get(ingredient) or MEDIUM_PRIORITY_OVERRIDES.get(ingredient)
    if details:
        return details
    role = blocker["expected_role"]
    canonical_name = normalize_for_id(ingredient)
    return {
        "canonical_name": canonical_name,
        "display_name": ingredient.replace("_", " ").title(),
        "food_group": default_food_group(role),
        "food_subgroup": default_food_subgroup(role),
    }


def build_summary(
    blocker_rows: list[dict[str, str]],
    source_blocked_rows: list[dict[str, str]],
    prefill_rows: list[dict[str, str]],
) -> str:
    high = [row for row in blocker_rows if row["priority"] == "high"]
    medium = [row for row in blocker_rows if row["priority"] == "medium"]
    low = [row for row in blocker_rows if row["priority"] == "low"]
    source_needed = [row for row in blocker_rows if row["source_needed"] == "true"]
    likely_recoverable = [row for row in source_blocked_rows if row["likely_recoverable_after_source_verification"] == "true"]

    lines = [
        "Food_DB v1.2 manual source verification summary",
        "",
        f"- total blockers: {len(blocker_rows)}",
        f"- high priority blockers: {len(high)}",
        f"- medium priority blockers: {len(medium)}",
        f"- keep-deferred / low priority blockers: {len(low)}",
        f"- source-needed blockers: {len(source_needed)}",
        f"- Food_DB prefill candidates: {len(prefill_rows)}",
        f"- source-blocked recipes: {len(source_blocked_rows)}",
        f"- likely recoverable recipes after source verification: {len(likely_recoverable)}",
        "",
        "High priority source items",
    ]
    lines.extend(format_blocker_lines(high))
    lines.extend(["", "Medium priority source items"])
    lines.extend(format_blocker_lines(medium))
    lines.extend(["", "Keep deferred / risky items"])
    lines.extend(format_blocker_lines(low))
    lines.extend(
        [
            "",
            "Strict notes",
            "- No nutrition values were filled in this round.",
            "- No Food_DB additions were applied.",
            "- Generic meat mappings remain deferred.",
            "- Processed foods require source-specific verification before use.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_checklist(blocker_rows: list[dict[str, str]]) -> str:
    high_rows = [row for row in blocker_rows if row["priority"] == "high" and row["source_needed"] == "true"]
    medium_rows = [row for row in blocker_rows if row["priority"] == "medium" and row["source_needed"] == "true"]
    lines = [
        "Food_DB v1.2 manual source checklist",
        "",
        "For each item, fill only source-backed values:",
        "- energy_kcal_100",
        "- protein_g_100",
        "- carbs_g_100",
        "- fat_g_100",
        "- source name",
        "- source URL",
        "- raw/cooked/processed state",
        "- edible portion notes if relevant",
        "",
        "High priority",
    ]
    lines.extend(checklist_lines(high_rows))
    lines.extend(["", "Medium priority"])
    lines.extend(checklist_lines(medium_rows))
    lines.extend(
        [
            "",
            "Do not fill values for generic beef, ham bone, frying oil, cooking spray, or vague ingredients until a separate modeling decision exists.",
        ]
    )
    return "\n".join(lines) + "\n"


def checklist_lines(rows: list[dict[str, str]]) -> list[str]:
    if not rows:
        return ["- none"]
    lines: list[str] = []
    for row in rows:
        lines.extend(
            [
                f"- {row['ingredient_name_normalized']}",
                f"  - why needed: {row['risk_notes']}",
                f"  - affected recipes: {row['affected_recipe_names']}",
                f"  - source type: {row['suggested_source_type']}",
                f"  - search query: {row['suggested_search_query']}",
                "  - data to capture: kcal/protein/carbs/fat per 100g, source name, URL, state, edible portion notes",
            ]
        )
    return lines


def format_blocker_lines(rows: list[dict[str, str]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        (
            f"- {row['ingredient_name_normalized']}: recipes={row['affected_recipe_count']}, "
            f"grams={row['total_grams_affected']}, action={row['recommended_action']}, "
            f"source={row['suggested_source_type']}"
        )
        for row in rows
    ]


def build_repair_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    lookup: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        recipe_id = row.get("recipe_id_candidate", "")
        ingredient = normalize_ingredient(row.get("ingredient_name_normalized") or row.get("blocking_ingredient"))
        if recipe_id and ingredient:
            lookup[(recipe_id, ingredient)] = row
    return lookup


def infer_role(ingredient: str) -> str:
    for token, role in ROLE_KEYWORDS:
        if token in ingredient:
            return role
    return "other"


def infer_keep_deferred_type(ingredient: str) -> str:
    if ingredient in {"beef", "pork", "turkey"}:
        return "ambiguous_generic_mapping"
    if "bone" in ingredient or "thigh" in ingredient or "lamb" in ingredient:
        return "needs_edible_yield"
    if "spray" in ingredient or "for frying" in ingredient or "piece " in ingredient:
        return "needs_unit_rule"
    if ingredient in {"sottocenere"}:
        return "processed_food_review"
    return "low_priority"


def suggested_source_type_for_role(role: str, ingredient: str) -> str:
    if role == "protein":
        return "USDA"
    if role in {"dairy", "carb", "veg", "fruit"}:
        return "CIQUAL"
    if role in {"processed", "sauce"} or any(token in ingredient for token in ["mix", "pastry", "beans"]):
        return "manufacturer_if_processed"
    return "official_nutrition_database"


def default_query(ingredient: str, role: str) -> str:
    source = suggested_source_type_for_role(role, ingredient)
    source_name = "official nutrition database" if source == "official_nutrition_database" else source
    return f"{source_name} {ingredient} nutrition 100g"


def default_food_group(role: str) -> str:
    if role == "protein":
        return "meat, egg and fish"
    if role == "dairy":
        return "milk and milk products"
    if role in {"carb", "veg", "fruit"}:
        return "fruits, vegetables, legumes and nuts"
    if role == "fat":
        return "fats and oils"
    return "other"


def default_food_subgroup(role: str) -> str:
    if role == "protein":
        return "raw meat"
    if role == "dairy":
        return "dairy or alternative"
    if role == "carb":
        return "legumes or cereal products"
    if role == "fat":
        return "oil or fat"
    return "review"


def highest_priority(values: Any) -> str:
    order = {"high": 3, "medium": 2, "low": 1}
    best = "low"
    for value in values:
        if order.get(value, 0) > order.get(best, 0):
            best = value
    return best


def make_candidate_food_id(canonical_name: str) -> str:
    return f"food_v1_2_candidate_{normalize_for_id(canonical_name)}"


def avoid_id_conflict(candidate_id: str, existing_ids: set[str], used_ids: set[str]) -> str:
    if candidate_id not in existing_ids and candidate_id not in used_ids:
        return candidate_id
    index = 2
    while f"{candidate_id}_{index}" in existing_ids or f"{candidate_id}_{index}" in used_ids:
        index += 1
    return f"{candidate_id}_{index}"


def normalize_ingredient(value: str | None) -> str:
    cleaned = (value or "").strip().lower()
    cleaned = cleaned.replace("Â", "")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def normalize_for_id(value: str) -> str:
    cleaned = normalize_ingredient(value)
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    return cleaned.strip("_") or "unknown"


def first_nonempty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def parse_float(value: str | None) -> float:
    try:
        if value is None or str(value).strip() == "":
            return 0.0
        return float(str(value).strip())
    except ValueError:
        return 0.0


def format_float(value: float) -> str:
    if abs(value - round(value)) < 0.0001:
        return str(int(round(value)))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def sorted_unique(values: Any) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def join_unique(values: list[str]) -> str:
    joined: list[str] = []
    for value in values:
        for item in split_joined(value):
            if item not in joined:
                joined.append(item)
    return " | ".join(joined)


def split_joined(value: str) -> list[str]:
    return [item.strip() for item in (value or "").split("|") if item.strip()]


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


if __name__ == "__main__":
    main()
