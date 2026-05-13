from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]

RECIPES = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round28_targeted_plus30.csv"
PARSED = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round28_plus30_ingredients_parsed.csv"
FOODDB = REPO_ROOT / "data/fooddb/draft/fooddb_v1_1_core_master_draft_round9.csv"
OLD_DATASET_DIR = (
    REPO_ROOT
    / "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated_plus10"
)
PLUS30_DATASET_DIR = (
    REPO_ROOT
    / "data/recipesdb/draft/v1_2_generator_ready_plus30"
)

OUT_UNIT_RULES = (
    REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round28_plus30_ingredients_unit_rules.csv"
)
OUT_MATCHES = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round28_plus30_food_matches.csv"
OUT_UNMAPPED = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round28_plus30_unmapped.csv"
OUT_MAPPING_SUMMARY = (
    REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round28_plus30_mapping_summary.txt"
)
OUT_MAPPING_REVIEW = (
    REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round28_plus30_mapping_review.csv"
)
OUT_MAPPING_QUALITY_AUDIT = (
    REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round28_plus30_mapping_quality_audit.csv"
)
OUT_CACHE = (
    REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round28_plus30_nutrition_cache.csv"
)
OUT_NUTRITION_SUMMARY = (
    REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round28_plus30_nutrition_summary.txt"
)
OUT_NUTRITION_AUDIT = (
    REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round28_plus30_nutrition_audit.csv"
)
OUT_GENERATOR_READY_AUDIT = (
    REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round28_plus30_generator_ready_audit.csv"
)
OUT_MATERIALIZATION_SUMMARY = (
    REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round28_plus30_materialization_summary.txt"
)
OUT_MATERIALIZATION_AUDIT = (
    REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round28_plus30_materialization_audit.csv"
)

ROUND28_TAG = "round28_plus30_targeted_expansion"
PLUS30_SCOPE_STATUS = "v1_2_generator_ready_draft"
PLUS30_NUTRITION_BASIS = "recipes_v1_2_round28_plus30_mapped_ingredients_draft"
PLUS30_CACHE_VERSION = "recipes_v1_2_round28_plus30_001"

MAPPING_COLUMNS = [
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "ingredient_position",
    "ingredient_raw_text",
    "ingredient_name_parsed",
    "ingredient_name_normalized",
    "quantity_value",
    "quantity_unit",
    "quantity_grams_estimated",
    "parse_status",
    "fooddb_version_used",
    "mapped_food_id",
    "mapped_food_canonical_name",
    "mapping_status",
    "mapping_confidence",
    "mapping_method",
    "mapping_notes",
    "edible_yield_factor",
    "uses_pilot_edible_yield",
    "edible_yield_reason",
    "manual_decision_notes",
]

ROUND28_CACHE_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "recipe_kind_guess",
    "target_bucket",
    "primary_protein",
    "nutrition_basis",
    "servings_basis",
    "uses_pilot_servings_fallback",
    "servings_estimation_method",
    "servings_estimation_reasons",
    "total_weight_grams_estimated",
    "mapped_weight_grams",
    "macro_relevant_mapped_weight_grams",
    "low_or_no_macro_mapped_weight_grams",
    "known_weight_grams_sum",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "energy_kcal_total",
    "protein_g_total",
    "carbs_g_total",
    "fat_g_total",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "ingredient_count",
    "accepted_auto_count",
    "accepted_auto_with_grams_count",
    "review_needed_count",
    "review_needed_with_grams_count",
    "unmapped_count",
    "unmapped_with_grams_count",
    "cache_status",
    "quality_flags",
    "generator_ready_candidate",
    "generator_ready_failure_reason",
    "cache_version",
]

NUTRITION_AUDIT_COLUMNS = ROUND28_CACHE_COLUMNS + [
    "selected_for_plus30_dataset",
]

MAPPING_QUALITY_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "target_bucket",
    "accepted_auto_count",
    "accepted_auto_with_grams_count",
    "review_needed_with_grams_count",
    "unmapped_with_grams_count",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "key_protein_mapped",
    "key_carb_mapped",
    "mapping_quality_status",
    "mapping_quality_flags",
]

MATERIALIZATION_AUDIT_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "target_bucket",
    "generator_ready_candidate",
    "materialized",
    "generator_ready_failure_reason",
]

LOW_OR_NO_MACRO_FOOD_IDS = {
    "food_water_municipal",
    "food_salt_white_sea_igneous_or_rock_no_enrichment",
    "food_black_pepper_powder",
    "food_oregano_dried",
    "food_basil_dried",
    "food_thyme_dried",
    "food_parsley_dried",
    "food_chicken_broth_ready_to_serve",
}

ROUND28_ALIAS_FOOD_IDS = {
    "salt": "food_salt_white_sea_igneous_or_rock_no_enrichment",
    "kosher salt": "food_salt_white_sea_igneous_or_rock_no_enrichment",
    "ground black pepper": "food_black_pepper_powder",
    "black pepper": "food_black_pepper_powder",
    "freshly ground black pepper": "food_black_pepper_powder",
    "olive oil": "food_olive_oil_extra_virgin",
    "vegetable oil": "food_combined_oil_blended_vegetable_oils",
    "butter": "food_butter_82_fat_unsalted",
    "unsalted butter": "food_butter_82_fat_unsalted",
    "garlic": "food_garlic_fresh",
    "garlic clove": "food_garlic_fresh",
    "garlic cloves": "food_garlic_fresh",
    "onion": "food_onion_raw",
    "onions": "food_onion_raw",
    "yellow onion": "food_yellow_onion_raw",
    "red onion": "food_red_onion_raw",
    "carrot": "food_carrot_raw",
    "carrots": "food_carrot_raw",
    "celery": "food_celery_stalk_raw",
    "broccoli": "food_broccoli_raw",
    "mushrooms": "food_button_mushroom_or_cultivated_mushroom_raw",
    "mushroom": "food_button_mushroom_or_cultivated_mushroom_raw",
    "zucchini": "food_courgette_or_zucchini_pulp_and_peel_raw",
    "tomato": "food_tomato_raw",
    "tomatoes": "food_tomato_raw",
    "potato": "food_potato_peeled_raw",
    "potatoes": "food_potato_peeled_raw",
    "rice": "food_rice_raw",
    "white rice": "food_rice_raw",
    "long grain rice": "food_rice_raw",
    "brown rice": "food_rice_brown_raw",
    "basmati rice": "food_basmati_rice_raw",
    "pasta": "food_dried_pasta_raw",
    "spaghetti": "food_dried_pasta_raw",
    "penne pasta": "food_dried_pasta_raw",
    "macaroni": "food_dried_pasta_raw",
    "orzo pasta": "food_dried_pasta_raw",
    "egg": "food_egg_raw",
    "eggs": "food_egg_raw",
    "chicken": "food_chicken_meat_raw",
    "chicken breast": "food_chicken_breast_without_skin_raw",
    "chicken breasts": "food_chicken_breast_without_skin_raw",
    "boneless chicken breasts": "food_chicken_breast_without_skin_raw",
    "ground beef": "food_beef_minced_steak_15_fat_raw",
    "lean ground beef": "food_beef_minced_steak_10_fat_raw",
    "beef": "food_beef_stewing_meat_raw",
    "sirloin steak": "food_beef_sirloin_steak_raw",
    "beef sirloin": "food_beef_sirloin_steak_raw",
    "pork tenderloin": "food_pork_tenderloin_lean_raw",
    "pork chop": "food_pork_chop_raw",
    "pork chops": "food_pork_chop_raw",
    "pork": "food_pork_loin_raw",
    "ground turkey": "food_turkey_meat_raw",
    "turkey": "food_turkey_meat_raw",
    "tuna": "food_tuna_plain_canned_drained",
    "canned tuna": "food_tuna_plain_canned_drained",
    "salmon": "food_salmon_canned_drained",
    "canned salmon": "food_salmon_canned_drained",
    "chickpeas": "food_chick_pea_boiled_cooked_in_water",
    "chickpea": "food_chick_pea_boiled_cooked_in_water",
    "lentils": "food_lentil_boiled_cooked_in_water",
    "lentil": "food_lentil_boiled_cooked_in_water",
    "kidney beans": "food_red_kidney_bean_boiled_cooked_in_water",
    "beans": "food_haricot_bean_boiled_cooked_in_water",
    "parmesan cheese": "food_parmesan_cheese_hard",
    "mozzarella cheese": "food_mozzarella_cheese_from_cow_s_milk",
    "milk": "food_milk_semi_skimmed_pasteurised",
    "chicken broth": "food_chicken_broth_ready_to_serve",
    "water": "food_water_municipal",
    "white onion": "food_onion_raw",
    "broccoli florets": "food_broccoli_raw",
    "fresh basil": "food_basil_fresh",
    "basil leaves": "food_basil_fresh",
    "oregano": "food_oregano_dried",
    "cabbage": "food_white_cabbage_raw",
    "red potatoes": "food_potato_peeled_raw",
    "tomato sauce": "food_basque_style_sauce_or_tomato_sauce_with_sweet_peppers_prepacked",
    "spaghetti sauce": "food_basque_style_sauce_or_tomato_sauce_with_sweet_peppers_prepacked",
    "pasta sauce": "food_basque_style_sauce_or_tomato_sauce_with_sweet_peppers_prepacked",
    "green peppers": "food_sweet_pepper_green_raw",
    "bell peppers": "food_sweet_pepper_green_yellow_or_red_raw",
    "tops and seeds removed": "food_sweet_pepper_green_raw",
    "heavy whipping cream": "food_thick_cream_30_fat_refrigerated",
    "all purpose flour": "food_wheat_flour_white_all_purpose_enriched_unbleached",
    "salted butter": "food_butter_82_fat_unsalted",
    "celery salt": "food_salt_white_sea_igneous_or_rock_no_enrichment",
    "garlic powder": "food_garlic_fresh",
    "oats": "food_oat_raw",
    "oat": "food_oat_raw",
    "rolled oats": "food_oat_raw",
    "old fashioned oats": "food_oat_raw",
    "old-fashioned oats": "food_oat_raw",
    "quick oats": "food_oat_raw",
    "quick-cooking oats": "food_oat_raw",
    "oatmeal": "food_oat_raw",
    "banana": "food_banana_pulp_raw",
    "bananas": "food_banana_pulp_raw",
    "apple": "food_apple_pulp_and_peel_raw",
    "apples": "food_apple_pulp_and_peel_raw",
    "strawberries": "food_strawberry_raw",
    "strawberry": "food_strawberry_raw",
    "blueberries": "food_blueberry_raw",
    "blueberry": "food_blueberry_raw",
    "raspberries": "food_raspberry_raw",
    "raspberry": "food_raspberry_raw",
    "honey": "food_honey",
    "maple syrup": "food_syrup_maple",
    "peanut butter": "food_peanut_butter_or_peanut_paste",
    "peanuts": "food_peanut",
    "peanut": "food_peanut",
    "almonds": "food_almond_with_peel",
    "almond": "food_almond_with_peel",
    "walnuts": "food_walnut_dried_husked",
    "walnut": "food_walnut_dried_husked",
    "yogurt": "food_yogurt_fermented_milk_or_dairy_specialty_plain",
    "plain yogurt": "food_yogurt_fermented_milk_or_dairy_specialty_plain",
    "greek yogurt": "food_yogurt_greek_style_plain",
    "bread": "food_bread_french_bread_baguette_or_ball_with_yeast",
    "toast": "food_bread_french_bread_baguette_or_ball_with_yeast",
    "whole wheat bread": "food_bread_wholemeal_or_integral_bread_made_with_flour_type_150",
    "tortilla": "food_wheat_tortilla_wrap_to_be_filled",
    "flour tortilla": "food_wheat_tortilla_wrap_to_be_filled",
    "corn tortilla": "food_corn_tortilla_wrap_to_be_filled",
    "all-purpose flour": "food_wheat_flour_white_all_purpose_enriched_unbleached",
    "dijon mustard": "food_mustard",
    "brown sugar": "food_sugar_brown",
    "green beans": "food_green_beans_cooked_unsalted",
    "cannellini beans": "food_haricot_bean_canned_drained",
    "black beans": "food_haricot_bean_canned_drained",
    "cherry tomatoes": "food_tomato_raw",
    "red pepper flakes": "food_chili_pepper_raw",
    "red lentils": "food_lentil_pink_or_red_dried",
}

CONTAINS_ALIAS_FOOD_IDS = [
    ("old fashioned oats", "food_oat_raw"),
    ("old-fashioned oats", "food_oat_raw"),
    ("rolled oats", "food_oat_raw"),
    ("quick oats", "food_oat_raw"),
    ("quick-cooking oats", "food_oat_raw"),
    ("oatmeal", "food_oat_raw"),
    ("greek yogurt", "food_yogurt_greek_style_plain"),
    ("plain yogurt", "food_yogurt_fermented_milk_or_dairy_specialty_plain"),
    ("maple syrup", "food_syrup_maple"),
    ("peanut butter", "food_peanut_butter_or_peanut_paste"),
    ("whole wheat bread", "food_bread_wholemeal_or_integral_bread_made_with_flour_type_150"),
    ("flour tortilla", "food_wheat_tortilla_wrap_to_be_filled"),
    ("corn tortilla", "food_corn_tortilla_wrap_to_be_filled"),
    ("chicken breast", "food_chicken_breast_without_skin_raw"),
    ("ground beef", "food_beef_minced_steak_15_fat_raw"),
    ("lean ground beef", "food_beef_minced_steak_10_fat_raw"),
    ("ground turkey", "food_turkey_meat_raw"),
    ("pork tenderloin", "food_pork_tenderloin_lean_raw"),
    ("pork chop", "food_pork_chop_raw"),
    ("sirloin steak", "food_beef_sirloin_steak_raw"),
    ("canned tuna", "food_tuna_plain_canned_drained"),
    ("canned salmon", "food_salmon_canned_drained"),
    ("brown rice", "food_rice_brown_raw"),
    ("basmati rice", "food_basmati_rice_raw"),
    ("red lentils", "food_lentil_pink_or_red_dried"),
    ("black bean", "food_haricot_bean_canned_drained"),
    ("cannellini", "food_haricot_bean_canned_drained"),
    ("tomato sauce", "food_basque_style_sauce_or_tomato_sauce_with_sweet_peppers_prepacked"),
    ("spaghetti sauce", "food_basque_style_sauce_or_tomato_sauce_with_sweet_peppers_prepacked"),
    ("green pepper", "food_sweet_pepper_green_raw"),
    ("bell pepper", "food_sweet_pepper_green_yellow_or_red_raw"),
    ("green beans", "food_green_beans_cooked_unsalted"),
    ("broccoli floret", "food_broccoli_raw"),
    ("rice", "food_rice_raw"),
    ("pasta", "food_dried_pasta_raw"),
    ("spaghetti", "food_dried_pasta_raw"),
    ("penne", "food_dried_pasta_raw"),
    ("macaroni", "food_dried_pasta_raw"),
    ("potato", "food_potato_peeled_raw"),
    ("banana", "food_banana_pulp_raw"),
    ("strawberr", "food_strawberry_raw"),
    ("blueberr", "food_blueberry_raw"),
    ("raspberr", "food_raspberry_raw"),
    ("bread", "food_bread_french_bread_baguette_or_ball_with_yeast"),
    ("tortilla", "food_wheat_tortilla_wrap_to_be_filled"),
    ("lentil", "food_lentil_boiled_cooked_in_water"),
    ("chickpea", "food_chick_pea_boiled_cooked_in_water"),
    ("kidney bean", "food_red_kidney_bean_boiled_cooked_in_water"),
]

UNIT_GRAMS = {
    ("rice", "cup"): 185.0,
    ("oats", "cup"): 80.0,
    ("oat", "cup"): 80.0,
    ("oatmeal", "cup"): 80.0,
    ("rolled oats", "cup"): 80.0,
    ("quick oats", "cup"): 80.0,
    ("pasta", "cup"): 100.0,
    ("spaghetti", "cup"): 100.0,
    ("lentils", "cup"): 198.0,
    ("beans", "cup"): 175.0,
    ("chickpeas", "cup"): 164.0,
    ("tomato sauce", "cup"): 245.0,
    ("onion", "cup"): 160.0,
    ("carrot", "cup"): 128.0,
    ("broccoli", "cup"): 90.0,
    ("mushrooms", "cup"): 70.0,
    ("zucchini", "cup"): 124.0,
    ("tomato", "cup"): 150.0,
    ("cherry tomatoes", "cup"): 150.0,
    ("green beans", "cup"): 125.0,
    ("milk", "cup"): 244.0,
    ("yogurt", "cup"): 245.0,
    ("greek yogurt", "cup"): 245.0,
    ("banana", "cup"): 225.0,
    ("strawberries", "cup"): 150.0,
    ("blueberries", "cup"): 148.0,
    ("raspberries", "cup"): 123.0,
    ("heavy whipping cream", "cup"): 240.0,
    ("sour cream", "cup"): 240.0,
    ("parmesan cheese", "cup"): 100.0,
    ("mozzarella cheese", "cup"): 112.0,
    ("lentils", "cup"): 192.0,
    ("red lentils", "cup"): 192.0,
    ("olive oil", "tablespoon"): 13.5,
    ("vegetable oil", "tablespoon"): 13.5,
    ("butter", "tablespoon"): 14.2,
    ("parmesan cheese", "tablespoon"): 5.0,
    ("all purpose flour", "tablespoon"): 8.0,
    ("dijon mustard", "tablespoon"): 15.0,
    ("brown sugar", "tablespoon"): 13.8,
    ("honey", "tablespoon"): 21.0,
    ("maple syrup", "tablespoon"): 20.0,
    ("peanut butter", "tablespoon"): 16.0,
    ("oats", "tablespoon"): 5.0,
    ("olive oil", "teaspoon"): 4.5,
    ("vegetable oil", "teaspoon"): 4.5,
    ("butter", "teaspoon"): 4.7,
    ("salted butter", "teaspoon"): 4.7,
    ("black pepper", "teaspoon"): 2.3,
    ("garlic powder", "teaspoon"): 3.1,
    ("oregano", "teaspoon"): 1.0,
    ("thyme", "teaspoon"): 0.9,
    ("turmeric", "teaspoon"): 3.0,
    ("red pepper flakes", "teaspoon"): 0.5,
}


def main() -> None:
    recipes = read_csv(RECIPES)
    parsed_rows = read_csv(PARSED)
    food_rows = read_csv(FOODDB)
    food_lookup = build_food_lookup(food_rows)

    unit_rows = apply_round28_unit_rules(parsed_rows)
    mapping_rows, unmapped_rows = build_mapping_rows(unit_rows, food_lookup)
    cache_rows, audit_rows = build_cache_rows(recipes, mapping_rows, food_lookup)
    ready_ids = {
        row["recipe_id_candidate"]
        for row in cache_rows
        if row["generator_ready_candidate"] == "true"
    }
    materialize_plus30_dataset(recipes, mapping_rows, cache_rows, ready_ids)

    write_csv(OUT_UNIT_RULES, unit_rows, list(unit_rows[0].keys()) if unit_rows else [])
    write_csv(OUT_MATCHES, mapping_rows, MAPPING_COLUMNS)
    write_csv(OUT_UNMAPPED, unmapped_rows, MAPPING_COLUMNS)
    write_csv(OUT_MAPPING_REVIEW, [row for row in mapping_rows if row["mapping_status"] != "accepted_auto"], MAPPING_COLUMNS)
    write_csv(OUT_MAPPING_QUALITY_AUDIT, build_mapping_quality_rows(recipes, mapping_rows), MAPPING_QUALITY_COLUMNS)
    write_csv(OUT_CACHE, cache_rows, ROUND28_CACHE_COLUMNS)
    write_csv(OUT_NUTRITION_AUDIT, audit_rows, NUTRITION_AUDIT_COLUMNS)
    write_csv(OUT_GENERATOR_READY_AUDIT, audit_rows, NUTRITION_AUDIT_COLUMNS)
    write_csv(OUT_MATERIALIZATION_AUDIT, build_materialization_audit_rows(recipes, cache_rows, ready_ids), MATERIALIZATION_AUDIT_COLUMNS)
    OUT_MAPPING_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_MAPPING_SUMMARY.write_text(build_mapping_summary(mapping_rows), encoding="utf-8")
    OUT_NUTRITION_SUMMARY.write_text(build_nutrition_summary(cache_rows), encoding="utf-8")
    OUT_MATERIALIZATION_SUMMARY.write_text(
        build_materialization_summary(recipes, ready_ids),
        encoding="utf-8",
    )

    print("Round28 +30 unit rules, mapping, nutrition cache and materialization written")
    print(f"unit_rules={OUT_UNIT_RULES}")
    print(f"matches={OUT_MATCHES}")
    print(f"unmapped={OUT_UNMAPPED}")
    print(f"mapping_review={OUT_MAPPING_REVIEW}")
    print(f"mapping_quality_audit={OUT_MAPPING_QUALITY_AUDIT}")
    print(f"mapping_summary={OUT_MAPPING_SUMMARY}")
    print(f"cache={OUT_CACHE}")
    print(f"nutrition_summary={OUT_NUTRITION_SUMMARY}")
    print(f"nutrition_audit={OUT_NUTRITION_AUDIT}")
    print(f"generator_ready_audit={OUT_GENERATOR_READY_AUDIT}")
    print(f"materialization_summary={OUT_MATERIALIZATION_SUMMARY}")
    print(f"materialization_audit={OUT_MATERIALIZATION_AUDIT}")
    print(f"generator_ready_count={len(ready_ids)}")
    print(f"plus30_dataset={PLUS30_DATASET_DIR}")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_food_lookup(food_rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in food_rows:
        food_id = clean_text(row.get("food_id"))
        if not food_id:
            continue
        lookup[food_id] = {
            "canonical_name": clean_text(row.get("canonical_name")),
            "energy": to_float(row.get("energy_kcal_100g")),
            "protein": to_float(row.get("protein_g_100g")),
            "carbs": to_float(row.get("carbs_g_100g")),
            "fat": to_float(row.get("fat_g_100g")),
        }
    return lookup


def apply_round28_unit_rules(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    result = []
    for row in rows:
        updated = dict(row)
        old_grams = to_optional_float(row.get("quantity_grams_estimated"))
        if old_grams is None:
            grams, method = infer_missing_grams(row)
            if grams is not None:
                updated["quantity_grams_estimated"] = format_number(grams)
                updated["grams_estimation_method"] = method
                notes = clean_text(updated.get("parse_notes"))
                updated["parse_notes"] = append_note(notes, "round28_unit_rule_applied")
        result.append(updated)
    return result


def infer_missing_grams(row: dict[str, str]) -> tuple[float | None, str]:
    quantity = to_optional_float(row.get("quantity_value"))
    unit = clean_text(row.get("quantity_unit")).lower()
    name = normalize_text(row.get("ingredient_name_normalized"))
    raw = normalize_text(row.get("ingredient_raw_text"))
    if quantity is None or quantity <= 0 or not unit:
        return None, ""
    count_based = infer_count_based_grams(quantity, unit, name, raw)
    if count_based is not None:
        grams, method = count_based
        return grams, method
    packaged = infer_packaged_grams(quantity, unit, name, raw)
    if packaged is not None:
        grams, method = packaged
        return grams, method
    for (term, unit_key), grams_per_unit in UNIT_GRAMS.items():
        if unit == unit_key and (term in name or term in raw):
            return round(quantity * grams_per_unit, 2), f"round28_{term}_{unit_key}_rule"
    return None, ""


def infer_count_based_grams(
    quantity: float,
    unit: str,
    name: str,
    raw: str,
) -> tuple[float, str] | None:
    text = f"{name} {raw}"
    if unit == "clove" and "garlic" in text:
        return round(quantity * 3.0, 2), "round28_garlic_clove_rule"
    if unit in {"count", "head"}:
        if "chicken breast" in text:
            return round(quantity * 150.0, 2), "round28_chicken_breast_count_rule"
        if "potato" in text:
            grams_each = 150.0 if "small" in text else 170.0
            return round(quantity * grams_each, 2), "round28_potato_count_rule"
        if "carrot" in text:
            grams_each = 70.0 if "large" in text else 60.0
            return round(quantity * grams_each, 2), "round28_carrot_count_rule"
        if "onion" in text:
            grams_each = 75.0 if "half" in raw or quantity < 1 else 110.0
            return round(quantity * grams_each, 2), "round28_onion_count_rule"
        if "pepper" in text and ("green" in text or "bell" in text):
            return round(quantity * 120.0, 2), "round28_green_pepper_count_rule"
        if "cabbage" in text:
            return round(quantity * 900.0, 2), "round28_cabbage_head_rule"
        if "egg" in text:
            return round(quantity * 50.0, 2), "round28_egg_count_rule"
        if "lemon" in text:
            return round(quantity * 58.0, 2), "round28_lemon_count_rule"
        if "banana" in text:
            return round(quantity * 118.0, 2), "round28_banana_count_rule"
        if "apple" in text:
            return round(quantity * 180.0, 2), "round28_apple_count_rule"
        if "slice" in text and "bread" in text:
            return round(quantity * 35.0, 2), "round28_bread_slice_count_rule"
        if "tortilla" in text:
            return round(quantity * 45.0, 2), "round28_tortilla_count_rule"
    return None


def infer_packaged_grams(
    quantity: float,
    unit: str,
    name: str,
    raw: str,
) -> tuple[float, str] | None:
    if unit not in {"can", "cans", "package", "packages", "jar", "jars"}:
        return None
    text = f"{name} {raw}"
    if unit in {"can", "cans"} and "beans" in text and "drained" in raw:
        return round(quantity * 255.0, 2), "round28_drained_bean_can_rule"
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(?:fluid\s+)?ounce", raw)
    if not match:
        return None
    ounce_value = float(match.group(1))
    return round(quantity * ounce_value * 28.3495, 2), "round28_package_ounce_rule"


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


def map_ingredient(
    row: dict[str, object],
    food_lookup: dict[str, dict[str, Any]],
) -> dict[str, str]:
    parse_status = clean_text(row.get("parse_status"))
    name = normalize_text(row.get("ingredient_name_normalized"))
    raw = normalize_text(row.get("ingredient_raw_text"))
    if parse_status == "failed_parse":
        return mapping_result("", "", "unmapped", "low", "parse_failed", "round28_parse_failed")

    food_id = food_id_for_name(name, raw)
    if food_id and food_id in food_lookup:
        return mapping_result(
            food_id,
            food_lookup[food_id]["canonical_name"],
            "accepted_auto",
            "high",
            "round28_existing_fooddb_alias",
            "round28_targeted_existing_fooddb_item",
        )
    if food_id and food_id not in food_lookup:
        return mapping_result(
            food_id,
            "",
            "review_needed",
            "low",
            "round28_alias_missing_fooddb_item",
            "target_food_id_not_found",
        )
    if parse_status == "review_needed":
        return mapping_result("", "", "review_needed", "low", "parse_review_needed", "")
    return mapping_result("", "", "unmapped", "low", "no_round28_safe_alias", "")


def food_id_for_name(name: str, raw: str) -> str:
    if "cooked rice" in raw:
        return "food_rice_cooked_unsalted"
    if "cooked pasta" in raw:
        return "food_dried_pasta_cooked_unsalted"
    if "tomato sauce" in name or "tomato sauce" in raw or "spaghetti sauce" in raw:
        return "food_basque_style_sauce_or_tomato_sauce_with_sweet_peppers_prepacked"
    for term, food_id in CONTAINS_ALIAS_FOOD_IDS:
        if term in name or term in raw:
            return food_id
    if name in ROUND28_ALIAS_FOOD_IDS:
        return ROUND28_ALIAS_FOOD_IDS[name]
    return ""


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
        "manual_notes": "round28_no_fooddb_rows_added",
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
        totals = nutrition_totals(rows, food_lookup)
        coverage = coverage_totals(rows)
        servings = estimate_servings(recipe, coverage["known_weight_grams_sum"])
        energy_per = totals["energy"] / servings if servings else 0.0
        protein_per = totals["protein"] / servings if servings else 0.0
        carbs_per = totals["carbs"] / servings if servings else 0.0
        fat_per = totals["fat"] / servings if servings else 0.0
        mapped_ratio = safe_ratio(totals["mapped_weight"], coverage["known_weight_grams_sum"])
        macro_ratio = safe_ratio(
            totals["macro_relevant_mapped_weight"],
            coverage["known_weight_grams_sum"],
        )
        cache_status = cache_status_for_recipe(
            coverage,
            macro_ratio,
            energy_per,
        )
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
            "nutrition_basis": PLUS30_NUTRITION_BASIS,
            "servings_basis": format_number(servings),
            "uses_pilot_servings_fallback": "True",
            "servings_estimation_method": "round28_bucket_default",
            "servings_estimation_reasons": f"{recipe.get('target_bucket', '')}_default_servings",
            "total_weight_grams_estimated": format_number(coverage["known_weight_grams_sum"]),
            "mapped_weight_grams": format_number(totals["mapped_weight"]),
            "macro_relevant_mapped_weight_grams": format_number(
                totals["macro_relevant_mapped_weight"]
            ),
            "low_or_no_macro_mapped_weight_grams": format_number(
                totals["low_or_no_macro_mapped_weight"]
            ),
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
            "accepted_auto_with_grams_count": str(
                int(coverage["accepted_auto_with_grams_count"])
            ),
            "review_needed_count": str(int(coverage["review_needed_count"])),
            "review_needed_with_grams_count": str(
                int(coverage["review_needed_with_grams_count"])
            ),
            "unmapped_count": str(int(coverage["unmapped_count"])),
            "unmapped_with_grams_count": str(int(coverage["unmapped_with_grams_count"])),
            "cache_status": cache_status,
            "quality_flags": ";".join(quality_flags),
            "generator_ready_candidate": str(ready).lower(),
            "generator_ready_failure_reason": ";".join(failure_reasons),
            "cache_version": PLUS30_CACHE_VERSION,
        }
        cache_rows.append(row)
        audit_row = dict(row)
        audit_row["selected_for_plus30_dataset"] = str(ready).lower()
        audit_rows.append(audit_row)
    return cache_rows, audit_rows


def nutrition_totals(
    rows: list[dict[str, object]],
    food_lookup: dict[str, dict[str, Any]],
) -> dict[str, float]:
    totals = {
        "energy": 0.0,
        "protein": 0.0,
        "carbs": 0.0,
        "fat": 0.0,
        "mapped_weight": 0.0,
        "macro_relevant_mapped_weight": 0.0,
        "low_or_no_macro_mapped_weight": 0.0,
    }
    for row in rows:
        if clean_text(row.get("mapping_status")) != "accepted_auto":
            continue
        grams = to_optional_float(row.get("quantity_grams_estimated"))
        food_id = clean_text(row.get("mapped_food_id"))
        if grams is None or grams <= 0 or food_id not in food_lookup:
            continue
        macros = food_lookup[food_id]
        totals["energy"] += grams * float(macros["energy"]) / 100
        totals["protein"] += grams * float(macros["protein"]) / 100
        totals["carbs"] += grams * float(macros["carbs"]) / 100
        totals["fat"] += grams * float(macros["fat"]) / 100
        totals["mapped_weight"] += grams
        if food_id in LOW_OR_NO_MACRO_FOOD_IDS:
            totals["low_or_no_macro_mapped_weight"] += grams
        else:
            totals["macro_relevant_mapped_weight"] += grams
    return totals


def coverage_totals(rows: list[dict[str, object]]) -> dict[str, float]:
    coverage = {
        "ingredient_count": 0.0,
        "accepted_auto_count": 0.0,
        "accepted_auto_with_grams_count": 0.0,
        "review_needed_count": 0.0,
        "review_needed_with_grams_count": 0.0,
        "unmapped_count": 0.0,
        "unmapped_with_grams_count": 0.0,
        "known_weight_grams_sum": 0.0,
    }
    for row in rows:
        status = clean_text(row.get("mapping_status"))
        grams = to_optional_float(row.get("quantity_grams_estimated"))
        coverage["ingredient_count"] += 1
        if grams is not None and grams > 0:
            coverage["known_weight_grams_sum"] += grams
        if status == "accepted_auto":
            coverage["accepted_auto_count"] += 1
            if grams is not None and grams > 0:
                coverage["accepted_auto_with_grams_count"] += 1
        elif status == "review_needed":
            coverage["review_needed_count"] += 1
            if grams is not None and grams > 0:
                coverage["review_needed_with_grams_count"] += 1
        else:
            coverage["unmapped_count"] += 1
            if grams is not None and grams > 0:
                coverage["unmapped_with_grams_count"] += 1
    return coverage


def build_mapping_quality_rows(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    by_recipe: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in mapping_rows:
        by_recipe[clean_text(row.get("recipe_id_candidate"))].append(row)

    rows = []
    for recipe in recipes:
        recipe_id = clean_text(recipe.get("recipe_id_candidate"))
        recipe_rows = by_recipe.get(recipe_id, [])
        coverage = coverage_totals(recipe_rows)
        mapped_weight = sum(
            to_float(row.get("quantity_grams_estimated"))
            for row in recipe_rows
            if clean_text(row.get("mapping_status")) == "accepted_auto"
        )
        macro_weight = sum(
            to_float(row.get("quantity_grams_estimated"))
            for row in recipe_rows
            if clean_text(row.get("mapping_status")) == "accepted_auto"
            and clean_text(row.get("mapped_food_id")) not in LOW_OR_NO_MACRO_FOOD_IDS
        )
        known_weight = coverage["known_weight_grams_sum"]
        key_protein_mapped = any(
            is_key_protein_row(row)
            and clean_text(row.get("mapping_status")) == "accepted_auto"
            and to_optional_float(row.get("quantity_grams_estimated")) is not None
            for row in recipe_rows
        )
        key_carb_mapped = any(
            is_key_carb_row(row)
            and clean_text(row.get("mapping_status")) == "accepted_auto"
            and to_optional_float(row.get("quantity_grams_estimated")) is not None
            for row in recipe_rows
        )
        flags = []
        if not key_protein_mapped:
            flags.append("key_protein_unmapped")
        if recipe.get("target_bucket") != "breakfast_competitor" and not key_carb_mapped:
            flags.append("key_carb_unmapped")
        if safe_ratio(mapped_weight, known_weight) < 0.45:
            flags.append("mapped_weight_ratio_low")
        if safe_ratio(macro_weight, known_weight) < 0.50:
            flags.append("macro_relevant_mapped_weight_ratio_low")
        if coverage["unmapped_with_grams_count"] >= 2:
            flags.append("too_many_unmapped_with_grams")
        if "key_protein_unmapped" in flags or "key_carb_unmapped" in flags:
            status = "fail"
        elif flags:
            status = "review"
        else:
            status = "good"
        rows.append(
            {
                "recipe_id_candidate": recipe_id,
                "display_name": recipe.get("display_name", ""),
                "target_bucket": recipe.get("target_bucket", ""),
                "accepted_auto_count": int(coverage["accepted_auto_count"]),
                "accepted_auto_with_grams_count": int(coverage["accepted_auto_with_grams_count"]),
                "review_needed_with_grams_count": int(coverage["review_needed_with_grams_count"]),
                "unmapped_with_grams_count": int(coverage["unmapped_with_grams_count"]),
                "mapped_weight_ratio": format_number(safe_ratio(mapped_weight, known_weight)),
                "macro_relevant_mapped_weight_ratio": format_number(safe_ratio(macro_weight, known_weight)),
                "key_protein_mapped": str(key_protein_mapped).lower(),
                "key_carb_mapped": str(key_carb_mapped).lower(),
                "mapping_quality_status": status,
                "mapping_quality_flags": ";".join(flags),
            }
        )
    return rows


def is_key_protein_row(row: dict[str, object]) -> bool:
    text = normalize_text(f"{row.get('ingredient_name_normalized')} {row.get('ingredient_raw_text')}")
    return any(
        term in text
        for term in [
            "chicken",
            "beef",
            "pork",
            "turkey",
            "fish",
            "salmon",
            "tuna",
            "egg",
            "lentil",
            "bean",
            "yogurt",
            "milk",
            "oat",
        ]
    )


def is_key_carb_row(row: dict[str, object]) -> bool:
    text = normalize_text(f"{row.get('ingredient_name_normalized')} {row.get('ingredient_raw_text')}")
    return any(
        term in text
        for term in ["rice", "pasta", "spaghetti", "penne", "potato", "bread", "tortilla", "oat", "flour", "bean", "lentil"]
    )


def estimate_servings(recipe: dict[str, str], known_weight: float) -> float:
    if clean_text(recipe.get("target_bucket")) == "breakfast_competitor":
        if known_weight and known_weight < 450:
            return 1.0
        if known_weight and known_weight < 900:
            return 2.0
        return 4.0
    if known_weight and known_weight < 650:
        return 2.0
    return 4.0


def cache_status_for_recipe(
    coverage: dict[str, float],
    macro_ratio: float,
    energy_per: float,
) -> str:
    if coverage["accepted_auto_count"] <= 0:
        return "no_accepted_mapped_ingredients"
    if coverage["accepted_auto_with_grams_count"] <= 0:
        return "mapped_without_weight_estimates"
    if (
        coverage["accepted_auto_with_grams_count"] >= 3
        and macro_ratio >= 0.45
        and energy_per >= 150
    ):
        return "usable_from_mapped_ingredients"
    return "partial_from_mapped_ingredients"


def generator_ready_status(
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
    macro_min = 0.50 if bucket != "breakfast_competitor" else 0.45
    if macro_ratio < macro_min:
        reasons.append(f"macro_relevant_mapped_weight_ratio_lt_{macro_min:.2f}")
    if bucket == "breakfast_competitor":
        if energy_per < 200:
            reasons.append("breakfast_kcal_per_serving_lt_200")
        if energy_per > 800:
            reasons.append("breakfast_kcal_per_serving_gt_800")
        if protein_per < 8:
            reasons.append("breakfast_protein_per_serving_lt_8")
    else:
        if energy_per < 300:
            reasons.append("main_kcal_per_serving_lt_300")
        if protein_per < 14:
            reasons.append("main_protein_per_serving_lt_14")
        if carbs_per < 20:
            reasons.append("main_carbs_per_serving_lt_20")
    return not reasons, reasons


def quality_flags_for_recipe(
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
    if macro_ratio < (0.50 if bucket != "breakfast_competitor" else 0.45):
        flags.append("low_macro_relevant_mapped_weight_ratio")
    if bucket == "breakfast_competitor":
        if energy_per < 200:
            flags.append("low_breakfast_kcal")
        if energy_per > 800:
            flags.append("high_breakfast_kcal")
        if protein_per < 8:
            flags.append("low_breakfast_protein")
    else:
        if energy_per < 300:
            flags.append("low_main_kcal")
        if protein_per < 14:
            flags.append("low_main_protein")
        if carbs_per < 20:
            flags.append("low_main_carbs")
    if coverage["review_needed_with_grams_count"] + coverage["unmapped_with_grams_count"] >= 3:
        flags.append("mapping_review_blockers")
    return flags


def materialize_plus30_dataset(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, object]],
    cache_rows: list[dict[str, object]],
    ready_ids: set[str],
) -> None:
    PLUS30_DATASET_DIR.mkdir(parents=True, exist_ok=True)

    old_recipes = read_csv(OLD_DATASET_DIR / "recipes.csv")
    old_ingredients = read_csv(OLD_DATASET_DIR / "recipe_ingredients.csv")
    old_cache = read_csv(OLD_DATASET_DIR / "recipe_nutrition_cache.csv")
    recipe_by_id = {row["recipe_id_candidate"]: row for row in recipes}
    cache_by_id = {row["recipe_id_candidate"]: row for row in cache_rows}

    plus_recipe_rows = [
        materialized_recipe_row(recipe_by_id[recipe_id], cache_by_id[recipe_id])
        for recipe_id in sorted(ready_ids)
    ]
    plus_ingredient_rows = [
        materialized_ingredient_row(row)
        for row in mapping_rows
        if clean_text(row.get("recipe_id_candidate")) in ready_ids
    ]
    plus_cache_rows = [
        materialized_cache_row(cache_by_id[recipe_id])
        for recipe_id in sorted(ready_ids)
    ]

    write_csv(
        PLUS30_DATASET_DIR / "recipes.csv",
        old_recipes + plus_recipe_rows,
        list(old_recipes[0].keys()),
    )
    write_csv(
        PLUS30_DATASET_DIR / "recipe_ingredients.csv",
        old_ingredients + plus_ingredient_rows,
        list(old_ingredients[0].keys()),
    )
    write_csv(
        PLUS30_DATASET_DIR / "recipe_nutrition_cache.csv",
        old_cache + plus_cache_rows,
        list(old_cache[0].keys()),
    )
    readme_text = "\n".join(
        [
            "Recipes_DB v1.2 generator-ready plus30 draft",
            "",
            "Draft/test only. Do not treat as current production data.",
            f"Base dataset: {OLD_DATASET_DIR}",
            f"Round28 ready additions: {len(ready_ids)}",
            f"QC tag: {ROUND28_TAG}",
            "",
        ]
    )
    (PLUS30_DATASET_DIR / "README_v1_2_generator_ready_plus30.txt").write_text(
        readme_text,
        encoding="utf-8",
    )


def materialized_recipe_row(recipe: dict[str, str], cache: dict[str, object]) -> dict[str, object]:
    source_index = clean_text(recipe.get("source_index"))
    directions = json.loads(recipe.get("directions_json") or "[]")
    active_time = estimate_active_time(recipe)
    target_bucket = clean_text(recipe.get("target_bucket"))
    allowed_slots = ["breakfast"] if target_bucket == "breakfast_competitor" else ["lunch", "dinner"]
    return {
        "recipe_id": recipe["recipe_id_candidate"],
        "source_recipe_id": source_index,
        "source_dataset": "recipes_dataset_64k_dishes_round28_plus30",
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
        "scope_status": PLUS30_SCOPE_STATUS,
        "has_ingredients_parsed": 1,
        "has_nutrition_cache": 1,
        "is_pilot_recipe": 0,
        "is_active": 1,
        "qc_recipe_status": "generator_ready_draft",
        "qc_notes": f"{ROUND28_TAG}; draft_only; no_fooddb_rows_added",
        "allowed_slots_json": json.dumps(allowed_slots),
        "slot_policy_reason": f"round28_targeted_{target_bucket}",
        "content_quality_status": "keep",
        "content_exclusion_reason": "",
        "active_time_estimated_min": active_time,
        "passive_time_estimated_min": 0,
        "effective_time_min_for_scoring": active_time,
        "has_long_passive_time": "False",
        "time_estimation_confidence": "medium",
        "time_estimation_method": "round28_direction_step_estimate",
        "time_estimation_reasons": "draft_plus30_direction_count_estimate",
    }


def materialized_ingredient_row(row: dict[str, object]) -> dict[str, object]:
    recipe_id = clean_text(row.get("recipe_id_candidate"))
    position = int(to_float(row.get("ingredient_position")))
    return {
        "recipe_ingredient_id": f"{recipe_id}_ingredient_{position:03d}",
        "recipe_id": recipe_id,
        "ingredient_position": position,
        "ingredient_raw_text": row.get("ingredient_raw_text"),
        "ingredient_name_parsed": row.get("ingredient_name_parsed"),
        "ingredient_name_normalized": row.get("ingredient_name_normalized"),
        "quantity_value": row.get("quantity_value"),
        "quantity_unit": row.get("quantity_unit"),
        "quantity_text": "",
        "quantity_grams_estimated": row.get("quantity_grams_estimated"),
        "ingredient_role": ingredient_role(row.get("ingredient_name_normalized")),
        "ingredient_slot_key": normalize_text(row.get("ingredient_name_normalized")).replace(" ", "_"),
        "is_optional": 0,
        "is_substitutable": 1,
        "substitution_group_id": "",
        "mapped_food_id": row.get("mapped_food_id"),
        "mapped_food_canonical_name": row.get("mapped_food_canonical_name"),
        "mapping_status": row.get("mapping_status"),
        "mapping_confidence": row.get("mapping_confidence"),
        "mapping_method": row.get("mapping_method"),
        "mapping_notes": row.get("mapping_notes"),
        "qc_ingredient_status": "accepted_auto_round28"
        if row.get("mapping_status") == "accepted_auto"
        else "needs_review_round28",
        "qc_notes": ROUND28_TAG,
    }


def materialized_cache_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "recipe_id": row["recipe_id_candidate"],
        "nutrition_basis": PLUS30_NUTRITION_BASIS,
        "servings_basis": row["servings_basis"],
        "total_weight_grams_estimated": row["total_weight_grams_estimated"],
        "energy_kcal_total": row["energy_kcal_total"],
        "protein_g_total": row["protein_g_total"],
        "carbs_g_total": row["carbs_g_total"],
        "fat_g_total": row["fat_g_total"],
        "fibre_g_total": "",
        "sugars_g_total": "",
        "salt_g_total": "",
        "water_g_total": "",
        "energy_kcal_per_serving": row["energy_kcal_per_serving"],
        "protein_g_per_serving": row["protein_g_per_serving"],
        "carbs_g_per_serving": row["carbs_g_per_serving"],
        "fat_g_per_serving": row["fat_g_per_serving"],
        "fibre_g_per_serving": "",
        "sugars_g_per_serving": "",
        "salt_g_per_serving": "",
        "water_g_per_serving": "",
        "mapped_ingredient_count": row["accepted_auto_count"],
        "unmapped_ingredient_count": row["unmapped_count"],
        "mapped_weight_ratio": row["mapped_weight_ratio"],
        "cache_status": row["cache_status"],
        "cache_version": PLUS30_CACHE_VERSION,
        "qc_notes": f"{ROUND28_TAG}; {row['generator_ready_failure_reason']}",
        "macro_relevant_mapped_weight_ratio": row["macro_relevant_mapped_weight_ratio"],
        "uses_pilot_servings_fallback": row["uses_pilot_servings_fallback"],
        "servings_estimation_method": row["servings_estimation_method"],
        "servings_adjustment_applied": "false",
        "original_servings_basis": row["servings_basis"],
        "adjusted_servings_basis": row["servings_basis"],
        "quality_flags": row["quality_flags"],
    }


def estimate_active_time(recipe: dict[str, str]) -> int:
    steps = int(to_float(recipe.get("num_steps")))
    ingredients = int(to_float(recipe.get("num_ingredients")))
    return int(min(120, max(35, 15 + steps * 8 + ingredients * 2)))


def ingredient_role(value: object) -> str:
    text = normalize_text(value)
    if any(term in text for term in ["chicken", "beef", "pork", "turkey", "tuna", "salmon", "egg", "lentil", "bean"]):
        return "protein"
    if any(term in text for term in ["rice", "pasta", "spaghetti", "potato", "bread"]):
        return "carb"
    if any(term in text for term in ["oil", "butter"]):
        return "fat_source"
    if any(term in text for term in ["salt", "pepper", "oregano", "basil", "thyme"]):
        return "seasoning"
    return "veg" if text else "other"


def build_mapping_summary(rows: list[dict[str, object]]) -> str:
    status_counts = Counter(clean_text(row.get("mapping_status")) for row in rows)
    method_counts = Counter(clean_text(row.get("mapping_method")) for row in rows)
    with_grams = Counter()
    for row in rows:
        if to_optional_float(row.get("quantity_grams_estimated")) is not None:
            with_grams[clean_text(row.get("mapping_status"))] += 1
    lines = [
        "Recipes_DB v1.2 Round28 +30 mapping summary",
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
            "Round28 only maps to existing Food_DB v1.1 round9 draft rows; no Food_DB rows were added.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_nutrition_summary(rows: list[dict[str, object]]) -> str:
    ready = [row for row in rows if row["generator_ready_candidate"] == "true"]
    status_counts = Counter(clean_text(row.get("cache_status")) for row in rows)
    lines = [
        "Recipes_DB v1.2 Round28 +30 nutrition summary",
        "",
        f"recipes={len(rows)}",
        f"generator_ready_count={len(ready)}",
        "",
        "Cache status counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in status_counts.most_common())
    lines.extend(["", "Per recipe:"])
    for row in rows:
        lines.append(
            (
                f"- {row['display_name']}: ready={row['generator_ready_candidate']} "
                f"kcal={row['energy_kcal_per_serving']} "
                f"P/C/F={row['protein_g_per_serving']}/"
                f"{row['carbs_g_per_serving']}/{row['fat_g_per_serving']} "
                f"mapped_ratio={row['mapped_weight_ratio']} "
                f"failure={row['generator_ready_failure_reason'] or 'none'}"
            )
        )
    return "\n".join(lines) + "\n"


def build_materialization_summary(
    recipes: list[dict[str, str]],
    ready_ids: set[str],
) -> str:
    old_count = len(read_csv(OLD_DATASET_DIR / "recipes.csv"))
    lines = [
        "Recipes_DB v1.2 Round28 +30 materialization summary",
        "",
        f"old_dataset={OLD_DATASET_DIR}",
        f"new_dataset={PLUS30_DATASET_DIR}",
        f"old_recipe_count={old_count}",
        f"selected_plus30_count={len(recipes)}",
        f"generator_ready_plus30_count={len(ready_ids)}",
        f"new_recipe_count={old_count + len(ready_ids)}",
        "",
        "Ready recipes included:",
    ]
    for row in recipes:
        if row["recipe_id_candidate"] in ready_ids:
            lines.append(f"- {row['recipe_id_candidate']} | {row['display_name']}")
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
                "materialized": str(recipe_id in ready_ids).lower(),
                "generator_ready_failure_reason": cache.get("generator_ready_failure_reason", ""),
            }
        )
    return rows


def append_note(existing: str, note: str) -> str:
    parts = [part for part in existing.split(";") if part]
    parts.append(note)
    return ";".join(dict.fromkeys(parts))


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def format_number(value: object) -> str:
    number = to_optional_float(value)
    if number is None:
        return ""
    return f"{number:.4f}".rstrip("0").rstrip(".")


def to_optional_float(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_float(value: object) -> float:
    parsed = to_optional_float(value)
    return parsed if parsed is not None else 0.0


def clean_text(value: object) -> str:
    return str(value or "").strip()


def normalize_text(value: object) -> str:
    text = clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


if __name__ == "__main__":
    main()
