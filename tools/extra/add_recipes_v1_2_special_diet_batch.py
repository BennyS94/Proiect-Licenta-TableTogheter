from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DATASET_DIR = ROOT / "data/recipesdb/draft/v1_2_demo_final"
FOODDB_PATH = ROOT / "data/fooddb/current/fooddb_v1_core_master_draft.csv"
RECIPES_PATH = DATASET_DIR / "recipes.csv"
INGREDIENTS_PATH = DATASET_DIR / "recipe_ingredients.csv"
NUTRITION_PATH = DATASET_DIR / "recipe_nutrition_cache.csv"

BATCH_PREFIXES = (
    "recipes_v1_2_special_veg_",
    "recipes_v1_2_special_keto_",
)
BATCH_SOURCE = "manual_special_diet_batch_v1"
BATCH_CACHE = "recipes_v1_2_special_diet_batch_v1"

MACRO_FIELDS = (
    ("energy_kcal", "energy_kcal_100g"),
    ("protein_g", "protein_g_100g"),
    ("carbs_g", "carbs_g_100g"),
    ("fat_g", "fat_g_100g"),
    ("fibre_g", "fibre_g_100g"),
    ("sugars_g", "sugars_g_100g"),
    ("salt_g", "salt_g_100g"),
    ("water_g", "water_g_100g"),
)


def main() -> None:
    food_rows = _read_csv(FOODDB_PATH)
    food_by_canonical = {row["canonical_name"]: row for row in food_rows}
    recipes = _recipes()
    _validate_foods(recipes, food_by_canonical)

    recipe_rows = _read_csv(RECIPES_PATH)
    ingredient_rows = _read_csv(INGREDIENTS_PATH)
    nutrition_rows = _read_csv(NUTRITION_PATH)

    recipe_fields = _fieldnames(RECIPES_PATH)
    ingredient_fields = _fieldnames(INGREDIENTS_PATH)
    nutrition_fields = _fieldnames(NUTRITION_PATH)

    recipe_rows = _without_batch(recipe_rows)
    ingredient_rows = _without_batch(ingredient_rows)
    nutrition_rows = _without_batch(nutrition_rows)

    for recipe in recipes:
        recipe_rows.append(_recipe_row(recipe))
        ingredient_rows.extend(_ingredient_rows(recipe, food_by_canonical))
        nutrition_rows.append(_nutrition_row(recipe, food_by_canonical))

    _write_csv(RECIPES_PATH, recipe_rows, recipe_fields)
    _write_csv(INGREDIENTS_PATH, ingredient_rows, ingredient_fields)
    _write_csv(NUTRITION_PATH, nutrition_rows, nutrition_fields)

    print(f"Added {len(recipes)} special-diet recipes to {DATASET_DIR}")
    print("Vegetarian:", sum(1 for recipe in recipes if recipe["diet_group"] == "vegetarian"))
    print("Keto:", sum(1 for recipe in recipes if recipe["diet_group"] == "keto"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _fieldnames(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _without_batch(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    result = []
    for row in rows:
        recipe_id = str(row.get("recipe_id", ""))
        if recipe_id.startswith(BATCH_PREFIXES):
            continue
        result.append(row)
    return result


def _validate_foods(
    recipes: list[dict[str, Any]],
    food_by_canonical: dict[str, dict[str, str]],
) -> None:
    missing = sorted(
        {
            ingredient["canonical"]
            for recipe in recipes
            for ingredient in recipe["ingredients"]
            if ingredient["canonical"] not in food_by_canonical
        }
    )
    if missing:
        raise RuntimeError("Missing Food_DB canonical names: " + ", ".join(missing))


def _recipe_row(recipe: dict[str, Any]) -> dict[str, Any]:
    total_time = recipe["prep_time_min"] + recipe["cook_time_min"]
    total_weight = sum(item["grams"] for item in recipe["ingredients"])
    slots = recipe["slots"]
    return {
        "recipe_id": recipe["recipe_id"],
        "source_recipe_id": recipe["recipe_id"],
        "source_dataset": BATCH_SOURCE,
        "recipe_name": recipe["display_name"],
        "display_name": recipe["display_name"],
        "recipe_family_name": recipe["family_name"],
        "recipe_kind": recipe["recipe_kind"],
        "recipe_category": "manual_curated",
        "recipe_subcategory": f"{recipe['diet_group']}_special_batch",
        "recipe_cuisine": "Practical household",
        "directions_json": json.dumps(recipe["directions"], ensure_ascii=False),
        "directions_step_count": str(len(recipe["directions"])),
        "servings_declared": str(recipe["servings"]),
        "servings_normalized": str(recipe["servings"]),
        "prep_time_min": _fmt(recipe["prep_time_min"]),
        "cook_time_min": _fmt(recipe["cook_time_min"]),
        "total_time_min": _fmt(total_time),
        "difficulty_level": "easy",
        "scope_status": "v1_2_generator_ready_draft",
        "has_ingredients_parsed": "1",
        "has_nutrition_cache": "1",
        "is_pilot_recipe": "0",
        "is_active": "1",
        "qc_recipe_status": "generator_ready_draft",
        "qc_notes": f"{BATCH_SOURCE}; diet_group={recipe['diet_group']}; total_weight_g={_fmt(total_weight)}",
        "allowed_slots_json": json.dumps(slots, ensure_ascii=False),
        "slot_policy_reason": f"{BATCH_SOURCE}_{recipe['diet_group']}",
        "content_quality_status": "keep",
        "content_exclusion_reason": "",
        "active_time_estimated_min": _fmt(total_time),
        "passive_time_estimated_min": "0",
        "effective_time_min_for_scoring": _fmt(total_time),
        "has_long_passive_time": "False",
        "time_estimation_confidence": "high",
        "time_estimation_method": BATCH_SOURCE,
        "time_estimation_reasons": "explicit_manual_curated_time",
    }


def _ingredient_rows(
    recipe: dict[str, Any],
    food_by_canonical: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    rows = []
    for index, ingredient in enumerate(recipe["ingredients"], start=1):
        food = food_by_canonical[ingredient["canonical"]]
        grams = ingredient["grams"]
        rows.append(
            {
                "recipe_ingredient_id": f"{recipe['recipe_id']}_ingredient_{index:03d}",
                "recipe_id": recipe["recipe_id"],
                "ingredient_position": str(index),
                "ingredient_raw_text": f"{_fmt(grams)} g {ingredient['name']}",
                "ingredient_name_parsed": ingredient["name"],
                "ingredient_name_normalized": ingredient["name"].strip().lower(),
                "quantity_value": _fmt(grams),
                "quantity_unit": "g",
                "quantity_text": f"{_fmt(grams)} g",
                "quantity_grams_estimated": _fmt(grams),
                "ingredient_role": ingredient["role"],
                "ingredient_slot_key": ingredient["role"],
                "is_optional": "0",
                "is_substitutable": "1",
                "substitution_group_id": f"{recipe['recipe_id']}_{ingredient['role']}",
                "mapped_food_id": food["food_id"],
                "mapped_food_canonical_name": food["canonical_name"],
                "mapping_status": "accepted_auto",
                "mapping_confidence": "high",
                "mapping_method": BATCH_SOURCE,
                "mapping_notes": "manual_special_diet_recipe_batch",
                "qc_ingredient_status": "accepted_auto_special_diet_batch",
                "qc_notes": BATCH_SOURCE,
            }
        )
    return rows


def _nutrition_row(
    recipe: dict[str, Any],
    food_by_canonical: dict[str, dict[str, str]],
) -> dict[str, Any]:
    totals = {name: 0.0 for name, _ in MACRO_FIELDS}
    total_weight = 0.0
    for ingredient in recipe["ingredients"]:
        food = food_by_canonical[ingredient["canonical"]]
        grams = ingredient["grams"]
        total_weight += grams
        for total_name, source_name in MACRO_FIELDS:
            totals[total_name] += _to_float(food.get(source_name)) * grams / 100.0

    servings = float(recipe["servings"])
    row = {
        "recipe_id": recipe["recipe_id"],
        "nutrition_basis": BATCH_CACHE,
        "servings_basis": _fmt(servings),
        "total_weight_grams_estimated": _fmt(total_weight),
        "mapped_ingredient_count": str(len(recipe["ingredients"])),
        "unmapped_ingredient_count": "0",
        "mapped_weight_ratio": "1.0",
        "cache_status": "usable_from_mapped_ingredients",
        "cache_version": BATCH_CACHE,
        "qc_notes": f"{BATCH_SOURCE}; all ingredients mapped",
        "macro_relevant_mapped_weight_ratio": "1.0",
        "uses_pilot_servings_fallback": "False",
        "servings_estimation_method": "explicit_manual_curated",
        "servings_adjustment_applied": "False",
        "original_servings_basis": _fmt(servings),
        "adjusted_servings_basis": _fmt(servings),
        "quality_flags": "",
    }
    for total_name, _ in MACRO_FIELDS:
        row[f"{total_name}_total"] = _fmt(totals[total_name])
        row[f"{total_name}_per_serving"] = _fmt(totals[total_name] / servings)
    return row


def _to_float(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def _fmt(value: float | int) -> str:
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def _ing(name: str, canonical: str, grams: float, role: str) -> dict[str, Any]:
    return {
        "name": name,
        "canonical": canonical,
        "grams": float(grams),
        "role": role,
    }


def _make_recipe(
    recipe_id: str,
    name: str,
    diet_group: str,
    kind: str,
    slots: list[str],
    ingredients: list[dict[str, Any]],
    prep: int,
    cook: int,
    directions: list[str],
    family_name: str | None = None,
) -> dict[str, Any]:
    return {
        "recipe_id": recipe_id,
        "display_name": name,
        "family_name": family_name or name,
        "diet_group": diet_group,
        "recipe_kind": kind,
        "slots": slots,
        "ingredients": ingredients,
        "prep_time_min": prep,
        "cook_time_min": cook,
        "servings": 1,
        "directions": directions,
    }


def _ld_steps(base: str, veg: str = "vegetables") -> list[str]:
    return [
        "Prepare the ingredients.",
        f"Cook the {base} until hot and safe to serve.",
        f"Add the {veg} and warm through.",
        "Combine everything in one bowl or plate.",
        "Finish and serve.",
    ]


def _breakfast_steps(main: str) -> list[str]:
    return [
        "Prepare the ingredients.",
        f"Cook or assemble the {main}.",
        "Add the vegetables or side ingredients.",
        "Serve warm or chilled as appropriate.",
    ]


def _snack_steps() -> list[str]:
    return [
        "Prepare the ingredients.",
        "Assemble the snack plate or bowl.",
        "Serve immediately.",
    ]


def _recipes() -> list[dict[str, Any]]:
    i = _ing
    recipes: list[dict[str, Any]] = []

    vegetarian = [
        ("Tofu Rice Broccoli Bowl", "complete_main", ["lunch", "dinner"], 10, 15, _ld_steps("tofu", "broccoli"), [i("Tofu", "tofu_plain", 180, "protein"), i("Cooked rice", "rice_cooked_unsalted", 220, "carb_source"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 130, "veg"), i("Tomato", "tomato_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Tofu Quinoa Pepper Bowl", "complete_main", ["lunch", "dinner"], 10, 15, _ld_steps("tofu", "pepper and quinoa"), [i("Tofu", "tofu_plain", 190, "protein"), i("Cooked quinoa", "quinoa_boiled_cooked_in_water_unsalted", 230, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 130, "veg"), i("Spinach", "spinach_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Tofu Pasta Tomato Spinach", "complete_main", ["lunch", "dinner"], 10, 14, _ld_steps("tofu", "tomato and spinach"), [i("Tofu", "tofu_plain", 170, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 260, "carb_source"), i("Tomato puree", "tomato_puree_canned", 130, "veg"), i("Spinach", "spinach_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Smoked Tofu Potato Mushroom Plate", "complete_main", ["lunch", "dinner"], 10, 16, _ld_steps("smoked tofu", "potato and mushrooms"), [i("Smoked tofu", "tofu_smoked", 170, "protein"), i("Cooked potato", "potato_boiled_cooked_in_water", 280, "carb_source"), i("Mushrooms", "button_mushroom_or_cultivated_mushroom_raw", 130, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Smoked Tofu Bulgur Zucchini Bowl", "complete_main", ["lunch", "dinner"], 10, 14, _ld_steps("smoked tofu", "bulgur and zucchini"), [i("Smoked tofu", "tofu_smoked", 170, "protein"), i("Cooked bulgur", "wheat_bulgur_cooked_unsalted", 260, "carb_source"), i("Zucchini", "courgette_or_zucchini_pulp_and_peel_raw", 150, "veg"), i("Tomato", "tomato_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Seitan Rice Pepper Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("seitan", "rice and peppers"), [i("Seitan", "seitan", 170, "protein"), i("Cooked rice", "rice_cooked_unsalted", 240, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 130, "veg"), i("Spinach", "spinach_raw", 70, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Seitan Quinoa Broccoli Plate", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("seitan", "quinoa and broccoli"), [i("Seitan", "seitan", 180, "protein"), i("Cooked quinoa", "quinoa_boiled_cooked_in_water_unsalted", 230, "carb_source"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 140, "veg"), i("Carrot", "carrot_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Seitan Pasta Tomato Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("seitan", "pasta and tomato"), [i("Seitan", "seitan", 170, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 270, "carb_source"), i("Tomato puree", "tomato_puree_canned", 140, "veg"), i("Mushrooms", "button_mushroom_or_cultivated_mushroom_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Lentil Rice Spinach Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("lentils", "rice and spinach"), [i("Cooked lentils", "lentil_boiled_cooked_in_water", 240, "protein"), i("Cooked rice", "rice_cooked_unsalted", 230, "carb_source"), i("Spinach", "spinach_raw", 90, "veg"), i("Tomato puree", "tomato_puree_canned", 110, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Lentil Potato Carrot Stew", "complete_main", ["lunch", "dinner"], 10, 18, _ld_steps("lentils", "potato and carrot"), [i("Cooked lentils", "lentil_green_boiled_cooked_in_water", 250, "protein"), i("Cooked potato", "potato_boiled_cooked_in_water", 280, "carb_source"), i("Carrot", "carrot_raw", 110, "veg"), i("Onion", "onion_raw", 70, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Lentil Bulgur Pepper Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("lentils", "bulgur and pepper"), [i("Cooked lentils", "lentil_boiled_cooked_in_water", 230, "protein"), i("Cooked bulgur", "wheat_bulgur_cooked_unsalted", 260, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 120, "veg"), i("Tomato", "tomato_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Lentil Quinoa Mushroom Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("lentils", "quinoa and mushrooms"), [i("Cooked lentils", "lentil_pink_or_red_boiled_cooked_in_water", 230, "protein"), i("Cooked quinoa", "quinoa_boiled_cooked_in_water_unsalted", 240, "carb_source"), i("Mushrooms", "button_mushroom_or_cultivated_mushroom_raw", 130, "veg"), i("Spinach", "spinach_raw", 70, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Kidney Bean Rice Pepper Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("beans", "rice and pepper"), [i("Red kidney beans", "red_kidney_bean_boiled_cooked_in_water", 230, "protein"), i("Cooked rice", "rice_cooked_unsalted", 230, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 130, "veg"), i("Tomato", "tomato_raw", 110, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Kidney Bean Pasta Tomato Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("beans", "pasta and tomato"), [i("Red kidney beans", "red_kidney_bean_boiled_cooked_in_water", 190, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 270, "carb_source"), i("Tomato puree", "tomato_puree_canned", 140, "veg"), i("Spinach", "spinach_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Haricot Bean Potato Spinach Bowl", "complete_main", ["lunch", "dinner"], 8, 14, _ld_steps("beans", "potato and spinach"), [i("Haricot beans", "haricot_bean_boiled_cooked_in_water", 240, "protein"), i("Cooked potato", "potato_boiled_cooked_in_water", 260, "carb_source"), i("Spinach", "spinach_raw", 90, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Mung Bean Rice Vegetable Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("mung beans", "rice and vegetables"), [i("Mung beans", "mung_bean_boiled_cooked_in_water", 240, "protein"), i("Cooked rice", "rice_cooked_unsalted", 230, "carb_source"), i("Carrot", "carrot_raw", 90, "veg"), i("Spinach", "spinach_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Broad Bean Couscous Tomato Bowl", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("broad beans", "couscous and tomato"), [i("Broad beans", "broad_bean_boiled_cooked_in_water", 250, "protein"), i("Cooked couscous", "couscous_precooked_durum_wheat_semolina_cooked_unsalted", 240, "carb_source"), i("Tomato", "tomato_raw", 130, "veg"), i("Cucumber", "cucumber_pulp_and_peel_raw", 90, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Egg Fried Rice Vegetable Bowl", "complete_main", ["lunch", "dinner"], 8, 10, _ld_steps("eggs", "rice and vegetables"), [i("Eggs", "egg_raw", 130, "protein"), i("Cooked rice", "rice_cooked_unsalted", 260, "carb_source"), i("Garden peas", "garden_peas_frozen_cooked", 120, "veg"), i("Carrot", "carrot_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Egg Potato Spinach Plate", "complete_main", ["lunch", "dinner"], 8, 12, _ld_steps("eggs", "potato and spinach"), [i("Eggs", "egg_raw", 140, "protein"), i("Cooked potato", "potato_boiled_cooked_in_water", 300, "carb_source"), i("Spinach", "spinach_raw", 100, "veg"), i("Feta cheese", "feta_type_cheese_from_cow_s_milk", 35, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")]),
        ("Egg Quinoa Feta Bowl", "complete_main", ["lunch", "dinner"], 8, 10, _ld_steps("eggs", "quinoa and feta"), [i("Eggs", "egg_raw", 120, "protein"), i("Cooked quinoa", "quinoa_boiled_cooked_in_water_unsalted", 240, "carb_source"), i("Feta cheese", "feta_type_cheese_from_cow_s_milk", 45, "dairy"), i("Spinach", "spinach_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")]),
        ("Ricotta Spinach Pasta Bowl", "complete_main", ["lunch", "dinner"], 8, 10, _ld_steps("ricotta", "pasta and spinach"), [i("Ricotta cheese", "ricotta_cheese", 120, "dairy"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 280, "carb_source"), i("Spinach", "spinach_raw", 100, "veg"), i("Tomato puree", "tomato_puree_canned", 110, "veg"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")]),
        ("Mozzarella Tomato Rice Bowl", "complete_main", ["lunch", "dinner"], 8, 8, _ld_steps("mozzarella", "rice and tomato"), [i("Mozzarella", "mozzarella_cheese_from_cow_s_milk", 100, "dairy"), i("Cooked rice", "rice_cooked_unsalted", 260, "carb_source"), i("Tomato", "tomato_raw", 150, "veg"), i("Lettuce", "lettuce_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Feta Bulgur Cucumber Bowl", "complete_main", ["lunch", "dinner"], 8, 8, _ld_steps("feta", "bulgur and cucumber"), [i("Feta cheese", "feta_type_cheese_from_cow_s_milk", 90, "dairy"), i("Cooked bulgur", "wheat_bulgur_cooked_unsalted", 280, "carb_source"), i("Cucumber", "cucumber_pulp_and_peel_raw", 120, "veg"), i("Tomato", "tomato_raw", 120, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Goat Cheese Lentil Salad Bowl", "complete_main", ["lunch", "dinner"], 8, 8, _ld_steps("lentils", "goat cheese and salad"), [i("Cooked lentils", "lentil_green_boiled_cooked_in_water", 230, "protein"), i("Goat cheese", "cheese_from_goat_s_milk_fresh", 70, "dairy"), i("Cooked quinoa", "quinoa_boiled_cooked_in_water_unsalted", 180, "carb_source"), i("Lettuce", "lettuce_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Greek Yogurt Oat Berry Bowl", "breakfast_meal", ["breakfast"], 5, 0, _breakfast_steps("yogurt oat bowl"), [i("Greek yogurt", "yogurt_greek_style_plain", 200, "dairy"), i("Raw oats", "oat_raw", 55, "carb_source"), i("Blueberries", "blueberry_raw", 100, "fruit"), i("Almonds", "almond_peeled_unpeeled_or_blanched", 15, "fat_source")]),
        ("Spinach Feta Omelette Toast", "breakfast_meal", ["breakfast"], 6, 8, _breakfast_steps("omelette"), [i("Eggs", "egg_raw", 130, "protein"), i("Feta cheese", "feta_type_cheese_from_cow_s_milk", 35, "dairy"), i("Spinach", "spinach_raw", 80, "veg"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 70, "carb_source"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")]),
        ("Tofu Mushroom Breakfast Scramble", "breakfast_meal", ["breakfast"], 6, 8, _breakfast_steps("tofu scramble"), [i("Tofu", "tofu_plain", 170, "protein"), i("Mushrooms", "button_mushroom_or_cultivated_mushroom_raw", 120, "veg"), i("Spinach", "spinach_raw", 70, "veg"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 60, "carb_source"), i("Olive oil", "olive_oil_extra_virgin", 6, "fat_source")]),
        ("Yogurt Apple Almond Snack", "snack", ["snack"], 3, 0, _snack_steps(), [i("Greek yogurt", "yogurt_greek_style_plain", 120, "dairy"), i("Apple", "apple_pulp_and_peel_raw", 130, "fruit"), i("Almonds", "almond_peeled_unpeeled_or_blanched", 15, "fat_source")]),
        ("Seitan Cucumber Snack Plate", "snack", ["snack"], 3, 0, _snack_steps(), [i("Seitan", "seitan", 75, "protein"), i("Cucumber", "cucumber_pulp_and_peel_raw", 120, "veg"), i("Fresh cheese", "drained_soft_fresh_cheese_around_6_fat", 70, "dairy")]),
        ("Fresh Cheese Berry Snack", "snack", ["snack"], 3, 0, _snack_steps(), [i("Fresh cheese", "drained_soft_fresh_cheese_around_6_fat", 140, "dairy"), i("Raspberries", "raspberry_raw", 100, "fruit"), i("Walnuts", "walnut_dried_husked", 12, "fat_source")]),
    ]

    keto = [
        ("Egg Avocado Spinach Plate", "breakfast_meal", ["breakfast"], 6, 8, _breakfast_steps("egg plate"), [i("Eggs", "egg_raw", 140, "protein"), i("Avocado", "avocado_pulp_raw", 100, "fat_source"), i("Spinach", "spinach_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Mushroom Cheese Omelette", "breakfast_meal", ["breakfast"], 6, 8, _breakfast_steps("omelette"), [i("Eggs", "egg_raw", 150, "protein"), i("Mushrooms", "button_mushroom_or_cultivated_mushroom_raw", 130, "veg"), i("Mozzarella", "mozzarella_cheese_from_cow_s_milk", 45, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Salmon Egg Cucumber Plate", "breakfast_meal", ["breakfast"], 6, 6, _breakfast_steps("salmon egg plate"), [i("Fish salmon", "salmon_smoked", 90, "protein"), i("Egg", "egg_raw", 70, "protein"), i("Cucumber", "cucumber_pulp_and_peel_raw", 120, "veg"), i("Avocado", "avocado_pulp_raw", 80, "fat_source")]),
        ("Chicken Egg Spinach Bowl", "breakfast_meal", ["breakfast"], 6, 8, _breakfast_steps("chicken egg bowl"), [i("Chicken breast", "chicken_breast_without_skin_raw", 110, "protein"), i("Egg", "egg_raw", 70, "protein"), i("Spinach", "spinach_raw", 90, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Greek Yogurt Chia Almond Bowl", "breakfast_meal", ["breakfast"], 5, 0, _breakfast_steps("yogurt bowl"), [i("Greek yogurt", "yogurt_greek_style_plain", 180, "dairy"), i("Chia seeds", "seeds_chia_dried", 20, "fat_source"), i("Almonds", "almond_peeled_unpeeled_or_blanched", 20, "fat_source"), i("Raspberries", "raspberry_raw", 50, "fruit")]),
        ("Feta Egg Pepper Scramble", "breakfast_meal", ["breakfast"], 6, 8, _breakfast_steps("egg scramble"), [i("Eggs", "egg_raw", 150, "protein"), i("Feta cheese", "feta_type_cheese_from_cow_s_milk", 45, "dairy"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 70, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Turkey Ham Avocado Egg Plate", "breakfast_meal", ["breakfast"], 6, 6, _breakfast_steps("turkey egg plate"), [i("Turkey cooked ham", "turkey_cooked_ham_in_slices", 90, "protein"), i("Egg", "egg_raw", 70, "protein"), i("Avocado", "avocado_pulp_raw", 100, "fat_source"), i("Lettuce", "lettuce_raw", 60, "veg")]),
        ("Chicken Broccoli Cream Bowl", "complete_main", ["lunch", "dinner"], 8, 16, _ld_steps("chicken", "broccoli and cream"), [i("Chicken breast", "chicken_breast_without_skin_raw", 170, "protein"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 170, "veg"), i("Liquid cream", "liquid_cream_30_fat_uht", 45, "fat_source"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Chicken Cauliflower Spinach Plate", "complete_main", ["lunch", "dinner"], 8, 16, _ld_steps("chicken", "cauliflower and spinach"), [i("Chicken breast", "chicken_breast_without_skin_raw", 180, "protein"), i("Cauliflower", "cauliflower_steamed", 180, "veg"), i("Spinach", "spinach_raw", 90, "veg"), i("Olive oil", "olive_oil_extra_virgin", 12, "fat_source")]),
        ("Chicken Zucchini Mushroom Skillet", "complete_main", ["lunch", "dinner"], 8, 16, _ld_steps("chicken", "zucchini and mushrooms"), [i("Chicken breast", "chicken_breast_without_skin_raw", 175, "protein"), i("Zucchini", "courgette_or_zucchini_pulp_and_peel_raw", 180, "veg"), i("Mushrooms", "button_mushroom_or_cultivated_mushroom_raw", 140, "veg"), i("Olive oil", "olive_oil_extra_virgin", 14, "fat_source")]),
        ("Chicken Avocado Lettuce Salad", "complete_main", ["lunch", "dinner"], 8, 10, _ld_steps("chicken", "avocado salad"), [i("Chicken breast", "chicken_breast_without_skin_raw", 170, "protein"), i("Avocado", "avocado_pulp_raw", 120, "fat_source"), i("Lettuce", "lettuce_raw", 90, "veg"), i("Cucumber", "cucumber_pulp_and_peel_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Beef Mushroom Spinach Bowl", "complete_main", ["lunch", "dinner"], 8, 18, _ld_steps("beef", "mushrooms and spinach"), [i("Beef round", "beef_round_raw", 170, "protein"), i("Mushrooms", "button_mushroom_or_cultivated_mushroom_raw", 150, "veg"), i("Spinach", "spinach_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 12, "fat_source")]),
        ("Beef Broccoli Cream Plate", "complete_main", ["lunch", "dinner"], 8, 18, _ld_steps("beef", "broccoli and cream"), [i("Beef round", "beef_round_raw", 170, "protein"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 170, "veg"), i("Liquid cream", "liquid_cream_30_fat_uht", 45, "fat_source"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Beef Cabbage Pepper Skillet", "complete_main", ["lunch", "dinner"], 8, 18, _ld_steps("beef", "cabbage and pepper"), [i("Beef round", "beef_round_raw", 170, "protein"), i("Green cabbage", "green_cabbage_raw", 160, "veg"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 90, "veg"), i("Olive oil", "olive_oil_extra_virgin", 12, "fat_source")]),
        ("Pork Tenderloin Cauliflower Plate", "complete_main", ["lunch", "dinner"], 8, 18, _ld_steps("pork", "cauliflower"), [i("Pork tenderloin", "pork_tenderloin_lean_raw", 180, "protein"), i("Cauliflower", "cauliflower_steamed", 190, "veg"), i("Liquid cream", "liquid_cream_30_fat_uht", 40, "fat_source"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Pork Zucchini Mushroom Bowl", "complete_main", ["lunch", "dinner"], 8, 18, _ld_steps("pork", "zucchini and mushrooms"), [i("Pork tenderloin", "pork_tenderloin_lean_raw", 175, "protein"), i("Zucchini", "courgette_or_zucchini_pulp_and_peel_raw", 180, "veg"), i("Mushrooms", "button_mushroom_or_cultivated_mushroom_raw", 130, "veg"), i("Olive oil", "olive_oil_extra_virgin", 12, "fat_source")]),
        ("Salmon Avocado Cucumber Salad", "complete_main", ["lunch", "dinner"], 6, 0, _ld_steps("salmon", "avocado and cucumber"), [i("Fish salmon", "salmon_canned_drained", 150, "protein"), i("Avocado", "avocado_pulp_raw", 120, "fat_source"), i("Cucumber", "cucumber_pulp_and_peel_raw", 140, "veg"), i("Lettuce", "lettuce_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Salmon Broccoli Cream Plate", "complete_main", ["lunch", "dinner"], 6, 10, _ld_steps("salmon", "broccoli and cream"), [i("Fish salmon", "salmon_canned_drained", 150, "protein"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 180, "veg"), i("Liquid cream", "liquid_cream_30_fat_uht", 45, "fat_source"), i("Olive oil", "olive_oil_extra_virgin", 6, "fat_source")]),
        ("Tuna Avocado Lettuce Bowl", "complete_main", ["lunch", "dinner"], 6, 0, _ld_steps("tuna", "avocado and lettuce"), [i("Fish tuna", "tuna_plain_canned_drained", 140, "protein"), i("Avocado", "avocado_pulp_raw", 130, "fat_source"), i("Lettuce", "lettuce_raw", 90, "veg"), i("Cucumber", "cucumber_pulp_and_peel_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Tuna Egg Cucumber Plate", "complete_main", ["lunch", "dinner"], 6, 6, _ld_steps("tuna and egg", "cucumber"), [i("Fish tuna", "tuna_plain_canned_drained", 120, "protein"), i("Egg", "egg_raw", 70, "protein"), i("Cucumber", "cucumber_pulp_and_peel_raw", 150, "veg"), i("Lettuce", "lettuce_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Mackerel Cabbage Bowl", "complete_main", ["lunch", "dinner"], 6, 8, _ld_steps("mackerel", "cabbage"), [i("Fish mackerel", "mackerel_canned_in_brine_drained", 150, "protein"), i("Green cabbage", "green_cabbage_raw", 170, "veg"), i("Cucumber", "cucumber_pulp_and_peel_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Sardine Avocado Salad", "complete_main", ["lunch", "dinner"], 6, 0, _ld_steps("sardines", "avocado salad"), [i("Fish sardines", "european_pilchard_or_sardine_in_oil_canned_drained", 140, "protein"), i("Avocado", "avocado_pulp_raw", 110, "fat_source"), i("Lettuce", "lettuce_raw", 90, "veg"), i("Cucumber", "cucumber_pulp_and_peel_raw", 100, "veg")]),
        ("Shrimp Broccoli Zucchini Bowl", "complete_main", ["lunch", "dinner"], 8, 10, _ld_steps("shrimp", "broccoli and zucchini"), [i("Shrimp", "shrimp_cooked", 170, "protein"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 160, "veg"), i("Zucchini", "courgette_or_zucchini_pulp_and_peel_raw", 140, "veg"), i("Olive oil", "olive_oil_extra_virgin", 12, "fat_source")]),
        ("Shrimp Cauliflower Cream Plate", "complete_main", ["lunch", "dinner"], 8, 10, _ld_steps("shrimp", "cauliflower and cream"), [i("Shrimp", "shrimp_cooked", 170, "protein"), i("Cauliflower", "cauliflower_steamed", 180, "veg"), i("Liquid cream", "liquid_cream_30_fat_uht", 45, "fat_source"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")]),
        ("Eggplant Mozzarella Beef Plate", "complete_main", ["lunch", "dinner"], 8, 18, _ld_steps("beef", "eggplant and mozzarella"), [i("Beef round", "beef_round_raw", 160, "protein"), i("Eggplant", "eggplant_raw", 170, "veg"), i("Mozzarella", "mozzarella_cheese_from_cow_s_milk", 60, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 12, "fat_source")]),
        ("Turkey Ham Avocado Salad", "complete_main", ["lunch", "dinner"], 6, 0, _ld_steps("turkey", "avocado salad"), [i("Turkey cooked ham", "turkey_cooked_ham_in_slices", 150, "protein"), i("Avocado", "avocado_pulp_raw", 130, "fat_source"), i("Lettuce", "lettuce_raw", 100, "veg"), i("Cucumber", "cucumber_pulp_and_peel_raw", 120, "veg"), i("Olive oil", "olive_oil_extra_virgin", 6, "fat_source")]),
        ("Cheese Almond Snack Plate", "snack", ["snack"], 3, 0, _snack_steps(), [i("Mozzarella", "mozzarella_cheese_from_cow_s_milk", 70, "dairy"), i("Almonds", "almond_peeled_unpeeled_or_blanched", 25, "fat_source"), i("Cucumber", "cucumber_pulp_and_peel_raw", 100, "veg")]),
        ("Boiled Egg Avocado Snack", "snack", ["snack"], 3, 8, _snack_steps(), [i("Egg", "egg_raw", 70, "protein"), i("Avocado", "avocado_pulp_raw", 90, "fat_source"), i("Lettuce", "lettuce_raw", 50, "veg")]),
        ("Greek Yogurt Chia Snack", "snack", ["snack"], 3, 0, _snack_steps(), [i("Greek yogurt", "yogurt_greek_style_plain", 120, "dairy"), i("Chia seeds", "seeds_chia_dried", 18, "fat_source"), i("Almonds", "almond_peeled_unpeeled_or_blanched", 15, "fat_source")]),
        ("Tuna Cucumber Snack", "snack", ["snack"], 3, 0, _snack_steps(), [i("Fish tuna", "tuna_plain_canned_drained", 70, "protein"), i("Cucumber", "cucumber_pulp_and_peel_raw", 140, "veg"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")]),
    ]

    for idx, item in enumerate(vegetarian, start=1):
        recipes.append(_make_recipe(f"recipes_v1_2_special_veg_{idx:03d}", item[0], "vegetarian", item[1], item[2], item[6], item[3], item[4], item[5]))

    for idx, item in enumerate(keto, start=1):
        recipes.append(_make_recipe(f"recipes_v1_2_special_keto_{idx:03d}", item[0], "keto", item[1], item[2], item[6], item[3], item[4], item[5]))

    return recipes


if __name__ == "__main__":
    main()
