from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
FOODDB_BATCH1 = ROOT / "data/fooddb/draft/fooddb_v1_2_core_master_manual_batch1.csv"
FOODDB_ROUND9 = ROOT / "data/fooddb/draft/fooddb_v1_1_core_master_draft_round9.csv"
BASELINE_DIR = ROOT / "data/recipesdb/draft/v1_2_generator_ready_round37_expanded_repaired_manual_batch1"
ROUND40_MANUAL_PLAN = ROOT / "data/recipesdb/audit/recipes_v1_2_round40_manual_curated_plan.csv"
MANUAL_REPAIR_QUEUE = ROOT / "data/recipesdb/draft/recipes_v1_2_manual_repair_queue.csv"
ROUND41_DATASET_DIR = ROOT / "data/recipesdb/draft/v1_2_generator_ready_round41_manual_curated"
RECIPES_DRAFT_DIR = ROOT / "data/recipesdb/draft"
RECIPES_AUDIT_DIR = ROOT / "data/recipesdb/audit"
FOODDB_AUDIT_DIR = ROOT / "data/fooddb/audit"


def fooddb_path() -> Path:
    if FOODDB_BATCH1.exists():
        return FOODDB_BATCH1
    return FOODDB_ROUND9


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def dumps_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def ingredient(name: str, canonical: str, grams: float, role: str) -> dict[str, Any]:
    return {
        "name": name,
        "canonical": canonical,
        "grams": float(grams),
        "role": role,
    }


def recipe(
    idx: int,
    name: str,
    kind: str,
    slots: list[str],
    bucket: str,
    protein: str,
    carb: str,
    veg: str,
    servings: int,
    prep: int,
    cook: int,
    ingredients: list[dict[str, Any]],
    directions: list[str],
    notes: str,
) -> dict[str, Any]:
    return {
        "manual_recipe_id": f"recipes_v1_2_round41_manual_{idx:03d}",
        "display_name": name,
        "recipe_kind": kind,
        "allowed_slots_json": dumps_json(slots),
        "target_bucket": bucket,
        "primary_protein": protein,
        "carb_source": carb,
        "veg_component": veg,
        "servings": servings,
        "prep_time_min": prep,
        "cook_time_min": cook,
        "ingredients": ingredients,
        "directions": directions,
        "notes": notes,
    }


def recipe_catalog() -> list[dict[str, Any]]:
    i = ingredient
    return [
        recipe(1, "Greek Yogurt Oat Banana Bowl", "breakfast_meal", ["breakfast"], "breakfast_competitor", "dairy", "oats/banana", "banana", 1, 5, 0, [i("Greek style plain yogurt", "yogurt_greek_style_plain", 180, "dairy"), i("Raw oats", "oat_raw", 55, "carb_source"), i("Banana", "banana_pulp_raw", 120, "fruit"), i("Semi skimmed milk", "milk_semi_skimmed_pasteurised", 60, "dairy")], ["Mix yogurt and milk in a bowl.", "Stir in oats and sliced banana.", "Serve chilled or after a short rest."], "Clean mapped breakfast with carbs and dairy protein."),
        recipe(2, "Apple Milk Oat Bowl", "breakfast_meal", ["breakfast"], "breakfast_competitor", "dairy", "oats/apple", "apple", 1, 5, 5, [i("Raw oats", "oat_raw", 60, "carb_source"), i("Semi skimmed milk", "milk_semi_skimmed_pasteurised", 220, "dairy"), i("Apple", "apple_pulp_and_peel_raw", 140, "fruit"), i("Greek style plain yogurt", "yogurt_greek_style_plain", 100, "dairy")], ["Warm milk with oats for a few minutes.", "Top with chopped apple and yogurt.", "Serve warm."], "Oat breakfast with explicit mapped ingredients."),
        recipe(3, "Egg Toast Spinach Plate", "breakfast_meal", ["breakfast"], "breakfast_competitor", "egg", "wholemeal bread", "spinach/tomato", 1, 5, 8, [i("Eggs", "egg_raw", 120, "protein"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 80, "carb_source"), i("Spinach", "spinach_raw", 60, "veg"), i("Tomato", "tomato_raw", 120, "veg"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Cook eggs with spinach in a small pan.", "Toast the bread.", "Serve with sliced tomato."], "Simple egg breakfast with carbs and veg."),
        recipe(4, "Potato Spinach Frittata", "breakfast_meal", ["breakfast"], "breakfast_competitor", "egg", "potato", "spinach", 1, 8, 15, [i("Eggs", "egg_raw", 130, "protein"), i("Cooked potato", "potato_cooked", 180, "carb_source"), i("Spinach", "spinach_raw", 70, "veg"), i("Cheddar cheese", "cheddar_cheese", 25, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Dice cooked potato.", "Cook spinach and potato briefly.", "Add beaten eggs and cheese and cook until set."], "Breakfast potato and egg alternative."),
        recipe(5, "Ham Cheese Egg Toast", "breakfast_meal", ["breakfast"], "breakfast_competitor", "egg/pork", "wholemeal bread", "tomato", 1, 5, 8, [i("Egg", "egg_raw", 70, "protein"), i("Cooked ham", "cooked_ham_choice_rind_less_and_fatless", 50, "protein"), i("Cheddar cheese", "cheddar_cheese", 25, "dairy"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 90, "carb_source"), i("Tomato", "tomato_raw", 100, "veg")], ["Cook the egg.", "Toast bread with ham and cheese.", "Serve with tomato."], "Mapped savory breakfast."),
        recipe(6, "Turkey Egg Breakfast Wrap", "breakfast_meal", ["breakfast"], "breakfast_competitor", "turkey/egg", "wheat wrap", "spinach", 1, 8, 8, [i("Wheat tortilla wrap", "wheat_tortilla_wrap_to_be_filled", 70, "carb_source"), i("Turkey cooked ham", "turkey_cooked_ham_in_slices", 70, "protein"), i("Egg", "egg_raw", 70, "protein"), i("Spinach", "spinach_raw", 50, "veg"), i("Tomato", "tomato_raw", 80, "veg")], ["Cook the egg.", "Warm the wrap.", "Fill with turkey, egg, spinach, and tomato."], "Turkey breakfast using existing canonical item."),
        recipe(7, "Tuna Tomato Toast Breakfast", "breakfast_meal", ["breakfast"], "breakfast_competitor", "fish", "wholemeal bread", "tomato", 1, 5, 0, [i("Plain canned tuna drained", "tuna_plain_canned_drained", 85, "protein"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 90, "carb_source"), i("Tomato", "tomato_raw", 120, "veg"), i("Greek style plain yogurt", "yogurt_greek_style_plain", 70, "dairy")], ["Mix tuna with yogurt.", "Toast bread.", "Serve tuna toast with tomato."], "Non-egg breakfast protein variety."),
        recipe(8, "Lentil Egg Breakfast Bowl", "breakfast_meal", ["breakfast"], "breakfast_competitor", "egg/legume", "lentils/potato", "tomato/spinach", 1, 8, 8, [i("Cooked lentils", "lentil_boiled_cooked_in_water", 120, "protein"), i("Egg", "egg_raw", 70, "protein"), i("Cooked potato", "potato_cooked", 120, "carb_source"), i("Spinach", "spinach_raw", 50, "veg"), i("Tomato", "tomato_raw", 80, "veg")], ["Warm lentils and potato.", "Cook the egg.", "Serve with spinach and tomato."], "Legume breakfast variety."),
        recipe(9, "Banana Milk Oat Bowl", "breakfast_meal", ["breakfast"], "breakfast_competitor", "dairy", "oats/banana", "banana", 1, 5, 5, [i("Raw oats", "oat_raw", 65, "carb_source"), i("Semi skimmed milk", "milk_semi_skimmed_pasteurised", 250, "dairy"), i("Banana", "banana_pulp_raw", 140, "fruit"), i("Cheddar cheese", "cheddar_cheese", 20, "dairy")], ["Warm oats with milk.", "Top with banana.", "Serve with cheese on the side."], "Carb-forward breakfast with mapped protein side."),
        recipe(10, "Spinach Cheese Omelette Toast", "breakfast_meal", ["breakfast"], "breakfast_competitor", "egg/dairy", "wholemeal bread", "spinach", 1, 5, 10, [i("Eggs", "egg_raw", 140, "protein"), i("Cheddar cheese", "cheddar_cheese", 30, "dairy"), i("Spinach", "spinach_raw", 70, "veg"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 70, "carb_source"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Cook spinach in oil.", "Add eggs and cheese.", "Serve omelette with toast."], "Compact savory breakfast."),
        recipe(11, "Potato Egg Pepper Skillet", "breakfast_meal", ["breakfast"], "breakfast_competitor", "egg", "potato", "pepper/onion", 1, 8, 12, [i("Cooked potato", "potato_cooked", 220, "carb_source"), i("Eggs", "egg_raw", 120, "protein"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 100, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook pepper and onion in oil.", "Add potato and warm through.", "Add eggs and cook until set."], "Breakfast alternative to waffles using potato carbs."),
        recipe(12, "Smoked Salmon Toast Plate", "breakfast_meal", ["breakfast"], "breakfast_competitor", "fish", "wholemeal bread", "cucumber/tomato", 1, 5, 0, [i("Smoked salmon", "salmon_smoked", 80, "protein"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 90, "carb_source"), i("Greek style plain yogurt", "yogurt_greek_style_plain", 80, "dairy"), i("Tomato", "tomato_raw", 100, "veg"), i("Lettuce", "lettuce_raw", 40, "veg")], ["Spread yogurt on toast.", "Add smoked salmon.", "Serve with tomato and lettuce."], "Fish breakfast variety."),
        recipe(13, "Fresh Cheese Banana Toast", "breakfast_meal", ["breakfast"], "breakfast_competitor", "dairy", "bread/banana", "banana", 1, 5, 0, [i("Soft fresh cheese", "drained_soft_fresh_cheese_around_6_fat", 140, "dairy"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 90, "carb_source"), i("Banana", "banana_pulp_raw", 120, "fruit"), i("Semi skimmed milk", "milk_semi_skimmed_pasteurised", 150, "dairy")], ["Spread fresh cheese on toast.", "Add banana slices.", "Serve with milk."], "Clean dairy breakfast without dessert framing."),
        recipe(14, "Rice Yogurt Apple Bowl", "breakfast_meal", ["breakfast"], "breakfast_competitor", "dairy", "rice/apple", "apple", 1, 5, 2, [i("Cooked rice", "rice_cooked_unsalted", 180, "carb_source"), i("Greek style plain yogurt", "yogurt_greek_style_plain", 160, "dairy"), i("Apple", "apple_pulp_and_peel_raw", 140, "fruit"), i("Semi skimmed milk", "milk_semi_skimmed_pasteurised", 80, "dairy")], ["Warm rice with milk.", "Top with yogurt and apple.", "Serve as a breakfast bowl."], "Carb breakfast without oats."),
        recipe(15, "Salmon Egg Potato Breakfast", "breakfast_meal", ["breakfast"], "breakfast_competitor", "fish/egg", "potato", "spinach", 1, 8, 10, [i("Canned salmon drained", "salmon_canned_drained", 80, "protein"), i("Egg", "egg_raw", 70, "protein"), i("Cooked potato", "potato_cooked", 170, "carb_source"), i("Spinach", "spinach_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Warm potato and spinach.", "Cook the egg.", "Serve with salmon."], "High-quality fish breakfast option."),
        recipe(16, "Chicken Rice Bowl with Vegetables", "complete_main", ["lunch", "dinner"], "carb_protein_main", "chicken", "rice", "broccoli/carrot", 1, 10, 18, [i("Chicken breast", "chicken_breast_without_skin_raw", 150, "protein"), i("Cooked rice", "rice_cooked_unsalted", 230, "carb_source"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 120, "veg"), i("Carrot", "carrot_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 10, "fat_source")], ["Cook chicken in a pan.", "Warm rice and vegetables.", "Serve together with olive oil."], "Core clean lunch/dinner meal."),
        recipe(17, "Chicken Couscous Vegetable Bowl", "complete_main", ["lunch", "dinner"], "carb_protein_main", "chicken", "couscous", "zucchini/pepper", 1, 10, 15, [i("Chicken breast", "chicken_breast_without_skin_raw", 150, "protein"), i("Cooked couscous", "couscous_precooked_durum_wheat_semolina_cooked_unsalted", 230, "carb_source"), i("Zucchini", "courgette_or_zucchini_pulp_and_peel_raw", 120, "veg"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 10, "fat_source")], ["Cook chicken and vegetables.", "Warm couscous.", "Combine in a bowl."], "Fast carb-protein main."),
        recipe(18, "Chicken Potato Tray Plate", "complete_main", ["lunch", "dinner"], "carb_protein_main", "chicken", "potato", "carrot/onion", 1, 10, 25, [i("Chicken thigh", "chicken_thigh_meat_and_skin_raw", 160, "protein"), i("Cooked potato", "potato_cooked", 260, "carb_source"), i("Carrot", "carrot_raw", 90, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Bake or pan-cook chicken.", "Serve with potato, carrot, and onion.", "Keep portion as one plate."], "Chicken thigh alternative with complete plate."),
        recipe(19, "Chicken Tomato Pasta", "complete_main", ["lunch", "dinner"], "carb_protein_main", "chicken", "pasta", "tomato/spinach", 1, 10, 18, [i("Chicken breast", "chicken_breast_without_skin_raw", 140, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 260, "carb_source"), i("Tomato puree", "tomato_puree_canned", 120, "veg"), i("Spinach", "spinach_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook chicken.", "Warm pasta with tomato puree and spinach.", "Serve chicken over pasta."], "Alternative to existing chicken pasta anchors."),
        recipe(20, "Chicken Bulgur Pepper Bowl", "complete_main", ["lunch", "dinner"], "carb_protein_main", "chicken", "bulgur", "pepper/onion", 1, 10, 16, [i("Chicken breast", "chicken_breast_without_skin_raw", 145, "protein"), i("Cooked bulgur", "wheat_bulgur_cooked_unsalted", 250, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 120, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 10, "fat_source")], ["Cook chicken with pepper and onion.", "Warm bulgur.", "Serve as a bowl."], "Adds bulgur carb variety."),
        recipe(21, "Chicken Lentil Rice Bowl", "complete_main", ["lunch", "dinner"], "carb_protein_main", "chicken/legume", "rice/lentils", "tomato", 1, 10, 16, [i("Chicken breast", "chicken_breast_without_skin_raw", 120, "protein"), i("Cooked rice", "rice_cooked_unsalted", 170, "carb_source"), i("Cooked lentils", "lentil_boiled_cooked_in_water", 140, "protein"), i("Tomato", "tomato_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook chicken.", "Warm rice and lentils.", "Serve with tomato."], "High protein/carb complete bowl."),
        recipe(22, "Chicken Sweetcorn Rice Plate", "complete_main", ["lunch", "dinner"], "carb_protein_main", "chicken", "rice/corn", "broccoli", 1, 10, 15, [i("Chicken breast", "chicken_breast_without_skin_raw", 145, "protein"), i("Cooked rice", "rice_cooked_unsalted", 190, "carb_source"), i("Sweet corn", "sweet_corn_canned_drained", 100, "carb_source"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook chicken.", "Warm rice, corn, and broccoli.", "Serve as one plate."], "Mapped chicken/corn main."),
        recipe(23, "Turkey Ham Pasta Peas", "complete_main", ["lunch", "dinner"], "carb_protein_main", "turkey", "pasta", "peas", 1, 8, 12, [i("Turkey cooked ham", "turkey_cooked_ham_in_slices", 130, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 280, "carb_source"), i("Garden peas", "garden_peas_frozen_cooked", 120, "veg"), i("Sour cream", "sour_cream_light", 50, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Warm pasta and peas.", "Add sliced turkey and sour cream.", "Serve warm."], "Turkey-like lunch using safe existing item."),
        recipe(24, "Turkey Ham Potato Pea Plate", "complete_main", ["lunch", "dinner"], "carb_protein_main", "turkey", "potato", "peas", 1, 8, 12, [i("Turkey cooked ham", "turkey_cooked_ham_in_slices", 140, "protein"), i("Cooked potato", "potato_cooked", 300, "carb_source"), i("Garden peas", "garden_peas_frozen_cooked", 130, "veg"), i("Cheddar cheese", "cheddar_cheese", 25, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Warm potato and peas.", "Add turkey slices and cheese.", "Serve as a plate."], "Useful turkey category until better raw turkey source exists."),
        recipe(25, "Turkey Egg Rice Bowl", "complete_main", ["lunch", "dinner"], "carb_protein_main", "turkey/egg", "rice", "pepper", 1, 8, 12, [i("Turkey cooked ham", "turkey_cooked_ham_in_slices", 100, "protein"), i("Egg", "egg_raw", 70, "protein"), i("Cooked rice", "rice_cooked_unsalted", 240, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook egg with pepper.", "Warm rice and turkey.", "Serve together."], "Turkey/egg no-repeat support."),
        recipe(26, "Salmon Potato Green Bean Plate", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "fish", "potato", "green beans", 1, 8, 12, [i("Canned salmon drained", "salmon_canned_drained", 130, "protein"), i("Cooked potato", "potato_cooked", 280, "carb_source"), i("Green beans", "green_beans_cooked_unsalted", 150, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Warm potato and green beans.", "Add salmon.", "Serve with olive oil."], "Clean fish complete meal."),
        recipe(27, "Salmon Spinach Pasta Bowl", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "fish", "pasta", "spinach", 1, 8, 12, [i("Canned salmon drained", "salmon_canned_drained", 120, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 270, "carb_source"), i("Spinach", "spinach_raw", 80, "veg"), i("Greek style plain yogurt", "yogurt_greek_style_plain", 70, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Warm pasta with spinach.", "Add salmon and yogurt.", "Serve as one bowl."], "Fish pasta alternative."),
        recipe(28, "Tuna Tomato Pasta", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "fish", "pasta", "tomato", 1, 8, 12, [i("Plain canned tuna drained", "tuna_plain_canned_drained", 120, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 290, "carb_source"), i("Tomato puree", "tomato_puree_canned", 130, "veg"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Warm pasta with tomato puree and pepper.", "Add tuna.", "Serve hot."], "Cheap fish carb-protein main."),
        recipe(29, "Tuna Rice Sweetcorn Bowl", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "fish", "rice/corn", "lettuce", 1, 8, 5, [i("Plain canned tuna drained", "tuna_plain_canned_drained", 120, "protein"), i("Cooked rice", "rice_cooked_unsalted", 240, "carb_source"), i("Sweet corn", "sweet_corn_canned_drained", 120, "carb_source"), i("Lettuce", "lettuce_raw", 60, "veg"), i("Greek style plain yogurt", "yogurt_greek_style_plain", 60, "dairy")], ["Warm rice if desired.", "Mix tuna, corn, and yogurt.", "Serve with lettuce."], "Low-time fish bowl."),
        recipe(30, "Smoked Salmon Potato Bowl", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "fish", "potato", "spinach", 1, 6, 5, [i("Smoked salmon", "salmon_smoked", 110, "protein"), i("Cooked potato", "potato_cooked", 280, "carb_source"), i("Spinach", "spinach_raw", 80, "veg"), i("Sour cream light", "sour_cream_light", 60, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Warm potato.", "Add salmon, spinach, and sour cream.", "Serve as a bowl."], "Fish/potato no-repeat support."),
        recipe(31, "Beef Potato Stew Plate", "complete_main", ["lunch", "dinner"], "carb_protein_main", "beef", "potato", "carrot/onion", 1, 10, 30, [i("Beef round steak", "beef_round_steak_raw", 150, "protein"), i("Cooked potato", "potato_cooked", 300, "carb_source"), i("Carrot", "carrot_raw", 100, "veg"), i("Onion", "onion_raw", 70, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook beef pieces.", "Simmer with carrot and onion.", "Serve with cooked potato."], "Specific beef cut, no generic beef."),
        recipe(32, "Beef Rice Pepper Bowl", "complete_main", ["lunch", "dinner"], "carb_protein_main", "beef", "rice", "pepper/onion", 1, 10, 20, [i("Beef round steak", "beef_round_steak_raw", 150, "protein"), i("Cooked rice", "rice_cooked_unsalted", 260, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 120, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook beef with pepper and onion.", "Warm rice.", "Serve as a bowl."], "Mapped beef rice main."),
        recipe(33, "Beef Tomato Pasta", "complete_main", ["lunch", "dinner"], "carb_protein_main", "beef", "pasta", "tomato", 1, 10, 20, [i("Beef round steak", "beef_round_steak_raw", 140, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 280, "carb_source"), i("Tomato puree", "tomato_puree_canned", 140, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook beef strips.", "Warm pasta with tomato puree and onion.", "Serve together."], "Beef pasta without vague bolognese mapping."),
        recipe(34, "Beef Lentil Potato Bowl", "complete_main", ["lunch", "dinner"], "carb_protein_main", "beef/legume", "potato/lentils", "carrot", 1, 10, 20, [i("Beef round steak", "beef_round_steak_raw", 120, "protein"), i("Cooked potato", "potato_cooked", 230, "carb_source"), i("Cooked lentils", "lentil_boiled_cooked_in_water", 130, "protein"), i("Carrot", "carrot_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Cook beef.", "Warm lentils and potato.", "Serve with carrot."], "High utility beef/legume meal."),
        recipe(35, "Beef Couscous Mushroom Bowl", "complete_main", ["lunch", "dinner"], "carb_protein_main", "beef", "couscous", "mushroom", 1, 10, 18, [i("Beef round steak", "beef_round_steak_raw", 140, "protein"), i("Cooked couscous", "couscous_precooked_durum_wheat_semolina_cooked_unsalted", 250, "carb_source"), i("Button mushroom", "button_mushroom_or_cultivated_mushroom_raw", 120, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook beef with mushrooms and onion.", "Warm couscous.", "Serve as a bowl."], "Beef carb variety."),
        recipe(36, "Pork Rice Vegetable Bowl", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "pork", "rice", "broccoli/pepper", 1, 10, 18, [i("Pork tenderloin", "pork_tenderloin_lean_raw", 150, "protein"), i("Cooked rice", "rice_cooked_unsalted", 250, "carb_source"), i("Broccoli", "broccoli_boiled_cooked_in_water_tender", 100, "veg"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook pork strips.", "Warm rice and vegetables.", "Serve as a bowl."], "Specific pork complete meal."),
        recipe(37, "Pork Potato Carrot Plate", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "pork", "potato", "carrot/onion", 1, 10, 22, [i("Pork tenderloin", "pork_tenderloin_lean_raw", 150, "protein"), i("Cooked potato", "potato_cooked", 300, "carb_source"), i("Carrot", "carrot_raw", 100, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook pork.", "Serve with potato, carrot, and onion.", "Keep as one complete plate."], "Pork potato main."),
        recipe(38, "Pork Pasta Peas", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "pork", "pasta", "peas", 1, 10, 16, [i("Pork tenderloin", "pork_tenderloin_lean_raw", 140, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 280, "carb_source"), i("Garden peas", "garden_peas_frozen_cooked", 130, "veg"), i("Sour cream light", "sour_cream_light", 50, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Cook pork.", "Warm pasta, peas, and sour cream.", "Serve together."], "Pork pasta no-repeat support."),
        recipe(39, "Pork Bulgur Pepper Bowl", "complete_main", ["lunch", "dinner"], "fish_turkey_pork_main", "pork", "bulgur", "pepper", 1, 10, 16, [i("Pork tenderloin", "pork_tenderloin_lean_raw", 150, "protein"), i("Cooked bulgur", "wheat_bulgur_cooked_unsalted", 260, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 120, "veg"), i("Onion", "onion_raw", 60, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook pork with pepper and onion.", "Warm bulgur.", "Serve as a bowl."], "Pork plus bulgur variety."),
        recipe(40, "Lentil Rice Tomato Bowl", "complete_main", ["lunch", "dinner"], "vegetarian_legume_balanced", "legume", "rice/lentils", "tomato/spinach", 1, 8, 12, [i("Cooked lentils", "lentil_boiled_cooked_in_water", 220, "protein"), i("Cooked rice", "rice_cooked_unsalted", 220, "carb_source"), i("Tomato puree", "tomato_puree_canned", 120, "veg"), i("Spinach", "spinach_raw", 70, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Warm lentils, rice, and tomato puree.", "Stir in spinach.", "Serve as one bowl."], "Vegetarian/legume balanced meal."),
        recipe(41, "Kidney Bean Rice Bowl", "complete_main", ["lunch", "dinner"], "vegetarian_legume_balanced", "legume", "rice/beans", "pepper/tomato", 1, 8, 12, [i("Cooked kidney beans", "red_kidney_bean_boiled_cooked_in_water", 220, "protein"), i("Cooked rice", "rice_cooked_unsalted", 220, "carb_source"), i("Sweet pepper", "sweet_pepper_green_yellow_or_red_raw", 110, "veg"), i("Tomato", "tomato_raw", 100, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Warm beans and rice.", "Add pepper and tomato.", "Serve with olive oil."], "Legume carb-protein complete meal."),
        recipe(42, "Bean Pasta Tomato Bowl", "complete_main", ["lunch", "dinner"], "vegetarian_legume_balanced", "legume", "pasta/beans", "tomato/spinach", 1, 8, 12, [i("Cooked kidney beans", "red_kidney_bean_boiled_cooked_in_water", 180, "protein"), i("Cooked pasta", "dried_pasta_cooked_unsalted", 260, "carb_source"), i("Tomato puree", "tomato_puree_canned", 120, "veg"), i("Spinach", "spinach_raw", 70, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Warm pasta with tomato puree.", "Add beans and spinach.", "Serve as one bowl."], "Vegetarian pasta main."),
        recipe(43, "Lentil Potato Stew Bowl", "complete_main", ["lunch", "dinner"], "vegetarian_legume_balanced", "legume", "potato/lentils", "carrot/onion", 1, 8, 18, [i("Cooked lentils", "lentil_boiled_cooked_in_water", 240, "protein"), i("Cooked potato", "potato_cooked", 250, "carb_source"), i("Carrot", "carrot_raw", 100, "veg"), i("Onion", "onion_raw", 70, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Warm lentils and potato.", "Cook carrot and onion briefly.", "Serve as a stew bowl."], "Vegetarian stew-style complete meal."),
        recipe(44, "Egg Fried Rice Vegetables", "complete_main", ["lunch", "dinner"], "vegetarian_legume_balanced", "egg", "rice", "peas/carrot", 1, 8, 10, [i("Eggs", "egg_raw", 120, "protein"), i("Cooked rice", "rice_cooked_unsalted", 260, "carb_source"), i("Garden peas", "garden_peas_frozen_cooked", 120, "veg"), i("Carrot", "carrot_raw", 80, "veg"), i("Olive oil", "olive_oil_extra_virgin", 8, "fat_source")], ["Cook eggs in a pan.", "Add rice, peas, and carrot.", "Stir-fry briefly."], "Vegetarian-ish complete meal."),
        recipe(45, "Egg Potato Spinach Bowl", "complete_main", ["lunch", "dinner"], "vegetarian_legume_balanced", "egg", "potato", "spinach", 1, 8, 12, [i("Eggs", "egg_raw", 130, "protein"), i("Cooked potato", "potato_cooked", 300, "carb_source"), i("Spinach", "spinach_raw", 100, "veg"), i("Cheddar cheese", "cheddar_cheese", 30, "dairy"), i("Olive oil", "olive_oil_extra_virgin", 5, "fat_source")], ["Warm potato and spinach.", "Cook eggs and add cheese.", "Serve as a bowl."], "Egg-based complete lunch/dinner."),
        recipe(46, "Yogurt Banana Oat Snack", "snack", ["snack"], "snack", "dairy", "oats/banana", "banana", 1, 3, 0, [i("Greek style plain yogurt", "yogurt_greek_style_plain", 100, "dairy"), i("Raw oats", "oat_raw", 25, "carb_source"), i("Banana", "banana_pulp_raw", 80, "fruit")], ["Mix yogurt and oats.", "Top with banana.", "Serve cold."], "Small mapped snack."),
        recipe(47, "Egg Toast Snack", "snack", ["snack"], "snack", "egg", "wholemeal bread", "tomato", 1, 3, 8, [i("Egg", "egg_raw", 60, "protein"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 45, "carb_source"), i("Tomato", "tomato_raw", 60, "veg")], ["Cook the egg.", "Serve on toast with tomato."], "Small snack, not a main dish."),
        recipe(48, "Ham Cheese Bread Snack", "snack", ["snack"], "snack", "pork/dairy", "wholemeal bread", "none", 1, 3, 0, [i("Cooked ham", "cooked_ham_choice_rind_less_and_fatless", 35, "protein"), i("Cheddar cheese", "cheddar_cheese", 20, "dairy"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 45, "carb_source")], ["Layer ham and cheese on bread.", "Serve as a small snack."], "Mapped savory snack."),
        recipe(49, "Apple Fresh Cheese Snack", "snack", ["snack"], "snack", "dairy", "apple", "apple", 1, 3, 0, [i("Soft fresh cheese", "drained_soft_fresh_cheese_around_6_fat", 120, "dairy"), i("Apple", "apple_pulp_and_peel_raw", 130, "fruit")], ["Slice apple.", "Serve with fresh cheese."], "Clean fruit/dairy snack."),
        recipe(50, "Tuna Bread Snack Plate", "snack", ["snack"], "snack", "fish", "wholemeal bread", "lettuce", 1, 3, 0, [i("Plain canned tuna drained", "tuna_plain_canned_drained", 55, "protein"), i("Wholemeal bread", "bread_wholemeal_or_integral_bread_made_with_flour_type_150", 50, "carb_source"), i("Lettuce", "lettuce_raw", 40, "veg"), i("Greek style plain yogurt", "yogurt_greek_style_plain", 30, "dairy")], ["Mix tuna with yogurt.", "Serve with bread and lettuce."], "Small protein snack."),
    ]


def expected_ranges_for_recipe(row: dict[str, Any]) -> tuple[str, str, str]:
    slots = json.loads(row["allowed_slots_json"])
    if slots == ["breakfast"]:
        return "250-800", "8-35", "20-95"
    if slots == ["snack"]:
        return "80-350", "5-25", "10-45"
    return "350-900", "14-60", "25-115"
