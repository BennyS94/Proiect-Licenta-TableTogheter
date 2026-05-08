from __future__ import annotations

import csv
import json
import math
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

FOODDB_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round9.csv"
REFERENCE_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready_slot_checked_time_enriched"
REFERENCE_RECIPES_PATH = REFERENCE_DIR / "recipes.csv"
REFERENCE_INGREDIENTS_PATH = REFERENCE_DIR / "recipe_ingredients.csv"
REFERENCE_NUTRITION_PATH = REFERENCE_DIR / "recipe_nutrition_cache.csv"

OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_manual_snacks"
OUT_RECIPES = OUT_DIR / "recipes.csv"
OUT_INGREDIENTS = OUT_DIR / "recipe_ingredients.csv"
OUT_NUTRITION = OUT_DIR / "recipe_nutrition_cache.csv"
OUT_README = OUT_DIR / "README_v1_1_manual_snacks.txt"
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_manual_snacks_summary.txt"
OUT_MATCH_AUDIT = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_manual_snacks_fooddb_match_audit.csv"

MANUAL_SNACK_VERSION = "round12"

FOOD_KEYS = {
    "greek_yogurt": ["yogurt_greek_style_plain"],
    "plain_yogurt": ["yogurt_fermented_milk_or_dairy_specialty_plain"],
    "banana": ["banana_pulp_raw"],
    "apple": ["apple_pulp_and_peel_raw"],
    "strawberry": ["strawberry_raw"],
    "blueberry": ["blueberry_raw"],
    "oats": ["oat_raw"],
    "milk": ["milk_semi_skimmed_uht"],
    "hard_boiled_egg": ["egg_hard_boiled"],
    "wholemeal_bread": ["bread_wholemeal_or_integral_bread_made_with_flour_type_150"],
    "carrot": ["carrot_raw"],
    "mozzarella": ["mozzarella_cheese_from_cow_s_milk"],
    "ricotta": ["ricotta_cheese"],
    "ham": ["cooked_ham_superior_quality_rind_less_and_fatless"],
    "turkey_ham": ["turkey_cooked_ham_in_slices"],
    "cottage_cheese": ["cottage_cheese"],
    "hummus": ["hummus_plain"],
}

SNACK_CANDIDATES = [
    {
        "slug": "greek_yogurt_banana",
        "display_name": "Greek Yogurt with Banana",
        "ingredients": [("greek_yogurt", 170, "Greek-style plain yogurt", "dairy"), ("banana", 80, "banana", "fruit")],
        "prep_time": 5,
        "cook_time": 0,
        "directions": ["Slice the banana.", "Serve it with the yogurt in a small bowl."],
    },
    {
        "slug": "greek_yogurt_oats",
        "display_name": "Greek Yogurt with Oats",
        "ingredients": [("greek_yogurt", 150, "Greek-style plain yogurt", "dairy"), ("oats", 25, "oats", "carb_source")],
        "prep_time": 5,
        "cook_time": 0,
        "directions": ["Stir the oats into the yogurt.", "Let stand briefly if a softer texture is preferred."],
    },
    {
        "slug": "greek_yogurt_apple",
        "display_name": "Greek Yogurt with Apple",
        "ingredients": [("greek_yogurt", 170, "Greek-style plain yogurt", "dairy"), ("apple", 120, "apple", "fruit")],
        "prep_time": 7,
        "cook_time": 0,
        "directions": ["Dice the apple.", "Serve it over the yogurt."],
    },
    {
        "slug": "greek_yogurt_strawberries",
        "display_name": "Greek Yogurt with Strawberries",
        "ingredients": [("greek_yogurt", 170, "Greek-style plain yogurt", "dairy"), ("strawberry", 120, "strawberries", "fruit")],
        "prep_time": 7,
        "cook_time": 0,
        "directions": ["Slice the strawberries.", "Serve them with the yogurt."],
    },
    {
        "slug": "greek_yogurt_blueberries",
        "display_name": "Greek Yogurt with Blueberries",
        "ingredients": [("greek_yogurt", 170, "Greek-style plain yogurt", "dairy"), ("blueberry", 100, "blueberries", "fruit")],
        "prep_time": 5,
        "cook_time": 0,
        "directions": ["Rinse the blueberries.", "Serve them with the yogurt."],
    },
    {
        "slug": "apple_plain_yogurt",
        "display_name": "Apple with Plain Yogurt",
        "ingredients": [("apple", 150, "apple", "fruit"), ("plain_yogurt", 170, "plain yogurt", "dairy")],
        "prep_time": 7,
        "cook_time": 0,
        "directions": ["Slice the apple.", "Serve it with plain yogurt."],
    },
    {
        "slug": "carrot_yogurt_dip",
        "display_name": "Carrot Sticks with Yogurt Dip",
        "ingredients": [("carrot", 120, "carrot sticks", "veg"), ("plain_yogurt", 150, "plain yogurt", "dairy")],
        "prep_time": 10,
        "cook_time": 0,
        "directions": ["Cut the carrot into sticks.", "Serve with plain yogurt as a simple dip."],
    },
    {
        "slug": "milk_oats",
        "display_name": "Milk with Oats",
        "ingredients": [("milk", 200, "semi-skimmed milk", "dairy"), ("oats", 25, "oats", "carb_source")],
        "prep_time": 5,
        "cook_time": 0,
        "directions": ["Stir the oats into the milk.", "Serve cold or let stand briefly."],
    },
    {
        "slug": "banana_milk",
        "display_name": "Banana with Milk",
        "ingredients": [("banana", 90, "banana", "fruit"), ("milk", 200, "semi-skimmed milk", "dairy")],
        "prep_time": 5,
        "cook_time": 0,
        "directions": ["Slice the banana.", "Serve it with a glass of milk."],
    },
    {
        "slug": "boiled_eggs_toast",
        "display_name": "Boiled Eggs with Wholemeal Toast",
        "ingredients": [("hard_boiled_egg", 100, "hard-boiled egg", "protein_source"), ("wholemeal_bread", 40, "wholemeal bread", "carb_source")],
        "prep_time": 5,
        "cook_time": 10,
        "directions": ["Boil the eggs until firm.", "Serve with a small slice of wholemeal bread."],
    },
    {
        "slug": "egg_bread_snack",
        "display_name": "Egg and Bread Snack",
        "ingredients": [("hard_boiled_egg", 50, "hard-boiled egg", "protein_source"), ("wholemeal_bread", 45, "wholemeal bread", "carb_source")],
        "prep_time": 5,
        "cook_time": 10,
        "directions": ["Prepare one hard-boiled egg.", "Serve it with wholemeal bread."],
    },
    {
        "slug": "cheese_bread_snack",
        "display_name": "Cheese and Bread Snack",
        "ingredients": [("mozzarella", 35, "mozzarella", "protein_source"), ("wholemeal_bread", 40, "wholemeal bread", "carb_source")],
        "prep_time": 5,
        "cook_time": 0,
        "directions": ["Slice the cheese.", "Serve with wholemeal bread."],
    },
    {
        "slug": "ham_cheese_toast",
        "display_name": "Ham and Cheese Toast",
        "ingredients": [
            ("ham", 40, "cooked ham", "protein_source"),
            ("mozzarella", 25, "mozzarella", "protein_source"),
            ("wholemeal_bread", 40, "wholemeal bread", "carb_source"),
        ],
        "prep_time": 5,
        "cook_time": 3,
        "directions": ["Layer ham and cheese on the bread.", "Toast briefly until warm."],
    },
    {
        "slug": "turkey_bread_snack",
        "display_name": "Small Turkey Bread Snack",
        "ingredients": [("turkey_ham", 50, "turkey cooked ham", "protein_source"), ("wholemeal_bread", 45, "wholemeal bread", "carb_source")],
        "prep_time": 5,
        "cook_time": 0,
        "directions": ["Place the turkey slices on the bread.", "Serve as a small snack plate."],
    },
    {
        "slug": "ricotta_apple",
        "display_name": "Ricotta with Apple",
        "ingredients": [("ricotta", 100, "ricotta", "dairy"), ("apple", 120, "apple", "fruit")],
        "prep_time": 7,
        "cook_time": 0,
        "directions": ["Dice the apple.", "Serve it with ricotta."],
    },
    {
        "slug": "cottage_cheese_fruit",
        "display_name": "Cottage Cheese with Fruit",
        "ingredients": [("cottage_cheese", 120, "cottage cheese", "dairy"), ("apple", 120, "apple", "fruit")],
        "prep_time": 7,
        "cook_time": 0,
        "directions": ["Serve cottage cheese with sliced fruit."],
    },
    {
        "slug": "hummus_carrots",
        "display_name": "Hummus with Carrots",
        "ingredients": [("hummus", 60, "hummus", "protein_source"), ("carrot", 120, "carrot sticks", "veg")],
        "prep_time": 10,
        "cook_time": 0,
        "directions": ["Cut the carrot into sticks.", "Serve with hummus."],
    },
]

MIN_SNACK_KCAL = 80
MAX_SNACK_KCAL = 350


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


def parse_float(value: object) -> float:
    text = clean_text(value)
    if not text:
        return 0.0
    try:
        parsed = float(text)
    except ValueError:
        return 0.0
    if math.isnan(parsed) or math.isinf(parsed):
        return 0.0
    return parsed


def round_number(value: float) -> str:
    rounded = round(value, 4)
    text = f"{rounded:.4f}".rstrip("0").rstrip(".")
    return text or "0"


def with_required_columns(base_columns: list[str], required_columns: list[str]) -> list[str]:
    columns = list(base_columns)
    for column in required_columns:
        if column not in columns:
            columns.append(column)
    return columns


def fooddb_by_canonical(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {clean_text(row.get("canonical_name")): row for row in rows if clean_text(row.get("canonical_name"))}


def match_food(food_key: str, food_by_name: dict[str, dict[str, str]]) -> tuple[dict[str, str] | None, str]:
    for canonical_name in FOOD_KEYS.get(food_key, []):
        food = food_by_name.get(canonical_name)
        if not food:
            continue
        if not all(clean_text(food.get(column)) for column in MACRO_COLUMNS):
            return None, f"macro_missing:{canonical_name}"
        return food, f"exact_fooddb_round9:{canonical_name}"
    return None, f"missing_fooddb_key:{food_key}"


MACRO_COLUMNS = ["energy_kcal_100g", "protein_g_100g", "carbs_g_100g", "fat_g_100g"]


def macro_total(food: dict[str, str], grams: float, column: str) -> float:
    return parse_float(food.get(column)) * grams / 100.0


def build_recipe_rows(
    recipe_columns: list[str],
    ingredient_columns: list[str],
    nutrition_columns: list[str],
    food_by_name: dict[str, dict[str, str]],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    recipes: list[dict[str, object]] = []
    ingredients: list[dict[str, object]] = []
    nutrition_rows: list[dict[str, object]] = []
    match_audit: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []

    for candidate in SNACK_CANDIDATES:
        matches: list[tuple[str, float, str, str, dict[str, str], str]] = []
        missing_reasons: list[str] = []
        for food_key, grams, label, role in candidate["ingredients"]:
            food, reason = match_food(food_key, food_by_name)
            if food is None:
                missing_reasons.append(reason)
                match_audit.append(skipped_match_row(candidate, food_key, label, grams, reason))
                continue
            matches.append((food_key, float(grams), label, role, food, reason))
            match_audit.append(match_row(candidate, food_key, label, grams, role, food, "matched", reason))

        if missing_reasons:
            skipped.append(
                {
                    "recipe_slug": candidate["slug"],
                    "display_name": candidate["display_name"],
                    "decision": "skipped",
                    "reason": "|".join(sorted(set(missing_reasons))),
                }
            )
            continue

        totals = nutrition_totals(matches)
        if totals["energy_kcal_total"] < MIN_SNACK_KCAL or totals["energy_kcal_total"] > MAX_SNACK_KCAL:
            reason = (
                "nutrition_outside_snack_range:"
                f"{round_number(totals['energy_kcal_total'])}_kcal"
            )
            skipped.append(
                {
                    "recipe_slug": candidate["slug"],
                    "display_name": candidate["display_name"],
                    "decision": "skipped",
                    "reason": reason,
                }
            )
            match_audit.append(recipe_skip_row(candidate, reason))
            continue

        recipe_number = len(recipes) + 1
        recipe_id = f"manual_snack_v1_1_round12_{recipe_number:03d}"
        recipes.append(recipe_row(recipe_id, candidate, totals, recipe_columns))
        ingredients.extend(ingredient_rows(recipe_id, candidate, matches, ingredient_columns))
        nutrition_rows.append(nutrition_row(recipe_id, totals, len(matches), nutrition_columns))

    return recipes, ingredients, nutrition_rows, match_audit, skipped


def nutrition_totals(matches: list[tuple[str, float, str, str, dict[str, str], str]]) -> dict[str, float]:
    totals = {
        "total_weight_grams_estimated": 0.0,
        "energy_kcal_total": 0.0,
        "protein_g_total": 0.0,
        "carbs_g_total": 0.0,
        "fat_g_total": 0.0,
    }
    for _, grams, _, _, food, _ in matches:
        totals["total_weight_grams_estimated"] += grams
        totals["energy_kcal_total"] += macro_total(food, grams, "energy_kcal_100g")
        totals["protein_g_total"] += macro_total(food, grams, "protein_g_100g")
        totals["carbs_g_total"] += macro_total(food, grams, "carbs_g_100g")
        totals["fat_g_total"] += macro_total(food, grams, "fat_g_100g")
    return totals


def empty_row(fieldnames: list[str]) -> dict[str, object]:
    return {field: "" for field in fieldnames}


def recipe_row(
    recipe_id: str,
    candidate: dict[str, object],
    totals: dict[str, float],
    fieldnames: list[str],
) -> dict[str, object]:
    prep_time = float(candidate["prep_time"])
    cook_time = float(candidate["cook_time"])
    total_time = prep_time + cook_time
    row = empty_row(fieldnames)
    row.update(
        {
            "recipe_id": recipe_id,
            "source_recipe_id": "",
            "source_dataset": "manual_curated_snacks_v1_1_round12",
            "recipe_name": candidate["display_name"],
            "display_name": candidate["display_name"],
            "recipe_family_name": candidate["display_name"],
            "recipe_kind": "snack",
            "recipe_category": "snack",
            "recipe_subcategory": "manual_curated",
            "recipe_cuisine": "european_practical",
            "directions_json": json.dumps(candidate["directions"], ensure_ascii=False),
            "directions_step_count": len(candidate["directions"]),
            "servings_declared": "1",
            "servings_normalized": "1",
            "prep_time_min": round_number(prep_time),
            "cook_time_min": round_number(cook_time),
            "total_time_min": round_number(total_time),
            "difficulty_level": "easy",
            "scope_status": "v1_1_generator_ready_draft",
            "has_ingredients_parsed": "1",
            "has_nutrition_cache": "1",
            "is_pilot_recipe": "0",
            "is_active": "1",
            "qc_recipe_status": "generator_ready_draft",
            "qc_notes": (
                "manual_snack_v1_1_round12; "
                "fooddb_v1_1_round9_exact_matches; "
                f"kcal={round_number(totals['energy_kcal_total'])}"
            ),
            "allowed_slots_json": json.dumps(["snack"]),
            "slot_policy_reason": "manual_round12_snack_only",
            "content_quality_status": "keep",
            "content_exclusion_reason": "",
            "active_time_estimated_min": round_number(total_time),
            "passive_time_estimated_min": "0",
            "effective_time_min_for_scoring": round_number(total_time),
            "has_long_passive_time": "False",
            "time_estimation_confidence": "high",
            "time_estimation_method": "manual_curated_time_round12",
            "time_estimation_reasons": "manual_curated_quick_snack",
        }
    )
    return row


def ingredient_rows(
    recipe_id: str,
    candidate: dict[str, object],
    matches: list[tuple[str, float, str, str, dict[str, str], str]],
    fieldnames: list[str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, (food_key, grams, label, role, food, reason) in enumerate(matches, start=1):
        row = empty_row(fieldnames)
        row.update(
            {
                "recipe_ingredient_id": f"{recipe_id}_ingredient_{index:03d}",
                "recipe_id": recipe_id,
                "ingredient_position": str(index),
                "ingredient_raw_text": f"{round_number(grams)} g {label}",
                "ingredient_name_parsed": label,
                "ingredient_name_normalized": label.lower().replace(" ", "_"),
                "quantity_value": round_number(grams),
                "quantity_unit": "g",
                "quantity_text": f"{round_number(grams)} g",
                "quantity_grams_estimated": round_number(grams),
                "ingredient_role": role,
                "ingredient_slot_key": food_key,
                "is_optional": "0",
                "is_substitutable": "1",
                "substitution_group_id": "",
                "mapped_food_id": clean_text(food.get("food_id")),
                "mapped_food_canonical_name": clean_text(food.get("canonical_name")),
                "mapping_status": "accepted_auto",
                "mapping_confidence": "high",
                "mapping_method": "manual_snack_fooddb_match_round12",
                "mapping_notes": reason,
                "qc_ingredient_status": "accepted_auto_round12",
                "qc_notes": "manual_snack_v1_1_round12",
            }
        )
        rows.append(row)
    return rows


def nutrition_row(
    recipe_id: str,
    totals: dict[str, float],
    mapped_count: int,
    fieldnames: list[str],
) -> dict[str, object]:
    row = empty_row(fieldnames)
    row.update(
        {
            "recipe_id": recipe_id,
            "nutrition_basis": "manual_snack_fooddb_v1_1_round12",
            "servings_basis": "1",
            "total_weight_grams_estimated": round_number(totals["total_weight_grams_estimated"]),
            "energy_kcal_total": round_number(totals["energy_kcal_total"]),
            "protein_g_total": round_number(totals["protein_g_total"]),
            "carbs_g_total": round_number(totals["carbs_g_total"]),
            "fat_g_total": round_number(totals["fat_g_total"]),
            "energy_kcal_per_serving": round_number(totals["energy_kcal_total"]),
            "protein_g_per_serving": round_number(totals["protein_g_total"]),
            "carbs_g_per_serving": round_number(totals["carbs_g_total"]),
            "fat_g_per_serving": round_number(totals["fat_g_total"]),
            "mapped_ingredient_count": str(mapped_count),
            "unmapped_ingredient_count": "0",
            "mapped_weight_ratio": "1.0",
            "cache_status": "usable_from_mapped_ingredients",
            "cache_version": "recipes_v1_1_manual_snacks_round12",
            "qc_notes": "manual_snack_v1_1_round12; exact_fooddb_round9_macro_sum",
            "macro_relevant_mapped_weight_ratio": "1.0",
            "uses_pilot_servings_fallback": "False",
            "servings_estimation_method": "manual_round12_one_serving",
            "servings_adjustment_applied": "false",
            "original_servings_basis": "1",
            "adjusted_servings_basis": "1",
            "quality_flags": "",
        }
    )
    return row


def match_row(
    candidate: dict[str, object],
    food_key: str,
    label: str,
    grams: float,
    role: str,
    food: dict[str, str],
    status: str,
    reason: str,
) -> dict[str, object]:
    return {
        "recipe_slug": candidate["slug"],
        "display_name": candidate["display_name"],
        "ingredient_label": label,
        "ingredient_food_key": food_key,
        "grams": round_number(float(grams)),
        "ingredient_role": role,
        "match_status": status,
        "matched_food_id": clean_text(food.get("food_id")),
        "matched_canonical_name": clean_text(food.get("canonical_name")),
        "energy_kcal_100g": clean_text(food.get("energy_kcal_100g")),
        "protein_g_100g": clean_text(food.get("protein_g_100g")),
        "carbs_g_100g": clean_text(food.get("carbs_g_100g")),
        "fat_g_100g": clean_text(food.get("fat_g_100g")),
        "decision_notes": reason,
    }


def skipped_match_row(
    candidate: dict[str, object],
    food_key: str,
    label: str,
    grams: float,
    reason: str,
) -> dict[str, object]:
    return {
        "recipe_slug": candidate["slug"],
        "display_name": candidate["display_name"],
        "ingredient_label": label,
        "ingredient_food_key": food_key,
        "grams": round_number(float(grams)),
        "ingredient_role": "",
        "match_status": "missing",
        "matched_food_id": "",
        "matched_canonical_name": "",
        "energy_kcal_100g": "",
        "protein_g_100g": "",
        "carbs_g_100g": "",
        "fat_g_100g": "",
        "decision_notes": reason,
    }


def recipe_skip_row(candidate: dict[str, object], reason: str) -> dict[str, object]:
    return {
        "recipe_slug": candidate["slug"],
        "display_name": candidate["display_name"],
        "ingredient_label": "",
        "ingredient_food_key": "",
        "grams": "",
        "ingredient_role": "",
        "match_status": "recipe_skipped",
        "matched_food_id": "",
        "matched_canonical_name": "",
        "energy_kcal_100g": "",
        "protein_g_100g": "",
        "carbs_g_100g": "",
        "fat_g_100g": "",
        "decision_notes": reason,
    }


def build_summary(
    recipes: list[dict[str, object]],
    nutrition_rows: list[dict[str, object]],
    skipped: list[dict[str, object]],
    match_audit: list[dict[str, object]],
) -> str:
    kcal_values = [parse_float(row.get("energy_kcal_per_serving")) for row in nutrition_rows]
    protein_values = [parse_float(row.get("protein_g_per_serving")) for row in nutrition_rows]
    skipped_counter = Counter(clean_text(row.get("reason")) for row in skipped)
    lines = [
        "Recipes_DB v1.1 manual snacks round12",
        "=" * 39,
        "",
        f"manual_snacks_created: {len(recipes)}",
        f"manual_candidates_skipped: {len(skipped)}",
        f"fooddb_match_audit_rows: {len(match_audit)}",
        f"min_kcal_per_serving: {round_number(min(kcal_values) if kcal_values else 0)}",
        f"max_kcal_per_serving: {round_number(max(kcal_values) if kcal_values else 0)}",
        f"min_protein_g_per_serving: {round_number(min(protein_values) if protein_values else 0)}",
        f"max_protein_g_per_serving: {round_number(max(protein_values) if protein_values else 0)}",
        "",
        "Created snacks:",
    ]
    for recipe, nutrition in zip(recipes, nutrition_rows):
        lines.append(
            "- "
            + clean_text(recipe.get("recipe_id"))
            + " | "
            + clean_text(recipe.get("display_name"))
            + " | kcal="
            + clean_text(nutrition.get("energy_kcal_per_serving"))
            + " | protein_g="
            + clean_text(nutrition.get("protein_g_per_serving"))
        )
    lines.extend(["", "Skipped candidates:"])
    if skipped:
        for row in skipped:
            lines.append(
                "- "
                + clean_text(row.get("display_name"))
                + " | "
                + clean_text(row.get("reason"))
            )
    else:
        lines.append("- none")
    lines.extend(["", "Skipped reason counts:"])
    if skipped_counter:
        for reason, count in skipped_counter.most_common():
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "Notes:",
            "- no Food_DB rows were added or promoted.",
            "- nutrition values are direct sums from Food_DB v1.1 round9 per-100g macros.",
            "- manual snacks are draft/test only for Generator v1.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_readme(created_count: int) -> str:
    return "\n".join(
        [
            "Recipes_DB v1.1 manual snacks round12",
            "=" * 39,
            "",
            "This folder is draft/test data only.",
            "It contains manually curated simple snack recipes for Generator v1 testing.",
            "Food_DB current and Recipes_DB current are unchanged.",
            "Full Recipes_DB v1.1 is not materialized here.",
            "",
            f"Created snack recipes: {created_count}",
            "Nutrition was calculated only from existing Food_DB v1.1 round9 rows.",
            "",
        ]
    )


def main() -> None:
    food_rows, _ = read_csv(FOODDB_PATH)
    _, recipe_reference_columns = read_csv(REFERENCE_RECIPES_PATH)
    _, ingredient_reference_columns = read_csv(REFERENCE_INGREDIENTS_PATH)
    _, nutrition_reference_columns = read_csv(REFERENCE_NUTRITION_PATH)

    recipe_columns = with_required_columns(
        recipe_reference_columns,
        [
            "allowed_slots_json",
            "slot_policy_reason",
            "content_quality_status",
            "content_exclusion_reason",
            "active_time_estimated_min",
            "passive_time_estimated_min",
            "effective_time_min_for_scoring",
            "has_long_passive_time",
            "time_estimation_confidence",
            "time_estimation_method",
            "time_estimation_reasons",
        ],
    )
    ingredient_columns = with_required_columns(
        ingredient_reference_columns,
        [
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
            "mapped_food_id",
            "mapped_food_canonical_name",
            "mapping_status",
            "mapping_confidence",
            "mapping_method",
            "mapping_notes",
        ],
    )
    nutrition_columns = with_required_columns(
        nutrition_reference_columns,
        [
            "recipe_id",
            "nutrition_basis",
            "servings_basis",
            "total_weight_grams_estimated",
            "energy_kcal_total",
            "protein_g_total",
            "carbs_g_total",
            "fat_g_total",
            "energy_kcal_per_serving",
            "protein_g_per_serving",
            "carbs_g_per_serving",
            "fat_g_per_serving",
            "mapped_ingredient_count",
            "unmapped_ingredient_count",
            "mapped_weight_ratio",
            "macro_relevant_mapped_weight_ratio",
            "cache_status",
            "cache_version",
        ],
    )

    recipes, ingredients, nutrition_rows, match_audit, skipped = build_recipe_rows(
        recipe_columns,
        ingredient_columns,
        nutrition_columns,
        fooddb_by_canonical(food_rows),
    )

    write_csv(OUT_RECIPES, recipes, recipe_columns)
    write_csv(OUT_INGREDIENTS, ingredients, ingredient_columns)
    write_csv(OUT_NUTRITION, nutrition_rows, nutrition_columns)
    write_csv(
        OUT_MATCH_AUDIT,
        match_audit,
        [
            "recipe_slug",
            "display_name",
            "ingredient_label",
            "ingredient_food_key",
            "grams",
            "ingredient_role",
            "match_status",
            "matched_food_id",
            "matched_canonical_name",
            "energy_kcal_100g",
            "protein_g_100g",
            "carbs_g_100g",
            "fat_g_100g",
            "decision_notes",
        ],
    )
    OUT_README.write_text(build_readme(len(recipes)), encoding="utf-8")
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(build_summary(recipes, nutrition_rows, skipped, match_audit), encoding="utf-8")

    print("Recipes_DB v1.1 manual snacks round12 written")
    print(f"manual_snacks_created={len(recipes)}")
    print(f"manual_candidates_skipped={len(skipped)}")
    print(f"written_recipes={OUT_RECIPES}")
    print(f"written_ingredients={OUT_INGREDIENTS}")
    print(f"written_nutrition={OUT_NUTRITION}")
    print(f"written_summary={OUT_SUMMARY}")


if __name__ == "__main__":
    main()
