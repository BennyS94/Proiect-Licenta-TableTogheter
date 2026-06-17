from __future__ import annotations

import csv
import json
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
AUDIT_DIR = ROOT / "data" / "recipesdb" / "audit"
FOODDB_PATH = ROOT / "data" / "fooddb" / "current" / "fooddb_v1_core_master_draft.csv"
RUNTIME_DB_PATH = ROOT / "data" / "runtime" / "tabletogether_demo.db"

CURRENT_RECIPES = ROOT / "data" / "recipesdb" / "current" / "recipes.csv"
CURRENT_INGREDIENTS = ROOT / "data" / "recipesdb" / "current" / "recipe_ingredients.csv"
CURRENT_CACHE = ROOT / "data" / "recipesdb" / "current" / "recipe_nutrition_cache.csv"

APP_RECIPES = ROOT / "data" / "recipesdb" / "draft" / "v1_2_demo_final" / "recipes.csv"
APP_INGREDIENTS = ROOT / "data" / "recipesdb" / "draft" / "v1_2_demo_final" / "recipe_ingredients.csv"
APP_CACHE = ROOT / "data" / "recipesdb" / "draft" / "v1_2_demo_final" / "recipe_nutrition_cache.csv"


NUTRIENTS: list[dict[str, Any]] = [
    {
        "key": "fiber",
        "name": "Fiber",
        "unit": "g",
        "columns": ["fibre_g_100g", "fiber_g_100g"],
        "kind": "nutrition quality marker",
    },
    {
        "key": "sodium_from_salt",
        "name": "Sodium equivalent from salt",
        "unit": "mg",
        "columns": ["salt_g_100g"],
        "derived_from": "salt_g_100g",
        "factor": 393.4,
        "kind": "derived nutrition quality marker",
    },
    {
        "key": "potassium",
        "name": "Potassium",
        "unit": "mg",
        "columns": ["potassium_mg_100g", "potassium_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "calcium",
        "name": "Calcium",
        "unit": "mg",
        "columns": ["calcium_mg_100g", "calcium_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "iron",
        "name": "Iron",
        "unit": "mg",
        "columns": ["iron_mg_100g", "iron_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "magnesium",
        "name": "Magnesium",
        "unit": "mg",
        "columns": ["magnesium_mg_100g", "magnesium_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "zinc",
        "name": "Zinc",
        "unit": "mg",
        "columns": ["zinc_mg_100g", "zinc_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "vitamin_c",
        "name": "Vitamin C",
        "unit": "mg",
        "columns": ["vitamin_c_mg_100g", "vitamin_c_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "vitamin_d",
        "name": "Vitamin D",
        "unit": "ug",
        "columns": ["vitamin_d_ug_100g", "vitamin_d_mcg_100g", "vitamin_d_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "vitamin_b12",
        "name": "Vitamin B12",
        "unit": "ug",
        "columns": ["vitamin_b12_ug_100g", "vitamin_b12_mcg_100g", "vitamin_b12_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "folate_b9",
        "name": "Folate / Vitamin B9",
        "unit": "ug",
        "columns": ["folate_ug_100g", "vitamin_b9_ug_100g", "folate_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "vitamin_a",
        "name": "Vitamin A",
        "unit": "ug",
        "columns": ["vitamin_a_ug_100g", "retinol_ug_100g", "vitamin_a_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "vitamin_e",
        "name": "Vitamin E",
        "unit": "mg",
        "columns": ["vitamin_e_mg_100g", "vitamin_e_100g"],
        "kind": "micronutrient",
    },
    {
        "key": "saturated_fat",
        "name": "Saturated fat",
        "unit": "g",
        "columns": ["saturated_fat_g_100g", "saturates_g_100g", "fa_sat_g_100g"],
        "kind": "nutrition quality marker",
    },
    {
        "key": "sugar",
        "name": "Sugar",
        "unit": "g",
        "columns": ["sugars_g_100g", "sugar_g_100g"],
        "kind": "nutrition quality marker",
    },
]


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def read_csv_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def pct(part: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return round((part / total) * 100.0, 2)


def fmt_num(value: float | None, digits: int = 4) -> str:
    if value is None:
        return ""
    return str(round(value, digits))


def is_active_recipe(row: dict[str, str]) -> bool:
    value = str(row.get("is_active", "")).strip().lower()
    if value in {"0", "false", "no", "n", "inactive"}:
        return False
    return True


def accepted_mapping(row: dict[str, str]) -> bool:
    food_id = str(row.get("mapped_food_id", "")).strip()
    if not food_id:
        return False
    status = str(row.get("mapping_status", "")).strip().lower()
    blocked_tokens = ("reject", "unmapped", "missing", "none", "failed")
    return not any(token in status for token in blocked_tokens)


def choose_column(headers: set[str], nutrient: dict[str, Any]) -> str:
    for column in nutrient["columns"]:
        if column in headers:
            return column
    return ""


def nutrient_value(row: dict[str, str], nutrient: dict[str, Any], headers: set[str]) -> float | None:
    column = choose_column(headers, nutrient)
    if not column:
        return None
    raw_value = to_float(row.get(column))
    if raw_value is None:
        return None
    factor = float(nutrient.get("factor", 1.0))
    return raw_value * factor


def load_fooddb() -> tuple[list[dict[str, str]], set[str], dict[str, dict[str, str]]]:
    rows = read_csv_rows(FOODDB_PATH)
    headers = set(read_csv_header(FOODDB_PATH))
    by_id = {str(row.get("food_id", "")).strip(): row for row in rows if row.get("food_id")}
    return rows, headers, by_id


def fooddb_coverage_rows(food_rows: list[dict[str, str]], headers: set[str]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    total = len(food_rows)
    ciqual_rows = sum(1 for row in food_rows if str(row.get("primary_source_ciqual_code", "")).strip())

    for nutrient in NUTRIENTS:
        column = choose_column(headers, nutrient)
        values: list[float] = []
        invalid = 0
        non_empty = 0
        for row in food_rows:
            if not column:
                continue
            raw = str(row.get(column, "")).strip()
            if raw:
                non_empty += 1
            value = to_float(raw)
            if raw and value is None:
                invalid += 1
            if value is not None:
                values.append(value * float(nutrient.get("factor", 1.0)))

        numeric_count = len(values)
        missing_count = total - numeric_count - invalid if column else total
        output.append(
            {
                "nutrient_key": nutrient["key"],
                "nutrient_name": nutrient["name"],
                "kind": nutrient["kind"],
                "unit": nutrient["unit"],
                "fooddb_column": column,
                "direct_column_present": "yes" if column and "derived_from" not in nutrient else "no",
                "derived_from": nutrient.get("derived_from", ""),
                "rows_total": total,
                "non_empty_count": non_empty,
                "numeric_count": numeric_count,
                "numeric_coverage_pct": pct(numeric_count, total),
                "missing_count": max(missing_count, 0),
                "missing_pct": pct(max(missing_count, 0), total),
                "invalid_count": invalid,
                "invalid_pct": pct(invalid, total),
                "min_value": fmt_num(min(values) if values else None),
                "max_value": fmt_num(max(values) if values else None),
                "median_value": fmt_num(statistics.median(values) if values else None),
                "appears_per_100g": "yes" if column.endswith("_100g") else "unknown",
                "source_signal": f"ciqual_code_rows={ciqual_rows}",
                "recommendation": "candidate" if numeric_count else "exclude",
                "notes": (
                    "Derived from salt; not a direct sodium column."
                    if nutrient["key"] == "sodium_from_salt" and column
                    else ("No matching current Food_DB column." if not column else "")
                ),
            }
        )
    return output


def recipe_title(row: dict[str, str]) -> str:
    for key in ("display_name", "recipe_name", "title", "name"):
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return ""


def recipe_servings(row: dict[str, str], cache_row: dict[str, str] | None) -> float | None:
    candidates: list[Any] = []
    if cache_row:
        candidates.extend(
            [
                cache_row.get("servings_basis"),
                cache_row.get("servings"),
                cache_row.get("servings_normalized"),
                cache_row.get("servings_declared"),
            ]
        )
    candidates.extend([row.get("servings_normalized"), row.get("servings_declared"), row.get("servings")])
    for candidate in candidates:
        value = to_float(candidate)
        if value and value > 0:
            return value
    return None


def confidence_from_ratio(ratio: float, has_column: bool) -> str:
    if not has_column or ratio <= 0:
        return "none"
    if ratio >= 0.8:
        return "high"
    if ratio >= 0.5:
        return "medium"
    return "low"


def audit_recipe_dataset(
    dataset_profile: str,
    recipes_path: Path,
    ingredients_path: Path,
    cache_path: Path,
    food_headers: set[str],
    food_by_id: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, float]], set[str]]:
    recipes = [row for row in read_csv_rows(recipes_path) if is_active_recipe(row)]
    ingredients = read_csv_rows(ingredients_path)
    cache_rows = read_csv_rows(cache_path)
    cache_by_recipe = {str(row.get("recipe_id", "")).strip(): row for row in cache_rows}
    ingredients_by_recipe: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in ingredients:
        recipe_id = str(row.get("recipe_id", "")).strip()
        if recipe_id:
            ingredients_by_recipe[recipe_id].append(row)

    output: list[dict[str, Any]] = []
    summary: dict[str, dict[str, float]] = {}
    active_recipe_ids = {str(row.get("recipe_id", "")).strip() for row in recipes if row.get("recipe_id")}

    for nutrient in NUTRIENTS:
        column = choose_column(food_headers, nutrient)
        estimable_count = 0
        confidence_counts = defaultdict(int)
        ratios: list[float] = []

        for recipe in recipes:
            recipe_id = str(recipe.get("recipe_id", "")).strip()
            ingredient_rows = ingredients_by_recipe.get(recipe_id, [])
            total_grams = 0.0
            mapped_grams = 0.0
            known_nutrient_grams = 0.0
            mapped_count = 0
            total_value = 0.0
            ingredient_rows_with_grams = 0

            for ingredient in ingredient_rows:
                grams = to_float(ingredient.get("quantity_grams_estimated"))
                if grams is None or grams <= 0:
                    continue
                ingredient_rows_with_grams += 1
                total_grams += grams
                mapped_food_id = str(ingredient.get("mapped_food_id", "")).strip()
                if not accepted_mapping(ingredient) or mapped_food_id not in food_by_id:
                    continue
                mapped_count += 1
                mapped_grams += grams
                value = nutrient_value(food_by_id[mapped_food_id], nutrient, food_headers)
                if value is None:
                    continue
                known_nutrient_grams += grams
                total_value += grams * value / 100.0

            mapped_weight_ratio = mapped_grams / total_grams if total_grams else 0.0
            known_ratio = known_nutrient_grams / total_grams if total_grams else 0.0
            servings = recipe_servings(recipe, cache_by_recipe.get(recipe_id))
            per_serving = total_value / servings if servings else None
            confidence = confidence_from_ratio(known_ratio, bool(column))
            can_estimate = bool(column) and known_ratio >= 0.5

            if can_estimate:
                estimable_count += 1
            confidence_counts[confidence] += 1
            ratios.append(known_ratio)

            output.append(
                {
                    "dataset_profile": dataset_profile,
                    "recipe_id": recipe_id,
                    "display_name": recipe_title(recipe),
                    "nutrient_key": nutrient["key"],
                    "nutrient_name": nutrient["name"],
                    "unit": nutrient["unit"],
                    "fooddb_column": column,
                    "ingredient_row_count": len(ingredient_rows),
                    "ingredient_rows_with_grams": ingredient_rows_with_grams,
                    "mapped_ingredient_count": mapped_count,
                    "mapped_weight_ratio_pct": pct(mapped_grams, total_grams),
                    "known_nutrient_weight_ratio_pct": pct(known_nutrient_grams, total_grams),
                    "nutrient_total_estimated": fmt_num(total_value),
                    "nutrient_per_serving_estimated": fmt_num(per_serving),
                    "servings_basis": fmt_num(servings, 2),
                    "can_estimate": "yes" if can_estimate else "no",
                    "confidence": confidence,
                    "notes": "No matching current Food_DB column." if not column else "",
                }
            )

        total_recipes = len(recipes)
        summary[nutrient["key"]] = {
            "active_recipes": float(total_recipes),
            "estimable_recipes": float(estimable_count),
            "estimable_pct": pct(estimable_count, total_recipes),
            "avg_known_ratio_pct": round(statistics.mean(ratios) * 100.0, 2) if ratios else 0.0,
            "high": float(confidence_counts["high"]),
            "medium": float(confidence_counts["medium"]),
            "low": float(confidence_counts["low"]),
            "none": float(confidence_counts["none"]),
        }

    return output, summary, active_recipe_ids


def parse_json(value: str) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def extract_meals(meal_completion: dict[str, Any], day_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    meals = meal_completion.get("meals")
    if isinstance(meals, list):
        return [meal for meal in meals if isinstance(meal, dict)]
    snapshot_meals = day_snapshot.get("meals")
    if isinstance(snapshot_meals, list):
        return [meal for meal in snapshot_meals if isinstance(meal, dict)]
    return []


def meal_is_eaten(meal: dict[str, Any], completed_keys: set[str]) -> bool:
    if isinstance(meal.get("eaten"), bool):
        return bool(meal.get("eaten"))
    meal_key = str(meal.get("meal_key", "")).strip()
    return bool(meal_key and meal_key in completed_keys)


def load_saved_progress_rows() -> list[dict[str, Any]]:
    if not RUNTIME_DB_PATH.exists():
        return []
    with sqlite3.connect(RUNTIME_DB_PATH) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT progress_id, member_profile_id, plan_id, day_index, saved_at,
                   meal_completion_json, day_snapshot_json
            FROM saved_daily_progress
            ORDER BY member_profile_id, saved_at DESC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def audit_saved_progress(current_ids: set[str], app_ids: set[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = load_saved_progress_rows()
    latest_by_profile: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        profile_id = str(row.get("member_profile_id", "")).strip()
        if len(latest_by_profile[profile_id]) < 7:
            latest_by_profile[profile_id].add(str(row.get("progress_id", "")))

    output: list[dict[str, Any]] = []
    all_eaten_recipe_ids: list[str] = []
    current_matches = 0
    app_matches = 0
    snapshots_with_meals = 0
    snapshots_with_portions = 0

    for row in rows:
        meal_completion = parse_json(row.get("meal_completion_json") or "")
        day_snapshot = parse_json(row.get("day_snapshot_json") or "")
        meals = extract_meals(meal_completion, day_snapshot)
        completed_keys = {
            str(key).strip()
            for key in meal_completion.get("completed_meal_keys", [])
            if str(key).strip()
        }
        eaten_meals = [meal for meal in meals if meal_is_eaten(meal, completed_keys)]
        eaten_recipe_ids = [str(meal.get("recipe_id", "")).strip() for meal in eaten_meals if meal.get("recipe_id")]
        portion_keys_present = any(
            any(key in meal for key in ("portion_multiplier", "serving_multiplier", "servings", "portion_grams"))
            for meal in eaten_meals
        )
        matched_current = sum(1 for recipe_id in eaten_recipe_ids if recipe_id in current_ids)
        matched_app = sum(1 for recipe_id in eaten_recipe_ids if recipe_id in app_ids)

        if meals:
            snapshots_with_meals += 1
        if portion_keys_present:
            snapshots_with_portions += 1
        current_matches += matched_current
        app_matches += matched_app
        all_eaten_recipe_ids.extend(eaten_recipe_ids)

        progress_id = str(row.get("progress_id", "")).strip()
        output.append(
            {
                "progress_id": progress_id,
                "member_profile_id": row.get("member_profile_id", ""),
                "plan_id": row.get("plan_id", ""),
                "day_index": row.get("day_index", ""),
                "saved_at": row.get("saved_at", ""),
                "meal_count": len(meals),
                "eaten_meal_count": len(eaten_meals),
                "eaten_recipe_count": len(eaten_recipe_ids),
                "recipe_ids_present": ";".join(eaten_recipe_ids),
                "matched_current_recipe_count": matched_current,
                "matched_app_facing_recipe_count": matched_app,
                "portion_multiplier_present": "yes" if portion_keys_present else "no",
                "can_identify_eaten_meals": "yes" if eaten_meals else "no",
                "can_map_current_recipes": "yes" if eaten_recipe_ids and matched_current == len(eaten_recipe_ids) else "no",
                "can_map_app_facing_recipes": "yes" if eaten_recipe_ids and matched_app == len(eaten_recipe_ids) else "no",
                "latest_7_for_profile": "yes" if progress_id in latest_by_profile[str(row.get("member_profile_id", "")).strip()] else "no",
                "source_hint": day_snapshot.get("source", "") or meal_completion.get("source", ""),
                "notes": "" if eaten_recipe_ids else "No eaten recipe IDs found in this snapshot.",
            }
        )

    total_mentions = len(all_eaten_recipe_ids)
    unique_profiles = {str(row.get("member_profile_id", "")).strip() for row in rows}
    summary = {
        "saved_rows": len(rows),
        "profiles": len(unique_profiles),
        "snapshots_with_meals": snapshots_with_meals,
        "snapshots_with_portions": snapshots_with_portions,
        "eaten_recipe_mentions": total_mentions,
        "unique_eaten_recipe_ids": len(set(all_eaten_recipe_ids)),
        "current_match_pct": pct(current_matches, total_mentions),
        "app_match_pct": pct(app_matches, total_mentions),
    }
    return output, summary


def recommendation_rows(
    food_rows: list[dict[str, Any]],
    recipe_summaries: dict[str, dict[str, dict[str, float]]],
    saved_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    food_by_key = {row["nutrient_key"]: row for row in food_rows}
    rows: list[dict[str, Any]] = []
    for nutrient in NUTRIENTS:
        key = nutrient["key"]
        food_cov = float(food_by_key[key]["numeric_coverage_pct"])
        current_cov = recipe_summaries.get("current", {}).get(key, {}).get("estimable_pct", 0.0)
        app_cov = recipe_summaries.get("app_facing_v1_2_demo_final", {}).get(key, {}).get("estimable_pct", 0.0)
        column = str(food_by_key[key].get("fooddb_column", ""))

        if not column:
            recommendation = "exclude"
            confidence = "none"
            reason = "No matching current Food_DB column."
        elif key in {"fiber", "sugar", "sodium_from_salt"}:
            recommendation = "maybe"
            confidence = "medium" if max(current_cov, app_cov) >= 50 else "low"
            reason = "Available as a nutrition quality marker, not a complete micronutrient target."
        elif min(food_cov, max(current_cov, app_cov)) >= 70:
            recommendation = "include"
            confidence = "medium"
            reason = "Coverage appears usable, pending target/reference approval."
        else:
            recommendation = "exclude"
            confidence = "low"
            reason = "Coverage is too weak for an MVP insight."

        rows.append(
            {
                "rank": 0,
                "nutrient_key": key,
                "nutrient_name": nutrient["name"],
                "unit": nutrient["unit"],
                "kind": nutrient["kind"],
                "fooddb_coverage_pct": food_cov,
                "current_recipe_coverage_pct": current_cov,
                "app_facing_recipe_coverage_pct": app_cov,
                "saved_progress_current_mapping_pct": saved_summary.get("current_match_pct", 0.0),
                "saved_progress_app_facing_mapping_pct": saved_summary.get("app_match_pct", 0.0),
                "confidence": confidence,
                "recommendation": recommendation,
                "reason": reason,
            }
        )

    priority = {"include": 0, "maybe": 1, "exclude": 2}
    rows.sort(
        key=lambda row: (
            priority.get(str(row["recommendation"]), 9),
            -float(row["app_facing_recipe_coverage_pct"]),
            -float(row["fooddb_coverage_pct"]),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def write_summary(
    path: Path,
    food_rows: list[dict[str, Any]],
    recipe_summaries: dict[str, dict[str, dict[str, float]]],
    saved_summary: dict[str, Any],
    top_rows: list[dict[str, Any]],
) -> None:
    found = [row for row in food_rows if row["fooddb_column"]]
    absent = [row for row in food_rows if not row["fooddb_column"]]
    maybe_rows = [row for row in top_rows if row["recommendation"] == "maybe"]
    include_rows = [row for row in top_rows if row["recommendation"] == "include"]
    excluded_rows = [row for row in top_rows if row["recommendation"] == "exclude"]

    lines = [
        "MICRO-AUDIT-1 - Weekly micronutrient insights feasibility",
        "",
        "Verdict",
        "- Full weekly micronutrient insights are not feasible from the current Food_DB because the current Food_DB does not contain direct vitamin/mineral columns for calcium, iron, potassium, magnesium, zinc, vitamins C/D/B12/B9/A/E.",
        "- A limited weekly nutrition-quality snapshot is feasible for Fiber, Sugar, and Salt-derived sodium equivalent, but this should not be presented as a complete micronutrient feature.",
        "- Saved progress stores enough meal-level structure to identify eaten meals and recipe IDs, but it does not consistently store explicit portion multipliers. Robust MICRO-1 should persist the consumed portion multiplier or serving quantity.",
        "- Runtime saved recipe IDs map poorly to data/recipesdb/current, so MICRO-1 should resolve recipe IDs against the app-facing recipe package used by generation or persist dataset_profile/source metadata.",
        "",
        "Food_DB candidate columns found",
    ]
    if found:
        for row in found:
            lines.append(
                f"- {row['nutrient_name']}: {row['fooddb_column']} ({row['numeric_coverage_pct']}% numeric coverage)"
            )
    else:
        lines.append("- None")

    lines.extend(["", "Candidate columns absent"])
    for row in absent:
        lines.append(f"- {row['nutrient_name']}")

    lines.extend(["", "Recipe coverage summary"])
    for dataset_name, summary in recipe_summaries.items():
        active_count = next(iter(summary.values()))["active_recipes"] if summary else 0
        lines.append(f"- {dataset_name}: {int(active_count)} active/app-facing recipes audited")
        for key in ("fiber", "sugar", "sodium_from_salt"):
            nutrient_summary = summary.get(key, {})
            if nutrient_summary:
                lines.append(
                    f"  - {key}: {nutrient_summary['estimable_pct']}% estimable, avg known weight ratio {nutrient_summary['avg_known_ratio_pct']}%"
                )

    lines.extend(
        [
            "",
            "Saved progress feasibility",
            f"- saved rows: {saved_summary.get('saved_rows', 0)}",
            f"- profiles with saved rows: {saved_summary.get('profiles', 0)}",
            f"- eaten recipe mentions: {saved_summary.get('eaten_recipe_mentions', 0)}",
            f"- unique eaten recipe IDs: {saved_summary.get('unique_eaten_recipe_ids', 0)}",
            f"- mapping to current Recipes_DB: {saved_summary.get('current_match_pct', 0.0)}%",
            f"- mapping to app-facing v1_2_demo_final: {saved_summary.get('app_match_pct', 0.0)}%",
            f"- snapshots with explicit portion multiplier/serving fields: {saved_summary.get('snapshots_with_portions', 0)}",
            "",
            "Top realistic MVP display candidates",
        ]
    )
    ranked = include_rows + maybe_rows
    if ranked:
        for row in ranked[:10]:
            lines.append(
                f"- {row['nutrient_name']} ({row['unit']}): {row['recommendation']}, confidence={row['confidence']}. {row['reason']}"
            )
    else:
        lines.append("- None for a true micronutrient feature.")

    lines.extend(["", "Future work / excluded for now"])
    for row in excluded_rows:
        lines.append(f"- {row['nutrient_name']}: {row['reason']}")

    lines.extend(
        [
            "",
            "Reference target recommendation",
            "- Safest MVP direction is Option C: display weekly estimated totals only, no percent-of-target, until a documented reference target table is approved.",
            "- Option A or B should be a separate data-model task with cited reference sources and age/sex handling. Do not hard-code clinical targets inside the UI.",
            "",
            "Safe UI wording",
            "- Weekly nutrition snapshot",
            "- Estimated from available recipe data",
            "- Limited nutrient coverage",
            "- Reference targets are approximate",
            "- Informational only, not medical advice",
            "",
            "Recommended MICRO-1 implementation outline",
            "1. Keep the calculation backend-side/offline; mobile should request a summarized payload.",
            "2. Persist or expose dataset_profile/source for generated plans and saved progress.",
            "3. Persist consumed portion multiplier or consumed serving quantity per saved meal.",
            "4. Add a proper nutrient source table or extend Food_DB with approved CIQUAL micronutrient columns.",
            "5. Start with totals-only cards for Fiber/Sugar/Salt-derived sodium equivalent, then add target comparison only after target references are approved.",
        ]
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    food_rows, food_headers, food_by_id = load_fooddb()

    food_output = fooddb_coverage_rows(food_rows, food_headers)
    write_csv(
        AUDIT_DIR / "micronutrient_fooddb_coverage.csv",
        food_output,
        [
            "nutrient_key",
            "nutrient_name",
            "kind",
            "unit",
            "fooddb_column",
            "direct_column_present",
            "derived_from",
            "rows_total",
            "non_empty_count",
            "numeric_count",
            "numeric_coverage_pct",
            "missing_count",
            "missing_pct",
            "invalid_count",
            "invalid_pct",
            "min_value",
            "max_value",
            "median_value",
            "appears_per_100g",
            "source_signal",
            "recommendation",
            "notes",
        ],
    )

    recipe_outputs: list[dict[str, Any]] = []
    recipe_summaries: dict[str, dict[str, dict[str, float]]] = {}
    current_output, current_summary, current_ids = audit_recipe_dataset(
        "current",
        CURRENT_RECIPES,
        CURRENT_INGREDIENTS,
        CURRENT_CACHE,
        food_headers,
        food_by_id,
    )
    recipe_outputs.extend(current_output)
    recipe_summaries["current"] = current_summary

    app_ids: set[str] = set()
    if APP_RECIPES.exists() and APP_INGREDIENTS.exists() and APP_CACHE.exists():
        app_output, app_summary, app_ids = audit_recipe_dataset(
            "app_facing_v1_2_demo_final",
            APP_RECIPES,
            APP_INGREDIENTS,
            APP_CACHE,
            food_headers,
            food_by_id,
        )
        recipe_outputs.extend(app_output)
        recipe_summaries["app_facing_v1_2_demo_final"] = app_summary

    write_csv(
        AUDIT_DIR / "micronutrient_recipe_coverage.csv",
        recipe_outputs,
        [
            "dataset_profile",
            "recipe_id",
            "display_name",
            "nutrient_key",
            "nutrient_name",
            "unit",
            "fooddb_column",
            "ingredient_row_count",
            "ingredient_rows_with_grams",
            "mapped_ingredient_count",
            "mapped_weight_ratio_pct",
            "known_nutrient_weight_ratio_pct",
            "nutrient_total_estimated",
            "nutrient_per_serving_estimated",
            "servings_basis",
            "can_estimate",
            "confidence",
            "notes",
        ],
    )

    saved_output, saved_summary = audit_saved_progress(current_ids, app_ids)
    write_csv(
        AUDIT_DIR / "micronutrient_saved_progress_feasibility.csv",
        saved_output,
        [
            "progress_id",
            "member_profile_id",
            "plan_id",
            "day_index",
            "saved_at",
            "meal_count",
            "eaten_meal_count",
            "eaten_recipe_count",
            "recipe_ids_present",
            "matched_current_recipe_count",
            "matched_app_facing_recipe_count",
            "portion_multiplier_present",
            "can_identify_eaten_meals",
            "can_map_current_recipes",
            "can_map_app_facing_recipes",
            "latest_7_for_profile",
            "source_hint",
            "notes",
        ],
    )

    top_rows = recommendation_rows(food_output, recipe_summaries, saved_summary)
    write_csv(
        AUDIT_DIR / "micronutrient_top10_recommendation.csv",
        top_rows,
        [
            "rank",
            "nutrient_key",
            "nutrient_name",
            "unit",
            "kind",
            "fooddb_coverage_pct",
            "current_recipe_coverage_pct",
            "app_facing_recipe_coverage_pct",
            "saved_progress_current_mapping_pct",
            "saved_progress_app_facing_mapping_pct",
            "confidence",
            "recommendation",
            "reason",
        ],
    )

    write_summary(
        AUDIT_DIR / "micronutrient_weekly_feasibility_summary.txt",
        food_output,
        recipe_summaries,
        saved_summary,
        top_rows,
    )

    print("MICRO-AUDIT-1 outputs written to data/recipesdb/audit")
    print(f"Food_DB rows audited: {len(food_rows)}")
    print(f"Saved progress rows audited: {saved_summary.get('saved_rows', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
