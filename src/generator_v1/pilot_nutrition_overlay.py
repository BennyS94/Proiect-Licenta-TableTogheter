from __future__ import annotations

from typing import Any

import pandas as pd

from src.generator_v1.pilot_ingredient_aliases import PILOT_SAFE_INGREDIENT_ALIASES
from src.generator_v1.pilot_servings_estimator import estimate_pilot_servings


MACRO_COLUMNS = {
    "energy_kcal": ("energy_kcal_100g", "energy_kcal_100"),
    "protein_g": ("protein_g_100g", "protein_g_100"),
    "carbs_g": ("carbs_g_100g", "carbs_g_100"),
    "fat_g": ("fat_g_100g", "fat_g_100"),
}


def compute_pilot_overlay_nutrition(
    recipe_row: pd.Series | dict[str, Any],
    ingredients_for_recipe: pd.DataFrame,
    fooddb_df: pd.DataFrame,
    nutrition_cache_row: pd.Series | dict[str, Any] | None = None,
) -> dict[str, Any]:
    empty_result = _empty_overlay_result()
    if ingredients_for_recipe.empty or fooddb_df.empty:
        empty_result["pilot_nutrition_overlay_reasons"].append(
            "pilot_overlay_missing_ingredients_or_fooddb"
        )
        return empty_result

    servings = estimate_pilot_servings(
        recipe_row=recipe_row,
        ingredients_df=ingredients_for_recipe,
        nutrition_row=nutrition_cache_row,
    )
    servings_basis = _positive_float(servings.get("estimated_servings_basis"))
    if servings_basis is None:
        empty_result["pilot_nutrition_overlay_reasons"].append(
            "pilot_overlay_missing_servings_basis"
        )
        return empty_result

    food_lookup = fooddb_df.set_index("food_id", drop=False)
    totals = {"energy_kcal": 0.0, "protein_g": 0.0, "carbs_g": 0.0, "fat_g": 0.0}
    existing_weight = 0.0
    alias_weight = 0.0
    existing_count = 0
    alias_count = 0
    aliases_used: list[str] = []
    missing_food_ids: list[str] = []

    for _, ingredient in ingredients_for_recipe.iterrows():
        grams = _positive_float(ingredient.get("quantity_grams_estimated"))
        if grams is None:
            continue

        food_id, source_kind = _ingredient_food_id(ingredient)
        if food_id is None:
            continue
        if food_id not in food_lookup.index:
            missing_food_ids.append(food_id)
            continue

        food_row = food_lookup.loc[food_id]
        _add_macros(totals, food_row, grams)
        if source_kind == "alias":
            alias_count += 1
            alias_weight += grams
            aliases_used.append(
                f"{_ingredient_name(ingredient)}->{food_id}"
            )
        else:
            existing_count += 1
            existing_weight += grams

    mapped_weight = existing_weight + alias_weight
    if mapped_weight <= 0 or existing_count + alias_count == 0:
        empty_result["overlay_estimated_servings_basis"] = servings_basis
        empty_result["pilot_nutrition_overlay_reasons"].append(
            "pilot_overlay_no_usable_weighted_ingredients"
        )
        return empty_result

    per_serving = {
        f"overlay_{key}_per_serving": round(value / servings_basis, 4)
        for key, value in totals.items()
    }
    result = {
        "overlay_energy_kcal_total": round(totals["energy_kcal"], 4),
        "overlay_protein_g_total": round(totals["protein_g"], 4),
        "overlay_carbs_g_total": round(totals["carbs_g"], 4),
        "overlay_fat_g_total": round(totals["fat_g"], 4),
        **per_serving,
        "overlay_mapped_weight_grams": round(mapped_weight, 4),
        "overlay_alias_weight_grams": round(alias_weight, 4),
        "overlay_used_alias_count": alias_count,
        "overlay_used_existing_mapping_count": existing_count,
        "overlay_estimated_servings_basis": servings_basis,
        "uses_pilot_nutrition_overlay": _is_overlay_useful(
            per_serving,
            nutrition_cache_row if nutrition_cache_row is not None else recipe_row,
        ),
        "pilot_nutrition_overlay_reasons": [
            "pilot_nutrition_overlay_read_only",
            *servings.get("servings_estimation_reasons", []),
        ],
        "overlay_aliases_used": sorted(set(aliases_used)),
        "overlay_missing_food_ids": sorted(set(missing_food_ids)),
    }
    if alias_count:
        result["pilot_nutrition_overlay_reasons"].append(
            f"pilot_safe_aliases_used={alias_count}"
        )
    if missing_food_ids:
        result["pilot_nutrition_overlay_reasons"].append(
            "pilot_overlay_missing_fooddb_rows"
        )
    if not result["uses_pilot_nutrition_overlay"]:
        result["pilot_nutrition_overlay_reasons"].append(
            "pilot_overlay_not_preferred_over_cache"
        )
    return result


def _empty_overlay_result() -> dict[str, Any]:
    return {
        "overlay_energy_kcal_total": None,
        "overlay_protein_g_total": None,
        "overlay_carbs_g_total": None,
        "overlay_fat_g_total": None,
        "overlay_energy_kcal_per_serving": None,
        "overlay_protein_g_per_serving": None,
        "overlay_carbs_g_per_serving": None,
        "overlay_fat_g_per_serving": None,
        "overlay_mapped_weight_grams": 0.0,
        "overlay_alias_weight_grams": 0.0,
        "overlay_used_alias_count": 0,
        "overlay_used_existing_mapping_count": 0,
        "overlay_estimated_servings_basis": None,
        "uses_pilot_nutrition_overlay": False,
        "pilot_nutrition_overlay_reasons": [],
        "overlay_aliases_used": [],
        "overlay_missing_food_ids": [],
    }


def _ingredient_food_id(ingredient: pd.Series) -> tuple[str | None, str | None]:
    mapping_status = str(ingredient.get("mapping_status") or "").strip().lower()
    mapped_food_id = _clean_text(ingredient.get("mapped_food_id"))
    if mapping_status == "accepted_auto" and mapped_food_id:
        return mapped_food_id, "existing"

    ingredient_name = _ingredient_name(ingredient)
    alias_food_id = PILOT_SAFE_INGREDIENT_ALIASES.get(ingredient_name)
    if alias_food_id:
        return alias_food_id, "alias"
    return None, None


def _clean_text(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value or "").strip()


def _ingredient_name(ingredient: pd.Series) -> str:
    for column in ("ingredient_name_normalized", "ingredient_name_parsed"):
        value = ingredient.get(column)
        if value is not None and str(value).strip():
            return str(value).strip().lower()
    return str(ingredient.get("ingredient_raw_text") or "").strip().lower()


def _add_macros(totals: dict[str, float], food_row: pd.Series, grams: float) -> None:
    for macro_name, possible_columns in MACRO_COLUMNS.items():
        per_100g = _first_numeric(food_row, possible_columns)
        if per_100g is None:
            continue
        totals[macro_name] += per_100g * grams / 100.0


def _is_overlay_useful(
    per_serving: dict[str, float],
    cache_row: pd.Series | dict[str, Any],
) -> bool:
    overlay_kcal = _positive_float(per_serving.get("overlay_energy_kcal_per_serving"))
    overlay_protein = _positive_float(per_serving.get("overlay_protein_g_per_serving"))
    cache_kcal = _positive_float(_get(cache_row, "energy_kcal_per_serving")) or 0.0
    cache_protein = _positive_float(_get(cache_row, "protein_g_per_serving")) or 0.0
    if overlay_kcal is None and overlay_protein is None:
        return False
    return (overlay_kcal or 0.0) > cache_kcal or (overlay_protein or 0.0) > cache_protein


def _first_numeric(row: pd.Series, columns: tuple[str, ...]) -> float | None:
    for column in columns:
        if column not in row.index:
            continue
        value = _to_float(row.get(column))
        if value is not None:
            return value
    return None


def _positive_float(value: object) -> float | None:
    numeric = _to_float(value)
    if numeric is None or numeric <= 0:
        return None
    return numeric


def _to_float(value: object) -> float | None:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def _get(row: pd.Series | dict[str, Any] | None, key: str) -> object:
    if row is None:
        return None
    if isinstance(row, pd.Series):
        return row.get(key)
    return row.get(key)
