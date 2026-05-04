from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd


SELECTED_MEAL_FIELDS = [
    "slot",
    "recipe_id",
    "display_name",
    "portion_multiplier",
    "kcal",
    "protein_g",
    "carbs_g",
    "fat_g",
    "total_time_min",
    "macro_fit",
    "time_fit",
    "slot_fit",
    "nutrition_quality",
    "nutrition_quality_reasons",
    "is_nutrition_suspicious",
    "base_score_preview",
    "feedback_fit",
    "variety_fit",
    "score_preview",
    "slot_fit_reasons",
]

SORT_COLUMNS = [
    "score_preview",
    "macro_fit",
    "time_fit",
    "recipe_id",
    "portion_multiplier",
]
SORT_ASCENDING = [False, False, False, True, True]


def select_one_day_plan(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    slot_order: Sequence[str],
) -> dict[str, object]:
    selected_meals: list[dict[str, object]] = []
    used_recipe_ids: set[str] = set()
    warnings: list[str] = []

    for slot in slot_order:
        candidates = slot_candidates_by_slot.get(slot)
        if candidates is None or candidates.empty:
            warnings.append(f"Nu exista candidati pentru slot: {slot}")
            continue

        sorted_candidates = candidates.sort_values(
            SORT_COLUMNS,
            ascending=SORT_ASCENDING,
            kind="mergesort",
            na_position="last",
        )
        selected_row = _first_unused_recipe(sorted_candidates, used_recipe_ids)
        if selected_row is None:
            warnings.append(f"Nu exista candidat nerepetat pentru slot: {slot}")
            continue

        selected_meals.append(selected_row)
        used_recipe_ids.add(str(selected_row["recipe_id"]))

    return {
        "selected_meals": selected_meals,
        "day_totals": _day_totals(selected_meals),
        "warnings": warnings,
    }


def _first_unused_recipe(
    sorted_candidates: pd.DataFrame,
    used_recipe_ids: set[str],
) -> dict[str, object] | None:
    for _, row in sorted_candidates.iterrows():
        recipe_id = str(row.get("recipe_id", "")).strip()
        if not recipe_id or recipe_id in used_recipe_ids:
            continue
        return _selected_meal_row(row)
    return None


def _selected_meal_row(row: pd.Series) -> dict[str, object]:
    return {
        field: _clean_value(row.get(field))
        for field in SELECTED_MEAL_FIELDS
    }


def _day_totals(selected_meals: list[dict[str, object]]) -> dict[str, object]:
    return {
        "total_kcal": round(sum(_to_float(meal.get("kcal")) for meal in selected_meals), 1),
        "total_protein_g": round(sum(_to_float(meal.get("protein_g")) for meal in selected_meals), 1),
        "total_carbs_g": round(sum(_to_float(meal.get("carbs_g")) for meal in selected_meals), 1),
        "total_fat_g": round(sum(_to_float(meal.get("fat_g")) for meal in selected_meals), 1),
        "total_time_min_sum": round(
            sum(_to_float(meal.get("total_time_min")) for meal in selected_meals),
            1,
        ),
        "selected_slot_count": len(selected_meals),
    }


def _to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)


def _clean_value(value: object) -> object:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value
