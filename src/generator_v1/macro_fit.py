from __future__ import annotations

import math
from collections.abc import Mapping


def clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, float(value)))


def kcal_fit(actual_kcal: object, target_kcal: object) -> float:
    actual_value, target_value = _component_values(actual_kcal, target_kcal)
    if target_value is None:
        return 0.50
    if actual_value is None:
        return 0.0
    return clamp(1 - abs(actual_value - target_value) / target_value)


def protein_fit(actual_protein_g: object, target_protein_g: object) -> float:
    actual_value, target_value = _component_values(actual_protein_g, target_protein_g)
    if target_value is None:
        return 0.50
    if actual_value is None:
        return 0.0
    if actual_value >= target_value:
        return 1.0
    return clamp(actual_value / target_value)


def carbs_fit(actual_carbs_g: object, target_carbs_g: object) -> float:
    actual_value, target_value = _component_values(actual_carbs_g, target_carbs_g)
    if target_value is None:
        return 0.50
    if actual_value is None:
        return 0.0
    return clamp(1 - abs(actual_value - target_value) / target_value)


def fat_fit(actual_fat_g: object, target_fat_g: object) -> float:
    actual_value, target_value = _component_values(actual_fat_g, target_fat_g)
    if target_value is None:
        return 0.50
    if actual_value is None:
        return 0.0
    if actual_value <= target_value:
        return 1.0
    return clamp(1 - ((actual_value - target_value) / target_value))


def macro_fit(
    actual: Mapping[str, object],
    target: Mapping[str, object],
) -> dict[str, float]:
    component_scores = {
        "protein_fit": protein_fit(actual.get("protein_g"), target.get("protein_g")),
        "kcal_fit": kcal_fit(actual.get("kcal"), target.get("kcal")),
        "carbs_fit": carbs_fit(actual.get("carbs_g"), target.get("carbs_g")),
        "fat_fit": fat_fit(actual.get("fat_g"), target.get("fat_g")),
    }
    total_fit = (
        0.40 * component_scores["protein_fit"]
        + 0.35 * component_scores["kcal_fit"]
        + 0.15 * component_scores["carbs_fit"]
        + 0.10 * component_scores["fat_fit"]
    )
    return {
        "macro_fit": round(clamp(total_fit), 4),
        "protein_fit": round(component_scores["protein_fit"], 4),
        "kcal_fit": round(component_scores["kcal_fit"], 4),
        "carbs_fit": round(component_scores["carbs_fit"], 4),
        "fat_fit": round(component_scores["fat_fit"], 4),
    }


def _component_values(actual: object, target: object) -> tuple[float | None, float | None]:
    target_value = _to_positive_target(target)
    actual_value = _to_float(actual)
    return actual_value, target_value


def _to_positive_target(value: object) -> float | None:
    numeric_value = _to_float(value)
    if numeric_value is None or numeric_value <= 0:
        return None
    return numeric_value


def _to_float(value: object) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value):
        return None
    return numeric_value

