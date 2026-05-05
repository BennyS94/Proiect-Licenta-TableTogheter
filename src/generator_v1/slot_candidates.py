from __future__ import annotations

from typing import Iterable

import pandas as pd

from src.generator_v1.macro_fit import macro_fit
from src.generator_v1.nutrition_quality import compute_nutrition_quality
from src.generator_v1.recipe_time_adapter import compute_time_features
from src.generator_v1.score_preview import compute_score_preview
from src.generator_v1.slot_fit import compute_slot_fit
from src.generator_v1.target_builder import NutritionTarget
from src.generator_v1.time_fit import household_time_fit


PORTION_MULTIPLIERS = (0.8, 1.0, 1.2)


def build_slot_candidates(
    target: NutritionTarget,
    filtered_candidates: pd.DataFrame,
    time_sensitivity: str = "normal",
    portion_multipliers: Iterable[float] = PORTION_MULTIPLIERS,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if filtered_candidates.empty:
        return pd.DataFrame(columns=_slot_candidate_columns())

    for slot in target.slot_targets:
        slot_target = target.slot_targets[slot]
        for _, recipe in filtered_candidates.iterrows():
            time_features = compute_time_features(recipe)
            time_fit = household_time_fit(
                total_time_min=time_features["effective_time_min_for_scoring"],
                slot=slot,
                time_sensitivity=time_sensitivity,
            )
            serving_weight_g_estimated = _serving_weight_g_estimated(recipe)
            for portion_multiplier in portion_multipliers:
                actual_macros = {
                    "kcal": _scaled(recipe.get("energy_kcal_per_serving"), portion_multiplier),
                    "protein_g": _scaled(recipe.get("protein_g_per_serving"), portion_multiplier),
                    "carbs_g": _scaled(recipe.get("carbs_g_per_serving"), portion_multiplier),
                    "fat_g": _scaled(recipe.get("fat_g_per_serving"), portion_multiplier),
                }
                macro_scores = macro_fit(actual=actual_macros, target=slot_target)
                candidate_row = {
                    "slot": slot,
                    "recipe_id": recipe.get("recipe_id"),
                    "display_name": recipe.get("display_name"),
                    "recipe_name": recipe.get("recipe_name"),
                    "recipe_kind": recipe.get("recipe_kind"),
                    "recipe_category": recipe.get("recipe_category"),
                    "recipe_subcategory": recipe.get("recipe_subcategory"),
                    "portion_multiplier": float(portion_multiplier),
                    "serving_weight_g_estimated": serving_weight_g_estimated,
                    "portion_grams_estimated": _scaled_optional(
                        serving_weight_g_estimated,
                        portion_multiplier,
                    ),
                    "kcal": actual_macros["kcal"],
                    "protein_g": actual_macros["protein_g"],
                    "carbs_g": actual_macros["carbs_g"],
                    "fat_g": actual_macros["fat_g"],
                    "macro_fit": macro_scores["macro_fit"],
                    "protein_fit": macro_scores["protein_fit"],
                    "kcal_fit": macro_scores["kcal_fit"],
                    "carbs_fit": macro_scores["carbs_fit"],
                    "fat_fit": macro_scores["fat_fit"],
                    "total_time_min": _to_float(recipe.get("total_time_min")),
                    "active_time_estimated_min": time_features["active_time_estimated_min"],
                    "passive_time_estimated_min": time_features["passive_time_estimated_min"],
                    "effective_time_min_for_scoring": time_features[
                        "effective_time_min_for_scoring"
                    ],
                    "original_effective_time_min_for_scoring": time_features[
                        "original_effective_time_min_for_scoring"
                    ],
                    "has_long_passive_time": time_features["has_long_passive_time"],
                    "uses_pilot_time_fallback": time_features["uses_pilot_time_fallback"],
                    "time_estimation_reasons": time_features["time_estimation_reasons"],
                    "time_fit": time_fit,
                }
                slot_scores = compute_slot_fit(candidate_row, slot)
                candidate_row.update(slot_scores)
                nutrition_scores = compute_nutrition_quality(candidate_row, slot_target)
                candidate_row.update(nutrition_scores)
                preview_scores = compute_score_preview(candidate_row)
                candidate_row.update(
                    {
                        "base_score_preview": preview_scores["base_score_preview"],
                        "nutrition_quality": preview_scores["nutrition_quality"],
                        "feedback_fit": preview_scores["feedback_fit"],
                        "variety_fit": preview_scores["variety_fit"],
                        "score_preview": preview_scores["score_preview"],
                    }
                )
                rows.append(
                    {
                        column: candidate_row.get(column)
                        for column in _slot_candidate_columns()
                    }
                )

    return pd.DataFrame(rows, columns=_slot_candidate_columns())


def _slot_candidate_columns() -> list[str]:
    return [
        "slot",
        "recipe_id",
        "display_name",
        "portion_multiplier",
        "serving_weight_g_estimated",
        "portion_grams_estimated",
        "kcal",
        "protein_g",
        "carbs_g",
        "fat_g",
        "macro_fit",
        "protein_fit",
        "kcal_fit",
        "carbs_fit",
        "fat_fit",
        "total_time_min",
        "active_time_estimated_min",
        "passive_time_estimated_min",
        "effective_time_min_for_scoring",
        "original_effective_time_min_for_scoring",
        "has_long_passive_time",
        "uses_pilot_time_fallback",
        "time_estimation_reasons",
        "time_fit",
        "slot_fit",
        "slot_fit_reasons",
        "nutrition_quality",
        "nutrition_quality_reasons",
        "is_nutrition_suspicious",
        "base_score_preview",
        "feedback_fit",
        "variety_fit",
        "score_preview",
    ]


def _scaled(value: object, portion_multiplier: float) -> float:
    numeric_value = _to_float(value)
    if numeric_value is None:
        return 0.0
    return round(numeric_value * float(portion_multiplier), 1)


def _scaled_optional(value: float | None, portion_multiplier: float) -> float | None:
    if value is None:
        return None
    return round(value * float(portion_multiplier), 1)


def _serving_weight_g_estimated(recipe: pd.Series) -> float | None:
    total_weight = _to_positive_float(recipe.get("total_weight_grams_estimated"))
    servings_basis = _to_positive_float(recipe.get("servings_basis"))
    if total_weight is None or servings_basis is None:
        return None
    return round(total_weight / servings_basis, 1)


def _to_positive_float(value: object) -> float | None:
    numeric_value = _to_float(value)
    if numeric_value is None or numeric_value <= 0:
        return None
    return numeric_value


def _to_float(value: object) -> float | None:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return None
    return float(numeric_value)
