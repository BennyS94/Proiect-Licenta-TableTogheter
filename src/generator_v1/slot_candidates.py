from __future__ import annotations

import json
from typing import Iterable

import pandas as pd

from src.generator_v1.feedback_fit import compute_feedback_fit
from src.generator_v1.health_diet_fit import compute_health_and_diet_fit
from src.generator_v1.macro_fit import macro_fit
from src.generator_v1.meal_realism import compute_meal_realism
from src.generator_v1.nutrition_quality import compute_nutrition_quality
from src.generator_v1.pilot_nutrition_overlay import compute_pilot_overlay_nutrition
from src.generator_v1.portion_policy import (
    PortionPolicyDecision,
    STANDARD_PORTION_MULTIPLIERS,
    get_portion_policy_decision,
    warnings_for_multiplier,
)
from src.generator_v1.recipe_time_adapter import compute_time_features
from src.generator_v1.score_preview import compute_score_preview
from src.generator_v1.slot_fit import compute_slot_fit
from src.generator_v1.target_builder import NutritionTarget
from src.generator_v1.time_fit import apply_time_feedback_penalty, household_time_fit


PORTION_MULTIPLIERS = tuple(STANDARD_PORTION_MULTIPLIERS)

INGREDIENT_TEXT_COLUMNS = (
    "ingredient_name_normalized",
    "ingredient_name_parsed",
    "mapped_food_canonical_name",
    "ingredient_raw_text",
)

HEALTH_PROXY_KEYWORDS = {
    "processed_salty": (
        "bacon",
        "cured meat",
        "ham",
        "pepperoni",
        "processed meat",
        "salami",
        "sausage",
    ),
    "salty_sauce": (
        "bouillon",
        "fish sauce",
        "soy sauce",
        "stock cube",
    ),
    "fatty_creamy": (
        "butter",
        "cheese",
        "cream",
        "heavy cream",
        "sour cream",
    ),
    "fried_heavy": (
        "deep fried",
        "fried",
    ),
}


def build_slot_candidates(
    target: NutritionTarget,
    filtered_candidates: pd.DataFrame,
    time_sensitivity: str = "normal",
    portion_multipliers: Iterable[float] | None = None,
    ingredients: pd.DataFrame | None = None,
    fooddb: pd.DataFrame | None = None,
    portion_policy_mode: str = "standard",
    feedback_preference_context: dict[str, object] | None = None,
    health_and_diet_preferences: dict[str, object] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if filtered_candidates.empty:
        return pd.DataFrame(columns=_slot_candidate_columns())

    for slot in target.slot_targets:
        slot_target = target.slot_targets[slot]
        for _, recipe in filtered_candidates.iterrows():
            if not _slot_allowed_for_recipe(recipe, slot):
                continue
            recipe_ingredients = _ingredients_for_recipe(recipe, ingredients)
            health_proxy_flags = _health_proxy_flags(recipe_ingredients)
            overlay = compute_pilot_overlay_nutrition(
                recipe_row=recipe,
                ingredients_for_recipe=recipe_ingredients,
                fooddb_df=fooddb if fooddb is not None else pd.DataFrame(),
                nutrition_cache_row=recipe,
            )
            time_features = compute_time_features(recipe)
            base_time_fit = household_time_fit(
                total_time_min=time_features["effective_time_min_for_scoring"],
                slot=slot,
                time_sensitivity=time_sensitivity,
            )
            time_feedback = apply_time_feedback_penalty(
                time_fit=base_time_fit,
                recipe_id=recipe.get("recipe_id"),
                preference_context=feedback_preference_context,
            )
            time_fit = time_feedback["time_fit"]
            serving_weight_g_estimated = _serving_weight_g_estimated(recipe)
            overlay_serving_weight_g_estimated = _overlay_serving_weight_g_estimated(
                overlay
            )
            macro_values = _macro_values(recipe, overlay)
            policy_decision = _portion_policy_decision(
                slot=slot,
                recipe=recipe,
                macro_values=macro_values,
                serving_weight_g_estimated=serving_weight_g_estimated,
                overlay_serving_weight_g_estimated=overlay_serving_weight_g_estimated,
                slot_target=slot_target,
                portion_multipliers=portion_multipliers,
                portion_policy_mode=portion_policy_mode,
            )
            for portion_multiplier in policy_decision.multipliers:
                original_portion_grams_estimated = _scaled_optional(
                    serving_weight_g_estimated,
                    portion_multiplier,
                )
                overlay_portion_grams_estimated = _scaled_optional(
                    overlay_serving_weight_g_estimated,
                    portion_multiplier,
                )
                portion_fields = _portion_gram_fields(
                    uses_overlay=bool(overlay["uses_pilot_nutrition_overlay"]),
                    original_portion_grams_estimated=original_portion_grams_estimated,
                    overlay_portion_grams_estimated=overlay_portion_grams_estimated,
                )
                actual_macros = {
                    "kcal": _scaled(macro_values["energy_kcal_per_serving"], portion_multiplier),
                    "protein_g": _scaled(macro_values["protein_g_per_serving"], portion_multiplier),
                    "carbs_g": _scaled(macro_values["carbs_g_per_serving"], portion_multiplier),
                    "fat_g": _scaled(macro_values["fat_g_per_serving"], portion_multiplier),
                    "sugars_g": _scaled(recipe.get("sugars_g_per_serving"), portion_multiplier),
                    "fibre_g": _scaled(recipe.get("fibre_g_per_serving"), portion_multiplier),
                    "salt_g": _scaled(recipe.get("salt_g_per_serving"), portion_multiplier),
                }
                macro_scores = macro_fit(actual=actual_macros, target=slot_target)
                candidate_row = {
                    "slot": slot,
                    "recipe_id": recipe.get("recipe_id"),
                    "display_name": recipe.get("display_name"),
                    "recipe_name": recipe.get("recipe_name"),
                    "recipe_family_name": recipe.get("recipe_family_name"),
                    "recipe_kind": recipe.get("recipe_kind"),
                    "recipe_category": recipe.get("recipe_category"),
                    "recipe_subcategory": recipe.get("recipe_subcategory"),
                    "recipe_cuisine": recipe.get("recipe_cuisine"),
                    "allowed_slots_json": recipe.get("allowed_slots_json"),
                    "slot_policy_reason": recipe.get("slot_policy_reason"),
                    "portion_multiplier": float(portion_multiplier),
                    "serving_weight_g_estimated": serving_weight_g_estimated,
                    "portion_grams_estimated": portion_fields["portion_grams_estimated"],
                    "original_portion_grams_estimated": original_portion_grams_estimated,
                    "overlay_serving_weight_g_estimated": overlay_serving_weight_g_estimated,
                    "overlay_portion_grams_estimated": overlay_portion_grams_estimated,
                    "portion_grams_source": portion_fields["portion_grams_source"],
                    "portion_policy_mode": policy_decision.mode,
                    "portion_policy_reasons": policy_decision.reasons,
                    "portion_policy_warnings": warnings_for_multiplier(
                        policy_decision,
                        float(portion_multiplier),
                    ),
                    "portion_multiplier_allowed_by_policy": True,
                    "original_energy_kcal_per_serving": _to_float(
                        recipe.get("energy_kcal_per_serving")
                    ),
                    "original_protein_g_per_serving": _to_float(
                        recipe.get("protein_g_per_serving")
                    ),
                    "original_carbs_g_per_serving": _to_float(
                        recipe.get("carbs_g_per_serving")
                    ),
                    "original_fat_g_per_serving": _to_float(
                        recipe.get("fat_g_per_serving")
                    ),
                    "original_sugars_g_per_serving": _to_float(
                        recipe.get("sugars_g_per_serving")
                    ),
                    "original_fibre_g_per_serving": _to_float(
                        recipe.get("fibre_g_per_serving")
                    ),
                    "original_salt_g_per_serving": _to_float(
                        recipe.get("salt_g_per_serving")
                    ),
                    "overlay_energy_kcal_per_serving": overlay[
                        "overlay_energy_kcal_per_serving"
                    ],
                    "overlay_protein_g_per_serving": overlay[
                        "overlay_protein_g_per_serving"
                    ],
                    "overlay_carbs_g_per_serving": overlay[
                        "overlay_carbs_g_per_serving"
                    ],
                    "overlay_fat_g_per_serving": overlay[
                        "overlay_fat_g_per_serving"
                    ],
                    "overlay_energy_kcal_total": overlay["overlay_energy_kcal_total"],
                    "overlay_protein_g_total": overlay["overlay_protein_g_total"],
                    "overlay_carbs_g_total": overlay["overlay_carbs_g_total"],
                    "overlay_fat_g_total": overlay["overlay_fat_g_total"],
                    "overlay_mapped_weight_grams": overlay["overlay_mapped_weight_grams"],
                    "overlay_alias_weight_grams": overlay["overlay_alias_weight_grams"],
                    "overlay_used_alias_count": overlay["overlay_used_alias_count"],
                    "overlay_used_existing_mapping_count": overlay[
                        "overlay_used_existing_mapping_count"
                    ],
                    "overlay_estimated_servings_basis": overlay[
                        "overlay_estimated_servings_basis"
                    ],
                    "uses_pilot_nutrition_overlay": overlay[
                        "uses_pilot_nutrition_overlay"
                    ],
                    "pilot_nutrition_overlay_reasons": overlay[
                        "pilot_nutrition_overlay_reasons"
                    ],
                    "overlay_aliases_used": overlay["overlay_aliases_used"],
                    "kcal": actual_macros["kcal"],
                    "protein_g": actual_macros["protein_g"],
                    "carbs_g": actual_macros["carbs_g"],
                    "fat_g": actual_macros["fat_g"],
                    "sugars_g": actual_macros["sugars_g"],
                    "fibre_g": actual_macros["fibre_g"],
                    "salt_g": actual_macros["salt_g"],
                    "macro_fit": macro_scores["macro_fit"],
                    "protein_fit": macro_scores["protein_fit"],
                    "kcal_fit": macro_scores["kcal_fit"],
                    "carbs_fit": macro_scores["carbs_fit"],
                    "fat_fit": macro_scores["fat_fit"],
                    "total_time_min": _to_float(recipe.get("total_time_min")),
                    "total_elapsed_time_min": time_features["total_elapsed_time_min"],
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
                    "time_confidence": time_features["time_confidence"],
                    "time_estimation_method": time_features["time_estimation_method"],
                    "time_warnings": time_features["time_warnings"],
                    "time_estimation_reasons": time_features["time_estimation_reasons"],
                    "time_fit": time_fit,
                    "time_feedback_penalty": time_feedback["time_feedback_penalty"],
                    "time_fit_reasons": time_feedback["time_fit_reasons"],
                    "health_proxy_flags": health_proxy_flags,
                }
                feedback_scores = compute_feedback_fit(
                    candidate_row,
                    feedback_preference_context,
                )
                candidate_row.update(feedback_scores)
                slot_scores = compute_slot_fit(candidate_row, slot)
                candidate_row.update(slot_scores)
                base_realism = compute_meal_realism(candidate_row, slot)
                practical_realism = compute_meal_realism(
                    candidate_row,
                    slot,
                    policy="practical",
                )
                candidate_row.update(base_realism)
                candidate_row.update(
                    {
                        "meal_realism_practical_score": practical_realism[
                            "meal_realism_score"
                        ],
                        "meal_realism_practical_penalty": practical_realism[
                            "meal_realism_penalty"
                        ],
                        "meal_realism_practical_flags": practical_realism[
                            "meal_realism_flags"
                        ],
                        "meal_realism_practical_reasons": practical_realism[
                            "meal_realism_reasons"
                        ],
                        "realism_hard_reject": practical_realism[
                            "realism_hard_reject"
                        ],
                        "realism_reject_reason": practical_realism[
                            "realism_reject_reason"
                        ],
                    }
                )
                nutrition_scores = compute_nutrition_quality(candidate_row, slot_target)
                candidate_row.update(nutrition_scores)
                health_and_diet_scores = compute_health_and_diet_fit(
                    candidate_row,
                    health_and_diet_preferences,
                )
                candidate_row.update(health_and_diet_scores)
                preview_scores = compute_score_preview(candidate_row)
                candidate_row.update(
                    {
                        "base_score_preview": preview_scores["base_score_preview"],
                        "nutrition_quality": preview_scores["nutrition_quality"],
                        "health_and_diet_fit": preview_scores["health_and_diet_fit"],
                        "health_and_diet_reasons": health_and_diet_scores[
                            "health_and_diet_reasons"
                        ],
                        "active_dietary_patterns": health_and_diet_scores[
                            "active_dietary_patterns"
                        ],
                        "active_health_modes": health_and_diet_scores[
                            "active_health_modes"
                        ],
                        "feedback_fit": preview_scores["feedback_fit"],
                        "feedback_bonus_penalty": feedback_scores[
                            "feedback_bonus_penalty"
                        ],
                        "feedback_reasons": feedback_scores["feedback_reasons"],
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
        "recipe_family_name",
        "recipe_kind",
        "recipe_category",
        "recipe_subcategory",
        "recipe_cuisine",
        "allowed_slots_json",
        "slot_policy_reason",
        "portion_multiplier",
        "serving_weight_g_estimated",
        "portion_grams_estimated",
        "original_portion_grams_estimated",
        "overlay_serving_weight_g_estimated",
        "overlay_portion_grams_estimated",
        "portion_grams_source",
        "portion_policy_mode",
        "portion_policy_reasons",
        "portion_policy_warnings",
        "portion_multiplier_allowed_by_policy",
        "original_energy_kcal_per_serving",
        "original_protein_g_per_serving",
        "original_carbs_g_per_serving",
        "original_fat_g_per_serving",
        "original_sugars_g_per_serving",
        "original_fibre_g_per_serving",
        "original_salt_g_per_serving",
        "overlay_energy_kcal_per_serving",
        "overlay_protein_g_per_serving",
        "overlay_carbs_g_per_serving",
        "overlay_fat_g_per_serving",
        "overlay_energy_kcal_total",
        "overlay_protein_g_total",
        "overlay_carbs_g_total",
        "overlay_fat_g_total",
        "overlay_mapped_weight_grams",
        "overlay_alias_weight_grams",
        "overlay_used_alias_count",
        "overlay_used_existing_mapping_count",
        "overlay_estimated_servings_basis",
        "uses_pilot_nutrition_overlay",
        "pilot_nutrition_overlay_reasons",
        "overlay_aliases_used",
        "kcal",
        "protein_g",
        "carbs_g",
        "fat_g",
        "sugars_g",
        "fibre_g",
        "salt_g",
        "macro_fit",
        "protein_fit",
        "kcal_fit",
        "carbs_fit",
        "fat_fit",
        "total_time_min",
        "total_elapsed_time_min",
        "active_time_estimated_min",
        "passive_time_estimated_min",
        "effective_time_min_for_scoring",
        "original_effective_time_min_for_scoring",
        "has_long_passive_time",
        "uses_pilot_time_fallback",
        "time_confidence",
        "time_estimation_method",
        "time_warnings",
        "time_estimation_reasons",
        "time_fit",
        "time_feedback_penalty",
        "time_fit_reasons",
        "health_proxy_flags",
        "slot_fit",
        "slot_fit_reasons",
        "is_slot_suspicious",
        "slot_suspicion_reasons",
        "meal_realism_score",
        "meal_realism_penalty",
        "meal_realism_flags",
        "meal_realism_reasons",
        "meal_realism_practical_score",
        "meal_realism_practical_penalty",
        "meal_realism_practical_flags",
        "meal_realism_practical_reasons",
        "realism_hard_reject",
        "realism_reject_reason",
        "nutrition_quality",
        "nutrition_quality_reasons",
        "health_and_diet_fit",
        "health_and_diet_reasons",
        "active_dietary_patterns",
        "active_health_modes",
        "is_nutrition_suspicious",
        "base_score_preview",
        "feedback_fit",
        "feedback_bonus_penalty",
        "feedback_reasons",
        "variety_fit",
        "score_preview",
    ]


def _portion_policy_decision(
    slot: str,
    recipe: pd.Series,
    macro_values: dict[str, object],
    serving_weight_g_estimated: float | None,
    overlay_serving_weight_g_estimated: float | None,
    slot_target: dict[str, object],
    portion_multipliers: Iterable[float] | None,
    portion_policy_mode: str,
) -> PortionPolicyDecision:
    if portion_multipliers is not None:
        multipliers = _sorted_unique_floats(portion_multipliers)
        return PortionPolicyDecision(
            mode="custom",
            multipliers=multipliers or list(PORTION_MULTIPLIERS),
            reasons=["explicit_portion_multipliers"],
            warnings_by_multiplier={},
        )

    policy_payload = {
        **recipe.to_dict(),
        **macro_values,
        "serving_weight_g_estimated": (
            overlay_serving_weight_g_estimated
            if overlay_serving_weight_g_estimated is not None
            else serving_weight_g_estimated
        ),
        "overlay_serving_weight_g_estimated": overlay_serving_weight_g_estimated,
    }
    return get_portion_policy_decision(
        slot=slot,
        recipe_row_or_candidate=policy_payload,
        slot_target=slot_target,
        mode=portion_policy_mode,
    )


def _sorted_unique_floats(values: Iterable[float]) -> list[float]:
    result: list[float] = []
    seen: set[float] = set()
    for value in values:
        numeric_value = _to_float(value)
        if numeric_value is None or numeric_value <= 0:
            continue
        rounded = round(numeric_value, 4)
        if rounded in seen:
            continue
        seen.add(rounded)
        result.append(rounded)
    return sorted(result)


def _ingredients_for_recipe(
    recipe: pd.Series,
    ingredients: pd.DataFrame | None,
) -> pd.DataFrame:
    if ingredients is None or ingredients.empty or "recipe_id" not in ingredients.columns:
        return pd.DataFrame()
    recipe_id = str(recipe.get("recipe_id") or "").strip()
    if not recipe_id:
        return pd.DataFrame()
    return ingredients.loc[ingredients["recipe_id"].astype(str).eq(recipe_id)].copy()


def _health_proxy_flags(recipe_ingredients: pd.DataFrame) -> list[str]:
    if recipe_ingredients.empty:
        return []
    available_columns = [
        column for column in INGREDIENT_TEXT_COLUMNS if column in recipe_ingredients.columns
    ]
    if not available_columns:
        return []
    text = (
        recipe_ingredients[available_columns]
        .fillna("")
        .astype(str)
        .agg(" ".join, axis=1)
        .str.lower()
    )
    joined = " ".join(text.tolist())
    flags: list[str] = []
    for flag, keywords in HEALTH_PROXY_KEYWORDS.items():
        if any(keyword in joined for keyword in keywords):
            flags.append(flag)
    return flags


def _slot_allowed_for_recipe(recipe: pd.Series, slot: str) -> bool:
    if "allowed_slots_json" not in recipe.index:
        return True
    allowed_slots = _parse_allowed_slots(recipe.get("allowed_slots_json"))
    if allowed_slots is None:
        return True
    return str(slot).strip().lower() in allowed_slots


def _parse_allowed_slots(value: object) -> set[str] | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return set()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = [part.strip() for part in text.split(",") if part.strip()]
    if not isinstance(parsed, list):
        return set()
    return {str(item).strip().lower() for item in parsed if str(item).strip()}


def _macro_values(recipe: pd.Series, overlay: dict[str, object]) -> dict[str, object]:
    if bool(overlay.get("uses_pilot_nutrition_overlay")):
        return {
            "energy_kcal_per_serving": overlay.get("overlay_energy_kcal_per_serving"),
            "protein_g_per_serving": overlay.get("overlay_protein_g_per_serving"),
            "carbs_g_per_serving": overlay.get("overlay_carbs_g_per_serving"),
            "fat_g_per_serving": overlay.get("overlay_fat_g_per_serving"),
        }
    return {
        "energy_kcal_per_serving": recipe.get("energy_kcal_per_serving"),
        "protein_g_per_serving": recipe.get("protein_g_per_serving"),
        "carbs_g_per_serving": recipe.get("carbs_g_per_serving"),
        "fat_g_per_serving": recipe.get("fat_g_per_serving"),
    }


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


def _overlay_serving_weight_g_estimated(overlay: dict[str, object]) -> float | None:
    mapped_weight = _to_positive_float(overlay.get("overlay_mapped_weight_grams"))
    servings_basis = _to_positive_float(overlay.get("overlay_estimated_servings_basis"))
    if mapped_weight is None or servings_basis is None:
        return None
    return round(mapped_weight / servings_basis, 1)


def _portion_gram_fields(
    uses_overlay: bool,
    original_portion_grams_estimated: float | None,
    overlay_portion_grams_estimated: float | None,
) -> dict[str, object]:
    if uses_overlay and overlay_portion_grams_estimated is not None:
        return {
            "portion_grams_estimated": overlay_portion_grams_estimated,
            "portion_grams_source": "overlay",
        }
    if original_portion_grams_estimated is not None:
        return {
            "portion_grams_estimated": original_portion_grams_estimated,
            "portion_grams_source": "cache",
        }
    return {
        "portion_grams_estimated": None,
        "portion_grams_source": "unknown",
    }


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
