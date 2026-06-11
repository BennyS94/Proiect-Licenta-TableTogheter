from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from src.generator_v1.macro_fit import clamp


DIETARY_PATTERN_KEYS = ("keto", "paleo", "mediterranean")
HEALTH_MODE_KEYS = (
    "diabetes_aware",
    "hypertension_friendly",
    "heart_friendly",
)

DEFAULT_HEALTH_AND_DIET_PREFERENCES = {
    "dietary_patterns": {
        "keto": False,
        "paleo": False,
        "mediterranean": False,
    },
    "health_modes": {
        "diabetes_aware": False,
        "hypertension_friendly": False,
        "heart_friendly": False,
    },
}


def normalize_health_and_diet_preferences(value: Any) -> dict[str, dict[str, bool]]:
    source = value if isinstance(value, Mapping) else {}
    dietary_source = (
        source.get("dietary_patterns")
        if isinstance(source.get("dietary_patterns"), Mapping)
        else {}
    )
    health_source = (
        source.get("health_modes") if isinstance(source.get("health_modes"), Mapping) else {}
    )
    return {
        "dietary_patterns": {
            key: bool(dietary_source.get(key, False)) for key in DIETARY_PATTERN_KEYS
        },
        "health_modes": {
            key: bool(health_source.get(key, False)) for key in HEALTH_MODE_KEYS
        },
    }


def compute_health_and_diet_fit(
    candidate_row: Mapping[str, object],
    preferences: Mapping[str, object] | None,
) -> dict[str, object]:
    normalized = normalize_health_and_diet_preferences(preferences)
    active_patterns = [
        key
        for key, enabled in normalized["dietary_patterns"].items()
        if bool(enabled)
    ]
    active_modes = [
        key for key, enabled in normalized["health_modes"].items() if bool(enabled)
    ]
    score = 1.0
    reasons: list[str] = []

    for pattern in active_patterns:
        pattern_score, pattern_reasons = _dietary_pattern_fit(candidate_row, pattern)
        score *= pattern_score
        reasons.extend(pattern_reasons)

    for mode in active_modes:
        mode_score, mode_reasons = _health_mode_fit(candidate_row, mode)
        score *= mode_score
        reasons.extend(mode_reasons)

    return {
        "health_and_diet_fit": round(clamp(score), 4),
        "health_and_diet_reasons": reasons or ["health_and_diet_neutral"],
        "active_dietary_patterns": active_patterns,
        "active_health_modes": active_modes,
    }


def _dietary_pattern_fit(
    candidate_row: Mapping[str, object],
    pattern: str,
) -> tuple[float, list[str]]:
    carbs = _to_float(candidate_row.get("carbs_g"))
    protein = _to_float(candidate_row.get("protein_g"))
    fat = _to_float(candidate_row.get("fat_g"))
    kcal = _to_float(candidate_row.get("kcal"))
    category_text = _candidate_text(candidate_row)

    if pattern == "keto":
        if carbs <= 8:
            return 1.0, ["keto_fit_low_carb"]
        if carbs <= 18:
            return 0.88, ["keto_fit_moderate_carb"]
        if carbs <= 30:
            return 0.72, ["keto_penalty_higher_carb"]
        return 0.55, ["keto_penalty_high_carb"]

    if pattern == "paleo":
        score = 1.0
        reasons = ["paleo_fit_neutral"]
        if protein >= 18:
            score *= 1.0
            reasons = ["paleo_fit_protein_forward"]
        if carbs > 55:
            score *= 0.82
            reasons.append("paleo_penalty_high_carb")
        return clamp(score), reasons

    if pattern == "mediterranean":
        score = 0.96
        reasons = ["mediterranean_fit_neutral"]
        if any(
            marker in category_text
            for marker in (
                "mediterranean",
                "greek",
                "italian",
                "fish",
                "seafood",
                "salad",
                "vegetarian",
            )
        ):
            score = 1.0
            reasons = ["mediterranean_fit_category"]
        if kcal > 0 and fat > 0 and fat * 9 / kcal > 0.55:
            score *= 0.9
            reasons.append("mediterranean_penalty_fat_heavy")
        return clamp(score), reasons

    return 1.0, [f"{pattern}_not_supported"]


def _health_mode_fit(
    candidate_row: Mapping[str, object],
    mode: str,
) -> tuple[float, list[str]]:
    carbs = _to_float(candidate_row.get("carbs_g"))
    protein = _to_float(candidate_row.get("protein_g"))
    sugars = _to_float(candidate_row.get("sugars_g"))
    salt = _to_float(candidate_row.get("salt_g"))
    proxy_flags = _string_set(candidate_row.get("health_proxy_flags"))

    if mode == "diabetes_aware":
        score = 1.0
        reasons = ["diabetes_aware_fit_neutral"]
        if carbs > 65 and protein < 18:
            score *= 0.55
            reasons = ["diabetes_aware_penalty_high_carb_low_protein"]
        elif carbs > 55:
            score *= 0.68
            reasons = ["diabetes_aware_penalty_high_carb"]
        elif carbs > 35 and protein < 12:
            score *= 0.78
            reasons = ["diabetes_aware_penalty_moderate_carb_low_protein"]
        elif carbs <= 35 and protein >= 15:
            reasons = ["diabetes_aware_fit_protein_moderate_carb"]
        if sugars > 30:
            score *= 0.6
            reasons.append("diabetes_aware_penalty_high_sugar")
        elif sugars > 18:
            score *= 0.78
            reasons.append("diabetes_aware_penalty_moderate_sugar")
        return clamp(score), reasons

    if mode == "hypertension_friendly":
        score = 1.0
        reasons = ["hypertension_friendly_fit_neutral"]
        if salt > 2.0:
            score *= 0.62
            reasons = ["hypertension_friendly_penalty_high_salt"]
        elif salt > 1.2:
            score *= 0.78
            reasons = ["hypertension_friendly_penalty_moderate_salt"]
        if "processed_salty" in proxy_flags:
            score *= 0.72
            reasons.append("hypertension_friendly_penalty_processed_salty_proxy")
        if "salty_sauce" in proxy_flags:
            score *= 0.78
            reasons.append("hypertension_friendly_penalty_salty_sauce_proxy")
        if score == 1.0:
            reasons = ["hypertension_friendly_fit_simple_recipe"]
        return clamp(score), reasons

    return 1.0, [f"{mode}_pending"]


def _candidate_text(candidate_row: Mapping[str, object]) -> str:
    parts = [
        candidate_row.get("display_name"),
        candidate_row.get("recipe_family_name"),
        candidate_row.get("recipe_category"),
        candidate_row.get("recipe_subcategory"),
        candidate_row.get("recipe_cuisine"),
    ]
    return " ".join(str(part or "").lower() for part in parts)


def _to_float(value: object) -> float:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(numeric_value) or math.isinf(numeric_value):
        return 0.0
    return numeric_value


def _string_set(value: object) -> set[str]:
    if isinstance(value, list):
        return {str(item).strip() for item in value if str(item).strip()}
    if isinstance(value, tuple):
        return {str(item).strip() for item in value if str(item).strip()}
    text = str(value or "").strip()
    if not text:
        return set()
    return {item.strip() for item in text.split(";") if item.strip()}
