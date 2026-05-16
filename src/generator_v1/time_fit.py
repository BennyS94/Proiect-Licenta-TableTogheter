from __future__ import annotations

import math
from collections.abc import Mapping


def base_time_fit(total_time_min: object, slot: str) -> float:
    total_time = _to_float(total_time_min)
    if total_time is None:
        return 0.0

    if slot == "snack":
        if total_time <= 10:
            return 1.00
        if total_time <= 20:
            return 0.60
        return 0.10

    if total_time <= 15:
        return 1.00
    if total_time <= 30:
        return 0.80
    if total_time <= 45:
        return 0.55
    if total_time <= 60:
        return 0.25
    return 0.05


def household_time_fit(
    total_time_min: object,
    slot: str,
    time_sensitivity: str = "normal",
) -> float:
    base_score = base_time_fit(total_time_min, slot)
    sensitivity = str(time_sensitivity or "normal").strip().lower()
    if sensitivity not in {"low", "normal", "high"}:
        raise ValueError(f"Sensibilitate timp necunoscuta: {time_sensitivity!r}")

    # TODO: ajusteaza low/high dupa ce profilul gospodariei are reguli de timp clare.
    return _clamp_01(base_score)


def apply_time_feedback_penalty(
    time_fit: object,
    recipe_id: object,
    preference_context: Mapping[str, object] | None,
) -> dict[str, object]:
    score = _to_float(time_fit)
    if score is None:
        score = 0.0
    too_long_count = _too_long_count(recipe_id, preference_context)
    if too_long_count <= 0:
        return {
            "time_fit": round(_clamp_01(score), 4),
            "time_feedback_penalty": 0.0,
            "time_fit_reasons": ["time_feedback_neutral"],
        }

    penalty = min(0.35, 0.12 * too_long_count)
    return {
        "time_fit": round(_clamp_01(score - penalty), 4),
        "time_feedback_penalty": round(penalty, 4),
        "time_fit_reasons": [
            f"too_long_count:{too_long_count}",
            "time_feedback_penalty_applied",
        ],
    }


def _too_long_count(
    recipe_id: object,
    preference_context: Mapping[str, object] | None,
) -> int:
    recipe_id_text = str(recipe_id or "").strip()
    if not recipe_id_text or not isinstance(preference_context, Mapping):
        return 0
    time_preferences = preference_context.get("time_preferences")
    if not isinstance(time_preferences, Mapping):
        return 0
    too_long_recipe_ids = time_preferences.get("too_long_recipe_ids")
    if not isinstance(too_long_recipe_ids, Mapping):
        return 0
    try:
        return max(0, int(too_long_recipe_ids.get(recipe_id_text, 0)))
    except (TypeError, ValueError):
        return 0


def _to_float(value: object) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value):
        return None
    return numeric_value


def _clamp_01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
