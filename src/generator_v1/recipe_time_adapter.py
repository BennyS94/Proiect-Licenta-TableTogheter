from __future__ import annotations

import math
import re
from typing import Any


PASSIVE_KEYWORDS = (
    "marinate",
    "marinated",
    "marinade",
    "chill",
    "chilled",
    "refrigerate",
    "refrigerated",
    "refrigerator",
    "overnight",
    "rest",
    "rested",
    "let stand",
    "freeze",
    "frozen",
    "slow cooker",
    "crockpot",
)

LONG_PASSIVE_THRESHOLD_MIN = 180.0
EXTREME_PASSIVE_THRESHOLD_MIN = 720.0
# Fallback temporar pentru pilot, nu model de timp final.
PILOT_PASSIVE_ACTIVE_CAP_MIN = 90.0
PILOT_EXTREME_PASSIVE_ACTIVE_CAP_MIN = 75.0
APPROX_TIME_EQUAL_TOLERANCE_MIN = 5.0


def compute_time_features(recipe_row: Any) -> dict[str, object]:
    prep_time = _to_non_negative_float(_get_value(recipe_row, "prep_time_min"))
    cook_time = _to_non_negative_float(_get_value(recipe_row, "cook_time_min"))
    total_time = _to_non_negative_float(_get_value(recipe_row, "total_time_min"))
    reasons: list[str] = []

    active_time = _active_time(prep_time, cook_time, reasons)
    if active_time is None and total_time is not None:
        active_time = total_time
        reasons.append("fallback_total_time_fara_prep_cook")

    passive_time = _passive_time(total_time, active_time, reasons)
    has_passive_keyword = _has_passive_keyword(recipe_row)
    original_effective_time = active_time
    has_long_passive_time = bool(
        (
            has_passive_keyword
            and total_time is not None
            and total_time >= LONG_PASSIVE_THRESHOLD_MIN
        )
        or (
            passive_time is not None
            and passive_time >= LONG_PASSIVE_THRESHOLD_MIN
        )
    )
    if has_passive_keyword:
        reasons.append("text_indica_timp_pasiv")
    if passive_time is not None and passive_time >= LONG_PASSIVE_THRESHOLD_MIN:
        reasons.append("pasiv_peste_180_min")

    uses_pilot_time_fallback = _should_apply_pilot_passive_fallback(
        has_passive_keyword=has_passive_keyword,
        total_time=total_time,
        active_time=active_time,
    )
    if uses_pilot_time_fallback and total_time is not None:
        active_time = _pilot_active_time_cap(total_time)
        passive_time = round(max(total_time - active_time, 0.0), 1)
        has_long_passive_time = True
        reasons.append("pilot_passive_time_cap_applied")

    return {
        "active_time_estimated_min": active_time,
        "passive_time_estimated_min": passive_time,
        "effective_time_min_for_scoring": active_time,
        "original_effective_time_min_for_scoring": original_effective_time,
        "has_long_passive_time": has_long_passive_time,
        "uses_pilot_time_fallback": uses_pilot_time_fallback,
        "time_estimation_reasons": reasons,
    }


def _active_time(
    prep_time: float | None,
    cook_time: float | None,
    reasons: list[str],
) -> float | None:
    if prep_time is None or cook_time is None:
        reasons.append("prep_sau_cook_lipsa")
        return None
    active_time = prep_time + cook_time
    reasons.append("active_time_din_prep_plus_cook")
    return round(max(active_time, 0.0), 1)


def _passive_time(
    total_time: float | None,
    active_time: float | None,
    reasons: list[str],
) -> float | None:
    if total_time is None or active_time is None:
        reasons.append("pasiv_necalculat_date_lipsa")
        return None
    passive_time = max(total_time - active_time, 0.0)
    if passive_time > 0:
        reasons.append("pasiv_din_total_minus_activ")
    return round(passive_time, 1)


def _has_passive_keyword(recipe_row: Any) -> bool:
    text = _normalize_text(
        " ".join(
            [
                str(_get_value(recipe_row, "display_name") or ""),
                str(_get_value(recipe_row, "recipe_name") or ""),
                str(_get_value(recipe_row, "directions_json") or ""),
                str(_get_value(recipe_row, "qc_notes") or ""),
            ]
        )
    )
    if not text:
        return False
    return any(_keyword_pattern(keyword).search(text) for keyword in PASSIVE_KEYWORDS)


def _should_apply_pilot_passive_fallback(
    has_passive_keyword: bool,
    total_time: float | None,
    active_time: float | None,
) -> bool:
    if not has_passive_keyword or total_time is None:
        return False
    if total_time < LONG_PASSIVE_THRESHOLD_MIN:
        return False
    if active_time is None:
        return True
    return _approximately_equal(active_time, total_time) or active_time >= LONG_PASSIVE_THRESHOLD_MIN


def _pilot_active_time_cap(total_time: float) -> float:
    if total_time >= EXTREME_PASSIVE_THRESHOLD_MIN:
        return PILOT_EXTREME_PASSIVE_ACTIVE_CAP_MIN
    return PILOT_PASSIVE_ACTIVE_CAP_MIN


def _approximately_equal(left: float, right: float) -> bool:
    return abs(left - right) <= APPROX_TIME_EQUAL_TOLERANCE_MIN


def _get_value(recipe_row: Any, key: str) -> object:
    if hasattr(recipe_row, "get"):
        return recipe_row.get(key)
    return getattr(recipe_row, key, None)


def _to_non_negative_float(value: object) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value):
        return None
    return max(numeric_value, 0.0)


def _normalize_text(value: str) -> str:
    text = value.lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _keyword_pattern(keyword: str) -> re.Pattern[str]:
    normalized = _normalize_text(keyword)
    return re.compile(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])")
