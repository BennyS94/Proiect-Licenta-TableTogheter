from __future__ import annotations

import json
import math
import re
from typing import Any


PASSIVE_KEYWORDS = (
    "marinate",
    "marinated",
    "marinade",
    "overnight",
    "chill",
    "chilled",
    "refrigerate",
    "refrigerated",
    "refrigerator",
    "rest",
    "rested",
    "let stand",
    "rise",
    "rising",
    "proof",
    "proofing",
    "soak",
    "soaked",
    "simmer",
    "slow cooker",
    "crockpot",
)
SLOW_METHOD_KEYWORDS = ("slow cooker", "crockpot", "overnight", "soak", "marinate")
SOUP_STEW_KEYWORDS = (
    "soup",
    "stew",
    "chili",
    "beans",
    "risotto",
    "casserole",
    "slow cooker",
)

PASSIVE_GAP_THRESHOLD_MIN = 30.0
LONG_PASSIVE_THRESHOLD_MIN = 180.0
EXTREME_PASSIVE_THRESHOLD_MIN = 720.0
PASSIVE_SCORING_FRACTION = 0.08
PASSIVE_SCORING_CAP_MIN = 20.0
SNACK_EFFECTIVE_CAP_MIN = 20.0
BREAKFAST_EFFECTIVE_CAP_MIN = 45.0


def normalize_recipe_time(
    recipe_row: Any,
    directions_text: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config or {}
    warnings: list[str] = []
    reasons: list[str] = []

    prep_time = _to_non_negative_float(_get_value(recipe_row, "prep_time_min"))
    cook_time = _to_non_negative_float(_get_value(recipe_row, "cook_time_min"))
    total_time = _first_numeric(
        _get_value(recipe_row, "total_elapsed_time_min"),
        _get_value(recipe_row, "total_time_min"),
    )
    existing_active = _to_non_negative_float(
        _get_value(recipe_row, "active_time_estimated_min")
    )
    existing_passive = _to_non_negative_float(
        _get_value(recipe_row, "passive_time_estimated_min")
    )
    existing_effective = _to_non_negative_float(
        _get_value(recipe_row, "effective_time_min_for_scoring")
    )
    existing_confidence = _clean_choice(
        _get_value(recipe_row, "time_confidence"),
        _get_value(recipe_row, "time_estimation_confidence"),
    )
    existing_method = _clean_text(_get_value(recipe_row, "time_estimation_method"))
    matched_passive_keywords = _matched_keywords(recipe_row, directions_text)
    has_passive_keyword = bool(matched_passive_keywords)
    allowed_slots = _allowed_slots(recipe_row)
    recipe_kind = _clean_text(_get_value(recipe_row, "recipe_kind"))

    if total_time is None:
        warnings.append("missing_total_time")
    if prep_time is None:
        warnings.append("missing_prep_time")
    if cook_time is None:
        warnings.append("missing_cook_time")

    active_time: float | None
    passive_time: float | None
    total_elapsed_time: float | None
    effective_time: float | None
    confidence: str
    method: str

    if _existing_time_is_reliable(
        existing_active=existing_active,
        existing_passive=existing_passive,
        existing_effective=existing_effective,
        total_time=total_time,
        has_passive_keyword=has_passive_keyword,
    ):
        active_time = existing_active
        passive_time = existing_passive or 0.0
        total_elapsed_time = total_time or round(active_time + passive_time, 1)
        effective_time = existing_effective
        confidence = existing_confidence or "medium"
        method = existing_method or "manual_curated"
        reasons.append("existing_time_fields_preserved")
    elif prep_time is not None or cook_time is not None or total_time is not None:
        active_time = _source_active_time(prep_time, cook_time, total_time, warnings, reasons)
        total_elapsed_time = _source_total_elapsed_time(total_time, active_time, reasons)
        if total_time is not None and prep_time is not None and cook_time is not None:
            source_active = prep_time + cook_time
            if total_time + 1e-6 < source_active:
                warnings.append("total_less_than_prep_plus_cook")
        if (
            active_time is not None
            and total_elapsed_time is not None
            and total_elapsed_time - active_time >= PASSIVE_GAP_THRESHOLD_MIN
        ):
            passive_time = round(total_elapsed_time - active_time, 1)
            reasons.append("passive_time_from_total_minus_active")
        else:
            passive_time = 0.0 if active_time is not None and total_elapsed_time is not None else None

        if _needs_passive_cap(
            has_passive_keyword=has_passive_keyword,
            total_elapsed_time=total_elapsed_time,
            active_time=active_time,
            prep_time=prep_time,
            cook_time=cook_time,
        ):
            active_time = _active_cap_for_long_passive(total_elapsed_time or 0.0)
            passive_time = round(max((total_elapsed_time or 0.0) - active_time, 0.0), 1)
            warnings.append("long_passive_active_time_capped")
            reasons.append("active_time_capped_for_long_passive_source")

        method = (
            "source_fields_with_passive_detection"
            if has_passive_keyword or (passive_time or 0.0) >= PASSIVE_GAP_THRESHOLD_MIN
            else "source_fields"
        )
        confidence = "medium" if method == "source_fields_with_passive_detection" else "high"
        effective_time = _effective_time_for_scoring(active_time, passive_time, warnings, reasons)
    else:
        fallback = _fallback_time_by_recipe_kind(recipe_kind, allowed_slots, recipe_row)
        active_time = fallback["active_time_estimated_min"]
        passive_time = fallback["passive_time_estimated_min"]
        total_elapsed_time = fallback["total_elapsed_time_min"]
        effective_time = fallback["effective_time_min_for_scoring"]
        confidence = "low"
        method = "fallback_by_recipe_kind"
        warnings.extend(fallback["warnings"])
        reasons.extend(fallback["reasons"])

    if has_passive_keyword:
        warnings.append("likely_passive_time")
        reasons.append("passive_keywords:" + ",".join(matched_passive_keywords[:5]))

    has_long_passive_time = bool((passive_time or 0.0) >= LONG_PASSIVE_THRESHOLD_MIN)
    if total_elapsed_time is not None and total_elapsed_time >= EXTREME_PASSIVE_THRESHOLD_MIN:
        warnings.append("very_long_total_time")
    if cook_time is not None and cook_time >= LONG_PASSIVE_THRESHOLD_MIN:
        warnings.append("very_long_cook_time")
    if active_time is None:
        warnings.append("no_active_time")
    if not confidence or confidence == "unknown":
        warnings.append("no_confidence")

    effective_time = _cap_slot_effective_time(
        effective_time=effective_time,
        recipe_kind=recipe_kind,
        allowed_slots=allowed_slots,
        warnings=warnings,
        reasons=reasons,
        config=cfg,
    )

    return {
        "prep_time_min": _round_optional(prep_time),
        "cook_time_min": _round_optional(cook_time),
        "total_elapsed_time_min": _round_optional(total_elapsed_time),
        "active_time_estimated_min": _round_optional(active_time),
        "passive_time_estimated_min": _round_optional(passive_time),
        "effective_time_min_for_scoring": _round_optional(effective_time),
        "original_effective_time_min_for_scoring": _round_optional(existing_effective or active_time),
        "has_long_passive_time": has_long_passive_time,
        "time_confidence": confidence or "unknown",
        "time_estimation_method": method or "missing",
        "time_warnings": _unique_strings(warnings),
        "time_reasons": _unique_strings(reasons),
        "time_estimation_reasons": _unique_strings(reasons),
        "uses_pilot_time_fallback": False,
        "matched_passive_keywords": matched_passive_keywords,
    }


def _source_active_time(
    prep_time: float | None,
    cook_time: float | None,
    total_time: float | None,
    warnings: list[str],
    reasons: list[str],
) -> float | None:
    if prep_time is not None and cook_time is not None:
        reasons.append("active_time_from_prep_plus_cook")
        return round(prep_time + cook_time, 1)
    if total_time is not None:
        warnings.append("active_time_from_total_due_missing_prep_or_cook")
        reasons.append("active_time_fallback_total_time")
        return round(total_time, 1)
    return None


def _source_total_elapsed_time(
    total_time: float | None,
    active_time: float | None,
    reasons: list[str],
) -> float | None:
    if total_time is not None:
        reasons.append("total_elapsed_from_source_total_time")
        return round(total_time, 1)
    if active_time is not None:
        reasons.append("total_elapsed_from_active_time")
        return round(active_time, 1)
    return None


def _needs_passive_cap(
    *,
    has_passive_keyword: bool,
    total_elapsed_time: float | None,
    active_time: float | None,
    prep_time: float | None,
    cook_time: float | None,
) -> bool:
    if not has_passive_keyword or total_elapsed_time is None:
        return False
    if total_elapsed_time < LONG_PASSIVE_THRESHOLD_MIN:
        return False
    if prep_time is None or cook_time is None:
        return True
    if active_time is None:
        return True
    return abs(active_time - total_elapsed_time) <= 5.0 or active_time >= LONG_PASSIVE_THRESHOLD_MIN


def _active_cap_for_long_passive(total_elapsed_time: float) -> float:
    if total_elapsed_time >= EXTREME_PASSIVE_THRESHOLD_MIN:
        return 75.0
    return 90.0


def _effective_time_for_scoring(
    active_time: float | None,
    passive_time: float | None,
    warnings: list[str],
    reasons: list[str],
) -> float | None:
    if active_time is None:
        return None
    passive = max(passive_time or 0.0, 0.0)
    if passive <= 0:
        return round(active_time, 1)
    passive_contribution = min(PASSIVE_SCORING_CAP_MIN, passive * PASSIVE_SCORING_FRACTION)
    if passive_contribution > 0:
        warnings.append("effective_time_includes_capped_passive")
        reasons.append("effective_time_active_plus_capped_passive")
    return round(active_time + passive_contribution, 1)


def _cap_slot_effective_time(
    *,
    effective_time: float | None,
    recipe_kind: str,
    allowed_slots: list[str],
    warnings: list[str],
    reasons: list[str],
    config: dict[str, Any],
) -> float | None:
    if effective_time is None:
        return None
    cap_snacks = bool(config.get("cap_snack_effective_time", True))
    cap_breakfast = bool(config.get("cap_breakfast_effective_time", True))
    if cap_snacks and _is_snack(recipe_kind, allowed_slots) and effective_time > SNACK_EFFECTIVE_CAP_MIN:
        warnings.append("snack_effective_time_capped")
        reasons.append("snack_effective_cap_for_scoring")
        return SNACK_EFFECTIVE_CAP_MIN
    if (
        cap_breakfast
        and _is_breakfast(recipe_kind, allowed_slots)
        and effective_time > BREAKFAST_EFFECTIVE_CAP_MIN
    ):
        warnings.append("breakfast_effective_time_capped")
        reasons.append("breakfast_effective_cap_for_scoring")
        return BREAKFAST_EFFECTIVE_CAP_MIN
    return round(effective_time, 1)


def _fallback_time_by_recipe_kind(
    recipe_kind: str,
    allowed_slots: list[str],
    recipe_row: Any,
) -> dict[str, Any]:
    text = _combined_text(recipe_row, None)
    warnings = ["fallback_by_recipe_kind_low_confidence"]
    reasons = ["missing_source_time_fields"]
    if _is_snack(recipe_kind, allowed_slots):
        active = 10.0
    elif _is_breakfast(recipe_kind, allowed_slots):
        active = 20.0
    elif any(_keyword_pattern(keyword).search(text) for keyword in SOUP_STEW_KEYWORDS):
        active = 60.0
        warnings.append("fallback_soup_stew_slow_method")
    else:
        active = 40.0
    return {
        "active_time_estimated_min": active,
        "passive_time_estimated_min": 0.0,
        "total_elapsed_time_min": active,
        "effective_time_min_for_scoring": active,
        "warnings": warnings,
        "reasons": reasons,
    }


def _existing_time_is_reliable(
    *,
    existing_active: float | None,
    existing_passive: float | None,
    existing_effective: float | None,
    total_time: float | None,
    has_passive_keyword: bool,
) -> bool:
    if existing_active is None or existing_effective is None:
        return False
    passive = existing_passive or 0.0
    if existing_active <= 0 or existing_effective <= 0:
        return False
    if passive <= 0 and not (has_passive_keyword and (total_time or 0.0) >= LONG_PASSIVE_THRESHOLD_MIN):
        return True
    if passive > 0 and existing_effective > existing_active:
        return True
    return False


def _allowed_slots(recipe_row: Any) -> list[str]:
    raw_value = _get_value(recipe_row, "allowed_slots_json")
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [_clean_text(item) for item in raw_value if _clean_text(item)]
    text = str(raw_value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = [part.strip() for part in re.split(r"[,;|]", text)]
    if isinstance(parsed, list):
        return [_clean_text(item) for item in parsed if _clean_text(item)]
    return []


def _is_snack(recipe_kind: str, allowed_slots: list[str]) -> bool:
    return "snack" in recipe_kind or allowed_slots == ["snack"]


def _is_breakfast(recipe_kind: str, allowed_slots: list[str]) -> bool:
    return "breakfast" in recipe_kind or allowed_slots == ["breakfast"]


def _matched_keywords(recipe_row: Any, directions_text: str | None) -> list[str]:
    text = _combined_text(recipe_row, directions_text)
    matched = [keyword for keyword in PASSIVE_KEYWORDS if _keyword_pattern(keyword).search(text)]
    long_context_matched = []
    if re.search(r"\b(bake|roast|simmer)\b.{0,40}\b([2-9]\s*hours?|[1-9][0-9]{2}\s*minutes?)\b", text):
        long_context_matched.append("long_heat_context")
    return _unique_strings([*matched, *long_context_matched])


def _combined_text(recipe_row: Any, directions_text: str | None) -> str:
    parts = [
        _get_value(recipe_row, "display_name"),
        _get_value(recipe_row, "recipe_name"),
        _get_value(recipe_row, "recipe_kind"),
        _get_value(recipe_row, "recipe_category"),
        directions_text,
        _get_value(recipe_row, "directions_json"),
        _get_value(recipe_row, "qc_notes"),
    ]
    return _normalize_text(" ".join(str(part or "") for part in parts))


def _get_value(recipe_row: Any, key: str) -> object:
    if hasattr(recipe_row, "get"):
        return recipe_row.get(key)
    return getattr(recipe_row, key, None)


def _first_numeric(*values: object) -> float | None:
    for value in values:
        numeric_value = _to_non_negative_float(value)
        if numeric_value is not None:
            return numeric_value
    return None


def _to_non_negative_float(value: object) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value):
        return None
    return max(numeric_value, 0.0)


def _round_optional(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 1)


def _clean_choice(*values: object) -> str:
    allowed = {"high", "medium", "low", "unknown"}
    for value in values:
        text = _clean_text(value)
        if text in allowed:
            return text
    return ""


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    if text in {"", "nan", "none", "null"}:
        return ""
    return text


def _normalize_text(value: str) -> str:
    text = value.lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _keyword_pattern(keyword: str) -> re.Pattern[str]:
    normalized = _normalize_text(keyword)
    return re.compile(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])")


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result
