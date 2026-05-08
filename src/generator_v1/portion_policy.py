from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass


STANDARD_PORTION_MULTIPLIERS = [0.8, 1.0, 1.2]
VALID_PORTION_POLICY_MODES = {"standard", "expanded_safe", "target_aware"}

_EXPANDED_MULTIPLIERS_BY_SLOT = {
    "breakfast": [0.8, 1.0, 1.2, 1.4],
    "lunch": [0.8, 1.0, 1.2, 1.4, 1.6],
    "dinner": [0.8, 1.0, 1.2, 1.4, 1.6],
    "snack": [0.75, 1.0, 1.25],
    "protein_component": [0.8, 1.0, 1.2],
    "carb_side": [0.8, 1.0, 1.2],
    "veg_side": [0.8, 1.0, 1.2],
    "component": [0.8, 1.0, 1.2],
}

_PORTION_GRAM_CAPS = {
    "breakfast": 500.0,
    "lunch": 850.0,
    "dinner": 850.0,
    "snack": 300.0,
    "protein_component": 350.0,
    "carb_side": 350.0,
    "veg_side": 350.0,
    "component": 350.0,
}


@dataclass(frozen=True)
class PortionPolicyDecision:
    mode: str
    multipliers: list[float]
    reasons: list[str]
    warnings_by_multiplier: dict[float, list[str]]


def get_portion_multipliers(
    slot: str,
    recipe_row_or_candidate: Mapping[str, object],
    slot_target: Mapping[str, object] | None = None,
    profile_context: Mapping[str, object] | None = None,
    mode: str = "standard",
) -> list[float]:
    return get_portion_policy_decision(
        slot=slot,
        recipe_row_or_candidate=recipe_row_or_candidate,
        slot_target=slot_target,
        profile_context=profile_context,
        mode=mode,
    ).multipliers


def get_portion_policy_decision(
    slot: str,
    recipe_row_or_candidate: Mapping[str, object],
    slot_target: Mapping[str, object] | None = None,
    profile_context: Mapping[str, object] | None = None,
    mode: str = "standard",
) -> PortionPolicyDecision:
    _ = profile_context
    slot_key = _slot_key(slot, recipe_row_or_candidate)
    resolved_mode = mode if mode in VALID_PORTION_POLICY_MODES else "standard"
    if resolved_mode == "standard":
        return PortionPolicyDecision(
            mode="standard",
            multipliers=STANDARD_PORTION_MULTIPLIERS.copy(),
            reasons=["standard_static_portions"],
            warnings_by_multiplier={},
        )

    base_kcal = _first_float(
        recipe_row_or_candidate,
        [
            "energy_kcal_per_serving",
            "original_energy_kcal_per_serving",
            "overlay_energy_kcal_per_serving",
            "kcal_per_serving",
        ],
    )
    base_weight = _first_float(
        recipe_row_or_candidate,
        [
            "serving_weight_g_estimated",
            "overlay_serving_weight_g_estimated",
            "portion_grams_estimated",
        ],
    )
    if base_kcal is None and base_weight is None:
        return PortionPolicyDecision(
            mode="standard",
            multipliers=STANDARD_PORTION_MULTIPLIERS.copy(),
            reasons=["fallback_standard_missing_portion_basis"],
            warnings_by_multiplier={},
        )

    reasons = [f"{resolved_mode}_slot_{slot_key}", "caps_applied"]
    multipliers = _expanded_multipliers(slot_key)
    if resolved_mode == "target_aware":
        target_kcal = _target_float(slot_target, "kcal")
        if slot_key in {"lunch", "dinner"} and target_kcal is not None and target_kcal >= 850:
            multipliers.append(1.8)
            reasons.append("target_aware_high_slot_kcal_added_1_8")

    target_kcal = _target_float(slot_target, "kcal")
    allowed: list[float] = []
    warnings_by_multiplier: dict[float, list[str]] = {}
    exclusion_reasons: list[str] = []

    for multiplier in _sorted_unique(multipliers):
        exclusion = _exclusion_reason(
            slot_key=slot_key,
            multiplier=multiplier,
            base_kcal=base_kcal,
            base_weight=base_weight,
            target_kcal=target_kcal,
        )
        if exclusion:
            exclusion_reasons.append(exclusion)
            continue
        allowed.append(multiplier)
        warnings = _warnings_for_multiplier(
            slot_key=slot_key,
            multiplier=multiplier,
            base_kcal=base_kcal,
            base_weight=base_weight,
            target_kcal=target_kcal,
        )
        if warnings:
            warnings_by_multiplier[_norm(multiplier)] = warnings

    if not allowed:
        return PortionPolicyDecision(
            mode="standard",
            multipliers=STANDARD_PORTION_MULTIPLIERS.copy(),
            reasons=["fallback_standard_after_policy_caps"],
            warnings_by_multiplier={},
        )

    reasons.extend(_distinct_limited(exclusion_reasons, limit=8))
    return PortionPolicyDecision(
        mode=resolved_mode,
        multipliers=allowed,
        reasons=reasons,
        warnings_by_multiplier=warnings_by_multiplier,
    )


def warnings_for_multiplier(
    decision: PortionPolicyDecision,
    multiplier: float,
) -> list[str]:
    return decision.warnings_by_multiplier.get(_norm(multiplier), [])


def _expanded_multipliers(slot_key: str) -> list[float]:
    return _EXPANDED_MULTIPLIERS_BY_SLOT.get(
        slot_key,
        STANDARD_PORTION_MULTIPLIERS.copy(),
    ).copy()


def _exclusion_reason(
    slot_key: str,
    multiplier: float,
    base_kcal: float | None,
    base_weight: float | None,
    target_kcal: float | None,
) -> str | None:
    if multiplier <= 0 or multiplier > 1.8:
        return f"excluded_{_text_multiplier(multiplier)}_outside_v1_cap"

    if _high_base_kcal_excludes(slot_key, base_kcal, multiplier):
        return f"excluded_{_text_multiplier(multiplier)}_recipe_already_high_kcal"

    gram_cap = _PORTION_GRAM_CAPS.get(slot_key)
    if gram_cap is not None and base_weight is not None and base_weight * multiplier > gram_cap:
        return f"excluded_{_text_multiplier(multiplier)}_portion_grams_over_cap"

    kcal_cap = _kcal_cap(slot_key, target_kcal)
    if kcal_cap is not None and base_kcal is not None and base_kcal * multiplier > kcal_cap:
        return f"excluded_{_text_multiplier(multiplier)}_kcal_over_cap"

    return None


def _warnings_for_multiplier(
    slot_key: str,
    multiplier: float,
    base_kcal: float | None,
    base_weight: float | None,
    target_kcal: float | None,
) -> list[str]:
    warnings: list[str] = []
    if multiplier >= 1.6:
        warnings.append("large_multiplier_review")

    gram_cap = _PORTION_GRAM_CAPS.get(slot_key)
    if gram_cap is not None and base_weight is not None and base_weight * multiplier >= gram_cap * 0.9:
        warnings.append("portion_near_cap_review")

    kcal_cap = _kcal_cap(slot_key, target_kcal)
    if kcal_cap is not None and base_kcal is not None and base_kcal * multiplier >= kcal_cap * 0.9:
        warnings.append("kcal_near_cap_review")

    return warnings


def _high_base_kcal_excludes(
    slot_key: str,
    base_kcal: float | None,
    multiplier: float,
) -> bool:
    if base_kcal is None:
        return False
    if slot_key == "breakfast" and base_kcal >= 600 and multiplier > 1.2:
        return True
    if slot_key in {"lunch", "dinner"} and base_kcal >= 850 and multiplier > 1.2:
        return True
    if slot_key == "snack" and base_kcal >= 320 and multiplier > 1.0:
        return True
    return False


def _kcal_cap(slot_key: str, target_kcal: float | None) -> float | None:
    if slot_key == "snack":
        return 400.0
    if slot_key == "breakfast":
        return 800.0
    if slot_key in {"lunch", "dinner"}:
        if target_kcal is not None and target_kcal >= 1000:
            return 1400.0
        return 1200.0
    return None


def _slot_key(slot: str, row: Mapping[str, object]) -> str:
    text = str(slot or "").strip().lower()
    if text:
        return text
    for field in ("recipe_kind", "recipe_category"):
        value = str(row.get(field) or "").strip().lower()
        if value:
            return value
    return "unknown"


def _target_float(slot_target: Mapping[str, object] | None, key: str) -> float | None:
    if not isinstance(slot_target, Mapping):
        return None
    return _to_float(slot_target.get(key))


def _first_float(row: Mapping[str, object], fields: list[str]) -> float | None:
    for field in fields:
        value = _to_float(row.get(field))
        if value is not None and value > 0:
            return value
    return None


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _sorted_unique(values: list[float]) -> list[float]:
    return sorted({_norm(value) for value in values})


def _norm(value: float) -> float:
    return round(float(value), 4)


def _text_multiplier(value: float) -> str:
    return str(_norm(value)).replace(".", "_")


def _distinct_limited(values: list[str], limit: int) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
        if len(result) >= limit:
            break
    return result
