from __future__ import annotations

from collections.abc import Mapping
from typing import Any


STATUS_NORMAL = "normal_demo_safe"
STATUS_EDGE = "edge_needs_warning"
STATUS_UNSUPPORTED = "unsupported_for_demo"

MODE_DEMO = "demo"
MODE_PERMISSIVE = "permissive"

_STATUS_RANK = {
    STATUS_NORMAL: 0,
    STATUS_EDGE: 1,
    STATUS_UNSUPPORTED: 2,
}


def evaluate_profile_guard(
    profile: Mapping[str, Any],
    target: Any,
    meal_config: Mapping[str, Any] | None = None,
    mode: str = MODE_DEMO,
) -> dict[str, Any]:
    normalized_mode = str(mode or MODE_DEMO).strip().lower()
    if normalized_mode not in {MODE_DEMO, MODE_PERMISSIVE}:
        raise ValueError(f"Mod profile guard necunoscut: {mode!r}")

    resolved_meal_config = meal_config or _mapping_value(profile, "meal_config") or {}
    target_kcal = _target_kcal(target)
    include_snack = _include_snack(resolved_meal_config)
    meals_per_day = _meals_per_day(resolved_meal_config)
    goal = _clean_text(_mapping_value(profile, "goal")).lower()
    goal_speed = _clean_text(_mapping_value(profile, "goal_speed")).lower()
    activity_level = _clean_text(_mapping_value(profile, "activity_level")).lower()

    status = STATUS_NORMAL
    reasons: list[str] = []
    recommendations: list[str] = []
    suggested_adjustments: dict[str, Any] = {}

    if target_kcal is not None and target_kcal < 1300:
        status = _max_status(status, STATUS_UNSUPPORTED)
        reasons.append("target_kcal_below_1300")
        recommendations.append(
            "Use aggressive-cut test mode explicitly, or adjust goal speed, activity, or snack count."
        )
        suggested_adjustments.update(
            {
                "use_aggressive_cut_test_mode": True,
                "goal_speed": "normal",
                "activity_level": "lightly_active",
            }
        )

    if target_kcal is not None and target_kcal < 1400 and include_snack:
        status = _max_status(status, STATUS_EDGE)
        reasons.append("target_kcal_below_1400_with_snack")
        recommendations.append("Use 3 meals without snack for this demo profile.")
        suggested_adjustments["remove_snack"] = {
            "meals_per_day": 3,
            "include_snacks": False,
            "day_structure": "3_meals",
        }

    if goal == "lose" and goal_speed == "fast" and activity_level == "sedentary":
        status = _max_status(status, STATUS_EDGE)
        reasons.append("sedentary_fast_loss")
        recommendations.append("Avoid sedentary + fast loss in normal demo flow.")
        suggested_adjustments.setdefault("goal_speed", "normal")
        suggested_adjustments.setdefault("activity_level", "lightly_active")

    if (
        target_kcal is not None
        and target_kcal < 1500
        and (include_snack or meals_per_day >= 4)
    ):
        recommendations.append("Reduce snack/eating events for very low kcal targets.")
        suggested_adjustments.setdefault(
            "remove_snack",
            {
                "meals_per_day": 3,
                "include_snacks": False,
                "day_structure": "3_meals",
            },
        )

    reasons = _dedupe(reasons)
    recommendations = _dedupe(recommendations)
    should_block = status == STATUS_UNSUPPORTED and normalized_mode == MODE_DEMO

    return {
        "profile_guard_status": status,
        "profile_guard_reasons": reasons,
        "profile_guard_recommendations": recommendations,
        "should_block_generation": should_block,
        "suggested_adjustments": suggested_adjustments,
    }


def _max_status(current: str, candidate: str) -> str:
    if _STATUS_RANK[candidate] > _STATUS_RANK[current]:
        return candidate
    return current


def _mapping_value(source: Mapping[str, Any], key: str) -> Any:
    if not isinstance(source, Mapping):
        return None
    return source.get(key)


def _target_kcal(target: Any) -> float | None:
    if isinstance(target, Mapping):
        return _to_float(target.get("kcal") or target.get("target_kcal"))
    return _to_float(getattr(target, "kcal", None))


def _include_snack(meal_config: Mapping[str, Any]) -> bool:
    raw_value = (
        meal_config.get("include_snacks")
        if "include_snacks" in meal_config
        else meal_config.get("include_snack")
    )
    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(raw_value)


def _meals_per_day(meal_config: Mapping[str, Any]) -> int:
    value = _to_float(meal_config.get("meals_per_day"))
    if value is None:
        return 0
    return int(value)


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
