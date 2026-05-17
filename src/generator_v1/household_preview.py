from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from src.generator_v1.target_builder import build_nutrition_target


DEFAULT_HOUSEHOLD_PROFILE_PATH = Path("profiles/household_profile_demo_v1.json")
DEFAULT_ALLOCATION_MODE = "macro_aware_simple"
DEFAULT_SHARED_SLOTS = ("lunch", "dinner")
MACRO_FIELDS = ("kcal", "protein_g", "carbs_g", "fat_g")
PORTION_MIN_DEFAULT = 0.4
PORTION_MAX_DEFAULT = 1.8


def load_household_profile(path: str | Path = DEFAULT_HOUSEHOLD_PROFILE_PATH) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_household_member_targets(household_profile: dict[str, Any]) -> dict[str, Any]:
    members = _active_members(household_profile)
    targets_by_member_id: dict[str, dict[str, Any]] = {}
    target_rows: list[dict[str, Any]] = []
    slot_target_rows: list[dict[str, Any]] = []
    for member in members:
        member_id = str(member.get("member_id") or "")
        target = build_nutrition_target(member)
        target_dict = _target_to_dict(target)
        targets_by_member_id[member_id] = target_dict
        target_rows.append(
            {
                "member_id": member_id,
                "member": member.get("display_name"),
                "kcal_target": target.kcal,
                "protein_g_target": target.protein_g,
                "carbs_g_target": target.carbs_g,
                "fat_g_target": target.fat_g,
                "goal": member.get("goal"),
                "goal_speed": member.get("goal_speed"),
                "activity_level": member.get("activity_level"),
            }
        )
        for slot, slot_target in target.slot_targets.items():
            slot_target_rows.append(
                {
                    "member_id": member_id,
                    "member": member.get("display_name"),
                    "slot": slot,
                    "slot_kcal_target": slot_target.get("kcal"),
                    "slot_protein_g_target": slot_target.get("protein_g"),
                    "slot_carbs_g_target": slot_target.get("carbs_g"),
                    "slot_fat_g_target": slot_target.get("fat_g"),
                }
            )

    household_totals = {
        field: round(
            sum(_to_float(targets_by_member_id[member_id].get(field)) or 0.0 for member_id in targets_by_member_id),
            1,
        )
        for field in MACRO_FIELDS
    }
    planning_config = household_profile.get("planning_config") or {}
    return {
        "household_id": household_profile.get("household_id"),
        "household_name": household_profile.get("household_name"),
        "members": members,
        "targets_by_member_id": targets_by_member_id,
        "target_rows": target_rows,
        "slot_target_rows": slot_target_rows,
        "household_totals": household_totals,
        "shared_slots": list(planning_config.get("shared_meals") or DEFAULT_SHARED_SLOTS),
        "portion_multiplier_min": _to_float(planning_config.get("portion_multiplier_min"))
        or PORTION_MIN_DEFAULT,
        "portion_multiplier_max": _to_float(planning_config.get("portion_multiplier_max"))
        or PORTION_MAX_DEFAULT,
        "breakfast_mode": planning_config.get("breakfast_mode", "flexible"),
        "snack_mode": planning_config.get("snack_mode", "individual"),
    }


def allocate_shared_meal_portions(
    plan: dict[str, Any],
    member_targets: dict[str, Any],
    mode: str = DEFAULT_ALLOCATION_MODE,
) -> list[dict[str, Any]]:
    members = list(member_targets.get("members") or [])
    targets_by_member_id = dict(member_targets.get("targets_by_member_id") or {})
    if not members or not targets_by_member_id:
        return []
    shared_slots = {str(slot) for slot in member_targets.get("shared_slots", DEFAULT_SHARED_SLOTS)}
    min_multiplier = float(member_targets.get("portion_multiplier_min") or PORTION_MIN_DEFAULT)
    max_multiplier = float(member_targets.get("portion_multiplier_max") or PORTION_MAX_DEFAULT)
    primary_member_id = str(members[0].get("member_id") or "")
    primary_target = targets_by_member_id.get(primary_member_id) or {}
    rows: list[dict[str, Any]] = []

    for day in _normalized_plan_days(plan):
        day_index = int(day.get("day_index") or 1)
        for meal in day.get("selected_meals", []):
            slot = str(meal.get("slot") or "")
            if slot not in shared_slots:
                continue
            meal_rows = []
            for member in members:
                member_id = str(member.get("member_id") or "")
                member_target = targets_by_member_id.get(member_id) or {}
                row = _allocate_member_portion(
                    mode=mode,
                    member=member,
                    member_target=member_target,
                    primary_target=primary_target,
                    meal=meal,
                    min_multiplier=min_multiplier,
                    max_multiplier=max_multiplier,
                )
                row.update(
                    {
                        "day_index": day_index,
                        "slot": slot,
                        "recipe_id": meal.get("recipe_id"),
                        "recipe": meal.get("display_name") or meal.get("recipe_name"),
                        "allocation_mode": mode,
                        "base_portion_multiplier": _to_float(
                            meal.get("portion_multiplier")
                        )
                        or 1.0,
                    }
                )
                meal_rows.append(row)
            portion_sum = sum(
                _to_float(row.get("portion_multiplier_member")) or 0.0
                for row in meal_rows
            )
            grams_sum = sum(
                _to_float(row.get("grams_estimated")) or 0.0
                for row in meal_rows
            )
            meal_has_review = any(
                _clean_text(row.get("household_portion_fit_warning"))
                for row in meal_rows
            )
            for row in meal_rows:
                row["member_portion_sum_for_meal"] = round(portion_sum, 3)
                row["household_total_grams_estimated"] = round(grams_sum, 1)
                row["household_portion_fit_review"] = meal_has_review
                rows.append(row)
    return rows


def summarize_household_plan(
    plan: dict[str, Any],
    allocations: list[dict[str, Any]],
    member_targets: dict[str, Any] | None = None,
) -> dict[str, Any]:
    days = _normalized_plan_days(plan)
    member_daily_rows: list[dict[str, Any]] = []
    day_summary_rows: list[dict[str, Any]] = []
    warning_counts = _warning_counts(allocations)
    min_multiplier = _min_numeric(allocations, "portion_multiplier_member")
    max_multiplier = _max_numeric(allocations, "portion_multiplier_member")
    review_meals = {
        (
            int(row.get("day_index") or 0),
            str(row.get("slot") or ""),
            str(row.get("recipe_id") or ""),
        )
        for row in allocations
        if _clean_text(row.get("household_portion_fit_warning"))
    }

    if member_targets is not None:
        member_daily_rows = _member_daily_macro_rows(days, allocations, member_targets)
        day_summary_rows = _day_summary_rows(member_daily_rows)

    return {
        "summary": {
            "days_generated": len(days),
            "member_count": len(member_targets.get("members", [])) if member_targets else 0,
            "shared_meal_count": len(
                {
                    (
                        int(row.get("day_index") or 0),
                        str(row.get("slot") or ""),
                        str(row.get("recipe_id") or ""),
                    )
                    for row in allocations
                }
            ),
            "allocation_count": len(allocations),
            "allocation_mode": allocations[0].get("allocation_mode") if allocations else "",
            "min_portion_multiplier": min_multiplier,
            "max_portion_multiplier": max_multiplier,
            "review_meal_count": len(review_meals),
            "warning_counts": warning_counts,
        },
        "member_daily_rows": member_daily_rows,
        "day_summary_rows": day_summary_rows,
        "warnings": [
            "not_household_native_selection",
            "breakfast_snack_assumed_individual_target_fill",
            "shared_meal_feasibility_is_audit_level",
            "grocery_scaling_quantity_only",
        ],
    }


def compute_household_grocery_scaling(
    plan: dict[str, Any],
    allocations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    allocations_by_meal: dict[tuple[int, str, str], list[dict[str, Any]]] = {}
    for row in allocations:
        key = (
            int(row.get("day_index") or 0),
            str(row.get("slot") or ""),
            str(row.get("recipe_id") or ""),
        )
        allocations_by_meal.setdefault(key, []).append(row)

    rows: list[dict[str, Any]] = []
    for day in _normalized_plan_days(plan):
        day_index = int(day.get("day_index") or 1)
        for meal in day.get("selected_meals", []):
            slot = str(meal.get("slot") or "")
            recipe_id = str(meal.get("recipe_id") or "")
            key = (day_index, slot, recipe_id)
            member_rows = allocations_by_meal.get(key, [])
            if not member_rows:
                continue
            member_portion_sum = sum(
                _to_float(row.get("portion_multiplier_member")) or 0.0
                for row in member_rows
            )
            single_profile_portion = _to_float(meal.get("portion_multiplier")) or 1.0
            factor = (
                member_portion_sum / single_profile_portion
                if single_profile_portion
                else None
            )
            warning = "household_quantity_factor_large" if factor is not None and factor > 4.0 else ""
            rows.append(
                {
                    "day_index": day_index,
                    "slot": slot,
                    "recipe_id": recipe_id,
                    "recipe": meal.get("display_name") or meal.get("recipe_name"),
                    "member_portion_sum": round(member_portion_sum, 3),
                    "single_profile_portion": round(single_profile_portion, 3),
                    "household_quantity_factor": _round_optional(factor, 3),
                    "warning": warning,
                }
            )
    return rows


def build_household_preview(
    plan: dict[str, Any],
    household_profile: dict[str, Any],
    allocation_mode: str = DEFAULT_ALLOCATION_MODE,
) -> dict[str, Any]:
    member_targets = build_household_member_targets(household_profile)
    allocations = allocate_shared_meal_portions(
        plan,
        member_targets,
        mode=allocation_mode,
    )
    summary = summarize_household_plan(plan, allocations, member_targets)
    grocery_scaling = compute_household_grocery_scaling(plan, allocations)
    summary["summary"]["max_grocery_scaling_factor"] = _max_numeric(
        grocery_scaling,
        "household_quantity_factor",
    )
    return {
        "household_profile": household_profile,
        "member_targets": member_targets,
        "allocations": allocations,
        "member_daily_rows": summary["member_daily_rows"],
        "day_summary_rows": summary["day_summary_rows"],
        "grocery_scaling": grocery_scaling,
        "summary": summary["summary"],
        "warnings": summary["warnings"],
    }


def household_preview_readable_lines(preview: dict[str, Any]) -> list[str]:
    member_targets = preview.get("member_targets", {})
    summary = preview.get("summary", {})
    lines = [
        "Household Preview",
        "",
        "Members",
    ]
    for row in member_targets.get("target_rows", []):
        lines.append(
            "- "
            f"{row.get('member')}: "
            f"{_format_number(row.get('kcal_target'))} kcal, "
            f"P {_format_number(row.get('protein_g_target'))}g, "
            f"C {_format_number(row.get('carbs_g_target'))}g, "
            f"F {_format_number(row.get('fat_g_target'))}g"
        )

    lines.extend(["", "Shared meals"])
    grouped_meals: dict[tuple[int, str, str], list[dict[str, Any]]] = {}
    for row in preview.get("allocations", []):
        key = (
            int(row.get("day_index") or 0),
            str(row.get("slot") or ""),
            str(row.get("recipe") or ""),
        )
        grouped_meals.setdefault(key, []).append(row)
    for (day_index, slot, recipe), rows in sorted(grouped_meals.items()):
        portions = ", ".join(
            f"{row.get('member')}: {row.get('portion_multiplier_member')}x"
            for row in rows
        )
        lines.append(f"- Day {day_index} {slot}: {recipe} | {portions}")

    lines.extend(["", "Daily macro summary"])
    for row in preview.get("member_daily_rows", []):
        lines.append(
            "- "
            f"Day {row.get('day_index')} {row.get('member')}: "
            f"{_format_number(row.get('kcal'))} kcal "
            f"({_format_signed(row.get('kcal_deviation_pct'))}%), "
            f"protein {_format_number(row.get('protein_g'))}g "
            f"({_format_signed(row.get('protein_deviation_pct'))}%)"
        )

    lines.extend(["", "Grocery scaling"])
    for row in preview.get("grocery_scaling", []):
        lines.append(
            "- "
            f"Day {row.get('day_index')} {row.get('slot')}: "
            f"{row.get('recipe')} | factor "
            f"{_format_number(row.get('household_quantity_factor'))}x"
        )

    lines.extend(
        [
            "",
            "Diagnostics",
            f"- allocation_mode={summary.get('allocation_mode')}",
            f"- max_grocery_scaling_factor={summary.get('max_grocery_scaling_factor')}",
            "- not household-native selection; preview only",
        ]
    )
    return lines


def _member_daily_macro_rows(
    days: list[dict[str, Any]],
    allocations: list[dict[str, Any]],
    member_targets: dict[str, Any],
) -> list[dict[str, Any]]:
    by_key = {
        (
            int(row.get("day_index") or 0),
            str(row.get("slot") or ""),
            str(row.get("member_id") or ""),
        ): row
        for row in allocations
    }
    rows: list[dict[str, Any]] = []
    targets_by_member_id = member_targets.get("targets_by_member_id", {})
    shared_slots = {str(slot) for slot in member_targets.get("shared_slots", DEFAULT_SHARED_SLOTS)}
    for day in days:
        day_index = int(day.get("day_index") or 1)
        for member in member_targets.get("members", []):
            member_id = str(member.get("member_id") or "")
            target = targets_by_member_id.get(member_id) or {}
            slot_targets = target.get("slot_targets") or {}
            totals = {field: 0.0 for field in MACRO_FIELDS}
            shared_slot_count = 0
            for slot, slot_target in slot_targets.items():
                if str(slot) in shared_slots:
                    allocated = by_key.get((day_index, str(slot), member_id))
                    if allocated:
                        shared_slot_count += 1
                        for field in MACRO_FIELDS:
                            totals[field] += _to_float(allocated.get(field)) or 0.0
                        continue
                for field in MACRO_FIELDS:
                    totals[field] += _to_float(slot_target.get(field)) or 0.0
            rows.append(
                {
                    "day_index": day_index,
                    "member_id": member_id,
                    "member": member.get("display_name"),
                    "shared_slot_count": shared_slot_count,
                    "kcal": round(totals["kcal"], 1),
                    "protein_g": round(totals["protein_g"], 1),
                    "carbs_g": round(totals["carbs_g"], 1),
                    "fat_g": round(totals["fat_g"], 1),
                    "target_kcal": target.get("kcal"),
                    "target_protein_g": target.get("protein_g"),
                    "target_carbs_g": target.get("carbs_g"),
                    "target_fat_g": target.get("fat_g"),
                    "kcal_deviation_pct": _percent(
                        _deviation_ratio(totals["kcal"], target.get("kcal"))
                    ),
                    "protein_deviation_pct": _percent(
                        _deviation_ratio(totals["protein_g"], target.get("protein_g"))
                    ),
                    "carbs_deviation_pct": _percent(
                        _deviation_ratio(totals["carbs_g"], target.get("carbs_g"))
                    ),
                    "fat_deviation_pct": _percent(
                        _deviation_ratio(totals["fat_g"], target.get("fat_g"))
                    ),
                }
            )
    return rows


def _day_summary_rows(member_daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[int, list[dict[str, Any]]] = {}
    for row in member_daily_rows:
        by_day.setdefault(int(row.get("day_index") or 0), []).append(row)
    rows = []
    for day_index, day_rows in sorted(by_day.items()):
        rows.append(
            {
                "day_index": day_index,
                "member_count": len(day_rows),
                "mean_abs_kcal_deviation_pct": _mean_abs(day_rows, "kcal_deviation_pct"),
                "max_abs_kcal_deviation_pct": _max_abs(day_rows, "kcal_deviation_pct"),
                "mean_abs_protein_deviation_pct": _mean_abs(day_rows, "protein_deviation_pct"),
                "max_abs_protein_deviation_pct": _max_abs(day_rows, "protein_deviation_pct"),
            }
        )
    return rows


def _allocate_member_portion(
    *,
    mode: str,
    member: dict[str, Any],
    member_target: dict[str, Any],
    primary_target: dict[str, Any],
    meal: dict[str, Any],
    min_multiplier: float,
    max_multiplier: float,
) -> dict[str, Any]:
    slot = str(meal.get("slot") or "")
    base_multiplier = _to_float(meal.get("portion_multiplier")) or 1.0
    selected_macros = {field: _to_float(meal.get(field)) or 0.0 for field in MACRO_FIELDS}
    per_one_portion = {
        field: value / base_multiplier if base_multiplier else value
        for field, value in selected_macros.items()
    }
    raw_multiplier = _raw_member_multiplier(
        mode=mode,
        member_target=member_target,
        primary_target=primary_target,
        slot=slot,
        base_multiplier=base_multiplier,
        per_one_portion=per_one_portion,
    )
    multiplier = _clamp(raw_multiplier, min_multiplier, max_multiplier)
    actual = {field: round(per_one_portion[field] * multiplier, 1) for field in MACRO_FIELDS}
    slot_target = (member_target.get("slot_targets") or {}).get(slot, {})
    warning_codes = []
    if not _approximately_equal(raw_multiplier, multiplier):
        warning_codes.append("portion_multiplier_clamped")
    if abs(_deviation_ratio(actual["kcal"], slot_target.get("kcal"))) > 0.25:
        warning_codes.append("kcal_slot_deviation_review")
    if abs(_deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))) > 0.35:
        warning_codes.append("protein_slot_deviation_review")

    per_one_grams = _meal_grams_per_one_portion(meal, base_multiplier)
    grams_estimated = per_one_grams * multiplier if per_one_grams is not None else None
    return {
        "member_id": member.get("member_id"),
        "member": member.get("display_name"),
        "portion_multiplier_raw": round(raw_multiplier, 3),
        "portion_multiplier_member": round(multiplier, 3),
        "portion_multiplier_clamped": not _approximately_equal(raw_multiplier, multiplier),
        "grams_estimated": _round_optional(grams_estimated, 1),
        "kcal": actual["kcal"],
        "protein_g": actual["protein_g"],
        "carbs_g": actual["carbs_g"],
        "fat_g": actual["fat_g"],
        "slot_kcal_target": slot_target.get("kcal"),
        "slot_protein_g_target": slot_target.get("protein_g"),
        "slot_carbs_g_target": slot_target.get("carbs_g"),
        "slot_fat_g_target": slot_target.get("fat_g"),
        "kcal_deviation_pct": _percent(_deviation_ratio(actual["kcal"], slot_target.get("kcal"))),
        "protein_deviation_pct": _percent(
            _deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))
        ),
        "carbs_deviation_pct": _percent(
            _deviation_ratio(actual["carbs_g"], slot_target.get("carbs_g"))
        ),
        "fat_deviation_pct": _percent(
            _deviation_ratio(actual["fat_g"], slot_target.get("fat_g"))
        ),
        "household_portion_fit_warning": ";".join(warning_codes),
    }


def _raw_member_multiplier(
    *,
    mode: str,
    member_target: dict[str, Any],
    primary_target: dict[str, Any],
    slot: str,
    base_multiplier: float,
    per_one_portion: dict[str, float],
) -> float:
    if mode == "equal_portions":
        return base_multiplier
    if mode == "proportional_to_daily_kcal":
        return base_multiplier * _ratio(member_target.get("kcal"), primary_target.get("kcal"))
    if mode == "proportional_to_slot_kcal":
        return base_multiplier * _ratio(
            (member_target.get("slot_targets") or {}).get(slot, {}).get("kcal"),
            (primary_target.get("slot_targets") or {}).get(slot, {}).get("kcal"),
        )
    if mode == "macro_aware_simple":
        return _macro_aware_multiplier(
            per_one_portion=per_one_portion,
            slot_target=(member_target.get("slot_targets") or {}).get(slot, {}),
        )
    raise ValueError(f"Mod household allocation necunoscut: {mode}")


def _macro_aware_multiplier(
    *,
    per_one_portion: dict[str, float],
    slot_target: dict[str, Any],
) -> float:
    best_multiplier = PORTION_MIN_DEFAULT
    best_loss = float("inf")
    step_count = int(round((PORTION_MAX_DEFAULT - PORTION_MIN_DEFAULT) / 0.05))
    for index in range(step_count + 1):
        multiplier = round(PORTION_MIN_DEFAULT + index * 0.05, 2)
        actual = {field: per_one_portion[field] * multiplier for field in MACRO_FIELDS}
        loss = (
            abs(_deviation_ratio(actual["kcal"], slot_target.get("kcal"))) * 0.55
            + abs(_deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))) * 0.25
            + abs(_deviation_ratio(actual["carbs_g"], slot_target.get("carbs_g"))) * 0.10
            + abs(_deviation_ratio(actual["fat_g"], slot_target.get("fat_g"))) * 0.10
        )
        if loss < best_loss:
            best_loss = loss
            best_multiplier = multiplier
    return best_multiplier


def _normalized_plan_days(plan: dict[str, Any]) -> list[dict[str, Any]]:
    days = plan.get("days")
    if isinstance(days, list) and days:
        return [day for day in days if isinstance(day, dict)]
    selected_meals = plan.get("selected_meals")
    if isinstance(selected_meals, list) and selected_meals:
        return [
            {
                "day_index": 1,
                "selected_meals": selected_meals,
                "day_totals": plan.get("day_totals", {}),
            }
        ]
    return []


def _active_members(household_profile: dict[str, Any]) -> list[dict[str, Any]]:
    active_ids = {
        str(member_id).strip()
        for member_id in household_profile.get("active_member_ids", [])
        if str(member_id).strip()
    }
    return [
        member
        for member in household_profile.get("members", [])
        if str(member.get("member_id") or "").strip() in active_ids
    ]


def _target_to_dict(target: Any) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def _meal_grams_per_one_portion(meal: dict[str, Any], base_multiplier: float) -> float | None:
    portion_grams = _to_float(meal.get("portion_grams_estimated"))
    if portion_grams is not None and portion_grams > 0 and base_multiplier > 0:
        return portion_grams / base_multiplier
    serving_grams = _to_float(meal.get("serving_weight_g_estimated"))
    if serving_grams is not None and serving_grams > 0:
        return serving_grams
    overlay_grams = _to_float(meal.get("overlay_serving_weight_g_estimated"))
    if overlay_grams is not None and overlay_grams > 0:
        return overlay_grams
    return None


def _warning_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for item in str(row.get("household_portion_fit_warning") or "").split(";"):
            code = item.strip()
            if not code:
                continue
            counts[code] = counts.get(code, 0) + 1
    return counts


def _ratio(left: Any, right: Any) -> float:
    left_value = _to_float(left)
    right_value = _to_float(right)
    if left_value is None or right_value in (None, 0.0):
        return 0.0
    return left_value / right_value


def _deviation_ratio(actual: Any, target: Any) -> float:
    actual_value = _to_float(actual)
    target_value = _to_float(target)
    if actual_value is None or target_value in (None, 0.0):
        return 0.0
    return (actual_value - target_value) / target_value


def _percent(value: float) -> float:
    return round(value * 100.0, 1)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return min(max(value, minimum), maximum)


def _approximately_equal(left: float, right: float) -> bool:
    return abs(left - right) < 0.0001


def _mean_abs(rows: list[dict[str, Any]], field: str) -> float:
    values = [abs(_to_float(row.get(field)) or 0.0) for row in rows]
    return round(sum(values) / len(values), 1) if values else 0.0


def _max_abs(rows: list[dict[str, Any]], field: str) -> float:
    values = [abs(_to_float(row.get(field)) or 0.0) for row in rows]
    return round(max(values), 1) if values else 0.0


def _min_numeric(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [_to_float(row.get(field)) for row in rows]
    clean_values = [value for value in values if value is not None]
    return round(min(clean_values), 3) if clean_values else None


def _max_numeric(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [_to_float(row.get(field)) for row in rows]
    clean_values = [value for value in values if value is not None]
    return round(max(clean_values), 3) if clean_values else None


def _round_optional(value: Any, digits: int) -> float | None:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return round(numeric, digits)


def _to_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def _format_number(value: Any) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return "n/a"
    if abs(numeric - round(numeric)) < 0.05:
        return str(int(round(numeric)))
    return f"{numeric:.1f}"


def _format_signed(value: Any) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return "n/a"
    sign = "+" if numeric > 0 else ""
    return f"{sign}{numeric:.1f}"
