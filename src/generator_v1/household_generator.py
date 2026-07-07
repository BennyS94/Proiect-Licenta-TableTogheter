from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from src.generator_v1.day_selector_balanced import select_one_day_plan_balanced
from src.generator_v1.household_preview import (
    DEFAULT_ALLOCATION_MODE,
    DEFAULT_HOUSEHOLD_PROFILE_PATH,
    build_household_member_targets,
    load_household_profile,
)
from src.generator_v1.household_ingredient_guard import (
    audit_household_ingredient_load,
    candidate_egg_load,
)
from src.generator_v1.household_pairing import (
    annotate_mixed_vegetarian_pairing,
    is_preferred_companion,
    preferred_companion_recipe_ids,
)
from src.generator_v1.macro_fit import macro_fit
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.plan_quality_gate import evaluate_plan_quality
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.score_preview import compute_score_preview
from src.generator_v1.target_builder import NutritionTarget


HOUSEHOLD_MODE_OFF = "off"
HOUSEHOLD_MODE_SHARED_ALL_SLOTS = "shared_all_slots"
HOUSEHOLD_MODE_SHARED_MAIN_MEALS = "shared_main_meals"
HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN = "individual_breakfast_shared_main"
HOUSEHOLD_MODES = (
    HOUSEHOLD_MODE_OFF,
    HOUSEHOLD_MODE_SHARED_ALL_SLOTS,
    HOUSEHOLD_MODE_SHARED_MAIN_MEALS,
    HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
)
HOUSEHOLD_ALLOCATION_MODES = (
    "proportional_to_slot_kcal",
    "macro_aware_simple",
)
MACRO_FIELDS = ("kcal", "protein_g", "carbs_g", "fat_g")
TIME_FIELDS = (
    "total_time_min",
    "total_elapsed_time_min",
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "effective_time_min_for_scoring",
    "time_confidence",
    "time_estimation_method",
    "time_warnings",
)
PORTION_MIN_DEFAULT = 0.4
PORTION_MAX_DEFAULT = 1.8
HOUSEHOLD_GROCERY_FACTOR_REVIEW_THRESHOLD = 5.0
HOUSEHOLD_MIN_CARBS_RATIO_REVIEW = 0.75
HOUSEHOLD_MAX_FAT_RATIO_REVIEW = 1.25


def build_member_targets(household_profile: dict[str, Any]) -> dict[str, Any]:
    return build_household_member_targets(household_profile)


def filter_household_profile_members(
    household_profile: Mapping[str, Any],
    selected_member_ids: Sequence[str],
) -> dict[str, Any]:
    selected_ids = [
        str(member_id).strip()
        for member_id in selected_member_ids
        if str(member_id).strip()
    ]
    selected_set = set(selected_ids)
    profile = json.loads(json.dumps(dict(household_profile), ensure_ascii=True))
    members = [
        dict(member)
        for member in profile.get("members", [])
        if str(member.get("member_id") or "").strip() in selected_set
    ]
    profile["active_member_ids"] = [
        str(member.get("member_id") or "").strip()
        for member in members
        if str(member.get("member_id") or "").strip()
    ]
    profile["members"] = members
    return profile


def build_household_aggregate_target(member_targets: dict[str, Any]) -> NutritionTarget:
    targets_by_member_id = member_targets.get("targets_by_member_id") or {}
    if not isinstance(targets_by_member_id, dict) or not targets_by_member_id:
        raise ValueError("Household profile nu are targeturi active.")

    slot_names: list[str] = []
    for target in targets_by_member_id.values():
        slot_targets = target.get("slot_targets") if isinstance(target, dict) else {}
        for slot in slot_targets or {}:
            if slot not in slot_names:
                slot_names.append(str(slot))

    slot_targets: dict[str, dict[str, float]] = {}
    for slot in slot_names:
        slot_targets[slot] = {
            field: round(
                sum(
                    _to_float(
                        ((target.get("slot_targets") or {}).get(slot) or {}).get(field)
                    )
                    or 0.0
                    for target in targets_by_member_id.values()
                    if isinstance(target, dict)
                ),
                1,
            )
            for field in MACRO_FIELDS
        }

    return NutritionTarget(
        kcal=round(
            sum(_to_float(target.get("kcal")) or 0.0 for target in targets_by_member_id.values()),
            1,
        ),
        protein_g=round(
            sum(
                _to_float(target.get("protein_g")) or 0.0
                for target in targets_by_member_id.values()
            ),
            1,
        ),
        carbs_g=round(
            sum(_to_float(target.get("carbs_g")) or 0.0 for target in targets_by_member_id.values()),
            1,
        ),
        fat_g=round(
            sum(_to_float(target.get("fat_g")) or 0.0 for target in targets_by_member_id.values()),
            1,
        ),
        slot_targets=slot_targets,
    )


def build_household_slot_candidates(
    slot_candidates_by_slot: pd.DataFrame | Mapping[str, pd.DataFrame],
    member_targets: dict[str, Any],
    config: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    resolved_config = _resolved_config(config)
    shared_slots = set(_shared_slots(member_targets, resolved_config))
    aggregate_target = build_household_aggregate_target(member_targets)
    candidates = _candidate_frame(slot_candidates_by_slot)
    if candidates.empty:
        return candidates.copy()
    if "household_generation_shared_slot" in candidates.columns:
        return candidates.copy()

    prepared = candidates.sort_values(
        ["slot", "recipe_id", "score_preview", "macro_fit", "portion_multiplier"],
        ascending=[True, True, False, False, True],
        kind="mergesort",
        na_position="last",
    ).copy()
    prepared = prepared.drop_duplicates(["slot", "recipe_id"], keep="first")

    rows: list[dict[str, Any]] = []
    for _, candidate in prepared.iterrows():
        row = candidate.to_dict()
        slot = str(row.get("slot") or "")
        if slot in shared_slots:
            household_row = _household_candidate_row(
                row=row,
                member_targets=member_targets,
                aggregate_slot_target=aggregate_target.slot_targets.get(slot, {}),
                config=resolved_config,
            )
            rows.append(household_row)
        else:
            rows.append(_non_shared_candidate_row(row, config=resolved_config))

    household_frame = pd.DataFrame(rows)
    return annotate_mixed_vegetarian_pairing(
        household_frame,
        member_targets=member_targets,
        config=resolved_config,
        shared_slots=list(shared_slots),
    )


def allocate_member_portions_for_candidate(
    candidate_row: Mapping[str, Any],
    member_slot_targets: dict[str, Any],
    config: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    resolved_config = _resolved_config(config)
    members = list(member_slot_targets.get("members") or [])
    targets_by_member_id = dict(member_slot_targets.get("targets_by_member_id") or {})
    min_multiplier = float(resolved_config["portion_multiplier_min"])
    max_multiplier = float(resolved_config["portion_multiplier_max"])
    mode = str(resolved_config["allocation_mode"])
    slot = str(candidate_row.get("slot") or "")
    per_one = _per_one_portion_macros(candidate_row)
    per_one_grams = _per_one_portion_grams(candidate_row)
    rows: list[dict[str, Any]] = []

    for member in members:
        member_id = str(member.get("member_id") or "")
        member_target = targets_by_member_id.get(member_id) or {}
        slot_target = (member_target.get("slot_targets") or {}).get(slot, {})
        raw_multiplier = _raw_member_multiplier(
            mode=mode,
            per_one_portion=per_one,
            slot_target=slot_target,
        )
        multiplier = _clamp(raw_multiplier, min_multiplier, max_multiplier)
        actual = {
            field: round(per_one[field] * multiplier, 1)
            for field in MACRO_FIELDS
        }
        warnings = _member_portion_warnings(
            raw_multiplier=raw_multiplier,
            multiplier=multiplier,
            actual=actual,
            slot_target=slot_target,
        )
        grams = per_one_grams * multiplier if per_one_grams is not None else None
        rows.append(
            {
                "member_id": member_id,
                "member": member.get("display_name"),
                "portion_multiplier_raw": round(raw_multiplier, 3),
                "portion_multiplier": round(multiplier, 3),
                "portion_multiplier_member": round(multiplier, 3),
                "portion_multiplier_clamped": not _approximately_equal(
                    raw_multiplier,
                    multiplier,
                ),
                "grams_estimated": _round_optional(grams, 1),
                "kcal": actual["kcal"],
                "protein_g": actual["protein_g"],
                "carbs_g": actual["carbs_g"],
                "fat_g": actual["fat_g"],
                "slot_kcal_target": slot_target.get("kcal"),
                "slot_protein_g_target": slot_target.get("protein_g"),
                "slot_carbs_g_target": slot_target.get("carbs_g"),
                "slot_fat_g_target": slot_target.get("fat_g"),
                "kcal_deviation_pct": _percent(
                    _deviation_ratio(actual["kcal"], slot_target.get("kcal"))
                ),
                "protein_deviation_pct": _percent(
                    _deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))
                ),
                "carbs_deviation_pct": _percent(
                    _deviation_ratio(actual["carbs_g"], slot_target.get("carbs_g"))
                ),
                "fat_deviation_pct": _percent(
                    _deviation_ratio(actual["fat_g"], slot_target.get("fat_g"))
                ),
                "household_portion_fit_warning": ";".join(warnings),
            }
        )
    return rows


def generate_household_plan(
    household_profile: dict[str, Any],
    *,
    slot_candidates: pd.DataFrame,
    individual_slot_candidates: pd.DataFrame | Mapping[str, pd.DataFrame] | None = None,
    days: int = 3,
    config: Mapping[str, Any] | None = None,
    profile: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_config = _resolved_config(config)
    day_count = max(1, min(5, int(days or 1)))
    member_targets = build_member_targets(household_profile)
    aggregate_target = build_household_aggregate_target(member_targets)
    household_candidates = build_household_slot_candidates(
        slot_candidates,
        member_targets,
        resolved_config,
    )
    slot_order = list(aggregate_target.slot_targets.keys())
    if day_count == 1:
        base_plan = select_one_day_plan_balanced(
            slot_candidates_by_slot=_slot_candidates_by_slot(household_candidates, slot_order),
            target=aggregate_target,
            slot_order=slot_order,
            config=_one_day_config(resolved_config),
        )
        base_plan["target"] = _target_to_dict(aggregate_target)
        base_plan["validation"] = validate_one_day_plan(base_plan, aggregate_target)
        base_plan["quality_gate"] = evaluate_plan_quality(
            base_plan,
            aggregate_target,
            config={"quality_gate": "demo_safe"},
        )
        days_payload = [
            {
                "day_index": 1,
                "selected_meals": base_plan.get("selected_meals", []),
                "day_totals": base_plan.get("day_totals", {}),
                "validation_status": base_plan["validation"].get("validation_status"),
                "quality_gate_status": base_plan["quality_gate"].get(
                    "quality_gate_status"
                ),
                "warnings": base_plan.get("warnings", []),
            }
        ]
        base_plan = {
            "days": days_payload,
            "multi_day_summary": {
                "requested_days": 1,
                "actual_days_generated": 1,
                "valid_day_count": 1 if base_plan["validation"].get("validation_status") == "valid" else 0,
                "accept_day_count": 1
                if base_plan["quality_gate"].get("quality_gate_status") == "accept"
                else 0,
                "review_day_count": 1
                if base_plan["quality_gate"].get("quality_gate_status") == "review"
                else 0,
            },
            "multi_day_validation": base_plan["validation"],
            "target": _target_to_dict(aggregate_target),
            "config": _serializable_config(resolved_config),
        }
    else:
        base_plan = generate_multi_day_plan(
            profile=profile or household_profile,
            target=aggregate_target,
            slot_candidates=household_candidates,
            days=day_count,
            config=_multi_day_config(resolved_config),
        )

    return _decorate_household_plan(
        base_plan=base_plan,
        household_profile=household_profile,
        member_targets=member_targets,
        aggregate_target=aggregate_target,
        household_candidates=household_candidates,
        individual_slot_candidates=individual_slot_candidates
        if individual_slot_candidates is not None
        else slot_candidates,
        config=resolved_config,
    )


def household_plan_readable_lines(plan: Mapping[str, Any]) -> list[str]:
    summary = plan.get("household_summary", {})
    member_targets = plan.get("member_targets", {})
    lines = [
        "Household Generation v1 Lite",
        "",
        f"household={plan.get('household_name', '')}",
        f"mode={plan.get('household_mode')}",
        f"allocation_mode={plan.get('household_allocation_mode')}",
        f"days_generated={summary.get('days_generated')}",
        f"household_quality_status={summary.get('household_quality_status')}",
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

    for day in plan.get("days", []):
        lines.extend(["", f"Day {day.get('day_index')}"])
        lines.append("Shared meals")
        shared_meals = [
            meal
            for meal in day.get("selected_meals", [])
            if bool(meal.get("household_generation_shared_slot", True))
        ]
        for meal in shared_meals:
            lines.append(
                "- "
                f"{meal.get('slot')}: {meal.get('display_name')} | "
                f"household_portion_sum={_format_number(meal.get('household_portion_sum'))}x | "
                f"grocery_factor={_format_number(meal.get('household_grocery_scaling_factor'))}x"
            )
            for member_row in meal.get("member_allocations", []):
                lines.append(
                    "  - "
                    f"{member_row.get('member')}: "
                    f"{_format_number(member_row.get('portion_multiplier_member'))}x, "
                    f"{_format_number(member_row.get('kcal'))} kcal, "
                    f"P {_format_number(member_row.get('protein_g'))}g"
                )
        individual_meals = [
            row
            for row in plan.get("individual_meals", [])
            if int(row.get("day_index") or 0) == int(day.get("day_index") or 0)
        ]
        if individual_meals:
            lines.append("Individual breakfast/snack")
            for row in individual_meals:
                lines.append(
                    "- "
                    f"{row.get('slot')} | {row.get('member')}: "
                    f"{row.get('display_name')} | "
                    f"{_format_number(row.get('portion_multiplier_member'))}x, "
                    f"{_format_number(row.get('kcal'))} kcal, "
                    f"P {_format_number(row.get('protein_g'))}g"
                )

    lines.extend(["", "Per-member daily macro totals"])
    for row in plan.get("member_daily_rows", []):
        lines.append(
            "- "
            f"Day {row.get('day_index')} {row.get('member')}: "
            f"{_format_number(row.get('kcal'))} kcal "
            f"({_format_signed(row.get('kcal_deviation_pct'))}%), "
            f"protein {_format_number(row.get('protein_g'))}g "
            f"({_format_signed(row.get('protein_deviation_pct'))}%), "
            f"protein_gap_after={_format_number(row.get('protein_gap_after_g'))}g"
        )

    correction_rows = list(plan.get("protein_correction_rows", []))
    if correction_rows:
        lines.extend(["", "Protein correction audit"])
        for row in correction_rows:
            lines.append(
                "- "
                f"Day {row.get('day_index')} {row.get('member')}: "
                f"before_gap={_format_number(row.get('protein_gap_before_g'))}g, "
                f"after_gap={_format_number(row.get('protein_gap_after_g'))}g, "
                f"meals={row.get('selected_correction_meals') or 'none'}"
            )

    lines.extend(["", "Household grocery scaling"])
    for row in plan.get("grocery_scaling", []):
        lines.append(
            "- "
            f"Day {row.get('day_index')} {row.get('slot')}: "
            f"{row.get('display_name')} | factor "
            f"{_format_number(row.get('household_quantity_factor'))}x"
        )

    warnings = list(plan.get("warnings", []))
    lines.extend(["", "Warnings"])
    lines.extend([f"- {warning}" for warning in warnings] or ["- none"])
    return lines


def write_household_plan_json(plan: Mapping[str, Any], path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(_json_safe(plan), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


def write_household_plan_readable(plan: Mapping[str, Any], path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(household_plan_readable_lines(plan)) + "\n", encoding="utf-8")


def write_household_allocations_csv(plan: Mapping[str, Any], path: str | Path) -> None:
    _write_rows(plan.get("allocations", []), path)


def write_household_member_macros_csv(plan: Mapping[str, Any], path: str | Path) -> None:
    _write_rows(plan.get("member_daily_rows", []), path)


def write_household_grocery_scaling_csv(plan: Mapping[str, Any], path: str | Path) -> None:
    _write_rows(plan.get("grocery_scaling", []), path)


def _household_candidate_row(
    *,
    row: dict[str, Any],
    member_targets: dict[str, Any],
    aggregate_slot_target: dict[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    allocations = allocate_member_portions_for_candidate(row, member_targets, config)
    household_portion_sum = sum(
        _to_float(item.get("portion_multiplier_member")) or 0.0
        for item in allocations
    )
    household_macros = {
        field: round(sum(_to_float(item.get(field)) or 0.0 for item in allocations), 1)
        for field in MACRO_FIELDS
    }
    household_grams = _household_grams(row, household_portion_sum)
    warnings = _candidate_warnings(allocations)
    household_loss = _household_loss(allocations)
    egg_guard = _household_candidate_egg_guard(row, allocations, config)
    egg_penalty = _to_float(egg_guard.get("egg_load_penalty")) or 0.0
    if egg_penalty > 0:
        household_loss += egg_penalty
        warnings.append("direct_egg_load_candidate_penalty")
    row = dict(row)
    row.update(household_macros)
    row["portion_multiplier"] = round(household_portion_sum, 3)
    row["portion_grams_estimated"] = _round_optional(household_grams, 1)
    row["household_portion_sum"] = round(household_portion_sum, 3)
    row["single_profile_portion_reference"] = 1.0
    row["household_grocery_scaling_factor"] = round(household_portion_sum, 3)
    row["member_portion_summary_json"] = json.dumps(allocations, ensure_ascii=True)
    row["household_fit_warnings"] = ";".join(warnings)
    row["household_loss"] = round(household_loss, 6)
    row["household_fit"] = round(max(0.0, 1.0 - household_loss), 4)
    row["egg_load_status"] = egg_guard.get("egg_load_status", "ok")
    row["direct_egg_count"] = egg_guard.get("direct_egg_count", 0.0)
    row["total_egg_count"] = egg_guard.get("egg_count", 0.0)
    row["egg_load_penalty"] = round(egg_penalty, 6)
    row["egg_load_reasons"] = ";".join(egg_guard.get("egg_load_reasons", []))
    row["household_generation_shared_slot"] = True
    row["household_allocation_mode"] = config["allocation_mode"]
    row["household_member_count"] = len(allocations)
    row["portion_policy_mode"] = "household_lite"
    row["portion_policy_reasons"] = ["household_portion_sum_from_member_allocations"]
    row["portion_policy_warnings"] = warnings
    macro_scores = macro_fit(actual=household_macros, target=aggregate_slot_target)
    row.update(macro_scores)
    preview_scores = compute_score_preview(row)
    row.update(preview_scores)
    row["score_preview"] = round(
        max(0.0, float(row.get("score_preview") or 0.0) - min(household_loss, 1.0) * 0.08),
        4,
    )
    return row


def _non_shared_candidate_row(
    row: dict[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    row = dict(row)
    row["household_portion_sum"] = _to_float(row.get("portion_multiplier")) or 1.0
    row["single_profile_portion_reference"] = _to_float(row.get("portion_multiplier")) or 1.0
    row["household_grocery_scaling_factor"] = 1.0
    row["member_portion_summary_json"] = "[]"
    row["household_fit_warnings"] = "non_shared_slot_target_fill_assumption"
    row["household_loss"] = 0.0
    row["household_fit"] = 1.0
    row["household_generation_shared_slot"] = False
    row["household_allocation_mode"] = config["allocation_mode"]
    row["household_member_count"] = 0
    return row


def _decorate_household_plan(
    *,
    base_plan: dict[str, Any],
    household_profile: dict[str, Any],
    member_targets: dict[str, Any],
    aggregate_target: NutritionTarget,
    household_candidates: pd.DataFrame,
    individual_slot_candidates: pd.DataFrame | Mapping[str, pd.DataFrame] | None,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    days = _normalized_days(base_plan)
    shared_allocations = _allocation_rows_from_days(days, config=config)
    individual_meals = _individual_meal_rows_from_candidates(
        days=days,
        slot_candidates=individual_slot_candidates,
        member_targets=member_targets,
        shared_allocations=shared_allocations,
        config=config,
    )
    allocations = [*shared_allocations, *individual_meals]
    member_daily_rows = _member_daily_rows(
        days,
        shared_allocations,
        individual_meals,
        member_targets,
        config,
    )
    grocery_scaling = _grocery_scaling_rows(days)
    day_quality = _household_day_quality_rows(
        days,
        member_daily_rows,
        allocations,
        grocery_scaling,
    )
    for day in days:
        quality = next(
            (
                row
                for row in day_quality
                if int(row.get("day_index") or 0) == int(day.get("day_index") or 0)
            ),
            {},
        )
        day["household_quality_status"] = quality.get("household_quality_status", "review")
        day["household_quality_reasons"] = quality.get("household_quality_reasons", "")

    summary = _household_summary(
        days=days,
        allocations=allocations,
        member_daily_rows=member_daily_rows,
        grocery_scaling=grocery_scaling,
        day_quality=day_quality,
        household_candidates=household_candidates,
    )
    protein_correction_rows = _protein_correction_rows(member_daily_rows)
    result = dict(base_plan)
    ingredient_audit = audit_household_ingredient_load(
        {"days": days, "household_summary": summary},
        allocations,
        config=config,
    )
    egg_load = ingredient_audit.get("egg_load", {})
    summary.update(_egg_summary_fields(egg_load))
    result.update(
        {
            "household_generation_version": "v1_lite",
            "household_mode": config["household_mode"],
            "household_allocation_mode": config["allocation_mode"],
            "household_id": household_profile.get("household_id"),
            "household_name": household_profile.get("household_name"),
            "household_profile_path": str(
                config.get("household_profile_path") or DEFAULT_HOUSEHOLD_PROFILE_PATH
            ),
            "household_profile": household_profile,
            "member_targets": member_targets,
            "household_target": _target_to_dict(aggregate_target),
            "days": days,
            "allocations": allocations,
            "shared_allocations": shared_allocations,
            "individual_meals": individual_meals,
            "protein_correction_rows": protein_correction_rows,
            "member_daily_rows": member_daily_rows,
            "household_day_quality_rows": day_quality,
            "grocery_scaling": grocery_scaling,
            "household_ingredient_audit": ingredient_audit,
            "egg_load_audit": egg_load,
            "household_summary": summary,
            "warnings": _household_warnings(config, summary),
        }
    )
    return result


def _member_daily_rows(
    days: list[dict[str, Any]],
    shared_allocations: list[dict[str, Any]],
    individual_meals: list[dict[str, Any]],
    member_targets: dict[str, Any],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    shared_by_key = {
        (
            int(row.get("day_index") or 0),
            str(row.get("slot") or ""),
            str(row.get("member_id") or ""),
        ): row
        for row in shared_allocations
    }
    individual_by_key = {
        (
            int(row.get("day_index") or 0),
            str(row.get("slot") or ""),
            str(row.get("member_id") or ""),
        ): row
        for row in individual_meals
    }
    shared_slots = set(_shared_slots(member_targets, config))
    targets_by_member_id = member_targets.get("targets_by_member_id") or {}
    rows: list[dict[str, Any]] = []
    for day in days:
        day_index = int(day.get("day_index") or 1)
        for member in member_targets.get("members", []):
            member_id = str(member.get("member_id") or "")
            target = targets_by_member_id.get(member_id) or {}
            totals = {field: 0.0 for field in MACRO_FIELDS}
            shared_totals = {field: 0.0 for field in MACRO_FIELDS}
            shared_slot_count = 0
            individual_slot_count = 0
            target_fill_slot_count = 0
            selected_correction_meals: list[str] = []
            for slot, slot_target in (target.get("slot_targets") or {}).items():
                if str(slot) in shared_slots:
                    allocated = shared_by_key.get((day_index, str(slot), member_id))
                    if allocated:
                        shared_slot_count += 1
                        for field in MACRO_FIELDS:
                            value = _to_float(allocated.get(field)) or 0.0
                            totals[field] += value
                            shared_totals[field] += value
                        continue
                individual = individual_by_key.get((day_index, str(slot), member_id))
                if individual:
                    individual_slot_count += 1
                    if bool(individual.get("protein_correction_applied")):
                        selected_correction_meals.append(
                            str(individual.get("display_name") or individual.get("recipe_id") or "")
                        )
                    for field in MACRO_FIELDS:
                        totals[field] += _to_float(individual.get(field)) or 0.0
                    continue
                target_fill_slot_count += 1
                for field in MACRO_FIELDS:
                    totals[field] += _to_float(slot_target.get(field)) or 0.0
            protein_gap_before = max(
                (_to_float(target.get("protein_g")) or 0.0) - shared_totals["protein_g"],
                0.0,
            )
            protein_gap_after = max(
                (_to_float(target.get("protein_g")) or 0.0) - totals["protein_g"],
                0.0,
            )
            rows.append(
                {
                    "day_index": day_index,
                    "member_id": member_id,
                    "member": member.get("display_name"),
                    "shared_slot_count": shared_slot_count,
                    "individual_slot_count": individual_slot_count,
                    "target_fill_slot_count": target_fill_slot_count,
                    "kcal": round(totals["kcal"], 1),
                    "protein_g": round(totals["protein_g"], 1),
                    "carbs_g": round(totals["carbs_g"], 1),
                    "fat_g": round(totals["fat_g"], 1),
                    "target_kcal": target.get("kcal"),
                    "target_protein_g": target.get("protein_g"),
                    "target_carbs_g": target.get("carbs_g"),
                    "target_fat_g": target.get("fat_g"),
                    "kcal_ratio": _round_optional(_ratio(totals["kcal"], target.get("kcal")), 4),
                    "protein_ratio": _round_optional(
                        _ratio(totals["protein_g"], target.get("protein_g")),
                        4,
                    ),
                    "carbs_ratio": _round_optional(_ratio(totals["carbs_g"], target.get("carbs_g")), 4),
                    "fat_ratio": _round_optional(_ratio(totals["fat_g"], target.get("fat_g")), 4),
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
                    "protein_gap_before_g": round(protein_gap_before, 1),
                    "protein_gap_after_g": round(protein_gap_after, 1),
                    "protein_correction_applied": protein_gap_after < protein_gap_before,
                    "selected_correction_meals": "; ".join(
                        item for item in selected_correction_meals if item
                    ),
                }
            )
    return rows


def _allocation_rows_from_days(
    days: list[dict[str, Any]],
    *,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    allowed_by_member = _allowed_recipe_ids_by_member(config)
    for day in days:
        day_index = int(day.get("day_index") or 1)
        for meal in day.get("selected_meals", []):
            allocations = _parse_allocations(meal.get("member_portion_summary_json"))
            kept_allocations = []
            excluded_member_ids = []
            for row in allocations:
                member_id = str(row.get("member_id") or "")
                recipe_id = str(meal.get("recipe_id") or "").strip()
                allowed_ids = allowed_by_member.get(member_id)
                if allowed_ids is not None and recipe_id and recipe_id not in allowed_ids:
                    excluded_member_ids.append(member_id)
                    continue
                kept_allocations.append(row)
            if excluded_member_ids:
                meal["member_allocations"] = kept_allocations
                meal["member_portion_summary_json"] = json.dumps(
                    kept_allocations,
                    ensure_ascii=True,
                )
                meal["household_dietary_excluded_member_ids"] = sorted(
                    member_id for member_id in excluded_member_ids if member_id
                )
                meal["household_dietary_partial_shared"] = True
                portion_sum = round(
                    sum(
                        _to_float(row.get("portion_multiplier_member")) or 0.0
                        for row in kept_allocations
                    ),
                    3,
                )
                meal["household_portion_sum"] = portion_sum
                meal["household_grocery_scaling_factor"] = portion_sum
            else:
                meal["member_allocations"] = allocations
            for row in kept_allocations:
                enriched = dict(row)
                enriched.update(
                    {
                        "day_index": day_index,
                        "slot": meal.get("slot"),
                        "recipe_id": meal.get("recipe_id"),
                        "display_name": meal.get("display_name"),
                        "recipe": meal.get("display_name"),
                        "directions_json": meal.get("directions_json"),
                        "directions_step_count": meal.get("directions_step_count"),
                        "cooking_steps": meal.get("cooking_steps", []),
                        "household_portion_sum": meal.get("household_portion_sum"),
                        "household_grocery_scaling_factor": meal.get(
                            "household_grocery_scaling_factor"
                        ),
                        "allocation_scope": "shared",
                        "household_generation_shared_slot": True,
                        **_time_fields_from_row(meal),
                    }
                )
                rows.append(enriched)
    return rows


def _protein_correction_rows(member_daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in member_daily_rows:
        before_gap = _to_float(row.get("protein_gap_before_g")) or 0.0
        after_gap = _to_float(row.get("protein_gap_after_g")) or 0.0
        if before_gap <= 0 and after_gap <= 0:
            continue
        rows.append(
            {
                "day_index": row.get("day_index"),
                "member_id": row.get("member_id"),
                "member": row.get("member"),
                "protein_gap_before_g": round(before_gap, 1),
                "protein_gap_after_g": round(after_gap, 1),
                "protein_gap_improved_g": round(max(before_gap - after_gap, 0.0), 1),
                "protein_correction_applied": bool(row.get("protein_correction_applied")),
                "selected_correction_meals": row.get("selected_correction_meals"),
            }
        )
    return rows


def _individual_meal_rows_from_candidates(
    *,
    days: list[dict[str, Any]],
    slot_candidates: pd.DataFrame | Mapping[str, pd.DataFrame] | None,
    member_targets: dict[str, Any],
    shared_allocations: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates = _candidate_frame(slot_candidates) if slot_candidates is not None else pd.DataFrame()
    if candidates.empty or "slot" not in candidates.columns:
        return []
    if "household_generation_shared_slot" in candidates.columns:
        candidates = candidates.loc[
            candidates["household_generation_shared_slot"].astype(str).str.lower() != "true"
        ].copy()
    if candidates.empty:
        return []

    rows: list[dict[str, Any]] = []
    shared_slots = set(_shared_slots(member_targets, config))
    base_individual_slots = [
        slot
        for slot in build_household_aggregate_target(member_targets).slot_targets.keys()
        if str(slot) not in shared_slots
    ]

    shared_by_member_day: dict[tuple[int, str], dict[str, float]] = {}
    for row in shared_allocations:
        key = (int(row.get("day_index") or 0), str(row.get("member_id") or ""))
        bucket = shared_by_member_day.setdefault(key, {field: 0.0 for field in MACRO_FIELDS})
        for field in MACRO_FIELDS:
            bucket[field] += _to_float(row.get(field)) or 0.0

    used_by_member_day: dict[tuple[int, str], set[str]] = {}
    for row in shared_allocations:
        key = (int(row.get("day_index") or 0), str(row.get("member_id") or ""))
        recipe_id = str(row.get("recipe_id") or "").strip()
        if recipe_id:
            used_by_member_day.setdefault(key, set()).add(recipe_id)

    used_by_member_all_days: dict[str, set[str]] = {}
    for row in shared_allocations:
        member_id = str(row.get("member_id") or "")
        recipe_id = str(row.get("recipe_id") or "").strip()
        if member_id and recipe_id:
            used_by_member_all_days.setdefault(member_id, set()).add(recipe_id)

    preferred_by_member_day_slot = _vegetarian_companion_preferences(days)

    targets_by_member_id = member_targets.get("targets_by_member_id") or {}
    for day in days:
        day_index = int(day.get("day_index") or 1)
        for member in member_targets.get("members", []):
            member_id = str(member.get("member_id") or "")
            member_candidates = _candidate_frame_for_member(
                slot_candidates,
                member_id,
            )
            if member_candidates.empty or "slot" not in member_candidates.columns:
                continue
            if "household_generation_shared_slot" in member_candidates.columns:
                member_candidates = member_candidates.loc[
                    member_candidates["household_generation_shared_slot"]
                    .astype(str)
                    .str.lower()
                    != "true"
                ].copy()
            if member_candidates.empty:
                continue
            prepared = member_candidates.sort_values(
                ["slot", "score_preview", "macro_fit", "recipe_id", "portion_multiplier"],
                ascending=[True, False, False, True, True],
                kind="mergesort",
                na_position="last",
            ).copy()
            target = targets_by_member_id.get(member_id) or {}
            member_total_protein = _to_float(target.get("protein_g")) or 0.0
            shared_protein = (
                shared_by_member_day.get((day_index, member_id), {}).get("protein_g", 0.0)
            )
            protein_priority = (
                member_total_protein > 0
                and (shared_protein / member_total_protein) < float(config.get("protein_correction_threshold", 0.85) or 0.85)
            )
            member_slots = list(base_individual_slots)
            shared_keys = {
                (
                    int(row.get("day_index") or 0),
                    str(row.get("slot") or ""),
                    str(row.get("member_id") or ""),
                )
                for row in shared_allocations
            }
            for slot in shared_slots:
                if (day_index, str(slot), member_id) not in shared_keys:
                    member_slots.append(str(slot))
            for slot in dict.fromkeys(member_slots):
                slot_target = (target.get("slot_targets") or {}).get(slot, {})
                if (_to_float(slot_target.get("kcal")) or 0.0) <= 0:
                    continue
                slot_frame = prepared.loc[prepared["slot"].astype(str) == str(slot)]
                selected = _select_individual_candidate(
                    slot_candidates=slot_frame,
                    slot_target=slot_target,
                    member=member,
                    day_index=day_index,
                    used_recipe_ids=used_by_member_day.setdefault(
                        (day_index, member_id),
                        set(),
                    ),
                    used_recipe_ids_global=used_by_member_all_days.setdefault(
                        member_id,
                        set(),
                    ),
                    preferred_recipe_ids=preferred_by_member_day_slot.get(
                        (day_index, str(slot), member_id),
                        [],
                    ),
                    protein_priority=protein_priority,
                    config=config,
                )
                if selected:
                    rows.append(selected)
                    recipe_id = str(selected.get("recipe_id") or "").strip()
                    if recipe_id:
                        used_by_member_day[(day_index, member_id)].add(recipe_id)
                        used_by_member_all_days.setdefault(member_id, set()).add(recipe_id)
    return rows


def _vegetarian_companion_preferences(
    days: Sequence[Mapping[str, Any]],
) -> dict[tuple[int, str, str], list[str]]:
    result: dict[tuple[int, str, str], list[str]] = {}
    for day in days:
        day_index = int(day.get("day_index") or 1)
        for meal in day.get("selected_meals", []) or []:
            if not isinstance(meal, Mapping):
                continue
            preferred_ids = preferred_companion_recipe_ids(meal)
            if not preferred_ids:
                continue
            excluded_member_ids = meal.get("household_dietary_excluded_member_ids") or []
            if isinstance(excluded_member_ids, str):
                excluded_member_ids = [excluded_member_ids]
            for member_id in excluded_member_ids:
                cleaned_member_id = str(member_id or "").strip()
                if cleaned_member_id:
                    result[
                        (day_index, str(meal.get("slot") or ""), cleaned_member_id)
                    ] = preferred_ids
    return result


def _select_individual_candidate(
    *,
    slot_candidates: pd.DataFrame,
    slot_target: Mapping[str, Any],
    member: Mapping[str, Any],
    day_index: int,
    used_recipe_ids: set[str],
    used_recipe_ids_global: set[str],
    preferred_recipe_ids: Sequence[str],
    protein_priority: bool,
    config: Mapping[str, Any],
) -> dict[str, Any] | None:
    if slot_candidates.empty:
        return None
    best: dict[str, Any] | None = None
    best_key: tuple[float, float, float, str] | None = None
    min_multiplier = float(config["portion_multiplier_min"])
    max_multiplier = float(config["portion_multiplier_max"])
    mode = str(config.get("allocation_mode") or DEFAULT_ALLOCATION_MODE)

    for _, candidate in slot_candidates.head(80).iterrows():
        row = candidate.to_dict()
        per_one = _per_one_portion_macros(row)
        if (_to_float(per_one.get("kcal")) or 0.0) <= 0:
            continue
        raw_multiplier = _raw_member_multiplier(
            mode=mode,
            per_one_portion=per_one,
            slot_target=slot_target,
        )
        multiplier = _clamp(raw_multiplier, min_multiplier, max_multiplier)
        actual = {
            field: round(per_one[field] * multiplier, 1)
            for field in MACRO_FIELDS
        }
        warnings = _member_portion_warnings(
            raw_multiplier=raw_multiplier,
            multiplier=multiplier,
            actual=actual,
            slot_target=slot_target,
        )
        recipe_id = str(row.get("recipe_id") or "").strip()
        protein_deficit = max(
            0.0,
            -_deviation_ratio(actual["protein_g"], slot_target.get("protein_g")),
        )
        loss = (
            abs(_deviation_ratio(actual["kcal"], slot_target.get("kcal"))) * 0.45
            + abs(_deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))) * 0.25
            + abs(_deviation_ratio(actual["carbs_g"], slot_target.get("carbs_g"))) * 0.15
            + abs(_deviation_ratio(actual["fat_g"], slot_target.get("fat_g"))) * 0.15
        )
        if protein_priority:
            loss += protein_deficit * 0.55
        preferred_companion = is_preferred_companion(recipe_id, preferred_recipe_ids)
        if preferred_companion:
            loss -= float(config.get("mixed_vegetarian_companion_bonus") or 0.22)
            warnings.append("mixed_vegetarian_companion_preferred")
        if recipe_id in used_recipe_ids:
            loss += 0.18
            warnings.append("individual_recipe_repeat_pressure")
        if recipe_id in used_recipe_ids_global:
            loss += float(config.get("individual_global_repeat_penalty") or 0.22)
            warnings.append("individual_multi_day_repeat_pressure")
        egg_guard = _individual_candidate_egg_guard(
            row=row,
            portion_multiplier=multiplier,
            config=config,
        )
        egg_penalty = _to_float(egg_guard.get("egg_load_penalty")) or 0.0
        if egg_penalty > 0:
            loss += egg_penalty
            warnings.append("direct_egg_load_candidate_penalty")
        protein_density = _protein_density(actual)
        score_preview = _to_float(row.get("score_preview")) or 0.0
        key = (
            round(loss, 8),
            -round(protein_density, 8) if protein_priority else 0.0,
            -score_preview,
            recipe_id,
        )
        if best_key is None or key < best_key:
            grams = _per_one_portion_grams(row)
            best_key = key
            best = {
                "day_index": day_index,
                "slot": row.get("slot"),
                "recipe_id": recipe_id,
                "display_name": row.get("display_name"),
                "recipe": row.get("display_name"),
                "directions_json": row.get("directions_json"),
                "directions_step_count": row.get("directions_step_count"),
                "cooking_steps": row.get("cooking_steps", []),
                "member_id": member.get("member_id"),
                "member": member.get("display_name"),
                "portion_multiplier_raw": round(raw_multiplier, 3),
                "portion_multiplier": round(multiplier, 3),
                "portion_multiplier_member": round(multiplier, 3),
                "portion_multiplier_clamped": not _approximately_equal(
                    raw_multiplier,
                    multiplier,
                ),
                "grams_estimated": _round_optional(
                    grams * multiplier if grams is not None else None,
                    1,
                ),
                "kcal": actual["kcal"],
                "protein_g": actual["protein_g"],
                "carbs_g": actual["carbs_g"],
                "fat_g": actual["fat_g"],
                "slot_kcal_target": slot_target.get("kcal"),
                "slot_protein_g_target": slot_target.get("protein_g"),
                "slot_carbs_g_target": slot_target.get("carbs_g"),
                "slot_fat_g_target": slot_target.get("fat_g"),
                "kcal_deviation_pct": _percent(
                    _deviation_ratio(actual["kcal"], slot_target.get("kcal"))
                ),
                "protein_deviation_pct": _percent(
                    _deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))
                ),
                "carbs_deviation_pct": _percent(
                    _deviation_ratio(actual["carbs_g"], slot_target.get("carbs_g"))
                ),
                "fat_deviation_pct": _percent(
                    _deviation_ratio(actual["fat_g"], slot_target.get("fat_g"))
                ),
                "protein_density_g_per_100_kcal": round(protein_density, 2),
                "protein_correction_applied": bool(protein_priority),
                "mixed_vegetarian_companion_selected": bool(preferred_companion),
                "individual_candidate_loss": round(loss, 6),
                "egg_load_status": egg_guard.get("egg_load_status", "ok"),
                "egg_source_type": egg_guard.get("egg_source_type", ""),
                "direct_egg_count": egg_guard.get("direct_egg_count", 0.0),
                "total_egg_count": egg_guard.get("egg_count", 0.0),
                "egg_load_penalty": round(egg_penalty, 6),
                "egg_load_reasons": ";".join(egg_guard.get("egg_load_reasons", [])),
                "allocation_scope": "individual",
                "household_generation_shared_slot": False,
                "household_portion_fit_warning": ";".join(warnings),
                **_time_fields_from_row(row),
            }
    return best


def _time_fields_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in TIME_FIELDS if field in row}


def _household_candidate_egg_guard(
    row: Mapping[str, Any],
    allocations: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(config.get("enable_egg_load_guard", True)):
        return _empty_egg_guard()
    ingredients = config.get("recipe_ingredients_df")
    if ingredients is None:
        return _empty_egg_guard()
    total_load = _empty_egg_guard()
    total_penalty = 0.0
    direct_count = 0.0
    total_count = 0.0
    reasons: list[str] = []
    status = "ok"
    for allocation in allocations:
        multiplier = _to_float(allocation.get("portion_multiplier_member")) or 1.0
        load = candidate_egg_load(
            row,
            recipe_ingredients_df=ingredients,
            portion_multiplier=multiplier,
            config=config,
        )
        total_penalty += _to_float(load.get("egg_load_penalty")) or 0.0
        direct_count += _to_float(load.get("direct_egg_count")) or 0.0
        total_count += _to_float(load.get("egg_count")) or 0.0
        reasons.extend(str(reason) for reason in load.get("egg_load_reasons", []) if reason)
        if _status_rank(str(load.get("egg_load_status") or "ok")) > _status_rank(status):
            status = str(load.get("egg_load_status") or "ok")
    total_load.update(
        {
            "egg_load_status": status,
            "direct_egg_count": round(direct_count, 3),
            "egg_count": round(total_count, 3),
            "egg_load_penalty": round(total_penalty, 6),
            "egg_load_reasons": sorted(dict.fromkeys(reasons)),
        }
    )
    return total_load


def _individual_candidate_egg_guard(
    *,
    row: Mapping[str, Any],
    portion_multiplier: float,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(config.get("enable_egg_load_guard", True)):
        return _empty_egg_guard()
    ingredients = config.get("recipe_ingredients_df")
    if ingredients is None:
        return _empty_egg_guard()
    return candidate_egg_load(
        row,
        recipe_ingredients_df=ingredients,
        portion_multiplier=portion_multiplier,
        config=config,
    )


def _empty_egg_guard() -> dict[str, Any]:
    return {
        "egg_load_status": "ok",
        "egg_source_type": "",
        "direct_egg_count": 0.0,
        "egg_count": 0.0,
        "egg_load_penalty": 0.0,
        "egg_load_reasons": [],
    }


def _grocery_scaling_rows(days: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in days:
        day_index = int(day.get("day_index") or 1)
        for meal in day.get("selected_meals", []):
            if not bool(meal.get("household_generation_shared_slot", True)):
                continue
            rows.append(
                {
                    "day_index": day_index,
                    "slot": meal.get("slot"),
                    "recipe_id": meal.get("recipe_id"),
                    "display_name": meal.get("display_name"),
                    "household_portion_sum": meal.get("household_portion_sum"),
                    "single_profile_portion_reference": meal.get(
                        "single_profile_portion_reference",
                        1.0,
                    ),
                    "household_quantity_factor": meal.get(
                        "household_grocery_scaling_factor"
                    ),
                    "warning": "household_quantity_factor_large"
                    if (
                        (_to_float(meal.get("household_grocery_scaling_factor")) or 0.0)
                        > HOUSEHOLD_GROCERY_FACTOR_REVIEW_THRESHOLD
                    )
                    else "",
                }
            )
    return rows


def _household_day_quality_rows(
    days: list[dict[str, Any]],
    member_daily_rows: list[dict[str, Any]],
    allocations: list[dict[str, Any]],
    grocery_scaling: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_day: dict[int, list[dict[str, Any]]] = {}
    for row in member_daily_rows:
        by_day.setdefault(int(row.get("day_index") or 0), []).append(row)
    allocations_by_day: dict[int, list[dict[str, Any]]] = {}
    for row in allocations:
        allocations_by_day.setdefault(int(row.get("day_index") or 0), []).append(row)
    grocery_by_day: dict[int, list[dict[str, Any]]] = {}
    for row in grocery_scaling:
        grocery_by_day.setdefault(int(row.get("day_index") or 0), []).append(row)

    for day in days:
        day_index = int(day.get("day_index") or 1)
        day_rows = by_day.get(day_index, [])
        reasons: list[str] = []
        base_day_validation_status = str(day.get("validation_status") or "not_validated")
        max_abs_kcal = _max_abs(day_rows, "kcal_deviation_pct")
        min_protein_ratio = _min_numeric(day_rows, "protein_ratio")
        min_kcal_ratio = _min_numeric(day_rows, "kcal_ratio")
        max_kcal_ratio = _max_numeric(day_rows, "kcal_ratio")
        min_carbs_ratio = _min_numeric(day_rows, "carbs_ratio")
        max_fat_ratio = _max_numeric(day_rows, "fat_ratio")
        if max_abs_kcal > 20.0:
            reasons.append("member_kcal_ratio_review")
        if min_protein_ratio is not None and min_protein_ratio < 0.80:
            reasons.append("member_protein_ratio_review")
        if (
            min_carbs_ratio is not None
            and min_carbs_ratio < HOUSEHOLD_MIN_CARBS_RATIO_REVIEW
        ):
            reasons.append("member_carbs_ratio_review")
        if (
            max_fat_ratio is not None
            and max_fat_ratio > HOUSEHOLD_MAX_FAT_RATIO_REVIEW
        ):
            reasons.append("member_fat_ratio_review")
        clamp_count = sum(
            1
            for row in allocations_by_day.get(day_index, [])
            if bool(row.get("portion_multiplier_clamped"))
        )
        if clamp_count > 0:
            reasons.append("member_portion_clamps_present")
        max_grocery_factor = _max_numeric(
            grocery_by_day.get(day_index, []),
            "household_quantity_factor",
        )
        if (
            max_grocery_factor is not None
            and max_grocery_factor > HOUSEHOLD_GROCERY_FACTOR_REVIEW_THRESHOLD
        ):
            reasons.append("household_grocery_scaling_review")
        severe = (
            not day_rows
        ) or (
            min_kcal_ratio is not None and min_kcal_ratio < 0.65
        ) or (
            max_kcal_ratio is not None and max_kcal_ratio > 1.35
        ) or (
            min_protein_ratio is not None and min_protein_ratio < 0.65
        ) or clamp_count >= 6
        if severe:
            status = "reject"
        elif reasons:
            status = "review"
        else:
            status = "accept"
        rows.append(
            {
                "day_index": day_index,
                "household_quality_status": status,
                "household_quality_reasons": ";".join(reasons),
                "max_abs_kcal_deviation_pct": round(max_abs_kcal, 1),
                "min_kcal_ratio": _round_optional(min_kcal_ratio, 4),
                "max_kcal_ratio": _round_optional(max_kcal_ratio, 4),
                "min_protein_ratio": _round_optional(min_protein_ratio, 4),
                "min_carbs_ratio": _round_optional(min_carbs_ratio, 4),
                "max_fat_ratio": _round_optional(max_fat_ratio, 4),
                "base_day_validation_status": base_day_validation_status,
                "portion_clamped_count": clamp_count,
                "max_grocery_scaling_factor": _round_optional(max_grocery_factor, 3),
            }
        )
    return rows


def _household_summary(
    *,
    days: list[dict[str, Any]],
    allocations: list[dict[str, Any]],
    member_daily_rows: list[dict[str, Any]],
    grocery_scaling: list[dict[str, Any]],
    day_quality: list[dict[str, Any]],
    household_candidates: pd.DataFrame,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for row in day_quality:
        status = str(row.get("household_quality_status") or "missing")
        status_counts[status] = status_counts.get(status, 0) + 1
    clamped_count = sum(
        1 for row in allocations if bool(row.get("portion_multiplier_clamped"))
    )
    protein_gap_rows = [
        row
        for row in member_daily_rows
        if (_to_float(row.get("protein_gap_after_g")) or 0.0) > 0.0
    ]
    worst_protein_row = min(
        member_daily_rows,
        key=lambda row: _to_float(row.get("protein_ratio")) or 999.0,
    ) if member_daily_rows else {}
    return {
        "member_count": len({str(row.get("member_id")) for row in member_daily_rows}),
        "days_generated": len(days),
        "shared_meal_count": len(
            {
                (
                    int(row.get("day_index") or 0),
                    str(row.get("slot") or ""),
                    str(row.get("recipe_id") or ""),
                )
                for row in allocations
                if str(row.get("allocation_scope") or "shared") == "shared"
            }
        ),
        "individual_meal_count": sum(
            1 for row in allocations if str(row.get("allocation_scope") or "") == "individual"
        ),
        "dietary_partial_shared_meal_count": sum(
            1
            for day in days
            for meal in day.get("selected_meals", [])
            if isinstance(meal, Mapping)
            and bool(meal.get("household_dietary_partial_shared"))
        ),
        "allocation_count": len(allocations),
        "household_candidate_count": int(len(household_candidates)),
        "household_accept_day_count": status_counts.get("accept", 0),
        "household_review_day_count": status_counts.get("review", 0),
        "household_reject_day_count": status_counts.get("reject", 0),
        "household_quality_status": _overall_quality(status_counts),
        "mean_abs_kcal_deviation_pct": _mean_abs(member_daily_rows, "kcal_deviation_pct"),
        "max_abs_kcal_deviation_pct": _max_abs(member_daily_rows, "kcal_deviation_pct"),
        "mean_abs_protein_deviation_pct": _mean_abs(
            member_daily_rows,
            "protein_deviation_pct",
        ),
        "max_abs_protein_deviation_pct": _max_abs(
            member_daily_rows,
            "protein_deviation_pct",
        ),
        "min_protein_ratio": _min_numeric(member_daily_rows, "protein_ratio"),
        "worst_protein_member": worst_protein_row.get("member"),
        "protein_gap_count": len(protein_gap_rows),
        "protein_correction_applied_count": sum(
            1 for row in member_daily_rows if bool(row.get("protein_correction_applied"))
        ),
        "min_portion_multiplier": _min_numeric(allocations, "portion_multiplier_member"),
        "max_portion_multiplier": _max_numeric(allocations, "portion_multiplier_member"),
        "clamped_portion_count": clamped_count,
        "max_grocery_scaling_factor": _max_numeric(
            grocery_scaling,
            "household_quantity_factor",
        ),
    }


def _household_warnings(config: Mapping[str, Any], summary: Mapping[str, Any]) -> list[str]:
    warnings = [
        "household_generation_v1_lite_not_production",
        "shared_recipe_model_no_separate_member_menus",
        "no_household_optimizer_or_backend",
        "grocery_scaling_quantity_only",
    ]
    if config["household_mode"] == HOUSEHOLD_MODE_SHARED_ALL_SLOTS:
        warnings.append("shared_all_slots_first_lite_mode")
    if config["household_mode"] == HOUSEHOLD_MODE_SHARED_MAIN_MEALS:
        warnings.append("breakfast_snack_individual_selection_prototype")
    if config["household_mode"] == HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN:
        warnings.append("individual_breakfast_shared_main_recommended_demo_mode")
    if int(summary.get("dietary_partial_shared_meal_count") or 0) > 0:
        warnings.append("household_member_dietary_split_applied")
    if int(summary.get("clamped_portion_count") or 0) > 0:
        warnings.append("member_portion_clamps_present")
    if int(summary.get("protein_gap_count") or 0) > 0:
        warnings.append("member_protein_gap_remaining")
    direct_eggs_per_person_day = _to_float(summary.get("direct_eggs_per_person_per_day")) or 0.0
    egg_status = str(summary.get("egg_load_status") or "ok")
    if direct_eggs_per_person_day > 3.0:
        warnings.append("household_direct_egg_load_severe_warning")
    elif direct_eggs_per_person_day > 2.0:
        warnings.append("household_direct_egg_load_warning")
    elif egg_status == "severe_warning":
        warnings.append("household_total_egg_load_severe_warning")
    elif egg_status == "warning":
        warnings.append("household_total_egg_load_warning")
    if str(summary.get("household_quality_status") or "") != "accept":
        warnings.append("household_quality_review_needed")
    return warnings


def _egg_summary_fields(egg_load: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "egg_load_status": egg_load.get("egg_load_status", "ok"),
        "total_egg_count": egg_load.get("total_egg_count", 0.0),
        "direct_egg_count": egg_load.get("direct_egg_count", 0.0),
        "embedded_egg_count": egg_load.get("embedded_egg_count", 0.0),
        "uncertain_egg_count": egg_load.get("uncertain_egg_count", 0.0),
        "eggs_per_person_per_day": egg_load.get("eggs_per_person_per_day", 0.0),
        "direct_eggs_per_person_per_day": egg_load.get(
            "direct_eggs_per_person_per_day",
            0.0,
        ),
        "egg_load_reasons": ";".join(egg_load.get("egg_load_reasons", [])),
    }


def _status_rank(status: str) -> int:
    return {"ok": 0, "warning": 1, "severe_warning": 2}.get(str(status), 0)


def _candidate_frame(value: pd.DataFrame | Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    pieces = [frame.copy() for frame in value.values() if isinstance(frame, pd.DataFrame)]
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()


def _candidate_frame_for_member(
    value: pd.DataFrame | Mapping[str, pd.DataFrame] | None,
    member_id: str,
) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if isinstance(value, Mapping):
        direct = value.get(member_id)
        if isinstance(direct, pd.DataFrame):
            return direct.copy()
        return _candidate_frame(value)
    return pd.DataFrame()


def _allowed_recipe_ids_by_member(config: Mapping[str, Any]) -> dict[str, set[str]]:
    raw = config.get("member_allowed_recipe_ids_by_member_id")
    if not isinstance(raw, Mapping):
        return {}
    result: dict[str, set[str]] = {}
    for member_id, values in raw.items():
        if values is None:
            continue
        if isinstance(values, str):
            iterable = [values]
        else:
            try:
                iterable = list(values)
            except TypeError:
                iterable = []
        result[str(member_id)] = {
            str(value).strip()
            for value in iterable
            if str(value).strip()
        }
    return result


def _slot_candidates_by_slot(
    candidates: pd.DataFrame,
    slot_order: Sequence[str],
) -> dict[str, pd.DataFrame]:
    return {
        str(slot): candidates.loc[candidates["slot"].astype(str) == str(slot)].copy()
        for slot in slot_order
    }


def _shared_slots(member_targets: dict[str, Any], config: Mapping[str, Any]) -> list[str]:
    mode = str(config.get("household_mode") or HOUSEHOLD_MODE_SHARED_ALL_SLOTS)
    if mode in {
        HOUSEHOLD_MODE_SHARED_MAIN_MEALS,
        HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
    }:
        return ["lunch", "dinner"]
    aggregate = build_household_aggregate_target(member_targets)
    return list(aggregate.slot_targets.keys())


def _resolved_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = dict(config or {})
    mode = str(raw.get("household_mode") or HOUSEHOLD_MODE_SHARED_ALL_SLOTS)
    if mode not in HOUSEHOLD_MODES or mode == HOUSEHOLD_MODE_OFF:
        mode = HOUSEHOLD_MODE_SHARED_ALL_SLOTS
    allocation_mode = str(raw.get("allocation_mode") or raw.get("household_allocation_mode") or DEFAULT_ALLOCATION_MODE)
    if allocation_mode not in HOUSEHOLD_ALLOCATION_MODES:
        allocation_mode = DEFAULT_ALLOCATION_MODE
    return {
        **raw,
        "household_mode": mode,
        "allocation_mode": allocation_mode,
        "portion_multiplier_min": _to_float(raw.get("portion_multiplier_min"))
        or PORTION_MIN_DEFAULT,
        "portion_multiplier_max": _to_float(raw.get("portion_multiplier_max"))
        or PORTION_MAX_DEFAULT,
        "enable_egg_load_guard": bool(raw.get("enable_egg_load_guard", True)),
        "egg_guard_warning_penalty_per_egg": _to_float(
            raw.get("egg_guard_warning_penalty_per_egg")
        )
        or 0.25,
        "egg_guard_severe_penalty_per_egg": _to_float(
            raw.get("egg_guard_severe_penalty_per_egg")
        )
        or 0.55,
        "egg_guard_total_penalty_per_egg": _to_float(
            raw.get("egg_guard_total_penalty_per_egg")
        )
        or 0.08,
    }


def _serializable_config(config: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in dict(config).items():
        if isinstance(value, pd.DataFrame):
            result[key] = f"<DataFrame rows={len(value)}>"
        else:
            result[key] = value
    return result


def _one_day_config(config: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "return_alternatives": True,
        "alternative_count": int(config.get("alternative_count", 3) or 3),
        "meal_realism_mode": config.get("meal_realism_mode", "practical"),
        "max_candidates_per_slot": int(config.get("max_candidates_per_slot", 40) or 40),
    }


def _multi_day_config(config: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": config.get("meal_realism_mode", "practical"),
        "quality_gate": config.get("quality_gate", "demo_safe"),
        "alternative_count": int(config.get("alternative_count", 3) or 3),
        "return_alternatives": True,
        "multi_day_mode": config.get("multi_day_mode", MULTI_DAY_MODE_GLOBAL),
        "candidate_day_alternative_count": int(
            config.get("candidate_day_alternative_count", 10) or 10
        ),
        "global_max_candidates_per_slot": int(
            config.get("global_max_candidates_per_slot", 26) or 26
        ),
        "day_candidate_pool_size_target": int(
            config.get("day_candidate_pool_size_target", 75) or 75
        ),
        "day_candidate_pool_max": int(config.get("day_candidate_pool_max", 150) or 150),
        "include_slot_forced_variants": True,
        "no_repeat_policy": config.get("no_repeat_policy", "hard"),
        "multi_day_speed_mode": config.get("multi_day_speed_mode", "fast"),
        "day_candidate_builder": config.get("day_candidate_builder", "direct_from_slots"),
        "direct_slot_shortlist_size": int(config.get("direct_slot_shortlist_size", 12) or 12),
    }


def _per_one_portion_macros(row: Mapping[str, Any]) -> dict[str, float]:
    base_multiplier = _to_float(row.get("portion_multiplier")) or 1.0
    if base_multiplier <= 0:
        base_multiplier = 1.0
    return {
        field: (_to_float(row.get(field)) or 0.0) / base_multiplier
        for field in MACRO_FIELDS
    }


def _per_one_portion_grams(row: Mapping[str, Any]) -> float | None:
    base_multiplier = _to_float(row.get("portion_multiplier")) or 1.0
    grams = _to_float(row.get("portion_grams_estimated"))
    if grams is not None and grams > 0 and base_multiplier > 0:
        return grams / base_multiplier
    for field in ("overlay_serving_weight_g_estimated", "serving_weight_g_estimated"):
        value = _to_float(row.get(field))
        if value is not None and value > 0:
            return value
    return None


def _raw_member_multiplier(
    *,
    mode: str,
    per_one_portion: dict[str, float],
    slot_target: Mapping[str, Any],
) -> float:
    if mode == "proportional_to_slot_kcal":
        return _ratio(slot_target.get("kcal"), per_one_portion.get("kcal"))
    return _macro_aware_multiplier(per_one_portion=per_one_portion, slot_target=slot_target)


def _macro_aware_multiplier(
    *,
    per_one_portion: dict[str, float],
    slot_target: Mapping[str, Any],
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


def _member_portion_warnings(
    *,
    raw_multiplier: float,
    multiplier: float,
    actual: Mapping[str, Any],
    slot_target: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if not _approximately_equal(raw_multiplier, multiplier):
        if raw_multiplier < multiplier:
            warnings.append("portion_clamped_low")
        else:
            warnings.append("portion_clamped_high")
    if abs(_deviation_ratio(actual.get("kcal"), slot_target.get("kcal"))) > 0.25:
        warnings.append("kcal_slot_deviation_review")
    if abs(_deviation_ratio(actual.get("protein_g"), slot_target.get("protein_g"))) > 0.35:
        warnings.append("protein_slot_deviation_review")
    return warnings


def _candidate_warnings(allocations: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for row in allocations:
        for code in str(row.get("household_portion_fit_warning") or "").split(";"):
            clean = code.strip()
            if clean and clean not in warnings:
                warnings.append(clean)
    return warnings


def _household_loss(allocations: list[dict[str, Any]]) -> float:
    if not allocations:
        return 1.0
    losses = []
    for row in allocations:
        losses.append(
            abs((_to_float(row.get("kcal_deviation_pct")) or 0.0) / 100.0) * 0.55
            + abs((_to_float(row.get("protein_deviation_pct")) or 0.0) / 100.0) * 0.25
            + abs((_to_float(row.get("carbs_deviation_pct")) or 0.0) / 100.0) * 0.10
            + abs((_to_float(row.get("fat_deviation_pct")) or 0.0) / 100.0) * 0.10
        )
    return sum(losses) / len(losses)


def _protein_density(actual: Mapping[str, Any]) -> float:
    kcal = _to_float(actual.get("kcal")) or 0.0
    protein = _to_float(actual.get("protein_g")) or 0.0
    if kcal <= 0:
        return 0.0
    return protein / kcal * 100.0


def _household_grams(row: Mapping[str, Any], portion_sum: float) -> float | None:
    per_one = _per_one_portion_grams(row)
    if per_one is None:
        return None
    return per_one * portion_sum


def _normalized_days(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    days = plan.get("days")
    if isinstance(days, list) and days:
        return [dict(day) for day in days if isinstance(day, dict)]
    selected_meals = plan.get("selected_meals")
    if isinstance(selected_meals, list) and selected_meals:
        return [
            {
                "day_index": 1,
                "selected_meals": selected_meals,
                "day_totals": plan.get("day_totals", {}),
                "validation_status": (plan.get("validation") or {}).get(
                    "validation_status",
                    "not_validated",
                )
                if isinstance(plan.get("validation"), dict)
                else "not_validated",
                "quality_gate_status": plan.get("quality_gate_status", "missing"),
                "warnings": plan.get("warnings", []),
            }
        ]
    return []


def _parse_allocations(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, dict)]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [dict(item) for item in decoded if isinstance(item, dict)]


def _day_clamp_count(day: Mapping[str, Any]) -> int:
    count = 0
    for meal in day.get("selected_meals", []):
        for allocation in meal.get("member_allocations", []):
            if bool(allocation.get("portion_multiplier_clamped")):
                count += 1
    return count


def _overall_quality(status_counts: Mapping[str, int]) -> str:
    if int(status_counts.get("reject", 0) or 0) > 0:
        return "reject"
    if int(status_counts.get("review", 0) or 0) > 0:
        return "review"
    if int(status_counts.get("accept", 0) or 0) > 0:
        return "accept"
    return "missing"


def _target_to_dict(target: NutritionTarget) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def _write_rows(rows: Any, path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(list(rows or [])).to_csv(output_path, index=False)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if pd.isna(value) if not isinstance(value, (dict, list, tuple, str, bytes)) else False:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            return str(value)
    return value


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


def _min_numeric(rows: Sequence[Mapping[str, Any]], field: str) -> float | None:
    values = [_to_float(row.get(field)) for row in rows]
    clean_values = [value for value in values if value is not None]
    return round(min(clean_values), 3) if clean_values else None


def _max_numeric(rows: Sequence[Mapping[str, Any]], field: str) -> float | None:
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
