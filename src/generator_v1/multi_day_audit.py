from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd


MACRO_FIELDS = {
    "kcal": ("total_kcal", "kcal"),
    "protein_g": ("total_protein_g", "protein_g"),
    "carbs_g": ("total_carbs_g", "carbs_g"),
    "fat_g": ("total_fat_g", "fat_g"),
}


def summarize_multi_day_plan(
    days: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    recipe_ids = _all_recipe_ids(days)
    counts = Counter(recipe_ids)
    repeated_recipe_ids = sorted(
        recipe_id for recipe_id, count in counts.items() if count > 1
    )
    day_losses = [_day_loss(day) for day in days]
    numeric_day_losses = [value for value in day_losses if value is not None]
    valid_day_count = sum(
        1
        for day in days
        if _validation_status(day) == "valid"
    )
    review_day_count = sum(
        1
        for day in days
        if _quality_gate_status(day) == "review"
    )
    accept_day_count = sum(
        1
        for day in days
        if _quality_gate_status(day) == "accept"
    )
    reject_day_count = sum(
        1
        for day in days
        if _quality_gate_status(day) == "reject"
    )
    fallback_day_count = sum(1 for day in days if bool(day.get("fallback_used")))
    repeated_recipes_by_slot = _repeated_recipes_by_slot(days)
    warnings = _multi_day_warnings(days, repeated_recipe_ids)
    summary = {
        "generated_day_count": len(days),
        "total_recipe_count": len(recipe_ids),
        "unique_recipe_count": len(counts),
        "repeated_recipe_count": len(repeated_recipe_ids),
        "repeated_recipe_occurrence_count": sum(
            max(0, count - 1) for count in counts.values()
        ),
        "repeated_recipe_ids": repeated_recipe_ids,
        "average_day_loss": (
            round(sum(numeric_day_losses) / len(numeric_day_losses), 6)
            if numeric_day_losses
            else None
        ),
        "valid_day_count": valid_day_count,
        "accept_day_count": accept_day_count,
        "review_day_count": review_day_count,
        "reject_day_count": reject_day_count,
        "fallback_day_count": fallback_day_count,
        "average_macro_ratios": _average_macro_ratios(days, target),
        "repeated_recipes_by_slot": repeated_recipes_by_slot,
        "multi_day_warnings": warnings,
    }
    summary["multi_day_classification"] = _classify_multi_day(summary)

    return summary


def validate_multi_day_plan(
    plan_or_days: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    target: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    days = _days(plan_or_days)
    summary = summarize_multi_day_plan(days, target)
    candidate_pool_summary = {}
    if isinstance(plan_or_days, Mapping):
        value = plan_or_days.get("candidate_day_pool_summary")
        if isinstance(value, Mapping):
            candidate_pool_summary = dict(value)
    all_days_valid = bool(days) and summary["valid_day_count"] == len(days)
    any_repeated_exact_recipe = bool(summary["repeated_recipe_ids"])
    any_day_fallback_used = any(bool(day.get("fallback_used")) for day in days)
    review_or_reject_days = [
        {
            "day_index": day.get("day_index"),
            "quality_gate_status": _quality_gate_status(day),
            "quality_gate_reasons": day.get("quality_gate_reasons", []),
        }
        for day in days
        if _quality_gate_status(day) in {"review", "reject"}
    ]

    warnings = list(summary["multi_day_warnings"])
    if not all_days_valid:
        warnings.append("Nu toate zilele sunt valide.")
    if review_or_reject_days:
        warnings.append("Cel putin o zi este marcata review/reject de quality gate.")
    if any_day_fallback_used:
        warnings.append("Cel putin o zi a folosit fallback la best plan.")
    if any_repeated_exact_recipe:
        warnings.append("Exista retete exacte repetate in planul multi-day.")
    if summary.get("multi_day_classification") == "multi_day_bad":
        warnings.append("Clasificarea stricta marcheaza planul ca multi_day_bad.")

    status = "valid"
    if not all_days_valid:
        status = "invalid"
    elif review_or_reject_days or any_day_fallback_used or any_repeated_exact_recipe:
        status = "review"

    return {
        "validation_status": status,
        "all_days_valid": all_days_valid,
        "any_repeated_exact_recipe": any_repeated_exact_recipe,
        "any_day_fallback_used": any_day_fallback_used,
        "review_or_reject_days": review_or_reject_days,
        "accept_day_count": summary["accept_day_count"],
        "review_day_count": summary["review_day_count"],
        "reject_day_count": summary["reject_day_count"],
        "fallback_day_count": summary["fallback_day_count"],
        "unique_recipe_count": summary["unique_recipe_count"],
        "repeated_recipe_count": summary["repeated_recipe_count"],
        "rejected_candidate_count": candidate_pool_summary.get(
            "reject_candidate_count",
            0,
        ),
        "multi_day_classification": summary["multi_day_classification"],
        "average_macro_ratios": summary["average_macro_ratios"],
        "repeated_recipes_by_slot": summary["repeated_recipes_by_slot"],
        "warnings": warnings,
    }


def write_multi_day_plan_json(
    plan: Mapping[str, Any],
    out_json: str | Path,
) -> None:
    output_path = Path(out_json)
    _ensure_parent(output_path)
    output_path.write_text(
        json.dumps(_clean_for_json(plan), indent=2),
        encoding="utf-8",
    )


def write_multi_day_plan_readable(
    plan: Mapping[str, Any],
    out_txt: str | Path,
) -> None:
    output_path = Path(out_txt)
    _ensure_parent(output_path)
    output_path.write_text(
        "\n".join(multi_day_readable_lines(plan)),
        encoding="utf-8",
    )


def write_multi_day_meals_csv(
    plan: Mapping[str, Any],
    out_csv: str | Path,
) -> None:
    output_path = Path(out_csv)
    _ensure_parent(output_path)
    pd.DataFrame(multi_day_meal_rows(plan)).to_csv(output_path, index=False)


def multi_day_meal_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in _days(plan):
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            rows.append(
                {
                    "day_index": day.get("day_index"),
                    "validation_status": day.get("validation_status"),
                    "quality_gate_status": day.get("quality_gate_status"),
                    "fallback_used": day.get("fallback_used"),
                    "diversity_mode_used": day.get("diversity_mode_used"),
                    "recent_recipe_count_used": day.get("recent_recipe_count_used"),
                    "multi_day_mode_used": day.get("multi_day_mode_used"),
                    "candidate_day_id": day.get("candidate_day_id"),
                    "repeated_vs_previous_days": str(
                        meal.get("recipe_id")
                    ) in set(day.get("repeated_recipe_ids_vs_previous_days", [])),
                    "slot": meal.get("slot"),
                    "recipe_id": meal.get("recipe_id"),
                    "display_name": meal.get("display_name"),
                    "portion_multiplier": meal.get("portion_multiplier"),
                    "portion_grams_estimated": meal.get("portion_grams_estimated"),
                    "kcal": meal.get("kcal"),
                    "protein_g": meal.get("protein_g"),
                    "carbs_g": meal.get("carbs_g"),
                    "fat_g": meal.get("fat_g"),
                    "effective_time_min": meal.get("effective_time_min_for_scoring"),
                    "total_elapsed_time_min": meal.get("total_elapsed_time_min"),
                    "active_time_estimated_min": meal.get("active_time_estimated_min"),
                    "passive_time_estimated_min": meal.get("passive_time_estimated_min"),
                    "time_confidence": meal.get("time_confidence"),
                    "time_estimation_method": meal.get("time_estimation_method"),
                    "time_warnings": _format_reasons(meal.get("time_warnings")),
                    "time_feedback_penalty": meal.get("time_feedback_penalty"),
                    "feedback_fit": meal.get("feedback_fit"),
                    "feedback_reasons": _format_reasons(
                        meal.get("feedback_reasons")
                    ),
                    "meal_realism_flags": _format_reasons(
                        meal.get("meal_realism_flags")
                    ),
                    "meal_realism_reasons": _format_reasons(
                        meal.get("meal_realism_reasons")
                    ),
                    "slot_suspicion_reasons": _format_reasons(
                        meal.get("slot_suspicion_reasons")
                    ),
                    "portion_policy_warnings": _format_reasons(
                        meal.get("portion_policy_warnings")
                    ),
                }
            )
    return rows


def multi_day_readable_lines(plan: Mapping[str, Any]) -> list[str]:
    lines = ["Generator v1 multi-day draft plan", ""]
    for day in _days(plan):
        totals = day.get("day_totals", {})
        lines.extend(
            [
                f"DAY {day.get('day_index')}",
                f"validation_status={day.get('validation_status')}",
                f"quality_gate_status={day.get('quality_gate_status')}",
                f"fallback_used={day.get('fallback_used')}",
                f"diversity_mode_used={day.get('diversity_mode_used')}",
                f"multi_day_mode_used={day.get('multi_day_mode_used', plan.get('multi_day_selector_mode', 'simple_3_day'))}",
                (
                    "repeated_recipe_ids_vs_previous_days="
                    + _format_list(day.get("repeated_recipe_ids_vs_previous_days"))
                ),
                (
                    "totals: "
                    f"kcal={_fmt(totals.get('total_kcal'))}, "
                    f"protein_g={_fmt(totals.get('total_protein_g'))}, "
                    f"carbs_g={_fmt(totals.get('total_carbs_g'))}, "
                    f"fat_g={_fmt(totals.get('total_fat_g'))}, "
                    f"effective_time_min={_fmt(totals.get('effective_time_min_sum'))}"
                ),
                (
                    "loss: "
                    f"base_day_loss={_fmt(day.get('selector_diagnostics', {}).get('base_day_loss'), decimals=6)}, "
                    f"adjusted_day_loss={_fmt(day.get('selector_diagnostics', {}).get('adjusted_day_loss'), decimals=6)}"
                ),
                "Meals:",
            ]
        )
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            lines.append(
                (
                    f"- {meal.get('slot')}: {meal.get('display_name')} | "
                    f"portion={_fmt(meal.get('portion_multiplier'))} | "
                    f"grams={_fmt(meal.get('portion_grams_estimated'), decimals=0)} | "
                    f"kcal={_fmt(meal.get('kcal'))} | "
                    f"P/C/F={_fmt(meal.get('protein_g'))}/"
                    f"{_fmt(meal.get('carbs_g'))}/"
                    f"{_fmt(meal.get('fat_g'))} | "
                    f"time_confidence={meal.get('time_confidence', 'unknown')} | "
                    "time_warnings="
                    f"{_format_reasons(meal.get('time_warnings'))} | "
                    "realism_flags="
                    f"{_format_reasons(meal.get('meal_realism_flags'))}"
                )
            )
        warnings = day.get("warnings", [])
        lines.append("Warnings: " + (_format_list(warnings) if warnings else "none"))
        lines.append("")

    summary = plan.get("multi_day_summary", {})
    if isinstance(summary, Mapping):
        lines.extend(
            [
                "Multi-day summary",
                f"requested_days={summary.get('requested_days')}",
                (
                    "actual_days_generated="
                    f"{summary.get('actual_days_generated', summary.get('generated_day_count'))}"
                ),
                f"valid_day_count={summary.get('valid_day_count')}",
                f"accept_day_count={summary.get('accept_day_count')}",
                f"review_day_count={summary.get('review_day_count')}",
                f"reject_day_count={summary.get('reject_day_count')}",
                f"fallback_day_count={summary.get('fallback_day_count')}",
                f"no_repeat_policy_requested={summary.get('no_repeat_policy_requested')}",
                f"no_repeat_policy_used={summary.get('no_repeat_policy_used')}",
                f"fallback_used={summary.get('fallback_used')}",
                f"fallback_reason={summary.get('fallback_reason')}",
                (
                    "day_candidate_pool_count="
                    f"{summary.get('day_candidate_pool_count', summary.get('candidate_day_pool_count'))}"
                ),
                f"feasible_no_repeat_combinations={summary.get('feasible_no_repeat_combinations')}",
                f"unique_recipe_count={summary.get('unique_recipe_count')}",
                f"repeated_recipe_count={summary.get('repeated_recipe_count')}",
                (
                    "repeated_recipe_ids="
                    + _format_list(summary.get("repeated_recipe_ids"))
                ),
                f"average_day_loss={summary.get('average_day_loss')}",
                f"multi_day_loss={summary.get('multi_day_loss')}",
                f"multi_day_classification={summary.get('multi_day_classification')}",
                (
                    "warnings="
                    + _format_list(summary.get("multi_day_warnings"))
                ),
            ]
        )
    return lines


def _classify_multi_day(summary: Mapping[str, Any]) -> str:
    generated = int(summary.get("generated_day_count", 0) or 0)
    valid = int(summary.get("valid_day_count", 0) or 0)
    accept = int(summary.get("accept_day_count", 0) or 0)
    review = int(summary.get("review_day_count", 0) or 0)
    reject = int(summary.get("reject_day_count", 0) or 0)
    fallback = int(summary.get("fallback_day_count", 0) or 0)
    repeated = int(summary.get("repeated_recipe_count", 0) or 0)
    repeated_by_slot = summary.get("repeated_recipes_by_slot", {})
    if not isinstance(repeated_by_slot, Mapping):
        repeated_by_slot = {}
    repeated_main = sum(
        len(repeated_by_slot.get(slot, []))
        for slot in ("lunch", "dinner")
        if isinstance(repeated_by_slot.get(slot, []), list)
    )

    if not generated or valid != generated:
        return "multi_day_bad"
    if reject > 0 or fallback > 0:
        return "multi_day_bad"
    if repeated_main >= 2 or repeated >= 4:
        return "multi_day_bad"
    if accept >= 2 and review == 0 and repeated <= 1 and repeated_main == 0:
        return "multi_day_good"
    return "multi_day_review"


def _multi_day_warnings(
    days: Sequence[Mapping[str, Any]],
    repeated_recipe_ids: Sequence[str],
) -> list[str]:
    warnings: list[str] = []
    if repeated_recipe_ids:
        warnings.append(
            "Retete repetate exact: " + ", ".join(str(item) for item in repeated_recipe_ids)
        )
    for day in days:
        status = _quality_gate_status(day)
        if status in {"review", "reject"}:
            warnings.append(
                f"Day {day.get('day_index')} quality_gate_status={status}."
            )
        if bool(day.get("fallback_used")):
            warnings.append(f"Day {day.get('day_index')} used fallback best plan.")
        if _validation_status(day) != "valid":
            warnings.append(
                f"Day {day.get('day_index')} validation_status={_validation_status(day)}."
            )
    return warnings


def _average_macro_ratios(
    days: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any] | None,
) -> dict[str, float | None]:
    if not target:
        return {name: None for name in MACRO_FIELDS}
    result: dict[str, float | None] = {}
    for name, (total_field, target_field) in MACRO_FIELDS.items():
        ratios = []
        target_value = _to_float(target.get(target_field))
        if target_value <= 0:
            result[name] = None
            continue
        for day in days:
            totals = day.get("day_totals", {})
            if not isinstance(totals, Mapping):
                continue
            ratios.append(_to_float(totals.get(total_field)) / target_value)
        result[name] = round(sum(ratios) / len(ratios), 4) if ratios else None
    return result


def _repeated_recipes_by_slot(
    days: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    by_slot: dict[str, Counter[str]] = {}
    names_by_id: dict[str, str] = {}
    for day in days:
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            slot = str(meal.get("slot", "unknown"))
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if not recipe_id:
                continue
            by_slot.setdefault(slot, Counter())[recipe_id] += 1
            names_by_id[recipe_id] = str(meal.get("display_name", ""))

    result: dict[str, list[dict[str, Any]]] = {}
    for slot, counts in by_slot.items():
        repeated = [
            {
                "recipe_id": recipe_id,
                "display_name": names_by_id.get(recipe_id, ""),
                "count": count,
            }
            for recipe_id, count in counts.items()
            if count > 1
        ]
        if repeated:
            result[slot] = sorted(repeated, key=lambda row: row["recipe_id"])
    return result


def _all_recipe_ids(days: Sequence[Mapping[str, Any]]) -> list[str]:
    recipe_ids: list[str] = []
    for day in days:
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if recipe_id:
                recipe_ids.append(recipe_id)
    return recipe_ids


def _days(
    plan_or_days: Mapping[str, Any] | Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    if isinstance(plan_or_days, Mapping):
        value = plan_or_days.get("days", [])
    else:
        value = plan_or_days
    return [day for day in value if isinstance(day, Mapping)]


def _validation_status(day: Mapping[str, Any]) -> str:
    status = day.get("validation_status")
    if status:
        return str(status)
    validation = day.get("validation", {})
    if isinstance(validation, Mapping):
        return str(validation.get("validation_status", "not_validated"))
    return "not_validated"


def _quality_gate_status(day: Mapping[str, Any]) -> str:
    status = day.get("quality_gate_status")
    if status:
        return str(status)
    quality_gate = day.get("quality_gate", {})
    if isinstance(quality_gate, Mapping):
        return str(quality_gate.get("quality_gate_status", "missing"))
    return "missing"


def _day_loss(day: Mapping[str, Any]) -> float | None:
    diagnostics = day.get("selector_diagnostics", {})
    if not isinstance(diagnostics, Mapping):
        return None
    for field in ("adjusted_day_loss", "day_loss", "base_day_loss"):
        if field in diagnostics:
            return round(_to_float(diagnostics.get(field)), 6)
    return None


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _format_reasons(value: object) -> str:
    if isinstance(value, list):
        return ";".join(str(item) for item in value)
    if isinstance(value, tuple):
        return ";".join(str(item) for item in value)
    if value is None:
        return ""
    return str(value)


def _format_list(value: object) -> str:
    if not isinstance(value, (list, tuple, set)) or not value:
        return "none"
    return ", ".join(str(item) for item in value)


def _fmt(value: object, decimals: int = 1) -> str:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return "missing"
    return f"{float(numeric_value):.{decimals}f}"


def _to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)


def _clean_for_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _clean_for_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clean_for_json(item) for item in value]
    if isinstance(value, tuple):
        return [_clean_for_json(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value
