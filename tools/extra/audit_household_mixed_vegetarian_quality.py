from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.service import generate_household_plan_from_request
from tools.extra.check_household_mixed_vegetarian_generation import (
    _contains_any_meat,
    _find_menu,
    _mixed_household_request,
    _vegetarian_violations,
)


def main() -> int:
    response = generate_household_plan_from_request(_mixed_household_request())
    plan = _mapping(response.get("generator_plan"))
    summary = _mapping(plan.get("household_summary"))
    member_rows = _list(response.get("member_macro_summaries"))
    day_quality_rows = _list(plan.get("household_day_quality_rows"))
    grocery_scaling_rows = _list(response.get("household_grocery_scaling"))
    partial_shared = _partial_shared_meals(plan)
    individual_rows = _list(plan.get("individual_meals"))
    violations = _vegetarian_violations(_find_menu(response, "alice"))
    normal_member_meat = {
        member_id: _contains_any_meat(_find_menu(response, member_id))
        for member_id in ("adrian", "marius")
    }

    print("HOUSEHOLD MIXED VEGETARIAN QUALITY AUDIT")
    print(f"status={response.get('status')}")
    print(f"household_quality_status={summary.get('household_quality_status')}")
    print(f"warnings={_join(response.get('warnings'))}")
    print(f"vegetarian_violations={len(violations)}")
    print(f"normal_member_meat={normal_member_meat}")
    print()

    print("SUMMARY")
    for key in (
        "member_count",
        "days_generated",
        "shared_meal_count",
        "individual_meal_count",
        "dietary_partial_shared_meal_count",
        "household_accept_day_count",
        "household_review_day_count",
        "household_reject_day_count",
        "mean_abs_kcal_deviation_pct",
        "max_abs_kcal_deviation_pct",
        "mean_abs_protein_deviation_pct",
        "max_abs_protein_deviation_pct",
        "min_protein_ratio",
        "max_grocery_scaling_factor",
        "egg_load_status",
        "eggs_per_person_per_day",
    ):
        print(f"{key}={summary.get(key)}")
    print()

    print("DAY QUALITY")
    for row in day_quality_rows:
        print(
            "day={day} status={status} reasons={reasons} "
            "max_abs_kcal_dev={kcal_dev}% min_protein_ratio={protein_ratio} "
            "max_grocery_factor={grocery_factor}".format(
                day=row.get("day_index"),
                status=row.get("household_quality_status"),
                reasons=row.get("household_quality_reasons") or "ok",
                kcal_dev=row.get("max_abs_kcal_deviation_pct"),
                protein_ratio=row.get("min_protein_ratio"),
                grocery_factor=row.get("max_grocery_scaling_factor"),
            )
        )
    print()

    print("MEMBER MACROS")
    for row in member_rows:
        print(
            "{member_id:<7} {member:<8} kcal={kcal}/{target_kcal} "
            "({kcal_dev:+}%) protein={protein}/{target_protein} "
            "({protein_dev:+}%) carbs={carbs}/{target_carbs} "
            "({carbs_dev:+}%) fat={fat}/{target_fat} ({fat_dev:+}%) "
            "shared={shared} individual={individual} fill={fill}".format(
                member_id=str(row.get("member_id") or ""),
                member=str(row.get("member") or ""),
                kcal=row.get("kcal"),
                target_kcal=row.get("target_kcal"),
                kcal_dev=row.get("kcal_deviation_pct"),
                protein=row.get("protein_g"),
                target_protein=row.get("target_protein_g"),
                protein_dev=row.get("protein_deviation_pct"),
                carbs=row.get("carbs_g"),
                target_carbs=row.get("target_carbs_g"),
                carbs_dev=row.get("carbs_deviation_pct"),
                fat=row.get("fat_g"),
                target_fat=row.get("target_fat_g"),
                fat_dev=row.get("fat_deviation_pct"),
                shared=row.get("shared_slot_count"),
                individual=row.get("individual_slot_count"),
                fill=row.get("target_fill_slot_count"),
            )
        )
    print()

    print("PARTIAL SHARED MEALS")
    if partial_shared:
        for meal in partial_shared:
            print(
                "day={day} slot={slot} recipe={recipe} excluded={excluded}".format(
                    day=meal.get("day_index"),
                    slot=meal.get("slot"),
                    recipe=meal.get("display_name"),
                    excluded=_join(meal.get("household_dietary_excluded_member_ids")),
                )
            )
    else:
        print("none")
    print()

    print("INDIVIDUAL MEALS")
    for row in individual_rows:
        print(
            "day={day} member={member} slot={slot} recipe={recipe} kcal={kcal} protein={protein}".format(
                day=row.get("day_index"),
                member=row.get("member_id"),
                slot=row.get("slot"),
                recipe=row.get("display_name"),
                kcal=row.get("kcal"),
                protein=row.get("protein_g"),
            )
        )
    print()

    print("GROCERY SCALING")
    for row in grocery_scaling_rows:
        print(
            "day={day} slot={slot} recipe={recipe} factor={factor} warning={warning}".format(
                day=row.get("day_index"),
                slot=row.get("slot"),
                recipe=row.get("display_name"),
                factor=row.get("household_quantity_factor"),
                warning=row.get("warning") or "ok",
            )
        )
    print()

    print("NEXT TUNING SIGNALS")
    for signal in _tuning_signals(summary, day_quality_rows, member_rows, grocery_scaling_rows):
        print(f"- {signal}")

    if violations:
        return 1
    if not any(normal_member_meat.values()):
        return 1
    return 0


def _partial_shared_meals(plan: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    meals: list[Mapping[str, Any]] = []
    for day in _list(plan.get("days")):
        for meal in _list(day.get("selected_meals")):
            if isinstance(meal, Mapping) and bool(meal.get("household_dietary_partial_shared")):
                meals.append(meal)
    return meals


def _tuning_signals(
    summary: Mapping[str, Any],
    day_quality_rows: list[Mapping[str, Any]],
    member_rows: list[Mapping[str, Any]],
    grocery_scaling_rows: list[Mapping[str, Any]],
) -> list[str]:
    signals: list[str] = []
    if str(summary.get("household_quality_status") or "") != "accept":
        signals.append(
            "Household quality is not accept; inspect day quality reasons before scoring changes."
        )
    for row in day_quality_rows:
        reasons = str(row.get("household_quality_reasons") or "")
        if "day_not_valid" in reasons:
            signals.append("Base day is invalid_nutrition before household allocation.")
        if "household_grocery_scaling_review" in reasons:
            signals.append("At least one shared meal has household quantity factor above 4.0.")
    for row in member_rows:
        kcal_dev = abs(_to_float(row.get("kcal_deviation_pct")) or 0.0)
        protein_ratio = _to_float(row.get("protein_ratio"))
        carbs_ratio = _to_float(row.get("carbs_ratio"))
        fat_ratio = _to_float(row.get("fat_ratio"))
        member = str(row.get("member") or row.get("member_id") or "member")
        if kcal_dev > 10.0:
            signals.append(f"{member} kcal deviation is above 10%.")
        if protein_ratio is not None and protein_ratio < 0.90:
            signals.append(f"{member} protein is below 90% of target.")
        if carbs_ratio is not None and carbs_ratio < 0.75:
            signals.append(f"{member} carbs are low; check adolescent/gain targets.")
        if fat_ratio is not None and fat_ratio > 1.25:
            signals.append(f"{member} fat is high versus target.")
    for row in grocery_scaling_rows:
        factor = _to_float(row.get("household_quantity_factor"))
        if factor is not None and factor > 4.0:
            signals.append(
                f"Shared recipe '{row.get('display_name')}' has grocery factor {factor}."
            )
    if not signals:
        signals.append("No obvious tuning signal found.")
    return list(dict.fromkeys(signals))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _join(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    return str(value or "")


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())
