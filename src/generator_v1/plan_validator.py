from __future__ import annotations

from typing import Any

import pandas as pd

from src.generator_v1.target_builder import NutritionTarget


MIN_DAILY_KCAL_RATIO = 0.70
MIN_DAILY_PROTEIN_RATIO = 0.70
MAX_DAY_TIME_MIN = 300
EXTREME_MEAL_TIME_MIN = 180


def validate_one_day_plan(
    plan: dict[str, Any],
    target: NutritionTarget | dict[str, Any],
) -> dict[str, object]:
    target_data = _target_to_dict(target)
    totals = plan.get("day_totals", {})
    selected_meals = plan.get("selected_meals", [])
    expected_slots = len(target_data.get("slot_targets", {}))
    selected_slots = int(totals.get("selected_slot_count") or len(selected_meals))
    total_kcal = _to_float(totals.get("total_kcal"))
    total_protein = _to_float(totals.get("total_protein_g"))
    total_time = _to_float(totals.get("total_time_min_sum"))
    target_kcal = _to_float(target_data.get("kcal"))
    target_protein = _to_float(target_data.get("protein_g"))

    warnings: list[str] = []
    incomplete_invalid = selected_slots != expected_slots
    nutrition_invalid = False
    time_invalid = False

    if incomplete_invalid:
        warnings.append(
            f"Plan incomplet: selected_slot_count={selected_slots}, expected_slots={expected_slots}"
        )

    kcal_ratio = _safe_ratio(total_kcal, target_kcal)
    protein_ratio = _safe_ratio(total_protein, target_protein)
    if kcal_ratio < MIN_DAILY_KCAL_RATIO:
        nutrition_invalid = True
        warnings.append("Total kcal sub 70pct din tinta zilnica.")
    if protein_ratio < MIN_DAILY_PROTEIN_RATIO:
        nutrition_invalid = True
        warnings.append("Total protein sub 70pct din tinta zilnica.")

    suspicious_meals = [
        meal.get("slot", "unknown")
        for meal in selected_meals
        if bool(meal.get("is_nutrition_suspicious", False))
    ]
    if suspicious_meals:
        nutrition_invalid = True
        warnings.append(
            "Mese cu nutritie suspecta: " + ", ".join(str(slot) for slot in suspicious_meals)
        )

    if total_time > MAX_DAY_TIME_MIN:
        time_invalid = True
        warnings.append(
            "total_time_min_sum depaseste 300; timpul include pasiv si planul poate fi nerealist pentru gatire in aceeasi zi."
        )

    long_meals = [
        str(meal.get("slot", "unknown"))
        for meal in selected_meals
        if _to_float(meal.get("total_time_min")) > EXTREME_MEAL_TIME_MIN
    ]
    if long_meals:
        time_invalid = True
        warnings.append(
            "Mese cu timp extrem peste 180 min: " + ", ".join(long_meals)
        )

    validation_status = _validation_status(
        incomplete_invalid=incomplete_invalid,
        nutrition_invalid=nutrition_invalid,
        time_invalid=time_invalid,
    )
    return {
        "is_valid_for_checkpoint_1": validation_status == "valid",
        "validation_status": validation_status,
        "validation_warnings": warnings,
        "target_comparison": {
            "total_kcal": total_kcal,
            "target_kcal": target_kcal,
            "kcal_ratio": round(kcal_ratio, 4),
            "min_kcal_ratio": MIN_DAILY_KCAL_RATIO,
            "total_protein_g": total_protein,
            "target_protein_g": target_protein,
            "protein_ratio": round(protein_ratio, 4),
            "min_protein_ratio": MIN_DAILY_PROTEIN_RATIO,
            "total_time_min_sum": total_time,
            "max_day_time_min": MAX_DAY_TIME_MIN,
            "selected_slot_count": selected_slots,
            "expected_slot_count": expected_slots,
        },
    }


def _validation_status(
    incomplete_invalid: bool,
    nutrition_invalid: bool,
    time_invalid: bool,
) -> str:
    invalid_reasons = sum([incomplete_invalid, nutrition_invalid, time_invalid])
    if invalid_reasons == 0:
        return "valid"
    if invalid_reasons > 1:
        return "invalid_mixed"
    if incomplete_invalid:
        return "invalid_incomplete"
    if nutrition_invalid:
        return "invalid_nutrition"
    return "invalid_time"


def _target_to_dict(target: NutritionTarget | dict[str, Any]) -> dict[str, Any]:
    if isinstance(target, NutritionTarget):
        return {
            "kcal": target.kcal,
            "protein_g": target.protein_g,
            "carbs_g": target.carbs_g,
            "fat_g": target.fat_g,
            "slot_targets": target.slot_targets,
        }
    return target


def _safe_ratio(actual: float, target: float) -> float:
    if target <= 0:
        return 0.0
    return actual / target


def _to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)

