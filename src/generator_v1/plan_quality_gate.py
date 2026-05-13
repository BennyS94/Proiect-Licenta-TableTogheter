from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from src.generator_v1.target_builder import NutritionTarget


QUALITY_GATE_OFF = "off"
QUALITY_GATE_DEMO_SAFE = "demo_safe"

REJECT_ADJUSTED_DAY_LOSS_MAX = 0.20
REJECT_BASE_DAY_LOSS_MAX = 0.18
REVIEW_ADJUSTED_DAY_LOSS_MIN = 0.12
REVIEW_ADJUSTED_DAY_LOSS_MAX = 0.20

REJECT_KCAL_RATIO_MIN = 0.75
REJECT_KCAL_RATIO_MAX = 1.25
REJECT_PROTEIN_RATIO_MIN = 0.75
REJECT_CARBS_RATIO_MIN = 0.60
REVIEW_CARBS_RATIO_MIN = 0.60
REVIEW_CARBS_RATIO_MAX = 0.75

SEVERE_REALISM_FLAGS = {
    "breakfast_too_large",
    "unrealistic_large_portion",
    "snack_too_large",
}
MAIN_REALISM_FLAGS = {
    "low_protein_main",
    "low_carb_main",
    "mostly_carb_meal",
    "mostly_protein_meal",
}
REVIEW_REALISM_FLAGS = {
    "borderline_large_portion",
}


def evaluate_plan_quality(
    plan: Mapping[str, object],
    target: NutritionTarget | Mapping[str, object],
    config: Mapping[str, object] | None = None,
) -> dict[str, object]:
    mode = _quality_gate_mode(config)
    if mode == QUALITY_GATE_OFF:
        return _result(
            status="accept",
            reasons=["quality_gate_off"],
            reject_count=0,
            review_count=0,
            severe_realism_issue_count=0,
            macro_issue_count=0,
        )

    target_data = _target_to_dict(target)
    validation = plan.get("validation", {})
    if not isinstance(validation, Mapping):
        validation = {}
    diagnostics = plan.get("selector_diagnostics", {})
    if not isinstance(diagnostics, Mapping):
        diagnostics = {}
    totals = plan.get("day_totals", {})
    if not isinstance(totals, Mapping):
        totals = {}

    reject_reasons: list[str] = []
    review_reasons: list[str] = []

    validation_status = str(validation.get("validation_status", "not_validated"))
    if validation_status != "valid":
        reject_reasons.append(f"validation_status_not_valid:{validation_status}")
    if validation.get("is_valid_for_checkpoint_1") is False:
        reject_reasons.append("not_valid_for_checkpoint_1")

    base_day_loss = _metric_value(plan, diagnostics, "base_day_loss")
    adjusted_day_loss = _metric_value(plan, diagnostics, "adjusted_day_loss")
    if adjusted_day_loss > REJECT_ADJUSTED_DAY_LOSS_MAX:
        reject_reasons.append(
            f"adjusted_day_loss_over_{REJECT_ADJUSTED_DAY_LOSS_MAX}"
        )
    elif REVIEW_ADJUSTED_DAY_LOSS_MIN <= adjusted_day_loss <= REVIEW_ADJUSTED_DAY_LOSS_MAX:
        review_reasons.append(
            f"adjusted_day_loss_between_{REVIEW_ADJUSTED_DAY_LOSS_MIN}_and_{REVIEW_ADJUSTED_DAY_LOSS_MAX}"
        )
    if base_day_loss > REJECT_BASE_DAY_LOSS_MAX:
        reject_reasons.append(f"base_day_loss_over_{REJECT_BASE_DAY_LOSS_MAX}")

    ratios = _macro_ratios(totals, target_data)
    macro_issue_count = 0
    if ratios["kcal_ratio"] < REJECT_KCAL_RATIO_MIN:
        macro_issue_count += 1
        reject_reasons.append(f"kcal_ratio_under_{REJECT_KCAL_RATIO_MIN}")
    if ratios["kcal_ratio"] > REJECT_KCAL_RATIO_MAX:
        macro_issue_count += 1
        reject_reasons.append(f"kcal_ratio_over_{REJECT_KCAL_RATIO_MAX}")
    if ratios["protein_ratio"] < REJECT_PROTEIN_RATIO_MIN:
        macro_issue_count += 1
        reject_reasons.append(f"protein_ratio_under_{REJECT_PROTEIN_RATIO_MIN}")
    if ratios["carbs_ratio"] < REJECT_CARBS_RATIO_MIN:
        macro_issue_count += 1
        reject_reasons.append(f"carbs_ratio_under_{REJECT_CARBS_RATIO_MIN}")
    elif REVIEW_CARBS_RATIO_MIN <= ratios["carbs_ratio"] < REVIEW_CARBS_RATIO_MAX:
        macro_issue_count += 1
        review_reasons.append(
            f"carbs_ratio_between_{REVIEW_CARBS_RATIO_MIN}_and_{REVIEW_CARBS_RATIO_MAX}"
        )

    meal_issue_data = _meal_issue_data(plan.get("selected_meals", []))
    severe_realism_issue_count = meal_issue_data["severe_realism_issue_count"]
    main_issue_count = meal_issue_data["main_issue_count"]
    if severe_realism_issue_count > 0:
        reject_reasons.append("severe_realism_issue_present")
    if main_issue_count >= 2:
        reject_reasons.append("two_or_more_main_realism_issues")
    elif main_issue_count == 1:
        review_reasons.append("one_main_realism_issue")
    if meal_issue_data["borderline_large_portion_count"] > 0:
        review_reasons.append("borderline_large_portion_present")

    status = "accept"
    if reject_reasons:
        status = "reject"
    elif review_reasons:
        status = "review"

    return _result(
        status=status,
        reasons=[*reject_reasons, *review_reasons] or ["quality_gate_passed"],
        reject_count=len(reject_reasons),
        review_count=len(review_reasons),
        severe_realism_issue_count=severe_realism_issue_count,
        macro_issue_count=macro_issue_count,
    )


def _result(
    status: str,
    reasons: list[str],
    reject_count: int,
    review_count: int,
    severe_realism_issue_count: int,
    macro_issue_count: int,
) -> dict[str, object]:
    penalty = reject_count * 0.25 + review_count * 0.08
    score = max(0.0, min(1.0, 1.0 - penalty))
    return {
        "quality_gate_status": status,
        "quality_gate_reasons": reasons,
        "quality_gate_score": round(score, 4),
        "severe_realism_issue_count": int(severe_realism_issue_count),
        "macro_issue_count": int(macro_issue_count),
    }


def _quality_gate_mode(config: Mapping[str, object] | None) -> str:
    if not config:
        return QUALITY_GATE_DEMO_SAFE
    mode = str(config.get("quality_gate", QUALITY_GATE_DEMO_SAFE)).strip().lower()
    if mode == QUALITY_GATE_OFF:
        return QUALITY_GATE_OFF
    return QUALITY_GATE_DEMO_SAFE


def _macro_ratios(
    totals: Mapping[str, object],
    target: Mapping[str, object],
) -> dict[str, float]:
    return {
        "kcal_ratio": _safe_ratio(totals.get("total_kcal"), target.get("kcal")),
        "protein_ratio": _safe_ratio(
            totals.get("total_protein_g"),
            target.get("protein_g"),
        ),
        "carbs_ratio": _safe_ratio(
            totals.get("total_carbs_g"),
            target.get("carbs_g"),
        ),
        "fat_ratio": _safe_ratio(totals.get("total_fat_g"), target.get("fat_g")),
    }


def _meal_issue_data(selected_meals: object) -> dict[str, int]:
    severe_count = 0
    main_issue_count = 0
    borderline_count = 0
    if not isinstance(selected_meals, list):
        selected_meals = []
    for meal in selected_meals:
        if not isinstance(meal, Mapping):
            continue
        flags = set(_reason_items(meal.get("meal_realism_flags")))
        if flags & SEVERE_REALISM_FLAGS:
            severe_count += 1
        if flags & REVIEW_REALISM_FLAGS:
            borderline_count += 1
        slot = str(meal.get("slot", "")).strip().lower()
        if slot in {"lunch", "dinner"} and flags & MAIN_REALISM_FLAGS:
            main_issue_count += 1
    return {
        "severe_realism_issue_count": severe_count,
        "main_issue_count": main_issue_count,
        "borderline_large_portion_count": borderline_count,
    }


def _metric_value(
    plan: Mapping[str, object],
    diagnostics: Mapping[str, object],
    field: str,
) -> float:
    if field in diagnostics:
        return _to_float(diagnostics.get(field))
    return _to_float(plan.get(field))


def _target_to_dict(target: NutritionTarget | Mapping[str, object]) -> dict[str, object]:
    if isinstance(target, NutritionTarget):
        return {
            "kcal": target.kcal,
            "protein_g": target.protein_g,
            "carbs_g": target.carbs_g,
            "fat_g": target.fat_g,
            "slot_targets": target.slot_targets,
        }
    return dict(target)


def _reason_items(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    if "|" in text:
        return [item.strip() for item in text.split("|") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    if ";" in text:
        return [item.strip() for item in text.split(";") if item.strip()]
    return [text]


def _safe_ratio(actual: object, target: object) -> float:
    target_value = _to_float(target)
    if target_value <= 0:
        return 0.0
    return _to_float(actual) / target_value


def _to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)
