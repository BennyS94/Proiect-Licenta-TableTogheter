from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.generator_v1.day_selector import SELECTED_MEAL_FIELDS


def write_plan_csv(plan: dict[str, Any], out_csv: str | Path) -> None:
    output_path = Path(out_csv)
    _ensure_parent(output_path)
    rows = []
    for meal in plan.get("selected_meals", []):
        row = {field: meal.get(field) for field in SELECTED_MEAL_FIELDS}
        row["slot_fit_reasons"] = _format_reasons(row.get("slot_fit_reasons"))
        row["slot_suspicion_reasons"] = _format_reasons(
            row.get("slot_suspicion_reasons")
        )
        row["nutrition_quality_reasons"] = _format_reasons(
            row.get("nutrition_quality_reasons")
        )
        row["time_estimation_reasons"] = _format_reasons(
            row.get("time_estimation_reasons")
        )
        row["pilot_nutrition_overlay_reasons"] = _format_reasons(
            row.get("pilot_nutrition_overlay_reasons")
        )
        row["overlay_aliases_used"] = _format_reasons(row.get("overlay_aliases_used"))
        rows.append(row)
    pd.DataFrame(rows, columns=SELECTED_MEAL_FIELDS).to_csv(output_path, index=False)


def write_plan_json(plan: dict[str, Any], out_json: str | Path) -> None:
    output_path = Path(out_json)
    _ensure_parent(output_path)
    output_path.write_text(
        json.dumps(_clean_for_json(plan), indent=2),
        encoding="utf-8",
    )


def write_plan_readable(plan: dict[str, Any], out_txt: str | Path) -> None:
    output_path = Path(out_txt)
    _ensure_parent(output_path)
    output_path.write_text("\n".join(_readable_lines(plan)), encoding="utf-8")


def _readable_lines(plan: dict[str, Any]) -> list[str]:
    target = plan.get("target", {})
    totals = plan.get("day_totals", {})
    warnings = plan.get("warnings", [])
    validation = plan.get("validation", {})
    diagnostics = plan.get("candidate_diagnostics", {})
    lines = [
        "Generator v1 one-day plan preview",
        "Status: " + str(validation.get("validation_status", "not_validated")),
        "Valid checkpoint 1: " + str(validation.get("is_valid_for_checkpoint_1", False)),
        "",
        "Nutrition target",
        f"kcal={_fmt(target.get('kcal'))}",
        f"protein_g={_fmt(target.get('protein_g'))}",
        f"carbs_g={_fmt(target.get('carbs_g'))}",
        f"fat_g={_fmt(target.get('fat_g'))}",
        "",
        "Selected meals",
    ]

    for meal in plan.get("selected_meals", []):
        lines.extend(
            [
                (
                    f"{meal.get('slot')}: {meal.get('display_name')} "
                    f"({meal.get('recipe_id')}, portion={_fmt(meal.get('portion_multiplier'))}, "
                    f"grams_estimated={_fmt(meal.get('portion_grams_estimated'), decimals=0)}, "
                    f"grams_source={meal.get('portion_grams_source')}, "
                    "original_grams_estimated="
                    f"{_fmt(meal.get('original_portion_grams_estimated'), decimals=0)}, "
                    "overlay_grams_estimated="
                    f"{_fmt(meal.get('overlay_portion_grams_estimated'), decimals=0)})"
                ),
                (
                    f"  kcal={_fmt(meal.get('kcal'))}, "
                    f"protein_g={_fmt(meal.get('protein_g'))}, "
                    f"carbs_g={_fmt(meal.get('carbs_g'))}, "
                    f"fat_g={_fmt(meal.get('fat_g'))}, "
                    "original_kcal_per_serving="
                    f"{_fmt(meal.get('original_energy_kcal_per_serving'))}, "
                    "overlay_kcal_per_serving="
                    f"{_fmt(meal.get('overlay_energy_kcal_per_serving'))}, "
                    "uses_pilot_nutrition_overlay="
                    f"{meal.get('uses_pilot_nutrition_overlay')}, "
                    f"total_time_min={_fmt(meal.get('total_time_min'), decimals=0)}, "
                    "active_time_estimated_min="
                    f"{_fmt(meal.get('active_time_estimated_min'), decimals=0)}, "
                    "passive_time_estimated_min="
                    f"{_fmt(meal.get('passive_time_estimated_min'), decimals=0)}, "
                    "effective_time_min_for_scoring="
                    f"{_fmt(meal.get('effective_time_min_for_scoring'), decimals=0)}, "
                    "original_effective_time_min_for_scoring="
                    f"{_fmt(meal.get('original_effective_time_min_for_scoring'), decimals=0)}, "
                    f"has_long_passive_time={meal.get('has_long_passive_time')}, "
                    f"uses_pilot_time_fallback={meal.get('uses_pilot_time_fallback')}"
                ),
                (
                    f"  score_preview={_fmt(meal.get('score_preview'), decimals=2)}, "
                    f"macro_fit={_fmt(meal.get('macro_fit'), decimals=2)}, "
                    f"time_fit={_fmt(meal.get('time_fit'), decimals=2)}, "
                    f"slot_fit={_fmt(meal.get('slot_fit'), decimals=2)}, "
                    f"nutrition_quality={_fmt(meal.get('nutrition_quality'), decimals=2)}, "
                    f"suspicious={meal.get('is_nutrition_suspicious')}, "
                    f"slot_suspicious={meal.get('is_slot_suspicious')}"
                ),
                f"  slot_fit_reasons={_format_reasons(meal.get('slot_fit_reasons'))}",
                (
                    "  slot_suspicion_reasons="
                    + _format_reasons(meal.get("slot_suspicion_reasons"))
                ),
                (
                    "  nutrition_quality_reasons="
                    + _format_reasons(meal.get("nutrition_quality_reasons"))
                ),
                (
                    "  time_estimation_reasons="
                    + _format_reasons(meal.get("time_estimation_reasons"))
                ),
            ]
        )

    lines.extend(
        [
            "",
            "Day totals",
            f"total_kcal={_fmt(totals.get('total_kcal'))}",
            f"total_protein_g={_fmt(totals.get('total_protein_g'))}",
            f"total_carbs_g={_fmt(totals.get('total_carbs_g'))}",
            f"total_fat_g={_fmt(totals.get('total_fat_g'))}",
            f"original_total_kcal={_fmt(totals.get('original_total_kcal'))}",
            f"original_total_protein_g={_fmt(totals.get('original_total_protein_g'))}",
            (
                "uses_pilot_nutrition_overlay_count="
                f"{totals.get('uses_pilot_nutrition_overlay_count', 0)}"
            ),
            f"total_time_min_sum={_fmt(totals.get('total_time_min_sum'), decimals=0)}",
            (
                "effective_time_min_sum="
                f"{_fmt(totals.get('effective_time_min_sum'), decimals=0)}"
            ),
            (
                "passive_time_estimated_sum="
                f"{_fmt(totals.get('passive_time_estimated_sum'), decimals=0)}"
            ),
            f"selected_slot_count={totals.get('selected_slot_count', 0)}",
            "",
            "Validation warnings",
        ]
    )
    lines.extend([str(warning) for warning in validation.get("validation_warnings", [])] or ["none"])
    lines.extend(
        [
            "",
            "Candidate diagnostics",
        ]
    )
    for slot, slot_diag in diagnostics.items():
        lines.append(
            (
                f"{slot}: total={slot_diag.get('total_slot_candidates')}, "
                f"unique_recipes={slot_diag.get('candidate_count')}, "
                f"suspicious={slot_diag.get('suspicious_nutrition_count')}, "
                f"non_suspicious={slot_diag.get('non_suspicious_nutrition_count')}, "
                f"kcal35_pass={slot_diag.get('kcal_35pct_pass_count')}, "
                f"protein20_pass={slot_diag.get('protein_20pct_pass_count')}, "
                f"both_pass={slot_diag.get('both_kcal_and_protein_pass_count')}, "
                f"time_gt_180={slot_diag.get('time_gt_180_count')}, "
                f"long_passive={slot_diag.get('long_passive_time_count', 0)}, "
                f"time_basis={slot_diag.get('time_diagnostic_basis', 'total_time_min')}"
            )
        )
    lines.extend(
        [
            "",
            "Warnings",
        ]
    )
    lines.extend([str(warning) for warning in warnings] or ["none"])
    lines.extend(
        [
            "",
            "Note",
            "feedback_fit si variety_fit sunt placeholder neutru 0.50, nu logica finala.",
            "pilot time fallback este temporar si foloseste keyword-uri de timp pasiv din text.",
            "pilot nutrition overlay este temporar si nu rescrie recipe_nutrition_cache.",
        ]
    )
    return lines


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _format_reasons(value: object) -> str:
    if isinstance(value, list):
        return ";".join(str(item) for item in value)
    if value is None:
        return ""
    return str(value)


def _fmt(value: object, decimals: int = 1) -> str:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return "missing"
    return f"{float(numeric_value):.{decimals}f}"


def _clean_for_json(value: object) -> object:
    if isinstance(value, dict):
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
