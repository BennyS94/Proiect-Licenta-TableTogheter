from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.generator_v1.candidate_filter import (
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.day_selector_balanced import select_one_day_plan_balanced
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target

try:
    from evaluate_generator_v1_round17_portion_policy import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        clean_text,
        classify_macro_gaps,
        macro_ratios,
        round_number,
        scenario_overrides,
        serialize_reason,
        slot_candidates_by_slot,
        slot_order,
        target_to_dict,
        to_float,
    )
except ImportError:
    from tools.extra.evaluate_generator_v1_round17_portion_policy import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        clean_text,
        classify_macro_gaps,
        macro_ratios,
        round_number,
        scenario_overrides,
        serialize_reason,
        slot_candidates_by_slot,
        slot_order,
        target_to_dict,
        to_float,
    )


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round20_practical_realism_summary.txt"
OUT_PLANS = OUT_DIR / "generator_v1_round20_practical_realism_plans.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round20_practical_realism_meals.csv"
OUT_REJECTIONS = OUT_DIR / "generator_v1_round20_practical_realism_rejections.csv"
OUT_COMPARISON = OUT_DIR / "generator_v1_round20_practical_realism_comparison.csv"

MEAL_REALISM_MODES = ["off", "soft", "practical"]
PORTION_POLICY = "target_aware"
SELECTION_MODE = "balanced_day"
ALTERNATIVE_COUNT = 3

PLAN_COLUMNS = [
    "scenario_id",
    "meal_realism_mode",
    "validation_status",
    "is_valid_for_checkpoint_1",
    "target_kcal",
    "target_protein_g",
    "target_carbs_g",
    "target_fat_g",
    "selected_total_kcal",
    "selected_total_protein_g",
    "selected_total_carbs_g",
    "selected_total_fat_g",
    "kcal_ratio",
    "protein_ratio",
    "carbs_ratio",
    "fat_ratio",
    "good_macro_fit",
    "carb_deficient",
    "base_day_loss",
    "adjusted_day_loss",
    "meal_realism_total_penalty",
    "meal_realism_applied_penalty",
    "breakfast_too_large_count",
    "borderline_large_portion_count",
    "unrealistic_large_portion_count",
    "low_protein_main_count",
    "low_carb_main_count",
    "selected_breakfast_max_kcal",
    "selected_breakfast_max_grams",
    "selected_snack_max_kcal",
    "selected_snack_max_grams",
    "hard_rejected_count_by_slot",
    "hard_reject_candidate_count_by_slot",
    "selected_recipes",
    "selected_recipe_names",
    "selector_warnings",
]

MEAL_COLUMNS = [
    "scenario_id",
    "meal_realism_mode",
    "slot",
    "recipe_id",
    "display_name",
    "recipe_kind",
    "portion_multiplier",
    "portion_grams_estimated",
    "kcal",
    "protein_g",
    "carbs_g",
    "fat_g",
    "meal_realism_score",
    "meal_realism_penalty",
    "meal_realism_flags",
    "meal_realism_reasons",
    "realism_hard_reject",
    "realism_reject_reason",
    "portion_policy_warnings",
    "total_time_min",
    "effective_time_min_for_scoring",
]

REJECTION_COLUMNS = [
    "scenario_id",
    "meal_realism_mode",
    "slot",
    "recipe_id",
    "display_name",
    "portion_multiplier",
    "portion_grams_estimated",
    "kcal",
    "realism_reject_reason",
    "meal_realism_flags",
]

COMPARISON_COLUMNS = [
    "scenario_id",
    "mode",
    "changed_vs_off",
    "base_day_loss_delta_vs_off",
    "adjusted_day_loss_delta_vs_off",
    "breakfast_too_large_delta_vs_off",
    "borderline_large_portion_delta_vs_off",
    "unrealistic_large_portion_delta_vs_off",
    "low_protein_main_delta_vs_off",
    "low_carb_main_delta_vs_off",
    "breakfast_max_kcal_delta_vs_off",
    "breakfast_max_grams_delta_vs_off",
    "snack_max_kcal_delta_vs_off",
    "snack_max_grams_delta_vs_off",
]


def main() -> None:
    base_profile = load_member_profile(BASE_PROFILE_PATH)
    fooddb = load_fooddb_current()
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    )

    plan_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []
    rejection_rows: list[dict[str, object]] = []

    for overrides in scenario_overrides():
        scenario_id = clean_text(overrides["scenario_id"])
        profile = build_scenario_profile(base_profile, overrides)
        scenario_result = run_scenario(
            scenario_id=scenario_id,
            profile=profile,
            pool=pool,
            fooddb=fooddb,
        )
        plan_rows.extend(scenario_result["plan_rows"])
        meal_rows.extend(scenario_result["meal_rows"])
        rejection_rows.extend(scenario_result["rejection_rows"])

    comparison_rows = build_comparison_rows(plan_rows)
    write_csv(OUT_PLANS, plan_rows, PLAN_COLUMNS)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    write_csv(OUT_REJECTIONS, rejection_rows, REJECTION_COLUMNS)
    write_csv(OUT_COMPARISON, comparison_rows, COMPARISON_COLUMNS)
    OUT_SUMMARY.write_text(
        build_summary(plan_rows, rejection_rows, comparison_rows),
        encoding="utf-8",
    )

    print("Generator v1 round20 practical realism evaluation written")
    print(f"plan_rows={len(plan_rows)}")
    print(f"meal_rows={len(meal_rows)}")
    print(f"rejection_rows={len(rejection_rows)}")
    for mode in MEAL_REALISM_MODES:
        summary = mode_summary(plan_rows, mode)
        print(
            f"{mode}: valid={summary['valid']}, "
            f"good_macro_fit={summary['good_macro_fit']}, "
            f"avg_base_day_loss={round_number(summary['avg_base_day_loss'])}, "
            f"breakfast_max_kcal={round_number(summary['breakfast_max_kcal'])}"
        )
    print(f"written_summary={OUT_SUMMARY}")


def run_scenario(
    scenario_id: str,
    profile: dict[str, Any],
    pool: object,
    fooddb: pd.DataFrame,
) -> dict[str, list[dict[str, object]]]:
    target = build_nutrition_target(profile)
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slots = slot_order(target)
    plan_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []
    rejection_rows: list[dict[str, object]] = []

    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode=PORTION_POLICY,
    )
    candidates_by_slot = slot_candidates_by_slot(slot_candidates, slots)

    for mode in MEAL_REALISM_MODES:
        plan = select_one_day_plan_balanced(
            slot_candidates_by_slot=candidates_by_slot,
            target=target,
            slot_order=slots,
            config={
                "return_alternatives": True,
                "alternative_count": ALTERNATIVE_COUNT,
                "diversity_mode": "none",
                "meal_realism_mode": mode,
            },
        )
        plan["target"] = target_to_dict(target)
        plan["validation"] = validate_one_day_plan(plan, target)
        plan_rows.append(build_plan_row(scenario_id, mode, target, plan))
        meal_rows.extend(build_meal_rows(scenario_id, mode, plan))
        rejection_rows.extend(build_rejection_rows(scenario_id, mode, plan))

    return {
        "plan_rows": plan_rows,
        "meal_rows": meal_rows,
        "rejection_rows": rejection_rows,
    }


def build_plan_row(
    scenario_id: str,
    mode: str,
    target: NutritionTarget,
    plan: dict[str, object],
) -> dict[str, object]:
    totals = plan.get("day_totals", {})
    validation = plan.get("validation", {})
    diagnostics = plan.get("selector_diagnostics", {})
    ratios = macro_ratios(totals, target)
    classes = classify_macro_gaps(ratios, clean_text(validation.get("validation_status")))
    flags = selected_flag_counter(plan)
    breakfast = meals_for_slot(plan, "breakfast")
    snacks = meals_for_slot(plan, "snack")
    return {
        "scenario_id": scenario_id,
        "meal_realism_mode": mode,
        "validation_status": clean_text(validation.get("validation_status")),
        "is_valid_for_checkpoint_1": str(bool(validation.get("is_valid_for_checkpoint_1", False))),
        "target_kcal": round_number(target.kcal),
        "target_protein_g": round_number(target.protein_g),
        "target_carbs_g": round_number(target.carbs_g),
        "target_fat_g": round_number(target.fat_g),
        "selected_total_kcal": round_number(totals.get("total_kcal")),
        "selected_total_protein_g": round_number(totals.get("total_protein_g")),
        "selected_total_carbs_g": round_number(totals.get("total_carbs_g")),
        "selected_total_fat_g": round_number(totals.get("total_fat_g")),
        "kcal_ratio": round_number(ratios["kcal_ratio"]),
        "protein_ratio": round_number(ratios["protein_ratio"]),
        "carbs_ratio": round_number(ratios["carbs_ratio"]),
        "fat_ratio": round_number(ratios["fat_ratio"]),
        "good_macro_fit": str("good_fit" in classes),
        "carb_deficient": str("carb_deficient" in classes),
        "base_day_loss": round_number(diag(diagnostics, "base_day_loss")),
        "adjusted_day_loss": round_number(diag(diagnostics, "adjusted_day_loss")),
        "meal_realism_total_penalty": round_number(diag(diagnostics, "meal_realism_total_penalty")),
        "meal_realism_applied_penalty": round_number(diag(diagnostics, "meal_realism_applied_penalty")),
        "breakfast_too_large_count": str(flags["breakfast_too_large"]),
        "borderline_large_portion_count": str(flags["borderline_large_portion"]),
        "unrealistic_large_portion_count": str(flags["unrealistic_large_portion"]),
        "low_protein_main_count": str(flags["low_protein_main"]),
        "low_carb_main_count": str(flags["low_carb_main"]),
        "selected_breakfast_max_kcal": round_number(max_value(breakfast, "kcal")),
        "selected_breakfast_max_grams": round_number(max_value(breakfast, "portion_grams_estimated")),
        "selected_snack_max_kcal": round_number(max_value(snacks, "kcal")),
        "selected_snack_max_grams": round_number(max_value(snacks, "portion_grams_estimated")),
        "hard_rejected_count_by_slot": json.dumps(
            diag(diagnostics, "hard_rejected_count_by_slot") or {},
            sort_keys=True,
        ),
        "hard_reject_candidate_count_by_slot": json.dumps(
            diag(diagnostics, "hard_reject_candidate_count_by_slot") or {},
            sort_keys=True,
        ),
        "selected_recipes": selected_recipes_text(plan),
        "selected_recipe_names": selected_recipe_names_text(plan),
        "selector_warnings": "|".join(
            clean_text(item)
            for item in (
                diagnostics.get("selector_warnings", [])
                if isinstance(diagnostics, dict)
                else []
            )
            if clean_text(item)
        ),
    }


def build_meal_rows(
    scenario_id: str,
    mode: str,
    plan: dict[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for meal in plan.get("selected_meals", []):
        rows.append(
            {
                "scenario_id": scenario_id,
                "meal_realism_mode": mode,
                "slot": clean_text(meal.get("slot")),
                "recipe_id": clean_text(meal.get("recipe_id")),
                "display_name": clean_text(meal.get("display_name")),
                "recipe_kind": clean_text(meal.get("recipe_kind")),
                "portion_multiplier": round_number(meal.get("portion_multiplier")),
                "portion_grams_estimated": round_number(meal.get("portion_grams_estimated")),
                "kcal": round_number(meal.get("kcal")),
                "protein_g": round_number(meal.get("protein_g")),
                "carbs_g": round_number(meal.get("carbs_g")),
                "fat_g": round_number(meal.get("fat_g")),
                "meal_realism_score": round_number(meal.get("meal_realism_score")),
                "meal_realism_penalty": round_number(meal.get("meal_realism_penalty")),
                "meal_realism_flags": serialize_reason(meal.get("meal_realism_flags")),
                "meal_realism_reasons": serialize_reason(meal.get("meal_realism_reasons")),
                "realism_hard_reject": str(bool(meal.get("realism_hard_reject", False))),
                "realism_reject_reason": serialize_reason(meal.get("realism_reject_reason")),
                "portion_policy_warnings": serialize_reason(meal.get("portion_policy_warnings")),
                "total_time_min": round_number(meal.get("total_time_min")),
                "effective_time_min_for_scoring": round_number(meal.get("effective_time_min_for_scoring")),
            }
        )
    return rows


def build_rejection_rows(
    scenario_id: str,
    mode: str,
    plan: dict[str, object],
) -> list[dict[str, object]]:
    diagnostics = plan.get("selector_diagnostics", {})
    if not isinstance(diagnostics, dict):
        return []
    reasons_by_slot = diagnostics.get("hard_reject_reasons", {})
    if not isinstance(reasons_by_slot, dict):
        return []
    rows: list[dict[str, object]] = []
    for slot, items in reasons_by_slot.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "meal_realism_mode": mode,
                    "slot": clean_text(slot),
                    "recipe_id": clean_text(item.get("recipe_id")),
                    "display_name": clean_text(item.get("display_name")),
                    "portion_multiplier": round_number(item.get("portion_multiplier")),
                    "portion_grams_estimated": round_number(item.get("portion_grams_estimated")),
                    "kcal": round_number(item.get("kcal")),
                    "realism_reject_reason": serialize_reason(
                        item.get("realism_reject_reason")
                    ),
                    "meal_realism_flags": serialize_reason(item.get("meal_realism_flags")),
                }
            )
    return rows


def build_comparison_rows(plan_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_key = {
        (clean_text(row.get("scenario_id")), clean_text(row.get("meal_realism_mode"))): row
        for row in plan_rows
    }
    rows: list[dict[str, object]] = []
    scenario_ids = sorted({scenario_id for scenario_id, _ in by_key})
    for scenario_id in scenario_ids:
        off = by_key.get((scenario_id, "off"))
        if off is None:
            continue
        for mode in ["soft", "practical"]:
            current = by_key.get((scenario_id, mode))
            if current is None:
                continue
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "mode": mode,
                    "changed_vs_off": str(
                        clean_text(current.get("selected_recipes"))
                        != clean_text(off.get("selected_recipes"))
                    ),
                    "base_day_loss_delta_vs_off": round_number(
                        to_float(current.get("base_day_loss"))
                        - to_float(off.get("base_day_loss"))
                    ),
                    "adjusted_day_loss_delta_vs_off": round_number(
                        to_float(current.get("adjusted_day_loss"))
                        - to_float(off.get("adjusted_day_loss"))
                    ),
                    "breakfast_too_large_delta_vs_off": round_number(
                        to_float(current.get("breakfast_too_large_count"))
                        - to_float(off.get("breakfast_too_large_count"))
                    ),
                    "borderline_large_portion_delta_vs_off": round_number(
                        to_float(current.get("borderline_large_portion_count"))
                        - to_float(off.get("borderline_large_portion_count"))
                    ),
                    "unrealistic_large_portion_delta_vs_off": round_number(
                        to_float(current.get("unrealistic_large_portion_count"))
                        - to_float(off.get("unrealistic_large_portion_count"))
                    ),
                    "low_protein_main_delta_vs_off": round_number(
                        to_float(current.get("low_protein_main_count"))
                        - to_float(off.get("low_protein_main_count"))
                    ),
                    "low_carb_main_delta_vs_off": round_number(
                        to_float(current.get("low_carb_main_count"))
                        - to_float(off.get("low_carb_main_count"))
                    ),
                    "breakfast_max_kcal_delta_vs_off": round_number(
                        to_float(current.get("selected_breakfast_max_kcal"))
                        - to_float(off.get("selected_breakfast_max_kcal"))
                    ),
                    "breakfast_max_grams_delta_vs_off": round_number(
                        to_float(current.get("selected_breakfast_max_grams"))
                        - to_float(off.get("selected_breakfast_max_grams"))
                    ),
                    "snack_max_kcal_delta_vs_off": round_number(
                        to_float(current.get("selected_snack_max_kcal"))
                        - to_float(off.get("selected_snack_max_kcal"))
                    ),
                    "snack_max_grams_delta_vs_off": round_number(
                        to_float(current.get("selected_snack_max_grams"))
                        - to_float(off.get("selected_snack_max_grams"))
                    ),
                }
            )
    return rows


def build_summary(
    plan_rows: list[dict[str, object]],
    rejection_rows: list[dict[str, object]],
    comparison_rows: list[dict[str, object]],
) -> str:
    scenario_count = len({clean_text(row.get("scenario_id")) for row in plan_rows})
    mode_summaries = {mode: mode_summary(plan_rows, mode) for mode in MEAL_REALISM_MODES}
    practical = mode_summaries["practical"]
    off = mode_summaries["off"]
    practical_changed = sum(
        1
        for row in comparison_rows
        if clean_text(row.get("mode")) == "practical"
        and clean_text(row.get("changed_vs_off")) == "True"
    )
    practical_rejections = sum(
        1
        for row in rejection_rows
        if clean_text(row.get("meal_realism_mode")) == "practical"
    )
    if practical["breakfast_too_large"] == 0 and practical["valid"] == off["valid"]:
        recommendation = "practical_recommended_for_v1_1_testing"
    elif practical["valid"] < off["valid"]:
        recommendation = "practical_too_strict_for_default_testing"
    else:
        recommendation = "practical_needs_more_calibration"

    lines = [
        "Generator v1 round20 practical realism evaluation",
        "=" * 55,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        f"selection_mode: {SELECTION_MODE}",
        f"portion_policy: {PORTION_POLICY}",
        f"alternative_count: {ALTERNATIVE_COUNT}",
        f"scenario_count: {scenario_count}",
        "",
        "Mode results:",
    ]
    for mode in MEAL_REALISM_MODES:
        summary = mode_summaries[mode]
        lines.append(
            "- "
            + mode
            + " | valid="
            + str(summary["valid"])
            + " | good_macro_fit="
            + str(summary["good_macro_fit"])
            + " | carb_deficient="
            + str(summary["carb_deficient"])
            + " | avg_base_day_loss="
            + round_number(summary["avg_base_day_loss"])
            + " | avg_adjusted_day_loss="
            + round_number(summary["avg_adjusted_day_loss"])
            + " | breakfast_too_large="
            + str(summary["breakfast_too_large"])
            + " | borderline_large_portion="
            + str(summary["borderline_large_portion"])
            + " | unrealistic_large_portion="
            + str(summary["unrealistic_large_portion"])
            + " | low_protein_main="
            + str(summary["low_protein_main"])
            + " | low_carb_main="
            + str(summary["low_carb_main"])
            + " | breakfast_max_kcal="
            + round_number(summary["breakfast_max_kcal"])
            + " | breakfast_max_grams="
            + round_number(summary["breakfast_max_grams"])
            + " | snack_max_kcal="
            + round_number(summary["snack_max_kcal"])
            + " | snack_max_grams="
            + round_number(summary["snack_max_grams"])
        )

    lines.extend(
        [
            "",
            "Practical impact:",
            f"- practical_changed_scenarios_vs_off: {practical_changed}",
            f"- practical_hard_rejection_rows: {practical_rejections}",
            f"- recommendation: {recommendation}",
            "- practical should be used for v1.1 demo/testing if it removes heavy breakfasts without invalidating scenarios.",
            "- validation_status valid is still insufficient by itself; meal realism flags remain mandatory QA.",
            "",
            "Output files:",
            f"- {OUT_PLANS}",
            f"- {OUT_MEALS}",
            f"- {OUT_REJECTIONS}",
            f"- {OUT_COMPARISON}",
        ]
    )
    return "\n".join(lines) + "\n"


def mode_summary(rows: list[dict[str, object]], mode: str) -> dict[str, object]:
    mode_rows = [
        row
        for row in rows
        if clean_text(row.get("meal_realism_mode")) == mode
    ]
    return {
        "valid": count_matching(mode_rows, "validation_status", "valid"),
        "good_macro_fit": count_matching(mode_rows, "good_macro_fit", "True"),
        "carb_deficient": count_matching(mode_rows, "carb_deficient", "True"),
        "avg_base_day_loss": average_field(mode_rows, "base_day_loss"),
        "avg_adjusted_day_loss": average_field(mode_rows, "adjusted_day_loss"),
        "breakfast_too_large": sum_field(mode_rows, "breakfast_too_large_count"),
        "borderline_large_portion": sum_field(mode_rows, "borderline_large_portion_count"),
        "unrealistic_large_portion": sum_field(mode_rows, "unrealistic_large_portion_count"),
        "low_protein_main": sum_field(mode_rows, "low_protein_main_count"),
        "low_carb_main": sum_field(mode_rows, "low_carb_main_count"),
        "breakfast_max_kcal": max_field(mode_rows, "selected_breakfast_max_kcal"),
        "breakfast_max_grams": max_field(mode_rows, "selected_breakfast_max_grams"),
        "snack_max_kcal": max_field(mode_rows, "selected_snack_max_kcal"),
        "snack_max_grams": max_field(mode_rows, "selected_snack_max_grams"),
    }


def selected_flag_counter(plan: dict[str, object]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for meal in plan.get("selected_meals", []):
        for flag in reason_items(meal.get("meal_realism_flags")):
            counter[flag] += 1
    return counter


def meals_for_slot(plan: dict[str, object], slot: str) -> list[dict[str, object]]:
    return [
        meal
        for meal in plan.get("selected_meals", [])
        if clean_text(meal.get("slot")) == slot
    ]


def max_value(rows: list[dict[str, object]], field: str) -> float:
    values = [to_float(row.get(field)) for row in rows]
    return max(values) if values else 0.0


def reason_items(value: object) -> list[str]:
    if isinstance(value, list):
        return [clean_text(item) for item in value if clean_text(item)]
    if isinstance(value, tuple):
        return [clean_text(item) for item in value if clean_text(item)]
    text = clean_text(value)
    if not text:
        return []
    if "|" in text:
        return [item.strip() for item in text.split("|") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text]


def selected_recipes_text(plan: dict[str, object]) -> str:
    return "|".join(
        clean_text(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if clean_text(meal.get("recipe_id"))
    )


def selected_recipe_names_text(plan: dict[str, object]) -> str:
    return "|".join(
        clean_text(meal.get("display_name"))
        for meal in plan.get("selected_meals", [])
        if clean_text(meal.get("display_name"))
    )


def count_matching(rows: list[dict[str, object]], field: str, expected: str) -> int:
    return sum(1 for row in rows if clean_text(row.get(field)) == expected)


def average_field(rows: list[dict[str, object]], field: str) -> float:
    values = [to_float(row.get(field)) for row in rows]
    return sum(values) / len(values) if values else 0.0


def sum_field(rows: list[dict[str, object]], field: str) -> int:
    return int(sum(to_float(row.get(field)) for row in rows))


def max_field(rows: list[dict[str, object]], field: str) -> float:
    values = [to_float(row.get(field)) for row in rows]
    return max(values) if values else 0.0


def diag(diagnostics: object, field: str) -> object:
    if not isinstance(diagnostics, dict):
        return ""
    return diagnostics.get(field, "")


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
