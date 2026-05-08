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
from src.generator_v1.day_selector_balanced import (
    compute_day_loss_for_plan,
    select_one_day_plan_balanced,
)
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target

try:
    from evaluate_generator_v1_round13_scenarios import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        scenario_overrides,
    )
except ImportError:
    from tools.extra.evaluate_generator_v1_round13_scenarios import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        scenario_overrides,
    )


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round17_portion_policy_summary.txt"
OUT_PLANS = OUT_DIR / "generator_v1_round17_portion_policy_plans.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round17_portion_policy_meals.csv"
OUT_MACRO_GAPS = OUT_DIR / "generator_v1_round17_portion_policy_macro_gaps.csv"
OUT_COMPARISON = OUT_DIR / "generator_v1_round17_portion_policy_comparison.csv"

PORTION_POLICIES = ["standard", "expanded_safe", "target_aware"]

PLAN_COLUMNS = [
    "scenario_id",
    "portion_policy",
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
    "day_loss",
    "macro_day_loss",
    "kcal_loss",
    "protein_loss",
    "carbs_loss",
    "fat_loss",
    "effective_time_min_sum",
    "total_time_min_sum",
    "selected_slot_count",
    "expected_slot_count",
    "good_macro_fit",
    "carb_deficient",
    "protein_heavy",
    "fat_heavy",
    "macro_gap_classes_json",
    "selected_recipes",
    "selected_portion_multipliers",
    "selected_portion_grams",
    "max_selected_portion_multiplier",
    "portion_policy_warning_count",
    "portion_policy_warnings",
    "evaluated_combination_count",
    "shortlist_counts_json",
    "selector_warnings",
]

MEAL_COLUMNS = [
    "scenario_id",
    "portion_policy",
    "slot",
    "recipe_id",
    "display_name",
    "recipe_kind",
    "recipe_category",
    "recipe_subcategory",
    "portion_multiplier",
    "portion_grams_estimated",
    "kcal",
    "protein_g",
    "carbs_g",
    "fat_g",
    "score_preview",
    "macro_fit",
    "time_fit",
    "slot_fit",
    "nutrition_quality",
    "total_time_min",
    "effective_time_min_for_scoring",
    "portion_policy_mode",
    "portion_policy_reasons",
    "portion_policy_warnings",
    "has_portion_policy_warning",
]

MACRO_GAP_COLUMNS = [
    "scenario_id",
    "portion_policy",
    "validation_status",
    "kcal_ratio",
    "protein_ratio",
    "carbs_ratio",
    "fat_ratio",
    "macro_gap_classes_json",
    "primary_macro_issue",
    "good_macro_fit",
    "carb_deficient",
    "protein_heavy",
    "fat_heavy",
]

COMPARISON_COLUMNS = [
    "scenario_id",
    "standard_validation_status",
    "expanded_safe_validation_status",
    "target_aware_validation_status",
    "standard_day_loss",
    "expanded_safe_day_loss",
    "target_aware_day_loss",
    "expanded_safe_day_loss_delta_vs_standard",
    "target_aware_day_loss_delta_vs_standard",
    "standard_kcal_ratio",
    "expanded_safe_kcal_ratio",
    "target_aware_kcal_ratio",
    "standard_carbs_ratio",
    "expanded_safe_carbs_ratio",
    "target_aware_carbs_ratio",
    "standard_good_macro_fit",
    "expanded_safe_good_macro_fit",
    "target_aware_good_macro_fit",
    "standard_carb_deficient",
    "expanded_safe_carb_deficient",
    "target_aware_carb_deficient",
    "standard_warning_count",
    "expanded_safe_warning_count",
    "target_aware_warning_count",
    "standard_selected_recipes",
    "expanded_safe_selected_recipes",
    "target_aware_selected_recipes",
    "standard_portions",
    "expanded_safe_portions",
    "target_aware_portions",
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
    macro_gap_rows: list[dict[str, object]] = []
    comparison_rows: list[dict[str, object]] = []

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
        macro_gap_rows.extend(scenario_result["macro_gap_rows"])
        comparison_rows.append(scenario_result["comparison_row"])

    write_csv(OUT_PLANS, plan_rows, PLAN_COLUMNS)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    write_csv(OUT_MACRO_GAPS, macro_gap_rows, MACRO_GAP_COLUMNS)
    write_csv(OUT_COMPARISON, comparison_rows, COMPARISON_COLUMNS)
    OUT_SUMMARY.write_text(
        build_summary(plan_rows, meal_rows, comparison_rows),
        encoding="utf-8",
    )

    print("Generator v1 round17 portion policy evaluation written")
    print(f"scenario_count={len(comparison_rows)}")
    for policy in PORTION_POLICIES:
        print(
            f"{policy}: valid={count_matching(plan_rows, policy, 'validation_status', 'valid')}, "
            f"good_macro_fit={count_matching(plan_rows, policy, 'good_macro_fit', 'True')}, "
            f"carb_deficient={count_matching(plan_rows, policy, 'carb_deficient', 'True')}, "
            f"avg_day_loss={round_number(average_metric(plan_rows, policy, 'day_loss'))}"
        )
    print(f"written_summary={OUT_SUMMARY}")
    print(f"written_comparison={OUT_COMPARISON}")


def run_scenario(
    scenario_id: str,
    profile: dict[str, Any],
    pool: object,
    fooddb: pd.DataFrame,
) -> dict[str, object]:
    target = build_nutrition_target(profile)
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )

    plan_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []
    macro_gap_rows: list[dict[str, object]] = []

    for policy in PORTION_POLICIES:
        slot_candidates = build_slot_candidates(
            target=target,
            filtered_candidates=filtered_candidates,
            time_sensitivity=preference_context.time_sensitivity,
            ingredients=pool.ingredients,
            fooddb=fooddb,
            portion_policy_mode=policy,
        )
        slots = slot_order(target)
        plan = select_one_day_plan_balanced(
            slot_candidates_by_slot=slot_candidates_by_slot(slot_candidates, slots),
            target=target,
            slot_order=slots,
        )
        plan = finalize_plan(plan, target)
        plan_row = build_plan_row(scenario_id, policy, target, plan)
        plan_rows.append(plan_row)
        meal_rows.extend(build_meal_rows(scenario_id, policy, plan))
        macro_gap_rows.append(build_macro_gap_row(plan_row))

    return {
        "plan_rows": plan_rows,
        "meal_rows": meal_rows,
        "macro_gap_rows": macro_gap_rows,
        "comparison_row": build_comparison_row(scenario_id, plan_rows),
    }


def finalize_plan(plan: dict[str, object], target: NutritionTarget) -> dict[str, object]:
    plan["target"] = target_to_dict(target)
    plan["validation"] = validate_one_day_plan(plan, target)
    plan["day_loss_components"] = compute_day_loss_for_plan(
        plan.get("selected_meals", []),
        target,
    )
    return plan


def build_plan_row(
    scenario_id: str,
    portion_policy: str,
    target: NutritionTarget,
    plan: dict[str, object],
) -> dict[str, object]:
    totals = plan.get("day_totals", {})
    validation = plan.get("validation", {})
    loss = plan.get("day_loss_components", {})
    ratios = macro_ratios(totals, target)
    classes = classify_macro_gaps(ratios, clean_text(validation.get("validation_status")))
    diagnostics = plan.get("selector_diagnostics", {})
    warnings = selected_portion_warnings(plan)
    return {
        "scenario_id": scenario_id,
        "portion_policy": portion_policy,
        "validation_status": validation.get("validation_status", ""),
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
        "day_loss": round_number(loss.get("day_loss")),
        "macro_day_loss": round_number(loss.get("macro_day_loss")),
        "kcal_loss": round_number(loss.get("kcal_loss")),
        "protein_loss": round_number(loss.get("protein_loss")),
        "carbs_loss": round_number(loss.get("carbs_loss")),
        "fat_loss": round_number(loss.get("fat_loss")),
        "effective_time_min_sum": round_number(totals.get("effective_time_min_sum")),
        "total_time_min_sum": round_number(totals.get("total_time_min_sum")),
        "selected_slot_count": clean_text(totals.get("selected_slot_count")),
        "expected_slot_count": str(len(target.slot_targets)),
        "good_macro_fit": str("good_fit" in classes),
        "carb_deficient": str("carb_deficient" in classes),
        "protein_heavy": str("protein_heavy" in classes),
        "fat_heavy": str("fat_heavy" in classes),
        "macro_gap_classes_json": json.dumps(classes),
        "selected_recipes": selected_recipes_text(plan),
        "selected_portion_multipliers": selected_portions_text(plan),
        "selected_portion_grams": selected_portion_grams_text(plan),
        "max_selected_portion_multiplier": round_number(max_selected_portion(plan)),
        "portion_policy_warning_count": str(len(warnings)),
        "portion_policy_warnings": "|".join(warnings),
        "evaluated_combination_count": clean_text(
            diagnostics.get("evaluated_combination_count")
            if isinstance(diagnostics, dict)
            else ""
        ),
        "shortlist_counts_json": json.dumps(
            diagnostics.get("candidate_count_per_slot_after_shortlist", {})
            if isinstance(diagnostics, dict)
            else {},
            sort_keys=True,
        ),
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
    portion_policy: str,
    plan: dict[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for meal in plan.get("selected_meals", []):
        warnings = serialize_reason(meal.get("portion_policy_warnings"))
        rows.append(
            {
                "scenario_id": scenario_id,
                "portion_policy": portion_policy,
                "slot": clean_text(meal.get("slot")),
                "recipe_id": clean_text(meal.get("recipe_id")),
                "display_name": clean_text(meal.get("display_name")),
                "recipe_kind": clean_text(meal.get("recipe_kind")),
                "recipe_category": clean_text(meal.get("recipe_category")),
                "recipe_subcategory": clean_text(meal.get("recipe_subcategory")),
                "portion_multiplier": round_number(meal.get("portion_multiplier")),
                "portion_grams_estimated": round_number(meal.get("portion_grams_estimated")),
                "kcal": round_number(meal.get("kcal")),
                "protein_g": round_number(meal.get("protein_g")),
                "carbs_g": round_number(meal.get("carbs_g")),
                "fat_g": round_number(meal.get("fat_g")),
                "score_preview": round_number(meal.get("score_preview")),
                "macro_fit": round_number(meal.get("macro_fit")),
                "time_fit": round_number(meal.get("time_fit")),
                "slot_fit": round_number(meal.get("slot_fit")),
                "nutrition_quality": round_number(meal.get("nutrition_quality")),
                "total_time_min": round_number(meal.get("total_time_min")),
                "effective_time_min_for_scoring": round_number(meal.get("effective_time_min_for_scoring")),
                "portion_policy_mode": clean_text(meal.get("portion_policy_mode")),
                "portion_policy_reasons": serialize_reason(meal.get("portion_policy_reasons")),
                "portion_policy_warnings": warnings,
                "has_portion_policy_warning": str(bool(warnings)),
            }
        )
    return rows


def build_macro_gap_row(plan_row: dict[str, object]) -> dict[str, object]:
    classes = parse_classes(plan_row)
    return {
        "scenario_id": plan_row["scenario_id"],
        "portion_policy": plan_row["portion_policy"],
        "validation_status": plan_row["validation_status"],
        "kcal_ratio": plan_row["kcal_ratio"],
        "protein_ratio": plan_row["protein_ratio"],
        "carbs_ratio": plan_row["carbs_ratio"],
        "fat_ratio": plan_row["fat_ratio"],
        "macro_gap_classes_json": plan_row["macro_gap_classes_json"],
        "primary_macro_issue": classes[0] if classes else "",
        "good_macro_fit": str("good_fit" in classes),
        "carb_deficient": str("carb_deficient" in classes),
        "protein_heavy": str("protein_heavy" in classes),
        "fat_heavy": str("fat_heavy" in classes),
    }


def build_comparison_row(
    scenario_id: str,
    plan_rows: list[dict[str, object]],
) -> dict[str, object]:
    by_policy = {clean_text(row.get("portion_policy")): row for row in plan_rows}
    standard = by_policy["standard"]
    expanded = by_policy["expanded_safe"]
    target_aware = by_policy["target_aware"]
    return {
        "scenario_id": scenario_id,
        "standard_validation_status": standard["validation_status"],
        "expanded_safe_validation_status": expanded["validation_status"],
        "target_aware_validation_status": target_aware["validation_status"],
        "standard_day_loss": standard["day_loss"],
        "expanded_safe_day_loss": expanded["day_loss"],
        "target_aware_day_loss": target_aware["day_loss"],
        "expanded_safe_day_loss_delta_vs_standard": round_number(
            to_float(expanded["day_loss"]) - to_float(standard["day_loss"])
        ),
        "target_aware_day_loss_delta_vs_standard": round_number(
            to_float(target_aware["day_loss"]) - to_float(standard["day_loss"])
        ),
        "standard_kcal_ratio": standard["kcal_ratio"],
        "expanded_safe_kcal_ratio": expanded["kcal_ratio"],
        "target_aware_kcal_ratio": target_aware["kcal_ratio"],
        "standard_carbs_ratio": standard["carbs_ratio"],
        "expanded_safe_carbs_ratio": expanded["carbs_ratio"],
        "target_aware_carbs_ratio": target_aware["carbs_ratio"],
        "standard_good_macro_fit": standard["good_macro_fit"],
        "expanded_safe_good_macro_fit": expanded["good_macro_fit"],
        "target_aware_good_macro_fit": target_aware["good_macro_fit"],
        "standard_carb_deficient": standard["carb_deficient"],
        "expanded_safe_carb_deficient": expanded["carb_deficient"],
        "target_aware_carb_deficient": target_aware["carb_deficient"],
        "standard_warning_count": standard["portion_policy_warning_count"],
        "expanded_safe_warning_count": expanded["portion_policy_warning_count"],
        "target_aware_warning_count": target_aware["portion_policy_warning_count"],
        "standard_selected_recipes": standard["selected_recipes"],
        "expanded_safe_selected_recipes": expanded["selected_recipes"],
        "target_aware_selected_recipes": target_aware["selected_recipes"],
        "standard_portions": standard["selected_portion_multipliers"],
        "expanded_safe_portions": expanded["selected_portion_multipliers"],
        "target_aware_portions": target_aware["selected_portion_multipliers"],
    }


def build_summary(
    plan_rows: list[dict[str, object]],
    meal_rows: list[dict[str, object]],
    comparison_rows: list[dict[str, object]],
) -> str:
    scenario_count = len(comparison_rows)
    policy_summaries = {
        policy: {
            "valid": count_matching(plan_rows, policy, "validation_status", "valid"),
            "good": count_matching(plan_rows, policy, "good_macro_fit", "True"),
            "carb_def": count_matching(plan_rows, policy, "carb_deficient", "True"),
            "day_loss": average_metric(plan_rows, policy, "day_loss"),
            "kcal_ratio": average_metric(plan_rows, policy, "kcal_ratio"),
            "carbs_ratio": average_metric(plan_rows, policy, "carbs_ratio"),
            "protein_ratio": average_metric(plan_rows, policy, "protein_ratio"),
            "fat_ratio": average_metric(plan_rows, policy, "fat_ratio"),
            "warnings": sum_metric(plan_rows, policy, "portion_policy_warning_count"),
        }
        for policy in PORTION_POLICIES
    }
    recommendation = recommend_policy(policy_summaries)
    lines = [
        "Generator v1 round17 portion policy evaluation",
        "=" * 50,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        f"selection_mode: balanced_day",
        f"scenario_count: {scenario_count}",
        "",
        "Policy results:",
    ]
    for policy in PORTION_POLICIES:
        summary = policy_summaries[policy]
        lines.append(
            "- "
            + policy
            + " | valid="
            + str(summary["valid"])
            + " | good_macro_fit="
            + str(summary["good"])
            + " | carb_deficient="
            + str(summary["carb_def"])
            + " | avg_day_loss="
            + round_number(summary["day_loss"])
            + " | avg_kcal_ratio="
            + round_number(summary["kcal_ratio"])
            + " | avg_carbs_ratio="
            + round_number(summary["carbs_ratio"])
            + " | avg_protein_ratio="
            + round_number(summary["protein_ratio"])
            + " | avg_fat_ratio="
            + round_number(summary["fat_ratio"])
            + " | portion_warnings="
            + str(summary["warnings"])
        )

    lines.extend(["", "Top recipe repetition by policy:"])
    for policy in PORTION_POLICIES:
        lines.append(f"- {policy}: " + top_repetition_text(meal_rows, policy))

    lines.extend(
        [
            "",
            "Recommendation:",
            f"- recommended_portion_policy_for_v1_1_testing: {recommendation}",
            "- dynamic portions remain optional and are not the pilot_current default.",
            "- warnings count selected meals near conservative caps or with large multipliers; cap violations are excluded before selection.",
            "",
            "Output files:",
            f"- {OUT_PLANS}",
            f"- {OUT_MEALS}",
            f"- {OUT_MACRO_GAPS}",
            f"- {OUT_COMPARISON}",
        ]
    )
    return "\n".join(lines) + "\n"


def recommend_policy(policy_summaries: dict[str, dict[str, float]]) -> str:
    target = policy_summaries["target_aware"]
    expanded = policy_summaries["expanded_safe"]
    standard = policy_summaries["standard"]
    if (
        target["good"] >= expanded["good"]
        and target["good"] >= standard["good"]
        and target["carb_def"] <= expanded["carb_def"]
        and target["day_loss"] <= expanded["day_loss"] + 0.005
        and target["warnings"] <= 30
    ):
        return "target_aware"
    if (
        expanded["good"] >= standard["good"]
        and expanded["day_loss"] <= standard["day_loss"]
        and expanded["warnings"] <= 30
    ):
        return "expanded_safe"
    return "standard"


def slot_order(target: NutritionTarget) -> list[str]:
    preferred = ["breakfast", "lunch", "dinner", "snack"]
    return [slot for slot in preferred if slot in target.slot_targets] + [
        slot for slot in target.slot_targets if slot not in preferred
    ]


def slot_candidates_by_slot(slot_candidates: pd.DataFrame, slots: list[str]) -> dict[str, pd.DataFrame]:
    return {
        slot: slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        for slot in slots
    }


def target_to_dict(target: NutritionTarget) -> dict[str, object]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def macro_ratios(totals: dict[str, object], target: NutritionTarget) -> dict[str, float]:
    return {
        "kcal_ratio": safe_ratio(totals.get("total_kcal"), target.kcal),
        "protein_ratio": safe_ratio(totals.get("total_protein_g"), target.protein_g),
        "carbs_ratio": safe_ratio(totals.get("total_carbs_g"), target.carbs_g),
        "fat_ratio": safe_ratio(totals.get("total_fat_g"), target.fat_g),
    }


def classify_macro_gaps(ratios: dict[str, float], validation_status: str) -> list[str]:
    classes = []
    if ratios["kcal_ratio"] < 0.85:
        classes.append("kcal_low")
    elif ratios["kcal_ratio"] > 1.15:
        classes.append("kcal_high")
    if ratios["protein_ratio"] < 0.90:
        classes.append("protein_low")
    elif ratios["protein_ratio"] > 1.60:
        classes.append("protein_high")
    if ratios["carbs_ratio"] < 0.70:
        classes.append("carbs_low")
    elif ratios["carbs_ratio"] > 1.30:
        classes.append("carbs_high")
    if ratios["fat_ratio"] < 0.70:
        classes.append("fat_low")
    elif ratios["fat_ratio"] > 1.40:
        classes.append("fat_high")
    if ratios["protein_ratio"] > 1.60 and ratios["carbs_ratio"] < 0.80:
        classes.append("protein_heavy")
    if ratios["carbs_ratio"] < 0.70:
        classes.append("carb_deficient")
    if ratios["fat_ratio"] > 1.40:
        classes.append("fat_heavy")
    if not classes and validation_status == "valid":
        classes.append("good_fit")
    elif not classes:
        classes.append("near_fit")
    return classes


def selected_recipes_text(plan: dict[str, object]) -> str:
    return "|".join(
        clean_text(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if clean_text(meal.get("recipe_id"))
    )


def selected_portions_text(plan: dict[str, object]) -> str:
    return "|".join(
        round_number(meal.get("portion_multiplier"))
        for meal in plan.get("selected_meals", [])
    )


def selected_portion_grams_text(plan: dict[str, object]) -> str:
    return "|".join(
        round_number(meal.get("portion_grams_estimated"))
        for meal in plan.get("selected_meals", [])
    )


def max_selected_portion(plan: dict[str, object]) -> float:
    values = [
        to_float(meal.get("portion_multiplier"))
        for meal in plan.get("selected_meals", [])
    ]
    return max(values) if values else 0.0


def selected_portion_warnings(plan: dict[str, object]) -> list[str]:
    warnings = []
    for meal in plan.get("selected_meals", []):
        warning_text = serialize_reason(meal.get("portion_policy_warnings"))
        if warning_text:
            warnings.append(
                clean_text(meal.get("slot"))
                + ":"
                + clean_text(meal.get("recipe_id"))
                + ":"
                + warning_text
            )
    return warnings


def parse_classes(row: dict[str, object]) -> list[str]:
    try:
        parsed = json.loads(clean_text(row.get("macro_gap_classes_json")) or "[]")
    except json.JSONDecodeError:
        return []
    return [clean_text(item) for item in parsed if clean_text(item)] if isinstance(parsed, list) else []


def top_repetition_text(meal_rows: list[dict[str, object]], policy: str) -> str:
    counter: Counter[tuple[str, str]] = Counter()
    for meal in meal_rows:
        if clean_text(meal.get("portion_policy")) != policy:
            continue
        counter[(clean_text(meal.get("recipe_id")), clean_text(meal.get("display_name")))] += 1
    top = counter.most_common(5)
    if not top:
        return "none"
    return "; ".join(f"{recipe_id} ({name}) x{count}" for (recipe_id, name), count in top)


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def serialize_reason(value: object) -> str:
    if isinstance(value, list):
        return "|".join(clean_text(item) for item in value if clean_text(item))
    return clean_text(value)


def safe_ratio(actual: object, target: object) -> float:
    target_value = to_float(target)
    if target_value <= 0:
        return 0.0
    return to_float(actual) / target_value


def to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def round_number(value: object) -> str:
    numeric = to_float(value)
    text = f"{numeric:.4f}".rstrip("0").rstrip(".")
    return text or "0"


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def count_matching(
    rows: list[dict[str, object]],
    portion_policy: str,
    field: str,
    expected: str,
) -> int:
    return sum(
        1
        for row in rows
        if clean_text(row.get("portion_policy")) == portion_policy
        and clean_text(row.get(field)) == expected
    )


def average_metric(rows: list[dict[str, object]], portion_policy: str, field: str) -> float:
    values = [
        to_float(row.get(field))
        for row in rows
        if clean_text(row.get("portion_policy")) == portion_policy
    ]
    return sum(values) / len(values) if values else 0.0


def sum_metric(rows: list[dict[str, object]], portion_policy: str, field: str) -> int:
    return sum(
        int(to_float(row.get(field)))
        for row in rows
        if clean_text(row.get("portion_policy")) == portion_policy
    )


if __name__ == "__main__":
    main()
