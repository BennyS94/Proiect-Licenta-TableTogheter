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
from src.generator_v1.day_selector import select_one_day_plan
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
OUT_SUMMARY = OUT_DIR / "generator_v1_round15_balanced_selector_summary.txt"
OUT_PLANS = OUT_DIR / "generator_v1_round15_balanced_selector_plans.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round15_balanced_selector_meals.csv"
OUT_MACRO_GAPS = OUT_DIR / "generator_v1_round15_balanced_selector_macro_gaps.csv"
OUT_REPETITION = OUT_DIR / "generator_v1_round15_balanced_selector_recipe_repetition.csv"
OUT_COMPARISON = OUT_DIR / "generator_v1_round15_balanced_selector_comparison.csv"

PLAN_COLUMNS = [
    "scenario_id",
    "selection_mode",
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
    "carb_deficient",
    "protein_heavy",
    "fat_heavy",
    "good_macro_fit",
    "macro_gap_classes_json",
    "selected_recipes",
    "selected_snack_quality",
    "evaluated_combination_count",
    "shortlist_counts_json",
    "selector_warnings",
]

MEAL_COLUMNS = [
    "scenario_id",
    "selection_mode",
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
    "allowed_slots_json",
    "is_slot_suspicious",
    "is_nutrition_suspicious",
    "slot_fit_reasons",
    "slot_suspicion_reasons",
    "nutrition_quality_reasons",
    "snack_quality",
]

MACRO_GAP_COLUMNS = [
    "scenario_id",
    "selection_mode",
    "validation_status",
    "kcal_ratio",
    "protein_ratio",
    "carbs_ratio",
    "fat_ratio",
    "macro_gap_classes_json",
    "primary_macro_issue",
    "good_macro_fit",
    "protein_heavy",
    "carb_deficient",
    "fat_heavy",
]

REPETITION_COLUMNS = [
    "selection_mode",
    "recipe_id",
    "display_name",
    "recipe_kind",
    "selected_total_count",
    "breakfast_count",
    "lunch_count",
    "dinner_count",
    "snack_count",
    "scenario_ids",
]

COMPARISON_COLUMNS = [
    "scenario_id",
    "greedy_validation_status",
    "balanced_validation_status",
    "greedy_day_loss",
    "balanced_day_loss",
    "day_loss_improvement",
    "greedy_kcal_ratio",
    "balanced_kcal_ratio",
    "greedy_carbs_ratio",
    "balanced_carbs_ratio",
    "greedy_protein_ratio",
    "balanced_protein_ratio",
    "greedy_fat_ratio",
    "balanced_fat_ratio",
    "greedy_carb_deficient",
    "balanced_carb_deficient",
    "greedy_selected_recipes",
    "balanced_selected_recipes",
    "balanced_improved_day_loss",
    "balanced_improved_carbs_ratio",
    "balanced_validation_improved",
]


def main() -> None:
    base_profile = load_member_profile(BASE_PROFILE_PATH)
    fooddb = load_fooddb_current()
    plan_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []
    macro_gap_rows: list[dict[str, object]] = []
    comparison_rows: list[dict[str, object]] = []

    for overrides in scenario_overrides():
        scenario_id = clean_text(overrides["scenario_id"])
        profile = build_scenario_profile(base_profile, overrides)
        scenario_result = run_scenario(scenario_id, profile, fooddb)
        plan_rows.extend(scenario_result["plan_rows"])
        meal_rows.extend(scenario_result["meal_rows"])
        macro_gap_rows.extend(scenario_result["macro_gap_rows"])
        comparison_rows.append(scenario_result["comparison_row"])

    repetition_rows = build_repetition_rows(meal_rows)
    write_csv(OUT_PLANS, plan_rows, PLAN_COLUMNS)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    write_csv(OUT_MACRO_GAPS, macro_gap_rows, MACRO_GAP_COLUMNS)
    write_csv(OUT_REPETITION, repetition_rows, REPETITION_COLUMNS)
    write_csv(OUT_COMPARISON, comparison_rows, COMPARISON_COLUMNS)
    OUT_SUMMARY.write_text(
        build_summary(plan_rows, comparison_rows, repetition_rows),
        encoding="utf-8",
    )

    greedy_valid = count_matching(plan_rows, "greedy", "validation_status", "valid")
    balanced_valid = count_matching(plan_rows, "balanced_day", "validation_status", "valid")
    print("Generator v1 round15 balanced selector evaluation written")
    print(f"scenario_count={len(comparison_rows)}")
    print(f"valid_greedy={greedy_valid}")
    print(f"valid_balanced_day={balanced_valid}")
    print(f"written_summary={OUT_SUMMARY}")
    print(f"written_comparison={OUT_COMPARISON}")


def run_scenario(
    scenario_id: str,
    profile: dict[str, Any],
    fooddb: pd.DataFrame,
) -> dict[str, object]:
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    )
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
    )
    slots = slot_order(target)
    candidates_by_slot = slot_candidates_by_slot(slot_candidates, slots)
    candidate_lookup = build_candidate_lookup(slot_candidates)
    greedy_plan = select_one_day_plan(candidates_by_slot, slots)
    greedy_plan["selector_mode"] = "greedy"
    greedy_plan["selector_diagnostics"] = {"selector_mode": "greedy"}
    balanced_plan = select_one_day_plan_balanced(candidates_by_slot, target, slots)

    plans = {
        "greedy": finalize_plan(greedy_plan, target),
        "balanced_day": finalize_plan(balanced_plan, target),
    }
    plan_rows = [
        build_plan_row(scenario_id, mode, target, plan)
        for mode, plan in plans.items()
    ]
    meal_rows = [
        row
        for mode, plan in plans.items()
        for row in build_meal_rows(scenario_id, mode, plan, candidate_lookup)
    ]
    macro_gap_rows = [
        build_macro_gap_row(row)
        for row in plan_rows
    ]
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
    selection_mode: str,
    target: NutritionTarget,
    plan: dict[str, object],
) -> dict[str, object]:
    totals = plan.get("day_totals", {})
    validation = plan.get("validation", {})
    loss = plan.get("day_loss_components", {})
    ratios = macro_ratios(totals, target)
    classes = classify_macro_gaps(ratios, str(validation.get("validation_status", "")))
    diagnostics = plan.get("selector_diagnostics", {})
    selected_recipes = selected_recipes_text(plan)
    return {
        "scenario_id": scenario_id,
        "selection_mode": selection_mode,
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
        "carb_deficient": str("carb_deficient" in classes),
        "protein_heavy": str("protein_heavy" in classes),
        "fat_heavy": str("fat_heavy" in classes),
        "good_macro_fit": str("good_fit" in classes),
        "macro_gap_classes_json": json.dumps(classes),
        "selected_recipes": selected_recipes,
        "selected_snack_quality": selected_snack_quality(plan),
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
    selection_mode: str,
    plan: dict[str, object],
    candidate_lookup: dict[tuple[str, str, float], dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for meal in plan.get("selected_meals", []):
        key = (
            clean_text(meal.get("slot")),
            clean_text(meal.get("recipe_id")),
            round(to_float(meal.get("portion_multiplier")), 4),
        )
        candidate = candidate_lookup.get(key, {})
        rows.append(
            {
                "scenario_id": scenario_id,
                "selection_mode": selection_mode,
                "slot": clean_text(meal.get("slot")),
                "recipe_id": clean_text(meal.get("recipe_id")),
                "display_name": clean_text(meal.get("display_name")),
                "recipe_kind": clean_text(candidate.get("recipe_kind")),
                "recipe_category": clean_text(candidate.get("recipe_category")),
                "recipe_subcategory": clean_text(candidate.get("recipe_subcategory")),
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
                "allowed_slots_json": clean_text(candidate.get("allowed_slots_json")),
                "is_slot_suspicious": str(to_bool(meal.get("is_slot_suspicious"))),
                "is_nutrition_suspicious": str(to_bool(meal.get("is_nutrition_suspicious"))),
                "slot_fit_reasons": serialize_reason(meal.get("slot_fit_reasons")),
                "slot_suspicion_reasons": serialize_reason(meal.get("slot_suspicion_reasons")),
                "nutrition_quality_reasons": serialize_reason(meal.get("nutrition_quality_reasons")),
                "snack_quality": snack_quality(meal),
            }
        )
    return rows


def build_macro_gap_row(plan_row: dict[str, object]) -> dict[str, object]:
    classes = parse_classes(plan_row)
    return {
        "scenario_id": plan_row["scenario_id"],
        "selection_mode": plan_row["selection_mode"],
        "validation_status": plan_row["validation_status"],
        "kcal_ratio": plan_row["kcal_ratio"],
        "protein_ratio": plan_row["protein_ratio"],
        "carbs_ratio": plan_row["carbs_ratio"],
        "fat_ratio": plan_row["fat_ratio"],
        "macro_gap_classes_json": plan_row["macro_gap_classes_json"],
        "primary_macro_issue": classes[0] if classes else "",
        "good_macro_fit": str("good_fit" in classes),
        "protein_heavy": str("protein_heavy" in classes),
        "carb_deficient": str("carb_deficient" in classes),
        "fat_heavy": str("fat_heavy" in classes),
    }


def build_comparison_row(
    scenario_id: str,
    plan_rows: list[dict[str, object]],
) -> dict[str, object]:
    by_mode = {clean_text(row.get("selection_mode")): row for row in plan_rows}
    greedy = by_mode["greedy"]
    balanced = by_mode["balanced_day"]
    greedy_valid = clean_text(greedy.get("validation_status")) == "valid"
    balanced_valid = clean_text(balanced.get("validation_status")) == "valid"
    return {
        "scenario_id": scenario_id,
        "greedy_validation_status": greedy["validation_status"],
        "balanced_validation_status": balanced["validation_status"],
        "greedy_day_loss": greedy["day_loss"],
        "balanced_day_loss": balanced["day_loss"],
        "day_loss_improvement": round_number(to_float(greedy["day_loss"]) - to_float(balanced["day_loss"])),
        "greedy_kcal_ratio": greedy["kcal_ratio"],
        "balanced_kcal_ratio": balanced["kcal_ratio"],
        "greedy_carbs_ratio": greedy["carbs_ratio"],
        "balanced_carbs_ratio": balanced["carbs_ratio"],
        "greedy_protein_ratio": greedy["protein_ratio"],
        "balanced_protein_ratio": balanced["protein_ratio"],
        "greedy_fat_ratio": greedy["fat_ratio"],
        "balanced_fat_ratio": balanced["fat_ratio"],
        "greedy_carb_deficient": greedy["carb_deficient"],
        "balanced_carb_deficient": balanced["carb_deficient"],
        "greedy_selected_recipes": greedy["selected_recipes"],
        "balanced_selected_recipes": balanced["selected_recipes"],
        "balanced_improved_day_loss": str(to_float(balanced["day_loss"]) < to_float(greedy["day_loss"])),
        "balanced_improved_carbs_ratio": str(to_float(balanced["carbs_ratio"]) > to_float(greedy["carbs_ratio"])),
        "balanced_validation_improved": str((not greedy_valid) and balanced_valid),
    }


def build_repetition_rows(meal_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], dict[str, object]] = {}
    for meal in meal_rows:
        key = (clean_text(meal.get("selection_mode")), clean_text(meal.get("recipe_id")))
        if key not in grouped:
            grouped[key] = {
                "selection_mode": key[0],
                "recipe_id": key[1],
                "display_name": clean_text(meal.get("display_name")),
                "recipe_kind": clean_text(meal.get("recipe_kind")),
                "selected_total_count": 0,
                "breakfast_count": 0,
                "lunch_count": 0,
                "dinner_count": 0,
                "snack_count": 0,
                "scenario_ids": [],
            }
        row = grouped[key]
        row["selected_total_count"] = int(row["selected_total_count"]) + 1
        slot = clean_text(meal.get("slot"))
        if f"{slot}_count" in row:
            row[f"{slot}_count"] = int(row[f"{slot}_count"]) + 1
        scenario_id = clean_text(meal.get("scenario_id"))
        if scenario_id not in row["scenario_ids"]:
            row["scenario_ids"].append(scenario_id)
    rows = []
    for row in grouped.values():
        rows.append(
            {
                **row,
                "scenario_ids": "|".join(row["scenario_ids"]),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            clean_text(row.get("selection_mode")),
            -int(row.get("selected_total_count") or 0),
            clean_text(row.get("recipe_id")),
        ),
    )


def build_summary(
    plan_rows: list[dict[str, object]],
    comparison_rows: list[dict[str, object]],
    repetition_rows: list[dict[str, object]],
) -> str:
    scenario_count = len(comparison_rows)
    greedy_valid = count_matching(plan_rows, "greedy", "validation_status", "valid")
    balanced_valid = count_matching(plan_rows, "balanced_day", "validation_status", "valid")
    greedy_good = count_matching(plan_rows, "greedy", "good_macro_fit", "True")
    balanced_good = count_matching(plan_rows, "balanced_day", "good_macro_fit", "True")
    greedy_carb_def = count_matching(plan_rows, "greedy", "carb_deficient", "True")
    balanced_carb_def = count_matching(plan_rows, "balanced_day", "carb_deficient", "True")
    greedy_loss = average_metric(plan_rows, "greedy", "day_loss")
    balanced_loss = average_metric(plan_rows, "balanced_day", "day_loss")
    greedy_carbs = average_metric(plan_rows, "greedy", "carbs_ratio")
    balanced_carbs = average_metric(plan_rows, "balanced_day", "carbs_ratio")
    greedy_kcal = average_metric(plan_rows, "greedy", "kcal_ratio")
    balanced_kcal = average_metric(plan_rows, "balanced_day", "kcal_ratio")
    dominant_greedy = top_repetition(repetition_rows, "greedy")
    dominant_balanced = top_repetition(repetition_rows, "balanced_day")
    should_default = (
        balanced_valid >= greedy_valid
        and balanced_loss < greedy_loss
        and balanced_carb_def <= greedy_carb_def
    )
    lines = [
        "Generator v1 round15 balanced selector evaluation",
        "=" * 49,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        f"scenario_count: {scenario_count}",
        f"valid_scenarios_greedy: {greedy_valid}",
        f"valid_scenarios_balanced_day: {balanced_valid}",
        f"good_macro_fit_greedy: {greedy_good}",
        f"good_macro_fit_balanced_day: {balanced_good}",
        f"carb_deficient_greedy: {greedy_carb_def}",
        f"carb_deficient_balanced_day: {balanced_carb_def}",
        f"average_day_loss_greedy: {round_number(greedy_loss)}",
        f"average_day_loss_balanced_day: {round_number(balanced_loss)}",
        f"average_carbs_ratio_greedy: {round_number(greedy_carbs)}",
        f"average_carbs_ratio_balanced_day: {round_number(balanced_carbs)}",
        f"average_kcal_ratio_greedy: {round_number(greedy_kcal)}",
        f"average_kcal_ratio_balanced_day: {round_number(balanced_kcal)}",
        f"balanced_day_should_be_default_for_v1_1_testing: {should_default}",
        "",
        "Per-scenario comparison:",
    ]
    for row in comparison_rows:
        lines.append(
            "- "
            + clean_text(row.get("scenario_id"))
            + " | greedy="
            + clean_text(row.get("greedy_validation_status"))
            + " loss="
            + clean_text(row.get("greedy_day_loss"))
            + " carbs_ratio="
            + clean_text(row.get("greedy_carbs_ratio"))
            + " | balanced="
            + clean_text(row.get("balanced_validation_status"))
            + " loss="
            + clean_text(row.get("balanced_day_loss"))
            + " carbs_ratio="
            + clean_text(row.get("balanced_carbs_ratio"))
        )
    lines.extend(["", "Top repetition greedy:"])
    lines.extend(repetition_lines(dominant_greedy))
    lines.extend(["", "Top repetition balanced_day:"])
    lines.extend(repetition_lines(dominant_balanced))
    lines.extend(
        [
            "",
            "Conclusion:",
            "- balanced_day should be used for v1.1 testing if the above default flag is True.",
            "- greedy remains available and unchanged for baseline/pilot comparison.",
            "- residual risks: deterministic repetition still exists and dynamic portions are still limited to existing multipliers.",
            "",
            "Output files:",
            f"- {OUT_PLANS}",
            f"- {OUT_MEALS}",
            f"- {OUT_MACRO_GAPS}",
            f"- {OUT_REPETITION}",
            f"- {OUT_COMPARISON}",
        ]
    )
    return "\n".join(lines) + "\n"


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


def build_candidate_lookup(slot_candidates: pd.DataFrame) -> dict[tuple[str, str, float], dict[str, object]]:
    lookup = {}
    for _, row in slot_candidates.iterrows():
        key = (
            clean_text(row.get("slot")),
            clean_text(row.get("recipe_id")),
            round(to_float(row.get("portion_multiplier")), 4),
        )
        lookup[key] = row.to_dict()
    return lookup


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


def selected_snack_quality(plan: dict[str, object]) -> str:
    snacks = [
        snack_quality(meal)
        for meal in plan.get("selected_meals", [])
        if clean_text(meal.get("slot")) == "snack"
    ]
    return "|".join(snacks) if snacks else "no_snack_slot"


def snack_quality(meal: dict[str, object]) -> str:
    if clean_text(meal.get("slot")) != "snack":
        return "not_snack"
    recipe_id = clean_text(meal.get("recipe_id"))
    if recipe_id.startswith("manual_snack_v1_1_round12_"):
        return "manual_snack_ready"
    return "snack_review"


def parse_classes(row: dict[str, object]) -> list[str]:
    try:
        parsed = json.loads(clean_text(row.get("macro_gap_classes_json")) or "[]")
    except json.JSONDecodeError:
        return []
    return [clean_text(item) for item in parsed if clean_text(item)] if isinstance(parsed, list) else []


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


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return clean_text(value).lower() in {"1", "true", "yes", "y"}


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
    selection_mode: str,
    field: str,
    expected: str,
) -> int:
    return sum(
        1
        for row in rows
        if clean_text(row.get("selection_mode")) == selection_mode
        and clean_text(row.get(field)) == expected
    )


def average_metric(rows: list[dict[str, object]], selection_mode: str, field: str) -> float:
    values = [
        to_float(row.get(field))
        for row in rows
        if clean_text(row.get("selection_mode")) == selection_mode
    ]
    return sum(values) / len(values) if values else 0.0


def top_repetition(rows: list[dict[str, object]], selection_mode: str) -> list[dict[str, object]]:
    selected = [
        row
        for row in rows
        if clean_text(row.get("selection_mode")) == selection_mode
    ]
    return sorted(
        selected,
        key=lambda row: (-int(row.get("selected_total_count") or 0), clean_text(row.get("recipe_id"))),
    )[:8]


def repetition_lines(rows: list[dict[str, object]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- "
        + clean_text(row.get("recipe_id"))
        + " | "
        + clean_text(row.get("display_name"))
        + " | count="
        + clean_text(row.get("selected_total_count"))
        + " | slots="
        + slot_count_text(row)
        for row in rows
    ]


def slot_count_text(row: dict[str, object]) -> str:
    parts = []
    for slot in ("breakfast", "lunch", "dinner", "snack"):
        count = int(row.get(f"{slot}_count") or 0)
        if count:
            parts.append(f"{slot}:{count}")
    return ",".join(parts) if parts else "none"


if __name__ == "__main__":
    main()
