from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.generator_v1.data_loader import (
    DEFAULT_INGREDIENTS_PATH,
    DEFAULT_NUTRITION_PATH,
    DEFAULT_RECIPES_PATH,
    load_recipe_candidate_pool,
)
from src.generator_v1.candidate_filter import (
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.candidate_diagnostics import build_candidate_diagnostics
from src.generator_v1.day_selector import select_one_day_plan
from src.generator_v1.plan_audit import (
    write_plan_csv,
    write_plan_json,
    write_plan_readable,
)
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


def main() -> None:
    args = _parse_args()
    profile = load_member_profile(args.profile)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=args.recipes,
        ingredients_path=args.ingredients,
        nutrition_path=args.nutrition,
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
    )
    candidate_diagnostics = build_candidate_diagnostics(
        slot_candidates=slot_candidates,
        slot_targets=target.slot_targets,
    )

    _print_target_summary(target)
    _print_pool_summary(pool.candidates, pool.eligible_candidates)
    _print_slot_candidate_summary(filtered_candidates, slot_candidates)
    _print_candidate_diagnostics(candidate_diagnostics)
    plan = select_one_day_plan(
        slot_candidates_by_slot=_slot_candidates_by_slot(slot_candidates, _slot_order(target)),
        slot_order=_slot_order(target),
    )
    plan["target"] = _target_to_dict(target)
    plan["candidate_diagnostics"] = candidate_diagnostics
    plan["validation"] = validate_one_day_plan(plan, target)
    _print_selected_day_plan(plan)
    _print_validation(plan["validation"])
    if not args.no_write_outputs:
        _write_outputs(plan, args)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test pentru Generator v1.")
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--recipes", default=DEFAULT_RECIPES_PATH, type=Path)
    parser.add_argument("--ingredients", default=DEFAULT_INGREDIENTS_PATH, type=Path)
    parser.add_argument("--nutrition", default=DEFAULT_NUTRITION_PATH, type=Path)
    parser.add_argument("--out_csv", default=Path("outputs/generator_v1_plan.csv"), type=Path)
    parser.add_argument("--out_json", default=Path("outputs/generator_v1_plan.json"), type=Path)
    parser.add_argument("--out_txt", default=Path("outputs/generator_v1_readable.txt"), type=Path)
    parser.add_argument("--no_write_outputs", action="store_true")
    return parser.parse_args()


def _print_target_summary(target: NutritionTarget) -> None:
    print("Nutrition target")
    print(
        "  "
        f"kcal={target.kcal:.1f}, "
        f"protein_g={target.protein_g:.1f}, "
        f"carbs_g={target.carbs_g:.1f}, "
        f"fat_g={target.fat_g:.1f}"
    )
    print("Slot targets")
    for slot, values in target.slot_targets.items():
        print(
            "  "
            f"{slot}: "
            f"kcal={values['kcal']:.1f}, "
            f"protein_g={values['protein_g']:.1f}, "
            f"carbs_g={values['carbs_g']:.1f}, "
            f"fat_g={values['fat_g']:.1f}"
        )


def _print_pool_summary(candidates: pd.DataFrame, eligible_candidates: pd.DataFrame) -> None:
    print("Recipe candidate pool")
    print(f"  total_recipes_loaded={len(candidates)}")
    print(f"  eligible_candidate_count={len(eligible_candidates)}")
    print("First eligible recipes")

    preview_columns = [
        "recipe_id",
        "display_name",
        "energy_kcal_per_serving",
        "protein_g_per_serving",
        "carbs_g_per_serving",
        "fat_g_per_serving",
        "total_time_min",
    ]
    preview = eligible_candidates.loc[:, preview_columns].head(5)
    if preview.empty:
        print("  none")
        return

    for _, row in preview.iterrows():
        print(
            "  "
            f"{row['recipe_id']} | "
            f"{row['display_name']} | "
            f"kcal={_format_number(row['energy_kcal_per_serving'])}, "
            f"protein_g={_format_number(row['protein_g_per_serving'])}, "
            f"carbs_g={_format_number(row['carbs_g_per_serving'])}, "
            f"fat_g={_format_number(row['fat_g_per_serving'])}, "
            f"total_time_min={_format_number(row['total_time_min'], decimals=0)}"
        )


def _print_slot_candidate_summary(
    filtered_candidates: pd.DataFrame,
    slot_candidates: pd.DataFrame,
) -> None:
    print("Slot candidate preparation")
    print(f"  filtered_candidate_count={len(filtered_candidates)}")

    if slot_candidates.empty:
        print("  slot_candidate_count=0")
        return

    slot_counts = slot_candidates.groupby("slot", sort=False).size()
    for slot, count in slot_counts.items():
        print(f"  {slot}: slot_candidate_count={count}")

    print("Top time-fit candidates by slot")
    sort_columns = [
        "time_fit",
        "effective_time_min_for_scoring",
        "recipe_id",
        "portion_multiplier",
    ]
    ascending = [False, True, True, True]
    for slot in slot_counts.index:
        preview = (
            slot_candidates.loc[slot_candidates["slot"].eq(slot)]
            .sort_values(sort_columns, ascending=ascending, kind="mergesort")
            .head(5)
        )
        for _, row in preview.iterrows():
            print(
                "  "
                f"{row['slot']} | "
                f"{row['recipe_id']} | "
                f"{row['display_name']} | "
                f"portion={_format_number(row['portion_multiplier'])}, "
                f"kcal={_format_number(row['kcal'])}, "
                f"protein_g={_format_number(row['protein_g'])}, "
                f"total_time_min={_format_number(row['total_time_min'], decimals=0)}, "
                "effective_time_min="
                f"{_format_number(row['effective_time_min_for_scoring'], decimals=0)}, "
                "original_effective_time_min="
                f"{_format_number(row['original_effective_time_min_for_scoring'], decimals=0)}, "
                f"uses_pilot_time_fallback={row['uses_pilot_time_fallback']}, "
                f"time_fit={_format_number(row['time_fit'], decimals=2)}"
            )

    print("Top macro-fit candidates by slot")
    sort_columns = ["macro_fit", "time_fit", "recipe_id", "portion_multiplier"]
    ascending = [False, False, True, True]
    for slot in slot_counts.index:
        preview = (
            slot_candidates.loc[slot_candidates["slot"].eq(slot)]
            .sort_values(sort_columns, ascending=ascending, kind="mergesort")
            .head(5)
        )
        for _, row in preview.iterrows():
            print(
                "  "
                f"{row['slot']} | "
                f"{row['recipe_id']} | "
                f"{row['display_name']} | "
                f"portion={_format_number(row['portion_multiplier'])}, "
                f"portion_g_est={_format_number(row['portion_grams_estimated'], decimals=0)}, "
                f"kcal={_format_number(row['kcal'])}, "
                f"protein_g={_format_number(row['protein_g'])}, "
                f"carbs_g={_format_number(row['carbs_g'])}, "
                f"fat_g={_format_number(row['fat_g'])}, "
                f"macro_fit={_format_number(row['macro_fit'], decimals=2)}, "
                f"protein_fit={_format_number(row['protein_fit'], decimals=2)}, "
                f"kcal_fit={_format_number(row['kcal_fit'], decimals=2)}, "
                f"carbs_fit={_format_number(row['carbs_fit'], decimals=2)}, "
                f"fat_fit={_format_number(row['fat_fit'], decimals=2)}, "
                "effective_time_min="
                f"{_format_number(row['effective_time_min_for_scoring'], decimals=0)}, "
                f"time_fit={_format_number(row['time_fit'], decimals=2)}"
            )

    print("Top score-preview candidates by slot")
    sort_columns = ["score_preview", "macro_fit", "time_fit", "recipe_id", "portion_multiplier"]
    ascending = [False, False, False, True, True]
    for slot in slot_counts.index:
        preview = (
            slot_candidates.loc[slot_candidates["slot"].eq(slot)]
            .sort_values(sort_columns, ascending=ascending, kind="mergesort")
            .head(5)
        )
        for _, row in preview.iterrows():
            print(
                "  "
                f"{row['slot']} | "
                f"{row['recipe_id']} | "
                f"{row['display_name']} | "
                f"portion={_format_number(row['portion_multiplier'])}, "
                f"kcal={_format_number(row['kcal'])}, "
                f"protein_g={_format_number(row['protein_g'])}, "
                f"carbs_g={_format_number(row['carbs_g'])}, "
                f"fat_g={_format_number(row['fat_g'])}, "
                f"total_time_min={_format_number(row['total_time_min'], decimals=0)}, "
                "effective_time_min="
                f"{_format_number(row['effective_time_min_for_scoring'], decimals=0)}, "
                "original_effective_time_min="
                f"{_format_number(row['original_effective_time_min_for_scoring'], decimals=0)}, "
                f"has_long_passive_time={row['has_long_passive_time']}, "
                f"uses_pilot_time_fallback={row['uses_pilot_time_fallback']}, "
                f"macro_fit={_format_number(row['macro_fit'], decimals=2)}, "
                f"time_fit={_format_number(row['time_fit'], decimals=2)}, "
                f"slot_fit={_format_number(row['slot_fit'], decimals=2)}, "
                f"nutrition_quality={_format_number(row['nutrition_quality'], decimals=2)}, "
                f"is_nutrition_suspicious={row['is_nutrition_suspicious']}, "
                f"feedback_fit={_format_number(row['feedback_fit'], decimals=2)}, "
                f"variety_fit={_format_number(row['variety_fit'], decimals=2)}, "
                f"base_score_preview={_format_number(row['base_score_preview'], decimals=2)}, "
                f"score_preview={_format_number(row['score_preview'], decimals=2)}, "
                f"slot_fit_reasons={_format_reasons(row['slot_fit_reasons'])}, "
                f"nutrition_quality_reasons={_format_reasons(row['nutrition_quality_reasons'])}"
            )


def _format_number(value: object, decimals: int = 1) -> str:
    if pd.isna(value):
        return "missing"
    return f"{float(value):.{decimals}f}"


def _format_reasons(value: object) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    if pd.isna(value):
        return "missing"
    return str(value)


def _slot_order(target: NutritionTarget) -> list[str]:
    preferred_order = ["breakfast", "lunch", "dinner", "snack"]
    known_slots = [slot for slot in preferred_order if slot in target.slot_targets]
    extra_slots = [
        slot
        for slot in target.slot_targets
        if slot not in preferred_order
    ]
    return known_slots + extra_slots


def _slot_candidates_by_slot(
    slot_candidates: pd.DataFrame,
    slot_order: list[str],
) -> dict[str, pd.DataFrame]:
    return {
        slot: slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        for slot in slot_order
    }


def _target_to_dict(target: NutritionTarget) -> dict[str, object]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def _print_selected_day_plan(plan: dict[str, object]) -> None:
    print("Selected one-day plan preview")
    for meal in plan.get("selected_meals", []):
        print(
            "  "
            f"{meal['slot']} | "
            f"{meal['recipe_id']} | "
            f"{meal['display_name']} | "
            f"portion={_format_number(meal['portion_multiplier'])}, "
            f"portion_g_est={_format_number(meal['portion_grams_estimated'], decimals=0)}, "
            f"kcal={_format_number(meal['kcal'])}, "
            f"protein_g={_format_number(meal['protein_g'])}, "
            f"carbs_g={_format_number(meal['carbs_g'])}, "
            f"fat_g={_format_number(meal['fat_g'])}, "
            f"total_time_min={_format_number(meal['total_time_min'], decimals=0)}, "
            "effective_time_min="
            f"{_format_number(meal['effective_time_min_for_scoring'], decimals=0)}, "
            "original_effective_time_min="
            f"{_format_number(meal['original_effective_time_min_for_scoring'], decimals=0)}, "
            f"has_long_passive_time={meal['has_long_passive_time']}, "
            f"uses_pilot_time_fallback={meal['uses_pilot_time_fallback']}, "
            f"nutrition_quality={_format_number(meal['nutrition_quality'], decimals=2)}, "
            f"is_nutrition_suspicious={meal['is_nutrition_suspicious']}, "
            f"score_preview={_format_number(meal['score_preview'], decimals=2)}, "
            f"slot_fit_reasons={_format_reasons(meal['slot_fit_reasons'])}, "
            f"nutrition_quality_reasons={_format_reasons(meal['nutrition_quality_reasons'])}"
        )

    totals = plan.get("day_totals", {})
    print(
        "  "
        f"total_kcal={_format_number(totals.get('total_kcal'))}, "
        f"total_protein_g={_format_number(totals.get('total_protein_g'))}, "
        f"total_carbs_g={_format_number(totals.get('total_carbs_g'))}, "
        f"total_fat_g={_format_number(totals.get('total_fat_g'))}, "
        f"total_time_min_sum={_format_number(totals.get('total_time_min_sum'), decimals=0)}, "
        "effective_time_min_sum="
        f"{_format_number(totals.get('effective_time_min_sum'), decimals=0)}, "
        "passive_time_estimated_sum="
        f"{_format_number(totals.get('passive_time_estimated_sum'), decimals=0)}, "
        f"selected_slot_count={totals.get('selected_slot_count', 0)}"
    )
    warnings = plan.get("warnings", [])
    if warnings:
        print("  warnings=" + " | ".join(str(warning) for warning in warnings))
    else:
        print("  warnings=none")


def _print_candidate_diagnostics(diagnostics: dict[str, dict[str, object]]) -> None:
    print("Candidate diagnostics by slot")
    for slot, values in diagnostics.items():
        print(
            "  "
            f"{slot}: "
            f"total={values['total_slot_candidates']}, "
            f"unique_recipes={values['candidate_count']}, "
            f"suspicious={values['suspicious_nutrition_count']}, "
            f"non_suspicious={values['non_suspicious_nutrition_count']}, "
            f"kcal35_pass={values['kcal_35pct_pass_count']}, "
            f"protein20_pass={values['protein_20pct_pass_count']}, "
            f"both_pass={values['both_kcal_and_protein_pass_count']}, "
            f"time_le_30={values['time_le_30_count']}, "
            f"time_le_60={values['time_le_60_count']}, "
            f"time_gt_60={values['time_gt_60_count']}, "
            f"time_gt_180={values['time_gt_180_count']}, "
            f"long_passive={values.get('long_passive_time_count', 0)}, "
            f"time_basis={values.get('time_diagnostic_basis', 'total_time_min')}, "
            f"best_macro_fit={_format_number(values['best_macro_fit'], decimals=2)}, "
            f"best_score_preview={_format_number(values['best_score_preview'], decimals=2)}, "
            f"median_kcal={_format_number(values['median_kcal'])}, "
            f"median_protein_g={_format_number(values['median_protein_g'])}"
        )


def _print_validation(validation: dict[str, object]) -> None:
    print("Plan validation")
    print(
        "  "
        f"is_valid_for_checkpoint_1={validation['is_valid_for_checkpoint_1']}, "
        f"validation_status={validation['validation_status']}"
    )
    for warning in validation.get("validation_warnings", []):
        print(f"  warning={warning}")
    comparison = validation.get("target_comparison", {})
    print(
        "  "
        f"kcal_ratio={_format_number(comparison.get('kcal_ratio'), decimals=2)}, "
        f"protein_ratio={_format_number(comparison.get('protein_ratio'), decimals=2)}, "
        "effective_time_min_sum="
        f"{_format_number(comparison.get('effective_time_min_sum'), decimals=0)}, "
        "time_used_for_validation_min="
        f"{_format_number(comparison.get('time_used_for_validation_min'), decimals=0)}, "
        f"selected_slot_count={comparison.get('selected_slot_count')}, "
        f"expected_slot_count={comparison.get('expected_slot_count')}"
    )


def _write_outputs(plan: dict[str, object], args: argparse.Namespace) -> None:
    write_plan_csv(plan, args.out_csv)
    write_plan_json(plan, args.out_json)
    write_plan_readable(plan, args.out_txt)
    print("Generator v1 outputs written")
    print(f"  csv={args.out_csv}")
    print(f"  json={args.out_json}")
    print(f"  txt={args.out_txt}")


if __name__ == "__main__":
    main()
