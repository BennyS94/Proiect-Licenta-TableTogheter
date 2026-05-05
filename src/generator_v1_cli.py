from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.generator_v1.data_loader import (
    DEFAULT_INGREDIENTS_PATH,
    DEFAULT_NUTRITION_PATH,
    DEFAULT_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.candidate_filter import (
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.candidate_diagnostics import build_candidate_diagnostics
from src.generator_v1.day_selector import select_one_day_plan
from src.generator_v1.ingredient_diagnostics import build_ingredient_diagnostics
from src.generator_v1.nutrition_cache_diagnostics import (
    build_nutrition_cache_diagnostics,
)
from src.generator_v1.pilot_servings_estimator import (
    build_pilot_servings_diagnostics,
)
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
    fooddb = load_fooddb_current()
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
    candidate_diagnostics = build_candidate_diagnostics(
        slot_candidates=slot_candidates,
        slot_targets=target.slot_targets,
    )
    nutrition_cache_diagnostics = build_nutrition_cache_diagnostics(
        recipes=pool.recipes,
        nutrition=pool.nutrition,
        candidates=pool.candidates,
        eligible_candidates=pool.eligible_candidates,
    )

    _print_target_summary(target)
    _print_pool_summary(pool.candidates, pool.eligible_candidates)
    _print_slot_candidate_summary(filtered_candidates, slot_candidates)
    _print_candidate_diagnostics(candidate_diagnostics)
    if args.show_nutrition_diagnostics:
        _print_nutrition_cache_diagnostics(nutrition_cache_diagnostics)
    plan = select_one_day_plan(
        slot_candidates_by_slot=_slot_candidates_by_slot(slot_candidates, _slot_order(target)),
        slot_order=_slot_order(target),
    )
    plan["target"] = _target_to_dict(target)
    plan["candidate_diagnostics"] = candidate_diagnostics
    plan["validation"] = validate_one_day_plan(plan, target)
    servings_diagnostics = None
    if args.show_servings_diagnostics:
        servings_diagnostics = build_pilot_servings_diagnostics(
            recipes_df=pool.recipes,
            ingredients_df=pool.ingredients,
            nutrition_df=pool.nutrition,
            eligible_candidates=pool.eligible_candidates,
            selected_recipe_ids=_selected_recipe_ids(plan),
        )
    ingredient_diagnostics = None
    if args.show_ingredient_diagnostics:
        ingredient_diagnostics = build_ingredient_diagnostics(
            recipes_df=pool.recipes,
            ingredients_df=pool.ingredients,
            nutrition_df=pool.nutrition,
            selected_recipe_ids=_selected_recipe_ids(plan),
        )
    _print_selected_day_plan(plan)
    _print_validation(plan["validation"])
    if servings_diagnostics is not None:
        _print_servings_diagnostics(servings_diagnostics)
    if args.show_pilot_nutrition_overlay:
        _print_pilot_nutrition_overlay(plan)
    if ingredient_diagnostics is not None:
        _print_ingredient_diagnostics(ingredient_diagnostics)
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
    parser.add_argument("--show_nutrition_diagnostics", action="store_true")
    parser.add_argument("--show_ingredient_diagnostics", action="store_true")
    parser.add_argument("--show_servings_diagnostics", action="store_true")
    parser.add_argument("--show_pilot_nutrition_overlay", action="store_true")
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
                f"portion_g_source={_safe_text(row.get('portion_grams_source'))}, "
                "original_portion_g_est="
                f"{_format_number(row.get('original_portion_grams_estimated'), decimals=0)}, "
                "overlay_portion_g_est="
                f"{_format_number(row.get('overlay_portion_grams_estimated'), decimals=0)}, "
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
                f"portion_g_est={_format_number(row['portion_grams_estimated'], decimals=0)}, "
                f"portion_g_source={_safe_text(row.get('portion_grams_source'))}, "
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
                f"is_slot_suspicious={row.get('is_slot_suspicious')}, "
                f"feedback_fit={_format_number(row['feedback_fit'], decimals=2)}, "
                f"variety_fit={_format_number(row['variety_fit'], decimals=2)}, "
                f"base_score_preview={_format_number(row['base_score_preview'], decimals=2)}, "
                f"score_preview={_format_number(row['score_preview'], decimals=2)}, "
                f"slot_fit_reasons={_format_reasons(row['slot_fit_reasons'])}, "
                "slot_suspicion_reasons="
                f"{_format_reasons(row.get('slot_suspicion_reasons'))}, "
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
            f"portion_g_source={_safe_text(meal.get('portion_grams_source'))}, "
            "original_portion_g_est="
            f"{_format_number(meal.get('original_portion_grams_estimated'), decimals=0)}, "
            "overlay_portion_g_est="
            f"{_format_number(meal.get('overlay_portion_grams_estimated'), decimals=0)}, "
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
            f"is_slot_suspicious={meal.get('is_slot_suspicious')}, "
            f"score_preview={_format_number(meal['score_preview'], decimals=2)}, "
            f"slot_fit_reasons={_format_reasons(meal['slot_fit_reasons'])}, "
            "slot_suspicion_reasons="
            f"{_format_reasons(meal.get('slot_suspicion_reasons'))}, "
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


def _print_nutrition_cache_diagnostics(diagnostics: dict[str, object]) -> None:
    print("Nutrition cache diagnostics")
    print(f"  recipe_rows={diagnostics.get('recipe_rows')}")
    print(f"  joined_candidate_rows={diagnostics.get('joined_candidate_rows')}")
    print(f"  total_nutrition_rows={diagnostics.get('total_nutrition_rows')}")
    print(
        "  cache_status_counts="
        + _format_counts(diagnostics.get("cache_status_counts", {}))
    )

    servings_basis = diagnostics.get("servings_basis", {})
    if isinstance(servings_basis, dict):
        print(
            "  servings_basis: "
            f"missing={servings_basis.get('missing_count')}, "
            f"zero_or_invalid={servings_basis.get('zero_or_invalid_count')}, "
            f"value_counts={_format_counts(servings_basis.get('value_counts', {}))}"
        )

    total_weight = diagnostics.get("total_weight_grams_estimated", {})
    if isinstance(total_weight, dict):
        print(
            "  total_weight_grams_estimated: "
            f"missing={total_weight.get('missing_count')}, "
            f"zero_or_invalid={total_weight.get('zero_or_invalid_count')}, "
            f"min={_format_number(total_weight.get('min'))}, "
            f"median={_format_number(total_weight.get('median'))}, "
            f"max={_format_number(total_weight.get('max'))}"
        )

    macros = diagnostics.get("per_serving_macros", {})
    if isinstance(macros, dict):
        print("  per_serving_macros")
        for column, values in macros.items():
            if not isinstance(values, dict):
                continue
            print(
                "    "
                f"{column}: "
                f"missing={values.get('missing_count')}, "
                f"zero_or_near_zero={values.get('zero_or_near_zero_count')}, "
                f"near_zero_threshold={_format_number(values.get('near_zero_threshold'))}, "
                f"min={_format_number(values.get('min'))}, "
                f"median={_format_number(values.get('median'))}, "
                f"max={_format_number(values.get('max'))}"
            )

    mapped_ratio = diagnostics.get("mapped_weight_ratio", {})
    if isinstance(mapped_ratio, dict):
        print(
            "  mapped_weight_ratio: "
            f"missing={mapped_ratio.get('missing_count')}, "
            f"min={_format_number(mapped_ratio.get('min'), decimals=4)}, "
            f"median={_format_number(mapped_ratio.get('median'), decimals=4)}, "
            f"max={_format_number(mapped_ratio.get('max'), decimals=4)}, "
            f"below_0_20={mapped_ratio.get('count_below_0.20')}, "
            f"below_0_40={mapped_ratio.get('count_below_0.40')}, "
            f"below_0_60={mapped_ratio.get('count_below_0.60')}"
        )

    eligible = diagnostics.get("eligible_candidates", {})
    if isinstance(eligible, dict):
        print(
            "  eligible_candidates: "
            f"rows={eligible.get('eligible_rows_count')}, "
            f"kcal_lt_150={eligible.get('kcal_per_serving_lt_150_count')}, "
            f"protein_lt_10={eligible.get('protein_per_serving_lt_10_count')}, "
            "kcal_ge_150_and_protein_ge_10="
            f"{eligible.get('kcal_ge_150_and_protein_ge_10_count')}"
        )

    suspicious = diagnostics.get("top_suspicious_eligible_recipes", [])
    print("  top_suspicious_eligible_recipes")
    if not suspicious:
        print("    none")
        return
    for row in suspicious:
        if not isinstance(row, dict):
            continue
        print(
            "    "
            f"{_safe_text(row.get('recipe_id'))} | "
            f"{_safe_text(row.get('display_name'))} | "
            f"cache_status={_safe_text(row.get('cache_status'))}, "
            f"servings_basis={_format_number(row.get('servings_basis'))}, "
            "total_weight_g="
            f"{_format_number(row.get('total_weight_grams_estimated'))}, "
            f"mapped_weight_ratio={_format_number(row.get('mapped_weight_ratio'), decimals=4)}, "
            f"kcal={_format_number(row.get('energy_kcal_per_serving'))}, "
            f"protein_g={_format_number(row.get('protein_g_per_serving'))}, "
            f"mapped_ingredients={row.get('mapped_ingredient_count')}, "
            f"unmapped_ingredients={row.get('unmapped_ingredient_count')}"
        )


def _print_servings_diagnostics(diagnostics: dict[str, object]) -> None:
    print("Pilot servings diagnostics")
    print(f"  eligible_recipe_count={diagnostics.get('eligible_recipe_count')}")
    print(
        "  estimated_servings_basis_distribution="
        + _format_counts(
            diagnostics.get("estimated_servings_basis_distribution", {})
        )
    )
    print(
        "  uses_pilot_servings_fallback_count="
        f"{diagnostics.get('uses_pilot_servings_fallback_count')}"
    )

    selected = diagnostics.get("selected_plan_servings", [])
    print("  selected_plan_servings")
    if not isinstance(selected, list) or not selected:
        print("    none")
        return

    for row in selected:
        if not isinstance(row, dict):
            continue
        print(
            "    "
            f"{_safe_text(row.get('recipe_id'))} | "
            f"{_safe_text(row.get('display_name'))} | "
            "cache_servings_basis="
            f"{_format_number(row.get('cache_servings_basis'))}, "
            "estimated_servings_basis="
            f"{_format_number(row.get('estimated_servings_basis'))}, "
            "uses_pilot_servings_fallback="
            f"{row.get('uses_pilot_servings_fallback')}, "
            "reasons="
            f"{_format_reasons(row.get('servings_estimation_reasons'))}"
        )


def _print_pilot_nutrition_overlay(plan: dict[str, object]) -> None:
    print("Pilot nutrition overlay")
    totals = plan.get("day_totals", {})
    print(
        "  original_day_totals: "
        f"kcal={_format_number(totals.get('original_total_kcal'))}, "
        f"protein_g={_format_number(totals.get('original_total_protein_g'))}, "
        f"carbs_g={_format_number(totals.get('original_total_carbs_g'))}, "
        f"fat_g={_format_number(totals.get('original_total_fat_g'))}"
    )
    print(
        "  overlay_based_day_totals: "
        f"kcal={_format_number(totals.get('total_kcal'))}, "
        f"protein_g={_format_number(totals.get('total_protein_g'))}, "
        f"carbs_g={_format_number(totals.get('total_carbs_g'))}, "
        f"fat_g={_format_number(totals.get('total_fat_g'))}, "
        "uses_overlay_count="
        f"{totals.get('uses_pilot_nutrition_overlay_count')}"
    )
    print("  selected_plan_overlay")
    for meal in plan.get("selected_meals", []):
        if not isinstance(meal, dict):
            continue
        print(
            "    "
            f"{_safe_text(meal.get('slot'))} | "
            f"{_safe_text(meal.get('recipe_id'))} | "
            f"{_safe_text(meal.get('display_name'))} | "
            "original_per_serving="
            f"kcal:{_format_number(meal.get('original_energy_kcal_per_serving'))}, "
            f"protein:{_format_number(meal.get('original_protein_g_per_serving'))}, "
            f"carbs:{_format_number(meal.get('original_carbs_g_per_serving'))}, "
            f"fat:{_format_number(meal.get('original_fat_g_per_serving'))}; "
            "overlay_per_serving="
            f"kcal:{_format_number(meal.get('overlay_energy_kcal_per_serving'))}, "
            f"protein:{_format_number(meal.get('overlay_protein_g_per_serving'))}, "
            f"carbs:{_format_number(meal.get('overlay_carbs_g_per_serving'))}, "
            f"fat:{_format_number(meal.get('overlay_fat_g_per_serving'))}; "
            "uses_pilot_nutrition_overlay="
            f"{meal.get('uses_pilot_nutrition_overlay')}, "
            "estimated_servings="
            f"{_format_number(meal.get('overlay_estimated_servings_basis'))}, "
            "original_portion_g="
            f"{_format_number(meal.get('original_portion_grams_estimated'), decimals=0)}, "
            "overlay_portion_g="
            f"{_format_number(meal.get('overlay_portion_grams_estimated'), decimals=0)}, "
            "portion_source="
            f"{_safe_text(meal.get('portion_grams_source'))}, "
            "alias_weight_g="
            f"{_format_number(meal.get('overlay_alias_weight_grams'))}, "
            "aliases="
            f"{_format_reasons(meal.get('overlay_aliases_used'))}, "
            "reasons="
            f"{_format_reasons(meal.get('pilot_nutrition_overlay_reasons'))}"
        )


def _print_ingredient_diagnostics(diagnostics: dict[str, object]) -> None:
    print("Ingredient diagnostics")
    global_summary = diagnostics.get("global_mapping_summary", {})
    if isinstance(global_summary, dict):
        print(
            "  global_mapping_summary: "
            f"total_rows={global_summary.get('total_ingredient_rows')}, "
            "mapping_status_counts="
            f"{_format_counts(global_summary.get('mapping_status_counts', {}))}, "
            f"mapped_food_id_present={global_summary.get('mapped_food_id_present_count')}, "
            "quantity_grams_gt_0="
            f"{global_summary.get('quantity_grams_estimated_gt_0_count')}, "
            "mapped_food_id_and_grams_gt_0="
            f"{global_summary.get('mapped_food_id_and_grams_gt_0_count')}, "
            "accepted_auto_with_grams_gt_0="
            f"{global_summary.get('accepted_auto_with_grams_gt_0_count')}, "
            "accepted_auto_without_grams="
            f"{global_summary.get('accepted_auto_without_grams_count')}, "
            "review_needed_with_grams_gt_0="
            f"{global_summary.get('review_needed_with_grams_gt_0_count')}, "
            "unmapped_with_grams_gt_0="
            f"{global_summary.get('unmapped_with_grams_gt_0_count')}"
        )

    print(f"  eligible_recipe_count={diagnostics.get('eligible_recipe_count')}")
    _print_suspicious_recipe_rows(
        "  top_suspicious_recipes",
        diagnostics.get("top_suspicious_recipes", []),
    )
    _print_common_ingredient_rows(
        "  common_unmapped_ingredients",
        diagnostics.get("common_unmapped_ingredients", []),
    )
    _print_common_ingredient_rows(
        "  common_review_needed_ingredients",
        diagnostics.get("common_review_needed_ingredients", []),
    )
    _print_selected_ingredient_breakdown(
        diagnostics.get("selected_plan_ingredient_breakdown", [])
    )


def _print_suspicious_recipe_rows(title: str, rows: object) -> None:
    print(title)
    if not isinstance(rows, list) or not rows:
        print("    none")
        return
    for row in rows:
        if not isinstance(row, dict):
            continue
        print(
            "    "
            f"{_safe_text(row.get('recipe_id'))} | "
            f"{_safe_text(row.get('display_name'))} | "
            f"ingredients={row.get('ingredient_count')}, "
            f"accepted_auto={row.get('accepted_auto_count')}, "
            "accepted_auto_with_grams="
            f"{row.get('accepted_auto_with_grams_count')}, "
            f"review_needed={row.get('review_needed_count')}, "
            f"unmapped={row.get('unmapped_count')}, "
            f"ingredients_with_grams={row.get('ingredients_with_grams_count')}, "
            f"mapped_with_grams={row.get('mapped_with_grams_count')}, "
            "mapped_weight_sum_g="
            f"{_format_number(row.get('mapped_weight_sum_grams'))}, "
            f"mapped_weight_ratio={_format_number(row.get('mapped_weight_ratio'), decimals=4)}, "
            f"kcal={_format_number(row.get('energy_kcal_per_serving'))}, "
            f"protein_g={_format_number(row.get('protein_g_per_serving'))}"
        )


def _print_common_ingredient_rows(title: str, rows: object) -> None:
    print(title)
    if not isinstance(rows, list) or not rows:
        print("    none")
        return
    for row in rows:
        if not isinstance(row, dict):
            continue
        print(
            "    "
            f"{_safe_text(row.get('diagnostic_ingredient_name'))}: "
            f"count={row.get('count')}, "
            f"with_grams={row.get('with_grams_count')}"
        )


def _print_selected_ingredient_breakdown(rows: object) -> None:
    print("  selected_plan_ingredient_breakdown")
    if not isinstance(rows, list) or not rows:
        print("    none")
        return
    for row in rows:
        if not isinstance(row, dict):
            continue
        print(
            "    "
            f"{_safe_text(row.get('recipe_id'))} | "
            f"{_safe_text(row.get('display_name'))} | "
            f"{_safe_text(row.get('ingredient_raw_text'))} | "
            f"normalized={_safe_text(row.get('ingredient_name_normalized'))}, "
            f"qty={_safe_text(row.get('quantity_value'))} "
            f"{_safe_text(row.get('quantity_unit'))}, "
            f"grams={_format_number(row.get('quantity_grams_estimated'))}, "
            f"status={_safe_text(row.get('mapping_status'))}, "
            f"mapped_food_id={_safe_text(row.get('mapped_food_id'))}, "
            f"food={_safe_text(row.get('mapped_food_canonical_name'))}, "
            f"confidence={_safe_text(row.get('mapping_confidence'))}, "
            f"role={_safe_text(row.get('ingredient_role'))}"
        )


def _format_counts(value: object) -> str:
    if not isinstance(value, dict) or not value:
        return "none"
    return ", ".join(f"{key}:{item}" for key, item in value.items())


def _safe_text(value: object) -> str:
    if value is None:
        return "missing"
    return str(value).encode("ascii", errors="replace").decode("ascii")


def _selected_recipe_ids(plan: dict[str, object]) -> list[str]:
    return [
        str(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if isinstance(meal, dict) and meal.get("recipe_id")
    ]


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
