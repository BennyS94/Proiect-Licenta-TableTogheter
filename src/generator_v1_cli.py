from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.generator_v1.data_loader import (
    DEFAULT_INGREDIENTS_PATH,
    DEFAULT_NUTRITION_PATH,
    DEFAULT_RECIPES_PATH,
    PILOT_CURRENT_PROFILE,
    V1_2_DEMO_CANDIDATE_INGREDIENTS_PATH,
    V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_INGREDIENTS_PATH,
    V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_NUTRITION_PATH,
    V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_PROFILE,
    V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_INGREDIENTS_PATH,
    V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_NUTRITION_PATH,
    V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_PROFILE,
    V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_RECIPES_PATH,
    V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_RECIPES_PATH,
    V1_2_DEMO_CANDIDATE_NUTRITION_PATH,
    V1_2_DEMO_CANDIDATE_PROFILE,
    V1_2_DEMO_CANDIDATE_RECIPES_PATH,
    V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_INGREDIENTS_PATH,
    V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_NUTRITION_PATH,
    V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_PROFILE,
    V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_RECIPES_PATH,
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_NUTRITION_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_PROFILE,
    V1_2_DEMO_FINAL_TIME_LAYER_RECIPES_PATH,
    V1_1_GENERATOR_READY_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_NUTRITION_PATH,
    V1_1_GENERATOR_READY_PROFILE,
    V1_1_GENERATOR_READY_RECIPES_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_RECIPES_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_RECIPES_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_RECIPES_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
    V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_RECIPES_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH,
    V1_2_GENERATOR_READY_PLUS30_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH,
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_NUTRITION_PATH,
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_PROFILE,
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_NUTRITION_PATH,
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE,
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_RECIPES_PATH,
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_RECIPES_PATH,
    V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_NUTRITION_PATH,
    V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_PROFILE,
    V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_RECIPES_PATH,
    V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_NUTRITION_PATH,
    V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_PROFILE,
    V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.candidate_filter import (
    build_household_preference_context as build_profile_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.candidate_diagnostics import build_candidate_diagnostics
from src.generator_v1.day_selector import select_one_day_plan
from src.generator_v1.day_selector_balanced import select_one_day_plan_balanced
from src.generator_v1.feedback_adapter import (
    build_household_preference_context as build_feedback_preference_context,
)
from src.generator_v1.feedback_store import (
    DEFAULT_FEEDBACK_EVENTS_PATH,
    clear_feedback_events,
    load_feedback_events,
)
from src.generator_v1.grocery_list import (
    build_grocery_list,
    write_grocery_list_csv,
    write_grocery_list_readable,
)
from src.generator_v1.household_generator import (
    HOUSEHOLD_ALLOCATION_MODES,
    HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
    HOUSEHOLD_MODE_OFF,
    HOUSEHOLD_MODE_SHARED_ALL_SLOTS,
    HOUSEHOLD_MODE_SHARED_MAIN_MEALS,
    build_household_aggregate_target,
    build_household_slot_candidates,
    build_member_targets,
    generate_household_plan,
    household_plan_readable_lines,
    load_household_profile,
    write_household_allocations_csv,
    write_household_grocery_scaling_csv,
    write_household_member_macros_csv,
    write_household_plan_json,
    write_household_plan_readable,
)
from src.generator_v1.ingredient_diagnostics import build_ingredient_diagnostics
from src.generator_v1.multi_day_audit import (
    multi_day_readable_lines,
    write_multi_day_meals_csv,
    write_multi_day_plan_json,
    write_multi_day_plan_readable,
)
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    MULTI_DAY_MODE_SIMPLE,
    generate_multi_day_plan,
)
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
from src.generator_v1.plan_quality_gate import evaluate_plan_quality
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_guard import evaluate_profile_guard
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.reroll_policy import select_quality_gated_reroll
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


V1_1_RECOMMENDED_TEST_PRESET = "v1_1_recommended_test"


def main() -> None:
    args = _parse_args()
    if args.clear_feedback:
        clear_feedback_events(args.feedback_events_path)
        print(f"Feedback events cleared: {_feedback_events_path(args)}")
        return

    if _should_run_household(args):
        _run_household_generation(args)
        return

    profile = load_member_profile(args.profile)
    target = build_nutrition_target(profile)
    profile_guard_result = _profile_guard_result(args, profile, target)
    if _profile_guard_blocks(profile_guard_result, args):
        _print_target_summary(target)
        _print_profile_guard(profile_guard_result, args)
        print(
            "Generation blocked by profile_guard=demo. "
            "Use --allow_unsupported_profile only for explicit aggressive-cut tests."
        )
        return

    pool = load_recipe_candidate_pool(
        recipes_path=args.recipes,
        ingredients_path=args.ingredients,
        nutrition_path=args.nutrition,
        dataset_profile=args.dataset_profile,
    )
    fooddb = load_fooddb_current()
    preference_context = build_profile_preference_context(profile)
    feedback_preference_context = _feedback_preference_context(args, profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
        feedback_preference_context=feedback_preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode=args.portion_policy,
        feedback_preference_context=feedback_preference_context,
        health_and_diet_preferences=preference_context.health_and_diet_preferences,
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

    _print_dataset_summary(args, pool)
    _print_target_summary(target)
    _print_profile_guard(profile_guard_result, args)
    if args.show_feedback_context:
        _print_feedback_context(feedback_preference_context, args)
    _print_pool_summary(pool.candidates, pool.eligible_candidates)
    _print_slot_candidate_summary(filtered_candidates, slot_candidates)
    _print_candidate_diagnostics(candidate_diagnostics)
    if args.show_nutrition_diagnostics:
        _print_nutrition_cache_diagnostics(nutrition_cache_diagnostics)

    if _should_run_multi_day(args):
        multi_day_plan = generate_multi_day_plan(
            profile=profile,
            target=target,
            slot_candidates=slot_candidates,
            days=args.days,
            config=_multi_day_selector_config(args),
        )
        multi_day_plan["candidate_diagnostics"] = candidate_diagnostics
        multi_day_plan["nutrition_cache_diagnostics"] = nutrition_cache_diagnostics
        if profile_guard_result is not None:
            multi_day_plan["profile_guard"] = profile_guard_result
        multi_day_plan["feedback_context"] = feedback_preference_context
        multi_day_plan["pool_summary"] = _pool_summary(args, pool, filtered_candidates, slot_candidates)
        _print_multi_day_plan(multi_day_plan)
        if not args.no_write_outputs:
            _write_multi_day_outputs(multi_day_plan, args)
        if args.write_grocery_list:
            _build_write_print_grocery_list(
                multi_day_plan,
                pool.ingredients,
                fooddb,
                args,
            )
        return

    selector_config = _balanced_selector_config(args)
    if _should_use_quality_gated_reroll(args):
        ordered_slots = _slot_order(target)
        plan = select_quality_gated_reroll(
            slot_candidates_by_slot=_slot_candidates_by_slot(
                slot_candidates,
                ordered_slots,
            ),
            target=target,
            slot_order=ordered_slots,
            recent_recipe_ids=_parse_recent_recipe_ids(args.recent_recipe_ids),
            base_config=selector_config,
        )
    else:
        plan = _select_one_day_plan(
            selection_mode=args.selection_mode,
            slot_candidates=slot_candidates,
            target=target,
            selector_config=selector_config,
        )
    plan["target"] = _target_to_dict(target)
    plan["candidate_diagnostics"] = candidate_diagnostics
    plan["feedback_context"] = feedback_preference_context
    if profile_guard_result is not None:
        plan["profile_guard"] = profile_guard_result
    plan["validation"] = validate_one_day_plan(plan, target)
    if args.quality_gate == "demo_safe" and "quality_gate" not in plan:
        plan["quality_gate"] = evaluate_plan_quality(
            plan,
            target,
            config={"quality_gate": "demo_safe"},
        )
        plan["quality_gate_status"] = plan["quality_gate"]["quality_gate_status"]
        plan["quality_gate_reasons"] = plan["quality_gate"]["quality_gate_reasons"]
        plan["quality_gate_score"] = plan["quality_gate"]["quality_gate_score"]
        plan["quality_gate_fallback_used"] = False
        plan["quality_gate_selected_mode"] = args.diversity_mode
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
    _print_selector_diagnostics(plan)
    _print_quality_gate(plan)
    _print_plan_alternatives(plan)
    _print_validation(plan["validation"])
    if servings_diagnostics is not None:
        _print_servings_diagnostics(servings_diagnostics)
    if args.show_pilot_nutrition_overlay:
        _print_pilot_nutrition_overlay(plan)
    if ingredient_diagnostics is not None:
        _print_ingredient_diagnostics(ingredient_diagnostics)
    if not args.no_write_outputs:
        _write_outputs(plan, args)
    if args.write_grocery_list:
        _build_write_print_grocery_list(
            plan,
            pool.ingredients,
            fooddb,
            args,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test pentru Generator v1.")
    parser.add_argument(
        "--profile",
        default=Path("profiles/member_profile_demo_v1.json"),
        type=Path,
    )
    parser.add_argument(
        "--dataset_profile",
        choices=[
            PILOT_CURRENT_PROFILE,
            V1_1_GENERATOR_READY_PROFILE,
            V1_1_GENERATOR_READY_SLOT_CHECKED_PROFILE,
            V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_PROFILE,
            V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
            V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE,
            V1_2_GENERATOR_READY_PLUS30_PROFILE,
            V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
            V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_PROFILE,
            V1_2_GENERATOR_READY_ROUND37_EXPANDED_PROFILE,
            V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE,
            V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_PROFILE,
            V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_PROFILE,
            V1_2_DEMO_CANDIDATE_PROFILE,
            V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_PROFILE,
            V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_PROFILE,
            V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_PROFILE,
            V1_2_DEMO_FINAL_PROFILE,
            V1_2_DEMO_FINAL_TIME_LAYER_PROFILE,
        ],
        default=PILOT_CURRENT_PROFILE,
    )
    parser.add_argument(
        "--test_preset",
        choices=["none", V1_1_RECOMMENDED_TEST_PRESET],
        default="none",
    )
    parser.add_argument("--recipes", default=None, type=Path)
    parser.add_argument("--ingredients", default=None, type=Path)
    parser.add_argument("--nutrition", default=None, type=Path)
    parser.add_argument(
        "--selection_mode",
        choices=["greedy", "balanced_day"],
        default="greedy",
    )
    parser.add_argument("--alternative_count", default=1, type=int)
    parser.add_argument(
        "--diversity_mode",
        choices=["none", "soft", "avoid_recent"],
        default="none",
    )
    parser.add_argument("--recent_recipe_ids", default="", type=str)
    parser.add_argument(
        "--portion_policy",
        choices=["standard", "expanded_safe", "target_aware"],
        default="standard",
    )
    parser.add_argument(
        "--meal_realism_mode",
        choices=["off", "audit", "soft", "practical"],
        default="off",
    )
    parser.add_argument(
        "--quality_gate",
        choices=["off", "demo_safe"],
        default="off",
    )
    parser.add_argument("--days", default=1, type=int)
    parser.add_argument(
        "--multi_day_mode",
        choices=["off", MULTI_DAY_MODE_SIMPLE, MULTI_DAY_MODE_GLOBAL],
        default="off",
    )
    parser.add_argument(
        "--multi_day_no_repeat_policy",
        choices=["none", "prefer", "hard", "main_only"],
        default="prefer",
    )
    parser.add_argument("--day_candidate_pool_size", default=75, type=int)
    parser.add_argument(
        "--multi_day_speed_mode",
        choices=["fast", "quality"],
        default="fast",
    )
    parser.add_argument(
        "--day_candidate_builder",
        choices=["balanced_repeated", "direct_from_slots"],
        default=None,
    )
    parser.add_argument("--direct_slot_shortlist_size", default=12, type=int)
    parser.add_argument(
        "--profile_guard",
        choices=["off", "demo", "permissive"],
        default="off",
    )
    parser.add_argument("--allow_unsupported_profile", action="store_true")
    parser.add_argument("--feedback_events_path", default=None, type=Path)
    parser.add_argument("--show_feedback_context", action="store_true")
    parser.add_argument("--clear_feedback", action="store_true")
    parser.add_argument("--feedback_disabled", action="store_true")
    parser.add_argument("--out_csv", default=Path("outputs/generator_v1_plan.csv"), type=Path)
    parser.add_argument("--out_json", default=Path("outputs/generator_v1_plan.json"), type=Path)
    parser.add_argument("--out_txt", default=Path("outputs/generator_v1_readable.txt"), type=Path)
    parser.add_argument("--write_grocery_list", action="store_true")
    parser.add_argument(
        "--grocery_out_csv",
        default=Path("outputs/generator_v1_grocery_list.csv"),
        type=Path,
    )
    parser.add_argument(
        "--grocery_out_txt",
        default=Path("outputs/generator_v1_grocery_list.txt"),
        type=Path,
    )
    parser.add_argument("--grocery_purchase_suggestions", action="store_true")
    parser.add_argument("--grocery_purchase_rules_path", default=None, type=Path)
    parser.add_argument("--grocery_price_estimates", action="store_true")
    parser.add_argument("--grocery_product_catalog_path", default=None, type=Path)
    parser.add_argument(
        "--grocery_cooked_to_raw",
        action="store_true",
        default=None,
    )
    parser.add_argument("--grocery_cooked_to_raw_rules_path", default=None, type=Path)
    parser.add_argument("--include_pantry_basics", action="store_true")
    parser.add_argument("--household_profile", default=None, type=Path)
    parser.add_argument(
        "--household_mode",
        choices=[
            HOUSEHOLD_MODE_OFF,
            HOUSEHOLD_MODE_SHARED_ALL_SLOTS,
            HOUSEHOLD_MODE_SHARED_MAIN_MEALS,
            HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
        ],
        default=HOUSEHOLD_MODE_OFF,
    )
    parser.add_argument(
        "--household_allocation_mode",
        choices=list(HOUSEHOLD_ALLOCATION_MODES),
        default="macro_aware_simple",
    )
    parser.add_argument(
        "--out_household_json",
        default=Path("outputs/generator_v1_household_plan.json"),
        type=Path,
    )
    parser.add_argument(
        "--out_household_txt",
        default=Path("outputs/generator_v1_household_readable.txt"),
        type=Path,
    )
    parser.add_argument(
        "--out_household_allocations_csv",
        default=Path("outputs/generator_v1_household_allocations.csv"),
        type=Path,
    )
    parser.add_argument(
        "--out_household_member_macros_csv",
        default=Path("outputs/generator_v1_household_member_macros.csv"),
        type=Path,
    )
    parser.add_argument(
        "--out_household_grocery_scaling_csv",
        default=Path("outputs/generator_v1_household_grocery_scaling.csv"),
        type=Path,
    )
    parser.add_argument(
        "--out_multiday_json",
        default=Path("outputs/generator_v1_multiday_plan.json"),
        type=Path,
    )
    parser.add_argument(
        "--out_multiday_txt",
        default=Path("outputs/generator_v1_multiday_readable.txt"),
        type=Path,
    )
    parser.add_argument(
        "--out_multiday_csv",
        default=Path("outputs/generator_v1_multiday_meals.csv"),
        type=Path,
    )
    parser.add_argument("--no_write_outputs", action="store_true")
    parser.add_argument("--show_nutrition_diagnostics", action="store_true")
    parser.add_argument("--show_ingredient_diagnostics", action="store_true")
    parser.add_argument("--show_servings_diagnostics", action="store_true")
    parser.add_argument("--show_pilot_nutrition_overlay", action="store_true")
    args = parser.parse_args()
    _validate_days(args, parser)
    _validate_household_args(args, parser)
    _apply_test_preset(args)
    _apply_multi_day_defaults(args)
    _resolve_dataset_paths(args)
    return args


def _validate_days(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if int(args.days or 0) < 1 or int(args.days or 0) > 5:
        parser.error("argument --days: trebuie sa fie intre 1 si 5.")


def _validate_household_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.household_mode != HOUSEHOLD_MODE_OFF and not args.household_profile:
        parser.error("argument --household_profile este obligatoriu cand --household_mode nu este off.")


def _apply_test_preset(args: argparse.Namespace) -> None:
    if args.test_preset != V1_1_RECOMMENDED_TEST_PRESET:
        return
    args.dataset_profile = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE
    args.selection_mode = "balanced_day"
    args.portion_policy = "target_aware"
    args.alternative_count = 3
    args.diversity_mode = "none"
    args.meal_realism_mode = "practical"
    args.quality_gate = "demo_safe"


def _apply_multi_day_defaults(args: argparse.Namespace) -> None:
    if not _should_run_multi_day(args):
        return
    if args.multi_day_mode == "off":
        args.multi_day_mode = MULTI_DAY_MODE_SIMPLE
    args.selection_mode = "balanced_day"
    args.portion_policy = "target_aware"
    args.meal_realism_mode = "practical"
    args.quality_gate = "demo_safe"
    args.alternative_count = max(3, int(args.alternative_count or 1))


def _resolve_dataset_paths(args: argparse.Namespace) -> None:
    if args.dataset_profile == V1_2_DEMO_FINAL_TIME_LAYER_PROFILE:
        args.recipes = args.recipes or V1_2_DEMO_FINAL_TIME_LAYER_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_DEMO_FINAL_TIME_LAYER_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_DEMO_FINAL_TIME_LAYER_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_DEMO_FINAL_PROFILE:
        args.recipes = args.recipes or V1_2_DEMO_FINAL_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_DEMO_FINAL_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_DEMO_FINAL_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_PROFILE:
        args.recipes = args.recipes or V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_PROFILE:
        args.recipes = (
            args.recipes
            or V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_RECIPES_PATH
        )
        args.ingredients = (
            args.ingredients
            or V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_INGREDIENTS_PATH
        )
        args.nutrition = (
            args.nutrition
            or V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_NUTRITION_PATH
        )
        return
    if args.dataset_profile == V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_PROFILE:
        args.recipes = args.recipes or V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_DEMO_CANDIDATE_PROFILE:
        args.recipes = args.recipes or V1_2_DEMO_CANDIDATE_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_DEMO_CANDIDATE_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_DEMO_CANDIDATE_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_PROFILE:
        args.recipes = args.recipes or V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_PROFILE:
        args.recipes = args.recipes or V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE:
        args.recipes = args.recipes or V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_GENERATOR_READY_ROUND37_EXPANDED_PROFILE:
        args.recipes = args.recipes or V1_2_GENERATOR_READY_ROUND37_EXPANDED_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_GENERATOR_READY_ROUND37_EXPANDED_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_GENERATOR_READY_ROUND37_EXPANDED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_PROFILE:
        args.recipes = args.recipes or V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE:
        args.recipes = args.recipes or V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH
        return
    if args.dataset_profile == V1_2_GENERATOR_READY_PLUS30_PROFILE:
        args.recipes = args.recipes or V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH
        args.ingredients = args.ingredients or V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH
        return
    if args.dataset_profile == V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE:
        args.recipes = (
            args.recipes
            or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_RECIPES_PATH
        )
        args.ingredients = (
            args.ingredients
            or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_INGREDIENTS_PATH
        )
        args.nutrition = (
            args.nutrition
            or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_NUTRITION_PATH
        )
        return
    if args.dataset_profile == V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE:
        args.recipes = args.recipes or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH
        args.ingredients = (
            args.ingredients
            or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH
        )
        args.nutrition = args.nutrition or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_PROFILE:
        args.recipes = args.recipes or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_RECIPES_PATH
        args.ingredients = args.ingredients or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_1_GENERATOR_READY_SLOT_CHECKED_PROFILE:
        args.recipes = args.recipes or V1_1_GENERATOR_READY_SLOT_CHECKED_RECIPES_PATH
        args.ingredients = args.ingredients or V1_1_GENERATOR_READY_SLOT_CHECKED_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_1_GENERATOR_READY_SLOT_CHECKED_NUTRITION_PATH
        return
    if args.dataset_profile == V1_1_GENERATOR_READY_PROFILE:
        args.recipes = args.recipes or V1_1_GENERATOR_READY_RECIPES_PATH
        args.ingredients = args.ingredients or V1_1_GENERATOR_READY_INGREDIENTS_PATH
        args.nutrition = args.nutrition or V1_1_GENERATOR_READY_NUTRITION_PATH
        return
    args.recipes = args.recipes or DEFAULT_RECIPES_PATH
    args.ingredients = args.ingredients or DEFAULT_INGREDIENTS_PATH
    args.nutrition = args.nutrition or DEFAULT_NUTRITION_PATH


def _print_dataset_summary(args: argparse.Namespace, pool: object) -> None:
    print("Dataset profile")
    print(f"  test_preset={args.test_preset}")
    print(f"  dataset_profile={args.dataset_profile}")
    print(f"  selection_mode={args.selection_mode}")
    print(f"  portion_policy={args.portion_policy}")
    print(f"  meal_realism_mode={args.meal_realism_mode}")
    print(f"  quality_gate={args.quality_gate}")
    print(f"  alternative_count={args.alternative_count}")
    print(f"  diversity_mode={args.diversity_mode}")
    print(f"  days={args.days}")
    print(f"  multi_day_mode={args.multi_day_mode}")
    print(f"  multi_day_no_repeat_policy={args.multi_day_no_repeat_policy}")
    print(f"  day_candidate_pool_size={args.day_candidate_pool_size}")
    print(f"  multi_day_speed_mode={args.multi_day_speed_mode}")
    print(f"  day_candidate_builder={args.day_candidate_builder or 'auto'}")
    print(f"  direct_slot_shortlist_size={args.direct_slot_shortlist_size}")
    print(f"  profile_guard={args.profile_guard}")
    print(f"  allow_unsupported_profile={args.allow_unsupported_profile}")
    print(f"  feedback_events_path={_feedback_events_path(args)}")
    print(f"  feedback_disabled={args.feedback_disabled}")
    recent_recipe_ids = _parse_recent_recipe_ids(args.recent_recipe_ids)
    if recent_recipe_ids:
        print(f"  recent_recipe_ids={','.join(recent_recipe_ids)}")
    if args.dataset_profile in {
        V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
        V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE,
        V1_2_GENERATOR_READY_PLUS30_PROFILE,
        V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
        V1_2_GENERATOR_READY_PLUS30_PLUS15_REPAIRED_PROFILE,
        V1_2_GENERATOR_READY_ROUND37_EXPANDED_PROFILE,
        V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE,
        V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_PROFILE,
        V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_PROFILE,
        V1_2_DEMO_CANDIDATE_PROFILE,
        V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_PROFILE,
        V1_2_DEMO_CANDIDATE_MANUAL_BATCH2_ROUND46_QA_PROFILE,
        V1_2_DEMO_CANDIDATE_ROUND48_CLEANED_PROFILE,
        V1_2_DEMO_FINAL_PROFILE,
        V1_2_DEMO_FINAL_TIME_LAYER_PROFILE,
    }:
        is_recommended = (
            args.selection_mode == "balanced_day"
            and args.portion_policy == "target_aware"
            and int(args.alternative_count or 1) == 3
            and args.diversity_mode == "none"
            and args.meal_realism_mode == "practical"
            and args.quality_gate == "demo_safe"
        )
        print(
            "  recommended_for_v1_1_testing="
            "balanced_day + target_aware + alternative_count=3 + "
            "meal_realism_mode=practical + quality_gate=demo_safe"
        )
        print(f"  v1_1_recommended_test_active={is_recommended}")
    print(f"  recipes_path={args.recipes}")
    print(f"  ingredients_path={args.ingredients}")
    print(f"  nutrition_path={args.nutrition}")
    diagnostics = getattr(pool, "loader_diagnostics", {})
    warnings = diagnostics.get("warnings") if isinstance(diagnostics, dict) else None
    if warnings:
        print(f"  loader_warnings={'; '.join(str(item) for item in warnings)}")


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


def _profile_guard_result(
    args: argparse.Namespace,
    profile: dict[str, object],
    target: NutritionTarget,
) -> dict[str, object] | None:
    if args.profile_guard == "off":
        return None
    return evaluate_profile_guard(
        profile=profile,
        target=target,
        meal_config=profile.get("meal_config") or {},
        mode=args.profile_guard,
    )


def _profile_guard_blocks(
    guard_result: dict[str, object] | None,
    args: argparse.Namespace,
) -> bool:
    if not guard_result:
        return False
    return bool(guard_result.get("should_block_generation")) and not bool(
        args.allow_unsupported_profile
    )


def _print_profile_guard(
    guard_result: dict[str, object] | None,
    args: argparse.Namespace,
) -> None:
    if guard_result is None:
        return
    print("Profile guard")
    print(f"  profile_guard={args.profile_guard}")
    print(
        "  profile_guard_status="
        f"{guard_result.get('profile_guard_status')}"
    )
    print(
        "  profile_guard_reasons="
        + _format_reasons(guard_result.get("profile_guard_reasons"))
    )
    print(
        "  profile_guard_recommendations="
        + _format_reasons(guard_result.get("profile_guard_recommendations"))
    )
    print(
        "  should_block_generation="
        f"{guard_result.get('should_block_generation')}"
    )
    print(
        "  allow_unsupported_profile="
        f"{args.allow_unsupported_profile}"
    )
    print(
        "  suggested_adjustments="
        + _format_suggested_adjustments(guard_result.get("suggested_adjustments"))
    )
    if guard_result.get("should_block_generation") and args.allow_unsupported_profile:
        print("  unsupported_profile_override=True")


def _feedback_events_path(args: argparse.Namespace) -> Path:
    return args.feedback_events_path or DEFAULT_FEEDBACK_EVENTS_PATH


def _feedback_preference_context(
    args: argparse.Namespace,
    profile: dict[str, object],
) -> dict[str, object]:
    events = [] if args.feedback_disabled else load_feedback_events(args.feedback_events_path)
    return build_feedback_preference_context(
        events=events,
        household_id=str(profile.get("household_id", "")),
        member_profile_id=str(profile.get("member_profile_id", "")),
        dataset_profile=args.dataset_profile,
    )


def _print_feedback_context(
    preference_context: dict[str, object],
    args: argparse.Namespace,
) -> None:
    hard_filters = preference_context.get("hard_filters", {})
    score_preferences = preference_context.get("score_preferences", {})
    time_preferences = preference_context.get("time_preferences", {})
    meta = preference_context.get("meta", {})
    liked = _count_mapping(score_preferences.get("liked_recipe_ids"))
    disliked = _count_mapping(score_preferences.get("disliked_recipe_ids"))
    too_long = _count_mapping(time_preferences.get("too_long_recipe_ids"))
    banned = hard_filters.get("banned_recipe_ids", [])
    print("Feedback context")
    print(f"  feedback_events_path={_feedback_events_path(args)}")
    print(f"  feedback_disabled={args.feedback_disabled}")
    print(f"  event_count={meta.get('event_count', 0)}")
    print(f"  last_updated_at={meta.get('last_updated_at', '') or 'missing'}")
    print(f"  liked_recipes={len(liked)}")
    print(f"  disliked_recipes={len(disliked)}")
    print(f"  too_long_recipes={len(too_long)}")
    print(f"  avoided_recipes={len(banned) if isinstance(banned, list) else 0}")
    if banned:
        print("  avoided_recipe_ids=" + ",".join(str(item) for item in banned))


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
    filter_diagnostics = filtered_candidates.attrs.get("filter_diagnostics", {})
    if isinstance(filter_diagnostics, dict):
        print(
            "  filtered_by_explicit_avoid="
            f"{filter_diagnostics.get('filtered_by_explicit_avoid', 0)}"
        )
        avoided_ids = filter_diagnostics.get("filtered_by_explicit_avoid_recipe_ids", [])
        if avoided_ids:
            print(
                "  filtered_by_explicit_avoid_recipe_ids="
                + ",".join(str(item) for item in avoided_ids)
            )

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
                "time_feedback_penalty="
                f"{_format_number(row.get('time_feedback_penalty'), decimals=2)}, "
                f"macro_fit={_format_number(row['macro_fit'], decimals=2)}, "
                f"time_fit={_format_number(row['time_fit'], decimals=2)}, "
                f"slot_fit={_format_number(row['slot_fit'], decimals=2)}, "
                f"nutrition_quality={_format_number(row['nutrition_quality'], decimals=2)}, "
                f"is_nutrition_suspicious={row['is_nutrition_suspicious']}, "
                f"is_slot_suspicious={row.get('is_slot_suspicious')}, "
                f"feedback_fit={_format_number(row['feedback_fit'], decimals=2)}, "
                f"feedback_reasons={_format_reasons(row.get('feedback_reasons'))}, "
                f"variety_fit={_format_number(row['variety_fit'], decimals=2)}, "
                f"base_score_preview={_format_number(row['base_score_preview'], decimals=2)}, "
                f"score_preview={_format_number(row['score_preview'], decimals=2)}, "
                f"slot_fit_reasons={_format_reasons(row['slot_fit_reasons'])}, "
                "slot_suspicion_reasons="
                f"{_format_reasons(row.get('slot_suspicion_reasons'))}, "
                f"nutrition_quality_reasons={_format_reasons(row['nutrition_quality_reasons'])}"
            )


def _format_number(value: object, decimals: int = 1) -> str:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return "missing"
    return f"{float(numeric_value):.{decimals}f}"


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


def _select_one_day_plan(
    selection_mode: str,
    slot_candidates: pd.DataFrame,
    target: NutritionTarget,
    selector_config: dict[str, object] | None = None,
) -> dict[str, object]:
    slot_order = _slot_order(target)
    candidates_by_slot = _slot_candidates_by_slot(slot_candidates, slot_order)
    if selection_mode == "balanced_day":
        return select_one_day_plan_balanced(
            slot_candidates_by_slot=candidates_by_slot,
            target=target,
            slot_order=slot_order,
            config=selector_config,
        )
    plan = select_one_day_plan(
        slot_candidates_by_slot=candidates_by_slot,
        slot_order=slot_order,
    )
    plan["selector_mode"] = "greedy"
    plan["selector_diagnostics"] = {"selector_mode": "greedy"}
    return plan


def _balanced_selector_config(args: argparse.Namespace) -> dict[str, object]:
    alternative_count = max(1, int(args.alternative_count or 1))
    return {
        "return_alternatives": alternative_count > 1,
        "alternative_count": alternative_count,
        "diversity_mode": args.diversity_mode,
        "recent_recipe_ids": _parse_recent_recipe_ids(args.recent_recipe_ids),
        "meal_realism_mode": args.meal_realism_mode,
    }


def _should_use_quality_gated_reroll(args: argparse.Namespace) -> bool:
    return (
        args.selection_mode == "balanced_day"
        and args.diversity_mode == "avoid_recent"
        and args.quality_gate == "demo_safe"
    )


def _should_run_multi_day(args: argparse.Namespace) -> bool:
    return int(getattr(args, "days", 1) or 1) > 1


def _should_run_household(args: argparse.Namespace) -> bool:
    return bool(args.household_profile) and args.household_mode != HOUSEHOLD_MODE_OFF


def _run_household_generation(args: argparse.Namespace) -> None:
    household_profile = load_household_profile(args.household_profile)
    member_targets = build_member_targets(household_profile)
    household_target = build_household_aggregate_target(member_targets)
    primary_member = _primary_household_member(household_profile)
    primary_context_profile = _household_context_profile(household_profile, primary_member)
    preference_context = build_profile_preference_context(primary_context_profile)

    pool = load_recipe_candidate_pool(
        recipes_path=args.recipes,
        ingredients_path=args.ingredients,
        nutrition_path=args.nutrition,
        dataset_profile=args.dataset_profile,
    )
    fooddb = load_fooddb_current()
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=household_target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode="target_aware",
        health_and_diet_preferences=preference_context.health_and_diet_preferences,
    )
    household_candidate_config = _household_generation_config(args)
    household_candidate_config["recipe_ingredients_df"] = pool.ingredients
    household_candidates = build_household_slot_candidates(
        slot_candidates,
        member_targets,
        household_candidate_config,
    )
    candidate_diagnostics = build_candidate_diagnostics(
        slot_candidates=household_candidates,
        slot_targets=household_target.slot_targets,
    )
    plan = generate_household_plan(
        household_profile,
        slot_candidates=household_candidates,
        individual_slot_candidates=slot_candidates,
        days=args.days,
        config=household_candidate_config,
        profile=primary_member,
    )
    plan["candidate_diagnostics"] = candidate_diagnostics
    plan["pool_summary"] = _pool_summary(
        args,
        pool,
        filtered_candidates,
        household_candidates,
    )
    _print_household_plan(plan)
    if not args.no_write_outputs:
        _write_household_outputs(plan, args)


def _household_generation_config(args: argparse.Namespace) -> dict[str, object]:
    config = _multi_day_selector_config(args)
    config.update(
        {
            "household_mode": args.household_mode,
            "allocation_mode": args.household_allocation_mode,
            "household_profile_path": args.household_profile,
            "meal_realism_mode": args.meal_realism_mode,
            "quality_gate": args.quality_gate,
            "global_max_candidates_per_slot": min(
                int(config.get("global_max_candidates_per_slot", 26) or 26),
                16,
            ),
            "day_candidate_pool_size_target": min(
                int(config.get("day_candidate_pool_size_target", 75) or 75),
                40,
            ),
            "day_candidate_pool_max": min(
                int(config.get("day_candidate_pool_max", 150) or 150),
                80,
            ),
            "direct_slot_shortlist_size": min(
                int(config.get("direct_slot_shortlist_size", 12) or 12),
                8,
            ),
        }
    )
    return config


def _multi_day_selector_config(args: argparse.Namespace) -> dict[str, object]:
    config: dict[str, object] = {
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": max(3, int(args.alternative_count or 1)),
        "return_alternatives": True,
        "multi_day_mode": args.multi_day_mode,
        "candidate_day_alternative_count": 10,
        "global_max_candidates_per_slot": 26,
        "day_candidate_pool_size_target": max(10, int(args.day_candidate_pool_size or 75)),
        "day_candidate_pool_max": max(150, int(args.day_candidate_pool_size or 75)),
        "include_slot_forced_variants": True,
        "no_repeat_policy": args.multi_day_no_repeat_policy,
        "multi_day_speed_mode": args.multi_day_speed_mode,
        "direct_slot_shortlist_size": max(
            4,
            int(args.direct_slot_shortlist_size or 12),
        ),
    }
    if args.day_candidate_builder:
        config["day_candidate_builder"] = args.day_candidate_builder
    return config


def _primary_household_member(household_profile: dict[str, object]) -> dict[str, object]:
    active_ids = {
        str(member_id).strip()
        for member_id in household_profile.get("active_member_ids", [])
        if str(member_id).strip()
    }
    for member in household_profile.get("members", []):
        if str(member.get("member_id", "")).strip() in active_ids:
            return dict(member)
    raise ValueError("Household profile nu are membri activi.")


def _household_context_profile(
    household_profile: dict[str, object],
    primary_member: dict[str, object],
) -> dict[str, object]:
    profile = dict(primary_member)
    preferences = household_profile.get("household_preferences") or {}
    profile["banned_recipe_ids"] = preferences.get("banned_recipe_ids", [])
    profile["banned_ingredient_names"] = preferences.get("banned_ingredient_names", [])
    profile["dietary_preferences"] = _merged_household_dietary_preferences(
        household_profile,
        primary_member,
    )
    profile["food_preferences"] = _merged_household_food_preferences(
        household_profile,
        primary_member,
    )
    profile["health_and_diet_preferences"] = _merged_household_health_and_diet_preferences(
        household_profile,
        primary_member,
    )
    return profile


def _merged_household_dietary_preferences(
    household_profile: dict[str, object],
    primary_member: dict[str, object],
) -> dict[str, bool]:
    keys = [
        "no_beef",
        "no_pork",
        "no_chicken",
        "no_fish",
        "no_dairy",
        "vegetarian",
        "vegan",
        "gluten_free",
    ]
    result = {
        key: bool((primary_member.get("dietary_preferences") or {}).get(key, False))
        for key in keys
    }
    active_ids = {
        str(member_id).strip()
        for member_id in household_profile.get("active_member_ids", [])
        if str(member_id).strip()
    }
    for member in household_profile.get("members", []):
        if str(member.get("member_id", "")).strip() not in active_ids:
            continue
        dietary = member.get("dietary_preferences") or {}
        for key in keys:
            result[key] = bool(result[key] or dietary.get(key, False))
    return result


def _merged_household_food_preferences(
    household_profile: dict[str, object],
    primary_member: dict[str, object],
) -> dict[str, object]:
    merged_ratings: dict[str, str] = {}
    avoid_ingredients: set[str] = set()
    cooking_time_preference = "balanced"

    for member in _active_household_members(household_profile, primary_member):
        food_preferences = member.get("food_preferences") or {}
        if not isinstance(food_preferences, dict):
            continue
        ratings = food_preferences.get("ratings") or {}
        if isinstance(ratings, dict):
            for key, raw_rating in ratings.items():
                food_key = str(key).strip()
                rating = str(raw_rating).strip().lower()
                if not food_key or rating not in {"like", "dislike", "avoid"}:
                    continue
                previous = merged_ratings.get(food_key)
                if rating == "avoid" or previous is None:
                    merged_ratings[food_key] = rating
                elif rating == "dislike" and previous == "like":
                    merged_ratings[food_key] = rating
        values = food_preferences.get("avoid_ingredients") or []
        if isinstance(values, str):
            values = [values]
        try:
            avoid_ingredients.update(
                str(value).strip()
                for value in values
                if str(value).strip()
            )
        except TypeError:
            pass
        time_preference = str(
            food_preferences.get("cooking_time_preference") or ""
        ).strip().lower()
        if time_preference == "quick":
            cooking_time_preference = "quick"
        elif time_preference == "no_rush" and cooking_time_preference == "balanced":
            cooking_time_preference = "no_rush"

    return {
        "ratings": dict(sorted(merged_ratings.items())),
        "avoid_ingredients": sorted(avoid_ingredients),
        "cooking_time_preference": cooking_time_preference,
    }


def _merged_household_health_and_diet_preferences(
    household_profile: dict[str, object],
    primary_member: dict[str, object],
) -> dict[str, object]:
    result = {
        "dietary_patterns": {
            "keto": False,
            "paleo": False,
            "mediterranean": False,
        },
        "health_modes": {
            "diabetes_aware": False,
            "hypertension_friendly": False,
            "heart_friendly": False,
        },
    }
    for member in _active_household_members(household_profile, primary_member):
        preferences = member.get("health_and_diet_preferences") or {}
        if not isinstance(preferences, dict):
            continue
        dietary_patterns = preferences.get("dietary_patterns") or {}
        if isinstance(dietary_patterns, dict):
            for key in result["dietary_patterns"]:
                result["dietary_patterns"][key] = bool(
                    result["dietary_patterns"][key]
                    or dietary_patterns.get(key, False)
                )
        health_modes = preferences.get("health_modes") or {}
        if isinstance(health_modes, dict):
            for key in result["health_modes"]:
                result["health_modes"][key] = bool(
                    result["health_modes"][key] or health_modes.get(key, False)
                )
    return result


def _active_household_members(
    household_profile: dict[str, object],
    primary_member: dict[str, object],
) -> list[dict[str, object]]:
    active_ids = {
        str(member_id).strip()
        for member_id in household_profile.get("active_member_ids", [])
        if str(member_id).strip()
    }
    members = [
        dict(member)
        for member in household_profile.get("members", [])
        if str(member.get("member_id", "")).strip() in active_ids
    ]
    return members or [dict(primary_member)]


def _parse_recent_recipe_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _pool_summary(
    args: argparse.Namespace,
    pool: object,
    filtered_candidates: pd.DataFrame,
    slot_candidates: pd.DataFrame,
) -> dict[str, object]:
    filter_diagnostics = filtered_candidates.attrs.get("filter_diagnostics", {})
    return {
        "dataset_profile": args.dataset_profile,
        "recipes_path": str(args.recipes),
        "ingredients_path": str(args.ingredients),
        "nutrition_path": str(args.nutrition),
        "total_recipes_loaded": len(pool.candidates),
        "eligible_candidate_count": len(pool.eligible_candidates),
        "filtered_candidate_count": len(filtered_candidates),
        "slot_candidate_count": len(slot_candidates),
        "selection_mode": args.selection_mode,
        "portion_policy": args.portion_policy,
        "meal_realism_mode": args.meal_realism_mode,
        "quality_gate": args.quality_gate,
        "alternative_count": args.alternative_count,
        "days": args.days,
        "multi_day_mode": args.multi_day_mode,
        "multi_day_no_repeat_policy": args.multi_day_no_repeat_policy,
        "day_candidate_pool_size": args.day_candidate_pool_size,
        "multi_day_speed_mode": args.multi_day_speed_mode,
        "day_candidate_builder": args.day_candidate_builder or "auto",
        "direct_slot_shortlist_size": args.direct_slot_shortlist_size,
        "profile_guard": args.profile_guard,
        "allow_unsupported_profile": args.allow_unsupported_profile,
        "feedback_disabled": args.feedback_disabled,
        "feedback_events_path": str(_feedback_events_path(args)),
        "filtered_by_explicit_avoid": (
            filter_diagnostics.get("filtered_by_explicit_avoid", 0)
            if isinstance(filter_diagnostics, dict)
            else 0
        ),
        "filtered_by_explicit_avoid_recipe_ids": (
            filter_diagnostics.get("filtered_by_explicit_avoid_recipe_ids", [])
            if isinstance(filter_diagnostics, dict)
            else []
        ),
        "loader_warnings": pool.loader_diagnostics.get("warnings", []),
    }


def _print_multi_day_plan(plan: dict[str, object]) -> None:
    print("Selected multi-day plan preview")
    summary = plan.get("multi_day_summary", {})
    if isinstance(summary, dict):
        print(f"  multi_day_selector_mode={plan.get('multi_day_selector_mode')}")
        print(f"  requested_days={summary.get('requested_days')}")
        print(f"  actual_days_generated={summary.get('actual_days_generated')}")
        print(f"  multi_day_loss={summary.get('multi_day_loss')}")
        print(f"  unique_recipe_count={summary.get('unique_recipe_count')}")
        print(f"  repeated_recipe_count={summary.get('repeated_recipe_count')}")
        print(
            "  repeated_recipe_ids="
            + _format_reasons(summary.get("repeated_recipe_ids"))
        )
        print(
            "  strict_verdict="
            + str(summary.get("multi_day_classification", "missing"))
        )
        print(
            "  no_repeat_policy_used="
            + str(summary.get("no_repeat_policy_used", "missing"))
        )
        print(
            "  fallback_used="
            + str(summary.get("fallback_used", False))
        )
        print(
            "  fallback_reason="
            + str(summary.get("fallback_reason", ""))
        )
        print(
            "  day_candidate_builder="
            + str(summary.get("day_candidate_builder", "missing"))
        )
        print(
            "  direct_slot_shortlist_size="
            + str(summary.get("direct_slot_shortlist_size", "missing"))
        )
        print(
            "  direct_candidate_combinations_evaluated="
            + str(summary.get("direct_candidate_combinations_evaluated", 0))
        )
        print(
            "  day_candidate_pool_count="
            + str(
                summary.get(
                    "day_candidate_pool_count",
                    summary.get("candidate_day_pool_count", 0),
                )
            )
        )
        print(
            "  feasible_no_repeat_combinations="
            + str(summary.get("feasible_no_repeat_combinations", 0))
        )
        print(
            "  feasible_main_no_repeat_combinations="
            + str(summary.get("feasible_main_no_repeat_combinations", 0))
        )
        print(
            "  combination_search_truncated="
            + str(summary.get("combination_search_truncated", False))
        )
        print(
            "  fallback_from_hard_no_repeat="
            + str(summary.get("fallback_from_hard_no_repeat", False))
        )
        print(
            "  candidate_pool_build_seconds="
            + str(summary.get("candidate_pool_build_seconds", "missing"))
        )
        print(
            "  combination_selection_seconds="
            + str(summary.get("combination_selection_seconds", "missing"))
        )
        candidate_pool = summary.get("candidate_day_pool_summary", {})
        if isinstance(candidate_pool, dict) and candidate_pool:
            print(
                "  candidate_pool="
                f"total:{candidate_pool.get('candidate_day_count')} "
                f"accept:{candidate_pool.get('accept_candidate_count')} "
                f"review:{candidate_pool.get('review_candidate_count')} "
                f"reject:{candidate_pool.get('reject_candidate_count')}"
            )
    for line in multi_day_readable_lines(plan):
        print(line)
    validation = plan.get("multi_day_validation", {})
    if isinstance(validation, dict):
        print("Multi-day validation")
        print(f"  validation_status={validation.get('validation_status')}")
        print(f"  all_days_valid={validation.get('all_days_valid')}")
        print(f"  any_repeated_exact_recipe={validation.get('any_repeated_exact_recipe')}")
        print(f"  any_day_fallback_used={validation.get('any_day_fallback_used')}")
        warnings = validation.get("warnings", [])
        if warnings:
            print("  warnings=" + " | ".join(str(item) for item in warnings))
        else:
            print("  warnings=none")


def _print_household_plan(plan: dict[str, object]) -> None:
    summary = plan.get("household_summary", {})
    print("Household Generation v1 Lite")
    print(f"  household={plan.get('household_name')}")
    print(f"  mode={plan.get('household_mode')}")
    print(f"  allocation_mode={plan.get('household_allocation_mode')}")
    if isinstance(summary, dict):
        print(f"  member_count={summary.get('member_count')}")
        print(f"  days_generated={summary.get('days_generated')}")
        print(f"  household_quality_status={summary.get('household_quality_status')}")
        print(f"  household_accept_days={summary.get('household_accept_day_count')}")
        print(f"  household_review_days={summary.get('household_review_day_count')}")
        print(f"  household_reject_days={summary.get('household_reject_day_count')}")
        print(
            "  mean_abs_kcal_deviation_pct="
            f"{summary.get('mean_abs_kcal_deviation_pct')}"
        )
        print(
            "  mean_abs_protein_deviation_pct="
            f"{summary.get('mean_abs_protein_deviation_pct')}"
        )
        print(f"  min_protein_ratio={summary.get('min_protein_ratio')}")
        print(f"  worst_protein_member={summary.get('worst_protein_member')}")
        print(
            "  protein_correction_applied_count="
            f"{summary.get('protein_correction_applied_count')}"
        )
        print(f"  protein_gap_count={summary.get('protein_gap_count')}")
        print(f"  min_portion_multiplier={summary.get('min_portion_multiplier')}")
        print(f"  max_portion_multiplier={summary.get('max_portion_multiplier')}")
        print(f"  clamped_portion_count={summary.get('clamped_portion_count')}")
        print(f"  max_grocery_scaling_factor={summary.get('max_grocery_scaling_factor')}")
    for line in household_plan_readable_lines(plan)[:140]:
        print(line)


def _print_selected_day_plan(plan: dict[str, object]) -> None:
    print("Selected one-day plan preview")
    print(f"  selector_mode={plan.get('selector_mode', 'greedy')}")
    if plan.get("alternatives"):
        print("  selected_alternative_rank=1")
    for meal in plan.get("selected_meals", []):
        print(
            "  "
            f"{meal['slot']} | "
            f"{meal['recipe_id']} | "
            f"{meal['display_name']} | "
            f"portion={_format_number(meal['portion_multiplier'])}, "
            f"portion_g_est={_format_number(meal['portion_grams_estimated'], decimals=0)}, "
            f"portion_g_source={_safe_text(meal.get('portion_grams_source'))}, "
            f"portion_policy={_safe_text(meal.get('portion_policy_mode'))}, "
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
            f"time_confidence={_safe_text(meal.get('time_confidence'))}, "
            f"time_estimation_method={_safe_text(meal.get('time_estimation_method'))}, "
            f"time_warnings={_format_reasons(meal.get('time_warnings'))}, "
            f"time_estimation_reasons={_format_reasons(meal.get('time_estimation_reasons'))}, "
            "time_feedback_penalty="
            f"{_format_number(meal.get('time_feedback_penalty'), decimals=2)}, "
            "time_fit_reasons="
            f"{_format_reasons(meal.get('time_fit_reasons'))}, "
            f"nutrition_quality={_format_number(meal['nutrition_quality'], decimals=2)}, "
            f"feedback_fit={_format_number(meal.get('feedback_fit'), decimals=2)}, "
            "feedback_reasons="
            f"{_format_reasons(meal.get('feedback_reasons'))}, "
            f"is_nutrition_suspicious={meal['is_nutrition_suspicious']}, "
            f"is_slot_suspicious={meal.get('is_slot_suspicious')}, "
            f"score_preview={_format_number(meal['score_preview'], decimals=2)}, "
            f"meal_realism_score={_format_number(meal.get('meal_realism_score'), decimals=2)}, "
            "meal_realism_penalty="
            f"{_format_number(meal.get('meal_realism_penalty'), decimals=2)}, "
            "meal_realism_flags="
            f"{_format_reasons(meal.get('meal_realism_flags'))}, "
            "meal_realism_reasons="
            f"{_format_reasons(meal.get('meal_realism_reasons'))}, "
            f"realism_hard_reject={meal.get('realism_hard_reject')}, "
            "realism_reject_reason="
            f"{_format_reasons(meal.get('realism_reject_reason'))}, "
            "portion_policy_reasons="
            f"{_format_reasons(meal.get('portion_policy_reasons'))}, "
            "portion_policy_warnings="
            f"{_format_reasons(meal.get('portion_policy_warnings'))}, "
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


def _print_selector_diagnostics(plan: dict[str, object]) -> None:
    diagnostics = plan.get("selector_diagnostics")
    if not isinstance(diagnostics, dict):
        return
    if diagnostics.get("selector_mode") != "balanced_day":
        return

    print("Balanced day selector diagnostics")
    for field in (
        "day_loss",
        "base_day_loss",
        "adjusted_day_loss",
        "macro_day_loss",
        "kcal_loss",
        "protein_loss",
        "carbs_loss",
        "fat_loss",
        "meal_realism_mode",
        "meal_realism_total_penalty",
        "realism_penalty_total",
        "meal_realism_applied_penalty",
        "average_score_preview",
        "evaluated_combination_count",
        "possible_combination_count_after_shortlist",
    ):
        print(f"  {field}={diagnostics.get(field)}")
    before = diagnostics.get("candidate_count_per_slot_before_shortlist")
    after = diagnostics.get("candidate_count_per_slot_after_shortlist")
    print(f"  shortlist_before={_format_counts(before)}")
    print(f"  shortlist_after={_format_counts(after)}")
    before_realism = diagnostics.get("candidate_count_per_slot_before_realism_filter")
    after_realism = diagnostics.get("candidate_count_per_slot_after_realism_filter")
    hard_rejected = diagnostics.get("hard_rejected_count_by_slot")
    hard_candidates = diagnostics.get("hard_reject_candidate_count_by_slot")
    print(f"  realism_filter_before={_format_counts(before_realism)}")
    print(f"  realism_filter_after={_format_counts(after_realism)}")
    print(f"  hard_rejected_count_by_slot={_format_counts(hard_rejected)}")
    print(f"  hard_reject_candidate_count_by_slot={_format_counts(hard_candidates)}")
    print(f"  diversity_mode={diagnostics.get('diversity_mode', 'none')}")
    print(
        "  recent_recipe_ids_considered="
        + ",".join(str(item) for item in diagnostics.get("recent_recipe_ids_considered", []))
    )
    print(f"  alternative_count_returned={diagnostics.get('alternative_count_returned')}")
    penalties = diagnostics.get("diversity_penalties", {})
    if isinstance(penalties, dict):
        print(f"  diversity_penalties={_format_counts(penalties)}")
    realism = diagnostics.get("meal_realism_penalties", {})
    if isinstance(realism, dict):
        print(f"  meal_realism_penalties={_format_counts(realism)}")
    flags_by_meal = diagnostics.get("meal_realism_flags_by_meal") or []
    if flags_by_meal:
        print("  meal_realism_flags_by_meal=" + _format_reasons(flags_by_meal))
    hard_reject_reasons = diagnostics.get("hard_reject_reasons") or {}
    if hard_reject_reasons:
        print("  hard_reject_reasons=" + _format_reasons(hard_reject_reasons))
    warnings = diagnostics.get("selector_warnings") or []
    if warnings:
        print("  selector_warnings=" + " | ".join(str(item) for item in warnings))


def _print_quality_gate(plan: dict[str, object]) -> None:
    quality_gate = plan.get("quality_gate")
    if not isinstance(quality_gate, dict):
        return
    print("Quality gate")
    print(f"  quality_gate_status={quality_gate.get('quality_gate_status')}")
    print(f"  quality_gate_score={quality_gate.get('quality_gate_score')}")
    print(
        "  quality_gate_reasons="
        + _format_reasons(quality_gate.get("quality_gate_reasons"))
    )
    print(f"  fallback_used={plan.get('quality_gate_fallback_used', False)}")
    print(
        "  selected_diversity_mode_after_gate="
        f"{plan.get('quality_gate_selected_mode', plan.get('selector_mode'))}"
    )
    diagnostics = plan.get("quality_gate_reroll_diagnostics")
    if isinstance(diagnostics, dict):
        print(
            "  attempted_modes="
            + _format_reasons(diagnostics.get("attempted_modes"))
        )


def _print_plan_alternatives(plan: dict[str, object]) -> None:
    alternatives = plan.get("alternatives")
    if not isinstance(alternatives, list) or len(alternatives) <= 1:
        return
    print("Balanced day alternatives")
    for alternative in alternatives:
        if not isinstance(alternative, dict):
            continue
        totals = alternative.get("day_totals", {})
        meals = alternative.get("selected_meals", [])
        recipe_text = " | ".join(
            f"{meal.get('slot')}:{meal.get('display_name')}"
            for meal in meals
            if isinstance(meal, dict)
        )
        penalties = alternative.get("diversity_penalties", {})
        print(
            "  "
            f"alternative #{alternative.get('alternative_rank')}: "
            f"base_day_loss={alternative.get('base_day_loss')}, "
            f"adjusted_day_loss={alternative.get('adjusted_day_loss')}, "
            f"kcal={_format_number(totals.get('total_kcal'))}, "
            f"protein_g={_format_number(totals.get('total_protein_g'))}, "
            f"carbs_g={_format_number(totals.get('total_carbs_g'))}, "
            f"fat_g={_format_number(totals.get('total_fat_g'))}, "
            f"diversity_penalty={_safe_penalty(penalties)}, "
            "meal_realism_penalty="
            f"{alternative.get('meal_realism_total_penalty', 0.0)}, "
            "meal_realism_applied="
            f"{alternative.get('meal_realism_applied_penalty', 0.0)}, "
            f"recipes={recipe_text}"
        )


def _safe_penalty(value: object) -> object:
    if not isinstance(value, dict):
        return "missing"
    return value.get("total_diversity_penalty", 0.0)


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


def _count_mapping(value: object) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for key, item in value.items():
        try:
            count = int(item)
        except (TypeError, ValueError):
            count = 0
        if str(key).strip() and count > 0:
            result[str(key)] = count
    return result


def _format_suggested_adjustments(value: object) -> str:
    if not isinstance(value, dict) or not value:
        return "none"
    return "; ".join(f"{key}={item}" for key, item in value.items())


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


def _write_multi_day_outputs(plan: dict[str, object], args: argparse.Namespace) -> None:
    write_multi_day_plan_json(plan, args.out_multiday_json)
    write_multi_day_plan_readable(plan, args.out_multiday_txt)
    write_multi_day_meals_csv(plan, args.out_multiday_csv)
    print("Generator v1 multi-day outputs written")
    print(f"  json={args.out_multiday_json}")
    print(f"  txt={args.out_multiday_txt}")
    print(f"  csv={args.out_multiday_csv}")


def _write_household_outputs(plan: dict[str, object], args: argparse.Namespace) -> None:
    write_household_plan_json(plan, args.out_household_json)
    write_household_plan_readable(plan, args.out_household_txt)
    write_household_allocations_csv(plan, args.out_household_allocations_csv)
    write_household_member_macros_csv(plan, args.out_household_member_macros_csv)
    write_household_grocery_scaling_csv(plan, args.out_household_grocery_scaling_csv)
    print("Generator v1 household outputs written")
    print(f"  json={args.out_household_json}")
    print(f"  txt={args.out_household_txt}")
    print(f"  allocations_csv={args.out_household_allocations_csv}")
    print(f"  member_macros_csv={args.out_household_member_macros_csv}")
    print(f"  grocery_scaling_csv={args.out_household_grocery_scaling_csv}")


def _build_write_print_grocery_list(
    plan: dict[str, object],
    recipe_ingredients_df: pd.DataFrame,
    fooddb_df: pd.DataFrame,
    args: argparse.Namespace,
) -> dict[str, object]:
    enable_cooked_to_raw = (
        bool(args.grocery_purchase_suggestions)
        if args.grocery_cooked_to_raw is None
        else bool(args.grocery_cooked_to_raw)
    )
    grocery_list = build_grocery_list(
        plan,
        recipe_ingredients_df,
        fooddb_df=fooddb_df,
        config={
            "include_pantry_basics": bool(args.include_pantry_basics),
            "include_purchase_suggestions": bool(args.grocery_purchase_suggestions),
            "purchase_rules_path": args.grocery_purchase_rules_path,
            "enable_cooked_to_raw_conversion": enable_cooked_to_raw,
            "cooked_to_raw_rules_path": args.grocery_cooked_to_raw_rules_path,
            "include_price_estimates": bool(args.grocery_price_estimates),
            "product_catalog_path": args.grocery_product_catalog_path,
            "exclude_water": True,
        },
    )
    write_grocery_list_csv(
        grocery_list,
        args.grocery_out_csv,
        include_pantry_basics=bool(args.include_pantry_basics),
    )
    write_grocery_list_readable(
        grocery_list,
        args.grocery_out_txt,
        include_pantry_basics=bool(args.include_pantry_basics),
        include_purchase_suggestions=bool(args.grocery_purchase_suggestions),
        include_price_estimates=bool(args.grocery_price_estimates),
    )
    _print_grocery_list_summary(grocery_list, args)
    return grocery_list


def _print_grocery_list_summary(
    grocery_list: dict[str, object],
    args: argparse.Namespace,
) -> None:
    summary = grocery_list.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    print("Generator v1 grocery list written")
    print(f"  csv={args.grocery_out_csv}")
    print(f"  txt={args.grocery_out_txt}")
    print(
        "  items="
        f"{summary.get('shopping_item_count', 0)} shopping / "
        f"{summary.get('display_item_count', summary.get('item_count', 0))} display / "
        f"{summary.get('raw_item_count', summary.get('item_count', 0))} raw; "
        f"mapped={summary.get('mapped_item_count', 0)}; "
        f"fallback={summary.get('fallback_item_count', 0)}; "
        f"pantry_basic={summary.get('pantry_basic_count', 0)}; "
        f"alias_groups={summary.get('safe_alias_group_count', 0)}"
    )
    purchase_summary = summary.get("purchase_summary", {})
    if isinstance(purchase_summary, dict) and purchase_summary:
        print(
            "  purchase="
            f"suggestions={purchase_summary.get('items_with_purchase_suggestions', 0)}; "
            f"fallback_grams={purchase_summary.get('fallback_grams_only_count', 0)}; "
            f"packages={purchase_summary.get('package_rounded_items_count', 0)}; "
            f"pieces={purchase_summary.get('piece_rounded_items_count', 0)}; "
            f"pantry_check={purchase_summary.get('pantry_basic_count', 0)}"
        )
        print(
            "  cooked_to_raw="
            f"converted={purchase_summary.get('cooked_to_raw_converted_item_count', 0)}; "
            f"warnings={purchase_summary.get('cooked_to_raw_warning_count', 0)}; "
            f"no_conversion={purchase_summary.get('cooked_raw_no_conversion_count', 0)}"
        )
    pricing_summary = summary.get("pricing_summary", {})
    if isinstance(pricing_summary, dict) and pricing_summary:
        total = pricing_summary.get("total_estimated_cost")
        currency = pricing_summary.get("currency") or "RON"
        total_text = f"{float(total):.2f} {currency}" if total is not None else "unavailable"
        print(
            "  pricing="
            f"priced={pricing_summary.get('priced_item_count', 0)}; "
            f"missing={pricing_summary.get('unpriced_item_count', 0)}; "
            f"safe_catalog_rows={pricing_summary.get('safe_catalog_rows_loaded', 0)}; "
            f"estimated_total={total_text}; "
            "label=demo estimates"
        )
    warnings = grocery_list.get("warnings", [])
    if warnings:
        print("  warnings=" + "; ".join(str(item) for item in warnings))


if __name__ == "__main__":
    main()
