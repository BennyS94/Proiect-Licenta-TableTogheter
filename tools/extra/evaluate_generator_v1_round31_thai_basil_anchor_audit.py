from __future__ import annotations

import itertools
import sys
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
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
    V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.day_selector_balanced import (
    compute_day_loss_for_plan,
    select_one_day_plan_balanced,
)
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    _build_candidate_day_pool,
    _collect_day_candidates,
    _score_day_combination,
    generate_multi_day_plan,
)
from src.generator_v1.plan_quality_gate import evaluate_plan_quality
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round31_thai_basil_anchor_summary.txt"
OUT_REPLACEMENTS = OUT_DIR / "generator_v1_round31_thai_basil_replacement_candidates.csv"
OUT_FEASIBILITY = OUT_DIR / "generator_v1_round31_no_repeat_feasibility.csv"
OUT_COMPARISON = OUT_DIR / "generator_v1_round31_multiday_comparison.csv"
OUT_RECOMMENDATION = OUT_DIR / "generator_v1_round31_recommendation.txt"

SLOTS = ["breakfast", "lunch", "dinner", "snack"]
MAIN_SLOTS = {"lunch", "dinner"}
THAI_BASIL_NAME = "Easy and Spicy Thai Basil Chicken with Egg"
PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"
QUALITY_GATE = "demo_safe"
DAYS = 3

GLOBAL_CONFIG = {
    "selection_mode": "balanced_day",
    "portion_policy": PORTION_POLICY,
    "meal_realism_mode": MEAL_REALISM_MODE,
    "quality_gate": QUALITY_GATE,
    "alternative_count": 3,
    "return_alternatives": True,
    "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
    "candidate_day_alternative_count": 10,
    "global_max_candidates_per_slot": 26,
}
SOURCE_CONFIG = {
    "return_alternatives": True,
    "alternative_count": 10,
    "diversity_mode": "none",
    "recent_recipe_ids": [],
    "meal_realism_mode": MEAL_REALISM_MODE,
    "max_candidates_per_slot": 26,
    "min_recipe_difference_between_alternatives": 1,
}


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    context = build_context()
    current_plan = generate_current_plan(context)
    thai_id = find_recipe_id(context["slot_candidates"], THAI_BASIL_NAME)
    candidate_days = build_augmented_candidate_day_pool(context, current_plan, thai_id)
    replacement_rows = build_replacement_rows(context, current_plan, thai_id)
    feasibility_rows = build_feasibility_rows(candidate_days, current_plan, thai_id)
    comparison_rows = build_comparison_rows(candidate_days, current_plan, thai_id)
    recommendation_text = build_recommendation_text(
        current_plan=current_plan,
        thai_id=thai_id,
        replacement_rows=replacement_rows,
        feasibility_rows=feasibility_rows,
        comparison_rows=comparison_rows,
    )
    summary_text = build_summary_text(
        context=context,
        current_plan=current_plan,
        thai_id=thai_id,
        candidate_days=candidate_days,
        replacement_rows=replacement_rows,
        feasibility_rows=feasibility_rows,
        comparison_rows=comparison_rows,
        recommendation_text=recommendation_text,
    )

    pd.DataFrame(replacement_rows).to_csv(OUT_REPLACEMENTS, index=False)
    pd.DataFrame(feasibility_rows).to_csv(OUT_FEASIBILITY, index=False)
    pd.DataFrame(comparison_rows).to_csv(OUT_COMPARISON, index=False)
    OUT_RECOMMENDATION.write_text(recommendation_text, encoding="utf-8")
    OUT_SUMMARY.write_text(summary_text, encoding="utf-8")

    no_repeat = scenario_value(feasibility_rows, "no_repeat_exact_recipe", "feasible")
    no_thai = scenario_value(feasibility_rows, "no_repeat_thai_basil", "feasible")
    best_replacement = best_replacement_name(replacement_rows)
    print("Generator v1 Round31 Thai Basil anchor audit written")
    print(f"summary={OUT_SUMMARY}")
    print(f"replacement_candidates={OUT_REPLACEMENTS}")
    print(f"no_repeat_feasibility={OUT_FEASIBILITY}")
    print(f"multiday_comparison={OUT_COMPARISON}")
    print(f"recommendation={OUT_RECOMMENDATION}")
    print(f"candidate_day_count={len(candidate_days)}")
    print(f"no_repeat_exact_recipe_feasible={no_repeat}")
    print(f"no_repeat_thai_basil_feasible={no_thai}")
    print(f"best_replacement={best_replacement}")


def build_context() -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH,
        ingredients_path=V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH,
        nutrition_path=V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH,
        dataset_profile=V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
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
        portion_policy_mode=PORTION_POLICY,
    )
    return {
        "profile": profile,
        "target": target,
        "pool": pool,
        "slot_candidates": slot_candidates,
    }


def generate_current_plan(context: Mapping[str, Any]) -> dict[str, Any]:
    return generate_multi_day_plan(
        profile=context["profile"],
        target=context["target"],
        slot_candidates=context["slot_candidates"],
        days=DAYS,
        config=GLOBAL_CONFIG,
    )


def find_recipe_id(slot_candidates: pd.DataFrame, display_name: str) -> str:
    matches = slot_candidates[slot_candidates["display_name"].astype(str).eq(display_name)]
    if matches.empty:
        return ""
    return str(matches.iloc[0].get("recipe_id", "")).strip()


def build_augmented_candidate_day_pool(
    context: Mapping[str, Any],
    current_plan: Mapping[str, Any],
    thai_id: str,
) -> list[dict[str, Any]]:
    slot_candidates = context["slot_candidates"]
    candidates_by_slot = split_candidates_by_slot(slot_candidates)
    target = context["target"]
    candidate_days = _build_candidate_day_pool(
        slot_candidates_by_slot=candidates_by_slot,
        target=target,
        slot_order=SLOTS,
        config=GLOBAL_CONFIG,
    )
    seen_keys = {tuple(candidate.get("recipe_key", ())) for candidate in candidate_days}
    seen_keys.discard(())

    collect_current_plan_days(candidate_days, seen_keys, current_plan)

    for source_mode, filtered_ids, diversity_mode, recent_ids in extra_source_specs(
        current_plan,
        thai_id,
        slot_candidates,
    ):
        filtered_by_slot = filter_candidates_by_recipe_ids(candidates_by_slot, filtered_ids)
        source_plan = select_source_plan(
            slot_candidates_by_slot=filtered_by_slot,
            target=target,
            diversity_mode=diversity_mode,
            recent_recipe_ids=recent_ids,
        )
        _collect_day_candidates(
            candidates=candidate_days,
            seen_keys=seen_keys,
            source_mode=source_mode,
            source_plan=source_plan,
            target=target,
        )

    return sorted(
        candidate_days,
        key=lambda item: (
            quality_rank(str(item.get("quality_gate_status"))),
            to_float(item.get("adjusted_day_loss", item.get("base_day_loss"))),
            str(item.get("candidate_day_id")),
        ),
    )


def collect_current_plan_days(
    candidate_days: list[dict[str, Any]],
    seen_keys: set[tuple[str, ...]],
    current_plan: Mapping[str, Any],
) -> None:
    for day in current_plan.get("days", []):
        if not isinstance(day, Mapping):
            continue
        recipe_key = tuple(sorted(day_recipe_ids(day)))
        if not recipe_key or recipe_key in seen_keys:
            continue
        seen_keys.add(recipe_key)
        diagnostics = day.get("selector_diagnostics", {})
        if not isinstance(diagnostics, Mapping):
            diagnostics = {}
        candidate_days.append(
            {
                "candidate_day_id": f"current_day_{day.get('day_index')}",
                "source_mode": "current_plan",
                "alternative_rank": day.get("day_index"),
                "plan": day_to_plan(day),
                "recipe_key": recipe_key,
                "quality_gate_status": day.get("quality_gate_status", "missing"),
                "validation_status": day.get("validation_status", "not_validated"),
                "base_day_loss": to_float(diagnostics.get("base_day_loss")),
                "adjusted_day_loss": to_float(diagnostics.get("adjusted_day_loss")),
                "meal_realism_warning_count": meal_realism_warning_count(
                    day.get("selected_meals", []),
                ),
            }
        )


def day_to_plan(day: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "selected_meals": list(day.get("selected_meals", [])),
        "day_totals": dict(day.get("day_totals", {})),
        "warnings": list(day.get("warnings", [])),
        "selector_mode": "balanced_day",
        "selector_diagnostics": dict(day.get("selector_diagnostics", {})),
        "validation": dict(day.get("validation", {})),
        "quality_gate": dict(day.get("quality_gate", {})),
        "quality_gate_status": day.get("quality_gate_status"),
        "quality_gate_reasons": day.get("quality_gate_reasons", []),
        "quality_gate_fallback_used": day.get("fallback_used", False),
        "quality_gate_selected_mode": day.get("diversity_mode_used"),
    }


def extra_source_specs(
    current_plan: Mapping[str, Any],
    thai_id: str,
    slot_candidates: pd.DataFrame,
) -> list[tuple[str, set[str], str, list[str]]]:
    specs: list[tuple[str, set[str], str, list[str]]] = []
    if thai_id:
        specs.extend(
            [
                ("avoid_thai_recent", set(), "avoid_recent", [thai_id]),
                ("soft_thai_recent", set(), "soft", [thai_id]),
                ("filter_no_thai", {thai_id}, "none", []),
                ("filter_no_thai_soft", {thai_id}, "soft", []),
            ]
        )

    current_day_ids = [day_recipe_ids(day) for day in current_plan.get("days", []) if isinstance(day, Mapping)]
    for index, recipe_ids in enumerate(current_day_ids, start=1):
        specs.append((f"avoid_current_day_{index}", set(), "avoid_recent", recipe_ids))

    for key, recipe_id in current_main_recipe_ids(current_plan).items():
        if recipe_id:
            specs.append((f"filter_no_{key}", {recipe_id}, "none", []))

    for recipe_id in top_main_recipe_ids(slot_candidates, limit=8):
        if recipe_id:
            short_id = recipe_id.replace("recipes_", "").replace("manual_", "")
            specs.append((f"filter_no_main_{short_id[:18]}", {recipe_id}, "none", []))

    unique_specs: list[tuple[str, set[str], str, list[str]]] = []
    seen_keys: set[tuple[str, tuple[str, ...], str, tuple[str, ...]]] = set()
    for source_mode, filtered_ids, diversity_mode, recent_ids in specs:
        key = (
            source_mode,
            tuple(sorted(filtered_ids)),
            diversity_mode,
            tuple(sorted(recent_ids)),
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_specs.append((source_mode, filtered_ids, diversity_mode, recent_ids))
    return unique_specs


def current_main_recipe_ids(current_plan: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for day in current_plan.get("days", []):
        if not isinstance(day, Mapping):
            continue
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, Mapping) or meal_slot(meal) not in MAIN_SLOTS:
                continue
            key = f"day{day.get('day_index')}_{meal_slot(meal)}"
            result[key] = meal_recipe_id(meal)
    return result


def top_main_recipe_ids(slot_candidates: pd.DataFrame, limit: int) -> list[str]:
    rows = []
    for slot in sorted(MAIN_SLOTS):
        frame = slot_candidates[slot_candidates["slot"].astype(str).eq(slot)].copy()
        if frame.empty:
            continue
        frame["_sort"] = frame.apply(
            lambda row: (
                -to_float(row.get("score_preview")),
                -to_float(row.get("macro_fit")),
                to_float(row.get("effective_time_min_for_scoring")),
            ),
            axis=1,
        )
        seen: set[str] = set()
        for _, row in frame.sort_values("_sort").iterrows():
            recipe_id = str(row.get("recipe_id", "")).strip()
            if not recipe_id or recipe_id in seen:
                continue
            seen.add(recipe_id)
            rows.append((recipe_id, to_float(row.get("score_preview"))))
            if len(seen) >= limit:
                break
    return [recipe_id for recipe_id, _score in sorted(set(rows), key=lambda item: (-item[1], item[0]))[:limit]]


def select_source_plan(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget,
    diversity_mode: str,
    recent_recipe_ids: Sequence[str],
) -> dict[str, Any]:
    config = dict(SOURCE_CONFIG)
    config["diversity_mode"] = diversity_mode
    config["recent_recipe_ids"] = list(recent_recipe_ids)
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=target,
        slot_order=SLOTS,
        config=config,
    )
    plan["validation"] = validate_one_day_plan(plan, target)
    plan["quality_gate"] = evaluate_plan_quality(
        plan,
        target,
        config={"quality_gate": QUALITY_GATE},
    )
    return plan


def split_candidates_by_slot(slot_candidates: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        slot: slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        for slot in SLOTS
    }


def filter_candidates_by_recipe_ids(
    candidates_by_slot: Mapping[str, pd.DataFrame],
    recipe_ids: set[str],
) -> dict[str, pd.DataFrame]:
    if not recipe_ids:
        return {slot: frame.copy() for slot, frame in candidates_by_slot.items()}
    return {
        slot: frame.loc[~frame["recipe_id"].astype(str).isin(recipe_ids)].copy()
        for slot, frame in candidates_by_slot.items()
    }


def build_replacement_rows(
    context: Mapping[str, Any],
    current_plan: Mapping[str, Any],
    thai_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not thai_id:
        return rows
    occurrences = thai_occurrences(current_plan, thai_id)
    slot_candidates = context["slot_candidates"]
    target = context["target"]
    for occurrence in occurrences:
        day = occurrence["day"]
        anchor_meal = occurrence["meal"]
        slot = meal_slot(anchor_meal)
        alternatives = top_replacement_candidates(
            slot_candidates=slot_candidates,
            target=target,
            slot=slot,
            excluded_recipe_id=thai_id,
            limit=10,
        )
        anchor_loss = slot_candidate_loss(anchor_meal, target, slot)
        for rank, replacement in enumerate(alternatives, start=1):
            replacement_meal = meal_from_row(replacement)
            swap = evaluate_swap(day, replacement_meal, slot, target)
            replacement_loss = slot_candidate_loss(replacement_meal, target, slot)
            flags = meal_flags(replacement_meal)
            rows.append(
                {
                    "anchor_recipe_id": thai_id,
                    "anchor_display_name": THAI_BASIL_NAME,
                    "anchor_day_index": day.get("day_index"),
                    "anchor_slot": slot,
                    "anchor_portion": anchor_meal.get("portion_multiplier"),
                    "anchor_grams": anchor_meal.get("portion_grams_estimated"),
                    "anchor_kcal": anchor_meal.get("kcal"),
                    "anchor_protein_g": anchor_meal.get("protein_g"),
                    "anchor_carbs_g": anchor_meal.get("carbs_g"),
                    "anchor_fat_g": anchor_meal.get("fat_g"),
                    "replacement_rank": rank,
                    "replacement_recipe_id": meal_recipe_id(replacement_meal),
                    "replacement_display_name": meal_name(replacement_meal),
                    "replacement_portion": replacement_meal.get("portion_multiplier"),
                    "replacement_grams": replacement_meal.get("portion_grams_estimated"),
                    "replacement_kcal": replacement_meal.get("kcal"),
                    "replacement_protein_g": replacement_meal.get("protein_g"),
                    "replacement_carbs_g": replacement_meal.get("carbs_g"),
                    "replacement_fat_g": replacement_meal.get("fat_g"),
                    "replacement_effective_time_min": replacement_meal.get("effective_time_min_for_scoring"),
                    "anchor_slot_loss": round(anchor_loss, 6),
                    "replacement_slot_loss": round(replacement_loss, 6),
                    "loss_gap_vs_anchor_slot": round(replacement_loss - anchor_loss, 6),
                    "base_day_loss_before_swap": swap.get("base_day_loss_before_swap"),
                    "base_day_loss_after_swap": swap.get("base_day_loss_after_swap"),
                    "day_loss_delta_if_swapped": swap.get("day_loss_delta_if_swapped"),
                    "quality_gate_status_after_swap": swap.get("quality_gate_status_after_swap"),
                    "validation_status_after_swap": swap.get("validation_status_after_swap"),
                    "realism_flags": ";".join(flags),
                    "why_replacement_loses": why_replacement_loses(
                        anchor_meal,
                        replacement_meal,
                        to_float(replacement_loss - anchor_loss),
                        to_float(swap.get("day_loss_delta_if_swapped")),
                        str(swap.get("quality_gate_status_after_swap", "")),
                        flags,
                    ),
                }
            )
    return rows


def thai_occurrences(
    current_plan: Mapping[str, Any],
    thai_id: str,
) -> list[dict[str, Mapping[str, Any]]]:
    rows = []
    for day in current_plan.get("days", []):
        if not isinstance(day, Mapping):
            continue
        for meal in day.get("selected_meals", []):
            if isinstance(meal, Mapping) and meal_recipe_id(meal) == thai_id:
                rows.append({"day": day, "meal": meal})
    return rows


def top_replacement_candidates(
    slot_candidates: pd.DataFrame,
    target: NutritionTarget,
    slot: str,
    excluded_recipe_id: str,
    limit: int,
) -> list[pd.Series]:
    frame = slot_candidates.loc[
        slot_candidates["slot"].astype(str).eq(slot)
        & ~slot_candidates["recipe_id"].astype(str).eq(excluded_recipe_id)
    ].copy()
    if frame.empty:
        return []
    frame["_slot_candidate_loss"] = frame.apply(
        lambda row: slot_candidate_loss(row, target, slot),
        axis=1,
    )
    frame["_sort_key"] = frame.apply(
        lambda row: (
            to_float(row.get("_slot_candidate_loss")),
            -to_float(row.get("score_preview")),
            to_float(row.get("effective_time_min_for_scoring")),
            str(row.get("display_name", "")),
        ),
        axis=1,
    )
    rows: list[pd.Series] = []
    seen: set[str] = set()
    for _, row in frame.sort_values("_sort_key").iterrows():
        recipe_id = str(row.get("recipe_id", "")).strip()
        if not recipe_id or recipe_id in seen:
            continue
        seen.add(recipe_id)
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def evaluate_swap(
    reference_day: Mapping[str, Any],
    replacement_meal: Mapping[str, Any],
    slot: str,
    target: NutritionTarget,
) -> dict[str, Any]:
    original_meals = [
        dict(meal)
        for meal in reference_day.get("selected_meals", [])
        if isinstance(meal, Mapping)
    ]
    replacement_meals = [
        dict(replacement_meal) if meal_slot(meal) == slot else dict(meal)
        for meal in original_meals
    ]
    before_loss = compute_day_loss_for_plan(
        original_meals,
        target,
        config={"meal_realism_mode": MEAL_REALISM_MODE},
    )
    after_loss = compute_day_loss_for_plan(
        replacement_meals,
        target,
        config={"meal_realism_mode": MEAL_REALISM_MODE},
    )
    plan = {
        "selected_meals": replacement_meals,
        "day_totals": day_totals(replacement_meals),
        "warnings": [],
        "selector_mode": "balanced_day",
        "selector_diagnostics": {
            "base_day_loss": after_loss.get("day_loss"),
            "adjusted_day_loss": after_loss.get("day_loss"),
            **after_loss,
        },
    }
    validation = validate_one_day_plan(plan, target)
    plan["validation"] = validation
    quality = evaluate_plan_quality(plan, target, config={"quality_gate": QUALITY_GATE})
    before_value = to_float(before_loss.get("day_loss"))
    after_value = to_float(after_loss.get("day_loss"))
    return {
        "base_day_loss_before_swap": round(before_value, 6),
        "base_day_loss_after_swap": round(after_value, 6),
        "day_loss_delta_if_swapped": round(after_value - before_value, 6),
        "quality_gate_status_after_swap": quality.get("quality_gate_status"),
        "validation_status_after_swap": validation.get("validation_status"),
    }


def build_feasibility_rows(
    candidate_days: Sequence[Mapping[str, Any]],
    current_plan: Mapping[str, Any],
    thai_id: str,
) -> list[dict[str, Any]]:
    scenarios: list[tuple[str, str, Callable[[Mapping[str, Any]], bool] | None]] = [
        ("current_plus30_plus15_global", "Actual current global result.", None),
        ("no_repeat_exact_recipe", "No exact recipe repeats across 3 days.", no_repeat_exact),
        ("no_repeat_thai_basil", "Thai Basil appears at most once.", lambda metrics: recipe_count_ok(metrics, thai_id)),
        ("no_repeat_lunch_dinner_main", "No lunch/dinner main repeats.", no_repeat_main),
        ("accept_only_no_repeat", "All days accept and no exact recipe repeats.", accept_only_no_repeat),
        ("allow_one_review_no_repeat", "At most one review day and no exact recipe repeats.", allow_one_review_no_repeat),
    ]
    rows = []
    for scenario, description, constraint in scenarios:
        if scenario == "current_plus30_plus15_global":
            rows.append(current_plan_row(current_plan, scenario, description, thai_id))
            continue
        combo, metrics = find_best_combination(candidate_days, constraint, "current")
        if combo is None or metrics is None:
            rows.append(
                {
                    "scenario": scenario,
                    "description": description,
                    "feasible": False,
                    "infeasible_reason": infeasible_reason(scenario, candidate_days, thai_id),
                }
            )
            continue
        rows.append(combination_row(scenario, description, combo, metrics, thai_id))
    return rows


def build_comparison_rows(
    candidate_days: Sequence[Mapping[str, Any]],
    current_plan: Mapping[str, Any],
    thai_id: str,
) -> list[dict[str, Any]]:
    current_summary = current_plan.get("multi_day_summary", {})
    current_loss = to_float(current_summary.get("multi_day_loss", current_plan.get("multi_day_loss")))
    variants = [
        (
            "current_repeat_penalty",
            "Existing objective.",
            "current",
            None,
        ),
        (
            "small_extra_same_lunch_dinner_repeat_penalty",
            "Audit-only +0.25 for each repeated lunch/dinner main occurrence.",
            "small_main_repeat",
            None,
        ),
        (
            "small_extra_exact_recipe_repeat_penalty",
            "Audit-only +0.15 for each exact repeat occurrence.",
            "small_exact_repeat",
            None,
        ),
        (
            "hard_no_repeat_lunch_dinner_only",
            "Audit-only hard no-repeat for lunch/dinner mains.",
            "current",
            no_repeat_main,
        ),
    ]
    rows = []
    for variant, description, score_variant, constraint in variants:
        combo, metrics = find_best_combination(candidate_days, constraint, score_variant)
        if combo is None or metrics is None:
            rows.append(
                {
                    "variant": variant,
                    "description": description,
                    "feasible": False,
                    "infeasible_reason": "no_valid_combination_for_variant",
                }
            )
            continue
        quality_drop = (
            int(metrics.get("review_day_count", 0) or 0) > int(current_summary.get("review_day_count", 0) or 0)
            or int(metrics.get("accept_day_count", 0) or 0) < int(current_summary.get("accept_day_count", 0) or 0)
            or to_float(metrics.get("multi_day_loss")) > current_loss + 0.08
        )
        rows.append(
            {
                "variant": variant,
                "description": description,
                "feasible": True,
                "valid_day_count": metrics["valid_day_count"],
                "accept_day_count": metrics["accept_day_count"],
                "review_day_count": metrics["review_day_count"],
                "reject_day_count": metrics["reject_day_count"],
                "unique_recipe_count": metrics["unique_recipe_count"],
                "repeated_recipe_count": metrics["repeated_recipe_count"],
                "repeated_recipe_ids": format_list(metrics["repeated_recipe_ids"]),
                "repeated_main_recipe_count": metrics["repeated_main_recipe_count"],
                "thai_basil_count": metrics["recipe_counts"].get(thai_id, 0),
                "multi_day_loss": metrics["multi_day_loss"],
                "objective_loss_used": metrics["objective_loss"],
                "average_base_day_loss": metrics["average_base_day_loss"],
                "average_adjusted_day_loss": metrics["average_adjusted_day_loss"],
                "finds_exact_no_repeat": metrics["repeated_recipe_count"] == 0,
                "finds_no_repeated_main": metrics["repeated_main_recipe_count"] == 0,
                "finds_no_repeated_thai": metrics["recipe_counts"].get(thai_id, 0) <= 1,
                "quality_drop_too_much": quality_drop,
                "selected_candidate_day_ids": format_list(metrics["selected_candidate_day_ids"]),
                "selected_plan_summary": metrics["display_names_by_day"],
                "strict_verdict": strict_verdict(metrics),
            }
        )
    return rows


def find_best_combination(
    candidate_days: Sequence[Mapping[str, Any]],
    constraint: Callable[[Mapping[str, Any]], bool] | None,
    score_variant: str,
) -> tuple[tuple[Mapping[str, Any], ...] | None, dict[str, Any] | None]:
    usable = [
        candidate
        for candidate in candidate_days
        if str(candidate.get("validation_status")) == "valid"
        and str(candidate.get("quality_gate_status")) in {"accept", "review"}
    ]
    best_combo: tuple[Mapping[str, Any], ...] | None = None
    best_metrics: dict[str, Any] | None = None
    best_key: tuple[Any, ...] | None = None
    for combo in itertools.combinations(usable, DAYS):
        metrics = combination_metrics(combo, score_variant)
        if constraint is not None and not constraint(metrics):
            continue
        key = (
            metrics["reject_day_count"],
            metrics["review_day_count"],
            metrics["objective_loss"],
            metrics["repeated_recipe_count"],
            metrics["average_base_day_loss"],
            ";".join(str(item) for item in metrics["selected_candidate_day_ids"]),
        )
        if best_key is None or key < best_key:
            best_combo = combo
            best_metrics = metrics
            best_key = key
    return best_combo, best_metrics


def combination_metrics(
    combo: Sequence[Mapping[str, Any]],
    score_variant: str,
) -> dict[str, Any]:
    base_score = _score_day_combination(combo)
    meals = combo_meals(combo)
    recipe_counts = Counter(meal_recipe_id(meal) for meal in meals if meal_recipe_id(meal))
    main_counts = Counter(
        meal_recipe_id(meal)
        for meal in meals
        if meal_slot(meal) in MAIN_SLOTS and meal_recipe_id(meal)
    )
    slot_recipe_counts = Counter(
        (meal_slot(meal), meal_recipe_id(meal))
        for meal in meals
        if meal_slot(meal) and meal_recipe_id(meal)
    )
    quality_counts = Counter(str(candidate.get("quality_gate_status", "missing")) for candidate in combo)
    validation_counts = Counter(str(candidate.get("validation_status", "missing")) for candidate in combo)
    repeated_recipe_ids = sorted(recipe_id for recipe_id, count in recipe_counts.items() if count > 1)
    repeated_main_ids = sorted(recipe_id for recipe_id, count in main_counts.items() if count > 1)
    repeated_same_slot_ids = sorted(
        recipe_id
        for (_slot, recipe_id), count in slot_recipe_counts.items()
        if count > 1
    )
    repeat_occurrences = sum(max(0, count - 1) for count in recipe_counts.values())
    repeated_main_occurrences = sum(max(0, count - 1) for count in main_counts.values())
    objective_loss = to_float(base_score.get("multi_day_loss"))
    if score_variant == "small_main_repeat":
        objective_loss += 0.25 * repeated_main_occurrences
    elif score_variant == "small_exact_repeat":
        objective_loss += 0.15 * repeat_occurrences
    average_base = sum(to_float(candidate.get("base_day_loss")) for candidate in combo) / max(1, len(combo))
    average_adjusted = sum(to_float(candidate.get("adjusted_day_loss")) for candidate in combo) / max(1, len(combo))
    return {
        **base_score,
        "multi_day_loss": round(to_float(base_score.get("multi_day_loss")), 6),
        "objective_loss": round(objective_loss, 6),
        "average_base_day_loss": round(average_base, 6),
        "average_adjusted_day_loss": round(average_adjusted, 6),
        "valid_day_count": validation_counts.get("valid", 0),
        "accept_day_count": quality_counts.get("accept", 0),
        "review_day_count": quality_counts.get("review", 0),
        "reject_day_count": quality_counts.get("reject", 0),
        "unique_recipe_count": len(recipe_counts),
        "repeated_recipe_ids": repeated_recipe_ids,
        "repeated_recipe_count": len(repeated_recipe_ids),
        "repeated_recipe_occurrence_count": repeat_occurrences,
        "repeated_main_ids": repeated_main_ids,
        "repeated_main_recipe_count": len(repeated_main_ids),
        "repeated_main_occurrence_count": repeated_main_occurrences,
        "repeated_same_slot_ids": repeated_same_slot_ids,
        "recipe_counts": dict(recipe_counts),
        "main_recipe_counts": dict(main_counts),
        "selected_candidate_day_ids": [candidate.get("candidate_day_id") for candidate in combo],
        "display_names_by_day": display_names_by_day(combo),
    }


def current_plan_row(
    current_plan: Mapping[str, Any],
    scenario: str,
    description: str,
    thai_id: str,
) -> dict[str, Any]:
    meals = []
    day_ids = []
    for day in current_plan.get("days", []):
        if not isinstance(day, Mapping):
            continue
        day_ids.append(day.get("candidate_day_id"))
        for meal in day.get("selected_meals", []):
            if isinstance(meal, Mapping):
                enriched = dict(meal)
                enriched["combo_day"] = day.get("day_index")
                meals.append(enriched)
    recipe_counts = Counter(meal_recipe_id(meal) for meal in meals if meal_recipe_id(meal))
    main_counts = Counter(
        meal_recipe_id(meal)
        for meal in meals
        if meal_slot(meal) in MAIN_SLOTS and meal_recipe_id(meal)
    )
    summary = current_plan.get("multi_day_summary", {})
    quality_counts = Counter(str(day.get("quality_gate_status", "missing")) for day in current_plan.get("days", []))
    return {
        "scenario": scenario,
        "description": description,
        "feasible": True,
        "valid_day_count": summary.get("valid_day_count"),
        "accept_day_count": quality_counts.get("accept", summary.get("accept_day_count")),
        "review_day_count": quality_counts.get("review", summary.get("review_day_count")),
        "reject_day_count": quality_counts.get("reject", summary.get("reject_day_count")),
        "unique_recipe_count": summary.get("unique_recipe_count"),
        "repeated_recipe_count": summary.get("repeated_recipe_count"),
        "repeated_recipe_ids": format_list(summary.get("repeated_recipe_ids")),
        "repeated_main_recipe_count": sum(1 for count in main_counts.values() if count > 1),
        "thai_basil_count": recipe_counts.get(thai_id, 0),
        "multi_day_loss": summary.get("multi_day_loss", current_plan.get("multi_day_loss")),
        "average_base_day_loss": summary.get("average_day_loss"),
        "average_adjusted_day_loss": summary.get("average_day_loss"),
        "selected_candidate_day_ids": format_list(day_ids),
        "selected_plan_summary": plan_display_names_by_day(current_plan),
        "strict_verdict": strict_verdict_from_counts(
            int(summary.get("valid_day_count", 0) or 0),
            quality_counts.get("accept", 0),
            quality_counts.get("review", 0),
            quality_counts.get("reject", 0),
            int(summary.get("repeated_recipe_count", 0) or 0),
            sum(1 for count in main_counts.values() if count > 1),
        ),
    }


def combination_row(
    scenario: str,
    description: str,
    combo: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, Any],
    thai_id: str,
) -> dict[str, Any]:
    return {
        "scenario": scenario,
        "description": description,
        "feasible": True,
        "valid_day_count": metrics["valid_day_count"],
        "accept_day_count": metrics["accept_day_count"],
        "review_day_count": metrics["review_day_count"],
        "reject_day_count": metrics["reject_day_count"],
        "unique_recipe_count": metrics["unique_recipe_count"],
        "repeated_recipe_count": metrics["repeated_recipe_count"],
        "repeated_recipe_ids": format_list(metrics["repeated_recipe_ids"]),
        "repeated_main_recipe_count": metrics["repeated_main_recipe_count"],
        "thai_basil_count": metrics["recipe_counts"].get(thai_id, 0),
        "multi_day_loss": metrics["multi_day_loss"],
        "average_base_day_loss": metrics["average_base_day_loss"],
        "average_adjusted_day_loss": metrics["average_adjusted_day_loss"],
        "selected_candidate_day_ids": format_list(metrics["selected_candidate_day_ids"]),
        "selected_plan_summary": metrics["display_names_by_day"],
        "strict_verdict": strict_verdict(metrics),
    }


def no_repeat_exact(metrics: Mapping[str, Any]) -> bool:
    return int(metrics.get("repeated_recipe_count", 0) or 0) == 0


def no_repeat_main(metrics: Mapping[str, Any]) -> bool:
    return int(metrics.get("repeated_main_recipe_count", 0) or 0) == 0


def recipe_count_ok(metrics: Mapping[str, Any], recipe_id: str) -> bool:
    if not recipe_id:
        return True
    counts = metrics.get("recipe_counts", {})
    return isinstance(counts, Mapping) and int(counts.get(recipe_id, 0) or 0) <= 1


def accept_only_no_repeat(metrics: Mapping[str, Any]) -> bool:
    return (
        int(metrics.get("accept_day_count", 0) or 0) == DAYS
        and no_repeat_exact(metrics)
    )


def allow_one_review_no_repeat(metrics: Mapping[str, Any]) -> bool:
    return (
        int(metrics.get("review_day_count", 0) or 0) <= 1
        and int(metrics.get("reject_day_count", 0) or 0) == 0
        and no_repeat_exact(metrics)
    )


def build_recommendation_text(
    current_plan: Mapping[str, Any],
    thai_id: str,
    replacement_rows: Sequence[Mapping[str, Any]],
    feasibility_rows: Sequence[Mapping[str, Any]],
    comparison_rows: Sequence[Mapping[str, Any]],
) -> str:
    no_repeat = scenario_row(feasibility_rows, "no_repeat_exact_recipe")
    no_thai = scenario_row(feasibility_rows, "no_repeat_thai_basil")
    no_main = scenario_row(feasibility_rows, "no_repeat_lunch_dinner_main")
    accept_no_repeat = scenario_row(feasibility_rows, "accept_only_no_repeat")
    one_review_no_repeat = scenario_row(feasibility_rows, "allow_one_review_no_repeat")
    small_main = variant_row(comparison_rows, "small_extra_same_lunch_dinner_repeat_penalty")
    small_exact = variant_row(comparison_rows, "small_extra_exact_recipe_repeat_penalty")

    if bool_value(accept_no_repeat.get("feasible")):
        choice = "A. No-repeat now feasible: implement/tune no-repeat constraint."
        rationale = "Exista o combinatie all-accept fara repetitii exacte in pool-ul curent."
    elif bool_value(no_repeat.get("feasible")) or bool_value(one_review_no_repeat.get("feasible")):
        choice = "A. No-repeat now feasible: implement/tune no-repeat constraint."
        rationale = "No-repeat este fezabil, dar poate necesita acceptarea unei zile review sau a unei mici pierderi de fit."
    elif bool_value(no_main.get("feasible")):
        choice = "B. No-repeat not feasible but no repeated main is feasible: implement main-only repeat constraint."
        rationale = "Nu exista no-repeat exact, dar mesele principale pot evita repetitia fara sa blocheze planul."
    elif small_penalty_solves(small_main, thai_id) or small_penalty_solves(small_exact, thai_id):
        choice = "C. Thai Basil repetition can be solved by small repetition penalty: tune multi-day objective."
        rationale = "Simularea cu penalizare mica gaseste plan fara Thai Basil repetat fara degradare mare de calitate."
    elif replacement_pool_is_weak(replacement_rows):
        choice = "D. Still data-limited: add a few more specific lunch/dinner alternatives."
        rationale = "Inlocuitorii directi sunt mai slabi pe proteina/carbohidrati sau cresc day_loss prea mult."
    else:
        choice = "E. Accept plus30_plus15 as technical demo and move on."
        rationale = "Planul este all-accept si repetitia ramasa este o singura ancora main, documentabila pentru demo."

    current_summary = current_plan.get("multi_day_summary", {})
    lines = [
        "Round31 recommendation",
        "",
        choice,
        f"Rationale: {rationale}",
        "",
        f"Current repeated_recipe_ids={format_list(current_summary.get('repeated_recipe_ids'))}",
        f"Thai Basil id={thai_id or 'missing'}",
        f"no_repeat_exact_recipe feasible={bool_value(no_repeat.get('feasible'))}",
        f"no_repeat_thai_basil feasible={bool_value(no_thai.get('feasible'))}",
        f"no_repeat_lunch_dinner_main feasible={bool_value(no_main.get('feasible'))}",
        f"accept_only_no_repeat feasible={bool_value(accept_no_repeat.get('feasible'))}",
        f"allow_one_review_no_repeat feasible={bool_value(one_review_no_repeat.get('feasible'))}",
        "",
        "Best direct replacement: " + best_replacement_name(replacement_rows),
    ]
    return "\n".join(lines) + "\n"


def build_summary_text(
    context: Mapping[str, Any],
    current_plan: Mapping[str, Any],
    thai_id: str,
    candidate_days: Sequence[Mapping[str, Any]],
    replacement_rows: Sequence[Mapping[str, Any]],
    feasibility_rows: Sequence[Mapping[str, Any]],
    comparison_rows: Sequence[Mapping[str, Any]],
    recommendation_text: str,
) -> str:
    summary = current_plan.get("multi_day_summary", {})
    quality_counts = Counter(str(candidate.get("quality_gate_status", "missing")) for candidate in candidate_days)
    validation_counts = Counter(str(candidate.get("validation_status", "missing")) for candidate in candidate_days)
    thai_occurrence_rows = thai_occurrences(current_plan, thai_id)
    lines = [
        "Round31 Thai Basil anchor audit",
        "",
        f"Dataset profile: {V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE}",
        f"recipes_loaded={len(context['pool'].candidates)}",
        f"eligible_candidates={len(context['pool'].eligible_candidates)}",
        "",
        "Current plus30_plus15 global plan:",
        f"- valid={summary.get('valid_day_count')}/3",
        f"- accept={summary.get('accept_day_count')}",
        f"- review={summary.get('review_day_count')}",
        f"- unique={summary.get('unique_recipe_count')}",
        f"- repeated={summary.get('repeated_recipe_count')}",
        f"- repeated_recipe_ids={format_list(summary.get('repeated_recipe_ids'))}",
        f"- multi_day_loss={summary.get('multi_day_loss')}",
        "",
        "Thai Basil anchor:",
        f"- recipe_id={thai_id or 'missing'}",
        "- selected_slots="
        + format_list([f"day{row['day'].get('day_index')}:{meal_slot(row['meal'])}" for row in thai_occurrence_rows]),
        "- macro_profile="
        + thai_macro_profile(thai_occurrence_rows),
        "- why_it_wins="
        + thai_win_reason(thai_occurrence_rows, replacement_rows),
        "",
        "Candidate day pool:",
        f"- total={len(candidate_days)}",
        f"- valid={validation_counts.get('valid', 0)}",
        f"- accept={quality_counts.get('accept', 0)}",
        f"- review={quality_counts.get('review', 0)}",
        f"- reject={quality_counts.get('reject', 0)}",
        "",
        "Scenario feasibility:",
    ]
    for row in feasibility_rows:
        if not bool_value(row.get("feasible")):
            lines.append(f"- {row.get('scenario')}: infeasible, reason={row.get('infeasible_reason')}")
            continue
        lines.append(
            f"- {row.get('scenario')}: feasible={row.get('feasible')}, "
            f"accept={row.get('accept_day_count')}, review={row.get('review_day_count')}, "
            f"repeated={row.get('repeated_recipe_count')}, repeated_main={row.get('repeated_main_recipe_count')}, "
            f"thai_count={row.get('thai_basil_count')}, unique={row.get('unique_recipe_count')}, "
            f"loss={row.get('multi_day_loss')}, verdict={row.get('strict_verdict')}"
        )

    lines.extend(["", "Objective tuning audit:"])
    for row in comparison_rows:
        if not bool_value(row.get("feasible")):
            lines.append(f"- {row.get('variant')}: infeasible")
            continue
        lines.append(
            f"- {row.get('variant')}: repeated={row.get('repeated_recipe_count')}, "
            f"repeated_main={row.get('repeated_main_recipe_count')}, "
            f"thai_count={row.get('thai_basil_count')}, "
            f"accept={row.get('accept_day_count')}, review={row.get('review_day_count')}, "
            f"loss={row.get('multi_day_loss')}, objective={row.get('objective_loss_used')}, "
            f"quality_drop={row.get('quality_drop_too_much')}"
        )

    lines.extend(["", "Top Thai Basil replacements:"])
    for row in best_replacements_by_occurrence(replacement_rows):
        lines.append(
            f"- day{row.get('anchor_day_index')} {row.get('anchor_slot')}: "
            f"{row.get('replacement_display_name')} | delta={row.get('day_loss_delta_if_swapped')} "
            f"| gate={row.get('quality_gate_status_after_swap')} "
            f"| reason={row.get('why_replacement_loses')}"
        )
    lines.extend(["", recommendation_text.strip(), ""])
    return "\n".join(lines) + "\n"


def small_penalty_solves(row: Mapping[str, Any], thai_id: str) -> bool:
    if not bool_value(row.get("feasible")):
        return False
    if bool_value(row.get("quality_drop_too_much")):
        return False
    if thai_id and int(row.get("thai_basil_count", 0) or 0) > 1:
        return False
    return True


def replacement_pool_is_weak(rows: Sequence[Mapping[str, Any]]) -> bool:
    usable = [
        row
        for row in rows
        if str(row.get("quality_gate_status_after_swap")) == "accept"
        and to_float(row.get("day_loss_delta_if_swapped")) <= 0.06
    ]
    return len(usable) < 2


def best_replacements_by_occurrence(
    rows: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    result = []
    seen: set[tuple[Any, Any]] = set()
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            to_float(row.get("day_loss_delta_if_swapped")),
            to_float(row.get("loss_gap_vs_anchor_slot")),
            str(row.get("replacement_display_name")),
        ),
    )
    for row in sorted_rows:
        key = (row.get("anchor_day_index"), row.get("anchor_slot"))
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def best_replacement_name(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "none"
    best = sorted(
        rows,
        key=lambda row: (
            to_float(row.get("day_loss_delta_if_swapped")),
            to_float(row.get("loss_gap_vs_anchor_slot")),
            str(row.get("replacement_display_name")),
        ),
    )[0]
    return (
        f"{best.get('replacement_display_name')} "
        f"(day{best.get('anchor_day_index')} {best.get('anchor_slot')}, "
        f"delta={best.get('day_loss_delta_if_swapped')}, "
        f"gate={best.get('quality_gate_status_after_swap')})"
    )


def thai_macro_profile(occurrences: Sequence[Mapping[str, Mapping[str, Any]]]) -> str:
    parts = []
    for row in occurrences:
        meal = row["meal"]
        parts.append(
            f"day{row['day'].get('day_index')}:{meal_slot(meal)} "
            f"kcal={fmt(meal.get('kcal'))} "
            f"P/C/F={fmt(meal.get('protein_g'))}/{fmt(meal.get('carbs_g'))}/{fmt(meal.get('fat_g'))} "
            f"portion={fmt(meal.get('portion_multiplier'))}"
        )
    return " | ".join(parts) if parts else "missing"


def thai_win_reason(
    occurrences: Sequence[Mapping[str, Mapping[str, Any]]],
    replacement_rows: Sequence[Mapping[str, Any]],
) -> str:
    reasons = []
    if not occurrences:
        return "Thai Basil is not selected in current plan."
    if replacement_rows:
        best = sorted(
            replacement_rows,
            key=lambda row: (
                to_float(row.get("day_loss_delta_if_swapped")),
                to_float(row.get("loss_gap_vs_anchor_slot")),
            ),
        )[0]
        if to_float(best.get("day_loss_delta_if_swapped")) > 0.03:
            reasons.append("best_direct_swap_raises_day_loss")
        if "lower_protein" in str(best.get("why_replacement_loses")):
            reasons.append("alternatives_lower_protein")
        if "lower_carbs" in str(best.get("why_replacement_loses")):
            reasons.append("alternatives_lower_carbs")
        if "lower_kcal" in str(best.get("why_replacement_loses")):
            reasons.append("alternatives_lower_kcal")
    reasons.append("excellent_kcal_protein_carbs_fit_for_main_slot")
    reasons.append("shorter_effective_time_than_old_anchor_mains")
    return ";".join(dict.fromkeys(reasons))


def infeasible_reason(
    scenario: str,
    candidate_days: Sequence[Mapping[str, Any]],
    thai_id: str,
) -> str:
    valid = [candidate for candidate in candidate_days if str(candidate.get("validation_status")) == "valid"]
    accept = [candidate for candidate in valid if str(candidate.get("quality_gate_status")) == "accept"]
    accept_review = [
        candidate
        for candidate in valid
        if str(candidate.get("quality_gate_status")) in {"accept", "review"}
    ]
    if len(valid) < DAYS:
        return "fewer_than_3_valid_candidate_days"
    if scenario == "accept_only_no_repeat" and len(accept) < DAYS:
        return "fewer_than_3_accept_candidate_days"
    if scenario == "allow_one_review_no_repeat" and len(accept_review) < DAYS:
        return "fewer_than_3_accept_or_review_candidate_days"
    if scenario == "no_repeat_thai_basil" and thai_id:
        without_extra = sum(1 for candidate in accept_review if thai_id not in candidate_recipe_ids(candidate))
        return f"not_enough_usable_days_without_extra_thai_basil;usable_without_anchor={without_extra}"
    if scenario == "no_repeat_lunch_dinner_main":
        return "all_best_valid_combinations_repeat_at_least_one_lunch_dinner_main"
    return "no_combination_satisfies_constraint_with_valid_accept_or_review_days"


def strict_verdict(metrics: Mapping[str, Any]) -> str:
    return strict_verdict_from_counts(
        int(metrics.get("valid_day_count", 0) or 0),
        int(metrics.get("accept_day_count", 0) or 0),
        int(metrics.get("review_day_count", 0) or 0),
        int(metrics.get("reject_day_count", 0) or 0),
        int(metrics.get("repeated_recipe_count", 0) or 0),
        int(metrics.get("repeated_main_recipe_count", 0) or 0),
    )


def strict_verdict_from_counts(
    valid_day_count: int,
    accept_day_count: int,
    review_day_count: int,
    reject_day_count: int,
    repeated_recipe_count: int,
    repeated_main_recipe_count: int,
) -> str:
    if valid_day_count < DAYS or reject_day_count > 0:
        return "bad"
    if accept_day_count == DAYS and repeated_recipe_count == 0:
        return "good_no_repeat"
    if accept_day_count == DAYS and repeated_main_recipe_count == 0:
        return "good_main_no_repeat"
    if review_day_count <= 1 and repeated_recipe_count == 0:
        return "review_no_repeat"
    if repeated_main_recipe_count > 0:
        return "review_repeated_main"
    if repeated_recipe_count > 0:
        return "review_repetition"
    return "review"


def combo_meals(combo: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    meals = []
    for day_index, candidate in enumerate(combo, start=1):
        plan = candidate.get("plan", {})
        if not isinstance(plan, Mapping):
            continue
        for meal in plan.get("selected_meals", []):
            if isinstance(meal, Mapping):
                enriched = dict(meal)
                enriched["combo_day"] = day_index
                meals.append(enriched)
    return meals


def display_names_by_day(combo: Sequence[Mapping[str, Any]]) -> str:
    day_parts = []
    for index, candidate in enumerate(combo, start=1):
        plan = candidate.get("plan", {})
        meals = plan.get("selected_meals", []) if isinstance(plan, Mapping) else []
        slot_parts = []
        for slot in SLOTS:
            meal = next((item for item in meals if isinstance(item, Mapping) and meal_slot(item) == slot), {})
            slot_parts.append(f"{slot}:{meal_name(meal)}")
        day_parts.append(f"D{index}[" + " | ".join(slot_parts) + "]")
    return " || ".join(day_parts)


def plan_display_names_by_day(plan: Mapping[str, Any]) -> str:
    day_parts = []
    for day in plan.get("days", []):
        if not isinstance(day, Mapping):
            continue
        meals = day.get("selected_meals", [])
        slot_parts = []
        for slot in SLOTS:
            meal = next((item for item in meals if isinstance(item, Mapping) and meal_slot(item) == slot), {})
            slot_parts.append(f"{slot}:{meal_name(meal)}")
        day_parts.append(f"D{day.get('day_index')}[" + " | ".join(slot_parts) + "]")
    return " || ".join(day_parts)


def day_recipe_ids(day: Mapping[str, Any]) -> list[str]:
    return [
        meal_recipe_id(meal)
        for meal in day.get("selected_meals", [])
        if isinstance(meal, Mapping) and meal_recipe_id(meal)
    ]


def candidate_recipe_ids(candidate: Mapping[str, Any]) -> set[str]:
    plan = candidate.get("plan", {})
    if not isinstance(plan, Mapping):
        return set()
    return {
        meal_recipe_id(meal)
        for meal in plan.get("selected_meals", [])
        if isinstance(meal, Mapping) and meal_recipe_id(meal)
    }


def day_totals(meals: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    return {
        "total_kcal": round(sum(to_float(meal.get("kcal")) for meal in meals), 1),
        "total_protein_g": round(sum(to_float(meal.get("protein_g")) for meal in meals), 1),
        "total_carbs_g": round(sum(to_float(meal.get("carbs_g")) for meal in meals), 1),
        "total_fat_g": round(sum(to_float(meal.get("fat_g")) for meal in meals), 1),
        "effective_time_min_sum": round(sum(to_float(meal.get("effective_time_min_for_scoring")) for meal in meals), 1),
    }


def slot_candidate_loss(row: Mapping[str, Any], target: NutritionTarget, slot: str) -> float:
    slot_target = target.slot_targets.get(slot, {})
    kcal_target = target_value(slot_target, "kcal")
    protein_target = target_value(slot_target, "protein_g")
    carbs_target = target_value(slot_target, "carbs_g")
    fat_target = target_value(slot_target, "fat_g")
    kcal = to_float(row.get("kcal"))
    protein = to_float(row.get("protein_g"))
    carbs = to_float(row.get("carbs_g"))
    fat = to_float(row.get("fat_g"))
    score_loss = max(0.0, 1.0 - to_float(row.get("score_preview")))
    realism_loss = to_float(row.get("meal_realism_practical_penalty", row.get("meal_realism_penalty"))) * 0.15
    return (
        0.35 * abs(kcal - kcal_target) / max(kcal_target, 1.0)
        + 0.25 * abs(protein - protein_target) / max(protein_target, 1.0)
        + 0.25 * abs(carbs - carbs_target) / max(carbs_target, 1.0)
        + 0.10 * abs(fat - fat_target) / max(fat_target, 1.0)
        + 0.05 * score_loss
        + realism_loss
    )


def target_value(slot_target: Any, field: str) -> float:
    if isinstance(slot_target, Mapping):
        return to_float(slot_target.get(field))
    return to_float(getattr(slot_target, field, 0.0))


def why_replacement_loses(
    anchor: Mapping[str, Any],
    replacement: Mapping[str, Any],
    slot_loss_gap: float,
    swap_loss_delta: float,
    quality_status: str,
    flags: Sequence[str],
) -> str:
    reasons = []
    if slot_loss_gap > 0.08:
        reasons.append("higher_slot_loss")
    if swap_loss_delta > 0.04:
        reasons.append("worse_day_loss")
    if to_float(replacement.get("kcal")) < to_float(anchor.get("kcal")) - 150:
        reasons.append("lower_kcal")
    if to_float(replacement.get("protein_g")) < to_float(anchor.get("protein_g")) - 12:
        reasons.append("lower_protein")
    if to_float(replacement.get("carbs_g")) < to_float(anchor.get("carbs_g")) - 25:
        reasons.append("lower_carbs")
    if to_float(replacement.get("fat_g")) > to_float(anchor.get("fat_g")) + 12:
        reasons.append("fat_worse")
    if to_float(replacement.get("effective_time_min_for_scoring")) > to_float(anchor.get("effective_time_min_for_scoring")) + 20:
        reasons.append("time_worse")
    if quality_status == "review":
        reasons.append("quality_review_after_swap")
    if quality_status == "reject":
        reasons.append("quality_reject_after_swap")
    if flags:
        reasons.append("realism_flags")
    return ";".join(reasons) or "slightly_worse_but_usable"


def meal_from_row(row: pd.Series) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in row.index
        if not str(key).startswith("_")
    }


def scenario_row(rows: Sequence[Mapping[str, Any]], scenario: str) -> Mapping[str, Any]:
    for row in rows:
        if str(row.get("scenario")) == scenario:
            return row
    return {}


def variant_row(rows: Sequence[Mapping[str, Any]], variant: str) -> Mapping[str, Any]:
    for row in rows:
        if str(row.get("variant")) == variant:
            return row
    return {}


def scenario_value(rows: Sequence[Mapping[str, Any]], scenario: str, key: str) -> Any:
    return scenario_row(rows, scenario).get(key)


def meal_recipe_id(meal: Mapping[str, Any]) -> str:
    return str(meal.get("recipe_id", "")).strip()


def meal_name(meal: Mapping[str, Any]) -> str:
    return str(meal.get("display_name") or meal.get("recipe_name") or meal.get("recipe_id", "")).strip()


def meal_slot(meal: Mapping[str, Any]) -> str:
    return str(meal.get("slot", "")).strip()


def meal_flags(meal: Mapping[str, Any]) -> list[str]:
    flags: list[str] = []
    for key in ("meal_realism_flags", "meal_realism_practical_flags", "realism_flags"):
        value = meal.get(key)
        if isinstance(value, list):
            flags.extend(str(item) for item in value if str(item))
        elif isinstance(value, str):
            flags.extend(part.strip() for part in value.replace("|", ";").split(";") if part.strip())
    return sorted(set(flags))


def meal_realism_warning_count(selected_meals: Sequence[Mapping[str, Any]]) -> int:
    return sum(len(meal_flags(meal)) for meal in selected_meals if isinstance(meal, Mapping))


def quality_rank(status: str) -> int:
    return {"accept": 0, "review": 1, "reject": 2}.get(status, 3)


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"true", "1", "yes"}


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def fmt(value: Any, decimals: int = 1) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return "missing"
    return f"{float(numeric):.{decimals}f}"


def format_list(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        if not value:
            return "none"
        return ";".join(str(item) for item in value)
    if value is None:
        return "none"
    text = str(value).strip()
    return text or "none"


if __name__ == "__main__":
    main()
