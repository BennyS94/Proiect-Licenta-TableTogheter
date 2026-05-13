from __future__ import annotations

import itertools
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
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
    V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH,
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
OUT_SUMMARY = OUT_DIR / "generator_v1_round29_plus30_no_repeat_summary.txt"
OUT_CANDIDATE_DAYS = OUT_DIR / "generator_v1_round29_plus30_no_repeat_candidate_days.csv"
OUT_COMBINATIONS = OUT_DIR / "generator_v1_round29_plus30_no_repeat_combinations.csv"
OUT_ANCHORS = OUT_DIR / "generator_v1_round29_plus30_anchor_recipe_analysis.csv"
OUT_RECOMMENDATION = OUT_DIR / "generator_v1_round29_plus30_next_action_recommendation.txt"

SLOTS = ["breakfast", "lunch", "dinner", "snack"]
MAIN_SLOTS = {"lunch", "dinner"}
ANCHOR_NAMES = {
    "waffles": "Mom's Best Waffles",
    "chicken_broccoli_pasta": "Chicken and Broccoli Pasta",
}
SEVERE_REALISM_FLAGS = {
    "breakfast_too_large",
    "unrealistic_large_portion",
    "snack_too_large",
    "ingredient_component_as_meal",
    "dessert_as_meal",
    "drink_as_meal",
}
GLOBAL_CONFIG = {
    "selection_mode": "balanced_day",
    "portion_policy": "target_aware",
    "meal_realism_mode": "practical",
    "quality_gate": "demo_safe",
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
    "meal_realism_mode": "practical",
    "max_candidates_per_slot": 26,
    "min_recipe_difference_between_alternatives": 1,
}


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    context = build_context()
    current_plan = generate_current_plan(context)
    anchor_ids = find_anchor_ids(context["slot_candidates"])
    candidate_days = build_augmented_candidate_day_pool(context, anchor_ids)
    candidate_rows = build_candidate_day_rows(candidate_days, anchor_ids)
    combination_rows = build_combination_rows(candidate_days, current_plan, anchor_ids)
    anchor_rows = build_anchor_analysis_rows(
        context=context,
        current_plan=current_plan,
        anchor_ids=anchor_ids,
    )
    recommendation_text = build_recommendation_text(
        combination_rows=combination_rows,
        anchor_rows=anchor_rows,
        candidate_rows=candidate_rows,
    )
    summary_text = build_summary_text(
        current_plan=current_plan,
        candidate_rows=candidate_rows,
        combination_rows=combination_rows,
        anchor_rows=anchor_rows,
        recommendation_text=recommendation_text,
    )

    pd.DataFrame(candidate_rows).to_csv(OUT_CANDIDATE_DAYS, index=False)
    pd.DataFrame(combination_rows).to_csv(OUT_COMBINATIONS, index=False)
    pd.DataFrame(anchor_rows).to_csv(OUT_ANCHORS, index=False)
    OUT_RECOMMENDATION.write_text(recommendation_text, encoding="utf-8")
    OUT_SUMMARY.write_text(summary_text, encoding="utf-8")

    feasible_exact = scenario_value(combination_rows, "no_repeat_exact_recipe", "feasible")
    feasible_accept = scenario_value(combination_rows, "accept_only_no_repeat", "feasible")
    print("Generator v1 Round29 plus30 no-repeat feasibility audit written")
    print(f"summary={OUT_SUMMARY}")
    print(f"candidate_days={OUT_CANDIDATE_DAYS}")
    print(f"combinations={OUT_COMBINATIONS}")
    print(f"anchors={OUT_ANCHORS}")
    print(f"recommendation={OUT_RECOMMENDATION}")
    print(f"candidate_day_count={len(candidate_rows)}")
    print(f"no_repeat_exact_recipe_feasible={feasible_exact}")
    print(f"accept_only_no_repeat_feasible={feasible_accept}")


def build_context() -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH,
        ingredients_path=V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH,
        nutrition_path=V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH,
        dataset_profile=V1_2_GENERATOR_READY_PLUS30_PROFILE,
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
        portion_policy_mode="target_aware",
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
        days=3,
        config=GLOBAL_CONFIG,
    )


def find_anchor_ids(slot_candidates: pd.DataFrame) -> dict[str, str]:
    anchor_ids: dict[str, str] = {}
    for key, display_name in ANCHOR_NAMES.items():
        matches = slot_candidates[slot_candidates["display_name"].astype(str).eq(display_name)]
        if matches.empty:
            anchor_ids[key] = ""
        else:
            anchor_ids[key] = str(matches.iloc[0].get("recipe_id", "")).strip()
    return anchor_ids


def build_augmented_candidate_day_pool(
    context: Mapping[str, Any],
    anchor_ids: Mapping[str, str],
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

    extra_specs = build_extra_source_specs(anchor_ids)
    for source_mode, filtered_ids, diversity_mode, recent_ids in extra_specs:
        filtered_by_slot = filter_candidates_by_recipe_ids(candidates_by_slot, filtered_ids)
        source_plan = select_and_evaluate_source_plan(
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


def build_extra_source_specs(anchor_ids: Mapping[str, str]) -> list[tuple[str, set[str], str, list[str]]]:
    waffles_id = anchor_ids.get("waffles", "")
    chicken_id = anchor_ids.get("chicken_broccoli_pasta", "")
    both_ids = {item for item in [waffles_id, chicken_id] if item}
    specs: list[tuple[str, set[str], str, list[str]]] = []
    if waffles_id:
        specs.append(("avoid_waffles_recent", set(), "avoid_recent", [waffles_id]))
        specs.append(("soft_waffles_recent", set(), "soft", [waffles_id]))
        specs.append(("filter_no_waffles", {waffles_id}, "none", []))
    if chicken_id:
        specs.append(("avoid_chicken_broccoli_recent", set(), "avoid_recent", [chicken_id]))
        specs.append(("soft_chicken_broccoli_recent", set(), "soft", [chicken_id]))
        specs.append(("filter_no_chicken_broccoli", {chicken_id}, "none", []))
    if both_ids:
        specs.append(("avoid_both_anchors_recent", set(), "avoid_recent", sorted(both_ids)))
        specs.append(("soft_both_anchors_recent", set(), "soft", sorted(both_ids)))
        specs.append(("filter_no_anchors", both_ids, "none", []))
        specs.append(("filter_no_anchors_soft", both_ids, "soft", []))
    return specs


def select_and_evaluate_source_plan(
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
        config={"quality_gate": "demo_safe"},
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


def build_candidate_day_rows(
    candidate_days: Sequence[Mapping[str, Any]],
    anchor_ids: Mapping[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidate_days:
        plan = candidate.get("plan", {})
        if not isinstance(plan, Mapping):
            plan = {}
        meals = list(plan.get("selected_meals", []))
        ids = [meal_recipe_id(meal) for meal in meals if isinstance(meal, Mapping)]
        names = [meal_name(meal) for meal in meals if isinstance(meal, Mapping)]
        flags = sorted({flag for meal in meals for flag in meal_flags(meal)})
        totals = plan.get("day_totals", {}) if isinstance(plan.get("day_totals"), Mapping) else {}
        row = {
            "day_candidate_id": candidate.get("candidate_day_id"),
            "source_mode": candidate.get("source_mode"),
            "alternative_rank": candidate.get("alternative_rank"),
            "selected_recipe_ids": ";".join(ids),
            "selected_recipe_names": ";".join(names),
            "slots": ";".join(meal_slot(meal) for meal in meals if isinstance(meal, Mapping)),
            "validation_status": candidate.get("validation_status"),
            "quality_gate_status": candidate.get("quality_gate_status"),
            "candidate_class": candidate.get("quality_gate_status"),
            "base_day_loss": round(to_float(candidate.get("base_day_loss")), 6),
            "adjusted_day_loss": round(to_float(candidate.get("adjusted_day_loss")), 6),
            "total_kcal": round(to_float(totals.get("total_kcal")), 1),
            "total_protein_g": round(to_float(totals.get("total_protein_g")), 1),
            "total_carbs_g": round(to_float(totals.get("total_carbs_g")), 1),
            "total_fat_g": round(to_float(totals.get("total_fat_g")), 1),
            "effective_time_min_sum": round(to_float(totals.get("effective_time_min_sum")), 1),
            "realism_flags": ";".join(flags),
            "contains_waffles": anchor_ids.get("waffles", "") in ids,
            "contains_chicken_broccoli_pasta": anchor_ids.get("chicken_broccoli_pasta", "") in ids,
            "contains_dominant_anchor_count": sum(
                1 for anchor_id in anchor_ids.values() if anchor_id and anchor_id in ids
            ),
            "has_internal_repeated_recipe_ids": len(ids) != len(set(ids)),
        }
        for slot in SLOTS:
            meal = next((item for item in meals if meal_slot(item) == slot), {})
            row[f"{slot}_recipe_id"] = meal_recipe_id(meal)
            row[f"{slot}_display_name"] = meal_name(meal)
        rows.append(row)
    return rows


def build_combination_rows(
    candidate_days: Sequence[Mapping[str, Any]],
    current_plan: Mapping[str, Any],
    anchor_ids: Mapping[str, str],
) -> list[dict[str, Any]]:
    scenario_specs = [
        ("current_best", "Actual global_alternatives_3_day result.", None),
        ("no_repeat_exact_recipe", "No recipe_id may repeat across all 3 days.", no_repeat_exact),
        ("no_repeat_same_slot", "No recipe_id may repeat in the same slot.", no_repeat_same_slot),
        ("no_waffles_repeat", "Mom's Best Waffles may appear at most once.", lambda metrics: anchor_count_ok(metrics, anchor_ids.get("waffles"))),
        ("no_chicken_broccoli_repeat", "Chicken and Broccoli Pasta may appear at most once.", lambda metrics: anchor_count_ok(metrics, anchor_ids.get("chicken_broccoli_pasta"))),
        ("no_anchor_repeat", "Both anchor recipes may appear at most once.", lambda metrics: all(anchor_count_ok(metrics, anchor_id) for anchor_id in anchor_ids.values() if anchor_id)),
        ("accept_only_no_repeat", "Only accept days and no exact recipe repeats.", accept_only_no_repeat),
        ("allow_one_review_no_repeat", "At most one review day and no exact recipe repeats.", allow_one_review_no_repeat),
    ]

    rows = []
    for scenario, description, constraint in scenario_specs:
        if scenario == "current_best":
            rows.append(current_plan_row(current_plan, scenario, description, anchor_ids))
            continue
        combo, metrics = find_best_combination(candidate_days, constraint)
        if combo is None or metrics is None:
            rows.append(
                {
                    "scenario": scenario,
                    "description": description,
                    "feasible": False,
                    "infeasible_reason": infeasible_reason(scenario, candidate_days, anchor_ids),
                }
            )
            continue
        rows.append(combination_row(scenario, description, combo, metrics, anchor_ids))
    return rows


def find_best_combination(
    candidate_days: Sequence[Mapping[str, Any]],
    constraint: Any,
) -> tuple[tuple[Mapping[str, Any], ...] | None, dict[str, Any] | None]:
    usable = [
        candidate
        for candidate in candidate_days
        if str(candidate.get("validation_status")) == "valid"
    ]
    best_combo: tuple[Mapping[str, Any], ...] | None = None
    best_metrics: dict[str, Any] | None = None
    best_key: tuple[Any, ...] | None = None
    for combo in itertools.combinations(usable, 3):
        metrics = combination_metrics(combo)
        if not constraint(metrics):
            continue
        key = (
            metrics["reject_day_count"],
            metrics["review_day_count"],
            metrics["multi_day_loss"],
            metrics["repeated_recipe_count"],
            metrics["average_base_day_loss"],
            ";".join(str(item) for item in metrics["selected_candidate_day_ids"]),
        )
        if best_key is None or key < best_key:
            best_combo = combo
            best_metrics = metrics
            best_key = key
    return best_combo, best_metrics


def combination_metrics(combo: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    score = _score_day_combination(combo)
    meals = combo_meals(combo)
    recipe_counts = Counter(meal_recipe_id(meal) for meal in meals if meal_recipe_id(meal))
    slot_recipe_counts = Counter(
        (meal_slot(meal), meal_recipe_id(meal))
        for meal in meals
        if meal_slot(meal) and meal_recipe_id(meal)
    )
    quality_counts = Counter(str(candidate.get("quality_gate_status", "missing")) for candidate in combo)
    validation_counts = Counter(str(candidate.get("validation_status", "missing")) for candidate in combo)
    repeated_same_slot_ids = sorted(
        recipe_id
        for (_, recipe_id), count in slot_recipe_counts.items()
        if count > 1
    )
    repeated_main_ids = sorted(
        recipe_id
        for (slot, recipe_id), count in slot_recipe_counts.items()
        if count > 1 and slot in MAIN_SLOTS
    )
    repeated_breakfast_ids = sorted(
        recipe_id
        for (slot, recipe_id), count in slot_recipe_counts.items()
        if count > 1 and slot == "breakfast"
    )
    average_adjusted = sum(to_float(candidate.get("adjusted_day_loss")) for candidate in combo) / max(1, len(combo))
    return {
        **score,
        "average_adjusted_day_loss": round(average_adjusted, 6),
        "valid_day_count": validation_counts.get("valid", 0),
        "accept_day_count": quality_counts.get("accept", 0),
        "review_day_count": quality_counts.get("review", 0),
        "reject_day_count": quality_counts.get("reject", 0),
        "repeated_same_slot_ids": repeated_same_slot_ids,
        "repeated_main_ids": repeated_main_ids,
        "repeated_breakfast_ids": repeated_breakfast_ids,
        "selected_candidate_day_ids": [candidate.get("candidate_day_id") for candidate in combo],
        "recipe_counts": dict(recipe_counts),
        "display_names_by_day": display_names_by_day(combo),
    }


def current_plan_row(
    current_plan: Mapping[str, Any],
    scenario: str,
    description: str,
    anchor_ids: Mapping[str, str],
) -> dict[str, Any]:
    meals = []
    day_ids = []
    for day in current_plan.get("days", []):
        day_ids.append(day.get("candidate_day_id"))
        for meal in day.get("selected_meals", []):
            if isinstance(meal, Mapping):
                enriched = dict(meal)
                enriched["day_index"] = day.get("day_index")
                meals.append(enriched)
    recipe_counts = Counter(meal_recipe_id(meal) for meal in meals if meal_recipe_id(meal))
    slot_counts = Counter((meal_slot(meal), meal_recipe_id(meal)) for meal in meals if meal_slot(meal))
    summary = current_plan.get("multi_day_summary", {})
    quality_counts = Counter(str(day.get("quality_gate_status", "missing")) for day in current_plan.get("days", []))
    validation_counts = Counter(str(day.get("validation_status", "missing")) for day in current_plan.get("days", []))
    return {
        "scenario": scenario,
        "description": description,
        "feasible": True,
        "valid_day_count": validation_counts.get("valid", 0),
        "accept_day_count": quality_counts.get("accept", 0),
        "review_day_count": quality_counts.get("review", 0),
        "reject_day_count": quality_counts.get("reject", 0),
        "repeated_recipe_count": len([rid for rid, count in recipe_counts.items() if count > 1]),
        "repeated_recipe_ids": ";".join(sorted(rid for rid, count in recipe_counts.items() if count > 1)),
        "repeated_same_slot_ids": ";".join(sorted(rid for (_, rid), count in slot_counts.items() if count > 1)),
        "repeated_main_recipe_ids": ";".join(sorted(rid for (slot, rid), count in slot_counts.items() if count > 1 and slot in MAIN_SLOTS)),
        "repeated_breakfast_recipe_ids": ";".join(sorted(rid for (slot, rid), count in slot_counts.items() if count > 1 and slot == "breakfast")),
        "unique_recipe_count": len(recipe_counts),
        "multi_day_loss": current_plan.get("multi_day_loss"),
        "average_base_day_loss": current_plan.get("average_day_loss") or summary.get("average_day_loss"),
        "average_adjusted_day_loss": current_plan.get("average_day_loss") or summary.get("average_day_loss"),
        "selected_candidate_day_ids": ";".join(str(item) for item in day_ids if item),
        "selected_days_meals": plan_display_names_by_day(current_plan),
        "waffles_count": recipe_counts.get(anchor_ids.get("waffles", ""), 0),
        "chicken_broccoli_pasta_count": recipe_counts.get(anchor_ids.get("chicken_broccoli_pasta", ""), 0),
        "strict_result": strict_result_from_counts(
            valid_day_count=validation_counts.get("valid", 0),
            accept_day_count=quality_counts.get("accept", 0),
            review_day_count=quality_counts.get("review", 0),
            reject_day_count=quality_counts.get("reject", 0),
            repeated_recipe_count=len([rid for rid, count in recipe_counts.items() if count > 1]),
        ),
    }


def combination_row(
    scenario: str,
    description: str,
    combo: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, Any],
    anchor_ids: Mapping[str, str],
) -> dict[str, Any]:
    recipe_counts = metrics.get("recipe_counts", {})
    return {
        "scenario": scenario,
        "description": description,
        "feasible": True,
        "valid_day_count": metrics.get("valid_day_count"),
        "accept_day_count": metrics.get("accept_day_count"),
        "review_day_count": metrics.get("review_day_count"),
        "reject_day_count": metrics.get("reject_day_count"),
        "repeated_recipe_count": metrics.get("repeated_recipe_count"),
        "repeated_recipe_ids": ";".join(metrics.get("repeated_recipe_ids", [])),
        "repeated_same_slot_ids": ";".join(metrics.get("repeated_same_slot_ids", [])),
        "repeated_main_recipe_ids": ";".join(metrics.get("repeated_main_ids", [])),
        "repeated_breakfast_recipe_ids": ";".join(metrics.get("repeated_breakfast_ids", [])),
        "unique_recipe_count": metrics.get("unique_recipe_count"),
        "multi_day_loss": metrics.get("multi_day_loss"),
        "average_base_day_loss": metrics.get("average_base_day_loss"),
        "average_adjusted_day_loss": metrics.get("average_adjusted_day_loss"),
        "repetition_penalty": metrics.get("repetition_penalty"),
        "review_day_penalty": metrics.get("review_day_penalty"),
        "selected_candidate_day_ids": ";".join(str(item) for item in metrics.get("selected_candidate_day_ids", [])),
        "selected_days_meals": metrics.get("display_names_by_day"),
        "waffles_count": int(recipe_counts.get(anchor_ids.get("waffles", ""), 0) or 0),
        "chicken_broccoli_pasta_count": int(recipe_counts.get(anchor_ids.get("chicken_broccoli_pasta", ""), 0) or 0),
        "strict_result": strict_result_from_counts(
            valid_day_count=int(metrics.get("valid_day_count", 0) or 0),
            accept_day_count=int(metrics.get("accept_day_count", 0) or 0),
            review_day_count=int(metrics.get("review_day_count", 0) or 0),
            reject_day_count=int(metrics.get("reject_day_count", 0) or 0),
            repeated_recipe_count=int(metrics.get("repeated_recipe_count", 0) or 0),
        ),
    }


def no_repeat_exact(metrics: Mapping[str, Any]) -> bool:
    return (
        metrics.get("valid_day_count") == 3
        and metrics.get("reject_day_count") == 0
        and int(metrics.get("repeated_recipe_count", 0) or 0) == 0
    )


def no_repeat_same_slot(metrics: Mapping[str, Any]) -> bool:
    return (
        metrics.get("valid_day_count") == 3
        and metrics.get("reject_day_count") == 0
        and not metrics.get("repeated_same_slot_ids")
    )


def anchor_count_ok(metrics: Mapping[str, Any], anchor_id: str | None) -> bool:
    if not anchor_id:
        return True
    recipe_counts = metrics.get("recipe_counts", {})
    return (
        metrics.get("valid_day_count") == 3
        and metrics.get("reject_day_count") == 0
        and int(recipe_counts.get(anchor_id, 0) or 0) <= 1
    )


def accept_only_no_repeat(metrics: Mapping[str, Any]) -> bool:
    return (
        metrics.get("valid_day_count") == 3
        and metrics.get("accept_day_count") == 3
        and int(metrics.get("repeated_recipe_count", 0) or 0) == 0
    )


def allow_one_review_no_repeat(metrics: Mapping[str, Any]) -> bool:
    return (
        metrics.get("valid_day_count") == 3
        and metrics.get("reject_day_count") == 0
        and int(metrics.get("review_day_count", 0) or 0) <= 1
        and int(metrics.get("repeated_recipe_count", 0) or 0) == 0
    )


def build_anchor_analysis_rows(
    context: Mapping[str, Any],
    current_plan: Mapping[str, Any],
    anchor_ids: Mapping[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    slot_candidates = context["slot_candidates"].copy()
    current_days = list(current_plan.get("days", []))
    for anchor_key, anchor_id in anchor_ids.items():
        if not anchor_id:
            continue
        anchor_name = ANCHOR_NAMES[anchor_key]
        anchor_slots = sorted(
            {
                str(row.get("slot", "")).strip()
                for _, row in slot_candidates[slot_candidates["recipe_id"].astype(str).eq(anchor_id)].iterrows()
            }
        )
        if anchor_key == "waffles":
            slots_to_check = ["breakfast"]
        else:
            slots_to_check = [slot for slot in ["lunch", "dinner"] if slot in anchor_slots or slot in MAIN_SLOTS]
        reference_by_slot = reference_day_for_anchor_slot(current_days, anchor_id, slots_to_check)
        for slot in slots_to_check:
            rows.extend(
                replacement_rows_for_anchor_slot(
                    slot_candidates=slot_candidates,
                    target=context["target"],
                    anchor_id=anchor_id,
                    anchor_name=anchor_name,
                    slot=slot,
                    reference_day=reference_by_slot.get(slot),
                    limit=10,
                )
            )
    return rows


def reference_day_for_anchor_slot(
    current_days: Sequence[Mapping[str, Any]],
    anchor_id: str,
    slots: Sequence[str],
) -> dict[str, Mapping[str, Any]]:
    references: dict[str, Mapping[str, Any]] = {}
    for slot in slots:
        for day in current_days:
            meals = day.get("selected_meals", [])
            if any(meal_slot(meal) == slot and meal_recipe_id(meal) == anchor_id for meal in meals):
                references[slot] = day
                break
        if slot not in references and current_days:
            references[slot] = current_days[0]
    return references


def replacement_rows_for_anchor_slot(
    slot_candidates: pd.DataFrame,
    target: NutritionTarget,
    anchor_id: str,
    anchor_name: str,
    slot: str,
    reference_day: Mapping[str, Any] | None,
    limit: int,
) -> list[dict[str, Any]]:
    slot_rows = slot_candidates[slot_candidates["slot"].astype(str).eq(slot)].copy()
    if slot_rows.empty:
        return []
    slot_rows["slot_candidate_loss"] = slot_rows.apply(
        lambda row: slot_candidate_loss(row, target, slot),
        axis=1,
    )
    anchor_rows = slot_rows[slot_rows["recipe_id"].astype(str).eq(anchor_id)].sort_values("slot_candidate_loss")
    if anchor_rows.empty:
        anchor_loss = 0.0
        anchor_nutrition = {}
    else:
        anchor_loss = to_float(anchor_rows.iloc[0].get("slot_candidate_loss"))
        anchor_nutrition = nutrition_from_row(anchor_rows.iloc[0])
    alternatives = (
        slot_rows[~slot_rows["recipe_id"].astype(str).eq(anchor_id)]
        .sort_values(["slot_candidate_loss", "score_preview"], ascending=[True, False])
        .drop_duplicates(subset=["recipe_id"])
        .head(limit)
    )
    rows = []
    for rank, (_, alt) in enumerate(alternatives.iterrows(), start=1):
        replacement_day = evaluate_swap(reference_day, alt, slot, target)
        alt_nutrition = nutrition_from_row(alt)
        loss_delta = (
            replacement_day.get("base_day_loss_after_swap", 0.0)
            - replacement_day.get("base_day_loss_before_swap", 0.0)
            if replacement_day
            else ""
        )
        rows.append(
            {
                "anchor_recipe_id": anchor_id,
                "anchor_display_name": anchor_name,
                "slot": slot,
                "anchor_slot_loss": round(anchor_loss, 6),
                "anchor_kcal": round(to_float(anchor_nutrition.get("kcal")), 1),
                "anchor_protein_g": round(to_float(anchor_nutrition.get("protein_g")), 1),
                "anchor_carbs_g": round(to_float(anchor_nutrition.get("carbs_g")), 1),
                "anchor_fat_g": round(to_float(anchor_nutrition.get("fat_g")), 1),
                "replacement_rank": rank,
                "replacement_recipe_id": alt.get("recipe_id"),
                "replacement_display_name": alt.get("display_name"),
                "replacement_slot_loss": round(to_float(alt.get("slot_candidate_loss")), 6),
                "loss_gap_vs_anchor_slot": round(to_float(alt.get("slot_candidate_loss")) - anchor_loss, 6),
                "replacement_kcal": round(to_float(alt_nutrition.get("kcal")), 1),
                "replacement_protein_g": round(to_float(alt_nutrition.get("protein_g")), 1),
                "replacement_carbs_g": round(to_float(alt_nutrition.get("carbs_g")), 1),
                "replacement_fat_g": round(to_float(alt_nutrition.get("fat_g")), 1),
                "portion_grams": round(to_float(alt.get("portion_grams_estimated")), 1),
                "score_preview": round(to_float(alt.get("score_preview")), 4),
                "quality_gate_status_after_swap": replacement_day.get("quality_gate_status_after_swap", "") if replacement_day else "",
                "validation_status_after_swap": replacement_day.get("validation_status_after_swap", "") if replacement_day else "",
                "base_day_loss_before_swap": replacement_day.get("base_day_loss_before_swap", "") if replacement_day else "",
                "base_day_loss_after_swap": replacement_day.get("base_day_loss_after_swap", "") if replacement_day else "",
                "day_loss_delta_if_swapped": round(to_float(loss_delta), 6) if loss_delta != "" else "",
                "realism_flags": ";".join(meal_flags(alt)),
                "why_replacement_loses": why_replacement_loses(
                    anchor_nutrition=anchor_nutrition,
                    replacement_nutrition=alt_nutrition,
                    slot_loss_gap=to_float(alt.get("slot_candidate_loss")) - anchor_loss,
                    swap_loss_delta=to_float(loss_delta) if loss_delta != "" else 0.0,
                    quality_status=str(replacement_day.get("quality_gate_status_after_swap", "")) if replacement_day else "",
                    flags=meal_flags(alt),
                    slot=slot,
                ),
            }
        )
    return rows


def evaluate_swap(
    reference_day: Mapping[str, Any] | None,
    replacement_row: pd.Series,
    slot: str,
    target: NutritionTarget,
) -> dict[str, Any]:
    if not reference_day:
        return {}
    original_meals = [
        dict(meal)
        for meal in reference_day.get("selected_meals", [])
        if isinstance(meal, Mapping)
    ]
    if not original_meals:
        return {}
    replacement_meals = []
    for meal in original_meals:
        if meal_slot(meal) == slot:
            replacement_meals.append(meal_from_slot_row(replacement_row))
        else:
            replacement_meals.append(dict(meal))
    before_loss = compute_day_loss_for_plan(
        original_meals,
        target,
        config={"meal_realism_mode": "practical"},
    )
    after_loss = compute_day_loss_for_plan(
        replacement_meals,
        target,
        config={"meal_realism_mode": "practical"},
    )
    plan = {
        "selected_meals": replacement_meals,
        "day_totals": day_totals(replacement_meals),
        "warnings": [],
        "selector_mode": "balanced_day",
        "selector_diagnostics": {
            "base_day_loss": after_loss["day_loss"],
            "adjusted_day_loss": after_loss["day_loss"],
            **after_loss,
        },
    }
    validation = validate_one_day_plan(plan, target)
    plan["validation"] = validation
    quality = evaluate_plan_quality(plan, target, config={"quality_gate": "demo_safe"})
    return {
        "base_day_loss_before_swap": round(to_float(before_loss.get("day_loss")), 6),
        "base_day_loss_after_swap": round(to_float(after_loss.get("day_loss")), 6),
        "quality_gate_status_after_swap": quality.get("quality_gate_status"),
        "validation_status_after_swap": validation.get("validation_status"),
    }


def meal_from_slot_row(row: pd.Series) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in row.index
        if not str(key).startswith("_balanced_")
    }


def day_totals(meals: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    return {
        "total_kcal": round(sum(to_float(meal.get("kcal")) for meal in meals), 1),
        "total_protein_g": round(sum(to_float(meal.get("protein_g")) for meal in meals), 1),
        "total_carbs_g": round(sum(to_float(meal.get("carbs_g")) for meal in meals), 1),
        "total_fat_g": round(sum(to_float(meal.get("fat_g")) for meal in meals), 1),
        "effective_time_min_sum": round(sum(to_float(meal.get("effective_time_min_for_scoring")) for meal in meals), 1),
    }


def slot_candidate_loss(row: pd.Series, target: NutritionTarget, slot: str) -> float:
    slot_target = target.slot_targets.get(slot, {})
    kcal_target = to_float(slot_target.get("kcal")) if isinstance(slot_target, Mapping) else to_float(getattr(slot_target, "kcal", 0.0))
    protein_target = to_float(slot_target.get("protein_g")) if isinstance(slot_target, Mapping) else to_float(getattr(slot_target, "protein_g", 0.0))
    carbs_target = to_float(slot_target.get("carbs_g")) if isinstance(slot_target, Mapping) else to_float(getattr(slot_target, "carbs_g", 0.0))
    fat_target = to_float(slot_target.get("fat_g")) if isinstance(slot_target, Mapping) else to_float(getattr(slot_target, "fat_g", 0.0))
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


def why_replacement_loses(
    anchor_nutrition: Mapping[str, Any],
    replacement_nutrition: Mapping[str, Any],
    slot_loss_gap: float,
    swap_loss_delta: float,
    quality_status: str,
    flags: Sequence[str],
    slot: str,
) -> str:
    reasons = []
    if slot_loss_gap > 0.08:
        reasons.append("higher_slot_loss")
    if swap_loss_delta > 0.04:
        reasons.append("worse_day_loss")
    if to_float(replacement_nutrition.get("kcal")) < to_float(anchor_nutrition.get("kcal")) - 150:
        reasons.append("lower_kcal")
    if to_float(replacement_nutrition.get("protein_g")) < to_float(anchor_nutrition.get("protein_g")) - 12:
        reasons.append("lower_protein")
    if to_float(replacement_nutrition.get("carbs_g")) < to_float(anchor_nutrition.get("carbs_g")) - 25:
        reasons.append("lower_carbs")
    if slot in MAIN_SLOTS and to_float(replacement_nutrition.get("protein_g")) < 20:
        reasons.append("main_protein_weak")
    if quality_status == "review":
        reasons.append("quality_review_after_swap")
    if quality_status == "reject":
        reasons.append("quality_reject_after_swap")
    if set(flags).intersection(SEVERE_REALISM_FLAGS):
        reasons.append("severe_realism_flag")
    return ";".join(reasons) or "slightly_worse_but_usable"


def build_recommendation_text(
    combination_rows: Sequence[Mapping[str, Any]],
    anchor_rows: Sequence[Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
) -> str:
    exact = scenario_row(combination_rows, "no_repeat_exact_recipe")
    accept_exact = scenario_row(combination_rows, "accept_only_no_repeat")
    one_review = scenario_row(combination_rows, "allow_one_review_no_repeat")
    no_waffles = scenario_row(combination_rows, "no_waffles_repeat")
    no_chicken = scenario_row(combination_rows, "no_chicken_broccoli_repeat")
    no_anchor = scenario_row(combination_rows, "no_anchor_repeat")
    waffles_need = anchor_need(anchor_rows, "Mom's Best Waffles")
    chicken_need = anchor_need(anchor_rows, "Chicken and Broccoli Pasta")

    if bool_value(accept_exact.get("feasible")):
        choice = "A. No-repeat is feasible now: implement/tune multi-day no-repeat constraints."
        rationale = "A fost gasita combinatie all-accept fara repetitii exacte; datele sunt deja suficiente pentru o constrangere no-repeat audit-only."
    elif bool_value(one_review.get("feasible")):
        choice = "B. No-repeat is feasible only with review days: decide if review-level days are acceptable for demo."
        rationale = "Exista combinatie fara repetitii exacte cu cel mult o zi review, dar nu all-accept."
    elif (
        waffles_need != "usable_accept_replacements_exist"
        and chicken_need != "usable_accept_replacements_exist"
    ):
        choice = "E. No-repeat is not feasible due to both: add +5 breakfast and +10 lunch/dinner targeted."
        rationale = (
            "Nu exista combinatie no-repeat exact nici cu accept-only, nici cu cel mult o zi review; "
            "inlocuitorii pentru ambele ancore sunt in mare parte review-level sau costisitori."
        )
    elif not bool_value(no_waffles.get("feasible")) or waffles_need == "needs_more_alternatives":
        choice = "C. No-repeat is not feasible due to breakfast bottleneck: add +5 high-quality breakfast alternatives."
        rationale = "Waffles ramane ancora greu de inlocuit fara cost de calitate."
    elif not bool_value(no_chicken.get("feasible")) or chicken_need == "needs_more_alternatives":
        choice = "D. No-repeat is not feasible due to lunch/dinner bottleneck: add +5 to +10 complete lunch/dinner carb-protein mains."
        rationale = "Chicken and Broccoli Pasta ramane ancora main greu de inlocuit fara pierdere de fit."
    elif bool_value(no_anchor.get("feasible")) and not bool_value(exact.get("feasible")):
        choice = "A. No-repeat is partly feasible now: tune multi-day repetition constraints before adding data."
        rationale = "Ancorele pot fi limitate audit-only, dar mai exista alte repetitii/compromisuri; merita intai o calibrare de obiectiv."
    else:
        choice = "F. Current plus30 is good enough as technical multi-day demo: accept repeated anchors for now, document risk."
        rationale = "Planul este all-accept, dar repetitia ramane un risc cunoscut pentru demo."

    accept_count = sum(1 for row in candidate_rows if str(row.get("quality_gate_status")) == "accept")
    review_count = sum(1 for row in candidate_rows if str(row.get("quality_gate_status")) == "review")
    lines = [
        "Round29 next action recommendation",
        "",
        choice,
        f"Rationale: {rationale}",
        "",
        f"Candidate day pool: total={len(candidate_rows)}, accept={accept_count}, review={review_count}",
        f"no_repeat_exact_recipe feasible={bool_value(exact.get('feasible'))}",
        f"accept_only_no_repeat feasible={bool_value(accept_exact.get('feasible'))}",
        f"allow_one_review_no_repeat feasible={bool_value(one_review.get('feasible'))}",
        f"no_waffles_repeat feasible={bool_value(no_waffles.get('feasible'))}",
        f"no_chicken_broccoli_repeat feasible={bool_value(no_chicken.get('feasible'))}",
        f"no_anchor_repeat feasible={bool_value(no_anchor.get('feasible'))}",
        "",
        f"Waffles replacement assessment: {waffles_need}",
        f"Chicken Broccoli Pasta replacement assessment: {chicken_need}",
    ]
    return "\n".join(lines) + "\n"


def anchor_need(anchor_rows: Sequence[Mapping[str, Any]], anchor_name: str) -> str:
    rows = [row for row in anchor_rows if str(row.get("anchor_display_name")) == anchor_name]
    if not rows:
        return "missing_anchor_rows"
    accept_like = [
        row for row in rows
        if str(row.get("quality_gate_status_after_swap")) == "accept"
        and to_float(row.get("day_loss_delta_if_swapped")) <= 0.06
    ]
    review_like = [
        row for row in rows
        if str(row.get("quality_gate_status_after_swap")) in {"accept", "review"}
        and to_float(row.get("day_loss_delta_if_swapped")) <= 0.10
    ]
    if len(accept_like) >= 3:
        return "usable_accept_replacements_exist"
    if review_like:
        return "mostly_review_or_costly_replacements"
    return "needs_more_alternatives"


def build_summary_text(
    current_plan: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    combination_rows: Sequence[Mapping[str, Any]],
    anchor_rows: Sequence[Mapping[str, Any]],
    recommendation_text: str,
) -> str:
    summary = current_plan.get("multi_day_summary", {})
    lines = [
        "Round29 plus30 no-repeat feasibility audit",
        "",
        f"Dataset profile: {V1_2_GENERATOR_READY_PLUS30_PROFILE}",
        f"Current plus30 valid days: {summary.get('valid_day_count')}/3",
        f"Current plus30 accept days: {summary.get('accept_day_count')}",
        f"Current plus30 review days: {summary.get('review_day_count')}",
        f"Current plus30 unique recipes: {summary.get('unique_recipe_count')}",
        f"Current plus30 repeated recipes: {format_list(summary.get('repeated_recipe_ids'))}",
        f"Current plus30 multi_day_loss: {current_plan.get('multi_day_loss')}",
        "",
        "Candidate day pool:",
        f"- total={len(candidate_rows)}",
        f"- accept={sum(1 for row in candidate_rows if str(row.get('quality_gate_status')) == 'accept')}",
        f"- review={sum(1 for row in candidate_rows if str(row.get('quality_gate_status')) == 'review')}",
        f"- reject={sum(1 for row in candidate_rows if str(row.get('quality_gate_status')) == 'reject')}",
        f"- contains Waffles={sum(1 for row in candidate_rows if bool_value(row.get('contains_waffles')))}",
        f"- contains Chicken Broccoli Pasta={sum(1 for row in candidate_rows if bool_value(row.get('contains_chicken_broccoli_pasta')))}",
        "",
        "Scenario feasibility:",
    ]
    for row in combination_rows:
        if not bool_value(row.get("feasible")):
            lines.append(f"- {row.get('scenario')}: infeasible, reason={row.get('infeasible_reason')}")
            continue
        lines.append(
            f"- {row.get('scenario')}: feasible={row.get('feasible')}, "
            f"accept={row.get('accept_day_count')}, review={row.get('review_day_count')}, "
            f"repeated={row.get('repeated_recipe_count')}, unique={row.get('unique_recipe_count')}, "
            f"loss={row.get('multi_day_loss')}, result={row.get('strict_result')}"
        )

    lines.extend(["", "Anchor replacement analysis:"])
    for anchor_name in ANCHOR_NAMES.values():
        rows = [row for row in anchor_rows if str(row.get("anchor_display_name")) == anchor_name]
        if not rows:
            lines.append(f"- {anchor_name}: no rows")
            continue
        best = sorted(rows, key=lambda row: (to_float(row.get("day_loss_delta_if_swapped")), to_float(row.get("loss_gap_vs_anchor_slot"))))[0]
        lines.append(
            f"- {anchor_name}: best replacement={best.get('replacement_display_name')} "
            f"[{best.get('slot')}], day_loss_delta={best.get('day_loss_delta_if_swapped')}, "
            f"gate={best.get('quality_gate_status_after_swap')}, reason={best.get('why_replacement_loses')}"
        )

    lines.extend(["", recommendation_text.strip(), ""])
    return "\n".join(lines) + "\n"


def infeasible_reason(
    scenario: str,
    candidate_days: Sequence[Mapping[str, Any]],
    anchor_ids: Mapping[str, str],
) -> str:
    valid = [candidate for candidate in candidate_days if str(candidate.get("validation_status")) == "valid"]
    accept = [candidate for candidate in valid if str(candidate.get("quality_gate_status")) == "accept"]
    accept_review = [
        candidate
        for candidate in valid
        if str(candidate.get("quality_gate_status")) in {"accept", "review"}
    ]
    if len(valid) < 3:
        return "fewer_than_3_valid_candidate_days"
    if scenario == "accept_only_no_repeat" and len(accept) < 3:
        return "fewer_than_3_accept_candidate_days"
    if scenario in {"no_repeat_exact_recipe", "allow_one_review_no_repeat"} and len(accept_review) < 3:
        return "fewer_than_3_accept_or_review_candidate_days"
    if scenario == "no_waffles_repeat":
        anchor_id = anchor_ids.get("waffles", "")
        count_without = sum(1 for candidate in accept_review if anchor_id not in candidate_recipe_ids(candidate))
        return f"not_enough_usable_days_without_extra_waffles;usable_without_anchor={count_without}"
    if scenario == "no_chicken_broccoli_repeat":
        anchor_id = anchor_ids.get("chicken_broccoli_pasta", "")
        count_without = sum(1 for candidate in accept_review if anchor_id not in candidate_recipe_ids(candidate))
        return f"not_enough_usable_days_without_extra_chicken_broccoli;usable_without_anchor={count_without}"
    return "no_combination_satisfies_constraint_with_valid_non_reject_days"


def strict_result_from_counts(
    valid_day_count: int,
    accept_day_count: int,
    review_day_count: int,
    reject_day_count: int,
    repeated_recipe_count: int,
) -> str:
    if valid_day_count < 3 or reject_day_count > 0:
        return "bad"
    if accept_day_count == 3 and repeated_recipe_count == 0:
        return "good"
    if review_day_count <= 1 and repeated_recipe_count == 0:
        return "review_no_repeat"
    if repeated_recipe_count > 0:
        return "review_repetition"
    return "review"


def candidate_recipe_ids(candidate: Mapping[str, Any]) -> set[str]:
    plan = candidate.get("plan", {})
    if not isinstance(plan, Mapping):
        return set()
    return {
        meal_recipe_id(meal)
        for meal in plan.get("selected_meals", [])
        if isinstance(meal, Mapping) and meal_recipe_id(meal)
    }


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
        meals = day.get("selected_meals", []) if isinstance(day, Mapping) else []
        slot_parts = []
        for slot in SLOTS:
            meal = next((item for item in meals if isinstance(item, Mapping) and meal_slot(item) == slot), {})
            slot_parts.append(f"{slot}:{meal_name(meal)}")
        day_parts.append(f"D{day.get('day_index')}[" + " | ".join(slot_parts) + "]")
    return " || ".join(day_parts)


def scenario_row(rows: Sequence[Mapping[str, Any]], scenario: str) -> Mapping[str, Any]:
    for row in rows:
        if str(row.get("scenario")) == scenario:
            return row
    return {}


def scenario_value(rows: Sequence[Mapping[str, Any]], scenario: str, key: str) -> Any:
    return scenario_row(rows, scenario).get(key)


def nutrition_from_row(row: Mapping[str, Any]) -> dict[str, float]:
    return {
        "kcal": to_float(row.get("kcal")),
        "protein_g": to_float(row.get("protein_g")),
        "carbs_g": to_float(row.get("carbs_g")),
        "fat_g": to_float(row.get("fat_g")),
    }


def meal_recipe_id(meal: Mapping[str, Any]) -> str:
    return str(meal.get("recipe_id", "")).strip()


def meal_name(meal: Mapping[str, Any]) -> str:
    return str(meal.get("display_name") or meal.get("recipe_name") or meal.get("recipe_id", "")).strip()


def meal_slot(meal: Mapping[str, Any]) -> str:
    return str(meal.get("slot", "")).strip()


def meal_flags(meal: Mapping[str, Any]) -> list[str]:
    flags = []
    for key in ("meal_realism_flags", "meal_realism_practical_flags", "realism_flags"):
        value = meal.get(key)
        if isinstance(value, list):
            flags.extend(str(item) for item in value if str(item))
        elif isinstance(value, str):
            flags.extend(part.strip() for part in value.replace("|", ";").split(";") if part.strip())
    return sorted(set(flags))


def quality_rank(status: str) -> int:
    return {"accept": 0, "review": 1, "reject": 2}.get(status, 3)


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"true", "1", "yes"}


def format_list(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value)
    if value is None:
        return "none"
    text = str(value).strip()
    return text or "none"


if __name__ == "__main__":
    main()
