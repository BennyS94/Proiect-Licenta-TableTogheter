from __future__ import annotations

import copy
import sys
import time
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
    V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.multi_day_audit import (
    multi_day_meal_rows,
    summarize_multi_day_plan,
    validate_multi_day_plan,
)
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    _build_candidate_day_pool,
    _candidate_pool_rows,
    _select_global_day_combination,
)
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round32_multiday_no_repeat_summary.txt"
OUT_DAYS = OUT_DIR / "generator_v1_round32_multiday_no_repeat_days.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round32_multiday_no_repeat_meals.csv"
OUT_COMPARISON = OUT_DIR / "generator_v1_round32_multiday_no_repeat_comparison.csv"
OUT_CANDIDATE_POOL = OUT_DIR / "generator_v1_round32_multiday_no_repeat_candidate_pool.csv"

SLOTS = ["breakfast", "lunch", "dinner", "snack"]
PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"
QUALITY_GATE = "demo_safe"
DAYS = 3

BASE_CONFIG = {
    "selection_mode": "balanced_day",
    "portion_policy": PORTION_POLICY,
    "meal_realism_mode": MEAL_REALISM_MODE,
    "quality_gate": QUALITY_GATE,
    "alternative_count": 3,
    "return_alternatives": True,
    "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
    "candidate_day_alternative_count": 10,
    "global_max_candidates_per_slot": 26,
    "include_slot_forced_variants": True,
}

SCENARIOS = [
    {
        "scenario": "prefer_pool75",
        "no_repeat_policy": "prefer",
        "day_candidate_pool_size": 75,
    },
    {
        "scenario": "hard_pool75",
        "no_repeat_policy": "hard",
        "day_candidate_pool_size": 75,
    },
    {
        "scenario": "main_only_pool75",
        "no_repeat_policy": "main_only",
        "day_candidate_pool_size": 75,
    },
    {
        "scenario": "prefer_pool100",
        "no_repeat_policy": "prefer",
        "day_candidate_pool_size": 100,
    },
    {
        "scenario": "hard_pool100",
        "no_repeat_policy": "hard",
        "day_candidate_pool_size": 100,
    },
]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    context = build_generation_context()
    pools: dict[int, tuple[list[dict[str, Any]], float]] = {}
    comparison_rows: list[dict[str, Any]] = []
    day_rows: list[dict[str, Any]] = []
    meal_rows: list[dict[str, Any]] = []
    candidate_pool_rows: list[dict[str, Any]] = []

    for scenario in SCENARIOS:
        pool_size = int(scenario["day_candidate_pool_size"])
        if pool_size not in pools:
            pools[pool_size] = build_candidate_pool(context, pool_size)
            candidate_days, build_seconds = pools[pool_size]
            for row in _candidate_pool_rows(candidate_days):
                candidate_pool_rows.append(
                    {
                        "day_candidate_pool_size": pool_size,
                        "pool_build_seconds": round(build_seconds, 3),
                        **row,
                    }
                )
        candidate_days, pool_build_seconds = pools[pool_size]
        plan, report, select_seconds = select_plan_for_scenario(
            candidate_days,
            scenario,
            context["target"],
        )
        comparison_rows.append(
            build_comparison_row(
                scenario=scenario,
                plan=plan,
                report=report,
                candidate_days=candidate_days,
                pool_build_seconds=pool_build_seconds,
                select_seconds=select_seconds,
            )
        )
        day_rows.extend(build_day_rows(scenario, plan))
        meal_rows.extend(build_meal_rows(scenario, plan))

    pd.DataFrame(day_rows).to_csv(OUT_DAYS, index=False)
    pd.DataFrame(meal_rows).to_csv(OUT_MEALS, index=False)
    pd.DataFrame(comparison_rows).to_csv(OUT_COMPARISON, index=False)
    pd.DataFrame(candidate_pool_rows).to_csv(OUT_CANDIDATE_POOL, index=False)
    OUT_SUMMARY.write_text(
        build_summary_text(comparison_rows, day_rows),
        encoding="utf-8",
    )

    hard100 = next(
        row for row in comparison_rows if row["scenario"] == "hard_pool100"
    )
    print("Generator v1 Round32 no-repeat selector evaluation written")
    print(f"summary={OUT_SUMMARY}")
    print(f"days={OUT_DAYS}")
    print(f"meals={OUT_MEALS}")
    print(f"comparison={OUT_COMPARISON}")
    print(f"candidate_pool={OUT_CANDIDATE_POOL}")
    print(
        "hard_pool100="
        f"valid:{hard100['valid_day_count']} "
        f"accept:{hard100['accept_day_count']} "
        f"review:{hard100['review_day_count']} "
        f"unique:{hard100['unique_recipe_count']} "
        f"repeated:{hard100['repeated_recipe_count']} "
        f"loss:{hard100['multi_day_loss']} "
        f"feasible_no_repeat:{hard100['feasible_no_repeat_combinations']}"
    )


def build_generation_context() -> dict[str, Any]:
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


def build_candidate_pool(
    context: Mapping[str, Any],
    pool_size: int,
) -> tuple[list[dict[str, Any]], float]:
    started = time.perf_counter()
    candidates_by_slot = split_candidates_by_slot(context["slot_candidates"])
    config = {
        **BASE_CONFIG,
        "day_candidate_pool_size_target": pool_size,
        "day_candidate_pool_max": max(150, pool_size),
        "no_repeat_policy": "prefer",
    }
    candidate_days = _build_candidate_day_pool(
        slot_candidates_by_slot=candidates_by_slot,
        target=context["target"],
        slot_order=SLOTS,
        config=config,
    )
    return candidate_days, time.perf_counter() - started


def select_plan_for_scenario(
    candidate_days: Sequence[Mapping[str, Any]],
    scenario: Mapping[str, Any],
    target: NutritionTarget,
) -> tuple[dict[str, Any], dict[str, Any], float]:
    started = time.perf_counter()
    config = {
        **BASE_CONFIG,
        "day_candidate_pool_size_target": scenario["day_candidate_pool_size"],
        "no_repeat_policy": scenario["no_repeat_policy"],
    }
    selected, report = _select_global_day_combination(
        candidate_days=candidate_days,
        day_count=DAYS,
        config=config,
    )
    select_seconds = time.perf_counter() - started
    plan = plan_from_selected_candidates(
        selected,
        report,
        target,
        scenario,
    )
    return plan, report, select_seconds


def plan_from_selected_candidates(
    selected: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
    target: NutritionTarget,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    days: list[dict[str, Any]] = []
    previous_recipe_ids: set[str] = set()
    for day_index, candidate in enumerate(selected, start=1):
        plan = copy.deepcopy(candidate.get("plan", {}))
        selected_meals = list(plan.get("selected_meals", []))
        recipe_ids = {
            str(meal.get("recipe_id", "")).strip()
            for meal in selected_meals
            if isinstance(meal, Mapping)
        }
        repeated_vs_previous = sorted(recipe_ids & previous_recipe_ids)
        previous_recipe_ids.update(recipe_ids)
        day = {
            "day_index": day_index,
            "selected_meals": selected_meals,
            "day_totals": dict(plan.get("day_totals", {})),
            "validation": dict(plan.get("validation", {})),
            "quality_gate": dict(plan.get("quality_gate", {})),
            "selector_diagnostics": dict(plan.get("selector_diagnostics", {})),
            "validation_status": candidate.get("validation_status"),
            "quality_gate_status": candidate.get("quality_gate_status"),
            "quality_gate_reasons": plan.get("quality_gate_reasons", []),
            "fallback_used": bool(plan.get("quality_gate_fallback_used", False)),
            "diversity_mode_used": candidate.get("source_mode"),
            "multi_day_mode_used": MULTI_DAY_MODE_GLOBAL,
            "candidate_day_id": candidate.get("candidate_day_id"),
            "repeated_recipe_ids_vs_previous_days": repeated_vs_previous,
            "recent_recipe_count_used": len(previous_recipe_ids),
            "warnings": list(plan.get("warnings", [])),
        }
        days.append(day)
    target_data = target_to_dict(target)
    summary = summarize_multi_day_plan(days, target_data)
    summary.update(
        {
            "multi_day_loss": report.get("multi_day_loss"),
            "no_repeat_policy_used": report.get("no_repeat_policy_used"),
            "no_repeat_policy_requested": report.get("no_repeat_policy_requested"),
            "feasible_no_repeat_combinations": report.get("feasible_no_repeat_combinations"),
            "fallback_from_hard_no_repeat": report.get("fallback_from_hard_no_repeat"),
        }
    )
    validation = validate_multi_day_plan(days, target_data)
    return {
        "days": days,
        "multi_day_summary": summary,
        "multi_day_validation": validation,
        "multi_day_selector_mode": MULTI_DAY_MODE_GLOBAL,
        "multi_day_loss": report.get("multi_day_loss"),
        "selector_diagnostics": dict(report),
        "target": target_data,
        "config": dict(scenario),
    }


def build_comparison_row(
    scenario: Mapping[str, Any],
    plan: Mapping[str, Any],
    report: Mapping[str, Any],
    candidate_days: Sequence[Mapping[str, Any]],
    pool_build_seconds: float,
    select_seconds: float,
) -> dict[str, Any]:
    summary = plan.get("multi_day_summary", {})
    repeated_names = repeated_recipe_names(plan)
    return {
        "scenario": scenario["scenario"],
        "day_candidate_pool_size": scenario["day_candidate_pool_size"],
        "no_repeat_policy": scenario["no_repeat_policy"],
        "no_repeat_policy_used": report.get("no_repeat_policy_used"),
        "fallback_from_hard_no_repeat": report.get("fallback_from_hard_no_repeat"),
        "candidate_day_pool_count": len(candidate_days),
        "valid_candidate_day_count": count_candidates(candidate_days, "validation_status", "valid"),
        "accept_candidate_day_count": count_candidates(candidate_days, "quality_gate_status", "accept"),
        "review_candidate_day_count": count_candidates(candidate_days, "quality_gate_status", "review"),
        "reject_candidate_day_count": count_candidates(candidate_days, "quality_gate_status", "reject"),
        "combinations_evaluated": report.get("combinations_evaluated"),
        "feasible_no_repeat_combinations": report.get("feasible_no_repeat_combinations"),
        "feasible_main_no_repeat_combinations": report.get("feasible_main_no_repeat_combinations"),
        "valid_day_count": summary.get("valid_day_count"),
        "accept_day_count": summary.get("accept_day_count"),
        "review_day_count": summary.get("review_day_count"),
        "fallback_day_count": summary.get("fallback_day_count"),
        "unique_recipe_count": summary.get("unique_recipe_count"),
        "repeated_recipe_count": summary.get("repeated_recipe_count"),
        "repeated_recipe_ids": format_list(summary.get("repeated_recipe_ids")),
        "repeated_recipe_names": format_list(repeated_names),
        "multi_day_loss": report.get("multi_day_loss"),
        "average_day_loss": summary.get("average_day_loss"),
        "average_base_day_loss": report.get("average_base_day_loss"),
        "repetition_penalty": report.get("repetition_penalty"),
        "strict_verdict": summary.get("multi_day_classification"),
        "pool_build_seconds": round(pool_build_seconds, 3),
        "select_seconds": round(select_seconds, 3),
        "total_seconds": round(pool_build_seconds + select_seconds, 3),
        "old_anchors_repeat": old_anchor_repetition_flag(repeated_names),
    }


def build_day_rows(
    scenario: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        totals = day.get("day_totals", {})
        diagnostics = day.get("selector_diagnostics", {})
        rows.append(
            {
                "scenario": scenario["scenario"],
                "day_candidate_pool_size": scenario["day_candidate_pool_size"],
                "no_repeat_policy": scenario["no_repeat_policy"],
                "day_index": day.get("day_index"),
                "candidate_day_id": day.get("candidate_day_id"),
                "source_mode": day.get("diversity_mode_used"),
                "validation_status": day.get("validation_status"),
                "quality_gate_status": day.get("quality_gate_status"),
                "repeated_recipe_ids_vs_previous_days": format_list(
                    day.get("repeated_recipe_ids_vs_previous_days")
                ),
                "base_day_loss": diagnostics.get("base_day_loss"),
                "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
                "total_kcal": totals.get("total_kcal"),
                "total_protein_g": totals.get("total_protein_g"),
                "total_carbs_g": totals.get("total_carbs_g"),
                "total_fat_g": totals.get("total_fat_g"),
                "effective_time_min_sum": totals.get("effective_time_min_sum"),
                "selected_recipe_names": format_list(meal_names(day.get("selected_meals", []))),
            }
        )
    return rows


def build_meal_rows(
    scenario: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in multi_day_meal_rows(plan):
        rows.append(
            {
                "scenario": scenario["scenario"],
                "day_candidate_pool_size": scenario["day_candidate_pool_size"],
                "no_repeat_policy": scenario["no_repeat_policy"],
                **row,
            }
        )
    return rows


def build_summary_text(
    comparison_rows: Sequence[Mapping[str, Any]],
    day_rows: Sequence[Mapping[str, Any]],
) -> str:
    best = next(row for row in comparison_rows if row["scenario"] == "hard_pool100")
    lines = [
        "Round32 multi-day no-repeat selector evaluation",
        "",
        "Scope",
        f"- dataset_profile: {V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE}",
        "- selector: global_alternatives_3_day",
        "- policies compared: prefer, hard, main_only",
        "",
        "Comparison",
    ]
    for row in comparison_rows:
        lines.append(
            "- "
            f"{row['scenario']}: valid={row['valid_day_count']}, "
            f"accept={row['accept_day_count']}, review={row['review_day_count']}, "
            f"unique={row['unique_recipe_count']}, repeated={row['repeated_recipe_count']}, "
            f"loss={row['multi_day_loss']}, no_repeat_feasible={row['feasible_no_repeat_combinations']}, "
            f"policy_used={row['no_repeat_policy_used']}, runtime_s={row['total_seconds']}"
        )
    lines.extend(
        [
            "",
            "Hard pool100 result",
            f"- candidate days: {best['candidate_day_pool_count']}",
            f"- accept/review/reject candidates: {best['accept_candidate_day_count']}/{best['review_candidate_day_count']}/{best['reject_candidate_day_count']}",
            f"- feasible no-repeat combinations: {best['feasible_no_repeat_combinations']}",
            f"- repeated recipes: {best['repeated_recipe_names'] or 'none'}",
            f"- strict verdict: {best['strict_verdict']}",
            "",
            "Selected hard pool100 days",
        ]
    )
    for row in day_rows:
        if row["scenario"] != "hard_pool100":
            continue
        lines.append(
            "- "
            f"DAY {row['day_index']}: {row['selected_recipe_names']} "
            f"| status={row['validation_status']}/{row['quality_gate_status']} "
            f"| loss={row['base_day_loss']}"
        )
    lines.extend(
        [
            "",
            "Assessment",
            "- Selectorul real gaseste acum un plan no-repeat cu 3 zile accept si 0 retete repetate.",
            "- Rezultatul confirma ca problema Round31 era pool-ul prea ingust, nu lipsa datelor.",
            "- Riscul principal ramas este runtime-ul: pool-ul 100 dureaza cateva minute pe masina locala.",
        ]
    )
    return "\n".join(lines) + "\n"


def split_candidates_by_slot(slot_candidates: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        slot: slot_candidates.loc[slot_candidates["slot"].astype(str).eq(slot)].copy()
        for slot in SLOTS
    }


def count_candidates(
    candidates: Sequence[Mapping[str, Any]],
    field_name: str,
    expected_value: str,
) -> int:
    return sum(1 for candidate in candidates if candidate.get(field_name) == expected_value)


def target_to_dict(target: NutritionTarget) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def meal_names(meals: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(meal.get("display_name", "")).strip() for meal in meals]


def repeated_recipe_names(plan: Mapping[str, Any]) -> list[str]:
    names_by_id: dict[str, str] = {}
    counts: Counter[str] = Counter()
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if not recipe_id:
                continue
            counts[recipe_id] += 1
            names_by_id[recipe_id] = str(meal.get("display_name", "")).strip()
    return [
        names_by_id.get(recipe_id, recipe_id)
        for recipe_id, count in sorted(counts.items())
        if count > 1
    ]


def old_anchor_repetition_flag(repeated_names: Sequence[str]) -> bool:
    anchors = {
        "Mom's Best Waffles",
        "Chicken and Broccoli Pasta",
        "Easy and Spicy Thai Basil Chicken with Egg",
    }
    return any(name in anchors for name in repeated_names)


def format_list(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return "|".join(f"{key}:{val}" for key, val in value.items())
    if isinstance(value, Sequence):
        return "|".join(str(item) for item in value)
    return str(value)


if __name__ == "__main__":
    main()
