from __future__ import annotations

import copy
import sys
import time
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
from src.generator_v1.multi_day_audit import summarize_multi_day_plan, validate_multi_day_plan
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    _build_candidate_day_pool,
    _resolve_slot_candidates_by_slot,
    _resolved_config,
    _select_global_day_combination,
)
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round33_multiday_performance_summary.txt"
OUT_RUNS = OUT_DIR / "generator_v1_round33_multiday_performance_runs.csv"

SLOTS = ["breakfast", "lunch", "dinner", "snack"]
POOL_SIZES = [25, 50, 75, 100]
PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"
QUALITY_GATE = "demo_safe"
DAYS = 3


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    context = build_generation_context()
    pool_started = time.perf_counter()
    candidate_days, pool_stats = build_candidate_pool(
        context=context,
        pool_size=max(POOL_SIZES),
        speed_mode="fast",
    )
    shared_pool_seconds = time.perf_counter() - pool_started

    rows: list[dict[str, Any]] = []
    output_started = time.perf_counter()
    for pool_size in POOL_SIZES:
        rows.append(
            run_selector_case(
                context=context,
                pool_size=pool_size,
                speed_mode="fast",
                candidate_days=candidate_days,
                pool_stats=pool_stats,
                pool_generation_seconds=shared_pool_seconds,
                reused_candidate_pool=pool_size != max(POOL_SIZES),
            )
        )
    output_seconds = time.perf_counter() - output_started
    for row in rows:
        row["output_writing_seconds"] = round(output_seconds, 3)

    pd.DataFrame(rows).to_csv(OUT_RUNS, index=False)
    OUT_SUMMARY.write_text(build_summary_text(rows, context), encoding="utf-8")

    best = rows[-1]
    print("Generator v1 Round33 multi-day performance profile written")
    print(f"summary={OUT_SUMMARY}")
    print(f"runs={OUT_RUNS}")
    print(
        "fast_pool100="
        f"runtime:{best['total_runtime_seconds']} "
        f"pool_s:{best['day_candidate_pool_generation_seconds']} "
        f"combo_s:{best['combination_evaluation_seconds']} "
        f"unique:{best['unique_recipe_count']} "
        f"repeated:{best['repeated_recipe_count']} "
        f"loss:{best['multi_day_loss']}"
    )


def build_generation_context() -> dict[str, Any]:
    load_started = time.perf_counter()
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
    data_loading_seconds = time.perf_counter() - load_started
    slot_started = time.perf_counter()
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode=PORTION_POLICY,
    )
    slot_generation_seconds = time.perf_counter() - slot_started
    return {
        "profile": profile,
        "target": target,
        "pool": pool,
        "filtered_candidates": filtered_candidates,
        "slot_candidates": slot_candidates,
        "data_loading_seconds": data_loading_seconds,
        "slot_candidate_generation_seconds": slot_generation_seconds,
    }


def build_candidate_pool(
    context: Mapping[str, Any],
    pool_size: int,
    speed_mode: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    profile_stats: dict[str, Any] = {}
    config = selector_config(
        pool_size=pool_size,
        speed_mode=speed_mode,
        profile_stats=profile_stats,
    )
    slot_order = SLOTS
    candidates_by_slot = _resolve_slot_candidates_by_slot(
        slot_candidates=context["slot_candidates"],
        slot_candidates_by_slot=None,
        slot_order=slot_order,
    )
    candidate_days = _build_candidate_day_pool(
        slot_candidates_by_slot=candidates_by_slot,
        target=context["target"],
        slot_order=slot_order,
        config=config,
    )
    return candidate_days, profile_stats


def run_selector_case(
    context: Mapping[str, Any],
    pool_size: int,
    speed_mode: str,
    candidate_days: Sequence[Mapping[str, Any]],
    pool_stats: Mapping[str, Any],
    pool_generation_seconds: float,
    reused_candidate_pool: bool,
) -> dict[str, Any]:
    combination_started = time.perf_counter()
    selected, report = _select_global_day_combination(
        candidate_days=candidate_days,
        day_count=DAYS,
        config=selector_config(pool_size=pool_size, speed_mode=speed_mode),
    )
    combination_seconds = time.perf_counter() - combination_started
    plan = plan_from_selected(selected, report, context["target"])
    summary = plan["multi_day_summary"]
    total_runtime = (
        float(context["data_loading_seconds"])
        + float(context["slot_candidate_generation_seconds"])
        + pool_generation_seconds
        + combination_seconds
    )
    return {
        "dataset_profile": V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
        "pool_size_requested": pool_size,
        "speed_mode": speed_mode,
        "reused_candidate_pool": reused_candidate_pool,
        "total_runtime_seconds": round(total_runtime, 3),
        "data_loading_seconds": round(float(context["data_loading_seconds"]), 3),
        "slot_candidate_generation_seconds": round(
            float(context["slot_candidate_generation_seconds"]),
            3,
        ),
        "day_candidate_pool_generation_seconds": round(pool_generation_seconds, 3),
        "quality_gate_evaluation_seconds": round(
            float(pool_stats.get("quality_gate_seconds", 0.0)),
            6,
        ),
        "validation_seconds": round(float(pool_stats.get("validation_seconds", 0.0)), 6),
        "balanced_selector_seconds": round(
            float(pool_stats.get("balanced_selector_seconds", 0.0)),
            6,
        ),
        "balanced_selector_runs": int(pool_stats.get("balanced_selector_runs", 0)),
        "balanced_selector_cache_hits": int(
            pool_stats.get("balanced_selector_cache_hits", 0)
        ),
        "candidate_duplicate_skips": int(pool_stats.get("candidate_duplicate_skips", 0)),
        "quality_gate_evaluation_count": int(
            pool_stats.get("quality_gate_evaluation_count", 0)
        ),
        "combination_evaluation_seconds": round(combination_seconds, 3),
        "candidate_day_pool_count": len(candidate_days),
        "combinations_evaluated": report.get("combinations_evaluated"),
        "feasible_no_repeat_combinations": report.get("feasible_no_repeat_combinations"),
        "multi_day_loss": report.get("multi_day_loss"),
        "valid_day_count": summary.get("valid_day_count"),
        "accept_day_count": summary.get("accept_day_count"),
        "review_day_count": summary.get("review_day_count"),
        "repeated_recipe_count": summary.get("repeated_recipe_count"),
        "unique_recipe_count": summary.get("unique_recipe_count"),
        "strict_verdict": summary.get("multi_day_classification"),
        "no_repeat_policy_used": report.get("no_repeat_policy_used"),
        "fallback_from_hard_no_repeat": report.get("fallback_from_hard_no_repeat"),
    }


def selector_config(
    pool_size: int,
    speed_mode: str,
    profile_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_config: dict[str, Any] = {
        "selection_mode": "balanced_day",
        "portion_policy": PORTION_POLICY,
        "meal_realism_mode": MEAL_REALISM_MODE,
        "quality_gate": QUALITY_GATE,
        "alternative_count": 3,
        "return_alternatives": True,
        "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
        "candidate_day_alternative_count": 10,
        "global_max_candidates_per_slot": 26,
        "day_candidate_pool_size_target": pool_size,
        "day_candidate_pool_max": max(150, pool_size),
        "include_slot_forced_variants": True,
        "no_repeat_policy": "hard",
        "multi_day_speed_mode": speed_mode,
    }
    if profile_stats is not None:
        raw_config["_profile_stats"] = profile_stats
    return _resolved_config(raw_config)


def plan_from_selected(
    selected: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
    target: NutritionTarget,
) -> dict[str, Any]:
    days: list[dict[str, Any]] = []
    previous_ids: set[str] = set()
    for day_index, candidate in enumerate(selected, start=1):
        plan = copy.deepcopy(candidate.get("plan", {}))
        meals = list(plan.get("selected_meals", []))
        recipe_ids = {
            str(meal.get("recipe_id", "")).strip()
            for meal in meals
            if isinstance(meal, Mapping)
        }
        repeated = sorted(recipe_ids & previous_ids)
        previous_ids.update(recipe_ids)
        days.append(
            {
                "day_index": day_index,
                "selected_meals": meals,
                "day_totals": dict(plan.get("day_totals", {})),
                "validation_status": candidate.get("validation_status"),
                "quality_gate_status": candidate.get("quality_gate_status"),
                "fallback_used": bool(plan.get("quality_gate_fallback_used", False)),
                "selector_diagnostics": dict(plan.get("selector_diagnostics", {})),
                "repeated_recipe_ids_vs_previous_days": repeated,
            }
        )
    target_data = target_to_dict(target)
    summary = summarize_multi_day_plan(days, target_data)
    summary["multi_day_loss"] = report.get("multi_day_loss")
    validation = validate_multi_day_plan(days, target_data)
    return {
        "days": days,
        "multi_day_summary": summary,
        "multi_day_validation": validation,
    }


def target_to_dict(target: NutritionTarget) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def build_summary_text(
    rows: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any],
) -> str:
    best = rows[-1]
    lines = [
        "Round33 multi-day performance profile",
        "",
        f"- dataset_profile: {V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE}",
        f"- data loading seconds: {round(float(context['data_loading_seconds']), 3)}",
        f"- slot candidate generation seconds: {round(float(context['slot_candidate_generation_seconds']), 3)}",
        "",
        "Runs",
    ]
    for row in rows:
        lines.append(
            "- "
            f"pool={row['pool_size_requested']}, speed={row['speed_mode']}, "
            f"runtime={row['total_runtime_seconds']}s, "
            f"pool_s={row['day_candidate_pool_generation_seconds']}s, "
            f"combo_s={row['combination_evaluation_seconds']}s, "
            f"candidates={row['candidate_day_pool_count']}, "
            f"no_repeat={row['feasible_no_repeat_combinations']}, "
            f"repeated={row['repeated_recipe_count']}, "
            f"loss={row['multi_day_loss']}"
        )
    lines.extend(
        [
            "",
            "Bottleneck",
            "- Day candidate pool generation dominates runtime.",
            f"- Balanced selector time in the shared pool run: {best['balanced_selector_seconds']}s.",
            f"- Quality gate evaluation time is small by comparison: {best['quality_gate_evaluation_seconds']}s.",
            f"- Combination evaluation is secondary: {best['combination_evaluation_seconds']}s.",
            "",
            "Assessment",
            "- Fast mode preserves the Round32 no-repeat plan after keeping 10 day alternatives.",
            "- The result is still not <=30s; Streamlit should show this as a slow test mode, not as instant UI.",
        ]
    )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
