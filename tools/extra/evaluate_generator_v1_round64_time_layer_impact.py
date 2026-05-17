from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.candidate_filter import (  # noqa: E402
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (  # noqa: E402
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_NUTRITION_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_PROFILE,
    V1_2_DEMO_FINAL_TIME_LAYER_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.multi_day_selector import (  # noqa: E402
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import build_nutrition_target  # noqa: E402


PROFILE_PATH = ROOT / "profiles/member_profile_demo_v1.json"
AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_round64_time_layer_impact_summary.txt"
DAYS_OUT = AUDIT_DIR / "generator_v1_round64_time_layer_days.csv"
MEALS_OUT = AUDIT_DIR / "generator_v1_round64_time_layer_meals.csv"
SELECTED_TIME_OUT = AUDIT_DIR / "generator_v1_round64_time_selected_recipe_time_details.csv"


DATASETS = [
    {
        "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
        "recipes": ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        "ingredients": ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        "nutrition": ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
    },
    {
        "dataset_profile": V1_2_DEMO_FINAL_TIME_LAYER_PROFILE,
        "recipes": ROOT / V1_2_DEMO_FINAL_TIME_LAYER_RECIPES_PATH,
        "ingredients": ROOT / V1_2_DEMO_FINAL_TIME_LAYER_INGREDIENTS_PATH,
        "nutrition": ROOT / V1_2_DEMO_FINAL_TIME_LAYER_NUTRITION_PATH,
    },
]


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    for config in DATASETS:
        if not Path(config["recipes"]).exists():
            continue
        results.append(_generate_dataset_result(config))
    day_rows = []
    meal_rows = []
    selected_rows = []
    for result in results:
        plan = result["plan"]
        dataset_profile = result["dataset_profile"]
        day_rows.extend(_day_rows(dataset_profile, plan))
        meal_rows.extend(_meal_rows(dataset_profile, plan))
        selected_rows.extend(_selected_time_rows(dataset_profile, plan))

    pd.DataFrame(day_rows).to_csv(DAYS_OUT, index=False)
    pd.DataFrame(meal_rows).to_csv(MEALS_OUT, index=False)
    pd.DataFrame(selected_rows).to_csv(SELECTED_TIME_OUT, index=False)
    SUMMARY_OUT.write_text(_summary_text(results), encoding="utf-8")

    print("Round64 time layer generator impact audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"days={DAYS_OUT}")
    print(f"meals={MEALS_OUT}")
    print(f"selected_time={SELECTED_TIME_OUT}")
    print(f"datasets_evaluated={len(results)}")


def _generate_dataset_result(config: dict[str, Any]) -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=config["recipes"],
        ingredients_path=config["ingredients"],
        nutrition_path=config["nutrition"],
        dataset_profile=config["dataset_profile"],
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
    plan = generate_multi_day_plan(
        profile=profile,
        target=target,
        slot_candidates=slot_candidates,
        days=3,
        config={
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "alternative_count": 3,
            "return_alternatives": True,
            "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
            "candidate_day_alternative_count": 10,
            "global_max_candidates_per_slot": 26,
            "day_candidate_pool_size_target": 75,
            "day_candidate_pool_max": 150,
            "include_slot_forced_variants": True,
            "no_repeat_policy": "hard",
            "multi_day_speed_mode": "fast",
            "day_candidate_builder": "direct_from_slots",
            "direct_slot_shortlist_size": 12,
        },
    )
    return {
        "dataset_profile": config["dataset_profile"],
        "plan": plan,
        "eligible_candidate_count": len(pool.eligible_candidates),
        "slot_candidate_count": len(slot_candidates),
    }


def _day_rows(dataset_profile: str, plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        totals = day.get("day_totals", {})
        rows.append(
            {
                "dataset_profile": dataset_profile,
                "day_index": day.get("day_index"),
                "validation_status": day.get("validation_status"),
                "quality_gate_status": day.get("quality_gate_status"),
                "total_kcal": totals.get("total_kcal"),
                "total_protein_g": totals.get("total_protein_g"),
                "effective_time_min_sum": totals.get("effective_time_min_sum"),
                "passive_time_estimated_sum": totals.get("passive_time_estimated_sum"),
                "day_loss": day.get("selector_diagnostics", {}).get("day_loss"),
            }
        )
    return rows


def _meal_rows(dataset_profile: str, plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            rows.append(
                {
                    "dataset_profile": dataset_profile,
                    "day_index": day.get("day_index"),
                    "slot": meal.get("slot"),
                    "recipe_id": meal.get("recipe_id"),
                    "display_name": meal.get("display_name"),
                    "portion_multiplier": meal.get("portion_multiplier"),
                    "kcal": meal.get("kcal"),
                    "protein_g": meal.get("protein_g"),
                    "total_time_min": meal.get("total_time_min"),
                    "total_elapsed_time_min": meal.get("total_elapsed_time_min"),
                    "active_time_estimated_min": meal.get("active_time_estimated_min"),
                    "passive_time_estimated_min": meal.get("passive_time_estimated_min"),
                    "effective_time_min_for_scoring": meal.get(
                        "effective_time_min_for_scoring"
                    ),
                    "has_long_passive_time": meal.get("has_long_passive_time"),
                    "time_confidence": meal.get("time_confidence"),
                    "time_estimation_method": meal.get("time_estimation_method"),
                    "time_warnings": _format_reasons(meal.get("time_warnings")),
                }
            )
    return rows


def _selected_time_rows(dataset_profile: str, plan: dict[str, Any]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    rows = []
    for row in _meal_rows(dataset_profile, plan):
        recipe_id = str(row.get("recipe_id") or "")
        if recipe_id in seen:
            continue
        seen.add(recipe_id)
        rows.append(row)
    return rows


def _summary_text(results: list[dict[str, Any]]) -> str:
    lines = [
        "Generator v1 Round64 time layer impact summary",
        "",
        "Config: v1_2 demo recommended 3-day, balanced_day, target_aware, practical, demo_safe, hard no-repeat, direct_from_slots.",
        "",
    ]
    profiles = {}
    for result in results:
        dataset_profile = result["dataset_profile"]
        plan = result["plan"]
        summary = plan.get("multi_day_summary", {})
        meals = _meal_rows(dataset_profile, plan)
        profiles[dataset_profile] = {str(row["recipe_id"]) for row in meals}
        time_warning_count = sum(
            1 for row in meals if str(row.get("time_warnings") or "").strip()
        )
        effective_sum = sum(_to_float(row.get("effective_time_min_for_scoring")) or 0.0 for row in meals)
        long_passive_count = sum(1 for row in meals if bool(row.get("has_long_passive_time")))
        lines.extend(
            [
                f"Dataset: {dataset_profile}",
                f"eligible_candidate_count={result['eligible_candidate_count']}",
                f"slot_candidate_count={result['slot_candidate_count']}",
                f"actual_days_generated={summary.get('actual_days_generated')}",
                f"valid_day_count={summary.get('valid_day_count')}",
                f"accept_day_count={summary.get('accept_day_count')}",
                f"multi_day_loss={summary.get('multi_day_loss')}",
                f"effective_time_sum_selected={round(effective_sum, 1)}",
                f"long_passive_selected_count={long_passive_count}",
                f"time_warning_selected_count={time_warning_count}",
                f"selected_recipe_ids={', '.join(sorted(profiles[dataset_profile]))}",
                "",
            ]
        )
    if len(profiles) >= 2:
        base = profiles.get(V1_2_DEMO_FINAL_PROFILE, set())
        layered = profiles.get(V1_2_DEMO_FINAL_TIME_LAYER_PROFILE, set())
        lines.extend(
            [
                "Selected recipe changes:",
                f"unchanged_count={len(base & layered)}",
                f"base_only={', '.join(sorted(base - layered)) or 'none'}",
                f"time_layer_only={', '.join(sorted(layered - base)) or 'none'}",
                "",
                "Interpretation:",
                "- Time layer is useful if effective-time warnings are clearer and selected long-passive recipes are not hidden as short active tasks.",
                "- Nutrition and selection formulas are otherwise unchanged; differences come from normalized time features only.",
            ]
        )
    else:
        lines.append("Time-layer dataset was not present, so only baseline was evaluated.")
    return "\n".join(lines) + "\n"


def _format_reasons(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ";".join(str(item) for item in value if str(item).strip())
    return str(value)


def _to_float(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


if __name__ == "__main__":
    main()
