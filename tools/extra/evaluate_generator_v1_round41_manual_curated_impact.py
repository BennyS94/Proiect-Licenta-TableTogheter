from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.generator_v1.candidate_filter import (  # noqa: E402
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (  # noqa: E402
    V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE,
    V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_NUTRITION_PATH,
    V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_PROFILE,
    V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.multi_day_audit import multi_day_meal_rows  # noqa: E402
from src.generator_v1.multi_day_selector import MULTI_DAY_MODE_GLOBAL, generate_multi_day_plan  # noqa: E402
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import build_nutrition_target  # noqa: E402
from tools.extra.round41_manual_curated_common import BASELINE_DIR, fooddb_path  # noqa: E402


PROFILE_PATH = REPO_ROOT / "profiles/member_profile_demo_v1.json"
AUDIT_DIR = REPO_ROOT / "data/recipesdb/audit"
OUT_SUMMARY = AUDIT_DIR / "generator_v1_round41_manual_curated_impact_summary.txt"
OUT_DAYS = AUDIT_DIR / "generator_v1_round41_manual_curated_days.csv"
OUT_MEALS = AUDIT_DIR / "generator_v1_round41_manual_curated_meals.csv"
OUT_REPETITION = AUDIT_DIR / "generator_v1_round41_manual_curated_repetition.csv"
OUT_SELECTED_NEW = AUDIT_DIR / "generator_v1_round41_manual_curated_selected_new_recipes.csv"

SCENARIOS = [
    {
        "scenario": "baseline_manual_batch1",
        "dataset_profile": V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE,
        "recipes_path": BASELINE_DIR / "recipes.csv",
        "ingredients_path": BASELINE_DIR / "recipe_ingredients.csv",
        "nutrition_path": BASELINE_DIR / "recipe_nutrition_cache.csv",
    },
    {
        "scenario": "round41_manual_curated",
        "dataset_profile": V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_PROFILE,
        "recipes_path": V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_RECIPES_PATH,
        "ingredients_path": V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_INGREDIENTS_PATH,
        "nutrition_path": V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_NUTRITION_PATH,
    },
]


def main() -> None:
    rows = []
    day_rows = []
    meal_rows = []
    repetition_rows = []
    selected_new_rows = []
    manual_ids = manual_recipe_ids()
    for scenario in SCENARIOS:
        plan, diagnostics = run_scenario(scenario)
        rows.append(summary_row(scenario, plan, diagnostics, manual_ids))
        day_rows.extend(day_output_rows(scenario, plan, manual_ids))
        meal_rows.extend(meal_output_rows(scenario, plan, manual_ids))
        repetition_rows.extend(repetition_output_rows(scenario, plan))
        selected_new_rows.extend(selected_new_output_rows(scenario, plan, manual_ids))
    pd.DataFrame(day_rows).to_csv(OUT_DAYS, index=False)
    pd.DataFrame(meal_rows).to_csv(OUT_MEALS, index=False)
    pd.DataFrame(repetition_rows).to_csv(OUT_REPETITION, index=False)
    pd.DataFrame(selected_new_rows).to_csv(OUT_SELECTED_NEW, index=False)
    OUT_SUMMARY.write_text(build_summary_text(rows, manual_ids), encoding="utf-8")
    final = next(row for row in rows if row["scenario"] == "round41_manual_curated")
    print("Generator v1 Round41 manual-curated impact evaluation written")
    print(f"summary={OUT_SUMMARY}")
    print(
        "round41="
        f"recipes:{final['recipe_count']} "
        f"valid:{final['valid_day_count']} "
        f"accept:{final['accept_day_count']} "
        f"review:{final['review_day_count']} "
        f"unique:{final['unique_recipe_count']} "
        f"repeated:{final['repeated_recipe_count']} "
        f"loss:{final['multi_day_loss']} "
        f"manual_selected:{final['manual_recipe_selected_count']}"
    )


def run_scenario(scenario: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    started = time.perf_counter()
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=scenario["recipes_path"],
        ingredients_path=scenario["ingredients_path"],
        nutrition_path=scenario["nutrition_path"],
        dataset_profile=scenario["dataset_profile"],
    )
    fooddb = load_fooddb_current(fooddb_path())
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
        config=multi_day_config(),
    )
    diagnostics = {
        "runtime_seconds": round(time.perf_counter() - started, 3),
        "recipe_count": int(len(pool.recipes)),
        "eligible_candidate_count": int(len(pool.eligible_candidates)),
        "slot_candidate_count": int(len(slot_candidates)),
    }
    return plan, diagnostics


def multi_day_config() -> dict[str, Any]:
    return {
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": 3,
        "return_alternatives": True,
        "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
        "no_repeat_policy": "hard",
        "multi_day_speed_mode": "fast",
        "day_candidate_builder": "direct_from_slots",
        "direct_slot_shortlist_size": 12,
        "day_candidate_pool_size_target": 50,
        "day_candidate_pool_max": 150,
        "include_slot_forced_variants": True,
    }


def summary_row(
    scenario: dict[str, Any],
    plan: dict[str, Any],
    diagnostics: dict[str, Any],
    manual_ids: set[str],
) -> dict[str, Any]:
    summary = plan.get("multi_day_summary", {})
    selected_ids = selected_recipe_ids(plan)
    selected_manual = sorted(manual_ids.intersection(selected_ids))
    return {
        "scenario": scenario["scenario"],
        "dataset_profile": scenario["dataset_profile"],
        "recipe_count": diagnostics["recipe_count"],
        "eligible_candidate_count": diagnostics["eligible_candidate_count"],
        "slot_candidate_count": diagnostics["slot_candidate_count"],
        "runtime_seconds": diagnostics["runtime_seconds"],
        "valid_day_count": summary.get("valid_day_count"),
        "accept_day_count": summary.get("accept_day_count"),
        "review_day_count": summary.get("review_day_count"),
        "fallback_day_count": summary.get("fallback_day_count"),
        "unique_recipe_count": summary.get("unique_recipe_count"),
        "repeated_recipe_count": summary.get("repeated_recipe_count"),
        "repeated_recipe_ids": ";".join(summary.get("repeated_recipe_ids", [])),
        "multi_day_loss": plan.get("multi_day_loss"),
        "average_day_loss": summary.get("average_day_loss"),
        "strict_verdict": summary.get("multi_day_classification"),
        "manual_recipe_selected_count": len(selected_manual),
        "manual_recipe_ids_selected": ";".join(selected_manual),
    }


def day_output_rows(
    scenario: dict[str, Any],
    plan: dict[str, Any],
    manual_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        meals = day.get("selected_meals", [])
        manual_selected = [
            meal.get("recipe_id")
            for meal in meals
            if meal.get("recipe_id") in manual_ids
        ]
        totals = day.get("day_totals", {})
        diagnostics = day.get("selector_diagnostics", {})
        rows.append(
            {
                "scenario": scenario["scenario"],
                "day_index": day.get("day_index"),
                "validation_status": day.get("validation_status"),
                "quality_gate_status": day.get("quality_gate_status"),
                "fallback_used": day.get("fallback_used"),
                "total_kcal": totals.get("total_kcal"),
                "total_protein_g": totals.get("total_protein_g"),
                "total_carbs_g": totals.get("total_carbs_g"),
                "total_fat_g": totals.get("total_fat_g"),
                "base_day_loss": diagnostics.get("base_day_loss"),
                "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
                "selected_meals": " | ".join(str(meal.get("display_name")) for meal in meals),
                "manual_recipe_ids_selected": ";".join(str(item) for item in manual_selected),
            }
        )
    return rows


def meal_output_rows(
    scenario: dict[str, Any],
    plan: dict[str, Any],
    manual_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for row in multi_day_meal_rows(plan):
        recipe_id = str(row.get("recipe_id", ""))
        rows.append(
            {
                "scenario": scenario["scenario"],
                "is_round41_manual_recipe": recipe_id in manual_ids,
                **row,
            }
        )
    return rows


def repetition_output_rows(scenario: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
    repeated_ids = plan.get("multi_day_summary", {}).get("repeated_recipe_ids", [])
    if not repeated_ids:
        return [{"scenario": scenario["scenario"], "recipe_id": "", "repeat_count": 0, "slots": ""}]
    rows = []
    for recipe_id in repeated_ids:
        slots = []
        count = 0
        for day in plan.get("days", []):
            for meal in day.get("selected_meals", []):
                if meal.get("recipe_id") == recipe_id:
                    count += 1
                    slots.append(str(meal.get("slot")))
        rows.append(
            {
                "scenario": scenario["scenario"],
                "recipe_id": recipe_id,
                "repeat_count": count,
                "slots": ";".join(slots),
            }
        )
    return rows


def selected_new_output_rows(
    scenario: dict[str, Any],
    plan: dict[str, Any],
    manual_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            if meal.get("recipe_id") in manual_ids:
                rows.append(
                    {
                        "scenario": scenario["scenario"],
                        "day_index": day.get("day_index"),
                        "slot": meal.get("slot"),
                        "recipe_id": meal.get("recipe_id"),
                        "display_name": meal.get("display_name"),
                        "kcal": meal.get("kcal"),
                        "protein_g": meal.get("protein_g"),
                        "carbs_g": meal.get("carbs_g"),
                        "fat_g": meal.get("fat_g"),
                    }
                )
    return rows


def selected_recipe_ids(plan: dict[str, Any]) -> set[str]:
    ids = set()
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            recipe_id = meal.get("recipe_id")
            if recipe_id:
                ids.add(str(recipe_id))
    return ids


def manual_recipe_ids() -> set[str]:
    path = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round41_manual_curated_50.csv"
    if not path.exists():
        return set()
    df = pd.read_csv(path)
    if "recipe_id" not in df.columns:
        return set()
    return set(df["recipe_id"].astype(str))


def build_summary_text(rows: list[dict[str, Any]], manual_ids: set[str]) -> str:
    lines = [
        "Generator v1 Round41 manual-curated impact summary",
        "",
        f"manual_ready_recipe_count={len(manual_ids)}",
    ]
    for row in rows:
        lines.append(
            "- "
            f"{row['scenario']}: recipes={row['recipe_count']}, "
            f"valid={row['valid_day_count']}, "
            f"accept={row['accept_day_count']}, "
            f"review={row['review_day_count']}, "
            f"unique={row['unique_recipe_count']}, "
            f"repeated={row['repeated_recipe_count']}, "
            f"loss={row['multi_day_loss']}, "
            f"runtime_s={row['runtime_seconds']}, "
            f"manual_selected={row['manual_recipe_selected_count']}"
        )
        if row["manual_recipe_ids_selected"]:
            lines.append(f"  manual_recipe_ids_selected={row['manual_recipe_ids_selected']}")
    lines.extend(
        [
            "",
            "Strict assessment:",
            "- Round41 increases data depth and should be judged by ready count and robustness, not only by current best 3-day loss.",
            "- If the multi-day loss changes only slightly, that is acceptable because the baseline plan was already no-repeat and strong.",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    main()
