from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.generator_v1.candidate_filter import build_household_preference_context, filter_recipe_candidates
from src.generator_v1.data_loader import (
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
from src.generator_v1.multi_day_audit import multi_day_meal_rows
from src.generator_v1.multi_day_selector import MULTI_DAY_MODE_GLOBAL, generate_multi_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import build_nutrition_target
from tools.extra.round41_manual_curated_common import fooddb_path


PROFILE_PATH = REPO_ROOT / "profiles/member_profile_demo_v1.json"
AUDIT_DIR = REPO_ROOT / "data/recipesdb/audit"
OUT_SUMMARY = AUDIT_DIR / "generator_v1_round42_dataset_expanded_impact_summary.txt"
OUT_DAYS = AUDIT_DIR / "generator_v1_round42_dataset_expanded_days.csv"
OUT_MEALS = AUDIT_DIR / "generator_v1_round42_dataset_expanded_meals.csv"
OUT_REPETITION = AUDIT_DIR / "generator_v1_round42_dataset_expanded_repetition.csv"
OUT_SELECTED_NEW = AUDIT_DIR / "generator_v1_round42_dataset_expanded_selected_new_recipes.csv"
ROUND42_SELECTED = REPO_ROOT / "data/recipesdb/draft/recipes_v1_2_round42_dataset_curated_selected.csv"
ROUND42_READY_AUDIT = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_dataset_generator_ready_audit.csv"
ROUND42_QUEUE_ADDITIONS = REPO_ROOT / "data/recipesdb/audit/recipes_v1_2_round42_manual_repair_queue_additions.csv"

SCENARIOS = [
    {
        "scenario": "round41_manual_curated",
        "dataset_profile": V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_PROFILE,
        "recipes_path": V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_RECIPES_PATH,
        "ingredients_path": V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_INGREDIENTS_PATH,
        "nutrition_path": V1_2_GENERATOR_READY_ROUND41_MANUAL_CURATED_NUTRITION_PATH,
    },
    {
        "scenario": "round42_dataset_expanded",
        "dataset_profile": V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_PROFILE,
        "recipes_path": V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_RECIPES_PATH,
        "ingredients_path": V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_INGREDIENTS_PATH,
        "nutrition_path": V1_2_GENERATOR_READY_ROUND42_DATASET_EXPANDED_NUTRITION_PATH,
    },
]


def main() -> None:
    rows = []
    day_rows = []
    meal_rows = []
    repetition_rows = []
    selected_new_rows = []
    round42_ids = round42_recipe_ids()
    for scenario in SCENARIOS:
        plan, diagnostics = run_scenario(scenario)
        rows.append(summary_row(scenario, plan, diagnostics, round42_ids))
        day_rows.extend(day_output_rows(scenario, plan, round42_ids))
        meal_rows.extend(meal_output_rows(scenario, plan, round42_ids))
        repetition_rows.extend(repetition_output_rows(scenario, plan))
        selected_new_rows.extend(selected_new_output_rows(scenario, plan, round42_ids))
    pd.DataFrame(day_rows).to_csv(OUT_DAYS, index=False)
    pd.DataFrame(meal_rows).to_csv(OUT_MEALS, index=False)
    pd.DataFrame(repetition_rows).to_csv(OUT_REPETITION, index=False)
    pd.DataFrame(selected_new_rows).to_csv(OUT_SELECTED_NEW, index=False)
    OUT_SUMMARY.write_text(build_summary_text(rows), encoding="utf-8")
    final = next(row for row in rows if row["scenario"] == "round42_dataset_expanded")
    print("Generator v1 Round42 dataset-expanded impact evaluation written")
    print(f"summary={OUT_SUMMARY}")
    print(
        "round42="
        f"recipes:{final['recipe_count']} "
        f"valid:{final['valid_day_count']} "
        f"accept:{final['accept_day_count']} "
        f"review:{final['review_day_count']} "
        f"unique:{final['unique_recipe_count']} "
        f"repeated:{final['repeated_recipe_count']} "
        f"loss:{final['multi_day_loss']} "
        f"new_selected:{final['round42_recipe_selected_count']}"
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
    round42_ids: set[str],
) -> dict[str, Any]:
    summary = plan.get("multi_day_summary", {})
    selected_ids = selected_recipe_ids(plan)
    selected_new = sorted(round42_ids.intersection(selected_ids))
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
        "round42_recipe_selected_count": len(selected_new),
        "round42_recipe_ids_selected": ";".join(selected_new),
    }


def day_output_rows(
    scenario: dict[str, Any],
    plan: dict[str, Any],
    round42_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        meals = day.get("selected_meals", [])
        selected_new = [meal.get("recipe_id") for meal in meals if meal.get("recipe_id") in round42_ids]
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
                "round42_recipe_ids_selected": ";".join(str(item) for item in selected_new),
            }
        )
    return rows


def meal_output_rows(
    scenario: dict[str, Any],
    plan: dict[str, Any],
    round42_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for row in multi_day_meal_rows(plan):
        recipe_id = str(row.get("recipe_id", ""))
        rows.append({"scenario": scenario["scenario"], "is_round42_recipe": recipe_id in round42_ids, **row})
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
        rows.append({"scenario": scenario["scenario"], "recipe_id": recipe_id, "repeat_count": count, "slots": ";".join(slots)})
    return rows


def selected_new_output_rows(
    scenario: dict[str, Any],
    plan: dict[str, Any],
    round42_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            recipe_id = str(meal.get("recipe_id", ""))
            if recipe_id not in round42_ids:
                continue
            rows.append(
                {
                    "scenario": scenario["scenario"],
                    "day_index": day.get("day_index"),
                    "slot": meal.get("slot"),
                    "recipe_id": recipe_id,
                    "display_name": meal.get("display_name"),
                    "kcal": meal.get("kcal"),
                    "protein_g": meal.get("protein_g"),
                    "carbs_g": meal.get("carbs_g"),
                    "fat_g": meal.get("fat_g"),
                }
            )
    return rows


def round42_recipe_ids() -> set[str]:
    if not ROUND42_SELECTED.exists():
        return set()
    df = pd.read_csv(ROUND42_SELECTED)
    return set(str(value) for value in df.get("recipe_id_candidate", pd.Series(dtype=str)).dropna())


def selected_recipe_ids(plan: dict[str, Any]) -> set[str]:
    ids = set()
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if recipe_id:
                ids.add(recipe_id)
    return ids


def build_summary_text(rows: list[dict[str, Any]]) -> str:
    selected_count = count_rows(ROUND42_SELECTED)
    ready_count = count_ready(ROUND42_READY_AUDIT, "generator_ready_candidate")
    strong_count = count_ready(ROUND42_READY_AUDIT, "strong_generator_ready")
    repair_count = count_rows(ROUND42_QUEUE_ADDITIONS)
    lines = [
        "Generator v1 Round42 dataset-expanded impact summary",
        "",
        f"selected_dataset_candidate_count={selected_count}",
        f"generator_ready_count={ready_count}",
        f"strong_generator_ready_count={strong_count}",
        f"manual_repair_queue_additions={repair_count}",
    ]
    for row in rows:
        lines.append(
            (
                f"- {row['scenario']}: recipes={row['recipe_count']}, "
                f"valid={row['valid_day_count']}, accept={row['accept_day_count']}, "
                f"review={row['review_day_count']}, unique={row['unique_recipe_count']}, "
                f"repeated={row['repeated_recipe_count']}, loss={row['multi_day_loss']}, "
                f"runtime_s={row['runtime_seconds']}, round42_selected={row['round42_recipe_selected_count']}"
            )
        )
        if row["round42_recipe_ids_selected"]:
            lines.append(f"  round42_recipe_ids_selected={row['round42_recipe_ids_selected']}")
    lines.extend(
        [
            "",
            "Strict assessment:",
            "- Round42 should be judged mostly by ready yield and repair queue signal, not by forcing a lower 3-day loss.",
            "- If the current best no-repeat plan changes little, that is acceptable because Round41 was already strong.",
        ]
    )
    return "\n".join(lines) + "\n"


def count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    return int(len(pd.read_csv(path)))


def count_ready(path: Path, column: str) -> int:
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    if column not in df.columns:
        return 0
    return int((df[column].astype(str).str.lower() == "true").sum())


if __name__ == "__main__":
    main()
