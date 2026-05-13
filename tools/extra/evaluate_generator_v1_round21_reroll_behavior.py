from __future__ import annotations

import csv
import sys
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
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.day_selector_balanced import select_one_day_plan_balanced
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round21_reroll_behavior_summary.txt"
OUT_RUNS = OUT_DIR / "generator_v1_round21_reroll_behavior_runs.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round21_reroll_behavior_meals.csv"

DATASET_PROFILE = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE
SELECTION_MODE = "balanced_day"
PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"
ALTERNATIVE_COUNT = 3
RECENT_MENU_WINDOW = 2

RUN_COLUMNS = [
    "run_number",
    "run_label",
    "generation_trigger",
    "diversity_mode_used",
    "recent_recipe_count_used",
    "recent_recipe_ids_used",
    "recent_recipe_names_used",
    "selected_recipe_ids",
    "selected_recipe_names",
    "overlap_with_previous_count",
    "overlap_with_previous_names",
    "overlap_with_last2_count",
    "overlap_with_last2_names",
    "changed_vs_previous",
    "changed_vs_run1",
    "validation_status",
    "is_valid_for_checkpoint_1",
    "base_day_loss",
    "adjusted_day_loss",
    "total_kcal",
    "total_protein_g",
    "total_carbs_g",
    "total_fat_g",
    "target_kcal",
    "target_protein_g",
    "target_carbs_g",
    "target_fat_g",
    "meal_realism_flags",
    "verdict",
]

MEAL_COLUMNS = [
    "run_number",
    "run_label",
    "slot",
    "recipe_id",
    "display_name",
    "portion_multiplier",
    "portion_grams_estimated",
    "kcal",
    "protein_g",
    "carbs_g",
    "fat_g",
    "meal_realism_score",
    "meal_realism_penalty",
    "meal_realism_flags",
    "meal_realism_reasons",
    "realism_hard_reject",
    "realism_reject_reason",
    "portion_policy_warnings",
    "total_time_min",
    "effective_time_min_for_scoring",
]


def main() -> None:
    context = build_generation_context()
    previous_plans: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []
    meal_rows: list[dict[str, Any]] = []

    run_specs = [
        {
            "run_number": 1,
            "run_label": "RUN 1 - Generate best",
            "generation_trigger": "best",
            "diversity_mode": "none",
        },
        {
            "run_number": 2,
            "run_label": "RUN 2 - Generate varied using Run 1 as recent",
            "generation_trigger": "varied",
            "diversity_mode": "avoid_recent",
        },
        {
            "run_number": 3,
            "run_label": "RUN 3 - Generate varied using Run 1 + Run 2 as recent",
            "generation_trigger": "varied",
            "diversity_mode": "avoid_recent",
        },
    ]

    for spec in run_specs:
        recent_details = recent_recipe_details(previous_plans)
        recent_ids = {str(row["recipe_id"]) for row in recent_details}
        plan = select_plan(
            context=context,
            diversity_mode=str(spec["diversity_mode"]),
            recent_recipe_ids=recent_ids,
        )
        plan["run_number"] = spec["run_number"]
        plan["run_label"] = spec["run_label"]
        plan["generation_trigger"] = spec["generation_trigger"]
        plan["diversity_mode_used"] = spec["diversity_mode"]
        plan["recent_recipe_ids_used"] = sorted(recent_ids)
        plan["recent_recipe_count_used"] = len(recent_ids)
        plan["recent_recipe_details_used"] = recent_details

        run_rows.append(run_row(plan, previous_plans, context["target"]))
        meal_rows.extend(meal_rows_for_plan(plan))
        previous_plans.insert(0, plan)
        previous_plans = previous_plans[:RECENT_MENU_WINDOW]

    write_csv(OUT_RUNS, run_rows, RUN_COLUMNS)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    OUT_SUMMARY.write_text(build_summary(run_rows, meal_rows), encoding="utf-8")

    print("Generator v1 round21 reroll behavior evaluation written")
    print(f"summary={OUT_SUMMARY}")
    print(f"runs={OUT_RUNS}")
    print(f"meals={OUT_MEALS}")
    for row in run_rows:
        print(
            f"run={row['run_number']} "
            f"recent={row['recent_recipe_count_used']} "
            f"changed_vs_previous={row['changed_vs_previous']} "
            f"base={row['base_day_loss']} "
            f"adjusted={row['adjusted_day_loss']} "
            f"verdict={row['verdict']}"
        )


def build_generation_context() -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=DATASET_PROFILE,
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
    ordered_slots = slot_order(target)
    return {
        "target": target,
        "slot_order": ordered_slots,
        "slot_candidates_by_slot": slot_candidates_by_slot(
            slot_candidates,
            ordered_slots,
        ),
    }


def select_plan(
    context: dict[str, Any],
    diversity_mode: str,
    recent_recipe_ids: set[str],
) -> dict[str, Any]:
    target = context["target"]
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=context["slot_candidates_by_slot"],
        target=target,
        slot_order=context["slot_order"],
        config={
            "return_alternatives": True,
            "alternative_count": ALTERNATIVE_COUNT,
            "diversity_mode": diversity_mode,
            "recent_recipe_ids": sorted(recent_recipe_ids),
            "meal_realism_mode": MEAL_REALISM_MODE,
        },
    )
    plan["target"] = target_to_dict(target)
    plan["validation"] = validate_one_day_plan(plan, target)
    return plan


def run_row(
    plan: dict[str, Any],
    previous_plans: list[dict[str, Any]],
    target: NutritionTarget,
) -> dict[str, Any]:
    selected_ids = selected_recipe_ids(plan)
    selected_names = selected_recipe_names(plan)
    previous_ids = selected_recipe_ids(previous_plans[0]) if previous_plans else set()
    last2_ids = set().union(*(selected_recipe_ids(item) for item in previous_plans))
    overlap_previous = selected_ids & previous_ids
    overlap_last2 = selected_ids & last2_ids
    diagnostics = plan.get("selector_diagnostics", {})
    totals = plan.get("day_totals", {})
    validation = plan.get("validation", {})
    run1_ids = selected_recipe_ids(previous_plans[-1]) if previous_plans else set()
    flags = realism_flags(plan)
    return {
        "run_number": plan["run_number"],
        "run_label": plan["run_label"],
        "generation_trigger": plan["generation_trigger"],
        "diversity_mode_used": plan["diversity_mode_used"],
        "recent_recipe_count_used": plan["recent_recipe_count_used"],
        "recent_recipe_ids_used": serialize(plan["recent_recipe_ids_used"]),
        "recent_recipe_names_used": serialize(
            [row["display_name"] for row in plan["recent_recipe_details_used"]]
        ),
        "selected_recipe_ids": serialize(selected_ids),
        "selected_recipe_names": serialize(selected_names),
        "overlap_with_previous_count": len(overlap_previous),
        "overlap_with_previous_names": serialize(names_for_ids(plan, overlap_previous)),
        "overlap_with_last2_count": len(overlap_last2),
        "overlap_with_last2_names": serialize(names_for_ids(plan, overlap_last2)),
        "changed_vs_previous": not previous_ids or selected_ids != previous_ids,
        "changed_vs_run1": not run1_ids or selected_ids != run1_ids,
        "validation_status": validation.get("validation_status"),
        "is_valid_for_checkpoint_1": validation.get("is_valid_for_checkpoint_1"),
        "base_day_loss": diagnostics.get("base_day_loss"),
        "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
        "total_kcal": totals.get("total_kcal"),
        "total_protein_g": totals.get("total_protein_g"),
        "total_carbs_g": totals.get("total_carbs_g"),
        "total_fat_g": totals.get("total_fat_g"),
        "target_kcal": target.kcal,
        "target_protein_g": target.protein_g,
        "target_carbs_g": target.carbs_g,
        "target_fat_g": target.fat_g,
        "meal_realism_flags": serialize(flags),
        "verdict": verdict_for_plan(plan, overlap_last2),
    }


def meal_rows_for_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for meal in plan.get("selected_meals", []):
        rows.append(
            {
                "run_number": plan["run_number"],
                "run_label": plan["run_label"],
                "slot": meal.get("slot"),
                "recipe_id": meal.get("recipe_id"),
                "display_name": meal.get("display_name"),
                "portion_multiplier": meal.get("portion_multiplier"),
                "portion_grams_estimated": meal.get("portion_grams_estimated"),
                "kcal": meal.get("kcal"),
                "protein_g": meal.get("protein_g"),
                "carbs_g": meal.get("carbs_g"),
                "fat_g": meal.get("fat_g"),
                "meal_realism_score": meal.get("meal_realism_score"),
                "meal_realism_penalty": meal.get("meal_realism_penalty"),
                "meal_realism_flags": serialize(meal.get("meal_realism_flags")),
                "meal_realism_reasons": serialize(meal.get("meal_realism_reasons")),
                "realism_hard_reject": meal.get("realism_hard_reject"),
                "realism_reject_reason": serialize(
                    meal.get("realism_reject_reason")
                ),
                "portion_policy_warnings": serialize(
                    meal.get("portion_policy_warnings")
                ),
                "total_time_min": meal.get("total_time_min"),
                "effective_time_min_for_scoring": meal.get(
                    "effective_time_min_for_scoring"
                ),
            }
        )
    return rows


def build_summary(
    run_rows: list[dict[str, Any]],
    meal_rows: list[dict[str, Any]],
) -> str:
    lines = [
        "Generator v1 round21 Streamlit reroll behavior",
        "",
        "Config:",
        f"- dataset_profile={DATASET_PROFILE}",
        f"- selection_mode={SELECTION_MODE}",
        f"- portion_policy={PORTION_POLICY}",
        f"- meal_realism_mode={MEAL_REALISM_MODE}",
        f"- alternative_count={ALTERNATIVE_COUNT}",
        "",
        "Runs:",
    ]
    for row in run_rows:
        lines.extend(
            [
                f"{row['run_label']}",
                f"- diversity_mode_used={row['diversity_mode_used']}",
                f"- recent_recipe_count_used={row['recent_recipe_count_used']}",
                f"- selected={row['selected_recipe_names']}",
                f"- overlap_with_previous_count={row['overlap_with_previous_count']}",
                f"- overlap_with_last2_count={row['overlap_with_last2_count']}",
                f"- changed_vs_previous={row['changed_vs_previous']}",
                f"- base_day_loss={row['base_day_loss']}",
                f"- adjusted_day_loss={row['adjusted_day_loss']}",
                f"- validation_status={row['validation_status']}",
                f"- verdict={row['verdict']}",
                "",
            ]
        )
    identical_run2 = str(run_rows[1]["changed_vs_run1"]) == "False"
    identical_run3 = str(run_rows[2]["changed_vs_run1"]) == "False"
    if identical_run2 or identical_run3:
        explanation = (
            "At least one varied run repeated Run 1. "
            "This points to either insufficient viable alternatives or an ineffective penalty."
        )
    else:
        explanation = (
            "Run 2 and Run 3 changed versus Run 1, so recent_recipe_ids wiring works."
        )
    lines.extend(
        [
            "Interpretation:",
            f"- {explanation}",
            "- If Streamlit repeats after pressing Generate best with diversity_mode=none, that is expected deterministic behavior.",
            "- Use Generate varied for QA rerolls that should consume session history.",
            f"- meal_rows={len(meal_rows)}",
        ]
    )
    return "\n".join(lines)


def verdict_for_plan(plan: dict[str, Any], overlap_last2: set[str]) -> str:
    severe_flags = {
        "unrealistic_large_portion",
        "breakfast_too_large",
        "snack_too_large",
        "main_too_large",
    }
    flags = set(realism_flags(plan))
    if flags & severe_flags or any(
        bool(meal.get("realism_hard_reject"))
        for meal in plan.get("selected_meals", [])
    ):
        return "bad"
    if flags or overlap_last2:
        return "needs review"
    for meal in plan.get("selected_meals", []):
        slot = str(meal.get("slot", "")).lower()
        kcal = to_float(meal.get("kcal"))
        grams = to_float(meal.get("portion_grams_estimated"))
        if slot == "breakfast" and (kcal > 750 or grams > 425):
            return "needs review"
        if slot == "snack" and (kcal > 350 or grams > 250):
            return "needs review"
    return "human-realistic"


def realism_flags(plan: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    for meal in plan.get("selected_meals", []):
        value = meal.get("meal_realism_flags")
        if isinstance(value, (list, tuple, set)):
            flags.extend(str(item) for item in value if str(item))
        elif value:
            flags.append(str(value))
    return sorted(set(flags))


def names_for_ids(plan: dict[str, Any], recipe_ids: set[str]) -> list[str]:
    names = []
    for meal in plan.get("selected_meals", []):
        if str(meal.get("recipe_id")) in recipe_ids:
            names.append(str(meal.get("display_name")))
    return names


def selected_recipe_ids(plan: dict[str, Any]) -> set[str]:
    return {
        str(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if meal.get("recipe_id")
    }


def selected_recipe_names(plan: dict[str, Any]) -> list[str]:
    return [
        str(meal.get("display_name"))
        for meal in plan.get("selected_meals", [])
        if meal.get("display_name")
    ]


def recent_recipe_details(plans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_recipe_ids: set[str] = set()
    for menu_index, plan in enumerate(plans[:RECENT_MENU_WINDOW], start=1):
        for meal in plan.get("selected_meals", []):
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if not recipe_id or recipe_id in seen_recipe_ids:
                continue
            seen_recipe_ids.add(recipe_id)
            rows.append(
                {
                    "source_menu": menu_index,
                    "run_id": f"round21-{plan.get('run_number')}",
                    "slot": meal.get("slot"),
                    "recipe_id": recipe_id,
                    "display_name": meal.get("display_name"),
                }
            )
    return rows


def slot_order(target: NutritionTarget) -> list[str]:
    preferred_order = ["breakfast", "lunch", "dinner", "snack"]
    known_slots = [slot for slot in preferred_order if slot in target.slot_targets]
    extra_slots = [slot for slot in target.slot_targets if slot not in preferred_order]
    return known_slots + extra_slots


def slot_candidates_by_slot(
    slot_candidates: pd.DataFrame,
    ordered_slots: list[str],
) -> dict[str, pd.DataFrame]:
    return {
        slot: slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        for slot in ordered_slots
    }


def target_to_dict(target: NutritionTarget) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def serialize(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, set):
        return "; ".join(str(item) for item in sorted(value))
    if isinstance(value, (list, tuple)):
        return "; ".join(str(item) for item in value)
    return str(value)


def to_float(value: object) -> float:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return 0.0
    return float(number)


def write_csv(
    path: Path,
    rows: list[dict[str, Any]],
    columns: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


if __name__ == "__main__":
    main()
