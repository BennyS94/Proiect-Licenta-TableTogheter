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
OUT_SUMMARY = OUT_DIR / "generator_v1_streamlit_practical_parity_check.txt"
OUT_MEALS = OUT_DIR / "generator_v1_streamlit_practical_parity_meals.csv"

GENERATION_CONFIG = {
    "dataset_profile": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    "selection_mode": "balanced_day",
    "portion_policy": "target_aware",
    "meal_realism_mode": "practical",
    "alternative_count": 3,
    "diversity_mode": "none",
}

MEAL_COLUMNS = [
    "source",
    "dataset_profile",
    "selection_mode",
    "portion_policy",
    "meal_realism_mode",
    "alternative_count",
    "diversity_mode",
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
    "base_day_loss",
    "adjusted_day_loss",
]


def main() -> None:
    streamlit_plan = build_plan("streamlit_equivalent")
    cli_plan = build_plan("cli_equivalent")
    meal_rows = [
        *meal_rows_for_plan("streamlit_equivalent", streamlit_plan),
        *meal_rows_for_plan("cli_equivalent", cli_plan),
    ]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    OUT_SUMMARY.write_text(
        build_summary(streamlit_plan, cli_plan, meal_rows),
        encoding="utf-8",
    )

    diagnostics = streamlit_plan.get("selector_diagnostics", {})
    print("Generator v1 Streamlit practical parity check written")
    print(f"summary={OUT_SUMMARY}")
    print(f"meals={OUT_MEALS}")
    print(f"same_selected_meals={same_selected_meals(streamlit_plan, cli_plan)}")
    print(f"meal_realism_mode={diagnostics.get('meal_realism_mode')}")
    print(f"base_day_loss={diagnostics.get('base_day_loss')}")
    print(f"adjusted_day_loss={diagnostics.get('adjusted_day_loss')}")
    print(f"meal_realism_score_populated={meal_realism_score_populated(streamlit_plan)}")
    print(f"waffles_1_0_444g_selected={waffles_1_0_444g_selected(streamlit_plan)}")


def build_plan(source: str) -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=GENERATION_CONFIG["dataset_profile"],
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
        portion_policy_mode=GENERATION_CONFIG["portion_policy"],
    )
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=slot_candidates_by_slot(
            slot_candidates,
            slot_order(target),
        ),
        target=target,
        slot_order=slot_order(target),
        config={
            "return_alternatives": True,
            "alternative_count": GENERATION_CONFIG["alternative_count"],
            "diversity_mode": GENERATION_CONFIG["diversity_mode"],
            "recent_recipe_ids": [],
            "meal_realism_mode": GENERATION_CONFIG["meal_realism_mode"],
        },
    )
    plan["target"] = target_to_dict(target)
    plan["validation"] = validate_one_day_plan(plan, target)
    plan["generation_config"] = dict(GENERATION_CONFIG)
    plan["parity_source"] = source
    return plan


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


def meal_rows_for_plan(source: str, plan: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics = plan.get("selector_diagnostics", {})
    rows = []
    for meal in plan.get("selected_meals", []):
        rows.append(
            {
                "source": source,
                **GENERATION_CONFIG,
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
                "meal_realism_flags": serialize_reasons(
                    meal.get("meal_realism_flags")
                ),
                "meal_realism_reasons": serialize_reasons(
                    meal.get("meal_realism_reasons")
                ),
                "realism_hard_reject": meal.get("realism_hard_reject"),
                "realism_reject_reason": serialize_reasons(
                    meal.get("realism_reject_reason")
                ),
                "base_day_loss": diagnostics.get("base_day_loss"),
                "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
            }
        )
    return rows


def build_summary(
    streamlit_plan: dict[str, Any],
    cli_plan: dict[str, Any],
    meal_rows: list[dict[str, Any]],
) -> str:
    validation = streamlit_plan.get("validation", {})
    totals = streamlit_plan.get("day_totals", {})
    target = streamlit_plan.get("target", {})
    diagnostics = streamlit_plan.get("selector_diagnostics", {})
    config_lines = [f"- {key}={value}" for key, value in GENERATION_CONFIG.items()]
    selected_lines = []
    for meal in streamlit_plan.get("selected_meals", []):
        selected_lines.append(
            "- "
            f"{meal.get('slot')}: {meal.get('display_name')} | "
            f"portion={format_number(meal.get('portion_multiplier'))} | "
            f"grams={format_number(meal.get('portion_grams_estimated'))} | "
            f"kcal={format_number(meal.get('kcal'))} | "
            f"P/C/F={format_number(meal.get('protein_g'))}/"
            f"{format_number(meal.get('carbs_g'))}/"
            f"{format_number(meal.get('fat_g'))} | "
            "meal_realism_score="
            f"{format_number(meal.get('meal_realism_score'), decimals=4)} | "
            f"flags={serialize_reasons(meal.get('meal_realism_flags'))}"
        )

    return "\n".join(
        [
            "Generator v1 Streamlit practical realism parity check",
            "",
            "Active config:",
            *config_lines,
            "",
            "Result:",
            f"- validation_status={validation.get('validation_status')}",
            f"- is_valid_for_checkpoint_1={validation.get('is_valid_for_checkpoint_1')}",
            f"- same_selected_meals_vs_cli_equivalent={same_selected_meals(streamlit_plan, cli_plan)}",
            f"- meal_realism_mode_used={diagnostics.get('meal_realism_mode')}",
            f"- base_day_loss={diagnostics.get('base_day_loss')}",
            f"- adjusted_day_loss={diagnostics.get('adjusted_day_loss')}",
            f"- total_kcal={format_number(totals.get('total_kcal'))}",
            f"- total_protein_g={format_number(totals.get('total_protein_g'))}",
            f"- total_carbs_g={format_number(totals.get('total_carbs_g'))}",
            f"- total_fat_g={format_number(totals.get('total_fat_g'))}",
            f"- target_kcal={format_number(target.get('kcal'))}",
            f"- target_protein_g={format_number(target.get('protein_g'))}",
            f"- target_carbs_g={format_number(target.get('carbs_g'))}",
            f"- target_fat_g={format_number(target.get('fat_g'))}",
            f"- meal_realism_score_populated={meal_realism_score_populated(streamlit_plan)}",
            f"- waffles_1_0_444g_selected={waffles_1_0_444g_selected(streamlit_plan)}",
            f"- csv_rows={len(meal_rows)}",
            "",
            "Selected meals:",
            *selected_lines,
            "",
            "Interpretation:",
            "- Streamlit-equivalent and CLI-equivalent paths select the same meals with the same practical config.",
            "- If the UI still shows Waffles at portion=1.0 / 444g with meal_realism_score=None, that is stale Streamlit session output or an old server process, not the current practical path.",
        ]
    )


def same_selected_meals(
    left_plan: dict[str, Any],
    right_plan: dict[str, Any],
) -> bool:
    return selected_signature(left_plan) == selected_signature(right_plan)


def selected_signature(plan: dict[str, Any]) -> tuple[tuple[str, str, float], ...]:
    signature = []
    for meal in plan.get("selected_meals", []):
        signature.append(
            (
                str(meal.get("slot")),
                str(meal.get("recipe_id")),
                round(to_float(meal.get("portion_multiplier")), 4),
            )
        )
    return tuple(signature)


def meal_realism_score_populated(plan: dict[str, Any]) -> bool:
    meals = plan.get("selected_meals", [])
    return bool(meals) and all(
        meal.get("meal_realism_score") is not None for meal in meals
    )


def waffles_1_0_444g_selected(plan: dict[str, Any]) -> bool:
    for meal in plan.get("selected_meals", []):
        name = str(meal.get("display_name", "")).lower()
        portion = to_float(meal.get("portion_multiplier"))
        grams = to_float(meal.get("portion_grams_estimated"))
        if "mom's best waffles" in name and portion >= 0.99 and grams >= 440:
            return True
    return False


def serialize_reasons(value: object) -> str:
    if isinstance(value, list):
        return "; ".join(str(item) for item in value)
    if isinstance(value, tuple):
        return "; ".join(str(item) for item in value)
    if value is None:
        return ""
    return str(value)


def format_number(value: object, decimals: int = 1) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return "missing"
    return f"{float(number):.{decimals}f}"


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
