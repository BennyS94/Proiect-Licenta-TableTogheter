from __future__ import annotations

import csv
import json
import sys
from collections import Counter
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

try:
    from evaluate_generator_v1_round17_portion_policy import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        clean_text,
        classify_macro_gaps,
        macro_ratios,
        round_number,
        scenario_overrides,
        serialize_reason,
        slot_candidates_by_slot,
        slot_order,
        target_to_dict,
        to_float,
    )
except ImportError:
    from tools.extra.evaluate_generator_v1_round17_portion_policy import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        clean_text,
        classify_macro_gaps,
        macro_ratios,
        round_number,
        scenario_overrides,
        serialize_reason,
        slot_candidates_by_slot,
        slot_order,
        target_to_dict,
        to_float,
    )


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round19_meal_realism_summary.txt"
OUT_PLANS = OUT_DIR / "generator_v1_round19_meal_realism_plans.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round19_meal_realism_meals.csv"
OUT_FLAGS = OUT_DIR / "generator_v1_round19_meal_realism_flags.csv"
OUT_HISTORY = OUT_DIR / "generator_v1_round19_streamlit_history_check.txt"

MEAL_REALISM_MODES = ["off", "audit", "soft"]
PORTION_POLICY = "target_aware"
SELECTION_MODE = "balanced_day"
ALTERNATIVE_COUNT = 3

PLAN_COLUMNS = [
    "scenario_id",
    "meal_realism_mode",
    "validation_status",
    "is_valid_for_checkpoint_1",
    "target_kcal",
    "target_protein_g",
    "target_carbs_g",
    "target_fat_g",
    "selected_total_kcal",
    "selected_total_protein_g",
    "selected_total_carbs_g",
    "selected_total_fat_g",
    "kcal_ratio",
    "protein_ratio",
    "carbs_ratio",
    "fat_ratio",
    "good_macro_fit",
    "carb_deficient",
    "protein_heavy",
    "fat_heavy",
    "day_loss",
    "base_day_loss",
    "adjusted_day_loss",
    "meal_realism_total_penalty",
    "meal_realism_applied_penalty",
    "effective_time_min_sum",
    "total_time_min_sum",
    "selected_recipes",
    "selected_recipe_names",
    "selected_portion_grams",
    "large_portion_flag_count",
    "mono_macro_flag_count",
    "snack_flag_count",
    "breakfast_low_protein_flag_count",
    "total_realism_flag_count",
    "evaluated_combination_count",
    "alternative_count_returned",
    "selector_warnings",
]

MEAL_COLUMNS = [
    "scenario_id",
    "meal_realism_mode",
    "slot",
    "recipe_id",
    "display_name",
    "recipe_kind",
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
    "portion_policy_warnings",
    "total_time_min",
    "effective_time_min_for_scoring",
]

FLAG_COLUMNS = [
    "scenario_id",
    "meal_realism_mode",
    "slot",
    "recipe_id",
    "display_name",
    "flag",
    "flag_group",
    "portion_grams_estimated",
    "kcal",
    "protein_g",
    "carbs_g",
    "fat_g",
]


def main() -> None:
    base_profile = load_member_profile(BASE_PROFILE_PATH)
    fooddb = load_fooddb_current()
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    )

    plan_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []
    flag_rows: list[dict[str, object]] = []

    for overrides in scenario_overrides():
        scenario_id = clean_text(overrides["scenario_id"])
        profile = build_scenario_profile(base_profile, overrides)
        scenario_result = run_scenario(
            scenario_id=scenario_id,
            profile=profile,
            pool=pool,
            fooddb=fooddb,
        )
        plan_rows.extend(scenario_result["plan_rows"])
        meal_rows.extend(scenario_result["meal_rows"])
        flag_rows.extend(scenario_result["flag_rows"])

    write_csv(OUT_PLANS, plan_rows, PLAN_COLUMNS)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    write_csv(OUT_FLAGS, flag_rows, FLAG_COLUMNS)
    OUT_SUMMARY.write_text(build_summary(plan_rows, flag_rows), encoding="utf-8")
    OUT_HISTORY.write_text(build_history_check(), encoding="utf-8")

    print("Generator v1 round19 meal realism evaluation written")
    print(f"plan_rows={len(plan_rows)}")
    print(f"meal_rows={len(meal_rows)}")
    print(f"flag_rows={len(flag_rows)}")
    for mode in MEAL_REALISM_MODES:
        print(
            f"{mode}: valid={count_matching(plan_rows, mode, 'validation_status', 'valid')}, "
            f"good_macro_fit={count_matching(plan_rows, mode, 'good_macro_fit', 'True')}, "
            f"avg_adjusted_day_loss={round_number(average_metric(plan_rows, mode, 'adjusted_day_loss'))}, "
            f"flags={sum_metric(plan_rows, mode, 'total_realism_flag_count')}"
        )
    print(f"written_summary={OUT_SUMMARY}")


def run_scenario(
    scenario_id: str,
    profile: dict[str, Any],
    pool: object,
    fooddb: pd.DataFrame,
) -> dict[str, list[dict[str, object]]]:
    target = build_nutrition_target(profile)
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slots = slot_order(target)
    plan_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []
    flag_rows: list[dict[str, object]] = []

    for mode in MEAL_REALISM_MODES:
        slot_candidates = build_slot_candidates(
            target=target,
            filtered_candidates=filtered_candidates,
            time_sensitivity=preference_context.time_sensitivity,
            ingredients=pool.ingredients,
            fooddb=fooddb,
            portion_policy_mode=PORTION_POLICY,
        )
        plan = select_one_day_plan_balanced(
            slot_candidates_by_slot=slot_candidates_by_slot(slot_candidates, slots),
            target=target,
            slot_order=slots,
            config={
                "return_alternatives": True,
                "alternative_count": ALTERNATIVE_COUNT,
                "diversity_mode": "none",
                "meal_realism_mode": mode,
            },
        )
        plan["target"] = target_to_dict(target)
        plan["validation"] = validate_one_day_plan(plan, target)
        plan_rows.append(build_plan_row(scenario_id, mode, target, plan))
        meal_rows.extend(build_meal_rows(scenario_id, mode, plan))
        flag_rows.extend(build_flag_rows(scenario_id, mode, plan))

    return {
        "plan_rows": plan_rows,
        "meal_rows": meal_rows,
        "flag_rows": flag_rows,
    }


def build_plan_row(
    scenario_id: str,
    mode: str,
    target: NutritionTarget,
    plan: dict[str, object],
) -> dict[str, object]:
    totals = plan.get("day_totals", {})
    validation = plan.get("validation", {})
    diagnostics = plan.get("selector_diagnostics", {})
    ratios = macro_ratios(totals, target)
    classes = classify_macro_gaps(ratios, clean_text(validation.get("validation_status")))
    flag_counts = flag_counter(plan)
    return {
        "scenario_id": scenario_id,
        "meal_realism_mode": mode,
        "validation_status": clean_text(validation.get("validation_status")),
        "is_valid_for_checkpoint_1": str(bool(validation.get("is_valid_for_checkpoint_1", False))),
        "target_kcal": round_number(target.kcal),
        "target_protein_g": round_number(target.protein_g),
        "target_carbs_g": round_number(target.carbs_g),
        "target_fat_g": round_number(target.fat_g),
        "selected_total_kcal": round_number(totals.get("total_kcal")),
        "selected_total_protein_g": round_number(totals.get("total_protein_g")),
        "selected_total_carbs_g": round_number(totals.get("total_carbs_g")),
        "selected_total_fat_g": round_number(totals.get("total_fat_g")),
        "kcal_ratio": round_number(ratios["kcal_ratio"]),
        "protein_ratio": round_number(ratios["protein_ratio"]),
        "carbs_ratio": round_number(ratios["carbs_ratio"]),
        "fat_ratio": round_number(ratios["fat_ratio"]),
        "good_macro_fit": str("good_fit" in classes),
        "carb_deficient": str("carb_deficient" in classes),
        "protein_heavy": str("protein_heavy" in classes),
        "fat_heavy": str("fat_heavy" in classes),
        "day_loss": round_number(_diag(diagnostics, "base_day_loss")),
        "base_day_loss": round_number(_diag(diagnostics, "base_day_loss")),
        "adjusted_day_loss": round_number(_diag(diagnostics, "adjusted_day_loss")),
        "meal_realism_total_penalty": round_number(_diag(diagnostics, "meal_realism_total_penalty")),
        "meal_realism_applied_penalty": round_number(_diag(diagnostics, "meal_realism_applied_penalty")),
        "effective_time_min_sum": round_number(totals.get("effective_time_min_sum")),
        "total_time_min_sum": round_number(totals.get("total_time_min_sum")),
        "selected_recipes": selected_recipes_text(plan),
        "selected_recipe_names": selected_recipe_names_text(plan),
        "selected_portion_grams": selected_portion_grams_text(plan),
        "large_portion_flag_count": str(flag_counts["large_portion"]),
        "mono_macro_flag_count": str(flag_counts["mono_macro"]),
        "snack_flag_count": str(flag_counts["snack"]),
        "breakfast_low_protein_flag_count": str(flag_counts["breakfast_low_protein"]),
        "total_realism_flag_count": str(flag_counts["total"]),
        "evaluated_combination_count": clean_text(_diag(diagnostics, "evaluated_combination_count")),
        "alternative_count_returned": clean_text(_diag(diagnostics, "alternative_count_returned")),
        "selector_warnings": "|".join(
            clean_text(item)
            for item in (
                diagnostics.get("selector_warnings", [])
                if isinstance(diagnostics, dict)
                else []
            )
            if clean_text(item)
        ),
    }


def build_meal_rows(
    scenario_id: str,
    mode: str,
    plan: dict[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for meal in plan.get("selected_meals", []):
        rows.append(
            {
                "scenario_id": scenario_id,
                "meal_realism_mode": mode,
                "slot": clean_text(meal.get("slot")),
                "recipe_id": clean_text(meal.get("recipe_id")),
                "display_name": clean_text(meal.get("display_name")),
                "recipe_kind": clean_text(meal.get("recipe_kind")),
                "portion_multiplier": round_number(meal.get("portion_multiplier")),
                "portion_grams_estimated": round_number(meal.get("portion_grams_estimated")),
                "kcal": round_number(meal.get("kcal")),
                "protein_g": round_number(meal.get("protein_g")),
                "carbs_g": round_number(meal.get("carbs_g")),
                "fat_g": round_number(meal.get("fat_g")),
                "meal_realism_score": round_number(meal.get("meal_realism_score")),
                "meal_realism_penalty": round_number(meal.get("meal_realism_penalty")),
                "meal_realism_flags": serialize_reason(meal.get("meal_realism_flags")),
                "meal_realism_reasons": serialize_reason(meal.get("meal_realism_reasons")),
                "portion_policy_warnings": serialize_reason(meal.get("portion_policy_warnings")),
                "total_time_min": round_number(meal.get("total_time_min")),
                "effective_time_min_for_scoring": round_number(meal.get("effective_time_min_for_scoring")),
            }
        )
    return rows


def build_flag_rows(
    scenario_id: str,
    mode: str,
    plan: dict[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for meal in plan.get("selected_meals", []):
        for flag in reason_items(meal.get("meal_realism_flags")):
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "meal_realism_mode": mode,
                    "slot": clean_text(meal.get("slot")),
                    "recipe_id": clean_text(meal.get("recipe_id")),
                    "display_name": clean_text(meal.get("display_name")),
                    "flag": flag,
                    "flag_group": flag_group(flag),
                    "portion_grams_estimated": round_number(meal.get("portion_grams_estimated")),
                    "kcal": round_number(meal.get("kcal")),
                    "protein_g": round_number(meal.get("protein_g")),
                    "carbs_g": round_number(meal.get("carbs_g")),
                    "fat_g": round_number(meal.get("fat_g")),
                }
            )
    return rows


def build_summary(
    plan_rows: list[dict[str, object]],
    flag_rows: list[dict[str, object]],
) -> str:
    scenario_count = len({clean_text(row.get("scenario_id")) for row in plan_rows})
    mode_summaries = {mode: mode_summary(plan_rows, flag_rows, mode) for mode in MEAL_REALISM_MODES}
    plan_changes = compare_plan_changes(plan_rows)
    soft = mode_summaries["soft"]
    off = mode_summaries["off"]
    soft_macro_damage = soft["good_macro_fit"] < off["good_macro_fit"] or soft["valid"] < off["valid"]
    soft_reduces_flags = soft["total_flags"] < off["total_flags"]
    if soft_macro_damage:
        recommendation = "soft_not_recommended_macro_regression"
    elif soft_reduces_flags:
        recommendation = "soft_useful_for_v1_1_testing"
    else:
        recommendation = "audit_recommended_soft_does_not_improve_rank1"

    lines = [
        "Generator v1 round19 meal realism evaluation",
        "=" * 48,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        f"selection_mode: {SELECTION_MODE}",
        f"portion_policy: {PORTION_POLICY}",
        f"alternative_count: {ALTERNATIVE_COUNT}",
        f"scenario_count: {scenario_count}",
        "",
        "Mode results:",
    ]
    for mode in MEAL_REALISM_MODES:
        summary = mode_summaries[mode]
        lines.append(
            "- "
            + mode
            + " | valid="
            + str(summary["valid"])
            + " | good_macro_fit="
            + str(summary["good_macro_fit"])
            + " | avg_day_loss="
            + round_number(summary["avg_day_loss"])
            + " | avg_adjusted_day_loss="
            + round_number(summary["avg_adjusted_day_loss"])
            + " | large_portion_flags="
            + str(summary["large_flags"])
            + " | mono_macro_flags="
            + str(summary["mono_flags"])
            + " | snack_flags="
            + str(summary["snack_flags"])
            + " | breakfast_low_protein_flags="
            + str(summary["breakfast_low_protein_flags"])
            + " | total_flags="
            + str(summary["total_flags"])
        )

    lines.extend(
        [
            "",
            "Plan changes vs off:",
            f"- audit_changed_scenarios: {plan_changes['audit_changed']}",
            f"- soft_changed_scenarios: {plan_changes['soft_changed']}",
            "",
            "Interpretation:",
            f"- recommendation: {recommendation}",
            "- audit mode is safer for QA because it exposes realism issues without changing macro selection.",
            "- soft mode is useful only if it reduces odd meals without reducing valid/good macro scenarios.",
            "- validation_status valid is not enough; large portions and mono-macro meals still need human review.",
            "",
            "Output files:",
            f"- {OUT_PLANS}",
            f"- {OUT_MEALS}",
            f"- {OUT_FLAGS}",
            f"- {OUT_HISTORY}",
        ]
    )
    return "\n".join(lines) + "\n"


def build_history_check() -> str:
    return "\n".join(
        [
            "Generator v1 round19 Streamlit history check",
            "=" * 49,
            "",
            "Implementation status:",
            "- generated_menus is initialized safely and preserved across normal reruns/widget changes.",
            "- latest_menu is stored separately.",
            "- feedback_events is initialized safely.",
            "- recent_recipe_ids is derived from the last 2 stored menus after generation.",
            "- avoid_recent receives recent_recipe_ids from stored Streamlit history.",
            "",
            "Manual QA expected result:",
            "1. Generate once: generated_menus length becomes 1 and Latest menu is visible.",
            "2. Generate again: generated_menus length becomes 2 and Previous menu remains visible.",
            "3. Switch diversity_mode to avoid_recent: recent recipe ids from stored history are shown.",
            "4. Generate with avoid_recent: selector receives previous-menu recipe ids.",
            "5. Change widgets without Generate: generated_menus should not reset.",
            "",
            "Automated limitation:",
            "- This is a text smoke check; Streamlit browser state was not automated here.",
            "",
        ]
    )


def mode_summary(
    plan_rows: list[dict[str, object]],
    flag_rows: list[dict[str, object]],
    mode: str,
) -> dict[str, object]:
    return {
        "valid": count_matching(plan_rows, mode, "validation_status", "valid"),
        "good_macro_fit": count_matching(plan_rows, mode, "good_macro_fit", "True"),
        "avg_day_loss": average_metric(plan_rows, mode, "day_loss"),
        "avg_adjusted_day_loss": average_metric(plan_rows, mode, "adjusted_day_loss"),
        "large_flags": count_flags(flag_rows, mode, "large_portion"),
        "mono_flags": count_flags(flag_rows, mode, "mono_macro"),
        "snack_flags": count_flags(flag_rows, mode, "snack"),
        "breakfast_low_protein_flags": count_flags(flag_rows, mode, "breakfast_low_protein"),
        "total_flags": sum_metric(plan_rows, mode, "total_realism_flag_count"),
    }


def compare_plan_changes(plan_rows: list[dict[str, object]]) -> dict[str, int]:
    by_scenario_mode = {
        (clean_text(row.get("scenario_id")), clean_text(row.get("meal_realism_mode"))): clean_text(row.get("selected_recipes"))
        for row in plan_rows
    }
    scenario_ids = sorted({scenario_id for scenario_id, _ in by_scenario_mode})
    audit_changed = 0
    soft_changed = 0
    for scenario_id in scenario_ids:
        base = by_scenario_mode.get((scenario_id, "off"), "")
        if by_scenario_mode.get((scenario_id, "audit"), "") != base:
            audit_changed += 1
        if by_scenario_mode.get((scenario_id, "soft"), "") != base:
            soft_changed += 1
    return {"audit_changed": audit_changed, "soft_changed": soft_changed}


def flag_counter(plan: dict[str, object]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for meal in plan.get("selected_meals", []):
        for flag in reason_items(meal.get("meal_realism_flags")):
            counter["total"] += 1
            counter[flag_group(flag)] += 1
    return counter


def flag_group(flag: str) -> str:
    if flag in {"borderline_large_portion", "unrealistic_large_portion", "breakfast_too_large"}:
        return "large_portion"
    if flag in {"mostly_carb_meal", "mostly_protein_meal", "low_carb_main", "low_protein_main"}:
        return "mono_macro"
    if flag in {"snack_too_large", "snack_too_meal_like"}:
        return "snack"
    if flag == "breakfast_low_protein":
        return "breakfast_low_protein"
    return "other"


def selected_recipes_text(plan: dict[str, object]) -> str:
    return "|".join(
        clean_text(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if clean_text(meal.get("recipe_id"))
    )


def selected_recipe_names_text(plan: dict[str, object]) -> str:
    return "|".join(
        clean_text(meal.get("display_name"))
        for meal in plan.get("selected_meals", [])
        if clean_text(meal.get("display_name"))
    )


def selected_portion_grams_text(plan: dict[str, object]) -> str:
    return "|".join(
        round_number(meal.get("portion_grams_estimated"))
        for meal in plan.get("selected_meals", [])
    )


def reason_items(value: object) -> list[str]:
    if isinstance(value, list):
        return [clean_text(item) for item in value if clean_text(item)]
    if isinstance(value, tuple):
        return [clean_text(item) for item in value if clean_text(item)]
    text = clean_text(value)
    if not text:
        return []
    if "|" in text:
        return [item.strip() for item in text.split("|") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text]


def count_matching(
    rows: list[dict[str, object]],
    mode: str,
    field: str,
    expected: str,
) -> int:
    return sum(
        1
        for row in rows
        if clean_text(row.get("meal_realism_mode")) == mode
        and clean_text(row.get(field)) == expected
    )


def count_flags(
    rows: list[dict[str, object]],
    mode: str,
    group: str,
) -> int:
    return sum(
        1
        for row in rows
        if clean_text(row.get("meal_realism_mode")) == mode
        and clean_text(row.get("flag_group")) == group
    )


def average_metric(rows: list[dict[str, object]], mode: str, field: str) -> float:
    values = [
        to_float(row.get(field))
        for row in rows
        if clean_text(row.get("meal_realism_mode")) == mode
    ]
    return sum(values) / len(values) if values else 0.0


def sum_metric(rows: list[dict[str, object]], mode: str, field: str) -> int:
    return sum(
        int(to_float(row.get(field)))
        for row in rows
        if clean_text(row.get("meal_realism_mode")) == mode
    )


def _diag(diagnostics: object, field: str) -> object:
    if not isinstance(diagnostics, dict):
        return ""
    return diagnostics.get(field, "")


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
