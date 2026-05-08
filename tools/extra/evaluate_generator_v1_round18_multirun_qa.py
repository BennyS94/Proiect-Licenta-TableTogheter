from __future__ import annotations

import csv
import json
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

try:
    from evaluate_generator_v1_round15_balanced_selector import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        classify_macro_gaps,
        clean_text,
        macro_ratios,
        round_number,
        selected_snack_quality,
        serialize_reason,
        slot_candidates_by_slot,
        slot_order,
        target_to_dict,
        to_float,
    )
except ImportError:
    from tools.extra.evaluate_generator_v1_round15_balanced_selector import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        classify_macro_gaps,
        clean_text,
        macro_ratios,
        round_number,
        selected_snack_quality,
        serialize_reason,
        slot_candidates_by_slot,
        slot_order,
        target_to_dict,
        to_float,
    )


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round18_multirun_qa_summary.txt"
OUT_RUNS = OUT_DIR / "generator_v1_round18_multirun_qa_runs.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round18_multirun_qa_meals.csv"

RUN_COLUMNS = [
    "evaluation_mode",
    "run_index",
    "alternative_rank",
    "portion_policy",
    "diversity_mode",
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
    "base_day_loss",
    "adjusted_day_loss",
    "diversity_penalty",
    "recent_recipe_count",
    "selected_recipe_overlap_with_recent",
    "recent_recipe_ids_selected",
    "good_macro_fit",
    "carb_deficient",
    "protein_heavy",
    "fat_heavy",
    "macro_gap_classes_json",
    "selected_recipes",
    "selected_snack_quality",
    "slot_suspicious_count",
    "portion_issue_count",
    "selector_warnings",
]

MEAL_COLUMNS = [
    "evaluation_mode",
    "run_index",
    "alternative_rank",
    "portion_policy",
    "diversity_mode",
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
    "score_preview",
    "total_time_min",
    "effective_time_min_for_scoring",
    "portion_policy_reasons",
    "portion_policy_warnings",
    "portion_warning_classification",
    "is_slot_suspicious",
    "slot_fit_reasons",
    "snack_quality",
]


def main() -> None:
    base_profile = load_member_profile(BASE_PROFILE_PATH)
    demo_profile = build_scenario_profile(
        base_profile,
        {"scenario_id": "demo_profile_existing"},
    )
    fooddb = load_fooddb_current()
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    )

    run_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []

    target_context = build_context(
        profile=demo_profile,
        pool=pool,
        fooddb=fooddb,
        portion_policy="target_aware",
    )
    plan = run_balanced_plan(
        context=target_context,
        diversity_mode="none",
        recent_recipe_ids=set(),
    )
    run_rows.extend(
        alternative_run_rows(
            evaluation_mode="target_aware_none",
            run_index="1",
            portion_policy="target_aware",
            diversity_mode="none",
            plan=plan,
            target=target_context["target"],
            recent_recipe_ids=set(),
        )
    )
    meal_rows.extend(
        alternative_meal_rows(
            evaluation_mode="target_aware_none",
            run_index="1",
            portion_policy="target_aware",
            diversity_mode="none",
            plan=plan,
        )
    )

    recent_menus: list[set[str]] = []
    for run_index in range(1, 4):
        recent_recipe_ids = set().union(*recent_menus[-2:]) if recent_menus else set()
        context = build_context(
            profile=demo_profile,
            pool=pool,
            fooddb=fooddb,
            portion_policy="target_aware",
        )
        plan = run_balanced_plan(
            context=context,
            diversity_mode="avoid_recent",
            recent_recipe_ids=recent_recipe_ids,
        )
        run_rows.extend(
            alternative_run_rows(
                evaluation_mode="target_aware_avoid_recent",
                run_index=str(run_index),
                portion_policy="target_aware",
                diversity_mode="avoid_recent",
                plan=plan,
                target=context["target"],
                recent_recipe_ids=recent_recipe_ids,
            )
        )
        meal_rows.extend(
            alternative_meal_rows(
                evaluation_mode="target_aware_avoid_recent",
                run_index=str(run_index),
                portion_policy="target_aware",
                diversity_mode="avoid_recent",
                plan=plan,
            )
        )
        recent_menus.append(selected_recipe_ids(plan))

    expanded_context = build_context(
        profile=demo_profile,
        pool=pool,
        fooddb=fooddb,
        portion_policy="expanded_safe",
    )
    expanded_plan = run_balanced_plan(
        context=expanded_context,
        diversity_mode="none",
        recent_recipe_ids=set(),
    )
    run_rows.extend(
        alternative_run_rows(
            evaluation_mode="expanded_safe_none",
            run_index="1",
            portion_policy="expanded_safe",
            diversity_mode="none",
            plan=expanded_plan,
            target=expanded_context["target"],
            recent_recipe_ids=set(),
        )
    )
    meal_rows.extend(
        alternative_meal_rows(
            evaluation_mode="expanded_safe_none",
            run_index="1",
            portion_policy="expanded_safe",
            diversity_mode="none",
            plan=expanded_plan,
        )
    )

    write_csv(OUT_RUNS, run_rows, RUN_COLUMNS)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    OUT_SUMMARY.write_text(build_summary(run_rows, meal_rows), encoding="utf-8")

    print("Generator v1 round18 multi-run QA written")
    print(f"run_rows={len(run_rows)}")
    print(f"meal_rows={len(meal_rows)}")
    print(f"written_summary={OUT_SUMMARY}")


def build_context(
    profile: dict[str, Any],
    pool: object,
    fooddb: pd.DataFrame,
    portion_policy: str,
) -> dict[str, Any]:
    target = build_nutrition_target(profile)
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
        portion_policy_mode=portion_policy,
    )
    slots = slot_order(target)
    return {
        "target": target,
        "slot_order": slots,
        "slot_candidates_by_slot": slot_candidates_by_slot(slot_candidates, slots),
    }


def run_balanced_plan(
    context: dict[str, Any],
    diversity_mode: str,
    recent_recipe_ids: set[str],
) -> dict[str, object]:
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=context["slot_candidates_by_slot"],
        target=context["target"],
        slot_order=context["slot_order"],
        config={
            "return_alternatives": True,
            "alternative_count": 3,
            "diversity_mode": diversity_mode,
            "recent_recipe_ids": sorted(recent_recipe_ids),
        },
    )
    plan["target"] = target_to_dict(context["target"])
    plan["validation"] = validate_one_day_plan(plan, context["target"])
    return plan


def alternative_run_rows(
    evaluation_mode: str,
    run_index: str,
    portion_policy: str,
    diversity_mode: str,
    plan: dict[str, object],
    target: NutritionTarget,
    recent_recipe_ids: set[str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    alternatives = plan.get("alternatives", [])
    if not isinstance(alternatives, list):
        alternatives = []
    for alternative in alternatives:
        if not isinstance(alternative, dict):
            continue
        alt_plan = {
            "selected_meals": alternative.get("selected_meals", []),
            "day_totals": alternative.get("day_totals", {}),
        }
        validation = validate_one_day_plan(alt_plan, target)
        totals = alternative.get("day_totals", {})
        ratios = macro_ratios(totals, target)
        classes = classify_macro_gaps(ratios, clean_text(validation.get("validation_status")))
        penalties = alternative.get("diversity_penalties", {})
        selected_ids = set(clean_text(item) for item in alternative.get("selected_recipe_ids", []))
        overlap = sorted(selected_ids.intersection(recent_recipe_ids))
        meals = [
            meal for meal in alternative.get("selected_meals", []) if isinstance(meal, dict)
        ]
        rows.append(
            {
                "evaluation_mode": evaluation_mode,
                "run_index": run_index,
                "alternative_rank": clean_text(alternative.get("alternative_rank")),
                "portion_policy": portion_policy,
                "diversity_mode": diversity_mode,
                "validation_status": validation.get("validation_status", ""),
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
                "base_day_loss": round_number(alternative.get("base_day_loss")),
                "adjusted_day_loss": round_number(alternative.get("adjusted_day_loss")),
                "diversity_penalty": round_number(
                    penalties.get("total_diversity_penalty")
                    if isinstance(penalties, dict)
                    else 0
                ),
                "recent_recipe_count": clean_text(
                    penalties.get("recent_recipe_count")
                    if isinstance(penalties, dict)
                    else 0
                ),
                "selected_recipe_overlap_with_recent": str(len(overlap)),
                "recent_recipe_ids_selected": "|".join(overlap),
                "good_macro_fit": str("good_fit" in classes),
                "carb_deficient": str("carb_deficient" in classes),
                "protein_heavy": str("protein_heavy" in classes),
                "fat_heavy": str("fat_heavy" in classes),
                "macro_gap_classes_json": json.dumps(classes),
                "selected_recipes": "|".join(sorted(selected_ids)),
                "selected_snack_quality": selected_snack_quality(alt_plan),
                "slot_suspicious_count": str(
                    sum(1 for meal in meals if clean_text(meal.get("is_slot_suspicious")) == "True")
                ),
                "portion_issue_count": str(
                    sum(1 for meal in meals if portion_issue_class(meal) != "acceptable_large_portion")
                ),
                "selector_warnings": "|".join(
                    clean_text(item)
                    for item in alternative.get("selector_warnings", [])
                    if clean_text(item)
                ),
            }
        )
    return rows


def alternative_meal_rows(
    evaluation_mode: str,
    run_index: str,
    portion_policy: str,
    diversity_mode: str,
    plan: dict[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    alternatives = plan.get("alternatives", [])
    if not isinstance(alternatives, list):
        alternatives = []
    for alternative in alternatives:
        if not isinstance(alternative, dict):
            continue
        rank = clean_text(alternative.get("alternative_rank"))
        for meal in alternative.get("selected_meals", []):
            if not isinstance(meal, dict):
                continue
            rows.append(
                {
                    "evaluation_mode": evaluation_mode,
                    "run_index": run_index,
                    "alternative_rank": rank,
                    "portion_policy": portion_policy,
                    "diversity_mode": diversity_mode,
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
                    "score_preview": round_number(meal.get("score_preview")),
                    "total_time_min": round_number(meal.get("total_time_min")),
                    "effective_time_min_for_scoring": round_number(
                        meal.get("effective_time_min_for_scoring")
                    ),
                    "portion_policy_reasons": serialize_reason(meal.get("portion_policy_reasons")),
                    "portion_policy_warnings": serialize_reason(meal.get("portion_policy_warnings")),
                    "portion_warning_classification": portion_issue_class(meal),
                    "is_slot_suspicious": clean_text(meal.get("is_slot_suspicious")),
                    "slot_fit_reasons": serialize_reason(meal.get("slot_fit_reasons")),
                    "snack_quality": selected_snack_quality({"selected_meals": [meal]}),
                }
            )
    return rows


def portion_issue_class(meal: dict[str, object]) -> str:
    slot = clean_text(meal.get("slot"))
    grams = to_float(meal.get("portion_grams_estimated"))
    kcal = to_float(meal.get("kcal"))
    multiplier = to_float(meal.get("portion_multiplier"))
    if grams <= 0:
        return "missing_portion_grams"
    if multiplier > 1.8:
        return "unrealistic_portion"
    if slot == "snack" and (grams > 300 or kcal > 400):
        return "snack_too_large"
    if slot == "breakfast" and (grams > 500 or kcal > 800):
        return "breakfast_too_large"
    if slot in {"lunch", "dinner"} and (grams > 850 or kcal > 1200):
        return "main_too_large"
    return "acceptable_large_portion"


def selected_recipe_ids(plan: dict[str, object]) -> set[str]:
    return {
        clean_text(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if isinstance(meal, dict) and clean_text(meal.get("recipe_id"))
    }


def build_summary(
    run_rows: list[dict[str, object]],
    meal_rows: list[dict[str, object]],
) -> str:
    target_none = rows_for(run_rows, "target_aware_none", rank="1")
    target_none_all = rows_for(run_rows, "target_aware_none")
    avoid_rank1 = rows_for(run_rows, "target_aware_avoid_recent", rank="1")
    avoid_all = rows_for(run_rows, "target_aware_avoid_recent")
    expanded_rank1 = rows_for(run_rows, "expanded_safe_none", rank="1")
    target_valid = all(clean_text(row.get("validation_status")) == "valid" for row in target_none_all)
    avoid_valid = all(clean_text(row.get("validation_status")) == "valid" for row in avoid_all)
    alternatives_unique = unique_recipe_sets(target_none_all)
    avoid_overlap = [
        int(to_float(row.get("selected_recipe_overlap_with_recent")))
        for row in avoid_rank1
    ]
    target_portion_issues = count_portion_issues(meal_rows, "target_aware_none")
    avoid_portion_issues = count_portion_issues(meal_rows, "target_aware_avoid_recent")
    expanded_portion_issues = count_portion_issues(meal_rows, "expanded_safe_none")
    lines = [
        "Generator v1 round18 multi-run QA",
        "=" * 37,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        "recommended_preset: balanced_day + target_aware + alternative_count=3 + diversity=none",
        "",
        "Target-aware / diversity none:",
        f"- alternatives_returned: {len(target_none_all)}",
        f"- all_alternatives_valid: {target_valid}",
        f"- unique_recipe_sets_across_alternatives: {alternatives_unique}",
        f"- rank1_day_loss: {first_field(target_none, 'base_day_loss')}",
        f"- rank1_macro_flags: {first_macro_flags(target_none)}",
        f"- portion_issue_count: {target_portion_issues}",
        "",
        "Target-aware / avoid_recent repeated demo runs:",
        f"- rank1_runs: {len(avoid_rank1)}",
        f"- all_alternatives_valid: {avoid_valid}",
        f"- rank1_recent_overlap_counts: {avoid_overlap}",
        f"- rank1_average_base_day_loss: {round_number(average_field(avoid_rank1, 'base_day_loss'))}",
        f"- rank1_average_adjusted_day_loss: {round_number(average_field(avoid_rank1, 'adjusted_day_loss'))}",
        f"- unique_rank1_recipes_across_runs: {len(unique_recipes(avoid_rank1))}",
        f"- portion_issue_count: {avoid_portion_issues}",
        "",
        "Expanded-safe / diversity none:",
        f"- rank1_day_loss: {first_field(expanded_rank1, 'base_day_loss')}",
        f"- rank1_macro_flags: {first_macro_flags(expanded_rank1)}",
        f"- portion_issue_count: {expanded_portion_issues}",
        "",
        "Repeated run details:",
    ]
    for row in avoid_rank1:
        lines.append(
            "- run "
            + clean_text(row.get("run_index"))
            + " | loss="
            + clean_text(row.get("base_day_loss"))
            + " | adjusted="
            + clean_text(row.get("adjusted_day_loss"))
            + " | overlap="
            + clean_text(row.get("selected_recipe_overlap_with_recent"))
            + " | recipes="
            + clean_text(row.get("selected_recipes"))
        )
    lines.extend(
        [
            "",
            "Conclusion:",
            f"- target_aware_remains_valid_across_repeated_ui_generations: {target_valid and avoid_valid}",
            f"- avoid_recent_reduces_repetition_without_invalidating_plans: {avoid_valid and max(avoid_overlap or [0]) <= 1}",
            f"- alternatives_are_useful_and_semantically_valid: {target_valid and alternatives_unique >= 2}",
            f"- obvious_slot_or_portion_issue_count: {target_portion_issues + avoid_portion_issues + expanded_portion_issues}",
            "- diversity ramane optiune de testare; diversity none ramane presetul recomandat pentru demonstratia v1.1.",
            "",
            "Output files:",
            f"- {OUT_RUNS}",
            f"- {OUT_MEALS}",
        ]
    )
    return "\n".join(lines) + "\n"


def rows_for(
    rows: list[dict[str, object]],
    evaluation_mode: str,
    rank: str | None = None,
) -> list[dict[str, object]]:
    selected = [
        row
        for row in rows
        if clean_text(row.get("evaluation_mode")) == evaluation_mode
    ]
    if rank is not None:
        selected = [
            row
            for row in selected
            if clean_text(row.get("alternative_rank")) == rank
        ]
    return selected


def first_field(rows: list[dict[str, object]], field: str) -> str:
    return clean_text(rows[0].get(field)) if rows else ""


def first_macro_flags(rows: list[dict[str, object]]) -> str:
    if not rows:
        return ""
    row = rows[0]
    return (
        "good_macro_fit="
        + clean_text(row.get("good_macro_fit"))
        + ", carb_deficient="
        + clean_text(row.get("carb_deficient"))
        + ", protein_heavy="
        + clean_text(row.get("protein_heavy"))
        + ", fat_heavy="
        + clean_text(row.get("fat_heavy"))
    )


def average_field(rows: list[dict[str, object]], field: str) -> float:
    values = [to_float(row.get(field)) for row in rows]
    return sum(values) / len(values) if values else 0.0


def unique_recipe_sets(rows: list[dict[str, object]]) -> int:
    return len({clean_text(row.get("selected_recipes")) for row in rows if clean_text(row.get("selected_recipes"))})


def unique_recipes(rows: list[dict[str, object]]) -> set[str]:
    recipes: set[str] = set()
    for row in rows:
        recipes.update(
            recipe_id
            for recipe_id in clean_text(row.get("selected_recipes")).split("|")
            if recipe_id
        )
    return recipes


def count_portion_issues(
    rows: list[dict[str, object]],
    evaluation_mode: str,
) -> int:
    bad_classes = {
        "missing_portion_grams",
        "unrealistic_portion",
        "snack_too_large",
        "breakfast_too_large",
        "main_too_large",
    }
    return sum(
        1
        for row in rows
        if clean_text(row.get("evaluation_mode")) == evaluation_mode
        and clean_text(row.get("portion_warning_classification")) in bad_classes
    )


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
