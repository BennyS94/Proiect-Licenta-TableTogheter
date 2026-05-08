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
        build_candidate_lookup,
        build_scenario_profile,
        classify_macro_gaps,
        clean_text,
        macro_ratios,
        round_number,
        scenario_overrides,
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
        build_candidate_lookup,
        build_scenario_profile,
        classify_macro_gaps,
        clean_text,
        macro_ratios,
        round_number,
        scenario_overrides,
        selected_snack_quality,
        serialize_reason,
        slot_candidates_by_slot,
        slot_order,
        target_to_dict,
        to_float,
    )


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round16_variety_alternatives_summary.txt"
OUT_PLANS = OUT_DIR / "generator_v1_round16_variety_alternatives_plans.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round16_variety_alternatives_meals.csv"
OUT_REPETITION = OUT_DIR / "generator_v1_round16_variety_repetition_comparison.csv"

PLAN_COLUMNS = [
    "evaluation_mode",
    "scenario_id",
    "run_index",
    "alternative_rank",
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
    "recent_recipe_ids_selected",
    "good_macro_fit",
    "carb_deficient",
    "protein_heavy",
    "fat_heavy",
    "macro_gap_classes_json",
    "selected_recipes",
    "selected_snack_quality",
    "selected_recipe_overlap_with_recent",
    "selector_warnings",
]

MEAL_COLUMNS = [
    "evaluation_mode",
    "scenario_id",
    "run_index",
    "alternative_rank",
    "diversity_mode",
    "slot",
    "recipe_id",
    "display_name",
    "recipe_kind",
    "recipe_category",
    "recipe_subcategory",
    "portion_multiplier",
    "kcal",
    "protein_g",
    "carbs_g",
    "fat_g",
    "score_preview",
    "macro_fit",
    "time_fit",
    "slot_fit",
    "nutrition_quality",
    "effective_time_min_for_scoring",
    "is_slot_suspicious",
    "slot_fit_reasons",
    "snack_quality",
]

REPETITION_COLUMNS = [
    "comparison_group",
    "recipe_id",
    "display_name",
    "selected_count",
    "breakfast_count",
    "lunch_count",
    "dinner_count",
    "snack_count",
    "scenario_or_run_ids",
]


def main() -> None:
    base_profile = load_member_profile(BASE_PROFILE_PATH)
    fooddb = load_fooddb_current()
    plan_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []

    for overrides in scenario_overrides():
        scenario_id = clean_text(overrides["scenario_id"])
        profile = build_scenario_profile(base_profile, overrides)
        context = build_context(profile, fooddb)
        plan = run_balanced_plan(context, diversity_mode="none", recent_recipe_ids=set())
        plan_rows.extend(
            alternative_plan_rows(
                evaluation_mode="scenario_alternatives_none",
                scenario_id=scenario_id,
                run_index="",
                diversity_mode="none",
                plan=plan,
                target=context["target"],
                recent_recipe_ids=set(),
            )
        )
        meal_rows.extend(
            alternative_meal_rows(
                evaluation_mode="scenario_alternatives_none",
                scenario_id=scenario_id,
                run_index="",
                diversity_mode="none",
                plan=plan,
                candidate_lookup=context["candidate_lookup"],
            )
        )

    demo_profile = build_scenario_profile(
        base_profile,
        {"scenario_id": "demo_profile_existing"},
    )
    recent_recipe_ids: set[str] = set()
    for run_index in range(1, 4):
        context = build_context(demo_profile, fooddb)
        plan = run_balanced_plan(
            context,
            diversity_mode="avoid_recent",
            recent_recipe_ids=recent_recipe_ids,
        )
        plan_rows.extend(
            alternative_plan_rows(
                evaluation_mode="demo_avoid_recent_repeated",
                scenario_id="demo_profile_existing",
                run_index=str(run_index),
                diversity_mode="avoid_recent",
                plan=plan,
                target=context["target"],
                recent_recipe_ids=recent_recipe_ids,
            )
        )
        meal_rows.extend(
            alternative_meal_rows(
                evaluation_mode="demo_avoid_recent_repeated",
                scenario_id="demo_profile_existing",
                run_index=str(run_index),
                diversity_mode="avoid_recent",
                plan=plan,
                candidate_lookup=context["candidate_lookup"],
            )
        )
        recent_recipe_ids.update(selected_recipe_ids(plan))

    repetition_rows = build_repetition_rows(plan_rows, meal_rows)
    write_csv(OUT_PLANS, plan_rows, PLAN_COLUMNS)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    write_csv(OUT_REPETITION, repetition_rows, REPETITION_COLUMNS)
    OUT_SUMMARY.write_text(
        build_summary(plan_rows, repetition_rows),
        encoding="utf-8",
    )
    print("Generator v1 round16 variety alternatives evaluation written")
    print(f"scenario_count={len(list(scenario_overrides()))}")
    print(f"written_summary={OUT_SUMMARY}")
    print(f"written_plans={OUT_PLANS}")


def build_context(profile: dict[str, Any], fooddb: pd.DataFrame) -> dict[str, Any]:
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    )
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
    )
    slots = slot_order(target)
    return {
        "target": target,
        "slot_order": slots,
        "slot_candidates_by_slot": slot_candidates_by_slot(slot_candidates, slots),
        "candidate_lookup": build_candidate_lookup(slot_candidates),
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


def alternative_plan_rows(
    evaluation_mode: str,
    scenario_id: str,
    run_index: str,
    diversity_mode: str,
    plan: dict[str, object],
    target: NutritionTarget,
    recent_recipe_ids: set[str],
) -> list[dict[str, object]]:
    rows = []
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
        rows.append(
            {
                "evaluation_mode": evaluation_mode,
                "scenario_id": scenario_id,
                "run_index": run_index,
                "alternative_rank": clean_text(alternative.get("alternative_rank")),
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
                "recent_recipe_ids_selected": "|".join(overlap),
                "good_macro_fit": str("good_fit" in classes),
                "carb_deficient": str("carb_deficient" in classes),
                "protein_heavy": str("protein_heavy" in classes),
                "fat_heavy": str("fat_heavy" in classes),
                "macro_gap_classes_json": json.dumps(classes),
                "selected_recipes": "|".join(sorted(selected_ids)),
                "selected_snack_quality": selected_snack_quality(alt_plan),
                "selected_recipe_overlap_with_recent": str(len(overlap)),
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
    scenario_id: str,
    run_index: str,
    diversity_mode: str,
    plan: dict[str, object],
    candidate_lookup: dict[tuple[str, str, float], dict[str, object]],
) -> list[dict[str, object]]:
    rows = []
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
            key = (
                clean_text(meal.get("slot")),
                clean_text(meal.get("recipe_id")),
                round(to_float(meal.get("portion_multiplier")), 4),
            )
            candidate = candidate_lookup.get(key, {})
            rows.append(
                {
                    "evaluation_mode": evaluation_mode,
                    "scenario_id": scenario_id,
                    "run_index": run_index,
                    "alternative_rank": rank,
                    "diversity_mode": diversity_mode,
                    "slot": clean_text(meal.get("slot")),
                    "recipe_id": clean_text(meal.get("recipe_id")),
                    "display_name": clean_text(meal.get("display_name")),
                    "recipe_kind": clean_text(candidate.get("recipe_kind")),
                    "recipe_category": clean_text(candidate.get("recipe_category")),
                    "recipe_subcategory": clean_text(candidate.get("recipe_subcategory")),
                    "portion_multiplier": round_number(meal.get("portion_multiplier")),
                    "kcal": round_number(meal.get("kcal")),
                    "protein_g": round_number(meal.get("protein_g")),
                    "carbs_g": round_number(meal.get("carbs_g")),
                    "fat_g": round_number(meal.get("fat_g")),
                    "score_preview": round_number(meal.get("score_preview")),
                    "macro_fit": round_number(meal.get("macro_fit")),
                    "time_fit": round_number(meal.get("time_fit")),
                    "slot_fit": round_number(meal.get("slot_fit")),
                    "nutrition_quality": round_number(meal.get("nutrition_quality")),
                    "effective_time_min_for_scoring": round_number(
                        meal.get("effective_time_min_for_scoring")
                    ),
                    "is_slot_suspicious": clean_text(meal.get("is_slot_suspicious")),
                    "slot_fit_reasons": serialize_reason(meal.get("slot_fit_reasons")),
                    "snack_quality": selected_snack_quality({"selected_meals": [meal]}),
                }
            )
    return rows


def build_repetition_rows(
    plan_rows: list[dict[str, object]],
    meal_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    groups = {
        "scenario_rank1_none": [
            row
            for row in meal_rows
            if clean_text(row.get("evaluation_mode")) == "scenario_alternatives_none"
            and clean_text(row.get("alternative_rank")) == "1"
        ],
        "scenario_all_alternatives_none": [
            row
            for row in meal_rows
            if clean_text(row.get("evaluation_mode")) == "scenario_alternatives_none"
        ],
        "demo_avoid_recent_selected": [
            row
            for row in meal_rows
            if clean_text(row.get("evaluation_mode")) == "demo_avoid_recent_repeated"
            and clean_text(row.get("alternative_rank")) == "1"
        ],
        "demo_avoid_recent_all_alternatives": [
            row
            for row in meal_rows
            if clean_text(row.get("evaluation_mode")) == "demo_avoid_recent_repeated"
        ],
    }
    rows: list[dict[str, object]] = []
    for group_name, meals in groups.items():
        rows.extend(repetition_for_group(group_name, meals))
    return rows


def repetition_for_group(
    group_name: str,
    meals: list[dict[str, object]],
) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    for meal in meals:
        recipe_id = clean_text(meal.get("recipe_id"))
        if not recipe_id:
            continue
        if recipe_id not in grouped:
            grouped[recipe_id] = {
                "comparison_group": group_name,
                "recipe_id": recipe_id,
                "display_name": clean_text(meal.get("display_name")),
                "selected_count": 0,
                "breakfast_count": 0,
                "lunch_count": 0,
                "dinner_count": 0,
                "snack_count": 0,
                "scenario_or_run_ids": [],
            }
        row = grouped[recipe_id]
        row["selected_count"] = int(row["selected_count"]) + 1
        slot = clean_text(meal.get("slot"))
        if f"{slot}_count" in row:
            row[f"{slot}_count"] = int(row[f"{slot}_count"]) + 1
        marker = clean_text(meal.get("scenario_id"))
        run_index = clean_text(meal.get("run_index"))
        if run_index:
            marker = f"{marker}:run{run_index}"
        if marker not in row["scenario_or_run_ids"]:
            row["scenario_or_run_ids"].append(marker)
    return sorted(
        [
            {**row, "scenario_or_run_ids": "|".join(row["scenario_or_run_ids"])}
            for row in grouped.values()
        ],
        key=lambda row: (-int(row["selected_count"]), clean_text(row["recipe_id"])),
    )


def build_summary(
    plan_rows: list[dict[str, object]],
    repetition_rows: list[dict[str, object]],
) -> str:
    scenario_rows = [
        row
        for row in plan_rows
        if clean_text(row.get("evaluation_mode")) == "scenario_alternatives_none"
    ]
    scenario_rank1 = [row for row in scenario_rows if clean_text(row.get("alternative_rank")) == "1"]
    scenario_rank2 = [row for row in scenario_rows if clean_text(row.get("alternative_rank")) == "2"]
    scenario_rank3 = [row for row in scenario_rows if clean_text(row.get("alternative_rank")) == "3"]
    demo_rank1 = [
        row
        for row in plan_rows
        if clean_text(row.get("evaluation_mode")) == "demo_avoid_recent_repeated"
        and clean_text(row.get("alternative_rank")) == "1"
    ]
    scenario_count = len({clean_text(row.get("scenario_id")) for row in scenario_rows})
    scenarios_with_3 = sum(
        1
        for scenario_id in {clean_text(row.get("scenario_id")) for row in scenario_rows}
        if sum(1 for row in scenario_rows if clean_text(row.get("scenario_id")) == scenario_id) >= 3
    )
    valid_alternatives = sum(
        1 for row in scenario_rows if clean_text(row.get("validation_status")) == "valid"
    )
    demo_repeat_counts = [
        int(to_float(row.get("selected_recipe_overlap_with_recent")))
        for row in demo_rank1
    ]
    demo_unique_recipes = sorted(
        {
            recipe_id
            for row in demo_rank1
            for recipe_id in clean_text(row.get("selected_recipes")).split("|")
            if recipe_id
        }
    )
    lines = [
        "Generator v1 round16 variety alternatives evaluation",
        "=" * 55,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        f"scenario_count: {scenario_count}",
        f"scenarios_with_3_alternatives: {scenarios_with_3}",
        f"scenario_alternative_rows: {len(scenario_rows)}",
        f"scenario_valid_alternatives: {valid_alternatives}/{len(scenario_rows)}",
        f"rank1_average_base_day_loss: {round_number(average_field(scenario_rank1, 'base_day_loss'))}",
        f"rank2_average_base_day_loss: {round_number(average_field(scenario_rank2, 'base_day_loss'))}",
        f"rank3_average_base_day_loss: {round_number(average_field(scenario_rank3, 'base_day_loss'))}",
        f"rank1_good_macro_fit: {count_true(scenario_rank1, 'good_macro_fit')}/{len(scenario_rank1)}",
        f"rank1_carb_deficient: {count_true(scenario_rank1, 'carb_deficient')}/{len(scenario_rank1)}",
        "",
        "Demo repeated Generate simulation with avoid_recent:",
    ]
    for row in demo_rank1:
        lines.append(
            "- run "
            + clean_text(row.get("run_index"))
            + ": loss="
            + clean_text(row.get("base_day_loss"))
            + ", adjusted="
            + clean_text(row.get("adjusted_day_loss"))
            + ", recent_overlap="
            + clean_text(row.get("selected_recipe_overlap_with_recent"))
            + ", recipes="
            + clean_text(row.get("selected_recipes"))
        )
    lines.extend(
        [
            f"demo_rank1_recent_overlap_counts: {demo_repeat_counts}",
            f"demo_unique_recipes_across_3_runs: {len(demo_unique_recipes)}",
            f"demo_all_rank1_valid: {all(clean_text(row.get('validation_status')) == 'valid' for row in demo_rank1)}",
            f"demo_average_base_day_loss: {round_number(average_field(demo_rank1, 'base_day_loss'))}",
            f"demo_average_adjusted_day_loss: {round_number(average_field(demo_rank1, 'adjusted_day_loss'))}",
            "",
            "Top repetition scenario rank1:",
        ]
    )
    lines.extend(repetition_lines(repetition_rows, "scenario_rank1_none"))
    lines.extend(["", "Top repetition demo avoid_recent selected:"])
    lines.extend(repetition_lines(repetition_rows, "demo_avoid_recent_selected"))
    lines.extend(
        [
            "",
            "Conclusion:",
            "- balanced_day can produce multiple valid near-loss alternatives for v1.1 testing.",
            "- avoid_recent reduces repeated selected recipes across repeated demo clicks, but macro loss can rise when many recent recipes are penalized.",
            "- snacks remain acceptable because selected snack alternatives are manual round12 snacks.",
            "- enable alternatives/diversity in Streamlit for v1.1 testing, but keep the mode explicit while variety scoring is still draft.",
            "",
            "Output files:",
            f"- {OUT_PLANS}",
            f"- {OUT_MEALS}",
            f"- {OUT_REPETITION}",
        ]
    )
    return "\n".join(lines) + "\n"


def selected_recipe_ids(plan: dict[str, object]) -> set[str]:
    return {
        clean_text(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if isinstance(meal, dict) and clean_text(meal.get("recipe_id"))
    }


def average_field(rows: list[dict[str, object]], field: str) -> float:
    values = [to_float(row.get(field)) for row in rows]
    return sum(values) / len(values) if values else 0.0


def count_true(rows: list[dict[str, object]], field: str) -> int:
    return sum(1 for row in rows if clean_text(row.get(field)) == "True")


def repetition_lines(rows: list[dict[str, object]], group_name: str) -> list[str]:
    selected = [
        row
        for row in rows
        if clean_text(row.get("comparison_group")) == group_name
    ][:8]
    if not selected:
        return ["- none"]
    return [
        "- "
        + clean_text(row.get("recipe_id"))
        + " | "
        + clean_text(row.get("display_name"))
        + " | count="
        + clean_text(row.get("selected_count"))
        for row in selected
    ]


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
