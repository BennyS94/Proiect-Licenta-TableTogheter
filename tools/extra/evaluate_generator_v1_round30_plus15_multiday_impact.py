from __future__ import annotations

import sys
from collections.abc import Mapping
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
    V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH,
    V1_2_GENERATOR_READY_PLUS30_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.multi_day_audit import multi_day_meal_rows
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round30_plus15_multiday_impact_summary.txt"
OUT_DAYS = OUT_DIR / "generator_v1_round30_plus15_multiday_days.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round30_plus15_multiday_meals.csv"
OUT_REPETITION = OUT_DIR / "generator_v1_round30_plus15_multiday_repetition.csv"

PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"
QUALITY_GATE = "demo_safe"
DAYS = 3
ROUND30_PREFIX = "recipes_v1_2_round30_plus15_"
DOMINANT_RECIPE_NAMES = {
    "Mom's Best Waffles",
    "Sweet and Sour Stuffed Cabbage",
    "Veggie Burgers",
    "Chicken and Broccoli Pasta",
}

DATASETS = [
    {
        "label": "round28_plus30",
        "dataset_profile": V1_2_GENERATOR_READY_PLUS30_PROFILE,
        "recipes_path": V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH,
        "ingredients_path": V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH,
        "nutrition_path": V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH,
    },
    {
        "label": "round30_plus30_plus15",
        "dataset_profile": V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
        "recipes_path": V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH,
        "ingredients_path": V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH,
        "nutrition_path": V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH,
    },
]


def main() -> None:
    plans = []
    day_rows = []
    meal_rows = []
    repetition_rows = []
    for dataset in DATASETS:
        context = build_generation_context(dataset)
        plan = generate_plan(context)
        plans.append((dataset, plan, context))
        target_data = target_to_dict(context["target"])
        day_rows.extend(build_day_rows(dataset, plan, target_data))
        meal_rows.extend(build_meal_rows(dataset, plan))
        repetition_rows.extend(build_repetition_rows(dataset, plan))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(day_rows).to_csv(OUT_DAYS, index=False)
    pd.DataFrame(meal_rows).to_csv(OUT_MEALS, index=False)
    pd.DataFrame(repetition_rows).to_csv(OUT_REPETITION, index=False)
    OUT_SUMMARY.write_text(
        build_summary(plans, day_rows, meal_rows, repetition_rows),
        encoding="utf-8",
    )

    old_summary = plans[0][1]["multi_day_summary"]
    plus_summary = plans[1][1]["multi_day_summary"]
    print("Generator v1 Round30 plus15 multi-day impact evaluation written")
    print(f"summary={OUT_SUMMARY}")
    print(f"days={OUT_DAYS}")
    print(f"meals={OUT_MEALS}")
    print(f"repetition={OUT_REPETITION}")
    print(
        "plus30="
        f"valid:{old_summary.get('valid_day_count')} "
        f"accept:{old_summary.get('accept_day_count')} "
        f"review:{old_summary.get('review_day_count')} "
        f"unique:{old_summary.get('unique_recipe_count')} "
        f"repeated:{old_summary.get('repeated_recipe_count')} "
        f"loss:{old_summary.get('multi_day_loss')}"
    )
    print(
        "plus30_plus15="
        f"valid:{plus_summary.get('valid_day_count')} "
        f"accept:{plus_summary.get('accept_day_count')} "
        f"review:{plus_summary.get('review_day_count')} "
        f"unique:{plus_summary.get('unique_recipe_count')} "
        f"repeated:{plus_summary.get('repeated_recipe_count')} "
        f"loss:{plus_summary.get('multi_day_loss')}"
    )


def build_generation_context(dataset: Mapping[str, Any]) -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=dataset["recipes_path"],
        ingredients_path=dataset["ingredients_path"],
        nutrition_path=dataset["nutrition_path"],
        dataset_profile=str(dataset["dataset_profile"]),
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


def generate_plan(context: Mapping[str, Any]) -> dict[str, Any]:
    return generate_multi_day_plan(
        profile=context["profile"],
        target=context["target"],
        slot_candidates=context["slot_candidates"],
        days=DAYS,
        config={
            "selection_mode": "balanced_day",
            "portion_policy": PORTION_POLICY,
            "meal_realism_mode": MEAL_REALISM_MODE,
            "quality_gate": QUALITY_GATE,
            "alternative_count": 3,
            "return_alternatives": True,
            "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
            "candidate_day_alternative_count": 10,
            "global_max_candidates_per_slot": 26,
        },
    )


def build_day_rows(
    dataset: Mapping[str, Any],
    plan: Mapping[str, Any],
    target: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        totals = day.get("day_totals", {})
        diagnostics = day.get("selector_diagnostics", {})
        rows.append(
            {
                "dataset_label": dataset["label"],
                "dataset_profile": dataset["dataset_profile"],
                "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
                "day_index": day.get("day_index"),
                "validation_status": day.get("validation_status"),
                "quality_gate_status": day.get("quality_gate_status"),
                "fallback_used": day.get("fallback_used"),
                "diversity_mode_used": day.get("diversity_mode_used"),
                "candidate_day_id": day.get("candidate_day_id"),
                "repeated_recipe_ids_vs_previous_days": format_list(
                    day.get("repeated_recipe_ids_vs_previous_days")
                ),
                "base_day_loss": diagnostics.get("base_day_loss"),
                "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
                "total_kcal": totals.get("total_kcal"),
                "kcal_ratio": ratio(totals.get("total_kcal"), target.get("kcal")),
                "total_protein_g": totals.get("total_protein_g"),
                "protein_ratio": ratio(totals.get("total_protein_g"), target.get("protein_g")),
                "total_carbs_g": totals.get("total_carbs_g"),
                "carbs_ratio": ratio(totals.get("total_carbs_g"), target.get("carbs_g")),
                "total_fat_g": totals.get("total_fat_g"),
                "fat_ratio": ratio(totals.get("total_fat_g"), target.get("fat_g")),
                "effective_time_min_sum": totals.get("effective_time_min_sum"),
                "quality_gate_reasons": format_list(day.get("quality_gate_reasons")),
                "warnings": format_list(day.get("warnings")),
                "verdict": day_verdict(day),
            }
        )
    return rows


def build_meal_rows(
    dataset: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for row in multi_day_meal_rows(plan):
        recipe_id = str(row.get("recipe_id", "")).strip()
        display_name = str(row.get("display_name", "")).strip()
        rows.append(
            {
                "dataset_label": dataset["label"],
                "dataset_profile": dataset["dataset_profile"],
                "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
                "is_round30_added_recipe": recipe_id.startswith(ROUND30_PREFIX),
                "is_dominant_recipe": display_name in DOMINANT_RECIPE_NAMES,
                **row,
            }
        )
    return rows


def build_repetition_rows(
    dataset: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    occurrences: dict[str, list[dict[str, Any]]] = {}
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if not recipe_id:
                continue
            occurrences.setdefault(recipe_id, []).append(
                {
                    "day_index": day.get("day_index"),
                    "slot": meal.get("slot"),
                    "display_name": meal.get("display_name"),
                }
            )

    rows = []
    for recipe_id, items in sorted(occurrences.items()):
        if len(items) <= 1:
            continue
        display_name = str(items[0].get("display_name", "")).strip()
        rows.append(
            {
                "dataset_label": dataset["label"],
                "dataset_profile": dataset["dataset_profile"],
                "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
                "recipe_id": recipe_id,
                "display_name": display_name,
                "count": len(items),
                "days": format_list([item.get("day_index") for item in items]),
                "slots": format_list([item.get("slot") for item in items]),
                "is_main_repeat": any(
                    str(item.get("slot")) in {"lunch", "dinner"} for item in items
                ),
                "is_breakfast_repeat": any(
                    str(item.get("slot")) == "breakfast" for item in items
                ),
                "is_round30_added_recipe": recipe_id.startswith(ROUND30_PREFIX),
                "is_dominant_recipe": display_name in DOMINANT_RECIPE_NAMES,
            }
        )
    return rows


def build_summary(
    plans: list[tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]],
    day_rows: list[dict[str, Any]],
    meal_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
) -> str:
    summaries = {
        dataset["label"]: plan.get("multi_day_summary", {})
        for dataset, plan, _context in plans
    }
    old = summaries["round28_plus30"]
    plus = summaries["round30_plus30_plus15"]
    comparison = compare_summaries(old, plus, meal_rows, repetition_rows)
    lines = [
        "Generator v1 Round30 plus15 multi-day impact evaluation",
        "",
        "selection_mode=balanced_day",
        f"portion_policy={PORTION_POLICY}",
        f"meal_realism_mode={MEAL_REALISM_MODE}",
        f"quality_gate={QUALITY_GATE}",
        f"days_requested={DAYS}",
        f"multi_day_mode={MULTI_DAY_MODE_GLOBAL}",
        "",
        "Dataset comparison",
    ]
    for dataset, plan, context in plans:
        summary = plan.get("multi_day_summary", {})
        pool = context["pool"]
        added_selected = selected_added_recipe_names(meal_rows, str(dataset["label"]))
        lines.extend(
            [
                f"dataset_label={dataset['label']}",
                f"dataset_profile={dataset['dataset_profile']}",
                f"recipes_loaded={len(pool.candidates)}",
                f"eligible_candidates={len(pool.eligible_candidates)}",
                f"generated_days_count={summary.get('generated_day_count')}",
                f"valid_days_count={summary.get('valid_day_count')}",
                f"accept_days_count={summary.get('accept_day_count')}",
                f"review_days_count={summary.get('review_day_count')}",
                f"fallback_days_count={summary.get('fallback_day_count')}",
                f"average_day_loss={summary.get('average_day_loss')}",
                f"multi_day_loss={summary.get('multi_day_loss')}",
                f"unique_recipe_count={summary.get('unique_recipe_count')}",
                f"repeated_recipe_count={summary.get('repeated_recipe_count')}",
                "repeated_recipe_ids=" + format_list(summary.get("repeated_recipe_ids")),
                f"repeated_main_recipe_count={repeated_main_count(repetition_rows, str(dataset['label']))}",
                f"dominant_recipe_meal_count={dominant_meal_count(meal_rows, str(dataset['label']))}",
                f"round30_added_recipe_meal_count={added_recipe_meal_count(meal_rows, str(dataset['label']))}",
                "round30_added_recipes_selected=" + format_list(added_selected),
                f"strict_assessment={summary.get('multi_day_classification')}",
                "",
            ]
        )
    lines.extend(
        [
            "Plus30 vs plus30_plus15",
            f"valid_day_delta={comparison['valid_day_delta']}",
            f"accept_day_delta={comparison['accept_day_delta']}",
            f"review_day_delta={comparison['review_day_delta']}",
            f"unique_recipe_delta={comparison['unique_recipe_delta']}",
            f"repeated_recipe_delta={comparison['repeated_recipe_delta']}",
            f"repeated_main_recipe_delta={comparison['repeated_main_recipe_delta']}",
            f"dominant_recipe_meal_delta={comparison['dominant_recipe_meal_delta']}",
            f"round30_added_recipe_meal_count={comparison['round30_added_recipe_meal_count']}",
            f"plus15_improved_variety={comparison['plus15_improved_variety']}",
            f"plus15_destroyed_quality={comparison['plus15_destroyed_quality']}",
            "",
            "Daily macro fit",
        ]
    )
    for row in day_rows:
        lines.append(
            (
                f"dataset={row['dataset_label']} day={row['day_index']} "
                f"status={row['validation_status']} gate={row['quality_gate_status']} "
                f"kcal_ratio={row['kcal_ratio']} protein_ratio={row['protein_ratio']} "
                f"carbs_ratio={row['carbs_ratio']} fat_ratio={row['fat_ratio']} "
                f"adjusted_day_loss={row['adjusted_day_loss']} verdict={row['verdict']}"
            )
        )
    lines.extend(["", "Strict conclusion", strict_conclusion(plus, comparison), ""])
    return "\n".join(lines) + "\n"


def compare_summaries(
    old: Mapping[str, Any],
    plus: Mapping[str, Any],
    meal_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    old_label = "round28_plus30"
    plus_label = "round30_plus30_plus15"
    quality_worse = (
        int(plus.get("valid_day_count", 0) or 0) < int(old.get("valid_day_count", 0) or 0)
        or int(plus.get("accept_day_count", 0) or 0) < int(old.get("accept_day_count", 0) or 0)
    )
    variety_better = (
        int(plus.get("unique_recipe_count", 0) or 0) > int(old.get("unique_recipe_count", 0) or 0)
        or int(plus.get("repeated_recipe_count", 0) or 0) < int(old.get("repeated_recipe_count", 0) or 0)
        or repeated_main_count(repetition_rows, plus_label) < repeated_main_count(repetition_rows, old_label)
        or dominant_meal_count(meal_rows, plus_label) < dominant_meal_count(meal_rows, old_label)
    )
    return {
        "valid_day_delta": int(plus.get("valid_day_count", 0) or 0) - int(old.get("valid_day_count", 0) or 0),
        "accept_day_delta": int(plus.get("accept_day_count", 0) or 0) - int(old.get("accept_day_count", 0) or 0),
        "review_day_delta": int(plus.get("review_day_count", 0) or 0) - int(old.get("review_day_count", 0) or 0),
        "unique_recipe_delta": int(plus.get("unique_recipe_count", 0) or 0) - int(old.get("unique_recipe_count", 0) or 0),
        "repeated_recipe_delta": int(plus.get("repeated_recipe_count", 0) or 0) - int(old.get("repeated_recipe_count", 0) or 0),
        "repeated_main_recipe_delta": repeated_main_count(repetition_rows, plus_label) - repeated_main_count(repetition_rows, old_label),
        "dominant_recipe_meal_delta": dominant_meal_count(meal_rows, plus_label) - dominant_meal_count(meal_rows, old_label),
        "round30_added_recipe_meal_count": added_recipe_meal_count(meal_rows, plus_label),
        "plus15_improved_variety": variety_better,
        "plus15_destroyed_quality": quality_worse,
    }


def strict_conclusion(
    plus_summary: Mapping[str, Any],
    comparison: Mapping[str, Any],
) -> str:
    if comparison.get("plus15_destroyed_quality"):
        return "Plus15 is not acceptable as an improvement: it reduces daily quality."
    if comparison.get("round30_added_recipe_meal_count", 0) <= 0:
        return "Plus15 did not solve the bottleneck: no added recipe was selected."
    if comparison.get("plus15_improved_variety") and plus_summary.get("multi_day_classification") == "multi_day_good":
        return "Plus15 improves variety enough for draft testing, but it is still draft data."
    if comparison.get("plus15_improved_variety"):
        return "Plus15 improves variety partially, but the multi-day plan still needs review."
    return "Plus15 is inconclusive: added recipes exist, but the selector still concentrates the plan."


def selected_added_recipe_names(
    meal_rows: list[dict[str, Any]],
    dataset_label: str,
) -> list[str]:
    names = {
        str(row.get("display_name", "")).strip()
        for row in meal_rows
        if row.get("dataset_label") == dataset_label
        and bool(row.get("is_round30_added_recipe"))
    }
    return sorted(name for name in names if name)


def added_recipe_meal_count(meal_rows: list[dict[str, Any]], dataset_label: str) -> int:
    return sum(
        1
        for row in meal_rows
        if row.get("dataset_label") == dataset_label
        and bool(row.get("is_round30_added_recipe"))
    )


def dominant_meal_count(meal_rows: list[dict[str, Any]], dataset_label: str) -> int:
    return sum(
        1
        for row in meal_rows
        if row.get("dataset_label") == dataset_label
        and bool(row.get("is_dominant_recipe"))
    )


def repeated_main_count(rows: list[dict[str, Any]], dataset_label: str) -> int:
    return sum(
        1
        for row in rows
        if row.get("dataset_label") == dataset_label and bool(row.get("is_main_repeat"))
    )


def day_verdict(day: Mapping[str, Any]) -> str:
    if day.get("validation_status") != "valid":
        return "bad"
    if day.get("quality_gate_status") == "reject":
        return "bad"
    if day.get("quality_gate_status") == "review":
        return "needs review"
    if day.get("fallback_used"):
        return "needs review"
    flags = []
    for meal in day.get("selected_meals", []):
        if isinstance(meal, Mapping):
            flags.extend(reason_items(meal.get("meal_realism_flags")))
    if any(flag in flags for flag in {"breakfast_too_large", "snack_too_large"}):
        return "bad"
    if any(flag in flags for flag in {"low_protein_main", "low_carb_main"}):
        return "needs review"
    return "human-realistic"


def target_to_dict(target: NutritionTarget | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(target, NutritionTarget):
        return {
            "kcal": target.kcal,
            "protein_g": target.protein_g,
            "carbs_g": target.carbs_g,
            "fat_g": target.fat_g,
            "slot_targets": target.slot_targets,
        }
    return dict(target)


def reason_items(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    for separator in ("|", ";", ","):
        if separator in text:
            return [item.strip() for item in text.split(separator) if item.strip()]
    return [text]


def format_list(value: object) -> str:
    if not isinstance(value, (list, tuple, set)) or not value:
        return "none"
    return ";".join(str(item) for item in value)


def ratio(actual: object, target: object) -> float | None:
    target_value = to_float(target)
    if target_value <= 0:
        return None
    return round(to_float(actual) / target_value, 4)


def to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)


if __name__ == "__main__":
    main()

