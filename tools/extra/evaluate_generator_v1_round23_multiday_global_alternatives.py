from __future__ import annotations

import sys
from collections import Counter
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
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.multi_day_audit import multi_day_meal_rows
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    MULTI_DAY_MODE_SIMPLE,
    generate_multi_day_plan,
)
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round23_multiday_global_summary.txt"
OUT_DAYS = OUT_DIR / "generator_v1_round23_multiday_global_days.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round23_multiday_global_meals.csv"
OUT_REPETITION = OUT_DIR / "generator_v1_round23_multiday_global_repetition.csv"
OUT_CANDIDATES = OUT_DIR / "generator_v1_round23_multiday_global_candidate_pool.csv"

DATASET_PROFILE = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE
PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"
QUALITY_GATE = "demo_safe"
DAYS = 3


def main() -> None:
    context = build_generation_context()
    target_data = target_to_dict(context["target"])
    plans = [
        (
            MULTI_DAY_MODE_SIMPLE,
            generate_plan(context, MULTI_DAY_MODE_SIMPLE),
        ),
        (
            MULTI_DAY_MODE_GLOBAL,
            generate_plan(context, MULTI_DAY_MODE_GLOBAL),
        ),
    ]

    day_rows = []
    meal_rows = []
    repetition_rows = []
    candidate_rows = []
    for mode, plan in plans:
        day_rows.extend(build_day_rows(mode, plan, target_data))
        meal_rows.extend(add_mode(mode, multi_day_meal_rows(plan)))
        repetition_rows.extend(build_repetition_rows(mode, plan))
        candidate_rows.extend(build_candidate_rows(mode, plan))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(day_rows).to_csv(OUT_DAYS, index=False)
    pd.DataFrame(meal_rows).to_csv(OUT_MEALS, index=False)
    pd.DataFrame(repetition_rows).to_csv(OUT_REPETITION, index=False)
    pd.DataFrame(candidate_rows).to_csv(OUT_CANDIDATES, index=False)
    OUT_SUMMARY.write_text(
        build_summary(plans, day_rows, repetition_rows),
        encoding="utf-8",
    )

    simple = plans[0][1]["multi_day_summary"]
    global_summary = plans[1][1]["multi_day_summary"]
    print("Generator v1 Round23 multi-day global alternatives evaluation written")
    print(f"summary={OUT_SUMMARY}")
    print(f"days={OUT_DAYS}")
    print(f"meals={OUT_MEALS}")
    print(f"repetition={OUT_REPETITION}")
    print(f"candidate_pool={OUT_CANDIDATES}")
    print(
        "simple="
        f"valid:{simple.get('valid_day_count')} "
        f"accept:{simple.get('accept_day_count')} "
        f"review:{simple.get('review_day_count')} "
        f"unique:{simple.get('unique_recipe_count')} "
        f"repeated:{simple.get('repeated_recipe_count')} "
        f"loss:{simple.get('multi_day_loss')}"
    )
    print(
        "global="
        f"valid:{global_summary.get('valid_day_count')} "
        f"accept:{global_summary.get('accept_day_count')} "
        f"review:{global_summary.get('review_day_count')} "
        f"unique:{global_summary.get('unique_recipe_count')} "
        f"repeated:{global_summary.get('repeated_recipe_count')} "
        f"loss:{global_summary.get('multi_day_loss')}"
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
    return {
        "profile": profile,
        "target": target,
        "pool": pool,
        "slot_candidates": slot_candidates,
    }


def generate_plan(context: Mapping[str, Any], mode: str) -> dict[str, Any]:
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
            "multi_day_mode": mode,
            "candidate_day_alternative_count": 10,
            "global_max_candidates_per_slot": 26,
        },
    )


def build_day_rows(
    mode: str,
    plan: Mapping[str, Any],
    target: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        totals = day.get("day_totals", {})
        diagnostics = day.get("selector_diagnostics", {})
        rows.append(
            {
                "multi_day_mode": mode,
                "day_index": day.get("day_index"),
                "validation_status": day.get("validation_status"),
                "quality_gate_status": day.get("quality_gate_status"),
                "fallback_used": day.get("fallback_used"),
                "diversity_mode_used": day.get("diversity_mode_used"),
                "candidate_day_id": day.get("candidate_day_id"),
                "repeated_recipe_ids_vs_previous_days": _format_list(
                    day.get("repeated_recipe_ids_vs_previous_days")
                ),
                "base_day_loss": diagnostics.get("base_day_loss"),
                "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
                "total_kcal": totals.get("total_kcal"),
                "kcal_ratio": _ratio(totals.get("total_kcal"), target.get("kcal")),
                "total_protein_g": totals.get("total_protein_g"),
                "protein_ratio": _ratio(
                    totals.get("total_protein_g"),
                    target.get("protein_g"),
                ),
                "total_carbs_g": totals.get("total_carbs_g"),
                "carbs_ratio": _ratio(
                    totals.get("total_carbs_g"),
                    target.get("carbs_g"),
                ),
                "total_fat_g": totals.get("total_fat_g"),
                "fat_ratio": _ratio(totals.get("total_fat_g"), target.get("fat_g")),
                "effective_time_min_sum": totals.get("effective_time_min_sum"),
                "quality_gate_reasons": _format_list(day.get("quality_gate_reasons")),
                "warnings": _format_list(day.get("warnings")),
                "verdict": day_verdict(day),
            }
        )
    return rows


def build_repetition_rows(
    mode: str,
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
        rows.append(
            {
                "multi_day_mode": mode,
                "recipe_id": recipe_id,
                "display_name": items[0].get("display_name"),
                "count": len(items),
                "days": _format_list([item.get("day_index") for item in items]),
                "slots": _format_list([item.get("slot") for item in items]),
                "is_main_repeat": any(
                    str(item.get("slot")) in {"lunch", "dinner"} for item in items
                ),
                "is_breakfast_repeat": any(
                    str(item.get("slot")) == "breakfast" for item in items
                ),
            }
        )
    return rows


def build_candidate_rows(
    mode: str,
    plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for row in plan.get("candidate_day_pool", []):
        if not isinstance(row, Mapping):
            continue
        rows.append({"multi_day_mode": mode, **dict(row)})
    if rows:
        return rows
    summary = plan.get("multi_day_summary", {})
    return [
        {
            "multi_day_mode": mode,
            "candidate_day_id": "not_available",
            "source_mode": mode,
            "validation_status": "not_available",
            "quality_gate_status": "not_available",
            "base_day_loss": summary.get("average_day_loss"),
            "adjusted_day_loss": summary.get("average_day_loss"),
        }
    ]


def add_mode(mode: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"multi_day_mode": mode, **row} for row in rows]


def build_summary(
    plans: list[tuple[str, Mapping[str, Any]]],
    day_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
) -> str:
    summary_by_mode = {
        mode: plan.get("multi_day_summary", {}) for mode, plan in plans
    }
    simple = summary_by_mode[MULTI_DAY_MODE_SIMPLE]
    global_summary = summary_by_mode[MULTI_DAY_MODE_GLOBAL]
    comparison = compare_summaries(simple, global_summary, repetition_rows)
    lines = [
        "Generator v1 Round23 multi-day global alternatives evaluation",
        "",
        f"dataset_profile={DATASET_PROFILE}",
        "selection_mode=balanced_day",
        f"portion_policy={PORTION_POLICY}",
        f"meal_realism_mode={MEAL_REALISM_MODE}",
        f"quality_gate={QUALITY_GATE}",
        f"days_requested={DAYS}",
        "",
        "Mode comparison",
    ]
    for mode, summary in summary_by_mode.items():
        lines.extend(
            [
                f"mode={mode}",
                f"generated_days_count={summary.get('generated_day_count')}",
                f"valid_days_count={summary.get('valid_day_count')}",
                f"accept_days_count={summary.get('accept_day_count')}",
                f"review_days_count={summary.get('review_day_count')}",
                f"reject_days_count={summary.get('reject_day_count')}",
                f"fallback_days_count={summary.get('fallback_day_count')}",
                f"average_day_loss={summary.get('average_day_loss')}",
                f"multi_day_loss={summary.get('multi_day_loss')}",
                f"unique_recipe_count={summary.get('unique_recipe_count')}",
                f"repeated_recipe_count={summary.get('repeated_recipe_count')}",
                "repeated_recipe_ids="
                + _format_list(summary.get("repeated_recipe_ids")),
                f"strict_assessment={summary.get('multi_day_classification')}",
                "",
            ]
        )
    lines.extend(
        [
            "Global candidate pool",
            str(global_summary.get("candidate_day_pool_summary", {})),
            "",
            "Simple vs global",
            f"valid_day_delta={comparison['valid_day_delta']}",
            f"accept_day_delta={comparison['accept_day_delta']}",
            f"review_day_delta={comparison['review_day_delta']}",
            f"unique_recipe_delta={comparison['unique_recipe_delta']}",
            f"repeated_recipe_delta={comparison['repeated_recipe_delta']}",
            f"repeated_main_recipe_delta={comparison['repeated_main_recipe_delta']}",
            f"repeated_breakfast_delta={comparison['repeated_breakfast_delta']}",
            f"global_improved_variety={comparison['global_improved_variety']}",
            f"global_destroyed_quality={comparison['global_destroyed_quality']}",
            "",
            "Daily macro fit",
        ]
    )
    for row in day_rows:
        lines.append(
            (
                f"mode={row['multi_day_mode']} day={row['day_index']} "
                f"status={row['validation_status']} gate={row['quality_gate_status']} "
                f"kcal_ratio={row['kcal_ratio']} protein_ratio={row['protein_ratio']} "
                f"carbs_ratio={row['carbs_ratio']} fat_ratio={row['fat_ratio']} "
                f"adjusted_day_loss={row['adjusted_day_loss']} verdict={row['verdict']}"
            )
        )
    lines.extend(
        [
            "",
            "Strict conclusion",
            strict_conclusion(simple, global_summary, comparison),
            "",
            "Biggest risks",
        ]
    )
    lines.extend(biggest_risks(global_summary, repetition_rows))
    return "\n".join(lines) + "\n"


def compare_summaries(
    simple: Mapping[str, Any],
    global_summary: Mapping[str, Any],
    repetition_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    simple_main = repeated_main_count(repetition_rows, MULTI_DAY_MODE_SIMPLE)
    global_main = repeated_main_count(repetition_rows, MULTI_DAY_MODE_GLOBAL)
    simple_breakfast = repeated_breakfast_count(repetition_rows, MULTI_DAY_MODE_SIMPLE)
    global_breakfast = repeated_breakfast_count(repetition_rows, MULTI_DAY_MODE_GLOBAL)
    quality_worse = (
        int(global_summary.get("valid_day_count", 0) or 0)
        < int(simple.get("valid_day_count", 0) or 0)
        or int(global_summary.get("accept_day_count", 0) or 0)
        < int(simple.get("accept_day_count", 0) or 0)
    )
    variety_better = (
        int(global_summary.get("unique_recipe_count", 0) or 0)
        > int(simple.get("unique_recipe_count", 0) or 0)
        or int(global_summary.get("repeated_recipe_count", 0) or 0)
        < int(simple.get("repeated_recipe_count", 0) or 0)
        or global_main < simple_main
    )
    return {
        "valid_day_delta": int(global_summary.get("valid_day_count", 0) or 0)
        - int(simple.get("valid_day_count", 0) or 0),
        "accept_day_delta": int(global_summary.get("accept_day_count", 0) or 0)
        - int(simple.get("accept_day_count", 0) or 0),
        "review_day_delta": int(global_summary.get("review_day_count", 0) or 0)
        - int(simple.get("review_day_count", 0) or 0),
        "unique_recipe_delta": int(global_summary.get("unique_recipe_count", 0) or 0)
        - int(simple.get("unique_recipe_count", 0) or 0),
        "repeated_recipe_delta": int(global_summary.get("repeated_recipe_count", 0) or 0)
        - int(simple.get("repeated_recipe_count", 0) or 0),
        "repeated_main_recipe_delta": global_main - simple_main,
        "repeated_breakfast_delta": global_breakfast - simple_breakfast,
        "global_improved_variety": variety_better,
        "global_destroyed_quality": quality_worse,
    }


def repeated_main_count(rows: list[dict[str, Any]], mode: str) -> int:
    return sum(
        1
        for row in rows
        if row.get("multi_day_mode") == mode and bool(row.get("is_main_repeat"))
    )


def repeated_breakfast_count(rows: list[dict[str, Any]], mode: str) -> int:
    return sum(
        1
        for row in rows
        if row.get("multi_day_mode") == mode and bool(row.get("is_breakfast_repeat"))
    )


def strict_conclusion(
    simple: Mapping[str, Any],
    global_summary: Mapping[str, Any],
    comparison: Mapping[str, Any],
) -> str:
    if comparison.get("global_destroyed_quality"):
        return (
            "Global alternatives improves nothing safely enough: it loses quality "
            "relative to simple_3_day."
        )
    if comparison.get("global_improved_variety"):
        if global_summary.get("multi_day_classification") == "multi_day_good":
            return (
                "Global alternatives improves variety without breaking daily quality, "
                "but it is still a draft selector."
            )
        return (
            "Global alternatives improves variety, but the result still needs review "
            "because quality or repetition risk remains."
        )
    if simple.get("repeated_recipe_count") == global_summary.get("repeated_recipe_count"):
        return (
            "Global alternatives does not materially improve variety on this pool; "
            "the candidate set is still too narrow."
        )
    return "Global alternatives is inconclusive and needs more scenario testing."


def biggest_risks(
    global_summary: Mapping[str, Any],
    repetition_rows: list[dict[str, Any]],
) -> list[str]:
    risks = []
    if int(global_summary.get("review_day_count", 0) or 0) > 0:
        risks.append("- global mode still returns review days.")
    if int(global_summary.get("fallback_day_count", 0) or 0) > 0:
        risks.append("- fallback days still appear; diverse safe alternatives are insufficient.")
    if repeated_main_count(repetition_rows, MULTI_DAY_MODE_GLOBAL) > 0:
        risks.append("- at least one lunch/dinner recipe repeats.")
    if int(global_summary.get("repeated_recipe_count", 0) or 0) > 1:
        risks.append("- exact recipe repetition is still too high for a good multi-day plan.")
    if not risks:
        risks.append("- remaining risk is limited scenario coverage and draft Recipes_DB quality.")
    return risks


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
        if not isinstance(meal, Mapping):
            continue
        flags.extend(_reason_items(meal.get("meal_realism_flags")))
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


def _reason_items(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    for separator in ("|", ";", ","):
        if separator in text:
            return [item.strip() for item in text.split(separator) if item.strip()]
    return [text]


def _format_list(value: object) -> str:
    if not isinstance(value, (list, tuple, set)) or not value:
        return "none"
    return ";".join(str(item) for item in value)


def _ratio(actual: object, target: object) -> float | None:
    target_value = _to_float(target)
    if target_value <= 0:
        return None
    return round(_to_float(actual) / target_value, 4)


def _to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)


if __name__ == "__main__":
    main()
