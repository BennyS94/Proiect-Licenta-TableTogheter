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
from src.generator_v1.multi_day_selector import generate_multi_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_checkpoint2_multiday_v1_summary.txt"
OUT_DAYS = OUT_DIR / "generator_v1_checkpoint2_multiday_v1_days.csv"
OUT_MEALS = OUT_DIR / "generator_v1_checkpoint2_multiday_v1_meals.csv"
OUT_REPETITION = OUT_DIR / "generator_v1_checkpoint2_multiday_v1_repetition.csv"

DATASET_PROFILE = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE
PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"
QUALITY_GATE = "demo_safe"
DAYS = 3


def main() -> None:
    context = build_generation_context()
    plan = generate_multi_day_plan(
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
        },
    )
    target_data = target_to_dict(context["target"])
    day_rows = build_day_rows(plan, target_data)
    meal_rows = multi_day_meal_rows(plan)
    repetition_rows = build_repetition_rows(plan)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(day_rows).to_csv(OUT_DAYS, index=False)
    pd.DataFrame(meal_rows).to_csv(OUT_MEALS, index=False)
    pd.DataFrame(repetition_rows).to_csv(OUT_REPETITION, index=False)
    OUT_SUMMARY.write_text(
        build_summary(plan, day_rows, meal_rows, repetition_rows),
        encoding="utf-8",
    )

    summary = plan["multi_day_summary"]
    print("Generator v1 Checkpoint 2 multi-day draft evaluation written")
    print(f"summary={OUT_SUMMARY}")
    print(f"days={OUT_DAYS}")
    print(f"meals={OUT_MEALS}")
    print(f"repetition={OUT_REPETITION}")
    print(
        "result="
        f"days={summary.get('generated_day_count')} "
        f"valid={summary.get('valid_day_count')} "
        f"review={summary.get('review_day_count')} "
        f"fallback={summary.get('fallback_day_count')} "
        f"unique_recipes={summary.get('unique_recipe_count')} "
        f"repeated={summary.get('repeated_recipe_count')}"
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
        "filtered_candidate_count": len(filtered_candidates),
    }


def build_day_rows(
    plan: Mapping[str, Any],
    target: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        totals = day.get("day_totals", {})
        diagnostics = day.get("selector_diagnostics", {})
        rows.append(
            {
                "day_index": day.get("day_index"),
                "validation_status": day.get("validation_status"),
                "quality_gate_status": day.get("quality_gate_status"),
                "fallback_used": day.get("fallback_used"),
                "diversity_mode_used": day.get("diversity_mode_used"),
                "recent_recipe_count_used": day.get("recent_recipe_count_used"),
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


def build_repetition_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
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
                "recipe_id": recipe_id,
                "display_name": items[0].get("display_name"),
                "count": len(items),
                "days": _format_list([item.get("day_index") for item in items]),
                "slots": _format_list([item.get("slot") for item in items]),
            }
        )
    return rows


def build_summary(
    plan: Mapping[str, Any],
    day_rows: list[dict[str, Any]],
    meal_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
) -> str:
    summary = plan.get("multi_day_summary", {})
    validation = plan.get("multi_day_validation", {})
    risk_counts = Counter(row.get("verdict") for row in day_rows)
    usable_status = usable_for_draft_testing(plan)
    lines = [
        "Generator v1 Checkpoint 2 multi-day v1 draft evaluation",
        "",
        f"dataset_profile={DATASET_PROFILE}",
        "selection_mode=balanced_day",
        f"portion_policy={PORTION_POLICY}",
        f"meal_realism_mode={MEAL_REALISM_MODE}",
        f"quality_gate={QUALITY_GATE}",
        f"days_requested={DAYS}",
        "",
        f"generated_days_count={summary.get('generated_day_count')}",
        f"valid_days_count={summary.get('valid_day_count')}",
        f"review_days_count={summary.get('review_day_count')}",
        f"fallback_days_count={summary.get('fallback_day_count')}",
        f"total_recipe_count={summary.get('total_recipe_count')}",
        f"unique_recipe_count={summary.get('unique_recipe_count')}",
        f"repeated_recipe_count={summary.get('repeated_recipe_count')}",
        "repeated_recipe_ids=" + _format_list(summary.get("repeated_recipe_ids")),
        f"average_day_loss={summary.get('average_day_loss')}",
        "average_macro_ratios=" + str(summary.get("average_macro_ratios")),
        "validation_status=" + str(validation.get("validation_status")),
        "usable_for_draft_testing=" + usable_status,
        "",
        "Daily macro fit",
    ]
    for row in day_rows:
        lines.append(
            (
                f"day={row['day_index']} status={row['validation_status']} "
                f"gate={row['quality_gate_status']} fallback={row['fallback_used']} "
                f"kcal_ratio={row['kcal_ratio']} protein_ratio={row['protein_ratio']} "
                f"carbs_ratio={row['carbs_ratio']} fat_ratio={row['fat_ratio']} "
                f"adjusted_day_loss={row['adjusted_day_loss']} "
                f"verdict={row['verdict']}"
            )
        )

    lines.extend(
        [
            "",
            "Quality and realism risk counts",
            "verdict_counts=" + ", ".join(f"{key}:{value}" for key, value in risk_counts.items()),
            f"meal_rows={len(meal_rows)}",
            f"repetition_rows={len(repetition_rows)}",
            "",
            "Biggest risks",
        ]
    )
    lines.extend(biggest_risks(plan, day_rows, repetition_rows))
    lines.extend(
        [
            "",
            "Strict conclusion",
            strict_conclusion(plan, day_rows, repetition_rows),
        ]
    )
    return "\n".join(lines) + "\n"


def usable_for_draft_testing(plan: Mapping[str, Any]) -> str:
    summary = plan.get("multi_day_summary", {})
    validation = plan.get("multi_day_validation", {})
    if summary.get("generated_day_count") != DAYS:
        return "no"
    if validation.get("all_days_valid") is not True:
        return "no"
    if summary.get("fallback_day_count", 0) > 0:
        return "usable_only_with_fallback_warning"
    if summary.get("review_day_count", 0) > 0:
        return "usable_with_review"
    if summary.get("repeated_recipe_count", 0) > 0:
        return "usable_with_repetition_warning"
    return "yes"


def biggest_risks(
    plan: Mapping[str, Any],
    day_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
) -> list[str]:
    risks = []
    summary = plan.get("multi_day_summary", {})
    if summary.get("review_day_count", 0) > 0:
        risks.append("- quality gate returns review days; valid nutrition does not mean good menu realism.")
    if summary.get("fallback_day_count", 0) > 0:
        risks.append("- at least one day fell back to best, so variety is weak under the gate.")
    if repetition_rows:
        risks.append("- exact recipe repetition exists across days.")
    for row in day_rows:
        if row.get("carbs_ratio") is not None and float(row["carbs_ratio"]) < 0.75:
            risks.append("- at least one day remains carb-weak.")
            break
    if not risks:
        risks.append("- main remaining risk is draft Recipes_DB quality, not selector determinism.")
    return risks


def strict_conclusion(
    plan: Mapping[str, Any],
    day_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
) -> str:
    usable_status = usable_for_draft_testing(plan)
    if usable_status == "yes":
        return (
            "Usable pentru draft testing, dar nu product-grade: este un selector "
            "determinist simplu, fara optimizare de gospodarie, pret sau grocery."
        )
    if usable_status == "usable_with_review":
        return (
            "Usable doar pentru draft testing cu review manual: cel putin o zi "
            "este acceptabila tehnic, dar marcata de gate ca slaba sau discutabila."
        )
    if usable_status == "usable_only_with_fallback_warning":
        return (
            "Usable doar ca test de fallback: varietatea quality-safe nu este "
            "suficienta in toate zilele."
        )
    if repetition_rows:
        return (
            "Usable limitat: planul are retete repetate si nu dovedeste inca "
            "varietate multi-day buna."
        )
    return "Nu este suficient pentru draft testing fara reparatii."


def day_verdict(day: Mapping[str, Any]) -> str:
    if day.get("validation_status") != "valid":
        return "bad"
    if day.get("quality_gate_status") == "reject":
        return "bad"
    if day.get("quality_gate_status") == "review":
        return "needs review"
    if day.get("fallback_used"):
        return "needs review"
    meals = day.get("selected_meals", [])
    issue_flags = []
    for meal in meals:
        issue_flags.extend(_reason_items(meal.get("meal_realism_flags")))
    if any(flag in issue_flags for flag in {"breakfast_too_large", "snack_too_large"}):
        return "bad"
    if any(
        flag in issue_flags
        for flag in {"low_protein_main", "low_carb_main", "mostly_carb_meal"}
    ):
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
    if "|" in text:
        return [item.strip() for item in text.split("|") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    if ";" in text:
        return [item.strip() for item in text.split(";") if item.strip()]
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
