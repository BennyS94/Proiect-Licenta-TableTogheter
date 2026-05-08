from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
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
from src.generator_v1.day_selector_balanced import (
    compute_day_loss_for_plan,
    select_one_day_plan_balanced,
)
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target

try:
    from evaluate_generator_v1_round17_portion_policy import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        clean_text,
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
        round_number,
        scenario_overrides,
        serialize_reason,
        slot_candidates_by_slot,
        slot_order,
        target_to_dict,
        to_float,
    )


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round18_target_aware_warning_summary.txt"
OUT_DETAILS = OUT_DIR / "generator_v1_round18_target_aware_warning_details.csv"
OUT_PORTIONS = OUT_DIR / "generator_v1_round18_selected_portion_grams_audit.csv"

PORTION_POLICY = "target_aware"
SELECTION_MODE = "balanced_day"

DETAIL_COLUMNS = [
    "scenario_id",
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
    "portion_policy_reasons",
    "portion_policy_warnings",
    "warning_classification",
    "total_time_min",
    "effective_time_min_for_scoring",
    "validation_status",
    "day_loss",
]

PORTION_COLUMNS = [
    "slot",
    "recipe_id",
    "display_name",
    "recipe_kind",
    "selected_count",
    "scenario_ids",
    "max_portion_multiplier",
    "max_portion_grams_estimated",
    "max_kcal",
    "warning_count",
    "warning_classifications_json",
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

    detail_rows: list[dict[str, object]] = []
    plan_summaries: list[dict[str, object]] = []
    for overrides in scenario_overrides():
        scenario_id = clean_text(overrides["scenario_id"])
        profile = build_scenario_profile(base_profile, overrides)
        plan = run_scenario(
            scenario_id=scenario_id,
            profile=profile,
            pool=pool,
            fooddb=fooddb,
        )
        plan_summaries.append(plan["summary"])
        detail_rows.extend(plan["details"])

    portion_rows = build_portion_rows(detail_rows)
    write_csv(OUT_DETAILS, detail_rows, DETAIL_COLUMNS)
    write_csv(OUT_PORTIONS, portion_rows, PORTION_COLUMNS)
    OUT_SUMMARY.write_text(
        build_summary(detail_rows, portion_rows, plan_summaries),
        encoding="utf-8",
    )

    print("Generator v1 round18 target_aware warning audit written")
    print(f"selected_meals={len(detail_rows)}")
    print(f"warning_meals={sum(1 for row in detail_rows if clean_text(row.get('portion_policy_warnings')))}")
    print(f"unrealistic_portions={unrealistic_count(detail_rows)}")
    print(f"written_summary={OUT_SUMMARY}")


def run_scenario(
    scenario_id: str,
    profile: dict[str, Any],
    pool: object,
    fooddb: pd.DataFrame,
) -> dict[str, object]:
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
        portion_policy_mode=PORTION_POLICY,
    )
    slots = slot_order(target)
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=slot_candidates_by_slot(slot_candidates, slots),
        target=target,
        slot_order=slots,
        config={
            "return_alternatives": True,
            "alternative_count": 3,
            "diversity_mode": "none",
        },
    )
    plan["target"] = target_to_dict(target)
    plan["validation"] = validate_one_day_plan(plan, target)
    plan["day_loss_components"] = compute_day_loss_for_plan(
        plan.get("selected_meals", []),
        target,
    )
    return {
        "summary": build_plan_summary(scenario_id, target, plan),
        "details": build_detail_rows(scenario_id, plan),
    }


def build_plan_summary(
    scenario_id: str,
    target: NutritionTarget,
    plan: dict[str, object],
) -> dict[str, object]:
    totals = plan.get("day_totals", {})
    validation = plan.get("validation", {})
    loss = plan.get("day_loss_components", {})
    return {
        "scenario_id": scenario_id,
        "validation_status": clean_text(validation.get("validation_status")),
        "target_kcal": round_number(target.kcal),
        "target_carbs_g": round_number(target.carbs_g),
        "selected_kcal": round_number(totals.get("total_kcal")),
        "selected_carbs_g": round_number(totals.get("total_carbs_g")),
        "day_loss": round_number(loss.get("day_loss")),
    }


def build_detail_rows(
    scenario_id: str,
    plan: dict[str, object],
) -> list[dict[str, object]]:
    validation = plan.get("validation", {})
    loss = plan.get("day_loss_components", {})
    rows: list[dict[str, object]] = []
    for meal in plan.get("selected_meals", []):
        rows.append(
            {
                "scenario_id": scenario_id,
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
                "portion_policy_reasons": serialize_reason(meal.get("portion_policy_reasons")),
                "portion_policy_warnings": serialize_reason(meal.get("portion_policy_warnings")),
                "warning_classification": classify_warning(meal),
                "total_time_min": round_number(meal.get("total_time_min")),
                "effective_time_min_for_scoring": round_number(
                    meal.get("effective_time_min_for_scoring")
                ),
                "validation_status": clean_text(validation.get("validation_status")),
                "day_loss": round_number(loss.get("day_loss")),
            }
        )
    return rows


def classify_warning(meal: dict[str, object]) -> str:
    slot = clean_text(meal.get("slot"))
    grams = to_float(meal.get("portion_grams_estimated"))
    kcal = to_float(meal.get("kcal"))
    multiplier = to_float(meal.get("portion_multiplier"))
    warning_text = serialize_reason(meal.get("portion_policy_warnings"))
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
    cap = slot_cap(slot)
    if warning_text or (cap and grams >= cap * 0.85) or multiplier >= 1.6:
        return "borderline_large_portion"
    return "acceptable_large_portion"


def slot_cap(slot: str) -> float:
    if slot == "snack":
        return 300.0
    if slot == "breakfast":
        return 500.0
    if slot in {"lunch", "dinner"}:
        return 850.0
    return 0.0


def build_portion_rows(detail_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in detail_rows:
        grouped[(clean_text(row.get("slot")), clean_text(row.get("recipe_id")))].append(row)

    rows = []
    for (slot, recipe_id), group in sorted(grouped.items()):
        classifications = Counter(clean_text(row.get("warning_classification")) for row in group)
        rows.append(
            {
                "slot": slot,
                "recipe_id": recipe_id,
                "display_name": clean_text(group[0].get("display_name")),
                "recipe_kind": clean_text(group[0].get("recipe_kind")),
                "selected_count": str(len(group)),
                "scenario_ids": "|".join(clean_text(row.get("scenario_id")) for row in group),
                "max_portion_multiplier": round_number(
                    max(to_float(row.get("portion_multiplier")) for row in group)
                ),
                "max_portion_grams_estimated": round_number(
                    max(to_float(row.get("portion_grams_estimated")) for row in group)
                ),
                "max_kcal": round_number(max(to_float(row.get("kcal")) for row in group)),
                "warning_count": str(
                    sum(1 for row in group if clean_text(row.get("portion_policy_warnings")))
                ),
                "warning_classifications_json": json.dumps(classifications, sort_keys=True),
            }
        )
    return rows


def build_summary(
    detail_rows: list[dict[str, object]],
    portion_rows: list[dict[str, object]],
    plan_summaries: list[dict[str, object]],
) -> str:
    scenario_count = len(plan_summaries)
    valid_count = sum(
        1 for row in plan_summaries if clean_text(row.get("validation_status")) == "valid"
    )
    warning_count = sum(
        1 for row in detail_rows if clean_text(row.get("portion_policy_warnings"))
    )
    classification_counts = Counter(
        clean_text(row.get("warning_classification")) for row in detail_rows
    )
    max_by_slot = max_portion_by_slot(detail_rows)
    unrealistic = unrealistic_count(detail_rows)
    target_aware_recommended = (
        valid_count == scenario_count
        and unrealistic == 0
        and classification_counts.get("snack_too_large", 0) == 0
        and classification_counts.get("breakfast_too_large", 0) == 0
        and classification_counts.get("main_too_large", 0) == 0
    )
    lines = [
        "Generator v1 round18 target_aware warning audit",
        "=" * 54,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        f"selection_mode: {SELECTION_MODE}",
        f"portion_policy: {PORTION_POLICY}",
        f"scenario_count: {scenario_count}",
        f"valid_scenarios: {valid_count}/{scenario_count}",
        f"selected_meal_count: {len(detail_rows)}",
        f"selected_meals_with_policy_warnings: {warning_count}",
        "",
        "Warning classifications:",
    ]
    for name, count in sorted(classification_counts.items()):
        lines.append(f"- {name}: {count}")
    lines.extend(["", "Max selected portion by slot:"])
    for slot, metrics in sorted(max_by_slot.items()):
        lines.append(
            "- "
            + slot
            + " | max_grams="
            + round_number(metrics["max_grams"])
            + " | max_multiplier="
            + round_number(metrics["max_multiplier"])
            + " | max_kcal="
            + round_number(metrics["max_kcal"])
        )
    lines.extend(
        [
            "",
            f"unrealistic_or_too_large_portions: {unrealistic}",
            f"target_aware_should_stay_recommended: {target_aware_recommended}",
            "",
            "Interpretation:",
            "- target_aware ramane recomandat daca nu apar portii nerealiste si toate scenariile raman valide.",
            "- warning-urile ramase marcheaza portii mari sau aproape de cap, nu incalcari de cap.",
            "",
            "Output files:",
            f"- {OUT_DETAILS}",
            f"- {OUT_PORTIONS}",
        ]
    )
    if portion_rows:
        lines.extend(["", "Most repeated selected portions:"])
        for row in sorted(
            portion_rows,
            key=lambda item: (-int(to_float(item.get("selected_count"))), str(item.get("recipe_id"))),
        )[:8]:
            lines.append(
                "- "
                + clean_text(row.get("recipe_id"))
                + " | "
                + clean_text(row.get("display_name"))
                + " | selected_count="
                + clean_text(row.get("selected_count"))
                + " | max_grams="
                + clean_text(row.get("max_portion_grams_estimated"))
            )
    return "\n".join(lines) + "\n"


def max_portion_by_slot(detail_rows: list[dict[str, object]]) -> dict[str, dict[str, float]]:
    values: dict[str, dict[str, float]] = {}
    for row in detail_rows:
        slot = clean_text(row.get("slot"))
        current = values.setdefault(
            slot,
            {"max_grams": 0.0, "max_multiplier": 0.0, "max_kcal": 0.0},
        )
        current["max_grams"] = max(current["max_grams"], to_float(row.get("portion_grams_estimated")))
        current["max_multiplier"] = max(current["max_multiplier"], to_float(row.get("portion_multiplier")))
        current["max_kcal"] = max(current["max_kcal"], to_float(row.get("kcal")))
    return values


def unrealistic_count(detail_rows: list[dict[str, object]]) -> int:
    bad_classes = {
        "unrealistic_portion",
        "snack_too_large",
        "breakfast_too_large",
        "main_too_large",
    }
    return sum(
        1
        for row in detail_rows
        if clean_text(row.get("warning_classification")) in bad_classes
    )


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
