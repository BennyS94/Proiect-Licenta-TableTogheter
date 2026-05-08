from __future__ import annotations

import copy
import csv
import json
import math
import sys
from collections import Counter
from pathlib import Path
from typing import Any


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
from src.generator_v1.day_selector import select_one_day_plan
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


BASE_PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"

OUT_SUMMARY = OUT_DIR / "generator_v1_round13_scenario_eval_summary.txt"
OUT_PLANS = OUT_DIR / "generator_v1_round13_scenario_eval_plans.csv"
OUT_MEALS = OUT_DIR / "generator_v1_round13_scenario_eval_meals.csv"
OUT_REPETITION = OUT_DIR / "generator_v1_round13_scenario_eval_recipe_repetition.csv"
OUT_MACRO_GAPS = OUT_DIR / "generator_v1_round13_scenario_eval_macro_gaps.csv"
OUT_SLOT_COUNTS = OUT_DIR / "generator_v1_round13_scenario_eval_slot_candidate_counts.csv"

PLAN_COLUMNS = [
    "scenario_id",
    "profile_name",
    "age",
    "sex",
    "weight_kg",
    "height_cm",
    "goal",
    "goal_speed",
    "activity_level",
    "training_type",
    "training_sessions_per_week",
    "include_snacks",
    "meals_per_day",
    "target_kcal",
    "target_protein_g",
    "target_carbs_g",
    "target_fat_g",
    "selected_total_kcal",
    "selected_total_protein_g",
    "selected_total_carbs_g",
    "selected_total_fat_g",
    "kcal_gap_abs",
    "kcal_gap_pct",
    "protein_gap_abs",
    "protein_gap_pct",
    "carbs_gap_abs",
    "carbs_gap_pct",
    "fat_gap_abs",
    "fat_gap_pct",
    "validation_status",
    "is_valid_for_checkpoint_1",
    "selected_slot_count",
    "expected_slot_count",
    "total_time_min_sum",
    "effective_time_min_sum",
    "passive_time_estimated_sum",
    "eligible_candidate_count",
    "filtered_candidate_count",
    "slot_candidate_counts_json",
    "warnings",
    "macro_gap_classes_json",
]

MEAL_COLUMNS = [
    "scenario_id",
    "slot",
    "recipe_id",
    "display_name",
    "recipe_kind",
    "recipe_category",
    "recipe_subcategory",
    "portion_multiplier",
    "portion_grams_estimated",
    "kcal",
    "protein_g",
    "carbs_g",
    "fat_g",
    "score_preview",
    "macro_fit",
    "time_fit",
    "slot_fit",
    "nutrition_quality",
    "total_time_min",
    "effective_time_min_for_scoring",
    "allowed_slots_json",
    "is_slot_suspicious",
    "is_nutrition_suspicious",
    "slot_fit_reasons",
    "slot_suspicion_reasons",
    "nutrition_quality_reasons",
    "time_estimation_reasons",
    "slot_quality_status",
    "slot_quality_notes",
]

MACRO_GAP_COLUMNS = [
    "scenario_id",
    "validation_status",
    "target_kcal",
    "selected_total_kcal",
    "kcal_ratio",
    "protein_ratio",
    "carbs_ratio",
    "fat_ratio",
    "macro_gap_classes_json",
    "good_fit",
    "protein_heavy",
    "carb_deficient",
    "fat_heavy",
    "primary_macro_issue",
]

REPETITION_COLUMNS = [
    "recipe_id",
    "display_name",
    "recipe_kind",
    "selected_total_count",
    "breakfast_count",
    "lunch_count",
    "dinner_count",
    "snack_count",
    "scenario_ids",
    "dominance_note",
]

SLOT_COUNT_COLUMNS = [
    "scenario_id",
    "slot",
    "slot_candidate_count",
    "unique_recipe_count",
    "suspicious_count",
    "non_suspicious_count",
    "median_kcal",
    "median_protein_g",
    "best_score_preview",
    "best_macro_fit",
]


def scenario_overrides() -> list[dict[str, Any]]:
    return [
        {
            "scenario_id": "demo_profile_existing",
            "profile_name": "Demo Member Existing",
        },
        {
            "scenario_id": "male_maintain_moderate_weights",
            "profile_name": "Male Maintain Moderate Weights",
            "age": 35,
            "sex": "male",
            "weight_kg": 82.0,
            "height_cm": 180.0,
            "activity_level": "moderately_active",
            "goal": "maintain",
            "goal_speed": "normal",
            "training": {"type": "weights", "sessions_per_week": 4},
            "meal_config": {"meals_per_day": 3, "include_snacks": True, "day_structure": "3_meals_plus_snack"},
        },
        {
            "scenario_id": "male_lose_normal_light_activity",
            "profile_name": "Male Lose Normal Light Activity",
            "age": 42,
            "sex": "male",
            "weight_kg": 95.0,
            "height_cm": 178.0,
            "activity_level": "lightly_active",
            "goal": "lose",
            "goal_speed": "normal",
            "training": {"type": "cardio", "sessions_per_week": 2},
            "meal_config": {"meals_per_day": 3, "include_snacks": True, "day_structure": "3_meals_plus_snack"},
        },
        {
            "scenario_id": "male_gain_normal_weights",
            "profile_name": "Male Gain Normal Weights",
            "age": 28,
            "sex": "male",
            "weight_kg": 75.0,
            "height_cm": 183.0,
            "activity_level": "moderately_active",
            "goal": "gain",
            "goal_speed": "normal",
            "training": {"type": "weights", "sessions_per_week": 5},
            "meal_config": {"meals_per_day": 3, "include_snacks": True, "day_structure": "3_meals_plus_snack"},
        },
        {
            "scenario_id": "female_maintain_light_activity",
            "profile_name": "Female Maintain Light Activity",
            "age": 34,
            "sex": "female",
            "weight_kg": 63.0,
            "height_cm": 165.0,
            "activity_level": "lightly_active",
            "goal": "maintain",
            "goal_speed": "normal",
            "training": {"type": "yoga", "sessions_per_week": 2},
            "meal_config": {"meals_per_day": 3, "include_snacks": True, "day_structure": "3_meals_plus_snack"},
        },
        {
            "scenario_id": "female_lose_slow_no_snack",
            "profile_name": "Female Lose Slow No Snack",
            "age": 30,
            "sex": "female",
            "weight_kg": 72.0,
            "height_cm": 168.0,
            "activity_level": "lightly_active",
            "goal": "lose",
            "goal_speed": "slow",
            "training": {"type": "none", "sessions_per_week": 0},
            "meal_config": {"meals_per_day": 3, "include_snacks": False, "day_structure": "3_meals"},
        },
        {
            "scenario_id": "female_gain_slow_moderate_activity",
            "profile_name": "Female Gain Slow Moderate Activity",
            "age": 25,
            "sex": "female",
            "weight_kg": 55.0,
            "height_cm": 165.0,
            "activity_level": "moderately_active",
            "goal": "gain",
            "goal_speed": "slow",
            "training": {"type": "weights", "sessions_per_week": 3},
            "meal_config": {"meals_per_day": 3, "include_snacks": True, "day_structure": "3_meals_plus_snack"},
        },
        {
            "scenario_id": "high_activity_mixed_training",
            "profile_name": "High Activity Mixed Training",
            "age": 31,
            "sex": "male",
            "weight_kg": 78.0,
            "height_cm": 181.0,
            "activity_level": "very_active",
            "goal": "maintain",
            "goal_speed": "normal",
            "training": {"type": "mixed", "sessions_per_week": 6},
            "meal_config": {"meals_per_day": 3, "include_snacks": True, "day_structure": "3_meals_plus_snack"},
        },
        {
            "scenario_id": "sedentary_lose_fast_with_snack",
            "profile_name": "Sedentary Lose Fast With Snack",
            "age": 50,
            "sex": "female",
            "weight_kg": 88.0,
            "height_cm": 162.0,
            "activity_level": "sedentary",
            "goal": "lose",
            "goal_speed": "fast",
            "training": {"type": "none", "sessions_per_week": 0},
            "meal_config": {"meals_per_day": 3, "include_snacks": True, "day_structure": "3_meals_plus_snack"},
        },
        {
            "scenario_id": "three_meals_no_snack_profile",
            "profile_name": "Three Meals No Snack Profile",
            "age": 45,
            "sex": "male",
            "weight_kg": 80.0,
            "height_cm": 175.0,
            "activity_level": "moderately_active",
            "goal": "maintain",
            "goal_speed": "normal",
            "training": {"type": "weights", "sessions_per_week": 3},
            "meal_config": {"meals_per_day": 3, "include_snacks": False, "day_structure": "3_meals"},
        },
    ]


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    return str(value or "").strip()


def to_float(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(parsed) or math.isinf(parsed):
        return 0.0
    return parsed


def round_number(value: object) -> str:
    numeric = to_float(value)
    text = f"{numeric:.4f}".rstrip("0").rstrip(".")
    return text or "0"


def target_to_dict(target: NutritionTarget) -> dict[str, object]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def build_scenario_profile(base_profile: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    profile = copy.deepcopy(base_profile)
    for key, value in overrides.items():
        if key == "scenario_id":
            continue
        profile[key] = copy.deepcopy(value)
    profile["member_profile_id"] = overrides["scenario_id"]
    profile["profile_name"] = overrides.get("profile_name", overrides["scenario_id"])
    return profile


def run_scenario(
    scenario_id: str,
    profile: dict[str, Any],
    fooddb: Any,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
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
    plan = select_one_day_plan(
        slot_candidates_by_slot=slot_candidates_by_slot(slot_candidates, slot_order(target)),
        slot_order=slot_order(target),
    )
    plan["target"] = target_to_dict(target)
    plan["validation"] = validate_one_day_plan(plan, target)

    candidate_lookup = build_candidate_lookup(slot_candidates)
    plan_row = build_plan_row(
        scenario_id=scenario_id,
        profile=profile,
        target=target,
        plan=plan,
        eligible_count=len(pool.eligible_candidates),
        filtered_count=len(filtered_candidates),
        slot_counts=slot_candidate_counts(slot_candidates),
    )
    meal_rows = build_meal_rows(scenario_id, plan, candidate_lookup)
    macro_gap_rows = [build_macro_gap_row(plan_row)]
    slot_count_rows = build_slot_count_rows(scenario_id, slot_candidates)
    return plan_row, meal_rows, macro_gap_rows, slot_count_rows


def slot_order(target: NutritionTarget) -> list[str]:
    return list(target.slot_targets.keys())


def slot_candidates_by_slot(slot_candidates: Any, slots: list[str]) -> dict[str, Any]:
    return {
        slot: slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        for slot in slots
    }


def build_candidate_lookup(slot_candidates: Any) -> dict[tuple[str, str, float], dict[str, object]]:
    lookup: dict[tuple[str, str, float], dict[str, object]] = {}
    for _, row in slot_candidates.iterrows():
        key = (
            clean_text(row.get("slot")),
            clean_text(row.get("recipe_id")),
            round(to_float(row.get("portion_multiplier")), 4),
        )
        lookup[key] = row.to_dict()
    return lookup


def slot_candidate_counts(slot_candidates: Any) -> dict[str, int]:
    if slot_candidates.empty:
        return {}
    return {
        str(slot): int(count)
        for slot, count in slot_candidates.groupby("slot", sort=False).size().items()
    }


def ratio(actual: object, target: object) -> float:
    target_float = to_float(target)
    if target_float <= 0:
        return 0.0
    return to_float(actual) / target_float


def pct_gap(actual: object, target: object) -> float:
    return ratio(actual, target) - 1.0


def build_plan_row(
    scenario_id: str,
    profile: dict[str, Any],
    target: NutritionTarget,
    plan: dict[str, Any],
    eligible_count: int,
    filtered_count: int,
    slot_counts: dict[str, int],
) -> dict[str, object]:
    totals = plan.get("day_totals", {})
    validation = plan.get("validation", {})
    warnings = list(plan.get("warnings", [])) + list(validation.get("validation_warnings", []))
    target_data = target_to_dict(target)
    macro_classes = classify_macro_gaps(
        target_data,
        totals,
        str(validation.get("validation_status", "")),
    )
    training = profile.get("training") or {}
    meal_config = profile.get("meal_config") or {}
    row = {
        "scenario_id": scenario_id,
        "profile_name": clean_text(profile.get("profile_name")),
        "age": clean_text(profile.get("age")),
        "sex": clean_text(profile.get("sex")),
        "weight_kg": clean_text(profile.get("weight_kg")),
        "height_cm": clean_text(profile.get("height_cm")),
        "goal": clean_text(profile.get("goal")),
        "goal_speed": clean_text(profile.get("goal_speed")),
        "activity_level": clean_text(profile.get("activity_level")),
        "training_type": clean_text(training.get("type")),
        "training_sessions_per_week": clean_text(training.get("sessions_per_week")),
        "include_snacks": str(bool(meal_config.get("include_snacks", False))),
        "meals_per_day": clean_text(meal_config.get("meals_per_day")),
        "target_kcal": target.kcal,
        "target_protein_g": target.protein_g,
        "target_carbs_g": target.carbs_g,
        "target_fat_g": target.fat_g,
        "selected_total_kcal": totals.get("total_kcal", 0),
        "selected_total_protein_g": totals.get("total_protein_g", 0),
        "selected_total_carbs_g": totals.get("total_carbs_g", 0),
        "selected_total_fat_g": totals.get("total_fat_g", 0),
        "kcal_gap_abs": round(to_float(totals.get("total_kcal")) - target.kcal, 4),
        "kcal_gap_pct": round(pct_gap(totals.get("total_kcal"), target.kcal), 4),
        "protein_gap_abs": round(to_float(totals.get("total_protein_g")) - target.protein_g, 4),
        "protein_gap_pct": round(pct_gap(totals.get("total_protein_g"), target.protein_g), 4),
        "carbs_gap_abs": round(to_float(totals.get("total_carbs_g")) - target.carbs_g, 4),
        "carbs_gap_pct": round(pct_gap(totals.get("total_carbs_g"), target.carbs_g), 4),
        "fat_gap_abs": round(to_float(totals.get("total_fat_g")) - target.fat_g, 4),
        "fat_gap_pct": round(pct_gap(totals.get("total_fat_g"), target.fat_g), 4),
        "validation_status": validation.get("validation_status", ""),
        "is_valid_for_checkpoint_1": str(bool(validation.get("is_valid_for_checkpoint_1", False))),
        "selected_slot_count": totals.get("selected_slot_count", 0),
        "expected_slot_count": len(target.slot_targets),
        "total_time_min_sum": totals.get("total_time_min_sum", 0),
        "effective_time_min_sum": totals.get("effective_time_min_sum", 0),
        "passive_time_estimated_sum": totals.get("passive_time_estimated_sum", 0),
        "eligible_candidate_count": eligible_count,
        "filtered_candidate_count": filtered_count,
        "slot_candidate_counts_json": json.dumps(slot_counts, sort_keys=True),
        "warnings": "|".join(clean_text(item) for item in warnings if clean_text(item)),
        "macro_gap_classes_json": json.dumps(macro_classes),
    }
    return row


def build_meal_rows(
    scenario_id: str,
    plan: dict[str, Any],
    candidate_lookup: dict[tuple[str, str, float], dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for meal in plan.get("selected_meals", []):
        key = (
            clean_text(meal.get("slot")),
            clean_text(meal.get("recipe_id")),
            round(to_float(meal.get("portion_multiplier")), 4),
        )
        candidate = candidate_lookup.get(key, {})
        slot_quality = classify_slot_quality(meal, candidate)
        row = {
            "scenario_id": scenario_id,
            "slot": clean_text(meal.get("slot")),
            "recipe_id": clean_text(meal.get("recipe_id")),
            "display_name": clean_text(meal.get("display_name")),
            "recipe_kind": clean_text(candidate.get("recipe_kind")),
            "recipe_category": clean_text(candidate.get("recipe_category")),
            "recipe_subcategory": clean_text(candidate.get("recipe_subcategory")),
            "portion_multiplier": round_number(meal.get("portion_multiplier")),
            "portion_grams_estimated": round_number(meal.get("portion_grams_estimated")),
            "kcal": round_number(meal.get("kcal")),
            "protein_g": round_number(meal.get("protein_g")),
            "carbs_g": round_number(meal.get("carbs_g")),
            "fat_g": round_number(meal.get("fat_g")),
            "score_preview": round_number(meal.get("score_preview")),
            "macro_fit": round_number(meal.get("macro_fit")),
            "time_fit": round_number(meal.get("time_fit")),
            "slot_fit": round_number(meal.get("slot_fit")),
            "nutrition_quality": round_number(meal.get("nutrition_quality")),
            "total_time_min": round_number(meal.get("total_time_min")),
            "effective_time_min_for_scoring": round_number(meal.get("effective_time_min_for_scoring")),
            "allowed_slots_json": clean_text(candidate.get("allowed_slots_json")),
            "is_slot_suspicious": str(bool(meal.get("is_slot_suspicious", False))),
            "is_nutrition_suspicious": str(bool(meal.get("is_nutrition_suspicious", False))),
            "slot_fit_reasons": serialized_reason(meal.get("slot_fit_reasons")),
            "slot_suspicion_reasons": serialized_reason(meal.get("slot_suspicion_reasons")),
            "nutrition_quality_reasons": serialized_reason(meal.get("nutrition_quality_reasons")),
            "time_estimation_reasons": serialized_reason(meal.get("time_estimation_reasons")),
            "slot_quality_status": slot_quality["status"],
            "slot_quality_notes": slot_quality["notes"],
        }
        rows.append(row)
    return rows


def serialized_reason(value: object) -> str:
    if isinstance(value, list):
        return "|".join(clean_text(item) for item in value if clean_text(item))
    return clean_text(value)


def parse_allowed_slots(value: object) -> set[str]:
    text = clean_text(value)
    if not text:
        return set()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {part.strip().lower() for part in text.split(",") if part.strip()}
    if not isinstance(parsed, list):
        return set()
    return {clean_text(item).lower() for item in parsed if clean_text(item)}


def classify_slot_quality(meal: dict[str, object], candidate: dict[str, object]) -> dict[str, str]:
    slot = clean_text(meal.get("slot")).lower()
    recipe_id = clean_text(meal.get("recipe_id"))
    recipe_kind = clean_text(candidate.get("recipe_kind")).lower()
    allowed = parse_allowed_slots(candidate.get("allowed_slots_json"))
    notes: list[str] = []
    status = "slot_quality_ok"

    if allowed and slot not in allowed:
        status = "slot_quality_problem"
        notes.append("slot_not_allowed_by_allowed_slots_json")
    if slot == "snack" and not recipe_id.startswith("manual_snack_v1_1_round12_"):
        status = "slot_quality_review"
        notes.append("snack_not_manual_round12")
    if slot in {"breakfast", "lunch", "dinner"} and recipe_kind in {"component", "protein_component", "carb_side", "veg_side"}:
        status = "slot_quality_problem"
        notes.append("component_selected_as_full_meal")
    if bool(meal.get("is_slot_suspicious", False)):
        status = "slot_quality_review" if status == "slot_quality_ok" else status
        notes.append("generator_marked_slot_suspicious")
    if not notes:
        notes.append("slot_matches_policy")
    return {"status": status, "notes": "|".join(notes)}


def classify_macro_gaps(target_data: dict[str, object], totals: dict[str, object], validation_status: str) -> list[str]:
    target_carbs = to_float(target_data.get("carbs_g"))
    target_fat = to_float(target_data.get("fat_g"))
    kcal_ratio = ratio(totals.get("total_kcal"), target_data.get("kcal"))
    protein_ratio = ratio(totals.get("total_protein_g"), target_data.get("protein_g"))
    carbs_ratio = ratio(totals.get("total_carbs_g"), target_data.get("carbs_g"))
    fat_ratio = ratio(totals.get("total_fat_g"), target_data.get("fat_g"))
    classes: list[str] = []

    if kcal_ratio < 0.85:
        classes.append("kcal_low")
    elif kcal_ratio > 1.15:
        classes.append("kcal_high")
    if protein_ratio < 0.90:
        classes.append("protein_low")
    elif protein_ratio > 1.60:
        classes.append("protein_high")
    if target_carbs > 0:
        if carbs_ratio < 0.70:
            classes.append("carbs_low")
        elif carbs_ratio > 1.30:
            classes.append("carbs_high")
    if target_fat > 0:
        if fat_ratio < 0.70:
            classes.append("fat_low")
        elif fat_ratio > 1.40:
            classes.append("fat_high")
    if target_carbs > 0 and protein_ratio > 1.60 and carbs_ratio < 0.80:
        classes.append("protein_heavy")
    if target_carbs > 0 and carbs_ratio < 0.70:
        classes.append("carb_deficient")
    if target_fat > 0 and fat_ratio > 1.40:
        classes.append("fat_heavy")
    if not classes and validation_status == "valid":
        classes.append("good_fit")
    elif not classes:
        classes.append("near_fit")
    return classes


def build_macro_gap_row(plan_row: dict[str, object]) -> dict[str, object]:
    classes = json.loads(clean_text(plan_row.get("macro_gap_classes_json")) or "[]")
    return {
        "scenario_id": plan_row["scenario_id"],
        "validation_status": plan_row["validation_status"],
        "target_kcal": plan_row["target_kcal"],
        "selected_total_kcal": plan_row["selected_total_kcal"],
        "kcal_ratio": round_number(1.0 + to_float(plan_row["kcal_gap_pct"])),
        "protein_ratio": round_number(1.0 + to_float(plan_row["protein_gap_pct"])),
        "carbs_ratio": round_number(1.0 + to_float(plan_row["carbs_gap_pct"])),
        "fat_ratio": round_number(1.0 + to_float(plan_row["fat_gap_pct"])),
        "macro_gap_classes_json": json.dumps(classes),
        "good_fit": str("good_fit" in classes),
        "protein_heavy": str("protein_heavy" in classes),
        "carb_deficient": str("carb_deficient" in classes),
        "fat_heavy": str("fat_heavy" in classes),
        "primary_macro_issue": classes[0] if classes else "",
    }


def build_slot_count_rows(scenario_id: str, slot_candidates: Any) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if slot_candidates.empty:
        return rows
    for slot, group in slot_candidates.groupby("slot", sort=False):
        rows.append(
            {
                "scenario_id": scenario_id,
                "slot": clean_text(slot),
                "slot_candidate_count": len(group),
                "unique_recipe_count": group["recipe_id"].astype(str).nunique() if "recipe_id" in group.columns else 0,
                "suspicious_count": int(group["is_slot_suspicious"].fillna(False).astype(bool).sum())
                if "is_slot_suspicious" in group.columns
                else 0,
                "non_suspicious_count": int((~group["is_slot_suspicious"].fillna(False).astype(bool)).sum())
                if "is_slot_suspicious" in group.columns
                else len(group),
                "median_kcal": round_number(group["kcal"].median() if "kcal" in group.columns else 0),
                "median_protein_g": round_number(group["protein_g"].median() if "protein_g" in group.columns else 0),
                "best_score_preview": round_number(group["score_preview"].max() if "score_preview" in group.columns else 0),
                "best_macro_fit": round_number(group["macro_fit"].max() if "macro_fit" in group.columns else 0),
            }
        )
    return rows


def build_repetition_rows(meal_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_recipe: dict[str, dict[str, object]] = {}
    for meal in meal_rows:
        recipe_id = clean_text(meal.get("recipe_id"))
        if recipe_id not in by_recipe:
            by_recipe[recipe_id] = {
                "recipe_id": recipe_id,
                "display_name": clean_text(meal.get("display_name")),
                "recipe_kind": clean_text(meal.get("recipe_kind")),
                "selected_total_count": 0,
                "breakfast_count": 0,
                "lunch_count": 0,
                "dinner_count": 0,
                "snack_count": 0,
                "scenario_ids": [],
            }
        row = by_recipe[recipe_id]
        slot = clean_text(meal.get("slot"))
        row["selected_total_count"] = int(row["selected_total_count"]) + 1
        if f"{slot}_count" in row:
            row[f"{slot}_count"] = int(row[f"{slot}_count"]) + 1
        scenario_id = clean_text(meal.get("scenario_id"))
        if scenario_id not in row["scenario_ids"]:
            row["scenario_ids"].append(scenario_id)

    rows: list[dict[str, object]] = []
    for row in by_recipe.values():
        selected_count = int(row["selected_total_count"])
        dominance_note = "dominates_selection" if selected_count >= 4 else "normal_repetition"
        rows.append(
            {
                **row,
                "scenario_ids": "|".join(row["scenario_ids"]),
                "dominance_note": dominance_note,
            }
        )
    return sorted(rows, key=lambda item: (-int(item["selected_total_count"]), str(item["recipe_id"])))


def build_summary(
    plan_rows: list[dict[str, object]],
    meal_rows: list[dict[str, object]],
    repetition_rows: list[dict[str, object]],
) -> str:
    scenario_count = len(plan_rows)
    valid_count = sum(1 for row in plan_rows if clean_text(row.get("validation_status")) == "valid")
    good_macro_count = sum(has_macro_class(row, "good_fit") for row in plan_rows)
    protein_heavy_count = sum(has_macro_class(row, "protein_heavy") for row in plan_rows)
    carb_deficient_count = sum(has_macro_class(row, "carb_deficient") for row in plan_rows)
    fat_heavy_count = sum(has_macro_class(row, "fat_heavy") for row in plan_rows)
    snack_rows = [row for row in meal_rows if clean_text(row.get("slot")) == "snack"]
    manual_snack_count = sum(clean_text(row.get("recipe_id")).startswith("manual_snack_v1_1_round12_") for row in snack_rows)
    top_repeated = repetition_rows[:8]
    same_plan_count = count_repeated_plan_shapes(meal_rows)
    recommended = recommend_next_step(
        scenario_count=scenario_count,
        valid_count=valid_count,
        good_macro_count=good_macro_count,
        protein_heavy_count=protein_heavy_count,
        carb_deficient_count=carb_deficient_count,
        repetition_rows=repetition_rows,
    )

    macro_class_counter: Counter[str] = Counter()
    for row in plan_rows:
        for macro_class in macro_classes_from_row(row):
            macro_class_counter[macro_class] += 1

    lines = [
        "Generator v1 round13 scenario evaluation",
        "=" * 42,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        f"scenario_count: {scenario_count}",
        f"valid_scenarios: {valid_count}",
        f"good_macro_fit_scenarios: {good_macro_count}",
        f"protein_heavy_scenarios: {protein_heavy_count}",
        f"carb_deficient_scenarios: {carb_deficient_count}",
        f"fat_heavy_scenarios: {fat_heavy_count}",
        f"manual_snack_selected_count: {manual_snack_count}/{len(snack_rows)}",
        f"repeated_plan_shapes: {same_plan_count}",
        "",
        "Macro class counts:",
    ]
    lines.extend(counter_lines(macro_class_counter))
    lines.extend(["", "Per-scenario status:"])
    for row in plan_rows:
        lines.append(
            "- "
            + clean_text(row.get("scenario_id"))
            + " | "
            + clean_text(row.get("validation_status"))
            + " | kcal_ratio="
            + round_number(1.0 + to_float(row.get("kcal_gap_pct")))
            + " | protein_ratio="
            + round_number(1.0 + to_float(row.get("protein_gap_pct")))
            + " | carbs_ratio="
            + round_number(1.0 + to_float(row.get("carbs_gap_pct")))
            + " | classes="
            + ",".join(macro_classes_from_row(row))
        )
    lines.extend(["", "Most repeated recipes:"])
    for row in top_repeated:
        lines.append(
            "- "
            + clean_text(row.get("recipe_id"))
            + " | "
            + clean_text(row.get("display_name"))
            + " | count="
            + clean_text(row.get("selected_total_count"))
            + " | slots="
            + slot_count_summary(row)
        )
    lines.extend(["", "Snack behavior:"])
    if snack_rows and manual_snack_count == len(snack_rows):
        lines.append("- all selected snacks are manual round12 snack-ready rows.")
    else:
        lines.append("- snack selection needs review; at least one selected snack is not manual round12.")
    lines.extend(["", "Slot quality:"])
    slot_quality_counter = Counter(clean_text(row.get("slot_quality_status")) for row in meal_rows)
    lines.extend(counter_lines(slot_quality_counter))
    lines.extend(["", "Recommended next step:"])
    lines.append(f"- {recommended}")
    lines.extend(
        [
            "",
            "Output files:",
            f"- {OUT_PLANS}",
            f"- {OUT_MEALS}",
            f"- {OUT_REPETITION}",
            f"- {OUT_MACRO_GAPS}",
            f"- {OUT_SLOT_COUNTS}",
        ]
    )
    return "\n".join(lines) + "\n"


def has_macro_class(row: dict[str, object], macro_class: str) -> bool:
    return macro_class in macro_classes_from_row(row)


def macro_classes_from_row(row: dict[str, object]) -> list[str]:
    try:
        parsed = json.loads(clean_text(row.get("macro_gap_classes_json")) or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [clean_text(item) for item in parsed if clean_text(item)]


def counter_lines(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in counter.most_common()]


def slot_count_summary(row: dict[str, object]) -> str:
    parts = []
    for slot in ("breakfast", "lunch", "dinner", "snack"):
        count = int(row.get(f"{slot}_count") or 0)
        if count:
            parts.append(f"{slot}:{count}")
    return ",".join(parts) if parts else "none"


def count_repeated_plan_shapes(meal_rows: list[dict[str, object]]) -> int:
    shape_counter: Counter[str] = Counter()
    by_scenario: dict[str, list[str]] = {}
    for meal in meal_rows:
        scenario_id = clean_text(meal.get("scenario_id"))
        by_scenario.setdefault(scenario_id, []).append(clean_text(meal.get("recipe_id")))
    for recipe_ids in by_scenario.values():
        shape_counter["|".join(recipe_ids)] += 1
    return sum(count for count in shape_counter.values() if count > 1)


def recommend_next_step(
    scenario_count: int,
    valid_count: int,
    good_macro_count: int,
    protein_heavy_count: int,
    carb_deficient_count: int,
    repetition_rows: list[dict[str, object]],
) -> str:
    dominant_recipe_count = sum(
        1
        for row in repetition_rows
        if clean_text(row.get("dominance_note")) == "dominates_selection"
    )
    if valid_count < scenario_count:
        return "D. more recipe data or target-specific coverage before selector changes"
    if carb_deficient_count >= max(3, scenario_count // 2) or protein_heavy_count >= max(3, scenario_count // 2):
        return "B. day_selector/day-level macro balancing, then A. scoring calibration"
    if dominant_recipe_count >= 3 or good_macro_count < scenario_count // 2:
        return "C. variety/repetition handling plus A. scoring calibration"
    return "E. Streamlit multi-run/testing support before broader changes"


def main() -> None:
    base_profile = load_member_profile(BASE_PROFILE_PATH)
    fooddb = load_fooddb_current()
    plan_rows: list[dict[str, object]] = []
    meal_rows: list[dict[str, object]] = []
    macro_gap_rows: list[dict[str, object]] = []
    slot_count_rows: list[dict[str, object]] = []

    for overrides in scenario_overrides():
        scenario_id = clean_text(overrides["scenario_id"])
        profile = build_scenario_profile(base_profile, overrides)
        plan_row, scenario_meals, scenario_macro_gaps, scenario_slot_counts = run_scenario(
            scenario_id=scenario_id,
            profile=profile,
            fooddb=fooddb,
        )
        plan_rows.append(plan_row)
        meal_rows.extend(scenario_meals)
        macro_gap_rows.extend(scenario_macro_gaps)
        slot_count_rows.extend(scenario_slot_counts)

    repetition_rows = build_repetition_rows(meal_rows)

    write_csv(OUT_PLANS, plan_rows, PLAN_COLUMNS)
    write_csv(OUT_MEALS, meal_rows, MEAL_COLUMNS)
    write_csv(OUT_MACRO_GAPS, macro_gap_rows, MACRO_GAP_COLUMNS)
    write_csv(OUT_REPETITION, repetition_rows, REPETITION_COLUMNS)
    write_csv(OUT_SLOT_COUNTS, slot_count_rows, SLOT_COUNT_COLUMNS)
    OUT_SUMMARY.write_text(build_summary(plan_rows, meal_rows, repetition_rows), encoding="utf-8")

    valid_count = sum(1 for row in plan_rows if clean_text(row.get("validation_status")) == "valid")
    protein_heavy_count = sum(has_macro_class(row, "protein_heavy") for row in plan_rows)
    carb_deficient_count = sum(has_macro_class(row, "carb_deficient") for row in plan_rows)
    print("Generator v1 round13 scenario evaluation written")
    print(f"scenario_count={len(plan_rows)}")
    print(f"valid_scenarios={valid_count}")
    print(f"protein_heavy_scenarios={protein_heavy_count}")
    print(f"carb_deficient_scenarios={carb_deficient_count}")
    print(f"written_summary={OUT_SUMMARY}")
    print(f"written_plans={OUT_PLANS}")
    print(f"written_meals={OUT_MEALS}")


if __name__ == "__main__":
    main()
