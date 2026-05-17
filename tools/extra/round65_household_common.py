from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.candidate_filter import (  # noqa: E402
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (  # noqa: E402
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_NUTRITION_PATH,
    V1_2_DEMO_FINAL_TIME_LAYER_PROFILE,
    V1_2_DEMO_FINAL_TIME_LAYER_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.multi_day_selector import (  # noqa: E402
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import (  # noqa: E402
    NutritionTarget,
    build_nutrition_target,
)


HOUSEHOLD_PROFILE_PATH = ROOT / "profiles/household_profile_demo_v1.json"
AUDIT_DIR = ROOT / "data/recipesdb/audit"

MEMBER_PORTION_MIN = 0.4
MEMBER_PORTION_MAX = 1.8
MACRO_FIELDS = ("kcal", "protein_g", "carbs_g", "fat_g")


def load_household_profile(path: Path = HOUSEHOLD_PROFILE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def active_members(household_profile: dict[str, Any]) -> list[dict[str, Any]]:
    active_ids = {
        str(member_id).strip()
        for member_id in household_profile.get("active_member_ids", [])
        if str(member_id).strip()
    }
    return [
        member
        for member in household_profile.get("members", [])
        if str(member.get("member_id", "")).strip() in active_ids
    ]


def target_to_dict(target: NutritionTarget) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def member_target_rows(
    household_profile: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, NutritionTarget]]:
    members = active_members(household_profile)
    targets = {str(member["member_id"]): build_nutrition_target(member) for member in members}
    household_totals = _household_target_totals(targets.values())
    rows: list[dict[str, Any]] = []
    for member in members:
        member_id = str(member["member_id"])
        target = targets[member_id]
        for slot, slot_target in target.slot_targets.items():
            rows.append(
                {
                    "household_id": household_profile.get("household_id"),
                    "member_id": member_id,
                    "display_name": member.get("display_name"),
                    "slot": slot,
                    "daily_kcal_target": target.kcal,
                    "daily_protein_g_target": target.protein_g,
                    "daily_carbs_g_target": target.carbs_g,
                    "daily_fat_g_target": target.fat_g,
                    "slot_kcal_target": slot_target.get("kcal"),
                    "slot_protein_g_target": slot_target.get("protein_g"),
                    "slot_carbs_g_target": slot_target.get("carbs_g"),
                    "slot_fat_g_target": slot_target.get("fat_g"),
                    "kcal_target_ratio": _ratio(target.kcal, household_totals["kcal"]),
                    "protein_target_ratio": _ratio(
                        target.protein_g,
                        household_totals["protein_g"],
                    ),
                    "carbs_target_ratio": _ratio(
                        target.carbs_g,
                        household_totals["carbs_g"],
                    ),
                    "fat_target_ratio": _ratio(target.fat_g, household_totals["fat_g"]),
                }
            )
    return rows, targets


def dataset_config() -> dict[str, Any]:
    time_layer = {
        "dataset_profile": V1_2_DEMO_FINAL_TIME_LAYER_PROFILE,
        "recipes": ROOT / V1_2_DEMO_FINAL_TIME_LAYER_RECIPES_PATH,
        "ingredients": ROOT / V1_2_DEMO_FINAL_TIME_LAYER_INGREDIENTS_PATH,
        "nutrition": ROOT / V1_2_DEMO_FINAL_TIME_LAYER_NUTRITION_PATH,
    }
    if all(Path(time_layer[key]).exists() for key in ("recipes", "ingredients", "nutrition")):
        return time_layer
    return {
        "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
        "recipes": ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        "ingredients": ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        "nutrition": ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
    }


def generate_primary_member_plan(
    household_profile: dict[str, Any],
    *,
    days: int = 3,
) -> dict[str, Any]:
    members = active_members(household_profile)
    if not members:
        raise ValueError("Household profile nu are membri activi.")
    primary_member = members[0]
    target = build_nutrition_target(primary_member)
    config = dataset_config()
    pool = load_recipe_candidate_pool(
        recipes_path=config["recipes"],
        ingredients_path=config["ingredients"],
        nutrition_path=config["nutrition"],
        dataset_profile=config["dataset_profile"],
    )
    fooddb = load_fooddb_current()
    preference_context = build_household_preference_context(primary_member)
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
        portion_policy_mode="target_aware",
    )
    plan = generate_multi_day_plan(
        profile=primary_member,
        target=target,
        slot_candidates=slot_candidates,
        days=days,
        config=multi_day_config(),
    )
    return {
        "dataset": config,
        "pool": pool,
        "fooddb": fooddb,
        "primary_member": primary_member,
        "primary_target": target,
        "slot_candidates": slot_candidates,
        "plan": plan,
    }


def multi_day_config() -> dict[str, Any]:
    return {
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": 3,
        "return_alternatives": True,
        "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
        "candidate_day_alternative_count": 10,
        "global_max_candidates_per_slot": 26,
        "day_candidate_pool_size_target": 75,
        "day_candidate_pool_max": 150,
        "include_slot_forced_variants": True,
        "no_repeat_policy": "hard",
        "multi_day_speed_mode": "fast",
        "day_candidate_builder": "direct_from_slots",
        "direct_slot_shortlist_size": 12,
    }


def allocation_rows_for_plan(
    household_profile: dict[str, Any],
    plan: dict[str, Any],
    targets: dict[str, NutritionTarget],
    *,
    methods: tuple[str, ...],
    shared_slots: set[str],
) -> list[dict[str, Any]]:
    members = active_members(household_profile)
    primary_member_id = str(members[0]["member_id"])
    primary_target = targets[primary_member_id]
    min_multiplier = _config_float(
        household_profile,
        "portion_multiplier_min",
        MEMBER_PORTION_MIN,
    )
    max_multiplier = _config_float(
        household_profile,
        "portion_multiplier_max",
        MEMBER_PORTION_MAX,
    )
    rows: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            slot = str(meal.get("slot") or "")
            if slot not in shared_slots:
                continue
            for method in methods:
                method_rows = []
                for member in members:
                    member_id = str(member["member_id"])
                    row = allocate_member_portion(
                        method=method,
                        member=member,
                        member_target=targets[member_id],
                        primary_target=primary_target,
                        meal=meal,
                        min_multiplier=min_multiplier,
                        max_multiplier=max_multiplier,
                    )
                    row.update(
                        {
                            "household_id": household_profile.get("household_id"),
                            "method": method,
                            "day_index": day.get("day_index"),
                            "slot": slot,
                            "recipe_id": meal.get("recipe_id"),
                            "display_name": meal.get("display_name"),
                            "base_portion_multiplier": _to_float(
                                meal.get("portion_multiplier")
                            ),
                        }
                    )
                    method_rows.append(row)
                household_total = sum(
                    _to_float(row.get("portion_multiplier_member")) or 0.0
                    for row in method_rows
                )
                household_grams = sum(
                    _to_float(row.get("grams_estimated")) or 0.0
                    for row in method_rows
                )
                household_review = any(
                    _clean_text(row.get("household_portion_fit_warning"))
                    for row in method_rows
                )
                for row in method_rows:
                    row["household_total_portion_sum"] = round(household_total, 3)
                    row["household_total_grams_estimated"] = round(household_grams, 1)
                    row["household_portion_fit_review"] = household_review
                    rows.append(row)
    return rows


def allocate_member_portion(
    *,
    method: str,
    member: dict[str, Any],
    member_target: NutritionTarget,
    primary_target: NutritionTarget,
    meal: dict[str, Any],
    min_multiplier: float = MEMBER_PORTION_MIN,
    max_multiplier: float = MEMBER_PORTION_MAX,
) -> dict[str, Any]:
    slot = str(meal.get("slot") or "")
    base_multiplier = _to_float(meal.get("portion_multiplier")) or 1.0
    selected_macros = {field: _to_float(meal.get(field)) or 0.0 for field in MACRO_FIELDS}
    per_one_portion = {
        field: value / base_multiplier if base_multiplier else value
        for field, value in selected_macros.items()
    }
    raw_multiplier = _raw_member_multiplier(
        method=method,
        member_target=member_target,
        primary_target=primary_target,
        slot=slot,
        base_multiplier=base_multiplier,
        per_one_portion=per_one_portion,
    )
    multiplier = _clamp(raw_multiplier, min_multiplier, max_multiplier)
    actual = {field: round(per_one_portion[field] * multiplier, 1) for field in MACRO_FIELDS}
    slot_target = member_target.slot_targets.get(slot, {})
    warning_codes = []
    if not _approximately_equal(raw_multiplier, multiplier):
        warning_codes.append("portion_multiplier_clamped")
    if abs(_deviation_ratio(actual["kcal"], slot_target.get("kcal"))) > 0.25:
        warning_codes.append("kcal_slot_deviation_review")
    if abs(_deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))) > 0.35:
        warning_codes.append("protein_slot_deviation_review")

    serving_grams = _to_float(meal.get("serving_weight_g_estimated"))
    if serving_grams is None:
        serving_grams = _to_float(meal.get("overlay_serving_weight_g_estimated"))
    grams_estimated = serving_grams * multiplier if serving_grams is not None else None

    return {
        "member_id": member.get("member_id"),
        "display_name_member": member.get("display_name"),
        "portion_multiplier_raw": round(raw_multiplier, 3),
        "portion_multiplier_member": round(multiplier, 3),
        "portion_multiplier_clamped": not _approximately_equal(raw_multiplier, multiplier),
        "grams_estimated": _round_optional(grams_estimated, 1),
        "kcal": actual["kcal"],
        "protein_g": actual["protein_g"],
        "carbs_g": actual["carbs_g"],
        "fat_g": actual["fat_g"],
        "slot_kcal_target": slot_target.get("kcal"),
        "slot_protein_g_target": slot_target.get("protein_g"),
        "slot_carbs_g_target": slot_target.get("carbs_g"),
        "slot_fat_g_target": slot_target.get("fat_g"),
        "kcal_deviation_pct": _percent(_deviation_ratio(actual["kcal"], slot_target.get("kcal"))),
        "protein_deviation_pct": _percent(
            _deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))
        ),
        "carbs_deviation_pct": _percent(
            _deviation_ratio(actual["carbs_g"], slot_target.get("carbs_g"))
        ),
        "fat_deviation_pct": _percent(
            _deviation_ratio(actual["fat_g"], slot_target.get("fat_g"))
        ),
        "household_portion_fit_warning": ";".join(warning_codes),
    }


def scenario_member_macro_rows(
    household_profile: dict[str, Any],
    plan: dict[str, Any],
    targets: dict[str, NutritionTarget],
    *,
    scenario_name: str,
    shared_slots: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    members = active_members(household_profile)
    allocation_rows = allocation_rows_for_plan(
        household_profile,
        plan,
        targets,
        methods=("macro_aware_simple",),
        shared_slots=shared_slots,
    )
    rows_by_day_slot_member = {
        (
            int(row.get("day_index") or 0),
            str(row.get("slot") or ""),
            str(row.get("member_id") or ""),
        ): row
        for row in allocation_rows
    }
    day_rows: list[dict[str, Any]] = []
    warning_rows: list[dict[str, Any]] = []
    grocery_rows: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        day_index = int(day.get("day_index") or 0)
        for member in members:
            member_id = str(member["member_id"])
            target = targets[member_id]
            totals = {field: 0.0 for field in MACRO_FIELDS}
            shared_count = 0
            for slot, slot_target in target.slot_targets.items():
                if slot in shared_slots:
                    allocated = rows_by_day_slot_member.get((day_index, slot, member_id))
                    if allocated is None:
                        for field in MACRO_FIELDS:
                            totals[field] += _to_float(slot_target.get(field)) or 0.0
                        warning_rows.append(
                            {
                                "scenario": scenario_name,
                                "day_index": day_index,
                                "slot": slot,
                                "member_id": member_id,
                                "warning_code": "missing_shared_slot_allocation",
                                "warning_detail": "slot target used as fallback",
                            }
                        )
                        continue
                    shared_count += 1
                    for field in MACRO_FIELDS:
                        totals[field] += _to_float(allocated.get(field)) or 0.0
                    warning_text = _clean_text(
                        allocated.get("household_portion_fit_warning")
                    )
                    if warning_text:
                        warning_rows.append(
                            {
                                "scenario": scenario_name,
                                "day_index": day_index,
                                "slot": slot,
                                "member_id": member_id,
                                "warning_code": "household_portion_fit_review",
                                "warning_detail": warning_text,
                            }
                        )
                else:
                    for field in MACRO_FIELDS:
                        totals[field] += _to_float(slot_target.get(field)) or 0.0
            day_rows.append(
                {
                    "scenario": scenario_name,
                    "day_index": day_index,
                    "member_id": member_id,
                    "display_name": member.get("display_name"),
                    "shared_slot_count": shared_count,
                    "kcal": round(totals["kcal"], 1),
                    "protein_g": round(totals["protein_g"], 1),
                    "carbs_g": round(totals["carbs_g"], 1),
                    "fat_g": round(totals["fat_g"], 1),
                    "target_kcal": target.kcal,
                    "target_protein_g": target.protein_g,
                    "target_carbs_g": target.carbs_g,
                    "target_fat_g": target.fat_g,
                    "kcal_deviation_pct": _percent(
                        _deviation_ratio(totals["kcal"], target.kcal)
                    ),
                    "protein_deviation_pct": _percent(
                        _deviation_ratio(totals["protein_g"], target.protein_g)
                    ),
                    "carbs_deviation_pct": _percent(
                        _deviation_ratio(totals["carbs_g"], target.carbs_g)
                    ),
                    "fat_deviation_pct": _percent(
                        _deviation_ratio(totals["fat_g"], target.fat_g)
                    ),
                }
            )
    grocery_rows.extend(
        grocery_scaling_rows(
            scenario_name=scenario_name,
            plan=plan,
            allocation_rows=allocation_rows,
            shared_slots=shared_slots,
        )
    )
    return day_rows, warning_rows, grocery_rows


def grocery_scaling_rows(
    *,
    scenario_name: str,
    plan: dict[str, Any],
    allocation_rows: list[dict[str, Any]],
    shared_slots: set[str],
) -> list[dict[str, Any]]:
    by_meal: dict[tuple[int, str, str], list[dict[str, Any]]] = {}
    for row in allocation_rows:
        key = (
            int(row.get("day_index") or 0),
            str(row.get("slot") or ""),
            str(row.get("recipe_id") or ""),
        )
        by_meal.setdefault(key, []).append(row)

    rows: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        day_index = int(day.get("day_index") or 0)
        for meal in day.get("selected_meals", []):
            slot = str(meal.get("slot") or "")
            if slot not in shared_slots:
                continue
            recipe_id = str(meal.get("recipe_id") or "")
            member_rows = by_meal.get((day_index, slot, recipe_id), [])
            member_portion_sum = sum(
                _to_float(row.get("portion_multiplier_member")) or 0.0
                for row in member_rows
            )
            single_profile_portion = _to_float(meal.get("portion_multiplier")) or 0.0
            factor = (
                member_portion_sum / single_profile_portion
                if single_profile_portion
                else None
            )
            warning = ""
            if factor is not None and factor > 4.0:
                warning = "household_quantity_factor_large"
            rows.append(
                {
                    "scenario": scenario_name,
                    "day_index": day_index,
                    "slot": slot,
                    "recipe_id": recipe_id,
                    "display_name": meal.get("display_name"),
                    "member_portion_sum": round(member_portion_sum, 3),
                    "single_profile_portion": round(single_profile_portion, 3),
                    "household_quantity_factor": _round_optional(factor, 3),
                    "warning": warning,
                }
            )
    return rows


def summarize_feasibility(member_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    by_scenario: dict[str, list[dict[str, Any]]] = {}
    for row in member_rows:
        by_scenario.setdefault(str(row.get("scenario") or ""), []).append(row)
    for scenario, rows in by_scenario.items():
        kcal_devs = [
            abs(_to_float(row.get("kcal_deviation_pct")) or 0.0)
            for row in rows
        ]
        protein_devs = [
            abs(_to_float(row.get("protein_deviation_pct")) or 0.0)
            for row in rows
        ]
        max_kcal = max(kcal_devs) if kcal_devs else 0.0
        mean_kcal = sum(kcal_devs) / len(kcal_devs) if kcal_devs else 0.0
        mean_protein = sum(protein_devs) / len(protein_devs) if protein_devs else 0.0
        summary[scenario] = {
            "row_count": len(rows),
            "mean_abs_kcal_deviation_pct": round(mean_kcal, 1),
            "max_abs_kcal_deviation_pct": round(max_kcal, 1),
            "mean_abs_protein_deviation_pct": round(mean_protein, 1),
            "feasible": mean_kcal <= 12.0 and max_kcal <= 20.0 and mean_protein <= 25.0,
        }
    return summary


def recommended_household_mode(feasibility: dict[str, dict[str, Any]]) -> str:
    if feasibility.get("individual_breakfast_shared_main", {}).get("feasible"):
        return "individual_breakfast_shared_main"
    if feasibility.get("shared_dinner_only", {}).get("feasible"):
        return "shared_dinner_only"
    if feasibility.get("shared_lunch_dinner", {}).get("feasible"):
        return "shared_lunch_dinner"
    return "not_ready"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def format_counts(values: dict[str, int] | pd.Series) -> str:
    if isinstance(values, pd.Series):
        values = values.to_dict()
    return "; ".join(f"{key}={value}" for key, value in sorted(values.items())) or "none"


def _raw_member_multiplier(
    *,
    method: str,
    member_target: NutritionTarget,
    primary_target: NutritionTarget,
    slot: str,
    base_multiplier: float,
    per_one_portion: dict[str, float],
) -> float:
    if method == "equal_portions":
        return base_multiplier
    if method == "proportional_to_daily_kcal":
        return base_multiplier * _ratio(member_target.kcal, primary_target.kcal)
    if method == "proportional_to_slot_kcal":
        return base_multiplier * _ratio(
            member_target.slot_targets.get(slot, {}).get("kcal"),
            primary_target.slot_targets.get(slot, {}).get("kcal"),
        )
    if method == "macro_aware_simple":
        return _macro_aware_multiplier(
            per_one_portion=per_one_portion,
            slot_target=member_target.slot_targets.get(slot, {}),
        )
    raise ValueError(f"Metoda portion allocation necunoscuta: {method}")


def _macro_aware_multiplier(
    *,
    per_one_portion: dict[str, float],
    slot_target: dict[str, float],
) -> float:
    best_multiplier = MEMBER_PORTION_MIN
    best_loss = float("inf")
    step_count = int(round((MEMBER_PORTION_MAX - MEMBER_PORTION_MIN) / 0.05))
    for index in range(step_count + 1):
        multiplier = round(MEMBER_PORTION_MIN + index * 0.05, 2)
        actual = {field: per_one_portion[field] * multiplier for field in MACRO_FIELDS}
        loss = (
            abs(_deviation_ratio(actual["kcal"], slot_target.get("kcal"))) * 0.55
            + abs(_deviation_ratio(actual["protein_g"], slot_target.get("protein_g"))) * 0.25
            + abs(_deviation_ratio(actual["carbs_g"], slot_target.get("carbs_g"))) * 0.10
            + abs(_deviation_ratio(actual["fat_g"], slot_target.get("fat_g"))) * 0.10
        )
        if loss < best_loss:
            best_loss = loss
            best_multiplier = multiplier
    return best_multiplier


def _household_target_totals(targets: Any) -> dict[str, float]:
    totals = {field: 0.0 for field in MACRO_FIELDS}
    for target in targets:
        totals["kcal"] += float(target.kcal)
        totals["protein_g"] += float(target.protein_g)
        totals["carbs_g"] += float(target.carbs_g)
        totals["fat_g"] += float(target.fat_g)
    return totals


def _config_float(
    household_profile: dict[str, Any],
    key: str,
    default: float,
) -> float:
    planning_config = household_profile.get("planning_config") or {}
    try:
        return float(planning_config.get(key, default))
    except (TypeError, ValueError):
        return default


def _ratio(left: Any, right: Any) -> float:
    left_value = _to_float(left)
    right_value = _to_float(right)
    if left_value is None or right_value in (None, 0.0):
        return 0.0
    return round(left_value / right_value, 4)


def _deviation_ratio(actual: Any, target: Any) -> float:
    actual_value = _to_float(actual)
    target_value = _to_float(target)
    if actual_value is None or target_value in (None, 0.0):
        return 0.0
    return (actual_value - target_value) / target_value


def _percent(value: float) -> float:
    return round(value * 100.0, 1)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return min(max(value, minimum), maximum)


def _approximately_equal(left: float, right: float) -> bool:
    return abs(left - right) < 0.0001


def _round_optional(value: Any, digits: int) -> float | None:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return round(numeric, digits)


def _to_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text
