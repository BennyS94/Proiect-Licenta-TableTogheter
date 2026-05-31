from __future__ import annotations

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
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.grocery_list import build_grocery_list  # noqa: E402
from src.generator_v1.household_generator import (  # noqa: E402
    HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
    build_household_aggregate_target,
    build_member_targets,
    filter_household_profile_members,
    generate_household_plan,
    load_household_profile,
)
from src.generator_v1.household_preview import DEFAULT_ALLOCATION_MODE  # noqa: E402
from src.generator_v1.multi_day_selector import MULTI_DAY_MODE_GLOBAL  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402


AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_round71_streamlit_generation_flow_summary.txt"
MEMBERS_OUT = AUDIT_DIR / "generator_v1_round71_streamlit_generation_flow_members.csv"
GROCERY_OUT = AUDIT_DIR / "generator_v1_round71_streamlit_generation_flow_grocery.csv"

SCENARIOS = {
    "alex_only": ["member_demo_adult_male_001"],
    "alex_mara": [
        "member_demo_adult_male_001",
        "member_demo_adult_female_001",
    ],
    "all_members": [
        "member_demo_adult_male_001",
        "member_demo_adult_female_001",
        "member_demo_lower_target_001",
    ],
    "no_members": [],
}


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    base_profile = load_household_profile(ROOT / "profiles/household_profile_demo_v1.json")
    ingredients = pd.read_csv(ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH)
    fooddb = load_fooddb_current()
    results: dict[str, dict[str, Any]] = {}
    for scenario, selected_member_ids in SCENARIOS.items():
        if not selected_member_ids:
            results[scenario] = {
                "scenario": scenario,
                "selected_member_ids": [],
                "generation_blocked": True,
                "generation_type": "blocked_no_members",
            }
            continue
        plan = _generate_plan_for_members(base_profile, selected_member_ids)
        generation_type = "single_member" if len(selected_member_ids) == 1 else "household"
        plan["latest_generation_type"] = generation_type
        plan["selected_member_ids_used"] = list(selected_member_ids)
        plan["days_used"] = 3
        grocery_plan = _member_aware_plan_for_grocery(plan)
        grocery_list = build_grocery_list(
            grocery_plan,
            ingredients,
            fooddb_df=fooddb,
            config={
                "include_pantry_basics": False,
                "include_purchase_suggestions": True,
                "enable_cooked_to_raw_conversion": True,
                "include_price_estimates": True,
                "exclude_water": True,
            },
        )
        results[scenario] = {
            "scenario": scenario,
            "selected_member_ids": list(selected_member_ids),
            "generation_blocked": False,
            "generation_type": generation_type,
            "plan": plan,
            "grocery_list": grocery_list,
            "grocery_plan": grocery_plan,
        }

    pd.DataFrame(_member_rows(results)).to_csv(MEMBERS_OUT, index=False)
    pd.DataFrame(_grocery_rows(results)).to_csv(GROCERY_OUT, index=False)
    SUMMARY_OUT.write_text(_summary_text(results), encoding="utf-8")

    print("Round71 Streamlit generation flow audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"members={MEMBERS_OUT}")
    print(f"grocery={GROCERY_OUT}")
    for scenario, result in results.items():
        if result.get("generation_blocked"):
            print(f"{scenario}: blocked_no_members")
            continue
        plan = result["plan"]
        summary = plan.get("household_summary") or {}
        grocery_summary = (result.get("grocery_list") or {}).get("summary") or {}
        print(
            f"{scenario}: type={result.get('generation_type')}; "
            f"members={summary.get('member_count')}; "
            f"days={summary.get('days_generated')}; "
            f"quality={summary.get('household_quality_status')}; "
            f"grocery_items={grocery_summary.get('shopping_item_count')}"
        )


def _generate_plan_for_members(
    base_profile: dict[str, Any],
    member_ids: list[str],
) -> dict[str, Any]:
    household_profile = filter_household_profile_members(base_profile, member_ids)
    member_targets = build_member_targets(household_profile)
    target = build_household_aggregate_target(member_targets)
    pool = _load_pool()
    fooddb = load_fooddb_current()
    primary = _primary_member(household_profile)
    preference_context = build_household_preference_context(
        _household_context_profile(household_profile, primary)
    )
    filtered = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode="target_aware",
    )
    plan = generate_household_plan(
        household_profile,
        slot_candidates=slot_candidates,
        individual_slot_candidates=slot_candidates,
        days=3,
        config=_generation_config(),
        profile=primary,
    )
    plan["selected_household_member_ids"] = list(member_ids)
    plan["selected_household_member_names"] = [
        str(member.get("display_name") or member.get("member_id"))
        for member in household_profile.get("members", [])
    ]
    plan["pool_summary"] = {
        "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
        "ingredients_path": str(V1_2_DEMO_FINAL_INGREDIENTS_PATH),
    }
    return plan


def _load_pool() -> Any:
    return load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )


def _generation_config() -> dict[str, Any]:
    return {
        "household_mode": HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
        "allocation_mode": DEFAULT_ALLOCATION_MODE,
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": 3,
        "return_alternatives": True,
        "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
        "candidate_day_alternative_count": 10,
        "global_max_candidates_per_slot": 16,
        "day_candidate_pool_size_target": 40,
        "day_candidate_pool_max": 80,
        "include_slot_forced_variants": True,
        "no_repeat_policy": "hard",
        "multi_day_speed_mode": "fast",
        "day_candidate_builder": "direct_from_slots",
        "direct_slot_shortlist_size": 8,
        "protein_correction_threshold": 0.85,
    }


def _member_aware_plan_for_grocery(plan: dict[str, Any]) -> dict[str, Any]:
    individual_by_day: dict[int, list[dict[str, Any]]] = {}
    for row in plan.get("individual_meals", []):
        day_index = int(row.get("day_index") or 1)
        individual_by_day.setdefault(day_index, []).append(dict(row))
    grocery_days: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        day_index = int(day.get("day_index") or len(grocery_days) + 1)
        selected_meals: list[dict[str, Any]] = []
        for meal in day.get("selected_meals", []):
            if not bool(meal.get("household_generation_shared_slot", True)):
                continue
            meal_row = dict(meal)
            meal_row["portion_multiplier"] = (
                _to_float(meal.get("household_portion_sum"))
                or _to_float(meal.get("portion_multiplier"))
                or 1.0
            )
            selected_meals.append(meal_row)
        for row in individual_by_day.get(day_index, []):
            selected_meals.append(
                {
                    "slot": row.get("slot"),
                    "recipe_id": row.get("recipe_id"),
                    "display_name": row.get("display_name") or row.get("recipe"),
                    "portion_multiplier": (
                        _to_float(row.get("portion_multiplier_member"))
                        or _to_float(row.get("portion_multiplier"))
                        or 1.0
                    ),
                }
            )
        grocery_days.append({"day_index": day_index, "selected_meals": selected_meals})
    result = dict(plan)
    result["days"] = grocery_days
    return result


def _member_rows(results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario, result in results.items():
        selected = set(result.get("selected_member_ids") or [])
        if result.get("generation_blocked"):
            rows.append(
                {
                    "scenario": scenario,
                    "selected_member_count": 0,
                    "generation_type": result.get("generation_type"),
                    "member_view_data_available": False,
                    "generation_blocked": True,
                }
            )
            continue
        plan = result["plan"]
        allocation_member_ids = {
            str(row.get("member_id") or "")
            for row in plan.get("allocations", [])
            if str(row.get("member_id") or "")
        }
        for member in (plan.get("member_targets") or {}).get("target_rows", []):
            member_id = str(member.get("member_id") or "")
            daily_rows = [
                row
                for row in plan.get("member_daily_rows", [])
                if str(row.get("member_id") or "") == member_id
            ]
            rows.append(
                {
                    "scenario": scenario,
                    "member_id": member_id,
                    "member": member.get("member"),
                    "selected_member_count": len(selected),
                    "generation_type": result.get("generation_type"),
                    "member_is_selected": member_id in selected,
                    "member_view_data_available": bool(daily_rows)
                    and member_id in allocation_member_ids,
                    "generated_days": (plan.get("household_summary") or {}).get(
                        "days_generated"
                    ),
                    "avg_kcal_ratio": _mean(daily_rows, "kcal_ratio"),
                    "avg_protein_ratio": _mean(daily_rows, "protein_ratio"),
                }
            )
    return rows


def _grocery_rows(results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario, result in results.items():
        if result.get("generation_blocked"):
            rows.append(
                {
                    "scenario": scenario,
                    "generation_type": result.get("generation_type"),
                    "aggregate_grocery_available": False,
                    "selected_member_count": 0,
                }
            )
            continue
        plan = result["plan"]
        grocery_summary = (result.get("grocery_list") or {}).get("summary") or {}
        pricing_summary = grocery_summary.get("pricing_summary") or {}
        rows.append(
            {
                "scenario": scenario,
                "generation_type": result.get("generation_type"),
                "selected_member_count": len(result.get("selected_member_ids") or []),
                "generated_days": (plan.get("household_summary") or {}).get(
                    "days_generated"
                ),
                "selected_meal_count_for_grocery": _selected_meal_count(
                    result.get("grocery_plan") or {}
                ),
                "aggregate_grocery_available": bool(
                    grocery_summary.get("shopping_item_count")
                ),
                "shopping_item_count": grocery_summary.get("shopping_item_count"),
                "display_item_count": grocery_summary.get("display_item_count"),
                "priced_item_count": pricing_summary.get("priced_item_count"),
                "unpriced_item_count": pricing_summary.get("unpriced_item_count"),
                "estimated_total_cost": pricing_summary.get("total_estimated_cost"),
                "currency": pricing_summary.get("currency"),
            }
        )
    return rows


def _summary_text(results: dict[str, dict[str, Any]]) -> str:
    lines = [
        "Round71 Streamlit generation flow audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"household_mode={HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN}",
        f"allocation_mode={DEFAULT_ALLOCATION_MODE}",
        "",
    ]
    for scenario, result in results.items():
        lines.append(f"Scenario: {scenario}")
        if result.get("generation_blocked"):
            lines.extend(
                [
                    "- selected_member_count=0",
                    "- generation_blocked=True",
                    "- reason=no_members_selected",
                    "",
                ]
            )
            continue
        plan = result["plan"]
        summary = plan.get("household_summary") or {}
        selected_ids = set(result.get("selected_member_ids") or [])
        allocation_ids = {
            str(row.get("member_id") or "")
            for row in plan.get("allocations", [])
            if str(row.get("member_id") or "")
        }
        grocery_summary = (result.get("grocery_list") or {}).get("summary") or {}
        lines.extend(
            [
                f"- selected_member_count={len(selected_ids)}",
                f"- generation_type={result.get('generation_type')}",
                f"- generated_days={summary.get('days_generated')}",
                f"- household_quality_status={summary.get('household_quality_status')}",
                f"- member_view_data_available={bool(plan.get('member_daily_rows')) and bool(plan.get('allocations'))}",
                f"- allocation_rows_only_selected_members={allocation_ids.issubset(selected_ids)}",
                f"- aggregate_grocery_available={bool(grocery_summary.get('shopping_item_count'))}",
                f"- shopping_item_count={grocery_summary.get('shopping_item_count')}",
                "",
            ]
        )
    return "\n".join(lines)


def _primary_member(household_profile: dict[str, Any]) -> dict[str, Any]:
    active_ids = set(household_profile.get("active_member_ids") or [])
    for member in household_profile.get("members", []):
        if str(member.get("member_id") or "") in active_ids:
            return dict(member)
    raise ValueError("Household profile has no active members.")


def _household_context_profile(
    household_profile: dict[str, Any],
    primary_member: dict[str, Any],
) -> dict[str, Any]:
    profile = dict(primary_member)
    preferences = household_profile.get("household_preferences") or {}
    profile["banned_recipe_ids"] = preferences.get("banned_recipe_ids", [])
    profile["banned_ingredient_names"] = preferences.get("banned_ingredient_names", [])
    return profile


def _selected_meal_count(plan: dict[str, Any]) -> int:
    return sum(len(day.get("selected_meals", [])) for day in plan.get("days", []))


def _to_float(value: Any) -> float | None:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def _mean(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_to_float(row.get(key)) for row in rows]
    usable = [value for value in values if value is not None]
    if not usable:
        return None
    return round(sum(usable) / len(usable), 4)


if __name__ == "__main__":
    main()
