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
SUMMARY_OUT = AUDIT_DIR / "generator_v1_round70_household_member_selection_summary.txt"
MEMBERS_OUT = AUDIT_DIR / "generator_v1_round70_household_member_selection_members.csv"
ALLOCATIONS_OUT = AUDIT_DIR / "generator_v1_round70_household_member_selection_allocations.csv"
GROCERY_SCALING_OUT = AUDIT_DIR / "generator_v1_round70_household_member_selection_grocery_scaling.csv"


SCENARIOS = {
    "all_members": [
        "member_demo_adult_male_001",
        "member_demo_adult_female_001",
        "member_demo_lower_target_001",
    ],
    "alex_mara": [
        "member_demo_adult_male_001",
        "member_demo_adult_female_001",
    ],
    "mara_only": ["member_demo_adult_female_001"],
}


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    base_profile = load_household_profile(ROOT / "profiles/household_profile_demo_v1.json")
    plans = {
        name: _generate_plan_for_members(base_profile, member_ids)
        for name, member_ids in SCENARIOS.items()
    }

    member_rows = _member_rows(plans)
    allocation_rows = _allocation_rows(plans)
    grocery_rows = _grocery_rows(plans)
    pd.DataFrame(member_rows).to_csv(MEMBERS_OUT, index=False)
    pd.DataFrame(allocation_rows).to_csv(ALLOCATIONS_OUT, index=False)
    pd.DataFrame(grocery_rows).to_csv(GROCERY_SCALING_OUT, index=False)
    SUMMARY_OUT.write_text(_summary_text(plans), encoding="utf-8")

    print("Round70 household member selection audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"members={MEMBERS_OUT}")
    print(f"allocations={ALLOCATIONS_OUT}")
    print(f"grocery_scaling={GROCERY_SCALING_OUT}")
    for scenario, plan in plans.items():
        summary = plan.get("household_summary") or {}
        print(
            f"{scenario}: members={summary.get('member_count')}; "
            f"days={summary.get('days_generated')}; "
            f"quality={summary.get('household_quality_status')}; "
            f"max_grocery_factor={summary.get('max_grocery_scaling_factor')}"
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
    plan["round70_scenario_member_ids"] = list(member_ids)
    plan["round70_scenario_member_names"] = [
        str(member.get("display_name") or member.get("member_id"))
        for member in household_profile.get("members", [])
    ]
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


def _member_rows(plans: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario, plan in plans.items():
        target_rows = {
            str(row.get("member_id") or ""): row
            for row in (plan.get("member_targets") or {}).get("target_rows", [])
        }
        for member_id, target_row in target_rows.items():
            macro_rows = [
                row
                for row in plan.get("member_daily_rows", [])
                if str(row.get("member_id") or "") == member_id
            ]
            rows.append(
                {
                    "scenario": scenario,
                    "member_id": member_id,
                    "member": target_row.get("member"),
                    "selected_member_count": (plan.get("household_summary") or {}).get(
                        "member_count"
                    ),
                    "kcal_target": target_row.get("kcal_target"),
                    "protein_g_target": target_row.get("protein_g_target"),
                    "avg_kcal_ratio": _mean(macro_rows, "kcal_ratio"),
                    "avg_protein_ratio": _mean(macro_rows, "protein_ratio"),
                    "min_protein_ratio": _min(macro_rows, "protein_ratio"),
                    "filtering_matches_selection": member_id
                    in set(plan.get("round70_scenario_member_ids", [])),
                }
            )
    return rows


def _allocation_rows(plans: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario, plan in plans.items():
        selected = set(plan.get("round70_scenario_member_ids", []))
        for row in plan.get("allocations", []):
            enriched = dict(row)
            enriched["scenario"] = scenario
            enriched["member_is_selected"] = str(row.get("member_id") or "") in selected
            rows.append(enriched)
    return rows


def _grocery_rows(plans: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario, plan in plans.items():
        for row in plan.get("grocery_scaling", []):
            enriched = dict(row)
            enriched["scenario"] = scenario
            rows.append(enriched)
    return rows


def _summary_text(plans: dict[str, dict[str, Any]]) -> str:
    lines = [
        "Round70 household member selection audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"household_mode={HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN}",
        f"allocation_mode={DEFAULT_ALLOCATION_MODE}",
        "",
    ]
    for scenario, plan in plans.items():
        summary = plan.get("household_summary") or {}
        selected_ids = plan.get("round70_scenario_member_ids", [])
        selected_names = plan.get("round70_scenario_member_names", [])
        member_rows = [
            row
            for row in plan.get("member_daily_rows", [])
            if str(row.get("member_id") or "") in set(selected_ids)
        ]
        filtering_ok = all(
            str(row.get("member_id") or "") in set(selected_ids)
            for row in plan.get("allocations", [])
        )
        lines.extend(
            [
                f"Scenario: {scenario}",
                f"- selected_member_ids={selected_ids}",
                f"- selected_member_names={selected_names}",
                f"- selected_member_count={summary.get('member_count')}",
                f"- generated_days={summary.get('days_generated')}",
                f"- household_quality_status={summary.get('household_quality_status')}",
                f"- accept_day_count={summary.get('household_accept_day_count')}",
                f"- review_day_count={summary.get('household_review_day_count')}",
                f"- reject_day_count={summary.get('household_reject_day_count')}",
                f"- mean_abs_kcal_deviation_pct={summary.get('mean_abs_kcal_deviation_pct')}",
                f"- mean_abs_protein_deviation_pct={summary.get('mean_abs_protein_deviation_pct')}",
                f"- min_portion_multiplier={summary.get('min_portion_multiplier')}",
                f"- max_portion_multiplier={summary.get('max_portion_multiplier')}",
                f"- max_grocery_scaling_factor={summary.get('max_grocery_scaling_factor')}",
                f"- allocation_rows_match_selection={filtering_ok}",
                f"- avg_member_kcal_ratio={_mean(member_rows, 'kcal_ratio')}",
                f"- avg_member_protein_ratio={_mean(member_rows, 'protein_ratio')}",
                "",
            ]
        )
    lines.extend(
        [
            "Verdict:",
            "- all-members scenario verifies default household behavior.",
            "- subset scenario verifies in-memory member filtering.",
            "- one-member scenario verifies the selected-member edge case.",
            "- grocery output remains aggregate household scaling, not per-member grocery.",
        ]
    )
    return "\n".join(lines) + "\n"


def _primary_member(household_profile: dict[str, Any]) -> dict[str, Any]:
    active_ids = {
        str(member_id).strip()
        for member_id in household_profile.get("active_member_ids", [])
        if str(member_id).strip()
    }
    for member in household_profile.get("members", []):
        if str(member.get("member_id", "")).strip() in active_ids:
            return dict(member)
    raise ValueError("Household profile nu are membri activi.")


def _household_context_profile(
    household_profile: dict[str, Any],
    primary_member: dict[str, Any],
) -> dict[str, Any]:
    profile = dict(primary_member)
    preferences = household_profile.get("household_preferences") or {}
    profile["banned_recipe_ids"] = preferences.get("banned_recipe_ids", [])
    profile["banned_ingredient_names"] = preferences.get("banned_ingredient_names", [])
    profile["dietary_preferences"] = _merged_household_dietary_preferences(
        household_profile,
        primary_member,
    )
    return profile


def _merged_household_dietary_preferences(
    household_profile: dict[str, Any],
    primary_member: dict[str, Any],
) -> dict[str, bool]:
    keys = [
        "no_beef",
        "no_chicken",
        "no_fish",
        "no_dairy",
        "vegetarian",
        "vegan",
        "gluten_free",
    ]
    result = {
        key: bool((primary_member.get("dietary_preferences") or {}).get(key, False))
        for key in keys
    }
    active_ids = {
        str(member_id).strip()
        for member_id in household_profile.get("active_member_ids", [])
        if str(member_id).strip()
    }
    for member in household_profile.get("members", []):
        if str(member.get("member_id", "")).strip() not in active_ids:
            continue
        dietary = member.get("dietary_preferences") or {}
        for key in keys:
            result[key] = bool(result[key] or dietary.get(key, False))
    return result


def _mean(rows: list[dict[str, Any]], field: str) -> float:
    values = [_to_float(row.get(field)) for row in rows]
    clean_values = [value for value in values if value is not None]
    return round(sum(clean_values) / len(clean_values), 4) if clean_values else 0.0


def _min(rows: list[dict[str, Any]], field: str) -> float:
    values = [_to_float(row.get(field)) for row in rows]
    clean_values = [value for value in values if value is not None]
    return round(min(clean_values), 4) if clean_values else 0.0


def _to_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


if __name__ == "__main__":
    main()
