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
from src.generator_v1.household_ingredient_guard import compute_egg_load  # noqa: E402
from src.generator_v1.household_preview import DEFAULT_ALLOCATION_MODE  # noqa: E402
from src.generator_v1.multi_day_selector import MULTI_DAY_MODE_GLOBAL  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402


AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_round73_household_egg_guard_summary.txt"
MEALS_OUT = AUDIT_DIR / "generator_v1_round73_household_egg_guard_meals.csv"
EGG_LOAD_OUT = AUDIT_DIR / "generator_v1_round73_household_egg_load.csv"
EGG_SOURCES_OUT = AUDIT_DIR / "generator_v1_round73_household_egg_sources.csv"

HOUSEHOLD_MEMBER_IDS = [
    "member_demo_adult_male_001",
    "member_demo_adult_female_001",
    "member_demo_lower_target_001",
]


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    ingredients = pd.read_csv(ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH)
    baseline = _generate_plan(enable_guard=False, ingredients=ingredients)
    guarded = _generate_plan(enable_guard=True, ingredients=ingredients)

    egg_rows = [
        _egg_summary_row("baseline_no_guard", baseline),
        _egg_summary_row("with_egg_guard", guarded),
    ]
    pd.DataFrame(egg_rows).to_csv(EGG_LOAD_OUT, index=False)
    pd.DataFrame(
        _meal_rows("baseline_no_guard", baseline)
        + _meal_rows("with_egg_guard", guarded)
    ).to_csv(MEALS_OUT, index=False)
    pd.DataFrame(
        _egg_source_rows("baseline_no_guard", baseline)
        + _egg_source_rows("with_egg_guard", guarded)
    ).to_csv(EGG_SOURCES_OUT, index=False)
    SUMMARY_OUT.write_text(_summary_text(baseline, guarded), encoding="utf-8")
    print("Round73 household egg guard audit written")
    print(SUMMARY_OUT)


def _generate_plan(*, enable_guard: bool, ingredients: pd.DataFrame) -> dict[str, Any]:
    base_profile = load_household_profile(ROOT / "profiles/household_profile_demo_v1.json")
    household_profile = filter_household_profile_members(base_profile, HOUSEHOLD_MEMBER_IDS)
    member_targets = build_member_targets(household_profile)
    target = build_household_aggregate_target(member_targets)
    pool = load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )
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
        fooddb=load_fooddb_current(),
        portion_policy_mode="target_aware",
    )
    plan = generate_household_plan(
        household_profile,
        slot_candidates=slot_candidates,
        individual_slot_candidates=slot_candidates,
        days=3,
        config=_generation_config(enable_guard=enable_guard, ingredients=ingredients),
        profile=primary,
    )
    plan["selected_household_member_ids"] = list(HOUSEHOLD_MEMBER_IDS)
    plan["selected_household_member_names"] = [
        str(member.get("display_name") or member.get("member_id"))
        for member in household_profile.get("members", [])
    ]
    if not plan.get("egg_load_audit"):
        plan["egg_load_audit"] = compute_egg_load(
            plan,
            plan.get("allocations", []),
            recipe_ingredients_df=ingredients,
        )
    return plan


def _generation_config(*, enable_guard: bool, ingredients: pd.DataFrame) -> dict[str, Any]:
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
        "enable_egg_load_guard": enable_guard,
        "recipe_ingredients_df": ingredients,
    }


def _egg_summary_row(label: str, plan: dict[str, Any]) -> dict[str, Any]:
    audit = plan.get("egg_load_audit") or {}
    summary = plan.get("household_summary") or {}
    return {
        "scenario": label,
        "household_quality_status": summary.get("household_quality_status"),
        "household_accept_day_count": summary.get("household_accept_day_count"),
        "household_review_day_count": summary.get("household_review_day_count"),
        "mean_abs_kcal_deviation_pct": summary.get("mean_abs_kcal_deviation_pct"),
        "mean_abs_protein_deviation_pct": summary.get("mean_abs_protein_deviation_pct"),
        "total_egg_count": audit.get("total_egg_count"),
        "direct_egg_count": audit.get("direct_egg_count"),
        "embedded_egg_count": audit.get("embedded_egg_count"),
        "eggs_per_person_per_day": audit.get("eggs_per_person_per_day"),
        "direct_eggs_per_person_per_day": audit.get("direct_eggs_per_person_per_day"),
        "egg_load_status": audit.get("egg_load_status"),
        "egg_load_reasons": "; ".join(audit.get("egg_load_reasons", [])),
    }


def _meal_rows(label: str, plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        day_index = int(day.get("day_index") or 0)
        for meal in day.get("selected_meals", []):
            rows.append(
                {
                    "scenario": label,
                    "day": day_index,
                    "slot": meal.get("slot"),
                    "recipe_id": meal.get("recipe_id"),
                    "display_name": meal.get("display_name"),
                    "meal_scope": "shared",
                    "household_portion_sum": meal.get("household_portion_sum"),
                    "egg_load_status": meal.get("egg_load_status"),
                    "egg_load_penalty": meal.get("egg_load_penalty"),
                }
            )
    for row in plan.get("individual_meals", []):
        rows.append(
            {
                "scenario": label,
                "day": row.get("day_index"),
                "slot": row.get("slot"),
                "recipe_id": row.get("recipe_id"),
                "display_name": row.get("display_name"),
                "meal_scope": f"individual:{row.get('member')}",
                "household_portion_sum": row.get("portion_multiplier_member"),
                "egg_load_status": row.get("egg_load_status"),
                "egg_load_penalty": row.get("egg_load_penalty"),
            }
        )
    return rows


def _egg_source_rows(label: str, plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in (plan.get("egg_load_audit") or {}).get("egg_source_breakdown", []):
        item = dict(row)
        item["scenario"] = label
        rows.append(item)
    return rows


def _summary_text(baseline: dict[str, Any], guarded: dict[str, Any]) -> str:
    before = _egg_summary_row("baseline_no_guard", baseline)
    after = _egg_summary_row("with_egg_guard", guarded)
    improved = (
        float(before.get("direct_eggs_per_person_per_day") or 0.0)
        > float(after.get("direct_eggs_per_person_per_day") or 0.0)
    )
    return "\n".join(
        [
            "Round73 household egg-load guard audit",
            "",
            f"before_direct_eggs_per_person_per_day={before.get('direct_eggs_per_person_per_day')}",
            f"after_direct_eggs_per_person_per_day={after.get('direct_eggs_per_person_per_day')}",
            f"before_total_eggs={before.get('total_egg_count')}",
            f"after_total_eggs={after.get('total_egg_count')}",
            f"before_direct_eggs={before.get('direct_egg_count')}",
            f"after_direct_eggs={after.get('direct_egg_count')}",
            f"before_household_quality={before.get('household_quality_status')}",
            f"after_household_quality={after.get('household_quality_status')}",
            f"before_mean_abs_kcal_deviation_pct={before.get('mean_abs_kcal_deviation_pct')}",
            f"after_mean_abs_kcal_deviation_pct={after.get('mean_abs_kcal_deviation_pct')}",
            f"before_mean_abs_protein_deviation_pct={before.get('mean_abs_protein_deviation_pct')}",
            f"after_mean_abs_protein_deviation_pct={after.get('mean_abs_protein_deviation_pct')}",
            f"egg_load_improved_without_ban={improved}",
            "",
            "Outputs:",
            f"- meals={MEALS_OUT}",
            f"- egg_load={EGG_LOAD_OUT}",
            f"- egg_sources={EGG_SOURCES_OUT}",
        ]
    ) + "\n"


def _primary_member(household_profile: dict[str, Any]) -> dict[str, Any]:
    active_ids = set(household_profile.get("active_member_ids") or [])
    for member in household_profile.get("members", []):
        if member.get("member_id") in active_ids:
            return dict(member)
    return dict((household_profile.get("members") or [{}])[0])


def _household_context_profile(
    household_profile: dict[str, Any],
    primary_member: dict[str, Any],
) -> dict[str, Any]:
    profile = dict(primary_member)
    preferences = household_profile.get("household_preferences") or {}
    profile["banned_recipe_ids"] = preferences.get("banned_recipe_ids", [])
    profile["banned_ingredient_names"] = preferences.get("banned_ingredient_names", [])
    profile["liked_recipe_ids"] = preferences.get("liked_recipe_ids", [])
    profile["disliked_recipe_ids"] = preferences.get("disliked_recipe_ids", [])
    profile["too_long_recipe_ids"] = preferences.get("too_long_recipe_ids", [])
    return profile


if __name__ == "__main__":
    main()
