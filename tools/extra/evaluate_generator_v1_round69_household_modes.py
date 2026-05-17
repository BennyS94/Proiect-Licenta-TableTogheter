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
    HOUSEHOLD_MODE_SHARED_ALL_SLOTS,
    HOUSEHOLD_MODE_SHARED_MAIN_MEALS,
    build_household_aggregate_target,
    build_member_targets,
    generate_household_plan,
    household_plan_readable_lines,
    load_household_profile,
)
from src.generator_v1.household_preview import DEFAULT_ALLOCATION_MODE  # noqa: E402
from src.generator_v1.multi_day_selector import MULTI_DAY_MODE_GLOBAL  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402


AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_round69_household_modes_summary.txt"
DAYS_OUT = AUDIT_DIR / "generator_v1_round69_household_modes_days.csv"
ALLOCATIONS_OUT = AUDIT_DIR / "generator_v1_round69_household_modes_allocations.csv"
MEMBER_MACROS_OUT = AUDIT_DIR / "generator_v1_round69_household_modes_member_macros.csv"
GROCERY_SCALING_OUT = AUDIT_DIR / "generator_v1_round69_household_modes_grocery_scaling.csv"
COMPARISON_OUT = AUDIT_DIR / "generator_v1_round69_household_modes_comparison.csv"


MODES = [
    HOUSEHOLD_MODE_SHARED_ALL_SLOTS,
    HOUSEHOLD_MODE_SHARED_MAIN_MEALS,
    HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
]


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    household_profile = load_household_profile(ROOT / "profiles/household_profile_demo_v1.json")
    context = _build_generation_context(household_profile)
    plans = {
        mode: generate_household_plan(
            household_profile,
            slot_candidates=context["slot_candidates"],
            individual_slot_candidates=context["slot_candidates"],
            days=3,
            config={**_generation_config(), "household_mode": mode},
            profile=context["primary_member"],
        )
        for mode in MODES
    }

    pd.DataFrame(_day_rows(plans)).to_csv(DAYS_OUT, index=False)
    pd.DataFrame(_allocation_rows(plans)).to_csv(ALLOCATIONS_OUT, index=False)
    pd.DataFrame(_member_macro_rows(plans)).to_csv(MEMBER_MACROS_OUT, index=False)
    pd.DataFrame(_grocery_rows(plans)).to_csv(GROCERY_SCALING_OUT, index=False)
    comparison_rows = _comparison_rows(plans)
    pd.DataFrame(comparison_rows).to_csv(COMPARISON_OUT, index=False)
    SUMMARY_OUT.write_text(
        _summary_text(plans=plans, comparison_rows=comparison_rows),
        encoding="utf-8",
    )

    recommended = _recommended_mode(comparison_rows)
    print("Round69 household modes audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"days={DAYS_OUT}")
    print(f"allocations={ALLOCATIONS_OUT}")
    print(f"member_macros={MEMBER_MACROS_OUT}")
    print(f"grocery_scaling={GROCERY_SCALING_OUT}")
    print(f"comparison={COMPARISON_OUT}")
    print(f"recommended_mode={recommended}")


def _build_generation_context(household_profile: dict[str, Any]) -> dict[str, Any]:
    member_targets = build_member_targets(household_profile)
    target = build_household_aggregate_target(member_targets)
    pool = load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )
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
    return {
        "member_targets": member_targets,
        "target": target,
        "pool": pool,
        "slot_candidates": slot_candidates,
        "primary_member": primary,
    }


def _generation_config() -> dict[str, Any]:
    return {
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


def _day_rows(plans: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mode, plan in plans.items():
        quality_by_day = {
            int(row.get("day_index") or 0): row
            for row in plan.get("household_day_quality_rows", [])
        }
        for day in plan.get("days", []):
            quality = quality_by_day.get(int(day.get("day_index") or 0), {})
            rows.append(
                {
                    "household_mode": mode,
                    "day_index": day.get("day_index"),
                    "validation_status": day.get("validation_status"),
                    "quality_gate_status": day.get("quality_gate_status"),
                    "household_quality_status": quality.get("household_quality_status"),
                    "household_quality_reasons": quality.get("household_quality_reasons"),
                    "max_abs_kcal_deviation_pct": quality.get("max_abs_kcal_deviation_pct"),
                    "min_protein_ratio": quality.get("min_protein_ratio"),
                    "portion_clamped_count": quality.get("portion_clamped_count"),
                    "max_grocery_scaling_factor": quality.get("max_grocery_scaling_factor"),
                }
            )
    return rows


def _allocation_rows(plans: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mode, plan in plans.items():
        for row in plan.get("allocations", []):
            enriched = dict(row)
            enriched["household_mode"] = mode
            rows.append(enriched)
    return rows


def _member_macro_rows(plans: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mode, plan in plans.items():
        for row in plan.get("member_daily_rows", []):
            enriched = dict(row)
            enriched["household_mode"] = mode
            rows.append(enriched)
    return rows


def _grocery_rows(plans: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mode, plan in plans.items():
        for row in plan.get("grocery_scaling", []):
            enriched = dict(row)
            enriched["household_mode"] = mode
            rows.append(enriched)
    return rows


def _comparison_rows(plans: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mode, plan in plans.items():
        summary = plan.get("household_summary") or {}
        rows.append(
            {
                "household_mode": mode,
                "household_quality_status": summary.get("household_quality_status"),
                "accept_day_count": summary.get("household_accept_day_count"),
                "review_day_count": summary.get("household_review_day_count"),
                "reject_day_count": summary.get("household_reject_day_count"),
                "mean_abs_kcal_deviation_pct": summary.get("mean_abs_kcal_deviation_pct"),
                "mean_abs_protein_deviation_pct": summary.get(
                    "mean_abs_protein_deviation_pct"
                ),
                "min_protein_ratio": summary.get("min_protein_ratio"),
                "worst_protein_member": summary.get("worst_protein_member"),
                "min_portion_multiplier": summary.get("min_portion_multiplier"),
                "max_portion_multiplier": summary.get("max_portion_multiplier"),
                "clamped_count": summary.get("clamped_portion_count"),
                "max_grocery_scaling_factor": summary.get("max_grocery_scaling_factor"),
                "shared_meal_count": summary.get("shared_meal_count"),
                "individual_meal_count": summary.get("individual_meal_count"),
                "protein_correction_applied_count": summary.get(
                    "protein_correction_applied_count"
                ),
                "protein_gap_count": summary.get("protein_gap_count"),
                "demo_usable": _demo_usable(summary),
            }
        )
    return rows


def _summary_text(
    *,
    plans: dict[str, dict[str, Any]],
    comparison_rows: list[dict[str, Any]],
) -> str:
    recommended = _recommended_mode(comparison_rows)
    lines = [
        "Round69 Household Generation Lite mode audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"allocation_mode={DEFAULT_ALLOCATION_MODE}",
        f"recommended_mode={recommended}",
        "",
        "Mode comparison:",
        *[
            "- " + "; ".join(f"{key}={value}" for key, value in row.items())
            for row in comparison_rows
        ],
        "",
        "Protein correction notes:",
    ]
    for mode, plan in plans.items():
        summary = plan.get("household_summary") or {}
        rows = plan.get("protein_correction_rows", [])
        lines.append(
            f"- {mode}: applied_days={summary.get('protein_correction_applied_count')}; "
            f"remaining_gap_count={summary.get('protein_gap_count')}; "
            f"worst_member={summary.get('worst_protein_member')}; "
            f"min_protein_ratio={summary.get('min_protein_ratio')}"
        )
        for row in rows[:3]:
            lines.append(
                "  - "
                f"Day {row.get('day_index')} {row.get('member')}: "
                f"gap_before={row.get('protein_gap_before_g')}g, "
                f"gap_after={row.get('protein_gap_after_g')}g, "
                f"meals={row.get('selected_correction_meals')}"
            )
    if recommended in plans:
        lines.extend(
            [
                "",
                "Recommended mode sample:",
                *household_plan_readable_lines(plans[recommended])[:90],
            ]
        )
    return "\n".join(lines) + "\n"


def _recommended_mode(comparison_rows: list[dict[str, Any]]) -> str:
    usable_rows = [row for row in comparison_rows if bool(row.get("demo_usable"))]
    preferred = [
        row
        for row in usable_rows
        if row.get("household_mode") == HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN
    ]
    if preferred:
        return HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN
    if usable_rows:
        return str(usable_rows[0].get("household_mode"))
    return str(comparison_rows[0].get("household_mode")) if comparison_rows else "none"


def _demo_usable(summary: dict[str, Any]) -> bool:
    return (
        int(summary.get("days_generated") or 0) == 3
        and str(summary.get("household_quality_status")) in {"accept", "review"}
        and float(summary.get("max_abs_kcal_deviation_pct") or 999.0) <= 35.0
        and float(summary.get("min_protein_ratio") or 0.0) >= 0.65
    )


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
    return profile


if __name__ == "__main__":
    main()
