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
    HOUSEHOLD_MODE_SHARED_ALL_SLOTS,
    build_household_aggregate_target,
    build_member_targets,
    generate_household_plan,
    household_plan_readable_lines,
    load_household_profile,
)
from src.generator_v1.household_preview import (  # noqa: E402
    DEFAULT_ALLOCATION_MODE,
    build_household_preview,
)
from src.generator_v1.multi_day_selector import (  # noqa: E402
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import build_nutrition_target  # noqa: E402


AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_round68_household_generation_summary.txt"
DAYS_OUT = AUDIT_DIR / "generator_v1_round68_household_generation_days.csv"
ALLOCATIONS_OUT = AUDIT_DIR / "generator_v1_round68_household_generation_allocations.csv"
MEMBER_MACROS_OUT = AUDIT_DIR / "generator_v1_round68_household_generation_member_macros.csv"
GROCERY_SCALING_OUT = AUDIT_DIR / "generator_v1_round68_household_generation_grocery_scaling.csv"
COMPARISON_OUT = AUDIT_DIR / "generator_v1_round68_household_generation_comparison.csv"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    household_profile = load_household_profile(ROOT / "profiles/household_profile_demo_v1.json")
    baseline = _generate_primary_baseline(household_profile, days=3)
    preview = build_household_preview(
        baseline["plan"],
        household_profile,
        allocation_mode=DEFAULT_ALLOCATION_MODE,
    )
    household_generation = _generate_household_lite(household_profile, days=3)

    pd.DataFrame(_day_rows(household_generation)).to_csv(DAYS_OUT, index=False)
    pd.DataFrame(household_generation.get("allocations", [])).to_csv(ALLOCATIONS_OUT, index=False)
    pd.DataFrame(household_generation.get("member_daily_rows", [])).to_csv(
        MEMBER_MACROS_OUT,
        index=False,
    )
    pd.DataFrame(household_generation.get("grocery_scaling", [])).to_csv(
        GROCERY_SCALING_OUT,
        index=False,
    )
    comparison_rows = _comparison_rows(baseline["plan"], preview, household_generation)
    pd.DataFrame(comparison_rows).to_csv(COMPARISON_OUT, index=False)
    SUMMARY_OUT.write_text(
        _summary_text(
            baseline=baseline["plan"],
            preview=preview,
            household_generation=household_generation,
            comparison_rows=comparison_rows,
        ),
        encoding="utf-8",
    )

    summary = household_generation.get("household_summary", {})
    print("Round68 household generation audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"days={DAYS_OUT}")
    print(f"allocations={ALLOCATIONS_OUT}")
    print(f"member_macros={MEMBER_MACROS_OUT}")
    print(f"grocery_scaling={GROCERY_SCALING_OUT}")
    print(f"comparison={COMPARISON_OUT}")
    print(
        "household_generation="
        f"members={summary.get('member_count')}; "
        f"days={summary.get('days_generated')}; "
        f"quality={summary.get('household_quality_status')}; "
        f"max_grocery_factor={summary.get('max_grocery_scaling_factor')}"
    )


def _generate_primary_baseline(
    household_profile: dict[str, Any],
    *,
    days: int,
) -> dict[str, Any]:
    primary = _primary_member(household_profile)
    target = build_nutrition_target(primary)
    pool = _load_pool()
    fooddb = load_fooddb_current()
    preference_context = build_household_preference_context(primary)
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
    plan = generate_multi_day_plan(
        profile=primary,
        target=target,
        slot_candidates=slot_candidates,
        days=days,
        config=_multi_day_config(),
    )
    return {"plan": plan, "pool": pool, "slot_candidates": slot_candidates}


def _generate_household_lite(
    household_profile: dict[str, Any],
    *,
    days: int,
) -> dict[str, Any]:
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
        days=days,
        config={
            **_multi_day_config(),
            "household_mode": HOUSEHOLD_MODE_SHARED_ALL_SLOTS,
            "allocation_mode": DEFAULT_ALLOCATION_MODE,
            "global_max_candidates_per_slot": 16,
            "day_candidate_pool_size_target": 40,
            "day_candidate_pool_max": 80,
            "direct_slot_shortlist_size": 8,
        },
        profile=primary,
    )
    return plan


def _load_pool() -> Any:
    return load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )


def _multi_day_config() -> dict[str, Any]:
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


def _day_rows(plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    quality_by_day = {
        int(row.get("day_index") or 0): row
        for row in plan.get("household_day_quality_rows", [])
    }
    for day in plan.get("days", []):
        day_index = int(day.get("day_index") or 0)
        quality = quality_by_day.get(day_index, {})
        rows.append(
            {
                "day_index": day_index,
                "validation_status": day.get("validation_status"),
                "quality_gate_status": day.get("quality_gate_status"),
                "household_quality_status": quality.get("household_quality_status"),
                "household_quality_reasons": quality.get("household_quality_reasons"),
                "max_abs_kcal_deviation_pct": quality.get("max_abs_kcal_deviation_pct"),
                "min_protein_ratio": quality.get("min_protein_ratio"),
                "portion_clamped_count": quality.get("portion_clamped_count"),
            }
        )
    return rows


def _comparison_rows(
    baseline_plan: dict[str, Any],
    preview: dict[str, Any],
    household_generation: dict[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "scenario": "individual_baseline",
            "days_generated": (baseline_plan.get("multi_day_summary") or {}).get(
                "actual_days_generated"
            ),
            "valid_days": (baseline_plan.get("multi_day_summary") or {}).get(
                "valid_day_count"
            ),
            "accept_days": (baseline_plan.get("multi_day_summary") or {}).get(
                "accept_day_count"
            ),
            "mean_abs_kcal_deviation_pct": "",
            "mean_abs_protein_deviation_pct": "",
            "max_grocery_scaling_factor": "",
        },
        {
            "scenario": "household_preview_from_baseline",
            "days_generated": (preview.get("summary") or {}).get("days_generated"),
            "valid_days": "",
            "accept_days": "",
            "mean_abs_kcal_deviation_pct": _mean_abs(
                preview.get("member_daily_rows", []),
                "kcal_deviation_pct",
            ),
            "mean_abs_protein_deviation_pct": _mean_abs(
                preview.get("member_daily_rows", []),
                "protein_deviation_pct",
            ),
            "max_grocery_scaling_factor": (preview.get("summary") or {}).get(
                "max_grocery_scaling_factor"
            ),
        },
        {
            "scenario": "household_generation_v1_lite",
            "days_generated": (household_generation.get("household_summary") or {}).get(
                "days_generated"
            ),
            "valid_days": (household_generation.get("multi_day_summary") or {}).get(
                "valid_day_count"
            ),
            "accept_days": (household_generation.get("multi_day_summary") or {}).get(
                "accept_day_count"
            ),
            "mean_abs_kcal_deviation_pct": (
                household_generation.get("household_summary") or {}
            ).get("mean_abs_kcal_deviation_pct"),
            "mean_abs_protein_deviation_pct": (
                household_generation.get("household_summary") or {}
            ).get("mean_abs_protein_deviation_pct"),
            "max_grocery_scaling_factor": (
                household_generation.get("household_summary") or {}
            ).get("max_grocery_scaling_factor"),
        },
    ]


def _summary_text(
    *,
    baseline: dict[str, Any],
    preview: dict[str, Any],
    household_generation: dict[str, Any],
    comparison_rows: list[dict[str, Any]],
) -> str:
    baseline_summary = baseline.get("multi_day_summary") or {}
    household_summary = household_generation.get("household_summary") or {}
    usable = (
        int(household_summary.get("days_generated") or 0) == 3
        and str(household_summary.get("household_quality_status")) in {"accept", "review"}
        and float(household_summary.get("max_abs_kcal_deviation_pct") or 999.0) <= 20.0
    )
    selected_differs = _selected_recipe_set(baseline) != _selected_recipe_set(
        household_generation
    )
    lines = [
        "Round68 Household Generation v1 Lite audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"member_count={household_summary.get('member_count')}",
        f"days_generated={household_summary.get('days_generated')}",
        f"baseline_valid_days={baseline_summary.get('valid_day_count')}",
        f"baseline_accept_days={baseline_summary.get('accept_day_count')}",
        f"household_accept_day_count={household_summary.get('household_accept_day_count')}",
        f"household_review_day_count={household_summary.get('household_review_day_count')}",
        f"household_reject_day_count={household_summary.get('household_reject_day_count')}",
        f"household_quality_status={household_summary.get('household_quality_status')}",
        f"mean_abs_kcal_deviation_pct={household_summary.get('mean_abs_kcal_deviation_pct')}",
        f"mean_abs_protein_deviation_pct={household_summary.get('mean_abs_protein_deviation_pct')}",
        f"min_portion_multiplier={household_summary.get('min_portion_multiplier')}",
        f"max_portion_multiplier={household_summary.get('max_portion_multiplier')}",
        f"clamped_portion_count={household_summary.get('clamped_portion_count')}",
        f"max_grocery_scaling_factor={household_summary.get('max_grocery_scaling_factor')}",
        f"household_generation_usable_for_demo={usable}",
        f"household_native_plan_differs_from_preview_baseline={selected_differs}",
        "",
        "Comparison:",
        *[
            "- "
            + "; ".join(f"{key}={value}" for key, value in row.items())
            for row in comparison_rows
        ],
        "",
        "Known limitations:",
        *[f"- {warning}" for warning in household_generation.get("warnings", [])],
        "",
        "Copy-friendly sample:",
        *household_plan_readable_lines(household_generation)[:90],
    ]
    return "\n".join(lines) + "\n"


def _selected_recipe_set(plan: dict[str, Any]) -> set[str]:
    recipe_ids: set[str] = set()
    for day in plan.get("days", []):
        for meal in day.get("selected_meals", []):
            recipe_id = str(meal.get("recipe_id") or "").strip()
            if recipe_id:
                recipe_ids.add(recipe_id)
    return recipe_ids


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


def _mean_abs(rows: list[dict[str, Any]], field: str) -> float:
    values = [abs(_to_float(row.get(field)) or 0.0) for row in rows]
    return round(sum(values) / len(values), 1) if values else 0.0


def _to_float(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


if __name__ == "__main__":
    main()
