from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.candidate_filter import (  # noqa: E402
    build_household_preference_context as build_profile_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (  # noqa: E402
    DEFAULT_FOODDB_PATH,
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.day_selector_balanced import select_one_day_plan_balanced  # noqa: E402
from src.generator_v1.feedback_adapter import (  # noqa: E402
    build_household_preference_context as build_feedback_preference_context,
)
from src.generator_v1.feedback_store import (  # noqa: E402
    append_feedback_event,
    clear_feedback_events,
    load_feedback_events,
)
from src.generator_v1.plan_quality_gate import evaluate_plan_quality  # noqa: E402
from src.generator_v1.plan_validator import validate_one_day_plan  # noqa: E402
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target  # noqa: E402


PROFILE_PATH = ROOT / "profiles/member_profile_demo_v1.json"
AUDIT_DIR = ROOT / "data/recipesdb/audit"
RUNTIME_FEEDBACK_PATH = ROOT / "data/runtime/generator_v1_round52_feedback_events.jsonl"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_round52_feedback_effect_summary.txt"
RUNS_OUT = AUDIT_DIR / "generator_v1_round52_feedback_effect_runs.csv"
MEALS_OUT = AUDIT_DIR / "generator_v1_round52_feedback_effect_meals.csv"
CONTEXT_OUT = AUDIT_DIR / "generator_v1_round52_feedback_context.json"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_FEEDBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
    clear_feedback_events(RUNTIME_FEEDBACK_PATH)

    profile = load_member_profile(PROFILE_PATH)
    pool = load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )
    fooddb = load_fooddb_current(ROOT / DEFAULT_FOODDB_PATH)

    baseline = generate_plan(
        scenario_id="baseline_feedback_disabled",
        profile=profile,
        pool=pool,
        fooddb=fooddb,
        feedback_context=feedback_context(profile, []),
    )
    baseline_meals = baseline["plan"].get("selected_meals", [])
    avoid_meal = choose_meal(baseline_meals, preferred_slots={"lunch", "dinner"})
    append_event(profile, avoid_meal, "explicit_avoid", "round52_avoid")

    avoid_context = feedback_context(profile, load_feedback_events(RUNTIME_FEEDBACK_PATH))
    after_avoid = generate_plan(
        scenario_id="after_explicit_avoid",
        profile=profile,
        pool=pool,
        fooddb=fooddb,
        feedback_context=avoid_context,
    )
    avoided_recipe_id = str(avoid_meal.get("recipe_id", ""))
    avoided_removed = avoided_recipe_id not in selected_recipe_ids(after_avoid["plan"])

    disliked_meal = choose_other_meal(
        baseline_meals,
        excluded_recipe_ids={avoided_recipe_id},
        fallback_meals=after_avoid["plan"].get("selected_meals", []),
    )
    append_event(profile, disliked_meal, "disliked", "round52_dislike")
    disliked_context = feedback_context(profile, load_feedback_events(RUNTIME_FEEDBACK_PATH))
    after_disliked = generate_plan(
        scenario_id="after_disliked",
        profile=profile,
        pool=pool,
        fooddb=fooddb,
        feedback_context=disliked_context,
    )
    disliked_recipe_id = str(disliked_meal.get("recipe_id", ""))
    disliked_fit = candidate_feedback_fit(
        after_disliked["slot_candidates"],
        disliked_recipe_id,
    )
    disliked_penalized = disliked_fit is not None and disliked_fit < 0.50
    disliked_selection_changed = disliked_recipe_id not in selected_recipe_ids(
        after_disliked["plan"]
    )

    liked_meal = choose_liked_candidate(
        baseline["slot_candidates"],
        excluded_recipe_ids={avoided_recipe_id, disliked_recipe_id},
    )
    append_event(profile, liked_meal, "liked", "round52_like")
    liked_context = feedback_context(profile, load_feedback_events(RUNTIME_FEEDBACK_PATH))
    after_liked = generate_plan(
        scenario_id="after_liked",
        profile=profile,
        pool=pool,
        fooddb=fooddb,
        feedback_context=liked_context,
    )
    liked_recipe_id = str(liked_meal.get("recipe_id", ""))
    liked_fit = candidate_feedback_fit(after_liked["slot_candidates"], liked_recipe_id)
    liked_increased = liked_fit is not None and liked_fit > 0.50

    too_long_meal = choose_other_meal(
        after_liked["plan"].get("selected_meals", []),
        excluded_recipe_ids={avoided_recipe_id},
        fallback_meals=baseline_meals,
    )
    append_event(profile, too_long_meal, "too_long", "round52_too_long")
    too_long_context = feedback_context(profile, load_feedback_events(RUNTIME_FEEDBACK_PATH))
    after_too_long = generate_plan(
        scenario_id="after_too_long",
        profile=profile,
        pool=pool,
        fooddb=fooddb,
        feedback_context=too_long_context,
    )
    too_long_recipe_id = str(too_long_meal.get("recipe_id", ""))
    time_penalty = candidate_time_penalty(
        after_too_long["slot_candidates"],
        too_long_recipe_id,
    )
    time_penalty_applied = time_penalty is not None and time_penalty > 0

    run_rows = [
        run_row(baseline, "baseline_feedback_disabled"),
        run_row(after_avoid, "after_explicit_avoid"),
        run_row(after_disliked, "after_disliked"),
        run_row(after_liked, "after_liked"),
        run_row(after_too_long, "after_too_long"),
    ]
    meal_rows = []
    for result in [baseline, after_avoid, after_disliked, after_liked, after_too_long]:
        meal_rows.extend(meal_output_rows(result))

    final_context = too_long_context
    CONTEXT_OUT.write_text(
        json.dumps(final_context, indent=2),
        encoding="utf-8",
    )
    pd.DataFrame(run_rows).to_csv(RUNS_OUT, index=False)
    pd.DataFrame(meal_rows).to_csv(MEALS_OUT, index=False)
    SUMMARY_OUT.write_text(
        build_summary(
            baseline=baseline,
            avoided_recipe_id=avoided_recipe_id,
            avoided_removed=avoided_removed,
            disliked_recipe_id=disliked_recipe_id,
            disliked_penalized=disliked_penalized,
            disliked_selection_changed=disliked_selection_changed,
            liked_recipe_id=liked_recipe_id,
            liked_increased=liked_increased,
            too_long_recipe_id=too_long_recipe_id,
            time_penalty_applied=time_penalty_applied,
            final_plan_valid=plan_is_valid(after_too_long["plan"]),
            final_context=final_context,
        ),
        encoding="utf-8",
    )
    print("Round52 feedback evaluator written")
    print(f"summary={SUMMARY_OUT}")
    print(f"runs={RUNS_OUT}")
    print(f"meals={MEALS_OUT}")
    print(f"context={CONTEXT_OUT}")


def generate_plan(
    scenario_id: str,
    profile: dict[str, Any],
    pool: Any,
    fooddb: pd.DataFrame,
    feedback_context: dict[str, Any],
) -> dict[str, Any]:
    target = build_nutrition_target(profile)
    profile_context = build_profile_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=profile_context,
        feedback_preference_context=feedback_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=profile_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode="target_aware",
        feedback_preference_context=feedback_context,
    )
    slot_order = slot_order_for_target(target)
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot={
            slot: slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
            for slot in slot_order
        },
        target=target,
        slot_order=slot_order,
        config={
            "meal_realism_mode": "practical",
            "return_alternatives": True,
            "alternative_count": 3,
        },
    )
    plan["target"] = target_to_dict(target)
    plan["validation"] = validate_one_day_plan(plan, target)
    plan["quality_gate"] = evaluate_plan_quality(
        plan,
        target,
        config={"quality_gate": "demo_safe"},
    )
    plan["quality_gate_status"] = plan["quality_gate"]["quality_gate_status"]
    return {
        "scenario_id": scenario_id,
        "target": target,
        "filtered_candidate_count": len(filtered_candidates),
        "slot_candidate_count": len(slot_candidates),
        "filter_diagnostics": filtered_candidates.attrs.get("filter_diagnostics", {}),
        "slot_candidates": slot_candidates,
        "plan": plan,
    }


def feedback_context(
    profile: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    return build_feedback_preference_context(
        events=events,
        household_id=str(profile.get("household_id", "")),
        member_profile_id=str(profile.get("member_profile_id", "")),
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )


def append_event(
    profile: dict[str, Any],
    meal: dict[str, Any],
    feedback_type: str,
    run_id: str,
) -> None:
    append_feedback_event(
        {
            "household_id": profile.get("household_id"),
            "member_profile_id": profile.get("member_profile_id"),
            "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
            "recipe_id": meal.get("recipe_id"),
            "recipe_family_name": meal.get("recipe_family_name"),
            "display_name": meal.get("display_name"),
            "slot": meal.get("slot"),
            "feedback_type": feedback_type,
            "source": "test",
            "run_id": run_id,
            "plan_id": run_id,
            "notes": "round52_evaluator",
        },
        path=RUNTIME_FEEDBACK_PATH,
    )


def choose_meal(
    meals: list[dict[str, Any]],
    preferred_slots: set[str] | None = None,
) -> dict[str, Any]:
    preferred_slots = preferred_slots or set()
    for meal in meals:
        if str(meal.get("slot", "")) in preferred_slots:
            return meal
    if not meals:
        raise RuntimeError("Nu exista mese selectate pentru scenariul Round52.")
    return meals[0]


def choose_other_meal(
    meals: list[dict[str, Any]],
    excluded_recipe_ids: set[str],
    fallback_meals: list[dict[str, Any]],
) -> dict[str, Any]:
    for source in (meals, fallback_meals):
        for meal in source:
            recipe_id = str(meal.get("recipe_id", ""))
            if recipe_id and recipe_id not in excluded_recipe_ids:
                return meal
    return choose_meal(fallback_meals or meals)


def choose_liked_candidate(
    slot_candidates: pd.DataFrame,
    excluded_recipe_ids: set[str],
) -> dict[str, Any]:
    if slot_candidates.empty:
        raise RuntimeError("Nu exista candidati pentru like.")
    candidates = slot_candidates.copy()
    candidates = candidates.loc[
        ~candidates["recipe_id"].astype(str).isin(excluded_recipe_ids)
    ].copy()
    if candidates.empty:
        candidates = slot_candidates.copy()
    candidates = candidates.sort_values(
        ["score_preview", "macro_fit", "recipe_id", "portion_multiplier"],
        ascending=[False, False, True, True],
        kind="mergesort",
    )
    return candidates.iloc[0].to_dict()


def candidate_feedback_fit(
    slot_candidates: pd.DataFrame,
    recipe_id: str,
) -> float | None:
    rows = slot_candidates.loc[slot_candidates["recipe_id"].astype(str).eq(recipe_id)]
    if rows.empty:
        return None
    return float(pd.to_numeric(rows["feedback_fit"], errors="coerce").max())


def candidate_time_penalty(
    slot_candidates: pd.DataFrame,
    recipe_id: str,
) -> float | None:
    rows = slot_candidates.loc[slot_candidates["recipe_id"].astype(str).eq(recipe_id)]
    if rows.empty:
        return None
    return float(pd.to_numeric(rows["time_feedback_penalty"], errors="coerce").max())


def selected_recipe_ids(plan: dict[str, Any]) -> set[str]:
    return {
        str(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if isinstance(meal, dict) and meal.get("recipe_id")
    }


def plan_is_valid(plan: dict[str, Any]) -> bool:
    validation = plan.get("validation", {})
    return bool(validation.get("is_valid_for_checkpoint_1")) and (
        validation.get("validation_status") == "valid"
    )


def run_row(result: dict[str, Any], scenario_id: str) -> dict[str, Any]:
    plan = result["plan"]
    totals = plan.get("day_totals", {})
    validation = plan.get("validation", {})
    quality = plan.get("quality_gate", {})
    diagnostics = plan.get("selector_diagnostics", {})
    return {
        "scenario_id": scenario_id,
        "selected_recipe_ids": "|".join(sorted(selected_recipe_ids(plan))),
        "filtered_candidate_count": result["filtered_candidate_count"],
        "slot_candidate_count": result["slot_candidate_count"],
        "filtered_by_explicit_avoid": result["filter_diagnostics"].get(
            "filtered_by_explicit_avoid",
            0,
        ),
        "validation_status": validation.get("validation_status"),
        "is_valid_for_checkpoint_1": validation.get("is_valid_for_checkpoint_1"),
        "quality_gate_status": quality.get("quality_gate_status"),
        "day_loss": diagnostics.get("day_loss"),
        "total_kcal": totals.get("total_kcal"),
        "total_protein_g": totals.get("total_protein_g"),
        "total_carbs_g": totals.get("total_carbs_g"),
        "total_fat_g": totals.get("total_fat_g"),
        "effective_time_min_sum": totals.get("effective_time_min_sum"),
    }


def meal_output_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for meal in result["plan"].get("selected_meals", []):
        rows.append(
            {
                "scenario_id": result["scenario_id"],
                "slot": meal.get("slot"),
                "recipe_id": meal.get("recipe_id"),
                "display_name": meal.get("display_name"),
                "recipe_family_name": meal.get("recipe_family_name"),
                "portion_multiplier": meal.get("portion_multiplier"),
                "kcal": meal.get("kcal"),
                "protein_g": meal.get("protein_g"),
                "carbs_g": meal.get("carbs_g"),
                "fat_g": meal.get("fat_g"),
                "time_fit": meal.get("time_fit"),
                "time_feedback_penalty": meal.get("time_feedback_penalty"),
                "time_fit_reasons": "|".join(meal.get("time_fit_reasons") or []),
                "feedback_fit": meal.get("feedback_fit"),
                "feedback_reasons": "|".join(meal.get("feedback_reasons") or []),
                "score_preview": meal.get("score_preview"),
            }
        )
    return rows


def build_summary(
    baseline: dict[str, Any],
    avoided_recipe_id: str,
    avoided_removed: bool,
    disliked_recipe_id: str,
    disliked_penalized: bool,
    disliked_selection_changed: bool,
    liked_recipe_id: str,
    liked_increased: bool,
    too_long_recipe_id: str,
    time_penalty_applied: bool,
    final_plan_valid: bool,
    final_context: dict[str, Any],
) -> str:
    baseline_ids = sorted(selected_recipe_ids(baseline["plan"]))
    meta = final_context.get("meta", {})
    return "\n".join(
        [
            "Generator v1 Round52 feedback effect summary",
            f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
            f"feedback_events_path={RUNTIME_FEEDBACK_PATH}",
            "baseline_selected_recipes=" + ", ".join(baseline_ids),
            f"avoided_recipe_id={avoided_recipe_id}",
            f"avoided_recipe_removed={avoided_removed}",
            f"disliked_recipe_id={disliked_recipe_id}",
            f"disliked_selection_changed={disliked_selection_changed}",
            f"disliked_feedback_fit_penalized={disliked_penalized}",
            f"liked_recipe_id={liked_recipe_id}",
            f"liked_feedback_fit_increased={liked_increased}",
            f"too_long_recipe_id={too_long_recipe_id}",
            f"time_penalty_applied={time_penalty_applied}",
            f"plan_remains_valid={final_plan_valid}",
            f"feedback_event_count={meta.get('event_count')}",
            "",
            "Conclusion",
            "explicit_avoid works as a hard filter if avoided_recipe_removed=True.",
            "liked/disliked influence score if their feedback_fit booleans are True.",
            "too_long affects time if time_penalty_applied=True.",
        ]
    )


def slot_order_for_target(target: NutritionTarget) -> list[str]:
    preferred = ["breakfast", "lunch", "dinner", "snack"]
    known = [slot for slot in preferred if slot in target.slot_targets]
    extra = [slot for slot in target.slot_targets if slot not in known]
    return known + extra


def target_to_dict(target: NutritionTarget) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


if __name__ == "__main__":
    main()
