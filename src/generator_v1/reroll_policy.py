from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd

from src.generator_v1.day_selector_balanced import select_one_day_plan_balanced
from src.generator_v1.plan_quality_gate import evaluate_plan_quality
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.target_builder import NutritionTarget


VARIED_ATTEMPT_MODES = ["avoid_recent", "soft"]


def select_quality_gated_reroll(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | dict[str, object],
    slot_order: Sequence[str],
    recent_recipe_ids: set[str] | Sequence[str],
    base_config: dict[str, object],
) -> dict[str, object]:
    recent_ids = _recent_recipe_ids(recent_recipe_ids)
    best_plan = _select_and_evaluate(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=target,
        slot_order=slot_order,
        config={
            **base_config,
            "diversity_mode": "none",
            "recent_recipe_ids": [],
        },
    )
    attempted_modes: list[dict[str, object]] = []

    for mode in VARIED_ATTEMPT_MODES:
        candidate_plan = _select_and_evaluate(
            slot_candidates_by_slot=slot_candidates_by_slot,
            target=target,
            slot_order=slot_order,
            config={
                **base_config,
                "diversity_mode": mode,
                "recent_recipe_ids": sorted(recent_ids),
            },
        )
        quality_gate = candidate_plan["quality_gate"]
        attempted_modes.append(
            _attempt_record(
                mode=mode,
                plan=candidate_plan,
                fallback_used=False,
            )
        )
        if quality_gate["quality_gate_status"] == "accept":
            return _finalise_plan(
                plan=candidate_plan,
                attempted_modes=attempted_modes,
                selected_mode=mode,
                fallback_used=False,
                recent_recipe_count=len(recent_ids),
            )
        if quality_gate["quality_gate_status"] == "review":
            candidate_plan.setdefault("warnings", []).append(
                "Quality gate marked varied plan for review."
            )
            return _finalise_plan(
                plan=candidate_plan,
                attempted_modes=attempted_modes,
                selected_mode=mode,
                fallback_used=False,
                recent_recipe_count=len(recent_ids),
            )

    best_quality_gate = best_plan["quality_gate"]
    attempted_modes.append(
        _attempt_record(
            mode="fallback_best",
            plan=best_plan,
            fallback_used=True,
        )
    )
    best_plan.setdefault("warnings", []).append(
        "No quality-safe varied plan found; returned best plan."
    )
    if best_quality_gate["quality_gate_status"] == "reject":
        best_plan.setdefault("warnings", []).append(
            "Fallback best plan also fails the quality gate."
        )
    return _finalise_plan(
        plan=best_plan,
        attempted_modes=attempted_modes,
        selected_mode="fallback_best",
        fallback_used=True,
        recent_recipe_count=len(recent_ids),
    )


def _select_and_evaluate(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | dict[str, object],
    slot_order: Sequence[str],
    config: dict[str, object],
) -> dict[str, object]:
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=target,
        slot_order=slot_order,
        config=config,
    )
    plan["validation"] = validate_one_day_plan(plan, target)
    plan["quality_gate"] = evaluate_plan_quality(
        plan,
        target,
        config={"quality_gate": "demo_safe"},
    )
    return plan


def _finalise_plan(
    plan: dict[str, object],
    attempted_modes: list[dict[str, object]],
    selected_mode: str,
    fallback_used: bool,
    recent_recipe_count: int,
) -> dict[str, object]:
    quality_gate = plan.get("quality_gate", {})
    if not isinstance(quality_gate, dict):
        quality_gate = {}
    diagnostics = {
        "attempted_modes": attempted_modes,
        "selected_mode": selected_mode,
        "quality_gate_status": quality_gate.get("quality_gate_status", "review"),
        "quality_gate_reasons": quality_gate.get("quality_gate_reasons", []),
        "quality_gate_score": quality_gate.get("quality_gate_score", 0.0),
        "fallback_used": fallback_used,
        "recent_recipe_count_used": recent_recipe_count,
    }
    plan["quality_gate_reroll_diagnostics"] = diagnostics
    plan["quality_gate_status"] = diagnostics["quality_gate_status"]
    plan["quality_gate_reasons"] = diagnostics["quality_gate_reasons"]
    plan["quality_gate_score"] = diagnostics["quality_gate_score"]
    plan["quality_gate_fallback_used"] = fallback_used
    plan["quality_gate_selected_mode"] = selected_mode
    return plan


def _attempt_record(
    mode: str,
    plan: dict[str, object],
    fallback_used: bool,
) -> dict[str, object]:
    quality_gate = plan.get("quality_gate", {})
    if not isinstance(quality_gate, dict):
        quality_gate = {}
    diagnostics = plan.get("selector_diagnostics", {})
    if not isinstance(diagnostics, dict):
        diagnostics = {}
    return {
        "mode": mode,
        "quality_gate_status": quality_gate.get("quality_gate_status"),
        "quality_gate_reasons": quality_gate.get("quality_gate_reasons", []),
        "base_day_loss": diagnostics.get("base_day_loss"),
        "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
        "fallback_used": fallback_used,
        "selected_recipe_ids": [
            str(meal.get("recipe_id"))
            for meal in plan.get("selected_meals", [])
            if isinstance(meal, dict) and meal.get("recipe_id")
        ],
    }


def _recent_recipe_ids(value: set[str] | Sequence[str]) -> set[str]:
    return {str(item).strip() for item in value if str(item).strip()}
