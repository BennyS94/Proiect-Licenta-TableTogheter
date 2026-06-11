from __future__ import annotations

import json
import math
import re
import uuid
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pandas as pd

from src.generator_v1.candidate_diagnostics import build_candidate_diagnostics
from src.generator_v1.candidate_filter import (
    build_household_preference_context as build_profile_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (
    V1_2_DEMO_FINAL_PROFILE,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.feedback_adapter import (
    build_household_preference_context as build_feedback_preference_context,
)
from src.generator_v1.feedback_store import (
    DEFAULT_FEEDBACK_EVENTS_PATH,
    feedback_event_to_dict,
    load_feedback_events,
)
from src.generator_v1.grocery_list import build_grocery_list
from src.generator_v1.household_generator import (
    HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
    HOUSEHOLD_MODE_OFF,
    build_household_aggregate_target,
    build_household_slot_candidates,
    build_member_targets,
    filter_household_profile_members,
    generate_household_plan,
    load_household_profile,
)
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.nutrition_cache_diagnostics import (
    build_nutrition_cache_diagnostics,
)
from src.generator_v1.plan_quality_gate import evaluate_plan_quality
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.recipe_similarity import (
    build_recipe_similarity_features,
    find_similar_recipes,
)
from src.generator_v1.reroll_policy import select_quality_gated_reroll
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import build_nutrition_target
from src.generator_v1_cli import (
    _apply_multi_day_defaults,
    _balanced_selector_config,
    _household_context_profile,
    _household_generation_config,
    _multi_day_selector_config,
    _pool_summary,
    _primary_household_member,
    _profile_guard_blocks,
    _profile_guard_result,
    _resolve_dataset_paths,
    _select_one_day_plan,
    _slot_candidates_by_slot,
    _slot_order,
    _target_to_dict,
    _should_use_quality_gated_reroll,
)


DEFAULT_GENERATION_OPTIONS = {
    "selection_mode": "balanced_day",
    "alternative_count": 3,
    "diversity_mode": "none",
    "recent_recipe_ids": "",
    "portion_policy": "target_aware",
    "meal_realism_mode": "practical",
    "quality_gate": "demo_safe",
    "profile_guard": "demo",
    "allow_unsupported_profile": False,
    "multi_day_no_repeat_policy": "hard",
    "day_candidate_pool_size": 75,
    "multi_day_speed_mode": "fast",
    "day_candidate_builder": "direct_from_slots",
    "direct_slot_shortlist_size": 12,
    "include_grocery_list": False,
    "include_purchase_suggestions": False,
    "include_price_estimates": False,
    "include_pantry_basics": False,
    "feedback_enabled": True,
}

INTERNAL_PATH_KEYS = {
    "recipes_path",
    "ingredients_path",
    "nutrition_path",
    "feedback_events_path",
    "household_profile_path",
}
DEFAULT_DEMO_HOUSEHOLD_PROFILE_PATH = Path("profiles/household_profile_demo_v1.json")
RECIPE_ALTERNATIVES_APPROVAL_MODES = {
    "approved_only",
    "include_review",
    "include_rejected_debug",
}
TIME_OUTPUT_FIELDS = (
    "total_time_min",
    "total_elapsed_time_min",
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "effective_time_min_for_scoring",
    "time_confidence",
    "time_estimation_method",
    "time_warnings",
)
PREP_ONLY_INGREDIENT_NAMES = {
    "chopped",
    "cubed",
    "diced",
    "melted",
    "minced",
    "pressed",
    "sliced",
}


def generate_individual_plan_from_request(request: dict[str, Any]) -> dict[str, Any]:
    args = _args_from_request(request)
    profile = _member_profile_from_request(request)
    target = build_nutrition_target(profile)
    profile_guard_result = _profile_guard_result(args, profile, target)
    if _profile_guard_blocks(profile_guard_result, args):
        return to_json_safe(
            {
                "status": "blocked",
                "error_code": "profile_guard_blocked",
                "generation_type": "individual",
                "dataset_profile": args.dataset_profile,
                "days": args.days,
                "member_profile_id": profile.get("member_profile_id", ""),
                "profile_guard": profile_guard_result,
                "warnings": ["profile_guard_blocked"],
            }
        )

    pool = load_recipe_candidate_pool(
        recipes_path=args.recipes,
        ingredients_path=args.ingredients,
        nutrition_path=args.nutrition,
        dataset_profile=args.dataset_profile,
    )
    fooddb = load_fooddb_current()
    preference_context = build_profile_preference_context(profile)
    feedback_context = _feedback_context_for_profile(args, profile, request)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
        feedback_preference_context=feedback_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode=args.portion_policy,
        feedback_preference_context=feedback_context,
        health_and_diet_preferences=preference_context.health_and_diet_preferences,
    )
    candidate_diagnostics = build_candidate_diagnostics(
        slot_candidates=slot_candidates,
        slot_targets=target.slot_targets,
    )
    nutrition_cache_diagnostics = build_nutrition_cache_diagnostics(
        recipes=pool.recipes,
        nutrition=pool.nutrition,
        candidates=pool.candidates,
        eligible_candidates=pool.eligible_candidates,
    )

    if int(args.days or 1) > 1:
        plan = generate_multi_day_plan(
            profile=profile,
            target=target,
            slot_candidates=slot_candidates,
            days=args.days,
            config=_multi_day_selector_config(args),
        )
        plan["candidate_diagnostics"] = candidate_diagnostics
        plan["nutrition_cache_diagnostics"] = nutrition_cache_diagnostics
        plan["feedback_context"] = feedback_context
        if profile_guard_result is not None:
            plan["profile_guard"] = profile_guard_result
    else:
        plan = _generate_one_day_plan(
            args=args,
            target=target,
            slot_candidates=slot_candidates,
            candidate_diagnostics=candidate_diagnostics,
            feedback_context=feedback_context,
            profile_guard_result=profile_guard_result,
        )

    plan["pool_summary"] = _pool_summary(
        args,
        pool,
        filtered_candidates,
        slot_candidates,
    )
    _attach_meal_ingredient_amounts(plan, pool.ingredients)

    grocery_list = None
    if _generation_option(args, "include_grocery_list", False):
        grocery_list = _build_grocery_list_with_loaded_data(
            plan=plan,
            args=args,
            recipe_ingredients_df=pool.ingredients,
            fooddb_df=fooddb,
            generation_type="individual",
            plan_id="",
        )

    plan_id = _new_id("plan_individual")
    response = {
        "status": "ok",
        "plan_id": plan_id,
        "generation_type": "individual",
        "dataset_profile": args.dataset_profile,
        "member_profile_id": profile.get("member_profile_id", ""),
        "days": int(args.days or 1),
        "daily_plan": _daily_plan_view(plan),
        "generator_plan": _strip_internal_paths(plan),
        "grocery_list": _with_plan_id(grocery_list, plan_id) if grocery_list else None,
        "feedback_context_summary": _feedback_context_summary(feedback_context),
        "warnings": _response_warnings(plan),
        "diagnostics_summary": _individual_diagnostics_summary(
            args=args,
            plan=plan,
            profile_guard_result=profile_guard_result,
            grocery_list=grocery_list,
        ),
    }
    return to_json_safe(response)


def generate_household_plan_from_request(request: dict[str, Any]) -> dict[str, Any]:
    args = _args_from_request(request, household=True)
    household_profile = _household_profile_from_request(request)
    selected_member_ids = _clean_list(request.get("selected_member_ids"))
    if selected_member_ids:
        household_profile = filter_household_profile_members(
            household_profile,
            selected_member_ids,
        )
    if not household_profile.get("members"):
        raise ValueError("household_profile nu are membri selectati.")

    member_targets = build_member_targets(household_profile)
    household_target = build_household_aggregate_target(member_targets)
    primary_member = _primary_household_member(household_profile)
    primary_context_profile = _household_context_profile(household_profile, primary_member)
    preference_context = build_profile_preference_context(primary_context_profile)
    feedback_context = _feedback_context_for_household(args, household_profile, request)
    pool = load_recipe_candidate_pool(
        recipes_path=args.recipes,
        ingredients_path=args.ingredients,
        nutrition_path=args.nutrition,
        dataset_profile=args.dataset_profile,
    )
    fooddb = load_fooddb_current()
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
        feedback_preference_context=feedback_context,
    )
    slot_candidates = build_slot_candidates(
        target=household_target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode="target_aware",
        feedback_preference_context=feedback_context,
        health_and_diet_preferences=preference_context.health_and_diet_preferences,
    )
    household_config = _household_generation_config(args)
    household_config["recipe_ingredients_df"] = pool.ingredients
    household_candidates = build_household_slot_candidates(
        slot_candidates,
        member_targets,
        household_config,
    )
    candidate_diagnostics = build_candidate_diagnostics(
        slot_candidates=household_candidates,
        slot_targets=household_target.slot_targets,
    )
    plan = generate_household_plan(
        household_profile,
        slot_candidates=household_candidates,
        individual_slot_candidates=slot_candidates,
        days=args.days,
        config=household_config,
        profile=primary_member,
    )
    plan["candidate_diagnostics"] = candidate_diagnostics
    plan["feedback_context"] = feedback_context
    plan["pool_summary"] = _pool_summary(
        args,
        pool,
        filtered_candidates,
        household_candidates,
    )
    _attach_meal_ingredient_amounts(plan, pool.ingredients)

    grocery_list = None
    if _generation_option(args, "include_grocery_list", False):
        grocery_list = _build_grocery_list_with_loaded_data(
            plan=_household_plan_for_grocery(plan),
            args=args,
            recipe_ingredients_df=pool.ingredients,
            fooddb_df=fooddb,
            generation_type="household",
            plan_id="",
        )

    plan_id = _new_id("plan_household")
    response = {
        "status": "ok",
        "household_plan_id": plan_id,
        "generation_type": "household",
        "dataset_profile": args.dataset_profile,
        "household_id": household_profile.get("household_id", ""),
        "selected_members": _selected_members(household_profile),
        "member_targets": _member_target_rows(plan),
        "days": int(args.days or 1),
        "daily_plan": _daily_plan_view(plan),
        "per_member_menus": _per_member_menus(plan),
        "shared_meals": _shared_meals(plan),
        "household_grocery_list": _with_plan_id(grocery_list, plan_id)
        if grocery_list
        else None,
        "household_grocery_scaling": plan.get("grocery_scaling", []),
        "member_macro_summaries": plan.get("member_daily_rows", []),
        "generator_plan": _strip_internal_paths(plan),
        "feedback_context_summary": _feedback_context_summary(feedback_context),
        "warnings": _response_warnings(plan),
        "diagnostics_summary": _household_diagnostics_summary(
            args=args,
            plan=plan,
            grocery_list=grocery_list,
        ),
    }
    return to_json_safe(response)


def build_grocery_list_for_plan(
    plan: dict[str, Any],
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    options_data = dict(options or {})
    request = {
        "dataset_profile": options_data.get("dataset_profile")
        or _nested_get(plan, ("pool_summary", "dataset_profile"))
        or V1_2_DEMO_FINAL_PROFILE,
        "days": options_data.get("days") or _plan_day_count(plan),
        "generation_options": options_data,
    }
    args = _args_from_request(request)
    pool = load_recipe_candidate_pool(
        recipes_path=args.recipes,
        ingredients_path=args.ingredients,
        nutrition_path=args.nutrition,
        dataset_profile=args.dataset_profile,
    )
    fooddb = load_fooddb_current()
    generation_type = str(options_data.get("generation_type") or plan.get("generation_type") or "")
    grocery_plan = _household_plan_for_grocery(plan) if _is_household_plan(plan) else plan
    return _build_grocery_list_with_loaded_data(
        plan=grocery_plan,
        args=args,
        recipe_ingredients_df=pool.ingredients,
        fooddb_df=fooddb,
        generation_type=generation_type or ("household" if _is_household_plan(plan) else "individual"),
        plan_id=str(options_data.get("plan_id") or plan.get("plan_id") or ""),
    )


def preview_meal_replacement_from_request(request: dict[str, Any]) -> dict[str, Any]:
    from src.generator_v1.plan_replacement import preview_meal_replacement

    return preview_meal_replacement(request)


def apply_meal_replacement_from_request(request: dict[str, Any]) -> dict[str, Any]:
    from src.generator_v1.plan_replacement import apply_meal_replacement

    return apply_meal_replacement(request)


def build_feedback_context_from_request(request: dict[str, Any]) -> dict[str, Any]:
    events = _feedback_events_from_request(request)
    context = build_feedback_preference_context(
        events=events,
        household_id=_clean_text(request.get("household_id")),
        member_profile_id=_clean_text(request.get("member_profile_id")),
        dataset_profile=_clean_text(request.get("dataset_profile")) or V1_2_DEMO_FINAL_PROFILE,
    )
    return to_json_safe(
        {
            "status": "ok",
            "feedback_context": context,
            "summary": _feedback_context_summary(context),
        }
    )


def submit_feedback_event_from_request(request: dict[str, Any]) -> dict[str, Any]:
    event = feedback_event_to_dict(
        event_id=request.get("event_id"),
        created_at=request.get("created_at"),
        household_id=request.get("household_id"),
        member_profile_id=request.get("member_profile_id"),
        dataset_profile=request.get("dataset_profile") or V1_2_DEMO_FINAL_PROFILE,
        recipe_id=request.get("recipe_id"),
        recipe_family_name=request.get("recipe_family_name"),
        display_name=request.get("display_name"),
        slot=request.get("slot"),
        feedback_type=request.get("feedback_type"),
        source=request.get("source") or "test",
        run_id=request.get("run_id"),
        plan_id=request.get("plan_id"),
        notes=request.get("notes"),
    )
    existing_events = _feedback_events_from_request(request)
    context = build_feedback_preference_context(
        events=[*existing_events, event],
        household_id=event.get("household_id"),
        member_profile_id=event.get("member_profile_id"),
        dataset_profile=event.get("dataset_profile"),
    )
    return to_json_safe(
        {
            "status": "ok",
            "stored": False,
            "storage_owner": "backend_sqlite",
            "event": event,
            "context_summary": _feedback_context_summary(context),
        }
    )


def get_recipe_alternatives_from_request(request: dict[str, Any]) -> dict[str, Any]:
    recipe_id = _clean_text(request.get("recipe_id"))
    if not recipe_id:
        return to_json_safe(
            {
                "status": "error",
                "error_code": "recipe_id_required",
                "message": "recipe_id este obligatoriu.",
            }
        )

    service_request = _alternatives_service_request(request)
    args = _args_from_request(service_request)
    top_k = _bounded_int(request.get("top_k"), default=5, minimum=1, maximum=25)
    candidate_pool_k = _bounded_int(
        request.get("candidate_pool_k"),
        default=20,
        minimum=1,
        maximum=75,
    )
    approval_mode = _approval_mode(request.get("approval_mode"))
    warnings: list[str] = []

    profile = _member_profile_from_request(service_request)
    target = build_nutrition_target(profile)
    preference_context = build_profile_preference_context(profile)
    feedback_context = _feedback_context_for_profile(args, profile, service_request)
    pool = load_recipe_candidate_pool(
        recipes_path=args.recipes,
        ingredients_path=args.ingredients,
        nutrition_path=args.nutrition,
        dataset_profile=args.dataset_profile,
    )
    fooddb = load_fooddb_current()
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
        feedback_preference_context=feedback_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode=args.portion_policy,
        feedback_preference_context=feedback_context,
        health_and_diet_preferences=preference_context.health_and_diet_preferences,
    )
    features = build_recipe_similarity_features(
        pool.recipes,
        pool.nutrition,
        pool.ingredients,
    )
    source_rows = features.loc[features["recipe_id"].astype(str).eq(recipe_id)]
    if source_rows.empty:
        return to_json_safe(
            {
                "status": "error",
                "error_code": "recipe_not_found",
                "recipe_id": recipe_id,
                "dataset_profile": args.dataset_profile,
                "message": "Reteta sursa nu exista in datasetul cerut.",
            }
        )

    source_recipe = source_rows.iloc[0]
    slot = _normalize_slot(request.get("slot")) or _infer_slot(source_recipe)
    if not _normalize_slot(request.get("slot")) and slot:
        warnings.append(f"slot_inferred:{slot}")
    if not slot:
        warnings.append("slot_missing_approval_uses_best_candidate_slot")

    neighbors = find_similar_recipes(
        recipe_id,
        features,
        top_k=candidate_pool_k,
        filters={
            "slot": slot,
            "same_slot": False if slot else True,
            "active_only": True,
        },
    )

    all_alternatives = []
    returned_alternatives = []
    for neighbor in neighbors:
        candidate_recipe_id = _clean_text(neighbor.get("candidate_recipe_id"))
        approval = _approve_recipe_alternative(
            slot_candidates=slot_candidates,
            pool=pool,
            filtered_candidates=filtered_candidates,
            feedback_context=feedback_context,
            slot=slot,
            candidate_recipe_id=candidate_recipe_id,
        )
        item = _recipe_alternative_item(neighbor, approval)
        all_alternatives.append(item)
        if _include_alternative_status(item["approval_status"], approval_mode):
            returned_alternatives.append(item)
        if len(returned_alternatives) >= top_k:
            continue

    response = {
        "status": "ok",
        "recipe_id": recipe_id,
        "source_recipe": _source_recipe_view(source_recipe),
        "slot": slot,
        "dataset_profile": args.dataset_profile,
        "approval_mode": approval_mode,
        "alternatives": returned_alternatives[:top_k],
        "summary": _recipe_alternatives_summary(
            all_alternatives,
            returned_alternatives[:top_k],
        ),
        "feedback_context_summary": _feedback_context_summary(feedback_context),
        "warnings": warnings,
    }
    return to_json_safe(response)


def _alternatives_service_request(request: Mapping[str, Any]) -> dict[str, Any]:
    service_request = dict(request)
    service_request["days"] = 1
    options = dict(service_request.get("generation_options") or {})
    for key in (
        "feedback_enabled",
        "selection_mode",
        "portion_policy",
        "meal_realism_mode",
        "quality_gate",
        "profile_guard",
    ):
        if service_request.get(key) is not None:
            options[key] = service_request[key]
    options.setdefault("selection_mode", "balanced_day")
    options.setdefault("portion_policy", "target_aware")
    options.setdefault("meal_realism_mode", "practical")
    options.setdefault("quality_gate", "demo_safe")
    options.setdefault("profile_guard", "demo")
    options.setdefault("feedback_enabled", True)
    service_request["generation_options"] = options
    return service_request


def _approve_recipe_alternative(
    *,
    slot_candidates: pd.DataFrame,
    pool: Any,
    filtered_candidates: pd.DataFrame,
    feedback_context: Mapping[str, Any],
    slot: str,
    candidate_recipe_id: str,
) -> dict[str, Any]:
    hard_reasons = _hard_rejection_reasons(
        pool=pool,
        filtered_candidates=filtered_candidates,
        feedback_context=feedback_context,
        slot=slot,
        candidate_recipe_id=candidate_recipe_id,
    )
    rows = _slot_candidate_rows(slot_candidates, slot, candidate_recipe_id)
    if rows.empty:
        hard_reasons = hard_reasons or ["not_in_generator_candidate_pool_for_slot"]
        return {
            "approval_status": "rejected",
            "approval_reasons": [],
            "rejection_reasons": _dedupe_texts(hard_reasons),
            "warnings": ["generator_candidate_missing_for_requested_slot"],
            "diagnostics": {},
        }

    row = _best_slot_candidate_row(rows)
    hard_reasons.extend(_row_hard_reasons(row))
    review_reasons = _row_review_reasons(row)
    approval_reasons = _row_approval_reasons(row)

    if hard_reasons:
        status = "rejected"
    elif review_reasons:
        status = "review"
    else:
        status = "approved"

    return {
        "approval_status": status,
        "approval_reasons": approval_reasons,
        "rejection_reasons": _dedupe_texts(hard_reasons),
        "warnings": _dedupe_texts([*review_reasons, *_warnings_from_candidate_row(row)]),
        "diagnostics": {
            "slot_used_for_approval": row.get("slot"),
            "portion_multiplier": _to_float(row.get("portion_multiplier")),
            "macro_fit": _to_float(row.get("macro_fit")),
            "kcal_fit": _to_float(row.get("kcal_fit")),
            "protein_fit": _to_float(row.get("protein_fit")),
            "time_fit": _to_float(row.get("time_fit")),
            "slot_fit": _to_float(row.get("slot_fit")),
            "meal_realism_score": _to_float(row.get("meal_realism_practical_score")),
            "nutrition_quality": _to_float(row.get("nutrition_quality")),
            "score_preview": _to_float(row.get("score_preview")),
        },
    }


def _hard_rejection_reasons(
    *,
    pool: Any,
    filtered_candidates: pd.DataFrame,
    feedback_context: Mapping[str, Any],
    slot: str,
    candidate_recipe_id: str,
) -> list[str]:
    reasons: list[str] = []
    candidate_rows = pool.candidates.loc[
        pool.candidates["recipe_id"].astype(str).eq(candidate_recipe_id)
    ]
    if candidate_rows.empty:
        return ["candidate_not_found"]

    candidate = candidate_rows.iloc[0]
    if _to_float(candidate.get("is_active")) != 1.0:
        reasons.append("inactive_recipe")
    if _missing_required_nutrition(candidate):
        reasons.append("missing_nutrition")
    if slot and not _recipe_allows_slot(candidate, slot):
        reasons.append("slot_incompatible")
    explicit_avoid = candidate_recipe_id in _feedback_banned_recipe_ids(feedback_context)
    if explicit_avoid:
        reasons.append("explicit_avoid")

    eligible_ids = set(pool.eligible_candidates["recipe_id"].astype(str))
    filtered_ids = set(filtered_candidates["recipe_id"].astype(str))
    if (
        candidate_recipe_id in eligible_ids
        and candidate_recipe_id not in filtered_ids
        and not explicit_avoid
    ):
        reasons.append("restricted_ingredient")
    return _dedupe_texts(reasons)


def _slot_candidate_rows(
    slot_candidates: pd.DataFrame,
    slot: str,
    candidate_recipe_id: str,
) -> pd.DataFrame:
    if slot_candidates.empty or not candidate_recipe_id:
        return pd.DataFrame()
    mask = slot_candidates["recipe_id"].astype(str).eq(candidate_recipe_id)
    if slot:
        mask &= slot_candidates["slot"].astype(str).str.lower().eq(slot)
    return slot_candidates.loc[mask].copy()


def _best_slot_candidate_row(rows: pd.DataFrame) -> pd.Series:
    sort_columns = [
        column
        for column in ("score_preview", "macro_fit", "time_fit", "slot_fit")
        if column in rows.columns
    ]
    if not sort_columns:
        return rows.iloc[0]
    return rows.sort_values(
        by=sort_columns,
        ascending=[False] * len(sort_columns),
        na_position="last",
    ).iloc[0]


def _row_hard_reasons(row: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    macro_fit = _to_float(row.get("macro_fit"))
    time_fit = _to_float(row.get("time_fit"))
    nutrition_quality = _to_float(row.get("nutrition_quality"))
    if bool(row.get("realism_hard_reject")):
        reasons.append("realism_warning_severe")
    if macro_fit is None or macro_fit < 0.35:
        reasons.append("macro_too_far")
    if time_fit is None or time_fit < 0.15:
        reasons.append("time_too_long")
    if nutrition_quality is None or nutrition_quality < 0.50:
        reasons.append("missing_nutrition")
    return _dedupe_texts(reasons)


def _row_review_reasons(row: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    macro_fit = _to_float(row.get("macro_fit"))
    time_fit = _to_float(row.get("time_fit"))
    slot_fit = _to_float(row.get("slot_fit"))
    realism_score = _to_float(row.get("meal_realism_practical_score"))
    nutrition_quality = _to_float(row.get("nutrition_quality"))
    if macro_fit is not None and 0.35 <= macro_fit < 0.65:
        reasons.append("macro_review")
    if time_fit is not None and 0.15 <= time_fit < 0.25:
        reasons.append("time_review")
    if slot_fit is not None and slot_fit < 0.40:
        reasons.append("slot_review")
    if realism_score is not None and realism_score < 0.65:
        reasons.append("realism_review")
    if nutrition_quality is not None and 0.50 <= nutrition_quality < 0.65:
        reasons.append("nutrition_review")
    if bool(row.get("is_slot_suspicious")):
        reasons.append("slot_suspicious")
    return _dedupe_texts(reasons)


def _row_approval_reasons(row: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    checks = (
        ("macro_fit", 0.65, "macro_fit_ok"),
        ("nutrition_quality", 0.65, "nutrition_quality_ok"),
        ("time_fit", 0.25, "time_fit_ok"),
        ("slot_fit", 0.40, "slot_fit_ok"),
        ("meal_realism_practical_score", 0.65, "meal_realism_ok"),
    )
    for column, threshold, reason in checks:
        value = _to_float(row.get(column))
        if value is not None and value >= threshold:
            reasons.append(reason)
    return reasons


def _recipe_alternative_item(
    neighbor: Mapping[str, Any],
    approval: Mapping[str, Any],
) -> dict[str, Any]:
    warnings = [
        *_split_reason_text(neighbor.get("warnings")),
        *list(approval.get("warnings", [])),
    ]
    return {
        "recipe_id": _clean_text(neighbor.get("candidate_recipe_id")),
        "display_name": _clean_text(neighbor.get("candidate_display_name")),
        "similarity_score": _to_float(neighbor.get("similarity_score")),
        "approval_status": approval.get("approval_status"),
        "approval_reasons": list(approval.get("approval_reasons", [])),
        "rejection_reasons": list(approval.get("rejection_reasons", [])),
        "macro_delta": {
            "kcal": _to_float(neighbor.get("macro_delta_kcal")),
            "protein_g": _to_float(neighbor.get("macro_delta_protein")),
            "carbs_g": _to_float(neighbor.get("macro_delta_carbs")),
            "fat_g": _to_float(neighbor.get("macro_delta_fat")),
        },
        "time_delta_min": _to_float(neighbor.get("time_delta_min")),
        "why_similar": _split_reason_text(neighbor.get("why_similar")),
        "warnings": _dedupe_texts(warnings),
        "diagnostics": dict(approval.get("diagnostics", {})),
    }


def _source_recipe_view(source_recipe: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "recipe_id": _clean_text(source_recipe.get("recipe_id")),
        "display_name": _clean_text(source_recipe.get("display_name")),
        "allowed_slots": sorted(_as_slot_set(source_recipe.get("allowed_slots"))),
        "recipe_kind": _clean_text(source_recipe.get("recipe_kind")),
        "recipe_category": _clean_text(source_recipe.get("recipe_category")),
        "recipe_family_name": _clean_text(source_recipe.get("recipe_family_name")),
        "kcal_per_serving": _to_float(source_recipe.get("kcal_per_serving")),
        "protein_g_per_serving": _to_float(source_recipe.get("protein_g_per_serving")),
        "carbs_g_per_serving": _to_float(source_recipe.get("carbs_g_per_serving")),
        "fat_g_per_serving": _to_float(source_recipe.get("fat_g_per_serving")),
        "effective_time_min": _to_float(source_recipe.get("effective_time_min")),
    }


def _recipe_alternatives_summary(
    all_alternatives: list[dict[str, Any]],
    returned_alternatives: list[dict[str, Any]],
) -> dict[str, Any]:
    status_counts = {
        "approved": 0,
        "review": 0,
        "rejected": 0,
    }
    for item in all_alternatives:
        status = _clean_text(item.get("approval_status"))
        if status in status_counts:
            status_counts[status] += 1
    return {
        "candidate_count": len(all_alternatives),
        "returned_count": len(returned_alternatives),
        "approved_count": status_counts["approved"],
        "review_count": status_counts["review"],
        "rejected_count": status_counts["rejected"],
    }


def _approval_mode(value: Any) -> str:
    mode = _clean_text(value) or "include_review"
    if mode not in RECIPE_ALTERNATIVES_APPROVAL_MODES:
        return "include_review"
    return mode


def _include_alternative_status(status: Any, approval_mode: str) -> bool:
    status_text = _clean_text(status)
    if approval_mode == "include_rejected_debug":
        return status_text in {"approved", "review", "rejected"}
    if approval_mode == "include_review":
        return status_text in {"approved", "review"}
    return status_text == "approved"


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default
    return max(minimum, min(maximum, result))


def _normalize_slot(value: Any) -> str:
    text = _clean_text(value).lower()
    return text if text in {"breakfast", "lunch", "dinner", "snack"} else ""


def _infer_slot(source_recipe: Mapping[str, Any]) -> str:
    slots = _as_slot_set(source_recipe.get("allowed_slots"))
    for slot in ("breakfast", "lunch", "dinner", "snack"):
        if slot in slots:
            return slot
    return ""


def _recipe_allows_slot(recipe: Mapping[str, Any], slot: str) -> bool:
    if not slot:
        return True
    if "allowed_slots_json" not in recipe:
        return True
    return slot in _as_slot_set(recipe.get("allowed_slots_json"))


def _missing_required_nutrition(recipe: Mapping[str, Any]) -> bool:
    return any(
        _to_float(recipe.get(column)) is None
        for column in (
            "energy_kcal_per_serving",
            "protein_g_per_serving",
            "carbs_g_per_serving",
            "fat_g_per_serving",
        )
    )


def _feedback_banned_recipe_ids(feedback_context: Mapping[str, Any]) -> set[str]:
    hard_filters = feedback_context.get("hard_filters", {})
    if not isinstance(hard_filters, Mapping):
        return set()
    return {
        _clean_text(item)
        for item in hard_filters.get("banned_recipe_ids", [])
        if _clean_text(item)
    }


def _warnings_from_candidate_row(row: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    for column in (
        "time_warnings",
        "portion_policy_warnings",
        "meal_realism_practical_flags",
        "nutrition_quality_reasons",
        "slot_suspicion_reasons",
        "realism_reject_reason",
    ):
        value = row.get(column)
        if isinstance(value, list):
            warnings.extend(_clean_text(item) for item in value if _clean_text(item))
        else:
            warnings.extend(_split_reason_text(value))
    return _dedupe_texts(warnings)


def _split_reason_text(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    text = _clean_text(value)
    if not text:
        return []
    normalized = text.replace(";", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def _as_slot_set(value: Any) -> set[str]:
    if isinstance(value, set):
        return {_clean_text(item).lower() for item in value if _clean_text(item)}
    if isinstance(value, (list, tuple)):
        return {_clean_text(item).lower() for item in value if _clean_text(item)}
    text = _clean_text(value)
    if not text:
        return set()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = [part.strip() for part in text.split(",") if part.strip()]
    if isinstance(parsed, list):
        return {_clean_text(item).lower() for item in parsed if _clean_text(item)}
    return set()


def _dedupe_texts(values: list[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def to_json_safe(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, pd.DataFrame):
        return to_json_safe(obj.to_dict(orient="records"))
    if isinstance(obj, pd.Series):
        return to_json_safe(obj.to_dict())
    if isinstance(obj, Mapping):
        return {str(key): to_json_safe(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_json_safe(item) for item in obj]
    if hasattr(obj, "item"):
        try:
            return to_json_safe(obj.item())
        except Exception:
            pass
    if hasattr(obj, "tolist") and not isinstance(obj, (str, bytes, bytearray)):
        try:
            return to_json_safe(obj.tolist())
        except Exception:
            pass
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    try:
        json.dumps(obj)
        return obj
    except TypeError:
        return str(obj)


def build_default_generation_options(request: Mapping[str, Any]) -> dict[str, Any]:
    days = normalize_days(dict(request))
    options = dict(DEFAULT_GENERATION_OPTIONS)
    options["multi_day_mode"] = MULTI_DAY_MODE_GLOBAL if days > 1 else "off"
    request_options = request.get("generation_options") or {}
    if request_options and not isinstance(request_options, Mapping):
        raise ValueError("generation_options trebuie sa fie obiect JSON.")
    options.update(dict(request_options))
    for key in (
        "selection_mode",
        "portion_policy",
        "meal_realism_mode",
        "quality_gate",
        "profile_guard",
        "feedback_events_path",
    ):
        if request.get(key) is not None:
            options[key] = request[key]
    return options


def normalize_dataset_profile(request: Mapping[str, Any]) -> str:
    value = _clean_text(request.get("dataset_profile"))
    return value or V1_2_DEMO_FINAL_PROFILE


def normalize_days(request: Mapping[str, Any]) -> int:
    try:
        days = int(request.get("days") or 1)
    except (TypeError, ValueError) as exc:
        raise ValueError("days trebuie sa fie intreg intre 1 si 5.") from exc
    if days < 1 or days > 5:
        raise ValueError("days trebuie sa fie intre 1 si 5.")
    return days


def normalize_profile_guard(request: Mapping[str, Any]) -> str:
    options = build_default_generation_options(request)
    value = _clean_text(options.get("profile_guard"))
    return value or "demo"


def normalize_grocery_options(request: Mapping[str, Any]) -> dict[str, Any]:
    options = build_default_generation_options(request)
    include_purchase = _as_bool(options.get("include_purchase_suggestions"), False)
    cooked_to_raw = options.get("include_cooked_to_raw_conversion")
    if cooked_to_raw is None:
        cooked_to_raw = options.get("grocery_cooked_to_raw")
    enable_cooked_to_raw = include_purchase if cooked_to_raw is None else _as_bool(cooked_to_raw)
    return {
        "include_pantry_basics": _as_bool(options.get("include_pantry_basics"), False),
        "include_purchase_suggestions": include_purchase,
        "purchase_rules_path": _path_or_none(options.get("purchase_rules_path")),
        "enable_cooked_to_raw_conversion": enable_cooked_to_raw,
        "cooked_to_raw_rules_path": _path_or_none(options.get("cooked_to_raw_rules_path")),
        "include_price_estimates": _as_bool(options.get("include_price_estimates"), False),
        "product_catalog_path": _path_or_none(options.get("product_catalog_path")),
        "product_aliases_path": _path_or_none(options.get("product_aliases_path")),
        "price_fallbacks_path": _path_or_none(options.get("price_fallbacks_path")),
        "exclude_water": True,
    }


def _args_from_request(
    request: Mapping[str, Any],
    *,
    household: bool = False,
) -> SimpleNamespace:
    options = build_default_generation_options(request)
    args = SimpleNamespace(
        profile=Path(str(request.get("profile_path") or "profiles/member_profile_demo_v1.json")),
        dataset_profile=normalize_dataset_profile(request),
        test_preset="none",
        recipes=_path_or_none(options.get("recipes_path") or request.get("recipes_path")),
        ingredients=_path_or_none(
            options.get("ingredients_path") or request.get("ingredients_path")
        ),
        nutrition=_path_or_none(options.get("nutrition_path") or request.get("nutrition_path")),
        selection_mode=str(options.get("selection_mode") or "balanced_day"),
        alternative_count=int(options.get("alternative_count") or 3),
        diversity_mode=str(options.get("diversity_mode") or "none"),
        recent_recipe_ids=str(options.get("recent_recipe_ids") or ""),
        portion_policy=str(options.get("portion_policy") or "target_aware"),
        meal_realism_mode=str(options.get("meal_realism_mode") or "practical"),
        quality_gate=str(options.get("quality_gate") or "demo_safe"),
        days=normalize_days(request),
        multi_day_mode=str(options.get("multi_day_mode") or "off"),
        multi_day_no_repeat_policy=str(options.get("multi_day_no_repeat_policy") or "hard"),
        day_candidate_pool_size=int(options.get("day_candidate_pool_size") or 75),
        multi_day_speed_mode=str(options.get("multi_day_speed_mode") or "fast"),
        day_candidate_builder=options.get("day_candidate_builder") or None,
        direct_slot_shortlist_size=int(options.get("direct_slot_shortlist_size") or 12),
        profile_guard=str(options.get("profile_guard") or "demo"),
        allow_unsupported_profile=_as_bool(options.get("allow_unsupported_profile"), False),
        feedback_events_path=_path_or_none(
            options.get("feedback_events_path") or request.get("feedback_events_path")
        ),
        feedback_disabled=not _as_bool(options.get("feedback_enabled"), True),
        grocery_purchase_suggestions=_as_bool(
            options.get("include_purchase_suggestions"),
            False,
        ),
        grocery_purchase_rules_path=_path_or_none(options.get("purchase_rules_path")),
        grocery_price_estimates=_as_bool(options.get("include_price_estimates"), False),
        grocery_product_catalog_path=_path_or_none(options.get("product_catalog_path")),
        grocery_product_aliases_path=_path_or_none(options.get("product_aliases_path")),
        grocery_price_fallbacks_path=_path_or_none(options.get("price_fallbacks_path")),
        grocery_cooked_to_raw=options.get("include_cooked_to_raw_conversion"),
        grocery_cooked_to_raw_rules_path=_path_or_none(
            options.get("cooked_to_raw_rules_path")
        ),
        include_pantry_basics=_as_bool(options.get("include_pantry_basics"), False),
        household_profile=_path_or_none(
            request.get("household_profile_path") or options.get("household_profile_path")
        ),
        household_mode=str(
            request.get("household_mode")
            or options.get("household_mode")
            or (
                HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN
                if household
                else HOUSEHOLD_MODE_OFF
            )
        ),
        household_allocation_mode=str(
            request.get("household_allocation_mode")
            or options.get("household_allocation_mode")
            or "macro_aware_simple"
        ),
        no_write_outputs=True,
    )
    args._service_generation_options = options
    _apply_multi_day_defaults(args)
    _resolve_dataset_paths(args)
    return args


def _generate_one_day_plan(
    *,
    args: SimpleNamespace,
    target: Any,
    slot_candidates: pd.DataFrame,
    candidate_diagnostics: dict[str, Any],
    feedback_context: dict[str, Any],
    profile_guard_result: dict[str, Any] | None,
) -> dict[str, Any]:
    selector_config = _balanced_selector_config(args)
    if _should_use_quality_gated_reroll(args):
        ordered_slots = _slot_order(target)
        plan = select_quality_gated_reroll(
            slot_candidates_by_slot=_slot_candidates_by_slot(slot_candidates, ordered_slots),
            target=target,
            slot_order=ordered_slots,
            recent_recipe_ids=_clean_list(args.recent_recipe_ids),
            base_config=selector_config,
        )
    else:
        plan = _select_one_day_plan(
            selection_mode=args.selection_mode,
            slot_candidates=slot_candidates,
            target=target,
            selector_config=selector_config,
        )
    plan["target"] = _target_to_dict(target)
    plan["candidate_diagnostics"] = candidate_diagnostics
    plan["feedback_context"] = feedback_context
    if profile_guard_result is not None:
        plan["profile_guard"] = profile_guard_result
    plan["validation"] = validate_one_day_plan(plan, target)
    if args.quality_gate == "demo_safe" and "quality_gate" not in plan:
        plan["quality_gate"] = evaluate_plan_quality(
            plan,
            target,
            config={"quality_gate": "demo_safe"},
        )
        plan["quality_gate_status"] = plan["quality_gate"]["quality_gate_status"]
        plan["quality_gate_reasons"] = plan["quality_gate"]["quality_gate_reasons"]
        plan["quality_gate_score"] = plan["quality_gate"]["quality_gate_score"]
        plan["quality_gate_fallback_used"] = False
        plan["quality_gate_selected_mode"] = args.diversity_mode
    return plan


def _build_grocery_list_with_loaded_data(
    *,
    plan: dict[str, Any],
    args: SimpleNamespace,
    recipe_ingredients_df: pd.DataFrame,
    fooddb_df: pd.DataFrame,
    generation_type: str,
    plan_id: str,
) -> dict[str, Any]:
    grocery_options = normalize_grocery_options(
        {
            "dataset_profile": args.dataset_profile,
            "days": args.days,
            "generation_options": getattr(args, "_service_generation_options", {}),
        }
    )
    grocery = build_grocery_list(
        plan,
        recipe_ingredients_df,
        fooddb_df=fooddb_df,
        config=grocery_options,
    )
    summary = grocery.get("summary", {}) if isinstance(grocery, dict) else {}
    return to_json_safe(
        {
            "status": "ok",
            "grocery_list_id": _new_id("grocery"),
            "plan_id": plan_id,
            "generation_type": generation_type,
            "currency": summary.get("estimated_total_currency") or "RON",
            "total_estimated_cost": summary.get("estimated_total_cost"),
            "items": grocery.get("display_items", []) or grocery.get("items", []),
            "raw_grocery_list": grocery,
            "summary": summary,
            "warnings": grocery.get("warnings", []),
        }
    )


def _household_profile_from_request(request: Mapping[str, Any]) -> dict[str, Any]:
    profile = request.get("household_profile")
    if isinstance(profile, Mapping):
        return _plain_dict(profile)
    profile_path = request.get("household_profile_path") or DEFAULT_DEMO_HOUSEHOLD_PROFILE_PATH
    return load_household_profile(Path(str(profile_path)))


def _member_profile_from_request(request: Mapping[str, Any]) -> dict[str, Any]:
    profile = request.get("member_profile")
    if isinstance(profile, Mapping):
        return _plain_dict(profile)
    profile_path = request.get("profile_path") or request.get("profile") or "profiles/member_profile_demo_v1.json"
    return load_member_profile(Path(str(profile_path)))


def _feedback_context_for_profile(
    args: SimpleNamespace,
    profile: Mapping[str, Any],
    request: Mapping[str, Any],
) -> dict[str, Any]:
    if args.feedback_disabled:
        events: list[dict[str, Any]] = []
    else:
        injected_context = request.get("feedback_context")
        if isinstance(injected_context, Mapping):
            return _plain_dict(injected_context)
        events = load_feedback_events(args.feedback_events_path)
    return build_feedback_preference_context(
        events=events,
        household_id=str(profile.get("household_id", "")),
        member_profile_id=str(profile.get("member_profile_id", "")),
        dataset_profile=args.dataset_profile,
    )


def _feedback_context_for_household(
    args: SimpleNamespace,
    household_profile: Mapping[str, Any],
    request: Mapping[str, Any],
) -> dict[str, Any]:
    household_id = str(
        request.get("household_id") or household_profile.get("household_id") or ""
    )
    if args.feedback_disabled:
        events: list[dict[str, Any]] = []
    else:
        injected_context = request.get("feedback_context")
        if isinstance(injected_context, Mapping):
            return _plain_dict(injected_context)
        events = load_feedback_events(args.feedback_events_path)
    return build_feedback_preference_context(
        events=events,
        household_id=household_id,
        member_profile_id="",
        dataset_profile=args.dataset_profile,
    )


def _feedback_events_from_request(request: Mapping[str, Any]) -> list[dict[str, Any]]:
    events = request.get("feedback_events")
    if isinstance(events, list):
        return [dict(event) for event in events if isinstance(event, Mapping)]
    path = request.get("feedback_events_path")
    if path is None:
        path = DEFAULT_FEEDBACK_EVENTS_PATH
    return load_feedback_events(path)


def _attach_meal_ingredient_amounts(
    plan: Mapping[str, Any],
    ingredients_df: pd.DataFrame,
) -> None:
    if ingredients_df.empty or "recipe_id" not in ingredients_df.columns:
        return
    ingredients_by_recipe = _ingredient_rows_by_recipe(ingredients_df)
    for meal in _iter_meal_records_for_ingredients(plan):
        recipe_id = _clean_text(meal.get("recipe_id"))
        if not recipe_id:
            continue
        ingredient_rows = ingredients_by_recipe.get(recipe_id, [])
        if not ingredient_rows:
            continue
        multiplier = _ingredient_multiplier_for_meal(meal)
        ingredient_amounts = [
            item
            for item in (
                _ingredient_amount_item(row, multiplier)
                for row in ingredient_rows
            )
            if item
        ]
        if not ingredient_amounts:
            continue
        meal["ingredient_amounts"] = ingredient_amounts
        meal["ingredients"] = [item["text"] for item in ingredient_amounts]


def _iter_meal_records_for_ingredients(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    selected_meals = plan.get("selected_meals")
    if isinstance(selected_meals, list):
        records.extend(item for item in selected_meals if isinstance(item, dict))
    for day in plan.get("days", []) or []:
        if not isinstance(day, dict):
            continue
        day_meals = day.get("selected_meals")
        if isinstance(day_meals, list):
            records.extend(item for item in day_meals if isinstance(item, dict))
    for key in ("allocations", "shared_allocations", "individual_meals"):
        rows = plan.get(key)
        if isinstance(rows, list):
            records.extend(item for item in rows if isinstance(item, dict))
    return records


def _ingredient_rows_by_recipe(ingredients_df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    ingredients = ingredients_df.copy()
    if "ingredient_position" in ingredients.columns:
        ingredients["_ingredient_position_sort"] = pd.to_numeric(
            ingredients["ingredient_position"],
            errors="coerce",
        )
        ingredients = ingredients.sort_values(
            ["recipe_id", "_ingredient_position_sort"],
            kind="mergesort",
            na_position="last",
        )
    rows_by_recipe: dict[str, list[dict[str, Any]]] = {}
    for _, row in ingredients.iterrows():
        recipe_id = _clean_text(row.get("recipe_id"))
        if not recipe_id:
            continue
        rows_by_recipe.setdefault(recipe_id, []).append(row.to_dict())
    return rows_by_recipe


def _ingredient_multiplier_for_meal(meal: Mapping[str, Any]) -> float:
    for key in (
        "portion_multiplier_member",
        "household_portion_sum",
        "portion_multiplier",
    ):
        value = _to_float(meal.get(key))
        if value is not None and value > 0:
            return value
    return 1.0


def _ingredient_amount_item(
    row: Mapping[str, Any],
    multiplier: float,
) -> dict[str, Any] | None:
    name = _ingredient_display_name(row)
    raw_text = _clean_text(row.get("ingredient_raw_text"))
    if not name and not raw_text:
        return None
    scaled_quantity = _scaled_quantity(row, multiplier)
    scaled_grams = _scaled_grams(row, multiplier)
    amount_text = _ingredient_amount_text(
        name=name,
        raw_text=raw_text,
        scaled_quantity=scaled_quantity,
        quantity_unit=_clean_text(row.get("quantity_unit")),
        scaled_grams=scaled_grams,
    )
    text = amount_text or raw_text or name
    return {
        "text": text,
        "name": name or raw_text,
        "raw_text": raw_text,
        "amount_text": amount_text,
        "portion_multiplier": round(multiplier, 4),
        "quantity_value_scaled": _round_optional(scaled_quantity, 3),
        "quantity_unit": _clean_text(row.get("quantity_unit")),
        "quantity_grams_scaled": _round_optional(scaled_grams, 1),
        "is_optional": bool(_to_float(row.get("is_optional")) or 0.0),
    }


def _ingredient_display_name(row: Mapping[str, Any]) -> str:
    raw_name = _ingredient_name_from_raw_text(row)
    parsed = _clean_text(row.get("ingredient_name_parsed")).replace("_", " ")
    if parsed and parsed.lower() not in PREP_ONLY_INGREDIENT_NAMES:
        return parsed
    mapped = _clean_text(row.get("mapped_food_canonical_name")).replace("_", " ")
    if mapped:
        return mapped
    if raw_name:
        return raw_name
    normalized = _clean_text(row.get("ingredient_name_normalized")).replace("_", " ")
    if normalized:
        return normalized
    if parsed:
        return parsed
    return ""


def _ingredient_name_from_raw_text(row: Mapping[str, Any]) -> str:
    text = _clean_text(row.get("ingredient_raw_text"))
    if not text:
        return ""
    quantity_value = _to_float(row.get("quantity_value"))
    prefixes: list[str] = []
    quantity_text = _clean_text(row.get("quantity_text"))
    if quantity_text:
        prefixes.append(quantity_text)
    if quantity_value is not None and quantity_value > 0:
        prefixes.append(_format_quantity(quantity_value))
        if abs(quantity_value - round(quantity_value)) < 0.001:
            prefixes.append(str(int(round(quantity_value))))
    for prefix in sorted(set(prefixes), key=len, reverse=True):
        if text.lower().startswith(prefix.lower()):
            text = text[len(prefix) :].strip()
            break
    text = re.sub(r"^\([^)]*\)\s*", "", text).strip()
    unit = _clean_text(row.get("quantity_unit"))
    if unit:
        unit_pattern = re.escape(unit.strip().lower().replace("_", " "))
        text = re.sub(
            rf"^(?:{unit_pattern}|{unit_pattern}s)\b\s*",
            "",
            text,
            flags=re.IGNORECASE,
        ).strip()
    return text.strip(" ,;-")


def _scaled_quantity(row: Mapping[str, Any], multiplier: float) -> float | None:
    value = _to_float(row.get("quantity_value"))
    if value is None or value <= 0:
        return None
    return value * multiplier


def _scaled_grams(row: Mapping[str, Any], multiplier: float) -> float | None:
    grams = _to_float(row.get("quantity_grams_estimated"))
    if grams is None or grams <= 0:
        return None
    return grams * multiplier


def _ingredient_amount_text(
    *,
    name: str,
    raw_text: str,
    scaled_quantity: float | None,
    quantity_unit: str,
    scaled_grams: float | None,
) -> str:
    display_name = name or raw_text
    if scaled_quantity is not None and quantity_unit:
        amount = _format_quantity(scaled_quantity)
        unit = _display_unit(quantity_unit, scaled_quantity)
        base = " ".join(part for part in (amount, unit, display_name) if part).strip()
        grams = _format_grams_suffix(scaled_grams, quantity_unit)
        return f"{base} {grams}".strip()
    if scaled_grams is not None:
        return f"{_format_grams(scaled_grams)} {display_name}".strip()
    if raw_text:
        return raw_text
    return display_name


def _display_unit(unit: str, quantity: float) -> str:
    normalized = unit.strip().lower().replace("_", " ")
    if normalized == "count":
        return ""
    irregular_units = {
        "pinch": "pinches",
    }
    if abs(quantity - 1.0) >= 0.001 and normalized in irregular_units:
        return irregular_units[normalized]
    unit_map = {
        "gram": "g",
        "grams": "g",
        "g": "g",
        "kilogram": "kg",
        "kilograms": "kg",
        "kg": "kg",
        "milliliter": "ml",
        "milliliters": "ml",
        "ml": "ml",
        "liter": "l",
        "liters": "l",
        "l": "l",
    }
    display = unit_map.get(normalized, normalized)
    if display in {"g", "kg", "ml", "l"}:
        return display
    if abs(quantity - 1.0) < 0.001 or display.endswith("s"):
        return display
    return f"{display}s"


def _format_grams_suffix(grams: float | None, quantity_unit: str) -> str:
    if grams is None:
        return ""
    if _display_unit(quantity_unit, 2.0) in {"g", "kg"}:
        return ""
    return f"({_format_grams(grams)})"


def _format_grams(grams: float) -> str:
    if grams >= 1000:
        return f"{_format_quantity(grams / 1000.0)} kg"
    return f"{_format_quantity(grams)} g"


def _format_quantity(value: float) -> str:
    if value >= 10:
        return f"{value:.0f}"
    elif value >= 1:
        text = f"{value:.1f}"
    else:
        text = f"{value:.2f}"
    return text.rstrip("0").rstrip(".")


def _daily_plan_view(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    days = plan.get("days")
    if isinstance(days, list) and days:
        rows = []
        for fallback_day, day in enumerate(days, start=1):
            if not isinstance(day, Mapping):
                continue
            rows.append(
                {
                    "day_index": int(day.get("day_index") or fallback_day),
                    "validation_status": day.get("validation_status")
                    or _nested_get(day, ("validation", "validation_status")),
                    "quality_status": day.get("quality_gate_status")
                    or day.get("household_quality_status"),
                    "totals": _totals_view(day.get("day_totals", {})),
                    "selected_meals": _meal_rows_view(day.get("selected_meals", [])),
                }
            )
        return rows
    return [
        {
            "day_index": 1,
            "validation_status": _nested_get(plan, ("validation", "validation_status")),
            "quality_status": plan.get("quality_gate_status")
            or _nested_get(plan, ("quality_gate", "quality_gate_status")),
            "totals": _totals_view(plan.get("day_totals", {})),
            "selected_meals": _meal_rows_view(plan.get("selected_meals", [])),
        }
    ]


def _meal_rows_view(meals: Any) -> list[dict[str, Any]]:
    if not isinstance(meals, list):
        return []
    rows = []
    for meal in meals:
        if not isinstance(meal, Mapping):
            continue
        rows.append(
            {
                "slot": meal.get("slot"),
                "recipe_id": meal.get("recipe_id"),
                "display_name": meal.get("display_name"),
                "directions_step_count": meal.get("directions_step_count"),
                "cooking_steps": meal.get("cooking_steps", []),
                "ingredients": meal.get("ingredients", []),
                "ingredient_amounts": meal.get("ingredient_amounts", []),
                "portion_multiplier": meal.get("portion_multiplier"),
                "meal_scope": "shared"
                if bool(meal.get("household_generation_shared_slot", False))
                else meal.get("allocation_scope", "individual"),
                "kcal": meal.get("kcal"),
                "protein_g": meal.get("protein_g"),
                "carbs_g": meal.get("carbs_g"),
                "fat_g": meal.get("fat_g"),
                **_meal_time_fields(meal),
                "feedback_fit": meal.get("feedback_fit"),
                "warnings": meal.get("warnings", []),
                "health_and_diet_fit": meal.get("health_and_diet_fit"),
                "health_and_diet_reasons": meal.get("health_and_diet_reasons", []),
                "active_dietary_patterns": meal.get("active_dietary_patterns", []),
                "active_health_modes": meal.get("active_health_modes", []),
            }
        )
    return rows


def _totals_view(totals: Any) -> dict[str, Any]:
    if not isinstance(totals, Mapping):
        return {}
    return {
        "kcal": totals.get("total_kcal", totals.get("kcal")),
        "protein_g": totals.get("total_protein_g", totals.get("protein_g")),
        "carbs_g": totals.get("total_carbs_g", totals.get("carbs_g")),
        "fat_g": totals.get("total_fat_g", totals.get("fat_g")),
    }


def _individual_diagnostics_summary(
    *,
    args: SimpleNamespace,
    plan: Mapping[str, Any],
    profile_guard_result: Mapping[str, Any] | None,
    grocery_list: Mapping[str, Any] | None,
) -> dict[str, Any]:
    summary = {
        "dataset_profile": args.dataset_profile,
        "days": int(args.days or 1),
        "selection_mode": args.selection_mode,
        "portion_policy": args.portion_policy,
        "quality_gate": args.quality_gate,
        "profile_guard_status": _clean_text(
            (profile_guard_result or {}).get("profile_guard_status")
        ),
    }
    multi_day_summary = plan.get("multi_day_summary")
    if isinstance(multi_day_summary, Mapping):
        summary.update(
            {
                "actual_days_generated": multi_day_summary.get("actual_days_generated"),
                "valid_days": multi_day_summary.get("valid_day_count"),
                "accept_days": multi_day_summary.get("accept_day_count"),
                "repeated_recipes": multi_day_summary.get("repeated_recipe_count"),
            }
        )
    else:
        summary.update(
            {
                "validation_status": _nested_get(plan, ("validation", "validation_status")),
                "quality_status": plan.get("quality_gate_status")
                or _nested_get(plan, ("quality_gate", "quality_gate_status")),
            }
        )
    if grocery_list:
        summary["grocery_shopping_item_count"] = _nested_get(
            grocery_list,
            ("summary", "shopping_item_count"),
        )
    return summary


def _household_diagnostics_summary(
    *,
    args: SimpleNamespace,
    plan: Mapping[str, Any],
    grocery_list: Mapping[str, Any] | None,
) -> dict[str, Any]:
    household_summary = plan.get("household_summary", {})
    summary = {
        "dataset_profile": args.dataset_profile,
        "household_mode": args.household_mode,
        "household_allocation_mode": args.household_allocation_mode,
        "household_quality_status": household_summary.get("household_quality_status"),
        "accept_day_count": household_summary.get("accept_day_count"),
        "max_grocery_scaling_factor": household_summary.get("max_grocery_scaling_factor"),
    }
    if grocery_list:
        summary["grocery_shopping_item_count"] = _nested_get(
            grocery_list,
            ("summary", "shopping_item_count"),
        )
        summary["estimated_total_cost"] = _nested_get(
            grocery_list,
            ("summary", "estimated_total_cost"),
        )
    return summary


def _feedback_context_summary(context: Mapping[str, Any]) -> dict[str, Any]:
    score_preferences = context.get("score_preferences", {})
    time_preferences = context.get("time_preferences", {})
    hard_filters = context.get("hard_filters", {})
    liked = score_preferences.get("liked_recipe_ids", {}) if isinstance(score_preferences, Mapping) else {}
    disliked = (
        score_preferences.get("disliked_recipe_ids", {})
        if isinstance(score_preferences, Mapping)
        else {}
    )
    too_long = (
        time_preferences.get("too_long_recipe_ids", {})
        if isinstance(time_preferences, Mapping)
        else {}
    )
    banned = (
        hard_filters.get("banned_recipe_ids", [])
        if isinstance(hard_filters, Mapping)
        else []
    )
    meta = context.get("meta", {}) if isinstance(context.get("meta"), Mapping) else {}
    return {
        "event_count": meta.get("event_count", 0),
        "liked_count": _sum_mapping_values(liked),
        "disliked_count": _sum_mapping_values(disliked),
        "too_long_count": _sum_mapping_values(too_long),
        "explicit_avoid_count": len(banned) if isinstance(banned, list) else 0,
    }


def _selected_members(household_profile: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "member_id": member.get("member_id"),
            "display_name": member.get("display_name") or member.get("profile_name"),
        }
        for member in household_profile.get("members", [])
        if isinstance(member, Mapping)
    ]


def _member_target_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    member_targets = plan.get("member_targets", {})
    target_rows = member_targets.get("target_rows", []) if isinstance(member_targets, Mapping) else []
    return list(target_rows) if isinstance(target_rows, list) else []


def _per_member_menus(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, int], dict[str, Any]] = {}
    for row in plan.get("allocations", []):
        if not isinstance(row, Mapping):
            continue
        member_id = _clean_text(row.get("member_id"))
        day_index = int(_to_float(row.get("day_index")) or 1)
        key = (member_id, day_index)
        bucket = buckets.setdefault(
            key,
            {
                "member_id": member_id,
                "day_index": day_index,
                "meals": [],
            },
        )
        bucket["meals"].append(
            {
                "slot": row.get("slot"),
                "recipe_id": row.get("recipe_id"),
                "display_name": row.get("display_name") or row.get("recipe"),
                "directions_step_count": row.get("directions_step_count"),
                "cooking_steps": row.get("cooking_steps", []),
                "ingredients": row.get("ingredients", []),
                "ingredient_amounts": row.get("ingredient_amounts", []),
                "portion_multiplier": row.get("portion_multiplier_member")
                or row.get("portion_multiplier"),
                "meal_scope": row.get("allocation_scope"),
                "kcal": row.get("kcal"),
                "protein_g": row.get("protein_g"),
                "carbs_g": row.get("carbs_g"),
                "fat_g": row.get("fat_g"),
                **_meal_time_fields(row),
            }
        )
    return [
        buckets[key]
        for key in sorted(buckets, key=lambda item: (item[1], item[0]))
    ]


def _shared_meals(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for day in plan.get("days", []):
        if not isinstance(day, Mapping):
            continue
        day_index = int(day.get("day_index") or 1)
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            if not bool(meal.get("household_generation_shared_slot", True)):
                continue
            rows.append(
                {
                    "day_index": day_index,
                    "slot": meal.get("slot"),
                    "recipe_id": meal.get("recipe_id"),
                    "display_name": meal.get("display_name"),
                    "directions_step_count": meal.get("directions_step_count"),
                    "cooking_steps": meal.get("cooking_steps", []),
                    "ingredients": meal.get("ingredients", []),
                    "ingredient_amounts": meal.get("ingredient_amounts", []),
                    "household_portion_sum": meal.get("household_portion_sum"),
                    "household_grocery_scaling_factor": meal.get(
                        "household_grocery_scaling_factor"
                    ),
                    **_meal_time_fields(meal),
                }
            )
    return rows


def _meal_time_fields(meal: Mapping[str, Any]) -> dict[str, Any]:
    return {field: meal.get(field) for field in TIME_OUTPUT_FIELDS if field in meal}


def _household_plan_for_grocery(plan: Mapping[str, Any]) -> dict[str, Any]:
    if plan.get("household_generation_version") != "v1_lite":
        return dict(plan)
    individual_by_day: dict[int, list[dict[str, Any]]] = {}
    for row in plan.get("individual_meals", []):
        if isinstance(row, Mapping):
            day_index = int(_to_float(row.get("day_index")) or 1)
            individual_by_day.setdefault(day_index, []).append(dict(row))

    grocery_days: list[dict[str, Any]] = []
    for fallback_index, day in enumerate(plan.get("days", []), start=1):
        if not isinstance(day, Mapping):
            continue
        day_index = int(_to_float(day.get("day_index")) or fallback_index)
        selected_meals: list[dict[str, Any]] = []
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            if not bool(meal.get("household_generation_shared_slot", True)):
                continue
            shared_meal = dict(meal)
            shared_meal["portion_multiplier"] = (
                _to_float(meal.get("household_portion_sum"))
                or _to_float(meal.get("portion_multiplier"))
                or 1.0
            )
            selected_meals.append(shared_meal)
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
                    "allocation_scope": "individual",
                    "member_id": row.get("member_id"),
                    "member": row.get("member"),
                }
            )
        grocery_day = dict(day)
        grocery_day["selected_meals"] = selected_meals
        grocery_days.append(grocery_day)

    result = dict(plan)
    result["days"] = grocery_days
    result["generation_trigger"] = "aggregate_household_grocery"
    return result


def _response_warnings(plan: Mapping[str, Any]) -> list[Any]:
    warnings = []
    raw = plan.get("warnings", [])
    if isinstance(raw, list):
        warnings.extend(raw)
    loader_warnings = _nested_get(plan, ("pool_summary", "loader_warnings"))
    if isinstance(loader_warnings, list):
        warnings.extend(loader_warnings)
    return warnings


def _strip_internal_paths(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {
            str(key): _strip_internal_paths(value)
            for key, value in obj.items()
            if str(key) not in INTERNAL_PATH_KEYS
        }
    if isinstance(obj, list):
        return [_strip_internal_paths(item) for item in obj]
    if isinstance(obj, tuple):
        return [_strip_internal_paths(item) for item in obj]
    return obj


def _with_plan_id(grocery_list: Mapping[str, Any], plan_id: str) -> dict[str, Any]:
    result = dict(grocery_list)
    result["plan_id"] = plan_id
    return result


def _is_household_plan(plan: Mapping[str, Any]) -> bool:
    return bool(plan.get("household_generation_version")) or bool(plan.get("allocations"))


def _plan_day_count(plan: Mapping[str, Any]) -> int:
    days = plan.get("days")
    if isinstance(days, list) and days:
        return len(days)
    return 1


def _plain_dict(data: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(to_json_safe(dict(data)), ensure_ascii=True))


def _nested_get(data: Any, keys: tuple[str, ...]) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _generation_option(args: SimpleNamespace, key: str, default: Any = None) -> Any:
    options = getattr(args, "_service_generation_options", {})
    if isinstance(options, Mapping):
        return options.get(key, default)
    return default


def _path_or_none(value: Any) -> Path | None:
    if value in (None, ""):
        return None
    return Path(str(value))


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "nat"}:
        return ""
    return text


def _clean_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _to_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _round_optional(value: Any, digits: int) -> float | None:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return round(numeric, digits)


def _sum_mapping_values(value: Any) -> int:
    if not isinstance(value, Mapping):
        return 0
    total = 0
    for item in value.values():
        try:
            total += int(item)
        except (TypeError, ValueError):
            continue
    return total


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"
