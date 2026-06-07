from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from typing import Any, Mapping

import pandas as pd

from src.generator_v1.candidate_filter import (
    build_household_preference_context as build_profile_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (
    V1_2_DEMO_FINAL_PROFILE,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.day_selector import SELECTED_MEAL_FIELDS
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import build_nutrition_target


MACRO_FIELDS = ("kcal", "protein_g", "carbs_g", "fat_g")
TIME_FIELDS = (
    "total_time_min",
    "total_elapsed_time_min",
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "effective_time_min_for_scoring",
    "time_confidence",
    "time_estimation_method",
    "time_warnings",
)
SUPPORTED_SLOTS = {"breakfast", "lunch", "dinner", "snack"}


def preview_meal_replacement(request: dict[str, Any]) -> dict[str, Any]:
    return _replace_meal(request, dry_run=True)


def apply_meal_replacement(request: dict[str, Any]) -> dict[str, Any]:
    return _replace_meal(request, dry_run=False)


def recompute_day_totals(plan: dict[str, Any], day_index: int) -> dict[str, Any]:
    target = _find_plan_day(plan, day_index)
    if target is None:
        return {}
    meals = target.get("selected_meals", [])
    if not isinstance(meals, list):
        meals = []
    totals = _meal_totals(meals)
    target["day_totals"] = totals
    return totals


def recompute_plan_summary(plan: dict[str, Any]) -> dict[str, Any]:
    for day in _plan_days(plan):
        recompute_day_totals(plan, int(_to_float(day.get("day_index")) or 1))
    if _is_household_generator_plan(plan):
        _recompute_household_member_daily_rows(plan)
    return plan


def rebuild_grocery_for_updated_plan(
    plan: dict[str, Any],
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from src.generator_v1.service import build_grocery_list_for_plan

    return build_grocery_list_for_plan(plan, options or {})


def _replace_meal(request: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    source_plan = _source_plan_from_request(request)
    if not source_plan:
        return _error_response("plan_required", "source_plan este obligatoriu.")

    slot = _normalize_slot(request.get("slot"))
    current_recipe_id = _clean_text(request.get("current_recipe_id") or request.get("recipe_id"))
    alternative_recipe_id = _clean_text(request.get("alternative_recipe_id"))
    day_index = max(1, int(_to_float(request.get("day_index")) or 1))
    if not slot:
        return _error_response("slot_required", "slot este obligatoriu pentru replacement.")
    if not current_recipe_id:
        return _error_response("current_recipe_required", "current_recipe_id este obligatoriu.")
    if not alternative_recipe_id:
        return _error_response(
            "alternative_recipe_required",
            "alternative_recipe_id este obligatoriu.",
        )
    if current_recipe_id == alternative_recipe_id:
        return _error_response(
            "same_recipe_replacement",
            "Reteta alternativa trebuie sa fie diferita de reteta curenta.",
        )

    gate = _approved_alternative_candidate(
        request=request,
        current_recipe_id=current_recipe_id,
        alternative_recipe_id=alternative_recipe_id,
        slot=slot,
    )
    if gate.get("status") != "ok":
        return gate

    alternative_item = gate["alternative_item"]
    approval_status = _clean_text(alternative_item.get("approval_status"))
    can_apply = approval_status == "approved"
    if not dry_run and not can_apply:
        return _error_response(
            "alternative_not_approved",
            "Doar alternativele approved pot fi aplicate.",
            approval_status=approval_status,
            replacement_allowed=False,
        )

    updated_response = copy.deepcopy(source_plan)
    generator_plan = _generator_plan_from_response(updated_response)
    generation_type = _generation_type(request, updated_response, generator_plan)
    replacement_meal = _selected_meal_from_candidate(
        gate["candidate_row"],
        slot=slot,
        alternative_item=alternative_item,
    )

    if generation_type == "household" or _is_household_generator_plan(generator_plan):
        result = _replace_household_meal(
            plan=generator_plan,
            request=request,
            day_index=day_index,
            slot=slot,
            current_recipe_id=current_recipe_id,
            replacement_meal=replacement_meal,
        )
    else:
        result = _replace_individual_meal(
            plan=generator_plan,
            day_index=day_index,
            slot=slot,
            current_recipe_id=current_recipe_id,
            replacement_meal=replacement_meal,
        )
    if result.get("status") != "ok":
        return result

    recompute_plan_summary(generator_plan)
    _refresh_response_views(
        updated_response,
        generator_plan,
        generation_type=generation_type,
    )
    grocery = rebuild_grocery_for_updated_plan(
        generator_plan,
        {
            "dataset_profile": _dataset_profile(request, updated_response, generator_plan),
            "days": updated_response.get("days") or request.get("days") or day_index,
            "generation_type": generation_type,
            "plan_id": _clean_text(updated_response.get("plan_id") or updated_response.get("household_plan_id")),
            **dict(request.get("generation_options") or {}),
        },
    )
    _attach_grocery(updated_response, grocery, generation_type)
    _append_replacement_metadata(
        updated_response,
        request=request,
        dry_run=dry_run,
        replacement=result,
        alternative_item=alternative_item,
    )

    impact = _replacement_impact(
        replacement=result,
        current_meal=result["current_meal"],
        replacement_meal=result["replacement_meal"],
        grocery=grocery,
        warnings=gate.get("warnings", []),
    )
    response = {
        "status": "ok",
        "dry_run": dry_run,
        "replacement_allowed": can_apply,
        "approval_status": approval_status,
        "plan_id": _clean_text(updated_response.get("plan_id") or updated_response.get("household_plan_id")),
        "source_plan_id": _clean_text(request.get("source_plan_id") or request.get("plan_id")),
        "generation_type": generation_type,
        "replacement": {
            "day_index": day_index,
            "slot": slot,
            "member_id": _clean_text(request.get("member_id") or request.get("member_profile_id")),
            "replace_scope": result.get("replace_scope") or "individual_meal",
            "current_meal": _meal_view(result["current_meal"]),
            "alternative_meal": _meal_view(result["replacement_meal"]),
            "alternative": alternative_item,
        },
        "impact": impact,
        "updated_plan": updated_response,
        "grocery_list": grocery,
        "warnings": gate.get("warnings", []),
    }
    return _to_json_safe(response)


def _approved_alternative_candidate(
    *,
    request: Mapping[str, Any],
    current_recipe_id: str,
    alternative_recipe_id: str,
    slot: str,
) -> dict[str, Any]:
    from src.generator_v1.service import get_recipe_alternatives_from_request

    alternatives_request = dict(request)
    alternatives_request.update(
        {
            "recipe_id": current_recipe_id,
            "slot": slot,
            "top_k": 25,
            "candidate_pool_k": 75,
            "approval_mode": "include_rejected_debug",
        }
    )
    alternatives = get_recipe_alternatives_from_request(alternatives_request)
    if alternatives.get("status") != "ok":
        return _error_response(
            _clean_text(alternatives.get("error_code")) or "alternatives_failed",
            _clean_text(alternatives.get("message")) or "Alternativele nu au putut fi validate.",
        )

    item = _find_alternative_item(alternatives, alternative_recipe_id)
    if item is None:
        return _error_response(
            "alternative_not_returned_by_gate",
            "Reteta alternativa nu a fost returnata de KNN + approval gate.",
            replacement_allowed=False,
        )

    candidate_row = _slot_candidate_row_for_request(
        alternatives_request,
        alternative_recipe_id=alternative_recipe_id,
        slot=slot,
        alternative_item=item,
    )
    if candidate_row is None:
        return _error_response(
            "alternative_candidate_row_missing",
            "Reteta alternativa nu exista ca rand generator pentru slotul cerut.",
            approval_status=item.get("approval_status"),
            replacement_allowed=False,
        )
    return {
        "status": "ok",
        "alternative_item": item,
        "candidate_row": candidate_row,
        "warnings": list(alternatives.get("warnings", [])),
    }


def _slot_candidate_row_for_request(
    request: Mapping[str, Any],
    *,
    alternative_recipe_id: str,
    slot: str,
    alternative_item: Mapping[str, Any],
) -> dict[str, Any] | None:
    from src.generator_v1.service import (
        _alternatives_service_request,
        _args_from_request,
        _feedback_context_for_profile,
        _member_profile_from_request,
    )

    service_request = _alternatives_service_request(request)
    args = _args_from_request(service_request)
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
    )
    if slot_candidates.empty:
        return None
    mask = slot_candidates["recipe_id"].astype(str).eq(alternative_recipe_id)
    if slot:
        mask &= slot_candidates["slot"].astype(str).str.lower().eq(slot)
    rows = slot_candidates.loc[mask].copy()
    if rows.empty:
        return None

    diagnostics = alternative_item.get("diagnostics", {})
    preferred_portion = (
        _to_float(diagnostics.get("portion_multiplier"))
        if isinstance(diagnostics, Mapping)
        else None
    )
    if preferred_portion is not None and "portion_multiplier" in rows.columns:
        rows["_portion_diff"] = (
            pd.to_numeric(rows["portion_multiplier"], errors="coerce") - preferred_portion
        ).abs()
    else:
        rows["_portion_diff"] = 0.0
    sort_columns = [
        column
        for column in (
            "_portion_diff",
            "score_preview",
            "macro_fit",
            "time_fit",
            "slot_fit",
            "portion_multiplier",
        )
        if column in rows.columns
    ]
    ascending = [True] + [False] * max(0, len(sort_columns) - 2) + [True]
    if len(ascending) != len(sort_columns):
        ascending = [True if column in {"_portion_diff", "portion_multiplier"} else False for column in sort_columns]
    row = rows.sort_values(
        sort_columns,
        ascending=ascending,
        kind="mergesort",
        na_position="last",
    ).iloc[0]
    return row.to_dict()


def _replace_individual_meal(
    *,
    plan: dict[str, Any],
    day_index: int,
    slot: str,
    current_recipe_id: str,
    replacement_meal: dict[str, Any],
) -> dict[str, Any]:
    day = _find_plan_day(plan, day_index)
    if day is None:
        return _error_response("day_not_found", "Ziua ceruta nu exista in plan.")
    meals = day.get("selected_meals", [])
    if not isinstance(meals, list):
        return _error_response("selected_meals_missing", "Planul nu contine selected_meals.")
    meal_index = _find_meal_index(meals, slot=slot, recipe_id=current_recipe_id)
    if meal_index is None:
        return _error_response(
            "meal_not_found",
            "Masa curenta nu exista in ziua si slotul cerut.",
        )
    current_meal = copy.deepcopy(meals[meal_index])
    replacement = _replacement_meal(
        replacement_meal,
        current_meal=current_meal,
        replace_scope="individual_meal",
    )
    before_totals = _meal_totals(meals)
    meals[meal_index] = replacement
    after_totals = _meal_totals(meals)
    day["day_totals"] = after_totals
    return {
        "status": "ok",
        "replace_scope": "individual_meal",
        "current_meal": current_meal,
        "replacement_meal": replacement,
        "day_totals_before": before_totals,
        "day_totals_after": after_totals,
        "affected_members": [],
    }


def _replace_household_meal(
    *,
    plan: dict[str, Any],
    request: Mapping[str, Any],
    day_index: int,
    slot: str,
    current_recipe_id: str,
    replacement_meal: dict[str, Any],
) -> dict[str, Any]:
    member_id = _clean_text(request.get("member_id") or request.get("member_profile_id"))
    allocation = _find_household_allocation(
        plan,
        day_index=day_index,
        slot=slot,
        recipe_id=current_recipe_id,
        member_id=member_id,
    )
    current_shared_meal = _find_household_selected_meal(
        plan,
        day_index=day_index,
        slot=slot,
        recipe_id=current_recipe_id,
    )
    if allocation is None and current_shared_meal is None:
        return _error_response(
            "meal_not_found",
            "Masa household curenta nu exista in ziua si slotul cerut.",
        )

    requested_scope = _clean_text(request.get("replace_scope"))
    allocation_scope = _clean_text((allocation or {}).get("allocation_scope"))
    shared_flag = bool((allocation or {}).get("household_generation_shared_slot")) or bool(
        (current_shared_meal or {}).get("household_generation_shared_slot")
    )
    is_shared = requested_scope == "household_shared_meal" or allocation_scope == "shared" or shared_flag
    replace_scope = "household_shared_meal" if is_shared else "household_member_meal"
    if not is_shared and not member_id:
        return _error_response(
            "member_id_required",
            "member_id este obligatoriu pentru inlocuirea unei mese individuale household.",
        )

    current_meal = copy.deepcopy(allocation or current_shared_meal or {})
    before_day = _meal_totals((_find_plan_day(plan, day_index) or {}).get("selected_meals", []))
    if is_shared:
        affected_rows = _replace_household_shared_rows(
            plan,
            day_index=day_index,
            slot=slot,
            current_recipe_id=current_recipe_id,
            replacement_meal=replacement_meal,
        )
    else:
        affected_rows = _replace_household_member_rows(
            plan,
            day_index=day_index,
            slot=slot,
            current_recipe_id=current_recipe_id,
            member_id=member_id,
            replacement_meal=replacement_meal,
        )
    if not affected_rows:
        return _error_response(
            "affected_household_rows_missing",
            "Nu a fost gasit niciun rand household de actualizat.",
        )

    after_day = recompute_day_totals(plan, day_index)
    _recompute_household_member_daily_rows(plan)
    affected_members = sorted(
        {
            _clean_text(row.get("member_id"))
            for row in affected_rows
            if _clean_text(row.get("member_id"))
        }
    )
    return {
        "status": "ok",
        "replace_scope": replace_scope,
        "current_meal": current_meal,
        "replacement_meal": _replacement_meal(
            replacement_meal,
            current_meal=current_meal,
            replace_scope=replace_scope,
        ),
        "day_totals_before": before_day,
        "day_totals_after": after_day,
        "affected_members": affected_members,
    }


def _replace_household_shared_rows(
    plan: dict[str, Any],
    *,
    day_index: int,
    slot: str,
    current_recipe_id: str,
    replacement_meal: dict[str, Any],
) -> list[dict[str, Any]]:
    affected = _update_rows_in_lists(
        [plan.get("allocations", []), plan.get("shared_allocations", [])],
        matcher=lambda row: _row_matches(
            row,
            day_index=day_index,
            slot=slot,
            recipe_id=current_recipe_id,
            allocation_scope="shared",
        ),
        updater=lambda row: _apply_replacement_to_allocation_row(
            row,
            replacement_meal,
            current_recipe_id=current_recipe_id,
        ),
    )
    selected_meal = _find_household_selected_meal(
        plan,
        day_index=day_index,
        slot=slot,
        recipe_id=current_recipe_id,
    )
    if selected_meal is not None:
        summary_rows = [
            row
            for row in plan.get("allocations", [])
            if _row_matches(
                row,
                day_index=day_index,
                slot=slot,
                recipe_id=_clean_text(replacement_meal.get("recipe_id")),
                allocation_scope="shared",
            )
        ]
        _apply_replacement_to_shared_meal(
            selected_meal,
            replacement_meal,
            current_recipe_id=current_recipe_id,
            member_rows=summary_rows,
        )
    return affected


def _replace_household_member_rows(
    plan: dict[str, Any],
    *,
    day_index: int,
    slot: str,
    current_recipe_id: str,
    member_id: str,
    replacement_meal: dict[str, Any],
) -> list[dict[str, Any]]:
    return _update_rows_in_lists(
        [plan.get("allocations", []), plan.get("individual_meals", [])],
        matcher=lambda row: _row_matches(
            row,
            day_index=day_index,
            slot=slot,
            recipe_id=current_recipe_id,
            member_id=member_id,
            allocation_scope="individual",
        ),
        updater=lambda row: _apply_replacement_to_allocation_row(
            row,
            replacement_meal,
            current_recipe_id=current_recipe_id,
        ),
    )


def _apply_replacement_to_allocation_row(
    row: dict[str, Any],
    replacement_meal: Mapping[str, Any],
    *,
    current_recipe_id: str,
) -> dict[str, Any]:
    multiplier = (
        _to_float(row.get("portion_multiplier_member"))
        or _to_float(row.get("portion_multiplier"))
        or _to_float(replacement_meal.get("portion_multiplier"))
        or 1.0
    )
    per_portion = _per_portion_values(replacement_meal)
    row["replacement_source_recipe_id"] = current_recipe_id
    row["replacement_applied_from_knn"] = True
    row["recipe_id"] = replacement_meal.get("recipe_id")
    row["display_name"] = replacement_meal.get("display_name")
    row["recipe"] = replacement_meal.get("display_name")
    row["portion_multiplier"] = round(multiplier, 3)
    row["portion_multiplier_member"] = round(multiplier, 3)
    row["portion_multiplier_raw"] = round(multiplier, 3)
    for field in MACRO_FIELDS:
        row[field] = _round(per_portion.get(field, 0.0) * multiplier, 1)
    grams = per_portion.get("portion_grams_estimated")
    if grams is not None:
        row["grams_estimated"] = _round(grams * multiplier, 1)
    row.update(_time_fields_from_row(replacement_meal))
    return row


def _apply_replacement_to_shared_meal(
    meal: dict[str, Any],
    replacement_meal: Mapping[str, Any],
    *,
    current_recipe_id: str,
    member_rows: list[Mapping[str, Any]],
) -> None:
    per_portion = _per_portion_values(replacement_meal)
    portion_sum = sum(
        _to_float(row.get("portion_multiplier_member")) or _to_float(row.get("portion_multiplier")) or 0.0
        for row in member_rows
    )
    if portion_sum <= 0:
        portion_sum = _to_float(meal.get("household_portion_sum")) or _to_float(
            replacement_meal.get("portion_multiplier")
        ) or 1.0
    meal["replacement_source_recipe_id"] = current_recipe_id
    meal["replacement_applied_from_knn"] = True
    meal["recipe_id"] = replacement_meal.get("recipe_id")
    meal["display_name"] = replacement_meal.get("display_name")
    meal["recipe_family_name"] = replacement_meal.get("recipe_family_name")
    meal["portion_multiplier"] = round(portion_sum, 3)
    meal["household_portion_sum"] = round(portion_sum, 3)
    meal["household_grocery_scaling_factor"] = round(portion_sum, 3)
    meal["household_generation_shared_slot"] = True
    meal["member_allocations"] = [dict(row) for row in member_rows]
    meal["member_portion_summary_json"] = json.dumps(
        [dict(row) for row in member_rows],
        ensure_ascii=True,
    )
    for field in MACRO_FIELDS:
        meal[field] = _round(per_portion.get(field, 0.0) * portion_sum, 1)
    grams = per_portion.get("portion_grams_estimated")
    if grams is not None:
        meal["portion_grams_estimated"] = _round(grams * portion_sum, 1)
    meal.update(_time_fields_from_row(replacement_meal))


def _refresh_response_views(
    response: dict[str, Any],
    generator_plan: dict[str, Any],
    *,
    generation_type: str,
) -> None:
    from src.generator_v1.service import _daily_plan_view, _per_member_menus, _shared_meals

    response["generator_plan"] = generator_plan
    response["daily_plan"] = _daily_plan_view(generator_plan)
    if generation_type == "household" or _is_household_generator_plan(generator_plan):
        response["generation_type"] = "household"
        response["per_member_menus"] = _per_member_menus(generator_plan)
        response["shared_meals"] = _shared_meals(generator_plan)
        response["household_grocery_scaling"] = generator_plan.get("grocery_scaling", [])
        response["member_macro_summaries"] = generator_plan.get("member_daily_rows", [])
    else:
        response["generation_type"] = "individual"


def _append_replacement_metadata(
    response: dict[str, Any],
    *,
    request: Mapping[str, Any],
    dry_run: bool,
    replacement: Mapping[str, Any],
    alternative_item: Mapping[str, Any],
) -> None:
    event = {
        "source_plan_id": _clean_text(request.get("source_plan_id") or request.get("plan_id")),
        "dry_run": dry_run,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "day_index": request.get("day_index"),
        "slot": request.get("slot"),
        "current_recipe_id": request.get("current_recipe_id"),
        "alternative_recipe_id": request.get("alternative_recipe_id"),
        "approval_status": alternative_item.get("approval_status"),
        "replace_scope": replacement.get("replace_scope"),
    }
    history = response.get("replacement_history")
    if not isinstance(history, list):
        history = []
    history.append(event)
    response["replacement_history"] = history
    response["replacement_status"] = "preview" if dry_run else "applied"


def _replacement_impact(
    *,
    replacement: Mapping[str, Any],
    current_meal: Mapping[str, Any],
    replacement_meal: Mapping[str, Any],
    grocery: Mapping[str, Any],
    warnings: list[Any],
) -> dict[str, Any]:
    return {
        "meal_macro_delta": _macro_delta(current_meal, replacement_meal),
        "day_totals_before": replacement.get("day_totals_before", {}),
        "day_totals_after": replacement.get("day_totals_after", {}),
        "day_totals_delta": _macro_delta(
            replacement.get("day_totals_before", {}),
            replacement.get("day_totals_after", {}),
        ),
        "affected_members": replacement.get("affected_members", []),
        "grocery_rebuilt": bool(grocery),
        "grocery_item_count": len(grocery.get("items", [])) if isinstance(grocery.get("items"), list) else None,
        "warnings": warnings,
    }


def _attach_grocery(
    response: dict[str, Any],
    grocery: dict[str, Any],
    generation_type: str,
) -> None:
    if generation_type == "household":
        response["household_grocery_list"] = grocery
    else:
        response["grocery_list"] = grocery


def _selected_meal_from_candidate(
    row: Mapping[str, Any],
    *,
    slot: str,
    alternative_item: Mapping[str, Any],
) -> dict[str, Any]:
    meal = {field: _clean_json_value(row.get(field)) for field in SELECTED_MEAL_FIELDS}
    meal["slot"] = slot
    meal["recipe_id"] = _clean_text(row.get("recipe_id"))
    meal["display_name"] = _clean_text(row.get("display_name")) or _clean_text(
        alternative_item.get("display_name")
    )
    meal["replacement_approval_status"] = alternative_item.get("approval_status")
    meal["replacement_similarity_score"] = alternative_item.get("similarity_score")
    return meal


def _replacement_meal(
    meal: Mapping[str, Any],
    *,
    current_meal: Mapping[str, Any],
    replace_scope: str,
) -> dict[str, Any]:
    result = copy.deepcopy(dict(meal))
    result["slot"] = current_meal.get("slot") or result.get("slot")
    result["replacement_source_recipe_id"] = current_meal.get("recipe_id")
    result["replacement_source_display_name"] = current_meal.get("display_name")
    result["replacement_applied_from_knn"] = True
    result["replacement_scope"] = replace_scope
    return result


def _source_plan_from_request(request: Mapping[str, Any]) -> dict[str, Any]:
    plan = request.get("source_plan") or request.get("plan") or request.get("updated_plan")
    return copy.deepcopy(plan) if isinstance(plan, dict) else {}


def _generator_plan_from_response(response: dict[str, Any]) -> dict[str, Any]:
    plan = response.get("generator_plan")
    if isinstance(plan, dict):
        return copy.deepcopy(plan)
    return copy.deepcopy(response)


def _generation_type(
    request: Mapping[str, Any],
    response: Mapping[str, Any],
    generator_plan: Mapping[str, Any],
) -> str:
    text = _clean_text(request.get("generation_type") or response.get("generation_type")).lower()
    if text == "household" or _is_household_generator_plan(generator_plan):
        return "household"
    return "individual"


def _dataset_profile(
    request: Mapping[str, Any],
    response: Mapping[str, Any],
    generator_plan: Mapping[str, Any],
) -> str:
    return (
        _clean_text(request.get("dataset_profile"))
        or _clean_text(response.get("dataset_profile"))
        or _clean_text(_nested_get(generator_plan, ("pool_summary", "dataset_profile")))
        or V1_2_DEMO_FINAL_PROFILE
    )


def _find_alternative_item(
    alternatives_response: Mapping[str, Any],
    recipe_id: str,
) -> dict[str, Any] | None:
    alternatives = alternatives_response.get("alternatives", [])
    if not isinstance(alternatives, list):
        return None
    for item in alternatives:
        if isinstance(item, Mapping) and _clean_text(item.get("recipe_id")) == recipe_id:
            return dict(item)
    return None


def _find_plan_day(plan: Mapping[str, Any], day_index: int) -> dict[str, Any] | None:
    days = plan.get("days")
    if isinstance(days, list) and days:
        for fallback_index, day in enumerate(days, start=1):
            if not isinstance(day, dict):
                continue
            current_index = int(_to_float(day.get("day_index")) or fallback_index)
            if current_index == day_index:
                return day
        return None
    if day_index == 1 and isinstance(plan, dict):
        return plan
    return None


def _plan_days(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    days = plan.get("days")
    if isinstance(days, list) and days:
        return [day for day in days if isinstance(day, dict)]
    return [plan] if isinstance(plan, dict) else []


def _find_meal_index(
    meals: list[Any],
    *,
    slot: str,
    recipe_id: str,
) -> int | None:
    for index, meal in enumerate(meals):
        if not isinstance(meal, Mapping):
            continue
        if _clean_text(meal.get("slot")).lower() != slot:
            continue
        if _clean_text(meal.get("recipe_id")) == recipe_id:
            return index
    return None


def _find_household_allocation(
    plan: Mapping[str, Any],
    *,
    day_index: int,
    slot: str,
    recipe_id: str,
    member_id: str,
) -> dict[str, Any] | None:
    rows = plan.get("allocations", [])
    if not isinstance(rows, list):
        return None
    fallback = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not _row_matches(row, day_index=day_index, slot=slot, recipe_id=recipe_id):
            continue
        if member_id and _clean_text(row.get("member_id")) == member_id:
            return row
        fallback = fallback or row
    return fallback


def _find_household_selected_meal(
    plan: Mapping[str, Any],
    *,
    day_index: int,
    slot: str,
    recipe_id: str,
) -> dict[str, Any] | None:
    day = _find_plan_day(plan, day_index)
    if not isinstance(day, Mapping):
        return None
    meals = day.get("selected_meals", [])
    if not isinstance(meals, list):
        return None
    index = _find_meal_index(meals, slot=slot, recipe_id=recipe_id)
    if index is None:
        return None
    meal = meals[index]
    return meal if isinstance(meal, dict) else None


def _row_matches(
    row: Mapping[str, Any],
    *,
    day_index: int,
    slot: str,
    recipe_id: str,
    member_id: str | None = None,
    allocation_scope: str | None = None,
) -> bool:
    if int(_to_float(row.get("day_index")) or 1) != day_index:
        return False
    if _clean_text(row.get("slot")).lower() != slot:
        return False
    if _clean_text(row.get("recipe_id")) != recipe_id:
        return False
    if member_id and _clean_text(row.get("member_id")) != member_id:
        return False
    if allocation_scope and _clean_text(row.get("allocation_scope")) != allocation_scope:
        return False
    return True


def _update_rows_in_lists(
    row_lists: list[Any],
    *,
    matcher: Any,
    updater: Any,
) -> list[dict[str, Any]]:
    affected: list[dict[str, Any]] = []
    seen: set[int] = set()
    for rows in row_lists:
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict) or not matcher(row):
                continue
            updater(row)
            marker = id(row)
            if marker not in seen:
                affected.append(row)
                seen.add(marker)
    return affected


def _recompute_household_member_daily_rows(plan: dict[str, Any]) -> None:
    allocations = [row for row in plan.get("allocations", []) if isinstance(row, Mapping)]
    existing = [row for row in plan.get("member_daily_rows", []) if isinstance(row, Mapping)]
    existing_by_key = {
        (
            int(_to_float(row.get("day_index")) or 1),
            _clean_text(row.get("member_id")),
        ): dict(row)
        for row in existing
    }
    keys = set(existing_by_key)
    for row in allocations:
        keys.add((int(_to_float(row.get("day_index")) or 1), _clean_text(row.get("member_id"))))

    rows: list[dict[str, Any]] = []
    for day_index, member_id in sorted(keys):
        if not member_id:
            continue
        old = existing_by_key.get((day_index, member_id), {})
        member_rows = [
            row
            for row in allocations
            if int(_to_float(row.get("day_index")) or 1) == day_index
            and _clean_text(row.get("member_id")) == member_id
        ]
        totals = _meal_totals(member_rows)
        row = dict(old)
        row.update(
            {
                "day_index": day_index,
                "member_id": member_id,
                "kcal": totals["total_kcal"],
                "protein_g": totals["total_protein_g"],
                "carbs_g": totals["total_carbs_g"],
                "fat_g": totals["total_fat_g"],
                "shared_slot_count": sum(
                    1 for item in member_rows if _clean_text(item.get("allocation_scope")) == "shared"
                ),
                "individual_slot_count": sum(
                    1 for item in member_rows if _clean_text(item.get("allocation_scope")) == "individual"
                ),
            }
        )
        _update_member_ratios(row)
        rows.append(row)
    plan["member_daily_rows"] = rows


def _update_member_ratios(row: dict[str, Any]) -> None:
    ratio_specs = (
        ("kcal", "target_kcal", "kcal_ratio", "kcal_deviation_pct"),
        ("protein_g", "target_protein_g", "protein_ratio", "protein_deviation_pct"),
        ("carbs_g", "target_carbs_g", "carbs_ratio", "carbs_deviation_pct"),
        ("fat_g", "target_fat_g", "fat_ratio", "fat_deviation_pct"),
    )
    for actual_key, target_key, ratio_key, deviation_key in ratio_specs:
        actual = _to_float(row.get(actual_key))
        target = _to_float(row.get(target_key))
        if target is None or target == 0 or actual is None:
            continue
        ratio = actual / target
        row[ratio_key] = _round(ratio, 4)
        row[deviation_key] = _round((ratio - 1.0) * 100.0, 1)


def _meal_totals(meals: Any) -> dict[str, Any]:
    if not isinstance(meals, list):
        meals = []
    return {
        "total_kcal": _round(sum(_to_float(meal.get("kcal")) or 0.0 for meal in meals if isinstance(meal, Mapping)), 1),
        "total_protein_g": _round(sum(_to_float(meal.get("protein_g")) or 0.0 for meal in meals if isinstance(meal, Mapping)), 1),
        "total_carbs_g": _round(sum(_to_float(meal.get("carbs_g")) or 0.0 for meal in meals if isinstance(meal, Mapping)), 1),
        "total_fat_g": _round(sum(_to_float(meal.get("fat_g")) or 0.0 for meal in meals if isinstance(meal, Mapping)), 1),
        "selected_slot_count": len([meal for meal in meals if isinstance(meal, Mapping)]),
    }


def _per_portion_values(meal: Mapping[str, Any]) -> dict[str, float | None]:
    multiplier = _to_float(meal.get("portion_multiplier")) or 1.0
    values: dict[str, float | None] = {}
    for field in (*MACRO_FIELDS, "portion_grams_estimated"):
        value = _to_float(meal.get(field))
        values[field] = None if value is None else value / multiplier
    return values


def _macro_delta(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    key_pairs = {
        "kcal": ("kcal", "total_kcal"),
        "protein_g": ("protein_g", "total_protein_g"),
        "carbs_g": ("carbs_g", "total_carbs_g"),
        "fat_g": ("fat_g", "total_fat_g"),
    }
    result: dict[str, Any] = {}
    for public_key, candidates in key_pairs.items():
        left_value = _first_number(left, candidates)
        right_value = _first_number(right, candidates)
        result[public_key] = (
            None if left_value is None or right_value is None else _round(right_value - left_value, 1)
        )
    return result


def _first_number(value: Mapping[str, Any], keys: tuple[str, str]) -> float | None:
    for key in keys:
        parsed = _to_float(value.get(key))
        if parsed is not None:
            return parsed
    return None


def _meal_view(meal: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "slot": meal.get("slot"),
        "recipe_id": meal.get("recipe_id"),
        "display_name": meal.get("display_name") or meal.get("recipe"),
        "portion_multiplier": meal.get("portion_multiplier_member")
        or meal.get("portion_multiplier"),
        "kcal": meal.get("kcal"),
        "protein_g": meal.get("protein_g"),
        "carbs_g": meal.get("carbs_g"),
        "fat_g": meal.get("fat_g"),
        **_time_fields_from_row(meal),
    }


def _time_fields_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in TIME_FIELDS if field in row}


def _is_household_generator_plan(plan: Mapping[str, Any]) -> bool:
    return bool(plan.get("household_generation_version")) or bool(plan.get("allocations"))


def _normalize_slot(value: Any) -> str:
    text = _clean_text(value).lower()
    return text if text in SUPPORTED_SLOTS else ""


def _nested_get(value: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    current: Any = value
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _error_response(error_code: str, message: str, **extra: Any) -> dict[str, Any]:
    return {
        "status": "error",
        "error_code": error_code,
        "message": message,
        **extra,
    }


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "nat"}:
        return ""
    return text


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = pd.to_numeric(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return float(parsed)


def _round(value: float, digits: int) -> float:
    return round(float(value), digits)


def _clean_json_value(value: Any) -> Any:
    if isinstance(value, float) and pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return _clean_json_value(value.item())
        except Exception:
            pass
    if isinstance(value, Mapping):
        return {str(key): _clean_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_clean_json_value(item) for item in value]
    return value


def _to_json_safe(value: Any) -> Any:
    from src.generator_v1.service import to_json_safe

    return to_json_safe(value)
