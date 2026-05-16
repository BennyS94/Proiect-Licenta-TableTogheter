from __future__ import annotations

import copy
import itertools
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from src.generator_v1.day_selector import SELECTED_MEAL_FIELDS
from src.generator_v1.day_selector_balanced import (
    compute_day_loss_for_plan,
    select_one_day_plan_balanced,
)
from src.generator_v1.multi_day_audit import (
    summarize_multi_day_plan,
    validate_multi_day_plan,
)
from src.generator_v1.plan_quality_gate import evaluate_plan_quality
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.target_builder import NutritionTarget


MULTI_DAY_MODE_SIMPLE = "simple_3_day"
MULTI_DAY_MODE_GLOBAL = "global_alternatives_3_day"

DEFAULT_MULTI_DAY_CONFIG = {
    "selection_mode": "balanced_day",
    "portion_policy": "target_aware",
    "meal_realism_mode": "practical",
    "quality_gate": "demo_safe",
    "alternative_count": 3,
    "return_alternatives": True,
    "max_candidates_per_slot": 28,
    "candidate_day_alternative_count": 10,
    "global_max_candidates_per_slot": 26,
    "day_candidate_pool_size_target": 75,
    "day_candidate_pool_max": 150,
    "include_slot_forced_variants": True,
    "no_repeat_policy": "prefer",
    "multi_day_speed_mode": "quality",
    "day_candidate_builder": "auto",
    "direct_slot_shortlist_size": 12,
    "direct_max_portions_per_recipe": 2,
    "direct_candidate_pool_record_limit": 0,
    "early_stop_if_no_repeat_accept_found": False,
    "max_dynamic_source_specs": 0,
    "dynamic_top_breakfast_limit": 6,
    "dynamic_top_main_limit": 8,
    "dynamic_seen_main_limit": 12,
}


def generate_multi_day_plan(
    profile: Mapping[str, Any] | None = None,
    target: NutritionTarget | Mapping[str, Any] | None = None,
    slot_candidates: pd.DataFrame | None = None,
    slot_candidates_by_slot: Mapping[str, pd.DataFrame] | None = None,
    days: int = 3,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_config = _resolved_config(config)
    if resolved_config.get("multi_day_mode") == MULTI_DAY_MODE_GLOBAL:
        return generate_multi_day_plan_global_alternatives(
            profile=profile,
            target=target,
            slot_candidates=slot_candidates,
            slot_candidates_by_slot=slot_candidates_by_slot,
            days=days,
            config=resolved_config,
        )
    if target is None:
        raise ValueError("target este obligatoriu pentru multi-day selector.")
    day_count = _normalize_day_count(days)
    slot_order = _slot_order(target)
    candidates_by_slot = _resolve_slot_candidates_by_slot(
        slot_candidates=slot_candidates,
        slot_candidates_by_slot=slot_candidates_by_slot,
        slot_order=slot_order,
    )

    previous_recipe_ids: set[str] = set()
    selected_days: list[dict[str, Any]] = []
    target_data = _target_to_dict(target)
    best_fallback_plan: dict[str, Any] | None = None

    for day_index in range(1, day_count + 1):
        recent_recipe_ids = sorted(previous_recipe_ids)
        if day_index == 1:
            day_plan = _select_best_day(
                slot_candidates_by_slot=candidates_by_slot,
                target=target,
                slot_order=slot_order,
                config=resolved_config,
            )
            best_fallback_plan = copy.deepcopy(day_plan)
        else:
            day_plan = _select_quality_gated_varied_day(
                slot_candidates_by_slot=candidates_by_slot,
                target=target,
                slot_order=slot_order,
                recent_recipe_ids=recent_recipe_ids,
                config=resolved_config,
                fallback_best_plan=best_fallback_plan,
            )

        day_plan["target"] = target_data
        day_payload = _day_payload(
            day_index=day_index,
            plan=day_plan,
            previous_recipe_ids=previous_recipe_ids,
            recent_recipe_ids=recent_recipe_ids,
        )
        selected_days.append(day_payload)
        previous_recipe_ids.update(_selected_recipe_ids(day_payload))

    summary = summarize_multi_day_plan(selected_days, target_data)
    summary.update(
        {
            "requested_days": day_count,
            "actual_days_generated": len(selected_days),
            "no_repeat_policy_requested": resolved_config.get("no_repeat_policy"),
            "no_repeat_policy_used": resolved_config.get("no_repeat_policy"),
            "fallback_used": bool(summary.get("fallback_day_count", 0)),
            "fallback_reason": (
                "day_level_quality_fallback"
                if int(summary.get("fallback_day_count", 0) or 0) > 0
                else ""
            ),
            "day_candidate_pool_count": 0,
            "feasible_no_repeat_combinations": 0,
        }
    )
    validation = validate_multi_day_plan(selected_days, target_data)
    return {
        "days": selected_days,
        "multi_day_summary": summary,
        "multi_day_validation": validation,
        "multi_day_selector_mode": MULTI_DAY_MODE_SIMPLE,
        "multi_day_loss": summary.get("multi_day_loss"),
        "accept_day_count": summary.get("accept_day_count", 0),
        "total_recipe_count": summary["total_recipe_count"],
        "unique_recipe_count": summary["unique_recipe_count"],
        "repeated_recipe_count": summary["repeated_recipe_count"],
        "repeated_recipe_ids": summary["repeated_recipe_ids"],
        "average_day_loss": summary["average_day_loss"],
        "valid_day_count": summary["valid_day_count"],
        "review_day_count": summary["review_day_count"],
        "fallback_day_count": summary["fallback_day_count"],
        "multi_day_warnings": summary["multi_day_warnings"],
        "target": target_data,
        "config": {
            **resolved_config,
            "days": day_count,
        },
    }


def generate_multi_day_plan_global_alternatives(
    profile: Mapping[str, Any] | None = None,
    target: NutritionTarget | Mapping[str, Any] | None = None,
    slot_candidates: pd.DataFrame | None = None,
    slot_candidates_by_slot: Mapping[str, pd.DataFrame] | None = None,
    days: int = 3,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if target is None:
        raise ValueError("target este obligatoriu pentru multi-day selector.")
    resolved_config = _resolved_config({**dict(config or {}), "multi_day_mode": MULTI_DAY_MODE_GLOBAL})
    day_count = _normalize_day_count(days)
    slot_order = _slot_order(target)
    candidates_by_slot = _resolve_slot_candidates_by_slot(
        slot_candidates=slot_candidates,
        slot_candidates_by_slot=slot_candidates_by_slot,
        slot_order=slot_order,
    )
    target_data = _target_to_dict(target)
    candidate_pool_started = time.perf_counter()
    candidate_days = _build_candidate_day_pool(
        slot_candidates_by_slot=candidates_by_slot,
        target=target,
        slot_order=slot_order,
        config=resolved_config,
        day_count=day_count,
    )
    candidate_pool_seconds = time.perf_counter() - candidate_pool_started
    combination_started = time.perf_counter()
    selected_candidates, selection_report = _select_global_day_combination(
        candidate_days=candidate_days,
        day_count=day_count,
        config=resolved_config,
    )
    combination_seconds = time.perf_counter() - combination_started
    selection_report["candidate_pool_build_seconds"] = round(candidate_pool_seconds, 3)
    selection_report["combination_selection_seconds"] = round(combination_seconds, 3)
    direct_diagnostics = _candidate_pool_direct_diagnostics(candidate_days)
    selection_report["direct_candidate_combinations_evaluated"] = direct_diagnostics.get(
        "direct_candidate_combinations_evaluated",
        0,
    )
    profile_stats = _profile_stats(resolved_config)
    if profile_stats is not None:
        selection_report["profile_stats"] = dict(profile_stats)
        selection_report["direct_candidate_combinations_evaluated"] = int(
            profile_stats.get("direct_candidate_combinations_evaluated", 0) or 0
        )

    selected_days: list[dict[str, Any]] = []
    previous_recipe_ids: set[str] = set()
    for day_index, candidate in enumerate(selected_candidates, start=1):
        plan = copy.deepcopy(candidate["plan"])
        day_payload = _day_payload(
            day_index=day_index,
            plan=plan,
            previous_recipe_ids=previous_recipe_ids,
            recent_recipe_ids=sorted(previous_recipe_ids),
        )
        day_payload["candidate_day_id"] = candidate["candidate_day_id"]
        day_payload["candidate_source_mode"] = candidate["source_mode"]
        day_payload["diversity_mode_used"] = candidate["source_mode"]
        day_payload["multi_day_mode_used"] = MULTI_DAY_MODE_GLOBAL
        selected_days.append(day_payload)
        previous_recipe_ids.update(_selected_recipe_ids(day_payload))

    candidate_pool_summary = _candidate_pool_summary(candidate_days)
    summary = summarize_multi_day_plan(selected_days, target_data)
    summary.update(
        {
            "requested_days": day_count,
            "actual_days_generated": len(selected_days),
            "multi_day_loss": selection_report.get("multi_day_loss"),
            "repetition_penalty": selection_report.get("repetition_penalty"),
            "review_day_penalty": selection_report.get("review_day_penalty"),
            "realism_warning_penalty": selection_report.get("realism_warning_penalty"),
            "quality_warnings": selection_report.get("quality_warnings", []),
            "candidate_day_pool_summary": candidate_pool_summary,
            "candidate_day_pool_count": candidate_pool_summary.get("candidate_day_count", 0),
            "day_candidate_pool_count": candidate_pool_summary.get("candidate_day_count", 0),
            "valid_candidate_day_count": candidate_pool_summary.get("valid_candidate_count", 0),
            "accept_candidate_day_count": candidate_pool_summary.get("accept_candidate_count", 0),
            "review_candidate_day_count": candidate_pool_summary.get("review_candidate_count", 0),
            "combinations_evaluated": selection_report.get("combinations_evaluated", 0),
            "feasible_no_repeat_combinations": selection_report.get(
                "feasible_no_repeat_combinations",
                0,
            ),
            "feasible_main_no_repeat_combinations": selection_report.get(
                "feasible_main_no_repeat_combinations",
                0,
            ),
            "no_repeat_policy_requested": selection_report.get("no_repeat_policy_requested"),
            "no_repeat_policy_used": selection_report.get("no_repeat_policy_used"),
            "fallback_from_hard_no_repeat": selection_report.get(
                "fallback_from_hard_no_repeat",
                False,
            ),
            "fallback_used": selection_report.get("fallback_used", False),
            "fallback_reason": selection_report.get("fallback_reason", ""),
            "day_candidate_builder": resolved_config.get("day_candidate_builder"),
            "direct_slot_shortlist_size": resolved_config.get(
                "direct_slot_shortlist_size"
            ),
            "direct_candidate_combinations_evaluated": selection_report.get(
                "direct_candidate_combinations_evaluated",
                0,
            ),
            "candidate_pool_build_seconds": selection_report.get(
                "candidate_pool_build_seconds",
                0.0,
            ),
            "combination_selection_seconds": selection_report.get(
                "combination_selection_seconds",
                0.0,
            ),
            "profile_stats": selection_report.get("profile_stats", {}),
            "combination_search_truncated": selection_report.get(
                "combination_search_truncated",
                False,
            ),
            "combination_candidate_count_available": selection_report.get(
                "combination_candidate_count_available",
                0,
            ),
            "combination_candidate_count_considered": selection_report.get(
                "combination_candidate_count_considered",
                0,
            ),
        }
    )
    summary["multi_day_warnings"] = list(
        dict.fromkeys(
            list(summary.get("multi_day_warnings", []))
            + list(selection_report.get("quality_warnings", []))
        )
    )
    validation = validate_multi_day_plan(
        {
            "days": selected_days,
            "candidate_day_pool_summary": candidate_pool_summary,
        },
        target_data,
    )
    validation["multi_day_classification"] = summary.get("multi_day_classification")
    quality_warnings = list(summary.get("multi_day_warnings", []))
    quality_warnings.extend(selection_report.get("quality_warnings", []))

    return {
        "days": selected_days,
        "multi_day_summary": summary,
        "multi_day_validation": validation,
        "multi_day_selector_mode": MULTI_DAY_MODE_GLOBAL,
        "multi_day_loss": selection_report.get("multi_day_loss"),
        "average_day_loss": summary.get("average_day_loss"),
        "repeated_recipe_ids": summary.get("repeated_recipe_ids", []),
        "repeated_recipe_count": summary.get("repeated_recipe_count", 0),
        "unique_recipe_count": summary.get("unique_recipe_count", 0),
        "review_day_count": summary.get("review_day_count", 0),
        "accept_day_count": summary.get("accept_day_count", 0),
        "fallback_day_count": summary.get("fallback_day_count", 0),
        "repetition_penalty": selection_report.get("repetition_penalty"),
        "quality_warnings": quality_warnings,
        "multi_day_warnings": summary.get("multi_day_warnings", []),
        "candidate_day_pool_summary": summary["candidate_day_pool_summary"],
        "candidate_day_pool": _candidate_pool_rows(candidate_days),
        "selector_diagnostics": {
            "requested_days": day_count,
            "actual_days_generated": len(selected_days),
            "candidate_day_pool_count": candidate_pool_summary.get("candidate_day_count", 0),
            "day_candidate_pool_count": candidate_pool_summary.get("candidate_day_count", 0),
            "valid_candidate_day_count": candidate_pool_summary.get("valid_candidate_count", 0),
            "accept_candidate_day_count": candidate_pool_summary.get("accept_candidate_count", 0),
            "review_candidate_day_count": candidate_pool_summary.get("review_candidate_count", 0),
            "reject_candidate_day_count": candidate_pool_summary.get("reject_candidate_count", 0),
            "combinations_evaluated": selection_report.get("combinations_evaluated", 0),
            "feasible_no_repeat_combinations": selection_report.get(
                "feasible_no_repeat_combinations",
                0,
            ),
            "feasible_main_no_repeat_combinations": selection_report.get(
                "feasible_main_no_repeat_combinations",
                0,
            ),
            "no_repeat_policy_requested": selection_report.get("no_repeat_policy_requested"),
            "no_repeat_policy_used": selection_report.get("no_repeat_policy_used"),
            "fallback_from_hard_no_repeat": selection_report.get(
                "fallback_from_hard_no_repeat",
                False,
            ),
            "fallback_used": selection_report.get("fallback_used", False),
            "fallback_reason": selection_report.get("fallback_reason", ""),
            "day_candidate_builder": resolved_config.get("day_candidate_builder"),
            "direct_slot_shortlist_size": resolved_config.get(
                "direct_slot_shortlist_size"
            ),
            "direct_candidate_combinations_evaluated": selection_report.get(
                "direct_candidate_combinations_evaluated",
                0,
            ),
            "candidate_pool_build_seconds": selection_report.get(
                "candidate_pool_build_seconds",
                0.0,
            ),
            "combination_selection_seconds": selection_report.get(
                "combination_selection_seconds",
                0.0,
            ),
            "profile_stats": selection_report.get("profile_stats", {}),
            "combination_search_truncated": selection_report.get(
                "combination_search_truncated",
                False,
            ),
            "combination_candidate_count_available": selection_report.get(
                "combination_candidate_count_available",
                0,
            ),
            "combination_candidate_count_considered": selection_report.get(
                "combination_candidate_count_considered",
                0,
            ),
        },
        "target": target_data,
        "config": {
            **resolved_config,
            "days": day_count,
        },
    }


def _build_candidate_day_pool(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | Mapping[str, Any],
    slot_order: Sequence[str],
    config: Mapping[str, Any],
    day_count: int,
) -> list[dict[str, Any]]:
    if str(config.get("day_candidate_builder", "balanced_repeated")) == "direct_from_slots":
        direct_candidates = build_day_candidates_direct_from_slots(
            slot_candidates_by_slot=slot_candidates_by_slot,
            target=target,
            config=config,
            slot_order=slot_order,
        )
        if direct_candidates and _candidate_pool_has_requested_path(
            direct_candidates,
            _no_repeat_policy(config),
            day_count,
        ):
            return direct_candidates
        if (
            day_count > 3
            and direct_candidates
            and _candidate_pool_has_fallback_path(direct_candidates, day_count)
        ):
            return direct_candidates
        profile_stats = _profile_stats(config)
        if profile_stats is not None:
            profile_stats["direct_builder_fallback_to_balanced"] = 1

    candidates: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, ...]] = set()
    pool_target = int(config.get("day_candidate_pool_size_target", 75) or 75)
    pool_max = int(config.get("day_candidate_pool_max", 150) or 150)
    include_forced = bool(config.get("include_slot_forced_variants", True))
    selector_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    profile_stats = _profile_stats(config)

    base_plan = _select_and_evaluate_cached(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=target,
        slot_order=slot_order,
        config=_global_selector_config(config, "none", []),
        filtered_ids=set(),
        selector_cache=selector_cache,
        profile_stats=profile_stats,
    )
    _collect_day_candidates(
        candidates=candidates,
        seen_keys=seen_keys,
        source_mode="none",
        source_plan=base_plan,
        target=target,
        profile_stats=profile_stats,
    )
    seed_recent_ids = _selected_recipe_ids(base_plan)
    seed_ids_by_slot = _selected_recipe_ids_by_slot(base_plan)
    source_specs: list[tuple[str, set[str], str, list[str]]] = [
        ("soft", set(), "soft", seed_recent_ids),
        ("avoid_recent", set(), "avoid_recent", seed_recent_ids),
    ]
    for slot in ("breakfast", "lunch", "dinner"):
        recipe_id = seed_ids_by_slot.get(slot)
        if not recipe_id:
            continue
        source_specs.append((f"soft_{slot}", set(), "soft", [recipe_id]))
        source_specs.append((f"avoid_{slot}", set(), "avoid_recent", [recipe_id]))
        if include_forced:
            source_specs.append((f"filter_no_{slot}", {recipe_id}, "none", []))
            source_specs.append((f"filter_no_{slot}_soft", {recipe_id}, "soft", []))

    if include_forced:
        for source_mode, filtered_ids, selector_diversity_mode, recent_ids in _forced_variant_specs(
            candidates=candidates,
            slot_candidates_by_slot=slot_candidates_by_slot,
            slot_order=slot_order,
            existing_specs=source_specs,
        ):
            source_specs.append((source_mode, filtered_ids, selector_diversity_mode, recent_ids))

    for source_mode, filtered_ids, selector_diversity_mode, recent_ids in _dedup_source_specs(source_specs):
        if len(candidates) >= pool_max:
            break
        candidate_source_by_slot = _filter_candidates_by_recipe_ids(
            slot_candidates_by_slot,
            filtered_ids,
        )
        source_plan = _select_and_evaluate_cached(
            slot_candidates_by_slot=candidate_source_by_slot,
            target=target,
            slot_order=slot_order,
            config=_global_selector_config(
                config,
                selector_diversity_mode,
                recent_ids,
            ),
            filtered_ids=filtered_ids,
            selector_cache=selector_cache,
            profile_stats=profile_stats,
        )
        _collect_day_candidates(
            candidates=candidates,
            seen_keys=seen_keys,
            source_mode=source_mode,
            source_plan=source_plan,
            target=target,
            profile_stats=profile_stats,
        )
        if _should_stop_candidate_pool(candidates, pool_target, config, day_count):
            break
    if (
        include_forced
        and len(candidates) < pool_max
        and (
            len(candidates) < pool_target
            or not _candidate_pool_has_no_repeat_path(candidates, day_count)
        )
    ):
        dynamic_specs = _dynamic_no_repeat_specs(
            candidates=candidates,
            slot_candidates_by_slot=slot_candidates_by_slot,
            slot_order=slot_order,
            day_count=day_count,
            existing_specs=source_specs,
            config=config,
        )
        for source_mode, filtered_ids, selector_diversity_mode, recent_ids in _dedup_source_specs(dynamic_specs):
            if len(candidates) >= pool_max:
                break
            candidate_source_by_slot = _filter_candidates_by_recipe_ids(
                slot_candidates_by_slot,
                filtered_ids,
            )
            source_plan = _select_and_evaluate_cached(
                slot_candidates_by_slot=candidate_source_by_slot,
                target=target,
                slot_order=slot_order,
                config=_global_selector_config(
                    config,
                    selector_diversity_mode,
                    recent_ids,
                ),
                filtered_ids=filtered_ids,
                selector_cache=selector_cache,
                profile_stats=profile_stats,
            )
            _collect_day_candidates(
                candidates=candidates,
                seen_keys=seen_keys,
                source_mode=source_mode,
                source_plan=source_plan,
                target=target,
                profile_stats=profile_stats,
            )
            if _should_stop_candidate_pool(candidates, pool_target, config, day_count):
                break
    return candidates


def build_day_candidates_direct_from_slots(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | Mapping[str, Any],
    config: Mapping[str, Any],
    slot_order: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    resolved_slot_order = list(slot_order or _slot_order(target))
    profile_stats = _profile_stats(config)
    shortlist_size = max(4, int(config.get("direct_slot_shortlist_size", 12) or 12))
    max_portions_per_recipe = max(
        1,
        int(config.get("direct_max_portions_per_recipe", 2) or 2),
    )
    pool_target = int(config.get("day_candidate_pool_size_target", 75) or 75)
    pool_max = int(config.get("day_candidate_pool_max", 150) or 150)
    record_limit_config = int(config.get("direct_candidate_pool_record_limit", 0) or 0)
    candidate_limit = record_limit_config or max(pool_target, min(pool_max, 150))
    direct_warnings: list[str] = []
    shortlists: dict[str, list[dict[str, Any]]] = {}
    count_before: dict[str, int] = {}
    count_after: dict[str, int] = {}
    hard_reject_counts: dict[str, int] = {}

    for slot in resolved_slot_order:
        frame = slot_candidates_by_slot.get(str(slot))
        count_before[str(slot)] = int(len(frame)) if frame is not None else 0
        shortlist, stats = _direct_shortlist_for_slot(
            candidates=frame,
            slot=str(slot),
            shortlist_size=shortlist_size,
            max_portions_per_recipe=max_portions_per_recipe,
            warnings=direct_warnings,
        )
        shortlists[str(slot)] = shortlist
        count_after[str(slot)] = len(shortlist)
        hard_reject_counts[str(slot)] = int(stats.get("hard_rejected_count", 0) or 0)

    missing_slots = [
        str(slot)
        for slot in resolved_slot_order
        if not shortlists.get(str(slot))
    ]
    if missing_slots:
        if profile_stats is not None:
            profile_stats["direct_candidate_builder_failed_missing_slots"] = ",".join(
                missing_slots
            )
        return []

    records: list[dict[str, Any]] = []
    seen_recipe_sets: set[tuple[str, ...]] = set()
    evaluated_count = 0
    rejected_repeated_count = 0
    target_data = _target_to_dict(target)
    combination_rows = [shortlists[str(slot)] for slot in resolved_slot_order]
    for combination in itertools.product(*combination_rows):
        recipe_ids = [
            str(row.get("recipe_id", "")).strip()
            for row in combination
            if str(row.get("recipe_id", "")).strip()
        ]
        if len(recipe_ids) != len(combination) or len(set(recipe_ids)) != len(recipe_ids):
            rejected_repeated_count += 1
            continue
        recipe_key = tuple(sorted(recipe_ids))
        signature = _direct_signature(combination)
        if not signature or recipe_key in seen_recipe_sets:
            continue
        seen_recipe_sets.add(recipe_key)
        selected_meals = [_selected_meal_from_direct_row(row) for row in combination]
        loss = compute_day_loss_for_plan(
            selected_meals,
            target_data,
            config=_direct_day_loss_config(config),
        )
        meal_realism_penalties = _direct_meal_realism_penalties(
            selected_meals,
            config,
        )
        adjusted_day_loss = _to_float(loss.get("day_loss")) + _to_float(
            meal_realism_penalties.get("meal_realism_applied_penalty")
        )
        portion_sum = sum(_to_float(meal.get("portion_multiplier")) for meal in selected_meals)
        key = (
            adjusted_day_loss,
            _to_float(loss.get("day_loss")),
            _to_float(loss.get("macro_day_loss")),
            -_to_float(loss.get("average_score_preview")),
            _to_float(loss.get("effective_time_min_sum")),
            recipe_key,
            portion_sum,
        )
        evaluated_count += 1
        records.append(
            {
                "key": key,
                "signature": signature,
                "recipe_key": recipe_key,
                "ids_by_slot": {
                    str(meal.get("slot")): str(meal.get("recipe_id"))
                    for meal in selected_meals
                },
                "alternative": _direct_alternative_payload(
                    selected_meals=selected_meals,
                    loss=loss,
                    adjusted_day_loss=adjusted_day_loss,
                    meal_realism_penalties=meal_realism_penalties,
                    warnings=direct_warnings,
                ),
            }
        )

    selected_records = _select_direct_records(
        records=records,
        limit=candidate_limit,
    )
    if profile_stats is not None:
        profile_stats["direct_candidate_combinations_evaluated"] = evaluated_count
        profile_stats["direct_candidate_repeated_recipe_rejections"] = rejected_repeated_count
        profile_stats["direct_candidate_records_available"] = len(records)
        profile_stats["direct_candidate_records_selected"] = len(selected_records)
        profile_stats["direct_slot_shortlist_size"] = shortlist_size
        profile_stats["direct_max_portions_per_recipe"] = max_portions_per_recipe
        profile_stats["direct_slot_shortlist_counts"] = dict(count_after)

    source_plan = {
        "selector_diagnostics": {
            "selector_mode": "direct_from_slots",
            "candidate_count_per_slot_before_shortlist": count_before,
            "candidate_count_per_slot_after_shortlist": count_after,
            "candidate_count_per_slot_before_realism_filter": count_before,
            "candidate_count_per_slot_after_realism_filter": count_after,
            "hard_rejected_count_by_slot": hard_reject_counts,
            "hard_reject_candidate_count_by_slot": hard_reject_counts,
            "hard_reject_reasons": {},
            "possible_combination_count_before_limit": _direct_combination_count(shortlists),
            "possible_combination_count_after_shortlist": _direct_combination_count(shortlists),
            "evaluated_combination_count": evaluated_count,
            "rejected_repeated_recipe_combination_count": rejected_repeated_count,
            "selector_warnings": direct_warnings.copy(),
            "diversity_mode": "direct_from_slots",
            "meal_realism_mode": "practical",
            "day_candidate_builder": "direct_from_slots",
            "direct_slot_shortlist_size": shortlist_size,
            "direct_max_portions_per_recipe": max_portions_per_recipe,
        },
        "warnings": direct_warnings.copy(),
    }
    candidates: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, ...]] = set()
    for index, record in enumerate(selected_records, start=1):
        alternative = dict(record["alternative"])
        alternative["alternative_rank"] = index
        plan = _plan_from_alternative(
            alternative=alternative,
            source_plan=source_plan,
            source_mode="direct_from_slots",
            target=target,
            profile_stats=profile_stats,
        )
        recipe_key = tuple(record["recipe_key"])
        if recipe_key in seen_keys:
            continue
        seen_keys.add(recipe_key)
        quality_gate = plan.get("quality_gate", {})
        validation = plan.get("validation", {})
        diagnostics = plan.get("selector_diagnostics", {})
        candidates.append(
            {
                "candidate_day_id": f"direct_from_slots_{len(candidates) + 1:03d}",
                "source_mode": "direct_from_slots",
                "alternative_rank": index,
                "plan": plan,
                "recipe_key": recipe_key,
                "quality_gate_status": (
                    quality_gate.get("quality_gate_status")
                    if isinstance(quality_gate, Mapping)
                    else "missing"
                ),
                "validation_status": (
                    validation.get("validation_status")
                    if isinstance(validation, Mapping)
                    else "not_validated"
                ),
                "base_day_loss": _to_float(diagnostics.get("base_day_loss"))
                if isinstance(diagnostics, Mapping)
                else 0.0,
                "adjusted_day_loss": _to_float(diagnostics.get("adjusted_day_loss"))
                if isinstance(diagnostics, Mapping)
                else 0.0,
                "meal_realism_warning_count": _meal_realism_warning_count(
                    plan.get("selected_meals", [])
                ),
                "selected_signature": tuple(record["signature"]),
            }
        )
    return candidates


def _direct_shortlist_for_slot(
    candidates: pd.DataFrame | None,
    slot: str,
    shortlist_size: int,
    max_portions_per_recipe: int,
    warnings: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if candidates is None or candidates.empty:
        warnings.append(f"Nu exista candidati pentru slot: {slot}")
        return [], {"hard_rejected_count": 0}
    prepared = _direct_prepare_realism_fields(candidates)
    hard_mask = prepared["realism_hard_reject"].map(_to_bool)
    hard_rejected_count = int(hard_mask.sum())
    if hard_rejected_count and hard_rejected_count < len(prepared):
        prepared = prepared.loc[~hard_mask].copy()
    elif hard_rejected_count == len(prepared):
        warnings.append(
            "Direct builder fallback realism: toate candidatele din shortlist au hard reject "
            f"pentru slot {slot}; se pastreaza cu penalty."
        )

    per_metric_count = max(shortlist_size, 6)
    unique_metric_count = max(shortlist_size * 2, 12)
    pieces = [
        _direct_top_unique_recipe_candidates(
            prepared,
            "score_preview",
            unique_metric_count,
            False,
        ),
        _direct_top_unique_recipe_candidates(
            prepared,
            "macro_fit",
            unique_metric_count,
            False,
        ),
        _direct_top_unique_recipe_candidates(
            prepared,
            "carbs_g",
            unique_metric_count,
            False,
        ),
        _direct_top_unique_recipe_candidates(
            prepared,
            "protein_g",
            unique_metric_count,
            False,
        ),
        _direct_top_unique_recipe_candidates(
            prepared,
            "kcal",
            unique_metric_count,
            False,
        ),
        _direct_top_unique_recipe_candidates(
            prepared,
            "meal_realism_penalty",
            unique_metric_count,
            True,
        ),
        _direct_top_candidates(prepared, "score_preview", per_metric_count, False),
        _direct_top_candidates(prepared, "macro_fit", per_metric_count, False),
        _direct_top_candidates(prepared, "carbs_g", per_metric_count, False),
        _direct_top_candidates(prepared, "kcal", per_metric_count, False),
        _direct_top_candidates(prepared, "protein_g", per_metric_count, False),
        _direct_top_candidates(prepared, "meal_realism_penalty", per_metric_count, True),
    ]
    shortlist = _direct_select_shortlist_rows(
        pieces=pieces,
        shortlist_size=shortlist_size,
        max_portions_per_recipe=max_portions_per_recipe,
        fallback_candidates=prepared,
    )
    if shortlist.empty:
        return [], {"hard_rejected_count": hard_rejected_count}
    return (
        [
            row.to_dict()
            for _, row in shortlist.iterrows()
        ],
        {"hard_rejected_count": hard_rejected_count},
    )


def _direct_prepare_realism_fields(candidates: pd.DataFrame) -> pd.DataFrame:
    prepared = candidates.copy()
    practical_pairs = {
        "meal_realism_practical_score": "meal_realism_score",
        "meal_realism_practical_penalty": "meal_realism_penalty",
        "meal_realism_practical_flags": "meal_realism_flags",
        "meal_realism_practical_reasons": "meal_realism_reasons",
    }
    for source, target in practical_pairs.items():
        if source in prepared.columns:
            prepared[target] = prepared[source]
    if "meal_realism_penalty" not in prepared.columns:
        prepared["meal_realism_penalty"] = 0.0
    if "realism_hard_reject" not in prepared.columns:
        prepared["realism_hard_reject"] = False
    if "realism_reject_reason" not in prepared.columns:
        prepared["realism_reject_reason"] = [[] for _ in range(len(prepared))]
    return prepared


def _direct_top_candidates(
    candidates: pd.DataFrame,
    column: str,
    count: int,
    ascending_primary: bool,
) -> pd.DataFrame:
    if count <= 0 or candidates.empty or column not in candidates.columns:
        return pd.DataFrame(columns=candidates.columns)
    sort_columns = []
    ascending = []
    if "is_slot_suspicious" in candidates.columns:
        sort_columns.append("is_slot_suspicious")
        ascending.append(True)
    if column != "meal_realism_penalty" and "meal_realism_penalty" in candidates.columns:
        sort_columns.append("meal_realism_penalty")
        ascending.append(True)
    sort_columns.append(column)
    ascending.append(ascending_primary)
    for fallback_column, fallback_ascending in [
        ("score_preview", False),
        ("macro_fit", False),
        ("carbs_g", False),
        ("kcal", False),
        ("protein_g", False),
        ("recipe_id", True),
        ("portion_multiplier", True),
    ]:
        if fallback_column in candidates.columns and fallback_column not in sort_columns:
            sort_columns.append(fallback_column)
            ascending.append(fallback_ascending)
    return (
        candidates.sort_values(
            sort_columns,
            ascending=ascending,
            kind="mergesort",
            na_position="last",
        )
        .head(count)
        .copy()
    )


def _direct_top_unique_recipe_candidates(
    candidates: pd.DataFrame,
    column: str,
    count: int,
    ascending_primary: bool,
) -> pd.DataFrame:
    if count <= 0 or candidates.empty or column not in candidates.columns:
        return pd.DataFrame(columns=candidates.columns)
    sorted_candidates = _direct_top_candidates(
        candidates=candidates,
        column=column,
        count=len(candidates),
        ascending_primary=ascending_primary,
    )
    rows: list[dict[str, Any]] = []
    seen_recipe_ids: set[str] = set()
    for _, row in sorted_candidates.iterrows():
        recipe_id = str(row.get("recipe_id", "")).strip()
        if not recipe_id or recipe_id in seen_recipe_ids:
            continue
        seen_recipe_ids.add(recipe_id)
        rows.append(row.to_dict())
        if len(rows) >= count:
            break
    return pd.DataFrame(rows, columns=candidates.columns)


def _direct_dedupe_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    seen: set[tuple[str, str, float]] = set()
    rows: list[dict[str, Any]] = []
    for _, row in candidates.iterrows():
        key = (
            str(row.get("slot", "")).strip(),
            str(row.get("recipe_id", "")).strip(),
            round(_to_float(row.get("portion_multiplier")), 4),
        )
        if not key[1] or key in seen:
            continue
        seen.add(key)
        rows.append(row.to_dict())
    return pd.DataFrame(rows, columns=candidates.columns)


def _direct_sort_shortlist(candidates: pd.DataFrame) -> pd.DataFrame:
    sort_columns = [
        column
        for column in [
            "is_slot_suspicious",
            "score_preview",
            "macro_fit",
            "carbs_g",
            "kcal",
            "protein_g",
            "meal_realism_penalty",
            "recipe_id",
            "portion_multiplier",
        ]
        if column in candidates.columns
    ]
    ascending = [
        column in {"is_slot_suspicious", "meal_realism_penalty", "recipe_id", "portion_multiplier"}
        for column in sort_columns
    ]
    return candidates.sort_values(
        sort_columns,
        ascending=ascending,
        kind="mergesort",
        na_position="last",
    )


def _direct_select_shortlist_rows(
    pieces: Sequence[pd.DataFrame],
    shortlist_size: int,
    max_portions_per_recipe: int,
    fallback_candidates: pd.DataFrame,
) -> pd.DataFrame:
    selected_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, float]] = set()
    recipe_counts: Counter[str] = Counter()

    def add_row(row_data: Mapping[str, Any], enforce_recipe_limit: bool = True) -> bool:
        recipe_id = str(row_data.get("recipe_id", "")).strip()
        if not recipe_id:
            return False
        key = (
            str(row_data.get("slot", "")).strip(),
            recipe_id,
            round(_to_float(row_data.get("portion_multiplier")), 4),
        )
        if key in seen_keys:
            return False
        if enforce_recipe_limit and recipe_counts[recipe_id] >= max_portions_per_recipe:
            return False
        seen_keys.add(key)
        recipe_counts[recipe_id] += 1
        selected_rows.append(dict(row_data))
        return True

    prepared_pieces = [
        _direct_dedupe_candidates(piece)
        for piece in pieces
        if piece is not None and not piece.empty
    ]
    positions = [0 for _ in prepared_pieces]
    while len(selected_rows) < shortlist_size and prepared_pieces:
        progressed = False
        for index, piece in enumerate(prepared_pieces):
            while positions[index] < len(piece):
                row_data = piece.iloc[positions[index]].to_dict()
                positions[index] += 1
                if add_row(row_data):
                    progressed = True
                    break
            if len(selected_rows) >= shortlist_size:
                break
        if not progressed:
            break

    if len(selected_rows) < shortlist_size:
        combined = pd.concat(prepared_pieces, ignore_index=True) if prepared_pieces else pd.DataFrame()
        fallback = _direct_sort_shortlist(_direct_dedupe_candidates(combined))
        for _, row in fallback.iterrows():
            if len(selected_rows) >= shortlist_size:
                break
            add_row(row.to_dict())

    if len(selected_rows) < shortlist_size:
        fallback = _direct_sort_shortlist(_direct_dedupe_candidates(fallback_candidates))
        for _, row in fallback.iterrows():
            if len(selected_rows) >= shortlist_size:
                break
            add_row(row.to_dict(), enforce_recipe_limit=False)

    return pd.DataFrame(selected_rows, columns=fallback_candidates.columns)


def _direct_limit_portions_per_recipe(
    candidates: pd.DataFrame,
    max_portions_per_recipe: int,
    limit: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for _, row in candidates.iterrows():
        recipe_id = str(row.get("recipe_id", "")).strip()
        if not recipe_id:
            continue
        if counts[recipe_id] >= max_portions_per_recipe:
            continue
        counts[recipe_id] += 1
        rows.append(row.to_dict())
        if len(rows) >= limit:
            break
    if len(rows) < limit:
        seen_keys = {
            (
                str(row.get("slot", "")).strip(),
                str(row.get("recipe_id", "")).strip(),
                round(_to_float(row.get("portion_multiplier")), 4),
            )
            for row in rows
        }
        for _, row in candidates.iterrows():
            key = (
                str(row.get("slot", "")).strip(),
                str(row.get("recipe_id", "")).strip(),
                round(_to_float(row.get("portion_multiplier")), 4),
            )
            if not key[1] or key in seen_keys:
                continue
            seen_keys.add(key)
            rows.append(row.to_dict())
            if len(rows) >= limit:
                break
    return pd.DataFrame(rows, columns=candidates.columns)


def _direct_signature(rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    parts = []
    for row in rows:
        slot = str(row.get("slot", "")).strip()
        recipe_id = str(row.get("recipe_id", "")).strip()
        portion = _to_float(row.get("portion_multiplier"))
        if not slot or not recipe_id:
            continue
        parts.append(f"{slot}:{recipe_id}:{portion:.4f}")
    return tuple(sorted(parts))


def _selected_meal_from_direct_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: _clean_direct_value(row.get(field))
        for field in SELECTED_MEAL_FIELDS
    }


def _direct_day_loss_config(config: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "meal_realism_mode": "practical",
        "max_effective_time_min": config.get("max_effective_time_min", 300.0),
    }


def _direct_meal_realism_penalties(
    selected_meals: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    mode = str(config.get("meal_realism_mode", "practical") or "practical")
    raw_penalties = [
        _to_float(meal.get("meal_realism_penalty"))
        for meal in selected_meals
    ]
    total_penalty = (
        sum(raw_penalties) / len(raw_penalties)
        if raw_penalties
        else 0.0
    )
    applied_penalty = total_penalty if mode in {"soft", "practical"} else 0.0
    flags_by_meal = []
    for meal in selected_meals:
        flags = _reason_items(meal.get("meal_realism_flags"))
        if not flags:
            continue
        flags_by_meal.append(
            {
                "slot": str(meal.get("slot", "")).strip(),
                "recipe_id": str(meal.get("recipe_id", "")).strip(),
                "flags": flags,
            }
        )
    return {
        "meal_realism_mode": mode,
        "meal_realism_total_penalty": round(total_penalty, 6),
        "meal_realism_applied_penalty": round(applied_penalty, 6),
        "meal_realism_flags_by_meal": flags_by_meal,
    }


def _direct_alternative_payload(
    selected_meals: list[dict[str, Any]],
    loss: Mapping[str, Any],
    adjusted_day_loss: float,
    meal_realism_penalties: Mapping[str, Any],
    warnings: Sequence[str],
) -> dict[str, Any]:
    diversity_penalties = {
        "diversity_mode": "direct_from_slots",
        "recent_recipe_count": 0,
        "recent_recipe_ids_selected": [],
        "recent_recipe_penalty": 0.0,
        "same_family_duplicate_count": 0,
        "same_family_penalty": 0.0,
        "total_diversity_penalty": 0.0,
    }
    return {
        "alternative_rank": None,
        "selected_meals": selected_meals,
        "day_totals": _direct_day_totals(selected_meals),
        "base_day_loss": round(_to_float(loss.get("day_loss")), 6),
        "adjusted_day_loss": round(adjusted_day_loss, 6),
        "kcal_loss": round(_to_float(loss.get("kcal_loss")), 6),
        "protein_loss": round(_to_float(loss.get("protein_loss")), 6),
        "carbs_loss": round(_to_float(loss.get("carbs_loss")), 6),
        "fat_loss": round(_to_float(loss.get("fat_loss")), 6),
        "macro_day_loss": round(_to_float(loss.get("macro_day_loss")), 6),
        "time_penalty": round(_to_float(loss.get("time_penalty")), 6),
        "average_score_preview": round(_to_float(loss.get("average_score_preview")), 6),
        "effective_time_min_sum": round(_to_float(loss.get("effective_time_min_sum")), 6),
        "selected_recipe_ids": [
            str(meal.get("recipe_id"))
            for meal in selected_meals
            if meal.get("recipe_id")
        ],
        "selector_warnings": list(warnings),
        "diversity_penalties": diversity_penalties,
        "meal_realism_penalties": dict(meal_realism_penalties),
        "meal_realism_total_penalty": meal_realism_penalties.get(
            "meal_realism_total_penalty",
            0.0,
        ),
        "meal_realism_applied_penalty": meal_realism_penalties.get(
            "meal_realism_applied_penalty",
            0.0,
        ),
    }


def _direct_day_totals(selected_meals: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "total_kcal": round(sum(_to_float(meal.get("kcal")) for meal in selected_meals), 1),
        "total_protein_g": round(sum(_to_float(meal.get("protein_g")) for meal in selected_meals), 1),
        "total_carbs_g": round(sum(_to_float(meal.get("carbs_g")) for meal in selected_meals), 1),
        "total_fat_g": round(sum(_to_float(meal.get("fat_g")) for meal in selected_meals), 1),
        "original_total_kcal": _direct_original_macro_total(
            selected_meals,
            "original_energy_kcal_per_serving",
        ),
        "original_total_protein_g": _direct_original_macro_total(
            selected_meals,
            "original_protein_g_per_serving",
        ),
        "original_total_carbs_g": _direct_original_macro_total(
            selected_meals,
            "original_carbs_g_per_serving",
        ),
        "original_total_fat_g": _direct_original_macro_total(
            selected_meals,
            "original_fat_g_per_serving",
        ),
        "uses_pilot_nutrition_overlay_count": sum(
            1
            for meal in selected_meals
            if bool(meal.get("uses_pilot_nutrition_overlay"))
        ),
        "total_time_min_sum": round(
            sum(_to_float(meal.get("total_time_min")) for meal in selected_meals),
            1,
        ),
        "effective_time_min_sum": _direct_sum_optional(
            selected_meals,
            "effective_time_min_for_scoring",
        ),
        "passive_time_estimated_sum": _direct_sum_optional(
            selected_meals,
            "passive_time_estimated_min",
        ),
        "selected_slot_count": len(selected_meals),
    }


def _direct_original_macro_total(
    selected_meals: Sequence[Mapping[str, Any]],
    field: str,
) -> float:
    total = 0.0
    for meal in selected_meals:
        total += _to_float(meal.get(field)) * _to_float(meal.get("portion_multiplier"))
    return round(total, 1)


def _direct_sum_optional(
    selected_meals: Sequence[Mapping[str, Any]],
    field: str,
) -> float | None:
    values = []
    for meal in selected_meals:
        value = pd.to_numeric(meal.get(field), errors="coerce")
        if pd.isna(value):
            continue
        values.append(float(value))
    if not values:
        return None
    return round(sum(values), 1)


def _select_direct_records(
    records: Sequence[Mapping[str, Any]],
    limit: int,
) -> list[Mapping[str, Any]]:
    if not records or limit <= 0:
        return []
    sorted_records = sorted(records, key=lambda record: record["key"])
    selected: list[Mapping[str, Any]] = []
    seen_signatures: set[tuple[str, ...]] = set()

    def add_record(record: Mapping[str, Any]) -> bool:
        signature = tuple(record.get("signature", ()))
        if not signature or signature in seen_signatures or len(selected) >= limit:
            return False
        seen_signatures.add(signature)
        selected.append(record)
        return True

    for record in sorted_records[: max(12, limit // 5)]:
        add_record(record)

    per_slot_quota = max(4, limit // 12)
    for slot in ("breakfast", "lunch", "dinner", "snack"):
        added_for_slot = 0
        seen_recipe_ids: set[str] = set()
        for record in sorted_records:
            ids_by_slot = record.get("ids_by_slot", {})
            if not isinstance(ids_by_slot, Mapping):
                continue
            recipe_id = str(ids_by_slot.get(slot, "")).strip()
            if not recipe_id or recipe_id in seen_recipe_ids:
                continue
            seen_recipe_ids.add(recipe_id)
            if add_record(record):
                added_for_slot += 1
            if added_for_slot >= per_slot_quota:
                break

    for pair_slots, pair_quota in [
        (("lunch", "dinner"), max(8, limit // 5)),
        (("breakfast", "lunch"), max(8, limit // 6)),
        (("breakfast", "dinner"), max(8, limit // 6)),
    ]:
        _add_direct_records_by_slot_key(
            sorted_records=sorted_records,
            selected_count=lambda: len(selected),
            limit=limit,
            add_record=add_record,
            slots=pair_slots,
            quota=pair_quota,
        )

    _add_direct_records_by_slot_key(
        sorted_records=sorted_records,
        selected_count=lambda: len(selected),
        limit=limit,
        add_record=add_record,
        slots=("breakfast", "lunch", "dinner"),
        quota=max(10, limit // 5),
    )

    for record in sorted_records:
        if len(selected) >= limit:
            break
        add_record(record)
    return selected


def _add_direct_records_by_slot_key(
    sorted_records: Sequence[Mapping[str, Any]],
    selected_count: Any,
    limit: int,
    add_record: Any,
    slots: Sequence[str],
    quota: int,
) -> None:
    added = 0
    seen_keys: set[tuple[str, ...]] = set()
    for record in sorted_records:
        if selected_count() >= limit:
            return
        ids_by_slot = record.get("ids_by_slot", {})
        if not isinstance(ids_by_slot, Mapping):
            continue
        key = tuple(str(ids_by_slot.get(slot, "")).strip() for slot in slots)
        if any(not item for item in key) or key in seen_keys:
            continue
        seen_keys.add(key)
        if add_record(record):
            added += 1
        if added >= quota:
            return


def _direct_combination_count(shortlists: Mapping[str, Sequence[Mapping[str, Any]]]) -> int:
    total = 1
    for rows in shortlists.values():
        total *= max(0, len(rows))
    return int(total)


def _candidate_pool_has_requested_path(
    candidates: Sequence[Mapping[str, Any]],
    policy: str,
    day_count: int,
) -> bool:
    usable = [
        candidate
        for candidate in candidates
        if candidate.get("validation_status") == "valid"
        and candidate.get("quality_gate_status") == "accept"
    ]
    if len(usable) < day_count:
        return False
    if policy == "none":
        return True
    return (
        _best_scored_combination(
            usable,
            day_count,
            no_repeat_policy=policy,
            require_policy=True,
        )
        is not None
    )


def _candidate_pool_has_fallback_path(
    candidates: Sequence[Mapping[str, Any]],
    day_count: int,
) -> bool:
    usable = [
        candidate
        for candidate in candidates
        if candidate.get("validation_status") == "valid"
        and candidate.get("quality_gate_status") in {"accept", "review"}
    ]
    if len(usable) >= day_count:
        return True
    valid_candidates = [
        candidate
        for candidate in candidates
        if candidate.get("validation_status") == "valid"
    ]
    return len(valid_candidates) >= day_count


def _clean_direct_value(value: object) -> object:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def _should_stop_candidate_pool(
    candidates: Sequence[Mapping[str, Any]],
    pool_target: int,
    config: Mapping[str, Any],
    day_count: int,
) -> bool:
    has_no_repeat_path = _candidate_pool_has_no_repeat_path(candidates, day_count)
    if bool(config.get("early_stop_if_no_repeat_accept_found", False)) and has_no_repeat_path:
        return True
    return len(candidates) >= pool_target and has_no_repeat_path


def _select_and_evaluate_cached(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | Mapping[str, Any],
    slot_order: Sequence[str],
    config: Mapping[str, Any],
    filtered_ids: set[str],
    selector_cache: dict[tuple[Any, ...], dict[str, Any]],
    profile_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cache_key = _selector_cache_key(config, filtered_ids)
    if cache_key in selector_cache:
        if profile_stats is not None:
            profile_stats["balanced_selector_cache_hits"] = (
                int(profile_stats.get("balanced_selector_cache_hits", 0)) + 1
            )
        return copy.deepcopy(selector_cache[cache_key])
    started = time.perf_counter()
    plan = _select_and_evaluate(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=target,
        slot_order=slot_order,
        config=config,
        profile_stats=profile_stats,
    )
    if profile_stats is not None:
        profile_stats["balanced_selector_runs"] = int(
            profile_stats.get("balanced_selector_runs", 0)
        ) + 1
        profile_stats["balanced_selector_seconds"] = round(
            _to_float(profile_stats.get("balanced_selector_seconds")) + time.perf_counter() - started,
            6,
        )
    selector_cache[cache_key] = copy.deepcopy(plan)
    return plan


def _selector_cache_key(
    config: Mapping[str, Any],
    filtered_ids: set[str],
) -> tuple[Any, ...]:
    return (
        tuple(sorted(filtered_ids)),
        str(config.get("diversity_mode", "none")),
        tuple(sorted(str(item) for item in config.get("recent_recipe_ids", []) or [])),
        int(config.get("alternative_count", 1) or 1),
        int(config.get("max_candidates_per_slot", 0) or 0),
        str(config.get("meal_realism_mode", "practical")),
        int(config.get("min_recipe_difference_between_alternatives", 1) or 1),
    )


def _forced_variant_specs(
    candidates: Sequence[Mapping[str, Any]],
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    slot_order: Sequence[str],
    existing_specs: Sequence[tuple[str, set[str], str, list[str]]],
) -> list[tuple[str, set[str], str, list[str]]]:
    specs: list[tuple[str, set[str], str, list[str]]] = []
    seen_ids = {
        recipe_id
        for _source_mode, filtered_ids, _diversity_mode, recent_ids in existing_specs
        for recipe_id in list(filtered_ids) + list(recent_ids)
    }
    for candidate in candidates:
        plan = candidate.get("plan", {})
        if not isinstance(plan, Mapping):
            continue
        for meal in plan.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            slot = str(meal.get("slot", "")).strip().lower()
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if slot not in {"breakfast", "lunch", "dinner"} or not recipe_id:
                continue
            if recipe_id in seen_ids:
                continue
            seen_ids.add(recipe_id)
            specs.append((f"filter_no_candidate_{slot}_{len(specs) + 1}", {recipe_id}, "none", []))

    for slot in slot_order:
        if slot not in {"breakfast", "lunch", "dinner"}:
            continue
        for recipe_id in _top_recipe_ids_for_slot(slot_candidates_by_slot.get(slot), limit=4):
            if recipe_id in seen_ids:
                continue
            seen_ids.add(recipe_id)
            specs.append((f"filter_no_top_{slot}_{len(specs) + 1}", {recipe_id}, "none", []))
    return specs


def _dynamic_no_repeat_specs(
    candidates: Sequence[Mapping[str, Any]],
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    slot_order: Sequence[str],
    day_count: int,
    existing_specs: Sequence[tuple[str, set[str], str, list[str]]],
    config: Mapping[str, Any],
) -> list[tuple[str, set[str], str, list[str]]]:
    specs: list[tuple[str, set[str], str, list[str]]] = []
    seen_ids = {
        recipe_id
        for _source_mode, filtered_ids, _diversity_mode, recent_ids in existing_specs
        for recipe_id in list(filtered_ids) + list(recent_ids)
    }
    usable = [
        candidate
        for candidate in candidates
        if candidate.get("validation_status") == "valid"
        and candidate.get("quality_gate_status") in {"accept", "review"}
    ]
    best_scored = _best_scored_combination(
        usable,
        min(day_count, len(usable)),
        no_repeat_policy="prefer",
    )
    if best_scored is not None:
        _key, best_combination, best_score = best_scored
        repeated_ids = list(best_score.get("repeated_recipe_ids", []))
        for recipe_id in repeated_ids:
            if recipe_id and recipe_id not in seen_ids:
                seen_ids.add(recipe_id)
                specs.append(
                    (
                        f"filter_no_repeated_{len(specs) + 1}",
                        {str(recipe_id)},
                        "none",
                        [],
                    )
                )
                specs.append(
                    (
                        f"avoid_repeated_{len(specs) + 1}",
                        set(),
                        "avoid_recent",
                        [str(recipe_id)],
                    )
                )
        for day_position, candidate in enumerate(best_combination, start=1):
            plan = candidate.get("plan", {})
            if not isinstance(plan, Mapping):
                continue
            day_recipe_ids = _selected_recipe_ids(plan)
            if day_recipe_ids:
                specs.append(
                    (
                        f"avoid_best_day_{day_position}",
                        set(),
                        "avoid_recent",
                        day_recipe_ids,
                    )
                )
            for meal in plan.get("selected_meals", []):
                if not isinstance(meal, Mapping):
                    continue
                slot = str(meal.get("slot", "")).strip().lower()
                recipe_id = str(meal.get("recipe_id", "")).strip()
                if slot not in {"breakfast", "lunch", "dinner"} or not recipe_id:
                    continue
                if recipe_id in seen_ids:
                    continue
                seen_ids.add(recipe_id)
                specs.append(
                    (
                        f"filter_no_best_{day_position}_{slot}",
                        {recipe_id},
                        "none",
                        [],
                    )
                )

    for slot in slot_order:
        if slot not in {"breakfast", "lunch", "dinner"}:
            continue
        top_limit = (
            int(config.get("dynamic_top_main_limit", 8) or 8)
            if slot in {"lunch", "dinner"}
            else int(config.get("dynamic_top_breakfast_limit", 6) or 6)
        )
        for recipe_id in _top_recipe_ids_for_slot(slot_candidates_by_slot.get(slot), limit=top_limit):
            if recipe_id in seen_ids:
                continue
            seen_ids.add(recipe_id)
            specs.append((f"filter_no_dynamic_{slot}_{len(specs) + 1}", {recipe_id}, "none", []))

    selected_main_ids: list[str] = []
    for candidate in usable:
        plan = candidate.get("plan", {})
        if not isinstance(plan, Mapping):
            continue
        for meal in plan.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            slot = str(meal.get("slot", "")).strip().lower()
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if slot not in {"lunch", "dinner"} or not recipe_id or recipe_id in selected_main_ids:
                continue
            selected_main_ids.append(recipe_id)
    seen_main_limit = int(config.get("dynamic_seen_main_limit", 12) or 12)
    for recipe_id in selected_main_ids[:seen_main_limit]:
        if recipe_id in seen_ids:
            continue
        seen_ids.add(recipe_id)
        specs.append((f"filter_no_seen_main_{len(specs) + 1}", {recipe_id}, "none", []))
    max_specs = int(config.get("max_dynamic_source_specs", 0) or 0)
    if max_specs > 0:
        return specs[:max_specs]
    return specs


def _top_recipe_ids_for_slot(frame: pd.DataFrame | None, limit: int) -> list[str]:
    if frame is None or frame.empty:
        return []
    working = frame.copy()
    working["_sort_score"] = working.apply(
        lambda row: (
            -_to_float(row.get("score_preview")),
            -_to_float(row.get("macro_fit")),
            _to_float(row.get("effective_time_min_for_scoring")),
            str(row.get("recipe_id", "")),
        ),
        axis=1,
    )
    recipe_ids: list[str] = []
    seen: set[str] = set()
    for _, row in working.sort_values("_sort_score").iterrows():
        recipe_id = str(row.get("recipe_id", "")).strip()
        if not recipe_id or recipe_id in seen:
            continue
        seen.add(recipe_id)
        recipe_ids.append(recipe_id)
        if len(recipe_ids) >= limit:
            break
    return recipe_ids


def _dedup_source_specs(
    specs: Sequence[tuple[str, set[str], str, list[str]]],
) -> list[tuple[str, set[str], str, list[str]]]:
    result: list[tuple[str, set[str], str, list[str]]] = []
    seen: set[tuple[tuple[str, ...], str, tuple[str, ...]]] = set()
    for source_mode, filtered_ids, diversity_mode, recent_ids in specs:
        key = (tuple(sorted(filtered_ids)), diversity_mode, tuple(sorted(recent_ids)))
        if key in seen:
            continue
        seen.add(key)
        result.append((source_mode, filtered_ids, diversity_mode, recent_ids))
    return result


def _filter_candidates_by_recipe_ids(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    recipe_ids: set[str],
) -> dict[str, pd.DataFrame]:
    if not recipe_ids:
        return {slot: frame.copy() for slot, frame in slot_candidates_by_slot.items()}
    return {
        slot: frame.loc[~frame["recipe_id"].astype(str).isin(recipe_ids)].copy()
        for slot, frame in slot_candidates_by_slot.items()
    }


def _candidate_pool_has_no_repeat_path(
    candidates: Sequence[Mapping[str, Any]],
    day_count: int,
) -> bool:
    usable = [
        candidate
        for candidate in candidates
        if candidate.get("validation_status") == "valid"
        and candidate.get("quality_gate_status") == "accept"
    ]
    if len(usable) < day_count:
        return False
    return (
        _best_scored_combination(
            usable,
            day_count,
            no_repeat_policy="hard",
            require_policy=True,
        )
        is not None
    )


def _collect_day_candidates(
    candidates: list[dict[str, Any]],
    seen_keys: set[tuple[str, ...]],
    source_mode: str,
    source_plan: Mapping[str, Any],
    target: NutritionTarget | Mapping[str, Any],
    profile_stats: dict[str, Any] | None = None,
) -> None:
    alternatives = source_plan.get("alternatives")
    if not isinstance(alternatives, list) or not alternatives:
        alternatives = [_alternative_from_plan(source_plan)]

    for alternative in alternatives:
        if not isinstance(alternative, Mapping):
            continue
        recipe_key = _alternative_recipe_key(alternative)
        if not recipe_key or recipe_key in seen_keys:
            if profile_stats is not None:
                profile_stats["candidate_duplicate_skips"] = int(
                    profile_stats.get("candidate_duplicate_skips", 0)
                ) + 1
            continue
        plan = _plan_from_alternative(
            alternative=alternative,
            source_plan=source_plan,
            source_mode=source_mode,
            target=target,
            profile_stats=profile_stats,
        )
        seen_keys.add(recipe_key)
        quality_gate = plan.get("quality_gate", {})
        validation = plan.get("validation", {})
        diagnostics = plan.get("selector_diagnostics", {})
        candidate_id = f"{source_mode}_{len(candidates) + 1:03d}"
        candidates.append(
            {
                "candidate_day_id": candidate_id,
                "source_mode": source_mode,
                "alternative_rank": alternative.get("alternative_rank"),
                "plan": plan,
                "recipe_key": recipe_key,
                "quality_gate_status": (
                    quality_gate.get("quality_gate_status")
                    if isinstance(quality_gate, Mapping)
                    else "missing"
                ),
                "validation_status": (
                    validation.get("validation_status")
                    if isinstance(validation, Mapping)
                    else "not_validated"
                ),
                "base_day_loss": _to_float(diagnostics.get("base_day_loss"))
                if isinstance(diagnostics, Mapping)
                else 0.0,
                "adjusted_day_loss": _to_float(diagnostics.get("adjusted_day_loss"))
                if isinstance(diagnostics, Mapping)
                else 0.0,
                "meal_realism_warning_count": _meal_realism_warning_count(
                    plan.get("selected_meals", [])
                ),
                "selected_signature": _alternative_signature(alternative),
            }
        )


def _alternative_recipe_key(alternative: Mapping[str, Any]) -> tuple[str, ...]:
    recipe_ids = [
        str(meal.get("recipe_id", "")).strip()
        for meal in alternative.get("selected_meals", [])
        if isinstance(meal, Mapping) and str(meal.get("recipe_id", "")).strip()
    ]
    return tuple(sorted(recipe_ids))


def _alternative_signature(alternative: Mapping[str, Any]) -> tuple[str, ...]:
    parts: list[str] = []
    for meal in alternative.get("selected_meals", []):
        if not isinstance(meal, Mapping):
            continue
        slot = str(meal.get("slot", "")).strip()
        recipe_id = str(meal.get("recipe_id", "")).strip()
        portion = _to_float(meal.get("portion_multiplier"))
        if not recipe_id:
            continue
        parts.append(f"{slot}:{recipe_id}:{portion:.4f}")
    return tuple(sorted(parts))


def _plan_from_alternative(
    alternative: Mapping[str, Any],
    source_plan: Mapping[str, Any],
    source_mode: str,
    target: NutritionTarget | Mapping[str, Any],
    profile_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selector_diagnostics = dict(source_plan.get("selector_diagnostics", {}))
    selector_diagnostics.update(
        {
            "selector_mode": "balanced_day",
            "diversity_mode": source_mode,
            "base_day_loss": alternative.get("base_day_loss"),
            "adjusted_day_loss": alternative.get("adjusted_day_loss"),
            "day_loss": alternative.get("adjusted_day_loss"),
            "kcal_loss": alternative.get("kcal_loss"),
            "protein_loss": alternative.get("protein_loss"),
            "carbs_loss": alternative.get("carbs_loss"),
            "fat_loss": alternative.get("fat_loss"),
            "meal_realism_total_penalty": alternative.get(
                "meal_realism_total_penalty",
                0.0,
            ),
            "meal_realism_applied_penalty": alternative.get(
                "meal_realism_applied_penalty",
                0.0,
            ),
            "diversity_penalties": alternative.get("diversity_penalties", {}),
            "meal_realism_penalties": alternative.get("meal_realism_penalties", {}),
            "alternative_rank": alternative.get("alternative_rank"),
        }
    )
    plan = {
        "selected_meals": list(alternative.get("selected_meals", [])),
        "day_totals": dict(alternative.get("day_totals", {})),
        "warnings": list(alternative.get("selector_warnings", [])),
        "selector_mode": "balanced_day",
        "selector_diagnostics": selector_diagnostics,
        "alternatives": [dict(alternative)],
    }
    validation_started = time.perf_counter()
    plan["validation"] = validate_one_day_plan(plan, target)
    if profile_stats is not None:
        profile_stats["validation_seconds"] = round(
            _to_float(profile_stats.get("validation_seconds"))
            + time.perf_counter()
            - validation_started,
            6,
        )
    quality_started = time.perf_counter()
    plan["quality_gate"] = evaluate_plan_quality(
        plan,
        target,
        config={"quality_gate": "demo_safe"},
    )
    if profile_stats is not None:
        profile_stats["quality_gate_seconds"] = round(
            _to_float(profile_stats.get("quality_gate_seconds"))
            + time.perf_counter()
            - quality_started,
            6,
        )
        profile_stats["quality_gate_evaluation_count"] = int(
            profile_stats.get("quality_gate_evaluation_count", 0)
        ) + 1
    _attach_quality_fields(
        plan=plan,
        selected_mode=source_mode,
        fallback_used=False,
        recent_recipe_count=0,
    )
    return plan


def _alternative_from_plan(source_plan: Mapping[str, Any]) -> dict[str, Any]:
    diagnostics = source_plan.get("selector_diagnostics", {})
    if not isinstance(diagnostics, Mapping):
        diagnostics = {}
    return {
        "alternative_rank": 1,
        "selected_meals": list(source_plan.get("selected_meals", [])),
        "day_totals": dict(source_plan.get("day_totals", {})),
        "base_day_loss": diagnostics.get("base_day_loss"),
        "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
        "kcal_loss": diagnostics.get("kcal_loss"),
        "protein_loss": diagnostics.get("protein_loss"),
        "carbs_loss": diagnostics.get("carbs_loss"),
        "fat_loss": diagnostics.get("fat_loss"),
        "selected_recipe_ids": _selected_recipe_ids(source_plan),
        "selector_warnings": list(source_plan.get("warnings", [])),
        "diversity_penalties": diagnostics.get("diversity_penalties", {}),
        "meal_realism_penalties": diagnostics.get("meal_realism_penalties", {}),
        "meal_realism_total_penalty": diagnostics.get("meal_realism_total_penalty", 0.0),
        "meal_realism_applied_penalty": diagnostics.get(
            "meal_realism_applied_penalty",
            0.0,
        ),
    }


def _select_global_day_combination(
    candidate_days: Sequence[Mapping[str, Any]],
    day_count: int,
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    requested_policy = _no_repeat_policy(config)
    accept_candidates = [
        candidate
        for candidate in candidate_days
        if candidate.get("validation_status") == "valid"
        and candidate.get("quality_gate_status") == "accept"
    ]
    accept_or_review_candidates = [
        candidate
        for candidate in candidate_days
        if candidate.get("validation_status") == "valid"
        and candidate.get("quality_gate_status") in {"accept", "review"}
    ]
    severe_warnings: list[str] = []

    if requested_policy == "hard":
        hard_score = _best_for_policy(
            accept_candidates=accept_candidates,
            accept_or_review_candidates=accept_or_review_candidates,
            candidate_days=candidate_days,
            day_count=day_count,
            policy="hard",
        )
        if hard_score is not None:
            return _selection_from_scored_combination(
                hard_score,
                severe_warnings,
                requested_policy=requested_policy,
                used_policy="hard",
                fallback_used=False,
                fallback_reason="",
                candidate_days=candidate_days,
                day_count=day_count,
            )
        severe_warnings.append(
            "Nu exista combinatie fezabila pentru no_repeat_policy=hard; se incearca main_only."
        )
        main_only_score = _best_for_policy(
            accept_candidates=accept_candidates,
            accept_or_review_candidates=accept_or_review_candidates,
            candidate_days=candidate_days,
            day_count=day_count,
            policy="main_only",
        )
        if main_only_score is not None:
            return _selection_from_scored_combination(
                main_only_score,
                severe_warnings,
                requested_policy=requested_policy,
                used_policy="main_only",
                fallback_used=True,
                fallback_reason="hard_no_repeat_infeasible",
                candidate_days=candidate_days,
                day_count=day_count,
            )
        severe_warnings.append(
            "Nu exista combinatie fezabila pentru no_repeat_policy=main_only; se incearca prefer."
        )

    elif requested_policy == "main_only":
        main_only_score = _best_for_policy(
            accept_candidates=accept_candidates,
            accept_or_review_candidates=accept_or_review_candidates,
            candidate_days=candidate_days,
            day_count=day_count,
            policy="main_only",
        )
        if main_only_score is not None:
            return _selection_from_scored_combination(
                main_only_score,
                severe_warnings,
                requested_policy=requested_policy,
                used_policy="main_only",
                fallback_used=False,
                fallback_reason="",
                candidate_days=candidate_days,
                day_count=day_count,
            )
        severe_warnings.append(
            "Nu exista combinatie fezabila pentru no_repeat_policy=main_only; se incearca prefer."
        )

    usable_candidates = accept_candidates
    if len(accept_candidates) >= day_count:
        accept_only_score = _best_scored_combination(
            accept_candidates,
            day_count,
            no_repeat_policy="prefer" if requested_policy in {"hard", "main_only"} else requested_policy,
        )
        if accept_only_score is not None:
            _, _, score = accept_only_score
            accept_only_is_diverse = (
                int(score.get("repeated_recipe_count", 0) or 0) == 0
                or (
                    int(score.get("repeated_recipe_count", 0) or 0) <= 1
                    and int(score.get("repeated_main_recipe_count", 0) or 0) == 0
                )
            )
            if accept_only_is_diverse:
                return _selection_from_scored_combination(
                    accept_only_score,
                    severe_warnings,
                    requested_policy=requested_policy,
                    used_policy="prefer" if requested_policy != "none" else "none",
                    fallback_used=requested_policy in {"hard", "main_only"},
                    fallback_reason=(
                        f"{requested_policy}_no_repeat_infeasible"
                        if requested_policy in {"hard", "main_only"}
                        else ""
                    ),
                    candidate_days=candidate_days,
                    day_count=day_count,
                )
        if len(accept_or_review_candidates) >= day_count:
            usable_candidates = accept_or_review_candidates
            severe_warnings.append(
                "Combinatia doar cu zile accept este prea repetitiva; sunt evaluate si zile review."
            )
    elif len(accept_or_review_candidates) >= day_count:
        severe_warnings.append(
            "Nu exista suficiente zile accept; sunt permise zile review."
        )
        usable_candidates = accept_or_review_candidates
    if len(usable_candidates) < day_count:
        usable_candidates = [
            candidate
            for candidate in candidate_days
            if candidate.get("validation_status") == "valid"
        ]
        severe_warnings.append(
            "Nu exista suficiente zile accept/review; sunt permise zile reject valide."
        )
    if len(usable_candidates) < day_count:
        usable_candidates = list(candidate_days)
        severe_warnings.append(
            "Nu exista suficiente zile valide; selectia globala foloseste cele mai bune candidate disponibile."
        )

    best_scored = _best_scored_combination(
        usable_candidates,
        day_count,
        no_repeat_policy="prefer" if requested_policy in {"hard", "main_only"} else requested_policy,
    )
    if best_scored is None:
        best_possible_score = _best_possible_scored_sequence(
            candidate_days=candidate_days,
            day_count=day_count,
            no_repeat_policy="prefer",
        )
        if best_possible_score is None:
            return [], {
                "multi_day_loss": None,
                "quality_warnings": ["Nu exista combinatii multi-day candidate."],
                "no_repeat_policy_requested": requested_policy,
                "no_repeat_policy_used": "none",
                "fallback_from_hard_no_repeat": requested_policy == "hard",
                "fallback_used": True,
                "fallback_reason": "no_candidate_days_available",
                **_combination_search_diagnostics(candidate_days, day_count),
            }
        severe_warnings.append(
            "Nu exista combinatie completa fara reutilizarea zilelor candidate; se returneaza cel mai bun plan posibil."
        )
        return _selection_from_scored_combination(
            best_possible_score,
            severe_warnings,
            requested_policy=requested_policy,
            used_policy="prefer",
            fallback_used=True,
            fallback_reason="best_possible_with_reused_candidates",
            candidate_days=candidate_days,
            day_count=day_count,
        )
    return _selection_from_scored_combination(
        best_scored,
        severe_warnings,
        requested_policy=requested_policy,
        used_policy="prefer" if requested_policy in {"hard", "main_only"} else requested_policy,
        fallback_used=requested_policy in {"hard", "main_only"},
        fallback_reason=(
            f"{requested_policy}_no_repeat_infeasible"
            if requested_policy in {"hard", "main_only"}
            else ""
        ),
        candidate_days=candidate_days,
        day_count=day_count,
    )


def _best_for_policy(
    accept_candidates: Sequence[Mapping[str, Any]],
    accept_or_review_candidates: Sequence[Mapping[str, Any]],
    candidate_days: Sequence[Mapping[str, Any]],
    day_count: int,
    policy: str,
) -> tuple[tuple[float, ...], tuple[Mapping[str, Any], ...], dict[str, Any]] | None:
    for candidates in (accept_candidates, accept_or_review_candidates):
        if len(candidates) < day_count:
            continue
        scored = _best_scored_combination(
            candidates,
            day_count,
            no_repeat_policy=policy,
            require_policy=True,
        )
        if scored is not None:
            return scored
    valid_candidates = [
        candidate
        for candidate in candidate_days
        if candidate.get("validation_status") == "valid"
        and candidate.get("quality_gate_status") != "reject"
    ]
    if len(valid_candidates) < day_count:
        return None
    return _best_scored_combination(
        valid_candidates,
        day_count,
        no_repeat_policy=policy,
        require_policy=True,
    )


def _best_scored_combination(
    usable_candidates: Sequence[Mapping[str, Any]],
    day_count: int,
    no_repeat_policy: str = "prefer",
    require_policy: bool = False,
) -> tuple[tuple[float, ...], tuple[Mapping[str, Any], ...], dict[str, Any]] | None:
    scored: list[tuple[tuple[float, ...], tuple[Mapping[str, Any], ...], dict[str, Any]]] = []
    search_candidates = _combination_search_candidates(usable_candidates, day_count)
    for combination in itertools.combinations(search_candidates, day_count):
        score = _score_day_combination(combination)
        if require_policy and not _score_satisfies_no_repeat_policy(score, no_repeat_policy):
            continue
        key = _scored_combination_key(score, combination, no_repeat_policy)
        scored.append((key, combination, score))

    if not scored:
        return None

    scored.sort(key=lambda item: item[0])
    return scored[0]


def _best_possible_scored_sequence(
    candidate_days: Sequence[Mapping[str, Any]],
    day_count: int,
    no_repeat_policy: str,
) -> tuple[tuple[float, ...], tuple[Mapping[str, Any], ...], dict[str, Any]] | None:
    ranked = _ranked_combination_candidates(candidate_days)
    if not ranked:
        return None
    selected = list(ranked[:day_count])
    while len(selected) < day_count:
        selected.append(ranked[len(selected) % len(ranked)])
    combination = tuple(selected)
    score = _score_day_combination(combination)
    key = _scored_combination_key(score, combination, no_repeat_policy)
    return key, combination, score


def _scored_combination_key(
    score: Mapping[str, Any],
    combination: Sequence[Mapping[str, Any]],
    no_repeat_policy: str,
) -> tuple[Any, ...]:
    return (
        _to_float(score.get("invalid_day_count")),
        _to_float(score.get("reject_day_count")),
        _to_float(score.get("fallback_day_count")),
        _to_float(score.get("review_day_count")),
        _repeat_sort_value(score, no_repeat_policy),
        -_to_float(score.get("accept_day_count")),
        _to_float(score.get("multi_day_loss")),
        -_to_float(score.get("unique_recipe_count")),
        _to_float(score.get("repeated_main_recipe_count")),
        _to_float(score.get("repeated_recipe_count")),
        _to_float(score.get("average_base_day_loss")),
        _combination_recipe_key(combination),
    )


def _combination_search_candidates(
    usable_candidates: Sequence[Mapping[str, Any]],
    day_count: int,
) -> list[Mapping[str, Any]]:
    ranked = _ranked_combination_candidates(usable_candidates)
    limit = _combination_candidate_limit(day_count, len(ranked))
    return ranked[:limit]


def _ranked_combination_candidates(
    candidates: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    return sorted(
        list(candidates),
        key=lambda candidate: (
            0 if candidate.get("validation_status") == "valid" else 1,
            _quality_status_sort_value(candidate.get("quality_gate_status")),
            _to_float(candidate.get("adjusted_day_loss")),
            _to_float(candidate.get("base_day_loss")),
            _to_float(candidate.get("meal_realism_warning_count")),
            ";".join(str(item) for item in candidate.get("recipe_key", [])),
            str(candidate.get("candidate_day_id", "")),
        ),
    )


def _quality_status_sort_value(value: object) -> int:
    status = str(value or "").strip().lower()
    if status == "accept":
        return 0
    if status == "review":
        return 1
    if status == "reject":
        return 2
    return 3


def _combination_candidate_limit(day_count: int, available_count: int) -> int:
    if day_count <= 3:
        return available_count
    if day_count == 4:
        return min(available_count, 48)
    return min(available_count, 36)


def _selection_from_scored_combination(
    scored_combination: tuple[
        tuple[float, ...],
        tuple[Mapping[str, Any], ...],
        dict[str, Any],
    ],
    severe_warnings: Sequence[str],
    requested_policy: str,
    used_policy: str,
    fallback_used: bool,
    fallback_reason: str,
    candidate_days: Sequence[Mapping[str, Any]],
    day_count: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _, best_combination, best_score = scored_combination
    selected = [copy.deepcopy(dict(candidate)) for candidate in best_combination]
    quality_warnings = list(severe_warnings)
    if best_score.get("review_day_count", 0) > 0:
        quality_warnings.append("Planul global contine zile review.")
    if best_score.get("repeated_recipe_count", 0) > 0:
        quality_warnings.append("Planul global contine retete repetate.")
    if best_score.get("reject_day_count", 0) > 0:
        quality_warnings.append("Planul global contine zile reject.")
    if fallback_used:
        quality_warnings.append(
            f"no_repeat_policy={requested_policy} nu a fost fezabil; s-a folosit {used_policy}."
        )
    best_score["quality_warnings"] = quality_warnings
    best_score["no_repeat_policy_requested"] = requested_policy
    best_score["no_repeat_policy_used"] = used_policy
    best_score["fallback_from_hard_no_repeat"] = (
        requested_policy == "hard" and used_policy != "hard"
    )
    best_score["fallback_used"] = bool(fallback_used)
    best_score["fallback_reason"] = str(fallback_reason or "")
    best_score.update(_combination_search_diagnostics(candidate_days, day_count))
    return selected, best_score


def _score_day_combination(
    combination: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    day_count = max(1, len(combination))
    base_losses = [_to_float(candidate.get("base_day_loss")) for candidate in combination]
    average_base_day_loss = sum(base_losses) / day_count
    repetition = _combination_repetition_data(combination)
    review_day_count = sum(
        1 for candidate in combination if candidate.get("quality_gate_status") == "review"
    )
    reject_day_count = sum(
        1 for candidate in combination if candidate.get("quality_gate_status") == "reject"
    )
    accept_day_count = sum(
        1 for candidate in combination if candidate.get("quality_gate_status") == "accept"
    )
    invalid_day_count = sum(
        1 for candidate in combination if candidate.get("validation_status") != "valid"
    )
    fallback_day_count = sum(
        1
        for candidate in combination
        if bool(candidate.get("plan", {}).get("quality_gate_fallback_used", False))
    )
    review_day_penalty = (
        review_day_count
        + 2 * reject_day_count
        + 2 * invalid_day_count
        + fallback_day_count
    ) / day_count
    realism_warning_penalty = min(
        1.0,
        sum(_to_float(candidate.get("meal_realism_warning_count")) for candidate in combination)
        / max(1, day_count * 4),
    )
    multi_day_loss = (
        0.55 * average_base_day_loss
        + 0.20 * repetition["repetition_penalty"]
        + 0.15 * review_day_penalty
        + 0.10 * realism_warning_penalty
    )
    return {
        "multi_day_loss": round(multi_day_loss, 6),
        "average_base_day_loss": round(average_base_day_loss, 6),
        "repetition_penalty": round(repetition["repetition_penalty"], 6),
        "review_day_penalty": round(review_day_penalty, 6),
        "realism_warning_penalty": round(realism_warning_penalty, 6),
        "review_day_count": review_day_count,
        "accept_day_count": accept_day_count,
        "reject_day_count": reject_day_count,
        "invalid_day_count": invalid_day_count,
        "fallback_day_count": fallback_day_count,
        **repetition,
    }


def _combination_repetition_data(
    combination: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    recipe_counts: Counter[str] = Counter()
    slot_recipe_counts: dict[str, Counter[str]] = {}
    for candidate in combination:
        plan = candidate.get("plan", {})
        if not isinstance(plan, Mapping):
            continue
        for meal in plan.get("selected_meals", []):
            if not isinstance(meal, Mapping):
                continue
            recipe_id = str(meal.get("recipe_id", "")).strip()
            slot = str(meal.get("slot", "")).strip().lower()
            if not recipe_id:
                continue
            recipe_counts[recipe_id] += 1
            slot_recipe_counts.setdefault(slot, Counter())[recipe_id] += 1

    repeated_recipe_ids = sorted(
        recipe_id for recipe_id, count in recipe_counts.items() if count > 1
    )
    repeated_occurrences = sum(max(0, count - 1) for count in recipe_counts.values())
    same_slot_penalty = 0.0
    repeated_main_recipe_count = 0
    repeated_breakfast_count = 0
    for slot, counts in slot_recipe_counts.items():
        for count in counts.values():
            repeat_count = max(0, count - 1)
            if repeat_count <= 0:
                continue
            if slot in {"lunch", "dinner"}:
                same_slot_penalty += 0.60 * repeat_count
                repeated_main_recipe_count += repeat_count
            elif slot == "breakfast":
                same_slot_penalty += 0.18 * repeat_count
                repeated_breakfast_count += repeat_count
            elif slot == "snack":
                same_slot_penalty += 0.18 * repeat_count
            else:
                same_slot_penalty += 0.08 * repeat_count
    unique_recipe_count = len(recipe_counts)
    expected_unique = max(1, len(combination) * 4)
    too_few_unique_penalty = max(0, expected_unique - unique_recipe_count) * 0.06
    repetition_penalty = (
        0.10 * repeated_occurrences + same_slot_penalty + too_few_unique_penalty
    )
    return {
        "repetition_penalty": repetition_penalty,
        "repeated_recipe_ids": repeated_recipe_ids,
        "repeated_recipe_count": len(repeated_recipe_ids),
        "repeated_recipe_occurrence_count": repeated_occurrences,
        "repeated_main_recipe_count": repeated_main_recipe_count,
        "repeated_breakfast_count": repeated_breakfast_count,
        "unique_recipe_count": unique_recipe_count,
    }


def _score_satisfies_no_repeat_policy(
    score: Mapping[str, Any],
    no_repeat_policy: str,
) -> bool:
    if no_repeat_policy == "hard":
        return int(score.get("repeated_recipe_count", 0) or 0) == 0
    if no_repeat_policy == "main_only":
        return int(score.get("repeated_main_recipe_count", 0) or 0) == 0
    return True


def _no_repeat_policy(config: Mapping[str, Any]) -> str:
    value = str(config.get("no_repeat_policy", "prefer") or "prefer")
    if value in {"none", "prefer", "hard", "main_only"}:
        return value
    return "prefer"


def _multi_day_speed_mode(config: Mapping[str, Any]) -> str:
    value = str(config.get("multi_day_speed_mode", "quality") or "quality").lower()
    if value in {"fast", "quality"}:
        return value
    return "quality"


def _day_candidate_builder(config: Mapping[str, Any]) -> str:
    value = str(config.get("day_candidate_builder", "auto") or "auto").lower()
    if value == "auto":
        return (
            "direct_from_slots"
            if _multi_day_speed_mode(config) == "fast"
            else "balanced_repeated"
        )
    if value in {"balanced_repeated", "direct_from_slots"}:
        return value
    return "balanced_repeated"


def _repeat_sort_value(
    score: Mapping[str, Any],
    no_repeat_policy: str,
) -> float:
    if no_repeat_policy == "none":
        return 0.0
    if no_repeat_policy == "main_only":
        return (
            10 * _to_float(score.get("repeated_main_recipe_count"))
            + _to_float(score.get("repeated_recipe_count"))
        )
    return (
        10 * _to_float(score.get("repeated_recipe_count"))
        + _to_float(score.get("repeated_main_recipe_count"))
    )


def _combination_recipe_key(
    combination: Sequence[Mapping[str, Any]],
) -> str:
    recipe_ids: list[str] = []
    for candidate in combination:
        recipe_ids.extend(candidate.get("recipe_key", []))
    return ";".join(sorted(str(recipe_id) for recipe_id in recipe_ids))


def _combination_search_diagnostics(
    candidate_days: Sequence[Mapping[str, Any]],
    day_count: int,
) -> dict[str, Any]:
    valid_non_reject = [
        candidate
        for candidate in candidate_days
        if candidate.get("validation_status") == "valid"
        and candidate.get("quality_gate_status") != "reject"
    ]
    available_count = len(valid_non_reject)
    search_candidates = _combination_search_candidates(valid_non_reject, day_count)
    combinations_evaluated = 0
    no_repeat_count = 0
    main_no_repeat_count = 0
    for combination in itertools.combinations(search_candidates, day_count):
        combinations_evaluated += 1
        repetition = _combination_repetition_data(combination)
        if repetition["repeated_recipe_count"] == 0:
            no_repeat_count += 1
        if repetition["repeated_main_recipe_count"] == 0:
            main_no_repeat_count += 1
    return {
        "combinations_evaluated": combinations_evaluated,
        "feasible_no_repeat_combinations": no_repeat_count,
        "feasible_main_no_repeat_combinations": main_no_repeat_count,
        "combination_candidate_count_available": available_count,
        "combination_candidate_count_considered": len(search_candidates),
        "combination_search_truncated": len(search_candidates) < available_count,
    }


def _select_quality_gated_varied_day(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | Mapping[str, Any],
    slot_order: Sequence[str],
    recent_recipe_ids: Sequence[str],
    config: Mapping[str, Any],
    fallback_best_plan: Mapping[str, Any] | None,
) -> dict[str, Any]:
    attempted_modes: list[dict[str, Any]] = []
    for mode in ("avoid_recent", "soft"):
        candidate_plan = _select_and_evaluate(
            slot_candidates_by_slot=slot_candidates_by_slot,
            target=target,
            slot_order=slot_order,
            config=_selector_config(
                config,
                diversity_mode=mode,
                recent_recipe_ids=recent_recipe_ids,
            ),
        )
        attempted_modes.append(
            _attempt_record(
                mode=mode,
                plan=candidate_plan,
                fallback_used=False,
            )
        )
        quality_gate = candidate_plan.get("quality_gate", {})
        if not isinstance(quality_gate, Mapping):
            quality_gate = {}
        if quality_gate.get("quality_gate_status") == "accept":
            _attach_quality_fields(
                plan=candidate_plan,
                selected_mode=mode,
                fallback_used=False,
                recent_recipe_count=len(recent_recipe_ids),
                attempted_modes=attempted_modes,
            )
            return candidate_plan
        if quality_gate.get("quality_gate_status") == "review":
            candidate_plan.setdefault("warnings", []).append(
                "Quality gate marked varied plan for review."
            )
            _attach_quality_fields(
                plan=candidate_plan,
                selected_mode=mode,
                fallback_used=False,
                recent_recipe_count=len(recent_recipe_ids),
                attempted_modes=attempted_modes,
            )
            return candidate_plan

    if fallback_best_plan is not None:
        plan = copy.deepcopy(dict(fallback_best_plan))
    else:
        plan = _select_best_day(
            slot_candidates_by_slot=slot_candidates_by_slot,
            target=target,
            slot_order=slot_order,
            config=config,
        )
    attempted_modes.append(
        _attempt_record(
            mode="fallback_best",
            plan=plan,
            fallback_used=True,
        )
    )
    plan.setdefault("warnings", []).append(
        "No quality-safe varied plan found; returned best plan."
    )
    quality_gate = plan.get("quality_gate", {})
    if isinstance(quality_gate, Mapping) and quality_gate.get("quality_gate_status") == "reject":
        plan.setdefault("warnings", []).append(
            "Fallback best plan also fails the quality gate."
        )
    _attach_quality_fields(
        plan=plan,
        selected_mode="fallback_best",
        fallback_used=True,
        recent_recipe_count=len(recent_recipe_ids),
        attempted_modes=attempted_modes,
    )
    return plan


def _select_best_day(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | Mapping[str, Any],
    slot_order: Sequence[str],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=target,
        slot_order=slot_order,
        config=_selector_config(
            config,
            diversity_mode="none",
            recent_recipe_ids=[],
        ),
    )
    plan["validation"] = validate_one_day_plan(plan, target)
    plan["quality_gate"] = evaluate_plan_quality(
        plan,
        target,
        config={"quality_gate": "demo_safe"},
    )
    _attach_quality_fields(
        plan=plan,
        selected_mode="none",
        fallback_used=False,
        recent_recipe_count=0,
    )
    return plan


def _select_and_evaluate(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | Mapping[str, Any],
    slot_order: Sequence[str],
    config: Mapping[str, Any],
    profile_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=target,
        slot_order=slot_order,
        config=dict(config),
    )
    validation_started = time.perf_counter()
    plan["validation"] = validate_one_day_plan(plan, target)
    if profile_stats is not None:
        profile_stats["validation_seconds"] = round(
            _to_float(profile_stats.get("validation_seconds"))
            + time.perf_counter()
            - validation_started,
            6,
        )
    quality_started = time.perf_counter()
    plan["quality_gate"] = evaluate_plan_quality(
        plan,
        target,
        config={"quality_gate": "demo_safe"},
    )
    if profile_stats is not None:
        profile_stats["quality_gate_seconds"] = round(
            _to_float(profile_stats.get("quality_gate_seconds"))
            + time.perf_counter()
            - quality_started,
            6,
        )
        profile_stats["quality_gate_evaluation_count"] = int(
            profile_stats.get("quality_gate_evaluation_count", 0)
        ) + 1
    return plan


def _day_payload(
    day_index: int,
    plan: Mapping[str, Any],
    previous_recipe_ids: set[str],
    recent_recipe_ids: Sequence[str],
) -> dict[str, Any]:
    selected_ids = _selected_recipe_ids(plan)
    repeated = sorted(recipe_id for recipe_id in selected_ids if recipe_id in previous_recipe_ids)
    quality_gate = plan.get("quality_gate", {})
    if not isinstance(quality_gate, Mapping):
        quality_gate = {}
    validation = plan.get("validation", {})
    if not isinstance(validation, Mapping):
        validation = {}
    diagnostics = plan.get("selector_diagnostics", {})
    if not isinstance(diagnostics, Mapping):
        diagnostics = {}
    warnings = list(plan.get("warnings", []))
    if repeated:
        warnings.append(
            "Retete repetate fata de zilele anterioare: " + ", ".join(repeated)
        )

    return {
        "day_index": day_index,
        "selected_meals": list(plan.get("selected_meals", [])),
        "day_totals": dict(plan.get("day_totals", {})),
        "validation": dict(validation),
        "validation_status": validation.get("validation_status", "not_validated"),
        "selector_diagnostics": dict(diagnostics),
        "selector_mode": plan.get("selector_mode", "balanced_day"),
        "quality_gate": dict(quality_gate),
        "quality_gate_status": plan.get(
            "quality_gate_status",
            quality_gate.get("quality_gate_status", "missing"),
        ),
        "quality_gate_reasons": plan.get(
            "quality_gate_reasons",
            quality_gate.get("quality_gate_reasons", []),
        ),
        "quality_gate_score": plan.get(
            "quality_gate_score",
            quality_gate.get("quality_gate_score"),
        ),
        "fallback_used": bool(plan.get("quality_gate_fallback_used", False)),
        "diversity_mode_used": _diversity_mode_used(plan, diagnostics),
        "recent_recipe_ids_used": list(recent_recipe_ids),
        "recent_recipe_count_used": len(recent_recipe_ids),
        "repeated_recipe_ids_vs_previous_days": repeated,
        "warnings": warnings,
        "alternatives": list(plan.get("alternatives", [])),
        "quality_gate_reroll_diagnostics": plan.get(
            "quality_gate_reroll_diagnostics",
            {},
        ),
    }


def _attach_quality_fields(
    plan: dict[str, Any],
    selected_mode: str,
    fallback_used: bool,
    recent_recipe_count: int,
    attempted_modes: list[dict[str, Any]] | None = None,
) -> None:
    quality_gate = plan.get("quality_gate", {})
    if not isinstance(quality_gate, Mapping):
        quality_gate = {}
    plan["quality_gate_status"] = quality_gate.get("quality_gate_status", "missing")
    plan["quality_gate_reasons"] = quality_gate.get("quality_gate_reasons", [])
    plan["quality_gate_score"] = quality_gate.get("quality_gate_score")
    plan["quality_gate_fallback_used"] = fallback_used
    plan["quality_gate_selected_mode"] = selected_mode
    plan["quality_gate_reroll_diagnostics"] = {
        "attempted_modes": attempted_modes
        or [
            _attempt_record(
                mode=selected_mode,
                plan=plan,
                fallback_used=fallback_used,
            )
        ],
        "selected_mode": selected_mode,
        "quality_gate_status": plan["quality_gate_status"],
        "quality_gate_reasons": plan["quality_gate_reasons"],
        "quality_gate_score": plan["quality_gate_score"],
        "fallback_used": fallback_used,
        "recent_recipe_count_used": recent_recipe_count,
    }


def _attempt_record(
    mode: str,
    plan: Mapping[str, Any],
    fallback_used: bool,
) -> dict[str, Any]:
    quality_gate = plan.get("quality_gate", {})
    if not isinstance(quality_gate, Mapping):
        quality_gate = {}
    diagnostics = plan.get("selector_diagnostics", {})
    if not isinstance(diagnostics, Mapping):
        diagnostics = {}
    return {
        "mode": mode,
        "quality_gate_status": quality_gate.get("quality_gate_status"),
        "quality_gate_reasons": quality_gate.get("quality_gate_reasons", []),
        "base_day_loss": diagnostics.get("base_day_loss"),
        "adjusted_day_loss": diagnostics.get("adjusted_day_loss"),
        "fallback_used": fallback_used,
        "selected_recipe_ids": _selected_recipe_ids(plan),
    }


def _selector_config(
    config: Mapping[str, Any],
    diversity_mode: str,
    recent_recipe_ids: Sequence[str],
) -> dict[str, Any]:
    alternative_count = max(1, min(3, int(config.get("alternative_count", 3) or 3)))
    return {
        "return_alternatives": bool(config.get("return_alternatives", True)),
        "alternative_count": alternative_count,
        "diversity_mode": diversity_mode,
        "recent_recipe_ids": list(recent_recipe_ids),
        "meal_realism_mode": "practical",
        "max_candidates_per_slot": int(config.get("max_candidates_per_slot", 28) or 28),
    }


def _global_selector_config(
    config: Mapping[str, Any],
    diversity_mode: str,
    recent_recipe_ids: Sequence[str],
) -> dict[str, Any]:
    alternative_count = max(
        1,
        min(10, int(config.get("candidate_day_alternative_count", 10) or 10)),
    )
    return {
        "return_alternatives": True,
        "alternative_count": alternative_count,
        "diversity_mode": diversity_mode,
        "recent_recipe_ids": list(recent_recipe_ids),
        "meal_realism_mode": "practical",
        "max_candidates_per_slot": int(
            config.get("global_max_candidates_per_slot", 26) or 26
        ),
        "min_recipe_difference_between_alternatives": 1,
    }


def _candidate_pool_summary(
    candidate_days: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    quality_counts = Counter(
        str(candidate.get("quality_gate_status", "missing"))
        for candidate in candidate_days
    )
    validation_counts = Counter(
        str(candidate.get("validation_status", "not_validated"))
        for candidate in candidate_days
    )
    source_counts = Counter(
        str(candidate.get("source_mode", "missing"))
        for candidate in candidate_days
    )
    return {
        "candidate_day_count": len(candidate_days),
        "accept_candidate_count": quality_counts.get("accept", 0),
        "review_candidate_count": quality_counts.get("review", 0),
        "reject_candidate_count": quality_counts.get("reject", 0),
        "valid_candidate_count": validation_counts.get("valid", 0),
        "invalid_candidate_count": sum(
            count
            for status, count in validation_counts.items()
            if status != "valid"
        ),
        "quality_gate_status_counts": dict(sorted(quality_counts.items())),
        "validation_status_counts": dict(sorted(validation_counts.items())),
        "source_mode_counts": dict(sorted(source_counts.items())),
    }


def _candidate_pool_rows(
    candidate_days: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidate_days:
        plan = candidate.get("plan", {})
        if not isinstance(plan, Mapping):
            plan = {}
        rows.append(
            {
                "candidate_day_id": candidate.get("candidate_day_id"),
                "source_mode": candidate.get("source_mode"),
                "alternative_rank": candidate.get("alternative_rank"),
                "validation_status": candidate.get("validation_status"),
                "quality_gate_status": candidate.get("quality_gate_status"),
                "base_day_loss": candidate.get("base_day_loss"),
                "adjusted_day_loss": candidate.get("adjusted_day_loss"),
                "meal_realism_warning_count": candidate.get(
                    "meal_realism_warning_count"
                ),
                "recipe_ids": ";".join(candidate.get("recipe_key", [])),
                "display_names": ";".join(
                    str(meal.get("display_name", ""))
                    for meal in plan.get("selected_meals", [])
                    if isinstance(meal, Mapping)
                ),
            }
        )
    return rows


def _candidate_pool_direct_diagnostics(
    candidate_days: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    for candidate in candidate_days:
        plan = candidate.get("plan", {})
        if not isinstance(plan, Mapping):
            continue
        diagnostics = plan.get("selector_diagnostics", {})
        if not isinstance(diagnostics, Mapping):
            continue
        if diagnostics.get("day_candidate_builder") != "direct_from_slots":
            continue
        return {
            "direct_candidate_combinations_evaluated": int(
                diagnostics.get("evaluated_combination_count", 0) or 0
            ),
            "direct_slot_shortlist_size": diagnostics.get("direct_slot_shortlist_size"),
        }
    return {
        "direct_candidate_combinations_evaluated": 0,
        "direct_slot_shortlist_size": None,
    }


def _meal_realism_warning_count(selected_meals: Sequence[Mapping[str, Any]]) -> int:
    warning_count = 0
    for meal in selected_meals:
        if not isinstance(meal, Mapping):
            continue
        warning_count += len(
            [
                item
                for item in _reason_items(meal.get("meal_realism_flags"))
                if item and item != "meal_realism_ok"
            ]
        )
    return warning_count


def _resolved_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    resolved = dict(DEFAULT_MULTI_DAY_CONFIG)
    if config:
        resolved.update(dict(config))
    resolved["selection_mode"] = "balanced_day"
    resolved["portion_policy"] = "target_aware"
    resolved["meal_realism_mode"] = "practical"
    resolved["quality_gate"] = "demo_safe"
    resolved["alternative_count"] = max(
        1,
        min(3, int(resolved.get("alternative_count", 3) or 3)),
    )
    resolved["return_alternatives"] = bool(resolved.get("return_alternatives", True))
    resolved["max_candidates_per_slot"] = int(
        resolved.get("max_candidates_per_slot", 28) or 28
    )
    resolved["multi_day_mode"] = str(
        resolved.get("multi_day_mode", MULTI_DAY_MODE_SIMPLE) or MULTI_DAY_MODE_SIMPLE
    )
    resolved["candidate_day_alternative_count"] = max(
        1,
        min(
            10,
            int(resolved.get("candidate_day_alternative_count", 10) or 10),
        ),
    )
    resolved["global_max_candidates_per_slot"] = int(
        resolved.get("global_max_candidates_per_slot", 26) or 26
    )
    resolved["day_candidate_pool_size_target"] = max(
        10,
        int(resolved.get("day_candidate_pool_size_target", 75) or 75),
    )
    resolved["day_candidate_pool_max"] = max(
        resolved["day_candidate_pool_size_target"],
        int(resolved.get("day_candidate_pool_max", 150) or 150),
    )
    resolved["include_slot_forced_variants"] = bool(
        resolved.get("include_slot_forced_variants", True)
    )
    resolved["no_repeat_policy"] = _no_repeat_policy(resolved)
    resolved["multi_day_speed_mode"] = _multi_day_speed_mode(resolved)
    resolved["day_candidate_builder"] = _day_candidate_builder(resolved)
    resolved["direct_slot_shortlist_size"] = max(
        4,
        min(30, int(resolved.get("direct_slot_shortlist_size", 12) or 12)),
    )
    resolved["direct_max_portions_per_recipe"] = max(
        1,
        min(5, int(resolved.get("direct_max_portions_per_recipe", 2) or 2)),
    )
    resolved["direct_candidate_pool_record_limit"] = max(
        0,
        int(resolved.get("direct_candidate_pool_record_limit", 0) or 0),
    )
    if resolved["multi_day_speed_mode"] == "fast":
        resolved["global_max_candidates_per_slot"] = min(
            resolved["global_max_candidates_per_slot"],
            24,
        )
        resolved["early_stop_if_no_repeat_accept_found"] = True
        resolved["max_dynamic_source_specs"] = int(
            resolved.get("max_dynamic_source_specs", 18) or 18
        )
        resolved["dynamic_top_breakfast_limit"] = min(
            int(resolved.get("dynamic_top_breakfast_limit", 5) or 5),
            5,
        )
        resolved["dynamic_top_main_limit"] = min(
            int(resolved.get("dynamic_top_main_limit", 7) or 7),
            7,
        )
        resolved["dynamic_seen_main_limit"] = min(
            int(resolved.get("dynamic_seen_main_limit", 10) or 10),
            10,
        )
    else:
        resolved["early_stop_if_no_repeat_accept_found"] = bool(
            resolved.get("early_stop_if_no_repeat_accept_found", False)
        )
        resolved["max_dynamic_source_specs"] = int(
            resolved.get("max_dynamic_source_specs", 0) or 0
        )
        resolved["dynamic_top_breakfast_limit"] = int(
            resolved.get("dynamic_top_breakfast_limit", 6) or 6
        )
        resolved["dynamic_top_main_limit"] = int(
            resolved.get("dynamic_top_main_limit", 8) or 8
        )
        resolved["dynamic_seen_main_limit"] = int(
            resolved.get("dynamic_seen_main_limit", 12) or 12
        )
    return resolved


def _normalize_day_count(days: int | str | None) -> int:
    try:
        day_count = int(days or 3)
    except (TypeError, ValueError) as exc:
        raise ValueError("days trebuie sa fie un numar intreg intre 1 si 5.") from exc
    if day_count < 1 or day_count > 5:
        raise ValueError("days trebuie sa fie intre 1 si 5 pentru multi-day draft.")
    return day_count


def _resolve_slot_candidates_by_slot(
    slot_candidates: pd.DataFrame | None,
    slot_candidates_by_slot: Mapping[str, pd.DataFrame] | None,
    slot_order: Sequence[str],
) -> dict[str, pd.DataFrame]:
    if slot_candidates_by_slot is not None:
        return {
            str(slot): frame.copy()
            for slot, frame in slot_candidates_by_slot.items()
        }
    if slot_candidates is None:
        raise ValueError("slot_candidates este obligatoriu pentru multi-day selector.")
    return {
        str(slot): slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        for slot in slot_order
    }


def _slot_order(target: NutritionTarget | Mapping[str, Any]) -> list[str]:
    target_data = _target_to_dict(target)
    slot_targets = target_data.get("slot_targets", {})
    preferred_order = ["breakfast", "lunch", "dinner", "snack"]
    known_slots = [slot for slot in preferred_order if slot in slot_targets]
    extra_slots = [slot for slot in slot_targets if slot not in preferred_order]
    return known_slots + extra_slots


def _target_to_dict(target: NutritionTarget | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(target, NutritionTarget):
        return {
            "kcal": target.kcal,
            "protein_g": target.protein_g,
            "carbs_g": target.carbs_g,
            "fat_g": target.fat_g,
            "slot_targets": target.slot_targets,
        }
    return dict(target)


def _selected_recipe_ids(plan: Mapping[str, Any]) -> list[str]:
    return [
        str(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if isinstance(meal, Mapping) and meal.get("recipe_id")
    ]


def _selected_recipe_ids_by_slot(plan: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for meal in plan.get("selected_meals", []):
        if not isinstance(meal, Mapping):
            continue
        slot = str(meal.get("slot", "")).strip().lower()
        recipe_id = str(meal.get("recipe_id", "")).strip()
        if slot and recipe_id:
            result[slot] = recipe_id
    return result


def _diversity_mode_used(
    plan: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> str:
    value = plan.get("quality_gate_selected_mode")
    if value:
        return str(value)
    value = diagnostics.get("diversity_mode")
    if value:
        return str(value)
    return "none"


def _reason_items(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    for separator in ("|", ";", ","):
        if separator in text:
            return [item.strip() for item in text.split(separator) if item.strip()]
    return [text]


def _to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _profile_stats(config: Mapping[str, Any]) -> dict[str, Any] | None:
    value = config.get("_profile_stats")
    if isinstance(value, dict):
        return value
    return None
