from __future__ import annotations

import itertools
import math
from collections.abc import Mapping, Sequence

import pandas as pd

from src.generator_v1.day_selector import SELECTED_MEAL_FIELDS
from src.generator_v1.target_builder import NutritionTarget


BALANCED_DAY_MODE = "balanced_day"

DEFAULT_BALANCED_CONFIG = {
    "top_n_score": 20,
    "top_n_macro": 15,
    "top_n_carbs": 15,
    "top_n_kcal": 15,
    "top_n_protein": 10,
    "max_candidates_per_slot": 40,
    "max_combinations_soft_limit": 1_500_000,
    "max_effective_time_min": 300.0,
    "return_alternatives": False,
    "alternative_count": 3,
    "diversity_mode": "none",
    "recent_recipe_ids": None,
    "recent_recipe_penalty": None,
    "same_family_penalty": 0.04,
    "meal_realism_mode": "off",
    "min_recipe_difference_between_alternatives": 1,
}


def select_one_day_plan_balanced(
    slot_candidates_by_slot: Mapping[str, pd.DataFrame],
    target: NutritionTarget | dict[str, object],
    slot_order: Sequence[str],
    config: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_config = _resolved_config(config)
    warnings: list[str] = []
    candidate_count_before: dict[str, int] = {}
    candidate_count_after: dict[str, int] = {}
    candidate_count_before_realism_filter: dict[str, int] = {}
    candidate_count_after_realism_filter: dict[str, int] = {}
    hard_rejected_count_by_slot: dict[str, int] = {}
    hard_reject_candidate_count_by_slot: dict[str, int] = {}
    hard_reject_reasons: dict[str, list[dict[str, object]]] = {}
    shortlists: dict[str, pd.DataFrame] = {}
    meal_realism_mode = _meal_realism_mode(resolved_config)

    for slot in slot_order:
        candidates = slot_candidates_by_slot.get(slot)
        if candidates is None or candidates.empty:
            candidate_count_before[str(slot)] = 0
            candidate_count_after[str(slot)] = 0
            candidate_count_before_realism_filter[str(slot)] = 0
            candidate_count_after_realism_filter[str(slot)] = 0
            hard_rejected_count_by_slot[str(slot)] = 0
            hard_reject_candidate_count_by_slot[str(slot)] = 0
            hard_reject_reasons[str(slot)] = []
            warnings.append(f"Nu exista candidati pentru slot: {slot}")
            continue

        shortlist = _shortlist_for_slot(candidates, resolved_config)
        candidate_count_before[str(slot)] = int(len(candidates))
        candidate_count_before_realism_filter[str(slot)] = int(len(shortlist))
        if meal_realism_mode == "practical":
            shortlist, realism_filter = _apply_practical_realism_filter(
                shortlist=shortlist,
                slot=str(slot),
                warnings=warnings,
            )
            hard_rejected_count_by_slot[str(slot)] = int(
                realism_filter["hard_rejected_count"]
            )
            hard_reject_candidate_count_by_slot[str(slot)] = int(
                realism_filter["hard_reject_candidate_count"]
            )
            hard_reject_reasons[str(slot)] = realism_filter["hard_reject_reasons"]
        else:
            hard_rejected_count_by_slot[str(slot)] = 0
            hard_reject_candidate_count_by_slot[str(slot)] = 0
            hard_reject_reasons[str(slot)] = []
        candidate_count_after_realism_filter[str(slot)] = int(len(shortlist))
        candidate_count_after[str(slot)] = int(len(shortlist))
        shortlists[str(slot)] = shortlist

    missing_slots = [str(slot) for slot in slot_order if str(slot) not in shortlists]
    if missing_slots:
        return _empty_plan(
            warnings=warnings,
            target=target,
            candidate_count_before=candidate_count_before,
            candidate_count_after=candidate_count_after,
            candidate_count_before_realism_filter=candidate_count_before_realism_filter,
            candidate_count_after_realism_filter=candidate_count_after_realism_filter,
            hard_rejected_count_by_slot=hard_rejected_count_by_slot,
            hard_reject_candidate_count_by_slot=hard_reject_candidate_count_by_slot,
            hard_reject_reasons=hard_reject_reasons,
            missing_slots=missing_slots,
        )

    possible_before_limit = _combination_count(shortlists)
    shortlists, reduction_warning = _apply_combination_limit(shortlists, resolved_config, slot_order)
    if reduction_warning:
        warnings.append(reduction_warning)
        candidate_count_after = {
            str(slot): int(len(shortlists.get(str(slot), pd.DataFrame())))
            for slot in slot_order
        }
    possible_after_limit = _combination_count(shortlists)

    alternative_count = _alternative_count(resolved_config)
    return_alternatives = _to_bool(resolved_config.get("return_alternatives"))
    record_buffer_limit = max(alternative_count * 25, 100)
    top_records: list[dict[str, object]] = []
    top_record_indexes: dict[tuple[str, ...], int] = {}
    evaluated_count = 0
    rejected_repeated_count = 0

    target_data = _target_to_dict(target)
    shortlist_rows = [
        [row.to_dict() for _, row in shortlists[str(slot)].iterrows()]
        for slot in slot_order
    ]

    for combination in itertools.product(*shortlist_rows):
        recipe_ids = [_clean_text(row.get("recipe_id")) for row in combination]
        if len(set(recipe_ids)) != len(recipe_ids) or any(not item for item in recipe_ids):
            rejected_repeated_count += 1
            continue

        loss = _compute_day_loss_for_numeric_rows(combination, target_data, resolved_config)
        diversity_penalties = _diversity_penalties_for_numeric_rows(
            combination,
            resolved_config,
        )
        meal_realism_penalties = _meal_realism_penalties_for_numeric_rows(
            combination,
            resolved_config,
        )
        evaluated_count += 1
        average_score = _to_float(loss.get("average_score_preview"))
        effective_time = _to_float(loss.get("effective_time_min_sum"))
        portion_sum = sum(_to_float(row.get("portion_multiplier")) for row in combination)
        adjusted_day_loss = (
            _to_float(loss.get("day_loss"))
            + _to_float(diversity_penalties.get("total_diversity_penalty"))
            + _to_float(meal_realism_penalties.get("meal_realism_applied_penalty"))
        )
        key = (
            adjusted_day_loss,
            _to_float(loss.get("day_loss")),
            _to_float(loss.get("macro_day_loss")),
            -average_score,
            effective_time,
            tuple(recipe_ids),
            portion_sum,
        )
        record = {
            "key": key,
            "rows": combination,
            "loss": loss,
            "diversity_penalties": diversity_penalties,
            "meal_realism_penalties": meal_realism_penalties,
            "recipe_ids": tuple(recipe_ids),
            "recipe_set_key": tuple(sorted(recipe_ids)),
            "adjusted_day_loss": adjusted_day_loss,
        }
        _insert_top_record(
            records=top_records,
            indexes=top_record_indexes,
            record=record,
            limit=record_buffer_limit,
        )

    selected_records = _select_diverse_records(
        records=top_records,
        alternative_count=alternative_count if return_alternatives else 1,
        min_recipe_difference=int(
            resolved_config["min_recipe_difference_between_alternatives"]
        ),
    )
    if not selected_records:
        warnings.append("Nu exista combinatie valida fara retete repetate.")
        return _empty_plan(
            warnings=warnings,
            target=target,
            candidate_count_before=candidate_count_before,
            candidate_count_after=candidate_count_after,
            candidate_count_before_realism_filter=candidate_count_before_realism_filter,
            candidate_count_after_realism_filter=candidate_count_after_realism_filter,
            hard_rejected_count_by_slot=hard_rejected_count_by_slot,
            hard_reject_candidate_count_by_slot=hard_reject_candidate_count_by_slot,
            hard_reject_reasons=hard_reject_reasons,
            missing_slots=[],
            possible_before_limit=possible_before_limit,
            possible_after_limit=possible_after_limit,
            rejected_repeated_count=rejected_repeated_count,
        )

    best_record = selected_records[0]
    best_meals = [_selected_meal_row(row) for row in best_record["rows"]]
    selected_recipe_ids = [_clean_text(meal.get("recipe_id")) for meal in best_meals]
    alternatives = [
        _alternative_payload(
            record=record,
            rank=index + 1,
            warnings=warnings,
        )
        for index, record in enumerate(selected_records)
    ]
    selected_alternative = alternatives[0]
    diagnostics = {
        "selector_mode": BALANCED_DAY_MODE,
        "candidate_count_per_slot_before_shortlist": candidate_count_before,
        "candidate_count_per_slot_after_shortlist": candidate_count_after,
        "candidate_count_per_slot_before_realism_filter": candidate_count_before_realism_filter,
        "candidate_count_per_slot_after_realism_filter": candidate_count_after_realism_filter,
        "hard_rejected_count_by_slot": hard_rejected_count_by_slot,
        "hard_reject_candidate_count_by_slot": hard_reject_candidate_count_by_slot,
        "hard_reject_reasons": hard_reject_reasons,
        "possible_combination_count_before_limit": possible_before_limit,
        "possible_combination_count_after_shortlist": possible_after_limit,
        "evaluated_combination_count": evaluated_count,
        "rejected_repeated_recipe_combination_count": rejected_repeated_count,
        "selected_recipe_ids": selected_recipe_ids,
        "average_score_preview": round(_average_score(best_meals), 4),
        "selector_warnings": warnings.copy(),
        "diversity_mode": _diversity_mode(resolved_config),
        "recent_recipe_ids_considered": sorted(_recent_recipe_ids(resolved_config)),
        "meal_realism_mode": meal_realism_mode,
        "meal_realism_total_penalty": selected_alternative[
            "meal_realism_total_penalty"
        ],
        "realism_penalty_total": selected_alternative[
            "meal_realism_total_penalty"
        ],
        "meal_realism_applied_penalty": selected_alternative[
            "meal_realism_applied_penalty"
        ],
        "meal_realism_flags_by_meal": _meal_realism_flags_by_meal(best_meals),
        "alternative_count_requested": alternative_count if return_alternatives else 1,
        "alternative_count_returned": len(alternatives),
        "base_day_loss": selected_alternative["base_day_loss"],
        "adjusted_day_loss": selected_alternative["adjusted_day_loss"],
        "diversity_penalties": selected_alternative["diversity_penalties"],
        "meal_realism_penalties": selected_alternative["meal_realism_penalties"],
        **_rounded_loss(best_record["loss"]),
    }
    diagnostics["day_loss"] = selected_alternative["adjusted_day_loss"]
    plan = {
        "selected_meals": best_meals,
        "day_totals": _day_totals(best_meals),
        "warnings": warnings,
        "selector_mode": BALANCED_DAY_MODE,
        "selector_diagnostics": diagnostics,
    }
    if return_alternatives:
        plan["alternatives"] = alternatives
    return plan


def compute_day_loss_for_plan(
    selected_meals: Sequence[Mapping[str, object]],
    target: NutritionTarget | dict[str, object],
    config: dict[str, object] | None = None,
) -> dict[str, float]:
    resolved_config = _resolved_config(config)
    target_data = _target_to_dict(target)
    total_kcal = sum(_to_float(meal.get("kcal")) for meal in selected_meals)
    total_protein = sum(_to_float(meal.get("protein_g")) for meal in selected_meals)
    total_carbs = sum(_to_float(meal.get("carbs_g")) for meal in selected_meals)
    total_fat = sum(_to_float(meal.get("fat_g")) for meal in selected_meals)
    effective_time = sum(
        _to_float(meal.get("effective_time_min_for_scoring"))
        for meal in selected_meals
    )
    target_kcal = _to_float(target_data.get("kcal"))
    target_protein = _to_float(target_data.get("protein_g"))
    target_carbs = _to_float(target_data.get("carbs_g"))
    target_fat = _to_float(target_data.get("fat_g"))

    kcal_loss = _absolute_ratio_loss(total_kcal, target_kcal)
    protein_loss = _protein_loss(total_protein, target_protein)
    carbs_loss = _absolute_ratio_loss(total_carbs, target_carbs)
    fat_loss = _absolute_ratio_loss(total_fat, target_fat)
    macro_day_loss = (
        0.35 * kcal_loss
        + 0.30 * protein_loss
        + 0.25 * carbs_loss
        + 0.10 * fat_loss
    )
    time_penalty = _time_penalty(effective_time, resolved_config)
    slot_suspicious_penalty = 0.05 * sum(
        1 for meal in selected_meals if _to_bool(meal.get("is_slot_suspicious"))
    )
    long_passive_penalty = 0.01 * min(
        2,
        sum(1 for meal in selected_meals if _to_bool(meal.get("has_long_passive_time"))),
    )
    average_score = _average_score(selected_meals)
    score_penalty = max(0.0, 0.70 - average_score) * 0.03
    day_loss = (
        macro_day_loss
        + time_penalty
        + slot_suspicious_penalty
        + long_passive_penalty
        + score_penalty
    )
    return {
        "day_loss": day_loss,
        "macro_day_loss": macro_day_loss,
        "kcal_loss": kcal_loss,
        "protein_loss": protein_loss,
        "carbs_loss": carbs_loss,
        "fat_loss": fat_loss,
        "time_penalty": time_penalty,
        "slot_suspicious_penalty": slot_suspicious_penalty,
        "long_passive_penalty": long_passive_penalty,
        "score_penalty": score_penalty,
        "average_score_preview": average_score,
        "effective_time_min_sum": effective_time,
        "total_kcal": total_kcal,
        "total_protein_g": total_protein,
        "total_carbs_g": total_carbs,
        "total_fat_g": total_fat,
    }


def _compute_day_loss_for_numeric_rows(
    rows: Sequence[Mapping[str, object]],
    target_data: dict[str, object],
    config: dict[str, object],
) -> dict[str, float]:
    total_kcal = sum(float(row.get("_balanced_kcal") or 0.0) for row in rows)
    total_protein = sum(float(row.get("_balanced_protein_g") or 0.0) for row in rows)
    total_carbs = sum(float(row.get("_balanced_carbs_g") or 0.0) for row in rows)
    total_fat = sum(float(row.get("_balanced_fat_g") or 0.0) for row in rows)
    effective_time = sum(float(row.get("_balanced_effective_time_min") or 0.0) for row in rows)
    average_score = (
        sum(float(row.get("_balanced_score_preview") or 0.0) for row in rows) / len(rows)
        if rows
        else 0.0
    )
    kcal_loss = _absolute_ratio_loss(total_kcal, _to_float(target_data.get("kcal")))
    protein_loss = _protein_loss(total_protein, _to_float(target_data.get("protein_g")))
    carbs_loss = _absolute_ratio_loss(total_carbs, _to_float(target_data.get("carbs_g")))
    fat_loss = _absolute_ratio_loss(total_fat, _to_float(target_data.get("fat_g")))
    macro_day_loss = (
        0.35 * kcal_loss
        + 0.30 * protein_loss
        + 0.25 * carbs_loss
        + 0.10 * fat_loss
    )
    time_penalty = _time_penalty(effective_time, config)
    slot_suspicious_penalty = 0.05 * sum(
        1 for row in rows if bool(row.get("_balanced_is_slot_suspicious"))
    )
    long_passive_penalty = 0.01 * min(
        2,
        sum(1 for row in rows if bool(row.get("_balanced_has_long_passive_time"))),
    )
    score_penalty = max(0.0, 0.70 - average_score) * 0.03
    day_loss = (
        macro_day_loss
        + time_penalty
        + slot_suspicious_penalty
        + long_passive_penalty
        + score_penalty
    )
    return {
        "day_loss": day_loss,
        "macro_day_loss": macro_day_loss,
        "kcal_loss": kcal_loss,
        "protein_loss": protein_loss,
        "carbs_loss": carbs_loss,
        "fat_loss": fat_loss,
        "time_penalty": time_penalty,
        "slot_suspicious_penalty": slot_suspicious_penalty,
        "long_passive_penalty": long_passive_penalty,
        "score_penalty": score_penalty,
        "average_score_preview": average_score,
        "effective_time_min_sum": effective_time,
        "total_kcal": total_kcal,
        "total_protein_g": total_protein,
        "total_carbs_g": total_carbs,
        "total_fat_g": total_fat,
    }


def _diversity_penalties_for_numeric_rows(
    rows: Sequence[Mapping[str, object]],
    config: dict[str, object],
) -> dict[str, object]:
    mode = _diversity_mode(config)
    if mode == "none":
        return {
            "diversity_mode": mode,
            "recent_recipe_count": 0,
            "recent_recipe_ids_selected": [],
            "recent_recipe_penalty": 0.0,
            "same_family_duplicate_count": 0,
            "same_family_penalty": 0.0,
            "total_diversity_penalty": 0.0,
        }

    selected_ids = [_clean_text(row.get("recipe_id")) for row in rows]
    recent_ids = _recent_recipe_ids(config)
    repeated_recent = sorted(recipe_id for recipe_id in selected_ids if recipe_id in recent_ids)
    recent_penalty = len(repeated_recent) * _to_float(config.get("recent_recipe_penalty"))

    family_counts: dict[str, int] = {}
    for row in rows:
        family = _clean_text(row.get("recipe_family_name"))
        if not family:
            continue
        family_counts[family] = family_counts.get(family, 0) + 1
    family_duplicates = sum(max(0, count - 1) for count in family_counts.values())
    family_penalty = family_duplicates * _to_float(config.get("same_family_penalty"))
    total_penalty = recent_penalty + family_penalty
    return {
        "diversity_mode": mode,
        "recent_recipe_count": len(repeated_recent),
        "recent_recipe_ids_selected": repeated_recent,
        "recent_recipe_penalty": round(recent_penalty, 6),
        "same_family_duplicate_count": family_duplicates,
        "same_family_penalty": round(family_penalty, 6),
        "total_diversity_penalty": round(total_penalty, 6),
    }


def _meal_realism_penalties_for_numeric_rows(
    rows: Sequence[Mapping[str, object]],
    config: dict[str, object],
) -> dict[str, object]:
    mode = _meal_realism_mode(config)
    raw_penalties = [
        _to_float(row.get("_balanced_meal_realism_penalty"))
        for row in rows
    ]
    total_penalty = (
        sum(raw_penalties) / len(raw_penalties)
        if raw_penalties
        else 0.0
    )
    applied_penalty = total_penalty if mode in {"soft", "practical"} else 0.0
    flags_by_meal = []
    for row in rows:
        flags = _reason_list(row.get("meal_realism_flags"))
        if not flags:
            continue
        flags_by_meal.append(
            {
                "slot": _clean_text(row.get("slot")),
                "recipe_id": _clean_text(row.get("recipe_id")),
                "flags": flags,
            }
        )
    return {
        "meal_realism_mode": mode,
        "meal_realism_total_penalty": round(total_penalty, 6),
        "meal_realism_applied_penalty": round(applied_penalty, 6),
        "meal_realism_flags_by_meal": flags_by_meal,
    }


def _empty_meal_realism_penalties() -> dict[str, object]:
    return {
        "meal_realism_mode": "off",
        "meal_realism_total_penalty": 0.0,
        "meal_realism_applied_penalty": 0.0,
        "meal_realism_flags_by_meal": [],
    }


def _insert_top_record(
    records: list[dict[str, object]],
    indexes: dict[tuple[str, ...], int],
    record: dict[str, object],
    limit: int,
) -> None:
    recipe_set_key = record["recipe_set_key"]
    if not isinstance(recipe_set_key, tuple):
        return
    existing_index = indexes.get(recipe_set_key)
    if existing_index is not None:
        existing = records[existing_index]
        if record["key"] >= existing["key"]:
            return
        records[existing_index] = record
        _sort_and_trim_records(records, indexes, limit)
        return

    if len(records) >= limit and records and record["key"] >= records[-1]["key"]:
        return
    records.append(record)
    _sort_and_trim_records(records, indexes, limit)


def _sort_and_trim_records(
    records: list[dict[str, object]],
    indexes: dict[tuple[str, ...], int],
    limit: int,
) -> None:
    records.sort(key=lambda record: record["key"])
    del records[limit:]
    indexes.clear()
    for index, record in enumerate(records):
        recipe_set_key = record.get("recipe_set_key")
        if isinstance(recipe_set_key, tuple):
            indexes[recipe_set_key] = index


def _select_diverse_records(
    records: list[dict[str, object]],
    alternative_count: int,
    min_recipe_difference: int,
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    min_difference = max(0, min_recipe_difference)
    for record in records:
        if all(
            _recipe_difference(record, existing) >= min_difference
            for existing in selected
        ):
            selected.append(record)
        if len(selected) >= alternative_count:
            return selected

    selected_keys = {record.get("recipe_set_key") for record in selected}
    for record in records:
        if record.get("recipe_set_key") in selected_keys:
            continue
        selected.append(record)
        selected_keys.add(record.get("recipe_set_key"))
        if len(selected) >= alternative_count:
            break
    return selected


def _recipe_difference(left: Mapping[str, object], right: Mapping[str, object]) -> int:
    left_ids = set(left.get("recipe_ids", ()))
    right_ids = set(right.get("recipe_ids", ()))
    return len(left_ids.symmetric_difference(right_ids))


def _alternative_payload(
    record: Mapping[str, object],
    rank: int,
    warnings: list[str],
) -> dict[str, object]:
    meals = [_selected_meal_row(row) for row in record.get("rows", ())]
    loss = record.get("loss", {})
    if not isinstance(loss, dict):
        loss = {}
    diversity_penalties = record.get("diversity_penalties", {})
    if not isinstance(diversity_penalties, dict):
        diversity_penalties = {}
    meal_realism_penalties = record.get("meal_realism_penalties", {})
    if not isinstance(meal_realism_penalties, dict):
        meal_realism_penalties = _empty_meal_realism_penalties()
    selected_recipe_ids = [_clean_text(meal.get("recipe_id")) for meal in meals]
    return {
        "alternative_rank": rank,
        "selected_meals": meals,
        "day_totals": _day_totals(meals),
        "base_day_loss": round(_to_float(loss.get("day_loss")), 6),
        "adjusted_day_loss": round(_to_float(record.get("adjusted_day_loss")), 6),
        "kcal_loss": round(_to_float(loss.get("kcal_loss")), 6),
        "protein_loss": round(_to_float(loss.get("protein_loss")), 6),
        "carbs_loss": round(_to_float(loss.get("carbs_loss")), 6),
        "fat_loss": round(_to_float(loss.get("fat_loss")), 6),
        "average_score_preview": round(_average_score(meals), 6),
        "selected_recipe_ids": selected_recipe_ids,
        "diversity_penalties": diversity_penalties,
        "meal_realism_penalties": meal_realism_penalties,
        "selector_warnings": warnings.copy(),
        "meal_realism_total_penalty": meal_realism_penalties[
            "meal_realism_total_penalty"
        ],
        "meal_realism_applied_penalty": meal_realism_penalties[
            "meal_realism_applied_penalty"
        ],
    }


def _shortlist_for_slot(
    candidates: pd.DataFrame,
    config: dict[str, object],
) -> pd.DataFrame:
    pieces = [
        _top_candidates(candidates, "score_preview", int(config["top_n_score"])),
        _top_candidates(candidates, "macro_fit", int(config["top_n_macro"])),
        _top_candidates(candidates, "carbs_g", int(config["top_n_carbs"])),
        _top_candidates(candidates, "kcal", int(config["top_n_kcal"])),
        _top_candidates(candidates, "protein_g", int(config["top_n_protein"])),
    ]
    combined = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    if combined.empty:
        return combined
    deduped = _dedupe_candidates(combined)
    sorted_rows = _sort_shortlist(deduped)
    shortlisted = sorted_rows.head(int(config["max_candidates_per_slot"])).reset_index(drop=True)
    return _with_numeric_helpers(shortlisted)


def _apply_practical_realism_filter(
    shortlist: pd.DataFrame,
    slot: str,
    warnings: list[str],
) -> tuple[pd.DataFrame, dict[str, object]]:
    prepared = _copy_practical_realism_fields(shortlist)
    if prepared.empty:
        return prepared, {
            "hard_rejected_count": 0,
            "hard_reject_candidate_count": 0,
            "hard_reject_reasons": [],
        }

    hard_mask = prepared["realism_hard_reject"].map(_to_bool)
    hard_rows = prepared.loc[hard_mask].copy()
    hard_reasons = _hard_reject_reason_rows(hard_rows, slot)
    if hard_rows.empty:
        return _with_numeric_helpers(prepared), {
            "hard_rejected_count": 0,
            "hard_reject_candidate_count": 0,
            "hard_reject_reasons": [],
        }

    kept = prepared.loc[~hard_mask].copy()
    if kept.empty:
        warnings.append(
            "Practical realism fallback: toate candidatele din shortlist au hard reject "
            f"pentru slot {slot}; se pastreaza cu penalty mare."
        )
        return _with_numeric_helpers(prepared), {
            "hard_rejected_count": 0,
            "hard_reject_candidate_count": int(len(hard_rows)),
            "hard_reject_reasons": hard_reasons,
        }

    return _with_numeric_helpers(kept.reset_index(drop=True)), {
        "hard_rejected_count": int(len(hard_rows)),
        "hard_reject_candidate_count": int(len(hard_rows)),
        "hard_reject_reasons": hard_reasons,
    }


def _copy_practical_realism_fields(candidates: pd.DataFrame) -> pd.DataFrame:
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
    if "realism_hard_reject" not in prepared.columns:
        prepared["realism_hard_reject"] = False
    if "realism_reject_reason" not in prepared.columns:
        prepared["realism_reject_reason"] = [[] for _ in range(len(prepared))]
    return prepared


def _hard_reject_reason_rows(
    rows: pd.DataFrame,
    slot: str,
) -> list[dict[str, object]]:
    result = []
    for _, row in rows.iterrows():
        result.append(
            {
                "slot": slot,
                "recipe_id": _clean_text(row.get("recipe_id")),
                "display_name": _clean_text(row.get("display_name")),
                "portion_multiplier": round(_to_float(row.get("portion_multiplier")), 4),
                "portion_grams_estimated": round(
                    _to_float(row.get("portion_grams_estimated")),
                    4,
                ),
                "kcal": round(_to_float(row.get("kcal")), 4),
                "realism_reject_reason": _reason_list(
                    row.get("realism_reject_reason")
                ),
                "meal_realism_flags": _reason_list(row.get("meal_realism_flags")),
            }
        )
    return result


def _with_numeric_helpers(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    prepared = candidates.copy()
    prepared["_balanced_kcal"] = prepared["kcal"].map(_to_float)
    prepared["_balanced_protein_g"] = prepared["protein_g"].map(_to_float)
    prepared["_balanced_carbs_g"] = prepared["carbs_g"].map(_to_float)
    prepared["_balanced_fat_g"] = prepared["fat_g"].map(_to_float)
    prepared["_balanced_effective_time_min"] = prepared["effective_time_min_for_scoring"].map(_to_float)
    prepared["_balanced_score_preview"] = prepared["score_preview"].map(_to_float)
    prepared["_balanced_is_slot_suspicious"] = prepared["is_slot_suspicious"].map(_to_bool)
    prepared["_balanced_has_long_passive_time"] = prepared["has_long_passive_time"].map(_to_bool)
    if "meal_realism_penalty" in prepared.columns:
        prepared["_balanced_meal_realism_penalty"] = prepared["meal_realism_penalty"].map(_to_float)
    else:
        prepared["_balanced_meal_realism_penalty"] = 0.0
    return prepared


def _top_candidates(candidates: pd.DataFrame, column: str, count: int) -> pd.DataFrame:
    if count <= 0 or candidates.empty or column not in candidates.columns:
        return pd.DataFrame(columns=candidates.columns)
    sort_columns = _sort_columns(column)
    ascending = _sort_ascending(column)
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


def _sort_columns(primary: str) -> list[str]:
    columns = []
    if primary != "is_slot_suspicious":
        columns.append("is_slot_suspicious")
    for column in [primary, "score_preview", "macro_fit", "recipe_id", "portion_multiplier"]:
        if column not in columns:
            columns.append(column)
    return columns


def _sort_ascending(primary: str) -> list[bool]:
    return [
        column in {"is_slot_suspicious", "recipe_id", "portion_multiplier"}
        for column in _sort_columns(primary)
    ]


def _dedupe_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    seen: set[tuple[str, str, float]] = set()
    rows: list[dict[str, object]] = []
    for _, row in candidates.iterrows():
        key = (
            _clean_text(row.get("slot")),
            _clean_text(row.get("recipe_id")),
            round(_to_float(row.get("portion_multiplier")), 4),
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(row.to_dict())
    return pd.DataFrame(rows, columns=candidates.columns)


def _sort_shortlist(candidates: pd.DataFrame) -> pd.DataFrame:
    sort_columns = [
        "is_slot_suspicious",
        "score_preview",
        "macro_fit",
        "carbs_g",
        "kcal",
        "recipe_id",
        "portion_multiplier",
    ]
    return candidates.sort_values(
        sort_columns,
        ascending=[True, False, False, False, False, True, True],
        kind="mergesort",
        na_position="last",
    )


def _apply_combination_limit(
    shortlists: dict[str, pd.DataFrame],
    config: dict[str, object],
    slot_order: Sequence[str],
) -> tuple[dict[str, pd.DataFrame], str | None]:
    limit = int(config["max_combinations_soft_limit"])
    current_count = _combination_count(shortlists)
    if current_count <= limit:
        return shortlists, None

    reduced = {slot: frame.copy() for slot, frame in shortlists.items()}
    while _combination_count(reduced) > limit and any(len(frame) > 1 for frame in reduced.values()):
        slot_to_reduce = max(
            (str(slot) for slot in slot_order),
            key=lambda slot: (len(reduced.get(slot, pd.DataFrame())), slot),
        )
        frame = reduced[slot_to_reduce]
        reduced[slot_to_reduce] = frame.iloc[:-1].reset_index(drop=True)

    warning = (
        "Shortlist redusa pentru limita combinatii: "
        f"{current_count} -> {_combination_count(reduced)}"
    )
    return reduced, warning


def _combination_count(shortlists: Mapping[str, pd.DataFrame]) -> int:
    total = 1
    for frame in shortlists.values():
        total *= max(0, len(frame))
    return int(total)


def _selected_meal_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        field: _clean_value(row.get(field))
        for field in SELECTED_MEAL_FIELDS
    }


def _day_totals(selected_meals: list[dict[str, object]]) -> dict[str, object]:
    return {
        "total_kcal": round(sum(_to_float(meal.get("kcal")) for meal in selected_meals), 1),
        "total_protein_g": round(sum(_to_float(meal.get("protein_g")) for meal in selected_meals), 1),
        "total_carbs_g": round(sum(_to_float(meal.get("carbs_g")) for meal in selected_meals), 1),
        "total_fat_g": round(sum(_to_float(meal.get("fat_g")) for meal in selected_meals), 1),
        "original_total_kcal": _original_macro_total(
            selected_meals,
            "original_energy_kcal_per_serving",
        ),
        "original_total_protein_g": _original_macro_total(
            selected_meals,
            "original_protein_g_per_serving",
        ),
        "original_total_carbs_g": _original_macro_total(
            selected_meals,
            "original_carbs_g_per_serving",
        ),
        "original_total_fat_g": _original_macro_total(
            selected_meals,
            "original_fat_g_per_serving",
        ),
        "uses_pilot_nutrition_overlay_count": sum(
            1
            for meal in selected_meals
            if _to_bool(meal.get("uses_pilot_nutrition_overlay"))
        ),
        "total_time_min_sum": round(
            sum(_to_float(meal.get("total_time_min")) for meal in selected_meals),
            1,
        ),
        "effective_time_min_sum": _sum_optional(
            selected_meals,
            "effective_time_min_for_scoring",
        ),
        "passive_time_estimated_sum": _sum_optional(
            selected_meals,
            "passive_time_estimated_min",
        ),
        "selected_slot_count": len(selected_meals),
    }


def _original_macro_total(
    selected_meals: list[dict[str, object]],
    field: str,
) -> float:
    total = 0.0
    for meal in selected_meals:
        total += _to_float(meal.get(field)) * _to_float(meal.get("portion_multiplier"))
    return round(total, 1)


def _sum_optional(selected_meals: list[dict[str, object]], field: str) -> float | None:
    values = [_to_optional_float(meal.get(field)) for meal in selected_meals]
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return None
    return round(sum(numeric_values), 1)


def _empty_plan(
    warnings: list[str],
    target: NutritionTarget | dict[str, object],
    candidate_count_before: dict[str, int],
    candidate_count_after: dict[str, int],
    missing_slots: list[str],
    candidate_count_before_realism_filter: dict[str, int] | None = None,
    candidate_count_after_realism_filter: dict[str, int] | None = None,
    hard_rejected_count_by_slot: dict[str, int] | None = None,
    hard_reject_candidate_count_by_slot: dict[str, int] | None = None,
    hard_reject_reasons: dict[str, list[dict[str, object]]] | None = None,
    possible_before_limit: int = 0,
    possible_after_limit: int = 0,
    rejected_repeated_count: int = 0,
) -> dict[str, object]:
    diagnostics = {
        "selector_mode": BALANCED_DAY_MODE,
        "candidate_count_per_slot_before_shortlist": candidate_count_before,
        "candidate_count_per_slot_after_shortlist": candidate_count_after,
        "candidate_count_per_slot_before_realism_filter": (
            candidate_count_before_realism_filter or {}
        ),
        "candidate_count_per_slot_after_realism_filter": (
            candidate_count_after_realism_filter or {}
        ),
        "hard_rejected_count_by_slot": hard_rejected_count_by_slot or {},
        "hard_reject_candidate_count_by_slot": hard_reject_candidate_count_by_slot or {},
        "hard_reject_reasons": hard_reject_reasons or {},
        "possible_combination_count_before_limit": possible_before_limit,
        "possible_combination_count_after_shortlist": possible_after_limit,
        "evaluated_combination_count": 0,
        "rejected_repeated_recipe_combination_count": rejected_repeated_count,
        "missing_slots": missing_slots,
        "selector_warnings": warnings.copy(),
        **_rounded_loss(compute_day_loss_for_plan([], target)),
    }
    diagnostics["base_day_loss"] = diagnostics["day_loss"]
    diagnostics["adjusted_day_loss"] = diagnostics["day_loss"]
    diagnostics["diversity_mode"] = "none"
    diagnostics["recent_recipe_ids_considered"] = []
    diagnostics["meal_realism_mode"] = "off"
    diagnostics["meal_realism_total_penalty"] = 0.0
    diagnostics["realism_penalty_total"] = 0.0
    diagnostics["meal_realism_applied_penalty"] = 0.0
    diagnostics["meal_realism_flags_by_meal"] = []
    diagnostics["alternative_count_requested"] = 0
    diagnostics["alternative_count_returned"] = 0
    diagnostics["diversity_penalties"] = {
        "diversity_mode": "none",
        "recent_recipe_count": 0,
        "recent_recipe_ids_selected": [],
        "recent_recipe_penalty": 0.0,
        "same_family_duplicate_count": 0,
        "same_family_penalty": 0.0,
        "total_diversity_penalty": 0.0,
    }
    diagnostics["meal_realism_penalties"] = _empty_meal_realism_penalties()
    return {
        "selected_meals": [],
        "day_totals": _day_totals([]),
        "warnings": warnings,
        "selector_mode": BALANCED_DAY_MODE,
        "selector_diagnostics": diagnostics,
    }


def _target_to_dict(target: NutritionTarget | dict[str, object]) -> dict[str, object]:
    if isinstance(target, NutritionTarget):
        return {
            "kcal": target.kcal,
            "protein_g": target.protein_g,
            "carbs_g": target.carbs_g,
            "fat_g": target.fat_g,
            "slot_targets": target.slot_targets,
        }
    return target


def _resolved_config(config: dict[str, object] | None) -> dict[str, object]:
    resolved = dict(DEFAULT_BALANCED_CONFIG)
    if config:
        resolved.update(config)
    if resolved.get("recent_recipe_penalty") is None:
        mode = _diversity_mode(resolved)
        if mode == "soft":
            resolved["recent_recipe_penalty"] = 0.04
        elif mode == "avoid_recent":
            resolved["recent_recipe_penalty"] = 0.18
        else:
            resolved["recent_recipe_penalty"] = 0.0
    return resolved


def _alternative_count(config: Mapping[str, object]) -> int:
    count = int(_to_float(config.get("alternative_count")))
    return max(1, min(10, count))


def _diversity_mode(config: Mapping[str, object]) -> str:
    mode = _clean_text(config.get("diversity_mode")).lower()
    if mode in {"soft", "avoid_recent"}:
        return mode
    return "none"


def _meal_realism_mode(config: Mapping[str, object]) -> str:
    mode = _clean_text(config.get("meal_realism_mode")).lower()
    if mode in {"audit", "soft", "practical"}:
        return mode
    return "off"


def _recent_recipe_ids(config: Mapping[str, object]) -> set[str]:
    value = config.get("recent_recipe_ids")
    if value is None:
        return set()
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        items = value
    else:
        items = []
    return {_clean_text(item) for item in items if _clean_text(item)}


def _meal_realism_flags_by_meal(
    meals: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    rows = []
    for meal in meals:
        flags = _reason_list(meal.get("meal_realism_flags"))
        if not flags:
            continue
        rows.append(
            {
                "slot": _clean_text(meal.get("slot")),
                "recipe_id": _clean_text(meal.get("recipe_id")),
                "flags": flags,
            }
        )
    return rows


def _reason_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    if isinstance(value, tuple):
        return [_clean_text(item) for item in value if _clean_text(item)]
    text = _clean_text(value)
    if not text:
        return []
    if "|" in text:
        return [item.strip() for item in text.split("|") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text]


def _absolute_ratio_loss(actual: float, target: float) -> float:
    if target <= 0:
        return 0.0
    return abs(actual - target) / target


def _protein_loss(actual: float, target: float) -> float:
    if target <= 0:
        return 0.0
    if actual < target:
        return (target - actual) / target
    return max(0.0, (actual - 1.6 * target) / target) * 0.5


def _time_penalty(effective_time: float, config: dict[str, object]) -> float:
    limit = _to_float(config.get("max_effective_time_min"))
    if limit <= 0 or effective_time <= limit:
        return 0.0
    return min(0.10, ((effective_time - limit) / limit) * 0.04)


def _average_score(selected_meals: Sequence[Mapping[str, object]]) -> float:
    if not selected_meals:
        return 0.0
    return sum(_to_float(meal.get("score_preview")) for meal in selected_meals) / len(selected_meals)


def _rounded_loss(loss: Mapping[str, object]) -> dict[str, float]:
    fields = [
        "day_loss",
        "macro_day_loss",
        "kcal_loss",
        "protein_loss",
        "carbs_loss",
        "fat_loss",
        "time_penalty",
        "slot_suspicious_penalty",
        "long_passive_penalty",
        "score_penalty",
        "average_score_preview",
        "effective_time_min_sum",
    ]
    return {field: round(_to_float(loss.get(field)), 6) for field in fields}


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value) or math.isinf(numeric_value):
        return None
    return numeric_value


def _to_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(numeric_value) or math.isinf(numeric_value):
        return 0.0
    return numeric_value


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    return text in {"1", "true", "yes", "y"}


def _clean_text(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value or "").strip()


def _clean_value(value: object) -> object:
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
