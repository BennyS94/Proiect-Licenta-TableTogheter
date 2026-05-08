from __future__ import annotations

import csv
import itertools
import json
import math
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.generator_v1.candidate_filter import (
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.day_selector import select_one_day_plan
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target

try:
    from evaluate_generator_v1_round13_scenarios import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        scenario_overrides,
    )
except ImportError:
    from tools.extra.evaluate_generator_v1_round13_scenarios import (
        BASE_PROFILE_PATH,
        build_scenario_profile,
        scenario_overrides,
    )


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round14_carb_availability_summary.txt"
OUT_DISTRIBUTION = OUT_DIR / "generator_v1_round14_slot_candidate_macro_distribution.csv"
OUT_SELECTED_VS_ALT = OUT_DIR / "generator_v1_round14_selected_vs_carb_alternatives.csv"
OUT_ORACLE_PLANS = OUT_DIR / "generator_v1_round14_oracle_day_balance_plans.csv"
OUT_ORACLE_MEALS = OUT_DIR / "generator_v1_round14_oracle_day_balance_meals.csv"
OUT_DOMINANCE = OUT_DIR / "generator_v1_round14_recipe_dominance_audit.csv"
OUT_RECOMMENDATION = OUT_DIR / "generator_v1_round14_next_step_recommendation.txt"

ORACLE_TOP_N_PER_SLOT = 20
ORACLE_DAY_TIME_SOFT_LIMIT = 300.0

DISTRIBUTION_COLUMNS = [
    "scenario_id",
    "slot",
    "slot_target_kcal",
    "slot_target_protein_g",
    "slot_target_carbs_g",
    "slot_target_fat_g",
    "candidate_count",
    "kcal_p25",
    "kcal_median",
    "kcal_p75",
    "kcal_max",
    "protein_p25",
    "protein_median",
    "protein_p75",
    "protein_max",
    "carbs_p25",
    "carbs_median",
    "carbs_p75",
    "carbs_max",
    "fat_p25",
    "fat_median",
    "fat_p75",
    "fat_max",
    "carb_ratio_p25",
    "carb_ratio_median",
    "carb_ratio_p75",
    "carb_ratio_max",
    "count_carbs_gte_30",
    "count_carbs_gte_50",
    "count_carbs_gte_70",
    "count_kcal_gte_slot_70pct",
    "count_protein_gte_slot_70pct",
    "count_carbs_gte_slot_70pct",
    "count_meeting_kcal_protein_carbs_approx",
    "top10_carb_forward_json",
    "top10_kcal_forward_json",
    "top10_score_preview_json",
]

SELECTED_ALT_COLUMNS = [
    "scenario_id",
    "slot",
    "selected_recipe_id",
    "selected_display_name",
    "selected_carbs_g",
    "selected_kcal",
    "selected_protein_g",
    "selected_fat_g",
    "selected_score_preview",
    "selected_macro_fit",
    "selected_protein_fit",
    "selected_kcal_fit",
    "selected_carbs_fit",
    "selected_fat_fit",
    "selected_time_fit",
    "selected_slot_fit",
    "selected_recipe_kind",
    "selected_allowed_slots_json",
    "alt_recipe_id",
    "alt_display_name",
    "alt_carbs_g",
    "alt_kcal",
    "alt_protein_g",
    "alt_fat_g",
    "alt_score_preview",
    "alt_macro_fit",
    "alt_protein_fit",
    "alt_kcal_fit",
    "alt_carbs_fit",
    "alt_fat_fit",
    "alt_time_fit",
    "alt_slot_fit",
    "alt_recipe_kind",
    "alt_allowed_slots_json",
    "score_gap",
    "carbs_gap",
    "why_alt_lost_guess",
]

ORACLE_PLAN_COLUMNS = [
    "scenario_id",
    "target_kcal",
    "target_protein_g",
    "target_carbs_g",
    "target_fat_g",
    "current_total_kcal",
    "current_total_protein_g",
    "current_total_carbs_g",
    "current_total_fat_g",
    "current_effective_time_min_sum",
    "current_validation_status",
    "current_day_loss",
    "oracle_total_kcal",
    "oracle_total_protein_g",
    "oracle_total_carbs_g",
    "oracle_total_fat_g",
    "oracle_effective_time_min_sum",
    "oracle_validation_status",
    "oracle_day_loss",
    "day_loss_improvement",
    "kcal_gap_improvement",
    "protein_gap_improvement",
    "carbs_gap_improvement",
    "fat_gap_improvement",
    "current_carbs_ratio",
    "oracle_carbs_ratio",
    "current_kcal_ratio",
    "oracle_kcal_ratio",
    "oracle_candidate_pool_sizes_json",
    "oracle_warning",
]

ORACLE_MEAL_COLUMNS = [
    "scenario_id",
    "slot",
    "recipe_id",
    "display_name",
    "recipe_kind",
    "portion_multiplier",
    "kcal",
    "protein_g",
    "carbs_g",
    "fat_g",
    "score_preview",
    "macro_fit",
    "time_fit",
    "slot_fit",
    "nutrition_quality",
    "allowed_slots_json",
    "effective_time_min_for_scoring",
    "oracle_source_rank_note",
]

DOMINANCE_COLUMNS = [
    "recipe_id",
    "display_name",
    "recipe_kind",
    "selected_count_current",
    "selected_count_oracle",
    "slots_used_current",
    "slots_used_oracle",
    "average_score_preview_current",
    "average_score_preview_oracle",
    "average_kcal",
    "average_protein_g",
    "average_carbs_g",
    "average_fat_g",
    "dominance_guess",
]


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value or "").strip()


def to_float(value: object) -> float:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return 0.0
    return float(numeric)


def round_number(value: object) -> str:
    text = f"{to_float(value):.4f}".rstrip("0").rstrip(".")
    return text or "0"


def ratio(actual: object, target: object) -> float:
    target_value = to_float(target)
    if target_value <= 0:
        return 0.0
    return to_float(actual) / target_value


def target_to_dict(target: NutritionTarget) -> dict[str, object]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def run_generator_flow(profile: dict[str, Any], fooddb: pd.DataFrame) -> dict[str, Any]:
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    )
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
    )
    plan = select_one_day_plan(
        slot_candidates_by_slot=slot_candidates_by_slot(slot_candidates, slot_order(target)),
        slot_order=slot_order(target),
    )
    plan["validation"] = validate_one_day_plan(plan, target)
    return {
        "target": target,
        "slot_candidates": slot_candidates,
        "current_plan": plan,
        "filtered_count": len(filtered_candidates),
        "eligible_count": len(pool.eligible_candidates),
    }


def slot_order(target: NutritionTarget) -> list[str]:
    return list(target.slot_targets.keys())


def slot_candidates_by_slot(slot_candidates: pd.DataFrame, slots: list[str]) -> dict[str, pd.DataFrame]:
    return {
        slot: slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        for slot in slots
    }


def candidate_lookup(slot_candidates: pd.DataFrame) -> dict[tuple[str, str, float], dict[str, object]]:
    lookup: dict[tuple[str, str, float], dict[str, object]] = {}
    for _, row in slot_candidates.iterrows():
        lookup[candidate_key(row)] = row.to_dict()
    return lookup


def candidate_key(row: pd.Series | dict[str, object]) -> tuple[str, str, float]:
    getter = row.get
    return (
        clean_text(getter("slot")),
        clean_text(getter("recipe_id")),
        round(to_float(getter("portion_multiplier")), 4),
    )


def add_carb_ratio(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    kcal = pd.to_numeric(result.get("kcal"), errors="coerce")
    carbs = pd.to_numeric(result.get("carbs_g"), errors="coerce")
    result["carb_ratio"] = (carbs * 4 / kcal).where(kcal > 0)
    return result


def quantile_value(series: pd.Series, quantile: float) -> str:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return ""
    return round_number(numeric.quantile(quantile))


def max_value(series: pd.Series) -> str:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return ""
    return round_number(numeric.max())


def candidate_distribution_rows(
    scenario_id: str,
    target: NutritionTarget,
    slot_candidates: pd.DataFrame,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    slot_candidates = add_carb_ratio(slot_candidates)
    for slot in slot_order(target):
        group = slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        slot_target = target.slot_targets[slot]
        row = {
            "scenario_id": scenario_id,
            "slot": slot,
            "slot_target_kcal": slot_target["kcal"],
            "slot_target_protein_g": slot_target["protein_g"],
            "slot_target_carbs_g": slot_target["carbs_g"],
            "slot_target_fat_g": slot_target["fat_g"],
            "candidate_count": len(group),
        }
        for macro_column, prefix in (
            ("kcal", "kcal"),
            ("protein_g", "protein"),
            ("carbs_g", "carbs"),
            ("fat_g", "fat"),
            ("carb_ratio", "carb_ratio"),
        ):
            row[f"{prefix}_p25"] = quantile_value(group[macro_column], 0.25) if macro_column in group else ""
            row[f"{prefix}_median"] = quantile_value(group[macro_column], 0.5) if macro_column in group else ""
            row[f"{prefix}_p75"] = quantile_value(group[macro_column], 0.75) if macro_column in group else ""
            row[f"{prefix}_max"] = max_value(group[macro_column]) if macro_column in group else ""
        row.update(candidate_count_metrics(group, slot_target))
        row["top10_carb_forward_json"] = top_candidates_json(group, "carbs_g")
        row["top10_kcal_forward_json"] = top_candidates_json(group, "kcal")
        row["top10_score_preview_json"] = top_candidates_json(group, "score_preview")
        rows.append(row)
    return rows


def candidate_count_metrics(group: pd.DataFrame, slot_target: dict[str, float]) -> dict[str, int]:
    if group.empty:
        return {
            "count_carbs_gte_30": 0,
            "count_carbs_gte_50": 0,
            "count_carbs_gte_70": 0,
            "count_kcal_gte_slot_70pct": 0,
            "count_protein_gte_slot_70pct": 0,
            "count_carbs_gte_slot_70pct": 0,
            "count_meeting_kcal_protein_carbs_approx": 0,
        }
    carbs = pd.to_numeric(group["carbs_g"], errors="coerce").fillna(0)
    kcal = pd.to_numeric(group["kcal"], errors="coerce").fillna(0)
    protein = pd.to_numeric(group["protein_g"], errors="coerce").fillna(0)
    kcal_ok = kcal >= float(slot_target["kcal"]) * 0.7
    protein_ok = protein >= float(slot_target["protein_g"]) * 0.7
    carbs_ok = carbs >= float(slot_target["carbs_g"]) * 0.7 if float(slot_target["carbs_g"]) > 0 else carbs >= 0
    return {
        "count_carbs_gte_30": int((carbs >= 30).sum()),
        "count_carbs_gte_50": int((carbs >= 50).sum()),
        "count_carbs_gte_70": int((carbs >= 70).sum()),
        "count_kcal_gte_slot_70pct": int(kcal_ok.sum()),
        "count_protein_gte_slot_70pct": int(protein_ok.sum()),
        "count_carbs_gte_slot_70pct": int(carbs_ok.sum()),
        "count_meeting_kcal_protein_carbs_approx": int((kcal_ok & protein_ok & carbs_ok).sum()),
    }


def top_candidates_json(group: pd.DataFrame, sort_column: str) -> str:
    if group.empty or sort_column not in group.columns:
        return "[]"
    preview_columns = [
        "recipe_id",
        "display_name",
        "recipe_kind",
        "portion_multiplier",
        "kcal",
        "protein_g",
        "carbs_g",
        "fat_g",
        "score_preview",
        "macro_fit",
        "protein_fit",
        "kcal_fit",
        "carbs_fit",
        "fat_fit",
        "time_fit",
        "slot_fit",
        "is_slot_suspicious",
    ]
    sorted_group = group.sort_values(sort_column, ascending=False, na_position="last").head(10)
    rows: list[dict[str, object]] = []
    for _, row in sorted_group.iterrows():
        rows.append(
            {
                column: clean_json_value(row.get(column))
                for column in preview_columns
                if column in sorted_group.columns
            }
        )
    return json.dumps(rows, ensure_ascii=False)


def clean_json_value(value: object) -> object:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(float(value)) or math.isinf(float(value)):
            return None
        return round(float(value), 4)
    text = clean_text(value)
    return text


def selected_vs_alternative_rows(
    scenario_id: str,
    target: NutritionTarget,
    slot_candidates: pd.DataFrame,
    current_plan: dict[str, Any],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    lookup = candidate_lookup(slot_candidates)
    selected_meals = {
        clean_text(meal.get("slot")): meal
        for meal in current_plan.get("selected_meals", [])
    }
    for slot in slot_order(target):
        selected_meal = selected_meals.get(slot, {})
        selected_candidate = lookup.get(candidate_key(selected_meal), {})
        group = slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        alt_candidate = best_carb_alternative(group, selected_candidate, target.slot_targets[slot])
        rows.append(build_selected_alt_row(scenario_id, slot, selected_candidate, alt_candidate))
    return rows


def best_carb_alternative(
    group: pd.DataFrame,
    selected_candidate: dict[str, object],
    slot_target: dict[str, float],
) -> dict[str, object]:
    if group.empty:
        return {}
    selected_recipe_id = clean_text(selected_candidate.get("recipe_id"))
    selected_carbs = to_float(selected_candidate.get("carbs_g"))
    candidates = group.copy()
    if "is_slot_suspicious" in candidates.columns:
        candidates = candidates.loc[~candidates["is_slot_suspicious"].fillna(False).astype(bool)].copy()
    if selected_recipe_id:
        candidates = candidates.loc[~candidates["recipe_id"].astype(str).eq(selected_recipe_id)].copy()
    if candidates.empty:
        return {}
    kcal = pd.to_numeric(candidates["kcal"], errors="coerce").fillna(0)
    carbs = pd.to_numeric(candidates["carbs_g"], errors="coerce").fillna(0)
    min_kcal = float(slot_target["kcal"]) * 0.45
    max_kcal = max(float(slot_target["kcal"]) * 1.60, min_kcal + 1)
    carb_floor = max(30.0, float(slot_target["carbs_g"]) * 0.40)
    preferred = candidates.loc[
        (kcal >= min_kcal)
        & (kcal <= max_kcal)
        & (carbs >= max(carb_floor, selected_carbs + 1))
    ].copy()
    if preferred.empty:
        preferred = candidates.loc[(carbs >= selected_carbs + 1)].copy()
    if preferred.empty:
        preferred = candidates.copy()
    preferred["_rank_carbs"] = pd.to_numeric(preferred["carbs_g"], errors="coerce").fillna(0)
    preferred["_rank_carbs_fit"] = pd.to_numeric(preferred.get("carbs_fit"), errors="coerce").fillna(0)
    preferred["_rank_score"] = pd.to_numeric(preferred.get("score_preview"), errors="coerce").fillna(0)
    sorted_candidates = preferred.sort_values(
        ["_rank_carbs", "_rank_carbs_fit", "_rank_score"],
        ascending=[False, False, False],
        kind="mergesort",
    )
    return sorted_candidates.iloc[0].to_dict()


def build_selected_alt_row(
    scenario_id: str,
    slot: str,
    selected: dict[str, object],
    alt: dict[str, object],
) -> dict[str, object]:
    row: dict[str, object] = {"scenario_id": scenario_id, "slot": slot}
    row.update(prefix_candidate("selected", selected))
    row.update(prefix_candidate("alt", alt))
    row["score_gap"] = round_number(to_float(selected.get("score_preview")) - to_float(alt.get("score_preview")))
    row["carbs_gap"] = round_number(to_float(alt.get("carbs_g")) - to_float(selected.get("carbs_g")))
    row["why_alt_lost_guess"] = why_alt_lost(selected, alt)
    return row


def prefix_candidate(prefix: str, candidate: dict[str, object]) -> dict[str, object]:
    fields = {
        "recipe_id": "recipe_id",
        "display_name": "display_name",
        "carbs_g": "carbs_g",
        "kcal": "kcal",
        "protein_g": "protein_g",
        "fat_g": "fat_g",
        "score_preview": "score_preview",
        "macro_fit": "macro_fit",
        "protein_fit": "protein_fit",
        "kcal_fit": "kcal_fit",
        "carbs_fit": "carbs_fit",
        "fat_fit": "fat_fit",
        "time_fit": "time_fit",
        "slot_fit": "slot_fit",
        "recipe_kind": "recipe_kind",
        "allowed_slots_json": "allowed_slots_json",
    }
    result: dict[str, object] = {}
    for out_name, source_name in fields.items():
        value = candidate.get(source_name, "")
        result[f"{prefix}_{out_name}"] = round_number(value) if source_name in NUMERIC_CANDIDATE_FIELDS else clean_text(value)
    return result


NUMERIC_CANDIDATE_FIELDS = {
    "carbs_g",
    "kcal",
    "protein_g",
    "fat_g",
    "score_preview",
    "macro_fit",
    "protein_fit",
    "kcal_fit",
    "carbs_fit",
    "fat_fit",
    "time_fit",
    "slot_fit",
}


def why_alt_lost(selected: dict[str, object], alt: dict[str, object]) -> str:
    if not alt:
        return "not_available"
    selected_score = to_float(selected.get("score_preview"))
    alt_score = to_float(alt.get("score_preview"))
    if alt_score >= selected_score:
        return "other"
    component_map = {
        "lower_protein_fit": to_float(selected.get("protein_fit")) - to_float(alt.get("protein_fit")),
        "lower_kcal_fit": to_float(selected.get("kcal_fit")) - to_float(alt.get("kcal_fit")),
        "time_penalty": to_float(selected.get("time_fit")) - to_float(alt.get("time_fit")),
        "slot_penalty": to_float(selected.get("slot_fit")) - to_float(alt.get("slot_fit")),
        "nutrition_quality_penalty": to_float(selected.get("nutrition_quality")) - to_float(alt.get("nutrition_quality")),
    }
    best_reason, best_gap = max(component_map.items(), key=lambda item: item[1])
    if best_gap > 0.05:
        return best_reason
    return "lower_score"


def oracle_subset_for_slot(group: pd.DataFrame, top_n: int = ORACLE_TOP_N_PER_SLOT) -> pd.DataFrame:
    if group.empty:
        return group.copy()
    non_suspicious = group.copy()
    if "is_slot_suspicious" in non_suspicious.columns:
        non_suspicious = non_suspicious.loc[~non_suspicious["is_slot_suspicious"].fillna(False).astype(bool)].copy()
    if non_suspicious.empty:
        non_suspicious = group.copy()
    frames = [
        non_suspicious.sort_values("score_preview", ascending=False, na_position="last").head(8),
        non_suspicious.sort_values("carbs_g", ascending=False, na_position="last").head(8),
        non_suspicious.sort_values("kcal", ascending=False, na_position="last").head(8),
    ]
    combined = pd.concat(frames, ignore_index=True)
    combined["_dedupe_key"] = combined.apply(lambda row: "|".join(map(str, candidate_key(row))), axis=1)
    combined = combined.drop_duplicates("_dedupe_key").copy()
    combined["_rank_score"] = pd.to_numeric(combined.get("score_preview"), errors="coerce").fillna(0)
    combined["_rank_carbs"] = pd.to_numeric(combined.get("carbs_g"), errors="coerce").fillna(0)
    combined["_rank_kcal"] = pd.to_numeric(combined.get("kcal"), errors="coerce").fillna(0)
    combined["_rank_combined"] = (
        combined["_rank_score"] * 1000
        + combined["_rank_carbs"] / 10
        + combined["_rank_kcal"] / 100
    )
    return combined.sort_values("_rank_combined", ascending=False, kind="mergesort").head(top_n).copy()


def oracle_for_scenario(
    scenario_id: str,
    target: NutritionTarget,
    slot_candidates: pd.DataFrame,
    current_plan: dict[str, Any],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    slot_subsets: dict[str, list[dict[str, object]]] = {}
    pool_sizes: dict[str, int] = {}
    for slot in slot_order(target):
        group = slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        subset = oracle_subset_for_slot(group)
        slot_subsets[slot] = [row.to_dict() for _, row in subset.iterrows()]
        pool_sizes[slot] = len(subset)

    best_combo: tuple[dict[str, object], ...] | None = None
    best_loss = float("inf")
    warning = ""
    if any(not rows for rows in slot_subsets.values()):
        warning = "missing_slot_candidates"
    else:
        for combo in itertools.product(*(slot_subsets[slot] for slot in slot_order(target))):
            recipe_ids = [clean_text(row.get("recipe_id")) for row in combo]
            if len(recipe_ids) != len(set(recipe_ids)):
                continue
            totals = totals_for_candidate_rows(combo)
            loss = day_loss(totals, target)
            if loss < best_loss:
                best_loss = loss
                best_combo = combo

    if best_combo is None:
        best_combo = tuple()
        warning = warning or "no_valid_oracle_combo"

    oracle_totals = totals_for_candidate_rows(best_combo)
    oracle_plan = {
        "selected_meals": [selected_meal_for_validator(row) for row in best_combo],
        "day_totals": oracle_totals,
    }
    oracle_validation = validate_one_day_plan(oracle_plan, target)
    current_totals = current_plan.get("day_totals", {})
    current_validation = current_plan.get("validation", {})
    current_loss = day_loss(current_totals, target)
    plan_row = build_oracle_plan_row(
        scenario_id=scenario_id,
        target=target,
        current_totals=current_totals,
        current_validation=current_validation,
        current_loss=current_loss,
        oracle_totals=oracle_totals,
        oracle_validation=oracle_validation,
        oracle_loss=best_loss if math.isfinite(best_loss) else 0.0,
        pool_sizes=pool_sizes,
        warning=warning,
    )
    meal_rows = [oracle_meal_row(scenario_id, row) for row in best_combo]
    return plan_row, meal_rows


def totals_for_candidate_rows(rows: tuple[dict[str, object], ...] | list[dict[str, object]]) -> dict[str, object]:
    return {
        "total_kcal": round(sum(to_float(row.get("kcal")) for row in rows), 1),
        "total_protein_g": round(sum(to_float(row.get("protein_g")) for row in rows), 1),
        "total_carbs_g": round(sum(to_float(row.get("carbs_g")) for row in rows), 1),
        "total_fat_g": round(sum(to_float(row.get("fat_g")) for row in rows), 1),
        "total_time_min_sum": round(sum(to_float(row.get("total_time_min")) for row in rows), 1),
        "effective_time_min_sum": round(sum(to_float(row.get("effective_time_min_for_scoring")) for row in rows), 1),
        "passive_time_estimated_sum": round(sum(to_float(row.get("passive_time_estimated_min")) for row in rows), 1),
        "selected_slot_count": len(rows),
    }


def selected_meal_for_validator(row: dict[str, object]) -> dict[str, object]:
    return {
        "slot": clean_text(row.get("slot")),
        "recipe_id": clean_text(row.get("recipe_id")),
        "display_name": clean_text(row.get("display_name")),
        "kcal": to_float(row.get("kcal")),
        "protein_g": to_float(row.get("protein_g")),
        "carbs_g": to_float(row.get("carbs_g")),
        "fat_g": to_float(row.get("fat_g")),
        "effective_time_min_for_scoring": to_float(row.get("effective_time_min_for_scoring")),
        "total_time_min": to_float(row.get("total_time_min")),
        "passive_time_estimated_min": to_float(row.get("passive_time_estimated_min")),
        "has_long_passive_time": bool(row.get("has_long_passive_time")),
        "is_nutrition_suspicious": bool(row.get("is_nutrition_suspicious")),
    }


def day_loss(totals: dict[str, object], target: NutritionTarget) -> float:
    kcal_loss = abs(to_float(totals.get("total_kcal")) - target.kcal) / target.kcal if target.kcal > 0 else 0.0
    protein_ratio = ratio(totals.get("total_protein_g"), target.protein_g)
    if protein_ratio < 1.0:
        protein_loss = (1.0 - protein_ratio) * 1.5
    elif protein_ratio > 1.6:
        protein_loss = (protein_ratio - 1.6) * 0.5
    else:
        protein_loss = 0.0
    carbs_loss = abs(to_float(totals.get("total_carbs_g")) - target.carbs_g) / target.carbs_g if target.carbs_g > 0 else 0.0
    fat_loss = abs(to_float(totals.get("total_fat_g")) - target.fat_g) / target.fat_g if target.fat_g > 0 else 0.0
    loss = 0.35 * kcal_loss + 0.30 * protein_loss + 0.25 * carbs_loss + 0.10 * fat_loss
    if to_float(totals.get("effective_time_min_sum")) > ORACLE_DAY_TIME_SOFT_LIMIT:
        loss += (to_float(totals.get("effective_time_min_sum")) - ORACLE_DAY_TIME_SOFT_LIMIT) / ORACLE_DAY_TIME_SOFT_LIMIT * 0.05
    return round(loss, 6)


def abs_gap(value: object, target_value: float) -> float:
    return abs(to_float(value) - float(target_value))


def build_oracle_plan_row(
    scenario_id: str,
    target: NutritionTarget,
    current_totals: dict[str, object],
    current_validation: dict[str, object],
    current_loss: float,
    oracle_totals: dict[str, object],
    oracle_validation: dict[str, object],
    oracle_loss: float,
    pool_sizes: dict[str, int],
    warning: str,
) -> dict[str, object]:
    return {
        "scenario_id": scenario_id,
        "target_kcal": target.kcal,
        "target_protein_g": target.protein_g,
        "target_carbs_g": target.carbs_g,
        "target_fat_g": target.fat_g,
        "current_total_kcal": current_totals.get("total_kcal", 0),
        "current_total_protein_g": current_totals.get("total_protein_g", 0),
        "current_total_carbs_g": current_totals.get("total_carbs_g", 0),
        "current_total_fat_g": current_totals.get("total_fat_g", 0),
        "current_effective_time_min_sum": current_totals.get("effective_time_min_sum", 0),
        "current_validation_status": current_validation.get("validation_status", ""),
        "current_day_loss": current_loss,
        "oracle_total_kcal": oracle_totals.get("total_kcal", 0),
        "oracle_total_protein_g": oracle_totals.get("total_protein_g", 0),
        "oracle_total_carbs_g": oracle_totals.get("total_carbs_g", 0),
        "oracle_total_fat_g": oracle_totals.get("total_fat_g", 0),
        "oracle_effective_time_min_sum": oracle_totals.get("effective_time_min_sum", 0),
        "oracle_validation_status": oracle_validation.get("validation_status", ""),
        "oracle_day_loss": oracle_loss,
        "day_loss_improvement": round_number(current_loss - oracle_loss),
        "kcal_gap_improvement": round_number(abs_gap(current_totals.get("total_kcal"), target.kcal) - abs_gap(oracle_totals.get("total_kcal"), target.kcal)),
        "protein_gap_improvement": round_number(abs_gap(current_totals.get("total_protein_g"), target.protein_g) - abs_gap(oracle_totals.get("total_protein_g"), target.protein_g)),
        "carbs_gap_improvement": round_number(abs_gap(current_totals.get("total_carbs_g"), target.carbs_g) - abs_gap(oracle_totals.get("total_carbs_g"), target.carbs_g)),
        "fat_gap_improvement": round_number(abs_gap(current_totals.get("total_fat_g"), target.fat_g) - abs_gap(oracle_totals.get("total_fat_g"), target.fat_g)),
        "current_carbs_ratio": round_number(ratio(current_totals.get("total_carbs_g"), target.carbs_g)),
        "oracle_carbs_ratio": round_number(ratio(oracle_totals.get("total_carbs_g"), target.carbs_g)),
        "current_kcal_ratio": round_number(ratio(current_totals.get("total_kcal"), target.kcal)),
        "oracle_kcal_ratio": round_number(ratio(oracle_totals.get("total_kcal"), target.kcal)),
        "oracle_candidate_pool_sizes_json": json.dumps(pool_sizes, sort_keys=True),
        "oracle_warning": warning,
    }


def oracle_meal_row(scenario_id: str, row: dict[str, object]) -> dict[str, object]:
    return {
        "scenario_id": scenario_id,
        "slot": clean_text(row.get("slot")),
        "recipe_id": clean_text(row.get("recipe_id")),
        "display_name": clean_text(row.get("display_name")),
        "recipe_kind": clean_text(row.get("recipe_kind")),
        "portion_multiplier": round_number(row.get("portion_multiplier")),
        "kcal": round_number(row.get("kcal")),
        "protein_g": round_number(row.get("protein_g")),
        "carbs_g": round_number(row.get("carbs_g")),
        "fat_g": round_number(row.get("fat_g")),
        "score_preview": round_number(row.get("score_preview")),
        "macro_fit": round_number(row.get("macro_fit")),
        "time_fit": round_number(row.get("time_fit")),
        "slot_fit": round_number(row.get("slot_fit")),
        "nutrition_quality": round_number(row.get("nutrition_quality")),
        "allowed_slots_json": clean_text(row.get("allowed_slots_json")),
        "effective_time_min_for_scoring": round_number(row.get("effective_time_min_for_scoring")),
        "oracle_source_rank_note": "audit_oracle_top_score_carbs_kcal_union",
    }


def current_meal_rows_from_plan(
    scenario_id: str,
    current_plan: dict[str, Any],
    lookup: dict[tuple[str, str, float], dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for meal in current_plan.get("selected_meals", []):
        candidate = lookup.get(candidate_key(meal), {})
        rows.append(
            {
                "scenario_id": scenario_id,
                "slot": clean_text(meal.get("slot")),
                "recipe_id": clean_text(meal.get("recipe_id")),
                "display_name": clean_text(meal.get("display_name")),
                "recipe_kind": clean_text(candidate.get("recipe_kind")),
                "kcal": to_float(meal.get("kcal")),
                "protein_g": to_float(meal.get("protein_g")),
                "carbs_g": to_float(meal.get("carbs_g")),
                "fat_g": to_float(meal.get("fat_g")),
                "score_preview": to_float(meal.get("score_preview")),
            }
        )
    return rows


def dominance_rows(current_meals: list[dict[str, object]], oracle_meals: list[dict[str, object]]) -> list[dict[str, object]]:
    recipe_ids = sorted({clean_text(row.get("recipe_id")) for row in current_meals + oracle_meals if clean_text(row.get("recipe_id"))})
    rows: list[dict[str, object]] = []
    for recipe_id in recipe_ids:
        current = [row for row in current_meals if clean_text(row.get("recipe_id")) == recipe_id]
        oracle = [row for row in oracle_meals if clean_text(row.get("recipe_id")) == recipe_id]
        all_rows = current + oracle
        display_name = clean_text(all_rows[0].get("display_name")) if all_rows else ""
        recipe_kind = clean_text(all_rows[0].get("recipe_kind")) if all_rows else ""
        current_count = len(current)
        oracle_count = len(oracle)
        rows.append(
            {
                "recipe_id": recipe_id,
                "display_name": display_name,
                "recipe_kind": recipe_kind,
                "selected_count_current": current_count,
                "selected_count_oracle": oracle_count,
                "slots_used_current": slots_used(current),
                "slots_used_oracle": slots_used(oracle),
                "average_score_preview_current": average_number(current, "score_preview"),
                "average_score_preview_oracle": average_number(oracle, "score_preview"),
                "average_kcal": average_number(all_rows, "kcal"),
                "average_protein_g": average_number(all_rows, "protein_g"),
                "average_carbs_g": average_number(all_rows, "carbs_g"),
                "average_fat_g": average_number(all_rows, "fat_g"),
                "dominance_guess": dominance_guess(current_count, oracle_count),
            }
        )
    return sorted(rows, key=lambda row: (-(int(row["selected_count_current"]) + int(row["selected_count_oracle"])), row["recipe_id"]))


def slots_used(rows: list[dict[str, object]]) -> str:
    counter = Counter(clean_text(row.get("slot")) for row in rows)
    return "|".join(f"{slot}:{count}" for slot, count in sorted(counter.items()) if slot)


def average_number(rows: list[dict[str, object]], column: str) -> str:
    values = [to_float(row.get(column)) for row in rows]
    if not values:
        return ""
    return round_number(sum(values) / len(values))


def dominance_guess(current_count: int, oracle_count: int) -> str:
    if current_count >= 4 and oracle_count >= 4:
        return "genuinely_best_or_limited_alternatives"
    if current_count >= 4 and oracle_count < current_count:
        return "scoring_bias_or_greedy_pressure"
    if oracle_count >= 4 and current_count < oracle_count:
        return "oracle_macro_pressure"
    return "normal_repetition"


def build_summary(
    distribution_rows: list[dict[str, object]],
    selected_alt_rows: list[dict[str, object]],
    oracle_plan_rows: list[dict[str, object]],
    dominance_audit_rows: list[dict[str, object]],
) -> tuple[str, str]:
    scenario_count = len({clean_text(row.get("scenario_id")) for row in oracle_plan_rows})
    current_valid = sum(1 for row in oracle_plan_rows if clean_text(row.get("current_validation_status")) == "valid")
    oracle_valid = sum(1 for row in oracle_plan_rows if clean_text(row.get("oracle_validation_status")) == "valid")
    oracle_improved_loss = sum(to_float(row.get("day_loss_improvement")) > 0.02 for row in oracle_plan_rows)
    oracle_improved_carbs = sum(to_float(row.get("carbs_gap_improvement")) > 20 for row in oracle_plan_rows)
    alt_exists = [row for row in selected_alt_rows if clean_text(row.get("alt_recipe_id"))]
    alt_has_more_carbs = sum(to_float(row.get("alt_carbs_g")) > to_float(row.get("selected_carbs_g")) for row in alt_exists)
    alt_loses_by_score = sum(clean_text(row.get("why_alt_lost_guess")) in {"lower_score", "lower_protein_fit", "lower_kcal_fit", "time_penalty", "slot_penalty", "nutrition_quality_penalty"} for row in alt_exists)
    carb_availability = summarize_carb_availability(distribution_rows)
    top_dominance = dominance_audit_rows[:8]
    recommendation = recommendation_text(
        scenario_count=scenario_count,
        oracle_improved_loss=oracle_improved_loss,
        oracle_improved_carbs=oracle_improved_carbs,
        oracle_valid=oracle_valid,
        current_valid=current_valid,
        alt_has_more_carbs=alt_has_more_carbs,
        alt_loses_by_score=alt_loses_by_score,
        dominance_rows=dominance_audit_rows,
        carb_availability=carb_availability,
    )
    lines = [
        "Generator v1 round14 carb availability and day balance audit",
        "=" * 61,
        "",
        f"dataset_profile: {V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE}",
        f"scenario_count: {scenario_count}",
        f"current_valid_scenarios: {current_valid}",
        f"oracle_valid_scenarios: {oracle_valid}",
        f"oracle_improved_day_loss_scenarios: {oracle_improved_loss}",
        f"oracle_improved_carbs_gap_scenarios: {oracle_improved_carbs}",
        f"selected_slots_with_carb_alternative: {alt_has_more_carbs}/{len(selected_alt_rows)}",
        f"carb_alternatives_losing_by_score_or_fit: {alt_loses_by_score}/{len(alt_exists)}",
        "",
        "Carb availability by slot:",
    ]
    for slot, values in carb_availability.items():
        lines.append(
            "- "
            + slot
            + " | avg_count_carbs_gte_50="
            + round_number(values.get("avg_count_carbs_gte_50"))
            + " | avg_count_meeting_kcal_protein_carbs="
            + round_number(values.get("avg_count_meeting_kpc"))
            + " | max_carbs_seen="
            + round_number(values.get("max_carbs_seen"))
        )
    lines.extend(["", "Most repeated recipes current vs oracle:"])
    for row in top_dominance:
        lines.append(
            "- "
            + clean_text(row.get("recipe_id"))
            + " | "
            + clean_text(row.get("display_name"))
            + " | current="
            + clean_text(row.get("selected_count_current"))
            + " | oracle="
            + clean_text(row.get("selected_count_oracle"))
            + " | "
            + clean_text(row.get("dominance_guess"))
        )
    lines.extend(["", "Decision answers:"])
    lines.append(answer_carb_availability(carb_availability))
    lines.append(answer_scoring_pressure(alt_has_more_carbs, alt_loses_by_score, len(selected_alt_rows)))
    lines.append(answer_oracle(oracle_improved_loss, oracle_improved_carbs, scenario_count))
    lines.append(answer_greedy_bottleneck(oracle_improved_loss, oracle_valid, current_valid))
    lines.append(answer_dataset_gap(carb_availability, oracle_improved_carbs, scenario_count))
    lines.extend(["", "Recommended next step:"])
    lines.append("- " + recommendation)
    lines.extend(
        [
            "",
            "Output files:",
            f"- {OUT_DISTRIBUTION}",
            f"- {OUT_SELECTED_VS_ALT}",
            f"- {OUT_ORACLE_PLANS}",
            f"- {OUT_ORACLE_MEALS}",
            f"- {OUT_DOMINANCE}",
            f"- {OUT_RECOMMENDATION}",
        ]
    )
    recommendation_lines = [
        "Generator v1 round14 next step recommendation",
        "=" * 45,
        "",
        recommendation,
        "",
        "Rationale:",
        f"- oracle_improved_day_loss_scenarios={oracle_improved_loss}/{scenario_count}",
        f"- oracle_improved_carbs_gap_scenarios={oracle_improved_carbs}/{scenario_count}",
        f"- current_valid_scenarios={current_valid}/{scenario_count}",
        f"- oracle_valid_scenarios={oracle_valid}/{scenario_count}",
        f"- selected_slots_with_carb_alternative={alt_has_more_carbs}/{len(selected_alt_rows)}",
        f"- carb_alternatives_losing_by_score_or_fit={alt_loses_by_score}/{len(alt_exists)}",
        "",
    ]
    return "\n".join(lines) + "\n", "\n".join(recommendation_lines)


def summarize_carb_availability(rows: list[dict[str, object]]) -> dict[str, dict[str, float]]:
    by_slot: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_slot.setdefault(clean_text(row.get("slot")), []).append(row)
    summary: dict[str, dict[str, float]] = {}
    for slot, slot_rows in by_slot.items():
        summary[slot] = {
            "avg_count_carbs_gte_50": average_raw(slot_rows, "count_carbs_gte_50"),
            "avg_count_meeting_kpc": average_raw(slot_rows, "count_meeting_kcal_protein_carbs_approx"),
            "max_carbs_seen": max(to_float(row.get("carbs_max")) for row in slot_rows),
        }
    return summary


def average_raw(rows: list[dict[str, object]], column: str) -> float:
    if not rows:
        return 0.0
    return sum(to_float(row.get(column)) for row in rows) / len(rows)


def answer_carb_availability(carb_availability: dict[str, dict[str, float]]) -> str:
    weak_slots = [
        slot
        for slot, values in carb_availability.items()
        if values.get("avg_count_carbs_gte_50", 0.0) < 3
        and slot in {"lunch", "dinner", "snack"}
    ]
    if weak_slots:
        return "- Carb-forward candidates exist, but availability is thin in: " + ", ".join(weak_slots) + "."
    return "- Carb-forward candidates exist in all evaluated slots, at least at candidate-pool level."


def answer_scoring_pressure(alt_has_more_carbs: int, alt_loses_by_score: int, total_slots: int) -> str:
    if total_slots and alt_has_more_carbs >= total_slots // 2 and alt_loses_by_score >= alt_has_more_carbs // 2:
        return "- Carb-forward alternatives often exist but lose under current score/fit pressure."
    return "- Carb-forward alternatives are not consistently present as strong competitors."


def answer_oracle(oracle_improved_loss: int, oracle_improved_carbs: int, scenario_count: int) -> str:
    if oracle_improved_loss >= scenario_count // 2 or oracle_improved_carbs >= scenario_count // 2:
        return "- Oracle finds meaningfully better day-level macro plans from existing candidates."
    return "- Oracle does not find enough improvement from existing candidates alone."


def answer_greedy_bottleneck(oracle_improved_loss: int, oracle_valid: int, current_valid: int) -> str:
    if oracle_improved_loss >= 5 or oracle_valid > current_valid:
        return "- Current greedy day selection is a material bottleneck."
    return "- Greedy selection contributes, but data coverage remains a major bottleneck."


def answer_dataset_gap(
    carb_availability: dict[str, dict[str, float]],
    oracle_improved_carbs: int,
    scenario_count: int,
) -> str:
    lunch_dinner_thin = any(
        carb_availability.get(slot, {}).get("avg_count_meeting_kpc", 0.0) < 2
        for slot in ("lunch", "dinner")
    )
    if lunch_dinner_thin and oracle_improved_carbs < scenario_count // 2:
        return "- Dataset still lacks enough carb-forward complete meals."
    return "- Dataset has some carb-forward options, but selector/objective needs day-level balancing."


def recommendation_text(
    scenario_count: int,
    oracle_improved_loss: int,
    oracle_improved_carbs: int,
    oracle_valid: int,
    current_valid: int,
    alt_has_more_carbs: int,
    alt_loses_by_score: int,
    dominance_rows: list[dict[str, object]],
    carb_availability: dict[str, dict[str, float]],
) -> str:
    repeated_both = sum(
        int(row.get("selected_count_current") or 0) >= 4
        and int(row.get("selected_count_oracle") or 0) >= 4
        for row in dominance_rows
    )
    if oracle_improved_loss >= scenario_count // 2 or oracle_valid > current_valid:
        return "B. day-level macro balancing selector, with F. dynamic portion multipliers evaluated next"
    if oracle_improved_carbs < scenario_count // 2:
        return "C. add more carb-forward complete recipes or D. meal_completion fallback before selector changes"
    if repeated_both >= 2:
        return "C. more data and E. variety/repetition handling"
    if alt_has_more_carbs and alt_loses_by_score >= max(1, alt_has_more_carbs // 2):
        return "A. scoring calibration for carb/kcal pressure"
    return "E. variety/repetition handling plus continued audit"


def main() -> None:
    base_profile = load_member_profile(BASE_PROFILE_PATH)
    fooddb = load_fooddb_current()
    distribution_rows: list[dict[str, object]] = []
    selected_alt_rows: list[dict[str, object]] = []
    oracle_plan_rows: list[dict[str, object]] = []
    oracle_meal_rows: list[dict[str, object]] = []
    current_meal_rows: list[dict[str, object]] = []

    for overrides in scenario_overrides():
        scenario_id = clean_text(overrides["scenario_id"])
        profile = build_scenario_profile(base_profile, overrides)
        flow = run_generator_flow(profile, fooddb)
        target = flow["target"]
        slot_candidates = flow["slot_candidates"]
        current_plan = flow["current_plan"]
        lookup = candidate_lookup(slot_candidates)
        distribution_rows.extend(candidate_distribution_rows(scenario_id, target, slot_candidates))
        selected_alt_rows.extend(selected_vs_alternative_rows(scenario_id, target, slot_candidates, current_plan))
        oracle_plan_row, oracle_meals = oracle_for_scenario(scenario_id, target, slot_candidates, current_plan)
        oracle_plan_rows.append(oracle_plan_row)
        oracle_meal_rows.extend(oracle_meals)
        current_meal_rows.extend(current_meal_rows_from_plan(scenario_id, current_plan, lookup))

    dominance_audit_rows = dominance_rows(current_meal_rows, oracle_meal_rows)
    summary_text, recommendation_text_output = build_summary(
        distribution_rows,
        selected_alt_rows,
        oracle_plan_rows,
        dominance_audit_rows,
    )

    write_csv(OUT_DISTRIBUTION, distribution_rows, DISTRIBUTION_COLUMNS)
    write_csv(OUT_SELECTED_VS_ALT, selected_alt_rows, SELECTED_ALT_COLUMNS)
    write_csv(OUT_ORACLE_PLANS, oracle_plan_rows, ORACLE_PLAN_COLUMNS)
    write_csv(OUT_ORACLE_MEALS, oracle_meal_rows, ORACLE_MEAL_COLUMNS)
    write_csv(OUT_DOMINANCE, dominance_audit_rows, DOMINANCE_COLUMNS)
    OUT_SUMMARY.write_text(summary_text, encoding="utf-8")
    OUT_RECOMMENDATION.write_text(recommendation_text_output, encoding="utf-8")

    print("Generator v1 round14 carb/day balance audit written")
    print(f"scenario_count={len(oracle_plan_rows)}")
    print(
        "current_valid_scenarios="
        + str(sum(clean_text(row.get("current_validation_status")) == "valid" for row in oracle_plan_rows))
    )
    print(
        "oracle_valid_scenarios="
        + str(sum(clean_text(row.get("oracle_validation_status")) == "valid" for row in oracle_plan_rows))
    )
    print(
        "oracle_improved_day_loss_scenarios="
        + str(sum(to_float(row.get("day_loss_improvement")) > 0.02 for row in oracle_plan_rows))
    )
    print(f"written_summary={OUT_SUMMARY}")
    print(f"written_recommendation={OUT_RECOMMENDATION}")


if __name__ == "__main__":
    main()
