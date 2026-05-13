from __future__ import annotations

import heapq
import itertools
import json
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
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
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round25_objective_threshold_summary.txt"
OUT_DAY_CANDIDATES = OUT_DIR / "generator_v1_round25_day_candidate_thresholds.csv"
OUT_THRESHOLD_SENSITIVITY = OUT_DIR / "generator_v1_round25_accept_threshold_sensitivity.csv"
OUT_OBJECTIVE_SENSITIVITY = OUT_DIR / "generator_v1_round25_objective_weight_sensitivity.csv"
OUT_MAIN_ALTERNATIVES = OUT_DIR / "generator_v1_round25_main_meal_alternative_quality.csv"
OUT_RECOMMENDATION = OUT_DIR / "generator_v1_round25_recommendation.txt"

DATASET_PROFILE = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE
PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"
SLOT_ORDER = ["breakfast", "lunch", "dinner", "snack"]
MAX_SLOT_SHORTLIST = 24
COLLECTOR_LIMIT = 450

DOMINANT_RECIPES = {
    "waffles": "recipes_v1_1_candidate_169",
    "cabbage": "recipes_v1_1_candidate_094",
    "veggie_burgers": "recipes_v1_1_candidate_096",
}
DOMINANT_RECIPE_IDS = set(DOMINANT_RECIPES.values())
MAIN_REFERENCE_RECIPES = [
    {
        "recipe_id": "recipes_v1_1_candidate_094",
        "display_name": "Sweet and Sour Stuffed Cabbage",
        "slots": ["lunch", "dinner"],
    },
    {
        "recipe_id": "recipes_v1_1_candidate_096",
        "display_name": "Veggie Burgers",
        "slots": ["lunch", "dinner"],
    },
    {
        "recipe_id": "recipes_v1_1_candidate_020",
        "display_name": "Thai Fried Rice",
        "slots": ["lunch", "dinner"],
    },
    {
        "recipe_id": "recipes_v1_1_candidate_115",
        "display_name": "Ground Turkey Lettuce Wraps",
        "slots": ["lunch", "dinner"],
    },
]

OBJECTIVE_WEIGHTS = {
    "current": {"kcal": 0.35, "protein": 0.30, "carbs": 0.25, "fat": 0.10},
    "carb_boost": {"kcal": 0.30, "protein": 0.25, "carbs": 0.35, "fat": 0.10},
    "balanced_macro": {"kcal": 0.30, "protein": 0.25, "carbs": 0.25, "fat": 0.20},
    "kcal_carbs_focus": {"kcal": 0.40, "protein": 0.20, "carbs": 0.30, "fat": 0.10},
    "protein_less_dominant": {"kcal": 0.35, "protein": 0.20, "carbs": 0.35, "fat": 0.10},
}

THRESHOLD_SETS = {
    "current_demo_safe": {
        "reject_adjusted_day_loss_max": 0.20,
        "reject_base_day_loss_max": 0.18,
        "review_adjusted_day_loss_min": 0.12,
        "kcal_ratio_min": 0.75,
        "kcal_ratio_max": 1.25,
        "protein_ratio_min": 0.75,
        "carbs_ratio_min": 0.60,
        "review_carbs_ratio_min": 0.60,
        "review_carbs_ratio_max": 0.75,
        "main_issue_review_limit": 0,
        "main_issue_reject_min": 2,
        "borderline_is_review": True,
    },
    "slightly_relaxed": {
        "reject_adjusted_day_loss_max": 0.23,
        "reject_base_day_loss_max": 0.21,
        "review_adjusted_day_loss_min": 0.15,
        "kcal_ratio_min": 0.75,
        "kcal_ratio_max": 1.25,
        "protein_ratio_min": 0.75,
        "carbs_ratio_min": 0.55,
        "review_carbs_ratio_min": 0.55,
        "review_carbs_ratio_max": 0.70,
        "main_issue_review_limit": 1,
        "main_issue_reject_min": 2,
        "borderline_is_review": True,
    },
    "macro_strict": {
        "reject_adjusted_day_loss_max": 0.18,
        "reject_base_day_loss_max": 0.16,
        "review_adjusted_day_loss_min": 0.10,
        "kcal_ratio_min": 0.80,
        "kcal_ratio_max": 1.20,
        "protein_ratio_min": 0.80,
        "carbs_ratio_min": 0.70,
        "review_carbs_ratio_min": 0.70,
        "review_carbs_ratio_max": 0.82,
        "main_issue_review_limit": 0,
        "main_issue_reject_min": 2,
        "borderline_is_review": True,
    },
    "realism_strict": {
        "reject_adjusted_day_loss_max": 0.20,
        "reject_base_day_loss_max": 0.18,
        "review_adjusted_day_loss_min": 0.12,
        "kcal_ratio_min": 0.75,
        "kcal_ratio_max": 1.25,
        "protein_ratio_min": 0.75,
        "carbs_ratio_min": 0.60,
        "review_carbs_ratio_min": 0.60,
        "review_carbs_ratio_max": 0.75,
        "main_issue_review_limit": 0,
        "main_issue_reject_min": 1,
        "borderline_is_review": True,
    },
    "multi_day_candidate_friendly": {
        "reject_adjusted_day_loss_max": 0.24,
        "reject_base_day_loss_max": 0.22,
        "review_adjusted_day_loss_min": 0.18,
        "kcal_ratio_min": 0.72,
        "kcal_ratio_max": 1.28,
        "protein_ratio_min": 0.72,
        "carbs_ratio_min": 0.55,
        "review_carbs_ratio_min": 0.55,
        "review_carbs_ratio_max": 0.65,
        "main_issue_review_limit": 1,
        "main_issue_reject_min": 3,
        "borderline_is_review": False,
    },
}

SEVERE_REALISM_FLAGS = {
    "breakfast_too_large",
    "unrealistic_large_portion",
    "snack_too_large",
}
MAIN_REALISM_FLAGS = {
    "low_protein_main",
    "low_carb_main",
    "mostly_carb_meal",
    "mostly_protein_meal",
}
MILD_MAIN_REALISM_FLAGS = {
    "low_protein_main",
    "low_carb_main",
    "mostly_carb_meal",
    "mostly_protein_meal",
}
REVIEW_REALISM_FLAGS = {"borderline_large_portion"}


class TopCollector:
    def __init__(self, limit: int, higher_is_better: bool = True) -> None:
        self.limit = limit
        self.higher_is_better = higher_is_better
        self._items: list[tuple[float, int, dict[str, Any]]] = []
        self._sequence = 0

    def consider(self, record: dict[str, Any], score: float) -> None:
        priority = score if self.higher_is_better else -score
        self._sequence += 1
        if len(self._items) < self.limit:
            item = (priority, self._sequence, compact_record(record))
            heapq.heappush(self._items, item)
            return
        if priority > self._items[0][0]:
            item = (priority, self._sequence, compact_record(record))
            heapq.heapreplace(self._items, item)

    def records(self) -> list[dict[str, Any]]:
        rows = [item[2] for item in self._items]
        return sorted(
            rows,
            key=lambda row: (
                row["current_adjusted_day_loss"],
                row["current_base_day_loss"],
                row["recipe_key"],
            ),
        )


def main() -> None:
    context = build_generation_context()
    target = context["target"]
    target_data = target_to_dict(target)
    slot_candidates = prepare_slot_candidates(
        context["slot_candidates"],
        context["pool"].recipes,
        target,
    )
    shortlists = build_audit_shortlists(slot_candidates)
    audit_result = audit_day_candidates(shortlists, target_data)
    day_candidate_rows = build_day_candidate_rows(audit_result["selected_records"])
    threshold_rows = build_threshold_rows(audit_result["threshold_stats"])
    objective_rows = build_objective_rows(audit_result["objective_top_records"])
    best_record = audit_result["best_current_record"]
    main_alternative_rows = build_main_alternative_rows(
        slot_candidates=slot_candidates,
        best_record=best_record,
        target_data=target_data,
    )
    recommendation_text = build_recommendation_text(
        evaluated_count=audit_result["evaluated_count"],
        threshold_rows=threshold_rows,
        objective_rows=objective_rows,
        main_alternative_rows=main_alternative_rows,
    )
    summary_text = build_summary(
        shortlists=shortlists,
        evaluated_count=audit_result["evaluated_count"],
        rejected_duplicate_count=audit_result["rejected_duplicate_count"],
        threshold_rows=threshold_rows,
        objective_rows=objective_rows,
        main_alternative_rows=main_alternative_rows,
        recommendation_text=recommendation_text,
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(day_candidate_rows).to_csv(OUT_DAY_CANDIDATES, index=False)
    pd.DataFrame(threshold_rows).to_csv(OUT_THRESHOLD_SENSITIVITY, index=False)
    pd.DataFrame(objective_rows).to_csv(OUT_OBJECTIVE_SENSITIVITY, index=False)
    pd.DataFrame(main_alternative_rows).to_csv(OUT_MAIN_ALTERNATIVES, index=False)
    OUT_SUMMARY.write_text(summary_text, encoding="utf-8")
    OUT_RECOMMENDATION.write_text(recommendation_text, encoding="utf-8")

    print("Generator v1 Round25 objective/threshold audit written")
    print(f"summary={OUT_SUMMARY}")
    print(f"day_candidates={OUT_DAY_CANDIDATES}")
    print(f"threshold_sensitivity={OUT_THRESHOLD_SENSITIVITY}")
    print(f"objective_sensitivity={OUT_OBJECTIVE_SENSITIVITY}")
    print(f"main_alternatives={OUT_MAIN_ALTERNATIVES}")
    print(f"recommendation={OUT_RECOMMENDATION}")
    print(
        "evaluated_day_candidates="
        f"{audit_result['evaluated_count']} "
        f"selected_for_csv={len(day_candidate_rows)}"
    )
    for row in threshold_rows:
        print(
            "threshold="
            f"{row['threshold_set']} accept={row['accept_count']} "
            f"review={row['review_count']} reject={row['reject_count']} "
            f"accept_without_dominant={row['accept_without_dominant_count']} "
            f"distinct_lunch_dinner_accept={row['distinct_lunch_dinner_accept_count']}"
        )


def build_generation_context() -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        ingredients_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        nutrition_path=V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        dataset_profile=DATASET_PROFILE,
    )
    fooddb = load_fooddb_current()
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
        portion_policy_mode=PORTION_POLICY,
    )
    return {
        "profile": profile,
        "target": target,
        "pool": pool,
        "slot_candidates": slot_candidates,
    }


def prepare_slot_candidates(
    slot_candidates: pd.DataFrame,
    recipes: pd.DataFrame,
    target: NutritionTarget,
) -> pd.DataFrame:
    family_columns = [
        column
        for column in [
            "recipe_id",
            "recipe_family_name",
            "recipe_cuisine",
            "content_quality_status",
        ]
        if column in recipes.columns
    ]
    prepared = slot_candidates.merge(
        recipes.loc[:, family_columns].drop_duplicates("recipe_id"),
        on="recipe_id",
        how="left",
    )
    if "meal_realism_practical_flags" in prepared.columns:
        prepared["meal_realism_flags"] = prepared["meal_realism_practical_flags"]
    if "meal_realism_practical_penalty" in prepared.columns:
        prepared["meal_realism_penalty"] = prepared["meal_realism_practical_penalty"]
    prepared["slot_usefulness_loss"] = prepared.apply(
        lambda row: slot_candidate_loss(row, target.slot_targets.get(row["slot"], {})),
        axis=1,
    )
    prepared["dominant_recipe_flag"] = prepared["recipe_id"].isin(DOMINANT_RECIPE_IDS)
    prepared["high_quality_candidate"] = prepared.apply(is_high_quality_candidate, axis=1)
    prepared["audit_candidate_status"] = prepared.apply(candidate_quality_status, axis=1)
    prepared["audit_priority"] = prepared.apply(slot_audit_priority, axis=1)
    return prepared


def build_audit_shortlists(slot_candidates: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    shortlists: dict[str, list[dict[str, Any]]] = {}
    for slot in SLOT_ORDER:
        frame = slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        if frame.empty:
            shortlists[slot] = []
            continue
        frame = frame.loc[~frame["realism_hard_reject"].map(to_bool)].copy()
        if frame.empty:
            frame = slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        pieces = [
            top_rows(frame, "slot_usefulness_loss", 18, True),
            top_rows(frame, "score_preview", 18, False),
            top_rows(frame, "macro_fit", 18, False),
            top_rows(frame, "carbs_g", 16, False),
            top_rows(frame, "kcal", 16, False),
            top_rows(frame, "protein_g", 14, False),
            top_rows(frame.loc[~frame["dominant_recipe_flag"]], "slot_usefulness_loss", 20, True),
        ]
        if slot in {"lunch", "dinner"}:
            pieces.append(top_rows(frame.loc[~frame["dominant_recipe_flag"]], "carbs_g", 20, False))
            pieces.append(top_one_per_recipe(frame, "slot_usefulness_loss", True, 28))
        combined = pd.concat(pieces, ignore_index=True)
        combined = dedupe_slot_candidates(combined)
        combined = combined.sort_values(
            ["audit_priority", "slot_usefulness_loss", "recipe_id", "portion_multiplier"],
            ascending=[True, True, True, True],
            kind="mergesort",
        )
        shortlists[slot] = [
            row.to_dict()
            for _, row in combined.head(MAX_SLOT_SHORTLIST).reset_index(drop=True).iterrows()
        ]
    return shortlists


def audit_day_candidates(
    shortlists: Mapping[str, list[dict[str, Any]]],
    target_data: Mapping[str, Any],
) -> dict[str, Any]:
    threshold_stats = {
        name: empty_threshold_stats()
        for name in THRESHOLD_SETS
    }
    collectors: dict[str, TopCollector] = {
        "current_adjusted": TopCollector(COLLECTOR_LIMIT, higher_is_better=False),
        "current_base": TopCollector(COLLECTOR_LIMIT, higher_is_better=False),
        "kcal_ratio": TopCollector(COLLECTOR_LIMIT, higher_is_better=False),
        "carbs_ratio": TopCollector(COLLECTOR_LIMIT, higher_is_better=False),
        "lower_repetition": TopCollector(COLLECTOR_LIMIT, higher_is_better=True),
        "lunch_dinner_diversity": TopCollector(COLLECTOR_LIMIT, higher_is_better=True),
    }
    for variant in OBJECTIVE_WEIGHTS:
        collectors[f"objective_{variant}"] = TopCollector(120, higher_is_better=False)

    evaluated_count = 0
    rejected_duplicate_count = 0
    slot_rows = [shortlists.get(slot, []) for slot in SLOT_ORDER]
    for combination in itertools.product(*slot_rows):
        recipe_ids = [clean_text(row.get("recipe_id")) for row in combination]
        if len(set(recipe_ids)) != len(recipe_ids):
            rejected_duplicate_count += 1
            continue
        record = build_day_record(combination, target_data)
        evaluated_count += 1
        for threshold_name, threshold_config in THRESHOLD_SETS.items():
            status, reasons = classify_threshold(record, threshold_config)
            update_threshold_stats(
                threshold_stats[threshold_name],
                record,
                status,
                reasons,
            )
        collectors["current_adjusted"].consider(record, record["current_adjusted_day_loss"])
        collectors["current_base"].consider(record, record["current_base_day_loss"])
        collectors["kcal_ratio"].consider(record, abs(1.0 - record["kcal_ratio"]))
        collectors["carbs_ratio"].consider(record, abs(1.0 - record["carbs_ratio"]))
        collectors["lower_repetition"].consider(
            record,
            -10.0 * record["dominant_recipe_count"] - record["current_adjusted_day_loss"],
        )
        collectors["lunch_dinner_diversity"].consider(
            record,
            -10.0 * record["dominant_main_recipe_count"]
            - record["current_adjusted_day_loss"],
        )
        for variant_name in OBJECTIVE_WEIGHTS:
            collectors[f"objective_{variant_name}"].consider(
                record,
                record["objective_losses"][variant_name]["adjusted_day_loss"],
            )

    selected_records = union_records(collectors)
    objective_top_records = {
        variant: collectors[f"objective_{variant}"].records()[:10]
        for variant in OBJECTIVE_WEIGHTS
    }
    best_current = collectors["objective_current"].records()[0]
    finalize_threshold_stats(threshold_stats)
    return {
        "evaluated_count": evaluated_count,
        "rejected_duplicate_count": rejected_duplicate_count,
        "selected_records": selected_records,
        "threshold_stats": threshold_stats,
        "objective_top_records": objective_top_records,
        "best_current_record": best_current,
    }


def build_day_record(
    meals: Sequence[Mapping[str, Any]],
    target_data: Mapping[str, Any],
) -> dict[str, Any]:
    meal_rows = [compact_meal(row) for row in meals]
    objective_losses = {
        name: compute_day_loss(meal_rows, target_data, weights)
        for name, weights in OBJECTIVE_WEIGHTS.items()
    }
    current_loss = objective_losses["current"]
    realism_penalty = average_realism_penalty(meal_rows)
    totals = day_totals(meal_rows)
    ratios = macro_ratios(totals, target_data)
    validation_status = validation_status_for_record(meal_rows, totals, target_data)
    realism = realism_issue_data(meal_rows)
    recipe_ids = [meal["recipe_id"] for meal in meal_rows]
    slots = {meal["slot"]: meal for meal in meal_rows}
    contains_waffles = DOMINANT_RECIPES["waffles"] in recipe_ids
    contains_cabbage = DOMINANT_RECIPES["cabbage"] in recipe_ids
    contains_veggie_burgers = DOMINANT_RECIPES["veggie_burgers"] in recipe_ids
    dominant_recipe_count = sum(
        [
            contains_waffles,
            contains_cabbage,
            contains_veggie_burgers,
        ]
    )
    dominant_main_recipe_count = sum(
        meal["recipe_id"] in DOMINANT_RECIPE_IDS
        for meal in [slots.get("lunch", {}), slots.get("dinner", {})]
    )
    return {
        "recipe_key": "|".join(
            f"{meal['slot']}:{meal['recipe_id']}:{meal['portion_multiplier']:.4f}"
            for meal in meal_rows
        ),
        "selected_recipe_ids": recipe_ids,
        "selected_recipe_names": [meal["display_name"] for meal in meal_rows],
        "meal_rows": meal_rows,
        "objective_losses": objective_losses,
        "current_base_day_loss": round(current_loss["day_loss"], 6),
        "current_adjusted_day_loss": round(
            current_loss["day_loss"] + realism_penalty,
            6,
        ),
        "kcal_loss": round(current_loss["kcal_loss"], 6),
        "protein_loss": round(current_loss["protein_loss"], 6),
        "carbs_loss": round(current_loss["carbs_loss"], 6),
        "fat_loss": round(current_loss["fat_loss"], 6),
        "meal_realism_applied_penalty": round(realism_penalty, 6),
        "validation_status": validation_status,
        "total_kcal": totals["total_kcal"],
        "total_protein_g": totals["total_protein_g"],
        "total_carbs_g": totals["total_carbs_g"],
        "total_fat_g": totals["total_fat_g"],
        "effective_time_min_sum": totals["effective_time_min_sum"],
        "kcal_ratio": ratios["kcal_ratio"],
        "protein_ratio": ratios["protein_ratio"],
        "carbs_ratio": ratios["carbs_ratio"],
        "fat_ratio": ratios["fat_ratio"],
        "meal_realism_flags": sorted(realism["all_flags"]),
        "severe_realism_issue_count": realism["severe_realism_issue_count"],
        "main_issue_count": realism["main_issue_count"],
        "borderline_large_portion_count": realism["borderline_large_portion_count"],
        "contains_waffles": contains_waffles,
        "contains_cabbage": contains_cabbage,
        "contains_veggie_burgers": contains_veggie_burgers,
        "dominant_recipe_count": dominant_recipe_count,
        "dominant_main_recipe_count": dominant_main_recipe_count,
        "breakfast_recipe_id": slots.get("breakfast", {}).get("recipe_id", ""),
        "breakfast_recipe": slots.get("breakfast", {}).get("display_name", ""),
        "lunch_recipe_id": slots.get("lunch", {}).get("recipe_id", ""),
        "lunch_recipe": slots.get("lunch", {}).get("display_name", ""),
        "dinner_recipe_id": slots.get("dinner", {}).get("recipe_id", ""),
        "dinner_recipe": slots.get("dinner", {}).get("display_name", ""),
        "snack_recipe_id": slots.get("snack", {}).get("recipe_id", ""),
        "snack_recipe": slots.get("snack", {}).get("display_name", ""),
    }


def compact_record(record: Mapping[str, Any]) -> dict[str, Any]:
    keep_fields = [
        "recipe_key",
        "selected_recipe_ids",
        "selected_recipe_names",
        "meal_rows",
        "objective_losses",
        "current_base_day_loss",
        "current_adjusted_day_loss",
        "kcal_loss",
        "protein_loss",
        "carbs_loss",
        "fat_loss",
        "meal_realism_applied_penalty",
        "validation_status",
        "total_kcal",
        "total_protein_g",
        "total_carbs_g",
        "total_fat_g",
        "effective_time_min_sum",
        "kcal_ratio",
        "protein_ratio",
        "carbs_ratio",
        "fat_ratio",
        "meal_realism_flags",
        "severe_realism_issue_count",
        "main_issue_count",
        "borderline_large_portion_count",
        "contains_waffles",
        "contains_cabbage",
        "contains_veggie_burgers",
        "dominant_recipe_count",
        "dominant_main_recipe_count",
        "breakfast_recipe_id",
        "breakfast_recipe",
        "lunch_recipe_id",
        "lunch_recipe",
        "dinner_recipe_id",
        "dinner_recipe",
        "snack_recipe_id",
        "snack_recipe",
    ]
    return {field: record[field] for field in keep_fields}


def build_day_candidate_rows(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for rank, record in enumerate(records, start=1):
        threshold_results = {
            name: classify_threshold(record, config)[0]
            for name, config in THRESHOLD_SETS.items()
        }
        current_status, current_reasons = classify_threshold(
            record,
            THRESHOLD_SETS["current_demo_safe"],
        )
        rows.append(
            {
                "audit_rank": rank,
                "recipe_key": record["recipe_key"],
                "day_loss": record["current_base_day_loss"],
                "adjusted_day_loss": record["current_adjusted_day_loss"],
                "kcal_loss": record["kcal_loss"],
                "protein_loss": record["protein_loss"],
                "carbs_loss": record["carbs_loss"],
                "fat_loss": record["fat_loss"],
                "validation_status": record["validation_status"],
                "quality_gate_status": current_status,
                "candidate_class": current_status,
                "quality_gate_reasons": ";".join(current_reasons),
                "meal_realism_flags": ";".join(record["meal_realism_flags"]),
                "contains_waffles": record["contains_waffles"],
                "contains_cabbage": record["contains_cabbage"],
                "contains_veggie_burgers": record["contains_veggie_burgers"],
                "dominant_recipe_count": record["dominant_recipe_count"],
                "lunch_recipe_id": record["lunch_recipe_id"],
                "lunch_recipe": record["lunch_recipe"],
                "dinner_recipe_id": record["dinner_recipe_id"],
                "dinner_recipe": record["dinner_recipe"],
                "breakfast_recipe": record["breakfast_recipe"],
                "snack_recipe": record["snack_recipe"],
                "selected_recipe_ids": "|".join(record["selected_recipe_ids"]),
                "selected_recipe_names": " | ".join(record["selected_recipe_names"]),
                "total_kcal": record["total_kcal"],
                "total_protein_g": record["total_protein_g"],
                "total_carbs_g": record["total_carbs_g"],
                "total_fat_g": record["total_fat_g"],
                "effective_time_min_sum": record["effective_time_min_sum"],
                "kcal_ratio": round(record["kcal_ratio"], 4),
                "protein_ratio": round(record["protein_ratio"], 4),
                "carbs_ratio": round(record["carbs_ratio"], 4),
                "fat_ratio": round(record["fat_ratio"], 4),
                **{
                    f"{name}_status": status
                    for name, status in threshold_results.items()
                },
            }
        )
    return rows


def build_threshold_rows(
    threshold_stats: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for name, stats in threshold_stats.items():
        accept_count = int(stats["status_counts"].get("accept", 0))
        rows.append(
            {
                "threshold_set": name,
                "accept_count": accept_count,
                "review_count": int(stats["status_counts"].get("review", 0)),
                "reject_count": int(stats["status_counts"].get("reject", 0)),
                "accept_without_dominant_count": int(stats["accept_without_dominant_count"]),
                "accept_without_dominant_share": safe_ratio(
                    stats["accept_without_dominant_count"],
                    accept_count,
                ),
                "distinct_lunch_dinner_accept_count": len(stats["accept_lunch_dinner_pairs"]),
                "distinct_lunch_accept_count": len(stats["accept_lunch_ids"]),
                "distinct_dinner_accept_count": len(stats["accept_dinner_ids"]),
                "average_day_loss_of_accept": round(
                    safe_ratio(stats["accept_day_loss_sum"], accept_count),
                    6,
                ),
                "average_adjusted_day_loss_of_accept": round(
                    safe_ratio(stats["accept_adjusted_day_loss_sum"], accept_count),
                    6,
                ),
                "worst_realism_issue_among_accept": worst_realism_label(
                    stats["accept_realism_flags"],
                ),
                "accept_with_waffles_count": int(stats["accept_with_waffles_count"]),
                "accept_with_cabbage_count": int(stats["accept_with_cabbage_count"]),
                "accept_with_veggie_burgers_count": int(
                    stats["accept_with_veggie_burgers_count"]
                ),
                "top_reject_reasons": format_counter(stats["reject_reasons"]),
                "top_review_reasons": format_counter(stats["review_reasons"]),
            }
        )
    return rows


def build_objective_rows(
    objective_top_records: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    rows = []
    for variant, records in objective_top_records.items():
        for rank, record in enumerate(records[:10], start=1):
            loss = record["objective_losses"][variant]
            status, reasons = classify_threshold(record, THRESHOLD_SETS["current_demo_safe"])
            rows.append(
                {
                    "objective_variant": variant,
                    "rank": rank,
                    "variant_adjusted_day_loss": round(loss["adjusted_day_loss"], 6),
                    "variant_base_day_loss": round(loss["day_loss"], 6),
                    "current_adjusted_day_loss": record["current_adjusted_day_loss"],
                    "quality_gate_status_current_thresholds": status,
                    "quality_gate_reasons": ";".join(reasons),
                    "dominant_recipe_count": record["dominant_recipe_count"],
                    "contains_waffles": record["contains_waffles"],
                    "contains_cabbage": record["contains_cabbage"],
                    "contains_veggie_burgers": record["contains_veggie_burgers"],
                    "lunch_recipe_id": record["lunch_recipe_id"],
                    "lunch_recipe": record["lunch_recipe"],
                    "dinner_recipe_id": record["dinner_recipe_id"],
                    "dinner_recipe": record["dinner_recipe"],
                    "selected_recipe_ids": "|".join(record["selected_recipe_ids"]),
                    "selected_recipe_names": " | ".join(record["selected_recipe_names"]),
                    "kcal_ratio": round(record["kcal_ratio"], 4),
                    "protein_ratio": round(record["protein_ratio"], 4),
                    "carbs_ratio": round(record["carbs_ratio"], 4),
                    "fat_ratio": round(record["fat_ratio"], 4),
                    "macro_day_loss": round(loss["macro_day_loss"], 6),
                    "meal_realism_flags": ";".join(record["meal_realism_flags"]),
                    "lunch_dinner_pair": (
                        f"{record['lunch_recipe_id']}|{record['dinner_recipe_id']}"
                    ),
                    "dominant_recipes_still_win": record["dominant_recipe_count"] > 0,
                }
            )
    return rows


def build_main_alternative_rows(
    slot_candidates: pd.DataFrame,
    best_record: Mapping[str, Any],
    target_data: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for reference in MAIN_REFERENCE_RECIPES:
        for slot in reference["slots"]:
            slot_frame = slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
            if slot_frame.empty:
                continue
            reference_rows = slot_frame.loc[
                slot_frame["recipe_id"].eq(reference["recipe_id"])
            ].copy()
            if reference_rows.empty:
                continue
            reference_row = (
                reference_rows.sort_values(
                    ["slot_usefulness_loss", "score_preview"],
                    ascending=[True, False],
                    kind="mergesort",
                )
                .iloc[0]
                .to_dict()
            )
            alternatives = (
                slot_frame.loc[
                    ~slot_frame["recipe_id"].eq(reference["recipe_id"])
                    & ~slot_frame["realism_hard_reject"].map(to_bool)
                ]
                .sort_values(
                    ["slot_usefulness_loss", "score_preview", "recipe_id"],
                    ascending=[True, False, True],
                    kind="mergesort",
                )
                .drop_duplicates("recipe_id", keep="first")
                .head(20)
            )
            for rank, (_, alternative) in enumerate(alternatives.iterrows(), start=1):
                alternative_row = alternative.to_dict()
                swapped = swap_meal(best_record["meal_rows"], slot, alternative_row)
                duplicate_after_swap = has_duplicate_recipes(swapped)
                swapped_record = build_day_record(swapped, target_data)
                status, reasons = classify_threshold(
                    swapped_record,
                    THRESHOLD_SETS["current_demo_safe"],
                )
                rows.append(
                    {
                        "reference_recipe_id": reference["recipe_id"],
                        "reference_display_name": reference["display_name"],
                        "slot": slot,
                        "alternative_rank": rank,
                        "alternative_recipe_id": alternative_row.get("recipe_id"),
                        "alternative_display_name": alternative_row.get("display_name"),
                        "alternative_quality_status": alternative_row.get("audit_candidate_status"),
                        "macro_gap_vs_slot_target": macro_gap_label(
                            alternative_row,
                            target_data.get("slot_targets", {}).get(slot, {}),
                        ),
                        "swap_day_loss_impact": round(
                            swapped_record["current_adjusted_day_loss"]
                            - best_record["current_adjusted_day_loss"],
                            6,
                        ),
                        "swap_quality_gate_status": status,
                        "swap_quality_gate_reasons": ";".join(reasons),
                        "alternative_realism_flags": ";".join(
                            reason_items(alternative_row.get("meal_realism_flags"))
                        ),
                        "alternative_kcal": round(to_float(alternative_row.get("kcal")), 2),
                        "alternative_protein_g": round(
                            to_float(alternative_row.get("protein_g")),
                            2,
                        ),
                        "alternative_carbs_g": round(
                            to_float(alternative_row.get("carbs_g")),
                            2,
                        ),
                        "alternative_fat_g": round(to_float(alternative_row.get("fat_g")), 2),
                        "reference_kcal": round(to_float(reference_row.get("kcal")), 2),
                        "reference_protein_g": round(
                            to_float(reference_row.get("protein_g")),
                            2,
                        ),
                        "reference_carbs_g": round(
                            to_float(reference_row.get("carbs_g")),
                            2,
                        ),
                        "reference_fat_g": round(to_float(reference_row.get("fat_g")), 2),
                        "why_it_loses": why_alternative_loses(
                            reference_row,
                            alternative_row,
                            target_data.get("slot_targets", {}).get(slot, {}),
                            duplicate_after_swap,
                            status,
                        ),
                    }
                )
    return rows


def build_summary(
    shortlists: Mapping[str, Sequence[Mapping[str, Any]]],
    evaluated_count: int,
    rejected_duplicate_count: int,
    threshold_rows: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    main_alternative_rows: Sequence[Mapping[str, Any]],
    recommendation_text: str,
) -> str:
    lines = [
        "Generator v1 Round25 day-level objective and accept-threshold audit",
        "",
        f"dataset_profile={DATASET_PROFILE}",
        "profile=profiles/member_profile_demo_v1.json",
        "selection_mode=balanced_day audit pool",
        f"portion_policy={PORTION_POLICY}",
        f"meal_realism_mode={MEAL_REALISM_MODE}",
        "",
        "Output files",
        f"day_candidate_thresholds={OUT_DAY_CANDIDATES}",
        f"accept_threshold_sensitivity={OUT_THRESHOLD_SENSITIVITY}",
        f"objective_weight_sensitivity={OUT_OBJECTIVE_SENSITIVITY}",
        f"main_meal_alternative_quality={OUT_MAIN_ALTERNATIVES}",
        f"recommendation={OUT_RECOMMENDATION}",
        "",
        "Audit pool",
        f"evaluated_day_candidates={evaluated_count}",
        f"rejected_duplicate_recipe_combinations={rejected_duplicate_count}",
    ]
    for slot in SLOT_ORDER:
        lines.append(f"slot={slot} audit_shortlist={len(shortlists.get(slot, []))}")

    lines.extend(["", "Threshold sensitivity"])
    for row in threshold_rows:
        lines.append(
            (
                f"threshold={row['threshold_set']} "
                f"accept={row['accept_count']} "
                f"review={row['review_count']} "
                f"reject={row['reject_count']} "
                f"accept_without_dominant={row['accept_without_dominant_count']} "
                f"distinct_lunch_dinner_accept={row['distinct_lunch_dinner_accept_count']} "
                f"avg_accept_loss={row['average_adjusted_day_loss_of_accept']} "
                f"worst_accept_realism={row['worst_realism_issue_among_accept']}"
            )
        )

    lines.extend(["", "Objective top-10 summaries"])
    for variant in OBJECTIVE_WEIGHTS:
        variant_rows = [row for row in objective_rows if row["objective_variant"] == variant]
        dominant_top = sum(1 for row in variant_rows if int(row["dominant_recipe_count"]) > 0)
        unique_lunch = len({row["lunch_recipe_id"] for row in variant_rows})
        unique_dinner = len({row["dinner_recipe_id"] for row in variant_rows})
        accept_top = sum(
            1
            for row in variant_rows
            if row["quality_gate_status_current_thresholds"] == "accept"
        )
        lines.append(
            (
                f"objective={variant} top10_accept={accept_top} "
                f"top10_with_dominant={dominant_top} "
                f"unique_lunch={unique_lunch} unique_dinner={unique_dinner}"
            )
        )

    lines.extend(["", "Main meal alternatives"])
    grouped = Counter()
    status_counts = Counter()
    for row in main_alternative_rows:
        grouped[(row["reference_display_name"], row["slot"])] += 1
        status_counts[row["swap_quality_gate_status"]] += 1
    for (display_name, slot), count in grouped.items():
        rows = [
            row
            for row in main_alternative_rows
            if row["reference_display_name"] == display_name and row["slot"] == slot
        ]
        close_rows = [
            row
            for row in rows
            if to_float(row["swap_day_loss_impact"]) <= 0.08
            and row["swap_quality_gate_status"] != "reject"
        ]
        lines.append(
            (
                f"reference={display_name} slot={slot} alternatives={count} "
                f"close_non_reject={len(close_rows)}"
            )
        )
    lines.append(f"swap_status_counts={dict(status_counts)}")

    lines.extend(["", "Recommendation", recommendation_text.strip()])
    return "\n".join(lines) + "\n"


def build_recommendation_text(
    evaluated_count: int,
    threshold_rows: Sequence[Mapping[str, Any]],
    objective_rows: Sequence[Mapping[str, Any]],
    main_alternative_rows: Sequence[Mapping[str, Any]],
) -> str:
    by_threshold = {row["threshold_set"]: row for row in threshold_rows}
    current = by_threshold["current_demo_safe"]
    relaxed = by_threshold["slightly_relaxed"]
    friendly = by_threshold["multi_day_candidate_friendly"]

    current_accept = int(current["accept_count"])
    relaxed_accept = int(relaxed["accept_count"])
    friendly_accept = int(friendly["accept_count"])
    current_non_dominant = int(current["accept_without_dominant_count"])
    friendly_non_dominant = int(friendly["accept_without_dominant_count"])

    current_objective_rows = [
        row for row in objective_rows if row["objective_variant"] == "current"
    ]
    best_variant_name = "current"
    best_variant_dominant = 999
    best_variant_accept = -1
    for variant in OBJECTIVE_WEIGHTS:
        rows = [row for row in objective_rows if row["objective_variant"] == variant]
        dominant_count = sum(1 for row in rows if int(row["dominant_recipe_count"]) > 0)
        accept_count = sum(
            1 for row in rows if row["quality_gate_status_current_thresholds"] == "accept"
        )
        if (dominant_count, -accept_count, variant) < (
            best_variant_dominant,
            -best_variant_accept,
            best_variant_name,
        ):
            best_variant_name = variant
            best_variant_dominant = dominant_count
            best_variant_accept = accept_count

    current_top_dominant = sum(
        1 for row in current_objective_rows if int(row["dominant_recipe_count"]) > 0
    )
    objective_helpful = best_variant_name != "current" and best_variant_dominant < current_top_dominant
    close_non_reject_swaps = [
        row
        for row in main_alternative_rows
        if to_float(row["swap_day_loss_impact"]) <= 0.08
        and row["swap_quality_gate_status"] != "reject"
    ]
    threshold_helpful = (
        friendly_accept > current_accept
        and friendly_non_dominant > current_non_dominant
    )
    recipe_need = (
        int(current["distinct_lunch_accept_count"]) <= 4
        or int(current["distinct_dinner_accept_count"]) <= 5
    )

    answers = [
        "Round25 recommendation",
        "",
        f"Evaluated day candidates: {evaluated_count}",
        "",
        "1. Can we improve multi-day variety by relaxing thresholds safely?",
    ]
    if threshold_helpful:
        answers.append(
            "Yes, but only as a multi-day candidate policy, not by pretending weak days are clean accepts."
        )
    else:
        answers.append("No clear safe threshold-only fix was found.")
    answers.extend(
        [
            (
                f"Current accepts={current_accept}, accepts without dominant recipes="
                f"{current_non_dominant}."
            ),
            (
                f"Multi-day friendly accepts={friendly_accept}, accepts without dominant "
                f"recipes={friendly_non_dominant}."
            ),
            "",
            "2. Can we improve variety by changing objective weights safely?",
        ]
    )
    if objective_helpful:
        answers.append(
            f"Possibly. Best audit-only variant is {best_variant_name}, with fewer dominant recipes in the top 10."
        )
    else:
        answers.append(
            "Not enough. The top plans remain concentrated around the same dominant recipes."
        )
    answers.extend(
        [
            "",
            "3. Are lunch/dinner alternatives good enough but overly penalized?",
        ]
    )
    if len(close_non_reject_swaps) >= 8:
        answers.append(
            "Some are close enough for review-level diversity, but not enough are clean accept replacements."
        )
    else:
        answers.append(
            "Mostly no. Many alternatives still lose on macro fit, carbs/protein, or quality status."
        )
    answers.extend(
        [
            "",
            "4. Do we need more high-quality lunch/dinner recipes?",
        ]
    )
    if recipe_need:
        answers.append(
            "Yes, likely +10 targeted complete lunch/dinner recipes would help, but after threshold/objective calibration."
        )
    else:
        answers.append(
            "Not immediately by count; the first issue is how candidates are accepted and ranked."
        )

    next_steps = []
    if threshold_helpful:
        next_steps.append("C. implement multi-day candidate-friendly acceptance")
        next_steps.append("A. adjust quality gate thresholds")
    if objective_helpful:
        next_steps.append("B. adjust day_loss objective weights")
    if recipe_need:
        next_steps.append("D. add +10/+25 lunch/dinner complete recipes")
    next_steps.append("F. accept current multi-day as technical demo only until calibrated")

    answers.extend(
        [
            "",
            "5. Recommended next step:",
            "; ".join(dict.fromkeys(next_steps)),
            "",
            "Strict assessment:",
            (
                "Do not call the current multi-day plan good. It remains a technical demo "
                "until accept/review handling and lunch/dinner diversity are improved."
            ),
        ]
    )
    return "\n".join(answers) + "\n"


def classify_threshold(
    record: Mapping[str, Any],
    config: Mapping[str, Any],
) -> tuple[str, list[str]]:
    reject_reasons: list[str] = []
    review_reasons: list[str] = []
    if record["validation_status"] != "valid":
        reject_reasons.append(f"validation_status_not_valid:{record['validation_status']}")
    if record["current_adjusted_day_loss"] > config["reject_adjusted_day_loss_max"]:
        reject_reasons.append(
            f"adjusted_day_loss_over_{config['reject_adjusted_day_loss_max']}"
        )
    elif record["current_adjusted_day_loss"] >= config["review_adjusted_day_loss_min"]:
        review_reasons.append(
            f"adjusted_day_loss_over_review_{config['review_adjusted_day_loss_min']}"
        )
    if record["current_base_day_loss"] > config["reject_base_day_loss_max"]:
        reject_reasons.append(f"base_day_loss_over_{config['reject_base_day_loss_max']}")
    if record["kcal_ratio"] < config["kcal_ratio_min"]:
        reject_reasons.append(f"kcal_ratio_under_{config['kcal_ratio_min']}")
    if record["kcal_ratio"] > config["kcal_ratio_max"]:
        reject_reasons.append(f"kcal_ratio_over_{config['kcal_ratio_max']}")
    if record["protein_ratio"] < config["protein_ratio_min"]:
        reject_reasons.append(f"protein_ratio_under_{config['protein_ratio_min']}")
    if record["carbs_ratio"] < config["carbs_ratio_min"]:
        reject_reasons.append(f"carbs_ratio_under_{config['carbs_ratio_min']}")
    elif (
        config["review_carbs_ratio_min"]
        <= record["carbs_ratio"]
        < config["review_carbs_ratio_max"]
    ):
        review_reasons.append(
            f"carbs_ratio_between_{config['review_carbs_ratio_min']}_and_{config['review_carbs_ratio_max']}"
        )
    if record["severe_realism_issue_count"] > 0:
        reject_reasons.append("severe_realism_issue_present")
    if record["main_issue_count"] >= config["main_issue_reject_min"]:
        reject_reasons.append("main_realism_issue_reject")
    elif record["main_issue_count"] > config["main_issue_review_limit"]:
        review_reasons.append("main_realism_issue_review")
    if config["borderline_is_review"] and record["borderline_large_portion_count"] > 0:
        review_reasons.append("borderline_large_portion_present")

    if reject_reasons:
        return "reject", reject_reasons + review_reasons
    if review_reasons:
        return "review", review_reasons
    return "accept", ["quality_gate_passed"]


def empty_threshold_stats() -> dict[str, Any]:
    return {
        "status_counts": Counter(),
        "accept_without_dominant_count": 0,
        "accept_with_waffles_count": 0,
        "accept_with_cabbage_count": 0,
        "accept_with_veggie_burgers_count": 0,
        "accept_day_loss_sum": 0.0,
        "accept_adjusted_day_loss_sum": 0.0,
        "accept_lunch_dinner_pairs": set(),
        "accept_lunch_ids": set(),
        "accept_dinner_ids": set(),
        "accept_realism_flags": Counter(),
        "reject_reasons": Counter(),
        "review_reasons": Counter(),
    }


def update_threshold_stats(
    stats: dict[str, Any],
    record: Mapping[str, Any],
    status: str,
    reasons: Sequence[str],
) -> None:
    stats["status_counts"][status] += 1
    if status == "reject":
        stats["reject_reasons"].update(reasons)
        return
    if status == "review":
        stats["review_reasons"].update(reasons)
        return
    if record["dominant_recipe_count"] == 0:
        stats["accept_without_dominant_count"] += 1
    if record["contains_waffles"]:
        stats["accept_with_waffles_count"] += 1
    if record["contains_cabbage"]:
        stats["accept_with_cabbage_count"] += 1
    if record["contains_veggie_burgers"]:
        stats["accept_with_veggie_burgers_count"] += 1
    stats["accept_day_loss_sum"] += to_float(record["current_base_day_loss"])
    stats["accept_adjusted_day_loss_sum"] += to_float(record["current_adjusted_day_loss"])
    stats["accept_lunch_dinner_pairs"].add(
        (record["lunch_recipe_id"], record["dinner_recipe_id"])
    )
    stats["accept_lunch_ids"].add(record["lunch_recipe_id"])
    stats["accept_dinner_ids"].add(record["dinner_recipe_id"])
    stats["accept_realism_flags"].update(record["meal_realism_flags"])


def finalize_threshold_stats(threshold_stats: Mapping[str, dict[str, Any]]) -> None:
    for stats in threshold_stats.values():
        for status in ["accept", "review", "reject"]:
            stats["status_counts"].setdefault(status, 0)


def compute_day_loss(
    meals: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any],
    weights: Mapping[str, float],
) -> dict[str, float]:
    totals = day_totals(meals)
    kcal_loss = absolute_ratio_loss(totals["total_kcal"], target.get("kcal"))
    protein_loss_value = protein_loss(totals["total_protein_g"], target.get("protein_g"))
    carbs_loss = absolute_ratio_loss(totals["total_carbs_g"], target.get("carbs_g"))
    fat_loss = absolute_ratio_loss(totals["total_fat_g"], target.get("fat_g"))
    macro_day_loss = (
        weights["kcal"] * kcal_loss
        + weights["protein"] * protein_loss_value
        + weights["carbs"] * carbs_loss
        + weights["fat"] * fat_loss
    )
    effective_time = totals["effective_time_min_sum"]
    average_score = average_score_preview(meals)
    time_penalty = time_penalty_for_effective_time(effective_time)
    slot_suspicious_penalty = 0.05 * sum(
        1 for meal in meals if to_bool(meal.get("is_slot_suspicious"))
    )
    long_passive_penalty = 0.01 * min(
        2,
        sum(1 for meal in meals if to_bool(meal.get("has_long_passive_time"))),
    )
    score_penalty = max(0.0, 0.70 - average_score) * 0.03
    day_loss = (
        macro_day_loss
        + time_penalty
        + slot_suspicious_penalty
        + long_passive_penalty
        + score_penalty
    )
    realism_penalty = average_realism_penalty(meals)
    return {
        "day_loss": round(day_loss, 6),
        "adjusted_day_loss": round(day_loss + realism_penalty, 6),
        "macro_day_loss": round(macro_day_loss, 6),
        "kcal_loss": round(kcal_loss, 6),
        "protein_loss": round(protein_loss_value, 6),
        "carbs_loss": round(carbs_loss, 6),
        "fat_loss": round(fat_loss, 6),
        "time_penalty": round(time_penalty, 6),
        "slot_suspicious_penalty": round(slot_suspicious_penalty, 6),
        "long_passive_penalty": round(long_passive_penalty, 6),
        "score_penalty": round(score_penalty, 6),
        "average_score_preview": round(average_score, 6),
        "effective_time_min_sum": round(effective_time, 6),
    }


def compact_meal(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "slot": clean_text(row.get("slot")),
        "recipe_id": clean_text(row.get("recipe_id")),
        "display_name": clean_text(row.get("display_name")),
        "recipe_family_name": clean_text(row.get("recipe_family_name")),
        "portion_multiplier": to_float(row.get("portion_multiplier")),
        "portion_grams_estimated": to_float(row.get("portion_grams_estimated")),
        "kcal": to_float(row.get("kcal")),
        "protein_g": to_float(row.get("protein_g")),
        "carbs_g": to_float(row.get("carbs_g")),
        "fat_g": to_float(row.get("fat_g")),
        "score_preview": to_float(row.get("score_preview")),
        "macro_fit": to_float(row.get("macro_fit")),
        "kcal_fit": to_float(row.get("kcal_fit")),
        "protein_fit": to_float(row.get("protein_fit")),
        "carbs_fit": to_float(row.get("carbs_fit")),
        "fat_fit": to_float(row.get("fat_fit")),
        "time_fit": to_float(row.get("time_fit")),
        "effective_time_min_for_scoring": to_float(
            row.get("effective_time_min_for_scoring")
        ),
        "total_time_min": to_float(row.get("total_time_min")),
        "passive_time_estimated_min": to_float(row.get("passive_time_estimated_min")),
        "has_long_passive_time": to_bool(row.get("has_long_passive_time")),
        "is_slot_suspicious": to_bool(row.get("is_slot_suspicious")),
        "is_nutrition_suspicious": to_bool(row.get("is_nutrition_suspicious")),
        "meal_realism_penalty": to_float(row.get("meal_realism_penalty")),
        "meal_realism_flags": reason_items(row.get("meal_realism_flags")),
        "realism_hard_reject": to_bool(row.get("realism_hard_reject")),
        "slot_usefulness_loss": to_float(row.get("slot_usefulness_loss")),
        "audit_candidate_status": clean_text(row.get("audit_candidate_status")),
    }


def day_totals(meals: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    return {
        "total_kcal": round(sum(to_float(meal.get("kcal")) for meal in meals), 1),
        "total_protein_g": round(sum(to_float(meal.get("protein_g")) for meal in meals), 1),
        "total_carbs_g": round(sum(to_float(meal.get("carbs_g")) for meal in meals), 1),
        "total_fat_g": round(sum(to_float(meal.get("fat_g")) for meal in meals), 1),
        "effective_time_min_sum": round(
            sum(to_float(meal.get("effective_time_min_for_scoring")) for meal in meals),
            1,
        ),
    }


def macro_ratios(
    totals: Mapping[str, Any],
    target: Mapping[str, Any],
) -> dict[str, float]:
    return {
        "kcal_ratio": safe_ratio(totals.get("total_kcal"), target.get("kcal")),
        "protein_ratio": safe_ratio(totals.get("total_protein_g"), target.get("protein_g")),
        "carbs_ratio": safe_ratio(totals.get("total_carbs_g"), target.get("carbs_g")),
        "fat_ratio": safe_ratio(totals.get("total_fat_g"), target.get("fat_g")),
    }


def validation_status_for_record(
    meals: Sequence[Mapping[str, Any]],
    totals: Mapping[str, Any],
    target: Mapping[str, Any],
) -> str:
    invalid_count = 0
    if len(meals) != len(target.get("slot_targets", {})):
        invalid_count += 1
    ratios = macro_ratios(totals, target)
    if ratios["kcal_ratio"] < 0.70 or ratios["protein_ratio"] < 0.70:
        invalid_count += 1
    if any(to_bool(meal.get("is_nutrition_suspicious")) for meal in meals):
        invalid_count += 1
    if to_float(totals.get("effective_time_min_sum")) > 300:
        invalid_count += 1
    if any(to_float(meal.get("effective_time_min_for_scoring")) > 180 for meal in meals):
        invalid_count += 1
    if invalid_count == 0:
        return "valid"
    if invalid_count > 1:
        return "invalid_mixed"
    return "invalid_nutrition_or_time"


def realism_issue_data(meals: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    all_flags: set[str] = set()
    severe_count = 0
    main_count = 0
    borderline_count = 0
    for meal in meals:
        flags = set(reason_items(meal.get("meal_realism_flags")))
        all_flags.update(flags)
        if flags & SEVERE_REALISM_FLAGS:
            severe_count += 1
        if flags & REVIEW_REALISM_FLAGS:
            borderline_count += 1
        if meal.get("slot") in {"lunch", "dinner"} and flags & MAIN_REALISM_FLAGS:
            main_count += 1
    return {
        "all_flags": all_flags,
        "severe_realism_issue_count": severe_count,
        "main_issue_count": main_count,
        "borderline_large_portion_count": borderline_count,
    }


def average_realism_penalty(meals: Sequence[Mapping[str, Any]]) -> float:
    if not meals:
        return 0.0
    return sum(to_float(meal.get("meal_realism_penalty")) for meal in meals) / len(meals)


def slot_candidate_loss(
    row: Mapping[str, Any],
    slot_target: Mapping[str, Any],
) -> float:
    kcal_loss = absolute_ratio_loss(row.get("kcal"), slot_target.get("kcal"))
    protein_loss_value = under_target_loss(row.get("protein_g"), slot_target.get("protein_g"))
    carbs_loss = absolute_ratio_loss(row.get("carbs_g"), slot_target.get("carbs_g"))
    fat_loss_value = absolute_ratio_loss(row.get("fat_g"), slot_target.get("fat_g"))
    macro_loss = (
        0.35 * kcal_loss
        + 0.30 * protein_loss_value
        + 0.25 * carbs_loss
        + 0.10 * fat_loss_value
    )
    time_penalty = max(0.0, 0.55 - to_float(row.get("time_fit"))) * 0.04
    realism_penalty = to_float(row.get("meal_realism_penalty"))
    slot_penalty = 0.05 if to_bool(row.get("is_slot_suspicious")) else 0.0
    nutrition_penalty = 0.04 if to_bool(row.get("is_nutrition_suspicious")) else 0.0
    return round(macro_loss + time_penalty + realism_penalty + slot_penalty + nutrition_penalty, 6)


def is_high_quality_candidate(row: Mapping[str, Any]) -> bool:
    slot = clean_text(row.get("slot"))
    kcal = to_float(row.get("kcal"))
    protein = to_float(row.get("protein_g"))
    flags = set(reason_items(row.get("meal_realism_flags")))
    if to_bool(row.get("realism_hard_reject")):
        return False
    if to_bool(row.get("is_slot_suspicious")):
        return False
    if flags & SEVERE_REALISM_FLAGS:
        return False
    if slot in {"lunch", "dinner"}:
        if kcal < 350 or protein < 15:
            return False
        if flags & MILD_MAIN_REALISM_FLAGS:
            return False
        return True
    if slot == "breakfast":
        if kcal < 150 or kcal > 750:
            return False
        if kcal > 300 and protein < 8:
            return False
        return True
    if slot == "snack":
        return 80 <= kcal <= 350 and "snack_too_large" not in flags
    return False


def candidate_quality_status(row: Mapping[str, Any]) -> str:
    flags = set(reason_items(row.get("meal_realism_flags")))
    if to_bool(row.get("realism_hard_reject")) or flags & SEVERE_REALISM_FLAGS:
        return "reject_like"
    if is_high_quality_candidate(row) and to_float(row.get("score_preview")) >= 0.60:
        return "accept_like"
    slot = clean_text(row.get("slot"))
    kcal = to_float(row.get("kcal"))
    protein = to_float(row.get("protein_g"))
    if slot in {"lunch", "dinner"} and kcal >= 300 and protein >= 12:
        return "review_like"
    if slot == "breakfast" and 120 <= kcal <= 800:
        return "review_like"
    if slot == "snack" and 60 <= kcal <= 380:
        return "review_like"
    return "reject_like"


def slot_audit_priority(row: Mapping[str, Any]) -> float:
    priority = to_float(row.get("slot_usefulness_loss"))
    if not to_bool(row.get("dominant_recipe_flag")):
        priority -= 0.03
    if to_bool(row.get("high_quality_candidate")):
        priority -= 0.02
    if candidate_quality_status(row) == "accept_like":
        priority -= 0.01
    return round(priority, 6)


def top_rows(
    frame: pd.DataFrame,
    column: str,
    count: int,
    ascending: bool,
) -> pd.DataFrame:
    if frame.empty or column not in frame.columns:
        return pd.DataFrame(columns=frame.columns)
    return (
        frame.sort_values(
            [column, "score_preview", "recipe_id", "portion_multiplier"],
            ascending=[ascending, False, True, True],
            kind="mergesort",
            na_position="last",
        )
        .head(count)
        .copy()
    )


def top_one_per_recipe(
    frame: pd.DataFrame,
    column: str,
    ascending: bool,
    count: int,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=frame.columns)
    sorted_frame = frame.sort_values(
        [column, "score_preview", "recipe_id", "portion_multiplier"],
        ascending=[ascending, False, True, True],
        kind="mergesort",
        na_position="last",
    )
    return sorted_frame.drop_duplicates("recipe_id", keep="first").head(count).copy()


def dedupe_slot_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    seen: set[tuple[str, str, float]] = set()
    rows = []
    for _, row in frame.iterrows():
        key = (
            clean_text(row.get("slot")),
            clean_text(row.get("recipe_id")),
            round(to_float(row.get("portion_multiplier")), 4),
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(row.to_dict())
    return pd.DataFrame(rows, columns=frame.columns)


def union_records(collectors: Mapping[str, TopCollector]) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for collector in collectors.values():
        for record in collector.records():
            records[record["recipe_key"]] = record
    return sorted(
        records.values(),
        key=lambda row: (
            row["current_adjusted_day_loss"],
            row["current_base_day_loss"],
            row["recipe_key"],
        ),
    )


def swap_meal(
    meals: Sequence[Mapping[str, Any]],
    slot: str,
    replacement: Mapping[str, Any],
) -> list[dict[str, Any]]:
    swapped = []
    for meal in meals:
        if clean_text(meal.get("slot")) == slot:
            swapped.append(compact_meal(replacement))
        else:
            swapped.append(dict(meal))
    return swapped


def has_duplicate_recipes(meals: Sequence[Mapping[str, Any]]) -> bool:
    recipe_ids = [clean_text(meal.get("recipe_id")) for meal in meals]
    return len(recipe_ids) != len(set(recipe_ids))


def macro_gap_label(
    row: Mapping[str, Any],
    slot_target: Mapping[str, Any],
) -> str:
    labels = []
    for field, target_field in [
        ("kcal", "kcal"),
        ("protein_g", "protein_g"),
        ("carbs_g", "carbs_g"),
        ("fat_g", "fat_g"),
    ]:
        ratio = safe_ratio(row.get(field), slot_target.get(target_field))
        labels.append(f"{field}_ratio={ratio:.3f}")
    return ";".join(labels)


def why_alternative_loses(
    reference: Mapping[str, Any],
    alternative: Mapping[str, Any],
    slot_target: Mapping[str, Any],
    duplicate_after_swap: bool,
    swap_status: str,
) -> str:
    reasons = []
    if to_float(alternative.get("kcal")) < to_float(slot_target.get("kcal")) * 0.85:
        reasons.append("kcal_too_low")
    if to_float(alternative.get("protein_g")) < to_float(slot_target.get("protein_g")) * 0.85:
        reasons.append("protein_too_low")
    if to_float(alternative.get("carbs_g")) < to_float(slot_target.get("carbs_g")) * 0.85:
        reasons.append("carbs_too_low")
    if to_float(alternative.get("fat_g")) > to_float(slot_target.get("fat_g")) * 1.25:
        reasons.append("fat_too_high")
    if to_float(alternative.get("time_fit")) < to_float(reference.get("time_fit")):
        reasons.append("time")
    if reason_items(alternative.get("meal_realism_flags")):
        reasons.append("realism")
    loss_gap = to_float(alternative.get("slot_usefulness_loss")) - to_float(
        reference.get("slot_usefulness_loss")
    )
    if 0.0 <= loss_gap <= 0.06:
        reasons.append("score_loss_marginal")
    elif loss_gap > 0.06:
        reasons.append("score_loss_gap")
    if duplicate_after_swap:
        reasons.append("duplicate/repetition")
    if swap_status == "reject":
        reasons.append("quality_gate_reject")
    if not reasons:
        reasons.append("close_alternative")
    return ";".join(dict.fromkeys(reasons))


def format_counter(counter: Counter) -> str:
    return ";".join(f"{name}:{count}" for name, count in counter.most_common(8))


def worst_realism_label(flags: Counter) -> str:
    if not flags:
        return "none"
    if any(flag in SEVERE_REALISM_FLAGS for flag in flags):
        return "severe:" + ",".join(
            flag for flag in flags if flag in SEVERE_REALISM_FLAGS
        )
    if any(flag in MAIN_REALISM_FLAGS for flag in flags):
        return "main:" + ",".join(flag for flag in flags if flag in MAIN_REALISM_FLAGS)
    return "mild:" + ",".join(flag for flag, _ in flags.most_common(4))


def target_to_dict(target: NutritionTarget | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(target, NutritionTarget):
        return {
            "kcal": target.kcal,
            "protein_g": target.protein_g,
            "carbs_g": target.carbs_g,
            "fat_g": target.fat_g,
            "slot_targets": target.slot_targets,
        }
    return dict(target)


def absolute_ratio_loss(actual: object, target: object) -> float:
    target_value = to_float(target)
    if target_value <= 0:
        return 0.0
    return abs(to_float(actual) - target_value) / target_value


def under_target_loss(actual: object, target: object) -> float:
    target_value = to_float(target)
    if target_value <= 0:
        return 0.0
    ratio = to_float(actual) / target_value
    if ratio >= 0.90:
        return 0.0
    return 0.90 - ratio


def protein_loss(actual: object, target: object) -> float:
    target_value = to_float(target)
    if target_value <= 0:
        return 0.0
    actual_value = to_float(actual)
    if actual_value < target_value:
        return (target_value - actual_value) / target_value
    return max(0.0, (actual_value - 1.6 * target_value) / target_value) * 0.5


def time_penalty_for_effective_time(effective_time: object) -> float:
    value = to_float(effective_time)
    if value <= 300:
        return 0.0
    return min(0.10, ((value - 300) / 300) * 0.04)


def average_score_preview(meals: Sequence[Mapping[str, Any]]) -> float:
    if not meals:
        return 0.0
    return sum(to_float(meal.get("score_preview")) for meal in meals) / len(meals)


def safe_ratio(actual: object, target: object) -> float:
    target_value = to_float(target)
    if target_value <= 0:
        return 0.0
    return to_float(actual) / target_value


def reason_items(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text.replace("'", '"'))
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            pass
    for separator in ("|", ";", ","):
        if separator in text:
            return [item.strip() for item in text.split(separator) if item.strip()]
    return [text]


def clean_text(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value or "").strip()


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return clean_text(value).lower() in {"1", "true", "yes", "y"}


def to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)


if __name__ == "__main__":
    main()
