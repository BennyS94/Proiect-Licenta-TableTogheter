from __future__ import annotations

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
OUT_SUMMARY = OUT_DIR / "generator_v1_round24_candidate_scarcity_summary.txt"
OUT_SLOT_QUALITY = OUT_DIR / "generator_v1_round24_slot_candidate_quality.csv"
OUT_FAMILY_DOMINANCE = OUT_DIR / "generator_v1_round24_recipe_family_dominance.csv"
OUT_REJECTED = OUT_DIR / "generator_v1_round24_rejected_alternatives.csv"
OUT_RECOMMENDATIONS = OUT_DIR / "generator_v1_round24_data_gap_recommendations.csv"
OUT_ROOT_CAUSES = OUT_DIR / "generator_v1_round24_repetition_root_causes.csv"

DATASET_PROFILE = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE
PORTION_POLICY = "target_aware"
MEAL_REALISM_MODE = "practical"

DOMINANT_RECIPES = [
    {
        "recipe_id": "recipes_v1_1_candidate_169",
        "display_name": "Mom's Best Waffles",
        "primary_slot": "breakfast",
    },
    {
        "recipe_id": "recipes_v1_1_candidate_094",
        "display_name": "Sweet and Sour Stuffed Cabbage",
        "primary_slot": "lunch",
    },
    {
        "recipe_id": "recipes_v1_1_candidate_096",
        "display_name": "Veggie Burgers",
        "primary_slot": "dinner",
    },
]

SEVERE_REALISM_FLAGS = {
    "breakfast_too_large",
    "main_too_large",
    "snack_too_large",
    "unrealistic_large_portion",
}
MONO_MACRO_FLAGS = {
    "low_carb_main",
    "low_protein_main",
    "mostly_carb_meal",
    "mostly_protein_meal",
}


def main() -> None:
    context = build_generation_context()
    target = context["target"]
    slot_candidates = enrich_slot_candidates(
        context["slot_candidates"],
        context["pool"].recipes,
        target,
    )
    slot_rows = build_slot_candidate_quality_rows(slot_candidates)
    family_rows = build_family_dominance_rows(slot_candidates)
    rejected_rows = build_rejected_rows(slot_candidates)
    root_cause_rows = build_repetition_root_cause_rows(slot_candidates)
    recommendation_rows = build_data_gap_recommendations(slot_candidates)
    summary_text = build_summary(
        slot_candidates=slot_candidates,
        slot_rows=slot_rows,
        family_rows=family_rows,
        rejected_rows=rejected_rows,
        root_cause_rows=root_cause_rows,
        recommendation_rows=recommendation_rows,
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(slot_rows).to_csv(OUT_SLOT_QUALITY, index=False)
    pd.DataFrame(family_rows).to_csv(OUT_FAMILY_DOMINANCE, index=False)
    pd.DataFrame(rejected_rows).to_csv(OUT_REJECTED, index=False)
    pd.DataFrame(root_cause_rows).to_csv(OUT_ROOT_CAUSES, index=False)
    pd.DataFrame(recommendation_rows).to_csv(OUT_RECOMMENDATIONS, index=False)
    OUT_SUMMARY.write_text(summary_text, encoding="utf-8")

    slot_summary = summarize_slots(slot_candidates)
    print("Generator v1 Round24 candidate scarcity audit written")
    print(f"summary={OUT_SUMMARY}")
    print(f"slot_quality={OUT_SLOT_QUALITY}")
    print(f"family_dominance={OUT_FAMILY_DOMINANCE}")
    print(f"rejected={OUT_REJECTED}")
    print(f"root_causes={OUT_ROOT_CAUSES}")
    print(f"recommendations={OUT_RECOMMENDATIONS}")
    for row in slot_summary:
        print(
            "slot="
            f"{row['slot']} total={row['total_candidates']} "
            f"unique={row['unique_recipe_count']} "
            f"high_quality_unique={row['high_quality_unique_recipe_count']} "
            f"accept_like_unique={row['accept_like_unique_recipe_count']} "
            f"review_allowed_unique={row['review_allowed_unique_recipe_count']} "
            f"feasibility={row['feasibility_classification']}"
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


def enrich_slot_candidates(
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
    enriched = slot_candidates.merge(
        recipes.loc[:, family_columns].drop_duplicates("recipe_id"),
        on="recipe_id",
        how="left",
    )
    enriched["slot_usefulness_loss"] = enriched.apply(
        lambda row: candidate_usefulness_loss(row, target.slot_targets.get(row["slot"], {})),
        axis=1,
    )
    enriched["carb_forward"] = enriched.apply(is_carb_forward, axis=1)
    enriched["protein_forward"] = enriched.apply(is_protein_forward, axis=1)
    enriched["balanced_candidate"] = enriched.apply(is_balanced_candidate, axis=1)
    enriched["high_quality_candidate"] = enriched.apply(is_high_quality_candidate, axis=1)
    enriched["candidate_quality_status"] = enriched.apply(candidate_quality_status, axis=1)
    enriched["quality_gate_status"] = enriched["candidate_quality_status"]
    enriched["root_rejection_causes"] = enriched.apply(rejection_causes, axis=1)
    enriched = add_slot_rank_columns(enriched)
    return enriched


def add_slot_rank_columns(slot_candidates: pd.DataFrame) -> pd.DataFrame:
    ranked = slot_candidates.copy()
    rank_specs = [
        ("slot_usefulness_loss", "rank_day_loss_usefulness", True),
        ("score_preview", "rank_score_preview", False),
        ("carbs_g", "rank_carb_contribution", False),
        ("protein_g", "rank_protein_contribution", False),
    ]
    for value_column, rank_column, ascending in rank_specs:
        ranked[rank_column] = (
            ranked.groupby("slot")[value_column]
            .rank(method="first", ascending=ascending)
        )
    return ranked


def build_slot_candidate_quality_rows(
    slot_candidates: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows = []
    for _, row in slot_candidates.iterrows():
        rows.append(
            {
                "slot": row.get("slot"),
                "recipe_id": row.get("recipe_id"),
                "display_name": row.get("display_name"),
                "recipe_kind": row.get("recipe_kind"),
                "recipe_family_name": row.get("recipe_family_name"),
                "portion_multiplier": row.get("portion_multiplier"),
                "portion_grams_estimated": row.get("portion_grams_estimated"),
                "kcal": row.get("kcal"),
                "protein_g": row.get("protein_g"),
                "carbs_g": row.get("carbs_g"),
                "fat_g": row.get("fat_g"),
                "score_preview": row.get("score_preview"),
                "macro_fit": row.get("macro_fit"),
                "kcal_fit": row.get("kcal_fit"),
                "protein_fit": row.get("protein_fit"),
                "carbs_fit": row.get("carbs_fit"),
                "fat_fit": row.get("fat_fit"),
                "time_fit": row.get("time_fit"),
                "slot_fit": row.get("slot_fit"),
                "slot_usefulness_loss": row.get("slot_usefulness_loss"),
                "rank_day_loss_usefulness": row.get("rank_day_loss_usefulness"),
                "rank_score_preview": row.get("rank_score_preview"),
                "rank_carb_contribution": row.get("rank_carb_contribution"),
                "rank_protein_contribution": row.get("rank_protein_contribution"),
                "meal_realism_flags": format_list(row.get("meal_realism_practical_flags")),
                "meal_realism_penalty": row.get("meal_realism_practical_penalty"),
                "quality_gate_status": row.get("quality_gate_status"),
                "allowed_slots_json": row.get("allowed_slots_json"),
                "carb_forward": bool(row.get("carb_forward")),
                "protein_forward": bool(row.get("protein_forward")),
                "balanced_candidate": bool(row.get("balanced_candidate")),
                "high_quality_candidate": bool(row.get("high_quality_candidate")),
                "realism_hard_reject": bool(row.get("realism_hard_reject")),
                "is_slot_suspicious": bool(row.get("is_slot_suspicious")),
                "slot_suspicion_reasons": format_list(row.get("slot_suspicion_reasons")),
                "rejection_causes": row.get("root_rejection_causes"),
            }
        )
    return rows


def build_family_dominance_rows(
    slot_candidates: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows = []
    for slot, slot_frame in slot_candidates.groupby("slot", sort=False):
        total = len(slot_frame)
        slot_unique = max(1, slot_frame["recipe_id"].nunique())
        hq_unique_slot = max(
            1,
            slot_frame.loc[
                slot_frame["high_quality_candidate"], "recipe_id"
            ].nunique(),
        )
        frame = slot_frame.copy()
        frame["recipe_family_name"] = frame["recipe_family_name"].fillna("unknown")
        for family, family_frame in frame.groupby("recipe_family_name", sort=True):
            best = family_frame.sort_values(
                ["slot_usefulness_loss", "score_preview"],
                ascending=[True, False],
                kind="mergesort",
            ).iloc[0]
            high_quality = family_frame.loc[family_frame["high_quality_candidate"]]
            rows.append(
                {
                    "slot": slot,
                    "recipe_family_name": family,
                    "candidate_count": len(family_frame),
                    "candidate_share": round(len(family_frame) / max(1, total), 4),
                    "unique_recipe_count": family_frame["recipe_id"].nunique(),
                    "unique_recipe_share": round(
                        family_frame["recipe_id"].nunique() / slot_unique,
                        4,
                    ),
                    "high_quality_candidate_count": len(high_quality),
                    "high_quality_unique_recipe_count": high_quality[
                        "recipe_id"
                    ].nunique(),
                    "high_quality_unique_share": round(
                        high_quality["recipe_id"].nunique() / hq_unique_slot,
                        4,
                    ),
                    "accept_like_candidate_count": int(
                        family_frame["candidate_quality_status"].eq("accept_like").sum()
                    ),
                    "best_recipe_id": best.get("recipe_id"),
                    "best_display_name": best.get("display_name"),
                    "best_slot_usefulness_loss": best.get("slot_usefulness_loss"),
                    "best_score_preview": best.get("score_preview"),
                    "best_macro_fit": best.get("macro_fit"),
                    "best_kcal": best.get("kcal"),
                    "best_protein_g": best.get("protein_g"),
                    "best_carbs_g": best.get("carbs_g"),
                    "dominance_note": dominance_note(family_frame, slot_frame),
                }
            )
    return sorted(
        rows,
        key=lambda item: (
            str(item["slot"]),
            -float(item["high_quality_unique_recipe_count"]),
            -float(item["candidate_share"]),
            str(item["recipe_family_name"]),
        ),
    )


def build_rejected_rows(
    slot_candidates: pd.DataFrame,
) -> list[dict[str, Any]]:
    rejected = slot_candidates.loc[
        slot_candidates["candidate_quality_status"].eq("reject_like")
        | slot_candidates["realism_hard_reject"].map(to_bool)
    ].copy()
    rejected = rejected.sort_values(
        ["slot", "root_rejection_causes", "slot_usefulness_loss"],
        kind="mergesort",
    )
    return [
        {
            "slot": row.get("slot"),
            "recipe_id": row.get("recipe_id"),
            "display_name": row.get("display_name"),
            "recipe_family_name": row.get("recipe_family_name"),
            "portion_multiplier": row.get("portion_multiplier"),
            "portion_grams_estimated": row.get("portion_grams_estimated"),
            "kcal": row.get("kcal"),
            "protein_g": row.get("protein_g"),
            "carbs_g": row.get("carbs_g"),
            "fat_g": row.get("fat_g"),
            "candidate_quality_status": row.get("candidate_quality_status"),
            "realism_hard_reject": row.get("realism_hard_reject"),
            "realism_reject_reason": format_list(row.get("realism_reject_reason")),
            "meal_realism_flags": format_list(row.get("meal_realism_practical_flags")),
            "is_slot_suspicious": row.get("is_slot_suspicious"),
            "slot_suspicion_reasons": format_list(row.get("slot_suspicion_reasons")),
            "slot_usefulness_loss": row.get("slot_usefulness_loss"),
            "score_preview": row.get("score_preview"),
            "macro_fit": row.get("macro_fit"),
            "rejection_causes": row.get("root_rejection_causes"),
        }
        for _, row in rejected.iterrows()
    ]


def build_repetition_root_cause_rows(
    slot_candidates: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for recipe in DOMINANT_RECIPES:
        slot = recipe["primary_slot"]
        slot_frame = slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        recipe_frame = slot_frame.loc[
            slot_frame["recipe_id"].eq(recipe["recipe_id"])
            | slot_frame["display_name"].eq(recipe["display_name"])
        ].copy()
        if recipe_frame.empty:
            rows.append(
                {
                    "dominant_recipe_id": recipe["recipe_id"],
                    "dominant_display_name": recipe["display_name"],
                    "slot": slot,
                    "root_cause": "dominant recipe missing from slot candidates",
                }
            )
            continue

        dominant = recipe_frame.sort_values(
            ["slot_usefulness_loss", "score_preview"],
            ascending=[True, False],
            kind="mergesort",
        ).iloc[0]
        comparable = comparable_alternatives(slot_frame, dominant)
        next_rows = comparable.head(5)
        if next_rows.empty:
            rows.append(root_cause_row(dominant, None, 0, slot_frame))
            continue
        for rank, (_, alternative) in enumerate(next_rows.iterrows(), start=1):
            rows.append(root_cause_row(dominant, alternative, rank, slot_frame))
    return rows


def build_data_gap_recommendations(
    slot_candidates: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows = []
    for slot_summary in summarize_slots(slot_candidates):
        slot = slot_summary["slot"]
        slot_frame = slot_candidates.loc[slot_candidates["slot"].eq(slot)]
        macro_gaps = macro_gap_labels(slot, slot_frame)
        rows.append(
            {
                "slot": slot,
                "total_candidates": slot_summary["total_candidates"],
                "unique_recipe_count": slot_summary["unique_recipe_count"],
                "high_quality_unique_recipe_count": slot_summary[
                    "high_quality_unique_recipe_count"
                ],
                "accept_like_unique_recipe_count": slot_summary[
                    "accept_like_unique_recipe_count"
                ],
                "review_allowed_unique_recipe_count": slot_summary[
                    "review_allowed_unique_recipe_count"
                ],
                "can_find_3_distinct_high_quality": slot_summary[
                    "can_find_3_distinct_high_quality"
                ],
                "can_find_3_distinct_acceptable": slot_summary[
                    "can_find_3_distinct_acceptable"
                ],
                "can_find_3_distinct_with_review": slot_summary[
                    "can_find_3_distinct_with_review"
                ],
                "feasibility_classification": slot_summary[
                    "feasibility_classification"
                ],
                "macro_gap_types": ";".join(macro_gaps),
                "recommended_additions": recommended_additions(slot, macro_gaps),
                "suggested_extra_recipes": suggested_extra_recipes(
                    slot_summary,
                    macro_gaps,
                ),
            }
        )
    return rows


def summarize_slots(slot_candidates: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for slot, frame in slot_candidates.groupby("slot", sort=False):
        high_quality = frame.loc[frame["high_quality_candidate"]]
        accept_like = frame.loc[frame["candidate_quality_status"].eq("accept_like")]
        review_allowed = frame.loc[
            frame["candidate_quality_status"].isin(["accept_like", "review_like"])
        ]
        hq_unique = int(high_quality["recipe_id"].nunique())
        accept_unique = int(accept_like["recipe_id"].nunique())
        review_unique = int(review_allowed["recipe_id"].nunique())
        if hq_unique >= 3 and accept_unique >= 3:
            feasibility = "enough_for_3_day_good"
        elif review_unique >= 3:
            feasibility = "enough_for_3_day_review"
        else:
            feasibility = "insufficient_candidates"
        rows.append(
            {
                "slot": slot,
                "total_candidates": int(len(frame)),
                "unique_recipe_count": int(frame["recipe_id"].nunique()),
                "high_quality_candidate_count": int(len(high_quality)),
                "high_quality_unique_recipe_count": hq_unique,
                "accept_like_candidate_count": int(len(accept_like)),
                "accept_like_unique_recipe_count": accept_unique,
                "review_like_candidate_count": int(
                    frame["candidate_quality_status"].eq("review_like").sum()
                ),
                "review_allowed_unique_recipe_count": review_unique,
                "practical_rejected_candidate_count": int(
                    frame["realism_hard_reject"].map(to_bool).sum()
                ),
                "good_carbs_candidate_count": int(frame["carb_forward"].sum()),
                "good_protein_candidate_count": int(frame["protein_forward"].sum()),
                "balanced_candidate_count": int(frame["balanced_candidate"].sum()),
                "can_find_3_distinct_high_quality": hq_unique >= 3,
                "can_find_3_distinct_acceptable": accept_unique >= 3,
                "can_find_3_distinct_with_review": review_unique >= 3,
                "feasibility_classification": feasibility,
            }
        )
    return rows


def build_summary(
    slot_candidates: pd.DataFrame,
    slot_rows: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    root_cause_rows: list[dict[str, Any]],
    recommendation_rows: list[dict[str, Any]],
) -> str:
    slot_summary = summarize_slots(slot_candidates)
    lines = [
        "Generator v1 Round24 candidate scarcity and diversity bottleneck audit",
        "",
        f"dataset_profile={DATASET_PROFILE}",
        "profile=profiles/member_profile_demo_v1.json",
        "selection_mode=balanced_day",
        f"portion_policy={PORTION_POLICY}",
        f"meal_realism_mode={MEAL_REALISM_MODE}",
        "",
        "Output files",
        f"slot_candidate_quality={OUT_SLOT_QUALITY}",
        f"recipe_family_dominance={OUT_FAMILY_DOMINANCE}",
        f"rejected_alternatives={OUT_REJECTED}",
        f"repetition_root_causes={OUT_ROOT_CAUSES}",
        f"data_gap_recommendations={OUT_RECOMMENDATIONS}",
        "",
        "Slot-level candidate quality",
    ]
    for row in slot_summary:
        lines.append(
            (
                f"slot={row['slot']} total={row['total_candidates']} "
                f"unique={row['unique_recipe_count']} "
                f"high_quality_candidates={row['high_quality_candidate_count']} "
                f"high_quality_unique={row['high_quality_unique_recipe_count']} "
                f"accept_like={row['accept_like_candidate_count']} "
                f"accept_like_unique={row['accept_like_unique_recipe_count']} "
                f"review_like={row['review_like_candidate_count']} "
                f"review_allowed_unique={row['review_allowed_unique_recipe_count']} "
                f"practical_rejected={row['practical_rejected_candidate_count']} "
                f"good_carbs={row['good_carbs_candidate_count']} "
                f"good_protein={row['good_protein_candidate_count']} "
                f"balanced={row['balanced_candidate_count']}"
            )
        )

    lines.extend(["", "No-repeat feasibility by slot"])
    for row in slot_summary:
        lines.append(
            (
                f"slot={row['slot']} "
                f"high_quality_3={row['can_find_3_distinct_high_quality']} "
                f"acceptable_3={row['can_find_3_distinct_acceptable']} "
                f"review_allowed_3={row['can_find_3_distinct_with_review']} "
                f"classification={row['feasibility_classification']}"
            )
        )

    lines.extend(["", "Dominant recipe root causes"])
    for recipe in DOMINANT_RECIPES:
        recipe_rows = [
            row
            for row in root_cause_rows
            if row.get("dominant_recipe_id") == recipe["recipe_id"]
        ]
        if not recipe_rows:
            lines.append(f"{recipe['display_name']}: no audit row")
            continue
        first = recipe_rows[0]
        lines.append(
            (
                f"{recipe['display_name']} ({recipe['primary_slot']}): "
                f"rank={first.get('dominant_slot_rank')} "
                f"best_loss={first.get('dominant_slot_usefulness_loss')} "
                f"comparable_high_quality_unique={first.get('comparable_high_quality_unique_count')} "
                f"root_cause={first.get('root_cause')}"
            )
        )
        for row in recipe_rows[:5]:
            lines.append(
                (
                    f"  alt_rank={row.get('alternative_rank')} "
                    f"alt={row.get('alternative_display_name')} "
                    f"loss_gap={row.get('slot_usefulness_loss_gap')} "
                    f"score_gap={row.get('score_preview_gap')} "
                    f"failure={row.get('alternative_failure_mode')}"
                )
            )

    lines.extend(["", "Top family dominance signals"])
    for row in sorted(
        family_rows,
        key=lambda item: (
            str(item["slot"]),
            -float(item["high_quality_unique_share"]),
            -float(item["candidate_share"]),
        ),
    )[:20]:
        lines.append(
            (
                f"slot={row['slot']} family={row['recipe_family_name']} "
                f"unique={row['unique_recipe_count']} "
                f"hq_unique={row['high_quality_unique_recipe_count']} "
                f"candidate_share={row['candidate_share']} "
                f"best={row['best_display_name']}"
            )
        )

    lines.extend(["", "Top 20 slot candidate lists"])
    lines.append(
        "The full CSV also contains rank_day_loss_usefulness, rank_score_preview, "
        "rank_carb_contribution, and rank_protein_contribution."
    )
    for slot, frame in slot_candidates.groupby("slot", sort=False):
        lines.append(f"slot={slot}")
        for label, value_column, ascending in [
            ("day_loss_usefulness", "slot_usefulness_loss", True),
            ("score_preview", "score_preview", False),
            ("carb_contribution", "carbs_g", False),
            ("protein_contribution", "protein_g", False),
        ]:
            lines.append(f"  top20_by={label}")
            top_rows = (
                frame.sort_values(
                    [value_column, "display_name"],
                    ascending=[ascending, True],
                    kind="mergesort",
                )
                .head(20)
                .itertuples(index=False)
            )
            for rank, row in enumerate(top_rows, start=1):
                lines.append(
                    (
                        f"    {rank}. {getattr(row, 'display_name')} "
                        f"recipe_id={getattr(row, 'recipe_id')} "
                        f"value={getattr(row, value_column)} "
                        f"status={getattr(row, 'candidate_quality_status')}"
                    )
                )

    lines.extend(["", "Data gap recommendations"])
    for row in recommendation_rows:
        lines.append(
            (
                f"slot={row['slot']} feasibility={row['feasibility_classification']} "
                f"macro_gaps={row['macro_gap_types']} "
                f"suggested_extra={row['suggested_extra_recipes']} "
                f"recommendation={row['recommended_additions']}"
            )
        )

    lines.extend(
        [
            "",
            "Strict conclusion",
            strict_conclusion(slot_summary, root_cause_rows),
            "",
            "Interpretation",
            interpretation(slot_summary, rejected_rows, root_cause_rows),
            "",
            "Generated row counts",
            f"slot_candidate_quality_rows={len(slot_rows)}",
            f"family_dominance_rows={len(family_rows)}",
            f"rejected_alternative_rows={len(rejected_rows)}",
            f"repetition_root_cause_rows={len(root_cause_rows)}",
            f"recommendation_rows={len(recommendation_rows)}",
        ]
    )
    return "\n".join(lines) + "\n"


def candidate_usefulness_loss(
    row: Mapping[str, Any],
    slot_target: Mapping[str, Any],
) -> float:
    kcal_loss = ratio_loss(row.get("kcal"), slot_target.get("kcal"))
    protein_loss = under_target_loss(row.get("protein_g"), slot_target.get("protein_g"))
    carbs_loss = ratio_loss(row.get("carbs_g"), slot_target.get("carbs_g"))
    fat_loss = ratio_loss(row.get("fat_g"), slot_target.get("fat_g"))
    macro_loss = 0.35 * kcal_loss + 0.30 * protein_loss + 0.25 * carbs_loss + 0.10 * fat_loss
    time_penalty = max(0.0, 0.55 - to_float(row.get("time_fit"))) * 0.04
    realism_penalty = to_float(row.get("meal_realism_practical_penalty"))
    slot_penalty = 0.05 if to_bool(row.get("is_slot_suspicious")) else 0.0
    nutrition_penalty = 0.04 if to_bool(row.get("is_nutrition_suspicious")) else 0.0
    return round(macro_loss + time_penalty + realism_penalty + slot_penalty + nutrition_penalty, 6)


def is_high_quality_candidate(row: Mapping[str, Any]) -> bool:
    slot = clean_text(row.get("slot"))
    kcal = to_float(row.get("kcal"))
    protein = to_float(row.get("protein_g"))
    flags = set(reason_items(row.get("meal_realism_practical_flags")))
    if to_bool(row.get("realism_hard_reject")):
        return False
    if to_bool(row.get("is_slot_suspicious")):
        return False
    if flags & SEVERE_REALISM_FLAGS:
        return False
    if slot in {"lunch", "dinner"}:
        if kcal < 350 or protein < 15:
            return False
        if flags & MONO_MACRO_FLAGS:
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
    flags = set(reason_items(row.get("meal_realism_practical_flags")))
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


def is_carb_forward(row: Mapping[str, Any]) -> bool:
    slot = clean_text(row.get("slot"))
    carbs = to_float(row.get("carbs_g"))
    if slot in {"lunch", "dinner"}:
        return carbs >= 45
    if slot == "breakfast":
        return carbs >= 30
    if slot == "snack":
        return carbs >= 15
    return False


def is_protein_forward(row: Mapping[str, Any]) -> bool:
    slot = clean_text(row.get("slot"))
    protein = to_float(row.get("protein_g"))
    if slot in {"lunch", "dinner"}:
        return protein >= 25
    if slot == "breakfast":
        return protein >= 12
    if slot == "snack":
        return protein >= 8
    return False


def is_balanced_candidate(row: Mapping[str, Any]) -> bool:
    slot = clean_text(row.get("slot"))
    kcal = to_float(row.get("kcal"))
    protein = to_float(row.get("protein_g"))
    carbs = to_float(row.get("carbs_g"))
    flags = set(reason_items(row.get("meal_realism_practical_flags")))
    if flags & (SEVERE_REALISM_FLAGS | MONO_MACRO_FLAGS):
        return False
    if slot in {"lunch", "dinner"}:
        return kcal >= 350 and protein >= 20 and carbs >= 35
    if slot == "breakfast":
        return 180 <= kcal <= 700 and protein >= 8 and carbs >= 20
    if slot == "snack":
        return 80 <= kcal <= 350 and protein >= 5
    return False


def rejection_causes(row: Mapping[str, Any]) -> str:
    causes = []
    slot = clean_text(row.get("slot"))
    flags = set(reason_items(row.get("meal_realism_practical_flags")))
    if to_bool(row.get("realism_hard_reject")):
        causes.append("practical_realism_hard_reject")
    if to_bool(row.get("is_slot_suspicious")):
        causes.append("slot_suspicious")
    if flags & SEVERE_REALISM_FLAGS:
        causes.append("severe_realism_flag")
    if flags & MONO_MACRO_FLAGS:
        causes.append("mono_macro_or_main_macro_warning")
    if slot in {"lunch", "dinner"}:
        if to_float(row.get("kcal")) < 350:
            causes.append("main_kcal_low")
        if to_float(row.get("protein_g")) < 15:
            causes.append("main_protein_low")
        if to_float(row.get("carbs_g")) < 30:
            causes.append("main_carbs_low")
    if slot == "breakfast":
        if to_float(row.get("kcal")) < 150:
            causes.append("breakfast_kcal_low")
        if to_float(row.get("kcal")) > 750:
            causes.append("breakfast_kcal_high")
        if to_float(row.get("kcal")) > 300 and to_float(row.get("protein_g")) < 8:
            causes.append("breakfast_protein_low")
    if slot == "snack":
        if to_float(row.get("kcal")) < 80:
            causes.append("snack_kcal_low")
        if to_float(row.get("kcal")) > 350:
            causes.append("snack_kcal_high")
    if not causes and not is_high_quality_candidate(row):
        causes.append("below_high_quality_threshold")
    return ";".join(dict.fromkeys(causes))


def comparable_alternatives(
    slot_frame: pd.DataFrame,
    dominant: Mapping[str, Any],
) -> pd.DataFrame:
    alternatives = slot_frame.loc[
        ~slot_frame["recipe_id"].eq(dominant.get("recipe_id"))
        & slot_frame["candidate_quality_status"].isin(["accept_like", "review_like"])
    ].copy()
    alternatives = alternatives.sort_values(
        ["slot_usefulness_loss", "score_preview"],
        ascending=[True, False],
        kind="mergesort",
    )
    return alternatives.drop_duplicates("recipe_id", keep="first")


def root_cause_row(
    dominant: Mapping[str, Any],
    alternative: Mapping[str, Any] | None,
    rank: int,
    slot_frame: pd.DataFrame,
) -> dict[str, Any]:
    comparable_hq = slot_frame.loc[
        slot_frame["high_quality_candidate"]
        & ~slot_frame["recipe_id"].eq(dominant.get("recipe_id"))
    ]["recipe_id"].nunique()
    dominant_rank = rank_in_slot(slot_frame, dominant)
    if alternative is None:
        return {
            "dominant_recipe_id": dominant.get("recipe_id"),
            "dominant_display_name": dominant.get("display_name"),
            "slot": dominant.get("slot"),
            "dominant_slot_rank": dominant_rank,
            "dominant_slot_usefulness_loss": dominant.get("slot_usefulness_loss"),
            "dominant_score_preview": dominant.get("score_preview"),
            "comparable_high_quality_unique_count": comparable_hq,
            "alternative_rank": None,
            "alternative_recipe_id": None,
            "alternative_display_name": None,
            "root_cause": "no comparable alternatives found",
        }

    loss_gap = round(
        to_float(alternative.get("slot_usefulness_loss"))
        - to_float(dominant.get("slot_usefulness_loss")),
        6,
    )
    score_gap = round(
        to_float(dominant.get("score_preview"))
        - to_float(alternative.get("score_preview")),
        6,
    )
    failure_mode = alternative_failure_mode(dominant, alternative)
    return {
        "dominant_recipe_id": dominant.get("recipe_id"),
        "dominant_display_name": dominant.get("display_name"),
        "slot": dominant.get("slot"),
        "dominant_slot_rank": dominant_rank,
        "dominant_slot_usefulness_loss": dominant.get("slot_usefulness_loss"),
        "dominant_score_preview": dominant.get("score_preview"),
        "dominant_macro_fit": dominant.get("macro_fit"),
        "dominant_kcal_fit": dominant.get("kcal_fit"),
        "dominant_protein_fit": dominant.get("protein_fit"),
        "dominant_carbs_fit": dominant.get("carbs_fit"),
        "dominant_time_fit": dominant.get("time_fit"),
        "comparable_high_quality_unique_count": comparable_hq,
        "alternative_rank": rank,
        "alternative_recipe_id": alternative.get("recipe_id"),
        "alternative_display_name": alternative.get("display_name"),
        "alternative_quality_status": alternative.get("candidate_quality_status"),
        "alternative_slot_usefulness_loss": alternative.get("slot_usefulness_loss"),
        "slot_usefulness_loss_gap": loss_gap,
        "alternative_score_preview": alternative.get("score_preview"),
        "score_preview_gap": score_gap,
        "alternative_macro_fit": alternative.get("macro_fit"),
        "alternative_kcal_fit": alternative.get("kcal_fit"),
        "alternative_protein_fit": alternative.get("protein_fit"),
        "alternative_carbs_fit": alternative.get("carbs_fit"),
        "alternative_time_fit": alternative.get("time_fit"),
        "alternative_kcal": alternative.get("kcal"),
        "alternative_protein_g": alternative.get("protein_g"),
        "alternative_carbs_g": alternative.get("carbs_g"),
        "alternative_flags": format_list(alternative.get("meal_realism_practical_flags")),
        "lower_macro_fit": to_float(alternative.get("macro_fit")) < to_float(dominant.get("macro_fit")),
        "lower_kcal_fit": to_float(alternative.get("kcal_fit")) < to_float(dominant.get("kcal_fit")),
        "lower_carbs": to_float(alternative.get("carbs_g")) < to_float(dominant.get("carbs_g")),
        "lower_protein": to_float(alternative.get("protein_g")) < to_float(dominant.get("protein_g")),
        "time_penalty_gap": round(
            to_float(dominant.get("time_fit")) - to_float(alternative.get("time_fit")),
            6,
        ),
        "alternative_failure_mode": failure_mode,
        "root_cause": root_cause_label(dominant, alternative, comparable_hq, dominant_rank),
    }


def alternative_failure_mode(
    dominant: Mapping[str, Any],
    alternative: Mapping[str, Any],
) -> str:
    causes = []
    if to_float(alternative.get("macro_fit")) < to_float(dominant.get("macro_fit")):
        causes.append("lower_macro_fit")
    if to_float(alternative.get("kcal_fit")) < to_float(dominant.get("kcal_fit")):
        causes.append("lower_kcal_fit")
    if to_float(alternative.get("carbs_g")) < to_float(dominant.get("carbs_g")):
        causes.append("lower_carbs")
    if to_float(alternative.get("protein_g")) < to_float(dominant.get("protein_g")):
        causes.append("lower_protein")
    if reason_items(alternative.get("meal_realism_practical_flags")):
        causes.append("realism_flags")
    if to_float(alternative.get("time_fit")) < to_float(dominant.get("time_fit")):
        causes.append("time_penalty")
    if alternative.get("candidate_quality_status") == "review_like":
        causes.append("review_like")
    return ";".join(causes) if causes else "close_alternative"


def root_cause_label(
    dominant: Mapping[str, Any],
    alternative: Mapping[str, Any],
    comparable_hq: int,
    dominant_rank: int,
) -> str:
    if dominant_rank <= 3 and comparable_hq < 3:
        return "dominates because few high-quality distinct alternatives exist"
    if dominant_rank <= 3:
        return "dominates because scoring/day-loss ranks it above alternatives"
    if alternative.get("candidate_quality_status") == "review_like":
        return "alternatives exist but are review-level"
    return "mixed scarcity and scoring effect"


def rank_in_slot(slot_frame: pd.DataFrame, row: Mapping[str, Any]) -> int:
    ranked = (
        slot_frame.sort_values(
            ["slot_usefulness_loss", "score_preview"],
            ascending=[True, False],
            kind="mergesort",
        )
        .drop_duplicates("recipe_id", keep="first")
        .reset_index(drop=True)
    )
    matches = ranked.index[ranked["recipe_id"].eq(row.get("recipe_id"))].tolist()
    return int(matches[0] + 1) if matches else 0


def macro_gap_labels(slot: str, frame: pd.DataFrame) -> list[str]:
    labels = []
    high_quality = frame.loc[frame["high_quality_candidate"]]
    if slot in {"lunch", "dinner"}:
        if high_quality.loc[high_quality["carb_forward"], "recipe_id"].nunique() < 3:
            labels.append("carb-forward mains")
        if high_quality.loc[high_quality["balanced_candidate"], "recipe_id"].nunique() < 3:
            labels.append("protein-balanced carb mains")
        if frame.loc[frame["protein_forward"] & frame["carb_forward"], "recipe_id"].nunique() < 5:
            labels.append("lunch/dinner alternatives")
    elif slot == "breakfast":
        lighter = high_quality.loc[
            (high_quality["kcal"].map(to_float) >= 150)
            & (high_quality["kcal"].map(to_float) <= 500)
        ]
        if lighter["recipe_id"].nunique() < 3:
            labels.append("lighter breakfasts")
        if high_quality["recipe_id"].nunique() < 6:
            labels.append("breakfast alternatives")
    elif slot == "snack":
        if high_quality["recipe_id"].nunique() < 6:
            labels.append("better snacks")
    return labels or ["no severe slot-level data gap by count"]


def recommended_additions(slot: str, macro_gaps: Sequence[str]) -> str:
    recommendations = []
    if "carb-forward mains" in macro_gaps:
        recommendations.append("add more complete carb-forward mains")
    if "protein-balanced carb mains" in macro_gaps:
        recommendations.append("add more balanced chicken/rice/pasta/potato recipes")
    if "lunch/dinner alternatives" in macro_gaps:
        recommendations.append("add more fish/pork/turkey complete meals")
        recommendations.append("add more vegetarian carb-protein meals")
    if "lighter breakfasts" in macro_gaps or "breakfast alternatives" in macro_gaps:
        recommendations.append("add more breakfast alternatives")
    if "better snacks" in macro_gaps:
        recommendations.append("add more better snacks")
    if not recommendations and slot in {"lunch", "dinner"}:
        recommendations.append("do not add broad recipes yet; inspect scoring and family concentration")
    if not recommendations:
        recommendations.append("no immediate recipe additions required by count")
    return "; ".join(dict.fromkeys(recommendations))


def suggested_extra_recipes(
    slot_summary: Mapping[str, Any],
    macro_gaps: Sequence[str],
) -> str:
    hq_unique = int(slot_summary.get("high_quality_unique_recipe_count", 0) or 0)
    accept_unique = int(slot_summary.get("accept_like_unique_recipe_count", 0) or 0)
    if hq_unique < 3 or accept_unique < 3:
        return "+25"
    if any(label != "no severe slot-level data gap by count" for label in macro_gaps):
        return "+10"
    return "+10"


def dominance_note(
    family_frame: pd.DataFrame,
    slot_frame: pd.DataFrame,
) -> str:
    share = len(family_frame) / max(1, len(slot_frame))
    hq_share = family_frame.loc[family_frame["high_quality_candidate"], "recipe_id"].nunique() / max(
        1,
        slot_frame.loc[slot_frame["high_quality_candidate"], "recipe_id"].nunique(),
    )
    if share >= 0.20 or hq_share >= 0.25:
        return "family dominates slot candidate pool"
    return "family present but not dominant"


def strict_conclusion(
    slot_summary: Sequence[Mapping[str, Any]],
    root_cause_rows: Sequence[Mapping[str, Any]],
) -> str:
    bad_slots = [
        row["slot"]
        for row in slot_summary
        if row["feasibility_classification"] != "enough_for_3_day_good"
    ]
    root_causes = Counter(row.get("root_cause") for row in root_cause_rows)
    if bad_slots:
        return (
            "3-day no-repeat good plan is not proven feasible for all slots; "
            f"weak slots: {', '.join(str(slot) for slot in bad_slots)}. "
            "This points to candidate/data scarcity plus scoring concentration, not a reason to add more generator logic now."
        )
    if root_causes.get("dominates because scoring/day-loss ranks it above alternatives", 0) >= 2:
        return (
            "There are enough candidates by count, but scoring/day-loss still concentrates around a few recipes."
        )
    return "No hard scarcity by count, but dominant recipe concentration still needs dataset/scoring review."


def interpretation(
    slot_summary: Sequence[Mapping[str, Any]],
    rejected_rows: Sequence[Mapping[str, Any]],
    root_cause_rows: Sequence[Mapping[str, Any]],
) -> str:
    reject_count = len(rejected_rows)
    insufficient = [
        row
        for row in slot_summary
        if row["feasibility_classification"] == "insufficient_candidates"
    ]
    scarcity_roots = [
        row
        for row in root_cause_rows
        if "few high-quality" in str(row.get("root_cause", ""))
    ]
    if insufficient:
        return "Primary issue: data scarcity in at least one slot."
    if scarcity_roots and reject_count > 0:
        return (
            "Primary issue: quality-safe candidate scarcity after practical realism and macro filters; "
            "some alternatives exist but many are review/reject-like."
        )
    return (
        "Primary issue: selector scoring and dataset shape both matter; there are alternatives by count, "
        "but the best macro/usefulness candidates are concentrated in a small recipe set."
    )


def ratio_loss(actual: object, target: object) -> float:
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


def format_list(value: object) -> str:
    return ";".join(reason_items(value))


def clean_text(value: object) -> str:
    return str(value or "").strip().lower()


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = clean_text(value)
    return text in {"1", "true", "yes", "y"}


def to_float(value: object) -> float:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return 0.0
    return float(numeric_value)


if __name__ == "__main__":
    main()
