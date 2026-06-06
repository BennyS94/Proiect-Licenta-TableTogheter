from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.generator_v1.candidate_filter import (
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.recipe_similarity import (
    build_recipe_similarity_features,
    find_similar_recipes,
    load_recipe_similarity_source,
)
from src.generator_v1.service import generate_individual_plan_from_request
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import build_nutrition_target


DATASET_PROFILE = "v1_2_demo_final"
PROFILE_PATH = PROJECT_ROOT / "profiles/member_profile_demo_v1.json"
AUDIT_DIR = PROJECT_ROOT / "data/recipesdb/audit"
SUMMARY_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_approval_summary.txt"
CANDIDATES_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_approval_candidates.csv"
EXAMPLES_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_approval_examples.txt"


def main() -> int:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    plan_response = generate_individual_plan_from_request(_generation_request())
    selected_meals = _selected_meals(plan_response)

    source = load_recipe_similarity_source(DATASET_PROFILE)
    features = build_recipe_similarity_features(
        source["recipes"],
        source["nutrition"],
        source["ingredients"],
    )
    slot_candidates = _build_generator_slot_candidates()

    rows = []
    for meal in selected_meals:
        source_recipe_id = str(meal.get("recipe_id") or "")
        slot = str(meal.get("slot") or "")
        neighbors = find_similar_recipes(
            source_recipe_id,
            features,
            top_k=10,
            filters={"slot": slot, "same_slot": False, "active_only": True},
        )
        for neighbor in neighbors:
            approval = _approve_candidate(slot_candidates, slot, neighbor["candidate_recipe_id"])
            rows.append(
                {
                    "day_index": meal.get("day_index"),
                    "source_slot": slot,
                    "source_recipe_id": source_recipe_id,
                    "source_display_name": meal.get("display_name"),
                    "candidate_recipe_id": neighbor["candidate_recipe_id"],
                    "candidate_display_name": neighbor["candidate_display_name"],
                    "similarity_score": neighbor["similarity_score"],
                    "approval_status": approval["approval_status"],
                    "approval_reasons": "; ".join(approval["approval_reasons"]),
                    "rejection_reasons": "; ".join(approval["rejection_reasons"]),
                    "macro_delta_vs_source_kcal": neighbor["macro_delta_kcal"],
                    "macro_delta_vs_source_protein": neighbor["macro_delta_protein"],
                    "macro_delta_vs_source_carbs": neighbor["macro_delta_carbs"],
                    "macro_delta_vs_source_fat": neighbor["macro_delta_fat"],
                    "macro_fit_vs_slot_target": approval.get("macro_fit"),
                    "kcal_fit_vs_slot_target": approval.get("kcal_fit"),
                    "protein_fit_vs_slot_target": approval.get("protein_fit"),
                    "time_delta_min": neighbor["time_delta_min"],
                    "candidate_time_fit": approval.get("time_fit"),
                    "candidate_slot_fit": approval.get("slot_fit"),
                    "candidate_realism_score": approval.get("realism_score"),
                    "warnings": "; ".join(
                        [item for item in [neighbor.get("warnings"), approval.get("warnings")] if item]
                    ),
                }
            )

    candidates_df = pd.DataFrame(rows)
    candidates_df.to_csv(CANDIDATES_PATH, index=False)
    EXAMPLES_PATH.write_text(_examples_text(candidates_df), encoding="utf-8")

    status_counts = Counter(candidates_df["approval_status"]) if not candidates_df.empty else Counter()
    rejection_counts = _reason_counter(candidates_df, "rejection_reasons")
    candidates_tested = len(candidates_df)
    approved_count = int(status_counts.get("approved", 0))
    review_count = int(status_counts.get("review", 0))
    rejected_count = int(status_counts.get("rejected", 0))
    approval_rate = approved_count / candidates_tested if candidates_tested else 0.0
    useful_enough = approval_rate >= 0.25 or (approved_count + review_count) / max(candidates_tested, 1) >= 0.60

    summary_lines = [
        "KNN recipe similarity v1 generator approval summary",
        "status=ok",
        f"dataset_profile={DATASET_PROFILE}",
        f"selected_meals_tested={len(selected_meals)}",
        f"candidates_tested={candidates_tested}",
        f"approved_count={approved_count}",
        f"review_count={review_count}",
        f"rejected_count={rejected_count}",
        f"approval_rate={approval_rate:.4f}",
        "common_rejection_reasons=" + _format_counter(rejection_counts),
        f"useful_enough_for_next_stage={useful_enough}",
        f"plan_status={plan_response.get('status')}",
        f"approval_candidates_csv={CANDIDATES_PATH.as_posix()}",
        f"approval_examples_txt={EXAMPLES_PATH.as_posix()}",
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0


def _generation_request() -> dict[str, Any]:
    return {
        "dataset_profile": DATASET_PROFILE,
        "days": 3,
        "profile_path": str(PROFILE_PATH),
        "include_grocery_list": False,
        "include_purchase_suggestions": False,
        "include_price_estimates": False,
        "feedback_enabled": False,
        "generation_options": {
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "profile_guard": "demo",
            "multi_day_mode": "global_alternatives_3_day",
            "multi_day_no_repeat_policy": "hard",
            "day_candidate_builder": "direct_from_slots",
        },
    }


def _selected_meals(plan_response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for fallback_day, day in enumerate(plan_response.get("daily_plan", []), start=1):
        if not isinstance(day, dict):
            continue
        day_index = int(day.get("day_index") or fallback_day)
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, dict):
                continue
            rows.append({**meal, "day_index": day_index})
    return rows


def _build_generator_slot_candidates() -> pd.DataFrame:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=DATASET_PROFILE,
    )
    fooddb = load_fooddb_current()
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
        feedback_preference_context=None,
    )
    return build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode="target_aware",
        feedback_preference_context=None,
    )


def _approve_candidate(
    slot_candidates: pd.DataFrame,
    slot: str,
    candidate_recipe_id: str,
) -> dict[str, Any]:
    rows = slot_candidates.loc[
        slot_candidates["slot"].astype(str).eq(slot)
        & slot_candidates["recipe_id"].astype(str).eq(candidate_recipe_id)
    ].copy()
    if rows.empty:
        return {
            "approval_status": "rejected",
            "approval_reasons": [],
            "rejection_reasons": ["not_in_generator_candidate_pool_for_slot"],
            "warnings": "generator_hard_filter_or_slot_filter_removed_candidate",
        }

    rows = rows.sort_values(
        by=["score_preview", "macro_fit", "time_fit"],
        ascending=[False, False, False],
    )
    row = rows.iloc[0]
    approval_reasons: list[str] = []
    rejection_reasons: list[str] = []
    review_reasons: list[str] = []

    if bool(row.get("realism_hard_reject")):
        rejection_reasons.append("realism_hard_reject")
    if bool(row.get("is_slot_suspicious")) and slot == "snack":
        rejection_reasons.append("slot_suspicious_for_snack")

    macro_fit = _float(row.get("macro_fit"))
    nutrition_quality = _float(row.get("nutrition_quality"))
    time_fit = _float(row.get("time_fit"))
    slot_fit = _float(row.get("slot_fit"))
    realism_score = _float(row.get("meal_realism_practical_score"))

    if macro_fit is not None and macro_fit >= 0.65:
        approval_reasons.append("macro_fit_ok")
    else:
        review_reasons.append("macro_fit_low")
    if nutrition_quality is not None and nutrition_quality >= 0.65:
        approval_reasons.append("nutrition_quality_ok")
    else:
        review_reasons.append("nutrition_quality_low")
    if time_fit is not None and time_fit >= 0.25:
        approval_reasons.append("time_fit_ok")
    else:
        review_reasons.append("time_fit_low")
    if slot_fit is not None and slot_fit >= 0.40:
        approval_reasons.append("slot_fit_ok")
    else:
        review_reasons.append("slot_fit_low")
    if realism_score is not None and realism_score >= 0.65:
        approval_reasons.append("meal_realism_ok")
    else:
        review_reasons.append("meal_realism_review")

    if rejection_reasons:
        status = "rejected"
    elif review_reasons:
        status = "review"
    else:
        status = "approved"

    return {
        "approval_status": status,
        "approval_reasons": approval_reasons,
        "rejection_reasons": rejection_reasons if rejection_reasons else review_reasons,
        "macro_fit": macro_fit,
        "kcal_fit": _float(row.get("kcal_fit")),
        "protein_fit": _float(row.get("protein_fit")),
        "time_fit": time_fit,
        "slot_fit": slot_fit,
        "realism_score": realism_score,
        "warnings": "; ".join(_warnings_from_candidate_row(row)),
    }


def _warnings_from_candidate_row(row: Any) -> list[str]:
    warnings = []
    for column in (
        "time_warnings",
        "portion_policy_warnings",
        "meal_realism_practical_flags",
        "nutrition_quality_reasons",
        "slot_suspicion_reasons",
    ):
        value = row.get(column)
        if isinstance(value, list):
            warnings.extend(str(item) for item in value if str(item).strip())
        elif str(value or "").strip() and str(value).lower() != "nan":
            warnings.append(str(value))
    return warnings


def _examples_text(candidates_df: pd.DataFrame) -> str:
    lines = [
        "KNN recipe similarity v1 approval examples",
        "",
    ]
    if candidates_df.empty:
        return "\n".join([*lines, "No candidates tested."])
    for status in ("approved", "review", "rejected"):
        subset = candidates_df.loc[candidates_df["approval_status"].eq(status)].head(5)
        lines.append(f"{status.upper()} examples")
        if subset.empty:
            lines.append("  none")
        for _, row in subset.iterrows():
            lines.append(
                "  "
                f"{row['source_slot']} | {row['source_display_name']} -> "
                f"{row['candidate_display_name']} | "
                f"score={float(row['similarity_score']):.3f}, "
                f"macro_fit={row['macro_fit_vs_slot_target']}, "
                f"reasons={row['approval_reasons'] or row['rejection_reasons']}"
            )
        lines.append("")
    return "\n".join(lines)


def _reason_counter(candidates_df: pd.DataFrame, column: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    if candidates_df.empty or column not in candidates_df.columns:
        return counter
    for value in candidates_df[column].fillna("").astype(str):
        for reason in value.split(";"):
            reason = reason.strip()
            if reason:
                counter[reason] += 1
    return counter


def _format_counter(counter: Counter[str]) -> str:
    if not counter:
        return "none"
    return ",".join(f"{key}:{value}" for key, value in counter.most_common(10))


def _float(value: Any) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric_value):
        return None
    return round(numeric_value, 4)


if __name__ == "__main__":
    raise SystemExit(main())
