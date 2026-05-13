from __future__ import annotations

import itertools
import json
import sys
from collections import Counter, defaultdict
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
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    _build_candidate_day_pool,
    generate_multi_day_plan,
)
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import build_nutrition_target


DATASET_PROFILE = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE
PROFILE_PATH = REPO_ROOT / "profiles" / "member_profile_demo_v1.json"
OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
DATASET_DIR = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "v1_1_generator_ready_slot_checked_time_enriched_snack_curated_plus10"
)
RECIPES_PATH = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_RECIPES_PATH
INGREDIENTS_PATH = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_INGREDIENTS_PATH
NUTRITION_CACHE_PATH = V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_NUTRITION_PATH
ROUND26_DAYS_PATH = OUT_DIR / "generator_v1_round26_plus10_multiday_days.csv"
ROUND26_MEALS_PATH = OUT_DIR / "generator_v1_round26_plus10_multiday_meals.csv"
ROUND26_REPETITION_PATH = OUT_DIR / "generator_v1_round26_plus10_multiday_repetition.csv"

SUMMARY_PATH = OUT_DIR / "generator_v1_round27_plus10_bottleneck_summary.txt"
ROOT_CAUSES_PATH = OUT_DIR / "generator_v1_round27_repeated_recipe_root_causes.csv"
SLOT_DEFICIT_PATH = OUT_DIR / "generator_v1_round27_slot_deficit_audit.csv"
RECOMMENDATIONS_PATH = OUT_DIR / "generator_v1_round27_next_expansion_recommendations.csv"
CANDIDATE_QUALITY_PATH = OUT_DIR / "generator_v1_round27_multi_day_candidate_quality.csv"

SLOTS = ["breakfast", "lunch", "dinner", "snack"]
MAIN_SLOTS = {"lunch", "dinner"}
DOMINANT_DISPLAY_NAMES = {"Mom's Best Waffles", "Chicken and Broccoli Pasta"}
SEVERE_REALISM_FLAGS = {
    "dessert_as_meal",
    "drink_as_meal",
    "snack_too_large",
    "ingredient_component_as_meal",
    "condiment_as_meal",
    "low_protein_main",
    "mostly_carb_meal",
    "mostly_fat_meal",
}

CONFIG = {
    "selection_mode": "balanced_day",
    "portion_policy": "target_aware",
    "meal_realism_mode": "practical",
    "quality_gate": "demo_safe",
    "alternative_count": 3,
    "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
    "candidate_day_alternative_count": 18,
    "global_max_candidates_per_slot": 26,
    "max_candidate_days": 34,
}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _flags(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item)]
        except json.JSONDecodeError:
            pass
    return [part.strip() for part in text.replace("|", ";").split(";") if part.strip()]


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return []


def _meal_flags(meal: dict[str, Any]) -> list[str]:
    for key in (
        "meal_realism_flags",
        "meal_realism_practical_flags",
        "realism_flags",
        "quality_warnings",
    ):
        if key in meal:
            flags = _flags(meal.get(key))
            if flags:
                return flags
    return []


def _display_name(row: pd.Series) -> str:
    for column in ("display_name", "recipe_name", "name", "title"):
        if column in row and str(row.get(column, "")).strip():
            return str(row.get(column)).strip()
    return str(row.get("recipe_id", "")).strip()


def _nutrition(row: pd.Series) -> dict[str, float]:
    return {
        "kcal": _to_float(row.get("kcal")),
        "protein": _to_float(row.get("protein_g", row.get("protein"))),
        "carbs": _to_float(row.get("carbs_g", row.get("carbs"))),
        "fat": _to_float(row.get("fat_g", row.get("fat"))),
    }


def _recipe_family(row: pd.Series) -> str:
    for column in ("recipe_family_name", "recipe_kind", "primary_protein", "source_category"):
        value = str(row.get(column, "")).strip()
        if value:
            return value
    return "unknown"


def _recipe_kind(row: pd.Series) -> str:
    for column in ("recipe_kind", "recipe_kind_guess", "source_category", "source_subcategory"):
        value = str(row.get(column, "")).strip()
        if value:
            return value
    return "unknown"


def _prepare_context() -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=RECIPES_PATH,
        ingredients_path=INGREDIENTS_PATH,
        nutrition_path=NUTRITION_CACHE_PATH,
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
        portion_policy_mode=CONFIG["portion_policy"],
    )
    recipes_df = pd.read_csv(RECIPES_PATH)
    nutrition_df = pd.read_csv(NUTRITION_CACHE_PATH)
    return {
        "profile": profile,
        "target": target,
        "pool": pool,
        "filtered_candidates": filtered_candidates,
        "slot_candidates": slot_candidates,
        "recipes_df": recipes_df,
        "nutrition_df": nutrition_df,
    }


def _generate_current_plan(context: dict[str, Any]) -> dict[str, Any]:
    return generate_multi_day_plan(
        profile=context["profile"],
        target=context["target"],
        slot_candidates=context["slot_candidates"],
        days=3,
        config=CONFIG,
    )


def _generate_candidate_days(context: dict[str, Any]) -> list[dict[str, Any]]:
    slot_candidates = context["slot_candidates"]
    candidates_by_slot = {
        slot: slot_candidates[slot_candidates["slot"] == slot].copy() for slot in SLOTS
    }
    return _build_candidate_day_pool(
        slot_candidates_by_slot=candidates_by_slot,
        target=context["target"],
        slot_order=SLOTS,
        config=CONFIG,
    )


def _selected_meals_from_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    meals: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        day_index = _to_int(day.get("day_index"))
        for meal in day.get("selected_meals", []):
            enriched = dict(meal)
            enriched["day_index"] = day_index
            enriched["quality_gate_status"] = day.get("quality_gate_status")
            enriched["validation_status"] = day.get("validation_status")
            enriched["base_day_loss"] = _to_float(day.get("base_day_loss", day.get("adjusted_day_loss")))
            enriched["adjusted_day_loss"] = _to_float(day.get("adjusted_day_loss", day.get("base_day_loss")))
            meals.append(enriched)
    return meals


def _selected_meals_from_candidate(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    plan = candidate.get("plan") or {}
    meals = []
    for meal in plan.get("selected_meals", []):
        enriched = dict(meal)
        enriched["candidate_day_id"] = candidate.get("candidate_day_id")
        enriched["quality_gate_status"] = candidate.get("quality_gate_status")
        enriched["validation_status"] = candidate.get("validation_status")
        enriched["base_day_loss"] = _to_float(candidate.get("base_day_loss", candidate.get("adjusted_day_loss")))
        enriched["adjusted_day_loss"] = _to_float(candidate.get("adjusted_day_loss", candidate.get("base_day_loss")))
        meals.append(enriched)
    return meals


def _meal_recipe_id(meal: dict[str, Any]) -> str:
    return str(meal.get("recipe_id", "")).strip()


def _meal_display_name(meal: dict[str, Any]) -> str:
    return str(meal.get("display_name") or meal.get("recipe_name") or meal.get("recipe_id", "")).strip()


def _meal_slot(meal: dict[str, Any]) -> str:
    return str(meal.get("slot", "")).strip()


def _meal_nutrition(meal: dict[str, Any]) -> dict[str, float]:
    return {
        "kcal": _to_float(meal.get("kcal")),
        "protein": _to_float(meal.get("protein_g", meal.get("protein"))),
        "carbs": _to_float(meal.get("carbs_g", meal.get("carbs"))),
        "fat": _to_float(meal.get("fat_g", meal.get("fat"))),
    }


def _current_repeats(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    meals = _selected_meals_from_plan(plan)
    by_recipe: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for meal in meals:
        recipe_id = _meal_recipe_id(meal)
        if recipe_id:
            by_recipe[recipe_id].append(meal)

    repeated: dict[str, dict[str, Any]] = {}
    for recipe_id, rows in by_recipe.items():
        display_name = _meal_display_name(rows[0])
        is_named_dominant = display_name in DOMINANT_DISPLAY_NAMES
        if len(rows) > 1 or is_named_dominant:
            repeated[recipe_id] = {
                "recipe_id": recipe_id,
                "display_name": display_name,
                "count": len(rows),
                "slots": sorted({_meal_slot(row) for row in rows}),
                "slot_counts": dict(Counter(_meal_slot(row) for row in rows)),
                "day_indices": sorted({_to_int(row.get("day_index")) for row in rows}),
                "rows": rows,
            }

    if ROUND26_REPETITION_PATH.exists():
        round26 = pd.read_csv(ROUND26_REPETITION_PATH)
        for _, row in round26.iterrows():
            recipe_id = str(row.get("recipe_id", "")).strip()
            display_name = str(row.get("display_name", "")).strip()
            if not recipe_id or recipe_id in repeated:
                continue
            if display_name in DOMINANT_DISPLAY_NAMES or _to_int(row.get("repeat_count")) > 0:
                repeated[recipe_id] = {
                    "recipe_id": recipe_id,
                    "display_name": display_name or recipe_id,
                    "count": _to_int(row.get("total_count", row.get("count", 0))),
                    "slots": _json_list(row.get("slots_json")) or [str(row.get("slot", "")).strip()],
                    "slot_counts": {},
                    "day_indices": [],
                    "rows": [],
                }
    return repeated


def _slot_target(target: Any, slot: str) -> dict[str, float]:
    slot_target = getattr(target, "slot_targets", {}).get(slot)
    if slot_target is None:
        return {"kcal": 0.0, "protein": 0.0, "carbs": 0.0, "fat": 0.0}
    if isinstance(slot_target, dict):
        return {
            "kcal": _to_float(slot_target.get("kcal")),
            "protein": _to_float(slot_target.get("protein_g", slot_target.get("protein"))),
            "carbs": _to_float(slot_target.get("carbs_g", slot_target.get("carbs"))),
            "fat": _to_float(slot_target.get("fat_g", slot_target.get("fat"))),
        }
    return {
        "kcal": _to_float(getattr(slot_target, "kcal", 0.0)),
        "protein": _to_float(getattr(slot_target, "protein_g", 0.0)),
        "carbs": _to_float(getattr(slot_target, "carbs_g", 0.0)),
        "fat": _to_float(getattr(slot_target, "fat_g", 0.0)),
    }


def _macro_gap(row: pd.Series, target: Any, slot: str) -> dict[str, float]:
    nutrition = _nutrition(row)
    target_values = _slot_target(target, slot)
    return {
        "kcal_gap": nutrition["kcal"] - target_values["kcal"],
        "protein_gap": nutrition["protein"] - target_values["protein"],
        "carbs_gap": nutrition["carbs"] - target_values["carbs"],
        "fat_gap": nutrition["fat"] - target_values["fat"],
    }


def _candidate_status(row: pd.Series, slot: str) -> str:
    high_quality = _is_high_quality_slot_candidate(row, slot)
    score_preview = _to_float(row.get("score_preview"))
    flags = _flags(row.get("meal_realism_practical_flags", row.get("meal_realism_flags")))
    if high_quality and score_preview >= 0.60 and not set(flags).intersection(SEVERE_REALISM_FLAGS):
        return "accept_like"
    if high_quality or score_preview >= 0.45:
        return "review_like"
    return "reject_like"


def _is_high_quality_slot_candidate(row: pd.Series, slot: str) -> bool:
    flags = set(_flags(row.get("meal_realism_practical_flags", row.get("meal_realism_flags"))))
    if flags.intersection(SEVERE_REALISM_FLAGS):
        return False

    allowed_slots = set(_json_list(row.get("allowed_slots_json")))
    if allowed_slots and slot not in allowed_slots:
        return False

    kcal = _to_float(row.get("kcal"))
    protein = _to_float(row.get("protein_g", row.get("protein")))
    carbs = _to_float(row.get("carbs_g", row.get("carbs")))
    fat = _to_float(row.get("fat_g", row.get("fat")))
    total_macros = protein + carbs + fat

    if slot in MAIN_SLOTS:
        if protein < 15 or kcal < 350:
            return False
        if total_macros > 0:
            if carbs / total_macros < 0.15 or protein / total_macros < 0.12:
                return False
        return True

    if slot == "breakfast":
        if kcal < 150 or kcal > 750:
            return False
        if kcal > 300 and protein < 8:
            return False
        return True

    if slot == "snack":
        if kcal < 80 or kcal > 350:
            return False
        if "snack_too_large" in flags:
            return False
        return True

    return True


def _candidate_slot_loss(row: pd.Series, target: Any, slot: str) -> float:
    nutrition = _nutrition(row)
    target_values = _slot_target(target, slot)
    losses = []
    for key in ("kcal", "protein", "carbs", "fat"):
        wanted = max(target_values[key], 1.0)
        losses.append(abs(nutrition[key] - wanted) / wanted)
    score_loss = 1.0 - _to_float(row.get("score_preview"))
    realism_loss = _to_float(row.get("meal_realism_practical_penalty", row.get("meal_realism_penalty"))) * 0.15
    return 0.35 * losses[0] + 0.25 * losses[1] + 0.25 * losses[2] + 0.10 * losses[3] + 0.05 * score_loss + realism_loss


def _prepare_slot_candidate_rows(
    slot_candidates: pd.DataFrame,
    target: Any,
) -> pd.DataFrame:
    rows = slot_candidates.copy()
    rows["display_name"] = rows.apply(_display_name, axis=1)
    rows["slot_candidate_loss"] = rows.apply(
        lambda row: _candidate_slot_loss(row, target, str(row.get("slot", ""))),
        axis=1,
    )
    rows["candidate_status_preview"] = rows.apply(
        lambda row: _candidate_status(row, str(row.get("slot", ""))),
        axis=1,
    )
    rows["is_high_quality_candidate"] = rows.apply(
        lambda row: _is_high_quality_slot_candidate(row, str(row.get("slot", ""))),
        axis=1,
    )
    rows["macro_fit_preview"] = 1.0 - rows["slot_candidate_loss"].clip(lower=0.0, upper=1.0)
    return rows


def _candidate_selection_counts(candidate_days: list[dict[str, Any]]) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = Counter()
    for candidate in candidate_days:
        for meal in _selected_meals_from_candidate(candidate):
            slot = _meal_slot(meal)
            recipe_id = _meal_recipe_id(meal)
            if slot and recipe_id:
                counts[(slot, recipe_id)] += 1
    return counts


def _candidate_days_containing_recipe(candidate_days: list[dict[str, Any]], recipe_id: str) -> int:
    count = 0
    for candidate in candidate_days:
        ids = {_meal_recipe_id(meal) for meal in _selected_meals_from_candidate(candidate)}
        if recipe_id in ids:
            count += 1
    return count


def _top_alternatives(
    slot_rows: pd.DataFrame,
    repeated_recipe_id: str,
    target: Any,
    slot: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    dominant_rows = slot_rows[slot_rows["recipe_id"].astype(str) == repeated_recipe_id].copy()
    dominant = dominant_rows.sort_values("slot_candidate_loss").head(1)
    if dominant.empty:
        dominant_loss = None
        dominant_nutrition = {"kcal": 0.0, "protein": 0.0, "carbs": 0.0, "fat": 0.0}
    else:
        dominant_loss = _to_float(dominant.iloc[0].get("slot_candidate_loss"))
        dominant_nutrition = _nutrition(dominant.iloc[0])

    alternatives = (
        slot_rows[slot_rows["recipe_id"].astype(str) != repeated_recipe_id]
        .sort_values(["slot_candidate_loss", "score_preview"], ascending=[True, False])
        .head(limit)
    )
    rows = []
    for rank, (_, alt) in enumerate(alternatives.iterrows(), start=1):
        nutrition = _nutrition(alt)
        gaps = _macro_gap(alt, target, slot)
        rows.append(
            {
                "alternative_rank": rank,
                "alternative_recipe_id": alt.get("recipe_id"),
                "alternative_display_name": _display_name(alt),
                "alternative_status_preview": alt.get("candidate_status_preview"),
                "alternative_loss": _to_float(alt.get("slot_candidate_loss")),
                "loss_gap_vs_repeated": ""
                if dominant_loss is None
                else _to_float(alt.get("slot_candidate_loss")) - dominant_loss,
                "kcal_gap_vs_repeated": nutrition["kcal"] - dominant_nutrition["kcal"],
                "protein_gap_vs_repeated": nutrition["protein"] - dominant_nutrition["protein"],
                "carbs_gap_vs_repeated": nutrition["carbs"] - dominant_nutrition["carbs"],
                "fat_gap_vs_repeated": nutrition["fat"] - dominant_nutrition["fat"],
                "kcal_gap_vs_slot_target": gaps["kcal_gap"],
                "protein_gap_vs_slot_target": gaps["protein_gap"],
                "carbs_gap_vs_slot_target": gaps["carbs_gap"],
                "fat_gap_vs_slot_target": gaps["fat_gap"],
                "score_preview": _to_float(alt.get("score_preview")),
                "time_fit": _to_float(alt.get("time_fit")),
                "slot_fit": _to_float(alt.get("slot_fit")),
                "meal_realism_flags": ";".join(_flags(alt.get("meal_realism_practical_flags", alt.get("meal_realism_flags")))),
            }
        )
    return rows


def _root_cause_label(
    slot_rows: pd.DataFrame,
    repeated_recipe_id: str,
    slot: str,
    candidate_days: list[dict[str, Any]],
) -> str:
    alternatives = slot_rows[slot_rows["recipe_id"].astype(str) != repeated_recipe_id]
    accept_like = alternatives[alternatives["candidate_status_preview"] == "accept_like"]
    review_like = alternatives[alternatives["candidate_status_preview"] == "review_like"]
    repeated_selection_count = _candidate_days_containing_recipe(candidate_days, repeated_recipe_id)
    top_alts = alternatives.sort_values(["slot_candidate_loss", "score_preview"], ascending=[True, False]).head(5)
    close_alts = top_alts[top_alts["slot_candidate_loss"] <= (slot_rows[slot_rows["recipe_id"].astype(str) == repeated_recipe_id]["slot_candidate_loss"].min() + 0.08)]

    if accept_like.empty and review_like.empty:
        return "dataset_gap_no_comparable_alternatives"
    if accept_like.empty:
        return "alternatives_exist_but_review_level"
    if repeated_selection_count >= 6 and len(close_alts) < 2:
        return "objective_scoring_concentration"
    if slot in MAIN_SLOTS and len(accept_like) < 6:
        return "weak_accept_like_main_diversity"
    if slot == "breakfast":
        return "breakfast_objective_concentration"
    return "selector_prefers_lower_loss_recipe"


def _build_root_cause_rows(
    plan: dict[str, Any],
    candidate_days: list[dict[str, Any]],
    slot_rows: pd.DataFrame,
    target: Any,
) -> pd.DataFrame:
    repeated = _current_repeats(plan)
    rows: list[dict[str, Any]] = []
    selection_counts = _candidate_selection_counts(candidate_days)

    for recipe_id, data in sorted(repeated.items(), key=lambda item: item[1]["display_name"]):
        slots = data.get("slots") or []
        if not slots:
            slots = [slot for slot, rid in selection_counts if rid == recipe_id]
        for slot in sorted(set(slots)):
            if not slot:
                continue
            same_slot = slot_rows[slot_rows["slot"] == slot].copy()
            dominant_rows = same_slot[same_slot["recipe_id"].astype(str) == recipe_id].sort_values("slot_candidate_loss")
            dominant = dominant_rows.head(1)
            if dominant.empty:
                dominant_loss = ""
                dominant_status = ""
                dominant_flags = ""
                dominant_nutrition = {"kcal": 0.0, "protein": 0.0, "carbs": 0.0, "fat": 0.0}
                dominant_gaps = {"kcal_gap": 0.0, "protein_gap": 0.0, "carbs_gap": 0.0, "fat_gap": 0.0}
            else:
                row = dominant.iloc[0]
                dominant_loss = _to_float(row.get("slot_candidate_loss"))
                dominant_status = row.get("candidate_status_preview")
                dominant_flags = ";".join(_flags(row.get("meal_realism_practical_flags", row.get("meal_realism_flags"))))
                dominant_nutrition = _nutrition(row)
                dominant_gaps = _macro_gap(row, target, slot)

            alternatives = _top_alternatives(same_slot, recipe_id, target, slot, limit=10)
            accept_alts = same_slot[
                (same_slot["recipe_id"].astype(str) != recipe_id)
                & (same_slot["candidate_status_preview"] == "accept_like")
            ]["recipe_id"].nunique()
            review_alts = same_slot[
                (same_slot["recipe_id"].astype(str) != recipe_id)
                & (same_slot["candidate_status_preview"] == "review_like")
            ]["recipe_id"].nunique()
            hq_alts = same_slot[
                (same_slot["recipe_id"].astype(str) != recipe_id)
                & (same_slot["is_high_quality_candidate"])
            ]["recipe_id"].nunique()
            cause = _root_cause_label(same_slot, recipe_id, slot, candidate_days)
            base = {
                "recipe_id": recipe_id,
                "display_name": data.get("display_name"),
                "slot": slot,
                "current_plan_count": data.get("count"),
                "current_plan_day_indices": ";".join(str(day) for day in data.get("day_indices", [])),
                "candidate_day_selection_count": selection_counts.get((slot, recipe_id), 0),
                "dominant_loss": dominant_loss,
                "dominant_status_preview": dominant_status,
                "dominant_kcal": dominant_nutrition["kcal"],
                "dominant_protein": dominant_nutrition["protein"],
                "dominant_carbs": dominant_nutrition["carbs"],
                "dominant_fat": dominant_nutrition["fat"],
                "dominant_kcal_gap_vs_slot_target": dominant_gaps["kcal_gap"],
                "dominant_protein_gap_vs_slot_target": dominant_gaps["protein_gap"],
                "dominant_carbs_gap_vs_slot_target": dominant_gaps["carbs_gap"],
                "dominant_fat_gap_vs_slot_target": dominant_gaps["fat_gap"],
                "dominant_realism_flags": dominant_flags,
                "same_slot_accept_like_alternatives": accept_alts,
                "same_slot_review_like_alternatives": review_alts,
                "same_slot_high_quality_alternatives": hq_alts,
                "root_cause": cause,
            }
            if not alternatives:
                rows.append(base | {"alternative_rank": "", "alternative_display_name": "", "why_alternative_loses": "no_alternative_found"})
                continue
            for alt in alternatives:
                why = _why_alternative_loses(base, alt, slot)
                rows.append(base | alt | {"why_alternative_loses": why})
    return pd.DataFrame(rows)


def _why_alternative_loses(base: dict[str, Any], alt: dict[str, Any], slot: str) -> str:
    reasons = []
    if _to_float(alt.get("loss_gap_vs_repeated")) > 0.08:
        reasons.append("higher_loss")
    if _to_float(alt.get("protein_gap_vs_slot_target")) < _to_float(base.get("dominant_protein_gap_vs_slot_target")) - 8:
        reasons.append("lower_protein_fit")
    if _to_float(alt.get("carbs_gap_vs_slot_target")) < _to_float(base.get("dominant_carbs_gap_vs_slot_target")) - 15:
        reasons.append("lower_carb_fit")
    if _to_float(alt.get("kcal_gap_vs_slot_target")) < _to_float(base.get("dominant_kcal_gap_vs_slot_target")) - 120:
        reasons.append("lower_kcal_fit")
    if _to_float(alt.get("fat_gap_vs_slot_target")) > _to_float(base.get("dominant_fat_gap_vs_slot_target")) + 15:
        reasons.append("fat_too_high")
    if str(alt.get("alternative_status_preview")) != "accept_like":
        reasons.append("review_or_reject_preview")
    flags = set(_flags(alt.get("meal_realism_flags")))
    if flags.intersection(SEVERE_REALISM_FLAGS):
        reasons.append("severe_realism_flag")
    if slot in MAIN_SLOTS and _to_float(alt.get("protein_gap_vs_slot_target")) < -12:
        reasons.append("main_protein_gap")
    return ";".join(reasons) or "score_loss_marginal"


def _slot_deficit_rows(
    plan: dict[str, Any],
    candidate_days: list[dict[str, Any]],
    slot_rows: pd.DataFrame,
) -> pd.DataFrame:
    selected_meals = _selected_meals_from_plan(plan)
    selected_by_slot = defaultdict(list)
    for meal in selected_meals:
        selected_by_slot[_meal_slot(meal)].append(meal)

    alt_selection_counts = _candidate_selection_counts(candidate_days)
    repeated_by_slot = defaultdict(list)
    for slot, meals in selected_by_slot.items():
        counts = Counter(_meal_recipe_id(meal) for meal in meals)
        for recipe_id, count in counts.items():
            if count > 1:
                name = next(_meal_display_name(meal) for meal in meals if _meal_recipe_id(meal) == recipe_id)
                repeated_by_slot[slot].append(name)

    rows = []
    for slot in SLOTS:
        rows_for_slot = slot_rows[slot_rows["slot"] == slot].copy()
        unique_count = rows_for_slot["recipe_id"].nunique()
        hq_count = rows_for_slot[rows_for_slot["is_high_quality_candidate"]]["recipe_id"].nunique()
        accept_count = rows_for_slot[rows_for_slot["candidate_status_preview"] == "accept_like"]["recipe_id"].nunique()
        review_count = rows_for_slot[rows_for_slot["candidate_status_preview"] == "review_like"]["recipe_id"].nunique()
        selected_in_alternatives = {
            recipe_id for (candidate_slot, recipe_id), count in alt_selection_counts.items() if candidate_slot == slot and count > 0
        }
        repeated_names = sorted(set(repeated_by_slot.get(slot, [])))
        top_need = _top_missing_category(slot, accept_count, review_count, repeated_names, plan)
        rows.append(
            {
                "slot": slot,
                "unique_candidates": unique_count,
                "high_quality_candidates": hq_count,
                "accept_like_candidates": accept_count,
                "review_like_candidates": review_count,
                "candidates_selected_in_multi_day_alternatives": len(selected_in_alternatives),
                "repeated_recipes_in_current_plan": ";".join(repeated_names),
                "top_missing_category_need": top_need,
                "bottleneck_assessment": _slot_bottleneck_assessment(slot, accept_count, repeated_names, plan),
                "top_10_by_loss": "; ".join(
                    rows_for_slot.sort_values(["slot_candidate_loss", "score_preview"], ascending=[True, False])
                    .head(10)["display_name"]
                    .astype(str)
                    .tolist()
                ),
            }
        )
    return pd.DataFrame(rows)


def _top_missing_category(slot: str, accept_count: int, review_count: int, repeated_names: list[str], plan: dict[str, Any]) -> str:
    day_review_flags = _all_plan_flags(plan)
    if slot == "breakfast" and any("Waffles" in name for name in repeated_names):
        return "more_breakfast_alternatives"
    if slot in MAIN_SLOTS and any("Chicken and Broccoli Pasta" in name for name in repeated_names):
        return "more_lunch_dinner_carb_protein_meals"
    if slot in MAIN_SLOTS and {"low_protein_main", "mostly_carb_meal"}.intersection(day_review_flags):
        return "more_protein_balanced_carb_mains"
    if slot in MAIN_SLOTS and accept_count < 8:
        return "more_lunch_dinner_accept_like_mains"
    if slot == "snack" and accept_count < 6:
        return "more_snack_variety"
    if review_count > accept_count * 2:
        return "quality_gap_not_count_gap"
    return "no_major_slot_gap"


def _slot_bottleneck_assessment(slot: str, accept_count: int, repeated_names: list[str], plan: dict[str, Any]) -> str:
    if slot == "breakfast" and repeated_names:
        return "breakfast_is_active_bottleneck"
    if slot in MAIN_SLOTS and repeated_names:
        return "main_slot_repetition_bottleneck"
    if slot in MAIN_SLOTS and {"low_protein_main", "mostly_carb_meal"}.intersection(_all_plan_flags(plan)):
        return "main_slot_quality_bottleneck"
    if accept_count >= 8:
        return "enough_by_count_but_not_always_selected"
    return "candidate_quality_deficit"


def _all_plan_flags(plan: dict[str, Any]) -> set[str]:
    flags = set()
    for meal in _selected_meals_from_plan(plan):
        flags.update(_meal_flags(meal))
    for day in plan.get("days", []):
        for warning in day.get("warnings", []) or []:
            flags.update(_flags(warning))
    return flags


def _candidate_recipe_summary(candidate: dict[str, Any]) -> dict[str, str]:
    meals = _selected_meals_from_candidate(candidate)
    row: dict[str, str] = {}
    for slot in SLOTS:
        slot_meal = next((meal for meal in meals if _meal_slot(meal) == slot), {})
        row[f"{slot}_recipe_id"] = _meal_recipe_id(slot_meal)
        row[f"{slot}_display_name"] = _meal_display_name(slot_meal)
        row[f"{slot}_flags"] = ";".join(_meal_flags(slot_meal))
    return row


def _candidate_quality_rows(candidate_days: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for candidate in candidate_days:
        meals = _selected_meals_from_candidate(candidate)
        names = [_meal_display_name(meal) for meal in meals]
        ids = [_meal_recipe_id(meal) for meal in meals]
        flags = []
        for meal in meals:
            flags.extend(_meal_flags(meal))
        rows.append(
            {
                "candidate_day_id": candidate.get("candidate_day_id"),
                "source_mode": candidate.get("source_mode"),
                "alternative_rank": candidate.get("alternative_rank"),
                "validation_status": candidate.get("validation_status"),
                "quality_gate_status": candidate.get("quality_gate_status"),
                "candidate_class": candidate.get("quality_gate_status"),
                "base_day_loss": _to_float(candidate.get("base_day_loss", candidate.get("adjusted_day_loss"))),
                "adjusted_day_loss": _to_float(candidate.get("adjusted_day_loss", candidate.get("base_day_loss"))),
                "meal_realism_warning_count": len(set(flags).intersection(SEVERE_REALISM_FLAGS)),
                "selected_recipe_ids": ";".join(ids),
                "selected_display_names": ";".join(names),
                "contains_waffles": any(name == "Mom's Best Waffles" for name in names),
                "contains_chicken_broccoli_pasta": any(name == "Chicken and Broccoli Pasta" for name in names),
                "contains_thai_fried_rice": any(name == "Thai Fried Rice" for name in names),
            }
            | _candidate_recipe_summary(candidate)
        )
    return pd.DataFrame(rows)


def _combination_metrics(candidates: tuple[dict[str, Any], ...], strong_repeats: bool = False) -> dict[str, Any]:
    meals = []
    for day_number, candidate in enumerate(candidates, start=1):
        for meal in _selected_meals_from_candidate(candidate):
            enriched = dict(meal)
            enriched["combo_day"] = day_number
            meals.append(enriched)

    recipe_counts = Counter(_meal_recipe_id(meal) for meal in meals if _meal_recipe_id(meal))
    slot_recipe_counts = Counter((_meal_slot(meal), _meal_recipe_id(meal)) for meal in meals if _meal_slot(meal) and _meal_recipe_id(meal))
    quality_counts = Counter(str(candidate.get("quality_gate_status", "unknown")) for candidate in candidates)
    validation_counts = Counter(str(candidate.get("validation_status", "unknown")) for candidate in candidates)
    repeated_ids = sorted(recipe_id for recipe_id, count in recipe_counts.items() if count > 1)
    repeated_main_ids = sorted(
        recipe_id for (slot, recipe_id), count in slot_recipe_counts.items() if count > 1 and slot in MAIN_SLOTS
    )
    repeated_breakfast_ids = sorted(
        recipe_id for (slot, recipe_id), count in slot_recipe_counts.items() if count > 1 and slot == "breakfast"
    )
    repeated_same_slot_ids = sorted(recipe_id for (_, recipe_id), count in slot_recipe_counts.items() if count > 1)
    avg_loss = sum(_to_float(candidate.get("base_day_loss", candidate.get("adjusted_day_loss"))) for candidate in candidates) / max(len(candidates), 1)
    realism_warning_count = 0
    for meal in meals:
        realism_warning_count += len(set(_meal_flags(meal)).intersection(SEVERE_REALISM_FLAGS))
    repeat_weight = 0.50 if strong_repeats else 0.28
    same_slot_weight = 0.70 if strong_repeats else 0.38
    main_weight = 0.45 if strong_repeats else 0.28
    breakfast_weight = 0.28 if strong_repeats else 0.18
    repetition_penalty = (
        repeat_weight * len(repeated_ids)
        + same_slot_weight * len(repeated_same_slot_ids)
        + main_weight * len(repeated_main_ids)
        + breakfast_weight * len(repeated_breakfast_ids)
    ) / 6.0
    review_penalty = quality_counts.get("review", 0) / max(len(candidates), 1)
    reject_penalty = quality_counts.get("reject", 0)
    realism_penalty = min(realism_warning_count / 8.0, 1.0)
    audit_loss = 0.55 * avg_loss + 0.20 * repetition_penalty + 0.15 * review_penalty + 0.10 * realism_penalty + reject_penalty
    return {
        "avg_base_day_loss": avg_loss,
        "audit_loss": audit_loss,
        "validation_counts": validation_counts,
        "quality_counts": quality_counts,
        "accept_day_count": quality_counts.get("accept", 0),
        "review_day_count": quality_counts.get("review", 0),
        "reject_day_count": quality_counts.get("reject", 0),
        "valid_day_count": validation_counts.get("valid", 0),
        "repeated_recipe_ids": repeated_ids,
        "repeated_same_slot_ids": repeated_same_slot_ids,
        "repeated_main_recipe_ids": repeated_main_ids,
        "repeated_breakfast_recipe_ids": repeated_breakfast_ids,
        "unique_recipe_count": len(recipe_counts),
        "selected_display_names_by_day": _combo_display_names_by_day(candidates),
        "selected_recipe_ids": sorted(recipe_counts),
        "selected_candidate_day_ids": [candidate.get("candidate_day_id") for candidate in candidates],
        "realism_warning_count": realism_warning_count,
    }


def _combo_display_names_by_day(candidates: tuple[dict[str, Any], ...]) -> str:
    day_strings = []
    for index, candidate in enumerate(candidates, start=1):
        meals = _selected_meals_from_candidate(candidate)
        names = []
        for slot in SLOTS:
            meal = next((item for item in meals if _meal_slot(item) == slot), {})
            names.append(f"{slot}:{_meal_display_name(meal)}")
        day_strings.append(f"D{index}[" + " | ".join(names) + "]")
    return " || ".join(day_strings)


def _is_valid_accept_review_combo(metrics: dict[str, Any]) -> bool:
    return metrics["valid_day_count"] == 3 and metrics["reject_day_count"] == 0


def _find_best_combo(
    candidate_days: list[dict[str, Any]],
    constraint: str,
    strong_repeats: bool = False,
) -> tuple[tuple[dict[str, Any], ...] | None, dict[str, Any] | None]:
    best_combo = None
    best_metrics = None
    best_key = None
    for combo in itertools.combinations(candidate_days, 3):
        metrics = _combination_metrics(combo, strong_repeats=strong_repeats)
        if constraint == "all_accept_no_repeat":
            if not (metrics["valid_day_count"] == 3 and metrics["accept_day_count"] == 3 and not metrics["repeated_recipe_ids"]):
                continue
        elif constraint == "no_repeat_same_slot":
            if not (_is_valid_accept_review_combo(metrics) and not metrics["repeated_same_slot_ids"]):
                continue
        elif constraint == "strong_repeat_penalty":
            if not _is_valid_accept_review_combo(metrics):
                continue
        elif constraint == "no_repeat_main_at_most_one_review":
            if not (
                _is_valid_accept_review_combo(metrics)
                and metrics["review_day_count"] <= 1
                and not metrics["repeated_main_recipe_ids"]
            ):
                continue
        elif constraint == "best_no_repeat":
            if not (_is_valid_accept_review_combo(metrics) and not metrics["repeated_recipe_ids"]):
                continue
        else:
            continue

        key = (
            metrics["audit_loss"],
            metrics["review_day_count"],
            len(metrics["repeated_recipe_ids"]),
            metrics["avg_base_day_loss"],
            ";".join(str(item) for item in metrics["selected_candidate_day_ids"]),
        )
        if best_key is None or key < best_key:
            best_combo = combo
            best_metrics = metrics
            best_key = key
    return best_combo, best_metrics


def _current_combo(candidate_days: list[dict[str, Any]], plan: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    ids = [day.get("candidate_day_id") for day in plan.get("days", []) if day.get("candidate_day_id")]
    by_id = {candidate.get("candidate_day_id"): candidate for candidate in candidate_days}
    candidates = tuple(by_id[candidate_id] for candidate_id in ids if candidate_id in by_id)
    return candidates


def _current_metrics_from_plan(plan: dict[str, Any]) -> dict[str, Any]:
    meals_by_day: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for meal in _selected_meals_from_plan(plan):
        meals_by_day[_to_int(meal.get("day_index"))].append(meal)

    recipe_counts = Counter(_meal_recipe_id(meal) for meal in _selected_meals_from_plan(plan) if _meal_recipe_id(meal))
    slot_recipe_counts = Counter((_meal_slot(meal), _meal_recipe_id(meal)) for meal in _selected_meals_from_plan(plan) if _meal_slot(meal) and _meal_recipe_id(meal))
    quality_counts = Counter(str(day.get("quality_gate_status", "unknown")) for day in plan.get("days", []))
    validation_counts = Counter(str(day.get("validation_status", "unknown")) for day in plan.get("days", []))
    avg_loss = _to_float(plan.get("average_day_loss"))
    return {
        "avg_base_day_loss": avg_loss,
        "audit_loss": _to_float(plan.get("multi_day_loss")),
        "valid_day_count": validation_counts.get("valid", 0),
        "accept_day_count": quality_counts.get("accept", 0),
        "review_day_count": quality_counts.get("review", 0),
        "reject_day_count": quality_counts.get("reject", 0),
        "repeated_recipe_ids": sorted(recipe_id for recipe_id, count in recipe_counts.items() if count > 1),
        "repeated_same_slot_ids": sorted(recipe_id for (_, recipe_id), count in slot_recipe_counts.items() if count > 1),
        "repeated_main_recipe_ids": sorted(recipe_id for (slot, recipe_id), count in slot_recipe_counts.items() if count > 1 and slot in MAIN_SLOTS),
        "repeated_breakfast_recipe_ids": sorted(recipe_id for (slot, recipe_id), count in slot_recipe_counts.items() if count > 1 and slot == "breakfast"),
        "unique_recipe_count": len(recipe_counts),
        "selected_display_names_by_day": _plan_display_names_by_day(plan),
        "selected_candidate_day_ids": [day.get("candidate_day_id") for day in plan.get("days", [])],
    }


def _plan_display_names_by_day(plan: dict[str, Any]) -> str:
    day_strings = []
    for day in plan.get("days", []):
        meals = day.get("selected_meals", [])
        names = []
        for slot in SLOTS:
            meal = next((item for item in meals if _meal_slot(item) == slot), {})
            names.append(f"{slot}:{_meal_display_name(meal)}")
        day_strings.append(f"D{day.get('day_index')}[" + " | ".join(names) + "]")
    return " || ".join(day_strings)


def _simulation_rows(candidate_days: list[dict[str, Any]], plan: dict[str, Any]) -> pd.DataFrame:
    current_metrics = _current_metrics_from_plan(plan)

    scenarios = [
        ("current_global_alternatives_3_day", None, False),
        ("stricter_no_repeat_same_slot_audit_only", "no_repeat_same_slot", False),
        ("allow_review_strong_repeat_penalty_audit_only", "strong_repeat_penalty", True),
        ("best_possible_no_repeat_audit_only", "best_no_repeat", False),
        ("all_accept_no_repeat_feasibility_audit_only", "all_accept_no_repeat", False),
        ("no_repeat_main_at_most_one_review_feasibility_audit_only", "no_repeat_main_at_most_one_review", False),
    ]

    rows = []
    current_names = set(_names_from_display_blob(current_metrics.get("selected_display_names_by_day", "")))
    for scenario, constraint, strong in scenarios:
        if constraint is None:
            metrics = current_metrics
            found = True
        else:
            _, metrics = _find_best_combo(candidate_days, constraint, strong_repeats=strong)
            found = metrics is not None
        if not found or metrics is None:
            rows.append(
                {
                    "scenario": scenario,
                    "found": False,
                    "strict_result": "not_feasible",
                    "selected_display_names_by_day": "",
                }
            )
            continue
        names = set(_names_from_display_blob(metrics.get("selected_display_names_by_day", "")))
        added_names = sorted(names - current_names)
        removed_names = sorted(current_names - names)
        rows.append(
            {
                "scenario": scenario,
                "found": True,
                "valid_day_count": metrics["valid_day_count"],
                "accept_day_count": metrics["accept_day_count"],
                "review_day_count": metrics["review_day_count"],
                "reject_day_count": metrics["reject_day_count"],
                "avg_base_day_loss": metrics["avg_base_day_loss"],
                "audit_loss": metrics["audit_loss"],
                "quality_cost_avg_loss_vs_current": metrics["avg_base_day_loss"] - current_metrics["avg_base_day_loss"],
                "audit_loss_delta_vs_current": metrics["audit_loss"] - current_metrics["audit_loss"],
                "unique_recipe_count": metrics["unique_recipe_count"],
                "repeated_recipe_count": len(metrics["repeated_recipe_ids"]),
                "repeated_recipe_ids": ";".join(metrics["repeated_recipe_ids"]),
                "repeated_same_slot_recipe_ids": ";".join(metrics["repeated_same_slot_ids"]),
                "repeated_main_recipe_ids": ";".join(metrics["repeated_main_recipe_ids"]),
                "repeated_breakfast_recipe_ids": ";".join(metrics["repeated_breakfast_recipe_ids"]),
                "selected_candidate_day_ids": ";".join(str(item) for item in metrics["selected_candidate_day_ids"]),
                "selected_display_names_by_day": metrics["selected_display_names_by_day"],
                "replacement_recipes_added_vs_current": ";".join(added_names),
                "replacement_recipes_removed_vs_current": ";".join(removed_names),
                "strict_result": _strict_scenario_result(scenario, metrics),
            }
        )
    return pd.DataFrame(rows)


def _names_from_display_blob(blob: str) -> list[str]:
    names = []
    for part in str(blob).replace("||", "|").split("|"):
        if ":" not in part:
            continue
        value = part.split(":", 1)[1].strip()
        if value.endswith("]"):
            value = value[:-1].strip()
        if value:
            names.append(value)
    return names


def _strict_scenario_result(scenario: str, metrics: dict[str, Any]) -> str:
    if scenario == "all_accept_no_repeat_feasibility_audit_only":
        return "feasible" if metrics["accept_day_count"] == 3 and not metrics["repeated_recipe_ids"] else "not_feasible"
    if scenario == "no_repeat_main_at_most_one_review_feasibility_audit_only":
        ok = metrics["valid_day_count"] == 3 and metrics["review_day_count"] <= 1 and not metrics["repeated_main_recipe_ids"]
        return "feasible" if ok else "not_feasible"
    if metrics["reject_day_count"] > 0:
        return "bad"
    if metrics["review_day_count"] > 1 or metrics["repeated_recipe_ids"]:
        return "review"
    return "good"


def _build_recommendations(
    slot_deficit_df: pd.DataFrame,
    simulation_df: pd.DataFrame,
    root_cause_df: pd.DataFrame,
) -> pd.DataFrame:
    all_accept_no_repeat = _scenario_found(simulation_df, "all_accept_no_repeat_feasibility_audit_only")
    no_repeat_main_review = _scenario_found(simulation_df, "no_repeat_main_at_most_one_review_feasibility_audit_only")
    breakfast_bottleneck = _slot_need(slot_deficit_df, "breakfast") == "more_breakfast_alternatives"
    main_bottleneck = any(
        need in {"more_lunch_dinner_carb_protein_meals", "more_protein_balanced_carb_mains", "more_lunch_dinner_accept_like_mains"}
        for need in slot_deficit_df["top_missing_category_need"].astype(str).tolist()
    )

    if all_accept_no_repeat and no_repeat_main_review:
        recommendation = "D_tune_multi_day_repetition_penalty"
        target_count = "0"
        rationale = "Audit-only combinations show a usable no-repeat plan already exists; current selector objective leaves variety on the table."
    elif breakfast_bottleneck and main_bottleneck:
        recommendation = "C_plus20_mixed_breakfast_lunch_dinner"
        target_count = "+20"
        rationale = "Plus10 improved mains, but Waffles and Chicken and Broccoli Pasta still repeat and one review day remains from low-protein carb main behavior."
    elif breakfast_bottleneck:
        recommendation = "A_plus10_breakfast_alternatives"
        target_count = "+10"
        rationale = "Breakfast has active repetition around Waffles while main repetition can be handled by existing alternatives."
    elif main_bottleneck:
        recommendation = "B_plus10_lunch_dinner_complete_carb_protein_meals"
        target_count = "+10"
        rationale = "Lunch/dinner are still the main quality gap; add more complete carb-protein meals before tuning logic."
    else:
        recommendation = "E_accept_current_as_technical_demo"
        target_count = "0"
        rationale = "No clear data gap remains in this audit, but the plan is still review-level."

    rows = [
        {
            "recommendation": recommendation,
            "target_count": target_count,
            "priority": 1,
            "target_recipe_types": (
                "5 breakfast options similar macro strength to waffles; 10 lunch/dinner complete carb-protein mains; "
                "5 protein-balanced fish/turkey/pork/legume mains"
                if recommendation.startswith("C_")
                else _target_recipe_types_for_recommendation(recommendation)
            ),
            "target_macros": (
                "breakfast 400-650 kcal, protein 15-30g, carbs 45-90g; mains 550-850 kcal, protein 30-55g, carbs 60-115g, fat 10-30g"
            ),
            "examples_desired": (
                "oatmeal/yogurt/fruit breakfasts; cottage-cheese or egg-potato breakfasts; chicken rice/pasta/potato bowls; "
                "turkey pasta/rice; fish with potatoes/rice; pork tenderloin with potatoes; lentil/bean rice mains"
            ),
            "what_not_to_add": (
                "components, sauces, salads without carbs/protein, desserts, drinks, snack-only rows, giant casseroles, very long prep recipes, obscure ingredients"
            ),
            "rationale": rationale,
            "all_accept_no_repeat_feasible_audit_only": all_accept_no_repeat,
            "no_repeat_main_at_most_one_review_feasible_audit_only": no_repeat_main_review,
            "root_causes_seen": ";".join(sorted(root_cause_df["root_cause"].dropna().astype(str).unique())) if not root_cause_df.empty else "",
        }
    ]
    return pd.DataFrame(rows)


def _scenario_found(simulation_df: pd.DataFrame, scenario: str) -> bool:
    rows = simulation_df[simulation_df["scenario"] == scenario]
    if rows.empty:
        return False
    return bool(rows.iloc[0].get("found")) and str(rows.iloc[0].get("strict_result")) == "feasible"


def _slot_need(slot_deficit_df: pd.DataFrame, slot: str) -> str:
    rows = slot_deficit_df[slot_deficit_df["slot"] == slot]
    if rows.empty:
        return ""
    return str(rows.iloc[0].get("top_missing_category_need", ""))


def _target_recipe_types_for_recommendation(recommendation: str) -> str:
    if recommendation.startswith("A_"):
        return "10 breakfast recipes with real protein and carb structure"
    if recommendation.startswith("B_"):
        return "10 lunch/dinner complete carb-protein meals, especially fish/turkey/pork/chicken/rice/pasta/potato"
    return "no new recipes"


def _summary_text(
    plan: dict[str, Any],
    root_cause_df: pd.DataFrame,
    slot_deficit_df: pd.DataFrame,
    simulation_df: pd.DataFrame,
    recommendation_df: pd.DataFrame,
) -> str:
    summary = plan.get("multi_day_summary", {})
    lines = [
        "Round27 plus10 bottleneck audit",
        "",
        f"Dataset profile: {DATASET_PROFILE}",
        f"Valid days: {summary.get('valid_day_count', plan.get('valid_day_count'))}/3",
        f"Accept days: {summary.get('accept_day_count', plan.get('accept_day_count'))}",
        f"Review days: {summary.get('review_day_count', plan.get('review_day_count'))}",
        f"Unique recipes: {summary.get('unique_recipe_count', plan.get('unique_recipe_count'))}",
        f"Repeated recipes: {', '.join(plan.get('repeated_recipe_ids', []) or summary.get('repeated_recipe_ids', []))}",
        f"Multi-day loss: {plan.get('multi_day_loss')}",
        f"Average day loss: {plan.get('average_day_loss')}",
        f"Strict verdict: {summary.get('strict_verdict') or summary.get('plan_classification') or plan.get('strict_verdict')}",
        "",
        "Repeated recipe root causes:",
    ]

    if root_cause_df.empty:
        lines.append("- No repeated recipes found in the current plus10 plan.")
    else:
        for (recipe_id, display_name, slot), rows in root_cause_df.groupby(["recipe_id", "display_name", "slot"], dropna=False):
            first = rows.iloc[0]
            lines.append(
                f"- {display_name} [{slot}]: cause={first.get('root_cause')}, "
                f"same-slot accept alternatives={first.get('same_slot_accept_like_alternatives')}, "
                f"candidate-day selections={first.get('candidate_day_selection_count')}"
            )

    lines.extend(["", "Slot deficits:"])
    for _, row in slot_deficit_df.iterrows():
        lines.append(
            f"- {row.get('slot')}: unique={row.get('unique_candidates')}, high_quality={row.get('high_quality_candidates')}, "
            f"accept_like={row.get('accept_like_candidates')}, review_like={row.get('review_like_candidates')}, "
            f"selected_in_alternatives={row.get('candidates_selected_in_multi_day_alternatives')}, "
            f"repeated={row.get('repeated_recipes_in_current_plan') or 'none'}, need={row.get('top_missing_category_need')}"
        )

    lines.extend(["", "Audit-only simulations:"])
    for _, row in simulation_df.iterrows():
        if not bool(row.get("found")):
            lines.append(f"- {row.get('scenario')}: not feasible")
            continue
        lines.append(
            f"- {row.get('scenario')}: result={row.get('strict_result')}, "
            f"accept={row.get('accept_day_count')}, review={row.get('review_day_count')}, "
            f"repeated={row.get('repeated_recipe_count')}, avg_loss={row.get('avg_base_day_loss')}, "
            f"audit_loss={row.get('audit_loss')}, quality_cost={row.get('quality_cost_avg_loss_vs_current')}"
        )

    rec = recommendation_df.iloc[0] if not recommendation_df.empty else {}
    lines.extend(
        [
            "",
            "Recommendation:",
            f"- {rec.get('recommendation', 'unknown')}",
            f"- Target count: {rec.get('target_count', '')}",
            f"- Rationale: {rec.get('rationale', '')}",
            f"- Target types: {rec.get('target_recipe_types', '')}",
            f"- Target macros: {rec.get('target_macros', '')}",
            f"- Do not add: {rec.get('what_not_to_add', '')}",
            "",
            "Strict assessment:",
            _strict_overall_assessment(simulation_df, recommendation_df),
        ]
    )
    return "\n".join(lines) + "\n"


def _strict_overall_assessment(simulation_df: pd.DataFrame, recommendation_df: pd.DataFrame) -> str:
    all_accept = _scenario_found(simulation_df, "all_accept_no_repeat_feasibility_audit_only")
    no_repeat_main = _scenario_found(simulation_df, "no_repeat_main_at_most_one_review_feasibility_audit_only")
    recommendation = ""
    if not recommendation_df.empty:
        recommendation = str(recommendation_df.iloc[0].get("recommendation", ""))
    if all_accept:
        return "A no-repeat all-accept combination exists audit-only; selector tuning may be enough before more data."
    if no_repeat_main and recommendation.startswith("C_"):
        return "Main repetition can be avoided audit-only, but breakfast and day quality still justify a mixed targeted expansion."
    if recommendation.startswith(("A_", "B_", "C_")):
        return "Plus10 improved the plan, but the remaining bottleneck is still data quality/diversity, not just threshold tuning."
    return "Current plan remains a technical demo result, not a strong multi-day meal plan."


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    context = _prepare_context()
    plan = _generate_current_plan(context)
    candidate_days = _generate_candidate_days(context)
    slot_rows = _prepare_slot_candidate_rows(context["slot_candidates"], context["target"])

    root_cause_df = _build_root_cause_rows(plan, candidate_days, slot_rows, context["target"])
    slot_deficit_df = _slot_deficit_rows(plan, candidate_days, slot_rows)
    candidate_quality_df = _candidate_quality_rows(candidate_days)
    simulation_df = _simulation_rows(candidate_days, plan)
    recommendation_df = _build_recommendations(slot_deficit_df, simulation_df, root_cause_df)

    root_cause_df.to_csv(ROOT_CAUSES_PATH, index=False)
    slot_deficit_df.to_csv(SLOT_DEFICIT_PATH, index=False)
    combined_candidate_quality = pd.concat(
        [
            candidate_quality_df.assign(row_type="candidate_day"),
            simulation_df.assign(row_type="simulation"),
        ],
        ignore_index=True,
        sort=False,
    )
    combined_candidate_quality.to_csv(CANDIDATE_QUALITY_PATH, index=False)
    recommendation_df.to_csv(RECOMMENDATIONS_PATH, index=False)
    SUMMARY_PATH.write_text(
        _summary_text(plan, root_cause_df, slot_deficit_df, simulation_df, recommendation_df),
        encoding="utf-8",
    )

    print(f"Wrote {SUMMARY_PATH}")
    print(f"Wrote {ROOT_CAUSES_PATH}")
    print(f"Wrote {SLOT_DEFICIT_PATH}")
    print(f"Wrote {RECOMMENDATIONS_PATH}")
    print(f"Wrote {CANDIDATE_QUALITY_PATH}")


if __name__ == "__main__":
    main()
