from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.data_loader import (
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
)
from src.generator_v1.recipe_time_adapter import compute_time_features


RECIPES_DRAFT_DIR = ROOT / "data/recipesdb/draft"
RECIPES_AUDIT_DIR = ROOT / "data/recipesdb/audit"
RECIPES_PATH = ROOT / V1_2_DEMO_FINAL_RECIPES_PATH
INGREDIENTS_PATH = ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH
NUTRITION_PATH = ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH
TIME_CACHE_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_data_qa_time_layer_cache.csv"
SUMMARY_PATH = RECIPES_AUDIT_DIR / "data_qa_time_coverage_summary.txt"
MISSING_PATH = RECIPES_AUDIT_DIR / "data_qa_time_missing_recipes.csv"
COVERAGE_PATH = RECIPES_AUDIT_DIR / "data_qa_time_coverage_recipes.csv"
QUEUE_PATH = RECIPES_DRAFT_DIR / "data_qa_time_completion_queue.csv"
COOKING_STEPS_SUMMARY_PATH = RECIPES_AUDIT_DIR / "data_qa_cooking_steps_missing_summary.txt"
COOKING_STEPS_MISSING_PATH = RECIPES_AUDIT_DIR / "data_qa_cooking_steps_missing_recipes.csv"

TIME_FIELDS = (
    "total_elapsed_time_min",
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "effective_time_min_for_scoring",
    "time_confidence",
    "time_estimation_method",
)


def main() -> None:
    RECIPES_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    RECIPES_DRAFT_DIR.mkdir(parents=True, exist_ok=True)

    recipes = pd.read_csv(RECIPES_PATH)
    nutrition = pd.read_csv(NUTRITION_PATH)
    active = _active_displayable_recipes(recipes, nutrition)
    generated_ids = _generated_recipe_ids_from_price_audit()

    coverage_rows = [_time_coverage_row(row) for _, row in active.iterrows()]
    missing_rows = [row for row in coverage_rows if _truthy(row.get("missing_after_task"))]
    queue_rows = [
        row
        for row in coverage_rows
        if row.get("time_estimation_method") in {"fallback_by_recipe_kind", "emergency_fallback"}
        or row.get("time_confidence") in {"low", "very_low"}
        or _truthy(row.get("missing_before_task"))
    ]
    cache_rows = [_time_cache_row(row) for row in coverage_rows]
    cooking_rows = [_cooking_steps_row(row, generated_ids) for _, row in active.iterrows()]
    cooking_missing_rows = [row for row in cooking_rows if not _truthy(row.get("has_directions"))]

    _write_csv(COVERAGE_PATH, coverage_rows, _coverage_columns())
    _write_csv(MISSING_PATH, missing_rows, _coverage_columns())
    _write_csv(QUEUE_PATH, queue_rows, _coverage_columns())
    _write_csv(TIME_CACHE_PATH, cache_rows, _cache_columns())
    _write_csv(COOKING_STEPS_MISSING_PATH, cooking_missing_rows, _cooking_columns())

    SUMMARY_PATH.write_text(
        "\n".join(_summary_lines(active, coverage_rows, missing_rows, queue_rows)),
        encoding="utf-8",
    )
    COOKING_STEPS_SUMMARY_PATH.write_text(
        "\n".join(_cooking_summary_lines(cooking_rows, cooking_missing_rows)),
        encoding="utf-8",
    )


def _active_displayable_recipes(recipes: pd.DataFrame, nutrition: pd.DataFrame) -> pd.DataFrame:
    merged = recipes.merge(nutrition, on="recipe_id", how="left", suffixes=("", "_nutrition"))
    mask = pd.to_numeric(merged.get("is_active"), errors="coerce").eq(1)
    if "has_ingredients_parsed" in merged.columns:
        mask &= pd.to_numeric(merged["has_ingredients_parsed"], errors="coerce").eq(1)
    if "scope_status" in merged.columns:
        mask &= merged["scope_status"].astype(str).isin(
            {
                "v1_1_generator_ready_slot_checked_time_enriched_snack_curated_draft",
                "v1_1_generator_ready_draft",
                "v1_2_generator_ready_draft",
            }
        )
    if "cache_status" in merged.columns:
        mask &= merged["cache_status"].astype(str).isin(
            {"usable_from_mapped_ingredients", "partial_from_mapped_ingredients"}
        )
    for column in (
        "energy_kcal_per_serving",
        "protein_g_per_serving",
        "carbs_g_per_serving",
        "fat_g_per_serving",
    ):
        if column in merged.columns:
            mask &= pd.to_numeric(merged[column], errors="coerce").notna()
    return merged.loc[mask].copy()


def _time_coverage_row(row: pd.Series) -> dict[str, Any]:
    features = compute_time_features(row)
    missing_before = _missing_before(row)
    missing_after = any(_is_missing(features.get(field)) for field in TIME_FIELDS)
    direct_time = all(not _is_missing(row.get(field)) for field in ("prep_time_min", "cook_time_min", "total_time_min"))
    has_time_layer = any(
        column in row.index and not _is_missing(row.get(column))
        for column in ("total_elapsed_time_min", "time_confidence", "time_warnings")
    )
    return {
        "recipe_id": row.get("recipe_id"),
        "display_name": row.get("display_name") or row.get("recipe_name"),
        "recipe_category": row.get("recipe_category"),
        "recipe_kind": row.get("recipe_kind"),
        "is_active": row.get("is_active"),
        "direct_time_available": direct_time,
        "time_layer_available": has_time_layer,
        "prep_time_min": row.get("prep_time_min"),
        "cook_time_min": row.get("cook_time_min"),
        "total_time_min": row.get("total_time_min"),
        "total_elapsed_time_min": features.get("total_elapsed_time_min"),
        "active_time_estimated_min": features.get("active_time_estimated_min"),
        "passive_time_estimated_min": features.get("passive_time_estimated_min"),
        "effective_time_min_for_scoring": features.get("effective_time_min_for_scoring"),
        "time_confidence": features.get("time_confidence"),
        "time_estimation_method": features.get("time_estimation_method"),
        "time_warnings": _join(features.get("time_warnings")),
        "time_estimation_reasons": _join(features.get("time_estimation_reasons")),
        "missing_before_task": missing_before,
        "missing_after_task": missing_after,
    }


def _time_cache_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "recipe_id": row.get("recipe_id"),
        "total_elapsed_time_min": row.get("total_elapsed_time_min"),
        "active_time_estimated_min": row.get("active_time_estimated_min"),
        "passive_time_estimated_min": row.get("passive_time_estimated_min"),
        "effective_time_min_for_scoring": row.get("effective_time_min_for_scoring"),
        "time_confidence": row.get("time_confidence"),
        "time_estimation_method": row.get("time_estimation_method"),
        "time_warnings": row.get("time_warnings"),
        "time_estimation_reasons": row.get("time_estimation_reasons"),
    }


def _cooking_steps_row(row: pd.Series, generated_ids: set[str]) -> dict[str, Any]:
    step_count = _to_int(row.get("directions_step_count"))
    has_directions = step_count > 0 or bool(_directions_list(row.get("directions_json")))
    recipe_id = _clean_text(row.get("recipe_id"))
    appears = recipe_id in generated_ids
    priority = "high" if appears else "medium"
    return {
        "recipe_id": recipe_id,
        "display_name": row.get("display_name") or row.get("recipe_name"),
        "recipe_category": row.get("recipe_category"),
        "current_directions_step_count": step_count,
        "has_directions": has_directions,
        "appears_in_demo_plan": "yes" if appears else "no",
        "priority": priority,
        "suggested_action": "" if has_directions else "DATA-QA-2 cooking steps completion",
    }


def _generated_recipe_ids_from_price_audit() -> set[str]:
    path = ROOT / "data/grocery/audit/data_qa_price_coverage_items.csv"
    if not path.exists():
        return set()
    ids: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            context = _clean_text(row.get("current_output_context"))
            if not context.startswith(("individual_generated", "household_generated")):
                continue
            for recipe_id in _clean_text(row.get("source_recipe_ids")).split(";"):
                if recipe_id:
                    ids.add(recipe_id)
    return ids


def _summary_lines(
    active: pd.DataFrame,
    coverage_rows: list[dict[str, Any]],
    missing_rows: list[dict[str, Any]],
    queue_rows: list[dict[str, Any]],
) -> list[str]:
    return [
        "DATA-QA-1 recipe time coverage summary",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"recipes_path={V1_2_DEMO_FINAL_RECIPES_PATH}",
        f"ingredients_path={V1_2_DEMO_FINAL_INGREDIENTS_PATH}",
        f"nutrition_path={V1_2_DEMO_FINAL_NUTRITION_PATH}",
        f"time_cache_output_path={TIME_CACHE_PATH.relative_to(ROOT)}",
        f"total_recipes_checked={len(coverage_rows)}",
        f"active_recipes={len(active)}",
        f"recipes_with_direct_time={sum(1 for row in coverage_rows if _truthy(row.get('direct_time_available')))}",
        f"recipes_with_time_layer_estimate={sum(1 for row in coverage_rows if _truthy(row.get('time_layer_available')))}",
        f"recipes_needing_fallback={len(queue_rows)}",
        f"recipes_still_missing_after_this_task={len(missing_rows)}",
        f"missing_before_this_task={sum(1 for row in coverage_rows if _truthy(row.get('missing_before_task')))}",
        f"coverage_recipes_path={COVERAGE_PATH.relative_to(ROOT)}",
        f"missing_recipes_path={MISSING_PATH.relative_to(ROOT)}",
        f"time_completion_queue_path={QUEUE_PATH.relative_to(ROOT)}",
        "target=0 app-facing missing cooking time estimates",
    ]


def _cooking_summary_lines(
    cooking_rows: list[dict[str, Any]],
    missing_rows: list[dict[str, Any]],
) -> list[str]:
    priorities = _count_by(missing_rows, "priority")
    return [
        "DATA-QA-1 cooking steps audit summary",
        "cooking_steps_generated=false",
        f"recipes_checked={len(cooking_rows)}",
        f"recipes_missing_cooking_steps={len(missing_rows)}",
        f"missing_priority_counts={json.dumps(priorities, sort_keys=True)}",
        f"missing_steps_queue_path={COOKING_STEPS_MISSING_PATH.relative_to(ROOT)}",
        "next_task=DATA-QA-2 cooking steps completion",
    ]


def _missing_before(row: pd.Series) -> bool:
    direct_fields = ("prep_time_min", "cook_time_min", "total_time_min")
    completion_fields = (
        "total_elapsed_time_min",
        "active_time_estimated_min",
        "passive_time_estimated_min",
        "effective_time_min_for_scoring",
        "time_confidence",
        "time_estimation_method",
    )
    return any(_is_missing(row.get(field)) for field in direct_fields) or any(
        field not in row.index or _is_missing(row.get(field)) for field in completion_fields
    )


def _directions_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    text = _clean_text(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _coverage_columns() -> list[str]:
    return [
        "recipe_id",
        "display_name",
        "recipe_category",
        "recipe_kind",
        "is_active",
        "direct_time_available",
        "time_layer_available",
        "prep_time_min",
        "cook_time_min",
        "total_time_min",
        "total_elapsed_time_min",
        "active_time_estimated_min",
        "passive_time_estimated_min",
        "effective_time_min_for_scoring",
        "time_confidence",
        "time_estimation_method",
        "time_warnings",
        "time_estimation_reasons",
        "missing_before_task",
        "missing_after_task",
    ]


def _cache_columns() -> list[str]:
    return [
        "recipe_id",
        "total_elapsed_time_min",
        "active_time_estimated_min",
        "passive_time_estimated_min",
        "effective_time_min_for_scoring",
        "time_confidence",
        "time_estimation_method",
        "time_warnings",
        "time_estimation_reasons",
    ]


def _cooking_columns() -> list[str]:
    return [
        "recipe_id",
        "display_name",
        "recipe_category",
        "current_directions_step_count",
        "has_directions",
        "appears_in_demo_plan",
        "priority",
        "suggested_action",
    ]


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = _clean_text(row.get(key)) or "missing"
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _join(value: Any) -> str:
    if isinstance(value, list):
        return ";".join(str(item) for item in value if str(item).strip())
    return _clean_text(value)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return _clean_text(value).lower() in {"", "nan", "none", "null"}


def _to_int(value: Any) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return 0
    return max(parsed, 0)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _clean_text(value).lower() in {"1", "true", "yes", "y"}


if __name__ == "__main__":
    main()
