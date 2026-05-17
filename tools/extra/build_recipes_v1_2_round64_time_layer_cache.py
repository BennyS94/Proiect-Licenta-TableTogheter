from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.recipe_time_layer import normalize_recipe_time  # noqa: E402


SOURCE_DIR = ROOT / "data/recipesdb/draft/v1_2_demo_final"
RECIPES_PATH = SOURCE_DIR / "recipes.csv"
INGREDIENTS_PATH = SOURCE_DIR / "recipe_ingredients.csv"
NUTRITION_PATH = SOURCE_DIR / "recipe_nutrition_cache.csv"
CACHE_OUT = ROOT / "data/recipesdb/draft/recipes_v1_2_round64_time_layer_cache.csv"
AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "recipes_v1_2_round64_time_layer_summary.txt"
REVIEW_OUT = AUDIT_DIR / "recipes_v1_2_round64_time_layer_review.csv"
MATERIALIZED_DIR = ROOT / "data/recipesdb/draft/v1_2_demo_final_time_layer"
MATERIALIZED_README = MATERIALIZED_DIR / "README_v1_2_demo_final_time_layer.txt"

TIME_CACHE_COLUMNS = [
    "recipe_id",
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "total_elapsed_time_min",
    "effective_time_min_for_scoring",
    "has_long_passive_time",
    "time_confidence",
    "time_estimation_method",
    "time_warnings",
    "time_reasons",
]


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_OUT.parent.mkdir(parents=True, exist_ok=True)
    MATERIALIZED_DIR.mkdir(parents=True, exist_ok=True)

    recipes = pd.read_csv(RECIPES_PATH)
    ingredients = pd.read_csv(INGREDIENTS_PATH)
    nutrition = pd.read_csv(NUTRITION_PATH)
    cache = pd.DataFrame([_cache_row(row) for _, row in recipes.iterrows()])
    review = cache.loc[_review_mask(cache)].copy()
    materialized_recipes = _merge_time_layer(recipes, cache)

    cache.to_csv(CACHE_OUT, index=False)
    review.to_csv(REVIEW_OUT, index=False)
    materialized_recipes.to_csv(MATERIALIZED_DIR / "recipes.csv", index=False)
    ingredients.to_csv(MATERIALIZED_DIR / "recipe_ingredients.csv", index=False)
    nutrition.to_csv(MATERIALIZED_DIR / "recipe_nutrition_cache.csv", index=False)
    MATERIALIZED_README.write_text(_materialized_readme(), encoding="utf-8")
    SUMMARY_OUT.write_text(
        _summary_text(cache=cache, review=review, recipes=recipes),
        encoding="utf-8",
    )

    print("Round64 time layer cache written")
    print(f"cache={CACHE_OUT}")
    print(f"summary={SUMMARY_OUT}")
    print(f"review={REVIEW_OUT}")
    print(f"dataset={MATERIALIZED_DIR}")
    print(
        "rows="
        f"{len(cache)}; review={len(review)}; "
        f"long_passive={int(cache['has_long_passive_time'].astype(bool).sum())}"
    )


def _cache_row(row: pd.Series) -> dict[str, Any]:
    normalized = normalize_recipe_time(row)
    return {
        "recipe_id": row.get("recipe_id"),
        "active_time_estimated_min": normalized["active_time_estimated_min"],
        "passive_time_estimated_min": normalized["passive_time_estimated_min"],
        "total_elapsed_time_min": normalized["total_elapsed_time_min"],
        "effective_time_min_for_scoring": normalized["effective_time_min_for_scoring"],
        "has_long_passive_time": normalized["has_long_passive_time"],
        "time_confidence": normalized["time_confidence"],
        "time_estimation_method": normalized["time_estimation_method"],
        "time_warnings": ";".join(normalized["time_warnings"]),
        "time_reasons": ";".join(normalized["time_reasons"]),
    }


def _merge_time_layer(recipes: pd.DataFrame, cache: pd.DataFrame) -> pd.DataFrame:
    merged = recipes.copy()
    cache_by_recipe = cache.set_index("recipe_id")
    for column in TIME_CACHE_COLUMNS:
        if column == "recipe_id":
            continue
        if column in cache_by_recipe.columns:
            merged[column] = merged["recipe_id"].map(cache_by_recipe[column])
    merged["time_estimation_confidence"] = merged["time_confidence"]
    merged["time_estimation_reasons"] = merged["time_reasons"]
    return merged


def _review_mask(cache: pd.DataFrame) -> pd.Series:
    confidence = cache["time_confidence"].fillna("").astype(str)
    warnings = cache["time_warnings"].fillna("").astype(str)
    return (
        confidence.isin(["low", "unknown", ""])
        | warnings.str.contains("missing_|total_less_than|very_long|fallback_|capped|no_active", regex=True)
    )


def _summary_text(
    *,
    cache: pd.DataFrame,
    review: pd.DataFrame,
    recipes: pd.DataFrame,
) -> str:
    confidence_counts = Counter(cache["time_confidence"])
    method_counts = Counter(cache["time_estimation_method"])
    warning_counts = Counter()
    for value in cache["time_warnings"].fillna(""):
        warning_counts.update(_split_codes(value))
    active_changed = _changed_count(recipes, cache, "active_time_estimated_min")
    passive_changed = _changed_count(recipes, cache, "passive_time_estimated_min")
    effective_changed = _changed_count(recipes, cache, "effective_time_min_for_scoring")
    lines = [
        "Recipes_DB v1.2 Round64 time layer cache summary",
        "",
        f"source_dataset={SOURCE_DIR}",
        f"cache_rows={len(cache)}",
        f"materialized_dataset={MATERIALIZED_DIR}",
        f"review_rows={len(review)}",
        f"long_passive_count={int(cache['has_long_passive_time'].astype(bool).sum())}",
        f"active_time_changed_count={active_changed}",
        f"passive_time_changed_count={passive_changed}",
        f"effective_time_changed_count={effective_changed}",
        "",
        f"time_confidence_counts={_format_counts(confidence_counts)}",
        f"time_estimation_method_counts={_format_counts(method_counts)}",
        f"time_warning_counts={_format_counts(warning_counts)}",
        "",
        "Review sample:",
    ]
    for _, row in review.head(20).iterrows():
        lines.append(
            f"- {row['recipe_id']} | confidence={row['time_confidence']} | "
            f"effective={row['effective_time_min_for_scoring']} | warnings={row['time_warnings']}"
        )
    lines.extend(
        [
            "",
            "Scope:",
            "- separate draft/cache only",
            "- v1_2_demo_final is not overwritten",
            "- current data folders are untouched",
            "- deterministic heuristic only, no web/AI calls",
        ]
    )
    return "\n".join(lines) + "\n"


def _changed_count(recipes: pd.DataFrame, cache: pd.DataFrame, column: str) -> int:
    if column not in recipes.columns:
        return len(cache)
    before = pd.to_numeric(recipes[column], errors="coerce")
    after = pd.to_numeric(cache[column], errors="coerce")
    delta = (before.fillna(-999999.0) - after.fillna(-999999.0)).abs()
    return int((delta > 0.05).sum())


def _materialized_readme() -> str:
    return "\n".join(
        [
            "# Recipes_DB v1.2 demo-final time-layer draft",
            "",
            "This draft copies `data/recipesdb/draft/v1_2_demo_final/` and adds Round64 normalized time fields.",
            "",
            "Added/normalized fields:",
            "- active_time_estimated_min",
            "- passive_time_estimated_min",
            "- total_elapsed_time_min",
            "- effective_time_min_for_scoring",
            "- has_long_passive_time",
            "- time_confidence",
            "- time_estimation_method",
            "- time_warnings",
            "",
            "Scope:",
            "- deterministic time audit/normalization only",
            "- no nutrition formula changes",
            "- no recipe additions",
            "- no external AI/API or web scraping",
            "- demo/draft only, not current production data",
            "",
        ]
    )


def _split_codes(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(";") if part.strip()]


def _format_counts(counter: Counter | dict[str, int]) -> str:
    if not counter:
        return "none"
    return "; ".join(f"{key}={value}" for key, value in sorted(counter.items()))


if __name__ == "__main__":
    main()
