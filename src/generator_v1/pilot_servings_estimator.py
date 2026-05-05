from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

import pandas as pd


DEFAULT_MAIN_DISH_SERVINGS = 4.0
SIDE_SALAD_SNACK_SERVINGS = 2.0
LARGE_MEAT_SERVINGS = 6.0
VERY_LARGE_MEAT_SERVINGS = 8.0
LARGE_MEAT_GRAMS_THRESHOLD = 900.0
VERY_LARGE_MEAT_GRAMS_THRESHOLD = 1800.0
ELIGIBLE_CACHE_STATUS = "partial_from_mapped_ingredients"

SIDE_SALAD_SNACK_KEYWORDS = (
    "side",
    "side_only",
    "salad",
    "snack",
    "appetizer",
    "starter",
)

MEAT_SIGNAL_KEYWORDS = (
    "beef",
    "chicken",
    "pork",
    "turkey",
    "lamb",
    "steak",
    "rib",
    "ribs",
    "short_ribs",
    "flank",
    "chuck",
    "tenderloin",
    "loin",
    "meat",
    "fish",
    "salmon",
    "tuna",
    "shrimp",
    "halibut",
    "catfish",
)


def estimate_pilot_servings(
    recipe_row: pd.Series | dict[str, Any],
    ingredients_df: pd.DataFrame | None = None,
    nutrition_row: pd.Series | dict[str, Any] | None = None,
) -> dict[str, Any]:
    servings_normalized = _positive_float(_get(recipe_row, "servings_normalized"))
    if servings_normalized is not None:
        return {
            "estimated_servings_basis": servings_normalized,
            "uses_pilot_servings_fallback": False,
            "servings_estimation_reasons": ["servings_normalized_available"],
        }

    servings_declared = _parse_servings_declared(_get(recipe_row, "servings_declared"))
    if servings_declared is not None:
        return {
            "estimated_servings_basis": servings_declared,
            "uses_pilot_servings_fallback": False,
            "servings_estimation_reasons": ["servings_declared_parseable"],
        }

    recipe_id = str(_get(recipe_row, "recipe_id") or "").strip()
    raw_meat_grams = _raw_meat_grams(recipe_id, ingredients_df)
    reasons = ["pilot_servings_fallback_no_declared_servings"]

    # Fallback temporar pentru pilot; nu reprezinta logica finala de portii.
    if raw_meat_grams >= VERY_LARGE_MEAT_GRAMS_THRESHOLD:
        reasons.extend(
            [
                "pilot_large_meat_signal_ge_1800g",
                f"raw_meat_grams_estimated={raw_meat_grams:.1f}",
            ]
        )
        return _fallback_result(VERY_LARGE_MEAT_SERVINGS, reasons)

    if raw_meat_grams >= LARGE_MEAT_GRAMS_THRESHOLD:
        reasons.extend(
            [
                "pilot_large_meat_signal_ge_900g",
                f"raw_meat_grams_estimated={raw_meat_grams:.1f}",
            ]
        )
        return _fallback_result(LARGE_MEAT_SERVINGS, reasons)

    if _looks_like_side_salad_or_snack(recipe_row):
        reasons.append("pilot_side_salad_snack_signal")
        return _fallback_result(SIDE_SALAD_SNACK_SERVINGS, reasons)

    if raw_meat_grams > 0:
        reasons.append(f"raw_meat_grams_estimated={raw_meat_grams:.1f}")
    reasons.append("pilot_default_main_dish_4")
    return _fallback_result(DEFAULT_MAIN_DISH_SERVINGS, reasons)


def build_pilot_servings_diagnostics(
    recipes_df: pd.DataFrame,
    ingredients_df: pd.DataFrame,
    nutrition_df: pd.DataFrame,
    eligible_candidates: pd.DataFrame,
    selected_recipe_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    eligible_rows = _eligible_rows(recipes_df, nutrition_df, eligible_candidates)
    selected_rows = _selected_rows(recipes_df, nutrition_df, selected_recipe_ids)

    eligible_estimates = [
        estimate_pilot_servings(row, ingredients_df)
        for _, row in eligible_rows.sort_values("recipe_id", kind="mergesort").iterrows()
    ]

    return {
        "eligible_recipe_count": int(len(eligible_rows)),
        "estimated_servings_basis_distribution": _servings_distribution(
            eligible_estimates
        ),
        "uses_pilot_servings_fallback_count": int(
            sum(
                bool(estimate.get("uses_pilot_servings_fallback"))
                for estimate in eligible_estimates
            )
        ),
        "selected_plan_servings": _selected_plan_servings(
            selected_rows,
            ingredients_df,
        ),
    }


def _eligible_rows(
    recipes_df: pd.DataFrame,
    nutrition_df: pd.DataFrame,
    eligible_candidates: pd.DataFrame,
) -> pd.DataFrame:
    if not eligible_candidates.empty and "cache_status" in eligible_candidates.columns:
        return eligible_candidates.copy()

    joined = recipes_df.merge(
        nutrition_df,
        on="recipe_id",
        how="left",
        suffixes=("", "_nutrition"),
    )
    return joined.loc[
        joined["scope_status"].eq("pilot_validated")
        & joined["is_active"].eq(1)
        & joined["has_ingredients_parsed"].eq(1)
        & joined["cache_status"].eq(ELIGIBLE_CACHE_STATUS)
    ].copy()


def _selected_rows(
    recipes_df: pd.DataFrame,
    nutrition_df: pd.DataFrame,
    selected_recipe_ids: Iterable[str] | None,
) -> pd.DataFrame:
    recipe_ids = [str(recipe_id) for recipe_id in selected_recipe_ids or [] if recipe_id]
    if not recipe_ids:
        return pd.DataFrame()

    order = {recipe_id: index for index, recipe_id in enumerate(recipe_ids)}
    joined = recipes_df.merge(
        nutrition_df,
        on="recipe_id",
        how="left",
        suffixes=("", "_nutrition"),
    )
    selected = joined.loc[joined["recipe_id"].astype(str).isin(order)].copy()
    selected["selected_order"] = selected["recipe_id"].astype(str).map(order)
    return selected.sort_values("selected_order", kind="mergesort")


def _selected_plan_servings(
    selected_rows: pd.DataFrame,
    ingredients_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, row in selected_rows.iterrows():
        estimate = estimate_pilot_servings(row, ingredients_df)
        rows.append(
            {
                "recipe_id": _clean_value(row.get("recipe_id")),
                "display_name": _clean_value(row.get("display_name")),
                "cache_servings_basis": _clean_value(row.get("servings_basis")),
                "estimated_servings_basis": _clean_value(
                    estimate.get("estimated_servings_basis")
                ),
                "uses_pilot_servings_fallback": bool(
                    estimate.get("uses_pilot_servings_fallback")
                ),
                "servings_estimation_reasons": estimate.get(
                    "servings_estimation_reasons",
                    [],
                ),
            }
        )
    return rows


def _servings_distribution(estimates: list[dict[str, Any]]) -> dict[str, int]:
    values = [
        estimate.get("estimated_servings_basis")
        for estimate in estimates
        if estimate.get("estimated_servings_basis") is not None
    ]
    if not values:
        return {}
    counts = pd.Series(values).value_counts().sort_index()
    return {_format_number(index): int(value) for index, value in counts.items()}


def _fallback_result(servings: float, reasons: list[str]) -> dict[str, Any]:
    return {
        "estimated_servings_basis": servings,
        "uses_pilot_servings_fallback": True,
        "servings_estimation_reasons": reasons,
    }


def _raw_meat_grams(recipe_id: str, ingredients_df: pd.DataFrame | None) -> float:
    if not recipe_id or ingredients_df is None or ingredients_df.empty:
        return 0.0
    if "recipe_id" not in ingredients_df.columns:
        return 0.0

    recipe_ingredients = ingredients_df.loc[
        ingredients_df["recipe_id"].astype(str).eq(recipe_id)
    ]
    if recipe_ingredients.empty:
        return 0.0

    grams = pd.to_numeric(
        recipe_ingredients.get("quantity_grams_estimated"),
        errors="coerce",
    )
    signal = _ingredient_signal_text(recipe_ingredients).apply(_has_meat_signal)
    valid_grams = grams.where(grams.gt(0), 0.0).fillna(0.0)
    return round(float(valid_grams.loc[signal].sum()), 4)


def _ingredient_signal_text(ingredients_df: pd.DataFrame) -> pd.Series:
    parts = []
    for column in (
        "ingredient_name_normalized",
        "ingredient_name_parsed",
        "ingredient_raw_text",
    ):
        if column in ingredients_df.columns:
            parts.append(ingredients_df[column].fillna("").astype(str))
    if not parts:
        return pd.Series("", index=ingredients_df.index)
    text = parts[0]
    for part in parts[1:]:
        text = text + " " + part
    return text.str.lower()


def _has_meat_signal(value: str) -> bool:
    return any(keyword in value for keyword in MEAT_SIGNAL_KEYWORDS)


def _looks_like_side_salad_or_snack(recipe_row: pd.Series | dict[str, Any]) -> bool:
    text_parts = [
        _get(recipe_row, "recipe_kind"),
        _get(recipe_row, "recipe_category"),
        _get(recipe_row, "recipe_subcategory"),
        _get(recipe_row, "recipe_name"),
        _get(recipe_row, "display_name"),
        _get(recipe_row, "recipe_family_name"),
    ]
    text = " ".join(str(part or "").lower() for part in text_parts)
    return any(keyword in text for keyword in SIDE_SALAD_SNACK_KEYWORDS)


def _parse_servings_declared(value: object) -> float | None:
    numeric = _positive_float(value)
    if numeric is not None:
        return numeric
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"(\d+(?:[.,]\d+)?)", text)
    if not match:
        return None
    return _positive_float(match.group(1).replace(",", "."))


def _positive_float(value: object) -> float | None:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric) or float(numeric) <= 0:
        return None
    return float(numeric)


def _get(row: pd.Series | dict[str, Any] | None, key: str) -> object:
    if row is None:
        return None
    if isinstance(row, pd.Series):
        return row.get(key)
    return row.get(key)


def _clean_value(value: object) -> object:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def _format_number(value: object) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return "missing"
    return f"{float(numeric):g}"
