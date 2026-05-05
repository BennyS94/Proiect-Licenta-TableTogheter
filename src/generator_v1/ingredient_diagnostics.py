from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd


ELIGIBLE_CACHE_STATUS = "partial_from_mapped_ingredients"
LOW_MAPPED_WEIGHT_RATIO = 0.20
LOW_KCAL_PER_SERVING = 150.0
LOW_PROTEIN_PER_SERVING_G = 10.0

TOP_RECIPE_COLUMNS = [
    "recipe_id",
    "display_name",
    "ingredient_count",
    "accepted_auto_count",
    "accepted_auto_with_grams_count",
    "review_needed_count",
    "unmapped_count",
    "ingredients_with_grams_count",
    "mapped_with_grams_count",
    "mapped_weight_sum_grams",
    "mapped_weight_ratio",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
]

SELECTED_INGREDIENT_COLUMNS = [
    "recipe_id",
    "display_name",
    "ingredient_raw_text",
    "ingredient_name_normalized",
    "quantity_value",
    "quantity_unit",
    "quantity_grams_estimated",
    "mapping_status",
    "mapped_food_id",
    "mapped_food_canonical_name",
    "mapping_confidence",
    "ingredient_role",
]


def build_ingredient_diagnostics(
    recipes_df: pd.DataFrame,
    ingredients_df: pd.DataFrame,
    nutrition_df: pd.DataFrame,
    top_n: int = 15,
    selected_recipe_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    eligible = _eligible_candidates(recipes_df, nutrition_df)
    eligible_ids = set(eligible["recipe_id"].dropna().astype(str))
    eligible_ingredients = ingredients_df.loc[
        ingredients_df["recipe_id"].astype(str).isin(eligible_ids)
    ].copy()
    recipe_summary = _eligible_recipe_summary(eligible, eligible_ingredients)

    return {
        "global_mapping_summary": _global_mapping_summary(ingredients_df),
        "eligible_recipe_count": int(len(eligible)),
        "eligible_recipe_level_summary": _clean_records(
            recipe_summary.to_dict("records")
        ),
        "top_suspicious_recipes": _top_suspicious_recipes(recipe_summary, top_n),
        "common_unmapped_ingredients": _common_ingredients(
            eligible_ingredients,
            mapping_status="unmapped",
            top_n=30,
        ),
        "common_review_needed_ingredients": _common_ingredients(
            eligible_ingredients,
            mapping_status="review_needed",
            top_n=30,
        ),
        "selected_plan_ingredient_breakdown": _selected_ingredient_breakdown(
            selected_recipe_ids=selected_recipe_ids,
            recipes_df=recipes_df,
            ingredients_df=ingredients_df,
        ),
    }


def _eligible_candidates(
    recipes_df: pd.DataFrame,
    nutrition_df: pd.DataFrame,
) -> pd.DataFrame:
    candidates = recipes_df.merge(
        nutrition_df,
        on="recipe_id",
        how="left",
        suffixes=("", "_nutrition"),
    )
    return candidates.loc[
        candidates["scope_status"].eq("pilot_validated")
        & candidates["is_active"].eq(1)
        & candidates["has_ingredients_parsed"].eq(1)
        & candidates["cache_status"].eq(ELIGIBLE_CACHE_STATUS)
    ].copy()


def _global_mapping_summary(ingredients_df: pd.DataFrame) -> dict[str, object]:
    mapping_status = _text_series(ingredients_df.get("mapping_status"))
    has_mapped_food = _present_series(ingredients_df.get("mapped_food_id"))
    has_grams = _grams_series(ingredients_df).gt(0)

    return {
        "total_ingredient_rows": int(len(ingredients_df)),
        "mapping_status_counts": _value_counts(mapping_status),
        "mapped_food_id_present_count": int(has_mapped_food.sum()),
        "quantity_grams_estimated_gt_0_count": int(has_grams.sum()),
        "mapped_food_id_and_grams_gt_0_count": int((has_mapped_food & has_grams).sum()),
        "accepted_auto_with_grams_gt_0_count": int(
            (mapping_status.eq("accepted_auto") & has_grams).sum()
        ),
        "accepted_auto_without_grams_count": int(
            (mapping_status.eq("accepted_auto") & ~has_grams).sum()
        ),
        "review_needed_with_grams_gt_0_count": int(
            (mapping_status.eq("review_needed") & has_grams).sum()
        ),
        "unmapped_with_grams_gt_0_count": int(
            (mapping_status.eq("unmapped") & has_grams).sum()
        ),
    }


def _eligible_recipe_summary(
    eligible: pd.DataFrame,
    eligible_ingredients: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, recipe in eligible.sort_values("recipe_id", kind="mergesort").iterrows():
        recipe_id = str(recipe.get("recipe_id"))
        recipe_ingredients = eligible_ingredients.loc[
            eligible_ingredients["recipe_id"].astype(str).eq(recipe_id)
        ]
        status = _text_series(recipe_ingredients.get("mapping_status"))
        grams = _grams_series(recipe_ingredients)
        has_grams = grams.gt(0)
        has_mapped_food = _present_series(recipe_ingredients.get("mapped_food_id"))

        rows.append(
            {
                "recipe_id": recipe_id,
                "display_name": recipe.get("display_name"),
                "ingredient_count": int(len(recipe_ingredients)),
                "accepted_auto_count": int(status.eq("accepted_auto").sum()),
                "accepted_auto_with_grams_count": int(
                    (status.eq("accepted_auto") & has_grams).sum()
                ),
                "review_needed_count": int(status.eq("review_needed").sum()),
                "unmapped_count": int(status.eq("unmapped").sum()),
                "ingredients_with_grams_count": int(has_grams.sum()),
                "mapped_with_grams_count": int((has_mapped_food & has_grams).sum()),
                "mapped_weight_sum_grams": _sum_rounded(
                    grams.loc[has_mapped_food & has_grams]
                ),
                "all_ingredient_known_weight_sum_grams": _sum_rounded(
                    grams.loc[has_grams]
                ),
                "mapped_weight_ratio": _clean_value(recipe.get("mapped_weight_ratio")),
                "energy_kcal_per_serving": _clean_value(
                    recipe.get("energy_kcal_per_serving")
                ),
                "protein_g_per_serving": _clean_value(recipe.get("protein_g_per_serving")),
            }
        )
    return pd.DataFrame(rows)


def _top_suspicious_recipes(
    recipe_summary: pd.DataFrame,
    top_n: int,
) -> list[dict[str, object]]:
    if recipe_summary.empty:
        return []

    suspicious = recipe_summary.loc[
        _numeric_series(recipe_summary.get("mapped_weight_ratio")).lt(
            LOW_MAPPED_WEIGHT_RATIO
        )
        | _numeric_series(recipe_summary.get("energy_kcal_per_serving")).lt(
            LOW_KCAL_PER_SERVING
        )
        | _numeric_series(recipe_summary.get("protein_g_per_serving")).lt(
            LOW_PROTEIN_PER_SERVING_G
        )
    ].copy()
    if suspicious.empty:
        return []

    sort_columns = [
        "mapped_weight_sum_grams",
        "mapped_weight_ratio",
        "energy_kcal_per_serving",
        "protein_g_per_serving",
        "recipe_id",
    ]
    suspicious = suspicious.sort_values(
        sort_columns,
        ascending=[True, True, True, True, True],
        kind="mergesort",
        na_position="last",
    )
    return _clean_records(
        suspicious.loc[:, TOP_RECIPE_COLUMNS].head(top_n).to_dict("records")
    )


def _common_ingredients(
    eligible_ingredients: pd.DataFrame,
    mapping_status: str,
    top_n: int,
) -> list[dict[str, object]]:
    if eligible_ingredients.empty:
        return []
    status = _text_series(eligible_ingredients.get("mapping_status"))
    subset = eligible_ingredients.loc[status.eq(mapping_status)].copy()
    if subset.empty:
        return []

    subset["diagnostic_ingredient_name"] = _ingredient_name_series(subset)
    subset["has_grams"] = _grams_series(subset).gt(0)
    grouped = (
        subset.groupby("diagnostic_ingredient_name", dropna=False)
        .agg(
            count=("diagnostic_ingredient_name", "size"),
            with_grams_count=("has_grams", "sum"),
        )
        .reset_index()
        .sort_values(
            ["count", "with_grams_count", "diagnostic_ingredient_name"],
            ascending=[False, False, True],
            kind="mergesort",
        )
        .head(top_n)
    )
    return _clean_records(grouped.to_dict("records"))


def _selected_ingredient_breakdown(
    selected_recipe_ids: Iterable[str] | None,
    recipes_df: pd.DataFrame,
    ingredients_df: pd.DataFrame,
) -> list[dict[str, object]]:
    recipe_ids = [str(recipe_id) for recipe_id in selected_recipe_ids or [] if recipe_id]
    if not recipe_ids:
        return []

    order = {recipe_id: index for index, recipe_id in enumerate(recipe_ids)}
    selected = ingredients_df.loc[
        ingredients_df["recipe_id"].astype(str).isin(order)
    ].copy()
    if selected.empty:
        return []

    names = recipes_df.loc[:, ["recipe_id", "display_name"]].copy()
    selected = selected.merge(names, on="recipe_id", how="left")
    selected["selected_order"] = selected["recipe_id"].astype(str).map(order)
    selected = selected.sort_values(
        ["selected_order", "ingredient_position"],
        kind="mergesort",
        na_position="last",
    )
    existing_columns = [
        column for column in SELECTED_INGREDIENT_COLUMNS if column in selected.columns
    ]
    return _clean_records(selected.loc[:, existing_columns].to_dict("records"))


def _ingredient_name_series(df: pd.DataFrame) -> pd.Series:
    normalized = df.get("ingredient_name_normalized", pd.Series(dtype="object"))
    parsed = df.get("ingredient_name_parsed", pd.Series(dtype="object"))
    raw = df.get("ingredient_raw_text", pd.Series(dtype="object"))
    names = normalized.fillna("").astype(str).str.strip()
    parsed_names = parsed.fillna("").astype(str).str.strip()
    raw_names = raw.fillna("").astype(str).str.strip()
    names = names.where(names.ne(""), parsed_names)
    names = names.where(names.ne(""), raw_names)
    return names.where(names.ne(""), "unknown")


def _text_series(series: object) -> pd.Series:
    if isinstance(series, pd.Series):
        return series.fillna("").astype(str).str.strip().str.lower()
    return pd.Series(dtype="object")


def _present_series(series: object) -> pd.Series:
    if isinstance(series, pd.Series):
        return series.notna() & series.astype(str).str.strip().ne("")
    return pd.Series(dtype="bool")


def _grams_series(df: pd.DataFrame) -> pd.Series:
    series = df.get("quantity_grams_estimated")
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce")
    return pd.Series(index=df.index, dtype="float64")


def _numeric_series(series: object) -> pd.Series:
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce")
    return pd.Series(dtype="float64")


def _value_counts(series: pd.Series, limit: int = 12) -> dict[str, int]:
    counts = series.where(series.ne(""), "missing").value_counts().head(limit)
    return {str(index): int(value) for index, value in counts.items()}


def _sum_rounded(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return round(float(series.sum()), 4)


def _clean_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    return [_clean_row(record) for record in records]


def _clean_row(row: dict[str, object]) -> dict[str, object]:
    return {str(key): _clean_value(value) for key, value in row.items()}


def _clean_value(value: object) -> object:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value
