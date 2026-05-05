from __future__ import annotations

from typing import Any

import pandas as pd


NEAR_ZERO_KCAL = 5.0
NEAR_ZERO_MACRO_G = 1.0
LOW_KCAL_PER_SERVING = 150.0
LOW_PROTEIN_PER_SERVING_G = 10.0
MAPPED_WEIGHT_THRESHOLDS = (0.20, 0.40, 0.60)

PER_SERVING_MACRO_COLUMNS = {
    "energy_kcal_per_serving": NEAR_ZERO_KCAL,
    "protein_g_per_serving": NEAR_ZERO_MACRO_G,
    "carbs_g_per_serving": NEAR_ZERO_MACRO_G,
    "fat_g_per_serving": NEAR_ZERO_MACRO_G,
}

SUSPICIOUS_ELIGIBLE_COLUMNS = [
    "recipe_id",
    "display_name",
    "cache_status",
    "servings_basis",
    "total_weight_grams_estimated",
    "mapped_weight_ratio",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "mapped_ingredient_count",
    "unmapped_ingredient_count",
]


def build_nutrition_cache_diagnostics(
    recipes: pd.DataFrame,
    nutrition: pd.DataFrame,
    candidates: pd.DataFrame,
    eligible_candidates: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "total_nutrition_rows": int(len(nutrition)),
        "cache_status_counts": _value_counts(nutrition.get("cache_status")),
        "servings_basis": _basis_diagnostics(nutrition.get("servings_basis")),
        "total_weight_grams_estimated": _positive_value_diagnostics(
            nutrition.get("total_weight_grams_estimated")
        ),
        "per_serving_macros": {
            column: _macro_diagnostics(nutrition.get(column), near_zero_threshold)
            for column, near_zero_threshold in PER_SERVING_MACRO_COLUMNS.items()
        },
        "mapped_weight_ratio": _mapped_weight_ratio_diagnostics(
            nutrition.get("mapped_weight_ratio")
        ),
        "eligible_candidates": _eligible_candidate_diagnostics(eligible_candidates),
        "top_suspicious_eligible_recipes": _top_suspicious_eligible_recipes(
            eligible_candidates
        ),
        "joined_candidate_rows": int(len(candidates)),
        "recipe_rows": int(len(recipes)),
    }


def _value_counts(series: object, limit: int = 12) -> dict[str, int]:
    if not isinstance(series, pd.Series):
        return {}
    counts = series.fillna("missing").astype(str).value_counts().head(limit)
    return {str(index): int(value) for index, value in counts.items()}


def _basis_diagnostics(series: object) -> dict[str, object]:
    numeric = _numeric_series(series)
    valid = numeric[numeric.gt(0)]
    return {
        "missing_count": int(numeric.isna().sum()),
        "zero_or_invalid_count": int(numeric.notna().sum() - valid.count()),
        "value_counts": _numeric_value_counts(valid),
    }


def _positive_value_diagnostics(series: object) -> dict[str, object]:
    numeric = _numeric_series(series)
    valid = numeric[numeric.gt(0)]
    return {
        "missing_count": int(numeric.isna().sum()),
        "zero_or_invalid_count": int(numeric.notna().sum() - valid.count()),
        **_summary_stats(valid),
    }


def _macro_diagnostics(
    series: object,
    near_zero_threshold: float,
) -> dict[str, object]:
    numeric = _numeric_series(series)
    valid = numeric[numeric.notna()]
    return {
        "missing_count": int(numeric.isna().sum()),
        "zero_or_near_zero_count": int(valid.le(near_zero_threshold).sum()),
        "near_zero_threshold": near_zero_threshold,
        **_summary_stats(valid),
    }


def _mapped_weight_ratio_diagnostics(series: object) -> dict[str, object]:
    numeric = _numeric_series(series)
    valid = numeric[numeric.notna()]
    result: dict[str, object] = {
        "missing_count": int(numeric.isna().sum()),
        **_summary_stats(valid),
    }
    for threshold in MAPPED_WEIGHT_THRESHOLDS:
        key = f"count_below_{threshold:.2f}"
        result[key] = int(valid.lt(threshold).sum())
    return result


def _eligible_candidate_diagnostics(eligible_candidates: pd.DataFrame) -> dict[str, int]:
    kcal = _numeric_series(eligible_candidates.get("energy_kcal_per_serving"))
    protein = _numeric_series(eligible_candidates.get("protein_g_per_serving"))
    kcal_ok = kcal.ge(LOW_KCAL_PER_SERVING)
    protein_ok = protein.ge(LOW_PROTEIN_PER_SERVING_G)
    return {
        "eligible_rows_count": int(len(eligible_candidates)),
        "kcal_per_serving_lt_150_count": int(kcal.lt(LOW_KCAL_PER_SERVING).sum()),
        "protein_per_serving_lt_10_count": int(
            protein.lt(LOW_PROTEIN_PER_SERVING_G).sum()
        ),
        "kcal_ge_150_and_protein_ge_10_count": int((kcal_ok & protein_ok).sum()),
    }


def _top_suspicious_eligible_recipes(
    eligible_candidates: pd.DataFrame,
    limit: int = 15,
) -> list[dict[str, object]]:
    if eligible_candidates.empty:
        return []

    candidates = eligible_candidates.copy()
    kcal = _numeric_series(candidates.get("energy_kcal_per_serving"))
    protein = _numeric_series(candidates.get("protein_g_per_serving"))
    mapped_ratio = _numeric_series(candidates.get("mapped_weight_ratio"))
    suspicious_mask = (
        kcal.lt(LOW_KCAL_PER_SERVING)
        | protein.lt(LOW_PROTEIN_PER_SERVING_G)
        | mapped_ratio.lt(MAPPED_WEIGHT_THRESHOLDS[0])
    )
    suspicious = candidates.loc[suspicious_mask].copy()
    if suspicious.empty:
        return []

    sort_columns = [
        "mapped_weight_ratio",
        "energy_kcal_per_serving",
        "protein_g_per_serving",
        "recipe_id",
    ]
    for column in sort_columns:
        if column not in suspicious.columns:
            suspicious[column] = pd.NA
    suspicious = suspicious.sort_values(
        sort_columns,
        ascending=[True, True, True, True],
        kind="mergesort",
        na_position="last",
    )
    existing_columns = [
        column for column in SUSPICIOUS_ELIGIBLE_COLUMNS if column in suspicious.columns
    ]
    return [
        _clean_row(row)
        for row in suspicious.loc[:, existing_columns].head(limit).to_dict("records")
    ]


def _numeric_series(series: object) -> pd.Series:
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce")
    return pd.Series(dtype="float64")


def _numeric_value_counts(series: pd.Series, limit: int = 10) -> dict[str, int]:
    counts = series.value_counts().sort_index().head(limit)
    return {_format_number(index): int(value) for index, value in counts.items()}


def _summary_stats(series: pd.Series) -> dict[str, float | None]:
    if series.empty or series.dropna().empty:
        return {"min": None, "median": None, "max": None}
    valid = series.dropna()
    return {
        "min": round(float(valid.min()), 4),
        "median": round(float(valid.median()), 4),
        "max": round(float(valid.max()), 4),
    }


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


def _format_number(value: object) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return "missing"
    return f"{float(numeric):g}"
