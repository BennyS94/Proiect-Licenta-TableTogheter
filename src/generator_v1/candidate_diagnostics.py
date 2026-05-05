from __future__ import annotations

import pandas as pd

from src.generator_v1.nutrition_quality import (
    MIN_KCAL_TARGET_RATIO,
    MIN_PROTEIN_TARGET_RATIO,
)


def build_candidate_diagnostics(
    slot_candidates: pd.DataFrame,
    slot_targets: dict[str, dict[str, float]],
) -> dict[str, dict[str, object]]:
    diagnostics: dict[str, dict[str, object]] = {}
    for slot, slot_target in slot_targets.items():
        slot_df = slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        diagnostics[slot] = _slot_diagnostics(slot_df, slot_target)
    return diagnostics


def _slot_diagnostics(
    slot_df: pd.DataFrame,
    slot_target: dict[str, float],
) -> dict[str, object]:
    kcal = pd.to_numeric(slot_df.get("kcal"), errors="coerce")
    protein = pd.to_numeric(slot_df.get("protein_g"), errors="coerce")
    time_column = (
        "effective_time_min_for_scoring"
        if "effective_time_min_for_scoring" in slot_df.columns
        else "total_time_min"
    )
    time_min = pd.to_numeric(slot_df.get(time_column), errors="coerce")
    target_kcal = float(slot_target.get("kcal") or 0)
    target_protein = float(slot_target.get("protein_g") or 0)
    kcal_pass = kcal >= target_kcal * MIN_KCAL_TARGET_RATIO if target_kcal > 0 else False
    protein_pass = (
        protein >= target_protein * MIN_PROTEIN_TARGET_RATIO
        if target_protein > 0
        else False
    )
    suspicious = slot_df.get("is_nutrition_suspicious", pd.Series(dtype=bool)).fillna(True)
    long_passive = slot_df.get("has_long_passive_time", pd.Series(dtype=bool)).fillna(False)

    return {
        "total_slot_candidates": int(len(slot_df)),
        "candidate_count": int(slot_df["recipe_id"].nunique()) if "recipe_id" in slot_df else 0,
        "suspicious_nutrition_count": int(suspicious.astype(bool).sum()),
        "non_suspicious_nutrition_count": int((~suspicious.astype(bool)).sum()),
        "kcal_35pct_pass_count": int(kcal_pass.sum()) if not isinstance(kcal_pass, bool) else 0,
        "protein_20pct_pass_count": int(protein_pass.sum()) if not isinstance(protein_pass, bool) else 0,
        "both_kcal_and_protein_pass_count": _both_pass_count(kcal_pass, protein_pass),
        "time_le_30_count": int((time_min <= 30).sum()),
        "time_le_60_count": int((time_min <= 60).sum()),
        "time_gt_60_count": int((time_min > 60).sum()),
        "time_gt_180_count": int((time_min > 180).sum()),
        "long_passive_time_count": int(long_passive.astype(bool).sum()),
        "time_diagnostic_basis": time_column,
        "best_macro_fit": _max_or_zero(slot_df.get("macro_fit")),
        "best_score_preview": _max_or_zero(slot_df.get("score_preview")),
        "median_kcal": _median_or_zero(kcal),
        "median_protein_g": _median_or_zero(protein),
    }


def _both_pass_count(kcal_pass: object, protein_pass: object) -> int:
    if isinstance(kcal_pass, bool) or isinstance(protein_pass, bool):
        return 0
    return int((kcal_pass & protein_pass).sum())


def _max_or_zero(series: object) -> float:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.empty or pd.isna(numeric.max()):
        return 0.0
    return round(float(numeric.max()), 4)


def _median_or_zero(series: pd.Series) -> float:
    if series.empty or pd.isna(series.median()):
        return 0.0
    return round(float(series.median()), 1)
