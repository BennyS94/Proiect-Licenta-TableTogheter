from __future__ import annotations

import math
from collections.abc import Mapping

from src.generator_v1.macro_fit import clamp


MIN_KCAL_TARGET_RATIO = 0.35
MIN_PROTEIN_TARGET_RATIO = 0.20
NEAR_ZERO_PROTEIN_G = 1.0
LOW_KCAL_PENALTY = 0.55
LOW_PROTEIN_PENALTY = 0.55
NEAR_ZERO_PROTEIN_PENALTY = 0.30


def compute_nutrition_quality(
    candidate_row: Mapping[str, object],
    slot_target: Mapping[str, object],
) -> dict[str, object]:
    kcal = _to_float(candidate_row.get("kcal"))
    protein_g = _to_float(candidate_row.get("protein_g"))
    target_kcal = _to_float(slot_target.get("kcal"))
    target_protein_g = _to_float(slot_target.get("protein_g"))
    quality = 1.0
    reasons: list[str] = []
    suspicious = False

    if kcal is None or kcal <= 0:
        return {
            "nutrition_quality": 0.0,
            "nutrition_quality_reasons": ["kcal_lipsa_sau_zero"],
            "is_nutrition_suspicious": True,
        }

    if protein_g is None:
        suspicious = True
        reasons.append("protein_lipsa")
        quality -= LOW_PROTEIN_PENALTY

    if target_kcal is not None and target_kcal > 0:
        min_kcal = target_kcal * MIN_KCAL_TARGET_RATIO
        if kcal < min_kcal:
            quality -= LOW_KCAL_PENALTY
            suspicious = True
            reasons.append("kcal_sub_35pct_tinta_slot")

    if protein_g is not None and protein_g < NEAR_ZERO_PROTEIN_G:
        quality -= NEAR_ZERO_PROTEIN_PENALTY
        suspicious = True
        reasons.append("protein_aproape_zero")

    if protein_g is not None and target_protein_g is not None and target_protein_g > 0:
        min_protein = target_protein_g * MIN_PROTEIN_TARGET_RATIO
        if protein_g < min_protein:
            quality -= LOW_PROTEIN_PENALTY
            suspicious = True
            reasons.append("protein_sub_20pct_tinta_slot")

    return {
        "nutrition_quality": round(clamp(quality), 4),
        "nutrition_quality_reasons": reasons or ["ok"],
        "is_nutrition_suspicious": suspicious,
    }


def _to_float(value: object) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value):
        return None
    return numeric_value
