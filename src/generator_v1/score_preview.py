from __future__ import annotations

import math
from collections.abc import Mapping

from src.generator_v1.macro_fit import clamp


def compute_score_preview(candidate_row: Mapping[str, object]) -> dict[str, float]:
    macro_score = _fit_value(candidate_row.get("macro_fit"))
    time_score = _fit_value(candidate_row.get("time_fit"))
    slot_score = _fit_value(candidate_row.get("slot_fit"))
    nutrition_quality = _fit_value(candidate_row.get("nutrition_quality"), default=1.0)

    # TODO: conecteaza feedback real cand exista semnale explicite pe reteta.
    feedback_fit = 0.50
    # TODO: conecteaza varietate reala cand exista selectie pe zi / istoric.
    variety_fit = 0.50

    base_score_preview = (
        0.55 * macro_score
        + 0.15 * time_score
        + 0.15 * slot_score
        + 0.10 * feedback_fit
        + 0.05 * variety_fit
    )
    score_preview = base_score_preview * nutrition_quality

    return {
        "score_preview": round(clamp(score_preview), 4),
        "base_score_preview": round(clamp(base_score_preview), 4),
        "macro_fit": round(macro_score, 4),
        "time_fit": round(time_score, 4),
        "slot_fit": round(slot_score, 4),
        "nutrition_quality": round(nutrition_quality, 4),
        "feedback_fit": feedback_fit,
        "variety_fit": variety_fit,
    }


def _fit_value(value: object, default: float = 0.0) -> float:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return clamp(default)
    if math.isnan(numeric_value):
        return clamp(default)
    return clamp(numeric_value)
