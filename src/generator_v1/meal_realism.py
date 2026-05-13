from __future__ import annotations

import math
from collections.abc import Mapping


BASE_PENALTIES = {
    "borderline_large_portion": 0.03,
    "unrealistic_large_portion": 0.10,
    "mostly_carb_meal": 0.04,
    "mostly_protein_meal": 0.04,
    "low_carb_main": 0.04,
    "low_protein_main": 0.04,
    "snack_too_large": 0.10,
    "breakfast_low_protein": 0.03,
}

PRACTICAL_PENALTIES = {
    "borderline_large_portion": 0.12,
    "unrealistic_large_portion": 0.30,
    "breakfast_too_large": 0.16,
    "main_too_large": 0.14,
    "mostly_carb_meal": 0.06,
    "mostly_protein_meal": 0.06,
    "low_carb_main": 0.06,
    "low_protein_main": 0.06,
    "snack_too_large": 0.18,
    "snack_too_meal_like": 0.12,
    "breakfast_low_protein": 0.08,
    "missing_portion_grams": 0.02,
}

PRACTICAL_HARD_REJECT_PENALTY = 0.60


def compute_meal_realism(
    candidate_row: Mapping[str, object],
    slot: str,
    policy: str = "base",
) -> dict[str, object]:
    policy_name = str(policy or "base").strip().lower()
    if policy_name == "practical":
        return _compute_practical_realism(candidate_row, slot)
    return _compute_base_realism(candidate_row, slot)


def _compute_base_realism(
    candidate_row: Mapping[str, object],
    slot: str,
) -> dict[str, object]:
    flags: list[str] = []
    reasons: list[str] = []
    slot_name = str(slot or "").strip().lower()
    kcal = _to_optional_float(candidate_row.get("kcal"))
    protein_g = _to_optional_float(candidate_row.get("protein_g"))
    carbs_g = _to_optional_float(candidate_row.get("carbs_g"))
    fat_g = _to_optional_float(candidate_row.get("fat_g"))
    grams = _to_optional_float(candidate_row.get("portion_grams_estimated"))

    if any(value is None for value in (kcal, protein_g, carbs_g, fat_g)):
        return _result(0.0, [], ["missing_macro_values_neutral"])

    if grams is None:
        reasons.append("missing_portion_grams_neutral")
    else:
        _add_base_portion_flags(flags, reasons, slot_name, grams)

    if kcal <= 0:
        return _result(0.0, flags, [*reasons, "missing_or_zero_kcal_neutral"])

    protein_share = (protein_g * 4.0) / kcal
    carbs_share = (carbs_g * 4.0) / kcal

    if slot_name in {"lunch", "dinner"}:
        if carbs_share > 0.70 and protein_g < 15:
            flags.append("mostly_carb_meal")
            reasons.append("carbs_over_70pct_kcal_and_protein_under_15g")
        if protein_share > 0.45 and carbs_g < 20:
            flags.append("mostly_protein_meal")
            reasons.append("protein_over_45pct_kcal_and_carbs_under_20g")
        if carbs_g < 20 and kcal > 350:
            flags.append("low_carb_main")
            reasons.append("main_meal_over_350kcal_with_carbs_under_20g")
        if protein_g < 15 and kcal > 350:
            flags.append("low_protein_main")
            reasons.append("main_meal_over_350kcal_with_protein_under_15g")

    if slot_name == "breakfast":
        if protein_g < 8 and kcal > 300:
            flags.append("breakfast_low_protein")
            reasons.append("breakfast_over_300kcal_with_protein_under_8g")
        if (grams is not None and grams > 500) or kcal > 800:
            flags.append("breakfast_too_large")
            reasons.append("breakfast_portion_or_kcal_high")

    if slot_name == "snack":
        if kcal > 350 or (grams is not None and grams > 300):
            flags.append("snack_too_large")
            reasons.append("snack_over_350kcal_or_300g")
        if kcal > 350 and _looks_like_full_meal(protein_g, carbs_g, fat_g):
            flags.append("snack_too_meal_like")
            reasons.append("snack_has_full_meal_macro_pattern")

    if not flags:
        reasons.append("meal_realism_ok")

    total_penalty = sum(BASE_PENALTIES.get(flag, 0.0) for flag in set(flags))
    return _result(total_penalty, flags, reasons)


def _compute_practical_realism(
    candidate_row: Mapping[str, object],
    slot: str,
) -> dict[str, object]:
    flags: list[str] = []
    reasons: list[str] = []
    reject_reasons: list[str] = []
    slot_name = str(slot or "").strip().lower()
    kcal = _to_optional_float(candidate_row.get("kcal"))
    protein_g = _to_optional_float(candidate_row.get("protein_g"))
    carbs_g = _to_optional_float(candidate_row.get("carbs_g"))
    fat_g = _to_optional_float(candidate_row.get("fat_g"))
    grams = _to_optional_float(candidate_row.get("portion_grams_estimated"))

    if any(value is None for value in (kcal, protein_g, carbs_g, fat_g)):
        return _result(
            0.0,
            [],
            ["missing_macro_values_neutral"],
            hard_reject=False,
            reject_reasons=[],
        )

    if grams is None:
        flags.append("missing_portion_grams")
        reasons.append("missing_portion_grams_warning")

    if kcal <= 0:
        return _result(
            PRACTICAL_PENALTIES.get("missing_portion_grams", 0.0)
            if "missing_portion_grams" in flags
            else 0.0,
            flags,
            [*reasons, "missing_or_zero_kcal_neutral"],
            hard_reject=False,
            reject_reasons=[],
        )

    protein_share = (protein_g * 4.0) / kcal
    carbs_share = (carbs_g * 4.0) / kcal

    if slot_name == "breakfast":
        _add_breakfast_practical_flags(
            flags=flags,
            reasons=reasons,
            reject_reasons=reject_reasons,
            kcal=kcal,
            protein_g=protein_g,
            grams=grams,
        )
    elif slot_name in {"lunch", "dinner"}:
        _add_main_practical_flags(
            flags=flags,
            reasons=reasons,
            reject_reasons=reject_reasons,
            kcal=kcal,
            protein_g=protein_g,
            carbs_g=carbs_g,
            protein_share=protein_share,
            carbs_share=carbs_share,
            grams=grams,
        )
    elif slot_name == "snack":
        _add_snack_practical_flags(
            flags=flags,
            reasons=reasons,
            reject_reasons=reject_reasons,
            kcal=kcal,
            grams=grams,
        )

    if not flags:
        reasons.append("meal_realism_ok")

    hard_reject = bool(reject_reasons)
    total_penalty = sum(PRACTICAL_PENALTIES.get(flag, 0.0) for flag in set(flags))
    if hard_reject:
        total_penalty += PRACTICAL_HARD_REJECT_PENALTY
    return _result(
        total_penalty,
        flags,
        reasons,
        hard_reject=hard_reject,
        reject_reasons=reject_reasons,
    )


def _add_breakfast_practical_flags(
    flags: list[str],
    reasons: list[str],
    reject_reasons: list[str],
    kcal: float,
    protein_g: float,
    grams: float | None,
) -> None:
    if kcal > 850:
        flags.append("breakfast_too_large")
        reasons.append("practical_breakfast_kcal_over_850")
        reject_reasons.append("breakfast_kcal_over_850")
    elif kcal > 750:
        flags.append("breakfast_too_large")
        reasons.append("practical_breakfast_kcal_over_750")

    if grams is not None and grams > 500:
        flags.extend(["unrealistic_large_portion", "breakfast_too_large"])
        reasons.append("practical_breakfast_portion_grams_over_500")
        reject_reasons.append("breakfast_portion_grams_over_500")
    elif grams is not None and grams > 425:
        flags.append("borderline_large_portion")
        reasons.append("practical_breakfast_portion_grams_over_425")

    if protein_g < 8 and kcal > 400:
        flags.append("breakfast_low_protein")
        reasons.append("practical_breakfast_over_400kcal_with_protein_under_8g")


def _add_main_practical_flags(
    flags: list[str],
    reasons: list[str],
    reject_reasons: list[str],
    kcal: float,
    protein_g: float,
    carbs_g: float,
    protein_share: float,
    carbs_share: float,
    grams: float | None,
) -> None:
    if grams is not None and grams > 850:
        flags.append("unrealistic_large_portion")
        reasons.append("practical_main_portion_grams_over_850")
        reject_reasons.append("main_portion_grams_over_850")
    elif grams is not None and grams > 700:
        flags.append("borderline_large_portion")
        reasons.append("practical_main_portion_grams_over_700")

    if kcal > 1250:
        flags.append("main_too_large")
        reasons.append("practical_main_kcal_over_1250")
        reject_reasons.append("main_kcal_over_1250")
    elif kcal > 1000:
        flags.append("main_too_large")
        reasons.append("practical_main_kcal_over_1000")

    if protein_g < 20 and kcal > 450:
        flags.append("low_protein_main")
        reasons.append("practical_main_over_450kcal_with_protein_under_20g")
    if carbs_g < 30 and kcal > 450:
        flags.append("low_carb_main")
        reasons.append("practical_main_over_450kcal_with_carbs_under_30g")
    if carbs_share > 0.70 and protein_g < 20:
        flags.append("mostly_carb_meal")
        reasons.append("practical_carbs_over_70pct_kcal_and_protein_under_20g")
    if protein_share > 0.45 and carbs_g < 30:
        flags.append("mostly_protein_meal")
        reasons.append("practical_protein_over_45pct_kcal_and_carbs_under_30g")


def _add_snack_practical_flags(
    flags: list[str],
    reasons: list[str],
    reject_reasons: list[str],
    kcal: float,
    grams: float | None,
) -> None:
    if kcal > 400:
        flags.append("snack_too_large")
        reasons.append("practical_snack_kcal_over_400")
        reject_reasons.append("snack_kcal_over_400")
    elif kcal > 350:
        flags.append("snack_too_large")
        reasons.append("practical_snack_kcal_over_350")

    if grams is not None and grams > 300:
        flags.extend(["unrealistic_large_portion", "snack_too_large"])
        reasons.append("practical_snack_portion_grams_over_300")
        reject_reasons.append("snack_portion_grams_over_300")
    elif grams is not None and grams > 250:
        flags.append("borderline_large_portion")
        reasons.append("practical_snack_portion_grams_over_250")

    if kcal > 350 or (grams is not None and grams > 250):
        flags.append("snack_too_meal_like")
        reasons.append("practical_snack_kcal_or_portion_meal_like")


def _add_base_portion_flags(
    flags: list[str],
    reasons: list[str],
    slot: str,
    grams: float,
) -> None:
    borderline = 0.0
    unrealistic = 0.0
    if slot == "breakfast":
        borderline = 400.0
        unrealistic = 500.0
    elif slot in {"lunch", "dinner"}:
        borderline = 650.0
        unrealistic = 850.0
    elif slot == "snack":
        borderline = 250.0
        unrealistic = 300.0

    if unrealistic and grams > unrealistic:
        flags.append("unrealistic_large_portion")
        reasons.append(f"portion_grams_over_{int(unrealistic)}")
    elif borderline and grams > borderline:
        flags.append("borderline_large_portion")
        reasons.append(f"portion_grams_over_{int(borderline)}")


def _looks_like_full_meal(protein_g: float, carbs_g: float, fat_g: float) -> bool:
    strong_macro_count = sum(
        [
            protein_g >= 20,
            carbs_g >= 40,
            fat_g >= 15,
        ]
    )
    return strong_macro_count >= 2


def _result(
    total_penalty: float,
    flags: list[str],
    reasons: list[str],
    hard_reject: bool = False,
    reject_reasons: list[str] | None = None,
) -> dict[str, object]:
    penalty = max(0.0, float(total_penalty))
    score = max(0.0, min(1.0, 1.0 - penalty))
    return {
        "meal_realism_score": round(score, 4),
        "meal_realism_penalty": round(penalty, 4),
        "meal_realism_flags": sorted(set(flags)),
        "meal_realism_reasons": _dedupe(reasons),
        "realism_hard_reject": bool(hard_reject),
        "realism_reject_reason": _dedupe(reject_reasons or []),
    }


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value) or math.isinf(numeric_value):
        return None
    return numeric_value
