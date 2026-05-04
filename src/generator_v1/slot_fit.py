from __future__ import annotations

import math
import re
from collections.abc import Mapping

from src.generator_v1.macro_fit import clamp


HEAVY_MAIN_HINTS = {
    "adobo",
    "beef",
    "burger",
    "catfish",
    "chicken",
    "dinner",
    "meatball",
    "meatloaf",
    "pork",
    "ribs",
    "roast",
    "sausage",
    "steak",
    "taco",
    "tenderloin",
    "tuna",
}


def compute_slot_fit(candidate_row: Mapping[str, object], slot: str) -> dict[str, object]:
    normalized_slot = str(slot).strip().lower()
    if normalized_slot == "breakfast":
        score, reasons = _breakfast_fit(candidate_row)
    elif normalized_slot == "lunch":
        score, reasons = _lunch_fit(candidate_row)
    elif normalized_slot == "dinner":
        score, reasons = _dinner_fit(candidate_row)
    elif normalized_slot == "snack":
        score, reasons = _snack_fit(candidate_row)
    else:
        score, reasons = 0.50, ["slot_necunoscut_neutru"]

    return {
        "slot_fit": round(clamp(score), 4),
        "slot_fit_reasons": reasons or ["neutru"],
    }


def _breakfast_fit(candidate_row: Mapping[str, object]) -> tuple[float, list[str]]:
    score = 0.50
    reasons: list[str] = []
    total_time = _to_float(candidate_row.get("total_time_min"))
    kcal = _to_float(candidate_row.get("kcal"))

    if total_time is not None and total_time <= 20:
        score += 0.15
        reasons.append("mic_dejun_rapid")
    if kcal is not None and kcal <= 650:
        score += 0.10
        reasons.append("kcal_mic_dejun_ok")
    if kcal is not None and kcal > 750:
        score -= 0.15
        reasons.append("kcal_prea_mare_mic_dejun")
    if _looks_like_heavy_main(candidate_row):
        score -= 0.15
        reasons.append("pare_fel_principal_greu")

    return score, reasons


def _lunch_fit(candidate_row: Mapping[str, object]) -> tuple[float, list[str]]:
    score = 0.50
    reasons: list[str] = []
    kcal = _to_float(candidate_row.get("kcal"))
    protein_g = _to_float(candidate_row.get("protein_g"))

    if _is_standalone(candidate_row):
        score += 0.10
        reasons.append("reteta_standalone")
    if (kcal is not None and kcal >= 300) or (protein_g is not None and protein_g >= 15):
        score += 0.10
        reasons.append("substantial_pranz")
    if kcal is not None and kcal < 150:
        score -= 0.10
        reasons.append("kcal_foarte_mic_pranz")

    return score, reasons


def _dinner_fit(candidate_row: Mapping[str, object]) -> tuple[float, list[str]]:
    score = 0.50
    reasons: list[str] = []
    kcal = _to_float(candidate_row.get("kcal"))
    protein_g = _to_float(candidate_row.get("protein_g"))

    if _is_standalone(candidate_row):
        score += 0.10
        reasons.append("reteta_standalone")
    if (kcal is not None and kcal >= 350) or (protein_g is not None and protein_g >= 20):
        score += 0.10
        reasons.append("substantial_cina")
    if (
        kcal is not None
        and protein_g is not None
        and kcal < 200
        and protein_g < 10
    ):
        score -= 0.15
        reasons.append("prea_usor_cina")

    return score, reasons


def _snack_fit(candidate_row: Mapping[str, object]) -> tuple[float, list[str]]:
    score = 0.50
    reasons: list[str] = []
    total_time = _to_float(candidate_row.get("total_time_min"))
    kcal = _to_float(candidate_row.get("kcal"))

    if total_time is not None and total_time <= 10:
        score += 0.15
        reasons.append("gustare_rapida")
    if kcal is not None and kcal <= 250:
        score += 0.15
        reasons.append("kcal_gustare_ok")
    if kcal is not None and kcal > 450:
        score -= 0.20
        reasons.append("kcal_prea_mare_gustare")
    if total_time is not None and total_time > 20:
        score -= 0.15
        reasons.append("timp_prea_mare_gustare")
    if _looks_like_heavy_main(candidate_row):
        score -= 0.15
        reasons.append("pare_fel_principal")

    return score, reasons


def _looks_like_heavy_main(candidate_row: Mapping[str, object]) -> bool:
    # TODO: cand Recipes_DB are slot tags canonice, inlocuieste acest fallback textual.
    search_text = _search_text(candidate_row)
    return any(_contains_word(search_text, hint) for hint in HEAVY_MAIN_HINTS)


def _is_standalone(candidate_row: Mapping[str, object]) -> bool:
    recipe_kind = str(candidate_row.get("recipe_kind", "")).strip().lower()
    return recipe_kind == "standalone"


def _search_text(candidate_row: Mapping[str, object]) -> str:
    parts = [
        candidate_row.get("display_name"),
        candidate_row.get("recipe_name"),
        candidate_row.get("recipe_kind"),
        candidate_row.get("recipe_category"),
        candidate_row.get("recipe_subcategory"),
    ]
    return " ".join(_normalize_text(part) for part in parts if part is not None)


def _normalize_text(value: object) -> str:
    text = str(value).lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _contains_word(text: str, word: str) -> bool:
    pattern = rf"(?<![a-z0-9]){re.escape(word)}(?![a-z0-9])"
    return re.search(pattern, text) is not None


def _to_float(value: object) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value):
        return None
    return numeric_value

