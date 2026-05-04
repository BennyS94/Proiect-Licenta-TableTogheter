###########################################################################
# FISIER: src/core/daily_rules.py
#
# SCOP
#   Reguli "daily-aware" aplicate dupa ce avem un plan initial pe mese.
#   Aceste reguli NU schimba item-urile (nu inlocuiesc alimente), ci doar
#   ajusteaza portiile in limitele (min,max) ale template-urilor, ca sa obtinem
#   o zi mai coerenta nutritional la nivel macro.
#
# CE FACE (pe scurt)
#   1) compute_day_metrics(day_meals)
#      - calculeaza carb total si distributia pe mese (B/L/D/S) pentru side_carb
#      - calculeaza total grame legume si portii de legume (servings)
#
#   2) adjust_day_portions_in_place(day_meals, portion_ranges, max_adjustments)
#      - incearca cateva "nudges" (max_adjustments) pe portii pentru a respecta:
#         a) carb mai mult devreme: (Breakfast + Lunch) / total_carb >= target
#         b) minim 2 portii legume/zi (servings >= 2)
#      - ajustarile sunt soft: nu garanteaza perfect, doar imbunatateste
#      - pastreaza log cu ajustarile facute + metrici before/after
#
# UNDE SE FOLOSESTE
#   - generator_v2.py construieste structura day_meals din plan_v2.csv (in memorie),
#     apoi apeleaza adjust_day_portions_in_place() si rescrie portiile inapoi in plan.
#
# INPUT STRUCTURA (day_meals)
#   Lista de mese, fiecare cu: { 'meal_id': str, 'items': [ ... ] }
#   Fiecare item asteptat:
#     {
#       'role': 'protein'|'side_carb'|'side_veg',
#       'uid': str,
#       'name': str,
#       'portion_g': float,
#       'nutr': {'carb_g_per_100g': float, 'fibre_g_per_100g': float}
#     }
#
# NOTE
#   - Comentarii in romana fara diacritice.
#   - Ajustarile sunt intentionat conservative (max 1-2 pasi) pentru a evita
#     efecte secundare mari asupra scorului pe masa.
###########################################################################
# src/core/daily_rules.py
# Daily-aware rules: post-hoc metrics + gentle adjustments.

from __future__ import annotations
from typing import Dict, List, Tuple, Any

VEG_SERVING_G = 90.0  # one veg serving ~90 g  # o portie de legume (heuristic) in grame
CARB_EARLY_TARGET_FRAC = 0.60  # (B + L) carbs / total carbs >= 0.60  # tinta: procent carbo la (Breakfast+Lunch) din total carbo

###########################################################################
# _meal_id_to_slot_key
# Ce face: mapare meal_id -> cheie scurta B/L/D/S pentru metrici

def _meal_id_to_slot_key(meal_id: str) -> str:
    # e.g., "breakfast" -> "B", "lunch" -> "L", "dinner" -> "D", "snack" -> "S"
    m = (meal_id or "").lower()
    if m.startswith("b"): return "B"
    if m.startswith("l"): return "L"
    if m.startswith("d"): return "D"
    if m.startswith("s"): return "S"
    return "?"

###########################################################################
# compute_day_metrics
# Ce face: calculeaza metrici zilnice (carb BL frac, veg servings etc.)
# Legaturi: folosit in adjust_day_portions_in_place

def compute_day_metrics(day_meals: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    day_meals: list of meal dicts returned by generator (each has 'meal_id', 'items' list).
      Expected each item: {
        'role': 'protein'|'side_carb'|'side_veg',
        'uid': str,
        'name': str,
        'portion_g': float,
        'nutr': {'carb_g_per_100g': float, 'fibre_g_per_100g': float}
      }
    Returns metrics dict. If structure not found, returns zeros.
    """
    carbs_by_meal = {"B": 0.0, "L": 0.0, "D": 0.0, "S": 0.0}  # acumulam carbo doar din side_carb pe fiecare masa
    veg_grams_total = 0.0  # total grame legume din side_veg pe zi

    try:
        for meal in day_meals:
            mkey = _meal_id_to_slot_key(meal.get("meal_id", ""))
            items = meal.get("items", [])
            for it in items:
                role = it.get("role", "")
                g = float(it.get("portion_g", 0.0) or 0.0)
                nutr = it.get("nutr", {}) or {}
                carb_per100 = float(nutr.get("carb_g_per_100g", 0.0) or 0.0)
                if role == "side_carb":  # doar garnitura carb contribuie la metricul de carbo (nu proteine/legume)
                    carbs_by_meal[mkey] += (g * carb_per100 / 100.0)  # acumulam carbo doar din side_carb pe fiecare masa
                if role == "side_veg":  # legumele contribuie la metricul de veg servings
                    veg_grams_total += g  # total grame legume din side_veg pe zi
    except Exception:
        # if anything is unexpected, keep zeros; upstream code should treat metrics as soft
        pass

    carb_total = sum(carbs_by_meal.values()) or 0.0  # acumulam carbo doar din side_carb pe fiecare masa
    frac_bl = ((carbs_by_meal["B"] + carbs_by_meal["L"]) / carb_total) if carb_total > 0 else 0.0  # acumulam carbo doar din side_carb pe fiecare masa
    veg_servings = veg_grams_total / VEG_SERVING_G if VEG_SERVING_G > 0 else 0.0  # o portie de legume (heuristic) in grame

    return {
        "carb_total_g": carb_total,
        "carb_B_g": carbs_by_meal["B"],  # acumulam carbo doar din side_carb pe fiecare masa
        "carb_L_g": carbs_by_meal["L"],  # acumulam carbo doar din side_carb pe fiecare masa
        "carb_D_g": carbs_by_meal["D"],  # acumulam carbo doar din side_carb pe fiecare masa
        "carb_S_g": carbs_by_meal["S"],  # acumulam carbo doar din side_carb pe fiecare masa
        "carb_BL_frac": frac_bl,  # (B+L)/total carbo; regula 'carb mai mult devreme'
        "veg_total_g": veg_grams_total,  # total grame legume din side_veg pe zi
        "veg_servings": veg_servings,
    }

###########################################################################
# adjust_day_portions_in_place
# Ce face: ajusteaza portii (in-place) in limitele template-urilor
# Legaturi: generator_v2.py

def adjust_day_portions_in_place(
    day_meals: List[Dict[str, Any]],
    portion_ranges: Dict[Tuple[str, str], Tuple[float, float]],
    max_adjustments: int = 2
) -> Dict[str, Any]:
    """
    Try up to `max_adjustments` portion nudges within template ranges to meet:
      1) carb earlier: (B+L)/total >= 0.60
      2) min veg servings: >= 2
    portion_ranges key: (meal_id, role) -> (min_g, max_g)
      Example: { ('breakfast','side_carb'): (40,80), ('dinner','side_veg'): (60,120) }
    Returns info dict with 'adjustments' log and final metrics.
    """
    info = {"adjustments": []}

    # compute initial metrics
    metrics = compute_day_metrics(day_meals)
    info["metrics_before"] = metrics

    # helper: get (min,max) range for a given meal+role; fallback to current portion if missing
    def _range_for(meal_id: str, role: str, current_g: float) -> Tuple[float, float]:  # helper: ia (min,max) pentru meal+role; fallback la portia curenta
        rng = portion_ranges.get((meal_id, role))
        if not rng:
            return (current_g, current_g)
        mn, mx = float(rng[0] or current_g), float(rng[1] or current_g)
        if mn > mx:
            mn, mx = mx, mn
        return (mn, mx)

    adj_left = max_adjustments  # cate ajustari mai avem voie sa facem (soft, limitat)

    # Rule 1: carb earlier
    if metrics["carb_total_g"] > 0 and metrics["carb_BL_frac"] < CARB_EARLY_TARGET_FRAC and adj_left > 0:  # tinta: procent carbo la (Breakfast+Lunch) din total carbo
        # strategy: push dinner carb portion to min, push breakfast or lunch carb to max (prefer lunch first)
        for meal_pref in ("dinner", "lunch", "breakfast"):
            # collect carb items for that meal
            for meal in day_meals:
                if meal.get("meal_id","").lower() != meal_pref:
                    continue
                for it in meal.get("items", []):
                    if it.get("role") != "side_carb":
                        continue
                    cur = float(it.get("portion_g", 0.0) or 0.0)
                    mn, mx = _range_for(meal_pref, "side_carb", cur)  # helper: ia (min,max) pentru meal+role; fallback la portia curenta
                    target = mn if meal_pref == "dinner" else mx  # scadem cina, crestem lunch/breakfast
                    if (meal_pref == "dinner" and cur > mn) or (meal_pref in ("lunch","breakfast") and cur < mx):
                        it["portion_g"] = target
                        info["adjustments"].append(
                            {"type":"carb_shift", "meal":meal_pref, "from":cur, "to":target})
                        adj_left -= 1  # cate ajustari mai avem voie sa facem (soft, limitat)
                        if adj_left <= 0:  # cate ajustari mai avem voie sa facem (soft, limitat)
                            break
                if adj_left <= 0:  # cate ajustari mai avem voie sa facem (soft, limitat)
                    break
            if adj_left <= 0:  # cate ajustari mai avem voie sa facem (soft, limitat)
                break

    # Rule 2: min 2 veg servings
    if metrics["veg_servings"] < 2.0 and adj_left > 0:  # cate ajustari mai avem voie sa facem (soft, limitat)
        needed_g = max(0.0, 2.0*VEG_SERVING_G - metrics["veg_total_g"])  # o portie de legume (heuristic) in grame
        if needed_g > 0:  # grame legume necesare ca sa atingem 2 servings
            # increase veg portions toward max on lunch then dinner
            for meal_pref in ("lunch","dinner","breakfast","snack"):
                if needed_g <= 0 or adj_left <= 0:  # cate ajustari mai avem voie sa facem (soft, limitat)
                    break
                for meal in day_meals:
                    if meal.get("meal_id","").lower() != meal_pref:
                        continue
                    for it in meal.get("items", []):
                        if it.get("role") != "side_veg":
                            continue
                        cur = float(it.get("portion_g", 0.0) or 0.0)
                        mn, mx = _range_for(meal_pref, "side_veg", cur)  # helper: ia (min,max) pentru meal+role; fallback la portia curenta
                        if cur < mx:
                            # nudge up but do not exceed what we need
                            delta = min(mx - cur, needed_g)  # grame legume necesare ca sa atingem 2 servings
                            new_g = cur + delta
                            it["portion_g"] = new_g
                            needed_g -= delta  # grame legume necesare ca sa atingem 2 servings
                            info["adjustments"].append(
                                {"type":"veg_servings", "meal":meal_pref, "from":cur, "to":new_g})
                            adj_left -= 1  # cate ajustari mai avem voie sa facem (soft, limitat)
                            if needed_g <= 0 or adj_left <= 0:  # cate ajustari mai avem voie sa facem (soft, limitat)
                                break
                # continue outer loops as needed

    # final metrics
    info["metrics_after"] = compute_day_metrics(day_meals)  # recalc metrici dupa ajustari
    return info
###############################################################################
# OBSERVATII / POSIBILE OPTIMIZARI (NU SCHIMBA ACUM, DOAR DE TINUT MINTE)
#
# 1) Regula carb devreme e foarte simpla (doar muta portii la min/max)
#    - E ok pentru v1. Pe viitor poti face "nudge" proportional (ex. +/-10g)
#      ca sa nu schimbi brusc kcal pe mese.
#
# 2) Legumele sunt masurate doar ca grame side_veg
#    - Daca in viitor ai legume in alte sloturi (ex. in protein dish), nu apar aici.
#      Poti extinde metricul sa includa tag-uri (ex. tag_salad_like) sau veg_bucket.
#
# 3) max_adjustments mic
#    - E intentionat. Daca vrei sa atingi tinta mai des, creste la 3-4, dar
#      ai grija: poate strica echilibrul pe masa si scorarea initiala.
#
# 4) Nu se recalculeaza scoring aici
#    - Ajustarile sunt post-hoc. Generatorul recalculaza macro-urile, dar scorul
#      original al combinatiilor nu este recalculat. Daca vrei coerenta completa,
#      poti recalcula score_combo dupa ajustari (mai scump, dar mai corect).
#
# 5) Fibre metric nefolosit
#    - Structura nutr contine si fibre, dar nu o folosim. Poti adauga o regula
#      daily-aware: fibre_total >= X (dar ai nevoie de estimate/target in rules).
###############################################################################
