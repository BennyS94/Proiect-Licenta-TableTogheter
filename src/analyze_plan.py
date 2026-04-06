###########################################################################
# FISIER: src/analyze_plan.py
#
# SCOP
#   Transforma output-ul real al generatorului (outputs/plan_v2.csv) intr-un
#   format text "readable" (outputs/plan_v2_readable.txt), usor de citit
#   de om si util pentru debug / validare / licenta.
#
# CE FACE (pe scurt)
#   - Citeste plan_v2.csv (mase + uid-uri + portii)
#   - Incarca foods_enriched (parquet; fallback CSV) pentru lookup pe uid
#   - Recalculeaza macro-urile pentru fiecare componenta (Ps/Sc/Ve) folosind
#     coloanele canonice per 100g/ml (kcal_sanitized, protein_g_100g_ml, etc.)
#   - Afiseaza alternativele alt_* daca exista in CSV
#   - Calculeaza totalurile zilnice din plan (kcal, P/C/F si procente)
#
# INTRARI (CLI)
#   --plan           outputs/plan_v2.csv
#   --foods          data/foods_enriched.parquet
#   --foods_fallback data/foods_enriched_min.csv.gz
#   --profile        profiles/user_profile_sample.json
#   --out            outputs/plan_v2_readable.txt
#
# IESIRI
#   --out: fisier text cu structura pe mese + totaluri zilnice
#
# LEGATURI
#   - generator_v2.py: scrie plan_v2.csv si alt_*; apeleaza acest script pentru readable
#   - enrich_foods.py: produce coloanele canonice folosite aici (kcal_sanitized, *_g_100g_ml)
#
# NOTE
#   - Comentarii in romana fara diacritice.
#   - Nu schimbam functionalitatea; doar documentam si clarificam punctele critice.
###########################################################################
# src/analyze_plan.py
import argparse, json, re
from pathlib import Path
import pandas as pd
import numpy as np

# --- aceleași split-uri ca în generator ---
###########################################################################
# meal_split
# Ce face: genereaza lista de mese + procente kcal (aceeasi logica ca in generator)
# Legaturi: generator_v2.py

def meal_split(meals, snacks):
    if meals==3 and not snacks: return ["Breakfast","Lunch","Dinner"], [0.30,0.40,0.30]
    if meals==3 and snacks:     return ["Breakfast","Lunch","Snack","Dinner"], [0.25,0.35,0.15,0.25]
    if meals==4 and not snacks: return ["Breakfast","Lunch","Snack","Dinner"], [0.25,0.35,0.15,0.25]
    if meals==4 and snacks:     return ["Breakfast","Snack1","Lunch","Dinner","Snack2"], [0.20,0.10,0.30,0.30,0.10]
    if meals==5 and not snacks: return ["Breakfast","Snack1","Lunch","Snack2","Dinner"], [0.20,0.10,0.30,0.10,0.30]
    if meals==5 and snacks:     return ["Breakfast","Snack1","Lunch","Snack2","Dinner","Snack3"], [0.18,0.10,0.27,0.10,0.25,0.10]
    return ["Breakfast","Lunch","Dinner"], [0.30,0.40,0.30]

###########################################################################
# read_foods
# Ce face: citeste foods din parquet sau csv (fallback robust)
# Legaturi: data/foods_enriched.parquet / data/foods_enriched_min.csv.gz

def read_foods(foods_path: Path) -> pd.DataFrame:
    if foods_path.suffix == ".parquet":
        try:
            return pd.read_parquet(foods_path)
        except Exception:
            pass
    return pd.read_csv(foods_path)

###########################################################################
# macros_for_row
# Ce face: calculeaza macro-urile pentru un rand foods si o portie in grame
# Legaturi: coloane *_g_100g_ml + kcal_sanitized

def macros_for_row(row: pd.Series, portion_g: float):
    f = (float(portion_g) if pd.notna(portion_g) else 0.0) / 100.0
    def num0(c):
        v = pd.to_numeric(row.get(c, 0.0), errors="coerce")
        return float(v) if pd.notna(v) else 0.0
    def pick_first(cands):
        for c in cands:
            if c in row.index:
                v = pd.to_numeric(row.get(c), errors="coerce")
                if pd.notna(v):
                    return float(v)
        return 0.0

    carb_candidates = [  # fallback pe nume alternative pentru carbo (stabilitate)
        "carbohydrate_g_100g_ml", "carbohydrates_g_100g_ml",
        "carbohydrate_g_100g", "carbohydrates_g_100g", "carbs_g_100g_ml"
    ]

    kcal = num0("kcal_sanitized") * f
    P    = num0("protein_g_100g_ml") * f
    C    = pick_first(carb_candidates) * f  # fallback pe nume alternative pentru carbo (stabilitate)
    F    = num0("fat_g_100g_ml") * f
    return dict(kcal=kcal, P=P, C=C, F=F)

###########################################################################
# fmt_macros
# Ce face: formateaza macro-urile intr-un string compact pentru afisare

def fmt_macros(d):
    return f"kcal {d['kcal']:.0f}; P {d['P']:.1f}; C {d['C']:.1f}; F {d['F']:.1f}"

###########################################################################
# _alts_from_row  # alternative alt_* (swap similar) daca exista in plan_v2.csv
# Ce face: extrage alternativele alt_*_{i}_name/_sim din plan_v2.csv
# Legaturi: plan_v2.csv

def _alts_from_row(row: pd.Series, prefix: str, K: int = 5):  # alternative alt_* (swap similar) daca exista in plan_v2.csv
    """Colectează alternativele din coloane de tip alt_<prefix>_{i}_name/_sim."""
    out = []
    for i in range(1, K + 1):
        n = str(row.get(f"{prefix}_{i}_name", "") or "").strip()
        if not n:
            continue
        s = row.get(f"{prefix}_{i}_sim", None)
        try:
            s_ok = (s is not None) and pd.notna(s)
        except Exception:
            s_ok = False
        out.append(f"{n} (sim {float(s):.2f})" if s_ok else n)
    return out

###########################################################################
# main
# Ce face: CLI: citeste planul, face lookup in foods, scrie fisierul readable
# Legaturi: outputs/plan_v2.csv -> outputs/plan_v2_readable.txt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default="outputs/plan_v2.csv")
    ap.add_argument("--foods", default="data/foods_enriched.parquet")
    ap.add_argument("--foods_fallback", default="data/foods_enriched_min.csv.gz")
    ap.add_argument("--profile", default="profiles/user_profile_sample.json")
    ap.add_argument("--out", default="outputs/plan_v2_readable.txt")
    args = ap.parse_args()

    plan_p = Path(args.plan); foods_p = Path(args.foods); out_p = Path(args.out)

    if not plan_p.exists():
        raise SystemExit(f"Nu găsesc {plan_p}")
    df_plan = pd.read_csv(plan_p)

    # profil → procentele țintă
    prof = json.loads(Path(args.profile).read_text(encoding="utf-8"))
    names, split = meal_split(prof.get("meals_per_day",3), bool(prof.get("include_snacks", False)))
    split_map = {n: f for n, f in zip(names, split)}  # map: nume_masa -> procent_kcal (pentru afisare)

    # încărcăm foods (parquet sau fallback CSV)
    if foods_p.exists():
        df_foods = read_foods(foods_p)
    else:
        fb = Path(args.foods_fallback)
        if not fb.exists():
            raise SystemExit(f"Nu găsesc {foods_p} sau fallback {fb}")
        df_foods = read_foods(fb)

    # index pentru lookup rapid după uid
    idx = df_foods.set_index("uid", drop=False)  # index rapid uid -> rand (lookup constant)

    # total kcal plan real (din plan)
    total_kcal_real = float(pd.to_numeric(df_plan["kcal_meal"], errors="coerce").sum())  # kcal total din plan (suma kcal_meal din CSV)

    lines = []
    for _, r in df_plan.iterrows():
        meal = str(r.get("meal",""))
        pct = split_map.get(meal, 0.0) * 100.0  # map: nume_masa -> procent_kcal (pentru afisare)
        # componente
        ps_uid = str(r.get("protein_uid",""))
        sc_uid = str(r.get("side_carb_uid",""))
        ve_uid = str(r.get("side_veg_uid",""))

        ps_name = str(r.get("protein_name","")).strip()
        sc_name = str(r.get("side_carb_name","")).strip()
        ve_name = str(r.get("side_veg_name","")).strip()

        ps_g = float(r.get("protein_portion_g", 0.0) or 0.0)
        sc_g = float(r.get("side_carb_portion_g", 0.0) or 0.0)
        ve_g = float(r.get("side_veg_portion_g", 0.0) or 0.0)

        # macros per component (din foods_enriched)
        def lookup(uid):  # helper: intoarce rand foods dupa uid sau Series gol
            return idx.loc[uid] if uid and uid in idx.index else pd.Series({})
        ps_row, sc_row, ve_row = lookup(ps_uid), lookup(sc_uid), lookup(ve_uid)

        ps_mac = macros_for_row(ps_row, ps_g) if not ps_row.empty else dict(kcal=0,P=0,C=0,F=0)
        sc_mac = macros_for_row(sc_row, sc_g) if not sc_row.empty else dict(kcal=0,P=0,C=0,F=0)
        ve_mac = macros_for_row(ve_row, ve_g) if not ve_row.empty else dict(kcal=0,P=0,C=0,F=0)

        # total masă
        tot_g = ps_g + sc_g + ve_g
        tot = dict(
            kcal = ps_mac["kcal"] + sc_mac["kcal"] + ve_mac["kcal"],
            P    = ps_mac["P"]    + sc_mac["P"]    + ve_mac["P"],
            C    = ps_mac["C"]    + sc_mac["C"]    + ve_mac["C"],
            F    = ps_mac["F"]    + sc_mac["F"]    + ve_mac["F"],
        )

        lines.append(f"{meal} {pct:.0f}%")  # header masa + procent tinta (din profil)
        if ps_name:
            lines.append(f"  Ps -> {ps_name}; {ps_g:.1f} g; {fmt_macros(ps_mac)}")
        if sc_name:
            lines.append(f"  Sc -> {sc_name}; {sc_g:.1f} g; {fmt_macros(sc_mac)}")
        if ve_name:
            lines.append(f"  Ve -> {ve_name}; {ve_g:.1f} g; {fmt_macros(ve_mac)}")
        lines.append(f"  Total: {tot_g:.1f} g; {fmt_macros(tot)}")
        lines.append("")  # linie goală între mese

        # --- ALTERNATIVE (dacă există în CSV) ---
        aps = _alts_from_row(r, "alt_protein", K=5)  # alternative alt_* (swap similar) daca exista in plan_v2.csv
        acs = _alts_from_row(r, "alt_carb",    K=5)  # alternative alt_* (swap similar) daca exista in plan_v2.csv
        avs = _alts_from_row(r, "alt_veg",     K=5)  # va rămâne gol dacă n-ai adăugat alt_veg în generator  # alternative alt_* (swap similar) daca exista in plan_v2.csv

        if aps:
            lines.append("  Alt protein: " + " | ".join(aps))
        if acs:
            lines.append("  Alt carb:    " + " | ".join(acs))
        if avs:
            lines.append("  Alt veg:     " + " | ".join(avs))



    # sumar zilnic din plan (real)
    kcalP = float(pd.to_numeric(df_plan.get("protein_meal_g"), errors="coerce").sum()) * 4  # conversie P/C/F in kcal pentru procentele zilnice
    kcalC = float(pd.to_numeric(df_plan.get("carb_meal_g"), errors="coerce").sum()) * 4
    kcalF = float(pd.to_numeric(df_plan.get("fat_meal_g"), errors="coerce").sum()) * 9
    pctP = 100*kcalP/total_kcal_real if total_kcal_real else 0  # kcal total din plan (suma kcal_meal din CSV)
    pctC = 100*kcalC/total_kcal_real if total_kcal_real else 0  # kcal total din plan (suma kcal_meal din CSV)
    pctF = 100*kcalF/total_kcal_real if total_kcal_real else 0  # kcal total din plan (suma kcal_meal din CSV)

    lines.append("DAILY TOTALS (from plan)")
    lines.append(f"  kcal: {total_kcal_real:.0f}")  # kcal total din plan (suma kcal_meal din CSV)
    lines.append(f"  P: {kcalP/4:.1f} g ({pctP:.1f}% kcal) | C: {kcalC/4:.1f} g ({pctC:.1f}%) | F: {kcalF/9:.1f} g ({pctF:.1f}%)")

    # scrie fișierul
    out_p.parent.mkdir(parents=True, exist_ok=True)
    Path(out_p).write_text("\n".join(lines), encoding="utf-8")  # scriere fisier readable (outputs/plan_v2_readable.txt)
    print(f"OK (written): {out_p}")

if __name__ == "__main__":
    main()
###############################################################################
# OBSERVATII / POSIBILE OPTIMIZARI (NU SCHIMBA ACUM, DOAR DE TINUT MINTE)
#
# 1) Duplicare logica meal_split
#    - meal_split exista si in generator_v2.py. Daca vrei o singura sursa de adevar,
#      muta meal_split intr-un modul comun (ex. src/core/profile_utils.py) si importa-l.
#
# 2) Lookup foods: Series gol vs NaN
#    - lookup() intoarce pd.Series({}) daca uid nu exista. Asta e ok, dar poate crea
#      warning-uri daca in viitor accesezi chei lipsa. Alternativ: foloseste None.
#
# 3) Performanta
#    - iterrows() e ok pentru planuri mici (cateva mese/zi). La multi-day (7 zile),
#      tot e ok. Daca ajungi la sute de randuri, poti vectoriza partial.
#
# 4) Alternative alt_veg
#    - Comentariul spune ca alt_veg poate lipsi in generator. Daca vei stabiliza schema,
#      decide daca pastrezi alt_veg intotdeauna (cu NaN) sau il omiti complet.
#
# 5) Coloane canonice
#    - macros_for_row foloseste kcal_sanitized si *_g_100g_ml. Daca datasetul are alias-uri,
#      ideal le normalizezi in enrich_foods.py, nu aici.
#
# 6) P/C/F procente zilnice
#    - Procentele sunt calculate folosind kcal total din plan (kcal_meal). Daca exista
#      diferente intre kcal_meal si kcal recalculat din foods, procentele pot diferi usor.
#      Daca vrei consistenta, calculeaza kcal total din P/C/F (4/4/9) sau din sum mac.
#
# 7) Output pentru API
#    - Pe viitor, pentru API, vei vrea o functie care intoarce structura JSON (nu doar text),
#      folosind exact acelasi mapping plan_v2.csv -> items/macros/alternatives.
###############################################################################
