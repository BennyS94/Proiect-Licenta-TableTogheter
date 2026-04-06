###########################################################################
# TableTogetherAPP - generator_v2.py (annotat)
#
# Scop: creeaza planuri zilnice/multi-zi pe baza profilului, a regulilor culinare
# si a sabloanelor (templates). Include:
#  - selectie mese (din template sau fallback blueprint),
#  - scoring pe reguli (scoring.py),
#  - reguli zilnice (daily_rules) pentru carbo dimineata si min. 2 portii legume/zi,
#  - propuneri de inlocuire (swap similar) pe baza matricei de similaritate,
#  - scriere CSV + sumar TXT + "readable" TXT.
#
# Cum se ruleaza (ex. PowerShell):
#   python src\\generator_v2.py ^
#     --profile profiles\\user_profile_sample.json ^
#     --rules configs\\culinary_rules.yaml ^
#     --templates templates\\meal_templates.yaml ^
#     --foods data\\foods_enriched.parquet ^
#     --taxonomy configs\\taxonomy.yaml ^
#     --subs data\\substitution_edges.csv.gz ^
#     --out_csv outputs\\plan_v2.csv ^
#     --out_txt outputs\\plan_v2_summary.txt ^
#     --out_readable outputs\\plan_v2_readable.txt
#
#Legaturi:
# - src/core/scoring.py (score_combo)
# - src/core/pools.py (build_pool)
# - src/core/daily_rules.py (compute_day_metrics, adjust_day_portions_in_place)
# - src/ontology.py (taxonomie)
# - src/analyze_plan.py (readable)
# - configs/*.yaml (rules, templates, taxonomy)
# - data/*.parquet, *.csv (alimente, substitutii)
#
###########################################################################
# importuri si constante
# Ce face: incarca bibliotecile, rutele implicite, si utilitare simple
# Legaturi: fisiere YAML/Parquet/CSV din proiect
###########################################################################

import argparse, json
import pathlib
from pathlib import Path
import pandas as pd
import numpy as np
import yaml
import re
import sys, subprocess
import random, math

# daily rules (carb devreme + min 2 portii legume/zi)
try:
    # cand rulezi: python -m src.generator_v2
    from .core.daily_rules import compute_day_metrics, adjust_day_portions_in_place
except Exception:
    # cand rulezi: python src/generator_v2.py
    from core.daily_rules import compute_day_metrics, adjust_day_portions_in_place


# RNG: vom folosi in practica numpy.random.Generator; random.Random e folosit minim
# Recomandare viitoare: unifica pe numpy RNG

###########################################################################
# soft_pick_from_topk
# Ce face: alege un element din top-k prin softmax(-score/temperature)
# Legaturi: folosit pentru sampling "soft" (viitorul modul de diversitate)
###########################################################################
def soft_pick_from_topk(candidates, scores, k=3, temperature=0.7, rng=None):
    """
    candidates: lista obiecte/indici aliniati cu scores
    scores: scoruri (mai mic e mai bun)
    alege un element din top-k pe baza unei distributii softmax peste -score/temperature

    daca temperature <= 0 sau k <= 1 -> alege argmin
    """
    if rng is None:
        rng = random
    if not candidates or not scores:
        return None
    n = len(candidates)
    order = np.argsort(scores)[:max(1, min(k, n))]
    if temperature is None or temperature <= 0:
        return candidates[int(order[0])]

    tops = [candidates[int(i)] for i in order]
    topscores = [scores[int(i)] for i in order]
    logits = np.array([-s / float(temperature) for s in topscores], dtype=float)
    m = float(np.max(logits))  # stabilizare numerica
    exps = np.exp(logits - m)
    probs = exps / exps.sum()
    idx = int(rng.choices(range(len(tops)), weights=probs, k=1)[0])
    return tops[idx]

###########################################################################
# rute implicite + utilitare pentru rules/templates
# Ce face: defineste rutele standard si functii de validare a YAML
# Legaturi: configs/culinary_rules.yaml, templates/meal_templates.yaml
###########################################################################

RULES_PATH = pathlib.Path("configs/culinary_rules.yaml")
TEMPLATES_PATH = pathlib.Path("templates/meal_templates.yaml")  # templates separate de rules

def _has_slots(obj):
    # detecteaza daca YAML gresit mai contine "slots" in rules
    if isinstance(obj, list):
        return any(isinstance(x, dict) and "slots" in x for x in obj)
    if isinstance(obj, dict):
        return any(_has_slots(v) for v in obj.values())
    return False

def warn_if_rules_have_slots(rules_path):
    p = Path(rules_path)
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        if _has_slots(data):
            print("[WARN] 'culinary_rules.yaml' contine chei cu 'slots' — sunt ignorate. "
                  "Muta-le in 'templates/meal_templates.yaml'.", file=sys.stderr)
    except Exception:
        pass

def load_rules_and_templates(rules_path=None, templates_path=None):
    rp = pathlib.Path(rules_path) if rules_path else RULES_PATH
    tp = pathlib.Path(templates_path) if templates_path else TEMPLATES_PATH
    if not rp.exists():
        print(f"[ERROR] Lipsa {rp}", file=sys.stderr); sys.exit(2)
    if not tp.exists():
        print(f"[ERROR] Lipsa {tp}", file=sys.stderr); sys.exit(2)
    rules = yaml.safe_load(rp.read_text(encoding="utf-8"))
    templates = yaml.safe_load(tp.read_text(encoding="utf-8"))
    if _has_slots(rules):
        print("[WARN] 'culinary_rules.yaml' pare sa contina chei cu 'slots' — sunt ignorate. "
              "Muta-le in 'templates/meal_templates.yaml'.", file=sys.stderr)
    return rules, templates

###########################################################################
# importuri robust pentru scoring/pools/ontology
# Ce face: import fallback pentru rulare ca modul sau script
# Legaturi: src/core/scoring.py; src/core/pools.py; src/ontology.py
###########################################################################
try:
    from src.core.scoring import score_combo
except Exception:
    try:
        from core.scoring import score_combo
    except Exception:
        raise

try:
    from src.core.pools import build_pool
except Exception:
    try:
        from core.pools import build_pool
    except Exception:
        raise

try:
    from src.ontology import load_taxonomy
except Exception:
    from ontology import load_taxonomy

###########################################################################
# Utilitare profil si tinte
# Ce face: calculeaza TDEE, split mese, prag proteina etc.
# Legaturi: profilul utilizatorului (JSON)
###########################################################################

# Map activitate; acceptam atat engleza cat si romana
ACTIVITY_MAP = {
    "sedentary": 1.2, "lightly_active": 1.375, "moderately_active": 1.55,
    "very_active": 1.725, "extremely_active": 1.9,
    "sedentar": 1.2, "usor_activ": 1.375, "moderat_activ": 1.55,
    "foarte_activ": 1.725, "extrem": 1.9,
}
def bf_to_percent(bf):
    return {"lean":0.125,"normal":0.20,"overweight":0.30,"obese":0.40}.get(str(bf).lower(),0.20)

def bmr_mifflin(w,h,a,s):
    return 10*w + 6.25*h - 5*a + (5 if str(s).lower()=="male" else -161)

def bmr_harris(w,h,a,s):
    return (88.362 + 13.397*w + 4.799*h - 5.677*a) if str(s).lower()=="male" else (447.593 + 9.247*w + 3.098*h - 4.330*a)

def bmr_katch(w,bf):
    return 370 + 21.6*(w*(1 - bf_to_percent(bf)))

def tdee_from_profile(p):
    w,h,a,s = p["weight_kg"], p["height_cm"], p["age"], p["sex"]
    base = (bmr_mifflin(w,h,a,s) + bmr_harris(w,h,a,s) + bmr_katch(w, p.get("bf_profile","normal")))/3.0
    af = ACTIVITY_MAP.get(str(p.get("activity_level","moderately_active")).lower(),1.55)
    return base*af

def adjust_for_goal(tdee, goal, speed):
    goal = str(goal).lower(); speed = str(speed).lower()
    if goal == "loss":   adj = {"slow": -0.10, "moderate": -0.175, "aggressive": -0.25}.get(speed, -0.175)
    elif goal == "gain": adj = {"slow": 0.10, "moderate": 0.15, "aggressive": 0.20}.get(speed, 0.15)
    else: adj = 0.0
    return tdee * (1 + adj)

def meal_split(meals, snacks):
    # genereaza numele meselor si distributia calorica implicita
    if meals==3 and not snacks: return ["Breakfast","Lunch","Dinner"], [0.30,0.40,0.30]
    if meals==3 and snacks:     return ["Breakfast","Lunch","Snack","Dinner"], [0.25,0.35,0.15,0.25]
    if meals==4 and not snacks: return ["Breakfast","Lunch","Snack","Dinner"], [0.25,0.35,0.15,0.25]
    if meals==4 and snacks:     return ["Breakfast","Snack1","Lunch","Dinner","Snack2"], [0.20,0.10,0.30,0.30,0.10]
    if meals==5 and not snacks: return ["Breakfast","Snack1","Lunch","Snack2","Dinner"], [0.20,0.10,0.30,0.10,0.30]
    if meals==5 and snacks:     return ["Breakfast","Snack1","Lunch","Snack2","Dinner","Snack3"], [0.18,0.10,0.27,0.10,0.25,0.10]
    return ["Breakfast","Lunch","Dinner"], [0.30,0.40,0.30]

def protein_target(weight_kg):  # euristica CASUAL
    return int(5*round((1.8*weight_kg)/5.0))

###########################################################################
# IO: citire foods/rules/templates
# Ce face: incarca datele din disk cu fallback robust
# Legaturi: fisiere proiect
###########################################################################
def read_foods(path_parquet_or_csv):
    p = Path(path_parquet_or_csv)
    if not p.exists():
        raise SystemExit(f"Nu gasesc {p}")
    try:
        if p.suffix == ".parquet":
            return pd.read_parquet(p)
    except Exception:
        pass
    return pd.read_csv(p)

def read_rules(path_yaml):
    with open(path_yaml, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_templates(path):
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data
###########################################################################
# filter_slot
# Ce face: filtreaza pool-ul pentru un slot (protein/side_carb/side_veg)
# Legaturi: templates (chei slot: role, require_bucket, include/exclude_name)
###########################################################################
def filter_slot(pool, meal_id, slot):
    df = pool.copy()
    role = slot.get("role")
    role_col = {"protein":"role_protein","side_carb":"role_side_carb","side_veg":"role_side_veg"}[role]
    df = df[df[role_col]==1]

    # allowed_meals
    df = df[df.apply(lambda r: allowed_for_meal(r, meal_id), axis=1)]

    # NOTE: datasetul standard foloseste *_g_100g_ml; nu protein_g/carb_g simple
    def col(name):
        return df[name].fillna(0).astype(int) if name in df.columns else pd.Series(0, index=df.index)

    # reguli specifice rolului
    if role == "protein":
        # prag densitate proteina (per 100g)
        if "protein_g_100g_ml" in df.columns:
            p100 = pd.to_numeric(df["protein_g_100g_ml"], errors="coerce").fillna(0.0)
            df = df[p100 >= 8.0]  # proteina minim 8 g/100g
        # evitam bauturi / deserturi / composite drept proteina
        for tcol in ("tag_drink","tag_dairy_dessert_hint","tag_composite_dish_hint","tag_dessert_like"):
            if tcol in df.columns:
                df = df[col(tcol) != 1]

    if role == "side_carb":
        # prag carbo (>=10 g/100g)
        if "carbohydrate_g_100g_ml" in df.columns:
            c100 = pd.to_numeric(df["carbohydrate_g_100g_ml"], errors="coerce").fillna(0.0)
            df = df[c100 >= 10.0]
        # evitam amidonuri/ingrediente (starch powders)
        if "tag_starch_powder" in df.columns:
            df = df[col("tag_starch_powder") != 1]
        # la pranz/cina evitam cereale micul-dejun si "breakfasty"
        if str(meal_id).lower() in ("lunch","dinner"):
            for tcol in ("tag_sugary_breakfast_cereal","tag_breakfasty"):
                if tcol in df.columns:
                    df = df[col(tcol) != 1]

    if role == "side_veg":
        # optional: la mic dejun evitam supele ca side
        if str(meal_id).lower().startswith("b") and "name_core" in df.columns:
            name_core = df["name_core"].fillna("").str.lower()
            df = df[~name_core.str.contains(r"\bsoup\b", regex=True)]

    # bucket-uri cerute de slot (daca exista)
    req_b = [b.lower() for b in slot.get("require_bucket", [])]
    if req_b:
        colname = "protein_bucket" if role=="protein" else ("carb_bucket" if role=="side_carb" else "veg_bucket")
        df = df[df[colname].astype(str).str.lower().isin(req_b)]

    # include/exclude dupa nume
    name_core = df["name_core"].fillna("").str.lower()
    inc = [t.lower() for t in slot.get("include_name", [])]
    exc = [t.lower() for t in slot.get("exclude_name", [])]
    if inc:
        df = df[name_core.str.contains("|".join([re.escape(t) for t in inc]), regex=True, na=False)]
        name_core = df["name_core"].fillna("").str.lower()
    if exc:
        pattern = "|".join([re.escape(t) for t in exc]) if exc else ""
        if pattern:
            mask = ~name_core.str.contains(pattern, regex=True, na=False)
            df = df.loc[mask]

    return df

###########################################################################
# pick_meal_from_template
# Ce face: incearca sa construiasca o masa dintr-un sablon (slots) si sa o puncteze
# Legaturi: score_combo; foloseste macros_for, combo_hard_banned
###########################################################################
def pick_meal_from_template(df_pool, meal_name, kcal_target, protein_min_g, template, rules, used_buckets, tax):
    meal_key = meal_name.lower()
    if meal_key.startswith("snack"):
        meal_id = "snack"
    else:
        meal_id = {"breakfast":"breakfast","lunch":"lunch","dinner":"dinner"}.get(meal_key.split()[0], "lunch")

    # candidati pe sloturi
    rows = {}
    for slot in template.get("slots", []):
        cand = filter_slot(df_pool, meal_id, slot)
        if cand is None:
            cand = df_pool.iloc[0:0]
        if cand.empty:
            return None  # daca vreun slot nu are candidati, sablonul pica
        role = slot.get("role")
        if role=="protein":
            cand["prot100"]  = pd.to_numeric(cand["protein_g_100g_ml"], errors="coerce").fillna(0.0)
            cand["kcal100"]  = pd.to_numeric(cand["kcal_sanitized"], errors="coerce").fillna(1.0)
            cand["prot_per_100kcal"] = np.where(cand["kcal100"]>0, cand["prot100"]/cand["kcal100"]*100.0, 0.0)  # proteine "lean"
            cand = cand.sort_values(["prot_per_100kcal","prot100"], ascending=[False,False]).head(30)
        elif role=="side_carb":
            cand["kcal100"] = pd.to_numeric(cand["kcal_sanitized"], errors="coerce").fillna(0.0)
            cand = cand.sort_values("kcal100", ascending=False).head(40)  # cautam garnituri energice
        else:  # veg
            cand = cand.head(20)  # un set mic si curat
        rows[role] = cand

    # portii implicite: mijlocul intervalului
    def rng(slot):
        lo,hi = slot.get("portion_g_range",[80,200])
        return (lo+hi)/2.0
    portion = {slot["role"]: rng(slot) for slot in template.get("slots", [])}

    best = None
    for _, rp in rows.get("protein", pd.DataFrame()).iterrows():
        mac_p = macros_for(rp, portion.get("protein",0))
        for _, rc in rows.get("side_carb", pd.DataFrame()).iterrows():
            mac_c = macros_for(rc, portion.get("side_carb",0))
            rv = None; mac_v = {"kcal":0,"protein":0,"carb":0,"fat":0,"sugars":0,"fibres":0,"salt":0}
            if "side_veg" in rows:
                for _, rv_try in rows["side_veg"].iterrows():
                    pv = portion.get("side_veg",0)
                    mv = macros_for(rv_try, pv)
                    # nu depasim cu >200 kcal tinta mesei (marja rezonabila)
                    if mac_p["kcal"]+mac_c["kcal"]+mv["kcal"] <= kcal_target+200:
                        rv = rv_try; mac_v = mv; break

            totals = {
                "kcal": mac_p["kcal"]+mac_c["kcal"]+mac_v["kcal"],
                "protein": mac_p["protein"]+mac_c["protein"]+mac_v["protein"],
                "carb": mac_p["carb"]+mac_c["carb"]+mac_v["carb"],
                "fat": mac_p["fat"]+mac_c["fat"]+mac_v["fat"],
                "sugars": mac_p["sugars"]+mac_c["sugars"]+mac_v["sugars"],
                "fibres": mac_p["fibres"]+mac_c["fibres"]+mac_v["fibres"],
                "salt": mac_p["salt"]+mac_c["salt"]+mac_v["salt"],
                "kcal_target": kcal_target,
                "protein_min_g": protein_min_g
            }
            if combo_hard_banned(meal_id, rp, rc, rv, rules):
                continue  # combinatii interzise (ex. cereal+lapte la micul dejun)

            score, reasons = score_combo(meal_id, totals, rp, rc, rv, rules, used_buckets, tax=tax)
            row = {
                "meal": meal_name,
                "template_used": template.get("id", template.get("name","")),
                "protein_name": rp.get("name_core",""), "protein_uid": rp.get("uid",""),
                "protein_portion_g": round(portion.get("protein",0),1),
                "side_carb_name": rc.get("name_core",""), "side_carb_uid": rc.get("uid",""),
                "side_carb_portion_g": round(portion.get("side_carb",0),1),
                "side_veg_name": (rv.get("name_core","") if rv is not None else ""),
                "side_veg_uid": (rv.get("uid","") if rv is not None else ""),
                "side_veg_portion_g": round(portion.get("side_veg",0),1) if rv is not None else 0.0,
                "kcal_meal": round(totals["kcal"],1),
                "protein_meal_g": round(totals["protein"],1),
                "carb_meal_g": round(totals["carb"],1),
                "fat_meal_g": round(totals["fat"],1),
                "sugars_meal_g": round(totals["sugars"],1),
                "fibres_meal_g": round(totals["fibres"],1),
                "salt_meal_g": round(totals["salt"],2),
                "score": float(score),
                "reasons": "tpl;" + reasons,
                "protein_bucket": rp.get("protein_bucket",""),
                "carb_bucket": rc.get("carb_bucket",""),
            }

            def _slot_rng(tpl, role):
                for s in tpl.get("slots", []):
                    if s.get("role") == role:
                        lo, hi = s.get("portion_g_range", [0, 0])
                        return (float(lo or 0.0), float(hi or 0.0))
                return None

            row["protein_portion_range"] = _slot_rng(template, "protein")
            row["side_carb_portion_range"] = _slot_rng(template, "side_carb")
            row["side_veg_portion_range"] = _slot_rng(template, "side_veg")

            if (best is None) or (score < best["score"]):
                best = row
    return best
###########################################################################
# allowed_for_meal
# Ce face: verifica daca item-ul este permis la masa curenta (allowed_meals)
###########################################################################
def allowed_for_meal(row, meal_id):
    am = str(row.get("allowed_meals","")).split(";")
    return meal_id in am

###########################################################################
# apply_global_bans
# Ce face: aplica interdictii globale (din rules) pe flags sau termeni in nume
# Legaturi: rules.global_hard_bans.* (disallow_flags, disallow_name_contains)
###########################################################################
def apply_global_bans(df, rules):
    df2 = df.copy()
    for flag in rules.get("global_hard_bans", {}).get("disallow_flags", []):
        if flag in df2.columns:
            df2 = df2[df2[flag] != 1]
    bad_terms = [t.lower() for t in rules.get("global_hard_bans", {}).get("disallow_name_contains", [])]
    if bad_terms:
        name = df2["name_core"].fillna("").str.lower()
        pattern = "|".join([re.escape(t) for t in bad_terms])
        mask = ~name.str.contains(pattern, regex=True, na=False)
        df2 = df2[mask]
    return df2

###########################################################################
# filter_by_dietary
# Ce face: elimina alimentele care contravin preferintelor dietetice din profil
# Legaturi: profile.dietary_preferences (vegan/vegetarian/no_beef/etc.)
###########################################################################
def filter_by_dietary(df, pref):
    pool = df.copy()
    name = pool["name_core"].fillna("").str.lower()
    mg   = pool["main_group"].fillna("").str.lower()
    if pref.get("vegan"):
        pool = pool[~mg.str.contains("meat|fish", regex=True)]
        pool = pool[~mg.str.contains("egg|eggs|dairy", regex=True)]
    elif pref.get("vegetarian"):
        pool = pool[~mg.str.contains("meat|fish", regex=True)]
    if pref.get("no_beef"):
        pool = pool[~name.str.contains("beef|veal|bovine")]
    if pref.get("no_chicken"):
        pool = pool[~name.str.contains("chicken|poulet|pui|turkey")]
    if pref.get("no_fish"):
        pool = pool[~mg.str.contains("fish")]
    if pref.get("no_dairy"):
        pool = pool[~mg.str.contains("dairy|milk|cheese|yogurt|kefir")]
    if pref.get("gluten_free"):
        pool = pool[~mg.str.contains("bakery")]
        pool = pool[~name.str.contains("wheat|bread|pasta|barley|gluten")]
    return pool

###########################################################################
# clamp
# Ce face: limiteaza o valoare in interval [lo, hi]
###########################################################################
def clamp(x, lo, hi):
    return float(min(max(x, lo), hi))

###########################################################################
# macros_for
# Ce face: calculeaza macro-uri pentru o portie in grame (folosind coloanele canonice)
# Legaturi: dataset cu *_g_100g_ml si kcal_sanitized
###########################################################################
def macros_for(row, portion_g):
    f = float(portion_g)/100.0
    def num0(col):
        v = pd.to_numeric(row.get(col), errors="coerce")
        return float(v) if pd.notna(v) else 0.0
    def pick_first(cands):
        for c in cands:
            if c in row.index:
                v = pd.to_numeric(row.get(c), errors="coerce")
                if pd.notna(v):
                    return float(v)
        return 0.0

    carb_candidates = [
        "carbohydrate_g_100g_ml", "carbohydrates_g_100g_ml",
        "carbohydrate_g_100g", "carbohydrates_g_100g", "carbs_g_100g_ml"
    ]

    kcal   = num0("kcal_sanitized")      * f
    prot   = num0("protein_g_100g_ml")   * f
    carb   = pick_first(carb_candidates) * f  # fallback robust pe nume alternative
    fat    = num0("fat_g_100g_ml")       * f
    sugars = num0("sugars_g_100g_ml")    * f
    fibres = num0("fibres_g_100g_ml")    * f
    salt   = num0("salt_g_100g_ml")      * f
    return dict(kcal=kcal, protein=prot, carb=carb, fat=fat, sugars=sugars, fibres=fibres, salt=salt)

###########################################################################
# combo_hard_banned
# Ce face: interzice combinatii punctuale dupa tag/nume/perechi din rules
# Legaturi: rules.meal_hard_bans[meal_key] (disallow_tags, disallow_cols, disallow_pairs)
###########################################################################
def combo_hard_banned(meal_key, prot_row, carb_row, veg_row, rules):
    mhb = rules.get("meal_hard_bans", {}).get(meal_key, {})
    # disallow_tags (ex. dessert_like la snack)
    bad_tags = set(mhb.get("disallow_tags", []))
    def has_tag(row, tag):
        col = f"tag_{tag}"
        return int(row.get(col, 0)) == 1
    for tag in bad_tags:
        if has_tag(prot_row, tag) or has_tag(carb_row, tag) or (veg_row is not None and has_tag(veg_row, tag)):
            return True

    # disallow_cols (col==val)
    for col, val in mhb.get("disallow_cols", {}).items():
        for r in (prot_row, carb_row, veg_row):
            if r is not None and str(r.get(col, "")) == str(val):
                return True

    # disallow_pairs (ex. ["breakfast_cereal","milk"])
    pairs = mhb.get("disallow_pairs", [])
    name_p = str(prot_row.get("name_core","")).lower()
    name_c = str(carb_row.get("name_core","")).lower()
    name_v = str(veg_row.get("name_core","")).lower() if veg_row is not None else ""
    has_cereal = int(carb_row.get("tag_breakfast_cereal",0)) == 1 or "cereal" in name_c
    has_milk = ("milk" in name_p) or ("milk" in name_c) or ("yogurt" in name_p) or ("kefir" in name_p)
    for a,b in pairs:
        if (a=="breakfast_cereal" and b=="milk") and has_cereal and has_milk:
            return True
    return False
###########################################################################
# pick_meal
# Ce face: daca nu reuseste sablonul, alege combinatia (Ps + Sc [+ Ve]) cu scor minim
# Legaturi: build_pool, score_combo, macros_for, combo_hard_banned
###########################################################################
def pick_meal(df, meal_name, kcal_target, protein_min_g, blueprint,
              rules, used_buckets, rng, topk_pro, topk_carb, topk_veg, tax,
              used_carb_uids=None, used_protein_uids=None):

    mlow = meal_name.lower()
    meal_id = "snack" if mlow.startswith("snack") else {
        "breakfast": "breakfast",
        "lunch": "lunch",
        "dinner": "dinner"
    }.get(mlow.split()[0], "lunch")

    roles = blueprint.get("items", [])
    pr = {it["role"]: it.get("portion_g_range",[80,200]) for it in roles}
    r_pro = pr.get("protein", [120,300])
    r_carb = pr.get("side_carb", [100,250])
    r_veg = pr.get("side_veg", [0,150])

    # pool-uri pe rol (filtrate deja de build_pool pe allowed_meals, tag-uri de baza etc.)
    cand_pro = build_pool(df, meal_id, "protein", rules)
    if used_protein_uids:
        cand_pro = cand_pro[~cand_pro["uid"].isin(used_protein_uids)]

    cand_carb = build_pool(df, meal_id, "side_carb", rules)
    if used_carb_uids:
        cand_carb = cand_carb[~cand_carb["uid"].isin(used_carb_uids)]

    # 1) eliminam "ingredient-like"/snacks in garnituri
    bad_side_terms = r"\b(?:raw|flour|powder|mix|crisps|chips|oil|butter|margarine|lard)\b"
    mask_bad = cand_carb["name_core"].fillna("").str.contains(
        bad_side_terms, case=False, regex=True, na=False
    )
    cand_carb = cand_carb[~mask_bad]

    # 2) side_carb nu trebuie sa fie proteina (ex. carne pane)
    cand_carb = cand_carb[cand_carb["role_protein"] != 1]

    # 3) praguri "sanity" pentru carbo si grasimi (per 100g)
    cand_carb["carb100"] = pd.to_numeric(cand_carb.get("carbohydrate_g_100g_ml"), errors="coerce").fillna(0.0)
    cand_carb["fat100"] = pd.to_numeric(cand_carb.get("fat_g_100g_ml"), errors="coerce").fillna(0.0)
    cand_carb = cand_carb[cand_carb["carb100"] >= 8]  # minim carbo reali
    # grasimea la Sc este penalizata in scoring; aici nu taiem, doar lasam pragul carbo

    # 4) preferam "gatit" si, la pranz/cina, il cerem obligatoriu
    cand_carb["kcal100"] = pd.to_numeric(cand_carb["kcal_sanitized"], errors="coerce").fillna(0.0)
    pattern_cooked = r"\b(?:cooked|boiled|steamed|baked|roasted)\b"
    cand_carb["is_cooked"] = cand_carb["name_core"].fillna("").str.contains(
        pattern_cooked, case=False, regex=True, na=False
    ).astype(int)
    require_cooked = meal_id in {"lunch", "dinner"}
    if require_cooked:
        cand_carb = cand_carb[cand_carb["is_cooked"] == 1]

    # 5) sortare si esantionare limitata pentru diversitate controlata
    cand_carb = cand_carb.sort_values(["is_cooked", "kcal100"], ascending=[False, False]).head(40)
    cand_carb = cand_carb.sample(
        n=min(len(cand_carb), topk_carb),
        random_state=int(rng.integers(0, 2 ** 31 - 1))  # RNG controlat de seed
    )

    # 6) candidati legume (curatenie + limitare kcal)
    cand_veg = build_pool(df, meal_id, "side_veg", rules)
    ncore = cand_veg["name_core"].fillna("").str.lower()
    bad_veg_terms = r"\b(?:lasagna|cannelloni|tart|quiche|pie|pizza|sandwich|wrap|burger)\b"
    cand_veg = cand_veg[~ncore.str.contains(bad_veg_terms, regex=True, na=False)]
    cand_veg = cand_veg[cand_veg["veg_bucket"].fillna("").ne("")]
    cand_veg["kcal100"] = pd.to_numeric(cand_veg["kcal_sanitized"], errors="coerce").fillna(0.0)
    cand_veg = cand_veg[cand_veg["kcal100"] <= 120]
    if meal_id == "breakfast":
        ncore2 = cand_veg["name_core"].fillna("").str.lower()
        mask_soup = ncore2.str.contains(r"\bsoup\b", regex=True, na=False)
        cand_veg = cand_veg.loc[~mask_soup]

    # 7) preferam proteine "lean": proteine per 100 kcal ridicat
    cand_pro["prot100"] = pd.to_numeric(cand_pro["protein_g_100g_ml"], errors="coerce").fillna(0.0)
    cand_pro["kcal100"] = pd.to_numeric(cand_pro["kcal_sanitized"], errors="coerce").fillna(0.0)
    cand_pro["prot_per_100kcal"] = np.where(cand_pro["kcal100"] > 0,
                                            (cand_pro["prot100"] / cand_pro["kcal100"]) * 100.0, 0.0)
    cand_pro = cand_pro.sort_values(["prot_per_100kcal", "prot100"], ascending=False).head(40)
    cand_pro = cand_pro.sample(
        n=min(len(cand_pro), topk_pro),
        random_state=int(rng.integers(0, 2 ** 31 - 1))
    )

    # 8) limitam si veg-urile
    cand_veg = cand_veg.head(30)
    if not cand_veg.empty:
        cand_veg = cand_veg.sample(
            n=min(len(cand_veg), topk_veg),
            random_state=int(rng.integers(0, 2 ** 31 - 1))
        )

    if cand_pro.empty or cand_carb.empty:
        return None

    best = None
    for _, rp in cand_pro.iterrows():
        prot100 = float(pd.to_numeric(rp.get("protein_g_100g_ml"), errors="coerce") or 0.0)
        kcal100_p = float(pd.to_numeric(rp.get("kcal_sanitized"), errors="coerce") or 0.0)
        # portie proteina ca sa atinga pragul minim; limitata la [r_pro]
        portion_p = r_pro[0] if prot100<=0 else protein_min_g / prot100 * 100.0
        portion_p = clamp(portion_p, r_pro[0], r_pro[1])
        mac_p = macros_for(rp, portion_p)

        # kcal ramase pentru garnitura
        rem_kcal = max(0.0, kcal_target - mac_p["kcal"])

        for _, rc in cand_carb.iterrows():
            kcal100_c = float(pd.to_numeric(rc.get("kcal_sanitized"), errors="coerce") or 0.0)
            if kcal100_c <= 0:
                continue
            # portie carb acopera kcal ramase; limitata la [r_carb]
            portion_c = (rem_kcal / kcal100_c) * 100.0 if rem_kcal > 0 else r_carb[0]
            portion_c = clamp(portion_c, r_carb[0], r_carb[1])
            mac_c = macros_for(rc, portion_c)

            # optional legume, daca nu depasim prea tare tinta
            rv = None
            portion_v = 0.0
            mac_v = {"kcal":0,"protein":0,"carb":0,"fat":0,"sugars":0,"fibres":0,"salt":0}
            if not cand_veg.empty and r_veg[1] > 0:
                for _, rv_try in cand_veg.iterrows():
                    portion_try = clamp((r_veg[0]+r_veg[1])/2.0, r_veg[0], r_veg[1])
                    mac_try = macros_for(rv_try, portion_try)
                    if (mac_p["kcal"] + mac_c["kcal"] + mac_try["kcal"]) <= (kcal_target + 200):
                        rv = rv_try; portion_v = portion_try; mac_v = mac_try
                        break

            totals = {
                "kcal": mac_p["kcal"] + mac_c["kcal"] + mac_v["kcal"],
                "protein": mac_p["protein"] + mac_c["protein"] + mac_v["protein"],
                "carb": mac_p["carb"] + mac_c["carb"] + mac_v["carb"],
                "fat": mac_p["fat"] + mac_c["fat"] + mac_v["fat"],
                "sugars": mac_p["sugars"] + mac_c["sugars"] + mac_v["sugars"],
                "fibres": mac_p["fibres"] + mac_c["fibres"] + mac_v["fibres"],
                "salt": mac_p["salt"] + mac_c["salt"] + mac_v["salt"],
                "kcal_target": kcal_target,
                "protein_min_g": protein_min_g
            }

            if combo_hard_banned(meal_id, rp, rc, rv, rules):
                continue

            score, reasons = score_combo(meal_id, totals, rp, rc, rv, rules, used_buckets, tax=tax)
            row = {
                "meal": meal_name,
                "carb_bucket": rc.get("carb_bucket", ""),
                "protein_name": rp.get("name_core",""),
                "protein_uid": rp.get("uid",""),
                "protein_portion_g": round(portion_p,1),
                "side_carb_name": rc.get("name_core",""),
                "side_carb_uid": rc.get("uid",""),
                "side_carb_portion_g": round(portion_c,1),
                "side_veg_name": (rv.get("name_core","") if rv is not None else ""),
                "side_veg_uid": (rv.get("uid","") if rv is not None else ""),
                "side_veg_portion_g": (round(portion_v,1) if rv is not None else 0.0),
                "kcal_meal": round(totals["kcal"],1),
                "protein_meal_g": round(totals["protein"],1),
                "carb_meal_g": round(totals["carb"],1),
                "fat_meal_g": round(totals["fat"],1),
                "sugars_meal_g": round(totals["sugars"],1),
                "fibres_meal_g": round(totals["fibres"],1),
                "salt_meal_g": round(totals["salt"],2),
                "score": float(score),
                "reasons": reasons,
                "protein_bucket": rp.get("protein_bucket",""),
                "protein_portion_range": tuple(r_pro),
                "side_carb_portion_range": tuple(r_carb),
                "side_veg_portion_range": tuple(r_veg),
            }
            if (best is None) or (score < best["score"]):
                best = row

    return best
###########################################################################
# generate
# Ce face: pipeline principal (citiri, filtre, selectie pe mese, daily rules, scriere)
# Legaturi: toate functiile de mai sus + load_subs/alt_candidates + write_readable
###########################################################################
def generate(foods_path, rules_path, profile_path, out_csv, out_txt,
             seed=42, topk_pro=3, topk_carb=3, topk_veg=3,
             tax=None, templates=None, subs_path=None,
             days=1, temperature=0.7, no_repeat_window=1):

    # RNG consistent (numpy)
    rng = np.random.default_rng(seed)

    # incarcare date
    df0 = read_foods(foods_path)
    rules = read_rules(rules_path)
    profile = json.loads(Path(profile_path).read_text(encoding="utf-8"))

    # filtre globale + dietetice + sanity kcal
    df = apply_global_bans(df0, rules)
    df = filter_by_dietary(df, profile.get("dietary_preferences", {}))
    df = df[pd.to_numeric(df.get("kcal_sanitized"), errors="coerce").fillna(0) > 0]

    # substitutii similare
    subs_map = load_subs(subs_path) if subs_path else {}
    foods_by_uid = df.set_index("uid", drop=False)  # acces rapid la rand dupa uid

    # tinte zilnice
    tdee = tdee_from_profile(profile)
    cals = adjust_for_goal(tdee, profile.get("goal","maintain"), profile.get("progress_speed","moderate"))
    prot_daily = protein_target(profile["weight_kg"])
    names, split = meal_split(profile.get("meals_per_day",3), profile.get("include_snacks",False))
    kcal_targets = [cals*s for s in split]
    pmin_default = min(35.0, max(20.0, prot_daily / len(names)))
    pmin_cfg = rules.get("per_meal_protein_min_g", {})

    def pmin_for(meal_key: str) -> float:
        return float(pmin_cfg.get(meal_key, pmin_default))

    meal_blue = rules.get("meal_blueprints", {})
    used_buckets = set()   # pentru variety (doar pe zi curenta)
    used_protein_uids = set()
    used_carb_uids = set()
    rows = []

    # o singura zi (days>1 ar presupune resetarea/rotirea starilor pe zi)
    for name, kcal_t in zip(names, kcal_targets):
        key = "snack" if name.lower().startswith("snack") else \
              {"breakfast": "breakfast", "lunch": "lunch", "dinner": "dinner"}.get(name.lower().split()[0], "lunch")

        protein_min = pmin_for(key)
        blueprint = meal_blue.get(
            "breakfast" if key == "breakfast" else "default",
            {"items": [
                {"role": "protein", "portion_g_range": [120, 300]},
                {"role": "side_carb", "portion_g_range": [100, 250]}
            ]}
        )

        pick = None

        # 1) incearca sabloanele (templates)
        if templates:
            for tpl in templates.get(key, []):
                pick = pick_meal_from_template(df, name, kcal_t, protein_min, tpl, rules, used_buckets, tax)
                if pick:
                    break

        # 2) fallback la algoritmul generic
        if pick is None:
            pick = pick_meal(
                df, name, kcal_t, protein_min, blueprint, rules,
                used_buckets, rng, topk_pro, topk_carb, topk_veg, tax,
                used_carb_uids=used_carb_uids, used_protein_uids=used_protein_uids
            )
            if pick is not None and "template_used" not in pick:
                pick["template_used"] = ""

        # 3) propuneri de "swap similar" (daca exista matrice)
        meal_id = key
        if pick:
            pu = pick.get("protein_uid")
            if pu and subs_map:
                for i, (auid, aname, asim) in enumerate(
                        alt_candidates(pu, "role_protein", meal_id, subs_map, foods_by_uid, k=3), 1):
                    pick[f"alt_protein_{i}_uid"] = auid
                    pick[f"alt_protein_{i}_name"] = aname
                    pick[f"alt_protein_{i}_sim"] = round(asim, 3)

            cu = pick.get("side_carb_uid")
            if cu and subs_map:
                for i, (auid, aname, asim) in enumerate(
                        alt_candidates(cu, "role_side_carb", meal_id, subs_map, foods_by_uid, k=3), 1):
                    pick[f"alt_carb_{i}_uid"] = auid
                    pick[f"alt_carb_{i}_name"] = aname
                    pick[f"alt_carb_{i}_sim"] = round(asim, 3)

            vu = pick.get("side_veg_uid")
            if vu:
                alts_v = alt_candidates(vu, "role_side_veg", meal_id, subs_map, foods_by_uid, k=3)
                for i, (auid, aname, asim) in enumerate(alts_v, 1):
                    pick[f"alt_veg_{i}_uid"] = auid
                    pick[f"alt_veg_{i}_name"] = aname
                    pick[f"alt_veg_{i}_sim"] = round(asim, 3)

        # 4) append si actualizare "variety" pe zi
        if pick is None:
            rows.append({"meal": name, "error": "no_candidate", "template_used": ""})
        else:
            rows.append(pick)
            pu = pick.get("protein_uid")
            cu = pick.get("side_carb_uid")
            if pu: used_protein_uids.add(pu)
            if cu: used_carb_uids.add(cu)
            pb = pick.get("protein_bucket", "")
            cb = pick.get("carb_bucket", "")
            if pb: used_buckets.add(pb)
            if cb: used_buckets.add(cb)

    # scriere CSV principal
    out_dir = Path(out_csv).parent; out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_csv, index=False)

    # ===== DAILY-AWARE RULES =====
    def _meal_key_from_name(n):
        m = str(n).lower()
        if m.startswith("snack"): return "snack"
        return {"breakfast": "breakfast", "lunch": "lunch", "dinner": "dinner"}.get(m.split()[0], "lunch")

    day_meals = []
    for r in rows:
        if "error" in r:  # sarim peste mesele nereusite
            continue
        meal_id = _meal_key_from_name(r["meal"])
        items = []

        def _add_item(role, uid_key, name_key, portion_key, rng_key):
            uid = r.get(uid_key)
            if not uid:
                return
            name = r.get(name_key, "")
            portion_g = float(r.get(portion_key, 0.0) or 0.0)
            pr = r.get(rng_key, None)
            nutr = {}
            try:
                rowx = foods_by_uid.loc[uid]
                carb100 = float(pd.to_numeric(rowx.get("carbohydrate_g_100g_ml"), errors="coerce") or 0.0)
                fibre100 = float(pd.to_numeric(rowx.get("fibres_g_100g_ml"), errors="coerce") or 0.0)
                nutr = {"carb_g_per_100g": carb100, "fibre_g_per_100g": fibre100}
            except Exception:
                nutr = {"carb_g_per_100g": 0.0, "fibre_g_per_100g": 0.0}
            items.append({
                "role": role,
                "uid": uid,
                "name": name,
                "portion_g": portion_g,
                "portion_range": tuple(pr) if (isinstance(pr, (list, tuple)) and len(pr) == 2) else (portion_g, portion_g),
                "nutr": nutr
            })

        _add_item("protein", "protein_uid", "protein_name", "protein_portion_g", "protein_portion_range")
        _add_item("side_carb", "side_carb_uid", "side_carb_name", "side_carb_portion_g", "side_carb_portion_range")
        _add_item("side_veg", "side_veg_uid", "side_veg_name", "side_veg_portion_g", "side_veg_portion_range")

        day_meals.append({"meal_id": meal_id, "items": items})

    portion_ranges = {}
    for M in day_meals:
        mid = M["meal_id"]
        for it in M["items"]:
            portion_ranges[(mid, it["role"])] = it.get("portion_range", (it["portion_g"], it["portion_g"]))

    try:
        info_daily = adjust_day_portions_in_place(day_meals, portion_ranges, max_adjustments=2)
        mb = info_daily.get("metrics_before", {})
        ma = info_daily.get("metrics_after", {})
        print(f"[DAILY] carb_BL: {mb.get('carb_BL_frac', 0):.2f} -> {ma.get('carb_BL_frac', 0):.2f} | "
              f"veg_servings: {mb.get('veg_servings', 0):.2f} -> {ma.get('veg_servings', 0):.2f} | "
              f"adj={len(info_daily.get('adjustments', []))}")
    except Exception as e:
        print("[DAILY] adjust skipped:", e)

    # scriem portiile ajustate inapoi in rows si refacem macro-urile
    new_portions = {}
    for M in day_meals:
        mid = M["meal_id"]
        for it in M["items"]:
            new_portions[(mid, it["role"], it["uid"])] = float(it.get("portion_g", 0.0) or 0.0)

    def _recalc_macros_for_meal(r):
        prot_row = foods_by_uid.loc[r["protein_uid"]] if r.get("protein_uid") else None
        carb_row = foods_by_uid.loc[r["side_carb_uid"]] if r.get("side_carb_uid") else None
        veg_row = foods_by_uid.loc[r["side_veg_uid"]] if r.get("side_veg_uid") else None

        mid = _meal_key_from_name(r["meal"])
        pp = new_portions.get((mid, "protein", r.get("protein_uid", "")), r.get("protein_portion_g", 0.0))
        cp = new_portions.get((mid, "side_carb", r.get("side_carb_uid", "")), r.get("side_carb_portion_g", 0.0))
        vp = new_portions.get((mid, "side_veg", r.get("side_veg_uid", "")), r.get("side_veg_portion_g", 0.0))

        r["protein_portion_g"] = round(float(pp or 0.0), 1)
        r["side_carb_portion_g"] = round(float(cp or 0.0), 1)
        r["side_veg_portion_g"] = round(float(vp or 0.0), 1)

        mac_p = macros_for(prot_row, pp) if prot_row is not None else {"kcal":0,"protein":0,"carb":0,"fat":0,"sugars":0,"fibres":0,"salt":0}
        mac_c = macros_for(carb_row, cp) if carb_row is not None else {"kcal":0,"protein":0,"carb":0,"fat":0,"sugars":0,"fibres":0,"salt":0}
        mac_v = macros_for(veg_row, vp) if veg_row is not None else {"kcal":0,"protein":0,"carb":0,"fat":0,"sugars":0,"fibres":0,"salt":0}

        r["kcal_meal"] = round(mac_p["kcal"] + mac_c["kcal"] + mac_v["kcal"], 1)
        r["protein_meal_g"] = round(mac_p["protein"] + mac_c["protein"] + mac_v["protein"], 1)
        r["carb_meal_g"] = round(mac_p["carb"] + mac_c["carb"] + mac_v["carb"], 1)
        r["fat_meal_g"] = round(mac_p["fat"] + mac_c["fat"] + mac_v["fat"], 1)
        r["sugars_meal_g"] = round(mac_p["sugars"] + mac_c["sugars"] + mac_v["sugars"], 1)
        r["fibres_meal_g"] = round(mac_p["fibres"] + mac_c["fibres"] + mac_v["fibres"], 1)
        r["salt_meal_g"] = round(mac_p["salt"] + mac_c["salt"] + mac_v["salt"], 2)
        return r

    rows = [_recalc_macros_for_meal(r) if "error" not in r else r for r in rows]

    # sumar TXT (v2)
    dfp = pd.DataFrame(rows)
    totals = {}
    for k in ["kcal_meal","protein_meal_g","carb_meal_g","fat_meal_g","sugars_meal_g","fibres_meal_g","salt_meal_g"]:
        totals[k] = float(pd.to_numeric(dfp[k], errors="coerce").sum()) if k in dfp.columns else 0.0

    lines = [
        f"TDEE_estimated: {tdee:.1f} kcal",
        f"Calories_target: {cals:.1f} kcal",
        f"Protein_target: {prot_daily} g",
        "---- DAILY TOTALS (v2) ----",
        *(f"{k}: {v:.1f}" for k,v in totals.items())
    ]
    Path(out_txt).write_text("\n".join(lines), encoding="utf-8")

    # readable (apeleaza analyze_plan.py)
    try:
        write_readable(out_csv, foods_path, profile_path, Path(out_txt).with_name(
            Path(out_txt).name.replace("_summary", "_readable")).as_posix()
        )
    except Exception:
        pass

    print("OK (plan_v2)")
###########################################################################
# load_subs
# Ce face: incarca substitutii similare din CSV (src_uid,dst_uid,sim)
# Legaturi: data/substitution_edges.csv.gz
###########################################################################
def load_subs(path):
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        return {}
    import pandas as pd
    df = pd.read_csv(p)
    sub = {}
    for r in df.itertuples(index=False):
        sub.setdefault(r.src_uid, []).append((r.dst_uid, float(r.sim)))
    for k in sub:
        sub[k].sort(key=lambda t: -t[1])  # desc dupa similaritate
    return sub

###########################################################################
# alt_candidates
# Ce face: returneaza top-k alternative valide (acelasi rol, allowed_meals, existente)
# Legaturi: subs_map + foods_by_uid
###########################################################################
def alt_candidates(uid, role_col, meal_id, subs_map, foods_by_uid, k=3):
    alts, seen_uid, seen_name = [], set(), set()
    for dst, sim in subs_map.get(uid, []):
        if dst == uid or dst in seen_uid:
            continue
        if dst not in foods_by_uid.index:
            continue
        row = foods_by_uid.loc[dst]
        if int(row.get(role_col, 0)) != 1:
            continue
        if not allowed_for_meal(row, meal_id):
            continue
        name = str(row.get("name_core","")).strip().lower()
        if name in seen_name:
            continue
        alts.append((dst, row.get("name_core",""), float(sim)))
        seen_uid.add(dst)
        seen_name.add(name)
        if len(alts) >= k:
            break
    return alts

###########################################################################
# write_readable
# Ce face: ruleaza analyze_plan.py ca proces separat si scrie TXT "readable"
# Legaturi: src/analyze_plan.py
###########################################################################
def write_readable(plan_csv: str, foods_path: str, profile_path: str, out_path: str,
                   foods_fallback: str = "data/foods_enriched_min.csv.gz"):
    here = Path(__file__).parent
    script = here / "analyze_plan.py"
    if not script.exists():
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text("[readable] analyze_plan.py lipsa", encoding="utf-8")
        return

    cmd = [
        sys.executable, str(script),
        "--plan", plan_csv,
        "--foods", foods_path,
        "--profile", profile_path,
        "--out", out_path,
        "--foods_fallback", foods_fallback,
    ]
    try:
        subprocess.run(cmd, check=False)
    except Exception as e:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(f"[readable] eroare: {e}", encoding="utf-8")

###########################################################################
# main (CLI)
# Ce face: parseaza argumentele si invoca generate()
# Legaturi: toate optiunile mentionate in header
###########################################################################
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subs", default="data/substitution_edges.csv.gz")
    ap.add_argument("--templates", default="templates/meal_templates.yaml")
    ap.add_argument("--taxonomy", default="configs/taxonomy.yaml")
    ap.add_argument("--foods", default="data/foods_enriched.parquet")
    ap.add_argument("--rules", default="configs/culinary_rules.yaml")
    ap.add_argument("--profile", default="profiles/user_profile_sample.json")
    ap.add_argument("--out_csv", default="outputs/plan_v2.csv")
    ap.add_argument("--out_txt", default="outputs/plan_v2_summary.txt")
    ap.add_argument("--out_readable", default="outputs/plan_v2_readable.txt")

    # diversitate si reproducibilitate
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--temperature", type=float, default=0.7)
    ap.add_argument("--no_repeat_window", type=int, default=1)

    # limitare candidate per rol (sampling din topuri)
    ap.add_argument("--topk_pro", type=int, default=3)
    ap.add_argument("--topk_carb", type=int, default=3)
    ap.add_argument("--topk_veg", type=int, default=3)

    args = ap.parse_args()

    warn_if_rules_have_slots(args.rules)
    tax = load_taxonomy(args.taxonomy)
    templates = read_templates(args.templates)

    np.random.seed(args.seed)  # pentru alte pachete care consulta rng global

    generate(args.foods, args.rules, args.profile, args.out_csv, args.out_txt,
             seed=args.seed, topk_pro=args.topk_pro, topk_carb=args.topk_carb,
             topk_veg=args.topk_veg, tax=tax, templates=templates, subs_path=args.subs)

if __name__ == "__main__":
    main()
