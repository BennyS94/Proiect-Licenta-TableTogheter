###########################################################################
# FISIER: src/core/scoring.py
#
# SCOP
#   Functii de scor pentru o masa (combo Ps + Sc + Ve). Scorul mai mic este
#   mai bun. Combina tinta kcal/proteina pe masa, limite soft (sare/zahar),
#   priors pe rol si compatibilitati Ps<->Sc, penalizari/bonusuri pe tag-uri,
#   varietate, distanta taxonomica, reguli specifice snack si (la final)
#   un delta mic din preferintele utilizatorului (feedback agregat).
#
# CUM SE APELEAZA (indirect din generator_v2.py)
#   score, reasons = score_combo(meal_key, totals, rp, rc, rv, rules, used_buckets, tax, prefs)
#   - meal_key: "breakfast" / "lunch" / "dinner" / "snack"
#   - totals: dict cu kcal/protein/carb/fat/sugars/fibres/salt + tinte
#   - rp, rc, rv: randuri (Series/dict-like) pentru protein/side_carb/side_veg
#   - rules: reguli din YAML (scoring_weights, priors, per_meal_macros, etc.)
#   - used_buckets: set cu bucket-uri folosite in ziua curenta (pt varietate)
#   - tax: structura taxonomica (optional; pt tree_distance)
#   - prefs: dict preferinte (user_prefs.json) — influenta mica, la final
#
# LEGATURI
#   - src/ontology.py: node_for_food(), tree_distance() pentru fallback si cost
#   - configs/culinary_rules.yaml: chei scor, priors, tag penalties/bonuses
#   - outputs/user_prefs.json: preferinte agregate (tools/ingest_feedback.py)
#
# NOTE
#   - Comentarii in romana fara diacritice, pe linii critice am pus explicatii
#     inline (la dreapta).
#   - Pastreaza greutatile din YAML calibrate astfel incat preferintele
#     utilizatorilor sa nu inunde regulile de baza.
###########################################################################

from __future__ import annotations
from typing import Optional, Set, Tuple, Dict
import pandas as pd

# Import robust pentru modulul local src/ontology.py
try:
    # cand rulezi ca pachet:  python -m src.generator_v2
    from ..ontology import node_for_food, tree_distance
except Exception:
    try:
        # cand rulezi ca script: python src/generator_v2.py
        from src.ontology import node_for_food, tree_distance
    except Exception:
        try:
            # fallback: daca radacina proiectului e deja in sys.path
            from ontology import node_for_food, tree_distance
        except Exception:
            node_for_food = None
            tree_distance = None


###########################################################################
# mici utilitare interne (numeric sigur, chei pentru priors)
###########################################################################

def _num(row, col) -> float:
    """ Extrage col numeric in float (0.0 daca lipseste/NaN). """
    v = pd.to_numeric(row.get(col), errors="coerce")
    return float(v) if pd.notna(v) else 0.0

def _leaf_from_node(node: Optional[str]) -> str:
    """ Intoarce frunza (ultimul segment) dintr-un id taxonomic. """
    return str(node).split("/")[-1] if node else ""

def _priors_key(row, role_key: str, tax_node: Optional[str]) -> str:
    """
    Cheia pentru lookup in priors:
      1) foloseste bucket explicit daca exista (ex. 'protein_bucket')
      2) altfel frunza din taxonomie (fallback robust)
    """
    b = str(row.get(role_key, "")).strip()
    return b if b else _leaf_from_node(tax_node)


###########################################################################
# preferinte utilizator (feedback agregat) — transformate in delta mic de scor
###########################################################################

def _pref_weights_from_rules(rules):
    """
    Citeste ponderi pentru preferinte din rules['user_preference_weights'].
    Magnitudini mici implicit, pentru a nu inunda regulile de baza.
    Semn negativ imbunatateste scorul (scade).
    """
    wp = (rules or {}).get("user_preference_weights", {})
    return {
        "tag_like":       float(wp.get("tag_like",      -0.15)),
        "tag_dislike":    float(wp.get("tag_dislike",     0.25)),
        "name_like":      float(wp.get("name_like",     -0.10)),
        "name_dislike":   float(wp.get("name_dislike",    0.20)),
        "bucket_like":    float(wp.get("bucket_like",   -0.12)),
        "bucket_dislike": float(wp.get("bucket_dislike",  0.18)),
        "item_upvote":    float(wp.get("item_upvote",   -0.25)),
        "item_downvote":  float(wp.get("item_downvote",   0.35)),
        "global_scale":   float(wp.get("global_scale",    1.0)),
    }

def _present_tags(row):
    """
    Intoarce setul de tag-uri prezente (coloane care incep cu 'tag_' si au 1).
    Accepta Series sau mapping dict-like.
    """
    if row is None:
        return set()
    tags = set()
    keys = (row.index if hasattr(row, "index") else row.keys())
    for c in keys:
        sc = str(c)
        if sc.startswith("tag_"):
            try:
                v = row.get(c, 0) if hasattr(row, "get") else row[c]
                if int(float(v or 0)) == 1:
                    tags.add(sc)
            except Exception:
                continue
    return tags

def _str_lower(x):
    """ Safeguard pentru normalizare string la lowercase. """
    try:
        return str(x or "").lower()
    except Exception:
        return ""

def _apply_user_prefs_delta(rp, rc, rv, meal_id, rules, prefs):
    """
    Calculeaza un mic delta de scor pe baza preferintelor utilizatorului.
    Intoarce (delta, reasons_str). Efectul este mic si aplicat la final.
    """
    if not prefs:
        return 0.0, ""

    w = _pref_weights_from_rules(rules)
    scale = w.get("global_scale", 1.0)

    # normalizare containere
    tags_like      = set(prefs.get("liked_tags", []) or [])
    tags_dislike   = set(prefs.get("disliked_tags", []) or [])
    names_like     = set(_str_lower(s) for s in (prefs.get("liked_names", []) or []))
    names_dislike  = set(_str_lower(s) for s in (prefs.get("disliked_names", []) or []))
    pb_like        = set(_str_lower(s) for s in (prefs.get("liked_protein_buckets", []) or []))
    pb_dislike     = set(_str_lower(s) for s in (prefs.get("disliked_protein_buckets", []) or []))
    cb_like        = set(_str_lower(s) for s in (prefs.get("liked_carb_buckets", []) or []))
    cb_dislike     = set(_str_lower(s) for s in (prefs.get("disliked_carb_buckets", []) or []))
    vb_like        = set(_str_lower(s) for s in (prefs.get("liked_veg_buckets", []) or []))
    vb_dislike     = set(_str_lower(s) for s in (prefs.get("disliked_veg_buckets", []) or []))
    item_votes     = prefs.get("item_votes", {}) or {}   # {uid: +n / -n}

    delta = 0.0
    reasons = []

    roles = [
        ("protein",   rp, "protein_bucket"),
        ("side_carb", rc, "carb_bucket"),
        ("side_veg",  rv, "veg_bucket"),
    ]
    for role, row, bucket_col in roles:
        if row is None:
            continue

        name = _str_lower(row.get("name_core", "") if hasattr(row, "get") else row["name_core"])
        uid  = _str_lower(row.get("uid", "")       if hasattr(row, "get") else row["uid"])
        bval = _str_lower(row.get(bucket_col, "")  if hasattr(row, "get") else row[bucket_col])
        ptags = _present_tags(row)

        # name like/dislike (substring simplu; eficient dar posibil zgomotos)
        if name:
            for n in names_like:
                if n and n in name:
                    delta += w["name_like"] * scale
                    reasons.append(f"+name:{n}")
            for n in names_dislike:
                if n and n in name:
                    delta += w["name_dislike"] * scale
                    reasons.append(f"-name:{n}")

        # tag like/dislike
        for t in tags_like:
            tcol = t if t.startswith("tag_") else f"tag_{t}"
            if tcol in ptags:
                delta += w["tag_like"] * scale
                reasons.append(f"+tag:{tcol}")
        for t in tags_dislike:
            tcol = t if t.startswith("tag_") else f"tag_{t}"
            if tcol in ptags:
                delta += w["tag_dislike"] * scale
                reasons.append(f"-tag:{tcol}")

        # bucket like/dislike per rol
        if role == "protein":
            if bval in pb_like:
                delta += w["bucket_like"] * scale; reasons.append(f"+pb:{bval}")
            if bval in pb_dislike:
                delta += w["bucket_dislike"] * scale; reasons.append(f"-pb:{bval}")
        elif role == "side_carb":
            if bval in cb_like:
                delta += w["bucket_like"] * scale; reasons.append(f"+cb:{bval}")
            if bval in cb_dislike:
                delta += w["bucket_dislike"] * scale; reasons.append(f"-cb:{bval}")
        elif role == "side_veg":
            if bval in vb_like:
                delta += w["bucket_like"] * scale; reasons.append(f"+vb:{bval}")
            if bval in vb_dislike:
                delta += w["bucket_dislike"] * scale; reasons.append(f"-vb:{bval}")

        # voturi explicite pe item (uid)
        if uid and uid in item_votes:
            try:
                v = float(item_votes[uid])
                if v > 0:
                    delta += w["item_upvote"] * v * scale; reasons.append(f"+uid:{uid}*{int(v)}")
                elif v < 0:
                    delta += w["item_downvote"] * abs(v) * scale; reasons.append(f"-uid:{uid}*{int(abs(v))}")
            except Exception:
                pass

    return float(delta), ("prefs:" + ",".join(reasons) if reasons else "")


###########################################################################
# SCORING — functia principala (lower = better)
###########################################################################

def score_combo(
    meal_key: str,
    totals: Dict[str, float],
    prot_row, carb_row, veg_row,
    rules: Dict,
    used_buckets: Optional[Set[str]] = None,
    tax=None,
    prefs=None,
) -> Tuple[float, str]:
    """
    Calculeaza scorul pentru o combinatie (Ps, Sc, Ve) intr-o masa.
    Intoarce (score, reasons_str). Scor mai mic este mai bun.
    """
    # greutati/limite/priori din YAML
    W = rules.get("scoring_weights", {})
    S = rules.get("soft_limits", {})
    Pri = rules.get("priors", {})

    # greutati principale (tine-le la magnitudini rezonabile)
    w_kcal = float(W.get("w_kcal_abs", 1.0))
    w_pgap = float(W.get("w_protein_gap", 0.5))
    w_pen  = float(W.get("w_rule_penalty", 1.0))
    w_var  = float(W.get("w_variety_bonus", 0.2))
    w_carb = float(W.get("w_carb_floor", 0.9))
    w_fat  = float(W.get("w_fat_ceiling", 1.2))
    w_prior= float(W.get("w_prior", 0.8))
    w_pair = float(W.get("w_pair", 0.8))
    w_ing  = float(W.get("w_ingredientness", 0.6))
    w_tree = float(W.get("w_tree_dist", 0.4))
    w_sc_fat100 = float(W.get("w_sc_fat100", 0.0))

    kcal_target = float(totals["kcal_target"])
    pmin        = float(totals["protein_min_g"])

    score = 0.0
    reasons = []

    # 1) tinta kcal si gap proteina pe masa
    score += w_kcal * abs(float(totals["kcal"]) - kcal_target)
    score += w_pgap * max(0.0, pmin - float(totals["protein"]))

    # 1b) praguri per-masa (carb min, fat max)
    PM = rules.get("per_meal_macros", {}).get(meal_key, {})
    carb_min = float(PM.get("carb_g_min", 0.0))
    fat_max  = float(PM.get("fat_g_max", 1e9))
    if carb_min > 0 and float(totals["carb"]) < carb_min:
        score += w_carb * (carb_min - float(totals["carb"]))
        reasons.append(f"carb<{carb_min}g")
    if float(totals["fat"]) > fat_max:
        score += w_fat * (float(totals["fat"]) - fat_max)
        reasons.append(f"fat>{fat_max}g")

    # 2) limite soft: sare si zahar (zahar penal numai pe mesele specificate)
    salt_max = float(S.get("salt_g_max_per_meal", 2.0))
    if float(totals["salt"]) > salt_max:
        score += w_pen * (float(totals["salt"]) - salt_max)
        reasons.append(f"salt>{salt_max}g")

    sugars_penalty_meals = {m.lower() for m in S.get("sugars_high_penalty_meals", [])}
    if meal_key in sugars_penalty_meals:
        if float(totals["sugars"]) > 25:
            score += w_pen * 0.5 * ((float(totals["sugars"]) - 25) / 5.0)  # mic slope
            reasons.append("high_sugars")
        is_sweet_any = (int(prot_row.get("is_sweet",0))==1 or
                        int(carb_row.get("is_sweet",0))==1 or
                        (veg_row is not None and int(veg_row.get("is_sweet",0))==1))
        if is_sweet_any:
            score += w_pen * 0.5
            reasons.append("sweet_item")

    # 3) priors + compatibilitate Ps<->Sc (cu fallback pe taxonomie)
    node_p = node_for_food(prot_row, "protein",  tax) if (tax is not None and node_for_food is not None) else None
    node_c = node_for_food(carb_row, "side_carb", tax) if (tax is not None and node_for_food is not None) else None

    meal_pr = Pri.get("meal_role_priors", {}).get(meal_key, {})
    pkey = _priors_key(prot_row, "protein_bucket", node_p)   # ex. 'hard_cheese' daca lipseste bucket
    ckey = _priors_key(carb_row, "carb_bucket",  node_c)

    prior_p = float(meal_pr.get("protein_bucket", {}).get(pkey, 0.5))
    prior_c = float(meal_pr.get("carb_bucket",  {}).get(ckey, 0.5))
    score += w_prior * ((1.0 - prior_p) + (1.0 - prior_c))
    if prior_p < 0.5: reasons.append(f"low_prior_pro:{pkey}")
    if prior_c < 0.5: reasons.append(f"low_prior_carb:{ckey}")

    compat_table = Pri.get("pairwise_compat", {}).get("protein_bucket", {})
    compat = float(compat_table.get(pkey, {}).get(ckey, Pri.get("pairwise_compat", {}).get("default", 0.5)))
    score += w_pair * (1.0 - compat)
    if compat < 0.5: reasons.append(f"low_pair:{pkey}+{ckey}={compat:.2f}")

    # 4) ingredientness — descurajam pulberi, mixuri, pre-fried
    def ingr_cost(row):
        txt = (str(row.get("name_core","")) + " " + str(row.get("main_group",""))).lower()
        bad_terms = ["powder","dehydrated","mix","pre-fried"]  # ideal: muta in YAML
        return w_ing * sum(1 for t in bad_terms if t in txt)
    score += ingr_cost(prot_row)
    score += ingr_cost(carb_row)
    if veg_row is not None:
        score += ingr_cost(veg_row)

    # 5) garda pe grasime/100g la side_carb (optional via w_sc_fat100)
    sc_fat_max100 = float(S.get("side_carb_fat_g_per100g_max", 1e9))
    fat100_c = _num(carb_row, "fat_g_100g_ml")
    if w_sc_fat100 > 0 and fat100_c > sc_fat_max100:
        score += w_sc_fat100 * (fat100_c - sc_fat_max100)
        reasons.append("side_carb_fat/100 high")

    # 6) varietate pe zi (bonus mic daca bucket-ul nu a fost folosit)
    used_buckets = used_buckets or set()
    pbucket = str(prot_row.get("protein_bucket", "")).strip()
    if pbucket and f"P|{pbucket}" not in used_buckets:
        score -= w_var * 0.2
        reasons.append(f"variety_pro:+{pbucket}")
    cbucket = str(carb_row.get("carb_bucket", "")).strip()
    if cbucket and f"C|{cbucket}" not in used_buckets:
        score -= w_var * 0.1
        reasons.append(f"variety_carb:+{cbucket}")

    # 7) cost taxonomic (daca taxonomie si distanta sunt disponibile)
    if tax is not None and tree_distance is not None:
        if node_p and node_c:
            try:
                dist = tree_distance(tax, node_p, node_c)
            except Exception:
                dist = 0
            score += w_tree * float(dist)
            reasons.append(f"tree_dist:{dist}")

    # --- per-meal macros (al doilea bloc pentru compatibilitate istorica) ---
    PM2 = rules.get("per_meal_macros", {})
    mm = PM2.get(meal_key, PM2.get("default", {}))

    W2 = rules.get("scoring_weights", {})
    w_carb2 = float(W2.get("w_carb_floor", 0.7))
    w_fat2  = float(W2.get("w_fat_ceiling", 0.7))
    w_kcal2 = float(W2.get("w_kcal_abs", 1.0))
    w_prot2 = float(W2.get("w_protein_gap", 0.5))
    w_rule2 = float(W2.get("w_rule_penalty", 1.0))

    carb_min2  = float(mm.get("carb_g_min", 0))
    fat_max2   = float(mm.get("fat_g_max", float('inf')))
    fibre_min2 = float(mm.get("fibre_g_min", 0))
    kcal_max2  = mm.get("kcal_max", None)
    prot_min2  = mm.get("protein_g_min", None)

    if totals.get("carb", 0) < carb_min2:
        score += w_carb2 * (carb_min2 - totals["carb"]); reasons.append(f"carb<{carb_min2}g")
    if totals.get("fat", 0) > fat_max2:
        score += w_fat2 * (totals["fat"] - fat_max2); reasons.append(f"fat>{fat_max2}g")
    if fibre_min2 and totals.get("fibres", 0) < fibre_min2:
        score += w_rule2 * 0.2 * (fibre_min2 - totals["fibres"]); reasons.append(f"fibre<{fibre_min2}g")
    if kcal_max2 is not None and totals.get("kcal", 0) > float(kcal_max2):
        score += w_kcal2 * (totals["kcal"] - float(kcal_max2)) / 50.0; reasons.append(f"kcal>{kcal_max2}")
    if prot_min2 is not None and totals.get("protein", 0) < float(prot_min2):
        score += w_prot2 * (float(prot_min2) - totals["protein"]); reasons.append(f"protein<{prot_min2}g")

    # --- penalizari pe nume (substring) din YAML ---
    NP = (rules.get("name_penalties", {}) or {}).get("contains", {})
    w_name = float(W.get("w_name_penalty", 0.8))

    def _name_pen(row):
        nm = str(row.get("name_core", "")).lower()
        add = 0.0
        for term, wv in NP.items():
            if term.lower() in nm:
                add += w_name * float(wv)
        return add

    score += _name_pen(prot_row); score += _name_pen(carb_row)
    if veg_row is not None:
        score += _name_pen(veg_row)

    # --- tag penalties / bonuses (din YAML) ---
    TP = rules.get("tag_penalties", {}) or {}
    TB = rules.get("tag_bonuses", {}) or {}
    w_tag = float(W.get("w_tag_penalty", 0.9))

    def _tag_val(row, tag):
        # accepta 'tag_x' sau 'x'
        return int(row.get(f"tag_{tag}", row.get(tag, 0)) or 0)

    def _apply_tags(row):
        add = 0.0; sub = 0.0
        for t, wv in TP.items():
            # penalizarea cerealelor zaharate doar la mic dejun
            if t == "sugary_breakfast_cereal" and meal_key != "breakfast":
                continue
            if _tag_val(row, t):
                add += w_tag * float(wv)
        for t, wv in TB.items():
            if _tag_val(row, t):
                sub += w_tag * float(wv)
        return add - sub

    score += _apply_tags(prot_row); score += _apply_tags(carb_row)
    if veg_row is not None:
        score += _apply_tags(veg_row)

    # --- bonus asociere: protein_bucket <-> carb_bucket ---
    AB = rules.get("assoc_buckets", {}) or {}
    w_assoc = float(W.get("w_assoc", 0.5))

    pb = str(prot_row.get("protein_bucket","") if prot_row is not None else "")
    cb = str(carb_row.get("carb_bucket","")    if carb_row is not None else "")

    s = 0.0
    if pb and cb and pb in AB and cb in (AB[pb] or {}):
        s = float(AB[pb][cb])

    if s > 0:
        score -= w_assoc * s
        reasons.append(f"assoc+{pb}-{cb}:{s:.2f}")
    else:
        # penalizare mica daca perechea nu are suport — evita combinatii ciudate
        score += w_assoc * 0.2
        reasons.append(f"assoc0 {pb}-{cb}")

    # --- reguli dedicate SNACK ---
    if meal_key == "snack":
        def _tag(row, key):
            return int(row.get(f"tag_{key}", row.get(key, 0)) or 0)
        # desert la snack? penalizare mare
        if any(r is not None and _tag(r, "dessert_like") for r in (prot_row, carb_row, veg_row)):
            score += float(W.get("w_rule_penalty", 1.0)) * 3.0
            reasons.append("dessert@snack")
        # sos greu ca proteina la snack? penalizare
        if prot_row is not None and _tag(prot_row, "heavy_sauce"):
            score += float(W.get("w_rule_penalty", 1.0)) * 2.0
            reasons.append("sauce@snack")

    # --- preferinte utilizator, la final (delta mic, nu rupe echilibrul) ---
    try:
        d_pref, why = _apply_user_prefs_delta(prot_row, carb_row, veg_row, meal_key, rules, prefs)
        score += d_pref
        if why:
            reasons.append(why)
    except Exception:
        # nu spargem scorarea daca preferintele au o problema
        pass

    return float(score), ";".join(reasons)