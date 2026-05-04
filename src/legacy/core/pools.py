###########################################################################
# FISIER: src/core/pools.py
#
# SCOP
#   Construieste pool-urile de candidati (DataFrame-uri filtrate) pentru
#   fiecare rol de masa: protein / side_carb / side_veg. Filtrele sunt
#   "purete" de productie, adica scoatem item-urile care nu se potrivesc
#   rolului (ingrediente, pulberi, preparate complete pentru rolul de legume,
#   garnituri fara carbo reali etc.).
#
# CUM SE APELEAZA
#   pool = build_pool(df, meal_id, role, rules)
#     - df: DataFrame cu alimente (deja filtrat global: bans/dietary)
#     - meal_id: "breakfast" / "lunch" / "dinner" / "snack"
#     - role: "protein" / "side_carb" / "side_veg"
#     - rules: dict din YAML (citit de generator), folosit aici pentru
#              praguri (ex. soft_limits.side_carb_fat_g_per100g_max)
#
# LEGATURI
#   - generator_v2.py -> foloseste build_pool() cand pick-uieste mesele
#   - configs/culinary_rules.yaml -> chei: soft_limits.side_carb_fat_g_per100g_max
#
# NOTE
#   - Comentarii in romana fara diacritice.
#   - In acest fisier incercam sa nu punem valori magice hard-codate; unde se
#     poate, luam pragurile din YAML (rules). Un set minimal de termeni regex
#     ramane aici, dar este recomandat sa fie mutat in YAML pe viitor.
###########################################################################

from __future__ import annotations
import pandas as pd
import re

###########################################################################
# _allowed_for_meal
# Ce face: verifica daca randul (alimentul) este permis la masa curenta,
#          folosind coloana "allowed_meals" (string cu valori separate prin ';').
# Legaturi: depinde de schema din enrich (col. allowed_meals).
###########################################################################

def _allowed_for_meal(row, meal_id: str) -> bool:
    am = str(row.get("allowed_meals", ""))
    return meal_id in am.split(";")

###########################################################################
# build_pool
# Ce face: aplica filtre de puritate specifice rolului si mesei pentru a
#          construi setul de candidati.
# Legaturi externe:
#   - rules: citit in generator si transmis aici; folosim rules["soft_limits"].
#   - df: trebuie sa contina cel putin: name_core, kcal_sanitized,
#         carbohydrate_g_100g_ml, fat_g_100g_ml, veg_bucket, role_* si
#         allowed_meals.
# Observatii:
#   - Filtrele sunt intentionat strict-utile (nu perfecte) pentru a reduce
#     combinatiile nepotrivite in scor. Ajustarile fine se fac in scoring.
###########################################################################

def build_pool(df: pd.DataFrame, meal_id: str, role: str, rules: dict) -> pd.DataFrame:
    """
    Intoarce subsetul de candidati pentru rolul {protein|side_carb|side_veg},
    deja filtrati la nivel de "puritate" pentru productie.
    Presupune ca df este deja filtrat global (bans/dietary) in generator.
    """
    role = role.lower()
    role_col = {"protein":"role_protein","side_carb":"role_side_carb","side_veg":"role_side_veg"}[role]

    # 1) Filtru de baza: are rolul setat pe 1 si este permis la masa curenta
    #    Notita: folosim .apply pentru allowed_meals; ar fi posibil de vectorizat
    #    cu .str.contains daca formatul devine strict (ex. ";lunch;"), dar aici
    #    ramanem simpli si robusti la variatii.
    pool = df[(df[role_col]==1) & df.apply(lambda r: _allowed_for_meal(r, meal_id), axis=1)].copy()

    # 2) Daca pool-ul a devenit gol, intoarcem un DataFrame gol (cu aceleasi coloane)
    if pool is None or len(pool) == 0:
        return df.iloc[0:0]

    # 3) Pregatim coloana de nume normalizata pentru regex-uri (folosita in toate rolurile)
    name = pool["name_core"].fillna("").str.lower()

    # ---------- purity pe rol ----------
    if role == "side_veg":
        # a) Exclude preparate complete care nu sunt potrivite ca "side" de legume
        #    (ex. lasagna, quiche, pizza, sandwiches etc.)
        #    Observatie: lista ar trebui migrata in YAML pe viitor.
        bad_veg_terms = r"\b(?:lasagna|cannelloni|ravioli|dumpling|stuffed|tart|quiche|pie|pizza|sandwich|wrap|burger)\b"
        pool = pool[~name.str.contains(bad_veg_terms, regex=True, na=False)]

        # b) Cerem un veg_bucket valid (altfel este scos din pool)
        pool = pool[pool["veg_bucket"].fillna("").ne("")]

        # c) Legumele ca side au densitate calorica relativ mica
        pool["kcal100"] = pd.to_numeric(pool.get("kcal_sanitized"), errors="coerce").fillna(0.0)
        pool = pool[pool["kcal100"] <= 120]

        # d) La mic dejun, evitam explicit supele ca side-veg (usability)
        if meal_id == "breakfast":
            n2 = pool["name_core"].fillna("").str.lower()
            pool = pool.loc[~n2.str.contains(r"\bsoup\b", regex=True, na=False)]

    elif role == "side_carb":
        # a) Scoatem ingrediente/grasimi/snacks improprii rolului de garnitura
        #    Notita: regex extins pentru a prinde variante comune; pe viitor, mutat in YAML.
        bad_side_terms = (
            r"\b(?:raw|flour|powder|mix|crisps|chips|oil|butter|margarine|lard|"
            r"fried|deep[- ]?fried|croquette|dauphine|dried|semolina|grits|uncooked|pre[- ]?cooked)\b"
        )
        pool = pool[~name.str.contains(bad_side_terms, regex=True, na=False)]

        # b) Garnitura trebuie sa aiba carbohidrati reali (prag minim/100 g)
        pool["carb100"] = pd.to_numeric(pool.get("carbohydrate_g_100g_ml"), errors="coerce").fillna(0.0)
        pool = pool[pool["carb100"] >= 10]

        # c) Scoatem item-urile marcate si ca proteina (ex. carne pane)
        pool = pool[pool["role_protein"] != 1]

        # d) Limitam grasimea/100 g pentru garnitura (sau preluam plafon din YAML)
        fat_max = float(rules.get("soft_limits", {}).get("side_carb_fat_g_per100g_max", 8.0))
        pool["fat100"] = pd.to_numeric(pool.get("fat_g_100g_ml"), errors="coerce").fillna(0.0)
        pool = pool[pool["fat100"] <= fat_max]

    else:  # protein
        # a) La micul dejun eliminam variante crude/uscate/pudra/semolina etc.
        if meal_id == "breakfast":
            n2 = pool["name_core"].fillna("").str.lower()
            pool = pool.loc[
                ~n2.str.contains(r"\b(?:dried|raw|powder|flour|semolina|uncooked|pre[- ]?cooked)\b", regex=True, na=False)
            ]

        # b) Preferam variante cu indicii de gatire (ajuta scorarea ulterioara)
        pool["is_cooked"] = pool["name_core"].fillna("").str.contains(
            r"\b(?:cooked|boiled|steamed|baked|roasted|grilled|pan[- ]?fried|sautéed|sauteed)\b",
            case=False, regex=True, na=False
        ).astype(int)
        pool = pool.sort_values("is_cooked", ascending=False)
        # Notita: Scorul final face oricum selectie pe densitatea proteica etc.

    # 4) Returnam pool-ul rezultat (sau DF gol daca totul a fost filtrat)
    if pool is None or len(pool) == 0:
        return df.iloc[0:0]
    return pool
