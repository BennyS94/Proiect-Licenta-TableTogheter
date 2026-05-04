# src/core/pools.py
from __future__ import annotations
import pandas as pd
import re

def _allowed_for_meal(row, meal_id: str) -> bool:
    am = str(row.get("allowed_meals", ""))
    return meal_id in am.split(";")

def build_pool(df: pd.DataFrame, meal_id: str, role: str, rules: dict) -> pd.DataFrame:
    """
    Returneaza subsetul de candidati pentru rolul {protein|side_carb|side_veg},
    deja filtrati la nivel de "puritate" pentru productie.
    Presupune ca df este deja filtrat global (bans/dietary).
    """
    role = role.lower()
    role_col = {"protein":"role_protein","side_carb":"role_side_carb","side_veg":"role_side_veg"}[role]

    pool = df[(df[role_col]==1) & df.apply(lambda r: _allowed_for_meal(r, meal_id), axis=1)].copy()

    # ---------- role-specific purity ----------
    name = pool["name_core"].fillna("").str.lower()

    if role == "side_veg":
        # exclude preparate complete (nu sunt garnituri de legume)
        bad_veg_terms = r"\b(?:lasagna|cannelloni|ravioli|dumpling|stuffed|tart|quiche|pie|pizza|sandwich|wrap|burger)\b"
        pool = pool[~name.str.contains(bad_veg_terms, regex=True, na=False)]
        # cere un veg_bucket valid
        pool = pool[pool["veg_bucket"].fillna("").ne("")]
        # legumele ca side au densitate kcal mai scazuta
        pool["kcal100"] = pd.to_numeric(pool.get("kcal_sanitized"), errors="coerce").fillna(0.0)
        pool = pool[pool["kcal100"] <= 120]
        # la mic dejun, evitam supele ca "side"
        if meal_id == "breakfast":
            n2 = pool["name_core"].fillna("").str.lower()
            pool = pool.loc[~n2.str.contains(r"\bsoup\b", regex=True, na=False)]
    elif role == "side_carb":
        # scoatem ingrediente/grasimi/crisps/fried etc.
        bad_side_terms = r"\b(?:raw|flour|powder|mix|crisps|chips|oil|butter|margarine|lard|fried|deep[- ]?fried|croquette|dauphine|dried|semolina|grits|uncooked|pre[- ]?cooked)\b"
        pool = pool[~name.str.contains(bad_side_terms, regex=True, na=False)]
        # sa aiba carbo reali
        pool["carb100"] = pd.to_numeric(pool.get("carbohydrate_g_100g_ml"), errors="coerce").fillna(0.0)
        pool = pool[pool["carb100"] >= 10]
        # sa nu fie "de fapt proteic" (ex. carne pane)
        pool = pool[pool["role_protein"] != 1]
        # limita grasime/100g pentru garnitura
        fat_max = float(rules.get("soft_limits", {}).get("side_carb_fat_g_per100g_max", 8.0))
        pool["fat100"] = pd.to_numeric(pool.get("fat_g_100g_ml"), errors="coerce").fillna(0.0)
        pool = pool[pool["fat100"] <= fat_max]

    else:  # protein
        # nimic agresiv aici; selectia finala se face din scor (lean etc.)
        n2 = pool["name_core"].fillna("").str.lower()
        # la mic dejun evitam proteine crude/uscat/pudra/semolina
        if meal_id == "breakfast":
            pool = pool.loc[
                ~n2.str.contains(r"\b(?:dried|raw|powder|flour|semolina|uncooked|pre[- ]?cooked)\b", regex=True,
                                 na=False)]

        # prefera variante gatite
        pool["is_cooked"] = pool["name_core"].fillna("").str.contains(
            r"\b(?:cooked|boiled|steamed|baked|roasted|grilled|pan[- ]?fried|sautéed|sauteed)\b",
            case=False, regex=True, na=False
        ).astype(int)
        pool = pool.sort_values("is_cooked", ascending=False)

        pass

    # inside build_pool / candidates_for_slot:
    if pool is None or len(pool) == 0:
        # return an empty DataFrame with same columns as the input df
        return df.iloc[0:0]
    return pool
