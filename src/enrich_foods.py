###########################################################################
# FISIER: src/enrich_foods.py
#
# SCOP
#   Motorul de "enrich" pentru datasetul de alimente (CIQUAL 2020 prelucrat).
#   Adauga/completeaza bucket-uri (macro/micro) pe roluri (protein/carb/veg),
#   aplica tag-uri (nutritie, procesare, context), semnalizeaza surse de micronutrienti
#   si scrie un audit sumar.
#
# CUM SE RULEAZA (exemple PowerShell, Windows)
#   python src\enrich_foods.py `
#       --in_parquet data\foods_enriched.parquet `
#       --out_parquet data\foods_enriched.parquet `
#       --rules configs\enrich_rules.yaml `
#       --taxonomy configs\taxonomy.yaml `
#       --audit_out outputs\enrich_audit.txt `
#       --overrides data\food_overrides.csv
#
# INTRARI
#   --in_parquet: fisier Parquet cu alimentele (are coloane canonice: kcal_sanitized,
#                 *_g_100g_ml, role_protein/side_carb/side_veg, name_core, main_group etc.)
#   --rules: YAML cu regulile de enrich (macro_buckets, micro_buckets, nutri_thresholds,
#            process_cues, context_cues, dietary_flags, micronutrient_thresholds)
#   --taxonomy: optional, nu il folosim activ aici; doar pentru viitor
#   --overrides: CSV optional cu informatii de ambalare (pack_unit, pack_size, etc.)
#
# IESIRI
#   --out_parquet: scrie datasetul completat, compatibil cu pipeline-ul existent
#   --audit_out:   fisier text cu sumar (nr. itemi, lipsuri bucket pe roluri, top tag-uri)
#
# LEGATURI CU ALTE MODULE
#   - generator_v2.py: foloseste coloanele si tag-urile puse aici (ex. *_bucket, tag_*)
#   - tools/grocery_list.py: poate utiliza coloanele de ambalare (pack_*, serving_g_default)
#   - configs/enrich_rules.yaml: sursa tuturor pragurilor si regulilor
#
# NOTE
#   - Toate comentariile sunt in romana fara diacritice (cerinta proiect).
#   - Functionalitate neschimbata fata de varianta de lucru; doar comentarii si docstrings.
###########################################################################

import re, sys, json, math, pathlib
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import yaml

HERE = pathlib.Path(__file__).resolve().parent
DEFAULT_IN = pathlib.Path("data/foods_enriched.parquet")              # input existent
DEFAULT_OUT = pathlib.Path("data/foods_enriched.parquet")             # suprascriem in-place
RULES_PATH = pathlib.Path("configs/enrich_rules.yaml")
TAXONOMY_PATH = pathlib.Path("configs/taxonomy.yaml")
AUDIT_OUT = pathlib.Path("outputs/enrich_audit.txt")

# Regex simplu pentru tokenizare denumiri (cu apostrof in interiorul cuvantului)
TOKEN_RE = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?", re.UNICODE)


###########################################################################
# load_yaml
# Ce face: incarca YAML din cale data si intoarce obiect Python (dict/list).
# Legaturi: folosit pentru rules (configs/enrich_rules.yaml) si taxonomy.
###########################################################################
def load_yaml(p: pathlib.Path):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


###########################################################################
# to_tokens / has_token / any_token
# Ce fac: utilitare pentru tokenizarea numelor si verificarea prezentei unor
#         indicii (hints) in lista de tokeni.
# Legaturi: folosite de infer_macro_micro si cue_tags.
###########################################################################
def to_tokens(name: str) -> List[str]:
    if not isinstance(name, str) or not name:
        return []
    name = name.lower()
    return TOKEN_RE.findall(name)

def has_token(tokens: List[str], needle: str) -> bool:
    n = needle.lower()
    return n in tokens

def any_token(tokens: List[str], needles: List[str]) -> bool:
    nset = {n.lower() for n in needles}
    return any(t in nset for t in tokens)


###########################################################################
# infer_macro_micro
# Ce face: pe baza de indicii din nume (hints) si a regulilor din YAML,
#          deduce macro-bucket si micro-bucket pentru un rol (protein/side_carb/side_veg).
#          Intoarce (macro, micro, evidence) unde evidence este un string scurt.
# Legaturi: necesita cheile "macro_buckets" si "micro_buckets" in rules.
###########################################################################
def infer_macro_micro(role: str, name_tokens: List[str], rules: Dict) -> Tuple[Optional[str], Optional[str], str]:
    """
    Returneaza: (macro, micro, evidence)
    """
    macro = None
    micro = None
    evidence = ""

    macro_rules = rules.get("macro_buckets", {}).get(role, [])
    for r in macro_rules:
        hints = r.get("name_hints", [])
        if hints and any_token(name_tokens, hints):
            macro = r["macro"]
            evidence = f"macro:name_hints:{macro}"
            break

    # Fallback: daca nu gasim macro, nu fortam; ramane None.
    # Micro: doar daca avem macro si exista reguli
    if macro:
        micro_rules = rules.get("micro_buckets", {}).get(macro, [])
        for mr in micro_rules:
            hints = mr.get("name_hints", [])
            if hints and any_token(name_tokens, hints):
                micro = mr["micro"]
                evidence += f"|micro:name_hints:{micro}"
                break

    return macro, micro, evidence


###########################################################################
# nutri_tags
# Ce face: calculeaza tag-uri pe baza pragurilor nutritionale per 100 g/ml
#          (densitate energetica, sare, zahar, acizi grasi saturati, fibre).
# Legaturi: pragurile sunt in rules["nutri_thresholds"].
###########################################################################
def nutri_tags(row: pd.Series, thr: Dict) -> Dict[str, int]:
    out = {}
    # presupunem ca avem coloane standard per 100g: energy_kcal, salt_g, sugars_g, sat_fat_g, fibre_g
    kcal = float(row.get("energy_kcal", np.nan))
    salt = float(row.get("salt_g", np.nan))
    sug = float(row.get("sugars_g", np.nan))
    sat = float(row.get("sat_fat_g", np.nan))
    fib = float(row.get("fibre_g", np.nan))

    def ge(x, t):
        return int(not math.isnan(x) and x >= t)
    def le(x, t):
        return int(not math.isnan(x) and x <= t)

    if not math.isnan(kcal) and kcal >= thr["high_energy_density_kcal_per_100g"]:
        out["tag_high_energy_density"] = 1
    if ge(salt, thr["very_high_salt_g_per_100g"]):
        out["tag_very_high_salt"] = 1
    elif ge(salt, thr["high_salt_g_per_100g"]):
        out["tag_high_salt"] = 1

    if ge(sug, thr["high_sugars_g_per_100g"]):
        out["tag_high_sugars"] = 1
    if ge(sat, thr["high_sat_fat_g_per_100g"]):
        out["tag_high_sat_fat"] = 1

    if le(fib, thr["low_fibre_g_per_100g"]):
        out["tag_low_fibre"] = 1
    if ge(fib, thr["high_fibre_g_per_100g"]):
        out["tag_high_fibre"] = 1

    return out


###########################################################################
# cue_tags
# Ce face: adauga tag-uri pe baza "cues" (procesare si context) definite in YAML
#          + cateva heuristici istorice pentru compatibilitate (refined_carb, etc.).
# Legaturi: rules["process_cues"], rules["context_cues"].
###########################################################################
def cue_tags(name_tokens: List[str], rules: Dict) -> Dict[str, int]:
    out = {}
    for cue, lst in rules.get("process_cues", {}).items():
        if lst and any_token(name_tokens, lst):
            out[f"tag_{cue}"] = 1
    for cue, lst in rules.get("context_cues", {}).items():
        if lst and any_token(name_tokens, lst):
            out[f"tag_{cue}"] = 1
    # mentinem compatibilitatea cu tag-urile istorice (heuristici simple)
    if any_token(name_tokens, ["chips", "fries", "wedges", "crisps"]):
        out["tag_fried_or_chips"] = 1
    if any_token(name_tokens, ["croissant", "cake", "tart", "donut", "brownie", "cookie", "muffin"]):
        out["tag_dessert_like"] = 1
    if any_token(name_tokens, ["white", "pastry", "couscous", "white rice"]):
        out["tag_refined_carb"] = 1
    if any_token(name_tokens, ["cream", "cheesy", "alfredo", "carbonara", "mayo", "aioli", "butter"]):
        out["tag_heavy_sauce"] = 1
    if any_token(name_tokens, ["granola", "cornflakes", "frosted", "cocoa"]):
        out["tag_sugary_breakfast_cereal"] = 1
    if any_token(name_tokens, ["starch", "potato starch", "rice starch", "cornstarch", "corn starch"]):
        out["tag_starch_powder"] = 1
    if any_token(name_tokens, ["pie", "stew", "casserole", "gratin", "bake", "lasagna", "shepherd", "cottage"]):
        out["tag_composite_dish_hint"] = 1
    # explicit drink/dairy dessert, chiar daca nu s-au potrivit cues
    if any_token(name_tokens, ["milkshake","smoothie","juice","soda","cola","soft","lemonade","ice","iced","kefir"]):
        out["tag_drink"] = 1
    if any_token(name_tokens, ["ice","cream","mousse","pudding","flan","custard"]):
        out["tag_dairy_dessert_hint"] = 1

    return out


###########################################################################
# micro_source_tags
# Ce face: marcheaza (tag-uri) daca un aliment este sursa buna/inalta pentru
#          anumiati micronutrienti pe 100 g (daca valorile exista ca si coloane).
# Legaturi: praguri in rules["nutri_thresholds"]["micronutrient_thresholds"].
###########################################################################
def micro_source_tags(row: pd.Series, thr: Dict) -> Dict[str, int]:
    """
    Micronutrient source flags based on per-100g content.
    If a column is missing, it is ignored.
    """
    out = {}
    micro = thr.get("micronutrient_thresholds", {})
    # Map: column_name -> (key, units)
    specs = {
        "iron_mg": ("iron_mg", "mg"),
        "calcium_mg": ("calcium_mg", "mg"),
        "magnesium_mg": ("magnesium_mg", "mg"),
        "potassium_mg": ("potassium_mg", "mg"),
        "vit_c_mg": ("vit_c_mg", "mg"),
        "vit_b12_ug": ("vit_b12_ug", "ug"),
    }
    for col, (mkey, _units) in specs.items():
        if col not in row.index:
            continue
        try:
            val = float(row.get(col, np.nan))
        except Exception:
            val = np.nan
        if np.isnan(val):
            continue
        limits = micro.get(mkey, None)
        if not limits:
            continue
        g = limits.get("good", None)
        h = limits.get("high", None)
        if h is not None and val >= h:
            out[f"tag_high_{mkey}_source"] = 1
        elif g is not None and val >= g:
            out[f"tag_good_{mkey}_source"] = 1
    return out


###########################################################################
# dietary_from_macros
# Ce face: deduce tag-uri dietetice de baza (vegan/vegetarian safe) pe baza
#          macro-bucket-ului deja dedus.
# Legaturi: rules["dietary_flags"].
###########################################################################
def dietary_from_macros(macro: Optional[str], rules: Dict) -> Dict[str, int]:
    out = {}
    veg_list = set(rules.get("dietary_flags", {}).get("vegan_safe_from_macros", []))
    vegt_list = set(rules.get("dietary_flags", {}).get("vegetarian_safe_from_macros", []))
    if macro in vegt_list:
        out["tag_vegetarian_safe"] = 1
    if macro in veg_list:
        out["tag_vegan_safe"] = 1
    return out


###########################################################################
# quality_gates
# Ce face: mic filtru de consistenta pentru tag-uri care pot fi conflictuale,
#          ex: daca e wholegrain sau fibra mare, nu marcăm refined_carb.
# Legaturi: se foloseste dupa ce am setat tag-urile.
###########################################################################
def quality_gates(row: pd.Series) -> Dict[str, int]:
    """
    Reguli de consistenta minimaliste (nu penalizam aici; doar ajustam tag-uri conflict).
    """
    out = {}
    # daca e wholegrain (fibre mari sau micro 'bread_wholegrain'), nu marca refined_carb
    high_fibre = int(row.get("tag_high_fibre", 0)) == 1
    bread_whole = str(row.get("carb_micro", "")) == "bread_wholegrain"
    refined = int(row.get("tag_refined_carb", 0)) == 1
    if (high_fibre or bread_whole) and refined:
        out["tag_refined_carb"] = 0
    return out


###########################################################################
# _apply_overrides
# Ce face: aplica (merge) optional informatii de ambalare din CSV (pack_unit,
#          pack_size, units_per_pack, serving_g_default, shelf_stable_days),
#          mapate dupa uid sau name_core. No-op sigur daca lipseste fisierul.
# Legaturi: folosit in main() inainte de rule engine.
###########################################################################
def _apply_overrides(df: pd.DataFrame, path: str) -> pd.DataFrame:
    """
    Merge optional packaging overrides (pack_unit, pack_size, units_per_pack, serving_g_default, shelf_stable_days)
    keyed by uid or name_core. Safe no-op if file missing or unreadable.
    """
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        return df
    try:
        ov = pd.read_csv(p)
    except Exception:
        return df

    # normalize keys
    for col in ("uid", "name_core"):
        if col in ov.columns:
            ov[col] = ov[col].astype(str).str.strip()
    if "uid" in df.columns:
        df["uid"] = df["uid"].astype(str).str.strip()
    if "name_core" in df.columns:
        df["name_core"] = df["name_core"].astype(str).str.strip()

    # prefer merge on uid; fallback on name_core
    if "uid" in ov.columns and ov["uid"].notna().any():
        df = df.merge(ov, how="left", on="uid", suffixes=("", "_ov"))
    elif "name_core" in ov.columns:
        df = df.merge(ov, how="left", on="name_core", suffixes=("", "_ov"))
    else:
        return df

    # copy expected columns (prefer base col, else *_ov)
    def pick_col(base):
        if base in df.columns: return base
        alt = base + "_ov"
        return alt if alt in df.columns else None

    for c in ("pack_unit", "pack_size", "units_per_pack", "serving_g_default", "shelf_stable_days"):
        sc = pick_col(c)
        if sc:
            df[c] = df[c].where(df[c].notna(), df[sc])

    # cast numeric where relevant
    for c in ("pack_size", "units_per_pack", "serving_g_default", "shelf_stable_days"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "pack_unit" in df.columns:
        df["pack_unit"] = df["pack_unit"].astype(str).str.lower().str.strip()

    return df


###########################################################################
# apply_engine
# Ce face: aplica pe fiecare rand din df regulile de enrich:
#          - infer macro/micro bucket pe roluri
#          - tag-uri nutritionale (din praguri)
#          - tag-uri de procesare/context (din cues)
#          - tag-uri sursa micronutrienti
#          - tag-uri dietetice din macro
#          - corectii de consistenta (quality_gates)
# Legaturi: foloseste functiile de mai sus, plus rules (YAML).
###########################################################################
def apply_engine(df: pd.DataFrame, rules: Dict) -> pd.DataFrame:
    thr = rules.get("nutri_thresholds", {})
    # initializam coloanele cheie daca lipsesc (compatibilitate)
    for c in ["protein_bucket","carb_bucket","veg_bucket","protein_micro","carb_micro","veg_micro"]:
        if c not in df.columns: df[c] = ""
    # tag-urile noi vor fi adaugate incremental; nu avem o lista fixa aici.

    # aplicam imbogatirea rand cu rand (claritate > micro-optimizare)
    def _enrich_row(row: pd.Series) -> pd.Series:
        name_tokens = to_tokens(str(row.get("name_core") or row.get("name") or ""))

        # PROTEIN role
        if int(row.get("role_protein", 0)) == 1:
            p_macro, p_micro, p_evd = infer_macro_micro("protein", name_tokens, rules)
            if p_macro: row["protein_bucket"] = p_macro
            if p_micro: row["protein_micro"] = p_micro
            row["protein_evidence"] = p_evd
            row["protein_conf"] = 0.9 if p_macro else 0.0

        # CARB role
        if int(row.get("role_side_carb", 0)) == 1:
            c_macro, c_micro, c_evd = infer_macro_micro("side_carb", name_tokens, rules)
            if c_macro: row["carb_bucket"] = c_macro
            if c_micro: row["carb_micro"] = c_micro
            row["carb_evidence"] = c_evd
            row["carb_conf"] = 0.9 if c_macro else 0.0

        # VEG role
        if int(row.get("role_side_veg", 0)) == 1:
            v_macro, v_micro, v_evd = infer_macro_micro("side_veg", name_tokens, rules)
            if v_macro: row["veg_bucket"] = v_macro
            if v_micro: row["veg_micro"] = v_micro
            row["veg_evidence"] = v_evd
            row["veg_conf"] = 0.9 if v_macro else 0.0

        # TAG-uri nutritionale (deterministe)
        for k, v in nutri_tags(row, thr).items():
            row[k] = v

        # Micronutrient source tags (deterministic daca exista coloane)
        for k, v in micro_source_tags(row, thr).items():
            row[k] = v

        # TAG-uri procesare/context + compat istoric
        for k, v in cue_tags(name_tokens, rules).items():
            row[k] = v

        # TAG-uri dietetice din macro-bucket
        macro_any = row.get("protein_bucket","") or row.get("carb_bucket","") or row.get("veg_bucket","")
        for k, v in dietary_from_macros(macro_any, rules).items():
            row[k] = v

        # Gates de calitate (consistenta)
        for k, v in quality_gates(row).items():
            row[k] = v

        return row

    df = df.apply(_enrich_row, axis=1)
    return df


###########################################################################
# audit_report
# Ce face: construieste un sumar text (nr. itemi, % lipsa bucket-uri per rol,
#          top tag-uri din df). Folosit pentru debug si sanity-check.
# Legaturi: scris de main() in fisierul de audit.
###########################################################################
def audit_report(df: pd.DataFrame) -> str:
    lines = []
    n = len(df)
    lines.append(f"Items: {n}")

    # % fara bucket pe roluri (raportam doar pe subsetul cu rolul activ)
    for role, col in [("protein","protein_bucket"), ("carb","carb_bucket"), ("veg","veg_bucket")]:
        role_col = f"role_{'protein' if role=='protein' else 'side_'+role}"
        subset = df[df[role_col] == 1] if role_col in df.columns else pd.DataFrame()
        if not subset.empty:
            missing = (subset[col] == "").sum()
            pct = 100.0 * missing / len(subset)
            lines.append(f"Missing {role}_bucket: {missing}/{len(subset)} ({pct:.1f}%)")

    # top cateva tag-uri noi
    tag_cols = [c for c in df.columns if c.startswith("tag_")]
    tag_counts = []
    for c in tag_cols:
        try:
            tag_counts.append((c, int(df[c].fillna(0).astype(int).sum())))
        except Exception:
            continue
    tag_counts.sort(key=lambda x: x[1], reverse=True)
    lines.append("Top tags:")
    for c, cnt in tag_counts[:20]:
        lines.append(f"  {c}: {cnt}")

    return "\n".join(lines)


###########################################################################
# main
# Ce face: CLI pentru imbogatire completa:
#          - citeste input parquet
#          - aplica overrides (ambalare) daca exista
#          - incarca reguli si (optional) taxonomy
#          - rule engine (apply_engine)
#          - scrie audit si parquet final
# Legaturi: parte din pipeline (rulat inainte de generator_v2.py).
###########################################################################
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_parquet", default=str(DEFAULT_IN))
    ap.add_argument("--out_parquet", default=str(DEFAULT_OUT))
    ap.add_argument("--rules", default=str(RULES_PATH))
    ap.add_argument("--taxonomy", default=str(TAXONOMY_PATH))
    ap.add_argument("--audit_out", default=str(AUDIT_OUT))
    ap.add_argument("--overrides", default="data/food_overrides.csv",
                    help="Optional CSV with packaging info keyed by uid or name_core")

    args = ap.parse_args()

    in_p = pathlib.Path(args.in_parquet)
    out_p = pathlib.Path(args.out_parquet)
    audit_p = pathlib.Path(args.audit_out)

    if not in_p.exists():
        print(f"[ERROR] Missing input: {in_p}", file=sys.stderr)
        sys.exit(2)
    if not pathlib.Path(args.rules).exists():
        print(f"[ERROR] Missing rules: {args.rules}", file=sys.stderr)
        sys.exit(3)

    # 1) citire parquet de baza
    df = pd.read_parquet(in_p)

    # 2) aplicare overrides (ambalare), daca CSV-ul exista
    df = _apply_overrides(df, args.overrides)

    # 3) incarca reguli si (optional) taxonomy
    rules = load_yaml(pathlib.Path(args.rules))
    try:
        taxonomy = load_yaml(pathlib.Path(args.taxonomy))
    except Exception:
        taxonomy = {}

    # 4) rule engine de imbogatire
    df = apply_engine(df, rules)

    # 5) audit + salvare
    audit_txt = audit_report(df)
    audit_p.parent.mkdir(parents=True, exist_ok=True)
    audit_p.write_text(audit_txt, encoding="utf-8")

    # IMPORTANT: pastram compatibilitatea — nu eliminam coloane existente
    out_p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_p, index=False)
    print("[OK] Enrich complet. Audit la:", audit_p)

if __name__ == "__main__":
    main()
