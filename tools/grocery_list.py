###########################################################################
# FISIER: tools/grocery_list.py
#
# SCOP
#   Genereaza o lista de cumparaturi (grocery list) dintr-un plan generat:
#     - input: outputs/plan_v2.csv (planul cu portii in grame)
#     - lookup: data/foods_enriched.parquet (nume, categorii, tag-uri, pack overrides)
#     - output: outputs/grocery_list.txt (agregare + rotunjire la pachete tipice)
#
# IDEE
#   1) "Explode" planul in linii {uid, portion_g} pentru fiecare rol (protein/carb/veg)
#   2) Agregam per uid (suma portiilor pe toate zilele/meselor)
#   3) Join cu foods pentru name_core, main_group, tag_*, si eventual pack overrides
#   4) Alegem o "pack policy":
#      - daca item-ul are override explicit (pack_unit + pack_size), il folosim
#      - altfel, aplicam reguli generale din configs/pack_defaults.yaml
#   5) Rotunjim la cele mai apropiate pachete (min overage; tie -> mai putine pachete)
#
# CUM SE RULEAZA (PowerShell)
#   python tools\grocery_list.py `
#     --plan outputs\plan_v2.csv `
#     --foods data\foods_enriched.parquet `
#     --out outputs\grocery_list.txt `
#     --group_by main_group `
#     --pack_defaults configs\pack_defaults.yaml `
#     --days 3
#
# IESIRI
#   - outputs/grocery_list.txt: categorie -> lista items + sugestie pachete
#
# LEGATURI
#   - src/generator_v2.py: produce outputs/plan_v2.csv
#   - src/enrich_foods.py: poate adauga pack overrides (pack_unit/pack_size/units_per_pack)
#   - configs/pack_defaults.yaml: reguli generale de pachete pe tag-uri / main_group
#
# NOTE
#   - Comentarii in romana fara diacritice.
#   - Lista este orientativa; nu garanteaza exact pachetele din magazine reale.
###########################################################################
# tools/grocery_list.py
# Build a grocery list from plan_v2.csv + foods_enriched, aggregated over multiple days.
# It resolves pack sizes via per-product overrides or via simple tag/main_group rules in pack_defaults.yaml.
# Usage (PowerShell):
#   python tools\grocery_list.py `
#     --plan outputs\plan_v2.csv `
#     --foods data\foods_enriched.parquet `
#     --out outputs\grocery_list.txt `
#     --group_by main_group `
#     --pack_defaults configs\pack_defaults.yaml `
#     --days 3

import argparse
import math
import re
from pathlib import Path

import pandas as pd
import yaml


###########################################################################
# load_foods
# Ce face: citeste foods_enriched din parquet/csv (fallback robust)
# Legaturi: data/foods_enriched.parquet

def load_foods(path):
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"[ERR] foods not found: {p}")
    try:
        if p.suffix == ".parquet":
            return pd.read_parquet(p)  # citire rapida si compacta
    except Exception:
        pass
    return pd.read_csv(p)


###########################################################################
# load_pack_defaults
# Ce face: citeste reguli pack_defaults.yaml (fallback implicit daca lipseste)
# Legaturi: configs/pack_defaults.yaml

def load_pack_defaults(path):
    p = Path(path)
    if not p.exists():
        # sensible fallback
        return {
            "tags_to_packs": [],  # reguli conditionale (when) -> pack policy
            "fallback": {
                "solids": {"unit": "g", "typical_sizes": [500, 1000], "shelf_stable_days": 30},
                "liquids": {"unit": "ml", "typical_sizes": [1000], "shelf_stable_days": 30},
            },
        }
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}  # YAML -> dict
        # ensure minimal structure
        if "fallback" not in data:
            data["fallback"] = {
                "solids": {"unit": "g", "typical_sizes": [500, 1000], "shelf_stable_days": 30},
                "liquids": {"unit": "ml", "typical_sizes": [1000], "shelf_stable_days": 30},
            }
        if "tags_to_packs" not in data:  # reguli conditionale (when) -> pack policy
            data["tags_to_packs"] = []  # reguli conditionale (when) -> pack policy
        return data
    except Exception:
        return {
            "tags_to_packs": [],  # reguli conditionale (when) -> pack policy
            "fallback": {
                "solids": {"unit": "g", "typical_sizes": [500, 1000], "shelf_stable_days": 30},
                "liquids": {"unit": "ml", "typical_sizes": [1000], "shelf_stable_days": 30},
            },
        }


###########################################################################
# resolve_pack_policy
# Ce face: alege politica de pachete pentru un item (pe baza tag/main_group/name/veg_bucket)
# Legaturi: pack_defaults.yaml + tag_*

def resolve_pack_policy(row, defaults):
    """
    Accepts dict-like or pandas Series.
    Returns dict {unit, typical_sizes, shelf_stable_days, units_per_pack_default?}
    """
    # generic getter
    def getv(k, default=""):  # getter generic: merge Series/dict-like fara sa crape
        if hasattr(row, "get"):
            return row.get(k, default)
        try:
            return row[k]
        except Exception:
            return default

    main_group = str(getv("main_group", "") or "").lower()
    veg_bucket = str(getv("veg_bucket", "") or "").lower()
    name_core  = str(getv("name_core", "") or "").lower()

    # collect keys
    keys = list(row.keys()) if hasattr(row, "keys") else list(getattr(row, "index", []))
    tag_cols = [c for c in keys if str(c).startswith("tag_")]  # includem tag_* in merged ca sa putem decide pack policy

    # which tag_* are present == 1
    present_tags = set()  # set cu tag_* care sunt active (==1) pentru item
    for c in tag_cols:
        try:
            v = getv(c, 0)
            v = pd.to_numeric(v, errors="coerce")
            if pd.notna(v) and int(v) == 1:
                present_tags.add(c)  # set cu tag_* care sunt active (==1) pentru item
        except Exception:
            continue

    def match_rule(rule):
        cond = rule.get("when", {}) or {}

        any_tags = cond.get("any_tags") or []
        if any_tags and not any(t in present_tags for t in any_tags):  # set cu tag_* care sunt active (==1) pentru item
            return False

        all_tags = cond.get("all_tags") or []
        for t in all_tags:
            if t not in present_tags:  # set cu tag_* care sunt active (==1) pentru item
                return False

        mgr = cond.get("main_group_regex")
        if mgr and not re.search(mgr, main_group or "", flags=re.IGNORECASE):  # match regex pe main_group/name_core (case-insensitive)
            return False

        nr = cond.get("name_regex")
        if nr and not re.search(nr, name_core or "", flags=re.IGNORECASE):  # match regex pe main_group/name_core (case-insensitive)
            return False

        vbin = cond.get("veg_bucket_in") or []
        if vbin and veg_bucket not in {v.lower() for v in vbin}:
            return False

        return True

    rules = defaults.get("tags_to_packs", []) or []  # reguli conditionale (when) -> pack policy
    for rule in rules:
        if match_rule(rule):
            out = {
                "unit": rule.get("unit", "g"),
                "typical_sizes": list(rule.get("typical_sizes", [])),
                "shelf_stable_days": int(rule.get("shelf_stable_days", 30)),
            }
            if "units_per_pack_default" in rule:
                out["units_per_pack_default"] = int(rule["units_per_pack_default"])
            return out

    # fallback
    fb = defaults.get("fallback", {})
    solids = fb.get("solids", {"unit": "g", "typical_sizes": [500, 1000], "shelf_stable_days": 30})
    liquids = fb.get("liquids", {"unit": "ml", "typical_sizes": [1000], "shelf_stable_days": 30})

    unit_guess = "g"  # fallback unit: ml daca e drink, altfel g
    pu = str(getv("pack_unit", "") or "").lower().strip()
    if pu == "ml":
        unit_guess = "ml"  # fallback unit: ml daca e drink, altfel g
    elif "tag_drink" in present_tags:  # set cu tag_* care sunt active (==1) pentru item
        unit_guess = "ml"  # fallback unit: ml daca e drink, altfel g

    return liquids if unit_guess == "ml" else solids  # fallback unit: ml daca e drink, altfel g


###########################################################################
# fmt_qty
# Ce face: formatare cantitate g/ml in kg/L cand depaseste 1000

def fmt_qty(x, unit):
    x = float(x or 0.0)
    if unit == "ml":  # afisare in ml/L
        if x >= 1000:  # convertim la kg/L peste 1000
            return f"{x/1000:.1f} L"
        return f"{int(round(x))} ml"
    else:
        if x >= 1000:  # convertim la kg/L peste 1000
            return f"{x/1000:.1f} kg"
        return f"{int(round(x))} g"


###########################################################################
# pack_line
# Ce face: pentru un item agregat, alege pachete tipice si construieste linia finala
# Legaturi: resolve_pack_policy + overrides

def pack_line(row, defaults, overage_warn_frac=0.10, perishable_short_days=5):  # warning pentru produse perisabile (cumpara aproape de utilizare)
    """
    Decide best packs for one item row (merged foods + total portion_g).
    Returns tuple (name_core, total, pretty_line)
    """
    name = str(row.get("name_core", "")).strip()
    total = float(row.get("portion_g", 0.0) or 0.0)

    # explicit per-product override?
    unit = str(row.get("pack_unit") or "").lower().strip()
    pack_size = float(row.get("pack_size", 0.0) or 0.0)
    upp = float(row.get("units_per_pack", 0.0) or 0.0)

    typical_sizes = []
    shelf_days = None

    if unit in ("g", "ml") and pack_size > 0:
        typical_sizes = [pack_size]
        shelf_days = int(row.get("shelf_stable_days", 0) or 0) if ("shelf_stable_days" in (
            row.keys() if hasattr(row, "keys") else getattr(row, "index", []))) else None
        # if upp > 1 we will display it; selection stays by pack_size
    else:
        pol = resolve_pack_policy(row, defaults)
        unit = pol.get("unit", "g")
        typical_sizes = list(pol.get("typical_sizes", []))
        shelf_days = int(pol.get("shelf_stable_days", 30))
        if upp <= 0 and "units_per_pack_default" in pol:
            upp = int(pol["units_per_pack_default"])

    if not typical_sizes:
        typical_sizes = [1000] if unit == "ml" else [500, 1000]  # afisare in ml/L

    # choose size by minimal overage, tie -> fewer packs
    best = None
    for s in typical_sizes:
        s = float(s)
        if s <= 0:
            continue
        packs = math.ceil(total / s) if total > 0 else 1  # numar pachete necesare (rotunjire in sus)
        over = packs * s - total
        key = (over, packs)  # criteriu selectie: overage minim; tie -> mai putine pachete
        cand = {"size": s, "packs": packs, "over": over}
        if best is None or key < (best["over"], best["packs"]):
            best = cand
    if best is None:
        best = {"size": float(typical_sizes[0]), "packs": 1, "over": max(0.0, float(typical_sizes[0]) - total)}

    if upp and upp > 1:
        packs_text = f"{best['packs']} x ({int(upp)} x {int(best['size'])}{unit})"
        total_nominal = best["packs"] * upp * best["size"]
    else:
        packs_text = f"{best['packs']} x {int(best['size'])}{unit}"
        total_nominal = best["packs"] * best["size"]

    warn = ""
    if total > 0:
        frac = best["over"] / total
        if frac > overage_warn_frac:
            warn = " | note: high overage"  # warning cand cumperi prea mult vs planned
    peris = ""
    if shelf_days is not None and shelf_days > 0 and shelf_days <= perishable_short_days:  # warning pentru produse perisabile (cumpara aproape de utilizare)
        peris = " | perishable: buy close to use"  # warning pentru produse perisabile (cumpara aproape de utilizare)

    line = f"{name}: {packs_text} (planned {fmt_qty(total, unit)}; over {fmt_qty(best['over'], unit)}){warn}{peris}"
    return name, total, line


###########################################################################
# main
# Ce face: CLI: citeste plan, agregare uid, join foods, scrie grocery_list.txt
# Legaturi: outputs/plan_v2.csv -> outputs/grocery_list.txt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True, help="outputs/plan_v2.csv")
    ap.add_argument("--foods", required=True, help="data/foods_enriched.parquet")
    ap.add_argument("--out", default="outputs/grocery_list.txt")
    ap.add_argument("--days", type=int, default=0, help="if >0, only aggregate rows with day <= this")
    ap.add_argument("--group_by", default="main_group", help="grouping column (e.g., main_group)")
    ap.add_argument("--pack_defaults", default="configs/pack_defaults.yaml")
    ap.add_argument("--overage_warn_frac", type=float, default=0.10)
    ap.add_argument("--perishable_short_days", type=int, default=5)  # warning pentru produse perisabile (cumpara aproape de utilizare)
    args = ap.parse_args()

    dfp = pd.read_csv(args.plan)  # plan_v2.csv (portii pe masa)
    if args.days and "day" in dfp.columns:
        dfp = dfp[dfp["day"] <= int(args.days)]  # daca ai multi-day, poti limita la primele N zile

    dff = load_foods(args.foods)
    defaults = load_pack_defaults(args.pack_defaults)

    # clean keys
    if "uid" in dff.columns:
        dff["uid"] = dff["uid"].astype(str)
    if "name_core" in dff.columns:
        dff["name_core"] = dff["name_core"].astype(str)

    # explode plan to uid + portion_g
    rows = []
    for r in dfp.itertuples(index=False):  # explode plan: 3 roluri -> linii uid+portion_g
        for role, uid_col, g_col in [
            ("protein", "protein_uid", "protein_portion_g"),
            ("side_carb", "side_carb_uid", "side_carb_portion_g"),
            ("side_veg", "side_veg_uid", "side_veg_portion_g"),
        ]:
            uid = getattr(r, uid_col, None)
            g = getattr(r, g_col, 0.0)
            if pd.notna(uid) and str(uid) != "" and pd.to_numeric(g, errors="coerce") > 0:
                rows.append({"uid": str(uid), "portion_g": float(g), "role": role})
    if not rows:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text("[empty] no items", encoding="utf-8")
        print(f"[OK] wrote empty grocery list -> {args.out}")
        return

    agg = pd.DataFrame(rows).groupby("uid", as_index=False)["portion_g"].sum()  # agregare cantitati per uid

    # collect tag columns available
    tag_cols = [c for c in dff.columns if c.startswith("tag_")]  # includem tag_* in merged ca sa putem decide pack policy

    # minimal columns we want to join
    cols_needed = [
        "uid",
        "name_core",
        args.group_by,
        "pack_unit",
        "pack_size",
        "units_per_pack",
        "main_group",
        "veg_bucket",
        # optional: shelf_stable_days can come from overrides; if missing, resolver provides by rule
        "shelf_stable_days",
    ] + tag_cols
    for c in cols_needed:
        if c not in dff.columns:
            dff[c] = None

    merged = agg.merge(dff[cols_needed], on="uid", how="left")  # join: uid -> nume/categorie/tag-uri/pack overrides

    # group by requested category
    grp_col = args.group_by
    merged[grp_col] = merged[grp_col].fillna("Other")

    groups = {}
    for r in merged.itertuples(index=False):
        cat = getattr(r, grp_col, "Other") or "Other"
        name, total_g, full_line = pack_line(
            r._asdict(), defaults, args.overage_warn_frac, args.perishable_short_days  # warning pentru produse perisabile (cumpara aproape de utilizare)
        )
        groups.setdefault(cat, []).append((name, total_g, full_line))  # colectam pe categorii pentru output final

    def sort_key_cat(c):  # ordonare categorii (optional; cosmetica)
        # put common categories early; adjust as needed
        pri = {
            "vegetables": 0,
            "fruits": 1,
            "meat": 2,
            "fish": 3,
            "grains": 4,
            "pasta": 5,
            "rice": 6,
            "dairy": 7,
            "frozen": 8,
        }
        return (pri.get(c.lower(), 99), c.lower())

    def sort_key_item(t):
        # sort by total desc, then name
        return (-t[1], t[0].lower())

    lines = []
    lines.append("=== GROCERY LIST ===")
    if args.days:
        lines.append(f"(aggregated for days 1..{args.days})")
    lines.append("")

    for cat in sorted(groups.keys(), key=sort_key_cat):  # ordonare categorii (optional; cosmetica)
        lines.append(f"[{cat}]")
        for name, total_g, full_line in sorted(groups[cat], key=sort_key_item):
            lines.append(f" - {full_line}")
        lines.append("")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")  # scriere outputs/grocery_list.txt
    print(f"[OK] wrote grocery list -> {out}")


if __name__ == "__main__":
    main()
###############################################################################
# OBSERVATII / POSIBILE OPTIMIZARI (NU SCHIMBA ACUM, DOAR DE TINUT MINTE)
#
# 1) pack_defaults.yaml devine "sursa de adevar"
#    - Pe masura ce adaugi reguli, tine-le cat mai generale (tag-uri + main_group).
#    - Evita reguli foarte specifice pe name_regex (fragile la denumiri).
#
# 2) Unitati g vs ml
#    - Acum ghicim ml daca pack_unit==ml sau tag_drink. Pentru alimente semi-lichide
#      (ex. iaurt) poate vrei reguli dedicate in YAML.
#
# 3) units_per_pack
#    - E suportat ca afisare (ex. 6 x 125g), dar selectia pachetelor ramane pe pack_size.
#      Daca vrei un model mai realist, poti selecta intre pachete discrete (6x125 vs 4x150 etc.).
#
# 4) Perisabilitate
#    - shelf_stable_days vine din override sau pack_defaults. Daca lipseste, e 30 by default.
#      Pentru acuratete, vei vrea o schema minima per main_group (ex. fresh meat 2-3 zile).
#
# 5) Agregare pe uid
#    - E robust. Daca vrei lista mai 'umana', poti grupa si pe role (protein vs carb vs veg)
#      sau poti pastra 2 sectiuni: "must-have" vs "optional".
#
# 6) Cantitati in plan sunt in grame (portion_g)
#    - Pentru lichide, portion_g poate fi de fapt ml in dataset. In prezent tratam tot ca float
#      si unitatea e decisa de pack policy. Daca vrei strict, adauga un camp density/unit_type.
#
# 7) Dependenta de coloane tag_*
#    - Daca foods nu contine tag-uri, resolver tot functioneaza (fallback), dar mai slab.
###############################################################################
