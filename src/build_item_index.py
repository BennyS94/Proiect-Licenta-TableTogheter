###########################################################################
# FISIER: src/build_item_index.py
#
# SCOP
#   Construieste indexul de itemi pentru substitutii (swap similar).
#   Converteste macro-urile per 100g/ml in macro-uri per 100 kcal si apoi
#   standardizeaza (z-score) aceste feature-uri, obtinand coloane `z_*`.
#
#   Output-ul (item_index.parquet) este consumat de build_substitutions.py,
#   care calculeaza cosine similarity intre vectorii z_*.
#
# INTRARI (CLI)
#   --in   data/foods_enriched_min.csv.gz (sau un .parquet echivalent)
#   --out  data/item_index.parquet
#
# IESIRI
#   --out contine (minim):
#     - uid, name_core
#     - role_protein, role_side_carb, role_side_veg
#     - protein_bucket, carb_bucket, veg_bucket
#     - z_protein_g_100kcal, z_carb_g_100kcal, z_fat_g_100kcal, z_sugars_g_100kcal,
#       z_fibres_g_100kcal, z_salt_g_100kcal
#
# LEGATURI
#   - src/build_substitutions.py: foloseste item_index.parquet pentru similitudini
#   - src/enrich_foods.py: asigura coloanele canonice (kcal_sanitized, *_g_100g_ml, *_bucket)
#
# NOTE
#   - Comentarii in romana fara diacritice.
#   - Nu schimbam functionalitatea; doar documentam clar.
###########################################################################
# src/build_item_index.py
import argparse, pandas as pd, numpy as np
from pathlib import Path

FEAT_RAW = ["protein_g_100g_ml","carbohydrate_g_100g_ml","fat_g_100g_ml",  # coloane canonice per 100g/ml; folosite pentru calcul per 100 kcal
            "sugars_g_100g_ml","fibres_g_100g_ml","salt_g_100g_ml","kcal_sanitized"]

###########################################################################
# read_any
# Ce face: citeste un fisier .parquet sau .csv(.gz) intr-un DataFrame
# Legaturi: pandas.read_parquet / pandas.read_csv

def read_any(path):
    p = Path(path)
    if p.suffix == ".parquet":
        return pd.read_parquet(p)
    return pd.read_csv(p)

###########################################################################
# main
# Ce face: CLI: citeste foods, calculeaza features/100kcal, z-score, scrie item_index.parquet
# Legaturi: data/foods_enriched_min.csv.gz -> data/item_index.parquet

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/foods_enriched_min.csv.gz")
    ap.add_argument("--out", default="data/item_index.parquet")
    args = ap.parse_args()

    df = read_any(args.inp).copy()
    df = df[pd.to_numeric(df["kcal_sanitized"], errors="coerce").fillna(0) > 0]  # pastram doar randuri cu kcal valide (>0)

    kcal = pd.to_numeric(df["kcal_sanitized"], errors="coerce").fillna(1.0).replace(0, 1.0)  # evita impartire la 0 pentru sigma=0 sau kcal=0
    factor = 100.0 / kcal  # transformare: per100g -> per100kcal (scale cu 100/kcal)

    def f(col): return pd.to_numeric(df[col], errors="coerce").fillna(0.0) * factor  # helper: ia col numeric, fillna(0), scale la 100 kcal
    feats = pd.DataFrame({
        "protein_g_100kcal": f("protein_g_100g_ml"),
        "carb_g_100kcal":    f("carbohydrate_g_100g_ml"),
        "fat_g_100kcal":     f("fat_g_100g_ml"),
        "sugars_g_100kcal":  f("sugars_g_100g_ml"),
        "fibres_g_100kcal":  f("fibres_g_100g_ml"),
        "salt_g_100kcal":    f("salt_g_100g_ml"),
    })

    mu, sigma = feats.mean(0), feats.std(0).replace(0, 1.0)  # z-score: standardizare features (media 0, dev std 1)
    z = (feats - mu) / sigma

    out = pd.DataFrame({
        "uid": df["uid"], "name_core": df["name_core"],
        "role_protein": df.get("role_protein", 0),
        "role_side_carb": df.get("role_side_carb", 0),
        "role_side_veg": df.get("role_side_veg", 0),
        "protein_bucket": df.get("protein_bucket",""),
        "carb_bucket": df.get("carb_bucket",""),
        "veg_bucket": df.get("veg_bucket",""),
    })
    for c in z.columns:
        out["z_"+c] = z[c].astype(float)  # prefix z_ ca semnal clar: feature standardizat

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.out, index=False)  # output compact; bun pentru downstream (substitutions)
    print(f"[item_index] rows={len(out)} -> {args.out}")

if __name__ == "__main__":
    main()
###############################################################################
# OBSERVATII / POSIBILE OPTIMIZARI (NU SCHIMBA ACUM, DOAR DE TINUT MINTE)
#
# 1) Stabilitatea standardizarii (mu/sigma)
#    - Acum mu si sigma se calculeaza pe datasetul curent (din --in).
#    - Daca datasetul se schimba (alt subset), distributiile z_* se schimba.
#    - Pentru stabilitate cross-run, poti salva mu/sigma in `data/item_index_stats.json`
#      si sa le refolosesti (train-time stats). Apoi regenerarea edges devine comparabila in timp.
#
# 2) Feature weighting pe rol
#    - Acum toate features au greutate egala in cosine (z-score uniform).
#    - Pentru protein, poate vrei sa maresti importanta `protein_g_100kcal`.
#      Pentru side_carb, `carb_g_100kcal`. Pentru side_veg, `fibres_g_100kcal`.
#    - Implementare simpla: inmultesti unele coloane z_* cu un factor (ex. 1.2) inainte de cosine.
#
# 3) Outlier handling
#    - Unele alimente au extreme (ex. foarte multa sare/100kcal). Z-score le amplifica.
#    - Optional: winsorize/clip per feature (ex. la percentila 1 si 99) inainte de z-score.
#
# 4) Kcal foarte mic (factor mare)
#    - Cand kcal_sanitized e foarte mic, factor=100/kcal devine foarte mare => macro/100kcal explodeaza.
#    - Ai deja un guard: kcal=fillna(1.0). Dar pentru valori mici (ex. <10 kcal), poti clipa
#      kcal la un minim (ex. 10) pentru a evita rezultate instabile.
#
# 5) Set de features
#    - Poti adauga ulterior: sat_fat_g_100kcal, protein_density, sugar_ratio, etc.
#    - Dar orice feature nou cere regenerare edges si actualizare documentatie.
###############################################################################
