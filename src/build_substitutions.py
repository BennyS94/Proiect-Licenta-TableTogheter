###########################################################################
# FISIER: src/build_substitutions.py
#
# SCOP
#   Genereaza substitutii "swap similar" intre itemi (directionat top-K).
#   Output-ul este folosit de generator_v2.py pentru campurile alt_*.
#
# IDEA
#   - Fiecare item are un vector de features standardizat la 100 kcal (z_*).
#   - Similaritatea este cosine(A,B) intre vectori.
#   - Pentru fiecare src_uid pastram top-K destinatii dst_uid cu sim >= min_sim.
#   - Edges sunt directionate: A->B exista daca B e in top-K pentru A.
#
# INTRARI
#   --index: data/item_index.parquet (real: contine uid, role_*, *_bucket, FEATZ z_*)
#   --out:   data/substitution_edges.csv.gz
#   --topk_*: cate muchii per src_uid (pe rol)
#   --min_sim: prag minim similaritate
#
# IESIRI
#   CSV(.gz) cu coloane: src_uid, dst_uid, sim
#
# LEGATURI
#   - build_item_index.py produce item_index.parquet (features z_* la 100 kcal)
#   - generator_v2.py consuma substitution_edges.csv.gz (alt_protein/alt_carb/alt_veg)
#
# NOTE
#   Comentarii in romana fara diacritice.
###########################################################################
# src/build_substitutions.py
import argparse, pandas as pd, numpy as np
from pathlib import Path

FEATZ = ["z_protein_g_100kcal","z_carb_g_100kcal","z_fat_g_100kcal",  # feature-uri standardizate (z_*) pe baza la 100 kcal
         "z_fibres_g_100kcal","z_sugars_g_100kcal","z_salt_g_100kcal"]

# topk per rol (poti mari ulterior)
ROLE_TOPK = {"protein": 25, "side_carb": 40, "side_veg": 25}  # topk default per rol (override prin CLI)

# familii de bucket (extinde dupa nevoie)
FAMILIES = {  # familii de bucket-uri: extindem cautarea cand bucket-ul e prea mic
    "protein": {
        "poultry": ["poultry"],
        "fish_white": ["fish_white"],
        "fish_fatty": ["fish_fatty"],
        "eggs": ["eggs"],
        "veggie": ["veggie","legume_pulse","tofu_tempeh"]
    },
    "side_carb": {
        "grains": ["grains","rice","quinoa","bulgur","maize","oat"],
        "potatoes": ["potatoes","sweet_potatoes"],
        "pasta_noodles": ["pasta_noodles","whole_pasta_noodles"],
        "bakery": ["bakery","crackers_whole","bread_whole"]
    },
    "side_veg": {
        "salad_like": ["salad_like","raw_veg","crudites"],
        "cooked_veg": ["cooked_veg","roasted_veg","steamed_veg"]
    }
}

EXCLUDE_TAGS = {"dessert_like","heavy_sauce","fried_or_chips","trans_risk"}  # tag-uri pe care nu vrem substitutii (zgomot/ingrediente/deserturi)

###########################################################################
# cosine_sim
# Ce face: cosine similarity intre doua matrici A,B (randuri = itemi, coloane = features)
# Legaturi: numpy.linalg.norm

def cosine_sim(A, B):
    Ad = np.linalg.norm(A, axis=1, keepdims=True) + 1e-9  # norma L2; +epsilon ca sa evitam div/0
    Bd = np.linalg.norm(B, axis=1, keepdims=True) + 1e-9  # norma L2; +epsilon ca sa evitam div/0
    return (A @ B.T) / (Ad @ Bd.T)

###########################################################################
# mask_clean
# Ce face: filtru de curatenie: scoate tag-uri nedorite + exclude global (daca exista)
# Legaturi: EXCLUDE_TAGS; coloane tag_*

def mask_clean(df):
    m = pd.Series(True, index=df.index)
    for t in EXCLUDE_TAGS:  # tag-uri pe care nu vrem substitutii (zgomot/ingrediente/deserturi)
        col = f"tag_{t}" if f"tag_{t}" in df.columns else t
        if col in df.columns:
            m &= (df[col].fillna(0).astype(int) == 0)
    # exclude globale (daca ai o coloana 'exclude')
    if "exclude" in df.columns:
        m &= (df["exclude"].fillna(0).astype(int) == 0)
    return m

###########################################################################
# subgraph
# Ce face: alege subgraful (grupul) pentru un bucket: same-bucket, apoi familie (siblings), apoi fallback pe rol
# Legaturi: FAMILIES; *_bucket

def subgraph(df, role, bucket, min_size=2):
    """Ia grupul primar (same-bucket), apoi largeste cu 'siblings' daca e prea mic."""
    if role == "protein":
        bc = "protein_bucket"
        rc = "role_protein"
    elif role == "side_carb":
        bc = "carb_bucket"
        rc = "role_side_carb"
    else:
        bc = "veg_bucket"
        rc = "role_side_veg"

    base = df[(df[rc]==1) & mask_clean(df)].copy()
    fam = FAMILIES.get(role, {})  # familii de bucket-uri: extindem cautarea cand bucket-ul e prea mic
    family = fam.get(bucket, [bucket]) if bucket else None

    if bucket:
        g = base[base[bc].isin([bucket])]
        if len(g) >= min_size:
            return g
        # extinde la siblings
        if family:
            g2 = base[base[bc].isin(family)]
            if len(g2) >= min_size:
                return g2
    # fallback: tot rolul
    g3 = base
    return g3 if len(g3) >= min_size else pd.DataFrame(columns=base.columns)

###########################################################################
# topk_edges_group
# Ce face: pentru un grup g, genereaza edges src->dst cu topk si min_sim
# Legaturi: np.argpartition; cosine_sim

def topk_edges_group(g, topk, min_sim=0.30):
    if len(g) < 2: return []
    X = g[FEATZ].to_numpy(dtype=float)
    S = cosine_sim(X, X)
    np.fill_diagonal(S, -1.0)  # nu permitem muchie catre sine (self-loop)
    uids = g["uid"].tolist()
    out = []
    for i, u in enumerate(uids):
        sims = S[i]
        k = min(topk, len(sims)-1)
        if k <= 0: continue
        idx = np.argpartition(-sims, range(k))[:k]  # top-k rapid (nu sorteaza complet); ordinea poate fi amestecata
        for j in idx:
            s = float(sims[j])
            if s < min_sim: continue  # prag minim similaritate; sub prag nu pastram edge
            out.append((u, uids[j], s))
    return out

###########################################################################
# build_role_edges
# Ce face: itereaza bucket-urile unui rol si concateneaza edges pe fiecare bucket
# Legaturi: subgraph; topk_edges_group

def build_role_edges(df, role, topk):
    if role == "protein":
        bc = "protein_bucket"
        rc = "role_protein"
    elif role == "side_carb":
        bc = "carb_bucket"
        rc = "role_side_carb"
    else:
        bc = "veg_bucket"
        rc = "role_side_veg"

    rows = []
    buckets = sorted(df[bc].dropna().unique().tolist())  # ATENTIE: bucket gol/"" nu intra aici ca sursa (posibil TODO)
    for b in buckets:
        g = subgraph(df, role, b, min_size=2)
        if len(g) < 2: continue
        rows.extend(topk_edges_group(g, topk))
    return rows

###########################################################################
# main
# Ce face: CLI: citeste index, construieste edges pe roluri, scrie CSV.gz + sumar
# Legaturi: pandas; argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", default="data/item_index.parquet")
    ap.add_argument("--out", default="data/substitution_edges.csv.gz")
    ap.add_argument("--topk_pro", type=int, default=ROLE_TOPK["protein"])  # topk default per rol (override prin CLI)
    ap.add_argument("--topk_carb", type=int, default=ROLE_TOPK["side_carb"])  # topk default per rol (override prin CLI)
    ap.add_argument("--topk_veg", type=int, default=ROLE_TOPK["side_veg"])  # topk default per rol (override prin CLI)
    ap.add_argument("--min_sim", type=float, default=0.30)
    args = ap.parse_args()

    df = pd.read_parquet(args.index)
    need = ["uid","name_core","role_protein","role_side_carb","role_side_veg",
            "protein_bucket","carb_bucket","veg_bucket"] + FEATZ
    df = df[need].copy()  # pastram doar coloanele strict necesare; evita NaN/coliziuni

    edges = []
    edges += build_role_edges(df, "protein",   args.topk_pro)
    edges += build_role_edges(df, "side_carb", args.topk_carb)
    edges += build_role_edges(df, "side_veg",  args.topk_veg)

    out = pd.DataFrame(edges, columns=["src_uid","dst_uid","sim"])
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, compression="gzip")  # output comprimat; fisier mai mic in repo
    # mic rezumat
    n = len(out)
    n_pro = (out["src_uid"].isin(df.loc[df["role_protein"]==1,"uid"])).sum()
    n_carb = (out["src_uid"].isin(df.loc[df["role_side_carb"]==1,"uid"])).sum()
    n_veg = (out["src_uid"].isin(df.loc[df["role_side_veg"]==1,"uid"])).sum()
    print(f"[substitutions] edges={n} (src: protein~{n_pro}, carb~{n_carb}, veg~{n_veg}) -> {args.out}")

if __name__ == "__main__":
    main()
###############################################################################
# OBSERVATII / POSIBILE OPTIMIZARI (NU SCHIMBA ACUM, DOAR DE TINUT MINTE)
#
# 1) BUG: --min_sim din CLI nu este propagat in build_role_edges()/topk_edges_group()
#    - Acum topk_edges_group() foloseste default min_sim=0.30, indiferent de args.min_sim.
#    - Fix: treci min_sim ca parametru: build_role_edges(..., min_sim=args.min_sim) si apoi
#           topk_edges_group(g, topk, min_sim=min_sim).
#
# 2) Stabilitate si ordonare top-K
#    - np.argpartition returneaza un set corect de indici top-k, dar nesortat.
#    - Pentru debug/consistenta, poti ordona local: idx = idx[np.argsort(-sims[idx])].
#
# 3) Acoperire pentru bucket gol/necunoscut
#    - buckets = df[bc].dropna().unique() ignora bucket=="" (string gol) si NaN.
#    - Daca ai multe itemuri fara bucket, ele nu vor avea alternative ca sursa.
#    - Optiune: trateaza explicit bucket=="" ca un bucket separat sau foloseste taxonomie ca fallback.
#
# 4) Validare date FEATZ
#    - Daca FEATZ contine NaN, cosine_sim poate produce NaN. Recomand fillna(0) pe FEATZ.
#    - Optional: assert ca toate coloanele FEATZ exista si sunt numerice.
#
# 5) Filtru de curatenie (mask_clean)
#    - EXCLUDE_TAGS este hard-codat. Mai flexibil: muta in YAML (configs) si citeste de acolo.
#
# 6) FAMILIES hard-codat
#    - E util, dar ar fi mai usor de intretinut daca e mutat in YAML (substitutions.families).
#
# 7) Complexitate O(n^2) pe grupuri mari
#    - cosine_sim(X,X) are cost O(n^2). Pentru grupuri foarte mari, optimizeaza:
#      - limiteaza dimensiunea grupului (sample), sau
#      - foloseste ANN (faiss) pentru vecini aproximativi, sau
#      - sparge pe sub-bucket / taxonomie.
#
# 8) Similaritati negative
#    - Dupa standardizare z_*, cosine poate deveni negativ. Acum min_sim=0.30 filtreaza implicit.
#    - Daca vrei, poti face clamp la 0: sim = max(sim, 0.0) (dar documenteaza alegerea).
###############################################################################
