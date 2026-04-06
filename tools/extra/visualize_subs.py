# src/visualize_subs.py
import argparse, pandas as pd, numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

def load_foods(path):
    p = Path(path)
    return pd.read_parquet(p) if p.suffix==".parquet" else pd.read_csv(p)

def role_cols(role):
    if role=="protein":   return "role_protein","protein_bucket"
    if role=="side_carb": return "role_side_carb","carb_bucket"
    if role=="side_veg":  return "role_side_veg","veg_bucket"
    raise ValueError("role must be one of: protein | side_carb | side_veg")

def ensure_outdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def heatmap(matrix_df, title, out_png):
    fig, ax = plt.subplots(figsize=(max(6, 0.6*len(matrix_df.columns)), max(5, 0.6*len(matrix_df.index))))
    im = ax.imshow(matrix_df.values, aspect="auto")
    ax.set_xticks(np.arange(len(matrix_df.columns)))
    ax.set_xticklabels(matrix_df.columns, rotation=45, ha="right")
    ax.set_yticks(np.arange(len(matrix_df.index)))
    ax.set_yticklabels(matrix_df.index)
    for i in range(matrix_df.shape[0]):
        for j in range(matrix_df.shape[1]):
            v = matrix_df.iat[i,j]
            if pd.notna(v):
                ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=8, color="white" if im.norm(v) > 0.6 else "black")
    ax.set_title(title)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)

def bar_alternatives(df_subs, foods, uid, out_png, topk=15):
    m = df_subs[df_subs["src_uid"]==uid].sort_values("sim", ascending=False).head(topk)
    if m.empty:
        return False
    name_map = foods.set_index("uid")["name_core"].to_dict()
    labels = [name_map.get(x, x) for x in m["dst_uid"]]
    vals = m["sim"].astype(float).values
    fig, ax = plt.subplots(figsize=(8, max(4, 0.4*len(labels))))
    ax.barh(range(len(labels)), vals)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("similarity (cosine)")
    ax.set_title(f"Alternatives for {name_map.get(uid, uid)}")
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    return True

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subs", default="data/substitution_edges.csv.gz")
    ap.add_argument("--foods", default="data/foods_enriched.parquet")
    ap.add_argument("--role", choices=["protein","side_carb","side_veg"], default=None, help="filtrează la nivel de rol (opțional)")
    ap.add_argument("--uid", default=None, help="desenează bar chart cu alternativele pentru acest uid (opțional)")
    ap.add_argument("--outdir", default="outputs")
    args = ap.parse_args()

    outdir = Path(args.outdir); ensure_outdir(outdir)

    foods = load_foods(args.foods)
    subs  = pd.read_csv(args.subs)
    need = {"uid","name_core","role_protein","role_side_carb","role_side_veg","protein_bucket","carb_bucket","veg_bucket"}
    missing = need - set(foods.columns)
    if missing:
        raise SystemExit(f"foods missing columns: {missing}")

    # 1) coverage per src (cate alternative are fiecare)
    cov = subs.groupby("src_uid", as_index=False).agg(n_alts=("dst_uid","count"), sim_mean=("sim","mean"))
    cov = cov.merge(foods[["uid","name_core","protein_bucket","carb_bucket","veg_bucket","role_protein","role_side_carb","role_side_veg"]],
                    left_on="src_uid", right_on="uid", how="left").drop(columns=["uid"])
    cov_out = outdir / "subs_coverage.csv"
    cov.to_csv(cov_out, index=False)

    # 2) bucket->bucket mean(sim) per rol (heatmap)
    roles = [args.role] if args.role else ["protein","side_carb","side_veg"]
    for role in roles:
        rcol, bcol = role_cols(role)
        src = foods[foods[rcol]==1][["uid", bcol]].rename(columns={bcol:"src_bucket"})
        dst = foods[foods[rcol]==1][["uid", bcol]].rename(columns={bcol:"dst_bucket"})
        e = subs.merge(src, left_on="src_uid", right_on="uid", how="inner") \
                .merge(dst, left_on="dst_uid", right_on="uid", how="inner")
        mat = e.groupby(["src_bucket","dst_bucket"])["sim"].mean().unstack(fill_value=np.nan)
        mat.to_csv(outdir / f"subs_bucket_matrix_{role}.csv")
        heatmap(mat, f"Substitutions mean(sim) — {role}", outdir / f"subs_bucket_heatmap_{role}.png")

    # 3) histogram numar alternative pe item (pentru QA rapid)
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(6,4))
    ax.hist(cov["n_alts"], bins=30)
    ax.set_title("Distribution: number of alternatives per item (all roles)")
    ax.set_xlabel("#alts"); ax.set_ylabel("count")
    fig.tight_layout(); fig.savefig(outdir / "subs_n_alts_hist.png", dpi=150); plt.close(fig)

    # 4) daca s-a dat --uid: bar chart cu alternativele lui
    if args.uid:
        ok = bar_alternatives(subs, foods, args.uid, outdir / f"subs_alts_{args.uid}.png")
        if not ok:
            print(f"[warn] no edges for uid={args.uid}")

    print(f"[ok] wrote: {cov_out} and PNG/CSV heatmaps in {outdir}")

if __name__ == "__main__":
    main()
