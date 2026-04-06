# src/visualize_assoc.py
import argparse, yaml, pandas as pd, numpy as np
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def heatmap(df, title, out_png):
    fig, ax = plt.subplots(figsize=(max(6, 0.6*len(df.columns)), max(5, 0.6*len(df.index))))
    im = ax.imshow(df.values, vmin=0, vmax=1, aspect="auto")
    ax.set_xticks(np.arange(len(df.columns))); ax.set_xticklabels(df.columns, rotation=45, ha="right")
    ax.set_yticks(np.arange(len(df.index)));  ax.set_yticklabels(df.index)
    for i in range(df.shape[0]):
        for j in range(df.shape[1]):
            v = df.iat[i,j]
            if pd.notna(v):
                ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=8,
                        color=("white" if v>0.6 else "black"))
    ax.set_title(title)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout(); fig.savefig(out_png, dpi=150); plt.close(fig)

def load_assoc_matrix(rules: dict) -> pd.DataFrame:
    """
    Suportă două formate:
      A) rules['assoc_buckets']                        # protein_bucket -> {carb_bucket: score}
      B) rules['priors']['pairwise_compat']['protein_bucket']  # idem
    Returnează un DataFrame (protein_bucket x carb_bucket) cu valori în [0,1].
    """
    AB = rules.get("assoc_buckets", None)
    if isinstance(AB, dict) and AB:
        norm = {str(pb): {str(cb): float(v) for cb, v in (m or {}).items()}
                for pb, m in AB.items() if isinstance(m, dict)}
        if norm:
            rows = sorted(norm.keys())
            cols = sorted({c for m in norm.values() for c in m.keys()})
            M = pd.DataFrame(index=rows, columns=cols, dtype=float)
            for r in rows:
                for c, v in norm[r].items():
                    M.loc[r, c] = float(v)
            return M.fillna(0.0)

    # fallback: priors.pairwise_compat.protein_bucket
    Pri = rules.get("priors", {}) or {}
    PC  = (Pri.get("pairwise_compat", {}) or {}).get("protein_bucket", {}) or {}
    if isinstance(PC, dict) and PC:
        rows = sorted(PC.keys())
        cols = sorted({c for m in PC.values() if isinstance(m, dict) for c in m.keys()})
        M = pd.DataFrame(index=rows, columns=cols, dtype=float)
        for r in rows:
            if not isinstance(PC[r], dict):
                continue
            for c, v in PC[r].items():
                M.loc[r, c] = float(v)
        return M.fillna(0.0)

    raise SystemExit("Nu am găsit nici assoc_buckets, nici priors.pairwise_compat.protein_bucket în YAML.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default="configs/culinary_rules.yaml")
    ap.add_argument("--outdir", default="outputs")
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    with open(args.rules, "r", encoding="utf-8") as f:
        rules = yaml.safe_load(f)

    M = load_assoc_matrix(rules)
    M.to_csv(outdir / "assoc_matrix.csv")
    heatmap(M, "Association (protein_bucket → carb_bucket)", outdir / "assoc_heatmap.png")
    print(f"[ok] scris: {outdir/'assoc_matrix.csv'} și {outdir/'assoc_heatmap.png'}")

if __name__ == "__main__":
    main()
