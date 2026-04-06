# src/debug_enriched.py
import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--foods", default="data/foods_enriched.parquet")
    ap.add_argument("--fallback", default="data/foods_enriched_min.csv.gz")
    ap.add_argument("--out", default="outputs/enriched_debug.txt")
    args = ap.parse_args()

    p = Path(args.foods)
    if p.suffix == ".parquet" and p.exists():
        try:
            df = pd.read_parquet(p)
        except Exception as e:
            print("Parquet read failed, falling back to CSV:", e)
            df = pd.read_csv(args.fallback)
    elif p.exists():
        df = pd.read_csv(p)
    else:
        df = pd.read_csv(args.fallback)

    lines = []
    lines.append(f"rows={len(df)}")
    cols = list(df.columns)
    lines.append("COLUMNS:")
    lines.append(", ".join(cols))

    carb_cols = [c for c in cols if "carb" in c.lower() or "carbo" in c.lower()]
    lines.append("\nCarb-ish columns:")
    lines.append(", ".join(carb_cols) if carb_cols else "(none)")

    for kw in ["rice", "pasta", "potato"]:
        sub = df[df["name_core"].str.contains(kw, case=False, na=False)].head(3)
        lines.append(f"\nSample rows for '{kw}':")
        for _, r in sub.iterrows():
            vals = {c: r.get(c) for c in carb_cols}
            lines.append(f" - {r.get('name_core')}: {vals}")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text("\n".join(lines), encoding="utf-8")
    print(f"OK -> {args.out}")

if __name__ == "__main__":
    main()
