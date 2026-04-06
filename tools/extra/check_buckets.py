import argparse
import pandas as pd
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--foods", default="data/foods_enriched.parquet")
ap.add_argument("--out", default="outputs/buckets_unique.txt")
args = ap.parse_args()

p = Path(args.foods)
if p.suffix == ".parquet":
    df = pd.read_parquet(p)
else:
    df = pd.read_csv(p)

lines = []
for col in ["protein_bucket", "carb_bucket", "veg_bucket"]:
    vals = sorted({str(v) for v in df[col].dropna().unique()})
    lines.append(f"{col} ({len(vals)}):\n" + "\n".join(f" - {v}" for v in vals))

Path(args.out).parent.mkdir(parents=True, exist_ok=True)
Path(args.out).write_text("\n\n".join(lines), encoding="utf-8")
print("\n\n".join(lines))
print(f"\nWritten: {args.out}")
