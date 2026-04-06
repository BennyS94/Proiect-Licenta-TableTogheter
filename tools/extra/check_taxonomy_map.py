import pandas as pd, argparse, json
from pathlib import Path
try:
    from src.ontology import load_taxonomy, node_for_food
except Exception:
    from ontology import load_taxonomy, node_for_food

ap = argparse.ArgumentParser()
ap.add_argument("--foods", required=True)
ap.add_argument("--taxonomy", default="configs/taxonomy.yaml")
ap.add_argument("--out_csv", default="outputs/taxonomy_coverage.csv")
args = ap.parse_args()

df = pd.read_parquet(args.foods) if args.foods.endswith(".parquet") else pd.read_csv(args.foods)
tax = load_taxonomy(args.taxonomy)

def mapped(role):
    nodes = []
    miss = []
    for _, r in df.iterrows():
        n = node_for_food(r, role, tax)
        if n: nodes.append(n)
        else: miss.append(r.get("name_core","") or r.get("name",""))
    return nodes, miss

nodes_p, miss_p = mapped("protein")
nodes_c, miss_c = mapped("side_carb")

cov_p = 100.0 * (len(nodes_p) / len(df))
cov_c = 100.0 * (len(nodes_c) / len(df))
Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
pd.DataFrame({
    "metric":["coverage_protein_%","coverage_side_carb_%","miss_protein_cnt","miss_side_carb_cnt"],
    "value":[cov_p, cov_c, len(miss_p), len(miss_c)]
}).to_csv(args.out_csv, index=False)

print(f"[taxonomy] coverage Ps: {cov_p:.2f}% | Sc: {cov_c:.2f}%")
if miss_p[:5] or miss_c[:5]:
    print("[taxonomy] sample missing:", (miss_p[:3] + miss_c[:3])[:6])
