from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.generator_v1.recipe_similarity import (
    build_recipe_similarity_features,
    find_similar_recipes,
    load_recipe_similarity_source,
)


DATASET_PROFILE = "v1_2_demo_final"
AUDIT_DIR = PROJECT_ROOT / "data/recipesdb/audit"
NEIGHBORS_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_neighbors.csv"
EXAMPLES_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_neighbor_examples.txt"
SUMMARY_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_neighbor_summary.txt"


def main() -> int:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    source = load_recipe_similarity_source(DATASET_PROFILE)
    features = build_recipe_similarity_features(
        source["recipes"],
        source["nutrition"],
        source["ingredients"],
    )
    seeds = _select_seed_recipes(features)
    rows = []
    for seed in seeds:
        neighbors = find_similar_recipes(
            seed["recipe_id"],
            features,
            top_k=10,
            filters={"same_slot": True, "active_only": True},
        )
        for rank, neighbor in enumerate(neighbors, start=1):
            rows.append(
                {
                    "seed_label": seed["seed_label"],
                    "rank": rank,
                    **neighbor,
                }
            )

    neighbors_df = pd.DataFrame(rows)
    neighbors_df.to_csv(NEIGHBORS_PATH, index=False)
    EXAMPLES_PATH.write_text(_example_text(seeds, neighbors_df), encoding="utf-8")

    warnings = Counter()
    if not neighbors_df.empty and "warnings" in neighbors_df.columns:
        for value in neighbors_df["warnings"].fillna("").astype(str):
            for warning in value.split(";"):
                warning = warning.strip()
                if warning:
                    warnings[warning] += 1

    score_median = (
        float(neighbors_df["similarity_score"].median())
        if not neighbors_df.empty
        else 0.0
    )
    good_match_count = int(neighbors_df["similarity_score"].ge(0.70).sum()) if not neighbors_df.empty else 0
    questionable_count = int(neighbors_df["warnings"].fillna("").astype(str).str.len().gt(0).sum()) if not neighbors_df.empty else 0
    summary_lines = [
        "KNN recipe similarity v1 neighbor summary",
        "status=ok",
        f"dataset_profile={DATASET_PROFILE}",
        f"seed_count={len(seeds)}",
        f"neighbor_rows={len(neighbors_df)}",
        f"median_similarity_score={score_median:.4f}",
        f"good_match_count_score_ge_0_70={good_match_count}",
        f"questionable_match_count_with_warnings={questionable_count}",
        "common_failure_patterns=" + _format_counter(warnings),
        f"neighbors_csv={NEIGHBORS_PATH.as_posix()}",
        f"examples_txt={EXAMPLES_PATH.as_posix()}",
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0


def _select_seed_recipes(features: pd.DataFrame) -> list[dict[str, str]]:
    seeds: list[dict[str, str]] = []
    active = features.loc[features["is_active_bool"]].copy()
    slot_targets = [
        ("breakfast", "breakfast"),
        ("lunch", "lunch"),
        ("dinner", "dinner"),
        ("snack", "snack"),
    ]
    for label, slot in slot_targets:
        row = _first_match(active, lambda frame: frame["allowed_slots_text"].str.contains(slot, na=False))
        if row:
            seeds.append({"seed_label": label, **row})

    topical_targets = [
        ("chicken", "chicken"),
        ("egg", "egg"),
        ("pasta_or_rice", "pasta|rice|noodle|lasagna"),
        ("vegetarian_or_legume", "vegetarian|bean|lentil|tofu"),
    ]
    for label, pattern in topical_targets:
        row = _first_match(
            active,
            lambda frame, pattern=pattern: _search_text(frame).str.contains(pattern, regex=True, na=False),
            existing_ids={item["recipe_id"] for item in seeds},
        )
        if row:
            seeds.append({"seed_label": label, **row})

    return seeds


def _first_match(
    frame: pd.DataFrame,
    mask_builder,
    existing_ids: set[str] | None = None,
) -> dict[str, str] | None:
    existing_ids = existing_ids or set()
    candidates = frame.loc[mask_builder(frame)].copy()
    candidates = candidates.loc[~candidates["recipe_id"].astype(str).isin(existing_ids)].copy()
    if candidates.empty:
        return None
    candidates = candidates.sort_values(
        by=["has_complete_nutrition", "display_name", "recipe_id"],
        ascending=[False, True, True],
    )
    row = candidates.iloc[0]
    return {
        "recipe_id": str(row.get("recipe_id")),
        "display_name": str(row.get("display_name")),
    }


def _search_text(frame: pd.DataFrame) -> pd.Series:
    columns = [
        "display_name",
        "recipe_kind",
        "recipe_category",
        "recipe_subcategory",
        "main_protein_family",
        "dominant_ingredient_name",
    ]
    parts = [frame[col].fillna("").astype(str).str.lower() for col in columns if col in frame.columns]
    if not parts:
        return pd.Series("", index=frame.index)
    return pd.concat(parts, axis=1).agg(" ".join, axis=1)


def _example_text(seeds: list[dict[str, str]], neighbors_df: pd.DataFrame) -> str:
    lines = [
        "KNN recipe similarity v1 neighbor examples",
        f"seed_count={len(seeds)}",
        "",
    ]
    for seed in seeds:
        lines.append(f"Seed [{seed['seed_label']}]: {seed['display_name']} ({seed['recipe_id']})")
        subset = neighbors_df.loc[neighbors_df["seed_label"].eq(seed["seed_label"])].head(5)
        if subset.empty:
            lines.append("  no neighbors")
        for _, row in subset.iterrows():
            warning_text = str(row.get("warnings") or "").strip() or "none"
            lines.append(
                "  "
                f"#{int(row['rank'])} score={float(row['similarity_score']):.3f} "
                f"{row['candidate_display_name']} ({row['candidate_recipe_id']}); "
                f"why={row['why_similar']}; warnings={warning_text}"
            )
        lines.append("")
    return "\n".join(lines)


def _format_counter(counter: Counter[str]) -> str:
    if not counter:
        return "none"
    return ",".join(f"{key}:{value}" for key, value in counter.most_common(8))


if __name__ == "__main__":
    raise SystemExit(main())
