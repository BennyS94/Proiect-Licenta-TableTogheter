from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.generator_v1.recipe_similarity import (
    build_recipe_similarity_features,
    load_recipe_similarity_source,
)


DATASET_PROFILE = "v1_2_demo_final"
AUDIT_DIR = PROJECT_ROOT / "data/recipesdb/audit"
SUMMARY_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_feature_coverage_summary.txt"
FEATURE_TABLE_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_feature_table.csv"
MISSING_FEATURES_PATH = AUDIT_DIR / "knn_recipe_similarity_v1_missing_features.csv"


def main() -> int:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    source = load_recipe_similarity_source(DATASET_PROFILE)
    features = build_recipe_similarity_features(
        source["recipes"],
        source["nutrition"],
        source["ingredients"],
    )
    features_for_csv = features.copy()
    features_for_csv["allowed_slots"] = features_for_csv["allowed_slots_text"]
    features_for_csv.to_csv(FEATURE_TABLE_PATH, index=False)

    missing = _missing_feature_rows(features)
    missing.to_csv(MISSING_FEATURES_PATH, index=False)

    recipe_count = len(features)
    active_count = int(features["is_active_bool"].sum())
    complete_nutrition_count = int(features["has_complete_nutrition"].sum())
    usable_time_count = int(features["has_usable_time"].sum())
    slot_count = int(features["allowed_slots_text"].astype(str).str.len().gt(0).sum())
    category_count = int(features["recipe_category"].fillna("").astype(str).str.len().gt(0).sum())
    family_count = int(features["recipe_family_name"].fillna("").astype(str).str.len().gt(0).sum())
    main_protein_count = int(features["main_protein_family"].fillna("").astype(str).str.len().gt(0).sum())
    dominant_count = int(
        features["dominant_ingredient_name"].fillna("").astype(str).str.len().gt(0).sum()
    )

    reliable_features = [
        "per-serving macros",
        "effective_time_min_for_scoring",
        "allowed_slots_json",
        "recipe_kind",
        "recipe_category",
    ]
    low_weight_features = []
    if family_count < recipe_count * 0.50:
        low_weight_features.append("recipe_family_name")
    if main_protein_count < recipe_count * 0.60:
        low_weight_features.append("main_protein_family")
    if dominant_count < recipe_count * 0.60:
        low_weight_features.append("dominant_ingredient_name")

    summary_lines = [
        "KNN recipe similarity v1 feature coverage summary",
        "status=ok",
        f"dataset_profile={DATASET_PROFILE}",
        f"dataset_path={source['dataset_path'].as_posix()}",
        f"recipe_count={recipe_count}",
        f"active_recipe_count={active_count}",
        f"complete_nutrition_count={complete_nutrition_count}",
        f"usable_time_count={usable_time_count}",
        f"slot_feature_count={slot_count}",
        f"category_feature_count={category_count}",
        f"family_feature_count={family_count}",
        f"main_protein_feature_count={main_protein_count}",
        f"dominant_ingredient_feature_count={dominant_count}",
        f"missing_feature_rows={len(missing)}",
        "reliable_features=" + ",".join(reliable_features),
        "low_weight_or_review_features=" + (",".join(low_weight_features) if low_weight_features else "none"),
        f"feature_table={FEATURE_TABLE_PATH.as_posix()}",
        f"missing_features={MISSING_FEATURES_PATH.as_posix()}",
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0


def _missing_feature_rows(features: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in features.iterrows():
        missing = []
        if not bool(row.get("has_complete_nutrition")):
            missing.append("nutrition")
        if not bool(row.get("has_usable_time")):
            missing.append("time")
        if not str(row.get("allowed_slots_text") or "").strip():
            missing.append("allowed_slots")
        if not str(row.get("recipe_category") or "").strip():
            missing.append("recipe_category")
        if not str(row.get("recipe_family_name") or "").strip():
            missing.append("recipe_family_name")
        if not str(row.get("main_protein_family") or "").strip():
            missing.append("main_protein_family")
        if not str(row.get("dominant_ingredient_name") or "").strip():
            missing.append("dominant_ingredient_name")
        if missing:
            rows.append(
                {
                    "recipe_id": row.get("recipe_id"),
                    "display_name": row.get("display_name"),
                    "allowed_slots": row.get("allowed_slots_text"),
                    "recipe_kind": row.get("recipe_kind"),
                    "recipe_category": row.get("recipe_category"),
                    "main_protein_family": row.get("main_protein_family"),
                    "dominant_ingredient_name": row.get("dominant_ingredient_name"),
                    "missing_features": ",".join(missing),
                }
            )
    return pd.DataFrame(rows)


if __name__ == "__main__":
    raise SystemExit(main())
