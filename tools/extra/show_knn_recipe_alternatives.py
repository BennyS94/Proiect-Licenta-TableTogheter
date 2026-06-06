from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.generator_v1.recipe_similarity import (
    build_recipe_similarity_features,
    find_similar_recipes,
    load_recipe_similarity_source,
)


def main() -> int:
    args = _parse_args()
    source = load_recipe_similarity_source(args.dataset_profile)
    features = build_recipe_similarity_features(
        source["recipes"],
        source["nutrition"],
        source["ingredients"],
    )
    neighbors = find_similar_recipes(
        args.recipe_id,
        features,
        top_k=args.top_k,
        filters={"same_slot": not args.allow_cross_slot, "active_only": True},
    )
    if not neighbors:
        print(f"No alternatives found for recipe_id={args.recipe_id}")
        return 1

    print(f"Alternatives for {args.recipe_id}")
    for rank, row in enumerate(neighbors, start=1):
        print(
            f"{rank}. score={row['similarity_score']:.3f} "
            f"{row['candidate_display_name']} ({row['candidate_recipe_id']})"
        )
        print(
            "   "
            f"delta kcal={row['macro_delta_kcal']}, "
            f"protein={row['macro_delta_protein']}, "
            f"time={row['time_delta_min']}; "
            f"why={row['why_similar']}; "
            f"warnings={row['warnings'] or 'none'}"
        )
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show KNN-lite recipe alternatives.")
    parser.add_argument("--recipe_id", required=True)
    parser.add_argument("--top_k", type=int, default=5)
    parser.add_argument("--dataset_profile", default="v1_2_demo_final")
    parser.add_argument("--allow_cross_slot", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
