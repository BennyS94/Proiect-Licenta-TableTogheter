from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra import parse_recipes_v1_2_round37_plus100_ingredients as parser


def main() -> None:
    parser.INPUT_RECIPES = Path("data/recipesdb/draft/recipes_v1_2_round42_dataset_curated_selected.csv")
    parser.OUT_PARSED = Path("data/recipesdb/draft/recipes_v1_2_round42_dataset_ingredients_parsed.csv")
    parser.OUT_SUMMARY = Path("data/recipesdb/audit/recipes_v1_2_round42_dataset_parse_summary.txt")
    parser.OUT_REVIEW = Path("data/recipesdb/audit/recipes_v1_2_round42_dataset_parse_review.csv")
    parser.OUT_QUALITY_FLAGS = Path("data/recipesdb/audit/recipes_v1_2_round42_dataset_parse_quality_flags.csv")
    parser.main()
    if parser.OUT_SUMMARY.exists():
        text = parser.OUT_SUMMARY.read_text(encoding="utf-8")
        text = text.replace("Round37 +100", "Round42 dataset-curated")
        text = text.replace("round37", "round42")
        parser.OUT_SUMMARY.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
