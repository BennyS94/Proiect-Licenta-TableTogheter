from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.build_recipes_v1_2_round30_plus15_nutrition_cache import (
    FOODDB,
    MAPPING_COLUMNS,
    MAPPING_QUALITY_COLUMNS,
    OUT_MAPPING_QUALITY_AUDIT,
    OUT_MAPPING_REVIEW,
    OUT_MAPPING_SUMMARY,
    OUT_MATCHES,
    OUT_UNMAPPED,
    OUT_UNIT_RULES,
    RECIPES,
    build_food_lookup,
    build_mapping_quality_rows,
    build_mapping_rows,
    build_mapping_summary,
    read_csv,
    write_csv,
)


def main() -> None:
    recipes = read_csv(RECIPES)
    unit_rows = read_csv(OUT_UNIT_RULES)
    food_rows = read_csv(FOODDB)
    food_lookup = build_food_lookup(food_rows)
    mapping_rows, unmapped_rows = build_mapping_rows(unit_rows, food_lookup)
    write_csv(OUT_MATCHES, mapping_rows, MAPPING_COLUMNS)
    write_csv(OUT_UNMAPPED, unmapped_rows, MAPPING_COLUMNS)
    write_csv(OUT_MAPPING_REVIEW, [row for row in mapping_rows if row["mapping_status"] != "accepted_auto"], MAPPING_COLUMNS)
    write_csv(OUT_MAPPING_QUALITY_AUDIT, build_mapping_quality_rows(recipes, mapping_rows), MAPPING_QUALITY_COLUMNS)
    OUT_MAPPING_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_MAPPING_SUMMARY.write_text(build_mapping_summary(mapping_rows), encoding="utf-8")
    print("Round30 +15 mapping written")
    print(f"matches={OUT_MATCHES}")
    print(f"unmapped={OUT_UNMAPPED}")
    print(f"mapping_review={OUT_MAPPING_REVIEW}")
    print(f"mapping_quality_audit={OUT_MAPPING_QUALITY_AUDIT}")
    print(f"summary={OUT_MAPPING_SUMMARY}")


if __name__ == "__main__":
    main()

