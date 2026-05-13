from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.parse_recipes_v1_1_curated_200_ingredients import (
    OUTPUT_COLUMNS,
    parse_ingredient_row,
)


INPUT_RECIPES = Path("data/recipesdb/draft/recipes_v1_1_round26_targeted_plus10.csv")
SOURCE_RECIPES = Path("data/recipesdb/source/1_Recipe_csv.csv")
OUT_PARSED = Path("data/recipesdb/draft/recipes_v1_1_round26_plus10_ingredients_parsed.csv")
OUT_SUMMARY = Path("data/recipesdb/audit/recipes_v1_1_round26_plus10_parse_summary.txt")
OUT_REVIEW = Path("data/recipesdb/audit/recipes_v1_1_round26_plus10_parse_review.csv")


def main() -> None:
    recipe_rows = read_csv(INPUT_RECIPES)
    parsed_rows: list[dict[str, object]] = []
    for recipe_row in recipe_rows:
        ingredients = json.loads(recipe_row["ingredients_json"])
        for ingredient_index, ingredient_raw in enumerate(ingredients, start=1):
            parsed_rows.append(
                parse_ingredient_row(recipe_row, ingredient_index, ingredient_raw)
            )

    review_rows = [
        row
        for row in parsed_rows
        if row["parse_status"] in {"parsed_partial", "review_needed", "failed_parse"}
    ]
    write_csv(OUT_PARSED, parsed_rows, OUTPUT_COLUMNS)
    write_csv(OUT_REVIEW, review_rows, OUTPUT_COLUMNS)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(build_summary(parsed_rows, recipe_rows), encoding="utf-8")

    status_counts = Counter(str(row["parse_status"]) for row in parsed_rows)
    grams_count = sum(1 for row in parsed_rows if row["quantity_grams_estimated"] != "")
    print("Round26 +10 ingredient parsing written")
    print(f"recipes={len(recipe_rows)}")
    print(f"ingredient_rows={len(parsed_rows)}")
    print(f"rows_with_grams={grams_count}")
    for status, count in status_counts.most_common():
        print(f"{status}={count}")
    print(f"parsed={OUT_PARSED}")
    print(f"summary={OUT_SUMMARY}")
    print(f"review={OUT_REVIEW}")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_summary(
    parsed_rows: list[dict[str, object]],
    recipe_rows: list[dict[str, str]],
) -> str:
    status_counts = Counter(str(row["parse_status"]) for row in parsed_rows)
    unit_counts = Counter(
        str(row["quantity_unit"]) for row in parsed_rows if row["quantity_unit"]
    )
    review_reason_counts = Counter()
    recipe_review_counts: dict[str, Counter] = defaultdict(Counter)
    grams_count = sum(1 for row in parsed_rows if row["quantity_grams_estimated"] != "")
    for row in parsed_rows:
        notes = [note for note in str(row["parse_notes"]).split(";") if note]
        if row["parse_status"] in {"parsed_partial", "review_needed", "failed_parse"}:
            review_reason_counts.update(notes)
            recipe_review_counts[str(row["display_name"])]["review_or_failed"] += 1

    lines = [
        "Recipes_DB v1.1 Round26 +10 ingredient parse summary",
        "",
        f"source_recipes={SOURCE_RECIPES}",
        f"input_recipes={INPUT_RECIPES}",
        f"recipes={len(recipe_rows)}",
        f"ingredient_rows={len(parsed_rows)}",
        f"rows_with_quantity_grams_estimated={grams_count}",
        f"rows_missing_quantity_grams_estimated={len(parsed_rows) - grams_count}",
        "",
        "Parse status counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in status_counts.most_common())
    lines.extend(["", "Top review reasons:"])
    lines.extend(f"- {name}: {count}" for name, count in review_reason_counts.most_common(25))
    lines.extend(["", "Top quantity units:"])
    lines.extend(f"- {name}: {count}" for name, count in unit_counts.most_common(20))
    lines.extend(["", "Recipes with review/failed rows:"])
    for title, counter in sorted(
        recipe_review_counts.items(),
        key=lambda item: (-item[1]["review_or_failed"], item[0]),
    )[:20]:
        lines.append(f"- {title}: {counter['review_or_failed']}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
