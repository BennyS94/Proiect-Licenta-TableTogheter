from __future__ import annotations

import csv
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.parse_recipes_v1_1_curated_200_ingredients import (
    OUTPUT_COLUMNS,
    parse_ingredient_row,
)


INPUT_RECIPES = Path("data/recipesdb/draft/recipes_v1_2_round37_targeted_plus100.csv")
SOURCE_RECIPES = Path("data/recipesdb/source/1_Recipe_csv.csv")
OUT_PARSED = Path("data/recipesdb/draft/recipes_v1_2_round37_plus100_ingredients_parsed.csv")
OUT_SUMMARY = Path("data/recipesdb/audit/recipes_v1_2_round37_plus100_parse_summary.txt")
OUT_REVIEW = Path("data/recipesdb/audit/recipes_v1_2_round37_plus100_parse_review.csv")
OUT_QUALITY_FLAGS = Path("data/recipesdb/audit/recipes_v1_2_round37_plus100_parse_quality_flags.csv")

QUALITY_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "target_bucket",
    "total_ingredient_rows",
    "parsed_clean_count",
    "parsed_partial_count",
    "review_needed_count",
    "failed_parse_count",
    "rows_with_quantity_grams_estimated",
    "main_protein_parsed",
    "main_carb_parsed",
    "critical_rows_total",
    "critical_rows_with_usable_parse",
    "parse_quality_status",
    "parse_quality_flags",
]


def main() -> None:
    recipe_rows = read_csv(INPUT_RECIPES)
    parsed_rows: list[dict[str, object]] = []
    for recipe_row in recipe_rows:
        ingredients = json.loads(recipe_row["ingredients_json"])
        for ingredient_index, ingredient_raw in enumerate(ingredients, start=1):
            parsed_rows.append(parse_ingredient_row(recipe_row, ingredient_index, ingredient_raw))

    review_rows = [
        row for row in parsed_rows
        if row["parse_status"] in {"parsed_partial", "review_needed", "failed_parse"}
    ]
    quality_rows = build_parse_quality_rows(recipe_rows, parsed_rows)
    write_csv(OUT_PARSED, parsed_rows, OUTPUT_COLUMNS)
    write_csv(OUT_REVIEW, review_rows, OUTPUT_COLUMNS)
    write_csv(OUT_QUALITY_FLAGS, quality_rows, QUALITY_COLUMNS)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(build_summary(parsed_rows, recipe_rows, quality_rows), encoding="utf-8")

    status_counts = Counter(str(row["parse_status"]) for row in parsed_rows)
    print("Round37 +100 ingredient parsing written")
    print(f"recipes={len(recipe_rows)}")
    print(f"ingredient_rows={len(parsed_rows)}")
    print(f"rows_with_grams={sum(1 for row in parsed_rows if row['quantity_grams_estimated'] != '')}")
    for status, count in status_counts.most_common():
        print(f"{status}={count}")
    print(f"parsed={OUT_PARSED}")
    print(f"summary={OUT_SUMMARY}")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
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
    quality_rows: list[dict[str, object]],
) -> str:
    status_counts = Counter(str(row["parse_status"]) for row in parsed_rows)
    unit_counts = Counter(str(row["quantity_unit"]) for row in parsed_rows if row["quantity_unit"])
    review_reason_counts = Counter()
    recipe_review_counts: dict[str, Counter] = defaultdict(Counter)
    grams_count = sum(1 for row in parsed_rows if row["quantity_grams_estimated"] != "")
    for row in parsed_rows:
        notes = [note for note in str(row["parse_notes"]).split(";") if note]
        if row["parse_status"] in {"parsed_partial", "review_needed", "failed_parse"}:
            review_reason_counts.update(notes)
            recipe_review_counts[str(row["display_name"])]["review_or_failed"] += 1

    lines = [
        "Recipes_DB v1.2 Round37 +100 ingredient parse summary",
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
    lines.extend(["", "Parse quality status counts:"])
    quality_counts = Counter(str(row["parse_quality_status"]) for row in quality_rows)
    lines.extend(f"- {name}: {count}" for name, count in quality_counts.most_common())
    lines.extend(["", "Recipes with review/failed rows:"])
    for title, counter in sorted(
        recipe_review_counts.items(),
        key=lambda item: (-item[1]["review_or_failed"], item[0]),
    )[:25]:
        lines.append(f"- {title}: {counter['review_or_failed']}")
    return "\n".join(lines) + "\n"


def build_parse_quality_rows(
    recipe_rows: list[dict[str, str]],
    parsed_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    by_recipe: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in parsed_rows:
        by_recipe[str(row.get("recipe_id_candidate", ""))].append(row)

    quality_rows = []
    for recipe in recipe_rows:
        recipe_id = str(recipe.get("recipe_id_candidate", "")).strip()
        rows = by_recipe.get(recipe_id, [])
        status_counts = Counter(str(row.get("parse_status")) for row in rows)
        grams_count = sum(1 for row in rows if str(row.get("quantity_grams_estimated", "")).strip())
        critical_rows = [row for row in rows if is_critical_ingredient(recipe, row)]
        critical_usable = [
            row for row in critical_rows
            if row.get("parse_status") in {"parsed_clean", "parsed_partial"}
            and str(row.get("ingredient_name_normalized", "")).strip()
        ]
        main_protein_parsed = any(is_protein_row(row) for row in critical_usable)
        main_carb_parsed = any(is_carb_row(row) for row in critical_usable)

        flags = []
        if status_counts["failed_parse"] > 0:
            flags.append("failed_parse_present")
        if status_counts["review_needed"] >= 4:
            flags.append("too_many_review_needed_rows")
        if grams_count < max(2, len(rows) // 3):
            flags.append("too_few_usable_quantity_rows")
        if critical_rows and len(critical_usable) < len(critical_rows):
            flags.append("critical_macro_ingredient_parse_gap")
        if not main_protein_parsed:
            flags.append("main_protein_not_detected_as_parsed")
        if recipe.get("target_bucket") != "breakfast_competitor" and not main_carb_parsed:
            flags.append("main_carb_not_detected_as_parsed")

        if "failed_parse_present" in flags or "critical_macro_ingredient_parse_gap" in flags:
            quality_status = "fail"
        elif flags:
            quality_status = "review"
        else:
            quality_status = "good"

        quality_rows.append(
            {
                "recipe_id_candidate": recipe_id,
                "display_name": recipe.get("display_name", ""),
                "target_bucket": recipe.get("target_bucket", ""),
                "total_ingredient_rows": len(rows),
                "parsed_clean_count": status_counts["parsed_clean"],
                "parsed_partial_count": status_counts["parsed_partial"],
                "review_needed_count": status_counts["review_needed"],
                "failed_parse_count": status_counts["failed_parse"],
                "rows_with_quantity_grams_estimated": grams_count,
                "main_protein_parsed": str(main_protein_parsed).lower(),
                "main_carb_parsed": str(main_carb_parsed).lower(),
                "critical_rows_total": len(critical_rows),
                "critical_rows_with_usable_parse": len(critical_usable),
                "parse_quality_status": quality_status,
                "parse_quality_flags": ";".join(flags),
            }
        )
    return quality_rows


def is_critical_ingredient(recipe: dict[str, str], row: dict[str, object]) -> bool:
    text = normalize_text(
        f"{row.get('ingredient_name_normalized', '')} {row.get('ingredient_raw_text', '')}"
    )
    return is_protein_text(text) or is_carb_text(text)


def is_protein_row(row: dict[str, object]) -> bool:
    text = normalize_text(
        f"{row.get('ingredient_name_normalized', '')} {row.get('ingredient_raw_text', '')}"
    )
    return is_protein_text(text)


def is_carb_row(row: dict[str, object]) -> bool:
    text = normalize_text(
        f"{row.get('ingredient_name_normalized', '')} {row.get('ingredient_raw_text', '')}"
    )
    return is_carb_text(text)


def is_protein_text(text: str) -> bool:
    return any(
        term in text
        for term in [
            "chicken",
            "beef",
            "pork",
            "turkey",
            "fish",
            "salmon",
            "tuna",
            "cod",
            "egg",
            "eggs",
            "lentil",
            "bean",
            "beans",
            "chickpea",
            "yogurt",
            "milk",
            "ham",
        ]
    )


def is_carb_text(text: str) -> bool:
    return any(
        term in text
        for term in [
            "rice",
            "pasta",
            "spaghetti",
            "penne",
            "potato",
            "bread",
            "toast",
            "tortilla",
            "oat",
            "flour",
            "bean",
            "lentil",
            "noodle",
        ]
    )


def normalize_text(value: object) -> str:
    text = str(value or "").strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


if __name__ == "__main__":
    main()
