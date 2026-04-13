import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path

import select_recipe_pilot_subset as base_selection


DEFAULT_SOURCE = Path("data/recipesdb/source/1_Recipe_csv.csv")
DEFAULT_CURRENT_SUBSET = Path("data/recipesdb/draft/recipes_pilot_recipes_clean.csv")
DEFAULT_MANUAL_AUDIT = Path("data/recipesdb/audit/recipes_pilot_manual_audit.csv")
DEFAULT_FINAL_SUBSET = Path("data/recipesdb/draft/recipes_pilot_subset_final.csv")
DEFAULT_REPLACEMENT_LOG = Path("data/recipesdb/audit/recipes_pilot_replacement_log.csv")
DEFAULT_SUMMARY = Path("data/recipesdb/audit/recipes_pilot_final_summary.txt")

REPLACEMENT_CANDIDATES = [
    (
        "Italian Wedding Soup",
        "add_soup_diversity",
        "savory soup that broadens the pilot beyond compact mains",
        "standalone",
    ),
    (
        "Best Ever Split Pea Soup",
        "add_soup_diversity",
        "vegetarian soup that adds legume-heavy savory coverage",
        "standalone",
    ),
    (
        "Vegan Broccoli Soup",
        "add_soup_diversity",
        "clean vegetarian soup with simple parseable ingredient structure",
        "standalone",
    ),
    (
        "Easy Black Bean Soup for Two",
        "add_soup_diversity",
        "meal-planning friendly soup that expands bean-based savory coverage",
        "standalone",
    ),
    (
        "Tofu Broccoli Stir-Fry",
        "add_vegetarian_main_diversity",
        "clear vegetarian main with protein-plus-vegetable structure",
        "standalone",
    ),
    (
        "Slow Cooker Vegetarian Curry",
        "add_vegetarian_main_diversity",
        "savory vegetarian main with a different cooking profile",
        "standalone",
    ),
    (
        "Black Beans and Rice",
        "add_vegetarian_main_diversity",
        "simple legume-and-grain main that maps well to Food_DB ingredients",
        "standalone",
    ),
    (
        "Mushroom Stir-Fry",
        "add_vegetarian_main_diversity",
        "clean vegetable-forward main that improves non-meat coverage",
        "standalone",
    ),
    (
        "Butternut Farro Salad with Blood Orange Vinaigrette",
        "add_salad_diversity",
        "main-dish salad that broadens the pilot beyond hot mains",
        "standalone",
    ),
    (
        "Kale, Quinoa, and Avocado Salad with Lemon Dijon Vinaigrette",
        "add_salad_diversity",
        "grain-based salad adds lighter savory planning coverage",
        "standalone",
    ),
    (
        "Mediterranean Lentil Salad",
        "add_salad_diversity",
        "legume-based salad adds a clean vegetarian savory pattern",
        "standalone",
    ),
    (
        "Oven Roasted Potatoes",
        "add_side_dish_diversity",
        "clean side dish with low-noise ingredients and common meal-planning use",
        "side_only",
    ),
    (
        "Grilled Stuffed Portobello Mushroom Caps",
        "add_vegetarian_main_diversity",
        "stuffed vegetable main adds a useful vegetarian centerpiece pattern",
        "standalone",
    ),
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingheata subsetul final de recipe pilot dupa auditul manual."
    )
    parser.add_argument("--source", default=str(DEFAULT_SOURCE))
    parser.add_argument("--current-subset", default=str(DEFAULT_CURRENT_SUBSET))
    parser.add_argument("--manual-audit", default=str(DEFAULT_MANUAL_AUDIT))
    parser.add_argument("--out-final-subset", default=str(DEFAULT_FINAL_SUBSET))
    parser.add_argument("--out-replacement-log", default=str(DEFAULT_REPLACEMENT_LOG))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY))
    return parser.parse_args()


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_manual_audit(path):
    rows = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(row)
    return rows


def load_current_subset(path):
    rows = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows[row["recipe_title"]] = row
    return rows


def rebuild_filtered_pool(source_path):
    grouped_rows = defaultdict(list)

    with source_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for source_row_number, raw_row in enumerate(reader, start=1):
            row = base_selection.build_row_record(source_row_number, raw_row)
            grouped_rows[row["recipe_signature"]].append(row)

    deduped_rows = [base_selection.choose_representative(rows) for rows in grouped_rows.values()]

    filtered_rows = []
    for row in deduped_rows:
        reason = base_selection.get_primary_exclusion_reason(row)
        if reason:
            continue
        row["selection_score"] = base_selection.compute_selection_score(row)
        row["selection_bucket"] = base_selection.build_selection_bucket(row)
        filtered_rows.append(row)

    return filtered_rows


def build_replacement_lookup(filtered_rows):
    lookup = {}
    for row in filtered_rows:
        lookup[row["recipe_title"]] = row
    return lookup


def main():
    args = parse_args()

    source_path = Path(args.source)
    current_subset_path = Path(args.current_subset)
    manual_audit_path = Path(args.manual_audit)
    final_subset_out = Path(args.out_final_subset)
    replacement_log_out = Path(args.out_replacement_log)
    summary_out = Path(args.out_summary)

    manual_rows = load_manual_audit(manual_audit_path)
    current_subset_rows = load_current_subset(current_subset_path)
    filtered_pool = rebuild_filtered_pool(source_path)
    pool_by_title = build_replacement_lookup(filtered_pool)

    keep_rows = [row for row in manual_rows if row["audit_decision"] == "keep_for_parsing"]
    review_rows = [row for row in manual_rows if row["audit_decision"] == "move_to_review"]
    remove_rows = [row for row in manual_rows if row["audit_decision"] == "remove_from_pilot"]

    final_rows = []
    replacement_log_rows = []

    for row in keep_rows:
        source_info = current_subset_rows[row["recipe_title"]]
        final_rows.append(
            {
                "final_recipe_id": row["pilot_recipe_id"],
                "source_row_number": row["source_row_number"],
                "recipe_signature": source_info["recipe_signature"],
                "recipe_title": row["recipe_title"],
                "category": row["category"],
                "subcategory": row["subcategory"],
                "description": source_info["description"],
                "ingredients_json": source_info["ingredients_json"],
                "directions_json": source_info["directions_json"],
                "num_ingredients": row["num_ingredients"],
                "num_steps": row["num_steps"],
                "recipe_kind_guess": row["recipe_kind_guess"],
                "final_subset_source": "kept_original",
                "final_subset_reason": row["audit_reason"],
                "selection_score": source_info["selection_score"],
                "noise_score": source_info["noise_score"],
            }
        )

    for row in remove_rows:
        replacement_log_rows.append(
            {
                "action": "removed_from_final_subset",
                "recipe_title": row["recipe_title"],
                "category": row["category"],
                "subcategory": row["subcategory"],
                "recipe_kind_guess": row["recipe_kind_guess"],
                "reason_code": "manual_remove",
                "reason": row["audit_reason"],
            }
        )

    for title, reason_code, reason_text, recipe_kind_guess in REPLACEMENT_CANDIDATES:
        if title not in pool_by_title:
            raise ValueError(f"Replacement title not found in eligible pool: {title}")

        pool_row = pool_by_title[title]
        final_recipe_id = f"pilot_recipe_final_{len(final_rows) + 1:03d}"

        final_rows.append(
            {
                "final_recipe_id": final_recipe_id,
                "source_row_number": pool_row["source_row_number"],
                "recipe_signature": pool_row["recipe_signature"],
                "recipe_title": pool_row["recipe_title"],
                "category": pool_row["category"],
                "subcategory": pool_row["subcategory"],
                "description": pool_row["description"],
                "ingredients_json": pool_row["ingredients"],
                "directions_json": pool_row["directions"],
                "num_ingredients": pool_row["num_ingredients_int"],
                "num_steps": pool_row["num_steps_int"],
                "recipe_kind_guess": recipe_kind_guess,
                "final_subset_source": "replacement",
                "final_subset_reason": reason_text,
                "selection_score": pool_row["selection_score"],
                "noise_score": pool_row["noise_score"],
            }
        )

        replacement_log_rows.append(
            {
                "action": "added_replacement",
                "recipe_title": pool_row["recipe_title"],
                "category": pool_row["category"],
                "subcategory": pool_row["subcategory"],
                "recipe_kind_guess": recipe_kind_guess,
                "reason_code": reason_code,
                "reason": reason_text,
            }
        )

    final_rows.sort(key=lambda row: (row["final_subset_source"] != "kept_original", row["recipe_title"]))
    for idx, row in enumerate(final_rows, start=1):
        row["final_recipe_id"] = f"pilot_recipe_final_{idx:03d}"

    write_csv(
        final_subset_out,
        final_rows,
        [
            "final_recipe_id",
            "source_row_number",
            "recipe_signature",
            "recipe_title",
            "category",
            "subcategory",
            "description",
            "ingredients_json",
            "directions_json",
            "num_ingredients",
            "num_steps",
            "recipe_kind_guess",
            "final_subset_source",
            "final_subset_reason",
            "selection_score",
            "noise_score",
        ],
    )

    write_csv(
        replacement_log_out,
        replacement_log_rows,
        [
            "action",
            "recipe_title",
            "category",
            "subcategory",
            "recipe_kind_guess",
            "reason_code",
            "reason",
        ],
    )

    kind_counts = Counter(row["recipe_kind_guess"] for row in final_rows)
    category_counts = Counter(row["category"] for row in final_rows)
    source_counts = Counter(row["final_subset_source"] for row in final_rows)

    summary_lines = [
        "Recipes pilot final subset summary",
        "",
        f"Original pilot rows: {len(manual_rows)}",
        f"keep_for_parsing rows retained: {len(keep_rows)}",
        f"move_to_review rows kept separate: {len(review_rows)}",
        f"remove_from_pilot rows removed: {len(remove_rows)}",
        f"replacement rows added: {len(REPLACEMENT_CANDIDATES)}",
        f"Final subset count: {len(final_rows)}",
        "",
        "Final subset sources:",
    ]

    for key, value in source_counts.items():
        summary_lines.append(f"- {key}: {value}")

    summary_lines.extend(
        [
            "",
            "Final recipe kind distribution:",
        ]
    )

    for key, value in kind_counts.most_common():
        summary_lines.append(f"- {key}: {value}")

    summary_lines.extend(
        [
            "",
            "Top final categories:",
        ]
    )

    for key, value in category_counts.most_common(10):
        summary_lines.append(f"- {key}: {value}")

    summary_lines.extend(
        [
            "",
            "Diversity note:",
            "- final subset adds soups, vegetarian mains, salads, and a clean side dish to offset the original concentration in compact meat-forward mains",
            "- move_to_review rows remain outside the parsing subset for now",
        ]
    )

    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"Final subset count: {len(final_rows)}")
    print("Removed rows:")
    for row in remove_rows:
        print(f" - {row['recipe_title']}")
    print("Added rows:")
    for title, _, _, _ in REPLACEMENT_CANDIDATES:
        print(f" - {title}")
    print("")
    print("Final recipe kind distribution:")
    for key, value in kind_counts.most_common():
        print(f" - {key}: {value}")
    print("")
    print(f"Written: {final_subset_out}")
    print(f"Written: {replacement_log_out}")
    print(f"Written: {summary_out}")


if __name__ == "__main__":
    main()
