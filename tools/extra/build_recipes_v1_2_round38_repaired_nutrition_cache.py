from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra import build_recipes_v1_2_round37_plus100_nutrition_cache as round37


RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"

ROUND37_RECIPES = RECIPES_DRAFT_DIR / "recipes_v1_2_round37_targeted_plus100.csv"
ROUND37_CACHE = RECIPES_DRAFT_DIR / "recipes_v1_2_round37_plus100_nutrition_cache.csv"
ROUND38_MATCHES = RECIPES_DRAFT_DIR / "recipes_v1_2_round38_repaired_food_matches.csv"
ROUND37_EXPANDED_DIR = RECIPES_DRAFT_DIR / "v1_2_generator_ready_round37_expanded"
ROUND38_REPAIRED_DIR = RECIPES_DRAFT_DIR / "v1_2_generator_ready_round37_expanded_repaired"
FOODDB = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round9.csv"

OUT_CACHE = RECIPES_DRAFT_DIR / "recipes_v1_2_round38_repaired_nutrition_cache.csv"
OUT_SUMMARY = AUDIT_DIR / "recipes_v1_2_round38_repaired_nutrition_summary.txt"
OUT_AUDIT = AUDIT_DIR / "recipes_v1_2_round38_repaired_nutrition_audit.csv"
OUT_READY_AUDIT = AUDIT_DIR / "recipes_v1_2_round38_repaired_generator_ready_audit.csv"
OUT_MATERIALIZATION_SUMMARY = AUDIT_DIR / "recipes_v1_2_round38_repaired_materialization_summary.txt"
OUT_MATERIALIZATION_AUDIT = AUDIT_DIR / "recipes_v1_2_round38_repaired_materialization_audit.csv"

ROUND38_NUTRITION_BASIS = "recipes_v1_2_round38_repaired_mapped_ingredients_draft"
ROUND38_CACHE_VERSION = "recipes_v1_2_round38_repaired_001"
ROUND38_TAG = "round38_safe_manual_repair"

AUDIT_COLUMNS = round37.CACHE_COLUMNS + [
    "round37_generator_ready_candidate",
    "recovered_generator_ready",
    "selected_for_round38_dataset",
]
MATERIALIZATION_AUDIT_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "target_bucket",
    "round37_generator_ready_candidate",
    "round38_generator_ready_candidate",
    "strong_generator_ready",
    "recovered_generator_ready",
    "materialized",
    "round37_failure_reason",
    "round38_failure_reason",
]


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    recipes = read_csv(ROUND37_RECIPES)
    mapping_rows = read_csv(ROUND38_MATCHES)
    food_lookup = round37.base.build_food_lookup(read_csv(FOODDB))
    original_cache = read_csv(ROUND37_CACHE)
    original_by_id = {row["recipe_id_candidate"]: row for row in original_cache}
    original_ready_ids = {
        row["recipe_id_candidate"]
        for row in original_cache
        if row.get("generator_ready_candidate") == "true"
    }

    cache_rows, audit_rows = round37.build_cache_rows(recipes, mapping_rows, food_lookup)
    cache_rows = [mark_cache_row(row) for row in cache_rows]
    ready_ids = {
        row["recipe_id_candidate"]
        for row in cache_rows
        if row.get("generator_ready_candidate") == "true"
    }
    strong_ids = {
        row["recipe_id_candidate"]
        for row in cache_rows
        if row.get("strong_generator_ready") == "true"
    }
    recovered_ids = ready_ids - original_ready_ids
    audit_rows = [
        mark_audit_row(row, original_by_id, recovered_ids)
        for row in cache_rows
    ]

    materialize_repaired_dataset(
        recipes=recipes,
        mapping_rows=mapping_rows,
        cache_rows=cache_rows,
        recovered_ids=recovered_ids,
        original_ready_ids=original_ready_ids,
    )

    write_csv(OUT_CACHE, cache_rows, round37.CACHE_COLUMNS)
    write_csv(OUT_AUDIT, audit_rows, AUDIT_COLUMNS)
    write_csv(OUT_READY_AUDIT, audit_rows, AUDIT_COLUMNS)
    write_csv(
        OUT_MATERIALIZATION_AUDIT,
        materialization_audit_rows(recipes, cache_rows, original_by_id, recovered_ids),
        MATERIALIZATION_AUDIT_COLUMNS,
    )
    OUT_SUMMARY.write_text(
        build_nutrition_summary(cache_rows, original_by_id, recovered_ids),
        encoding="utf-8",
    )

    print("Round38 repaired nutrition cache and materialization written")
    print(f"- round37_ready_count: {len(original_ready_ids)}")
    print(f"- round38_ready_count: {len(ready_ids)}")
    print(f"- recovered_generator_ready_count: {len(recovered_ids)}")
    print(f"- strong_generator_ready_count: {len(strong_ids)}")
    print(f"- repaired_dataset: {ROUND38_REPAIRED_DIR}")


def materialize_repaired_dataset(
    *,
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, str]],
    cache_rows: list[dict[str, Any]],
    recovered_ids: set[str],
    original_ready_ids: set[str],
) -> None:
    ROUND38_REPAIRED_DIR.mkdir(parents=True, exist_ok=True)
    base_recipes = read_csv(ROUND37_EXPANDED_DIR / "recipes.csv")
    base_ingredients = read_csv(ROUND37_EXPANDED_DIR / "recipe_ingredients.csv")
    base_cache = read_csv(ROUND37_EXPANDED_DIR / "recipe_nutrition_cache.csv")
    recipe_by_id = {row["recipe_id_candidate"]: row for row in recipes}
    cache_by_id = {row["recipe_id_candidate"]: row for row in cache_rows}
    existing_recipe_ids = {row.get("recipe_id", "") for row in base_recipes}

    add_ids = sorted(
        recipe_id for recipe_id in recovered_ids
        if recipe_id not in existing_recipe_ids
        and recipe_id not in original_ready_ids
    )
    new_recipes = [
        mark_recipe_row(round37.materialized_recipe_row(recipe_by_id[recipe_id], cache_by_id[recipe_id]))
        for recipe_id in add_ids
    ]
    new_ingredients = [
        mark_ingredient_row(round37.materialized_ingredient_row(row))
        for row in mapping_rows
        if row.get("recipe_id_candidate") in add_ids
    ]
    new_cache = [
        mark_materialized_cache(round37.materialized_cache_row(cache_by_id[recipe_id]))
        for recipe_id in add_ids
    ]

    write_csv(ROUND38_REPAIRED_DIR / "recipes.csv", base_recipes + new_recipes, list(base_recipes[0].keys()))
    write_csv(
        ROUND38_REPAIRED_DIR / "recipe_ingredients.csv",
        base_ingredients + new_ingredients,
        list(base_ingredients[0].keys()),
    )
    write_csv(
        ROUND38_REPAIRED_DIR / "recipe_nutrition_cache.csv",
        base_cache + new_cache,
        list(base_cache[0].keys()),
    )
    (ROUND38_REPAIRED_DIR / "README_v1_2_generator_ready_round37_expanded_repaired.txt").write_text(
        "\n".join(
            [
                "Recipes_DB v1.2 generator-ready Round37 expanded repaired draft",
                "",
                "Draft/test only. Do not treat as current production data.",
                f"Base dataset: {ROUND37_EXPANDED_DIR}",
                f"Round38 recovered ready additions: {len(add_ids)}",
                f"QC tag: {ROUND38_TAG}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    OUT_MATERIALIZATION_SUMMARY.write_text(
        build_materialization_summary(
            base_count=len(base_recipes),
            recovered_ids=set(add_ids),
            new_count=len(base_recipes) + len(new_recipes),
        ),
        encoding="utf-8",
    )


def mark_cache_row(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated["nutrition_basis"] = ROUND38_NUTRITION_BASIS
    updated["cache_version"] = ROUND38_CACHE_VERSION
    return updated


def mark_audit_row(
    row: dict[str, Any],
    original_by_id: dict[str, dict[str, str]],
    recovered_ids: set[str],
) -> dict[str, Any]:
    recipe_id = str(row.get("recipe_id_candidate", ""))
    original = original_by_id.get(recipe_id, {})
    updated = dict(row)
    updated["round37_generator_ready_candidate"] = original.get("generator_ready_candidate", "")
    updated["recovered_generator_ready"] = str(recipe_id in recovered_ids).lower()
    updated["selected_for_round38_dataset"] = str(recipe_id in recovered_ids).lower()
    return updated


def mark_recipe_row(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated["qc_notes"] = append_note(str(updated.get("qc_notes", "")), ROUND38_TAG)
    return updated


def mark_ingredient_row(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated["qc_notes"] = append_note(str(updated.get("qc_notes", "")), ROUND38_TAG)
    if str(updated.get("mapping_method", "")).startswith("round38_repair"):
        updated["qc_ingredient_status"] = "accepted_auto_round38_repair"
    return updated


def mark_materialized_cache(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated["nutrition_basis"] = ROUND38_NUTRITION_BASIS
    updated["cache_version"] = ROUND38_CACHE_VERSION
    updated["qc_notes"] = append_note(str(updated.get("qc_notes", "")), ROUND38_TAG)
    return updated


def materialization_audit_rows(
    recipes: list[dict[str, str]],
    cache_rows: list[dict[str, Any]],
    original_by_id: dict[str, dict[str, str]],
    recovered_ids: set[str],
) -> list[dict[str, Any]]:
    cache_by_id = {row["recipe_id_candidate"]: row for row in cache_rows}
    rows = []
    for recipe in recipes:
        recipe_id = recipe["recipe_id_candidate"]
        original = original_by_id.get(recipe_id, {})
        cache = cache_by_id.get(recipe_id, {})
        rows.append(
            {
                "recipe_id_candidate": recipe_id,
                "display_name": recipe.get("display_name", ""),
                "target_bucket": recipe.get("target_bucket", ""),
                "round37_generator_ready_candidate": original.get("generator_ready_candidate", ""),
                "round38_generator_ready_candidate": cache.get("generator_ready_candidate", ""),
                "strong_generator_ready": cache.get("strong_generator_ready", ""),
                "recovered_generator_ready": str(recipe_id in recovered_ids).lower(),
                "materialized": str(recipe_id in recovered_ids).lower(),
                "round37_failure_reason": original.get("generator_ready_failure_reason", ""),
                "round38_failure_reason": cache.get("generator_ready_failure_reason", ""),
            }
        )
    return rows


def build_nutrition_summary(
    rows: list[dict[str, Any]],
    original_by_id: dict[str, dict[str, str]],
    recovered_ids: set[str],
) -> str:
    ready_rows = [row for row in rows if row.get("generator_ready_candidate") == "true"]
    strong_rows = [row for row in rows if row.get("strong_generator_ready") == "true"]
    original_ready = [
        row for row in original_by_id.values()
        if row.get("generator_ready_candidate") == "true"
    ]
    recovered_rows = [row for row in rows if row.get("recipe_id_candidate") in recovered_ids]
    failure_counts = Counter()
    for row in rows:
        if row.get("generator_ready_candidate") != "true":
            failure_counts.update(
                reason for reason in str(row.get("generator_ready_failure_reason", "")).split(";") if reason
            )
    lines = [
        "Recipes_DB v1.2 Round38 repaired nutrition summary",
        "",
        f"recipes={len(rows)}",
        f"round37_generator_ready_count={len(original_ready)}",
        f"round38_generator_ready_count={len(ready_rows)}",
        f"recovered_generator_ready_count={len(recovered_rows)}",
        f"strong_generator_ready_count={len(strong_rows)}",
        "",
        "Top remaining failure reasons:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in failure_counts.most_common(20))
    lines.extend(["", "Recovered recipes:"])
    if recovered_rows:
        for row in recovered_rows:
            lines.append(
                (
                    f"- {row['recipe_id_candidate']} | {row['display_name']} | "
                    f"kcal={row['energy_kcal_per_serving']} "
                    f"P/C/F={row['protein_g_per_serving']}/"
                    f"{row['carbs_g_per_serving']}/{row['fat_g_per_serving']} "
                    f"strong={row['strong_generator_ready']}"
                )
            )
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def build_materialization_summary(
    *,
    base_count: int,
    recovered_ids: set[str],
    new_count: int,
) -> str:
    return "\n".join(
        [
            "Recipes_DB v1.2 Round38 repaired materialization summary",
            "",
            f"base_dataset={ROUND37_EXPANDED_DIR}",
            f"new_dataset={ROUND38_REPAIRED_DIR}",
            f"base_recipe_count={base_count}",
            f"recovered_ready_recipe_count={len(recovered_ids)}",
            f"new_recipe_count={new_count}",
            f"recovered_recipe_ids={', '.join(sorted(recovered_ids)) if recovered_ids else 'none'}",
            "",
        ]
    )


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def append_note(existing: str, note: str) -> str:
    parts = [part.strip() for part in existing.split(";") if part.strip()]
    if note and note not in parts:
        parts.append(note)
    return "; ".join(parts)


if __name__ == "__main__":
    main()
