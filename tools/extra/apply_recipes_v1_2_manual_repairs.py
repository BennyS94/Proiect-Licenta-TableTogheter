from __future__ import annotations

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra import build_recipes_v1_2_round28_plus30_nutrition_cache as round28
from tools.extra import build_recipes_v1_2_round30_plus15_nutrition_cache as round30

RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"

REVIEW_PATH = AUDIT_DIR / "recipes_v1_2_manual_repair_review.csv"
BASE_DATASET_DIR = RECIPES_DRAFT_DIR / "v1_2_generator_ready_plus30_plus15"
REPAIRED_DATASET_DIR = RECIPES_DRAFT_DIR / "v1_2_generator_ready_plus30_plus15_repaired"

OUT_APPLIED_RECIPES = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_applied_recipes.csv"
OUT_APPLIED_INGREDIENTS = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_applied_ingredients.csv"
OUT_APPLIED_CACHE = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_applied_nutrition_cache.csv"
OUT_SUMMARY = AUDIT_DIR / "recipes_v1_2_manual_repair_applied_summary.txt"
OUT_AUDIT = AUDIT_DIR / "recipes_v1_2_manual_repair_applied_audit.csv"
OUT_DEFERRED = AUDIT_DIR / "recipes_v1_2_manual_repair_deferred.csv"
OUT_MATERIALIZATION_SUMMARY = AUDIT_DIR / "recipes_v1_2_manual_repair_materialization_summary.txt"

ROUND_MODULES = {
    "round28": round28,
    "round30": round30,
}

APPLIED_AUDIT_COLUMNS = [
    "repair_id",
    "recipe_id_candidate",
    "display_name",
    "created_from_round",
    "blocking_ingredient",
    "repair_decision",
    "approved_fix_type",
    "approved_food_id",
    "approved_quantity_grams_estimated",
    "applied",
    "before_generator_ready",
    "after_generator_ready",
    "recovered_generator_ready",
    "before_failure_reason",
    "after_failure_reason",
    "after_energy_kcal_per_serving",
    "after_protein_g_per_serving",
    "after_carbs_g_per_serving",
    "after_fat_g_per_serving",
    "application_notes",
]


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    RECIPES_DRAFT_DIR.mkdir(parents=True, exist_ok=True)

    review_rows = read_csv(REVIEW_PATH)
    applied_review_rows = [
        row for row in review_rows
        if row.get("apply_repair", "").strip().lower() == "true"
        and row.get("repair_decision") in {"approve_safe", "approve_with_review"}
    ]
    deferred_rows = [row for row in review_rows if row not in applied_review_rows]
    write_csv(OUT_DEFERRED, deferred_rows, list(review_rows[0].keys()) if review_rows else [])

    all_applied_recipe_rows: list[dict[str, Any]] = []
    all_applied_ingredient_rows: list[dict[str, Any]] = []
    all_applied_cache_rows: list[dict[str, Any]] = []
    applied_audit_rows: list[dict[str, Any]] = []
    recovered_ids: set[str] = set()

    for round_id, module in ROUND_MODULES.items():
        round_review_rows = [
            row for row in applied_review_rows
            if row.get("created_from_round") == round_id
        ]
        if not round_review_rows:
            continue
        result = apply_round_repairs(module, round_id, round_review_rows)
        all_applied_recipe_rows.extend(result["recipe_rows"])
        all_applied_ingredient_rows.extend(result["ingredient_rows"])
        all_applied_cache_rows.extend(result["cache_rows"])
        applied_audit_rows.extend(result["audit_rows"])
        recovered_ids.update(result["recovered_ids"])

    base_recipes = read_csv(BASE_DATASET_DIR / "recipes.csv")
    base_ingredients = read_csv(BASE_DATASET_DIR / "recipe_ingredients.csv")
    base_cache = read_csv(BASE_DATASET_DIR / "recipe_nutrition_cache.csv")
    write_csv(OUT_APPLIED_RECIPES, all_applied_recipe_rows, list(base_recipes[0].keys()))
    write_csv(OUT_APPLIED_INGREDIENTS, all_applied_ingredient_rows, list(base_ingredients[0].keys()))
    write_csv(OUT_APPLIED_CACHE, all_applied_cache_rows, list(base_cache[0].keys()))
    write_csv(OUT_AUDIT, applied_audit_rows, APPLIED_AUDIT_COLUMNS)

    materialize_repaired_dataset(
        base_recipes=base_recipes,
        base_ingredients=base_ingredients,
        base_cache=base_cache,
        repaired_recipes=all_applied_recipe_rows,
        repaired_ingredients=all_applied_ingredient_rows,
        repaired_cache=all_applied_cache_rows,
        recovered_ids=recovered_ids,
    )
    OUT_SUMMARY.write_text(
        build_apply_summary(
            review_rows=review_rows,
            applied_review_rows=applied_review_rows,
            deferred_rows=deferred_rows,
            applied_audit_rows=applied_audit_rows,
            recovered_ids=recovered_ids,
            new_dataset_recipe_count=len(base_recipes) + len(all_applied_recipe_rows),
        ),
        encoding="utf-8",
    )

    print("Recipes_DB v1.2 manual repairs applied")
    print(f"- reviewed rows: {len(review_rows)}")
    print(f"- approved/applied repair rows: {len(applied_review_rows)}")
    print(f"- recovered generator-ready recipes: {len(recovered_ids)}")
    print(f"- repaired dataset: {REPAIRED_DATASET_DIR}")


def apply_round_repairs(module: Any, round_id: str, review_rows: list[dict[str, str]]) -> dict[str, Any]:
    recipes = module.read_csv(module.RECIPES)
    mapping_rows = module.read_csv(module.OUT_MATCHES)
    food_lookup = module.build_food_lookup(module.read_csv(module.FOODDB))
    original_cache_rows = module.read_csv(module.OUT_CACHE)
    original_cache_by_id = {row["recipe_id_candidate"]: row for row in original_cache_rows}
    recipe_by_id = {row["recipe_id_candidate"]: row for row in recipes}

    review_by_recipe = group_review_rows(review_rows)
    repaired_mapping_rows = apply_mapping_repairs(mapping_rows, review_by_recipe, food_lookup)
    cache_rows, _audit_rows = module.build_cache_rows(recipes, repaired_mapping_rows, food_lookup)
    cache_by_id = {row["recipe_id_candidate"]: row for row in cache_rows}

    recovered_ids = {
        recipe_id
        for recipe_id, cache in cache_by_id.items()
        if recipe_id in review_by_recipe
        and str(cache.get("generator_ready_candidate", "")).lower() == "true"
        and str(original_cache_by_id.get(recipe_id, {}).get("generator_ready_candidate", "")).lower() != "true"
    }

    recipe_rows: list[dict[str, Any]] = []
    ingredient_rows: list[dict[str, Any]] = []
    materialized_cache_rows: list[dict[str, Any]] = []
    for recipe_id in sorted(recovered_ids):
        recipe_rows.append(mark_recipe_repaired(module.materialized_recipe_row(recipe_by_id[recipe_id], cache_by_id[recipe_id])))
        recipe_mapping_rows = [
            row for row in repaired_mapping_rows
            if clean_text(row.get("recipe_id_candidate")) == recipe_id
        ]
        ingredient_rows.extend(
            mark_ingredient_repaired(module.materialized_ingredient_row(row))
            for row in recipe_mapping_rows
        )
        materialized_cache_rows.append(mark_cache_repaired(module.materialized_cache_row(cache_by_id[recipe_id])))

    applied_audit_rows = []
    for review in review_rows:
        recipe_id = review.get("recipe_id_candidate", "")
        before = original_cache_by_id.get(recipe_id, {})
        after = cache_by_id.get(recipe_id, {})
        applied_audit_rows.append(
            {
                "repair_id": review.get("repair_id", ""),
                "recipe_id_candidate": recipe_id,
                "display_name": review.get("display_name", ""),
                "created_from_round": round_id,
                "blocking_ingredient": review.get("blocking_ingredient", ""),
                "repair_decision": review.get("repair_decision", ""),
                "approved_fix_type": review.get("approved_fix_type", ""),
                "approved_food_id": review.get("approved_food_id", ""),
                "approved_quantity_grams_estimated": review.get("approved_quantity_grams_estimated", ""),
                "applied": "true",
                "before_generator_ready": before.get("generator_ready_candidate", ""),
                "after_generator_ready": after.get("generator_ready_candidate", ""),
                "recovered_generator_ready": str(recipe_id in recovered_ids).lower(),
                "before_failure_reason": before.get("generator_ready_failure_reason", ""),
                "after_failure_reason": after.get("generator_ready_failure_reason", ""),
                "after_energy_kcal_per_serving": after.get("energy_kcal_per_serving", ""),
                "after_protein_g_per_serving": after.get("protein_g_per_serving", ""),
                "after_carbs_g_per_serving": after.get("carbs_g_per_serving", ""),
                "after_fat_g_per_serving": after.get("fat_g_per_serving", ""),
                "application_notes": "round36_manual_repair_applied",
            }
        )

    return {
        "recipe_rows": recipe_rows,
        "ingredient_rows": ingredient_rows,
        "cache_rows": materialized_cache_rows,
        "audit_rows": applied_audit_rows,
        "recovered_ids": recovered_ids,
    }


def group_review_rows(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("recipe_id_candidate", "")].append(row)
    return grouped


def apply_mapping_repairs(
    mapping_rows: list[dict[str, Any]],
    review_by_recipe: dict[str, list[dict[str, str]]],
    food_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    repaired_rows = []
    for row in mapping_rows:
        updated = dict(row)
        recipe_id = clean_text(row.get("recipe_id_candidate"))
        ingredient = normalize_ingredient(row.get("ingredient_name_normalized"))
        for review in review_by_recipe.get(recipe_id, []):
            if ingredient != normalize_ingredient(review.get("ingredient_name_normalized") or review.get("blocking_ingredient")):
                continue
            approved_grams = clean_text(review.get("approved_quantity_grams_estimated"))
            approved_food_id = clean_text(review.get("approved_food_id"))
            if approved_grams:
                updated["quantity_grams_estimated"] = approved_grams
            if approved_food_id and approved_food_id in food_lookup:
                updated["mapped_food_id"] = approved_food_id
                updated["mapped_food_canonical_name"] = food_lookup[approved_food_id]["canonical_name"]
                updated["mapping_status"] = "accepted_auto"
                updated["mapping_confidence"] = "high" if review.get("repair_decision") == "approve_safe" else "medium"
                updated["mapping_method"] = f"round36_manual_repair_{review.get('approved_fix_type', '')}"
                updated["mapping_notes"] = "round36_manual_repair_local_fooddb"
                updated["manual_decision_notes"] = append_note(
                    clean_text(updated.get("manual_decision_notes")),
                    review.get("repair_id", ""),
                )
        repaired_rows.append(updated)
    return repaired_rows


def materialize_repaired_dataset(
    *,
    base_recipes: list[dict[str, str]],
    base_ingredients: list[dict[str, str]],
    base_cache: list[dict[str, str]],
    repaired_recipes: list[dict[str, Any]],
    repaired_ingredients: list[dict[str, Any]],
    repaired_cache: list[dict[str, Any]],
    recovered_ids: set[str],
) -> None:
    REPAIRED_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    existing_recipe_ids = {row.get("recipe_id", "") for row in base_recipes}
    new_recipes = [
        row for row in repaired_recipes
        if row.get("recipe_id", "") not in existing_recipe_ids
    ]
    new_ids = {row.get("recipe_id", "") for row in new_recipes}
    new_ingredients = [
        row for row in repaired_ingredients
        if row.get("recipe_id", "") in new_ids
    ]
    new_cache = [
        row for row in repaired_cache
        if row.get("recipe_id", "") in new_ids
    ]

    write_csv(REPAIRED_DATASET_DIR / "recipes.csv", base_recipes + new_recipes, list(base_recipes[0].keys()))
    write_csv(
        REPAIRED_DATASET_DIR / "recipe_ingredients.csv",
        base_ingredients + new_ingredients,
        list(base_ingredients[0].keys()),
    )
    write_csv(
        REPAIRED_DATASET_DIR / "recipe_nutrition_cache.csv",
        base_cache + new_cache,
        list(base_cache[0].keys()),
    )
    readme = [
        "Recipes_DB v1.2 plus30_plus15 repaired draft",
        "",
        "Draft/test only. Do not treat as current production data.",
        f"Base dataset: {BASE_DATASET_DIR}",
        f"Recovered generator-ready recipes: {len(new_ids)}",
        "Repair tag: round36_manual_repair",
        "",
    ]
    (REPAIRED_DATASET_DIR / "README_v1_2_generator_ready_plus30_plus15_repaired.txt").write_text(
        "\n".join(readme),
        encoding="utf-8",
    )
    OUT_MATERIALIZATION_SUMMARY.write_text(
        "\n".join(
            [
                "Recipes_DB v1.2 manual repair materialization summary",
                "",
                f"- base_recipe_count: {len(base_recipes)}",
                f"- recovered_ready_recipe_count: {len(new_ids)}",
                f"- repaired_dataset_recipe_count: {len(base_recipes) + len(new_recipes)}",
                f"- recovered_recipe_ids: {', '.join(sorted(recovered_ids)) if recovered_ids else 'none'}",
                f"- output_dir: {REPAIRED_DATASET_DIR}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def build_apply_summary(
    *,
    review_rows: list[dict[str, str]],
    applied_review_rows: list[dict[str, str]],
    deferred_rows: list[dict[str, str]],
    applied_audit_rows: list[dict[str, Any]],
    recovered_ids: set[str],
    new_dataset_recipe_count: int,
) -> str:
    decision_counts = Counter(row.get("repair_decision", "") for row in review_rows)
    applied_fix_counts = Counter(row.get("approved_fix_type", "") for row in applied_review_rows)
    deferred_counts = Counter(row.get("repair_decision", "") for row in deferred_rows)
    recovered_rows = [
        row for row in applied_audit_rows
        if row.get("recovered_generator_ready") == "true"
    ]

    lines = [
        "Recipes_DB v1.2 manual repair applied summary",
        "",
        f"- reviewed repair rows: {len(review_rows)}",
        f"- applied repair rows: {len(applied_review_rows)}",
        f"- deferred/rejected repair rows: {len(deferred_rows)}",
        f"- recovered generator-ready recipes: {len(recovered_ids)}",
        f"- repaired dataset recipe count: {new_dataset_recipe_count}",
        "",
        "Review decisions",
        *format_counter(decision_counts),
        "",
        "Applied fix types",
        *format_counter(applied_fix_counts),
        "",
        "Deferred/rejected decisions",
        *format_counter(deferred_counts),
        "",
        "Recovered recipes",
    ]
    if recovered_rows:
        for row in recovered_rows:
            lines.append(
                "- "
                f"{row['recipe_id_candidate']} | {row['display_name']} | "
                f"kcal={row['after_energy_kcal_per_serving']} "
                f"P={row['after_protein_g_per_serving']} "
                f"C={row['after_carbs_g_per_serving']} "
                f"F={row['after_fat_g_per_serving']}"
            )
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "Strict assessment",
            "- Repairs are draft/test only and use only local Food_DB items.",
            "- Deferred rows still need manual source review or a separate modeling decision.",
        ]
    )
    return "\n".join(lines) + "\n"


def mark_recipe_repaired(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated["qc_notes"] = append_note(clean_text(updated.get("qc_notes")), "round36_manual_repair_recovered")
    return updated


def mark_ingredient_repaired(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    if str(updated.get("mapping_method", "")).startswith("round36_manual_repair"):
        updated["qc_notes"] = append_note(clean_text(updated.get("qc_notes")), "round36_manual_repair_applied")
        updated["qc_ingredient_status"] = "accepted_auto_round36_manual_repair"
    return updated


def mark_cache_repaired(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated["qc_notes"] = append_note(clean_text(updated.get("qc_notes")), "round36_manual_repair_recovered")
    updated["cache_version"] = "recipes_v1_2_round36_manual_repair_001"
    updated["servings_adjustment_applied"] = "false"
    return updated


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


def clean_text(value: object) -> str:
    return "" if value is None else str(value).strip()


def normalize_ingredient(value: object) -> str:
    return clean_text(value).lower()


def append_note(existing: str, note: str) -> str:
    existing = "; ".join(part.strip() for part in existing.split(";") if part.strip())
    if not note:
        return existing
    if not existing:
        return note
    if note in existing.split("; "):
        return existing
    return f"{existing}; {note}"


def format_counter(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key or 'blank'}: {value}" for key, value in counter.most_common()]


if __name__ == "__main__":
    main()
