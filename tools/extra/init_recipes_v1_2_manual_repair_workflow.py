from __future__ import annotations

import csv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
FOODDB_DRAFT_DIR = REPO_ROOT / "data" / "fooddb" / "draft"
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"

MANUAL_REPAIR_QUEUE_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_queue.csv"
FOODDB_ADDITIONS_PATH = FOODDB_DRAFT_DIR / "fooddb_v1_2_manual_additions_candidates.csv"
STRATEGY_SUMMARY_PATH = AUDIT_DIR / "recipes_v1_2_expansion_strategy_summary.txt"
WORKFLOW_README_PATH = AUDIT_DIR / "recipes_v1_2_manual_repair_workflow_readme.txt"
MANUAL_RECIPE_TEMPLATE_PATH = RECIPES_DRAFT_DIR / "manual_recipe_template_v1_2.csv"
MANUAL_RECIPE_README_PATH = AUDIT_DIR / "manual_recipe_template_v1_2_readme.txt"


MANUAL_REPAIR_COLUMNS = [
    "repair_id",
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "recipe_source",
    "recipe_kind_guess",
    "target_slot",
    "target_bucket",
    "priority",
    "problem_type",
    "blocking_ingredient",
    "ingredient_raw_text",
    "ingredient_name_normalized",
    "current_mapping_status",
    "current_quantity_grams_estimated",
    "proposed_fix_type",
    "proposed_fix_detail",
    "needs_fooddb_addition",
    "needs_alias",
    "needs_unit_rule",
    "needs_servings_fix",
    "needs_web_source",
    "source_needed",
    "source_name",
    "source_url",
    "decision_status",
    "decision_notes",
    "expected_generator_value",
    "created_from_round",
    "qc_notes",
]

FOODDB_ADDITION_COLUMNS = [
    "candidate_food_id",
    "canonical_name",
    "display_name",
    "food_group",
    "food_subgroup",
    "role",
    "energy_kcal_100",
    "protein_g_100",
    "carbs_g_100",
    "fat_g_100",
    "source_name",
    "source_url",
    "source_type",
    "added_reason",
    "linked_recipe_ids",
    "linked_ingredients",
    "qc_status",
    "qc_notes",
]

MANUAL_RECIPE_TEMPLATE_COLUMNS = [
    "manual_recipe_id",
    "display_name",
    "recipe_kind",
    "allowed_slots_json",
    "recipe_category",
    "recipe_subcategory",
    "recipe_cuisine",
    "servings",
    "prep_time_min",
    "cook_time_min",
    "total_time_min",
    "directions_json",
    "ingredient_position",
    "ingredient_name_normalized",
    "ingredient_display_name",
    "quantity_grams",
    "mapped_food_id",
    "mapped_food_canonical_name",
    "notes",
]


def main() -> None:
    RECIPES_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    FOODDB_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    actions = [
        create_csv_if_missing(MANUAL_REPAIR_QUEUE_PATH, MANUAL_REPAIR_COLUMNS),
        create_csv_if_missing(FOODDB_ADDITIONS_PATH, FOODDB_ADDITION_COLUMNS),
        create_csv_if_missing(MANUAL_RECIPE_TEMPLATE_PATH, MANUAL_RECIPE_TEMPLATE_COLUMNS),
        write_text_if_missing(STRATEGY_SUMMARY_PATH, strategy_summary_text()),
        write_text_if_missing(WORKFLOW_README_PATH, workflow_readme_text()),
        write_text_if_missing(MANUAL_RECIPE_README_PATH, manual_recipe_readme_text()),
    ]

    print("Recipes_DB v1.2 manual repair workflow initialized")
    for action in actions:
        print(f"- {action}")


def create_csv_if_missing(path: Path, columns: list[str]) -> str:
    if path.exists():
        return f"exists: {path}"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
    return f"created: {path}"


def write_text_if_missing(path: Path, text: str) -> str:
    if path.exists():
        return f"exists: {path}"
    path.write_text(text, encoding="utf-8")
    return f"created: {path}"


def strategy_summary_text() -> str:
    return """Recipes_DB v1.2 expansion strategy

Strategic decision
- Generator v1 is technically usable for one-day and multi-day draft testing.
- Remaining quality bottleneck is data coverage: Recipes_DB size/diversity and Food_DB/mapping coverage.
- Valuable recipes should not be discarded only because automatic mapping fails.
- Failed but useful recipes move into a manual repair queue.

Manual repair may add
- safe alias mappings;
- unit-to-grams rules;
- serving decisions;
- Food_DB ingredient additions with verified nutrition sources.

Boundaries
- Food_DB remains canonical ingredients only.
- Recipes_DB remains composed recipes linked to ingredient rows.
- No broad/random import.
- No production data is modified directly.
- Expansion should optimize generator utility, not raw volume.
"""


def workflow_readme_text() -> str:
    return """Recipes_DB v1.2 manual repair workflow

Files
- data/recipesdb/draft/recipes_v1_2_manual_repair_queue.csv
- data/fooddb/draft/fooddb_v1_2_manual_additions_candidates.csv

Workflow
1. Seed the repair queue from failed but useful targeted recipes.
2. Review each pending row manually.
3. Decide whether the fix is alias_mapping, fooddb_addition, unit_rule, servings_fix, recipe_metadata_fix, recipe_replacement, keep_unmapped, or reject_recipe.
4. If Food_DB nutrition is needed, add a candidate row with a source and keep qc_status=pending until verified.
5. Apply approved fixes in a separate round; do not patch current data directly.

Decision statuses
- pending: row needs review.
- needs_review: more source/context needed.
- approved: ready to apply in a controlled patch.
- rejected: not worth repairing.
- applied: fix was applied in a later draft round.
"""


def manual_recipe_readme_text() -> str:
    return """Manual recipe template v1.2

Purpose
- Manual recipes are allowed for a seed/test library when source data is weak.
- They should be clean, practical recipes with explicit ingredient grams.

Rules
- Ingredients must map to Food_DB or enter the Food_DB additions queue.
- Quantity grams must be explicit.
- Source/type should be marked manual_curated when materialized later.
- Do not fake nutrition; nutrition must come from Food_DB ingredients.
- Do not add production rows directly from this template.

Expected usage
- Add one row per ingredient.
- Repeat recipe-level fields for all ingredient rows of the same manual_recipe_id.
- Keep directions_json as a JSON list string.
"""


if __name__ == "__main__":
    main()
