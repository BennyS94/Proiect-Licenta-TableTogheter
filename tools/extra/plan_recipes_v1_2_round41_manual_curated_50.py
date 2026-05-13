from __future__ import annotations

import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.round41_manual_curated_common import (
    BASELINE_DIR,
    RECIPES_AUDIT_DIR,
    RECIPES_DRAFT_DIR,
    expected_ranges_for_recipe,
    read_csv,
    recipe_catalog,
    write_csv,
    write_text,
)


OUT_PLAN = RECIPES_DRAFT_DIR / "recipes_v1_2_round41_manual_curated_50_plan.csv"
OUT_SUMMARY = RECIPES_AUDIT_DIR / "recipes_v1_2_round41_manual_curated_50_plan_summary.txt"


def main() -> None:
    baseline_names = {
        str(row.get("display_name", "")).strip().lower()
        for row in read_csv(BASELINE_DIR / "recipes.csv")
    }
    rows = []
    duplicate_risks = []
    for item in recipe_catalog():
        kcal_range, protein_range, carbs_range = expected_ranges_for_recipe(item)
        display_name = str(item["display_name"])
        duplicate_risk = "possible_duplicate" if display_name.strip().lower() in baseline_names else "low"
        if duplicate_risk != "low":
            duplicate_risks.append(display_name)
        rows.append(
            {
                "manual_recipe_id": item["manual_recipe_id"],
                "display_name": display_name,
                "recipe_kind": item["recipe_kind"],
                "allowed_slots_json": item["allowed_slots_json"],
                "target_bucket": item["target_bucket"],
                "primary_protein": item["primary_protein"],
                "carb_source": item["carb_source"],
                "veg_component": item["veg_component"],
                "expected_kcal_range": kcal_range,
                "expected_protein_range": protein_range,
                "expected_carbs_range": carbs_range,
                "expected_generator_value": expected_value(item),
                "duplicate_risk": duplicate_risk,
                "ingredient_mapping_risk": mapping_risk(item),
                "notes": item["notes"],
            }
        )
    write_csv(
        OUT_PLAN,
        rows,
        [
            "manual_recipe_id",
            "display_name",
            "recipe_kind",
            "allowed_slots_json",
            "target_bucket",
            "primary_protein",
            "carb_source",
            "veg_component",
            "expected_kcal_range",
            "expected_protein_range",
            "expected_carbs_range",
            "expected_generator_value",
            "duplicate_risk",
            "ingredient_mapping_risk",
            "notes",
        ],
    )
    counts = count_by_slot(rows)
    summary = [
        "Recipes_DB v1.2 Round41 manual-curated 50 plan summary",
        "",
        f"planned_recipes={len(rows)}",
        f"breakfast={counts.get('breakfast', 0)}",
        f"lunch_dinner={counts.get('lunch_dinner', 0)}",
        f"snack={counts.get('snack', 0)}",
        f"duplicate_risks={len(duplicate_risks)}",
        f"duplicate_risk_names={'; '.join(duplicate_risks)}",
        "",
        "Strategy:",
        "- Use explicit grams and Food_DB canonical ingredients only.",
        "- Prefer clean manual recipes with predictable mapping over volume.",
        "- Fill Round40 gaps: lunch/dinner carb-protein, fish/turkey/pork, legumes, and non-waffle/oat/egg breakfast patterns.",
    ]
    write_text(OUT_SUMMARY, "\n".join(summary))
    print("\n".join(summary))


def expected_value(item: dict[str, object]) -> str:
    bucket = str(item["target_bucket"])
    protein = str(item["primary_protein"])
    if bucket == "breakfast_competitor":
        return "breakfast_variety"
    if bucket == "snack":
        return "snack_variety"
    if "fish" in protein:
        return "fish_variety"
    if "turkey" in protein:
        return "turkey_variety"
    if "pork" in protein:
        return "pork_variety"
    if "legume" in protein or bucket == "vegetarian_legume_balanced":
        return "vegetarian_balanced"
    return "lunch_dinner_variety"


def mapping_risk(item: dict[str, object]) -> str:
    ingredients = item.get("ingredients", [])
    canonicals = {str(row.get("canonical", "")) for row in ingredients}
    medium = {
        "turkey_cooked_ham_in_slices",
        "sour_cream_light",
        "drained_soft_fresh_cheese_around_6_fat",
        "salmon_smoked",
    }
    if canonicals.intersection(medium):
        return "medium"
    return "low"


def count_by_slot(rows: list[dict[str, str]]) -> dict[str, int]:
    counts = {"breakfast": 0, "lunch_dinner": 0, "snack": 0}
    for row in rows:
        slots = json.loads(row["allowed_slots_json"])
        if slots == ["breakfast"]:
            counts["breakfast"] += 1
        elif slots == ["snack"]:
            counts["snack"] += 1
        elif "lunch" in slots or "dinner" in slots:
            counts["lunch_dinner"] += 1
    return counts


if __name__ == "__main__":
    main()
