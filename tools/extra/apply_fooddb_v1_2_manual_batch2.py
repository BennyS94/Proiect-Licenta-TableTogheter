from __future__ import annotations

import csv
import math
import sys
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.generator_v1.data_loader import V1_2_DEMO_CANDIDATE_PROFILE  # noqa: E402
from tools.extra import apply_fooddb_v1_2_manual_batch1 as batch1  # noqa: E402


FOODDB_BASE = REPO_ROOT / "data/fooddb/draft/fooddb_v1_2_core_master_manual_batch1.csv"
VERIFIED_BATCH = REPO_ROOT / "data/fooddb/draft/fooddb_v1_2_manual_additions_verified_batch2.csv"
FOODDB_BATCH2 = REPO_ROOT / "data/fooddb/draft/fooddb_v1_2_core_master_manual_batch2.csv"
VALIDATION_OUT = REPO_ROOT / "data/fooddb/audit/fooddb_v1_2_manual_batch2_validation.csv"
SUMMARY_OUT = REPO_ROOT / "data/fooddb/audit/fooddb_v1_2_manual_batch2_summary.txt"

RECIPES_DRAFT_DIR = REPO_ROOT / "data/recipesdb/draft"
RECIPES_AUDIT_DIR = REPO_ROOT / "data/recipesdb/audit"
BASE_DATASET_DIR = RECIPES_DRAFT_DIR / "v1_2_demo_candidate"
BATCH2_DATASET_DIR = RECIPES_DRAFT_DIR / "v1_2_demo_candidate_manual_batch2"

OUT_AFFECTED_MATCHES = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_batch2_affected_food_matches.csv"
OUT_AFFECTED_NUTRITION = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_batch2_affected_nutrition_cache.csv"
OUT_MAPPING_AUDIT = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_batch2_mapping_audit.csv"
OUT_RECOVERED = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_batch2_recovered_recipes.csv"
OUT_MATERIALIZATION_SUMMARY = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_batch2_materialization_summary.txt"
OUT_MATERIALIZATION_AUDIT = RECIPES_AUDIT_DIR / "recipes_v1_2_manual_batch2_materialization_audit.csv"

OUT_IMPACT_SUMMARY = RECIPES_AUDIT_DIR / "generator_v1_manual_batch2_impact_summary.txt"
OUT_IMPACT_DAYS = RECIPES_AUDIT_DIR / "generator_v1_manual_batch2_days.csv"
OUT_IMPACT_MEALS = RECIPES_AUDIT_DIR / "generator_v1_manual_batch2_meals.csv"
OUT_IMPACT_REPETITION = RECIPES_AUDIT_DIR / "generator_v1_manual_batch2_repetition.csv"

BATCH2_NUTRITION_BASIS = "recipes_v1_2_manual_batch2_verified_fooddb_draft"
BATCH2_CACHE_VERSION = "recipes_v1_2_manual_batch2_001"
BATCH2_TAG = "manual_batch2_verified_fooddb"

ROUND_INPUTS = [
    {
        "round": "round42",
        "recipes": RECIPES_DRAFT_DIR / "recipes_v1_2_round42_dataset_curated_selected.csv",
        "matches": RECIPES_DRAFT_DIR / "recipes_v1_2_round42_dataset_food_matches.csv",
    },
]

SAFE_MAPPING_RULES = {
    "baked beans with pork": "baked_beans_with_pork",
    "beef stew meat": "beef_stew_meat",
    "beef top sirloin": "beef_top_sirloin",
    "cod fillets": "cod_fillets",
    "crumbled cotija cheese": "crumbled_cotija_cheese",
    "dry white beans": "dry_white_beans",
    "flank steak": "flank_steak",
}


def main() -> None:
    configure_batch1_adapter()
    RECIPES_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    (REPO_ROOT / "data/fooddb/audit").mkdir(parents=True, exist_ok=True)

    base_fooddb = batch1.read_csv(FOODDB_BASE)
    verified_rows = batch1.read_csv(VERIFIED_BATCH)
    validation_rows, safe_rows = batch1.validate_verified_rows(verified_rows, base_fooddb)
    batch_fooddb = base_fooddb + [fooddb_row_from_verified(row, base_fooddb) for row in safe_rows]
    batch1.write_csv(FOODDB_BATCH2, batch_fooddb, list(base_fooddb[0].keys()))
    batch1.write_csv(VALIDATION_OUT, validation_rows, batch1.VALIDATION_COLUMNS)

    food_lookup = batch1.round37.base.build_food_lookup(batch_fooddb)
    safe_by_canonical = {row["canonical_name"]: row for row in safe_rows}
    affected = batch1.rebuild_affected_recipes(food_lookup, safe_by_canonical)
    materialization = batch1.materialize_batch1_dataset(affected)
    impact_rows = evaluate_multiday_impact()

    SUMMARY_OUT.write_text(
        build_summary(validation_rows, safe_rows, affected, materialization, impact_rows),
        encoding="utf-8",
    )
    print("Food_DB v1.2 manual batch2 applied to draft/audit outputs")
    print(f"fooddb_draft={FOODDB_BATCH2}")
    print(f"safe_additions={len(safe_rows)}")
    print(f"recovered_recipes={len(materialization['recovered_ids'])}")
    print(f"impact_summary={OUT_IMPACT_SUMMARY}")


def configure_batch1_adapter() -> None:
    batch1.FOODDB_BASE = FOODDB_BASE
    batch1.VERIFIED_BATCH = VERIFIED_BATCH
    batch1.FOODDB_BATCH1 = FOODDB_BATCH2
    batch1.VALIDATION_OUT = VALIDATION_OUT
    batch1.SUMMARY_OUT = SUMMARY_OUT

    batch1.BASE_DATASET_DIR = BASE_DATASET_DIR
    batch1.BATCH1_DATASET_DIR = BATCH2_DATASET_DIR
    batch1.OUT_AFFECTED_MATCHES = OUT_AFFECTED_MATCHES
    batch1.OUT_AFFECTED_NUTRITION = OUT_AFFECTED_NUTRITION
    batch1.OUT_MAPPING_AUDIT = OUT_MAPPING_AUDIT
    batch1.OUT_RECOVERED = OUT_RECOVERED
    batch1.OUT_MATERIALIZATION_SUMMARY = OUT_MATERIALIZATION_SUMMARY
    batch1.OUT_MATERIALIZATION_AUDIT = OUT_MATERIALIZATION_AUDIT
    batch1.OUT_IMPACT_SUMMARY = OUT_IMPACT_SUMMARY
    batch1.OUT_IMPACT_DAYS = OUT_IMPACT_DAYS
    batch1.OUT_IMPACT_MEALS = OUT_IMPACT_MEALS
    batch1.OUT_IMPACT_REPETITION = OUT_IMPACT_REPETITION

    batch1.BATCH1_NUTRITION_BASIS = BATCH2_NUTRITION_BASIS
    batch1.BATCH1_CACHE_VERSION = BATCH2_CACHE_VERSION
    batch1.BATCH1_TAG = BATCH2_TAG
    batch1.ROUND_INPUTS = ROUND_INPUTS
    batch1.SAFE_MAPPING_RULES = SAFE_MAPPING_RULES
    batch1.V1_2_GENERATOR_READY_ROUND37_EXPANDED_REPAIRED_PROFILE = V1_2_DEMO_CANDIDATE_PROFILE

    batch1.fooddb_row_from_verified = fooddb_row_from_verified
    batch1.apply_batch1_mapping_rules = apply_batch2_mapping_rules
    batch1.source_dataset_for_round = source_dataset_for_round
    batch1.evaluate_multiday_impact = evaluate_multiday_impact
    batch1.build_summary = build_summary
    batch1.build_impact_summary = build_impact_summary
    batch1.build_materialization_summary = build_materialization_summary


def fooddb_row_from_verified(row: dict[str, str], base_fooddb: list[dict[str, str]]) -> dict[str, str]:
    output = {column: "" for column in base_fooddb[0].keys()}
    role = clean(row.get("role"))
    source_group = clean(row.get("food_group"))
    group_key = normalized_group_key(source_group, role)
    state = clean(row.get("raw_or_cooked_state"))
    kcal = parse_float(row.get("energy_kcal_100")) or 0.0
    protein = parse_float(row.get("protein_g_100")) or 0.0
    carbs = parse_float(row.get("carbs_g_100")) or 0.0
    fat = parse_float(row.get("fat_g_100")) or 0.0
    canonical = clean(row.get("canonical_name"))
    output.update(
        {
            "food_id": clean(row.get("candidate_food_id")),
            "canonical_name": canonical,
            "display_name": clean(row.get("display_name")),
            "food_family_name": clean(row.get("display_name")),
            "entity_level": entity_level_for_state(state),
            "food_group": food_group_for_verified(group_key),
            "food_subgroup": food_subgroup_for_verified(group_key, state),
            "energy_kcal_100g": format_float(kcal),
            "protein_g_100g": format_float(protein),
            "carbs_g_100g": format_float(carbs),
            "fat_g_100g": format_float(fat),
            "processing_state": state,
            "preservation_state": "",
            "helper_macro_profile": batch1.helper_macro_profile(protein, carbs, fat),
            "helper_use_as_protein": str(protein >= 8 or role in {"protein", "carb_protein"}),
            "helper_use_as_carb_side": str(carbs >= 15 or role in {"carb", "carb_protein"}),
            "helper_use_as_veg_side": "False",
            "helper_is_sweet": "False",
            "helper_is_salty": str(group_key in {"processed_legume", "dairy_cheese"}),
            "helper_is_drink": "False",
            "helper_is_vegetarian": str(group_key not in {"meat_beef", "fish"}),
            "helper_is_vegan": str(group_key in {"legumes", "processed_legume"}),
            "helper_protein_bucket": protein_bucket(group_key, protein, fat),
            "helper_carb_bucket": carb_bucket(group_key, carbs),
            "helper_veg_bucket": "",
            "primary_source_uid": clean(row.get("candidate_food_id")),
            "primary_source_name": clean(row.get("source_name")),
            "primary_source_ciqual_code": "",
            "primary_source_name_tags": clean(row.get("source_type")),
            "qc_macro_complete": "True",
            "qc_taxonomy_complete": "True",
            "qc_canonicalization_status": "manual_batch2_verified_source",
            "qc_scope_status": "accepted_core",
            "qc_source_merge_count": "1",
            "qc_notes": build_fooddb_qc_notes(row),
        }
    )
    return output


def evaluate_multiday_impact() -> list[dict[str, Any]]:
    scenarios = [
        {
            "scenario": "v1_2_demo_candidate",
            "recipes": BASE_DATASET_DIR / "recipes.csv",
            "ingredients": BASE_DATASET_DIR / "recipe_ingredients.csv",
            "nutrition": BASE_DATASET_DIR / "recipe_nutrition_cache.csv",
            "fooddb": FOODDB_BASE,
        },
        {
            "scenario": "manual_batch2",
            "recipes": BATCH2_DATASET_DIR / "recipes.csv",
            "ingredients": BATCH2_DATASET_DIR / "recipe_ingredients.csv",
            "nutrition": BATCH2_DATASET_DIR / "recipe_nutrition_cache.csv",
            "fooddb": FOODDB_BATCH2,
        },
    ]
    rows: list[dict[str, Any]] = []
    day_rows: list[dict[str, Any]] = []
    meal_rows: list[dict[str, Any]] = []
    repetition_rows: list[dict[str, Any]] = []
    for scenario in scenarios:
        plan, diagnostics = batch1.run_multiday_scenario(scenario)
        row = batch1.summary_row(scenario, plan, diagnostics)
        rows.append(row)
        day_rows.extend(batch1.day_rows_for_scenario(scenario, plan))
        meal_rows.extend(batch1.meal_rows_for_scenario(scenario, plan))
        repetition_rows.extend(batch1.repetition_rows_for_scenario(scenario, plan))
    batch1.write_csv(OUT_IMPACT_DAYS, day_rows, list(day_rows[0].keys()) if day_rows else [])
    batch1.write_csv(OUT_IMPACT_MEALS, meal_rows, list(meal_rows[0].keys()) if meal_rows else [])
    batch1.write_csv(OUT_IMPACT_REPETITION, repetition_rows, list(repetition_rows[0].keys()) if repetition_rows else [])
    OUT_IMPACT_SUMMARY.write_text(build_impact_summary(rows), encoding="utf-8")
    return rows


def apply_batch2_mapping_rules(
    *,
    rows: list[dict[str, str]],
    source_round: str,
    safe_by_canonical: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], set[str], list[dict[str, str]]]:
    updated_rows: list[dict[str, Any]] = []
    affected_ids: set[str] = set()
    audit_rows: list[dict[str, str]] = []
    for row in rows:
        updated = dict(row)
        ingredient = batch1.normalize_ingredient(row.get("ingredient_name_normalized"))
        canonical = SAFE_MAPPING_RULES.get(ingredient, "")
        safe_row = safe_by_canonical.get(canonical)
        applied = False
        notes = ""
        if safe_row and clean(row.get("mapping_status")) != "accepted_auto":
            updated["mapped_food_id"] = safe_row["candidate_food_id"]
            updated["mapped_food_canonical_name"] = safe_row["canonical_name"]
            updated["mapping_status"] = "accepted_auto"
            updated["mapping_confidence"] = "high" if clean(safe_row.get("confidence")) == "high" else "medium"
            updated["mapping_method"] = "manual_batch2_verified_source_alias"
            updated["mapping_notes"] = "manual_batch2_verified_fooddb_item"
            updated["manual_decision_notes"] = batch1.append_note(
                clean(row.get("manual_decision_notes")),
                "manual_batch2_source_verified",
            )
            applied = True
            affected_ids.add(clean(row.get("recipe_id_candidate")))
            notes = "mapped_to_verified_batch2_fooddb_item"
        elif canonical and not safe_row:
            notes = "target_canonical_not_safe_to_add"
        if applied or canonical:
            audit_rows.append(
                {
                    "round": source_round,
                    "recipe_id_candidate": clean(row.get("recipe_id_candidate")),
                    "display_name": clean(row.get("display_name")),
                    "ingredient_position": clean(row.get("ingredient_position")),
                    "ingredient_name_normalized": ingredient,
                    "ingredient_raw_text": clean(row.get("ingredient_raw_text")),
                    "previous_mapping_status": clean(row.get("mapping_status")),
                    "previous_food_id": clean(row.get("mapped_food_id")),
                    "new_mapping_status": clean(updated.get("mapping_status")),
                    "new_food_id": clean(updated.get("mapped_food_id")),
                    "applied": str(applied).lower(),
                    "application_notes": notes,
                }
            )
        updated_rows.append(updated)
    return updated_rows, affected_ids, audit_rows


def build_impact_summary(rows: list[dict[str, Any]]) -> str:
    lines = ["Generator v1 manual batch2 impact summary", ""]
    for row in rows:
        lines.append(
            "- {scenario}: recipes={recipe_count}, valid={valid_day_count}, accept={accept_day_count}, "
            "review={review_day_count}, unique={unique_recipe_count}, repeated={repeated_recipe_count}, "
            "loss={multi_day_loss}, runtime_s={runtime_seconds}".format(**row)
        )
    return "\n".join(lines) + "\n"


def build_summary(
    validation_rows: list[dict[str, str]],
    safe_rows: list[dict[str, str]],
    affected: dict[str, Any],
    materialization: dict[str, Any],
    impact_rows: list[dict[str, Any]],
) -> str:
    validation_counts = Counter(row["apply_status"] for row in validation_rows)
    changed_mappings = [row for row in affected["mapping_audit_rows"] if row.get("applied") == "true"]
    return "\n".join(
        [
            "Food_DB v1.2 manual additions batch2 summary",
            "",
            f"- verified rows: {len(validation_rows)}",
            f"- safe additions applied to draft Food_DB: {len(safe_rows)}",
            f"- kept needs_review: {validation_counts.get('kept_needs_review', 0)}",
            f"- kept deferred: {validation_counts.get('kept_deferred', 0)}",
            f"- rejected by sanity: {validation_counts.get('rejected_sanity', 0)}",
            f"- affected mapping rows changed: {len(changed_mappings)}",
            f"- affected recipe count: {len(affected['cache_rows'])}",
            f"- recovered generator-ready recipes: {len(materialization['recovered_ids'])}",
            f"- new dataset recipe count: {materialization['new_count']}",
            "",
            "Safe additions applied",
            *[f"- {row.get('canonical_name', '')}" for row in safe_rows],
            "",
            "Recovered recipe ids",
            f"- {', '.join(sorted(materialization['recovered_ids'])) or 'none'}",
            "",
            "Multi-day impact",
            *[
                (
                    f"- {row['scenario']}: recipes={row['recipe_count']}, valid={row['valid_day_count']}, "
                    f"accept={row['accept_day_count']}, review={row['review_day_count']}, "
                    f"unique={row['unique_recipe_count']}, repeated={row['repeated_recipe_count']}, "
                    f"loss={row['multi_day_loss']}, runtime_s={row['runtime_seconds']}"
                )
                for row in impact_rows
            ],
            "",
            "Strict notes",
            "- data/fooddb/current and data/recipesdb/current were not modified.",
            "- needs_review and keep_deferred rows were not applied.",
            "- Nutrition values came only from the verified batch2 CSV.",
            "",
        ]
    )


def build_materialization_summary(
    base_recipes: list[dict[str, str]],
    recovered_ids: set[str],
    cache_by_id: dict[str, dict[str, Any]],
) -> str:
    ready_count = sum(1 for row in cache_by_id.values() if row.get("generator_ready_candidate") == "true")
    strong_count = sum(1 for row in cache_by_id.values() if row.get("strong_generator_ready") == "true")
    return "\n".join(
        [
            "Recipes_DB v1.2 manual batch2 materialization summary",
            "",
            f"base_dataset={BASE_DATASET_DIR}",
            f"new_dataset={BATCH2_DATASET_DIR}",
            f"base_recipe_count={len(base_recipes)}",
            f"affected_recipe_count={len(cache_by_id)}",
            f"affected_ready_count={ready_count}",
            f"affected_strong_ready_count={strong_count}",
            f"recovered_ready_recipe_count={len(recovered_ids)}",
            f"new_recipe_count={len(base_recipes) + len(recovered_ids)}",
            f"recovered_recipe_ids={', '.join(sorted(recovered_ids))}",
            "",
        ]
    )


def source_dataset_for_round(source_round: str) -> str:
    if source_round == "round42":
        return "recipes_dataset_64k_dishes_round42_dataset_curated_manual_batch2"
    return f"recipes_dataset_64k_dishes_{source_round}_manual_batch2"


def normalized_group_key(source_group: str, role: str) -> str:
    text = source_group.lower()
    if "beef" in text:
        return "meat_beef"
    if "finfish" in text or "shellfish" in text or "fish" in text:
        return "fish"
    if "legume" in text:
        return "processed_legume" if "baked" in role else "legumes"
    if "dairy" in text or "egg products" in text:
        return "dairy_cheese"
    if role == "carb_protein":
        return "legumes"
    return "other"


def entity_level_for_state(state: str) -> str:
    if any(token in state.lower() for token in ["processed", "canned", "cheese", "sauce"]):
        return "semi_atomic"
    return "atomic"


def food_group_for_verified(group: str) -> str:
    if group in {"meat_beef", "fish"}:
        return "meat, egg and fish"
    if group in {"legumes", "processed_legume"}:
        return "fruits, vegetables, legumes and nuts"
    if group == "dairy_cheese":
        return "milk and milk products"
    return "other"


def food_subgroup_for_verified(group: str, state: str) -> str:
    state_lower = state.lower()
    if group == "meat_beef":
        return "raw meat" if "raw" in state_lower else "meat"
    if group == "fish":
        return "seafood, raw" if "raw" in state_lower else "seafood"
    if group == "legumes":
        return "legumes, dry" if "dry" in state_lower or "raw" in state_lower else "legumes"
    if group == "processed_legume":
        return "legumes, canned"
    if group == "dairy_cheese":
        return "cheese"
    return "review"


def protein_bucket(group: str, protein: float, fat: float) -> str:
    if group == "meat_beef":
        return "lean_red" if fat < 10 else "fatty_red"
    if group == "fish":
        return "fish_white"
    if group == "dairy_cheese":
        return "dairy_fat"
    if protein >= 10:
        return "plant_protein"
    return ""


def carb_bucket(group: str, carbs: float) -> str:
    if group in {"legumes", "processed_legume"}:
        return "legume"
    if carbs >= 20:
        return "carb_source"
    return ""


def build_fooddb_qc_notes(row: dict[str, str]) -> str:
    parts = [
        BATCH2_TAG,
        f"source_url={clean(row.get('source_url'))}",
        f"confidence={clean(row.get('confidence'))}",
        clean(row.get("qc_notes")),
    ]
    return "; ".join(part for part in parts if part)


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def parse_float(value: Any) -> float | None:
    text = clean(value)
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def format_float(value: Any) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if abs(number - round(number)) < 0.000001:
        return str(int(round(number)))
    return f"{number:.6f}".rstrip("0").rstrip(".")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


if __name__ == "__main__":
    main()
