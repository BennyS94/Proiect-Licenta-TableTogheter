from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path


DEFAULT_UNMAPPED = Path("data/recipesdb/draft/recipes_v1_1_ingredient_food_unmapped.csv")
DEFAULT_MATCHES = Path("data/recipesdb/draft/recipes_v1_1_ingredient_food_matches_draft.csv")
DEFAULT_GAP_AUDIT = Path("data/recipesdb/audit/recipes_v1_1_mapping_fooddb_gap_candidates.csv")
DEFAULT_MAPPING_SUMMARY = Path("data/recipesdb/audit/recipes_v1_1_mapping_pass_summary.txt")
DEFAULT_FOODDB_CURRENT = Path("data/fooddb/current/fooddb_v1_core_master_draft.csv")
DEFAULT_CIQUAL_SOURCE = Path("data/fooddb/source/ciqual2020_cleand.xlsx")
DEFAULT_USDA_SOURCE = Path("data/fooddb/source/comprehensive_foods_usda.csv")
DEFAULT_CANDIDATES_OUT = Path("data/fooddb/draft/fooddb_v1_1_gap_additions_candidates.csv")
DEFAULT_SUMMARY_OUT = Path("data/fooddb/audit/fooddb_v1_1_gap_additions_summary.txt")
DEFAULT_REVIEW_OUT = Path("data/fooddb/audit/fooddb_v1_1_gap_additions_review.csv")
DEFAULT_IMPACT_OUT = Path("data/recipesdb/audit/recipes_v1_1_fooddb_gap_impact.csv")

CANDIDATE_COLUMNS = [
    "ingredient_name_normalized",
    "suggested_food_id",
    "suggested_canonical_name",
    "suggested_display_name",
    "suggested_food_group",
    "suggested_role",
    "source_used",
    "source_row_reference",
    "energy_kcal_100",
    "protein_g_100",
    "carbs_g_100",
    "fat_g_100",
    "frequency_in_v1_1",
    "rows_with_grams",
    "total_grams_affected",
    "example_raw_texts",
    "example_recipes",
    "proposed_action",
    "safety",
    "review_notes",
]

IMPACT_COLUMNS = [
    "ingredient_name_normalized",
    "proposed_action",
    "safety",
    "affected_ingredient_rows",
    "affected_rows_with_grams",
    "total_grams_affected",
    "affected_recipe_count",
    "example_recipe_ids",
    "example_recipes",
    "why_it_matters_for_generator",
]

HIGH_PRIORITY = {
    "beef",
    "vegetable oil",
    "brown sugar",
    "green beans",
    "parmesan cheese",
    "mushrooms",
    "chicken thighs",
    "turkey",
    "butter",
    "lemon",
    "green onions",
}

MEDIUM_PRIORITY = {
    "celery",
    "potatoes",
    "red potatoes",
    "chicken broth",
    "tomato sauce",
    "cornstarch",
    "carrot",
    "carrots",
    "rice",
    "milk",
    "shrimp",
    "chicken breast",
    "beef chuck",
    "onion",
    "onions",
    "yellow onion",
    "red onion",
}

LOW_PRIORITY = {
    "kosher salt",
    "seasoned salt",
    "salt",
    "black pepper",
    "water",
}


@dataclass(frozen=True)
class CandidatePlan:
    role: str
    current_food_id: str = ""
    usda_fdc_id: str = ""
    suggested_food_id: str = ""
    suggested_canonical_name: str = ""
    suggested_display_name: str = ""
    proposed_action: str = "review_needed"
    safety: str = "needs_review"
    review_notes: str = ""


PLANS = {
    "beef": CandidatePlan(
        role="protein",
        current_food_id="food_beef_steak_or_beef_steak_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="broad_beef_term_uses_generic_current_raw_beef_steak_candidate",
    ),
    "vegetable oil": CandidatePlan(
        role="fat",
        current_food_id="food_combined_oil_blended_vegetable_oils",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="generic_vegetable_oil_maps_to_existing_blended_vegetable_oils",
    ),
    "brown sugar": CandidatePlan(
        role="other",
        current_food_id="food_sugar_brown",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="exact_common_ingredient_exists_in_current_fooddb",
    ),
    "green beans": CandidatePlan(
        role="veg",
        usda_fdc_id="169141",
        suggested_food_id="food_green_beans_cooked_unsalted",
        suggested_canonical_name="green_beans_cooked_unsalted",
        suggested_display_name="Green beans, cooked, boiled, drained, without salt",
        proposed_action="add_to_fooddb_draft",
        safety="needs_review",
        review_notes="local_usda_source_exists_but_cooked_state_may_not_match_raw_recipe_line",
    ),
    "parmesan cheese": CandidatePlan(
        role="dairy",
        usda_fdc_id="170848",
        suggested_food_id="food_parmesan_cheese_hard",
        suggested_canonical_name="parmesan_cheese_hard",
        suggested_display_name="Parmesan cheese, hard",
        proposed_action="add_to_fooddb_draft",
        safety="needs_review",
        review_notes="local_usda_source_exact_enough_but_needs_current_fooddb_style_review",
    ),
    "mushrooms": CandidatePlan(
        role="veg",
        current_food_id="food_button_mushroom_or_cultivated_mushroom_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_mushrooms_mapped_to_button_mushroom_current_candidate",
    ),
    "chicken thighs": CandidatePlan(
        role="protein",
        usda_fdc_id="172385",
        suggested_food_id="food_chicken_thigh_meat_and_skin_raw",
        suggested_canonical_name="chicken_thigh_meat_and_skin_raw",
        suggested_display_name="Chicken thigh, meat and skin, raw",
        proposed_action="add_to_fooddb_draft",
        safety="needs_review",
        review_notes="local_usda_source_exists_but_energy_value_needs_review_before_materialization",
    ),
    "turkey": CandidatePlan(
        role="protein",
        current_food_id="food_turkey_meat_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_turkey_maps_to_existing_turkey_meat_raw_candidate",
    ),
    "butter": CandidatePlan(
        role="fat",
        current_food_id="food_butter_82_fat_unsalted",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_butter_could_be_salted_or_unsalted",
    ),
    "unsalted butter": CandidatePlan(
        role="fat",
        current_food_id="food_butter_82_fat_unsalted",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="explicit_unsalted_butter_current_item_exists",
    ),
    "lemon": CandidatePlan(
        role="fruit",
        current_food_id="food_lemon_pulp_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_lemon_may_mean_whole_lemon_or_juice",
    ),
    "green onions": CandidatePlan(
        role="veg",
        current_food_id="food_chive_or_spring_onion_fresh",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="green_onion_scallion_approximated_by_chive_or_spring_onion_current_item",
    ),
    "celery": CandidatePlan(
        role="veg",
        current_food_id="food_celery_stalk_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="current_fooddb_has_celery_stalk_raw",
    ),
    "potatoes": CandidatePlan(
        role="carb",
        current_food_id="food_potato_peeled_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_potatoes_use_raw_peeled_potato_candidate",
    ),
    "red potatoes": CandidatePlan(
        role="carb",
        current_food_id="food_potato_peeled_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="red_potato_specific_item_missing_generic_raw_potato_candidate_exists",
    ),
    "chicken broth": CandidatePlan(
        role="sauce",
        usda_fdc_id="174536",
        suggested_food_id="food_chicken_broth_ready_to_serve",
        suggested_canonical_name="chicken_broth_ready_to_serve",
        suggested_display_name="Chicken broth, ready-to-serve",
        proposed_action="add_to_fooddb_draft",
        safety="needs_review",
        review_notes="local_usda_source_exists_but_broth_concentration_varies_by_recipe",
    ),
    "tomato sauce": CandidatePlan(
        role="sauce",
        usda_fdc_id="169074",
        suggested_food_id="food_tomato_sauce_canned_no_salt_added",
        suggested_canonical_name="tomato_sauce_canned_no_salt_added",
        suggested_display_name="Tomato sauce, canned, no salt added",
        proposed_action="add_to_fooddb_draft",
        safety="needs_review",
        review_notes="local_usda_source_exists_but_energy_value_needs_review_before_materialization",
    ),
    "cornstarch": CandidatePlan(
        role="carb",
        usda_fdc_id="169698",
        suggested_food_id="food_cornstarch",
        suggested_canonical_name="cornstarch",
        suggested_display_name="Cornstarch",
        proposed_action="add_to_fooddb_draft",
        safety="safe_auto",
        review_notes="local_usda_source_exact_common_single_ingredient",
    ),
    "carrot": CandidatePlan(
        role="veg",
        current_food_id="food_carrot_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="current_fooddb_has_carrot_raw",
    ),
    "carrots": CandidatePlan(
        role="veg",
        current_food_id="food_carrot_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="plural_carrots_to_current_carrot_raw",
    ),
    "rice": CandidatePlan(
        role="carb",
        current_food_id="food_rice_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_rice_may_be_raw_or_cooked",
    ),
    "white rice": CandidatePlan(
        role="carb",
        current_food_id="food_rice_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="white_rice_maps_to_current_raw_rice_candidate",
    ),
    "uncooked white rice": CandidatePlan(
        role="carb",
        current_food_id="food_rice_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="explicit_uncooked_white_rice_maps_to_current_raw_rice",
    ),
    "jasmine rice": CandidatePlan(
        role="carb",
        current_food_id="food_rice_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="jasmine_rice_specific_item_missing_generic_raw_rice_candidate_exists",
    ),
    "milk": CandidatePlan(
        role="dairy",
        current_food_id="food_milk_semi_skimmed_pasteurised",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_milk_type_unspecified",
    ),
    "shrimp": CandidatePlan(
        role="protein",
        current_food_id="food_shrimp_or_prawn_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="current_fooddb_has_raw_shrimp_or_prawn",
    ),
    "yellow onion": CandidatePlan(
        role="veg",
        current_food_id="food_yellow_onion_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="current_fooddb_has_yellow_onion_raw",
    ),
    "red onion": CandidatePlan(
        role="veg",
        current_food_id="food_red_onion_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="current_fooddb_has_red_onion_raw",
    ),
    "onion": CandidatePlan(
        role="veg",
        current_food_id="food_yellow_onion_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_onion_defaults_to_yellow_onion_candidate_for_review",
    ),
    "onions": CandidatePlan(
        role="veg",
        current_food_id="food_yellow_onion_raw",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="generic_plural_onions_defaults_to_yellow_onion_candidate_for_review",
    ),
    "chicken breast": CandidatePlan(
        role="protein",
        current_food_id="food_chicken_breast_without_skin_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="current_fooddb_has_chicken_breast_without_skin_raw",
    ),
    "chicken breasts": CandidatePlan(
        role="protein",
        current_food_id="food_chicken_breast_without_skin_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="plural_chicken_breasts_to_current_raw_chicken_breast",
    ),
    "beef chuck": CandidatePlan(
        role="protein",
        current_food_id="food_beef_chuck_raw",
        proposed_action="alias_to_existing",
        safety="safe_auto",
        review_notes="current_fooddb_has_beef_chuck_raw",
    ),
    "kosher salt": CandidatePlan(
        role="seasoning",
        current_food_id="food_salt_white_sea_igneous_or_rock_no_enrichment",
        proposed_action="alias_to_existing",
        safety="needs_review",
        review_notes="low_macro_priority_salt_variant_easy_existing_alias_only",
    ),
    "seasoned salt": CandidatePlan(
        role="seasoning",
        proposed_action="keep_unmapped",
        safety="needs_review",
        review_notes="seasoning_mix_not_a_core_macro_gap",
    ),
    "water": CandidatePlan(
        role="other",
        proposed_action="keep_unmapped",
        safety="safe_auto",
        review_notes="water_low_macro_priority_not_needed_for_macro_cache",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pregateste candidati draft pentru Food_DB v1.1 pe baza gap-urilor Recipes_DB v1.1."
    )
    parser.add_argument("--unmapped", default=str(DEFAULT_UNMAPPED))
    parser.add_argument("--matches", default=str(DEFAULT_MATCHES))
    parser.add_argument("--gap-audit", default=str(DEFAULT_GAP_AUDIT))
    parser.add_argument("--mapping-summary", default=str(DEFAULT_MAPPING_SUMMARY))
    parser.add_argument("--fooddb-current", default=str(DEFAULT_FOODDB_CURRENT))
    parser.add_argument("--ciqual-source", default=str(DEFAULT_CIQUAL_SOURCE))
    parser.add_argument("--usda-source", default=str(DEFAULT_USDA_SOURCE))
    parser.add_argument("--out-candidates", default=str(DEFAULT_CANDIDATES_OUT))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY_OUT))
    parser.add_argument("--out-review", default=str(DEFAULT_REVIEW_OUT))
    parser.add_argument("--out-impact", default=str(DEFAULT_IMPACT_OUT))
    return parser.parse_args()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    return str(value or "").strip()


def normalize_text(value: object) -> str:
    text = clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = text.replace("_", " ")
    text = text.replace("-", " ")
    text = text.replace("/", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def float_or_none(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed


def positive_float(value: object) -> float:
    parsed = float_or_none(value)
    if parsed is None or parsed <= 0:
        return 0.0
    return parsed


def format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".rstrip("0").rstrip(".")


def build_by_id(rows: list[dict[str, str]], id_column: str) -> dict[str, dict[str, str]]:
    output = {}
    for row in rows:
        key = clean_text(row.get(id_column))
        if key and key not in output:
            output[key] = row
    return output


def load_ciqual_rows(path: Path) -> tuple[list[dict[str, str]], str]:
    if not path.exists():
        return [], "ciqual_source_missing"
    try:
        import pandas as pd

        frame = pd.read_excel(path)
    except Exception as exc:
        return [], f"ciqual_source_unavailable:{type(exc).__name__}"
    return frame.fillna("").astype(str).to_dict("records"), "ciqual_source_loaded"


def macro_from_current(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        clean_text(row.get("energy_kcal_100g")),
        clean_text(row.get("protein_g_100g")),
        clean_text(row.get("carbs_g_100g")),
        clean_text(row.get("fat_g_100g")),
    )


def macro_from_usda(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        clean_text(row.get("calories")),
        clean_text(row.get("protein_g")),
        clean_text(row.get("carbs_g")),
        clean_text(row.get("fat_g")),
    )


def source_energy_review_note(energy: str, protein: str, carbs: str, fat: str) -> str:
    energy_value = float_or_none(energy)
    protein_value = float_or_none(protein) or 0.0
    carbs_value = float_or_none(carbs) or 0.0
    fat_value = float_or_none(fat) or 0.0
    macro_energy = 4 * protein_value + 4 * carbs_value + 9 * fat_value
    if energy_value is None:
        return "source_energy_missing"
    if macro_energy > 0 and energy_value > macro_energy * 1.7:
        return "source_energy_suspicious_possible_kj_or_non_kcal_unit"
    return ""


def stats_for_name(name: str, rows: list[dict[str, str]]) -> dict[str, object]:
    matching_rows = [
        row for row in rows
        if normalize_text(row.get("ingredient_name_normalized")) == name
        and clean_text(row.get("mapping_status")) != "accepted_auto"
    ]
    grams_values = [positive_float(row.get("quantity_grams_estimated")) for row in matching_rows]
    grams_values = [value for value in grams_values if value > 0]
    recipe_ids = [clean_text(row.get("recipe_id_candidate")) for row in matching_rows]
    return {
        "frequency": len(matching_rows),
        "rows_with_grams": len(grams_values),
        "total_grams": sum(grams_values),
        "recipe_count": len(set(recipe_id for recipe_id in recipe_ids if recipe_id)),
        "example_raw_texts": clipped_examples([row.get("ingredient_raw_text", "") for row in matching_rows]),
        "example_recipes": clipped_examples([row.get("display_name", "") for row in matching_rows]),
        "example_recipe_ids": clipped_examples(recipe_ids, limit=8),
    }


def clipped_examples(values: list[str], limit: int = 5) -> str:
    output = []
    seen = set()
    for value in values:
        cleaned = clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        output.append(cleaned)
        seen.add(cleaned)
        if len(output) >= limit:
            break
    return " | ".join(output)


def infer_role(name: str, gap_row: dict[str, str] | None) -> str:
    if name in PLANS:
        return PLANS[name].role
    if gap_row:
        role = clean_text(gap_row.get("suggested_role"))
        if role:
            return role
    if any(term in name for term in ["chicken", "turkey", "beef", "pork", "shrimp", "fish", "egg"]):
        return "protein"
    if any(term in name for term in ["rice", "potato", "pasta", "flour", "cornstarch", "beans"]):
        return "carb"
    if any(term in name for term in ["milk", "cheese", "yogurt"]):
        return "dairy"
    if any(term in name for term in ["oil", "butter"]):
        return "fat"
    if any(term in name for term in ["onion", "celery", "mushroom", "carrot", "pepper", "zucchini", "tomato"]):
        return "veg"
    if any(term in name for term in ["salt", "pepper", "oregano", "basil", "thyme"]):
        return "seasoning"
    return "other"


def food_group_for_role(role: str) -> str:
    return {
        "protein": "meat_fish_egg_or_legume",
        "carb": "cereal_starch_or_legume",
        "veg": "vegetable",
        "fruit": "fruit",
        "dairy": "dairy",
        "fat": "fat_oil",
        "seasoning": "seasoning",
        "sauce": "sauce",
    }.get(role, "other")


def candidate_names(
    all_rows: list[dict[str, str]],
    gap_rows: list[dict[str, str]],
) -> list[str]:
    names = set()
    blocking_names = set()
    for row in all_rows:
        if clean_text(row.get("mapping_status")) == "accepted_auto":
            continue
        name = normalize_text(row.get("ingredient_name_normalized"))
        if not name:
            continue
        blocking_names.add(name)
        if positive_float(row.get("quantity_grams_estimated")) > 0:
            names.add(name)
    for row in gap_rows:
        name = normalize_text(row.get("ingredient_name_normalized"))
        if name and name in blocking_names:
            names.add(name)
    names.update(HIGH_PRIORITY)
    names.update(MEDIUM_PRIORITY)
    names.update(LOW_PRIORITY)
    return sorted(names)


def build_candidate_row(
    name: str,
    all_rows: list[dict[str, str]],
    current_by_id: dict[str, dict[str, str]],
    usda_by_id: dict[str, dict[str, str]],
    gap_by_name: dict[str, dict[str, str]],
    ciqual_status: str,
) -> dict[str, object]:
    plan = PLANS.get(name)
    gap_row = gap_by_name.get(name)
    role = infer_role(name, gap_row)
    stats = stats_for_name(name, all_rows)

    suggested_food_id = ""
    suggested_canonical_name = name.replace(" ", "_")
    suggested_display_name = name.title()
    source_used = "not_found"
    source_row_reference = ""
    energy = ""
    protein = ""
    carbs = ""
    fat = ""
    proposed_action = "keep_unmapped"
    safety = "needs_review"
    review_notes = "no_controlled_candidate_plan"

    if plan:
        suggested_food_id = plan.suggested_food_id
        suggested_canonical_name = plan.suggested_canonical_name or suggested_canonical_name
        suggested_display_name = plan.suggested_display_name or suggested_display_name
        proposed_action = plan.proposed_action
        safety = plan.safety
        review_notes = plan.review_notes
        if plan.current_food_id:
            current_row = current_by_id.get(plan.current_food_id)
            if current_row:
                source_used = "current_fooddb_existing"
                source_row_reference = f"food_id={plan.current_food_id}"
                suggested_food_id = plan.current_food_id
                suggested_canonical_name = clean_text(current_row.get("canonical_name"))
                suggested_display_name = clean_text(current_row.get("display_name"))
                energy, protein, carbs, fat = macro_from_current(current_row)
            else:
                proposed_action = "review_needed"
                safety = "unsafe"
                review_notes = f"{review_notes}; current_food_id_not_found:{plan.current_food_id}"
        elif plan.usda_fdc_id:
            usda_row = usda_by_id.get(plan.usda_fdc_id)
            if usda_row:
                source_used = "other_local_source"
                source_row_reference = f"comprehensive_foods_usda.csv:fdc_id={plan.usda_fdc_id}"
                energy, protein, carbs, fat = macro_from_usda(usda_row)
                energy_note = source_energy_review_note(energy, protein, carbs, fat)
                if energy_note:
                    safety = "needs_review"
                    review_notes = f"{review_notes}; {energy_note}"
            else:
                proposed_action = "needs_external_source"
                safety = "unsafe"
                review_notes = f"{review_notes}; usda_fdc_id_not_found:{plan.usda_fdc_id}"
        elif proposed_action == "keep_unmapped":
            source_used = "not_found"
    elif gap_row:
        proposed_action = clean_text(gap_row.get("proposed_action")) or "review_needed"
        if proposed_action in {"add_to_fooddb", "add_unit_rule"}:
            proposed_action = "needs_external_source"
        safety = "needs_review"
        review_notes = "gap_audit_candidate_but_no_controlled_local_source_selected"

    if source_used == "not_found" and ciqual_status != "ciqual_source_loaded":
        review_notes = f"{review_notes}; {ciqual_status}"

    if name in LOW_PRIORITY and proposed_action not in {"alias_to_existing", "keep_unmapped"}:
        proposed_action = "keep_unmapped"
        review_notes = f"{review_notes}; low_macro_priority"

    return {
        "ingredient_name_normalized": name,
        "suggested_food_id": suggested_food_id,
        "suggested_canonical_name": suggested_canonical_name,
        "suggested_display_name": suggested_display_name,
        "suggested_food_group": food_group_for_role(role),
        "suggested_role": role,
        "source_used": source_used,
        "source_row_reference": source_row_reference,
        "energy_kcal_100": energy,
        "protein_g_100": protein,
        "carbs_g_100": carbs,
        "fat_g_100": fat,
        "frequency_in_v1_1": stats["frequency"],
        "rows_with_grams": stats["rows_with_grams"],
        "total_grams_affected": format_float(stats["total_grams"]),
        "example_raw_texts": stats["example_raw_texts"],
        "example_recipes": stats["example_recipes"],
        "proposed_action": proposed_action,
        "safety": safety,
        "review_notes": review_notes,
    }


def why_matters(row: dict[str, object]) -> str:
    role = clean_text(row.get("suggested_role"))
    grams = positive_float(row.get("total_grams_affected"))
    if role == "protein":
        return "protein_macro_coverage_for_main_meals"
    if role == "carb":
        return "carb_and_energy_coverage_for_complete_meals"
    if role == "fat":
        return "fat_and_energy_coverage_for_oils_or_butter"
    if role == "dairy":
        return "protein_fat_and_energy_coverage_for_dairy"
    if grams >= 500:
        return "large_weight_coverage_for_recipe_totals"
    return "secondary_macro_or_weight_coverage"


def build_impact_row(candidate_row: dict[str, object], all_rows: list[dict[str, str]]) -> dict[str, object]:
    name = clean_text(candidate_row["ingredient_name_normalized"])
    stats = stats_for_name(name, all_rows)
    return {
        "ingredient_name_normalized": name,
        "proposed_action": candidate_row["proposed_action"],
        "safety": candidate_row["safety"],
        "affected_ingredient_rows": stats["frequency"],
        "affected_rows_with_grams": stats["rows_with_grams"],
        "total_grams_affected": format_float(stats["total_grams"]),
        "affected_recipe_count": stats["recipe_count"],
        "example_recipe_ids": stats["example_recipe_ids"],
        "example_recipes": stats["example_recipes"],
        "why_it_matters_for_generator": why_matters(candidate_row),
    }


def sort_candidate_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    priority_order = {
        "alias_to_existing": 0,
        "add_to_fooddb_draft": 1,
        "review_needed": 2,
        "needs_external_source": 3,
        "keep_unmapped": 4,
    }
    return sorted(
        rows,
        key=lambda row: (
            priority_order.get(clean_text(row.get("proposed_action")), 9),
            -positive_float(row.get("total_grams_affected")),
            clean_text(row.get("ingredient_name_normalized")),
        ),
    )


def build_summary(
    candidate_rows: list[dict[str, object]],
    ciqual_status: str,
) -> str:
    action_counts = Counter(clean_text(row["proposed_action"]) for row in candidate_rows)
    safety_counts = Counter(clean_text(row["safety"]) for row in candidate_rows)
    missing_macro_rows = [
        row for row in candidate_rows
        if clean_text(row.get("proposed_action")) in {"alias_to_existing", "add_to_fooddb_draft"}
        and not all(clean_text(row.get(column)) for column in ["energy_kcal_100", "protein_g_100", "carbs_g_100", "fat_g_100"])
    ]
    highest_additions = [
        row for row in candidate_rows
        if clean_text(row.get("proposed_action")) == "add_to_fooddb_draft"
    ]
    highest_aliases = [
        row for row in candidate_rows
        if clean_text(row.get("proposed_action")) == "alias_to_existing"
    ]
    next_patch = "A. apply safe alias_to_existing promotions"
    safe_alias_count = sum(
        1 for row in highest_aliases
        if clean_text(row.get("safety")) == "safe_auto"
    )
    if safe_alias_count == 0 and highest_additions:
        next_patch = "B. create Food_DB v1.1 draft file with safe additions"
    elif not highest_aliases and not highest_additions:
        next_patch = "D. mapping review"

    lines = [
        "Food_DB v1.1 gap additions preparation summary",
        "",
        f"Total candidate gaps reviewed: {len(candidate_rows)}",
        f"CIQUAL source status: {ciqual_status}",
        "",
        "Proposed action counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in action_counts.most_common())
    lines.extend(["", "Safety counts:"])
    lines.extend(f"- {name}: {count}" for name, count in safety_counts.most_common())
    lines.extend(
        [
            "",
            f"Rows with missing source macros among proposed alias/add actions: {len(missing_macro_rows)}",
            "",
            "Top 20 highest-impact additions by total_grams_affected:",
        ]
    )
    lines.extend(
        f"- {row['ingredient_name_normalized']}: grams={row['total_grams_affected']}, "
        f"source={row['source_used']}, safety={row['safety']}, notes={row['review_notes']}"
        for row in sorted(highest_additions, key=lambda row: -positive_float(row.get("total_grams_affected")))[:20]
    )
    lines.append("")
    lines.append("Top 20 highest-impact alias_to_existing opportunities:")
    lines.extend(
        f"- {row['ingredient_name_normalized']} -> {row['suggested_food_id']}: "
        f"grams={row['total_grams_affected']}, safety={row['safety']}, notes={row['review_notes']}"
        for row in sorted(highest_aliases, key=lambda row: -positive_float(row.get("total_grams_affected")))[:20]
    )
    lines.extend(["", f"Recommended next patch: {next_patch}"])
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    unmapped_rows = read_csv_rows(Path(args.unmapped))
    matches_rows = read_csv_rows(Path(args.matches))
    gap_rows = read_csv_rows(Path(args.gap_audit))
    read_csv_rows(Path(args.mapping_summary))
    current_rows = read_csv_rows(Path(args.fooddb_current))
    usda_rows = read_csv_rows(Path(args.usda_source))
    _, ciqual_status = load_ciqual_rows(Path(args.ciqual_source))

    all_rows = matches_rows
    current_by_id = build_by_id(current_rows, "food_id")
    usda_by_id = build_by_id(usda_rows, "fdc_id")
    gap_by_name = {
        normalize_text(row.get("ingredient_name_normalized")): row
        for row in gap_rows
    }

    names = candidate_names(all_rows or unmapped_rows, gap_rows)
    candidate_rows = [
        build_candidate_row(name, all_rows or unmapped_rows, current_by_id, usda_by_id, gap_by_name, ciqual_status)
        for name in names
    ]
    candidate_rows = sort_candidate_rows(candidate_rows)
    impact_rows = [
        build_impact_row(row, all_rows or unmapped_rows)
        for row in candidate_rows
    ]
    review_rows = [
        row for row in candidate_rows
        if clean_text(row.get("safety")) != "safe_auto"
        or clean_text(row.get("proposed_action")) not in {"alias_to_existing", "add_to_fooddb_draft"}
    ]

    write_csv(Path(args.out_candidates), candidate_rows, CANDIDATE_COLUMNS)
    write_csv(Path(args.out_review), review_rows, CANDIDATE_COLUMNS)
    write_csv(Path(args.out_impact), impact_rows, IMPACT_COLUMNS)
    summary = build_summary(candidate_rows, ciqual_status)
    summary_path = Path(args.out_summary)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary, encoding="utf-8")

    action_counts = Counter(clean_text(row["proposed_action"]) for row in candidate_rows)
    print(f"Total candidate gaps reviewed: {len(candidate_rows)}")
    print(f"CIQUAL source status: {ciqual_status}")
    print("Proposed action counts:")
    for action, count in action_counts.most_common():
        print(f" - {action}: {count}")
    print(f"Written: {args.out_candidates}")
    print(f"Written: {args.out_summary}")
    print(f"Written: {args.out_review}")
    print(f"Written: {args.out_impact}")


if __name__ == "__main__":
    main()
