from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


DEFAULT_RECIPES = Path("data/recipesdb/draft/recipes_v1_1_curated_200.csv")
DEFAULT_INGREDIENTS = Path("data/recipesdb/draft/recipes_v1_1_ingredients_parsed.csv")
DEFAULT_FOODDB = Path("data/fooddb/current/fooddb_v1_core_master_draft.csv")
DEFAULT_MATCHES_OUT = Path("data/recipesdb/draft/recipes_v1_1_ingredient_food_matches_draft.csv")
DEFAULT_UNMAPPED_OUT = Path("data/recipesdb/draft/recipes_v1_1_ingredient_food_unmapped.csv")
DEFAULT_SUMMARY_OUT = Path("data/recipesdb/audit/recipes_v1_1_mapping_pass_summary.txt")
DEFAULT_REVIEW_OUT = Path("data/recipesdb/audit/recipes_v1_1_mapping_pass_review.csv")
DEFAULT_ALIAS_USAGE_OUT = Path("data/recipesdb/audit/recipes_v1_1_safe_alias_usage.csv")
DEFAULT_FOODDB_V1_1_MATCHES_OUT = Path(
    "data/recipesdb/draft/recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1.csv"
)
DEFAULT_FOODDB_V1_1_UNIT_RULES_MATCHES_OUT = Path(
    "data/recipesdb/draft/recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_unit_rules.csv"
)
DEFAULT_FOODDB_V1_1_UNIT_RULES_REVIEW_PROMOTIONS_MATCHES_OUT = Path(
    "data/recipesdb/draft/recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_unit_rules_review_promotions.csv"
)
DEFAULT_FOODDB_V1_1_ROUND2_MATCHES_OUT = Path(
    "data/recipesdb/draft/recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round2_unit_rules_review_promotions.csv"
)
ROUND2_FOODDB_APPLIED_AUDIT = Path("data/fooddb/audit/fooddb_v1_1_round2_additions_applied.csv")
ROUND2_FOODDB_DEFERRED_AUDIT = Path("data/fooddb/audit/fooddb_v1_1_round2_additions_deferred.csv")
ROUND3_FOODDB_APPLIED_AUDIT = Path("data/fooddb/audit/fooddb_v1_1_round3_additions_applied.csv")
ROUND3_FOODDB_DEFERRED_AUDIT = Path("data/fooddb/audit/fooddb_v1_1_round3_additions_deferred.csv")

OUTPUT_COLUMNS = [
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "ingredient_position",
    "ingredient_raw_text",
    "ingredient_name_parsed",
    "ingredient_name_normalized",
    "quantity_value",
    "quantity_unit",
    "quantity_grams_estimated",
    "parse_status",
    "fooddb_version_used",
    "mapped_food_id",
    "mapped_food_canonical_name",
    "mapping_status",
    "mapping_confidence",
    "mapping_method",
    "mapping_notes",
]

SAFE_ALIAS_BY_NORMALIZED_NAME = {
    "garlic": "food_garlic_fresh",
    "salt": "food_salt_white_sea_igneous_or_rock_no_enrichment",
    "olive oil": "food_olive_oil_extra_virgin",
    "soy sauce": "food_soy_sauce_prepacked",
    "egg": "food_egg_raw",
    "eggs": "food_egg_raw",
    "white sugar": "food_sugar_white",
    "black pepper": "food_black_pepper_powder",
    "chicken breast": "food_chicken_breast_without_skin_raw",
    "chicken breasts": "food_chicken_breast_without_skin_raw",
    "beef chuck": "food_beef_chuck_raw",
    "shrimp": "food_shrimp_or_prawn_raw",
    "brown sugar": "food_sugar_brown",
    "vegetable oil": "food_combined_oil_blended_vegetable_oils",
    "celery": "food_celery_stalk_raw",
}

FOODDB_V1_1_DRAFT_ALIAS_BY_NORMALIZED_NAME = {
    "green beans": "food_green_beans_cooked_unsalted",
    "parmesan cheese": "food_parmesan_cheese_hard",
    "cornstarch": "food_cornstarch",
}

NEW_SAFE_ALIAS_KEYS = {
    "chicken breast",
    "chicken breasts",
    "beef chuck",
    "shrimp",
    "brown sugar",
    "vegetable oil",
    "celery",
}

FOODDB_V1_1_DRAFT_ITEM_IDS = set(FOODDB_V1_1_DRAFT_ALIAS_BY_NORMALIZED_NAME.values())

REVIEW_PROMOTION_FOOD_IDS = {
    "onion": "food_onion_raw",
    "yellow onion": "food_yellow_onion_raw",
    "red onion": "food_red_onion_raw",
    "generic milk": "food_milk_fat_content_unknown_uht_sterilized",
    "whole milk": "food_milk_whole_pasteurised",
    "semi-skimmed milk": "food_milk_semi_skimmed_pasteurised",
    "skim milk": "food_milk_skimmed_pasteurised",
    "skimmed milk": "food_milk_skimmed_pasteurised",
    "cooked rice": "food_rice_cooked_unsalted",
    "raw rice": "food_rice_raw",
    "water": "food_water_municipal",
}

ROUND2_PROMOTION_FOOD_IDS = {
    "butter": "food_butter_82_fat_unsalted",
    "unsalted butter": "food_butter_82_fat_unsalted",
    "carrot": "food_carrot_raw",
    "carrots": "food_carrot_raw",
    "green onion": "food_chive_or_spring_onion_fresh",
    "green onions": "food_chive_or_spring_onion_fresh",
    "scallion": "food_chive_or_spring_onion_fresh",
    "scallions": "food_chive_or_spring_onion_fresh",
    "chicken broth": "food_chicken_broth_ready_to_serve",
}

ROUND2_HERB_PROMOTION_FOOD_IDS = {
    "oregano": "food_oregano_dried",
    "basil": "food_basil_dried",
    "thyme": "food_thyme_dried",
    "parsley": "food_parsley_dried",
}

ROUND2_SPECIFIC_BEEF_PROMOTION_FOOD_IDS = {
    "beef chuck": "food_beef_chuck_raw",
    "flank steak": "food_beef_flank_steak_raw",
    "beef flank steak": "food_beef_flank_steak_raw",
    "beef short ribs": "food_beef_short_ribs_raw",
    "short ribs": "food_beef_short_ribs_raw",
    "beef tenderloin": "food_beef_tenderloin_raw",
}

ROUND3_TARGET_FOOD_IDS = {
    "ground_beef": "food_beef_minced_steak_15_fat_raw",
    "lean_ground_beef": "food_beef_minced_steak_10_fat_raw",
    "cubed_beef_stew_meat": "food_beef_stewing_meat_raw",
    "potato_raw": "food_potato_peeled_raw",
    "potato_cooked": "food_potato_boiled_cooked_in_water",
    "white_rice_cooked": "food_rice_cooked_unsalted",
    "white_rice_raw": "food_rice_raw",
    "jasmine_rice_cooked": "food_rice_thai_cooked",
    "mozzarella": "food_mozzarella_cheese_from_cow_s_milk",
    "pork_shoulder": "food_pork_shoulder_raw",
    "pork_loin": "food_pork_loin_raw",
    "onions": "food_onion_raw",
    "garlic_cloves": "food_garlic_fresh",
    "kosher_salt": "food_salt_white_sea_igneous_or_rock_no_enrichment",
}

BASELINE_CURRENT_FOODDB_MAPPING = {
    "accepted_auto": 583,
    "review_needed": 508,
    "unmapped": 770,
    "accepted_auto_with_grams": 427,
    "review_needed_with_grams": 177,
    "unmapped_with_grams": 237,
}

OUT_OF_SCOPE_PATTERNS = [
    "cooking spray",
    "nonstick cooking spray",
    "aluminum foil",
    "foil",
    "parchment paper",
    "water to cover",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Construieste mapping draft read-only pentru ingredientele Recipes_DB v1.1."
    )
    parser.add_argument("--recipes", default=str(DEFAULT_RECIPES))
    parser.add_argument("--ingredients", "--ingredients_path", dest="ingredients", default=str(DEFAULT_INGREDIENTS))
    parser.add_argument("--fooddb", "--fooddb_path", dest="fooddb", default=str(DEFAULT_FOODDB))
    parser.add_argument("--output-suffix", "--output_suffix", dest="output_suffix", default="")
    parser.add_argument("--fooddb-version-used", "--fooddb_version_used", dest="fooddb_version_used", default="")
    parser.add_argument("--out-matches", default="")
    parser.add_argument("--out-unmapped", default="")
    parser.add_argument("--out-summary", default="")
    parser.add_argument("--out-review", default="")
    parser.add_argument("--out-alias-usage", default="")
    return parser.parse_args()


def add_output_suffix(path: Path, output_suffix: str) -> Path:
    suffix = clean_text(output_suffix)
    if not suffix:
        return path
    return path.with_name(f"{path.stem}_{suffix}{path.suffix}")


def resolve_output_path(explicit_path: str, default_path: Path, output_suffix: str) -> Path:
    if clean_text(explicit_path):
        return Path(explicit_path)
    return add_output_suffix(default_path, output_suffix)


def infer_fooddb_version_used(fooddb_path: Path, output_suffix: str) -> str:
    combined = normalize_match_text(f"{fooddb_path.as_posix()} {output_suffix}")
    if "round3" in combined:
        return "fooddb_v1_1_draft_round3"
    if "round2" in combined:
        return "fooddb_v1_1_draft_round2"
    if "fooddb v1 1" in combined or "v1 1" in combined:
        return "fooddb_v1_1_draft"
    return "fooddb_current"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
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


def exact_key(value: object) -> str:
    return clean_text(value).casefold()


def normalize_match_text(value: object) -> str:
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


def dedupe_food_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    output = []
    seen_ids = set()
    for row in rows:
        food_id = clean_text(row.get("food_id"))
        if not food_id or food_id in seen_ids:
            continue
        seen_ids.add(food_id)
        output.append(row)
    return output


def build_index(
    food_rows: list[dict[str, str]],
    column_name: str,
    key_fn,
) -> dict[str, list[dict[str, str]]]:
    index: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in food_rows:
        value = clean_text(row.get(column_name))
        if not value:
            continue
        key = key_fn(value)
        if key:
            index[key].append(row)
    return dict(index)


def build_food_indexes(food_rows: list[dict[str, str]]) -> dict[str, dict[str, list[dict[str, str]]]]:
    return {
        "by_food_id": build_index(food_rows, "food_id", clean_text),
        "canonical_exact": build_index(food_rows, "canonical_name", exact_key),
        "display_exact": build_index(food_rows, "display_name", exact_key),
        "canonical_normalized": build_index(food_rows, "canonical_name", normalize_match_text),
        "display_normalized": build_index(food_rows, "display_name", normalize_match_text),
        "family_normalized": build_index(food_rows, "food_family_name", normalize_match_text),
    }


def first_unique(rows: list[dict[str, str]]) -> dict[str, str] | None:
    unique_rows = dedupe_food_rows(rows)
    if len(unique_rows) == 1:
        return unique_rows[0]
    return None


def get_by_food_id(
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
    food_id: str,
) -> dict[str, str] | None:
    return first_unique(food_indexes["by_food_id"].get(food_id, []))


def is_parse_review(row: dict[str, str]) -> bool:
    return clean_text(row.get("parse_status")) == "review_needed"


def has_grams(row: dict[str, object]) -> bool:
    return clean_text(row.get("quantity_grams_estimated")) != ""


def is_out_of_scope(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_normalized', '')}"
    )
    return any(pattern in text for pattern in OUT_OF_SCOPE_PATTERNS)


def has_forbidden_onion_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    blocked_terms = [
        "onion powder",
        "fried onion",
        "fried onions",
        "onion soup",
        "onion flakes",
        "dried onion",
        "dehydrated onion",
    ]
    return any(term in text for term in blocked_terms)


def has_forbidden_rice_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_normalized', '')}"
    )
    blocked_terms = [
        "rice vinegar",
        "rice wine",
        "rice flour",
        "rice noodle",
        "rice noodles",
        "rice paper",
        "rice wrapper",
        "rice wrappers",
        "rice vermicelli",
    ]
    return any(term in text for term in blocked_terms)


def has_forbidden_carrot_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    blocked_terms = ["carrot cake", "carrot juice", "pickled carrot", "pickled carrots"]
    return any(term in text for term in blocked_terms)


def has_forbidden_lemon_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    blocked_terms = ["lemon juice", "juiced", "zested", "lemon zest"]
    return any(term in text for term in blocked_terms)


def has_forbidden_butter_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    blocked_terms = ["peanut butter", "almond butter", "margarine", "shortening", "cocoa butter"]
    return any(term in text for term in blocked_terms)


def has_forbidden_broth_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    blocked_terms = ["beef broth", "vegetable broth", "water"]
    return any(term in text for term in blocked_terms)


def has_dried_herb_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    return "dried" in text or "ground dried" in text


def has_forbidden_specific_beef_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    return "ground beef" in text or "lean ground beef" in text or text.strip() == "beef"


def has_forbidden_potato_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    blocked_terms = [
        "sweet potato",
        "fries",
        "french fries",
        "fried potatoes",
        "potato chips",
        "potato crisps",
    ]
    return any(term in text for term in blocked_terms)


def potato_preparation_key(row: dict[str, str]) -> str:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_normalized', '')}"
    )
    if has_forbidden_potato_signal(row):
        return ""
    if "cooked" in text or "boiled" in text:
        return "potato_cooked"
    raw_signals = ["peeled", "pound potatoes", "potatoes cut", "potatoes diced", "potatoes sliced"]
    if any(signal in text for signal in raw_signals):
        return "potato_raw"
    return ""


def has_forbidden_cheese_signal(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    blocked_terms = ["parmesan", "feta", "cheddar", "blue cheese", "ricotta"]
    return any(term in text for term in blocked_terms)


def round3_rice_key(row: dict[str, str]) -> str:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_normalized', '')}"
    )
    if has_forbidden_rice_signal(row):
        return ""
    if "cooked jasmine rice" in text:
        return "jasmine_rice_cooked"
    if "cooked white rice" in text or "cooked rice" in text:
        return "white_rice_cooked"
    if "uncooked white rice" in text or "raw white rice" in text or "dry white rice" in text:
        return "white_rice_raw"
    return ""


def round3_beef_key(row: dict[str, str]) -> str:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_parsed', '')}"
    )
    if "cubed beef stew meat" in text or "beef stew meat" in text:
        return "cubed_beef_stew_meat"
    if "lean ground beef" in text:
        return "lean_ground_beef"
    if "ground beef" in text:
        return "ground_beef"
    return ""


def rice_preparation_key(row: dict[str, str]) -> str:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_normalized', '')}"
    )
    if has_forbidden_rice_signal(row):
        return ""
    if "cooked" in text:
        return "cooked rice"
    raw_signals = ["uncooked", "raw", "dry", "dried"]
    if any(signal in text for signal in raw_signals):
        return "raw rice"
    return ""


def attempt_review_promotion(
    ingredient_row: dict[str, str],
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
) -> dict[str, str] | None:
    ingredient_name = normalize_match_text(ingredient_row.get("ingredient_name_normalized"))
    parsed_name = normalize_match_text(ingredient_row.get("ingredient_name_parsed"))

    promotion_food_id = ""
    promotion_note = ""

    if ingredient_name in {"onion", "yellow onion", "red onion"} and not has_forbidden_onion_signal(ingredient_row):
        promotion_food_id = REVIEW_PROMOTION_FOOD_IDS.get(ingredient_name, "")
        promotion_note = f"review_promotion_v1_1:onion_family_exact:{ingredient_name}"
    elif ingredient_name == "milk":
        promotion_food_id = REVIEW_PROMOTION_FOOD_IDS["generic milk"]
        promotion_note = "review_promotion_v1_1:generic_milk_to_fat_unknown_milk"
    elif ingredient_name in {"whole milk", "semi-skimmed milk", "skim milk", "skimmed milk"}:
        promotion_food_id = REVIEW_PROMOTION_FOOD_IDS.get(ingredient_name, "")
        promotion_note = f"review_promotion_v1_1:exact_milk_variant:{ingredient_name}"
    elif ingredient_name == "rice":
        rice_key = rice_preparation_key(ingredient_row)
        promotion_food_id = REVIEW_PROMOTION_FOOD_IDS.get(rice_key, "")
        if rice_key:
            promotion_note = f"review_promotion_v1_1:rice_preparation_clear:{rice_key}"
    elif ingredient_name == "water":
        promotion_food_id = REVIEW_PROMOTION_FOOD_IDS["water"]
        promotion_note = "review_promotion_v1_1:water_low_macro_impact"

    if not promotion_food_id:
        return None

    food_row = get_by_food_id(food_indexes, promotion_food_id)
    if not food_row:
        return None

    result = match_result(
        food_row=food_row,
        status="accepted_auto",
        confidence="medium",
        method="review_promotion_v1_1",
        notes=[promotion_note],
    )
    if parsed_name and parsed_name != ingredient_name:
        notes = [note.strip() for note in clean_text(result.get("mapping_notes")).split(";") if note.strip()]
        notes.append(f"parsed_name={parsed_name}")
        result["mapping_notes"] = "; ".join(dict.fromkeys(notes))
    return result


def attempt_round2_promotion(
    ingredient_row: dict[str, str],
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
) -> dict[str, str] | None:
    ingredient_name = normalize_match_text(ingredient_row.get("ingredient_name_normalized"))
    parsed_name = normalize_match_text(ingredient_row.get("ingredient_name_parsed"))

    promotion_food_id = ""
    promotion_note = ""

    if ingredient_name in {"carrot", "carrots"} and not has_forbidden_carrot_signal(ingredient_row):
        promotion_food_id = ROUND2_PROMOTION_FOOD_IDS.get(ingredient_name, "")
        promotion_note = f"round2_promotion_v1_1:carrot_family_exact:{ingredient_name}"
    elif ingredient_name == "lemon" and not has_forbidden_lemon_signal(ingredient_row):
        promotion_food_id = "food_lemon_pulp_raw"
        promotion_note = "round2_promotion_v1_1:whole_or_wedge_lemon_to_lemon_pulp_raw"
    elif ingredient_name in {"green onion", "green onions", "scallion", "scallions"}:
        promotion_food_id = ROUND2_PROMOTION_FOOD_IDS.get(ingredient_name, "")
        promotion_note = f"round2_promotion_v1_1:green_onion_scallion_exact:{ingredient_name}"
    elif ingredient_name in {"butter", "unsalted butter"} and not has_forbidden_butter_signal(ingredient_row):
        promotion_food_id = ROUND2_PROMOTION_FOOD_IDS.get(ingredient_name, "")
        promotion_note = "round2_promotion_v1_1:butter_to_unsalted_butter_for_macro_pilot"
    elif ingredient_name == "chicken broth" and not has_forbidden_broth_signal(ingredient_row):
        promotion_food_id = ROUND2_PROMOTION_FOOD_IDS["chicken broth"]
        promotion_note = "round2_promotion_v1_1:chicken_broth_exact"
    elif ingredient_name in ROUND2_HERB_PROMOTION_FOOD_IDS and has_dried_herb_signal(ingredient_row):
        promotion_food_id = ROUND2_HERB_PROMOTION_FOOD_IDS[ingredient_name]
        promotion_note = f"round2_promotion_v1_1:dried_herb_exact:{ingredient_name}"
    elif ingredient_name in ROUND2_SPECIFIC_BEEF_PROMOTION_FOOD_IDS and not has_forbidden_specific_beef_signal(ingredient_row):
        promotion_food_id = ROUND2_SPECIFIC_BEEF_PROMOTION_FOOD_IDS[ingredient_name]
        promotion_note = f"round2_promotion_v1_1:specific_beef_cut_exact:{ingredient_name}"
    elif parsed_name in ROUND2_SPECIFIC_BEEF_PROMOTION_FOOD_IDS and not has_forbidden_specific_beef_signal(ingredient_row):
        promotion_food_id = ROUND2_SPECIFIC_BEEF_PROMOTION_FOOD_IDS[parsed_name]
        promotion_note = f"round2_promotion_v1_1:specific_beef_cut_from_parsed_name:{parsed_name}"

    if not promotion_food_id:
        return None

    food_row = get_by_food_id(food_indexes, promotion_food_id)
    if not food_row:
        return None

    result = match_result(
        food_row=food_row,
        status="accepted_auto",
        confidence="medium",
        method="round2_promotion_v1_1",
        notes=[promotion_note],
    )
    if parsed_name and parsed_name != ingredient_name:
        notes = [note.strip() for note in clean_text(result.get("mapping_notes")).split(";") if note.strip()]
        notes.append(f"parsed_name={parsed_name}")
        result["mapping_notes"] = "; ".join(dict.fromkeys(notes))
    return result


def attempt_round3_promotion(
    ingredient_row: dict[str, str],
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
) -> dict[str, str] | None:
    ingredient_name = normalize_match_text(ingredient_row.get("ingredient_name_normalized"))
    parsed_name = normalize_match_text(ingredient_row.get("ingredient_name_parsed"))

    promotion_food_id = ""
    promotion_note = ""

    if ingredient_name == "beef":
        beef_key = round3_beef_key(ingredient_row)
        promotion_food_id = ROUND3_TARGET_FOOD_IDS.get(beef_key, "")
        if beef_key:
            promotion_note = f"round3_targeted_blocker:beef_row_level:{beef_key}"
    elif ingredient_name == "cubed beef stew meat":
        promotion_food_id = ROUND3_TARGET_FOOD_IDS["cubed_beef_stew_meat"]
        promotion_note = "round3_targeted_blocker:cubed_beef_stew_meat_exact"
    elif ingredient_name in {"potatoes", "white potatoes"}:
        potato_key = potato_preparation_key(ingredient_row)
        promotion_food_id = ROUND3_TARGET_FOOD_IDS.get(potato_key, "")
        if potato_key:
            promotion_note = f"round3_targeted_blocker:potato_state_clear:{potato_key}"
    elif ingredient_name in {"white rice", "uncooked white rice", "jasmine rice", "rice"}:
        rice_key = round3_rice_key(ingredient_row)
        promotion_food_id = ROUND3_TARGET_FOOD_IDS.get(rice_key, "")
        if rice_key:
            promotion_note = f"round3_targeted_blocker:rice_state_clear:{rice_key}"
    elif ingredient_name == "mozzarella cheese" and not has_forbidden_cheese_signal(ingredient_row):
        promotion_food_id = ROUND3_TARGET_FOOD_IDS["mozzarella"]
        promotion_note = "round3_targeted_blocker:mozzarella_cheese_exact"
    elif ingredient_name == "pork shoulder":
        promotion_food_id = ROUND3_TARGET_FOOD_IDS["pork_shoulder"]
        promotion_note = "round3_targeted_blocker:pork_shoulder_exact"
    elif ingredient_name == "pork loin":
        promotion_food_id = ROUND3_TARGET_FOOD_IDS["pork_loin"]
        promotion_note = "round3_targeted_blocker:pork_loin_exact"
    elif ingredient_name == "onions" and not has_forbidden_onion_signal(ingredient_row):
        promotion_food_id = ROUND3_TARGET_FOOD_IDS["onions"]
        promotion_note = "round3_targeted_blocker:onions_plural_to_onion_raw"
    elif ingredient_name == "garlic cloves":
        promotion_food_id = ROUND3_TARGET_FOOD_IDS["garlic_cloves"]
        promotion_note = "round3_targeted_blocker:garlic_cloves_to_garlic_fresh"
    elif ingredient_name == "kosher salt":
        promotion_food_id = ROUND3_TARGET_FOOD_IDS["kosher_salt"]
        promotion_note = "round3_targeted_blocker:kosher_salt_to_salt_low_macro_impact"

    if not promotion_food_id:
        return None

    food_row = get_by_food_id(food_indexes, promotion_food_id)
    if not food_row:
        return None

    result = match_result(
        food_row=food_row,
        status="accepted_auto",
        confidence="medium",
        method="round3_targeted_blocker",
        notes=[promotion_note],
    )
    if parsed_name and parsed_name != ingredient_name:
        notes = [note.strip() for note in clean_text(result.get("mapping_notes")).split(";") if note.strip()]
        notes.append(f"parsed_name={parsed_name}")
        result["mapping_notes"] = "; ".join(dict.fromkeys(notes))
    return result


def empty_result(status: str, method: str, notes: list[str]) -> dict[str, str]:
    return {
        "mapped_food_id": "",
        "mapped_food_canonical_name": "",
        "mapping_status": status,
        "mapping_confidence": "low",
        "mapping_method": method,
        "mapping_notes": "; ".join(dict.fromkeys(note for note in notes if note)),
    }


def match_result(
    food_row: dict[str, str],
    status: str,
    confidence: str,
    method: str,
    notes: list[str],
) -> dict[str, str]:
    return {
        "mapped_food_id": clean_text(food_row.get("food_id")),
        "mapped_food_canonical_name": clean_text(food_row.get("canonical_name")),
        "mapping_status": status,
        "mapping_confidence": confidence,
        "mapping_method": method,
        "mapping_notes": "; ".join(dict.fromkeys(note for note in notes if note)),
    }


def gate_review_if_needed(row: dict[str, str], result: dict[str, str]) -> dict[str, str]:
    if not is_parse_review(row):
        return result
    gated = dict(result)
    gated["mapping_status"] = "review_needed"
    gated["mapping_confidence"] = "low"
    notes = [note.strip() for note in clean_text(gated.get("mapping_notes")).split(";") if note.strip()]
    notes.append("parse_status_review_gate")
    gated["mapping_notes"] = "; ".join(dict.fromkeys(notes))
    return gated


def attempt_match(
    ingredient_row: dict[str, str],
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
    enable_review_promotions: bool = False,
    enable_round2_promotions: bool = False,
    enable_round3_promotions: bool = False,
) -> dict[str, str]:
    ingredient_name = normalize_match_text(ingredient_row.get("ingredient_name_normalized"))
    if not ingredient_name:
        return empty_result("review_needed", "no_match", ["missing_normalized_ingredient_name"])

    if is_out_of_scope(ingredient_row):
        return empty_result("unmapped", "no_match", ["out_of_scope_non_food_or_non_nutrition_line"])

    if " and " in f" {ingredient_name} " or " or " in f" {ingredient_name} ":
        return empty_result("review_needed", "review_candidate", ["compound_or_alternative_ingredient_name"])

    alias_food_id = SAFE_ALIAS_BY_NORMALIZED_NAME.get(ingredient_name, "")
    v1_1_alias_food_id = FOODDB_V1_1_DRAFT_ALIAS_BY_NORMALIZED_NAME.get(ingredient_name, "")
    if not alias_food_id and v1_1_alias_food_id:
        alias_food_id = v1_1_alias_food_id
    if alias_food_id:
        alias_row = get_by_food_id(food_indexes, alias_food_id)
        if alias_row:
            alias_note_prefix = "safe_alias_v1_1"
            if ingredient_name in FOODDB_V1_1_DRAFT_ALIAS_BY_NORMALIZED_NAME:
                alias_note_prefix = "fooddb_v1_1_draft_addition_alias"
            result = match_result(
                food_row=alias_row,
                status="accepted_auto",
                confidence="high",
                method="safe_alias_v1_1",
                notes=[f"{alias_note_prefix}:{ingredient_name}->{alias_food_id}"],
            )
            return gate_review_if_needed(ingredient_row, result)
        if ingredient_name in FOODDB_V1_1_DRAFT_ALIAS_BY_NORMALIZED_NAME:
            pass
        else:
            return empty_result(
                "review_needed",
                "review_candidate",
                [f"safe_alias_target_missing:{ingredient_name}->{alias_food_id}"],
            )

    if enable_review_promotions:
        promoted_result = attempt_review_promotion(ingredient_row, food_indexes)
        if promoted_result:
            return gate_review_if_needed(ingredient_row, promoted_result)

    if enable_round2_promotions:
        round2_result = attempt_round2_promotion(ingredient_row, food_indexes)
        if round2_result:
            return gate_review_if_needed(ingredient_row, round2_result)

    if enable_round3_promotions:
        round3_result = attempt_round3_promotion(ingredient_row, food_indexes)
        if round3_result:
            return gate_review_if_needed(ingredient_row, round3_result)

    exact_candidate = exact_key(ingredient_name)
    normalized_candidate = normalize_match_text(ingredient_name)
    match_attempts = [
        ("exact_match", "high", food_indexes["canonical_exact"].get(exact_candidate, []), "exact canonical_name match"),
        ("exact_match", "high", food_indexes["display_exact"].get(exact_candidate, []), "exact display_name match"),
        (
            "normalized_match",
            "medium",
            food_indexes["canonical_normalized"].get(normalized_candidate, []),
            "normalized canonical_name match",
        ),
        (
            "normalized_match",
            "medium",
            food_indexes["display_normalized"].get(normalized_candidate, []),
            "normalized display_name match",
        ),
    ]

    for method, confidence, candidate_rows, note in match_attempts:
        unique_rows = dedupe_food_rows(candidate_rows)
        if not unique_rows:
            continue
        if len(unique_rows) == 1:
            result = match_result(
                food_row=unique_rows[0],
                status="accepted_auto",
                confidence=confidence,
                method=method,
                notes=[note],
            )
            return gate_review_if_needed(ingredient_row, result)
        return empty_result(
            "review_needed",
            "review_candidate",
            [f"{note}; ambiguous_candidate_count={len(unique_rows)}"],
        )

    family_rows = dedupe_food_rows(food_indexes["family_normalized"].get(normalized_candidate, []))
    if family_rows:
        preview_ids = ",".join(clean_text(row.get("food_id")) for row in family_rows[:5])
        return empty_result(
            "review_needed",
            "review_candidate",
            [f"family_name_candidate_only; candidate_count={len(family_rows)}; preview={preview_ids}"],
        )

    return empty_result("unmapped", "no_match", ["no_exact_normalized_or_safe_alias_match"])


def output_row(
    ingredient_row: dict[str, str],
    match_data: dict[str, str],
    fooddb_version_used: str,
) -> dict[str, object]:
    row = {column: clean_text(ingredient_row.get(column)) for column in OUTPUT_COLUMNS if column in ingredient_row}
    row.update(match_data)
    row["fooddb_version_used"] = fooddb_version_used
    return {column: row.get(column, "") for column in OUTPUT_COLUMNS}


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


def build_safe_alias_usage(mapped_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in mapped_rows:
        if row["mapping_method"] != "safe_alias_v1_1":
            continue
        grouped[clean_text(row["ingredient_name_normalized"])].append(row)

    output = []
    for alias_key, rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        first = rows[0]
        output.append(
            {
                "fooddb_version_used": clean_text(first.get("fooddb_version_used")),
                "alias_key": alias_key,
                "mapped_food_id": first["mapped_food_id"],
                "mapped_food_canonical_name": first["mapped_food_canonical_name"],
                "usage_count": len(rows),
                "rows_with_grams": sum(1 for row in rows if has_grams(row)),
                "example_raw_texts": clipped_examples([clean_text(row["ingredient_raw_text"]) for row in rows]),
                "example_recipes": clipped_examples([clean_text(row["display_name"]) for row in rows]),
            }
        )
    return output


def top_by_ingredient(rows: list[dict[str, object]], with_grams_only: bool = True, limit: int = 20) -> list[tuple[str, int]]:
    counts = Counter()
    for row in rows:
        if with_grams_only and not has_grams(row):
            continue
        key = clean_text(row["ingredient_name_normalized"])
        if key:
            counts[key] += 1
    return counts.most_common(limit)


def summarize_mapping_counts(mapped_rows: list[dict[str, object]]) -> dict[str, int]:
    status_counts = Counter(clean_text(row["mapping_status"]) for row in mapped_rows)
    accepted_rows = [row for row in mapped_rows if row["mapping_status"] == "accepted_auto"]
    review_rows = [row for row in mapped_rows if row["mapping_status"] == "review_needed"]
    unmapped_rows = [row for row in mapped_rows if row["mapping_status"] == "unmapped"]
    return {
        "accepted_auto": status_counts["accepted_auto"],
        "review_needed": status_counts["review_needed"],
        "unmapped": status_counts["unmapped"],
        "accepted_auto_with_grams": sum(1 for row in accepted_rows if has_grams(row)),
        "review_needed_with_grams": sum(1 for row in review_rows if has_grams(row)),
        "unmapped_with_grams": sum(1 for row in unmapped_rows if has_grams(row)),
    }


def load_baseline_counts(path: Path) -> dict[str, int]:
    if not path.exists():
        return dict(BASELINE_CURRENT_FOODDB_MAPPING)
    try:
        rows = read_csv_rows(path)
    except OSError:
        return dict(BASELINE_CURRENT_FOODDB_MAPPING)
    return summarize_mapping_counts(rows)


def choose_baseline_path(output_suffix: str) -> Path:
    normalized_suffix = normalize_match_text(output_suffix)
    if "round3" in normalized_suffix and DEFAULT_FOODDB_V1_1_ROUND2_MATCHES_OUT.exists():
        return DEFAULT_FOODDB_V1_1_ROUND2_MATCHES_OUT
    if "round2" in normalized_suffix and DEFAULT_FOODDB_V1_1_UNIT_RULES_REVIEW_PROMOTIONS_MATCHES_OUT.exists():
        return DEFAULT_FOODDB_V1_1_UNIT_RULES_REVIEW_PROMOTIONS_MATCHES_OUT
    if "review promotions" in normalized_suffix and DEFAULT_FOODDB_V1_1_UNIT_RULES_MATCHES_OUT.exists():
        return DEFAULT_FOODDB_V1_1_UNIT_RULES_MATCHES_OUT
    if "unit rules" in normalized_suffix and DEFAULT_FOODDB_V1_1_MATCHES_OUT.exists():
        return DEFAULT_FOODDB_V1_1_MATCHES_OUT
    return DEFAULT_MATCHES_OUT


def build_new_fooddb_item_usage(mapped_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output = []
    for alias_key, food_id in sorted(FOODDB_V1_1_DRAFT_ALIAS_BY_NORMALIZED_NAME.items()):
        rows = [row for row in mapped_rows if clean_text(row.get("mapped_food_id")) == food_id]
        first = rows[0] if rows else {}
        output.append(
            {
                "alias_key": alias_key,
                "mapped_food_id": food_id,
                "mapped_food_canonical_name": clean_text(first.get("mapped_food_canonical_name")),
                "usage_count": len(rows),
                "rows_with_grams": sum(1 for row in rows if has_grams(row)),
            }
        )
    return output


def build_promoted_usage(mapped_rows: list[dict[str, object]]) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
    promoted_rows = [
        row for row in mapped_rows
        if clean_text(row.get("mapping_method")) == "review_promotion_v1_1"
        and clean_text(row.get("mapping_status")) == "accepted_auto"
    ]
    promoted_counts = Counter(clean_text(row.get("ingredient_name_normalized")) for row in promoted_rows)
    promoted_with_grams_counts = Counter(
        clean_text(row.get("ingredient_name_normalized")) for row in promoted_rows if has_grams(row)
    )
    return promoted_counts.most_common(), promoted_with_grams_counts.most_common()


def build_method_usage(
    mapped_rows: list[dict[str, object]],
    method: str,
) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
    method_rows = [
        row for row in mapped_rows
        if clean_text(row.get("mapping_method")) == method
        and clean_text(row.get("mapping_status")) == "accepted_auto"
    ]
    counts = Counter(clean_text(row.get("ingredient_name_normalized")) for row in method_rows)
    with_grams_counts = Counter(
        clean_text(row.get("ingredient_name_normalized")) for row in method_rows if has_grams(row)
    )
    return counts.most_common(), with_grams_counts.most_common()


def build_kept_review_reasons(mapped_rows: list[dict[str, object]], limit: int = 20) -> list[tuple[str, int]]:
    review_rows = [
        row for row in mapped_rows
        if clean_text(row.get("mapping_status")) == "review_needed"
    ]
    counts = Counter()
    for row in review_rows:
        ingredient_name = clean_text(row.get("ingredient_name_normalized"))
        notes = clean_text(row.get("mapping_notes"))
        first_note = notes.split(";")[0].strip() if notes else "review_needed"
        counts[f"{ingredient_name} | {first_note}"] += 1
    return counts.most_common(limit)


def load_round2_fooddb_audit(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        return read_csv_rows(path)
    except OSError:
        return []


def build_summary(
    mapped_rows: list[dict[str, object]],
    alias_usage_rows: list[dict[str, object]],
    baseline_counts: dict[str, int],
    baseline_path: Path,
    fooddb_version_used: str,
    fooddb_path: Path,
) -> str:
    status_counts = Counter(clean_text(row["mapping_status"]) for row in mapped_rows)
    accepted_rows = [row for row in mapped_rows if row["mapping_status"] == "accepted_auto"]
    review_rows = [row for row in mapped_rows if row["mapping_status"] == "review_needed"]
    unmapped_rows = [row for row in mapped_rows if row["mapping_status"] == "unmapped"]
    after_counts = summarize_mapping_counts(mapped_rows)

    blocking_rows = [row for row in mapped_rows if row["mapping_status"] != "accepted_auto" or not has_grams(row)]
    blocking_counts = top_by_ingredient(blocking_rows, with_grams_only=False, limit=20)

    expected_next_patch = "B. Food_DB gap additions"
    if after_counts["review_needed_with_grams"] > after_counts["unmapped_with_grams"]:
        expected_next_patch = "C. review mappings"
    no_grams_accepted = sum(1 for row in accepted_rows if not has_grams(row))
    if no_grams_accepted > max(after_counts["unmapped_with_grams"], after_counts["review_needed_with_grams"]):
        expected_next_patch = "A. unit-to-grams rules"

    new_alias_usage = [
        row for row in alias_usage_rows
        if clean_text(row["alias_key"]) in NEW_SAFE_ALIAS_KEYS
    ]
    new_fooddb_item_usage = build_new_fooddb_item_usage(mapped_rows)
    promoted_counts, promoted_with_grams_counts = build_promoted_usage(mapped_rows)
    round2_counts, round2_with_grams_counts = build_method_usage(mapped_rows, "round2_promotion_v1_1")
    round3_counts, round3_with_grams_counts = build_method_usage(mapped_rows, "round3_targeted_blocker")
    kept_review_reasons = build_kept_review_reasons(mapped_rows)
    round2_applied_rows = load_round2_fooddb_audit(ROUND2_FOODDB_APPLIED_AUDIT)
    round2_deferred_rows = load_round2_fooddb_audit(ROUND2_FOODDB_DEFERRED_AUDIT)
    round3_applied_rows = load_round2_fooddb_audit(ROUND3_FOODDB_APPLIED_AUDIT)
    round3_deferred_rows = load_round2_fooddb_audit(ROUND3_FOODDB_DEFERRED_AUDIT)

    lines = [
        "Recipes_DB v1.1 safe-alias mapping pass summary",
        "",
        f"Food_DB version used: {fooddb_version_used}",
        f"Food_DB path: {fooddb_path}",
        f"Baseline mapping path: {baseline_path}",
        "",
        f"Total ingredient rows: {len(mapped_rows)}",
        "",
        "Baseline mapping vs this run:",
    ]
    for key in [
        "accepted_auto",
        "review_needed",
        "unmapped",
        "accepted_auto_with_grams",
        "review_needed_with_grams",
        "unmapped_with_grams",
    ]:
        before = baseline_counts.get(key, 0)
        after = after_counts[key]
        delta = after - before
        lines.append(f"- {key}: {before} -> {after} ({delta:+d})")
    lines.extend(
        [
            "",
            "Previously promoted safe aliases still active:",
        ]
    )
    lines.extend(
        f"- {row['alias_key']} -> {row['mapped_food_canonical_name']}: "
        f"usage={row['usage_count']}, rows_with_grams={row['rows_with_grams']}"
        for row in new_alias_usage
    )
    lines.extend(
        [
            "",
            "Food_DB v1.1 draft additions used:",
        ]
    )
    lines.extend(
        f"- {row['alias_key']} -> {row['mapped_food_id']}: "
        f"usage={row['usage_count']}, rows_with_grams={row['rows_with_grams']}"
        for row in new_fooddb_item_usage
    )
    lines.extend(
        [
            "",
            "Review promotions in this run:",
        ]
    )
    if promoted_counts:
        lines.extend(f"- {name}: {count}" for name, count in promoted_counts)
    else:
        lines.append("- none")
    lines.extend(["", "Review promotions with grams:"])
    if promoted_with_grams_counts:
        lines.extend(f"- {name}: {count}" for name, count in promoted_with_grams_counts)
    else:
        lines.append("- none")
    lines.extend(["", "Round2 promotions in this run:"])
    if round2_counts:
        lines.extend(f"- {name}: {count}" for name, count in round2_counts)
    else:
        lines.append("- none")
    lines.extend(["", "Round2 promotions with grams:"])
    if round2_with_grams_counts:
        lines.extend(f"- {name}: {count}" for name, count in round2_with_grams_counts)
    else:
        lines.append("- none")
    lines.extend(["", "Round3 targeted blocker promotions in this run:"])
    if round3_counts:
        lines.extend(f"- {name}: {count}" for name, count in round3_counts)
    else:
        lines.append("- none")
    lines.extend(["", "Round3 targeted blocker promotions with grams:"])
    if round3_with_grams_counts:
        lines.extend(f"- {name}: {count}" for name, count in round3_with_grams_counts)
    else:
        lines.append("- none")
    lines.extend(["", "Round2 Food_DB additions/promotions applied:"])
    if round2_applied_rows:
        lines.extend(
            f"- {row.get('ingredient_name_normalized', '')} | {row.get('final_action', '')} | "
            f"{row.get('suggested_food_id', '')}"
            for row in round2_applied_rows
        )
    else:
        lines.append("- none")
    lines.extend(["", "Round2 Food_DB additions deferred:"])
    if round2_deferred_rows:
        lines.extend(
            f"- {row.get('ingredient_name_normalized', '')} | reason={row.get('defer_reason', '')}"
            for row in round2_deferred_rows
        )
    else:
        lines.append("- none")
    lines.extend(["", "Round3 Food_DB alias decisions applied:"])
    if round3_applied_rows:
        lines.extend(
            f"- {row.get('ingredient_name_normalized', '')} | {row.get('final_action', '')} | "
            f"{row.get('suggested_food_id', '')}"
            for row in round3_applied_rows
        )
    else:
        lines.append("- none")
    lines.extend(["", "Round3 Food_DB decisions deferred:"])
    if round3_deferred_rows:
        lines.extend(
            f"- {row.get('ingredient_name_normalized', '')} | reason={row.get('defer_reason', '')}"
            for row in round3_deferred_rows
        )
    else:
        lines.append("- none")
    lines.extend(["", "Items kept in review and why:"])
    lines.extend(f"- {name}: {count}" for name, count in kept_review_reasons)
    lines.extend(
        [
            "",
            "Mapping status counts:",
        ]
    )
    lines.extend(f"- {status}: {count}" for status, count in status_counts.most_common())
    lines.extend(
        [
            "",
            f"accepted_auto_with_grams: {after_counts['accepted_auto_with_grams']}",
            f"review_needed_with_grams: {after_counts['review_needed_with_grams']}",
            f"unmapped_with_grams: {after_counts['unmapped_with_grams']}",
            "",
            "Top safe aliases used:",
        ]
    )
    lines.extend(
        f"- {row['alias_key']} -> {row['mapped_food_canonical_name']}: "
        f"usage={row['usage_count']}, rows_with_grams={row['rows_with_grams']}"
        for row in alias_usage_rows[:20]
    )
    lines.append("")
    lines.append("Top unmapped ingredients with grams:")
    lines.extend(f"- {name}: {count}" for name, count in top_by_ingredient(unmapped_rows, limit=20))
    lines.append("")
    lines.append("Top review_needed ingredients with grams:")
    lines.extend(f"- {name}: {count}" for name, count in top_by_ingredient(review_rows, limit=20))
    lines.append("")
    lines.append("Top ingredients still blocking nutrition cache:")
    lines.extend(f"- {name}: {count}" for name, count in blocking_counts)
    lines.extend(["", f"Expected next patch: {expected_next_patch}"])
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    fooddb_path = Path(args.fooddb)
    fooddb_version_used = clean_text(args.fooddb_version_used) or infer_fooddb_version_used(fooddb_path, args.output_suffix)
    out_matches = resolve_output_path(args.out_matches, DEFAULT_MATCHES_OUT, args.output_suffix)
    out_unmapped = resolve_output_path(args.out_unmapped, DEFAULT_UNMAPPED_OUT, args.output_suffix)
    out_summary = resolve_output_path(args.out_summary, DEFAULT_SUMMARY_OUT, args.output_suffix)
    out_review = resolve_output_path(args.out_review, DEFAULT_REVIEW_OUT, args.output_suffix)
    out_alias_usage = resolve_output_path(args.out_alias_usage, DEFAULT_ALIAS_USAGE_OUT, args.output_suffix)

    read_csv_rows(Path(args.recipes))
    ingredient_rows = read_csv_rows(Path(args.ingredients))
    food_rows = read_csv_rows(fooddb_path)
    food_indexes = build_food_indexes(food_rows)
    baseline_path = choose_baseline_path(args.output_suffix)
    baseline_counts = load_baseline_counts(baseline_path)
    normalized_suffix = normalize_match_text(args.output_suffix)
    enable_review_promotions = "review promotions" in normalized_suffix or "round3" in normalized_suffix
    enable_round2_promotions = "round2" in normalized_suffix or "round3" in normalized_suffix
    enable_round3_promotions = "round3" in normalized_suffix

    mapped_rows = [
        output_row(
            row,
            attempt_match(
                row,
                food_indexes,
                enable_review_promotions,
                enable_round2_promotions,
                enable_round3_promotions,
            ),
            fooddb_version_used,
        )
        for row in ingredient_rows
    ]
    unmapped_rows = [row for row in mapped_rows if row["mapping_status"] == "unmapped"]
    review_rows = [row for row in mapped_rows if row["mapping_status"] == "review_needed"]
    alias_usage_rows = build_safe_alias_usage(mapped_rows)

    write_csv(out_matches, mapped_rows, OUTPUT_COLUMNS)
    write_csv(out_unmapped, unmapped_rows, OUTPUT_COLUMNS)
    write_csv(out_review, review_rows, OUTPUT_COLUMNS)
    write_csv(
        out_alias_usage,
        alias_usage_rows,
        [
            "fooddb_version_used",
            "alias_key",
            "mapped_food_id",
            "mapped_food_canonical_name",
            "usage_count",
            "rows_with_grams",
            "example_raw_texts",
            "example_recipes",
        ],
    )
    summary = build_summary(mapped_rows, alias_usage_rows, baseline_counts, baseline_path, fooddb_version_used, fooddb_path)
    summary_path = out_summary
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary, encoding="utf-8")

    status_counts = Counter(clean_text(row["mapping_status"]) for row in mapped_rows)
    print(f"Total ingredient rows: {len(mapped_rows)}")
    print("Mapping status counts:")
    for status, count in status_counts.most_common():
        print(f" - {status}: {count}")
    print(f"accepted_auto_with_grams: {sum(1 for row in mapped_rows if row['mapping_status'] == 'accepted_auto' and has_grams(row))}")
    print(f"review_needed_with_grams: {sum(1 for row in mapped_rows if row['mapping_status'] == 'review_needed' and has_grams(row))}")
    print(f"unmapped_with_grams: {sum(1 for row in mapped_rows if row['mapping_status'] == 'unmapped' and has_grams(row))}")
    print(f"Safe alias keys used: {len(alias_usage_rows)}")
    print(f"Food_DB version used: {fooddb_version_used}")
    print(f"Written: {out_matches}")
    print(f"Written: {out_unmapped}")
    print(f"Written: {out_summary}")
    print(f"Written: {out_review}")
    print(f"Written: {out_alias_usage}")


if __name__ == "__main__":
    main()
