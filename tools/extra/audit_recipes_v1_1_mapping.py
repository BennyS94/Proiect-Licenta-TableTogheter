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
DEFAULT_SUMMARY_OUT = Path("data/recipesdb/audit/recipes_v1_1_mapping_summary.txt")
DEFAULT_AUDIT_OUT = Path("data/recipesdb/audit/recipes_v1_1_mapping_audit.csv")
DEFAULT_ALIAS_OUT = Path("data/recipesdb/audit/recipes_v1_1_mapping_alias_candidates.csv")
DEFAULT_GAP_OUT = Path("data/recipesdb/audit/recipes_v1_1_mapping_fooddb_gap_candidates.csv")
DEFAULT_UNIT_RULES_OUT = Path("data/recipesdb/audit/recipes_v1_1_mapping_unit_rules_needed.csv")

AUDIT_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "ingredient_position",
    "ingredient_raw_text",
    "ingredient_name_normalized",
    "quantity_value",
    "quantity_unit",
    "quantity_grams_estimated",
    "parse_status",
    "mapping_audit_status",
    "suggested_food_id",
    "suggested_food_canonical_name",
    "mapping_confidence_guess",
    "mapping_reason",
    "needs_unit_to_grams_rule",
    "likely_fooddb_gap",
    "priority",
]

ALIAS_COLUMNS = [
    "ingredient_name_normalized",
    "suggested_food_id",
    "suggested_food_canonical_name",
    "frequency",
    "rows_with_grams",
    "example_raw_texts",
    "example_recipes",
    "priority",
    "safety",
]

GAP_COLUMNS = [
    "ingredient_name_normalized",
    "suggested_canonical_name",
    "suggested_food_group",
    "suggested_role",
    "frequency",
    "rows_with_grams",
    "example_raw_texts",
    "example_recipes",
    "priority",
    "proposed_action",
]

UNIT_RULE_COLUMNS = [
    "ingredient_name_normalized",
    "quantity_unit",
    "frequency",
    "example_raw_texts",
    "likely_safe_rule",
    "proposed_grams_per_unit",
    "safety",
]

CORE_MACRO_TERMS = {
    "chicken",
    "turkey",
    "beef",
    "pork",
    "fish",
    "salmon",
    "tuna",
    "cod",
    "shrimp",
    "egg",
    "eggs",
    "rice",
    "pasta",
    "potato",
    "potatoes",
    "oats",
    "bread",
    "beans",
    "lentils",
    "milk",
    "yogurt",
    "cheese",
    "olive oil",
    "butter",
}

MAIN_RECIPE_KINDS = {"complete_main", "near_complete_main"}
MAPPABLE_STATUSES = {"exact_match", "normalized_match", "alias_candidate"}
FOCUS_UNITS = {
    "teaspoon",
    "tablespoon",
    "cup",
    "clove",
    "can",
    "package",
    "packet",
    "jar",
    "box",
    "slice",
    "count",
    "pound",
    "ounce",
}
OUT_OF_SCOPE_TERMS = {
    "cooking spray",
    "nonstick cooking spray",
    "aluminum foil",
    "foil",
    "parchment paper",
    "water to cover",
}

ALIAS_TARGETS_BY_ID = {
    "black pepper": ("food_black_pepper_powder", "safe_auto"),
    "ground black pepper": ("food_black_pepper_powder", "safe_auto"),
    "freshly ground black pepper": ("food_black_pepper_powder", "safe_auto"),
    "freshly black pepper": ("food_black_pepper_powder", "safe_auto"),
    "garlic powder": ("food_garlic_powder_dried", "safe_auto"),
    "soy sauce": ("food_soy_sauce_prepacked", "safe_auto"),
    "light soy sauce": ("food_soy_sauce_prepacked", "needs_review"),
    "olive oil": ("food_olive_oil_extra_virgin", "safe_auto"),
    "virgin olive oil": ("food_olive_oil_extra_virgin", "safe_auto"),
    "extra virgin olive oil": ("food_olive_oil_extra_virgin", "safe_auto"),
    "salt": ("food_salt_white_sea_igneous_or_rock_no_enrichment", "safe_auto"),
    "kosher salt": ("food_salt_white_sea_igneous_or_rock_no_enrichment", "needs_review"),
    "sea salt": ("food_sea_salt_grey_no_enrichment", "needs_review"),
    "white sugar": ("food_sugar_white", "safe_auto"),
    "sugar": ("food_sugar_white", "needs_review"),
    "egg": ("food_egg_raw", "safe_auto"),
    "eggs": ("food_egg_raw", "safe_auto"),
    "garlic": ("food_garlic_fresh", "safe_auto"),
    "garlic cloves": ("food_garlic_fresh", "safe_auto"),
    "onion": ("food_yellow_onion_raw", "needs_review"),
    "onions": ("food_yellow_onion_raw", "needs_review"),
    "yellow onion": ("food_yellow_onion_raw", "safe_auto"),
    "red onion": ("food_red_onion_raw", "safe_auto"),
    "green onions": ("food_chive_or_spring_onion_fresh", "needs_review"),
    "flank steak": ("food_beef_flank_steak_raw", "safe_auto"),
    "beef short ribs": ("food_beef_short_ribs_raw", "safe_auto"),
    "rice": ("food_rice_raw", "needs_review"),
    "white rice": ("food_rice_raw", "safe_auto"),
    "uncooked white rice": ("food_rice_raw", "safe_auto"),
    "cooked rice": ("food_rice_cooked_unsalted", "safe_auto"),
    "basmati rice": ("food_basmati_rice_raw", "safe_auto"),
    "jasmine rice": ("food_rice_raw", "needs_review"),
    "pasta": ("food_dried_pasta_raw", "needs_review"),
    "butter": ("food_butter_82_fat_unsalted", "needs_review"),
    "unsalted butter": ("food_butter_82_fat_unsalted", "safe_auto"),
    "oregano": ("food_oregano_dried", "needs_review"),
    "basil": ("food_basil_dried", "needs_review"),
    "thyme": ("food_thyme_dried", "needs_review"),
    "parsley": ("food_parsley_fresh", "needs_review"),
    "balsamic vinegar": ("food_vinegar_balsamic", "safe_auto"),
    "red wine vinegar": ("food_wine_vinegar", "needs_review"),
    "vinegar": ("food_vinegar", "needs_review"),
    "sesame oil": ("food_sesame_oil", "safe_auto"),
    "cumin": ("food_cumin_seed", "needs_review"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ruleaza un audit read-only pentru mapping-ul ingredientelor Recipes_DB v1.1."
    )
    parser.add_argument("--recipes", default=str(DEFAULT_RECIPES))
    parser.add_argument("--ingredients", default=str(DEFAULT_INGREDIENTS))
    parser.add_argument("--fooddb", default=str(DEFAULT_FOODDB))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY_OUT))
    parser.add_argument("--out-audit", default=str(DEFAULT_AUDIT_OUT))
    parser.add_argument("--out-aliases", default=str(DEFAULT_ALIAS_OUT))
    parser.add_argument("--out-gaps", default=str(DEFAULT_GAP_OUT))
    parser.add_argument("--out-unit-rules", default=str(DEFAULT_UNIT_RULES_OUT))
    return parser.parse_args()


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


def singularize(value: str) -> str:
    text = normalize_match_text(value)
    words = text.split()
    if not words:
        return ""
    last = words[-1]
    if len(last) > 4 and last.endswith("ies"):
        words[-1] = last[:-3] + "y"
    elif len(last) > 4 and last.endswith("es"):
        words[-1] = last[:-2]
    elif len(last) > 3 and last.endswith("s") and not last.endswith("ss"):
        words[-1] = last[:-1]
    return " ".join(words)


def bool_from_string(value: object) -> bool:
    return clean_text(value).lower() in {"1", "true", "yes"}


def float_or_none(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def build_index(rows: list[dict[str, str]], column_name: str, key_fn) -> dict[str, list[dict[str, str]]]:
    index: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        value = clean_text(row.get(column_name))
        if not value:
            continue
        key = key_fn(value)
        if key:
            index[key].append(row)
    return dict(index)


def build_food_indexes(food_rows: list[dict[str, str]]) -> dict[str, dict[str, list[dict[str, str]]]]:
    return {
        "canonical_exact": build_index(food_rows, "canonical_name", exact_key),
        "display_exact": build_index(food_rows, "display_name", exact_key),
        "family_exact": build_index(food_rows, "food_family_name", exact_key),
        "canonical_normalized": build_index(food_rows, "canonical_name", normalize_match_text),
        "display_normalized": build_index(food_rows, "display_name", normalize_match_text),
        "family_normalized": build_index(food_rows, "food_family_name", normalize_match_text),
        "by_food_id": build_index(food_rows, "food_id", clean_text),
    }


def unique_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen = set()
    output = []
    for row in rows:
        food_id = clean_text(row.get("food_id"))
        if food_id and food_id not in seen:
            seen.add(food_id)
            output.append(row)
    return output


def first_unique(rows: list[dict[str, str]]) -> dict[str, str] | None:
    deduped = unique_rows(rows)
    if len(deduped) == 1:
        return deduped[0]
    return None


def find_by_food_id(food_indexes: dict[str, dict[str, list[dict[str, str]]]], food_id: str) -> dict[str, str] | None:
    return first_unique(food_indexes["by_food_id"].get(food_id, []))


def target_alias_row(
    ingredient_name: str,
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
) -> tuple[dict[str, str] | None, str]:
    target = ALIAS_TARGETS_BY_ID.get(ingredient_name)
    if not target:
        return None, ""
    food_id, safety = target
    return find_by_food_id(food_indexes, food_id), safety


def classify_direct_match(
    ingredient_name: str,
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
) -> tuple[str, dict[str, str] | None, str, str]:
    exact_candidate = exact_key(ingredient_name)
    normalized_candidate = normalize_match_text(ingredient_name)
    singular_candidate = singularize(ingredient_name)

    exact_attempts = [
        ("canonical exact", food_indexes["canonical_exact"].get(exact_candidate, [])),
        ("display exact", food_indexes["display_exact"].get(exact_candidate, [])),
    ]
    for reason, candidate_rows in exact_attempts:
        candidate = first_unique(candidate_rows)
        if candidate:
            return "exact_match", candidate, "high", reason
        if len(unique_rows(candidate_rows)) > 1:
            return "ambiguous_review", None, "low", f"{reason}; multiple Food_DB rows"

    normalized_attempts = [
        ("canonical normalized", food_indexes["canonical_normalized"].get(normalized_candidate, [])),
        ("display normalized", food_indexes["display_normalized"].get(normalized_candidate, [])),
        ("canonical singular normalized", food_indexes["canonical_normalized"].get(singular_candidate, [])),
        ("display singular normalized", food_indexes["display_normalized"].get(singular_candidate, [])),
    ]
    for reason, candidate_rows in normalized_attempts:
        candidate = first_unique(candidate_rows)
        if candidate:
            return "normalized_match", candidate, "medium", reason
        if len(unique_rows(candidate_rows)) > 1:
            return "ambiguous_review", None, "low", f"{reason}; multiple Food_DB rows"

    family_attempts = [
        ("family exact", food_indexes["family_exact"].get(exact_candidate, [])),
        ("family normalized", food_indexes["family_normalized"].get(normalized_candidate, [])),
        ("family singular normalized", food_indexes["family_normalized"].get(singular_candidate, [])),
    ]
    for reason, candidate_rows in family_attempts:
        if candidate_rows:
            candidate = first_unique(candidate_rows)
            if candidate:
                return "ambiguous_review", candidate, "low", f"{reason}; family match needs review"
            preview_count = len(unique_rows(candidate_rows))
            return "ambiguous_review", None, "low", f"{reason}; {preview_count} Food_DB rows"

    return "no_match", None, "low", "no conservative match"


def has_composite_name(value: str) -> bool:
    text = normalize_match_text(value)
    return " and " in f" {text} " or " or " in f" {text} "


def is_out_of_scope(row: dict[str, str]) -> bool:
    text = normalize_match_text(
        f"{row.get('ingredient_raw_text', '')} {row.get('ingredient_name_normalized', '')}"
    )
    if any(term in text for term in OUT_OF_SCOPE_TERMS):
        return True
    return clean_text(row.get("ingredient_role")) == "other" and not clean_text(row.get("ingredient_name_normalized"))


def infer_role(ingredient_name: str, ingredient_role: str) -> str:
    role = clean_text(ingredient_role)
    name = normalize_match_text(ingredient_name)
    if any(term in name for term in ["green bean", "celery", "zucchini", "mushroom", "bell pepper", "cabbage"]):
        return "veg"
    if any(
        term in name
        for term in [
            "pepper flakes",
            "cayenne",
            "paprika",
            "oregano",
            "basil",
            "thyme",
            "parsley",
            "cumin",
            "cinnamon",
            "chili powder",
            "garlic powder",
            "onion powder",
            "bay leaves",
            "rosemary",
            "ginger",
        ]
    ):
        return "seasoning"
    if "broth" in name or "stock" in name:
        return "sauce"
    if "cheese" in name or "milk" in name or "yogurt" in name:
        return "dairy"
    if role == "protein":
        return "protein"
    if role == "carb":
        return "carb"
    if role == "veg":
        return "veg"
    if role == "fruit":
        return "fruit"
    if role == "fat_source":
        return "fat"
    if role == "dairy":
        return "dairy"
    if role == "seasoning":
        return "seasoning"
    if role == "sauce":
        return "sauce"
    if any(term in name for term in ["chicken", "turkey", "beef", "pork", "fish", "salmon", "tuna", "shrimp", "egg"]):
        return "protein"
    if any(term in name for term in ["rice", "pasta", "potato", "oats", "bread", "flour", "beans", "lentils"]):
        return "carb"
    if any(term in name for term in ["oil", "butter"]):
        return "fat"
    if any(term in name for term in ["milk", "yogurt", "cheese"]):
        return "dairy"
    return "other"


def suggested_food_group_for_role(role: str) -> str:
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


def has_core_macro_signal(name: str, role: str) -> bool:
    normalized = normalize_match_text(name)
    if role in {"protein", "carb", "dairy", "fat"}:
        return True
    return any(term in normalized for term in CORE_MACRO_TERMS)


def priority_for_row(row: dict[str, str], name_frequency: int, recipe_kind: str) -> str:
    name = clean_text(row.get("ingredient_name_normalized"))
    role = infer_role(name, clean_text(row.get("ingredient_role")))
    has_grams = float_or_none(row.get("quantity_grams_estimated")) is not None
    in_main = recipe_kind in MAIN_RECIPE_KINDS
    if (name_frequency >= 8 and has_grams) or (has_grams and has_core_macro_signal(name, role) and in_main):
        return "high"
    if name_frequency >= 4 or has_grams or has_core_macro_signal(name, role):
        return "medium"
    return "low"


def priority_from_group(frequency: int, rows_with_grams: int, role: str) -> str:
    if frequency >= 8 or rows_with_grams >= 5 or role in {"protein", "carb", "dairy", "fat"}:
        return "high"
    if frequency >= 3 or rows_with_grams > 0:
        return "medium"
    return "low"


def audit_mapping_row(
    row: dict[str, str],
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
    recipe_kind_by_id: dict[str, str],
    name_counts: Counter,
) -> dict[str, object]:
    ingredient_name = normalize_match_text(row.get("ingredient_name_normalized"))
    recipe_kind = recipe_kind_by_id.get(clean_text(row.get("recipe_id_candidate")), "")
    priority = priority_for_row(row, name_counts[ingredient_name], recipe_kind)
    needs_unit_rule = needs_unit_rule_for_row(row)

    status = "no_match"
    candidate = None
    confidence = "low"
    reason = "no conservative match"
    likely_gap = ""

    if is_out_of_scope(row):
        status = "out_of_scope"
        reason = "non-food or non-nutrition line"
    elif not ingredient_name:
        status = "no_match"
        reason = "missing normalized ingredient name"
    elif has_composite_name(ingredient_name):
        status = "ambiguous_review"
        reason = "composite ingredient name"
    else:
        status, candidate, confidence, reason = classify_direct_match(ingredient_name, food_indexes)
        alias_candidate, alias_safety = target_alias_row(ingredient_name, food_indexes)
        if status in {"no_match", "ambiguous_review"} and alias_candidate:
            status = "alias_candidate"
            candidate = alias_candidate
            confidence = "high" if alias_safety == "safe_auto" else "medium"
            reason = f"explicit audit alias candidate; safety={alias_safety}; previous_reason={reason}"
        elif status == "no_match":
            role = infer_role(ingredient_name, clean_text(row.get("ingredient_role")))
            if priority in {"high", "medium"} and role != "seasoning":
                status = "fooddb_gap_candidate"
                reason = "high or medium impact ingredient without Food_DB match"
                likely_gap = "1"

    if clean_text(row.get("parse_status")) == "review_needed" and status in {"exact_match", "normalized_match"}:
        status = "ambiguous_review"
        reason = f"{reason}; parse_status review gate"
        confidence = "low"

    return {
        "recipe_id_candidate": clean_text(row.get("recipe_id_candidate")),
        "display_name": clean_text(row.get("display_name")),
        "ingredient_position": clean_text(row.get("ingredient_position")),
        "ingredient_raw_text": clean_text(row.get("ingredient_raw_text")),
        "ingredient_name_normalized": ingredient_name,
        "quantity_value": clean_text(row.get("quantity_value")),
        "quantity_unit": clean_text(row.get("quantity_unit")),
        "quantity_grams_estimated": clean_text(row.get("quantity_grams_estimated")),
        "parse_status": clean_text(row.get("parse_status")),
        "mapping_audit_status": status,
        "suggested_food_id": clean_text(candidate.get("food_id")) if candidate else "",
        "suggested_food_canonical_name": clean_text(candidate.get("canonical_name")) if candidate else "",
        "mapping_confidence_guess": confidence,
        "mapping_reason": reason,
        "needs_unit_to_grams_rule": "1" if needs_unit_rule else "",
        "likely_fooddb_gap": likely_gap,
        "priority": priority,
    }


def needs_unit_rule_for_row(row: dict[str, str]) -> bool:
    unit = clean_text(row.get("quantity_unit"))
    grams = float_or_none(row.get("quantity_grams_estimated"))
    if unit == "count" and grams is None:
        return True
    return unit in FOCUS_UNITS and grams is None


def clipped_examples(values: list[str], limit: int = 5) -> str:
    output = []
    seen = set()
    for value in values:
        cleaned = clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        output.append(cleaned)
        if len(output) >= limit:
            break
    return " | ".join(output)


def build_alias_candidates(audit_rows: list[dict[str, object]], source_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    grouped: dict[str, list[tuple[dict[str, object], dict[str, str]]]] = defaultdict(list)
    source_by_key = {
        (
            clean_text(row.get("recipe_id_candidate")),
            clean_text(row.get("ingredient_position")),
        ): row
        for row in source_rows
    }
    for audit_row in audit_rows:
        if audit_row["mapping_audit_status"] != "alias_candidate":
            continue
        key = (
            clean_text(audit_row.get("recipe_id_candidate")),
            clean_text(audit_row.get("ingredient_position")),
        )
        grouped[clean_text(audit_row["ingredient_name_normalized"])].append((audit_row, source_by_key[key]))

    output = []
    for ingredient_name, pairs in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        first_audit = pairs[0][0]
        rows_with_grams = sum(1 for audit_row, _ in pairs if clean_text(audit_row["quantity_grams_estimated"]))
        target = ALIAS_TARGETS_BY_ID.get(ingredient_name, ("", "needs_review"))
        role = infer_role(ingredient_name, clean_text(pairs[0][1].get("ingredient_role")))
        output.append(
            {
                "ingredient_name_normalized": ingredient_name,
                "suggested_food_id": clean_text(first_audit["suggested_food_id"]),
                "suggested_food_canonical_name": clean_text(first_audit["suggested_food_canonical_name"]),
                "frequency": len(pairs),
                "rows_with_grams": rows_with_grams,
                "example_raw_texts": clipped_examples([pair[1].get("ingredient_raw_text", "") for pair in pairs]),
                "example_recipes": clipped_examples([pair[0].get("display_name", "") for pair in pairs]),
                "priority": priority_from_group(len(pairs), rows_with_grams, role),
                "safety": target[1],
            }
        )
    return output


def build_gap_candidates(audit_rows: list[dict[str, object]], source_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    grouped: dict[str, list[tuple[dict[str, object], dict[str, str]]]] = defaultdict(list)
    source_by_key = {
        (
            clean_text(row.get("recipe_id_candidate")),
            clean_text(row.get("ingredient_position")),
        ): row
        for row in source_rows
    }
    for audit_row in audit_rows:
        if audit_row["mapping_audit_status"] != "fooddb_gap_candidate":
            continue
        key = (
            clean_text(audit_row.get("recipe_id_candidate")),
            clean_text(audit_row.get("ingredient_position")),
        )
        grouped[clean_text(audit_row["ingredient_name_normalized"])].append((audit_row, source_by_key[key]))

    output = []
    for ingredient_name, pairs in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        role = infer_role(ingredient_name, clean_text(pairs[0][1].get("ingredient_role")))
        rows_with_grams = sum(1 for audit_row, _ in pairs if clean_text(audit_row["quantity_grams_estimated"]))
        priority = priority_from_group(len(pairs), rows_with_grams, role)
        action = "add_to_fooddb" if priority == "high" and role != "seasoning" else "keep_review"
        if any(clean_text(audit_row["needs_unit_to_grams_rule"]) for audit_row, _ in pairs):
            action = "add_unit_rule" if rows_with_grams == 0 else action
        output.append(
            {
                "ingredient_name_normalized": ingredient_name,
                "suggested_canonical_name": ingredient_name.replace(" ", "_"),
                "suggested_food_group": suggested_food_group_for_role(role),
                "suggested_role": role,
                "frequency": len(pairs),
                "rows_with_grams": rows_with_grams,
                "example_raw_texts": clipped_examples([pair[1].get("ingredient_raw_text", "") for pair in pairs]),
                "example_recipes": clipped_examples([pair[0].get("display_name", "") for pair in pairs]),
                "priority": priority,
                "proposed_action": action,
            }
        )
    return output


def proposed_unit_rule(ingredient_name: str, unit: str) -> tuple[str, str, str]:
    name = normalize_match_text(ingredient_name)
    if unit == "clove" and "garlic" in name:
        return "garlic clove count", "3", "safe_auto"
    if unit == "count" and name in {"egg", "eggs"}:
        return "egg count", "50", "safe_auto"
    if unit == "count" and "onion" in name:
        return "medium onion fallback", "110", "needs_review"
    if unit in {"tablespoon", "teaspoon"}:
        if "olive oil" in name or "sesame oil" in name or "vegetable oil" in name:
            return f"{unit} oil density", "13.5" if unit == "tablespoon" else "4.5", "safe_auto"
        if "butter" in name:
            return f"{unit} butter density", "14.2" if unit == "tablespoon" else "4.7", "safe_auto"
        if "sugar" in name:
            return f"{unit} sugar density", "12.5" if unit == "tablespoon" else "4.2", "safe_auto"
        if "salt" in name:
            return f"{unit} salt density", "18" if unit == "tablespoon" else "6", "safe_auto"
        if "soy sauce" in name:
            return f"{unit} soy sauce density", "16" if unit == "tablespoon" else "5.3", "safe_auto"
        return f"{unit} needs ingredient density", "", "needs_review"
    if unit == "cup":
        if "rice" in name:
            return "cup rice density depends raw/cooked", "", "needs_review"
        if "milk" in name or "water" in name:
            return "cup liquid density", "240", "safe_auto"
        if "flour" in name:
            return "cup flour density", "120", "safe_auto"
        if "sugar" in name:
            return "cup sugar density", "200", "safe_auto"
        return "cup needs ingredient density", "", "needs_review"
    if unit in {"can", "package", "packet", "jar", "box"}:
        return "packaging unit needs label-size parsing", "", "needs_review"
    if unit == "slice":
        if "bread" in name:
            return "bread slice estimate", "30", "needs_review"
        return "slice needs food-specific rule", "", "needs_review"
    if unit in {"pound", "ounce"}:
        return f"{unit} should already be direct weight", "453.592" if unit == "pound" else "28.3495", "safe_auto"
    if unit == "count":
        return "count needs food-specific rule", "", "needs_review"
    return "unit needs review", "", "needs_review"


def build_unit_rules_needed(source_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in source_rows:
        unit = clean_text(row.get("quantity_unit"))
        if unit == "":
            continue
        if unit not in FOCUS_UNITS:
            continue
        if float_or_none(row.get("quantity_grams_estimated")) is not None and unit not in {"can", "package", "packet", "jar", "box"}:
            continue
        name = normalize_match_text(row.get("ingredient_name_normalized"))
        grouped[(name, unit)].append(row)

    output = []
    for (name, unit), rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0][1], item[0][0])):
        likely_rule, grams_per_unit, safety = proposed_unit_rule(name, unit)
        output.append(
            {
                "ingredient_name_normalized": name,
                "quantity_unit": unit,
                "frequency": len(rows),
                "example_raw_texts": clipped_examples([row.get("ingredient_raw_text", "") for row in rows]),
                "likely_safe_rule": likely_rule,
                "proposed_grams_per_unit": grams_per_unit,
                "safety": safety,
            }
        )
    return output


def build_summary(
    audit_rows: list[dict[str, object]],
    alias_rows: list[dict[str, object]],
    gap_rows: list[dict[str, object]],
    unit_rows: list[dict[str, object]],
) -> str:
    status_counts = Counter(clean_text(row["mapping_audit_status"]) for row in audit_rows)
    rows_with_grams_and_mappable = sum(
        1
        for row in audit_rows
        if clean_text(row["quantity_grams_estimated"]) and row["mapping_audit_status"] in MAPPABLE_STATUSES
    )
    rows_with_grams_but_no_mapping = sum(
        1
        for row in audit_rows
        if clean_text(row["quantity_grams_estimated"]) and row["mapping_audit_status"] not in MAPPABLE_STATUSES
    )
    rows_with_mapping_but_no_grams = sum(
        1
        for row in audit_rows
        if not clean_text(row["quantity_grams_estimated"]) and row["mapping_audit_status"] in MAPPABLE_STATUSES
    )
    safe_alias_rows_with_grams = sum(
        int(row["rows_with_grams"])
        for row in alias_rows
        if row["safety"] == "safe_auto"
    )
    high_gap_count = sum(1 for row in gap_rows if row["priority"] == "high")
    if safe_alias_rows_with_grams >= 50:
        recommendation = "A. alias patch"
    elif unit_rows and len(unit_rows) > high_gap_count:
        recommendation = "C. unit-to-grams rules"
    elif high_gap_count:
        recommendation = "B. Food_DB additions"
    else:
        recommendation = "D. parser improvements"

    lines = [
        "Recipes_DB v1.1 mapping readiness audit",
        "",
        f"Total ingredient rows: {len(audit_rows)}",
        "",
        "Mapping audit status counts:",
    ]
    lines.extend(f"- {status}: {count}" for status, count in status_counts.most_common())
    lines.extend(
        [
            "",
            f"Rows with grams and mappable: {rows_with_grams_and_mappable}",
            f"Rows with grams but no mapping: {rows_with_grams_but_no_mapping}",
            f"Rows with mapping but no grams: {rows_with_mapping_but_no_grams}",
            "",
            "Top 30 alias candidates:",
        ]
    )
    lines.extend(
        f"- {row['ingredient_name_normalized']} -> {row['suggested_food_canonical_name']}: "
        f"freq={row['frequency']}, grams={row['rows_with_grams']}, priority={row['priority']}, safety={row['safety']}"
        for row in alias_rows[:30]
    )
    lines.append("")
    lines.append("Top 30 Food_DB gaps:")
    lines.extend(
        f"- {row['ingredient_name_normalized']}: freq={row['frequency']}, grams={row['rows_with_grams']}, "
        f"role={row['suggested_role']}, priority={row['priority']}, action={row['proposed_action']}"
        for row in gap_rows[:30]
    )
    lines.append("")
    lines.append("Top 30 unit-to-grams needs:")
    lines.extend(
        f"- {row['ingredient_name_normalized']} [{row['quantity_unit']}]: freq={row['frequency']}, "
        f"rule={row['likely_safe_rule']}, safety={row['safety']}"
        for row in unit_rows[:30]
    )
    lines.extend(
        [
            "",
            f"Likely next patch recommendation: {recommendation}",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    recipes_path = Path(args.recipes)
    ingredients_path = Path(args.ingredients)
    fooddb_path = Path(args.fooddb)

    recipe_rows = read_csv_rows(recipes_path)
    ingredient_rows = read_csv_rows(ingredients_path)
    food_rows = read_csv_rows(fooddb_path)

    recipe_kind_by_id = {
        clean_text(row.get("recipe_id_candidate")): clean_text(row.get("recipe_kind_guess"))
        for row in recipe_rows
    }
    food_indexes = build_food_indexes(food_rows)
    name_counts = Counter(normalize_match_text(row.get("ingredient_name_normalized")) for row in ingredient_rows)

    audit_rows = [
        audit_mapping_row(row, food_indexes, recipe_kind_by_id, name_counts)
        for row in ingredient_rows
    ]
    alias_rows = build_alias_candidates(audit_rows, ingredient_rows)
    gap_rows = build_gap_candidates(audit_rows, ingredient_rows)
    unit_rows = build_unit_rules_needed(ingredient_rows)

    write_csv(Path(args.out_audit), audit_rows, AUDIT_COLUMNS)
    write_csv(Path(args.out_aliases), alias_rows, ALIAS_COLUMNS)
    write_csv(Path(args.out_gaps), gap_rows, GAP_COLUMNS)
    write_csv(Path(args.out_unit_rules), unit_rows, UNIT_RULE_COLUMNS)
    summary = build_summary(audit_rows, alias_rows, gap_rows, unit_rows)
    summary_path = Path(args.out_summary)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary, encoding="utf-8")

    status_counts = Counter(clean_text(row["mapping_audit_status"]) for row in audit_rows)
    print(f"Total ingredient rows: {len(audit_rows)}")
    print("Mapping audit status counts:")
    for status, count in status_counts.most_common():
        print(f" - {status}: {count}")
    print(f"Alias candidates: {len(alias_rows)}")
    print(f"Food_DB gap candidates: {len(gap_rows)}")
    print(f"Unit-to-grams needs: {len(unit_rows)}")
    print(f"Written: {args.out_summary}")
    print(f"Written: {args.out_audit}")
    print(f"Written: {args.out_aliases}")
    print(f"Written: {args.out_gaps}")
    print(f"Written: {args.out_unit_rules}")


if __name__ == "__main__":
    main()
