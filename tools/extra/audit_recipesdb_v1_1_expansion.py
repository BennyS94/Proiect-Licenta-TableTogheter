from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


DEFAULT_SOURCE_RECIPES = Path("data/recipesdb/source/1_Recipe_csv.csv")
DEFAULT_CURRENT_RECIPES = Path("data/recipesdb/current/recipes.csv")
DEFAULT_CURRENT_INGREDIENTS = Path("data/recipesdb/current/recipe_ingredients.csv")
DEFAULT_FOODDB = Path("data/fooddb/current/fooddb_v1_core_master_draft.csv")

DEFAULT_RECIPE_AUDIT_OUT = Path(
    "data/recipesdb/audit/recipes_v1_1_candidate_recipe_audit.csv"
)
DEFAULT_SUMMARY_OUT = Path("data/recipesdb/audit/recipes_v1_1_candidate_summary.txt")
DEFAULT_INGREDIENT_GAP_OUT = Path(
    "data/recipesdb/audit/recipes_v1_1_ingredient_gap_audit.csv"
)
DEFAULT_FOODDB_GAP_OUT = Path("data/fooddb/audit/fooddb_v1_1_gap_candidates.csv")

MIN_STEPS = 1
MAX_STEPS = 8
MAX_INGREDIENTS_DEFAULT = 14
MAX_INGREDIENTS_BREAKFAST_SNACK = 10
KEEP_SCORE_THRESHOLD = 6.0
REVIEW_SCORE_THRESHOLD = 4.0

UNIT_NORMALIZATION = {
    "cup": "cup",
    "cups": "cup",
    "teaspoon": "teaspoon",
    "teaspoons": "teaspoon",
    "tsp": "teaspoon",
    "tablespoon": "tablespoon",
    "tablespoons": "tablespoon",
    "tbsp": "tablespoon",
    "ounce": "ounce",
    "ounces": "ounce",
    "oz": "ounce",
    "pound": "pound",
    "pounds": "pound",
    "lb": "pound",
    "lbs": "pound",
    "gram": "gram",
    "grams": "gram",
    "g": "gram",
    "kilogram": "kilogram",
    "kilograms": "kilogram",
    "kg": "kilogram",
    "milliliter": "milliliter",
    "milliliters": "milliliter",
    "ml": "milliliter",
    "liter": "liter",
    "liters": "liter",
    "l": "liter",
    "clove": "clove",
    "cloves": "clove",
    "slice": "slice",
    "slices": "slice",
    "piece": "piece",
    "pieces": "piece",
    "can": "can",
    "cans": "can",
    "package": "package",
    "packages": "package",
    "jar": "jar",
    "jars": "jar",
    "bottle": "bottle",
    "bottles": "bottle",
    "packet": "packet",
    "packets": "packet",
    "bunch": "bunch",
    "bunches": "bunch",
    "stalk": "stalk",
    "stalks": "stalk",
    "head": "head",
    "heads": "head",
    "fillet": "fillet",
    "fillets": "fillet",
    "breast": "breast",
    "breasts": "breast",
    "thigh": "thigh",
    "thighs": "thigh",
}

FRACTION_MAP = {
    "¼": "1/4",
    "½": "1/2",
    "¾": "3/4",
    "Â¼": "1/4",
    "Â½": "1/2",
    "Â¾": "3/4",
    "â…“": "1/3",
    "â…”": "2/3",
    "â…›": "1/8",
    "â…œ": "3/8",
    "â…": "5/8",
    "â…ž": "7/8",
}

DIRECT_WEIGHT_UNITS = {
    "gram",
    "kilogram",
    "ounce",
    "pound",
}

UNIT_RULE_UNITS = {
    "cup",
    "teaspoon",
    "tablespoon",
    "clove",
    "slice",
    "piece",
    "bunch",
    "stalk",
    "head",
    "fillet",
    "breast",
    "thigh",
}

PACKAGING_UNITS = {
    "can",
    "package",
    "jar",
    "bottle",
    "packet",
}

SAFE_ALIAS_RULES = {
    "ground black pepper": "Black pepper, powder",
    "freshly ground black pepper": "Black pepper, powder",
    "garlic powder": "Garlic, powder, dried",
    "ground cinnamon": "Cinnamon, powder",
    "dried basil": "Basil, dried",
    "dried oregano": "Oregano, dried",
    "dried thyme": "Thyme, dried",
    "dried parsley": "Parsley, dried",
    "balsamic vinegar": "Vinegar, balsamic",
}

SELECTED_FAMILY_PROMOTIONS = {
    "soy sauce": "Soy sauce, prepacked",
    "black pepper": "Black pepper, powder",
    "red onion": "Red onion, raw",
    "yellow onion": "Yellow onion, raw",
    "lemon zest": "Lemon zest, raw",
    "poppy seeds": "Poppy, seed",
}

COMMON_CANONICAL_ALIASES = {
    "egg": "egg_raw",
    "eggs": "egg_raw",
    "garlic": "garlic_fresh",
    "garlic clove": "garlic_fresh",
    "garlic cloves": "garlic_fresh",
    "olive oil": "olive_oil_extra_virgin",
    "onion": "onion_raw",
    "onions": "onion_raw",
    "salt": "salt_white_sea_igneous_or_rock_no_enrichment",
    "white sugar": "sugar_white",
    "sugar": "sugar_white",
    "brown sugar": "sugar_brown",
}

PREFERRED_PROTEINS = {
    "chicken",
    "beef",
    "pork",
    "turkey",
    "fish",
    "salmon",
    "tuna",
    "cod",
    "shrimp",
    "egg",
    "eggs",
    "lentil",
    "lentils",
    "bean",
    "beans",
}

PREFERRED_CARBS = {
    "rice",
    "potato",
    "potatoes",
    "pasta",
    "spaghetti",
    "noodle",
    "noodles",
    "bread",
    "oats",
    "oatmeal",
    "barley",
    "couscous",
    "polenta",
}

COMMON_VEGETABLES = {
    "onion",
    "garlic",
    "tomato",
    "tomatoes",
    "carrot",
    "carrots",
    "pepper",
    "peppers",
    "zucchini",
    "cabbage",
    "spinach",
    "broccoli",
    "cauliflower",
    "lettuce",
    "cucumber",
    "mushroom",
    "mushrooms",
    "peas",
    "kale",
    "eggplant",
}

OUT_OF_SCOPE_KEYWORDS = {
    "cake",
    "cakes",
    "cookie",
    "cookies",
    "dessert",
    "brownie",
    "brownies",
    "cupcake",
    "cupcakes",
    "frosting",
    "icing",
    "candy",
    "cocktail",
    "martini",
    "smoothie",
    "shake",
    "liqueur",
    "jam",
    "jelly",
    "ice cream",
    "non edible",
    "non-edible",
    "ornament",
    "ornaments",
    "pie",
    "pies",
    "doughnut",
    "doughnuts",
    "donut",
    "donuts",
}

HARD_TO_SOURCE_KEYWORDS = {
    "gochujang",
    "tamarind",
    "yuzu",
    "pandan",
    "annatto",
    "hominy",
    "masa harina",
    "plantain",
    "jicama",
    "jackfruit",
}

PROCESSED_KEYWORDS = {
    "velveeta",
    "cake mix",
    "jell-o",
    "cool whip",
    "crescent roll",
    "tater tots",
    "cream of chicken soup",
    "cream of mushroom soup",
    "ranch dressing mix",
    "seasoning packet",
}

SAUCE_COMPONENT_KEYWORDS = {
    "sauce",
    "dressing",
    "marinade",
    "dip",
    "salsa",
    "syrup",
    "spread",
    "aioli",
    "pesto",
    "seasoning",
    "spice blend",
    "spice mix",
    "rub",
    "herb blend",
}

COMPONENT_CATEGORY_KEYWORDS = {
    "food gifts",
    "homemade spice blends",
    "sauces and condiments",
    "marinades",
    "dips and spreads",
}

PREPARATION_WORDS = {
    "fresh",
    "dried",
    "ground",
    "minced",
    "chopped",
    "diced",
    "sliced",
    "shredded",
    "grated",
    "crushed",
    "peeled",
    "trimmed",
    "boneless",
    "skinless",
    "lean",
    "extra",
    "large",
    "small",
    "medium",
    "cooked",
    "raw",
    "frozen",
    "thawed",
}

RECIPE_AUDIT_COLUMNS = [
    "source_row_number",
    "recipe_title",
    "category",
    "subcategory",
    "num_ingredients",
    "num_steps",
    "already_in_current",
    "keep_for_v1_1",
    "review_reason",
    "recipe_kind_guess",
    "european_practicality_score",
    "ingredient_complexity_score",
    "expected_mapping_difficulty",
    "ingredient_count_parsed",
    "already_mappable_count",
    "needs_alias_only_count",
    "needs_fooddb_item_count",
    "needs_unit_to_grams_rule_count",
    "too_ambiguous_count",
    "out_of_scope_ingredient_count",
]

INGREDIENT_GAP_COLUMNS = [
    "source_row_number",
    "recipe_title",
    "recipe_kind_guess",
    "keep_for_v1_1",
    "ingredient_position",
    "ingredient_raw",
    "ingredient_name_normalized",
    "food_name_candidate",
    "quantity_value",
    "quantity_unit",
    "gap_class",
    "matched_food_id",
    "matched_canonical_name",
    "matched_display_name",
    "suggested_action",
    "suggested_role",
    "gap_notes",
]

FOODDB_GAP_COLUMNS = [
    "ingredient_name_normalized",
    "suggested_canonical_name",
    "suggested_food_group",
    "suggested_role",
    "frequency_in_candidate_recipes",
    "example_recipes",
    "priority",
    "proposed_action",
    "gap_class",
    "matched_food_id",
    "matched_canonical_name",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit Recipes_DB v1.1 si gap-uri Food_DB, fara modificari de date curente."
    )
    parser.add_argument("--source-recipes", default=str(DEFAULT_SOURCE_RECIPES))
    parser.add_argument("--current-recipes", default=str(DEFAULT_CURRENT_RECIPES))
    parser.add_argument("--current-ingredients", default=str(DEFAULT_CURRENT_INGREDIENTS))
    parser.add_argument("--fooddb", default=str(DEFAULT_FOODDB))
    parser.add_argument("--out-recipe-audit", default=str(DEFAULT_RECIPE_AUDIT_OUT))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY_OUT))
    parser.add_argument("--out-ingredient-gap", default=str(DEFAULT_INGREDIENT_GAP_OUT))
    parser.add_argument("--out-fooddb-gap", default=str(DEFAULT_FOODDB_GAP_OUT))
    return parser.parse_args()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def normalize_space(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_label(text: object) -> str:
    return normalize_space(text).casefold()


def normalize_match_text(value: object) -> str:
    text = normalize_label(value)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = text.replace("_", " ")
    text = text.replace("-", " ")
    text = text.replace("/", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_slot_key(value: object) -> str:
    return normalize_match_text(value).replace(" ", "_")


def singularize_normalized_text(value: object) -> str:
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


KEYWORD_NORMALIZATION_CACHE: dict[str, str] = {}


def contains_any(text: str, keywords: set[str]) -> bool:
    return any(contains_keyword(text, keyword) for keyword in keywords)


def keyword_count(text: str, keywords: set[str]) -> int:
    return sum(1 for keyword in keywords if contains_keyword(text, keyword))


def contains_keyword(text: str, keyword: str) -> bool:
    normalized_keyword = KEYWORD_NORMALIZATION_CACHE.get(keyword)
    if normalized_keyword is None:
        normalized_keyword = normalize_match_text(keyword)
        KEYWORD_NORMALIZATION_CACHE[keyword] = normalized_keyword
    if not normalized_keyword:
        return False
    if " " in normalized_keyword:
        return normalized_keyword in text
    pattern = rf"(?<![a-z0-9]){re.escape(normalized_keyword)}(?![a-z0-9])"
    return re.search(pattern, text) is not None


def safe_load_list(raw_value: object) -> list[str]:
    try:
        value = json.loads(str(raw_value or ""))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def build_signature(title: str, ingredients: list[str], directions: list[str]) -> str:
    payload = "||".join(
        [
            normalize_label(title),
            " || ".join(normalize_label(item) for item in ingredients),
            " || ".join(normalize_label(item) for item in directions),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def build_food_indexes(food_rows: list[dict[str, str]]) -> dict[str, dict[str, list[dict[str, str]]]]:
    indexes: dict[str, dict[str, list[dict[str, str]]]] = {
        "canonical": defaultdict(list),
        "display": defaultdict(list),
        "family": defaultdict(list),
    }
    for row in food_rows:
        for index_name, column in [
            ("canonical", "canonical_name"),
            ("display", "display_name"),
            ("family", "food_family_name"),
        ]:
            key = normalize_match_text(row.get(column))
            if key:
                indexes[index_name][key].append(row)
    return indexes


def build_current_mapping_index(
    ingredient_rows: list[dict[str, str]],
) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    for row in ingredient_rows:
        if normalize_label(row.get("mapping_status")) != "accepted_auto":
            continue
        key = normalize_match_text(row.get("ingredient_name_normalized"))
        if not key or key in index:
            continue
        mapped_food_id = normalize_space(row.get("mapped_food_id"))
        mapped_name = normalize_space(row.get("mapped_food_canonical_name"))
        if not mapped_food_id:
            continue
        index[key] = {
            "matched_food_id": mapped_food_id,
            "matched_canonical_name": mapped_name,
            "matched_display_name": mapped_name,
            "match_method": "current_recipe_ingredient_mapping",
        }
    return index


def unique_row(rows: list[dict[str, str]]) -> dict[str, str] | None:
    seen: dict[str, dict[str, str]] = {}
    for row in rows:
        food_id = normalize_space(row.get("food_id"))
        if food_id:
            seen.setdefault(food_id, row)
    if len(seen) == 1:
        return next(iter(seen.values()))
    return None


def match_food_candidate(
    candidate: str,
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
    current_mapping_index: dict[str, dict[str, str]],
) -> dict[str, object]:
    normalized = normalize_match_text(candidate)
    if not normalized:
        return empty_match("missing_candidate")

    current_match = current_mapping_index.get(normalized)
    if current_match:
        return {
            **current_match,
            "gap_class": "already_mappable",
            "suggested_action": "already_mappable",
            "gap_notes": "matched_current_accepted_mapping",
        }

    for index_name, method in [
        ("canonical", "fooddb_canonical_exact"),
        ("display", "fooddb_display_exact"),
    ]:
        food_row = unique_row(food_indexes[index_name].get(normalized, []))
        if food_row:
            return food_match_result(food_row, "already_mappable", method)

    alias_target = SAFE_ALIAS_RULES.get(normalized) or SELECTED_FAMILY_PROMOTIONS.get(normalized)
    if alias_target:
        alias_row = unique_row(food_indexes["display"].get(normalize_match_text(alias_target), []))
        if alias_row:
            return food_match_result(alias_row, "needs_alias_only", "safe_alias_or_family_promotion")

    canonical_alias_target = COMMON_CANONICAL_ALIASES.get(normalized)
    if canonical_alias_target:
        alias_row = unique_row(food_indexes["canonical"].get(normalize_match_text(canonical_alias_target), []))
        if alias_row:
            return food_match_result(alias_row, "needs_alias_only", "common_canonical_alias")

    stripped = strip_preparation_words(normalized)
    if stripped and stripped != normalized:
        stripped_match = match_existing_only(stripped, food_indexes, current_mapping_index)
        if stripped_match:
            stripped_match["gap_class"] = "needs_alias_only"
            stripped_match["suggested_action"] = "alias_to_existing"
            stripped_match["gap_notes"] = "stripped_preparation_words_match"
            return stripped_match

    singular = singularize_normalized_text(normalized)
    if singular and singular != normalized:
        singular_match = match_existing_only(singular, food_indexes, current_mapping_index)
        if singular_match:
            singular_match["gap_class"] = "needs_alias_only"
            singular_match["suggested_action"] = "alias_to_existing"
            singular_match["gap_notes"] = "singularized_match"
            return singular_match

    family_row = unique_row(food_indexes["family"].get(normalized, []))
    if family_row:
        return food_match_result(family_row, "needs_alias_only", "fooddb_family_exact")

    return empty_match("no_exact_or_alias_match")


def match_existing_only(
    normalized_candidate: str,
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
    current_mapping_index: dict[str, dict[str, str]],
) -> dict[str, object] | None:
    current_match = current_mapping_index.get(normalized_candidate)
    if current_match:
        return {
            **current_match,
            "suggested_action": "alias_to_existing",
        }
    for index_name, method in [
        ("canonical", "fooddb_canonical_exact"),
        ("display", "fooddb_display_exact"),
        ("family", "fooddb_family_exact"),
    ]:
        food_row = unique_row(food_indexes[index_name].get(normalized_candidate, []))
        if food_row:
            return food_match_result(food_row, "needs_alias_only", method)
    return None


def food_match_result(food_row: dict[str, str], gap_class: str, method: str) -> dict[str, object]:
    suggested_action = "already_mappable" if gap_class == "already_mappable" else "alias_to_existing"
    return {
        "gap_class": gap_class,
        "matched_food_id": normalize_space(food_row.get("food_id")),
        "matched_canonical_name": normalize_space(food_row.get("canonical_name")),
        "matched_display_name": normalize_space(food_row.get("display_name")),
        "suggested_action": suggested_action,
        "gap_notes": method,
    }


def empty_match(note: str) -> dict[str, object]:
    return {
        "gap_class": "needs_fooddb_item",
        "matched_food_id": "",
        "matched_canonical_name": "",
        "matched_display_name": "",
        "suggested_action": "add_to_fooddb",
        "gap_notes": note,
    }


def strip_preparation_words(normalized: str) -> str:
    words = [word for word in normalized.split() if word not in PREPARATION_WORDS]
    return " ".join(words).strip()


def parse_ingredient(ingredient_raw: str) -> dict[str, object]:
    working = normalize_for_parse(ingredient_raw)
    lower = normalize_label(working)
    quantity_value = ""
    quantity_unit = ""

    quantity_match = re.match(
        r"^\s*(?P<qty>\d+\s+\d+/\d+|\d+/\d+|\d+(?:\.\d+)?)\s*(?P<rest>.*)$",
        working,
    )
    body = working
    if quantity_match:
        quantity_value = quantity_match.group("qty")
        rest = quantity_match.group("rest").strip()
        unit_match = re.match(r"^(?P<unit>[A-Za-z.-]+)\b(?P<after>.*)$", rest)
        if unit_match:
            raw_unit = normalize_label(unit_match.group("unit").rstrip("."))
            normalized_unit = UNIT_NORMALIZATION.get(raw_unit, "")
            if normalized_unit:
                quantity_unit = normalized_unit
                body = unit_match.group("after").strip()
            else:
                body = rest
        else:
            body = rest

    body = re.sub(r"^\([^)]*\)\s*", "", body)
    body = re.sub(r"\([^)]*(ounce|ounces|oz|pound|lb|gram|kg|count)[^)]*\)", "", body, flags=re.I)
    body = re.sub(r"\bsuch as\b.*$", "", body, flags=re.I).strip()
    body = body.split(",", 1)[0]
    body = re.sub(r"\b(optional|to taste|as needed|as desired|if desired)\b", "", body, flags=re.I)
    body = re.sub(r"\b(divided|for garnish|for serving)\b", "", body, flags=re.I)
    body = normalize_space(body)

    candidate = strip_preparation_words(normalize_match_text(body))
    if not candidate:
        candidate = normalize_match_text(body)

    return {
        "quantity_value": quantity_value,
        "quantity_unit": quantity_unit,
        "food_name_candidate": candidate,
        "ingredient_name_normalized": normalize_slot_key(candidate),
        "has_alternative": " or " in f" {lower} ",
        "is_optional": "optional" in lower,
        "is_packaged": quantity_unit in PACKAGING_UNITS or contains_any(lower, PROCESSED_KEYWORDS),
        "raw_lower": lower,
    }


def normalize_for_parse(text: object) -> str:
    value = normalize_space(text)
    for source, target in FRACTION_MAP.items():
        value = value.replace(source, f" {target} ")
    value = value.replace("â€”", "-").replace("â€“", "-")
    return normalize_space(value)


def classify_recipe_kind(record: dict[str, object]) -> str:
    title_text = normalize_match_text(record["recipe_title"])
    category_text = normalize_match_text(
        " ".join([str(record["category"]), str(record["subcategory"])])
    )
    recipe_text = normalize_match_text(
        " ".join(
            [
                str(record["recipe_title"]),
                str(record["category"]),
                str(record["subcategory"]),
            ]
        )
    )
    broad_text = normalize_match_text(
        " ".join(
            [
                str(record["recipe_title"]),
                str(record["category"]),
                str(record["subcategory"]),
                str(record["description"]),
            ]
        )
    )
    if contains_any(broad_text, OUT_OF_SCOPE_KEYWORDS):
        return "out_of_scope"
    if contains_any(title_text, SAUCE_COMPONENT_KEYWORDS):
        return "sauce_or_component"
    if (
        contains_keyword(recipe_text, "soup")
        or contains_keyword(recipe_text, "stew")
        or contains_keyword(recipe_text, "chili")
    ):
        return "soup"
    if contains_keyword(recipe_text, "salad"):
        return "salad"
    if (
        contains_keyword(recipe_text, "breakfast")
        or contains_keyword(recipe_text, "oatmeal")
        or contains_keyword(recipe_text, "oats")
        or contains_keyword(recipe_text, "omelet")
        or contains_keyword(recipe_text, "pancake")
        or contains_keyword(recipe_text, "pancakes")
    ):
        return "breakfast"
    if (
        contains_keyword(recipe_text, "snack")
        or contains_keyword(recipe_text, "bites")
        or contains_keyword(recipe_text, "appetizer")
    ):
        return "snack"
    if contains_any(category_text, COMPONENT_CATEGORY_KEYWORDS):
        return "sauce_or_component"
    if contains_any(recipe_text, PREFERRED_PROTEINS):
        return "protein_main"
    if contains_any(recipe_text, PREFERRED_CARBS):
        return "carb_side"
    if contains_any(recipe_text, COMMON_VEGETABLES):
        return "veg_side"
    if (
        contains_keyword(recipe_text, "main")
        or contains_keyword(recipe_text, "dinner")
        or contains_keyword(recipe_text, "lunch")
    ):
        return "standalone_main"
    return "standalone_main"


def score_recipe(record: dict[str, object], recipe_kind: str) -> float:
    text = normalize_match_text(
        " ".join(
            [
                str(record["recipe_title"]),
                str(record["category"]),
                str(record["subcategory"]),
                str(record["description"]),
                " ".join(record["ingredients_list"]),
            ]
        )
    )
    score = 3.0
    if recipe_kind in {"standalone_main", "protein_main", "soup", "salad"}:
        score += 2.0
    if recipe_kind in {"breakfast", "snack", "carb_side", "veg_side"}:
        score += 1.0
    if contains_any(text, PREFERRED_PROTEINS):
        score += 1.5
    if contains_any(text, PREFERRED_CARBS):
        score += 1.5
    if contains_any(text, COMMON_VEGETABLES):
        score += 1.0
    if "italian" in text or "mediterranean" in text or "greek" in text or "french" in text:
        score += 1.0
    if int(record["num_ingredients"]) <= 10:
        score += 1.0
    if int(record["num_steps"]) <= 6:
        score += 0.5
    score -= keyword_count(text, HARD_TO_SOURCE_KEYWORDS) * 1.0
    score -= keyword_count(text, PROCESSED_KEYWORDS) * 1.2
    if recipe_kind in {"out_of_scope", "sauce_or_component"}:
        score -= 4.0
    return round(max(0.0, min(10.0, score)), 2)


def ingredient_complexity_score(record: dict[str, object]) -> float:
    ingredients = record["ingredients_list"]
    text = normalize_match_text(" ".join(ingredients))
    score = max(0, int(record["num_ingredients"]) - 5) * 0.55
    score += sum(1 for item in ingredients if " or " in normalize_label(item)) * 0.7
    score += sum(1 for item in ingredients if "optional" in normalize_label(item)) * 0.5
    score += sum(1 for item in ingredients if "such as" in normalize_label(item)) * 0.5
    score += keyword_count(text, HARD_TO_SOURCE_KEYWORDS) * 1.0
    score += keyword_count(text, PROCESSED_KEYWORDS) * 1.0
    return round(max(0.0, min(10.0, score)), 2)


def initial_review_reasons(
    record: dict[str, object],
    recipe_kind: str,
    practicality_score: float,
    complexity_score: float,
    current_source_ids: set[str],
) -> list[str]:
    reasons: list[str] = []
    ingredient_count = int(record["num_ingredients"])
    step_count = int(record["num_steps"])
    max_ingredients = (
        MAX_INGREDIENTS_BREAKFAST_SNACK
        if recipe_kind in {"breakfast", "snack"}
        else MAX_INGREDIENTS_DEFAULT
    )
    if str(record["source_row_number"]) in current_source_ids:
        reasons.append("already_in_current_recipes")
    if recipe_kind == "out_of_scope":
        reasons.append("out_of_scope_recipe_kind")
    if recipe_kind == "sauce_or_component":
        reasons.append("component_only_review")
    if ingredient_count > max_ingredients:
        reasons.append("too_many_ingredients")
    if step_count < MIN_STEPS or step_count > MAX_STEPS:
        reasons.append("step_count_outside_range")
    if practicality_score < KEEP_SCORE_THRESHOLD:
        reasons.append("low_european_practicality_score")
    if complexity_score > 7.0:
        reasons.append("high_ingredient_complexity")
    return reasons


def classify_ingredient_gap(
    parsed: dict[str, object],
    match_result: dict[str, object],
) -> dict[str, object]:
    candidate = str(parsed["food_name_candidate"])
    raw_lower = str(parsed["raw_lower"])
    unit = str(parsed["quantity_unit"])
    role = suggested_role(candidate)

    if parsed["has_alternative"] or len(candidate.split()) > 6:
        match_result["gap_class"] = "too_ambiguous"
        match_result["suggested_action"] = "review"
        match_result["gap_notes"] = f"{match_result['gap_notes']}; ambiguous_or_alternative"
        return {**match_result, "suggested_role": role}

    if contains_any(raw_lower, OUT_OF_SCOPE_KEYWORDS):
        match_result["gap_class"] = "out_of_scope"
        match_result["suggested_action"] = "keep_unmapped"
        match_result["gap_notes"] = f"{match_result['gap_notes']}; ingredient_out_of_scope"
        return {**match_result, "suggested_role": role}

    if unit in PACKAGING_UNITS:
        match_result["gap_class"] = "too_ambiguous"
        match_result["suggested_action"] = "review"
        match_result["gap_notes"] = f"{match_result['gap_notes']}; packaging_unit"
        return {**match_result, "suggested_role": role}

    if (
        match_result["gap_class"] in {"already_mappable", "needs_alias_only"}
        and unit in UNIT_RULE_UNITS
    ):
        match_result["gap_class"] = "needs_unit_to_grams_rule"
        match_result["suggested_action"] = "add_unit_conversion"
        match_result["gap_notes"] = f"{match_result['gap_notes']}; unit_requires_grams_rule"
        return {**match_result, "suggested_role": role}

    return {**match_result, "suggested_role": role}


def suggested_role(normalized_candidate: str) -> str:
    text = normalize_match_text(normalized_candidate)
    if contains_any(text, {"salt", "pepper", "paprika", "cumin", "oregano", "thyme", "basil", "parsley"}):
        return "seasoning"
    if "powder" in text or "seasoning" in text or "spice" in text:
        return "seasoning"
    if contains_any(text, {"oil", "butter", "lard", "margarine"}):
        return "fat_source"
    if contains_any(text, {"milk", "cream", "yogurt", "cheese", "parmesan", "mozzarella"}):
        return "dairy"
    if contains_any(text, PREFERRED_PROTEINS):
        return "protein"
    if contains_any(text, PREFERRED_CARBS):
        return "carb_side"
    if contains_any(text, COMMON_VEGETABLES):
        return "veg_side"
    if contains_any(text, {"apple", "banana", "berry", "berries", "lemon", "lime", "orange"}):
        return "fruit"
    if contains_any(text, {"sauce", "broth", "stock", "vinegar", "mustard", "ketchup"}):
        return "sauce"
    return "other"


def suggested_food_group(role: str) -> str:
    return {
        "protein": "meat, egg and fish",
        "carb_side": "cereal products / starchy foods",
        "veg_side": "vegetables",
        "fat_source": "fats and oils",
        "dairy": "milk and dairy products",
        "fruit": "fruit",
        "seasoning": "seasonings",
        "sauce": "sauces",
    }.get(role, "other")


def mapping_difficulty(gap_counts: Counter) -> str:
    total = sum(gap_counts.values())
    if total == 0:
        return "unknown"
    hard = (
        gap_counts["needs_fooddb_item"]
        + gap_counts["too_ambiguous"]
        + gap_counts["out_of_scope"]
    )
    fixable = gap_counts["needs_alias_only"] + gap_counts["needs_unit_to_grams_rule"]
    hard_ratio = hard / total
    if hard_ratio <= 0.20 and fixable <= 4:
        return "low"
    if hard_ratio <= 0.45:
        return "medium"
    return "high"


def make_recipe_record(source_row_number: int, row: dict[str, str]) -> dict[str, object]:
    ingredients = safe_load_list(row.get("ingredients"))
    directions = safe_load_list(row.get("directions"))
    title = normalize_space(row.get("recipe_title"))
    return {
        "source_row_number": source_row_number,
        "recipe_title": title,
        "category": normalize_space(row.get("category")),
        "subcategory": normalize_space(row.get("subcategory")),
        "description": normalize_space(row.get("description")),
        "ingredients_list": ingredients,
        "directions_list": directions,
        "num_ingredients": int(row.get("num_ingredients") or len(ingredients) or 0),
        "num_steps": int(row.get("num_steps") or len(directions) or 0),
        "signature": build_signature(title, ingredients, directions),
    }


def build_deduped_source_records(source_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    records_by_signature: dict[str, dict[str, object]] = {}
    for source_row_number, row in enumerate(source_rows, start=1):
        record = make_recipe_record(source_row_number, row)
        signature = str(record["signature"])
        if signature not in records_by_signature:
            records_by_signature[signature] = record
    return list(records_by_signature.values())


def audit_ingredients_for_record(
    record: dict[str, object],
    food_indexes: dict[str, dict[str, list[dict[str, str]]]],
    current_mapping_index: dict[str, dict[str, str]],
    keep_for_v1_1: bool,
    recipe_kind: str,
) -> tuple[list[dict[str, object]], Counter]:
    rows: list[dict[str, object]] = []
    counts: Counter = Counter()
    for position, ingredient_raw in enumerate(record["ingredients_list"], start=1):
        parsed = parse_ingredient(ingredient_raw)
        match_result = match_food_candidate(
            str(parsed["food_name_candidate"]),
            food_indexes,
            current_mapping_index,
        )
        gap = classify_ingredient_gap(parsed, match_result)
        gap_class = str(gap["gap_class"])
        counts[gap_class] += 1
        rows.append(
            {
                "source_row_number": record["source_row_number"],
                "recipe_title": record["recipe_title"],
                "recipe_kind_guess": recipe_kind,
                "keep_for_v1_1": keep_for_v1_1,
                "ingredient_position": position,
                "ingredient_raw": ingredient_raw,
                "ingredient_name_normalized": parsed["ingredient_name_normalized"],
                "food_name_candidate": parsed["food_name_candidate"],
                "quantity_value": parsed["quantity_value"],
                "quantity_unit": parsed["quantity_unit"],
                "gap_class": gap_class,
                "matched_food_id": gap["matched_food_id"],
                "matched_canonical_name": gap["matched_canonical_name"],
                "matched_display_name": gap["matched_display_name"],
                "suggested_action": gap["suggested_action"],
                "suggested_role": gap["suggested_role"],
                "gap_notes": gap["gap_notes"],
            }
        )
    return rows, counts


def build_fooddb_gap_candidates(ingredient_gap_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    recipe_sets: dict[str, set[str]] = defaultdict(set)
    example_sets: dict[str, list[str]] = defaultdict(list)

    for row in ingredient_gap_rows:
        if not row.get("keep_for_v1_1"):
            continue
        gap_class = str(row["gap_class"])
        if gap_class in {"already_mappable", "too_ambiguous", "out_of_scope"}:
            continue
        key = str(row["ingredient_name_normalized"])
        if not key:
            continue
        recipe_sets[key].add(str(row["source_row_number"]))
        if len(example_sets[key]) < 4:
            title = str(row["recipe_title"])
            if title not in example_sets[key]:
                example_sets[key].append(title)
        if key not in grouped:
            role = str(row["suggested_role"])
            grouped[key] = {
                "ingredient_name_normalized": key,
                "suggested_canonical_name": suggested_canonical_name(row),
                "suggested_food_group": suggested_food_group(role),
                "suggested_role": role,
                "gap_class": gap_class,
                "matched_food_id": row["matched_food_id"],
                "matched_canonical_name": row["matched_canonical_name"],
                "proposed_action": proposed_action_for_gap(row),
            }

    rows: list[dict[str, object]] = []
    for key, row in grouped.items():
        frequency = len(recipe_sets[key])
        row["frequency_in_candidate_recipes"] = frequency
        row["example_recipes"] = " | ".join(example_sets[key])
        row["priority"] = priority_for_gap(
            frequency=frequency,
            role=str(row["suggested_role"]),
            proposed_action=str(row["proposed_action"]),
        )
        rows.append(row)

    return sorted(
        rows,
        key=lambda item: (
            priority_sort_key(str(item["priority"])),
            -int(item["frequency_in_candidate_recipes"]),
            str(item["ingredient_name_normalized"]),
        ),
    )


def suggested_canonical_name(row: dict[str, object]) -> str:
    matched = normalize_space(row.get("matched_canonical_name"))
    if matched:
        return matched
    return normalize_space(str(row.get("food_name_candidate")).replace("_", " "))


def proposed_action_for_gap(row: dict[str, object]) -> str:
    gap_class = str(row["gap_class"])
    if gap_class == "needs_alias_only":
        return "alias_to_existing"
    if gap_class == "needs_unit_to_grams_rule":
        return "add_unit_conversion"
    if gap_class == "needs_fooddb_item":
        return "add_to_fooddb"
    if gap_class == "too_ambiguous":
        return "review"
    return "keep_unmapped"


def priority_for_gap(frequency: int, role: str, proposed_action: str) -> str:
    high_roles = {"protein", "carb_side", "fat_source", "dairy"}
    if proposed_action == "add_to_fooddb" and role in high_roles and frequency >= 5:
        return "high"
    if proposed_action in {"alias_to_existing", "add_unit_conversion"} and frequency >= 8:
        return "high"
    if frequency >= 4 or role in high_roles:
        return "medium"
    return "low"


def priority_sort_key(priority: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(priority, 3)


def finalize_keep_decision(
    initial_reasons: list[str],
    mapping_level: str,
) -> tuple[bool, str]:
    reasons = list(initial_reasons)
    if mapping_level == "high":
        reasons.append("high_expected_mapping_difficulty")
    keep = not reasons
    if not reasons:
        reasons.append("suitable_for_v1_1_audit")
    return keep, ";".join(dict.fromkeys(reasons))


def main() -> None:
    args = parse_args()
    source_rows = read_csv_rows(Path(args.source_recipes))
    current_recipe_rows = read_csv_rows(Path(args.current_recipes))
    current_ingredient_rows = read_csv_rows(Path(args.current_ingredients))
    food_rows = read_csv_rows(Path(args.fooddb))

    current_source_ids = {
        normalize_space(row.get("source_recipe_id"))
        for row in current_recipe_rows
        if normalize_space(row.get("source_recipe_id"))
    }
    food_indexes = build_food_indexes(food_rows)
    current_mapping_index = build_current_mapping_index(current_ingredient_rows)

    source_records = build_deduped_source_records(source_rows)
    recipe_audit_rows: list[dict[str, object]] = []
    ingredient_gap_rows: list[dict[str, object]] = []

    recipe_kind_counts: Counter = Counter()
    preliminary_candidate_count = 0

    for record in source_records:
        recipe_kind = classify_recipe_kind(record)
        recipe_kind_counts[recipe_kind] += 1
        practicality_score = score_recipe(record, recipe_kind)
        complexity_score = ingredient_complexity_score(record)
        initial_reasons = initial_review_reasons(
            record,
            recipe_kind,
            practicality_score,
            complexity_score,
            current_source_ids,
        )
        should_audit_ingredients = (
            practicality_score >= REVIEW_SCORE_THRESHOLD
            and recipe_kind != "out_of_scope"
            and "already_in_current_recipes" not in initial_reasons
        )
        if should_audit_ingredients:
            preliminary_candidate_count += 1
            gap_rows, gap_counts = audit_ingredients_for_record(
                record,
                food_indexes,
                current_mapping_index,
                keep_for_v1_1=False,
                recipe_kind=recipe_kind,
            )
        else:
            gap_rows = []
            gap_counts = Counter()

        expected_difficulty = mapping_difficulty(gap_counts)
        keep_for_v1_1, review_reason = finalize_keep_decision(
            initial_reasons,
            expected_difficulty,
        )

        for gap_row in gap_rows:
            gap_row["keep_for_v1_1"] = keep_for_v1_1
        ingredient_gap_rows.extend(gap_rows)

        recipe_audit_rows.append(
            {
                "source_row_number": record["source_row_number"],
                "recipe_title": record["recipe_title"],
                "category": record["category"],
                "subcategory": record["subcategory"],
                "num_ingredients": record["num_ingredients"],
                "num_steps": record["num_steps"],
                "already_in_current": str(record["source_row_number"]) in current_source_ids,
                "keep_for_v1_1": keep_for_v1_1,
                "review_reason": review_reason,
                "recipe_kind_guess": recipe_kind,
                "european_practicality_score": practicality_score,
                "ingredient_complexity_score": complexity_score,
                "expected_mapping_difficulty": expected_difficulty,
                "ingredient_count_parsed": sum(gap_counts.values()),
                "already_mappable_count": gap_counts["already_mappable"],
                "needs_alias_only_count": gap_counts["needs_alias_only"],
                "needs_fooddb_item_count": gap_counts["needs_fooddb_item"],
                "needs_unit_to_grams_rule_count": gap_counts["needs_unit_to_grams_rule"],
                "too_ambiguous_count": gap_counts["too_ambiguous"],
                "out_of_scope_ingredient_count": gap_counts["out_of_scope"],
            }
        )

    recipe_audit_rows.sort(
        key=lambda row: (
            not bool(row["keep_for_v1_1"]),
            -float(row["european_practicality_score"]),
            float(row["ingredient_complexity_score"]),
            int(row["source_row_number"]),
        )
    )
    fooddb_gap_rows = build_fooddb_gap_candidates(ingredient_gap_rows)

    write_csv(Path(args.out_recipe_audit), recipe_audit_rows, RECIPE_AUDIT_COLUMNS)
    write_csv(Path(args.out_ingredient_gap), ingredient_gap_rows, INGREDIENT_GAP_COLUMNS)
    write_csv(Path(args.out_fooddb_gap), fooddb_gap_rows, FOODDB_GAP_COLUMNS)
    write_summary(
        Path(args.out_summary),
        source_rows_scanned=len(source_rows),
        deduped_recipes_scanned=len(source_records),
        preliminary_candidate_count=preliminary_candidate_count,
        recipe_audit_rows=recipe_audit_rows,
        ingredient_gap_rows=ingredient_gap_rows,
        fooddb_gap_rows=fooddb_gap_rows,
        recipe_kind_counts=recipe_kind_counts,
    )

    print(f"Source rows scanned: {len(source_rows)}")
    print(f"Deduped recipes scanned: {len(source_records)}")
    print(f"Suitable for v1.1: {sum(1 for row in recipe_audit_rows if row['keep_for_v1_1'])}")
    print(f"Written: {args.out_recipe_audit}")
    print(f"Written: {args.out_summary}")
    print(f"Written: {args.out_ingredient_gap}")
    print(f"Written: {args.out_fooddb_gap}")


def write_summary(
    path: Path,
    source_rows_scanned: int,
    deduped_recipes_scanned: int,
    preliminary_candidate_count: int,
    recipe_audit_rows: list[dict[str, object]],
    ingredient_gap_rows: list[dict[str, object]],
    fooddb_gap_rows: list[dict[str, object]],
    recipe_kind_counts: Counter,
) -> None:
    keep_rows = [row for row in recipe_audit_rows if row["keep_for_v1_1"]]
    keep_kind_counts = Counter(str(row["recipe_kind_guess"]) for row in keep_rows)
    gap_class_counts = Counter(str(row["gap_class"]) for row in ingredient_gap_rows if row["keep_for_v1_1"])

    missing_rows = [
        row for row in fooddb_gap_rows if row["proposed_action"] == "add_to_fooddb"
    ][:20]
    alias_rows = [
        row for row in fooddb_gap_rows if row["proposed_action"] == "alias_to_existing"
    ][:20]
    unit_rows = [
        row for row in fooddb_gap_rows if row["proposed_action"] == "add_unit_conversion"
    ][:20]

    lines = [
        "Recipes_DB v1.1 / Food_DB gap-driven expansion audit",
        "",
        f"Source rows scanned: {source_rows_scanned}",
        f"Deduped recipes scanned: {deduped_recipes_scanned}",
        f"Preliminary candidate recipes audited for ingredient gaps: {preliminary_candidate_count}",
        f"Recipes suitable for v1.1: {len(keep_rows)}",
        "",
        "Top recipe categories among suitable recipes:",
    ]
    lines.extend(format_counter_lines(keep_kind_counts, 12))
    lines.extend(["", "Top recipe categories across deduped source:"])
    lines.extend(format_counter_lines(recipe_kind_counts, 12))
    lines.extend(["", "Ingredient gap classes among suitable recipes:"])
    lines.extend(format_counter_lines(gap_class_counts, 12))
    lines.extend(["", "Top missing ingredients needing Food_DB items:"])
    lines.extend(format_gap_lines(missing_rows))
    lines.extend(["", "Top alias opportunities:"])
    lines.extend(format_gap_lines(alias_rows))
    lines.extend(["", "Top unit-to-grams rule opportunities:"])
    lines.extend(format_gap_lines(unit_rows))
    lines.extend(
        [
            "",
            "Recommended next patch:",
            "- review the high-priority gap candidates first",
            "- add only canonical Food_DB ingredients that appear frequently in suitable recipes",
            "- add alias rules separately from Food_DB additions",
            "- add unit-to-grams rules for high-frequency mapped ingredients before materializing v1.1",
            "- do not generate production Recipes_DB v1.1 tables until this audit is manually reviewed",
        ]
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def format_counter_lines(counter: Counter, limit: int) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key}: {count}" for key, count in counter.most_common(limit)]


def format_gap_lines(rows: list[dict[str, object]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        (
            f"- {row['ingredient_name_normalized']} "
            f"({row['proposed_action']}, {row['priority']}, "
            f"freq={row['frequency_in_candidate_recipes']}): "
            f"{row['suggested_canonical_name']}"
        )
        for row in rows
    ]


if __name__ == "__main__":
    main()
