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
DEFAULT_FOODDB = Path("data/fooddb/current/fooddb_v1_core_master_draft.csv")

DEFAULT_CURATED_OUT = Path("data/recipesdb/draft/recipes_v1_1_curated_200.csv")
DEFAULT_SUMMARY_OUT = Path("data/recipesdb/audit/recipes_v1_1_curated_200_summary.txt")
DEFAULT_EXCLUSION_OUT = Path(
    "data/recipesdb/audit/recipes_v1_1_curated_200_exclusion_log.csv"
)
DEFAULT_CATEGORY_MIX_OUT = Path(
    "data/recipesdb/audit/recipes_v1_1_curated_200_category_mix.csv"
)
DEFAULT_REVIEW_AUDIT_OUT = Path(
    "data/recipesdb/audit/recipes_v1_1_curated_200_review_audit.csv"
)
DEFAULT_REVIEW_SUMMARY_OUT = Path(
    "data/recipesdb/audit/recipes_v1_1_curated_200_review_summary.txt"
)
DEFAULT_REPLACEMENT_LOG_OUT = Path(
    "data/recipesdb/audit/recipes_v1_1_curated_200_replacement_log.csv"
)

TARGET_TOTAL = 200
TARGET_MAIN_MEALS = 155
TARGET_BREAKFAST = 20
TARGET_SNACK = 10
TARGET_COMPONENTS = 15

MIN_STEPS = 1
MAX_STEPS_MAIN = 9
MAX_STEPS_OTHER = 8
MAX_INGREDIENTS_MAIN = 16
MAX_INGREDIENTS_BREAKFAST = 11
MAX_INGREDIENTS_SNACK = 8
MAX_INGREDIENTS_COMPONENT = 10

OUTPUT_COLUMNS = [
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "source_category",
    "source_subcategory",
    "recipe_kind_guess",
    "primary_protein",
    "protein_hits_detected",
    "has_carb_component",
    "has_veg_component",
    "is_complete_or_near_complete_meal",
    "is_component_only",
    "european_practicality_score",
    "completeness_score",
    "ingredient_complexity_score",
    "expected_mapping_difficulty",
    "selection_reason",
    "num_ingredients",
    "num_steps",
    "ingredients_json",
    "directions_json",
]

EXCLUSION_COLUMNS = [
    "source_index",
    "display_name",
    "source_category",
    "source_subcategory",
    "recipe_kind_guess",
    "primary_protein",
    "has_carb_component",
    "has_veg_component",
    "is_complete_or_near_complete_meal",
    "is_component_only",
    "european_practicality_score",
    "completeness_score",
    "ingredient_complexity_score",
    "expected_mapping_difficulty",
    "exclusion_reason",
]

CATEGORY_MIX_COLUMNS = ["metric_group", "name", "count"]

REVIEW_AUDIT_COLUMNS = [
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "source_category",
    "source_subcategory",
    "recipe_kind_guess",
    "primary_protein",
    "protein_hits_detected",
    "review_status",
    "review_reason",
    "is_weird_or_random",
    "is_too_american_processed",
    "is_complete_meal_really",
    "has_clear_protein",
    "has_clear_carb_or_veg",
    "likely_mapping_risk",
    "title_quality_score",
    "selection_reason",
    "num_ingredients",
    "num_steps",
]

REPLACEMENT_LOG_COLUMNS = [
    "removed_title",
    "removed_reason",
    "replacement_title",
    "replacement_recipe_kind_guess",
    "replacement_primary_protein",
    "replacement_reason",
]

MANUAL_REPLACEMENT_REASONS = {
    "Hawaiian Garlic Shrimp Scampi": "replace_review: weird/random fusion signal",
    "Hawaiian Loco Moco": "replace_review: weird/random fusion signal",
    "Egg and Cheese Breakfast Biscuit Bombs": "replace_review: too American/processed-style breakfast",
    "Szechuan Edamame (Soy Beans)": "replace_review: hard-to-source/random snack signal",
    "French Toast II": "review_decision_replace: near-duplicate breakfast cluster",
    "Ultimate French Toast": "review_decision_replace: near-duplicate breakfast cluster",
}

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
}

DIRECT_WEIGHT_UNITS = {"gram", "kilogram", "ounce", "pound"}
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
}
PACKAGING_UNITS = {"can", "package", "jar", "bottle", "packet"}

FRACTION_MAP = {
    "Â¼": "1/4",
    "Â½": "1/2",
    "Â¾": "3/4",
    "Ã‚Â¼": "1/4",
    "Ã‚Â½": "1/2",
    "Ã‚Â¾": "3/4",
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

ANIMAL_PROTEIN_KEYWORDS = {
    "chicken": {"chicken", "drumstick", "drumsticks", "chicken breast", "chicken thigh"},
    "turkey": {"turkey", "turkey breast", "ground turkey"},
    "fish": {
        "fish",
        "salmon",
        "tuna",
        "cod",
        "trout",
        "halibut",
        "tilapia",
        "snapper",
        "catfish",
        "shrimp",
        "prawn",
        "prawns",
        "seafood",
    },
    "beef": {
        "beef",
        "steak",
        "ground beef",
        "short ribs",
        "beef ribs",
        "brisket",
        "meatloaf",
    },
    "pork": {
        "pork",
        "ham",
        "bacon",
        "sausage",
        "chorizo",
        "prosciutto",
        "pancetta",
    },
    "egg": {"egg", "eggs", "omelet", "omelette", "frittata"},
}

PLANT_PROTEIN_KEYWORDS = {
    "lentil",
    "lentils",
    "bean",
    "beans",
    "chickpea",
    "chickpeas",
    "tofu",
    "tempeh",
    "hummus",
    "edamame",
    "peas",
}

CARB_KEYWORDS = {
    "rice",
    "potato",
    "potatoes",
    "pasta",
    "spaghetti",
    "noodle",
    "noodles",
    "macaroni",
    "penne",
    "tortilla",
    "tortillas",
    "bread",
    "bun",
    "buns",
    "biscuit",
    "biscuits",
    "toast",
    "waffle",
    "waffles",
    "oats",
    "oatmeal",
    "barley",
    "couscous",
    "polenta",
    "quinoa",
    "farro",
    "bulgur",
    "gnocchi",
    "beans",
    "lentils",
    "chickpeas",
    "corn",
}

VEG_COMPONENT_KEYWORDS = {
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
    "asparagus",
    "squash",
    "pumpkin",
    "celery",
    "leek",
    "leeks",
    "green beans",
    "green bean",
    "beets",
    "beet",
    "okra",
    "radish",
    "radishes",
    "salad",
}

AROMATIC_VEG_KEYWORDS = {"onion", "onions", "garlic", "shallot", "shallots"}

BREAKFAST_KEYWORDS = {
    "breakfast",
    "oatmeal",
    "oats",
    "omelet",
    "omelette",
    "frittata",
    "pancake",
    "pancakes",
    "scrambled eggs",
}

SNACK_KEYWORDS = {
    "snack",
    "snacks",
    "appetizer",
    "appetizers",
    "hummus",
    "nuts",
    "peanuts",
    "almonds",
    "pumpkin seeds",
    "seeds",
}

SOUP_KEYWORDS = {"soup", "stew", "chili", "chowder"}
SALAD_KEYWORDS = {"salad"}
BOWL_MAIN_KEYWORDS = {"bowl", "casserole", "stir fry", "stir-fry", "lasagna", "taco", "tacos"}

COMPONENT_KEYWORDS = {
    "sauce",
    "dressing",
    "dip",
    "salsa",
    "marinade",
    "brine",
    "rub",
    "seasoning",
    "spread",
    "relish",
    "chutney",
    "syrup",
}

SIDE_CATEGORY_KEYWORDS = {
    "side dish",
    "side dishes",
    "potato side",
    "rice side",
    "vegetable side",
    "vegetarian side",
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
    "muffin",
    "muffins",
    "frosting",
    "icing",
    "candy",
    "cocktail",
    "martini",
    "mojito",
    "smoothie",
    "shake",
    "milkshake",
    "liqueur",
    "jam",
    "jelly",
    "lemonade",
    "orangeade",
    "tea",
    "cider",
    "drink",
    "drinks",
    "ice cream",
    "sorbet",
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
    "apple butter",
    "fruit butter",
    "venison",
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
    "processed cheese",
    "instant ramen",
    "johnsonville",
}

HARD_TO_SOURCE_KEYWORDS = {
    "gochujang",
    "tamarind",
    "yuzu",
    "pandan",
    "annatto",
    "masa harina",
    "jicama",
    "jackfruit",
    "plantain",
    "venison",
    "veal",
    "wasabi",
    "furikake",
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
    "black pepper": "black_pepper_powder",
    "garlic powder": "garlic_powder_dried",
    "soy sauce": "soy_sauce_prepacked",
    "balsamic vinegar": "vinegar_balsamic",
}

PROTEIN_SOFT_CAPS = {
    "chicken": 45,
    "turkey": 22,
    "fish": 35,
    "beef": 35,
    "pork": 30,
    "egg": 12,
    "vegetarian": 24,
    "unknown": 6,
}

KEYWORD_NORMALIZATION_CACHE: dict[str, str] = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Selectie draft Recipes_DB v1.1 curated_200, fara materializare finala."
    )
    parser.add_argument("--source-recipes", default=str(DEFAULT_SOURCE_RECIPES))
    parser.add_argument("--current-recipes", default=str(DEFAULT_CURRENT_RECIPES))
    parser.add_argument("--fooddb", default=str(DEFAULT_FOODDB))
    parser.add_argument("--out-curated", default=str(DEFAULT_CURATED_OUT))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY_OUT))
    parser.add_argument("--out-exclusion-log", default=str(DEFAULT_EXCLUSION_OUT))
    parser.add_argument("--out-category-mix", default=str(DEFAULT_CATEGORY_MIX_OUT))
    parser.add_argument("--out-review-audit", default=str(DEFAULT_REVIEW_AUDIT_OUT))
    parser.add_argument("--out-review-summary", default=str(DEFAULT_REVIEW_SUMMARY_OUT))
    parser.add_argument("--out-replacement-log", default=str(DEFAULT_REPLACEMENT_LOG_OUT))
    parser.add_argument("--target-total", type=int, default=TARGET_TOTAL)
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


def replacement_title_keys() -> set[str]:
    return {normalize_match_text(title) for title in MANUAL_REPLACEMENT_REASONS}


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


def contains_any(text: str, keywords: set[str]) -> bool:
    return any(contains_keyword(text, keyword) for keyword in keywords)


def keyword_count(text: str, keywords: set[str]) -> int:
    return sum(1 for keyword in keywords if contains_keyword(text, keyword))


def safe_load_list(raw_value: object) -> list[str]:
    try:
        value = json.loads(str(raw_value or ""))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def build_signature(title: str, ingredients: list[str], directions: list[str]) -> str:
    payload = "||".join(
        [
            normalize_label(title),
            " || ".join(normalize_label(item) for item in ingredients),
            " || ".join(normalize_label(item) for item in directions),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def normalize_for_parse(text: object) -> str:
    value = normalize_space(text)
    for source, target in FRACTION_MAP.items():
        value = value.replace(source, f" {target} ")
    value = value.replace("Ã¢â‚¬â€", "-").replace("Ã¢â‚¬â€œ", "-")
    return normalize_space(value)


def strip_preparation_words(normalized: str) -> str:
    words = [word for word in normalized.split() if word not in PREPARATION_WORDS]
    return " ".join(words).strip()


def parse_ingredient(ingredient_raw: str) -> dict[str, object]:
    working = normalize_for_parse(ingredient_raw)
    raw_lower = normalize_match_text(working)
    quantity_unit = ""
    body = working
    quantity_match = re.match(
        r"^\s*(?:\d+\s+\d+/\d+|\d+/\d+|\d+(?:\.\d+)?)\s*(?P<rest>.*)$",
        working,
    )
    if quantity_match:
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
    body = re.sub(
        r"\([^)]*(ounce|ounces|oz|pound|lb|gram|kg|count)[^)]*\)",
        "",
        body,
        flags=re.I,
    )
    body = re.sub(r"\bsuch as\b.*$", "", body, flags=re.I).strip()
    prep_pattern = "|".join(sorted(PREPARATION_WORDS, key=len, reverse=True))
    body = re.sub(rf"\b({prep_pattern})\b,?\s*", " ", body, flags=re.I)
    body = body.split(",", 1)[0]
    body = re.sub(r"\b(optional|to taste|as needed|as desired|if desired)\b", "", body, flags=re.I)
    body = re.sub(r"\b(divided|for garnish|for serving)\b", "", body, flags=re.I)
    body = normalize_space(body)

    candidate = strip_preparation_words(normalize_match_text(body))
    if not candidate:
        candidate = normalize_match_text(body)

    return {
        "raw": ingredient_raw,
        "raw_lower": raw_lower,
        "quantity_unit": quantity_unit,
        "candidate": candidate,
        "candidate_key": normalize_slot_key(candidate),
        "has_alternative": " or " in f" {raw_lower} ",
        "is_optional": "optional" in raw_lower,
        "is_packaged": quantity_unit in PACKAGING_UNITS or contains_any(raw_lower, PROCESSED_KEYWORDS),
    }


def build_food_index(food_rows: list[dict[str, str]]) -> set[str]:
    index: set[str] = set()
    for row in food_rows:
        for column in ["canonical_name", "display_name", "food_family_name"]:
            value = normalize_match_text(row.get(column))
            if value:
                index.add(value)
                index.add(normalize_slot_key(value))
    return index


def build_source_record(source_index: int, row: dict[str, str]) -> dict[str, object]:
    ingredients = safe_load_list(row.get("ingredients"))
    directions = safe_load_list(row.get("directions"))
    title = normalize_space(row.get("recipe_title"))
    category = normalize_space(row.get("category"))
    subcategory = normalize_space(row.get("subcategory"))
    description = normalize_space(row.get("description"))
    parsed_ingredients = [parse_ingredient(item) for item in ingredients]

    return {
        "source_index": source_index,
        "signature": build_signature(title, ingredients, directions),
        "display_name": title,
        "source_category": category,
        "source_subcategory": subcategory,
        "description": description,
        "ingredients": ingredients,
        "directions": directions,
        "ingredients_json": json.dumps(ingredients, ensure_ascii=False),
        "directions_json": json.dumps(directions, ensure_ascii=False),
        "num_ingredients": safe_int(row.get("num_ingredients"), len(ingredients)),
        "num_steps": safe_int(row.get("num_steps"), len(directions)),
        "parsed_ingredients": parsed_ingredients,
        "core_text": normalize_match_text(" ".join([title, category, subcategory])),
        "full_text": normalize_match_text(
            " ".join([title, category, subcategory, description, " ".join(ingredients)])
        ),
        "ingredient_text": normalize_match_text(" ".join(ingredients)),
        "has_brand_marker": has_brand_marker(" ".join([title, description, " ".join(ingredients)])),
    }


def has_brand_marker(text: object) -> bool:
    value = str(text or "")
    if "\u00ae" in value or "\u2122" in value:
        return True
    return contains_any(normalize_match_text(value), {"brand name", "brand-name", "johnsonville"})


def representative_score(record: dict[str, object]) -> float:
    core_text = str(record["core_text"])
    score = 0.0
    if contains_any(core_text, {"main dish", "main dishes", "dinner", "lunch"}):
        score += 5.0
    if contains_any(core_text, SOUP_KEYWORDS | SALAD_KEYWORDS):
        score += 3.0
    if contains_any(core_text, BREAKFAST_KEYWORDS):
        score += 2.0
    if contains_any(core_text, OUT_OF_SCOPE_KEYWORDS):
        score -= 8.0
    score -= int(record["num_ingredients"]) * 0.02
    score -= int(record["source_index"]) / 1_000_000
    return score


def dedupe_source_records(source_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    for source_index, row in enumerate(source_rows, start=1):
        record = build_source_record(source_index, row)
        signature = str(record["signature"])
        if signature not in grouped or representative_score(record) > representative_score(grouped[signature]):
            grouped[signature] = record
    return list(grouped.values())


def detect_primary_protein(record: dict[str, object]) -> str:
    core_text = str(record["core_text"])
    ingredient_candidates = [
        str(item["candidate"]) for item in record["parsed_ingredients"] if str(item["candidate"])
    ]
    combined_ingredient_text = " ".join(ingredient_candidates)
    combined_text = " ".join([core_text, combined_ingredient_text])

    for protein in ["chicken", "turkey", "fish", "beef", "pork", "egg"]:
        if contains_any(core_text, ANIMAL_PROTEIN_KEYWORDS[protein]):
            return protein
    for protein in ["chicken", "turkey", "fish", "beef", "pork", "egg"]:
        if ingredient_has_primary_protein(ingredient_candidates, protein):
            return protein
    if contains_any(combined_text, PLANT_PROTEIN_KEYWORDS) or contains_any(
        core_text, {"vegetarian", "vegan"}
    ):
        return "vegetarian"
    return "unknown"


def detect_protein_hits(record: dict[str, object]) -> list[str]:
    core_text = str(record["core_text"])
    ingredient_candidates = [
        str(item["candidate"]) for item in record["parsed_ingredients"] if str(item["candidate"])
    ]
    combined_ingredient_text = " ".join(ingredient_candidates)
    hits: list[str] = []
    for protein in ["chicken", "turkey", "fish", "beef", "pork", "egg"]:
        if contains_any(core_text, ANIMAL_PROTEIN_KEYWORDS[protein]) or ingredient_has_primary_protein(
            ingredient_candidates,
            protein,
        ):
            hits.append(protein)
    if not hits and (
        contains_any(combined_ingredient_text, PLANT_PROTEIN_KEYWORDS)
        or contains_any(core_text, {"vegetarian", "vegan"})
    ):
        hits.append("vegetarian")
    return hits


def ingredient_has_primary_protein(ingredient_candidates: list[str], protein: str) -> bool:
    excluded_context = {"broth", "stock", "bouillon", "seasoning", "sauce", "gravy"}
    for candidate in ingredient_candidates:
        candidate_text = normalize_match_text(candidate)
        if contains_any(candidate_text, excluded_context):
            continue
        if contains_any(candidate_text, ANIMAL_PROTEIN_KEYWORDS[protein]):
            return True
    return False


def detect_component_flags(record: dict[str, object]) -> tuple[bool, bool]:
    ingredient_candidates = [
        str(item["candidate"]) for item in record["parsed_ingredients"] if str(item["candidate"])
    ]
    carb_candidates = [
        candidate
        for candidate in ingredient_candidates
        if not contains_any(
            normalize_match_text(candidate),
            {"bread crumb", "bread crumbs", "breadcrumbs", "panko"},
        )
    ]
    veg_candidates = [
        candidate
        for candidate in ingredient_candidates
        if not contains_any(
            normalize_match_text(candidate),
            {
                "black pepper",
                "ground black pepper",
                "freshly black pepper",
                "white pepper",
                "cayenne pepper",
                "salt and pepper",
            },
        )
    ]
    core_text = str(record["core_text"])
    carb_text = " ".join([core_text, " ".join(carb_candidates)])
    veg_text = " ".join([core_text, " ".join(veg_candidates)])
    has_carb = contains_any(carb_text, CARB_KEYWORDS)
    has_veg = contains_any(veg_text, VEG_COMPONENT_KEYWORDS)
    return has_carb, has_veg


def is_out_of_scope(record: dict[str, object]) -> bool:
    full_text = str(record["full_text"])
    core_text = str(record["core_text"])
    if contains_any(full_text, OUT_OF_SCOPE_KEYWORDS):
        return True
    if contains_any(full_text, PROCESSED_KEYWORDS):
        return True
    if bool(record.get("has_brand_marker")):
        return True
    if contains_any(core_text, {"cocktails", "drinks", "desserts", "jams", "jellies"}):
        return True
    return False


def ingredient_complexity_score(record: dict[str, object]) -> float:
    ingredients = record["parsed_ingredients"]
    full_text = str(record["full_text"])
    score = max(0, int(record["num_ingredients"]) - 7) * 0.45
    score += sum(1 for item in ingredients if item["has_alternative"]) * 0.8
    score += sum(1 for item in ingredients if item["is_optional"]) * 0.5
    score += sum(1 for item in ingredients if item["is_packaged"]) * 0.9
    score += sum(1 for item in ingredients if "such as" in str(item["raw_lower"])) * 0.8
    score += sum(1 for item in ingredients if str(item["raw"]).strip().endswith(":")) * 1.0
    score += keyword_count(full_text, HARD_TO_SOURCE_KEYWORDS) * 1.0
    score += keyword_count(full_text, PROCESSED_KEYWORDS) * 1.2
    return round(max(0.0, min(10.0, score)), 2)


def completeness_score(
    record: dict[str, object],
    primary_protein: str,
    has_carb_component: bool,
    has_veg_component: bool,
) -> float:
    core_text = str(record["core_text"])
    score = 0.0
    if primary_protein != "unknown":
        score += 3.0
    if has_carb_component:
        score += 2.0
    if has_veg_component:
        score += 2.0
    if contains_any(core_text, SOUP_KEYWORDS | BOWL_MAIN_KEYWORDS):
        score += 1.4
    if contains_any(core_text, {"main dish", "main dishes", "dinner", "lunch"}):
        score += 1.0
    if primary_protein != "unknown" and not has_carb_component and not has_veg_component:
        score -= 2.2
    if primary_protein == "unknown" and not has_carb_component:
        score -= 1.0
    if int(record["num_ingredients"]) < 4:
        score -= 0.6
    return round(max(0.0, min(10.0, score)), 2)


def european_practicality_score(
    record: dict[str, object],
    primary_protein: str,
    has_carb_component: bool,
    has_veg_component: bool,
    complete_score: float,
    complexity_score: float,
) -> float:
    core_text = str(record["core_text"])
    full_text = str(record["full_text"])
    score = 4.0
    if primary_protein in {"chicken", "turkey", "fish", "beef", "pork"}:
        score += 1.5
    elif primary_protein in {"egg", "vegetarian"}:
        score += 0.8
    if has_carb_component:
        score += 0.9
    if has_veg_component:
        score += 0.9
    if complete_score >= 7.0:
        score += 1.3
    elif complete_score >= 5.0:
        score += 0.6
    if contains_any(core_text, {"italian", "mediterranean", "greek", "french", "spanish"}):
        score += 0.6
    if 5 <= int(record["num_ingredients"]) <= 12:
        score += 0.5
    if MIN_STEPS <= int(record["num_steps"]) <= 7:
        score += 0.4
    score -= complexity_score * 0.35
    score -= keyword_count(full_text, HARD_TO_SOURCE_KEYWORDS) * 0.8
    score -= keyword_count(full_text, PROCESSED_KEYWORDS) * 1.0
    return round(max(0.0, min(10.0, score)), 2)


def classify_recipe_kind(
    record: dict[str, object],
    primary_protein: str,
    has_carb_component: bool,
    has_veg_component: bool,
    complete_score: float,
) -> tuple[str, bool, bool]:
    core_text = str(record["core_text"])
    is_complete_meal = False
    is_component_only = False

    if contains_any(core_text, BREAKFAST_KEYWORDS):
        return "breakfast", False, False

    if (
        contains_any(core_text, SNACK_KEYWORDS)
        and int(record["num_ingredients"]) <= MAX_INGREDIENTS_SNACK
        and primary_protein not in {"chicken", "turkey", "fish", "beef", "pork"}
    ):
        return "snack", False, False

    if is_component_only_title(core_text) or (
        contains_any(core_text, COMPONENT_KEYWORDS) and primary_protein == "unknown"
    ):
        return "component", False, True

    if contains_any(core_text, SOUP_KEYWORDS):
        is_complete_meal = complete_score >= 5.0 or (
            primary_protein == "vegetarian" and (has_carb_component or has_veg_component)
        )
        return "soup", is_complete_meal, not is_complete_meal

    if contains_any(core_text, SALAD_KEYWORDS):
        is_complete_meal = primary_protein != "unknown" and (has_carb_component or has_veg_component)
        if is_complete_meal:
            return "salad", True, False
        return "veg_side", False, True

    if primary_protein != "unknown":
        if has_carb_component and has_veg_component:
            return "complete_main", True, False
        if has_carb_component or has_veg_component:
            return "near_complete_main", True, False
        return "protein_component", False, True

    if contains_any(core_text, SIDE_CATEGORY_KEYWORDS):
        if has_carb_component:
            return "carb_side", False, True
        if has_veg_component:
            return "veg_side", False, True

    if has_carb_component and has_veg_component and contains_any(core_text, {"main", "dinner", "lunch", "vegetarian"}):
        return "near_complete_main", True, False
    if has_carb_component:
        return "carb_side", False, True
    if has_veg_component:
        return "veg_side", False, True
    return "component", False, True


def is_component_only_title(core_text: str) -> bool:
    component_endings = {
        "sauce",
        "marinade",
        "rub",
        "dip",
        "dressing",
        "salsa",
        "spread",
        "relish",
        "chutney",
        "syrup",
    }
    words = core_text.split()
    if not words:
        return False
    if words[-1] in component_endings:
        return True
    if contains_any(core_text, {"sauce and marinade", "rubbed and baked"}):
        return True
    if contains_any(core_text, {"marinade", "rub", "dip", "dressing"}) and not contains_keyword(core_text, "with"):
        return True
    return False


def classify_mapping_need(parsed: dict[str, object], food_index: set[str]) -> str:
    candidate = str(parsed["candidate"])
    candidate_key = str(parsed["candidate_key"])
    unit = str(parsed["quantity_unit"])
    if not candidate:
        return "too_ambiguous"
    if parsed["has_alternative"] or len(candidate.split()) > 6:
        return "too_ambiguous"
    if parsed["is_packaged"] or unit in PACKAGING_UNITS:
        return "too_ambiguous"
    if candidate in COMMON_CANONICAL_ALIASES or candidate_key in COMMON_CANONICAL_ALIASES.values():
        if unit in UNIT_RULE_UNITS:
            return "needs_unit_to_grams_rule"
        return "needs_alias_only"
    if candidate in food_index or candidate_key in food_index:
        if unit in UNIT_RULE_UNITS:
            return "needs_unit_to_grams_rule"
        return "already_mappable"
    if unit in DIRECT_WEIGHT_UNITS:
        return "needs_fooddb_item"
    return "needs_fooddb_item"


def expected_mapping_difficulty(parsed_ingredients: list[dict[str, object]], food_index: set[str]) -> tuple[str, Counter]:
    counts: Counter = Counter()
    for parsed in parsed_ingredients:
        counts[classify_mapping_need(parsed, food_index)] += 1
    total = sum(counts.values())
    if total == 0:
        return "unknown", counts
    hard = counts["needs_fooddb_item"] + counts["too_ambiguous"]
    hard_ratio = hard / total
    if hard_ratio <= 0.30 and counts["too_ambiguous"] <= 1:
        return "low", counts
    if hard_ratio <= 0.55 and counts["too_ambiguous"] <= 3:
        return "medium", counts
    return "high", counts


def analyze_record(
    record: dict[str, object],
    food_index: set[str],
    current_source_ids: set[str],
) -> dict[str, object]:
    primary_protein = detect_primary_protein(record)
    protein_hits = detect_protein_hits(record)
    has_carb_component, has_veg_component = detect_component_flags(record)
    complexity = ingredient_complexity_score(record)
    complete_score = completeness_score(
        record,
        primary_protein,
        has_carb_component,
        has_veg_component,
    )
    practicality = european_practicality_score(
        record,
        primary_protein,
        has_carb_component,
        has_veg_component,
        complete_score,
        complexity,
    )
    recipe_kind, is_complete_meal, is_component_only = classify_recipe_kind(
        record,
        primary_protein,
        has_carb_component,
        has_veg_component,
        complete_score,
    )
    mapping_level, mapping_counts = expected_mapping_difficulty(
        list(record["parsed_ingredients"]),
        food_index,
    )
    exclusion_reasons = base_exclusion_reasons(
        record,
        recipe_kind,
        is_complete_meal,
        is_component_only,
        practicality,
        complexity,
        mapping_level,
        primary_protein,
        has_carb_component,
        has_veg_component,
        current_source_ids,
    )
    selection_score = compute_selection_score(
        recipe_kind,
        primary_protein,
        is_complete_meal,
        is_component_only,
        practicality,
        complete_score,
        complexity,
        mapping_level,
        has_carb_component,
        has_veg_component,
    )
    return {
        **record,
        "primary_protein": primary_protein,
        "protein_hits_detected": "|".join(protein_hits),
        "has_carb_component": has_carb_component,
        "has_veg_component": has_veg_component,
        "recipe_kind_guess": recipe_kind,
        "is_complete_or_near_complete_meal": is_complete_meal,
        "is_component_only": is_component_only,
        "european_practicality_score": practicality,
        "completeness_score": complete_score,
        "ingredient_complexity_score": complexity,
        "expected_mapping_difficulty": mapping_level,
        "mapping_counts": mapping_counts,
        "exclusion_reasons": exclusion_reasons,
        "selection_score": selection_score,
    }


def base_exclusion_reasons(
    record: dict[str, object],
    recipe_kind: str,
    is_complete_meal: bool,
    is_component_only: bool,
    practicality: float,
    complexity: float,
    mapping_level: str,
    primary_protein: str,
    has_carb_component: bool,
    has_veg_component: bool,
    current_source_ids: set[str],
) -> list[str]:
    reasons: list[str] = []
    if str(record["source_index"]) in current_source_ids:
        reasons.append("already_in_current_recipes")
    if is_out_of_scope(record):
        reasons.append("out_of_scope_or_processed")
    if contains_any(str(record["full_text"]), HARD_TO_SOURCE_KEYWORDS):
        reasons.append("hard_to_source_or_non_target_ingredient")
    if recipe_kind == "component" and is_component_only_title(str(record["core_text"])):
        reasons.append("sauce_marinade_or_rub_component_not_in_curated_200")
    if (
        recipe_kind == "breakfast"
        and primary_protein in {"chicken", "turkey", "fish", "beef", "pork"}
        and not has_carb_component
    ):
        reasons.append("breakfast_meat_component_not_full_breakfast")
    if int(record["num_steps"]) < MIN_STEPS:
        reasons.append("too_few_steps")
    if is_complete_meal and int(record["num_ingredients"]) <= 2:
        reasons.append("too_few_ingredients_for_complete_meal")
    max_steps = MAX_STEPS_MAIN if is_complete_meal else MAX_STEPS_OTHER
    if int(record["num_steps"]) > max_steps:
        reasons.append("too_many_steps")

    max_ingredients = MAX_INGREDIENTS_MAIN
    if recipe_kind == "breakfast":
        max_ingredients = MAX_INGREDIENTS_BREAKFAST
    elif recipe_kind == "snack":
        max_ingredients = MAX_INGREDIENTS_SNACK
    elif is_component_only:
        max_ingredients = MAX_INGREDIENTS_COMPONENT
    if int(record["num_ingredients"]) > max_ingredients:
        reasons.append("too_many_ingredients")

    if practicality < 5.5:
        reasons.append("low_european_practicality_score")
    if complexity > 6.0:
        reasons.append("high_ingredient_complexity")
    if mapping_level == "high":
        reasons.append("high_expected_mapping_difficulty")
    if recipe_kind in {"complete_main", "near_complete_main", "soup", "salad"} and not is_complete_meal:
        reasons.append("main_not_complete_enough")
    if recipe_kind == "snack" and int(record["num_ingredients"]) > MAX_INGREDIENTS_SNACK:
        reasons.append("snack_too_complex")
    return reasons


def compute_selection_score(
    recipe_kind: str,
    primary_protein: str,
    is_complete_meal: bool,
    is_component_only: bool,
    practicality: float,
    complete_score: float,
    complexity: float,
    mapping_level: str,
    has_carb_component: bool,
    has_veg_component: bool,
) -> float:
    score = practicality * 1.4 + complete_score * 1.7 - complexity * 0.7
    if recipe_kind == "complete_main":
        score += 5.0
    elif recipe_kind == "near_complete_main":
        score += 3.0
    elif recipe_kind in {"soup", "salad"} and is_complete_meal:
        score += 2.6
    elif recipe_kind == "breakfast":
        score += 1.6
    elif recipe_kind == "snack":
        score += 1.0
    elif is_component_only:
        score -= 2.5
    if primary_protein in {"chicken", "turkey", "fish", "beef", "pork"}:
        score += 1.6
    if primary_protein == "turkey":
        score += 1.2
    if has_carb_component and has_veg_component:
        score += 1.4
    if mapping_level == "low":
        score += 0.8
    elif mapping_level == "high":
        score -= 1.2
    return round(score, 3)


def candidate_bucket(row: dict[str, object]) -> str:
    kind = str(row["recipe_kind_guess"])
    if kind == "breakfast":
        return "breakfast"
    if kind == "snack":
        return "snack"
    if bool(row["is_complete_or_near_complete_meal"]):
        return "main"
    if bool(row["is_component_only"]):
        return "component"
    return "other"


def sort_candidates(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        rows,
        key=lambda row: (
            -float(row["selection_score"]),
            -float(row["completeness_score"]),
            float(row["ingredient_complexity_score"]),
            str(row["expected_mapping_difficulty"]),
            int(row["source_index"]),
        ),
    )


def select_with_caps(
    rows: list[dict[str, object]],
    target: int,
    selected_keys: set[str],
    selected_source_ids: set[int],
    use_protein_caps: bool,
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    protein_counts: Counter = Counter()

    for relax_caps in [False, True]:
        for row in sort_candidates(rows):
            if len(selected) >= target:
                return selected
            title_key = normalize_match_text(row["display_name"])
            source_index = int(row["source_index"])
            if title_key in selected_keys or source_index in selected_source_ids:
                continue
            protein = str(row["primary_protein"])
            if (
                use_protein_caps
                and not relax_caps
                and protein_counts[protein] >= PROTEIN_SOFT_CAPS.get(protein, 8)
            ):
                continue
            selected.append(row)
            selected_keys.add(title_key)
            selected_source_ids.add(source_index)
            protein_counts[protein] += 1
    return selected


def select_component_support(
    rows: list[dict[str, object]],
    target: int,
    selected_keys: set[str],
    selected_source_ids: set[int],
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    kind_targets = [
        ("protein_component", 5),
        ("carb_side", 5),
        ("veg_side", 5),
    ]
    for kind, kind_target in kind_targets:
        if len(selected) >= target:
            break
        picked = select_with_caps(
            [row for row in rows if str(row["recipe_kind_guess"]) == kind],
            min(kind_target, target - len(selected)),
            selected_keys,
            selected_source_ids,
            use_protein_caps=False,
        )
        selected.extend(picked)

    if len(selected) < target:
        remaining_rows = [row for row in rows if row not in selected]
        selected.extend(
            select_with_caps(
                remaining_rows,
                target - len(selected),
                selected_keys,
                selected_source_ids,
                use_protein_caps=False,
            )
        )
    return selected


def select_curated_rows(
    analyzed_rows: list[dict[str, object]],
    target_total: int,
    blocked_title_keys: set[str] | None = None,
) -> list[dict[str, object]]:
    blocked_title_keys = blocked_title_keys or set()
    clean_rows = [
        row
        for row in analyzed_rows
        if not row["exclusion_reasons"]
        and normalize_match_text(row["display_name"]) not in blocked_title_keys
    ]
    buckets = {
        "main": [row for row in clean_rows if candidate_bucket(row) == "main"],
        "breakfast": [row for row in clean_rows if candidate_bucket(row) == "breakfast"],
        "snack": [row for row in clean_rows if candidate_bucket(row) == "snack"],
        "component": [row for row in clean_rows if candidate_bucket(row) == "component"],
    }
    selected_keys: set[str] = set()
    selected_source_ids: set[int] = set()
    selected: list[dict[str, object]] = []

    main_target = min(TARGET_MAIN_MEALS, target_total)
    selected_main = select_with_caps(
        buckets["main"],
        main_target,
        selected_keys,
        selected_source_ids,
        use_protein_caps=True,
    )
    mark_selection_reason(selected_main, "main_complete_or_near_complete")
    selected.extend(selected_main)

    remaining_total = target_total - len(selected)
    breakfast_target = min(TARGET_BREAKFAST, max(0, remaining_total))
    selected_breakfast = select_with_caps(
        buckets["breakfast"],
        breakfast_target,
        selected_keys,
        selected_source_ids,
        use_protein_caps=False,
    )
    mark_selection_reason(selected_breakfast, "breakfast_target_bucket")
    selected.extend(selected_breakfast)

    remaining_total = target_total - len(selected)
    snack_target = min(TARGET_SNACK, max(0, remaining_total))
    selected_snacks = select_with_caps(
        buckets["snack"],
        snack_target,
        selected_keys,
        selected_source_ids,
        use_protein_caps=False,
    )
    mark_selection_reason(selected_snacks, "small_clean_snack_bucket")
    selected.extend(selected_snacks)

    remaining_total = target_total - len(selected)
    component_target = min(TARGET_COMPONENTS, max(0, remaining_total))
    selected_components = select_component_support(
        buckets["component"],
        component_target,
        selected_keys,
        selected_source_ids,
    )
    mark_selection_reason(selected_components, "minority_component_support_bucket")
    selected.extend(selected_components)

    if len(selected) < target_total:
        fallback_pool = [row for row in clean_rows if row not in selected]
        selected_fallback = select_with_caps(
            fallback_pool,
            target_total - len(selected),
            selected_keys,
            selected_source_ids,
            use_protein_caps=False,
        )
        mark_selection_reason(selected_fallback, "fallback_fill_after_target_buckets")
        selected.extend(selected_fallback)

    return selected[:target_total]


def mark_selection_reason(rows: list[dict[str, object]], reason: str) -> None:
    for row in rows:
        row["selection_reason"] = reason


def selected_output_rows(selected_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, row in enumerate(selected_rows, start=1):
        rows.append(
            {
                "recipe_id_candidate": f"recipes_v1_1_candidate_{index:03d}",
                "source_index": row["source_index"],
                "display_name": row["display_name"],
                "source_category": row["source_category"],
                "source_subcategory": row["source_subcategory"],
                "recipe_kind_guess": row["recipe_kind_guess"],
                "primary_protein": row["primary_protein"],
                "protein_hits_detected": row["protein_hits_detected"],
                "has_carb_component": row["has_carb_component"],
                "has_veg_component": row["has_veg_component"],
                "is_complete_or_near_complete_meal": row["is_complete_or_near_complete_meal"],
                "is_component_only": row["is_component_only"],
                "european_practicality_score": row["european_practicality_score"],
                "completeness_score": row["completeness_score"],
                "ingredient_complexity_score": row["ingredient_complexity_score"],
                "expected_mapping_difficulty": row["expected_mapping_difficulty"],
                "selection_reason": row.get("selection_reason", ""),
                "num_ingredients": row["num_ingredients"],
                "num_steps": row["num_steps"],
                "ingredients_json": row["ingredients_json"],
                "directions_json": row["directions_json"],
            }
        )
    return rows


def exclusion_rows(
    analyzed_rows: list[dict[str, object]],
    selected_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    selected_source_ids = {int(row["source_index"]) for row in selected_rows}
    rows: list[dict[str, object]] = []
    for row in analyzed_rows:
        if int(row["source_index"]) in selected_source_ids:
            continue
        reasons = list(row["exclusion_reasons"])
        if not reasons:
            bucket = candidate_bucket(row)
            reasons.append(f"not_selected_lower_rank_{bucket}")
        rows.append(
            {
                "source_index": row["source_index"],
                "display_name": row["display_name"],
                "source_category": row["source_category"],
                "source_subcategory": row["source_subcategory"],
                "recipe_kind_guess": row["recipe_kind_guess"],
                "primary_protein": row["primary_protein"],
                "has_carb_component": row["has_carb_component"],
                "has_veg_component": row["has_veg_component"],
                "is_complete_or_near_complete_meal": row["is_complete_or_near_complete_meal"],
                "is_component_only": row["is_component_only"],
                "european_practicality_score": row["european_practicality_score"],
                "completeness_score": row["completeness_score"],
                "ingredient_complexity_score": row["ingredient_complexity_score"],
                "expected_mapping_difficulty": row["expected_mapping_difficulty"],
                "exclusion_reason": ";".join(dict.fromkeys(reasons)),
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            str(item["exclusion_reason"]),
            str(item["recipe_kind_guess"]),
            -float(item["european_practicality_score"]),
            int(item["source_index"]),
        ),
    )


def category_mix_rows(selected_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    metrics = {
        "recipe_kind_guess": Counter(str(row["recipe_kind_guess"]) for row in selected_rows),
        "primary_protein": Counter(str(row["primary_protein"]) for row in selected_rows),
        "selection_reason": Counter(str(row.get("selection_reason", "")) for row in selected_rows),
        "meal_structure": Counter(meal_structure_label(row) for row in selected_rows),
        "mapping_difficulty": Counter(str(row["expected_mapping_difficulty"]) for row in selected_rows),
    }
    for metric_group, counter in metrics.items():
        for name, count in counter.most_common():
            rows.append({"metric_group": metric_group, "name": name, "count": count})
    return rows


def meal_structure_label(row: dict[str, object]) -> str:
    if bool(row["is_complete_or_near_complete_meal"]):
        return "complete_or_near_complete_meal"
    if str(row["recipe_kind_guess"]) == "breakfast":
        return "breakfast"
    if str(row["recipe_kind_guess"]) == "snack":
        return "snack"
    if bool(row["is_component_only"]):
        return "component_or_side"
    return "other"


def aggregate_selected_gaps(selected_rows: list[dict[str, object]], food_index: set[str]) -> dict[str, Counter]:
    counters = {
        "fooddb": Counter(),
        "alias_unit": Counter(),
    }
    for row in selected_rows:
        for parsed in row["parsed_ingredients"]:
            gap_class = classify_mapping_need(parsed, food_index)
            candidate_key = str(parsed["candidate_key"])
            if not candidate_key:
                continue
            if gap_class == "needs_fooddb_item":
                counters["fooddb"][candidate_key] += 1
            elif gap_class in {"needs_alias_only", "needs_unit_to_grams_rule"}:
                counters["alias_unit"][candidate_key] += 1
    return counters


def title_review_key(row: dict[str, object]) -> str:
    text = normalize_match_text(row["display_name"])
    removable = {
        "easy",
        "best",
        "ultimate",
        "classic",
        "homemade",
        "simple",
        "perfect",
        "quick",
        "air",
        "fryer",
        "instant",
        "pot",
        "slow",
        "cooker",
        "ii",
        "iii",
    }
    words = [word for word in text.split() if word not in removable]
    return " ".join(words[:4])


def is_complete_meal_really(row: dict[str, object]) -> bool:
    kind = str(row["recipe_kind_guess"])
    primary = str(row["primary_protein"])
    has_protein = primary != "unknown"
    has_carb = bool(row["has_carb_component"])
    has_veg = bool(row["has_veg_component"])
    if kind == "complete_main":
        return has_protein and has_carb and has_veg
    if kind == "near_complete_main":
        return has_protein and (has_carb or has_veg)
    if kind in {"soup", "salad"}:
        return has_protein and (has_carb or has_veg)
    return False


def title_quality_score(row: dict[str, object], reasons: list[str]) -> float:
    title = normalize_match_text(row["display_name"])
    score = 8.0
    if len(title.split()) < 2:
        score -= 2.0
    if len(title.split()) > 10:
        score -= 1.0
    if bool(row.get("has_brand_marker")):
        score -= 2.5
    if contains_any(title, OUT_OF_SCOPE_KEYWORDS | PROCESSED_KEYWORDS):
        score -= 3.0
    if contains_any(title, HARD_TO_SOURCE_KEYWORDS):
        score -= 2.0
    if contains_any(title, {"bombs", "crazy", "loaded", "super", "world", "copycat"}):
        score -= 1.0
    if any(reason.startswith("near_duplicate_title") for reason in reasons):
        score -= 1.0
    return round(max(0.0, min(10.0, score)), 2)


def review_selected_rows(selected_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    duplicate_keys = Counter(title_review_key(row) for row in selected_rows)
    review_rows: list[dict[str, object]] = []
    for index, row in enumerate(selected_rows, start=1):
        core_text = str(row["core_text"])
        full_text = str(row["full_text"])
        protein_hits = [hit for hit in str(row.get("protein_hits_detected", "")).split("|") if hit]
        weird = contains_any(full_text, HARD_TO_SOURCE_KEYWORDS) or contains_any(
            core_text,
            {"fusion", "hawaiian", "mardi gras", "cajun", "szechuan"},
        )
        american_processed = bool(row.get("has_brand_marker")) or contains_any(
            full_text,
            PROCESSED_KEYWORDS
            | {"tailgate", "super bowl", "biscuit bombs", "tater", "ranch"},
        )
        complete_really = is_complete_meal_really(row)
        has_protein = str(row["primary_protein"]) != "unknown"
        has_carb_or_veg = bool(row["has_carb_component"]) or bool(row["has_veg_component"])
        reasons: list[str] = []

        if duplicate_keys[title_review_key(row)] > 1:
            reasons.append("near_duplicate_title")
        if weird:
            reasons.append("weird_or_hard_to_source")
        if american_processed:
            reasons.append("too_american_processed_or_brand_heavy")
        if contains_any(full_text, OUT_OF_SCOPE_KEYWORDS):
            reasons.append("dessert_drink_or_out_of_scope_signal")
        if str(row["selection_reason"]) == "main_complete_or_near_complete" and not complete_really:
            reasons.append("main_meal_not_really_complete")
        if str(row["recipe_kind_guess"]) in {"soup", "salad"} and not complete_really:
            reasons.append("soup_or_salad_too_weak_as_meal")
        if not has_protein and str(row["recipe_kind_guess"]) not in {"carb_side", "veg_side", "snack"}:
            reasons.append("missing_clear_protein")
        if not has_carb_or_veg and str(row["recipe_kind_guess"]) not in {"protein_component"}:
            reasons.append("missing_clear_carb_or_veg")
        if str(row["expected_mapping_difficulty"]) == "high":
            reasons.append("high_mapping_risk")
        if int(row["num_ingredients"]) <= 2:
            reasons.append("very_short_ingredient_list")

        quality_score = title_quality_score(row, reasons)
        if quality_score < 6.0:
            reasons.append("low_title_quality_score")

        replace_reasons = {
            "dessert_drink_or_out_of_scope_signal",
            "too_american_processed_or_brand_heavy",
            "main_meal_not_really_complete",
            "soup_or_salad_too_weak_as_meal",
            "weird_or_hard_to_source",
        }
        if any(reason in replace_reasons for reason in reasons):
            status = "replace"
        elif reasons:
            status = "review"
        else:
            status = "keep"

        review_rows.append(
            {
                "recipe_id_candidate": f"recipes_v1_1_candidate_{index:03d}",
                "source_index": row["source_index"],
                "display_name": row["display_name"],
                "source_category": row["source_category"],
                "source_subcategory": row["source_subcategory"],
                "recipe_kind_guess": row["recipe_kind_guess"],
                "primary_protein": row["primary_protein"],
                "protein_hits_detected": row["protein_hits_detected"],
                "review_status": status,
                "review_reason": ";".join(dict.fromkeys(reasons)) if reasons else "quality_pass",
                "is_weird_or_random": weird,
                "is_too_american_processed": american_processed,
                "is_complete_meal_really": complete_really,
                "has_clear_protein": has_protein,
                "has_clear_carb_or_veg": has_carb_or_veg,
                "likely_mapping_risk": row["expected_mapping_difficulty"],
                "title_quality_score": quality_score,
                "selection_reason": row.get("selection_reason", ""),
                "num_ingredients": row["num_ingredients"],
                "num_steps": row["num_steps"],
            }
        )
    return review_rows


def write_review_summary(
    path: Path,
    review_rows: list[dict[str, object]],
    selected_rows: list[dict[str, object]],
) -> None:
    status_counts = Counter(str(row["review_status"]) for row in review_rows)
    protein_counts = Counter(str(row["primary_protein"]) for row in selected_rows)
    reason_counts: Counter = Counter()
    multiple_protein_hit_count = sum(
        1
        for row in selected_rows
        if len([hit for hit in str(row.get("protein_hits_detected", "")).split("|") if hit]) > 1
    )
    for row in review_rows:
        for reason in str(row["review_reason"]).split(";"):
            if reason and reason != "quality_pass":
                reason_counts[reason] += 1
    complete_main_total = sum(1 for row in review_rows if row["recipe_kind_guess"] == "complete_main")
    confirmed_complete_main = sum(
        1
        for row in review_rows
        if row["recipe_kind_guess"] == "complete_main" and row["is_complete_meal_really"]
    )
    replace_count = status_counts["replace"]
    recommendation = (
        "replacement pass recomandat inainte de ingredient parsing"
        if replace_count
        else "se poate trece la ingredient parsing dupa review manual rapid"
    )
    lines = [
        "Recipes_DB v1.1 curated_200 review audit",
        "",
        f"Selected count: {len(selected_rows)}",
        f"Primary protein distribution total: {sum(protein_counts.values())}",
        "",
        "Review status counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in status_counts.most_common())
    lines.extend(["", "Corrected primary_protein distribution:"])
    lines.extend(f"- {name}: {count}" for name, count in protein_counts.most_common())
    lines.extend(
        [
            "",
            f"Rows with multiple protein hits tracked separately: {multiple_protein_hit_count}",
            "",
            f"Complete_main confirmed real meals: {confirmed_complete_main}/{complete_main_total}",
            f"Recipes marked replace: {replace_count}",
            "",
            "Top review/replace reasons:",
        ]
    )
    lines.extend(f"- {name}: {count}" for name, count in reason_counts.most_common(20))
    lines.extend(["", "Recommendation:", f"- {recommendation}"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def replacement_log_rows(
    initial_selected_rows: list[dict[str, object]],
    final_selected_rows: list[dict[str, object]],
    analyzed_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    blocked_keys = replacement_title_keys()
    initial_keys = {normalize_match_text(row["display_name"]) for row in initial_selected_rows}
    analyzed_by_key = {normalize_match_text(row["display_name"]): row for row in analyzed_rows}
    initial_by_key = {normalize_match_text(row["display_name"]): row for row in initial_selected_rows}
    replacement_rows = [
        row
        for row in final_selected_rows
        if normalize_match_text(row["display_name"]) not in initial_keys
    ]
    available_replacements = list(replacement_rows)
    rows: list[dict[str, object]] = []
    for removed_title, removed_reason in MANUAL_REPLACEMENT_REASONS.items():
        removed_key = normalize_match_text(removed_title)
        removed = initial_by_key.get(removed_key) or analyzed_by_key.get(removed_key, {})
        if not removed:
            rows.append(
                {
                    "removed_title": removed_title,
                    "removed_reason": removed_reason,
                    "replacement_title": "not_found_in_source_audit",
                    "replacement_recipe_kind_guess": "",
                    "replacement_primary_protein": "",
                    "replacement_reason": "source_title_not_found",
                }
            )
            continue
        if removed_key not in initial_by_key:
            rows.append(
                {
                    "removed_title": removed_title,
                    "removed_reason": removed_reason,
                    "replacement_title": "not_selected_after_quality_calibration",
                    "replacement_recipe_kind_guess": "",
                    "replacement_primary_protein": "",
                    "replacement_reason": "removed_before_pairing_by_tightened_audit_rules",
                }
            )
            continue
        removed_bucket = candidate_bucket(removed)
        replacement = take_replacement_for_bucket(available_replacements, removed_bucket)
        rows.append(
            {
                "removed_title": removed_title,
                "removed_reason": removed_reason,
                "replacement_title": replacement.get("display_name", ""),
                "replacement_recipe_kind_guess": replacement.get("recipe_kind_guess", ""),
                "replacement_primary_protein": replacement.get("primary_protein", ""),
                "replacement_reason": replacement.get("selection_reason", ""),
            }
        )
    return rows


def take_replacement_for_bucket(
    available_replacements: list[dict[str, object]],
    bucket: str,
) -> dict[str, object]:
    for index, row in enumerate(available_replacements):
        if candidate_bucket(row) == bucket:
            return available_replacements.pop(index)
    if available_replacements:
        return available_replacements.pop(0)
    return {}


def write_summary(
    path: Path,
    selected_rows: list[dict[str, object]],
    analyzed_rows: list[dict[str, object]],
    source_count: int,
    deduped_count: int,
    food_index: set[str],
) -> None:
    kind_counts = Counter(str(row["recipe_kind_guess"]) for row in selected_rows)
    protein_counts = Counter(str(row["primary_protein"]) for row in selected_rows)
    protein_total = sum(protein_counts.values())
    structure_counts = Counter(meal_structure_label(row) for row in selected_rows)
    gap_counters = aggregate_selected_gaps(selected_rows, food_index)
    clean_candidate_count = sum(1 for row in analyzed_rows if not row["exclusion_reasons"])
    complete_count = structure_counts["complete_or_near_complete_meal"]
    component_count = structure_counts["component_or_side"]
    breakfast_count = structure_counts["breakfast"]
    snack_count = structure_counts["snack"]
    risks = []
    if len(selected_rows) < TARGET_TOTAL:
        risks.append("nu s-au gasit 200 retete curate cu regulile curente")
    if complete_count < TARGET_MAIN_MEALS:
        risks.append("mesele complete/near-complete sunt sub tinta de 155")
    if component_count > TARGET_COMPONENTS:
        risks.append("component bucket depaseste tinta de 15")
    if not risks:
        risks.append("selectia este draft si necesita review manual inainte de materializare")

    lines: list[str] = [
        "Recipes_DB v1.1 curated_200 draft selection",
        "",
        f"Source rows scanned: {source_count}",
        f"Deduped recipes scanned: {deduped_count}",
        f"Clean eligible candidates after filters: {clean_candidate_count}",
        f"Selected count: {len(selected_rows)}",
        f"Primary protein distribution total: {protein_total}",
        "",
        "Target direction:",
        f"- main complete/near-complete meals target: {TARGET_MAIN_MEALS}",
        f"- breakfast target: {TARGET_BREAKFAST}",
        f"- snack target: {TARGET_SNACK}",
        f"- component/side target: {TARGET_COMPONENTS}",
        "",
        "Selected structure:",
        f"- complete/near-complete meals: {complete_count}",
        f"- components/sides: {component_count}",
        f"- breakfast: {breakfast_count}",
        f"- snack: {snack_count}",
        "",
        "Count by recipe_kind_guess:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in kind_counts.most_common())
    lines.extend(["", "Count by primary_protein:"])
    lines.extend(f"- {name}: {count}" for name, count in protein_counts.most_common())
    lines.extend(["", "Top likely Food_DB gaps among selected recipes:"])
    lines.extend(
        f"- {name}: {count}"
        for name, count in gap_counters["fooddb"].most_common(20)
    )
    lines.extend(["", "Top likely alias/unit-to-grams needs among selected recipes:"])
    lines.extend(
        f"- {name}: {count}"
        for name, count in gap_counters["alias_unit"].most_common(20)
    )
    lines.extend(["", "Risks:"])
    lines.extend(f"- {risk}" for risk in risks)
    lines.extend(
        [
            "",
            "Recommended next step:",
            "- review recipes_v1_1_curated_200.csv manually by bucket",
            "- remove weak complete-meal false positives before ingredient parsing",
            "- add alias rules and unit-to-grams rules separately from Food_DB additions",
            "- keep this as draft/audit only until curated_200 is approved",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    source_rows = read_csv_rows(Path(args.source_recipes))
    current_rows = read_csv_rows(Path(args.current_recipes))
    food_rows = read_csv_rows(Path(args.fooddb))
    current_source_ids = {
        normalize_space(row.get("source_recipe_id"))
        for row in current_rows
        if normalize_space(row.get("source_recipe_id"))
    }
    food_index = build_food_index(food_rows)
    source_records = dedupe_source_records(source_rows)
    analyzed_rows = [
        analyze_record(record, food_index, current_source_ids)
        for record in source_records
    ]
    initial_selected_rows = select_curated_rows(analyzed_rows, int(args.target_total))
    selected_rows = select_curated_rows(
        analyzed_rows,
        int(args.target_total),
        blocked_title_keys=replacement_title_keys(),
    )

    write_csv(
        Path(args.out_curated),
        selected_output_rows(selected_rows),
        OUTPUT_COLUMNS,
    )
    write_csv(
        Path(args.out_exclusion_log),
        exclusion_rows(analyzed_rows, selected_rows),
        EXCLUSION_COLUMNS,
    )
    write_csv(
        Path(args.out_category_mix),
        category_mix_rows(selected_rows),
        CATEGORY_MIX_COLUMNS,
    )
    review_rows = review_selected_rows(selected_rows)
    write_csv(
        Path(args.out_review_audit),
        review_rows,
        REVIEW_AUDIT_COLUMNS,
    )
    write_csv(
        Path(args.out_replacement_log),
        replacement_log_rows(initial_selected_rows, selected_rows, analyzed_rows),
        REPLACEMENT_LOG_COLUMNS,
    )
    write_summary(
        Path(args.out_summary),
        selected_rows,
        analyzed_rows,
        len(source_rows),
        len(source_records),
        food_index,
    )
    write_review_summary(
        Path(args.out_review_summary),
        review_rows,
        selected_rows,
    )

    print(f"Source rows scanned: {len(source_rows)}")
    print(f"Deduped recipes scanned: {len(source_records)}")
    print(f"Selected curated recipes: {len(selected_rows)}")
    print(f"Written: {args.out_curated}")
    print(f"Written: {args.out_summary}")
    print(f"Written: {args.out_exclusion_log}")
    print(f"Written: {args.out_category_mix}")
    print(f"Written: {args.out_review_audit}")
    print(f"Written: {args.out_review_summary}")
    print(f"Written: {args.out_replacement_log}")


if __name__ == "__main__":
    main()
