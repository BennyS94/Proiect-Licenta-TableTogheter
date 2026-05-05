from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from fractions import Fraction
from pathlib import Path


DEFAULT_INPUT = Path("data/recipesdb/draft/recipes_v1_1_curated_200.csv")
DEFAULT_SOURCE_RECIPES = Path("data/recipesdb/source/1_Recipe_csv.csv")
DEFAULT_PARSED_OUT = Path("data/recipesdb/draft/recipes_v1_1_ingredients_parsed.csv")
DEFAULT_SUMMARY_OUT = Path("data/recipesdb/audit/recipes_v1_1_parse_summary.txt")
DEFAULT_REVIEW_OUT = Path("data/recipesdb/audit/recipes_v1_1_parse_review.csv")
DEFAULT_FAILED_OUT = Path("data/recipesdb/audit/recipes_v1_1_parse_failed.csv")

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
    "quantity_text",
    "quantity_grams_estimated",
    "grams_estimation_method",
    "ingredient_role",
    "ingredient_slot_key",
    "is_optional",
    "is_substitutable",
    "parse_status",
    "parse_notes",
]

FRACTION_MAP = {
    "¼": "1/4",
    "½": "1/2",
    "¾": "3/4",
    "⅐": "1/7",
    "⅑": "1/9",
    "⅒": "1/10",
    "⅓": "1/3",
    "⅔": "2/3",
    "⅕": "1/5",
    "⅖": "2/5",
    "⅗": "3/5",
    "⅘": "4/5",
    "⅙": "1/6",
    "⅚": "5/6",
    "⅛": "1/8",
    "⅜": "3/8",
    "⅝": "5/8",
    "⅞": "7/8",
    "Â¼": "1/4",
    "Â½": "1/2",
    "Â¾": "3/4",
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
    "pinch": "pinch",
    "dash": "dash",
    "clove": "clove",
    "cloves": "clove",
    "head": "head",
    "heads": "head",
    "bunch": "bunch",
    "bunches": "bunch",
    "slice": "slice",
    "slices": "slice",
    "strip": "strip",
    "strips": "strip",
    "stalk": "stalk",
    "stalks": "stalk",
    "ear": "ear",
    "ears": "ear",
    "sprig": "sprig",
    "sprigs": "sprig",
    "leaf": "leaf",
    "leaves": "leaf",
    "cube": "cube",
    "cubes": "cube",
    "filet": "filet",
    "filets": "filet",
    "fillet": "fillet",
    "fillets": "fillet",
    "breast": "breast",
    "breasts": "breast",
    "thigh": "thigh",
    "thighs": "thigh",
    "package": "package",
    "packages": "package",
    "packet": "packet",
    "packets": "packet",
    "can": "can",
    "cans": "can",
    "jar": "jar",
    "jars": "jar",
    "bottle": "bottle",
    "bottles": "bottle",
    "box": "box",
    "boxes": "box",
    "envelope": "envelope",
    "envelopes": "envelope",
}

PACKAGING_UNITS = {"package", "packet", "can", "jar", "bottle", "box", "envelope"}
DIRECT_WEIGHT_UNITS = {"gram", "kilogram", "ounce", "pound"}

OPTIONAL_RE = re.compile(r"\boptional\b", flags=re.IGNORECASE)
GARNISH_RE = re.compile(r"\bfor garnish\b|\bfor serving\b|\bto serve\b", flags=re.IGNORECASE)
ALTERNATIVE_RE = re.compile(
    r"\bor\b|\bto taste\b|\bas needed\b|\bas desired\b|\bif desired\b",
    flags=re.IGNORECASE,
)
PACKAGING_RE = re.compile(
    r"\bpackage\b|\bpackages\b|\bcan\b|\bcans\b|\bjar\b|\bbottle\b|\bbox\b|\bboxes\b|\bpacket\b|\bpackets\b|\benvelope\b",
    flags=re.IGNORECASE,
)
ATYPICAL_REVIEW_RE = re.compile(
    r"\bcooking spray\b|\bnonstick cooking spray\b|\baluminum foil\b|\bfoil\b|\bparchment paper\b|\bwater to cover\b",
    flags=re.IGNORECASE,
)
BRAND_HINT_RE = re.compile(r"\bsuch as\b.*$", flags=re.IGNORECASE)
SECTION_HEADER_RE = re.compile(r":\s*$")
QUANTITY_RE = re.compile(
    r"^\s*(?P<prefix>about\s+|approximately\s+)?(?P<qty>(?:\d+\s+\d+/\d+|\d+-\d+/\d+|\d+/\d+|\d+(?:\.\d+)?)(?:\s*(?:to|-)\s*(?:\d+\s+\d+/\d+|\d+-\d+/\d+|\d+/\d+|\d+(?:\.\d+)?))?)\b(?P<rest>.*)$",
    flags=re.IGNORECASE,
)
PAREN_RE = re.compile(r"^\s*(\([^)]*\))(.*)$")
PAREN_PACK_SIZE_RE = re.compile(
    r"(ounce|ounces|oz|pound|pounds|lb|lbs|gram|grams|g|kg|kilogram|kilograms|ml|milliliter|milliliters|liter|liters|l|count)",
    flags=re.IGNORECASE,
)
SIZE_WORD_RE = re.compile(
    r"^(small|medium|large|extra-large|extra large|jumbo)\b\s*",
    flags=re.IGNORECASE,
)
LEADING_DESCRIPTOR_RE = re.compile(
    r"^(skinless|boneless|fat-trimmed|fat trimmed|trimmed|thick|thin)\b[\s-]*",
    flags=re.IGNORECASE,
)
PREPARATION_FRAGMENT_RE = re.compile(
    r"^(?:(?:very|finely|coarsely|roughly|thinly|thickly|lightly)\s+)?(?:with\b|for\b|plus\b|remaining\b|reserved\b|divided\b|chopped\b|diced\b|minced\b|sliced\b|softened\b|melted\b|drained\b|rinsed\b|thawed\b|crushed\b|peeled\b|trimmed\b|cubed\b|halved\b|quartered\b|beaten\b|packed\b|warmed\b|broken\b|juiced\b|zested\b|shredded\b|grated\b|removed\b|cut\b)",
    flags=re.IGNORECASE,
)

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
    "beaten",
    "melted",
    "softened",
    "packed",
}

ROLE_KEYWORDS = {
    "protein": {
        "chicken",
        "turkey",
        "beef",
        "pork",
        "fish",
        "salmon",
        "tuna",
        "cod",
        "tilapia",
        "shrimp",
        "scallops",
        "egg",
        "eggs",
        "tofu",
        "beans",
        "lentils",
        "chickpeas",
    },
    "carb": {
        "rice",
        "potato",
        "potatoes",
        "pasta",
        "spaghetti",
        "noodle",
        "noodles",
        "lasagna",
        "flour",
        "oats",
        "oatmeal",
        "bread",
        "tortilla",
        "tortillas",
        "couscous",
        "quinoa",
        "corn",
    },
    "veg": {
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
        "mushroom",
        "mushrooms",
        "celery",
        "kale",
        "lettuce",
        "cucumber",
        "green beans",
    },
    "fat_source": {"oil", "olive oil", "butter", "margarine", "lard", "bacon grease"},
    "dairy": {"milk", "cream", "cheese", "parmesan", "mozzarella", "ricotta", "yogurt"},
    "seasoning": {
        "salt",
        "pepper",
        "paprika",
        "cumin",
        "oregano",
        "basil",
        "thyme",
        "parsley",
        "rosemary",
        "cinnamon",
        "garlic powder",
    },
    "sauce": {"sauce", "soy sauce", "vinegar", "mustard", "broth", "stock", "pesto"},
}

COUNT_GRAMS = {
    "egg": 50.0,
    "eggs": 50.0,
    "garlic clove": 3.0,
    "garlic cloves": 3.0,
    "clove garlic": 3.0,
    "cloves garlic": 3.0,
    "lemon": 58.0,
    "lime": 67.0,
}

ONION_SIZE_GRAMS = {
    "small": 70.0,
    "medium": 110.0,
    "large": 150.0,
}

SPOON_GRAMS = {
    "olive oil": {"tablespoon": 13.5, "teaspoon": 4.5},
    "vegetable oil": {"tablespoon": 13.5, "teaspoon": 4.5},
    "sesame oil": {"tablespoon": 13.5, "teaspoon": 4.5},
    "butter": {"tablespoon": 14.2, "teaspoon": 4.7},
    "white sugar": {"tablespoon": 12.5, "teaspoon": 4.2},
    "brown sugar": {"tablespoon": 13.8, "teaspoon": 4.6},
    "sugar": {"tablespoon": 12.5, "teaspoon": 4.2},
    "salt": {"tablespoon": 18.0, "teaspoon": 6.0},
    "soy sauce": {"tablespoon": 16.0, "teaspoon": 5.3},
    "vinegar": {"tablespoon": 15.0, "teaspoon": 5.0},
    "balsamic vinegar": {"tablespoon": 15.0, "teaspoon": 5.0},
    "honey": {"tablespoon": 21.0, "teaspoon": 7.0},
    "milk": {"tablespoon": 15.0, "teaspoon": 5.0},
    "water": {"tablespoon": 15.0, "teaspoon": 5.0},
}

CUP_GRAMS = {
    "water": 240.0,
    "milk": 245.0,
    "all purpose flour": 120.0,
    "flour": 120.0,
    "white sugar": 200.0,
    "brown sugar": 220.0,
    "oats": 80.0,
    "quick cooking oats": 80.0,
    "olive oil": 216.0,
    "vegetable oil": 216.0,
    "rice": 185.0,
    "cooked rice": 158.0,
    "bread crumbs": 108.0,
    "breadcrumbs": 108.0,
    "shredded cheese": 113.0,
    "parmesan cheese": 100.0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parseaza conservator ingredientele Recipes_DB v1.1 curated_200."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--source-recipes", default=str(DEFAULT_SOURCE_RECIPES))
    parser.add_argument("--out-parsed", default=str(DEFAULT_PARSED_OUT))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY_OUT))
    parser.add_argument("--out-review", default=str(DEFAULT_REVIEW_OUT))
    parser.add_argument("--out-failed", default=str(DEFAULT_FAILED_OUT))
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


def normalize_for_parse(text: object) -> str:
    value = str(text or "")
    for source, target in FRACTION_MAP.items():
        value = value.replace(source, f" {target} ")
    value = value.replace("â€”", "-").replace("â€“", "-")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_name(text: object) -> str:
    value = normalize_for_parse(text).casefold()
    value = value.replace("&", " and ")
    value = value.replace("-", " ")
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_slot_key(text: object) -> str:
    return normalize_name(text).replace(" ", "_")


def parse_single_amount(text: str) -> float | None:
    value = text.strip().lower()
    value = re.sub(r"^(about|approximately)\s+", "", value)
    if re.fullmatch(r"\d+-\d+/\d+", value):
        whole, frac = value.split("-", 1)
        return float(whole) + float(Fraction(frac))
    if re.fullmatch(r"\d+\s+\d+/\d+", value):
        whole, frac = value.split()
        return float(whole) + float(Fraction(frac))
    if re.fullmatch(r"\d+/\d+", value):
        return float(Fraction(value))
    if re.fullmatch(r"\d+(?:\.\d+)?", value):
        return float(value)
    return None


def parse_quantity_range(text: str) -> tuple[float | None, float | None]:
    value = text.strip().lower()
    value = re.sub(r"^(about|approximately)\s+", "", value)
    if " to " in value:
        left, right = value.split(" to ", 1)
        return parse_single_amount(left), parse_single_amount(right)
    if re.search(r"\s-\s", value):
        left, right = re.split(r"\s-\s", value, maxsplit=1)
        return parse_single_amount(left), parse_single_amount(right)
    single = parse_single_amount(value)
    return single, single


def average_quantity(low: float | None, high: float | None) -> float | None:
    if low is None or high is None:
        return None
    return round((low + high) / 2, 4)


def split_parenthetical_prefix(rest_text: str) -> tuple[list[str], str]:
    rest = rest_text
    parenthetical_chunks: list[str] = []
    while True:
        match = PAREN_RE.match(rest)
        if not match:
            break
        parenthetical_chunks.append(match.group(1).strip())
        rest = match.group(2).strip()
    return parenthetical_chunks, rest


def extract_quantity_and_unit(ingredient_raw: str) -> dict[str, object]:
    working = normalize_for_parse(ingredient_raw)
    quantity_raw = ""
    quantity_low = None
    quantity_high = None
    unit_raw = ""
    unit_normalized = ""
    parse_notes: list[str] = []

    match = QUANTITY_RE.match(working)
    if not match:
        return {
            "quantity_raw": "",
            "quantity_value_low": None,
            "quantity_value_high": None,
            "unit_raw": "",
            "unit_normalized": "",
            "ingredient_body": working,
            "leading_parentheticals": [],
            "quantity_notes": [],
        }

    quantity_raw = match.group("qty").strip()
    quantity_low, quantity_high = parse_quantity_range(quantity_raw)
    rest = match.group("rest").strip()
    parenthetical_chunks, rest = split_parenthetical_prefix(rest)
    if parenthetical_chunks:
        quantity_raw = f"{quantity_raw} {' '.join(parenthetical_chunks)}".strip()
        if any(PAREN_PACK_SIZE_RE.search(chunk) for chunk in parenthetical_chunks):
            parse_notes.append("pack_size_note")

    if quantity_low is None or quantity_high is None:
        parse_notes.append("unparsed_quantity")

    unit_match = re.match(r"^([A-Za-z][A-Za-z.-]*)\b(.*)$", rest)
    if unit_match:
        candidate_unit = unit_match.group(1).strip()
        candidate_normalized = UNIT_NORMALIZATION.get(candidate_unit.lower(), "")
        if candidate_normalized:
            unit_raw = candidate_unit
            unit_normalized = candidate_normalized
            rest = unit_match.group(2).strip()

    return {
        "quantity_raw": quantity_raw,
        "quantity_value_low": quantity_low,
        "quantity_value_high": quantity_high,
        "unit_raw": unit_raw,
        "unit_normalized": unit_normalized,
        "ingredient_body": rest.strip(" ,;-"),
        "leading_parentheticals": parenthetical_chunks,
        "quantity_notes": parse_notes,
    }


def remove_meta_phrases(text: str) -> tuple[str, list[str]]:
    value = text
    notes: list[str] = []
    brand_match = BRAND_HINT_RE.search(value)
    if brand_match:
        value = value[: brand_match.start()].rstrip(" ,;-")
        notes.append("brand_hint_removed")
    value = re.sub(r"\((optional)\)", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\boptional\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bfor garnish\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bfor serving\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bto serve\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bto taste\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bas needed\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bas desired\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bif desired\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\s*,\s*", ", ", value)
    return value.strip(" ,;-"), notes


def extract_leading_modifiers(primary_part: str, allow_safe_descriptor_cleanup: bool) -> tuple[str, list[str], list[str]]:
    working = primary_part.strip()
    prep_parts: list[str] = []
    derivation_notes: list[str] = []

    size_match = SIZE_WORD_RE.match(working)
    if size_match:
        prep_parts.append(size_match.group(1).strip())
        derivation_notes.append("size_descriptor_removed")
        working = working[size_match.end() :].strip(" ,;-")

    if allow_safe_descriptor_cleanup:
        while True:
            descriptor_match = LEADING_DESCRIPTOR_RE.match(working)
            if not descriptor_match:
                break
            prep_parts.append(descriptor_match.group(1).strip())
            derivation_notes.append("leading_descriptor_removed")
            working = working[descriptor_match.end() :].strip(" ,;-")

    return working, prep_parts, derivation_notes


def derive_food_name_and_prep(
    ingredient_body: str,
    allow_safe_descriptor_cleanup: bool = True,
) -> tuple[str, str, str, list[str], list[str]]:
    clean_body, clean_notes = remove_meta_phrases(ingredient_body)
    if not clean_body:
        return "", "", "", clean_notes, []

    parts = [part.strip(" ,;-") for part in clean_body.split(",") if part.strip(" ,;-")]
    if not parts:
        parts = [clean_body]

    candidate_parts = [part for part in parts if not PREPARATION_FRAGMENT_RE.match(part)]
    primary_pool = candidate_parts if candidate_parts else parts
    primary_part = max(primary_pool, key=lambda value: (len(value), -primary_pool.index(value)))
    prep_parts = [part for part in parts if part != primary_part]
    primary_part, extracted_prep_parts, derivation_notes = extract_leading_modifiers(
        primary_part=primary_part,
        allow_safe_descriptor_cleanup=allow_safe_descriptor_cleanup,
    )
    prep_parts = extracted_prep_parts + prep_parts
    primary_part = re.sub(r"\([^)]*\)", "", primary_part).strip(" ,;-")
    primary_part = re.sub(r"\s+", " ", primary_part)

    preparation_note = "; ".join(prep_parts).strip(" ;")
    return clean_body, primary_part, preparation_note, clean_notes, derivation_notes


def normalized_lookup_name(ingredient_name: str) -> str:
    words = [word for word in normalize_name(ingredient_name).split() if word not in PREPARATION_WORDS]
    return " ".join(words).strip()


def contains_word(text: str, keyword: str) -> bool:
    normalized_text = normalize_name(text)
    normalized_keyword = normalize_name(keyword)
    if " " in normalized_keyword:
        return normalized_keyword in normalized_text
    return re.search(rf"(?<![a-z0-9]){re.escape(normalized_keyword)}(?![a-z0-9])", normalized_text) is not None


def estimate_grams(
    quantity_value: float | None,
    unit: str,
    ingredient_name: str,
    preparation_note: str,
    quantity_raw: str,
) -> tuple[float | None, str, list[str]]:
    if quantity_value is None or quantity_value <= 0:
        return None, "", ["grams_missing_quantity"]

    lookup = normalized_lookup_name(ingredient_name)
    notes: list[str] = []

    if unit == "gram":
        return round(quantity_value, 2), "direct_gram", notes
    if unit == "kilogram":
        return round(quantity_value * 1000, 2), "direct_kilogram", notes
    if unit == "ounce":
        return round(quantity_value * 28.3495, 2), "direct_ounce", notes
    if unit == "pound":
        return round(quantity_value * 453.592, 2), "direct_pound", notes
    if unit == "milliliter" and is_water_like(lookup):
        return round(quantity_value, 2), "ml_water_like_density_1", notes
    if unit == "liter" and is_water_like(lookup):
        return round(quantity_value * 1000, 2), "liter_water_like_density_1", notes
    if unit in {"tablespoon", "teaspoon"}:
        grams = estimate_spoon_grams(quantity_value, unit, lookup)
        if grams is not None:
            return grams, f"{unit}_safe_density_rule", notes
        return None, "", [f"{unit}_needs_density_rule"]
    if unit == "cup":
        grams = estimate_cup_grams(quantity_value, lookup)
        if grams is not None:
            return grams, "cup_safe_density_rule", notes
        return None, "", ["cup_needs_density_rule"]
    if unit == "clove" and contains_word(lookup, "garlic"):
        return round(quantity_value * 3.0, 2), "garlic_clove_rule", notes
    if unit in {"slice", "stalk", "head", "bunch", "sprig", "leaf", "fillet", "breast", "thigh"}:
        return None, "", [f"{unit}_needs_food_specific_rule"]
    if not unit:
        grams = estimate_count_grams(quantity_value, lookup, preparation_note, quantity_raw)
        if grams is not None:
            return grams, "count_safe_food_rule", notes
        return None, "", ["count_without_safe_grams_rule"]
    if unit in PACKAGING_UNITS:
        return None, "", ["packaging_unit_no_grams_estimate"]
    return None, "", [f"{unit}_no_grams_rule"]


def is_water_like(lookup: str) -> bool:
    return lookup in {"water", "broth", "stock"} or lookup.endswith(" broth") or lookup.endswith(" stock")


def estimate_spoon_grams(quantity_value: float, unit: str, lookup: str) -> float | None:
    for key, rules in SPOON_GRAMS.items():
        if key in lookup:
            return round(quantity_value * rules[unit], 2)
    return None


def estimate_cup_grams(quantity_value: float, lookup: str) -> float | None:
    for key, grams_per_cup in CUP_GRAMS.items():
        if key in lookup:
            return round(quantity_value * grams_per_cup, 2)
    return None


def estimate_count_grams(
    quantity_value: float,
    lookup: str,
    preparation_note: str,
    quantity_raw: str,
) -> float | None:
    for key, grams_each in COUNT_GRAMS.items():
        if lookup == key or key in lookup:
            return round(quantity_value * grams_each, 2)
    if "onion" in lookup:
        size = first_size_signal(preparation_note, quantity_raw)
        return round(quantity_value * ONION_SIZE_GRAMS.get(size, 110.0), 2)
    return None


def first_size_signal(preparation_note: str, quantity_raw: str) -> str:
    text = normalize_name(f"{quantity_raw} {preparation_note}")
    for size in ["small", "medium", "large"]:
        if contains_word(text, size):
            return size
    return "medium"


def ingredient_role(ingredient_name: str) -> str:
    lookup = normalized_lookup_name(ingredient_name)
    for role, keywords in ROLE_KEYWORDS.items():
        for keyword in keywords:
            if contains_word(lookup, keyword):
                return role
    return "other"


def classify_status(
    is_section_header: bool,
    ingredient_name: str,
    flags: dict[str, int],
    quantity_info: dict[str, object],
    parse_notes: list[str],
) -> str:
    if is_section_header:
        return "review_needed"
    if not ingredient_name:
        return "failed_parse"
    if "unparsed_quantity" in parse_notes:
        return "review_needed"
    if flags["packaging_flag"]:
        return "review_needed"
    if flags["alternative_flag"]:
        return "review_needed"
    if flags["garnish_flag"]:
        return "review_needed"
    if "mapping_risk_line" in parse_notes:
        return "review_needed"
    if flags["optional_flag"]:
        return "parsed_partial"
    if not quantity_info["quantity_raw"]:
        return "parsed_partial"
    if quantity_info["quantity_raw"] and not quantity_info["unit_normalized"]:
        return "parsed_partial"
    if quantity_info["leading_parentheticals"]:
        return "parsed_partial"
    if any(note.endswith("_needs_density_rule") for note in parse_notes):
        return "parsed_partial"
    if any(note.endswith("_needs_food_specific_rule") for note in parse_notes):
        return "parsed_partial"
    if parse_notes:
        return "parsed_partial"
    return "parsed_clean"


def parse_ingredient_row(recipe_row: dict[str, str], ingredient_index: int, ingredient_raw: str) -> dict[str, object]:
    stripped_raw = str(ingredient_raw or "").strip()
    is_section_header = bool(SECTION_HEADER_RE.search(stripped_raw))
    optional_flag = int(bool(OPTIONAL_RE.search(stripped_raw)))
    garnish_flag = int(bool(GARNISH_RE.search(stripped_raw)))
    alternative_flag = int(bool(ALTERNATIVE_RE.search(stripped_raw)))
    packaging_flag = int(bool(PACKAGING_RE.search(stripped_raw)))
    parse_notes: list[str] = []

    quantity_info = {
        "quantity_raw": "",
        "quantity_value_low": None,
        "quantity_value_high": None,
        "unit_raw": "",
        "unit_normalized": "",
        "ingredient_body": normalize_for_parse(stripped_raw),
        "leading_parentheticals": [],
        "quantity_notes": [],
    }
    ingredient_name = ""
    preparation_note = ""

    if is_section_header:
        parse_notes.append("section_header_row")
        ingredient_name = stripped_raw.rstrip(":").strip()
    else:
        quantity_info = extract_quantity_and_unit(stripped_raw)
        parse_notes.extend(quantity_info["quantity_notes"])
        if quantity_info["leading_parentheticals"]:
            parse_notes.append("leading_parenthetical_note")
        if quantity_info["unit_normalized"] in PACKAGING_UNITS:
            packaging_flag = 1

        _, ingredient_name, preparation_note, clean_notes, derivation_notes = derive_food_name_and_prep(
            str(quantity_info["ingredient_body"]),
            allow_safe_descriptor_cleanup=True,
        )
        parse_notes.extend(clean_notes)
        parse_notes.extend(derivation_notes)
        if preparation_note:
            parse_notes.append("preparation_note_from_commas")
        if not quantity_info["quantity_raw"]:
            parse_notes.append("missing_quantity")
        elif quantity_info["quantity_value_low"] != quantity_info["quantity_value_high"]:
            parse_notes.append("quantity_range")
        if quantity_info["quantity_raw"] and not quantity_info["unit_raw"]:
            parse_notes.append("count_without_measure_unit")
        if optional_flag:
            parse_notes.append("optional_phrase")
        if garnish_flag:
            parse_notes.append("garnish_phrase")
        if alternative_flag:
            parse_notes.append("alternative_phrase")
        if packaging_flag:
            parse_notes.append("packaging_phrase")
        if ATYPICAL_REVIEW_RE.search(stripped_raw):
            parse_notes.append("mapping_risk_line")

    quantity_value = average_quantity(
        quantity_info["quantity_value_low"],
        quantity_info["quantity_value_high"],
    )
    grams, grams_method, grams_notes = estimate_grams(
        quantity_value=quantity_value,
        unit=str(quantity_info["unit_normalized"]),
        ingredient_name=ingredient_name,
        preparation_note=preparation_note,
        quantity_raw=str(quantity_info["quantity_raw"]),
    )
    parse_notes.extend(grams_notes)
    parse_notes = list(dict.fromkeys(note for note in parse_notes if note))
    parse_status = classify_status(
        is_section_header=is_section_header,
        ingredient_name=ingredient_name,
        flags={
            "optional_flag": optional_flag,
            "garnish_flag": garnish_flag,
            "alternative_flag": alternative_flag,
            "packaging_flag": packaging_flag,
        },
        quantity_info=quantity_info,
        parse_notes=parse_notes,
    )
    if parse_status == "failed_parse" and "empty_ingredient_name" not in parse_notes:
        parse_notes.append("empty_ingredient_name")

    normalized = normalized_lookup_name(ingredient_name)
    role = ingredient_role(ingredient_name)
    return {
        "recipe_id_candidate": recipe_row["recipe_id_candidate"],
        "source_index": recipe_row["source_index"],
        "display_name": recipe_row["display_name"],
        "ingredient_position": ingredient_index,
        "ingredient_raw_text": stripped_raw,
        "ingredient_name_parsed": ingredient_name,
        "ingredient_name_normalized": normalized,
        "quantity_value": quantity_value if quantity_value is not None else "",
        "quantity_unit": quantity_info["unit_normalized"] or ("count" if quantity_info["quantity_raw"] else ""),
        "quantity_text": quantity_info["quantity_raw"],
        "quantity_grams_estimated": grams if grams is not None else "",
        "grams_estimation_method": grams_method,
        "ingredient_role": role,
        "ingredient_slot_key": normalize_slot_key(normalized),
        "is_optional": optional_flag,
        "is_substitutable": int(role not in {"seasoning", "sauce"} and not optional_flag),
        "parse_status": parse_status,
        "parse_notes": ";".join(parse_notes),
    }


def build_summary(parsed_rows: list[dict[str, object]], recipe_count: int) -> str:
    status_counts = Counter(str(row["parse_status"]) for row in parsed_rows)
    unit_counts = Counter(str(row["quantity_unit"]) for row in parsed_rows if row["quantity_unit"])
    normalized_counts = Counter(str(row["ingredient_name_normalized"]) for row in parsed_rows if row["ingredient_name_normalized"])
    partial_reason_counts = Counter()
    review_reason_counts = Counter()
    recipe_review_counts: dict[str, Counter] = defaultdict(Counter)
    grams_count = sum(1 for row in parsed_rows if row["quantity_grams_estimated"] != "")

    for row in parsed_rows:
        notes = [note for note in str(row["parse_notes"]).split(";") if note]
        if row["parse_status"] == "parsed_partial":
            partial_reason_counts.update(notes)
        if row["parse_status"] in {"review_needed", "failed_parse"}:
            review_reason_counts.update(notes)
            recipe_review_counts[str(row["display_name"])]["review_or_failed"] += 1

    lines = [
        "Recipes_DB v1.1 curated_200 ingredient parse summary",
        "",
        f"Total recipes parsed: {recipe_count}",
        f"Total ingredient rows: {len(parsed_rows)}",
        f"Rows with quantity_grams_estimated > 0: {grams_count}",
        f"Rows missing quantity_grams_estimated: {len(parsed_rows) - grams_count}",
        "",
        "Parse status counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in status_counts.most_common())
    lines.extend(["", "Top parsed_partial reasons:"])
    lines.extend(f"- {name}: {count}" for name, count in partial_reason_counts.most_common(20))
    lines.extend(["", "Top review_needed reasons:"])
    lines.extend(f"- {name}: {count}" for name, count in review_reason_counts.most_common(20))
    lines.extend(["", "Top quantity units:"])
    lines.extend(f"- {name}: {count}" for name, count in unit_counts.most_common(20))
    lines.extend(["", "Top normalized ingredient names:"])
    lines.extend(f"- {name}: {count}" for name, count in normalized_counts.most_common(30))
    lines.extend(["", "Recipes with many review/failed ingredients:"])
    for title, counter in sorted(
        recipe_review_counts.items(),
        key=lambda item: (-item[1]["review_or_failed"], item[0]),
    )[:20]:
        lines.append(f"- {title}: {counter['review_or_failed']}")
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    parsed_out = Path(args.out_parsed)
    summary_out = Path(args.out_summary)
    review_out = Path(args.out_review)
    failed_out = Path(args.out_failed)

    recipe_rows = read_csv_rows(input_path)
    parsed_rows: list[dict[str, object]] = []
    for recipe_row in recipe_rows:
        ingredients = json.loads(recipe_row["ingredients_json"])
        for ingredient_index, ingredient_raw in enumerate(ingredients, start=1):
            parsed_rows.append(parse_ingredient_row(recipe_row, ingredient_index, ingredient_raw))

    review_rows = [
        row
        for row in parsed_rows
        if row["parse_status"] in {"parsed_partial", "review_needed"}
    ]
    failed_rows = [row for row in parsed_rows if row["parse_status"] == "failed_parse"]

    write_csv(parsed_out, parsed_rows, OUTPUT_COLUMNS)
    write_csv(review_out, review_rows, OUTPUT_COLUMNS)
    write_csv(failed_out, failed_rows, OUTPUT_COLUMNS)
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text(build_summary(parsed_rows, len(recipe_rows)), encoding="utf-8")

    status_counts = Counter(str(row["parse_status"]) for row in parsed_rows)
    grams_count = sum(1 for row in parsed_rows if row["quantity_grams_estimated"] != "")
    print(f"Total recipes parsed: {len(recipe_rows)}")
    print(f"Total ingredient rows: {len(parsed_rows)}")
    print(f"Rows with quantity_grams_estimated > 0: {grams_count}")
    print("Parse status counts:")
    for status, count in status_counts.most_common():
        print(f" - {status}: {count}")
    print(f"Written: {parsed_out}")
    print(f"Written: {summary_out}")
    print(f"Written: {review_out}")
    print(f"Written: {failed_out}")


if __name__ == "__main__":
    main()
