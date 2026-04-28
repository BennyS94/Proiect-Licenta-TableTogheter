import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


DEFAULT_RECIPES_INPUT = Path("data/recipesdb/current/recipes_pilot_subset_final.csv")
DEFAULT_INGREDIENTS_INPUT = Path("data/recipesdb/current/recipes_pilot_ingredients_parsed.csv")
DEFAULT_MAPPING_INPUT = Path("data/recipesdb/current/recipe_ingredient_food_matches_draft.csv")
DEFAULT_FOODDB_INPUT = Path("data/fooddb/current/fooddb_v1_core_master_draft.csv")

DEFAULT_RECIPES_OUT = Path("data/recipesdb/current/recipes.csv")
DEFAULT_RECIPE_INGREDIENTS_OUT = Path("data/recipesdb/current/recipe_ingredients.csv")
DEFAULT_NUTRITION_OUT = Path("data/recipesdb/current/recipe_nutrition_cache.csv")
DEFAULT_COMPONENTS_OUT = Path("data/recipesdb/current/recipe_components.csv")

SOURCE_DATASET = "recipes_dataset_64k_dishes_pilot"
CACHE_VERSION = "pilot_v1"

RECIPES_COLUMNS = [
    "recipe_id",
    "source_recipe_id",
    "source_dataset",
    "recipe_name",
    "display_name",
    "recipe_family_name",
    "recipe_kind",
    "recipe_category",
    "recipe_subcategory",
    "recipe_cuisine",
    "directions_json",
    "directions_step_count",
    "servings_declared",
    "servings_normalized",
    "prep_time_min",
    "cook_time_min",
    "total_time_min",
    "difficulty_level",
    "scope_status",
    "has_ingredients_parsed",
    "has_nutrition_cache",
    "is_pilot_recipe",
    "is_active",
    "qc_recipe_status",
    "qc_notes",
]

RECIPE_INGREDIENT_COLUMNS = [
    "recipe_ingredient_id",
    "recipe_id",
    "ingredient_position",
    "ingredient_raw_text",
    "ingredient_name_parsed",
    "ingredient_name_normalized",
    "quantity_value",
    "quantity_unit",
    "quantity_text",
    "quantity_grams_estimated",
    "ingredient_role",
    "ingredient_slot_key",
    "is_optional",
    "is_substitutable",
    "substitution_group_id",
    "mapped_food_id",
    "mapped_food_canonical_name",
    "mapping_status",
    "mapping_confidence",
    "mapping_method",
    "qc_ingredient_status",
    "qc_notes",
]

NUTRITION_COLUMNS = [
    "recipe_id",
    "nutrition_basis",
    "servings_basis",
    "total_weight_grams_estimated",
    "energy_kcal_total",
    "protein_g_total",
    "carbs_g_total",
    "fat_g_total",
    "fibre_g_total",
    "sugars_g_total",
    "salt_g_total",
    "water_g_total",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "fibre_g_per_serving",
    "sugars_g_per_serving",
    "salt_g_per_serving",
    "water_g_per_serving",
    "mapped_ingredient_count",
    "unmapped_ingredient_count",
    "mapped_weight_ratio",
    "cache_status",
    "cache_version",
    "qc_notes",
]

COMPONENT_COLUMNS = [
    "recipe_component_id",
    "parent_recipe_id",
    "child_recipe_id",
    "component_role",
    "component_slot_key",
    "quantity_g_per_serving",
    "is_optional",
    "is_substitutable",
    "substitution_group_id",
    "sort_order",
    "qc_status",
    "notes",
]

DIRECT_WEIGHT_UNITS = {
    "g": 1.0,
    "gram": 1.0,
    "grams": 1.0,
    "kg": 1000.0,
    "kilogram": 1000.0,
    "kilograms": 1000.0,
    "ounce": 28.3495,
    "ounces": 28.3495,
    "oz": 28.3495,
    "pound": 453.592,
    "pounds": 453.592,
    "lb": 453.592,
    "lbs": 453.592,
}

HOUSEHOLD_WEIGHT_LOOKUP = {
    "black_pepper_powder": {"teaspoon": 2.3, "tablespoon": 6.9, "pinch": 0.4},
    "paprika": {"teaspoon": 2.3},
    "garlic_powder_dried": {"teaspoon": 3.1, "tablespoon": 9.3},
    "basil_dried": {"teaspoon": 0.7},
    "oregano_dried": {"teaspoon": 1.0},
    "thyme_dried": {"teaspoon": 1.0},
    "parsley_dried": {"teaspoon": 0.5, "tablespoon": 1.5},
    "cayenne_pepper": {"teaspoon": 1.8, "tablespoon": 5.4},
    "cinnamon_powder": {"teaspoon": 2.6},
    "curry_powder": {"teaspoon": 2.0},
    "celery_salt": {"teaspoon": 4.0},
    "soy_sauce_prepacked": {"teaspoon": 5.0, "tablespoon": 15.0, "cup": 240.0},
    "honey": {"teaspoon": 7.0, "tablespoon": 21.0},
    "sesame_oil": {"teaspoon": 4.5, "tablespoon": 13.5, "cup": 216.0},
    "vinegar_balsamic": {"teaspoon": 5.0, "tablespoon": 15.0, "cup": 240.0},
    "vinegar": {"cup": 240.0},
    "red_onion_raw": {"cup": 150.0},
    "yellow_onion_raw": {"cup": 150.0},
    "lemon_zest_raw": {"teaspoon": 2.0},
    "poppy_seed": {"tablespoon": 9.0},
}

CUISINE_KEYWORDS = {
    "mexican": "Mexican",
    "italian": "Italian",
    "filipino": "Filipino",
    "greek": "Greek",
    "thai": "Thai",
    "indian": "Indian",
    "mediterranean": "Mediterranean",
    "asian": "Asian",
    "japanese": "Japanese",
    "korean": "Korean",
    "chinese": "Chinese",
    "french": "French",
    "southern": "Southern US",
}

UNICODE_FRACTIONS = {
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
}

TIME_RANGE_PATTERN = re.compile(
    r"(?P<low>\d+(?:\.\d+)?(?:\s+\d+/\d+)?|\d+/\d+)\s*(?:to|-)\s*(?P<high>\d+(?:\.\d+)?(?:\s+\d+/\d+)?|\d+/\d+)\s*(?P<unit>hours?|hrs?|minutes?|mins?)",
    re.IGNORECASE,
)
TIME_HOUR_MINUTE_PATTERN = re.compile(
    r"(?P<hours>\d+(?:\.\d+)?(?:\s+\d+/\d+)?|\d+/\d+)\s*(?:hours?|hrs?)\s*(?:and\s+)?(?P<minutes>\d+(?:\.\d+)?(?:\s+\d+/\d+)?|\d+/\d+)?\s*(?:minutes?|mins?)?",
    re.IGNORECASE,
)
TIME_SINGLE_PATTERN = re.compile(
    r"(?:about\s+)?(?P<value>\d+(?:\.\d+)?(?:\s+\d+/\d+)?|\d+/\d+)\s*(?P<unit>hours?|hrs?|minutes?|mins?)",
    re.IGNORECASE,
)

COOK_VERBS = {
    "bake",
    "roast",
    "simmer",
    "boil",
    "cook",
    "fry",
    "grill",
    "broil",
    "saute",
    "sautee",
    "steam",
    "air fry",
    "air-fry",
    "preheat",
}

PASSIVE_PREP_VERBS = {
    "chill",
    "marinate",
    "refrigerate",
    "rest",
    "stand",
    "cool",
    "soak",
    "let sit",
}


def read_csv_rows(path: Path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def normalize_name(text: str) -> str:
    cleaned = normalize_spaces((text or "").lower())
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def normalize_family_name(text: str) -> str:
    cleaned = normalize_spaces((text or "").lower())
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return normalize_spaces(cleaned)


def as_int_flag(value) -> str:
    text = str(value or "").strip().lower()
    return "1" if text in {"1", "true", "yes", "y"} else "0"


def parse_float(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def format_number(value):
    if value is None:
        return ""
    text = f"{value:.4f}"
    text = text.rstrip("0").rstrip(".")
    return text or "0"


def parse_numeric_token(text: str) -> float | None:
    raw = normalize_spaces(text)
    if not raw:
        return None
    if " " in raw:
        parts = raw.split()
        if len(parts) == 2 and "/" in parts[1]:
            whole = parse_numeric_token(parts[0])
            frac = parse_numeric_token(parts[1])
            if whole is not None and frac is not None:
                return whole + frac
    if "/" in raw:
        try:
            numerator, denominator = raw.split("/", 1)
            return float(numerator) / float(denominator)
        except (ValueError, ZeroDivisionError):
            return None
    try:
        return float(raw)
    except ValueError:
        return None


def normalize_time_text(text: str) -> str:
    normalized = text or ""
    for source, target in UNICODE_FRACTIONS.items():
        normalized = normalized.replace(source, f" {target}")
    normalized = normalized.replace("\u2013", "-").replace("\u2014", "-")
    normalized = normalized.replace("–", "-").replace("—", "-")
    normalized = normalize_spaces(normalized.lower())
    return normalized


def convert_to_minutes(value: float | None, unit: str) -> float | None:
    if value is None:
        return None
    unit_text = (unit or "").lower()
    if unit_text.startswith("hour") or unit_text.startswith("hr"):
        return value * 60.0
    return value


def extract_step_minutes(step_text: str) -> float | None:
    text = normalize_time_text(step_text)
    candidates = []

    if "overnight" in text:
        candidates.append(480.0)

    for match in TIME_RANGE_PATTERN.finditer(text):
        upper = parse_numeric_token(match.group("high"))
        minutes = convert_to_minutes(upper, match.group("unit"))
        if minutes is not None:
            candidates.append(minutes)

    for match in TIME_HOUR_MINUTE_PATTERN.finditer(text):
        hours = parse_numeric_token(match.group("hours"))
        minutes_part = parse_numeric_token(match.group("minutes") or "")
        total_minutes = 0.0
        if hours is not None:
            total_minutes += hours * 60.0
        if minutes_part is not None:
            total_minutes += minutes_part
        if total_minutes > 0:
            candidates.append(total_minutes)

    for match in TIME_SINGLE_PATTERN.finditer(text):
        value = parse_numeric_token(match.group("value"))
        minutes = convert_to_minutes(value, match.group("unit"))
        if minutes is not None:
            candidates.append(minutes)

    if not candidates:
        return None
    return max(candidates)


def classify_time_step(step_text: str) -> str:
    text = normalize_time_text(step_text)
    if any(verb in text for verb in COOK_VERBS):
        return "cook"
    if any(verb in text for verb in PASSIVE_PREP_VERBS):
        return "prep"
    return "prep"


def has_long_passive_time(step_text: str, minutes_value: float | None) -> bool:
    text = normalize_time_text(step_text)
    if "overnight" in text:
        return True
    if not any(verb in text for verb in {"chill", "marinate", "refrigerate", "soak", "let sit"}):
        return False
    return minutes_value is not None and minutes_value >= 60.0


def derive_time_fields(row) -> tuple[str, str, str, list[str]]:
    notes = []
    prep_minutes = 0.0
    cook_minutes = 0.0
    time_found = False
    long_passive = False

    try:
        steps = json.loads(row.get("directions_json") or "[]")
    except json.JSONDecodeError:
        steps = []

    for step in steps:
        minutes_value = extract_step_minutes(step)
        if minutes_value is None:
            continue
        time_found = True
        if classify_time_step(step) == "cook":
            cook_minutes += minutes_value
        else:
            prep_minutes += minutes_value
        if has_long_passive_time(step, minutes_value):
            long_passive = True

    if time_found:
        notes.append("time_estimated_from_directions_v1")
        if long_passive:
            notes.append("long_passive_time_estimated")
        total_minutes = prep_minutes + cook_minutes
        return (
            format_number(prep_minutes),
            format_number(cook_minutes),
            format_number(total_minutes),
            notes,
        )

    steps_count = parse_float(row.get("num_steps"))
    if steps_count is not None and steps_count > 0:
        prep_minutes = max(5.0, steps_count * 5.0)
        notes.append("time_estimated_from_step_count_fallback_v1")
        total_minutes = prep_minutes
        return (
            format_number(prep_minutes),
            "",
            format_number(total_minutes),
            notes,
        )

    notes.append("time_not_available")
    return "", "", "", notes


def derive_recipe_cuisine(category: str, subcategory: str) -> str:
    haystack = f"{category} {subcategory}".lower()
    for key, label in CUISINE_KEYWORDS.items():
        if key in haystack:
            return label
    return ""


def derive_recipe_status(row, time_notes: list[str]) -> tuple[str, str]:
    notes = []
    if not str(row.get("description") or "").strip():
        notes.append("description_missing")
    notes.append("servings_not_available")
    notes.extend(time_notes)
    return "pilot_materialized_with_missing_metadata", "; ".join(notes)


def pick_quantity_value(row) -> tuple[float | None, list[str]]:
    notes = []
    low = parse_float(row.get("quantity_value_low"))
    high = parse_float(row.get("quantity_value_high"))
    if low is None and high is None:
        return None, notes
    if low is not None and high is not None and abs(low - high) > 1e-9:
        notes.append(f"quantity_range_present:{format_number(low)}-{format_number(high)}")
        return None, notes
    return low if low is not None else high, notes


def estimate_weight_grams(
    quantity_value: float | None,
    unit_normalized: str,
    mapped_canonical_name: str,
) -> tuple[float | None, list[str]]:
    notes = []
    unit = normalize_spaces((unit_normalized or "").lower())
    if quantity_value is None:
        return None, notes
    if not unit:
        notes.append("weight_not_estimated:missing_unit")
        return None, notes
    if unit not in DIRECT_WEIGHT_UNITS:
        canonical_name = normalize_spaces((mapped_canonical_name or "").lower())
        canonical_lookup = HOUSEHOLD_WEIGHT_LOOKUP.get(canonical_name, {})
        if unit in canonical_lookup:
            notes.append("weight_estimated:household_measure_lookup")
            return quantity_value * canonical_lookup[unit], notes
        notes.append(f"weight_not_estimated:unit_{unit}")
        return None, notes
    return quantity_value * DIRECT_WEIGHT_UNITS[unit], notes


def derive_ingredient_role(row) -> str:
    if as_int_flag(row.get("is_section_header")) == "1":
        return "section_header"
    if as_int_flag(row.get("garnish_flag")) == "1":
        return "garnish"
    if as_int_flag(row.get("optional_flag")) == "1":
        return "optional_ingredient"
    return "ingredient"


def derive_qc_ingredient_status(row) -> str:
    if as_int_flag(row.get("is_section_header")) == "1":
        return "section_header"
    if row.get("parse_status") == "review_needed":
        return "parse_review_needed"
    if row.get("mapping_status") == "accepted_auto":
        return "mapped_auto"
    if row.get("mapping_status") == "review_needed":
        return "mapping_review_needed"
    return "unmapped"


def combine_notes(*note_groups):
    notes = []
    for note_group in note_groups:
        if not note_group:
            continue
        if isinstance(note_group, str):
            text = note_group.strip()
            if text:
                notes.append(text)
            continue
        for item in note_group:
            text = str(item or "").strip()
            if text:
                notes.append(text)
    seen = []
    for note in notes:
        if note not in seen:
            seen.append(note)
    return "; ".join(seen)


def build_recipes_rows(recipe_rows):
    output = []
    for row in recipe_rows:
        prep_time_min, cook_time_min, total_time_min, time_notes = derive_time_fields(row)
        qc_status, qc_notes = derive_recipe_status(row, time_notes)
        output.append(
            {
                "recipe_id": row["final_recipe_id"],
                "source_recipe_id": row["source_row_number"],
                "source_dataset": SOURCE_DATASET,
                "recipe_name": row["recipe_title"],
                "display_name": row["recipe_title"],
                "recipe_family_name": normalize_family_name(row["recipe_title"]),
                "recipe_kind": row.get("recipe_kind_guess", ""),
                "recipe_category": row.get("category", ""),
                "recipe_subcategory": row.get("subcategory", ""),
                "recipe_cuisine": derive_recipe_cuisine(row.get("category", ""), row.get("subcategory", "")),
                "directions_json": row.get("directions_json", ""),
                "directions_step_count": row.get("num_steps", ""),
                "servings_declared": "",
                "servings_normalized": "",
                "prep_time_min": prep_time_min,
                "cook_time_min": cook_time_min,
                "total_time_min": total_time_min,
                "difficulty_level": "",
                "scope_status": "pilot_validated",
                "has_ingredients_parsed": "1",
                "has_nutrition_cache": "1",
                "is_pilot_recipe": "1",
                "is_active": "1",
                "qc_recipe_status": qc_status,
                "qc_notes": qc_notes,
            }
        )
    return output


def build_recipe_ingredient_rows(mapping_rows):
    output = []
    grouped = defaultdict(list)

    for row in mapping_rows:
        recipe_id = row["recipe_source_id"]
        quantity_value, quantity_notes = pick_quantity_value(row)
        estimated_grams, weight_notes = estimate_weight_grams(
            quantity_value,
            row.get("unit_normalized", ""),
            row.get("matched_canonical_name", ""),
        )
        ingredient_name_parsed = row.get("food_name_candidate") or row.get("ingredient_text_clean") or row.get("ingredient_raw")
        normalized_name = normalize_name(ingredient_name_parsed)
        qc_notes = combine_notes(
            row.get("parse_notes", ""),
            row.get("mapping_notes", ""),
            quantity_notes,
            weight_notes,
        )
        ingredient_row = {
            "recipe_ingredient_id": f"{recipe_id}__ing_{int(row['ingredient_index']):03d}",
            "recipe_id": recipe_id,
            "ingredient_position": row["ingredient_index"],
            "ingredient_raw_text": row.get("ingredient_raw", ""),
            "ingredient_name_parsed": ingredient_name_parsed,
            "ingredient_name_normalized": normalized_name,
            "quantity_value": format_number(quantity_value),
            "quantity_unit": row.get("unit_normalized", "") or row.get("unit_raw", ""),
            "quantity_text": row.get("quantity_raw", ""),
            "quantity_grams_estimated": format_number(estimated_grams),
            "ingredient_role": derive_ingredient_role(row),
            "ingredient_slot_key": normalized_name,
            "is_optional": as_int_flag(row.get("optional_flag")),
            "is_substitutable": "",
            "substitution_group_id": "",
            "mapped_food_id": row.get("matched_food_id", ""),
            "mapped_food_canonical_name": row.get("matched_canonical_name", ""),
            "mapping_status": row.get("mapping_status", ""),
            "mapping_confidence": row.get("match_confidence", ""),
            "mapping_method": row.get("match_method", ""),
            "qc_ingredient_status": derive_qc_ingredient_status(row),
            "qc_notes": qc_notes,
        }
        output.append(ingredient_row)
        grouped[recipe_id].append(ingredient_row)

    return output, grouped


def sum_nutrient(food_row, nutrient_key: str, grams: float) -> float | None:
    nutrient_value = parse_float(food_row.get(nutrient_key))
    if nutrient_value is None:
        return None
    return nutrient_value * grams / 100.0


def resolve_servings_basis(recipe_cache_row, recipe_lookup_row):
    existing_basis = parse_float(recipe_cache_row.get("servings_basis"))
    if existing_basis is not None and existing_basis > 0:
        return existing_basis, "servings_basis_from_existing_value"

    normalized_basis = parse_float(recipe_lookup_row.get("servings_normalized"))
    if normalized_basis is not None and normalized_basis > 0:
        return normalized_basis, "servings_basis_from_recipes_servings_normalized"

    return None, ""


def build_nutrition_rows(recipe_rows, ingredients_by_recipe, food_lookup):
    output = []
    status_counter = Counter()
    recipe_lookup = {row["final_recipe_id"]: row for row in recipe_rows}

    for recipe_row in recipe_rows:
        recipe_id = recipe_row["final_recipe_id"]
        ingredient_rows = ingredients_by_recipe.get(recipe_id, [])
        base_rows = [row for row in ingredient_rows if row["ingredient_role"] != "section_header"]

        totals = {
            "energy_kcal_total": 0.0,
            "protein_g_total": 0.0,
            "carbs_g_total": 0.0,
            "fat_g_total": 0.0,
            "fibre_g_total": 0.0,
            "sugars_g_total": 0.0,
            "salt_g_total": 0.0,
            "water_g_total": 0.0,
        }
        optional_seen = {key: False for key in totals if key not in {"energy_kcal_total", "protein_g_total", "carbs_g_total", "fat_g_total"}}

        mapped_ingredient_count = 0
        unmapped_ingredient_count = 0
        mapped_weight = 0.0
        total_estimated_weight = 0.0
        notes = ["derived_from_mapped_ingredients_only"]

        for ingredient_row in base_rows:
            grams = parse_float(ingredient_row["quantity_grams_estimated"])
            if grams is not None:
                total_estimated_weight += grams

            if ingredient_row["mapping_status"] != "accepted_auto":
                unmapped_ingredient_count += 1
                continue

            mapped_ingredient_count += 1
            if grams is None:
                notes.append("mapped_rows_without_weight_estimate_present")
                continue

            food_row = food_lookup.get(ingredient_row["mapped_food_id"])
            if not food_row:
                notes.append("mapped_food_missing_from_fooddb_snapshot")
                continue

            mapped_weight += grams

            required_map = {
                "energy_kcal_total": "energy_kcal_100g",
                "protein_g_total": "protein_g_100g",
                "carbs_g_total": "carbs_g_100g",
                "fat_g_total": "fat_g_100g",
            }
            optional_map = {
                "fibre_g_total": "fibre_g_100g",
                "sugars_g_total": "sugars_g_100g",
                "salt_g_total": "salt_g_100g",
                "water_g_total": "water_g_100g",
            }

            for out_key, food_key in required_map.items():
                contribution = sum_nutrient(food_row, food_key, grams)
                if contribution is not None:
                    totals[out_key] += contribution

            for out_key, food_key in optional_map.items():
                contribution = sum_nutrient(food_row, food_key, grams)
                if contribution is not None:
                    totals[out_key] += contribution
                    optional_seen[out_key] = True

        if mapped_ingredient_count == 0:
            cache_status = "no_accepted_mapped_ingredients"
            notes.append("nutrition_totals_empty")
        elif mapped_weight == 0:
            cache_status = "mapped_without_weight_estimates"
            notes.append("weight_estimates_available_for_mass_units_only")
        else:
            cache_status = "partial_from_mapped_ingredients"
            if unmapped_ingredient_count == 0:
                cache_status = "mapped_only_pilot_complete"
            elif total_estimated_weight > 0 and mapped_weight / total_estimated_weight < 0.5:
                notes.append("low_weight_coverage")

        mapped_weight_ratio = ""
        if total_estimated_weight > 0:
            mapped_weight_ratio = format_number(mapped_weight / total_estimated_weight)

        totals_present = all(
            format_number(totals[key]) != ""
            for key in ["energy_kcal_total", "protein_g_total", "carbs_g_total", "fat_g_total"]
        ) and mapped_weight > 0

        servings_basis_value = None
        servings_basis_note = ""
        if totals_present:
            servings_basis_value, servings_basis_note = resolve_servings_basis({}, recipe_lookup[recipe_id])
            if servings_basis_value is None:
                servings_basis_value = 1.0
                servings_basis_note = "servings_basis_pilot_fallback_1"

        if servings_basis_note:
            notes.append(servings_basis_note)
        elif totals_present:
            notes.append("servings_basis_missing_per_serving_left_empty")

        per_serving_values = {}
        if totals_present and servings_basis_value is not None and servings_basis_value > 0:
            for total_key, per_serving_key in [
                ("energy_kcal_total", "energy_kcal_per_serving"),
                ("protein_g_total", "protein_g_per_serving"),
                ("carbs_g_total", "carbs_g_per_serving"),
                ("fat_g_total", "fat_g_per_serving"),
                ("fibre_g_total", "fibre_g_per_serving"),
                ("sugars_g_total", "sugars_g_per_serving"),
                ("salt_g_total", "salt_g_per_serving"),
                ("water_g_total", "water_g_per_serving"),
            ]:
                total_value = parse_float(format_number(totals[total_key]))
                if total_value is not None:
                    per_serving_values[per_serving_key] = format_number(total_value / servings_basis_value)
                else:
                    per_serving_values[per_serving_key] = ""
        else:
            per_serving_values = {
                "energy_kcal_per_serving": "",
                "protein_g_per_serving": "",
                "carbs_g_per_serving": "",
                "fat_g_per_serving": "",
                "fibre_g_per_serving": "",
                "sugars_g_per_serving": "",
                "salt_g_per_serving": "",
                "water_g_per_serving": "",
            }

        if not totals_present:
            notes.append("servings_unknown_per_serving_left_empty")

        nutrition_row = {
            "recipe_id": recipe_id,
            "nutrition_basis": "whole_recipe_estimated_from_accepted_mapped_ingredients",
            "servings_basis": format_number(servings_basis_value),
            "total_weight_grams_estimated": format_number(mapped_weight) if mapped_weight > 0 else "",
            "energy_kcal_total": format_number(totals["energy_kcal_total"]) if mapped_weight > 0 else "",
            "protein_g_total": format_number(totals["protein_g_total"]) if mapped_weight > 0 else "",
            "carbs_g_total": format_number(totals["carbs_g_total"]) if mapped_weight > 0 else "",
            "fat_g_total": format_number(totals["fat_g_total"]) if mapped_weight > 0 else "",
            "fibre_g_total": format_number(totals["fibre_g_total"]) if optional_seen["fibre_g_total"] and mapped_weight > 0 else "",
            "sugars_g_total": format_number(totals["sugars_g_total"]) if optional_seen["sugars_g_total"] and mapped_weight > 0 else "",
            "salt_g_total": format_number(totals["salt_g_total"]) if optional_seen["salt_g_total"] and mapped_weight > 0 else "",
            "water_g_total": format_number(totals["water_g_total"]) if optional_seen["water_g_total"] and mapped_weight > 0 else "",
            "energy_kcal_per_serving": per_serving_values["energy_kcal_per_serving"],
            "protein_g_per_serving": per_serving_values["protein_g_per_serving"],
            "carbs_g_per_serving": per_serving_values["carbs_g_per_serving"],
            "fat_g_per_serving": per_serving_values["fat_g_per_serving"],
            "fibre_g_per_serving": per_serving_values["fibre_g_per_serving"],
            "sugars_g_per_serving": per_serving_values["sugars_g_per_serving"],
            "salt_g_per_serving": per_serving_values["salt_g_per_serving"],
            "water_g_per_serving": per_serving_values["water_g_per_serving"],
            "mapped_ingredient_count": str(mapped_ingredient_count),
            "unmapped_ingredient_count": str(unmapped_ingredient_count),
            "mapped_weight_ratio": mapped_weight_ratio,
            "cache_status": cache_status,
            "cache_version": CACHE_VERSION,
            "qc_notes": combine_notes(notes),
        }
        output.append(nutrition_row)
        status_counter[cache_status] += 1

    return output, status_counter


def main():
    parser = argparse.ArgumentParser(description="Materializeaza tabelele pilot pentru Recipes_DB.")
    parser.add_argument("--recipes-input", type=Path, default=DEFAULT_RECIPES_INPUT)
    parser.add_argument("--ingredients-input", type=Path, default=DEFAULT_INGREDIENTS_INPUT)
    parser.add_argument("--mapping-input", type=Path, default=DEFAULT_MAPPING_INPUT)
    parser.add_argument("--fooddb-input", type=Path, default=DEFAULT_FOODDB_INPUT)
    parser.add_argument("--recipes-out", type=Path, default=DEFAULT_RECIPES_OUT)
    parser.add_argument("--recipe-ingredients-out", type=Path, default=DEFAULT_RECIPE_INGREDIENTS_OUT)
    parser.add_argument("--nutrition-out", type=Path, default=DEFAULT_NUTRITION_OUT)
    parser.add_argument("--components-out", type=Path, default=DEFAULT_COMPONENTS_OUT)
    args = parser.parse_args()

    recipe_rows = read_csv_rows(args.recipes_input)
    parsed_rows = read_csv_rows(args.ingredients_input)
    mapping_rows = read_csv_rows(args.mapping_input)
    food_rows = read_csv_rows(args.fooddb_input)

    if len(parsed_rows) != len(mapping_rows):
        raise ValueError("Parsed ingredients si mapping draft nu au acelasi numar de randuri.")

    food_lookup = {row["food_id"]: row for row in food_rows}

    recipes_rows = build_recipes_rows(recipe_rows)
    recipe_ingredient_rows, ingredients_by_recipe = build_recipe_ingredient_rows(mapping_rows)
    nutrition_rows, nutrition_status_counter = build_nutrition_rows(recipe_rows, ingredients_by_recipe, food_lookup)

    write_csv_rows(args.recipes_out, RECIPES_COLUMNS, recipes_rows)
    write_csv_rows(args.recipe_ingredients_out, RECIPE_INGREDIENT_COLUMNS, recipe_ingredient_rows)
    write_csv_rows(args.nutrition_out, NUTRITION_COLUMNS, nutrition_rows)
    write_csv_rows(args.components_out, COMPONENT_COLUMNS, [])

    recipe_count = len(recipes_rows)
    ingredient_count = len(recipe_ingredient_rows)
    mapped_count = sum(1 for row in recipe_ingredient_rows if row["mapping_status"] == "accepted_auto")
    unmapped_count = sum(1 for row in recipe_ingredient_rows if row["mapping_status"] == "unmapped")
    review_count = sum(1 for row in recipe_ingredient_rows if row["mapping_status"] == "review_needed")

    print(f"recipes_rows={recipe_count}")
    print(f"recipe_ingredients_rows={ingredient_count}")
    print(f"recipe_nutrition_rows={len(nutrition_rows)}")
    print("recipe_components_rows=0")
    print(f"accepted_auto_ingredients={mapped_count}")
    print(f"review_needed_ingredients={review_count}")
    print(f"unmapped_ingredients={unmapped_count}")
    print(f"nutrition_status_counts={dict(nutrition_status_counter)}")


if __name__ == "__main__":
    main()
