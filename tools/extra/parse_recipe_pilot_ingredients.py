import argparse
import csv
import json
import re
from collections import Counter
from fractions import Fraction
from pathlib import Path


DEFAULT_INPUT = Path("data/recipesdb/draft/recipes_pilot_subset_final.csv")
DEFAULT_EXPLODED_OUT = Path("data/recipesdb/draft/recipes_pilot_ingredients_exploded_raw.csv")
DEFAULT_PARSED_OUT = Path("data/recipesdb/draft/recipes_pilot_ingredients_parsed.csv")
DEFAULT_REVIEW_OUT = Path("data/recipesdb/audit/recipes_pilot_parse_review.csv")
DEFAULT_SUMMARY_OUT = Path("data/recipesdb/audit/recipes_pilot_parse_summary.txt")

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
    "pinches": "pinch",
    "dash": "dash",
    "dashes": "dash",
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

PACKAGING_UNITS = {
    "package",
    "packet",
    "can",
    "jar",
    "bottle",
    "box",
    "envelope",
}

OPTIONAL_RE = re.compile(r"\boptional\b", flags=re.IGNORECASE)
GARNISH_RE = re.compile(r"\bfor garnish\b|\bfor serving\b|\bto serve\b", flags=re.IGNORECASE)
ALTERNATIVE_RE = re.compile(
    r"\bor\b|\bto taste\b|\bas needed\b|\bas desired\b|\bif desired\b",
    flags=re.IGNORECASE,
)
ATYPICAL_REVIEW_RE = re.compile(
    r"\bcooking spray\b|\bnonstick cooking spray\b|\baluminum foil\b|\bfoil\b|\bparchment paper\b|\bwater to cover\b",
    flags=re.IGNORECASE,
)
PACKAGING_RE = re.compile(
    r"\bpackage\b|\bpackages\b|\bcan\b|\bcans\b|\bjar\b|\bbottle\b|\bbox\b|\bboxes\b|\bpacket\b|\bpackets\b|\benvelope\b",
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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parseaza conservator ingredientele din subsetul pilot Recipes_DB."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--out-exploded", default=str(DEFAULT_EXPLODED_OUT))
    parser.add_argument("--out-parsed", default=str(DEFAULT_PARSED_OUT))
    parser.add_argument("--out-review", default=str(DEFAULT_REVIEW_OUT))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY_OUT))
    return parser.parse_args()


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def normalize_for_parse(text):
    value = str(text or "")
    for source, target in FRACTION_MAP.items():
        value = value.replace(source, f" {target} ")
    value = value.replace("—", "-").replace("–", "-")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def parse_single_amount(text):
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


def parse_quantity_range(text):
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


def split_parenthetical_prefix(rest_text):
    rest = rest_text
    parenthetical_chunks = []

    while True:
        match = PAREN_RE.match(rest)
        if not match:
            break
        parenthetical_chunks.append(match.group(1).strip())
        rest = match.group(2).strip()

    return parenthetical_chunks, rest


def extract_quantity_and_unit(ingredient_raw):
    working = normalize_for_parse(ingredient_raw)
    quantity_raw = ""
    quantity_low = None
    quantity_high = None
    unit_raw = ""
    unit_normalized = ""
    parse_notes = []

    match = QUANTITY_RE.match(working)
    if not match:
        return {
            "quantity_raw": quantity_raw,
            "quantity_value_low": quantity_low,
            "quantity_value_high": quantity_high,
            "unit_raw": unit_raw,
            "unit_normalized": unit_normalized,
            "ingredient_body": working,
            "leading_parentheticals": [],
            "quantity_notes": parse_notes,
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


def remove_meta_phrases(text):
    value = text
    notes = []

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


def extract_leading_modifiers(primary_part, allow_safe_descriptor_cleanup):
    working = primary_part.strip()
    prep_parts = []
    derivation_notes = []

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


def derive_food_name_and_prep(ingredient_body, allow_safe_descriptor_cleanup=False):
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


def classify_status(is_section_header, food_name_candidate, flags, quantity_info, parse_notes):
    if is_section_header:
        return "review_needed"

    if not food_name_candidate:
        return "failed_parse"

    if "unparsed_quantity" in parse_notes:
        return "review_needed"

    if quantity_info["unit_raw"] and not quantity_info["unit_normalized"]:
        return "review_needed"

    if "mapping_risk_line" in parse_notes:
        return "review_needed"

    if flags["alternative_flag"] or flags["packaging_flag"]:
        return "review_needed"

    if flags["garnish_flag"]:
        return "review_needed"

    if flags["optional_flag"]:
        return "parsed_partial"

    if not quantity_info["quantity_raw"]:
        return "parsed_partial"

    if quantity_info["quantity_raw"] and not quantity_info["unit_normalized"]:
        return "parsed_partial"

    if quantity_info["leading_parentheticals"]:
        return "parsed_partial"

    if parse_notes:
        return "parsed_partial"

    return "parsed_clean"


def parse_ingredient_row(recipe_row, ingredient_index, ingredient_raw, section_name_raw):
    stripped_raw = str(ingredient_raw or "").strip()
    is_section_header = bool(SECTION_HEADER_RE.search(stripped_raw))

    optional_flag = int(bool(OPTIONAL_RE.search(stripped_raw)))
    garnish_flag = int(bool(GARNISH_RE.search(stripped_raw)))
    alternative_flag = int(bool(ALTERNATIVE_RE.search(stripped_raw)))
    packaging_flag = int(bool(PACKAGING_RE.search(stripped_raw)))

    parse_notes = []
    ingredient_text_clean = ""
    food_name_candidate = ""
    preparation_note = ""

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

    if is_section_header:
        parse_notes.append("section_header_row")
        ingredient_text_clean = stripped_raw.rstrip(":").strip()
    else:
        quantity_info = extract_quantity_and_unit(stripped_raw)
        if quantity_info["quantity_notes"]:
            parse_notes.extend(quantity_info["quantity_notes"])

        if quantity_info["leading_parentheticals"]:
            parse_notes.append("leading_parenthetical_note")

        if quantity_info["unit_normalized"] in PACKAGING_UNITS:
            packaging_flag = 1

        ingredient_text_clean, food_name_candidate, preparation_note, clean_notes, derivation_notes = derive_food_name_and_prep(
            quantity_info["ingredient_body"],
            allow_safe_descriptor_cleanup=bool(quantity_info["leading_parentheticals"]),
        )
        if clean_notes:
            parse_notes.extend(clean_notes)
        if derivation_notes:
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

    parse_notes = list(dict.fromkeys(note for note in parse_notes if note))
    parse_status = classify_status(
        is_section_header=is_section_header,
        food_name_candidate=food_name_candidate,
        flags={
            "optional_flag": optional_flag,
            "garnish_flag": garnish_flag,
            "alternative_flag": alternative_flag,
            "packaging_flag": packaging_flag,
        },
        quantity_info=quantity_info,
        parse_notes=parse_notes,
    )

    if parse_status == "failed_parse" and "empty_food_name_candidate" not in parse_notes:
        parse_notes.append("empty_food_name_candidate")

    return {
        "recipe_source_id": recipe_row["final_recipe_id"],
        "recipe_title_raw": recipe_row["recipe_title"],
        "category_raw": recipe_row["category"],
        "subcategory_raw": recipe_row["subcategory"],
        "ingredient_index": ingredient_index,
        "ingredient_raw": stripped_raw,
        "section_name_raw": section_name_raw,
        "is_section_header": int(is_section_header),
        "quantity_raw": quantity_info["quantity_raw"],
        "quantity_value_low": quantity_info["quantity_value_low"],
        "quantity_value_high": quantity_info["quantity_value_high"],
        "unit_raw": quantity_info["unit_raw"],
        "unit_normalized": quantity_info["unit_normalized"],
        "ingredient_text_clean": ingredient_text_clean,
        "food_name_candidate": food_name_candidate,
        "preparation_note": preparation_note,
        "optional_flag": optional_flag,
        "garnish_flag": garnish_flag,
        "alternative_flag": alternative_flag,
        "packaging_flag": packaging_flag,
        "parse_status": parse_status,
        "parse_notes": "; ".join(parse_notes),
    }


def main():
    args = parse_args()

    input_path = Path(args.input)
    exploded_out = Path(args.out_exploded)
    parsed_out = Path(args.out_parsed)
    review_out = Path(args.out_review)
    summary_out = Path(args.out_summary)

    exploded_rows = []
    parsed_rows = []
    review_rows = []
    parse_status_counts = Counter()
    unit_counts = Counter()
    review_reason_counts = Counter()

    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for recipe_row in reader:
            ingredients = json.loads(recipe_row["ingredients_json"])
            current_section_name = ""

            for ingredient_index, ingredient_raw in enumerate(ingredients, start=1):
                stripped_raw = str(ingredient_raw or "").strip()
                is_section_header = int(bool(SECTION_HEADER_RE.search(stripped_raw)))

                if is_section_header:
                    current_section_name = stripped_raw.rstrip(":").strip()

                exploded_rows.append(
                    {
                        "recipe_source_id": recipe_row["final_recipe_id"],
                        "recipe_title_raw": recipe_row["recipe_title"],
                        "category_raw": recipe_row["category"],
                        "subcategory_raw": recipe_row["subcategory"],
                        "ingredient_index": ingredient_index,
                        "ingredient_raw": stripped_raw,
                        "section_name_raw": current_section_name,
                        "is_section_header": is_section_header,
                    }
                )

                parsed_row = parse_ingredient_row(
                    recipe_row=recipe_row,
                    ingredient_index=ingredient_index,
                    ingredient_raw=stripped_raw,
                    section_name_raw=current_section_name,
                )

                parsed_rows.append(parsed_row)
                parse_status_counts[parsed_row["parse_status"]] += 1

                if parsed_row["unit_raw"]:
                    unit_counts[parsed_row["unit_raw"].lower()] += 1

                if parsed_row["parse_status"] in {"review_needed", "failed_parse"}:
                    review_rows.append(parsed_row)
                    notes = [note.strip() for note in parsed_row["parse_notes"].split(";") if note.strip()]
                    for note in notes:
                        review_reason_counts[note] += 1

    fieldnames_exploded = [
        "recipe_source_id",
        "recipe_title_raw",
        "category_raw",
        "subcategory_raw",
        "ingredient_index",
        "ingredient_raw",
        "section_name_raw",
        "is_section_header",
    ]

    fieldnames_parsed = [
        "recipe_source_id",
        "recipe_title_raw",
        "category_raw",
        "subcategory_raw",
        "ingredient_index",
        "ingredient_raw",
        "section_name_raw",
        "is_section_header",
        "quantity_raw",
        "quantity_value_low",
        "quantity_value_high",
        "unit_raw",
        "unit_normalized",
        "ingredient_text_clean",
        "food_name_candidate",
        "preparation_note",
        "optional_flag",
        "garnish_flag",
        "alternative_flag",
        "packaging_flag",
        "parse_status",
        "parse_notes",
    ]

    write_csv(exploded_out, exploded_rows, fieldnames_exploded)
    write_csv(parsed_out, parsed_rows, fieldnames_parsed)
    write_csv(review_out, review_rows, fieldnames_parsed)

    summary_lines = [
        "Recipes pilot parse summary",
        "",
        f"Total exploded ingredient rows: {len(parsed_rows)}",
        "",
        "Parse status counts:",
    ]

    for status, count in parse_status_counts.most_common():
        summary_lines.append(f"- {status}: {count}")

    summary_lines.extend(
        [
            "",
            "Top unit_raw values:",
        ]
    )

    for unit_raw, count in unit_counts.most_common(20):
        summary_lines.append(f"- {unit_raw}: {count}")

    summary_lines.extend(
        [
            "",
            "Top review reasons:",
        ]
    )

    for note, count in review_reason_counts.most_common(20):
        summary_lines.append(f"- {note}: {count}")

    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"Total exploded ingredient rows: {len(parsed_rows)}")
    print("Parse status counts:")
    for status, count in parse_status_counts.most_common():
        print(f" - {status}: {count}")
    print("Top review reasons:")
    for note, count in review_reason_counts.most_common(10):
        print(f" - {note}: {count}")
    print(f"Written: {exploded_out}")
    print(f"Written: {parsed_out}")
    print(f"Written: {review_out}")
    print(f"Written: {summary_out}")


if __name__ == "__main__":
    main()
