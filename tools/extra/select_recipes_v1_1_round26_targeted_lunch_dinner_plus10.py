from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import Counter
from pathlib import Path


SOURCE_RECIPES = Path("data/recipesdb/source/1_Recipe_csv.csv")
CURATED_200 = Path("data/recipesdb/draft/recipes_v1_1_curated_200.csv")
SNACK_CURATED_RECIPES = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated/recipes.csv"
)
ROUND25_ALTERNATIVES = Path(
    "data/recipesdb/audit/generator_v1_round25_main_meal_alternative_quality.csv"
)
ROUND25_RECOMMENDATION = Path(
    "data/recipesdb/audit/generator_v1_round25_recommendation.txt"
)

OUT_SELECTED = Path("data/recipesdb/draft/recipes_v1_1_round26_targeted_plus10.csv")
OUT_SUMMARY = Path("data/recipesdb/audit/recipes_v1_1_round26_targeted_plus10_summary.txt")
OUT_EXCLUSION = Path(
    "data/recipesdb/audit/recipes_v1_1_round26_targeted_plus10_exclusion_log.csv"
)

OUTPUT_COLUMNS = [
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "source_category",
    "source_subcategory",
    "recipe_kind_guess",
    "primary_protein",
    "has_carb_component",
    "has_veg_component",
    "expected_mapping_difficulty",
    "expected_generator_value",
    "selection_reason",
    "risk_notes",
    "selection_score",
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
    "exclusion_reason",
    "primary_protein",
    "has_carb_component",
    "has_veg_component",
    "num_ingredients",
    "num_steps",
]

TARGET_QUOTAS = {
    "chicken": 2,
    "fish": 2,
    "beef": 2,
    "pork": 1,
    "turkey": 1,
    "vegetarian": 2,
}

PROTEIN_KEYWORDS = {
    "chicken": [
        "chicken breast",
        "chicken breasts",
        "chicken thighs",
        "chicken thigh",
        "ground chicken",
        "chicken",
    ],
    "fish": [
        "canned tuna",
        "tuna",
        "canned salmon",
        "salmon",
        "cod",
        "fish fillet",
        "fish",
    ],
    "beef": [
        "ground beef",
        "lean ground beef",
        "beef sirloin",
        "sirloin steak",
        "beef steak",
        "beef",
    ],
    "pork": [
        "pork tenderloin",
        "pork chop",
        "pork chops",
        "pork loin",
        "ground pork",
        "pork",
    ],
    "turkey": [
        "ground turkey",
        "turkey breast",
        "turkey",
    ],
    "vegetarian": [
        "lentils",
        "lentil",
        "chickpeas",
        "chickpea",
        "black beans",
        "kidney beans",
        "beans",
        "eggs",
        "egg",
        "tofu",
    ],
}

CARB_KEYWORDS = {
    "rice",
    "brown rice",
    "white rice",
    "basmati rice",
    "jasmine rice",
    "potato",
    "potatoes",
    "pasta",
    "ziti",
    "spaghetti",
    "penne",
    "macaroni",
    "egg noodles",
    "orzo",
    "noodles",
    "noodle",
    "beans",
    "lentils",
    "chickpeas",
}

VEG_KEYWORDS = {
    "onion",
    "garlic",
    "tomato",
    "tomatoes",
    "pepper",
    "bell pepper",
    "carrot",
    "carrots",
    "broccoli",
    "spinach",
    "zucchini",
    "mushroom",
    "mushrooms",
    "green beans",
    "peas",
    "corn",
    "cabbage",
    "celery",
}

GOOD_MAPPING_TERMS = {
    "salt",
    "black pepper",
    "olive oil",
    "vegetable oil",
    "butter",
    "garlic",
    "onion",
    "yellow onion",
    "red onion",
    "carrot",
    "carrots",
    "celery",
    "tomato",
    "tomatoes",
    "tomato sauce",
    "potato",
    "potatoes",
    "rice",
    "white rice",
    "brown rice",
    "basmati rice",
    "pasta",
    "spaghetti",
    "penne",
    "macaroni",
    "chicken breast",
    "chicken breasts",
    "chicken",
    "ground beef",
    "beef",
    "sirloin steak",
    "pork tenderloin",
    "pork chops",
    "ground turkey",
    "tuna",
    "canned tuna",
    "salmon",
    "canned salmon",
    "lentils",
    "beans",
    "eggs",
    "egg",
    "parmesan cheese",
    "mozzarella cheese",
    "milk",
}

BAD_TITLE_OR_CATEGORY = {
    "dessert",
    "cake",
    "cookie",
    "cookies",
    "pie",
    "drink",
    "smoothie",
    "cocktail",
    "beverage",
    "pet",
    "dog",
    "cat",
    "sauce",
    "dressing",
    "condiment",
    "dip",
    "jam",
    "jelly",
    "candy",
    "ice cream",
    "popsicle",
    "bread",
    "muffin",
    "pancake",
    "waffle",
    "breakfast",
    "snack",
    "snacks",
    "appetizer",
    "appetizers",
    "egg roll",
    "egg rolls",
    "dumpling",
    "dumplings",
    "pot sticker",
    "pot stickers",
    "wrapper",
    "wrappers",
    "salad",
    "salads",
    "liver",
    "butter soup",
}

RISK_TERMS = {
    "coconut milk",
    "curry paste",
    "fish sauce",
    "mirin",
    "gochujang",
    "hoisin",
    "oyster sauce",
    "wine vinegar",
    "sriracha",
    "chipotle",
    "adobo",
    "processed cheese",
    "velveeta",
    "ranch",
    "cream of",
    "cooking spray",
    "aluminum foil",
    "parchment",
    "american cheese",
    "bacon",
    "pancetta",
    "sausage",
    "kielbasa",
    "smoked",
    "shrimp",
    "prawn",
    "egg roll wrapper",
    "wonton",
    "leftover",
    "soy sauce",
    "taco seasoning",
    "taco",
    "dhal",
    "falafel",
    "curried",
}


def main() -> None:
    source_rows = read_csv(SOURCE_RECIPES)
    curated_rows = read_csv(CURATED_200)
    snack_rows = read_csv(SNACK_CURATED_RECIPES)
    exclusion_indices, exclusion_titles = existing_recipe_keys(curated_rows, snack_rows)

    candidates: list[dict[str, object]] = []
    exclusion_rows: list[dict[str, object]] = []
    seen_titles: set[str] = set()
    for zero_index, row in enumerate(source_rows):
        source_index = zero_index + 1
        title = clean_text(row.get("recipe_title"))
        title_key = normalize_text(title)
        ingredients = load_json_list(row.get("ingredients"))
        directions = load_json_list(row.get("directions"))
        analysis = analyze_recipe(row, ingredients)
        exclusion_reason = exclusion_reason_for_recipe(
            source_index=source_index,
            title_key=title_key,
            row=row,
            ingredients=ingredients,
            directions=directions,
            exclusion_indices=exclusion_indices,
            exclusion_titles=exclusion_titles,
            seen_titles=seen_titles,
            analysis=analysis,
        )
        if exclusion_reason:
            if len(exclusion_rows) < 5000:
                exclusion_rows.append(exclusion_row(source_index, row, analysis, exclusion_reason))
            continue
        seen_titles.add(title_key)
        candidates.append(candidate_row(source_index, row, ingredients, directions, analysis))

    selected = select_targeted_plus10(candidates)
    selected_rows = [finalize_selected_row(index, row) for index, row in enumerate(selected, start=1)]

    write_csv(OUT_SELECTED, selected_rows, OUTPUT_COLUMNS)
    write_csv(OUT_EXCLUSION, exclusion_rows, EXCLUSION_COLUMNS)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(
        build_summary(selected_rows, candidates, exclusion_rows),
        encoding="utf-8",
    )

    print("Round26 targeted lunch/dinner +10 selection written")
    print(f"selected={OUT_SELECTED}")
    print(f"summary={OUT_SUMMARY}")
    print(f"exclusion_log={OUT_EXCLUSION}")
    print(f"selected_count={len(selected_rows)}")
    for row in selected_rows:
        print(
            f"- {row['recipe_id_candidate']} | {row['display_name']} | "
            f"{row['primary_protein']} | {row['expected_generator_value']}"
        )


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def existing_recipe_keys(
    curated_rows: list[dict[str, str]],
    snack_rows: list[dict[str, str]],
) -> tuple[set[int], set[str]]:
    indices: set[int] = set()
    titles: set[str] = set()
    for row in curated_rows:
        add_int(indices, row.get("source_index"))
        titles.add(normalize_text(row.get("display_name")))
    for row in snack_rows:
        add_int(indices, row.get("source_recipe_id"))
        titles.add(normalize_text(row.get("display_name") or row.get("recipe_name")))
    return indices, {title for title in titles if title}


def add_int(values: set[int], value: object) -> None:
    try:
        number = int(float(str(value or "").strip()))
    except ValueError:
        return
    if number > 0:
        values.add(number)


def analyze_recipe(row: dict[str, str], ingredients: list[str]) -> dict[str, object]:
    title = clean_text(row.get("recipe_title"))
    category = clean_text(row.get("category"))
    subcategory = clean_text(row.get("subcategory"))
    joined = normalize_text(" ".join([title, category, subcategory, *ingredients]))
    primary_protein = detect_primary_protein(joined)
    has_carb = detect_carb_component(ingredients)
    has_veg = any(contains_term(joined, term) for term in VEG_KEYWORDS)
    risk_hits = sorted(term for term in RISK_TERMS if contains_term(joined, term))
    good_hits = sorted(term for term in GOOD_MAPPING_TERMS if contains_term(joined, term))
    expected_mapping_difficulty = mapping_difficulty(ingredients, risk_hits, good_hits)
    score = selection_score(row, primary_protein, has_carb, has_veg, risk_hits, good_hits)
    if primary_protein == "vegetarian":
        if any(contains_term(joined, term) for term in ["lentil", "lentils", "bean", "beans", "chickpea", "chickpeas"]):
            score += 10
        else:
            score -= 10
    return {
        "primary_protein": primary_protein,
        "has_carb_component": has_carb,
        "has_veg_component": has_veg,
        "risk_hits": risk_hits,
        "good_mapping_hits": good_hits,
        "expected_mapping_difficulty": expected_mapping_difficulty,
        "selection_score": score,
    }


def detect_primary_protein(text: str) -> str:
    for protein in ["chicken", "fish", "beef", "pork", "turkey", "vegetarian"]:
        for term in PROTEIN_KEYWORDS[protein]:
            if contains_term(text, term):
                return protein
    return ""


def detect_carb_component(ingredients: list[str]) -> bool:
    for ingredient in ingredients:
        text = normalize_text(ingredient)
        if not text:
            continue
        if "sauce" in text and not any(term in text for term in ["rice", "bean", "lentil"]):
            continue
        if "bread crumb" in text or "breadcrumbs" in text:
            continue
        if any(contains_term(text, term) for term in CARB_KEYWORDS):
            return True
    return False


def mapping_difficulty(
    ingredients: list[str],
    risk_hits: list[str],
    good_hits: list[str],
) -> str:
    ingredient_count = len(ingredients)
    good_count = len(good_hits)
    risk_count = len(risk_hits)
    if risk_count >= 3 or ingredient_count > 13:
        return "high"
    if risk_count >= 1 or good_count < 5:
        return "medium"
    return "low"


def selection_score(
    row: dict[str, str],
    primary_protein: str,
    has_carb: bool,
    has_veg: bool,
    risk_hits: list[str],
    good_hits: list[str],
) -> float:
    num_ingredients = to_int(row.get("num_ingredients"))
    num_steps = to_int(row.get("num_steps"))
    category_text = normalize_text(f"{row.get('category')} {row.get('subcategory')}")
    score = 0.0
    if primary_protein:
        score += 40
    if has_carb:
        score += 25
    if has_veg:
        score += 20
    if primary_protein in {"chicken", "fish", "beef", "pork", "turkey"}:
        score += 8
    if primary_protein == "vegetarian":
        score += 4
    if any(term in category_text for term in ["main dish", "casserole", "pasta", "stew", "rice"]):
        score += 8
    if 5 <= num_ingredients <= 11:
        score += 8
    elif num_ingredients <= 14:
        score += 3
    if 3 <= num_steps <= 7:
        score += 6
    score += min(10, len(good_hits))
    score -= 8 * len(risk_hits)
    if num_ingredients > 14:
        score -= 20
    if num_steps > 8:
        score -= 12
    return round(score, 3)


def exclusion_reason_for_recipe(
    source_index: int,
    title_key: str,
    row: dict[str, str],
    ingredients: list[str],
    directions: list[str],
    exclusion_indices: set[int],
    exclusion_titles: set[str],
    seen_titles: set[str],
    analysis: dict[str, object],
) -> str:
    title = normalize_text(row.get("recipe_title"))
    category = normalize_text(row.get("category"))
    subcategory = normalize_text(row.get("subcategory"))
    combined_scope = f"{title} {category} {subcategory}"
    if source_index in exclusion_indices or title_key in exclusion_titles:
        return "already_in_curated_or_snack_curated"
    if title_key in seen_titles:
        return "duplicate_title"
    if any(contains_term(combined_scope, term) for term in BAD_TITLE_OR_CATEGORY):
        return "out_of_scope_title_or_category"
    if not ingredients or not directions:
        return "missing_ingredients_or_directions"
    if not (5 <= len(ingredients) <= 14):
        return "ingredient_count_out_of_target"
    if not (2 <= len(directions) <= 8):
        return "step_count_out_of_target"
    if not analysis["primary_protein"]:
        return "missing_target_protein"
    if not analysis["has_carb_component"]:
        return "missing_carb_component"
    if not analysis["has_veg_component"]:
        return "missing_veg_component"
    if analysis["expected_mapping_difficulty"] == "high":
        return "expected_mapping_difficulty_high"
    if analysis["selection_score"] < 82:
        return "selection_score_below_target"
    return ""


def candidate_row(
    source_index: int,
    row: dict[str, str],
    ingredients: list[str],
    directions: list[str],
    analysis: dict[str, object],
) -> dict[str, object]:
    primary = str(analysis["primary_protein"])
    if primary in {"chicken", "beef", "pork", "turkey", "fish"}:
        generator_value = "carb_protein_main"
    elif primary == "vegetarian":
        generator_value = "lunch_dinner_variety"
    else:
        generator_value = "cabbage_alternative"
    if primary in {"beef", "pork", "turkey"}:
        generator_value = "cabbage_alternative"
    if primary in {"fish", "vegetarian"}:
        generator_value = "veggie_burger_alternative"
    return {
        "source_index": source_index,
        "display_name": clean_text(row.get("recipe_title")),
        "source_category": clean_text(row.get("category")),
        "source_subcategory": clean_text(row.get("subcategory")),
        "recipe_kind_guess": "complete_main",
        "primary_protein": primary,
        "has_carb_component": bool(analysis["has_carb_component"]),
        "has_veg_component": bool(analysis["has_veg_component"]),
        "expected_mapping_difficulty": analysis["expected_mapping_difficulty"],
        "expected_generator_value": generator_value,
        "selection_reason": selection_reason(analysis),
        "risk_notes": risk_notes(analysis),
        "selection_score": analysis["selection_score"],
        "num_ingredients": len(ingredients),
        "num_steps": len(directions),
        "ingredients_json": json.dumps(ingredients, ensure_ascii=False),
        "directions_json": json.dumps(directions, ensure_ascii=False),
    }


def select_targeted_plus10(candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    by_protein: dict[str, list[dict[str, object]]] = {}
    for row in sorted(
        candidates,
        key=lambda item: (
            -float(item["selection_score"]),
            str(item["expected_mapping_difficulty"]),
            str(item["display_name"]),
        ),
    ):
        by_protein.setdefault(str(row["primary_protein"]), []).append(row)

    selected: list[dict[str, object]] = []
    used_titles: set[str] = set()
    for protein, quota in TARGET_QUOTAS.items():
        for row in by_protein.get(protein, []):
            if len([item for item in selected if item["primary_protein"] == protein]) >= quota:
                break
            title_key = normalize_text(row["display_name"])
            if title_key in used_titles:
                continue
            selected.append(row)
            used_titles.add(title_key)

    if len(selected) < 10:
        for row in sorted(candidates, key=lambda item: -float(item["selection_score"])):
            if len(selected) >= 10:
                break
            title_key = normalize_text(row["display_name"])
            if title_key in used_titles:
                continue
            selected.append(row)
            used_titles.add(title_key)
    return selected[:10]


def finalize_selected_row(index: int, row: dict[str, object]) -> dict[str, object]:
    finalized = dict(row)
    finalized["recipe_id_candidate"] = f"recipes_v1_1_round26_plus10_{index:03d}"
    return finalized


def selection_reason(analysis: dict[str, object]) -> str:
    parts = [
        "protein_plus_carb_main",
        "veg_present" if analysis["has_veg_component"] else "veg_missing",
        f"mapping={analysis['expected_mapping_difficulty']}",
        "round25_lunch_dinner_gap",
    ]
    return ";".join(parts)


def risk_notes(analysis: dict[str, object]) -> str:
    risk_hits = analysis.get("risk_hits") or []
    if risk_hits:
        return "risk_terms=" + "|".join(str(item) for item in risk_hits)
    return "low_obvious_mapping_risk"


def exclusion_row(
    source_index: int,
    row: dict[str, str],
    analysis: dict[str, object],
    reason: str,
) -> dict[str, object]:
    return {
        "source_index": source_index,
        "display_name": clean_text(row.get("recipe_title")),
        "source_category": clean_text(row.get("category")),
        "source_subcategory": clean_text(row.get("subcategory")),
        "exclusion_reason": reason,
        "primary_protein": analysis.get("primary_protein", ""),
        "has_carb_component": analysis.get("has_carb_component", False),
        "has_veg_component": analysis.get("has_veg_component", False),
        "num_ingredients": row.get("num_ingredients", ""),
        "num_steps": row.get("num_steps", ""),
    }


def build_summary(
    selected_rows: list[dict[str, object]],
    candidates: list[dict[str, object]],
    exclusion_rows: list[dict[str, object]],
) -> str:
    selected_proteins = Counter(str(row["primary_protein"]) for row in selected_rows)
    candidate_proteins = Counter(str(row["primary_protein"]) for row in candidates)
    exclusions = Counter(str(row["exclusion_reason"]) for row in exclusion_rows)
    round25_note = ""
    if ROUND25_RECOMMENDATION.exists():
        round25_note = ROUND25_RECOMMENDATION.read_text(encoding="utf-8").strip().splitlines()[0]
    lines = [
        "Recipes_DB v1.1 Round26 targeted lunch/dinner +10 selection",
        "",
        f"source={SOURCE_RECIPES}",
        f"curated_200={CURATED_200}",
        f"snack_curated={SNACK_CURATED_RECIPES}",
        f"round25_alternatives_available={ROUND25_ALTERNATIVES.exists()}",
        f"round25_recommendation_note={round25_note}",
        "",
        f"candidate_pool_after_filters={len(candidates)}",
        f"selected_count={len(selected_rows)}",
        "",
        "Selected protein mix:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in selected_proteins.most_common())
    lines.extend(["", "Candidate protein mix:"])
    lines.extend(f"- {name}: {count}" for name, count in candidate_proteins.most_common())
    lines.extend(["", "Selected recipes:"])
    for row in selected_rows:
        lines.append(
            (
                f"- {row['recipe_id_candidate']} | {row['display_name']} | "
                f"{row['primary_protein']} | {row['expected_generator_value']} | "
                f"score={row['selection_score']} | {row['risk_notes']}"
            )
        )
    lines.extend(["", "Top exclusions sampled:"])
    lines.extend(f"- {name}: {count}" for name, count in exclusions.most_common(20))
    lines.extend(
        [
            "",
            "Strict note:",
            "This is a targeted draft/test expansion, not a broad Recipes_DB expansion.",
        ]
    )
    return "\n".join(lines) + "\n"


def load_json_list(value: object) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def contains_term(text: str, term: str) -> bool:
    normalized_term = normalize_text(term)
    if not normalized_term:
        return False
    if " " in normalized_term:
        return normalized_term in text
    return re.search(rf"(?<![a-z0-9]){re.escape(normalized_term)}(?![a-z0-9])", text) is not None


def normalize_text(value: object) -> str:
    text = clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_text(value: object) -> str:
    return str(value or "").strip()


def to_int(value: object) -> int:
    try:
        return int(float(str(value or "").strip()))
    except ValueError:
        return 0


if __name__ == "__main__":
    main()
