from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any


SOURCE_RECIPES = Path("data/recipesdb/source/1_Recipe_csv.csv")
CURATED_200 = Path("data/recipesdb/draft/recipes_v1_1_curated_200.csv")
PLUS30_RECIPES = Path(
    "data/recipesdb/draft/v1_2_generator_ready_plus30/recipes.csv"
)
ROUND28_SELECTED = Path("data/recipesdb/draft/recipes_v1_2_round28_targeted_plus30.csv")
ROUND29_RECOMMENDATION = Path(
    "data/recipesdb/audit/generator_v1_round29_plus30_next_action_recommendation.txt"
)
ROUND29_ANCHOR_ANALYSIS = Path(
    "data/recipesdb/audit/generator_v1_round29_plus30_anchor_recipe_analysis.csv"
)

OUT_CANDIDATES = Path("data/recipesdb/draft/recipes_v1_2_round30_targeted_plus15_candidates.csv")
OUT_SELECTED = Path("data/recipesdb/draft/recipes_v1_2_round30_targeted_plus15.csv")
OUT_SUMMARY = Path("data/recipesdb/audit/recipes_v1_2_round30_targeted_plus15_summary.txt")
OUT_EXCLUSION = Path("data/recipesdb/audit/recipes_v1_2_round30_targeted_plus15_exclusion_log.csv")
OUT_QUALITY_AUDIT = Path("data/recipesdb/audit/recipes_v1_2_round30_targeted_plus15_quality_audit.csv")

OUTPUT_COLUMNS = [
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "source_category",
    "source_subcategory",
    "recipe_kind_guess",
    "target_bucket",
    "primary_protein",
    "has_carb_component",
    "has_veg_component",
    "expected_mapping_difficulty",
    "expected_generator_value",
    "selection_reason",
    "risk_notes",
    "selection_score",
    "title_quality_status",
    "is_weird_or_random",
    "is_too_american_processed",
    "is_dessert_like",
    "is_drink",
    "is_pet_food",
    "is_component_only",
    "has_clear_protein",
    "has_clear_carb_or_veg",
    "ingredient_count",
    "step_count",
    "expected_unit_to_grams_risk",
    "expected_fooddb_gap_risk",
    "expected_servings_risk",
    "manual_review_recommendation",
    "ingredients_json",
    "directions_json",
]

EXCLUSION_COLUMNS = [
    "source_index",
    "display_name",
    "source_category",
    "source_subcategory",
    "exclusion_reason",
    "target_bucket",
    "primary_protein",
    "has_carb_component",
    "has_veg_component",
    "ingredient_count",
    "step_count",
    "selection_score",
]

TARGET_QUOTAS = {
    "breakfast_competitor": 5,
    "carb_protein_main": 10,
}

PROTEIN_KEYWORDS = {
    "chicken": ["chicken breast", "chicken breasts", "chicken thighs", "ground chicken", "chicken"],
    "fish": ["canned tuna", "tuna", "canned salmon", "salmon", "cod", "tilapia", "fish fillet", "fish"],
    "beef": ["ground beef", "lean ground beef", "sirloin steak", "beef steak", "beef"],
    "pork": ["pork tenderloin", "pork chops", "pork chop", "pork loin", "ground pork", "pork"],
    "turkey": ["ground turkey", "turkey breast", "turkey"],
    "legume_vegetarian": [
        "lentils",
        "lentil",
        "black beans",
        "kidney beans",
        "white beans",
        "chickpeas",
        "chickpea",
        "beans",
        "tofu",
    ],
}

BREAKFAST_PROTEIN_KEYWORDS = {
    "egg",
    "eggs",
    "yogurt",
    "greek yogurt",
    "milk",
    "ham",
    "cheese",
    "peanut butter",
    "peanut",
    "almond",
    "oats",
    "oatmeal",
}

CARB_KEYWORDS = {
    "rice",
    "brown rice",
    "basmati rice",
    "jasmine rice",
    "potato",
    "potatoes",
    "sweet potato",
    "pasta",
    "spaghetti",
    "penne",
    "macaroni",
    "orzo",
    "noodles",
    "noodle",
    "bread",
    "toast",
    "tortilla",
    "oats",
    "oatmeal",
    "rolled oats",
    "flour",
    "beans",
    "lentils",
    "chickpeas",
}

MAIN_CARB_KEYWORDS = {
    "rice",
    "brown rice",
    "basmati rice",
    "jasmine rice",
    "potato",
    "potatoes",
    "sweet potato",
    "pasta",
    "spaghetti",
    "penne",
    "macaroni",
    "orzo",
    "noodles",
    "noodle",
    "tortilla",
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
    "broccoli",
    "spinach",
    "zucchini",
    "mushroom",
    "green beans",
    "peas",
    "corn",
    "cabbage",
    "celery",
    "asparagus",
}

GOOD_MAPPING_TERMS = {
    "salt",
    "black pepper",
    "olive oil",
    "vegetable oil",
    "butter",
    "garlic",
    "onion",
    "carrot",
    "celery",
    "tomato",
    "potato",
    "rice",
    "pasta",
    "spaghetti",
    "penne",
    "chicken breast",
    "chicken",
    "ground beef",
    "beef",
    "pork tenderloin",
    "pork chop",
    "ground turkey",
    "tuna",
    "salmon",
    "lentils",
    "beans",
    "black beans",
    "egg",
    "eggs",
    "milk",
    "yogurt",
    "oats",
    "rolled oats",
    "banana",
    "apple",
    "strawberries",
    "blueberries",
    "bread",
    "tortilla",
}

HARD_BAD_TERMS = {
    "pet",
    "dog",
    "cat",
    "cocktail",
    "margarita",
    "martini",
    "smoothie",
    "drink",
    "beverage",
    "candy",
    "ice cream",
    "popsicle",
    "frosting",
    "sauce",
    "dressing",
    "dip",
    "jam",
    "jelly",
    "syrup",
    "condiment",
}

DESSERT_TERMS = {
    "cake",
    "cookie",
    "cookies",
    "pie",
    "brownie",
    "dessert",
    "candy",
    "frosting",
    "cupcake",
    "cream pie",
    "sticky buns",
    "cinnamon rolls",
}

PROCESSED_TERMS = {
    "bisquick",
    "pancake mix",
    "crescent rolls",
    "crescent dough",
    "refrigerated biscuits",
    "cream of",
    "velveeta",
    "american cheese",
    "spam",
    "hot dog",
    "hot dogs",
    "tater tots",
    "ranch",
    "shake and pour",
    "protein shake",
    "breakfast sausage patties",
}

WEIRD_TERMS = {
    "gochujang",
    "sriracha",
    "ramen",
    "chile crisp",
    "everything bagel",
    "fusion",
    "shot",
    "puff pastry",
    "wonton",
    "egg roll",
    "dumpling",
    "leftover",
    "japanese",
    "korean",
    "donburi",
    "tonkatsu",
    "tempura",
    "nachos",
    "monkey bread",
    "fried green tomatoes",
    "waffle",
    "waffles",
    "latke",
    "latkes",
    "pancake",
    "pancakes",
    "french toast",
    "caramel",
    "creme brulee",
    "muffin",
    "muffins",
    "soup",
    "meatloaf",
    "fried chicken",
    "mexican rice",
    "hibachi",
    "mochiko",
    "vitamix",
    "caesar",
    "keto",
    "cauliflower rice",
    "gravy",
}

UNIT_RISK_TERMS = {
    "can",
    "cans",
    "jar",
    "package",
    "packages",
    "loaf",
    "sheet",
    "to taste",
    "as needed",
    "serving",
    "pinch",
}

FOODDB_GAP_TERMS = {
    "cream cheese",
    "cottage cheese",
    "halloumi",
    "queso",
    "chorizo",
    "sausage",
    "bacon",
    "panko",
    "mango chutney",
    "almond milk",
    "coconut milk",
    "goat cheese",
    "gruyere",
    "bacon bits",
}


def main() -> None:
    source_rows = read_csv(SOURCE_RECIPES)
    curated_rows = read_csv(CURATED_200)
    plus30_rows = read_csv(PLUS30_RECIPES)
    round28_rows = read_csv(ROUND28_SELECTED)
    exclusion_indices, exclusion_titles = existing_recipe_keys(curated_rows, plus30_rows, round28_rows)

    candidates: list[dict[str, object]] = []
    exclusion_rows: list[dict[str, object]] = []
    seen_titles: set[str] = set()
    for zero_index, row in enumerate(source_rows):
        source_index = zero_index + 1
        ingredients = load_json_list(row.get("ingredients"))
        directions = load_json_list(row.get("directions"))
        analysis = analyze_recipe(row, ingredients, directions)
        title_key = normalize_title_key(row.get("recipe_title"))
        exclusion_reason = exclusion_reason_for_recipe(
            source_index=source_index,
            title_key=title_key,
            row=row,
            ingredients=ingredients,
            directions=directions,
            analysis=analysis,
            exclusion_indices=exclusion_indices,
            exclusion_titles=exclusion_titles,
            seen_titles=seen_titles,
        )
        if exclusion_reason:
            if len(exclusion_rows) < 8000:
                exclusion_rows.append(exclusion_row(source_index, row, analysis, exclusion_reason))
            continue
        seen_titles.add(title_key)
        candidates.append(candidate_row(source_index, row, ingredients, directions, analysis))

    candidate_pool = sorted(
        candidates,
        key=lambda item: (
            str(item["manual_review_recommendation"]) != "keep",
            -float(item["selection_score"]),
            str(item["target_bucket"]),
            str(item["display_name"]),
        ),
    )[:150]
    selected = select_targeted_plus15(candidate_pool)
    selected_rows = [finalize_selected_row(index, row) for index, row in enumerate(selected, start=1)]
    candidate_rows = [
        finalize_candidate_pool_row(row, index) for index, row in enumerate(candidate_pool, start=1)
    ]

    write_csv(OUT_CANDIDATES, candidate_rows, OUTPUT_COLUMNS)
    write_csv(OUT_SELECTED, selected_rows, OUTPUT_COLUMNS)
    write_csv(OUT_QUALITY_AUDIT, candidate_rows, OUTPUT_COLUMNS)
    write_csv(OUT_EXCLUSION, exclusion_rows, EXCLUSION_COLUMNS)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(
        build_summary(selected_rows, candidate_rows, exclusion_rows),
        encoding="utf-8",
    )

    print("Round30 targeted v1.2 +15 selection written")
    print(f"candidates={OUT_CANDIDATES}")
    print(f"selected={OUT_SELECTED}")
    print(f"summary={OUT_SUMMARY}")
    print(f"quality_audit={OUT_QUALITY_AUDIT}")
    print(f"exclusion_log={OUT_EXCLUSION}")
    print(f"candidate_pool={len(candidate_rows)} selected={len(selected_rows)}")
    for row in selected_rows:
        print(
            f"- {row['recipe_id_candidate']} | {row['target_bucket']} | "
            f"{row['display_name']} | score={row['selection_score']}"
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
    plus30_rows: list[dict[str, str]],
    round28_rows: list[dict[str, str]],
) -> tuple[set[int], set[str]]:
    indices: set[int] = set()
    titles: set[str] = set()
    for row in curated_rows:
        add_int(indices, row.get("source_index"))
        titles.add(normalize_title_key(row.get("display_name")))
    for row in plus30_rows:
        add_int(indices, row.get("source_recipe_id"))
        titles.add(normalize_title_key(row.get("display_name") or row.get("recipe_name")))
    for row in round28_rows:
        add_int(indices, row.get("source_index"))
        titles.add(normalize_title_key(row.get("display_name") or row.get("recipe_name")))
    return indices, {title for title in titles if title}


def add_int(values: set[int], value: object) -> None:
    try:
        number = int(float(str(value or "").strip()))
    except ValueError:
        return
    if number > 0:
        values.add(number)


def analyze_recipe(
    row: dict[str, str],
    ingredients: list[str],
    directions: list[str],
) -> dict[str, Any]:
    title = clean_text(row.get("recipe_title"))
    category = clean_text(row.get("category"))
    subcategory = clean_text(row.get("subcategory"))
    ingredient_text = " ".join(ingredients)
    combined = normalize_text(" ".join([title, category, subcategory, ingredient_text]))
    title_scope = normalize_text(f"{title} {category} {subcategory}")
    ingredient_count = len(ingredients)
    step_count = len(directions)
    is_breakfast = any(
        contains_term(title_scope, term)
        for term in ["breakfast", "brunch", "oatmeal", "oats", "omelet", "omelette", "frittata"]
    )
    primary_protein = detect_primary_protein(combined, is_breakfast)
    has_carb = detect_carb_component(ingredients, is_breakfast)
    has_veg = any(contains_term(combined, term) for term in VEG_KEYWORDS)
    has_breakfast_protein = any(contains_term(combined, term) for term in BREAKFAST_PROTEIN_KEYWORDS)
    is_dessert_like = any(contains_term(title_scope, term) for term in DESSERT_TERMS)
    is_drink = any(contains_term(title_scope, term) for term in ["drink", "cocktail", "smoothie", "margarita", "martini"])
    is_pet_food = any(contains_term(title_scope, term) for term in ["dog", "cat", "pet"])
    is_component_only = component_only(title_scope, combined)
    is_too_american_processed = any(contains_term(combined, term) for term in PROCESSED_TERMS)
    is_weird_or_random = any(contains_term(combined, term) for term in WEIRD_TERMS)
    target_bucket = choose_target_bucket(is_breakfast, primary_protein, has_carb, has_veg, combined)
    risk_hits = sorted(term for term in FOODDB_GAP_TERMS | UNIT_RISK_TERMS | PROCESSED_TERMS | WEIRD_TERMS if contains_term(combined, term))
    good_hits = sorted(term for term in GOOD_MAPPING_TERMS if contains_term(combined, term))
    expected_mapping_difficulty = mapping_difficulty(ingredient_count, risk_hits, good_hits)
    expected_unit_to_grams_risk = unit_risk(ingredients)
    expected_fooddb_gap_risk = fooddb_gap_risk(risk_hits, good_hits)
    expected_servings_risk = servings_risk(ingredient_count, step_count, combined)
    has_clear_protein = bool(primary_protein) or has_breakfast_protein
    has_clear_carb_or_veg = has_carb or has_veg
    score = selection_score(
        target_bucket=target_bucket,
        primary_protein=primary_protein,
        has_carb=has_carb,
        has_veg=has_veg,
        has_breakfast_protein=has_breakfast_protein,
        ingredient_count=ingredient_count,
        step_count=step_count,
        expected_mapping_difficulty=expected_mapping_difficulty,
        expected_unit_to_grams_risk=expected_unit_to_grams_risk,
        expected_fooddb_gap_risk=expected_fooddb_gap_risk,
        expected_servings_risk=expected_servings_risk,
        good_hits=good_hits,
        risk_hits=risk_hits,
        is_too_american_processed=is_too_american_processed,
        is_weird_or_random=is_weird_or_random,
        is_dessert_like=is_dessert_like,
    )
    title_quality_status, manual_review = quality_status(
        target_bucket=target_bucket,
        has_clear_protein=has_clear_protein,
        has_clear_carb_or_veg=has_clear_carb_or_veg,
        is_dessert_like=is_dessert_like,
        is_drink=is_drink,
        is_pet_food=is_pet_food,
        is_component_only=is_component_only,
        is_too_american_processed=is_too_american_processed,
        is_weird_or_random=is_weird_or_random,
        ingredient_count=ingredient_count,
        step_count=step_count,
        expected_mapping_difficulty=expected_mapping_difficulty,
        expected_unit_to_grams_risk=expected_unit_to_grams_risk,
        expected_fooddb_gap_risk=expected_fooddb_gap_risk,
        expected_servings_risk=expected_servings_risk,
        selection_score=score,
    )
    return {
        "primary_protein": primary_protein,
        "has_carb_component": has_carb,
        "has_veg_component": has_veg,
        "has_breakfast_protein": has_breakfast_protein,
        "target_bucket": target_bucket,
        "expected_mapping_difficulty": expected_mapping_difficulty,
        "expected_unit_to_grams_risk": expected_unit_to_grams_risk,
        "expected_fooddb_gap_risk": expected_fooddb_gap_risk,
        "expected_servings_risk": expected_servings_risk,
        "risk_hits": risk_hits,
        "good_hits": good_hits,
        "selection_score": score,
        "title_quality_status": title_quality_status,
        "manual_review_recommendation": manual_review,
        "is_weird_or_random": is_weird_or_random,
        "is_too_american_processed": is_too_american_processed,
        "is_dessert_like": is_dessert_like,
        "is_drink": is_drink,
        "is_pet_food": is_pet_food,
        "is_component_only": is_component_only,
        "has_clear_protein": has_clear_protein,
        "has_clear_carb_or_veg": has_clear_carb_or_veg,
        "ingredient_count": ingredient_count,
        "step_count": step_count,
    }


def detect_primary_protein(text: str, is_breakfast: bool) -> str:
    for protein in ["chicken", "fish", "beef", "pork", "turkey", "legume_vegetarian"]:
        for term in PROTEIN_KEYWORDS[protein]:
            if contains_term(text, term):
                if protein == "chicken" and (
                    "chicken broth" in text or "chicken bouillon" in text
                ) and not any(
                    phrase in text
                    for phrase in [
                        "chicken breast",
                        "chicken breasts",
                        "chicken thigh",
                        "chicken thighs",
                        "ground chicken",
                        "shredded chicken",
                        "cooked chicken",
                    ]
                ):
                    continue
                if protein == "legume_vegetarian":
                    return "vegetarian"
                return protein
    if is_breakfast and any(contains_term(text, term) for term in ["egg", "eggs", "yogurt", "milk", "oats"]):
        return "egg_dairy_oat"
    return ""


def detect_carb_component(ingredients: list[str], is_breakfast: bool) -> bool:
    terms = CARB_KEYWORDS if is_breakfast else MAIN_CARB_KEYWORDS
    for ingredient in ingredients:
        text = normalize_text(ingredient)
        if not text:
            continue
        if "sauce" in text and not any(term in text for term in ["rice", "bean", "lentil", "oat"]):
            continue
        if any(contains_term(text, term) for term in terms):
            return True
    return bool(is_breakfast and any("oat" in normalize_text(item) for item in ingredients))


def component_only(title_scope: str, combined: str) -> bool:
    if any(contains_term(title_scope, term) for term in ["sauce", "dressing", "dip", "seasoning", "marinade"]):
        return True
    if any(
        contains_term(title_scope, term)
        for term in [
            "salad",
            "bread",
            "monkey bread",
            "fried green tomatoes",
            "potato salad",
            "tomato salad",
            "side dish",
            "appetizer",
            "snack",
            "nachos",
        ]
    ):
        return True
    if any(contains_term(title_scope, term) for term in ["side dish", "appetizer", "snack"]) and not any(
        contains_term(combined, term) for term in ["chicken", "beef", "pork", "turkey", "fish", "egg", "lentil", "bean", "rice", "pasta", "potato", "oat"]
    ):
        return True
    return False


def choose_target_bucket(
    is_breakfast: bool,
    primary_protein: str,
    has_carb: bool,
    has_veg: bool,
    combined: str,
) -> str:
    if is_breakfast and has_carb:
        return "breakfast_competitor"
    if primary_protein and has_carb and (has_veg or primary_protein in {"chicken", "beef", "fish", "turkey", "pork", "vegetarian"}):
        return "carb_protein_main"
    if any(contains_term(combined, term) for term in ["rice", "pasta", "potato"]) and primary_protein:
        return "carb_protein_main"
    return ""


def mapping_difficulty(ingredient_count: int, risk_hits: list[str], good_hits: list[str]) -> str:
    if ingredient_count > 14 or len(risk_hits) >= 5:
        return "high"
    if ingredient_count > 11 or len(risk_hits) >= 2 or len(good_hits) < 4:
        return "medium"
    return "low"


def unit_risk(ingredients: list[str]) -> str:
    hits = 0
    for ingredient in ingredients:
        text = normalize_text(ingredient)
        if any(contains_term(text, term) for term in UNIT_RISK_TERMS):
            hits += 1
    if hits >= 3:
        return "high"
    if hits >= 1:
        return "medium"
    return "low"


def fooddb_gap_risk(risk_hits: list[str], good_hits: list[str]) -> str:
    gap_hits = [hit for hit in risk_hits if hit in FOODDB_GAP_TERMS]
    if len(gap_hits) >= 2:
        return "high"
    if gap_hits or len(good_hits) < 5:
        return "medium"
    return "low"


def servings_risk(ingredient_count: int, step_count: int, combined: str) -> str:
    if ingredient_count > 14 or step_count > 9 or any(contains_term(combined, term) for term in ["loaf", "sheet pan", "party", "crowd"]):
        return "high"
    if ingredient_count > 11 or step_count > 7:
        return "medium"
    return "low"


def selection_score(
    target_bucket: str,
    primary_protein: str,
    has_carb: bool,
    has_veg: bool,
    has_breakfast_protein: bool,
    ingredient_count: int,
    step_count: int,
    expected_mapping_difficulty: str,
    expected_unit_to_grams_risk: str,
    expected_fooddb_gap_risk: str,
    expected_servings_risk: str,
    good_hits: list[str],
    risk_hits: list[str],
    is_too_american_processed: bool,
    is_weird_or_random: bool,
    is_dessert_like: bool,
) -> float:
    score = 0.0
    if target_bucket:
        score += 40
    if target_bucket == "breakfast_competitor":
        score += 15
    if target_bucket == "carb_protein_main" and primary_protein in {"chicken", "fish", "turkey", "pork", "beef"}:
        score += 14
    if primary_protein:
        score += 20
    if has_breakfast_protein:
        score += 10
    if has_carb:
        score += 18
    if has_veg:
        score += 14
    if 5 <= ingredient_count <= 10:
        score += 10
    elif ingredient_count <= 13:
        score += 4
    if 2 <= step_count <= 6:
        score += 8
    elif step_count <= 8:
        score += 3
    score += min(14, len(good_hits) * 1.4)
    score -= len(risk_hits) * 3.5
    score -= {"low": 0, "medium": 8, "high": 25}[expected_mapping_difficulty]
    score -= {"low": 0, "medium": 4, "high": 12}[expected_unit_to_grams_risk]
    score -= {"low": 0, "medium": 5, "high": 16}[expected_fooddb_gap_risk]
    score -= {"low": 0, "medium": 4, "high": 12}[expected_servings_risk]
    if is_too_american_processed:
        score -= 18
    if is_weird_or_random:
        score -= 12
    if is_dessert_like:
        score -= 35
    return round(score, 3)


def quality_status(**kwargs: Any) -> tuple[str, str]:
    if (
        kwargs["is_pet_food"]
        or kwargs["is_drink"]
        or kwargs["is_dessert_like"]
        or kwargs["is_component_only"]
        or not kwargs["target_bucket"]
        or not kwargs["has_clear_protein"]
        or not kwargs["has_clear_carb_or_veg"]
        or kwargs["ingredient_count"] < 4
        or kwargs["step_count"] < 1
    ):
        return "reject", "reject"
    if (
        kwargs["expected_mapping_difficulty"] == "high"
        or kwargs["expected_fooddb_gap_risk"] == "high"
        or kwargs["expected_unit_to_grams_risk"] == "high"
        or kwargs["expected_servings_risk"] == "high"
        or kwargs["is_too_american_processed"]
        or kwargs["is_weird_or_random"]
        or kwargs["selection_score"] < 82
    ):
        return "review", "review"
    return "good", "keep"


def exclusion_reason_for_recipe(
    source_index: int,
    title_key: str,
    row: dict[str, str],
    ingredients: list[str],
    directions: list[str],
    analysis: dict[str, Any],
    exclusion_indices: set[int],
    exclusion_titles: set[str],
    seen_titles: set[str],
) -> str:
    if source_index in exclusion_indices or title_key in exclusion_titles:
        return "already_selected_or_generator_ready"
    if title_key in seen_titles:
        return "duplicate_or_near_duplicate_title"
    title_scope = normalize_text(f"{row.get('recipe_title')} {row.get('category')} {row.get('subcategory')}")
    if any(contains_term(title_scope, term) for term in HARD_BAD_TERMS):
        return "hard_scope_exclusion"
    if not ingredients or not directions:
        return "missing_ingredients_or_directions"
    if analysis["manual_review_recommendation"] == "reject":
        return "quality_gate_reject"
    if analysis["manual_review_recommendation"] != "keep":
        return "quality_gate_review"
    if analysis["selection_score"] < 88:
        return "selection_score_below_keep_threshold"
    return ""


def candidate_row(
    source_index: int,
    row: dict[str, str],
    ingredients: list[str],
    directions: list[str],
    analysis: dict[str, Any],
) -> dict[str, object]:
    target_bucket = str(analysis["target_bucket"])
    return {
        "recipe_id_candidate": "",
        "source_index": source_index,
        "display_name": clean_text(row.get("recipe_title")),
        "source_category": clean_text(row.get("category")),
        "source_subcategory": clean_text(row.get("subcategory")),
        "recipe_kind_guess": "breakfast_meal" if target_bucket == "breakfast_competitor" else "complete_main",
        "target_bucket": target_bucket,
        "primary_protein": analysis["primary_protein"],
        "has_carb_component": bool(analysis["has_carb_component"]),
        "has_veg_component": bool(analysis["has_veg_component"]),
        "expected_mapping_difficulty": analysis["expected_mapping_difficulty"],
        "expected_generator_value": expected_generator_value(target_bucket, str(analysis["primary_protein"])),
        "selection_reason": selection_reason(analysis),
        "risk_notes": risk_notes(analysis),
        "selection_score": analysis["selection_score"],
        "title_quality_status": analysis["title_quality_status"],
        "is_weird_or_random": bool(analysis["is_weird_or_random"]),
        "is_too_american_processed": bool(analysis["is_too_american_processed"]),
        "is_dessert_like": bool(analysis["is_dessert_like"]),
        "is_drink": bool(analysis["is_drink"]),
        "is_pet_food": bool(analysis["is_pet_food"]),
        "is_component_only": bool(analysis["is_component_only"]),
        "has_clear_protein": bool(analysis["has_clear_protein"]),
        "has_clear_carb_or_veg": bool(analysis["has_clear_carb_or_veg"]),
        "ingredient_count": analysis["ingredient_count"],
        "step_count": analysis["step_count"],
        "expected_unit_to_grams_risk": analysis["expected_unit_to_grams_risk"],
        "expected_fooddb_gap_risk": analysis["expected_fooddb_gap_risk"],
        "expected_servings_risk": analysis["expected_servings_risk"],
        "manual_review_recommendation": analysis["manual_review_recommendation"],
        "ingredients_json": json.dumps(ingredients, ensure_ascii=False),
        "directions_json": json.dumps(directions, ensure_ascii=False),
    }


def expected_generator_value(target_bucket: str, primary_protein: str) -> str:
    if target_bucket == "breakfast_competitor":
        return "waffle_alternative"
    if primary_protein == "chicken":
        return "chicken_broccoli_pasta_alternative"
    if target_bucket == "carb_protein_main":
        return "multi_day_variety"
    return "chicken_broccoli_pasta_alternative"


def selection_reason(analysis: dict[str, Any]) -> str:
    return ";".join(
        [
            str(analysis["target_bucket"]),
            "protein_clear" if analysis["has_clear_protein"] else "protein_unclear",
            "carb_or_veg_clear" if analysis["has_clear_carb_or_veg"] else "carb_or_veg_unclear",
            f"mapping={analysis['expected_mapping_difficulty']}",
            "round29_anchor_repetition_gap",
        ]
    )


def risk_notes(analysis: dict[str, Any]) -> str:
    risks = analysis.get("risk_hits") or []
    if risks:
        return "risk_terms=" + "|".join(str(item) for item in risks)
    return "low_obvious_mapping_risk"


def exclusion_row(
    source_index: int,
    row: dict[str, str],
    analysis: dict[str, Any],
    reason: str,
) -> dict[str, object]:
    return {
        "source_index": source_index,
        "display_name": clean_text(row.get("recipe_title")),
        "source_category": clean_text(row.get("category")),
        "source_subcategory": clean_text(row.get("subcategory")),
        "exclusion_reason": reason,
        "target_bucket": analysis.get("target_bucket", ""),
        "primary_protein": analysis.get("primary_protein", ""),
        "has_carb_component": analysis.get("has_carb_component", False),
        "has_veg_component": analysis.get("has_veg_component", False),
        "ingredient_count": analysis.get("ingredient_count", 0),
        "step_count": analysis.get("step_count", 0),
        "selection_score": analysis.get("selection_score", 0),
    }


def select_targeted_plus15(candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    used_titles: set[str] = set()
    keepers = [
        row
        for row in candidates
        if row["manual_review_recommendation"] == "keep" and row["title_quality_status"] == "good"
    ]
    for bucket, quota in TARGET_QUOTAS.items():
        for row in sorted(keepers, key=lambda item: (-float(item["selection_score"]), str(item["display_name"]))):
            if str(row["target_bucket"]) != bucket:
                continue
            if len([item for item in selected if item["target_bucket"] == bucket]) >= quota:
                break
            title_key = normalize_title_key(row["display_name"])
            if title_key in used_titles:
                continue
            selected.append(row)
            used_titles.add(title_key)
    if len(selected) < 15:
        for row in sorted(keepers, key=lambda item: (-float(item["selection_score"]), str(item["display_name"]))):
            if len(selected) >= 15:
                break
            title_key = normalize_title_key(row["display_name"])
            if title_key in used_titles:
                continue
            selected.append(row)
            used_titles.add(title_key)
    return selected[:15]


def finalize_selected_row(index: int, row: dict[str, object]) -> dict[str, object]:
    finalized = dict(row)
    finalized["recipe_id_candidate"] = f"recipes_v1_2_round30_plus15_{index:03d}"
    return finalized


def finalize_candidate_pool_row(row: dict[str, object], index: int) -> dict[str, object]:
    finalized = dict(row)
    finalized["recipe_id_candidate"] = f"round30_candidate_pool_{index:03d}"
    return finalized


def build_summary(
    selected_rows: list[dict[str, object]],
    candidate_rows: list[dict[str, object]],
    exclusion_rows: list[dict[str, object]],
) -> str:
    selected_buckets = Counter(str(row["target_bucket"]) for row in selected_rows)
    candidate_buckets = Counter(str(row["target_bucket"]) for row in candidate_rows)
    selected_proteins = Counter(str(row["primary_protein"]) for row in selected_rows)
    exclusions = Counter(str(row["exclusion_reason"]) for row in exclusion_rows)
    lines = [
        "Recipes_DB v1.2 Round30 targeted +15 selection",
        "",
        f"source={SOURCE_RECIPES}",
        f"curated_200={CURATED_200}",
        f"plus30_dataset={PLUS30_RECIPES}",
        f"round28_selected_excluded={ROUND28_SELECTED.exists()}",
        f"round29_recommendation_available={ROUND29_RECOMMENDATION.exists()}",
        f"round29_anchor_analysis_available={ROUND29_ANCHOR_ANALYSIS.exists()}",
        "",
        f"candidate_pool_written={len(candidate_rows)}",
        f"selected_count={len(selected_rows)}",
        "",
        "Selected bucket mix:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in selected_buckets.most_common())
    lines.extend(["", "Candidate bucket mix:"])
    lines.extend(f"- {name}: {count}" for name, count in candidate_buckets.most_common())
    lines.extend(["", "Selected protein mix:"])
    lines.extend(f"- {name}: {count}" for name, count in selected_proteins.most_common())
    lines.extend(["", "Selected recipes:"])
    for row in selected_rows:
        lines.append(
            f"- {row['recipe_id_candidate']} | {row['target_bucket']} | {row['display_name']} | "
            f"{row['primary_protein']} | score={row['selection_score']} | {row['risk_notes']}"
        )
    lines.extend(["", "Top exclusions sampled:"])
    lines.extend(f"- {name}: {count}" for name, count in exclusions.most_common(20))
    lines.extend(
        [
            "",
            "Strict note:",
            "Round30 is targeted draft data only; weak or review-level recipes are not forced into the final selection.",
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


def normalize_title_key(value: object) -> str:
    text = normalize_text(value)
    text = re.sub(r"\b(best|easy|quick|simple|homemade|mom s|grandma s)\b", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


if __name__ == "__main__":
    main()

