from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.generator_v1.health_diet_fit import normalize_health_and_diet_preferences


DIETARY_KEYS = (
    "vegetarian",
    "vegan",
    "gluten_free",
    "no_beef",
    "no_pork",
    "no_chicken",
    "no_fish",
    "no_dairy",
)

INGREDIENT_TEXT_COLUMNS = (
    "ingredient_name_normalized",
    "ingredient_name_parsed",
    "mapped_food_canonical_name",
    "ingredient_raw_text",
)

DIETARY_KEYWORDS = {
    "no_beef": {"beef", "steak", "veal"},
    "no_pork": {
        "bacon",
        "chorizo",
        "ham",
        "ham hock",
        "pancetta",
        "pepperoni",
        "pork",
        "pork chop",
        "pork loin",
        "pork shoulder",
        "pork sausage",
        "pork tenderloin",
        "prosciutto",
        "salami",
    },
    "no_chicken": {"chicken", "poultry"},
    "no_fish": {
        "anchovy",
        "clam",
        "cod",
        "crab",
        "fish",
        "halibut",
        "mussel",
        "oyster",
        "salmon",
        "scallop",
        "seafood",
        "shrimp",
        "tilapia",
        "tuna",
    },
    "no_dairy": {
        "butter",
        "cheddar",
        "cheese",
        "cream",
        "dairy",
        "milk",
        "mozzarella",
        "parmesan",
        "sour cream",
        "yogurt",
    },
    "vegetarian": {
        "bacon",
        "beef",
        "chicken",
        "fish",
        "ham",
        "lamb",
        "meat",
        "pork",
        "prosciutto",
        "salami",
        "sausage",
        "seafood",
        "shrimp",
        "turkey",
        "veal",
    },
    "vegan": {
        "bacon",
        "beef",
        "butter",
        "cheese",
        "chicken",
        "cream",
        "dairy",
        "egg",
        "fish",
        "ham",
        "honey",
        "lamb",
        "meat",
        "milk",
        "pork",
        "sausage",
        "seafood",
        "shrimp",
        "turkey",
        "yogurt",
    },
    "gluten_free": {
        "barley",
        "bread",
        "breadcrumb",
        "couscous",
        "flour",
        "noodle",
        "pasta",
        "rye",
        "tortilla",
        "wheat",
    },
}

FOOD_RATING_TO_DIETARY_KEY = {
    "beef": "no_beef",
    "pork": "no_pork",
    "chicken": "no_chicken",
    "fish": "no_fish",
    "dairy": "no_dairy",
}

FOOD_PREFERENCE_AVOID_KEYWORDS = {
    "beef": DIETARY_KEYWORDS["no_beef"],
    "pork": DIETARY_KEYWORDS["no_pork"],
    "chicken": DIETARY_KEYWORDS["no_chicken"],
    "fish": DIETARY_KEYWORDS["no_fish"],
    "dairy": DIETARY_KEYWORDS["no_dairy"],
    "eggs": {"egg", "eggs"},
    "beans": {
        "bean",
        "beans",
        "black bean",
        "chickpea",
        "garbanzo",
        "kidney bean",
        "lentil",
        "pinto bean",
    },
    "mushrooms": {"mushroom", "mushrooms"},
    "onions": {"onion", "onions", "shallot", "shallots"},
    "turkey": {"turkey"},
    "rice": {"rice"},
    "pasta": {"noodle", "noodles", "pasta"},
    "potatoes": {"potato", "potatoes"},
    "oats": {"oat", "oats", "oatmeal"},
    "bread": {"bread", "breadcrumb", "breadcrumbs"},
    "vegetables": {"vegetable", "vegetables"},
    "soups": {"soup"},
    "spicy_food": {"cayenne", "chili", "chilli", "hot sauce", "jalapeno", "spicy"},
}

DIETARY_PATTERN_HARD_KEYWORDS = {
    "keto": {
        "bread",
        "breadcrumb",
        "corn",
        "flour",
        "honey",
        "noodle",
        "pasta",
        "potato",
        "rice",
        "sugar",
        "syrup",
        "tortilla",
    },
    "paleo": {
        "bean",
        "beans",
        "bread",
        "cheese",
        "cream",
        "flour",
        "lentil",
        "milk",
        "noodle",
        "oat",
        "pasta",
        "rice",
        "soy sauce",
        "sugar",
        "tortilla",
        "yogurt",
    },
}


@dataclass(frozen=True)
class HouseholdPreferenceContext:
    banned_recipe_ids: set[str] = field(default_factory=set)
    banned_ingredient_names: set[str] = field(default_factory=set)
    dietary_preferences: dict[str, bool] = field(default_factory=dict)
    health_and_diet_preferences: dict[str, dict[str, bool]] = field(default_factory=dict)
    time_sensitivity: str = "normal"


def build_household_preference_context(profile: dict[str, Any]) -> HouseholdPreferenceContext:
    dietary_preferences = profile.get("dietary_preferences") or {}
    food_preferences = profile.get("food_preferences") or {}
    health_and_diet_preferences = normalize_health_and_diet_preferences(
        profile.get("health_and_diet_preferences")
    )
    meal_config = profile.get("meal_config") or {}
    resolved_dietary_preferences = {
        key: bool(dietary_preferences.get(key, False))
        for key in DIETARY_KEYS
    }
    resolved_banned_ingredients = _as_normalized_set(
        profile.get("banned_ingredient_names", [])
    )
    resolved_banned_ingredients |= _food_preference_avoid_terms(
        food_preferences=food_preferences,
        dietary_preferences=resolved_dietary_preferences,
    )
    resolved_banned_ingredients |= _as_normalized_set(
        _food_preferences_avoid_ingredients(food_preferences)
    )

    return HouseholdPreferenceContext(
        banned_recipe_ids=_as_string_set(profile.get("banned_recipe_ids", [])),
        banned_ingredient_names=resolved_banned_ingredients,
        dietary_preferences=resolved_dietary_preferences,
        health_and_diet_preferences=health_and_diet_preferences,
        time_sensitivity=str(
            meal_config.get("time_sensitivity", profile.get("time_sensitivity", "normal"))
        ).strip().lower(),
    )


def filter_recipe_candidates(
    eligible_candidates: pd.DataFrame,
    ingredients: pd.DataFrame,
    context: HouseholdPreferenceContext,
    feedback_preference_context: Mapping[str, object] | None = None,
) -> pd.DataFrame:
    if eligible_candidates.empty:
        filtered = eligible_candidates.copy()
        filtered.attrs["filter_diagnostics"] = _filter_diagnostics(
            eligible_candidates=eligible_candidates,
            filtered_candidates=filtered,
            feedback_banned_recipe_ids=set(),
            banned_ingredient_recipe_ids=set(),
        )
        return filtered

    filtered = eligible_candidates.copy()
    feedback_banned_recipe_ids = _feedback_banned_recipe_ids(feedback_preference_context)
    banned_recipe_ids = set(context.banned_recipe_ids) | feedback_banned_recipe_ids
    if banned_recipe_ids and "recipe_id" in filtered.columns:
        filtered = filtered.loc[
            ~filtered["recipe_id"].astype(str).isin(banned_recipe_ids)
        ].copy()

    banned_ingredient_recipe_ids = _recipe_ids_with_banned_ingredients(
        ingredients=ingredients,
        banned_ingredient_names=context.banned_ingredient_names,
        dietary_preferences=context.dietary_preferences,
        health_and_diet_preferences=context.health_and_diet_preferences,
    )
    if banned_ingredient_recipe_ids:
        filtered = filtered.loc[
            ~filtered["recipe_id"].astype(str).isin(banned_ingredient_recipe_ids)
        ].copy()

    filtered.attrs["filter_diagnostics"] = _filter_diagnostics(
        eligible_candidates=eligible_candidates,
        filtered_candidates=filtered,
        feedback_banned_recipe_ids=feedback_banned_recipe_ids,
        banned_ingredient_recipe_ids=banned_ingredient_recipe_ids,
    )
    return filtered


def _recipe_ids_with_banned_ingredients(
    ingredients: pd.DataFrame,
    banned_ingredient_names: set[str],
    dietary_preferences: dict[str, bool],
    health_and_diet_preferences: dict[str, dict[str, bool]],
) -> set[str]:
    required_columns = {"recipe_id", *INGREDIENT_TEXT_COLUMNS}
    available_columns = [col for col in INGREDIENT_TEXT_COLUMNS if col in ingredients.columns]
    if "recipe_id" not in ingredients.columns or not available_columns:
        # TODO: cand Recipes_DB are flags canonice de dieta/alergeni, filtreaza pe ele.
        return set()

    text = _ingredient_search_text(ingredients, available_columns)
    mask = pd.Series(False, index=ingredients.index)

    for banned_name in banned_ingredient_names:
        mask |= text.str.contains(_keyword_pattern(banned_name), regex=True, na=False)

    for preference_key, enabled in dietary_preferences.items():
        if not enabled:
            continue
        for keyword in DIETARY_KEYWORDS.get(preference_key, set()):
            mask |= text.str.contains(_keyword_pattern(keyword), regex=True, na=False)

    dietary_patterns = health_and_diet_preferences.get("dietary_patterns", {})
    for pattern_key, enabled in dietary_patterns.items():
        if not enabled:
            continue
        for keyword in DIETARY_PATTERN_HARD_KEYWORDS.get(pattern_key, set()):
            mask |= text.str.contains(_keyword_pattern(keyword), regex=True, na=False)

    return set(ingredients.loc[mask, "recipe_id"].astype(str))


def _food_preference_avoid_terms(
    food_preferences: Any,
    dietary_preferences: dict[str, bool],
) -> set[str]:
    if not isinstance(food_preferences, Mapping):
        return set()
    ratings = food_preferences.get("ratings")
    if not isinstance(ratings, Mapping):
        return set()

    terms: set[str] = set()
    for raw_key, raw_rating in ratings.items():
        food_key = _normalize_preference_key(raw_key)
        rating = _normalize_text(raw_rating)
        if rating != "avoid":
            continue
        dietary_key = FOOD_RATING_TO_DIETARY_KEY.get(food_key)
        if dietary_key:
            dietary_preferences[dietary_key] = True
        terms |= {
            _normalize_text(keyword)
            for keyword in FOOD_PREFERENCE_AVOID_KEYWORDS.get(food_key, set())
            if _normalize_text(keyword)
        }
    return terms


def _food_preferences_avoid_ingredients(food_preferences: Any) -> list[str]:
    if not isinstance(food_preferences, Mapping):
        return []
    values = food_preferences.get("avoid_ingredients")
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    try:
        return [str(value).strip() for value in values if str(value).strip()]
    except TypeError:
        return []


def _ingredient_search_text(
    ingredients: pd.DataFrame,
    columns: list[str],
) -> pd.Series:
    normalized_parts = []
    for column in columns:
        normalized_parts.append(
            ingredients[column]
            .fillna("")
            .astype(str)
            .map(_normalize_text)
        )
    return pd.concat(normalized_parts, axis=1).agg(" ".join, axis=1)


def _as_normalized_set(values: Any) -> set[str]:
    if values is None:
        return set()
    if isinstance(values, str):
        values = [values]
    return {
        _normalize_text(value)
        for value in values
        if str(value).strip()
    }


def _as_string_set(values: Any) -> set[str]:
    if values is None:
        return set()
    if isinstance(values, str):
        values = [values]
    return {
        str(value).strip()
        for value in values
        if str(value).strip()
    }


def _feedback_banned_recipe_ids(
    feedback_preference_context: Mapping[str, object] | None,
) -> set[str]:
    if not isinstance(feedback_preference_context, Mapping):
        return set()
    hard_filters = feedback_preference_context.get("hard_filters")
    if not isinstance(hard_filters, Mapping):
        return set()
    return _as_string_set(hard_filters.get("banned_recipe_ids", []))


def _filter_diagnostics(
    eligible_candidates: pd.DataFrame,
    filtered_candidates: pd.DataFrame,
    feedback_banned_recipe_ids: set[str],
    banned_ingredient_recipe_ids: set[str],
) -> dict[str, object]:
    eligible_ids = _recipe_ids_from_frame(eligible_candidates)
    filtered_ids = _recipe_ids_from_frame(filtered_candidates)
    filtered_by_explicit_avoid = sorted(
        recipe_id
        for recipe_id in feedback_banned_recipe_ids
        if recipe_id in eligible_ids and recipe_id not in filtered_ids
    )
    return {
        "filtered_by_explicit_avoid": len(filtered_by_explicit_avoid),
        "filtered_by_explicit_avoid_recipe_ids": filtered_by_explicit_avoid,
        "filtered_by_banned_ingredients": len(banned_ingredient_recipe_ids),
    }


def _recipe_ids_from_frame(frame: pd.DataFrame) -> set[str]:
    if "recipe_id" not in frame.columns:
        return set()
    return set(frame["recipe_id"].astype(str))


def _normalize_text(value: Any) -> str:
    text = str(value).lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_preference_key(value: Any) -> str:
    return _normalize_text(value).replace(" ", "_")


def _keyword_pattern(keyword: str) -> str:
    normalized = _normalize_text(keyword)
    if not normalized:
        return r"a^"
    return rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])"
