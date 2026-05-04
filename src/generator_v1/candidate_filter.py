from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


DIETARY_KEYS = (
    "vegetarian",
    "vegan",
    "gluten_free",
    "no_beef",
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


@dataclass(frozen=True)
class HouseholdPreferenceContext:
    banned_recipe_ids: set[str] = field(default_factory=set)
    banned_ingredient_names: set[str] = field(default_factory=set)
    dietary_preferences: dict[str, bool] = field(default_factory=dict)
    time_sensitivity: str = "normal"


def build_household_preference_context(profile: dict[str, Any]) -> HouseholdPreferenceContext:
    dietary_preferences = profile.get("dietary_preferences") or {}
    meal_config = profile.get("meal_config") or {}

    return HouseholdPreferenceContext(
        banned_recipe_ids=_as_string_set(profile.get("banned_recipe_ids", [])),
        banned_ingredient_names=_as_normalized_set(profile.get("banned_ingredient_names", [])),
        dietary_preferences={
            key: bool(dietary_preferences.get(key, False))
            for key in DIETARY_KEYS
        },
        time_sensitivity=str(
            meal_config.get("time_sensitivity", profile.get("time_sensitivity", "normal"))
        ).strip().lower(),
    )


def filter_recipe_candidates(
    eligible_candidates: pd.DataFrame,
    ingredients: pd.DataFrame,
    context: HouseholdPreferenceContext,
) -> pd.DataFrame:
    if eligible_candidates.empty:
        return eligible_candidates.copy()

    filtered = eligible_candidates.copy()
    if context.banned_recipe_ids:
        filtered = filtered.loc[
            ~filtered["recipe_id"].astype(str).isin(context.banned_recipe_ids)
        ].copy()

    banned_ingredient_recipe_ids = _recipe_ids_with_banned_ingredients(
        ingredients=ingredients,
        banned_ingredient_names=context.banned_ingredient_names,
        dietary_preferences=context.dietary_preferences,
    )
    if banned_ingredient_recipe_ids:
        filtered = filtered.loc[
            ~filtered["recipe_id"].astype(str).isin(banned_ingredient_recipe_ids)
        ].copy()

    return filtered


def _recipe_ids_with_banned_ingredients(
    ingredients: pd.DataFrame,
    banned_ingredient_names: set[str],
    dietary_preferences: dict[str, bool],
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

    return set(ingredients.loc[mask, "recipe_id"].astype(str))


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


def _normalize_text(value: Any) -> str:
    text = str(value).lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _keyword_pattern(keyword: str) -> str:
    normalized = _normalize_text(keyword)
    if not normalized:
        return r"a^"
    return rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])"
