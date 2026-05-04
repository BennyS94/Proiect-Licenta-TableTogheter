from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


DEFAULT_RECIPES_PATH = Path("data/recipesdb/current/recipes.csv")
DEFAULT_INGREDIENTS_PATH = Path("data/recipesdb/current/recipe_ingredients.csv")
DEFAULT_NUTRITION_PATH = Path("data/recipesdb/current/recipe_nutrition_cache.csv")


@dataclass(frozen=True)
class RecipeCandidatePool:
    recipes: pd.DataFrame
    ingredients: pd.DataFrame
    nutrition: pd.DataFrame
    candidates: pd.DataFrame
    eligible_candidates: pd.DataFrame


def load_recipe_candidate_pool(
    recipes_path: str | Path = DEFAULT_RECIPES_PATH,
    ingredients_path: str | Path = DEFAULT_INGREDIENTS_PATH,
    nutrition_path: str | Path = DEFAULT_NUTRITION_PATH,
) -> RecipeCandidatePool:
    recipes = pd.read_csv(Path(recipes_path))
    ingredients = pd.read_csv(Path(ingredients_path))
    nutrition = pd.read_csv(Path(nutrition_path))

    _require_columns(
        recipes,
        {
            "recipe_id",
            "display_name",
            "scope_status",
            "is_active",
            "has_ingredients_parsed",
            "total_time_min",
        },
        "recipes",
    )
    _require_columns(
        nutrition,
        {
            "recipe_id",
            "cache_status",
            "energy_kcal_per_serving",
            "protein_g_per_serving",
            "carbs_g_per_serving",
            "fat_g_per_serving",
        },
        "recipe_nutrition_cache",
    )
    _require_columns(ingredients, {"recipe_id"}, "recipe_ingredients")

    candidates = recipes.merge(
        nutrition,
        on="recipe_id",
        how="left",
        suffixes=("", "_nutrition"),
    )
    eligible_candidates = candidates.loc[_eligible_mask(candidates)].copy()

    return RecipeCandidatePool(
        recipes=recipes,
        ingredients=ingredients,
        nutrition=nutrition,
        candidates=candidates,
        eligible_candidates=eligible_candidates,
    )


def _eligible_mask(candidates: pd.DataFrame) -> pd.Series:
    # has_nutrition_cache este structural in pilot, nu dovada de nutritie utilizabila.
    return (
        candidates["scope_status"].eq("pilot_validated")
        & candidates["is_active"].eq(1)
        & candidates["has_ingredients_parsed"].eq(1)
        & candidates["cache_status"].eq("partial_from_mapped_ingredients")
    )


def _require_columns(df: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"Lipsesc coloane in {source_name}: {', '.join(missing)}")

