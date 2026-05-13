from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


DEFAULT_RECIPES_PATH = Path("data/recipesdb/current/recipes.csv")
DEFAULT_INGREDIENTS_PATH = Path("data/recipesdb/current/recipe_ingredients.csv")
DEFAULT_NUTRITION_PATH = Path("data/recipesdb/current/recipe_nutrition_cache.csv")
DEFAULT_FOODDB_PATH = Path("data/fooddb/current/fooddb_v1_core_master_draft.csv")
V1_1_GENERATOR_READY_RECIPES_PATH = Path("data/recipesdb/draft/v1_1_generator_ready/recipes.csv")
V1_1_GENERATOR_READY_INGREDIENTS_PATH = Path("data/recipesdb/draft/v1_1_generator_ready/recipe_ingredients.csv")
V1_1_GENERATOR_READY_NUTRITION_PATH = Path("data/recipesdb/draft/v1_1_generator_ready/recipe_nutrition_cache.csv")
V1_1_GENERATOR_READY_SLOT_CHECKED_RECIPES_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked/recipes.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_INGREDIENTS_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked/recipe_ingredients.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_NUTRITION_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked/recipe_nutrition_cache.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_RECIPES_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched/recipes.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_INGREDIENTS_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched/recipe_ingredients.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_NUTRITION_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched/recipe_nutrition_cache.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated/recipes.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated/recipe_ingredients.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated/recipe_nutrition_cache.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_RECIPES_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated_plus10/recipes.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_INGREDIENTS_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated_plus10/recipe_ingredients.csv"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_NUTRITION_PATH = Path(
    "data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated_plus10/recipe_nutrition_cache.csv"
)
V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH = Path(
    "data/recipesdb/draft/v1_2_generator_ready_plus30/recipes.csv"
)
V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH = Path(
    "data/recipesdb/draft/v1_2_generator_ready_plus30/recipe_ingredients.csv"
)
V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH = Path(
    "data/recipesdb/draft/v1_2_generator_ready_plus30/recipe_nutrition_cache.csv"
)
V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH = Path(
    "data/recipesdb/draft/v1_2_generator_ready_plus30_plus15/recipes.csv"
)
V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH = Path(
    "data/recipesdb/draft/v1_2_generator_ready_plus30_plus15/recipe_ingredients.csv"
)
V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH = Path(
    "data/recipesdb/draft/v1_2_generator_ready_plus30_plus15/recipe_nutrition_cache.csv"
)

PILOT_CURRENT_PROFILE = "pilot_current"
V1_1_GENERATOR_READY_PROFILE = "v1_1_generator_ready"
V1_1_GENERATOR_READY_SLOT_CHECKED_PROFILE = "v1_1_generator_ready_slot_checked"
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_PROFILE = (
    "v1_1_generator_ready_slot_checked_time_enriched"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE = (
    "v1_1_generator_ready_slot_checked_time_enriched_snack_curated"
)
V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE = (
    "v1_1_generator_ready_slot_checked_time_enriched_snack_curated_plus10"
)
V1_2_GENERATOR_READY_PLUS30_PROFILE = "v1_2_generator_ready_plus30"
V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE = "v1_2_generator_ready_plus30_plus15"

DATASET_PROFILE_PRESETS = {
    PILOT_CURRENT_PROFILE: {
        "allowed_scope_statuses": {"pilot_validated"},
        "allowed_cache_statuses": {"partial_from_mapped_ingredients"},
        "min_mapped_weight_ratio": None,
        "require_per_serving_macros": False,
    },
    V1_1_GENERATOR_READY_PROFILE: {
        "allowed_scope_statuses": {"v1_1_generator_ready_draft"},
        "allowed_cache_statuses": {
            "usable_from_mapped_ingredients",
            "partial_from_mapped_ingredients",
        },
        "min_mapped_weight_ratio": None,
        "require_per_serving_macros": True,
    },
    V1_1_GENERATOR_READY_SLOT_CHECKED_PROFILE: {
        "allowed_scope_statuses": {"v1_1_generator_ready_slot_checked_draft"},
        "allowed_cache_statuses": {
            "usable_from_mapped_ingredients",
            "partial_from_mapped_ingredients",
        },
        "min_mapped_weight_ratio": None,
        "require_per_serving_macros": True,
    },
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_PROFILE: {
        "allowed_scope_statuses": {"v1_1_generator_ready_slot_checked_time_enriched_draft"},
        "allowed_cache_statuses": {
            "usable_from_mapped_ingredients",
            "partial_from_mapped_ingredients",
        },
        "min_mapped_weight_ratio": None,
        "require_per_serving_macros": True,
    },
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE: {
        "allowed_scope_statuses": {"v1_1_generator_ready_slot_checked_time_enriched_snack_curated_draft"},
        "allowed_cache_statuses": {
            "usable_from_mapped_ingredients",
            "partial_from_mapped_ingredients",
        },
        "min_mapped_weight_ratio": None,
        "require_per_serving_macros": True,
    },
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE: {
        "allowed_scope_statuses": {
            "v1_1_generator_ready_slot_checked_time_enriched_snack_curated_draft",
            "v1_1_generator_ready_draft",
        },
        "allowed_cache_statuses": {
            "usable_from_mapped_ingredients",
            "partial_from_mapped_ingredients",
        },
        "min_mapped_weight_ratio": None,
        "require_per_serving_macros": True,
    },
    V1_2_GENERATOR_READY_PLUS30_PROFILE: {
        "allowed_scope_statuses": {
            "v1_1_generator_ready_slot_checked_time_enriched_snack_curated_draft",
            "v1_1_generator_ready_draft",
            "v1_2_generator_ready_draft",
        },
        "allowed_cache_statuses": {
            "usable_from_mapped_ingredients",
            "partial_from_mapped_ingredients",
        },
        "min_mapped_weight_ratio": None,
        "require_per_serving_macros": True,
    },
    V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE: {
        "allowed_scope_statuses": {
            "v1_1_generator_ready_slot_checked_time_enriched_snack_curated_draft",
            "v1_1_generator_ready_draft",
            "v1_2_generator_ready_draft",
        },
        "allowed_cache_statuses": {
            "usable_from_mapped_ingredients",
            "partial_from_mapped_ingredients",
        },
        "min_mapped_weight_ratio": None,
        "require_per_serving_macros": True,
    },
}


@dataclass(frozen=True)
class RecipeCandidatePool:
    recipes: pd.DataFrame
    ingredients: pd.DataFrame
    nutrition: pd.DataFrame
    candidates: pd.DataFrame
    eligible_candidates: pd.DataFrame
    loader_diagnostics: dict[str, object]


def load_recipe_candidate_pool(
    recipes_path: str | Path = DEFAULT_RECIPES_PATH,
    ingredients_path: str | Path = DEFAULT_INGREDIENTS_PATH,
    nutrition_path: str | Path = DEFAULT_NUTRITION_PATH,
    allowed_scope_statuses: set[str] | list[str] | None = None,
    allowed_cache_statuses: set[str] | list[str] | None = None,
    min_mapped_weight_ratio: float | None = None,
    dataset_profile: str | None = None,
) -> RecipeCandidatePool:
    recipes = pd.read_csv(Path(recipes_path))
    ingredients = pd.read_csv(Path(ingredients_path))
    nutrition = pd.read_csv(Path(nutrition_path))
    eligibility_config = _resolve_eligibility_config(
        dataset_profile=dataset_profile,
        allowed_scope_statuses=allowed_scope_statuses,
        allowed_cache_statuses=allowed_cache_statuses,
        min_mapped_weight_ratio=min_mapped_weight_ratio,
    )

    _require_columns(
        recipes,
        {
            "recipe_id",
            "display_name",
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
    loader_diagnostics: dict[str, object] = {
        "dataset_profile": eligibility_config["dataset_profile"],
        "allowed_scope_statuses": sorted(eligibility_config["allowed_scope_statuses"]),
        "allowed_cache_statuses": sorted(eligibility_config["allowed_cache_statuses"]),
        "min_mapped_weight_ratio": eligibility_config["min_mapped_weight_ratio"],
        "require_per_serving_macros": eligibility_config["require_per_serving_macros"],
        "warnings": [],
    }
    eligible_candidates = candidates.loc[_eligible_mask(candidates, eligibility_config, loader_diagnostics)].copy()

    return RecipeCandidatePool(
        recipes=recipes,
        ingredients=ingredients,
        nutrition=nutrition,
        candidates=candidates,
        eligible_candidates=eligible_candidates,
        loader_diagnostics=loader_diagnostics,
    )


def load_fooddb_current(
    fooddb_path: str | Path = DEFAULT_FOODDB_PATH,
) -> pd.DataFrame:
    fooddb = pd.read_csv(Path(fooddb_path))
    _require_columns(
        fooddb,
        {
            "food_id",
            "canonical_name",
            "energy_kcal_100g",
            "protein_g_100g",
            "carbs_g_100g",
            "fat_g_100g",
        },
        "fooddb_current",
    )
    return fooddb


def _resolve_eligibility_config(
    dataset_profile: str | None,
    allowed_scope_statuses: set[str] | list[str] | None,
    allowed_cache_statuses: set[str] | list[str] | None,
    min_mapped_weight_ratio: float | None,
) -> dict[str, object]:
    profile = (dataset_profile or "").strip() or PILOT_CURRENT_PROFILE
    if profile not in DATASET_PROFILE_PRESETS:
        known_profiles = ", ".join(sorted(DATASET_PROFILE_PRESETS))
        raise ValueError(f"dataset_profile necunoscut: {profile!r}. Valori acceptate: {known_profiles}")

    preset = dict(DATASET_PROFILE_PRESETS[profile])
    if allowed_scope_statuses is not None:
        preset["allowed_scope_statuses"] = _as_string_set(allowed_scope_statuses)
    if allowed_cache_statuses is not None:
        preset["allowed_cache_statuses"] = _as_string_set(allowed_cache_statuses)
    if min_mapped_weight_ratio is not None:
        preset["min_mapped_weight_ratio"] = min_mapped_weight_ratio
    preset["dataset_profile"] = profile
    return preset


def _eligible_mask(
    candidates: pd.DataFrame,
    config: dict[str, object],
    diagnostics: dict[str, object],
) -> pd.Series:
    mask = pd.Series(True, index=candidates.index)
    mask &= _isin_column(
        candidates,
        "scope_status",
        config["allowed_scope_statuses"],
        diagnostics,
        required=True,
    )
    mask &= _isin_column(
        candidates,
        "cache_status",
        config["allowed_cache_statuses"],
        diagnostics,
        required=True,
    )
    mask &= _equals_if_present(candidates, "is_active", 1, diagnostics)
    mask &= _equals_if_present(candidates, "has_ingredients_parsed", 1, diagnostics)

    min_ratio = config.get("min_mapped_weight_ratio")
    if min_ratio is not None:
        mask &= _min_numeric_if_present(candidates, "mapped_weight_ratio", float(min_ratio), diagnostics)

    if bool(config.get("require_per_serving_macros")):
        for column in (
            "energy_kcal_per_serving",
            "protein_g_per_serving",
            "carbs_g_per_serving",
            "fat_g_per_serving",
        ):
            mask &= _numeric_present(candidates, column, diagnostics)

    diagnostics["eligible_candidate_count"] = int(mask.sum())
    return mask


def _as_string_set(values: set[str] | list[str]) -> set[str]:
    return {str(value).strip() for value in values if str(value).strip()}


def _isin_column(
    df: pd.DataFrame,
    column: str,
    allowed_values: object,
    diagnostics: dict[str, object],
    required: bool,
) -> pd.Series:
    if column not in df.columns:
        diagnostics["warnings"].append(f"missing_column:{column}")
        return pd.Series(not required, index=df.index)
    allowed = _as_string_set(list(allowed_values or []))
    return df[column].fillna("").astype(str).isin(allowed)


def _equals_if_present(
    df: pd.DataFrame,
    column: str,
    expected_value: object,
    diagnostics: dict[str, object],
) -> pd.Series:
    if column not in df.columns:
        diagnostics["warnings"].append(f"missing_optional_filter_column:{column}")
        return pd.Series(False, index=df.index)
    return pd.to_numeric(df[column], errors="coerce").eq(float(expected_value))


def _min_numeric_if_present(
    df: pd.DataFrame,
    column: str,
    minimum: float,
    diagnostics: dict[str, object],
) -> pd.Series:
    if column not in df.columns:
        diagnostics["warnings"].append(f"missing_optional_filter_column:{column}")
        return pd.Series(False, index=df.index)
    return pd.to_numeric(df[column], errors="coerce").ge(minimum)


def _numeric_present(
    df: pd.DataFrame,
    column: str,
    diagnostics: dict[str, object],
) -> pd.Series:
    if column not in df.columns:
        diagnostics["warnings"].append(f"missing_required_macro_column:{column}")
        return pd.Series(False, index=df.index)
    return pd.to_numeric(df[column], errors="coerce").notna()


def _require_columns(df: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"Lipsesc coloane in {source_name}: {', '.join(missing)}")
