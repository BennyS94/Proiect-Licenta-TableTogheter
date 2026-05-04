from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.generator_v1.data_loader import (
    DEFAULT_INGREDIENTS_PATH,
    DEFAULT_NUTRITION_PATH,
    DEFAULT_RECIPES_PATH,
    load_recipe_candidate_pool,
)
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


def main() -> None:
    args = _parse_args()
    profile = load_member_profile(args.profile)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=args.recipes,
        ingredients_path=args.ingredients,
        nutrition_path=args.nutrition,
    )

    _print_target_summary(target)
    _print_pool_summary(pool.candidates, pool.eligible_candidates)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test pentru Generator v1.")
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--recipes", default=DEFAULT_RECIPES_PATH, type=Path)
    parser.add_argument("--ingredients", default=DEFAULT_INGREDIENTS_PATH, type=Path)
    parser.add_argument("--nutrition", default=DEFAULT_NUTRITION_PATH, type=Path)
    return parser.parse_args()


def _print_target_summary(target: NutritionTarget) -> None:
    print("Nutrition target")
    print(
        "  "
        f"kcal={target.kcal:.1f}, "
        f"protein_g={target.protein_g:.1f}, "
        f"carbs_g={target.carbs_g:.1f}, "
        f"fat_g={target.fat_g:.1f}"
    )
    print("Slot targets")
    for slot, values in target.slot_targets.items():
        print(
            "  "
            f"{slot}: "
            f"kcal={values['kcal']:.1f}, "
            f"protein_g={values['protein_g']:.1f}, "
            f"carbs_g={values['carbs_g']:.1f}, "
            f"fat_g={values['fat_g']:.1f}"
        )


def _print_pool_summary(candidates: pd.DataFrame, eligible_candidates: pd.DataFrame) -> None:
    print("Recipe candidate pool")
    print(f"  total_recipes_loaded={len(candidates)}")
    print(f"  eligible_candidate_count={len(eligible_candidates)}")
    print("First eligible recipes")

    preview_columns = [
        "recipe_id",
        "display_name",
        "energy_kcal_per_serving",
        "protein_g_per_serving",
        "carbs_g_per_serving",
        "fat_g_per_serving",
        "total_time_min",
    ]
    preview = eligible_candidates.loc[:, preview_columns].head(5)
    if preview.empty:
        print("  none")
        return

    for _, row in preview.iterrows():
        print(
            "  "
            f"{row['recipe_id']} | "
            f"{row['display_name']} | "
            f"kcal={_format_number(row['energy_kcal_per_serving'])}, "
            f"protein_g={_format_number(row['protein_g_per_serving'])}, "
            f"carbs_g={_format_number(row['carbs_g_per_serving'])}, "
            f"fat_g={_format_number(row['fat_g_per_serving'])}, "
            f"total_time_min={_format_number(row['total_time_min'], decimals=0)}"
        )


def _format_number(value: object, decimals: int = 1) -> str:
    if pd.isna(value):
        return "missing"
    return f"{float(value):.{decimals}f}"


if __name__ == "__main__":
    main()

