# Recipes_DB Pilot Status

## Current Demo Package
- `v1_2_demo_final` is the current Generator v1 demo-final draft dataset.
- Path: `data/recipesdb/draft/v1_2_demo_final/`.
- Total recipes: `266`.
- Active recipes: `261`.
- Source: `v1_2_demo_candidate_round48_cleaned`.
- This is not production/current.
- `data/recipesdb/current` remains untouched.
- `data/fooddb/current` remains untouched.

Recommended demo config:
- `dataset_profile=v1_2_demo_final`
- `selection_mode=balanced_day`
- `portion_policy=target_aware`
- `meal_realism_mode=practical`
- `quality_gate=demo_safe`
- `days=3`
- `multi_day_mode=global_alternatives_3_day`
- `multi_day_no_repeat_policy=hard`
- `day_candidate_builder=direct_from_slots`
- `profile_guard=demo`

Smoke result:
- one-day valid/accept = true
- three-day valid = 3/3
- accept = 3/3
- repeated recipes = 0
- `multi_day_loss=0.006322`
- aggressive low-kcal edge profile is blocked by `profile_guard=demo`

## Completed For Now
- Food_DB v1 draft is accepted as the current working baseline.
- The pilot recipe subset is frozen.
- Pilot ingredient parsing is completed.
- The first and second conservative Food_DB mapping passes are completed.
- This pilot stage is considered good enough to move toward real table materialization and generator-oriented implementation.

## Not Final Yet
- `v1_2_demo_final` is demo-final draft only, not production QA.
- Family-level variety is still imperfect.
- Outlier risks remain with warnings.
- Household multi-member planning is not implemented.
- Grocery/price flow is not implemented.
- The `recipes` table is not yet materialized as the final table.
- The `recipe_ingredients` table is not yet materialized as the final table.
- `recipe_nutrition_cache` is not implemented yet.
- `recipe_components` remains future-ready only.
- The current ingredient-to-food mapping is usable for pilot validation, but not yet fully refined production-quality mapping.

## Current Active Working Files
Food_DB:
- `data/fooddb/current/fooddb_v1_core_master_draft.csv`

Recipes_DB:
- `data/recipesdb/current/recipes_pilot_subset_final.csv`
- `data/recipesdb/current/recipes_pilot_ingredients_parsed.csv`
- `data/recipesdb/current/recipe_ingredient_food_matches_draft.csv`

## Next Phase
- materialize `recipes`
- materialize `recipe_ingredients`
- build `recipe_nutrition_cache`
- continue toward generator adaptation on the new architecture
