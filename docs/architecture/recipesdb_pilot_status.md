# Recipes_DB Pilot Status

## Completed For Now
- Food_DB v1 draft is accepted as the current working baseline.
- The pilot recipe subset is frozen.
- Pilot ingredient parsing is completed.
- The first and second conservative Food_DB mapping passes are completed.
- This pilot stage is considered good enough to move toward real table materialization and generator-oriented implementation.

## Not Final Yet
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
