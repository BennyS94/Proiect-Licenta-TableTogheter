# Recipes_DB Current

This folder contains the active app-facing recipe dataset used by TableTogether.

## Files

- `recipes.csv` - recipe metadata, slot eligibility, status flags, cooking time and cooking steps.
- `recipe_ingredients.csv` - ingredient rows mapped to canonical Food_DB entries.
- `recipe_nutrition_cache.csv` - cached nutrition per recipe, calculated from mapped ingredients.

## Runtime role

The backend calls the Python generator, and the generator reads this folder as the current recipe source. The mobile app receives generated plans through HTTP/JSON responses and does not read these CSV files directly.

## Current scope

The current app-facing dataset contains 326 recipes. It is curated for the TableTogether prototype and supports meal generation, recipe details, cooking steps, alternatives and grocery list aggregation.

Historical drafts, raw recipe sources and audit files are intentionally excluded from the public repository.
