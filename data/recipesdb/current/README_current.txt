Acest folder pastreaza view-ul curent de lucru pentru Recipes_DB pilot.

Important:
- `current` este baseline de lucru/pilot pentru Recipes_DB.
- Fluxurile app-facing ale Generator v1 folosesc in prezent `dataset_profile=v1_2_demo_final`
  din `data/recipesdb/draft/v1_2_demo_final/`, nu acest folder.
- Nu promova automat fisiere draft in `current` fara audit si decizie explicita.

Tabele pilot materializate:
- `recipes.csv`
  - tabel de retete pilot materializate
  - rows curent verificate: 106
- `recipe_ingredients.csv`
  - ingrediente parse/mapping pentru retetele pilot
  - rows curent verificate: 947
- `recipe_nutrition_cache.csv`
  - cache nutritional per reteta
  - rows curent verificate: 106
- `recipe_components.csv`
  - placeholder future-ready
  - rows curent verificate: 0

Fisiere istorice/intermediare pastrate pentru audit si trasabilitate:
- `recipes_pilot_subset_final.csv`
- `recipes_pilot_ingredients_parsed.csv`
- `recipe_ingredient_food_matches_draft.csv`

Rol acum:
- pastreaza structura conceptuala Food_DB / Recipes_DB / Recipe Ingredients / Nutrition Cache;
- sustine auditul si evolutia controlata a datelor;
- nu inlocuieste inca pachetul demo-final folosit in aplicatia mobila.

Istoric, audit si alte drafturi raman in:
- `data/recipesdb/draft/`
- `data/recipesdb/audit/`
- `data/recipesdb/source/`
