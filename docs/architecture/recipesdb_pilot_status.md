# Recipes_DB Pilot Status

## Current Reality

Exista doua niveluri care nu trebuie confundate:

1. `data/recipesdb/current`
   - baseline de lucru pentru Recipes_DB pilot;
   - contine tabele materializate pilot pentru retete, ingrediente si cache nutritional;
   - nu este datasetul app-facing principal al Generator v1 demo.

2. `data/recipesdb/draft/v1_2_demo_final`
   - pachet demo-final draft folosit de fluxurile generator/mobile/backend;
   - are acoperire mai buna pentru demo: sloturi, time layer, grocery/price coverage si QA app-facing;
   - nu este productie si nu este promovat in `current`.

## Current Recipes_DB Working Tables

Fisiere active in `data/recipesdb/current`:

- `recipes.csv`
  - rows: `106`
  - rol: tabel pilot materializat de retete
- `recipe_ingredients.csv`
  - rows: `947`
  - rol: ingrediente parse/mapping pentru retetele pilot
- `recipe_nutrition_cache.csv`
  - rows: `106`
  - rol: cache nutritional per reteta
- `recipe_components.csv`
  - rows: `0`
  - rol: placeholder future-ready pentru componente

Fisiere istorice/intermediare pastrate in acelasi folder:

- `recipes_pilot_subset_final.csv`
- `recipes_pilot_ingredients_parsed.csv`
- `recipe_ingredient_food_matches_draft.csv`

Acestea raman utile pentru audit si trasabilitate, dar tabelele care descriu structura Recipes_DB pilot materializata sunt `recipes.csv`, `recipe_ingredients.csv` si `recipe_nutrition_cache.csv`.

## Current Demo Package

Datasetul folosit in fluxurile app-facing/demo:

- `dataset_profile=v1_2_demo_final`
- path: `data/recipesdb/draft/v1_2_demo_final/`
- total recipes: `266`
- active recipes: `261`
- source: `v1_2_demo_candidate_round48_cleaned`
- status: demo-final draft, nu productie/current

Config demo recomandat:

- `selection_mode=balanced_day`
- `portion_policy=target_aware`
- `meal_realism_mode=practical`
- `quality_gate=demo_safe`
- `days=1..5`
- `multi_day_mode=global_alternatives_3_day`
- `multi_day_no_repeat_policy=hard`
- `day_candidate_builder=direct_from_slots`
- `profile_guard=demo`

## Implemented Around Recipes_DB

Pe baza acestor tabele/drafturi exista deja:

- Generator v1 recipe-based;
- generare individuala 1-5 zile;
- Household Generation v1 Lite;
- grocery list determinista;
- purchase suggestions;
- cooked-to-raw helpers pentru afisare shopping;
- price estimates demo/source-backed prin catalog, aliasuri si fallback-uri controlate;
- KNN-lite alternatives ca provider auxiliar;
- meal-level replacement explicit;
- integrare FastAPI backend si mobile Expo/React Native.

## Not Final Yet

- `v1_2_demo_final` este demo-final draft, nu productie QA.
- `data/recipesdb/current` este baseline pilot, nu baza finala production.
- `recipe_components.csv` ramane placeholder si nu sustine inca un model componentizat.
- Ingredient mapping-ul este utilizabil pentru pilot/demo, dar nu complet production-grade.
- Family-level variety si unele outlier risks raman imperfecte.
- Nu exista optimizare pe magazine/branduri, pantry inventory sau price fetching live.
- Nu exista ingredient-level substitution.
- Household Generation v1 Lite nu este optimizer household global.

## Next Phase

Directia corecta nu este intoarcerea la modelul vechi de itemi amestecati. Urmatorii pasi trebuie sa pastreze separarea:

- Food_DB = alimente canonice / ingrediente;
- Recipes_DB = retete compuse;
- Recipe Ingredients = legatura reteta-ingredient;
- Recipe Nutrition Cache = calcul/prestocare nutritionala pentru retete;
- Generator = selectie determinista pe retete, cu filtre hard si scoring.

Pentru evolutie spre aplicatia finala:

- mentine `v1_2_demo_final` ca pachet app-facing pana exista un pachet mai bun;
- imbunatateste `current` incremental, fara promovari automate;
- foloseste audituri pentru mapping, nutrition cache, time coverage si price coverage;
- pastreaza orice extindere KNN/ML ca strat auxiliar peste date curate, nu ca inlocuitor al baseline-ului rule/scoring-based.
