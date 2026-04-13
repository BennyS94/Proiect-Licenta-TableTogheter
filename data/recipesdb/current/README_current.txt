Acest folder pastreaza view-ul curent de lucru pentru Recipes_DB pilot.

Fisierele din acest folder sunt copii ale subsetului activ, ale ingredientelor parse
si ale mappingului curent, pentru acces rapid la starea de lucru actuala.

Rol acum:
- `recipes_pilot_subset_final.csv` este subsetul pilot inghetat pentru parsing si mapping.
- `recipes_pilot_ingredients_parsed.csv` este tabelul intermediar curent de ingrediente parse.
- `recipe_ingredient_food_matches_draft.csv` este draftul curent de mapping catre Food_DB.

Istoric, audit si alte drafturi raman in:
- `data/recipesdb/draft/`
- `data/recipesdb/audit/`
- `data/recipesdb/source/`
