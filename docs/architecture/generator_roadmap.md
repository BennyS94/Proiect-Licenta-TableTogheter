# Generator roadmap

## Premisa curenta

TableTogether evolueaza incremental dintr-un demo tehnic spre o aplicatie Android-first folosibila. Pentru comoditate de dezvoltare, interfata mobila ruleaza in Expo, dar directia produsului nu este un prototip izolat: mobile-ul consuma backend-ul FastAPI, iar generatorul Python ramane in backend.

Modelul de produs curent:
- un cont local principal;
- un household asociat contului;
- mai multe `member_profile` interne;
- membrii household-ului nu sunt conturi/utilizatori separati;
- un plan poate fi individual sau household;
- feedback-ul este explicit si se aplica la generari viitoare;
- grocery list ramane agregata la nivel de plan/household.

Model conceptual minim:
- `account`
- `household`
- `member_profile`
- `plan`
- `grocery_list`
- `feedback_event`

## Status implementat

Checkpoint-uri de generator:
- Checkpoint 0: contract generator, profil, `nutrition_target`, feedback, hard filters, scoring - conceptual complet.
- Checkpoint 1: generare 1 zi pentru un profil - demo/testing-ready.
- Checkpoint 2: multi-day v1 draft 1-5 zile - implementat/demo-ready.
- Checkpoint 3: feedback explicit - implementat in generator si integrat cu SQLite prin backend.
- Checkpoint 4: grocery list, purchase suggestions, cooked-to-raw helpers si price estimates demo - implementate.
- Checkpoint 5: Household Generation v1 Lite - implementat ca strat functional MVP/demo, nu optimizer global.

Checkpoint-uri de produs/API/mobile:
- FastAPI backend exista si expune generare individuala, generare household, profile, feedback, auth local, alternatives si meal replacement.
- SQLite local/demo persista conturi, sesiuni, household-uri, profile, feedback, planuri generate si grocery lists.
- Mobile Expo/React Native consuma backend-ul prin HTTP/JSON si nu ruleaza generatorul local.
- Meal Plan este family-first: un singur buton `Generate meal plan`, cu alegere interna intre individual si household in functie de numarul de profiluri active.
- KNN-lite este integrat ca provider de alternative, iar replacement-ul este explicit, la nivel de masa/reteta intreaga.

## Dataset si configuratie demo

Datasetul principal pentru fluxurile app-facing/demo este:
- `dataset_profile=current`
- path: `data/recipesdb/current/`
- total recipes: `266`
- active recipes: `261`
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
- `household_mode=individual_breakfast_shared_main`
- `household_allocation_mode=macro_aware_simple`

`data/recipesdb/current` si `data/fooddb/current` raman baseline-uri de lucru pentru Food_DB / Recipes_DB pilot. Ele nu sunt acelasi lucru cu pachetul demo-final folosit de generatorul app-facing.

## Rolul KNN

KNN nu este motorul principal in v1.

Rolul curent:
- provider auxiliar de alternative de retete;
- generator approval gate pentru slot, profil, feedback, macro, timp si realism;
- preview si aplicare explicita de meal-level replacement;
- fara inlocuire automata si fara ingredient-level substitution.

## Rolul Streamlit

Streamlit ramane unealta de testare si audit. Produsul principal se muta treptat spre Android mobile + FastAPI backend.

## Explicit in afara scope-ului curent

- OR-Tools / MILP / CP-SAT.
- KNN/ML ca motor principal de selectie.
- live price fetching sau scraping runtime.
- store, brand, pantry sau cart optimization.
- cloud auth / Firebase / Supabase.
- email verification, password reset, change email/change password functional.
- ingredient-level substitution.
- optimizer household global.
- productie QA completa.

## Directie urmatoare

Prioritatea curenta nu este rescrierea generatorului intr-un optimizer avansat. Directia realista este:
- polish si stabilizare mobile UI;
- testare pe telefon fizic si emulator;
- consolidarea flow-urilor app-facing: account -> add members -> generate -> grocery -> feedback -> alternatives -> replace -> insights;
- pastrarea backend-ului ca layer de orchestrare;
- reducerea treptata a datoriilor din documentatie si din date;
- pregatirea unei fundatii mai curate pentru Food_DB / Recipes_DB fara a rupe MVP-ul care functioneaza deja.
