# Generator v1 checkpoint status

## Snapshot

Starea curenta: Generator v1 este operational pentru fluxuri MVP/demo individuale si household, expuse prin FastAPI backend si consumate de aplicatia mobila Expo/React Native.

Ce exista acum:
- generare individuala 1-5 zile;
- Household Generation v1 Lite;
- grocery list determinista;
- purchase suggestions;
- cooked-to-raw helpers pentru afisare shopping;
- price estimates demo/source-backed prin catalog, aliasuri si fallback-uri controlate;
- feedback explicit in generator si in backend SQLite;
- KNN-lite alternatives ca provider auxiliar;
- meal-level replacement explicit cu preview/apply;
- wrapper service Python pentru backend, fara CLI parsing;
- persistenta SQLite pentru planuri, profile, feedback, grocery si auth local.

Dataset app-facing/demo:
- `dataset_profile=current`
- path: `data/recipesdb/current/`
- total recipes: `266`
- active recipes: `261`
- sursa: `v1_2_demo_candidate_round48_cleaned`
- status: demo-final draft, nu productie/current

Baseline-uri de lucru separate:
- `data/recipesdb/current`
- `data/fooddb/current`

KNN-lite este implementat, dar nu este activ ca motor principal de generatie. El propune alternative; generatorul/backend-ul valideaza candidatii.

## Checkpoint 0

Status: conceptual complet.

Acoperit:
- contract generator;
- profil membru;
- `nutrition_target`;
- hard filters;
- scoring deterministic;
- directie recipe-based.

## Checkpoint 1

Status: demo/testing-ready.

Acoperit:
- generare pentru un profil;
- generare 1 zi;
- output CSV/JSON/readable pentru debug;
- Streamlit dashboard pentru test;
- CLI smoke pentru profilul demo.

## Checkpoint 2

Status: multi-day v1 demo-ready.

Acoperit:
- generare configurabila 1-5 zile;
- `global_alternatives_3_day`;
- hard no-repeat exact recipe, cu fallback daca este infezabil pentru 4/5 zile;
- `direct_from_slots`;
- `quality_gate=demo_safe`;
- `profile_guard=demo`.

Smoke istoric relevant:
- one-day valid/accept = true;
- three-day valid = 3/3;
- accept = 3/3;
- repeated recipes = 0;
- `multi_day_loss=0.006322`;
- `--days 1,2,3,4,5` functioneaza pe `current`;
- `--days 6` este respins clar.

## Feedback v1

Status: implementat.

Rol:
- semnal explicit, interpretabil, folosit la generari viitoare;
- nu schimba formulele de nutritie;
- nu promoveaza datasetul `current` in `current`;
- nu este ML/KNN.

Storage:
- CLI/Streamlit: `data/runtime/generator_v1_feedback_events.jsonl`;
- Backend/Mobile: SQLite local/demo, tabela `feedback_events`.

Tipuri:
- `liked`
- `disliked`
- `too_long`
- `explicit_avoid`

Comportament:
- `explicit_avoid` aplica hard filter pe `recipe_id`;
- `liked` influenteaza scorul prin bonus soft;
- `disliked` influenteaza scorul prin penalizare soft;
- `too_long` reduce `time_fit` prin penalizare de timp;
- feedback-ul se aplica la generatiile urmatoare.

Limitari:
- fara cloud sync;
- fara propagare completa pe ingrediente/familii;
- `Like`/`Dislike` din profil sunt persistate, dar scoring-ul ingredient/family este deferat.

## Grocery List v1 / Purchase Rules v1 / Price Estimates

Status: implementat ca feature determinist MVP/demo.

Rol:
- construieste grocery list din retetele selectate si `portion_multiplier`;
- agregheaza ingredientele la nivel de plan/household;
- pastreaza gramele exacte in outputul detaliat;
- afiseaza nume curate, categorii si cantitati shopping-friendly;
- adauga sugestii simple de cumparare;
- adauga helper-e cooked-to-raw pentru afisare;
- poate include cost estimat demo.

Comportament:
- oua -> bucati;
- ceapa / unele legume / fructe -> bucati aproximative;
- paste / orez / oats / beans -> pachete sau bags simple;
- lapte / yogurt -> carton sau tub;
- carne / peste -> grame rotunjite grosier;
- uleiuri / sare / condimente / unele sweeteners -> `check pantry`;
- cooked rice/pasta/beans/lentils/chickpeas pot primi conversii raw/dry estimate pentru purchase display;
- helper-ele cooked-to-raw nu modifica recipe nutrition calculations.

DATA-QA-1:
- outputurile app-facing nu trebuie sa expuna preturi lipsa in fluxurile normale generate;
- mesele individuale, household si replacement trebuie sa expuna estimari utilizabile de cooking time;
- pricing-ul este static/demo/reference-based, nu live fetching;
- checker: `python tools/extra/check_data_qa_price_time_no_missing.py`.

Limitari:
- nu exista live price fetching;
- nu exista store/brand/cart optimization;
- nu exista pantry inventory real;
- multe estimari sunt demo/category fallback, nu preturi exacte pentru fiecare produs.

## Household Generation v1 Lite

Status: implementat ca strat functional MVP/demo.

Rol:
- porneste de la un household cu mai multe profile interne;
- construieste targeturi per membru si target agregat household;
- selecteaza mese shared si individuale conform modului ales;
- ajusteaza portii per membru;
- calculeaza macro summaries per membru;
- produce grocery scaling/list agregata.

Moduri suportate:
- `shared_all_slots`;
- `shared_main_meals`;
- `individual_breakfast_shared_main`.

Mod recomandat app-facing:
- `individual_breakfast_shared_main`
  - breakfast/snack individuale/flexibile;
  - lunch/dinner shared;
  - portii diferite per membru.

Allocation:
- `macro_aware_simple` este modul implicit folosit de MVP;
- este euristic, nu optimizer matematic global.

Limitari:
- nu exista OR-Tools/MILP/CP-SAT;
- nu exista optimizer household global;
- nu exista meniuri complet separate per membru by default;
- proteina/carbs pot ramane in review pentru anumite profiluri;
- grocery scaling este cantitativ, nu shopping optimization.

## KNN-lite Alternatives / Meal Replacement

Status: implementat ca strat auxiliar.

Comportament:
- `POST /recipes/similar` propune alternative aprobate/review;
- KNN-lite calculeaza similaritate intre retete;
- generator approval gate verifica slot, profil, feedback, macro, timp, realism si nutritie;
- `POST /plans/{plan_id}/replace-meal?dry_run=true` face preview;
- `dry_run=false` creeaza un plan nou derivat si recalculeaza grocery list;
- alternativele `review` sunt preview-only.

Limitari:
- KNN nu este motor principal;
- nu exista inlocuire automata;
- nu exista ingredient-level substitution;
- planul original nu este suprascris.

## Backend / API / Mobile Integration

Status: implementat MVP/local.

Backend:
- FastAPI app shell;
- generation endpoints;
- profile endpoints;
- feedback endpoints;
- local auth endpoints;
- recipe alternatives endpoint;
- meal replacement endpoint;
- SQLite persistence.

Mobile:
- Expo/React Native Android-first;
- Home, Meal Plan, Insights, Household / Account;
- local create account / login / logout;
- Add Member wizard;
- one-button family-first meal generation;
- grocery display;
- feedback buttons;
- alternatives + preview/replace;
- insights de baza.

Limitari:
- mobile session nu este persistata peste restart;
- cloud auth/sync nu exista;
- UI polish final si testarea completa pe telefon raman in lucru;
- Expo este folosit pentru viteza de dezvoltare, nu ca dovada ca produsul ramane doar prototip.

## Explicit out of scope

- OR-Tools / MILP / CP-SAT.
- KNN/ML ca motor principal de selectie.
- live price fetching / runtime scraping.
- store, brand, cart sau pantry optimization.
- cloud production deployment.
- Firebase/Supabase fara justificare ulterioara.
- email verification / password reset / change email / change password functional.
- ingredient-level substitution.
- productie QA completa.
- `current` promovat automat in `current`.

## Limitari cunoscute

- Nu este productie QA.
- Family-level variety ramane imperfecta.
- Unele outlier risks raman cu warnings.
- Food_DB si Recipes_DB current sunt baseline-uri pilot, nu model final production.
- `recipe_components.csv` ramane placeholder.
- Feedback-ul nu propaga inca complet preferintele la nivel de ingrediente/familii.
- Household Generation v1 Lite este euristic, nu optimizer global.
- Pricing-ul este demo/source-backed/fallback-based, nu live.

## Urmatoarele checkpoint-uri posibile

A. Mobile UI polish + testare telefon fizic.

B. Persistenta sesiune mobila peste restart.

C. Feedback explainability si profile preference scoring pentru Like/Dislike.

D. Family-level variety polish.

E. Food_DB / Recipes_DB cleanup si promotion strategy mai clara.

F. Ingredient-level substitution design, doar dupa stabilizarea datelor.
