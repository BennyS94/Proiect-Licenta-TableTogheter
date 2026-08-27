# Current architecture

## 1. Scope and current status

TableTogether is a bachelor thesis project for multi-day household meal planning. The system uses internal member profiles, nutrition goals, restrictions, preferences, explicit feedback and an aggregated grocery list.

Starea curenta nu mai este doar un pipeline de generator izolat. Exista un MVP local functional cu:

- Android-first mobile app in React Native + Expo;
- FastAPI backend;
- local SQLite for accounts, sessions, households, profiles, feedback, generated plans, grocery lists and saved daily progress snapshots;
- Python Generator v1 exposed through a service wrapper;
- Food_DB and Recipes_DB as curated CSV datasets;
- real mobile -> backend -> generator -> persistence flow.

Expo is used for fast development and testing. The product direction remains an incrementally shippable mobile app, not a separate demo disconnected from the real code.

The architecture is still evolving, but the current direction is clear: Android mobile app + FastAPI backend + deterministic Python generator + curated Food_DB / Recipes_DB data.

## 2. Current system shape

Flux operational app-facing:

```text
Android Mobile App
  -> FastAPI Backend
  -> Generator Service / Python Generator
  -> Food_DB + Recipes_DB
  -> SQLite persistence
  -> Mobile display
```

Roluri:

- Mobile:
  - create account / login / logout local;
  - gestioneaza profile interne de membri;
  - declanseaza generare plan;
  - afiseaza plan, grocery list, feedback, alternatives, replacement si insights.
- Backend:
  - valideaza requesturi;
  - incarca profile/feedback din SQLite;
  - apeleaza `src/generator_v1/service.py`;
  - persista planuri si grocery lists;
  - expune endpoints pentru alternatives si replacement.
- Generator:
  - calculeaza `nutrition_target`;
  - aplica `profile_guard`;
  - incarca Recipes_DB / Food_DB;
  - aplica hard filters;
  - construieste candidati pe sloturi;
  - calculeaza scoring deterministic;
  - selecteaza plan individual sau household v1 Lite;
  - construieste grocery list optionala.
- SQLite:
  - stocheaza conturi locale, sesiuni, household-uri, profile, feedback events, planuri generate, grocery lists si saved daily progress snapshots.

Mobile-ul nu citeste CSV-uri si nu ruleaza generatorul.

## 3. Generator v1 status

Generator v1 este recipe-based, deterministic, scoring-driven si modular.

Dataset app-facing/demo:

- `dataset_profile=current`
- path: `data/recipesdb/current/`
- total recipes: `326`
- active recipes: `326`
- status: curated app-facing dataset, not a complete production-scale recipe database

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

Capabilitati curente:

- generare individuala 1-5 zile;
- Household Generation v1 Lite;
- feedback fit;
- grocery list determinista;
- purchase suggestions;
- cooked-to-raw helpers;
- price estimates demo/source-backed;
- KNN-lite alternatives ca strat auxiliar;
- meal-level replacement explicit.

## 4. Household model

Modelul curent este household/family-first:

- contul local reprezinta household-ul;
- membrii sunt profile interne, nu utilizatori separati;
- `default viewer` controleaza profilul afisat primul in mobile;
- un singur profil activ produce plan individual;
- doua sau mai multe profile active produc plan household prin `POST /household-plans/generate`.

Household Generation v1 Lite:

- calculeaza targeturi per membru;
- construieste target agregat household;
- foloseste mese principale shared in modul recomandat `individual_breakfast_shared_main`;
- pastreaza breakfast/snack individuale/flexibile;
- ajusteaza portii per membru;
- calculeaza macro summary per membru;
- pastreaza grocery list agregata.

Limitare importanta: nu exista optimizer household global. `macro_aware_simple` este euristic, nu MILP/OR-Tools.

## 5. Grocery and pricing

Grocery List v1 este implementata determinist:

- extrage ingredientele retetelor selectate;
- aplica multiplicatori de portie;
- agrega ingredientele;
- curata numele;
- categorizeaza itemii;
- poate adauga purchase suggestions;
- poate adauga cooked-to-raw helpers pentru shopping display;
- poate include cost estimat.

Preturile sunt estimari demo/source-backed/fallback-based. Nu exista:

- live price fetching;
- runtime scraping;
- optimizare pe magazine;
- optimizare pe branduri;
- pantry inventory real;
- cart optimization.

DATA-QA-1 este completat pentru fluxurile app-facing normale: nu ar trebui sa existe preturi lipsa sau cooking-time estimates lipsa in outputurile generate.

## 6. Feedback

Feedback v1 este implementat in generator si in backend/mobile.

Tipuri:

- `liked`
- `disliked`
- `too_long`
- `explicit_avoid`

Comportament:

- `explicit_avoid` exclude reteta prin hard filter pe `recipe_id`;
- `liked` si `disliked` modifica `feedback_fit`;
- `too_long` penalizeaza `time_fit`;
- feedback-ul se aplica la generari viitoare, nu modifica retroactiv planul afisat.

Storage:

- CLI/Streamlit: JSONL local;
- backend/mobile: SQLite local/demo.

Limitari:

- nu exista cloud sync;
- nu exista ML;
- nu exista propagare completa la ingrediente/familii;
- `Like`/`Dislike` din profile sunt persistate, dar soft scoring ingredient/family ramane deferat.
- endpointurile feedback/replacement sunt inca demo/local si nu au acelasi nivel de account ownership enforcement ca profilele auth-scoped.

## 7. KNN-lite and replacement

KNN-lite este strat auxiliar, nu motor principal.

Implementat:

- `POST /recipes/similar`;
- approved/review alternatives;
- generator approval gate;
- `POST /plans/{plan_id}/replace-meal`;
- preview cu `dry_run=true`;
- apply cu `dry_run=false`;
- plan derivat nou si grocery recalculata.
- mobile pregateste preview-urile din panoul `Alternatives` secvential si local-cache-uit, fara request-uri paralele si fara schimbare de contract backend.

Limitari:

- nu exista inlocuire automata;
- nu exista ingredient-level substitution;
- alternativele `review` sunt preview-only.
- macro-urile ajustate pentru alternative sunt afisate in mobile doar dupa un preview real `dry_run=true`, nu din estimari per-serving.

## 8. Insights and progress state

Page 3 / Insights este o vizualizare nutritionala app-facing peste planul generat curent. Include:

- selector profil / viewer;
- Day 1-Day 5 si Average mode;
- Daily Balance cu macro donut dinamic;
- meal contribution cards cu toggle eaten/not eaten local;
- Macro Targets care se actualizeaza dupa mesele marcate eaten;
- control `Save day` / `Saved` / `Delete saved day` pentru snapshotul zilei curente;
- sectiune `Trends` peste snapshoturile salvate, cu range 1..30 zile disponibile, Target adherence, Consistency si Macro pattern;
- placeholder scurt pentru Micronutrients.

Starea eaten/not eaten ramane React state local pentru interactiunea curenta, scopata pe profil, zi si semnatura meselor. PROGRESS-1 salveaza un snapshot explicit al zilei cand utilizatorul apasa `Save day`.

Persistenta PROGRESS-1 include:

- tabela SQLite `saved_daily_progress`;
- `POST /progress/daily`, `GET /progress/daily`, `DELETE /progress/daily/{progress_id}` si aliasul mobil `POST /progress/daily/{progress_id}/delete`;
- unicitate pe `member_profile_id + plan_id + day_index`;
- maximum 30 snapshoturi salvate per profil, enforced backend-side;
- campuri planned/target/consumed pentru comparatii istorice; randurile vechi fara target explicit folosesc planned ca fallback documentat;
- scoping pe `Authorization: Bearer <session_token>` si household-ul contului.

PROGRESS-2 adauga grafice compacte peste aceste snapshoturi salvate. Charts sunt despre aderenta la plan si progres fata de targeturi nutritionale, nu despre diagnostic sau tratament medical.

## 9. Current data model reality

Food_DB:

- baseline activ: `data/fooddb/current/fooddb_current.csv`;
- contine alimente canonice si valori nutritionale/taxonomice;
- nu este baza production/cloud.

Recipes_DB current:

- `data/recipesdb/current/recipes.csv` - 326 app-facing recipes;
- `data/recipesdb/current/recipe_ingredients.csv` - 2498 mapped ingredient rows;
- `data/recipesdb/current/recipe_nutrition_cache.csv` - 326 nutrition cache rows.

Recipes_DB app-facing:

- `data/recipesdb/current/`;
- used by Generator v1 in the current app flow.

Directia ramane separarea curata:

- Food_DB = alimente canonice;
- Recipes_DB = retete compuse;
- Recipe Ingredients = legatura reteta-ingredient;
- Recipe Nutrition Cache = macro/nutrition cache pentru retete.

## 10. Current strengths

- Exista flux functional mobile -> backend -> generator -> SQLite -> mobile.
- Generatorul produce planuri individuale si household v1 Lite.
- Grocery list este integrata in outputul app-facing.
- Feedback-ul explicit este functional si se aplica la generari viitoare.
- Alternatives si replacement explicit exista end-to-end, cu preview mobil pregatit secvential si cache-uit pe contextul plan/zi/masa/profil.
- Insights este o pagina nutrition dashboard acceptata vizual, cu eaten-meals state local, macro-uri dinamice, saved daily progress snapshots persistate la cerere si Trends peste istoricul salvat.
- Datele demo au coverage verificat pentru price/time in scenariile normale.
- Arhitectura este suficient de modulara pentru evolutie incrementala.

## 11. Current limitations

- Nu exista productie QA completa.
- SQLite este local/demo, nu production DB.
- Auth-M1 este local si nu include email verification, password reset sau cloud sync.
- Sesiunea mobila nu este persistata peste restart.
- Trends este MVP compact si nu include inca Top foods / Top contributors sau analiza avansata de contributori.
- Household Generation v1 Lite este euristic, nu optimizer global.
- KNN nu este motor principal si nu face ingredient substitution.
- Price estimates nu sunt live prices.
- Food_DB / Recipes_DB current sunt baseline-uri pilot si au nevoie de curatare treptata.
- Unele documente vechi pot descrie stadii anterioare si nu trebuie tratate ca sursa curenta de adevar.

## 12. Immediate next priority

Prioritatea imediata este consolidarea aplicatiei reale, nu adaugarea unui optimizer avansat.

Pasi rezonabili:

- polish UI si assets pentru mobile;
- testare reala pe telefon/emulator;
- stabilizarea flow-ului account -> add members -> generate -> grocery -> feedback -> alternatives -> replace -> insights;
- rafinarea vizuala si culinara a Trends dupa testare pe mai multe profiluri reale;
- clarificarea documentatiei de licenta pe baza implementarii reale;
- imbunatatirea treptata a Food_DB / Recipes_DB fara a rupe MVP-ul functional;
- amanarea ML/optimizerilor pana cand datele si flow-urile de baza sunt stabile.

## 13. Transitional note

Acest document descrie starea curenta a proiectului. Arhitectura tinta ramane documentata separat in `docs/architecture/restructure_target.md`, dar orice decizie noua trebuie verificata si fata de codul real, nu doar fata de documentele istorice.
