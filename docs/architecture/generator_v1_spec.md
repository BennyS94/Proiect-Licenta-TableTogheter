# Generator v1 - specificatie consolidata

## 1. Scopul documentului

Acest document fixeaza deciziile de arhitectura si contractele operationale pentru Generator v1.
Scopul este de a produce o versiune functionala, testabila si extensibila. Starea curenta include generare individuala 1-5 zile, Household Generation v1 Lite, feedback explicit, grocery list determinista si integrare prin FastAPI backend / mobile MVP.

---

## 2. Directia generala

Generatorul v1 este:
- recipe-based, deterministic, scoring-based si modular
- proiectat pentru testare rapida (Streamlit), integrare backend si iteratie pe pilot
- demo/testing-ready pentru generare individuala 1-5 zile
- integrat cu Household Generation v1 Lite pentru fluxuri household MVP/demo
- expus catre mobile prin FastAPI backend, nu rulat in aplicatia mobila

Unitatea principala de selectie este reteta (`recipe`).

Datele principale consumate:
- `recipes`, `recipe_ingredients`, `recipe_nutrition_cache`, `member_profile`, `nutrition_target`, `household_preference_context`
- pentru Feedback v1 local CLI/Streamlit: `data/runtime/generator_v1_feedback_events.jsonl`
- pentru Feedback v1 backend/mobile: SQLite local/demo prin tabela `feedback_events`

Decizii arhitecturale importante:
- NU folosim OR-Tools/MILP in Generator v1 demo
- KNN nu este motorul principal in v1; este strat auxiliar pentru alternatives si meal-level replacement explicit
- Nu se rescrie structura retetelor; se permit doar multiplicatori de portie

---

## 3. Principiul central al generatorului

Generatorul construieste planul pe baza de `recipes` si metadate asociate. Fiecare reteta este considerata un candidat atomic care poate fi inclus intr-un slot (breakfast/lunch/dinner/snack) si ajustat ca portie.

---

## 4. Ce NU face generatorul v1

- household optimization complet multi-profile
- optimizer household global multi-objective
- meniuri complet separate per membru by default
- live grocery price fetching sau store/brand/cart optimization
- folosirea KNN ca motor principal
- OR-Tools / MILP / CP-SAT
- rescrierea automata a retetelor sau componentizare automata
- promovarea automata a dataseturilor draft in `current`
- ingredient-level substitution

Generatorul poate ajusta portiile retetelor prin multiplicatori (ex. 0.8x/1.0x/1.2x), dar nu modifica lista de ingrediente.

---

## 5. Modelul profilului utilizatorului

Profilul este folosit pentru calculul `nutrition_target`, configurarea meselor si aplicarea restrictiilor hard.

Campuri obligatorii in `member_profile` (v1):
- `age`, `sex`, `weight_kg`, `height_cm`, `activity_level`, `goal`, `goal_speed`
- `training.sessions_per_week`, `training.type`
- `meal_config.meals_per_day`, `meal_config.include_snacks`
- `dietary_preferences` (chei booleene, inclusiv `no_pork`)
- `food_preferences` (PROFILE-WIZARD-1: ratings + avoid ingredients + cooking time preference)
- `health_and_diet_preferences` (DIET-HEALTH-PROFILES: dietary patterns si health-aware modes)
- `bf_profile` (pastrat, dar nefolosit in formula energetica v1)

Exemplu minim:

```json
{
  "age": 22,
  "sex": "male",
  "weight_kg": 67.0,
  "height_cm": 176.0,
  "activity_level": "moderately_active",
  "goal": "maintain",
  "goal_speed": "normal",
  "training": {"sessions_per_week": 3, "type": "weights"},
  "meal_config": {"meals_per_day": 3, "include_snacks": true},
  "dietary_preferences": {
    "no_beef": false,
    "no_pork": false,
    "no_chicken": false,
    "no_fish": false,
    "no_dairy": false,
    "vegetarian": false,
    "vegan": false,
    "gluten_free": false
  },
  "food_preferences": {
    "ratings": {},
    "avoid_ingredients": [],
    "cooking_time_preference": "balanced"
  },
  "health_and_diet_preferences": {
    "dietary_patterns": {
      "keto": false,
      "paleo": false,
      "mediterranean": true
    },
    "health_modes": {
      "diabetes_aware": false,
      "hypertension_friendly": false,
      "heart_friendly": false
    }
  },
  "bf_profile": "normal"
}
```

---

## 6. Calculul `nutrition_target`

Metoda adoptata in v1:
- formula Mifflin-St Jeor pentru BMR
- aplicare activity multiplier
- ajustare pentru obiectiv (`goal`) folosind delta in kcal
- NOTA: nu se mediaza mai multe formule in v1 â€” doar Mifflin-St Jeor

Activity multipliers (v1):
- `sedentary` = 1.20
- `lightly_active` = 1.35
- `moderately_active` = 1.50
- `very_active` = 1.70

Goal adjustment (delta kcal):
- `maintain`: 0
- `lose`: `slow`=-250, `normal`=-400, `fast`=-550
- `gain`: `slow`=+200, `normal`=+300, `fast`=+450

---

## 7. Obiectul `nutrition_target`

Structura minima:
- `daily_kcal_target`
- `protein_g_target`
- `fat_g_target`
- `carb_g_target` (calculat ca rest energetic)
- `meals_per_day`, `include_snacks`

Macro strategy (v1):
- Proteina: se calculeaza prima, pe baza g/kg in functie de goal si training
- Grasimi: prag rezonabil (~0.8 g/kg baza)
- Carbohidratii: restul energetic

Protein rules (valori recomandate v1):
- maintain fara weights = 1.6 g/kg
- maintain + weights = 1.8 g/kg
- lose = 2.0 g/kg
- gain = 1.8 g/kg

Fat baseline: 0.8 g/kg

---

## 8. Structura zilei

Moduri suportate (v1):
- `3_meals` (breakfast, lunch, dinner)
- `3_meals_plus_snack` (breakfast, lunch, dinner, snack)

Sloturi: `breakfast`, `lunch`, `dinner`, `snack` (optional)

Meal split (v1):
- `3_meals`: breakfast=25%, lunch=40%, dinner=35%
- `3_meals_plus_snack`: breakfast=22%, lunch=33%, dinner=30%, snack=15%

Portion multipliers disponibile: 0.8x, 1.0x, 1.2x

---

## 9. Portionarea retetelor

- Portia recomandata este calculata pe baza `recipe_nutrition_cache` si `nutrition_target`.
- Pentru ajustare se folosesc multiplicatori discreti (0.8/1.0/1.2).

---

## 10. Household preference context

- In v1, preferintele sunt comune la nivel de `household`.
- Profilul activ (`member_profile`) este folosit pentru `nutrition_target`.
- `household_preference_context` contine liste simple: liked/disliked/avoid si time sensitivity.
- In CLI/Streamlit, contextul poate fi agregat din evenimente JSONL locale.
- In backend/mobile MVP, contextul este agregat din SQLite si injectat in requestul de generare cand `feedback_enabled=true`.
- Contextul ramane explicit si interpretabil; nu este ML si nu este personalizare cloud/productie.

---

## 11. Feedback

Tipuri minime:
- `liked`, `disliked`, `too_long`, `explicit_avoid`

Status curent:
- Feedback v1 este implementat ca feature local/demo pentru Generator v1 si este integrat in backend/mobile MVP.
- Storage CLI/Streamlit: `data/runtime/generator_v1_feedback_events.jsonl`
- Storage backend/mobile: SQLite local/demo, tabela `feedback_events`
- Feedback-ul se aplica la generatiile urmatoare.

Reguli implementate:
- `liked` -> bonus soft prin `feedback_fit`
- `disliked` -> penalizare soft prin `feedback_fit` (nu hard ban implicit)
- `too_long` -> penalizare de timp prin `time_feedback_penalty` si `time_fit`
- `explicit_avoid` -> hard filter pe `recipe_id`

Limitari:
- fara cloud sync
- SQLite local/demo, nu productie
- fara ML/KNN
- fara personalizare de productie
- fara propagare ingredient/family

---

## 12. Hard filters v1

- restrictii alimentare din `member_profile.dietary_preferences`
- `dietary_preferences.no_pork` exclude ingrediente pork-specific (`pork`, `bacon`, `ham`, `prosciutto`, `pancetta`, `salami`, `chorizo`, `pepperoni`, `pork sausage` etc.)
- `food_preferences.ratings[*] = "avoid"` se mapeaza la hard filter pentru cheile suportate; `like` si `dislike` sunt persistate, dar nu modifica inca scoringul de ingrediente/familii.
- `food_preferences.avoid_ingredients` intra in filtrul hard de ingrediente.
- `Neutral` inseamna lipsa cheii in `food_preferences.ratings`.
- `health_and_diet_preferences.dietary_patterns.keto` si `.paleo` folosesc filtre conservative pentru ingrediente clar incompatibile si penalizari de scoring cand exista date macro.
- `health_and_diet_preferences.dietary_patterns.mediterranean` este aplicat ca preferinta/scoring mode, fara hard ban general.
- `health_and_diet_preferences.health_modes.diabetes_aware` penalizeaza soft mesele foarte high-carb, high-sugar sau high-carb/low-protein unde exista date suficiente.
- `health_and_diet_preferences.health_modes.hypertension_friendly` penalizeaza soft sare mare unde exista cache si proxy-uri de ingrediente sarate/procesate precum bacon, ham, salami, pepperoni, sausage, soy sauce, fish sauce, bouillon sau stock cube.
- Pentru blood-pressure friendly, sodium/salt coverage poate fi incomplet, deci scoringul ramane proxy-based si interpretabil.
- `health_and_diet_preferences.health_modes.heart_friendly` penalizeaza soft retete foarte fat-heavy, prajite, cremoase sau cu carne procesata; nu interzice toate grasimile si trebuie sa ramana compatibil cu selectii conflictuale precum keto + heart-friendly.
- DIET-HEALTH-PROFILES nu schimba formulele nutritionale si nu aplica claims medicale.
- `banned_recipe_ids`, `banned_ingredient_names`
- `recipe.is_active` si `recipe.scope_status` acceptate
- `recipe_nutrition_cache.cache_status` acceptat (configurabil)
- `mapped_weight_ratio` prag minim configurabil
- `hard_time_gate` configurabil (ex. exclude > X minute)
- incompatibilitate slot (ex. cereal+milk la breakfast daca interzis)

---

## 13. Pipeline generator v1

1. incarca profilul / household context
2. calculeaza `nutrition_target` individual sau targeturi per membru + agregat household
3. evalueaza devreme `profile_guard`
4. incarca Recipes_DB / Food_DB / nutrition cache
5. aplica `hard_filters`
6. construieste candidati pe sloturi
7. calculeaza scoruri si semnale auxiliare (`macro_fit`, `time_fit`, `slot_fit`, `feedback_fit`, `health_and_diet_fit`, `variety_fit`, `nutrition_quality`, realism)
8. selecteaza plan individual sau household v1 Lite
9. aplica validare / quality gate
10. optional construieste grocery list si output JSON-safe pentru backend/mobile

---

## 14. Candidate filtering pe sloturi

- filtre role-based: breakfast/lunch/dinner/snack
- filtre pe tag-uri si buckets din `recipes` si `recipe_ingredients`
- exclude ingrediente/retete interzise

---

## 15. Scoring v1

Formula agregata:

score_total =
0.55 * macro_fit
+ 0.15 * time_fit
+ 0.15 * slot_fit
+ 0.10 * feedback_fit
+ 0.05 * variety_fit

Detaliere macro_fit:
macro_fit = 0.40*protein_fit + 0.35*kcal_fit + 0.15*carbs_fit + 0.10*fat_fit

---

## 16. Macro fit

- `protein_fit`: potrivirea cantitatii proteice masei la tinta
- `kcal_fit`: apropierea de kcal_target pentru slot
- `carbs_fit` si `fat_fit`: penalizari/bonusuri pe praguri

---

## 17. Time fit

- exista din prima rulare, chiar fara feedback
- combina `base_time_fit` (pe baza campurilor prep/cook/total time din Recipes_DB) si `feedback_time_adjustment`

Base time fit mese principale (v1):
- 0-15 min = 1.00
- 16-30 min = 0.80
- 31-45 min = 0.55
- 46-60 min = 0.25
- >60 min = 0.05

Base time fit snack:
- 0-10 min = 1.00
- 11-20 min = 0.60
- >20 min = 0.10

---

## 17.1. DATA-QA-1 output coverage

DATA-QA-1 este completat pentru acoperirea preturilor si a timpilor de gatire in outputurile app-facing.

Contract operational:
- grocery output nu trebuie sa expuna preturi lipsa in fluxurile normale generate;
- mesele individuale, household si replacement trebuie sa expuna estimari utilizabile de cooking time;
- pricing-ul este static/demo/reference-based, nu live fetching;
- preturile folosesc catalog exact, alias catalog si fallback-uri controlate;
- checker-ul `python tools/extra/check_data_qa_price_time_no_missing.py` trebuie rulat dupa schimbari in retete, grocery catalog, aliasuri sau fallback-uri.

household_time_sensitivity: `low`, `normal`, `high` (default `normal`)

---

## 18. Slot fit

- potrivire rol-slot (ex. protein dense pentru protein slot)
- avoid combinations prin reguli hard (ex. cereal+milk la breakfast daca e interzis)

---

## 19. Feedback fit

- valoare neutra = 0.50
- liked exact = +0.12 per eveniment, contributie maxima +0.25
- disliked exact = -0.15 per eveniment, contributie maxima -0.30
- daca aceeasi reteta are liked si disliked, scorul foloseste efectul net
- `explicit_avoid` ar trebui eliminat in filtrare; daca ajunge la scoring, primeste scor foarte scazut
- clamp intre 0.0 si 1.0

Extensii viitoare: acumulare preferinte familie/ingredient

---

## 20. Variety fit

- valoare neutra = 0.50
- intra-day doar in v1
- reteta identica in aceeasi zi se blocheaza local
- aceeasi family = -0.20
- ingredient dominant repetat = -0.10
- categorie apropiata = -0.08
- familie noua = +0.10
- ingredient dominant nou = +0.05
- clamp intre 0.0 si 1.0

---

## 21. Day assembly

- se selecteaza cel mai bun candidat per slot in ordinea: breakfast -> lunch -> dinner -> snack
- se respecta used_recipe_ids/used_ingredient_uids pentru variety

---

## 22. Post-pass validation

- aplicare daily rules (ex. min legume/zi, carb BL fraction)
- ajustare portii (multiplicatori) daca este nevoie si posibil

---

## 23. Output generator v1

- format CSV/JSON cu coloane/chei: meal, recipe_id, portion_multiplier, kcal_meal, protein_meal_g, carb_meal_g, fat_meal_g, score, reasons
- pentru backend/mobile, randurile de masa expun si `directions_step_count` + `cooking_steps`, derivate din `directions_json`
- versiune `readable` (text) pentru inspectie

---

## 23.1. Dataset demo v1.2

Datasetul demo curent este:
- `dataset_profile=current`
- path: `data/recipesdb/current/`
- total recipes: `266`
- active recipes: `261`
- sursa: `v1_2_demo_candidate_round48_cleaned`
- status: demo-final draft, nu productie/current

Datele `data/recipesdb/current` si `data/fooddb/current` raman neatinse de acest pachet demo.

Cooking steps:
- COOKING-STEPS-1 valideaza ca `data/recipesdb/current/recipes.csv` are `directions_json` valid si `directions_step_count > 0` pentru toate cele `106/106` retete active/app-facing.
- Pasii sunt instructiuni MVP practice, suficiente pentru demo/prezentare mobila, nu text extern copiat/scraped.
- Generatorul expune `cooking_steps` in outputurile individuale si household, astfel incat fallback-ul mobil pentru pasi lipsa nu este asteptat pentru retetele active generate.
- Rafinarea culinara ramane posibila ulterior fara schimbarea `recipe_id`, nutrition cache, grocery logic sau flow-ul de replacement.

Config recomandat pentru demo:
- `selection_mode=balanced_day`
- `portion_policy=target_aware`
- `meal_realism_mode=practical`
- `quality_gate=demo_safe`
- `days=3`
- `multi_day_mode=global_alternatives_3_day`
- `multi_day_no_repeat_policy=hard`
- `day_candidate_builder=direct_from_slots`
- `profile_guard=demo`

Smoke curent:
- one-day valid/accept = true
- three-day valid = 3/3
- accept = 3/3
- repeated recipes = 0
- `multi_day_loss=0.006322`

---

## 23.2. Profile guard

`profile_guard` este un strat de avertizare/blocare pentru demo. Nu modifica targeturile si nu modifica profilul.

Moduri:
- `off`: comportament vechi, fara guard
- `demo`: blocheaza profilurile unsupported pentru demo
- `permissive`: avertizeaza, dar permite generarea

Reguli principale:
- `target_kcal < 1300` => `unsupported_for_demo`; in modul `demo` blocheaza generarea daca nu exista override explicit
- `target_kcal < 1400` cu snack activ => cel putin `edge_needs_warning`
- `goal=lose`, `goal_speed=fast`, `activity_level=sedentary` => cel putin `edge_needs_warning`

Exemplu validat:
- profilul `sedentary_lose_fast_with_snack`, `target_kcal=1227.8`, este blocat in `profile_guard=demo`
- acelasi profil avertizeaza si continua in `profile_guard=permissive`

---

## 24. Module logice recomandate

- `target_builder` (calc nutrition_target)
- `feedback_store` (append/load/clear evenimente JSONL locale pentru CLI/Streamlit)
- `feedback_adapter` (agrega evenimente in household_preference_context)
- `feedback_fit` (calculeaza scorul local de feedback)
- `candidate_filter` (aplica hard filters)
- `recipe_scorer` (scoring v1)
- `day_selector` / `plan_builder` (asambleaza ziua)
- `plan_audit` (validari + readable)

---

## 25. Dependente minime pe date

- `recipes` cu `directions_json`/`directions_step_count`; COOKING-STEPS-1 cere pasi valizi pentru toate retetele active/app-facing
- `recipe_ingredients` parsed
- optional: `recipe_nutrition_cache` pentru macro estimates (soft)

---

## 26. Rolul KNN

- KNN ramane strat auxiliar: recipe alternatives, candidate support si meal-level replacement explicit
- NU este folosit ca motor principal de selectie in v1
- NU face ingredient-level substitution in MVP

---

## 27. Rolul Streamlit

- instrument de debug/test pentru iteratii rapide
- prezenta optionala in repo ca prototip de UI pentru testare

---

## 28. Checkpoint-uri

- Checkpoint 0: contract generator, profil, nutrition_target, feedback, hard filters, scoring - conceptual complet
- Checkpoint 1: 1 household + 1 member_profile activ + 1 zi - demo/testing-ready
- Checkpoint 2: multi-day v1 draft + demo polish - implementat/demo-ready
- Checkpoint 3: Feedback v1 implementat local si backend/mobile SQLite
- Checkpoint 4: Grocery/list, purchase suggestions, cooked-to-raw helpers si price estimates demo implementate
- Checkpoint 5: Household Generation v1 Lite implementat; optimizer household global ramane viitor
- Checkpoint KNN-2/KNN-4: alternatives si meal-level replacement explicit implementate ca strat auxiliar

---

## 29. Decizie despre optimizare avansata

- OR-Tools / MILP / CP-SAT: nu in v1; se poate reevalua la Checkpoint 4/5

---

## 30. Decizie despre surse externe de date

- cooktime estimate: poate folosi API/AI extern cu caching local (optional)
- preturi / grocery external: nu exista live fetching in runtime; DATA-QA-1 foloseste catalog static, aliasuri si fallback-uri demo controlate

---

## 31. Concluzie

Acest document stabileste contractul operational pentru Generator v1: recipe-based, deterministic, scoring-driven si integrat prin backend/mobile MVP. Generarea individuala 1-5 zile, Household Generation v1 Lite, feedback explicit, grocery list, price/time coverage demo, KNN alternatives si meal-level replacement exista deja pe pachetul `current`. Urmatorii pasi tin de polish mobile, QA pe telefon, feedback explainability, varietate family-level si consolidarea Food_DB / Recipes_DB, nu de introducerea prematura a unui optimizer global sau a unui motor ML principal.
