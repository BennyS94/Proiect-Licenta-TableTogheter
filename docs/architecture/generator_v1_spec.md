# Generator v1 - specificatie consolidata

## 1. Scopul documentului

Acest document fixeaza deciziile de arhitectura si contractele operationale pentru Generator v1.
Scopul este de a produce o versiune functionala, testabila si extensibila. Starea curenta include demo/testing pentru o zi si un draft multi-day configurabil 1-5 zile, dar ramane limitata la un singur household si un singur member_profile activ.

---

## 2. Directia generala

Generatorul v1 este:
- recipe-based, deterministic, scoring-based si modular
- proiectat pentru testare rapida (Streamlit) si iteratie pe pilot
- limitat la 1 household + 1 member_profile activ
- demo/testing-ready pentru 1 zi si demo-ready pentru multi-day v1 draft configurabil 1-5 zile

Unitatea principala de selectie este reteta (`recipe`).

Datele principale consumate:
- `recipes`, `recipe_ingredients`, `recipe_nutrition_cache`, `member_profile`, `nutrition_target`, `household_preference_context`
- pentru Feedback v1 local/demo: `data/runtime/generator_v1_feedback_events.jsonl`

Decizii arhitecturale importante:
- NU folosim OR-Tools/MILP in Generator v1 demo
- KNN nu este motorul principal in v1 (doar strat auxiliar viitor)
- Nu se rescrie structura retetelor; se permit doar multiplicatori de portie

---

## 3. Principiul central al generatorului

Generatorul construieste planul pe baza de `recipes` si metadate asociate. Fiecare reteta este considerata un candidat atomic care poate fi inclus intr-un slot (breakfast/lunch/dinner/snack) si ajustat ca portie.

---

## 4. Ce NU face generatorul v1

- household optimization complet multi-profile
- household multi-member simultan
- live grocery price fetching sau store/brand/cart optimization
- folosirea KNN ca motor principal
- OR-Tools / MILP / CP-SAT
- rescrierea automata a retetelor sau componentizare automata
- promovarea automata a dataseturilor draft in `current`

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
  "bf_profile": "normal"
}
```

---

## 6. Calculul `nutrition_target`

Metoda adoptata in v1:
- formula Mifflin-St Jeor pentru BMR
- aplicare activity multiplier
- ajustare pentru obiectiv (`goal`) folosind delta in kcal
- NOTA: nu se mediaza mai multe formule in v1 — doar Mifflin-St Jeor

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
- In Feedback v1 local/demo, contextul este agregat din evenimente JSONL locale si ramane household/demo, nu productie.

---

## 11. Feedback

Tipuri minime:
- `liked`, `disliked`, `too_long`, `explicit_avoid`

Status curent:
- Feedback v1 este implementat ca feature local/demo pentru Generator v1.
- Storage: `data/runtime/generator_v1_feedback_events.jsonl`
- Feedback-ul se aplica la generatiile urmatoare.

Reguli implementate:
- `liked` -> bonus soft prin `feedback_fit`
- `disliked` -> penalizare soft prin `feedback_fit` (nu hard ban implicit)
- `too_long` -> penalizare de timp prin `time_feedback_penalty` si `time_fit`
- `explicit_avoid` -> hard filter pe `recipe_id`

Limitari:
- local JSONL only
- fara conturi reale
- fara DB/backend/server
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
- `banned_recipe_ids`, `banned_ingredient_names`
- `recipe.is_active` si `recipe.scope_status` acceptate
- `recipe_nutrition_cache.cache_status` acceptat (configurabil)
- `mapped_weight_ratio` prag minim configurabil
- `hard_time_gate` configurabil (ex. exclude > X minute)
- incompatibilitate slot (ex. cereal+milk la breakfast daca interzis)

---

## 13. Pipeline generator v1

1. incarca `member_profile` -> calculeaza `nutrition_target`
2. construieste pool initial de `recipes` aplicand `hard_filters`
3. pentru fiecare slot: filtru candidat + calcul scor (candidate_filter + recipe_scorer)
4. selectie cea mai buna per slot (deterministic tie-breaker)
5. assemble day -> aplica post-pass daily rules (ex. adjust veg servings)
6. output CSV/JSON plan + readable

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
- versiune `readable` (text) pentru inspectie

---

## 23.1. Dataset demo v1.2

Datasetul demo curent este:
- `dataset_profile=v1_2_demo_final`
- path: `data/recipesdb/draft/v1_2_demo_final/`
- total recipes: `266`
- active recipes: `261`
- sursa: `v1_2_demo_candidate_round48_cleaned`
- status: demo-final draft, nu productie/current

Datele `data/recipesdb/current` si `data/fooddb/current` raman neatinse de acest pachet demo.

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
- `feedback_store` (append/load/clear evenimente JSONL locale)
- `feedback_adapter` (agrega evenimente in household_preference_context)
- `feedback_fit` (calculeaza scorul local de feedback)
- `candidate_filter` (aplica hard filters)
- `recipe_scorer` (scoring v1)
- `day_selector` / `plan_builder` (asambleaza ziua)
- `plan_audit` (validari + readable)

---

## 25. Dependente minime pe date

- `recipes` cu `directions_json`/`directions_step_count`
- `recipe_ingredients` parsed
- optional: `recipe_nutrition_cache` pentru macro estimates (soft)

---

## 26. Rolul KNN

- KNN ramane strat auxiliar: substitutii, candidate expansion, propagare feedback
- NU este folosit ca motor principal de selectie in v1

---

## 27. Rolul Streamlit

- instrument de debug/test pentru iteratii rapide
- prezenta optionala in repo ca prototip de UI pentru testare

---

## 28. Checkpoint-uri

- Checkpoint 0: contract generator, profil, nutrition_target, feedback, hard filters, scoring - conceptual complet
- Checkpoint 1: 1 household + 1 member_profile activ + 1 zi - demo/testing-ready
- Checkpoint 2: multi-day v1 draft + demo polish - implementat/demo-ready
- Checkpoint 3: Feedback v1 local/demo implementat; raman feedback explainability, UI polish si family-level variety polish
- Checkpoint 4: source verification suplimentar si pregatire grocery/list
- Checkpoint 5: household generation multi-profile

---

## 29. Decizie despre optimizare avansata

- OR-Tools / MILP / CP-SAT: nu in v1; se poate reevalua la Checkpoint 4/5

---

## 30. Decizie despre surse externe de date

- cooktime estimate: poate folosi API/AI extern cu caching local (optional)
- preturi / grocery external: nu exista live fetching in runtime; DATA-QA-1 foloseste catalog static, aliasuri si fallback-uri demo controlate

---

## 31. Concluzie

Acest document stabileste contractul operational pentru Generator v1: recipe-based, deterministic, scoring-driven, limitat la 1 household + 1 profile activ. Checkpoint 1 si Checkpoint 2 sunt demo-ready pe pachetul `v1_2_demo_final`, iar Feedback v1 exista ca feature local/demo. Urmatorii pasi tin de feedback explainability, polish de varietate, source verification, grocery/list si household multi-member ulterior.

