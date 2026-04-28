# Generator v1 - specificatie consolidata

## 1. Scopul documentului

Acest document fixeaza deciziile de arhitectura si contractele operationale pentru Generator v1.
Scopul este de a produce o versiune functionala, testabila si extensibila, limitata initial la un singur household, un singur member_profile activ si o singura zi.

---

## 2. Directia generala

Generatorul v1 este:
- recipe-based, deterministic, scoring-based si modular
- proiectat pentru testare rapida (Streamlit) si iteratie pe pilot
- limitat la 1 household + 1 member_profile activ + 1 zi

Unitatea principala de selectie este reteta (`recipe`).

Datele principale consumate:
- `recipes`, `recipe_ingredients`, `recipe_nutrition_cache`, `member_profile`, `nutrition_target`, `household_preference_context`

Decizii arhitecturale importante:
- NU folosim OR-Tools/MILP in v1
- KNN nu este motorul principal in v1 (doar strat auxiliar)
- Nu se rescrie structura retetelor; se permit doar multiplicatori de portie

---

## 3. Principiul central al generatorului

Generatorul construieste planul pe baza de `recipes` si metadate asociate. Fiecare reteta este considerata un candidat atomic care poate fi inclus intr-un slot (breakfast/lunch/dinner/snack) si ajustat ca portie.

---

## 4. Ce NU face generatorul v1

- household optimization complet multi-profile
- planificare multi-day
- grocery/price estimation
- folosirea KNN ca motor principal
- rescrierea automata a retetelor sau componentizare automata

Generatorul poate ajusta portiile retetelor prin multiplicatori (ex. 0.8x/1.0x/1.2x), dar nu modifica lista de ingrediente.

---

## 5. Modelul profilului utilizatorului

Profilul este folosit pentru calculul `nutrition_target`, configurarea meselor si aplicarea restrictiilor hard.

Campuri obligatorii in `member_profile` (v1):
- `age`, `sex`, `weight_kg`, `height_cm`, `activity_level`, `goal`, `goal_speed`
- `training.sessions_per_week`, `training.type`
- `meal_config.meals_per_day`, `meal_config.include_snacks`
- `dietary_preferences` (chei booleene)
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
  "dietary_preferences": {"no_beef": false, "no_chicken": false},
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

---

## 11. Feedback

Tipuri minime:
- `liked`, `disliked`, `too_long`, `explicit_avoid`

Reguli:
- `liked` -> bonus de scor
- `disliked` -> penalizare de scor (nu hard ban implicit)
- `too_long` -> influenteaza `time_fit`
- `explicit_avoid` -> hard filter

---

## 12. Hard filters v1

- restrictii alimentare din `member_profile.dietary_preferences`
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

household_time_sensitivity: `low`, `normal`, `high` (default `normal`)

---

## 18. Slot fit

- potrivire rol-slot (ex. protein dense pentru protein slot)
- avoid combinations prin reguli hard (ex. cereal+milk la breakfast daca e interzis)

---

## 19. Feedback fit

- valoare neutra = 0.50
- liked exact = +0.25
- disliked exact = -0.30
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

## 24. Module logice recomandate

- `target_builder` (calc nutrition_target)
- `feedback_adapter` (normalizeaza evenimente de feedback)
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

- Checkpoint 0: contract generator, profil, nutrition_target, feedback, hard filters, scoring (acum)
- Checkpoint 1: 1 household + 1 member_profile activ + 1 zi
- Checkpoint 2: retete realiste + feedback minim + polish pe pilot
- Checkpoint 3: extindere la mai multe zile
- Checkpoint 4: ingredient reuse, grocery realism
- Checkpoint 5: household generation multi-profile

---

## 29. Decizie despre optimizare avansata

- OR-Tools / MILP / CP-SAT: nu in v1; se poate reevalua la Checkpoint 4/5

---

## 30. Decizie despre surse externe de date

- cooktime estimate: poate folosi API/AI extern cu caching local (optional)
- preturi / grocery external: nu in v1

---

## 31. Concluzie

Acest document stabileste contractul operational pentru Generator v1: recipe-based, deterministic, scoring-driven, limitat la 1 household + 1 profile + 1 zi. Urmatorul pas este implementarea Checkpoint 1 conform roadmap-ului.

