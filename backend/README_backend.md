# Backend README

## Purpose

Acest folder contine backend-ul FastAPI MVP pentru TableTogether API.

Backend-ul este gandit pentru arhitectura:

```text
Android Mobile App -> FastAPI Backend -> Python Generator -> Food_DB + Recipes_DB + SQLite
```

## Current scope

Nota: sectiunile M1/M3/M4/etc. pastreaza istoricul checkpoint-urilor, dar starea curenta include toate checkpoint-urile bifate mai jos, nu doar scheletul M1.

Implementat in M1:

- structura minima `backend/`
- FastAPI app shell
- `GET /`
- `GET /health`
- configurare de baza
- schema SQLite MVP
- helper de initializare SQLite
- smoke script fara server pornit

Implementat in M3:

- `POST /plans/generate`
- `POST /household-plans/generate`
- `GET /plans/{plan_id}`
- `GET /household-plans/{plan_id}`
- `GET /plans/{plan_id}/grocery-list`
- persistenta SQLite demo pentru request JSON, response JSON si grocery JSON
- indexare best-effort pentru zile, mese si grocery items

Implementat in M4:

- `GET /households/demo`
- `GET /profiles`
- `POST /profiles`
- `GET /profiles/{member_profile_id}`
- `POST /feedback`
- `GET /feedback/context`
- `DELETE /feedback`
- persistenta SQLite demo pentru profile si feedback events
- feedback context agregat din SQLite

Implementat in M5:

- `POST /plans/generate` poate folosi `member_profile_id` din SQLite
- `POST /household-plans/generate` poate construi household din profile SQLite pentru `household_id`
- `selected_member_ids` filtreaza membrii pentru generarea household
- contextul feedback SQLite este injectat in generatie cand `feedback_enabled=true`
- profilurile inline raman suportate pentru testare si compatibilitate

Implementat in M8:

- `DELETE /profiles/{member_profile_id}?confirm=true` soft-dezactiveaza profiluri salvate
- `GET /profiles` continua sa returneze doar profiluri active implicit
- `DELETE /feedback?confirm=true` ramane cleanup local/demo pentru feedback events

Implementat in KNN-2:

- `POST /recipes/similar` returneaza alternative de retete prin KNN-lite + generator approval gate
- KNN propune candidati, dar Generator v1 valideaza slot/profil/feedback/macro/timp/realism
- endpointul nu persista alternative, nu modifica planuri si nu inlocuieste mese automat

Implementat in KNN-4:

- `POST /plans/{plan_id}/replace-meal` face preview/apply pentru inlocuire explicita de masa/reteta intreaga
- `dry_run=true` returneaza impact preview fara persistenta
- `dry_run=false` creeaza un plan nou derivat, recalculeaza grocery list si salveaza rezultatul in SQLite
- aplica doar alternative `approved`; alternativele `review` raman preview-only
- KNN ramane candidate provider, iar Generator v1 ramane approval gate
- ingredient-level substitution nu este implementat in KNN-4

Implementat in Auth-M1:

- `POST /auth/register`
- `POST /auth/login`
- `POST /auth/logout`
- `GET /auth/me`
- conturi locale SQLite cu `users`, `user_sessions` si household implicit per cont
- parole stocate ca PBKDF2-HMAC-SHA256 hash + salt, niciodata plaintext
- token brut returnat clientului si hash de token stocat in SQLite
- profile scoped pe household-ul contului cand requestul include `Authorization: Bearer <session_token>`
- fara email verification, password reset, email sending, cloud auth sau production-grade auth claims

Nota de scoping curenta:

- auth, profile si household settings folosesc sesiunea locala cand este trimis `Authorization: Bearer <session_token>`.
- endpointurile istorice de generatie, feedback, alternatives si replacement raman MVP/local si nu au inca ownership enforcement uniform pe account.

Implementat in PROGRESS-1:

- `POST /progress/daily`
- `GET /progress/daily?member_profile_id=...`
- `DELETE /progress/daily/{progress_id}`
- persistenta SQLite in `saved_daily_progress`
- unicitate pe `member_profile_id + plan_id + day_index`
- maximum 30 snapshoturi salvate per profil, enforced backend-side
- scoping obligatoriu pe `Authorization: Bearer <session_token>` si household-ul contului
- delete sterge doar snapshotul de progres, nu planul generat

Implementat in PROFILE-WIZARD-1:

- `POST /profiles`, `GET /profiles` si `GET /profiles/{member_profile_id}` accepta/returneaza `dietary_preferences.no_pork`
- profilurile accepta/returneaza `food_preferences.ratings`, `food_preferences.avoid_ingredients` si `food_preferences.cooking_time_preference`
- profilurile accepta/returneaza `health_and_diet_preferences.dietary_patterns` pentru `keto`, `paleo` si `mediterranean`
- profilurile accepta/returneaza `health_and_diet_preferences.health_modes.diabetes_aware`
- profilurile accepta/returneaza `health_and_diet_preferences.health_modes.hypertension_friendly`
- profilurile accepta/returneaza `health_and_diet_preferences.health_modes.heart_friendly`
- profilurile vechi fara aceste campuri primesc defaults compatibile: `no_pork=false`, `ratings={}`, `avoid_ingredients=[]`, `cooking_time_preference=balanced`
- profilurile vechi fara `health_and_diet_preferences` primesc defaults compatibile cu toate optiunile `false`
- SQLite foloseste `food_preferences_json` si `health_and_diet_preferences_json` in `member_profiles`; migratia locala este aplicata in `init_db`
- `Avoid` pentru cheile suportate si ingredientele custom intra in hard filter-ul generatorului; `Dislike` ramane soft/persistat, nu hard ban
- soft scoring pentru `like`/`dislike` la nivel de aliment/familie este deferat pentru PROFILE-PREF-2

## Run

Instaleaza dependintele backend minime:

```powershell
pip install -r backend/requirements_backend.txt
```

Ruleaza API-ul din radacina proiectului:

```powershell
uvicorn backend.app.main:app --reload
```

Mediul Python trebuie sa aiba instalate `fastapi` si `uvicorn`. Dependintele backend sunt declarate separat de restul proiectului in `backend/requirements_backend.txt`.

Health endpoint:

```text
http://127.0.0.1:8000/health
```

Raspuns asteptat dupa initializarea DB:

```json
{
  "status": "ok",
  "service": "tabletogether-api",
  "version": "v1",
  "database": "ok"
}
```

Generation endpoints MVP:

```text
POST http://127.0.0.1:8000/plans/generate
POST http://127.0.0.1:8000/household-plans/generate
GET  http://127.0.0.1:8000/plans/{plan_id}
GET  http://127.0.0.1:8000/household-plans/{plan_id}
GET  http://127.0.0.1:8000/plans/{plan_id}/grocery-list
```

Endpointurile de generare apeleaza `src/generator_v1/service.py`. Backend-ul nu ruleaza CLI-ul si nu parseaza output text.

Profile si feedback endpoints MVP:

```text
GET    http://127.0.0.1:8000/households/demo
GET    http://127.0.0.1:8000/profiles
POST   http://127.0.0.1:8000/profiles
GET    http://127.0.0.1:8000/profiles/{member_profile_id}
DELETE http://127.0.0.1:8000/profiles/{member_profile_id}?confirm=true
POST   http://127.0.0.1:8000/feedback
GET    http://127.0.0.1:8000/feedback/context
DELETE http://127.0.0.1:8000/feedback?confirm=true
```

Feedback API foloseste SQLite ca sursa backend. Din M5, endpointurile de generatie pot primi context feedback agregat din SQLite. CLI/Streamlit isi pastreaza comportamentul JSONL/local existent.

Profile delete din M8 este soft delete: seteaza `is_active=0` si actualizeaza `updated_at`; nu sterge randul fizic din SQLite. Endpointul cere `confirm=true`.

Auth endpoints MVP:

```text
POST http://127.0.0.1:8000/auth/register
POST http://127.0.0.1:8000/auth/login
POST http://127.0.0.1:8000/auth/logout
GET  http://127.0.0.1:8000/auth/me
```

`/auth/register` creeaza cont local, household implicit si sesiune. `/auth/login` creeaza o sesiune noua. `/auth/logout` revoca tokenul daca este furnizat. `/auth/me` cere header `Authorization: Bearer <session_token>`.

Profilele create/listate cu acelasi header sunt limitate la household-ul contului. Fara header, endpointurile de profile pastreaza calea dev/smoke existenta pentru compatibilitate.

Progress endpoints:

```text
POST   http://127.0.0.1:8000/progress/daily
GET    http://127.0.0.1:8000/progress/daily?member_profile_id={member_profile_id}
DELETE http://127.0.0.1:8000/progress/daily/{progress_id}
```

Toate endpointurile PROGRESS-1 cer `Authorization: Bearer <session_token>`. Snapshoturile sunt per profil, plan si zi. Duplicatele returneaza `status=already_saved` si nu creeaza rand nou.

Schema profilului suporta in plus:

```json
{
  "dietary_preferences": {
    "vegetarian": false,
    "vegan": false,
    "gluten_free": false,
    "no_beef": false,
    "no_pork": false,
    "no_chicken": false,
    "no_fish": false,
    "no_dairy": false
  },
  "food_preferences": {
    "ratings": {
      "chicken": "like",
      "pork": "avoid"
    },
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
      "diabetes_aware": true,
      "hypertension_friendly": true,
      "heart_friendly": true
    }
  }
}
```

Recipe alternatives endpoint KNN-2:

```text
POST http://127.0.0.1:8000/recipes/similar
```

Endpointul accepta `recipe_id`, `slot`, `top_k`, `candidate_pool_k`, `dataset_profile`, profil inline sau `member_profile_id`, si `approval_mode`. Daca `feedback_enabled=true`, contextul SQLite poate respinge candidati prin `explicit_avoid`.

Meal replacement endpoint KNN-4:

```text
POST http://127.0.0.1:8000/plans/{plan_id}/replace-meal?dry_run=true
POST http://127.0.0.1:8000/plans/{plan_id}/replace-meal?dry_run=false
```

Endpointul accepta `day_index`, `slot`, `current_recipe_id`, `alternative_recipe_id`, `generation_type`, optional `replace_scope` si optional `member_id` / `member_profile_id`. Preview-ul nu persista nimic. Apply-ul creeaza plan nou derivat si grocery list nou; planul original ramane in SQLite nemodificat. Replacement-ul este doar meal-level / recipe-level, nu inlocuire de ingrediente in interiorul retetei.

## SQLite

Path implicit:

```text
data/runtime/tabletogether_demo.db
```

Folderul `data/runtime/` este local/demo si este ignorat de git.

Schema este in:

```text
backend/app/db/schema.sql
```

Initializarea este in:

```text
backend/app/db/database.py
```

## Smoke check

```powershell
python tools/extra/check_backend_m1.py
```

Output sumar:

```text
data/recipesdb/audit/backend_m1_smoke_summary.txt
```

Smoke-ul initializeaza SQLite, verifica tabelele cerute si, daca `fastapi` este instalat, testeaza `GET /health` prin `TestClient` fara server pornit.

Smoke pentru wrapper-ul Generator v1:

```powershell
python tools/extra/check_generator_service_m2.py
```

Smoke pentru endpointurile de generare M3:

```powershell
python tools/extra/check_backend_m3_generation_endpoints.py
```

Output sumar M3:

```text
data/recipesdb/audit/backend_m3_generation_endpoints_summary.txt
```

Smoke pentru saved daily progress snapshots:

```powershell
python tools/extra/check_progress_daily_snapshots.py
```

Output sumar PROGRESS-1:

```text
data/recipesdb/audit/progress_daily_snapshots_summary.txt
```

Mostre response M3:

```text
data/recipesdb/audit/backend_m3_individual_response_sample.json
data/recipesdb/audit/backend_m3_household_response_sample.json
```

Smoke pentru profile si feedback M4:

```powershell
python tools/extra/check_backend_m4_profiles_feedback.py
```

Output sumar M4:

```text
data/recipesdb/audit/backend_m4_profiles_feedback_summary.txt
```

Mostre M4:

```text
data/recipesdb/audit/backend_m4_profile_sample.json
data/recipesdb/audit/backend_m4_feedback_context_sample.json
```

Smoke pentru generatie persistence-aware M5:

```powershell
python tools/extra/check_backend_m5_persistence_aware_generation.py
```

Output sumar M5:

```text
data/recipesdb/audit/backend_m5_persistence_generation_summary.txt
```

Mostre M5:

```text
data/recipesdb/audit/backend_m5_individual_response_sample.json
data/recipesdb/audit/backend_m5_household_response_sample.json
data/recipesdb/audit/backend_m5_feedback_context_used_sample.json
```

Smoke pentru cleanup profile/feedback M8:

```powershell
python tools/extra/check_backend_m8_profile_feedback_cleanup.py
```

Output sumar M8:

```text
data/recipesdb/audit/backend_m8_profile_feedback_cleanup_summary.txt
```

Smoke pentru Auth-M1:

```powershell
python tools/extra/check_backend_auth_m1.py
```

Output Auth-M1:

```text
data/recipesdb/audit/backend_auth_m1_summary.txt
data/recipesdb/audit/backend_auth_m1_response_sample.json
```

Smoke pentru alternative retete KNN-2:

```powershell
python tools/extra/check_backend_knn_recipe_alternatives.py
```

Output KNN-2:

```text
data/recipesdb/audit/backend_knn_recipe_alternatives_summary.txt
data/recipesdb/audit/backend_knn_recipe_alternatives_response_sample.json
data/recipesdb/audit/backend_knn_recipe_alternatives_candidates.csv
```

Smoke pentru replacement retete KNN-4:

```powershell
python tools/extra/check_backend_knn_meal_replacement.py
```

Output KNN-4:

```text
data/recipesdb/audit/backend_knn_meal_replacement_summary.txt
data/recipesdb/audit/backend_knn_meal_replacement_preview_sample.json
data/recipesdb/audit/backend_knn_meal_replacement_apply_sample.json
```

DATA-QA-1 price/time coverage:

```powershell
python tools/extra/audit_data_qa_price_time_coverage.py
python tools/extra/audit_data_qa_recipe_time_coverage.py
python tools/extra/check_data_qa_price_time_no_missing.py
```

DATA-QA-1 verifica faptul ca outputurile app-facing de grocery nu expun preturi lipsa si ca mesele generate expun estimari utilizabile de cooking time. Pricing-ul backend ramane static/demo/reference-based: catalog de produse, aliasuri si fallback-uri controlate. Nu exista live price fetching, scraping runtime, store optimization sau cart/brand optimization.

## Not implemented yet

- cloud deployment
- production DB
- 7/14/30 day progress charts
- live price scraping
- advanced household optimizer
- automatic meal replacement
- ingredient substitution
- email verification
- password reset email
- change email / change password endpoints

## Current limitations

- Endpointurile M3 sunt MVP/demo local.
- Persistenta este SQLite locala, nu schema production.
- Auth-M1 este local SQLite MVP, nu sistem production-grade.
- Sesiunile sunt long-lived local si nu au inca management avansat.
- Generatorul ramane Python si ruleaza in backend process.
- `GET /profiles` returneaza doar profilurile salvate in SQLite; pentru membrii demo foloseste `GET /households/demo`.
