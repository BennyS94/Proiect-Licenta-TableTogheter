# Backend README

## Purpose

Acest folder contine scheletul backend M1 pentru TableTogether API.

Backend-ul este gandit pentru arhitectura:

```text
Android Mobile App -> FastAPI Backend -> Python Generator -> Food_DB + Recipes_DB + SQLite
```

## Current scope

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

## Not implemented yet

- mobile app
- auth/login
- cloud deployment
- production DB
- live price scraping
- advanced household optimizer

## Current limitations

- Endpointurile M3 sunt MVP/demo local.
- Persistenta este SQLite locala, nu schema production.
- Nu exista auth/login.
- Nu exista app mobile inca.
- Generatorul ramane Python si ruleaza in backend process.
- `GET /profiles` returneaza doar profilurile salvate in SQLite; pentru membrii demo foloseste `GET /households/demo`.
