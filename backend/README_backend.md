# TableTogether Backend

This folder contains the FastAPI backend used by the TableTogether mobile app.

The backend owns the HTTP API, local SQLite persistence, request validation and orchestration of the Python meal-plan generator. The mobile app talks to this service over HTTP/JSON; it does not read CSV files or run generator code directly.

## Runtime Role

```text
React Native / Expo mobile app
  -> FastAPI backend
  -> SQLite persistence
  -> Generator service
  -> Food_DB / Recipes_DB
```

The backend is responsible for:

- local account registration, login, logout and session lookup;
- household and member profile storage;
- plan generation requests for one profile or for a household;
- generated plan and grocery-list persistence;
- feedback storage and feedback context retrieval;
- recipe alternatives and meal replacement preview/apply flows;
- daily progress snapshots and trend history.

## Main Files

- `backend/app/main.py` - FastAPI app setup and route registration.
- `backend/app/api/routes/` - route modules grouped by feature.
- `backend/app/schemas/` - request and response models.
- `backend/app/db/schema.sql` - SQLite schema.
- `backend/app/db/database.py` - database initialization and connection helpers.
- `backend/app/db/repositories.py` - persistence helpers for plans, profiles, feedback and progress.
- `backend/app/db/auth_repository.py` - account/session persistence.
- `backend/app/core/config.py` - backend configuration.

The generator integration is handled through `src/generator_v1/service.py`.

## Local Setup

From the repository root:

```powershell
pip install -r backend/requirements_backend.txt
uvicorn backend.app.main:app --reload
```

For testing from a physical phone on the same network:

```powershell
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```

Health check:

```text
GET http://127.0.0.1:8000/health
```

Expected response shape:

```json
{
  "status": "ok",
  "service": "tabletogether-api",
  "version": "v1",
  "database": "ok"
}
```

## API Surface

Authentication:

```text
POST /auth/register
POST /auth/login
POST /auth/logout
GET  /auth/me
```

Profiles and household:

```text
GET    /households/demo
GET    /profiles
POST   /profiles
GET    /profiles/{member_profile_id}
DELETE /profiles/{member_profile_id}?confirm=true
```

Plan generation and retrieval:

```text
POST /plans/generate
POST /household-plans/generate
GET  /plans/{plan_id}
GET  /household-plans/{plan_id}
GET  /plans/{plan_id}/grocery-list
```

Feedback:

```text
POST   /feedback
GET    /feedback/context
DELETE /feedback?confirm=true
```

Recipe alternatives and replacement:

```text
POST /recipes/similar
POST /plans/{plan_id}/replace-meal?dry_run=true
POST /plans/{plan_id}/replace-meal?dry_run=false
```

Daily progress:

```text
POST   /progress/daily
GET    /progress/daily?member_profile_id={member_profile_id}&limit=30
DELETE /progress/daily/{progress_id}
POST   /progress/daily/{progress_id}/delete
```

## Persistence

Default SQLite path:

```text
data/runtime/tabletogether_demo.db
```

`data/runtime/` is local runtime state and is ignored by Git.

The current backend is local-first. It is useful for development, thesis validation and portfolio demonstration, but it is not a cloud production deployment.

## Generator Integration

The backend calls `src/generator_v1/service.py` directly as a Python service layer. It does not shell out to the CLI and does not parse text output.

Generation supports:

- individual profile plans;
- household plans based on saved member profiles;
- deterministic grocery-list output;
- feedback-aware generation;
- recipe alternatives through KNN-lite candidate retrieval plus generator approval;
- explicit meal-level replacement with preview before apply.

KNN-lite is an auxiliary alternatives mechanism, not the main meal-plan generator.

## Verification

Useful checks from the repository root:

```powershell
python tools/extra/check_backend_m1.py
python tools/extra/check_generator_service_m2.py
python tools/extra/check_backend_m3_generation_endpoints.py
python tools/extra/check_backend_m4_profiles_feedback.py
python tools/extra/check_backend_m5_persistence_aware_generation.py
python tools/extra/check_backend_auth_m1.py
python tools/extra/check_backend_knn_recipe_alternatives.py
python tools/extra/check_backend_knn_meal_replacement.py
python tools/extra/check_progress_daily_snapshots.py
python tools/extra/check_progress_trends_backend.py
python tools/extra/check_data_qa_price_time_no_missing.py
```

General compile check:

```powershell
python -m compileall backend tools/extra src/generator_v1 src/generator_v1_cli.py
```

## Current Limitations

- SQLite persistence is local, not cloud-hosted.
- Local authentication does not include email verification or password reset email.
- Generated prices are static/reference-based estimates, not live supermarket prices.
- Pantry inventory, store optimization and brand optimization are not implemented.
- Health-aware options are non-clinical preference/scoring modes, not medical advice.
- Ingredient-level substitution is not implemented; replacement is recipe/meal-level.
