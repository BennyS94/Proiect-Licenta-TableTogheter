# Backend readiness checklist

## Purpose

Acest checklist marcheaza ce trebuie sa existe inainte de crearea folderului `backend/` si implementarea FastAPI.

## Contract and planning

- [x] API contract exists: `docs/architecture/api_contract_v1.md`
- [x] SQLite schema exists: `docs/architecture/sqlite_schema_v1.md`
- [x] Generator service wrapper plan exists: `docs/architecture/generator_service_wrapper_plan.md`
- [x] Sample requests/responses exist: `docs/api_examples/`
- [x] Mobile/backend roadmap exists: `docs/architecture/mobile_backend_roadmap.md`

## Generator demo foundation

- [x] `v1_2_demo_final` dataset exists as demo draft.
- [x] `profile_guard` works for demo safety.
- [x] Individual generation works for 1-5 days.
- [x] Household Generation v1 Lite works.
- [x] Household member selection and per-member output exist in Streamlit.
- [x] Grocery list with purchase suggestions exists.
- [x] Grocery cooked-to-raw helpers exist.
- [x] Grocery price estimates exist as demo estimates.
- [x] Feedback v1 works as local/demo feature.

## Guardrails

- [x] No `data/recipesdb/current` mutation needed for backend skeleton.
- [x] No `data/fooddb/current` mutation needed for backend skeleton.
- [x] No generator logic change needed for backend skeleton.
- [x] No grocery/pricing behavior change needed for backend skeleton.
- [x] No mobile UI code needed before API skeleton.

## Ready for next checkpoint

- [x] Ready to create `backend/` folder.
- [x] Backend M1 skeleton created.
- [x] Implement FastAPI app shell.
- [x] Add `GET /health`.
- [x] Add SQLite connection/settings.
- [x] Add SQLite initialization helper.
- [x] Backend dependencies declared in `backend/requirements_backend.txt`.
- [x] Full FastAPI smoke is available after installing `backend/requirements_backend.txt`.
- [x] Full FastAPI `/health` smoke passed in the current environment after installing backend requirements.
- [x] Generator service wrapper module exists at `src/generator_v1/service.py`.
- [x] Generator service smoke exists at `tools/extra/check_generator_service_m2.py`.
- [x] Generator service smoke passed for individual generation, household generation, grocery list, feedback context and JSON-safe responses.
- [x] Backend M3 generation endpoints implemented.
- [x] `POST /plans/generate` calls the Generator v1 service wrapper.
- [x] `POST /household-plans/generate` calls the Generator v1 service wrapper.
- [x] `GET /plans/{plan_id}` returns saved response JSON.
- [x] `GET /household-plans/{plan_id}` returns saved household response JSON.
- [x] `GET /plans/{plan_id}/grocery-list` returns saved grocery JSON.
- [x] SQLite stores request JSON, response JSON, grocery JSON and best-effort day/meal/item indexes.
- [x] Backend M3 smoke passed for generation, retrieval, grocery retrieval and SQLite row creation.
- [x] Backend M4 demo household endpoint implemented.
- [x] Backend M4 profile endpoints implemented: `GET /profiles`, `POST /profiles`, `GET /profiles/{member_profile_id}`.
- [x] Backend M4 feedback endpoints implemented: `POST /feedback`, `GET /feedback/context`, `DELETE /feedback`.
- [x] Feedback API persists to SQLite.
- [x] Feedback context is aggregated from SQLite feedback events.
- [x] Backend M4 smoke passed for demo household, profile CRUD-lite, feedback create/context/delete and generation endpoint continuity.
- [x] Backend M5 persistence-aware generation implemented.
- [x] `POST /plans/generate` accepts `member_profile_id` and resolves the profile from SQLite.
- [x] `POST /household-plans/generate` can build a household profile from SQLite profiles and filter with `selected_member_ids`.
- [x] SQLite feedback context is injected into generation when `feedback_enabled=true`.
- [x] Inline JSON profile mode remains supported for testing/compatibility.
- [x] Backend M5 smoke passed for profile-id generation, selected-member household generation, feedback injection and persistence.
- [x] Mobile M1 Expo Android skeleton created under `mobile/`.
- [x] Mobile M1 can call `GET /health` through a small fetch client.
- [x] Mobile M1 structure smoke exists at `tools/extra/check_mobile_m1_structure.py`.
- [x] Mobile M2 demo household, member selection, individual generation and plan display implemented.
- [x] Mobile M3 grocery list display with purchase suggestions and price estimates implemented.
- [x] Mobile M4 meal feedback buttons, feedback context panel and regenerate-with-feedback flow implemented.
- [x] Mobile M4 runtime validation confirmed `Avoid` feedback can remove the avoided recipe from the next plan when alternatives exist.
- [x] Mobile M5 household generation mobile screen implemented.
- [x] Mobile M6 saved profile creation/list/select implemented.
- [x] Mobile M7 saved-profile household generation implemented.
- [x] Backend M8 profile soft-deactivate endpoint implemented: `DELETE /profiles/{member_profile_id}?confirm=true`.
- [x] Backend M8 profile deactivation endpoint documented in `docs/architecture/api_contract_v1.md`.
- [x] Backend KNN-2 recipe alternatives endpoint implemented: `POST /recipes/similar`.
- [x] KNN-2 keeps KNN-lite as candidate provider and Generator v1 as approval gate.
- [x] Mobile M8 saved profile remove UI implemented for saved profiles only.
- [x] Mobile M8 clear feedback UI implemented through `DELETE /feedback?confirm=true`.

## Next checkpoint

- [x] Backend M2: generator service wrapper.
- [x] Backend M3: FastAPI endpoints for plan generation, household plan generation and grocery list retrieval.
- [x] Backend M4: feedback endpoints and profile/household CRUD-lite.
- [x] Backend M5: persistence-aware generation endpoints.
- [x] Mobile M1: Expo Android skeleton with `/health` connectivity.
- [x] Mobile M2: call demo household and generation endpoints from mobile.
- [x] Mobile M3: display grocery list from generated plan response.
- [x] Mobile M4: submit feedback and regenerate with SQLite feedback context.
- [x] Mobile M5: household generation mobile screen.
- [x] Mobile M6: profile persistence / create profile UI.
- [x] Mobile M7: saved-profile household generation.
- [x] Mobile M8: saved profile remove and clear feedback cleanup.
- [x] Backend KNN-2: approved/review recipe alternatives endpoint.
- [ ] Later: UI polish for MVP demo.
- [ ] Later: mobile alternatives UI and explicit replacement flow.

## Non-goals for backend skeleton

- No mobile app implementation.
- No iOS.
- No cloud production deployment.
- No login complex.
- No live price scraping.
- No advanced household optimizer.
