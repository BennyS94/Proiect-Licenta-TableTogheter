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
- [ ] Implement FastAPI app shell.
- [ ] Add `GET /health`.
- [ ] Add SQLite connection/settings.
- [ ] Add generator service wrapper module after a small dedicated implementation checkpoint.

## Non-goals for backend skeleton

- No mobile app implementation.
- No iOS.
- No cloud production deployment.
- No login complex.
- No live price scraping.
- No advanced household optimizer.
