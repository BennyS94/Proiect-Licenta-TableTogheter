# Generator service wrapper plan

## Purpose

Acest document defineste limita dintre backend-ul FastAPI si Generator v1. Backend-ul trebuie sa apeleze functii Python, nu CLI, Streamlit sau fisiere text generate.

Documentul este planificare. Nu creeaza modulul `src/generator_v1/service.py` si nu modifica logica generatorului.

## Planned Python module

Modul planificat:

```text
src/generator_v1/service.py
```

Rol:
- primeste request-uri deja validate de backend;
- traduce request-ul API in parametri pentru Generator v1;
- apeleaza functiile Python existente sau viitoare;
- intoarce dict-uri JSON-serializable;
- izoleaza backend-ul de detaliile CLI/Streamlit.

## Planned functions

```python
generate_individual_plan_from_request(request: dict) -> dict
generate_household_plan_from_request(request: dict) -> dict
build_grocery_list_for_plan(plan: dict, options: dict) -> dict
apply_feedback_event_from_request(request: dict) -> dict
build_feedback_context_for_household(household_id: str) -> dict
```

## Function responsibilities

`generate_individual_plan_from_request(request: dict) -> dict`:
- valideaza shape-ul minim primit de la backend;
- foloseste `dataset_profile`, `days`, `member_profile` si `generation_options`;
- apeleaza fluxul Generator v1 individual;
- intoarce `plan_id`, `generation_type=individual`, `daily_plan`, warnings si diagnostics summary.

`generate_household_plan_from_request(request: dict) -> dict`:
- foloseste `household_profile`, `selected_member_ids`, `household_mode` si `household_allocation_mode`;
- apeleaza Household Generation v1 Lite;
- intoarce `household_plan_id`, `per_member_menus`, `shared_meals`, macro summaries, grocery scaling si diagnostics summary.

`build_grocery_list_for_plan(plan: dict, options: dict) -> dict`:
- construieste grocery list pentru plan individual sau household;
- respecta optiunile `include_purchase_suggestions` si `include_price_estimates`;
- intoarce itemi JSON-safe, cost estimat optional si warnings.

`apply_feedback_event_from_request(request: dict) -> dict`:
- accepta `liked`, `disliked`, `too_long`, `explicit_avoid`;
- intoarce event-ul normalizat si un summary de context;
- pentru MVP, persistenta efectiva este responsabilitatea backend-ului.

`build_feedback_context_for_household(household_id: str) -> dict`:
- intoarce context agregat JSON-safe pentru household;
- poate fi conectat ulterior la SQLite in backend.

## Rules

- Backend-ul nu parseaza output text CLI.
- Backend-ul nu depinde de Streamlit.
- Backend-ul apeleaza functii Python, nu comenzi shell.
- Request-urile si raspunsurile trebuie sa fie JSON-serializable.
- Mobile app nu are acces direct la CSV-uri.
- Pentru MVP, data loading-ul file-based al generatorului poate ramane.
- Persistenta SQLite apartine backend layer, nu generatorului.
- Generatorul ramane sursa pentru target calculation, filtering/scoring, menu generation, grocery generation si feedback fit.

## Boundary sketch

```text
FastAPI route
  -> Pydantic/request validation
  -> generator_v1.service.generate_*_from_request(...)
  -> JSON-safe response dict
  -> SQLite persistence in backend
  -> HTTP JSON response
```

## JSON-safe conversion expectations

Wrapper-ul trebuie sa converteasca:

- pandas/numpy scalar types in `int`, `float`, `str`, `bool` sau `null`;
- DataFrame rows in liste de dict-uri;
- set-uri/tuple-uri in liste;
- NaN/Inf in `null` sau valori absente explicite;
- path-uri locale in metadata interna, nu in raspuns mobile-facing.

## Expected refactor risks

- Unele functii curente pot returna pandas/numpy types care nu sunt JSON-safe.
- Planurile pot contine structuri nested mari si campuri debug prea detaliate pentru mobile.
- Grocery output necesita formatare JSON-safe pentru `needed_grams`, `purchase_display`, warnings si preturi estimate.
- Feedback v1 este acum local/demo JSONL si trebuie ulterior inlocuit sau oglindit in SQLite.
- Exista risc de cuplare la CLI daca wrapper-ul nu primeste parametri expliciti.
- Household Lite are structuri de audit utile, dar mobile are nevoie de un view compact per membru/zi.

## Non-goals

- Nu implementeaza FastAPI.
- Nu implementeaza `src/generator_v1/service.py` in acest checkpoint.
- Nu modifica selectia de retete.
- Nu modifica formulele de target.
- Nu modifica grocery/pricing behavior.
- Nu introduce dependinte noi.
