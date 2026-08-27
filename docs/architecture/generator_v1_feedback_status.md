# Generator v1 Feedback v1 status

## Purpose

Feedback v1 documenteaza bucla explicita prin care TableTogether poate salva evenimente de feedback, le poate agrega intr-un `household_preference_context` si le poate folosi la generatiile urmatoare.

Mecanismul ramane simplu si interpretabil. Nu este ML, nu este KNN si nu invata automat un model opac.

## Current status

Feedback v1 exista in doua contexte:

1. CLI / generator local
   - foloseste JSONL local;
   - este util pentru audit, smoke si debug.

2. Backend / Mobile MVP
   - foloseste SQLite prin endpointurile FastAPI;
   - evenimentele sunt persistate in `feedback_events`;
   - contextul agregat este injectat in generare cand `feedback_enabled=true`.

Astfel, formularea veche care limita feedback-ul la stocare locala fara integrare backend nu mai este corecta pentru flow-ul app-facing.

## Implemented modules

Generator:

- `src/generator_v1/feedback_store.py`
  - append/load/clear pentru evenimente JSONL locale;
  - folosit in CLI si smoke-uri locale.
- `src/generator_v1/feedback_adapter.py`
  - agrega evenimentele in `household_preference_context`;
  - filtreaza optional pe `household_id`, `member_profile_id`, `dataset_profile`.
- `src/generator_v1/feedback_fit.py`
  - calculeaza `feedback_fit`, bonus/penalty si motive pentru candidat.

Backend:

- `backend/app/api/routes/feedback.py`
  - `POST /feedback`
  - `GET /feedback/context`
  - `DELETE /feedback?confirm=true`
- `backend/app/db/repositories.py`
  - salveaza evenimentele in SQLite;
  - construieste contextul agregat pentru generare.
- `backend/app/api/routes/plans.py`
  - injecteaza feedback context in generarea individuala.
- `backend/app/api/routes/household_plans.py`
  - injecteaza feedback context in generarea household.
- `backend/app/api/routes/recipes.py`
  - foloseste feedback context pentru alternatives/approval.
- `backend/app/api/routes/plan_replacements.py`
  - foloseste feedback context in preview/apply replacement.

Mobile:

- trimite feedback pentru mese generate;
- poate cere contextul agregat;
- poate curata feedback-ul local/demo;
- mesajul operational este ca feedback-ul se aplica la generari viitoare.

## Storage

CLI/generator-local default:

```text
data/runtime/generator_v1_feedback_events.jsonl
```

Backend/mobile:

```text
data/runtime/tabletogether_demo.db
table: feedback_events
```

SQLite ramane local/demo in MVP. Nu este cloud sync si nu este sistem production-grade de preferinte.

## Event schema summary

Campuri principale:

- `event_id`
- `created_at`
- `household_id`
- `member_profile_id`
- `dataset_profile`
- `recipe_id`
- `recipe_family_name`
- `display_name`
- `slot`
- `feedback_type`
- `source`
- `run_id`
- `plan_id`
- `notes`

Tipuri suportate pentru `feedback_type`:

- `liked`
- `disliked`
- `too_long`
- `explicit_avoid`

Surse curente:

- `mobile`
- `api_demo`
- `streamlit`
- `cli`
- `test`

## Household preference context

Structura agregata:

```json
{
  "household_id": "...",
  "hard_filters": {
    "banned_recipe_ids": [],
    "banned_ingredient_names": []
  },
  "score_preferences": {
    "liked_recipe_ids": {},
    "disliked_recipe_ids": {}
  },
  "time_preferences": {
    "too_long_recipe_ids": {},
    "household_time_sensitivity": "normal"
  },
  "meta": {
    "event_count": 0,
    "last_updated_at": "..."
  }
}
```

Agregare curenta:

- `liked`: count per `recipe_id`
- `disliked`: count per `recipe_id`
- `too_long`: count per `recipe_id`
- `explicit_avoid`: adauga `recipe_id` in `banned_recipe_ids`

Daca aceeasi reteta are `liked` si `disliked`, se pastreaza ambele count-uri. Scoring-ul aplica efectul net.

## Generation behavior

- `explicit_avoid` este hard filter pe `recipe_id` si exclude candidatul inainte de scoring.
- `liked` creste `feedback_fit` cu +0.12 per eveniment, cu contributie maxima +0.25.
- `disliked` scade `feedback_fit` cu -0.15 per eveniment, cu contributie maxima -0.30.
- `too_long` aplica `time_feedback_penalty` si reduce `time_fit`.
- Feedback-ul nu modifica formulele de target nutritional.
- Validarea planului ramane neschimbata; feedback-ul nu poate face un plan nutritional invalid sa treaca.

## CLI support

Argumente disponibile:

- `--feedback_events_path`
- `--show_feedback_context`
- `--clear_feedback`
- `--feedback_disabled`

Comportament:

- implicit, CLI incarca feedback daca fisierul JSONL exista;
- `--feedback_disabled` foloseste context neutru;
- `--show_feedback_context` afiseaza sumarul contextului agregat;
- `--clear_feedback` sterge evenimentele locale si se opreste.

## Legacy dashboard note

Earlier local experiments used a Streamlit dashboard for manual feedback testing. That dashboard is not part of the current public runtime path. The supported app-facing feedback flow is mobile -> FastAPI -> SQLite -> generator context.

## Backend/mobile support

Endpointuri:

```text
POST   /feedback
GET    /feedback/context
DELETE /feedback?confirm=true
```

Generarea individuala si household primeste context agregat din SQLite atunci cand `feedback_enabled=true`.

Behavior important:

- `Avoid` pe masa/reteta exclude reteta marcata in generari ulterioare daca exista alternative fezabile.
- Ingredientele evitate explicit la nivel de profil sunt tratate separat prin `food_preferences.avoid_ingredients`.
- `Like` si `Dislike` la nivel de aliment/familie din profile sunt persistate, dar soft scoring ingredient/family ramane deferat.

## Evaluators and checks

Evaluator generator:

```text
tools/extra/evaluate_generator_v1_round52_feedback_effect.py
```

Backend checks:

```text
tools/extra/check_backend_m4_profiles_feedback.py
tools/extra/check_backend_m5_persistence_aware_generation.py
```

## Limitations

- SQLite este local/demo, nu cloud sau productie.
- Nu exista sincronizare intre dispozitive.
- Nu exista invatare ML.
- Nu exista propagare automata completa la ingrediente sau familii de retete.
- `Like`/`Dislike` din profil sunt persistate, dar scoring-ul ingredient/family este viitor.
- Feedback-ul influenteaza generari viitoare, nu modifica retroactiv planul deja afisat.

## Next possible improvements

- persistenta sesiune mobila peste restart de app;
- analytics pentru feedback;
- vizualizare before/after candidate ranks;
- propagare controlata la nivel de ingrediente si familii;
- eventual ML/KNN mai tarziu, dupa date curate si feedback explicit suficient.
