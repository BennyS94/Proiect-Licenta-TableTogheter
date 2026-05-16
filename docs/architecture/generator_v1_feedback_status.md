# Generator v1 Feedback v1 status

## Purpose

Feedback v1 documenteaza bucla locala/demo prin care Generator v1 poate salva evenimente de feedback, le poate agrega intr-un `household_preference_context` si le poate folosi la generatiile urmatoare.

Acest mecanism este product-like polish pentru demo si testare. Nu este backend de productie, nu este ML si nu este sistem de conturi.

## Implemented modules

- `src/generator_v1/feedback_store.py`
  - append/load/clear pentru evenimente JSONL locale
  - parsing robust, cu warning pentru linii malformate
- `src/generator_v1/feedback_adapter.py`
  - agrega evenimentele in `household_preference_context`
  - filtreaza optional pe `household_id`, `member_profile_id`, `dataset_profile`
- `src/generator_v1/feedback_fit.py`
  - calculeaza `feedback_fit`, bonus/penalty si motive pentru candidat

## Storage

Storage default:

```text
data/runtime/generator_v1_feedback_events.jsonl
```

Fisierul este stare locala/demo. Nu trebuie tratat ca date de productie.

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

Surse folosite curent:

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

Daca aceeasi reteta are liked si disliked, se pastreaza ambele count-uri. Scoring-ul aplica efectul net.

## Generation behavior

- `explicit_avoid` este hard filter si exclude candidatul inainte de scoring.
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

- implicit, CLI incarca feedback daca fisierul JSONL exista
- `--feedback_disabled` foloseste context neutru
- `--show_feedback_context` afiseaza sumarul contextului agregat
- `--clear_feedback` sterge evenimentele locale si se opreste

## Streamlit support

Dashboard-ul `streamlit_app/generator_v1_dashboard.py` include controale pe fiecare masa generata:

- `Like`
- `Dislike`
- `Too long`
- `Avoid this recipe`

Dashboard-ul mai include:

- feedback context summary
- active feedback count
- `Clear feedback events`

Feedback-ul salvat se aplica la urmatoarea generatie. Meniul deja afisat nu este mutat retroactiv.

## Evaluator

Evaluator:

```text
tools/extra/evaluate_generator_v1_round52_feedback_effect.py
```

Output-uri:

- `data/recipesdb/audit/generator_v1_round52_feedback_effect_summary.txt`
- `data/recipesdb/audit/generator_v1_round52_feedback_effect_runs.csv`
- `data/recipesdb/audit/generator_v1_round52_feedback_effect_meals.csv`
- `data/recipesdb/audit/generator_v1_round52_feedback_context.json`

Rezultat confirmat pe `v1_2_demo_final`:

- `explicit_avoid` elimina reteta evitata
- `disliked` influenteaza scorul si poate schimba selectia
- `liked` creste `feedback_fit`
- `too_long` aplica penalizare de timp
- planul ramane valid

## Limitations

- local JSONL only
- fara conturi reale
- fara DB/backend/server
- fara ML/KNN
- fara personalizare de productie
- fara propagare la nivel de ingrediente
- fara propagare la nivel de familie de retete
- context household/demo only

## Next possible improvements

- storage persistent in DB
- integrare cu user/account real
- propagare feedback la ingrediente si familii de retete
- analytics pentru feedback
- vizualizare before/after candidate ranks in dashboard
- ML/KNN mai tarziu, dupa date curate si feedback explicit suficient
