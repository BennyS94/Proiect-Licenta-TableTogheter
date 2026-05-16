# Generator v1 checkpoint status

## Snapshot

Starea curenta: Generator v1 este demo/testing-ready pentru un profil activ, cu demo multi-day draft de 3 zile pe datasetul `v1_2_demo_final` si Feedback v1 local/demo.

Dataset demo:
- `dataset_profile=v1_2_demo_final`
- path: `data/recipesdb/draft/v1_2_demo_final/`
- total recipes: `266`
- active recipes: `261`
- sursa: `v1_2_demo_candidate_round48_cleaned`
- status: demo-final draft, nu productie/current

Date neatinse:
- `data/recipesdb/current`
- `data/fooddb/current`

## Checkpoint 0

Status: conceptual complet.

Acoperit:
- contract generator
- profil membru activ
- `nutrition_target`
- hard filters
- scoring deterministic
- directie recipe-based

## Checkpoint 1

Status: demo/testing-ready.

Acoperit:
- 1 household operational
- 1 `member_profile` activ
- generare 1 zi
- output CSV/JSON/readable
- Streamlit dashboard pentru test
- CLI smoke pentru profilul demo

Checkpoint 1 ramane demo/testing-ready dupa introducerea Feedback v1.

## Checkpoint 2

Status: multi-day v1 draft demo-ready.

Acoperit:
- generare 3 zile
- `global_alternatives_3_day`
- hard no-repeat exact recipe
- `direct_from_slots`
- `quality_gate=demo_safe`
- `profile_guard=demo`

Checkpoint 2 ramane demo/testing-ready dupa introducerea Feedback v1.

Smoke Round49:
- one-day valid/accept = true
- three-day valid = 3/3
- accept = 3/3
- repeated recipes = 0
- `multi_day_loss=0.006322`
- edge profile `sedentary_lose_fast_with_snack`, `target_kcal=1227.8`, este blocat in `profile_guard=demo`

## Feedback v1

Status: implementat ca feature local/demo pentru Generator v1.

Rol:
- post-demo feature / product-like polish pentru demo si testare
- nu schimba formulele de nutritie
- nu promoveaza datasetul `v1_2_demo_final` in `current`

Storage:
- `data/runtime/generator_v1_feedback_events.jsonl`

Tipuri:
- `liked`
- `disliked`
- `too_long`
- `explicit_avoid`

Comportament:
- `explicit_avoid` aplica hard filter pe `recipe_id`
- `liked` influenteaza scorul prin bonus soft
- `disliked` influenteaza scorul prin penalizare soft
- `too_long` reduce `time_fit` prin penalizare de timp
- feedback-ul se aplica la generatiile urmatoare
- `Clear feedback events` reseteaza contextul local

## Explicit out of scope

- Nu exista OR-Tools / MILP / CP-SAT.
- KNN nu este motor principal de selectie.
- Nu exista grocery/price in Generator v1 demo.
- Nu exista household multi-member simultan.
- Feedback v1 nu este backend de productie.
- Feedback v1 nu este sistem de conturi utilizator.
- Feedback v1 nu este ML/KNN.
- `profile_guard` nu schimba formulele de target.
- `v1_2_demo_final` nu este promovat la `current`.

## Limitari cunoscute

- Nu este productie QA.
- Family-level variety ramane imperfecta.
- Unele outlier risks raman cu warnings.
- Feedback v1 este local JSONL si nu este bucla reala de invatare.
- Feedback v1 nu propaga inca preferinte la nivel de ingrediente/familie.
- Grocery/list prep este viitor.
- Household multi-member este ulterior.

## Urmatoarele checkpoint-uri posibile

A. Feedback explainability + UI polish.

B. Family-level variety polish.

C. Source verification batch3.

D. Grocery/list prep.

E. Household multi-member later.
