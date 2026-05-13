# Generator v1 checkpoint status

## Snapshot

Starea curenta: Generator v1 este demo/testing-ready pentru un profil activ, cu demo multi-day draft de 3 zile pe datasetul `v1_2_demo_final`.

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

## Checkpoint 2

Status: multi-day v1 draft demo-ready.

Acoperit:
- generare 3 zile
- `global_alternatives_3_day`
- hard no-repeat exact recipe
- `direct_from_slots`
- `quality_gate=demo_safe`
- `profile_guard=demo`

Smoke Round49:
- one-day valid/accept = true
- three-day valid = 3/3
- accept = 3/3
- repeated recipes = 0
- `multi_day_loss=0.006322`
- edge profile `sedentary_lose_fast_with_snack`, `target_kcal=1227.8`, este blocat in `profile_guard=demo`

## Explicit out of scope

- Nu exista OR-Tools / MILP / CP-SAT.
- KNN nu este motor principal de selectie.
- Nu exista grocery/price in Generator v1 demo.
- Nu exista household multi-member simultan.
- `profile_guard` nu schimba formulele de target.
- `v1_2_demo_final` nu este promovat la `current`.

## Limitari cunoscute

- Nu este productie QA.
- Family-level variety ramane imperfecta.
- Unele outlier risks raman cu warnings.
- Feedback-ul nu este inca bucla reala de invatare.
- Grocery/list prep este viitor.
- Household multi-member este ulterior.

## Urmatoarele checkpoint-uri posibile

A. Feedback + UI polish.

B. Family-level variety polish.

C. Source verification batch3.

D. Grocery/list prep.

E. Household multi-member later.
