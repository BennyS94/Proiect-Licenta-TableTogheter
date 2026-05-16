# Generator v1 checkpoint status

## Snapshot

Starea curenta: Generator v1 este demo/testing-ready pentru un profil activ, cu demo multi-day draft configurabil 1-5 zile pe datasetul `v1_2_demo_final`, Feedback v1 local/demo si Grocery List v1 cu Purchase Rules v1 demo.

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
- generare configurabila 1-5 zile pentru demo/debug
- `global_alternatives_3_day`
- hard no-repeat exact recipe, cu fallback daca este infezabil pentru 4/5 zile
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

Smoke Round54:
- `--days 1,2,3,4,5` functioneaza pe `v1_2_demo_final`
- `--days 5`: valid = 5/5, accept = 5/5, fallback la `main_only`, repeated recipes = 2, `multi_day_review`, `multi_day_loss=0.071414`
- `--days 6` este respins clar

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

## Grocery List v1 / Purchase Rules v1

Status: implementat ca feature determinist local/demo pentru Generator v1.

Rol:
- construieste o lista grocery draft din retetele selectate si `portion_multiplier`
- pastreaza gramele exacte in outputul detaliat
- afiseaza nume curate, categorii si grame rotunjite pentru demo
- adauga sugestii simple de cumparare prin `--grocery_purchase_suggestions` si checkbox-ul Streamlit `Show purchase suggestions`

Comportament:
- oua -> bucati
- ceapa / unele legume / fructe -> bucati aproximative
- paste / orez / oats / beans -> pachete sau bags simple
- lapte / yogurt -> carton sau tub
- carne / peste -> grame rotunjite grosier
- uleiuri / sare / condimente / unele sweeteners -> `check pantry`

Limitari:
- nu estimeaza preturi
- nu alege magazine, branduri sau produse
- nu face pantry inventory real
- nu face optimizare avansata de pachete
- nu converteste cooked-to-raw
- ramane demo/helper rules, nu grocery planner de productie

## Explicit out of scope

- Nu exista OR-Tools / MILP / CP-SAT.
- KNN nu este motor principal de selectie.
- Nu exista price, store, brand sau grocery optimization in Generator v1 demo.
- Nu exista weekly planning complet; 5 zile ramane demo/debug planning.
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
- Grocery List v1 si Purchase Rules v1 sunt demo-level; nu sunt grocery planner de productie.
- Household multi-member este ulterior.

## Urmatoarele checkpoint-uri posibile

A. Feedback explainability + UI polish.

B. Family-level variety polish.

C. Source verification batch3.

D. Grocery purchase-unit polish.

E. Household multi-member later.
