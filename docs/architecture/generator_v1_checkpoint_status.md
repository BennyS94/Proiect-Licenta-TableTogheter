# Generator v1 checkpoint status

## Snapshot

Starea curenta: Generator v1 este demo/testing-ready pentru un profil activ, cu demo multi-day draft configurabil 1-5 zile pe datasetul `v1_2_demo_final`, Feedback v1 local/demo, Grocery List v1 cu Purchase Rules v1 demo si Household Preview v1 draft.

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

Nota backend/API:
- Generator v1 are acum o fundatie demo suficient de clara pentru a fi expusa printr-un backend API intr-un checkpoint viitor.
- Urmatorul pas de produs trebuie sa fie un wrapper backend/API peste generator, nu mutarea logicii in aplicatia mobila.

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
- include helper-e cooked-to-raw aproximative pentru purchase display: cooked rice -> raw rice, cooked pasta -> dry pasta, cooked beans/lentils/chickpeas -> dry legumes
- helper-ele cooked-to-raw nu modifica recipe nutrition calculations
- cooked vegetables raman warning-only si nu sunt convertite automat

Limitari:
- nu estimeaza preturi
- nu alege magazine, branduri sau produse
- nu face pantry inventory real
- nu face optimizare avansata de pachete
- helper-ele cooked-to-raw sunt estimari demo pentru purchase display, nu conversii nutritionale
- ramane demo/helper rules, nu grocery planner de productie si nu price/store/brand logic

## Household Preview v1

Status: implementat ca feature local/demo-audit pentru Streamlit.

Rol:
- incarca `profiles/household_profile_demo_v1.json`
- afiseaza targeturile nutritionale pentru membrii household demo
- foloseste ultimul plan generat in dashboard
- aloca portii per membru pentru mesele shared
- afiseaza totaluri macro per membru/zi
- afiseaza factorul de grocery scaling pentru mesele shared
- Household Grocery realism guard este implementat: penalizare/warning pentru oua directe excesive, fara ban global pe oua.
- ofera output copy-ready si diagnostice

Comportament curent:
- allocation mode: `macro_aware_simple`
- conceptul recomandat ramane `individual_breakfast_shared_main`
- Round66 smoke: `member_count=3`, `days_generated=3`, `plan_accept_day_count=3`, `shared_meal_count=6`, `max_grocery_scaling_factor=2.194`, `preview_usable_for_demo=True`

Limitari:
- nu este household-native selection
- nu selecteaza retete optimizand toti membrii simultan
- breakfast/snack sunt tratate ca asumptii individuale/flexibile in preview
- grocery scaling este cantitativ, nu grocery optimization
- household-native selector ramane lucru viitor

## Explicit out of scope

- Nu exista OR-Tools / MILP / CP-SAT.
- KNN nu este motor principal de selectie.
- Nu exista price, store, brand sau grocery optimization in Generator v1 demo.
- Nu exista weekly planning complet; 5 zile ramane demo/debug planning.
- Nu exista household-native multi-member selection.
- Household Preview v1 exista doar ca preview peste un plan deja generat.
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
- Household Preview v1 este disponibil in Streamlit, dar household-native generation ramane ulterior.

## Urmatoarele checkpoint-uri posibile

A. Feedback explainability + UI polish.

B. Family-level variety polish.

C. Source verification batch3.

D. Grocery purchase-unit polish.

E. Household-native candidate scoring/audit.
