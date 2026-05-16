# Generator roadmap

Premisa de baza:
- un cont principal
- un household
- mai multe member profiles
- contul este folosit comun de membrii casei
- generatorul v1 foloseste un singur `member_profile` activ
- household optimization vine mai tarziu

Model conceptual minim:
- `account`
- `household`
- `member_profile`
- `plan`
- `feedback_event`

Checkpoint-uri:
- Checkpoint 0: contract generator, profil, `nutrition_target`, feedback, hard filters, scoring - conceptual complet
- Checkpoint 1: 1 household + 1 `member_profile` activ + 1 zi - demo/testing-ready pe Generator v1
- Checkpoint 2: multi-day v1 draft + demo polish - implementat/demo-ready pentru 3 zile
- Checkpoint 3: Feedback v1 local/demo implementat; raman feedback explainability, UI polish si family-level variety polish
- Checkpoint 4: pregatire grocery/list si source verification suplimentar
- Checkpoint 5: household generation cu mai multe profile active simultan

Rolul KNN:
- nu este motor principal in v1
- ramane strat auxiliar pentru substitutii, retete similare, candidate expansion si propagare feedback

Rolul Streamlit:
- unealta de testare rapida
- nu produs final

Stare demo curenta:
- dataset demo: `v1_2_demo_final`
- path: `data/recipesdb/draft/v1_2_demo_final/`
- total recipes: `266`
- active recipes: `261`
- sursa: `v1_2_demo_candidate_round48_cleaned`
- status: demo-final draft, nu productie/current
- Feedback v1 local/demo este implementat cu storage JSONL in `data/runtime/generator_v1_feedback_events.jsonl`
- `data/recipesdb/current` ramane neatins
- `data/fooddb/current` ramane neatins

Config demo recomandat:
- `dataset_profile=v1_2_demo_final`
- `selection_mode=balanced_day`
- `portion_policy=target_aware`
- `meal_realism_mode=practical`
- `quality_gate=demo_safe`
- `days=3`
- `multi_day_mode=global_alternatives_3_day`
- `multi_day_no_repeat_policy=hard`
- `day_candidate_builder=direct_from_slots`
- `profile_guard=demo`

Explicit in afara scope-ului demo curent:
- fara OR-Tools / KNN ca motor principal / MILP
- fara grocery/price
- fara household multi-member simultan
- Feedback v1 nu este backend de productie, ML sau sistem de conturi
- `profile_guard` este strat de protectie demo, nu schimbare de formula target
