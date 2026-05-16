# Generator v1 demo guide

## Purpose

Acest ghid descrie rularea demo pentru Generator v1 folosind datasetul `v1_2_demo_final`.

Demo-ul arata:
- generare pentru 1 zi
- generare pentru 3 zile fara repetitii exacte
- filtrare demo-safe prin `profile_guard`
- integrarea datasetului Recipes_DB v1.2 demo-final draft
- Feedback v1 ca functie locala/demo

Demo-ul nu trebuie prezentat ca productie sau ca arhitectura finala.

## Dataset si config recomandat

Dataset:
- `dataset_profile=v1_2_demo_final`
- path: `data/recipesdb/draft/v1_2_demo_final/`
- total recipes: `266`
- active recipes: `261`
- sursa: `v1_2_demo_candidate_round48_cleaned`
- status: demo-final draft, nu productie/current

Config recomandat:
- `selection_mode=balanced_day`
- `portion_policy=target_aware`
- `meal_realism_mode=practical`
- `quality_gate=demo_safe`
- `days=3`
- `multi_day_mode=global_alternatives_3_day`
- `multi_day_no_repeat_policy=hard`
- `day_candidate_builder=direct_from_slots`
- `profile_guard=demo`

Datele `data/recipesdb/current` si `data/fooddb/current` raman neatinse.

## Streamlit

Pornire dashboard:

```powershell
streamlit run streamlit_app/generator_v1_dashboard.py
```

In dashboard:
- selecteaza `Recipes_DB v1.2 demo-final draft`
- verifica `Profile guard = demo`
- foloseste `Generate 1 day` pentru demo rapid
- foloseste `Generate 3 days` pentru demo multi-day

Pentru dataseturile v1.2 demo, Streamlit seteaza implicit `profile_guard=demo`.

## Feedback v1 demo

Pornire Streamlit:

```powershell
streamlit run streamlit_app/generator_v1_dashboard.py
```

Config recomandat pentru demonstratia de feedback:
- `dataset_profile=v1_2_demo_final`
- `selection_mode=balanced_day`
- `portion_policy=target_aware`
- `meal_realism_mode=practical`
- `quality_gate=demo_safe`
- `profile_guard=demo`

Flux recomandat:
- genereaza 1 zi
- apasa `Like` pe o reteta
- apasa `Dislike` pe alta reteta
- apasa `Too long` pe o reteta
- apasa `Avoid this recipe` pe o reteta
- genereaza din nou

Explicatie pentru demo:
- `Avoid this recipe` elimina reteta la generatiile urmatoare prin hard filter
- `Dislike` scade scorul prin `feedback_fit`
- `Like` creste `feedback_fit`
- `Too long` scade `time_fit` prin `time_feedback_penalty`
- feedback-ul se aplica la urmatoarea generatie, nu modifica meniul deja afisat

Storage local:
- `data/runtime/generator_v1_feedback_events.jsonl`

Reset pentru demo:
- foloseste `Clear feedback events` in panoul de feedback
- curata istoricul de meniuri generate din dashboard daca vrei o demonstratie pornita de la zero

Ce sa nu pretinzi despre Feedback v1:
- nu este ML
- nu este backend real de personalizare
- nu este sistem de conturi utilizator
- nu este recommendation engine de productie
- nu propaga inca feedback la nivel de ingrediente sau familii de retete

## CLI 1-day

```powershell
python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_2_demo_final --selection_mode balanced_day --portion_policy target_aware --meal_realism_mode practical --quality_gate demo_safe --days 1 --profile_guard demo
```

Output asteptat:
- dataset profile: `v1_2_demo_final`
- profile guard: `normal_demo_safe`
- plan de o zi valid/accept pentru profilul demo
- output files in `outputs/` daca nu este folosit `--no_write_outputs`

## CLI 3-day

```powershell
python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_2_demo_final --selection_mode balanced_day --portion_policy target_aware --meal_realism_mode practical --quality_gate demo_safe --days 3 --multi_day_mode global_alternatives_3_day --multi_day_no_repeat_policy hard --day_candidate_builder direct_from_slots --profile_guard demo
```

Output asteptat conform smoke Round49:
- valid days: `3/3`
- accept days: `3/3`
- repeated recipes: `0`
- `multi_day_loss=0.006322`
- output files in `outputs/generator_v1_multiday_*`

## Profile guard

`profile_guard` este un strat de protectie pentru demo. Nu modifica profilul si nu modifica formulele din `target_builder`.

Moduri:
- `off`: comportament fara guard
- `demo`: blocheaza profiluri unsupported pentru demo
- `permissive`: avertizeaza, dar continua

Reguli principale:
- `target_kcal < 1300` => `unsupported_for_demo`
- `target_kcal < 1400` si snack activ => `edge_needs_warning` sau mai sever
- `goal=lose`, `goal_speed=fast`, `activity_level=sedentary` => avertizare

Exemplu demonstrabil:
- `sedentary_lose_fast_with_snack`, `target_kcal=1227.8`, este blocat in `profile_guard=demo`
- in `profile_guard=permissive`, acelasi profil avertizeaza si continua

## Ce sa arati in prezentare

- Selectia datasetului `v1_2_demo_final`
- Generare 1 zi cu profilul demo
- Generare 3 zile cu no-repeat hard
- Statusurile de validare si quality gate
- `profile_guard` ca protectie pentru profiluri extreme
- Feedback v1: Like, Dislike, Too long, Avoid this recipe
- Faptul ca datasetul demo-final este draft/demo si nu modifica `current`

## Ce sa nu pretinzi

- Nu pretinde ca este productie.
- Nu pretinde ca exista household multi-member simultan.
- Nu pretinde ca exista grocery/price.
- Nu pretinde ca exista OR-Tools/KNN/MILP ca motor de selectie.
- Nu pretinde ca Feedback v1 este ML, backend real sau personalizare de productie.
- Nu pretinde ca family-level variety este complet rezolvata.
- Nu pretinde ca toate outlier risks sunt eliminate.

## Roadmap dupa demo

- feedback explainability + UI polish
- family-level variety polish
- Food_DB/source verification batch3
- grocery/list prep
- household multi-member ulterior
