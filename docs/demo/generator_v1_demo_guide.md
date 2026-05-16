# Generator v1 demo guide

## Purpose

Acest ghid descrie rularea demo pentru Generator v1 folosind datasetul `v1_2_demo_final`.

Demo-ul arata:
- generare pentru 1 zi
- generare multi-day configurabila 1-5 zile pentru demo/debug
- filtrare demo-safe prin `profile_guard`
- integrarea datasetului Recipes_DB v1.2 demo-final draft
- Feedback v1 ca functie locala/demo

Demo-ul nu trebuie prezentat ca productie, weekly planning complet sau arhitectura finala.

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

Nota Round54:
- `--days` suporta valori `1..5`
- pentru 4/5 zile, `multi_day_no_repeat_policy=hard` poate face fallback la `main_only` sau `prefer` daca no-repeat exact nu este fezabil
- 5 zile ramane demo/debug planning, nu weekly planning complet
- price/store logic si grocery optimization raman in afara scopului

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
- foloseste `Generate 3 days` pentru quick action multi-day
- pentru `1..5` zile configurabile foloseste CLI-ul Round54
- dupa generare, foloseste expanderul `Grocery list draft` pentru lista de cumparaturi draft si sugestii de achizitie

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

## CLI multi-day 1-5 zile

Exemplu recomandat pentru 3 zile:

```powershell
python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_2_demo_final --selection_mode balanced_day --portion_policy target_aware --meal_realism_mode practical --quality_gate demo_safe --days 3 --multi_day_mode global_alternatives_3_day --multi_day_no_repeat_policy hard --day_candidate_builder direct_from_slots --profile_guard demo
```

Output asteptat conform smoke Round54 pentru `--days 3`:
- valid days: `3/3`
- accept days: `3/3`
- repeated recipes: `0`
- `multi_day_loss=0.006322`
- output files in `outputs/generator_v1_multiday_*`

Pentru `--days 5`, outputul validat Round54 este `valid=5/5`, `accept=5/5`, fallback la `main_only`, `repeated=2`, `multi_day_review`, `multi_day_loss=0.071414`.

## Grocery List / Purchase Suggestions

Generator v1 poate construi o lista de grocery draft din meniul generat. Lista foloseste retetele selectate, ingredientele lor si `portion_multiplier`, apoi grupeaza cantitatile in grame. Round57 adauga reguli demo pentru sugestii simple de cumparare peste lista curatata.

CLI basic grocery list:

```powershell
python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_2_demo_final --selection_mode balanced_day --portion_policy target_aware --meal_realism_mode practical --quality_gate demo_safe --profile_guard demo --days 3 --multi_day_mode global_alternatives_3_day --multi_day_no_repeat_policy hard --day_candidate_builder direct_from_slots --write_grocery_list
```

CLI grocery list cu purchase suggestions:

```powershell
python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_2_demo_final --selection_mode balanced_day --portion_policy target_aware --meal_realism_mode practical --quality_gate demo_safe --profile_guard demo --days 3 --multi_day_mode global_alternatives_3_day --multi_day_no_repeat_policy hard --day_candidate_builder direct_from_slots --write_grocery_list --grocery_purchase_suggestions
```

CLI grocery list cu purchase suggestions si estimari cooked-to-raw:

```powershell
python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_2_demo_final --selection_mode balanced_day --portion_policy target_aware --meal_realism_mode practical --quality_gate demo_safe --profile_guard demo --days 3 --multi_day_mode global_alternatives_3_day --multi_day_no_repeat_policy hard --day_candidate_builder direct_from_slots --write_grocery_list --grocery_purchase_suggestions --grocery_cooked_to_raw
```

Output:
- `outputs/generator_v1_grocery_list.csv`
- `outputs/generator_v1_grocery_list.txt`

Regulile implicite sunt in `data/grocery/reference/grocery_purchase_rules_v1.csv`; pentru teste punctuale pot fi suprascrise cu `--grocery_purchase_rules_path`.
Regulile cooked-to-raw sunt in `data/grocery/reference/grocery_cooked_to_raw_rules_v1.csv`; pentru teste punctuale pot fi suprascrise cu `--grocery_cooked_to_raw_rules_path`.

Exemple de sugestii:
- `Eggs: need ~264g; buy 6 eggs`
- `Onions: need ~522g; buy 6 medium onions / about 600g`
- `Pasta (dry): need ~846g; buy 2 x 500g packs`
- `Rice (raw): need ~444g; buy 1 x 1kg bag`
- `Greek yogurt: need ~270g; buy 1 x 500g tub`
- `Olive oil: check pantry; need about 89.6g`
- `Rice (cooked): need ~228g cooked; buy about 80g raw rice`

In Streamlit, dupa generarea unui meniu, deschide `Grocery list draft`. Checkbox-ul `Show purchase suggestions` afiseaza coloana de sugestie de cumparare si actualizeaza textul copy-friendly. Checkbox-ul `Convert cooked rice/pasta/beans to raw purchase estimate` afiseaza echivalentul raw/dry aproximativ pentru cazurile acoperite.

Nota cooked-to-raw:
- acopera conservator cooked rice, cooked pasta si cooked beans/lentils/chickpeas
- nu modifica nutrition calculation sau gramele exacte din detail/CSV
- adauga warnings de tip `cooked_to_raw_estimate`
- cooked/boiled vegetables raman cu warning, nu se convertesc automat

Limitari:
- nu exista preturi
- nu exista selectie de magazin, brand sau produs
- nu exista pantry inventory real
- nu exista optimizare avansata de pachete
- cooked-to-raw este demo-level si acopera doar cazurile explicite de mai sus
- sugestiile sunt reguli aproximative pentru demo/readability

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
- Generare multi-day 1-5 zile; pentru demo rapid, 3 zile cu no-repeat hard
- Statusurile de validare si quality gate
- `profile_guard` ca protectie pentru profiluri extreme
- Feedback v1: Like, Dislike, Too long, Avoid this recipe
- Grocery list draft si purchase suggestions v1 ca helper demo determinist
- Faptul ca datasetul demo-final este draft/demo si nu modifica `current`

## Ce sa nu pretinzi

- Nu pretinde ca este productie.
- Nu pretinde ca exista household multi-member simultan.
- Nu pretinde ca exista preturi, magazine, branduri sau grocery optimization.
- Nu pretinde ca purchase suggestions sunt o lista realista finala cu inventar/pachete optimizate.
- Nu pretinde ca 5 zile este weekly planning complet.
- Nu pretinde ca exista OR-Tools/KNN/MILP ca motor de selectie.
- Nu pretinde ca Feedback v1 este ML, backend real sau personalizare de productie.
- Nu pretinde ca family-level variety este complet rezolvata.
- Nu pretinde ca toate outlier risks sunt eliminate.

## Roadmap dupa demo

- feedback explainability + UI polish
- family-level variety polish
- Food_DB/source verification batch3
- grocery purchase-unit polish
- household multi-member ulterior
