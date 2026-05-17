# Household Generation v1 Design

## Purpose

Household Generation v1 este directia de proiectare pentru generare la nivel de familie/gospodarie in TableTogether.

Scopul nu este sa produca meniuri complet separate pentru fiecare membru, ci sa porneasca de la mese shared:

- se selecteaza retete comune pentru household;
- fiecare membru primeste un `portion_multiplier` propriu;
- se calculeaza macro-uri per membru;
- grocery list se scaleaza la cantitatea totala household.

Acest document este design si audit de fezabilitate. Nu marcheaza implementarea unui generator household complet.

## Current Status

Generator v1 curent suporta:

- un singur `member_profile` activ;
- generare 1-5 zile pentru demo/debug;
- `v1_2_demo_final` si `v1_2_demo_final_time_layer`;
- grocery list, purchase suggestions, cooked-to-raw helpers si price estimates demo.

Round65 adauga:

- profil household demo: `profiles/household_profile_demo_v1.json`;
- audit targeturi per membru;
- audit portion allocation pentru mese shared;
- audit fezabilitate shared meals si grocery scaling.

Nu a fost schimbata selectia Generator v1.

## Non-Goals

Household Generation v1 nu include in aceasta etapa:

- meniuri complet separate per membru by default;
- OR-Tools, MILP, CP-SAT sau optimizator multi-objective avansat;
- schimbarea formulelor de target nutritional;
- grocery optimization, price optimization sau pantry inventory real;
- backend, conturi, mobile app sau API;
- generare de retete noi;
- promovarea dataseturilor draft in `current`.

## Household Profile Contract

Profilul demo este:

`profiles/household_profile_demo_v1.json`

Structura principala:

- `household_id`
- `household_name`
- `active_member_ids`
- `members`
- `household_preferences`
- `meal_config`
- `planning_config`

Fiecare membru pastreaza campurile compatibile cu `member_profile` Generator v1:

- `member_id`
- `display_name`
- `age`
- `sex`
- `weight_kg`
- `height_cm`
- `activity_level`
- `goal`
- `goal_speed`
- `training`
- `meal_config`
- `dietary_preferences`
- `bf_profile`

Profilul demo Round65 are 3 membri activi:

- Alex: adult male, `gain slow`, moderately active;
- Mara: adult female, `lose slow`, lightly active;
- Nina: lower target member, `maintain`, lightly active.

Profilele sunt intentionat non-extreme pentru primul audit household.

## Member Target Calculation

Targeturile sunt calculate cu `src/generator_v1/target_builder.py`, fara modificari de formule.

Rezultat audit Round65:

- Alex: `2862.5 kcal`, `147.6g protein`, `420.4g carbs`, `65.6g fat`;
- Mara: `1584.7 kcal`, `128.0g protein`, `153.0g carbs`, `51.2g fat`;
- Nina: `1709.8 kcal`, `83.2g protein`, `250.6g carbs`, `41.6g fat`.

Total household:

- `6157.0 kcal`;
- `358.8g protein`;
- `824.0g carbs`;
- `158.4g fat`.

Concluzie:

- diferentele de kcal cer portii diferite per membru;
- diferentele de protein/carbs cer probabil breakfast/snack individual sau top-up flexibil;
- nu este realist sa folosim portii egale ca default.

## Shared Meal Model

Modelul preferat:

1. Generatorul selecteaza retete comune pentru sloturile shared.
2. Household v1 aloca portii diferite pe membri.
3. Macro-urile sunt calculate per membru din aceeasi reteta.
4. Grocery quantities folosesc suma multiplicatorilor membrilor.

Exemplu:

Dinner: Chicken rice bowl

- Alex: `1.2x`
- Mara: `0.8x`
- Nina: `0.6x`

Household grocery factor pentru reteta = `1.2 + 0.8 + 0.6 = 2.6x`.

## Portion Allocation Model

Metode auditate in Round65:

- `equal_portions`
- `proportional_to_daily_kcal`
- `proportional_to_slot_kcal`
- `macro_aware_simple`

Bounds folosite:

- minim `0.4x`
- maxim `1.8x`

Rezultat allocation audit pe lunch+dinner shared:

- `equal_portions`: mean abs kcal deviation `54.3%`, protein `55.3%`;
- `proportional_to_daily_kcal`: mean abs kcal deviation `4.7%`, protein `29.3%`;
- `proportional_to_slot_kcal`: mean abs kcal deviation `4.7%`, protein `29.3%`;
- `macro_aware_simple`: mean abs kcal deviation `1.4%`, protein `28.6%`.

Recomandare tehnica:

- pentru prima implementare simpla: `proportional_to_slot_kcal`;
- pentru preview/audit: `macro_aware_simple` poate fi folosit ca benchmark euristic;
- orice varianta trebuie sa pastreze warning `household_portion_fit_review` cand proteina/carbs deviaza prea mult.

## Feasibility Audit

Dataset folosit:

- `v1_2_demo_final_time_layer`

Plan baseline:

- `days=3`
- `valid_day_count=3`
- `accept_day_count=3`
- `multi_day_loss=0.008484`

Scenarii auditate:

- `shared_dinner_only`
- `shared_lunch_dinner`
- `shared_all_except_snack`
- `individual_breakfast_shared_main`

Rezultat:

- `shared_dinner_only`: feasible true, mean abs kcal deviation `0.3%`, protein `9.8%`;
- `shared_lunch_dinner`: feasible true, mean abs kcal deviation `0.7%`, protein `11.5%`;
- `shared_all_except_snack`: feasible true, mean abs kcal deviation `0.8%`, protein `14.7%`;
- `individual_breakfast_shared_main`: feasible true, mean abs kcal deviation `0.7%`, protein `11.5%`.

Recomandarea Round65:

`individual_breakfast_shared_main`

Aceasta inseamna:

- breakfast ramane individual/flexibil;
- lunch si dinner pot fi shared;
- snack ramane individual/optional;
- per-member macro summaries sunt obligatorii;
- warnings raman vizibile pentru portii sau macro fit.

## Grocery Scaling

Pentru retetele shared:

`household_quantity = ingredient_quantity_per_recipe * sum(member_portion_multipliers)`

Round65 a masurat doar scaling cantitativ:

- nu include price optimization;
- nu include pantry;
- nu alege package/store/brand;
- nu schimba regulile Grocery List existente.

Max observed household quantity factor fata de portia single-profile in audit:

- `2.19x`

Acest lucru inseamna ca grocery list household este fezabila ca pas urmator, dar trebuie pastrata ca scaling explicit, nu ca optimizare de cumparaturi.

## Streamlit Preview Recommendation

Urmatorul round ar trebui sa adauge un panel de preview household, nu generator complet.

Panel recomandat:

- incarcare `profiles/household_profile_demo_v1.json`;
- afisare targeturi per membru;
- generare baseline shared plan pe primary member sau household aggregate prototype;
- tabel per meal cu reteta shared si portii per membru;
- per-member daily macro totals si deviation;
- grocery scaling factor per reteta shared;
- warnings pentru `household_portion_fit_review`.

Nu este recomandat sa ascundem debug-ul. Streamlit ramane dashboard de test.

## Round66 Household Preview Implementation Status

Status: implementat ca preview demo/audit, nu ca generator household-native.

Module si fisiere:

- `src/generator_v1/household_preview.py`
- `streamlit_app/generator_v1_dashboard.py`
- `profiles/household_profile_demo_v1.json`
- `tools/extra/evaluate_generator_v1_round66_household_preview.py`

Streamlit behavior:

- panel collapsed: `Household preview (draft)`;
- incarca profilul household demo;
- afiseaza targeturi per membru;
- foloseste ultimul plan generat in dashboard;
- buton: `Build household preview from latest generated plan`;
- afiseaza portii per membru pentru mesele shared;
- afiseaza totaluri macro per membru/zi;
- afiseaza factorul de grocery scaling;
- include output copy-ready si diagnostice.

Evaluator Round66:

- `data/recipesdb/audit/generator_v1_round66_household_preview_summary.txt`
- `data/recipesdb/audit/generator_v1_round66_household_preview_allocations.csv`
- `data/recipesdb/audit/generator_v1_round66_household_preview_member_macros.csv`
- `data/recipesdb/audit/generator_v1_round66_household_preview_grocery_scaling.csv`

Rezultat Round66:

- `member_count=3`
- `days_generated=3`
- `plan_accept_day_count=3`
- `allocation_mode=macro_aware_simple`
- `shared_meal_count=6`
- `min_portion_multiplier=0.6`
- `max_portion_multiplier=1.8`
- `max_grocery_scaling_factor=2.194`
- `preview_usable_for_demo=True`

Limitari Round66:

- nu exista household-native recipe selection;
- preview-ul nu optimizeaza toti membrii simultan;
- breakfast/snack sunt asumate individual/flexibil in macro totals;
- grocery scaling este cantitativ, nu grocery optimization;
- nu exista backend/mobile/account system.

Urmatorii pasi recomandati:

1. household-native candidate scoring/audit;
2. selector pentru shared lunch/dinner;
3. household grocery list pe baza portiilor alocate;
4. backend/mobile mai tarziu.

## Round68 Household Generation v1 Lite Implementation Status

Status: implementat ca prim strat functional household, demo/audit-level.

Module si fisiere:

- `src/generator_v1/household_generator.py`
- `src/generator_v1_cli.py`
- `streamlit_app/generator_v1_dashboard.py`
- `tools/extra/evaluate_generator_v1_round68_household_generation.py`

Diferenta fata de Household Preview:

- Household Preview porneste de la ultimul plan individual si aloca portii dupa selectie.
- Household Generation v1 Lite construieste targetul agregat al householdului, transforma candidatii pe portii simulate per membru si genereaza planul folosind acele targeturi household.

Comportament Round68:

- mod principal: `shared_all_slots`;
- allocation mode: `macro_aware_simple`;
- selecteaza retete shared;
- aloca portii diferite per membru;
- calculeaza totaluri macro per membru;
- calculeaza grocery scaling cantitativ.

Rezultat evaluator Round68 pe `v1_2_demo_final`:

- `member_count=3`;
- `days_generated=3`;
- baseline individual: `valid_days=3`, `accept_days=3`;
- household generation: `valid_days=3`, `household_quality_status=review`;
- `mean_abs_kcal_deviation_pct=0.8`;
- `mean_abs_protein_deviation_pct=19.9`;
- `min_portion_multiplier=0.7`;
- `max_portion_multiplier=1.8`;
- `clamped_portion_count=0`;
- `max_grocery_scaling_factor=3.85`;
- `household_generation_usable_for_demo=True`;
- planul household Lite difera de preview baseline.

Limitari Round68:

- nu este optimizer household global;
- `shared_all_slots` este prima varianta Lite, nu modul final recomandat pentru productie;
- nu genereaza meniuri separate per membru;
- proteina poate ramane in `review` pentru unii membri chiar daca kcal este aproape de target;
- grocery scaling este cantitativ, nu grocery optimization;
- nu exista backend/mobile/account household.

## Round69 Household Modes And Protein Correction Status

Status: implementat ca imbunatatire Lite, demo/audit-level.

Moduri suportate:

- `shared_all_slots`: baseline/debug Round68, toate sloturile sunt shared.
- `shared_main_meals`: lunch si dinner sunt shared, breakfast/snack sunt selectate individual per membru.
- `individual_breakfast_shared_main`: modul recomandat pentru demo; breakfast/snack individual, lunch/dinner shared.

Protein correction v1:

- dupa alocarea meselor shared, sistemul calculeaza gap-ul de proteina per membru;
- pentru breakfast/snack individual prefera candidati existenti cu densitate proteica mai buna si kcal rezonabil;
- nu foloseste suplimente, produse artificiale sau ingrediente noi;
- raporteaza `protein_gap_before`, `protein_gap_after`, `protein_correction_applied` si mesele de corectie selectate.

Rezultat evaluator Round69 pe `v1_2_demo_final`:

- `shared_all_slots`: `household_quality_status=review`, `min_protein_ratio=0.727`, worst member `Mara`.
- `shared_main_meals`: `household_quality_status=accept`, `accept_day_count=3`, `min_protein_ratio=0.938`.
- `individual_breakfast_shared_main`: `household_quality_status=accept`, `accept_day_count=3`, `min_protein_ratio=0.938`.
- pentru Mara, proteina s-a imbunatatit de la aproximativ `0.727` in baseline la `0.938` in modul recomandat.
- `max_grocery_scaling_factor=3.45` in modul recomandat.

Limitari Round69:

- `shared_main_meals` si `individual_breakfast_shared_main` folosesc aceeasi selectie individuala breakfast/snack in implementarea Lite curenta;
- nu exista inca optimizer household global;
- nu exista meniuri complet separate per membru;
- grocery scaling ramane cantitativ, fara price/grocery optimization.

## Limitations

- Auditul foloseste un plan baseline generat pentru primul membru, nu o selectie household-native.
- `macro_aware_simple` este euristic, nu optimizator global.
- Breakfast/snack individual sunt tratate ca target-fill in audit, nu generate separat per membru.
- Protein/carbs pot devia chiar daca kcal arata bine.
- Nu exista inca restrictii diferite per membru in selectia shared.
- Grocery scaling este doar cantitate, nu shopping optimization.
- Nu exista account/backend/mobile.

## Next Implementation Steps

1. Adauga un preview Streamlit household, fara sa schimbi generator selection logic.
2. Creeaza un contract intern pentru `household_plan`:
   - shared meals;
   - member portions;
   - member macro totals;
   - household grocery multiplier.
3. Incepe cu `individual_breakfast_shared_main`.
4. Pastreaza `shared_dinner_only` ca fallback pentru demo daca macro warnings devin prea multe.
5. Dupa preview stabil, decide daca selectia trebuie facuta pe primary member, aggregate target sau day-level household score.
