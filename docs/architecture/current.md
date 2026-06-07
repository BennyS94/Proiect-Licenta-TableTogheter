# Current architecture

## 1. Scope and current status

TableTogether este in prezent un proiect-concept de licenta pentru planificarea meniurilor pe mai multe zile, orientat spre gospodarie / familie. Sistemul actual urmareste generarea unui plan alimentar pe baza preferintelor, obiectivelor si restrictiilor utilizatorilor, folosind o abordare in principal rule-based, sustinuta de scoring si de un strat de feedback de baza.

Implementarea existenta este functionala la nivel de pipeline, dar arhitectura nu este considerata finala. Proiectul se afla intr-o faza de tranzitie, iar urmatoarea prioritate majora este reorganizarea stratului de date si clarificarea separarii dintre alimente canonice si entitati compuse.

## 2. Current functional pipeline

In forma actuala, sistemul este organizat in jurul urmatoarelor etape principale:

- enrich pentru alimente si atasare de buckets, micro-buckets si tag-uri utile
- construire item index pentru similaritate si substitutions
- generare de substitutions item-based
- generator principal rule/scoring-driven pentru selectia planului
- ajustari zilnice de portii prin daily rules
- feedback de baza agregat in preferinte simple
- generare de output operational, inclusiv plan si grocery list

Acest pipeline permite obtinerea unui plan functional si ofera deja o baza practica pentru experimentare si iteratie.

Nota operationala: codul generatorului vechi este izolat pentru referinta in `src/legacy/`. Lucrul activ pentru Generator v1 este separat in `src/generator_v1/` si `src/generator_v1_cli.py`.

## 2.1. Generator v1 demo status

Generator v1 este acum demo/testing-ready pentru un profil activ si pentru un draft multi-day configurabil 1-5 zile.

Datasetul recomandat pentru demo este:
- `dataset_profile=v1_2_demo_final`
- path: `data/recipesdb/draft/v1_2_demo_final/`
- total recipes: `266`
- active recipes: `261`
- sursa: `v1_2_demo_candidate_round48_cleaned`
- status: demo-final draft, nu productie/current

Config demo recomandat:
- `selection_mode=balanced_day`
- `portion_policy=target_aware`
- `meal_realism_mode=practical`
- `quality_gate=demo_safe`
- `days=3`
- `multi_day_mode=global_alternatives_3_day`
- `multi_day_no_repeat_policy=hard`
- `day_candidate_builder=direct_from_slots`
- `profile_guard=demo`

Smoke-ul curent pentru acest pachet:
- one-day valid/accept = true
- three-day valid = 3/3
- accept = 3/3
- repeated recipes = 0
- `multi_day_loss=0.006322`

`profile_guard` este un strat de protectie pentru demo. Profilul edge `sedentary_lose_fast_with_snack` cu `target_kcal=1227.8` este blocat in modul `demo`; modul `permissive` avertizeaza si continua. Guard-ul nu schimba formulele din `target_builder`.

Limitari explicite ale demo-ului curent:
- fara OR-Tools / KNN ca motor principal / MILP
- Grocery List v1 si Purchase Rules v1 exista ca feature determinist demo/helper
- Household Preview v1 exista in Streamlit ca preview demo/audit peste ultimul plan generat
- fara preturi live, store/brand optimization, pantry inventory real sau grocery optimization
- fara household-native multi-member selection
- family-level variety este inca imperfecta
- unele outlier risks raman cu warnings
- Feedback v1 este local/demo, nu productie
- `data/recipesdb/current` ramane neatins
- `data/fooddb/current` ramane neatins

Nota DATA-QA-1:
- DATA-QA-1 este completat pentru price/time coverage in fluxurile app-facing generate.
- Grocery prices folosesc un strat determinist: catalog exact, alias catalog, category fallback si emergency fallback.
- Cooking time foloseste campurile directe/time-layer si fallback-uri controlate pentru estimari utilizabile.
- Rezultatul verificat: `0` preturi lipsa in outputurile grocery app-facing si `0` cooking-time estimates lipsa pentru retetele active/displayable din `v1_2_demo_final`.
- Limitarea ramasa: multe preturi sunt estimari demo controlate, nu preturi live sau source-backed exact pentru fiecare item.

Nota KNN-lite: exista un modul auxiliar `src/generator_v1/recipe_similarity.py` pentru retete similare, documentat in `docs/architecture/knn_substitution_v1_design.md`. KNN-backed alternatives suporta acum meal-level replacement cu preview si confirmare explicita prin backend/mobile MVP. KNN poate propune candidati, dar generatorul principal ramane deterministic, scoring/constraint-based si validator/approver; nu exista substitutii automate sau substitutii de ingrediente in MVP.

## 2.2. Feedback v1 local/demo

Generator v1 are Feedback v1 implementat ca functie locala pentru demo si testare.

Storage:
- `data/runtime/generator_v1_feedback_events.jsonl`

Tipuri suportate:
- `liked`
- `disliked`
- `too_long`
- `explicit_avoid`

Aplicare:
- `explicit_avoid` este hard filter pe `recipe_id`
- `liked` creste scorul prin `feedback_fit`
- `disliked` scade scorul prin `feedback_fit`
- `too_long` aplica penalizare de timp si reduce `time_fit`

Limitari:
- JSONL local only
- fara conturi reale
- fara DB/backend/server
- fara ML/KNN
- fara personalizare de productie
- fara propagare la nivel de ingrediente
- context household/demo only

## 2.3. Next product direction: Android mobile MVP via FastAPI backend

Directia urmatoare de produs este un MVP Android care consuma un backend FastAPI prin HTTP/JSON. Generatorul ramane Python in backend, iar aplicatia mobila nu citeste CSV-uri, nu ruleaza generatorul si nu acceseaza direct Food_DB sau Recipes_DB.

Roadmap-ul pentru aceasta directie este documentat in `docs/architecture/mobile_backend_roadmap.md`.

## 3. Current data model reality

Modelul actual este construit peste un dataset nutritional prelucrat, imbogatit cu clasificari suplimentare si semnale utile pentru generare. In aceasta forma, baza de date curenta este suficienta pentru rularea pipeline-ului existent, dar nu separa inca suficient de clar:

- alimente canonice / atomice
- preparate sau entitati compuse
- nivelul de ingredient
- nivelul de reteta

Aceasta lipsa de separare face ca unele componente sa devina mai greu de extins elegant, mai ales in perspectiva introducerii unui model mai clar de recipes, feedback mai expresiv si ML ulterior.

## 4. Current strengths

Arhitectura actuala are cateva puncte forte importante:

- exista deja un pipeline cap-coada functional
- exista un nucleu de scoring si selectie care poate produce rezultate utilizabile
- exista output-uri practice pentru plan si grocery
- exista o baza initiala pentru substitutions si feedback
- exista deja documentatie tehnica si structura modulara suficient de buna pentru refactorizare incrementala

Aceste lucruri reprezinta o baza buna pentru urmatoarea etapa de dezvoltare.

## 5. Current limitations

Forma actuala a sistemului are si limitari importante:

- modelul de date nu este inca suficient de curat pentru a sustine natural separarea Food_DB / Recipes_DB
- logica de generare a acumulat datorie tehnica si euristici distribuite in mai multe module
- unele reguli, fallback-uri si conventii de coloane trebuie canonizate mai clar
- unele documente mai vechi descriu o directie intermediara si nu trebuie tratate ca sursa finala de adevar
- structura actuala este suficienta pentru experimentare, dar nu este forma dorita pe termen mediu

## 6. Immediate next priority

Prioritatea imediata a proiectului nu este extinderea directa a componentei de ML, ci reorganizarea stratului de date si a modelului de lucru. Inainte de KNN mai avansat, feedback mai bogat sau ranking supervizat, este necesara clarificarea unei fundatii mai curate pentru:

- alimente canonice
- retete
- relatia dintre retete si ingrediente
- semnale de timp, cost si feedback

Aceasta etapa este considerata preconditie pentru dezvoltarea urmatoarelor componente ale sistemului.

## 7. Transitional note

Acest document descrie starea curenta a proiectului si limitele ei. El nu trebuie interpretat ca descrierea arhitecturii tinta. Directia de evolutie planificata va fi descrisa separat in documentul `docs/architecture/restructure_target.md`.
