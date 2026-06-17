# Mobile backend roadmap

## Purpose

Acest document fixeaza directia de produs pentru trecerea de la demo-ul Generator v1 la un MVP Android cu backend/API.

Scopul este sa clarifice arhitectura aleasa, responsabilitatile fiecarei componente, endpoint-urile minime si ordinea de implementare. Documentul este doar planificare. Nu adauga cod mobil, backend, schimbari in generator, schimbari in grocery/pricing sau modificari in `data/recipesdb/current` / `data/fooddb/current`.

## Current foundation

Generator v1 are acum o fundatie demo solida:

- dataset demo `v1_2_demo_final`
- generare configurabila 1-5 zile
- `profile_guard`
- Feedback v1 local/demo
- grocery list cu purchase suggestions, cooked-to-raw helpers si estimari demo de pret
- Household Preview si Household Generation v1 Lite
- dashboard Streamlit pentru debug/demo

Aceasta fundatie trebuie expusa treptat printr-un backend API, nu mutata in aplicatia mobila.

## Chosen architecture

Directia aleasa:

```text
Android Mobile App
  |
  | HTTP / JSON
  v
FastAPI Backend
  |
  v
Python Generator
  |
  v
Food_DB + Recipes_DB + SQLite
```

Principiul principal: backend/API first, apoi mobile UI.

Aplicatia mobila nu trebuie sa citeasca fisiere CSV, sa ruleze generatorul sau sa dubleze logica nutritionala. Mobilul consuma contracte JSON stabile expuse de backend.

## Technology stack

- Mobile: React Native + Expo, Android-first.
- Editor principal: VS Code.
- Android Studio: doar pentru Android SDK / emulator.
- Backend: FastAPI.
- Persistenta backend locala: SQLite.
- Generator: Python, pastrat in backend/generator.
- Date: Food_DB si Recipes_DB raman citite de backend/generator.

## Why backend/API comes before mobile UI

Backend-ul trebuie construit inaintea ecranelor mobile pentru ca:

- API-ul stabileste contractul real dintre UI si generator.
- Generatorul ramane Python si nu trebuie portat in React Native.
- Persistenta pentru household, profiluri, feedback, planuri si grocery lists apartine backend-ului.
- Mobile UI poate fi testat pe date reale, nu pe mock-uri care vor fi schimbate.
- Se evita duplicarea logicii de nutritie, scoring, grocery si feedback in aplicatia mobila.

Construirea multor ecrane pe mock data inainte de API ar crea risc mare de rescriere.

## Role separation

Mobile app:

- UI pentru Home / Dashboard.
- Household setup.
- Member profiles prin Add Member wizard in 3 pasi.
- Buton de generare plan.
- Afisare plan pe zile.
- Afisare detalii masa.
- Afisare grocery list.
- Butoane feedback: like, dislike, too long, avoid.
- Nu contine logica de selectie sau nutritie.

FastAPI backend:

- Primeste cereri HTTP/JSON.
- Valideaza request-urile.
- Persista household, profiluri, feedback, planuri generate si grocery lists in SQLite.
- Apeleaza generatorul Python printr-un service wrapper curat.
- Returneaza JSON stabil catre mobil.

Python Generator:

- Calculeaza targeturi.
- Filtreaza si scordeaza retete.
- Genereaza meniuri individuale si household Lite.
- Genereaza grocery list determinist.
- Pastreaza logica de profile guard, feedback fit, purchase suggestions si estimari de pret in stratul Python existent/backend.

SQLite:

- `households`
- `users`
- `user_sessions`
- `member_profiles`
- `feedback_events`
- `generated_plans`
- `generated_plan_days`
- `generated_plan_meals`
- `grocery_lists`
- `grocery_list_items`
- `saved_daily_progress`

## What mobile must not do

- Nu citeste CSV-uri.
- Nu ruleaza generatorul.
- Nu dubleaza logica nutritionala.
- Nu acceseaza direct Food_DB sau Recipes_DB.
- Nu implementeaza scoring de retete.
- Nu construieste grocery list din ingrediente brute.
- Nu calculeaza preturi sau sugestii de cumparare.

## PROFILE-WIZARD-1

Household Management foloseste un Add Member wizard in 3 pasi:
- Step 1: General Info
- Step 2: Food Preferences
- Step 3: Activity & Goal

Modelul de profil accepta `dietary_preferences.no_pork` si `food_preferences`.
`food_preferences.ratings` foloseste `like`, `dislike`, `avoid`, iar lipsa unei chei inseamna `Neutral`.
`Avoid` este hard filter; `Dislike` ramane preferinta soft si nu este tratat ca ban.
Custom avoided ingredients sunt salvate in `food_preferences.avoid_ingredients` si sunt mapate pe filtrul hard existent.
Soft scoring pentru `like`/`dislike` la nivel de aliment/familie este deferat pentru PROFILE-PREF-2.
DIET-HEALTH-PROFILES Phase 1 adauga in Step 2 compact chips pentru `Keto`, `Paleo` si `Mediterranean`.
Acestea sunt trimise ca `health_and_diet_preferences.dietary_patterns`; helper text-ul spune explicit ca nu sunt sfat medical.
DIET-HEALTH-PROFILES Phase 2 adauga `Diabetes-aware` sub Health-aware preferences; modul este o preferinta de prioritizare/scoring, nu recomandare medicala.
DIET-HEALTH-PROFILES Phase 3 adauga `Blood-pressure friendly`; backend key-ul este `hypertension_friendly`, iar scoringul foloseste proxy-uri pentru sare/ingrediente procesate.
DIET-HEALTH-PROFILES Phase 4 adauga `Heart-friendly`; backend key-ul este `heart_friendly`, iar scoringul penalizeaza soft retete foarte grase/procesate.

## Planned backend/API endpoints

Minimum MVP:

- `GET /health`
- `GET /households/demo`
- `GET /profiles`
- `POST /profiles`
- `DELETE /profiles/{member_profile_id}`
- `POST /plans/generate`
- `GET /plans/{plan_id}`
- `GET /plans/{plan_id}/grocery-list`
- `POST /feedback`
- `GET /feedback/context`
- `DELETE /feedback`
- `POST /progress/daily`
- `GET /progress/daily`
- `DELETE /progress/daily/{progress_id}`
- `POST /progress/daily/{progress_id}/delete` ca alias mobil pentru stergere

Household endpoints:

- `POST /household-plans/generate`
- `GET /household-plans/{plan_id}`
- `GET /household-plans/{plan_id}/grocery-list`

Endpoint-urile trebuie sa expuna contracte JSON simple si stabile. Detaliile interne ale generatorului, fisierele CSV si path-urile locale nu trebuie expuse catre mobile.

## SQLite MVP schema plan

Tabele minime:

- `households`
- `member_profiles`
- `feedback_events`
- `generated_plans`
- `generated_plan_days`
- `generated_plan_meals`
- `grocery_lists`
- `grocery_list_items`
- `saved_daily_progress`

Pentru MVP, `generated_plans` poate stoca initial raspunsul complet JSON al generatorului. Aceasta permite testarea rapida a fluxului backend/mobile fara normalizare prematura.

O schema mai normalizata poate fi adaugata ulterior, dupa ce contractele API si UI-ul real stabilizeaza campurile folosite frecvent.

## Milestone plan

M1 - Backend skeleton:

- folder `backend/`
- FastAPI app
- endpoint `GET /health`
- conexiune SQLite
- settings/config de baza

M2 - Generator service wrapper:

- backend-ul poate apela generatorul Python
- request JSON curat
- response JSON curat
- fara dependenta de Streamlit sau CLI

M3 - API endpoints:

- plan generation
- household plan generation
- grocery list retrieval
- feedback submission

M4 - Mobile skeleton (implemented):

- folder `mobile/`
- React Native + Expo app
- setup Android emulator
- apel `GET /health` din mobile

M5 - Mobile MVP screens (partially implemented through Mobile M2-M4):

- Home / Dashboard
- Household setup
- Member profiles
- Generate plan
- Daily plan screen
- Meal detail screen
- Grocery list screen
- Feedback buttons

M6 - Demo flow (individual demo flow implemented):

- profile -> generate -> plan -> grocery list -> feedback -> regenerate

## Current implementation status

Backend M1-M5 sunt implementate:

- M1: FastAPI skeleton, `GET /health`, SQLite init si configurare de baza.
- M2: Generator v1 service wrapper fara dependenta de Streamlit/CLI.
- M3: endpointuri de generare individuala/household, retrieval plan si grocery list, cu persistenta SQLite demo.
- M4: demo household, profile endpoints si feedback endpoints.
- M5: generatie persistence-aware cu profile SQLite si injectare de feedback context cand `feedback_enabled=true`.

Mobile M1-M8, UI-1/UI-2 si polish-urile principale de produs sunt implementate:

- M1: Expo Android skeleton si `GET /health`.
- M2: `GET /households/demo`, selectie membru demo, `POST /plans/generate` si afisare plan individual pe 3 zile.
- M3: request de generare cu grocery list, purchase suggestions si price estimates; afisare grocery list pe categorii.
- M4: butoane feedback per masa, `POST /feedback`, panel `GET /feedback/context`, regenerare cu `feedback_enabled=true`.
- M5: modul `Household plan` pentru membrii demo, cu selectie multipla, `POST /household-plans/generate`, per-member view, day selector si grocery list household agregata.
- M6: creare si listare profiluri salvate prin SQLite backend, plus generare individuala prin `member_profile_id`.
- M7: generatie household cu profiluri salvate; mobile selecteaza profiluri salvate multiple si apeleaza `POST /household-plans/generate` cu `selected_member_ids`.
- M8: cleanup local/demo pentru profiluri si feedback; mobile poate soft-dezactiva profiluri salvate prin `DELETE /profiles/{member_profile_id}?confirm=true` si poate sterge feedback events prin `DELETE /feedback?confirm=true`.
- UI-1: shell mobil cu 4 pagini (`Home`, `Meal Plan`, `Insights`, `Household / Account`), navigatie flotanta, guard states si flow-uri reale mobile/backend.
- PROGRESS-1: saved daily progress snapshots persistate per profil prin SQLite si `/progress/daily`; Page 3 poate salva/sterge snapshotul zilei curente.
- PROGRESS-2: Page 3 Trends foloseste istoricul salvat real pentru Target adherence, Consistency si Macro pattern pe range 1..30 zile disponibile.

Flow mobil validat:

```text
health -> load demo household -> select member -> generate plan -> grocery list -> feedback -> regenerate
```

Runtime M4 a confirmat ca feedback-ul `Avoid` salvat din mobile a fost aplicat la urmatoarea regenerare si reteta evitata nu a mai aparut cand au existat alternative.

Flow-ul M7 reutilizeaza afisarea household din M5: selector de membru, selector de zi, meniu per membru si grocery list agregata la nivel de household. Aplicatia mobila continua sa consume doar FastAPI prin HTTP/JSON si nu citeste CSV-uri sau ruleaza generatorul local.

Flow-ul M8 nu adauga auth/login sau cloud sync. Profilurile salvate sunt soft-dezactivate in SQLite local/demo, nu hard-deleted. Cleanup pentru planuri generate ramane neimplementat in M8 si poate fi adaugat doar ca endpoint dev/demo separat daca devine necesar.

UI-1 nu schimba backend/generator/grocery/pricing si nu modifica `data/recipesdb/current` sau `data/fooddb/current`. Meal Plan pastreaza fluxurile reale deja validate: generatie individuala, generatie household, grocery list, feedback, KNN alternatives si meal-level replacement explicit. Home foloseste continut hardcoded MVP, iar Insights a evoluat intr-un nutrition dashboard acceptat vizual, fara claims complete de micronutrienti.

Auth-M1 + UI-2A sunt implementate ca productization pass local:

- `POST /auth/register`, `POST /auth/login`, `POST /auth/logout` si `GET /auth/me`.
- Conturile sunt locale SQLite, fara Firebase/Supabase, fara cloud auth, fara email verification si fara password reset.
- Parolele sunt stocate ca PBKDF2-HMAC-SHA256 hash + salt; plaintext password nu se stocheaza.
- Sesiunile returneaza token brut clientului, dar SQLite stocheaza doar hash-ul tokenului.
- Fiecare cont primeste un household implicit.
- Profilele create cu `Authorization: Bearer <session_token>` sunt salvate automat sub household-ul contului.
- Mobile pastreaza sesiunea in React state pentru acest MVP; persistenta peste restart de app ramane limitare/future work deoarece nu exista inca AsyncStorage.
- Page 4 este reorganizata ca hub: Account Settings, Household Management si App Settings; Developer Diagnostics este tinut separat.
- Page 4 final polish pastreaza Change Email / Change Password ca actiuni vizibile cu info sheet, fara endpoint functional de schimbare email/parola.
- Household Management permite setarea numelui household-ului, foloseste carduri de membri cu goal badge compact si pastreaza selectorul de default viewer `Who's using this device?`.
- App Settings expune Feedback context si Saved data in afara Developer Diagnostics, iar diagnostics ramane pentru runtime/backend tooling.
- `Create Account` si `Log In` sunt flow-uri reale locale; `Log Out` revoca sesiunea cand exista token si curata state-ul local.
- `Household ID` nu mai este expus in formularul Add Profile.
- Textele principale de demo/test/MVP au fost scoase din flow-ul user-facing; fallback-ul optional este denumit `Try Sample Household`.
- Safe area/status bar spacing este reparat global in mobile shell, iar empty/guard states sunt centrate.
- Meal Plan are titlu centrat, selector de profil cu sageti, selector compact de zile si control 1-5 zile fara dependency noua.
- DATA-QA-1 a rezolvat acoperirea app-facing pentru missing price si missing cooking time in fluxurile generate. UI-ul pastreaza fallback-uri curate pentru cazuri neasteptate sau date viitoare incomplete.
- COOKING-STEPS-1 a validat acoperirea cooking steps pentru toate retetele active/current si backend-ul expune `cooking_steps` in meal rows pentru mobile. Pasii sunt instructiuni MVP practice, nu text extern scraped/copiat.

UI-2B este implementat ca productization pass family-first:

- Meal Plan expune un singur buton user-facing: `Generate meal plan`.
- Mobile alege intern endpoint-ul individual cand exista un singur profil activ si endpoint-ul household cand exista doua sau mai multe profiluri active.
- Sample/Demo household nu mai apare in flow-ul principal de utilizator.
- Meal Plan nu mai afiseaza metadata tehnica dupa generare precum `plan_id`, status backend/generator, quality sau accept/review/reject summaries.
- Mesele sunt afisate vizual in ordinea Breakfast, Lunch, Snack, Dinner fara a schimba semantica backend-ului.
- Selectorul de zi afiseaza conceptual Day 1-5 si dezactiveaza/grieste zilele negenerate.
- Target summary citeste fallback-uri pentru `target_protein_g`, `target_carbs_g` si `target_fat_g`, fara schimbari in formulele backend.
- Add Profile ramane pe Household Management dupa salvare si afiseaza CTA catre Meal Plan.
- Validarile profilului acopera nume fara cifre, age 4-120, weight 15-300 kg, height 80-230 cm, sessions/week 0-7, meals/day 1-5 si Goal Speed inactiv pentru Maintain.
- Auth UI mapeaza login 401 la `Invalid email or password`.
- Change Email si Change Password sunt butoane vizibile cu info sheet, dar fara endpoint functional; Language si Appearance raman read-only.
- Tema mobila foloseste white/off-white + accent pear green `#74B72E`.
- Investigarea keep-awake nu a gasit cod de aplicatie care sa tina ecranul treaz; `expo-keep-awake` apare doar tranzitiv in Expo package lock. Comportamentul ramane cel mai probabil Expo Go/dev mode sau OS/device.
- DATA-QA-1 este completat pentru price/time coverage. Rafinarea culinara a cooking steps ramane viitoare daca devine necesara.
- COOKING-STEPS-1 valideaza cooking steps pentru `106/106` retete active/current si pastreaza fallback-ul mobil doar pentru date viitoare neasteptat incomplete.

UI-2C este implementat ca polish product-facing peste Meal Plan:

- Headerul Meal Plan este redus la titlul centrat `Meal Plan`; profilul activ nu mai este duplicat sus.
- Generation card afiseaza copy scurt de produs, un control slider-like 1-5 zile si un singur buton `Generate meal plan`.
- Selectorul de membru/profil de vizualizat apare o singura data, sub taburile interne `Meal Plan` / `Grocery List`.
- Selectorul de zile generate pastreaza Day 1-Day 5 pe un singur rand, cu zilele negenerate disabled/gri.
- Strip-ul macro zilnic de sub Day 1-Day 5 a fost eliminat, astfel incat lista de mese incepe direct dupa selectorul de zi.
- Padding-ul global de jos din `AppScreen` a fost marit ca sa nu ascunda ultimul continut sub floating nav.
- UI-2C nu adauga dependency noua, nu schimba endpointuri si nu modifica generator/backend generation logic.

HOME-1 este implementat ca Page 1 / Home discovery:

- Home porneste ca prima pagina a shell-ului mobil si ramane in acelasi bottom-nav existent.
- Home foloseste layout warm/family-oriented: hero, household CTA, Daily Food Tip, carusele de resurse si subpagini interne See all.
- Continutul este hardcoded si centralizat in `mobile/src/data/homeContent.ts`.
- Home nu face call-uri backend, nu modifica planuri generate si nu afecteaza generator/grocery/pricing/KNN.
- Hero-ul Home foloseste `mobile/assets/home/welcome/cooking_lottie.json`.
- Daily Food Tip foloseste cele 4 ilustratii PNG locale din `mobile/assets/home/tips/`.
- Linkurile externe din carusele raman continut static/hardcoded si pot fi rafinate ulterior.
- Structura este verificata cu `tools/extra/check_mobile_home_page_structure.py`.

Page 2 / Meal Plan si Grocery List au primit polish product-facing:

- Generate card compact cu slider discret 1-5 zile.
- Meal cards cu `Cook / Steps`, `Alternatives` si control `Rate meal`.
- Meal Plan nu mai afiseaza bara separata de macro-uri zilnice sub selectorul Day 1-Day 5.
- Alternatives pastreaza backend-ul neschimbat, dar pregateste in mobile preview-urile `dry_run=true` secvential si cache-uit; cardurile nu afiseaza macro-uri ajustate pana cand exista `alternative_meal` real pentru contextul profilului.
- Grocery List are categorii restranse implicit, icon holders/category PNGs, checkbox row tappable si formatting compact pentru Need/Buy.
- Estimated Total foloseste emblema grocery locala si include actiunile placeholder `Share` / `Copy` in acelasi card.

Page 3 / Insights este acceptat ca nutrition dashboard:

- selector profil si Day 1-Day 5 + Average;
- Daily Balance cu macro donut custom si flame icon;
- Meal contribution separat ca mini-carduri tappable;
- eaten/not-eaten state local care actualizeaza Daily Balance si Macro Targets;
- `Save day`, `Saved` si `Delete saved day` pentru snapshotul zilei curente;
- Macro Targets dinamice si Micronutrients placeholder scurt.

PROGRESS-1 persista snapshoturi zilnice salvate per profil:

- tabela SQLite `saved_daily_progress`;
- API `POST /progress/daily`, `GET /progress/daily`, `DELETE /progress/daily/{progress_id}` si alias mobil `POST /progress/daily/{progress_id}/delete`;
- maximum 30 snapshoturi per profil;
- unicitate pe profil + plan + zi;
- scoping pe cont/household prin `Authorization`.

PROGRESS-2 adauga:

- campuri `target_*` in `saved_daily_progress`, cu backfill planned-as-target pentru randurile istorice;
- `GET /progress/daily` cu `limit` optional 1..30;
- sectiune `Trends` in Page 3 sub Macro Targets si peste Micronutrients;
- range selector `Last X days`, default la maximum disponibil pentru profil;
- Target adherence cu metric selector Calories / Protein / Carbs / Fats;
- Consistency bazat pe calories si protein in target sau close;
- Macro pattern 4-row heatmap, maximum 5 zile vizibile per bloc;
- Top foods / Top contributors raman deferate.

## Remaining mobile milestones

Later - UI polish after UI-1:

- polish vizual incremental pe zone ramase neacceptate explicit
- iconuri finale in locul placeholderelor ASCII din navigatia flotanta, daca mai devine prioritar
- rafinare thumbnail-uri/linkuri Home pentru carusele
- stari loading/error mai polishate
- pregatire demo MVP mai apropiata de produs

Later - Product/account work:

- persistenta sigura a sesiunii peste restart de app
- change email / change password daca devin necesare pentru prezentare
- rafinare vizuala pentru Trends dupa testare reala pe telefon
- cloud sync doar dupa ce fluxul local/demo este stabil
- ecrane household dedicate si feedback household imbunatatit

## Non-goals for now

- Fara iOS.
- Fara Play Store publishing.
- Fara login cloud/complex.
- Fara payments.
- Fara cloud deployment complexity.
- Fara Firebase/Supabase daca nu exista o justificare ulterioara clara.
- Fara generator in mobile.
- Fara CSV reads din mobile.
- Fara live price scraping.
- Fara advanced household optimizer.
- Fara full household mobile screen de productie inca.
- Fara Top foods / Top contributors in PROGRESS-2.

## Implementation guardrails

- Nu modifica `data/recipesdb/current`.
- Nu modifica `data/fooddb/current`.
- Nu muta logica in mobile.
- Nu introduce backend/mobile inainte de scheletul API minim.
- Nu schimba generator logic ca parte din roadmap documentation.
- Nu schimba grocery/pricing logic ca parte din roadmap documentation.
