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
- Member profiles.
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
- `member_profiles`
- `feedback_events`
- `generated_plans`
- `generated_plan_days`
- `generated_plan_meals`
- `grocery_lists`
- `grocery_list_items`

## What mobile must not do

- Nu citeste CSV-uri.
- Nu ruleaza generatorul.
- Nu dubleaza logica nutritionala.
- Nu acceseaza direct Food_DB sau Recipes_DB.
- Nu implementeaza scoring de retete.
- Nu construieste grocery list din ingrediente brute.
- Nu calculeaza preturi sau sugestii de cumparare.

## Planned backend/API endpoints

Minimum MVP:

- `GET /health`
- `GET /households/demo`
- `GET /profiles`
- `POST /profiles`
- `POST /plans/generate`
- `GET /plans/{plan_id}`
- `GET /plans/{plan_id}/grocery-list`
- `POST /feedback`
- `GET /feedback/context`
- `DELETE /feedback`

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

M4 - Mobile skeleton:

- folder `mobile/`
- React Native + Expo app
- setup Android emulator
- apel `GET /health` din mobile

M5 - Mobile MVP screens:

- Home / Dashboard
- Household setup
- Member profiles
- Generate plan
- Daily plan screen
- Meal detail screen
- Grocery list screen
- Feedback buttons

M6 - Demo flow:

- profile -> generate -> plan -> grocery list -> feedback -> regenerate

## Non-goals for now

- Fara iOS.
- Fara Play Store publishing.
- Fara login complex.
- Fara payments.
- Fara cloud deployment complexity.
- Fara Firebase/Supabase daca nu exista o justificare ulterioara clara.
- Fara generator in mobile.
- Fara CSV reads din mobile.
- Fara live price scraping.
- Fara advanced household optimizer.

## Implementation guardrails

- Nu modifica `data/recipesdb/current`.
- Nu modifica `data/fooddb/current`.
- Nu muta logica in mobile.
- Nu introduce backend/mobile inainte de scheletul API minim.
- Nu schimba generator logic ca parte din roadmap documentation.
- Nu schimba grocery/pricing logic ca parte din roadmap documentation.
