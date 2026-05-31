# TableTogether Mobile

## Purpose

Acest folder contine scheletul Android MVP pentru aplicatia mobila TableTogether.

Scopul curent este conectivitatea cu backend-ul FastAPI si un flow demo minim: health check, incarcare household demo, selectie membru si generare plan individual. Aplicatia mobila nu citeste CSV-uri, nu ruleaza generatorul si nu contine logica nutritionala.

## Requirements

- Node.js
- Expo CLI prin `npx expo`
- Android Studio pentru Android SDK / emulator sau Expo Go pe telefon
- Backend FastAPI pornit local

## Run backend

Din radacina proiectului:

```powershell
uvicorn backend.app.main:app --reload
```

## Run mobile

```powershell
cd mobile
npm install
npx expo start
```

Pentru emulator Android:

```powershell
npx expo start --android
```

## Backend URL

Valoarea implicita pentru emulator Android este:

```text
http://10.0.2.2:8000
```

Pentru telefon fizic pe acelasi Wi-Fi, schimba `API_BASE_URL` in `mobile/src/config/api.ts` la IP-ul LAN al PC-ului, de exemplu:

```text
http://192.168.x.x:8000
```

Pentru web/browser local poate fi folosit:

```text
http://127.0.0.1:8000
```

## Current Scope

- Afiseaza numele aplicatiei.
- Afiseaza URL-ul backend.
- Apeleaza `GET /health`.
- Afiseaza service, version si database status.
- Apeleaza `GET /households/demo`.
- Afiseaza membrii demo ca selectii simple.
- Genereaza un plan individual de 3 zile prin `POST /plans/generate`.
- Afiseaza zilele generate si mesele cu kcal/protein cand sunt disponibile.

## Mobile M2 Flow

1. Porneste backend-ul FastAPI.
2. Apasa `Check backend health`.
3. Apasa `Load demo household`.
4. Selecteaza un membru demo.
5. Apasa `Generate plan for selected member`.

Mobile M2 trimite profilul demo inline din `/households/demo`. Nu foloseste inca `member_profile_id` persistent din SQLite.

Grocery screen, feedback buttons, household generation screen, auth/login si cloud sync raman pentru checkpointuri ulterioare.

## Next Step

Urmatorul pas este Mobile M3: profil persistent sau flow demo cu salvare profil, apoi ecran pentru grocery list sau feedback.
