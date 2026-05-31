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

## Troubleshooting

Pe setup-ul testat local, `npx expo start --android` a functionat cu URL LAN pentru Expo.

`npx expo start --android --localhost` a esuat in Expo Go deoarece Metro a expus un URL localhost/IPv6, iar aplicatia a primit `exp://127.0.0.1:8081`.

Pastreaza `API_BASE_URL` pentru emulator Android ca:

```text
http://10.0.2.2:8000
```

Pentru telefon fizic, foloseste IP-ul LAN al PC-ului pentru backend, nu `localhost`.

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
- Cere si afiseaza grocery list pentru planul generat cand backend-ul o returneaza.
- Afiseaza butoane feedback pentru mesele generate si salveaza feedback-ul prin backend.

## Mobile M2 Flow

1. Porneste backend-ul FastAPI.
2. Apasa `Check backend health`.
3. Apasa `Load demo household`.
4. Selecteaza un membru demo.
5. Apasa `Generate plan for selected member`.

Mobile M2 trimite profilul demo inline din `/households/demo`. Nu foloseste inca `member_profile_id` persistent din SQLite.

Grocery screen, feedback buttons, household generation screen, auth/login si cloud sync raman pentru checkpointuri ulterioare.

## Mobile M3 Flow

Mobile M3 pastreaza acelasi flow pe un singur ecran, dar requestul de generare cere si grocery list:

- `include_grocery_list=true`
- `include_purchase_suggestions=true`
- `include_price_estimates=true`

Dupa generarea planului, aplicatia afiseaza sectiunea `Grocery list` sub plan. Lista foloseste payload-ul returnat de backend si poate afisa categorii, cantitati necesare, purchase suggestions / sugestii de cumparare, costuri estimate si avertizari.

Preturile sunt estimari demo, nu preturi live. Unele itemuri pot ramane fara estimare de pret si vor fi marcate ca missing/no price estimate.

Nu exista inca feedback screen, household mobile generation, auth/login sau cloud sync. Aplicatia mobila continua sa consume doar FastAPI prin HTTP/JSON si nu citeste CSV-uri.

## Mobile M4 Flow

Mobile M4 adauga butoane feedback compacte pentru fiecare masa generata:

- `Like`
- `Dislike`
- `Too long`
- `Avoid`

Feedback-ul este trimis catre backend prin `POST /feedback` si salvat in SQLite local/demo. Dupa salvare, aplicatia afiseaza mesajul `Feedback saved. Generate again to apply it.`

Generarea individuala foloseste acum `feedback_enabled=true`, astfel incat urmatorul `POST /plans/generate` poate primi contextul feedback agregat din backend. Pentru `Avoid`, backend-ul trateaza reteta ca preferinta hard cand exista alternative.

Sectiunea `Feedback context` poate reimprospata `GET /feedback/context` si afiseaza numarul total de evenimente plus numerele pentru avoided, liked, disliked si too long.

Nu exista inca account/auth, cloud sync sau feedback screen separat. Feedback-ul este comportament demo/local SQLite prin FastAPI, iar mobile nu citeste CSV-uri si nu ruleaza generatorul.

## Next Step

Urmatorul pas este Mobile M5: ecran pentru household generation sau Mobile M6: profil persistent / create profile UI.
