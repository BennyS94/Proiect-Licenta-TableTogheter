# TableTogether Mobile

## Purpose

Acest folder contine scheletul Android MVP pentru aplicatia mobila TableTogether.

Scopul curent este conectivitatea cu backend-ul FastAPI si un flow demo minim: health check, incarcare household demo, selectie membru, generare plan individual si generare plan household. Aplicatia mobila nu citeste CSV-uri, nu ruleaza generatorul si nu contine logica nutritionala.

Auth-M1 schimba flow-ul principal spre cont local SQLite: utilizatorul poate crea cont, se poate loga, poate crea profiluri salvate sub household-ul contului si poate iesi prin Log Out.

UI-2B/UI-2C fac Meal Plan family-first: utilizatorul vede un singur buton `Generate meal plan`, iar aplicatia alege intern endpoint-ul individual cand exista un singur profil si endpoint-ul household cand exista doua sau mai multe profiluri active. Sample/Demo household nu mai este parte din flow-ul principal.

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

Pentru testare viitoare pe telefon fizic in aceeasi retea Wi-Fi, backend-ul trebuie pornit ascultand pe LAN:

```powershell
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
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

Forma asteptata pentru telefon fizic este:

```text
http://<PC_LAN_IP>:8000
```

Pentru web/browser local poate fi folosit:

```text
http://127.0.0.1:8000
```

Pentru test pe telefon fara hardcode permanent, porneste Expo cu:

```powershell
$env:EXPO_PUBLIC_API_BASE_URL='http://<PC_LAN_IP>:8000'
npx expo start --go --host lan
```

## Current Scope

Nota: sectiunile `Mobile M2 Flow`, `Mobile M3 Flow` etc. de mai jos pastreaza istoricul checkpoint-urilor. Starea curenta a aplicatiei este cea din aceasta sectiune si include Auth-M1, UI-2B/UI-2C, HOME-1, PROFILE-WIZARD-1, KNN alternatives si meal-level replacement explicit.

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
- Genereaza un plan household de 3 zile prin `POST /household-plans/generate`.
- Afiseaza cate un membru household pe rand, cu selector de zi, mese, totaluri si grocery list agregata cand backend-ul o returneaza.
- Listeaza si creeaza profiluri salvate prin backend SQLite.
- Poate genera plan individual folosind `member_profile_id` pentru un profil salvat.
- Poate selecta profiluri salvate multiple si genera plan household prin `selected_member_ids`.
- Poate elimina profiluri salvate prin `DELETE /profiles/{member_profile_id}?confirm=true`; backend-ul le soft-dezactiveaza.
- Poate curata feedback-ul local/demo prin `DELETE /feedback?confirm=true`.
- Poate afisa KNN alternatives read-only pentru mese cu `recipe_id`, prin `POST /recipes/similar`.
- Poate face meal-level replacement explicit dintr-o alternativa `approved`, prin `POST /plans/{plan_id}/replace-meal`.
- UI-1 adauga shell mobil prefinal cu 4-page navigation: Home, Meal Plan, Insights si Household / Account.
- Home este hardcoded pentru MVP si nu apeleaza backend-ul.
- Meal Plan pastreaza fluxurile reale de generare, feedback, KNN alternatives, replacement si grocery list.
- Insights are guard state si o vizualizare de baza peste planul generat, fara claims complete de micronutrienti.
- Household / Account gazduieste setup demo, profiluri salvate, default viewer, status backend si tool-uri demo.
- Auth-M1 adauga Create Account, Log In, Log Out si account-scoped profile calls.
- Add Profile nu mai expune `Household ID`; backend-ul il asigneaza automat din sesiunea contului cand exista token.
- PROFILE-WIZARD-1 inlocuieste formularul lung Add Profile cu Add Member wizard in 3 pasi: General Info, Food Preferences, Activity & Goal.
- Food Preferences foloseste o matrice `food item -> Like / Dislike / Avoid`; lipsa selectiei inseamna Neutral.
- `dietary_preferences.no_pork` si `food_preferences` sunt trimise la backend la Save Member.
- DIET-HEALTH-PROFILES Phase 1 trimite si `health_and_diet_preferences.dietary_patterns` pentru Keto, Paleo si Mediterranean.
- DIET-HEALTH-PROFILES Phase 2 trimite `health_and_diet_preferences.health_modes.diabetes_aware` prin chip-ul `Diabetes-aware`.
- DIET-HEALTH-PROFILES Phase 3 trimite `health_and_diet_preferences.health_modes.hypertension_friendly` prin chip-ul `Blood-pressure friendly`.
- `Avoid` este hard filter pentru cheile suportate si pentru custom avoided ingredients; `Dislike` este soft preference persistata, nu hard ban.
- PROFILE-WIZARD-1 summary: Add Member este un 3-step wizard; Neutral = no selection; Avoid = hard filter; Dislike = soft preference only.
- Soft scoring pentru `Like`/`Dislike` la nivel de aliment/familie este deferat; edit wizard ramane polish viitor.
- Page 4 este hub cu Account Settings, Household Management si App Settings.
- Safe area/status bar spacing este reparat global in `AppScreen`, iar empty states sunt centrate.
- UI-2B: Meal Plan are un singur buton `Generate meal plan`, selector 1-5 zile, selector de zi cu zile negenerate gri/inactive si afisare mese in ordinea Breakfast, Lunch, Snack, Dinner.
- UI-2C: Meal Plan are header curat doar cu titlul `Meal Plan`; selectorul de membru/profil apare o singura data in zona rezultatului, dupa taburile `Meal Plan` / `Grocery List`.
- UI-2C: controlul de generare 1-5 zile este slider-like custom, fara dependency noua, si afiseaza valoarea selectata ca `1 day` / `N days`.
- UI-2C: selectorul de zile generate arata Day 1-Day 5 pe un singur rand, cu zilele negenerate disabled/gri.
- PAGE2-POLISH-1: Page 2 are slider 1-5 zile centrat/compact, selector profil fara label `Viewing`, eye Lottie mic langa numele profilului, iar `Target summary` a fost scos din Meal Plan.
- PAGE2-POLISH-1: Grocery List nu mai afiseaza `Missing prices` ca metric permanent; sumarul de sus foloseste card vizual pentru `Estimated total`, item count si actiuni `Send to` / `Copy` 50/50.
- HOME-1: Home este acum Page 1 warm/family discovery, cu hero, household CTA, Daily Food Tip rotativ, carusele de resurse si subpagini interne See all.
- HOME-1: continutul Home este hardcoded in `mobile/src/data/homeContent.ts`; pagina nu apeleaza backend-ul si nu afecteaza planurile generate.
- HOME-1: asset-urile vizuale sunt placeholder-uri React Native usoare; Lottie si imaginile finale raman pending.
- HOME-1: linkurile externe sunt temporare si se deschid prin browser/YouTube cand utilizatorul apasa cardurile.
- UI-ASSETS-1: `mobile/assets/` are structura pregatita pentru Home, brand, common, navigation, meal_plan, grocery, insights si household assets.
- UI-ASSETS-1: regulile de naming si integrare sunt in `mobile/assets/README_assets.md`.
- HOME-Lottie: hero-ul Home foloseste acum `mobile/assets/home/welcome/cooking_lottie.json` prin `lottie-react-native`.
- UI-2B: Sample/Demo wording este ascuns din flow-ul principal; metadata tehnica de generator precum `plan_id`, `status`, `quality` si accept/review/reject nu mai este afisata in Meal Plan.
- UI-2B: Tema foloseste white/off-white plus accent pear green `#74B72E` prin componentele mobile comune.
- UI-2B: Login 401 afiseaza `Invalid email or password`; Change Email, Change Password, Language si Appearance sunt read-only/Coming soon unde nu exista implementare reala.
- UI-2B: Add Profile ramane in Household Management dupa salvare, selecteaza profilul nou si afiseaza CTA catre Meal Plan.
- UI-2B: Validarile profilului acopera nume fara cifre, age 4-120, weight 15-300 kg, height 80-230 cm, sessions/week 0-7 si meals/day 1-5; Goal Speed este inactiv pentru Maintain.
- Backend-ul trimite estimari de pret si cooking time in outputurile generate. Dupa DATA-QA-1, `Price unavailable` sau missing time nu ar trebui sa apara in fluxurile normale generate; daca apar, ruleaza checker-ul DATA-QA.
- Missing cooking steps afiseaza mesaj dedicat cand instructiunile nu sunt disponibile.

## Screen idle / keep-awake investigation

UI-2B a verificat `keep-awake`, `KeepAwake`, `activateKeepAwake` si configurile mobile. Nu exista cod de aplicatie care activeaza explicit keep-awake. `expo-keep-awake` apare doar tranzitiv in `package-lock.json`, prin Expo. Comportamentul observat pe telefon este cel mai probabil legat de Expo Go/dev mode sau de setarile OS/device, nu de codul TableTogether. Nu s-a adaugat workaround in aplicatie.

## Mobile UI-2B Flow

1. Porneste backend-ul FastAPI.
2. Deschide aplicatia si foloseste `Create Account` sau `Log In`.
3. Mergi in `Household Management`.
4. Apasa `Add Member` si completeaza wizard-ul in 3 pasi.
5. Ramai pe pagina de profiluri dupa salvare si foloseste CTA-ul `Go to Meal Plan`.
6. In Meal Plan apasa `Generate meal plan`.
7. Daca exista un singur profil, aplicatia foloseste intern generarea individuala; daca exista doua sau mai multe profiluri, foloseste intern generarea household.
8. Selecteaza membrul de vizualizat, schimba ziua, verifica mesele, grocery list, insights si alternatives.

## Mobile UI-2C Flow

UI-2C pastreaza flow-ul UI-2B, dar face Page 2 / Meal Plan mai product-facing:

1. Headerul Meal Plan nu mai afiseaza profilul curent.
2. Generation card afiseaza doar intentia produsului, slider-ul 1-5 zile si `Generate meal plan`.
3. Dupa generare, selectorul de profil apare o singura data sub taburi, ca pill central cu eye indicator si nume.
4. Ziua generata se alege din Day 1-Day 5, toate pe un singur rand.
5. Mesele raman ordonate Breakfast, Lunch, Snack, Dinner.
6. Debug/status/generator metadata nu apar in main flow.
7. Grocery List foloseste card vizual pentru estimarea totala si actiuni placeholder `Send to` / `Copy`.

## Mobile M2 Flow

1. Porneste backend-ul FastAPI.
2. Apasa `Check backend health`.
3. Apasa `Load demo household`.
4. Selecteaza un membru demo.
5. Apasa `Generate plan for selected member`.

Mobile M2 trimite profilul demo inline din `/households/demo`. Nu foloseste inca `member_profile_id` persistent din SQLite.

Auth/login si cloud sync raman pentru checkpointuri ulterioare.

## Mobile Auth-M1 Flow

1. Porneste backend-ul FastAPI.
2. Deschide aplicatia.
3. Alege `Create Account`.
4. Introdu email, parola si confirmare parola.
5. La succes, aplicatia seteaza sesiunea in React state si intra in Household / Account.
6. Mergi la `Household Management`.
7. Creeaza primul profil; aplicatia selecteaza profilul si Meal Plan devine utilizabil fara Load Demo.
8. Pentru revenire ulterioara in aceeasi sesiune de app, foloseste `Log In`.
9. `Log Out` revoca sesiunea daca exista token si curata state-ul local.

Limitari Auth-M1:

- sesiunea nu este persistata peste restart de app deoarece nu exista inca AsyncStorage sau storage echivalent;
- nu exista email verification;
- nu exista password reset email;
- nu exista change email / change password functional;
- nu exista cloud sync, Firebase sau Supabase.

## PROFILE-WIZARD-1 Flow

1. In Household Management, apasa `Add Member`.
2. Step 1 / General Info colecteaza nume, sex, varsta, inaltime si greutate.
3. Step 2 / Food Preferences colecteaza restrictii dietetice, pattern-uri dietare si rating-uri `Like`, `Dislike`, `Avoid`.
4. Step 3 / Activity & Goal colecteaza obiectivul, activitatea, antrenamentul, mesele pe zi, snack-ul si preferinta de timp.
5. Backend-ul este apelat doar la `Save Member`.
6. La succes, utilizatorul ramane in Household Management si vede mesajul `Member added`.

Limitari PROFILE-WIZARD-1:

- `Neutral` nu se stocheaza explicit; lipsa cheii din `food_preferences.ratings` inseamna neutral.
- `Avoid` este integrat ca hard filter.
- `Dislike` si `Like` sunt persistate, dar nu au inca scoring ingredient-level/family-level.
- Keto/Paleo/Mediterranean sunt preferinte de filtrare/scoring si nu sunt afisate ca sfat medical.
- `Diabetes-aware` este o preferinta de prioritizare/scoring si nu este formulat ca tratament sau management medical.
- `Blood-pressure friendly` prioritizeaza soft alegeri mai simple/mai putin sarate unde exista semnale, fara claims medicale.
- Editarea profilului cu acelasi wizard ramane viitoare.

## Mobile M3 Flow

Mobile M3 pastreaza acelasi flow pe un singur ecran, dar requestul de generare cere si grocery list:

- `include_grocery_list=true`
- `include_purchase_suggestions=true`
- `include_price_estimates=true`

Dupa generarea planului, aplicatia afiseaza sectiunea `Grocery list` sub plan. Lista foloseste payload-ul returnat de backend si poate afisa categorii, cantitati necesare, purchase suggestions / sugestii de cumparare, costuri estimate si avertizari.

Preturile sunt estimari demo/reference-based, nu preturi live. Dupa DATA-QA-1, itemurile app-facing generate nu ar trebui sa ramana fara estimare de pret; daca UI-ul afiseaza `Price unavailable` sau timp lipsa, ruleaza:

```powershell
python tools/extra/check_data_qa_price_time_no_missing.py
```

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

## Mobile M5 Flow

Mobile M5 adauga modul `Household plan` in acelasi ecran principal:

1. Porneste backend-ul FastAPI.
2. Apasa `Check backend health`.
3. Apasa `Load demo household`.
4. Alege `Household plan`.
5. Selecteaza unul sau mai multi membri demo.
6. Apasa `Generate household plan`.

Aplicatia trimite requestul catre `POST /household-plans/generate` si afiseaza raspunsul backend-ului: summary household, selector de membru, selector de zi, mese pe membru, portion multiplier, macro-uri disponibile si grocery list agregata.

Modul household este inca demo Android-first. Nu exista auth/login, cloud sync, UI polish final sau ecran separat pentru household setup.

## Mobile M6 Flow

Mobile M6 adauga profiluri persistente demo in acelasi ecran principal:

1. Porneste backend-ul FastAPI.
2. Apasa `Check backend health`.
3. Apasa `Load saved profiles`.
4. Completeaza formularul `Create profile`.
5. Apasa `Save profile`.
6. Selecteaza profilul salvat.
7. Apasa `Generate plan for selected profile`.

Profilurile sunt create prin `POST /profiles`, listate prin `GET /profiles` si stocate in SQLite local/demo in backend. Generarea individuala poate trimite doar `member_profile_id`, iar backend-ul incarca profilul salvat si il foloseste pentru Generator v1.

Membrii demo raman disponibili prin `GET /households/demo`, iar household generation ramane pe flow-ul M5. Nu exista login/auth, cloud sync sau conturi reale. Daca baza locala `data/runtime/tabletogether_demo.db` este stearsa, profilurile salvate dispar.

## Mobile M7 Flow

Mobile M7 adauga generatie household pe baza profilurilor salvate:

1. Porneste backend-ul FastAPI.
2. Apasa `Load saved profiles`.
3. Creeaza sau selecteaza cel putin un profil salvat.
4. Alege `Household plan`.
5. Seteaza `Household source` la `Saved profiles`.
6. Selecteaza 2 sau mai multe profiluri salvate pentru scenariul household normal.
7. Apasa `Generate household plan`.

Aplicatia trimite `POST /household-plans/generate` cu `selected_member_ids` si `household_id`, astfel incat backend-ul poate construi household-ul din profilurile SQLite. Afisarea planului reutilizeaza selectorul de membru si selectorul de zi din flow-ul M5, iar grocery list ramane agregata la nivel de household prin sectiunea `Household grocery list`.

Sursa `Demo household` ramane disponibila pentru flow-ul M5. Nu exista inca auth/login, cloud sync, Firebase/Supabase, ecran household final, feedback household dedicat sau polish UI final.

## Mobile M8 Flow

Mobile M8 adauga cleanup minim pentru testare locala/demo:

1. Apasa `Load saved profiles`.
2. Creeaza sau selecteaza un profil salvat.
3. Apasa `Remove` pe cardul profilului salvat.
4. Confirma `Remove this saved profile?`.
5. Profilul este dezactivat prin `DELETE /profiles/{member_profile_id}?confirm=true` si dispare din lista implicita `GET /profiles`.
6. In panelul `Feedback context`, apasa `Clear feedback`.
7. Confirma stergerea feedback-ului pentru household-ul activ.

Profilurile salvate sunt soft-deactivated in SQLite local/demo, nu hard-deleted. Membrii demo din `GET /households/demo` nu au buton de stergere. Clear feedback foloseste endpointul existent `DELETE /feedback?confirm=true` si ramane comportament local/demo.

Nu exista auth/login, cloud sync, Firebase/Supabase, reset generat de planuri din mobile, UI polish final sau KNN/substitutions in Mobile M8.

## Mobile KNN-3 Flow

Mobile KNN-3 adauga butonul `Alternatives` pentru mesele generate care au `recipe_id`:

1. Genereaza un plan individual sau household.
2. Apasa `Alternatives` pe un rand de masa.
3. Aplicatia apeleaza backend-ul prin `POST /recipes/similar`.
4. Sunt afisate doar alternativele `approved` si `review`.

KNN propune candidati, dar backend-ul pastreaza generator approval gate pentru slot, profil, feedback, macro, timp si realism. Afisarea este read-only: nu exista inlocuire automata de masa, nu exista substitutie de ingrediente si nu se recalculeaza grocery list din alternative.

## Mobile KNN-4 Flow

Mobile KNN-4 adauga replacement explicit peste panoul `Alternatives`:

1. Genereaza un plan individual sau household.
2. Apasa `Alternatives` pe un rand de masa.
3. Apasa `Preview replacement` pe o alternativa.
4. Aplicatia apeleaza `POST /plans/{plan_id}/replace-meal?dry_run=true`.
5. Preview-ul afiseaza masa curenta, alternativa, delta macro si avertizari.
6. Apasa `Replace meal` pentru aplicare explicita.
7. Aplicatia apeleaza `POST /plans/{plan_id}/replace-meal?dry_run=false`.
8. Backend-ul returneaza plan nou derivat si grocery list recalculata, iar mobile actualizeaza state-ul local.

`Replace meal` schimba reteta/masa intreaga. Ingredient substitutions, precum schimbarea unui ingredient in interiorul retetei, nu fac parte din MVP-ul curent.

Alternativele `review` pot fi previzualizate, dar nu pot fi aplicate in MVP. Replacement-ul nu porneste automat cand se deschide panoul si nu face substitutii de ingrediente. Aplicatia consuma doar FastAPI prin HTTP/JSON; nu citeste CSV-uri si nu importa generatorul.

## Mobile UI-1 Flow

Mobile UI-1 adauga structura prefinala de produs, fara polish final:

1. App-ul porneste in Home si poate ghida utilizatorul catre Account Setup cand nu exista profiluri.
2. `Create Account` sau `Log In` activeaza flow-ul principal local.
3. Navigatia flotanta comuta intre Home, Meal Plan, Insights si Household.
4. Home afiseaza continut hardcoded scurt, family-friendly.
5. Meal Plan pastreaza generarea individuala si household, tab intern `Meal Plan` / `Grocery List`, feedback pe mese, KNN alternatives si replacement explicit.
6. Insights afiseaza empty state inainte de plan si o vizualizare de baza dupa generare.
7. Household / Account este hub pentru Account Settings, Household Management si App Settings.

UI-1 nu adauga cloud, payments, animatii finale sau chart dependency. Auth-M1 adauga auth local SQLite, nu production-grade cloud auth. Pentru emulator backend URL implicit ramane `http://10.0.2.2:8000`.

Structura UI-1 este verificata cu:

```powershell
python tools/extra/check_mobile_ui_shell_structure.py
```

## Mobile HOME-1 Flow

HOME-1 implementeaza Page 1 / Home ca ecran de descoperire, fara call-uri backend:

1. Hero-ul foloseste profilul selectat/default daca exista; altfel afiseaza fallback generic.
2. Household CTA trimite la `Go to Account Setup` cand nu exista profiluri si la `Go to Meal Plan` cand household-ul are profiluri.
3. `Daily Food Tip` se schimba local prin butonul de refresh.
4. `Highlights of the Week`, `Family & Kids Food Ideas` si `Healthy Habits` folosesc acelasi carusel reutilizabil.
5. `See all` deschide subpagini interne Home, nu pagini noi in bottom nav.
6. Cardurile de resurse deschid URL-uri externe temporare prin `Linking`.

Structura HOME-1 este verificata cu:

```powershell
python tools/extra/check_mobile_home_page_structure.py
```

## Mobile UI-ASSETS-1

UI-ASSETS-1 pregateste organizarea asset-urilor mobile fara sa schimbe designul curent si fara sa instaleze dependinte noi.

Foldere principale:

- `mobile/assets/brand/` pentru icon si splash assets viitoare de APK.
- `mobile/assets/common/` pentru placeholders, backgrounds si patterns comune.
- `mobile/assets/home/` pentru hero animation, tips, highlights, family/kids si healthy habits.
- `mobile/assets/navigation/` pentru iconuri custom viitoare.
- `mobile/assets/meal_plan/` pentru meal slots, actions, recipe details si cooking steps.
- `mobile/assets/grocery/` pentru package icons si category visuals.
- `mobile/assets/insights/` pentru macro si micronutrient visuals.
- `mobile/assets/household/` pentru account, members si profile wizard visuals.

Reguli importante:

- Foloseste lowercase snake_case pentru fisiere.
- Pentru Home hero, asset-ul activ este `mobile/assets/home/welcome/cooking_lottie.json`.
- Fallback acceptat pentru Home hero: `cooking_loop.gif` sau `cooking_loop.webp`.
- GIF/WebP/PNG pot fi adaugate fara dependency noua daca sunt folosite prin `Image`.
- Lottie foloseste `lottie-react-native`, instalat pentru hero-ul Home.
- Animated SVG nu este recomandat pana nu exista un renderer decis pentru React Native.
- Nu importa asset-uri inexistente in cod, altfel Metro/TypeScript pot esua.

Registry-ul sigur, fara importuri runtime, este:

```text
mobile/src/assets/assetRegistry.ts
```

Structura UI-ASSETS-1 este verificata cu:

```powershell
python tools/extra/check_mobile_assets_structure.py
```

## Next Step

Urmatorul pas este sa pui asset-ul ales in folderul documentat si sa imi spui ce placeholder vrei sa inlocuiasca. Pentru hero-ul Home, pune Lottie/GIF/WebP in `mobile/assets/home/welcome/`. Ingredient-level substitution ramane in afara MVP-ului curent.
