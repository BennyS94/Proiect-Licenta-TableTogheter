# TableTogether Mobile

This folder contains the React Native / Expo mobile application for TableTogether.

The mobile app is the user-facing layer. It handles screens, local UI state, navigation and HTTP calls to the FastAPI backend. It does not read Food_DB / Recipes_DB CSV files and does not run the Python generator directly.

## Main Stack

- React Native
- Expo SDK 56
- TypeScript
- `react-native-svg`
- `lottie-react-native`

## Main Files

- `App.tsx` - mobile entry point.
- `src/screens/HomeScreen.tsx` - top-level screen orchestration and page routing.
- `src/hooks/useHomeScreenState.ts` - shared state for auth, profiles, generated plan, selected tab/day/profile and progress.
- `src/api/apiClient.ts` - HTTP client and API request helpers.
- `src/screens/MealPlanPage.tsx` - Page 2, including generation, meal cards and grocery list.
- `src/screens/InsightsPage.tsx` - Page 3, including daily balance, meal contribution, macro targets, saved progress and trends.
- `src/screens/HouseholdPage.tsx` - Page 4, including account, household management and app settings.
- `src/data/homeContent.ts` - hardcoded editorial content for Home resources.
- `assets/` - checked-in image, SVG, Lottie and UI assets.

## Local Setup

Install dependencies:

```powershell
cd mobile
npm install
```

Start Expo:

```powershell
npx expo start
```

Run on Android emulator:

```powershell
npx expo start --android
```

## Backend Connection

For Android emulator, the backend URL is usually:

```text
http://10.0.2.2:8000
```

For a physical phone on the same network, use the PC LAN IP:

```powershell
$env:EXPO_PUBLIC_API_BASE_URL='http://<PC_LAN_IP>:8000'
npx expo start --go --host lan
```

For a physical phone connected through USB, `adb reverse` is usually the most stable local setup:

```powershell
adb reverse tcp:8000 tcp:8000
adb reverse tcp:8081 tcp:8081
$env:EXPO_PUBLIC_API_BASE_URL='http://127.0.0.1:8000'
npx expo start --host lan --port 8081 --clear
```

Backend command from the repository root:

```powershell
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Current User Flow

1. Create an account or log in.
2. Add household member profiles.
3. Generate a meal plan.
4. View the plan by member and day.
5. Open cooking steps.
6. Request and preview recipe alternatives.
7. Replace a meal when the backend approves the alternative.
8. Submit meal feedback.
9. Open the household grocery list.
10. Mark eaten meals in Insights.
11. Save or delete daily progress.
12. Review progress trends from saved days.

## Page Summary

Home:

- warm discovery page with hero animation;
- household CTA;
- rotating Daily Food Tip;
- resource sections and "See all" detail pages.

Meal Plan / Grocery:

- generation card with 1-5 day selector;
- automatic individual vs household generation based on active profiles;
- profile viewer selector;
- day selector;
- meal cards with Cook / Steps, Alternatives and Rate meal;
- grocery list with estimated total, Share/Copy actions, category cards and item selection.

Insights:

- Daily Balance with macro donut;
- Meal contribution with eaten/not-eaten toggles;
- Macro Targets;
- saved daily progress;
- Trends from persisted progress snapshots;
- Micronutrients placeholder without full micronutrient claims.

Household / Account:

- account settings;
- household name and member management;
- current device viewer selection;
- app settings;
- local diagnostic tools.

## Backend API Usage

The app calls the backend through `src/api/apiClient.ts`.

Important calls:

- `GET /health`
- `POST /auth/register`
- `POST /auth/login`
- `POST /auth/logout`
- `GET /auth/me`
- `GET /profiles`
- `POST /profiles`
- `DELETE /profiles/{member_profile_id}?confirm=true`
- `POST /plans/generate`
- `POST /household-plans/generate`
- `GET /plans/{plan_id}/grocery-list`
- `POST /feedback`
- `GET /feedback/context`
- `POST /recipes/similar`
- `POST /plans/{plan_id}/replace-meal`
- `POST /progress/daily`
- `GET /progress/daily`
- `DELETE /progress/daily/{progress_id}`
- `POST /progress/daily/{progress_id}/delete`

## Alternatives Behavior

Recipe alternatives come from the backend. The mobile app may prefetch replacement previews and cache them by plan/day/slot/profile context, but it does not invent alternative meals locally.

Only approved alternatives can be applied. Review alternatives may be previewed but remain disabled for replacement.

## Assets

Mobile visual assets are documented in `assets/README_assets.md`.

Important active asset areas:

- Home hero: `assets/home/welcome/cooking_lottie.json`
- Daily Food Tips: `assets/home/tips/tip_*.png`
- Grocery category icons: `assets/grocery/categories/`
- Bottom navigation icon source area: `assets/navigation/`

Lottie rendering uses `lottie-react-native`.

## Verification

TypeScript:

```powershell
cd mobile
npx tsc --noEmit
```

Expo dependency check:

```powershell
cd mobile
npx expo install --check
```

Mobile structure checks from the repository root:

```powershell
python tools/extra/check_mobile_ui_shell_structure.py
python tools/extra/check_mobile_home_page_structure.py
python tools/extra/check_mobile_profile_wizard_structure.py
python tools/extra/check_mobile_knn_alternatives_structure.py
python tools/extra/check_mobile_knn_replacement_structure.py
python tools/extra/check_mobile_progress_daily_structure.py
python tools/extra/check_mobile_progress_trends_structure.py
python tools/extra/check_mobile_assets_structure.py
```

## Current Limitations

- The app depends on a locally running backend during development.
- Session persistence across full app restarts is limited.
- Change email and change password are UI-facing settings, but backend update endpoints are not implemented.
- Home resources are hardcoded local content.
- Prices are static estimates from backend reference data, not live prices.
- Micronutrients are not implemented as a complete nutrition analysis feature.
- Health-aware options are non-clinical preference modes, not medical advice.
