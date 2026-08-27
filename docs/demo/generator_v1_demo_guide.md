# Generator v1 Demo Guide

This guide describes safe local ways to exercise Generator v1 from the current public repository.

The current recommended demo path is the mobile app plus FastAPI backend. Direct generator commands remain useful for technical validation, but they are not the main product flow.

## Current Runtime Path

```text
React Native / Expo mobile app
  -> FastAPI backend
  -> src/generator_v1/service.py
  -> data/fooddb/current
  -> data/recipesdb/current
```

The mobile app does not read CSV files and does not import the generator.

## Recommended Backend/Mobile Demo

Start the backend from the repository root:

```powershell
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```

Start the mobile app:

```powershell
cd mobile
npx expo start --go --host lan
```

Useful mobile flow:

1. Create an account or log in.
2. Add one or more member profiles.
3. Generate a meal plan.
4. Open Cook / Steps for a recipe.
5. Open Alternatives and preview a replacement.
6. Open Grocery List.
7. Open Insights and save daily progress.

## CLI Smoke Example

Run from the repository root:

```powershell
python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile current --selection_mode balanced_day --portion_policy target_aware --meal_realism_mode practical --quality_gate demo_safe --days 3 --multi_day_mode global_alternatives_3_day --multi_day_no_repeat_policy hard --day_candidate_builder direct_from_slots --profile_guard demo
```

Expected high-level result:

- a valid generated plan using `data/recipesdb/current`;
- no direct mobile dependency;
- no live price fetching;
- no cloud persistence.

## Useful Checks

```powershell
python tools/extra/check_generator_service_m2.py
python tools/extra/check_backend_m3_generation_endpoints.py
python tools/extra/check_backend_m5_persistence_aware_generation.py
python tools/extra/check_backend_knn_recipe_alternatives.py
python tools/extra/check_backend_knn_meal_replacement.py
python tools/extra/check_data_qa_price_time_no_missing.py
```

## What Not To Claim

- Do not present the system as a clinical nutrition tool.
- Do not claim live supermarket prices or pantry inventory.
- Do not describe KNN-lite as the main generator.
- Do not describe the current generator as ML-first.
- Do not claim cloud sync or production authentication.
