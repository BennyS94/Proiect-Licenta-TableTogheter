# API contract v1

## Purpose

Acest document defineste contractul API pregatit pentru primul backend TableTogether. Scopul este sa existe o limita clara intre aplicatia Android si generatorul Python inainte de implementarea FastAPI.

Contractul este planificare pentru Backend Prep 1. Nu implementeaza endpoint-uri, nu modifica Generator v1, nu modifica formule nutritionale, nu modifica grocery/pricing si nu schimba fisierele din `data/recipesdb/current` sau `data/fooddb/current`.

## Shared conventions

- API-ul foloseste HTTP/JSON.
- Raspunsurile trebuie sa fie JSON-serializable.
- `dataset_profile` recomandat pentru MVP demo este `v1_2_demo_final`.
- `days` accepta valori `1..5`.
- Mobile app nu citeste CSV-uri si nu ruleaza generatorul.
- FastAPI backend apeleaza un wrapper Python peste Generator v1.
- SQLite persista household-uri, profiluri, feedback, planuri generate si grocery lists.

## Shared generation options

Exemplu comun:

```json
{
  "selection_mode": "balanced_day",
  "portion_policy": "target_aware",
  "meal_realism_mode": "practical",
  "quality_gate": "demo_safe",
  "profile_guard": "demo",
  "multi_day_mode": "global_alternatives_3_day",
  "multi_day_no_repeat_policy": "hard",
  "day_candidate_builder": "direct_from_slots",
  "include_grocery_list": true,
  "include_purchase_suggestions": true,
  "include_price_estimates": true,
  "feedback_enabled": true
}
```

## GET /health

Purpose:
- Verifica daca backend-ul raspunde.

Request schema example:

```json
{}
```

Response schema example:

```json
{
  "status": "ok",
  "service": "tabletogether-api",
  "version": "v1"
}
```

MVP notes:
- Nu necesita SQLite.
- Trebuie sa fie primul endpoint implementat in backend skeleton.

Non-goals:
- Nu verifica disponibilitatea completa a generatorului.
- Nu verifica integritatea datasetului.

## POST /plans/generate

Purpose:
- Genereaza un plan individual pentru un singur `member_profile`.

Request schema example:

```json
{
  "dataset_profile": "v1_2_demo_final",
  "days": 3,
  "member_profile": {
    "account_id": "account_demo_001",
    "household_id": "household_demo_001",
    "member_profile_id": "member_demo_001",
    "profile_name": "Demo Member",
    "is_active": true,
    "age": 22,
    "sex": "male",
    "weight_kg": 67.0,
    "height_cm": 176.0,
    "activity_level": "moderately_active",
    "goal": "maintain",
    "goal_speed": "normal",
    "training": {
      "sessions_per_week": 3,
      "type": "weights"
    },
    "meal_config": {
      "meals_per_day": 3,
      "include_snacks": true,
      "day_structure": "3_meals_plus_snack"
    },
    "dietary_preferences": {
      "no_beef": false,
      "no_chicken": false,
      "no_fish": false,
      "no_dairy": false,
      "vegetarian": false,
      "vegan": false,
      "gluten_free": false
    },
    "bf_profile": "normal"
  },
  "generation_options": {
    "selection_mode": "balanced_day",
    "portion_policy": "target_aware",
    "meal_realism_mode": "practical",
    "quality_gate": "demo_safe",
    "profile_guard": "demo",
    "multi_day_mode": "global_alternatives_3_day",
    "multi_day_no_repeat_policy": "hard",
    "day_candidate_builder": "direct_from_slots",
    "include_grocery_list": true,
    "include_purchase_suggestions": true,
    "include_price_estimates": true,
    "feedback_enabled": true
  }
}
```

Response schema example:

```json
{
  "plan_id": "plan_demo_individual_001",
  "generation_type": "individual",
  "member_profile_id": "member_demo_001",
  "days": 3,
  "daily_plan": [
    {
      "day_index": 1,
      "validation_status": "valid",
      "quality_status": "accept",
      "totals": {
        "kcal": 2225.0,
        "protein_g": 148.4,
        "carbs_g": 255.2,
        "fat_g": 72.1
      },
      "selected_meals": [
        {
          "slot": "breakfast",
          "recipe_id": "recipes_v1_2_round42_dataset_015",
          "display_name": "Egg-Free and Milk-Free Baked Oatmeal",
          "portion_multiplier": 1.0,
          "kcal": 547.8,
          "protein_g": 24.6,
          "carbs_g": 75.1,
          "fat_g": 13.1,
          "effective_time_min_for_scoring": 35.0,
          "feedback_fit": 0.5,
          "warnings": []
        }
      ]
    }
  ],
  "grocery_list": {
    "grocery_list_id": "grocery_demo_individual_001",
    "currency": "RON",
    "total_estimated_cost": 242.56,
    "items": []
  },
  "feedback_context_summary": {
    "event_count": 0,
    "explicit_avoid_count": 0,
    "liked_count": 0,
    "disliked_count": 0,
    "too_long_count": 0
  },
  "warnings": [],
  "diagnostics_summary": {
    "dataset_profile": "v1_2_demo_final",
    "profile_guard_status": "normal_demo_safe",
    "valid_days": 3,
    "accept_days": 3,
    "repeated_recipes": 0
  }
}
```

MVP notes:
- Backend-ul poate salva request-ul si raspunsul complet in `generated_plans.request_json` si `generated_plans.response_json`.
- `grocery_list` este optional si apare doar cand `include_grocery_list=true`.
- Campurile detaliate ale meal rows pot ramane nested in `meal_json`.

Non-goals:
- Nu face weekly planning complet peste 5 zile.
- Nu face optimizare de magazin, brand sau pantry.
- Nu expune path-uri locale catre mobile.

## POST /household-plans/generate

Purpose:
- Genereaza un plan household pentru membrii selectati.

Request schema example:

```json
{
  "dataset_profile": "v1_2_demo_final",
  "days": 3,
  "household_profile": {
    "household_id": "household_demo_family_001",
    "household_name": "Demo Family Household",
    "active_member_ids": [
      "member_demo_adult_male_001",
      "member_demo_adult_female_001",
      "member_demo_lower_target_001"
    ],
    "members": []
  },
  "selected_member_ids": [
    "member_demo_adult_male_001",
    "member_demo_adult_female_001"
  ],
  "household_mode": "individual_breakfast_shared_main",
  "household_allocation_mode": "macro_aware_simple",
  "generation_options": {
    "selection_mode": "balanced_day",
    "portion_policy": "target_aware",
    "meal_realism_mode": "practical",
    "quality_gate": "demo_safe",
    "profile_guard": "demo",
    "multi_day_mode": "global_alternatives_3_day",
    "multi_day_no_repeat_policy": "hard",
    "day_candidate_builder": "direct_from_slots",
    "include_grocery_list": true,
    "include_purchase_suggestions": true,
    "include_price_estimates": true,
    "feedback_enabled": true
  }
}
```

Response schema example:

```json
{
  "household_plan_id": "household_plan_demo_001",
  "generation_type": "household",
  "selected_members": [
    {
      "member_id": "member_demo_adult_male_001",
      "display_name": "Alex"
    },
    {
      "member_id": "member_demo_adult_female_001",
      "display_name": "Mara"
    }
  ],
  "member_targets": [
    {
      "member_id": "member_demo_adult_male_001",
      "target_kcal": 2920.0,
      "target_protein_g": 164.0
    }
  ],
  "days": 3,
  "per_member_menus": [
    {
      "member_id": "member_demo_adult_male_001",
      "day_index": 1,
      "meals": [
        {
          "slot": "lunch",
          "recipe_id": "recipes_v1_2_round41_manual_021",
          "display_name": "Chicken Lentil Rice Bowl",
          "portion_multiplier": 1.8,
          "meal_scope": "shared"
        }
      ]
    }
  ],
  "shared_meals": [
    {
      "day_index": 1,
      "slot": "lunch",
      "recipe_id": "recipes_v1_2_round41_manual_021",
      "display_name": "Chicken Lentil Rice Bowl",
      "household_portion_sum": 3.45
    }
  ],
  "household_grocery_list": {
    "grocery_list_id": "grocery_demo_household_001",
    "currency": "RON",
    "total_estimated_cost": 303.77,
    "items": []
  },
  "household_grocery_scaling": [
    {
      "day_index": 1,
      "slot": "lunch",
      "recipe_id": "recipes_v1_2_round41_manual_021",
      "household_grocery_scaling_factor": 3.45
    }
  ],
  "member_macro_summaries": [
    {
      "member_id": "member_demo_adult_male_001",
      "day_index": 1,
      "kcal_total": 2910.0,
      "protein_total_g": 157.0,
      "kcal_ratio": 0.997,
      "protein_ratio": 0.957
    }
  ],
  "warnings": [
    {
      "code": "household_generation_lite",
      "message": "Household generation is demo/audit level."
    }
  ],
  "diagnostics_summary": {
    "household_mode": "individual_breakfast_shared_main",
    "household_allocation_mode": "macro_aware_simple",
    "household_quality_status": "accept",
    "accept_day_count": 3,
    "max_grocery_scaling_factor": 3.45
  }
}
```

MVP notes:
- `household_mode` accepta `individual_breakfast_shared_main`, `shared_all_slots` si `shared_main_meals`.
- `household_allocation_mode` accepta initial `macro_aware_simple`.
- `per_member_menus` trebuie sa fie direct consumabil de mobile.

Non-goals:
- Nu este advanced household optimizer.
- Nu introduce OR-Tools, MILP sau CP-SAT.
- Nu optimizeaza preturi, magazine sau pachete.

## GET /plans/{plan_id}

Purpose:
- Returneaza un plan individual salvat.

Request schema example:

```json
{
  "path_params": {
    "plan_id": "plan_demo_individual_001"
  }
}
```

Response schema example:

```json
{
  "plan_id": "plan_demo_individual_001",
  "generation_type": "individual",
  "member_profile_id": "member_demo_001",
  "days": 3,
  "daily_plan": [],
  "warnings": [],
  "diagnostics_summary": {
    "dataset_profile": "v1_2_demo_final"
  }
}
```

MVP notes:
- Poate returna direct `generated_plans.response_json`.

Non-goals:
- Nu regenereaza planul.
- Nu recalculaza grocery list.

## GET /household-plans/{plan_id}

Purpose:
- Returneaza un plan household salvat.

Request schema example:

```json
{
  "path_params": {
    "plan_id": "household_plan_demo_001"
  }
}
```

Response schema example:

```json
{
  "household_plan_id": "household_plan_demo_001",
  "generation_type": "household",
  "selected_members": [],
  "days": 3,
  "per_member_menus": [],
  "shared_meals": [],
  "warnings": [],
  "diagnostics_summary": {
    "household_mode": "individual_breakfast_shared_main"
  }
}
```

MVP notes:
- Poate returna direct `generated_plans.response_json` pentru `generation_type=household`.

Non-goals:
- Nu ruleaza din nou generatorul.
- Nu modifica household profile.

## GET /plans/{plan_id}/grocery-list

Purpose:
- Returneaza grocery list pentru un plan individual sau household.

Request schema example:

```json
{
  "path_params": {
    "plan_id": "plan_demo_individual_001"
  }
}
```

Response schema example:

```json
{
  "grocery_list_id": "grocery_demo_individual_001",
  "plan_id": "plan_demo_individual_001",
  "household_id": "household_demo_001",
  "currency": "RON",
  "total_estimated_cost": 242.56,
  "items": [
    {
      "display_name": "Eggs",
      "category": "protein",
      "needed_grams": 264.0,
      "purchase_display": "buy 6 eggs",
      "estimated_cost": 8.99,
      "currency": "RON",
      "warnings": []
    }
  ],
  "warnings": [
    {
      "code": "demo_price_estimate",
      "message": "Prices are demo estimates."
    }
  ]
}
```

MVP notes:
- Poate returna `grocery_lists.grocery_json`.
- `estimated_cost` poate fi null cand lipseste acoperirea catalogului demo.

Non-goals:
- Nu face live price scraping.
- Nu alege magazin, brand sau pachet optim.
- Nu scade pantry inventory.

## POST /feedback

Purpose:
- Salveaza un feedback event si il face disponibil pentru generatiile urmatoare.

Feedback types:
- `liked`
- `disliked`
- `too_long`
- `explicit_avoid`

Request schema example:

```json
{
  "household_id": "household_demo_family_001",
  "member_profile_id": "member_demo_adult_male_001",
  "plan_id": "household_plan_demo_001",
  "recipe_id": "recipes_v1_2_round41_manual_021",
  "slot": "lunch",
  "feedback_type": "liked",
  "notes": "Worked well as shared lunch."
}
```

Response schema example:

```json
{
  "event_id": "feedback_event_demo_001",
  "stored": true,
  "feedback_type": "liked",
  "recipe_id": "recipes_v1_2_round41_manual_021",
  "context_summary": {
    "event_count": 1,
    "liked_count": 1,
    "disliked_count": 0,
    "too_long_count": 0,
    "explicit_avoid_count": 0
  }
}
```

MVP notes:
- Backend salveaza event-ul in SQLite.
- Pentru compatibilitate demo, backend-ul poate oglindi temporar sau importa contextul local JSONL.

Non-goals:
- Nu este ML.
- Nu propaga automat preferinte la nivel de ingredient/familie in MVP.

## GET /feedback/context

Purpose:
- Returneaza context feedback agregat pentru household si optional pentru un membru.

Request schema example:

```json
{
  "query_params": {
    "household_id": "household_demo_family_001",
    "member_profile_id": "member_demo_adult_male_001"
  }
}
```

Response schema example:

```json
{
  "household_id": "household_demo_family_001",
  "member_profile_id": "member_demo_adult_male_001",
  "event_count": 4,
  "liked_recipe_ids": [
    "recipes_v1_2_round41_manual_021"
  ],
  "disliked_recipe_ids": [],
  "too_long_recipe_ids": [],
  "explicit_avoid_recipe_ids": [],
  "summary": {
    "liked_count": 1,
    "disliked_count": 0,
    "too_long_count": 0,
    "explicit_avoid_count": 0
  }
}
```

MVP notes:
- Agregarea poate fi simpla pe `feedback_events`.

Non-goals:
- Nu antreneaza un model.
- Nu calculeaza similaritate intre retete.

## DELETE /feedback

Purpose:
- Sterge feedback-ul local/demo pentru household si optional pentru membru.

Request schema example:

```json
{
  "household_id": "household_demo_family_001",
  "member_profile_id": "member_demo_adult_male_001"
}
```

Response schema example:

```json
{
  "deleted": true,
  "deleted_event_count": 4,
  "household_id": "household_demo_family_001"
}
```

MVP notes:
- Endpoint-ul este util pentru demo reset.

Non-goals:
- Nu implementeaza audit log de productie.
- Nu implementeaza soft delete obligatoriu in MVP.

## GET /profiles

Purpose:
- Returneaza profilurile salvate.

Request schema example:

```json
{
  "query_params": {
    "household_id": "household_demo_family_001"
  }
}
```

Response schema example:

```json
{
  "household_id": "household_demo_family_001",
  "profiles": [
    {
      "member_profile_id": "member_demo_adult_male_001",
      "display_name": "Alex",
      "is_active": true,
      "age": 35,
      "sex": "male",
      "weight_kg": 82.0,
      "height_cm": 180.0,
      "activity_level": "moderately_active",
      "goal": "gain",
      "goal_speed": "slow"
    }
  ]
}
```

MVP notes:
- Pentru demo, poate returna profilurile din SQLite sau profilul demo seed-uit.

Non-goals:
- Nu implementeaza login complex.
- Nu implementeaza multi-tenant cloud accounts.

## POST /profiles

Purpose:
- Creeaza sau salveaza un profil membru.

Request schema example:

```json
{
  "household_id": "household_demo_family_001",
  "display_name": "Alex",
  "age": 35,
  "sex": "male",
  "weight_kg": 82.0,
  "height_cm": 180.0,
  "activity_level": "moderately_active",
  "goal": "gain",
  "goal_speed": "slow",
  "training": {
    "sessions_per_week": 4,
    "type": "weights"
  },
  "meal_config": {
    "meals_per_day": 3,
    "include_snacks": true,
    "day_structure": "3_meals_plus_snack"
  },
  "dietary_preferences": {
    "no_beef": false,
    "no_chicken": false,
    "no_fish": false,
    "no_dairy": false,
    "vegetarian": false,
    "vegan": false,
    "gluten_free": false
  }
}
```

Response schema example:

```json
{
  "member_profile_id": "member_demo_adult_male_001",
  "household_id": "household_demo_family_001",
  "stored": true,
  "profile": {
    "display_name": "Alex",
    "is_active": true
  }
}
```

MVP notes:
- Backend-ul trebuie sa pastreze forma profilului compatibila cu `target_builder`.

Non-goals:
- Nu valideaza medical obiectivele.
- Nu introduce conturi reale sau autentificare complexa.

## GET /households/demo

Purpose:
- Returneaza household-ul demo pentru conectarea rapida a mobile UI la API.

Request schema example:

```json
{}
```

Response schema example:

```json
{
  "household_id": "household_demo_family_001",
  "household_name": "Demo Family Household",
  "active_member_ids": [
    "member_demo_adult_male_001",
    "member_demo_adult_female_001",
    "member_demo_lower_target_001"
  ],
  "members": [
    {
      "member_id": "member_demo_adult_male_001",
      "display_name": "Alex",
      "age": 35,
      "goal": "gain"
    }
  ],
  "planning_config": {
    "days": 3,
    "shared_meals": [
      "lunch",
      "dinner"
    ],
    "breakfast_mode": "flexible",
    "snack_mode": "individual"
  }
}
```

MVP notes:
- Endpoint util pentru demo si smoke testing mobile.
- Poate fi seed-uit din `profiles/household_profile_demo_v1.json`.

Non-goals:
- Nu este sistem real de onboarding.
- Nu este login sau cloud sync.
