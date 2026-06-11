# API contract v1

## Purpose

Acest document defineste contractul API pregatit pentru primul backend TableTogether. Scopul este sa existe o limita clara intre aplicatia Android si generatorul Python inainte de implementarea FastAPI.

Contractul a pornit ca planificare pentru Backend Prep 1. Backend M3 implementeaza primele endpointuri de generare si retrieval prin FastAPI, folosind `src/generator_v1/service.py` si SQLite local/demo. Backend M4 adauga demo household, profile API si feedback API cu persistenta SQLite. Backend M5 face endpointurile de generatie persistence-aware prin `member_profile_id`, `selected_member_ids` si context feedback SQLite. Auth-M1 adauga autentificare locala SQLite pentru conturi MVP si profile scoped pe household-ul contului.
Backend KNN-2 adauga `POST /recipes/similar` pentru alternative de retete aprobate/review prin KNN-lite + generator approval gate. Backend KNN-4 adauga meal-level replacement explicit prin `POST /plans/{plan_id}/replace-meal`; acesta schimba o reteta/masa intreaga, nu ingrediente individuale.

Implementarile M3/M4/M5 nu modifica formule nutritionale, grocery/pricing si nu schimba fisierele din `data/recipesdb/current` sau `data/fooddb/current`.

## Shared conventions

- API-ul foloseste HTTP/JSON.
- Raspunsurile trebuie sa fie JSON-serializable.
- `dataset_profile` recomandat pentru MVP demo este `v1_2_demo_final`.
- `days` accepta valori `1..5`.
- Mobile app nu citeste CSV-uri si nu ruleaza generatorul.
- FastAPI backend apeleaza un wrapper Python peste Generator v1.
- SQLite persista household-uri, profiluri, feedback, planuri generate si grocery lists.
- Auth-M1 foloseste conturi locale SQLite, fara cloud auth, fara email verification si fara password reset.
- Parolele sunt stocate ca `password_hash` + `password_salt`; parola plaintext nu se stocheaza.
- Sesiunile folosesc token brut returnat clientului o singura data si hash de token stocat in SQLite.
- Daca `Authorization: Bearer <session_token>` este prezent la profile API, profilurile sunt scoped pe household-ul contului.

## POST /auth/register

Purpose:
- Creeaza un cont local MVP, creeaza household-ul implicit al contului si autentifica utilizatorul.

Request schema example:

```json
{
  "email": "alex@example.com",
  "password": "Secret123",
  "confirm_password": "Secret123"
}
```

Response schema example:

```json
{
  "status": "ok",
  "message": "Account created",
  "session_token": "raw-token-returned-once",
  "account": {
    "user_id": "user_abc123",
    "email": "alex@example.com",
    "household_id": "household_abc123",
    "household_display_name": "My Household"
  }
}
```

MVP notes:
- Emailul este normalizat lowercase/trim.
- Parola minima are 6 caractere.
- `password` si `confirm_password` trebuie sa coincida.
- Duplicate email returneaza eroare clara.
- Nu se trimite email.

## POST /auth/login

Purpose:
- Autentifica un cont local si creeaza o sesiune noua.

Request schema example:

```json
{
  "email": "alex@example.com",
  "password": "Secret123"
}
```

Response:
- Aceeasi forma ca `/auth/register`, cu mesaj `Logged in`.

## POST /auth/logout

Purpose:
- Revoca sesiunea locala daca tokenul este furnizat.

Request schema example:

```json
{
  "session_token": "raw-token"
}
```

Response schema example:

```json
{
  "status": "ok",
  "message": "Logged out"
}
```

## GET /auth/me

Purpose:
- Returneaza contul curent pentru un `Authorization: Bearer <session_token>` valid.

Response schema example:

```json
{
  "status": "ok",
  "account": {
    "user_id": "user_abc123",
    "email": "alex@example.com",
    "household_id": "household_abc123",
    "household_display_name": "My Household"
  }
}
```

Error cases:
- `401 unauthenticated` daca tokenul lipseste, este invalid sau a fost revocat.

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
  "version": "v1",
  "database": "ok"
}
```

MVP notes:
- Nu necesita SQLite.
- Trebuie sa fie primul endpoint implementat in backend skeleton.
- In M1/M3 raspunsul include si `database`, cu valoarea `ok` sau `not_initialized`.

Non-goals:
- Nu verifica disponibilitatea completa a generatorului.
- Nu verifica integritatea datasetului.

## POST /plans/generate

Purpose:
- Genereaza un plan individual pentru un singur `member_profile` inline sau pentru un profil salvat in SQLite prin `member_profile_id`.

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
      "no_pork": false,
      "no_chicken": false,
      "no_fish": false,
      "no_dairy": false,
      "vegetarian": false,
      "vegan": false,
      "gluten_free": false
    },
    "food_preferences": {
      "ratings": {
        "chicken": "like",
        "pork": "avoid"
      },
      "avoid_ingredients": [],
      "cooking_time_preference": "balanced"
    },
    "health_and_diet_preferences": {
      "dietary_patterns": {
        "keto": false,
        "paleo": false,
        "mediterranean": true
      },
      "health_modes": {
        "diabetes_aware": false,
        "hypertension_friendly": false,
        "heart_friendly": false
      }
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
- Backend M3 implementeaza endpointul prin `src.generator_v1.service.generate_individual_plan_from_request`.
- Backend M3 persista full request/response JSON si indexeaza best-effort zilele, mesele si grocery list-ul.
- Backend M5 accepta `member_profile_id`; profilul este incarcat din SQLite si convertit la forma compatibila cu generatorul.
- Profilul inline ramane suportat pentru smoke/test si compatibilitate.
- Daca `feedback_enabled=true`, Backend M5 injecteaza contextul agregat din SQLite in requestul trimis catre service.
- Daca `feedback_enabled=false`, generarea foloseste context feedback neutru.

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
- Backend M3 implementeaza endpointul prin `src.generator_v1.service.generate_household_plan_from_request`.
- Backend M3 persista full request/response JSON si grocery JSON in SQLite local/demo.
- Backend M5 poate construi `household_profile` din profilurile active SQLite pentru `household_id`.
- `selected_member_ids` filtreaza membrii pentru profil inline, demo fallback si profil household construit din SQLite.
- Demo fallback din `profiles/household_profile_demo_v1.json` ramane suportat.
- Daca `feedback_enabled=true`, Backend M5 injecteaza context feedback SQLite la nivel de household.

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
- Backend M3 returneaza direct `generated_plans.response_json`.

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
- Backend M3 returneaza direct `generated_plans.response_json` si verifica `generation_type=household`.

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
- Backend M3 returneaza `grocery_lists.grocery_json` pentru planuri individuale sau household.

Non-goals:
- Nu face live price scraping.
- Nu alege magazin, brand sau pachet optim.
- Nu scade pantry inventory.

## POST /recipes/similar

Purpose:
- Returneaza alternative pentru o reteta existenta folosind KNN-lite ca provider de candidati si Generator v1 ca validator/approver.

Request schema example:

```json
{
  "recipe_id": "recipes_v1_2_round41_manual_012",
  "slot": "breakfast",
  "top_k": 5,
  "candidate_pool_k": 20,
  "dataset_profile": "v1_2_demo_final",
  "household_id": "household_demo_family_001",
  "member_profile_id": "member_demo_adult_male_001",
  "feedback_enabled": true,
  "approval_mode": "include_review"
}
```

Response schema example:

```json
{
  "status": "ok",
  "recipe_id": "recipes_v1_2_round41_manual_012",
  "source_recipe": {
    "recipe_id": "recipes_v1_2_round41_manual_012",
    "display_name": "Smoked Salmon Toast Plate"
  },
  "slot": "breakfast",
  "dataset_profile": "v1_2_demo_final",
  "alternatives": [
    {
      "recipe_id": "recipes_v1_2_round41_manual_007",
      "display_name": "Tuna Tomato Toast Breakfast",
      "similarity_score": 0.926,
      "approval_status": "approved",
      "approval_reasons": [
        "macro_fit_ok",
        "nutrition_quality_ok",
        "time_fit_ok",
        "slot_fit_ok",
        "meal_realism_ok"
      ],
      "rejection_reasons": [],
      "macro_delta": {
        "kcal": -60.4,
        "protein_g": 4.5,
        "carbs_g": -1.2,
        "fat_g": -8.1
      },
      "time_delta_min": 0.0,
      "why_similar": [
        "slot_overlap:breakfast",
        "same_recipe_kind",
        "kcal_close",
        "protein_close",
        "time_close"
      ],
      "warnings": []
    }
  ],
  "summary": {
    "candidate_count": 20,
    "approved_count": 12,
    "review_count": 7,
    "rejected_count": 1
  },
  "warnings": []
}
```

MVP notes:
- `recipe_id` este obligatoriu.
- `slot` este optional, dar recomandat pentru approval mai precis.
- `approval_mode` accepta `approved_only`, `include_review` si `include_rejected_debug`.
- Daca `member_profile_id` este prezent, backend-ul rezolva profilul din SQLite.
- Daca `feedback_enabled=true`, backend-ul injecteaza context feedback SQLite; `explicit_avoid` respinge candidatul.
- Endpointul nu persista alternative si nu modifica planuri generate.

Error cases:
- `400` daca lipseste `recipe_id`.
- `404` daca reteta sursa nu exista in dataset sau profilul cerut nu exista.

Non-goals:
- Nu inlocuieste mese automat.
- Nu face substitutii de ingrediente.
- Nu modifica planuri direct; replacement-ul explicit este separat in `POST /plans/{plan_id}/replace-meal`.
- Nu introduce KNN ca motor principal al generatorului.

## POST /plans/{plan_id}/replace-meal

Purpose:
- Preview sau aplica inlocuirea explicita a unei mese/retete complete dintr-un plan generat cu o alta reteta validata prin KNN-lite + generator approval gate.

Query params:
- `dry_run=true` pentru preview fara persistenta.
- `dry_run=false` pentru aplicare cu plan nou derivat si grocery list recalculata.

Request schema example:

```json
{
  "day_index": 1,
  "slot": "breakfast",
  "current_recipe_id": "recipes_v1_2_round41_manual_003",
  "alternative_recipe_id": "recipes_v1_2_round41_manual_005",
  "generation_type": "individual",
  "replace_scope": "individual_meal",
  "member_profile_id": "member_demo_adult_male_001",
  "dataset_profile": "v1_2_demo_final",
  "feedback_enabled": true
}
```

Household scopes:
- `household_member_meal` inlocuieste masa unui membru.
- `household_shared_meal` inlocuieste masa shared pentru membrii afectati.

Response schema example:

```json
{
  "status": "ok",
  "dry_run": false,
  "replacement_allowed": true,
  "approval_status": "approved",
  "plan_id": "plan_individual_replaced_abc123",
  "source_plan_id": "plan_individual_original_001",
  "new_plan_id": "plan_individual_replaced_abc123",
  "generation_type": "individual",
  "replacement": {
    "day_index": 1,
    "slot": "breakfast",
    "replace_scope": "individual_meal",
    "current_meal": {
      "recipe_id": "recipes_v1_2_round41_manual_003",
      "display_name": "Egg Toast Spinach Plate"
    },
    "alternative_meal": {
      "recipe_id": "recipes_v1_2_round41_manual_005",
      "display_name": "Turkey Egg Toast Plate"
    }
  },
  "impact": {
    "meal_macro_delta": {
      "kcal": 149.3,
      "protein_g": 14.9,
      "carbs_g": 12.7,
      "fat_g": 3.9
    },
    "grocery_rebuilt": true,
    "affected_members": []
  },
  "updated_plan": {},
  "grocery_list": {}
}
```

MVP notes:
- `dry_run=true` nu persista nimic.
- `dry_run=false` creeaza un rand nou in `generated_plans`, nu suprascrie planul original.
- Planul nou include metadata `replacement_parent_plan_id` / `replacement_source_plan_id`.
- Grocery list este reconstruita din planul actualizat si salvata pentru noul plan.
- KNN ramane candidate provider; generator approval gate ramane obligatoriu.
- Doar alternativele `approved` pot fi aplicate. Alternativele `review` pot fi previzualizate, dar nu aplicate in MVP.
- Replacement-ul este meal-level / recipe-level: inlocuieste masa intreaga cu alta reteta aprobata.
- Ingredient-level substitution nu este implementat in MVP si nu este expus prin acest endpoint.
- Comportamentul este demo/local SQLite.
- Nu exista auth/user ownership enforcement inca.

Error cases:
- `400` daca lipsesc `slot`, `current_recipe_id` sau `alternative_recipe_id`.
- `400` daca alternativa nu este returnata de KNN + approval gate.
- `400` daca se incearca aplicarea unei alternative care nu este `approved`.
- `404` daca `plan_id`, ziua sau masa curenta nu exista.

Non-goals:
- Nu inlocuieste mese automat la deschiderea panoului Alternatives.
- Nu face substitutii de ingrediente.
- Nu schimba motorul principal al generatorului.

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
- Backend M4 foloseste SQLite ca sursa pentru feedback API.
- Backend M5 foloseste SQLite ca sursa pentru contextul feedback injectat in generare.

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
- Backend M4 agregheaza feedback-ul din SQLite cu aceeasi forma de context folosita de `generator_v1.feedback_adapter`.
- Backend M5 refoloseste acest context pentru endpointurile de generatie cand `feedback_enabled=true`.

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
- Backend M4 cere `confirm=true`.

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
- Backend M4 returneaza profiluri din SQLite.
- Backend M8 returneaza profiluri active implicit; profilurile soft-dezactivate nu apar in lista standard.
- `active_only=false` poate fi folosit pentru inspectie demo/dev a profilurilor inactive, daca este necesar.
- Daca nu exista profiluri SQLite, raspunsul este lista goala; mobile poate folosi `GET /households/demo` pentru membrii demo.
- In Auth-M1, daca requestul include `Authorization: Bearer <session_token>`, backend-ul ignora `household_id` din query si returneaza doar profilurile household-ului contului.

Non-goals:
- Nu implementeaza login cloud sau production-grade auth claims.
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
    "no_pork": false,
    "no_chicken": false,
    "no_fish": false,
    "no_dairy": false,
    "vegetarian": false,
    "vegan": false,
    "gluten_free": false
  },
  "food_preferences": {
    "ratings": {},
    "avoid_ingredients": [],
    "cooking_time_preference": "balanced"
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
- Backend M4 salveaza profilul in SQLite si pastreaza `training`, `meal_config`, `dietary_preferences`, `food_preferences` si `health_and_diet_preferences` ca JSON.
- In Auth-M1, daca requestul include `Authorization: Bearer <session_token>`, `household_id` este asignat automat din cont si orice `household_id` trimis de client este ignorat/overridden.
- Mobile nu trebuie sa expuna `household_id` in formularul Add Profile.
- PROFILE-WIZARD-1 extinde profilul cu `dietary_preferences.no_pork` si `food_preferences`.
- `food_preferences.ratings` foloseste valorile `like`, `dislike`, `avoid`; lipsa unei chei inseamna `Neutral`.
- `Avoid` este hard filter pentru cheile suportate si pentru `avoid_ingredients`; `Dislike` este preferinta soft persistata, nu hard ban.
- In implementarea curenta, soft scoring pentru `like`/`dislike` la nivel de ingredient/familie este deferat pentru PROFILE-PREF-2.
- DIET-HEALTH-PROFILES Phase 1 adauga `health_and_diet_preferences.dietary_patterns` cu `keto`, `paleo` si `mediterranean`, toate default `false` pentru profilurile vechi.
- `keto` si `paleo` pot activa filtre conservative pentru ingrediente clar incompatibile si penalizari de scoring; `mediterranean` este preferinta de scoring, nu hard ban.
- `health_and_diet_preferences.health_modes` exista in contract cu valori default `false`, dar modurile health-aware sunt activate in fazele urmatoare.
- DIET-HEALTH-PROFILES Phase 2 activeaza `health_and_diet_preferences.health_modes.diabetes_aware` ca mod de preferinta/scoring. Nu este tratament, diagnostic sau management medical.
- DIET-HEALTH-PROFILES Phase 3 activeaza `health_and_diet_preferences.health_modes.hypertension_friendly`; UI-ul foloseste label-ul `Blood-pressure friendly` si ramane o preferinta de selectie, nu tratament.
- DIET-HEALTH-PROFILES Phase 4 activeaza `health_and_diet_preferences.health_modes.heart_friendly` ca preferinta soft pentru retete mai usoare si mai putin procesate.

Non-goals:
- Nu valideaza medical obiectivele.
- Nu introduce cloud auth, email verification sau sincronizare cloud.

## GET /profiles/{member_profile_id}

Purpose:
- Returneaza un profil salvat in SQLite.

Request schema example:

```json
{
  "path_params": {
    "member_profile_id": "member_demo_adult_male_001"
  }
}
```

Response schema example:

```json
{
  "member_profile_id": "member_demo_adult_male_001",
  "household_id": "household_demo_family_001",
  "display_name": "Alex",
  "is_active": true,
  "age": 35,
  "sex": "male",
  "weight_kg": 82.0,
  "height_cm": 180.0,
  "activity_level": "moderately_active",
  "goal": "gain",
  "goal_speed": "slow",
  "training": {},
  "meal_config": {},
  "dietary_preferences": {},
  "food_preferences": {
    "ratings": {},
    "avoid_ingredients": [],
    "cooking_time_preference": "balanced"
  },
  "created_at": "2026-05-30T12:00:00+00:00",
  "updated_at": "2026-05-30T12:00:00+00:00"
}
```

MVP notes:
- Backend M4 returneaza 404 daca profilul lipseste.

Non-goals:
- Nu face profile ownership/auth.

## DELETE /profiles/{member_profile_id}

Purpose:
- Dezactiveaza un profil membru salvat in SQLite.

Request schema example:

```json
{
  "path_params": {
    "member_profile_id": "member_demo_adult_male_001"
  },
  "query_params": {
    "confirm": true
  }
}
```

Response schema example:

```json
{
  "status": "ok",
  "member_profile_id": "member_demo_adult_male_001",
  "deactivated": true,
  "deleted": false
}
```

MVP notes:
- Backend M8 cere `confirm=true`.
- Endpoint-ul face soft delete: seteaza `is_active=0` si actualizeaza `updated_at`.
- Profilul nu este sters fizic din SQLite.
- Dupa dezactivare, `GET /profiles` nu mai returneaza profilul in lista implicita active-only.
- Endpoint-ul este comportament demo/local SQLite.
- Mobile foloseste endpoint-ul pentru actiunea `Remove` pe profiluri salvate.
- In Auth-M1, daca requestul include `Authorization: Bearer <session_token>`, profilul poate fi dezactivat doar daca apartine household-ului contului.

Error cases:
- `400` daca lipseste `confirm=true`.
- `404` daca profilul nu exista.

Non-goals:
- Nu face hard delete by default.
- Nu implementeaza auth sau user ownership enforcement inca.

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
- Backend M4 citeste direct `profiles/household_profile_demo_v1.json` si nu il persista automat.

Non-goals:
- Nu este sistem real de onboarding.
- Nu este login sau cloud sync.
