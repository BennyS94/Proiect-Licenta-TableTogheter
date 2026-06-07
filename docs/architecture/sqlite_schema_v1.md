# SQLite schema v1

## Purpose

Acest document defineste schema SQLite MVP pentru backend-ul local/demo TableTogether. Schema este pregatita pentru API-first development si pentru persistenta minima necesara aplicatiei Android.

Nu implementeaza baza de date si nu modifica generatorul. In MVP, JSON-ul complet al request-urilor si raspunsurilor poate fi pastrat in coloane text pentru a evita overengineering timpuriu.

## Scope

SQLite este folosit pentru:

- users
- user sessions
- households
- member profiles
- feedback events
- generated plans
- generated plan days
- generated plan meals
- grocery lists
- grocery list items

SQLite nu este baza cloud de productie in aceasta etapa.

## Table: households

Rol:
- Pastreaza gospodariile locale/demo.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `household_id` | TEXT PRIMARY KEY | ID stabil pentru household |
| `user_id` | TEXT NULL | Owner local Auth-M1, nullable pentru compatibilitate demo/dev |
| `household_name` | TEXT NOT NULL | Nume afisat |
| `display_name` | TEXT NULL | Nume afisat Auth-M1; fallback la `household_name` |
| `created_at` | TEXT NOT NULL | ISO timestamp |
| `updated_at` | TEXT NOT NULL | ISO timestamp |
| `settings_json` | TEXT NOT NULL | Setari household serializate JSON |
| `is_active` | INTEGER NOT NULL DEFAULT 1 | Soft active flag pentru Auth-M1 |

## Table: users

Rol:
- Pastreaza conturile locale MVP.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `user_id` | TEXT PRIMARY KEY | ID stabil utilizator |
| `email` | TEXT UNIQUE NOT NULL | Email normalizat lowercase/trim |
| `password_hash` | TEXT NOT NULL | Hash PBKDF2-HMAC-SHA256 |
| `password_salt` | TEXT NOT NULL | Salt generat cu `secrets.token_hex` |
| `created_at` | TEXT NOT NULL | ISO timestamp |
| `updated_at` | TEXT NOT NULL | ISO timestamp |
| `is_active` | INTEGER NOT NULL DEFAULT 1 | Soft active flag |

## Table: user_sessions

Rol:
- Pastreaza sesiunile locale MVP.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `session_id` | TEXT PRIMARY KEY | ID stabil sesiune |
| `user_id` | TEXT NOT NULL | FK logic spre `users.user_id` |
| `session_token_hash` | TEXT NOT NULL | Hash SHA-256 al tokenului brut |
| `created_at` | TEXT NOT NULL | ISO timestamp |
| `expires_at` | TEXT NULL | Nullable in MVP; sesiune long-lived local |
| `revoked_at` | TEXT NULL | Setat la logout |
| `is_active` | INTEGER NOT NULL DEFAULT 1 | 0 dupa revocare |

## Table: member_profiles

Rol:
- Pastreaza profilurile membrilor folosite de target builder si generator.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `member_profile_id` | TEXT PRIMARY KEY | ID stabil profil |
| `household_id` | TEXT NOT NULL | FK logic spre `households.household_id` |
| `display_name` | TEXT NOT NULL | Nume afisat |
| `age` | INTEGER NOT NULL | Varsta |
| `sex` | TEXT NOT NULL | Valoare compatibila cu profilurile curente |
| `weight_kg` | REAL NOT NULL | Greutate |
| `height_cm` | REAL NOT NULL | Inaltime |
| `activity_level` | TEXT NOT NULL | Ex: `moderately_active` |
| `goal` | TEXT NOT NULL | Ex: `maintain`, `lose`, `gain` |
| `goal_speed` | TEXT NOT NULL | Ex: `normal`, `slow` |
| `training_json` | TEXT NOT NULL | Training serializat JSON |
| `meal_config_json` | TEXT NOT NULL | Config mese serializat JSON |
| `dietary_preferences_json` | TEXT NOT NULL | Preferinte/restrictii serializate JSON |
| `food_preferences_json` | TEXT NOT NULL DEFAULT '{}' | Rating-uri alimentare, ingrediente evitate si preferinta de timp |
| `is_active` | INTEGER NOT NULL | 0/1 |
| `created_at` | TEXT NOT NULL | ISO timestamp |
| `updated_at` | TEXT NOT NULL | ISO timestamp |

## Table: feedback_events

Rol:
- Pastreaza feedback-ul local/demo consumat de generatiile urmatoare.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `event_id` | TEXT PRIMARY KEY | ID feedback event |
| `household_id` | TEXT NOT NULL | Household relevant |
| `member_profile_id` | TEXT NULL | Optional pentru feedback household-level |
| `recipe_id` | TEXT NOT NULL | Reteta afectata |
| `plan_id` | TEXT NULL | Optional, planul din care a venit feedback-ul |
| `slot` | TEXT NULL | Ex: `breakfast`, `lunch`, `dinner`, `snack` |
| `feedback_type` | TEXT NOT NULL | `liked`, `disliked`, `too_long`, `explicit_avoid` |
| `notes` | TEXT NULL | Note optionale |
| `source` | TEXT NOT NULL | Ex: `mobile`, `streamlit_demo`, `api_demo` |
| `created_at` | TEXT NOT NULL | ISO timestamp |

## Table: generated_plans

Rol:
- Pastreaza planuri individuale si household generate prin API.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `plan_id` | TEXT PRIMARY KEY | ID plan |
| `household_id` | TEXT NOT NULL | Household relevant |
| `member_profile_id` | TEXT NULL | Setat pentru plan individual, null pentru household |
| `generation_type` | TEXT NOT NULL | `individual` sau `household` |
| `dataset_profile` | TEXT NOT NULL | Ex: `v1_2_demo_final` |
| `days` | INTEGER NOT NULL | 1..5 |
| `request_json` | TEXT NOT NULL | Request complet serializat JSON |
| `response_json` | TEXT NOT NULL | Raspuns complet serializat JSON |
| `created_at` | TEXT NOT NULL | ISO timestamp |

## Table: generated_plan_days

Rol:
- Index minimal pentru zilele generate si rezumatul lor.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `plan_day_id` | TEXT PRIMARY KEY | ID rand zi |
| `plan_id` | TEXT NOT NULL | FK logic spre `generated_plans.plan_id` |
| `day_index` | INTEGER NOT NULL | 1-based |
| `kcal_total` | REAL NULL | Total kcal zi |
| `protein_total` | REAL NULL | Total protein zi |
| `carbs_total` | REAL NULL | Total carbs zi |
| `fat_total` | REAL NULL | Total fat zi |
| `validation_status` | TEXT NULL | Ex: `valid`, `invalid`, `multi_day_review` |
| `quality_status` | TEXT NULL | Ex: `accept`, `review`, `reject` |

## Table: generated_plan_meals

Rol:
- Index minimal pentru mesele din plan si campuri utile in UI.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `plan_meal_id` | TEXT PRIMARY KEY | ID rand masa |
| `plan_id` | TEXT NOT NULL | FK logic spre `generated_plans.plan_id` |
| `day_index` | INTEGER NOT NULL | 1-based |
| `slot` | TEXT NOT NULL | `breakfast`, `lunch`, `dinner`, `snack` |
| `recipe_id` | TEXT NOT NULL | ID reteta |
| `display_name` | TEXT NOT NULL | Nume afisat |
| `portion_multiplier` | REAL NULL | Multiplicator portie |
| `kcal` | REAL NULL | Kcal masa |
| `protein_g` | REAL NULL | Proteine |
| `carbs_g` | REAL NULL | Carbohidrati |
| `fat_g` | REAL NULL | Grasimi |
| `meal_json` | TEXT NOT NULL | Masa completa serializata JSON |

## Table: grocery_lists

Rol:
- Pastreaza grocery list generata pentru un plan.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `grocery_list_id` | TEXT PRIMARY KEY | ID lista |
| `plan_id` | TEXT NOT NULL | Plan sursa |
| `household_id` | TEXT NOT NULL | Household relevant |
| `total_estimated_cost` | REAL NULL | Cost demo estimat |
| `currency` | TEXT NULL | Ex: `RON` |
| `grocery_json` | TEXT NOT NULL | Lista completa serializata JSON |
| `created_at` | TEXT NOT NULL | ISO timestamp |

## Table: grocery_list_items

Rol:
- Index minimal pentru itemii de cumparaturi.

Columns:

| Column | Type | Notes |
| --- | --- | --- |
| `grocery_item_id` | TEXT PRIMARY KEY | ID item |
| `grocery_list_id` | TEXT NOT NULL | FK logic spre `grocery_lists.grocery_list_id` |
| `display_name` | TEXT NOT NULL | Nume item |
| `category` | TEXT NULL | Categorie demo |
| `needed_grams` | REAL NULL | Cantitate necesara |
| `purchase_display` | TEXT NULL | Sugestie de cumparare |
| `estimated_cost` | REAL NULL | Cost estimat, nullable |
| `currency` | TEXT NULL | Ex: `RON` |
| `item_json` | TEXT NOT NULL | Item complet serializat JSON |

## MVP notes

- `response_json` si `grocery_json` pot stoca raspunsurile complete in MVP.
- Tabelele normalizate sunt utile pentru query/display mai tarziu.
- Normalizarea completa poate fi amanata pana cand contractul API si UI-ul mobil se stabilizeaza.
- Schema este pentru SQLite local MVP/preview, nu pentru cloud production DB.
- Auth-M1 nu adauga email verification, password reset, email sending sau productie-grade auth claims.

## Non-goals

- Nu defineste autentificare complexa/cloud.
- Nu defineste sincronizare cloud.
- Nu defineste migratii production-grade.
- Nu modifica Generator v1 sau fisierele de date curente.
