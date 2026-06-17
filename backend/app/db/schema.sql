CREATE TABLE IF NOT EXISTS households (
    household_id TEXT PRIMARY KEY,
    user_id TEXT,
    household_name TEXT NOT NULL,
    display_name TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    settings_json TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    password_salt TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    session_token_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT,
    is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS member_profiles (
    member_profile_id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    age INTEGER NOT NULL,
    sex TEXT NOT NULL,
    weight_kg REAL NOT NULL,
    height_cm REAL NOT NULL,
    activity_level TEXT NOT NULL,
    goal TEXT NOT NULL,
    goal_speed TEXT NOT NULL,
    training_json TEXT NOT NULL,
    meal_config_json TEXT NOT NULL,
    dietary_preferences_json TEXT NOT NULL,
    food_preferences_json TEXT NOT NULL DEFAULT '{}',
    health_and_diet_preferences_json TEXT NOT NULL DEFAULT '{}',
    is_active INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS feedback_events (
    event_id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    member_profile_id TEXT,
    recipe_id TEXT NOT NULL,
    plan_id TEXT,
    slot TEXT,
    feedback_type TEXT NOT NULL,
    notes TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS generated_plans (
    plan_id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    member_profile_id TEXT,
    generation_type TEXT NOT NULL,
    dataset_profile TEXT NOT NULL,
    days INTEGER NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS generated_plan_days (
    plan_day_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    day_index INTEGER NOT NULL,
    kcal_total REAL,
    protein_total REAL,
    carbs_total REAL,
    fat_total REAL,
    validation_status TEXT,
    quality_status TEXT
);

CREATE TABLE IF NOT EXISTS generated_plan_meals (
    plan_meal_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    day_index INTEGER NOT NULL,
    slot TEXT NOT NULL,
    recipe_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    portion_multiplier REAL,
    kcal REAL,
    protein_g REAL,
    carbs_g REAL,
    fat_g REAL,
    meal_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS grocery_lists (
    grocery_list_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    household_id TEXT NOT NULL,
    total_estimated_cost REAL,
    currency TEXT,
    grocery_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS grocery_list_items (
    grocery_item_id TEXT PRIMARY KEY,
    grocery_list_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    category TEXT,
    needed_grams REAL,
    purchase_display TEXT,
    estimated_cost REAL,
    currency TEXT,
    item_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS saved_daily_progress (
    progress_id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    member_profile_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    day_index INTEGER NOT NULL,
    saved_at TEXT NOT NULL,
    planned_kcal REAL,
    planned_protein_g REAL,
    planned_carbs_g REAL,
    planned_fat_g REAL,
    consumed_kcal REAL,
    consumed_protein_g REAL,
    consumed_carbs_g REAL,
    consumed_fat_g REAL,
    meal_completion_json TEXT NOT NULL,
    day_snapshot_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(member_profile_id, plan_id, day_index)
);

CREATE INDEX IF NOT EXISTS idx_saved_daily_progress_profile_saved_at
ON saved_daily_progress(member_profile_id, saved_at DESC);

CREATE INDEX IF NOT EXISTS idx_saved_daily_progress_household
ON saved_daily_progress(household_id, member_profile_id);
