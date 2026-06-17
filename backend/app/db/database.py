import sqlite3
from pathlib import Path

from backend.app.core.config import DEFAULT_SQLITE_PATH


SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def get_sqlite_path() -> Path:
    return DEFAULT_SQLITE_PATH


def ensure_runtime_dir() -> Path:
    sqlite_path = get_sqlite_path()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite_path.parent


def get_connection() -> sqlite3.Connection:
    ensure_runtime_dir()
    return sqlite3.connect(get_sqlite_path())


def init_db() -> Path:
    ensure_runtime_dir()
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with get_connection() as connection:
        connection.executescript(schema_sql)
        _ensure_household_auth_columns(connection)
        _ensure_member_profile_preference_columns(connection)
        _ensure_saved_daily_progress_target_columns(connection)
    return get_sqlite_path()


def check_db_connection() -> bool:
    sqlite_path = get_sqlite_path()
    if not sqlite_path.exists():
        return False
    try:
        with sqlite3.connect(sqlite_path) as connection:
            connection.execute("SELECT 1")
        return True
    except sqlite3.Error:
        return False


def _ensure_household_auth_columns(connection: sqlite3.Connection) -> None:
    existing_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(households)").fetchall()
    }
    column_sql = {
        "user_id": "ALTER TABLE households ADD COLUMN user_id TEXT",
        "display_name": "ALTER TABLE households ADD COLUMN display_name TEXT",
        "is_active": "ALTER TABLE households ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1",
    }
    for column_name, statement in column_sql.items():
        if column_name not in existing_columns:
            connection.execute(statement)


def _ensure_member_profile_preference_columns(connection: sqlite3.Connection) -> None:
    existing_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(member_profiles)").fetchall()
    }
    column_sql = {
        "food_preferences_json": (
            "ALTER TABLE member_profiles "
            "ADD COLUMN food_preferences_json TEXT NOT NULL DEFAULT '{}'"
        ),
        "health_and_diet_preferences_json": (
            "ALTER TABLE member_profiles "
            "ADD COLUMN health_and_diet_preferences_json TEXT NOT NULL DEFAULT '{}'"
        ),
    }
    for column_name, statement in column_sql.items():
        if column_name not in existing_columns:
            connection.execute(statement)


def _ensure_saved_daily_progress_target_columns(connection: sqlite3.Connection) -> None:
    existing_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(saved_daily_progress)").fetchall()
    }
    column_sql = {
        "target_kcal": "ALTER TABLE saved_daily_progress ADD COLUMN target_kcal REAL",
        "target_protein_g": (
            "ALTER TABLE saved_daily_progress ADD COLUMN target_protein_g REAL"
        ),
        "target_carbs_g": "ALTER TABLE saved_daily_progress ADD COLUMN target_carbs_g REAL",
        "target_fat_g": "ALTER TABLE saved_daily_progress ADD COLUMN target_fat_g REAL",
    }
    added_columns = False
    for column_name, statement in column_sql.items():
        if column_name not in existing_columns:
            connection.execute(statement)
            added_columns = True
    if added_columns:
        connection.execute(
            """
            UPDATE saved_daily_progress
            SET
                target_kcal = COALESCE(target_kcal, planned_kcal),
                target_protein_g = COALESCE(target_protein_g, planned_protein_g),
                target_carbs_g = COALESCE(target_carbs_g, planned_carbs_g),
                target_fat_g = COALESCE(target_fat_g, planned_fat_g)
            """
        )
