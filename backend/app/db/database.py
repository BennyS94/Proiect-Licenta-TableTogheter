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
