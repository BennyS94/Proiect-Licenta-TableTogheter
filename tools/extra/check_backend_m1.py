from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m1_smoke_summary.txt"
REQUIRED_TABLES = {
    "households",
    "member_profiles",
    "feedback_events",
    "generated_plans",
    "generated_plan_days",
    "generated_plan_meals",
    "grocery_lists",
    "grocery_list_items",
}


def _load_app_status() -> tuple[bool, str, object | None]:
    try:
        from backend.app.main import app

        return True, "ok", app
    except ModuleNotFoundError as exc:
        if exc.name == "fastapi":
            return False, "missing_fastapi", None
        return False, f"module_import_error:{exc}", None
    except Exception as exc:
        return False, f"import_error:{type(exc).__name__}:{exc}", None


def _get_existing_tables(db_path: Path) -> set[str]:
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    return {row[0] for row in rows}


def _call_health_with_test_client(app: object | None) -> dict[str, object]:
    if app is None:
        return {
            "called": False,
            "ok": False,
            "status_code": None,
            "result": "skipped_app_not_imported",
        }
    try:
        from fastapi.testclient import TestClient
    except Exception as exc:
        return {
            "called": False,
            "ok": False,
            "status_code": None,
            "result": f"skipped_testclient_unavailable:{type(exc).__name__}",
        }

    try:
        client = TestClient(app)
        response = client.get("/health")
        payload = response.json()
        payload_ok = (
            response.status_code == 200
            and payload.get("status") == "ok"
            and payload.get("service") == "tabletogether-api"
            and payload.get("version") == "v1"
            and payload.get("database") in {"ok", "not_initialized"}
        )
        return {
            "called": True,
            "ok": payload_ok,
            "status_code": response.status_code,
            "result": payload,
        }
    except Exception as exc:
        return {
            "called": True,
            "ok": False,
            "status_code": None,
            "result": f"testclient_error:{type(exc).__name__}:{exc}",
        }


def main() -> int:
    from backend.app.db.database import check_db_connection, get_sqlite_path, init_db

    app_imported, app_status, app = _load_app_status()
    db_path = init_db()
    db_exists = db_path.exists()
    db_connection_ok = check_db_connection()
    existing_tables = _get_existing_tables(db_path)
    missing_tables = sorted(REQUIRED_TABLES - existing_tables)
    tables_ok = not missing_tables
    health_result = _call_health_with_test_client(app)
    fastapi_smoke_ok = (
        not app_imported
        and app_status == "missing_fastapi"
    ) or bool(health_result["ok"])

    summary_lines = [
        "Backend M1 smoke summary",
        "status=ok"
        if db_exists and db_connection_ok and tables_ok and fastapi_smoke_ok
        else "status=failed",
        f"app_imported={str(app_imported).lower()}",
        f"app_status={app_status}",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"db_exists={str(db_exists).lower()}",
        f"db_connection_ok={str(db_connection_ok).lower()}",
        f"required_table_count={len(REQUIRED_TABLES)}",
        f"existing_required_table_count={len(REQUIRED_TABLES) - len(missing_tables)}",
        f"missing_tables={','.join(missing_tables) if missing_tables else 'none'}",
        f"health_testclient_called={str(health_result['called']).lower()}",
        f"health_testclient_ok={str(health_result['ok']).lower()}",
        f"health_status_code={health_result['status_code']}",
        f"health_result={health_result['result']}",
    ]

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))

    if not (db_exists and db_connection_ok and tables_ok and fastapi_smoke_ok):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
