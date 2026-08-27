from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/progress_daily_snapshots_summary.txt"
SAMPLE_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/progress_daily_snapshots_sample.json"

EMAIL_ONE = "progress_daily_check_one@tabletogether.test"
EMAIL_TWO = "progress_daily_check_two@tabletogether.test"
PASSWORD = "Secret123"


def _cleanup_previous_test_rows(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        user_rows = conn.execute(
            "SELECT user_id FROM users WHERE email IN (?, ?)",
            (EMAIL_ONE, EMAIL_TWO),
        ).fetchall()
        user_ids = [row[0] for row in user_rows]
        if not user_ids:
            return
        user_placeholders = ",".join("?" for _ in user_ids)
        household_rows = conn.execute(
            f"SELECT household_id FROM households WHERE user_id IN ({user_placeholders})",
            user_ids,
        ).fetchall()
        household_ids = [row[0] for row in household_rows]
        if household_ids:
            household_placeholders = ",".join("?" for _ in household_ids)
            conn.execute(
                f"DELETE FROM saved_daily_progress WHERE household_id IN ({household_placeholders})",
                household_ids,
            )
            conn.execute(
                f"DELETE FROM generated_plan_meals WHERE plan_id IN (SELECT plan_id FROM generated_plans WHERE household_id IN ({household_placeholders}))",
                household_ids,
            )
            conn.execute(
                f"DELETE FROM generated_plan_days WHERE plan_id IN (SELECT plan_id FROM generated_plans WHERE household_id IN ({household_placeholders}))",
                household_ids,
            )
            conn.execute(
                f"DELETE FROM grocery_list_items WHERE grocery_list_id IN (SELECT grocery_list_id FROM grocery_lists WHERE household_id IN ({household_placeholders}))",
                household_ids,
            )
            conn.execute(
                f"DELETE FROM grocery_lists WHERE household_id IN ({household_placeholders})",
                household_ids,
            )
            conn.execute(
                f"DELETE FROM generated_plans WHERE household_id IN ({household_placeholders})",
                household_ids,
            )
            conn.execute(
                f"DELETE FROM member_profiles WHERE household_id IN ({household_placeholders})",
                household_ids,
            )
            conn.execute(
                f"DELETE FROM households WHERE household_id IN ({household_placeholders})",
                household_ids,
            )
        conn.execute(
            f"DELETE FROM user_sessions WHERE user_id IN ({user_placeholders})",
            user_ids,
        )
        conn.execute(
            f"DELETE FROM users WHERE user_id IN ({user_placeholders})",
            user_ids,
        )


def _profile_request(display_name: str) -> dict[str, Any]:
    return {
        "display_name": display_name,
        "age": 34,
        "sex": "female",
        "weight_kg": 64.0,
        "height_cm": 168.0,
        "activity_level": "moderately_active",
        "goal": "maintain",
        "goal_speed": "normal",
        "training": {"sessions_per_week": 3, "type": "mixed"},
        "meal_config": {
            "meals_per_day": 3,
            "include_snacks": True,
            "day_structure": "3_meals_plus_snack",
        },
        "dietary_preferences": {
            "vegetarian": False,
            "vegan": False,
            "gluten_free": False,
            "no_beef": False,
            "no_pork": False,
            "no_chicken": False,
            "no_fish": False,
            "no_dairy": False,
        },
        "bf_profile": "normal",
    }


def _progress_request(
    household_id: str,
    member_profile_id: str,
    plan_id: str,
    *,
    day_index: int = 1,
) -> dict[str, Any]:
    return {
        "household_id": household_id,
        "member_profile_id": member_profile_id,
        "plan_id": plan_id,
        "day_index": day_index,
        "planned": {
            "kcal": 2100,
            "protein_g": 140,
            "carbs_g": 240,
            "fat_g": 70,
        },
        "consumed": {
            "kcal": 650 + day_index,
            "protein_g": 45,
            "carbs_g": 70,
            "fat_g": 22,
        },
        "meal_completion": {
            "completed_meal_keys": ["breakfast"],
            "meals": [{"slot": "breakfast", "eaten": True, "recipe_id": "recipe_check"}],
        },
        "day_snapshot": {
            "selected_day": day_index,
            "source": "check_progress_daily_snapshots",
        },
    }


def _register(client: Any, email: str) -> tuple[str, str]:
    response = client.post(
        "/auth/register",
        json={"email": email, "password": PASSWORD, "confirm_password": PASSWORD},
    )
    payload = response.json()
    if response.status_code != 200:
        raise RuntimeError(f"register_failed:{email}:{response.status_code}:{payload}")
    account = payload.get("account") if isinstance(payload.get("account"), dict) else {}
    return str(payload.get("session_token") or ""), str(account.get("household_id") or "")


def _create_profile(client: Any, token: str, display_name: str) -> str:
    response = client.post(
        "/profiles",
        headers={"Authorization": f"Bearer {token}"},
        json=_profile_request(display_name),
    )
    payload = response.json()
    if response.status_code != 200:
        raise RuntimeError(f"profile_failed:{response.status_code}:{payload}")
    return str(payload.get("member_profile_id") or "")


def _insert_fake_plan(
    db_path: Path,
    *,
    household_id: str,
    member_profile_id: str,
    plan_id: str,
) -> None:
    from backend.app.db.repositories import save_generated_plan

    response_json = {
        "status": "ok",
        "plan_id": plan_id,
        "household_id": household_id,
        "member_profile_id": member_profile_id,
        "daily_plan": [
            {
                "day_index": 1,
                "totals": {
                    "kcal": 2100,
                    "protein_g": 140,
                    "carbs_g": 240,
                    "fat_g": 70,
                },
                "selected_meals": [],
            }
        ],
    }
    with sqlite3.connect(db_path) as conn:
        save_generated_plan(
            conn,
            plan_id=plan_id,
            household_id=household_id,
            member_profile_id=member_profile_id,
            generation_type="individual",
            dataset_profile="current",
            days=1,
            request_json={"source": "check_progress_daily_snapshots"},
            response_json=response_json,
        )


def _progress_count(db_path: Path, member_profile_id: str, plan_id: str, day_index: int) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM saved_daily_progress
            WHERE member_profile_id = ?
              AND plan_id = ?
              AND day_index = ?
            """,
            (member_profile_id, plan_id, day_index),
        ).fetchone()
    return int(row[0] if row else 0)


def _profile_progress_total(db_path: Path, member_profile_id: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM saved_daily_progress WHERE member_profile_id = ?",
            (member_profile_id,),
        ).fetchone()
    return int(row[0] if row else 0)


def _table_columns(db_path: Path, table_name: str) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    db_path = init_db()
    _cleanup_previous_test_rows(db_path)

    client = TestClient(app)
    errors: list[str] = []

    required_columns = {
        "progress_id",
        "household_id",
        "member_profile_id",
        "plan_id",
        "day_index",
        "saved_at",
        "meal_completion_json",
        "day_snapshot_json",
    }
    table_columns = _table_columns(db_path, "saved_daily_progress")
    if not required_columns.issubset(table_columns):
        errors.append("schema_columns_missing")

    token_one, household_one = _register(client, EMAIL_ONE)
    profile_one = _create_profile(client, token_one, "Progress Check One")
    plan_one = "progress_check_plan_one"
    _insert_fake_plan(
        db_path,
        household_id=household_one,
        member_profile_id=profile_one,
        plan_id=plan_one,
    )

    save_response = client.post(
        "/progress/daily",
        headers={"Authorization": f"Bearer {token_one}"},
        json=_progress_request(household_one, profile_one, plan_one),
    )
    save_payload = save_response.json()
    snapshot = save_payload.get("snapshot") if isinstance(save_payload, dict) else {}
    progress_id = str(snapshot.get("progress_id") or "") if isinstance(snapshot, dict) else ""
    if save_response.status_code != 200:
        errors.append(f"save_status={save_response.status_code}")
    if save_payload.get("status") != "saved":
        errors.append(f"save_payload_status={save_payload.get('status')}")
    if not progress_id:
        errors.append("save_missing_progress_id")

    duplicate_response = client.post(
        "/progress/daily",
        headers={"Authorization": f"Bearer {token_one}"},
        json=_progress_request(household_one, profile_one, plan_one),
    )
    duplicate_payload = duplicate_response.json()
    if duplicate_response.status_code != 200:
        errors.append(f"duplicate_status={duplicate_response.status_code}")
    if duplicate_payload.get("status") != "already_saved":
        errors.append(f"duplicate_payload_status={duplicate_payload.get('status')}")
    duplicate_row_count_after_duplicate = _progress_count(db_path, profile_one, plan_one, 1)
    if duplicate_row_count_after_duplicate != 1:
        errors.append("duplicate_created_extra_row")

    list_response = client.get(
        f"/progress/daily?member_profile_id={profile_one}",
        headers={"Authorization": f"Bearer {token_one}"},
    )
    list_payload = list_response.json()
    snapshots = list_payload.get("snapshots") if isinstance(list_payload, dict) else []
    if list_response.status_code != 200:
        errors.append(f"list_status={list_response.status_code}")
    if not isinstance(snapshots, list) or not snapshots:
        errors.append("list_missing_saved_snapshot")

    token_two, household_two = _register(client, EMAIL_TWO)
    profile_two = _create_profile(client, token_two, "Progress Check Two")
    plan_two = "progress_check_plan_two"
    _insert_fake_plan(
        db_path,
        household_id=household_two,
        member_profile_id=profile_two,
        plan_id=plan_two,
    )
    other_scope_response = client.get(
        f"/progress/daily?member_profile_id={profile_one}",
        headers={"Authorization": f"Bearer {token_two}"},
    )
    if other_scope_response.status_code != 404:
        errors.append(f"cross_household_profile_read_status={other_scope_response.status_code}")
    other_delete_response = client.delete(
        f"/progress/daily/{progress_id}",
        headers={"Authorization": f"Bearer {token_two}"},
    )
    if other_delete_response.status_code != 404:
        errors.append(f"cross_household_delete_status={other_delete_response.status_code}")

    for index in range(31):
        plan_id = f"progress_check_plan_extra_{index:02d}"
        _insert_fake_plan(
            db_path,
            household_id=household_one,
            member_profile_id=profile_one,
            plan_id=plan_id,
        )
        response = client.post(
            "/progress/daily",
            headers={"Authorization": f"Bearer {token_one}"},
            json=_progress_request(household_one, profile_one, plan_id, day_index=1),
        )
        if response.status_code != 200:
            errors.append(f"max_save_status_{index}={response.status_code}")
            break
    profile_total = _profile_progress_total(db_path, profile_one)
    if profile_total > 30:
        errors.append(f"profile_progress_count_over_30={profile_total}")

    delete_target_response = client.get(
        f"/progress/daily?member_profile_id={profile_one}",
        headers={"Authorization": f"Bearer {token_one}"},
    )
    delete_snapshots = delete_target_response.json().get("snapshots", [])
    delete_target_id = str(delete_snapshots[0].get("progress_id") or "") if delete_snapshots else ""
    delete_response = client.delete(
        f"/progress/daily/{delete_target_id}",
        headers={"Authorization": f"Bearer {token_one}"},
    )
    if delete_response.status_code != 200:
        errors.append(f"delete_status={delete_response.status_code}")
    if delete_response.json().get("deleted") is not True:
        errors.append("delete_payload_not_deleted")

    health_response = client.get("/health")
    profiles_response = client.get(
        "/profiles",
        headers={"Authorization": f"Bearer {token_one}"},
    )
    if health_response.status_code != 200:
        errors.append(f"health_status={health_response.status_code}")
    if profiles_response.status_code != 200:
        errors.append(f"profiles_status={profiles_response.status_code}")

    sample = {
        "save": {
            "status_code": save_response.status_code,
            "status": save_payload.get("status"),
            "snapshot": snapshot,
        },
        "duplicate": {
            "status_code": duplicate_response.status_code,
            "status": duplicate_payload.get("status"),
        },
        "list_count_after_max": profile_total,
        "cross_household": {
            "read_status": other_scope_response.status_code,
            "delete_status": other_delete_response.status_code,
        },
        "delete": {
            "status_code": delete_response.status_code,
            "payload": delete_response.json(),
        },
    }
    SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SAMPLE_PATH.write_text(json.dumps(sample, indent=2, ensure_ascii=True), encoding="utf-8")

    status_ok = not errors
    summary_lines = [
        "Progress daily snapshots summary",
        "status=ok" if status_ok else "status=failed",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"schema_columns_present={str(required_columns.issubset(table_columns)).lower()}",
        f"save_status={save_response.status_code}",
        f"duplicate_status={duplicate_response.status_code}",
        f"duplicate_row_count_after_duplicate={duplicate_row_count_after_duplicate}",
        f"list_status={list_response.status_code}",
        f"cross_household_profile_read_status={other_scope_response.status_code}",
        f"cross_household_delete_status={other_delete_response.status_code}",
        f"profile_progress_count_after_max={profile_total}",
        f"delete_status={delete_response.status_code}",
        f"health_status={health_response.status_code}",
        f"profiles_status={profiles_response.status_code}",
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
