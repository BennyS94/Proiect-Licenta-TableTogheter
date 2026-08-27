from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_auth_m1_summary.txt"
SAMPLE_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_auth_m1_response_sample.json"

EMAIL = "auth_m1_test@example.com"
PASSWORD = "Secret123"


def _cleanup_previous_test_rows(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        user_rows = conn.execute(
            "SELECT user_id FROM users WHERE email = ?",
            (EMAIL,),
        ).fetchall()
        user_ids = [row[0] for row in user_rows]
        household_rows: list[tuple[str]] = []
        if user_ids:
            placeholders = ",".join("?" for _ in user_ids)
            household_rows = conn.execute(
                f"SELECT household_id FROM households WHERE user_id IN ({placeholders})",
                user_ids,
            ).fetchall()
            conn.execute(
                f"DELETE FROM user_sessions WHERE user_id IN ({placeholders})",
                user_ids,
            )
            conn.execute(
                f"DELETE FROM users WHERE user_id IN ({placeholders})",
                user_ids,
            )
        household_ids = [row[0] for row in household_rows]
        if household_ids:
            placeholders = ",".join("?" for _ in household_ids)
            conn.execute(
                f"DELETE FROM member_profiles WHERE household_id IN ({placeholders})",
                household_ids,
            )
            conn.execute(
                f"DELETE FROM households WHERE household_id IN ({placeholders})",
                household_ids,
            )


def _profile_request() -> dict[str, Any]:
    return {
        "display_name": "Auth Smoke Alex",
        "age": 35,
        "sex": "male",
        "weight_kg": 82.0,
        "height_cm": 178.0,
        "activity_level": "moderately_active",
        "goal": "maintain",
        "goal_speed": "normal",
        "training": {
            "sessions_per_week": 3,
            "type": "mixed",
        },
        "meal_config": {
            "meals_per_day": 3,
            "include_snacks": True,
            "day_structure": "3_meals_plus_snack",
        },
        "dietary_preferences": {
            "no_beef": False,
            "no_chicken": False,
            "no_fish": False,
            "no_dairy": False,
            "vegetarian": False,
            "vegan": False,
            "gluten_free": False,
        },
        "bf_profile": "normal",
    }


def _user_row(db_path: Path, email: str) -> dict[str, Any] | None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                user_id,
                email,
                password_hash,
                password_salt,
                is_active
            FROM users
            WHERE email = ?
            """,
            (email,),
        ).fetchone()
    if row is None:
        return None
    return {
        "user_id": row[0],
        "email": row[1],
        "password_hash": row[2],
        "password_salt": row[3],
        "is_active": bool(row[4]),
    }


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    db_path = init_db()
    _cleanup_previous_test_rows(db_path)

    client = TestClient(app)
    errors: list[str] = []

    register_response = client.post(
        "/auth/register",
        json={
            "email": EMAIL,
            "password": PASSWORD,
            "confirm_password": PASSWORD,
        },
    )
    register_payload = register_response.json()
    token = str(register_payload.get("session_token") or "")
    account = register_payload.get("account") if isinstance(register_payload.get("account"), dict) else {}
    household_id = str(account.get("household_id") or "")

    if register_response.status_code != 200:
        errors.append(f"register_status={register_response.status_code}")
    if register_payload.get("status") != "ok":
        errors.append(f"register_payload_status={register_payload.get('status')}")
    if register_payload.get("message") != "Account created":
        errors.append("register_message_mismatch")
    if not token:
        errors.append("register_missing_session_token")
    if not household_id:
        errors.append("register_missing_household_id")

    user = _user_row(db_path, EMAIL)
    if user is None:
        errors.append("user_row_missing")
    else:
        if not user["password_hash"]:
            errors.append("password_hash_missing")
        if not user["password_salt"]:
            errors.append("password_salt_missing")
        if PASSWORD in json.dumps(user, ensure_ascii=True):
            errors.append("plaintext_password_found_in_users_row")

    duplicate_response = client.post(
        "/auth/register",
        json={
            "email": EMAIL,
            "password": PASSWORD,
            "confirm_password": PASSWORD,
        },
    )
    if duplicate_response.status_code not in {400, 409}:
        errors.append(f"duplicate_status={duplicate_response.status_code}")

    wrong_login_response = client.post(
        "/auth/login",
        json={
            "email": EMAIL,
            "password": "Wrong123",
        },
    )
    if wrong_login_response.status_code != 401:
        errors.append(f"wrong_login_status={wrong_login_response.status_code}")

    login_response = client.post(
        "/auth/login",
        json={
            "email": EMAIL,
            "password": PASSWORD,
        },
    )
    login_payload = login_response.json()
    login_token = str(login_payload.get("session_token") or "")
    if login_response.status_code != 200:
        errors.append(f"login_status={login_response.status_code}")
    if login_payload.get("status") != "ok":
        errors.append(f"login_payload_status={login_payload.get('status')}")
    if not login_token:
        errors.append("login_missing_session_token")

    me_response = client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {login_token}"},
    )
    me_payload = me_response.json()
    if me_response.status_code != 200:
        errors.append(f"me_status={me_response.status_code}")
    if me_payload.get("status") != "ok":
        errors.append(f"me_payload_status={me_payload.get('status')}")

    profile_response = client.post(
        "/profiles",
        json=_profile_request(),
        headers={"Authorization": f"Bearer {login_token}"},
    )
    profile_payload = profile_response.json()
    if profile_response.status_code != 200:
        errors.append(f"profile_post_status={profile_response.status_code}")
    if profile_payload.get("household_id") != household_id:
        errors.append("profile_household_scope_mismatch")

    profiles_response = client.get(
        "/profiles",
        headers={"Authorization": f"Bearer {login_token}"},
    )
    profiles_payload = profiles_response.json()
    profiles = profiles_payload.get("profiles") if isinstance(profiles_payload, dict) else []
    if profiles_response.status_code != 200:
        errors.append(f"profiles_get_status={profiles_response.status_code}")
    if profiles_payload.get("household_id") != household_id:
        errors.append("profiles_list_household_scope_mismatch")
    if not isinstance(profiles, list) or len(profiles) != 1:
        errors.append("profiles_list_count_mismatch")

    logout_response = client.post(
        "/auth/logout",
        json={"session_token": login_token},
    )
    if logout_response.status_code != 200:
        errors.append(f"logout_status={logout_response.status_code}")

    me_after_logout_response = client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {login_token}"},
    )
    if me_after_logout_response.status_code != 401:
        errors.append(f"me_after_logout_status={me_after_logout_response.status_code}")

    sample = {
        "register": {
            "status_code": register_response.status_code,
            "status": register_payload.get("status"),
            "message": register_payload.get("message"),
            "session_token_returned": bool(token),
            "account": register_payload.get("account"),
        },
        "duplicate": {
            "status_code": duplicate_response.status_code,
            "detail": duplicate_response.json().get("detail"),
        },
        "login": {
            "status_code": login_response.status_code,
            "status": login_payload.get("status"),
            "message": login_payload.get("message"),
            "session_token_returned": bool(login_token),
            "account": login_payload.get("account"),
        },
        "me": {
            "status_code": me_response.status_code,
            "status": me_payload.get("status"),
            "account": me_payload.get("account"),
        },
        "profile": {
            "status_code": profile_response.status_code,
            "member_profile_id": profile_payload.get("member_profile_id"),
            "household_id": profile_payload.get("household_id"),
        },
        "logout": {
            "status_code": logout_response.status_code,
            "me_after_logout_status": me_after_logout_response.status_code,
        },
    }
    SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SAMPLE_PATH.write_text(json.dumps(sample, indent=2, ensure_ascii=True), encoding="utf-8")

    status_ok = not errors
    summary_lines = [
        "Backend Auth-M1 smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"register_status={register_response.status_code}",
        f"register_message={register_payload.get('message')}",
        f"session_token_returned={str(bool(token)).lower()}",
        f"household_id_returned={str(bool(household_id)).lower()}",
        f"user_row_exists={str(user is not None).lower()}",
        f"password_hash_exists={str(bool(user and user.get('password_hash'))).lower()}",
        f"password_salt_exists={str(bool(user and user.get('password_salt'))).lower()}",
        f"plaintext_password_found={str(bool(user and PASSWORD in json.dumps(user))).lower()}",
        f"duplicate_status={duplicate_response.status_code}",
        f"wrong_login_status={wrong_login_response.status_code}",
        f"login_status={login_response.status_code}",
        f"me_status={me_response.status_code}",
        f"profile_post_status={profile_response.status_code}",
        f"profile_household_id={profile_payload.get('household_id')}",
        f"profiles_get_status={profiles_response.status_code}",
        f"profiles_count={len(profiles) if isinstance(profiles, list) else 'invalid'}",
        f"logout_status={logout_response.status_code}",
        f"me_after_logout_status={me_after_logout_response.status_code}",
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
