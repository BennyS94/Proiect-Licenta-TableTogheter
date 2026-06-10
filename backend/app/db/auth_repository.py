from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.app.db.database import get_connection, init_db


PBKDF2_ITERATIONS = 120_000


def create_user_with_household(email: str, password: str) -> dict[str, Any]:
    normalized_email = _normalize_email(email)
    _validate_email(normalized_email)
    _validate_password(password)

    init_db()
    with get_connection() as conn:
        if _get_user_by_email(conn, normalized_email) is not None:
            raise ValueError("email_already_exists")

        now = _utc_now_iso()
        user_id = _new_id("user")
        household_id = _new_id("household")
        salt = secrets.token_hex(16)
        password_hash = _hash_password(password, salt)

        conn.execute(
            """
            INSERT INTO users (
                user_id,
                email,
                password_hash,
                password_salt,
                created_at,
                updated_at,
                is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, 1)
            """,
            (user_id, normalized_email, password_hash, salt, now, now),
        )
        conn.execute(
            """
            INSERT INTO households (
                household_id,
                user_id,
                household_name,
                display_name,
                created_at,
                updated_at,
                settings_json,
                is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                household_id,
                user_id,
                "My Household",
                "My Household",
                now,
                now,
                json.dumps({}, sort_keys=True),
            ),
        )

        return {
            "user_id": user_id,
            "email": normalized_email,
            "household_id": household_id,
            "household_display_name": "My Household",
        }


def get_user_by_email(email: str) -> dict[str, Any] | None:
    init_db()
    with get_connection() as conn:
        return _get_user_by_email(conn, _normalize_email(email))


def verify_user_password(email: str, password: str) -> dict[str, Any] | None:
    normalized_email = _normalize_email(email)
    init_db()
    with get_connection() as conn:
        user = _get_user_by_email(conn, normalized_email)
        if user is None or not user.get("is_active"):
            return None
        expected_hash = _hash_password(password, str(user["password_salt"]))
        if not hmac.compare_digest(expected_hash, str(user["password_hash"])):
            return None
        household = _get_household_for_user(conn, str(user["user_id"]))
        return _account_from_user_and_household(user, household)


def create_user_session(user_id: str) -> dict[str, Any]:
    init_db()
    with get_connection() as conn:
        user = _get_user_by_id(conn, user_id)
        if user is None or not user.get("is_active"):
            raise ValueError("user_not_found")
        token = secrets.token_hex(32)
        session = _create_user_session(conn, user_id, token)
        session["session_token"] = token
        return session


def revoke_user_session(session_token: str) -> bool:
    token_hash = _hash_session_token(session_token)
    if not token_hash:
        return False
    init_db()
    with get_connection() as conn:
        now = _utc_now_iso()
        cursor = conn.execute(
            """
            UPDATE user_sessions
            SET is_active = 0,
                revoked_at = ?
            WHERE session_token_hash = ?
              AND is_active = 1
              AND revoked_at IS NULL
            """,
            (now, token_hash),
        )
        return bool(cursor.rowcount)


def get_session_context(session_token: str) -> dict[str, Any] | None:
    token_hash = _hash_session_token(session_token)
    if not token_hash:
        return None
    init_db()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                s.session_id,
                s.user_id,
                s.created_at,
                s.expires_at,
                u.email,
                u.is_active
            FROM user_sessions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.session_token_hash = ?
              AND s.is_active = 1
              AND s.revoked_at IS NULL
            """,
            (token_hash,),
        ).fetchone()
        if row is None or not bool(row[5]):
            return None
        if row[3] and str(row[3]) <= _utc_now_iso():
            return None
        user = {
            "user_id": row[1],
            "email": row[4],
            "is_active": bool(row[5]),
        }
        household = _get_household_for_user(conn, str(row[1]))
        account = _account_from_user_and_household(user, household)
        return {
            "session_id": row[0],
            "user_id": row[1],
            "email": row[4],
            "household_id": account["household_id"],
            "household_display_name": account["household_display_name"],
            "account": account,
        }


def get_household_for_user(user_id: str) -> dict[str, Any] | None:
    init_db()
    with get_connection() as conn:
        return _get_household_for_user(conn, user_id)


def update_household_display_name(user_id: str, display_name: str) -> dict[str, Any]:
    clean_user_id = str(user_id or "").strip()
    clean_display_name = _normalize_household_display_name(display_name)
    init_db()
    with get_connection() as conn:
        user = _get_user_by_id(conn, clean_user_id)
        if user is None or not user.get("is_active"):
            raise ValueError("user_not_found")
        household = _get_household_for_user(conn, clean_user_id)
        if household is None:
            raise ValueError("household_not_found")

        now = _utc_now_iso()
        conn.execute(
            """
            UPDATE households
            SET household_name = ?,
                display_name = ?,
                updated_at = ?,
                is_active = 1
            WHERE household_id = ?
              AND user_id = ?
            """,
            (
                clean_display_name,
                clean_display_name,
                now,
                household["household_id"],
                clean_user_id,
            ),
        )
        updated_household = _get_household_for_user(conn, clean_user_id)
        return _account_from_user_and_household(user, updated_household)


def create_session_for_account(account: dict[str, Any]) -> dict[str, Any]:
    session = create_user_session(str(account["user_id"]))
    return {
        "session_token": session["session_token"],
        "session_id": session["session_id"],
    }


def _create_user_session(
    conn: sqlite3.Connection,
    user_id: str,
    session_token: str,
) -> dict[str, Any]:
    now = _utc_now_iso()
    session_id = _new_id("session")
    conn.execute(
        """
        INSERT INTO user_sessions (
            session_id,
            user_id,
            session_token_hash,
            created_at,
            expires_at,
            revoked_at,
            is_active
        )
        VALUES (?, ?, ?, ?, NULL, NULL, 1)
        """,
        (
            session_id,
            user_id,
            _hash_session_token(session_token),
            now,
        ),
    )
    return {
        "session_id": session_id,
        "user_id": user_id,
        "created_at": now,
        "expires_at": None,
    }


def _get_user_by_email(conn: sqlite3.Connection, email: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            user_id,
            email,
            password_hash,
            password_salt,
            created_at,
            updated_at,
            is_active
        FROM users
        WHERE email = ?
        """,
        (email,),
    ).fetchone()
    return _user_row_to_dict(row) if row else None


def _get_user_by_id(conn: sqlite3.Connection, user_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            user_id,
            email,
            password_hash,
            password_salt,
            created_at,
            updated_at,
            is_active
        FROM users
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()
    return _user_row_to_dict(row) if row else None


def _get_household_for_user(
    conn: sqlite3.Connection,
    user_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            household_id,
            user_id,
            household_name,
            display_name,
            created_at,
            updated_at,
            settings_json,
            is_active
        FROM households
        WHERE user_id = ?
          AND is_active = 1
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    if row is None:
        return None
    display_name = row[3] or row[2] or "My Household"
    return {
        "household_id": row[0],
        "user_id": row[1],
        "household_name": row[2],
        "display_name": display_name,
        "household_display_name": display_name,
        "created_at": row[4],
        "updated_at": row[5],
        "settings": _json_loads(row[6]),
        "is_active": bool(row[7]),
    }


def _account_from_user_and_household(
    user: dict[str, Any],
    household: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "user_id": str(user["user_id"]),
        "email": str(user["email"]),
        "household_id": str(household["household_id"]) if household else "",
        "household_display_name": (
            str(household.get("household_display_name") or household.get("display_name"))
            if household
            else "My Household"
        ),
    }


def _user_row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "user_id": row[0],
        "email": row[1],
        "password_hash": row[2],
        "password_salt": row[3],
        "created_at": row[4],
        "updated_at": row[5],
        "is_active": bool(row[6]),
    }


def _normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def _validate_email(email: str) -> None:
    if not email or "@" not in email:
        raise ValueError("email_invalid")


def _validate_password(password: str) -> None:
    if len(str(password or "")) < 6:
        raise ValueError("password_min_6")


def _normalize_household_display_name(display_name: str) -> str:
    clean_name = " ".join(str(display_name or "").strip().split())
    if not clean_name:
        raise ValueError("household_name_required")
    if len(clean_name) > 64:
        raise ValueError("household_name_too_long")
    return clean_name


def _hash_password(password: str, salt: str) -> str:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(password).encode("utf-8"),
        str(salt).encode("utf-8"),
        PBKDF2_ITERATIONS,
    )
    return digest.hex()


def _hash_session_token(session_token: str) -> str:
    token = str(session_token or "").strip()
    if not token:
        return ""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _json_loads(payload: str | None) -> dict[str, Any]:
    if not payload:
        return {}
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"
