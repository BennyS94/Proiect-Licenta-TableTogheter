from __future__ import annotations

import secrets
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

EMAIL = "moraruhousehold@tabletogether.test"
PASSWORD = "123456"
HOUSEHOLD_NAME = "Moraru Household"


COMMON_DIETARY_PREFERENCES = {
    "vegetarian": False,
    "vegan": False,
    "gluten_free": False,
    "no_beef": False,
    "no_pork": False,
    "no_chicken": False,
    "no_fish": False,
    "no_dairy": False,
}

COMMON_FOOD_PREFERENCES = {
    "ratings": {
        "chicken": "like",
        "eggs": "like",
        "dairy": "like",
        "rice": "like",
        "pasta": "like",
        "potatoes": "like",
        "vegetables": "like",
    },
    "avoid_ingredients": [],
    "cooking_time_preference": "balanced",
}

COMMON_MEAL_CONFIG = {
    "meals_per_day": 3,
    "include_snacks": True,
    "day_structure": "3_meals_plus_snack",
}

PROFILES = [
    {
        "stable_id": "member_profile_moraru_alice",
        "display_name": "Alice",
        "age": 38,
        "sex": "female",
        "weight_kg": 72.0,
        "height_cm": 165.0,
        "activity_level": "moderately_active",
        "goal": "lose",
        "goal_speed": "normal",
        "training": {
            "sessions_per_week": 2,
            "type": "mixed",
        },
    },
    {
        "stable_id": "member_profile_moraru_radu",
        "display_name": "Radu",
        "age": 41,
        "sex": "male",
        "weight_kg": 82.0,
        "height_cm": 180.0,
        "activity_level": "moderately_active",
        "goal": "gain",
        "goal_speed": "normal",
        "training": {
            "sessions_per_week": 4,
            "type": "weights",
        },
    },
    {
        "stable_id": "member_profile_moraru_mara",
        "display_name": "Mara",
        "age": 9,
        "sex": "female",
        "weight_kg": 31.0,
        "height_cm": 136.0,
        "activity_level": "moderately_active",
        "goal": "maintain",
        "goal_speed": "normal",
        "training": {
            "sessions_per_week": 2,
            "type": "mixed",
        },
    },
    {
        "stable_id": "member_profile_moraru_adrian",
        "display_name": "Adrian",
        "age": 16,
        "sex": "male",
        "weight_kg": 64.0,
        "height_cm": 174.0,
        "activity_level": "very_active",
        "goal": "gain",
        "goal_speed": "normal",
        "training": {
            "sessions_per_week": 3,
            "type": "weights",
        },
    },
]


def main() -> int:
    from backend.app.db.auth_repository import (
        _hash_password,
        create_user_with_household,
        get_household_for_user,
        get_user_by_email,
        verify_user_password,
    )
    from backend.app.db.database import get_connection, get_sqlite_path, init_db
    from backend.app.db.repositories import list_member_profiles, save_member_profile
    from src.generator_v1.target_builder import build_nutrition_target

    db_path = init_db()
    user = get_user_by_email(EMAIL)
    created_account = False
    password_reset = False

    if user is None:
        account = create_user_with_household(EMAIL, PASSWORD)
        created_account = True
    else:
        if verify_user_password(EMAIL, PASSWORD) is None:
            _reset_password(_hash_password)
            password_reset = True
        household = get_household_for_user(str(user["user_id"]))
        if household is None:
            household = _create_household_for_user(str(user["user_id"]))
        account = {
            "user_id": str(user["user_id"]),
            "email": EMAIL,
            "household_id": str(household["household_id"]),
            "household_display_name": str(
                household.get("household_display_name")
                or household.get("display_name")
                or HOUSEHOLD_NAME
            ),
        }

    household_id = str(account["household_id"])
    with get_connection() as conn:
        _rename_household(conn, household_id)
        for profile in PROFILES:
            payload = _profile_payload(conn, household_id, profile)
            build_nutrition_target(payload)
            save_member_profile(conn, payload)

        profiles = [
            profile
            for profile in list_member_profiles(conn, household_id=household_id)
            if profile["display_name"] in {item["display_name"] for item in PROFILES}
        ]

    verified = verify_user_password(EMAIL, PASSWORD) is not None
    print(f"db_path={get_sqlite_path()}")
    print(f"email={EMAIL}")
    print(f"password={PASSWORD}")
    print(f"created_account={created_account}")
    print(f"password_reset={password_reset}")
    print(f"login_verified={verified}")
    print(f"household_id={household_id}")
    print(f"household_name={HOUSEHOLD_NAME}")
    print(f"profile_count={len(profiles)}")
    for profile in sorted(profiles, key=lambda item: item["display_name"]):
        print(
            "profile="
            f"{profile['display_name']}|"
            f"age={profile['age']}|"
            f"sex={profile['sex']}|"
            f"goal={profile['goal']}|"
            f"activity={profile['activity_level']}|"
            f"training={profile['training'].get('type')}:"
            f"{profile['training'].get('sessions_per_week')}"
        )

    if not verified or len(profiles) != len(PROFILES):
        return 1
    return 0


def _reset_password(hash_password: Any) -> None:
    from backend.app.db.database import get_connection

    now = _utc_now_iso()
    salt = secrets.token_hex(16)
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE users
            SET password_hash = ?,
                password_salt = ?,
                updated_at = ?,
                is_active = 1
            WHERE email = ?
            """,
            (hash_password(PASSWORD, salt), salt, now, EMAIL),
        )


def _create_household_for_user(user_id: str) -> dict[str, Any]:
    from backend.app.db.database import get_connection

    now = _utc_now_iso()
    household_id = f"household_{uuid.uuid4().hex[:12]}"
    with get_connection() as conn:
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
            VALUES (?, ?, ?, ?, ?, ?, '{}', 1)
            """,
            (
                household_id,
                user_id,
                HOUSEHOLD_NAME,
                HOUSEHOLD_NAME,
                now,
                now,
            ),
        )
    return {
        "household_id": household_id,
        "user_id": user_id,
        "display_name": HOUSEHOLD_NAME,
        "household_display_name": HOUSEHOLD_NAME,
    }


def _rename_household(conn: sqlite3.Connection, household_id: str) -> None:
    conn.execute(
        """
        UPDATE households
        SET household_name = ?,
            display_name = ?,
            updated_at = ?,
            is_active = 1
        WHERE household_id = ?
        """,
        (HOUSEHOLD_NAME, HOUSEHOLD_NAME, _utc_now_iso(), household_id),
    )


def _profile_payload(
    conn: sqlite3.Connection,
    household_id: str,
    profile: dict[str, Any],
) -> dict[str, Any]:
    existing_id = _find_profile_id(conn, household_id, str(profile["display_name"]))
    return {
        "member_profile_id": existing_id or profile["stable_id"],
        "household_id": household_id,
        "display_name": profile["display_name"],
        "age": profile["age"],
        "sex": profile["sex"],
        "weight_kg": profile["weight_kg"],
        "height_cm": profile["height_cm"],
        "activity_level": profile["activity_level"],
        "goal": profile["goal"],
        "goal_speed": profile["goal_speed"],
        "training": profile["training"],
        "meal_config": COMMON_MEAL_CONFIG,
        "dietary_preferences": COMMON_DIETARY_PREFERENCES,
        "food_preferences": COMMON_FOOD_PREFERENCES,
        "bf_profile": "normal",
        "is_active": True,
    }


def _find_profile_id(
    conn: sqlite3.Connection,
    household_id: str,
    display_name: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT member_profile_id
        FROM member_profiles
        WHERE household_id = ?
          AND lower(display_name) = lower(?)
          AND is_active = 1
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (household_id, display_name),
    ).fetchone()
    return str(row[0]) if row else None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


if __name__ == "__main__":
    raise SystemExit(main())
