from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/progress_trends_backend_summary.txt"
SAMPLE_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/progress_trends_backend_sample.json"

EMAIL_ONE = "progress_trends_check_one@tabletogether.test"
EMAIL_TWO = "progress_trends_check_two@tabletogether.test"
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
    day_index: int,
    target_kcal: float | None = None,
) -> dict[str, Any]:
    planned = {
        "kcal": 2100 + day_index,
        "protein_g": 140,
        "carbs_g": 240,
        "fat_g": 70,
    }
    payload: dict[str, Any] = {
        "household_id": household_id,
        "member_profile_id": member_profile_id,
        "plan_id": plan_id,
        "day_index": day_index,
        "planned": planned,
        "consumed": {
            "kcal": 1800 + day_index,
            "protein_g": 118 + (day_index % 7),
            "carbs_g": 210,
            "fat_g": 64,
        },
        "meal_completion": {
            "completed_meal_keys": ["breakfast", "lunch"],
            "meals": [
                {"slot": "breakfast", "eaten": True, "recipe_id": "recipe_check"},
                {"slot": "lunch", "eaten": True, "recipe_id": "recipe_check_lunch"},
            ],
        },
        "day_snapshot": {
            "selected_day": day_index,
            "source": "check_progress_trends_backend",
        },
    }
    if target_kcal is not None:
        payload["target"] = {
            **planned,
            "kcal": target_kcal,
        }
    return payload


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
            request_json={"source": "check_progress_trends_backend"},
            response_json=response_json,
        )


def _table_columns(db_path: Path, table_name: str) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _timestamps_descending(snapshots: list[dict[str, Any]]) -> bool:
    previous: str | None = None
    for snapshot in snapshots:
        current = str(snapshot.get("saved_at") or "")
        if previous is not None and current > previous:
            return False
        previous = current
    return True


def _has_trend_totals(snapshot: dict[str, Any]) -> bool:
    target = snapshot.get("target")
    planned = snapshot.get("planned")
    consumed = snapshot.get("consumed")
    if not isinstance(target, dict) or not isinstance(planned, dict) or not isinstance(consumed, dict):
        return False
    required = {"kcal", "protein_g", "carbs_g", "fat_g"}
    return all(key in target and key in planned and key in consumed for key in required)


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import init_db
    from backend.app.main import app

    db_path = init_db()
    _cleanup_previous_test_rows(db_path)

    client = TestClient(app)
    errors: list[str] = []

    required_columns = {
        "target_kcal",
        "target_protein_g",
        "target_carbs_g",
        "target_fat_g",
    }
    table_columns = _table_columns(db_path, "saved_daily_progress")
    if not required_columns.issubset(table_columns):
        errors.append("target_columns_missing")

    token_one, household_one = _register(client, EMAIL_ONE)
    profile_one = _create_profile(client, token_one, "Progress Trends One")

    explicit_plan_id = "progress_trends_explicit_target"
    _insert_fake_plan(
        db_path,
        household_id=household_one,
        member_profile_id=profile_one,
        plan_id=explicit_plan_id,
    )
    explicit_response = client.post(
        "/progress/daily",
        headers={"Authorization": f"Bearer {token_one}"},
        json=_progress_request(
            household_one,
            profile_one,
            explicit_plan_id,
            day_index=1,
            target_kcal=2200,
        ),
    )
    explicit_payload = explicit_response.json()
    if explicit_response.status_code != 200:
        errors.append(f"explicit_save_status={explicit_response.status_code}")
    explicit_snapshot = explicit_payload.get("snapshot") if isinstance(explicit_payload, dict) else {}
    explicit_target = explicit_snapshot.get("target") if isinstance(explicit_snapshot, dict) else {}
    if not isinstance(explicit_target, dict) or round(float(explicit_target.get("kcal") or 0)) != 2200:
        errors.append("explicit_target_not_returned")

    duplicate_response = client.post(
        "/progress/daily",
        headers={"Authorization": f"Bearer {token_one}"},
        json=_progress_request(
            household_one,
            profile_one,
            explicit_plan_id,
            day_index=1,
            target_kcal=2200,
        ),
    )
    if duplicate_response.status_code != 200:
        errors.append(f"duplicate_status={duplicate_response.status_code}")
    if duplicate_response.json().get("status") != "already_saved":
        errors.append("duplicate_status_not_preserved")

    for index in range(34):
        plan_id = f"progress_trends_plan_{index:02d}"
        _insert_fake_plan(
            db_path,
            household_id=household_one,
            member_profile_id=profile_one,
            plan_id=plan_id,
        )
        response = client.post(
            "/progress/daily",
            headers={"Authorization": f"Bearer {token_one}"},
            json=_progress_request(
                household_one,
                profile_one,
                plan_id,
                day_index=(index % 5) + 1,
            ),
        )
        if response.status_code != 200:
            errors.append(f"bulk_save_status_{index}={response.status_code}")
            break

    list_response = client.get(
        f"/progress/daily?profile_id={profile_one}&limit=30",
        headers={"Authorization": f"Bearer {token_one}"},
    )
    list_payload = list_response.json()
    snapshots = list_payload.get("snapshots") if isinstance(list_payload, dict) else []
    if list_response.status_code != 200:
        errors.append(f"trend_list_status={list_response.status_code}")
    if not isinstance(snapshots, list):
        errors.append("trend_snapshots_not_list")
        snapshots = []
    if len(snapshots) > 30:
        errors.append(f"trend_snapshot_count_over_30={len(snapshots)}")
    if snapshots and not all(_has_trend_totals(snapshot) for snapshot in snapshots):
        errors.append("trend_totals_missing")
    if snapshots and not _timestamps_descending(snapshots):
        errors.append("trend_order_not_descending")
    fallback_rows = [
        snapshot
        for snapshot in snapshots
        if isinstance(snapshot.get("target"), dict)
        and isinstance(snapshot.get("planned"), dict)
        and snapshot["target"].get("kcal") == snapshot["planned"].get("kcal")
    ]
    if not fallback_rows:
        errors.append("planned_fallback_target_not_observed")

    token_two, _household_two = _register(client, EMAIL_TWO)
    profile_two = _create_profile(client, token_two, "Progress Trends Two")
    cross_profile_response = client.get(
        f"/progress/daily?member_profile_id={profile_one}",
        headers={"Authorization": f"Bearer {token_two}"},
    )
    if cross_profile_response.status_code != 404:
        errors.append(f"cross_household_profile_read_status={cross_profile_response.status_code}")
    own_empty_response = client.get(
        f"/progress/daily?member_profile_id={profile_two}",
        headers={"Authorization": f"Bearer {token_two}"},
    )
    if own_empty_response.status_code != 200:
        errors.append(f"own_empty_status={own_empty_response.status_code}")
    elif own_empty_response.json().get("snapshots") != []:
        errors.append("profile_b_history_leak")

    delete_target_id = str(snapshots[0].get("progress_id") or "") if snapshots else ""
    delete_response = client.delete(
        f"/progress/daily/{delete_target_id}",
        headers={"Authorization": f"Bearer {token_one}"},
    )
    if delete_response.status_code != 200:
        errors.append(f"delete_status={delete_response.status_code}")
    if delete_response.json().get("deleted") is not True:
        errors.append("delete_not_preserved")

    sample = {
        "list_status_code": list_response.status_code,
        "snapshot_count": len(snapshots),
        "first_snapshot": snapshots[0] if snapshots else None,
        "fallback_target_note": "existing rows without explicit target use planned totals",
        "cross_household_status": cross_profile_response.status_code,
        "delete_status_code": delete_response.status_code,
    }
    status_ok = not errors
    summary_lines = [
        "Progress trends backend summary",
        "status=ok" if status_ok else "status=failed",
        f"target_columns_present={str(required_columns.issubset(table_columns)).lower()}",
        f"trend_list_status={list_response.status_code}",
        f"snapshot_count={len(snapshots)}",
        f"max_30={str(len(snapshots) <= 30).lower()}",
        f"target_totals_present={str(bool(snapshots) and all(_has_trend_totals(snapshot) for snapshot in snapshots)).lower()}",
        "target_backfill_behavior=target uses explicit payload when present; otherwise planned totals fallback",
        f"cross_household_status={cross_profile_response.status_code}",
        f"delete_status={delete_response.status_code}",
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    SAMPLE_PATH.write_text(json.dumps(sample, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
