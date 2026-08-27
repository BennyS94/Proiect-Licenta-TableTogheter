from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m8_profile_feedback_cleanup_summary.txt"
API_EXAMPLES_DIR = PROJECT_ROOT / "docs/api_examples"

HOUSEHOLD_ID = "household_backend_m8_smoke"
MEMBER_PROFILE_ID = "member_backend_m8_smoke_001"
FEEDBACK_RECIPE_ID = "recipes_v1_2_round41_manual_021"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_check(name: str, payload: dict[str, Any]) -> str:
    try:
        json.dumps(payload, ensure_ascii=True, allow_nan=False, sort_keys=True)
    except Exception as exc:
        return f"{name}:json_failed:{type(exc).__name__}:{exc}"
    return f"{name}:json_ok"


def _row_count(db_path: Path, table: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] if row else 0)


def _profile_request() -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": MEMBER_PROFILE_ID,
        "display_name": "M8 Cleanup Member",
        "age": 30,
        "sex": "female",
        "weight_kg": 64.0,
        "height_cm": 168.0,
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


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    db_path = init_db()
    client = TestClient(app)
    errors: list[str] = []

    client.delete(
        "/feedback",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "confirm": "true",
        },
    )

    profile_post = client.post("/profiles", json=_profile_request())
    profile_post_payload = profile_post.json()
    if profile_post.status_code != 200:
        errors.append(f"profile_post_status={profile_post.status_code}")
    if profile_post_payload.get("member_profile_id") != MEMBER_PROFILE_ID:
        errors.append("profile_post_member_id_mismatch")

    profiles_before = client.get("/profiles", params={"household_id": HOUSEHOLD_ID})
    profiles_before_payload = profiles_before.json()
    if profiles_before.status_code != 200:
        errors.append(f"profiles_before_status={profiles_before.status_code}")
    profiles_before_list = profiles_before_payload.get("profiles", [])
    if not any(item.get("member_profile_id") == MEMBER_PROFILE_ID for item in profiles_before_list):
        errors.append("profiles_before_missing_created_profile")

    delete_without_confirm = client.delete(f"/profiles/{MEMBER_PROFILE_ID}")
    if delete_without_confirm.status_code != 400:
        errors.append(f"profile_delete_without_confirm_status={delete_without_confirm.status_code}")

    delete_response = client.delete(
        f"/profiles/{MEMBER_PROFILE_ID}",
        params={"confirm": "true"},
    )
    delete_payload = delete_response.json()
    if delete_response.status_code != 200:
        errors.append(f"profile_delete_status={delete_response.status_code}")
    if delete_payload.get("status") != "ok":
        errors.append(f"profile_delete_payload_status={delete_payload.get('status')}")
    if delete_payload.get("deactivated") is not True:
        errors.append("profile_delete_not_deactivated")
    if delete_payload.get("deleted") is not False:
        errors.append("profile_delete_hard_delete_flag_unexpected")

    missing_delete = client.delete(
        "/profiles/member_backend_m8_missing",
        params={"confirm": "true"},
    )
    if missing_delete.status_code != 404:
        errors.append(f"profile_missing_delete_status={missing_delete.status_code}")

    profiles_after = client.get("/profiles", params={"household_id": HOUSEHOLD_ID})
    profiles_after_payload = profiles_after.json()
    if profiles_after.status_code != 200:
        errors.append(f"profiles_after_status={profiles_after.status_code}")
    profiles_after_list = profiles_after_payload.get("profiles", [])
    if any(item.get("member_profile_id") == MEMBER_PROFILE_ID for item in profiles_after_list):
        errors.append("profiles_after_still_in_active_list")

    inactive_lookup = client.get(
        "/profiles",
        params={
            "household_id": HOUSEHOLD_ID,
            "active_only": "false",
        },
    )
    inactive_lookup_payload = inactive_lookup.json()
    inactive_profiles = inactive_lookup_payload.get("profiles", [])
    inactive_profile = next(
        (
            item
            for item in inactive_profiles
            if item.get("member_profile_id") == MEMBER_PROFILE_ID
        ),
        None,
    )
    if inactive_lookup.status_code != 200:
        errors.append(f"inactive_lookup_status={inactive_lookup.status_code}")
    if inactive_profile is None:
        errors.append("inactive_profile_missing_from_active_only_false")
    elif inactive_profile.get("is_active") is not False:
        errors.append("inactive_profile_is_active_not_false")

    feedback_post = client.post(
        "/feedback",
        json={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "recipe_id": FEEDBACK_RECIPE_ID,
            "slot": "lunch",
            "feedback_type": "liked",
            "notes": "M8 cleanup smoke event.",
            "source": "api",
        },
    )
    feedback_post_payload = feedback_post.json()
    if feedback_post.status_code != 200:
        errors.append(f"feedback_post_status={feedback_post.status_code}")

    context_before_delete = client.get(
        "/feedback/context",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
        },
    )
    context_before_delete_payload = context_before_delete.json()
    if context_before_delete.status_code != 200:
        errors.append(f"feedback_context_before_status={context_before_delete.status_code}")
    if int(context_before_delete_payload.get("event_count") or 0) < 1:
        errors.append("feedback_context_before_empty")

    feedback_delete = client.delete(
        "/feedback",
        params={
            "household_id": HOUSEHOLD_ID,
            "confirm": "true",
        },
    )
    feedback_delete_payload = feedback_delete.json()
    if feedback_delete.status_code != 200:
        errors.append(f"feedback_delete_status={feedback_delete.status_code}")
    if int(feedback_delete_payload.get("deleted_event_count") or 0) < 1:
        errors.append("feedback_delete_count_too_low")

    context_after_delete = client.get(
        "/feedback/context",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
        },
    )
    context_after_delete_payload = context_after_delete.json()
    if context_after_delete.status_code != 200:
        errors.append(f"feedback_context_after_status={context_after_delete.status_code}")
    if int(context_after_delete_payload.get("event_count") or 0) != 0:
        errors.append("feedback_context_after_not_empty")

    generation_request = _load_json(API_EXAMPLES_DIR / "individual_plan_generate_request.json")
    generation_request["days"] = 1
    generation_options = dict(generation_request.get("generation_options") or {})
    generation_options["include_grocery_list"] = False
    generation_request["generation_options"] = generation_options
    generation_request["include_grocery_list"] = False
    generation_request["include_purchase_suggestions"] = False
    generation_request["include_price_estimates"] = False
    generation_response = client.post("/plans/generate", json=generation_request)
    generation_payload = generation_response.json()
    if generation_response.status_code != 200:
        errors.append(f"generation_post_status={generation_response.status_code}")
    if generation_payload.get("status") not in {"ok", "blocked"}:
        errors.append(f"generation_payload_status={generation_payload.get('status')}")

    json_checks = [
        _json_check("profile_post", profile_post_payload),
        _json_check("profiles_before", profiles_before_payload),
        _json_check("profile_delete", delete_payload),
        _json_check("profiles_after", profiles_after_payload),
        _json_check("inactive_lookup", inactive_lookup_payload),
        _json_check("feedback_post", feedback_post_payload),
        _json_check("feedback_context_before_delete", context_before_delete_payload),
        _json_check("feedback_delete", feedback_delete_payload),
        _json_check("feedback_context_after_delete", context_after_delete_payload),
        _json_check("generation_post", generation_payload),
    ]
    errors.extend([check for check in json_checks if not check.endswith(":json_ok")])

    status_ok = not errors
    summary_lines = [
        "Backend M8 profile and feedback cleanup smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"profile_post_status={profile_post.status_code}",
        f"profiles_before_status={profiles_before.status_code}",
        f"profiles_before_count={len(profiles_before_list)}",
        f"profile_delete_without_confirm_status={delete_without_confirm.status_code}",
        f"profile_delete_status={delete_response.status_code}",
        f"profile_delete_payload_status={delete_payload.get('status')}",
        f"profile_delete_deactivated={delete_payload.get('deactivated')}",
        f"profile_delete_deleted={delete_payload.get('deleted')}",
        f"profile_missing_delete_status={missing_delete.status_code}",
        f"profiles_after_status={profiles_after.status_code}",
        f"profiles_after_count={len(profiles_after_list)}",
        f"inactive_lookup_status={inactive_lookup.status_code}",
        f"inactive_profile_found={inactive_profile is not None}",
        f"inactive_profile_is_active={inactive_profile.get('is_active') if inactive_profile else ''}",
        f"feedback_post_status={feedback_post.status_code}",
        f"feedback_context_before_count={context_before_delete_payload.get('event_count')}",
        f"feedback_delete_status={feedback_delete.status_code}",
        f"feedback_deleted_count={feedback_delete_payload.get('deleted_event_count')}",
        f"feedback_context_after_count={context_after_delete_payload.get('event_count')}",
        f"generation_post_status={generation_response.status_code}",
        f"generation_payload_status={generation_payload.get('status')}",
        f"member_profiles_row_count={_row_count(db_path, 'member_profiles')}",
        f"feedback_events_row_count={_row_count(db_path, 'feedback_events')}",
        "json_checks=" + ";".join(json_checks),
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
