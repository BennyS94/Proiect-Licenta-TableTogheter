from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m4_profiles_feedback_summary.txt"
FEEDBACK_CONTEXT_SAMPLE_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m4_feedback_context_sample.json"
PROFILE_SAMPLE_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m4_profile_sample.json"
API_EXAMPLES_DIR = PROJECT_ROOT / "docs/api_examples"


HOUSEHOLD_ID = "household_backend_m4_smoke"
MEMBER_PROFILE_ID = "member_backend_m4_smoke_001"
LIKED_RECIPE_ID = "recipes_v1_2_round41_manual_021"
DISLIKED_RECIPE_ID = "recipes_v1_2_round41_manual_023"
TOO_LONG_RECIPE_ID = "recipes_v1_2_round42_dataset_037"
AVOID_RECIPE_ID = "recipes_v1_2_round42_dataset_015"


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


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    db_path = init_db()
    client = TestClient(app)
    errors: list[str] = []

    demo_response = client.get("/households/demo")
    demo_payload = demo_response.json()
    if demo_response.status_code != 200:
        errors.append(f"demo_household_status={demo_response.status_code}")
    if not demo_payload.get("members"):
        errors.append("demo_household_members_missing")

    profile_request = {
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": MEMBER_PROFILE_ID,
        "display_name": "M4 Smoke Member",
        "age": 29,
        "sex": "male",
        "weight_kg": 74.0,
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
    profile_post = client.post("/profiles", json=profile_request)
    profile_payload = profile_post.json()
    if profile_post.status_code != 200:
        errors.append(f"profile_post_status={profile_post.status_code}")
    if profile_payload.get("member_profile_id") != MEMBER_PROFILE_ID:
        errors.append("profile_post_member_id_mismatch")

    profiles_list = client.get("/profiles", params={"household_id": HOUSEHOLD_ID})
    profiles_list_payload = profiles_list.json()
    if profiles_list.status_code != 200:
        errors.append(f"profiles_list_status={profiles_list.status_code}")
    profiles = profiles_list_payload.get("profiles", [])
    if not any(item.get("member_profile_id") == MEMBER_PROFILE_ID for item in profiles):
        errors.append("profiles_list_missing_created_profile")

    profile_get = client.get(f"/profiles/{MEMBER_PROFILE_ID}")
    profile_get_payload = profile_get.json()
    if profile_get.status_code != 200:
        errors.append(f"profile_get_status={profile_get.status_code}")
    if profile_get_payload.get("member_profile_id") != MEMBER_PROFILE_ID:
        errors.append("profile_get_member_id_mismatch")

    client.delete(
        "/feedback",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "confirm": "true",
        },
    )
    feedback_requests = [
        {
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "recipe_id": LIKED_RECIPE_ID,
            "slot": "lunch",
            "feedback_type": "liked",
            "notes": "M4 smoke liked.",
            "source": "api",
        },
        {
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "recipe_id": DISLIKED_RECIPE_ID,
            "slot": "dinner",
            "feedback_type": "disliked",
            "notes": "M4 smoke disliked.",
            "source": "api",
        },
        {
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "recipe_id": TOO_LONG_RECIPE_ID,
            "slot": "lunch",
            "feedback_type": "too_long",
            "notes": "M4 smoke too long.",
            "source": "api",
        },
        {
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "recipe_id": AVOID_RECIPE_ID,
            "slot": "breakfast",
            "feedback_type": "explicit_avoid",
            "notes": "M4 smoke avoid.",
            "source": "api",
        },
    ]
    feedback_statuses = []
    for event in feedback_requests:
        response = client.post("/feedback", json=event)
        feedback_statuses.append(response.status_code)
        payload = response.json()
        if response.status_code != 200:
            errors.append(f"feedback_post_status={response.status_code}")
        if payload.get("feedback_type") != event["feedback_type"]:
            errors.append(f"feedback_type_mismatch={event['feedback_type']}")

    context_response = client.get(
        "/feedback/context",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
        },
    )
    context_payload = context_response.json()
    if context_response.status_code != 200:
        errors.append(f"feedback_context_status={context_response.status_code}")

    hard_filters = context_payload.get("hard_filters", {})
    score_preferences = context_payload.get("score_preferences", {})
    time_preferences = context_payload.get("time_preferences", {})
    if AVOID_RECIPE_ID not in hard_filters.get("banned_recipe_ids", []):
        errors.append("explicit_avoid_missing_from_context")
    if LIKED_RECIPE_ID not in score_preferences.get("liked_recipe_ids", {}):
        errors.append("liked_missing_from_context")
    if DISLIKED_RECIPE_ID not in score_preferences.get("disliked_recipe_ids", {}):
        errors.append("disliked_missing_from_context")
    if TOO_LONG_RECIPE_ID not in time_preferences.get("too_long_recipe_ids", {}):
        errors.append("too_long_missing_from_context")

    delete_response = client.delete(
        "/feedback",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "confirm": "true",
        },
    )
    delete_payload = delete_response.json()
    if delete_response.status_code != 200:
        errors.append(f"feedback_delete_status={delete_response.status_code}")
    if int(delete_payload.get("deleted_event_count") or 0) < 4:
        errors.append("feedback_delete_count_too_low")

    context_after_delete = client.get(
        "/feedback/context",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
        },
    )
    context_after_delete_payload = context_after_delete.json()
    if int(context_after_delete_payload.get("event_count") or 0) != 0:
        errors.append("feedback_context_not_empty_after_delete")

    generation_request = _load_json(API_EXAMPLES_DIR / "individual_plan_generate_request.json")
    generation_request["days"] = 1
    generation_options = dict(generation_request.get("generation_options") or {})
    generation_options["include_grocery_list"] = False
    generation_request["generation_options"] = generation_options
    generation_response = client.post("/plans/generate", json=generation_request)
    generation_payload = generation_response.json()
    if generation_response.status_code != 200:
        errors.append(f"generation_post_status={generation_response.status_code}")
    if generation_payload.get("status") not in {"ok", "blocked"}:
        errors.append(f"generation_payload_status={generation_payload.get('status')}")

    json_checks = [
        _json_check("demo_household", demo_payload),
        _json_check("profile_post", profile_payload),
        _json_check("profiles_list", profiles_list_payload),
        _json_check("profile_get", profile_get_payload),
        _json_check("feedback_context", context_payload),
        _json_check("feedback_delete", delete_payload),
        _json_check("feedback_context_after_delete", context_after_delete_payload),
        _json_check("generation_post", generation_payload),
    ]
    errors.extend([check for check in json_checks if not check.endswith(":json_ok")])

    PROFILE_SAMPLE_PATH.write_text(
        json.dumps(profile_get_payload, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    FEEDBACK_CONTEXT_SAMPLE_PATH.write_text(
        json.dumps(context_payload, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )

    status_ok = not errors
    summary_lines = [
        "Backend M4 profiles and feedback smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"demo_household_status={demo_response.status_code}",
        f"demo_household_member_count={len(demo_payload.get('members', []))}",
        f"profile_post_status={profile_post.status_code}",
        f"profile_get_status={profile_get.status_code}",
        f"profiles_list_status={profiles_list.status_code}",
        f"profiles_list_count={len(profiles)}",
        "feedback_post_statuses=" + ",".join(str(item) for item in feedback_statuses),
        f"feedback_context_status={context_response.status_code}",
        f"feedback_context_event_count={context_payload.get('event_count')}",
        f"feedback_delete_status={delete_response.status_code}",
        f"feedback_deleted_count={delete_payload.get('deleted_event_count')}",
        f"feedback_context_after_delete_count={context_after_delete_payload.get('event_count')}",
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
