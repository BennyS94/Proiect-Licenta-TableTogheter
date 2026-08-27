from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m5_persistence_generation_summary.txt"
INDIVIDUAL_SAMPLE_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m5_individual_response_sample.json"
HOUSEHOLD_SAMPLE_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m5_household_response_sample.json"
FEEDBACK_CONTEXT_SAMPLE_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_m5_feedback_context_used_sample.json"

HOUSEHOLD_ID = "household_backend_m5_smoke"
MEMBER_PROFILE_ID = "member_backend_m5_smoke_001"
SECOND_MEMBER_PROFILE_ID = "member_backend_m5_smoke_002"


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


def _latest_plan_request(db_path: Path, plan_id: str) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT request_json FROM generated_plans WHERE plan_id = ?",
            (plan_id,),
        ).fetchone()
    if row is None:
        return {}
    return json.loads(row[0])


def _profile_request(member_profile_id: str, display_name: str, *, sex: str) -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": member_profile_id,
        "display_name": display_name,
        "age": 31 if sex == "male" else 29,
        "sex": sex,
        "weight_kg": 78.0 if sex == "male" else 63.0,
        "height_cm": 178.0 if sex == "male" else 166.0,
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


def _individual_generation_request(
    feedback_enabled: bool,
    *,
    include_grocery_list: bool,
) -> dict[str, Any]:
    return {
        "dataset_profile": "current",
        "days": 1,
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": MEMBER_PROFILE_ID,
        "include_grocery_list": include_grocery_list,
        "include_purchase_suggestions": include_grocery_list,
        "include_price_estimates": include_grocery_list,
        "feedback_enabled": feedback_enabled,
        "generation_options": {
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "profile_guard": "demo",
            "day_candidate_builder": "direct_from_slots",
        },
    }


def _household_generation_request() -> dict[str, Any]:
    return {
        "dataset_profile": "current",
        "days": 1,
        "household_id": HOUSEHOLD_ID,
        "selected_member_ids": [
            MEMBER_PROFILE_ID,
            SECOND_MEMBER_PROFILE_ID,
        ],
        "household_mode": "individual_breakfast_shared_main",
        "household_allocation_mode": "macro_aware_simple",
        "include_grocery_list": False,
        "include_purchase_suggestions": False,
        "include_price_estimates": False,
        "feedback_enabled": True,
        "generation_options": {
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "profile_guard": "demo",
            "day_candidate_builder": "direct_from_slots",
        },
    }


def _selected_recipe_ids(payload: dict[str, Any]) -> set[str]:
    result: set[str] = set()
    for day in payload.get("daily_plan", []):
        if not isinstance(day, dict):
            continue
        for meal in day.get("selected_meals", []):
            if isinstance(meal, dict) and meal.get("recipe_id"):
                result.add(str(meal["recipe_id"]))
    return result


def _first_recipe_id(payload: dict[str, Any]) -> str:
    selected = sorted(_selected_recipe_ids(payload))
    return selected[0] if selected else "recipes_v1_2_round41_manual_021"


def _sample_response(payload: dict[str, Any]) -> dict[str, Any]:
    sample_keys = [
        "status",
        "plan_id",
        "household_plan_id",
        "generation_type",
        "dataset_profile",
        "household_id",
        "member_profile_id",
        "selected_members",
        "days",
        "daily_plan",
        "feedback_context_summary",
        "diagnostics_summary",
    ]
    sample = {key: payload.get(key) for key in sample_keys if key in payload}
    if isinstance(sample.get("daily_plan"), list):
        sample["daily_plan"] = sample["daily_plan"][:1]
    grocery = payload.get("grocery_list") or payload.get("household_grocery_list")
    if isinstance(grocery, dict):
        sample["grocery_summary"] = grocery.get("summary", {})
    return sample


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    db_path = init_db()
    client = TestClient(app)
    errors: list[str] = []

    generated_plans_before = _row_count(db_path, "generated_plans")
    grocery_lists_before = _row_count(db_path, "grocery_lists")

    for profile in (
        _profile_request(MEMBER_PROFILE_ID, "M5 Smoke Member A", sex="male"),
        _profile_request(SECOND_MEMBER_PROFILE_ID, "M5 Smoke Member B", sex="female"),
    ):
        response = client.post("/profiles", json=profile)
        if response.status_code != 200:
            errors.append(f"profile_post_status={response.status_code}")

    client.delete(
        "/feedback",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "confirm": "true",
        },
    )

    baseline_response = client.post(
        "/plans/generate",
        json=_individual_generation_request(
            feedback_enabled=False,
            include_grocery_list=False,
        ),
    )
    baseline_payload = baseline_response.json()
    if baseline_response.status_code != 200:
        errors.append(f"baseline_post_status={baseline_response.status_code}")
    if baseline_payload.get("status") != "ok":
        errors.append(f"baseline_payload_status={baseline_payload.get('status')}")

    avoid_recipe_id = _first_recipe_id(baseline_payload)
    feedback_events = [
        ("explicit_avoid", avoid_recipe_id, "breakfast"),
        ("liked", "recipes_v1_2_round41_manual_021", "lunch"),
        ("disliked", "recipes_v1_2_round41_manual_023", "dinner"),
        ("too_long", "recipes_v1_2_round42_dataset_037", "lunch"),
    ]
    feedback_statuses = []
    for feedback_type, recipe_id, slot in feedback_events:
        response = client.post(
            "/feedback",
            json={
                "household_id": HOUSEHOLD_ID,
                "member_profile_id": MEMBER_PROFILE_ID,
                "recipe_id": recipe_id,
                "slot": slot,
                "feedback_type": feedback_type,
                "notes": "M5 smoke event.",
                "source": "api",
            },
        )
        feedback_statuses.append(response.status_code)
        if response.status_code != 200:
            errors.append(f"feedback_post_status={response.status_code}")

    feedback_response = client.post(
        "/plans/generate",
        json=_individual_generation_request(
            feedback_enabled=True,
            include_grocery_list=True,
        ),
    )
    feedback_payload = feedback_response.json()
    if feedback_response.status_code != 200:
        errors.append(f"profile_id_generation_status={feedback_response.status_code}")
    if feedback_payload.get("status") != "ok":
        errors.append(f"profile_id_generation_payload_status={feedback_payload.get('status')}")
    if feedback_payload.get("member_profile_id") != MEMBER_PROFILE_ID:
        errors.append("profile_id_generation_member_id_mismatch")

    feedback_summary = feedback_payload.get("feedback_context_summary", {})
    if int(feedback_summary.get("event_count") or 0) < 4:
        errors.append("feedback_context_not_injected")
    if int(feedback_summary.get("explicit_avoid_count") or 0) < 1:
        errors.append("explicit_avoid_not_reported")
    if avoid_recipe_id in _selected_recipe_ids(feedback_payload):
        errors.append("explicit_avoid_recipe_selected")

    no_feedback_response = client.post(
        "/plans/generate",
        json=_individual_generation_request(
            feedback_enabled=False,
            include_grocery_list=False,
        ),
    )
    no_feedback_payload = no_feedback_response.json()
    if no_feedback_response.status_code != 200:
        errors.append(f"feedback_disabled_status={no_feedback_response.status_code}")
    no_feedback_summary = no_feedback_payload.get("feedback_context_summary", {})
    if int(no_feedback_summary.get("event_count") or 0) != 0:
        errors.append("feedback_disabled_event_count_not_zero")

    household_response = client.post(
        "/household-plans/generate",
        json=_household_generation_request(),
    )
    household_payload = household_response.json()
    if household_response.status_code != 200:
        errors.append(f"household_selected_status={household_response.status_code}")
    if household_payload.get("status") != "ok":
        errors.append(f"household_selected_payload_status={household_payload.get('status')}")

    selected_members = household_payload.get("selected_members", [])
    selected_ids = {
        str(member.get("member_id"))
        for member in selected_members
        if isinstance(member, dict)
    }
    expected_selected_ids = {MEMBER_PROFILE_ID, SECOND_MEMBER_PROFILE_ID}
    if selected_ids != expected_selected_ids:
        errors.append(
            "household_selected_member_ids_mismatch="
            + ",".join(sorted(selected_ids))
        )

    generated_plans_after = _row_count(db_path, "generated_plans")
    grocery_lists_after = _row_count(db_path, "grocery_lists")
    if generated_plans_after <= generated_plans_before:
        errors.append("generated_plan_not_persisted")
    if grocery_lists_after <= grocery_lists_before:
        errors.append("grocery_list_not_persisted")

    feedback_plan_id = str(feedback_payload.get("plan_id") or "")
    feedback_request_json = _latest_plan_request(db_path, feedback_plan_id)
    if feedback_request_json.get("member_profile_id") != MEMBER_PROFILE_ID:
        errors.append("persisted_request_missing_member_profile_id")
    if not isinstance(feedback_request_json.get("feedback_context"), dict):
        errors.append("persisted_request_missing_feedback_context")

    feedback_context_sample = {
        "avoid_recipe_id": avoid_recipe_id,
        "feedback_context_summary": feedback_summary,
        "persisted_request_feedback_context": feedback_request_json.get("feedback_context", {}),
        "feedback_disabled_summary": no_feedback_summary,
    }

    json_checks = [
        _json_check("baseline_generation", baseline_payload),
        _json_check("profile_id_generation", feedback_payload),
        _json_check("feedback_disabled_generation", no_feedback_payload),
        _json_check("household_selected_generation", household_payload),
        _json_check("feedback_context_used", feedback_context_sample),
    ]
    errors.extend([check for check in json_checks if not check.endswith(":json_ok")])

    INDIVIDUAL_SAMPLE_PATH.write_text(
        json.dumps(_sample_response(feedback_payload), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    HOUSEHOLD_SAMPLE_PATH.write_text(
        json.dumps(_sample_response(household_payload), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    FEEDBACK_CONTEXT_SAMPLE_PATH.write_text(
        json.dumps(feedback_context_sample, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )

    status_ok = not errors
    summary_lines = [
        "Backend M5 persistence-aware generation smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"profile_id_generation_status={feedback_response.status_code}",
        f"profile_id_generation_payload_status={feedback_payload.get('status')}",
        f"profile_id_generation_plan_id={feedback_payload.get('plan_id')}",
        f"feedback_post_statuses={','.join(str(item) for item in feedback_statuses)}",
        f"feedback_context_event_count={feedback_summary.get('event_count')}",
        f"feedback_context_explicit_avoid_count={feedback_summary.get('explicit_avoid_count')}",
        f"feedback_disabled_event_count={no_feedback_summary.get('event_count')}",
        f"avoid_recipe_id={avoid_recipe_id}",
        f"avoid_recipe_selected={avoid_recipe_id in _selected_recipe_ids(feedback_payload)}",
        f"household_selected_status={household_response.status_code}",
        f"household_selected_payload_status={household_payload.get('status')}",
        f"household_selected_member_ids={','.join(sorted(selected_ids))}",
        f"generated_plans_before={generated_plans_before}",
        f"generated_plans_after={generated_plans_after}",
        f"generated_plans_created={generated_plans_after - generated_plans_before}",
        f"grocery_lists_before={grocery_lists_before}",
        f"grocery_lists_after={grocery_lists_after}",
        f"grocery_lists_created={grocery_lists_after - grocery_lists_before}",
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
