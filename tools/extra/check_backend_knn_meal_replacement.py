from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

AUDIT_DIR = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb"
SUMMARY_PATH = AUDIT_DIR / "backend_knn_meal_replacement_summary.txt"
PREVIEW_SAMPLE_PATH = AUDIT_DIR / "backend_knn_meal_replacement_preview_sample.json"
APPLY_SAMPLE_PATH = AUDIT_DIR / "backend_knn_meal_replacement_apply_sample.json"

HOUSEHOLD_ID = "household_backend_knn_replace_smoke"
MEMBER_PROFILE_ID = "member_backend_knn_replace_smoke_001"
MEMBER_PROFILE_ID_2 = "member_backend_knn_replace_smoke_002"


GENERATION_OPTIONS = {
    "selection_mode": "balanced_day",
    "portion_policy": "target_aware",
    "meal_realism_mode": "practical",
    "quality_gate": "demo_safe",
    "profile_guard": "demo",
    "day_candidate_builder": "direct_from_slots",
}


def _profile_request(member_profile_id: str, display_name: str, sex: str = "male") -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": member_profile_id,
        "display_name": display_name,
        "age": 31,
        "sex": sex,
        "weight_kg": 78.0 if sex == "male" else 64.0,
        "height_cm": 178.0 if sex == "male" else 165.0,
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


def _individual_generation_request() -> dict[str, Any]:
    return {
        "dataset_profile": "current",
        "days": 1,
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": MEMBER_PROFILE_ID,
        "include_grocery_list": True,
        "include_purchase_suggestions": False,
        "include_price_estimates": False,
        "feedback_enabled": False,
        "generation_options": GENERATION_OPTIONS,
    }


def _household_generation_request() -> dict[str, Any]:
    return {
        "dataset_profile": "current",
        "days": 1,
        "household_id": HOUSEHOLD_ID,
        "selected_member_ids": [MEMBER_PROFILE_ID, MEMBER_PROFILE_ID_2],
        "include_grocery_list": True,
        "include_purchase_suggestions": False,
        "include_price_estimates": False,
        "feedback_enabled": False,
        "household_mode": "individual_breakfast_shared_main",
        "household_allocation_mode": "macro_aware_simple",
        "generation_options": GENERATION_OPTIONS,
    }


def _alternatives_request(meal: dict[str, Any], member_profile_id: str | None = None) -> dict[str, Any]:
    return {
        "recipe_id": meal.get("recipe_id"),
        "slot": meal.get("slot"),
        "top_k": 5,
        "candidate_pool_k": 20,
        "dataset_profile": "current",
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": member_profile_id,
        "feedback_enabled": False,
        "approval_mode": "include_review",
        "generation_options": GENERATION_OPTIONS,
    }


def _replacement_request(
    *,
    meal: dict[str, Any],
    alternative_recipe_id: str,
    generation_type: str,
    member_id: str | None = None,
    replace_scope: str | None = None,
) -> dict[str, Any]:
    return {
        "day_index": 1,
        "slot": meal.get("slot"),
        "current_recipe_id": meal.get("recipe_id"),
        "alternative_recipe_id": alternative_recipe_id,
        "generation_type": generation_type,
        "replace_scope": replace_scope,
        "member_id": member_id,
        "member_profile_id": member_id,
        "dataset_profile": "current",
        "feedback_enabled": False,
        "generation_options": GENERATION_OPTIONS,
    }


def _first_generated_meal(plan: dict[str, Any]) -> dict[str, Any]:
    for day in plan.get("daily_plan", []):
        if not isinstance(day, dict):
            continue
        for meal in day.get("selected_meals", []):
            if isinstance(meal, dict) and meal.get("recipe_id") and meal.get("slot"):
                return meal
    return {}


def _first_household_shared_meal(plan: dict[str, Any]) -> tuple[dict[str, Any], str]:
    for menu in plan.get("per_member_menus", []):
        if not isinstance(menu, dict):
            continue
        member_id = str(menu.get("member_id") or menu.get("member_profile_id") or "")
        for meal in menu.get("meals", []):
            if not isinstance(meal, dict):
                continue
            if meal.get("recipe_id") and meal.get("slot") and meal.get("meal_scope") == "shared":
                return meal, member_id
    return {}, ""


def _approved_alternative(client: Any, meal: dict[str, Any], member_profile_id: str | None) -> dict[str, Any]:
    response = client.post(
        "/recipes/similar",
        json=_alternatives_request(meal, member_profile_id),
    )
    if response.status_code != 200:
        return {}
    for item in response.json().get("alternatives", []):
        if isinstance(item, dict) and item.get("approval_status") == "approved":
            return item
    return {}


def _compact_replacement_response(payload: dict[str, Any]) -> dict[str, Any]:
    replacement = payload.get("replacement", {})
    impact = payload.get("impact", {})
    return {
        "status": payload.get("status"),
        "dry_run": payload.get("dry_run"),
        "replacement_allowed": payload.get("replacement_allowed"),
        "approval_status": payload.get("approval_status"),
        "plan_id": payload.get("plan_id"),
        "source_plan_id": payload.get("source_plan_id"),
        "new_plan_id": payload.get("new_plan_id"),
        "generation_type": payload.get("generation_type"),
        "replacement": {
            "replace_scope": replacement.get("replace_scope")
            if isinstance(replacement, dict)
            else None,
            "current_meal": (replacement.get("current_meal") if isinstance(replacement, dict) else {}),
            "alternative_meal": (replacement.get("alternative_meal") if isinstance(replacement, dict) else {}),
        },
        "impact": impact,
        "updated_plan_id": (payload.get("updated_plan") or {}).get("plan_id")
        if isinstance(payload.get("updated_plan"), dict)
        else None,
        "grocery_item_count": len((payload.get("grocery_list") or {}).get("items", []))
        if isinstance(payload.get("grocery_list"), dict)
        else None,
    }


def _json_check(name: str, payload: dict[str, Any]) -> str:
    try:
        json.dumps(payload, ensure_ascii=True, allow_nan=False, sort_keys=True)
    except Exception as exc:
        return f"{name}:json_failed:{type(exc).__name__}:{exc}"
    return f"{name}:json_ok"


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    client = TestClient(app)
    errors: list[str] = []

    profile_statuses = [
        client.post(
            "/profiles",
            json=_profile_request(MEMBER_PROFILE_ID, "KNN Replace Smoke 1", "male"),
        ).status_code,
        client.post(
            "/profiles",
            json=_profile_request(MEMBER_PROFILE_ID_2, "KNN Replace Smoke 2", "female"),
        ).status_code,
    ]
    if any(status != 200 for status in profile_statuses):
        errors.append(f"profile_statuses={profile_statuses}")

    individual_plan_response = client.post("/plans/generate", json=_individual_generation_request())
    individual_plan = individual_plan_response.json()
    if individual_plan_response.status_code != 200:
        errors.append(f"individual_plan_status={individual_plan_response.status_code}")

    individual_meal = _first_generated_meal(individual_plan)
    individual_alt = _approved_alternative(client, individual_meal, MEMBER_PROFILE_ID)
    if not individual_alt:
        errors.append("individual_approved_alternative_missing")

    preview_payload: dict[str, Any] = {}
    apply_payload: dict[str, Any] = {}
    if individual_meal and individual_alt:
        request = _replacement_request(
            meal=individual_meal,
            alternative_recipe_id=str(individual_alt.get("recipe_id")),
            generation_type="individual",
            member_id=MEMBER_PROFILE_ID,
            replace_scope="individual_meal",
        )
        plan_id = str(individual_plan.get("plan_id") or "")
        preview_response = client.post(
            f"/plans/{plan_id}/replace-meal",
            params={"dry_run": "true"},
            json=request,
        )
        preview_payload = preview_response.json()
        if preview_response.status_code != 200:
            errors.append(f"individual_preview_status={preview_response.status_code}")
        if preview_payload.get("status") != "ok" or not preview_payload.get("dry_run"):
            errors.append("individual_preview_payload_invalid")

        apply_response = client.post(
            f"/plans/{plan_id}/replace-meal",
            params={"dry_run": "false"},
            json=request,
        )
        apply_payload = apply_response.json()
        if apply_response.status_code != 200:
            errors.append(f"individual_apply_status={apply_response.status_code}")
        if apply_payload.get("status") != "ok" or apply_payload.get("dry_run"):
            errors.append("individual_apply_payload_invalid")
        if not apply_payload.get("new_plan_id") or apply_payload.get("new_plan_id") == plan_id:
            errors.append("individual_new_plan_id_missing_or_same")
        updated_plan = apply_payload.get("updated_plan", {})
        if isinstance(updated_plan, dict):
            selected_recipe_ids = [
                meal.get("recipe_id")
                for day in updated_plan.get("daily_plan", [])
                if isinstance(day, dict)
                for meal in day.get("selected_meals", [])
                if isinstance(meal, dict)
            ]
            if individual_alt.get("recipe_id") not in selected_recipe_ids:
                errors.append("individual_updated_plan_missing_alternative")
        if not isinstance(apply_payload.get("grocery_list"), dict):
            errors.append("individual_grocery_missing")

    household_plan_response = client.post(
        "/household-plans/generate",
        json=_household_generation_request(),
    )
    household_plan = household_plan_response.json()
    if household_plan_response.status_code != 200:
        errors.append(f"household_plan_status={household_plan_response.status_code}")
    household_meal, household_member_id = _first_household_shared_meal(household_plan)
    household_alt = _approved_alternative(client, household_meal, household_member_id)
    household_apply_status = "not_tested"
    if household_meal and household_alt:
        household_request = _replacement_request(
            meal=household_meal,
            alternative_recipe_id=str(household_alt.get("recipe_id")),
            generation_type="household",
            member_id=household_member_id,
            replace_scope="household_shared_meal",
        )
        household_plan_id = str(household_plan.get("plan_id") or household_plan.get("household_plan_id") or "")
        household_apply = client.post(
            f"/plans/{household_plan_id}/replace-meal",
            params={"dry_run": "false"},
            json=household_request,
        )
        household_apply_payload = household_apply.json()
        household_apply_status = str(household_apply.status_code)
        if household_apply.status_code != 200:
            errors.append(f"household_apply_status={household_apply.status_code}")
        if household_apply_payload.get("status") != "ok":
            errors.append("household_apply_payload_invalid")
        affected_members = (
            household_apply_payload.get("impact", {}).get("affected_members", [])
            if isinstance(household_apply_payload.get("impact"), dict)
            else []
        )
        if not affected_members:
            errors.append("household_affected_members_missing")
    else:
        errors.append("household_shared_approved_alternative_missing")

    preview_sample = _compact_replacement_response(preview_payload)
    apply_sample = _compact_replacement_response(apply_payload)
    json_checks = [
        _json_check("preview_sample", preview_sample),
        _json_check("apply_sample", apply_sample),
    ]
    errors.extend([check for check in json_checks if not check.endswith(":json_ok")])

    PREVIEW_SAMPLE_PATH.write_text(
        json.dumps(preview_sample, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    APPLY_SAMPLE_PATH.write_text(
        json.dumps(apply_sample, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )

    status_ok = not errors
    summary_lines = [
        "Backend KNN meal replacement smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"profile_statuses={profile_statuses}",
        f"individual_plan_status={individual_plan_response.status_code}",
        f"individual_plan_id={individual_plan.get('plan_id')}",
        f"individual_current_recipe_id={individual_meal.get('recipe_id')}",
        f"individual_alternative_recipe_id={individual_alt.get('recipe_id')}",
        f"individual_new_plan_id={apply_payload.get('new_plan_id')}",
        f"household_plan_status={household_plan_response.status_code}",
        f"household_current_recipe_id={household_meal.get('recipe_id')}",
        f"household_alternative_recipe_id={household_alt.get('recipe_id')}",
        f"household_apply_status={household_apply_status}",
        "json_checks=" + ";".join(json_checks),
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
