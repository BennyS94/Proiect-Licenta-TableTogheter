from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/diabetes_aware_profile_summary.txt"
HOUSEHOLD_ID = "diabetes_aware_check_household"


def _cleanup_previous_test_rows(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM member_profiles WHERE household_id = ?",
            (HOUSEHOLD_ID,),
        )


def _profile_request(diabetes_aware: bool = True) -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "display_name": "Diabetes Aware Alex",
        "age": 36,
        "sex": "male",
        "weight_kg": 82.0,
        "height_cm": 180.0,
        "activity_level": "moderately_active",
        "goal": "maintain",
        "goal_speed": "normal",
        "training": {"sessions_per_week": 3, "type": "mixed"},
        "meal_config": {
            "meals_per_day": 3,
            "include_snacks": True,
            "day_structure": "3_meals_plus_snack",
        },
        "dietary_preferences": {},
        "food_preferences": {
            "ratings": {},
            "avoid_ingredients": [],
            "cooking_time_preference": "balanced",
        },
        "health_and_diet_preferences": {
            "dietary_patterns": {
                "keto": False,
                "paleo": False,
                "mediterranean": False,
            },
            "health_modes": {
                "diabetes_aware": diabetes_aware,
                "hypertension_friendly": False,
                "heart_friendly": False,
            },
        },
        "bf_profile": "normal",
    }


def _scoring_checks() -> list[str]:
    from src.generator_v1.health_diet_fit import compute_health_and_diet_fit

    errors: list[str] = []
    preferences = {
        "dietary_patterns": {},
        "health_modes": {"diabetes_aware": True},
    }
    high_carb_low_protein = compute_health_and_diet_fit(
        {"carbs_g": 85, "protein_g": 6, "sugars_g": 32},
        preferences,
    )
    moderate_plate = compute_health_and_diet_fit(
        {"carbs_g": 30, "protein_g": 22, "sugars_g": 6},
        preferences,
    )
    if float(high_carb_low_protein.get("health_and_diet_fit") or 1.0) >= float(
        moderate_plate.get("health_and_diet_fit") or 0.0
    ):
        errors.append("diabetes_aware_high_carb_not_penalized")
    reasons = high_carb_low_protein.get("health_and_diet_reasons")
    if not isinstance(reasons, list) or not any("diabetes_aware" in item for item in reasons):
        errors.append("diabetes_aware_reasons_missing")
    return errors


def _generation_check(profile_payload: dict[str, Any]) -> list[str]:
    from src.generator_v1.service import generate_individual_plan_from_request
    from src.generator_v1.target_builder import build_nutrition_target

    errors: list[str] = []
    baseline_target = build_nutrition_target(_profile_request(diabetes_aware=False))
    diabetes_target = build_nutrition_target(profile_payload)
    if baseline_target != diabetes_target:
        errors.append("diabetes_aware_changed_macro_targets")

    response = generate_individual_plan_from_request(
        {
            "dataset_profile": "current",
            "days": 1,
            "member_profile": profile_payload,
            "generation_options": {
                "profile_guard": "off",
                "feedback_enabled": False,
            },
        }
    )
    if response.get("status") != "ok":
        return [*errors, f"generation_status={response.get('status')}"]
    meals = ((response.get("daily_plan") or [{}])[0] or {}).get("selected_meals") or []
    if not meals:
        errors.append("generation_selected_meals_missing")
        return errors
    first_meal = meals[0]
    if "diabetes_aware" not in first_meal.get("active_health_modes", []):
        errors.append("meal_active_health_modes_missing_diabetes_aware")
    return errors


def _mobile_checks() -> list[str]:
    wizard_path = PROJECT_ROOT / "mobile/src/components/AddMemberWizard.tsx"
    wizard = wizard_path.read_text(encoding="utf-8", errors="replace")
    errors: list[str] = []
    required = [
        "Diabetes-aware",
        "health_and_diet_preferences",
        "diabetes_aware",
        "These options help TableTogether prioritize and filter meals. They are not",
        "medical advice.",
    ]
    for marker in required:
        if marker not in wizard:
            errors.append(f"mobile_marker_missing:{marker}")
    forbidden = ["treat diabetes", "diagnose", "medical treatment", "cure"]
    lowered = wizard.lower()
    for marker in forbidden:
        if marker in lowered:
            errors.append(f"medical_claim_marker_present:{marker}")
    return errors


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import init_db
    from backend.app.main import app

    db_path = init_db()
    _cleanup_previous_test_rows(db_path)

    client = TestClient(app)
    errors: list[str] = []
    profile_request = _profile_request(diabetes_aware=True)
    response = client.post("/profiles", json=profile_request)
    payload = response.json()
    if response.status_code != 200:
        errors.append(f"profile_status={response.status_code}")
    modes = (
        payload.get("health_and_diet_preferences", {}).get("health_modes")
        if isinstance(payload.get("health_and_diet_preferences"), dict)
        else {}
    )
    if not isinstance(modes, dict) or modes.get("diabetes_aware") is not True:
        errors.append("profile_diabetes_aware_not_returned")

    old_response = client.post("/profiles", json=_profile_request(diabetes_aware=False))
    old_payload = old_response.json()
    old_modes = (
        old_payload.get("health_and_diet_preferences", {}).get("health_modes")
        if isinstance(old_payload.get("health_and_diet_preferences"), dict)
        else {}
    )
    if old_response.status_code != 200:
        errors.append(f"old_profile_status={old_response.status_code}")
    if not isinstance(old_modes, dict) or old_modes.get("diabetes_aware") is not False:
        errors.append("old_profile_diabetes_default_failed")

    errors.extend(_scoring_checks())
    errors.extend(_generation_check(profile_request))
    errors.extend(_mobile_checks())

    status_ok = not errors
    summary_lines = [
        "Diabetes-aware profile summary",
        "status=ok" if status_ok else "status=failed",
        f"db_path={db_path}",
        f"profile_status={response.status_code}",
        f"old_profile_status={old_response.status_code}",
        "profile_health_modes=" + json.dumps(modes, sort_keys=True),
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
