from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/heart_friendly_profile_summary.txt"
HOUSEHOLD_ID = "heart_friendly_check_household"


def _cleanup_previous_test_rows(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM member_profiles WHERE household_id = ?",
            (HOUSEHOLD_ID,),
        )


def _profile_request(enabled: bool = True) -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "display_name": "Heart Friendly Alex",
        "age": 39,
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
                "diabetes_aware": False,
                "hypertension_friendly": False,
                "heart_friendly": enabled,
            },
        },
        "bf_profile": "normal",
    }


def _scoring_checks() -> list[str]:
    from src.generator_v1.health_diet_fit import compute_health_and_diet_fit

    errors: list[str] = []
    preferences = {
        "dietary_patterns": {},
        "health_modes": {"heart_friendly": True},
    }
    heavy_recipe = compute_health_and_diet_fit(
        {
            "kcal": 650,
            "fat_g": 50,
            "health_proxy_flags": ["fatty_creamy", "fried_heavy", "processed_salty"],
        },
        preferences,
    )
    lighter_recipe = compute_health_and_diet_fit(
        {"kcal": 420, "fat_g": 12, "health_proxy_flags": []},
        preferences,
    )
    if float(heavy_recipe.get("health_and_diet_fit") or 1.0) >= float(
        lighter_recipe.get("health_and_diet_fit") or 0.0
    ):
        errors.append("heart_friendly_heavy_recipe_not_penalized")
    reasons = heavy_recipe.get("health_and_diet_reasons")
    if not isinstance(reasons, list) or not any("heart_friendly" in item for item in reasons):
        errors.append("heart_friendly_reasons_missing")
    return errors


def _generation_check(profile_payload: dict[str, Any]) -> list[str]:
    from src.generator_v1.service import generate_individual_plan_from_request

    errors: list[str] = []
    response = generate_individual_plan_from_request(
        {
            "dataset_profile": "v1_2_demo_final",
            "days": 1,
            "member_profile": profile_payload,
            "generation_options": {
                "profile_guard": "off",
                "feedback_enabled": False,
            },
        }
    )
    if response.get("status") != "ok":
        return [f"generation_status={response.get('status')}"]
    meals = ((response.get("daily_plan") or [{}])[0] or {}).get("selected_meals") or []
    if not meals:
        errors.append("generation_selected_meals_missing")
        return errors
    first_meal = meals[0]
    if "heart_friendly" not in first_meal.get("active_health_modes", []):
        errors.append("meal_active_health_modes_missing_heart_friendly")
    return errors


def _mobile_checks() -> list[str]:
    wizard = (
        PROJECT_ROOT / "mobile/src/components/AddMemberWizard.tsx"
    ).read_text(encoding="utf-8", errors="replace")
    errors: list[str] = []
    for marker in (
        "Heart-friendly",
        "heart_friendly",
        "These options help TableTogether prioritize and filter meals. They are not",
        "medical advice.",
    ):
        if marker not in wizard:
            errors.append(f"mobile_marker_missing:{marker}")
    lowered = wizard.lower()
    for marker in ("treat heart", "heart disease treatment", "diagnose", "cure"):
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
    profile_request = _profile_request(enabled=True)
    response = client.post("/profiles", json=profile_request)
    payload = response.json()
    if response.status_code != 200:
        errors.append(f"profile_status={response.status_code}")
    modes = (
        payload.get("health_and_diet_preferences", {}).get("health_modes")
        if isinstance(payload.get("health_and_diet_preferences"), dict)
        else {}
    )
    if not isinstance(modes, dict) or modes.get("heart_friendly") is not True:
        errors.append("profile_heart_friendly_not_returned")

    errors.extend(_scoring_checks())
    errors.extend(_generation_check(profile_request))
    errors.extend(_mobile_checks())

    status_ok = not errors
    summary_lines = [
        "Heart-friendly profile summary",
        "status=ok" if status_ok else "status=failed",
        f"db_path={db_path}",
        f"profile_status={response.status_code}",
        "profile_health_modes=" + json.dumps(modes, sort_keys=True),
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
