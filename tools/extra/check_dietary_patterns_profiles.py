from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/dietary_pattern_profile_options_summary.txt"
HOUSEHOLD_ID = "dietary_pattern_check_household"


def _cleanup_previous_test_rows(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM member_profiles WHERE household_id = ?",
            (HOUSEHOLD_ID,),
        )


def _profile_request() -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "display_name": "Diet Pattern Alex",
        "age": 35,
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
        "food_preferences": {
            "ratings": {},
            "avoid_ingredients": [],
            "cooking_time_preference": "balanced",
        },
        "health_and_diet_preferences": {
            "dietary_patterns": {
                "keto": True,
                "paleo": False,
                "mediterranean": False,
            },
            "health_modes": {
                "diabetes_aware": False,
                "hypertension_friendly": False,
                "heart_friendly": False,
            },
        },
        "bf_profile": "normal",
    }


def _candidate_filter_checks() -> list[str]:
    from src.generator_v1.candidate_filter import (
        build_household_preference_context,
        filter_recipe_candidates,
    )

    errors: list[str] = []
    eligible_candidates = pd.DataFrame(
        [
            {"recipe_id": "recipe_rice", "display_name": "Rice test"},
            {"recipe_id": "recipe_egg", "display_name": "Egg test"},
        ]
    )
    ingredients = pd.DataFrame(
        [
            {
                "recipe_id": "recipe_rice",
                "ingredient_name_normalized": "white rice",
                "ingredient_name_parsed": "white rice",
                "mapped_food_canonical_name": "white rice",
                "ingredient_raw_text": "1 cup white rice",
            },
            {
                "recipe_id": "recipe_egg",
                "ingredient_name_normalized": "egg",
                "ingredient_name_parsed": "egg",
                "mapped_food_canonical_name": "egg",
                "ingredient_raw_text": "2 eggs",
            },
        ]
    )
    context = build_household_preference_context(
        {
            "health_and_diet_preferences": {
                "dietary_patterns": {"keto": True},
                "health_modes": {},
            }
        }
    )
    filtered = filter_recipe_candidates(
        eligible_candidates=eligible_candidates,
        ingredients=ingredients,
        context=context,
    )
    recipe_ids = set(filtered["recipe_id"].astype(str))
    if "recipe_rice" in recipe_ids:
        errors.append("keto_pattern_did_not_filter_rice")
    if "recipe_egg" not in recipe_ids:
        errors.append("keto_pattern_removed_neutral_recipe")
    return errors


def _generation_check(profile_payload: dict[str, Any]) -> list[str]:
    from src.generator_v1.service import generate_individual_plan_from_request

    errors: list[str] = []
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
        return [f"generation_status={response.get('status')}"]
    meals = ((response.get("daily_plan") or [{}])[0] or {}).get("selected_meals") or []
    if not meals:
        errors.append("generation_selected_meals_missing")
        return errors
    first_meal = meals[0]
    if "health_and_diet_fit" not in first_meal:
        errors.append("meal_health_and_diet_fit_missing")
    if "keto" not in first_meal.get("active_dietary_patterns", []):
        errors.append("meal_active_dietary_patterns_missing_keto")
    return errors


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import init_db
    from backend.app.main import app

    db_path = init_db()
    _cleanup_previous_test_rows(db_path)

    client = TestClient(app)
    errors: list[str] = []
    profile_request = _profile_request()
    response = client.post("/profiles", json=profile_request)
    payload = response.json()
    if response.status_code != 200:
        errors.append(f"profile_status={response.status_code}")
    preferences = payload.get("health_and_diet_preferences")
    if not isinstance(preferences, dict):
        errors.append("profile_health_and_diet_preferences_missing")
    else:
        patterns = preferences.get("dietary_patterns")
        if not isinstance(patterns, dict) or patterns.get("keto") is not True:
            errors.append("profile_keto_pattern_not_returned")
        modes = preferences.get("health_modes")
        if not isinstance(modes, dict) or modes.get("diabetes_aware") is not False:
            errors.append("profile_health_modes_defaults_missing")

    list_response = client.get(f"/profiles?household_id={HOUSEHOLD_ID}")
    list_payload = list_response.json()
    if list_response.status_code != 200:
        errors.append(f"profile_list_status={list_response.status_code}")
    listed = list_payload.get("profiles")
    if not isinstance(listed, list) or not listed:
        errors.append("profile_list_missing_created_profile")
    elif "health_and_diet_preferences" not in listed[0]:
        errors.append("profile_list_health_and_diet_preferences_missing")

    errors.extend(_candidate_filter_checks())
    errors.extend(_generation_check(profile_request))

    status_ok = not errors
    summary_lines = [
        "Dietary pattern profile options summary",
        "status=ok" if status_ok else "status=failed",
        f"db_path={db_path}",
        f"profile_status={response.status_code}",
        f"profile_list_status={list_response.status_code}",
        "profile_health_and_diet_preferences="
        + json.dumps(payload.get("health_and_diet_preferences", {}), sort_keys=True),
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
