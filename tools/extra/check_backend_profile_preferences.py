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

SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/backend_profile_preferences_summary.txt"
HOUSEHOLD_ID = "profile_preferences_check_household"


def _cleanup_previous_test_rows(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM member_profiles WHERE household_id = ?",
            (HOUSEHOLD_ID,),
        )


def _new_profile_request() -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "display_name": "Preference Check Alex",
        "age": 35,
        "sex": "male",
        "weight_kg": 82.0,
        "height_cm": 180.0,
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
            "vegetarian": False,
            "vegan": False,
            "gluten_free": False,
            "no_beef": False,
            "no_pork": True,
            "no_chicken": False,
            "no_fish": False,
            "no_dairy": False,
        },
        "food_preferences": {
            "ratings": {
                "chicken": "like",
                "pork": "avoid",
                "fish": "dislike",
            },
            "avoid_ingredients": ["mushroom"],
            "cooking_time_preference": "quick",
        },
        "bf_profile": "normal",
    }


def _old_profile_request() -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "display_name": "Preference Check Legacy",
        "age": 34,
        "sex": "female",
        "weight_kg": 64.0,
        "height_cm": 168.0,
        "activity_level": "lightly_active",
        "goal": "maintain",
        "goal_speed": "normal",
        "training": {
            "sessions_per_week": 2,
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


def _candidate_filter_checks() -> list[str]:
    from src.generator_v1.candidate_filter import (
        build_household_preference_context,
        filter_recipe_candidates,
    )

    errors: list[str] = []
    eligible_candidates = pd.DataFrame(
        [
            {"recipe_id": "recipe_pork", "display_name": "Pork test"},
            {"recipe_id": "recipe_chicken", "display_name": "Chicken test"},
        ]
    )
    ingredients = pd.DataFrame(
        [
            {
                "recipe_id": "recipe_pork",
                "ingredient_name_normalized": "pork tenderloin",
                "ingredient_name_parsed": "pork tenderloin",
                "mapped_food_canonical_name": "pork tenderloin",
                "ingredient_raw_text": "1 lb pork tenderloin",
            },
            {
                "recipe_id": "recipe_chicken",
                "ingredient_name_normalized": "chicken breast",
                "ingredient_name_parsed": "chicken breast",
                "mapped_food_canonical_name": "chicken breast",
                "ingredient_raw_text": "1 lb chicken breast",
            },
        ]
    )

    no_pork_context = build_household_preference_context(
        {"dietary_preferences": {"no_pork": True}}
    )
    no_pork_filtered = filter_recipe_candidates(
        eligible_candidates=eligible_candidates,
        ingredients=ingredients,
        context=no_pork_context,
    )
    no_pork_ids = set(no_pork_filtered["recipe_id"].astype(str))
    if "recipe_pork" in no_pork_ids:
        errors.append("candidate_filter_no_pork_failed")
    if "recipe_chicken" not in no_pork_ids:
        errors.append("candidate_filter_no_pork_removed_chicken")

    rating_context = build_household_preference_context(
        {"food_preferences": {"ratings": {"pork": "avoid"}}}
    )
    rating_filtered = filter_recipe_candidates(
        eligible_candidates=eligible_candidates,
        ingredients=ingredients,
        context=rating_context,
    )
    rating_ids = set(rating_filtered["recipe_id"].astype(str))
    if "recipe_pork" in rating_ids:
        errors.append("candidate_filter_pork_rating_avoid_failed")
    if rating_context.dietary_preferences.get("no_pork") is not True:
        errors.append("pork_rating_avoid_did_not_set_no_pork")

    return errors


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    db_path = init_db()
    _cleanup_previous_test_rows(db_path)

    client = TestClient(app)
    errors: list[str] = []

    new_response = client.post("/profiles", json=_new_profile_request())
    new_payload = new_response.json()
    if new_response.status_code != 200:
        errors.append(f"new_profile_status={new_response.status_code}")
    if new_payload.get("dietary_preferences", {}).get("no_pork") is not True:
        errors.append("new_profile_no_pork_not_returned")
    food_preferences = new_payload.get("food_preferences")
    if not isinstance(food_preferences, dict):
        errors.append("new_profile_food_preferences_missing")
    else:
        ratings = food_preferences.get("ratings")
        if not isinstance(ratings, dict) or ratings.get("pork") != "avoid":
            errors.append("new_profile_ratings_not_returned")
        if food_preferences.get("avoid_ingredients") != ["mushroom"]:
            errors.append("new_profile_avoid_ingredients_not_returned")
        if food_preferences.get("cooking_time_preference") != "quick":
            errors.append("new_profile_cooking_time_not_returned")

    old_response = client.post("/profiles", json=_old_profile_request())
    old_payload = old_response.json()
    if old_response.status_code != 200:
        errors.append(f"old_profile_status={old_response.status_code}")
    if old_payload.get("dietary_preferences", {}).get("no_pork") is not False:
        errors.append("old_profile_no_pork_default_failed")
    old_food_preferences = old_payload.get("food_preferences")
    if old_food_preferences != {
        "ratings": {},
        "avoid_ingredients": [],
        "cooking_time_preference": "balanced",
    }:
        errors.append("old_profile_food_preferences_default_failed")

    list_response = client.get(f"/profiles?household_id={HOUSEHOLD_ID}")
    list_payload = list_response.json()
    if list_response.status_code != 200:
        errors.append(f"profile_list_status={list_response.status_code}")
    profiles = list_payload.get("profiles")
    if not isinstance(profiles, list) or len(profiles) < 2:
        errors.append("profile_list_missing_created_profiles")
    elif not all("food_preferences" in profile for profile in profiles):
        errors.append("profile_list_food_preferences_missing")

    errors.extend(_candidate_filter_checks())

    status_ok = not errors
    summary_lines = [
        "Backend profile preferences summary",
        "status=ok" if status_ok else "status=failed",
        f"db_path={db_path}",
        f"new_profile_status={new_response.status_code}",
        f"old_profile_status={old_response.status_code}",
        f"profile_list_status={list_response.status_code}",
        "new_profile_food_preferences="
        + json.dumps(new_payload.get("food_preferences", {}), sort_keys=True),
        "old_profile_food_preferences="
        + json.dumps(old_payload.get("food_preferences", {}), sort_keys=True),
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
