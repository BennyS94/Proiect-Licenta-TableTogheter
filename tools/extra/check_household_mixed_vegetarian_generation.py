from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.service import generate_household_plan_from_request


VEGETARIAN_BANNED_KEYWORDS = {
    "anchovy",
    "bacon",
    "beef",
    "chicken",
    "clam",
    "cod",
    "crab",
    "fish",
    "ham",
    "halibut",
    "lamb",
    "meat",
    "mussel",
    "oyster",
    "pork",
    "prosciutto",
    "salmon",
    "salami",
    "sausage",
    "scallop",
    "seafood",
    "shrimp",
    "tilapia",
    "tuna",
    "turkey",
    "veal",
}


def main() -> int:
    response = generate_household_plan_from_request(_mixed_household_request())
    alice_menu = _find_menu(response, "alice")
    violations = _vegetarian_violations(alice_menu)
    normal_member_meat = {
        member_id: _contains_any_meat(_find_menu(response, member_id))
        for member_id in ("adrian", "marius")
    }

    print(f"status={response.get('status')}")
    print(f"alice_meals={len(alice_menu.get('meals', []))}")
    print(f"alice_vegetarian_violations={len(violations)}")
    print(f"normal_member_meat={normal_member_meat}")

    if violations:
        for violation in violations:
            print(
                "VIOLATION "
                f"day={violation['day_index']} "
                f"slot={violation['slot']} "
                f"recipe={violation['display_name']} "
                f"ingredient={violation['ingredient']}"
            )
        return 1

    if not any(normal_member_meat.values()):
        print(
            "WARNING normal members did not receive any meat-containing meal; "
            "this may be acceptable for a single generated day but should be watched."
        )
    return 0


def _mixed_household_request() -> dict[str, Any]:
    members = [
        {
            "member_id": "adrian",
            "display_name": "Adrian",
            "age": 42,
            "sex": "male",
            "height_cm": 180,
            "weight_kg": 88,
            "activity_level": "moderately_active",
            "goal": "maintain",
            "goal_speed": "normal",
            "training": {"type": "running", "sessions_per_week": 3},
            "meal_config": {
                "meals_per_day": 3,
                "include_snacks": True,
                "day_structure": "3_meals_plus_snack",
                "time_sensitivity": "normal",
            },
            "dietary_preferences": {},
            "food_preferences": {},
            "health_and_diet_preferences": {},
        },
        {
            "member_id": "alice",
            "display_name": "Alice",
            "age": 39,
            "sex": "female",
            "height_cm": 166,
            "weight_kg": 64,
            "activity_level": "lightly_active",
            "goal": "maintain",
            "goal_speed": "normal",
            "training": {"type": "yoga", "sessions_per_week": 2},
            "meal_config": {
                "meals_per_day": 3,
                "include_snacks": True,
                "day_structure": "3_meals_plus_snack",
                "time_sensitivity": "normal",
            },
            "dietary_preferences": {"vegetarian": True},
            "food_preferences": {},
            "health_and_diet_preferences": {},
        },
        {
            "member_id": "marius",
            "display_name": "Marius",
            "age": 16,
            "sex": "male",
            "height_cm": 176,
            "weight_kg": 68,
            "activity_level": "very_active",
            "goal": "gain",
            "goal_speed": "normal",
            "training": {"type": "weights", "sessions_per_week": 4},
            "meal_config": {
                "meals_per_day": 3,
                "include_snacks": True,
                "day_structure": "3_meals_plus_snack",
                "time_sensitivity": "normal",
            },
            "dietary_preferences": {},
            "food_preferences": {},
            "health_and_diet_preferences": {},
        },
    ]
    return {
        "dataset_profile": "v1_2_demo_final",
        "days": 1,
        "household_mode": "individual_breakfast_shared_main",
        "household_allocation_mode": "macro_aware_simple",
        "selected_member_ids": ["adrian", "alice", "marius"],
        "household_profile": {
            "household_id": "mixed_vegetarian_smoke",
            "household_name": "Mixed Vegetarian Smoke",
            "active_member_ids": ["adrian", "alice", "marius"],
            "household_preferences": {},
            "members": members,
        },
        "generation_options": {
            "include_grocery_list": True,
            "include_purchase_suggestions": True,
            "include_price_estimates": True,
            "quality_gate": "demo_safe",
            "profile_guard": "off",
            "multi_day_mode": "off",
        },
    }


def _find_menu(response: Mapping[str, Any], member_id: str) -> dict[str, Any]:
    for menu in response.get("per_member_menus", []) or []:
        if str(menu.get("member_id") or "") == member_id:
            return dict(menu)
    return {"member_id": member_id, "meals": []}


def _vegetarian_violations(menu: Mapping[str, Any]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for meal in menu.get("meals", []) or []:
        for text in [str(meal.get("display_name") or ""), *_meal_ingredient_texts(meal)]:
            if _contains_banned_keyword(text):
                violations.append(
                    {
                        "day_index": menu.get("day_index"),
                        "slot": meal.get("slot"),
                        "display_name": meal.get("display_name"),
                        "ingredient": text,
                    }
                )
    return violations


def _contains_any_meat(menu: Mapping[str, Any]) -> bool:
    for meal in menu.get("meals", []) or []:
        if any(_contains_banned_keyword(item) for item in _meal_ingredient_texts(meal)):
            return True
    return False


def _meal_ingredient_texts(meal: Mapping[str, Any]) -> list[str]:
    amounts = meal.get("ingredient_amounts")
    if isinstance(amounts, list):
        return [
            str(item.get("ingredient") or item.get("text") or "")
            for item in amounts
            if isinstance(item, Mapping)
        ]
    ingredients = meal.get("ingredients")
    if isinstance(ingredients, list):
        return [str(item) for item in ingredients]
    return []


def _contains_banned_keyword(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(text).lower()).strip()
    return any(
        re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", normalized)
        for keyword in VEGETARIAN_BANNED_KEYWORDS
    )


if __name__ == "__main__":
    raise SystemExit(main())
