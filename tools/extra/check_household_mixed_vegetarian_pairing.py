from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.service import generate_household_plan_from_request
from tools.extra.check_household_mixed_vegetarian_generation import (
    _contains_any_meat,
    _mixed_household_request,
    _vegetarian_violations,
)


def main() -> int:
    request = _mixed_household_request()
    request["days"] = 3
    request["generation_options"]["multi_day_mode"] = "global_alternatives_3_day"
    request["generation_options"]["multi_day_no_repeat_policy"] = "hard"

    response = generate_household_plan_from_request(request)
    plan = _mapping(response.get("generator_plan"))
    menus_by_member = _menus_by_member(response)
    individual_meals = _list(plan.get("individual_meals"))
    partial_shared = _partial_shared_meals(plan)

    alice_meals = _all_member_meals(menus_by_member, "alice")
    adrian_meals = _all_member_meals(menus_by_member, "adrian")
    marius_meals = _all_member_meals(menus_by_member, "marius")
    alice_violations = _vegetarian_violations({"meals": alice_meals})
    normal_member_meat = {
        "adrian": _contains_any_meat({"meals": adrian_meals}),
        "marius": _contains_any_meat({"meals": marius_meals}),
    }
    companion_selected_count = sum(
        1
        for row in individual_meals
        if bool(row.get("mixed_vegetarian_companion_selected"))
    )
    unpaired_partial_count = sum(
        1
        for meal in partial_shared
        if str(meal.get("mixed_vegetarian_pairing_status") or "") != "paired"
    )

    print("HOUSEHOLD MIXED VEGETARIAN PAIRING CHECK")
    print(f"status={response.get('status')}")
    print(f"alice_meals={len(alice_meals)}")
    print(f"alice_unique_meals={_unique_meal_count(alice_meals)}")
    print(f"alice_vegetarian_violations={len(alice_violations)}")
    print(f"normal_member_meat={normal_member_meat}")
    print(f"partial_shared_count={len(partial_shared)}")
    print(f"unpaired_partial_count={unpaired_partial_count}")
    print(f"companion_selected_count={companion_selected_count}")
    for member_id, meals in {
        "adrian": adrian_meals,
        "alice": alice_meals,
        "marius": marius_meals,
    }.items():
        print(
            f"{member_id}_meal_count={len(meals)} "
            f"{member_id}_unique_meals={_unique_meal_count(meals)}"
        )

    errors: list[str] = []
    if response.get("status") != "ok":
        errors.append("generation_status_not_ok")
    if alice_violations:
        errors.append("vegetarian_member_has_meat_violation")
    if not all(normal_member_meat.values()):
        errors.append("normal_members_missing_meat_meals")
    if len(alice_meals) != 12:
        errors.append("vegetarian_member_missing_three_day_menu")
    if _unique_meal_count(alice_meals) < 10:
        errors.append("vegetarian_member_repetition_too_high")
    if companion_selected_count < 2:
        errors.append("vegetarian_companion_selection_not_used")
    if unpaired_partial_count > 0:
        errors.append("partial_shared_meals_without_pairing")

    print(f"errors={errors}")
    return 1 if errors else 0


def _menus_by_member(response: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    result: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for menu in _list(response.get("per_member_menus")):
        if isinstance(menu, Mapping):
            result[str(menu.get("member_id") or "")].append(menu)
    return result


def _all_member_meals(
    menus_by_member: Mapping[str, list[Mapping[str, Any]]],
    member_id: str,
) -> list[Mapping[str, Any]]:
    meals: list[Mapping[str, Any]] = []
    for menu in menus_by_member.get(member_id, []):
        meals.extend(_list(menu.get("meals")))
    return meals


def _unique_meal_count(meals: list[Mapping[str, Any]]) -> int:
    return len({str(meal.get("display_name") or meal.get("recipe_id") or "") for meal in meals})


def _partial_shared_meals(plan: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    meals: list[Mapping[str, Any]] = []
    for day in _list(plan.get("days")):
        for meal in _list(day.get("selected_meals")):
            if isinstance(meal, Mapping) and bool(meal.get("household_dietary_partial_shared")):
                meals.append(meal)
    return meals


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


if __name__ == "__main__":
    raise SystemExit(main())
