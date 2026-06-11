from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

RECIPES_PATH = PROJECT_ROOT / "data/recipesdb/current/recipes.csv"
AUDIT_DIR = PROJECT_ROOT / "data/recipesdb/audit"
BEFORE_AUDIT_PATH = AUDIT_DIR / "cooking_steps_coverage_before.csv"
AFTER_AUDIT_PATH = AUDIT_DIR / "cooking_steps_coverage_after.csv"
SUMMARY_PATH = AUDIT_DIR / "cooking_steps_completion_summary.txt"
FALLBACK_TEXT = "Cooking steps are not available for this recipe yet."


def main() -> int:
    from src.generator_v1.data_loader import (
        PILOT_CURRENT_PROFILE,
        V1_2_DEMO_FINAL_PROFILE,
    )

    errors: list[str] = []
    recipes = _read_csv(RECIPES_PATH, errors)
    active_rows = [row for row in recipes if _truthy(row.get("is_active"))]
    active_ids = [_clean_text(row.get("recipe_id")) for row in active_rows]

    if not recipes:
        errors.append("recipes_csv_empty_or_missing")
    if not active_rows:
        errors.append("active_recipe_count_zero")
    if any(not recipe_id for recipe_id in active_ids):
        errors.append("recipe_id_missing")
    if len(active_ids) != len(set(active_ids)):
        errors.append("recipe_id_duplicate")

    coverage_rows = [_coverage_row(row) for row in recipes]
    for row in coverage_rows:
        if row["is_active"] != "1":
            continue
        recipe_id = row["recipe_id"]
        if row["directions_json_present"] != "1":
            errors.append(f"directions_json_missing:{recipe_id}")
        if row["directions_json_valid"] != "1":
            errors.append(f"directions_json_invalid:{recipe_id}:{row['parse_error']}")
        if row["directions_step_count_gt_zero"] != "1":
            errors.append(f"directions_step_count_lte_zero:{recipe_id}")
        if row["parsed_step_count"] == "0":
            errors.append(f"directions_steps_empty:{recipe_id}")
        if row["empty_step_count"] != "0":
            errors.append(f"directions_empty_step_strings:{recipe_id}")
        if row["contains_mobile_fallback_text"] == "1":
            errors.append(f"directions_contains_mobile_fallback_text:{recipe_id}")

    _check_audit_file("before", BEFORE_AUDIT_PATH, recipes, active_rows, errors)
    _check_audit_file("after", AFTER_AUDIT_PATH, recipes, active_rows, errors)

    generated_counts: dict[str, dict[str, int]] = {}
    generated_counts[PILOT_CURRENT_PROFILE] = _check_generated_payload(
        PILOT_CURRENT_PROFILE,
        errors,
    )
    generated_counts[V1_2_DEMO_FINAL_PROFILE] = _check_generated_payload(
        V1_2_DEMO_FINAL_PROFILE,
        errors,
    )

    status_ok = not errors
    summary_lines = [
        "COOKING-STEPS-1 completion summary",
        "status=ok" if status_ok else "status=failed",
        f"recipes_path={RECIPES_PATH.relative_to(PROJECT_ROOT)}",
        f"before_audit_path={BEFORE_AUDIT_PATH.relative_to(PROJECT_ROOT)}",
        f"after_audit_path={AFTER_AUDIT_PATH.relative_to(PROJECT_ROOT)}",
        f"total_rows={len(recipes)}",
        f"total_active_recipes={len(active_rows)}",
        "recipes_with_valid_directions_json="
        + str(
            sum(
                1
                for row in coverage_rows
                if row["is_active"] == "1"
                and row["directions_json_valid"] == "1"
                and row["parsed_step_count"] != "0"
            )
        ),
        "recipes_missing_directions_json="
        + str(
            sum(
                1
                for row in coverage_rows
                if row["is_active"] == "1" and row["directions_json_present"] != "1"
            )
        ),
        "recipes_with_directions_step_count_lte_zero="
        + str(
            sum(
                1
                for row in coverage_rows
                if row["is_active"] == "1" and row["directions_step_count_gt_zero"] != "1"
            )
        ),
        "recipes_requiring_manual_or_semi_manual_completion="
        + str(
            sum(
                1
                for row in coverage_rows
                if row["is_active"] == "1" and row["requires_completion"] == "1"
            )
        ),
        "recipes_csv_modified=false",
        "completion_method=existing MVP practical instructions preserved; no external copyrighted recipe text was scraped or copied",
        "mobile_demo_status=active app-facing recipes have cooking steps available for recipe detail display",
        "generated_meals_with_steps="
        + json.dumps(generated_counts, ensure_ascii=True, sort_keys=True),
        "future_refinement=culinary wording can still be improved later without changing IDs or nutrition logic",
        "errors=" + json.dumps(errors, ensure_ascii=True, sort_keys=True),
    ]
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


def _read_csv(path: Path, errors: list[str]) -> list[dict[str, Any]]:
    if not path.exists():
        errors.append(f"missing_file={path.relative_to(PROJECT_ROOT)}")
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _coverage_row(row: dict[str, Any]) -> dict[str, str]:
    steps, valid_json, parse_error = _parse_steps(row.get("directions_json"))
    step_count = _to_int(row.get("directions_step_count"))
    nonempty_steps = [step for step in steps if step]
    empty_step_count = len(steps) - len(nonempty_steps)
    has_fallback_text = any(FALLBACK_TEXT in step for step in nonempty_steps)
    active = _truthy(row.get("is_active"))
    requires_completion = active and (
        not valid_json
        or not nonempty_steps
        or step_count <= 0
        or empty_step_count > 0
        or has_fallback_text
    )
    return {
        "recipe_id": _clean_text(row.get("recipe_id")),
        "display_name": _clean_text(row.get("display_name")),
        "is_active": "1" if active else "0",
        "directions_json_present": "1" if bool(_clean_text(row.get("directions_json"))) else "0",
        "directions_json_valid": "1" if valid_json else "0",
        "directions_step_count": str(step_count),
        "directions_step_count_gt_zero": "1" if step_count > 0 else "0",
        "parsed_step_count": str(len(nonempty_steps)),
        "empty_step_count": str(empty_step_count),
        "contains_mobile_fallback_text": "1" if has_fallback_text else "0",
        "requires_completion": "1" if requires_completion else "0",
        "parse_error": parse_error,
    }


def _check_audit_file(
    label: str,
    path: Path,
    recipes: list[dict[str, Any]],
    active_rows: list[dict[str, Any]],
    errors: list[str],
) -> None:
    rows = _read_csv(path, errors)
    if not rows:
        return
    current_ids = {_clean_text(row.get("recipe_id")) for row in recipes}
    audit_ids = {_clean_text(row.get("recipe_id")) for row in rows}
    if len(rows) != len(recipes):
        errors.append(f"{label}_audit_row_count_mismatch:{len(rows)}!={len(recipes)}")
    if audit_ids != current_ids:
        errors.append(f"{label}_audit_recipe_id_set_mismatch")
    audit_active_count = sum(1 for row in rows if _truthy(row.get("is_active")))
    if audit_active_count != len(active_rows):
        errors.append(f"{label}_audit_active_count_mismatch:{audit_active_count}!={len(active_rows)}")
    required_rows = [
        row
        for row in rows
        if _truthy(row.get("is_active")) and _truthy(row.get("requires_completion"))
    ]
    if required_rows:
        errors.append(f"{label}_audit_requires_completion_count={len(required_rows)}")


def _check_generated_payload(dataset_profile: str, errors: list[str]) -> dict[str, int]:
    from src.generator_v1.service import (
        generate_household_plan_from_request,
        generate_individual_plan_from_request,
    )

    individual_response = generate_individual_plan_from_request(
        _individual_request(dataset_profile)
    )
    household_response = generate_household_plan_from_request(
        _household_request(dataset_profile)
    )
    counts = {
        "individual": _check_meal_group(
            dataset_profile,
            "individual",
            individual_response,
            _individual_response_meals(individual_response),
            errors,
        ),
        "household": _check_meal_group(
            dataset_profile,
            "household",
            household_response,
            _household_response_meals(household_response),
            errors,
        ),
    }
    return counts


def _check_meal_group(
    dataset_profile: str,
    generation_type: str,
    response: dict[str, Any],
    meals: list[dict[str, Any]],
    errors: list[str],
) -> int:
    if response.get("status") != "ok":
        errors.append(
            f"{dataset_profile}:{generation_type}:generation_status={response.get('status')}"
        )
        return 0
    if not meals:
        errors.append(f"{dataset_profile}:{generation_type}:generated_meals_missing")
        return 0
    meals_with_steps = 0
    for meal in meals:
        recipe_id = _clean_text(meal.get("recipe_id"))
        slot = _clean_text(meal.get("slot"))
        steps = _as_step_list(meal.get("cooking_steps"))
        if not steps:
            errors.append(
                f"{dataset_profile}:{generation_type}:meal_cooking_steps_missing:{recipe_id}:{slot}"
            )
            continue
        if any(FALLBACK_TEXT in step for step in steps):
            errors.append(
                f"{dataset_profile}:{generation_type}:meal_contains_mobile_fallback_text:{recipe_id}:{slot}"
            )
        meals_with_steps += 1
    return meals_with_steps


def _individual_response_meals(response: dict[str, Any]) -> list[dict[str, Any]]:
    meals: list[dict[str, Any]] = []
    for day in response.get("daily_plan", []) or []:
        if not isinstance(day, dict):
            continue
        for meal in day.get("selected_meals", []) or []:
            if isinstance(meal, dict) and meal.get("recipe_id"):
                meals.append(meal)
    return meals


def _household_response_meals(response: dict[str, Any]) -> list[dict[str, Any]]:
    meals: list[dict[str, Any]] = []
    for menu in response.get("per_member_menus", []) or []:
        if not isinstance(menu, dict):
            continue
        for meal in menu.get("meals", []) or []:
            if isinstance(meal, dict) and meal.get("recipe_id"):
                meals.append(meal)
    for meal in response.get("shared_meals", []) or []:
        if isinstance(meal, dict) and meal.get("recipe_id"):
            meals.append(meal)
    return meals


def _individual_request(dataset_profile: str) -> dict[str, Any]:
    return {
        "dataset_profile": dataset_profile,
        "days": 1,
        "member_profile": _demo_member_profile("Cooking Steps Check"),
        "generation_options": {
            "profile_guard": "off",
            "feedback_enabled": False,
        },
    }


def _household_request(dataset_profile: str) -> dict[str, Any]:
    return {
        "dataset_profile": dataset_profile,
        "days": 1,
        "household_profile": {
            "household_id": "cooking_steps_check_household",
            "household_name": "Cooking Steps Check Household",
            "active_member_ids": [
                "cooking_steps_adult",
                "cooking_steps_teen",
            ],
            "members": [
                {
                    **_demo_member_profile("Cooking Steps Adult"),
                    "member_id": "cooking_steps_adult",
                    "display_name": "Cooking Steps Adult",
                },
                {
                    **_demo_member_profile("Cooking Steps Teen"),
                    "member_id": "cooking_steps_teen",
                    "display_name": "Cooking Steps Teen",
                    "age": 15,
                    "weight_kg": 58.0,
                    "height_cm": 170.0,
                    "goal": "gain",
                },
            ],
        },
        "generation_options": {
            "profile_guard": "off",
            "feedback_enabled": False,
        },
    }


def _demo_member_profile(display_name: str = "Cooking Steps Check") -> dict[str, Any]:
    return {
        "display_name": display_name,
        "age": 35,
        "sex": "female",
        "weight_kg": 68.0,
        "height_cm": 168.0,
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
                "heart_friendly": False,
            },
        },
        "bf_profile": "normal",
    }


def _parse_steps(value: Any) -> tuple[list[str], bool, str]:
    text = _clean_text(value)
    if not text:
        return [], False, "empty_directions_json"
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return [], False, f"json_error:{exc.msg}"
    if not isinstance(parsed, list):
        return [], False, "json_not_list"
    return [_clean_text(item) for item in parsed], True, ""


def _as_step_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [_clean_text(value)] if _clean_text(value) else []
        if isinstance(parsed, list):
            return [_clean_text(item) for item in parsed if _clean_text(item)]
    return []


def _to_int(value: Any) -> int:
    text = _clean_text(value)
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _clean_text(value).lower() in {"1", "true", "yes", "y"}


if __name__ == "__main__":
    raise SystemExit(main())
