from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

GROCERY_AUDIT_DIR = PROJECT_ROOT / "data/grocery/audit"
RECIPES_AUDIT_DIR = PROJECT_ROOT / "data/recipesdb/audit"
PRICE_COVERAGE_PATH = GROCERY_AUDIT_DIR / "data_qa_price_coverage_items.csv"
TIME_COVERAGE_PATH = RECIPES_AUDIT_DIR / "data_qa_time_coverage_recipes.csv"
COOKING_STEPS_SUMMARY_PATH = RECIPES_AUDIT_DIR / "data_qa_cooking_steps_missing_summary.txt"
SUMMARY_PATH = RECIPES_AUDIT_DIR / "data_qa_price_time_no_missing_summary.txt"

PRICE_METHODS = {
    "exact_catalog",
    "alias_catalog",
    "category_fallback",
    "emergency_fallback",
    "pantry_check",
}
PRICE_CONFIDENCES = {
    "high_source_exact",
    "medium_source_equivalent",
    "low_category_fallback",
    "very_low_emergency_fallback",
}
TIME_FIELDS = (
    "total_elapsed_time_min",
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "effective_time_min_for_scoring",
    "time_confidence",
    "time_estimation_method",
)
MEAL_TIME_FIELDS = (
    "total_time_min",
    "total_elapsed_time_min",
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "effective_time_min_for_scoring",
    "time_confidence",
    "time_estimation_method",
)


def main() -> int:
    from src.generator_v1.data_loader import V1_2_DEMO_FINAL_PROFILE
    from src.generator_v1.service import (
        generate_household_plan_from_request,
        generate_individual_plan_from_request,
    )

    errors: list[str] = []
    warnings: list[str] = []

    price_rows = _read_csv(PRICE_COVERAGE_PATH, errors)
    time_rows = _read_csv(TIME_COVERAGE_PATH, errors)
    app_price_rows = [
        row
        for row in price_rows
        if _clean_text(row.get("current_output_context")) != "product_catalog_reference"
    ]
    generated_price_rows = [
        row
        for row in app_price_rows
        if _clean_text(row.get("current_output_context")).startswith(
            ("individual_generated", "household_generated", "individual_replacement")
        )
    ]
    fallback_rows = [
        row
        for row in app_price_rows
        if _clean_text(row.get("price_estimation_method")) in {"category_fallback", "emergency_fallback"}
    ]
    emergency_rows = [
        row
        for row in app_price_rows
        if _clean_text(row.get("price_estimation_method")) == "emergency_fallback"
    ]

    missing_price_rows = [
        row
        for row in app_price_rows
        if _truthy(row.get("missing_after_task")) or _missing(row.get("estimated_price_ron"))
    ]
    if missing_price_rows:
        errors.append(f"price_missing_after_task_count={len(missing_price_rows)}")
    for index, row in enumerate(app_price_rows, start=2):
        _check_price_row(row, index, errors)
    if fallback_rows and not price_rows:
        errors.append("fallback_rows_not_listed_in_price_audit")

    missing_time_rows = [
        row
        for row in time_rows
        if _truthy(row.get("missing_after_task"))
        or any(_missing(row.get(field)) for field in TIME_FIELDS)
    ]
    if missing_time_rows:
        errors.append(f"time_missing_after_task_count={len(missing_time_rows)}")

    request_options = {
        "include_grocery_list": True,
        "include_purchase_suggestions": True,
        "include_price_estimates": True,
        "feedback_enabled": False,
    }
    individual_response = generate_individual_plan_from_request(
        {
            "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
            "days": 1,
            "generation_options": request_options,
            "include_grocery_list": True,
            "include_purchase_suggestions": True,
            "include_price_estimates": True,
            "feedback_enabled": False,
        }
    )
    household_response = generate_household_plan_from_request(
        {
            "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
            "days": 1,
            "generation_options": request_options,
            "include_grocery_list": True,
            "include_purchase_suggestions": True,
            "include_price_estimates": True,
            "feedback_enabled": False,
        }
    )

    individual_meals = _individual_meals(individual_response)
    household_meals = _household_meals(household_response)
    _check_meals("individual_generated_1_day", individual_meals, errors)
    _check_meals("household_generated_1_day", household_meals, errors)
    _check_grocery("individual_generated_1_day", individual_response.get("grocery_list") or {}, errors)
    _check_grocery(
        "household_generated_1_day",
        household_response.get("household_grocery_list") or {},
        errors,
    )

    cooking_steps_status = _cooking_steps_status()
    if not cooking_steps_status:
        warnings.append("cooking_steps_summary_missing_or_empty")

    status = "ok" if not errors else "failed"
    summary_lines = [
        "DATA-QA-1 price/time hard checker summary",
        f"status={status}",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"price_coverage_path={PRICE_COVERAGE_PATH.relative_to(PROJECT_ROOT)}",
        f"time_coverage_path={TIME_COVERAGE_PATH.relative_to(PROJECT_ROOT)}",
        f"app_price_rows_checked={len(app_price_rows)}",
        f"generated_price_rows_checked={len(generated_price_rows)}",
        f"price_missing_after_task={len(missing_price_rows)}",
        f"fallback_price_rows_listed={len(fallback_rows)}",
        f"emergency_fallback_rows_listed={len(emergency_rows)}",
        f"time_rows_checked={len(time_rows)}",
        f"time_missing_after_task={len(missing_time_rows)}",
        f"generated_individual_meals_checked={len(individual_meals)}",
        f"generated_household_meals_checked={len(household_meals)}",
        f"cooking_steps_summary_present={str(bool(cooking_steps_status)).lower()}",
        f"errors={json.dumps(errors, ensure_ascii=True, sort_keys=True)}",
        f"warnings={json.dumps(warnings, ensure_ascii=True, sort_keys=True)}",
    ]
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if not errors else 1


def _read_csv(path: Path, errors: list[str]) -> list[dict[str, Any]]:
    if not path.exists():
        errors.append(f"missing_audit_file={path.relative_to(PROJECT_ROOT)}")
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _check_price_row(row: dict[str, Any], line_number: int, errors: list[str]) -> None:
    if _truthy(row.get("not_for_purchase")):
        return
    method = _clean_text(row.get("price_estimation_method"))
    confidence = _clean_text(row.get("price_confidence"))
    if _missing(row.get("estimated_price_ron")):
        errors.append(f"price_missing:line={line_number}:item={row.get('item_name')}")
    if _missing(row.get("currency")):
        errors.append(f"price_currency_missing:line={line_number}:item={row.get('item_name')}")
    if method not in PRICE_METHODS:
        errors.append(f"price_method_invalid:line={line_number}:method={method}")
    if confidence not in PRICE_CONFIDENCES:
        errors.append(f"price_confidence_invalid:line={line_number}:confidence={confidence}")
    if method in {"category_fallback", "emergency_fallback"} and _missing(row.get("price_fallback_id")):
        errors.append(f"price_fallback_id_missing:line={line_number}:item={row.get('item_name')}")


def _check_meals(context: str, meals: list[dict[str, Any]], errors: list[str]) -> None:
    if not meals:
        errors.append(f"{context}:no_meals_found")
        return
    for meal in meals:
        recipe_id = _clean_text(meal.get("recipe_id"))
        slot = _clean_text(meal.get("slot"))
        for field in MEAL_TIME_FIELDS:
            if _missing(meal.get(field)):
                errors.append(f"{context}:meal_time_missing:{recipe_id}:{slot}:{field}")


def _check_grocery(context: str, grocery: dict[str, Any], errors: list[str]) -> None:
    items = grocery.get("items") or grocery.get("display_items") or []
    if not isinstance(items, list) or not items:
        errors.append(f"{context}:grocery_items_missing")
        return
    for item in items:
        if not isinstance(item, dict) or item.get("not_for_purchase"):
            continue
        name = _clean_text(item.get("display_name_clean") or item.get("display_name") or item.get("canonical_name"))
        if _missing(item.get("estimated_cost")):
            errors.append(f"{context}:grocery_price_missing:{name}")
        if _clean_text(item.get("price_estimation_method")) not in PRICE_METHODS:
            errors.append(f"{context}:grocery_price_method_invalid:{name}")
        if _clean_text(item.get("price_confidence")) not in PRICE_CONFIDENCES:
            errors.append(f"{context}:grocery_price_confidence_invalid:{name}")
        if _missing(item.get("price_unit_basis")):
            errors.append(f"{context}:grocery_price_unit_basis_missing:{name}")


def _individual_meals(response: dict[str, Any]) -> list[dict[str, Any]]:
    meals: list[dict[str, Any]] = []
    for day in response.get("daily_plan", []) or []:
        if not isinstance(day, dict):
            continue
        for meal in day.get("selected_meals", []) or []:
            if isinstance(meal, dict) and meal.get("recipe_id"):
                meals.append(meal)
    return meals


def _household_meals(response: dict[str, Any]) -> list[dict[str, Any]]:
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


def _cooking_steps_status() -> str:
    if not COOKING_STEPS_SUMMARY_PATH.exists():
        return ""
    return COOKING_STEPS_SUMMARY_PATH.read_text(encoding="utf-8").strip()


def _missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    text = _clean_text(value)
    return text.lower() in {"", "nan", "none", "null", "undefined"}


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
