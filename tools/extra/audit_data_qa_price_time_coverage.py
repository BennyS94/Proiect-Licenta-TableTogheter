from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.data_loader import (
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.grocery_list import build_grocery_list
from src.generator_v1.grocery_pricing import (
    DEFAULT_PRICE_FALLBACKS_PATH,
    DEFAULT_PRODUCT_ALIASES_PATH,
    REFERENCE_PRODUCT_CATALOG_PATH,
    apply_price_estimates,
    estimate_grocery_item_cost,
    load_grocery_price_fallbacks,
    load_grocery_product_aliases,
    load_grocery_product_catalog,
    match_catalog_item,
)
from src.generator_v1.grocery_purchase import (
    DEFAULT_PURCHASE_RULES_PATH,
    apply_purchase_rules,
    load_grocery_purchase_rules,
)
from src.generator_v1.grocery_cooked_raw import DEFAULT_COOKED_TO_RAW_RULES_PATH
from src.generator_v1.service import (
    apply_meal_replacement_from_request,
    generate_household_plan_from_request,
    generate_individual_plan_from_request,
    get_recipe_alternatives_from_request,
)


GROCERY_AUDIT_DIR = ROOT / "data/grocery/audit"
GROCERY_DRAFT_DIR = ROOT / "data/grocery/draft"
SUMMARY_PATH = GROCERY_AUDIT_DIR / "data_qa_price_coverage_summary.txt"
MISSING_PATH = GROCERY_AUDIT_DIR / "data_qa_price_missing_items.csv"
COVERAGE_PATH = GROCERY_AUDIT_DIR / "data_qa_price_coverage_items.csv"
RESEARCH_TEMPLATE_PATH = GROCERY_DRAFT_DIR / "data_qa_price_research_template.csv"


def main() -> None:
    GROCERY_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    GROCERY_DRAFT_DIR.mkdir(parents=True, exist_ok=True)

    catalog = load_grocery_product_catalog(REFERENCE_PRODUCT_CATALOG_PATH)
    aliases = load_grocery_product_aliases(DEFAULT_PRODUCT_ALIASES_PATH)
    fallbacks = load_grocery_price_fallbacks(DEFAULT_PRICE_FALLBACKS_PATH)
    rows: list[dict[str, Any]] = []

    for context, grocery in _generated_grocery_contexts():
        rows.extend(_coverage_rows_from_grocery(context, grocery, catalog))

    rows.extend(_coverage_rows_from_grocery("all_active_recipe_ingredients", _all_active_recipe_grocery(), catalog))
    rows.extend(_purchase_rule_rows(catalog))
    rows.extend(_cooked_to_raw_rule_rows(catalog))
    rows.extend(_catalog_reference_rows(catalog))

    missing_rows = [row for row in rows if _truthy(row.get("missing_after_task"))]
    research_rows = [
        _research_template_row(row)
        for row in rows
        if _truthy(row.get("missing_before_task"))
        or row.get("price_estimation_method") in {"category_fallback", "emergency_fallback"}
    ]

    _write_csv(COVERAGE_PATH, rows, _coverage_columns())
    _write_csv(MISSING_PATH, missing_rows, _coverage_columns())
    _write_csv(RESEARCH_TEMPLATE_PATH, research_rows, _research_columns())
    SUMMARY_PATH.write_text(
        "\n".join(_summary_lines(rows, missing_rows, research_rows, catalog, aliases, fallbacks)),
        encoding="utf-8",
    )


def _generated_grocery_contexts() -> list[tuple[str, dict[str, Any]]]:
    options = {
        "include_grocery_list": True,
        "include_purchase_suggestions": True,
        "include_price_estimates": True,
        "feedback_enabled": False,
    }
    contexts: list[tuple[str, dict[str, Any]]] = []
    for days in (1, 3, 5):
        response = generate_individual_plan_from_request(
            {
                "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
                "days": days,
                "generation_options": options,
                "include_grocery_list": True,
                "include_purchase_suggestions": True,
                "include_price_estimates": True,
                "feedback_enabled": False,
            }
        )
        contexts.append((f"individual_generated_{days}_day", response.get("grocery_list") or {}))

    household_response = generate_household_plan_from_request(
        {
            "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
            "days": 3,
            "generation_options": options,
            "include_grocery_list": True,
            "include_purchase_suggestions": True,
            "include_price_estimates": True,
            "feedback_enabled": False,
        }
    )
    contexts.append(("household_generated_3_day", household_response.get("household_grocery_list") or {}))
    replacement_grocery = _individual_replacement_grocery_context(options)
    if replacement_grocery:
        contexts.append(("individual_replacement_applied_1_day", replacement_grocery))
    return contexts


def _individual_replacement_grocery_context(options: dict[str, Any]) -> dict[str, Any]:
    response = generate_individual_plan_from_request(
        {
            "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
            "days": 1,
            "generation_options": options,
            "include_grocery_list": True,
            "include_purchase_suggestions": True,
            "include_price_estimates": True,
            "feedback_enabled": False,
        }
    )
    meal = _first_selected_meal(response)
    if not meal:
        return {}
    alternatives = get_recipe_alternatives_from_request(
        {
            "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
            "recipe_id": meal.get("recipe_id"),
            "slot": meal.get("slot"),
            "top_k": 5,
            "candidate_pool_k": 25,
            "approval_mode": "include_review",
            "feedback_enabled": False,
            "generation_options": options,
        }
    )
    alternative = _first_approved_alternative(alternatives)
    if not alternative:
        return {}
    replacement = apply_meal_replacement_from_request(
        {
            "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
            "source_plan": response,
            "day_index": meal.get("day_index") or 1,
            "slot": meal.get("slot"),
            "current_recipe_id": meal.get("recipe_id"),
            "alternative_recipe_id": alternative.get("recipe_id"),
            "generation_type": "individual",
            "feedback_enabled": False,
            "generation_options": options,
            "include_grocery_list": True,
            "include_purchase_suggestions": True,
            "include_price_estimates": True,
        }
    )
    if replacement.get("status") != "ok":
        return {}
    return replacement.get("grocery_list") or {}


def _all_active_recipe_grocery() -> dict[str, Any]:
    pool = load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )
    recipe_rows = (
        pool.eligible_candidates[["recipe_id", "display_name"]]
        .drop_duplicates("recipe_id")
        .sort_values("recipe_id")
        .to_dict("records")
    )
    meals = [
        {
            "slot": "data_qa_pool",
            "recipe_id": row["recipe_id"],
            "display_name": row.get("display_name") or row["recipe_id"],
            "portion_multiplier": 1.0,
        }
        for row in recipe_rows
    ]
    plan = {"days": [{"day_index": 1, "selected_meals": meals}]}
    return build_grocery_list(
        plan,
        pool.ingredients,
        fooddb_df=load_fooddb_current(),
        config=_grocery_config(),
    )


def _purchase_rule_rows(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rules = load_grocery_purchase_rules(DEFAULT_PURCHASE_RULES_PATH)
    items = []
    for rule in rules:
        match_value = str(rule.get("match_value") or "").split("|")[0].strip()
        if not match_value:
            continue
        items.append(_synthetic_item(match_value, rule.get("grocery_category") or "other_review", "purchase_rule_output"))
    purchase_result = apply_purchase_rules(items, rules, config=_grocery_config())
    pricing_result = apply_price_estimates(purchase_result["items"], catalog, config=_grocery_config())
    return [
        _coverage_row("purchase_rules_output_names", item, catalog)
        for item in pricing_result["items"]
    ]


def _cooked_to_raw_rule_rows(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items = [
        _synthetic_item("cooked rice", "carbs_grains", "cooked_to_raw_rule_output"),
        _synthetic_item("cooked pasta", "carbs_grains", "cooked_to_raw_rule_output"),
        _synthetic_item("cooked lentils", "legumes_beans", "cooked_to_raw_rule_output"),
        _synthetic_item("cooked beans", "legumes_beans", "cooked_to_raw_rule_output"),
    ]
    rules = load_grocery_purchase_rules(DEFAULT_PURCHASE_RULES_PATH)
    purchase_result = apply_purchase_rules(items, rules, config=_grocery_config())
    pricing_result = apply_price_estimates(purchase_result["items"], catalog, config=_grocery_config())
    return [
        _coverage_row("cooked_to_raw_rule_output_names", item, catalog)
        for item in pricing_result["items"]
    ]


def _catalog_reference_rows(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in catalog:
        decision_status = _clean_text(row.get("decision_status"))
        price_method = _clean_text(row.get("price_method"))
        numeric_price = any(
            _to_float(row.get(column)) is not None
            for column in ("reference_price", "price_per_kg", "price_per_liter")
        )
        rows.append(
            {
                "current_output_context": "product_catalog_reference",
                "item_name": row.get("display_name_en") or row.get("canonical_ingredient_name"),
                "normalized_item_name": _normalise(row.get("display_name_en") or row.get("canonical_ingredient_name")),
                "grocery_category": row.get("shopping_category"),
                "purchase_unit": row.get("package_type") or row.get("sold_by"),
                "purchase_suggestion": row.get("purchase_format_suggestion"),
                "quantity_basis": row.get("sold_by"),
                "estimated_price_ron": row.get("reference_price") or row.get("price_per_kg") or row.get("price_per_liter"),
                "currency": row.get("currency"),
                "price_estimation_method": "catalog_reference",
                "price_confidence": row.get("confidence"),
                "price_warning": "" if numeric_price or price_method == "pantry_check" else "catalog_price_missing",
                "price_catalog_item_id": row.get("catalog_item_id"),
                "price_fallback_id": "",
                "source_store": row.get("store_name"),
                "source_url": row.get("source_url"),
                "source_checked_at": row.get("captured_at"),
                "source_recipe_ids": "",
                "missing_before_task": not numeric_price and price_method != "pantry_check",
                "missing_after_task": decision_status == "safe_to_use_demo" and not numeric_price and price_method != "pantry_check",
                "not_for_purchase": False,
                "notes": "catalog row audit",
            }
        )
    return rows


def _coverage_rows_from_grocery(
    context: str,
    grocery: dict[str, Any],
    catalog: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [_coverage_row(context, item, catalog) for item in _grocery_items(grocery)]


def _coverage_row(context: str, item: dict[str, Any], catalog: list[dict[str, Any]]) -> dict[str, Any]:
    exact_row = match_catalog_item(item, catalog)
    before = estimate_grocery_item_cost(item, exact_row) if exact_row else {"estimated_cost": None}
    before_missing = before.get("estimated_cost") is None and not bool(item.get("not_for_purchase"))
    after_missing = item.get("estimated_cost") is None and not bool(item.get("not_for_purchase"))
    return {
        "current_output_context": context,
        "item_name": item.get("display_name_clean") or item.get("display_name") or item.get("canonical_name"),
        "normalized_item_name": _normalise(item.get("display_name_clean") or item.get("display_name") or item.get("canonical_name")),
        "grocery_category": item.get("grocery_category"),
        "purchase_unit": item.get("purchase_unit_type"),
        "purchase_suggestion": item.get("purchase_display"),
        "quantity_basis": item.get("purchase_basis_grams") or item.get("total_grams"),
        "estimated_price_ron": item.get("estimated_cost"),
        "currency": item.get("currency"),
        "price_estimation_method": item.get("price_estimation_method"),
        "price_confidence": item.get("price_confidence"),
        "price_warning": item.get("price_warning"),
        "price_catalog_item_id": item.get("price_catalog_item_id"),
        "price_fallback_id": item.get("price_fallback_id"),
        "source_store": item.get("price_store_name"),
        "source_url": item.get("price_source_url"),
        "source_checked_at": item.get("price_source_checked_at"),
        "source_recipe_ids": _source_recipe_ids(item),
        "missing_before_task": before_missing,
        "missing_after_task": after_missing,
        "not_for_purchase": bool(item.get("not_for_purchase")),
        "notes": item.get("warnings") or "",
    }


def _research_template_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "item_name": row.get("item_name"),
        "normalized_item_name": row.get("normalized_item_name"),
        "grocery_category": row.get("grocery_category"),
        "purchase_unit": row.get("purchase_unit"),
        "purchase_suggestion": row.get("purchase_suggestion"),
        "quantity_basis": row.get("quantity_basis"),
        "current_output_context": row.get("current_output_context"),
        "suggested_catalog_key": row.get("normalized_item_name"),
        "price_needed": True,
        "source_store": row.get("source_store"),
        "source_url": row.get("source_url"),
        "observed_price_ron": "",
        "observed_package_size": "",
        "normalized_price_ron_per_unit": row.get("estimated_price_ron"),
        "confidence": row.get("price_confidence"),
        "notes": row.get("price_estimation_method") or row.get("notes"),
    }


def _summary_lines(
    rows: list[dict[str, Any]],
    missing_rows: list[dict[str, Any]],
    research_rows: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
    aliases: list[dict[str, Any]],
    fallbacks: list[dict[str, Any]],
) -> list[str]:
    app_rows = [row for row in rows if row.get("current_output_context") != "product_catalog_reference"]
    methods = _count_by(app_rows, "price_estimation_method")
    return [
        "DATA-QA-1 price coverage summary",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"recipes_path={V1_2_DEMO_FINAL_RECIPES_PATH}",
        f"ingredients_path={V1_2_DEMO_FINAL_INGREDIENTS_PATH}",
        f"nutrition_path={V1_2_DEMO_FINAL_NUTRITION_PATH}",
        f"product_catalog_path={REFERENCE_PRODUCT_CATALOG_PATH.relative_to(ROOT)}",
        f"product_aliases_path={DEFAULT_PRODUCT_ALIASES_PATH.relative_to(ROOT)}",
        f"price_fallbacks_path={DEFAULT_PRICE_FALLBACKS_PATH.relative_to(ROOT)}",
        f"purchase_rules_path={DEFAULT_PURCHASE_RULES_PATH.relative_to(ROOT)}",
        f"cooked_to_raw_rules_path={DEFAULT_COOKED_TO_RAW_RULES_PATH.relative_to(ROOT)}",
        f"catalog_rows_loaded={len(catalog)}",
        f"alias_rows_loaded={len(aliases)}",
        f"fallback_rows_loaded={len(fallbacks)}",
        f"total_grocery_items_checked={len(app_rows)}",
        f"items_with_exact_catalog_price={methods.get('exact_catalog', 0)}",
        f"items_with_alias_catalog_price={methods.get('alias_catalog', 0)}",
        f"items_with_category_fallback_price={methods.get('category_fallback', 0)}",
        f"items_with_emergency_fallback_price={methods.get('emergency_fallback', 0)}",
        f"items_with_missing_price_before_this_task={sum(1 for row in app_rows if _truthy(row.get('missing_before_task')))}",
        f"items_still_missing_after_this_task={len(missing_rows)}",
        f"research_template_rows={len(research_rows)}",
        f"coverage_items_path={COVERAGE_PATH.relative_to(ROOT)}",
        f"missing_items_path={MISSING_PATH.relative_to(ROOT)}",
        f"research_template_path={RESEARCH_TEMPLATE_PATH.relative_to(ROOT)}",
        "target=0 app-facing missing price estimates",
        "live_price_fetching=not_used",
    ]


def _grocery_config() -> dict[str, Any]:
    return {
        "include_pantry_basics": False,
        "include_purchase_suggestions": True,
        "enable_cooked_to_raw_conversion": True,
        "cooked_to_raw_rules_path": DEFAULT_COOKED_TO_RAW_RULES_PATH,
        "include_price_estimates": True,
        "product_catalog_path": REFERENCE_PRODUCT_CATALOG_PATH,
        "product_aliases_path": DEFAULT_PRODUCT_ALIASES_PATH,
        "price_fallbacks_path": DEFAULT_PRICE_FALLBACKS_PATH,
        "exclude_water": True,
    }


def _synthetic_item(name: str, category: str, source: str) -> dict[str, Any]:
    return {
        "grocery_item_id": source,
        "display_name_clean": name,
        "display_name": name,
        "canonical_name": name,
        "total_grams": 300.0,
        "grocery_category": category,
        "category_label": category,
        "ingredient_names_seen": [name],
        "source_item_names": [name],
        "warnings": [],
        "is_pantry_basic": False,
        "is_low_priority": False,
        "not_for_purchase": False,
    }


def _grocery_items(grocery: dict[str, Any]) -> list[dict[str, Any]]:
    items = grocery.get("display_items") or grocery.get("items") or []
    return [item for item in items if isinstance(item, dict)]


def _source_recipe_ids(item: dict[str, Any]) -> str:
    values = []
    for source in item.get("source_recipes", []) or []:
        if isinstance(source, dict) and source.get("recipe_id"):
            values.append(str(source["recipe_id"]))
    return ";".join(sorted(set(values)))


def _first_selected_meal(response: dict[str, Any]) -> dict[str, Any]:
    for day in response.get("daily_plan", []) or []:
        if not isinstance(day, dict):
            continue
        for meal in day.get("selected_meals", []) or []:
            if isinstance(meal, dict) and meal.get("recipe_id") and meal.get("slot"):
                result = dict(meal)
                result.setdefault("day_index", day.get("day_index") or 1)
                return result
    return {}


def _first_approved_alternative(response: dict[str, Any]) -> dict[str, Any]:
    for item in response.get("alternatives", []) or []:
        if isinstance(item, dict) and item.get("approval_status") == "approved":
            return item
    return {}


def _coverage_columns() -> list[str]:
    return [
        "current_output_context",
        "item_name",
        "normalized_item_name",
        "grocery_category",
        "purchase_unit",
        "purchase_suggestion",
        "quantity_basis",
        "estimated_price_ron",
        "currency",
        "price_estimation_method",
        "price_confidence",
        "price_warning",
        "price_catalog_item_id",
        "price_fallback_id",
        "source_store",
        "source_url",
        "source_checked_at",
        "source_recipe_ids",
        "missing_before_task",
        "missing_after_task",
        "not_for_purchase",
        "notes",
    ]


def _research_columns() -> list[str]:
    return [
        "item_name",
        "normalized_item_name",
        "grocery_category",
        "purchase_unit",
        "purchase_suggestion",
        "quantity_basis",
        "current_output_context",
        "suggested_catalog_key",
        "price_needed",
        "source_store",
        "source_url",
        "observed_price_ron",
        "observed_package_size",
        "normalized_price_ron_per_unit",
        "confidence",
        "notes",
    ]


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})


def _csv_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return value


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = _clean_text(row.get(key)) or "missing"
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _normalise(value: Any) -> str:
    return " ".join(_clean_text(value).lower().replace("_", " ").replace("-", " ").split())


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _to_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _clean_text(value).lower() in {"1", "true", "yes", "y"}


if __name__ == "__main__":
    main()
