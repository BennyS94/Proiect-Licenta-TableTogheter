from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.generator_v1.grocery_purchase import (
    apply_purchase_rules,
    load_grocery_purchase_rules,
)


DEFAULT_CONFIG = {
    "exclude_water": True,
    "include_pantry_basics": False,
    "include_purchase_suggestions": False,
    "purchase_rules_path": None,
    "enable_cooked_to_raw_conversion": False,
    "cooked_to_raw_rules_path": None,
    "round_grams_for_display": True,
}

CATEGORY_ORDER = {
    "meat_fish": 10,
    "dairy_eggs": 20,
    "carbs_grains": 30,
    "vegetables": 40,
    "fruits": 50,
    "legumes_beans": 60,
    "oils_fats": 70,
    "sauces_canned": 80,
    "sweeteners": 90,
    "seasonings_spices": 100,
    "pantry_basics": 110,
    "other_review": 120,
}

CATEGORY_LABELS = {
    "meat_fish": "Meat & fish",
    "dairy_eggs": "Dairy & eggs",
    "carbs_grains": "Carbs & grains",
    "vegetables": "Vegetables",
    "fruits": "Fruits",
    "legumes_beans": "Legumes & beans",
    "oils_fats": "Oils & fats",
    "sauces_canned": "Sauces & canned",
    "sweeteners": "Sweeteners",
    "seasonings_spices": "Seasonings & spices",
    "pantry_basics": "Pantry basics / check at home",
    "other_review": "Other / review",
}

SAFE_ALIAS_CATEGORIES = {
    "Olive oil": "oils_fats",
    "Parmesan cheese": "dairy_eggs",
    "Black pepper": "pantry_basics",
    "Salt": "pantry_basics",
}


def build_grocery_list(
    plan: dict[str, Any],
    recipe_ingredients_df: pd.DataFrame,
    fooddb_df: pd.DataFrame | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config_data = dict(DEFAULT_CONFIG)
    if config:
        config_data.update(config)

    warnings: list[str] = []
    selected_meals = _selected_meal_records(plan)
    if not selected_meals:
        return {
            "items": [],
            "display_items": [],
            "summary": {
                "selected_meal_count": 0,
                "selected_recipe_count": 0,
                "raw_item_count": 0,
                "display_item_count": 0,
                "item_count": 0,
                "shopping_item_count": 0,
                "mapped_item_count": 0,
                "fallback_item_count": 0,
                "pantry_basic_count": 0,
                "excluded_water_count": 0,
                "missing_quantity_count": 0,
                "missing_or_zero_quantity_count": 0,
                "category_counts": {},
                "warning_counts": {},
                "is_demo_usable": False,
                "config": config_data,
            },
            "warnings": ["no_selected_meals"],
        }

    required_columns = {"recipe_id", "quantity_grams_estimated"}
    missing_columns = sorted(required_columns.difference(recipe_ingredients_df.columns))
    if missing_columns:
        raise ValueError(
            "recipe_ingredients_df lipseste coloane obligatorii: "
            + ", ".join(missing_columns)
        )

    ingredients = recipe_ingredients_df.copy()
    ingredients["recipe_id"] = ingredients["recipe_id"].map(_clean_text)
    food_index = _fooddb_index(fooddb_df)
    groups: dict[str, dict[str, Any]] = {}
    excluded_water_count = 0
    missing_quantity_count = 0
    ingredient_row_count = 0
    selected_recipe_ids = {
        meal["recipe_id"]
        for meal in selected_meals
        if meal.get("recipe_id")
    }

    for meal in selected_meals:
        recipe_id = meal["recipe_id"]
        if not recipe_id:
            continue
        recipe_rows = ingredients[ingredients["recipe_id"] == recipe_id]
        if recipe_rows.empty:
            warnings.append(f"missing_ingredients_for_recipe:{recipe_id}")
            continue

        for _, row in recipe_rows.iterrows():
            ingredient_row_count += 1
            grams = _to_float(row.get("quantity_grams_estimated"))
            if grams <= 0:
                missing_quantity_count += 1
                continue
            scaled_grams = grams * meal["portion_multiplier"]
            mapped_food_id = _clean_text(row.get("mapped_food_id"))
            food_record = food_index.get(mapped_food_id, {}) if mapped_food_id else {}
            canonical_name = _canonical_name(row, food_record)
            display_name = _display_name(row, food_record, canonical_name)
            ingredient_name = _ingredient_name(row)

            if _is_water(mapped_food_id, canonical_name, display_name, ingredient_name):
                if bool(config_data.get("exclude_water", True)):
                    excluded_water_count += 1
                    continue

            grouping_method = "mapped_food_id" if mapped_food_id else "normalized_name_fallback"
            group_key = (
                f"mapped_food_id:{mapped_food_id}"
                if mapped_food_id
                else f"normalized_name_fallback:{_normalised_fallback_key(row)}"
            )
            category = _infer_grocery_category(
                row=row,
                food_record=food_record,
                canonical_name=canonical_name,
                display_name=display_name,
                ingredient_name=ingredient_name,
            )
            item_warnings = _item_warnings(
                grouping_method=grouping_method,
                category=category,
                mapped_food_id=mapped_food_id,
                canonical_name=canonical_name,
                display_name=display_name,
                ingredient_name=ingredient_name,
                water_included=not bool(config_data.get("exclude_water", True))
                and _is_water(mapped_food_id, canonical_name, display_name, ingredient_name),
            )
            group = groups.setdefault(
                group_key,
                {
                    "mapped_food_id": mapped_food_id,
                    "canonical_name": canonical_name,
                    "display_name": display_name,
                    "total_grams": 0.0,
                    "recipe_totals": {},
                    "meal_totals": {},
                    "ingredient_names_seen": set(),
                    "grouping_method": grouping_method,
                    "grocery_category": category,
                    "warnings": set(),
                    "is_pantry_basic": False,
                    "is_low_priority": False,
                    "not_for_purchase": False,
                },
            )
            group["total_grams"] += scaled_grams
            group["ingredient_names_seen"].add(ingredient_name)
            group["warnings"].update(item_warnings)
            group["is_pantry_basic"] = (
                group["is_pantry_basic"] or "pantry_basic_detected" in item_warnings
            )
            group["is_low_priority"] = (
                group["is_low_priority"] or "low_priority_seasoning" in item_warnings
            )
            group["not_for_purchase"] = group["not_for_purchase"] or "not_for_purchase" in item_warnings
            _add_recipe_total(group, meal, scaled_grams)
            _add_meal_total(group, meal, scaled_grams)

    raw_items = _materialise_items(groups)
    display_items = _materialise_display_items(raw_items, config_data)
    purchase_result: dict[str, Any] | None = None
    if bool(config_data.get("include_purchase_suggestions", False)):
        purchase_rules = load_grocery_purchase_rules(config_data.get("purchase_rules_path"))
        purchase_result = apply_purchase_rules(
            display_items,
            purchase_rules,
            config=config_data,
        )
        display_items = purchase_result["items"]
    summary = _summary(
        raw_items=raw_items,
        display_items=display_items,
        selected_meals=selected_meals,
        selected_recipe_ids=selected_recipe_ids,
        ingredient_row_count=ingredient_row_count,
        excluded_water_count=excluded_water_count,
        missing_quantity_count=missing_quantity_count,
        config_data=config_data,
    )
    if purchase_result is not None:
        purchase_summary = purchase_result.get("summary", {})
        if isinstance(purchase_summary, dict):
            summary["purchase_summary"] = purchase_summary
            summary["purchase_item_count"] = purchase_summary.get("purchase_item_count", 0)
            summary["items_with_purchase_suggestions"] = purchase_summary.get(
                "items_with_purchase_suggestions",
                0,
            )
            summary["purchase_confidence_counts"] = purchase_summary.get(
                "purchase_confidence_counts",
                {},
            )
            summary["purchase_fallback_grams_only_count"] = purchase_summary.get(
                "fallback_grams_only_count",
                0,
            )
            summary["purchase_package_rounded_items_count"] = purchase_summary.get(
                "package_rounded_items_count",
                0,
            )
            summary["purchase_piece_rounded_items_count"] = purchase_summary.get(
                "piece_rounded_items_count",
                0,
            )
            summary["cooked_to_raw_converted_item_count"] = purchase_summary.get(
                "cooked_to_raw_converted_item_count",
                0,
            )
            summary["cooked_to_raw_warning_count"] = purchase_summary.get(
                "cooked_to_raw_warning_count",
                0,
            )
            summary["cooked_raw_no_conversion_count"] = purchase_summary.get(
                "cooked_raw_no_conversion_count",
                0,
            )
        warnings.extend(purchase_result.get("warnings", []))
    warnings.extend(_summary_warnings(summary))
    return {
        "items": raw_items,
        "display_items": display_items,
        "summary": summary,
        "warnings": sorted(dict.fromkeys(warnings)),
    }


def normalize_grocery_display_name(item: dict[str, Any]) -> str:
    raw_name = _clean_text(item.get("display_name"))
    canonical = _clean_text(item.get("canonical_name"))
    names_seen = " ".join(str(value) for value in item.get("ingredient_names_seen", []))
    text = _normalise_name(" ".join([raw_name, canonical, names_seen]))

    if "pressed" == _normalise_name(raw_name) or "pressed" == _normalise_name(canonical):
        return "Pressed"
    if "olive oil" in text or "virgin olive oil" in text:
        return "Olive oil"
    if "parmesan" in text:
        return "Parmesan cheese"
    if "black pepper" in text:
        return "Black pepper"
    if _contains_any(text, ["salt"]):
        return "Salt"
    if "low fat milk" in text or _normalise_name(raw_name).startswith("2 low fat milk"):
        return "Low-fat milk"
    if "white sugar" in text:
        return "White sugar"
    if "sugar brown" in text or "brown sugar" in text:
        return "Brown sugar"
    if "sun" in text and "tomato" in text and "oil" in text:
        return "Sun-dried tomatoes in oil"
    if "garlic clove" in text or _normalise_name(raw_name) == "cloves garlic clove":
        return "Garlic"
    if "chicken breast without skin raw" in text:
        return "Chicken breast"
    if "beef minced steak" in text:
        return "Minced beef"
    if "beef round steak raw" in text:
        return "Beef steak"
    if "cooked ham" in text:
        return "Cooked ham"
    if "salmon smoked" in text:
        return "Smoked salmon"
    if "egg hard boiled" in text:
        return "Hard-boiled eggs"
    if "egg raw" in text:
        return "Eggs"
    if "cheddar cheese" in text:
        return "Cheddar cheese"
    if "mozzarella cheese" in text:
        return "Mozzarella cheese"
    if "milk semi skimmed" in text:
        return "Semi-skimmed milk"
    if "soy milk unsweetened plain" in text:
        return "Unsweetened soy milk"
    if "yogurt greek style" in text or "greek style plain yogurt" in text:
        return "Greek yogurt"
    if "bread wholemeal" in text or "wholemeal bread" in text:
        return "Wholemeal bread"
    if "dried pasta raw" in text:
        return "Pasta (dry)"
    if "oat raw" in text or "rolled oats" in text:
        return "Oats"
    if "rice brown raw" in text:
        return "Brown rice (raw)"
    if "rice cooked unsalted" in text:
        return "Rice (cooked)"
    if _normalise_name(raw_name) == "rice raw" or "rice raw" in text:
        return "Rice (raw)"
    if "broccoli" in text:
        return "Broccoli"
    if "lettuce" in text:
        return "Lettuce"
    if "onion raw" in text or _normalise_name(raw_name) == "onion raw":
        return "Onions"
    if "sweet pepper" in text:
        return "Sweet peppers"
    if "tomato raw" in text:
        return "Tomatoes"
    if "sweet corn canned" in text:
        return "Sweet corn (canned)"
    if "banana pulp raw" in text:
        return "Bananas"
    if "butter 82" in text:
        return "Butter"
    if "combined oil blended vegetable oils" in text:
        return "Vegetable oil"
    if "garlic fresh" in text:
        return "Garlic"
    if "dry white beans" in text or "white beans dry" in text:
        return "White beans (dry)"
    if "great northern beans dry" in text:
        return "Great Northern beans (dry)"
    if "green beans cooked" in text:
        return "Green beans (cooked)"
    if "ham hocks" in text:
        return "Ham hocks"
    if "italian sausage" in text:
        return "Italian sausage"

    return _humanise_display_name(raw_name or canonical or "Unknown item")


def normalize_grocery_category(item: dict[str, Any]) -> str:
    display_name = _clean_text(item.get("display_name_clean")) or normalize_grocery_display_name(item)
    text = _normalise_name(
        " ".join(
            [
                display_name,
                _clean_text(item.get("display_name")),
                _clean_text(item.get("canonical_name")),
                " ".join(str(value) for value in item.get("ingredient_names_seen", [])),
            ]
        )
    )
    grams = _to_float(item.get("total_grams"))

    if _is_unclear_grocery_item_name(item):
        return "other_review"
    if _display_name_is_pantry_basic(display_name):
        return "pantry_basics"
    if _is_tiny_spice_or_herb(text, grams):
        return "pantry_basics"
    if "sun dried tomatoes in oil" in text:
        return "sauces_canned"
    if _contains_any(text, ["honey", "sugar", "syrup", "molasses"]):
        return "sweeteners"
    if _contains_any(text, ["oil", "butter", "margarine"]):
        return "oils_fats"
    if _contains_any(text, ["egg", "milk", "yogurt", "yoghurt", "cheese", "sour cream"]):
        return "dairy_eggs"
    if _contains_any(
        text,
        [
            "chicken",
            "beef",
            "pork",
            "fish",
            "turkey",
            "ham",
            "sausage",
            "salmon",
            "tuna",
            "shrimp",
            "seafood",
        ],
    ):
        return "meat_fish"
    if _contains_any(text, ["rice", "pasta", "potato", "oat", "oats", "bread", "flour", "noodle", "quinoa"]):
        return "carbs_grains"
    if _contains_any(text, ["green beans", "broccoli", "onion", "pepper", "tomato", "lettuce", "spinach", "zucchini", "cabbage", "carrot", "garlic", "corn"]):
        if "garlic" in text and grams <= 50:
            return "seasonings_spices"
        return "vegetables"
    if _contains_any(text, ["beans", "lentils", "lentil", "chickpeas", "chickpea"]):
        return "legumes_beans"
    if _contains_any(text, ["banana", "apple", "berries", "berry", "orange", "lemon", "lime", "strawberry", "blueberry"]):
        return "fruits"
    if _contains_any(text, ["salt", "pepper", "herb", "spice", "basil", "oregano", "cumin", "paprika", "thyme", "parsley"]):
        return "seasonings_spices"
    if _contains_any(text, ["sauce", "salsa", "ketchup", "mustard", "vinegar", "canned", "broth", "stock"]):
        return "sauces_canned"

    category = _clean_text(item.get("grocery_category"))
    if category in CATEGORY_ORDER:
        return category
    return "other_review"


def get_display_group_alias(item: dict[str, Any]) -> str | None:
    clean_name = _clean_text(item.get("display_name_clean")) or normalize_grocery_display_name(item)
    if clean_name in SAFE_ALIAS_CATEGORIES:
        return clean_name
    return None


def round_grams_for_display(total_grams: Any) -> str:
    grams = _to_float(total_grams)
    if grams <= 0:
        return "0g"
    if grams < 1:
        return "<1g"
    if grams < 10:
        rounded = round(grams * 2) / 2
        if abs(rounded - round(rounded)) < 0.05:
            return f"{int(round(rounded))}g"
        return f"{rounded:.1f}g"
    if grams < 100:
        return f"{int(round(grams))}g"
    rounded = int(round(grams / 5.0) * 5)
    return f"{rounded}g"


def grocery_list_rows(
    grocery_list: dict[str, Any],
    include_pantry_basics: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _display_items(grocery_list):
        if not _include_in_shopping_rows(item, include_pantry_basics):
            continue
        rows.append(_display_item_row(item))
    return rows


def grocery_raw_item_rows(grocery_list: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in grocery_list.get("items", []):
        rows.append(
            {
                "grocery_item_id": item.get("grocery_item_id"),
                "display_name": item.get("display_name"),
                "canonical_name": item.get("canonical_name"),
                "mapped_food_id": item.get("mapped_food_id"),
                "total_grams": item.get("total_grams"),
                "total_kg": item.get("total_kg"),
                "grocery_category": item.get("grocery_category"),
                "grouping_method": item.get("grouping_method"),
                "recipe_count": item.get("recipe_count"),
                "meal_count": item.get("meal_count"),
                "is_pantry_basic": item.get("is_pantry_basic", False),
                "is_low_priority": item.get("is_low_priority", False),
                "not_for_purchase": item.get("not_for_purchase", False),
                "ingredient_names_seen": _join_values(item.get("ingredient_names_seen", [])),
                "source_recipes": _format_source_recipes(item.get("source_recipes", [])),
                "source_meals": _format_source_meals(item.get("source_meals", [])),
                "warnings": _join_values(item.get("warnings", [])),
            }
        )
    return rows


def grocery_list_recipe_breakdown_rows(
    grocery_list: dict[str, Any],
    include_pantry_basics: bool = True,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _display_items(grocery_list):
        if not _include_in_shopping_rows(item, include_pantry_basics):
            continue
        for meal in item.get("source_meals", []):
            rows.append(
                {
                    "grocery_item_id": item.get("grocery_item_id"),
                    "display_name_clean": item.get("display_name_clean"),
                    "grocery_category": item.get("grocery_category"),
                    "category_label": item.get("category_label"),
                    "grouped_display_method": item.get("grouped_display_method"),
                    "day_index": meal.get("day_index"),
                    "slot": meal.get("slot"),
                    "recipe_id": meal.get("recipe_id"),
                    "recipe_name": meal.get("display_name"),
                    "portion_multiplier": meal.get("portion_multiplier"),
                    "grams": meal.get("total_grams"),
                    "display_grams": round_grams_for_display(meal.get("total_grams")),
                    "warnings": _join_values(item.get("warnings", [])),
                }
            )
    return rows


def grocery_list_readable_lines(
    grocery_list: dict[str, Any],
    include_pantry_basics: bool = False,
    include_purchase_suggestions: bool | None = None,
) -> list[str]:
    lines = ["Grocery List"]
    if include_purchase_suggestions is None:
        include_purchase_suggestions = _has_purchase_suggestions(grocery_list)
    display_items = [
        item
        for item in _display_items(grocery_list)
        if _include_in_shopping_rows(item, include_pantry_basics)
    ]
    main_items = [
        item
        for item in display_items
        if not item.get("is_pantry_basic", False)
        and item.get("grocery_category") != "pantry_basics"
    ]
    pantry_items = [
        item
        for item in _display_items(grocery_list)
        if item.get("is_pantry_basic", False)
        or item.get("grocery_category") == "pantry_basics"
    ]

    if not main_items and not pantry_items:
        lines.append("- No grocery items generated.")
    for category in CATEGORY_ORDER:
        category_items = [
            item for item in main_items if item.get("grocery_category") == category
        ]
        if not category_items:
            continue
        lines.extend(["", CATEGORY_LABELS.get(category, category)])
        for item in category_items:
            lines.append(_readable_item_line(item, bool(include_purchase_suggestions)))

    if pantry_items:
        lines.extend(["", CATEGORY_LABELS["pantry_basics"]])
        for item in pantry_items:
            lines.append(_readable_item_line(item, bool(include_purchase_suggestions)))

    warnings = _readable_warnings(grocery_list)
    if warnings:
        lines.extend(["", "Warnings"])
        for warning in warnings:
            lines.append(f"- {warning}")
    return lines


def write_grocery_list_csv(
    grocery_list: dict[str, Any],
    path: Path,
    include_pantry_basics: bool = False,
) -> None:
    _ensure_parent(path)
    rows = grocery_list_rows(
        grocery_list,
        include_pantry_basics=include_pantry_basics,
    )
    pd.DataFrame(rows).to_csv(path, index=False)


def write_grocery_list_readable(
    grocery_list: dict[str, Any],
    path: Path,
    include_pantry_basics: bool = False,
    include_purchase_suggestions: bool | None = None,
) -> None:
    _ensure_parent(path)
    path.write_text(
        "\n".join(
            grocery_list_readable_lines(
                grocery_list,
                include_pantry_basics=include_pantry_basics,
                include_purchase_suggestions=include_purchase_suggestions,
            )
        )
        + "\n",
        encoding="utf-8",
    )


def _selected_meal_records(plan: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    days = plan.get("days")
    if isinstance(days, list) and days:
        for day_index, day in enumerate(days, start=1):
            if not isinstance(day, dict):
                continue
            selected_meals = day.get("selected_meals", [])
            if not isinstance(selected_meals, list):
                continue
            for meal_index, meal in enumerate(selected_meals, start=1):
                if isinstance(meal, dict):
                    records.append(_meal_record(meal, day, day_index, meal_index))
        return records

    selected_meals = plan.get("selected_meals", [])
    if isinstance(selected_meals, list):
        for meal_index, meal in enumerate(selected_meals, start=1):
            if isinstance(meal, dict):
                records.append(_meal_record(meal, {}, 1, meal_index))
    return records


def _meal_record(
    meal: dict[str, Any],
    day: dict[str, Any],
    fallback_day_index: int,
    meal_index: int,
) -> dict[str, Any]:
    recipe_id = _clean_text(meal.get("recipe_id"))
    day_index = int(_to_float(day.get("day_index"), fallback=float(fallback_day_index)))
    portion_multiplier = _to_float(meal.get("portion_multiplier"), fallback=1.0)
    if portion_multiplier <= 0:
        portion_multiplier = 1.0
    slot = _clean_text(meal.get("slot")) or f"meal_{meal_index}"
    display_name = _clean_text(meal.get("display_name")) or recipe_id
    return {
        "meal_instance_id": f"day_{day_index}_{meal_index}_{slot}_{recipe_id}",
        "day_index": day_index,
        "slot": slot,
        "recipe_id": recipe_id,
        "display_name": display_name,
        "portion_multiplier": portion_multiplier,
    }


def _fooddb_index(fooddb_df: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if fooddb_df is None or fooddb_df.empty or "food_id" not in fooddb_df.columns:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for _, row in fooddb_df.iterrows():
        food_id = _clean_text(row.get("food_id"))
        if food_id:
            result[food_id] = row.to_dict()
    return result


def _canonical_name(row: pd.Series, food_record: dict[str, Any]) -> str:
    return (
        _clean_text(food_record.get("canonical_name"))
        or _clean_text(row.get("mapped_food_canonical_name"))
        or _clean_text(row.get("ingredient_name_normalized"))
        or _clean_text(row.get("ingredient_name_parsed"))
        or _clean_text(row.get("ingredient_raw_text"))
        or "unknown_item"
    )


def _display_name(row: pd.Series, food_record: dict[str, Any], canonical_name: str) -> str:
    return (
        _clean_text(food_record.get("display_name"))
        or _title_from_text(canonical_name)
    )


def _ingredient_name(row: pd.Series) -> str:
    return (
        _clean_text(row.get("ingredient_name_normalized"))
        or _clean_text(row.get("ingredient_name_parsed"))
        or _clean_text(row.get("ingredient_raw_text"))
        or "unknown_ingredient"
    )


def _normalised_fallback_key(row: pd.Series) -> str:
    value = _ingredient_name(row).lower().strip()
    if not value:
        return "unknown_ingredient"
    return " ".join(value.replace("_", " ").split())


def _infer_grocery_category(
    row: pd.Series,
    food_record: dict[str, Any],
    canonical_name: str,
    display_name: str,
    ingredient_name: str,
) -> str:
    text = " ".join(
        _clean_text(value).lower().replace("_", " ")
        for value in [
            canonical_name,
            display_name,
            ingredient_name,
            row.get("ingredient_raw_text"),
            food_record.get("food_group"),
            food_record.get("food_subgroup"),
            food_record.get("food_subgroup_detail"),
            food_record.get("food_family_name"),
            food_record.get("helper_macro_profile"),
            food_record.get("helper_protein_bucket"),
            food_record.get("helper_carb_bucket"),
            food_record.get("helper_veg_bucket"),
        ]
        if _clean_text(value)
    )
    food_group = _clean_text(food_record.get("food_group")).lower()
    food_subgroup = _clean_text(food_record.get("food_subgroup")).lower()

    if food_record:
        if food_group == "meat, egg and fish" or food_subgroup in {
            "raw meat",
            "cooked meat",
            "fish, cooked",
            "fish products",
            "seafood, raw",
            "seafood, cooked",
        }:
            return "meat_fish"
        if food_subgroup == "eggs":
            return "dairy_eggs"
        if food_subgroup in {"pasta, rice and grains", "breads and similar", "potatoes and other tubers"}:
            return "carbs_grains"
        if food_subgroup in {"milk", "cheese and similar", "cream and similar", "dairy products and similar"}:
            return "dairy_eggs"
        if food_group == "fats and oils" or food_subgroup in {"vegetable oils", "butters", "other fats", "fish oils"}:
            return "oils_fats"
        if food_subgroup == "fruits":
            return "fruits"
        if food_subgroup in {"legumes", "nuts and seeds"}:
            return "legumes_beans"
        if food_subgroup in {"vegetables", "seaweed"}:
            return "vegetables"
        if food_subgroup in {"salts", "spices", "herbs", "condiments"}:
            return "seasonings_spices"
        if food_subgroup == "sauces":
            return "sauces_canned"
        if food_group == "sugar and confectionery":
            return "sweeteners"
        if _truthy(food_record.get("helper_use_as_carb_side")):
            return "carbs_grains"
        if _truthy(food_record.get("helper_use_as_veg_side")):
            return "vegetables"
        if _truthy(food_record.get("helper_use_as_protein")):
            return "meat_fish"

    return _keyword_category(text)


def _keyword_category(text: str) -> str:
    if _contains_any(text, ["honey", "sugar", "syrup", "molasses"]):
        return "sweeteners"
    if _contains_any(text, ["oil", "butter", "margarine"]):
        return "oils_fats"
    if _contains_any(text, ["milk", "yogurt", "yoghurt", "cheese", "sour cream", "egg"]):
        return "dairy_eggs"
    if _contains_any(
        text,
        [
            "chicken",
            "beef",
            "pork",
            "fish",
            "turkey",
            "salmon",
            "tuna",
            "shrimp",
            "ham",
            "sausage",
        ],
    ):
        return "meat_fish"
    if _contains_any(text, ["rice", "pasta", "potato", "oats", "bread", "flour", "noodle", "quinoa"]):
        return "carbs_grains"
    if _contains_any(text, ["green beans", "onion", "carrot", "pepper", "broccoli", "spinach", "tomato", "lettuce", "cucumber", "zucchini", "garlic", "mushroom", "cabbage", "corn"]):
        return "vegetables"
    if _contains_any(text, ["beans", "lentils", "lentil", "chickpeas", "chickpea"]):
        return "legumes_beans"
    if _contains_any(text, ["apple", "banana", "berries", "berry", "orange", "lemon", "lime", "strawberry", "blueberry"]):
        return "fruits"
    if _contains_any(text, ["salt", "pepper", "herb", "spice", "basil", "oregano", "cumin", "paprika", "thyme", "parsley"]):
        return "seasonings_spices"
    if _contains_any(text, ["sauce", "salsa", "ketchup", "mustard", "vinegar", "canned", "broth", "stock"]):
        return "sauces_canned"
    return "other_review"


def _item_warnings(
    grouping_method: str,
    category: str,
    mapped_food_id: str,
    canonical_name: str,
    display_name: str,
    ingredient_name: str,
    water_included: bool,
) -> set[str]:
    warnings: set[str] = set()
    if grouping_method == "normalized_name_fallback":
        warnings.add("normalized_name_fallback")
    if category == "seasonings_spices":
        warnings.add("low_priority_seasoning")
    if _is_pantry_basic(mapped_food_id, canonical_name, display_name, ingredient_name):
        warnings.add("pantry_basic_detected")
    if water_included:
        warnings.add("not_for_purchase")
    return warnings


def _is_water(
    mapped_food_id: str,
    canonical_name: str,
    display_name: str,
    ingredient_name: str,
) -> bool:
    values = {
        _normalise_name(mapped_food_id),
        _normalise_name(canonical_name),
        _normalise_name(display_name),
        _normalise_name(ingredient_name),
    }
    return bool(values.intersection({"water", "apa", "food water"}))


def _is_pantry_basic(
    mapped_food_id: str,
    canonical_name: str,
    display_name: str,
    ingredient_name: str,
) -> bool:
    text = " ".join(
        _normalise_name(value)
        for value in [mapped_food_id, canonical_name, display_name, ingredient_name]
    )
    if _contains_any(text, ["salt"]):
        return True
    if _contains_any(
        text,
        [
            "black pepper",
            "white pepper",
            "ground pepper",
            "peppercorn",
        ],
    ):
        return True
    return text.strip() in {"pepper", "food pepper"}


def _add_recipe_total(group: dict[str, Any], meal: dict[str, Any], grams: float) -> None:
    recipe_id = meal["recipe_id"]
    totals = group["recipe_totals"]
    recipe = totals.setdefault(
        recipe_id,
        {
            "recipe_id": recipe_id,
            "display_name": meal["display_name"],
            "total_grams": 0.0,
        },
    )
    recipe["total_grams"] += grams


def _add_meal_total(group: dict[str, Any], meal: dict[str, Any], grams: float) -> None:
    meal_id = meal["meal_instance_id"]
    totals = group["meal_totals"]
    entry = totals.setdefault(
        meal_id,
        {
            "day_index": meal["day_index"],
            "slot": meal["slot"],
            "recipe_id": meal["recipe_id"],
            "display_name": meal["display_name"],
            "portion_multiplier": round(meal["portion_multiplier"], 4),
            "total_grams": 0.0,
        },
    )
    entry["total_grams"] += grams


def _materialise_items(groups: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    materialised: list[dict[str, Any]] = []
    for group in groups.values():
        total_grams = float(group["total_grams"])
        source_recipes = [
            {
                **value,
                "total_grams": round(float(value["total_grams"]), 1),
            }
            for value in group["recipe_totals"].values()
        ]
        source_meals = [
            {
                **value,
                "total_grams": round(float(value["total_grams"]), 1),
            }
            for value in group["meal_totals"].values()
        ]
        source_recipes.sort(key=lambda item: (str(item.get("display_name")), str(item.get("recipe_id"))))
        source_meals.sort(
            key=lambda item: (
                int(item.get("day_index") or 0),
                str(item.get("slot")),
                str(item.get("recipe_id")),
            )
        )
        materialised.append(
            {
                "grocery_item_id": "",
                "mapped_food_id": group["mapped_food_id"] or None,
                "canonical_name": group["canonical_name"],
                "display_name": group["display_name"],
                "total_grams": round(total_grams, 1),
                "total_kg": round(total_grams / 1000.0, 3),
                "recipe_count": len(source_recipes),
                "meal_count": len(source_meals),
                "source_recipes": source_recipes,
                "source_meals": source_meals,
                "ingredient_names_seen": sorted(group["ingredient_names_seen"]),
                "grouping_method": group["grouping_method"],
                "grocery_category": group["grocery_category"],
                "warnings": sorted(group["warnings"]),
                "is_pantry_basic": bool(group["is_pantry_basic"]),
                "is_low_priority": bool(group["is_low_priority"]),
                "not_for_purchase": bool(group["not_for_purchase"]),
            }
        )

    materialised.sort(key=_raw_item_sort_key)
    for index, item in enumerate(materialised, start=1):
        item["grocery_item_id"] = f"raw_grocery_{index:03d}"
    return materialised


def _materialise_display_items(
    raw_items: list[dict[str, Any]],
    config_data: dict[str, Any],
) -> list[dict[str, Any]]:
    display_groups: dict[str, dict[str, Any]] = {}
    for raw_item in raw_items:
        prepared = _prepare_display_source_item(raw_item, config_data)
        alias = get_display_group_alias(prepared)
        if alias:
            group_key = f"safe_display_alias:{_normalise_name(alias)}"
            grouped_display_method = "safe_display_alias"
            display_name_clean = alias
            category = SAFE_ALIAS_CATEGORIES[alias]
        elif prepared.get("grouping_method") == "mapped_food_id" and prepared.get("mapped_food_id"):
            group_key = f"exact_mapped_id:{prepared['mapped_food_id']}"
            grouped_display_method = "exact_mapped_id"
            display_name_clean = prepared["display_name_clean"]
            category = prepared["grocery_category"]
        else:
            group_key = f"normalized_name_fallback:{_normalise_name(prepared['display_name_clean'])}"
            grouped_display_method = "normalized_name_fallback"
            display_name_clean = prepared["display_name_clean"]
            category = prepared["grocery_category"]

        group = display_groups.setdefault(
            group_key,
            {
                "grocery_item_id": "",
                "display_name_clean": display_name_clean,
                "canonical_name": prepared.get("canonical_name"),
                "mapped_food_id": prepared.get("mapped_food_id"),
                "source_mapped_food_ids": set(),
                "source_item_names": set(),
                "source_raw_item_ids": [],
                "total_grams": 0.0,
                "source_recipes": {},
                "source_meals": {},
                "ingredient_names_seen": set(),
                "grouped_display_method": grouped_display_method,
                "grouping_method": grouped_display_method,
                "grocery_category": category,
                "category_label": CATEGORY_LABELS.get(category, category),
                "warnings": set(),
                "is_pantry_basic": False,
                "is_low_priority": False,
                "not_for_purchase": False,
                "source_item_count": 0,
            },
        )
        group["total_grams"] += _to_float(prepared.get("total_grams"))
        group["source_item_names"].add(_clean_text(prepared.get("display_name")))
        if prepared.get("mapped_food_id"):
            group["source_mapped_food_ids"].add(_clean_text(prepared.get("mapped_food_id")))
        group["source_raw_item_ids"].append(prepared.get("grocery_item_id"))
        group["ingredient_names_seen"].update(prepared.get("ingredient_names_seen", []))
        group["warnings"].update(prepared.get("warnings", []))
        if grouped_display_method == "safe_display_alias":
            group["warnings"].add("display_alias_grouping_used")
        group["is_pantry_basic"] = (
            group["is_pantry_basic"]
            or bool(prepared.get("is_pantry_basic"))
            or category == "pantry_basics"
        )
        group["is_low_priority"] = group["is_low_priority"] or bool(prepared.get("is_low_priority"))
        group["not_for_purchase"] = group["not_for_purchase"] or bool(prepared.get("not_for_purchase"))
        group["source_item_count"] += 1
        _merge_recipe_totals(group["source_recipes"], prepared.get("source_recipes", []))
        _merge_meal_totals(group["source_meals"], prepared.get("source_meals", []))

    display_items: list[dict[str, Any]] = []
    for group in display_groups.values():
        total_grams = round(float(group["total_grams"]), 1)
        source_recipes = list(group["source_recipes"].values())
        source_meals = list(group["source_meals"].values())
        source_recipes.sort(key=lambda item: (str(item.get("display_name")), str(item.get("recipe_id"))))
        source_meals.sort(
            key=lambda item: (
                int(item.get("day_index") or 0),
                str(item.get("slot")),
                str(item.get("recipe_id")),
            )
        )
        display_items.append(
            {
                "grocery_item_id": "",
                "display_name_clean": group["display_name_clean"],
                "canonical_name": group["canonical_name"],
                "mapped_food_id": group["mapped_food_id"],
                "source_mapped_food_ids": sorted(group["source_mapped_food_ids"]),
                "source_item_names": sorted(group["source_item_names"]),
                "source_raw_item_ids": list(group["source_raw_item_ids"]),
                "total_grams": total_grams,
                "total_kg": round(total_grams / 1000.0, 3),
                "display_grams": round_grams_for_display(total_grams)
                if bool(config_data.get("round_grams_for_display", True))
                else _format_grams(total_grams),
                "recipe_count": len(source_recipes),
                "meal_count": len(source_meals),
                "source_recipes": source_recipes,
                "source_meals": source_meals,
                "ingredient_names_seen": sorted(group["ingredient_names_seen"]),
                "grouped_display_method": group["grouped_display_method"],
                "grouping_method": group["grouped_display_method"],
                "grocery_category": group["grocery_category"],
                "category_label": group["category_label"],
                "warnings": sorted(group["warnings"]),
                "is_pantry_basic": bool(group["is_pantry_basic"]),
                "is_low_priority": bool(group["is_low_priority"]),
                "not_for_purchase": bool(group["not_for_purchase"]),
                "source_item_count": int(group["source_item_count"]),
            }
        )

    display_items.sort(key=_display_item_sort_key)
    for index, item in enumerate(display_items, start=1):
        item["grocery_item_id"] = f"grocery_{index:03d}"
    return display_items


def _prepare_display_source_item(
    raw_item: dict[str, Any],
    config_data: dict[str, Any],
) -> dict[str, Any]:
    prepared = dict(raw_item)
    warnings = set(raw_item.get("warnings", []))
    display_name_clean = normalize_grocery_display_name(prepared)
    prepared["display_name_clean"] = display_name_clean
    category = normalize_grocery_category(prepared)
    prepared["grocery_category"] = category
    prepared["category_label"] = CATEGORY_LABELS.get(category, category)
    if _is_unclear_grocery_item_name(prepared):
        warnings.add("unclear_grocery_item_name")
    if _has_cooked_raw_purchase_ambiguity(prepared):
        warnings.add("cooked_raw_purchase_ambiguity")
    if category == "pantry_basics" or _display_name_is_pantry_basic(display_name_clean):
        prepared["is_pantry_basic"] = True
        warnings.add("pantry_basic_detected")
    if category == "seasonings_spices":
        prepared["is_low_priority"] = True
        warnings.add("low_priority_seasoning")
    if not bool(config_data.get("include_pantry_basics", False)) and prepared.get("is_pantry_basic"):
        warnings.add("pantry_basic_detected")
    prepared["warnings"] = sorted(warnings)
    return prepared


def _merge_recipe_totals(
    target: dict[str, dict[str, Any]],
    source_recipes: list[dict[str, Any]],
) -> None:
    for recipe in source_recipes:
        recipe_id = _clean_text(recipe.get("recipe_id"))
        if not recipe_id:
            continue
        entry = target.setdefault(
            recipe_id,
            {
                "recipe_id": recipe_id,
                "display_name": recipe.get("display_name"),
                "total_grams": 0.0,
            },
        )
        entry["total_grams"] = round(
            _to_float(entry.get("total_grams")) + _to_float(recipe.get("total_grams")),
            1,
        )


def _merge_meal_totals(
    target: dict[str, dict[str, Any]],
    source_meals: list[dict[str, Any]],
) -> None:
    for meal in source_meals:
        meal_key = "|".join(
            [
                str(meal.get("day_index")),
                str(meal.get("slot")),
                str(meal.get("recipe_id")),
                str(meal.get("portion_multiplier")),
            ]
        )
        entry = target.setdefault(
            meal_key,
            {
                "day_index": meal.get("day_index"),
                "slot": meal.get("slot"),
                "recipe_id": meal.get("recipe_id"),
                "display_name": meal.get("display_name"),
                "portion_multiplier": meal.get("portion_multiplier"),
                "total_grams": 0.0,
            },
        )
        entry["total_grams"] = round(
            _to_float(entry.get("total_grams")) + _to_float(meal.get("total_grams")),
            1,
        )


def _summary(
    raw_items: list[dict[str, Any]],
    display_items: list[dict[str, Any]],
    selected_meals: list[dict[str, Any]],
    selected_recipe_ids: set[str],
    ingredient_row_count: int,
    excluded_water_count: int,
    missing_quantity_count: int,
    config_data: dict[str, Any],
) -> dict[str, Any]:
    include_pantry_basics = bool(config_data.get("include_pantry_basics", False))
    warning_counts = _warning_counts(display_items)
    category_counts: dict[str, int] = {}
    for item in display_items:
        category = str(item.get("grocery_category", "other_review"))
        category_counts[category] = category_counts.get(category, 0) + 1
    shopping_item_count = sum(
        1 for item in display_items if _include_in_shopping_rows(item, include_pantry_basics)
    )
    safe_alias_groups = [
        item for item in display_items if item.get("grouped_display_method") == "safe_display_alias"
    ]
    return {
        "selected_meal_count": len(selected_meals),
        "selected_recipe_count": len(selected_recipe_ids),
        "ingredient_row_count": ingredient_row_count,
        "included_ingredient_row_count": max(
            0,
            ingredient_row_count - excluded_water_count - missing_quantity_count,
        ),
        "raw_item_count": len(raw_items),
        "display_item_count": len(display_items),
        "item_count": len(display_items),
        "shopping_item_count": shopping_item_count,
        "mapped_item_count": sum(1 for item in display_items if item.get("source_mapped_food_ids")),
        "fallback_item_count": sum(
            1 for item in display_items if item.get("grouped_display_method") == "normalized_name_fallback"
        ),
        "raw_mapped_item_count": sum(1 for item in raw_items if item.get("grouping_method") == "mapped_food_id"),
        "raw_fallback_item_count": sum(
            1 for item in raw_items if item.get("grouping_method") == "normalized_name_fallback"
        ),
        "pantry_basic_count": sum(1 for item in display_items if item.get("is_pantry_basic")),
        "low_priority_count": sum(1 for item in display_items if item.get("is_low_priority")),
        "not_for_purchase_count": sum(1 for item in display_items if item.get("not_for_purchase")),
        "safe_alias_group_count": len(safe_alias_groups),
        "safe_alias_source_item_count": sum(int(item.get("source_item_count") or 0) for item in safe_alias_groups),
        "unclear_item_count": warning_counts.get("unclear_grocery_item_name", 0),
        "cooked_raw_ambiguity_count": warning_counts.get("cooked_raw_purchase_ambiguity", 0),
        "excluded_water_count": excluded_water_count,
        "missing_quantity_count": missing_quantity_count,
        "missing_or_zero_quantity_count": missing_quantity_count,
        "total_grams": round(sum(float(item.get("total_grams") or 0.0) for item in display_items), 1),
        "category_counts": dict(sorted(category_counts.items())),
        "warning_counts": warning_counts,
        "is_demo_usable": shopping_item_count > 0,
        "config": config_data,
    }


def _summary_warnings(summary: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if int(summary.get("fallback_item_count") or 0) > 0:
        warnings.append("fallback_grouped_items_present")
    if int(summary.get("safe_alias_group_count") or 0) > 0:
        warnings.append("display_alias_grouping_used")
    if int(summary.get("pantry_basic_count") or 0) > 0:
        warnings.append("pantry_basic_detected")
    if int(summary.get("excluded_water_count") or 0) > 0:
        warnings.append("water_excluded_by_default")
    if int(summary.get("missing_or_zero_quantity_count") or 0) > 0:
        warnings.append("missing_or_zero_quantity_skipped")
    warning_counts = summary.get("warning_counts", {})
    if isinstance(warning_counts, dict):
        for warning in [
            "normalized_name_fallback",
            "unclear_grocery_item_name",
            "cooked_raw_purchase_ambiguity",
            "low_priority_seasoning",
        ]:
            if int(warning_counts.get(warning, 0) or 0) > 0:
                warnings.append(warning)
    return warnings


def _warning_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        for warning in item.get("warnings", []):
            warning_text = str(warning)
            counts[warning_text] = counts.get(warning_text, 0) + 1
    return dict(sorted(counts.items()))


def _display_item_row(item: dict[str, Any]) -> dict[str, Any]:
    warnings = list(item.get("warnings", []))
    purchase_warnings = list(item.get("purchase_warnings", []))
    return {
        "grocery_item_id": item.get("grocery_item_id"),
        "display_name_clean": item.get("display_name_clean"),
        "display_name": item.get("display_name_clean"),
        "category_label": item.get("category_label"),
        "grocery_category": item.get("grocery_category"),
        "display_grams": item.get("display_grams"),
        "total_grams": item.get("total_grams"),
        "total_kg": item.get("total_kg"),
        "mapped_food_id": item.get("mapped_food_id"),
        "source_mapped_food_ids": _join_values(item.get("source_mapped_food_ids", [])),
        "source_item_names": _join_values(item.get("source_item_names", [])),
        "source_raw_item_ids": _join_values(item.get("source_raw_item_ids", [])),
        "canonical_name": item.get("canonical_name"),
        "grouped_display_method": item.get("grouped_display_method"),
        "grouping_method": item.get("grouping_method"),
        "recipe_count": item.get("recipe_count"),
        "meal_count": item.get("meal_count"),
        "source_item_count": item.get("source_item_count"),
        "is_pantry_basic": item.get("is_pantry_basic", False),
        "is_low_priority": item.get("is_low_priority", False),
        "not_for_purchase": item.get("not_for_purchase", False),
        "ingredient_names_seen": _join_values(item.get("ingredient_names_seen", [])),
        "source_recipes": _format_source_recipes(item.get("source_recipes", [])),
        "source_meals": _format_source_meals(item.get("source_meals", [])),
        "warnings": _join_values(sorted(dict.fromkeys(warnings + purchase_warnings))),
        "needed_grams_exact": item.get("needed_grams_exact"),
        "needed_grams_display": item.get("needed_grams_display"),
        "purchase_basis_grams": item.get("purchase_basis_grams"),
        "purchase_display": item.get("purchase_display"),
        "purchase_unit_type": item.get("purchase_unit_type"),
        "purchase_quantity": item.get("purchase_quantity"),
        "purchase_amount_grams": item.get("purchase_amount_grams"),
        "estimated_leftover_grams": item.get("estimated_leftover_grams"),
        "purchase_rule_id": item.get("purchase_rule_id"),
        "purchase_rule_match_type": item.get("purchase_rule_match_type"),
        "purchase_rule_confidence": item.get("purchase_rule_confidence"),
        "purchase_rounding_strategy": item.get("purchase_rounding_strategy"),
        "purchase_is_pantry_basic": item.get("purchase_is_pantry_basic"),
        "purchase_warnings": _join_values(purchase_warnings),
        "cooked_to_raw_applied": item.get("cooked_to_raw_applied", False),
        "cooked_to_raw_rule_id": item.get("cooked_to_raw_rule_id"),
        "original_needed_grams": item.get("original_needed_grams"),
        "raw_equivalent_grams": item.get("raw_equivalent_grams"),
        "raw_purchase_display_name": item.get("raw_purchase_display_name"),
        "raw_equivalent_factor": item.get("raw_equivalent_factor"),
        "raw_equivalent_basis": item.get("raw_equivalent_basis"),
        "cooked_to_raw_confidence": item.get("cooked_to_raw_confidence"),
        "cooked_to_raw_warning": item.get("cooked_to_raw_warning"),
    }


def _display_items(grocery_list: dict[str, Any]) -> list[dict[str, Any]]:
    display_items = grocery_list.get("display_items", [])
    if isinstance(display_items, list) and display_items:
        return display_items
    return grocery_list.get("items", [])


def _include_in_shopping_rows(item: dict[str, Any], include_pantry_basics: bool) -> bool:
    if item.get("not_for_purchase", False):
        return False
    if item.get("is_pantry_basic", False) and not include_pantry_basics:
        return False
    if item.get("grocery_category") == "pantry_basics" and not include_pantry_basics:
        return False
    return True


def _raw_item_sort_key(item: dict[str, Any]) -> tuple[int, int, int, str]:
    return (
        1 if item.get("is_pantry_basic", False) else 0,
        1 if item.get("not_for_purchase", False) else 0,
        CATEGORY_ORDER.get(str(item.get("grocery_category", "other_review")), 999),
        str(item.get("display_name", "")).lower(),
    )


def _display_item_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    return (
        1 if item.get("is_pantry_basic", False) else 0,
        CATEGORY_ORDER.get(str(item.get("grocery_category", "other_review")), 999),
        str(item.get("display_name_clean", "")).lower(),
    )


def _display_item_suffix(item: dict[str, Any]) -> str:
    suffixes = []
    warnings = set(item.get("warnings", [])) | set(item.get("purchase_warnings", []))
    if "cooked_to_raw_estimate" in warnings:
        suffixes.append("cooked-to-raw estimate")
    elif "cooked_raw_purchase_ambiguity" in warnings:
        suffixes.append("check cooked/raw")
    if "unclear_grocery_item_name" in warnings:
        suffixes.append("review")
    if "normalized_name_fallback" in warnings:
        suffixes.append("fallback")
    if not suffixes:
        return ""
    return " (" + ", ".join(suffixes) + ")"


def _has_purchase_suggestions(grocery_list: dict[str, Any]) -> bool:
    return any(
        bool(item.get("purchase_display"))
        for item in _display_items(grocery_list)
    )


def _readable_item_line(item: dict[str, Any], include_purchase_suggestions: bool) -> str:
    name = item.get("display_name_clean")
    suffix = _display_item_suffix(item)
    if include_purchase_suggestions and item.get("purchase_display"):
        needed = item.get("needed_grams_display") or item.get("display_grams")
        purchase_display = str(item.get("purchase_display") or "").strip()
        if purchase_display.startswith("check pantry") or purchase_display.startswith("review item"):
            return f"- {name}: {purchase_display}{suffix}"
        return f"- {name}: need {needed}; buy {purchase_display}{suffix}"
    return f"- {name}: {item.get('display_grams')}{suffix}"


def _readable_warnings(grocery_list: dict[str, Any]) -> list[str]:
    warnings = list(grocery_list.get("warnings", []))
    for item in _display_items(grocery_list):
        warnings.extend(str(warning) for warning in item.get("warnings", []))
        warnings.extend(str(warning) for warning in item.get("purchase_warnings", []))
    return sorted(dict.fromkeys(str(item) for item in warnings if str(item).strip()))


def _is_unclear_grocery_item_name(item: dict[str, Any]) -> bool:
    display_name = _normalise_name(item.get("display_name_clean") or item.get("display_name"))
    canonical = _normalise_name(item.get("canonical_name"))
    return display_name in {"pressed", "unknown item", "unknown ingredient"} or canonical == "pressed"


def _has_cooked_raw_purchase_ambiguity(item: dict[str, Any]) -> bool:
    text = _normalise_name(
        " ".join(
            [
                _clean_text(item.get("display_name_clean")),
                _clean_text(item.get("display_name")),
                _clean_text(item.get("canonical_name")),
                " ".join(str(value) for value in item.get("ingredient_names_seen", [])),
            ]
        )
    )
    if not _contains_any(text, ["cooked", "boiled", "cooked in water"]):
        return False
    return _contains_any(
        text,
        [
            "rice",
            "pasta",
            "beans",
            "lentils",
            "chickpeas",
            "grain",
            "grains",
            "oat",
            "oats",
            "broccoli",
            "green beans",
            "carrot",
            "potato",
            "tomato",
            "pepper",
            "spinach",
            "zucchini",
            "cabbage",
            "vegetable",
            "vegetables",
        ],
    )


def _display_name_is_pantry_basic(display_name: str) -> bool:
    text = _normalise_name(display_name)
    return text in {"salt", "black pepper", "white pepper", "ground pepper"}


def _is_tiny_spice_or_herb(text: str, grams: float) -> bool:
    if grams > 10:
        return False
    return _contains_any(
        text,
        [
            "basil",
            "oregano",
            "cumin",
            "paprika",
            "thyme",
            "parsley",
            "spice",
            "herb",
        ],
    )


def _humanise_display_name(value: str) -> str:
    text = _clean_text(value).replace("_", " ")
    text = text.replace(",", " ")
    text = " ".join(text.split())
    if not text:
        return "Unknown item"
    lower = text.lower()
    replacements = {
        "freshly ": "",
        "fresh ": "",
    }
    for prefix, replacement in replacements.items():
        if lower.startswith(prefix):
            text = replacement + text[len(prefix):]
            lower = text.lower()
    words = []
    for word in text.split():
        if word.lower() in {"of", "in", "with", "and", "or"}:
            words.append(word.lower())
        else:
            words.append(word[:1].upper() + word[1:].lower())
    result = " ".join(words)
    if result:
        return result[0].upper() + result[1:]
    return "Unknown item"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    return text in {"true", "1", "yes", "y"}


def _contains_any(text: str, keywords: list[str]) -> bool:
    clean = _normalise_name(text)
    padded = f" {clean} "
    return any(f" {_normalise_name(keyword)} " in padded for keyword in keywords)


def _normalise_name(value: Any) -> str:
    return " ".join(_clean_text(value).lower().replace("_", " ").replace("-", " ").split())


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _title_from_text(value: str) -> str:
    text = value.replace("_", " ").strip()
    return " ".join(part.capitalize() for part in text.split()) or "Unknown item"


def _to_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _format_grams(value: Any) -> str:
    grams = _to_float(value)
    if abs(grams - round(grams)) < 0.05:
        return f"{int(round(grams))}g"
    return f"{grams:.1f}g"


def _join_values(values: list[Any]) -> str:
    return "; ".join(str(item) for item in values if str(item).strip())


def _format_source_recipes(values: list[dict[str, Any]]) -> str:
    return "; ".join(
        f"{item.get('recipe_id')}:{item.get('display_name')}:{_format_grams(item.get('total_grams'))}"
        for item in values
    )


def _format_source_meals(values: list[dict[str, Any]]) -> str:
    return "; ".join(
        (
            f"day{item.get('day_index')}:{item.get('slot')}:"
            f"{item.get('recipe_id')}:{_format_grams(item.get('total_grams'))}"
        )
        for item in values
    )


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
