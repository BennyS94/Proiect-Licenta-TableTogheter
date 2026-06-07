from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_PRODUCT_CATALOG_PATH = PROJECT_ROOT / "data/grocery/reference/grocery_product_catalog_v1.csv"
FULL_PRODUCT_CATALOG_PATH = PROJECT_ROOT / "data/grocery/draft/grocery_product_catalog_v1_full.csv"
BATCH1_PRODUCT_CATALOG_PATH = PROJECT_ROOT / "data/grocery/draft/grocery_product_catalog_v1_batch1.csv"
TEMPLATE_PRODUCT_CATALOG_PATH = (
    PROJECT_ROOT / "data/grocery/draft/grocery_product_catalog_v1_price_research_template.csv"
)
DEFAULT_PRODUCT_ALIASES_PATH = PROJECT_ROOT / "data/grocery/reference/grocery_product_aliases_v1.csv"
DEFAULT_PRICE_FALLBACKS_PATH = PROJECT_ROOT / "data/grocery/reference/grocery_price_fallbacks_v1.csv"

SAFE_DECISION_STATUS = "safe_to_use_demo"
SAFE_ALIAS_STATUS = "safe_to_use_demo"
SAFE_FALLBACK_STATUS = "safe_to_use_demo"
PRICE_CONFIDENCE_MAP = {
    "high": "high_source_exact",
    "source_exact": "high_source_exact",
    "high_source_exact": "high_source_exact",
    "medium": "medium_source_equivalent",
    "source_equivalent": "medium_source_equivalent",
    "medium_source_equivalent": "medium_source_equivalent",
    "low": "medium_source_equivalent",
    "category_fallback": "low_category_fallback",
    "low_category_fallback": "low_category_fallback",
    "emergency_fallback": "very_low_emergency_fallback",
    "very_low_emergency_fallback": "very_low_emergency_fallback",
}


def load_grocery_product_catalog(path: Path | str | None = None) -> list[dict[str, Any]]:
    resolved_path = Path(path) if path else _default_catalog_path()
    if not resolved_path.exists():
        return []
    with resolved_path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_grocery_product_aliases(path: Path | str | None = None) -> list[dict[str, Any]]:
    resolved_path = _resolve_optional_reference_path(path, DEFAULT_PRODUCT_ALIASES_PATH)
    if not resolved_path.exists():
        return []
    with resolved_path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_grocery_price_fallbacks(path: Path | str | None = None) -> list[dict[str, Any]]:
    resolved_path = _resolve_optional_reference_path(path, DEFAULT_PRICE_FALLBACKS_PATH)
    if not resolved_path.exists():
        return []
    with resolved_path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def match_catalog_item(
    grocery_item: dict[str, Any],
    catalog: list[dict[str, Any]],
) -> dict[str, Any] | None:
    match = _match_catalog_item(grocery_item, catalog, aliases=[])
    return match.get("row") if match else None


def match_catalog_item_with_aliases(
    grocery_item: dict[str, Any],
    catalog: list[dict[str, Any]],
    aliases: list[dict[str, Any]],
) -> dict[str, Any] | None:
    return _match_catalog_item(grocery_item, catalog, aliases=aliases)


def _match_catalog_item(
    grocery_item: dict[str, Any],
    catalog: list[dict[str, Any]],
    aliases: list[dict[str, Any]],
) -> dict[str, Any] | None:
    safe_rows = [
        row
        for row in catalog
        if _clean_text(row.get("decision_status")) == SAFE_DECISION_STATUS
    ]
    if not safe_rows:
        return None

    mapped_ids = _grocery_item_mapped_food_ids(grocery_item)
    display_key = _normalise_key(
        grocery_item.get("display_name_clean")
        or grocery_item.get("display_name")
        or grocery_item.get("canonical_name")
    )
    canonical_key = _normalise_key(grocery_item.get("canonical_name"))

    for row in safe_rows:
        row_mapped_ids = _split_ids(row.get("mapped_food_id"))
        if row_mapped_ids and row_mapped_ids.intersection(mapped_ids):
            return {"row": row, "match_layer": "exact_catalog", "match_method": "mapped_food_id"}

    match_keys = {
        f"display_alias:{display_key}",
        f"normalized_name_keyword:{display_key}",
        f"canonical:{canonical_key}",
    }
    for row in safe_rows:
        row_key = _normalise_match_key(row.get("match_key"))
        if row_key and row_key in match_keys:
            return {"row": row, "match_layer": "exact_catalog", "match_method": row_key.split(":", 1)[0]}

    alias_match = _match_alias(grocery_item, aliases)
    if alias_match:
        target_catalog_item_id = _clean_text(alias_match.get("target_catalog_item_id"))
        target_match_key = _normalise_match_key(alias_match.get("target_match_key"))
        for row in safe_rows:
            if target_catalog_item_id and _clean_text(row.get("catalog_item_id")) == target_catalog_item_id:
                return {
                    "row": row,
                    "match_layer": "alias_catalog",
                    "match_method": _clean_text(alias_match.get("match_type")) or "alias",
                    "alias_id": _clean_text(alias_match.get("alias_id")),
                }
        for row in safe_rows:
            if target_match_key and _normalise_match_key(row.get("match_key")) == target_match_key:
                return {
                    "row": row,
                    "match_layer": "alias_catalog",
                    "match_method": _clean_text(alias_match.get("match_type")) or "alias",
                    "alias_id": _clean_text(alias_match.get("alias_id")),
                }

    return None


def estimate_grocery_item_cost(
    grocery_item: dict[str, Any],
    catalog_row: dict[str, Any] | None,
    *,
    price_layer: str = "exact_catalog",
    match_method: str = "",
    alias_id: str = "",
) -> dict[str, Any]:
    if not catalog_row or _clean_text(catalog_row.get("decision_status")) != SAFE_DECISION_STATUS:
        return _price_result(None, catalog_row, "price_missing", price_layer, match_method, alias_id)

    price_method = _clean_text(catalog_row.get("price_method"))
    reference_price = _to_float(catalog_row.get("reference_price"))
    price_per_kg = _to_float(catalog_row.get("price_per_kg"))
    price_per_liter = _to_float(catalog_row.get("price_per_liter"))
    purchase_quantity = _to_float(grocery_item.get("purchase_quantity"))
    purchase_amount_grams = _to_float(grocery_item.get("purchase_amount_grams"))
    needed_grams = _to_float(grocery_item.get("needed_grams_exact"))
    if needed_grams is None:
        needed_grams = _to_float(grocery_item.get("total_grams"))
    purchase_amount_ml = _purchase_amount_ml(grocery_item, catalog_row)

    if price_method == "price_per_kg" and price_per_kg is not None:
        grams_for_price = purchase_amount_grams if purchase_amount_grams is not None else needed_grams
        if grams_for_price is None:
            return _price_result(None, catalog_row, "price_quantity_missing", price_layer, match_method, alias_id)
        return _price_result(
            round((grams_for_price / 1000.0) * price_per_kg, 2),
            catalog_row,
            "",
            price_layer,
            match_method,
            alias_id,
        )

    if price_method == "price_per_liter" and price_per_liter is not None:
        if purchase_amount_ml is None:
            return _price_result(None, catalog_row, "price_quantity_missing", price_layer, match_method, alias_id)
        return _price_result(
            round((purchase_amount_ml / 1000.0) * price_per_liter, 2),
            catalog_row,
            "",
            price_layer,
            match_method,
            alias_id,
        )

    if price_method in {"price_per_package", "price_per_piece"} and reference_price is not None:
        quantity_for_price = _purchase_quantity_for_catalog_price(
            grocery_item,
            catalog_row,
            price_method=price_method,
            purchase_quantity=purchase_quantity,
            purchase_amount_grams=purchase_amount_grams,
            needed_grams=needed_grams,
        )
        if quantity_for_price is None:
            return _price_result(None, catalog_row, "price_quantity_missing", price_layer, match_method, alias_id)
        return _price_result(
            round(quantity_for_price * reference_price, 2),
            catalog_row,
            "",
            price_layer,
            match_method,
            alias_id,
        )

    if price_method == "pantry_check":
        return _price_result(0.0, catalog_row, "price_pantry_check", price_layer, match_method, alias_id)

    return _price_result(None, catalog_row, "price_missing", price_layer, match_method, alias_id)


def apply_price_estimates(
    grocery_items: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config_data = config or {}
    aliases = load_grocery_product_aliases(config_data.get("product_aliases_path"))
    fallbacks = load_grocery_price_fallbacks(config_data.get("price_fallbacks_path"))
    priced_items: list[dict[str, Any]] = []
    warning_counts: dict[str, int] = {}
    method_counts: dict[str, int] = {}
    priced_count = 0
    total_estimated_cost = 0.0

    for item in grocery_items:
        match = match_catalog_item_with_aliases(item, catalog, aliases)
        price_fields = (
            estimate_grocery_item_cost(
                item,
                match.get("row"),
                price_layer=match.get("match_layer", "exact_catalog"),
                match_method=match.get("match_method", ""),
                alias_id=match.get("alias_id", ""),
            )
            if match
            else _price_result(None, None, "price_missing", "exact_catalog", "", "")
        )
        if price_fields.get("estimated_cost") is None:
            fallback_row = match_price_fallback(item, fallbacks)
            if fallback_row:
                price_fields = estimate_grocery_item_cost(
                    item,
                    fallback_row,
                    price_layer="category_fallback",
                    match_method=_clean_text(fallback_row.get("fallback_id")),
                )
                if not _clean_text(price_fields.get("price_warning")):
                    price_fields["price_warning"] = "category_fallback_estimate"
        if price_fields.get("estimated_cost") is None:
            emergency_row = _emergency_fallback_row(item)
            price_fields = estimate_grocery_item_cost(
                item,
                emergency_row,
                price_layer="emergency_fallback",
                match_method=_clean_text(emergency_row.get("fallback_id")),
            )
            if not _clean_text(price_fields.get("price_warning")):
                price_fields["price_warning"] = "emergency_fallback_estimate"

        updated = dict(item)
        updated.update(price_fields)
        priced_items.append(updated)
        warning = _clean_text(price_fields.get("price_warning"))
        if warning:
            warning_counts[warning] = warning_counts.get(warning, 0) + 1
        method = _clean_text(price_fields.get("price_estimation_method"))
        if method:
            method_counts[method] = method_counts.get(method, 0) + 1
        if price_fields.get("estimated_cost") is not None:
            priced_count += 1
            total_estimated_cost += float(price_fields["estimated_cost"])

    return {
        "items": priced_items,
        "summary": {
            "pricing_item_count": len(priced_items),
            "priced_item_count": priced_count,
            "unpriced_item_count": len(priced_items) - priced_count,
            "total_estimated_cost": round(total_estimated_cost, 2) if priced_count else None,
            "currency": _common_currency(priced_items),
            "price_warning_counts": dict(sorted(warning_counts.items())),
            "price_method_counts": dict(sorted(method_counts.items())),
            "safe_catalog_rows_loaded": sum(
                1
                for row in catalog
                if _clean_text(row.get("decision_status")) == SAFE_DECISION_STATUS
            ),
            "product_alias_rows_loaded": len(aliases),
            "price_fallback_rows_loaded": len(fallbacks),
        },
    }


def _price_result(
    estimated_cost: float | None,
    catalog_row: dict[str, Any] | None,
    warning: str,
    price_layer: str,
    match_method: str,
    alias_id: str,
) -> dict[str, Any]:
    row = catalog_row or {}
    method = _price_estimation_method(price_layer, warning)
    return {
        "estimated_cost": estimated_cost,
        "currency": _clean_text(row.get("currency")),
        "price_source_name": _clean_text(row.get("reference_product_name"))
        or _clean_text(row.get("store_name")),
        "price_store_name": _clean_text(row.get("store_name")),
        "price_source_url": _clean_text(row.get("source_url")),
        "price_source_checked_at": _clean_text(row.get("source_checked_at"))
        or _clean_text(row.get("captured_at")),
        "price_confidence": _normalise_confidence(row.get("confidence"), method),
        "price_warning": warning,
        "price_catalog_item_id": _clean_text(row.get("catalog_item_id")),
        "price_fallback_id": _clean_text(row.get("fallback_id")),
        "price_estimation_method": method,
        "price_match_method": match_method,
        "price_alias_id": alias_id,
        "price_unit_basis": _clean_text(row.get("unit_basis"))
        or _unit_basis_from_row(row),
        "price_reference_value": _clean_text(row.get("reference_price"))
        or _clean_text(row.get("price_per_kg"))
        or _clean_text(row.get("price_per_liter")),
    }


def match_price_fallback(
    grocery_item: dict[str, Any],
    fallbacks: list[dict[str, Any]],
) -> dict[str, Any] | None:
    safe_rows = [
        row
        for row in fallbacks
        if _clean_text(row.get("decision_status")) == SAFE_FALLBACK_STATUS
    ]
    if not safe_rows:
        return None

    category = _clean_text(grocery_item.get("grocery_category"))
    text = _normalise_key(
        " ".join(
            [
                _clean_text(grocery_item.get("display_name_clean")),
                _clean_text(grocery_item.get("display_name")),
                _clean_text(grocery_item.get("canonical_name")),
                " ".join(_list_values(grocery_item.get("ingredient_names_seen"))),
                " ".join(_list_values(grocery_item.get("source_item_names"))),
            ]
        )
    )
    category_rows = [
        row
        for row in safe_rows
        if not _clean_text(row.get("grocery_category"))
        or _clean_text(row.get("grocery_category")) == category
    ]
    for row in category_rows:
        keywords = _match_values(row.get("match_keywords"))
        if keywords and any(_contains_phrase(text, keyword) for keyword in keywords):
            return _fallback_row_as_catalog(row)
    for row in category_rows:
        if not _match_values(row.get("match_keywords")):
            return _fallback_row_as_catalog(row)
    return None


def _purchase_amount_ml(
    grocery_item: dict[str, Any],
    catalog_row: dict[str, Any],
) -> float | None:
    direct_ml = _to_float(grocery_item.get("purchase_amount_ml"))
    if direct_ml is not None:
        return direct_ml
    quantity = _to_float(grocery_item.get("purchase_quantity"))
    package_size_ml = _to_float(catalog_row.get("package_size_ml"))
    if quantity is None or package_size_ml is None:
        return None
    return quantity * package_size_ml


def _purchase_quantity_for_catalog_price(
    grocery_item: dict[str, Any],
    catalog_row: dict[str, Any],
    *,
    price_method: str,
    purchase_quantity: float | None,
    purchase_amount_grams: float | None,
    needed_grams: float | None,
) -> float | None:
    unit_count = _to_float(catalog_row.get("unit_count"))
    package_size_g = _to_float(catalog_row.get("package_size_g"))
    if price_method == "price_per_piece":
        return purchase_quantity
    if unit_count and purchase_quantity:
        return float(math.ceil(purchase_quantity / unit_count))
    grams_for_price = purchase_amount_grams if purchase_amount_grams is not None else needed_grams
    if package_size_g and grams_for_price is not None:
        return float(math.ceil(grams_for_price / package_size_g))
    package_size_ml = _to_float(catalog_row.get("package_size_ml"))
    purchase_amount_ml = _purchase_amount_ml(grocery_item, catalog_row)
    if package_size_ml and purchase_amount_ml is not None:
        return float(math.ceil(purchase_amount_ml / package_size_ml))
    if package_size_ml and needed_grams is not None:
        return float(math.ceil(needed_grams / package_size_ml))
    return purchase_quantity


def _match_alias(
    grocery_item: dict[str, Any],
    aliases: list[dict[str, Any]],
) -> dict[str, Any] | None:
    safe_aliases = [
        row
        for row in aliases
        if _clean_text(row.get("decision_status")) == SAFE_ALIAS_STATUS
    ]
    if not safe_aliases:
        return None

    display_name = _normalise_key(
        grocery_item.get("display_name_clean") or grocery_item.get("display_name")
    )
    search_text = _normalise_key(
        " ".join(
            [
                _clean_text(grocery_item.get("display_name_clean")),
                _clean_text(grocery_item.get("display_name")),
                _clean_text(grocery_item.get("canonical_name")),
                " ".join(_list_values(grocery_item.get("ingredient_names_seen"))),
                " ".join(_list_values(grocery_item.get("source_item_names"))),
            ]
        )
    )
    for alias in safe_aliases:
        match_type = _clean_text(alias.get("match_type"))
        match_values = _match_values(alias.get("match_value"))
        if not match_values:
            continue
        if match_type == "display_alias":
            if any(display_name == _normalise_key(value) for value in match_values):
                return alias
            continue
        if any(_contains_phrase(search_text, value) for value in match_values):
            return alias
    return None


def _fallback_row_as_catalog(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "catalog_item_id": "",
        "fallback_id": _clean_text(row.get("fallback_id")),
        "decision_status": SAFE_DECISION_STATUS,
        "price_method": _clean_text(row.get("price_method")) or "price_per_kg",
        "reference_price": _clean_text(row.get("reference_price")),
        "price_per_kg": _clean_text(row.get("price_per_kg")),
        "price_per_liter": _clean_text(row.get("price_per_liter")),
        "package_size_g": _clean_text(row.get("package_size_g")),
        "package_size_ml": _clean_text(row.get("package_size_ml")),
        "unit_count": _clean_text(row.get("unit_count")),
        "currency": _clean_text(row.get("currency")) or "RON",
        "reference_product_name": _clean_text(row.get("reference_product_name"))
        or _clean_text(row.get("fallback_label")),
        "store_name": _clean_text(row.get("source_store")) or "Controlled demo fallback",
        "source_url": _clean_text(row.get("source_url")),
        "source_checked_at": _clean_text(row.get("source_checked_at")),
        "confidence": _clean_text(row.get("confidence")) or "low_category_fallback",
        "unit_basis": _clean_text(row.get("unit_basis")),
    }


def _emergency_fallback_row(grocery_item: dict[str, Any]) -> dict[str, Any]:
    category = _clean_text(grocery_item.get("grocery_category")) or "other_review"
    return {
        "catalog_item_id": "",
        "fallback_id": f"emergency_{category}",
        "decision_status": SAFE_DECISION_STATUS,
        "price_method": "price_per_kg",
        "price_per_kg": "30",
        "currency": "RON",
        "reference_product_name": "Emergency demo fallback per kg",
        "store_name": "Controlled emergency fallback",
        "source_url": "",
        "source_checked_at": "",
        "confidence": "very_low_emergency_fallback",
        "unit_basis": "RON per kg",
    }


def _price_estimation_method(price_layer: str, warning: str) -> str:
    if price_layer == "category_fallback":
        return "category_fallback"
    if price_layer == "emergency_fallback":
        return "emergency_fallback"
    if price_layer == "alias_catalog":
        return "alias_catalog"
    if warning == "price_pantry_check":
        return "pantry_check"
    return "exact_catalog"


def _normalise_confidence(value: Any, method: str) -> str:
    if method == "category_fallback":
        return "low_category_fallback"
    if method == "emergency_fallback":
        return "very_low_emergency_fallback"
    text = _clean_text(value).lower()
    return PRICE_CONFIDENCE_MAP.get(text, text or "medium_source_equivalent")


def _unit_basis_from_row(row: dict[str, Any]) -> str:
    method = _clean_text(row.get("price_method"))
    if method == "price_per_kg":
        return "RON per kg"
    if method == "price_per_liter":
        return "RON per liter"
    if method == "price_per_piece":
        return "RON per piece"
    if method == "price_per_package":
        package_size_g = _clean_text(row.get("package_size_g"))
        package_size_ml = _clean_text(row.get("package_size_ml"))
        if package_size_g:
            return f"RON per {package_size_g}g package"
        if package_size_ml:
            return f"RON per {package_size_ml}ml package"
        return "RON per package"
    if method == "pantry_check":
        return "pantry check"
    return method


def _grocery_item_mapped_food_ids(grocery_item: dict[str, Any]) -> set[str]:
    result = _split_ids(grocery_item.get("source_mapped_food_ids"))
    mapped_food_id = _clean_text(grocery_item.get("mapped_food_id"))
    if mapped_food_id:
        result.add(mapped_food_id)
    return result


def _split_ids(value: Any) -> set[str]:
    if isinstance(value, list):
        return {_clean_text(item) for item in value if _clean_text(item)}
    return {
        _clean_text(item)
        for item in str(value or "").replace(",", ";").split(";")
        if _clean_text(item)
    }


def _list_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    text = _clean_text(value)
    if not text:
        return []
    if ";" in text:
        return [item.strip() for item in text.split(";") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text]


def _match_values(value: Any) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    return [item.strip() for item in text.split("|") if item.strip()]


def _contains_phrase(text: str, phrase: Any) -> bool:
    clean_text = f" {_normalise_key(text)} "
    clean_phrase = _normalise_key(phrase)
    if not clean_phrase:
        return False
    return f" {clean_phrase} " in clean_text


def _default_catalog_path() -> Path:
    if REFERENCE_PRODUCT_CATALOG_PATH.exists():
        return REFERENCE_PRODUCT_CATALOG_PATH
    if FULL_PRODUCT_CATALOG_PATH.exists():
        return FULL_PRODUCT_CATALOG_PATH
    if BATCH1_PRODUCT_CATALOG_PATH.exists():
        return BATCH1_PRODUCT_CATALOG_PATH
    return TEMPLATE_PRODUCT_CATALOG_PATH


def _resolve_optional_reference_path(path: Path | str | None, default_path: Path) -> Path:
    if path is None or str(path).strip() == "":
        return default_path
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    root_candidate = PROJECT_ROOT / candidate
    if root_candidate.exists():
        return root_candidate
    return candidate


def _common_currency(items: list[dict[str, Any]]) -> str:
    currencies = sorted(
        {
            _clean_text(item.get("currency"))
            for item in items
            if item.get("estimated_cost") is not None and _clean_text(item.get("currency"))
        }
    )
    return currencies[0] if len(currencies) == 1 else ""


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _normalise_key(value: Any) -> str:
    text = _clean_text(value).lower()
    for char in ["_", "-", "(", ")", ","]:
        text = text.replace(char, " ")
    return " ".join(text.split())


def _normalise_match_key(value: Any) -> str:
    text = _clean_text(value)
    if ":" not in text:
        return _normalise_key(text)
    prefix, raw_key = text.split(":", 1)
    return f"{prefix.strip()}:{_normalise_key(raw_key)}"


def _to_float(value: Any) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
