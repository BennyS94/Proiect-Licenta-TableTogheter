from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FULL_PRODUCT_CATALOG_PATH = PROJECT_ROOT / "data/grocery/draft/grocery_product_catalog_v1_full.csv"
BATCH1_PRODUCT_CATALOG_PATH = PROJECT_ROOT / "data/grocery/draft/grocery_product_catalog_v1_batch1.csv"
TEMPLATE_PRODUCT_CATALOG_PATH = (
    PROJECT_ROOT / "data/grocery/draft/grocery_product_catalog_v1_price_research_template.csv"
)

SAFE_DECISION_STATUS = "safe_to_use_demo"


def load_grocery_product_catalog(path: Path | str | None = None) -> list[dict[str, Any]]:
    resolved_path = Path(path) if path else _default_catalog_path()
    if not resolved_path.exists():
        return []
    with resolved_path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def match_catalog_item(
    grocery_item: dict[str, Any],
    catalog: list[dict[str, Any]],
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
            return row

    match_keys = {
        f"display_alias:{display_key}",
        f"normalized_name_keyword:{display_key}",
        f"canonical:{canonical_key}",
    }
    for row in safe_rows:
        row_key = _normalise_match_key(row.get("match_key"))
        if row_key and row_key in match_keys:
            return row

    return None


def estimate_grocery_item_cost(
    grocery_item: dict[str, Any],
    catalog_row: dict[str, Any] | None,
) -> dict[str, Any]:
    if not catalog_row or _clean_text(catalog_row.get("decision_status")) != SAFE_DECISION_STATUS:
        return _price_result(None, catalog_row, "price_missing")

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
            return _price_result(None, catalog_row, "price_quantity_missing")
        return _price_result(
            round((grams_for_price / 1000.0) * price_per_kg, 2),
            catalog_row,
            "",
        )

    if price_method == "price_per_liter" and price_per_liter is not None:
        if purchase_amount_ml is None:
            return _price_result(None, catalog_row, "price_quantity_missing")
        return _price_result(
            round((purchase_amount_ml / 1000.0) * price_per_liter, 2),
            catalog_row,
            "",
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
            return _price_result(None, catalog_row, "price_quantity_missing")
        return _price_result(round(quantity_for_price * reference_price, 2), catalog_row, "")

    if price_method == "pantry_check":
        return _price_result(None, catalog_row, "price_pantry_check")

    return _price_result(None, catalog_row, "price_missing")


def apply_price_estimates(
    grocery_items: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _ = config or {}
    priced_items: list[dict[str, Any]] = []
    warning_counts: dict[str, int] = {}
    priced_count = 0
    total_estimated_cost = 0.0

    for item in grocery_items:
        catalog_row = match_catalog_item(item, catalog)
        price_fields = estimate_grocery_item_cost(item, catalog_row)
        updated = dict(item)
        updated.update(price_fields)
        priced_items.append(updated)
        warning = _clean_text(price_fields.get("price_warning"))
        if warning:
            warning_counts[warning] = warning_counts.get(warning, 0) + 1
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
            "safe_catalog_rows_loaded": sum(
                1
                for row in catalog
                if _clean_text(row.get("decision_status")) == SAFE_DECISION_STATUS
            ),
        },
    }


def _price_result(
    estimated_cost: float | None,
    catalog_row: dict[str, Any] | None,
    warning: str,
) -> dict[str, Any]:
    row = catalog_row or {}
    return {
        "estimated_cost": estimated_cost,
        "currency": _clean_text(row.get("currency")),
        "price_source_name": _clean_text(row.get("reference_product_name"))
        or _clean_text(row.get("store_name")),
        "price_store_name": _clean_text(row.get("store_name")),
        "price_source_url": _clean_text(row.get("source_url")),
        "price_confidence": _clean_text(row.get("confidence")) or "unknown",
        "price_warning": warning,
        "price_catalog_item_id": _clean_text(row.get("catalog_item_id")),
    }


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


def _default_catalog_path() -> Path:
    if FULL_PRODUCT_CATALOG_PATH.exists():
        return FULL_PRODUCT_CATALOG_PATH
    if BATCH1_PRODUCT_CATALOG_PATH.exists():
        return BATCH1_PRODUCT_CATALOG_PATH
    return TEMPLATE_PRODUCT_CATALOG_PATH


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
