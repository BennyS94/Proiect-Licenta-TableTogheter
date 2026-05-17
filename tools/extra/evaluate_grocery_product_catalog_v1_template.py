from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = ROOT / "data/grocery/draft/grocery_product_catalog_v1_price_research_template.csv"
SUMMARY_OUT = ROOT / "data/grocery/audit/grocery_product_catalog_v1_template_eval_summary.txt"

EXPECTED_COLUMNS = [
    "catalog_item_id",
    "match_key",
    "mapped_food_id",
    "canonical_ingredient_name",
    "display_name_ro",
    "display_name_en",
    "shopping_category",
    "purchase_format_suggestion",
    "package_type",
    "package_size_g",
    "package_size_ml",
    "unit_count",
    "sold_by",
    "price_method",
    "reference_product_name",
    "store_name",
    "reference_price",
    "price_per_kg",
    "price_per_liter",
    "currency",
    "source_url",
    "captured_at",
    "confidence",
    "decision_status",
    "qc_notes",
    "priority",
    "used_in_demo_grocery",
    "occurrence_count",
    "total_needed_grams_in_sample",
]

PRICE_SOURCE_FIELDS = [
    "reference_product_name",
    "store_name",
    "reference_price",
    "price_per_kg",
    "price_per_liter",
    "currency",
    "source_url",
    "captured_at",
]


def main() -> None:
    SUMMARY_OUT.parent.mkdir(parents=True, exist_ok=True)
    catalog = pd.read_csv(CATALOG_PATH).fillna("") if CATALOG_PATH.exists() else pd.DataFrame()
    summary = evaluate_catalog(catalog)
    SUMMARY_OUT.write_text(summary, encoding="utf-8")
    print("Grocery Product Catalog v1 template evaluation written")
    print(f"summary={SUMMARY_OUT}")
    print(_one_line_summary(catalog))


def evaluate_catalog(catalog: pd.DataFrame) -> str:
    missing_columns = [column for column in EXPECTED_COLUMNS if column not in catalog.columns]
    extra_columns = [column for column in catalog.columns if column not in EXPECTED_COLUMNS]
    row_count = len(catalog)
    priority_counts = _counts(catalog.get("priority", []))
    used_count = (
        sum(1 for value in catalog.get("used_in_demo_grocery", []) if _truthy(value))
        if row_count
        else 0
    )
    missing_price_field_count = _missing_price_field_count(catalog)
    status_counts = _counts(catalog.get("decision_status", []))
    ready = (
        row_count > 0
        and not missing_columns
        and missing_price_field_count == row_count
        and set(status_counts) == {"pending_research"}
    )
    lines = [
        "Grocery Product Catalog v1 template evaluation",
        "",
        f"catalog_path={CATALOG_PATH}",
        f"catalog_rows={row_count}",
        f"missing_columns={', '.join(missing_columns) if missing_columns else 'none'}",
        f"extra_columns={', '.join(extra_columns) if extra_columns else 'none'}",
        f"priority_counts={_format_counts(priority_counts)}",
        f"used_in_demo_grocery_count={used_count}",
        f"missing_price_field_count={missing_price_field_count}",
        f"decision_status_counts={_format_counts(status_counts)}",
        f"ready_for_research={str(ready).lower()}",
        "",
        "Top 20 rows to research first:",
        *_top_rows(catalog),
    ]
    return "\n".join(lines) + "\n"


def _top_rows(catalog: pd.DataFrame) -> list[str]:
    if catalog.empty:
        return ["none"]
    data = catalog.copy()
    data["_priority_rank"] = data["priority"].map({"high": 0, "medium": 1, "low": 2}).fillna(9)
    data["_used_rank"] = data["used_in_demo_grocery"].map(lambda value: 0 if _truthy(value) else 1)
    data = data.sort_values(["_priority_rank", "_used_rank", "display_name_en"])
    rows: list[str] = []
    for _, row in data.head(20).iterrows():
        rows.append(
            (
                f"- {row.get('catalog_item_id')} | {row.get('display_name_en')} | "
                f"category={row.get('shopping_category')} | priority={row.get('priority')} | "
                f"used={row.get('used_in_demo_grocery')} | format={row.get('purchase_format_suggestion')}"
            )
        )
    return rows


def _missing_price_field_count(catalog: pd.DataFrame) -> int:
    if catalog.empty:
        return 0
    missing_count = 0
    for _, row in catalog.iterrows():
        if not any(_clean_text(row.get(field)) for field in PRICE_SOURCE_FIELDS):
            missing_count += 1
    return missing_count


def _one_line_summary(catalog: pd.DataFrame) -> str:
    if catalog.empty:
        return "rows=0; ready_for_research=false"
    high_count = int((catalog["priority"] == "high").sum()) if "priority" in catalog else 0
    used_count = (
        sum(1 for value in catalog["used_in_demo_grocery"] if _truthy(value))
        if "used_in_demo_grocery" in catalog
        else 0
    )
    missing_price_count = _missing_price_field_count(catalog)
    return (
        f"rows={len(catalog)}; high_priority={high_count}; "
        f"used_in_demo={used_count}; missing_price_rows={missing_price_count}"
    )


def _counts(values: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = _clean_text(value) or "blank"
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return "; ".join(f"{key}={value}" for key, value in counts.items())


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _truthy(value: Any) -> bool:
    return _clean_text(value).lower() in {"true", "1", "yes", "y"}


if __name__ == "__main__":
    main()
