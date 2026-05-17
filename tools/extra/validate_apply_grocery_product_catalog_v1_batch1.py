from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = ROOT / "data/grocery/draft/grocery_product_catalog_v1_price_research_filled_batch1.csv"
CATALOG_OUT = ROOT / "data/grocery/draft/grocery_product_catalog_v1_batch1.csv"
VALIDATION_OUT = ROOT / "data/grocery/audit/grocery_product_catalog_v1_batch1_validation.csv"
SUMMARY_OUT = ROOT / "data/grocery/audit/grocery_product_catalog_v1_batch1_summary.txt"
DEFERRED_OUT = ROOT / "data/grocery/audit/grocery_product_catalog_v1_batch1_deferred.csv"

ALLOWED_DECISIONS = {"safe_to_use_demo", "needs_review", "keep_deferred"}
PRICE_METHODS = {
    "price_per_kg",
    "price_per_liter",
    "price_per_package",
    "price_per_piece",
    "pantry_check",
    "unknown",
}


def main() -> None:
    CATALOG_OUT.parent.mkdir(parents=True, exist_ok=True)
    VALIDATION_OUT.parent.mkdir(parents=True, exist_ok=True)
    batch = pd.read_csv(INPUT_PATH).fillna("")
    validation_rows = [validate_row(row.to_dict()) for _, row in batch.iterrows()]
    validation = pd.DataFrame(validation_rows)
    safe_mask = (
        (validation["decision_status"] == "safe_to_use_demo")
        & (validation["validation_status"] == "pass")
    )
    applied = batch.loc[safe_mask].copy()
    deferred = batch.loc[~safe_mask].copy()
    deferred_validation = validation.loc[~safe_mask, [
        "validation_status",
        "validation_errors",
        "applied_to_catalog",
    ]]
    deferred = pd.concat(
        [
            deferred.reset_index(drop=True),
            deferred_validation.reset_index(drop=True),
        ],
        axis=1,
    )

    validation.to_csv(VALIDATION_OUT, index=False)
    applied.to_csv(CATALOG_OUT, index=False)
    deferred.to_csv(DEFERRED_OUT, index=False)
    SUMMARY_OUT.write_text(
        build_summary(batch=batch, validation=validation, applied=applied, deferred=deferred),
        encoding="utf-8",
    )

    print("Grocery Product Catalog v1 batch1 validated and applied")
    print(f"catalog={CATALOG_OUT}")
    print(f"validation={VALIDATION_OUT}")
    print(f"summary={SUMMARY_OUT}")
    print(f"deferred={DEFERRED_OUT}")
    print(
        "rows="
        f"{len(batch)}; safe_applied={len(applied)}; "
        f"needs_review={(batch['decision_status'] == 'needs_review').sum()}; "
        f"keep_deferred={(batch['decision_status'] == 'keep_deferred').sum()}"
    )


def validate_row(row: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    decision = _clean_text(row.get("decision_status"))
    price_method = _clean_text(row.get("price_method"))

    if decision not in ALLOWED_DECISIONS:
        errors.append("invalid_decision_status")
    if price_method and price_method not in PRICE_METHODS:
        errors.append("invalid_price_method")

    if decision == "safe_to_use_demo":
        errors.extend(_validate_safe_row(row, price_method))

    validation_status = "pass" if not errors else "fail"
    applied = decision == "safe_to_use_demo" and validation_status == "pass"
    result = dict(row)
    result.update(
        {
            "validation_status": validation_status,
            "validation_errors": ";".join(errors),
            "applied_to_catalog": str(applied).lower(),
        }
    )
    return result


def _validate_safe_row(row: dict[str, Any], price_method: str) -> list[str]:
    errors: list[str] = []
    if not _clean_text(row.get("source_url")):
        errors.append("missing_source_url")
    if not _clean_text(row.get("captured_at")):
        errors.append("missing_captured_at")
    if _clean_text(row.get("currency")) != "RON":
        errors.append("currency_not_ron")
    if price_method not in PRICE_METHODS or price_method in {"unknown", ""}:
        errors.append("unsafe_price_method")

    for field in ["reference_price", "price_per_kg", "price_per_liter"]:
        value = _clean_text(row.get(field))
        if value and _to_float(value) is None:
            errors.append(f"{field}_not_numeric")
        elif value and float(value) < 0:
            errors.append(f"{field}_negative")

    if price_method == "price_per_kg" and _to_float(row.get("price_per_kg")) is None:
        errors.append("missing_price_per_kg")
    if price_method == "price_per_liter" and _to_float(row.get("price_per_liter")) is None:
        errors.append("missing_price_per_liter")
    if price_method in {"price_per_package", "price_per_piece"}:
        if _to_float(row.get("reference_price")) is None:
            errors.append("missing_reference_price")
        if price_method == "price_per_package" and not _has_package_basis(row):
            errors.append("missing_package_basis")
    if price_method == "pantry_check" and not _clean_text(row.get("qc_notes")):
        errors.append("missing_pantry_check_note")
    return errors


def _has_package_basis(row: dict[str, Any]) -> bool:
    return any(
        _to_float(row.get(field)) is not None and float(_to_float(row.get(field)) or 0) > 0
        for field in ["package_size_g", "package_size_ml", "unit_count"]
    )


def build_summary(
    *,
    batch: pd.DataFrame,
    validation: pd.DataFrame,
    applied: pd.DataFrame,
    deferred: pd.DataFrame,
) -> str:
    status_counts = _counts(batch.get("decision_status", []))
    validation_counts = _counts(validation.get("validation_status", []))
    safe_rows = batch[batch["decision_status"] == "safe_to_use_demo"]
    failed_safe = validation[
        (validation["decision_status"] == "safe_to_use_demo")
        & (validation["validation_status"] != "pass")
    ]
    lines = [
        "Grocery Product Catalog v1 batch1 validation summary",
        "",
        f"input_path={INPUT_PATH}",
        f"catalog_out={CATALOG_OUT}",
        "",
        f"rows_reviewed={len(batch)}",
        f"safe_rows_submitted={len(safe_rows)}",
        f"safe_rows_applied={len(applied)}",
        f"safe_rows_rejected={len(failed_safe)}",
        f"needs_review_count={status_counts.get('needs_review', 0)}",
        f"keep_deferred_count={status_counts.get('keep_deferred', 0)}",
        f"deferred_audit_count={len(deferred)}",
        f"decision_status_counts={_format_counts(status_counts)}",
        f"validation_status_counts={_format_counts(validation_counts)}",
        "",
        "Applied rules:",
        "- only decision_status=safe_to_use_demo rows with passing sanity checks are written to the usable catalog",
        "- needs_review and keep_deferred rows remain audit-only",
        "- source_url, confidence, and qc_notes are preserved",
        "- no prices were invented by the validator",
    ]
    if not failed_safe.empty:
        lines.extend(
            [
                "",
                "Rejected safe rows:",
                *[
                    (
                        f"- {row.get('catalog_item_id')} | {row.get('display_name_en')} | "
                        f"errors={row.get('validation_errors')}"
                    )
                    for _, row in failed_safe.iterrows()
                ],
            ]
        )
    return "\n".join(lines) + "\n"


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


def _to_float(value: Any) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


if __name__ == "__main__":
    main()
